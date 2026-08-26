// pi_gateway/lifecycle/reconnect-watcher.ts — failed-platform retry queue +
// supervised self-respawning reconnect watcher. Hermes anchors (READ-ONLY
// reference; semantics ported, no code vendored — gateway/run.py):
//   GatewayRunner._failed_platforms        → FailedPlatformQueue
//     (startup-failed / dropped platforms are QUEUED for background
//     reconnection instead of staying down until process restart)
//   _platform_reconnect_watcher            → createReconnectWatcherService
//     (initial 10s delay lets startup finish; retries on exponential backoff
//     30s → 60s → 120s … capped at 300s (_reconnect_backoff); a healthy pass
//     idles on a 30s cadence)
//   _spawn_supervised (#71758)             → supervise() inside the service
//     entry: an exception escaping one pass is contained and the watcher
//     RESPAWNS itself loudly rather than dying — the queue outlives any
//     single watcher crash.
//
// DEC-040 seam: this module exposes a ServiceEntry for stage 8
// (embedded_watchers) so the composition root registers it like any other
// embedded service. The reconnect callback is injected — adapters live in
// later phases; nothing here imports platform code (01 §5.3).

import type {
	ServiceEntry,
	ServiceHandle,
	ServiceStartOutcome,
} from "./lifecycle.js";
import type { Logger } from "./shutdown.js";
import { realTimerPort, type TimerPort } from "./watchdog.js";

/** One queued platform's retry bookkeeping. */
export interface FailedPlatformInfo {
	platform: string;
	attempts: number;
	/** Timer-port ms timestamp of the original enqueue (attention math). */
	queuedAt: number;
	/** Next retry deadline (same clock as queuedAt). */
	nextRetryAt: number;
	reason?: string;
}

/**
 * Max seconds between reconnect retries (run.py:_RECONNECT_BACKOFF_CAP).
 */
export const RECONNECT_BACKOFF_CAP_MS = 300_000;
/** Exponential base: first retry after 30s (run.py:_reconnect_backoff). */
export const RECONNECT_BASE_DELAY_MS = 30_000;
/** Initial watcher delay — let startup finish before first pass. */
export const RECONNECT_INITIAL_DELAY_MS = 10_000;
/** Idle cadence when there is nothing to retry (30s-cadent watcher). */
export const RECONNECT_POLL_INTERVAL_MS = 30_000;

/** min(30s * 2^(attempt-1), 300s) — run.py:_reconnect_backoff parity. */
export function reconnectBackoffDelayMs(attempt: number): number {
	const n = Math.max(1, Math.floor(attempt));
	return Math.min(
		RECONNECT_BASE_DELAY_MS * 2 ** (n - 1),
		RECONNECT_BACKOFF_CAP_MS,
	);
}

export class FailedPlatformQueue {
	private readonly entries = new Map<string, FailedPlatformInfo>();
	private readonly nowMs: () => number;

	constructor(nowMs: () => number = Date.now) {
		this.nowMs = nowMs;
	}

	enqueue(
		platform: string,
		info: { attempts?: number; reason?: string; backoffMs?: number } = {},
	): FailedPlatformInfo {
		const now = this.nowMs();
		const existing = this.entries.get(platform);
		const attempts = info.attempts ?? (existing?.attempts ?? 0) + 1;
		const entry: FailedPlatformInfo = {
			platform,
			attempts,
			queuedAt: existing?.queuedAt ?? now,
			nextRetryAt: now + (info.backoffMs ?? reconnectBackoffDelayMs(attempts)),
			...(info.reason !== undefined ? { reason: info.reason } : {}),
		};
		this.entries.set(platform, entry);
		return entry;
	}

	get(platform: string): FailedPlatformInfo | null {
		return this.entries.get(platform) ?? null;
	}

	remove(platform: string): void {
		this.entries.delete(platform);
	}

	keys(): string[] {
		return [...this.entries.keys()];
	}

	get size(): number {
		return this.entries.size;
	}
}

/** Reconnect attempt seam — true ⇒ reconnected (queue entry removed). */
export interface ReconnectHooks {
	reconnect(
		platform: string,
		info: FailedPlatformInfo,
	): Promise<boolean> | boolean;
}

export interface ReconnectWatcherOptions {
	queue: FailedPlatformQueue;
	hooks: ReconnectHooks;
	logger?: Logger;
	timer?: TimerPort;
	initialDelayMs?: number;
	pollIntervalMs?: number;
}

interface WatcherRuntime {
	stop(): void;
	respawns(): number;
}

/**
 * The supervised reconnect loop: every poll pass retries every queued
 * platform whose next_retry deadline passed; success removes it, failure
 * bumps attempts and reschedules with doubled backoff. An exception escaping
 * ANY point is caught, logged CRITICALLY-loud, and the loop respawns — the
 * watcher supervises itself (#71758 semantics adapted in-process).
 */
function runSupervisedWatcher(
	options: ReconnectWatcherOptions,
): WatcherRuntime {
	const timer = options.timer ?? realTimerPort();
	const initialDelayMs = options.initialDelayMs ?? RECONNECT_INITIAL_DELAY_MS;
	const pollIntervalMs = options.pollIntervalMs ?? RECONNECT_POLL_INTERVAL_MS;
	let stopped = false;
	let handle: unknown = null;
	let respawnCount = 0;

	/**
	 * One retry sweep over the queue. Per-platform failures (thrown or false)
	 * are contained HERE so one broken adapter can never starve its siblings;
	 * anything escaping this function hits the supervision boundary below.
	 */
	async function passOnce(): Promise<void> {
		const now = timer.nowMs();
		for (const platform of options.queue.keys()) {
			if (stopped) return;
			const info = options.queue.get(platform);
			if (info === null || now < info.nextRetryAt) continue;
			options.logger?.info("reconnecting platform", {
				platform,
				attempt: info.attempts + 1,
			});
			let ok = false;
			try {
				ok = await options.hooks.reconnect(platform, info);
			} catch (err) {
				options.logger?.warn("reconnect attempt raised", {
					platform,
					error: String(err),
				});
				ok = false;
			}
			if (stopped) return;
			if (ok) {
				options.queue.remove(platform);
				options.logger?.info("platform reconnected", { platform });
			} else {
				// Keep the ORIGINAL queuedAt (continuous-outage accounting) but
				// escalate the backoff for the next attempt.
				options.queue.enqueue(platform, {
					attempts: info.attempts + 1,
					...(info.reason !== undefined ? { reason: info.reason } : {}),
				});
			}
		}
	}

	function tick(): void {
		if (stopped) return;
		// Re-arm BEFORE the pass so the cadence stays fixed regardless of pass
		// duration or microtask interleaving.
		handle = timer.setTimeout(tick, pollIntervalMs);
		void passOnce().catch((err) => {
			// Supervision boundary (#71758 parity): a crash anywhere outside the
			// per-platform containment must not kill the watcher — log loud and
			// RESPAWN; the next tick is already scheduled.
			respawnCount++;
			options.logger?.error("reconnect watcher crashed — respawning", {
				respawn: respawnCount,
				error: String(err),
			});
		});
	}

	handle = timer.setTimeout(tick, initialDelayMs);

	return {
		stop(): void {
			stopped = true;
			if (handle !== null) timer.clearTimeout(handle);
		},
		respawns(): number {
			return respawnCount;
		},
	};
}

/**
 * Stage-8 service entry wrapping the supervised reconnect watcher (DEC-040).
 * The returned handle stops the watcher on drain like every other service.
 */
export function createReconnectWatcherService(
	options: ReconnectWatcherOptions,
): ServiceEntry & { runtime(): WatcherRuntime | null } {
	let runtime: WatcherRuntime | null = null;
	const service: ServiceEntry = {
		name: "platform.reconnect-watcher",
		start(_ctx): ServiceStartOutcome {
			runtime = runSupervisedWatcher(options);
			const handle: ServiceHandle = {
				name: "platform.reconnect-watcher",
				stop: async () => {
					runtime?.stop();
				},
			};
			return { ok: true, handle };
		},
	};
	return {
		...service,
		runtime(): WatcherRuntime | null {
			return runtime;
		},
	};
}
