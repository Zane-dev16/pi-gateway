// watcher.ts — the supervised stage-8 loop-wakeup watcher: fires due /loop
// wakeups for idle gateway sessions. Port of gateway/run.py:_loop_wakeup_watcher
// (spawned supervised at start_gateway; gap-audit R1 roster row "loop-wakeup
// 15 s (persisted loop:* rows)").
//
// The gateway has no per-session scheduler thread, so a coarse ticker scans
// persisted loops (state_meta `loop:<session_id>` rows) every 15 s and
// injects each due wakeup prompt into the session's chat via the SAME
// synthetic-message path used by watch notifications and async-delegation
// completions (DEC-022 push lane: internal event through the NORMAL ingress —
// both guards traverse). Deferrals, run.py arrival order:
//
//   - tick already awaiting its turn's response → skip (double-fire guard);
//   - no routing metadata on the loop → skip silently (CLI/TUI-owned loops;
//     their own schedulers drive them);
//   - platform not connected (no adapter) → skip with a ONE-TIME debug warn
//     per session;
//   - session currently running an agent turn (busy probe on the derived
//     routing key — `_running_agents` membership parity; production composes
//     RunnerBusyGuard.hasRunningTurn) → skip (stays due; the adapter FIFO
//     would race the live turn otherwise);
//   - active non-parked /goal on the session → skip (goal owns the idle
//     boundary; goals.py:goal_blocks_loop_tick seam).
//
// Injection: LoopManager.fireTick() claims the tick, then the wakeup text is
// dispatched as an internal IncomingEvent whose metadata carries the derived
// gateway_session_key. Slash-command loops dispatch through the command path
// and never hit a post-turn completion hook — completeTick("") runs
// immediately after acceptance (caps + next-tick scheduling). A throwing
// dispatcher abandons the tick (ticks_fired rolls back; stays due).

import type Database from "better-sqlite3";

import { listActiveLoopRows } from "../../pi_state/index.js";
import {
	type IsolationFlags,
	buildSessionKey,
	type SessionSource,
} from "../../pi_gateway/resolution/session-key.js";
import type { IncomingEvent } from "../../pi_gateway/guards/events.js";
import type { GatewayClock } from "./clock.js";
import { systemClock } from "./clock.js";
import { LoopManager, type LoopsConfigOf, routeFromSource } from "./manager.js";

/** Poll cadence (run.py:_loop_wakeup_watcher interval default 15.0s). */
export const LOOP_WAKEUP_SCAN_INTERVAL_MS = 15000;

/**
 * Initial delay before the FIRST tick — platforms must finish connecting
 * before wakeups inject through them (run.py `await asyncio.sleep(5)`).
 */
export const LOOP_WAKEUP_STARTUP_DELAY_MS = 5000;

/**
 * The normal-pipeline dispatch port (handoff/delegation SyntheticTurnDispatcher
 * shape — production composes GuardQuiesceDispatcher over the destination L1
 * guard so the wakeup rides BOTH guards like any turn).
 */
export interface SyntheticTurnDispatcher {
	dispatch(event: IncomingEvent): Promise<void>;
}

export interface LoopWakeupLogger {
	debug?(message: string, meta?: Record<string, unknown>): void;
	info?(message: string, meta?: Record<string, unknown>): void;
	warn?(message: string, meta?: Record<string, unknown>): void;
}

export interface LoopWatcherDeps {
	/** Open state.db handle (loop rows live in state_meta). */
	db: Database.Database;
	/**
	 * The synthetic-turn port into the NORMAL pipeline (L1-guard composition).
	 * Rejections/throws abandon the fired tick — never a false ack.
	 */
	dispatcher: SyntheticTurnDispatcher;
	/**
	 * `_running_agents` membership probe keyed by ROUTING KEY (the busy
	 * deferral). Production composes RunnerBusyGuard.hasRunningTurn; absent ⇒
	 * nothing is ever busy.
	 */
	isSessionKeyBusy?: (sessionKey: string) => boolean;
	/**
	 * goals.py:goal_blocks_loop_tick seam — True when an ACTIVE non-parked
	 * /goal owns this session's idle boundary. Pi has no goals subsystem yet,
	 * so the hook is injected; absent ⇒ goals never block.
	 */
	goalBlocksTick?: (sessionId: string) => boolean;
	/**
	 * Adapter-presence probe over the gateway's adapters map (platform value →
	 * adapter). Null/undefined ⇒ not connected ⇒ skip + one-time debug warn
	 * per session. Absent seam ⇒ every platform counts as connected.
	 */
	adapterFor?: (platform: string) => unknown;
	/** Platform isolation flags for key derivation (composition identity). */
	isolationFlags?: () => IsolationFlags | undefined;
	/** Profile namespace for session keys (default profile ⇒ agent:main). */
	profileName?: string;
	/** WhatsApp canonicalization options forwarded to buildSessionKey. */
	keyOptions?: Parameters<typeof buildSessionKey>[3];
	/** `loops:` config reader handed to each per-session manager. */
	configOf?: LoopsConfigOf;
	clock?: GatewayClock;
	intervalMs?: number;
	startupDelayMs?: number;
	log?: LoopWakeupLogger;
}

export interface LoopWakeupTickReport {
	/** Active loop rows enumerated this scan. */
	scanned: number;
	/** Wakeups claimed AND accepted by the dispatcher. */
	injected: number;
	/** Skips: mid-turn target sessions. */
	busyDeferred: number;
	/** Skips: an active /goal owns the idle boundary. */
	goalDeferred: number;
	/** Skips: CLI/TUI-owned rows without routing metadata. */
	unrouted: number;
	/** Skips: platform present in the route but not connected. */
	adapterMissing: number;
}

export class LoopWakeupWatcher {
	private readonly db: Database.Database;
	private readonly dispatcher: SyntheticTurnDispatcher;
	private readonly isSessionKeyBusy:
		| ((sessionKey: string) => boolean)
		| undefined;
	private readonly goalBlocksTick: ((sessionId: string) => boolean) | undefined;
	private readonly adapterFor: ((platform: string) => unknown) | undefined;
	private readonly isolationFlags:
		| (() => IsolationFlags | undefined)
		| undefined;
	private readonly profileName: string | undefined;
	private readonly keyOptions: Parameters<typeof buildSessionKey>[3];
	private readonly configOf: LoopsConfigOf | undefined;
	private readonly clock: GatewayClock;
	private readonly intervalMs: number;
	private readonly startupDelayMs: number;
	private readonly log: LoopWakeupLogger | undefined;

	private running = false;
	private loopPromise: Promise<void> | null = null;
	private pendingSleepResolvers: Array<() => void> = [];
	/** One-time "no adapter for platform" warnings, per session id (#parity). */
	private readonly warnedNoRoute = new Set<string>();

	constructor(deps: LoopWatcherDeps) {
		this.db = deps.db;
		this.dispatcher = deps.dispatcher;
		this.isSessionKeyBusy = deps.isSessionKeyBusy;
		this.goalBlocksTick = deps.goalBlocksTick;
		this.adapterFor = deps.adapterFor;
		this.isolationFlags = deps.isolationFlags;
		this.profileName = deps.profileName;
		this.keyOptions = deps.keyOptions;
		this.configOf = deps.configOf;
		this.clock = deps.clock ?? systemClock;
		this.intervalMs = deps.intervalMs ?? LOOP_WAKEUP_SCAN_INTERVAL_MS;
		this.startupDelayMs = deps.startupDelayMs ?? LOOP_WAKEUP_STARTUP_DELAY_MS;
		this.log = deps.log;
	}

	get isRunning(): boolean {
		return this.running;
	}

	/**
	 * ONE scan cycle at an explicit instant — never throws (best-effort inner
	 * try/except parity). Tests drive this directly instead of racing timers.
	 */
	async tick(): Promise<LoopWakeupTickReport> {
		const report: LoopWakeupTickReport = {
			scanned: 0,
			injected: 0,
			busyDeferred: 0,
			goalDeferred: 0,
			unrouted: 0,
			adapterMissing: 0,
		};
		try {
			await this.scanOnce(report);
		} catch (err) {
			// Outer containment: a broken store must not kill the watcher.
			this.log?.debug?.("loop wakeup watcher error", { error: String(err) });
		}
		return report;
	}

	/** Begin the background loop (idempotent). Startup delay precedes tick 1. */
	start(): void {
		if (this.running) return;
		this.running = true;
		this.loopPromise = this.loop();
	}

	/** Stop the loop and join it; breaks any in-flight sleep immediately. */
	async stop(): Promise<void> {
		this.running = false;
		const waiters = this.pendingSleepResolvers;
		this.pendingSleepResolvers = [];
		for (const wake of waiters) wake();
		const loop = this.loopPromise;
		this.loopPromise = null;
		if (loop) await loop;
	}

	private async loop(): Promise<void> {
		if (!(await this.sleepInterruptible(this.startupDelayMs))) return;
		while (this.running) {
			await this.tick();
			if (!this.running) break;
			if (!(await this.sleepInterruptible(this.intervalMs))) return;
		}
	}

	/** Clock sleep that stop() can break — never leaves a hung loop behind. */
	private sleepInterruptible(ms: number): Promise<boolean> {
		return new Promise<boolean>((resolvePromise) => {
			let done = false;
			const finish = (woke: boolean): void => {
				if (done) return;
				done = true;
				this.pendingSleepResolvers = this.pendingSleepResolvers.filter(
					(w) => w !== wake,
				);
				resolvePromise(woke);
			};
			const wake = (): void => finish(false); // cancelled by stop()
			this.pendingSleepResolvers.push(wake);
			void this.clock.sleepMs(ms).then(
				() => finish(true),
				() => finish(true),
			);
		});
	}

	// -- one scan ------------------------------------------------------------

	private async scanOnce(report: LoopWakeupTickReport): Promise<void> {
		const now = this.clock.nowSeconds();
		for (const [sid, state] of listActiveLoopRows(this.db)) {
			report.scanned += 1;
			if (state.awaitingResponse || now < state.nextDueAt) continue;

			// Route capture happened at creation; CLI/TUI-owned loops carry none
			// and are driven by their own surfaces.
			const route = state.route ?? {};
			const platformName = route["platform"] ?? "";
			const chatId = route["chat_id"] ?? "";
			if (!platformName || !chatId) {
				report.unrouted += 1;
				continue;
			}
			if (
				this.adapterFor !== undefined &&
				(this.adapterFor(platformName) ?? null) === null
			) {
				report.adapterMissing += 1;
				if (!this.warnedNoRoute.has(sid)) {
					this.warnedNoRoute.add(sid);
					this.log?.debug?.(
						`loop wakeup: no adapter for platform '${platformName}' (session ${sid})`,
					);
				}
				continue;
			}

			// Build the source + routing key to check business.
			const source = sourceFromRoute(route);
			let sessionKey = "";
			try {
				sessionKey = buildSessionKey(
					source,
					this.isolationFlags?.(),
					this.profileName,
					this.keyOptions,
				);
			} catch {
				continue; // derivation failed — stay due, retry next scan
			}
			if (!sessionKey) continue;
			if (
				this.isSessionKeyBusy !== undefined &&
				this.isSessionKeyBusy(sessionKey)
			) {
				report.busyDeferred += 1; // busy — stays due, next scan retries
				continue;
			}
			if (this.goalBlocksTick !== undefined && this.goalBlocksTick(sid)) {
				report.goalDeferred += 1;
				continue;
			}

			const mgr = new LoopManager({
				sessionId: sid,
				db: this.db,
				configOf: this.configOf,
				clock: this.clock,
			});
			if (!mgr.isDue(now)) continue; // fresh re-read arm (is_due re-check)
			const wakeup = await mgr.fireTick();
			if (wakeup === null) continue;
			try {
				const synthEvent = forgeLoopWakeupEvent({ wakeup, sessionKey, source });
				this.log?.info?.(
					`loop wakeup #${mgr.state ? String(mgr.state.ticksFired) : "?"}` +
						` — injecting for ${platformName} chat=${source.chatId ?? ""}` +
						` thread=${source.threadId ?? ""}`,
				);
				await this.dispatcher.dispatch(synthEvent);
				// Slash-command loops dispatch through the command path and never
				// hit the post-turn completion hook — complete the tick immediately
				// (caps + scheduling).
				if (wakeup.trimStart().startsWith("/")) {
					await mgr.completeTick("");
				}
				report.injected += 1;
			} catch (err) {
				this.log?.warn?.(
					`loop wakeup injection failed for ${sid}: ${String(err)}`,
				);
				try {
					await mgr.abandonTick();
				} catch {
					// containment parity: rollback failure never kills the scan
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Route ⇄ source helpers
// ---------------------------------------------------------------------------

/**
 * Rebuild the arrival snapshot from a persisted route dict
 * (run.py evt_stub + _build_process_event_source parity): the same five route
 * dimensions captured by manager.routeFromSource feed back into a SessionSource
 * the ingress can key on.
 */
export function sourceFromRoute(route: Record<string, string>): SessionSource {
	return {
		platform: route["platform"] ?? "",
		chatType: route["chat_type"] ?? "",
		...(route["chat_id"] ? { chatId: route["chat_id"] } : {}),
		...(route["thread_id"] ? { threadId: route["thread_id"] } : {}),
		...(route["user_id"] ? { userId: route["user_id"] } : {}),
	};
}

/**
 * Forge the synthetic internal MessageEvent that carries the wakeup through
 * the NORMAL pipeline (MessageEvent(text=wakeup, internal=True) +
 * DEC-022 push-lane metadata: gateway_session_key is THE routing key the L1
 * guard resolves ingress against).
 */
export function forgeLoopWakeupEvent(input: {
	wakeup: string;
	sessionKey: string;
	source: SessionSource;
}): IncomingEvent {
	return {
		messageType: "text",
		text: input.wakeup,
		internal: true,
		source: input.source,
		metadata: { gateway_session_key: input.sessionKey },
	};
}

/** Re-exported for compositions building routes at command time. */
export { routeFromSource };
