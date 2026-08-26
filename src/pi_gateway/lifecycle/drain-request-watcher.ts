// pi_gateway/lifecycle/drain-request-watcher.ts — external begin/cancel-drain
// watcher stage. Hermes anchor (READ-ONLY reference; semantics ported, no
// code vendored — gateway/drain_control.py + run.py drain watcher): the
// dashboard has NO control channel into a running gateway; it writes (or
// removes) `.drain_request.json` and this watcher reacts:
//   - presence stamped with the CURRENT instantiation epoch ⇒ external drain
//     active: flip gateway_state → "draining" and stop accepting new turns;
//   - absence / prior-epoch / expired (>3600s) marker ⇒ not draining (revert
//     to "running" if we had flipped it);
//   - malformed/present-but-contentless markers STILL count as active —
//     fail-safe toward quiescing (a corrupt begin marker must not be ignored);
//   - reading never raises.
//
// DEC-040 seam: exposed as an embedded_watchers ServiceEntry. The flip only
// touches the persisted status + callbacks; turn-admission policy consumes
// `externallyDraining()` / onDrainRequested in the dispatch layer.

import { writeRuntimeStatus } from "./status-stamp.js";
import { clearDrainRequest, drainRequested } from "./markers.js";
import type { Logger } from "./shutdown.js";
import type { ServiceStartOutcome } from "./lifecycle.js";
import { realTimerPort, type TimerPort } from "./watchdog.js";
import type { StageContext } from "./lifecycle.js";

export const DRAIN_REQUEST_POLL_INTERVAL_MS = 1_000;

export interface DrainRequestWatcherOptions {
	/** Poll cadence (Hermes re-reads every second). */
	intervalMs?: number;
	/** Epoch override (tests). Default currentInstantiationEpoch(). */
	epoch?: string;
	nowMs?: () => number;
	timer?: TimerPort;
	logger?: Logger;
	/** Fired on false→true transition (stop accepting turns). */
	onDrainRequested?: () => void;
	/** Fired on true→false transition (turn admission may resume). */
	onDrainReleased?: () => void;
}

export interface DrainRequestWatcherEntry {
	name: "gateway.drain-request-watcher";
	isExternallyDraining(): boolean;
	stop(): void;
}

/**
 * Build the stage-8 entry for the external drain-request marker. The ctx's
 * home + pid drive the gateway_state flips; the returned probe exposes the
 * live flag between polls.
 */
export function createDrainRequestWatcherService(
	options: DrainRequestWatcherOptions = {},
): import("./lifecycle.js").ServiceEntry & {
	watcher(): DrainRequestWatcherEntry | null;
} {
	let draining = false;
	let stopped = false;
	const timer = options.timer ?? realTimerPort();
	const intervalMs = Math.max(
		100,
		options.intervalMs ?? DRAIN_REQUEST_POLL_INTERVAL_MS,
	);
	let handle: unknown = null;
	let stopFn: (() => void) | null = null;

	const entry = {
		name: "gateway.drain-request-watcher",
		start(ctx: StageContext): ServiceStartOutcome {
			const log = options.logger ?? ctx.log;

			function poll(): void {
				if (stopped) return;
				try {
					const requested = drainRequested(ctx.home, {
						...(options.epoch !== undefined ? { epoch: options.epoch } : {}),
						...(options.nowMs !== undefined ? { nowMs: options.nowMs } : {}),
					});
					if (requested && !draining) {
						draining = true;
						log.info("external drain request observed — flipping to draining", {
							marker: ".drain_request.json",
						});
						writeRuntimeStatus(
							ctx.home,
							{ gateway_state: "draining" },
							{ pid: ctx.selfPid, home: ctx.home },
						);
						options.onDrainRequested?.();
					} else if (!requested && draining) {
						draining = false;
						log.info("external drain released — back to running");
						writeRuntimeStatus(
							ctx.home,
							{ gateway_state: "running" },
							{ pid: ctx.selfPid, home: ctx.home },
						);
						options.onDrainReleased?.();
					}
				} catch (err) {
					log.warn("drain-request poll failed", { error: String(err) });
				}
			}

			handle = timer.setTimeout(function tick() {
				poll();
				if (!stopped) handle = timer.setTimeout(tick, intervalMs);
			}, intervalMs);

			const serviceStop = (): void => {
				stopped = true;
				if (handle !== null) timer.clearTimeout(handle);
			};
			stopFn = serviceStop;
			const outcome: ServiceStartOutcome = {
				ok: true,
				handle: { name: entry.name, stop: async () => serviceStop() },
			};
			return outcome;
		},
	};

	return {
		...entry,
		watcher(): DrainRequestWatcherEntry | null {
			if (entry === null) return null;
			return {
				name: "gateway.drain-request-watcher",
				isExternallyDraining(): boolean {
					return draining;
				},
				stop(): void {
					stopFn?.();
				},
			};
		},
	};
}

export { clearDrainRequest };
