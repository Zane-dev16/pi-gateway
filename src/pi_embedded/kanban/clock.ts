// clock.ts — injected time seam for the kanban dispatcher.
//
// HARD RULE: no dispatcher/store logic ever reads a wall clock directly.
// Every time observation flows through this interface so behavior contracts
// can drive claim TTLs, stale-claim reclaim boundaries, and breaker timing
// deterministically (07 §6 tick shape; required contract test "stale-card
// reclaim boundary (injected clock)"). `systemClock` is the ONLY place
// Date.now appears. Mirror of pi_gateway/delegation/clock.ts and
// pi_gateway/obligations/clock.ts (same GatewayClock shape so the seams are
// type-compatible drop-ins).

export interface GatewayClock {
	/** Wall-clock seconds since the epoch (parity of Python time.time()). */
	nowSeconds(): number;
	/** Await this many milliseconds. */
	sleepMs(ms: number): Promise<void>;
}

/** Production clock. The single wall-clock boundary of this module. */
export const systemClock: GatewayClock = {
	nowSeconds: () => Date.now() / 1000,
	sleepMs: (ms) =>
		new Promise<void>((resolvePromise) => {
			setTimeout(resolvePromise, ms);
		}),
};
