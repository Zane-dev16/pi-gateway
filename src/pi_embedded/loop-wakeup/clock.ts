// clock.ts — injected time seam for the loop-wakeup subsystem.
//
// HARD RULE (same contract as ../delegation-watcher/clock.ts): no manager or
// watcher logic ever reads a wall clock directly. Every time observation flows
// through this interface so behavior contracts drive tick cadence, deferral
// windows and self-paced backoff deterministically (injected clocks are
// mandatory for timing assertions). `systemClock` is the ONLY place Date.now
// appears. Same GatewayClock shape as the sibling watchers so composition
// shares ONE instance across rail + delegation + loop wakeups.

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
