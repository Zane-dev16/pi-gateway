// clock.ts — injected time seam for the async-delegation watcher.
//
// HARD RULE: no watcher logic ever reads a wall clock directly. Every time
// observation flows through this interface so behavior contracts can drive
// the 3 s startup delay, the 2 s poll cadence, claim-staleness takeover and
// idle-end retarget timing deterministically (injected clocks are mandatory
// for timing assertions). `systemClock` is the ONLY place Date.now appears.
// Same GatewayClock shape as pi_gateway/delegation/clock.ts so composition
// shares ONE instance across rail + watcher.

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
