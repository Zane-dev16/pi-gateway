// clock.ts — injected time seam for the async-delegation durability rail.
//
// HARD RULE: no rail logic ever reads a wall clock directly. Every time
// observation flows through this interface so behavior contracts can drive
// time deterministically (300 s stale-claim takeover, 48 h replay-age cap,
// 7 d durable retention). `systemClock` is the ONLY place Date.now appears.
// Mirror of pi_gateway/obligations/clock.ts (same GatewayClock shape so a
// future shared seam is a type-compatible drop-in).

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
