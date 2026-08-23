// clock.ts — the injected time seam for the delivery-obligations ledger.
//
// HARD RULE: no ledger/scheduler logic ever reads a wall clock directly.
// Every time observation flows through this interface so behavior contracts
// can drive time deterministically (24h stale window, 7d retention, backoff
// schedule). `systemClock` is the ONLY place Date.now()/setTimeout appear;
// tests inject a manual implementation instead.

export interface GatewayClock {
	/** Wall-clock seconds since the epoch (parity of Python time.time()). */
	nowSeconds(): number;
	/** Await this many milliseconds (parity of asyncio.sleep granularity). */
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
