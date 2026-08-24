// clock.ts — injected time seam for the handoff watcher (DEC-008).
//
// HARD RULE: no watcher/queue/binder logic ever reads a wall clock directly.
// Every time observation flows through this interface so behavior contracts
// can drive time deterministically (2s poll cadence, 5s startup delay, 60s
// CLI poll-block deadline). `systemClock` is the ONLY place Date.now appears.
//
// Shape-identical mirror of pi_gateway/delegation/clock.ts and
// pi_gateway/obligations/clock.ts (same GatewayClock structure so a future
// shared seam is a type-compatible drop-in).

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
			const t = setTimeout(resolvePromise, ms);
			t.unref?.();
		}),
};
