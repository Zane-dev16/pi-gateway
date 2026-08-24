// clock.ts — injected time seam for the exec-approval bridge (07 §8).
//
// HARD RULE: no queue/gate/delivery logic ever reads a wall clock directly.
// Every time observation flows through this interface so behavior contracts
// (300 s approval deadline, ≤1 s wait slices, ~10 s activity heartbeats,
// 15 s send classification, human-wait clamps) run deterministically under
// injected clocks. `systemClock` is the ONLY place Date.now appears.
//
// Shape-identical mirror of pi_embedded/handoff/clock.ts,
// pi_embedded/cron/clock.ts and pi_gateway/delegation/clock.ts (same
// GatewayClock structure so a future shared seam is a type-compatible
// drop-in).

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
