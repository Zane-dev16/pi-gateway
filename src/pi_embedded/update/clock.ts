// clock.ts — injected time seam for the update pipeline.
//
// HARD RULE (repo-wide convention, mirrors pi_embedded/{cron,handoff}/clock.ts):
// no update-stage logic ever reads a wall clock or sleeps directly. Every time
// observation flows through this interface so behavior contracts can drive
// time deterministically — the verify stage's settled window (~2s, 08 §8) and
// the restart stage's drain waits are both clock-driven. `systemClock` is the
// ONLY place Date.now/setTimeout appear.
//
// Shape-identical mirror of the other GatewayClock seams so a future shared
// seam is a type-compatible drop-in.

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
