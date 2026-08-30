// pi_embedded/cron/clock.ts — the injected time seam for the cron ticker.
//
// HARD RULE (roadmap Phase-5 top risk): cron bounds timing assertions use
// INJECTED CLOCKS; wall-clock assumptions are banned. No scheduler/store/
// monitor logic ever reads Date.now()/setTimeout directly — every time
// observation flows through this interface (same discipline as
// pi_gateway/obligations/clock.ts, mirrored structurally because embedded
// services keep their own leaf seams).
//
// Hermes anchors: cron/scheduler.py uses time.monotonic()/hermes_time.now();
// cron/jobs.py:_hermes_now. Pi collapses both onto epoch seconds — all cron
// bounds (grace windows, fire-claim TTLs) compare epoch seconds, never wall
// deltas. (DEC-070: the inactivity bound and its limits were removed.)

export interface CronClock {
	/** Wall-clock seconds since the epoch (parity of hermes_time.now()). */
	nowSeconds(): number;
	/** Await approximately this many milliseconds of REAL time. Logic must
	 * stay correct at ANY granularity — background loops sleep through this,
	 * behavior contracts drive tick()/monitor steps explicitly instead. */
	sleepMs(ms: number): Promise<void>;
}

/** Production clock. The single wall-clock boundary of this module. */
export const systemCronClock: CronClock = {
	nowSeconds: () => Date.now() / 1000,
	sleepMs: (ms) =>
		new Promise<void>((resolvePromise) => {
			setTimeout(resolvePromise, ms);
		}),
};
