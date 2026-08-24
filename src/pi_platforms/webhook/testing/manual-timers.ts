// pi_platforms/webhook/testing/manual-timers — deterministic time for the
// webhook contracts. No wall-clock reads in any timing path under test:
// windows fire when the test advances the clock, sleeps resolve when released.

/** Monotonic-ms clock + epoch-seconds view advanced by hand.
 *
 * All accessors are ARROW-BOUND PROPERTIES: these get passed as bare
 * callbacks (`nowMs: timers.nowMs`) into seams that invoke them detached —
 * an unbound method would read `this` against the CALLER and return
 * undefined silently.
 */
export class ManualTimers {
	private currentMs = 1_700_000_000_000;
	private seq = 0;
	readonly pending = new Map<number, () => void>();
	/** Sleeps awaited by production code; each entry releases via releaseOneSleep. */
	readonly sleepWaiters: Array<{ ms: number; release: () => void }> = [];

	readonly nowMs = (): number => this.currentMs;

	readonly nowSeconds = (): number => Math.floor(this.currentMs / 1000);

	advance(ms: number): void {
		this.currentMs += ms;
		const due = [...this.pending.values()];
		for (const fn of due) {
			fn();
		}
		this.pending.clear();
	}

	setSeconds(seconds: number): void {
		this.currentMs = seconds * 1000;
	}

	/** TimerSeam-compatible delay: fires on advance(). */
	readonly delay = (ms: number): { done: Promise<"fired">; cancel(): void } => {
		void ms; // manual clock: firing is test-driven, not wall-driven
		let fired = false;
		let resolveFn: ((v: "fired") => void) | null = null;
		const id = ++this.seq;
		const done = new Promise<"fired">((resolve) => {
			resolveFn = resolve;
		});
		this.pending.set(id, () => {
			if (fired) return;
			fired = true;
			resolveFn?.("fired");
		});
		return {
			done,
			cancel: () => {
				this.pending.delete(id);
				if (!fired) {
					fired = true;
					resolveFn?.("fired"); // cancelled timers race-resolve as no-ops
				}
			},
		};
	};

	/** Sleep seam: registers a waiter the test must release explicitly. */
	readonly sleep = (ms: number): Promise<void> => {
		return new Promise<void>((resolve) => {
			this.sleepWaiters.push({
				ms,
				release: () => resolve(),
			});
		});
	};

	/** Release the oldest pending sleep (backoff ladders drain one rung per
	 * call — tests assert waiter order). */
	releaseOneSleep(): boolean {
		const next = this.sleepWaiters.shift();
		if (next === undefined) return false;
		next.release();
		return true;
	}
}
