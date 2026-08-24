// pi_platforms/polling/clock — injected time for the polling engine.
//
// Every timing-sensitive obligation (04 §3.1: conflict RETRY_DELAY ladder,
// HEARTBEAT_INTERVAL probes, PROBE_TIMEOUT races, FloodWait retry_after
// backoff) resolves through THIS seam so behavior contracts never sleep on
// wall clocks (phase rules: injected clocks for timing).

/** Monotonic ms clock + sleep seam the engine consumes. */
export interface PollingClock {
	nowMs(): number;
	/** Backoff sleep (conflict ladder, FloodWait honor). */
	sleep(ms: number): Promise<void>;
}

/** Timer seam (heartbeat cadence, probe timeouts). Returns a canceller. */
export type TimerSeam = (delayMs: number, fn: () => void) => () => void;

/** Production clock: real monotonic time, real sleeps/timers. */
export function realPollingClock(): PollingClock & { timer: TimerSeam } {
	const timer: TimerSeam = (delayMs, fn) => {
		const t = setTimeout(fn, delayMs);
		t.unref?.();
		return () => clearTimeout(t);
	};
	return {
		nowMs: () => Date.now(),
		sleep: (ms) =>
			new Promise<void>((resolve) => {
				timer(ms, resolve);
			}),
		timer,
	};
}

/**
 * Manual clock for behavior contracts: sleeps RECORD their durations and
 * advance virtual time instantly; timers fire on explicit advance(). A few
 * escape hatches exist for genuinely asynchronous simulation (long-poll
 * waits ride the fake server, never this clock).
 */
export class ManualPollingClock implements PollingClock {
	nowVal = 0;
	/** Durations of every sleep() call, in order — the backoff audit trail. */
	readonly sleeps: number[] = [];
	private timers: Array<{ at: number; fn: () => void; cancelled: boolean }> =
		[];

	readonly nowMs = (): number => this.nowVal;

	readonly sleep = (ms: number): Promise<void> => {
		this.sleeps.push(ms);
		this.nowVal += ms;
		return Promise.resolve();
	};

	/**
	 * REAL-wall sleep for simulations that must yield to concurrent macrotasks
	 * (wedged-consumer modeling); deliberately NOT recorded as engine backoff.
	 */
	wallSleep(ms: number): Promise<void> {
		return new Promise<void>((resolve) => {
			const t = setTimeout(resolve, ms);
			t.unref?.();
		});
	}

	readonly timer: TimerSeam = (delayMs, fn) => {
		const entry = {
			at: this.nowVal + delayMs,
			fn,
			cancelled: false,
		};
		this.timers.push(entry);
		return () => {
			entry.cancelled = true;
		};
	};

	get pendingTimerCount(): number {
		return this.timers.filter((t) => !t.cancelled).length;
	}

	/**
	 * Advance virtual time PROGRESSIVELY: each due timer fires at its own
	 * timestamp (earliest-first), so timers chained during a callback land at
	 * their correct later positions within the same advance window.
	 */
	async advance(ms: number): Promise<void> {
		const target = this.nowVal + ms;
		for (;;) {
			let due: { at: number; fn: () => void; cancelled: boolean } | undefined;
			for (const t of this.timers) {
				if (t.cancelled || t.at > target) continue;
				if (due === undefined || t.at < due.at) due = t;
			}
			if (due === undefined) break;
			this.nowVal = Math.max(this.nowVal, due.at);
			due.cancelled = true;
			due.fn();
			// Let async callbacks run their continuations (chained reschedules
			// register at nowVal + delay, i.e. their true virtual position).
			await Promise.resolve();
			await new Promise<void>((r) => setTimeout(r, 0));
		}
		this.nowVal = target;
	}
}
