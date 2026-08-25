// pi_platforms/irc/clock — the IRC family's deterministic clock (workspace
// rule: timing behavior is proven against an INJECTED clock — never wall
// sleeps). AutoAdvanceClock keeps the ManualClock SHAPE (nowMs/sleepMs/
// advance) while letting UNADVANCED suites — the shared conformance rows —
// pass through paced sends without deadlocking: every sleep resolves
// immediately and is RECORDED as virtual elapsed time, so rate-pacing
// contracts still assert exact gaps against the log. Scenario fixtures that
// need parked-time control use plain ManualClock instead.

export interface RecordedSleep {
	/** Requested duration (ms). */
	ms: number;
	/** Virtual clock value when the sleep began. */
	atMs: number;
}

/** Structural clock seam shared by ManualClock and AutoAdvanceClock. */
export interface PacingClock {
	nowMs(): number;
	sleepMs(ms: number): Promise<void>;
	advance(ms: number): Promise<void>;
}

function yieldMacrotask(): Promise<void> {
	return new Promise((resolve) => setImmediate(resolve));
}

export class AutoAdvanceClock implements PacingClock {
	private currentMs = 0;
	private waiters: Array<{ at: number; resolve: () => void }> = [];

	readonly sleepLog: RecordedSleep[] = [];

	nowMs: () => number = () => this.currentMs;

	sleepMs: (ms: number) => Promise<void> = (ms: number) => {
		this.sleepLog.push({ ms, atMs: this.currentMs });
		this.currentMs += Math.max(0, ms);
		return Promise.resolve();
	};

	/** Total virtual time consumed by sleeps so far. */
	get sleptMsTotal(): number {
		return this.sleepLog.reduce((acc, s) => acc + s.ms, 0);
	}

	get pendingWaits(): number {
		return this.waiters.length;
	}

	/**
	 * Move the clock forward, resolving every waiter whose deadline falls
	 * within the advance in deadline order (ManualClock semantics).
	 */
	async advance(ms: number): Promise<void> {
		const target = this.currentMs + Math.max(0, ms);
		for (;;) {
			if (this.waiters.length === 0) break;
			this.waiters.sort((a, b) => a.at - b.at);
			const next = this.waiters[0];
			if (!next || next.at > target) break;
			this.waiters.shift();
			this.currentMs = Math.max(this.currentMs, next.at);
			next.resolve();
			await yieldMacrotask();
		}
		this.currentMs = target;
	}
}
