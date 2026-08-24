// pi_platforms/persistent-ws/manual-clock — injected deterministic clock for
// the persistent-WS transport (watchdog staleness, reconnect ladder sleeps).
//
// Test discipline (workspace hard rules): timing behavior is proven against an
// INJECTED clock — never wall-clock sleeps. `advance()` walks due waiters in
// order and yields the macrotask queue between them so woken continuations
// (close → ladder sleep → resubscribe) make real progress deterministically.

export type NowFn = () => number;
export type SleepFn = (ms: number) => Promise<void>;

function yieldMacrotask(): Promise<void> {
	return new Promise((resolve) => setImmediate(resolve));
}

export class ManualClock {
	private currentMs = 0;
	private waiters: Array<{ at: number; resolve: () => void }> = [];

	nowMs: NowFn = () => this.currentMs;

	/** Deterministic sleep — registers a waiter at now+ms. */
	sleepMs: SleepFn = (ms: number) =>
		new Promise<void>((resolve) => {
			this.waiters.push({ at: this.currentMs + Math.max(0, ms), resolve });
		});

	/** Number of pending timed waits (observability for "no stray timers"). */
	get pendingWaits(): number {
		return this.waiters.length;
	}

	/**
	 * Move the clock forward, resolving every waiter whose deadline falls
	 * within the advance in deadline order. Between resolutions the loop
	 * yields so each woken continuation runs to its NEXT await point before
	 * the following waiter fires (deterministic interleaving without wall
	 * time).
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
