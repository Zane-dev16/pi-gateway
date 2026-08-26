// TEST INFRASTRUCTURE — deterministic virtual clock for loop-wakeup timing
// contracts (5s startup delay, 15s scan cadence, tick due times, self-paced
// backoff). No test ever sleeps a real wall interval.
//
// Same semantics as pi_embedded/delegation-watcher/testing/manual-clock.ts:
//   - sleepMs(ms) registers a wake at now+ms and returns immediately-pending;
//   - advance(ms) fires every due item within the horizon in due order,
//     yielding between items so woken continuations can register follow-up
//     sleeps (loop cadence) before the next fire.

import type { GatewayClock } from "../clock.js";

interface ClockItem {
	dueMs: number;
	resolveSleep?: () => void;
}

export class ManualClock implements GatewayClock {
	private currentMs = 0;
	private readonly items = new Set<ClockItem>();

	nowSeconds(): number {
		return this.currentMs / 1000;
	}

	sleepMs(ms: number): Promise<void> {
		return new Promise<void>((resolve) => {
			this.items.add({ dueMs: this.currentMs + ms, resolveSleep: resolve });
		});
	}

	/** Fire every sleep due within the next `ms` virtual milliseconds. */
	async advance(ms: number): Promise<void> {
		const horizon = this.currentMs + ms;
		for (;;) {
			let next: ClockItem | undefined;
			for (const item of this.items) {
				if (
					item.dueMs <= horizon &&
					(next === undefined || item.dueMs < next.dueMs)
				) {
					next = item;
				}
			}
			if (next === undefined) break;
			this.items.delete(next);
			this.currentMs = Math.max(this.currentMs, next.dueMs);
			if (next.resolveSleep !== undefined) next.resolveSleep();
			// Yield so woken continuations run and can register follow-ups.
			await new Promise<void>((r) => setTimeout(r, 0));
		}
		this.currentMs = horizon;
	}

	setSeconds(seconds: number): void {
		this.currentMs = seconds * 1000;
	}
}
