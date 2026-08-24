// TEST INFRASTRUCTURE — deterministic clock for handoff timing contracts
// (2s poll cadence, 5s startup delay, 60s CLI poll-block deadline). Virtual
// time only: no test ever sleeps a real wall interval to observe timing.
//
// Semantics:
//   - sleepMs(ms) registers a wake at now+ms and returns immediately-pending.
//   - at(inMs, fn) schedules fn to RUN (and be awaited) when virtual time
//     reaches its due point — the "gateway side acts at time T" half of a
//     protocol where the CLI side is poll-blocking.
//   - advance(ms) fires every due item within the horizon in due order,
//     yielding between items so woken continuations can register follow-up
//     sleeps (watcher loop cadence) before the next fire.
//   - Wakes beyond the horizon stay pending; cancelAll() drops them.

import type { GatewayClock } from "../clock.js";

interface ClockItem {
	dueMs: number;
	resolveSleep?: () => void;
	run?: () => void | Promise<void>;
}

export class ManualClock implements GatewayClock {
	private currentMs = 0;
	private readonly items = new Set<ClockItem>();
	private sleepCounter = 0;

	nowSeconds(): number {
		return this.currentMs / 1000;
	}

	sleepMs(ms: number): Promise<void> {
		this.sleepCounter++;
		return new Promise<void>((resolve) => {
			this.items.add({ dueMs: this.currentMs + ms, resolveSleep: resolve });
		});
	}

	/** Schedule an action at now+inMs (virtual); awaited during advance(). */
	at(inMs: number, fn: () => void | Promise<void>): void {
		this.items.add({ dueMs: this.currentMs + inMs, run: fn });
	}

	/** Fire every item due within the next `ms` virtual milliseconds. */
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
			if (next.resolveSleep !== undefined) {
				next.resolveSleep();
			} else if (next.run !== undefined) {
				await next.run();
			}
			// Yield so woken continuations run and can register follow-ups.
			await new Promise<void>((r) => setTimeout(r, 0));
		}
		this.currentMs = horizon;
	}

	/** Drop all pending wakes/timers without resolving them. */
	cancelAll(): void {
		this.items.clear();
	}

	get pendingSleeps(): number {
		return this.items.size;
	}

	get totalSleepRequests(): number {
		return this.sleepCounter;
	}
}

/**
 * Advance the clock in fixed steps forever-until-cap, so poll loops' sleeps
 * keep firing while a protocol plays out. All virtual: 130 steps × 500ms =
 * 65 virtual seconds cost microseconds of wall time.
 */
export async function pumpClock(
	clock: ManualClock,
	totalSteps: number,
	stepMs: number,
): Promise<void> {
	for (let i = 0; i < totalSteps; i++) {
		await clock.advance(stepMs);
	}
}
