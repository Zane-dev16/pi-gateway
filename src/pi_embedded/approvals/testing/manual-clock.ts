// TEST INFRASTRUCTURE — deterministic clock for exec-approval timing
// contracts (300 s deadline, ≤1 s wait slices, ~10 s heartbeats, 15 s send
// classification, human-wait clamps). Virtual time only: no test sleeps a
// real wall interval to observe timing.
//
// Semantics:
//   - nowSeconds() reads the logical clock (ms/1000).
//   - sleepMs(ms) ADVANCES the logical clock by ms, fires every action due
//     within that window in due order (awaiting each), then yields a
//     macrotask so concurrently-settling promise callbacks run before the
//     sleeper resumes — poll loops walk to their deadlines deterministically
//     and resolvers interleave at slice boundaries.
//   - at(inMs, fn) schedules fn to RUN when virtual time reaches its due
//     point ("the human answers at T=5s" half of the protocol).
//   - advance(ms) moves time without sleeping (fires due actions).

import type { GatewayClock } from "../clock.js";

interface ClockAction {
	dueMs: number;
	fn: () => void | Promise<void>;
}

export class ManualClock implements GatewayClock {
	private currentMs: number;
	private readonly actions = new Set<ClockAction>();
	private firedActions = 0;

	constructor(startMs = 1_000_000) {
		this.currentMs = startMs;
	}

	nowSeconds(): number {
		return this.currentMs / 1000;
	}

	get nowMs(): number {
		return this.currentMs;
	}

	async sleepMs(ms: number): Promise<void> {
		this.advance(ms);
		await new Promise<void>((resolvePromise) => {
			setTimeout(resolvePromise, 0);
		});
	}

	/** Schedule an action at now+inMs (virtual); runs when time passes it. */
	at(inMs: number, fn: () => void | Promise<void>): void {
		this.actions.add({ dueMs: this.currentMs + inMs, fn });
	}

	/** Move logical time forward, firing every action due within the window. */
	advance(ms: number): void {
		const horizon = this.currentMs + ms;
		for (;;) {
			let next: ClockAction | undefined;
			for (const action of this.actions) {
				if (
					action.dueMs <= horizon &&
					(next === undefined || action.dueMs < next.dueMs)
				) {
					next = action;
				}
			}
			if (!next) break;
			this.actions.delete(next);
			this.currentMs = Math.max(this.currentMs, next.dueMs);
			this.firedActions++;
			next.fn();
		}
		this.currentMs = horizon;
	}

	get scheduledActionCount(): number {
		return this.firedActions;
	}

	/** Drop all pending actions without running them. */
	cancelAll(): void {
		this.actions.clear();
	}
}

/** Deferred promise gate for scripted resolvers. */
export class Gate {
	private resolveFn: (() => void) | null = null;
	private readonly promiseValue: Promise<void>;

	constructor() {
		this.promiseValue = new Promise<void>((resolvePromise) => {
			this.resolveFn = resolvePromise;
		});
	}

	open(): void {
		this.resolveFn?.();
	}

	get wait(): Promise<void> {
		return this.promiseValue;
	}
}
