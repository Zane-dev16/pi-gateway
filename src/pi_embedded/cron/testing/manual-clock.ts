// TEST INFRASTRUCTURE — manual clock for cron behavior contracts.
// No wall-clock reads anywhere: logical time moves only when a test advances
// it (advance/set) or when code sleeps (sleepMs advances by the slept amount,
// then yields a macrotask so pending promise callbacks settle deterministically).

import type { CronClock } from "../clock.js";

export class ManualClock implements CronClock {
	private currentSeconds: number;

	constructor(startSeconds = 1_000_000) {
		this.currentSeconds = startSeconds;
	}

	nowSeconds(): number {
		return this.currentSeconds;
	}

	async sleepMs(ms: number): Promise<void> {
		// Logical sleep: time advances BY the slept amount; a macrotask yield
		// lets concurrently-settling promises run before the sleeper resumes.
		this.currentSeconds += ms / 1000;
		await new Promise<void>((resolvePromise) => {
			setTimeout(resolvePromise, 0);
		});
	}

	advance(seconds: number): void {
		this.currentSeconds += seconds;
	}

	set(seconds: number): void {
		this.currentSeconds = seconds;
	}
}

/** Deferred promise gate for scripted executors. */
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
