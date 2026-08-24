// TEST INFRASTRUCTURE — manual clock for update behavior contracts.
// No wall-clock reads anywhere in stage logic: logical time moves only when a
// test advances it or code sleeps (sleepMs advances by the slept amount and
// yields a macrotask so concurrent promise callbacks settle deterministically).

import type { GatewayClock } from "../clock.js";

export class ManualClock implements GatewayClock {
	private currentSeconds: number;
	readonly sleeps: number[] = [];

	constructor(startSeconds = 1_000_000) {
		this.currentSeconds = startSeconds;
	}

	nowSeconds(): number {
		return this.currentSeconds;
	}

	async sleepMs(ms: number): Promise<void> {
		this.sleeps.push(ms);
		this.currentSeconds += ms / 1000;
		await new Promise<void>((resolvePromise) => {
			setTimeout(resolvePromise, 0);
		});
	}
}
