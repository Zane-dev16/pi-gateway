// TEST INFRASTRUCTURE — manual clock for kanban dispatcher behavior
// contracts. No wall-clock reads anywhere: time moves only when a test
// advances it (required for the stale-claim reclaim boundary and breaker
// timing assertions). Mirrors pi_embedded/handoff/testing/manual-clock.ts.

import type { GatewayClock } from "../clock.js";

export class ManualClock implements GatewayClock {
	private currentSeconds: number;

	constructor(startSeconds = 1_000_000) {
		this.currentSeconds = startSeconds;
	}

	nowSeconds(): number {
		return this.currentSeconds;
	}

	async sleepMs(ms: number): Promise<void> {
		await new Promise<void>((resolvePromise) => {
			setTimeout(resolvePromise, ms);
		});
	}

	advance(seconds: number): void {
		this.currentSeconds += seconds;
	}

	set(seconds: number): void {
		this.currentSeconds = seconds;
	}
}
