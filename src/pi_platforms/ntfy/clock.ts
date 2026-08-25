// pi_platforms/ntfy/clock — structural clock seam shared with the IRC/EMAIL
// families. Ntfy stream/reconnect timing is driven EXPLICITLY by fixtures, so
// only the nowMs/sleepMs/advance shape is required.

export interface RecordedSleep {
	ms: number;
	atMs: number;
}

/** Structural clock seam (ManualClock satisfies this shape). */
export interface PacingClockLike {
	nowMs(): number;
	sleepMs(ms: number): Promise<void>;
	advance(ms: number): Promise<void>;
}

function yieldMacrotask(): Promise<void> {
	return new Promise((resolve) => setImmediate(resolve));
}

export class AutoAdvanceClock implements PacingClockLike {
	private currentMs = 0;

	readonly sleepLog: RecordedSleep[] = [];

	nowMs: () => number = () => this.currentMs;

	sleepMs: (ms: number) => Promise<void> = (ms: number) => {
		this.sleepLog.push({ ms, atMs: this.currentMs });
		this.currentMs += Math.max(0, ms);
		return Promise.resolve();
	};

	get pendingWaits(): number {
		return 0;
	}

	async advance(ms: number): Promise<void> {
		const target = this.currentMs + Math.max(0, ms);
		this.currentMs = target;
		await yieldMacrotask();
	}
}
