// pi_gateway/lifecycle/restore-gate.ts — inbound-dispatch gate serializing
// boot restore against live traffic (run.py: GatewayRunner.start —
// `_startup_restore_in_progress` / `_startup_restore_queue`: platform
// adapters can begin receiving messages before restart-interrupted sessions
// are auto-resumed, so inbound work is QUEUED until the resume pass ran and
// every synthetic boot turn finished).
//
// Spec: /root/pi-gateway/08-operations.md §1.1 (boot restore ordering);
// 01-architecture.md §3.1. Structural seam only: the streaming/dispatch layer
// awaits `whenOpen()` (or consults `closed`) before accepting a turn and hands
// early arrivals back via the registered consumer when the gate opens.

export interface RestoreGate {
	/** True while boot restore is in flight — new turns must queue. */
	readonly closed: boolean;
	/** Begin gating (idempotent; nesting counted). */
	begin(): void;
	/** Reopen and flush queued items to the consumer IN ARRIVAL ORDER. */
	finish(): Promise<void>;
	/** Park an inbound item captured while the gate was closed. */
	enqueueInbound(item: unknown): void;
	queuedCount(): number;
	/** Register where queued items go when the gate opens. */
	setConsumer(consumer: (item: unknown) => void | Promise<void>): void;
	/** Resolves once the gate is open (immediately when never closed). */
	whenOpen(): Promise<void>;
}

export function createRestoreGate(): RestoreGate {
	let depth = 0;
	const queued: unknown[] = [];
	let consumer: ((item: unknown) => void | Promise<void>) | null = null;
	const openWaiters: Array<() => void> = [];

	function open(): void {
		for (const waiter of openWaiters.splice(0)) waiter();
	}

	return {
		get closed(): boolean {
			return depth > 0;
		},
		begin(): void {
			depth++;
		},
		async finish(): Promise<void> {
			depth = Math.max(0, depth - 1);
			if (depth > 0) return; // nested begin — still gated
			open();
			const pending = queued.splice(0);
			if (consumer === null) return;
			for (const item of pending) await consumer(item);
		},
		enqueueInbound(item: unknown): void {
			queued.push(item);
		},
		queuedCount(): number {
			return queued.length;
		},
		setConsumer(fn: (item: unknown) => void | Promise<void>): void {
			consumer = fn;
		},
		whenOpen(): Promise<void> {
			if (depth === 0) return Promise.resolve();
			return new Promise<void>((resolve) => {
				openWaiters.push(resolve);
			});
		},
	};
}
