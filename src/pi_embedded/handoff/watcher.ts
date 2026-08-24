// watcher.ts — the poll-and-claim loop over CLI-written handoff rows.
// Port of gateway/run.py:_handoff_watcher:
//
//   - initial STARTUP DELAY (5s) so platforms are fully connected before the
//     first dispatch attempt;
//   - poll every 2s: list pending rows (oldest first);
//   - per row: ATOMIC claim (pending → running) — a loser tick or a second
//     gateway skips harmlessly;
//   - process via the DEC-008 pipeline; success ⇒ completed, throw ⇒
//     failed(+str(error)) — synchronous error visibility preserved;
//   - the whole tick body is failure-contained: a bad row or a broken store
//     NEVER crashes the loop (per-iteration inner try/except parity).
//
// Supervision context: in production this watcher is spawned as an
// OPTIONAL-stage service (01 §3.1 stage 8 `embedded_watchers`): startup
// degrades LOUDLY per-service without blocking later stages; stop() joins the
// loop deterministically (drain semantics). This module is the loop only —
// stage wiring belongs to the gateway driver.
//
// HARD RULE honored here: time enters only through the injected GatewayClock.
// Behavior contracts drive the 5s delay, the 2s cadence, and stop() wakeups
// deterministically; logic never touches a wall clock.

import type { GatewayClock } from "./clock.js";
import { systemClock } from "./clock.js";
import type { HandoffQueue, HandoffRow } from "./queue.js";

/** Poll cadence (run.py:_handoff_watcher interval default 2.0s). */
export const HANDOFF_POLL_INTERVAL_MS = 2000;

/**
 * Initial delay before the FIRST poll — the gateway must be fully connected
 * to its platforms before dispatching through them.
 */
export const HANDOFF_STARTUP_DELAY_MS = 5000;

export interface HandoffWatcherDeps {
	queue: HandoffQueue;
	/** The DEC-008 pipeline step for one claimed row. Throws ⇒ fail path. */
	processRow(row: HandoffRow): Promise<void>;
	clock?: GatewayClock;
	/** Poll cadence override (tests inject; production default 2000ms). */
	intervalMs?: number;
	/** Startup delay override (tests inject; production default 5000ms). */
	startupDelayMs?: number;
	log?: {
		warn?(message: string, meta?: Record<string, unknown>): void;
		debug?(message: string, meta?: Record<string, unknown>): void;
	};
}

export interface HandoffTickReport {
	/** Rows listed pending at tick start. */
	pending: number;
	/** Rows this tick WON the claim race for. */
	claimed: number;
	completed: number;
	/** Rows moved to failed (+error payload captured from the throw). */
	failed: number;
	failures: Array<{ sessionId: string; error: string }>;
}

export class HandoffWatcher {
	private readonly queue: HandoffQueue;
	private readonly processRow: (row: HandoffRow) => Promise<void>;
	private readonly clock: GatewayClock;
	private readonly intervalMs: number;
	private readonly startupDelayMs: number;
	private readonly log: HandoffWatcherDeps["log"];

	private running = false;
	private loopPromise: Promise<void> | null = null;
	private pendingSleepResolvers: Array<() => void> = [];

	constructor(deps: HandoffWatcherDeps) {
		this.queue = deps.queue;
		this.processRow = deps.processRow;
		this.clock = deps.clock ?? systemClock;
		this.intervalMs = deps.intervalMs ?? HANDOFF_POLL_INTERVAL_MS;
		this.startupDelayMs = deps.startupDelayMs ?? HANDOFF_STARTUP_DELAY_MS;
		this.log = deps.log;
	}

	get isRunning(): boolean {
		return this.running;
	}

	/**
	 * ONE poll cycle at an explicit instant — never throws (best-effort
	 * parity with the inner try/except of _handoff_watcher). Tests drive this
	 * directly instead of racing real timers.
	 */
	async tick(): Promise<HandoffTickReport> {
		const report: HandoffTickReport = {
			pending: 0,
			claimed: 0,
			completed: 0,
			failed: 0,
			failures: [],
		};
		try {
			const pendingRows = await this.queue.listPendingHandoffs();
			report.pending = pendingRows.length;
			for (const row of pendingRows) {
				if (!row.id) continue;
				// Atomic claim: another tick / another gateway already claimed it.
				if (!(await this.queue.claimHandoff(row.id))) continue;
				report.claimed++;
				try {
					await this.processRow(row);
					await this.queue.completeHandoff(row.id);
					report.completed++;
				} catch (err) {
					const message = err instanceof Error ? err.message : String(err);
					this.log?.warn?.("handoff failed", {
						session_id: row.id,
						error: message,
					});
					await this.queue.failHandoff(row.id, message);
					report.failed++;
					report.failures.push({ sessionId: row.id, error: message });
				}
			}
		} catch (err) {
			// Outer-loop containment: a broken store must not kill the watcher.
			this.log?.debug?.("handoff watcher tick error", {
				error: String(err),
			});
		}
		return report;
	}

	/** Begin the background loop (idempotent). Startup delay precedes tick 1. */
	start(): void {
		if (this.running) return;
		this.running = true;
		this.loopPromise = this.loop();
	}

	/** Stop the loop and join it; breaks any in-flight sleep immediately. */
	async stop(): Promise<void> {
		this.running = false;
		const waiters = this.pendingSleepResolvers;
		this.pendingSleepResolvers = [];
		for (const wake of waiters) wake();
		const loop = this.loopPromise;
		this.loopPromise = null;
		if (loop) await loop;
	}

	private async loop(): Promise<void> {
		// Initial delay so the gateway is fully connected to its platforms
		// before we try to dispatch handoffs through them (_handoff_watcher).
		if (!(await this.sleepInterruptible(this.startupDelayMs))) return;
		while (this.running) {
			await this.tick();
			if (!this.running) break;
			if (!(await this.sleepInterruptible(this.intervalMs))) return;
		}
	}

	/** Clock sleep that stop() can break — never leaves a hung loop behind. */
	private sleepInterruptible(ms: number): Promise<boolean> {
		return new Promise<boolean>((resolvePromise) => {
			let done = false;
			const finish = (woke: boolean): void => {
				if (done) return;
				done = true;
				this.pendingSleepResolvers = this.pendingSleepResolvers.filter(
					(w) => w !== wake,
				);
				resolvePromise(woke);
			};
			const wake = (): void => finish(false); // cancelled by stop()
			this.pendingSleepResolvers.push(wake);
			void this.clock.sleepMs(ms).then(
				() => finish(true),
				() => finish(true),
			);
		});
	}
}
