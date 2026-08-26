// scheduler.ts — the in-process retry loop for the obligations ledger.
//
// Hermes redelivers ONLY at restart boundaries (run.py:_redeliver_pending_obligations
// sweeps once at boot). A long-lived gateway process additionally owes its own
// failed sends a bounded, backoff-capped retry between boots; this module is
// that loop. DIVERGENCE RATIFIED AS DEC-053: "the gateway runs a periodic
// obligation retry tick (default every 15s). Dead-owner rows claim immediately
// (restart-boundary parity); self-owned pending/failed rows wait out the
// exponential backoff (60s ×4 growth, 1h cap) so the ledger never busy-retries."
//
// Claim-time resume-clear parity (#91969): run.py:_claim_pending_obligations
// clears resume_pending for EVERY claimed row BEFORE redelivery, so boot
// resume never re-runs (re-pays) a turn whose answer the ledger already
// holds. This scheduler is pi's claim point, so tick() hands each claimed
// session_key to the injected clearResumePending hook before driveClaimed.
// The lifecycle drain's clearResumePending stays pre-drain only — this is
// the claim-time complement, not a replacement.
//
// HARD RULE honored here: time enters only through the injected GatewayClock.
// The default systemClock touches Date.now/setTimeout; logic never does.

import type { DeliveryLedger } from "./ledger.js";
import type { ClaimedObligation, SettleReport } from "./ledger.js";
import type { DeliverySender } from "./sender.js";
import type { GatewayClock } from "./clock.js";

export const DEFAULT_TICK_INTERVAL_SECONDS = 15;

export interface RetrySchedulerOptions {
	clock?: GatewayClock;
	/** Poll cadence for the background loop (tests drive tick() directly instead). */
	intervalSeconds?: number;
	/**
	 * Platforms currently deliverable — evaluated fresh each tick so adapters
	 * connecting late start claiming without a restart. A function returning
	 * undefined means "no filter" (claim everything).
	 */
	deliverablePlatforms?:
		| ReadonlySet<string>
		| (() => ReadonlySet<string> | undefined);
	/**
	 * Resume-clear parity (#91969, run.py:_claim_pending_obligations):
	 * invoked once per claimed row's session key AFTER the claim and BEFORE
	 * any driveClaimed send — a session with a claimed obligation already
	 * produced its answer (only delivery is owed), so its resume_pending
	 * flag must be gone before redelivery or boot-resume scheduling can
	 * observe it. Per-key best-effort exactly like Hermes: a throwing key is
	 * swallowed and never blocks the remaining keys or the sends. Absent ⇒
	 * no-op (the lifecycle drain hook covers shutdown only).
	 */
	clearResumePending?: (sessionKey: string) => Promise<void>;
}

export interface SchedulerTickReport {
	/** Rows claimed via dead-owner recovery (restart-boundary semantics). */
	recovered: number;
	/** Rows claimed via the backoff-due path (self-owned or orphaned). */
	retried: number;
	results: SettleReport[];
}

/**
 * Drives claimed obligations through a sender on a fixed cadence. Start/stop
 * manage one background loop; tests call `tick(nowSeconds)` directly and
 * never construct wall time themselves.
 */
export class ObligationRetryScheduler {
	private readonly clock: GatewayClock;
	private readonly intervalSeconds: number;
	private readonly deliverablePlatformsOpt:
		| ReadonlySet<string>
		| (() => ReadonlySet<string> | undefined)
		| undefined;
	private readonly clearResumePendingOpt:
		| ((sessionKey: string) => Promise<void>)
		| undefined;
	private running = false;
	private loopPromise: Promise<void> | null = null;
	private pendingSleepResolvers: Array<() => void> = [];

	private readonly ledger: DeliveryLedger;
	private readonly sender: DeliverySender;

	constructor(
		ledger: DeliveryLedger,
		sender: DeliverySender,
		opts: RetrySchedulerOptions = {},
	) {
		this.ledger = ledger;
		this.sender = sender;
		this.clock = opts.clock ?? ledger.nowClock();
		this.intervalSeconds =
			opts.intervalSeconds ?? DEFAULT_TICK_INTERVAL_SECONDS;
		this.deliverablePlatformsOpt = opts.deliverablePlatforms;
		this.clearResumePendingOpt = opts.clearResumePending;
	}

	private deliverableNow(): ReadonlySet<string> | undefined {
		const raw = this.deliverablePlatformsOpt;
		if (raw === undefined) return undefined;
		return typeof raw === "function" ? raw() : raw;
	}

	/** One full cycle at an explicit instant. Never throws (best-effort parity). */
	async tick(nowSeconds?: number): Promise<SchedulerTickReport> {
		const now = nowSeconds ?? this.clock.nowSeconds();
		const deliverable = this.deliverableNow();
		const report: SchedulerTickReport = {
			recovered: 0,
			retried: 0,
			results: [],
		};
		try {
			const filter: {
				nowSeconds: number;
				deliverablePlatforms?: ReadonlySet<string>;
			} = {
				nowSeconds: now,
			};
			if (deliverable !== undefined) filter.deliverablePlatforms = deliverable;
			const recovered = await this.ledger.sweepRecoverable(filter);
			report.recovered = recovered.length;
			const retried = await this.ledger.claimDueRetries(filter);
			report.retried = retried.length;
			const combined: ClaimedObligation[] = [...recovered, ...retried];
			if (combined.length > 0) {
				// #91969 parity: clears complete BEFORE any send so even a hung
				// redelivery leaves no replay window for the resume path.
				await this.clearResumeForClaimed(combined);
				report.results = await this.ledger.driveClaimed(combined, this.sender, {
					nowSeconds: now,
				});
			}
		} catch {
			// Best-effort: a failed tick must never crash the gateway loop.
		}
		return report;
	}

	/**
	 * Per-claimed-row resume_pending clear (run.py:_claim_pending_obligations
	 * loop): empty keys are skipped (`if not session_key: continue`), each key
	 * is awaited individually and isolated — one store failure neither blocks
	 * the remaining keys nor the redeliveries that follow.
	 */
	private async clearResumeForClaimed(
		claimed: readonly ClaimedObligation[],
	): Promise<void> {
		const clear = this.clearResumePendingOpt;
		if (clear === undefined) return;
		for (const row of claimed) {
			if (!row.sessionKey) continue;
			try {
				await clear(row.sessionKey);
			} catch {
				// parity: "clear_resume_pending failed for %s" at debug level
			}
		}
	}

	/** Begin the background loop (idempotent). */
	start(): void {
		if (this.running) return;
		this.running = true;
		this.loopPromise = this.loop();
	}

	async stop(): Promise<void> {
		this.running = false;
		const waiters = this.pendingSleepResolvers;
		this.pendingSleepResolvers = [];
		for (const wake of waiters) wake(); // break a blocked sleep deterministically
		const loop = this.loopPromise;
		this.loopPromise = null;
		if (loop) await loop;
	}

	get isRunning(): boolean {
		return this.running;
	}

	private async loop(): Promise<void> {
		while (this.running) {
			await this.tick();
			if (!this.running) break;
			await this.sleepInterruptible(this.intervalSeconds * 1000);
		}
	}

	/** Clock sleep that stop() can break — never leaves a hung loop behind. */
	private sleepInterruptible(ms: number): Promise<void> {
		return new Promise<void>((resolvePromise) => {
			let done = false;
			const finish = (): void => {
				if (done) return;
				done = true;
				this.pendingSleepResolvers = this.pendingSleepResolvers.filter(
					(w) => w !== finish,
				);
				resolvePromise();
			};
			this.pendingSleepResolvers.push(finish);
			void this.clock.sleepMs(ms).then(finish, finish);
		});
	}
}
