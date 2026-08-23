// scheduler.ts — the in-process retry loop for the obligations ledger.
//
// Hermes redelivers ONLY at restart boundaries (run.py:_redeliver_pending_obligations
// sweeps once at boot). A long-lived gateway process additionally owes its own
// failed sends a bounded, backoff-capped retry between boots; this module is
// that loop. PROPOSED DEC text: "the gateway runs a periodic obligation retry
// tick (default every 15s). Dead-owner rows claim immediately (restart-
// boundary parity); self-owned pending/failed rows wait out the exponential
// backoff (60s ×4 growth, 1h cap) so the ledger never busy-retries."
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
				report.results = await this.ledger.driveClaimed(combined, this.sender, {
					nowSeconds: now,
				});
			}
		} catch {
			// Best-effort: a failed tick must never crash the gateway loop.
		}
		return report;
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
