// pi_embedded/cron/scheduler.ts — the in-process cron ticker service.
//
// Hermes anchors (READ-ONLY reference):
//   cron/scheduler.py:tick                 → CronScheduler.tickOnce
//   cron/scheduler.py run_job (claim → monitor → mark → deliver) → runDueJob
//   cron/scheduler_provider.py (ticker loop ownership + EMFILE backoff)
//                                          → start/stop/loop
//   agent/estop.py:check_paused("cron")    → estop sentinel check
//
// Binding tick shape (07 §5.2): acquire the tick lock (contention ⇒ silent
// skip; fd exhaustion ⇒ LOUD raise) → emergency-stop sentinel check (due
// jobs wait; in-flight runs untouched) → get due jobs → advance recurring
// next_run_at BEFORE execution (at-most-once) → per job: claim an execution
// token → run under the claim-heartbeat watch → mark_job_run (fire-owner CAS
// discards stale completions) → deliver.
// DEC-070 scope note: the former true-inactivity runaway bound
// (HERMES_CRON_TIMEOUT idle kill) was removed; cron jobs now run to
// completion regardless of duration. The claim-heartbeat polling that
// guards fire ownership (07 §5.2) survives unchanged.
//
// Lifecycle integration: this is a lifecycle OPTIONAL stage (#7
// `cron_scheduler` — degrade LOUDLY per-service without blocking later
// stages, 01 §3.1). pi_embedded cannot import pi_gateway/lifecycle (the
// layering gate bans runner internals), so the service exposes a STRUCTURAL
// handle ({name, stop}) that the composition root wires into
// ctx.services.cron via its stageBodies — plus startCronTickerOrDegraded,
// which implements the loud-degrade semantics pre-packaged. No live config
// reload (DEC-013): every option is read ONCE at construction; flipping
// requires restart.

import { existsSync } from "node:fs";
import { join } from "node:path";

import type { CronClock } from "./clock.js";
import { systemCronClock } from "./clock.js";
import type { CronLogger } from "./logger.js";
import { stderrLogger } from "./logger.js";
import { runWithClaimHeartbeat } from "./claim-heartbeat.js";
import {
	deliverCronResult,
	type CronWrapConfig,
	type DeliverySink,
	type MirrorAppender,
	type MirrorConfig,
} from "./delivery.js";
import type {
	CronDeliveryTarget,
	CronJobRecord,
	CronJobStore,
	DueJob,
} from "./store.js";
import {
	TickLock,
	backoffWaitSeconds,
	classifyTickAcquireFailure,
	noteTickFailure,
	TickLockAcquisitionError,
} from "./tick-lock.js";

export const DEFAULT_TICK_INTERVAL_SECONDS = 60;

/**
 * The runner surface the scheduler drives. Production adapter:
 * executor.ts's cronExecutorAsRunner.
 */
export interface ScheduledJobRunner {
	run(ctx: { job: CronJobRecord }): Promise<RunnerOutcome>;
	interrupt(jobId: string): Promise<boolean>;
}

export interface RunnerOutcome {
	ok: boolean;
	outputText?: string;
	error?: string;
}

export type CronRunStatus =
	| "ok"
	| "error"
	| "interrupted"
	| "claim_lost_before_start"
	| "claim_lost"
	| "stale_mark_discarded";

export interface CronRunReport {
	jobId: string;
	status: CronRunStatus;
	fastForwarded: boolean;
	error?: string;
	deliveryErrors?: string[];
	mirrored?: boolean;
	/** False when nothing was persisted for this fire (winner owns record). */
	wroteResults: boolean;
}

export interface CronTickReport {
	executed: number;
	results: CronRunReport[];
	skipped?: "lock_contention" | "estop" | "no_due_jobs";
}

export interface CronSchedulerOptions {
	store: CronJobStore;
	runner: ScheduledJobRunner;
	clock?: CronClock;
	logger?: CronLogger;
	/** Ticker cadence in seconds (Hermes builtin: 60). Read ONCE (DEC-013). */
	intervalSeconds?: number;
	/** Emergency-stop sentinel path (`hermes pause` analogue). */
	estopPath?: string;
	/** Delivery transport seam. Absent ⇒ runs mark results without delivery. */
	deliverySink?: DeliverySink;
	wrap?: CronWrapConfig;
	mirror?: MirrorConfig;
	appender?: MirrorAppender;
	tickLock?: TickLock;
}

export interface CronServiceHandle {
	name: "cron";
	stop(): Promise<void>;
	/**
	 * Live count of in-flight cron job executions — the gateway shutdown
	 * drain's wait input (#60432; cron/scheduler.py:get_running_job_ids
	 * parity). A run counts from dispatch until it fully settles (claim →
	 * tool work → mark → deliver), so the drain can hold teardown open on
	 * its OWN budget instead of killing the run mid-flight (#82161 — a
	 * killed run is a permanent jobs.json failure nobody is waiting on).
	 */
	inflightCount(): number;
}

export class CronScheduler {
	private readonly store: CronJobStore;
	private readonly runner: ScheduledJobRunner;
	private readonly clock: CronClock;
	private readonly log: CronLogger;
	private readonly intervalSeconds: number;
	private readonly estopPath: string;
	private readonly deliverySink: DeliverySink | undefined;
	private readonly wrapConfig: CronWrapConfig | undefined;
	private readonly mirrorConfig: MirrorConfig | undefined;
	private readonly appender: MirrorAppender | undefined;
	private readonly tickLock: TickLock;

	private running = false;
	private loopPromise: Promise<void> | null = null;
	private pendingSleepResolvers: Array<() => void> = [];
	private consecutiveFailures = 0;
	/** Job ids currently executing end-to-end (dispatch → settle). */
	private readonly inflightJobIds = new Set<string>();

	constructor(options: CronSchedulerOptions) {
		this.store = options.store;
		this.runner = options.runner;
		this.clock = options.clock ?? systemCronClock;
		this.log = options.logger ?? stderrLogger();
		this.intervalSeconds =
			options.intervalSeconds ?? DEFAULT_TICK_INTERVAL_SECONDS;
		this.estopPath =
			options.estopPath ?? join(options.store.paths.cronDir, ".estop");
		this.deliverySink = options.deliverySink;
		this.wrapConfig = options.wrap;
		this.mirrorConfig = options.mirror;
		this.appender = options.appender;
		this.tickLock =
			options.tickLock ?? new TickLock(options.store.paths.cronDir);
	}

	get isRunning(): boolean {
		return this.running;
	}

	/** Structural service handle for the lifecycle services list. */
	handle(): CronServiceHandle {
		return {
			name: "cron",
			stop: () => this.stop(),
			inflightCount: () => this.inflightJobIds.size,
		};
	}

	/**
	 * Snapshot of in-flight job ids (get_running_job_ids shape parity): a job
	 * is a member from dispatch into runDueJob until it settles — the ENTIRE
	 * run, tool calls and delivery included, not just the dispatch instant.
	 */
	get runningJobs(): readonly string[] {
		return [...this.inflightJobIds];
	}

	/** Drain-input count (#60432): jobs mid-run right now. */
	get inflightJobCount(): number {
		return this.inflightJobIds.size;
	}

	/**
	 * ONE full tick at the current logical instant. Never both silently skips
	 * on a REAL failure and reports healthy: contention returns a skipped
	 * report; every other acquisition failure THROWS (the #87644 fork).
	 */
	async tickOnce(): Promise<CronTickReport> {
		const lease = this.tickLock.acquire();
		if (!lease.acquired) {
			// Silent skip (debug-level parity of tick's contention branch).
			this.log.info("tick skipped — another instance holds the lock", {
				skipped: "lock_contention",
			});
			return { executed: 0, results: [], skipped: "lock_contention" };
		}
		try {
			if (existsSync(this.estopPath)) {
				// Emergency stop: due jobs WAIT (return untouched); in-flight
				// runs are never touched by the sentinel.
				return { executed: 0, results: [], skipped: "estop" };
			}
			const nowSec = this.clock.nowSeconds();
			const dueReport = await this.store.getDueJobs(nowSec);
			if (dueReport.due.length === 0) {
				return { executed: 0, results: [], skipped: "no_due_jobs" };
			}
			// Advance recurring next_run_at BEFORE any execution begins —
			// at-most-once semantics under the jobs lock (mark_job_run later
			// re-anchors from completion time).
			const dueJobIds = dueReport.due.map((d) => d.job.id);
			await this.store.advanceNextRuns(dueJobIds, nowSec);

			const results: CronRunReport[] = [];
			for (const entry of dueReport.due) {
				results.push(await this.runDueJob(entry));
			}
			return { executed: results.length, results };
		} finally {
			lease.lease.release();
		}
	}

	/** One due job end-to-end: claim → monitor → mark → deliver. */
	private async runDueJob(entry: DueJob): Promise<CronRunReport> {
		// In-flight registration spans the WHOLE run — dispatch until full
		// settlement — so the shutdown drain sees claim attempts, tool work,
		// marking AND delivery as active work (#60432).
		this.inflightJobIds.add(entry.job.id);
		try {
			return await this.runDueJobRegistered(entry);
		} finally {
			this.inflightJobIds.delete(entry.job.id);
		}
	}

	private async runDueJobRegistered(entry: DueJob): Promise<CronRunReport> {
		const jobId = entry.job.id;
		const nowSec = this.clock.nowSeconds();
		const claimed = await this.store.claimJobForFire(jobId, { now: nowSec });
		if (
			claimed === null ||
			claimed.fire_claim === null ||
			claimed.fire_claim === undefined
		) {
			return {
				jobId,
				status: "claim_lost_before_start",
				fastForwarded: entry.fastForwarded,
				wroteResults: false,
			};
		}
		const owner = claimed.fire_claim.by;

		try {
			const bound = await runWithClaimHeartbeat({
				exec: () => this.runner.run({ job: claimed }),
				interrupt: () => this.runner.interrupt(jobId),
				clock: this.clock,
				shouldAbort: async (now) => {
					// Claim heartbeat doubles as the claim-loss detector: when
					// another tick owns the claim, abort the stale run instead
					// of double-writing results (07 §5.2 fire ownership).
					try {
						const alive = await this.store.heartbeatRunClaim(jobId, owner, {
							now,
						});
						return !alive;
					} catch {
						return false; // transient store failure never kills the watchdog
					}
				},
			});

			if (bound.aborted === true) {
				// Stale run: interrupt issued; the WINNER persists results.
				return {
					jobId,
					status: "claim_lost",
					fastForwarded: entry.fastForwarded,
					wroteResults: false,
				};
			}

			const outcome = bound.result!;
			let deliveryError: string | undefined;
			let deliveryErrors: string[] = [];
			let mirrored = false;
			if (outcome.ok && this.deliverySink !== undefined) {
				const targets: readonly CronDeliveryTarget[] = claimed.deliver ?? [];
				if (targets.length > 0 && outcome.outputText !== undefined) {
					const delivery = await deliverCronResult({
						job: {
							id: claimed.id,
							name: claimed.name,
							...(claimed.attach_to_session !== undefined
								? { attachToSession: claimed.attach_to_session }
								: {}),
						},
						outputText: outcome.outputText,
						targets,
						sink: this.deliverySink,
						...(this.wrapConfig !== undefined ? { wrap: this.wrapConfig } : {}),
						...(this.mirrorConfig !== undefined
							? { mirror: this.mirrorConfig }
							: {}),
						...(this.appender !== undefined ? { appender: this.appender } : {}),
					});
					deliveryErrors = delivery.deliveryErrors;
					if (delivery.deliveryErrors.length > 0) {
						deliveryError = delivery.deliveryErrors.join("; ");
					}
					mirrored = delivery.mirrored;
				}
			}

			const marked = await this.store.markJobRun(jobId, {
				success: outcome.ok,
				...(outcome.error !== undefined ? { error: outcome.error } : {}),
				...(deliveryError !== undefined ? { deliveryError } : {}),
				expectedFireOwner: owner,
			});
			if (!marked) {
				// Owner changed between completion and mark — discard silently
				// (warning parity of mark_job_run's stale-completion branch).
				this.log.warn("completion discarded — fire claim owner changed", {
					job_id: jobId,
				});
				return {
					jobId,
					status: "stale_mark_discarded",
					fastForwarded: entry.fastForwarded,
					wroteResults: false,
				};
			}
			return {
				jobId,
				status: outcome.ok ? "ok" : "error",
				fastForwarded: entry.fastForwarded,
				...(outcome.error !== undefined ? { error: outcome.error } : {}),
				...(deliveryErrors.length > 0 ? { deliveryErrors } : {}),
				...(mirrored ? { mirrored } : {}),
				wroteResults: true,
			};
		} catch (err) {
			// Executor failure outside the bound's settlement contract: persist
			// an errored run (re-arm forward parity), never crash the ticker.
			const message = err instanceof Error ? err.message : String(err);
			try {
				await this.store.markJobRun(jobId, {
					success: false,
					error: message,
					expectedFireOwner: owner,
				});
			} catch {
				/* best-effort */
			}
			return {
				jobId,
				status: "error",
				fastForwarded: entry.fastForwarded,
				error: message,
				wroteResults: true,
			};
		}
	}

	// ------------------------------------------------------------------
	// Background loop (ticker ownership + #87644 backoff)
	// ------------------------------------------------------------------

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

	private async loop(): Promise<void> {
		while (this.running) {
			try {
				await this.tickOnce();
				this.consecutiveFailures = 0;
			} catch (err) {
				// LOUD: a failed tick must degrade liveness VISIBLY (#87644).
				this.log.error(`cron tick failed: ${String(err)}`, {
					kind: classifyTickAcquireFailure(err),
				});
				this.consecutiveFailures = noteTickFailure(
					err,
					this.consecutiveFailures,
				);
			}
			if (!this.running) break;
			const waitSeconds = backoffWaitSeconds(
				this.intervalSeconds,
				this.consecutiveFailures,
			);
			await this.sleepInterruptible(waitSeconds * 1000);
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

// TickLockAcquisitionError re-export keeps the loud-failure vocabulary local.
export { TickLockAcquisitionError };

// -----------------------------------------------------------------------
// Lifecycle optional-stage integration (01 §3.1 degraded-start semantics)
// -----------------------------------------------------------------------

export interface StartCronTickerOptions extends CronSchedulerOptions {
	log?: CronLogger;
}

export interface CronStartupResult {
	ok: boolean;
	handle?: CronServiceHandle;
	scheduler?: CronScheduler;
	/** Set when startup degraded loudly (optional-stage semantics). */
	degradedReason?: string;
}

/**
 * Construct + start the ticker, degrading LOUDLY instead of throwing when
 * construction fails — the exact optional-stage contract (a broken cron must
 * not block watchers/adapters from starting). Composition roots that prefer
 * raw throws can construct CronScheduler themselves and wire handle().
 */
export function startCronTickerOrDegraded(
	options: StartCronTickerOptions,
): CronStartupResult {
	const log = options.log ?? options.logger ?? stderrLogger();
	try {
		const scheduler = new CronScheduler({ ...options, logger: log });
		scheduler.start();
		return { ok: true, handle: scheduler.handle(), scheduler };
	} catch (err) {
		const message = err instanceof Error ? err.message : String(err);
		log.error(`cron scheduler DEGRADED at startup: ${message}`, {
			reason_code: "degraded_start",
			service: "cron",
		});
		return { ok: false, degradedReason: message };
	}
}
