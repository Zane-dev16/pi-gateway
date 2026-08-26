// pi_embedded/cron/store.ts — the cron job store.
//
// SUBSTRATE CHOICE (recorded per phase brief): Hermes keeps jobs in a JSON
// FILE, not SQLite — 07 §5.1 binding: "Store is a **JSON file**, not SQLite:
// `<home>/cron/jobs.json` under a cross-process advisory lock for critical
// sections (`JOBS_FILE`, `_CronStorePaths` indirection so tests can re-point
// stores)". Pi ports that verbatim: `<home>/cron/jobs.json` + atomic
// tmpfile+rename saves (`_secure_write` parity) + JobsFileLock critical
// sections. No pi_state tables, no reconcile entries — state.db stays
// conversation-shaped (DEC-001 posture) and the cron store stays
// operator-inspectable exactly like `cron/jobs.py`.
//
// Ported semantics (Hermes anchors, READ-ONLY reference; no code vendored):
//   cron/jobs.py:load_jobs / save_jobs          → loadJobs / saveJobs
//   cron/jobs.py:create_job (one-shot grace)    → createJob
//   cron/jobs.py:get_due_jobs (catchup/ff)      → getDueJobs
//   cron/jobs.py:advance_next_runs              → advanceNextRuns
//   cron/jobs.py:claim_job_for_fire             → claimJobForFire
//   cron/jobs.py:heartbeat_run_claim / clear_run_claim → …
//   cron/jobs.py:mark_job_run                   → markJobRun
//   cron/jobs.py:pause_job / resume_job / update_job / remove_job → …
//   cron/jobs.py:record_catch_up_occurrence     → catchUpOccurrenceCount
//   cron/jobs.py:get_due_jobs stale-error re-arm (_job_is_stale_error_
//     recurring + _record_persisted_error_recovery) → getDueJobs wedge
//     branch (below) — a recurring job whose persisted state shows
//     last_status=error with last_run_at older than a full cadence+grace
//     and next_run_at parked in the future has been sitting WEDGED since
//     its post-error re-arm; it is invisible to every other sweep (not
//     running, not due), so it just sits dead. getDueJobs re-arms it:
//     interval → now (no excluded times ⇒ immediate retry is always a
//     legal fire), cron → the next LEGAL occurrence from now (re-arming to
//     now would fire at instants the expression explicitly excludes). Each
//     re-arm appends one countable row to
//     `<cronDir>/persisted_error_recoveries.jsonl` (parity of
//     _record_persisted_error_recovery's JSONL telemetry sink).

import {
	appendFileSync,
	existsSync,
	mkdirSync,
	readFileSync,
	renameSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { randomUUID } from "node:crypto";
import { join } from "node:path";

import type { CronClock } from "./clock.js";
import { systemCronClock } from "./clock.js";
import {
	catchupGraceSeconds,
	computeNextRun,
	epochToIso,
	isoToEpoch,
	parseSchedule,
	scheduleCadenceSeconds,
	type CronSchedule,
} from "./schedule.js";
import { JobsFileLock } from "./jobs-lock.js";

/** Re-pointable store paths (parity of _CronStorePaths). */
export interface CronStorePaths {
	cronDir: string;
	jobsFile: string;
	outputDir: string;
}

export function defaultCronStorePaths(home: string): CronStorePaths {
	const cronDir = join(home, "cron");
	return {
		cronDir,
		jobsFile: join(cronDir, "jobs.json"),
		outputDir: join(cronDir, "output"),
	};
}

/** Delivery target carried on a job record (07 §5.1 multi-platform delivery). */
export interface CronDeliveryTarget {
	platform: string;
	chatId: string;
	threadId?: string;
}

/** Schedule as persisted in jobs.json (Hermes field names for file parity). */
export type StoredSchedule =
	| { kind: "once"; run_at: string }
	| { kind: "interval"; minutes: number }
	| { kind: "cron"; expr: string };

function toStoredSchedule(schedule: CronSchedule): StoredSchedule {
	switch (schedule.kind) {
		case "once":
			return { kind: "once", run_at: epochToIso(schedule.runAtSeconds) };
		case "interval":
			return { kind: "interval", minutes: schedule.minutes };
		case "cron":
			return { kind: "cron", expr: schedule.expr };
	}
}

export function storedScheduleToCron(stored: StoredSchedule): CronSchedule {
	switch (stored.kind) {
		case "once": {
			const seconds = isoToEpoch(stored.run_at);
			if (seconds === null) throw new Error(`bad run_at ${stored.run_at}`);
			return { kind: "once", runAtSeconds: seconds };
		}
		case "interval":
			return { kind: "interval", minutes: stored.minutes };
		case "cron":
			return { kind: "cron", expr: stored.expr };
	}
}

export type JobState = "scheduled" | "paused" | "completed" | "error";

/** One persisted job record. Field names follow cron/jobs.py's dict shape. */
export interface CronJobRecord {
	id: string;
	name: string;
	prompt: string;
	schedule: StoredSchedule;
	enabled: boolean;
	state: JobState;
	paused_at?: string | null;
	paused_reason?: string | null;
	created_at: string;
	next_run_at: string | null;
	last_run_at?: string | null;
	last_status?:
		| "ok"
		| "error"
		| "interrupted"
		| "blocked_config"
		| (string & {});
	last_error?: string | null;
	last_delivery_error?: string | null;
	deliver?: CronDeliveryTarget[];
	attach_to_session?: boolean;
	fire_claim?: { at: string; by: string } | null;
	failure_streak?: number;
}

export interface CreateJobInput {
	name?: string;
	prompt: string;
	/** Schedule string (ticker grammar) or pre-parsed schedule. Required. */
	schedule: string | CronSchedule;
	deliver?: CronDeliveryTarget[];
	attachToSession?: boolean;
}

export interface DueJob {
	job: CronJobRecord;
	/**
	 * True when the scheduled slot was STALE past the catchup grace window:
	 * accumulated missed runs were skipped (fast-forwarded) and this is the
	 * single execute-once-now fire (07 §5.2 catchup window row).
	 */
	fastForwarded: boolean;
}

export class OneShotGraceError extends Error {
	constructor(runAtIso: string, graceSeconds: number) {
		super(
			`Requested one-shot time ${runAtIso} is more than ` +
				`${graceSeconds}s in the past and cannot be scheduled.`,
		);
		this.name = "OneShotGraceError";
	}
}

export interface MarkRunInput {
	success: boolean;
	error?: string;
	deliveryError?: string;
	status?: string;
	/**
	 * Fire-ownership CAS (07 §5.2 fire ownership): when set and the persisted
	 * claim owner differs, the completion is DISCARDED (stale runner must not
	 * double-write results over the winner's record).
	 */
	expectedFireOwner?: string;
}

export interface GetDueReport {
	due: DueJob[];
	/** Fast-forwarded job ids whose provisional next_run_at was persisted. */
	fastForwarded: string[];
}

const FIRE_CLAIM_TTL_SECONDS = 300;

/** Recent-recovery ring bound (parity _PERSISTED_ERROR_RECOVERY_HISTORY). */
export const PERSISTED_ERROR_RECOVERY_HISTORY = 20;

/** JSONL telemetry sink under cronDir (parity file name). */
export const PERSISTED_ERROR_RECOVERIES_FILENAME =
	"persisted_error_recoveries.jsonl";

/** One persisted-error re-arm record (field names follow jobs.py's entry). */
export interface PersistedErrorRecoveryEntry {
	job_id: string;
	name: string;
	/** The wedged next_run_at this recovery replaced. */
	previous_next_run_at: string;
	rearmed_at: string;
}

export interface PersistedErrorRecoveryStats {
	persisted_error_recoveries: number;
	recent: PersistedErrorRecoveryEntry[];
}

/**
 * True when a RECURRING job is wedged in a stale persisted error state
 * (parity _job_is_stale_error_recurring, all must hold):
 *   - persisted last_status === "error" (a prior fire errored and never
 *     recovered);
 *   - no live run gets re-armed underneath itself (#62002): pi realizes the
 *     reference's running-set liveness as the persisted fire-claim lease —
 *     a FRESH claim means a run is alive somewhere (this process or a
 *     sibling tick), so the job is left alone until the lease expires;
 *   - last_run_at exists and is older than cadence+grace: markJobRun stamps
 *     last_run_at on EVERY fire (success or failure), so a merely erroring-
 *     and-retrying job stays fresh and never trips this; a full silent
 *     period does.
 * Unknown cadence falls back to the grace window for the cadence half
 * (never re-arm anything younger than the grace floor).
 */
function isStaleErrorRecurring(
	job: CronJobRecord,
	schedule: CronSchedule,
	nowSec: number,
): boolean {
	if (job.last_status !== "error") return false;
	if (hasFreshFireClaim(job, nowSec)) return false;
	if (!job.last_run_at) return false;
	const lastRunSec = isoToEpoch(job.last_run_at);
	if (lastRunSec === null) return false;
	const age = nowSec - lastRunSec;
	if (age < 0) return false; // clock-skewed last_run never counts as old
	const grace = catchupGraceSeconds(schedule, nowSec);
	const cadence = scheduleCadenceSeconds(schedule, nowSec) ?? grace;
	return age > cadence + grace;
}

/** Fresh fire claim ≙ a run is live right now (both-sides-bounded age, the
 * heartbeatRunClaim freshness rule). */
function hasFreshFireClaim(job: CronJobRecord, nowSec: number): boolean {
	const claim = job.fire_claim;
	if (claim === null || claim === undefined) return false;
	const claimedAt = isoToEpoch(claim.at);
	if (claimedAt === null) return false;
	const age = nowSec - claimedAt;
	return age >= 0 && age < FIRE_CLAIM_TTL_SECONDS;
}

export interface CronJobStoreOptions {
	paths?: CronStorePaths;
	clock?: CronClock;
}

export class CronJobStore {
	private readonly pathsValue: CronStorePaths;
	private readonly clock: CronClock;
	private readonly lock: JobsFileLock;
	private recoveryCount = 0;
	private recentRecoveries: PersistedErrorRecoveryEntry[] = [];

	constructor(options: CronJobStoreOptions = {}) {
		this.pathsValue = options.paths ?? defaultCronStorePaths(".");
		this.clock = options.clock ?? systemCronClock;
		this.lock = new JobsFileLock(this.pathsValue.cronDir);
	}

	get paths(): CronStorePaths {
		return this.pathsValue;
	}

	get clockNow(): number {
		return this.clock.nowSeconds();
	}

	ensureDirs(): void {
		mkdirSync(this.pathsValue.cronDir, { recursive: true });
		mkdirSync(this.pathsValue.outputDir, { recursive: true });
	}

	close(): void {
		this.lock.close();
	}

	// ------------------------------------------------------------------
	// Raw load/save (atomic write; torn readers never observe truncation)
	// ------------------------------------------------------------------

	async loadJobs(): Promise<CronJobRecord[]> {
		return this.lock.withJobsLock(() => this.loadJobsUnlocked());
	}

	private loadJobsUnlocked(): CronJobRecord[] {
		const path = this.pathsValue.jobsFile;
		if (!existsSync(path)) return [];
		let parsed: unknown;
		try {
			parsed = JSON.parse(readFileSync(path, "utf8"));
		} catch {
			return []; // unreadable junk behaves like an empty store; save rebuilds
		}
		if (!Array.isArray(parsed)) return [];
		return parsed.filter(
			(j): j is CronJobRecord =>
				typeof j === "object" &&
				j !== null &&
				typeof (j as CronJobRecord).id === "string",
		);
	}

	async saveJobs(jobs: CronJobRecord[]): Promise<void> {
		return this.lock.withJobsLock(() => this.saveJobsUnlocked(jobs));
	}

	private saveJobsUnlocked(jobs: CronJobRecord[]): void {
		this.ensureDirs();
		const path = this.pathsValue.jobsFile;
		const tmp = `${path}.${randomUUID()}.tmp`;
		writeFileSync(tmp, `${JSON.stringify(jobs, null, 2)}\n`, "utf8");
		try {
			renameSync(tmp, path); // atomic swap — readers see old or new bytes
		} catch (err) {
			try {
				unlinkSync(tmp);
			} catch {
				/* best-effort cleanup */
			}
			throw err;
		}
	}

	/** Run a read-modify-write cycle under the full critical-section lock. */
	async mutate<T>(fn: (jobs: CronJobRecord[]) => T | Promise<T>): Promise<T> {
		return this.lock.withJobsLock(async () => {
			const jobs = this.loadJobsUnlocked();
			const result = await fn(jobs);
			this.saveJobsUnlocked(jobs);
			return result;
		});
	}

	// ------------------------------------------------------------------
	// create / update / pause / resume / remove
	// ------------------------------------------------------------------

	async createJob(input: CreateJobInput): Promise<CronJobRecord> {
		const schedule =
			typeof input.schedule === "string"
				? parseSchedule(input.schedule)
				: input.schedule;
		const now = this.clock.nowSeconds();
		const nextRun = computeNextRun(schedule, now, null);
		if (schedule.kind === "once" && nextRun === null) {
			// Parity of create_job's rejection (#59395 shape): a one-shot more
			// than ONESHOT_GRACE_SECONDS in the past would persist
			// next_run_at=None with state=scheduled — a ghost job that never
			// fires. Reject with an explicit error instead.
			throw new OneShotGraceError(epochToIso(schedule.runAtSeconds), 120);
		}
		const id = randomUUID().replace(/-/g, "").slice(0, 12);
		const name = input.name?.trim() || input.prompt.slice(0, 50).trim();
		const record: CronJobRecord = {
			id,
			name,
			prompt: input.prompt,
			schedule: toStoredSchedule(schedule),
			enabled: true,
			state: "scheduled",
			created_at: epochToIso(now),
			next_run_at: nextRun === null ? null : epochToIso(nextRun),
			...(input.deliver !== undefined ? { deliver: input.deliver } : {}),
			...(input.attachToSession !== undefined
				? { attach_to_session: input.attachToSession }
				: {}),
		};
		await this.mutate((jobs) => {
			jobs.push(record);
		});
		return structuredClone(record);
	}

	async updateJob(
		jobId: string,
		patch: Partial<
			Pick<CronJobRecord, "prompt" | "deliver" | "attach_to_session">
		> & {
			schedule?: string | CronSchedule;
		},
	): Promise<CronJobRecord | null> {
		return this.mutate((jobs) => {
			const job = jobs.find((j) => j.id === jobId);
			if (job === undefined) return null;
			if (patch.prompt !== undefined) job.prompt = patch.prompt;
			if (patch.deliver !== undefined) job.deliver = patch.deliver;
			if (patch.attach_to_session !== undefined)
				job.attach_to_session = patch.attach_to_session;
			if (patch.schedule !== undefined) {
				const schedule =
					typeof patch.schedule === "string"
						? parseSchedule(patch.schedule)
						: patch.schedule;
				const now = this.clock.nowSeconds();
				const nextRun = computeNextRun(schedule, now, null);
				if (
					schedule.kind === "once" &&
					nextRun === null &&
					job.state !== "paused"
				) {
					throw new OneShotGraceError(epochToIso(schedule.runAtSeconds), 120);
				}
				job.schedule = toStoredSchedule(schedule);
				job.next_run_at = nextRun === null ? null : epochToIso(nextRun);
			}
			return structuredClone(job);
		});
	}

	async pauseJob(
		jobId: string,
		reason?: string,
	): Promise<CronJobRecord | null> {
		return this.mutate((jobs) => {
			const job = jobs.find((j) => j.id === jobId);
			if (job === undefined) return null;
			job.enabled = false;
			job.state = "paused";
			job.paused_at = epochToIso(this.clock.nowSeconds());
			job.paused_reason = reason ?? null;
			return structuredClone(job);
		});
	}

	async resumeJob(jobId: string): Promise<CronJobRecord | null> {
		return this.mutate((jobs) => {
			const job = jobs.find((j) => j.id === jobId);
			if (job === undefined) return null;
			const schedule = storedScheduleToCron(job.schedule);
			const now = this.clock.nowSeconds();
			const nextRun = computeNextRun(schedule, now, null);
			if (nextRun === null && schedule.kind === "once") {
				throw new OneShotGraceError(epochToIso(schedule.runAtSeconds), 120);
			}
			job.enabled = true;
			job.state = "scheduled";
			job.paused_at = null;
			job.paused_reason = null;
			job.next_run_at = nextRun === null ? null : epochToIso(nextRun);
			return structuredClone(job);
		});
	}

	async removeJob(jobId: string): Promise<boolean> {
		return this.mutate((jobs) => {
			const before = jobs.length;
			const after = jobs.filter((j) => j.id !== jobId);
			if (after.length === before) return false;
			jobs.length = 0;
			jobs.push(...after);
			return true;
		});
	}

	async getJob(jobId: string): Promise<CronJobRecord | null> {
		const jobs = await this.loadJobs();
		const found = jobs.find((j) => j.id === jobId);
		return found === undefined ? null : structuredClone(found);
	}

	async listJobs(): Promise<CronJobRecord[]> {
		return this.loadJobs();
	}

	// ------------------------------------------------------------------
	// due computation: catch-up vs fast-forward (07 §5.2 catchup window)
	// ------------------------------------------------------------------

	async getDueJobs(now?: number): Promise<GetDueReport> {
		const nowSec = now ?? this.clock.nowSeconds();
		return this.mutate((jobs) => {
			const report: GetDueReport = { due: [], fastForwarded: [] };
			for (const job of jobs) {
				if (!isRunnable(job)) continue;
				const nextRaw = job.next_run_at;
				if (nextRaw === null || nextRaw === undefined) continue;
				let nextSec = isoToEpoch(nextRaw);
				if (nextSec === null) continue;

				const schedule = storedScheduleToCron(job.schedule);
				if (schedule.kind === "once") {
					// Terminal after its successful fire: last_run_at makes a
					// one-shot permanently ineligible (07 §5.3). A still-unrun
					// one-shot within its grace window stays due at run_at; past
					// grace it can never fire — mark terminal so it stops
					// accumulating ticks as a ghost.
					const lastRun =
						job.last_run_at !== null && job.last_run_at !== undefined
							? isoToEpoch(job.last_run_at)
							: null;
					if (lastRun !== null) continue;
					const graceAgedOut =
						schedule.runAtSeconds < nowSec - 120 && nextSec <= nowSec;
					if (graceAgedOut) {
						job.enabled = false;
						job.state = "completed";
						continue;
					}
					report.due.push({ job: structuredClone(job), fastForwarded: false });
					continue;
				}

				// Persisted-state stale-error recovery (t_8b5480b3 parity): a
				// recurring job parked in the FUTURE whose persisted state shows
				// last_status=error with last_run_at older than a full cadence has
				// been sitting wedged (errored once, next_run_at re-armed forward
				// by markJobRun, never re-dispatched — invisible to every other
				// sweep). Re-arm so the job re-dispatches WITHOUT force-run/resume:
				//   * interval → now — no excluded times, immediate retry is a
				//     legal fire and lands DUE THIS TICK below;
				//   * cron → the next LEGAL occurrence from now — re-arming to now
				//     would fire at instants the expression explicitly excludes; a
				//     correctly-parked cron value is left as-is.
				if (nextSec > nowSec && isStaleErrorRecurring(job, schedule, nowSec)) {
					const recoveredNext =
						schedule.kind === "interval"
							? nowSec
							: computeNextRun(schedule, nowSec, null);
					if (recoveredNext !== null && recoveredNext < nextSec) {
						this.recordPersistedErrorRecoveryUnlocked(job, nextRaw, nowSec); // count + JSONL telemetry BEFORE the row mutates
						job.next_run_at = epochToIso(recoveredNext);
						nextSec = recoveredNext;
					}
				}
				if (nextSec > nowSec) continue;

				// Recurring: stale slot past the catchup grace window ⇒ skip the
				// accumulated missed runs but still execute once NOW (fast-
				// forward); within grace ⇒ plain catch-up of the missed slot.
				const grace = catchupGraceSeconds(schedule, nowSec);
				let fastForwarded = false;
				if (nowSec - nextSec > grace) {
					const newNext = computeNextRun(schedule, nowSec, null);
					if (newNext !== null) {
						job.next_run_at = epochToIso(newNext);
						report.fastForwarded.push(job.id);
						fastForwarded = true;
						this.recordCatchUpOccurrenceUnlocked();
					}
				}
				report.due.push({ job: structuredClone(job), fastForwarded });
			}
			return report;
		});
	}

	/**
	 * Advance next_run_at for recurring due jobs BEFORE execution (at-most-once
	 * parity of advance_next_runs: the bump happens under the lock before any
	 * run starts; mark_job_run re-anchors from completion time afterwards).
	 */
	async advanceNextRuns(
		jobIds: readonly string[],
		now?: number,
	): Promise<number> {
		const nowSec = now ?? this.clock.nowSeconds();
		const wanted = new Set(jobIds);
		return this.mutate((jobs) => {
			let advanced = 0;
			for (const job of jobs) {
				if (!wanted.has(job.id)) continue;
				if (job.schedule.kind === "once") continue;
				const schedule = storedScheduleToCron(job.schedule);
				const next = computeNextRun(schedule, nowSec, null);
				if (next !== null) {
					job.next_run_at = epochToIso(next);
					advanced++;
				}
			}
			return advanced;
		});
	}

	// ------------------------------------------------------------------
	// Fire ownership (execution tokens; 07 §5.2 fire ownership row)
	// ------------------------------------------------------------------

	/**
	 * Atomically claim a job for ONE fire. Returns the claimed record (with
	 * the fresh per-acquisition token minted into fire_claim.by) or null when
	 * somebody else holds a fresh claim / the job is not runnable.
	 */
	async claimJobForFire(
		jobId: string,
		options: { ttlSeconds?: number; now?: number } = {},
	): Promise<CronJobRecord | null> {
		const ttl = options.ttlSeconds ?? FIRE_CLAIM_TTL_SECONDS;
		const nowSec = options.now ?? this.clock.nowSeconds();
		return this.mutate((jobs) => {
			const job = jobs.find((j) => j.id === jobId);
			if (job === undefined || !isRunnable(job)) return null;
			const existing = job.fire_claim;
			if (existing !== null && existing !== undefined) {
				const claimedAt = isoToEpoch(existing.at);
				if (claimedAt !== null) {
					// Bounded on BOTH sides (#60703 shape): a FUTURE-dated claim
					// (clock skew) counts as stale, never fresh-forever.
					const age = nowSec - claimedAt;
					if (age >= 0 && age < ttl) return null; // someone holds it
				}
			}
			// Per-acquisition token: reclaiming our own stale lease mints a NEW
			// token so a previous runner can never heartbeat the new claim.
			const owner = `${process.pid}:${randomUUID()}`;
			job.fire_claim = { at: epochToIso(nowSec), by: owner };
			if (job.schedule.kind !== "once") {
				// At-most-once bump so a stale re-delivery for the old time
				// cannot re-fire (parity of claim_job_for_fire).
				const schedule = storedScheduleToCron(job.schedule);
				const nxt = computeNextRun(schedule, nowSec, null);
				if (nxt !== null) job.next_run_at = epochToIso(nxt);
			}
			const claimed = structuredClone(job);
			claimed.fire_claim = { at: epochToIso(nowSec), by: owner };
			return claimed;
		});
	}

	/** Refresh OUR claim; false = claim lost mid-run (first-class condition). */
	async heartbeatRunClaim(
		jobId: string,
		expectedOwner: string,
		options: { ttlSeconds?: number; now?: number } = {},
	): Promise<boolean> {
		const ttl = options.ttlSeconds ?? FIRE_CLAIM_TTL_SECONDS;
		const nowSec = options.now ?? this.clock.nowSeconds();
		return this.mutate((jobs) => {
			const job = jobs.find((j) => j.id === jobId);
			const claim = job?.fire_claim;
			if (
				job === undefined ||
				claim === null ||
				claim === undefined ||
				claim.by !== expectedOwner
			)
				return false;
			const claimedAt = isoToEpoch(claim.at);
			if (claimedAt !== null) {
				const age = nowSec - claimedAt;
				if (age < 0 || age >= ttl) return false; // expired — not ours anymore
			}
			claim.at = epochToIso(nowSec);
			return true;
		});
	}

	/** Clear only OUR claim — a stale release can never free a newer claim. */
	async clearRunClaim(jobId: string, expectedOwner: string): Promise<boolean> {
		return this.mutate((jobs) => {
			const job = jobs.find((j) => j.id === jobId);
			const claim = job?.fire_claim;
			if (
				job === undefined ||
				claim === null ||
				claim === undefined ||
				claim.by !== expectedOwner
			)
				return false;
			job.fire_claim = null;
			return true;
		});
	}

	// ------------------------------------------------------------------
	// mark_job_run port
	// ------------------------------------------------------------------

	/**
	 * Record a finished run: stamps last_run_at/last_status/error fields,
	 * clears the fire claim, re-anchors next_run_at FROM COMPLETION TIME for
	 * recurring schedules, and makes one-shots terminal (enabled=false,
	 * state=completed — inspectable record retained, 07 §5.3). With
	 * expectedFireOwner set, a changed claim owner DISCARDS the completion
	 * (stale-runner no-double-write) and returns false.
	 */
	async markJobRun(
		jobId: string,
		input: MarkRunInput,
		now?: number,
	): Promise<boolean> {
		const nowSec = now ?? this.clock.nowSeconds();
		const nowIso = epochToIso(nowSec);
		return this.mutate((jobs) => {
			const job = jobs.find((j) => j.id === jobId);
			if (job === undefined) return false;
			if (input.expectedFireOwner !== undefined) {
				const claim = job.fire_claim;
				if (
					claim === null ||
					claim === undefined ||
					claim.by !== input.expectedFireOwner
				) {
					return false; // discard stale completion — winner owns the record
				}
			}
			job.last_run_at = nowIso;
			const status: NonNullable<CronJobRecord["last_status"]> =
				input.status ?? (input.success ? "ok" : "error");
			job.last_status = status;
			job.last_error = input.success ? null : (input.error ?? null);
			job.last_delivery_error = input.deliveryError ?? null;
			job.failure_streak = input.success ? 0 : (job.failure_streak ?? 0) + 1;
			job.fire_claim = null;

			const schedule = storedScheduleToCron(job.schedule);
			// Re-anchor FROM COMPLETION TIME (mark_job_run parity): interval jobs
			// chain off the actual finish, cron jobs measure from it; a null next
			// (one-shot) persists as null exactly like compute_next_run returning
			// None before the terminal branch.
			const next = computeNextRun(schedule, nowSec, nowSec);
			job.next_run_at = next === null ? null : epochToIso(next);
			if (next === null) {
				if (schedule.kind === "cron" || schedule.kind === "interval") {
					// Recurring jobs are NEVER silently disabled when the next
					// slot cannot be computed (#16265 parity): loud error state,
					// still enabled.
					job.state = "error";
					if (!job.last_error) {
						job.last_error =
							"Failed to compute next run for recurring schedule";
					}
				} else {
					job.enabled = false;
					job.state = "completed"; // one-shot terminal
				}
			} else if (job.state !== "paused") {
				job.state = "scheduled";
			}
			return true;
		});
	}

	// ------------------------------------------------------------------
	// persisted-error wedge recovery telemetry (parity of
	// _record_persisted_error_recovery / get_persisted_error_recovery_stats)
	// ------------------------------------------------------------------

	/** Count + append one stale-error re-arm to the JSONL sink. Telemetry
	 * NEVER breaks a tick (best-effort append, parity try/except). */
	private recordPersistedErrorRecoveryUnlocked(
		job: CronJobRecord,
		previousNextRunAt: string,
		nowSec: number,
	): void {
		const entry: PersistedErrorRecoveryEntry = {
			job_id: job.id,
			name: job.name || job.id,
			previous_next_run_at: previousNextRunAt,
			rearmed_at: epochToIso(nowSec),
		};
		this.recoveryCount++;
		this.recentRecoveries.push(entry);
		if (this.recentRecoveries.length > PERSISTED_ERROR_RECOVERY_HISTORY) {
			this.recentRecoveries.splice(
				0,
				this.recentRecoveries.length - PERSISTED_ERROR_RECOVERY_HISTORY,
			);
		}
		try {
			this.ensureDirs();
			appendFileSync(
				join(this.pathsValue.cronDir, PERSISTED_ERROR_RECOVERIES_FILENAME),
				`${JSON.stringify(entry)}\n`,
				"utf8",
			);
		} catch {
			/* never let telemetry break a tick */
		}
	}

	/** Probe-visible snapshot of persisted-error recoveries. */
	getPersistedErrorRecoveryStats(): PersistedErrorRecoveryStats {
		return {
			persisted_error_recoveries: this.recoveryCount,
			recent: [...this.recentRecoveries],
		};
	}

	// ------------------------------------------------------------------
	// catch-up telemetry (parity of record_catch_up_occurrence)
	// ------------------------------------------------------------------

	private recordCatchUpOccurrenceUnlocked(): void {
		try {
			this.ensureDirs();
			const path = join(this.pathsValue.cronDir, "catch_up_occurrences");
			const prior = existsSync(path)
				? Number(readFileSync(path, "utf8").trim()) || 0
				: 0;
			writeFileSync(path, String(prior + 1), "utf8");
		} catch {
			/* never let telemetry break a tick */
		}
	}

	async catchUpOccurrenceCount(): Promise<number> {
		const path = join(this.pathsValue.cronDir, "catch_up_occurrences");
		if (!existsSync(path)) return 0;
		return Number(readFileSync(path, "utf8").trim()) || 0;
	}
}

/** Runnable gate: enabled AND not paused AND not completed (parity of
 * is_job_runnable — a half-paused record must never claim/fire). */
function isRunnable(job: CronJobRecord): boolean {
	return job.enabled && job.state !== "paused" && job.state !== "completed";
}
