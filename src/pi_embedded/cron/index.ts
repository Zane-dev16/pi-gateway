// pi_embedded/cron/index.ts — public surface of the cron ticker.
//
// Layering: pi_embedded (rank 4) imports pi_state / pi_agent_core downward
// only; the lifecycle composition root (pi_gateway, rank 3) may import THIS
// module — never the reverse. Runner internals (pi_gateway/lifecycle/**)
// stay invisible here by gate; the service handle is structural.

export type { CronClock } from "./clock.js";
export { systemCronClock } from "./clock.js";

export {
	CATCHUP_GRACE_MAX_SECONDS,
	CATCHUP_GRACE_MIN_SECONDS,
	ONESHOT_GRACE_SECONDS,
	catchupGraceSeconds,
	computeNextRun,
	cronNextAfter,
	epochToIso,
	isoToEpoch,
	parseDuration,
	parseSchedule,
	recoverableOneshotRunAt,
	scheduleCadenceSeconds,
	type CronSchedule,
} from "./schedule.js";

export {
	CronJobStore,
	OneShotGraceError,
	defaultCronStorePaths,
	storedScheduleToCron,
	type CreateJobInput,
	type CronDeliveryTarget,
	type CronJobRecord,
	type CronStorePaths,
	type DueJob,
	type GetDueReport,
	type JobState,
	type MarkRunInput,
	type StoredSchedule,
} from "./store.js";

export {
	JobsFileLock,
	JobsLockTimeoutError,
	JOBS_LOCK_TIMEOUT_MS,
} from "./jobs-lock.js";

export {
	TickLock,
	TickLockAcquisitionError,
	backoffWaitSeconds,
	classifyTickAcquireFailure,
	isFdExhaustion,
	isLockContention,
	noteTickFailure,
	reclaimFdsBestEffort,
	DEFAULT_TICK_INTERVAL_SECONDS as DEFAULT_TICK_LOCK_INTERVAL_SENTINEL,
	EMFILE_BACKOFF_MAX_SECONDS,
	TICK_LOCK_FILENAME,
	type AcquireResult,
	type TickFailureKind,
} from "./tick-lock.js";

export {
	runWithClaimHeartbeat,
	CLAIM_HEARTBEAT_POLL_SECONDS,
	type ClaimHeartbeatOptions,
	type ClaimHeartbeatResult,
} from "./claim-heartbeat.js";

export {
	applyWrap,
	cleanedCronOutput,
	deliverCronResult,
	matchesOrigin,
	resolveMirrorEnabled,
	wrapCronResponse,
	type CronJobLike,
	type CronWrapConfig,
	type DeliverCronResultInput,
	type DeliverCronResultReport,
	type DeliverySink,
	type MirrorAppender,
	type MirrorConfig,
} from "./delivery.js";

export {
	CRON_SESSION_ENV,
	CronMemoryPolicyError,
	CronTurnExecutor,
	constructCronAgentPlan,
	cronExecutorAsRunner,
	cronSessionId,
	isCronSessionContext,
	runInCronSession,
	type CronAgentConstruction,
	type CronRunnerSurface,
	type CronTurnExecutorOptions,
	type CronTurnResult,
	type ExecutorRunnerAdapterOptions,
	type RunCronTurnInput,
} from "./executor.js";

export {
	CronScheduler,
	startCronTickerOrDegraded,
	DEFAULT_TICK_INTERVAL_SECONDS,
	type CronRunReport,
	type CronSchedulerOptions,
	type CronServiceHandle,
	type CronStartupResult,
	type CronTickReport,
	type CronRunStatus,
	type ScheduledJobRunner,
	type RunnerOutcome,
} from "./scheduler.js";

export {
	ensureCronSessionRow,
	newOriginSessionId,
	stateStoreMirrorAppender,
} from "./transcript-sink.js";

export {
	CRON_TICKER_SERVICE_NAME,
	cronTickerServiceEntry,
} from "./stage-entry.js";
