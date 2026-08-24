// pi_embedded/cron/executor.ts — cron job turns run THROUGH the normal
// runner pipeline (DEC-023: the host pi agent loop is reused directly), with
// the DEC-012 memory policy made provable at the construction seam.
//
// Hermes anchors (READ-ONLY reference):
//   cron/scheduler.py run_job constructor call (~6020–6047): the cron agent
//     is constructed with `skip_memory=False` EXPLICITLY — memory ENABLED,
//     MEMORY.md/USER.md load, the memory tool resolves normally (DEC-012;
//     the reference repo's AGENTS.md "skip_memory=True" line is a documented
//     doc bug, never ported).
//   cron/scheduler.py:request_hard_interrupt → runner.interrupt analogue.
//
// DEC-012 in this codebase: memory on/off is decided at agent construction
// (GatewayAgentRunner's MemoryTurnHooks). The cron executor therefore
// declares its policy at construction — skipMemory MUST be false; anything
// else throws CronMemoryPolicyError BEFORE any turn runs (a future
// memory-off cron mode requires explicit DEC sign-off, never silent
// divergence). The construction plan is recorded on every run so tests can
// assert the constructor argument byte-for-byte, and an end-to-end harness
// run proves the memory hooks actually fire through the REAL pipeline.

import type { TurnOutcome } from "../../pi_agent_core/runner-types.js";

/** Structural runner surface consumed here (GatewayAgentRunner subset). */
export interface CronRunnerSurface {
	handleTurn(request: {
		sessionId: string;
		routingKey: string;
		text: string;
	}): Promise<TurnOutcome>;
	interrupt(sessionId: string): Promise<boolean>;
}

export class CronMemoryPolicyError extends Error {
	constructor(received: unknown) {
		super(
			`DEC-012 violation: cron agents run WITH memory (skip_memory=False). ` +
				`Refusing to construct a cron turn with skipMemory=${String(received)} — ` +
				`a memory-off cron mode requires an explicit signed-off DEC entry.`,
		);
		this.name = "CronMemoryPolicyError";
	}
}

/**
 * THE construction seam (parity of the AIAgent(... skip_memory=False)
 * constructor call). Always emits skipMemory:false; recorded verbatim into
 * run records so the DEC-012 contract test asserts what construction
 * ACTUALLY passed, not what a constant claims.
 */
export interface CronAgentConstruction {
	sessionId: string;
	skipMemory: false;
	platform: "cron";
}

export function constructCronAgentPlan(
	sessionId: string,
): CronAgentConstruction {
	return { sessionId, skipMemory: false, platform: "cron" };
}

/** Dedicated per-job session namespace (cron isolation: deliveries originate
 * in the job's OWN session, 07 §5.2). Deterministic per job id. */
export function cronSessionId(jobId: string): string {
	return `cron:${jobId}`;
}

export interface CronTurnExecutorOptions {
	/**
	 * DEC-012. Only `false` (or omitted) constructs an executor; `true`
	 * throws CronMemoryPolicyError at construction time.
	 */
	skipMemory?: false;
}

export interface RunCronTurnInput {
	jobId: string;
	prompt: string;
	/** Ensure the session row exists before prompting (resolution-chain stand-in). */
	ensureSession?: (sessionId: string) => void | Promise<void>;
	/** Called right before handleTurn with the recorded construction plan. */
	onConstructed?: (plan: CronAgentConstruction) => void;
}

export interface CronTurnResult {
	outcome: TurnOutcome;
	construction: CronAgentConstruction;
}

export class CronTurnExecutor {
	private readonly runner: CronRunnerSurface;
	/** Last construction plan emitted (observation point for contracts). */
	lastConstruction: CronAgentConstruction | null = null;

	constructor(
		runner: CronRunnerSurface,
		options: CronTurnExecutorOptions = {},
	) {
		// DEC-012 guard: reject any attempt to express memory-off cron turns
		// (a `true` can only arrive through an unsafe cast — that is precisely
		// the deviation this seam exists to stop, loudly, before any turn).
		const requested = (options as { skipMemory?: unknown }).skipMemory;
		if (requested !== undefined && requested !== false) {
			throw new CronMemoryPolicyError(requested);
		}
		this.runner = runner;
	}

	async run(input: RunCronTurnInput): Promise<CronTurnResult> {
		const sessionId = cronSessionId(input.jobId);
		const construction = constructCronAgentPlan(sessionId);
		if (construction.skipMemory !== false) {
			// Defense-in-depth: even if constructCronAgentPlan were ever edited
			// to emit something else, refuse loudly instead of running silent.
			throw new CronMemoryPolicyError(construction.skipMemory);
		}
		this.lastConstruction = construction;
		input.onConstructed?.(construction);
		await input.ensureSession?.(sessionId);
		const outcome = await this.runner.handleTurn({
			sessionId,
			routingKey: sessionId,
			text: input.prompt,
		});
		return { outcome, construction };
	}

	interrupt(jobId: string): Promise<boolean> {
		return this.runner.interrupt(cronSessionId(jobId));
	}
}

// -----------------------------------------------------------------------
// ScheduledJobRunner adapter (bridges into the scheduler's runner seam)
// -----------------------------------------------------------------------

import type { CronClock } from "./clock.js";
import { systemCronClock } from "./clock.js";
import type { TimestampActivityLog } from "./inactivity.js";
import type { RunnerOutcome, ScheduledJobRunner } from "./scheduler.js";

export interface ExecutorRunnerAdapterOptions {
	/** Ensure the cron session row exists before the turn (mint-once). */
	ensureSession?: (sessionId: string) => void | Promise<void>;
	/** Map a TurnOutcome to ok/output/error (default: finalized ⇒ ok). */
	toOutcome?: (outcome: TurnOutcome) => RunnerOutcome;
	/** Injected clock for activity stamps (same clock the scheduler runs on). */
	clock?: CronClock;
}

function defaultToOutcome(outcome: TurnOutcome): RunnerOutcome {
	const ok = outcome.exitReason === "finalized";
	return {
		ok,
		...(outcome.finalText !== undefined
			? { outputText: outcome.finalText }
			: {}),
		...(!ok
			? {
					error:
						outcome.errorMessage ??
						`turn ended with exitReason=${outcome.exitReason}`,
				}
			: {}),
	};
}

/**
 * Adapt a CronTurnExecutor to the scheduler's ScheduledJobRunner: touches the
 * job's activity log at turn start AND settlement so the inactivity bound
 * sees real pipeline progress, and routes every turn through the NORMAL
 * runner pipeline (DEC-023) with the DEC-012 construction recorded.
 */
export function cronExecutorAsRunner(
	executor: CronTurnExecutor,
	options: ExecutorRunnerAdapterOptions = {},
): ScheduledJobRunner {
	const clock = options.clock ?? systemCronClock;
	return {
		async run({ job, activity }) {
			activity.touch(clock.nowSeconds()); // turn started = activity
			const { outcome } = await executor.run({
				jobId: job.id,
				prompt: job.prompt,
				...(options.ensureSession !== undefined
					? { ensureSession: options.ensureSession }
					: {}),
			});
			activity.touch(clock.nowSeconds());
			const map = options.toOutcome ?? defaultToOutcome;
			return map(outcome);
		},
		interrupt: (jobId) => executor.interrupt(jobId),
	};
}
