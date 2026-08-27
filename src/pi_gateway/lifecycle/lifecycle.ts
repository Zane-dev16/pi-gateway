// pi_gateway/lifecycle/lifecycle.ts — the GatewayLifecycle engine: binding
// ten-stage startup, duplicate-instance guard wiring, graceful shutdown.
//
// Spec: /root/pi-gateway/01-architecture.md §3 (startup ordering; degraded
// per-service start), §3.2 (takeover handshake), §3.4 (shutdown);
// /root/pi-gateway/08-operations.md §1.1–§1.3. Whole-sequence parity anchor:
// gateway/run.py:start_gateway. Mechanics live in sibling modules whose
// headers carry the file:symbol anchors (markers, instance-guard, takeover,
// boot-fingerprint, status-stamp, shutdown).
//
// Skeleton posture (roadmap Phase 1 item 5): the stage FRAMEWORK is built
// now; adapters land in Phases 3+ through `stageBodies`. Stages 7–8 are
// WIRED (DEC-040): per-service entries registered via `registerService()` /
// the `services` option run inside their stage bodies with per-service loud
// degradation; stage 9 carries the SAME DEC-040 seam for ADAPTERS
// (registerAdapter / the `adapters` option): per-adapter instantiate → wire →
// connect, loud-disable per adapter, and retryable connect failures enqueue
// into the failed-platform queue feeding the supervised reconnect watcher
// (run.py:_failed_platforms + _platform_reconnect_watcher). The engine stays
// service/adapter-agnostic (structural entries, no pi_embedded imports —
// layering 01 §5.3).
//
// Boot choreography (run.py:start parity) runs POST-STAGE-9: clean-shutdown
// receipt vs unclean-session recovery, stuck-loop suspension (#7536), then
// boot sends + resume-pending scheduling under the inbound RESTORE GATE.
// The §1.3 backstops are armed here too: signal-path forensics (<10ms
// snapshot + detached diagnostic), an unref'd OS-level shutdown watchdog
// around every drain (hard-exit ≤ drain+60s), and the lifetime loop-liveness
// guard + heartbeat pair (3 missed probes ⇒ exit 75).

import { join } from "node:path";
import { readFileSync } from "node:fs";
import { resolvePiHome } from "../../pi_home.js";
import { StateStore, type StateStoreOptions } from "../../pi_state/index.js";
import {
	bootFingerprintValue,
	detectCodeSkew,
	recordBootFingerprint,
	shortFingerprintLabel,
	type FingerprintReader,
} from "./boot-fingerprint.js";
import {
	RuntimeLock,
	getRunningPid,
	recordHomeMatches,
	removePidFile,
	writePidFile,
	type RunningInstance,
} from "./instance-guard.js";
import { processStartTime } from "./process-info.js";
import { persistExitStatusPatch } from "./status-persist.js";
import {
	readRuntimeStatus,
	writeRuntimeStatus,
	type GatewayRuntimeState,
} from "./status-stamp.js";
import {
	SHUTDOWN_EXIT_CODES,
	classifySignalForSelf,
	executeDrain,
	persistsStopped,
	recoverPendingToDb,
	type DrainHooks,
	type DrainOutcome,
	type Logger,
	type RecoveryReport,
	type ShutdownClass,
} from "./shutdown.js";
import {
	SERVICE_STAGE_IDS,
	STAGE_IDS,
	canTransition,
	isOptionalStage,
	type ServiceStageId,
	type StageEvent,
	type StageId,
	type StartupState,
} from "./stages.js";
import {
	performTakeover,
	type TakeoverOptions,
	type TakeoverResult,
} from "./takeover.js";
import {
	cleanShutdownMarkerExists,
	consumeCleanShutdownMarker,
} from "./shutdown.js";
import {
	formatContextForLog,
	snapshotShutdownContext,
	spawnAsyncDiagnostic,
} from "./forensics.js";
import {
	armShutdownWatchdog,
	realTimerPort,
	resolveShutdownWatchdogDelayMs,
	shutdownWatchdogDumpPath,
	startLoopHeartbeat,
	startLoopLivenessGuard,
	type HeartbeatHandle,
	type LoopLivenessGuardHandle,
	type TimerPort,
} from "./watchdog.js";
import {
	FATAL_CONFIG_EXIT_CODE,
	FATAL_CONFIG_REASON_CODE,
	resolveCronDrainBudget,
} from "./restart.js";
import {
	runBootChoreography,
	type BootRecoveryHooks,
	type BootRecoveryReport,
} from "./boot-recovery.js";
import { createRestoreGate, type RestoreGate } from "./restore-gate.js";
import {
	FailedPlatformQueue,
	createReconnectWatcherService,
	type ReconnectHooks,
} from "./reconnect-watcher.js";
import type { CommandRegistry } from "../commands/registry.js";

/** Minimal supervised-service seam filled by Phases 5+ (cron/watchers/adapters). */
export interface ServiceHandle {
	name: string;
	stop?: () => Promise<void>;
	/**
	 * Cooperative-drain input (#60432/#82161): live count of in-flight work
	 * this service is driving (cron: job executions, get_running_job_ids
	 * parity). The shutdown drain polls it to wait in-flight ticks out on
	 * their OWN budget instead of killing them mid-run; absent ⇒ 0.
	 */
	inflightCount?: () => number;
}

/**
 * Classified result of ONE registered optional-stage service entry (DEC-040,
 * 01 §3.1 per-service degraded start). A throw inside start() is ALSO a
 * classification — the engine converts it to `{ ok:false, degraded:true }`.
 */
export interface ServiceStartOutcome {
	ok: boolean;
	/**
	 * true (with ok=false) ⇒ loud per-service DEGRADE — startup continues.
	 * Absent/falsy with ok=false ⇒ disabled/skipped: loud, but NOT a failure
	 * (e.g. an env gate off, or the singleton dispatcher role held elsewhere).
	 */
	degraded?: boolean;
	/** Machine-readable degrade/disable reason (carried in the loud log). */
	reason?: string;
	/** Stoppable handle; appended to ctx.services.{cron|watchers}. */
	handle?: ServiceHandle;
}

/**
 * One named service bound to an OPTIONAL stage (DEC-040). Structural contract
 * only: embedded services expose conforming entries WITHOUT importing this
 * module (pi_embedded may not reach pi_gateway/lifecycle — 01 §5.3); the
 * composition root registers them here before startup.
 */
export interface ServiceEntry {
	name: string;
	start(ctx: StageContext): Promise<ServiceStartOutcome> | ServiceStartOutcome;
}

/** Per-service degradation recorded during startup (DEC-040), in fire order. */
export interface ServiceDegradation {
	stage: ServiceStageId;
	service: string;
	reason: string;
}

export interface AdapterRecord {
	platform: string;
	enabled: boolean;
	/** Loud-disable reason (missing secret ⇒ loud disable, 08 §1.1 step 7). */
	reason?: string;
	handle?: ServiceHandle;
}

/**
 * Classified result of ONE registered stage-9 adapter entry (DEC-040 seam for
 * platform adapters — run.py:_create_adapter + _connect_adapter_with_timeout).
 * A throw inside start() classifies as a LOUD DISABLE (startup continues);
 * `retryable: true` marks a connect failure the RECONNECT WATCHER should keep
 * retrying instead of leaving the platform down until process restart.
 */
export interface AdapterStartOutcome {
	ok: boolean;
	/** true (with ok=false) ⇒ loud per-adapter DISABLE — startup continues. */
	degraded?: boolean;
	reason?: string;
	handle?: ServiceHandle;
	/** Retryable connect failure ⇒ queued for background reconnection. */
	retryable?: boolean;
}

/** One platform adapter bound to OPTIONAL stage 9 (DEC-040, structural). */
export interface AdapterEntry {
	platform: string;
	start(ctx: StageContext): Promise<AdapterStartOutcome> | AdapterStartOutcome;
}

export interface ConfigSnapshot {
	home: string;
	/** Raw config payload placeholder until the Phase-4/5 config system lands. */
	raw?: unknown;
}

/** Failure carrying a machine-parsable reason code (08 §3 rule). */
export class LifecycleError extends Error {
	readonly reasonCode: string;

	constructor(reasonCode: string, message: string) {
		super(message);
		this.name = "LifecycleError";
		this.reasonCode = reasonCode;
	}
}

/** Mutable per-boot context threaded through stage bodies. */
export interface StageContext {
	home: string;
	selfPid: number;
	log: Logger;
	config: ConfigSnapshot | null;
	fingerprint: string | null;
	existingInstance: RunningInstance | null;
	takeover: TakeoverResult | null;
	lock: RuntimeLock | null;
	store: StateStore | null;
	services: { cron: ServiceHandle[]; watchers: ServiceHandle[] };
	adapters: AdapterRecord[];
	/** The ONE built-and-frozen builtin command registry (07 §9) — constructed
	 *  at the stage-8 assembly seam and registered here so consumers reach it
	 *  without hand-built command lists anywhere downstream. */
	commands: CommandRegistry | null;
}

export type StageBody = (ctx: StageContext) => Promise<void>;

export interface LifecycleOptions {
	/**
	 * Profile home. Default resolves through the single accessor (01 §6) —
	 * entrypoints install overrides BEFORE any project import, so resolution
	 * here observes them.
	 */
	home?: string;
	/** `--replace` takeover authority (08 §1.1 stage 4). */
	replace?: boolean;
	/** Stage-body overrides: production seams (Phases 2–5) and test spies. */
	stageBodies?: Partial<Record<StageId, StageBody>>;
	/**
	 * Per-service optional-stage entries registered BEFORE startup (DEC-040).
	 * Equivalent to calling registerService() for each entry pre-startup.
	 */
	services?: Partial<Record<ServiceStageId, ServiceEntry[]>>;
	/**
	 * Per-adapter stage-9 entries registered BEFORE startup (DEC-040 seam,
	 * structure parity of run.py:start's platform loop + _create_adapter).
	 * Equivalent to calling registerAdapter() for each entry pre-startup.
	 */
	adapters?: AdapterEntry[];
	/**
	 * Reconnect backend for the failed-platform queue (run.py:
	 * _connect_adapter_with_timeout(is_reconnect=true)). Absent ⇒ queued
	 * platforms are retried against a loud no-op (queue still observable).
	 */
	reconnectHooks?: ReconnectHooks;
	stateStorePath?: string;
	stateStoreOptions?: StateStoreOptions;
	/** Active-turn grace window for the drain (default 0 — interrupting chat
	 *  turns is cheap; sessions are pre-marked resume_pending, #27856). */
	drainGraceMs?: number;
	/** Cron-only drain floor for in-flight ticks (#82161). Default
	 *  DEFAULT_CRON_DRAIN_TIMEOUT_MS (agent.cron_drain_timeout parity);
	 *  clamped to the watchdog leash by resolveCronDrainBudget. */
	cronDrainTimeoutMs?: number;
	logger?: Logger;
	/** Fingerprint reader override (tests / non-git installs). */
	fingerprintReader?: FingerprintReader;
	takeover?: Pick<
		TakeoverOptions,
		"graceTimeoutMs" | "pollIntervalMs" | "sleep" | "terminate"
	>;
	/** Hard-exit primitive for double-signal escalation (tests inject a spy). */
	hardExit?: (code: number) => void;
	selfPid?: number;
	selfStartTime?: number | null;
	/**
	 * Extra drain hooks merged UNDER the engine-provided ones (production
	 * runner seams: notify-active-sessions, resume-pending marks, lease
	 * release…). Engine defaults stay no-op so the skeleton drains cleanly.
	 */
	shutdownHooks?: Partial<DrainHooks>;
	/** Boot-session-recovery seams (boot-recovery.ts; wired by the runner). */
	bootRecovery?: BootRecoveryHooks;
	/** Injectable timers for every lifecycle-owned interval/timeout (tests). */
	timers?: TimerPort;
	/** Command-registry override (tests); default builds the SHIPPED builtins. */
	commandRegistry?: CommandRegistry;
	/** Signal-path forensics toggles (both default ON — 08 §1.3(b)). */
	forensics?: { snapshot?: boolean; spawnDiagnostic?: boolean };
	/** Lifetime loop-liveness guard config (gateway.loop_watchdog opt-out). */
	loopWatchdog?: {
		enabled?: boolean;
		probeIntervalMs?: number;
		probeTimeoutMs?: number;
		maxStrikes?: number;
	};
	/** Loop-heartbeat cadence config (08 §1.3(c) heartbeat task). */
	loopHeartbeat?: { enabled?: boolean; intervalMs?: number };
}

export interface StartupResult {
	ok: boolean;
	failedStage: StageId | null;
	error: string | null;
	reasonCode: string | null;
	degradedStages: StageId[];
	trace: StageEvent[];
	home: string;
	durationMs: number;
	/** Process exit code this abort implies (null on success). Fatal-config
	 *  aborts exit 78 (EX_CONFIG); any other required-stage abort exits 1. */
	exitCode: number | null;
}

function stderrLogger(): Logger {
	const line = (
		level: string,
		message: string,
		meta?: Record<string, unknown>,
	) => {
		let suffix = "";
		if (meta !== undefined) {
			try {
				suffix =
					" " +
					Object.entries(meta)
						.map(
							([k, v]) =>
								`${k}=${typeof v === "string" ? v : JSON.stringify(v)}`,
						)
						.join(" ");
			} catch {
				suffix = "";
			}
		}
		try {
			process.stderr.write(`[pi-gateway] ${level} ${message}${suffix}\n`);
		} catch {
			/* stderr unavailable */
		}
	};
	return {
		info: (m, meta) => line("INFO", m, meta),
		warn: (m, meta) => line("WARN", m, meta),
		error: (m, meta) => line("ERROR", m, meta),
	};
}

let cachedPkgVersion: string | null | undefined;
/** code_version stamp; degrades to null rather than failing a status write. */
function packageVersion(): string | null {
	if (cachedPkgVersion !== undefined) return cachedPkgVersion;
	try {
		const pkgUrl = new URL("../../../package.json", import.meta.url);
		const parsed = JSON.parse(readFileSync(pkgUrl, "utf8")) as {
			version?: unknown;
		};
		cachedPkgVersion =
			typeof parsed.version === "string" ? parsed.version : null;
	} catch {
		cachedPkgVersion = null;
	}
	return cachedPkgVersion;
}

const DEFAULT_DRAIN_GRACE_MS = 0;

/**
 * Cron-only drain floor for in-flight ticks (parity of Hermes config default
 * agent.cron_drain_timeout = 30s): a chat turn interrupted by a restart is
 * announced and resumable, but an interrupted cron run lands in jobs.json as
 * a permanent failure nobody is waiting on, so it must not inherit the 0s
 * chat budget (#82161). Clamped at runtime to the shutdown-watchdog leash
 * minus the cleanup reserve (restart.ts:resolveCronDrainBudget) so raising
 * it past ~50s has no effect; 0 opts out (legacy: cron drains on the chat
 * budget). Read ONCE per drain (DEC-013).
 */
export const DEFAULT_CRON_DRAIN_TIMEOUT_MS = 30_000;

/**
 * Post-drain cooperative ticker-join bound (parity of run.py's
 * _CRON_SHUTDOWN_DRAIN_TIMEOUT = 65.0): after the FULL drain completes, the
 * cron handle stop is awaited at most this long before the join gives up
 * with a dropped-delivery warning (#58818) — daemon-thread give-up parity,
 * never a hanging exit.
 */
export const CRON_SHUTDOWN_DRAIN_TIMEOUT_MS = 65_000;

/** Drain poll cadence (run.py:_drain_active_agents asyncio.sleep(0.1)). */
const CRON_DRAIN_POLL_MS = 100;

export class GatewayLifecycle {
	readonly events: StageEvent[] = [];
	private readonly opts: LifecycleOptions;
	private readonly homeValue: string;
	private readonly selfPid: number;
	private readonly log: Logger;
	private readonly bodies: Partial<Record<StageId, StageBody>>;
	private readonly serviceEntries: Record<ServiceStageId, ServiceEntry[]> = {
		cron_scheduler: [],
		embedded_watchers: [],
	};
	private readonly adapterEntries: AdapterEntry[] = [];
	/** Failed-platform retry queue feeding the reconnect watcher (stage 9 in). */
	readonly failedPlatforms: FailedPlatformQueue;
	/** Inbound-dispatch gate held closed during boot restore (run.py parity). */
	readonly restoreGate: RestoreGate = createRestoreGate();
	private bootReportValue: BootRecoveryReport | null = null;
	private livenessGuard: LoopLivenessGuardHandle | null = null;
	private loopHeartbeat: HeartbeatHandle | null = null;
	private readonly serviceDegradations: ServiceDegradation[] = [];
	private readonly completedStages = new Set<StageId>();
	private readonly degradedStages = new Set<StageId>();

	private ctx: StageContext;
	private ownedLock: RuntimeLock | null = null;
	private ownedStore: StateStore | null = null;
	private startupResult: StartupResult | null = null;
	private readonly timers: TimerPort;

	private _state: StartupState = "idle";
	private pendingSlots: unknown[] = [];
	private signalCount = 0;
	private signalInitiatedShutdown = false;
	private shutdownClass: ShutdownClass | null = null;
	private shutdownOutcome: DrainOutcome | null = null;
	private shutdownInFlight: Promise<DrainOutcome> | null = null;
	private shutdownWaiters: Array<(outcome: DrainOutcome) => void> = [];
	private escalated = false;

	constructor(options: LifecycleOptions = {}) {
		this.opts = options;
		this.log = options.logger ?? stderrLogger();
		this.homeValue = options.home ?? resolvePiHome();
		this.selfPid = options.selfPid ?? process.pid;
		this.bodies = options.stageBodies ?? {};
		this.timers = options.timers ?? realTimerPort();
		this.failedPlatforms = new FailedPlatformQueue(() => this.timers.nowMs());
		for (const stage of SERVICE_STAGE_IDS) {
			for (const entry of options.services?.[stage] ?? []) {
				this.registerService(stage, entry);
			}
		}
		for (const entry of options.adapters ?? []) {
			this.registerAdapter(entry);
		}
		this.ctx = {
			home: this.homeValue,
			selfPid: this.selfPid,
			log: this.log,
			config: null,
			fingerprint: null,
			existingInstance: null,
			takeover: null,
			lock: null,
			store: null,
			services: { cron: [], watchers: [] },
			adapters: [],
			commands: null,
		};
	}

	get home(): string {
		return this.homeValue;
	}

	get state(): StartupState {
		return this._state;
	}

	get store(): StateStore | null {
		return this.ownedStore ?? this.ctx.store;
	}

	get degraded(): StageId[] {
		return [...this.degradedStages];
	}

	/** Per-service degradations recorded during startup (DEC-040), in order. */
	get degradedServices(): ReadonlyArray<ServiceDegradation> {
		return [...this.serviceDegradations];
	}

	/** Boot-recovery report once the post-stage-9 choreography has run. */
	get bootReport(): BootRecoveryReport | null {
		return this.bootReportValue;
	}

	/**
	 * Register one per-service optional-stage entry (DEC-040). Entries run when
	 * their stage body executes — AFTER every earlier required stage succeeded,
	 * in REGISTRATION order, each isolated from its siblings' failures.
	 */
	registerService(stage: ServiceStageId, entry: ServiceEntry): this {
		this.serviceEntries[stage].push(entry);
		return this;
	}

	/**
	 * Register one stage-9 adapter entry (DEC-040 seam). Entries run inside the
	 * platform_adapters body — instantiate → wire handlers → connect per
	 * adapter, each isolated; a missing secret / throwing start is a LOUD
	 * DISABLE of that adapter only (08 §1.1 step 7).
	 */
	registerAdapter(entry: AdapterEntry): this {
		this.adapterEntries.push(entry);
		return this;
	}

	/**
	 * Build the supervised reconnect-watcher entry bound to THIS lifecycle's
	 * failed-platform queue (register into embedded_watchers via
	 * registerService("embedded_watchers", lifecycle.reconnectWatcherService())).
	 */
	get reconnectWatcherService(): ServiceEntry {
		return createReconnectWatcherService({
			queue: this.failedPlatforms,
			hooks: this.opts.reconnectHooks ?? {
				reconnect(platform) {
					// No backend wired yet — loud no-op keeps queue observable.
					throw new Error(`no reconnect backend registered for ${platform}`);
				},
			},
			logger: this.log,
			timer: this.timers,
		});
	}

	/** True when an UNEXPECTED signal initiated the shutdown (08 §1.2 mirror). */
	get unexpectedSignalInitiated(): boolean {
		return this.signalInitiatedShutdown;
	}

	/** In-memory pending-message slots drained by the shutdown backstop. */
	registerPendingMessage(payload: unknown): void {
		this.pendingSlots.push(payload);
	}

	get pendingCount(): number {
		return this.pendingSlots.length;
	}

	private transition(to: StartupState): void {
		if (!canTransition(this._state, to)) {
			throw new LifecycleError(
				"invalid_state_transition",
				`cannot transition ${this._state} → ${to}`,
			);
		}
		this._state = to;
	}

	private bodyFor(id: StageId): StageBody {
		const override = this.bodies[id];
		if (override) return override;
		return this.defaultBody(id);
	}

	// Dispatch only — every default body is a named private method so stage
	// mechanics stay reviewable in isolation (and nothing declares inside the
	// switch arms).
	private defaultBody(id: StageId): StageBody {
		switch (id) {
			case "profile_override":
				return (ctx) => this.stageProfileOverride(ctx);
			case "load_config":
				return (ctx) => this.stageLoadConfig(ctx);
			case "boot_fingerprint":
				return (ctx) => this.stageBootFingerprint(ctx);
			case "duplicate_guard":
				return (ctx) => this.stageDuplicateGuard(ctx);
			case "runtime_lock":
				return (ctx) => this.stageRuntimeLock(ctx);
			case "open_state_db":
				return (ctx) => this.stageOpenStateDb(ctx);
			case "cron_scheduler":
				return (ctx) => this.stageCronScheduler(ctx);
			case "embedded_watchers":
				return (ctx) => this.stageEmbeddedWatchers(ctx);
			case "platform_adapters":
				return (ctx) => this.stagePlatformAdapters(ctx);
			case "runtime_identity":
				return (ctx) => this.stageRuntimeIdentity(ctx);
			default: {
				// STAGE_IDS is the closed universe; a new StageId without a body is
				// a programmer error, not a runtime condition.
				throw new LifecycleError(
					"stage_body_missing",
					`no default body for stage ${String(id)}`,
				);
			}
		}
	}

	private async stageProfileOverride(ctx: StageContext): Promise<void> {
		// 01 §6 step 1–2: the entrypoint installed the override before any
		// project import; this stage VALIDATES the discipline (a home must
		// resolve through the single accessor — never a hardcoded ~/.pi).
		if (!ctx.home || ctx.home.trim() === "") {
			throw new LifecycleError("home_unresolved", "PI_HOME did not resolve");
		}
		ctx.log.info("profile home resolved", { home: ctx.home });
	}

	private async stageLoadConfig(ctx: StageContext): Promise<void> {
		// 08 §1.1 step 2: load + validate config. The behavior/secrets split
		// arrives with the Phase-4 security work; the skeleton records the
		// resolved-home snapshot every later stage reads.
		ctx.config = { home: ctx.home };
	}

	private async stageBootFingerprint(ctx: StageContext): Promise<void> {
		// 08 §1.1 step 5 / 01 §3.3: snapshot while imports still match disk.
		ctx.fingerprint = await recordBootFingerprint(this.opts.fingerprintReader);
	}

	private async stageDuplicateGuard(ctx: StageContext): Promise<void> {
		// 08 §1.1 step 4 / 01 §3.2: PID-file-scoped duplicate guard.
		const existing = getRunningPid(ctx.home, { selfPid: ctx.selfPid });
		if (existing === null || existing.pid === ctx.selfPid) {
			ctx.existingInstance = existing;
			return;
		}
		if (this.opts.replace !== true) {
			throw new LifecycleError(
				"duplicate_instance",
				`another gateway instance is already running (pid ${existing.pid}); use --replace or stop it first`,
			);
		}
		// Destructive-action authority check (#89315 parity) — see
		// recordHomeMatches. Refusals name the competing home loudly.
		if (!recordHomeMatches(existing.record, ctx.home)) {
			const recordedHome =
				typeof existing.record.pi_home === "string"
					? existing.record.pi_home.trim()
					: "";
			ctx.log.error?.(
				recordedHome === ""
					? `refusing --replace: pid record predates pi_home stampings; ownership of pid ${existing.pid} unprovable`
					: `refusing --replace: pid record belongs to a different pi home (${recordedHome}, ours ${ctx.home}); remove the stale pid record or stop the owning profile explicitly`,
			);
			throw new LifecycleError(
				"replace_refused",
				recordedHome === ""
					? `could not prove ownership of running instance (pid ${existing.pid}): legacy pid record without pi_home`
					: `running instance (pid ${existing.pid}) belongs to another pi home (${recordedHome})`,
			);
		}
		ctx.log.info("replacing existing gateway instance", {
			old_pid: existing.pid,
		});
		const result = await performTakeover(ctx.home, existing.pid, {
			selfPid: ctx.selfPid,
			...(this.opts.takeover ?? {}),
			log: (level, message, meta) => this.log[level](message, meta),
		});
		ctx.takeover = result;
		if (!result.ok) {
			throw new LifecycleError(
				"takeover_failed",
				`could not replace running instance (pid ${existing.pid}): ${String(result.failure)}`,
			);
		}
	}

	private async stageRuntimeLock(ctx: StageContext): Promise<void> {
		// 08 §1.1 step 8 first half / 01 §3.1 stage 5: claim the runtime lock +
		// PID file BEFORE adapters accept traffic. Re-checks the raced-starter
		// condition (two concurrent starters both passing the guard): only the
		// O_EXCL winner proceeds.
		const raced = getRunningPid(ctx.home, { selfPid: ctx.selfPid });
		if (raced !== null && raced.pid !== ctx.selfPid) {
			throw new LifecycleError(
				"duplicate_instance",
				`another gateway instance (pid ${raced.pid}) started during our startup`,
			);
		}
		const lock = new RuntimeLock(ctx.home, { selfPid: ctx.selfPid });
		if (!lock.acquire()) {
			throw new LifecycleError(
				"runtime_lock_held",
				"gateway runtime lock is already held by another instance",
			);
		}
		if (!writePidFile(ctx.home, { selfPid: ctx.selfPid })) {
			lock.release();
			throw new LifecycleError(
				"pid_file_race_lost",
				"PID file claim lost to another gateway instance",
			);
		}
		this.ownedLock = lock;
		ctx.lock = lock;
	}

	private async stageOpenStateDb(ctx: StageContext): Promise<void> {
		// 08 §1.1 step 3: open state.db → reconcile schema → FTS check
		// (02 §2–§3; repair cascade when needed). Also replays shutdown-flush
		// recovery files (#72680) — best-effort, loud.
		const dbPath = this.opts.stateStorePath ?? join(ctx.home, "state.db");
		const store = await StateStore.open(dbPath, this.opts.stateStoreOptions);
		this.ownedStore = store;
		ctx.store = store;
		let report: RecoveryReport = { recovered: 0, preserved: [] };
		try {
			report = await recoverPendingToDb(store, ctx.home, ctx.log);
		} catch (err) {
			ctx.log.warn("pending-message recovery sweep failed", {
				error: String(err),
			});
		}
		if (report.recovered > 0) {
			ctx.log.info("recovered pending messages from shutdown flush", {
				count: report.recovered,
			});
		}
		if (report.preserved.length > 0) {
			ctx.log.warn("preserved unrecoverable flush files for operator", {
				count: report.preserved.length,
			});
		}
	}

	private async stageCronScheduler(ctx: StageContext): Promise<void> {
		// 08 §1.1 step 6 first half / 01 §3.1 stage 7 (optional): registered cron
		// providers start HERE, each isolated (DEC-040) — one provider's failure
		// degrades THAT provider loudly without blocking siblings or later stages.
		const entries = this.serviceEntries.cron_scheduler;
		if (entries.length === 0) {
			ctx.log.info("cron scheduler: nothing configured yet (lands Phase 5)");
			return;
		}
		for (const entry of entries) {
			await this.startRegisteredService(ctx, "cron_scheduler", entry);
		}
	}

	private async stageEmbeddedWatchers(ctx: StageContext): Promise<void> {
		// 08 §1.1 step 6 second half / 01 §3.1 stage 8 (optional): registered
		// embedded watchers/extensions start HERE under the same per-service
		// isolation (DEC-040).
		// Assembly seam: construct the ONE built-and-frozen builtin command
		// registry and register it on the ctx BEFORE entries run, so every
		// downstream consumer reaches commands through ctx (07 §9 — no hand-
		// built lists; hermes_cli/commands.py:COMMAND_REGISTRY parity). Loaded
		// LAZILY: bare-node strip-only runners (the two-process child drivers)
		// cannot parse every sibling module; there the registry degrades to
		// absent instead of failing the stage.
		if (ctx.commands === null) {
			ctx.commands =
				this.opts.commandRegistry ?? (await this.loadBuiltinCommandRegistry());
		}
		const entries = this.serviceEntries.embedded_watchers;
		if (entries.length === 0) {
			ctx.log.info("embedded watchers: nothing configured yet (lands Phase 5)");
			return;
		}
		for (const entry of entries) {
			await this.startRegisteredService(ctx, "embedded_watchers", entry);
		}
	}

	/**
	 * Built-and-frozen builtin set (commands/builtins.ts:createBuiltinCommand-
	 * Registry). Best-effort by design: a load failure degrades to null loudly
	 * rather than failing the optional stage.
	 */
	private async loadBuiltinCommandRegistry(): Promise<CommandRegistry | null> {
		try {
			const mod = (await import("../commands/builtins.js")) as {
				createBuiltinCommandRegistry(): CommandRegistry;
			};
			return mod.createBuiltinCommandRegistry();
		} catch (err) {
			this.log.warn("builtin command registry unavailable", {
				reason_code: "command_registry_unavailable",
				error: String(err),
			});
			return null;
		}
	}

	/**
	 * Start ONE registered service with per-service isolation (01 §3.1, DEC-040):
	 * a thrown error OR a degraded outcome is logged loudly and recorded WITHOUT
	 * blocking sibling services or later stages; a disabled outcome is loud but
	 * not a failure. NEVER throws — the stage slot is consumed either way.
	 */
	private async startRegisteredService(
		ctx: StageContext,
		stage: ServiceStageId,
		entry: ServiceEntry,
	): Promise<void> {
		let outcome: ServiceStartOutcome;
		try {
			outcome = await entry.start(ctx);
		} catch (err) {
			outcome = {
				ok: false,
				degraded: true,
				reason: err instanceof Error ? err.message : String(err),
			};
		}
		if (outcome.ok) {
			if (outcome.handle !== undefined) {
				if (stage === "cron_scheduler") ctx.services.cron.push(outcome.handle);
				else ctx.services.watchers.push(outcome.handle);
			}
			ctx.log.info(`service ${entry.name} started (stage ${stage})`, {
				stage,
				service: entry.name,
			});
			return;
		}
		const reason = outcome.reason ?? "unspecified";
		if (outcome.degraded === true) {
			this.serviceDegradations.push({ stage, service: entry.name, reason });
			ctx.log.error(
				`service ${entry.name} DEGRADED at stage ${stage}: ${reason}`,
				{ stage, service: entry.name, reason_code: "degraded_start" },
			);
			return;
		}
		ctx.log.warn(
			`service ${entry.name} not started (stage ${stage}): ${reason}`,
			{ stage, service: entry.name },
		);
	}

	private async stagePlatformAdapters(ctx: StageContext): Promise<void> {
		// 08 §1.1 step 7: instantiate adapters for configured platforms;
		// missing secret ⇒ loud disable. Per-adapter DEC-040 isolation parity:
		// each entry runs instantiate → wire handlers → connect INDEPENDENTLY
		// (run.py:start platform loop + _create_adapter); one adapter's failure
		// never blocks siblings or later stages. Retryable connect failures are
		// queued for the supervised reconnect watcher instead of staying down.
		ctx.adapters = [];
		const entries = this.adapterEntries;
		if (entries.length === 0) {
			ctx.log.info("platform adapters: none configured yet (land Phase 3+)");
			return;
		}
		for (const entry of entries) {
			let outcome: AdapterStartOutcome;
			try {
				outcome = await entry.start(ctx);
			} catch (err) {
				outcome = {
					ok: false,
					degraded: true,
					reason: err instanceof Error ? err.message : String(err),
				};
			}
			if (outcome.ok && outcome.handle !== undefined) {
				ctx.adapters.push({
					platform: entry.platform,
					enabled: true,
					handle: outcome.handle,
				});
				ctx.log.info(`platform adapter ${entry.platform} connected`, {
					platform: entry.platform,
				});
				continue;
			}
			const reason = outcome.reason ?? "unspecified";
			if (outcome.ok) {
				// Connected but produced no stoppable handle — record enabled.
				ctx.adapters.push({ platform: entry.platform, enabled: true });
				continue;
			}
			if (outcome.retryable === true) {
				// Connect failed but the platform is worth retrying in background
				// (network blip class) — queue for the reconnect watcher.
				this.failedPlatforms.enqueue(entry.platform, { reason });
				ctx.log.warn(
					`platform adapter ${entry.platform} connect failed — queued for reconnection: ${reason}`,
					{ platform: entry.platform, reason_code: "reconnect_queued" },
				);
				continue;
			}
			// Loud disable (missing secret / non-retryable): recorded, logged at
			// ERROR, never fatal to the stage (08 §1.1 step 7).
			ctx.adapters.push({ platform: entry.platform, enabled: false, reason });
			ctx.log.error(`platform adapter ${entry.platform} DISABLED: ${reason}`, {
				platform: entry.platform,
				reason_code: "adapter_disabled",
			});
		}
	}

	private async stageRuntimeIdentity(ctx: StageContext): Promise<void> {
		// 08 §1.1 step 8 second half / 01 §3.1 stage 10: stamp runtime identity
		// + code fingerprint, then READY. Code stamps degrade to absent fields
		// rather than failing the write (08 §4).
		const fingerprint = ctx.fingerprint ?? bootFingerprintValue();
		writeRuntimeStatus(
			ctx.home,
			{
				gateway_state: "running",
				code_sha: fingerprint ? shortFingerprintLabel(fingerprint) : null,
				code_version: packageVersion(),
			},
			{ pid: ctx.selfPid, home: ctx.home },
		);
		ctx.log.info("gateway READY", { home: ctx.home, pid: ctx.selfPid });
	}

	/**
	 * Run the ten binding stages IN ORDER. Idempotent: a completed startup
	 * returns its recorded result without re-executing any body, and each
	 * individual stage skips itself if already completed (partial-restart safe).
	 * Required-stage failure aborts; optional-stage failure degrades loudly.
	 * Post-stage-9 boot choreography (clean/unclean branch + stuck-loop
	 * suspension + restore gate) and post-stage-10 resume scheduling run
	 * exactly once per process (run.py:start parity).
	 */
	async startup(): Promise<StartupResult> {
		if (this.startupResult !== null) return this.startupResult;
		const startedAt = Date.now();
		if (this._state === "idle") this.transition("starting");

		const trace: StageEvent[] = [];
		for (const id of STAGE_IDS) {
			if (this.completedStages.has(id)) continue;
			const stageStart = Date.now();
			let bodyOk = false;
			try {
				await this.bodyFor(id)(this.ctx);
				bodyOk = true;
			} catch (err) {
				if (!isOptionalStage(id)) {
					// REQUIRED stage failure: abort startup. Stages after it never run.
					return this.abortStartup(id, err, trace, startedAt);
				}
				// OPTIONAL stage failure: degrade loudly; later stages still run
				// (01 §3.1). The slot is consumed — a partial restart never re-runs it.
				const message = err instanceof Error ? err.message : String(err);
				const reasonCode =
					err instanceof LifecycleError ? err.reasonCode : "stage_error";
				this.degradedStages.add(id);
				this.completedStages.add(id);
				this.log.error(`startup degraded at stage ${id}: ${message}`, {
					stage: id,
					reason_code: "degraded_start",
					...(reasonCode !== "stage_error" ? { detail: reasonCode } : {}),
				});
				const event: StageEvent = {
					stage: id,
					ok: false,
					degraded: true,
					error: message,
					durationMs: Date.now() - stageStart,
				};
				trace.push(event);
				this.events.push(event);
			}
			if (bodyOk) {
				this.completedStages.add(id);
				const okEvent: StageEvent = {
					stage: id,
					ok: true,
					durationMs: Date.now() - stageStart,
				};
				trace.push(okEvent);
				this.events.push(okEvent);
			}
			// POST-STAGE-9 boot choreography (run.py:start): clean-shutdown
			// receipt vs unclean recovery + stuck-loop suspension + gate the
			// inbound dispatch queue. Runs ONCE, immediately after the adapter
			// stage completes — regardless of which body ran. FAIL-CLOSED: a
			// choreography error is a REQUIRED abort (never a stage-9 degrade).
			if (id === "platform_adapters" && !this.bootChoreographyRan) {
				try {
					await this.runPostAdapterBootRecovery();
				} catch (err) {
					return this.abortStartup(id, err, trace, startedAt, {
						stageFailed: false,
						reasonCode: "boot_recovery_failed",
					});
				}
			}
		}

		// POST-STAGE-10 finish (run.py:_await_startup_boot_sends →
		// _schedule_resume_pending_sessions → _finish_startup_restore): boot
		// sends and the auto-resume pass fire here; the inbound restore gate
		// opens in its finally so queued traffic flushes to the consumer.
		await this.finishStartupRestore();
		this.armLoopLivenessBackstops();

		if (this._state === "starting") this.transition("running");
		this.startupResult = {
			ok: true,
			failedStage: null,
			error: null,
			reasonCode: null,
			degradedStages: [...this.degradedStages],
			trace,
			home: this.homeValue,
			durationMs: Date.now() - startedAt,
			exitCode: null,
		};
		return this.startupResult;
	}

	private transitionSafe(to: StartupState): void {
		try {
			this.transition(to);
		} catch {
			/* already terminal — keep first classification */
		}
	}

	/**
	 * Required-stage abort path: record the failure event loudly, persist the
	 * exit reason, release locks (never stranded), and memoize a FAILED result.
	 * Fatal-config reason codes exit 78 (EX_CONFIG — s6 RestartPreventExitStatus,
	 * restart.py); every other abort exits 1.
	 */
	private abortStartup(
		id: StageId,
		err: unknown,
		trace: StageEvent[],
		startedAt: number,
		options: { stageFailed?: boolean; reasonCode?: string } = {},
	): StartupResult {
		const message = err instanceof Error ? err.message : String(err);
		const reasonCode =
			options.reasonCode ??
			(err instanceof LifecycleError ? err.reasonCode : "stage_error");
		if (options.stageFailed !== false) {
			const event: StageEvent = {
				stage: id,
				ok: false,
				error: message,
				durationMs: Date.now() - startedAt,
			};
			trace.push(event);
			this.events.push(event);
		}
		this.transitionSafe("aborted");
		this.log.error(`startup ABORTED at required stage ${id}`, {
			stage: id,
			reason_code: reasonCode,
			error: message,
		});
		try {
			writeRuntimeStatus(
				this.homeValue,
				{ exit_reason: `startup_failed:${id}` },
				{ pid: this.selfPid, home: this.homeValue },
			);
		} catch {
			/* status persistence is best-effort */
		}
		this.releaseOwnership();
		this.startupResult = {
			ok: false,
			failedStage: id,
			error: message,
			reasonCode,
			degradedStages: [...this.degradedStages],
			trace,
			home: this.homeValue,
			durationMs: Date.now() - startedAt,
			exitCode:
				reasonCode === FATAL_CONFIG_REASON_CODE ? FATAL_CONFIG_EXIT_CODE : 1,
		};
		return this.startupResult;
	}

	// ---------------------------------------------------------------------
	// Boot choreography (post-stage-9 / post-stage-10 — run.py:start parity)
	// ---------------------------------------------------------------------

	private bootChoreographyRan = false;

	/**
	 * Clean-shutdown receipt vs unclean-session recovery + stuck-loop
	 * suspension, then CLOSE the inbound restore gate (run.py:start's
	 * `_recover_unclean_sessions` / `.clean_shutdown` branch /
	 * `_suspend_stuck_loop_sessions` / `_startup_restore_in_progress`). A
	 * throw propagates — startup fails CLOSED so a stale clean receipt can
	 * never mask an unclean exit.
	 */
	private async runPostAdapterBootRecovery(): Promise<void> {
		this.bootChoreographyRan = true;
		const report = await runBootChoreography(
			this.homeValue,
			this.opts.bootRecovery ?? {},
			{
				cleanShutdownMarkerExists: cleanShutdownMarkerExists(this.homeValue),
				log: this.log,
			},
		);
		this.bootReportValue = report;
		// Consume the receipt file AFTER a successful pass (fail-closed ordering:
		// runBootChoreography already decided the branch from its existence).
		if (report.cleanShutdown) consumeCleanShutdownMarker(this.homeValue);
		// Gate inbound dispatch while boot restore finishes (resume turns etc.).
		this.restoreGate.begin();
	}

	/**
	 * Boot sends + resume-pending scheduling + gate release (run.py:
	 * _await_startup_boot_sends → _schedule_resume_pending_sessions →
	 * _finish_startup_restore). Best-effort: hook failures are logged loudly;
	 * the restore gate ALWAYS reopens in the finally so inbound dispatch can
	 * never stay gated by a broken seam.
	 */
	private async finishStartupRestore(): Promise<void> {
		try {
			await this.opts.bootRecovery?.bootSends?.();
			await this.opts.bootRecovery?.scheduleResumePending?.();
		} catch (err) {
			this.log.error("boot restore hooks failed", { error: String(err) });
		} finally {
			await this.restoreGate.finish();
		}
	}

	/**
	 * Lifetime loop-liveness backstops (08 §1.3(c); armed at running like
	 * run.py:_start_loop_liveness_guards): heartbeat file refresh + the
	 * 3-strike probe guard whose breach exits 75 (service-restart code).
	 * Config-only opt-out via opts.loopWatchdog.enabled === false.
	 */
	private armLoopLivenessBackstops(): void {
		if (this.opts.loopWatchdog?.enabled === false) return;
		if (this.livenessGuard === null) {
			this.livenessGuard = startLoopLivenessGuard({
				...(this.opts.loopWatchdog?.probeIntervalMs !== undefined
					? { probeIntervalMs: this.opts.loopWatchdog.probeIntervalMs }
					: {}),
				...(this.opts.loopWatchdog?.probeTimeoutMs !== undefined
					? { probeTimeoutMs: this.opts.loopWatchdog.probeTimeoutMs }
					: {}),
				...(this.opts.loopWatchdog?.maxStrikes !== undefined
					? { maxStrikes: this.opts.loopWatchdog.maxStrikes }
					: {}),
				timer: this.timers,
				onBreach: (strikes) => {
					this.log.error(
						"event loop missed consecutive liveness probes — hard-exiting for supervisor restart",
						{ strikes, reason_code: "loop_liveness_watchdog" },
					);
				},
				hardExit: (code) => {
					this.releaseOwnership(); // locks never stranded on ANY exit path
					(
						this.opts.hardExit ?? ((exitCode: number) => process.exit(exitCode))
					)(code);
				},
			});
		}
		if (
			this.opts.loopHeartbeat?.enabled !== false &&
			this.loopHeartbeat === null
		) {
			this.loopHeartbeat = startLoopHeartbeat(this.homeValue, {
				...(this.opts.loopHeartbeat?.intervalMs !== undefined
					? { intervalMs: this.opts.loopHeartbeat.intervalMs }
					: {}),
				timer: this.timers,
			});
		}
	}

	private releaseOwnership(): void {
		// Locks must NEVER be stranded (08 §1.3 watchdog ordering rule).
		removePidFile(this.homeValue, {
			selfPid: this.selfPid,
			selfStartTime: this.opts.selfStartTime ?? processStartTime(this.selfPid),
		});
		this.ownedLock?.release();
		this.ownedLock = null;
	}

	// -----------------------------------------------------------------------
	// Graceful shutdown (08 §1.2)
	// -----------------------------------------------------------------------

	private drainHooks(): DrainHooks {
		// Drain-phase clock (run.py:_stop_impl_body _phase_elapsed parity): the
		// cron budget's watchdog clamp is computed from time already spent in
		// the earlier phases, so the extension can never overrun the leash.
		const drainStartedAtMs = this.timers.nowMs();
		const cronFloorMs =
			this.opts.cronDrainTimeoutMs ?? DEFAULT_CRON_DRAIN_TIMEOUT_MS;
		const adapters = () =>
			this.ctx.adapters.filter((a) => a.handle?.stop !== undefined);
		const cron = () =>
			this.ctx.services.cron.filter((c) => c.stop !== undefined);
		// Engine defaults are no-op seams; opts.shutdownHooks overlays them with
		// the production runner's implementations (notify, resume-pending, …).
		return {
			notifyActiveSessions: async () => {
				// Active-chat notification lands with the outbound layer; the
				// phase pins the ORDERING — adapters are still connected here.
			},
			markResumePendingPreDrain: async () => {
				// Session resume-pending marks land with the Phase-1 runner seam.
				return [] as readonly string[];
			},
			stopIngress: async () => {
				// Adapters ONLY. The cron ticker deliberately KEEPS RUNNING through
				// the whole drain (#82161/#60432): its in-flight runs are waited
				// on their own budget below, then joined cooperatively after
				// teardown — killing one mid-flight is a permanent jobs.json
				// failure nobody is waiting on. Embedded WATCHER services moved
				// to the same post-drain bounded join (run.py start_gateway tail:
				// housekeeping + planned-stop watcher joins follow the ticker's).
				for (const adapter of adapters()) await adapter.handle?.stop?.();
			},
			awaitActiveTurns: async (graceMs) => {
				// CHAT turns: runner turn-tracking arrives with the Phase-1
				// runner; the grace budget is honored here so the contract shape
				// is fixed now (default 0 — interrupting chat turns is announced
				// + resumable, #27856/#82161). The skeleton NEVER reports a
				// chat-side timeout.
				if (graceMs > 0) {
					await new Promise<void>((resolve) =>
						this.timers.setTimeout(resolve, graceMs),
					);
				}
				// IN-FLIGHT CRON work drains on its OWN deadline (#82161/#60432;
				// two-budget loop of run.py:_drain_active_agents(timeout,
				// cron_timeout)): a cron run killed mid-flight is recorded in
				// jobs.json as a permanent failure nobody waits on, so the wait
				// EXTENDS past the chat budget by resolveCronDrainBudget —
				// clamped to the armed shutdown-watchdog leash minus the cleanup
				// reserve so the extension can never cost the process its
				// post-drain teardown window (restart.py:resolve_cron_drain_budget
				// parity; a floor ≤ 0 opts out — legacy shared-budget behavior).
				// SEAM CONTRACT: a runner-side overlay replacing this hook MUST
				// keep folding ctx.services.cron's inflightCount into the wait on
				// its own resolveCronDrainBudget deadline — dropping it reopens
				// #82161 (zero-budget kill of mid-flight runs).
				const handles = cron();
				if (handles.length === 0) return;
				const startedMs = this.timers.nowMs();
				const budgetMs =
					resolveCronDrainBudget(graceMs / 1000, cronFloorMs / 1000, {
						watchdogDelayS: resolveShutdownWatchdogDelayMs(graceMs) / 1000,
						elapsedS: (startedMs - drainStartedAtMs) / 1000,
					}) * 1000;
				while (
					handles.reduce(
						(total, handle) =>
							total + Math.max(0, handle.inflightCount?.() ?? 0),
						0,
					) > 0
				) {
					if (this.timers.nowMs() - startedMs >= budgetMs) {
						// Budget expired with ticks still in flight — the drain
						// TIMED OUT (clean-shutdown receipt suppressed so the next
						// boot suspends half-finished work; timed_out parity).
						return true;
					}
					await new Promise<void>((resolve) =>
						this.timers.setTimeout(resolve, CRON_DRAIN_POLL_MS),
					);
				}
			},
			releaseLeases: async () => {
				// Cross-process turn leases are per-conversation rows released by
				// the runner per turn (DEC-004); the lifecycle-level sweep is a
				// Phase-1-runner seam. Nothing to sweep in the skeleton.
			},
			flushDeliveryObligations: async () => {
				// delivery_obligations ledger logic lands Phase 2 (DEC-007).
			},
			activeSessionKeys: () => [] as readonly string[],
			flushTokenRollups: async () => {
				// Coalescing-writer flush barrier (02 §7.2) — drains queued deltas
				// so rollups land before the DB closes.
				await this.store?.flushTokenCounts(5000);
			},
			closeDatabase: async () => {
				const store = this.store;
				this.ownedStore = null;
				await store?.close(true);
			},
			persistExitStatus: async (outcome) => {
				persistExitStatusPatch(this.homeValue, outcome, {
					pid: this.selfPid,
					home: this.homeValue,
				});
			},
			...this.opts.shutdownHooks,
		};
	}

	/**
	 * POST-DRAIN cooperative join of embedded background services — moved OUT
	 * of stop_ingress (#82161/#60432; parity of the run.py:start_gateway tail:
	 * cron_stop.set() → _stop_cron_provider → _await_thread_exit(cron_thread,
	 * timeout=_CRON_SHUTDOWN_DRAIN_TIMEOUT=65) → housekeeping/planned-stop
	 * joins). Runs only AFTER executeDrain completed so an in-flight run's
	 * mark+deliver tail finished inside its own budget above; each handle is
	 * joined cooperatively but BOUNDED, so a wedged service can extend
	 * shutdown by at most one window (daemon-thread give-up parity — never a
	 * hanging exit). Cron tickers join FIRST: their delivery needs the live
	 * loop (#58818); watchers follow.
	 */
	private async joinEmbeddedServicesPostDrain(): Promise<void> {
		const crons = this.ctx.services.cron.filter((c) => c.stop !== undefined);
		const watchers = this.ctx.services.watchers.filter(
			(w) => w.stop !== undefined,
		);
		for (const ticker of crons) {
			const breached = await this.boundedServiceStop(
				ticker,
				CRON_SHUTDOWN_DRAIN_TIMEOUT_MS,
			);
			if (breached) {
				// Warning parity of the _CRON_SHUTDOWN_DRAIN_TIMEOUT breach.
				this.log.warn(
					`Cron ticker did not exit within ${Math.round(CRON_SHUTDOWN_DRAIN_TIMEOUT_MS / 1000)}s of shutdown — an in-flight delivery may have been dropped.`,
					{ service: ticker.name },
				);
			}
		}
		for (const watcher of watchers) {
			const breached = await this.boundedServiceStop(
				watcher,
				CRON_SHUTDOWN_DRAIN_TIMEOUT_MS,
			);
			if (breached) {
				this.log.warn(
					`service ${watcher.name} did not exit within ${Math.round(CRON_SHUTDOWN_DRAIN_TIMEOUT_MS / 1000)}s of shutdown — continuing teardown`,
					{ service: watcher.name },
				);
			}
		}
	}

	/**
	 * One bounded cooperative stop (_await_thread_exit parity): race the
	 * handle's stop promise against the window on the injected timer port;
	 * a stop that RAISES is isolated (logged, never propagates into the
	 * recorded drain outcome). Returns true when the window expired first.
	 */
	private async boundedServiceStop(
		handle: ServiceHandle,
		timeoutMs: number,
	): Promise<boolean> {
		const stop = handle.stop;
		if (stop === undefined) return false;
		let breached = false;
		await new Promise<void>((resolve) => {
			let settled = false;
			const finish = (): void => {
				if (settled) return;
				settled = true;
				resolve();
			};
			const timer = this.timers.setTimeout(() => {
				breached = true;
				finish();
			}, timeoutMs);
			void Promise.resolve()
				.then(stop)
				.then(
					() => {
						this.timers.clearTimeout(timer);
						finish();
					},
					(err) => {
						this.timers.clearTimeout(timer);
						this.log.warn(
							`service ${handle.name} raised during post-drain stop`,
							{ service: handle.name, error: String(err) },
						);
						finish();
					},
				);
		});
		return breached;
	}

	/**
	 * Programmatic graceful shutdown. Idempotent: concurrent/repeat callers
	 * share one drain. `klass` selects the recorded outcome class (programmatic
	 * stops are planned stops).
	 */
	async requestShutdown(
		klass: ShutdownClass = "planned_stop",
	): Promise<DrainOutcome> {
		if (this.shutdownOutcome !== null) return this.shutdownOutcome;
		if (this.shutdownInFlight !== null) return this.shutdownInFlight;

		this.shutdownInFlight = (async (): Promise<DrainOutcome> => {
			this.shutdownClass = klass;
			// Mirror BEFORE teardown begins (run.py:_signal_initiated_shutdown).
			this.signalInitiatedShutdown = klass === "unexpected_signal";
			this.transitionSafe("draining");
			if (persistsStopped(klass)) {
				try {
					writeRuntimeStatus(
						this.homeValue,
						{ gateway_state: "draining" },
						{ pid: this.selfPid, home: this.homeValue },
					);
				} catch {
					/* status persistence is best-effort */
				}
			}
			// Unexpected signals NEVER touch gateway_state (#42675): the file
			// keeps its pre-signal value so a container boot never reads a
			// clean-looking state from an externally killed gateway.
			// §1.3(c) backstop: an UNREF'D OS-LEVEL watchdog hard-exits ≤ drain
			// + 60s grace if teardown wedges (arm_shutdown_watchdog parity). The
			// exit callback releases PID file + runtime lock BEFORE exiting.
			const graceMs = this.opts.drainGraceMs ?? DEFAULT_DRAIN_GRACE_MS;
			const watch = armShutdownWatchdog({
				delayMs: resolveShutdownWatchdogDelayMs(graceMs),
				exitCode: SHUTDOWN_EXIT_CODES.unexpected_signal,
				dumpPath: shutdownWatchdogDumpPath(this.homeValue),
				timer: this.timers,
				snapshotFn: () => ({
					klass,
					draining: true,
					pending_messages: this.pendingSlots.length,
					adapters: this.ctx.adapters.length,
					// In-flight cron visibility in the post-mortem dump
					// (run.py _shutdown_watchdog_snapshot active_cron_jobs parity).
					cron_jobs: this.ctx.services.cron.reduce(
						(total, handle) =>
							total + Math.max(0, handle.inflightCount?.() ?? 0),
						0,
					),
				}),
				hardExit: (code) => {
					this.releaseOwnership(); // locks must never be stranded
					(this.opts.hardExit ?? ((c: number) => process.exit(c)))(code);
				},
			});
			let outcome: DrainOutcome;
			try {
				outcome = await executeDrain({
					home: this.homeValue,
					klass,
					graceMs,
					hooks: this.drainHooks(),
					takePendingSlots: () => {
						const slots = this.pendingSlots;
						this.pendingSlots = []; // cleared ONLY after the flush step ran
						return slots;
					},
					log: this.log,
				});
			} finally {
				watch.disarm();
			}
			// Post-drain cooperative join of cron/watcher handles (moved OUT of
			// stop_ingress — #82161/#60432): the full drain above already waited
			// in-flight ticks on their own budget, so this is teardown-only and
			// runs OUTSIDE the watchdog leash exactly like Hermes joins its ticker
			// after wait_for_shutdown. Never throws into the recorded outcome.
			await this.joinEmbeddedServicesPostDrain();
			this.shutdownOutcome = outcome;
			this.transitionSafe("stopped");
			// Release PID file + runtime lock AFTER teardown (never stranded).
			this.releaseOwnership();
			for (const waiter of this.shutdownWaiters.splice(0)) waiter(outcome);
			return outcome;
		})();
		return this.shutdownInFlight;
	}

	/**
	 * Signal entry point (parity of run.py:shutdown_signal_handler + its
	 * SIGUSR1 sibling restart_signal_handler). Captures the <10ms forensic
	 * snapshot FIRST (never blocks, never raises), classifies via markers /
	 * signal identity, mirrors the unexpected-signal flag BEFORE teardown
	 * begins, schedules the drain, and escalates a SECOND signal to fast-exit.
	 */
	handleSignal(signalName: string | null): void {
		this.signalCount++;
		if (this.signalCount >= 2) {
			this.escalateFastExit();
			return;
		}
		// 08 §1.3(b) forensics: sync probe + one-line context log, then the
		// heavyweight ps-walk as a DETACHED fire-and-forget subprocess.
		if (this.opts.forensics?.snapshot !== false) {
			try {
				const ctx = snapshotShutdownContext({
					signal: signalName,
					home: this.homeValue,
					selfPid: this.selfPid,
				});
				this.log.warn(`shutdown context: ${formatContextForLog(ctx)}`);
			} catch {
				/* forensics must never break the signal path */
			}
		}
		if (this.opts.forensics?.spawnDiagnostic !== false && signalName !== null) {
			try {
				spawnAsyncDiagnostic(join(this.homeValue, "logs"), signalName);
			} catch {
				/* best-effort */
			}
		}
		const klass = classifySignalForSelf(this.homeValue, signalName, {
			selfPid: this.selfPid,
		});
		void this.requestShutdown(klass).catch((err) => {
			this.log.error("drain failed after signal", { error: String(err) });
			this.escalateFastExit();
		});
	}

	/**
	 * Install SIGTERM/SIGINT/SIGUSR1 handlers driving handleSignal (parity of
	 * the loop.add_signal_handler wiring in run.py:start_gateway — including
	 * `loop.add_signal_handler(SIGUSR1, restart_signal_handler)` so the update
	 * flow's drain-first SIGUSR1 works against the REAL process; without a
	 * listener Node would open the inspector instead of draining).
	 * SIGHUP is deliberately NOT handled: it is not a reload signal (DEC-013),
	 * and Hermes installs SIGHUP→SIG_IGN only around update runs (Phase 5 scope).
	 */
	installSignalHandlers(): void {
		process.on("SIGTERM", () => this.handleSignal("SIGTERM"));
		process.on("SIGINT", () => this.handleSignal("SIGINT"));
		try {
			process.on("SIGUSR1", () => this.handleSignal("SIGUSR1"));
		} catch {
			/* platform without SIGUSR1 support — drain-first restart degrades to
			   the updater's SIGTERM escalation (08 §7 stop-after-window). */
		}
	}

	/** Settles when a signal-initiated drain finishes (driver/test sync). */
	get drainSettled(): Promise<DrainOutcome> {
		if (this.shutdownInFlight === null) {
			return Promise.reject(
				new LifecycleError("not_draining", "no shutdown requested"),
			);
		}
		return this.shutdownInFlight;
	}

	/**
	 * Resolves when this lifecycle's FIRST shutdown drain completes (or
	 * immediately if one already has). Unlike `drainSettled`, safe to await
	 * before any shutdown was requested — drivers and supervisors park here.
	 */
	waitShutdown(): Promise<DrainOutcome> {
		if (this.shutdownOutcome !== null)
			return Promise.resolve(this.shutdownOutcome);
		return new Promise<DrainOutcome>((resolve) => {
			this.shutdownWaiters.push(resolve);
		});
	}

	/**
	 * Second-signal escalation (08 §1.2 "fast-exit") with §1.3(c) ordering:
	 * release the PID file + runtime lock BEFORE the hard exit — locks must
	 * never be stranded on any exit path.
	 */
	escalateFastExit(): void {
		if (this.escalated) return;
		this.escalated = true;
		this.signalInitiatedShutdown = true;
		this.log.error("second signal received — fast-exit", {
			reason_code: "double_signal",
		});
		this.releaseOwnership();
		const exitCode =
			this.shutdownClass !== null
				? SHUTDOWN_EXIT_CODES[this.shutdownClass]
				: SHUTDOWN_EXIT_CODES.unexpected_signal;
		(this.opts.hardExit ?? ((code: number) => process.exit(code)))(
			exitCode || 1,
		);
	}

	get didEscalate(): boolean {
		return this.escalated;
	}

	// -----------------------------------------------------------------------
	// Diagnostics
	// -----------------------------------------------------------------------

	/** Skew probe (01 §3.3): null when boot had no fingerprint (never lies). */
	detectSkew(): Promise<{ boot: string; disk: string } | null> {
		return detectCodeSkew(this.opts.fingerprintReader);
	}

	statusSnapshot(): ReturnType<typeof readRuntimeStatus> {
		return readRuntimeStatus(this.homeValue);
	}

	/** Test/driver hook: dispose resources without a full drain. */
	dispose(): void {
		this.livenessGuard?.stop();
		this.loopHeartbeat?.stop();
		this.restoreGate.finish().catch(() => undefined);
		this.releaseOwnership();
		void this.ownedStore?.close(false).catch(() => undefined);
		this.ownedStore = null;
	}
}

export type {
	GatewayRuntimeState,
	ServiceStageId,
	StageEvent,
	StageId,
	StartupState,
	TakeoverResult,
};
