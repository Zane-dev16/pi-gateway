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
// now; adapters land in Phases 3+ through `stageBodies` — the default body
// for stage 9 reports "nothing configured yet" and succeeds. Stages 7–8 are
// WIRED (DEC-040): per-service entries registered via `registerService()` /
// the `services` option run inside their stage bodies with per-service loud
// degradation; the engine stays service-agnostic (structural entries, no
// pi_embedded imports — layering 01 §5.3).

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

/** Minimal supervised-service seam filled by Phases 5+ (cron/watchers/adapters). */
export interface ServiceHandle {
	name: string;
	stop?: () => Promise<void>;
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
	stateStorePath?: string;
	stateStoreOptions?: StateStoreOptions;
	/** Active-turn grace window for the drain (default 0 — interrupting chat
	 *  turns is cheap; sessions are pre-marked resume_pending, #27856). */
	drainGraceMs?: number;
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
	private readonly serviceDegradations: ServiceDegradation[] = [];
	private readonly completedStages = new Set<StageId>();
	private readonly degradedStages = new Set<StageId>();

	private ctx: StageContext;
	private ownedLock: RuntimeLock | null = null;
	private ownedStore: StateStore | null = null;
	private startupResult: StartupResult | null = null;

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
		for (const stage of SERVICE_STAGE_IDS) {
			for (const entry of options.services?.[stage] ?? []) {
				this.registerService(stage, entry);
			}
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

	/**
	 * Register one per-service optional-stage entry (DEC-040). Entries run when
	 * their stage body executes — AFTER every earlier required stage succeeded,
	 * in REGISTRATION order, each isolated from its siblings' failures.
	 */
	registerService(stage: ServiceStageId, entry: ServiceEntry): this {
		this.serviceEntries[stage].push(entry);
		return this;
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
		// missing secret ⇒ loud disable. Reference adapters land Phase 3.
		ctx.adapters = [];
		ctx.log.info("platform adapters: none configured yet (land Phase 3+)");
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
	 */
	async startup(): Promise<StartupResult> {
		if (this.startupResult !== null) return this.startupResult;
		const startedAt = Date.now();
		if (this._state === "idle") this.transition("starting");

		const trace: StageEvent[] = [];
		for (const id of STAGE_IDS) {
			if (this.completedStages.has(id)) continue;
			const stageStart = Date.now();
			try {
				await this.bodyFor(id)(this.ctx);
				this.completedStages.add(id);
				trace.push({
					stage: id,
					ok: true,
					durationMs: Date.now() - stageStart,
				});
				const recorded = trace.at(-1);
				if (recorded !== undefined) this.events.push(recorded);
			} catch (err) {
				const message = err instanceof Error ? err.message : String(err);
				const reasonCode =
					err instanceof LifecycleError ? err.reasonCode : "stage_error";
				const event: StageEvent = {
					stage: id,
					ok: false,
					error: message,
					durationMs: Date.now() - stageStart,
				};
				if (isOptionalStage(id)) {
					// 01 §3.1: degraded-start allowed PER SERVICE with a loud log —
					// never blocks later stages.
					event.degraded = true;
					this.degradedStages.add(id);
					this.completedStages.add(id); // consumed its slot; do not re-run
					this.log.error(`startup degraded at stage ${id}: ${message}`, {
						stage: id,
						reason_code: "degraded_start",
						...(reasonCode !== "stage_error" ? { detail: reasonCode } : {}),
					});
					trace.push(event);
					this.events.push(event);
					continue;
				}
				// REQUIRED stage failure: abort startup. Stages after it never run.
				trace.push(event);
				this.events.push(event);
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
				};
				return this.startupResult;
			}
		}

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
		const adapters = () =>
			this.ctx.adapters.filter((a) => a.handle?.stop !== undefined);
		const watchers = () =>
			this.ctx.services.watchers.filter((w) => w.stop !== undefined);
		const cron = () =>
			this.ctx.services.cron.filter((c) => c.stop !== undefined);
		return {
			stopIngress: async () => {
				// Adapters stop polling/reading; embedded background services join
				// this step until each gets its own cooperative-drain contract
				// (cron bounded window, Phase 5).
				for (const adapter of adapters()) await adapter.handle?.stop?.();
				for (const service of [...watchers(), ...cron()])
					await service.stop?.();
			},
			awaitActiveTurns: async (graceMs) => {
				// Runner turn-tracking arrives with the Phase-1 runner; the grace
				// budget is honored here so the contract shape is fixed now.
				if (graceMs > 0) {
					await new Promise<void>((resolve) => setTimeout(resolve, graceMs));
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
		};
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
			const outcome = await executeDrain({
				home: this.homeValue,
				klass,
				graceMs: this.opts.drainGraceMs ?? DEFAULT_DRAIN_GRACE_MS,
				hooks: this.drainHooks(),
				takePendingSlots: () => {
					const slots = this.pendingSlots;
					this.pendingSlots = []; // cleared ONLY after the flush step ran
					return slots;
				},
				log: this.log,
			});
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
	 * Signal entry point (parity of run.py:shutdown_signal_handler). Classifies
	 * via markers, mirrors the unexpected-signal flag BEFORE teardown begins,
	 * schedules the drain, and escalates a SECOND signal to fast-exit.
	 */
	handleSignal(signalName: string | null): void {
		this.signalCount++;
		if (this.signalCount >= 2) {
			this.escalateFastExit();
			return;
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
	 * Install SIGTERM/SIGINT handlers driving handleSignal (parity of the
	 * loop.add_signal_handler wiring in run.py:start_gateway). SIGHUP is
	 * deliberately NOT handled: it is not a reload signal (DEC-013), and Hermes
	 * installs SIGHUP→SIG_IGN only around update runs (Phase 5 scope).
	 */
	installSignalHandlers(): void {
		process.on("SIGTERM", () => this.handleSignal("SIGTERM"));
		process.on("SIGINT", () => this.handleSignal("SIGINT"));
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
