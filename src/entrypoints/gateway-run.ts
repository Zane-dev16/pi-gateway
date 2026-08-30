// entrypoints/gateway-run.ts — 'pi gateway run': THE COMPOSITION ROOT.
//
// Tops the module graph per 01-architecture.md §5.3 ("entrypoints (pi main,
// pi gateway run, pi serve)") exactly the way gateway/run.py:start_gateway
// tops Hermes' graph. Everything below this layer is a library; THIS module
// binds it into a running gateway. Binding order mirrors start_gateway:
//
//   record_boot_fingerprint ───────────► engine stage 3 (boot_fingerprint)
//   duplicate guard / takeover ────────► engine stage 4 (duplicate_guard;
//                                        takeover.ts handshake parity)
//   PID file + runtime lock claim ─────► engine stage 5 — BEFORE any adapter
//                                        accepts traffic (the --replace
//                                        O_EXCL race rule; #19471)
//   state.db open + flush recovery ────► engine stage 6 (recoverPendingToDb,
//                                        shutdown_flush.py:72680 parity)
//   cron provider bind ────────────────► stage 7 entry: cron-ticker (Hermes:
//                                        cron-scheduler thread started by the
//                                        resolved provider)
//   housekeeping/watchers bind ────────► stage 8 entries: optional
//                                        handoff / kanban watchers, extra
//                                        sibling entries, and the supervised
//                                        platform reconnect watcher (run.py:
//                                        _platform_reconnect_watcher)
//   adapters bind from manifests ──────► stage 9 AdapterEntry entries derived
//                                        from pi_platforms PluginManifest
//                                        requiresEnv gates through the §4.2
//                                        register(ctx) flow (Hermes:
//                                        _create_adapter per configured
//                                        platform; missing secret ⇒ LOUD
//                                        disable). Registration side effects
//                                        (token locks) run INSIDE stage 9 —
//                                        after the lock claim, never before.
//   signal handlers ───────────────────► installSignalHandlers() before the
//                                        startup wait parks (parity of
//                                        loop.add_signal_handler wiring in
//                                        start_gateway: SIGTERM/SIGINT drain,
//                                        SIGUSR1 service-restart exit 75)
//
// REAL production seams bound here (the engine's no-op defaults remain no-op
// for bare test drivers; composition overlays them):
//   - flush_delivery_obligations → DeliveryLedger.prune over the lifecycle-
//     owned store BEFORE closeDatabase (retention GC: caps 3/24h/7d/500).
//   - release_leases → sweep session_turn_leases rows held by THIS process
//     (structured holder pid match, hermes_state.py holder grammar) so a
//     graceful stop never leaves TTL-stalled leases for the next boot.
//   - notify_active_sessions + active-session keys → live conversations read
//     from non-expired turn-lease rows; an injected notice sender fires while
//     adapters are still connected (run.py:_notify_active_sessions_of_shutdown
//     ordering); keys also feed #7536 restart-failure counting at teardown.
//   - boot sends → pending-obligation redelivery through the injected
//     DeliverySender, filtered to ACTUALLY-connected platforms so absent ones
//     never spend an attempt (run.py:_redeliver_pending_obligations inside
//     _await_startup_boot_sends).
//
// Layering: rank 6 (above pi_server) — the one layer that may import every
// other layer, which is precisely what makes this the place where pi_embedded
// stage entries, pi_platforms registrations, and the pi_gateway engine meet
// (01 §5.3; DEC-058).

import {
	DeliveryLedger,
	ObligationRetryScheduler,
	type DeliverySender,
} from "../pi_gateway/obligations/index.js";
import { kitScopedSecretReader } from "../pi_gateway/security/secretscope/wrapper.js";
import { extractHolderPid } from "../pi_state/leases.js";
import type { Database } from "better-sqlite3";

import {
	GatewayLifecycle,
	type AdapterEntry,
	type AdapterStartOutcome,
	type BootRecoveryHooks,
	type DrainHooks,
	type Logger,
	type ReconnectHooks,
} from "../pi_gateway/lifecycle/index.js";
import type { TimerPort } from "../pi_gateway/lifecycle/watchdog.js";
import type { CommandRegistry } from "../pi_gateway/commands/registry.js";

import type {
	EmbeddedServiceEntry,
	EmbeddedServiceHandle,
	EmbeddedServiceOutcome,
} from "../pi_embedded/service-entry.js";
import {
	CRON_TICKER_SERVICE_NAME,
	CronJobStore,
	cronTickerServiceEntry,
	defaultCronStorePaths,
	type DeliverySink,
	type ScheduledJobRunner,
} from "../pi_embedded/cron/index.js";
import type { StartCronTickerOptions } from "../pi_embedded/cron/scheduler.js";
import {
	handoffWatcherServiceEntry,
	type HandoffWatcher,
} from "../pi_embedded/handoff/index.js";
import {
	kanbanDispatcherServiceEntry,
	kanbanNotifierServiceEntry,
	type KanbanDispatcherEntryDeps,
	type KanbanNotifierEntryDeps,
} from "../pi_embedded/kanban/index.js";
// kit/registration (PluginContext) and the per-platform adapter modules use
// TypeScript parameter properties, which bare-node strip-only runners cannot
// parse — so the composition root loads them LAZILY at stage-9 execution
// (the same posture as the engine's lazy builtin-command-registry load).
// Under full runtimes the real §4.2 registration flow (token locks,
// standalone-sender hooks) runs; under bare runners it degrades LOUDLY to a
// local manifest enablement walk. PlatformHosting stays a structural type
// here — no static kit import is required for the derivation itself.
import type {
	PlatformFactory,
	PluginManifest,
} from "../pi_platforms/kit/index.js";

// ─────────────────────────────────────────────────────────────────────────────
// Input surface — what a host supplies vs what this module binds itself.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Structural view of a connected adapter (kit BasePlatformAdapter contract:
 * connect({isReconnect}) / disconnect()). Declared locally because the
 * registry boundary (`PlatformFactory = () => unknown`) erases kit types by
 * design — adapters meet the runner only through registries (01 §5.3).
 */
export interface AdapterConnectSurface {
	connect(opts: {
		isReconnect: boolean;
	}): Promise<boolean | void> | boolean | void;
	disconnect(): Promise<void> | void;
}

/**
 * Structural mirror of kit/registration.ts:PluginContext — the registration
 * face hosting builders may use. Declared locally so this module keeps zero
 * static kit imports (parameter-property chains break bare strip-only
 * runners); the real PluginContext satisfies it structurally.
 */
export interface PluginRegistrationContext {
	registerPlatform(
		manifest: PluginManifest,
		factory: PlatformFactory,
		opts?: { standaloneSenderFn?: unknown },
	): unknown;
}

/** One hosted platform: manifest gate + adapter factory + registration. */
export interface PlatformHosting {
	platform: string;
	manifest: PluginManifest;
	factory: PlatformFactory;
	/**
	 * register(ctx) parity (04 §4.2). Default: ctx.registerPlatform(manifest,
	 * factory). Platforms with extra registration hooks (standalone senders)
	 * pass their own helper here.
	 */
	register?: (
		ctx: PluginRegistrationContext,
		factory: PlatformFactory,
	) => unknown;
}

/** Cron ticker wiring (stage 7). Absent ⇒ cron genuinely not configured. */
export interface CronHosting {
	runner: ScheduledJobRunner;
	/** Delivery transport seam; absent ⇒ runs mark results without delivery. */
	deliverySink?: DeliverySink;
	/** Ticker cadence override (builtin 60s). */
	intervalSeconds?: number;
}

export interface GatewayRunInput {
	/** Profile home (01 §6). Default resolves through the single accessor. */
	home?: string;
	/** `--replace` authority forwarded to the duplicate-guard stage. */
	replace?: boolean;
	logger?: Logger;
	/** Active-turn grace window for the drain (engine default 0). */
	drainGraceMs?: number;
	/** Injectable timers for lifecycle-owned intervals (tests). */
	timers?: TimerPort;
	/**
	 * Install SIGTERM/SIGINT/SIGUSR1 handlers (default TRUE — start_gateway
	 * signal wiring). In-process tests pass false and drive requestShutdown()
	 * programmatically instead of owning real process signals.
	 */
	installSignals?: boolean;
	/**
	 * Scoped secret reader for adapter enablement (DEC-003/009 fail-closed).
	 * Default: kitScopedSecretReader() per the Phase-4 WIRING NOTE — the seam
	 * was reserved for exactly this construction site.
	 */
	secretReader?: (name: string) => string | undefined;
	/** Hosted platforms (stage 9 entries derive from their manifests). */
	platforms?: readonly PlatformHosting[];
	/** Cron ticker (stage 7). */
	cron?: CronHosting;
	/** Handoff queue watcher (stage 8). create() throws ⇒ loud degrade. */
	handoffWatcher?: { create: () => HandoffWatcher };
	/** Kanban dispatcher/notifier (stage 8) — env/singleton gated internally. */
	kanban?: {
		dispatcher?: KanbanDispatcherEntryDeps;
		notifier?: KanbanNotifierEntryDeps;
	};
	/** Extra stage-8 entries from sibling subsystems (e.g. loop watchers). */
	extraWatchers?: readonly EmbeddedServiceEntry[];
	/**
	 * Reconnect backend for the failed-platform queue. Default re-runs the
	 * hosting factory + connect({isReconnect:true}) for known platforms.
	 */
	reconnectBackend?: ReconnectHooks;
	/**
	 * Live-send transport for shutdown notices (notify phase runs while
	 * adapters are still connected). Absent ⇒ loud warning when sessions are
	 * active; no fake sends.
	 */
	shutdownNoticeSender?: (sessionKey: string) => Promise<void>;
	/**
	 * Obligation redelivery transport (boot sends). Absent ⇒ redelivery
	 * skipped loudly; rows keep waiting for a host that can send.
	 */
	deliverySender?: DeliverySender;
	/**
	 * Boot-recovery seams beyond redelivery (exact-turn recovery, suspension,
	 * resume scheduling). Unwired hooks stay absent — the choreography skips
	 * them gracefully (boot-recovery.ts contract).
	 */
	bootRecovery?: Pick<
		BootRecoveryHooks,
		| "discardActiveTurnMarkers"
		| "recoverInterruptedTurns"
		| "suspendRecentlyActive"
		| "suspendSession"
		| "scheduleResumePending"
	>;
	/** Override self-PID for lease ownership matching (tests/multiplex). */
	selfPid?: number;
	/** Command-registry override (engine default builds the shipped set). */
	commandRegistry?: CommandRegistry;
	/** Host hook after READY, before parking on shutdown (see StartupOkHook). */
	onStartupOk?: StartupOkHook;
}

// ─────────────────────────────────────────────────────────────────────────────
// Hosting builders for the census platforms live in ./platform-hosting.ts
// (they statically import the real registration helpers + manifests, whose
// chains cannot load under bare strip-only runners — see the import note
// above). This module stays builder-free and derives entries structurally.
// ─────────────────────────────────────────────────────────────────────────────

// ─────────────────────────────────────────────────────────────────────────────
// Stage-9 adapter entries derived from manifests.
// ─────────────────────────────────────────────────────────────────────────────

interface DerivedAdapters {
	entries: AdapterEntry[];
	/** Platforms whose adapter reported a successful connect (drain filter). */
	connected: Set<string>;
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

/**
 * Local enablement walk over a manifest's requiresEnv list — the
 * registration.ts:resolveEnablement semantics (first missing secret wins,
 * fail-closed) mirrored by VALUE SHAPE so stage 9 never statically imports
 * kit/registration (parameter properties break bare strip-only runners;
 * see the lazy PluginContext load below). Under full runtimes BOTH paths
 * run: the real §4.2 registration AND this gate.
 */
function firstMissingSecret(
	manifest: PluginManifest,
	secrets: (name: string) => string | undefined,
): string | null {
	for (const spec of manifest.requiresEnv) {
		if (secrets(spec.name) === undefined) return spec.name;
	}
	return null;
}

/**
 * Derive stage-9 AdapterEntry objects from hosted platforms' MANIFESTS.
 *
 * Enablement resolves each requiresEnv name through the SCOPED reader — first
 * missing secret ⇒ LOUD DISABLE outcome, never silent (08 §1.1 step 7;
 * run.py:_create_adapter returning None parity). Enabled ⇒ construct →
 * connect({isReconnect:false}) → stoppable handle wrapping disconnect(). A
 * refused/failed CONNECT is RETRYABLE — queued into the failed-platform queue
 * feeding the supervised reconnect watcher (run.py:_connect_adapter_with_
 * timeout failed-platform semantics) — while disabled/fatal-state throws are
 * terminal config facts and classify as loud disables.
 *
 * The shared PluginContext registers lazily on the FIRST entry execution —
 * i.e. inside engine stage 9, strictly AFTER the stage-5 runtime-lock claim,
 * so a losing --replace starter never grabs token locks (start_gateway binds
 * adapters only after the PID-file claim too). The kit module loads via
 * dynamic import; on bare strip-only runners that load fails and registration
 * degrades LOUDLY to the local enablement walk (lazy-load posture of
 * lifecycle.ts:loadBuiltinCommandRegistry).
 */
function deriveAdapterEntries(input: GatewayRunInput): DerivedAdapters {
	const platforms = input.platforms ?? [];
	const secrets = input.secretReader ?? kitScopedSecretReader();
	const log = input.logger ?? stderrLogger();
	const connected = new Set<string>();

	let registered = false;
	const ensureRegistered = async (): Promise<void> => {
		if (registered) return;
		registered = true;
		try {
			const kit = (await import(
				"../pi_platforms/kit/index.js"
			)) as typeof import("../pi_platforms/kit/index.js");
			const ctx = new kit.PluginContext(secrets);
			for (const hosting of platforms) {
				if (hosting.register !== undefined)
					hosting.register(ctx, hosting.factory);
				else ctx.registerPlatform(hosting.manifest, hosting.factory);
			}
		} catch (err) {
			log.warn(
				"plugin-context registration unavailable — running enablement gates only",
				{
					reason_code: "plugin_context_unavailable",
					error: String(err),
				},
			);
		}
	};

	const classifyConnectFailure = (err: unknown): AdapterStartOutcome => {
		const message = err instanceof Error ? err.message : String(err);
		// Disabled/fatal states are terminal config facts, not network blips.
		if (/\bdisabled\b|\bfatal\b/i.test(message))
			return { ok: false, degraded: true, reason: message };
		return { ok: false, retryable: true, reason: message };
	};

	/** construct → connect({isReconnect:false}) → stoppable disconnect handle. */
	const startOne = async (
		hosting: PlatformHosting,
	): Promise<AdapterStartOutcome> => {
		let adapter: unknown;
		try {
			adapter = hosting.factory();
		} catch (err) {
			return {
				ok: false,
				degraded: true,
				reason: err instanceof Error ? err.message : String(err),
			};
		}
		const surface = adapter as Partial<AdapterConnectSurface>;
		if (typeof surface.connect !== "function") {
			return {
				ok: false,
				degraded: true,
				reason: `adapter for ${hosting.platform} exposes no connect()`,
			};
		}
		try {
			const okFlag = await surface.connect({ isReconnect: false });
			if (okFlag === false)
				return { ok: false, retryable: true, reason: "connect refused" };
		} catch (err) {
			return classifyConnectFailure(err);
		}
		connected.add(hosting.platform);
		return {
			ok: true,
			handle: {
				name: hosting.platform,
				stop: async () => {
					try {
						await surface.disconnect?.();
					} finally {
						connected.delete(hosting.platform);
					}
				},
			},
		};
	};

	const entries: AdapterEntry[] = platforms.map((hosting) => ({
		platform: hosting.platform,
		async start(): Promise<AdapterStartOutcome> {
			await ensureRegistered();
			const missing = firstMissingSecret(hosting.manifest, secrets);
			if (missing !== null) {
				return {
					ok: false,
					degraded: true,
					reason: `secret_missing:${missing}`,
				};
			}
			return startOne(hosting);
		},
	}));

	return { entries, connected };
}

/**
 * Default reconnect backend: fresh factory + connect({isReconnect:true}) per
 * attempt (the base contract preserves server-side queues across reconnects).
 * Unknown platform or refused connect ⇒ false keeps the row queued under
 * backoff (reconnect-watcher.ts contract).
 */
function defaultReconnectBackend(input: GatewayRunInput): ReconnectHooks {
	const byPlatform = new Map<string, PlatformHosting>();
	for (const hosting of input.platforms ?? [])
		byPlatform.set(hosting.platform, hosting);
	return {
		async reconnect(platform: string): Promise<boolean> {
			const hosting = byPlatform.get(platform);
			if (hosting === undefined) return false;
			let adapter: unknown;
			try {
				adapter = hosting.factory();
			} catch {
				return false;
			}
			const surface = adapter as Partial<AdapterConnectSurface>;
			if (typeof surface.connect !== "function") return false;
			try {
				return (await surface.connect({ isReconnect: true })) !== false;
			} catch {
				return false;
			}
		},
	};
}

// ─────────────────────────────────────────────────────────────────────────────
// Production drain-hook + boot-recovery overlays.
// ─────────────────────────────────────────────────────────────────────────────

/** Non-expired conversation keys — the live-turn truth this layer can read. */
function liveLeaseSessionKeys(db: Database, nowSeconds: number): string[] {
	const rows = db
		.prepare(
			"SELECT conversation_id FROM session_turn_leases WHERE expires_at > ?",
		)
		.all(nowSeconds) as Array<{ conversation_id: string }>;
	return rows.map((r) => String(r.conversation_id));
}

/**
 * Notify phase body (run.py:_notify_active_sessions_of_shutdown): per-key
 * isolated sends through the host transport while adapters are still
 * connected; no transport configured ⇒ loud truth instead of fake sends.
 * Returns the drain-start active-key snapshot taken here.
 */
async function deliverShutdownNotices(
	db: Database,
	send: ((sessionKey: string) => Promise<void>) | undefined,
	log: Logger,
): Promise<readonly string[]> {
	const keys = liveLeaseSessionKeys(db, Date.now() / 1000);
	if (keys.length === 0 || send === undefined) {
		if (keys.length > 0) {
			log.warn(
				"active sessions at shutdown but no notice transport configured",
				{ count: keys.length },
			);
		}
		return keys;
	}
	for (const key of keys) {
		try {
			await send(key);
		} catch (err) {
			log.warn("shutdown notice delivery failed", {
				session_key: key,
				error: String(err),
			});
		}
	}
	return keys;
}

/**
 * Self-held lease sweep: a graceful stop releases THIS process's turn leases
 * instead of leaving the next boot to wait out TTLs. Foreign-process rows are
 * untouched (their owners/TTLs own recovery). Guarded deletes under one
 * write transaction; returns the released row count.
 */
function sweepSelfHeldLeases(db: Database, selfPid: number): number {
	const rows = db
		.prepare("SELECT conversation_id, holder FROM session_turn_leases")
		.all() as Array<{ conversation_id: string; holder: string }>;
	let released = 0;
	db.exec("BEGIN IMMEDIATE");
	try {
		for (const row of rows) {
			if (extractHolderPid(String(row.holder)) !== selfPid) continue;
			db.prepare(
				"DELETE FROM session_turn_leases WHERE conversation_id = ? AND holder = ?",
			).run(String(row.conversation_id), String(row.holder));
			released++;
		}
		db.exec("COMMIT");
	} catch (err) {
		try {
			db.exec("ROLLBACK");
		} catch {
			/* best-effort */
		}
		throw err;
	}
	return released;
}

/**
 * Production drain-hook overlays (structure-7 core). Bound LAZILY through
 * `storeOf()` — the db opens at engine stage 6, overlays run at drain time.
 * Only the no-op engine DEFAULTS are overridden here; stopIngress /
 * awaitActiveTurns / closeDatabase / persistExitStatus stay engine-owned.
 */
function buildShutdownHookOverlays(
	input: GatewayRunInput,
	storeOf: () => { db: Database } | null,
	_connected: Set<string>,
): Partial<DrainHooks> {
	const log = input.logger ?? stderrLogger();
	const selfPid = input.selfPid ?? process.pid;

	// Drain-start snapshot of live conversations (_drain_active_agents
	// snapshot semantics): captured in the FIRST drain phase while the db is
	// still open, reused for restart-failure counting after close_database —
	// the engine runs that step post-close by design (#7536 parity).
	const drainState: { activeKeys: readonly string[] } = { activeKeys: [] };

	return {
		// run.py:_stop_impl_body notify phase — adapters still connected here,
		// so notices can actually reach chats. Per-key isolated like Hermes'
		// notify loop.
		notifyActiveSessions: async () => {
			const store = storeOf();
			if (store === null) return;
			drainState.activeKeys = await deliverShutdownNotices(
				store.db,
				input.shutdownNoticeSender,
				log,
			);
		},

		// Self-held lease sweep — see sweepSelfHeldLeases above.
		releaseLeases: async () => {
			const store = storeOf();
			if (store === null) return;
			const released = sweepSelfHeldLeases(store.db, selfPid);
			if (released > 0)
				log.info("released this process's turn leases", { count: released });
		},

		// Retention GC before closeDatabase (delivery_ledger.py:_prune parity:
		// delivered/abandoned past the 7d window go, then the 500-row cap).
		flushDeliveryObligations: async () => {
			const store = storeOf();
			if (store === null) return;
			const pruned = await new DeliveryLedger(store.db).prune();
			if (pruned > 0)
				log.info("delivery obligations pruned at shutdown", { count: pruned });
		},

		// Sessions still mid-conversation at teardown feed #7536 restart-failure
		// counting (run.py:_stop_impl_body active_agents gate parity).
		// Drain-start snapshot (see drainState above) — the db is closed by the
		// time the engine runs this step, and Hermes counts who was active AT
		// DRAIN START, not whoever holds a lease after teardown (#7536 parity).
		activeSessionKeys: () => drainState.activeKeys,
	};
}

/**
 * Boot-recovery overlays: pending-obligation redelivery as the boot send this
 * layer can honor today (run.py:_redeliver_pending_obligations), deliverable-
 * platform filtered by CONNECTED adapters so absent platforms never spend an
 * attempt. Exact-turn recovery / suspension / resume scheduling pass through
 * from the host when wired; unwired hooks stay absent (choreography skips).
 */
function buildBootRecoveryOverlays(
	input: GatewayRunInput,
	storeOf: () => { db: Database } | null,
	connected: Set<string>,
): BootRecoveryHooks {
	const log = input.logger ?? stderrLogger();
	return {
		...(input.bootRecovery ?? {}),
		bootSends: async () => {
			const store = storeOf();
			const sender = input.deliverySender;
			if (store === null || sender === undefined) return;
			if (connected.size === 0) {
				log.warn(
					"pending obligation redelivery skipped — no adapters connected",
					{ reason_code: "no_deliverable_platforms" },
				);
				return;
			}
			const ledger = new DeliveryLedger(store.db);
			// One-shot tick: exact sweep/claim/drive semantics incl. #91969
			// claim-time resume-clearing; the background loop stays unstarted.
			const scheduler = new ObligationRetryScheduler(ledger, sender, {
				deliverablePlatforms: new Set(connected),
			});
			const report = await scheduler.tick();
			const claimed = report.recovered + report.retried;
			if (claimed > 0) {
				const delivered = report.results.filter((r) => r.ok).length;
				log.info("obligation redelivery at boot", { claimed, delivered });
			}
		},
	};
}

// ─────────────────────────────────────────────────────────────────────────────
// Composition + run loop.
// ─────────────────────────────────────────────────────────────────────────────

/**
 * Owned cron ticker entry: ticker stop ALSO closes the jobs store so the
 * jobs-file lock releases deterministically at teardown (never stranded).
 * Store construction happens INSIDE start() — a broken path classifies as the
 * service's loud degrade instead of failing composition.
 */
function ownedCronEntry(
	hosting: CronHosting,
	home: string,
	log: Logger,
): EmbeddedServiceEntry {
	let built: EmbeddedServiceEntry | null = null;
	return {
		name: CRON_TICKER_SERVICE_NAME,
		start: async (): Promise<EmbeddedServiceOutcome> => {
			if (built === null) {
				const store = new CronJobStore({
					paths: defaultCronStorePaths(home),
				});
				const deps: StartCronTickerOptions = {
					store,
					runner: hosting.runner,
					log,
					...(hosting.intervalSeconds !== undefined
						? { intervalSeconds: hosting.intervalSeconds }
						: {}),
					...(hosting.deliverySink !== undefined
						? { deliverySink: hosting.deliverySink }
						: {}),
				};
				const inner = cronTickerServiceEntry(deps);
				built = {
					name: inner.name,
					start: async () => {
						const outcome = await inner.start();
						if (!outcome.ok || outcome.handle === undefined) return outcome;
						const innerStop = outcome.handle.stop?.bind(outcome.handle);
						const handle: EmbeddedServiceHandle = {
							name: outcome.handle.name,
							stop: async () => {
								await innerStop?.();
								store.close(); // jobs-file lock released at teardown
							},
						};
						return { ok: true, handle };
					},
				};
			}
			// A second start() call (partial restart) must not rebuild the store.
			return built.start();
		},
	};
}

/** Composed handle: the constructed lifecycle plus observability seams. */
export interface ComposedGateway {
	lifecycle: GatewayLifecycle;
	/** Platforms whose adapter entry reported a successful connect. */
	connectedPlatforms(): readonly string[];
}

/**
 * Compose 'pi gateway run': construct GatewayLifecycle with production stage
 * entries, REAL drain-hook/boot-recovery overlays, and the reconnect watcher
 * registered as a stage-8 entry. Services/adapters register BEFORE startup()
 * runs (run.py:start_gateway binding order; 01 §3.1 stage order is binding).
 */
export function composeGatewayLifecycle(
	input: GatewayRunInput,
): ComposedGateway {
	const logger = input.logger ?? stderrLogger();
	const derived = deriveAdapterEntries(input);

	// The store opens at engine stage 6 — overlays reach it lazily through
	// this box rather than pretending it exists at composition time.
	const box: { lifecycle: GatewayLifecycle | null } = { lifecycle: null };
	const storeOf = (): { db: Database } | null => box.lifecycle?.store ?? null;

	const lifecycle = new GatewayLifecycle({
		...(input.home !== undefined ? { home: input.home } : {}),
		...(input.replace !== undefined ? { replace: input.replace } : {}),
		logger,
		...(input.drainGraceMs !== undefined
			? { drainGraceMs: input.drainGraceMs }
			: {}),
		...(input.timers !== undefined ? { timers: input.timers } : {}),
		shutdownHooks: buildShutdownHookOverlays(input, storeOf, derived.connected),
		bootRecovery: buildBootRecoveryOverlays(input, storeOf, derived.connected),
		reconnectHooks: input.reconnectBackend ?? defaultReconnectBackend(input),
		...(input.commandRegistry !== undefined
			? { commandRegistry: input.commandRegistry }
			: {}),
	});
	box.lifecycle = lifecycle;

	const composed: ComposedGateway = {
		lifecycle,
		connectedPlatforms: () => [...derived.connected],
	};

	// ── stage 7: cron provider (Hermes binds one; hosts wire the executor/
	// transport through CronHosting — absent wiring is TRUE "not configured").
	if (input.cron !== undefined) {
		lifecycle.registerService(
			"cron_scheduler",
			ownedCronEntry(input.cron, lifecycle.home, logger),
		);
	}

	// ── stage 8 + stage 9 bindings.
	registerEmbeddedWatchers(input, lifecycle);
	for (const entry of derived.entries) lifecycle.registerAdapter(entry);

	return composed;
}

/**
 * Stage-8 bindings: host-wired watchers, extra sibling entries, and the
 * supervised platform reconnect watcher (always last: it must observe
 * every enqueue the adapter stage makes after stage 9 runs).
 */
function registerEmbeddedWatchers(
	input: GatewayRunInput,
	lifecycle: GatewayLifecycle,
): void {
	if (input.handoffWatcher !== undefined) {
		lifecycle.registerService(
			"embedded_watchers",
			handoffWatcherServiceEntry(input.handoffWatcher),
		);
	}
	if (input.kanban?.dispatcher !== undefined) {
		lifecycle.registerService(
			"embedded_watchers",
			kanbanDispatcherServiceEntry(input.kanban.dispatcher),
		);
	}
	if (input.kanban?.notifier !== undefined) {
		lifecycle.registerService(
			"embedded_watchers",
			kanbanNotifierServiceEntry(input.kanban.notifier),
		);
	}
	for (const entry of input.extraWatchers ?? []) {
		lifecycle.registerService("embedded_watchers", entry);
	}
	lifecycle.registerService(
		"embedded_watchers",
		lifecycle.reconnectWatcherService,
	);
}

/** Process-level result of a full run (exit-code contract, 08 §1.2). */
export interface GatewayRunResult {
	exitCode: number;
	/** True when startup reached running and a drain completed. */
	ran: boolean;
}

/**
 * Host hook fired after READY (stage 10 ok) and before parking on shutdown —
 * the composition-root seam for post-start work (run.py:start_gateway does
 * boot sends / cron start between runner.start() and wait_for_shutdown;
 * the redelivery half of that lives in the boot-recovery overlay here).
 */
export type StartupOkHook = (
	gateway: ComposedGateway,
	result: import("../pi_gateway/lifecycle/index.js").StartupResult,
) => void | Promise<void>;

/**
 * Run until interrupted: compose → signals → startup → park on shutdown →
 * exit code (start_gateway's boolean→exit mapping: a failed start returns
 * non-zero so the supervisor revives us; planned stops/takeovers exit 0;
 * SIGUSR1 service-restart exits 75; unexpected signals exit 1).
 */
export async function runGateway(
	input: GatewayRunInput,
): Promise<GatewayRunResult> {
	const composed = composeGatewayLifecycle(input);
	const { lifecycle } = composed;
	if (input.installSignals !== false) lifecycle.installSignalHandlers();
	const startup = await lifecycle.startup();
	if (!startup.ok) {
		lifecycle.dispose(); // early-exit paths release ownership + close the db
		return { exitCode: startup.exitCode ?? 1, ran: false };
	}
	if (input.onStartupOk !== undefined)
		await input.onStartupOk(composed, startup);
	const outcome = await lifecycle.waitShutdown();
	return { exitCode: outcome.exitCode, ran: true };
}
