// pi_agent_core/runner.ts — the runner loop: resolved session + inbound
// message → REAL host pi AgentSession driven to its final text (DEC-023: the
// host agent loop is reused DIRECTLY via createAgentSession/prompt/steer/
// abort — never re-implemented, never shimmed).
//
// Loop-integration responsibilities ported around the host loop (05 §4):
//   - cached agent instances (byte-stable system prompt + toolset per session)
//     with LRU + DEC-021 memory-pressure shedding;
//   - replay seeding from pi_state rows (persist-what-you-send sidecars);
//   - alternation repair as a PRE-CALL chokepoint over EACH model call
//     (DEC-015: "immediately before EACH API call") — armed on the host
//     loop's transformContext seam for the duration of prompt(), it repairs
//     the exact context list the loop converts to the wire copy, INCLUDING
//     the freshly appended user turn (conversation_loop.py parity: the
//     repair fires inside run_conversation per API call, merging crash/
//     interrupt user;user tails with the new ask); persisted bytes are never
//     rewritten;
//   - budget/grace iteration semantics + recorded exit reasons (05 §4.1),
//     enforced over the host loop's turn_end event stream — the narrowest real
//     SDK seam for "iterations" (each turn_end = one completed model call);
//     the default cap is the UNLIMITED sentinel (config.py:TURN_LIMIT_UNLIMITED
//     parity — a finite cap exists only when agent.max_turns is configured);
//   - two-layer turn-lease prologue (02 §5, DEC-004): L1 in-process registry
//     acquire/release keyed on the resolved session id + durable DB lease via
//     StateStore.leases (run_agent.py:run_conversation turn prologue parity:
//     structured holder pid/turn/platform, ttl 300 / wait 1800 / on_wait /
//     should_abort, waited ⇒ resume-tip re-resolve + transcript reload,
//     60s holder-scoped refresh daemon, fail-closed timeout ⇒ resend notice);
//   - interrupt at boundaries via session.abort() (/stop analogue);
//   - steer drain placement delegated to the host queue (delivered after the
//     current assistant turn finishes its tool calls, before the next model
//     call — docs/sdk.md steer());
//   - checkpoint dedup ledger reset per turn (05 §4 pseudocode);
//   - memory turn-boundary contract (05 §4.2): prefetch once before the tool
//     loop; interrupted turns skip BOTH sync and warming (#15218).
//
// Hermes anchors: run_agent.py:AIAgent.run_conversation →
// agent/conversation_loop.py::run_conversation; gateway/run.py::_agent_cache;
// gateway/run.py::_session_expiry_watcher (300s idle sweep of the agent cache).
import type DatabaseType from "better-sqlite3";

import {
	AgentInstanceCache,
	defaultAgentCacheMaxTotalBytes,
	transcriptPersistenceCaughtUp,
	type AgentCacheOptions,
} from "./agent-cache.js";
import {
	repairMessageSequence,
	sanitizeToolCallArguments,
	repairToolCallArgumentsJson,
} from "./alternation-repair.js";
import {
	ConversationState,
	runWithConversation,
} from "./conversation-state.js";
import {
	createAgentSession,
	DefaultResourceLoader,
	SessionManager,
	SettingsManager,
	type Api,
	type AssistantMessage,
	type CreateAgentSessionOptions,
	type Message,
	type Model,
	type StopReason,
	type TextContent,
	type ToolCall,
	type ToolResultMessage,
	type Usage,
	type ToolDefinition,
	type AgentSession,
} from "./host.js";
import {
	DEFAULT_TTL_SECONDS,
	DEFAULT_WAIT_SECONDS,
	SESSION_ACTIVITY_HEARTBEAT_MIN_INTERVAL_SECONDS,
	SessionTurnLeaseLostError,
	isExplicitForkChildRow,
	structuredHolder,
} from "../pi_state/index.js";
import {
	readReplayMessages,
	substituteApiContent,
	type MessageRow,
	type NewMessage,
	type TokenDelta,
} from "../pi_state/index.js";
import type {
	TurnExitReason,
	TurnOutcome,
	TurnUsageSnapshot,
} from "./runner-types.js";
import { TurnWorkerPool } from "./worker-pool.js";

/** Memory turn-boundary hooks (05 §4.2). All optional; failures never break a turn. */
export interface MemoryTurnHooks {
	/** Prefetch ONCE per turn, before the first model call. */
	prefetchAll?(query: string): Promise<string> | string;
	/** End-of-turn sync of durable conversational truth. */
	syncAll?(input: {
		userText: string;
		responseText: string;
	}): void | Promise<void>;
	/** Warm NEXT-turn recall in the background after a normal finalize. */
	queuePrefetchAll?(query: string): void;
	/** Trivial-prompt gate (greetings/acks skip prefetch AND warming). */
	isTrivialPrompt?(query: string): boolean;
}

/**
 * Minimal store surface the runner consumes (structural subset of
 * pi_state's StateStore — composition stays downward-only).
 */
export interface RunnerStore {
	db: DatabaseType.Database;
	appendMessage(message: NewMessage): number | Promise<number>;
	queueTokenCounts(sessionId: string, delta: TokenDelta): unknown;
	/**
	 * Cross-process turn-lease layer (02 §5). Optional so minimal test stores
	 * can omit it; StateStore satisfies this structurally via its `leases`.
	 */
	leases?: RunnerStoreLeases;
	/**
	 * Durable mid-turn activity heartbeat (hermes_state.py:
	 * touch_session_activity — observation-only, never backwards). Optional;
	 * failures never break a turn.
	 */
	touchSessionActivity?(
		sessionId: string,
		opts?: { ts?: number },
	): void | Promise<void>;
}

/** Structural subset of pi_state's DbTurnLeaseStore the runner drives. */
export interface RunnerStoreLeases {
	acquireWait(
		sessionId: string,
		holder: string,
		options?: {
			ttlSeconds?: number;
			waitSeconds?: number;
			pollIntervalSeconds?: number;
			onWait?: (elapsedSeconds: number) => void;
			shouldAbort?: () => boolean;
		},
	): Promise<boolean>;
	refresh(sessionId: string, holder: string, ttlSeconds?: number): boolean;
	releaseHolder(sessionId: string, holder: string): void;
}

/**
 * L1 in-process turn-lease registry seam (02 §5 / DEC-004). Declared
 * structurally so pi_agent_core never imports upward into pi_gateway: the
 * composition layer passes the real SessionTurnLeaseRegistry, whose
 * acquire()/release() signatures satisfy this shape. Acquire rejections (the
 * registry's TurnLeaseTimeoutError) propagate untouched to the caller — the
 * routing layer converts them into a resend notice.
 */
export interface RunnerTurnLeaseRegistry {
	acquire(
		sessionId: string,
		options: { ownerKey: string; generation: number; timeoutMs?: number },
	): Promise<unknown>;
	release(token: unknown): boolean;
}

/** Cancellable interval handle (injected-timer seam for deterministic tests). */
export interface IntervalHandle {
	cancel(): void;
}

/** Default unref'd setInterval — a background sweep must never pin exit. */
function defaultStartInterval(fn: () => void, ms: number): IntervalHandle {
	const timer = setInterval(fn, ms);
	timer.unref?.();
	return { cancel: () => clearInterval(timer) };
}

export interface GatewayAgentRunnerOptions {
	store: RunnerStore;
	systemPrompt: string;
	model: Model<Api>;
	modelRuntime: import("./host.js").ModelRuntime;
	customTools?: ToolDefinition[];
	/**
	 * Per-turn model-call budget (Hermes agent.max_turns). UNSET means
	 * UNLIMITED — the Hermes default (hermes_cli/config.py:
	 * TURN_LIMIT_UNLIMITED / resolve_turn_limit); only a configured finite cap
	 * ever bounds a turn. When a finite cap is set and the host loop would
	 * exceed it, exactly ONE grace call is allowed, then the turn ends with
	 * exitReason "budget_exhausted".
	 */
	maxIterations?: number;
	memoryHooks?: MemoryTurnHooks;
	cacheOptions?: AgentCacheOptions;
	poolMaxWorkers?: number;
	/** Injected clock (ms) for cache recency + turn timestamps. */
	now?: () => number;
	/** L1 in-process turn-lease registry (02 §5). Absent ⇒ prologue skips L1. */
	turnLeaseRegistry?: RunnerTurnLeaseRegistry;
	/** Durable-lease TTL seconds (default 300, 02 §5). */
	leaseTtlSeconds?: number;
	/** Durable-lease bounded-wait budget seconds (default 1800, 02 §5). */
	leaseWaitSeconds?: number;
	/** Durable-lease poll cadence seconds (test hook; default 1.0). */
	leasePollIntervalSeconds?: number;
	/** Holder-refresh daemon cadence ms (run_agent.py parity default 60_000). */
	leaseRefreshIntervalMs?: number;
	/** Abort probe polled while waiting on the durable lease (default never). */
	shouldAbortLeaseWait?: () => boolean;
	/** Cached-agent idle-sweep cadence ms (_session_expiry_watcher: 300_000). */
	cacheSweepIntervalMs?: number;
	/** Interval-timer seam (tests drive ticks deterministically). */
	startInterval?: (fn: () => void, ms: number) => IntervalHandle;
}

interface CachedHostSession {
	session: AgentSession;
	/**
	 * Flushed-through marker (run.py `_last_flushed_db_idx` parity): null
	 * while the live transcript's durability is UNKNOWN — from turn start
	 * until the assistant row lands durably — otherwise the live-loop message
	 * count at the moment persistence was last fully caught up. Consulted by
	 * transcriptPersistenceCaughtUp through the pressure-shedding gate; a
	 * failed/lost write leaves it null, which blocks soft eviction of the
	 * entry until the next successful turn re-stamps it (fail-closed).
	 */
	flushedDbIdx: number | null;
}

interface InflightTurn {
	session: AgentSession;
	interruptRequested: boolean;
	budgetAborted: boolean;
}

/**
 * Hermes turn-limit sentinel parity (hermes_cli/config.py:TURN_LIMIT_UNLIMITED
 * = sys.maxsize): turns are UNLIMITED unless a finite agent.max_turns cap is
 * configured. Number.MAX_SAFE_INTEGER is the JS parity — far beyond any real
 * conversation, and safe in the `startedCalls > max + 1` comparisons below.
 */
export const TURN_LIMIT_UNLIMITED = Number.MAX_SAFE_INTEGER;

/** run_agent.py turn prologue: holder-scoped refresh daemon cadence. */
export const LEASE_REFRESH_INTERVAL_MS = 60_000;

/** gateway/run.py:_session_expiry_watcher cadence (5-minute idle sweep). */
export const SESSION_EXPIRY_SWEEP_INTERVAL_MS = 300_000;

const DEFAULT_MAX_ITERATIONS = TURN_LIMIT_UNLIMITED;

/**
 * Fail-closed DB-layer lease timeout (run_agent.py prologue parity: "Fail
 * closed like gateway TurnLeaseTimeoutError … surface a resend notice instead
 * of a bare TimeoutError that looks like a hang"). Callers catch BOTH this
 * and the L1 registry's TurnLeaseTimeoutError and send the same resend notice.
 */
export class SessionTurnLeaseTimeoutError extends Error {
	readonly sessionId: string;
	constructor(sessionId: string) {
		super(`session_turn_lease_timeout:${sessionId}`);
		this.name = "SessionTurnLeaseTimeoutError";
		this.sessionId = sessionId;
	}
}

export class GatewayAgentRunner {
	private readonly store: RunnerStore;
	private readonly systemPrompt: string;
	private readonly model: Model<Api>;
	private readonly modelRuntime: GatewayAgentRunnerOptions["modelRuntime"];
	private readonly customTools: ToolDefinition[] | undefined;
	private readonly maxIterations: number;
	private readonly memoryHooks: MemoryTurnHooks | undefined;
	private readonly now: () => number;
	private readonly cache: AgentInstanceCache<CachedHostSession>;
	private readonly pool: TurnWorkerPool;
	private readonly inflight = new Map<string, InflightTurn>();
	private readonly generations = new Map<string, number>();
	/** Per-session monotonic ms of the last durable activity stamp (60s gate). */
	private readonly activityStamps = new Map<string, number>();
	private readonly turnLeaseRegistry: RunnerTurnLeaseRegistry | null;
	private readonly leaseTtlSeconds: number;
	private readonly leaseWaitSeconds: number;
	private readonly leasePollIntervalSeconds: number;
	private readonly leaseRefreshIntervalMs: number;
	private readonly shouldAbortLeaseWait: (() => boolean) | null;
	private readonly startInterval: (
		fn: () => void,
		ms: number,
	) => IntervalHandle;
	private sweepTimer: IntervalHandle | null;
	private closed = false;

	constructor(options: GatewayAgentRunnerOptions) {
		this.store = options.store;
		this.systemPrompt = options.systemPrompt;
		this.model = options.model;
		this.modelRuntime = options.modelRuntime;
		this.customTools = options.customTools;
		this.maxIterations = options.maxIterations ?? DEFAULT_MAX_ITERATIONS;
		this.memoryHooks = options.memoryHooks;
		this.now = options.now ?? (() => Date.now());
		// DEC-021 startup wiring (agent_cache_pressure.py "auto" bounds parity):
		// absent an operator byte bound, the pressure budget is DERIVED from the
		// cgroup quota / host memory (0.65 fraction, 512MB floor) so shedPressure
		// is live on every production construction path — the resident size of
		// the cached-agent core stays bounded without configuration.
		//
		// Evictability gates (run.py parity — GatewayRunner owns the closures,
		// exactly as _sweep_agent_cache_under_pressure builds _is_evictable and
		// _enforce_agent_cache_cap snapshots running_ids):
		//  • pressure shedding admits an entry only when it is NOT mid-turn AND
		//    its transcript persistence has caught up;
		//  • the LRU entry-cap enforcer skips mid-turn holders only.
		this.cache = new AgentInstanceCache<CachedHostSession>({
			...options.cacheOptions,
			maxTotalBytes:
				options.cacheOptions?.maxTotalBytes ?? defaultAgentCacheMaxTotalBytes(),
			isEvictable: (sessionId, cached) =>
				!this.isTurnActive(sessionId) && transcriptPersistenceCaughtUp(cached),
			isCapEvictable: (sessionId) => !this.isTurnActive(sessionId),
		});
		this.pool = new TurnWorkerPool({
			maxWorkers: options.poolMaxWorkers ?? 10,
		});
		this.turnLeaseRegistry = options.turnLeaseRegistry ?? null;
		this.leaseTtlSeconds = options.leaseTtlSeconds ?? DEFAULT_TTL_SECONDS;
		this.leaseWaitSeconds = options.leaseWaitSeconds ?? DEFAULT_WAIT_SECONDS;
		this.leasePollIntervalSeconds = options.leasePollIntervalSeconds ?? 1.0;
		this.leaseRefreshIntervalMs =
			options.leaseRefreshIntervalMs ?? LEASE_REFRESH_INTERVAL_MS;
		this.shouldAbortLeaseWait = options.shouldAbortLeaseWait ?? null;
		this.startInterval = options.startInterval ?? defaultStartInterval;
		// gateway/run.py:_session_expiry_watcher parity: periodic unref'd idle
		// sweep of the cached-agent instance cache; timer injectable so tests drive
		// ticks without real 300s waits.
		this.sweepTimer = this.startInterval(() => {
			this.cache.sweepIdle();
		}, options.cacheSweepIntervalMs ?? SESSION_EXPIRY_SWEEP_INTERVAL_MS);
	}

	get poolStats(): { active: number; pending: number; max: number } {
		return {
			active: this.pool.active,
			pending: this.pool.pending,
			max: this.pool.maxConcurrent,
		};
	}

	/** Diagnostics: current cache entry count / byte estimate total. */
	get cacheStats(): { entries: number; bytes: number } {
		return { entries: this.cache.size, bytes: this.cache.totalBytes };
	}

	/** Diagnostics: is this session's host agent instance currently cached? */
	isCached(sessionId: string): boolean {
		return this.cache.has(sessionId);
	}

	/**
	 * Effective DEC-021 pressure bound the cache enforces — the operator byte
	 * override when provided, else the startup-derived memory budget.
	 */
	get cacheByteBudget(): number {
		return this.cache.byteBudget;
	}

	/** Diagnostics: cached session ids in LRU→MRU order. */
	get cachedSessionIds(): string[] {
		return this.cache.keys();
	}

	/** True while a turn occupies a worker slot for this session. */
	isTurnActive(sessionId: string): boolean {
		return this.inflight.has(sessionId) || this.pool.isRunning(sessionId);
	}

	/**
	 * Drive ONE turn end-to-end. Executes on the bounded worker pool under the
	 * session's ConversationState context (DEC-020).
	 */
	async handleTurn(request: {
		sessionId: string;
		routingKey: string;
		text: string;
	}): Promise<TurnOutcome> {
		if (this.closed) throw new Error("runner is closed");
		const generation = (this.generations.get(request.sessionId) ?? 0) + 1;
		this.generations.set(request.sessionId, generation);

		return this.pool.submit({
			key: request.sessionId,
			generation,
			run: async () => {
				const state = new ConversationState(request.sessionId, {
					routingKey: request.routingKey,
					generation,
				});
				return runWithConversation(state, () => this.runTurn(request, state));
			},
		});
	}

	/** Queue a steering message onto the in-flight turn (host drain placement). */
	async steer(sessionId: string, text: string): Promise<void> {
		const turn = this.inflight.get(sessionId);
		if (!turn) throw new Error(`no in-flight turn for ${sessionId}`);
		await turn.session.steer(text);
	}

	/**
	 * /stop analogue: mark the in-flight turn interrupted and abort the host
	 * loop; abort() resolves when the agent is idle again.
	 */
	async interrupt(sessionId: string): Promise<boolean> {
		const turn = this.inflight.get(sessionId);
		if (!turn) return false;
		turn.interruptRequested = true;
		await turn.session.abort();
		return true;
	}

	/** Drop the cached session for a session id (e.g. post-compression rekey). */
	dropCachedSession(sessionId: string): void {
		const entry = this.cache.peek(sessionId);
		entry?.session.dispose();
		this.cache.delete(sessionId);
	}

	async close(): Promise<void> {
		this.closed = true;
		this.sweepTimer?.cancel();
		this.sweepTimer = null;
		for (const key of this.cache.keys()) this.dropCachedSession(key);
	}

	// ------------------------------------------------------------------

	private async runTurn(
		request: { sessionId: string; routingKey: string; text: string },
		state: ConversationState,
	): Promise<TurnOutcome> {
		state.turnStartedAt = this.now();
		state.checkpointLedger.newTurn();
		state.iterations = 0;
		state.exitReason = null;

		// ---- Turn-lease prologue (02 §5 two-layer serialization, DEC-004) ---
		// L1 first (in-process registry keyed on the RESOLVED session id,
		// gateway/turn_lease.py:SessionTurnLeaseRegistry.acquire parity), then
		// the durable DB lease via StateStore.leases keyed on the
		// compression-lineage root (run_agent.py:run_conversation turn
		// prologue: structured pid/turn/platform holder, ttl 300 / wait 1800 /
		// on_wait / should_abort). A fresh session id skips the durable layer —
		// process-unique, nothing to race over (#84234 probe-failure parity:
		// hasDurableSessionRow fails CLOSED).
		const generation = state.generation;
		let l1Token: unknown = null;
		if (this.turnLeaseRegistry !== null) {
			l1Token = await this.turnLeaseRegistry.acquire(request.sessionId, {
				ownerKey: request.routingKey,
				generation,
			});
		}
		let dbHolder: string | null = null;
		let waited = false;
		if (
			this.store.leases !== undefined &&
			this.hasDurableSessionRow(request.sessionId)
		) {
			dbHolder = structuredHolder(
				`turn=g${generation}:platform=${platformFromRoutingKey(request.routingKey)}`,
				process.pid,
			);
			const acquired = await this.store.leases.acquireWait(
				request.sessionId,
				dbHolder,
				{
					ttlSeconds: this.leaseTtlSeconds,
					waitSeconds: this.leaseWaitSeconds,
					pollIntervalSeconds: this.leasePollIntervalSeconds,
					onWait: () => {
						waited = true;
					},
					shouldAbort: () => this.shouldAbortLeaseWait?.() ?? false,
				},
			);
			if (!acquired) {
				// Fail closed like gateway TurnLeaseTimeoutError: do not enter
				// load/run/flush; the caller surfaces a resend notice.
				throw new SessionTurnLeaseTimeoutError(request.sessionId);
			}
		}

		// Waited ⇒ another process may have compressed/rotated the session while
		// we queued: re-resolve the resume tip BEFORE loading history and reload
		// the latest transcript below (run_agent.py `_lease_waited` ⇒
		// resolve_resume_session_id + get_messages_as_conversation reload).
		let effectiveSessionId = request.sessionId;
		if (waited) {
			effectiveSessionId = this.resolveResumeSessionId(request.sessionId);
			if (effectiveSessionId !== request.sessionId) {
				this.dropCachedSession(request.sessionId);
			}
		}

		try {
			return await this.driveTurn(
				request,
				state,
				effectiveSessionId,
				waited,
				dbHolder,
			);
		} finally {
			if (dbHolder !== null) {
				try {
					// Holder-scoped delete: idempotent, and a stale unwind can
					// never free a successor's row (release_session_turn_lease).
					this.store.leases?.releaseHolder(request.sessionId, dbHolder);
				} catch {
					/* release failure must never mask the turn result */
				}
			}
			// L1 released last — symmetric with acquire-first nesting.
			if (l1Token !== null) this.turnLeaseRegistry?.release(l1Token);
		}
	}

	/** True when the sessions table holds a durable row for this id. */
	private hasDurableSessionRow(sessionId: string): boolean {
		try {
			const row = this.store.db
				.prepare("SELECT id FROM sessions WHERE id = ? LIMIT 1")
				.get(sessionId);
			return row !== undefined;
		} catch {
			return true; // #84234: probe failure ⇒ acquire rather than run unsynchronized
		}
	}

	/**
	 * hermes_state.py:resolve_resume_session_id parity (read-only): walk
	 * forward to the compression tip (get_compression_tip semantics — children
	 * of compression-ended parents only, explicit fork/tool children excluded),
	 * then follow most-recently-started eligible children to the deepest node
	 * holding messages. Depth-capped; returns the input id unchanged when no
	 * descendant holds messages.
	 */
	private resolveResumeSessionId(sessionId: string): string {
		if (!sessionId) return sessionId;
		const db = this.store.db;
		const compressionChildren = db.prepare(`
			SELECT child.id, child.source, child.model_config, child.parent_session_id
			FROM sessions parent JOIN sessions child ON child.parent_session_id = parent.id
			WHERE parent.id = ? AND parent.end_reason = 'compression'
			ORDER BY child.started_at DESC, child.id DESC
		`);
		const anyChildren = db.prepare(`
			SELECT id, source, model_config, parent_session_id FROM sessions
			WHERE parent_session_id = ?
			ORDER BY started_at DESC, id DESC
		`);
		const hasMessages = db.prepare(
			"SELECT 1 FROM messages WHERE session_id = ? LIMIT 1",
		);
		interface ChildRow {
			id: string;
			source: string | null;
			model_config: string | null;
			parent_session_id: string | null;
		}
		const eligible = (rows: ChildRow[]): string | undefined => {
			for (const row of rows) {
				if (
					isExplicitForkChildRow({
						source: row.source,
						model_config: row.model_config,
						parent_session_id: row.parent_session_id,
					})
				) {
					continue;
				}
				return String(row.id);
			}
			return undefined;
		};

		try {
			// Step 1: compression-tip walk (bounded 100, cycle-safe).
			let tip = sessionId;
			const seenTip = new Set<string>([tip]);
			for (let hop = 0; hop < 100; hop++) {
				const next = eligible(compressionChildren.all(tip) as ChildRow[]);
				if (next === undefined || seenTip.has(next)) break;
				seenTip.add(next);
				tip = next;
			}

			// Step 2: empty-head walk to the deepest descendant WITH messages.
			let best: string | null = null;
			let current: string | undefined = tip;
			const seen = new Set<string>([current]);
			for (let depth = 0; depth < 32 && current !== undefined; depth++) {
				if (hasMessages.get(current) !== undefined) best = current;
				const next = eligible(anyChildren.all(current) as ChildRow[]);
				if (next === undefined || seen.has(next)) break;
				seen.add(next);
				current = next;
			}
			return best ?? sessionId;
		} catch {
			return sessionId; // resolution failure never blocks the waited turn
		}
	}

	// ------------------------------------------------------------------

	/**
	 * Drive ONE admitted turn end-to-end on the bounded worker pool (runs
	 * inside runTurn's lease-scoped try — every resource this method touches is
	 * released by runTurn's finally or here).
	 */
	private async driveTurn(
		request: { sessionId: string; routingKey: string; text: string },
		state: ConversationState,
		sessionId: string,
		waited: boolean,
		dbHolder: string | null,
	): Promise<TurnOutcome> {
		const host = await this.acquireHostSession(sessionId);
		const session = host.session;
		// Turn-start flush-cursor reset (run.py:_init_cached_agent_for_turn
		// parity: "Reset the SessionDB flush cursor so the new turn's messages
		// are fully persisted"). From here until the assistant row lands
		// durably the entry is persistence-UNCAUGHT-UP, so pressure shedding
		// cannot touch it even in the post-inflight window between prompt()
		// resolving and the assistant-row append below; only a fully successful
		// write re-stamps it.
		host.flushedDbIdx = null;
		if (waited) {
			// "Session is free; loading the latest transcript..." parity: the
			// cached history may predate the wait — reload from durable rows.
			await this.seedReplay(session, sessionId);
		}

		// Holder-scoped refresh daemon: long model/tool turns outlive a fixed
		// TTL; refresh() losing means another process reclaimed the lineage slot
		// ⇒ abort to protect the transcript (run_agent.py:
		// _refresh_durable_turn_lease hard-interrupt parity).
		let refresher: IntervalHandle | null = null;
		let leaseLost = false;
		if (dbHolder !== null && this.store.leases !== undefined) {
			const leases = this.store.leases;
			refresher = this.startInterval(() => {
				let ok = false;
				try {
					ok = leases.refresh(sessionId, dbHolder, this.leaseTtlSeconds);
				} catch {
					ok = false;
				}
				if (!ok && !leaseLost) {
					leaseLost = true;
					void session.abort().catch(() => {});
				}
			}, this.leaseRefreshIntervalMs);
		}

		// ---- Mid-turn activity heartbeat (hermes_state.py:
		// touch_session_activity) -------------------------------------------
		// Rate-limited (≥60s per session) durable last_activity_at stamp so
		// freshest-of sibling ordering (_sql_session_last_active consumers like
		// get_compression_tip) sees a LIVE session even before its next message
		// row lands. Observation-only: failures never break a turn.
		this.stampActivityIfDue(sessionId);

		// Append-time lease-loss detection: the in-txn admission guard
		// (_check_transcript_write_guards) may reject the assistant flush after
		// another process reclaimed the lineage slot. Hermes' conversation_loop
		// catches flush persistence errors and records the turn as failed
		// (session_persistence_failed, cause "turn_lease") instead of crashing;
		// the flag below folds this into the SAME recorded outcome shape the
		// refresher-abort path produces.
		let appendLeaseLost = false;

		// ---- Pre-call alternation repair CHOKEPOINT (DEC-015) ----------------
		// DEC-015's own contract places the pass "immediately before EACH API
		// call". Hermes anchors exactly that:
		// agent/conversation_loop.py:run_conversation runs
		// _sanitize_tool_call_arguments + repair_message_sequence_with_cursor
		// INSIDE the per-API-call loop, over the message list INCLUDING the
		// freshly appended user turn, before api_messages are built. A one-shot
		// pre-append pass (what this replaced) repaired only the seeded history
		// BEFORE prompt() appended the live user turn — so a trailing durable
		// user row (crash/interrupt between the user-row persist and the
		// assistant reply, or gateway multi-queue replay tails) reached the
		// provider alongside the new ask as consecutive users.
		//
		// Host-loop seam: Agent.transformContext runs before EVERY model call,
		// after steering/follow-up injection, over the exact context list the
		// loop converts into the wire copy (pi-agent-core
		// agent-loop.js:streamAssistantResponse) — the unconditional pre-send
		// chokepoint parity seam. Repairs mutate that list in place
		// (Hermes `messages[:] = merged` parity); gateway-owned durable rows
		// are never touched here.
		let repairs = 0;
		const hostAgent = session.agent;
		const priorTransform = hostAgent.transformContext;
		hostAgent.transformContext = async (messages) => {
			try {
				const loop = messages as unknown as Message[];
				repairs += repairMessageSequence(loop);
				// Companion pre-request sanitation (DEC-015 repair family):
				// corrupted tool_call arguments JSON is repaired before the
				// request goes out instead of silently degrading
				// (sanitize_tool_call_arguments).
				repairs += sanitizeToolCallArguments(loop);
			} catch {
				// Loop contract: transformContext must never reject — degrade to
				// the unrepaired sequence rather than kill the turn.
			}
			return messages;
		};

		// ---- Persist user row BEFORE prompting (crash-safe ordering) --------
		// The append carries the turn's durable holder: admission is verified
		// INSIDE the write txn (hermes_state.py:_check_transcript_write_guards
		// via insertMessageInTx) — a >TTL-stalled writer whose lease another
		// process reclaimed raises SessionTurnLeaseLostError here instead of
		// interleaving its flush into the new owner's transcript. An expired-
		// but-still-matching lease renews in that same txn (starved-refresher
		// recovery without weakening the foreign-holder fence).
		const userRowId = await this.store.appendMessage({
			sessionId,
			role: "user",
			content: request.text,
			apiContent: request.text,
			...(dbHolder !== null
				? {
						turnLeaseHolder: dbHolder,
						turnLeaseTtlSeconds: this.leaseTtlSeconds,
					}
				: {}),
		});

		// ---- Memory: prefetch ONCE before the tool loop ----------------------
		const query = request.text;
		if (
			this.memoryHooks?.prefetchAll &&
			!(this.memoryHooks.isTrivialPrompt?.(query) ?? false)
		) {
			try {
				state.extPrefetchCache = await this.memoryHooks.prefetchAll(query);
			} catch {
				state.extPrefetchCache = null; // provider failure never blocks a turn
			}
		}

		// ---- Event-driven enforcement over the REAL loop ---------------------
		const inflight: InflightTurn = {
			session,
			interruptRequested: false,
			budgetAborted: false,
		};
		this.inflight.set(request.sessionId, inflight);

		let lastAssistantError: string | undefined;
		let lastStopReason: StopReason | undefined;
		// Mutable holder: TS flow analysis can't see callback assignments to a
		// `let`, so read observed state through properties, never a bare let.
		const observed: { final: AssistantMessage | null } = { final: null };
		/** Model calls STARTED (turn_start precedes every model call). */
		let startedCalls = 0;

		const unsubscribe = session.subscribe((event) => {
			if (event.type === "turn_start") {
				startedCalls += 1;
				state.checkpointLedger.record(
					`iteration:${state.iterations + 1}:start`,
				);
				// Budget gate BEFORE the model call (Hermes checks the budget at
				// the loop top, before call_model): allowance = maxIterations
				// normal calls + exactly ONE grace call. Anything beyond is cut
				// off before it streams.
				if (
					startedCalls > this.maxIterations + 1 &&
					!inflight.budgetAborted &&
					!inflight.interruptRequested
				) {
					inflight.budgetAborted = true;
					void session.abort().catch(() => {});
				}
				return;
			}
			if (event.type === "turn_end") {
				const finished = event.message as unknown as AssistantMessage;
				lastStopReason = finished.stopReason;
				lastAssistantError = finished.errorMessage;
				// An aborted/errored cycle is NOT a completed iteration (the
				// budget gate kills the not-allowed call mid-stream; it must not
				// count against the turn's iteration record).
				if (
					finished.stopReason !== "aborted" &&
					finished.stopReason !== "error"
				) {
					state.iterations += 1;
					observed.final = finished;
				}
				state.checkpointLedger.record(`iteration:${state.iterations}:end`);
				// Budget + grace (05 §4.1): after `maxIterations` calls the NEXT
				// call IS the single grace call; when it completes we stop.
				if (
					state.iterations >= this.maxIterations + 1 &&
					!inflight.budgetAborted &&
					!inflight.interruptRequested
				) {
					inflight.budgetAborted = true;
					void session.abort().catch(() => {});
				}
				return;
			}
			if (event.type === "message_end") {
				const msg = event.message as unknown as Message;
				if (msg.role === "assistant") {
					lastStopReason = msg.stopReason;
					lastAssistantError = msg.errorMessage;
				}
			}
		});

		try {
			await session.prompt(request.text);
		} finally {
			// Unarm the chokepoint: post-turn session work (auto-compaction,
			// branch summaries) must not route through the turn's repair pass.
			// Restore exactly what was armed before this turn (absent stays
			// absent — exactOptionalPropertyTypes forbids assigning undefined).
			if (priorTransform === undefined) {
				delete hostAgent.transformContext;
			} else {
				hostAgent.transformContext = priorTransform;
			}
			unsubscribe();
			this.inflight.delete(request.sessionId);
			// Tear down the holder-scoped lease refresher with the turn; the
			// daemon must not outlive the inflight entry it serves.
			refresher?.cancel();
		}
		state.repairCount = repairs;

		// ---- Exit reason (recorded, never silent) ----------------------------
		// Precedence mirrors Hermes: an EXTERNAL interrupt wins; a budget abort
		// counts as budget_exhausted UNLESS the grace call delivered the final
		// payload (stopReason "stop" ⇒ the loop would have finalized anyway);
		// provider errors surface as "error". A LOST durable lease overrides all
		// of these — the turn was aborted to protect the transcript, not by the
		// user or the budget. (Budget-cut error-shaped cycles re-bucket to
		// budget_exhausted per DEC-071.)
		const interrupted =
			inflight.interruptRequested || lastStopReason === "aborted";
		// Budget-cut cycles surface in TWO host shapes (DEC-071): (a) the cut-off
		// call ends aborted (stopReason "aborted"); (b) the cut-off call ends as a
		// provider-style error cycle whose only complaint is generic abort
		// boilerplate (host ModelRuntime maps an in-flight abort through its
		// lazyStream setup path to stopReason "error" / "This operation was
		// aborted"). Shape (b) is only claimed when a budget abort actually fired.
		const budgetAbortErrorShape =
			inflight.budgetAborted &&
			lastStopReason === "error" &&
			isGenericAbortBoilerplate(lastAssistantError);
		let exitReason: TurnExitReason;
		if (leaseLost || appendLeaseLost) {
			exitReason = "error";
		} else if (inflight.interruptRequested) {
			exitReason = "interrupted_by_user";
		} else if (
			inflight.budgetAborted &&
			(budgetAbortErrorShape ||
				(lastStopReason !== "stop" && lastStopReason !== "error"))
		) {
			exitReason = "budget_exhausted";
		} else if (lastAssistantError || lastStopReason === "error") {
			exitReason = "error";
		} else if (interrupted && !inflight.budgetAborted) {
			exitReason = "interrupted_by_user"; // foreign abort
		} else exitReason = "finalized";
		state.exitReason = exitReason;

		// ---- Authoritative final payload --------------------------------------
		const assistantMsg: AssistantMessage | null = observed.final;
		const finalText = assistantText(assistantMsg);

		// ---- Memory sync leg (interrupted turns SKIP BOTH legs, #15218; a
		// lost-lease abort protects the transcript and stays equally silent) --
		if (
			exitReason !== "interrupted_by_user" &&
			!leaseLost &&
			!appendLeaseLost
		) {
			if (request.text.length > 0 && finalText.length > 0) {
				try {
					await this.memoryHooks?.syncAll?.({
						userText: request.text,
						responseText: finalText,
					});
				} catch {
					/* swallowed — misconfigured backend must not block delivery */
				}
			}
			if (
				request.text.length > 0 &&
				finalText.length > 0 &&
				!(this.memoryHooks?.isTrivialPrompt?.(query) ?? false)
			) {
				try {
					this.memoryHooks?.queuePrefetchAll?.(query);
				} catch {
					/* swallowed */
				}
			}
		}

		// ---- Persist assistant row + api_content sidecar (byte-exact) --------
		let assistantRowId: number | null = null;
		let usageSnapshot: TurnUsageSnapshot | null = null;
		if (assistantMsg) {
			const u: Usage | undefined = assistantMsg.usage;
			usageSnapshot = u
				? {
						inputTokens: u.input,
						outputTokens: u.output,
						cacheReadTokens: u.cacheRead,
						cacheWriteTokens: u.cacheWrite,
						totalTokens: u.totalTokens,
					}
				: null;
			const wireBytes = JSON.stringify({
				role: "assistant",
				content: assistantMsg.content,
			});
			try {
				assistantRowId = await this.store.appendMessage({
					sessionId,
					role: "assistant",
					content: finalText,
					apiContent: wireBytes,
					finishReason: assistantMsg.stopReason,
					...(usageSnapshot ? { tokenCount: usageSnapshot.outputTokens } : {}),
					...(dbHolder !== null
						? {
								turnLeaseHolder: dbHolder,
								turnLeaseTtlSeconds: this.leaseTtlSeconds,
							}
						: {}),
				});
				// Flush-cursor advance on the FULLY SUCCESSFUL write
				// (_flush_messages_to_session_db parity: _last_flushed_db_idx moves to
				// len(messages) only here) — the live transcript is durable through
				// its current length, re-arming pressure evictability for the idle
				// session. A lease-lost refusal or a thrown write leaves the marker
				// null ⇒ the entry stays un-evictable until a later turn succeeds.
				host.flushedDbIdx = session.agent.state.messages.length;
			} catch (err) {
				if (!(err instanceof SessionTurnLeaseLostError)) throw err;
				// Refused write: the reply must NOT be projected as durable when
				// a newer turn owns the lineage slot (hermes flush-failure break).
				appendLeaseLost = true;
			}
			if (usageSnapshot) {
				this.store.queueTokenCounts(sessionId, {
					inputTokens: usageSnapshot.inputTokens,
					outputTokens: usageSnapshot.outputTokens,
					cacheReadTokens: usageSnapshot.cacheReadTokens,
					cacheWriteTokens: usageSnapshot.cacheWriteTokens,
					apiCallCount: 1,
					model: `${assistantMsg.provider}/${assistantMsg.model}`,
				});
			}
		}

		const outcome: TurnOutcome = {
			exitReason,
			finalText,
			iterations: state.iterations,
			repairs,
			userRowId,
			assistantRowId,
			usage: usageSnapshot,
		};
		if (leaseLost || appendLeaseLost) {
			// The abort's generic "Request was aborted" (or a completed reply that
			// could not be persisted) must not mask the root cause: the
			// transcript-protecting lease-loss failure.
			outcome.errorMessage =
				"session turn lease lost; turn aborted to protect the transcript";
		} else if (lastAssistantError !== undefined) {
			outcome.errorMessage = lastAssistantError;
		}
		return outcome;
	}

	// ------------------------------------------------------------------

	/**
	 * One durable heartbeat per 60s window per session (agent/session_activity.py:
	 * SESSION_ACTIVITY_HEARTBEAT_MIN_INTERVAL_SECONDS contract: MUST stay ≥30s;
	 * Hermes ships 60s). The latch is set BEFORE the write — a failed stamp waits
	 * out the window exactly like a landed one, so a contended store never turns
	 * the observation into per-turn write pressure.
	 */
	private stampActivityIfDue(sessionId: string): void {
		const touch = this.store.touchSessionActivity;
		if (touch === undefined || !sessionId) return;
		const nowMs = this.now();
		const last = this.activityStamps.get(sessionId);
		if (
			last !== undefined &&
			nowMs - last < SESSION_ACTIVITY_HEARTBEAT_MIN_INTERVAL_SECONDS * 1000
		) {
			return;
		}
		this.activityStamps.set(sessionId, nowMs);
		try {
			const result = touch.call(this.store, sessionId, { ts: nowMs / 1000 });
			if (result instanceof Promise) result.catch(() => {});
		} catch {
			/* observation-only — never breaks a turn */
		}
	}

	/** Cache get-or-build so consecutive turns reuse byte-identical prompt+tools. */
	private async acquireHostSession(
		sessionId: string,
	): Promise<CachedHostSession> {
		const hit = this.cache.get(sessionId);
		if (hit) return hit;
		const built = await this.buildHostSession(sessionId);
		this.cache.set(sessionId, built, estimateSessionBytes(built.session));
		return built;
	}

	private async buildHostSession(
		sessionId: string,
	): Promise<CachedHostSession> {
		const settingsManager = SettingsManager.inMemory({});
		const resourceLoader = new DefaultResourceLoader({
			cwd: process.cwd(),
			agentDir: process.cwd(),
			systemPromptOverride: () => this.systemPrompt,
			// Deterministic prompt bytes: the gateway owns prompt composition;
			// ambient project context files (AGENTS.md discovery) must never leak
			// into messaging-gateway turns nor flip between turns.
			agentsFilesOverride: () => ({ agentsFiles: [] }),
			settingsManager,
		});
		await resourceLoader.reload();

		const createOptions: CreateAgentSessionOptions = {
			cwd: process.cwd(),
			agentDir: process.cwd(),
			sessionManager: SessionManager.inMemory(process.cwd()),
			settingsManager,
			resourceLoader,
			modelRuntime: this.modelRuntime,
			model: this.model,
			thinkingLevel: "off",
			noTools: "builtin",
		};
		if (this.customTools) {
			createOptions.customTools = [...this.customTools];
		}
		const { session } = await createAgentSession(createOptions);

		await this.seedReplay(session, sessionId);
		// Freshly built from durable rows: the live transcript IS the persisted
		// one, so the entry starts fully caught up (agent_init.py:1762 parity —
		// a rebuilt agent never carries an unflushed tail).
		return {
			session,
			flushedDbIdx: session.agent.state.messages.length,
		};
	}

	/**
	 * Seed the host loop with persisted history (02 §7.3 replay projection):
	 * sidecar substitution applies (api_content over content), tool rows map to
	 * toolResult messages, ancestors included, clone-defense dedupe on.
	 */
	private async seedReplay(
		session: AgentSession,
		sessionId: string,
	): Promise<void> {
		const rows: MessageRow[] = readReplayMessages(this.store.db, sessionId, {
			includeAncestors: true,
			dedupeReplayedUserRows: true,
		});
		if (rows.length === 0) return;
		const seeded: Message[] = [];
		for (const row of rows) {
			seeded.push(rowToLoopMessage(row, this.model));
		}
		session.agent.state.messages =
			seeded as unknown as typeof session.agent.state.messages;
	}
}

// ----------------------------------------------------------------------

/**
 * run_agent.py durable-holder parity: platform is routing-key field [2]
 * ("agent:<ns>:<platform>:…", gateway/run.py key shape); unknown when absent.
 */
function platformFromRoutingKey(routingKey: string): string {
	const parts = routingKey.split(":");
	return parts[2] && parts[2].length > 0 ? parts[2] : "unknown";
}

/** Map one persisted row onto the host loop's message vocabulary. */
export function rowToLoopMessage(row: MessageRow, model: Model<Api>): Message {
	const tsMs = Math.round(row.timestamp * 1000);
	const content = substituteApiContent(row) ?? row.content ?? "";
	if (row.role === "user") {
		return { role: "user", content, timestamp: tsMs };
	}
	if (row.role === "assistant") {
		const calls = parseToolCalls(row.tool_calls);
		const contentBlocks: Array<TextContent | ToolCall> = [
			...(content ? [{ type: "text" as const, text: content }] : []),
			...calls,
		];
		return {
			role: "assistant",
			content: contentBlocks,
			api: model.api,
			provider: model.provider,
			model: model.id,
			usage: zeroUsage(),
			stopReason: (row.finish_reason as StopReason | null) ?? "stop",
			timestamp: tsMs,
		};
	}
	// role === "tool": a persisted tool result.
	const toolResult: ToolResultMessage = {
		role: "toolResult",
		toolCallId: row.tool_call_id ?? "",
		toolName: row.tool_name ?? "",
		content: [{ type: "text", text: content }],
		isError: row.effect_disposition === "error",
		timestamp: tsMs,
	};
	return toolResult;
}

function zeroUsage(): Usage {
	return {
		input: 0,
		output: 0,
		cacheRead: 0,
		cacheWrite: 0,
		totalTokens: 0,
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
	};
}

/**
 * Decode a stored tool_calls sidecar into host ToolCall blocks. Corrupt JSON
 * goes through the DEC-015 repair ladder (_repair_tool_call_arguments parity)
 * instead of silently mapping to [] — dropping calls here orphaned their
 * results downstream; per-call arguments strings repair to objects, empty or
 * unshapeable arguments degrade to {}.
 */
export function parseToolCalls(raw: string | null): ToolCall[] {
	if (!raw || raw.trim() === "") return []; // empty tool_calls array → no blocks
	let parsed: unknown;
	try {
		parsed = JSON.parse(raw);
	} catch {
		parsed = tryParseJson(repairToolCallArgumentsJson(raw));
	}
	if (!Array.isArray(parsed)) return [];
	return parsed.flatMap((c) => {
		if (typeof c !== "object" || c === null) return [];
		const record = c as { id?: unknown; name?: unknown; arguments?: unknown };
		if (
			typeof record.id !== "string" ||
			record.id === "" ||
			typeof record.name !== "string" ||
			record.name === ""
		) {
			return []; // identity cannot be repaired — drop this call only
		}
		return [
			{
				type: "toolCall" as const,
				id: record.id,
				name: record.name,
				arguments: decodeToolArguments(record.arguments),
			},
		];
	});
}

function tryParseJson(text: string): unknown {
	try {
		return JSON.parse(text);
	} catch {
		return undefined;
	}
}

function decodeToolArguments(args: unknown): Record<string, unknown> {
	if (typeof args === "string") {
		const parsed = tryParseJson(repairToolCallArgumentsJson(args));
		if (
			typeof parsed === "object" &&
			parsed !== null &&
			!Array.isArray(parsed)
		) {
			return parsed as Record<string, unknown>;
		}
		return {};
	}
	if (args === undefined || args === null || args === "") return {};
	if (typeof args === "object" && !Array.isArray(args)) {
		return args as Record<string, unknown>;
	}
	return {};
}

/**
 * True when the message is exactly the generic abort boilerplate the host
 * emits for an in-flight abort (both the legacy "Request was aborted" text and
 * the DOMException-derived "This operation was aborted"). Anything else — a
 * provider message, a quota error, a timeout classification — is NOT abort
 * boilerplate and must not be re-bucketed by the budget classifier.
 */
function isGenericAbortBoilerplate(message: string | undefined): boolean {
	if (message === undefined) return false;
	const normalized = message.trim().toLowerCase();
	return (
		normalized === "request was aborted" ||
		normalized === "this operation was aborted"
	);
}

function assistantText(msg: AssistantMessage | null): string {
	if (!msg) return "";
	const out: string[] = [];
	for (const b of msg.content) {
		if (b.type === "text") out.push(b.text);
	}
	return out.join("");
}

function estimateSessionBytes(session: AgentSession): number {
	let total = 4096; // scaffolding baseline
	for (const m of session.agent.state.messages) {
		total += JSON.stringify(m).length * 2; // UTF-16-ish estimate
	}
	return total;
}
