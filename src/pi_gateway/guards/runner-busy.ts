// pi_gateway/guards/runner-busy.ts — the RUNNER side of the two-guard system
// (03-message-routing.md §3.1, §5.4, §6; DEC-005, #28503).
//
// Ownership split (DEC-005): the ADAPTER owns only the single pending slot;
// the FIFO overflow lives HERE on the runner (`conversation.queued_events`
// parity). Depth = slot + overflow, capped at 32 with drop-newest warning.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/run.py:_BUSY_QUEUE_MAX_PENDING            → BUSY_QUEUE_MAX_PENDING
//   gateway/run.py:_enqueue_fifo                      → enqueueFifo
//   gateway/run.py:_promote_queued_event              → promoteQueuedEvent
//   gateway/run.py:_queue_depth                       → queueDepth
//   gateway/run.py:_queue_or_replace_pending_event    → queueOrReplacePendingEvent (#28503)
//   gateway/run.py:_dispatch_busy_slash_command       → dispatchBusySlashCommand
//   gateway/run.py:_handle_message HERMES_TELEGRAM_FOLLOWUP_GRACE_SECONDS
//                                                     → follow-up grace
//   gateway/run.py:_handle_message staleness eviction (~17208,
//     HERMES_AGENT_TIMEOUT idle/age sweep)            → maybeEvictStaleRunningAgent
//   gateway/run.py:_check_slash_access (~17282 fast-path / ~17507 cold path)
//                                                     → slash-access gate
//                                                     (guards/slash-access.ts)

import type { IncomingEvent, PendingSlotMap } from "./events.js";
import {
	type CommandDef,
	type CommandRegistry,
	buildCommandLookup,
	bypassCommandNames,
	catchAllBusyRejectText,
	isInterruptThenDispatch,
	resolveBusyDispatch,
	resolveCommand,
	shouldBypassActiveSession,
} from "./busy-policy.js";
import { mergePendingEvent } from "./events.js";
import { type SlashAccessPolicy, checkSlashAccess } from "./slash-access.js";

/** run.py:_BUSY_QUEUE_MAX_PENDING. */
export const BUSY_QUEUE_MAX_PENDING = 32;

/**
 * run.py env bridge: `float(os.getenv("HERMES_TELEGRAM_FOLLOWUP_GRACE_SECONDS",
 * "3.0"))` — the env name is ported VERBATIM per repo convention. Default
 * 3.0s of TEXT follow-ups after RUN START queue/merge WITHOUT interrupting,
 * even in interrupt mode.
 */
export const TELEGRAM_FOLLOWUP_GRACE_SECONDS_ENV =
	"HERMES_TELEGRAM_FOLLOWUP_GRACE_SECONDS";

/** run.py default "3.0". */
export const DEFAULT_FOLLOWUP_GRACE_SECONDS = 3.0;

/** run.py env bridge: `float(os.getenv("HERMES_AGENT_TIMEOUT", 1800))`. */
export const AGENT_TIMEOUT_SECONDS_ENV = "HERMES_AGENT_TIMEOUT";

/** run.py `_float_env("HERMES_AGENT_TIMEOUT", 1800)` default. */
export const DEFAULT_AGENT_TIMEOUT_SECONDS = 1800;

/**
 * Env parsing for the staleness-eviction timeout. Same fail-safe posture as
 * the follow-up grace: Python float() would raise into the busy path; a
 * messaging gate must not die on a bad env var, so garbage falls back to the
 * 1800s default. Values ≤ 0 DISABLE eviction entirely (parity: both eviction
 * clauses key on ``_raw_stale_timeout > 0``).
 */
export function resolveAgentTimeoutSeconds(
	env: Record<string, string | undefined> = process.env,
): number {
	const raw = (env[AGENT_TIMEOUT_SECONDS_ENV] ?? "").trim();
	if (raw === "") return DEFAULT_AGENT_TIMEOUT_SECONDS;
	const parsed = Number(raw);
	return Number.isFinite(parsed) ? parsed : DEFAULT_AGENT_TIMEOUT_SECONDS;
}

/**
 * run.py `_wall_ttl` — extreme wall-clock age bound: max(10× timeout, 7200s),
 * or ∞ when the timeout is disabled. Catches entries whose agent object was
 * lost while still reporting activity.
 */
export function staleRunningAgentWallTtlSeconds(
	timeoutSeconds: number,
): number {
	return timeoutSeconds > 0
		? Math.max(timeoutSeconds * 10, 7200)
		: Number.POSITIVE_INFINITY;
}

/** Inputs of the run.py staleness predicate (seconds). */
export interface StaleRunningEntryInputs {
	/** now − turn.started_ts. */
	ageSeconds: number;
	/** get_activity_summary().seconds_since_activity (∞ when unknowable). */
	idleSeconds: number;
	/** HERMES_AGENT_TIMEOUT (≤0 disables). */
	timeoutSeconds: number;
}

/**
 * run.py staleness predicate (~17208): evict a REAL (non-sentinel) entry when
 * the agent has been IDLE beyond the inactivity timeout, OR when wall-clock
 * age exceeds max(10× timeout, 7200s). A disabled timeout (≤0) disables both
 * clauses — nothing ever evicts. The pending-sentinel exclusion lives at the
 * call site (run.py guards `_stale_agent is not _AGENT_PENDING_SENTINEL`).
 */
export function isStaleRunningEntry(inputs: StaleRunningEntryInputs): boolean {
	const wallTtl = staleRunningAgentWallTtlSeconds(inputs.timeoutSeconds);
	return (
		(inputs.timeoutSeconds > 0 &&
			inputs.idleSeconds >= inputs.timeoutSeconds) ||
		inputs.ageSeconds > wallTtl
	);
}

/**
 * Env parsing for the grace value. Deviation note: Python float() would raise
 * on garbage and take the whole busy path down with it; a messaging gate must
 * not die on a bad env var, so unparseable values fail safe to the 3.0s
 * default.
 */
export function resolveFollowupGraceSeconds(
	env: Record<string, string | undefined> = process.env,
): number {
	const raw = (env[TELEGRAM_FOLLOWUP_GRACE_SECONDS_ENV] ?? "").trim();
	if (raw === "") return DEFAULT_FOLLOWUP_GRACE_SECONDS;
	const parsed = Number(raw);
	return Number.isFinite(parsed) ? parsed : DEFAULT_FOLLOWUP_GRACE_SECONDS;
}

export type BusyInputMode = "queue" | "steer" | "interrupt";

/**
 * Read/write view over the adapter's single pending slot map. The adapter OWNS
 * the map; the runner sees it through this narrow seam so the FIFO helpers can
 * stage heads into it without owning adapter state.
 */
export interface PendingSlotView {
	get(sessionKey: string): IncomingEvent | undefined;
	set(sessionKey: string, event: IncomingEvent): void;
	has(sessionKey: string): boolean;
	delete(sessionKey: string): void;
}

export interface SpecialHandlerContext {
	sessionKey: string;
}

/** A mid-run special handler: receives the event, returns the reply text. */
export type SpecialHandler = (
	event: IncomingEvent,
	ctx: SpecialHandlerContext,
) => Promise<string | null | undefined> | string | null | undefined;

/** Plain mid-run handler table entry (busy_policy dispatch rows). */
export type PlainHandler = (
	event: IncomingEvent,
) => Promise<string | null | undefined> | string | null | undefined;

export interface RunnerBusyOptions {
	registry: CommandRegistry;
	/** The adapter's pending-slot map (single slot per key) via a view seam. */
	slots: PendingSlotMap & {
		has(sessionKey: string): boolean;
		delete(sessionKey: string): void;
	};
	/** Warning sink (cap drops, missing handlers). Default silent. */
	onWarning?: (message: string) => void;
	/** Mid-run special handler table keyed by busy_handler. */
	specialHandlers?: Readonly<Record<string, SpecialHandler>>;
	/** Plain handler table keyed by command NAME for dispatch-policy rows. */
	plainHandlers?: Readonly<Record<string, PlainHandler>>;
	/** run.py busy_input_mode (display.busy_input_mode). Default "interrupt". */
	busyInputMode?: BusyInputMode;
	/** agent.steer() injection for mode==="steer"; return false ⇒ FIFO fallback. */
	steer?: (text: string) => boolean;
	maxPending?: number;
	/** Explicit grace override; default resolves from the env bridge above. */
	followupGraceSeconds?: number;
	/** Env record for the grace env-var; defaults to process.env. */
	env?: Record<string, string | undefined>;
	/** Injected clock (ms) for the grace window — tests assert timing with it. */
	now?: () => number;
	/**
	 * Explicit staleness-eviction timeout override; default resolves
	 * HERMES_AGENT_TIMEOUT from `env`. Values ≤ 0 disable eviction.
	 */
	agentTimeoutSeconds?: number;
	/**
	 * run.py:_invalidate_session_run_generation seam — fired on stale eviction
	 * so an in-flight async run for the OLD generation cannot clobber newer
	 * state during its unwind.
	 */
	invalidateRunGeneration?: (sessionKey: string, reason: string) => void;
	/**
	 * run.py:_release_running_agent_state seam — pops ALL per-running-agent
	 * state for the key (turn state only: agent/started_ts/lease/busy_ack;
	 * queued pending events and cross-turn state have their own lifecycles).
	 */
	releaseRunningAgentState?: (sessionKey: string) => void;
	/**
	 * run.py:_check_slash_access config binding — resolve the slash-access
	 * policy (gateway/slash_access.py:policy_for_source parity) for THIS
	 * event's platform+scope. Absent ⇒ gating disabled everywhere
	 * (backward-compat: no admin list ⇒ every allowed user keeps commands).
	 */
	slashAccessPolicyOf?: (event: IncomingEvent) => SlashAccessPolicy;
}

/** Per-key running-turn bookkeeping (run.py session-state turn slice). */
interface RunningTurnRecord {
	/** turn.started_ts parity — wall-clock ms of run start. */
	startedAtMs: number;
	/** get_activity_summary last-activity parity; defaults to run start. */
	lastActivityAtMs: number;
	/**
	 * _AGENT_PENDING_SENTINEL parity: a just-placed async-setup placeholder
	 * has no activity tracker yet — it must NEVER be evicted by the idle sweep
	 * or the setup path races itself.
	 */
	pendingSentinel: boolean;
}

/**
 * L2 guard machinery: registry-derived busy dispatch + the runner-owned FIFO
 * overflow queue. One instance per runner; all methods are synchronous-safe
 * against the single event loop.
 */
export class RunnerBusyGuard {
	readonly maxPending: number;
	private readonly lookup: ReadonlyMap<string, CommandDef>;
	private readonly slots: PendingSlotView;
	private readonly onWarning: (message: string) => void;
	private readonly specialHandlers: Readonly<Record<string, SpecialHandler>>;
	private readonly plainHandlers: Readonly<Record<string, PlainHandler>>;
	private readonly busyInputMode: BusyInputMode;
	private readonly steerFn: ((text: string) => boolean) | null;
	/** session_key → overflow tail (conversation.queued_events parity). */
	private readonly overflow = new Map<string, IncomingEvent[]>();
	/** session_key → running-turn record (_peek_session_state(...).turn parity). */
	private readonly runningTurns = new Map<string, RunningTurnRecord>();
	private readonly followupGraceSeconds: number;
	private readonly nowFn: () => number;
	private readonly agentTimeoutSeconds: number;
	private readonly invalidateRunGenerationFn:
		| ((sessionKey: string, reason: string) => void)
		| null;
	private readonly releaseRunningAgentStateFn:
		| ((sessionKey: string) => void)
		| null;
	private readonly slashAccessPolicyOf:
		| ((event: IncomingEvent) => SlashAccessPolicy)
		| null;

	constructor(options: RunnerBusyOptions) {
		this.lookup = buildCommandLookup(options.registry);
		this.slots = options.slots;
		this.onWarning = options.onWarning ?? (() => {});
		this.specialHandlers = options.specialHandlers ?? {};
		this.plainHandlers = options.plainHandlers ?? {};
		this.busyInputMode = options.busyInputMode ?? "interrupt";
		this.steerFn = options.steer ?? null;
		this.followupGraceSeconds =
			options.followupGraceSeconds ?? resolveFollowupGraceSeconds(options.env);
		this.nowFn = options.now ?? (() => Date.now());
		this.agentTimeoutSeconds =
			options.agentTimeoutSeconds ?? resolveAgentTimeoutSeconds(options.env);
		this.invalidateRunGenerationFn = options.invalidateRunGeneration ?? null;
		this.releaseRunningAgentStateFn = options.releaseRunningAgentState ?? null;
		this.slashAccessPolicyOf = options.slashAccessPolicyOf ?? null;
		this.maxPending =
			options.maxPending !== undefined && options.maxPending > 0
				? options.maxPending
				: BUSY_QUEUE_MAX_PENDING;
	}

	// -- registry predicates (thin delegations; ONE source of truth) ---------

	resolve(rawName: string | null | undefined): CommandDef | null {
		return resolveCommand(this.lookup, rawName);
	}

	shouldBypassActiveSession(rawName: string | null | undefined): boolean {
		return shouldBypassActiveSession(this.lookup, rawName);
	}

	isInterruptThenDispatch(rawName: string | null | undefined): boolean {
		return isInterruptThenDispatch(this.lookup, rawName);
	}

	bypassCommandNames(): Set<string> {
		return bypassCommandNames(
			[...this.lookup.values()].filter(
				(cmd, i, all) => all.findIndex((c) => c.name === cmd.name) === i,
			),
		);
	}

	// -- /queue FIFO helpers --------------------------------------------------

	/** run.py:_enqueue_fifo — first item takes the slot; further items append to the overflow. */
	enqueueFifo(
		sessionKey: string,
		queuedEvent: IncomingEvent,
	): "slot" | "overflow" | "dropped" {
		if (!this.slots.has(sessionKey)) {
			this.slots.set(sessionKey, queuedEvent);
			return "slot";
		}
		if (this.queueDepth(sessionKey) >= this.maxPending) {
			this.onWarning(
				`Dropping busy-mode follow-up for session ${sessionKey} — pending queue at cap (${this.maxPending}).`,
			);
			return "dropped";
		}
		const list = this.overflow.get(sessionKey);
		if (list) list.push(queuedEvent);
		else this.overflow.set(sessionKey, [queuedEvent]);
		return "overflow";
	}

	/**
	 * run.py:_promote_queued_event — called at each drain after the slot was
	 * consumed. Moves the overflow head into the emptied slot (or returns it as
	 * the next pending event); when the slot is already re-populated, stages
	 * the overflow head there for the NEXT drain.
	 */
	promoteQueuedEvent(
		sessionKey: string,
		pendingEvent: IncomingEvent | null,
	): IncomingEvent | null {
		const overflow = this.overflow.get(sessionKey);
		const nextQueued = overflow?.shift();
		if (nextQueued === undefined) return pendingEvent;
		if (pendingEvent === null) return nextQueued;
		this.slots.set(sessionKey, nextQueued);
		return pendingEvent;
	}

	/** run.py:_queue_depth — total pending items: slot + overflow. */
	queueDepth(sessionKey: string): number {
		let depth = this.overflow.get(sessionKey)?.length ?? 0;
		if (this.slots.has(sessionKey)) depth += 1;
		return depth;
	}

	overflowOf(sessionKey: string): readonly IncomingEvent[] {
		return this.overflow.get(sessionKey) ?? [];
	}

	/** Clear everything queued for one key (/new, /reset — stale text must not replay, #2170). */
	clearQueue(sessionKey: string): void {
		this.overflow.delete(sessionKey);
		this.slots.delete(sessionKey);
	}

	// -- turn-start tracking (grace window + staleness eviction) --------------

	/**
	 * The runner loop records run start here so the busy ladder can apply the
	 * Telegram TEXT follow-up grace (run.py reads _peek_session_state(...).
	 * turn.started_ts at the same decision point) AND the staleness eviction
	 * can age the entry. Records a REAL agent binding (not the pending
	 * sentinel); activity defaults to run start — a fresh run is active now.
	 */
	markTurnStarted(sessionKey: string, startedAtMs?: number): void {
		const startedAt = startedAtMs ?? this.nowFn();
		this.runningTurns.set(sessionKey, {
			startedAtMs: startedAt,
			lastActivityAtMs: startedAt,
			pendingSentinel: false,
		});
	}

	/**
	 * run.py _AGENT_PENDING_SENTINEL placement parity: the async setup phase
	 * marks the slot BEFORE the real agent exists. Sentinel entries are never
	 * idle-evicted (they carry no activity tracker yet — the sweep would race
	 * the setup path) and report no start for the grace window.
	 */
	markPendingTurnStart(sessionKey: string, startedAtMs?: number): void {
		const now = startedAtMs ?? this.nowFn();
		this.runningTurns.set(sessionKey, {
			startedAtMs: now,
			lastActivityAtMs: now,
			pendingSentinel: true,
		});
	}

	/**
	 * get_activity_summary producer seam: the runner loop records agent
	 * activity (model call / tool event) here; idleness for the eviction sweep
	 * measures from THIS stamp, not from run start.
	 */
	markAgentActivity(sessionKey: string, atMs?: number): void {
		const record = this.runningTurns.get(sessionKey);
		if (record === undefined || record.pendingSentinel) return;
		record.lastActivityAtMs = atMs ?? this.nowFn();
	}

	/** Turn finished/cleaned up — the grace window closes. */
	markTurnFinished(sessionKey: string): void {
		this.runningTurns.delete(sessionKey);
	}

	/** Test/diagnostic probe. */
	turnStartOf(sessionKey: string): number | undefined {
		return this.runningTurns.get(sessionKey)?.startedAtMs;
	}

	// -- staleness eviction (run.py ~17208) -----------------------------------

	/** True when a non-sentinel running-turn record exists for the key. */
	hasRunningTurn(sessionKey: string): boolean {
		const record = this.runningTurns.get(sessionKey);
		return record !== undefined && !record.pendingSentinel;
	}

	/**
	 * run.py:_handle_message staleness eviction (~17208): detect leaked locks
	 * from hung/crashed handlers. With inactivity-based timeout, ACTIVE tasks
	 * can run for hours, so wall-clock age alone isn't sufficient — evict only
	 * when the agent has been IDLE ≥ HERMES_AGENT_TIMEOUT (default 1800s), or
	 * when wall age exceeds max(10× timeout, 7200s). Pending sentinels are
	 * NEVER evicted. A disabled timeout (≤0) disables eviction entirely.
	 *
	 * On evict: warn, INVALIDATE the session's run generation (an in-flight
	 * async unwind must not clobber newer state), RELEASE the running-agent
	 * state, and drop the local record — follow-ups stop queueing behind the
	 * dead guard and the arrival takes the COLD path instead.
	 *
	 * Returns true when an entry was evicted.
	 */
	maybeEvictStaleRunningAgent(sessionKey: string): boolean {
		const record = this.runningTurns.get(sessionKey);
		if (record === undefined || record.pendingSentinel) return false;
		const nowMs = this.nowFn();
		const ageSeconds = (nowMs - record.startedAtMs) / 1000;
		const idleSeconds = (nowMs - record.lastActivityAtMs) / 1000;
		if (
			!isStaleRunningEntry({
				ageSeconds,
				idleSeconds,
				timeoutSeconds: this.agentTimeoutSeconds,
			})
		) {
			return false;
		}
		this.onWarning(
			`Evicting stale _running_agents entry for ${sessionKey} ` +
				`(age: ${Math.round(ageSeconds)}s, idle: ${formatIdle(idleSeconds)}s, ` +
				`timeout: ${Math.round(this.agentTimeoutSeconds)}s)`,
		);
		this.invalidateRunGenerationFn?.(
			sessionKey,
			"stale_running_agent_eviction",
		);
		this.releaseRunningAgentStateFn?.(sessionKey);
		this.runningTurns.delete(sessionKey);
		return true;
	}

	/** True when a Telegram TEXT arrival lands inside the post-start grace. */
	withinFollowupGrace(sessionKey: string, event: IncomingEvent): boolean {
		if (this.followupGraceSeconds <= 0) return false;
		if (event.messageType !== "text") return false;
		const platform = event.source?.platform;
		if (platform !== "telegram") return false;
		const startedAt = this.runningTurns.get(sessionKey)?.startedAtMs;
		if (startedAt === undefined) return false; // parity: falsy started_ts ⇒ skip
		return this.nowFn() - startedAt <= this.followupGraceSeconds * 1000;
	}

	/**
	 * run.py:_queue_or_replace_pending_event — queue-mode text follow-ups get
	 * their OWN turn in arrival order (#28503 — no more silent overwrite).
	 * Photo/media events still merge into the head slot, gated on matching
	 * security context (internal flag, control flag, metadata keys).
	 */
	queueOrReplacePendingEvent(sessionKey: string, event: IncomingEvent): void {
		const existing = this.slots.get(sessionKey);
		const sameSecurityContext =
			existing !== undefined &&
			(existing.internal ?? false) === (event.internal ?? false) &&
			(existing.allowGatewayControl ?? true) ===
				(event.allowGatewayControl ?? true) &&
			SECURITY_METADATA_KEYS.every(
				(key) => (existing.metadata ?? {})[key] === (event.metadata ?? {})[key],
			);
		if (
			sameSecurityContext &&
			(existing.messageType === "photo" ||
				event.messageType === "photo" ||
				(existing.mediaUrls?.length ?? 0) > 0 ||
				(event.mediaUrls?.length ?? 0) > 0)
		) {
			// Preserve photo-burst / media-merge semantics for the head slot.
			mergePendingEvent(this.slots, sessionKey, event, {
				mergeText: event.messageType === "text",
			});
			return;
		}
		this.enqueueFifo(sessionKey, event);
	}

	// -- busy-input ladder (§6, queue/steer modes; interrupt is runner-loop work)

	/**
	 * Route a plain-text follow-up per busy_input_mode. Returns the disposition
	 * taken. mode==="interrupt" demotes to queue here (subagent/compression
	 * demotion lives in the runner loop, Phase 1 scope item 4's loop half).
	 *
	 * AHEAD of that ladder sit, in run.py arrival order: the staleness eviction
	 * (~17208 — a hung-but-live guard releases its session and the arrival is
	 * reported "evicted": treat it as a COLD/fresh turn, never queue it behind
	 * the dead entry) and the Telegram TEXT follow-up grace
	 * (HERMES_TELEGRAM_FOLLOWUP_GRACE_SECONDS block): arrivals within grace
	 * seconds of RUN START queue without interrupting — even in interrupt mode,
	 * and even in steer mode (parity: the grace branch enqueues in queue-mode
	 * and merges into the head slot otherwise, it never steers). Grace
	 * dispositions report "queued".
	 */
	handlePlainTextFollowUp(
		sessionKey: string,
		event: IncomingEvent,
	): "steered" | "queued" | "evicted" {
		if (this.maybeEvictStaleRunningAgent(sessionKey)) {
			return "evicted"; // session released — caller takes the cold path
		}
		if (this.withinFollowupGrace(sessionKey, event)) {
			// Parity of the grace branch's queue/merge split:
			if (this.busyInputMode === "queue") {
				this.enqueueFifo(sessionKey, event);
			} else {
				mergePendingEvent(this.slots, sessionKey, event, {
					mergeText: true,
				});
			}
			return "queued";
		}
		if (
			this.busyInputMode === "steer" &&
			event.text !== undefined &&
			this.steerFn !== null &&
			this.steerFn(event.text)
		) {
			return "steered";
		}
		this.queueOrReplacePendingEvent(sessionKey, event);
		return "queued";
	}

	// -- §5.4 dispatch table ---------------------------------------------------

	/**
	 * run.py:_dispatch_busy_slash_command — resolve and EXECUTE a recognized
	 * slash command while the agent runs. Resolution order: staleness sweep →
	 * pre-gate → SLASH-ACCESS GATE (run.py ~17282 — every non-pregate
	 * recognized command, including /stop and /approve, so an in-flight agent
	 * can't be used to bypass admin/user gating) → special busy_handler →
	 * policy-dispatch plain handler → catch-all reject. Unknown commands
	 * return null (caller queues them as text).
	 *
	 * null ALSO when the arrival itself triggered a stale eviction: the entry
	 * was just released, so there is no mid-run context left to dispatch into
	 * — the caller must treat the event as a COLD arrival.
	 */
	async dispatchBusySlashCommand(
		rawName: string,
		event: IncomingEvent,
		sessionKey: string,
	): Promise<string | null> {
		if (this.maybeEvictStaleRunningAgent(sessionKey)) return null;

		const resolved = resolveBusyDispatch(this.lookup, rawName);
		if (resolved === null) return null; // unknown "/foo" → queues as text

		// run.py ~17282: the access gate sits BETWEEN the status/context
		// pre-gate and busy dispatch. /help and /whoami pass under the
		// always-allowed floor inside checkSlashAccess.
		if (resolved.kind !== "pregate") {
			const denied = this.deniedSlashReply(event, resolved.cmd.name);
			if (denied !== null) return denied;
		}

		switch (resolved.kind) {
			case "pregate": {
				const handler = this.plainHandlers[resolved.cmd.name];
				if (handler !== undefined) {
					return orNull(await handler(event));
				}
				this.onWarning(
					`pregate command /${resolved.cmd.name} has no mid-run handler`,
				);
				return "";
			}
			case "special": {
				const special = this.specialHandlers[resolved.handlerKey ?? ""];
				if (special !== undefined) {
					return orNull(await special(event, { sessionKey }));
				}
				this.onWarning(
					`busy_handler ${resolved.handlerKey} for /${resolved.cmd.name} has no mid-run handler`,
				);
				break; // fall through to catch-all reject
			}
			case "plain": {
				const plain = this.plainHandlers[resolved.cmd.name];
				if (plain !== undefined) {
					return orNull(await plain(event));
				}
				this.onWarning(
					`busy_policy=${resolved.policy} for /${resolved.cmd.name} has no mid-run handler — falling back to busy-reject`,
				);
				break;
			}
			case "reject":
				return resolved.rejectText ?? catchAllBusyRejectText(resolved.cmd.name);
			default: {
				const exhaustive: never = resolved.kind;
				throw new Error(
					`unreachable busy dispatch kind: ${String(exhaustive)}`,
				);
			}
		}
		return catchAllBusyRejectText(resolved.cmd.name);
	}

	/**
	 * run.py:_check_slash_access COLD-path call site (~17507): gate a command
	 * before built-in dispatch when NO agent is running. The known-command
	 * precondition lives here (parity of `is_gateway_known_command(canonical)`):
	 * unknown names are plain text and are NEVER gated. Pass the RAW typed name
	 * or an alias — resolution goes through the same registry lookup the busy
	 * path uses, so gating always keys on the CANONICAL row name. Quick-command
	 * sinks (#44727) reuse this with their raw typed names once they exist.
	 */
	checkColdPathSlashAccess(
		event: IncomingEvent,
		rawOrCanonicalName: string,
	): string | null {
		const cmd = resolveCommand(this.lookup, rawOrCanonicalName);
		if (cmd === null) return null;
		return this.deniedSlashReply(event, cmd.name);
	}

	/**
	 * run.py:_check_slash_access core bound to THIS guard's config seam:
	 * resolve the scope policy for the event, deny non-admins outside
	 * user_allowed_commands (floor /help,/whoami), log the denial, return the
	 * BYTE-STABLE text. Gating disabled ⇒ null without touching the resolver.
	 */
	private deniedSlashReply(
		event: IncomingEvent,
		canonicalCmd: string,
	): string | null {
		const policyOf = this.slashAccessPolicyOf;
		if (policyOf === null) return null;
		const denied = checkSlashAccess(
			policyOf(event),
			event.source?.userId ?? null,
			canonicalCmd,
		);
		if (denied !== null) {
			this.onWarning(
				`Slash command /${canonicalCmd} denied for ` +
					`${event.source?.platform ?? "?"}:${event.source?.userId ?? ""} ` +
					`(not admin, not in user_allowed_commands)`,
			);
		}
		return denied;
	}
}

function orNull(value: string | null | undefined): string | null {
	return typeof value === "string" && value.length > 0
		? value
		: (value ?? null);
}

/** run.py "%s" idle formatting: ∞ stays readable instead of "Infinity". */
function formatIdle(idleSeconds: number): string {
	return Number.isFinite(idleSeconds) ? String(Math.round(idleSeconds)) : "∞";
}

/** Metadata keys that must match before head-slot merges are allowed. */
const SECURITY_METADATA_KEYS = [
	"hermes_plugin_id",
	"hermes_plugin_injection",
	"gateway_session_key",
	"gateway_session_id",
	"gateway_session_strict",
] as const;
