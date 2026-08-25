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
//   gateway/run.py:_handle_message (HERMES_TELEGRAM_FOLLOWUP_GRACE_SECONDS)
//                                                     → Telegram TEXT follow-up grace ahead of the interrupt demotion

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

/**
 * Env parsing for the grace value. Deviation note: Python float() would raise
 * on garbage and take the whole busy path down with it; a messaging gate must
 * not die on a bad env var, so unparseable values fail safe to the 3.0s
 * default (same posture as HERMES_CRON_TIMEOUT handling).
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
	/** session_key → wall-clock ms timestamp of the current turn's start. */
	private readonly turnStartedAtMs = new Map<string, number>();
	private readonly followupGraceSeconds: number;
	private readonly nowFn: () => number;

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

	// -- turn-start tracking for the follow-up grace window -------------------

	/**
	 * The runner loop records run start here so the busy ladder can apply the
	 * Telegram TEXT follow-up grace (run.py reads _peek_session_state(...).
	 * turn.started_ts at the same decision point).
	 */
	markTurnStarted(sessionKey: string, startedAtMs?: number): void {
		this.turnStartedAtMs.set(sessionKey, startedAtMs ?? this.nowFn());
	}

	/** Turn finished/cleaned up — the grace window closes. */
	markTurnFinished(sessionKey: string): void {
		this.turnStartedAtMs.delete(sessionKey);
	}

	/** Test/diagnostic probe. */
	turnStartOf(sessionKey: string): number | undefined {
		return this.turnStartedAtMs.get(sessionKey);
	}

	/** True when a Telegram TEXT arrival lands inside the post-start grace. */
	withinFollowupGrace(sessionKey: string, event: IncomingEvent): boolean {
		if (this.followupGraceSeconds <= 0) return false;
		if (event.messageType !== "text") return false;
		const platform = event.source?.platform;
		if (platform !== "telegram") return false;
		const startedAt = this.turnStartedAtMs.get(sessionKey);
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
	 * AHEAD of that ladder sits the Telegram TEXT follow-up grace
	 * (run.py:_handle_message, HERMES_TELEGRAM_FOLLOWUP_GRACE_SECONDS block):
	 * arrivals within grace seconds of RUN START queue without interrupting —
	 * even in interrupt mode, and even in steer mode (parity: the grace branch
	 * enqueues in queue-mode and merges into the head slot otherwise, it never
	 * steers). Grace dispositions report "queued".
	 */
	handlePlainTextFollowUp(
		sessionKey: string,
		event: IncomingEvent,
	): "steered" | "queued" {
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
	 * slash command while the agent runs. Resolution order: pre-gate → special
	 * busy_handler → policy-dispatch plain handler → catch-all reject.
	 * Unknown commands return null (caller queues them as text).
	 */
	async dispatchBusySlashCommand(
		rawName: string,
		event: IncomingEvent,
		sessionKey: string,
	): Promise<string | null> {
		const resolved = resolveBusyDispatch(this.lookup, rawName);
		if (resolved === null) return null; // unknown "/foo" → queues as text

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
}

function orNull(value: string | null | undefined): string | null {
	return typeof value === "string" && value.length > 0
		? value
		: (value ?? null);
}

/** Metadata keys that must match before head-slot merges are allowed. */
const SECURITY_METADATA_KEYS = [
	"hermes_plugin_id",
	"hermes_plugin_injection",
	"gateway_session_key",
	"gateway_session_id",
	"gateway_session_strict",
] as const;
