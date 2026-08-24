// queue.ts — the per-session FIFO of pending exec-approvals + THE one
// resolution primitive (07-integrations.md §8.1 queue model, §8.4).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   tools/approval.py:_ApprovalEntry          → ApprovalEntry
//   tools/approval.py:_gateway_queues         → ApprovalQueues.queues
//   tools/approval.py:_gateway_notify_cbs     → ApprovalQueues.notifyCbs
//   tools/approval.py:register_gateway_notify      → registerNotify
//   tools/approval.py:unregister_gateway_notify    → unregisterNotify
//   tools/approval.py:resolve_gateway_approval     → resolve  (THE primitive)
//   tools/approval.py:list_gateway_approvals       → listApprovals
//   tools/approval.py:ack_gateway_approval         → ack
//   tools/approval.py:has_blocking_approval        → hasBlocking
//   tools/approval.py:get_pending_gateway_approval → oldestPending
//   tools/approval.py:clear_session (queue half)   → clearSession
//
// Binding clauses realized here:
//   • resolve() is the SINGLE resolution function beneath every answer
//     surface (buttons, /approve, /deny, bare words, api request_id).
//     Targeting order: request_id first, then resolve_all, else FIFO oldest.
//   • Returns the count resolved — 0 means NOTHING was pending, and callers
//     must treat that as authoritative when rendering outcomes (#63501: a
//     stale tap must never claim "Approved" for a command that will not run).
//   • unregisterNotify signals ALL blocked waits WITHOUT a result so run
//     completion / interrupt can never strand a waiter; the gate normalizes
//     the null choice to "timeout" (parity of event-set-without-result).
//   • clearSession denies + releases blocked waits IMMEDIATELY instead of
//     letting them idle to timeout (session-boundary cleanup parity).
//   • The queue is process-local by design (§8.6): a gateway restart loses
//     it; boot-time resume replays the interrupted TURN, not the approval.

import { randomUUID } from "node:crypto";

export const APPROVAL_CHOICES = ["once", "session", "always", "deny"] as const;

export type ApprovalChoice = (typeof APPROVAL_CHOICES)[number];

export function isApprovalChoice(value: string): value is ApprovalChoice {
	return (APPROVAL_CHOICES as readonly string[]).includes(value);
}

/** Shape handed to notify/delivery and stored on each entry (§8.1 `data`). */
export interface ApprovalRequestData {
	command: string;
	description: string;
	/** Primary guard pattern key (informational; coalescing uses patternKeys). */
	patternKey?: string;
	/**
	 * Full pattern-key set — the coalescing identity is (command,
	 * patternKeys) EXACTLY (§8.4), so this must be stable per guard verdict.
	 */
	patternKeys?: readonly string[];
	/** uuid4-hex parity; generated when absent. */
	requestId?: string;
	/** Card/fallback gating flags (default true / true / false). */
	allowPermanent?: boolean;
	allowSession?: boolean;
	smartDenied?: boolean;
}

/** Fully-normalized entry data with every default applied. */
export interface NormalizedApprovalData {
	command: string;
	description: string;
	patternKey: string;
	patternKeys: string[];
	requestId: string;
	allowPermanent: boolean;
	allowSession: boolean;
	smartDenied: boolean;
}

export function normalizeApprovalData(
	data: ApprovalRequestData,
): NormalizedApprovalData {
	const patternKey = data.patternKey ?? "";
	const rawKeys =
		data.patternKeys ??
		(data.patternKey !== undefined ? [data.patternKey] : [patternKey]);
	return {
		command: data.command,
		description: data.description,
		patternKey,
		patternKeys: [...rawKeys],
		requestId: data.requestId ?? randomUUID().replace(/-/g, ""),
		allowPermanent: data.allowPermanent ?? true,
		allowSession: data.allowSession ?? true,
		smartDenied: data.smartDenied ?? false,
	};
}

/**
 * One pending dangerous-command approval inside a gateway session.
 * The `wait` promise replaces Python's threading.Event: `settle()` resolves
 * it exactly once; result assignment happens BEFORE settle so waiters read
 * a complete decision (Hermes sets result under lock, then the event).
 */
export class ApprovalEntry {
	readonly data: NormalizedApprovalData;
	/** Decision slot: "once"|"session"|"always"|"deny" (null until resolved). */
	result: ApprovalChoice | null = null;
	/** Free-text reason from `/deny <reason>`, relayed verbatim to the agent. */
	reason: string | null = null;
	/** api_server replay-support flag (ack_gateway_approval parity). */
	acknowledged = false;

	private resolveFn: (() => void) | null = null;
	private done = false;
	// NOTE: declared AFTER resolveFn so the executor's assignment is not
	// clobbered by a later field initializer (field init order = declaration
	// order; the executor runs synchronously during construction).
	private readonly settled: Promise<void>;

	constructor(data: ApprovalRequestData) {
		this.data = normalizeApprovalData(data);
		this.settled = new Promise<void>((resolvePromise) => {
			this.resolveFn = resolvePromise;
		});
	}

	/** Resolves once; later calls are no-ops (idempotence at the entry level). */
	settle(): void {
		if (this.done) return;
		this.done = true;
		this.resolveFn?.();
	}

	get wait(): Promise<void> {
		return this.settled;
	}
}

export type NotifyCallback = (approvalData: NormalizedApprovalData) => void;

export interface ResolveOptions {
	/** `/approve all` parity: resolve every pending entry in the session. */
	resolveAll?: boolean;
	/** Deny reason relayed verbatim into the BLOCKED result (≤280 enforced upstream). */
	reason?: string | null | undefined;
	/** Target specific entries by requestId (api_server /v1/runs lane). */
	requestId?: string | null | undefined;
}

/**
 * `_gateway_queues` + `_gateway_notify_cbs` as an instance so tests get
 * mkdtemp-grade isolation without module-global state.
 */
export class ApprovalQueues {
	private readonly queues = new Map<string, ApprovalEntry[]>();
	private readonly notifyCbs = new Map<string, NotifyCallback>();

	// ── registration lifecycle (§8.1) ────────────────────────────────────────

	registerNotify(sessionKey: string, cb: NotifyCallback): void {
		this.notifyCbs.set(sessionKey, cb);
	}

	/**
	 * Unregister the per-session notify callback AND signal ALL blocked waits
	 * (event set WITHOUT result) so completion/shutdown never strands them.
	 */
	unregisterNotify(sessionKey: string): void {
		this.notifyCbs.delete(sessionKey);
		const entries = this.queues.get(sessionKey);
		this.queues.delete(sessionKey);
		for (const entry of entries ?? []) {
			entry.settle();
		}
	}

	isNotifyRegistered(sessionKey: string): boolean {
		return this.notifyCbs.has(sessionKey);
	}

	getNotify(sessionKey: string): NotifyCallback | undefined {
		return this.notifyCbs.get(sessionKey);
	}

	// ── queue body ─────────────────────────────────────────────────────────────

	enqueue(sessionKey: string, entry: ApprovalEntry): void {
		const queue = this.queues.get(sessionKey);
		if (queue) {
			queue.push(entry);
		} else {
			this.queues.set(sessionKey, [entry]);
		}
	}

	/** Remove one specific entry (notify-failed path); drops empty queues. */
	dropEntry(sessionKey: string, entry: ApprovalEntry): void {
		const queue = this.queues.get(sessionKey);
		if (!queue) return;
		const index = queue.indexOf(entry);
		if (index >= 0) {
			queue.splice(index, 1);
		}
		if (queue.length === 0) {
			this.queues.delete(sessionKey);
		}
	}

	/**
	 * THE resolution primitive (`resolve_gateway_approval`). Every answer
	 * surface routes through here. Returns the count resolved — callers MUST
	 * treat 0 as authoritative "nothing was pending".
	 */
	resolve(
		sessionKey: string,
		choice: ApprovalChoice,
		options: ResolveOptions = {},
	): number {
		const queue = this.queues.get(sessionKey);
		if (!queue || queue.length === 0) return 0;

		let targets: ApprovalEntry[];
		if (options.requestId) {
			targets = queue.filter(
				(entry) => entry.data.requestId === options.requestId,
			);
			if (targets.length === 0) return 0;
			this.queues.set(
				sessionKey,
				queue.filter((entry) => !targets.includes(entry)),
			);
		} else if (options.resolveAll === true) {
			targets = [...queue];
			queue.length = 0;
		} else {
			targets = [queue.shift() as ApprovalEntry];
		}
		if ((this.queues.get(sessionKey)?.length ?? 0) === 0) {
			this.queues.delete(sessionKey);
		}

		for (const entry of targets) {
			entry.result = choice;
			if (options.reason) {
				entry.reason = options.reason;
			}
			entry.settle();
		}
		return targets.length;
	}

	hasBlocking(sessionKey: string): boolean {
		return (this.queues.get(sessionKey)?.length ?? 0) > 0;
	}

	/** Replay-safe snapshots of unresolved approvals for one session. */
	listApprovals(sessionKey: string): NormalizedApprovalData[] {
		return (this.queues.get(sessionKey) ?? []).map((entry) => ({
			...entry.data,
			patternKeys: [...entry.data.patternKeys],
		}));
	}

	/** Oldest unresolved snapshot (reconnectable-client restore parity). */
	oldestPending(sessionKey: string): NormalizedApprovalData | null {
		const first = this.queues.get(sessionKey)?.[0];
		return first
			? { ...first.data, patternKeys: [...first.data.patternKeys] }
			: null;
	}

	/**
	 * Coalescing lookup (§8.4): the LIVE pending entry whose data matches
	 * (command, patternKeys) EXACTLY — the coalescing identity. Returns null
	 * when no identical approval is pending.
	 */
	findIdenticalPending(
		sessionKey: string,
		command: string,
		patternKeys: readonly string[],
	): ApprovalEntry | null {
		for (const entry of this.queues.get(sessionKey) ?? []) {
			if (
				entry.data.command === command &&
				entry.data.patternKeys.length === patternKeys.length &&
				entry.data.patternKeys.every(
					(key: string, index: number) => key === patternKeys[index],
				)
			) {
				return entry;
			}
		}
		return null;
	}

	/** Record that a client received a particular pending request. */
	ack(sessionKey: string, requestId: string): boolean {
		for (const entry of this.queues.get(sessionKey) ?? []) {
			if (entry.data.requestId === requestId) {
				entry.acknowledged = true;
				return true;
			}
		}
		return false;
	}

	/**
	 * Session-boundary cleanup: deny + release blocked waits IMMEDIATELY so
	 * the old run unwinds instead of idling to timeout (clear_session parity).
	 */
	clearSession(sessionKey: string): void {
		const entries = this.queues.get(sessionKey);
		this.queues.delete(sessionKey);
		for (const entry of entries ?? []) {
			entry.result = "deny";
			entry.settle();
		}
	}
}
