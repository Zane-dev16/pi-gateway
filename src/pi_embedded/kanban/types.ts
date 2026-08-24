// types.ts — the kanban board client seam and dispatcher vocabulary (07 §6).
//
// The dispatcher NEVER talks to a concrete board store: it drives the
// BoardClient seam so tests run a fake board in-memory (no network, no
// SQLite) while production runs SqliteKanbanBoard. Every mutation on the
// seam is an ATOMIC column transition with CAS semantics — parity anchors:
//   hermes_cli/kanban_db.py:claim_task            → claimCard
//     (single UPDATE … WHERE status='ready' AND claim_lock IS NULL;
//      rowcount != 1 ⇒ lost race ⇒ null)
//   hermes_cli/kanban_db.py:release_stale_claims  → reclaimStaleClaims
//   hermes_cli/kanban_db.py:recompute_ready       → promoteReady
//   hermes_cli/kanban_db.py:_record_task_failure  → recordFailure
//   hermes_cli/kanban_db.py:complete_task         → completeCard

import type { GatewayClock } from "./clock.js";

export type CardStatus =
	| "todo"
	| "ready"
	| "running"
	| "review"
	| "blocked"
	| "done"
	| "archived";

/** Non-success outcomes that feed the auto-block breaker (07 §6). */
export type FailureOutcome = "spawn_failed" | "timed_out" | "crashed";

export interface KanbanCard {
	id: string;
	title: string;
	status: CardStatus;
	assignee: string | null;
	tenant: string | null;
	priority: number;
	/** Claim token minted by the winning racer; null when unclaimed. */
	claimLock: string | null;
	/** Epoch seconds; reclaim boundary is `claimExpires < now` (strict). */
	claimExpires: number | null;
	consecutiveFailures: number;
	/** Per-task breaker override (parity tasks.max_retries). */
	maxRetries: number | null;
	lastFailureError: string | null;
	createdAt: number;
	startedAt: number | null;
}

export interface NewCard {
	id?: string;
	title?: string;
	status?: CardStatus;
	assignee?: string | null;
	tenant?: string | null;
	priority?: number;
	createdAt?: number;
	/** Per-task breaker override (parity tasks.max_retries). */
	maxRetries?: number | null;
}

export interface ClaimRequest {
	cardId: string;
	/** Opaque claim token (owner identity) minted by the caller. */
	lock: string;
	expiresAt: number;
	nowSeconds: number;
}

/**
 * The injectable board seam. Implementations must guarantee per-method
 * atomicity: each mutating call is one CAS transition — concurrent racers
 * observe exactly one winner. All methods are async so production stores can
 * hop threads (parity of the gateway's asyncio.to_thread discipline) while
 * fakes stay trivial.
 */
export interface BoardClient {
	/** The board this client is pinned to (HARD boundary — never another). */
	readonly board: string;

	/**
	 * Ready cards with no claim, ordered priority DESC then createdAt ASC
	 * (parity of the dispatch loop's SELECT ordering).
	 */
	listReady(): Promise<KanbanCard[]>;

	/** Count of running cards (live-concurrency accounting for maxSpawn). */
	countRunning(): Promise<number>;

	/**
	 * Reset running cards whose claim has expired back to ready and clear
	 * their claims. Boundary: `claimExpires < now` STRICTLY (parity
	 * release_stale_claims' `claim_expires < ?`). Returns reclaimed ids.
	 */
	reclaimStaleClaims(nowSeconds: number): Promise<string[]>;

	/**
	 * Promote todo/blocked cards whose parents are all done/archived to
	 * ready (parity recompute_ready). Sticky operator blocks stay blocked;
	 * breaker-tripped cards stay blocked even when parents complete.
	 * Returns promoted ids.
	 */
	promoteReady(nowSeconds: number, failureLimit: number): Promise<string[]>;

	/**
	 * Atomically transition ready → running. Returns the claimed card, or
	 * null when the card was already claimed / not in ready (lost race).
	 * Parity invariant: never claim while any parent is non-terminal — a
	 * racy writer's card is demoted to todo instead.
	 */
	claimCard(request: ClaimRequest): Promise<KanbanCard | null>;

	/**
	 * Record a non-success outcome and maybe trip the auto-block breaker
	 * (parity _record_task_failure spawn path): counter += 1; when the
	 * effective limit is reached the card transitions to blocked with the
	 * last error recorded; otherwise it is released back to ready for the
	 * next tick. Effective limit resolution order: per-task maxRetries →
	 * caller failureLimit.
	 */
	recordFailure(
		cardId: string,
		outcome: FailureOutcome,
		error: string,
		opts: { failureLimit: number; nowSeconds: number },
	): Promise<{ blocked: boolean; failures: number }>;

	/**
	 * Mark a successful completion: status → done, claims cleared, the
	 * consecutive-failure breaker counter RESETS (parity complete_task).
	 * CAS-guarded like every other transition; returns false when the card
	 * was not claimable-complete.
	 */
	completeCard(cardId: string, nowSeconds: number): Promise<boolean>;

	/** Append-only event log for diagnostics (parity _append_event). */
	events(cardId: string): Promise<Array<{ event: string; at: number }>>;
}

/** Outcome of ONE dispatcher tick (subset of DispatchResult fields the v0.1 tick realizes). */
export interface DispatchResult {
	board: string;
	/** Card ids reclaimed from expired claims this tick. */
	reclaimed: string[];
	/** Card ids promoted todo/blocked → ready this tick. */
	promoted: string[];
	/** Cards claimed and handed to spawnFn as (cardId, assignee). */
	spawned: Array<{ cardId: string; assignee: string }>;
	/** Ready cards skipped for having no assignee (operator-actionable). */
	skippedUnassigned: string[];
	/** Cards auto-blocked by the failure breaker this tick. */
	autoBlocked: string[];
}

/** Spawn callback parity: spawn_fn(task, workspace_path, board). */
export type SpawnFn = (card: KanbanCard) => Promise<void> | void;

export interface DispatcherConfig {
	failureLimit: number;
	maxSpawn?: number | null;
}

/** Raised by dispatchOnce on programmer errors in the tick itself. */
export class DispatchTickError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "DispatchTickError";
	}
}

export function resolveDispatcherConfig(raw: {
	failureLimit?: unknown;
	maxSpawn?: unknown;
}): DispatcherConfig {
	const failureLimitRaw = raw.failureLimit;
	let failureLimit =
		typeof failureLimitRaw === "number" && Number.isFinite(failureLimitRaw)
			? Math.trunc(failureLimitRaw)
			: DEFAULT_FAILURE_LIMIT;
	if (failureLimit < 1) failureLimit = DEFAULT_FAILURE_LIMIT;
	const maxSpawnRaw = raw.maxSpawn;
	const maxSpawn =
		typeof maxSpawnRaw === "number" && Number.isFinite(maxSpawnRaw)
			? Math.trunc(maxSpawnRaw)
			: null;
	return {
		failureLimit,
		maxSpawn: maxSpawn !== null && maxSpawn < 1 ? null : maxSpawn,
	};
}

/** Parity of kanban_db.py:DEFAULT_FAILURE_LIMIT = 2 (07 §6 auto-block). */
export const DEFAULT_FAILURE_LIMIT = 2;

export type { GatewayClock };
