// rail.ts — the durable delivery side of async-delegation completions
// (06 §7 store side; DEC-018). ONE SQLite row carries TWO independent state
// machines: the dispatch state (dispatched → running → completed | failed |
// stalled | unknown) and the DELIVERY machine that rides it
// (pending → claimed → delivered | dropped, expressed as delivery_state +
// delivery_claim/delivery_claimed_at columns — a claim never flips
// delivery_state, so restore keeps seeing claimed-but-unacked rows).
//
// Ported semantics, Hermes anchors (READ-ONLY reference; no code vendored):
//   tools/async_delegation.py:_persist_dispatch        → recordDispatch
//   tools/async_delegation.py:_persist_completion      → publishCompletion
//   tools/async_delegation.py:claim_completion_delivery → claimCompletion
//     (atomic claim WHERE pending AND (claim NULL OR claim older than 300 s);
//     attempt counter increments PER CLAIM)
//   tools/async_delegation.py:_MAX_DELIVERY_ATTEMPTS=8 → MAX_DELIVERY_ATTEMPTS;
//     release on an exhausted budget converges to terminal 'dropped' instead
//     of returning to pending (unroutable rows must not replay forever)
//   tools/async_delegation.py:release_completion_delivery → releaseClaim
//   tools/async_delegation.py:drop_completion_delivery    → dropClaim
//   tools/async_delegation.py:complete_completion_delivery → completeClaim
//   tools/async_delegation.py:mark_completion_delivered    → markDelivered
//   tools/async_delegation.py:recover_abandoned_delegations → recoverAbandoned
//     (dead-owner running rows synthesize a status:'unknown' completion)
//   tools/async_delegation.py:restore_undelivered_completions → restoreUndelivered
//     (pending rows re-enqueued stamped restored=True — IN-MEMORY ONLY; the
//     48 h replay-age cap terminally drops stale pending rows, payload queryable)
//   tools/async_delegation.py:_prune_durable_records   → pruneDurable
//   tools/async_delegation.py:claim_event_delivery     → makeClaimId
//
// Every mutation is a single guarded UPDATE inside BEGIN IMMEDIATE
// (pi_state/wal.ts executeWrite), so two engines/processes racing the
// handshake get exactly one winner per row transition.
//
// Divergences from Hermes (each needs a DEC entry — see phase report):
//   1. PROPOSED DEC-035: claimCompletion on an UNKNOWN delegation_id returns
//      false. Hermes returned true for events created before durable
//      dispatch existed ("legacy" rows); a greenfield store has no
//      pre-durable events, and silently succeeding on unknown ids would
//      break the exactly-one-owner contract this module exists to prove.
//   2. pruneDurable's active-state set is ('dispatched','running',
//      'finalizing') vs Hermes ('running','finalizing'): 06 §7.1 adds the
//      explicit pre-running 'dispatched' vocabulary; both are non-terminal
//      for retention purposes.

import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";

import type Database from "better-sqlite3";

import { executeWrite } from "../../pi_state/wal.js";
import { systemClock, type GatewayClock } from "./clock.js";

export { systemClock };
export type { GatewayClock };

/** Verified Hermes constants (async_delegation.py module head). */
export const CLAIM_STALE_SECONDS = 300;
export const MAX_DELIVERY_ATTEMPTS = 8;
export const COMPLETION_REPLAY_AGE_SECONDS = 48 * 3600;
export const DURABLE_RETENTION_SECONDS = 7 * 24 * 60 * 60;
export const MAX_DURABLE_PENDING = 1000;
/** Terminal-row history bound (parity _MAX_RETAINED_COMPLETED). */
export const MAX_RETAINED_TERMINAL = 50;

/** Non-terminal dispatch states retained by pruneDurable (divergence 2). */
export const ACTIVE_DISPATCH_STATES: readonly string[] = [
	"dispatched",
	"running",
	"finalizing",
];

export type DeliveryState = "pending" | "delivered" | "dropped";

export interface DelegationRow {
	delegation_id: string;
	origin_session: string;
	parent_session_id: string | null;
	state: string;
	dispatched_at: number;
	completed_at: number | null;
	updated_at: number;
	event_json: string | null;
	result_json: string | null;
	task_json: string | null;
	delivery_state: DeliveryState;
	delivery_attempts: number;
	delivered_at: number | null;
	owner_pid: number | null;
	owner_started_at: number | null;
	delivery_claim: string | null;
	delivery_claimed_at: number | null;
}

export interface OwnerStamp {
	pid: number;
	/** Boot-relative process start ticks; null when unavailable. */
	startedAt: number | null;
}

export interface DispatchInput {
	delegationId: string;
	/** session_key of the dispatching conversation. */
	originSession: string;
	parentSessionId?: string | null;
	/** task payload persisted for post-restart routing reconstruction. */
	task?: Record<string, unknown>;
	/** Dispatch-machine state to persist (default 'running', parity). */
	state?: string;
	dispatchedAt?: number;
}

export interface CompletionPublishInput {
	delegationId: string;
	/** Final dispatch-machine state (default 'completed'). */
	status?: string;
	/** The forged-inbound event payload (persisted as event_json). */
	event: Record<string, unknown>;
	/** The result payload (persisted as result_json; stays queryable). */
	result?: Record<string, unknown>;
	completedAt?: number;
}

export interface NowOptions {
	/** Injected now (epoch seconds); defaults to the rail's clock. */
	nowSeconds?: number;
}

interface ExpiryCandidateRow {
	delegation_id: string;
	completed_at: number | null;
	dispatched_at: number;
}

interface RestoreCandidateRow extends ExpiryCandidateRow {
	event_json: string;
}

/**
 * Linux-only cheap start-time read (/proc/<pid>/stat field 22); null when
 * unavailable. Same primitive as obligations/ledger.ts (kept local so each
 * engine owns its seams).
 */
export function readProcessStartTime(pid: number): number | null {
	if (process.platform !== "linux") return null;
	try {
		const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
		const afterComm = stat.slice(stat.lastIndexOf(")") + 2);
		const raw = afterComm.split(" ")[19]; // overall field 22; slice starts at 3
		if (raw === undefined) return null;
		const value = Number.parseInt(raw, 10);
		return Number.isFinite(value) ? value : null;
	} catch {
		return null;
	}
}

/** Conservative kernel liveness probe: ESRCH proves death; doubt ⇒ alive. */
function defaultProcessAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (err) {
		return (err as NodeJS.ErrnoException)?.code === "ESRCH" ? false : true;
	}
}

export class DelegationRailError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "DelegationRailError";
	}
}

export class DelegationNotFoundError extends DelegationRailError {
	readonly delegationId: string;
	constructor(delegationId: string) {
		super(`delegation not found: ${delegationId}`);
		this.name = "DelegationNotFoundError";
		this.delegationId = delegationId;
	}
}

export interface DelegationRailOptions {
	clock?: GatewayClock;
	/** Override this process's ownership stamp (tests/multi-engine simulation). */
	selfStamp?: OwnerStamp;
	/** Liveness probe override (default ESRCH-only kill(pid,0)). */
	processAlive?: (pid: number) => boolean;
	/** Start-time reader override (default /proc field 22). */
	processStartTime?: (pid: number) => number | null;
}

/**
 * The durable rail engine. One instance per open DB connection; safe to
 * share one DB across instances/processes — every transition is one CAS'd
 * UPDATE inside one write transaction.
 */
export class DelegationRail {
	readonly db: Database.Database;
	private readonly clock: GatewayClock;
	private readonly selfStamp: OwnerStamp;
	private readonly processAliveFn: (pid: number) => boolean;
	private readonly processStartTimeFn: (pid: number) => number | null;

	constructor(db: Database.Database, opts: DelegationRailOptions = {}) {
		this.db = db;
		this.clock = opts.clock ?? systemClock;
		this.selfStamp = opts.selfStamp ?? {
			pid: process.pid,
			startedAt: readProcessStartTime(process.pid),
		};
		this.processAliveFn = opts.processAlive ?? defaultProcessAlive;
		this.processStartTimeFn = opts.processStartTime ?? readProcessStartTime;
	}

	nowSeconds(): number {
		return this.clock.nowSeconds();
	}

	/** Claim-token factory (parity claim_event_delivery). */
	makeClaimId(consumer: string): string {
		return `${consumer}:${process.pid}:${randomUUID()}`;
	}

	// ------------------------------------------------------------------
	// Producer side (durable publish BEFORE the shared completion queue)
	// ------------------------------------------------------------------

	/** Persist the dispatch record (parity _persist_dispatch), then prune. */
	async recordDispatch(
		input: DispatchInput,
		opts: NowOptions = {},
	): Promise<void> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		await executeWrite(this.db, (conn) => {
			conn
				.prepare(
					`INSERT OR REPLACE INTO async_delegations
				   (delegation_id, origin_session, parent_session_id, state,
				    dispatched_at, updated_at, delivery_state, delivery_attempts,
				    owner_pid, owner_started_at, task_json)
				 VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?)`,
				)
				.run(
					input.delegationId,
					input.originSession,
					input.parentSessionId ?? null,
					input.state ?? "running",
					input.dispatchedAt ?? now,
					now,
					this.selfStamp.pid,
					this.selfStamp.startedAt,
					input.task === undefined ? null : JSON.stringify(input.task),
				);
		});
		await this.pruneDurable(opts);
	}

	/**
	 * Write the completion event + result durably with delivery_state reset
	 * to 'pending' (parity _persist_completion). Callers enter the shared
	 * completion queue only AFTER this resolves.
	 */
	async publishCompletion(
		input: CompletionPublishInput,
		opts: NowOptions = {},
	): Promise<void> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		const status = input.status ?? "completed";
		await executeWrite(this.db, (conn) => {
			const cursor = conn
				.prepare(
					`UPDATE async_delegations
					 SET state = ?, completed_at = ?, updated_at = ?,
					     event_json = ?, result_json = ?, delivery_state = 'pending'
					 WHERE delegation_id = ?`,
				)
				.run(
					status,
					input.completedAt ?? now,
					now,
					JSON.stringify(input.event),
					input.result === undefined ? null : JSON.stringify(input.result),
					input.delegationId,
				);
			if (cursor.changes === 0)
				throw new DelegationNotFoundError(input.delegationId);
		});
	}

	// ------------------------------------------------------------------
	// Handshake: claim / release / drop / complete (+ unguarded ack)
	// ------------------------------------------------------------------

	/**
	 * Atomically claim one pending completion across competing consumers.
	 * Wins iff the row is pending AND its claim is absent or stale
	 * (> CLAIM_STALE_SECONDS old); increments the per-claim attempt counter.
	 * Returns false when another live claim owns the row or the row is not
	 * pending. Divergence 1: unknown ids return FALSE (see header).
	 */
	async claimCompletion(
		delegationId: string,
		claimId: string,
		opts: NowOptions = {},
	): Promise<boolean> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		return executeWrite(this.db, (conn) => {
			const cursor = conn
				.prepare(
					`UPDATE async_delegations
					 SET delivery_claim = ?, delivery_claimed_at = ?,
					     delivery_attempts = delivery_attempts + 1, updated_at = ?
					 WHERE delegation_id = ?
					   AND delivery_state = 'pending'
					   AND (delivery_claim IS NULL OR delivery_claimed_at < ?)`,
				)
				.run(claimId, now, now, delegationId, now - CLAIM_STALE_SECONDS);
			return cursor.changes === 1;
		});
	}

	/**
	 * Release a failed delivery claim so another consumer may retry.
	 * Attempts are counted at CLAIM time, so a repeatedly claimed+released
	 * row burns real budget; once exhausted it converges to terminal
	 * 'dropped' instead of returning to pending. Returns true when the row
	 * was released OR converged-to-dropped by THIS claim holder.
	 */
	async releaseClaim(
		delegationId: string,
		claimId: string,
		opts: NowOptions = {},
	): Promise<boolean> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		return executeWrite(this.db, (conn) => {
			const capped = conn
				.prepare(
					`UPDATE async_delegations SET delivery_state = 'dropped',
					       delivery_claim = NULL, delivery_claimed_at = NULL, updated_at = ?
					 WHERE delegation_id = ? AND delivery_state = 'pending'
					   AND delivery_claim = ? AND delivery_attempts >= ?`,
				)
				.run(now, delegationId, claimId, MAX_DELIVERY_ATTEMPTS);
			if (capped.changes === 1) return true;
			const released = conn
				.prepare(
					`UPDATE async_delegations SET delivery_claim = NULL,
					       delivery_claimed_at = NULL, updated_at = ?
					 WHERE delegation_id = ? AND delivery_state = 'pending'
					   AND delivery_claim = ?`,
				)
				.run(now, delegationId, claimId);
			return released.changes === 1;
		});
	}

	/**
	 * Terminally drop a claimed completion that can NEVER be delivered
	 * (parent ended at a user boundary — #55578). NOT 'delivered' keeps the
	 * ack honest; NOT 'pending' keeps restart recovery from replaying a row
	 * that would be fail-closed dropped again every boot.
	 */
	async dropClaim(
		delegationId: string,
		claimId: string,
		opts: NowOptions = {},
	): Promise<boolean> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		return executeWrite(this.db, (conn) => {
			const cursor = conn
				.prepare(
					`UPDATE async_delegations SET delivery_state = 'dropped',
					       updated_at = ?, delivery_claim = NULL, delivery_claimed_at = NULL
					 WHERE delegation_id = ? AND delivery_state = 'pending'
					   AND delivery_claim = ?`,
				)
				.run(now, delegationId, claimId);
			return cursor.changes === 1;
		});
	}

	/**
	 * Ack acceptance FOR THE HOLDER OF THIS CLAIM (parity
	 * complete_completion_delivery): pending + matching claim → delivered.
	 */
	async completeClaim(
		delegationId: string,
		claimId: string,
		opts: NowOptions = {},
	): Promise<boolean> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		return executeWrite(this.db, (conn) => {
			const cursor = conn
				.prepare(
					`UPDATE async_delegations SET delivery_state = 'delivered',
					       delivered_at = ?, updated_at = ?, delivery_claim = NULL,
					       delivery_claimed_at = NULL
					 WHERE delegation_id = ? AND delivery_state = 'pending'
					   AND delivery_claim = ?`,
				)
				.run(now, now, delegationId, claimId);
			return cursor.changes === 1;
		});
	}

	/**
	 * Unguarded ack after adapter acceptance (parity mark_completion_delivered):
	 * first writer moves any non-delivered row to delivered; repeat acks are
	 * false (idempotent-at-most-once semantics).
	 */
	async markDelivered(
		delegationId: string,
		opts: NowOptions = {},
	): Promise<boolean> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		return executeWrite(this.db, (conn) => {
			const cursor = conn
				.prepare(
					`UPDATE async_delegations SET delivery_state = 'delivered',
					       delivered_at = ?, updated_at = ?
					 WHERE delegation_id = ? AND delivery_state != 'delivered'`,
				)
				.run(now, now, delegationId);
			return cursor.changes === 1;
		});
	}

	// ------------------------------------------------------------------
	// Boot restore (parity restore_undelivered_completions)
	// ------------------------------------------------------------------

	/**
	 * Classify running/finalizing rows whose owning PROCESS disappeared as
	 * outcome unknown (parity recover_abandoned_delegations): synthesized
	 * status:'unknown' completion published with delivery reset to pending so
	 * the result is queryable and routable instead of stuck forever. Live
	 * owners are never touched. Returns the recovered count.
	 */
	async recoverAbandoned(opts: NowOptions = {}): Promise<number> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		return executeWrite(this.db, (conn) => {
			const rows = conn
				.prepare(
					`SELECT delegation_id, origin_session, parent_session_id,
					        dispatched_at, owner_pid, owner_started_at, task_json
					 FROM async_delegations WHERE state IN ('running','finalizing')`,
				)
				.all() as unknown as Array<{
				delegation_id: string;
				origin_session: string;
				parent_session_id: string | null;
				dispatched_at: number;
				owner_pid: number | null;
				owner_started_at: number | null;
				task_json: string | null;
			}>;
			let recovered = 0;
			for (const row of rows) {
				if (this.rowOwnerIsAlive(row.owner_pid, row.owner_started_at)) continue;
				let task: Record<string, unknown> = {};
				try {
					const parsed: unknown = JSON.parse(row.task_json ?? "{}");
					if (parsed !== null && typeof parsed === "object") {
						task = parsed as Record<string, unknown>;
					}
				} catch {
					task = {};
				}
				const errorText =
					"Delegation owner exited before recording a terminal result; outcome unknown.";
				const event: Record<string, unknown> = {
					type: "async_delegation",
					delegation_id: row.delegation_id,
					session_key: row.origin_session,
					parent_session_id: row.parent_session_id,
					goal: task["goal"] ?? "",
					goals: task["goals"],
					context: task["context"],
					status: "unknown",
					summary: null,
					error: errorText,
					dispatched_at: row.dispatched_at,
					completed_at: now,
				};
				// Routing origin captured at dispatch rides along (scope_id /
				// user_id / user_name) so post-restart scoped completions stay
				// routable (staging 2026-08-09 defect #4).
				for (const key of ["scope_id", "user_id", "user_name"]) {
					const value = task[key];
					if (typeof value === "string" && value !== "") event[key] = value;
				}
				conn
					.prepare(
						`UPDATE async_delegations SET state = 'unknown', completed_at = ?,
						       updated_at = ?, event_json = ?, result_json = ?,
						       delivery_state = 'pending'
						 WHERE delegation_id = ?`,
					)
					.run(
						now,
						now,
						JSON.stringify(event),
						JSON.stringify({
							status: "unknown",
							summary: null,
							error: errorText,
						}),
						row.delegation_id,
					);
				recovered++;
			}
			return recovered;
		});
	}

	/**
	 * Replay-age cap applied to restore candidates (parity of the inline
	 * staleness branch): pending completions older than
	 * COMPLETION_REPLAY_AGE_SECONDS (age basis: completed_at, else
	 * dispatched_at; STRICTLY greater drops) converge to terminal 'dropped'
	 * with the claim cleared — payload stays queryable. Returns dropped count.
	 */
	async pruneExpiredPending(opts: NowOptions = {}): Promise<number> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		return executeWrite(this.db, (conn) => {
			const rows = conn
				.prepare(
					`SELECT delegation_id, completed_at, dispatched_at
					 FROM async_delegations
					 WHERE state != 'running' AND delivery_state = 'pending'
					   AND event_json IS NOT NULL`,
				)
				.all() as unknown as ExpiryCandidateRow[];
			let dropped = 0;
			for (const row of rows) {
				const ageBasis = row.completed_at ?? row.dispatched_at;
				if (!ageBasis) continue;
				if (now - ageBasis <= COMPLETION_REPLAY_AGE_SECONDS) continue;
				const cursor = conn
					.prepare(
						`UPDATE async_delegations SET delivery_state = 'dropped',
						       delivery_claim = NULL, delivery_claimed_at = NULL, updated_at = ?
						 WHERE delegation_id = ? AND delivery_state = 'pending'`,
					)
					.run(now, row.delegation_id);
				dropped += cursor.changes;
			}
			return dropped;
		});
	}

	/**
	 * Re-enqueue undelivered durable completions as fresh turns after process
	 * start (parity restore_undelivered_completions). Every restored event is
	 * stamped restored=true — IN MEMORY ONLY, added AFTER deserialization,
	 * never persisted. Restored events originate from a PREVIOUS process, so
	 * no consumer in THIS process implicitly owns them: drain paths without
	 * an ownership filter must leave them for consumers proving ownership
	 * (#64484). Runs recoverAbandoned first (Hermes order). Synchronous sink;
	 * a throwing sink rolls the whole pass back. Returns restored count.
	 */
	async restoreUndelivered(
		sink: (evt: Record<string, unknown>) => void,
		opts: NowOptions = {},
	): Promise<number> {
		await this.recoverAbandoned(opts);
		await this.pruneExpiredPending(opts); // same predicates + clock as this pass
		return executeWrite(this.db, (conn) => {
			const rows = conn
				.prepare(
					`SELECT delegation_id, event_json, completed_at, dispatched_at
					 FROM async_delegations
					 WHERE state != 'running' AND delivery_state = 'pending'
					   AND event_json IS NOT NULL
					 ORDER BY completed_at, delegation_id`,
				)
				.all() as unknown as RestoreCandidateRow[];
			let restored = 0;
			for (const row of rows) {
				let evt: unknown;
				try {
					evt = JSON.parse(row.event_json);
				} catch {
					continue; // unparsable payload is not replayable
				}
				if (evt !== null && typeof evt === "object") {
					(evt as Record<string, unknown>)["restored"] = true;
				}
				sink(evt as Record<string, unknown>);
				restored++;
			}
			return restored;
		});
	}

	// ------------------------------------------------------------------
	// Retention GC (parity _prune_durable_records)
	// ------------------------------------------------------------------

	/**
	 * Bound terminal history preferring delivered rows for deletion, then cap
	 * pending volume oldest-first. Best-effort: failures return 0.
	 */
	async pruneDurable(opts: NowOptions = {}): Promise<number> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		try {
			return await executeWrite(this.db, (conn) => {
				let deleted = conn
					.prepare(
						`DELETE FROM async_delegations
						 WHERE delivery_state = 'delivered' AND updated_at < ?`,
					)
					.run(now - DURABLE_RETENTION_SECONDS).changes;
				const activeList = ACTIVE_DISPATCH_STATES.map((s) => `'${s}'`).join(
					",",
				);
				const terminalCount = (
					conn
						.prepare(
							`SELECT COUNT(*) AS n FROM async_delegations
							 WHERE state NOT IN (${activeList})`,
						)
						.get() as { n: number }
				).n;
				const excess = Math.max(0, terminalCount - MAX_RETAINED_TERMINAL);
				if (excess > 0) {
					deleted += conn
						.prepare(
							`DELETE FROM async_delegations WHERE delegation_id IN (
							   SELECT delegation_id FROM async_delegations
							   WHERE state NOT IN (${activeList})
							   ORDER BY CASE delivery_state WHEN 'delivered' THEN 0 ELSE 1 END,
							            updated_at ASC LIMIT ?
							 )`,
						)
						.run(excess).changes;
				}
				const pendingCount = (
					conn
						.prepare(
							`SELECT COUNT(*) AS n FROM async_delegations
							 WHERE state NOT IN (${activeList}) AND delivery_state = 'pending'`,
						)
						.get() as { n: number }
				).n;
				const overflow = Math.max(0, pendingCount - MAX_DURABLE_PENDING);
				if (overflow > 0) {
					deleted += conn
						.prepare(
							`DELETE FROM async_delegations WHERE delegation_id IN (
							   SELECT delegation_id FROM async_delegations
							   WHERE state NOT IN (${activeList}) AND delivery_state = 'pending'
							   ORDER BY updated_at ASC LIMIT ?
							 )`,
						)
						.run(overflow).changes;
				}
				return deleted;
			});
		} catch {
			return 0; // best-effort GC never blocks dispatch/delivery
		}
	}

	// ------------------------------------------------------------------
	// Reads
	// ------------------------------------------------------------------

	row(delegationId: string): DelegationRow | null {
		const r = this.db
			.prepare("SELECT * FROM async_delegations WHERE delegation_id = ?")
			.get(delegationId) as unknown as DelegationRow | undefined;
		return r ?? null;
	}

	deliveryStateOf(delegationId: string): DeliveryState | null {
		return this.row(delegationId)?.delivery_state ?? null;
	}

	countByDeliveryState(state: DeliveryState): number {
		return (
			this.db
				.prepare(
					"SELECT COUNT(*) AS n FROM async_delegations WHERE delivery_state = ?",
				)
				.get(state) as { n: number }
		).n;
	}

	selfOwner(): OwnerStamp {
		return { ...this.selfStamp };
	}

	// internal ----------------------------------------------------------

	private rowOwnerIsAlive(
		pid: number | null,
		startedAt: number | null,
	): boolean {
		if (!pid) return false;
		let live = this.processAliveFn(pid);
		if (live && startedAt !== null && startedAt !== undefined) {
			const currentStart = this.processStartTimeFn(pid);
			live = currentStart !== null && currentStart === startedAt;
		}
		return live;
	}
}
