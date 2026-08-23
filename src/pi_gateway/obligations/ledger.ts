// ledger.ts — the durable delivery-obligations engine over pi_state's
// `delivery_obligations` table (DDL already ships in pi_state/schema.ts; this
// module NEVER migrates — 02 §2.1, DEC-007).
//
// Ported semantics, Hermes anchors (READ-ONLY reference; no code vendored):
//   gateway/delivery_ledger.py:MAX_ATTEMPTS/STALE_AFTER_SECONDS/_RETENTION_SECONDS/_MAX_ROWS
//     → MAX_ATTEMPTS=3 / STALE_AFTER_SECONDS=86400 / RETENTION_SECONDS=604800 / MAX_ROWS=500
//   gateway/delivery_ledger.py:compute_obligation_id  → computeObligationId
//     (sha256("sessionKey|messageRef|content") hex[:24]; stable re-record id)
//   gateway/delivery_ledger.py:record_obligation      → record (INSERT OR REPLACE,
//     state='pending', attempts=0, owner=self) + immediate prune()
//   gateway/delivery_ledger.py:_update_state          → guarded CAS transitions
//     markAttempting/markDelivered/markFailed (last_error truncated to 500 chars)
//   gateway/delivery_ledger.py:sweep_recoverable      → sweepRecoverable: claims
//     non-terminal rows whose owner process is DEAD; capped/stale rows become
//     'abandoned'; absent platforms are skipped WITHOUT spending an attempt;
//     the claim is an atomic guarded UPDATE (`owner_pid IS ? OR owner_pid=?`)
//     so two racing gateways can never double-claim.
//   gateway/delivery_ledger.py:_prune                 → prune: deletes
//     delivered/abandoned rows older than 7d by updated_at, then enforces the
//     500-row cap evicting delivered < abandoned < active, oldest first.
//   gateway/delivery_ledger.py:_owner_alive (+gateway/status.py:get_process_start_time)
//     → ownerIsAlive via pid + process-start-time stamp; unreadable start time
//     falls back to a conservative existence probe (EPERM-means-alive).
//
// Crash-ambiguity contract (delivery_ledger.py module docstring): pending
// rows redeliver plainly; attempting/failed rows are ambiguous or previously
// rejected and redeliver WITH the visible recovered-reply marker (sender.ts).
//
// Best-effort posture: ledger failures must never block a real send. The
// drive/settle helpers swallow and report; only protocol violations (illegal
// transitions, unknown ids) throw typed errors for callers to observe.
//
// Divergences from Hermes (each needs a DEC entry — see PROGRESS report):
//   1. State mutations are CAS-guarded to the legal-transition set and throw
//      IllegalTransitionError instead of silently overwriting (Hermes relied
//      on call-site discipline with unconditional UPDATEs).
//   2. In-process retry scheduling with exponential backoff exists here
//      (Hermes retries ONLY at restart boundaries). Constants proposed in
//      scheduler.ts; caps/stale rules stay Hermes-exact.

import type Database from "better-sqlite3";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

import { executeWrite } from "../../pi_state/wal.js";
import { systemClock, type GatewayClock } from "./clock.js";
import {
	composeDeliveryContent,
	normalizeSendFailure,
	type DeliveryOutcome,
	type DeliveryRequest,
	type DeliverySender,
} from "./sender.js";

export { systemClock };
export type { GatewayClock };

/** Verified Hermes constants (delivery_ledger.py module head; DEC-007). */
export const MAX_ATTEMPTS = 3;
export const STALE_AFTER_SECONDS = 24 * 60 * 60;
export const RETENTION_SECONDS = 7 * 24 * 60 * 60;
export const MAX_ROWS = 500;
export const LAST_ERROR_MAX_CHARS = 500;

export type ObligationState =
	| "pending"
	| "attempting"
	| "delivered"
	| "failed"
	| "abandoned";

export const OBLIGATION_STATES: readonly ObligationState[] = [
	"pending",
	"attempting",
	"delivered",
	"failed",
	"abandoned",
];

/**
 * Backoff before the next redelivery given completed redeliveries so far.
 * PROPOSED DEC (no Hermes analogue — Hermes retries only at restart
 * boundaries): 60s base, ×4 growth, 1h cap. With MAX_ATTEMPTS=3 the worst
 * case spread is 60s + 240s + 960s ≈ 21min, comfortably inside the 24h
 * stale window. The behavior contract is: monotone growth, capped, never
 * busy.
 */
export const RETRY_BASE_SECONDS = 60;
export const RETRY_GROWTH_FACTOR = 4;
export const RETRY_MAX_SECONDS = 3600;

export function nextRetryDelaySeconds(attempts: number): number {
	const done =
		Number.isFinite(attempts) && attempts > 0 ? Math.floor(attempts) : 0;
	return Math.min(
		RETRY_BASE_SECONDS * RETRY_GROWTH_FACTOR ** Math.min(done, 8),
		RETRY_MAX_SECONDS,
	);
}

/**
 * Legal transition sources per target state. `abandoned` is written directly
 * by the claim/prune paths (cap + stale), never via this map; terminal states
 * have no outgoing edges. Port note: divergence (1) in the header.
 */
const LEGAL_SOURCES: Record<
	"attempting" | "delivered" | "failed",
	ObligationState[]
> = {
	attempting: ["pending", "failed"],
	delivered: ["pending", "attempting", "failed"],
	failed: ["pending", "attempting"],
};

export class ObligationLedgerError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "ObligationLedgerError";
	}
}

export class ObligationNotFoundError extends ObligationLedgerError {
	readonly obligationId: string;
	constructor(obligationId: string) {
		super(`obligation not found: ${obligationId}`);
		this.name = "ObligationNotFoundError";
		this.obligationId = obligationId;
	}
}

export class IllegalTransitionError extends ObligationLedgerError {
	readonly obligationId: string;
	readonly from: string;
	readonly to: string;
	constructor(obligationId: string, from: string, to: string) {
		super(
			`illegal obligation transition ${from} -> ${to} (${obligationId}); ` +
				`terminal rows (delivered/abandoned) are immutable and pending is never re-entered`,
		);
		this.name = "IllegalTransitionError";
		this.obligationId = obligationId;
		this.from = from;
		this.to = to;
	}
}

export interface OwnerStamp {
	pid: number;
	/** Boot-relative process start ticks (parity of get_process_start_time); null when unknown. */
	startedAt: number | null;
}

/**
 * Linux-only cheap start-time read (/proc/<pid>/stat field 22). Returns null
 * when unavailable (other platforms, race with exit, container without proc).
 */
export function readProcessStartTime(pid: number): number | null {
	if (process.platform !== "linux") return null;
	try {
		const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
		// comm (field 2) may contain spaces and parens — parse after its last ')'.
		const afterComm = stat.slice(stat.lastIndexOf(")") + 2);
		const fields = afterComm.split(" ");
		const raw = fields[19]; // field 22 overall; afterComm starts at field 3
		if (raw === undefined) return null;
		const value = Number.parseInt(raw, 10);
		return Number.isFinite(value) ? value : null;
	} catch {
		return null;
	}
}

/** Conservative kernel liveness probe: ESRCH proves death, everything else is doubt ⇒ alive. */
function defaultProcessAlive(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch (err) {
		return (err as NodeJS.ErrnoException)?.code === "ESRCH" ? false : true;
	}
}

export interface NewObligation {
	/** Precomputed id override; defaults to computeObligationId(...). */
	obligationId?: string;
	sessionKey: string;
	platform: string;
	chatId: string;
	threadId?: string | null;
	content: string;
	/** Triggering inbound message id — distinguishes turns sharing one session_key. */
	messageRef?: string;
}

export interface NowOptions {
	/** Injected now (epoch seconds); defaults to the ledger's clock. */
	nowSeconds?: number;
}

export interface SweepOptions extends NowOptions {
	/**
	 * Platforms the caller can actually send on right now. Rows outside the
	 * set are left untouched so a dead adapter's boot never burns the
	 * redelivery budget on a no-op (sweep_recoverable docstring parity).
	 */
	deliverablePlatforms?: ReadonlySet<string>;
}

export interface ClaimedObligation {
	obligationId: string;
	sessionKey: string;
	platform: string;
	chatId: string;
	threadId: string | null;
	/** RAW stored content — marker composition happens in the drive path. */
	content: string;
	/** true when the prior attempt was ambiguous/rejected (marker required). */
	needsMarker: boolean;
	/** Redelivery attempt budget position AFTER this claim (1-based). */
	attempts: number;
	/** Non-terminal state the row was in when claimed. */
	fromState: ObligationState;
}

interface ObligationRow {
	obligation_id: string;
	session_key: string;
	platform: string;
	chat_id: string;
	thread_id: string | null;
	content: string;
	state: string;
	attempts: number;
	created_at: number;
	updated_at: number;
	owner_pid: number | null;
	owner_started_at: number | null;
	last_error: string | null;
}

const NON_TERMINAL_STATES = "('pending', 'attempting', 'failed')";

const SELECT_CLAIMABLE = `
		SELECT obligation_id, session_key, platform, chat_id, thread_id,
		       content, state, attempts, created_at, updated_at,
		       owner_pid, owner_started_at, last_error
		FROM delivery_obligations
		WHERE state IN ${NON_TERMINAL_STATES}`;

/** Stable, idempotent obligation id (byte-parity of compute_obligation_id). */
export function computeObligationId(
	sessionKey: string,
	messageRef: string,
	content: string,
): string {
	const payload = `${sessionKey}|${messageRef}|${content}`;
	return createHash("sha256")
		.update(payload, "utf8")
		.digest("hex")
		.slice(0, 24);
}

export interface DeliveryLedgerOptions {
	/** Injected clock (default systemClock). No logic path reads Date.now directly. */
	clock?: GatewayClock;
	/** Override this process's ownership stamp (tests/multi-engine simulation). */
	selfStamp?: OwnerStamp;
	/** Liveness probe override (default ESRCH-only kill(pid,0)). */
	processAlive?: (pid: number) => boolean;
	/** Start-time reader override (default /proc field 22, null when unavailable). */
	processStartTime?: (pid: number) => number | null;
}

export interface SettleReport {
	obligationId: string;
	ok: boolean;
	error: string | null;
}

export interface InlineDeliveryReport {
	obligationId: string;
	recorded: boolean;
	outcome: DeliveryOutcome | null;
}

/**
 * The ledger engine. One instance per open DB connection; safe to share one
 * DB across instances/processes — every mutation is a single SQLite write
 * transaction and claims are CAS-guarded on the previous owner stamp.
 */
export class DeliveryLedger {
	readonly db: Database.Database;
	private readonly clock: GatewayClock;
	private readonly selfStamp: OwnerStamp;
	private readonly processAliveFn: (pid: number) => boolean;
	private readonly processStartTimeFn: (pid: number) => number | null;

	constructor(db: Database.Database, opts: DeliveryLedgerOptions = {}) {
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

	/** The clock this ledger runs on (scheduler default injection point). */
	nowClock(): GatewayClock {
		return this.clock;
	}

	// ------------------------------------------------------------------
	// Ownership liveness (parity _owner_alive)
	// ------------------------------------------------------------------

	ownerIsAlive(stamp: {
		pid: number | null;
		startedAt: number | null;
	}): boolean {
		if (!stamp.pid) return false;
		const currentStart = this.processStartTimeFn(stamp.pid);
		if (currentStart === null) {
			// Unreadable-but-maybe-extant: route through the conservative probe
			// (EPERM-means-alive; raw sig-0 semantics stay behind the seam).
			return this.processAliveFn(stamp.pid);
		}
		if (stamp.startedAt === null) return true; // cannot disprove
		return currentStart === stamp.startedAt;
	}

	private rowOwnerIsAlive(row: ObligationRow): boolean {
		return this.ownerIsAlive({
			pid: row.owner_pid,
			startedAt: row.owner_started_at,
		});
	}

	private isSelf(row: ObligationRow): boolean {
		return (
			row.owner_pid === this.selfStamp.pid &&
			row.owner_started_at !== null &&
			row.owner_started_at === this.selfStamp.startedAt
		);
	}

	// ------------------------------------------------------------------
	// Recording (parity record_obligation)
	// ------------------------------------------------------------------

	async record(input: NewObligation, opts: NowOptions = {}): Promise<string> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		const id =
			input.obligationId ??
			computeObligationId(
				input.sessionKey,
				input.messageRef ?? "",
				input.content,
			);
		await executeWrite(this.db, (conn) => {
			conn
				.prepare(
					`INSERT OR REPLACE INTO delivery_obligations
				   (obligation_id, session_key, platform, chat_id, thread_id,
				    content, state, attempts, created_at, updated_at,
				    owner_pid, owner_started_at, last_error)
				 VALUES (?, ?, ?, ?, ?, ?, 'pending', 0, ?, ?, ?, ?, NULL)`,
				)
				.run(
					id,
					input.sessionKey,
					input.platform,
					input.chatId,
					input.threadId != null ? String(input.threadId) : null,
					input.content,
					now,
					now,
					this.selfStamp.pid,
					this.selfStamp.startedAt,
				);
		});
		await this.prune(opts);
		return id;
	}

	// ------------------------------------------------------------------
	// Guarded state-machine transitions (divergence 1: CAS + typed errors)
	// ------------------------------------------------------------------

	private async casTransition(
		id: string,
		to: "attempting" | "delivered" | "failed",
		opts: NowOptions & { error?: string | null },
	): Promise<boolean> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		const error = to === "failed" ? (opts.error ?? "") : "";
		return executeWrite(this.db, (conn) => {
			const current = conn
				.prepare(
					"SELECT state FROM delivery_obligations WHERE obligation_id = ?",
				)
				.get(id) as { state: string } | undefined;
			if (current === undefined) throw new ObligationNotFoundError(id);
			const sources = LEGAL_SOURCES[to];
			if (!sources.includes(current.state as ObligationState)) {
				throw new IllegalTransitionError(id, current.state, to);
			}
			const res = conn
				.prepare(
					`UPDATE delivery_obligations
					 SET state = ?, updated_at = ?, last_error = ?
					 WHERE obligation_id = ?`,
				)
				.run(
					to,
					now,
					error.length > 0 ? error.slice(0, LAST_ERROR_MAX_CHARS) : null,
					id,
				);
			return res.changes === 1;
		});
	}

	/** pending|failed → attempting. Throws IllegalTransitionError/ObligationNotFoundError. */
	beginAttempt(id: string, opts: NowOptions = {}): Promise<boolean> {
		return this.casTransition(id, "attempting", opts);
	}

	/** any non-terminal → delivered. */
	markDelivered(id: string, opts: NowOptions = {}): Promise<boolean> {
		return this.casTransition(id, "delivered", opts);
	}

	/** pending|attempting → failed with truncated error (parity _update_state). */
	markFailed(
		id: string,
		error: string,
		opts: NowOptions = {},
	): Promise<boolean> {
		return this.casTransition(id, "failed", { ...opts, error });
	}

	// ------------------------------------------------------------------
	// Claiming — crash recovery (parity sweep_recoverable) and due retries
	// ------------------------------------------------------------------

	/**
	 * Claim undelivered rows owned by DEAD processes for redelivery. Capped or
	 * stale rows transition to 'abandoned' instead of being returned. Atomic
	 * guarded UPDATE ⇒ exactly one racing gateway ever claims a row.
	 */
	async sweepRecoverable(
		opts: SweepOptions = {},
	): Promise<ClaimedObligation[]> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		return executeWrite(this.db, (conn) =>
			this.claimTxn(conn, {
				now,
				deliverablePlatforms: opts.deliverablePlatforms,
				mode: "dead-owned-immediate",
			}),
		);
	}

	/**
	 * Claim rows THIS process owns (or that were orphaned by a dead owner)
	 * whose next backoff slot has arrived. Self-owned rows are gated by the
	 * retry schedule so a ticking loop can never busy-retry; dead-owned rows
	 * claim immediately (a restart boundary is a natural retry point).
	 * Capped/stale rows are abandoned on sight regardless of owner — leaving
	 * live poison rows in 'failed' forever would contradict the cap contract.
	 */
	async claimDueRetries(opts: SweepOptions = {}): Promise<ClaimedObligation[]> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		return executeWrite(this.db, (conn) =>
			this.claimTxn(conn, {
				now,
				deliverablePlatforms: opts.deliverablePlatforms,
				mode: "due-gated",
			}),
		);
	}

	private abandonRow(
		conn: Database.Database,
		obligationId: string,
		now: number,
	): void {
		conn
			.prepare(
				`UPDATE delivery_obligations
				 SET state = 'abandoned', updated_at = ?
				 WHERE obligation_id = ?`,
			)
			.run(now, obligationId);
	}

	private claimTxn(
		conn: Database.Database,
		cfg: {
			now: number;
			deliverablePlatforms?: ReadonlySet<string> | undefined;
			mode: "dead-owned-immediate" | "due-gated";
		},
	): ClaimedObligation[] {
		const rows = conn
			.prepare(SELECT_CLAIMABLE)
			.all() as unknown as ObligationRow[];
		const claimed: ClaimedObligation[] = [];
		for (const row of rows) {
			const aliveOwner = this.rowOwnerIsAlive(row);
			if (cfg.mode === "dead-owned-immediate") {
				if (aliveOwner) continue; // a live gateway still owns this row
			} else if (aliveOwner) {
				if (!this.isSelf(row)) continue; // another live engine owns it
				if (row.state === "attempting") continue; // OUR in-flight send — hands off
				// Poison rule FIRST (sweep parity): a capped or stale row we can
				// examine is abandoned on sight, regardless of backoff due-ness.
				if (
					row.attempts >= MAX_ATTEMPTS ||
					cfg.now - row.created_at > STALE_AFTER_SECONDS
				) {
					this.abandonRow(conn, row.obligation_id, cfg.now);
					continue;
				}
				const dueAt = row.updated_at + nextRetryDelaySeconds(row.attempts);
				if (cfg.now < dueAt) continue; // backoff window — no busy retry
			}
			// Dead-owned rows always fall through: a restart boundary is a
			// natural retry point (sweep_recoverable parity), no backoff gate.
			if (
				row.attempts >= MAX_ATTEMPTS ||
				cfg.now - row.created_at > STALE_AFTER_SECONDS
			) {
				this.abandonRow(conn, row.obligation_id, cfg.now);
				continue;
			}
			if (
				cfg.deliverablePlatforms !== undefined &&
				!cfg.deliverablePlatforms.has(row.platform)
			) {
				continue; // cannot send this boot — do NOT spend the budget
			}
			const cursor = conn
				.prepare(
					`UPDATE delivery_obligations
					 SET owner_pid = ?, owner_started_at = ?, attempts = attempts + 1,
					     updated_at = ?
					 WHERE obligation_id = ?
					   AND (owner_pid IS ? OR owner_pid = ?)`,
				)
				.run(
					this.selfStamp.pid,
					this.selfStamp.startedAt,
					cfg.now,
					row.obligation_id,
					row.owner_pid,
					row.owner_pid,
				);
			if (cursor.changes === 0) continue; // lost the race — exactly-one-winner
			claimed.push({
				obligationId: row.obligation_id,
				sessionKey: row.session_key,
				platform: row.platform,
				chatId: row.chat_id,
				threadId: row.thread_id,
				content: row.content,
				needsMarker: row.state !== "pending",
				attempts: row.attempts + 1,
				fromState: row.state as ObligationState,
			});
		}
		return claimed;
	}

	// ------------------------------------------------------------------
	// Driving claimed work through the sender seam
	// ------------------------------------------------------------------

	/**
	 * Send each claimed row through `sender` and settle the state machine.
	 * Marker composition happens HERE (driver parity of run.py). Every step
	 * is best-effort: a sender throw becomes ok:false ("send failed" parity),
	 * a settle failure never masks the send outcome.
	 */
	async driveClaimed(
		claimed: readonly ClaimedObligation[],
		sender: DeliverySender,
		opts: NowOptions = {},
	): Promise<SettleReport[]> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		const reports: SettleReport[] = [];
		for (const item of claimed) {
			const request: DeliveryRequest = {
				obligationId: item.obligationId,
				sessionKey: item.sessionKey,
				platform: item.platform,
				chatId: item.chatId,
				threadId: item.threadId,
				content: composeDeliveryContent(item.content, item.needsMarker),
				needsMarker: item.needsMarker,
				attempts: item.attempts,
			};
			let outcome: DeliveryOutcome;
			try {
				outcome = await sender(request);
			} catch (err) {
				outcome = normalizeSendFailure(err);
			}
			let settledError: string | null = null;
			try {
				if (outcome.ok) {
					await this.markDelivered(item.obligationId, { nowSeconds: now });
				} else {
					await this.markFailed(
						item.obligationId,
						outcome.error ?? "send failed",
						{
							nowSeconds: now,
						},
					);
					settledError = outcome.error ?? "send failed";
				}
			} catch {
				// Ledger trouble must never mask nor block delivery progress.
			}
			reports.push({
				obligationId: item.obligationId,
				ok: outcome.ok,
				error: outcome.ok ? null : settledError,
			});
		}
		return reports;
	}

	/**
	 * The inline first-send path (parity platforms/base.py inline block):
	 * record → beginAttempt → send → settle. Slash-command/ephemeral
	 * suppression policy lives upstream (adapters), not here.
	 */
	async deliverNew(
		input: NewObligation,
		sender: DeliverySender,
		opts: NowOptions = {},
	): Promise<InlineDeliveryReport> {
		const id = await this.record(input, opts);
		try {
			await this.beginAttempt(id, opts);
		} catch {
			// Record succeeded but the transition raced a sweeper — fall through
			// to the send anyway; whichever path settles first wins legally.
		}
		const reports = await this.driveClaimed(
			[
				{
					obligationId: id,
					sessionKey: input.sessionKey,
					platform: input.platform,
					chatId: input.chatId,
					threadId: input.threadId ?? null,
					content: input.content,
					needsMarker: false,
					attempts: 0,
					fromState: "attempting",
				},
			],
			sender,
			opts,
		);
		const first = reports[0];
		return {
			obligationId: id,
			recorded: true,
			outcome: first
				? { ok: first.ok, ...(first.error ? { error: first.error } : {}) }
				: null,
		};
	}

	// ------------------------------------------------------------------
	// Retention GC (parity _prune) — called after every record()
	// ------------------------------------------------------------------

	/**
	 * Delete delivered/abandoned rows past the 7d confirmation window, then
	 * enforce the 500-row hard cap (evict delivered < abandoned < active,
	 * oldest-updated first). Best-effort: failures never propagate.
	 * Returns the number of rows deleted (0 when pruning failed).
	 */
	async prune(opts: NowOptions = {}): Promise<number> {
		const now = opts.nowSeconds ?? this.nowSeconds();
		try {
			return await executeWrite(this.db, (conn) => {
				const cutoff = now - RETENTION_SECONDS;
				const gone = conn
					.prepare(
						`DELETE FROM delivery_obligations
						 WHERE state IN ('delivered', 'abandoned') AND updated_at < ?`,
					)
					.run(cutoff).changes;
				const total = (
					conn
						.prepare("SELECT COUNT(*) AS n FROM delivery_obligations")
						.get() as {
						n: number;
					}
				).n;
				const excess = Math.max(0, total - MAX_ROWS);
				if (excess > 0) {
					const evicted = conn
						.prepare(
							`DELETE FROM delivery_obligations WHERE obligation_id IN (
							   SELECT obligation_id FROM delivery_obligations
							   ORDER BY CASE state
							              WHEN 'delivered' THEN 0
							              WHEN 'abandoned' THEN 1
							              ELSE 2
							            END, updated_at ASC
							   LIMIT ?)`,
						)
						.run(excess).changes;
					return gone + evicted;
				}
				return gone;
			});
		} catch {
			return 0; // parity: "delivery ledger prune failed" at debug level
		}
	}

	// ------------------------------------------------------------------
	// Reads
	// ------------------------------------------------------------------

	row(obligationId: string): ObligationRow | null {
		return (
			(this.db
				.prepare("SELECT * FROM delivery_obligations WHERE obligation_id = ?")
				.get(obligationId) as unknown as ObligationRow | undefined) ?? null
		);
	}

	stateOf(obligationId: string): ObligationState | null {
		const r = this.row(obligationId);
		return r ? (r.state as ObligationState) : null;
	}

	selfOwner(): OwnerStamp {
		return { ...this.selfStamp };
	}
}
