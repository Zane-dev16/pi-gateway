// sqlite-board.ts — the production BoardClient over better-sqlite3.
//
// Every mutation is a single guarded UPDATE inside BEGIN IMMEDIATE
// (pi_state/wal.ts executeWrite), so two racers — two connections in this
// process, two child processes on the same file, or the dispatcher and a CLI
// verb — get exactly ONE winner per transition. This is the port of the
// hermes kanban_db.py write discipline (write_txn + rowcount checks):
//   claim_task            L4662-4696  → claimCard
//   release_stale_claims  L5030-5044  → reclaimStaleClaims
//   recompute_ready       L4510-4611  → promoteReady
//   _record_task_failure  L9152-9282  → recordFailure
//   complete_task         L5352-5467  → completeCard
//   _has_sticky_block / unblock_task reset semantics → stickyBlockSource
//
// Schema scope: the DISPATCHER's slice of the hermes `tasks` table (status,
// claim bookkeeping, breaker counters, priority/created_at ordering, parent
// links for promotion). The worker toolset verbs (comments, attachments,
// review lanes) are later-phase surfaces and intentionally absent.

import { randomUUID } from "node:crypto";

import type Database from "better-sqlite3";

import { executeWrite } from "../../pi_state/wal.js";
import type {
	BoardClient,
	CardStatus,
	ClaimRequest,
	FailureOutcome,
	KanbanCard,
	NewCard,
} from "./types.js";

/** Terminal statuses for dependency gating (parity _parents_satisfied). */
const PARENT_TERMINAL: ReadonlySet<string> = new Set(["done", "archived"]);

export interface SqliteBoardOptions {
	board: string;
}

export class SqliteKanbanBoard implements BoardClient {
	readonly board: string;
	private readonly db: Database.Database;

	constructor(db: Database.Database, opts: SqliteBoardOptions) {
		this.db = db;
		this.board = opts.board;
	}

	// ------------------------------------------------------------------
	// Schema
	// ------------------------------------------------------------------

	/** Create the dispatcher-slice schema if missing. Idempotent. */
	static ensureSchema(db: Database.Database): void {
		db.exec(`
			CREATE TABLE IF NOT EXISTS kanban_cards (
				id                   TEXT PRIMARY KEY,
				title                TEXT NOT NULL DEFAULT '',
				status               TEXT NOT NULL DEFAULT 'todo',
				assignee             TEXT,
				tenant               TEXT,
				priority             INTEGER NOT NULL DEFAULT 0,
				claim_lock           TEXT,
				claim_expires        INTEGER,
				consecutive_failures INTEGER NOT NULL DEFAULT 0,
				max_retries          INTEGER,
				last_failure_error   TEXT,
				started_at           INTEGER,
				created_at           INTEGER NOT NULL,
				current_run_id       TEXT
			);
			CREATE INDEX IF NOT EXISTS idx_kanban_cards_status
				ON kanban_cards(status);
			CREATE INDEX IF NOT EXISTS idx_kanban_cards_tenant
				ON kanban_cards(tenant);
			CREATE TABLE IF NOT EXISTS kanban_links (
				child_id  TEXT NOT NULL,
				parent_id TEXT NOT NULL,
				PRIMARY KEY (child_id, parent_id)
			);
			CREATE TABLE IF NOT EXISTS kanban_events (
				seq     INTEGER PRIMARY KEY AUTOINCREMENT,
				card_id TEXT NOT NULL,
				event   TEXT NOT NULL,
				payload TEXT,
				at      INTEGER NOT NULL
			);
			CREATE INDEX IF NOT EXISTS idx_kanban_events_card
				ON kanban_events(card_id);
		`);
	}

	// ------------------------------------------------------------------
	// Card creation (test/driver convenience; parity create_task subset)
	// ------------------------------------------------------------------

	createCard(newCard: NewCard = {}): KanbanCard {
		const id = newCard.id ?? `t_${randomUUID()}`;
		const createdAt = newCard.createdAt ?? Math.floor(Date.now() / 1000);
		const status = newCard.status ?? "todo";
		executeWrite(this.db, (tx) => {
			tx.prepare(
				`INSERT INTO kanban_cards
					(id, title, status, assignee, tenant, priority, max_retries, created_at)
					 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
			).run(
				id,
				newCard.title ?? "",
				status,
				newCard.assignee ?? null,
				newCard.tenant ?? null,
				newCard.priority ?? 0,
				newCard.maxRetries ?? null,
				createdAt,
			);
			appendEvent(tx, id, "created", createdAt);
		});
		const card = this.getCard(id);
		if (!card) throw new Error(`kanban: card ${id} vanished after insert`);
		return card;
	}

	/** Link child depends-on parent (parity task_links). */
	linkParent(childId: string, parentId: string): void {
		executeWrite(this.db, (tx) => {
			tx.prepare(
				"INSERT OR IGNORE INTO kanban_links (child_id, parent_id) VALUES (?, ?)",
			).run(childId, parentId);
		});
	}

	getCard(cardId: string): KanbanCard | null {
		const row = this.db
			.prepare("SELECT * FROM kanban_cards WHERE id = ?")
			.get(cardId) as RawCardRow | undefined;
		return row ? rowToCard(row) : null;
	}

	/**
	 * Operator block/unblock (parity block_task / unblock_task subset):
	 * manual blocks are STICKY — recompute_ready never auto-recovers them;
	 * an explicit unblock resets the breaker counter (fresh retry budget).
	 */
	async blockCard(
		cardId: string,
		nowSeconds: number,
		source: "manual" | "auto",
	): Promise<boolean> {
		return executeWrite(this.db, (tx) => {
			const cur = tx
				.prepare(
					"UPDATE kanban_cards SET status = 'blocked' WHERE id = ? AND status IN ('ready','running','review','todo')",
				)
				.run(cardId);
			if (cur.changes !== 1) return false;
			appendEvent(tx, cardId, "blocked", nowSeconds, source);
			return true;
		});
	}

	async unblockCard(
		cardId: string,
		toStatus: Extract<CardStatus, "ready" | "todo">,
		nowSeconds: number,
	): Promise<boolean> {
		return executeWrite(this.db, (tx) => {
			const cur = tx
				.prepare(
					`UPDATE kanban_cards SET status = ?, current_run_id = NULL,
						consecutive_failures = 0, last_failure_error = NULL
					 WHERE id = ? AND status = 'blocked'`,
				)
				.run(toStatus, cardId);
			if (cur.changes !== 1) return false;
			appendEvent(tx, cardId, "unblocked", nowSeconds);
			return true;
		});
	}

	// ------------------------------------------------------------------
	// BoardClient
	// ------------------------------------------------------------------

	async listReady(): Promise<KanbanCard[]> {
		const rows = this.db
			.prepare(
				`SELECT * FROM kanban_cards
				 WHERE status = 'ready' AND claim_lock IS NULL
				 ORDER BY priority DESC, created_at ASC`,
			)
			.all() as RawCardRow[];
		return rows.map(rowToCard);
	}

	async countRunning(): Promise<number> {
		const row = this.db
			.prepare(
				"SELECT COUNT(*) AS n FROM kanban_cards WHERE status = 'running'",
			)
			.get() as { n: number };
		return row.n;
	}

	async reclaimStaleClaims(nowSeconds: number): Promise<string[]> {
		const stale = this.db
			.prepare(
				`SELECT id, claim_lock FROM kanban_cards
				 WHERE status = 'running' AND claim_expires IS NOT NULL
				   AND claim_expires < ?`,
			)
			.all(nowSeconds) as Array<{ id: string; claim_lock: string | null }>;
		const reclaimed: string[] = [];
		for (const row of stale) {
			const done = await executeWrite(this.db, (tx) => {
				// Guarded CAS: still running, same lock, still expired.
				const cur = tx
					.prepare(
						`UPDATE kanban_cards SET status = 'ready', claim_lock = NULL,
							claim_expires = NULL
						 WHERE id = ? AND status = 'running' AND claim_lock IS ?
						 AND claim_expires IS NOT NULL AND claim_expires < ?`,
					)
					.run(row.id, row.claim_lock ?? null, nowSeconds);
				if (cur.changes !== 1) return false;
				tx.prepare(
					"UPDATE kanban_cards SET current_run_id = NULL WHERE id = ?",
				).run(row.id);
				appendEvent(tx, row.id, "reclaimed", nowSeconds);
				return true;
			});
			if (done) reclaimed.push(row.id);
		}
		return reclaimed;
	}

	async promoteReady(
		_nowSeconds: number,
		failureLimit: number,
	): Promise<string[]> {
		return executeWrite(this.db, (tx) => {
			const promoted: string[] = [];
			const rows = tx
				.prepare(
					`SELECT id, status, consecutive_failures, max_retries FROM kanban_cards
					 WHERE status IN ('todo','blocked') ORDER BY created_at ASC`,
				)
				.all() as Array<{
				id: string;
				status: string;
				consecutive_failures: number;
				max_retries: number | null;
			}>;
			for (const row of rows) {
				if (row.status === "blocked" && this.stickyBlock(tx, row.id)) continue;
				const undone = tx
					.prepare(
						`SELECT 1 FROM kanban_links l JOIN kanban_cards p ON p.id = l.parent_id
						 WHERE l.child_id = ? AND p.status NOT IN ('done','archived') LIMIT 1`,
					)
					.get(row.id);
				if (undone !== undefined) continue;
				if (row.status === "blocked") {
					// Breaker accumulation guard (parity recompute_ready #35072):
					// a card that tripped the circuit breaker stays blocked even
					// when its parents finish — otherwise every recovery cycle
					// would reset the trip and the breaker could never hold.
					const failures = Number(row.consecutive_failures ?? 0);
					const effective =
						row.max_retries !== null && row.max_retries !== undefined
							? Number(row.max_retries)
							: failureLimit;
					if (failures >= effective) continue;
				}
				const cur = tx
					.prepare(
						"UPDATE kanban_cards SET status = 'ready' WHERE id = ? AND status IN ('todo','blocked')",
					)
					.run(row.id);
				if (cur.changes !== 1) continue;
				appendEvent(tx, row.id, "promoted", _nowSeconds);
				promoted.push(row.id);
			}
			return promoted;
		});
	}

	async claimCard(request: ClaimRequest): Promise<KanbanCard | null> {
		return executeWrite(this.db, (tx) => {
			// Structural invariant (parity claim_task): never ready → running
			// while a parent is non-terminal; demote a racy writer's card.
			const undone = tx
				.prepare(
					`SELECT 1 FROM kanban_links l JOIN kanban_cards p ON p.id = l.parent_id
					 WHERE l.child_id = ? AND p.status NOT IN ('done','archived') LIMIT 1`,
				)
				.get(request.cardId);
			if (undone !== undefined) {
				tx.prepare(
					"UPDATE kanban_cards SET status = 'todo' WHERE id = ? AND status = 'ready'",
				).run(request.cardId);
				appendEvent(tx, request.cardId, "claim_rejected", request.nowSeconds);
				return null;
			}
			// THE CAS: exactly one racer's UPDATE flips the rowcount.
			const cur = tx
				.prepare(
					`UPDATE kanban_cards SET status = 'running', claim_lock = ?,
						claim_expires = ?, started_at = COALESCE(started_at, ?),
						current_run_id = ?
					 WHERE id = ? AND status = 'ready' AND claim_lock IS NULL`,
				)
				.run(
					request.lock,
					request.expiresAt,
					request.nowSeconds,
					`r_${randomUUID()}`,
					request.cardId,
				);
			if (cur.changes !== 1) return null;
			appendEvent(tx, request.cardId, "claimed", request.nowSeconds);
			const row = tx
				.prepare("SELECT * FROM kanban_cards WHERE id = ?")
				.get(request.cardId) as RawCardRow;
			return rowToCard(row);
		});
	}

	async recordFailure(
		cardId: string,
		outcome: FailureOutcome,
		error: string,
		opts: { failureLimit: number; nowSeconds: number },
	): Promise<{ blocked: boolean; failures: number }> {
		return executeWrite(this.db, (tx) => {
			const row = tx
				.prepare(
					"SELECT consecutive_failures, max_retries, status FROM kanban_cards WHERE id = ?",
				)
				.get(cardId) as
				| {
						consecutive_failures: number;
						max_retries: number | null;
						status: string;
				  }
				| undefined;
			if (row === undefined) return { blocked: false, failures: 0 };
			const failures = Number(row.consecutive_failures ?? 0) + 1;
			const effective =
				row.max_retries !== null && row.max_retries !== undefined
					? Number(row.max_retries)
					: opts.failureLimit;
			if (failures >= effective) {
				// Breaker trips: blocked with last error, claim released.
				tx.prepare(
					`UPDATE kanban_cards SET status = 'blocked', claim_lock = NULL,
						claim_expires = NULL, consecutive_failures = ?, last_failure_error = ?
					 WHERE id = ? AND status IN ('running','ready','review')`,
				).run(failures, error.slice(0, 500), cardId);
				appendEvent(tx, cardId, "gave_up", opts.nowSeconds);
				return { blocked: true, failures };
			}
			// Spawn-failure path: release back to ready, keep the counter.
			tx.prepare(
				`UPDATE kanban_cards SET status = 'ready', claim_lock = NULL,
					claim_expires = NULL, consecutive_failures = ?, last_failure_error = ?
				 WHERE id = ? AND status IN ('running','ready','review')`,
			).run(failures, error.slice(0, 500), cardId);
			appendEvent(tx, cardId, outcome, opts.nowSeconds);
			return { blocked: false, failures };
		});
	}

	async completeCard(cardId: string, nowSeconds: number): Promise<boolean> {
		return executeWrite(this.db, (tx) => {
			const cur = tx
				.prepare(
					`UPDATE kanban_cards SET status = 'done', claim_lock = NULL,
						claim_expires = NULL, consecutive_failures = 0,
						last_failure_error = NULL
					 WHERE id = ? AND status IN ('running','ready','blocked','review')`,
				)
				.run(cardId);
			if (cur.changes !== 1) return false;
			appendEvent(tx, cardId, "completed", nowSeconds);
			return true;
		});
	}

	async events(cardId: string): Promise<Array<{ event: string; at: number }>> {
		return this.db
			.prepare(
				"SELECT event, at FROM kanban_events WHERE card_id = ? ORDER BY seq ASC",
			)
			.all(cardId) as Array<{ event: string; at: number }>;
	}

	/**
	 * Parity _has_sticky_block: a block whose most recent block EVENT was
	 * worker/operator-initiated ("manual") stays until explicit unblock.
	 * Auto (breaker) blocks are recoverable by recompute_ready within limits.
	 */
	private stickyBlock(tx: Database.Database, cardId: string): boolean {
		const row = tx
			.prepare(
				`SELECT payload FROM kanban_events WHERE card_id = ? AND event = 'blocked'
				 ORDER BY seq DESC LIMIT 1`,
			)
			.get(cardId) as { payload: string | null } | undefined;
		if (row === undefined) return true; // fail-safe: unproven provenance stays blocked
		return row.payload !== "auto";
	}
}

interface RawCardRow {
	id: string;
	title: string;
	status: string;
	assignee: string | null;
	tenant: string | null;
	priority: number;
	claim_lock: string | null;
	claim_expires: number | null;
	consecutive_failures: number;
	max_retries: number | null;
	last_failure_error: string | null;
	started_at: number | null;
	created_at: number;
}

function appendEvent(
	tx: Database.Database,
	cardId: string,
	event: string,
	at: number,
	payload?: string,
): void {
	tx.prepare(
		"INSERT INTO kanban_events (card_id, event, at, payload) VALUES (?, ?, ?, ?)",
	).run(cardId, event, at, payload ?? null);
}

function rowToCard(row: RawCardRow): KanbanCard {
	return {
		id: row.id,
		title: row.title,
		status: row.status as CardStatus,
		assignee: row.assignee,
		tenant: row.tenant,
		priority: row.priority,
		claimLock: row.claim_lock,
		claimExpires: row.claim_expires,
		consecutiveFailures: row.consecutive_failures,
		maxRetries: row.max_retries,
		lastFailureError: row.last_failure_error,
		createdAt: row.created_at,
		startedAt: row.started_at,
	};
}
