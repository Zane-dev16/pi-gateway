// notify-store.ts — the kanban NOTIFICATION subscription seam and its
// production SQLite implementation (07 §6 notifier slice).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/kanban_db.py:add_notify_sub              → addSub
//     (idempotent on (task, platform, chat, thread); new subs start
//      "caught up": last_event_id snaps to MAX(task_events.id) so a fresh
//      subscription never replays historical terminal events)
//   hermes_cli/kanban_db.py:list_notify_subs            → listSubs
//   hermes_cli/kanban_db.py:claim_unseen_events_for_sub → claimUnseenEvents
//     (THE atomic claim: BEGIN IMMEDIATE read-then-advance so concurrent
//      watchers serialize on the writer lock and exactly one process claims
//      a given event range; CAS-guarded UPDATE on last_event_id)
//   hermes_cli/kanban_db.py:advance_notify_cursor       → advanceCursor
//   hermes_cli/kanban_db.py:rewind_notify_cursor        → rewindCursor
//     (CAS guard: only rewinds if no later notifier advanced past the claim)
//   hermes_cli/kanban_db.py:remove_notify_sub           → removeSub
//   hermes_cli/kanban_db.py:purge_stale_done_notify_subs→ purgeStaleDoneSubs
//     (GC bounds the "done survives" retention: age measured from the task's
//      most recent event, falling back to card creation; <=0 days disables)

import type Database from "better-sqlite3";

import { executeWrite } from "../../pi_state/wal.js";

/** A gateway source subscribed to terminal events for one card. */
export interface NotifySubscription {
	taskId: string;
	platform: string;
	chatId: string;
	/** Thread routing anchor; empty string when the platform has none. */
	threadId: string;
	/** Delivery cursor: last event seq handed to (or seen by) this sub. */
	lastEventId: number;
}

/** One kanban_events row relevant to delivery (Event parity). */
export interface NotifyEvent {
	id: number;
	taskId: string;
	kind: string;
	/** Parsed JSON payload when the row holds JSON, else null. */
	payload: Record<string, unknown> | null;
	/** Epoch seconds the event was recorded. */
	createdAt: number;
}

/** The card facts rendering needs (get_task parity subset). */
export interface NotifyTaskView {
	id: string;
	title: string;
	status: string;
	assignee: string | null;
	/**
	 * Legacy completion handoff text. The dispatcher-slice schema has no
	 * result column yet, so the production store returns null; the field
	 * stays for render parity (completed falls back to it).
	 */
	result: string | null;
}

/** Result of ONE atomic claim: `(old_cursor, new_cursor, events)` parity. */
export interface ClaimedEvents {
	oldCursor: number;
	newCursor: number;
	events: NotifyEvent[];
}

/** Identity tuple of a subscription (remove/advance targeting parity). */
export interface SubKey {
	taskId: string;
	platform: string;
	chatId: string;
	threadId: string;
}

/**
 * The injectable subscription-store seam. Async methods so the production
 * store can ride pi_state's write ladder while fakes stay trivial (same
 * discipline as BoardClient).
 */
export interface NotifySubStore {
	/** The board this store is pinned to (HARD boundary — never another). */
	readonly board: string;

	listSubs(): Promise<NotifySubscription[]>;

	/**
	 * Atomically claim unseen events for one subscription whose kind is in
	 * `kinds`, advancing the stored cursor to the newest claimed seq INSIDE
	 * the same transaction. Returns null when the subscription vanished;
	 * `{ oldCursor, newCursor: oldCursor, events: [] }` when nothing is new.
	 */
	claimUnseenEvents(
		sub: SubKey,
		kinds: readonly string[],
	): Promise<ClaimedEvents | null>;

	advanceCursor(sub: SubKey, newCursor: number): Promise<void>;

	/**
	 * Undo a claim after delivery failure. CAS-guarded: only rewinds when the
	 * cursor still equals `claimedCursor` (no later notifier advanced it).
	 * Returns whether the rewind applied.
	 */
	rewindCursor(
		sub: SubKey,
		claimedCursor: number,
		oldCursor: number,
	): Promise<boolean>;

	removeSub(sub: SubKey): Promise<boolean>;

	getTask(taskId: string): Promise<NotifyTaskView | null>;

	/**
	 * Delete subscriptions whose task sat in `done` untouched past
	 * `maxAgeDays` (age = most recent event, else card creation).
	 * `maxAgeDays <= 0` disables the sweep. Returns rows deleted.
	 */
	purgeStaleDoneSubs(opts: {
		maxAgeDays: number;
		nowSeconds: number;
	}): Promise<number>;
}

interface RawSubRow {
	task_id: string;
	platform: string;
	chat_id: string;
	thread_id: string;
	last_event_id: number;
}

function rowToSub(row: RawSubRow): NotifySubscription {
	return {
		taskId: row.task_id,
		platform: row.platform,
		chatId: row.chat_id,
		threadId: row.thread_id,
		lastEventId: Number(row.last_event_id),
	};
}

export function subKeyOf(sub: SubKey): string {
	return `${sub.taskId}\u0000${sub.platform}\u0000${sub.chatId}\u0000${sub.threadId}`;
}

interface RawEventRow {
	seq: number;
	card_id: string;
	event: string;
	payload: string | null;
	at: number;
}

function parsePayload(raw: string | null): Record<string, unknown> | null {
	if (raw === null || raw === "") return null;
	try {
		const parsed: unknown = JSON.parse(raw);
		return typeof parsed === "object" && parsed !== null
			? (parsed as Record<string, unknown>)
			: null;
	} catch {
		return null; // plain-string payloads (block provenance etc.) carry no dict
	}
}

export interface SqliteNotifyStoreOptions {
	/** Reserved for future per-board scoping (BoardClient.board parity). */
	board: string;
}

/**
 * Production NotifySubStore over better-sqlite3, sharing the dispatcher's
 * database file: `kanban_notify_subs` joins `kanban_cards`, and delivery
 * reads the SAME `kanban_events` append-only log the board writes. Every
 * mutation is a guarded UPDATE inside BEGIN IMMEDIATE (pi_state executeWrite)
 * so two racers — two connections here, two processes on the file, or the
 * notifier and a CLI verb — get exactly ONE claim per event range.
 */
export class SqliteKanbanNotifyStore implements NotifySubStore {
	readonly board: string;
	private readonly db: Database.Database;

	constructor(db: Database.Database, opts: SqliteNotifyStoreOptions) {
		this.db = db;
		this.board = opts.board;
	}

	/** Create the notifier-slice schema if missing. Idempotent. */
	static ensureSchema(db: Database.Database): void {
		db.exec(`
			CREATE TABLE IF NOT EXISTS kanban_notify_subs (
				task_id       TEXT NOT NULL,
				platform      TEXT NOT NULL,
				chat_id       TEXT NOT NULL,
				thread_id     TEXT NOT NULL DEFAULT '',
				created_at    INTEGER NOT NULL,
				last_event_id INTEGER NOT NULL DEFAULT 0,
				PRIMARY KEY (task_id, platform, chat_id, thread_id)
			);
			CREATE INDEX IF NOT EXISTS idx_kanban_notify_subs_task
				ON kanban_notify_subs(task_id);
		`);
	}

	/**
	 * Register a notification source (test/driver convenience; parity
	 * add_notify_sub): idempotent on the identity tuple, and new rows start
	 * CAUGHT UP — the cursor snaps to the card's newest event so subscribing
	 * never replays history.
	 */
	addSub(input: {
		taskId: string;
		platform: string;
		chatId: string;
		threadId?: string | null;
		createdAt?: number;
	}): void {
		executeWrite(this.db, (tx) => {
			tx.prepare(
				`INSERT OR IGNORE INTO kanban_notify_subs
					(task_id, platform, chat_id, thread_id, created_at, last_event_id)
				 VALUES (?, ?, ?, ?, ?,
					COALESCE((SELECT MAX(seq) FROM kanban_events WHERE card_id = ?), 0))`,
			).run(
				input.taskId,
				input.platform,
				input.chatId,
				input.threadId ?? "",
				input.createdAt ?? Math.floor(Date.now() / 1000),
				input.taskId,
			);
		});
	}

	async listSubs(): Promise<NotifySubscription[]> {
		const rows = this.db
			.prepare("SELECT * FROM kanban_notify_subs ORDER BY rowid ASC")
			.all() as RawSubRow[];
		return rows.map(rowToSub);
	}

	async claimUnseenEvents(
		sub: SubKey,
		kinds: readonly string[],
	): Promise<ClaimedEvents | null> {
		return executeWrite(this.db, (tx) => {
			const row = tx
				.prepare(
					`SELECT last_event_id FROM kanban_notify_subs
					 WHERE task_id = ? AND platform = ? AND chat_id = ? AND thread_id = ?`,
				)
				.get(sub.taskId, sub.platform, sub.chatId, sub.threadId) as
				| { last_event_id: number }
				| undefined;
			if (row === undefined) return null; // subscription vanished
			const oldCursor = Number(row.last_event_id);
			const placeholders = kinds.map(() => "?").join(",");
			const rows = tx
				.prepare(
					`SELECT seq, card_id, event, payload, at FROM kanban_events
					 WHERE card_id = ? AND seq > ? AND event IN (${placeholders})
					 ORDER BY seq ASC`,
				)
				.all(sub.taskId, oldCursor, ...kinds) as RawEventRow[];
			if (rows.length === 0) {
				return { oldCursor, newCursor: oldCursor, events: [] };
			}
			const newCursor = Math.max(oldCursor, Number(rows[rows.length - 1]!.seq));
			// THE CAS: only a watcher that still sees old_cursor advances it —
			// concurrent watchers serialize here and exactly one claims the range.
			const cur = tx
				.prepare(
					`UPDATE kanban_notify_subs SET last_event_id = ?
					 WHERE task_id = ? AND platform = ? AND chat_id = ? AND thread_id = ?
					   AND last_event_id = ?`,
				)
				.run(
					newCursor,
					sub.taskId,
					sub.platform,
					sub.chatId,
					sub.threadId,
					oldCursor,
				);
			if (cur.changes !== 1) {
				// Another racer advanced between our read and write; treat the
				// range as lost (they own delivery now).
				return { oldCursor, newCursor: oldCursor, events: [] };
			}
			return {
				oldCursor,
				newCursor,
				events: rows.map((r) => ({
					id: Number(r.seq),
					taskId: r.card_id,
					kind: r.event,
					payload: parsePayload(r.payload),
					createdAt: Number(r.at),
				})),
			};
		});
	}

	async advanceCursor(sub: SubKey, newCursor: number): Promise<void> {
		executeWrite(this.db, (tx) => {
			tx.prepare(
				`UPDATE kanban_notify_subs SET last_event_id = ?
				 WHERE task_id = ? AND platform = ? AND chat_id = ? AND thread_id = ?`,
			).run(newCursor, sub.taskId, sub.platform, sub.chatId, sub.threadId);
		});
	}

	async rewindCursor(
		sub: SubKey,
		claimedCursor: number,
		oldCursor: number,
	): Promise<boolean> {
		return executeWrite(this.db, (tx) => {
			const cur = tx
				.prepare(
					`UPDATE kanban_notify_subs SET last_event_id = ?
					 WHERE task_id = ? AND platform = ? AND chat_id = ? AND thread_id = ?
					   AND last_event_id = ?`,
				)
				.run(
					oldCursor,
					sub.taskId,
					sub.platform,
					sub.chatId,
					sub.threadId,
					claimedCursor,
				);
			return cur.changes === 1;
		});
	}

	async removeSub(sub: SubKey): Promise<boolean> {
		return executeWrite(this.db, (tx) => {
			const cur = tx
				.prepare(
					`DELETE FROM kanban_notify_subs
					 WHERE task_id = ? AND platform = ? AND chat_id = ? AND thread_id = ?`,
				)
				.run(sub.taskId, sub.platform, sub.chatId, sub.threadId);
			return cur.changes > 0;
		});
	}

	async getTask(taskId: string): Promise<NotifyTaskView | null> {
		const row = this.db
			.prepare(
				"SELECT id, title, status, assignee FROM kanban_cards WHERE id = ?",
			)
			.get(taskId) as
			| { id: string; title: string; status: string; assignee: string | null }
			| undefined;
		if (row === undefined) return null;
		return {
			id: row.id,
			title: row.title,
			status: row.status,
			assignee: row.assignee,
			result: null, // dispatcher-slice schema carries no result column yet
		};
	}

	async purgeStaleDoneSubs(opts: {
		maxAgeDays: number;
		nowSeconds: number;
	}): Promise<number> {
		const days = Number.isFinite(opts.maxAgeDays)
			? Math.trunc(opts.maxAgeDays)
			: DEFAULT_DONE_SUB_RETENTION_DAYS;
		if (days <= 0) return 0; // operator-disabled sweep
		const cutoff = opts.nowSeconds - days * 86400;
		return executeWrite(this.db, (tx) => {
			const cur = tx
				.prepare(
					`DELETE FROM kanban_notify_subs WHERE task_id IN (
						SELECT c.id FROM kanban_cards c
						WHERE c.status = 'done'
						  AND COALESCE(
							(SELECT MAX(e.at) FROM kanban_events e WHERE e.card_id = c.id),
							c.created_at, 0
						  ) < ?
					)`,
				)
				.run(cutoff);
			return cur.changes;
		});
	}
}

/** Parity kanban.done_sub_retention_days default (30; 0 disables). */
export const DEFAULT_DONE_SUB_RETENTION_DAYS = 30;
