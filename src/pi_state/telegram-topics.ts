// pi_state/telegram-topics.ts — the Telegram DM topic-bindings subsystem:
// per-chat topic-mode switch + (chat_id, thread_id) → session binding rows
// with an EXPLICIT, version-gated schema migration walk.
//
// Deliberately NOT part of SCHEMA_SQL/startup reconcile: operators must be
// able to upgrade and keep the old Telegram bot behavior running; side tables
// appear only when the feature opts in (/topic parity). Reads never trigger
// the migration — a missing table means "feature never used here" and every
// reader degrades to empty/false.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_state.py:apply_telegram_topic_migration        → applyTelegramTopicMigration
//   hermes_state.py:enable_telegram_topic_mode            → enableTelegramTopicMode
//   hermes_state.py:disable_telegram_topic_mode           → disableTelegramTopicMode
//   hermes_state.py:is_telegram_topic_mode_enabled        → isTelegramTopicModeEnabled
//   hermes_state.py:bind_telegram_topic                   → bindTelegramTopic
//   hermes_state.py:get_telegram_topic_binding            → getTelegramTopicBinding
//   hermes_state.py:list_telegram_topic_bindings_for_chat → listTelegramTopicBindingsForChat
//   hermes_state.py:get_telegram_topic_binding_by_session → getTelegramTopicBindingBySession
//   hermes_state.py:is_telegram_session_linked_to_topic   → isTelegramSessionLinkedToTopic
//   hermes_state.py:delete_telegram_topic_binding (#31501 prune)
//                                                         → deleteTelegramTopicBinding

import type Database from "better-sqlite3";

import { getMeta, setMeta } from "./reconcile.js";
import { executeWrite, type ExecuteWriteOptions } from "./wal.js";

/**
 * state_meta key gating the v1→v2 bindings rebuild
 * (hermes_state.py:telegram_dm_topic_schema_version).
 */
export const TELEGRAM_TOPIC_SCHEMA_VERSION_KEY =
	"telegram_dm_topic_schema_version";

/** Current topic-mode schema version (hermes: v2 adds ON DELETE CASCADE). */
export const TELEGRAM_TOPIC_SCHEMA_VERSION = 2;

/**
 * Side-table DDL — column-for-column parity of
 * hermes_state.py:apply_telegram_topic_migration. `IF NOT EXISTS` added per
 * the pi idempotent-reconcile idiom; executes inside ONE BEGIN IMMEDIATE so
 * concurrent migrators converge instead of interleaving DDL.
 */
const TELEGRAM_TOPIC_DDL = `
CREATE TABLE IF NOT EXISTS telegram_dm_topic_mode (
    chat_id TEXT PRIMARY KEY,
    user_id TEXT NOT NULL,
    enabled INTEGER NOT NULL DEFAULT 1,
    activated_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    has_topics_enabled INTEGER,
    allows_users_to_create_topics INTEGER,
    capability_checked_at REAL,
    intro_message_id TEXT,
    pinned_message_id TEXT
);

CREATE TABLE IF NOT EXISTS telegram_dm_topic_bindings (
    chat_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_key TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    managed_mode TEXT NOT NULL DEFAULT 'auto',
    linked_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (chat_id, thread_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_telegram_dm_topic_bindings_session
ON telegram_dm_topic_bindings(session_id);

CREATE INDEX IF NOT EXISTS idx_telegram_dm_topic_bindings_user
ON telegram_dm_topic_bindings(user_id, chat_id);
`;

/** Rebuild target for the v1→v2 walk (same shape; the point IS the FK). */
const TELEGRAM_TOPIC_BINDINGS_V2_DDL = `
CREATE TABLE telegram_dm_topic_bindings_new (
    chat_id TEXT NOT NULL,
    thread_id TEXT NOT NULL,
    user_id TEXT NOT NULL,
    session_key TEXT NOT NULL,
    session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
    managed_mode TEXT NOT NULL DEFAULT 'auto',
    linked_at REAL NOT NULL,
    updated_at REAL NOT NULL,
    PRIMARY KEY (chat_id, thread_id)
);
INSERT INTO telegram_dm_topic_bindings_new
    SELECT chat_id, thread_id, user_id, session_key,
           session_id, managed_mode, linked_at, updated_at
    FROM telegram_dm_topic_bindings;
DROP TABLE telegram_dm_topic_bindings;
ALTER TABLE telegram_dm_topic_bindings_new RENAME TO telegram_dm_topic_bindings;
CREATE UNIQUE INDEX idx_telegram_dm_topic_bindings_session
ON telegram_dm_topic_bindings(session_id);
CREATE INDEX idx_telegram_dm_topic_bindings_user
ON telegram_dm_topic_bindings(user_id, chat_id);
`;

/** Row shape of telegram_dm_topic_bindings (column names as declared). */
export interface TelegramTopicBindingRow {
	chat_id: string;
	thread_id: string;
	user_id: string;
	session_key: string;
	session_id: string;
	managed_mode: string;
	linked_at: number;
	updated_at: number;
}

/** Row shape of telegram_dm_topic_mode (column names as declared). */
export interface TelegramTopicModeRow {
	chat_id: string;
	user_id: string;
	enabled: number;
	activated_at: number;
	updated_at: number;
	has_topics_enabled: number | null;
	allows_users_to_create_topics: number | null;
	capability_checked_at: number | null;
	intro_message_id: string | null;
	pinned_message_id: string | null;
}

/** Telegram client forum capabilities recorded at /topic opt-in (hermes Optional[bool]). */
export interface TelegramTopicCapabilityFlags {
	hasTopicsEnabled?: boolean | null | undefined;
	allowsUsersToCreateTopics?: boolean | null | undefined;
}

function toInt(value: boolean | null | undefined): number | null {
	if (value === undefined || value === null) return null;
	return value ? 1 : 0;
}

function errMessage(err: unknown): string {
	if (err instanceof Error) return err.message;
	return String(err);
}

/** sqlite OperationalError analogue for "feature tables absent". */
function isMissingTable(err: unknown): boolean {
	return errMessage(err).toLowerCase().includes("no such table");
}

interface WriteClock {
	/** Injected wall-clock SECONDS (parity of time.time()); default Date.now. */
	nowSeconds?: () => number;
}

/** Write options for the topic subsystem: ladder tuning + injected clock. */
export type TelegramTopicWriteOptions = WriteClock & ExecuteWriteOptions;

function nowSecondsOf(opts?: WriteClock): () => number {
	return opts?.nowSeconds ?? (() => Date.now() / 1000);
}

/**
 * Create/upgrade the topic-mode side tables (explicit opt-in migration —
 * NEVER called from store open/reconcile). Version-gated v1→v2 rebuild:
 * SQLite cannot ALTER a foreign key, so a pre-CASCADE bindings table is
 * rebuilt row-preserving inside the same transaction. Idempotent; safe under
 * concurrent writers (single BEGIN IMMEDIATE + the contended-write ladder).
 */
export function applyTelegramTopicMigration(
	db: Database.Database,
	opts: TelegramTopicWriteOptions = {},
): Promise<void> {
	return executeWrite(
		db,
		(conn) => {
			conn.exec(TELEGRAM_TOPIC_DDL);

			const current = getMeta(conn, TELEGRAM_TOPIC_SCHEMA_VERSION_KEY);
			const currentVersion =
				current !== null && /^\d+$/.test(current)
					? Number.parseInt(current, 10)
					: 0;
			if (currentVersion < TELEGRAM_TOPIC_SCHEMA_VERSION) {
				const fkRows = conn
					.prepare("PRAGMA foreign_key_list('telegram_dm_topic_bindings')")
					.all() as Array<Record<string, unknown>>;
				const needsRebuild = fkRows.some(
					(row) =>
						String(row["table"]) === "sessions" &&
						String(row["on_delete"] ?? "") !== "CASCADE",
				);
				if (needsRebuild) conn.exec(TELEGRAM_TOPIC_BINDINGS_V2_DDL);
			}

			setMeta(
				conn,
				TELEGRAM_TOPIC_SCHEMA_VERSION_KEY,
				String(TELEGRAM_TOPIC_SCHEMA_VERSION),
			);
		},
		opts,
	);
}

/**
 * Enable DM topic mode for one private chat/user. Owns the explicit
 * migration (parity: enable_telegram_topic_mode applies it first), then
 * upserts the mode row keyed on chat_id — re-enabling flips enabled back to 1
 * and refreshes capability stamps without touching existing bindings.
 */
export async function enableTelegramTopicMode(
	db: Database.Database,
	args: {
		chatId: string;
		userId: string;
	} & TelegramTopicCapabilityFlags,
	opts: TelegramTopicWriteOptions = {},
): Promise<void> {
	await applyTelegramTopicMigration(db, opts);
	const now = nowSecondsOf(opts)();
	const chatId = String(args.chatId);
	const userId = String(args.userId);
	const hasTopics = toInt(args.hasTopicsEnabled);
	const allowsCreate = toInt(args.allowsUsersToCreateTopics);
	await executeWrite(
		db,
		(conn) => {
			conn
				.prepare(
					`
                INSERT INTO telegram_dm_topic_mode (
                    chat_id, user_id, enabled, activated_at, updated_at,
                    has_topics_enabled, allows_users_to_create_topics,
                    capability_checked_at
                ) VALUES (?, ?, 1, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    enabled = 1,
                    updated_at = excluded.updated_at,
                    has_topics_enabled = excluded.has_topics_enabled,
                    allows_users_to_create_topics = excluded.allows_users_to_create_topics,
                    capability_checked_at = excluded.capability_checked_at
                `,
				)
				.run(chatId, userId, now, now, hasTopics, allowsCreate, now);
		},
		opts,
	);
}

/**
 * Disable DM topic mode for one private chat. Default clears the chat's
 * binding rows so a later re-enable starts clean (`clearBindings=false`
 * preserves them). Absent tables ⇒ silent no-op (never creates them).
 */
export async function disableTelegramTopicMode(
	db: Database.Database,
	args: { chatId: string; clearBindings?: boolean },
	opts: TelegramTopicWriteOptions = {},
): Promise<void> {
	const now = nowSecondsOf(opts)();
	const clearBindings = args.clearBindings ?? true;
	await executeWrite(
		db,
		(conn) => {
			try {
				conn
					.prepare(
						"UPDATE telegram_dm_topic_mode SET enabled = 0, updated_at = ? WHERE chat_id = ?",
					)
					.run(now, String(args.chatId));
				if (clearBindings) {
					conn
						.prepare("DELETE FROM telegram_dm_topic_bindings WHERE chat_id = ?")
						.run(String(args.chatId));
				}
			} catch (err) {
				if (isMissingTable(err)) return; // nothing to disable
				throw err;
			}
		},
		opts,
	);
}

/**
 * Whether DM topic mode is enabled for this chat/user. Missing tables ⇒
 * false (read-only; never migrates).
 */
export function isTelegramTopicModeEnabled(
	db: Database.Database,
	args: { chatId: string; userId: string },
): boolean {
	try {
		const row = db
			.prepare(
				"SELECT enabled FROM telegram_dm_topic_mode WHERE chat_id = ? AND user_id = ?",
			)
			.get(String(args.chatId), String(args.userId)) as
			| { enabled: number }
			| undefined;
		return row !== undefined && Boolean(row.enabled);
	} catch (err) {
		if (isMissingTable(err)) return false;
		throw err;
	}
}

export interface BindTelegramTopicArgs {
	chatId: string;
	threadId: string;
	userId: string;
	sessionKey: string;
	sessionId: string;
	managedMode?: string | undefined;
}

/**
 * Bind one Telegram DM topic thread to one session. A session may link to at
 * most ONE topic (MVP rule): rebinding the same (chat, thread) is idempotent;
 * linking a session that already lives on a DIFFERENT topic throws
 * ("session is already linked to another Telegram topic"). Upsert keeps the
 * ORIGINAL linked_at on update — only liveness fields move.
 */
export async function bindTelegramTopic(
	db: Database.Database,
	args: BindTelegramTopicArgs,
	opts: TelegramTopicWriteOptions = {},
): Promise<void> {
	await applyTelegramTopicMigration(db, opts);
	const now = nowSecondsOf(opts)();
	const chatId = String(args.chatId);
	const threadId = String(args.threadId);
	const userId = String(args.userId);
	const sessionKey = String(args.sessionKey);
	const sessionId = String(args.sessionId);
	const managedMode =
		args.managedMode === undefined ? "auto" : String(args.managedMode);
	await executeWrite(
		db,
		(conn) => {
			const existing = conn
				.prepare(
					"SELECT chat_id, thread_id FROM telegram_dm_topic_bindings WHERE session_id = ?",
				)
				.get(sessionId) as { chat_id: string; thread_id: string } | undefined;
			if (
				existing !== undefined &&
				(String(existing.chat_id) !== chatId ||
					String(existing.thread_id) !== threadId)
			) {
				throw new Error("session is already linked to another Telegram topic");
			}
			conn
				.prepare(
					`
                INSERT INTO telegram_dm_topic_bindings (
                    chat_id, thread_id, user_id, session_key, session_id,
                    managed_mode, linked_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(chat_id, thread_id) DO UPDATE SET
                    user_id = excluded.user_id,
                    session_key = excluded.session_key,
                    session_id = excluded.session_id,
                    managed_mode = excluded.managed_mode,
                    updated_at = excluded.updated_at
                `,
				)
				.run(
					chatId,
					threadId,
					userId,
					sessionKey,
					sessionId,
					managedMode,
					now,
					now,
				);
		},
		opts,
	);
}

/**
 * The session binding for one (chat, thread) pair, if present. Read-only;
 * missing tables ⇒ undefined.
 */
export function getTelegramTopicBinding(
	db: Database.Database,
	args: { chatId: string; threadId: string },
): TelegramTopicBindingRow | undefined {
	try {
		const row = db
			.prepare(
				"SELECT * FROM telegram_dm_topic_bindings WHERE chat_id = ? AND thread_id = ?",
			)
			.get(String(args.chatId), String(args.threadId));
		return row === undefined
			? undefined
			: (row as unknown as TelegramTopicBindingRow);
	} catch (err) {
		if (isMissingTable(err)) return undefined;
		throw err;
	}
}

/**
 * ALL bindings for one chat, NEWEST first (ORDER BY updated_at DESC) — the
 * recovery walk's source (run.py:_recover_telegram_topic_thread_id scans
 * this for the user's most recent lane). Read-only; missing tables ⇒ []
 * WITHOUT migrating (feature-absent stays feature-absent until opt-in).
 */
export function listTelegramTopicBindingsForChat(
	db: Database.Database,
	args: { chatId: string },
): TelegramTopicBindingRow[] {
	try {
		const rows = db
			.prepare(
				"SELECT * FROM telegram_dm_topic_bindings WHERE chat_id = ? ORDER BY updated_at DESC",
			)
			.all(String(args.chatId));
		return rows as unknown as TelegramTopicBindingRow[];
	} catch (err) {
		if (isMissingTable(err)) return [];
		throw err;
	}
}

/**
 * Reverse lookup by session_id via the UNIQUE session index. Read-only;
 * missing tables ⇒ undefined.
 */
export function getTelegramTopicBindingBySession(
	db: Database.Database,
	args: { sessionId: string },
): TelegramTopicBindingRow | undefined {
	try {
		const row = db
			.prepare("SELECT * FROM telegram_dm_topic_bindings WHERE session_id = ?")
			.get(String(args.sessionId));
		return row === undefined
			? undefined
			: (row as unknown as TelegramTopicBindingRow);
	} catch (err) {
		if (isMissingTable(err)) return undefined;
		throw err;
	}
}

/** True when the session holds any topic binding. Read-only; absent tables ⇒ false. */
export function isTelegramSessionLinkedToTopic(
	db: Database.Database,
	args: { sessionId: string },
): boolean {
	try {
		const row = db
			.prepare(
				"SELECT 1 FROM telegram_dm_topic_bindings WHERE session_id = ? LIMIT 1",
			)
			.get(String(args.sessionId));
		return row !== undefined;
	} catch (err) {
		if (isMissingTable(err)) return false;
		throw err;
	}
}

/**
 * Remove one (chat, thread) binding row — the #31501 prune. When it was the
 * chat's LAST binding, the same transaction also flips the chat's topic mode
 * off: recovery must not keep steering lobby messages at an empty lane set.
 * Returns rows deleted (0 = already absent or tables never migrated; cleanup
 * hot paths never raise).
 */
export async function deleteTelegramTopicBinding(
	db: Database.Database,
	args: { chatId: string; threadId: string },
	opts: TelegramTopicWriteOptions = {},
): Promise<number> {
	let deleted = 0;
	await executeWrite(
		db,
		(conn) => {
			try {
				const res = conn
					.prepare(
						"DELETE FROM telegram_dm_topic_bindings WHERE chat_id = ? AND thread_id = ?",
					)
					.run(String(args.chatId), String(args.threadId));
				deleted = Number(res.changes);
			} catch (err) {
				if (isMissingTable(err)) {
					deleted = 0;
					return;
				}
				throw err;
			}
			if (deleted === 0) return;
			try {
				const remaining = conn
					.prepare(
						"SELECT 1 FROM telegram_dm_topic_bindings WHERE chat_id = ? LIMIT 1",
					)
					.get(String(args.chatId));
				if (remaining === undefined) {
					conn
						.prepare(
							"UPDATE telegram_dm_topic_mode SET enabled = 0, updated_at = ? WHERE chat_id = ?",
						)
						.run(nowSecondsOf(opts)(), String(args.chatId));
				}
			} catch (err) {
				if (isMissingTable(err)) return; // binding prune still stands
				throw err;
			}
		},
		opts,
	);
	return deleted;
}
