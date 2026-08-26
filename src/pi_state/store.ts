// pi_state/store.ts — the StateStore facade: one open, one connection,
// initialized schema, and every substrate service attached.
//
// Spec: /root/pi-gateway/02-session-and-state.md §1 (the store), §3
// (reconcile at open; read-only opens never DDL), 01-architecture.md §5.1
// (substrate posture). Dependency layer: pi_state depends only on pi_home +
// better-sqlite3 (01 §5.3) — nothing here imports upward.

import type Database from "better-sqlite3";

import { DbTurnLeaseStore, type TurnLeaseStoreOptions } from "./leases.js";
import {
	dropStaleApiContentInTx,
	getApiContent,
	getMessageRow,
	insertMessageInTx,
	listMessages,
	readReplayMessages,
	setLatestUserApiContent,
	type MessageRow,
	type NewMessage,
} from "./messages.js";
import {
	assertStoreMatchesSchema,
	connectAndInitWithPatience,
	initStore,
	type InitReport,
	type InitStoreOptions,
} from "./reconcile.js";
import {
	clearLoopRow,
	listActiveLoopRows,
	type LoopState,
	loadLoopRow,
	migrateLoopRowToSession,
	saveLoopRow,
} from "./loops.js";
import {
	applyTelegramTopicMigration,
	bindTelegramTopic,
	type BindTelegramTopicArgs,
	deleteTelegramTopicBinding,
	disableTelegramTopicMode,
	enableTelegramTopicMode,
	getTelegramTopicBinding,
	getTelegramTopicBindingBySession,
	isTelegramSessionLinkedToTopic,
	isTelegramTopicModeEnabled,
	listTelegramTopicBindingsForChat,
	type TelegramTopicBindingRow,
	type TelegramTopicCapabilityFlags,
	type TelegramTopicWriteOptions,
} from "./telegram-topics.js";
import {
	TokenWriter,
	updateTokenCounts,
	type TokenDelta,
	type TokenWriterOptions,
} from "./usage.js";
import {
	executeWrite,
	openDatabase,
	type ExecuteWriteOptions,
	type JournalMode,
} from "./wal.js";

/**
 * Mid-turn activity heartbeat cadence floor (agent/session_activity.py:
 * SESSION_ACTIVITY_HEARTBEAT_MIN_INTERVAL_SECONDS): durable stamps MUST stay
 * ≥60s apart per session — the write path is contended and the heartbeat is
 * an observation-only projection that never justifies extra write pressure.
 */
export const SESSION_ACTIVITY_HEARTBEAT_MIN_INTERVAL_SECONDS = 60;

/** Description budget (agent/session_activity.py:ACTIVITY_DESCRIPTION_MAX). */
export const ACTIVITY_DESCRIPTION_MAX = 120;

/**
 * hermes_state.py:bound_activity_description — clamp free-form activity text
 * to the shared description budget.
 */
export function boundActivityDescription(description?: string | null): string {
	const text = (description ?? "").trim();
	if (text.length <= ACTIVITY_DESCRIPTION_MAX) return text;
	return `${text.slice(0, ACTIVITY_DESCRIPTION_MAX - 1)}…`;
}

export interface StateStoreOptions {
	/** Open read-only: never DDL, never flip journal modes (02 §3). */
	readOnly?: boolean;
	/** Raw `database.journal_mode` operator setting (default wal; invalid fails safe). */
	operatorJournalMode?: unknown;
	requireWal?: boolean;
	busyTimeoutMs?: number;
	/** Whole-open retry patience for the init ladder (default 20s). */
	patienceMs?: number;
	/** Options for the startup reconcile sequence (02 §3). */
	init?: InitStoreOptions;
	/** Lease-store tuning (clocks/PID overrides are mostly test hooks). */
	lease?: TurnLeaseStoreOptions;
	/** Token-writer tuning (monotonic-clock injection is a test hook). */
	tokens?: TokenWriterOptions;
}

/**
 * One SQLite substrate per profile. Writable opens run the full reconcile
 * sequence under jittered whole-init patience; read-only opens run derived
 * read-probes and refuse behind-schema stores loudly (caller reopens writable
 * to heal — 02 §3).
 */
export class StateStore {
	readonly path: string;
	readonly db: Database.Database;
	readonly journalMode: JournalMode;
	readonly initReport: InitReport | null;
	readonly leases: DbTurnLeaseStore;
	readonly tokens: TokenWriter;

	private constructor(
		path: string,
		db: Database.Database,
		journalMode: JournalMode,
		initReport: InitReport | null,
		opts: StateStoreOptions,
	) {
		this.path = path;
		this.db = db;
		this.journalMode = journalMode;
		this.initReport = initReport;
		this.leases = new DbTurnLeaseStore(db, opts.lease);
		this.tokens = new TokenWriter(db, opts.tokens);
	}

	/** Writable open: WAL ladder + full reconcile with whole-init retry. */
	static async open(
		path: string,
		opts: StateStoreOptions = {},
	): Promise<StateStore> {
		if (opts.readOnly === true) return StateStore.openReadOnly(path);
		const openOpts: {
			path: string;
			busyTimeoutMs?: number;
			operatorJournalMode?: unknown;
			requireWal?: boolean;
		} = { path };
		if (opts.busyTimeoutMs !== undefined) {
			openOpts.busyTimeoutMs = opts.busyTimeoutMs;
		}
		if (opts.operatorJournalMode !== undefined) {
			openOpts.operatorJournalMode = opts.operatorJournalMode;
		}
		if (opts.requireWal !== undefined) openOpts.requireWal = opts.requireWal;

		const initOpts: { patienceMs?: number } = {};
		if (opts.patienceMs !== undefined) initOpts.patienceMs = opts.patienceMs;

		const store = await connectAndInitWithPatience(async () => {
			const opened = await openDatabase(openOpts);
			try {
				const report = initStore(
					opened.db,
					opts.init as InitStoreOptions | undefined,
				);
				return new StateStore(
					path,
					opened.db,
					opened.journalMode,
					report,
					opts,
				);
			} catch (err) {
				opened.db.close();
				throw err;
			}
		}, initOpts);
		return store;
	}

	/**
	 * Read-only open: NEVER DDL. Derived read-probes detect behind-schema
	 * stores (any prepare-time failure ⇒ StoreBehindSchemaError; caller reopens
	 * writable to heal — 02 §3 "Read-only opens never DDL").
	 */
	static async openReadOnly(path: string): Promise<StateStore> {
		const opened = await openDatabase({ path, readOnly: true });
		assertStoreMatchesSchema(opened.db);
		return new StateStore(path, opened.db, opened.journalMode, null, {});
	}

	/**
	 * BEGIN IMMEDIATE + two-band jittered-patience write ladder
	 * (hermes SessionDB._execute_write semantics).
	 */
	withWrite<T>(
		fn: (db: Database.Database) => T,
		opts?: ExecuteWriteOptions,
	): Promise<T> {
		return executeWrite(this.db, fn, opts);
	}

	// -- message / sidecar surface (byte-exact discipline lives in messages.ts)

	/**
	 * hermes_state.py:touch_session_activity — stamp durable mid-turn session
	 * activity (observation-only). Never moves last_activity_at backwards; the
	 * bounded description is written only when the timestamp advances. No-ops
	 * when sessionId is empty or the row does not exist.
	 *
	 * Observation-only write: rides a SHORT sub-second busy budget
	 * (_ACTIVITY_WRITE_PATIENCE_S = 0.5) instead of the full routine write
	 * patience — under contention a heartbeat must not delay the response-
	 * critical path it is merely observing.
	 *
	 * (pi's Phase-1 schema omits last_activity_provenance, so no provenance
	 * column is written — the only label surface is last_activity_description.)
	 */
	async touchSessionActivity(
		sessionId: string,
		opts: { ts?: number; description?: string | null } = {},
	): Promise<void> {
		if (!sessionId) return;
		const when = opts.ts ?? Date.now() / 1000;
		const desc = boundActivityDescription(opts.description);
		await this.withWrite(
			(conn) => {
				conn
					.prepare(
						"UPDATE sessions SET last_activity_at = ?, last_activity_description = ? " +
							"WHERE id = ? AND (last_activity_at IS NULL OR last_activity_at < ?)",
					)
					.run(when, desc, sessionId, when);
			},
			{ patienceMs: 500 },
		);
	}

	appendMessage(m: NewMessage): Promise<number> {
		return this.withWrite((conn) => insertMessageInTx(conn, m));
	}

	getMessage(id: number, includeInactive = false): MessageRow | undefined {
		return getMessageRow(this.db, id, includeInactive);
	}

	listMessages(
		sessionId: string,
		opts: { includeInactive?: boolean; includeCompacted?: boolean } = {},
	): MessageRow[] {
		return listMessages(this.db, sessionId, opts);
	}

	readReplayMessages(
		sessionId: string,
		opts?: Parameters<typeof readReplayMessages>[2],
	): MessageRow[] {
		return readReplayMessages(this.db, sessionId, opts);
	}

	/** Exact sidecar bytes for one row (or null). */
	getApiContent(id: number): string | null {
		return getApiContent(this.db, id);
	}

	/**
	 * Content-rewrite companion: drop the sidecar so replay never resends
	 * removed content (one cache-boundary miss — never wrong content).
	 */
	dropStaleApiContent(messageId: number): Promise<void> {
		return this.withWrite((conn) => {
			dropStaleApiContentInTx(conn, messageId);
		});
	}

	/**
	 * Stamp the newest ACTIVE user row's sidecar (crash-resilient backfill);
	 * returns rows updated (0 or 1).
	 */
	setLatestUserApiContent(
		sessionId: string,
		apiContent: string,
		expectedContent?: string,
	): Promise<number> {
		return this.withWrite((conn) =>
			setLatestUserApiContent(conn, sessionId, apiContent, expectedContent),
		);
	}

	// -- usage accounting (02 §7.2)

	queueTokenCounts(sessionId: string, delta: TokenDelta): void {
		this.tokens.queueTokenCounts(sessionId, delta);
	}

	flushTokenCounts(timeoutMs = 5000): Promise<boolean> {
		return this.tokens.flushTokenCounts(timeoutMs);
	}

	/** Direct synchronous-path apply (rare: callers needing raised errors). */
	updateTokenCounts(sessionId: string, delta: TokenDelta): Promise<void> {
		return updateTokenCounts(this.db, sessionId, delta);
	}

	// -- /loop rows (state_meta `loop:<session_id>`; loops.py persistence half)

	/** Load one session's loop row, or null when absent/unparseable. */
	loadLoop(sessionId: string): LoopState | null {
		return loadLoopRow(this.db, sessionId);
	}

	/** Persist a loop row (no-op for empty session ids). */
	saveLoop(
		sessionId: string,
		state: LoopState,
		opts?: ExecuteWriteOptions,
	): Promise<void> {
		return saveLoopRow(this.db, sessionId, state, opts);
	}

	/** Mark a loop cleared (audit-preserving); false when no live row. */
	clearLoop(sessionId: string, opts?: ExecuteWriteOptions): Promise<boolean> {
		return clearLoopRow(this.db, sessionId, opts);
	}

	/** [(sessionId, state)] for every ACTIVE loop row ([] on DB failure). */
	listActiveLoops(): Array<[string, LoopState]> {
		return listActiveLoopRows(this.db);
	}

	/** Carry a loop onto a continuation session (#33618); best-effort. */
	migrateLoopToSession(
		oldSessionId: string,
		newSessionId: string,
		reason = "",
	): Promise<boolean> {
		return migrateLoopRowToSession(this.db, oldSessionId, newSessionId, reason);
	}

	// -- telegram DM topic bindings (explicit opt-in migration; 02 §2 side tables)
	// SessionDB surface parity: hermes_state.py:apply_telegram_topic_migration
	// family. Startup reconcile NEVER creates these tables — only an explicit
	// enable/bind does; readers degrade to false/[]/undefined until then.

	applyTelegramTopicMigration(opts?: TelegramTopicWriteOptions): Promise<void> {
		return applyTelegramTopicMigration(this.db, opts);
	}

	enableTelegramTopicMode(
		args: { chatId: string; userId: string } & TelegramTopicCapabilityFlags,
		opts?: TelegramTopicWriteOptions,
	): Promise<void> {
		return enableTelegramTopicMode(this.db, args, opts);
	}

	disableTelegramTopicMode(
		args: { chatId: string; clearBindings?: boolean },
		opts?: TelegramTopicWriteOptions,
	): Promise<void> {
		return disableTelegramTopicMode(this.db, args, opts);
	}

	isTelegramTopicModeEnabled(args: {
		chatId: string;
		userId: string;
	}): boolean {
		return isTelegramTopicModeEnabled(this.db, args);
	}

	bindTelegramTopic(
		args: BindTelegramTopicArgs,
		opts?: TelegramTopicWriteOptions,
	): Promise<void> {
		return bindTelegramTopic(this.db, args, opts);
	}

	getTelegramTopicBinding(args: {
		chatId: string;
		threadId: string;
	}): TelegramTopicBindingRow | undefined {
		return getTelegramTopicBinding(this.db, args);
	}

	listTelegramTopicBindingsForChat(args: {
		chatId: string;
	}): TelegramTopicBindingRow[] {
		return listTelegramTopicBindingsForChat(this.db, args);
	}

	getTelegramTopicBindingBySession(args: {
		sessionId: string;
	}): TelegramTopicBindingRow | undefined {
		return getTelegramTopicBindingBySession(this.db, args);
	}

	isTelegramSessionLinkedToTopic(args: { sessionId: string }): boolean {
		return isTelegramSessionLinkedToTopic(this.db, args);
	}

	deleteTelegramTopicBinding(
		args: { chatId: string; threadId: string },
		opts?: TelegramTopicWriteOptions,
	): Promise<number> {
		return deleteTelegramTopicBinding(this.db, args, opts);
	}

	/** Drain the token writer, then close the connection. */
	async close(drainTokens = true): Promise<void> {
		if (drainTokens) await this.tokens.stop();
		this.db.close();
	}
}
