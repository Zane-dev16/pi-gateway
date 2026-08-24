// binder.ts — the switch_session re-bind: the half of DEC-008 that carries
// the transcript-replay guarantee. The destination channel's session_key is
// re-pointed at the CLI's EXISTING session id, so the next turn on that key
// resolves to the CLI session row and its full history replays
// (runner.seedReplay reads pi_state rows for the resolved session).
//
// Identity re-bind + replay through the normal pipeline — NOT side-channel
// event injection (09-open-questions.md Q21/DEC-008 corrected premise C1).
//
// Storage: `gateway_routing` (scope, session_key) → entry_json + the
// `sessions` lifecycle columns. The entry JSON keeps Hermes SessionEntry's
// attribute names verbatim (session_key/session_id/created_at/updated_at/
// origin/display_name/platform/chat_type) so the future full resolution chain
// (02 §4 runtime half) reads these rows without migration.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/session.py:switch_session            → switchSession
//     (missing entry ⇒ None; same-id no-op; fresh created_at on the new
//      entry; predecessor ended 'session_switch'; target reopened)
//   hermes_state.py:promote_to_session_reset     → promoteEndedPredecessor
//     (promotes ONLY live rows or accidental ends agent_close/ws_orphan_reap;
//      explicit boundaries like compression keep their first reason)
//   hermes_state.py:reopen_session               → reopenTarget
//     (clears ended_at/end_reason AFTER stamping markerless legacy reset
//      children with `$._reset_from` — _legacy_reset_child_sql parity)
//
// Divergence (safe strengthening): end/reopen land in the SAME BEGIN
// IMMEDIATE transaction as the entry swap; Hermes performs them as separate
// committed steps after its in-memory swap. A crash between the Hermes steps
// can strand a switched key over an unended predecessor; one transaction
// cannot.

import type Database from "better-sqlite3";

import { executeWrite } from "../../pi_state/wal.js";
import { systemClock, type GatewayClock } from "./clock.js";

export { systemClock };
export type { GatewayClock };

/**
 * Routing entry (Hermes SessionEntry attribute vocabulary, JSON-encoded in
 * gateway_routing.entry_json). Timestamps are epoch SECONDS.
 */
export interface RoutingEntry {
	session_key: string;
	session_id: string;
	created_at: number;
	updated_at: number;
	origin?: string | null;
	display_name?: string | null;
	platform?: string | null;
	chat_type?: string | null;
}

/** Seed facts used when a routing entry must be created fresh. */
export interface RoutingEntrySeed {
	origin?: string | null;
	display_name?: string | null;
	platform?: string | null;
	chat_type?: string | null;
}

/**
 * End reasons whose boundary means "the USER closed this thread of work" —
 * promote_to_session_reset promotes live rows and ACCIDENTAL ends only.
 * Parity: hermes_state.py:promote_to_session_reset's WHERE clause.
 */
const PROMOTABLE_ACCIDENTAL_ENDS = ["agent_close", "ws_orphan_reap"];

/**
 * Reset-boundary reasons for reopen-time child stabilization.
 * Parity: hermes_state_common.py:_RESET_END_REASONS.
 */
const RESET_END_REASONS = [
	"session_reset",
	"session_switch",
	"idle",
	"daily",
	"suspended",
	"resume_pending_expired",
] as const;

interface RoutingBinderOptions {
	clock?: GatewayClock;
}

function parseEntry(
	raw: string | undefined | null,
	key: string,
): RoutingEntry | null {
	if (!raw) return null;
	try {
		const parsed = JSON.parse(raw) as Partial<RoutingEntry>;
		if (typeof parsed.session_id !== "string" || parsed.session_id === "") {
			return null;
		}
		return {
			session_key: key,
			session_id: parsed.session_id,
			created_at: typeof parsed.created_at === "number" ? parsed.created_at : 0,
			updated_at: typeof parsed.updated_at === "number" ? parsed.updated_at : 0,
			origin: parsed.origin ?? null,
			display_name: parsed.display_name ?? null,
			platform: parsed.platform ?? null,
			chat_type: parsed.chat_type ?? null,
		};
	} catch {
		// Corrupt entry_json behaves like a missing entry: loud at the caller
		// (switchSession returns null ⇒ handoff fails with its error payload),
		// never a crash inside the watcher tick.
		return null;
	}
}

/**
 * hermes_state_common.py:_legacy_reset_child_sql — pre-marker reset-
 * continuation heuristic. A child is a legacy reset continuation when it
 * rides its parent's exact non-empty session_key and the parent ended at a
 * reset boundary. Shared by the stabilization UPDATE below so listing and
 * stamping cannot drift (Hermes keeps ONE definition; so do we).
 */
const LEGACY_RESET_CHILD_SQL = `
EXISTS (SELECT 1 FROM sessions p
        WHERE p.id = child.parent_session_id
        AND p.end_reason IN (${RESET_END_REASONS.map((r) => `'${r}'`).join(", ")})
        AND child.session_key IS NOT NULL
        AND child.session_key != ''
        AND child.session_key = p.session_key)`;

export class RoutingBinder {
	private readonly db: Database.Database;
	private readonly clock: GatewayClock;

	constructor(db: Database.Database, opts: RoutingBinderOptions = {}) {
		this.db = db;
		this.clock = opts.clock ?? systemClock;
	}

	/** Read the entry bound to a routing key (null when none/corrupt). */
	entryOf(sessionKey: string, scope = ""): RoutingEntry | null {
		const row = this.db
			.prepare(
				"SELECT entry_json FROM gateway_routing WHERE scope = ? AND session_key = ?",
			)
			.get(scope, sessionKey) as { entry_json: string } | undefined;
		return parseEntry(row?.entry_json, sessionKey);
	}

	/**
	 * get_or_create_session seeding step of the handoff path (run.py
	 * _process_handoff calls get_or_create_session(dest_source) BEFORE
	 * switch_session so a never-used home channel has an entry to re-point).
	 * INSERT OR IGNORE under BEGIN IMMEDIATE; returns the surviving entry.
	 */
	async ensureEntry(
		sessionKey: string,
		seed: RoutingEntrySeed = {},
		scope = "",
	): Promise<RoutingEntry> {
		return executeWrite(this.db, (conn) => {
			const existing = conn
				.prepare(
					"SELECT entry_json FROM gateway_routing WHERE scope = ? AND session_key = ?",
				)
				.get(scope, sessionKey) as { entry_json: string } | undefined;
			const entry = parseEntry(existing?.entry_json, sessionKey);
			if (entry !== null) return entry;

			const now = this.clock.nowSeconds();
			const fresh: RoutingEntry = {
				session_key: sessionKey,
				session_id: `pending-${now}-${Math.floor(Math.random() * 1e9)}`,
				created_at: now,
				updated_at: now,
				origin: seed.origin ?? null,
				display_name: seed.display_name ?? null,
				platform: seed.platform ?? null,
				chat_type: seed.chat_type ?? null,
			};
			conn
				.prepare(
					"INSERT OR IGNORE INTO gateway_routing (scope, session_key, entry_json, updated_at) VALUES (?, ?, ?, ?)",
				)
				.run(scope, sessionKey, JSON.stringify(fresh), now);
			// INSERT OR IGNORE raced a sibling winner — adopt THEIRS (publish-
			// if-absent semantics, 02 §4.1 loser adopts winner's entry).
			const after = conn
				.prepare(
					"SELECT entry_json FROM gateway_routing WHERE scope = ? AND session_key = ?",
				)
				.get(scope, sessionKey) as { entry_json: string } | undefined;
			const adopted = parseEntry(after?.entry_json, sessionKey);
			return adopted ?? fresh;
		});
	}

	/**
	 * Switch a session key to point at an existing session id. Used by the
	 * handoff watcher exactly like /resume uses it in Hermes: ends the prior
	 * session in SQLite (like reset) but REUSES target_session_id so the old
	 * transcript loads on the next message. Returns the new entry, or NULL
	 * when no entry exists for the key (caller fails the handoff loudly).
	 */
	async switchSession(
		sessionKey: string,
		targetSessionId: string,
		scope = "",
	): Promise<RoutingEntry | null> {
		return executeWrite(this.db, (conn) => {
			const now = this.clock.nowSeconds();

			const row = conn
				.prepare(
					"SELECT entry_json FROM gateway_routing WHERE scope = ? AND session_key = ?",
				)
				.get(scope, sessionKey) as { entry_json: string } | undefined;
			const oldEntry = parseEntry(row?.entry_json, sessionKey);
			if (oldEntry === null) return null;

			// Don't switch if already on that session (switch_session parity).
			if (oldEntry.session_id === targetSessionId) return oldEntry;

			const newEntry: RoutingEntry = {
				session_key: oldEntry.session_key,
				session_id: targetSessionId,
				created_at: now,
				updated_at: now,
				origin: oldEntry.origin ?? null,
				display_name: oldEntry.display_name ?? null,
				platform: oldEntry.platform ?? null,
				chat_type: oldEntry.chat_type ?? null,
			};

			// Entry swap (fresh created_at/updated_at, identity carried over).
			conn
				.prepare(
					"UPDATE gateway_routing SET entry_json = ?, updated_at = ? " +
						"WHERE scope = ? AND session_key = ?",
				)
				.run(JSON.stringify(newEntry), now, scope, sessionKey);

			this.promoteEndedPredecessor(conn, oldEntry.session_id, now);
			this.reopenTarget(conn, targetSessionId);
			return newEntry;
		});
	}

	// ------------------------------------------------------------------
	// sessions-row lifecycle arms of switch_session (single txn with swap)
	// ------------------------------------------------------------------

	/**
	 * hermes_state.py:promote_to_session_reset — durably mark the PREDECESSOR
	 * session ended by the explicit 'session_switch' boundary. Promotes ONLY
	 * live rows or accidental ends (agent_close / ws_orphan_reap); a stale
	 * agent_close must not leave the outgoing session recoverable (#61220
	 * class). Explicit boundaries keep first-reason-wins.
	 */
	private promoteEndedPredecessor(
		conn: Database.Database,
		predecessorId: string,
		now: number,
	): void {
		conn
			.prepare(
				`UPDATE sessions SET ended_at = ?, end_reason = 'session_switch'
			 WHERE id = ? AND (ended_at IS NULL OR end_reason IN ('agent_close', 'ws_orphan_reap'))`,
			)
			.run(now, predecessorId);
	}

	/**
	 * hermes_state.py:reopen_session — clear ended_at/end_reason so the CLI
	 * session resumes, BUT first stabilize markerless legacy reset children
	 * (stamp `$._reset_from`) so lineage walks survive the cleared boundary.
	 */
	private reopenTarget(conn: Database.Database, sessionId: string): void {
		conn
			.prepare(
				`UPDATE sessions AS child SET model_config = json_set(
			   COALESCE(child.model_config, '{}'), '$._reset_from',
			   child.parent_session_id)
			 WHERE child.parent_session_id = ?
			 AND json_extract(COALESCE(child.model_config, '{}'),
			                  '$._reset_from') IS NULL
			 AND ${LEGACY_RESET_CHILD_SQL}`,
			)
			.run(sessionId);
		conn
			.prepare(
				"UPDATE sessions SET ended_at = NULL, end_reason = NULL WHERE id = ?",
			)
			.run(sessionId);
	}
}
