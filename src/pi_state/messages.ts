// pi_state/messages.ts — message persistence + byte-exact api_content
// sidecar discipline (02-session-and-state.md §7 "History Storage for
// Cache-Safe Replay").
//
// THE INVARIANT (§7.1): persist-what-you-send. `messages.api_content` stores
// the EXACT bytes sent to the API; replay substitutes the sidecar VERBATIM.
// Nothing in this module sanitizes, strips, trims, or normalizes content —
// cleaning the sidecar would reintroduce the very divergence it exists to
// remove (hermes_state.py:_rows_to_conversation returns it verbatim for
// precisely that reason). DEC-007 ratified the column shape.
//
// Hermes anchors (READ-ONLY reference):
//   hermes_state.py:add_message / get_messages        → insertMessage / listMessages
//   hermes_state.py:set_latest_user_api_content       → setLatestUserApiContent (crash-resilient backfill)
//   hermes_state.py:_scrub_surrogates                 → scrubSurrogates (driver-boundary mapping, made explicit)
//
// Strict role alternation is NEVER repaired here — repair runs pre-request in
// the agent layer on live history AND wire copy, never at persist time
// (DEC-015). Persistence stores bytes.

import type Database from "better-sqlite3";

/** Fixed replay projection — includes the sidecar column (02 §7.3 step 2). */
const MESSAGE_PROJECTION = [
	"id",
	"session_id",
	"role",
	"content",
	"api_content",
	"tool_call_id",
	"tool_calls",
	"tool_name",
	"effect_disposition",
	"finish_reason",
	"token_count",
	"reasoning",
	"reasoning_content",
	"reasoning_details",
	"codex_reasoning_items",
	"codex_message_items",
	"platform_message_id",
	"observed",
	"active",
	"compacted",
	"timestamp",
	"display_kind",
	"display_metadata",
] as const;

export interface MessageRow {
	id: number;
	session_id: string;
	role: string;
	content: string | null;
	api_content: string | null;
	tool_call_id: string | null;
	tool_calls: string | null;
	tool_name: string | null;
	effect_disposition: string | null;
	finish_reason: string | null;
	token_count: number | null;
	reasoning: string | null;
	reasoning_content: string | null;
	reasoning_details: string | null;
	codex_reasoning_items: string | null;
	codex_message_items: string | null;
	platform_message_id: string | null;
	observed: number;
	active: number;
	compacted: number;
	timestamp: number;
	display_kind: string | null;
	display_metadata: string | null;
}

export interface NewMessage {
	sessionId: string;
	role: string;
	/** Clean display/persisted form. */
	content?: string;
	/**
	 * EXACT bytes sent to the API when they differ from display content.
	 * Stored verbatim — never normalized anywhere in the persist path (§7.1).
	 */
	apiContent?: string;
	toolCallId?: string;
	toolCalls?: string;
	toolName?: string;
	effectDisposition?: string;
	finishReason?: string;
	tokenCount?: number;
	reasoning?: string;
	reasoningContent?: string;
	reasoningDetails?: string;
	codexReasoningItems?: string;
	codexMessageItems?: string;
	platformMessageId?: string;
	observed?: boolean;
	active?: boolean;
	compacted?: boolean;
	/** Wall-clock seconds; defaults to Date.now()/1000. */
	timestamp?: number;
	displayKind?: string;
	displayMetadata?: string;
}

/**
 * JS strings may carry unpaired surrogates that SQLite's UTF-16→UTF-8 encoder
 * maps each to U+FFFD (WTF-8 is rejected). Hermes scrubs them explicitly at
 * the same boundary (_scrub_surrogates); porting the explicit scrub makes the
 * round-trip contract deterministic instead of driver-incidental.
 */
export function scrubSurrogates(text: string): string {
	let out = "";
	let i = 0;
	while (i < text.length) {
		const code = text.charCodeAt(i);
		const isHigh = code >= 0xd800 && code <= 0xdbff;
		const isLow = code >= 0xdc00 && code <= 0xdfff;
		if (isHigh && i + 1 < text.length) {
			const next = text.charCodeAt(i + 1);
			if (next >= 0xdc00 && next <= 0xdfff) {
				out += text.slice(i, i + 2); // paired surrogate pair survives intact
				i += 2;
				continue;
			}
		}
		if (isHigh || isLow) {
			out += "\uFFFD"; // lone surrogate → documented replacement form
			i += 1;
			continue;
		}
		const ch = String.fromCharCode(code);
		out += ch;
		i += 1;
	}
	return out;
}

/**
 * Insert one message row inside an EXISTING transaction (compose multiple
 * statements with other writes). apiContent binds as-is — better-sqlite3
 * converts UTF-16→UTF-8 WITHOUT normalization, which is exactly what the
 * sidecar invariant requires (byte-exactness proven by round-trip tests over
 * astral/combining/ZWJ/RTL/CJK/NUL-bearing corpora).
 *
 * Returns the new rowid.
 */
export function insertMessageInTx(
	conn: Database.Database,
	m: NewMessage,
): number {
	const info = conn
		.prepare(
			`INSERT INTO messages (
			   session_id, role, content, api_content,
			   tool_call_id, tool_calls, tool_name,
			   effect_disposition, finish_reason, token_count,
			   reasoning, reasoning_content, reasoning_details,
			   codex_reasoning_items, codex_message_items,
			   platform_message_id, observed, active, compacted,
			   timestamp, display_kind, display_metadata
			 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		)
		.run(
			m.sessionId,
			m.role,
			m.content ?? null,
			m.apiContent === undefined ? null : m.apiContent,
			m.toolCallId ?? null,
			m.toolCalls ?? null,
			m.toolName ?? null,
			m.effectDisposition ?? null,
			m.finishReason ?? null,
			m.tokenCount === undefined ? null : m.tokenCount,
			m.reasoning ?? null,
			m.reasoningContent ?? null,
			m.reasoningDetails ?? null,
			m.codexReasoningItems ?? null,
			m.codexMessageItems ?? null,
			m.platformMessageId ?? null,
			m.observed === true ? 1 : 0,
			m.active === false ? 0 : 1,
			m.compacted === true ? 1 : 0,
			m.timestamp ?? Date.now() / 1000,
			m.displayKind ?? null,
			m.displayMetadata ?? null,
		);
	return Number(info.lastInsertRowid);
}

/** Read one row by id through the fixed projection. */
export function getMessageRow(
	db: Database.Database,
	id: number,
	includeInactive = false,
): MessageRow | undefined {
	const rows = db
		.prepare(
			`SELECT ${MESSAGE_PROJECTION.join(", ")} FROM messages WHERE id = ?${includeInactive ? "" : " AND active = 1"} LIMIT 1`,
		)
		.all(id) as MessageRow[];
	const row = rows[0];
	return row === undefined ? undefined : normalizeCounts(row);
}

function normalizeCounts(r: MessageRow): MessageRow {
	return {
		...r,
		observed: Number(r.observed),
		active: Number(r.active),
		compacted: Number(r.compacted),
	};
}

/**
 * List messages for a session in insertion order (hermes get_messages shape):
 * default ACTIVE-only; includeCompacted additionally loads rows preserved by
 * compaction (active=0, compacted=1 — durable display history); soft-deleted
 * rewind rows (active=0, compacted=0) need includeInactive.
 */
export function listMessages(
	db: Database.Database,
	sessionId: string,
	opts: { includeInactive?: boolean; includeCompacted?: boolean } = {},
): MessageRow[] {
	let sql = `SELECT ${MESSAGE_PROJECTION.join(", ")} FROM messages WHERE session_id = ?`;
	if (opts.includeInactive !== true) {
		sql +=
			opts.includeCompacted === true
				? " AND (active = 1 OR compacted = 1)"
				: " AND active = 1";
	}
	sql += " ORDER BY id";
	const rows = db.prepare(sql).all(sessionId) as MessageRow[];
	return rows.map(normalizeCounts);
}

/**
 * The exact replay-read path projection (02 §7.3 steps 2–3): fixed projection
 * INCLUDING api_content, over self + compression ancestors, ordered by id
 * (insertion order), so the provider prefix is byte-identical to what previous
 * turns sent ⇒ cache hits. Ancestors contribute ONLY through compression-ended
 * parents (`get_compression_tip` walk semantics, bounded 100 hops). Rows are
 * returned RAW — sidecars verbatim, alternation repair left to the agent layer
 * pre-request (DEC-015), dedupe of duplicated replayed user rows available as
 * the explicit defense strip from §7.3 step 3.
 */
export function readReplayMessages(
	db: Database.Database,
	sessionId: string,
	opts: {
		/** Walk self + compression ancestors (default true, per §7.3). */
		includeAncestors?: boolean;
		/** Drop duplicated replayed user rows (§7.3 include_ancestors defense). */
		dedupeReplayedUserRows?: boolean;
	} = {},
): MessageRow[] {
	const ids: string[] = [sessionId];
	if (opts.includeAncestors !== false) {
		const stmt = db.prepare(
			"SELECT id, parent_session_id, end_reason FROM sessions WHERE id = ?",
		);
		let currentId: string | null = sessionId;
		const seen = new Set<string>(ids);
		let hops = 0;
		while (currentId !== null && hops < 100) {
			hops++;
			const row = stmt.get(currentId) as
				| {
						id: string;
						parent_session_id: string | null;
						end_reason: string | null;
				  }
				| undefined;
			if (!row) break;
			const parent = row.parent_session_id;
			if (!parent || seen.has(parent)) break;
			const parentRow = stmt.get(parent) as
				| { id: string; end_reason: string | null }
				| undefined;
			// Only compression-ended ancestors contribute history (§4.2/§7.3).
			if (!parentRow || parentRow.end_reason !== "compression") break;
			seen.add(parent);
			ids.push(parent);
			currentId = parent;
		}
	}
	const placeholders = ids.map(() => "?").join(", ");
	const rows = db
		.prepare(
			`SELECT ${MESSAGE_PROJECTION.join(", ")} FROM messages
			 WHERE session_id IN (${placeholders}) AND active = 1
			 ORDER BY id`,
		)
		.all(...ids) as MessageRow[];
	const mapped = rows.map(normalizeCounts);
	if (opts.dedupeReplayedUserRows !== true) return mapped;
	// Defense strip: when ancestor segments were cloned into children the same
	// user row can appear twice under DIFFERENT session ids — key on CONTENT
	// across the merged stream, keep the LAST occurrence (highest id).
	const lastIndexOfUser = new Map<string, number>();
	for (let i = mapped.length - 1; i >= 0; i--) {
		const r = mapped[i]!;
		if (r.role !== "user") continue;
		const key = r.content ?? "";
		if (!lastIndexOfUser.has(key)) {
			lastIndexOfUser.set(key, i);
		}
	}
	return mapped.filter((r, i) => {
		if (r.role !== "user") return true;
		const key = r.content ?? "";
		const keep = lastIndexOfUser.get(key);
		return keep === undefined || keep === i ? true : false;
	});
}

/**
 * Read the exact sidecar bytes for one row (or null). This is what replay
 * substitutes into content at every API-bound build site (§7.1 triad).
 */
export function getApiContent(
	db: Database.Database,
	id: number,
): string | null {
	const row = db
		.prepare("SELECT api_content FROM messages WHERE id = ?")
		.get(id) as { api_content: string | null } | undefined;
	return row?.api_content ?? null;
}

/**
 * Content-rewrite companion (§7.1 drop_stale_api_content): call at EVERY
 * content-rewrite path (image strip, merge-repairs, redactions). Replaying
 * the pre-rewrite sidecar would resend exactly what the rewrite removed. Cost
 * of dropping: one cache-boundary miss — never wrong content. In-tx form.
 */
export function dropStaleApiContentInTx(
	conn: Database.Database,
	messageId: number,
): void {
	conn
		.prepare("UPDATE messages SET api_content = NULL WHERE id = ?")
		.run(messageId);
}

/**
 * Backfill stamp parity of hermes set_latest_user_api_content: stamps the
 * newest ACTIVE user row of the session. When expectedContent is provided it
 * is a defensive guard — if the newest active user row is not the message the
 * caller stamped (racing rewrite, unexpected tail shape), NOTHING is written.
 * Returns rows updated (0 or 1). Crash-resilient ordering: the user row is
 * written once, last, with its final sidecar (agent/turn_context.py:
 * build_turn_context).
 */
export function setLatestUserApiContent(
	conn: Database.Database,
	sessionId: string,
	apiContent: string,
	expectedContent?: string,
): number {
	const guarded =
		expectedContent === undefined
			? conn.prepare(
					`UPDATE messages SET api_content = ? WHERE id = (
					   SELECT id FROM messages
					   WHERE session_id = ? AND role = 'user' AND active = 1
					   ORDER BY id DESC LIMIT 1
					 )`,
				)
			: conn.prepare(
					`UPDATE messages SET api_content = ? WHERE id = (
					   SELECT id FROM messages
					   WHERE session_id = ? AND role = 'user' AND active = 1
					   ORDER BY id DESC LIMIT 1
					 ) AND content IS ?`,
				);
	const info =
		expectedContent === undefined
			? guarded.run(apiContent, sessionId)
			: guarded.run(apiContent, sessionId, expectedContent);
	return Number(info.changes);
}

/**
 * Substitute the sidecar into the display field for replay builds (§7.1
 * substitute_api_content): returns content-with-sidecar-overwritten. Pure
 * function — the DB is untouched; this exists so every replay build site can
 * share ONE implementation instead of re-deriving the rule.
 */
export function substituteApiContent(row: {
	content: string | null;
	api_content: string | null;
}): string | null {
	return row.api_content ?? row.content;
}
