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
//   hermes_state.py:get_messages_as_conversation      → readReplayMessages (lineage + _is_explicit_branch_session gate)
//   hermes_state.py:_find_duplicate_replayed_user_message
//     + _exact_replayed_user_clone_key                → boundary-respecting replayed-user dedupe
//
// Strict role alternation is NEVER repaired here — repair runs pre-request in
// the agent layer on live history AND wire copy, never at persist time
// (DEC-015). Persistence stores bytes.

import type Database from "better-sqlite3";

import {
	checkTurnLeaseWriteGuardOnConn,
	DEFAULT_TTL_SECONDS,
} from "./leases.js";

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
	/**
	 * Turn-lease ADMISSION (hermes_state.py:_check_transcript_write_guards):
	 * when presented, the write txn first verifies THIS holder still owns the
	 * lineage-root lease — foreign/missing raises SessionTurnLeaseLostError
	 * (fail-fast, never retried), an expired-but-matching lease is renewed in
	 * the same txn. Absent ⇒ no ownership check (parity: appends without a
	 * turn_lease_holder skip the guard).
	 */
	turnLeaseHolder?: string;
	/** Renewal TTL for the admission leg (default 300s, 02 §5). */
	turnLeaseTtlSeconds?: number;
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
 * When `m.turnLeaseHolder` is presented, the transcript-write ADMISSION guard
 * runs FIRST inside this same transaction (hermes append_message ordering:
 * _check_transcript_write_guards precedes the INSERT), so a >TTL-stalled
 * writer whose lease another process reclaimed can NEVER land its flush.
 *
 * Returns the new rowid.
 */
export function insertMessageInTx(
	conn: Database.Database,
	m: NewMessage,
): number {
	if (m.turnLeaseHolder !== undefined && m.turnLeaseHolder !== "") {
		checkTurnLeaseWriteGuardOnConn(conn, m.sessionId, {
			holder: m.turnLeaseHolder,
			ttlSeconds: m.turnLeaseTtlSeconds ?? DEFAULT_TTL_SECONDS,
		});
	}
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
 * hermes_state.py:_is_explicit_branch_session — whether the SELF row is a
 * copied user-facing branch (`_branched_from` marker in sessions.model_config).
 * Branches and compression continuations both use parent_session_id, but a
 * branch OWNS its copied transcript while a compression continuation needs its
 * ended parent's archived rows; presence of the durable marker is the
 * discriminator written by every branch-creation path.
 */
function isExplicitBranchSession(modelConfig: string | null): boolean {
	if (!modelConfig) return false;
	let cfg: unknown;
	try {
		cfg = JSON.parse(modelConfig);
	} catch {
		return false;
	}
	if (typeof cfg !== "object" || cfg === null) return false;
	return Boolean((cfg as Record<string, unknown>)["_branched_from"]);
}

/**
 * hermes_state.py:_canonical_replayed_user_content — canonical live payload of
 * a replayed user row. Hermes splits composite handoff/live carriers via the
 * context compressor; pi rows carry a single content payload today, so the
 * canonical form is the sidecar-substituted content and `is_composite` is
 * always false (the carrier-preference seam in the dedupe below stays wired
 * for when pi grows the compaction scaffold).
 */
function canonicalReplayedUserContent(row: MessageRow): string {
	return substituteApiContent(row) ?? "";
}

/**
 * hermes_state.py:_exact_replayed_user_clone_key — hashable key for a
 * column-exact rotation clone: (timestamp, json-encoded content). Compression
 * rotation copies the ask verbatim INCLUDING its timestamp, so exact clones
 * are rotation artifacts while legitimate repeats happen at later times.
 */
function exactReplayedUserCloneKey(row: MessageRow): string | null {
	if (row.timestamp === null || row.timestamp === undefined) return null;
	const content = canonicalReplayedUserContent(row);
	if (content === "") return null; // parity: None/""/[] never clone-key
	return `${row.timestamp}\u0000${JSON.stringify(content)}`;
}

/**
 * Assistant rows block the backward dedupe scan only when they actually carry
 * a reply payload (parity: `prev.get("content") or prev.get("tool_calls")`).
 * An assistant row with neither must NOT hide an older duplicate behind it.
 */
function assistantCarriesPayload(row: MessageRow): boolean {
	if ((row.content ?? "").length > 0) return true;
	const raw = row.tool_calls;
	if (!raw || raw.trim() === "" || raw.trim() === "[]") return false;
	try {
		const decoded: unknown = JSON.parse(raw);
		return Array.isArray(decoded) && decoded.length > 0;
	} catch {
		return true; // corrupt non-empty blob most likely carried calls
	}
}

/**
 * hermes_state.py:_find_duplicate_replayed_user_message — BACKWARD scan for a
 * duplicate of `msg` among already-kept rows. The scan STOPS at the first
 * assistant message carrying content/tool_calls, so repeated legitimate asks
 * separated by assistant replies survive (the global keep-last filter this
 * replaces dropped them from replayed history entirely). Returns the duplicate
 * index; the caller drops the LATER occurrence (earlier wins) unless the
 * current row is a composite carrier, which pi cannot produce yet.
 */
function findDuplicateReplayedUserMessage(
	kept: MessageRow[],
	msg: MessageRow,
): number | null {
	if (msg.role !== "user") return null;
	const content = canonicalReplayedUserContent(msg);
	if (content === "") return null;
	for (let i = kept.length - 1; i >= 0; i--) {
		const prev = kept[i]!;
		if (
			prev.role === "user" &&
			canonicalReplayedUserContent(prev) === content
		) {
			// Match condition `(prefer_current or prev_is_composite or
			// isinstance(content, str))` reduces to true for pi's plain-string
			// payloads.
			return i;
		}
		if (prev.role === "assistant" && assistantCarriesPayload(prev)) {
			return null; // assistant-boundary stop (hermes anchor above)
		}
	}
	return null;
}

/**
 * The exact replay-read path projection (02 §7.3 steps 2–3): fixed projection
 * INCLUDING api_content, over self + compression ancestors, ordered by id
 * (insertion order), so the provider prefix is byte-identical to what previous
 * turns sent ⇒ cache hits. Ancestors contribute ONLY through compression-ended
 * parents (`get_compression_tip` walk semantics, bounded 100 hops) AND only
 * when the self row is not an explicit `_branched_from` branch — branches own
 * copied transcripts, so walking their lineage would duplicate every ancestor
 * row with the clone (hermes get_messages_as_conversation →
 * _is_explicit_branch_session gate). Rows are returned RAW — sidecars
 * verbatim, alternation repair left to the agent layer pre-request (DEC-015),
 * dedupe of duplicated replayed user rows available as the explicit defense
 * strip from §7.3 step 3 (boundary-respecting backward scan).
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
		// Explicit-branch gate (hermes get_messages_as_conversation): a branched
		// child replays ONLY its copied transcript — never the live parent rows.
		const selfCfg = db
			.prepare("SELECT model_config FROM sessions WHERE id = ?")
			.get(sessionId) as { model_config: string | null } | undefined;
		if (!isExplicitBranchSession(selfCfg?.model_config ?? null)) {
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
	// Defense strip (§7.3 include_ancestors defense): when ancestor segments
	// were cloned into children the same user ask can appear twice under
	// DIFFERENT session ids. Dedupe is the hermes boundary-respecting backward
	// scan — NOT a global keep-last-by-content filter, which erased repeated
	// legitimate asks that an assistant reply separates:
	//   1. EXACT rotation clones — identical (timestamp, json-content) key —
	//      keep the LATER row: the child carrier owns the durable row identity
	//      (_exact_replayed_user_clone_key + prefer_current pop).
	//   2. Same-ask duplicates inside the current assistant-free window — the
	//      backward scan stops at the first assistant carrying content or
	//      tool_calls; the EARLIER occurrence survives (_find_duplicate_
	//      replayed_user_message → `continue`). Composite-carrier preference is
	//      wired but inert until pi grows split carriers (see
	//      canonicalReplayedUserContent).
	const kept: MessageRow[] = [];
	/** clone key → exact row object kept in `kept` (identity-scanned on hit). */
	const exactClones = new Map<string, MessageRow>();
	for (const row of mapped) {
		let cloneKey: string | null = null;
		if (row.role === "user") {
			cloneKey = exactReplayedUserCloneKey(row);
			let duplicateIndex: number | null = null;
			let preferCurrent = false;
			const prevExact =
				cloneKey === null ? undefined : exactClones.get(cloneKey);
			if (prevExact !== undefined) {
				const idx = kept.indexOf(prevExact);
				if (idx >= 0) {
					duplicateIndex = idx;
					preferCurrent = true; // exact clone ⇒ later carrier wins
				}
			}
			if (duplicateIndex === null) {
				duplicateIndex = findDuplicateReplayedUserMessage(kept, row);
			}
			if (duplicateIndex !== null) {
				if (preferCurrent) {
					kept.splice(duplicateIndex, 1); // pop the ancestor copy
				} else {
					continue; // ordinary window duplicate: earlier occurrence stays
				}
			}
		}
		kept.push(row);
		if (cloneKey !== null) exactClones.set(cloneKey, row);
	}
	return kept;
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
