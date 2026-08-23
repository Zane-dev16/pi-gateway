// pi_gateway/resolution/compression-tip.ts — compression LIVE-TIP walk
// (02-session-and-state.md §4.2, port of hermes_state.py:get_compression_tip).
//
// A compression continuation is a child of a session whose
// end_reason='compression'. Older Hermes builds required
// child.started_at >= parent.ended_at; that ordering is too brittle — races
// insert the real continuation before the parent's ended_at lands while a
// stale websocket reuses a sibling that DOES satisfy the timestamp test, and
// resume follows the wrong sibling ("lost messages"). So: follow ONLY children
// of compression-ended parents, exclude explicit branch/delegate/tool
// children, prefer continuing/live children over stale closed siblings, and
// never trust timestamps.
//
// The gateway heals stale routing entries on read with this tip
// (hermes gateway/session.py:_heal_compression_tip_locked).

import type Database from "better-sqlite3";

/** Bounded walk — compression chains this deep are pathological (parity 100). */
const MAX_TIP_HOPS = 100;

/**
 * Walk the compression-continuation chain forward from `sessionId` and return
 * the tip (the input id when no continuation exists). Read-only; cycle- and
 * depth-bounded. Exclusion predicates match hermes_state.py:get_compression_tip:
 * `$._branched_from` / `$._delegate_from` markers in child.model_config and
 * source='tool' children are NOT continuations; ordering prefers
 * end_reason='compression' children, then still-live children, then stale
 * closed siblings by last activity.
 */
export function getCompressionTip(
	db: Database.Database,
	sessionId: string,
): string {
	if (!sessionId) return sessionId;
	const stmt = db.prepare(`
		SELECT child.id
		FROM sessions parent
		JOIN sessions child ON child.parent_session_id = parent.id
		WHERE parent.id = ?
		  AND parent.end_reason = 'compression'
		  AND json_extract(COALESCE(child.model_config, '{}'), '$._branched_from') IS NULL
		  AND json_extract(COALESCE(child.model_config, '{}'), '$._delegate_from') IS NULL
		  AND COALESCE(child.source, '') != 'tool'
		ORDER BY
		  CASE
		    WHEN child.end_reason = 'compression' THEN 0
		    WHEN child.ended_at IS NULL THEN 1
		    ELSE 2
		  END,
		  COALESCE(child.last_activity_at, child.started_at) DESC,
		  child.started_at DESC,
		  child.id DESC
		LIMIT 1
	`);
	let current = sessionId;
	const seen = new Set<string>([current]);
	for (let hop = 0; hop < MAX_TIP_HOPS; hop++) {
		const row = stmt.get(current) as { id: string } | undefined;
		if (!row) return current;
		const childId = String(row.id);
		if (!childId || seen.has(childId)) return current;
		seen.add(childId);
		current = childId;
	}
	return current;
}
