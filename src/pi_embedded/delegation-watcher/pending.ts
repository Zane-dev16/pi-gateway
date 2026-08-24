// pending.ts — read-only view of the durable completion backlog.
//
// The rail store (pi_gateway/delegation/rail.ts) owns every MUTATION of
// async_delegations; this module only SELECTs the rows the watcher drains:
// delivery_state='pending' with a persisted event payload, oldest first
// (completed_at ASC, delegation_id ASC tiebreak) so a fan-out batch replays
// in dispatch order. Hermes drained the equivalent set from the shared
// completion queue after restore_undelivered_completions re-enqueued it;
// here the durable row IS the queue, so the poll is a plain read and the
// claim handshake (rail.claimCompletion) is what arbitrates consumers.
//
// Reading (not mutating) another module's spec'd table keeps Phase-4 code
// untouched: the DDL is normative (02 §2.1 `async_delegations`).

import type Database from "better-sqlite3";

/** One drainable pending completion: the row identity plus its event. */
export interface PendingCompletion {
	delegationId: string;
	/** session_key of the dispatching conversation — the ROUTING KEY. */
	originSession: string;
	parentSessionId: string | null;
	/** Persisted forged-inbound event payload (event_json), parsed. */
	event: Record<string, unknown>;
	completedAt: number | null;
	dispatchedAt: number;
}

interface PendingRowRaw {
	delegation_id: string;
	origin_session: string;
	parent_session_id: string | null;
	event_json: string | null;
	completed_at: number | null;
	dispatched_at: number;
}

/**
 * All completions awaiting delivery, OLDEST FIRST. Rows without an event
 * payload are not replayable (nothing to forge a turn from) and stay
 * invisible to the watcher — the rail's restore/prune paths converge them.
 */
export function listPendingCompletions(
	db: Database.Database,
): PendingCompletion[] {
	const rows = db
		.prepare(
			`SELECT delegation_id, origin_session, parent_session_id,
			        event_json, completed_at, dispatched_at
			 FROM async_delegations
			 WHERE delivery_state = 'pending' AND event_json IS NOT NULL
			 ORDER BY completed_at ASC, delegation_id ASC`,
		)
		.all() as unknown as PendingRowRaw[];
	const out: PendingCompletion[] = [];
	for (const r of rows) {
		let event: Record<string, unknown> = {};
		try {
			const parsed: unknown = JSON.parse(r.event_json ?? "{}");
			if (parsed !== null && typeof parsed === "object") {
				event = parsed as Record<string, unknown>;
			}
		} catch {
			continue; // unparsable payload is not replayable
		}
		out.push({
			delegationId: r.delegation_id,
			originSession: r.origin_session,
			parentSessionId: r.parent_session_id,
			event,
			completedAt: r.completed_at,
			dispatchedAt: r.dispatched_at,
		});
	}
	return out;
}
