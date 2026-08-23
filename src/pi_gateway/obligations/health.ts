// health.ts — undelivered-obligation health snapshot for future /status
// consumption. Pure query + shape; no CLI, no I/O beyond the open DB handle.
//
// Spec anchor: 09-open-questions.md Q16 — "undelivered obligations surface
// in /status health"; 08-operations.md carries the table but owns no CLI yet.

import type Database from "better-sqlite3";

import {
	MAX_ATTEMPTS,
	MAX_ROWS,
	STALE_AFTER_SECONDS,
	nextRetryDelaySeconds,
	type ObligationState,
} from "./ledger.js";

export interface ObligationsHealth {
	/** Total rows currently stored (cap is MAX_ROWS=500). */
	total: number;
	/** Counts per state, all five keys always present. */
	byState: Record<ObligationState, number>;
	/** pending + attempting + failed — anything a send could still be owed for. */
	undelivered: number;
	/** Age of the oldest undelivered row (created_at basis); null when none. */
	oldestUndeliveredAgeSeconds: number | null;
	/** Undelivered rows past the 24h stale window (poison candidates). */
	staleUndelivered: number;
	/** Undelivered rows that have exhausted the 3-attempt budget. */
	exhaustedUndelivered: number;
	/**
	 * Seconds until the SOONEST backoff-gated retry becomes due (min over
	 * pending/failed rows), null when nothing is scheduled or everything is
	 * already due (value 0).
	 */
	nextRetryInSeconds: number | null;
	capacity: { maxRows: number; utilization: number };
}

interface CountRow {
	state: string;
	n: number;
}

interface ExtremesRow {
	oldest_created_at: number | null;
	stale_n: number;
	exhausted_n: number;
}

/**
 * Snapshot at `nowSeconds` (caller-supplied or clock-driven by the ledger
 * wrapper). One pass, two queries, zero writes.
 */
export function obligationHealthSnapshot(
	db: Database.Database,
	nowSeconds: number,
): ObligationsHealth {
	const counts = db
		.prepare(
			"SELECT state, COUNT(*) AS n FROM delivery_obligations GROUP BY state",
		)
		.all() as unknown as CountRow[];

	const byState: Record<ObligationState, number> = {
		pending: 0,
		attempting: 0,
		delivered: 0,
		failed: 0,
		abandoned: 0,
	};
	let total = 0;
	for (const c of counts) {
		if (Object.hasOwn(byState, c.state)) {
			byState[c.state as ObligationState] = c.n;
		}
		total += c.n;
	}
	const undelivered = byState.pending + byState.attempting + byState.failed;

	const extremes = db
		.prepare(
			`SELECT MIN(created_at) AS oldest_created_at,
			        SUM(CASE WHEN created_at < ? THEN 1 ELSE 0 END) AS stale_n,
			        SUM(CASE WHEN attempts >= ? THEN 1 ELSE 0 END) AS exhausted_n
			 FROM delivery_obligations
			 WHERE state IN ('pending', 'attempting', 'failed')`,
		)
		.get(nowSeconds - STALE_AFTER_SECONDS, MAX_ATTEMPTS) as unknown as
		| ExtremesRow
		| undefined;

	const oldestAge =
		extremes?.oldest_created_at != null
			? Math.max(0, nowSeconds - extremes.oldest_created_at)
			: null;

	const nextRetryIn = soonestNextRetry(db, nowSeconds);

	return {
		total,
		byState,
		undelivered,
		oldestUndeliveredAgeSeconds: oldestAge,
		staleUndelivered: extremes?.stale_n ?? 0,
		exhaustedUndelivered: extremes?.exhausted_n ?? 0,
		nextRetryInSeconds: nextRetryIn,
		capacity: { maxRows: MAX_ROWS, utilization: total / MAX_ROWS },
	};
}

/** Seconds until the soonest backoff-gated retry comes due; null when none scheduled. */
function soonestNextRetry(db: Database.Database, now: number): number | null {
	// Per-row delay depends on attempts; evaluate the schedule over the
	// (cap-bounded ≤500) candidate set in JS.
	const rows = db
		.prepare(
			`SELECT updated_at, attempts FROM delivery_obligations
				 WHERE state IN ('pending', 'failed') AND attempts < ?`,
		)
		.all(MAX_ATTEMPTS) as unknown as Array<{
		updated_at: number;
		attempts: number;
	}>;
	let soonest: number | null = null;
	for (const r of rows) {
		const dueAt = r.updated_at + nextRetryDelaySeconds(r.attempts);
		if (soonest === null || dueAt < soonest) soonest = dueAt;
	}
	if (soonest === null) return null;
	return Math.max(0, soonest - now);
}
