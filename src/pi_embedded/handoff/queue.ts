// queue.ts — the DEC-008 handoff row protocol over the `sessions` handoff
// columns (02 §2.1 DDL: handoff_state / handoff_platform / handoff_error).
//
// The CLI is a client like the TUI except for chat handoff TO the gateway:
// it writes its session row `handoff_state='pending'` and poll-blocks on a
// terminal state; THIS queue is the gateway-side mutation surface the watcher
// drives. State machine (hermes_state.py handoff block docstring):
//
//   NULL ──request──▶ pending ──claim──▶ running ──▶ completed | failed(+error)
//   completed/failed ──request──▶ pending          (retry is legal)
//   pending/running  ──request──✗                  (already in flight)
//
// Every mutation is ONE guarded UPDATE inside BEGIN IMMEDIATE
// (pi_state/wal.ts executeWrite — hermes SessionDB._execute_write parity), so
// two ticks or two gateways racing one row get exactly one winner.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_state.py:request_handoff        → requestHandoff
//   hermes_state.py:get_handoff_state      → getHandoffState
//   hermes_state.py:list_pending_handoffs  → listPendingHandoffs (oldest first)
//   hermes_state.py:claim_handoff          → claimHandoff (atomic CAS)
//   hermes_state.py:complete_handoff       → completeHandoff (error := NULL)
//   hermes_state.py:fail_handoff           → failHandoff (error truncated :500)

import type Database from "better-sqlite3";

import { executeWrite } from "../../pi_state/wal.js";
import { systemClock, type GatewayClock } from "./clock.js";

export { systemClock };
export type { GatewayClock };

export type HandoffState = "pending" | "running" | "completed" | "failed";

export interface HandoffStateSnapshot {
	state: HandoffState;
	platform: string | null;
	error: string | null;
}

/**
 * The subset of a pending sessions row the watcher/pipeline consume
 * (`list_pending_handoffs` projection; oldest first by started_at ASC).
 */
export interface HandoffRow {
	id: string;
	source: string | null;
	title: string | null;
	startedAt: number;
	handoffPlatform: string | null;
}

/** fail_handoff stores at most this many error bytes (parity `error[:500]`). */
export const HANDOFF_ERROR_MAX_LENGTH = 500;

interface HandoffQueueOptions {
	clock?: GatewayClock;
}

interface SessionsHandoffRowRaw {
	id: string;
	source: string | null;
	title: string | null;
	started_at: number;
	handoff_platform: string | null;
}

export class HandoffQueue {
	private readonly db: Database.Database;
	private readonly clock: GatewayClock;

	constructor(db: Database.Database, opts: HandoffQueueOptions = {}) {
		this.db = db;
		this.clock = opts.clock ?? systemClock;
	}

	/**
	 * CLI-side stub creation (cli_commands_mixin.py:_handle_handoff_command):
	 * an empty session still needs a row so the gateway has something to
	 * switch_session onto. INSERT OR IGNORE — never clobbers an existing row.
	 * Returns true when THIS call created the row.
	 */
	async ensureSessionRow(
		sessionId: string,
		opts: { source?: string } = {},
	): Promise<boolean> {
		return executeWrite(this.db, (conn) => {
			const r = conn
				.prepare(
					"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, ?, ?)",
				)
				.run(sessionId, opts.source ?? "cli", this.clock.nowSeconds());
			return r.changes > 0;
		});
	}

	/**
	 * Mark a session pending handoff to the given platform. True when the row
	 * was found and not already in flight (NULL/completed/failed ⇒ pending);
	 * false when already pending/running (hermes request_handoff contract).
	 */
	async requestHandoff(sessionId: string, platform: string): Promise<boolean> {
		return executeWrite(this.db, (conn) => {
			const r = conn
				.prepare(
					"UPDATE sessions SET handoff_state = 'pending', " +
						"handoff_platform = ?, handoff_error = NULL " +
						"WHERE id = ? AND (handoff_state IS NULL " +
						"OR handoff_state IN ('completed', 'failed'))",
				)
				.run(platform, sessionId);
			return r.changes > 0;
		});
	}

	/**
	 * Read the current handoff state for a session, or null when the row does
	 * not exist (get_handoff_state parity: missing row ≙ no handoff record).
	 */
	getHandoffState(sessionId: string): HandoffStateSnapshot | null {
		const row = this.db
			.prepare(
				"SELECT handoff_state, handoff_platform, handoff_error " +
					"FROM sessions WHERE id = ?",
			)
			.get(sessionId) as
			| {
					handoff_state: string | null;
					handoff_platform: string | null;
					handoff_error: string | null;
			  }
			| undefined;
		if (!row || row.handoff_state === null) return null;
		return {
			state: row.handoff_state as HandoffState,
			platform: row.handoff_platform,
			error: row.handoff_error,
		};
	}

	/**
	 * All sessions in handoff_state='pending', OLDEST FIRST (started_at ASC).
	 * Used by the watcher's poll tick. A crashed claimer's 'running' rows are
	 * deliberately NOT listed — exactly-once dispatch survives crashes because
	 * a claimed row is invisible until it reaches a terminal state (and the
	 * CLI's timeout path is what recovers it — see two-process crash test).
	 */
	listPendingHandoffs(): HandoffRow[] {
		const rows = this.db
			.prepare(
				"SELECT id, source, title, started_at, handoff_platform " +
					"FROM sessions WHERE handoff_state = 'pending' " +
					"ORDER BY started_at ASC",
			)
			.all() as unknown as SessionsHandoffRowRaw[];
		return rows.map((r) => ({
			id: r.id,
			source: r.source,
			title: r.title,
			startedAt: r.started_at,
			handoffPlatform: r.handoff_platform,
		}));
	}

	/**
	 * Atomically transition pending → running. TRUE only for the winner:
	 * another tick or another gateway loses the CAS harmlessly
	 * (claim_handoff parity — UPDATE ... WHERE handoff_state='pending').
	 */
	async claimHandoff(sessionId: string): Promise<boolean> {
		return executeWrite(this.db, (conn) => {
			const r = conn
				.prepare(
					"UPDATE sessions SET handoff_state = 'running' " +
						"WHERE id = ? AND handoff_state = 'pending'",
				)
				.run(sessionId);
			return r.changes > 0;
		});
	}

	/** Mark a handoff completed; clears any prior error payload. */
	async completeHandoff(sessionId: string): Promise<void> {
		await executeWrite(this.db, (conn) => {
			conn
				.prepare(
					"UPDATE sessions SET handoff_state = 'completed', " +
						"handoff_error = NULL WHERE id = ?",
				)
				.run(sessionId);
		});
	}

	/**
	 * Mark a handoff failed and record the reason (truncated to 500 chars).
	 * UNCONDITIONAL on purpose (no state precondition): this is also the
	 * recovery path that rescues a row stranded at 'running' by a crashed
	 * gateway — the CLI's poll-block deadline calls exactly this
	 * (cli_commands_mixin.py: fail_handoff(..., "timed out waiting ...")).
	 */
	async failHandoff(sessionId: string, error: string): Promise<void> {
		await executeWrite(this.db, (conn) => {
			conn
				.prepare(
					"UPDATE sessions SET handoff_state = 'failed', " +
						"handoff_error = ? WHERE id = ?",
				)
				.run(error.slice(0, HANDOFF_ERROR_MAX_LENGTH), sessionId);
		});
	}
}
