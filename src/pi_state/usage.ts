// pi_state/usage.ts — token/cost accounting write path: a coalescing
// background writer that NEVER blocks a turn (02-session-and-state.md §7.2).
//
// Measured Hermes motivation for the async design: synchronous UPDATE
// statements stalled the turn thread p50 3.3ms / p95 70ms per API call on
// multi-GB DBs.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_state.py:queue_token_counts      → TokenWriter.queueTokenCounts (append + notify; lazy worker spawn)
//   hermes_state.py:_token_writer_loop      → worker loop (swap queue→batch, busy set BEFORE clear, idle retirement)
//   hermes_state.py:_coalesce_token_deltas  → coalesceTokenDeltas (ADJACENT same-route only; None-preserving costs)
//   hermes_state.py:flush_token_counts      → flushTokenCounts (barrier; caller-drain claims busy like the writer)
//   hermes_state.py:_stop_token_writer      → stop (drain + synchronous leftover apply)
//   hermes_state.py:update_token_counts     → updateTokenCounts (incremental vs absolute SQL, first-accounted-route stamp)
//   hermes_state.py:_record_model_usage     → recordModelUsage (session_model_usage upsert keyed session × route × task)
//
// DEC-011: usage lands ONLY in sessions + session_model_usage; daily/per-period
// spend is derived by query over first_seen/last_seen — no rollup table.

import type Database from "better-sqlite3";

import { executeWrite } from "./wal.js";

/** Route fields that participate in the coalescing key (hermes _TOKEN_DELTA_ROUTE_FIELDS). */
export const TOKEN_DELTA_ROUTE_FIELDS = [
	"model",
	"costStatus",
	"costSource",
	"pricingVersion",
	"billingProvider",
	"billingBaseUrl",
	"billingMode",
] as const;

/** Sum fields merged when adjacent deltas share a route (hermes _TOKEN_DELTA_SUM_FIELDS). */
export const TOKEN_DELTA_SUM_FIELDS = [
	"inputTokens",
	"outputTokens",
	"cacheReadTokens",
	"cacheWriteTokens",
	"reasoningTokens",
	"apiCallCount",
] as const;

/** Cost fields summed None-preserving (hermes _TOKEN_DELTA_COST_FIELDS). */
export const TOKEN_DELTA_COST_FIELDS = [
	"estimatedCostUsd",
	"actualCostUsd",
] as const;

export interface TokenDelta {
	inputTokens?: number;
	outputTokens?: number;
	cacheReadTokens?: number;
	cacheWriteTokens?: number;
	reasoningTokens?: number;
	apiCallCount?: number;
	estimatedCostUsd?: number | null;
	actualCostUsd?: number | null;
	model?: string;
	costStatus?: string;
	costSource?: string;
	pricingVersion?: string;
	billingProvider?: string;
	billingBaseUrl?: string;
	billingMode?: string;
	/**
	 * Distinguishes what consumed the tokens ('' = main agent loop; auxiliary
	 * calls record 'vision'/'compression'/… — hermes issue #23270).
	 */
	task?: string;
	/**
	 * false (default): values are INCREMENTED (per-API-call deltas).
	 * true: values are SET directly (caller already holds cumulative totals).
	 * Absolute deltas never merge in the coalescer.
	 */
	absolute?: boolean;
}

export interface QueuedDelta {
	sessionId: string;
	delta: TokenDelta;
}

const DEFAULT_IDLE_RETIRE_SECONDS = 30;

// ---------------------------------------------------------------------------
// Coalescing (pure function; exported for contract tests)
// ---------------------------------------------------------------------------

function routeKeyOf(q: QueuedDelta): string | null {
	if (q.delta.absolute === true) return null; // absolute deltas never merge
	const parts = TOKEN_DELTA_ROUTE_FIELDS.map((f) => {
		const v = q.delta[f];
		return v === undefined ? "" : String(v);
	});
	return JSON.stringify([q.sessionId, ...parts]);
}

/**
 * Merge CONSECUTIVE incremental deltas with an identical route. Only adjacent
 * deltas merge, so ordering across sessions and across a mid-session model
 * switch is preserved exactly. Sum fields add; cost fields sum None-preserving
 * (an all-None run stays None so COALESCE leaves stored values untouched);
 * absolute=True deltas never merge.
 */
export function coalesceTokenDeltas(
	batch: readonly QueuedDelta[],
): QueuedDelta[] {
	const groups: Array<{ key: string | null; merged: QueuedDelta }> = [];
	for (const q of batch) {
		const key = routeKeyOf(q);
		const last = groups[groups.length - 1];
		if (last !== undefined && key !== null && last.key === key) {
			const merged = last.merged.delta;
			for (const f of TOKEN_DELTA_SUM_FIELDS) {
				merged[f] = (merged[f] ?? 0) + (q.delta[f] ?? 0);
			}
			for (const f of TOKEN_DELTA_COST_FIELDS) {
				const value = q.delta[f];
				if (value !== undefined && value !== null) {
					merged[f] = (merged[f] ?? 0) + value;
				}
			}
		} else {
			groups.push({
				key,
				merged: { sessionId: q.sessionId, delta: { ...q.delta } },
			});
		}
	}
	return groups.map((g) => g.merged);
}

// ---------------------------------------------------------------------------
// Direct apply path (updateTokenCounts + recordModelUsage)
// ---------------------------------------------------------------------------

interface SessionRouteRow {
	model: string | null;
	billing_provider: string | null;
	billing_base_url: string | null;
	billing_mode: string | null;
	api_call_count: number | null;
}

function num(v: number | undefined): number {
	return typeof v === "number" ? v : 0;
}

function hasAccountedUsage(d: TokenDelta): boolean {
	return Boolean(
		num(d.inputTokens) ||
			num(d.outputTokens) ||
			num(d.cacheReadTokens) ||
			num(d.cacheWriteTokens) ||
			num(d.reasoningTokens) ||
			num(d.apiCallCount) ||
			d.estimatedCostUsd ||
			d.actualCostUsd,
	);
}

function ensureSessionRow(db: Database.Database, sessionId: string): void {
	// Defensive idempotent INSERT so the UPDATE below can't silently affect 0
	// rows under concurrent load (parity of _insert_session_row fallback).
	db.prepare(
		`INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'unknown', ?)`,
	).run(sessionId, Date.now() / 1000);
}

/**
 * Accumulate a per-API-call usage delta into session_model_usage. Runs inside
 * the CALLER's write transaction (after the sessions UPDATE). When the caller
 * omits model/provider, fall back to the values already recorded on the
 * session row (same COALESCE-from-session behaviour as the summary update).
 */
function recordModelUsage(
	conn: Database.Database,
	sessionId: string,
	d: TokenDelta,
	firstSeen: number,
): void {
	const sessRow = conn
		.prepare(
			"SELECT model, billing_provider, billing_base_url, billing_mode FROM sessions WHERE id = ?",
		)
		.get(sessionId) as
		| Pick<
				SessionRouteRow,
				"model" | "billing_provider" | "billing_base_url" | "billing_mode"
		  >
		| undefined;
	const model = d.model ?? sessRow?.model ?? "";
	const provider = d.billingProvider ?? sessRow?.billing_provider ?? "";
	const baseUrl = d.billingBaseUrl ?? sessRow?.billing_base_url ?? "";
	const mode = d.billingMode ?? sessRow?.billing_mode ?? "";
	const task = d.task ?? "";
	conn
		.prepare(
			`INSERT INTO session_model_usage (
		   session_id, model, billing_provider, billing_base_url, billing_mode, task,
		   api_call_count, input_tokens, output_tokens,
		   cache_read_tokens, cache_write_tokens, reasoning_tokens,
		   estimated_cost_usd, actual_cost_usd, first_seen, last_seen
		 ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, COALESCE(?, 0), COALESCE(?, 0), ?, ?)
		 ON CONFLICT(session_id, model, billing_provider, billing_base_url, billing_mode, task) DO UPDATE SET
		   api_call_count = api_call_count + excluded.api_call_count,
		   input_tokens = input_tokens + excluded.input_tokens,
		   output_tokens = output_tokens + excluded.output_tokens,
		   cache_read_tokens = cache_read_tokens + excluded.cache_read_tokens,
		   cache_write_tokens = cache_write_tokens + excluded.cache_write_tokens,
		   reasoning_tokens = reasoning_tokens + excluded.reasoning_tokens,
		   estimated_cost_usd = estimated_cost_usd + excluded.estimated_cost_usd,
		   actual_cost_usd = CASE WHEN excluded.actual_cost_usd IS NULL
		                          THEN actual_cost_usd
		                          ELSE COALESCE(actual_cost_usd, 0) + excluded.actual_cost_usd END,
		   last_seen = excluded.last_seen`,
		)
		.run(
			sessionId,
			model,
			provider,
			baseUrl,
			mode,
			task,
			num(d.apiCallCount),
			num(d.inputTokens),
			num(d.outputTokens),
			num(d.cacheReadTokens),
			num(d.cacheWriteTokens),
			num(d.reasoningTokens),
			d.estimatedCostUsd ?? null,
			d.actualCostUsd ?? null,
			firstSeen,
			Date.now() / 1000,
		);
}

export interface ApplyTokenCountsOptions extends Record<string, unknown> {}

/**
 * Update session token counters (+ per-model usage attribution). Single
 * chokepoint every per-API-call delta flows through. `absolute=false`
 * increments; `absolute=true` sets directly.
 *
 * Route-stamp rule (first_accounted_route): the first ACCOUNTED usage event on
 * a bare row stamps the authoritative primary route (model/billing_*); after
 * that the legacy row is preserved — one row cannot represent mixed-provider
 * usage (hermes issue #51607). Per-model rows keep the accurate breakdown via
 * session_model_usage keyed by the LIVE route of each call.
 */
export async function updateTokenCounts(
	db: Database.Database,
	sessionId: string,
	d: TokenDelta,
	opts: ApplyTokenCountsOptions = {},
): Promise<void> {
	await executeWrite(
		db,
		(conn) => {
			applyTokenCountsInTx(conn, sessionId, d);
		},
		opts,
	);
}

/** Transaction-body form — compose with other statements inside one txn. */
export function applyTokenCountsInTx(
	conn: Database.Database,
	sessionId: string,
	d: TokenDelta,
): void {
	ensureSessionRow(conn, sessionId);
	const accounted = hasAccountedUsage(d);

	const existing = conn
		.prepare(
			"SELECT model, billing_provider, api_call_count FROM sessions WHERE id = ?",
		)
		.get(sessionId) as
		| Pick<SessionRouteRow, "model" | "billing_provider" | "api_call_count">
		| undefined;
	const existingModel = existing?.model ?? null;
	const existingProvider = existing?.billing_provider ?? null;
	const existingApiCalls = Number(existing?.api_call_count ?? 0) || 0;

	// First accounted usage event is the first authoritative route.
	if (
		existingApiCalls === 0 &&
		accounted &&
		d.model &&
		d.billingProvider &&
		(existingModel !== d.model || existingProvider !== d.billingProvider)
	) {
		conn
			.prepare(
				`UPDATE sessions SET model = ?, billing_provider = ?, billing_base_url = ?, billing_mode = ?
			 WHERE id = ?`,
			)
			.run(
				d.model ?? "",
				d.billingProvider ?? "",
				d.billingBaseUrl ?? "",
				d.billingMode ?? "",
				sessionId,
			);
	}

	const est = d.estimatedCostUsd ?? null;
	const act = d.actualCostUsd ?? null;
	let info;
	if (d.absolute === true) {
		info = conn
			.prepare(
				`UPDATE sessions SET
				   input_tokens = ?, output_tokens = ?,
				   cache_read_tokens = ?, cache_write_tokens = ?, reasoning_tokens = ?,
				   estimated_cost_usd = COALESCE(?, 0),
				   actual_cost_usd = CASE WHEN ? IS NULL THEN actual_cost_usd ELSE ? END,
				   cost_status = COALESCE(?, cost_status),
				   cost_source = COALESCE(?, cost_source),
				   pricing_version = COALESCE(?, pricing_version),
				   billing_provider = COALESCE(billing_provider, ?),
				   billing_base_url = COALESCE(billing_base_url, ?),
				   billing_mode = COALESCE(billing_mode, ?),
				   model = COALESCE(model, ?),
				   api_call_count = ?
				 WHERE id = ?`,
			)
			.run(
				num(d.inputTokens),
				num(d.outputTokens),
				num(d.cacheReadTokens),
				num(d.cacheWriteTokens),
				num(d.reasoningTokens),
				est,
				act,
				act,
				d.costStatus ?? null,
				d.costSource ?? null,
				d.pricingVersion ?? null,
				accounted ? (d.billingProvider ?? null) : null,
				accounted ? (d.billingBaseUrl ?? null) : null,
				accounted ? (d.billingMode ?? null) : null,
				accounted ? (d.model ?? null) : null,
				num(d.apiCallCount),
				sessionId,
			);
	} else {
		info = conn
			.prepare(
				`UPDATE sessions SET
				   input_tokens = input_tokens + ?, output_tokens = output_tokens + ?,
				   cache_read_tokens = cache_read_tokens + ?, cache_write_tokens = cache_write_tokens + ?,
				   reasoning_tokens = reasoning_tokens + ?,
				   estimated_cost_usd = COALESCE(estimated_cost_usd, 0) + COALESCE(?, 0),
				   actual_cost_usd = CASE WHEN ? IS NULL THEN actual_cost_usd ELSE COALESCE(actual_cost_usd, 0) + ? END,
				   cost_status = COALESCE(?, cost_status),
				   cost_source = COALESCE(?, cost_source),
				   pricing_version = COALESCE(?, pricing_version),
				   billing_provider = COALESCE(billing_provider, ?),
				   billing_base_url = COALESCE(billing_base_url, ?),
				   billing_mode = COALESCE(billing_mode, ?),
				   model = COALESCE(model, ?),
				   api_call_count = COALESCE(api_call_count, 0) + ?
				 WHERE id = ?`,
			)
			.run(
				num(d.inputTokens),
				num(d.outputTokens),
				num(d.cacheReadTokens),
				num(d.cacheWriteTokens),
				num(d.reasoningTokens),
				est,
				act,
				act,
				d.costStatus ?? null,
				d.costSource ?? null,
				d.pricingVersion ?? null,
				accounted ? (d.billingProvider ?? null) : null,
				accounted ? (d.billingBaseUrl ?? null) : null,
				accounted ? (d.billingMode ?? null) : null,
				accounted ? (d.model ?? null) : null,
				num(d.apiCallCount),
				sessionId,
			);
	}
	void info;

	// Only the incremental path records per-model usage; absolute cumulative
	// updates cannot be split back into routes.
	const recordModel =
		d.absolute !== true &&
		Boolean(
			num(d.inputTokens) ||
				num(d.outputTokens) ||
				num(d.cacheReadTokens) ||
				num(d.cacheWriteTokens) ||
				num(d.reasoningTokens) ||
				num(d.apiCallCount) ||
				d.estimatedCostUsd,
		);
	if (recordModel) {
		recordModelUsage(conn, sessionId, d, Date.now() / 1000);
	}
}

// ---------------------------------------------------------------------------
// The coalescing background writer
// ---------------------------------------------------------------------------

/**
 * Minimal promise-based condition variable (the threading primitive behind
 * hermes's `_token_queue_cond`): waiters resolve on notify or timeout.
 */
class CondVar {
	private waiter: (() => void) | null = null;

	wait(timeoutMs: number): Promise<void> {
		return new Promise<void>((resolve) => {
			let done = false;
			const fire = (): void => {
				if (done) return;
				done = true;
				this.waiter = null;
				resolve();
			};
			this.waiter = fire;
			const t = setTimeout(fire, Math.max(0, timeoutMs));
			t.unref?.();
		});
	}

	notify(): void {
		const w = this.waiter;
		this.waiter = null;
		w?.();
	}
}

export interface TokenWriterOptions {
	/** Injected monotonic clock in SECONDS. Default performance.now()/1000. */
	monotonicSeconds?: () => number;
	/** Writer retires after this much idle time (default 30s, hermes constant). */
	idleRetireSeconds?: number;
	/**
	 * Test hook wrapping each applied batch (throw to exercise the
	 * log-dont-raise apply-failure contract).
	 */
	applyHook?: (batch: readonly QueuedDelta[]) => void;
}

/**
 * Coalescing background token writer over ONE open state.db connection.
 *
 * Invariants (02 §7.2):
 * - queueTokenCounts is cheap (append + notify); spawns the worker lazily.
 * - The worker swaps queue→batch with the busy flag set BEFORE the clear, so
 *   the lock-free fast path in flushTokenCounts can never observe an empty
 *   queue while a popped batch is still unapplied.
 * - Apply failures are logged, never raised into a turn.
 * - A permanently stopped writer falls back to synchronous apply so deltas
 *   can't drop silently (queueTokenCounts then raises like the direct path).
 * - After every model/route change, callers flush BEFORE the route-field
 *   UPDATE (barrier against reordered deltas) — flushTokenCounts IS the barrier.
 */
export class TokenWriter {
	private readonly db: Database.Database;
	private readonly monotonicSecondsFn: () => number;
	private readonly idleRetireSeconds: number;
	private readonly applyHook:
		| ((batch: readonly QueuedDelta[]) => void)
		| undefined;

	private queue: QueuedDelta[] = [];
	private busy = false;
	private writerActive = false;
	private stopped = false;
	private lastActivityMono: number;
	private cond = new CondVar();
	private beforeExitInstalled = false;

	constructor(db: Database.Database, options: TokenWriterOptions = {}) {
		this.db = db;
		this.monotonicSecondsFn =
			options.monotonicSeconds ?? ((): number => performance.now() / 1000);
		this.idleRetireSeconds =
			options.idleRetireSeconds ?? DEFAULT_IDLE_RETIRE_SECONDS;
		this.applyHook = options.applyHook;
		this.lastActivityMono = this.monotonicSecondsFn();
	}

	/** Number of deltas awaiting application (diagnostics/tests). */
	pendingCount(): number {
		return this.queue.length;
	}

	/** Whether a batch is currently mid-apply (diagnostics/tests). */
	isBusy(): boolean {
		return this.busy;
	}

	/** Whether the lazy worker is alive (diagnostics/tests). */
	isWriterActive(): boolean {
		return this.writerActive;
	}

	/**
	 * Enqueue a token/cost delta for the background writer. Cheap (append +
	 * notify); safe to call on the turn thread after every API call. After
	 * stop(), falls back to the SYNCHRONOUS path and may raise like
	 * updateTokenCounts — enqueueing would otherwise drop the delta silently.
	 */
	queueTokenCounts(sessionId: string, delta: TokenDelta): void {
		if (this.stopped) {
			throwOnError(() => {
				void updateTokenCounts(this.db, sessionId, delta);
			});
			return;
		}
		this.queue.push({ sessionId, delta });
		this.lastActivityMono = this.monotonicSecondsFn();
		if (!this.writerActive) this.spawnWriter();
		this.cond.notify();
		this.installBeforeExitHookOnce();
	}

	private spawnWriter(): void {
		this.writerActive = true;
		void this.writerLoop();
	}

	/**
	 * Worker loop parity of _token_writer_loop: wait for work or idle
	 * retirement; swap queue→batch with busy set BEFORE the clear; apply;
	 * release busy and notify waiters.
	 */
	private async writerLoop(): Promise<void> {
		try {
			await this.writerLoopBody();
		} finally {
			// Defensive parity of hermes's "respawn if not is_alive()": whatever
			// exits this loop must clear the active flag or enqueues would strand.
			this.writerActive = false;
			this.cond.notify();
		}
	}

	/** Worker loop parity of _token_writer_loop: wait for work or idle retirement. */
	private async writerLoopBody(): Promise<void> {
		for (;;) {
			while (
				this.queue.length === 0 &&
				!this.stopped &&
				this.monotonicSecondsFn() - this.lastActivityMono <
					this.idleRetireSeconds
			) {
				const remainingMs =
					(this.lastActivityMono +
						this.idleRetireSeconds -
						this.monotonicSecondsFn()) *
					1000;
				// Cap each wait slice so retirement decisions re-evaluate promptly
				// even when an INJECTED clock jumps forward between real ticks.
				await this.cond.wait(Math.max(1, Math.min(remainingMs, 100)));
			}
			if (this.queue.length === 0) {
				// Idle retirement or stop requested and fully drained. Publish
				// retirement under the same discipline queueTokenCounts uses to
				// decide whether to spawn, so an enqueue can't strand a delta
				// behind an exiting worker.
				this.writerActive = false;
				if (!this.stopped) this.cond.notify();
				return;
			}
			this.busy = true; // BEFORE the clear — see invariant comment above
			const batch = this.queue;
			this.queue = [];
			try {
				this.applyBatch(batch);
			} finally {
				this.busy = false;
				this.cond.notify();
			}
		}
	}

	private applyBatch(batch: readonly QueuedDelta[]): void {
		// NEVER raises: accounting loss is accepted and logged (02 §12 row
		// "Token-delta apply fails — totals lag, never wrong").
		try {
			this.applyHook?.(batch);
		} catch (err) {
			console.warn(
				`[pi_state] async token accounting: batch hook failed: ${String(err)}`,
			);
		}
		let coalesced: QueuedDelta[];
		try {
			coalesced = coalesceTokenDeltas(batch);
		} catch (err) {
			// Coalescing must never kill the writer — fall back to the raw batch.
			console.warn(
				`[pi_state] async token accounting: coalesce failed, applying raw batch: ${String(err)}`,
			);
			coalesced = [...batch];
		}
		for (const q of coalesced) {
			try {
				// Synchronous in-tx form: batches are already off the turn thread.
				applyTokenCountsInTx(this.db, q.sessionId, q.delta);
			} catch (err) {
				console.warn(
					`[pi_state] async token accounting: apply failed (session=${q.sessionId}): ${String(err)}`,
				);
			}
		}
	}

	/**
	 * Block until every queued token delta has been applied (flush barrier —
	 * callers MUST run this BEFORE switching a session's model/route so queued
	 * old-route deltas land first). Returns true when fully drained, false on
	 * timeout. Never raises. A live-but-stop-flagged writer remains
	 * authoritative (its loop drains before exiting); only when the writer is
	 * dead does the caller take the leftovers — claiming busy exactly like the
	 * writer so a concurrent flush cannot double-drain.
	 */
	async flushTokenCounts(timeoutMs: number = 5000): Promise<boolean> {
		// Fast path — nothing queued, nothing in flight.
		if (this.queue.length === 0 && !this.busy) return true;
		let batch: QueuedDelta[] | null = null;
		const deadline = this.monotonicSecondsFn() + timeoutMs / 1000;
		while (this.queue.length > 0 || this.busy) {
			if (!this.writerActive && !this.busy) {
				this.busy = true;
				batch = this.queue;
				this.queue = [];
				break;
			}
			const remainingMs = (deadline - this.monotonicSecondsFn()) * 1000;
			if (remainingMs <= 0) return false;
			await this.cond.wait(remainingMs);
		}
		if (batch && batch.length > 0) {
			try {
				this.applyBatch(batch);
			} finally {
				this.busy = false;
				this.cond.notify();
			}
		}
		return true;
	}

	/**
	 * Stop the writer and drain remaining deltas (parity of
	 * _stop_token_writer). Never raises. After this resolves the writer is
	 * permanently retired and queueTokenCounts applies synchronously.
	 */
	async stop(joinTimeoutMs: number = 10_000): Promise<void> {
		this.stopped = true;
		this.cond.notify();
		const deadline = this.monotonicSecondsFn() + joinTimeoutMs / 1000;
		while (this.writerActive) {
			const remainingMs = (deadline - this.monotonicSecondsFn()) * 1000;
			if (remainingMs <= 0) {
				console.warn(
					`[pi_state] token writer did not stop within ${(joinTimeoutMs / 1000).toFixed(0)}s; ${this.queue.length} queued delta(s) not persisted`,
				);
				return;
			}
			await this.cond.wait(Math.min(remainingMs, 250));
		}
		// Writer exited (or never started) — apply leftovers synchronously,
		// claiming busy like the writer/flush drains do.
		while (this.busy) {
			const remainingMs = (deadline - this.monotonicSecondsFn()) * 1000;
			if (remainingMs <= 0) {
				console.warn(
					`[pi_state] concurrent token drain did not finish within ${(joinTimeoutMs / 1000).toFixed(0)}s; ${this.queue.length} queued delta(s) not persisted`,
				);
				return;
			}
			await this.cond.wait(Math.min(remainingMs, 250));
		}
		const batch = this.queue;
		if (batch.length > 0) {
			this.busy = true;
			this.queue = [];
			try {
				this.applyBatch(batch);
			} finally {
				this.busy = false;
				this.cond.notify();
			}
		}
	}

	/**
	 * atexit analog: drain on process exit once a delta has been enqueued
	 * (registered lazily; daemon-like — never hangs exit).
	 */
	private installBeforeExitHookOnce(): void {
		if (this.beforeExitInstalled) return;
		this.beforeExitInstalled = true;
		process.once("beforeExit", () => {
			void this.stop().catch(() => {
				/* best effort — never fatal at shutdown */
			});
		});
	}
}

function throwOnError(fn: () => void): void {
	fn();
}
