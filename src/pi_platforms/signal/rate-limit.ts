// pi_platforms/signal/rate-limit — A18 ride-along: the Signal attachment
// rate-limit scheduler + signal-cli error-shape detection, ported from the
// READ-ONLY Hermes gateway/platforms/signal_rate_limit.py (semantics only).
//
// Shape summary (all anchors in that file):
//   - Process-wide token-bucket SIMULATOR of the server's per-account
//     attachment bucket: capacity 50, refill 1 token / retry-after seconds
//     (default 4s). `acquire(n)` BLOCKS until n tokens are modeled available
//     and does NOT deduct — the bucket is a read-only model; deduction
//     happens in report_rpc_duration() AFTER a successful send, WITHOUT
//     crediting refill for the upload window ("Signal's server checks the
//     bucket at RPC start and does not refill during request processing").
//   - feedback(retry_after, n) applies server truth after a 429: zero the
//     bucket and calibrate refill_rate = 1/retry_after when exposed.
//   - Detection helpers fish a 429 out of signal-cli's THREE error shapes:
//     typed code -5 (≥ v0.14.3), legacy "[429]/RateLimitException"
//     substrings, and libsignal-net "Retry after N seconds" leaked through
//     AttachmentInvalidException.
//   - _format_wait / _signal_send_timeout user-facing pacing helpers.
//
// Concurrency parity: Python serializes acquire/report through an asyncio.Lock
// (FIFO across agent sessions); this port serializes through a promise-chain
// mutex. Time is INJECTED (nowMs/sleepMs) so ladders run on the manual clock.

import {
	SIGNAL_RATE_LIMIT_BUCKET_CAPACITY,
	SIGNAL_RATE_LIMIT_DEFAULT_RETRY_AFTER_S,
	SIGNAL_RPC_ERROR_RATELIMIT,
} from "./manifest.js";

// ── errors ───────────────────────────────────────────────────────────────────

/**
 * Raised by the adapter's rpc() for rate-limit responses when the caller
 * opted in via raiseOnRateLimit. Carries the server-supplied per-token
 * Retry-After (seconds) when signal-cli ≥ v0.14.3 exposes it.
 */
export class SignalRateLimitError extends Error {
	readonly retryAfter: number | null;
	constructor(message: string, retryAfter: number | null = null) {
		super(message);
		this.name = "SignalRateLimitError";
		this.retryAfter = retryAfter;
	}
}

/** acquire(n) requested more than the whole bucket. */
export class SignalSchedulerError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "SignalSchedulerError";
	}
}

// ── detection helpers ────────────────────────────────────────────────────────

/** Numeric parse core (agent.retry_utils.parse_retry_after_seconds shape):
 * positive finite numbers win; garbage → null. */
export function parseRetryAfterSeconds(v: unknown): number | null {
	const n = typeof v === "number" ? v : Number(String(v ?? ""));
	if (!Number.isFinite(n) || n <= 0) return null;
	return n;
}

const RETRY_AFTER_RE = /Retry after (\d+(?:\.\d+)?)\s*second/i;

/**
 * Pull the per-token Retry-After window from a signal-cli rate-limit error.
 * Two ordered sources:
 *   1. error.data.response.results[*].retryAfterSeconds (structured,
 *      ≥ v0.14.3 plain RateLimitException) — MAX across results wins;
 *   2. "Retry after N seconds" parsed from the message (libsignal-net
 *      RetryLaterException wrapped as AttachmentInvalidException during
 *      upload, where the structured field stays null).
 */
export function extractRetryAfterSeconds(err: unknown): number | null {
	let msg: string;
	if (err !== null && typeof err === "object" && !Array.isArray(err)) {
		const rec = err as Record<string, unknown>;
		const data = rec["data"];
		const response =
			data !== null && typeof data === "object"
				? (data as Record<string, unknown>)["response"]
				: undefined;
		const resultsRaw =
			response !== null && typeof response === "object"
				? (response as Record<string, unknown>)["results"]
				: undefined;
		if (Array.isArray(resultsRaw)) {
			const candidates: number[] = [];
			for (const r of resultsRaw) {
				if (r === null || typeof r !== "object") continue;
				const retryAfter = (r as Record<string, unknown>)["retryAfterSeconds"];
				if (retryAfter === undefined || retryAfter === null || retryAfter === 0)
					continue;
				const parsed = parseRetryAfterSeconds(retryAfter);
				if (parsed !== null) candidates.push(parsed);
			}
			if (candidates.length > 0) return Math.max(...candidates);
		}
		msg =
			typeof rec["message"] === "string"
				? rec["message"]
				: String(rec["message"] ?? "");
	} else {
		msg = String(err);
	}
	const match = RETRY_AFTER_RE.exec(msg);
	return match ? parseRetryAfterSeconds(match[1]) : null;
}

/**
 * True when a signal-cli RPC error reflects a rate-limit failure. Three
 * layers: typed code -5; "[429]" substring; case-insensitive ratelimit /
 * RetryLaterException / "retry after" substrings.
 */
export function isSignalRateLimitError(err: unknown): boolean {
	if (
		err !== null &&
		typeof err === "object" &&
		(err as Record<string, unknown>)["code"] === SIGNAL_RPC_ERROR_RATELIMIT
	) {
		return true;
	}
	const message =
		err !== null && typeof err === "object" && !Array.isArray(err)
			? String((err as Record<string, unknown>)["message"] ?? "")
			: String(err);
	const lower = message.toLowerCase();
	return (
		message.includes("[429]") ||
		lower.includes("ratelimit") ||
		lower.includes("retrylaterexception") ||
		lower.includes("retry after")
	);
}

/** Human-friendly wait label for pacing notices (<90s ⇒ "Ns", else minutes). */
export function formatWait(seconds: number): string {
	const s = Math.max(0, seconds);
	if (s < 90) return `${Math.round(s)}s`;
	return `${Math.max(1, Math.round(s / 60))} min`;
}

/** HTTP timeout for a send RPC: ≤0 ⇒ 30s; else max(60, 5·n) — uploads are
 * serial server-side, so time scales with batch size. */
export function signalSendTimeout(numAttachments: number): number {
	if (numAttachments <= 0) return 30_000;
	return Math.max(60_000, 5_000 * numAttachments);
}

// ── scheduler ────────────────────────────────────────────────────────────────

export interface SchedulerClock {
	nowMs: () => number;
	sleepMs: (ms: number) => Promise<void>;
}

const realClock: SchedulerClock = {
	nowMs: () => Date.now(),
	sleepMs: (ms) => new Promise((r) => setTimeout(r, ms)),
};

export interface SchedulerOptions {
	capacity?: number | undefined;
	defaultRetryAfter?: number | undefined;
	clock?: SchedulerClock | undefined;
}

/**
 * Token-bucket simulator for Signal attachment sends (Hermes
 * SignalAttachmentScheduler). One instance per process in production.
 */
export class SignalAttachmentScheduler {
	readonly capacity: number;
	tokens: number;
	refillRate: number;
	lastRefillMs: number;

	private readonly clock: SchedulerClock;
	private mutexTail: Promise<unknown> = Promise.resolve();
	/** Mutation-test seam: subclasses override the single refills. */
	protected refillEnabled = true;

	constructor(opts: SchedulerOptions = {}) {
		this.capacity = opts.capacity ?? SIGNAL_RATE_LIMIT_BUCKET_CAPACITY;
		this.tokens = this.capacity;
		this.refillRate =
			1.0 / (opts.defaultRetryAfter ?? SIGNAL_RATE_LIMIT_DEFAULT_RETRY_AFTER_S);
		this.clock = opts.clock ?? realClock;
		this.lastRefillMs = this.clock.nowMs();
	}

	/** Serialize critical sections FIFO (asyncio.Lock parity). */
	private serialized<T>(body: () => Promise<T> | T): Promise<T> {
		const run = this.mutexTail.then(body, body);
		this.mutexTail = run.catch(() => undefined);
		return run;
	}

	/** Refill projection up to now (advances lastRefillMs). */
	private refillLocked(): void {
		const now = this.clock.nowMs();
		const elapsedS = (now - this.lastRefillMs) / 1000;
		if (this.refillEnabled && elapsedS > 0 && this.tokens < this.capacity) {
			this.tokens = Math.min(
				this.capacity,
				this.tokens + elapsedS * this.refillRate,
			);
		}
		this.lastRefillMs = now;
	}

	/** Best-effort seconds until n tokens would be available (lock-free). */
	estimateWait(n: number): number {
		const now = this.clock.nowMs();
		const elapsedS = (now - this.lastRefillMs) / 1000;
		let projected = this.tokens;
		if (elapsedS > 0 && projected < this.capacity)
			projected = Math.min(
				this.capacity,
				projected + elapsedS * this.refillRate,
			);
		const deficit = n - projected;
		if (deficit <= 0) return 0;
		return deficit / this.refillRate;
	}

	/**
	 * Block until ≥n tokens are modeled available; returns seconds slept.
	 * Does NOT deduct tokens — call report_rpc_duration() after the RPC.
	 */
	async acquire(n: number): Promise<number> {
		if (n <= 0) return 0;
		if (n > this.capacity) {
			throw new SignalSchedulerError(
				`Signal scheduler was called requesting ${n} tokens (max is ${this.capacity})`,
			);
		}
		let totalSleptS = 0;
		for (;;) {
			const deficitS = await this.serialized(() => {
				this.refillLocked();
				if (this.tokens >= n) return null;
				return (n - this.tokens) / this.refillRate;
			});
			if (deficitS === null) return totalSleptS;
			await this.clock.sleepMs(deficitS * 1000);
			totalSleptS += deficitS;
		}
	}

	/**
	 * Record a completed attachment-send RPC: deduct n tokens WITHOUT
	 * crediting refill during the upload window; next refill counts from now.
	 */
	async reportRpcDuration(
		rpcDurationS: number,
		nAttachments: number,
	): Promise<void> {
		if (nAttachments <= 0) return;
		await this.serialized(() => {
			const now = this.clock.nowMs();
			this.tokens = Math.max(0, this.tokens - nAttachments);
			this.lastRefillMs = now;
			void rpcDurationS; // observability-only upstream
		});
	}

	/**
	 * Apply server feedback after a 429: the reported per-token window is
	 * AUTHORITATIVE — recalibrate refill_rate from it; drain the bucket.
	 */
	feedback(retryAfter: number | null, nAttempted: number): void {
		void nAttempted; // observability-only upstream
		if (retryAfter !== null && retryAfter > 0) {
			this.refillRate = 1.0 / retryAfter;
		}
		this.tokens = 0;
		this.lastRefillMs = this.clock.nowMs();
	}

	/** Diagnostic snapshot; read-only (does not advance lastRefillMs). */
	state(): {
		tokens: number;
		capacity: number;
		refillRate: number;
		refillSecondsPerToken: number;
	} {
		const now = this.clock.nowMs();
		const elapsedS = (now - this.lastRefillMs) / 1000;
		let projected = this.tokens;
		if (elapsedS > 0 && projected < this.capacity)
			projected = Math.min(
				this.capacity,
				projected + elapsedS * this.refillRate,
			);
		return {
			tokens: Math.round(projected * 10) / 10,
			capacity: Math.trunc(this.capacity),
			refillRate: Math.round(this.refillRate * 10_000) / 10_000,
			refillSecondsPerToken:
				this.refillRate > 0
					? Math.round((1 / this.refillRate) * 10) / 10
					: Infinity,
		};
	}
}

// ── process-wide singleton ───────────────────────────────────────────────────

let scheduler: SignalAttachmentScheduler | null = null;

/** Process-wide scheduler, created on first access (get_scheduler parity). */
export function getScheduler(): SignalAttachmentScheduler {
	if (scheduler === null) scheduler = new SignalAttachmentScheduler();
	return scheduler;
}

/** Drop the cached scheduler (test-only; never call from production paths). */
export function resetScheduler(): void {
	scheduler = null;
}
