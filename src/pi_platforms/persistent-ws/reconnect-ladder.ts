// pi_platforms/persistent-ws/reconnect-ladder — the ws-shape reconnect ladder
// (04 §3 obligation matrix, Persistent WS column): exponential backoff with a
// jitter cap; server Retry-After AUTHORITATIVE over the local schedule;
// healthy session RESETS the ladder.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/slack/adapter.py:conversations_replies 429 ladder
//     (1s·2^attempt generic backoff) + Retry-After header capture into
//     SendResult.retry_after (~L2875) — the captured value shapes the next
//     delay instead of the computed exponential step.
//   gateway/platforms/base.py:_send_with_retry — retry_after honored ONCE
//     over the local schedule (kit send-retry.ts owns the SEND ladder; this
//     module owns the TRANSPORT/reconnect ladder).

import type { SleepFn } from "./manual-clock.js";

export interface ReconnectLadderOptions {
	/** First computed delay. Default 1000ms. */
	baseDelayMs?: number | undefined;
	/** Hard cap for COMPUTED delays. Default 60_000ms. */
	maxDelayMs?: number | undefined;
	/** Jitter fraction 0..1 (deterministic when rng injected). Default 0.2. */
	jitterFraction?: number | undefined;
	rng?: (() => number) | undefined;
	sleep?: ((ms: number) => Promise<void>) | undefined;
}

export interface LadderStep {
	delayMs: number;
	/** True when the delay came from a server-authoritative Retry-After. */
	authoritative: boolean;
	attempt: number;
}

/**
 * ONE ladder instance per adapter session. `wait()` sleeps the next delay and
 * returns what it chose; `reset()` on a healthy (re)established session.
 */
export class ReconnectLadder {
	private attempts = 0;
	/** Every delay chosen so far — escalation/cap/Retry-After observability. */
	readonly steps: LadderStep[] = [];

	private readonly baseDelayMs: number;
	private readonly maxDelayMs: number;
	private readonly jitterFraction: number;
	private readonly rng: () => number;
	private readonly sleepFn: SleepFn;

	constructor(opts: ReconnectLadderOptions = {}) {
		this.baseDelayMs = opts.baseDelayMs ?? 1_000;
		this.maxDelayMs = opts.maxDelayMs ?? 60_000;
		this.jitterFraction = opts.jitterFraction ?? 0.2;
		this.rng = opts.rng ?? (() => Math.random());
		this.sleepFn =
			opts.sleep ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
	}

	get attemptCount(): number {
		return this.attempts;
	}

	/**
	 * The delay the NEXT reconnect attempt would wait. Server Retry-After
	 * (seconds) is authoritative: honored verbatim over the exponential
	 * schedule (capped only at the ladder's hard ceiling), and it does NOT
	 * advance the exponential attempt counter by itself.
	 */
	delayFor(retryAfterSeconds?: number | null): LadderStep {
		if (
			retryAfterSeconds !== undefined &&
			retryAfterSeconds !== null &&
			retryAfterSeconds >= 0
		) {
			return {
				delayMs: Math.min(retryAfterSeconds * 1000, this.maxDelayMs),
				authoritative: true,
				attempt: this.attempts,
			};
		}
		const exponential =
			this.baseDelayMs *
			2 ** this.attempts *
			(1 + this.jitterFraction * this.rng());
		const capped = Math.min(exponential, this.maxDelayMs);
		this.attempts += 1;
		return { delayMs: capped, authoritative: false, attempt: this.attempts };
	}

	/** Sleep the next delay (Retry-After shaped when provided). */
	async wait(retryAfterSeconds?: number | null): Promise<LadderStep> {
		const step = this.delayFor(retryAfterSeconds);
		this.steps.push(step);
		await this.sleepFn(step.delayMs);
		return step;
	}

	/** Healthy session — back to base delay (ladder resets on success). */
	reset(): void {
		this.attempts = 0;
	}
}
