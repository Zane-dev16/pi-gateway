// pi_platforms/discord/rate-buckets — the Q17 per-route rate-bucket LEDGER.
// X-RateLimit-Bucket semantics simplified to data: every REST route maps to a
// bucket (manifest.ts RATE_BUCKETS); the ledger consumes tokens per
// (bucket, scope) on the INJECTED clock and gates BEFORE egress. Server 429s
// record AUTHORITATIVE retry_after that overrides the computed window.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/discord/adapter.py:2312-2335 _extract_discord_retry_after
//     (Retry-After / X-RateLimit-Reset-After capture, clamped ≥1.0s)
//   adapter.py:2336-2369 _is_discord_rate_limit (429 classification — the
//     SendResult this ledger produces must classify as flood/retryable)
//   adapter.py:81 max_ratelimit_timeout cap 30s (RATE_LIMIT_SLEEP_CAP)

import {
	RATE_BUCKETS,
	RETRY_AFTER_FLOOR_SECONDS,
	type RateBucketSpec,
	type RateRouteOp,
} from "./manifest.js";
import type { NowFn } from "./clock.js";

export interface RateVerdictAllowed {
	allowed: true;
	bucketId: string;
}

export interface RateVerdictBlocked {
	allowed: false;
	bucketId: string;
	/** Authoritative-or-computed wait in seconds (≥ floor). */
	retryAfterSeconds: number;
}

export type RateVerdict = RateVerdictAllowed | RateVerdictBlocked;

interface BucketWindow {
	/** Fixed-window index: floor(nowMs / windowMs). */
	windowIndex: number;
	used: number;
	/** Unix-ms until which the bucket is frozen by an authoritative 429. */
	blockedUntilMs: number | null;
}

export interface RateBucketLedgerOptions {
	nowMs?: NowFn | undefined;
	buckets?: readonly RateBucketSpec[] | undefined;
}

export class RateBucketLedger {
	private readonly windows = new Map<string, BucketWindow>();
	private readonly nowFn: NowFn;
	private readonly buckets: readonly RateBucketSpec[];

	constructor(opts: RateBucketLedgerOptions = {}) {
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.buckets = opts.buckets ?? RATE_BUCKETS;
	}

	/** Bucket specs governing a route (data-driven resolution). */
	bucketsFor(route: RateRouteOp): RateBucketSpec[] {
		return this.buckets.filter((b) => b.routes.includes(route));
	}

	/**
	 * Consume ONE token for `route` scoped to `channelId` (global buckets key
	 * on the constant scope ""). Blocked ⇒ retryAfterSeconds (authoritative
	 * freeze remainder when present, else next window boundary).
	 */
	consume(route: RateRouteOp, channelId: string): RateVerdict {
		let worst: RateVerdictBlocked | null = null;
		for (const spec of this.bucketsFor(route)) {
			const key = `${spec.id}#${spec.scope === "global" ? "" : channelId}`;
			const verdict = this.consumeOne(spec, key);
			if (!verdict.allowed) {
				const blocked = verdict;
				if (
					worst === null ||
					blocked.retryAfterSeconds > worst.retryAfterSeconds
				)
					worst = blocked;
			}
		}
		return worst ?? { allowed: true, bucketId: "ok" };
	}

	/**
	 * Record a server-authoritative 429 for a route+scope: EVERY bucket
	 * governing the route freezes for the captured delay (clamped to the ≥1s
	 * floor, `_extract_discord_retry_after` parity).
	 */
	recordAuthority(
		route: RateRouteOp,
		channelId: string,
		retryAfterSeconds: number,
	): void {
		const clamped = Math.max(RETRY_AFTER_FLOOR_SECONDS, retryAfterSeconds);
		const until = this.nowFn() + clamped * 1000;
		for (const spec of this.bucketsFor(route)) {
			const key = `${spec.id}#${spec.scope === "global" ? "" : channelId}`;
			const win = this.windowFor(key);
			win.blockedUntilMs = Math.max(win.blockedUntilMs ?? 0, until);
		}
	}

	/** Reset all windows (healthy-session parity with ladder reset). */
	reset(): void {
		this.windows.clear();
	}

	// ── internals ──

	private windowFor(key: string): BucketWindow {
		let win = this.windows.get(key);
		if (win === undefined) {
			win = { windowIndex: -1, used: 0, blockedUntilMs: null };
			this.windows.set(key, win);
		}
		return win;
	}

	private consumeOne(spec: RateBucketSpec, key: string): RateVerdict {
		const now = this.nowFn();
		const win = this.windowFor(key);
		if (win.blockedUntilMs !== null) {
			if (now < win.blockedUntilMs) {
				return {
					allowed: false,
					bucketId: spec.id,
					retryAfterSeconds: (win.blockedUntilMs - now) / 1000,
				};
			}
			win.blockedUntilMs = null; // freeze expired
		}
		const windowMs = spec.windowSeconds * 1000;
		const idx = Math.floor(now / windowMs);
		if (win.windowIndex !== idx) {
			win.windowIndex = idx;
			win.used = 0;
		}
		if (win.used >= spec.limit) {
			const retryAfterSeconds = ((idx + 1) * windowMs - now) / 1000;
			return { allowed: false, bucketId: spec.id, retryAfterSeconds };
		}
		win.used += 1;
		return { allowed: true, bucketId: spec.id };
	}
}
