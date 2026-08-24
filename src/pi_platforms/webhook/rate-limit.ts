// pi_platforms/webhook/rate-limit — per-route sliding-window ingress rate
// limit (DEC-017; 04 §3 webhook row "Provider quotas per window").
//
// Ported from the READ-ONLY Hermes reference:
//   webhook.py:_record_rate_limit_hit (@437): per-route deque of hit
//   timestamps, _RATE_WINDOW_SECONDS=60 sliding window; AT the limit the hit
//   is rejected WITHOUT being appended (rejected requests never consume
//   window slots); checked AFTER signature validation.

export interface RateLimiterOptions {
	/** Rejected-when-window-holds threshold per route. */
	limit: number;
	/** Sliding window width (webhook.py: 60_000 ms). */
	windowMs?: number | undefined;
	/** Injected monotonic-ms clock — no wall-clock reads anywhere. */
	nowMs: () => number;
}

export class SlidingWindowRateLimiter {
	private readonly windows = new Map<string, number[]>();
	private readonly limit: number;
	private readonly windowMs: number;
	private readonly nowMs: () => number;

	constructor(opts: RateLimiterOptions) {
		this.limit = opts.limit;
		this.windowMs = opts.windowMs ?? 60_000;
		this.nowMs = opts.nowMs;
	}

	/**
	 * Record one hit for `route`; false ⇒ the caller must answer 429 and the
	 * hit is NOT recorded (parity of the early return before window.append).
	 */
	tryRecord(route: string): boolean {
		const now = this.nowMs();
		const window = this.windows.get(route) ?? [];
		const cutoff = now - this.windowMs;
		while (window.length > 0 && (window[0] as number) < cutoff) {
			window.shift();
		}
		if (window.length >= this.limit) {
			this.windows.set(route, window);
			return false;
		}
		window.push(now);
		this.windows.set(route, window);
		return true;
	}

	/** Current in-window hit count (observability/tests). */
	hitsInWindow(route: string): number {
		const now = this.nowMs();
		const window = this.windows.get(route) ?? [];
		const cutoff = now - this.windowMs;
		return window.filter((t) => t >= cutoff).length;
	}
}
