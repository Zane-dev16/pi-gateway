// pi_platforms/slack/rate-gate — Q17 egress gating realized adapter-side:
// "adapters declare budgets as data; the runner consults before egress."
// The gate consumes one token per transmission PER TIER CLASS (method class),
// refuses BEFORE the wire when a tier's fixed window is exhausted, and
// reports the server-shaped retry horizon as DATA (seconds to window reset).
//
// Window rotation reads the INJECTED clock only (workspace flake rule) —
// tests rotate windows by advancing ManualClock, never by sleeping.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/slack/adapter.py:_fetch_thread_context — Tier-3 429
//     ladder (1s·2^attempt, "rate_limited" classification) shows the
//     vendor's per-class budget awareness; this module moves the CLASS
//     boundary to egress admission instead of post-hoc 429 recovery.
//   kit/capabilities.ts:governingTier — WHICH tier governs an op.

import type { RateBudget, RateOp } from "../kit/capabilities.js";
import { governingTier } from "../kit/capabilities.js";
import type { NowFn } from "../persistent-ws/manual-clock.js";

export interface GateDecision {
	admitted: boolean;
	/** Governing tier when a decision was made; null when ungated op. */
	tier: string | null;
	/** When refused: milliseconds until the governing window rotates. */
	retryAfterMs: number;
}

interface WindowState {
	windowStartMs: number;
	used: number;
}

export class RateBudgetGate {
	private readonly states = new Map<string, WindowState>();

	constructor(
		private readonly budget: RateBudget | undefined,
		private readonly nowMs: NowFn,
	) {}

	/**
	 * Consult + consume BEFORE an egress op. Exhausted tier ⇒ refused with
	 * the time-to-window-reset; NOTHING is transmitted and NO token is
	 * consumed by the refusal itself.
	 */
	check(op: RateOp): GateDecision {
		const tier = governingTier(this.budget ?? undefined, op);
		if (tier === null) return { admitted: true, tier: null, retryAfterMs: 0 };
		const windowMs = tier.windowSeconds * 1000;
		const now = this.nowMs();
		const currentWindowStart = Math.floor(now / windowMs) * windowMs;
		const state = this.states.get(tier.name);
		const fresh =
			state === undefined || state.windowStartMs !== currentWindowStart;
		const used = fresh ? 0 : state.used;
		if (used >= tier.limit) {
			return {
				admitted: false,
				tier: tier.name,
				retryAfterMs: currentWindowStart + windowMs - now,
			};
		}
		this.states.set(tier.name, {
			windowStartMs: currentWindowStart,
			used: used + 1,
		});
		return { admitted: true, tier: tier.name, retryAfterMs: 0 };
	}

	/** Tokens consumed so far in the CURRENT window of the governing tier. */
	usedInCurrentWindow(op: RateOp): number | null {
		const tier = governingTier(this.budget ?? undefined, op);
		if (tier === null) return null;
		const windowMs = tier.windowSeconds * 1000;
		const now = this.nowMs();
		const state = this.states.get(tier.name);
		if (
			state === undefined ||
			state.windowStartMs !== Math.floor(now / windowMs) * windowMs
		)
			return 0;
		return state.used;
	}
}
