// human-wait.ts — human-wait accounting for approval waits (07 §8.2 binding;
// #79719). The wait is genuinely parked on a human answer and must NOT count
// against concurrent tool-batch deadlines; a window that overstays its
// ceiling was itself wedged and contributes at most the ceiling.
//
// Hermes anchors (READ-ONLY reference):
//   tools/approval.py:HUMAN_WAIT_MARGIN_S        → HUMAN_WAIT_MARGIN_S
//   tools/approval.py:human_wait_ceiling         → humanWaitCeiling
//   tools/approval.py:_clamped_window_seconds    → clampedWindowSeconds
//   tools/approval.py:human_wait_window          → HumanWaitAccounting.begin
//   tools/approval.py:human_wait_seconds         → HumanWaitAccounting.seconds

import type { GatewayClock } from "./clock.js";

/** Margin added on top of approvals.timeout when clamping window accrual. */
export const HUMAN_WAIT_MARGIN_S = 60;

/**
 * Max seconds a single window may contribute: approvals.timeout + margin.
 * Every legitimate human wait self-terminates at the approval timeout, so a
 * window that overstays this ceiling is itself wedged and must not keep
 * extending a batch deadline.
 */
export function humanWaitCeiling(timeoutSeconds: number): number {
	return timeoutSeconds + HUMAN_WAIT_MARGIN_S;
}

/** Shared by close-time accrual and open-window reads so both stay identical. */
export function clampedWindowSeconds(
	startedSeconds: number,
	nowSeconds: number,
	ceilingSeconds: number,
): number {
	return Math.min(Math.max(0, nowSeconds - startedSeconds), ceilingSeconds);
}

interface HumanWaitState {
	windowStartedSeconds: number | null;
	pending: number;
	completedSeconds: number;
}

const MAX_SESSIONS = 256;

/**
 * Per-session human-wait ledger. Overlapping windows for the same session
 * COALESCE via a pending counter so two serialized prompts don't double-count
 * the same wall clock (human_wait_window parity). `begin()` returns the close
 * function; callers MUST close exactly once (the gate's finally block).
 */
export class HumanWaitAccounting {
	private readonly states = new Map<string, HumanWaitState>();

	constructor(
		private readonly clock: GatewayClock,
		private readonly ceilingSeconds: number,
	) {}

	/**
	 * Mark the enclosed block as time blocked on a human prompt. Wrap ONLY the
	 * genuinely-parked wait; anything else re-creates the #79719 hang.
	 */
	begin(sessionKey: string): () => void {
		const now = this.clock.nowSeconds();
		let state = this.states.get(sessionKey);
		if (!state) {
			state = this.createState(sessionKey);
		}
		if (state.pending === 0) {
			state.windowStartedSeconds = now;
		}
		state.pending += 1;
		let closed = false;
		return () => {
			if (closed) return;
			closed = true;
			const end = this.clock.nowSeconds();
			const current = this.states.get(sessionKey);
			if (!current) return;
			current.pending -= 1;
			if (current.pending === 0) {
				if (current.windowStartedSeconds !== null) {
					current.completedSeconds += clampedWindowSeconds(
						current.windowStartedSeconds,
						end,
						this.ceilingSeconds,
					);
				}
				current.windowStartedSeconds = null;
			}
		};
	}

	/**
	 * Total human-wait seconds for a session: completed windows plus the open
	 * one (clamped identically). Monotonically non-decreasing except when an
	 * idle entry is evicted under cap pressure — which can only shrink a
	 * consumer's baseline delta to zero (the safe direction).
	 */
	seconds(sessionKey: string): number {
		const state = this.states.get(sessionKey);
		if (!state) return 0;
		let total = state.completedSeconds;
		if (state.windowStartedSeconds !== null) {
			total += clampedWindowSeconds(
				state.windowStartedSeconds,
				this.clock.nowSeconds(),
				this.ceilingSeconds,
			);
		}
		return total;
	}

	/**
	 * Evict idle entries insertion-order-first until under the cap (best-effort
	 * parity of `_human_wait_state` eviction). Open windows are NEVER evicted —
	 * that would corrupt live accounting.
	 */
	private createState(sessionKey: string): HumanWaitState {
		if (this.states.size >= MAX_SESSIONS) {
			for (const key of [...this.states.keys()]) {
				if (this.states.size < MAX_SESSIONS) break;
				const candidate = this.states.get(key);
				if (candidate && candidate.pending === 0) {
					this.states.delete(key);
				}
			}
		}
		const state: HumanWaitState = {
			windowStartedSeconds: null,
			pending: 0,
			completedSeconds: 0,
		};
		this.states.set(sessionKey, state);
		return state;
	}
}
