// cli-client.ts — the CLI side of DEC-008: request + poll-block on terminal
// state. Port of cli_commands_mixin.py:_handle_handoff_command's DB half
// (the UX pre-checks — platform enabled, home channel exists, agent idle —
// need live config/agent state and belong to the CLI entrypoint surface).
//
//   1. requestHandoff: mark this session row pending (refused while already
//      in flight).
//   2. pollUntilTerminal: tick every 0.5s, bail at ~60s. completed ⇒ the
//      gateway owns the session now (CLI exits); failed ⇒ error payload for
//      the user; timeout ⇒ fail_handoff("timed out waiting for gateway") so
//      a stranded 'pending'/'running' row becomes retryable.
//
// The timeout path is ALSO the crash-recovery contract for rows stranded at
// 'running' by a SIGKILLed gateway: fail_handoff is unconditional, so the
// blocked CLI always converges the row to a retryable state.
//
// HARD RULE honored here: deadline/cadence time flows through the injected
// GatewayClock only (contracts advance virtual time; no wall sleeps).

import type { GatewayClock } from "./clock.js";
import { systemClock } from "./clock.js";
import type { HandoffQueue } from "./queue.js";

/** Poll-block deadline (parity: 60s). */
export const HANDOFF_POLL_DEADLINE_SECONDS = 60;
/** Poll-block cadence (parity: 0.5s). */
export const HANDOFF_POLL_TICK_MS = 500;

/** Exact recovery message recorded when the gateway never settles the row. */
export const HANDOFF_TIMED_OUT_MESSAGE = "timed out waiting for gateway";

export type HandoffPollOutcome =
	| { kind: "completed" }
	| { kind: "failed"; error: string }
	| { kind: "timeout"; lastState: string };

interface HandoffCliClientOptions {
	clock?: GatewayClock;
}

export class HandoffCliClient {
	private readonly queue: HandoffQueue;
	private readonly clock: GatewayClock;

	constructor(queue: HandoffQueue, opts: HandoffCliClientOptions = {}) {
		this.queue = queue;
		this.clock = opts.clock ?? systemClock;
	}

	/**
	 * Mark the session row pending. False means already in flight — the user
	 * must wait for the current handoff to settle before retrying
	 * (request_handoff CAS parity).
	 */
	requestHandoff(sessionId: string, platform: string): Promise<boolean> {
		return this.queue.requestHandoff(sessionId, platform);
	}

	/**
	 * Poll-block until the row reaches a terminal state or the deadline fires.
	 * Never throws; state-read hiccups degrade to "still waiting" for that
	 * tick (get_handoff_state exception parity).
	 */
	async pollUntilTerminal(
		sessionId: string,
		opts: { timeoutSeconds?: number; tickMs?: number } = {},
	): Promise<HandoffPollOutcome> {
		const timeoutSeconds = opts.timeoutSeconds ?? HANDOFF_POLL_DEADLINE_SECONDS;
		const tickMs = opts.tickMs ?? HANDOFF_POLL_TICK_MS;
		const deadline = this.clock.nowSeconds() + timeoutSeconds;

		for (;;) {
			let snapshot: {
				state: string | null;
				platform: string | null;
				error: string | null;
			} | null = null;
			try {
				snapshot = this.queue.getHandoffState(sessionId);
			} catch {
				snapshot = null; // transient read failure ≙ keep waiting
			}
			const state = snapshot?.state ?? null;
			if (state === "completed") return { kind: "completed" };
			if (state === "failed") {
				return { kind: "failed", error: snapshot?.error ?? "unknown error" };
			}
			if (this.clock.nowSeconds() >= deadline) {
				await this.queue.failHandoff(sessionId, HANDOFF_TIMED_OUT_MESSAGE);
				return { kind: "timeout", lastState: state ?? "pending" };
			}
			await this.clock.sleepMs(tickMs);
		}
	}
}
