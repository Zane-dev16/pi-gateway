// pi_embedded/cron/claim-heartbeat.ts — the fire-claim heartbeat watch.
//
// Hermes anchors (READ-ONLY reference):
//   cron/scheduler.py run_job claim-heartbeat polling (07 §5.2 fire
//   ownership) → runWithClaimHeartbeat
//   cron/scheduler.py:request_hard_interrupt → interrupt callback
//
// Scope note (DEC-070): this watch is the SURVIVING half of the former
// true-inactivity runaway bound (inactivity.ts, removed under the DEC-070
// scope amendment). The inactivity watchdog (HERMES_CRON_TIMEOUT idle kill)
// is gone; what stays is the claim-loss detector — while a job runs, its
// fire claim is heartbeated every poll, and a STOLEN claim aborts the stale
// run so only the winner ever writes results.
//
// HARD RULE: all timing flows through the injected CronClock. No logic path
// reads a wall clock; tests drive polls with a manual clock.

import type { CronClock } from "./clock.js";

/** Poll cadence of the claim-heartbeat loop (parity of scheduler polling). */
export const CLAIM_HEARTBEAT_POLL_SECONDS = 5;

export interface ClaimHeartbeatResult<T> {
	/** Set when the executor settled before any abort. */
	result?: T;
	/** True when shouldAbort bailed the watch (claim-loss shape). */
	aborted?: boolean;
}

export interface ClaimHeartbeatOptions<T> {
	/** The job body (agent turn(s)); must settle once abort() lands. */
	exec: () => Promise<T>;
	/** Hard-interrupt analogue (session.abort() / request_hard_interrupt). */
	interrupt: () => boolean | Promise<boolean> | void | Promise<void>;
	clock: CronClock;
	/** Poll cadence in logical seconds (default CLAIM_HEARTBEAT_POLL_SECONDS). */
	pollSeconds?: number;
	/** Per-poll bail-out (fire-claim loss). True ⇒ abort the watch: the
	 * interrupt is issued, settlement awaited, and `aborted` reported —
	 * the caller must then NEVER write results (the winner owns them). May
	 * be async; the loop awaits it between polls. */
	shouldAbort?: (nowSeconds: number) => boolean | Promise<boolean>;
}

/**
 * Run `exec` under the claim-heartbeat watch. Polls every pollSeconds of
 * LOGICAL time (via the injected clock's sleepMs): while the executor is
 * pending, evaluates shouldAbort(now); true ⇒ issue the hard interrupt,
 * mark aborted, and still AWAIT settlement so the abort has landed before
 * callers persist results.
 */
export async function runWithClaimHeartbeat<T>(
	options: ClaimHeartbeatOptions<T>,
): Promise<ClaimHeartbeatResult<T>> {
	const pollSeconds = options.pollSeconds ?? CLAIM_HEARTBEAT_POLL_SECONDS;
	if (options.shouldAbort === undefined) {
		const result = await options.exec();
		return { result };
	}
	let execSettled = false;
	const execPromise = options.exec().then(
		(value) => {
			execSettled = true;
			return value;
		},
		(err) => {
			execSettled = true;
			throw err;
		},
	);
	let aborted = false;
	for (;;) {
		if (execSettled) break;
		await options.clock.sleepMs(pollSeconds * 1000);
		if (execSettled) break;
		const now = options.clock.nowSeconds();
		if ((await options.shouldAbort?.(now)) === true) {
			aborted = true;
			await options.interrupt();
			break;
		}
	}
	const result = (await execPromise) as T;
	if (!aborted) return { result };
	return { result, aborted };
}
