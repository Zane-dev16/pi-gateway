// pi_embedded/cron/inactivity.ts — the TRUE-inactivity runaway bound.
//
// Hermes anchors (READ-ONLY reference):
//   cron/scheduler.py:_cron_inactivity_seconds → resolveInactivityLimitSeconds
//   cron/scheduler.py run_job inactivity monitor (lines ~6049–6180) →
//     runWithInactivityBound
//   cron/scheduler.py:request_hard_interrupt(agent, "Cron job timed out
//     (inactivity)")                            → onInterrupt callback
//
// Binding invariant (07 §5.2 runaway bound row): the bound is an INACTIVITY
// timeout, not a fixed wall — default 600s WITHOUT tool/API/stream activity
// ⇒ hard interrupt (`HERMES_CRON_TIMEOUT` env; 0 = unlimited; bad input =
// 600 with a warning). An ACTIVE job MAY legitimately run for hours. The
// "3-minute-class runaway protection" is expressed AS this inactivity bound.
//
// HARD RULE: all timing flows through the injected CronClock. No logic path
// reads a wall clock; tests drive polls with a manual clock and scripted
// probes (a probe reporting fresh activity keeps the job alive past any
// number of simulated hours; an idle probe trips exactly at the limit).

import type { CronClock } from "./clock.js";

export const DEFAULT_CRON_INACTIVITY_SECONDS = 600;
/** Poll granularity of the monitor loop (parity of scheduler._POLL_INTERVAL). */
export const INACTIVITY_POLL_SECONDS = 5;

/**
 * Parse HERMES_CRON_TIMEOUT (seconds) exactly like _cron_inactivity_seconds:
 * unset/empty → 600; unparsable → warn + 600; parsed value returned as-is
 * (0 and negatives mean UNLIMITED at the call sites that check > 0).
 */
export function resolveInactivityLimitSeconds(
	env: Record<string, string | undefined> = process.env,
	onWarn?: (raw: string) => void,
): number {
	const raw = (env["HERMES_CRON_TIMEOUT"] ?? "").trim();
	if (raw === "") return DEFAULT_CRON_INACTIVITY_SECONDS;
	const parsed = Number(raw);
	if (!Number.isFinite(parsed)) {
		onWarn?.(raw);
		return DEFAULT_CRON_INACTIVITY_SECONDS;
	}
	return parsed;
}

/** Activity probe seam (parity of agent.get_activity_summary's
 * seconds_since_activity). Production wiring composes the timestamp log with
 * pipeline-level signals as the runner grows them; behavior contracts inject
 * scripted probes. */
export interface InactivityProbe {
	/** Seconds of observed idleness at logical instant `nowSeconds`. */
	secondsSinceActivity(nowSeconds: number): number;
}

/**
 * Production activity stamp log: anything that observes job progress
 * (turn events, transcript writes, tool activity) calls touch(); the monitor
 * reads the age of the freshest stamp.
 */
export class TimestampActivityLog implements InactivityProbe {
	private lastActivitySeconds: number;

	constructor(startSeconds: number) {
		this.lastActivitySeconds = startSeconds;
	}

	touch(nowSeconds: number): void {
		this.lastActivitySeconds = nowSeconds;
	}

	secondsSinceActivity(nowSeconds: number): number {
		return Math.max(0, nowSeconds - this.lastActivitySeconds);
	}
}

export interface InactivityBoundResult<T> {
	/** Set when the executor settled before any breach. */
	result?: T;
	/** True when the inactivity limit fired and the interrupt was issued. */
	timedOut: boolean;
	/** True when shouldAbort bailed the watch (claim-loss shape). */
	aborted?: boolean;
	/** Idle seconds observed at breach time (diagnostics parity). */
	idleAtBreach?: number;
}

export interface InactivityBoundOptions<T> {
	/** The job body (agent turn(s)); must settle once abort() lands. */
	exec: () => Promise<T>;
	/** Hard-interrupt analogue (session.abort() / request_hard_interrupt). */
	interrupt: () => boolean | Promise<boolean> | void | Promise<void>;
	/** Activity source; null ⇒ treated as unlimited regardless of the limit. */
	probe: InactivityProbe | null;
	/**
	 * Limit in seconds; <= 0 ⇒ unlimited (HERMES_CRON_TIMEOUT=0 parity).
	 * Unlimited still honors claim-loss polling upstream — only the
	 * inactivity watchdog is disabled.
	 */
	limitSeconds: number;
	clock: CronClock;
	/** Poll cadence in logical seconds (default INACTIVITY_POLL_SECONDS). */
	pollSeconds?: number;
	/** Extra per-poll bail-out (fire-claim loss). True ⇒ abort the watch:
	 * the interrupt is issued, settlement awaited, and `aborted` reported —
	 * the caller must then NEVER write results (the winner owns them). May
	 * be async; the loop awaits it between polls. */
	shouldAbort?: (nowSeconds: number) => boolean | Promise<boolean>;
}

/**
 * Run `exec` under the true-inactivity watchdog. Polls every pollSeconds of
 * LOGICAL time (via the injected clock's sleepMs): while the executor is
 * pending, computes idle = probe.secondsSinceActivity(now); idle >= limit ⇒
 * issue the hard interrupt, mark timedOut, and still AWAIT settlement so the
 * abort has landed before callers persist results. An active job never trips
 * no matter how long it runs — that is the whole point of the bound.
 */
export async function runWithInactivityBound<T>(
	options: InactivityBoundOptions<T>,
): Promise<InactivityBoundResult<T>> {
	const pollSeconds = options.pollSeconds ?? INACTIVITY_POLL_SECONDS;
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

	if (options.probe === null && options.shouldAbort === undefined) {
		const result = await execPromise;
		return { result, timedOut: false };
	}

	// HERMES_CRON_TIMEOUT=0 disables ONLY the inactivity watchdog — claim-loss
	// polling keeps running (parity: unlimited jobs still heartbeat their
	// run claim and abort when another tick takes over).
	const probe = options.probe;
	const inactivityEnabled = probe !== null && options.limitSeconds > 0;
	let timedOut = false;
	let aborted = false;
	let idleAtBreach: number | undefined;
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
		if (inactivityEnabled && probe !== null) {
			const idle = probe.secondsSinceActivity(now);
			if (idle >= options.limitSeconds) {
				timedOut = true;
				idleAtBreach = idle;
				await options.interrupt();
				break;
			}
		}
	}
	const result = (await execPromise) as T;
	if (!timedOut && !aborted) return { result, timedOut: false };
	return {
		result,
		timedOut,
		...(aborted ? { aborted } : {}),
		...(idleAtBreach !== undefined ? { idleAtBreach } : {}),
	};
}
