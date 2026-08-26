// pi_gateway/lifecycle/boot-recovery.ts — boot choreography: clean-shutdown
// receipt branch vs unclean-exit recovery, stuck-loop suspension, and the
// resume-scheduling seam. Hermes anchors (READ-ONLY reference; semantics
// ported, no code vendored — gateway/run.py GatewayRunner.start):
//   `.clean_shutdown` consume-or-recover block
//     → runBootChoreography (clean receipt ⇒ SKIP session suspension; the
//       marker is consumed FIRST and a cleanup failure FAILS STARTUP CLOSED
//       so a later unclean exit can never masquerade as clean)
//   _recover_unclean_sessions
//     → hooks.recoverInterruptedTurns (exact durable turn markers) +
//       hooks.suspendRecentlyActive (120s recency fallback)
//   _suspend_stuck_loop_sessions (.restart_failure_counts, threshold 3, #7536)
//     → suspendStuckLoopSessions (selects keys ≥ STUCK_LOOP_THRESHOLD, hands
//       each to the suspend hook, then clears the file — counters restart
//       fresh after a suspension pass)
//
// The session-store operations behind the hooks land with the Phase-1 runner;
// the FILE mechanics (receipt + counters) are complete here so behavior is
// testable and the choreography order is binding now.

import {
	STUCK_LOOP_THRESHOLD,
	readRestartFailureCounts,
	restartFailureCountsPath,
} from "./shutdown.js";
import { unlinkSync } from "node:fs";
import type { Logger } from "./shutdown.js";

/** Session-store seams (injected once the runner wires them). */
export interface BootRecoveryHooks {
	/**
	 * Discard orphan active-turn markers after a CLEAN shutdown. RAISING fails
	 * startup closed (parity of _consume_clean_shutdown_marker).
	 */
	discardActiveTurnMarkers?: () => Promise<number>;
	/** Exact recovery of interrupted turns after an UNCLEAN exit. */
	recoverInterruptedTurns?: () => Promise<number>;
	/** Legacy recency fallback (120s window) for pre-marker turns. */
	suspendRecentlyActive?: () => Promise<number>;
	/** Suspension action for one stuck-loop session; false ⇒ not suspended. */
	suspendSession?: (sessionKey: string) => Promise<boolean> | boolean;
	/**
	 * Boot-time lifecycle sends (run.py:_await_startup_boot_sends): restart
	 * notification, home-channel startup notice, obligation redelivery.
	 * Runs AFTER adapters connected, BEFORE resume scheduling.
	 */
	bootSends?: () => Promise<void>;
	/**
	 * Auto-resume pass for restart-interrupted sessions
	 * (run.py:_schedule_resume_pending_sessions). Runs while the restore
	 * gate is still closed; the gate opens once this settles.
	 */
	scheduleResumePending?: () => Promise<number | void> | number | void;
}

export interface BootRecoveryReport {
	/** A `.clean_shutdown` receipt was present (suspension path skipped). */
	cleanShutdown: boolean;
	/** Orphan active-turn markers discarded on the clean path. */
	discardedMarkers: number;
	/** Sessions recovered by the exact-turn mechanism (unclean path). */
	exactRecovered: number;
	/** Sessions recovered by the recency fallback (unclean path). */
	fallbackRecovered: number;
	/** Sessions auto-suspended as stuck loops (#7536). */
	stuckSuspended: string[];
}

/**
 * Select sessions whose restart-failure count reached the stuck threshold.
 * Exported for contract tests; mirrors run.py's stuck_keys computation.
 */
export function selectStuckSessions(counts: Record<string, number>): string[] {
	return Object.entries(counts)
		.filter(([, v]) => v >= STUCK_LOOP_THRESHOLD)
		.map(([k]) => k);
}

/**
 * Suspend stuck-loop sessions (#7536): read `.restart_failure_counts`, hand
 * every key at/over the threshold to the suspend hook, then clear the file —
 * counters start fresh after a suspension pass. Without a suspend hook the
 * file is PRESERVED (clearing would silently lose the evidence); callers log
 * loudly instead.
 */
export async function suspendStuckLoopSessions(
	home: string,
	hooks: BootRecoveryHooks,
	log?: Logger,
): Promise<string[]> {
	const counts = readRestartFailureCounts(home);
	if (Object.keys(counts).length === 0) return [];
	const stuckKeys = selectStuckSessions(counts);
	if (stuckKeys.length === 0) return [];
	if (hooks.suspendSession === undefined) {
		log?.warn(
			"stuck-loop sessions detected but no suspension backend registered; counts preserved",
			{ count: stuckKeys.length },
		);
		return [];
	}
	const suspended: string[] = [];
	for (const key of stuckKeys) {
		try {
			if (await hooks.suspendSession(key)) suspended.push(key);
		} catch (err) {
			log?.warn("stuck-session suspension failed", {
				session_key: key,
				error: String(err),
			});
		}
	}
	try {
		unlinkSync(restartFailureCountsPath(home));
	} catch {
		/* best-effort — counters start fresh after a pass */
	}
	for (const key of suspended) {
		log?.warn("auto-suspended stuck session", {
			session_key: key,
			consecutive_restarts: counts[key],
		});
	}
	return suspended;
}

/**
 * Boot choreography part 1 (runs post-stage-9, before resume scheduling):
 * clean-receipt branch vs unclean recovery, then stuck-loop suspension.
 * FAIL-CLOSED: if consuming the clean receipt fails, startup must abort so
 * the old receipt can never mask a later unclean exit (run.py raises
 * RuntimeError("clean-start recovery cleanup failed")).
 */
export async function runBootChoreography(
	home: string,
	hooks: BootRecoveryHooks,
	options: { cleanShutdownMarkerExists: boolean; log?: Logger },
): Promise<BootRecoveryReport> {
	const report: BootRecoveryReport = {
		cleanShutdown: false,
		discardedMarkers: 0,
		exactRecovered: 0,
		fallbackRecovered: 0,
		stuckSuspended: [],
	};

	if (options.cleanShutdownMarkerExists) {
		report.cleanShutdown = true;
		if (hooks.discardActiveTurnMarkers !== undefined) {
			let discarded: number;
			try {
				discarded = await hooks.discardActiveTurnMarkers();
			} catch (err) {
				throw new Error(
					`clean-start recovery cleanup failed: ${String(err)} ` +
						"(refusing startup so the clean-exit receipt cannot mask an unclean exit)",
				);
			}
			report.discardedMarkers = discarded;
			if (discarded > 0) {
				options.log?.info("discarded orphan active-turn markers", {
					count: discarded,
				});
			}
		}
		options.log?.info(
			"previous gateway exited cleanly — skipping session suspension",
		);
	} else {
		try {
			report.exactRecovered = (await hooks.recoverInterruptedTurns?.()) ?? 0;
		} catch (err) {
			options.log?.warn("exact active-turn recovery failed", {
				error: String(err),
			});
		}
		try {
			report.fallbackRecovered = (await hooks.suspendRecentlyActive?.()) ?? 0;
		} catch (err) {
			options.log?.warn("legacy session recovery failed", {
				error: String(err),
			});
		}
		const recovered = report.exactRecovered + report.fallbackRecovered;
		if (recovered > 0) {
			options.log?.info(
				"marked in-flight sessions resumable from previous run",
				{
					exact: report.exactRecovered,
					legacy: report.fallbackRecovered,
				},
			);
		}
	}

	report.stuckSuspended = await suspendStuckLoopSessions(
		home,
		hooks,
		options.log,
	);
	return report;
}
