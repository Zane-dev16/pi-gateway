// restart.ts — restart-per-kind stage (08 §7: fleet-wide, drain-first).
//
// Binding rules ported:
//   - FLEET-WIDE: EVERY unit restarts, never only the invoking profile —
//     siblings left on stale modules were historically the largest repeat-bug
//     class (08 §7; hermes_cli/update_cmd.py restart phase).
//   - DRAIN-FIRST: every launcher resolves BEFORE any drain signal flies;
//     drained workers disappear during the wait, survivors stop AFTER it;
//     the drain timeout has a sane floor.
//   - PER-UNIT FAILURE ISOLATION: one unit's failure never aborts the rest
//     (#68523).
//   - FAIL CLOSED (#78574): an escaped phase with unknown state fails the
//     update — survivors unprovable ⇒ assume stale; gateways stopped without
//     a verified replacement count as incomplete unless provably nothing ran
//     before (update_cmd.py:_restart_phase_failure_is_incomplete).
//   - Desktop-spawned serve is OUT of updater scope: reported, not restarted
//     (08 §7 table).
//
// Drain transport: SIGUSR1 graceful-drain wiring where supported (08 §7
// systemd row). The updater signals the PID resolved through CANONICAL
// process matchers only — never argv-substring inference (08 §9).

import type { GatewayClock } from "./clock.js";
import { systemClock } from "./clock.js";

export type SupervisorKind = "systemd" | "launchd" | "desktop" | "manual";

/** One restartable gateway unit on this host (fleet member). */
export interface RestartUnit {
	profile: string;
	supervisor: SupervisorKind;
	/** Live PID of the unit's gateway process (canonical-matcher derived). */
	pid: number | null;
}

export interface UnitRestartTrace {
	profile: string;
	/** Launcher resolved BEFORE draining (drain-first precondition). */
	resolved: boolean;
	resolveError: string | null;
	/** Drain signal delivered. */
	drainSignaled: boolean;
	/** Worker disappeared during the drain window. */
	drained: boolean;
	/** Survivor stopped after the drain window elapsed. */
	stoppedAfterWindow: boolean;
	error: string | null;
}

export interface RestartStageResult {
	outcome: "completed" | "incomplete";
	preRestartPids: number[] | null;
	survivingPids: number[] | null;
	units: UnitRestartTrace[];
	reason: string | null;
}

/** Sane floor for the drain wait (08 §7: "drain timeout has a sane floor"). */
export const MIN_DRAIN_TIMEOUT_MS = 2_000;

export interface RestartPorts {
	/** Liveness probe. Default: kill(pid, 0). */
	liveness?(pid: number): boolean;
	/** Signal delivery. Default: process.kill. Injectable for pure tests. */
	signal?(pid: number, signal: "SIGUSR1" | "SIGTERM"): void;
	clock?: GatewayClock;
	drainTimeoutMs?: number;
	pollIntervalMs?: number;
}

function defaultLiveness(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

function defaultSignal(pid: number, signal: "SIGUSR1" | "SIGTERM"): void {
	try {
		process.kill(pid, signal);
	} catch {
		/* per-unit isolation: signal races are this unit's failure alone */
	}
}

/**
 * Fail-closed verdict for an escaped/finished restart phase (verbatim port of
 * update_cmd.py:_restart_phase_failure_is_incomplete):
 *   - surviving null        — probe could not determine state ⇒ assume stale;
 *   - surviving non-empty   — a gateway still runs pre-update code;
 *   - surviving empty       — proof of safety ONLY when provably nothing ran
 *     before (pre_restart_pids known-empty); otherwise stopped-without-
 *     verified-replacement ⇒ incomplete.
 */
export function restartPhaseFailureIsIncomplete(
	surviving: readonly number[] | null,
	preRestartPids: readonly number[] | null,
): boolean {
	if (surviving === null || surviving.length > 0) return true;
	return preRestartPids === null || preRestartPids.length > 0;
}

async function waitForDeath(
	pids: readonly number[],
	liveness: (pid: number) => boolean,
	clock: GatewayClock,
	timeoutMs: number,
	pollMs: number,
): Promise<number[]> {
	const deadline = clock.nowSeconds() + timeoutMs / 1000;
	let alive = pids.filter(liveness);
	while (alive.length > 0 && clock.nowSeconds() < deadline) {
		await clock.sleepMs(pollMs);
		alive = pids.filter(liveness);
	}
	return alive;
}

/**
 * Restart the WHOLE fleet drain-first. Per-unit failures isolate; the phase
 * fails closed unless every pre-live unit provably drained or was verifiably
 * replaced.
 */
export async function restartFleet(
	units: readonly RestartUnit[],
	ports: RestartPorts = {},
): Promise<RestartStageResult> {
	const liveness = ports.liveness ?? defaultLiveness;
	const signalFn = ports.signal ?? defaultSignal;
	const clock = ports.clock ?? systemClock;
	const drainTimeoutMs = Math.max(
		ports.drainTimeoutMs ?? MIN_DRAIN_TIMEOUT_MS,
		MIN_DRAIN_TIMEOUT_MS,
	);
	const pollMs = ports.pollIntervalMs ?? 50;

	const trace: UnitRestartTrace[] = [];
	// Drain-first precondition: resolve EVERY launcher before ANY signal.
	const resolvable: RestartUnit[] = [];
	for (const unit of units) {
		if (unit.supervisor === "desktop") {
			// Out of updater scope — reported in the plan, not restarted (§7).
			trace.push({
				profile: unit.profile,
				resolved: false,
				resolveError:
					"desktop-spawned serve respawns out of updater scope (reported, not restarted)",
				drainSignaled: false,
				drained: false,
				stoppedAfterWindow: false,
				error: null,
			});
			continue;
		}
		if (
			unit.pid === null ||
			!Number.isInteger(unit.pid) ||
			!liveness(unit.pid)
		) {
			trace.push({
				profile: unit.profile,
				resolved: false,
				resolveError: "no live PID resolvable at plan time",
				drainSignaled: false,
				drained: false,
				stoppedAfterWindow: false,
				error: null,
			});
			continue;
		}
		resolvable.push(unit);
		trace.push({
			profile: unit.profile,
			resolved: true,
			resolveError: null,
			drainSignaled: false,
			drained: false,
			stoppedAfterWindow: false,
			error: null,
		});
	}

	const preRestartPids = resolvable.map((unit) => unit.pid as number);
	const preStateReadable = true; // resolvable units were liveness-proven above

	const drainedPids: number[] = [];
	let phaseEscaped = false;
	for (const unit of resolvable) {
		const entry = trace.find((t) => t.profile === unit.profile)!;
		try {
			signalFn(unit.pid as number, "SIGUSR1");
			entry.drainSignaled = true;
			drainedPids.push(unit.pid as number);
		} catch (error) {
			entry.error = error instanceof Error ? error.message : String(error);
			// A resolved live unit we could NOT signal is unaccounted for — the
			// phase no longer knows its fate (#78574 class).
			phaseEscaped = true;
		}
	}

	// Drained workers disappear DURING the wait…
	const stillAliveAfterWindow = await waitForDeath(
		drainedPids,
		liveness,
		clock,
		drainTimeoutMs,
		pollMs,
	);
	for (const unit of resolvable) {
		const entry = trace.find((t) => t.profile === unit.profile)!;
		if (!entry.drainSignaled) continue;
		if (!stillAliveAfterWindow.includes(unit.pid as number)) {
			entry.drained = true;
		}
	}
	// …survivors STOP after the window.
	for (const pid of stillAliveAfterWindow) {
		const unit = resolvable.find((u) => u.pid === pid);
		if (!unit) continue;
		const entry = trace.find((t) => t.profile === unit.profile)!;
		try {
			signalFn(pid, "SIGTERM");
			entry.stoppedAfterWindow = true;
		} catch (error) {
			entry.error = error instanceof Error ? error.message : String(error);
		}
	}

	// Final survivor probe over the ORIGINAL pids this phase accounted for.
	const survivors = drainedPids.filter(liveness);

	// Verdict split, matching the Hermes structure:
	//   NORMAL completion — every accounted unit provably drained ⇒ completed.
	//   Replacement proof is the VERIFY stage's job (fleet sha matrix), not
	//   the restart phase's. ESCAPED/unknown state — apply the verbatim
	//   fail-closed table (_restart_phase_failure_is_incomplete).
	let outcome: "completed" | "incomplete";
	let reason: string | null;
	if (
		!phaseEscaped &&
		survivors.length === 0 &&
		drainedPids.length === preRestartPids.length
	) {
		outcome = "completed";
		reason = null;
	} else if (!phaseEscaped && survivors.length > 0) {
		outcome = "incomplete";
		reason = incompleteReason(survivors);
	} else {
		const tableVerdict = restartPhaseFailureIsIncomplete(
			survivors,
			preStateReadable ? preRestartPids : null,
		);
		outcome = tableVerdict ? "incomplete" : "completed";
		reason = tableVerdict ? incompleteReason(survivors) : null;
	}
	return {
		outcome,
		preRestartPids: preStateReadable ? preRestartPids : null,
		survivingPids: survivors,
		units: trace,
		reason,
	};
}

function incompleteReason(survivingPids: readonly number[] | null): string {
	if (survivingPids === null) {
		return "survivor probe could not determine fleet state — assumed stale";
	}
	if (survivingPids.length > 0) {
		return "gateways still running after drain+stop — assumed pre-update code";
	}
	return "gateways stopped without a verified replacement";
}
