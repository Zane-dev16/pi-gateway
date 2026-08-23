// pi_gateway/lifecycle/process-info.ts — PID liveness + start-time probes.
//
// Spec: /root/pi-gateway/08-operations.md §1.1 stage 4 / §9 (process identity),
// 01-architecture.md §3.2 (duplicate-instance guard). Hermes anchors
// (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/status.py:_pid_exists              → processAlive (ESRCH-only death,
//                                                zombie = dead, doubt = alive)
//   gateway/status.py:_get_process_start_time  → processStartTime (/proc stat
//                                                field 22; null off-Linux)
//
// PID reuse guard: a recorded start_time that mismatches the LIVE process's
// start time means the original holder exited and the OS recycled the PID —
// never signal or wait on the replacement (status.py:_scoped_lock_owner_state).
// Off-Linux there is no start-time source: every consumer must fall back to
// PID equality alone, bounded by marker TTLs (documented rule in 08 §1.2 /
// status.py:_consume_pid_marker_for_self — "when either is unknown, fall back
// to PID equality alone").

import { existsSync, readFileSync } from "node:fs";

export interface ProcessInfo {
	/** ESRCH-proven or zombie-proven death ⇒ false; any doubt ⇒ true. */
	alive: boolean;
	/** /proc stat field 22 (clock ticks at sched switch-in); null off-Linux. */
	startTime: number | null;
}

/**
 * Conservative kernel liveness probe (parity of status.py:_pid_exists):
 * - POSIX `kill(pid, 0)` succeeds → alive UNLESS Linux /proc proves zombie
 *   (a defunct process still answers kill(0) but can never run again —
 *   treating it alive made `--replace` wait forever then abort, #42126).
 * - ProcessLookupError/ESRCH → provably dead.
 * - EPERM / anything else → exists-but-doubtful → ALIVE (TTLs remain the
 *   recovery path; a false "dead" steals locks and double-runs gateways).
 */
export function probeProcess(pid: number): ProcessInfo {
	if (!Number.isInteger(pid) || pid <= 0) {
		return { alive: false, startTime: readStartTime(pid) };
	}
	const procStat = readProcStat(pid);
	if (procStat !== null && procStat.state === "Z") {
		return { alive: false, startTime: procStat.startTime };
	}
	try {
		process.kill(pid, 0);
		return { alive: true, startTime: procStat?.startTime ?? null };
	} catch (err) {
		const code = (err as NodeJS.ErrnoException).code;
		if (code === "ESRCH")
			return { alive: false, startTime: procStat?.startTime ?? null };
		return { alive: true, startTime: procStat?.startTime ?? null }; // EPERM etc. → doubt → alive
	}
}

/** Convenience wrapper: true only when provably gone (or proven zombie). */
export function processAlive(pid: number): boolean {
	return probeProcess(pid).alive;
}

/**
 * Process start time (Linux /proc/<pid>/stat field 22, raw clock ticks —
 * raw ticks are fine: consumers only test EQUALITY against a recorded value).
 * Returns null on non-Linux or unreadable /proc — callers MUST treat null as
 * "unknown" per the documented fallback rule (see module header).
 */
export function processStartTime(pid: number): number | null {
	return readStartTime(pid);
}

function readStartTime(pid: number): number | null {
	return readProcStat(pid)?.startTime ?? null;
}

interface ProcStat {
	state: string;
	startTime: number;
}

/**
 * Parse /proc/<pid>/stat. comm may contain spaces/parens, so split after the
 * LAST ')'; post-')' token[0] is state (field 3), token[19] is starttime
 * (field 22). Never throws.
 */
function readProcStat(pid: number): ProcStat | null {
	if (process.platform !== "linux") return null;
	try {
		const path = `/proc/${pid}/stat`;
		if (!existsSync(path)) return null;
		const text = readFileSync(path, "utf8");
		const close = text.lastIndexOf(")");
		if (close < 0) return null;
		const rest = text
			.slice(close + 1)
			.trim()
			.split(/\s+/);
		const state = rest[0] ?? "";
		const rawStart = rest[19];
		if (rawStart === undefined) return null;
		const startTime = Number.parseInt(rawStart, 10);
		if (!Number.isFinite(startTime)) return null;
		return { state, startTime };
	} catch {
		return null;
	}
}

/**
 * Do a recorded start_time and a live one AGREE for ownership purposes?
 * Both known → equality; either unknown → fall back to `pidMatches` alone
 * (the bounded-TTL rule cited in the module header).
 */
export function startTimeMatches(
	recorded: number | null | undefined,
	live: number | null | undefined,
	pidMatches: boolean,
): boolean {
	if (
		recorded === null ||
		recorded === undefined ||
		live === null ||
		live === undefined
	) {
		return pidMatches;
	}
	return recorded === live && pidMatches;
}
