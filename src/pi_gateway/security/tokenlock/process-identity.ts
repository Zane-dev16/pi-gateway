// tokenlock/process-identity — PID + process START TIME fingerprint used by
// the scoped token locks as a PID-reuse guard (06 §5).
//
// Port of gateway/status.py process primitives (READ-ONLY Hermes reference;
// semantics ported, no code vendored):
//   status.py:_get_process_start_time → getProcessStartTime  (/proc/<pid>/stat
//                                     field 22 — clock ticks since boot)
//   status.py:_pid_exists             → isProcessAlive        (no-signal probe)
//   status.py:_read_process_cmdline   → readProcessCmdline    (/proc/<pid>/cmdline)
//   acquire_scoped_lock stopped-state rung → readProcessState / isProcessStopped
//
// The (pid, start_time) pair uniquely identifies a process on this host: a
// recycled PID yields a different start time and is NEVER mistaken for the
// original holder. Only same-source equality matters (06 §5 stale ladder).
//
// PLATFORM GATING (documented per task contract; host-honest tests only):
// - Linux (/proc available): full fidelity — start time, cmdline, and process
//   state are all readable from /proc. This is the supported host.
// - Windows: no /proc. getProcessStartTime returns null, state reads return
//   null, and the staleness ladder degrades to the kernel liveness probe
//   (process.kill(pid, 0), which Node maps to ESRCH for dead pids on all
//   platforms). Callers must treat "start_time unavailable" via the
//   cmdline-oracle rung of the ladder (token-lock.ts), never guess.
// - macOS/other POSIX without /proc: same degradation path as Windows; the
//   liveness probe still works (POSIX kill(0)).

import { readFileSync } from "node:fs";

/** `/proc/<pid>/stat` field 22 — start time in clock ticks since boot. */
export function getProcessStartTime(pid: number): number | null {
	if (!Number.isInteger(pid) || pid <= 0) return null;
	try {
		// comm ("(executable name)") may contain spaces AND parentheses —
		// split AFTER the final ')' and index from there. After it, fields
		// restart at #3 (state); field 22 lands at offset 22 - 3 = 19.
		const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
		const closeParen = stat.lastIndexOf(")");
		if (closeParen === -1) return null;
		const after = stat.slice(closeParen + 1).trim();
		const fields = after.split(" ");
		const raw = fields[19];
		if (raw === undefined) return null;
		const parsed = Number.parseInt(raw, 10);
		return Number.isFinite(parsed) ? parsed : null;
	} catch {
		return null;
	}
}

/** Process state letter from `/proc/<pid>/status`, or null when unreadable. */
export function readProcessState(pid: number): string | null {
	if (!Number.isInteger(pid) || pid <= 0) return null;
	try {
		const status = readFileSync(`/proc/${pid}/status`, "utf8");
		for (const line of status.split("\n")) {
			if (line.startsWith("State:")) {
				const letter = line.split(/\s+/)[1];
				return letter ?? null;
			}
		}
		return null;
	} catch {
		return null;
	}
}

/** True when the live process exists but holds nothing runnable: a zombie has
 * exited and will never release its credential cleanly. */
export function isProcessZombie(pid: number): boolean {
	return readProcessState(pid) === "Z";
}

/**
 * Stopped processes (Ctrl+Z / SIGTSTP / tracer stop, state T or t) still pass
 * liveness probes but are not actually running — the stale ladder treats them
 * as stale so an explicit replace can proceed (status.py acquire_scoped_lock
:: stopped-state rung).
 */
export function isProcessStopped(pid: number): boolean {
	const state = readProcessState(pid);
	return state === "T" || state === "t";
}

/**
 * Conservative kernel liveness probe. ESRCH proves death; every other answer
 * (including EPERM for unprivileged probes and zombies that still occupy the
 * PID slot) is doubt. Zombies are resolved one level up: isProcessAlive
 * consults the state byte where /proc exists, because a zombie will never
 * release the lock file it recorded.
 */
export function probeSignaledDead(pid: number): boolean {
	if (!Number.isInteger(pid) || pid <= 0) return false;
	try {
		process.kill(pid, 0);
		return false;
	} catch (err) {
		return (err as NodeJS.ErrnoException).code === "ESRCH";
	}
}

/** Liveness = signal probe alive AND (on Linux) not a zombie. Any doubt ⇒
 * alive — the caller keeps refusing while the record could still belong to a
 * running holder (fail-closed, parity with pi_state/leases.ts discipline). */
export function isProcessAlive(pid: number): boolean {
	if (probeSignaledDead(pid)) return false;
	if (isProcessZombie(pid)) return false;
	return true;
}

/** Process command line as a single space-separated string, or null. */
export function readProcessCmdline(pid: number): string | null {
	if (!Number.isInteger(pid) || pid <= 0) return null;
	try {
		const raw = readFileSync(`/proc/${pid}/cmdline`);
		if (raw.length === 0) return null;
		return raw.toString("utf8").replaceAll("\u0000", " ").trim() || null;
	} catch {
		return null;
	}
}
