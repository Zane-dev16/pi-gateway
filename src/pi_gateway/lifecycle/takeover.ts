// pi_gateway/lifecycle/takeover.ts — duplicate-instance takeover handshake.
//
// Spec: /root/pi-gateway/08-operations.md §1.1 stage 4; 01-architecture.md
// §3.2 (step-by-step handshake, binding). Hermes anchors (READ-ONLY reference;
// semantics ported, no code vendored):
//   gateway/run.py:start_gateway (--replace block)
//     → performTakeover: marker BEFORE SIGTERM → bounded wait ≤10s @0.5s →
//       force → confirm reap → cleanup on give-up
//   gateway/status.py:_snapshot_gateway_children / reap_gateway_children
//     → snapshotProcessChildren / reapOrphanedChildren (POSIX-only parent
//       walk taken BEFORE the old process exits; skip children whose ppid
//       still names the parent — caller raced or was lied to)
//   gateway/run.py:shutdown_signal_handler (old-process half)
//     → markers.ts:consumeTakeoverMarkerForSelf exits 0 on the OLD side
//
// Permission-denied and still-alive-after-force both FAIL startup: the marker
// is cleared first — it is scoped to a specific target and must not grief an
// unrelated future shutdown.

import { readdirSync, readFileSync } from "node:fs";
import { clearTakeoverMarker, writeTakeoverMarker } from "./markers.js";
import { processAlive } from "./process-info.js";
import { forceRemovePidFile } from "./instance-guard.js";

export interface TakeoverOptions {
	selfPid?: number;
	/** Total graceful-wait budget after SIGTERM (spec default 10_000ms). */
	graceTimeoutMs?: number;
	/** Poll interval while waiting for exit (spec default 500ms). */
	pollIntervalMs?: number;
	/** Post-SIGKILL confirmation budget (parity 20 × 0.25s = 5s). */
	forceConfirmTimeoutMs?: number;
	/** Termination primitive override (tests). Default process.kill. */
	terminate?: (pid: number, signal: NodeJS.Signals) => void;
	sleep?: (ms: number) => Promise<void>;
	log?: (
		level: "info" | "warn" | "error",
		message: string,
		meta?: Record<string, unknown>,
	) => void;
}

export type TakeoverFailure =
	| "signal_permission_denied"
	| "survived_force_kill";

export interface TakeoverResult {
	ok: boolean;
	failure: TakeoverFailure | null;
	markerWritten: boolean;
	childrenSnapshot: number[];
}

async function defaultSleep(ms: number): Promise<void> {
	await new Promise<void>((resolve) => setTimeout(resolve, ms));
}

function defaultTerminate(pid: number, signal: NodeJS.Signals): void {
	process.kill(pid, signal);
}

// ---------------------------------------------------------------------------
// POSIX child accounting (#19471 class: orphaned adapter subprocesses keep
// holding scoped token locks against the replacement)
// ---------------------------------------------------------------------------

/**
 * Best-effort snapshot of `pid`'s live children via the Linux /proc parent
 * walk — MUST be taken while the old gateway is still alive; once it exits,
 * orphans are reparented and unfindable (01 §3.2 step 3). Non-Linux returns
 * [] (Windows force-kill tree-kills via its own mechanism; macOS degrades to
 * no-reap exactly like Hermes' psutil-missing path). Never raises.
 */
export function snapshotProcessChildren(pid: number): number[] {
	if (process.platform !== "linux") return [];
	const children: number[] = [];
	try {
		for (const entry of readdirSync("/proc")) {
			if (!/^\d+$/.test(entry)) continue;
			const childPid = Number.parseInt(entry, 10);
			if (childPid === pid) continue;
			try {
				const stat = readFileSync(`/proc/${entry}/stat`, "utf8");
				const close = stat.lastIndexOf(")");
				if (close < 0) continue;
				const rest = stat
					.slice(close + 1)
					.trim()
					.split(/\s+/);
				const state = rest[0] ?? "";
				if (state === "Z") continue; // defunct children need no reaping
				const ppid = Number.parseInt(rest[1] ?? "", 10);
				if (ppid === pid) children.push(childPid);
			} catch {
				/* raced exit — skip */
			}
		}
	} catch {
		return [];
	}
	return children;
}

/**
 * Best-effort reap AFTER the main PID is confirmed dead (01 §3.2 step 4
 * parity of reap_gateway_children). A child whose CURRENT ppid still equals
 * the parent is SKIPPED: that means the parent is in fact alive and the child
 * is not an orphan. SIGTERM first, bounded wait, SIGKILL for survivors.
 * Never raises; returns the count signalled.
 */
export async function reapOrphanedChildren(
	children: readonly number[],
	parentPid: number,
	options: Pick<TakeoverOptions, "sleep" | "terminate"> = {},
): Promise<number> {
	if (children.length === 0 || process.platform === "win32") return 0;
	const terminate = options.terminate ?? defaultTerminate;
	const sleep = options.sleep ?? defaultSleep;
	let signalled = 0;
	const survivors: number[] = [];
	for (const child of children) {
		try {
			// Identity-aware liveness: probeProcess reads /proc directly, so a
			// recycled PID is never signalled (psutil.is_running parity).
			if (!processAlive(child)) continue;
			if (readPpid(child) === parentPid) continue; // parent alive ⇒ not an orphan
			terminate(child, "SIGTERM");
			signalled++;
			survivors.push(child);
		} catch {
			/* gone already */
		}
	}
	if (survivors.length > 0) await sleep(1500);
	for (const child of survivors) {
		try {
			if (processAlive(child) && readPpid(child) !== parentPid) {
				terminate(child, "SIGKILL");
			}
		} catch {
			/* gone already */
		}
	}
	return signalled;
}

function readPpid(pid: number): number | null {
	try {
		const stat = readFileSync(`/proc/${pid}/stat`, "utf8");
		const close = stat.lastIndexOf(")");
		if (close < 0) return null;
		const [, ppidRaw] = stat
			.slice(close + 1)
			.trim()
			.split(/\s+/);
		return Number.parseInt(ppidRaw ?? "", 10);
	} catch {
		return null;
	}
}

// ---------------------------------------------------------------------------
// The handshake itself (01 §3.2 steps 1–5)
// ---------------------------------------------------------------------------

/**
 * Replace the live instance at `existingPid` with the current process:
 *  1. write a takeover marker naming the old PID (+start time) — best-effort,
 *     proceed even if it fails (the old side then treats SIGTERM as
 *     unexpected, which only changes ITS exit code, not our progress);
 *  2. snapshot the old gateway's children BEFORE signalling;
 *  3. SIGTERM; wait up to graceTimeoutMs polling at pollIntervalMs;
 *  4. still alive ⇒ SIGKILL, then CONFIRM the kill reaped before declaring
 *     success (#19471: blindly clearing metadata spawns duplicate gateways);
 *  5. permission denied / survived-force ⇒ clear the marker (scoped to this
 *     target) and fail startup.
 */
export async function performTakeover(
	home: string,
	existingPid: number,
	options: TakeoverOptions = {},
): Promise<TakeoverResult> {
	const graceTimeoutMs = options.graceTimeoutMs ?? 10_000;
	const pollIntervalMs = Math.max(10, options.pollIntervalMs ?? 500);
	const forceConfirmTimeoutMs = options.forceConfirmTimeoutMs ?? 5_000;
	const terminate = options.terminate ?? defaultTerminate;
	const sleep = options.sleep ?? defaultSleep;
	const log = options.log;

	// Step 2 (marker BEFORE signal — the old side's classifier depends on it).
	const markerWritten = writeTakeoverMarker(home, existingPid);
	if (!markerWritten) {
		log?.("warn", "could not write takeover marker; proceeding best-effort", {
			target_pid: existingPid,
		});
	}

	// Step 3: snapshot children while the old gateway is STILL ALIVE.
	const childrenSnapshot = snapshotProcessChildren(existingPid);

	// Step 4: SIGTERM + bounded wait.
	try {
		terminate(existingPid, "SIGTERM");
	} catch (err) {
		const code = (err as NodeJS.ErrnoException).code;
		if (code === "ESRCH") {
			// Already gone between probe and signal — takeover trivially done.
			clearTakeoverMarker(home);
			return { ok: true, failure: null, markerWritten, childrenSnapshot };
		}
		// Step 5: give-up path MUST clear the marker (scoped to this target).
		clearTakeoverMarker(home);
		log?.("error", "permission denied signalling old gateway; cannot replace", {
			target_pid: existingPid,
			error: String(err),
		});
		return {
			ok: false,
			failure: "signal_permission_denied",
			markerWritten,
			childrenSnapshot,
		};
	}

	const exitedWithin = async (budgetMs: number): Promise<boolean> => {
		const deadline = Date.now() + budgetMs;
		for (;;) {
			if (!processAlive(existingPid)) return true;
			if (Date.now() >= deadline) return false;
			await sleep(pollIntervalMs);
		}
	};

	let exited = await exitedWithin(graceTimeoutMs);
	if (!exited) {
		log?.("warn", "old gateway did not exit after SIGTERM; sending SIGKILL", {
			target_pid: existingPid,
		});
		try {
			terminate(existingPid, "SIGKILL");
		} catch (err) {
			if ((err as NodeJS.ErrnoException).code !== "ESRCH") {
				clearTakeoverMarker(home);
				return {
					ok: false,
					failure: "signal_permission_denied",
					markerWritten,
					childrenSnapshot,
				};
			}
		}
		exited = await exitedWithin(forceConfirmTimeoutMs);
		if (!exited) {
			// Still appears alive after SIGKILL — aborting replacement avoids a
			// duplicate gateway fighting over one substrate (#19471).
			clearTakeoverMarker(home);
			log?.(
				"error",
				"old gateway still alive after SIGKILL; aborting replacement",
				{
					target_pid: existingPid,
				},
			);
			return {
				ok: false,
				failure: "survived_force_kill",
				markerWritten,
				childrenSnapshot,
			};
		}
	}

	// Old gateway confirmed dead — reap orphaned descendants (POSIX), clean
	// the PID file it can no longer remove, and drop any unconsumed marker
	// (SIGKILL'd before its handler could read it).
	try {
		await reapOrphanedChildren(childrenSnapshot, existingPid, {
			sleep,
			terminate,
		});
	} catch {
		/* best-effort — never blocks the replacement */
	}
	forceRemovePidFile(home);
	clearTakeoverMarker(home);

	log?.("info", "replaced previous gateway instance", {
		old_pid: existingPid,
		reaped_children: childrenSnapshot.length,
	});
	return { ok: true, failure: null, markerWritten, childrenSnapshot };
}
