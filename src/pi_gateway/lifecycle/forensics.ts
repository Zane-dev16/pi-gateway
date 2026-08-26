// pi_gateway/lifecycle/forensics.ts — fast (<10ms) shutdown-context probe +
// detached heavyweight diagnostic. 08 §1.3(b): forensics preserves EVIDENCE
// when the killer is invisible; the signal path must never block.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored —
// gateway/shutdown_forensics.py):
//   snapshot_shutdown_context → snapshotShutdownContext
//     (signal name/num; pid/ppid + parent proc summary; systemd detection via
//     INVOCATION_ID / JOURNAL_STREAM / ppid==1; loadavg_1m; TracerPid;
//     takeover/planned-stop marker presence + whether a takeover names THIS
//     pid — sibling --replace race hint; pure stdlib+/proc, never raises)
//   format_context_for_log    → formatContextForLog (one key=value line,
//     parent_cmdline LAST — often the longest field)
//   spawn_async_diagnostic    → spawnAsyncDiagnostic (detached subprocess with
//     start_new_session so it survives KillMode=control-group teardown;
//     bounded by its own timeout; Windows skipped)

import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { loadavg } from "node:os";
import { spawn as nodeSpawn } from "node:child_process";

/** Injected /proc-style reader: returns file body or null when unreadable. */
export type ProcReader = (path: string) => string | null;

export interface ForensicsOptions {
	/** Received signal name (e.g. "SIGTERM") or null for programmatic stops. */
	signal?: string | null;
	/** Profile home used for the marker-presence hints (default PI_HOME). */
	home?: string;
	selfPid?: number;
	environ?: Record<string, string | undefined>;
	procReader?: ProcReader;
	nowMs?: () => number;
}

function defaultProcReader(path: string): string | null {
	try {
		if (!existsSync(path)) return null;
		return readFileSync(path, "utf8");
	} catch {
		return null;
	}
}

/**
 * /proc summary of one pid: name/state/uid/cmdline(≤300 chars). Never raises;
 * degrades to `{}` off-Linux or when the proc entry is gone.
 * (shutdown_forensics.py:_proc_summary / _read_proc_cmdline.)
 */
export function procSummary(
	pid: number,
	reader: ProcReader = defaultProcReader,
): Record<string, unknown> {
	const out: Record<string, unknown> = {};
	const stat = reader(`/proc/${pid}/stat`);
	if (stat !== null) {
		const close = stat.lastIndexOf(")");
		if (close >= 0) {
			const head = stat.slice(0, close);
			const open = head.indexOf("(");
			if (open >= 0) {
				out["name"] = head.slice(open + 1);
				const tail = stat
					.slice(close + 1)
					.trim()
					.split(/\s+/);
				out["state"] = tail[0] ?? "";
			}
		}
	}
	const status = reader(`/proc/${pid}/status`);
	if (status !== null) {
		for (const line of status.split("\n")) {
			if (line.startsWith("Uid:")) {
				out["uid"] = line.split(/\s+/)[1] ?? "";
				break;
			}
		}
	}
	const cmdline = reader(`/proc/${pid}/cmdline`);
	if (cmdline !== null) {
		const joined = cmdline.replace(/\0/g, " ").trim();
		if (joined !== "") out["cmdline"] = joined.slice(0, 300);
	}
	return out;
}

const SIGNAL_NUMBERS: Readonly<Record<string, number>> = {
	SIGTERM: 15,
	SIGINT: 2,
	SIGHUP: 1,
	SIGQUIT: 3,
	SIGUSR1: 10,
	SIGUSR2: 12,
};

/**
 * Fast (<10ms) snapshot of who/what is asking us to shut down. Pure stdlib +
 * /proc reads, never blocks on subprocesses, NEVER RAISES — every section is
 * individually best-effort (run.py:shutdown_signal_handler calls this first).
 */
export function snapshotShutdownContext(
	options: ForensicsOptions = {},
): Record<string, unknown> {
	const ctx: Record<string, unknown> = {};
	try {
		const nowMs = options.nowMs ?? Date.now;
		const selfPid = options.selfPid ?? process.pid;
		const environ = options.environ ?? process.env;
		const reader = options.procReader ?? defaultProcReader;
		ctx["ts"] = Math.floor(nowMs() / 1000);
		ctx["ts_monotonic"] = Number((nowMs() / 1000).toFixed(3));
		ctx["signal"] = options.signal ?? null;
		ctx["signal_num"] =
			options.signal !== undefined && options.signal !== null
				? (SIGNAL_NUMBERS[options.signal] ?? null)
				: null;
		let ppid = 0;
		try {
			ppid = process.ppid;
		} catch {
			ppid = 0;
		}
		ctx["pid"] = selfPid;
		ctx["ppid"] = ppid;
		ctx["parent"] = ppid > 0 ? procSummary(ppid, reader) : {};
		ctx["self"] = procSummary(selfPid, reader);

		// systemd context: INVOCATION_ID set in our env when started by a unit;
		// ppid==1 also signals init reaped+forwarded the SIGTERM.
		const invocationId = environ["INVOCATION_ID"];
		if (invocationId) ctx["systemd_invocation_id"] = invocationId;
		const journalStream = environ["JOURNAL_STREAM"];
		if (journalStream) ctx["systemd_journal_stream"] = journalStream;
		ctx["under_systemd"] = Boolean(invocationId) || ppid === 1;

		// High load points at "the box is crushed" rather than an external killer.
		try {
			ctx["loadavg_1m"] = loadavg()[0];
		} catch {
			/* not available on every platform */
		}

		// TracerPid ≠ 0 means a debugger/strace is attached ("phantom SIGKILL"
		// that turns out to be a manual gdb session).
		const status = reader(`/proc/${selfPid}/status`);
		if (status !== null) {
			for (const line of status.split("\n")) {
				if (line.startsWith("TracerPid:")) {
					const tracer = line.split(/\s+/)[1] ?? "";
					if (tracer !== "" && tracer !== "0") {
						const parsed = Number.parseInt(tracer, 10);
						ctx["tracer_pid"] = Number.isFinite(parsed) ? parsed : tracer;
					}
					break;
				}
			}
		}

		// Race-detection hint: a takeover marker on disk that does NOT name us
		// is a smoking gun for "another --replace instance is killing us".
		// Filenames mirror markers.ts; read raw so the signal path stays
		// import-light and can never mutate marker state. The for-self check is
		// whitespace-insensitive: our own atomic writer emits compact JSON while
		// Python writers emit spaced JSON (shutdown_forensics.py checks both).
		const home = options.home;
		if (home !== undefined) {
			try {
				const takeoverRaw = reader(join(home, ".gateway-takeover.json"));
				if (takeoverRaw !== null) {
					ctx["takeover_marker"] = takeoverRaw.slice(0, 300);
					ctx["takeover_marker_for_self"] = takeoverRaw
						.replace(/\s+/g, "")
						.includes(`"target_pid":${selfPid}`);
				}
				const plannedStopRaw = reader(join(home, ".gateway-planned-stop.json"));
				if (plannedStopRaw !== null) {
					ctx["planned_stop_marker"] = plannedStopRaw.slice(0, 300);
				}
			} catch {
				/* never raise from a signal handler */
			}
		}
	} catch {
		/* the snapshot itself must never break the signal path */
	}
	return ctx;
}

/**
 * One-line scannable rendering (parity of format_context_for_log):
 * `key=value` joined; parent_cmdline deliberately emitted LAST because it is
 * usually the longest field.
 */
export function formatContextForLog(ctx: Record<string, unknown>): string {
	const parts: string[] = [];
	const parent = ctx["parent"];
	const parentCmdline =
		typeof parent === "object" &&
		parent !== null &&
		typeof (parent as Record<string, unknown>)["cmdline"] === "string"
			? ((parent as Record<string, unknown>)["cmdline"] as string)
			: null;
	for (const [key, value] of Object.entries(ctx)) {
		if (key === "parent" || key === "self") continue;
		parts.push(
			`${key}=${typeof value === "string" ? value : JSON.stringify(value)}`,
		);
	}
	if (parentCmdline !== null) parts.push(`parent_cmdline=${parentCmdline}`);
	return parts.join(" ");
}

export interface SpawnDiagnosticOptions {
	timeoutSeconds?: number;
	spawn?: (
		cmd: string,
		args: readonly string[],
		opts: { detached: boolean; stdio: ["ignore", "inherit", "ignore"] },
	) => { unref?(): void } | null;
	logDirName?: string;
}

/**
 * Fire-and-forget ps-walk written to `<logDir>/gateway-shutdown-diag.log`
 * (parity of spawn_async_diagnostic). Detached (new session) so it survives
 * cgroup teardown; bounded by its own `timeout` wrapper; Windows skipped.
 * Returns the child PID or null on any failure. NEVER raises, never blocks.
 */
export function spawnAsyncDiagnostic(
	logDir: string,
	signalName: string,
	options: SpawnDiagnosticOptions = {},
): number | null {
	if (process.platform === "win32") return null; // no ps on the platform
	const timeoutSeconds = options.timeoutSeconds ?? 5;
	const logPath = join(
		logDir,
		options.logDirName ?? "gateway-shutdown-diag.log",
	);
	const script = [
		`echo '=== shutdown diagnostic @ ${signalName} ==='`,
		"echo '--- date ---'; date -u +%Y-%m-%dT%H:%M:%SZ",
		"echo '--- ps auxf (top 60 by cpu) ---'",
		"ps auxf --sort=-pcpu 2>/dev/null | head -60",
		"echo '--- /proc/loadavg ---'; cat /proc/loadavg 2>/dev/null || true",
		"echo '--- recent dmesg (oom/killed) ---'",
		"dmesg -T 2>/dev/null | tail -20 || true",
		"echo '=== end ==='",
	].join("; ");
	try {
		const child = options.spawn
			? options.spawn(
					"timeout",
					[`${Math.max(1, Math.floor(timeoutSeconds))}`, "bash", "-c", script],
					{ detached: true, stdio: ["ignore", "inherit", "ignore"] },
				)
			: nodeSpawn(
					"timeout",
					[`${Math.max(1, Math.floor(timeoutSeconds))}`, "bash", "-c", script],
					{ detached: true, stdio: ["ignore", "inherit", "ignore"] },
				);
		child?.unref?.();
		return (child as { pid?: number } | null)?.pid ?? null;
	} catch {
		return null;
	}
}
