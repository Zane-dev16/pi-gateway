// Behavior contracts for the <10ms shutdown forensics probe (08 §1.3(b);
// gateway/shutdown_forensics.py port): snapshot content, never-raise
// discipline, systemd detection, marker race hints, one-line log format with
// parent_cmdline LAST, and the DETACHED diagnostic spawn contract.

import { describe, expect, it } from "vitest";
import {
	formatContextForLog,
	procSummary,
	snapshotShutdownContext,
	spawnAsyncDiagnostic,
} from "./forensics.js";
import { writeTakeoverMarker } from "./markers.js";

const READER = (files: Record<string, string>) => (path: string) =>
	files[path] ?? null;

describe("snapshotShutdownContext (fast sync probe)", () => {
	it("captures pid/ppid/signal identity and NEVER raises on hostile readers", () => {
		const ctx = snapshotShutdownContext({
			signal: "SIGTERM",
			selfPid: 4242,
			procReader: () => {
				throw new Error("proc exploded");
			},
			environ: {},
		});
		expect(ctx["pid"]).toBe(4242);
		expect(ctx["signal"]).toBe("SIGTERM");
		expect(ctx["signal_num"]).toBe(15);
		expect(ctx["ts"]).toBeTypeOf("number");
		expect(ctx["ts_monotonic"]).toBeTypeOf("number");
	});

	it("null signal (programmatic stop) carries no signal_num", () => {
		const ctx = snapshotShutdownContext({ signal: null, environ: {} });
		expect(ctx["signal"]).toBeNull();
		expect(ctx["signal_num"]).toBeNull();
	});

	it("systemd context: INVOCATION_ID or ppid==1 ⇒ under_systemd", () => {
		const withInvocation = snapshotShutdownContext({
			environ: { INVOCATION_ID: "inv-123" },
		});
		expect(withInvocation["under_systemd"]).toBe(true);
		expect(withInvocation["systemd_invocation_id"]).toBe("inv-123");

		const bare = snapshotShutdownContext({ environ: {} });
		expect(bare["under_systemd"]).toBe(false);
	});

	it("TracerPid ≠ 0 surfaces a debugger hint; 0 stays silent", () => {
		const files = {
			"/proc/99/status": "Name:\tinit\nTracerPid:\t555\n",
		};
		const traced = snapshotShutdownContext({
			selfPid: 99,
			environ: {},
			procReader: READER(files),
		});
		expect(traced["tracer_pid"]).toBe(555);

		const clean = snapshotShutdownContext({
			selfPid: 99,
			environ: {},
			procReader: READER({
				"/proc/99/status": "TracerPid:\t0\n",
			}),
		});
		expect("tracer_pid" in clean).toBe(false);
	});

	it("marker race hints: a takeover marker naming ANOTHER pid is the smoking gun", () => {
		const home = "/tmp/fake-home-fixture"; // never touched — reader is injected
		const files = {
			[`${home}/.gateway-takeover.json`]: `{"target_pid": 777, "replacer_pid": 42, "note": "${"x".repeat(500)}"}`,
			[`${home}/.gateway-planned-stop.json`]: `{"target_pid": 4242}`,
		};
		const ctx = snapshotShutdownContext({
			signal: "SIGTERM",
			selfPid: 4242,
			home,
			environ: {},
			procReader: READER(files),
		});
		expect(String(ctx["takeover_marker"])).toHaveLength(300); // raw ≤300
		expect(ctx["takeover_marker_for_self"]).toBe(false); // names 777, not us
		expect(ctx["planned_stop_marker"]).toBe(`{"target_pid": 4242}`);
	});
});

describe("procSummary", () => {
	it("parses comm/state from /proc stat and truncates cmdline to 300", () => {
		const summary = procSummary(
			7,
			READER({
				"/proc/7/stat": "7 (systemd) S 1 1 0 0 -1 4194560 ...".concat(
					" ".repeat(40),
				),
				"/proc/7/cmdline": `/usr/bin/${"long".repeat(200)}\0`,
			}),
		);
		expect(summary["name"]).toBe("systemd");
		expect(summary["state"]).toBe("S");
		expect(String(summary["cmdline"])).toHaveLength(300);
	});
});

describe("formatContextForLog", () => {
	it("one key=value line with parent_cmdline emitted LAST", () => {
		const line = formatContextForLog({
			ts: 12,
			signal: "SIGTERM",
			parent: { cmdline: "/bin/very long parent command" },
			self: {},
		});
		expect(line).toContain("ts=12");
		expect(line).toContain("signal=SIGTERM");
		expect(
			line.trimEnd().endsWith("parent_cmdline=/bin/very long parent command"),
		).toBe(true);
	});
});

describe("spawnAsyncDiagnostic (detached ps walk)", () => {
	it("spawns a self-timeout-limited DETACHED bash script writing to the diag log", () => {
		const calls: Array<{
			cmd: string;
			args: readonly string[];
			opts: Record<string, unknown>;
		}> = [];
		const pid = spawnAsyncDiagnostic("/tmp/diag-dir", "SIGUSR1", {
			spawn: (cmd, args, opts) => {
				calls.push({ cmd, args, opts });
				return { unref() {}, pid: 31337 };
			},
		});
		expect(pid).toBe(31337);
		const call = calls[0];
		if (!call) throw new Error("spawn never invoked");
		expect(call.cmd).toBe("timeout");
		expect(call.args[0]).toBe("5"); // default self-timeout
		expect(call.args[1]).toBe("bash");
		expect(String(call.args[3])).toContain("shutdown diagnostic @ SIGUSR1");
		expect(String(call.args[3])).toContain("ps auxf");
		expect(call.opts["detached"]).toBe(true); // survives cgroup teardown
	});

	it("spawn failure is swallowed (never raises from the signal path)", () => {
		const pid = spawnAsyncDiagnostic("/tmp/diag-dir", "SIGTERM", {
			spawn: () => {
				throw new Error("no such binary");
			},
		});
		expect(pid).toBeNull();
	});
});

// Integration guard: real fs marker presence flows through the same contract.
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

describe("real-marker integration", () => {
	let home: string;
	try {
		home = mkdtempSync(join(tmpdir(), "pi-forensics-"));
	} catch {
		home = "";
	}
	const canRun = home !== "";

	it.skipIf(!canRun)(
		"a real takeover marker naming us flips the for-self hint",
		() => {
			writeTakeoverMarker(home, process.pid);
			const ctx = snapshotShutdownContext({
				signal: "SIGTERM",
				home,
				environ: {},
			});
			expect(ctx["takeover_marker_for_self"]).toBe(true);
			rmSync(home, { recursive: true, force: true });
		},
	);
});
