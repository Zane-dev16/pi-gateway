// Two-OS-process contracts for the duplicate-instance takeover handshake
// (01 §3.2; 08 §1.1 stage 4): a real second PROCESS takes over a running one,
// and a follower WITHOUT --replace exits cleanly without disturbing the
// winner. Marker-before-SIGTERM ordering is asserted from inside the old
// process's own classification.

import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { spawn, type ChildProcess } from "node:child_process";
import { GatewayLifecycle } from "./lifecycle.js";
import { getRunningPid } from "./instance-guard.js";
import { readRuntimeStatus } from "./status-stamp.js";

const DRIVER_TS = fileURLToPath(
	new URL("./testing/lifecycle-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("../../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let home: string;
let coord: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-lifecycle-takeover-home-"));
	coord = mkdtempSync(join(tmpdir(), "pi-lifecycle-takeover-coord-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
	rmSync(coord, { recursive: true, force: true });
});

interface ChildRun {
	code: number | null;
	stdout: string;
	stderr: string;
}

function spawnDriver(scenarioArgs: Record<string, string>): ChildProcess {
	const flat: string[] = [];
	for (const [k, v] of Object.entries(scenarioArgs)) {
		flat.push(`--${k}`, v);
	}
	return spawn(
		process.execPath,
		["--import", RESOLVE_MJS, DRIVER_TS, ...flat],
		{ stdio: ["ignore", "pipe", "pipe"] },
	);
}

function collect(child: ChildProcess): Promise<ChildRun> {
	return new Promise((resolvePromise) => {
		let stdout = "";
		let stderr = "";
		// stdio is ["ignore","pipe","pipe"], but strict types widen to null —
		// optional chaining satisfies both tsc and no-non-null-assertion.
		child.stdout?.on("data", (d: Buffer) => {
			stdout += d.toString("utf8");
		});
		child.stderr?.on("data", (d: Buffer) => {
			stderr += d.toString("utf8");
		});
		child.on("close", (code: number | null) => {
			resolvePromise({ code, stdout, stderr });
		});
	});
}

async function waitForMarker(name: string, timeoutMs = 15_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!existsSync(join(coord, name))) {
		if (Date.now() > deadline) throw new Error(`timeout waiting for ${name}`);
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

function parseResult(run: ChildRun): Record<string, unknown> {
	const lines = run.stdout
		.split("\n")
		.filter((l) => l.startsWith("RESULT_JSON "));
	if (lines.length === 0) {
		throw new Error(`no RESULT_JSON; stderr=${run.stderr}`);
	}
	const last = lines.at(-1);
	if (last === undefined) {
		throw new Error(`empty RESULT_JSON stream; out=${run.stdout}`);
	}
	try {
		return JSON.parse(last.slice("RESULT_JSON ".length)) as Record<
			string,
			unknown
		>;
	} catch (err) {
		throw new Error(
			`malformed RESULT_JSON (${String(err)}); out=${run.stdout}`,
		);
	}
}

const QUIET_LOGGER = {
	info: () => {},
	warn: () => {},
	error: () => {},
};

describe("duplicate-instance takeover across OS processes (01 §3.2)", () => {
	it("REPLACER: writes marker BEFORE SIGTERM; old process classifies takeover and exits 0; replacer proceeds to READY", async () => {
		// Process A: a REAL gateway-like child that starts, signals ready, and
		// keeps its event loop alive until signalled. Start collecting output
		// IMMEDIATELY — A exits as soon as its drain finishes, which can beat
		// any later await.
		const procA = spawnDriver({
			scenario: "hold-running",
			home,
			coord,
			"ready-marker": "ready-a",
		});
		const collectedA = collect(procA);
		try {
			await waitForMarker("ready-a");
			const readyA = JSON.parse(
				readFileSync(join(coord, "ready-a"), "utf8"),
			) as {
				pid: number;
				ok: boolean;
			};
			expect(readyA.ok).toBe(true);
			expect(getRunningPid(home)?.pid).toBe(readyA.pid);

			// Process B (in-parent, distinct pid by construction): replaces A.
			const lifecycleB = new GatewayLifecycle({
				home,
				replace: true,
				logger: QUIET_LOGGER,
			});
			const resultB = await lifecycleB.startup();

			expect(resultB.ok).toBe(true);
			expect(resultB.failedStage).toBeNull();
			expect(lifecycleB.state).toBe("running");
			// B now owns the instance record.
			expect(getRunningPid(home)?.pid).toBe(process.pid);

			// A classified its SIGTERM as a planned takeover and exited CLEANLY:
			// exit code 0, no unexpected-signal mirror, stopped persisted.
			const closeInfo = await collectedA;
			expect(closeInfo.code).toBe(0);
			const outcomeA = parseResult(closeInfo);
			expect(outcomeA["klass"]).toBe("takeover");
			expect(outcomeA["exitCode"]).toBe(0);
			expect(outcomeA["persistedStopped"]).toBe(true);
			expect(outcomeA["unexpected"]).toBe(false);

			// B stamped READY with the boot fingerprint fields present in shape.
			const status = readRuntimeStatus(home);
			expect(status?.gateway_state).toBe("running");
			expect(status?.code_version).toBeTruthy();

			await lifecycleB.requestShutdown("planned_stop");
		} finally {
			if (!procA.killed && procA.exitCode === null) procA.kill("SIGKILL");
		}
	}, 30_000);

	it("FOLLOWER: second instance WITHOUT --replace exits cleanly; original unaffected", async () => {
		const procA = spawnDriver({
			scenario: "hold-running",
			home,
			coord,
			"ready-marker": "ready-a2",
		});
		const collectedA = collect(procA);
		try {
			await waitForMarker("ready-a2");
			const readyA = JSON.parse(
				readFileSync(join(coord, "ready-a2"), "utf8"),
			) as {
				pid: number;
			};

			// Follower startup must FAIL with the duplicate-instance reason…
			const follower = new GatewayLifecycle({ home, logger: QUIET_LOGGER });
			const result = await follower.startup();
			expect(result.ok).toBe(false);
			expect(result.reasonCode).toBe("duplicate_instance");
			expect(follower.state).toBe("aborted");

			// …the pid file still names the ORIGINAL winner…
			expect(getRunningPid(home)?.pid).toBe(readyA.pid);

			// …and the original remains alive and untouched.
			await new Promise<void>((r) => setTimeout(r, 200));
			expect(procA.exitCode).toBeNull(); // still running
			expect(existsSync(join(coord, "done-never"))).toBe(false);

			// Clean teardown of A via a planned-stop marker + SIGTERM.
			const { writePlannedStopMarker } = await import("./markers.js");
			expect(writePlannedStopMarker(home, readyA.pid)).toBe(true);
			procA.kill("SIGTERM");
			const closeInfo = await collectedA;
			expect(closeInfo.code).toBe(0);
			const outcomeA = parseResult(closeInfo);
			expect(outcomeA["klass"]).toBe("planned_stop");
		} finally {
			if (!procA.killed && procA.exitCode === null) procA.kill("SIGKILL");
		}
	}, 30_000);
});
