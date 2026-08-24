// Process-identity primitives behind the token-lock staleness ladder (06 §5).
// Host-honest contracts only: everything here probes REAL OS state on this
// Linux host (/proc present); Windows gating is documented, never faked.

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	getProcessStartTime,
	isProcessAlive,
	isProcessStopped,
	isProcessZombie,
	probeSignaledDead,
	readProcessCmdline,
	readProcessState,
} from "./process-identity.js";

let scratch: string;
const children: ChildProcess[] = [];

beforeEach(() => {
	scratch = mkdtempSync(join(tmpdir(), "pi-tokenlock-proc-"));
});

afterEach(() => {
	for (const child of children.splice(0)) {
		try {
			child.kill("SIGKILL");
		} catch {
			/* already dead */
		}
	}
	rmSync(scratch, { recursive: true, force: true });
});

function spawnSleeper(): ChildProcess {
	const child = spawn(
		process.execPath,
		["-e", "setInterval(() => {}, 60_000)"],
		{ stdio: "ignore" },
	);
	children.push(child);
	return child;
}

async function waitFor(predicate: () => boolean, ms = 5000): Promise<void> {
	const deadline = Date.now() + ms;
	while (!predicate()) {
		if (Date.now() > deadline) throw new Error("condition not reached");
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

describe("getProcessStartTime — the PID-reuse fingerprint (status.py port)", () => {
	it("self start time is a positive integer and STABLE across reads", () => {
		const first = getProcessStartTime(process.pid);
		expect(first).not.toBeNull();
		expect(Number.isInteger(first)).toBe(true);
		expect(first!).toBeGreaterThan(0);
		expect(getProcessStartTime(process.pid)).toBe(first);
	});

	it("two different live processes carry different fingerprints", () => {
		const child = spawnSleeper();
		const ours = getProcessStartTime(process.pid)!;
		const theirs = getProcessStartTime(child.pid!);
		expect(theirs).not.toBeNull();
		// Distinct spawns cannot share a boot-ticks stamp (same host source).
		expect(theirs).not.toBe(ours);
	});

	it("unusable pids yield null instead of throwing", () => {
		expect(getProcessStartTime(-1)).toBeNull();
		expect(getProcessStartTime(0)).toBeNull();
		expect(getProcessStartTime(Number.NaN)).toBeNull();
	});
});

describe("liveness probes — ESRCH discipline + state bytes", () => {
	it("a live sleeper is alive, not zombie, not stopped", async () => {
		const child = spawnSleeper();
		await waitFor(() => probeSignaledDead(child.pid!) === false);
		expect(isProcessAlive(child.pid!)).toBe(true);
		expect(isProcessZombie(child.pid!)).toBe(false);
		expect(isProcessStopped(child.pid!)).toBe(false);
	});

	it("an EXITED (reaped) pid is proven dead — the kill-holder reclaim path", async () => {
		const doomed = spawn(process.execPath, ["-e", "process.exit(0)"], {
			stdio: "ignore",
		});
		const pid = doomed.pid!;
		await new Promise<void>((resolve) => doomed.on("close", () => resolve()));
		expect(probeSignaledDead(pid)).toBe(true);
		expect(isProcessAlive(pid)).toBe(false);
	});

	it("SIGSTOPped holder reports state T/t — the ladder's stopped rung", async () => {
		const child = spawnSleeper();
		await waitFor(() => isProcessAlive(child.pid!));
		child.kill("SIGSTOP");
		await waitFor(() => isProcessStopped(child.pid!));
		expect(["T", "t"]).toContain(readProcessState(child.pid!));
		// Still passes signal-liveness — ONLY the state byte distinguishes it.
		expect(probeSignaledDead(child.pid!)).toBe(false);
		child.kill("SIGCONT");
		await waitFor(() => !isProcessStopped(child.pid!));
	});

	it("readProcessCmdline returns this process's own command line", () => {
		const cmdline = readProcessCmdline(process.pid);
		expect(cmdline).not.toBeNull();
		expect(cmdline!.length).toBeGreaterThan(0);
	});

	it("cmdline/state/start-time of a dead pid are null, never fabricated", async () => {
		const doomed = spawn(process.execPath, ["-e", "process.exit(0)"], {
			stdio: "ignore",
		});
		const pid = doomed.pid!;
		await new Promise<void>((resolve) => doomed.on("close", () => resolve()));
		expect(readProcessCmdline(pid)).toBeNull();
		expect(readProcessState(pid)).toBeNull();
		expect(getProcessStartTime(pid)).toBeNull();
	});
});
