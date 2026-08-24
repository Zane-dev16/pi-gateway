// TWO OS PROCESS token-lock contracts (06 §10 lock rows). Real second
// PROCESSES contend over one machine-local lock dir:
//   - B refused while A holds (named holder; A unaffected)
//   - SIGKILL A ⇒ B acquires PROMPTLY (< 2 s wall — the ONLY wall bound here,
//     unavoidable for a cross-process liveness claim; the engine itself has
//     NO staleness TTL: liveness IS the reclaim signal)
//   - racing starters on a stale record ⇒ EXACTLY one winner
//   - inventory sees the cross-process holder; release ownership across pids
// Marker-file + RESULT_JSON protocol per the lifecycle-driver harness.

import { existsSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { spawn, type ChildProcess } from "node:child_process";
import { getProcessStartTime, isProcessAlive } from "./process-identity.js";
import { listScopedLocks } from "./inventory.js";
import { scopedLockPath } from "./lock-record.js";
import { makeRecord, makeScratchDir, plantRecord } from "./testing/plant.js";

const DRIVER_TS = fileURLToPath(
	new URL("./testing/token-lock-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("../../../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let lockDir: string;
let coord: string;
const children: ChildProcess[] = [];

beforeEach(() => {
	lockDir = makeScratchDir("pi-tokenlock-xproc-locks-");
	coord = mkdtempSync(join(tmpdir(), "pi-tokenlock-xproc-coord-"));
});

afterEach(() => {
	for (const child of children.splice(0)) {
		try {
			child.kill("SIGKILL");
		} catch {
			/* already dead */
		}
	}
	rmSync(lockDir, { recursive: true, force: true });
	rmSync(coord, { recursive: true, force: true });
});

interface ChildRun {
	code: number | null;
	stdout: string;
	stderr: string;
}

function spawnDriver(scenarioArgs: Record<string, string>): ChildProcess {
	const flat: string[] = ["--scenario", scenarioArgs["scenario"] ?? ""];
	for (const [k, v] of Object.entries(scenarioArgs)) {
		if (k === "scenario") continue;
		flat.push(`--${k}`, v);
	}
	const child = spawn(
		process.execPath,
		["--import", RESOLVE_MJS, DRIVER_TS, ...flat],
		{ stdio: ["ignore", "pipe", "pipe"] },
	);
	children.push(child);
	return child;
}

function collect(child: ChildProcess): Promise<ChildRun> {
	return new Promise((resolvePromise) => {
		let stdout = "";
		let stderr = "";
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

function parseResult(run: ChildRun): Record<string, unknown> {
	const lines = run.stdout
		.split("\n")
		.filter((l) => l.startsWith("RESULT_JSON "));
	if (lines.length === 0) {
		throw new Error(`no RESULT_JSON; stderr=${run.stderr}`);
	}
	const last = lines.at(-1);
	if (last === undefined) throw new Error("empty RESULT_JSON stream");
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

async function waitForMarker(name: string, timeoutMs = 15_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!existsSync(join(coord, name))) {
		if (Date.now() > deadline) throw new Error(`timeout waiting for ${name}`);
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

async function killAndReap(child: ChildProcess): Promise<void> {
	child.kill("SIGKILL");
	await new Promise<void>((resolve) => {
		if (child.exitCode !== null || child.signalCode !== null) return resolve();
		child.on("close", () => resolve());
	});
}

const COMMON = () => ({
	dir: lockDir,
	scope: "telegram-bot-token",
	identity: "cross-process-bot-token",
	coord,
});

function markGo(): void {
	writeFileSync(join(coord, "go-race"), "go");
}

describe("two-process token locks (06 §5 machine-local semantics)", () => {
	it("B is REFUSED while A holds — refusal names A's owner; A unaffected", async () => {
		const common = COMMON();
		const a = spawnDriver({
			scenario: "hold",
			...common,
			owner: "instance-A",
			"ready-marker": "a-ready",
		});
		await waitForMarker("a-ready");
		expect(isProcessAlive(a.pid!)).toBe(true);

		const b = await collect(
			spawnDriver({ scenario: "try-once", ...common, owner: "instance-B" }),
		);
		const result = parseResult(b);
		expect(result["acquired"]).toBe(false);
		expect(result["holderOwner"]).toBe("instance-A"); // NAMED holder

		// A still alive and STILL the recorded holder after B's refused try.
		expect(isProcessAlive(a.pid!)).toBe(true);
		expect(
			existsSync(scopedLockPath(lockDir, common.scope, common.identity)),
		).toBe(true);
		await killAndReap(a);
	}, 20_000);

	it("SIGKILL A ⇒ B acquires promptly — no TTL wait (< 2s wall bound)", async () => {
		const common = COMMON();
		const a = spawnDriver({
			scenario: "hold",
			...common,
			owner: "doomed-A",
			"ready-marker": "a-ready-2",
		});
		await waitForMarker("a-ready-2");

		// B's FIRST attempt happens while A lives (proving the refusal), then
		// it polls; elapsedMs measures kill-to-reclaim latency exactly.
		const b = spawnDriver({
			scenario: "refuse-then-poll",
			...common,
			owner: "instance-B",
			"attempted-marker": "b-attempted",
			"timeout-ms": "10000",
		});
		await waitForMarker("b-attempted");
		const killedAt = Date.now();
		a.kill("SIGKILL");

		const run = await collect(b);
		const result = parseResult(run);
		expect(result["refusedFirst"]).toBe(true); // genuinely contended first
		expect(result["acquired"]).toBe(true); // reclaimed after the kill
		const elapsedMs = Number(result["elapsedMs"]);
		expect(elapsedMs).toBeLessThan(2000); // well under any TTL; engine has NONE
		void killedAt;
	}, 20_000);

	it("racing starters on ONE stale record ⇒ EXACTLY one winner", async () => {
		const common = COMMON();
		// Plant a record whose holder is REALLY dead (spawn + reap + fingerprint).
		const doomed = spawn(process.execPath, ["-e", "process.exit(0)"], {
			stdio: "ignore",
		});
		const deadPid = doomed.pid!;
		const deadStart = getProcessStartTime(deadPid)!;
		await new Promise<void>((resolve) => doomed.on("close", () => resolve()));
		plantRecord(
			lockDir,
			common.scope,
			common.identity,
			makeRecord(common.identity, {
				pid: deadPid,
				start_time: deadStart,
				scope: common.scope,
				owner: "crashed-first-starter",
			}),
		);

		// Two starters hit the SAME stale file at the same moment: both are
		// barrier-gated behind go-race so neither can win-and-exit before the
		// other even boots (that would be a legitimate dead-holder reclaim,
		// not an exclusion violation).
		const r1 = spawnDriver({
			scenario: "race",
			...common,
			owner: "racer-1",
			"ready-marker": "race-ready-1",
			"go-marker": "go-race",
			"done-marker": "race-done-1",
		});
		const r2 = spawnDriver({
			scenario: "race",
			...common,
			owner: "racer-2",
			"ready-marker": "race-ready-2",
			"go-marker": "go-race",
			"done-marker": "race-done-2",
		});
		await waitForMarker("race-ready-1");
		await waitForMarker("race-ready-2");
		markGo();
		const runs = await Promise.all([collect(r1), collect(r2)]);
		const outcomes = runs.map(parseResult);

		const winners = outcomes.filter((o) => o["acquired"] === true);
		expect(winners).toHaveLength(1); // atomic tombstone + O_EXCL decides
		// Every loser was refused against the WINNER'S fresh record — never the
		// corpse's — so all losers name the SAME (winner) holder.
		const losers = outcomes.filter((o) => o["acquired"] === false);
		expect(losers.length).toBe(1);
		expect(typeof losers[0]!["holderOwner"]).toBe("string");
		expect(
			losers[0]!["holderOwner"] === "racer-1" ||
				losers[0]!["holderOwner"] === "racer-2",
		).toBe(true);
	}, 20_000);

	it("inventory sees the cross-process holder; foreign-PID release is a cross-process no-op; own release clears", async () => {
		const common = COMMON();
		const a = spawnDriver({
			scenario: "hold-then-release-on-marker",
			...common,
			owner: "instance-A",
			"ready-marker": "a-ready-4",
			"release-marker": "release-now",
		});
		await waitForMarker("a-ready-4");

		// Parent-side inventory: WHO holds WHAT since WHEN, with liveness.
		let rows = listScopedLocks({ dir: lockDir });
		expect(rows).toHaveLength(1);
		expect(rows[0]!.owner).toBe("instance-A");
		expect(rows[0]!.pid).toBe(a.pid!);
		expect(rows[0]!.alive).toBe(true);
		expect(rows[0]!.reclaimableByUs).toBe(false);
		expect(Number.isFinite(rows[0]!.heldSinceMs)).toBe(true);

		// THIS process (yet another pid) cannot release A's lock.
		const path = scopedLockPath(lockDir, common.scope, common.identity);
		const { releaseScopedLock } = await import("./token-lock.js");
		releaseScopedLock(common.scope, common.identity, "instance-A", {
			dir: lockDir,
		});
		expect(existsSync(path)).toBe(true); // foreign-pid release ignored

		// Authority marker: A releases ITS OWN lock and verifies the clear.
		const { writeFileSync: mark } = await import("node:fs");
		mark(join(coord, "release-now"), "go");
		const run = await collect(a);
		const result = parseResult(run);
		expect(result["released"]).toBe(true);
		expect(existsSync(path)).toBe(false);
		rows = listScopedLocks({ dir: lockDir });
		expect(rows).toHaveLength(0);
	}, 20_000);
});
