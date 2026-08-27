// Behavior contracts: duplicate-instance guard primitives — PID file
// O_EXCL race + ownership-guarded removal, runtime-lock mutual exclusion +
// death auto-release, marker TTL/PID-reuse/cross-home rules, live-instance
// probe evidence chain. Anchors: 01 §3.2, 08 §1.1 stage 4.

import { execFile, spawn, type ChildProcess } from "node:child_process";
import {
	existsSync,
	mkdtempSync,
	realpathSync as nodeRealpathSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	RuntimeLock,
	forceRemovePidFile,
	getRunningPid,
	isRuntimeLockActive,
	pidFilePath,
	readPidFile,
	recordHomeMatches,
	removePidFile,
	writePidFile,
} from "./instance-guard.js";
import {
	MARKER_TTL_SECONDS,
	clearPlannedStopMarker,
	clearTakeoverMarker,
	consumePlannedStopMarkerForSelf,
	consumeTakeoverMarkerForSelf,
	plannedStopMarkerPath,
	plannedStopMarkerTargetsSelf,
	takeoverMarkerPath,
	writePlannedStopMarker,
	writeTakeoverMarker,
} from "./markers.js";
import { probeProcess } from "./process-info.js";

const exec = promisify(execFile);

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-lifecycle-guard-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

const SELF = process.pid;

describe("pid file (status.py write_pid_file parity)", () => {
	it("claims atomically; a DIFFERENT live starter loses the O_EXCL race", () => {
		expect(writePidFile(home)).toBe(true);
		const record = readPidFile(home);
		expect(record?.pid).toBe(SELF);
		expect(record?.kind).toBe("pi-gateway");

		// Second claim by another pid must fail…
		expect(writePidFile(home, { selfPid: SELF + 1_000_000 })).toBe(false);
		// …and the file still names US.
		expect(readPidFile(home)?.pid).toBe(SELF);
	});

	it("is idempotent for OUR OWN record (partial-restart re-entry)", () => {
		expect(writePidFile(home)).toBe(true);
		expect(writePidFile(home)).toBe(true); // ours already — ok
	});

	it("remove is ownership-GUARDED: never grieves another pid's file", () => {
		writePidFile(home, { selfPid: 777_777 });
		removePidFile(home); // we are NOT 777777 → file survives
		expect(existsSync(pidFilePath(home))).toBe(true);
		forceRemovePidFile(home);
		expect(existsSync(pidFilePath(home))).toBe(false);
	});
});

describe("runtime lock (flock-equivalent over the SQLite sidecar)", () => {
	it("mutual exclusion: contender refused while held; wins after release", () => {
		const holder = new RuntimeLock(home);
		const contender = new RuntimeLock(home);
		expect(holder.acquire()).toBe(true);
		expect(isRuntimeLockActive(home)).toBe(true);
		expect(contender.acquire()).toBe(false); // busy ⇒ not acquired

		holder.release();
		expect(contender.acquire()).toBe(true); // released ⇒ contender wins
		contender.release();
		expect(isRuntimeLockActive(home)).toBe(false);
	});

	it("acquire is idempotent for the same holder", () => {
		const lock = new RuntimeLock(home);
		expect(lock.acquire()).toBe(true);
		expect(lock.acquire()).toBe(true);
		lock.release();
		expect(new RuntimeLock(home).acquire()).toBe(true);
	});

	it("releases automatically on process DEATH (OS-owned liveness)", async () => {
		const driverTs = new URL("./testing/lifecycle-driver.ts", import.meta.url);
		const resolveMjs = new URL(
			"../../pi_state/testing/node-ts-resolve.mjs",
			import.meta.url,
		);
		const coordDir = home;
		const child = spawnDriver(
			{
				scenario: "hold-runtime-lock",
				home,
				coord: coordDir,
				"ready-marker": "lock-held",
				"release-marker": "release-never",
			},
			driverTs,
			resolveMjs,
		);

		await waitForMarker(join(coordDir, "lock-held"));
		expect(isRuntimeLockActive(home)).toBe(true);

		// SIGKILL the holder: the OS closes its fds — SQLite releases the lock.
		child.kill("SIGKILL");
		await exited(child);
		// Loose bound ≥2s per flake discipline: poll up to 10s for the release.
		const deadline = Date.now() + 10_000;
		while (isRuntimeLockActive(home) && Date.now() < deadline) {
			await sleep(50);
		}
		expect(isRuntimeLockActive(home)).toBe(false);
	}, 20_000);
});

describe("live-instance probe (get_running_pid evidence chain)", () => {
	it("no pid file ⇒ null; stale file with dead pid ⇒ cleaned to null", () => {
		expect(getRunningPid(home)).toBeNull();

		// Dead pid + no lock ⇒ stale leftover, cleaned up.
		writeFileSync(
			pidFilePath(home),
			JSON.stringify({
				pid: spawnDeadPid(),
				kind: "pi-gateway",
				argv: [],
				start_time: null,
				pi_home: home,
			}),
		);
		expect(getRunningPid(home)).toBeNull();
		expect(existsSync(pidFilePath(home))).toBe(false);
	});

	it("live pid WITH active lock names the running instance", async () => {
		const lock = new RuntimeLock(home);
		expect(lock.acquire()).toBe(true);
		expect(writePidFile(home)).toBe(true);
		const running = getRunningPid(home);
		expect(running?.pid).toBe(SELF);
		expect(running?.record.pi_home).toBe(home);
		lock.release();
	});

	it("PID-REUSE guard: recorded start_time mismatching the live process ⇒ stale", () => {
		const lock = new RuntimeLock(home);
		lock.acquire();
		// Record OUR pid but a start_time that cannot be ours (0 ticks).
		writeFileSync(
			pidFilePath(home),
			JSON.stringify({
				pid: SELF,
				kind: "pi-gateway",
				argv: [],
				start_time: 0,
				pi_home: home,
			}),
		);
		if (probeProcess(SELF).startTime === null) {
			// Off-Linux: start times unknown on BOTH sides → documented fallback
			// rule applies and the instance IS reported (bounded by marker TTLs).
			expect(getRunningPid(home)?.pid).toBe(SELF);
		} else {
			expect(getRunningPid(home)).toBeNull();
			expect(existsSync(pidFilePath(home))).toBe(false);
		}
		lock.release();
	});

	// Destructive-action authority check (#89315 parity): --replace must not
	// signal a target whose OWNERSHIP the persisted record cannot prove.
	describe("recordHomeMatches (--replace ownership authority, #89315)", () => {
		it("exact same home proves ownership", () => {
			expect(recordHomeMatches({ pi_home: home }, home)).toBe(true);
		});

		it("a FOREIGN profile's live gateway is never owned by this home", () => {
			expect(recordHomeMatches({ pi_home: "/home/other-profile" }, home)).toBe(
				false,
			);
		});

		it("a LEGACY record without pi_home stamping is unprovable ⇒ fail closed", () => {
			expect(recordHomeMatches({}, home)).toBe(false);
			expect(recordHomeMatches({ pi_home: "   " }, home)).toBe(false);
			expect(recordHomeMatches({ pi_home: 1234567890 }, home)).toBe(false);
		});

		it("symlink/realpath differences of the SAME directory still match", () => {
			let real = "";
			try {
				real = nodeRealpathSync(home);
			} catch {
				real = home;
			}
			expect(recordHomeMatches({ pi_home: real }, home)).toBe(true);
		});
	});
});

describe("markers — takeover + planned stop (08 §1.2 classification inputs)", () => {
	it("takeover marker written before signal is consumed EXACTLY once by target", () => {
		expect(writeTakeoverMarker(home, SELF)).toBe(true);
		expect(existsSync(takeoverMarkerPath(home))).toBe(true);
		expect(consumeTakeoverMarkerForSelf(home)).toBe(true);
		expect(existsSync(takeoverMarkerPath(home))).toBe(false); // consumed+unlinked
		expect(consumeTakeoverMarkerForSelf(home)).toBe(false); // second read: gone
	});

	it("marker naming ANOTHER pid is consumed-and-rejected (not left behind)", () => {
		writeTakeoverMarker(home, 999_999_999);
		expect(consumeTakeoverMarkerForSelf(home)).toBe(false);
		expect(existsSync(takeoverMarkerPath(home))).toBe(false);
	});

	it("stale marker (>60s TTL) is ignored AND cleaned so it can't wedge boot", () => {
		writeTakeoverMarker(home, SELF, {
			nowMs: () => Date.now() - (MARKER_TTL_SECONDS + 5) * 1000,
		});
		expect(consumeTakeoverMarkerForSelf(home)).toBe(false);
		expect(existsSync(takeoverMarkerPath(home))).toBe(false);
	});

	it("malformed marker can never match anyone (consume leaves it; probe drops it)", () => {
		writeFileSync(takeoverMarkerPath(home), "{not json");
		// Authoritative consume: unreadable ⇒ false, file untouched
		// (_consume_pid_marker_for_self returns False without unlinking).
		expect(consumeTakeoverMarkerForSelf(home)).toBe(false);
		expect(existsSync(takeoverMarkerPath(home))).toBe(true);
		// The NON-DESTRUCTIVE PROBE is what cleans malformed markers so they
		// can never wedge a future boot (planned_stop_marker_targets_self).
		expect(plannedStopMarkerTargetsSelf(home)).toBe(false);
		expect(plannedStopMarkerTargetsSelf(home)).toBe(false);
		clearTakeoverMarker(home);
		expect(existsSync(takeoverMarkerPath(home))).toBe(false);
	});

	it("planned-stop: authoritative consume vs non-destructive probe agree on target rules", () => {
		expect(writePlannedStopMarker(home, SELF)).toBe(true);
		// Probe does NOT unlink a matching marker…
		expect(plannedStopMarkerTargetsSelf(home)).toBe(true);
		expect(existsSync(plannedStopMarkerPath(home))).toBe(true);
		// …only the authoritative consume does.
		expect(consumePlannedStopMarkerForSelf(home)).toBe(true);
		expect(existsSync(plannedStopMarkerPath(home))).toBe(false);

		// Stale planned-stop markers are cleaned by the probe itself.
		writePlannedStopMarker(home, SELF, {
			nowMs: () => Date.now() - (MARKER_TTL_SECONDS + 5) * 1000,
		});
		expect(plannedStopMarkerTargetsSelf(home)).toBe(false);
		expect(existsSync(plannedStopMarkerPath(home))).toBe(false);
	});

	it("clear functions remove markers unconditionally and repeatedly", () => {
		writeTakeoverMarker(home, SELF);
		writePlannedStopMarker(home, SELF);
		clearTakeoverMarker(home);
		clearPlannedStopMarker(home);
		clearTakeoverMarker(home); // idempotent
		clearPlannedStopMarker(home);
		expect(existsSync(takeoverMarkerPath(home))).toBe(false);
		expect(existsSync(plannedStopMarkerPath(home))).toBe(false);
	});

	it("takeover marker naming a DIFFERENT target_pi_home is IGNORED (#29092)", () => {
		const otherHome = mkdtempSync(join(tmpdir(), "pi-lifecycle-other-"));
		try {
			// Marker content names another home but sits in OUR directory.
			writeFileSync(
				takeoverMarkerPath(home),
				JSON.stringify({
					target_pid: SELF,
					target_start_time: probeProcess(SELF).startTime,
					target_pi_home: otherHome,
					replacer_pid: 1,
					replacer_pi_home: otherHome,
					written_at: new Date().toISOString(),
				}),
			);
			// Not ours — and NOT unlinked (another profile owns it).
			expect(consumeTakeoverMarkerForSelf(home)).toBe(false);
			expect(existsSync(takeoverMarkerPath(home))).toBe(true);
		} finally {
			rmSync(otherHome, { recursive: true, force: true });
		}
	});
});

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

function spawnDriver(
	scenarioArgs: Record<string, string>,
	driverTs: URL,
	resolveMjs: URL,
): ChildProcess {
	const flat: string[] = [];
	for (const [k, v] of Object.entries(scenarioArgs)) {
		flat.push(`--${k}`, v);
	}
	return spawn(
		process.execPath,
		["--import", fileURLToPath(resolveMjs), fileURLToPath(driverTs), ...flat],
		{ stdio: ["ignore", "pipe", "pipe"] },
	);
}

async function exited(child: ChildProcess): Promise<void> {
	await new Promise<void>((resolve) => {
		if (child.exitCode !== null || child.signalCode !== null) resolve();
		else child.once("close", () => resolve());
	});
}

async function waitForMarker(path: string, timeoutMs = 15_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!existsSync(path)) {
		if (Date.now() > deadline) throw new Error(`timeout waiting for ${path}`);
		await sleep(10);
	}
}

/** Spawn a short-lived child and wait for exit — yields a provably-dead pid. */
function spawnDeadPid(): number {
	const child = exec(process.execPath, ["-e", "process.exit(0)"]);
	return child.child.pid ?? -1;
}

function sleep(ms: number): Promise<void> {
	return new Promise((r) => setTimeout(r, ms));
}
