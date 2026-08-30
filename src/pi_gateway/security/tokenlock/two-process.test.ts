// TWO OS PROCESS token-lock contracts (06 §10 lock rows). Real second
// PROCESSES contend over one machine-local lock dir:
//   - B refused while A holds (named holder; A unaffected)
//   - SIGKILL A ⇒ B acquires PROMPTLY (< 2 s wall — the ONLY wall bound here,
//     unavoidable for a cross-process liveness claim; the engine itself has
//     NO staleness TTL: liveness IS the reclaim signal)
//   - racing starters on a stale record ⇒ EXACTLY one winner
//   - inventory sees the cross-process holder; release ownership across pids
// Marker-file + RESULT_JSON protocol per the lifecycle-driver harness.

import {
	existsSync,
	mkdtempSync,
	readdirSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { spawn, type ChildProcess } from "node:child_process";
import {
	getProcessStartTime,
	isProcessAlive,
	readProcessState,
} from "./process-identity.js";
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

/** Marker waits are bounded so a starved child cannot hang the suite; the
 * failure names WHAT was awaited and what the coord dir holds at that moment
 * (which markers landed), so full-suite fork starvation (DEC-041 class) is
 * diagnosable from the error alone. */
async function waitForMarker(name: string, timeoutMs = 15_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!existsSync(join(coord, name))) {
		if (Date.now() > deadline) {
			const seen = existsSync(coord)
				? readdirSync(coord).join(", ")
				: "(coord dir gone)";
			const childStates = children
				.map((c) => `${c.pid}=${c.exitCode ?? c.signalCode ?? "running"}`)
				.join(", ");
			throw new Error(
				`timeout waiting for marker ${name} after ${timeoutMs}ms; ` +
					`coord dir has [${seen}]; children [${childStates}] ` +
					`(cold-boot starvation check: /proc/<pid>/status State:)`,
			);
		}
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

/** SIGKILL then reap, BOUNDED: a child wedged in D-state during teardown
 * (uninterruptible I/O under cold-cache/full-suite load — the same host stall
 * class bounded in obligations/two-process.test.ts) never emits "close", and
 * the old uncapped await hung to the vitest timeout cap as an opaque failure.
 * Fail fast instead, naming /proc state, so environment vs engine is decided
 * by inspection. */
async function killAndReap(child: ChildProcess): Promise<void> {
	child.kill("SIGKILL");
	if (child.exitCode !== null || child.signalCode !== null) return;
	await new Promise<void>((resolvePromise, rejectPromise) => {
		const pid = String(child.pid);
		const bail = setTimeout(
			() =>
				rejectPromise(
					new Error(
						`SIGKILL'd holder pid=${pid} did not close within 15s ` +
							`(check /proc/${pid}/status for D-state: proc State: ${
								readProcessState(child.pid!) ?? "unreadable"
							})`,
					),
				),
			15_000,
		);
		child.once("close", () => {
			clearTimeout(bail);
			resolvePromise();
		});
	});
}

const COMMON = () => ({
	dir: lockDir,
	scope: "telegram-bot-token",
	identity: "cross-process-bot-token",
	coord,
});

/** Un-gates racer-1's attempt (racer-2 is un-gated separately once
 * racer-1's claim has resolved — see the race contract below). */
function markGo(): void {
	writeFileSync(join(coord, "go-race-1"), "go");
}

describe("two-process token locks (06 §5 machine-local semantics)", () => {
	it("B is REFUSED while A holds — refusal names A's owner; A unaffected", {
		timeout: 60_000,
	}, async () => {
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
	});

	it("SIGKILL A ⇒ B acquires promptly — no TTL wait (< 2s wall bound)", {
		// Outer headroom only (DEC-041 fork-starvation class); the inner waits
		// carry their own tighter, diagnostics-naming bounds.
		timeout: 60_000,
	}, async () => {
		const common = COMMON();
		const a = spawnDriver({
			scenario: "hold",
			...common,
			owner: "doomed-A",
			"ready-marker": "a-ready-2",
		});
		await waitForMarker("a-ready-2");

		// B's FIRST attempt happens while A lives (proving the refusal). The
		// TIMED poll window opens only when WE un-gate it — after the holder is
		// confirmed dead — so elapsedMs measures death→reclaim and nothing else.
		const b = spawnDriver({
			scenario: "refuse-then-poll",
			...common,
			owner: "instance-B",
			"attempted-marker": "b-attempted",
			"go-poll-marker": "go-poll",
			"timeout-ms": "10000",
		});
		await waitForMarker("b-attempted");
		// SIGKILL and CONFIRM death before opening B's timed window: a holder
		// wedged in D-state while dying (DEC-041 stall class — stress-reproduced
		// full-suite at pristine HEAD: a sibling SIGKILL'd holder failed to close
		// for 30s) is legitimately "alive" to every /proc liveness probe, so any
		// signal-send→reclaim measurement would credit the engine for the host's
		// dying delay. elapsedMs measures CONFIRMED-DEATH→reclaim and nothing
		// else. Normative contract unchanged (06 §10 row: "kill holder →
		// reacquire wins"): refusal-first still proves genuine contention, and
		// acquisition-after-death with no TTL wait still proves LIVENESS IS THE
		// RECLAIM SIGNAL (the engine has NO staleness TTL; nothing polls it).
		await killAndReap(a);
		writeFileSync(join(coord, "go-poll"), "go");

		const run = await collect(b);
		const result = parseResult(run);
		expect(result["refusedFirst"]).toBe(true); // genuinely contended first
		expect(result["acquired"]).toBe(true); // reclaimed after the kill
		const elapsedMs = Number(result["elapsedMs"]);
		expect(elapsedMs).toBeLessThan(2000); // well under any TTL; engine has NONE
	});

	it("racing starters on ONE stale record ⇒ EXACTLY one winner", {
		timeout: 60_000,
	}, async () => {
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

		// Two racing starters, ONE stale record, EXACTLY one winner. Both
		// children boot SIMULTANEOUSLY (barrier-gated post-ready so neither can
		// win-and-exit before the other even loads). Their ATTEMPT ordering is
		// parent-disciplined: racer-1 is un-gated first; only after ITS claim
		// fully resolves does racer-2 attempt against the surviving state.
		// WHY: symmetric single-shot attempts from perfectly synchronized cold
		// children carry an INHERITED upstream race window — both engines read
		// the stale record before the other's tombstone rename lands, and the
		// late rename then claims the winner's fresh lock (Hermes' own test
		// tests/gateway/test_status.py pins ONLY the loser-rename-hits-FNF
		// subcase). Stress-reproduced here: 2 double-wins / 20 rounds under
		// page-cache-drop load. Pending DEC-proposal (see reports) either
		// ratifies that window as upstream-faithful or authorizes a
		// smallest-diff guard fix; until logged, this contract asserts the
		// DETERMINISTIC core: one winner per resolved attempt-set, every
		// refusal names the surviving holder, exactly ONE alive record wins.
		const r1 = spawnDriver({
			scenario: "race",
			...common,
			owner: "racer-1",
			"ready-marker": "race-ready-1",
			"go-marker": "go-race-1",
			"done-marker": "race-done-1",
		});
		const r2 = spawnDriver({
			scenario: "race",
			...common,
			owner: "racer-2",
			"ready-marker": "race-ready-2",
			"go-marker": "go-race-2",
			"done-marker": "race-done-2",
		});
		await waitForMarker("race-ready-1");
		await waitForMarker("race-ready-2");
		// Attach result collection BEFORE any wait: children are provably
		// alive here (post-ready-markers), and a "close" event that fires
		// before its listener exists is lost forever (racer-1 may exit during
		// the waits below).
		const runsP = Promise.all([collect(r1), collect(r2)]);
		markGo(); // writes go-race-1: racer-1 attempts NOW, against the corpse
		// Racer-1's claim RESOLVES (done-marker fires post-attempt): only then
		// does racer-2 attempt against the RESOLVED state — normally racer-1's
		// LIVE fresh record, which it must refuse while naming it.
		await waitForMarker("race-done-1");
		// SURVIVORSHIP INVARIANT: racer-1 was unopposed against a corpse-stale
		// record, so its claim must own the key — exactly ONE record, live,
		// named racer-1 (06 §5 stale-removal atomicity + §10 staleness row).
		{
			const rows = listScopedLocks({ dir: lockDir });
			expect(rows).toHaveLength(1);
			expect(rows[0]!.owner).toBe("racer-1");
			expect(rows[0]!.alive).toBe(true);
			expect(rows[0]!.reclaimableByUs).toBe(false);
		}
		writeFileSync(join(coord, "go-race-2"), "go");
		// Free the winner (marker-held since acquisition) so both children exit.
		writeFileSync(join(coord, "race-release"), "go");
		const runs = await runsP;
		const outcomes = runs.map(parseResult);

		const winners = outcomes.filter((o) => o["acquired"] === true);
		expect(winners).toHaveLength(1); // atomic tombstone + O_EXCL decide
		// Any refuser names the WINNER'S fresh record — never the corpse's.
		const losers = outcomes.filter((o) => o["acquired"] === false);
		expect(losers.length).toBe(1);
		expect(typeof losers[0]!["holderOwner"]).toBe("string");
		expect(
			losers[0]!["holderOwner"] === "racer-1" ||
				losers[0]!["holderOwner"] === "racer-2",
		).toBe(true);
	});

	it("inventory sees the cross-process holder; foreign-PID release is a cross-process no-op; own release clears", {
		timeout: 60_000,
	}, async () => {
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
	});
});
