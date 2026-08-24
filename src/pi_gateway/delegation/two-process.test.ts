// Two-OS-PROCESS contracts for the delegation rail (01 §3.2 posture:
// cross-process behavior needs real processes, not mocks):
//
//   1. CRASH between claim-write and durable ack ⇒ the NEXT boot restores
//      the undelivered completion EXACTLY once (restored=True stamp), a
//      fresh consumer claims it via stale-claim takeover and acks, and a
//      THIRD boot finds nothing. The SIGKILL proves durability comes from
//      committed WAL rows, not graceful shutdown.
//   2. N live processes race one atomic claim ⇒ EXACTLY ONE winner; the
//      losers' guarded UPDATEs change nothing (attempts stay 1).

import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

const DRIVER_TS = fileURLToPath(
	new URL("./testing/rail-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("../../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let dir: string;
let dbPath: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-delegation-twoproc-"));
	dbPath = join(dir, "state.db");
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

interface ChildRun {
	code: number | null;
	stdout: string;
	stderr: string;
}

function runDriver(args: string[], timeoutMs = 20_000): Promise<ChildRun> {
	return new Promise((resolvePromise) => {
		const child: ChildProcess = spawn(
			process.execPath,
			["--import", RESOLVE_MJS, DRIVER_TS, ...args],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		let stdout = "";
		let stderr = "";
		child.stdout?.on("data", (d: Buffer) => {
			stdout += d.toString("utf8");
		});
		child.stderr?.on("data", (d: Buffer) => {
			stderr += d.toString("utf8");
		});
		const timer = setTimeout(() => {
			child.kill("SIGKILL");
		}, timeoutMs);
		child.on("close", (code) => {
			clearTimeout(timer);
			resolvePromise({ code, stdout, stderr });
		});
	});
}

function parseResult(run: ChildRun): Record<string, unknown> {
	expect(run.code).toBe(0);
	const lines = run.stdout
		.split("\n")
		.filter((l) => l.startsWith("RESULT_JSON "));
	if (lines.length === 0) {
		throw new Error(`no RESULT_JSON; stderr=${run.stderr}`);
	}
	const last = lines.at(-1);
	if (!last) throw new Error(`empty RESULT_JSON; out=${run.stdout}`);
	try {
		return JSON.parse(last.slice("RESULT_JSON ".length)) as Record<
			string,
			unknown
		>;
	} catch (err) {
		throw new Error(
			`malformed RESULT_JSON (${String(err)}); line=${last}; stderr=${run.stderr}`,
		);
	}
}

async function waitForFile(path: string, timeoutMs = 20_000): Promise<unknown> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		try {
			return JSON.parse(readFileSync(path, "utf8")) as unknown;
		} catch {
			// not there yet
		}
		if (Date.now() > deadline) throw new Error(`timeout waiting ${path}`);
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

describe("two-process rail contracts (real processes)", () => {
	it("SIGKILL between claim and ack ⇒ boot restore replays EXACTLY once; third boot sees nothing", {
		timeout: 300_000, // child launches pay Node+TS-resolve startup under full-suite load (4 CPUs); isolation runs ~1.5s — headroom against fork starvation onlyr load
	}, async () => {
		const delegationId = "dlg-crash-two-proc";
		const claimedMarker = join(dir, "claimed");

		// BOOT 1: publish durably, claim, then get murdered mid-delivery.
		const doomed = spawn(
			process.execPath,
			[
				"--import",
				RESOLVE_MJS,
				DRIVER_TS,
				"publish-claim-hold",
				dbPath,
				dir,
				delegationId,
				claimedMarker,
			],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		doomed.stderr?.resume();
		await waitForFile(claimedMarker);
		expect(doomed.pid).toBeDefined();
		doomed.kill("SIGKILL");
		// Bounded close-wait: a SIGKILL'd child wedged in D-state (uninterruptible
		// I/O, observed under cold-cache full-suite load) never emits "close" and
		// previously hung the whole test to its timeout cap. Fail fast + diagnose
		// instead; the durability contract itself needs only that the row was
		// committed before the kill, which BOOT 2's restore proves either way.
		await new Promise<void>((resolvePromise, rejectPromise) => {
			const bail = setTimeout(
				() =>
					rejectPromise(
						new Error(
							`SIGKILL'd holder pid=${String(doomed.pid)} did not close within 30s ` +
							`(check /proc/${String(doomed.pid)}/status for D-state)`,
						),
				),
				30_000,
			);
			doomed.once("close", () => {
				clearTimeout(bail);
				resolvePromise();
			});
		});

		// BOOT 2: restore hands the row to exactly ONE consumer.
		const bootTwo = parseResult(
			await runDriver(["restore-complete", dbPath, delegationId, "301"]),
		);
		expect(bootTwo.firstRestore).toBe(1);
		const events = bootTwo.events as Array<Record<string, unknown>>;
		expect(events).toHaveLength(1);
		const evt = events[0];
		if (!evt) throw new Error("unreachable");
		expect(evt["delegation_id"]).toBe(delegationId);
		expect(evt["restored"]).toBe(true); // ownership-proof stamp
		expect(bootTwo.claimed).toBe(true); // stale takeover won
		expect(bootTwo.completed).toBe(true); // ack landed
		expect(bootTwo.secondRestore).toBe(0); // same pass never re-sees it
		expect(bootTwo.deliveryState).toBe("delivered");
		expect(bootTwo.attempts).toBe(2); // doomed child + boot-two consumer

		// The durable payload survived the crash byte-for-byte (the restored
		// flag is in-memory only — event_json must NOT contain it).
		const durable = String(bootTwo.eventJsonStillDurable ?? "");
		expect(durable).toContain(delegationId);
		expect(durable).not.toContain("restored");

		// BOOT 3: nothing undelivered remains — replay happened exactly once.
		const bootThree = parseResult(await runDriver(["restore-only", dbPath]));
		expect(bootThree.restored).toBe(0);
		expect(bootThree.seen).toEqual([]);
	});

	it("N processes race one atomic claim ⇒ exactly ONE winner; attempts increment once", {
		timeout: 120_000,
	}, async () => {
		const delegationId = "dlg-race";
		const setup = parseResult(
			await runDriver(["setup-pending", dbPath, delegationId]),
		);
		expect(setup.deliveryState).toBe("pending");

		const RACERS = 6;
		const goMarker = join(dir, "go");
		const children: ChildProcess[] = [];
		const resultFiles: string[] = [];
		try {
			for (let i = 0; i < RACERS; i++) {
				const resultFile = join(dir, `result-${i}`);
				resultFiles.push(resultFile);
				const child = spawn(
					process.execPath,
					[
						"--import",
						RESOLVE_MJS,
						DRIVER_TS,
						"racer",
						dbPath,
						goMarker,
						resultFile,
						String(i),
						delegationId,
					],
					{ stdio: ["ignore", "pipe", "pipe"] },
				);
				child.stderr?.resume();
				children.push(child);
			}
			// Fire the starting gun only once every racer is live.
			const { writeFileSync } = await import("node:fs");
			writeFileSync(goMarker, JSON.stringify({ t: Date.now() }));
			const results: Array<{
				won: boolean;
				completed: boolean;
				index: number;
			}> = [];
			for (const [i, f] of resultFiles.entries()) {
				const r = (await waitForFile(f)) as {
					won: boolean;
					completed: boolean;
					index: number;
				};
				expect(r.index).toBe(i);
				results.push(r);
			}
			expect(results.filter((r) => r.won)).toHaveLength(1); // ONE owner
			const winner = results.find((r) => r.won);
			expect(winner?.completed).toBe(true); // winner's ack accepted
			expect(results.filter((r) => !r.won)).toHaveLength(RACERS - 1);

			// Durable truth, read from yet another process: delivered, never
			// replayed, and the losers' CAS UPDATEs changed NOTHING (attempts 1).
			const probe = parseResult(
				await runDriver(["probe", dbPath, delegationId]),
			);
			expect(probe.exists).toBe(true);
			expect(probe.deliveryState).toBe("delivered");
			expect(probe.attempts).toBe(1); // exactly one racer ever moved the row
			expect(probe.claim).toBeNull(); // ack cleared the winning claim
			const verdict = parseResult(await runDriver(["restore-only", dbPath]));
			expect(verdict.restored).toBe(0); // delivered ⇒ never replayed
		} finally {
			for (const c of children) c.kill("SIGKILL");
		}
	});
});
