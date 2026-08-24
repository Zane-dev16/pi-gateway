// Two-OS-PROCESS contracts for the handoff claim (01 §3 posture: liveness /
// exactly-once claims need real processes):
//
//   1. CRASH MID-CLAIM (SIGKILL between the durable pending→running write and
//      any terminal write) ⇒ the row stays 'running' durably; a FRESH gateway
//      boot does NOT re-dispatch it (running rows are invisible to
//      list_pending_handoffs — exactly-once survives crashes); and the CLI's
//      unconditional timeout fail recovers it to a retryable state.
//   2. N LIVE PROCESSES race one atomic claim ⇒ EXACTLY ONE winner; the
//      losers' CAS UPDATEs change nothing.

import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

const DRIVER_TS = fileURLToPath(
	new URL("./testing/crash-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("../../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let dir: string;
let dbPath: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-handoff-twoproc-"));
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

describe("two-process handoff contracts (real processes)", () => {
	it("SIGKILL mid-claim ⇒ durable 'running' row; fresh boot never re-dispatches; CLI-timeout path recovers it", {
		timeout: 120_000,
	}, async () => {
		const sessionId = "cli-crash-victim";
		const claimedMarker = join(dir, "claimed");

		// BOOT 1 (the doomed gateway): seed pending, CLAIM, signal, get murdered.
		const doomed = spawn(
			process.execPath,
			[
				"--import",
				RESOLVE_MJS,
				DRIVER_TS,
				"claim-and-hold",
				dbPath,
				sessionId,
				"telegram",
				claimedMarker,
			],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		doomed.stderr?.resume();
		const claimedSignal = (await waitForFile(claimedMarker)) as {
			claimed: boolean;
		};
		expect(claimedSignal.claimed).toBe(true);
		expect(doomed.pid).toBeDefined();
		doomed.kill("SIGKILL");
		await new Promise<void>((resolvePromise) => {
			doomed.once("close", resolvePromise);
		});

		// BOOT 2 (a fresh gateway after the crash): durable truth is that the
		// claim SURVIVED — and a fresh watcher must NOT re-dispatch it.
		const probe1 = parseResult(await runDriver(["probe", dbPath, sessionId]));
		expect(probe1.state).toMatchObject({ state: "running" }); // durable claim
		expect(probe1.pendingIds).toEqual([]); // invisible ⇒ exactly-once

		// The poll-blocked CLI hits its deadline: the UNCONDITIONAL timeout
		// fail converges the stranded row to a retryable terminal state.
		const { StateStore } = await import("../../pi_state/index.js");
		const { HandoffCliClient, HandoffQueue } = await import("./index.js");
		const store = await StateStore.open(dbPath);
		try {
			const queue = new HandoffQueue(store.db);
			const client = new HandoffCliClient(queue);
			const outcome = await client.pollUntilTerminal(sessionId, {
				timeoutSeconds: 0,
			});
			expect(outcome).toEqual({ kind: "timeout", lastState: "running" });

			// Recovered: failed + reason recorded, retry request accepted.
			expect(queue.getHandoffState(sessionId)).toMatchObject({
				state: "failed",
				error: "timed out waiting for gateway",
			});
			expect(await queue.requestHandoff(sessionId, "telegram")).toBe(true);

			// And now the fresh watcher CAN dispatch it (retry path complete).
			const probe2 = parseResult(await runDriver(["probe", dbPath, sessionId]));
			expect(probe2.pendingIds).toEqual([sessionId]);
		} finally {
			await store.close();
		}
	});

	it("N processes race one atomic claim ⇒ exactly ONE winner", {
		timeout: 120_000,
	}, async () => {
		const sessionId = "cli-race";
		// Setup in a setup child (keeps parent free of driver imports).
		const setup = parseResult(
			await runDriver(["claim-and-hold-setup", dbPath, sessionId, "telegram"]),
		);
		void setup;

		const RACERS = 5;
		const goMarker = join(dir, "go");
		const children: ChildProcess[] = [];
		const resultFiles: string[] = [];
		try {
			for (let i = 0; i < RACERS; i++) {
				const resultFile = join(dir, `result-${i}`);
				resultFiles.push(resultFile);
				children.push(
					spawn(
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
							sessionId,
						],
						{ stdio: ["ignore", "pipe", "pipe"] },
					),
				);
			}
			// Fire the starting gun only once every racer is live.
			const { writeFileSync } = await import("node:fs");
			writeFileSync(goMarker, JSON.stringify({ t: Date.now() }));

			const results: Array<{ won: boolean; index: number }> = [];
			for (const f of resultFiles) {
				results.push((await waitForFile(f)) as { won: boolean; index: number });
			}
			expect(results.filter((r) => r.won)).toHaveLength(1); // ONE owner

			// Durable truth from another process: running (claimed), not pending.
			const probe = parseResult(await runDriver(["probe", dbPath, sessionId]));
			expect(probe.state).toMatchObject({ state: "running" });
			expect(probe.pendingIds).toEqual([]);
		} finally {
			for (const c of children) c.kill("SIGKILL");
		}
	});
});
