// Two-OS-PROCESS contracts for the tick lock — the ONE liveness claim that
// mocks cannot prove: while a real second process holds the tick lock, this
// process's ticker must SKIP SILENTLY (contention shape, jobs untouched), and
// the moment the holder releases, ticks proceed. Everything else about the
// fork stays in tick-lock.test.ts (injected faults).

import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { CronJobStore, defaultCronStorePaths } from "./store.js";
import { CronScheduler, type ScheduledJobRunner } from "./scheduler.js";
import { systemCronClock } from "./clock.js";

const DRIVER_TS = fileURLToPath(
	new URL("./testing/tick-lock-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("../../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-cron-twoproc-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

interface ChildRun {
	code: number | null;
	stdout: string;
	stderr: string;
}

function runDriver(args: string[], timeoutMs = 30_000): Promise<ChildRun> {
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
	if (lines.length === 0)
		throw new Error(`no RESULT_JSON; stderr=${run.stderr}`);
	const last = lines.at(-1)!;
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

async function waitForFile(path: string, timeoutMs = 30_000): Promise<unknown> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		try {
			return JSON.parse(readFileSync(path, "utf8")) as unknown;
		} catch {
			/* not there yet */
		}
		if (Date.now() > deadline) throw new Error(`timeout waiting ${path}`);
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

describe("two-process tick-lock mutual exclusion (real processes)", () => {
	it("foreign holder ⇒ silent contention skip; release ⇒ next tick proceeds", async () => {
		const cronDir = defaultCronStorePaths(dir).cronDir;
		const store = new CronJobStore({
			paths: defaultCronStorePaths(dir),
			clock: systemCronClock,
		});
		try {
			await store.createJob({
				prompt: "p",
				name: "two-proc",
				schedule: "every 1m",
			});
			// Make it due NOW.
			await store.mutate((jobs) => {
				for (const j of jobs) {
					j.next_run_at = new Date(
						Date.now() / 1000 - 5 * 60 * 1000,
					).toISOString();
				}
			});

			const ran: string[] = [];
			const runner: ScheduledJobRunner = {
				run: async ({ job }) => {
					ran.push(job.id);
					return { ok: true, outputText: "done" };
				},
				interrupt: () => Promise.resolve(true),
			};
			const scheduler = new CronScheduler({ store, runner });

			// CHILD: a REAL second process acquires the tick lock and holds it.
			const childPromise = runDriver([
				"--scenario",
				"hold-tick-lock",
				"--coord",
				dir,
				"--cron-dir",
				cronDir,
				"--acquired-marker",
				"child-acquired",
				"--release-marker",
				"parent-release",
			]);

			const acquired = (await waitForFile(join(dir, "child-acquired"))) as {
				acquired: boolean;
			};
			expect(acquired.acquired).toBe(true);

			// While held: our tick SKIPS silently — no run, no error, job intact.
			{
				const report = await scheduler.tickOnce();
				expect(report.skipped).toBe("lock_contention");
				expect(report.executed).toBe(0);
			}
			expect(ran).toEqual([]);

			// Release: the child exits, the OS drops its handle; we win.
			const { writeFileSync } = await import("node:fs");
			writeFileSync(join(dir, "parent-release"), "");
			await childPromise;
			// Small settle window for fs visibility (bounded, generous).
			for (let i = 0; i < 500 && ran.length === 0; i++) {
				const report = await scheduler.tickOnce();
				if (report.executed > 0) break;
				await new Promise<void>((r) => setTimeout(r, 10));
			}
			expect(ran.length).toBe(1); // exactly one execution after release
		} finally {
			store.close();
		}
	}, 120_000);

	it("a contender process observes contention then wins after release", async () => {
		const cronDir = defaultCronStorePaths(dir).cronDir;
		// Holder child (this test's own process holds instead — simpler): take
		// the lock HERE, spawn the contender driver, verify its observations.
		const holder = new (await import("./tick-lock.js")).TickLock(cronDir);
		const held = holder.acquire();
		if (!held.acquired) throw new Error("parent must acquire the lock first");

		const contender = runDriver([
			"--scenario",
			"steal-attempt",
			"--coord",
			dir,
			"--cron-dir",
			cronDir,
			"--release-marker",
			"drop-it",
		]);
		// Give the child a beat to attempt while we hold (it signals nothing;
		// its first attempt deterministically loses because we acquired first).
		await new Promise<void>((r) => setTimeout(r, 500));

		held.lease.release();
		const { writeFileSync } = await import("node:fs");
		writeFileSync(join(dir, "drop-it"), "");
		const result = parseResult(await contender);
		expect(result.firstOutcome).toBe("contention"); // held ⇒ skip shape
		expect(result.secondOutcome).toBe("won"); // released ⇒ clean handover
	}, 60_000);
});
