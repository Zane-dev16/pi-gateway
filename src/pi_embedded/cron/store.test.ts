// Behavior contracts for the JSON job store (07 §5.1 store; 07 §5.2 catchup
// window, one-shot grace, fire ownership; mark_job_run re-arm semantics).
// All timing via ManualClock — no wall clock in any assertion.

import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { isoToEpoch } from "./schedule.js";
import {
	CronJobStore,
	OneShotGraceError,
	defaultCronStorePaths,
	type CronDeliveryTarget,
	type CronStorePaths,
} from "./store.js";
import { ManualClock } from "./testing/manual-clock.js";

let dir: string;
let clock: ManualClock;
let store: CronJobStore;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-cron-store-"));
	clock = new ManualClock(1_770_000_000);
	store = new CronJobStore({
		paths: defaultCronStorePaths(dir),
		clock,
	});
});

afterEach(() => {
	store.close();
	rmSync(dir, { recursive: true, force: true });
});

async function makeIntervalJob(
	minutes = 10,
	name = "tick-job",
): Promise<string> {
	const job = await store.createJob({
		prompt: "do the thing",
		name,
		schedule: `every ${minutes}m`,
	});
	return job.id;
}

function readStoreFile(paths: CronStorePaths): Array<Record<string, unknown>> {
	let text = "";
	try {
		text = readFileSync(paths.jobsFile, "utf8");
	} catch {
		return []; // missing file ⇒ empty; the caller's length expectation fails
	}
	try {
		const parsed: unknown = JSON.parse(text);
		return Array.isArray(parsed)
			? (parsed as Array<Record<string, unknown>>)
			: [];
	} catch {
		return []; // unparsable bytes ⇒ empty; the caller's expectation fails
	}
}

describe("store substrate + round-trip", () => {
	it("persists jobs to <home>/cron/jobs.json (JSON file, NOT SQLite)", async () => {
		const paths = defaultCronStorePaths(dir);
		expect(paths.jobsFile).toBe(join(dir, "cron", "jobs.json"));
		const job = await store.createJob({
			prompt: "hello",
			name: "greet",
			schedule: "2026-02-03T14:00:00Z",
		});
		const raw = readStoreFile(paths) as Array<{
			id: string;
			schedule: { kind: string; run_at?: string };
		}>;
		expect(raw).toHaveLength(1);
		expect(raw[0]!.id).toBe(job.id);
		expect(raw[0]!.schedule.kind).toBe("once");
		expect(typeof raw[0]!.schedule.run_at).toBe("string"); // ISO parity
	});

	it("atomic save leaves no tmp residue and reload round-trips fields", async () => {
		const targets: CronDeliveryTarget[] = [
			{ platform: "telegram", chatId: "42" },
		];
		const created = await store.createJob({
			prompt: "p",
			name: "n",
			schedule: "every 5m",
			deliver: targets,
			attachToSession: false,
		});
		const loaded = await store.getJob(created.id);
		expect(loaded).not.toBeNull();
		expect(loaded?.deliver).toEqual(targets);
		expect(loaded?.attach_to_session).toBe(false);
		const cronDirFiles = readFileSync(join(dir, "cron", "jobs.json"), "utf8");
		expect(cronDirFiles).toContain('"n"');
		expect(existsSync(join(dir, "cron", "jobs.json.tmp"))).toBe(false);
	});
});

describe("one-shot grace at CREATE (just-inside fires, just-outside rejects)", () => {
	it("accepts a one-shot 119s in the past (inside the 120s window)", async () => {
		const schedule = {
			kind: "once" as const,
			runAtSeconds: clock.nowSeconds() - 119,
		};
		const job = await store.createJob({ prompt: "late-but-fresh", schedule });
		expect(job.next_run_at).not.toBeNull(); // scheduled — fires on next tick
	});

	it("rejects a one-shot 121s in the past with an explicit error", async () => {
		const schedule = {
			kind: "once" as const,
			runAtSeconds: clock.nowSeconds() - 121,
		};
		await expect(
			store.createJob({ prompt: "too-late", schedule }),
		).rejects.toBeInstanceOf(OneShotGraceError);
	});

	it("boundary is exact: exactly -120s still schedules", async () => {
		const schedule = {
			kind: "once" as const,
			runAtSeconds: clock.nowSeconds() - 120,
		};
		const job = await store.createJob({ prompt: "edge", schedule });
		expect(job.next_run_at).not.toBeNull();
	});

	it("update path rejects the same ghost shape (#59395)", async () => {
		const id = await makeIntervalJob();
		await expect(
			store.updateJob(id, {
				schedule: {
					kind: "once",
					runAtSeconds: clock.nowSeconds() - 5000,
				},
			}),
		).rejects.toBeInstanceOf(OneShotGraceError);
	});
});

describe("getDueJobs — catch-up vs fast-forward", () => {
	async function seedStale(minutes: number, latenessSeconds: number) {
		const id = await makeIntervalJob(minutes);
		// Park next_run_at latenessSeconds in the past.
		await store.mutate((jobs) => {
			const job = jobs.find((j) => j.id === id)!;
			job.next_run_at = new Date(
				(clock.nowSeconds() - latenessSeconds) * 1000,
			).toISOString();
		});
		return id;
	}

	it("within grace ⇒ plain catch-up: due, NOT fast-forwarded, counter untouched", async () => {
		const id = await seedStale(10, 200); // grace for 10m = 300s
		const report = await store.getDueJobs();
		expect(report.due).toHaveLength(1);
		expect(report.due[0]!.fastForwarded).toBe(false);
		expect(report.fastForwarded).toHaveLength(0);
		expect(await store.catchUpOccurrenceCount()).toBe(0);
		void id;
	});

	it("past grace ⇒ fast-forward: still fires ONCE now, slot jumps ahead, occurrence recorded", async () => {
		const id = await seedStale(10, 301); // just past the 300s grace
		const before = await store.getJob(id);
		const report = await store.getDueJobs();
		expect(report.due).toHaveLength(1);
		expect(report.due[0]!.fastForwarded).toBe(true);
		expect(report.fastForwarded).toEqual([id]);
		expect(await store.catchUpOccurrenceCount()).toBe(1);
		const after = await store.getJob(id);
		const afterSec = isoToEpoch(after!.next_run_at!)!;
		expect(afterSec).toBeGreaterThan(clock.nowSeconds()); // provisionally future
		expect(afterSec).toBeGreaterThan(isoToEpoch(before!.next_run_at!)!);
	});

	it("MIN-bound edge (every-2m job, grace=120): late by 120 catches up, 121 fast-forwards", async () => {
		const inside = await seedStale(2, 120);
		let report = await store.getDueJobs();
		expect(report.due.find((d) => d.job.id === inside)?.fastForwarded).toBe(
			false,
		);

		clock.advance(600); // move on; re-seed a second stale window past grace
		const outside = await seedStale(2, 121);
		report = await store.getDueJobs();
		expect(report.due.length).toBe(2);
		expect(report.due.find((d) => d.job.id === outside)?.fastForwarded).toBe(
			true,
		);
	});

	it("MAX-bound edge (every-8h job, grace=7200): late by 7200 catches up, 7201 fast-forwards", async () => {
		const inside = await seedStale(480, 7200);
		let report = await store.getDueJobs();
		expect(report.due.find((d) => d.job.id === inside)?.fastForwarded).toBe(
			false,
		);

		clock.advance(60_000);
		const outside = await seedStale(480, 7201);
		report = await store.getDueJobs();
		expect(report.due.find((d) => d.job.id === outside)?.fastForwarded).toBe(
			true,
		);
	});

	it("one-shot aged out while unrun becomes terminal (never a ghost)", async () => {
		const id2 = (
			await store.createJob({
				prompt: "x",
				schedule: { kind: "once", runAtSeconds: clock.nowSeconds() + 60 },
			})
		).id;
		clock.advance(3600); // ticker down an hour; grace long gone
		const report = await store.getDueJobs();
		expect(report.due.find((d) => d.job.id === id2)).toBeUndefined();
		const job = await store.getJob(id2);
		expect(job?.state).toBe("completed");
		expect(job?.enabled).toBe(false);
	});
});

describe("fire claims (execution tokens)", () => {
	it("exactly one fresh claim; loser rejected until TTL expiry (injected clock)", async () => {
		const id = await makeIntervalJob();
		const first = await store.claimJobForFire(id);
		expect(first?.fire_claim?.by).toBeTruthy();
		expect(await store.claimJobForFire(id)).toBeNull();

		clock.advance(299); // still fresh
		expect(await store.claimJobForFire(id)).toBeNull();

		clock.advance(2); // past 300s TTL
		const third = await store.claimJobForFire(id);
		expect(third?.fire_claim?.by).toBeTruthy();
		expect(third?.fire_claim?.by).not.toBe(first?.fire_claim?.by);
	});

	it("future-dated claim counts as STALE (clock-skew can never wedge a job forever)", async () => {
		const id = await makeIntervalJob();
		await store.mutate((jobs) => {
			const job = jobs.find((j) => j.id === id)!;
			job.fire_claim = {
				at: new Date((clock.nowSeconds() + 999_999) * 1000).toISOString(),
				by: "ghost:holder",
			};
		});
		const claimed = await store.claimJobForFire(id);
		expect(claimed?.fire_claim?.by).not.toBe("ghost:holder");
	});

	it("heartbeat refreshes only the owner; wrong owner and expired both fail", async () => {
		const id = await makeIntervalJob();
		const claim = await store.claimJobForFire(id);
		const owner = claim!.fire_claim!.by;
		clock.advance(100);
		expect(await store.heartbeatRunClaim(id, owner)).toBe(true);
		expect(await store.heartbeatRunClaim(id, "someone:else")).toBe(false);
		clock.advance(400); // lapsed without refresh
		expect(await store.heartbeatRunClaim(id, owner)).toBe(false);
	});

	it("clearRunClaim only frees OUR claim", async () => {
		const id = await makeIntervalJob();
		const claim = await store.claimJobForFire(id);
		expect(await store.clearRunClaim(id, "not:us")).toBe(false);
		expect(await store.clearRunClaim(id, claim!.fire_claim!.by)).toBe(true);
	});
});

describe("markJobRun", () => {
	it("success re-anchors next_run FROM COMPLETION and clears the claim", async () => {
		const id = await makeIntervalJob(10);
		clock.advance(700); // due
		const claim = await store.claimJobForFire(id);
		const owner = claim!.fire_claim!.by;
		clock.advance(120); // ran for 2 minutes
		expect(
			await store.markJobRun(id, { success: true, expectedFireOwner: owner }),
		).toBe(true);
		const job = await store.getJob(id)!;
		const anchoredAt = clock.nowSeconds(); // completion time
		expect(isoToEpoch(job!.next_run_at!)).toBe(anchoredAt + 600);
		expect(job!.last_status).toBe("ok");
		expect(job!.failure_streak).toBe(0);
		expect(job!.fire_claim ?? null).toBeNull();
	});

	it("error run keeps the recurring job scheduled (re-arm forward), streak increments", async () => {
		const id = await makeIntervalJob(10);
		clock.advance(700);
		const claim = await store.claimJobForFire(id);
		await store.markJobRun(id, {
			success: false,
			error: "provider exploded",
			expectedFireOwner: claim!.fire_claim!.by,
		});
		const job = await store.getJob(id)!;
		expect(job!.last_status).toBe("error");
		expect(job!.last_error).toBe("provider exploded");
		expect(job!.state).toBe("scheduled");
		expect(isoToEpoch(job!.next_run_at!)).toBe(clock.nowSeconds() + 600);
		expect(job!.failure_streak).toBe(1);
	});

	it("successful one-shot becomes terminal but stays inspectable (07 §5.3)", async () => {
		const job = await store.createJob({
			prompt: "once",
			schedule: { kind: "once", runAtSeconds: clock.nowSeconds() + 30 },
		});
		clock.advance(31);
		const claim = await store.claimJobForFire(job.id);
		await store.markJobRun(job.id, {
			success: true,
			expectedFireOwner: claim!.fire_claim!.by,
		});
		const done = await store.getJob(job.id);
		expect(done!.enabled).toBe(false);
		expect(done!.state).toBe("completed");
		expect(done!.next_run_at).toBeNull();
		expect(done!.last_run_at).not.toBeNull(); // permanently ineligible
	});

	it("stale-owner completion is DISCARDED (no double-write over the winner)", async () => {
		const id = await makeIntervalJob(10);
		clock.advance(700);
		const claimA = await store.claimJobForFire(id);
		const ownerA = claimA!.fire_claim!.by;
		// Another process steals after expiry and completes first.
		clock.advance(400);
		const claimB = await store.claimJobForFire(id);
		const ownerB = claimB!.fire_claim!.by;
		await store.markJobRun(id, { success: true, expectedFireOwner: ownerB });

		const beforeDiscard = await store.getJob(id);
		const staleWrite = await store.markJobRun(id, {
			success: true,
			expectedFireOwner: ownerA,
		});
		expect(staleWrite).toBe(false);
		const afterDiscard = await store.getJob(id);
		expect(afterDiscard).toEqual(beforeDiscard); // record untouched
	});

	it("delivery errors are tracked SEPARATELY from agent success", async () => {
		const id = await makeIntervalJob(10);
		clock.advance(700);
		const claim = await store.claimJobForFire(id);
		await store.markJobRun(id, {
			success: true,
			deliveryError: "telegram 502",
			expectedFireOwner: claim!.fire_claim!.by,
		});
		const job = await store.getJob(id)!;
		expect(job!.last_status).toBe("ok");
		expect(job!.last_delivery_error).toBe("telegram 502");
	});
});

describe("pause / resume / remove", () => {
	it("paused jobs are not runnable and do not become due", async () => {
		const id = await makeIntervalJob(10);
		await store.pauseJob(id, "maintenance");
		clock.advance(3600);
		expect((await store.getDueJobs()).due).toHaveLength(0);
		const resumed = await store.resumeJob(id);
		expect(resumed?.state).toBe("scheduled");
	});

	it("resume of a grace-aged-out one-shot refuses loudly", async () => {
		const job = await store.createJob({
			prompt: "x",
			schedule: { kind: "once", runAtSeconds: clock.nowSeconds() + 60 },
		});
		await store.pauseJob(job.id);
		clock.advance(5000);
		await expect(store.resumeJob(job.id)).rejects.toBeInstanceOf(
			OneShotGraceError,
		);
	});

	it("removeJob deletes only the named record", async () => {
		const a = await makeIntervalJob(10, "a");
		const b = await makeIntervalJob(20, "b");
		expect(await store.removeJob(a)).toBe(true);
		expect(await store.getJob(a)).toBeNull();
		expect(await store.getJob(b)).not.toBeNull();
	});
});
