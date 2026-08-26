// End-to-end ticker contracts (07 §5.2 tick shape; 08 §11 cron bounds rows):
// at-most-once advance, estop sentinel, contention skip vs LOUD fd-exhaustion,
// claim-loss interrupts the stale run without double-write, error re-arm
// forward, loop backoff. All timing via ManualClock.

import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	CronJobStore,
	defaultCronStorePaths,
	type CronJobRecord,
} from "./store.js";
import { CronScheduler, type ScheduledJobRunner } from "./scheduler.js";
import type { TimestampActivityLog } from "./inactivity.js";
import { Gate, ManualClock } from "./testing/manual-clock.js";
import { TickLockAcquisitionError } from "./tick-lock.js";

let dir: string;
let clock: ManualClock;
let store: CronJobStore;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-cron-sched-"));
	clock = new ManualClock(1_770_000_000);
	store = new CronJobStore({ paths: defaultCronStorePaths(dir), clock });
});

afterEach(() => {
	store.close();
	rmSync(dir, { recursive: true, force: true });
});

function scriptedRunner(
	behavior: (
		job: CronJobRecord,
	) => Promise<{ ok: boolean; outputText?: string; error?: string }>,
	interrupts?: Array<string>,
): ScheduledJobRunner {
	return {
		run: ({ job }) => behavior(job),
		interrupt: (jobId) => {
			interrupts?.push(jobId);
			return Promise.resolve(true);
		},
	};
}

async function dueIntervalJob(minutes = 10): Promise<CronJobRecord> {
	const job = await store.createJob({
		prompt: "the prompt",
		name: "nightly",
		schedule: `every ${minutes}m`,
		deliver: [{ platform: "telegram", chatId: "42" }],
	});
	clock.advance(minutes * 60 + 1);
	return job;
}

function isoAfter(iso: string | null | undefined): number {
	return Math.round(new Date(iso ?? "").getTime() / 1000);
}

describe("tickOnce happy path", () => {
	it("due job runs ONCE through the runner; results marked; wrapped delivery sent", async () => {
		const job = await dueIntervalJob();
		const sent: string[] = [];
		const scheduler = new CronScheduler({
			store,
			clock,
			env: {},
			runner: scriptedRunner(async () => ({
				ok: true,
				outputText: "fresh data",
			})),
			deliverySink: {
				deliver: async (_target, content) => {
					sent.push(content);
					return null;
				},
			},
		});

		const report = await scheduler.tickOnce();
		expect(report.executed).toBe(1);
		expect(report.results[0]?.status).toBe("ok");
		expect(report.results[0]?.wroteResults).toBe(true);
		expect(sent).toHaveLength(1);
		expect(sent[0]).toContain("Cronjob Response: nightly");

		const after = await store.getJob(job.id);
		expect(after?.last_status).toBe("ok");
		expect(isoAfter(after?.next_run_at)).toBe(clock.nowSeconds() + 600);

		// AT-MOST-ONCE: an immediate second tick finds nothing due.
		const second = await scheduler.tickOnce();
		expect(second.executed).toBe(0);
		expect(second.skipped).toBe("no_due_jobs");
	});

	it("failed run re-arms FORWARD with status=error and a growing failure streak", async () => {
		await dueIntervalJob();
		const scheduler = new CronScheduler({
			store,
			clock,
			env: {},
			runner: scriptedRunner(async () => ({
				ok: false,
				error: "provider down",
			})),
		});
		const report = await scheduler.tickOnce();
		expect(report.results[0]?.status).toBe("error");
		const job = (await store.listJobs())[0]!;
		expect(job.last_status).toBe("error");
		expect(job.state).toBe("scheduled"); // recurring jobs never silently stop
		expect(job.failure_streak).toBe(1);
	});
});

describe("tick gating", () => {
	it("estop sentinel makes due jobs WAIT untouched (in-flight semantics preserved)", async () => {
		await dueIntervalJob();
		writeFileSync(join(dir, "cron", ".estop"), "");
		const scheduler = new CronScheduler({
			store,
			clock,
			env: {},
			runner: scriptedRunner(async () => {
				throw new Error("must not run under estop");
			}),
		});
		const report = await scheduler.tickOnce();
		expect(report.skipped).toBe("estop");
		expect(report.executed).toBe(0);
		const job = (await store.listJobs())[0]!;
		expect(job.last_run_at ?? null).toBeNull(); // untouched
	});

	it("lock CONTENTION skips silently and runs nothing (#87644 healthy half)", async () => {
		await dueIntervalJob();
		let acquires = 0;
		const scheduler = new CronScheduler({
			store,
			clock,
			env: {},
			runner: scriptedRunner(async () => ({ ok: true })),
			tickLock: {
				acquire: () => {
					acquires++;
					return { acquired: false as const, kind: "contention" as const };
				},
			} as never,
		});
		const report = await scheduler.tickOnce();
		expect(acquires).toBe(1);
		expect(report.skipped).toBe("lock_contention");
		expect(report.executed).toBe(0);
	});

	it("fd EXHAUSTION throws LOUDLY out of tickOnce — never a healthy-looking skip", async () => {
		await dueIntervalJob();
		const fault = Object.assign(new Error("EMFILE: too many open files"), {
			code: "EMFILE",
		});
		const scheduler = new CronScheduler({
			store,
			clock,
			env: {},
			runner: scriptedRunner(async () => ({ ok: true })),
			tickLock: {
				acquire: () => {
					throw new TickLockAcquisitionError("fd_exhaustion", fault);
				},
			} as never,
		});
		await expect(scheduler.tickOnce()).rejects.toBeInstanceOf(
			TickLockAcquisitionError,
		);
	});
});

describe("true-inactivity runaway bound inside a tick", () => {
	it("a wedged job is interrupted AT the inactivity bound and recorded interrupted", async () => {
		const job = await dueIntervalJob();
		const limitGate = new Gate();
		const interrupts: string[] = [];
		let runnerSawActivity: TimestampActivityLog | null = null;
		const runner: ScheduledJobRunner = {
			run: async ({ activity }) => {
				runnerSawActivity = activity;
				activity.touch(clock.nowSeconds());
				await limitGate.wait; // wedge forever until the hard interrupt
				return { ok: false, error: "aborted" };
			},
			interrupt: (jobId) => {
				interrupts.push(jobId);
				limitGate.open();
				return Promise.resolve(true);
			},
		};
		// Default limit: 600s (no HERMES_CRON_TIMEOUT in env).
		const scheduler = new CronScheduler({ store, clock, env: {}, runner });

		const report = await scheduler.tickOnce(); // monitor polls self-drive logical time

		expect(report.results[0]?.status).toBe("interrupted");
		expect(report.results[0]?.wroteResults).toBe(true);
		expect(interrupts).toEqual([job.id]); // request_hard_interrupt parity
		const after = await store.getJob(job.id);
		expect(after?.last_status).toBe("interrupted");
		// Re-arm forward despite the interruption (recurring job keeps its slot).
		expect(after?.state).toBe("scheduled");
		expect(isoAfter(after?.next_run_at)).toBe(clock.nowSeconds() + 600);
		void runnerSawActivity;
	});

	it("HERMES_CRON_TIMEOUT=0 runs an unlimited job to completion", async () => {
		const job = await dueIntervalJob();
		const runner: ScheduledJobRunner = {
			run: async ({ activity }) => {
				// Active the whole time: touch each poll cycle for 2 simulated hours.
				for (let i = 0; i < 1440; i++) {
					await clock.sleepMs(5000);
					activity.touch(clock.nowSeconds());
				}
				return { ok: true, outputText: "finally done" };
			},
			interrupt: () => Promise.resolve(true),
		};
		const scheduler = new CronScheduler({
			store,
			clock,
			env: { HERMES_CRON_TIMEOUT: "0" },
			runner,
		});
		const report = await scheduler.tickOnce();
		expect(report.results[0]?.status).toBe("ok");
		expect((await store.getJob(job.id))?.last_status).toBe("ok");
	}, 60_000);
});

describe("fire ownership: claim-loss interrupts the stale run WITHOUT double-write", () => {
	it("a stolen claim aborts the stale runner; only the winner's completion lands", async () => {
		const job = await dueIntervalJob();
		const stolenGate = new Gate();
		let stole = false;
		let interrupted = false;

		const runner: ScheduledJobRunner = {
			run: async ({ activity }) => {
				activity.touch(clock.nowSeconds());
				// Mid-run, ANOTHER ticker wins the claim (our TTL lapsed during a
				// stall). The record now names owner B.
				if (!stole) {
					stole = true;
					await store.mutate((jobs) => {
						const j = jobs.find((x) => x.id === job.id)!;
						j.fire_claim = {
							at: new Date(clock.nowSeconds() * 1000).toISOString(),
							by: "winner:B",
						};
					});
				}
				await stolenGate.wait;
				return { ok: true, outputText: "STALE OUTPUT" }; // must never land
			},
			interrupt: () => {
				interrupted = true;
				stolenGate.open();
				return Promise.resolve(true);
			},
		};
		const scheduler = new CronScheduler({ store, clock, env: {}, runner });

		const report = await scheduler.tickOnce();

		expect(report.results[0]?.status).toBe("claim_lost");
		expect(report.results[0]?.wroteResults).toBe(false);
		expect(interrupted).toBe(true); // stale run WAS interrupted
		const midRecord = await store.getJob(job.id);
		expect(midRecord?.last_run_at ?? null).toBeNull(); // NO double-write
		expect(midRecord?.last_status ?? null).toBeNull();

		// The WINNER completes normally: its completion lands exactly once.
		await store.markJobRun(job.id, {
			success: true,
			expectedFireOwner: "winner:B",
		});
		const final = await store.getJob(job.id);
		expect(final?.last_status).toBe("ok");
		expect(final?.last_run_at).not.toBeNull();
	});

	it("owner changing BETWEEN completion and mark discards the stale completion", async () => {
		const job = await dueIntervalJob();
		let ran = false;
		const stealingRunner: ScheduledJobRunner = {
			run: async (ctx) => {
				ctx.activity.touch(clock.nowSeconds());
				ran = true;
				// Owner swaps to a foreign ticker right as the run completes.
				await store.mutate((jobs) => {
					const j = jobs.find((x) => x.id === job.id)!;
					j.fire_claim = {
						at: new Date(clock.nowSeconds() * 1000).toISOString(),
						by: "winner:C",
					};
				});
				return { ok: true, outputText: "late result" };
			},
			interrupt: () => Promise.resolve(true),
		};
		const scheduler2 = new CronScheduler({
			store,
			clock,
			env: {},
			runner: stealingRunner,
		});

		const report = await scheduler2.tickOnce();
		expect(report.results[0]?.status).toBe("stale_mark_discarded");
		expect(report.results[0]?.wroteResults).toBe(false);
		expect(ran).toBe(true);
		const record = await store.getJob(job.id);
		expect(record?.fire_claim?.by).toBe("winner:C"); // untouched by the loser
		expect(record?.last_run_at ?? null).toBeNull();
	});
});

describe("ticker loop lifecycle (#87644 backoff ownership)", () => {
	it("loop survives EMFILE failures with escalating backoff, then recovers; stop() breaks sleep", async () => {
		const job = await dueIntervalJob(1); // due immediately
		let failures = 0;
		const fault = Object.assign(new Error("EMFILE: too many open files"), {
			code: "EMFILE",
		});
		const reclaimCalls: number[] = [];
		let healthy = false;
		const scheduler = new CronScheduler({
			store,
			clock,
			env: {},
			intervalSeconds: 60,
			runner: scriptedRunner(async () => ({ ok: true })),
			tickLock: {
				acquire: () => {
					if (healthy)
						return {
							acquired: true as const,
							lease: { release: () => undefined },
						};
					failures++;
					if (failures >= 3) healthy = true;
					throw new TickLockAcquisitionError("fd_exhaustion", fault);
				},
			} as never,
		});
		// Observe reclamation through noteTickFailure's default gc hook path.
		const gcSpy = (): void => {
			reclaimCalls.push(reclaimCalls.length + 1);
		};
		(globalThis as { gc?: () => void }).gc = gcSpy;

		scheduler.start();
		try {
			// Tick1 fail → wait 60; tick2 fail → wait 120... after 3rd failure
			// healthy. Advance logical time to let the loop reach it.
			for (let i = 0; i < 40 && !healthy; i++) await clock.sleepMs(30_000);
			expect(healthy).toBe(true);
			expect(failures).toBeGreaterThanOrEqual(3);
			for (
				let i = 0;
				i < 40 && !(await store.getJob(job.id))?.last_run_at;
				i++
			) {
				await clock.sleepMs(60_000);
			}
			const ran = await store.getJob(job.id);
			expect(ran?.last_status).toBe("ok"); // recovered and executed
			expect(reclaimCalls.length).toBeGreaterThanOrEqual(
				failures - (healthy ? 0 : 0),
			);
		} finally {
			await scheduler.stop(); // deterministic break of a blocked sleep
			delete (globalThis as { gc?: () => void }).gc;
		}
		expect(scheduler.isRunning).toBe(false);
	}, 60_000);

	it("start is idempotent and stop() settles even mid-sleep", async () => {
		const scheduler = new CronScheduler({
			store,
			clock,
			env: {},
			intervalSeconds: 3600,
			runner: scriptedRunner(async () => ({ ok: true })),
		});
		scheduler.start();
		expect(scheduler.isRunning).toBe(true);
		scheduler.start(); // second call is a no-op
		await scheduler.stop();
		expect(scheduler.isRunning).toBe(false);
	});
});

describe("in-flight visibility (gateway shutdown-drain input, #60432/#82161)", () => {
	it("a mid-run job is visible via the scheduler AND its handle; cleared only after full settlement", async () => {
		const job = await dueIntervalJob();
		const runGate = new Gate();
		let deliveryDone = false;
		let sawInflightDuringRun = false;
		const scheduler = new CronScheduler({
			store,
			clock,
			env: {},
			runner: {
				run: async () => {
					// The run must be visible to the drain WHILE it works…
					sawInflightDuringRun = scheduler.inflightJobCount === 1;
					await runGate.wait;
					return { ok: true, outputText: "late result" };
				},
				interrupt: async () => true,
			},
			deliverySink: {
				deliver: async () => {
					// …and still visible during the deliver tail (the phase a
					// pre-drain stop would amputate — #82232 shape).
					deliveryDone = scheduler.inflightJobCount === 1;
					return null;
				},
			},
		});
		expect(scheduler.inflightJobCount).toBe(0);
		expect(scheduler.runningJobs).toEqual([]);
		const handle = scheduler.handle();
		expect(handle.inflightCount()).toBe(0);

		const tick = scheduler.tickOnce();
		await new Promise<void>((r) => setImmediate(r)); // let the run start + park on the gate
		expect(sawInflightDuringRun).toBe(true);
		expect(scheduler.inflightJobCount).toBe(1);
		expect(scheduler.runningJobs).toEqual([job.id]);
		expect(handle.inflightCount()).toBe(1); // drain reads the SAME counter

		runGate.open();
		const report = await tick;
		expect(report.executed).toBe(1);
		expect(deliveryDone).toBe(true); // count stayed up through delivery
		expect(scheduler.inflightJobCount).toBe(0);
		expect(handle.inflightCount()).toBe(0);
	});

	it("a claim-lost stale run is registered too — the drain sees it while it unwinds", async () => {
		const job = await dueIntervalJob();
		const stolenGate = new Gate();
		let stole = false;
		let sawInflight = false;
		const runner: ScheduledJobRunner = {
			run: async ({ activity }) => {
				activity.touch(clock.nowSeconds());
				if (!stole) {
					stole = true;
					await store.mutate((jobs) => {
						const j = jobs.find((x) => x.id === job.id)!;
						j.fire_claim = {
							at: new Date(clock.nowSeconds() * 1000).toISOString(),
							by: "winner:B",
						};
					});
				}
				sawInflight = scheduler.inflightJobCount === 1;
				await stolenGate.wait;
				return { ok: true };
			},
			interrupt: () => {
				stolenGate.open();
				return Promise.resolve(true);
			},
		};
		const scheduler = new CronScheduler({ store, clock, env: {}, runner });

		// The bound's heartbeat poll detects the stolen claim and fires the
		// interrupt itself (which opens the gate); monitor polls self-drive
		// logical time.
		const report = await scheduler.tickOnce();

		expect(report.results[0]?.status).toBe("claim_lost");
		expect(sawInflight).toBe(true);
		expect(scheduler.inflightJobCount).toBe(0); // finally always releases
	});
});
