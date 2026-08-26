// Behavior contracts for the retry scheduler (injected clock only — no test
// and no logic path reads a wall clock). Covers: backoff gating under the
// proposed schedule, dead-owner immediate recovery, claim-time resume-clear
// parity (#91969 — every claimed session key cleared BEFORE any send),
// deliverable-platform filtering without budget burn, and start/stop loop
// lifecycle.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { StateStore } from "../../pi_state/index.js";
import { ManualClock, ScriptedSender } from "./testing/manual-clock.js";
import {
	DeliveryLedger,
	ObligationRetryScheduler,
	type DeliveryLedgerOptions,
	type NewObligation,
	type OwnerStamp,
} from "./index.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-obligations-sched-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

const SELF_STAMP: OwnerStamp = { pid: 980_001, startedAt: 777 };

function fakeEnv(): Pick<
	DeliveryLedgerOptions,
	"processAlive" | "processStartTime"
> {
	return {
		processAlive: (pid) => pid === SELF_STAMP.pid,
		processStartTime: (pid) =>
			pid === SELF_STAMP.pid ? SELF_STAMP.startedAt : null,
	};
}

async function makeScaffold(): Promise<{
	ledger: DeliveryLedger;
	store: StateStore;
	clock: ManualClock;
}> {
	const store = await StateStore.open(
		join(dir, `db-${Math.random().toString(36).slice(2)}.db`),
	);
	const clock = new ManualClock(1_000_000);
	const ledger = new DeliveryLedger(store.db, {
		clock,
		selfStamp: SELF_STAMP,
		...fakeEnv(),
	});
	return { ledger, store, clock };
}

function sample(overrides: Partial<NewObligation> = {}): NewObligation {
	return {
		sessionKey: "sk",
		platform: "telegram",
		chatId: "c1",
		threadId: null,
		content: "owed text",
		messageRef: "m1",
		...overrides,
	};
}

/** Poll on macrotasks until cond() holds (loop lifecycle needs real yields). */
async function until(cond: () => boolean, tries = 2_000): Promise<void> {
	for (let i = 0; i < tries && !cond(); i++) {
		await new Promise<void>((resolvePromise) => {
			setTimeout(resolvePromise, 1);
		});
	}
}

describe("backoff-gated retries under an injected clock", () => {
	it("never busy-retries; slots follow 60s/240s/960s and the cap abandons on sight", async () => {
		const { ledger, store, clock } = await makeScaffold();
		const sender = new ScriptedSender().alwaysFail("platform down");
		const scheduler = new ObligationRetryScheduler(ledger, sender.bind(), {
			clock,
		});

		const id = await ledger.record(sample(), {
			nowSeconds: clock.nowSeconds(),
		});
		const t0 = clock.nowSeconds();

		// First slot: due at t0+60. Anything earlier sends NOTHING.
		await scheduler.tick(t0 + 10);
		expect(sender.callCount).toBe(0);
		await scheduler.tick(t0 + 59);
		expect(sender.callCount).toBe(0);
		await scheduler.tick(t0 + 61);
		expect(sender.callCount).toBe(1);
		expect(ledger.stateOf(id)).toBe("failed");
		expect(ledger.row(id)?.attempts).toBe(1);

		// Second slot: failed_at(+61) + 240 → due t0+301.
		await scheduler.tick(t0 + 70);
		await scheduler.tick(t0 + 250);
		expect(sender.callCount).toBe(1);
		await scheduler.tick(t0 + 302);
		expect(sender.callCount).toBe(2);
		expect(ledger.row(id)?.attempts).toBe(2);

		// Third slot: failed_at(+302) + 960 → due t0+1262.
		await scheduler.tick(t0 + 800);
		expect(sender.callCount).toBe(2);
		await scheduler.tick(t0 + 1300);
		expect(sender.callCount).toBe(3);
		expect(ledger.row(id)?.attempts).toBe(3);
		expect(ledger.stateOf(id)).toBe("failed");

		// Budget exhausted: examined on the very next tick → abandoned, no send.
		await scheduler.tick(t0 + 1400);
		expect(sender.callCount).toBe(3); // 4th send refused
		expect(ledger.stateOf(id)).toBe("abandoned");
		await store.close();
	});

	it("inter-send gaps never shrink below the schedule (clock-driven ticks)", async () => {
		const { ledger, store, clock } = await makeScaffold();
		const sendTimes: number[] = [];
		const scheduler = new ObligationRetryScheduler(
			ledger,
			async () => {
				sendTimes.push(clock.nowSeconds());
				return { ok: false, error: "down" };
			},
			{ clock },
		);
		await ledger.record(sample(), { nowSeconds: clock.nowSeconds() });

		const stepSeconds = 45;
		for (let step = 0; step <= 40; step++) {
			clock.set(clock.nowSeconds() + stepSeconds); // time moves ONLY here
			await scheduler.tick();
		}
		expect(sendTimes.length).toBeGreaterThanOrEqual(2);
		const gaps: number[] = [];
		for (let i = 1; i < sendTimes.length; i++) {
			const prev = sendTimes[i - 1];
			const curr = sendTimes[i];
			if (prev !== undefined && curr !== undefined) gaps.push(curr - prev);
		}
		// First slot ≥ base backoff minus sampling granularity.
		expect(gaps[0]).toBeGreaterThanOrEqual(60 - stepSeconds);
		// Gaps grow monotonically (60 → 240 → 960 …) within sampling noise.
		for (let i = 1; i < gaps.length; i++) {
			const prevGap = gaps[i - 1];
			const gap = gaps[i];
			if (prevGap !== undefined && gap !== undefined) {
				expect(gap).toBeGreaterThanOrEqual(prevGap - stepSeconds * 2);
			}
		}
		await store.close();
	});
});

describe("dead-owner immediate recovery vs backoff", () => {
	it("claims dead-owned rows immediately even when young (restart-boundary parity)", async () => {
		const { ledger, store, clock } = await makeScaffold();
		store.db
			.prepare(
				`INSERT INTO delivery_obligations
				   (obligation_id, session_key, platform, chat_id, thread_id, content,
				    state, attempts, created_at, updated_at, owner_pid, owner_started_at)
				 VALUES ('orphan', 'sk', 'telegram', 'c', NULL, 'lost text',
				         'attempting', 1, ?, ?, 970_001, 42)`,
			)
			.run(clock.nowSeconds(), clock.nowSeconds());
		const scheduler = new ObligationRetryScheduler(
			ledger,
			new ScriptedSender().bind(),
			{ clock },
		);
		const report = await scheduler.tick(clock.nowSeconds() + 1); // no backoff wait
		expect(report.recovered).toBe(1);
		expect(report.results[0]?.ok).toBe(true);
		await store.close();
	});
});

describe("claim-time resume-clear parity (#91969)", () => {
	it("clears resume_pending for EVERY claimed session key BEFORE any send, on both claim paths", async () => {
		const { ledger, store, clock } = await makeScaffold();
		const events: string[] = [];
		// Dead-owned row → sweepRecoverable path; self-owned pending row past
		// its first backoff slot → claimDueRetries path. Both must be cleared.
		store.db
			.prepare(
				`INSERT INTO delivery_obligations
				   (obligation_id, session_key, platform, chat_id, thread_id, content,
				    state, attempts, created_at, updated_at, owner_pid, owner_started_at)
				 VALUES ('orphan', 'sk-dead', 'telegram', 'c', NULL, 'lost text',
				         'attempting', 1, ?, ?, 970_001, 42)`,
			)
			.run(clock.nowSeconds(), clock.nowSeconds());
		await ledger.record(
			sample({ sessionKey: "sk-live", content: "owed", messageRef: "m2" }),
			{ nowSeconds: clock.nowSeconds() },
		);
		const t0 = clock.nowSeconds();
		const scheduler = new ObligationRetryScheduler(
			ledger,
			async (req) => {
				events.push(`send:${req.sessionKey}`);
				return { ok: true };
			},
			{
				clock,
				clearResumePending: async (key) => {
					events.push(`clear:${key}`);
				},
			},
		);

		const report = await scheduler.tick(t0 + 61); // dead-owner immediate + first retry slot

		expect(report.recovered).toBe(1);
		expect(report.retried).toBe(1);
		// Exact interleaving: clears complete before the FIRST send (a hung
		// redelivery must never reopen the boot-resume replay window).
		expect(events).toEqual([
			"clear:sk-dead",
			"clear:sk-live",
			"send:sk-dead",
			"send:sk-live",
		]);
		expect(report.results.map((r) => r.ok)).toEqual([true, true]);
		await store.close();
	});

	it("isolates a throwing key: later keys still clear and ALL claimed rows still deliver", async () => {
		const { ledger, store, clock } = await makeScaffold();
		const cleared: string[] = [];
		for (const [id, key] of [
			["bad-row", "sk-bad"],
			["good-row", "sk-good"],
		] as const) {
			store.db
				.prepare(
					`INSERT INTO delivery_obligations
					   (obligation_id, session_key, platform, chat_id, thread_id, content,
					    state, attempts, created_at, updated_at, owner_pid, owner_started_at)
					 VALUES (?, ?, 'telegram', 'c', NULL, 'text', 'pending', 0, ?, ?, 970_001, 42)`,
				)
				.run(id, key, clock.nowSeconds(), clock.nowSeconds());
		}
		const sender = new ScriptedSender();
		const scheduler = new ObligationRetryScheduler(ledger, sender.bind(), {
			clock,
			clearResumePending: async (key) => {
				cleared.push(key);
				if (key === "sk-bad") throw new Error("store offline");
			},
		});

		const report = await scheduler.tick(clock.nowSeconds() + 1);

		// The failing key was attempted, the remaining key still cleared…
		expect(cleared).toEqual(["sk-bad", "sk-good"]);
		// …and neither the sends nor the settles were blocked.
		expect(report.recovered).toBe(2);
		expect(sender.calls.map((c) => c.sessionKey)).toEqual([
			"sk-bad",
			"sk-good",
		]);
		expect(ledger.stateOf("bad-row")).toBe("delivered");
		expect(ledger.stateOf("good-row")).toBe("delivered");
		await store.close();
	});

	it("skips empty session keys but still drives those rows (if not session_key: continue)", async () => {
		const { ledger, store, clock } = await makeScaffold();
		store.db
			.prepare(
				`INSERT INTO delivery_obligations
				   (obligation_id, session_key, platform, chat_id, thread_id, content,
				    state, attempts, created_at, updated_at, owner_pid, owner_started_at)
				 VALUES ('anon', '', 'telegram', 'c', NULL, 'keyless',
				         'attempting', 1, ?, ?, 970_001, 42)`,
			)
			.run(clock.nowSeconds(), clock.nowSeconds());
		const cleared: string[] = [];
		const sender = new ScriptedSender();
		const scheduler = new ObligationRetryScheduler(ledger, sender.bind(), {
			clock,
			clearResumePending: async (key) => {
				cleared.push(key);
			},
		});

		const report = await scheduler.tick(clock.nowSeconds() + 1);

		expect(cleared).toEqual([]); // nothing to clear for a keyless row
		expect(sender.calls).toHaveLength(1);
		expect(report.results[0]?.ok).toBe(true);
		await store.close();
	});

	it("never invokes the hook when a tick claims nothing", async () => {
		const { ledger, store, clock } = await makeScaffold();
		let calls = 0;
		const scheduler = new ObligationRetryScheduler(
			ledger,
			new ScriptedSender().bind(),
			{
				clock,
				clearResumePending: async () => {
					calls++;
				},
			},
		);
		const report = await scheduler.tick(clock.nowSeconds() + 1);
		expect(report.recovered).toBe(0);
		expect(report.retried).toBe(0);
		expect(calls).toBe(0);
		await store.close();
	});
});

describe("deliverable-platform filter through ticks", () => {
	it("does not spend the redelivery budget while the platform is absent", async () => {
		const { ledger, store, clock } = await makeScaffold();
		const platforms = new Set<string>();
		const sender = new ScriptedSender().alwaysFail("unused");
		const scheduler = new ObligationRetryScheduler(ledger, sender.bind(), {
			clock,
			deliverablePlatforms: () => platforms,
		});
		const id = await ledger.record(
			sample({ platform: "discord", content: "hold", messageRef: "m9" }),
			{ nowSeconds: clock.nowSeconds() },
		);
		const t0 = clock.nowSeconds();

		// Hours past every backoff slot — still untouched while absent, and
		// comfortably inside the 24h stale window so nothing gets poisoned.
		await scheduler.tick(t0 + 3_600);
		await scheduler.tick(t0 + 7_200);
		expect(sender.callCount).toBe(0);
		expect(ledger.row(id)?.attempts).toBe(0);
		expect(ledger.stateOf(id)).toBe("pending");

		platforms.add("discord"); // adapter connects late — no restart needed
		const report = await scheduler.tick(t0 + 7_201);
		expect(report.retried).toBe(1);
		expect(sender.callCount).toBe(1);
		await store.close();
	});
});

describe("background loop lifecycle", () => {
	it("start fires the due tick immediately; stop breaks sleep and terminates cleanly", async () => {
		const { ledger, store, clock } = await makeScaffold();
		const sender = new ScriptedSender(); // ok outcomes
		const scheduler = new ObligationRetryScheduler(ledger, sender.bind(), {
			clock,
			intervalSeconds: 15,
		});
		const t0 = clock.nowSeconds();
		await ledger.record(sample({ content: "loop", messageRef: "mL" }), {
			nowSeconds: t0,
		});

		// Jump PAST the first backoff slot so the loop's very first tick sends.
		clock.set(t0 + 61);
		scheduler.start();
		expect(scheduler.isRunning).toBe(true);
		await until(() => sender.callCount === 1);
		expect(sender.callCount).toBe(1);
		expect(sender.calls[0]?.content).toBe("loop");

		await scheduler.stop(); // must resolve even with a pending 15s sleep
		expect(scheduler.isRunning).toBe(false);
		const callsAfterStop = sender.callCount;

		// Time marches far past several would-be slots — nothing more sends.
		clock.set(t0 + 100_000);
		await until(() => sender.callCount > callsAfterStop, 40);
		expect(sender.callCount).toBe(callsAfterStop);
		await store.close();
	});
});
