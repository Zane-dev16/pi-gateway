// Behavior contracts for HandoffWatcher (run.py:_handoff_watcher port):
// claim→process→complete/fail per row, loser-tick skips, failure containment,
// and ALL timing through the injected clock — 5s startup delay, 2s poll
// cadence, deterministic stop().

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { StateStore } from "../../pi_state/index.js";
import { ManualClock } from "./testing/manual-clock.js";
import {
	HANDOFF_POLL_INTERVAL_MS,
	HANDOFF_STARTUP_DELAY_MS,
	HandoffQueue,
	HandoffWatcher,
	type HandoffRow,
} from "./index.js";

let dir: string;
let store: StateStore;
let queue: HandoffQueue;
const clock = new ManualClock();

beforeEach(async () => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-handoff-watcher-"));
	store = await StateStore.open(join(dir, "state.db"));
	queue = new HandoffQueue(store.db, { clock });
});

afterEach(async () => {
	await store.close();
});

async function seedPending(id: string): Promise<void> {
	clock.advance(1); // distinct started_at ⇒ oldest-first is observable
	await queue.ensureSessionRow(id);
	await queue.requestHandoff(id, "telegram");
}

function makeWatcher(
	processed: HandoffRow[] = [],
	failIds: string[] = [],
): HandoffWatcher {
	return new HandoffWatcher({
		queue,
		processRow: async (row) => {
			if (failIds.includes(row.id)) throw new Error(`boom ${row.id}`);
			processed.push(row);
		},
		clock,
	});
}

describe("HandoffWatcher.tick", () => {
	it("claims then processes then completes each pending row in order", async () => {
		await seedPending("a");
		await seedPending("b");
		const processed: HandoffRow[] = [];
		const w = makeWatcher(processed);

		const report = await w.tick();
		expect(report).toMatchObject({
			pending: 2,
			claimed: 2,
			completed: 2,
			failed: 0,
		});
		expect(processed.map((r) => r.id)).toEqual(["a", "b"]);
		expect(queue.getHandoffState("a")?.state).toBe("completed");
		expect(queue.getHandoffState("b")?.state).toBe("completed");
	});

	it("a row that throws lands in failed WITH its error payload; siblings still served", async () => {
		await seedPending("bad");
		await seedPending("good");
		const w = makeWatcher([], ["bad"]);

		const report = await w.tick();
		expect(report.completed).toBe(1);
		expect(report.failed).toBe(1);
		expect(report.failures).toEqual([{ sessionId: "bad", error: "boom bad" }]);
		expect(queue.getHandoffState("bad")).toMatchObject({
			state: "failed",
			error: "boom bad",
		});
		expect(queue.getHandoffState("good")?.state).toBe("completed");
	});

	it("non-Error throws stringify into the payload", async () => {
		await seedPending("s");
		const w = new HandoffWatcher({
			queue,
			processRow: async (): Promise<void> => {
				throw "plain string" as unknown as Error;
			},
			clock,
		});
		const report = await w.tick();
		expect(report.failures[0]?.error).toBe("plain string");
		expect(queue.getHandoffState("s")?.state).toBe("failed");
	});

	it("running rows are invisible: a crashed claimer's work never re-dispatches", async () => {
		await seedPending("crashed-mid-claim");
		expect(await queue.claimHandoff("crashed-mid-claim")).toBe(true);
		const processed: HandoffRow[] = [];
		const w = makeWatcher(processed);
		const report = await w.tick();
		expect(report.pending).toBe(0);
		expect(report.claimed).toBe(0);
		expect(processed).toHaveLength(0);
		expect(queue.getHandoffState("crashed-mid-claim")?.state).toBe("running"); // untouched
	});
});

describe("HandoffWatcher — exactly-one-winner under racing ticks", () => {
	it("N concurrent ticks over one pending row process it EXACTLY once", async () => {
		await seedPending("raced");
		const processed: HandoffRow[] = [];
		// Six independent watchers (≈ six gateways/ticks) share one store.
		const watchers = Array.from(
			{ length: 6 },
			() =>
				new HandoffWatcher({
					queue,
					processRow: async (row) => {
						processed.push(row);
					},
					clock,
				}),
		);
		const reports = await Promise.all(watchers.map((w) => w.tick()));
		const totalClaims = reports.reduce((sum, r) => sum + r.claimed, 0);
		expect(totalClaims).toBe(1); // ONE atomic winner
		expect(processed).toHaveLength(1);
		expect(queue.getHandoffState("raced")?.state).toBe("completed");
	});
});

describe("HandoffWatcher — loop timing via injected clock", () => {
	function countingQueue(base: HandoffQueue): {
		queue: HandoffQueue;
		tickCount(): number;
	} {
		let count = 0;
		const proxy = {
			listPendingHandoffs: (): HandoffRow[] => {
				count++;
				return base.listPendingHandoffs();
			},
			claimHandoff: (id: string) => base.claimHandoff(id),
			completeHandoff: (id: string) => base.completeHandoff(id),
			failHandoff: (id: string, error: string) => base.failHandoff(id, error),
		};
		return { queue: proxy as unknown as HandoffQueue, tickCount: () => count };
	}

	it("startup delay precedes the FIRST tick; cadence follows the interval", async () => {
		const { queue: cq, tickCount } = countingQueue(queue);
		const w = new HandoffWatcher({
			queue: cq,
			processRow: async () => undefined,
			clock,
		});
		w.start();
		await clock.advance(HANDOFF_STARTUP_DELAY_MS - 1);
		expect(tickCount()).toBe(0); // still inside the startup delay

		await clock.advance(1);
		expect(tickCount()).toBe(1); // delay elapsed → first tick

		await clock.advance(HANDOFF_POLL_INTERVAL_MS - 1);
		expect(tickCount()).toBe(1); // not due yet
		await clock.advance(1);
		expect(tickCount()).toBe(2); // second tick at interval boundary
		await clock.advance(HANDOFF_POLL_INTERVAL_MS * 3);
		expect(tickCount()).toBe(5);

		await w.stop();
		await clock.advance(HANDOFF_POLL_INTERVAL_MS * 10);
		expect(tickCount()).toBe(5); // stopped loops never tick again
	});

	it("stop() breaks an in-flight sleep immediately (no hung joins)", async () => {
		const w = makeWatcher([]);
		w.start(); // parks on the 5s startup sleep
		const joined = w.stop();
		await expect(
			Promise.race([
				joined.then(() => "stopped"),
				new Promise<string>((r) => setTimeout(() => r("hung"), 1000)),
			]),
		).resolves.toBe("stopped");
		expect(w.isRunning).toBe(false);
	});

	it("tick errors are contained: a broken queue never crashes the loop", async () => {
		const failing = {
			listPendingHandoffs: (): HandoffRow[] => {
				throw new Error("disk on fire");
			},
			claimHandoff: async (): Promise<boolean> => false,
			completeHandoff: async (): Promise<void> => undefined,
			failHandoff: async (): Promise<void> => undefined,
		};
		const w = new HandoffWatcher({
			queue: failing as unknown as HandoffQueue,
			processRow: async () => undefined,
			clock,
		});
		const report = await w.tick(); // must NOT throw
		expect(report.pending).toBe(0);
	});
});
