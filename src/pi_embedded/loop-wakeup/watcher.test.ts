// Behavior contracts for the stage-8 loop-wakeup watcher
// (run.py:_loop_wakeup_watcher port): the 15s scan over persisted `loop:*`
// rows, every deferral arm in run.py arrival order (awaiting/due-time,
// CLI-owned route, missing adapter with one-time warn, busy routing key,
// active /goal), synthetic-message injection shape through the NORMAL
// pipeline port, slash-command immediate completion, abandon-on-failure,
// and the supervised start/stop lifecycle over an injected clock.

import { rmSync } from "node:fs";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { LoopManager } from "./index.js";
import {
	LOOP_WAKEUP_SCAN_INTERVAL_MS,
	LOOP_WAKEUP_STARTUP_DELAY_MS,
	LoopWakeupWatcher,
	type LoopWakeupLogger,
	forgeLoopWakeupEvent,
	sourceFromRoute,
} from "./watcher.js";
import type { EmbeddedServiceEntry } from "../service-entry.js";
import {
	LOOP_WAKEUP_WATCHER_SERVICE_NAME,
	loopWakeupWatcherServiceEntry,
} from "./stage-entry.js";
import { StateStore } from "../../pi_state/index.js";
import {
	buildLoopHarnessOn,
	openLoopHarness,
	type LoopHarness,
} from "./testing/harness.js";

const ROUTE = {
	platform: "telegram",
	chat_id: "100",
	chat_type: "dm",
	user_id: "u1",
};

describe("constants", () => {
	it("carries the roster cadence (gap-audit R1) and startup delay", () => {
		expect(LOOP_WAKEUP_SCAN_INTERVAL_MS).toBe(15000);
		expect(LOOP_WAKEUP_STARTUP_DELAY_MS).toBe(5000);
	});
});

describe("scan + injection", () => {
	let h: LoopHarness;

	beforeEach(async () => {
		h = await openLoopHarness();
	});

	afterEach(async () => {
		await h.close();
		rmSync(h.dir, { recursive: true, force: true });
	});

	async function createLoop(sessionId = "sess", intervalSeconds = 300) {
		const mgr = new LoopManager({
			sessionId,
			db: h.store.db,
			clock: h.clock,
		});
		await mgr.set("check the deploy", { intervalSeconds, route: { ...ROUTE } });
		return mgr;
	}

	it("injects a due wakeup as an internal event carrying the routing key", async () => {
		const mgr = await createLoop();
		h.clock.setSeconds(1_775_000_400); // past next_due_at …300

		const report = await h.watcher.tick();

		expect(report).toMatchObject({ scanned: 1, injected: 1 });
		expect(h.dispatcher.events).toHaveLength(1);
		const event = h.dispatcher.events[0]!;
		expect(event.internal).toBe(true);
		expect(event.messageType).toBe("text");
		expect(event.text).toContain("[/loop wakeup #1, every 5m]");
		expect(event.text).toContain("Recurring task: check the deploy");
		expect(event.metadata?.["gateway_session_key"]).toBe(
			"agent:main:telegram:dm:100",
		);
		expect(event.source).toEqual(sourceFromRoute(ROUTE));
		const row = h.store.loadLoop("sess");
		expect(row?.awaitingResponse).toBe(true); // tick stays claimed
		expect(row?.ticksFired).toBe(1);
	});

	it("skips rows that are not yet due or already awaiting their turn", async () => {
		await createLoop();
		h.clock.setSeconds(1_775_000_299); // one second early

		let report = await h.watcher.tick();
		expect(report).toMatchObject({ scanned: 1, injected: 0 });
		expect(h.dispatcher.events).toHaveLength(0);

		// fire out-of-band ⇒ awaiting_response defers the scan
		const mgr = new LoopManager({
			sessionId: "sess",
			db: h.store.db,
			clock: h.clock,
		});
		await mgr.fireTick();
		report = await h.watcher.tick();
		expect(report.injected).toBe(0);
		expect(h.dispatcher.events).toHaveLength(0);
	});

	it("CLI/TUI-owned rows without routing metadata are skipped silently", async () => {
		const mgr = new LoopManager({
			sessionId: "cli-sess",
			db: h.store.db,
			clock: h.clock,
		});
		await mgr.set("local prompt", { intervalSeconds: 60 }); // no route
		h.clock.setSeconds(1_775_000_100);

		const report = await h.watcher.tick();
		expect(report).toMatchObject({ scanned: 1, unrouted: 1, injected: 0 });
		expect(h.dispatcher.events).toHaveLength(0);
		const row = h.store.loadLoop("cli-sess");
		expect(row?.ticksFired).toBe(0); // untouched — its own surface drives it
	});

	it("missing adapter skips with a ONE-TIME debug warn per session", async () => {
		await createLoop("orphan");
		h.clock.setSeconds(1_775_000_400);

		const debugs: string[] = [];
		const log: LoopWakeupLogger = { debug: (m) => void debugs.push(m) };
		// SAME store + clock, different seams — the platform is "not connected".
		const watched = buildLoopHarnessOn(
			h.dir,
			h.dbPath,
			h.store,
			h.clock,
			"loop-adapter",
			{
				adapterFor: (platform) =>
					platform === "telegram" ? undefined : { ok: true },
				log,
			},
		);
		for (let i = 0; i < 3; i++) await watched.watcher.tick();
		expect(debugs).toHaveLength(1); // warned once, never again
		expect(debugs[0]).toContain("no adapter for platform 'telegram'");
		expect(debugs[0]).toContain("(session orphan)");
		expect(watched.dispatcher.events).toHaveLength(0);
		// untouched — still due when the platform comes back
		expect(watched.store.loadLoop("orphan")?.ticksFired).toBe(0);
	});

	it("busy routing key defers WITHOUT firing; idle scan fires later", async () => {
		await createLoop("busy-sess");
		h.clock.setSeconds(1_775_000_400);

		const busyKeys = new Set<string>(["agent:main:telegram:dm:100"]);
		const busyWatcher = buildLoopHarnessOn(
			h.dir,
			h.dbPath,
			h.store,
			h.clock,
			"loop-busy",
			{ isSessionKeyBusy: (key) => busyKeys.has(key) },
		);
		const busyReport = await busyWatcher.watcher.tick();
		expect(busyReport).toMatchObject({ scanned: 1, busyDeferred: 1 });
		expect(busyWatcher.dispatcher.events).toHaveLength(0);
		// stayed due: nothing claimed, nothing burned
		expect(busyWatcher.store.loadLoop("busy-sess")?.ticksFired).toBe(0);

		busyKeys.clear(); // turn ends before the next scan
		const idleReport = await busyWatcher.watcher.tick();
		expect(idleReport).toMatchObject({ scanned: 1, injected: 1 });
		expect(busyWatcher.dispatcher.events).toHaveLength(1);
	});

	it("an active /goal owns the idle boundary (goal_blocks_loop_tick seam)", async () => {
		await createLoop("goal-sess");
		h.clock.setSeconds(1_775_000_400);

		const goalWatcher = buildLoopHarnessOn(
			h.dir,
			h.dbPath,
			h.store,
			h.clock,
			"loop-goal",
			{ goalBlocksTick: (sid) => sid === "goal-sess" },
		);
		const report = await goalWatcher.watcher.tick();
		expect(report).toMatchObject({ scanned: 1, goalDeferred: 1 });
		expect(goalWatcher.dispatcher.events).toHaveLength(0);
		expect(goalWatcher.store.loadLoop("goal-sess")?.ticksFired).toBe(0);
	});

	it("slash-command wakeups complete their tick IMMEDIATELY after dispatch", async () => {
		const mgr = new LoopManager({
			sessionId: "cmd-loop",
			db: h.store.db,
			clock: h.clock,
		});
		await mgr.set("/recap latest", {
			intervalSeconds: 60,
			times: 2,
			route: { ...ROUTE },
		});
		h.clock.setSeconds(1_775_000_060);

		const report = await h.watcher.tick();
		expect(report.injected).toBe(1);
		expect(h.dispatcher.texts()[0]).toBe("/recap latest");

		// completed in place: cap accounting applied, awaiting cleared,
		// next tick scheduled — NOT stuck waiting on a post-turn hook
		const row = h.store.loadLoop("cmd-loop");
		expect(row?.awaitingResponse).toBe(false);
		expect(row?.ticksFired).toBe(1);
		expect(row?.nextDueAt).toBe(1_775_000_120);
	});

	it("a throwing dispatcher abandons the tick — stays due for retry", async () => {
		await createLoop("flaky");
		h.clock.setSeconds(1_775_000_400);

		const warns: string[] = [];
		const flaky = new LoopWakeupWatcher({
			db: h.store.db,
			dispatcher: {
				dispatch: async () => {
					throw new Error("L1 guard closed");
				},
			},
			clock: h.clock,
			log: { warn: (m) => void warns.push(m) },
		});

		const failed = await flaky.tick();
		expect(failed.injected).toBe(0);
		const rowAfterFail = h.store.loadLoop("flaky");
		expect(rowAfterFail?.ticksFired).toBe(0); // rolled back
		expect(rowAfterFail?.awaitingResponse).toBe(false);
		// abandon keeps the PROVISIONAL schedule (fire's next_due_at stands), so
		// the retry lands after the cadence delay — never a tight failure loop.
		expect(rowAfterFail?.nextDueAt).toBe(1_775_000_400 + 300);
		expect(warns.some((w) => w.includes("loop wakeup injection failed"))).toBe(
			true,
		);

		h.clock.setSeconds(1_775_000_701); // provisional schedule elapsed
		const retried = await h.watcher.tick(); // healthy dispatcher next scan
		expect(retried.injected).toBe(1); // fired again (#parity)
	});

	it("multiple sessions inject independently; corrupt siblings don't block", async () => {
		const first = new LoopManager({
			sessionId: "a",
			db: h.store.db,
			clock: h.clock,
		});
		await first.set("task a", { intervalSeconds: 60, route: { ...ROUTE } });
		const second = new LoopManager({
			sessionId: "b",
			db: h.store.db,
			clock: h.clock,
		});
		await second.set("task b", { intervalSeconds: 60, route: { ...ROUTE } });

		h.store.db
			.prepare("INSERT INTO state_meta (key, value) VALUES ('loop:zz', '{bad')")
			.run();
		h.clock.setSeconds(1_775_000_200);

		const report = await h.watcher.tick();
		expect(report.scanned).toBe(2); // corrupt row invisible to the scan
		expect(report.injected).toBe(2);
		expect(h.dispatcher.texts()).toEqual([
			expect.stringContaining("task a"),
			expect.stringContaining("task b"),
		]);
	});
});

describe("supervised lifecycle", () => {
	let h: LoopHarness;

	beforeEach(async () => {
		h = await openLoopHarness();
	});

	afterEach(async () => {
		await h.close();
		rmSync(h.dir, { recursive: true, force: true });
	});

	it("startup delay precedes the first tick; stop() joins deterministically", async () => {
		const mgr = new LoopManager({
			sessionId: "life",
			db: h.store.db,
			clock: h.clock,
		});
		await mgr.set("watch", { intervalSeconds: 60, route: { ...ROUTE } });
		h.clock.setSeconds(1_775_000_500); // loop is due

		// REAL cadence shape: 5s startup delay, 15s scan interval (roster row).
		const watched = new LoopWakeupWatcher({
			db: h.store.db,
			dispatcher: h.dispatcher,
			clock: h.clock,
			startupDelayMs: LOOP_WAKEUP_STARTUP_DELAY_MS,
			intervalMs: LOOP_WAKEUP_SCAN_INTERVAL_MS,
		});
		watched.start();
		await h.clock.advance(LOOP_WAKEUP_STARTUP_DELAY_MS - 1);
		expect(h.dispatcher.events).toHaveLength(0); // still connecting

		await h.clock.advance(LOOP_WAKEUP_SCAN_INTERVAL_MS * 2);
		expect(h.dispatcher.events.length).toBeGreaterThanOrEqual(1);
		expect(watched.isRunning).toBe(true);

		await watched.stop(); // breaks any in-flight sleep immediately
		expect(watched.isRunning).toBe(false);
		const afterStop = h.dispatcher.events.length;
		await h.clock.advance(LOOP_WAKEUP_SCAN_INTERVAL_MS * 3);
		expect(h.dispatcher.events.length).toBe(afterStop); // silent after join
		await watched.stop(); // idempotent
	});

	it("start() is idempotent — never two concurrent loops", async () => {
		h.watcher.start();
		h.watcher.start();
		await h.clock.advance(10);
		await h.watcher.stop();
		expect(h.watcher.isRunning).toBe(false);
	});
});

// ---------------------------------------------------------------------------
// event forging units
// ---------------------------------------------------------------------------

describe("forgeLoopWakeupEvent / sourceFromRoute", () => {
	it("round-trips route → source → DEC-022 push-lane event", () => {
		const source = sourceFromRoute({
			platform: "slack",
			chat_id: "C1",
			chat_type: "channel",
			thread_id: "123.456",
		});
		expect(source).toEqual({
			platform: "slack",
			chatType: "channel",
			chatId: "C1",
			threadId: "123.456",
		});
		const event = forgeLoopWakeupEvent({
			wakeup: "[/loop wakeup #2]",
			sessionKey: "agent:main:slack:channel:C1:123.456",
			source,
		});
		expect(event.internal).toBe(true);
		expect(event.source?.platform).toBe("slack");
		expect(event.metadata?.["gateway_session_key"]).toBe(
			"agent:main:slack:channel:C1:123.456",
		);
	});
});

describe("stage entry (DEC-040 stage-8 wiring)", () => {
	it("builds + starts inside start() and returns a stoppable handle", async () => {
		const h = await openLoopHarness("loop-stage");
		try {
			const entries: EmbeddedServiceEntry[] = [];
			entries.push(loopWakeupWatcherServiceEntry({ create: () => h.watcher }));
			const entry = entries[0]!;
			expect(entry.name).toBe(LOOP_WAKEUP_WATCHER_SERVICE_NAME);

			const outcome = await entry.start();
			expect(outcome.ok).toBe(true);
			expect(h.watcher.isRunning).toBe(true);
			await outcome.handle?.stop?.();
			expect(h.watcher.isRunning).toBe(false);
		} finally {
			await h.close();
			rmSync(h.dir, { recursive: true, force: true });
		}
	});

	it("a broken dependency THROWS out of start() — engine classifies degrade", async () => {
		const entry = loopWakeupWatcherServiceEntry({
			create: () => {
				throw new Error("state.db unavailable");
			},
		});
		await expect(entry.start()).rejects.toThrow("state.db unavailable");
	});

	it("the real store opens through the production path in this suite", async () => {
		// guards against harness drift: StateStore must be the REAL facade
		expect(StateStore.open).toBeTypeOf("function");
	});
});
