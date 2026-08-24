// Behavior contracts for DelegationWatcher (gateway/run.py
// _async_delegation_watcher port):
//
//   ★ IDLE-GATE RACE (06 §10 row; completes Phase 4's deferred matrix
//     entry): a delegation completing while the session is BUSY waits — the
//     row stays unclaimed and attempt-free — then re-enters as ONE new
//     forged turn exactly once after idle end.
//   - route-scoped coalescing at tick granularity;
//   - ALL loop timing through the injected clock: 3s startup delay, 2s
//     poll cadence, deterministic stop();
//   - failure containment: a broken store never crashes the loop.

import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { StateStore } from "../../pi_state/index.js";
import {
	DELEGATION_POLL_INTERVAL_MS,
	DELEGATION_WATCHER_STARTUP_DELAY_MS,
} from "./index.js";
import {
	buildHarnessOn,
	openWatcherHarness,
	pendingRow,
	seedCompletion,
	seedRouting,
	seedSession,
	type WatcherHarness,
} from "./testing/harness.js";

const KEY = "agent:main:telegram:dm:100";

let h: WatcherHarness;

beforeEach(async () => {
	h = await openWatcherHarness();
	await seedSession(h, "parent", { sessionKey: KEY });
	await seedRouting(h, KEY, "parent");
});

afterEach(async () => {
	await h.close();
});

describe("★ IDLE-GATE RACE — completion during an active turn", () => {
	it("waits untouched while busy, then fires as a NEW forged turn exactly once after idle end", async () => {
		await seedCompletion(h, {
			delegationId: "dlg-race",
			originSession: KEY,
			parentSessionId: "parent",
			goal: "background scan",
		});

		// The parent session is mid-turn when the completion lands.
		h.liveness.busy.add("parent");

		const busyTick = await h.watcher.tick();
		expect(busyTick).toMatchObject({ pending: 1, busy: 1, delivered: 0 });
		let row = pendingRow(h, "dlg-race");
		expect(row?.delivery_state).toBe("pending");
		expect(row?.delivery_attempts).toBe(0); // zero claim churn while waiting

		// Still busy several polls later — nothing burns.
		await h.watcher.tick();
		await h.watcher.tick();
		row = pendingRow(h, "dlg-race");
		expect(row?.delivery_attempts).toBe(0);

		// The turn ends (idle end); the next poll delivers exactly once.
		h.liveness.busy.delete("parent");
		const idleTick = await h.watcher.tick();
		expect(idleTick).toMatchObject({ delivered: 1, busy: 0 });
		row = pendingRow(h, "dlg-race");
		expect(row?.delivery_state).toBe("delivered");
		expect(row?.delivery_attempts).toBe(1); // ONE claim for ONE delivery

		// New FORGED TURN through the normal pipeline port — internal event.
		expect(h.dispatcher.events).toHaveLength(1);
		expect(h.dispatcher.events[0]?.internal).toBe(true);
		expect(h.dispatcher.events[0]?.metadata?.["gateway_session_key"]).toBe(KEY);

		// Idempotent: further ticks never re-deliver.
		await h.watcher.tick();
		expect(h.dispatcher.events).toHaveLength(1);
	});

	it("busy target retargeted by idle-end resolution gates on the RESOLVED session", async () => {
		// Parent idle-ended; chat's current session is B — and B is mid-turn.
		await seedSession(h, "parent", {
			sessionKey: KEY,
			endedAt: h.clock.nowSeconds() - 30,
			endReason: "idle",
		});
		await seedSession(h, "session-b", { sessionKey: KEY });
		await seedRouting(h, KEY, "session-b");
		await seedCompletion(h, {
			delegationId: "dlg-retarget-busy",
			originSession: KEY,
			parentSessionId: "parent",
		});
		h.liveness.busy.add("session-b"); // the TARGET is busy, not the parent

		const r = await h.watcher.tick();
		expect(r.busy).toBe(1);
		expect(pendingRow(h, "dlg-retarget-busy")?.delivery_attempts).toBe(0);

		h.liveness.busy.delete("session-b");
		const r2 = await h.watcher.tick();
		expect(r2.delivered).toBe(1);
		expect(r2.retried).toBe(0);
	});
});

describe("tick coalescing scope", () => {
	it("same-parent fan-out coalesces into one turn; different sessions never merge", async () => {
		await seedSession(h, "other-parent", {
			sessionKey: "agent:main:telegram:dm:200",
		});
		await seedRouting(h, "agent:main:telegram:dm:200", "other-parent");
		await seedCompletion(h, {
			delegationId: "fan-1",
			originSession: KEY,
			parentSessionId: "parent",
			goal: "f1",
		});
		await seedCompletion(h, {
			delegationId: "fan-2",
			originSession: KEY,
			parentSessionId: "parent",
			goal: "f2",
		});
		await seedCompletion(h, {
			delegationId: "other-1",
			originSession: "agent:main:telegram:dm:200",
			parentSessionId: "other-parent",
			goal: "o1",
		});

		const r = await h.watcher.tick();
		expect(r.pending).toBe(3);
		expect(r.delivered).toBe(2); // two GROUPS ⇒ two turns
		expect(h.dispatcher.events).toHaveLength(2);

		const texts = h.dispatcher.texts();
		const consolidated = texts.find((t) => t.includes("[IMPORTANT:"));
		expect(consolidated).toContain("did fan-1");
		expect(consolidated).toContain("did fan-2");
		const other = texts.find((t) => t.includes("o1"));
		expect(other).toContain("other-1");
		expect(other).not.toContain("[IMPORTANT:");
	});
});

describe("loop timing via injected clock (no wall sleeps)", () => {
	it("startup delay precedes the FIRST tick; cadence follows the interval", async () => {
		const tickSpy = vi.spyOn(h.watcher, "tick");
		h.watcher.start();

		await h.clock.advance(DELEGATION_WATCHER_STARTUP_DELAY_MS - 1);
		expect(tickSpy).not.toHaveBeenCalled(); // platforms still connecting

		await h.clock.advance(1);
		expect(tickSpy).toHaveBeenCalledTimes(1); // first tick after 3s delay

		await h.clock.advance(DELEGATION_POLL_INTERVAL_MS - 1);
		expect(tickSpy).toHaveBeenCalledTimes(1); // not due yet
		await h.clock.advance(1);
		expect(tickSpy).toHaveBeenCalledTimes(2); // 2s cadence boundary
		await h.clock.advance(DELEGATION_POLL_INTERVAL_MS * 3);
		expect(tickSpy).toHaveBeenCalledTimes(5);

		await h.watcher.stop();
		await h.clock.advance(DELEGATION_POLL_INTERVAL_MS * 10);
		expect(tickSpy).toHaveBeenCalledTimes(5); // stopped loops never tick again
		tickSpy.mockRestore();
	});

	it("stop() breaks an in-flight sleep immediately (no hung joins)", async () => {
		h.watcher.start(); // parks on the 3s startup sleep
		const joined = h.watcher.stop();
		await expect(
			Promise.race([
				joined.then(() => "stopped"),
				new Promise<string>((r) => setTimeout(() => r("hung"), 1000)),
			]),
		).resolves.toBe("stopped");
		expect(h.watcher.isRunning).toBe(false);
	});
});

describe("failure containment", () => {
	it("a broken store never crashes the tick (best-effort parity)", async () => {
		await seedCompletion(h, {
			delegationId: "doomed-read",
			originSession: KEY,
			parentSessionId: "parent",
		});
		await h.store.db.close(); // every read now throws SqliteError

		const r = await h.watcher.tick(); // must NOT throw
		expect(r.pending).toBe(0);
		expect(r.delivered).toBe(0);
	});

	it("boot() on a closed store degrades LOUDLY but never blocks startup", async () => {
		await h.store.db.close();
		const warnings: string[] = [];
		const w = new (Object.getPrototypeOf(h.watcher).constructor)({
			db: h.store.db,
			liveness: h.liveness,
			dispatcher: h.dispatcher,
			clock: h.clock,
			log: { warn: (m: string) => warnings.push(m) },
		});
		const boot = await w.boot();
		expect(boot.restored).toBe(0);
		expect(warnings[0]).toContain("boot restore failed");
	});
});

describe("restart over the same store file (fresh open)", () => {
	it("a fresh watcher instance sees committed rows and delivers them once", async () => {
		await seedCompletion(h, {
			delegationId: "survives-open",
			originSession: KEY,
			parentSessionId: "parent",
			goal: "durable work",
		});

		const dbPath = h.dbPath;
		const firstReport = await h.watcher.tick();
		expect(firstReport.delivered).toBe(1);
		await h.close();

		// "Reboot": brand-new open of the same file; a fresh watcher must find
		// NOTHING to deliver — the first delivery was durably acked.
		const reopened = await StateStore.open(dbPath);
		try {
			const clock = h.clock; // same virtual timeline continues
			const second = await buildHarnessOn(h.dir, dbPath, reopened, clock);
			await seedRouting(second, KEY, "parent"); // routing rows persisted too
			const r = await second.watcher.tick();
			expect(r.pending).toBe(0); // delivered rows never replay…
			expect(second.dispatcher.events).toHaveLength(0); // …EXACTLY once total
		} finally {
			await reopened.close(false);
		}
	});
});
