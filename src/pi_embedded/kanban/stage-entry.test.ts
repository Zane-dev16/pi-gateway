// Behavior contracts for the kanban dispatcher's DEC-040 stage entry: ALL
// THREE native ServiceStartResult classifications must survive the mapping —
// started (handle), degraded (loud per-service degrade), disabled/skipped
// (loud, NOT a failure). Fake board client + injected clock; no real spawn.

import { describe, expect, it } from "vitest";
import type { GatewayClock } from "./clock.js";
import {
	KANBAN_DISPATCHER_SERVICE_NAME,
	kanbanDispatcherServiceEntry,
} from "./stage-entry.js";
import { KANBAN_DISPATCH_IN_GATEWAY_ENV } from "./service.js";
import type { BoardClient, KanbanCard, SpawnFn } from "./types.js";

const clock: GatewayClock = {
	nowSeconds: () => 1000,
	sleepMs: async () => {},
};

function fakeBoard(board: string): BoardClient {
	return {
		board,
		listReady: async () => [],
		countRunning: async () => 0,
		reclaimStaleClaims: async () => [],
		promoteReady: async () => [],
		claimCard: async () => null,
		recordFailure: async () => ({ blocked: false, failures: 0 }),
		completeCard: async () => true,
		events: async () => [],
	};
}

const noSpawn: SpawnFn = async (_card: KanbanCard) => {};

describe("kanbanDispatcherServiceEntry (DEC-040 stage 8 wiring)", () => {
	it("maps a running dispatcher onto ok:true with a stoppable handle", async () => {
		let opened = "";
		const entry = kanbanDispatcherServiceEntry({
			pinnedBoard: "test-board",
			openBoard: async (board) => {
				opened = board;
				return fakeBoard(board);
			},
			spawn: noSpawn,
			clock,
			logLines: () => {},
		});
		expect(entry.name).toBe(KANBAN_DISPATCHER_SERVICE_NAME);

		const outcome = await entry.start();

		expect(opened).toBe("test-board");
		expect(outcome.ok).toBe(true);
		expect(outcome.degraded).toBeUndefined();
		expect(outcome.handle?.name).toBe(KANBAN_DISPATCHER_SERVICE_NAME);
		await outcome.handle?.stop?.();
	});

	it("maps a refused board onto ok:false + degraded:true with the reason", async () => {
		const outcome = await kanbanDispatcherServiceEntry({
			pinnedBoard: "BAD SLUG!!", // invalid slug ⇒ HARD boundary refusal
			openBoard: async (board) => fakeBoard(board),
			spawn: noSpawn,
			clock,
			logLines: () => {},
		}).start();

		expect(outcome.ok).toBe(false);
		expect(outcome.degraded).toBe(true);
		expect(outcome.reason).toContain("BAD SLUG!!");
		expect(outcome.handle).toBeUndefined();
	});

	it("maps a DISABLED env gate onto ok:false WITHOUT the degraded flag", async () => {
		const outcome = await kanbanDispatcherServiceEntry({
			env: { [KANBAN_DISPATCH_IN_GATEWAY_ENV]: "off" },
			openBoard: async (board) => fakeBoard(board),
			spawn: noSpawn,
			clock,
			logLines: () => {},
		}).start();

		expect(outcome.ok).toBe(false);
		expect(outcome.degraded).toBeUndefined();
		expect(outcome.reason).toContain(KANBAN_DISPATCH_IN_GATEWAY_ENV);
		expect(outcome.handle).toBeUndefined();
	});
});
