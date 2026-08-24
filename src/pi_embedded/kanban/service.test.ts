// §6 embedded-dispatcher service contracts: config resolved ONCE at boot
// (DEC-013), env escape hatch, optional-stage degrade-loudly behavior, the
// HARD board boundary refusal, per-tick failure isolation ("failures in one
// tick don't stop subsequent ticks"), and clean stop.

import { describe, expect, it } from "vitest";

import { KANBAN_BOARD_ENV } from "./board.js";
import {
	KANBAN_DISPATCH_IN_GATEWAY_ENV,
	resolveDispatcherServiceConfig,
	startKanbanDispatcher,
	type RunningKanbanDispatcher,
} from "./service.js";
import { ManualClock } from "./testing/manual-clock.js";
import type { BoardClient, KanbanCard } from "./types.js";

function lines(): { log: (s: string) => void; out: string[] } {
	const out: string[] = [];
	return { log: (s: string) => out.push(s), out };
}

describe("resolveDispatcherServiceConfig — read ONCE at boot (DEC-013)", () => {
	it("defaults: enabled, default board, 60s interval, failure_limit 2", () => {
		const cfg = resolveDispatcherServiceConfig({ env: {} });
		expect(cfg).toMatchObject({
			board: "default",
			boardSource: "default",
			intervalSeconds: 60,
			failureLimit: 2,
			enabled: true,
		});
	});

	it("env escape hatch false-y values disable without touching YAML", () => {
		for (const v of ["0", "false", "no", "off", " OFF "]) {
			const cfg = resolveDispatcherServiceConfig({
				env: { [KANBAN_DISPATCH_IN_GATEWAY_ENV]: v },
			});
			expect(cfg.enabled).toBe(false);
		}
		expect(
			resolveDispatcherServiceConfig({
				env: { [KANBAN_DISPATCH_IN_GATEWAY_ENV]: "yes" },
			}).enabled,
		).toBe(true);
	});

	it("invalid interval/failure-limit values fall back to defaults WITH warnings", () => {
		const cfg = resolveDispatcherServiceConfig({
			config: {
				dispatch_interval_seconds: "banana",
				failure_limit: 0,
			},
		});
		expect(cfg.intervalSeconds).toBe(60);
		expect(cfg.failureLimit).toBe(2);
		expect(cfg.warnings.length).toBeGreaterThanOrEqual(2);
	});

	it("interval floor is 1s (tighter is a footgun)", () => {
		const cfg = resolveDispatcherServiceConfig({
			config: { dispatch_interval_seconds: 0.1 },
		});
		expect(cfg.intervalSeconds).toBe(1);
	});
});

describe("HARD boundary — wrong/invalid pinned board refuses LOUDLY", () => {
	it("invalid pinned slug ⇒ degraded result, loud warning naming restart scope", async () => {
		const l = lines();
		let ticks = 0;
		const { result } = await startKanbanDispatcher(
			{
				openBoard: () => {
					ticks += 1000;
					throw new Error("should never open a board");
				},
				spawn: () => {},
				pinnedBoard: "NOT VALID",
				env: {},
				clock: new ManualClock(),
			},
			l.log,
		);
		expect(result.ok).toBe(false);
		expect(result.degraded).toBe(true); // optional-stage classification
		expect(ticks).toBe(0); // never dispatched anywhere
		const reason = `${result.reason} ${l.out.join(" ")}`;
		expect(reason).toContain("hard board boundary");
		expect(reason).toContain("RESTART");
	});

	it("board client resolving to a DIFFERENT board than pinned ⇒ refusal", async () => {
		const l = lines();
		const impostor: BoardClient = impostorBoard("default"); // not the pinned one
		const { result } = await startKanbanDispatcher(
			{
				openBoard: () => impostor,
				spawn: () => {},
				env: { [KANBAN_BOARD_ENV]: "sre" },
				clock: new ManualClock(),
			},
			l.log,
		);
		expect(result.ok).toBe(false);
		expect(result.degraded).toBe(true);
		expect(result.reason).toContain("hard board boundary");
	});

	it("openBoard failure (missing store) ⇒ degraded loudly, no throw", async () => {
		const l = lines();
		const { result } = await startKanbanDispatcher(
			{
				openBoard: () => {
					throw new Error("kanban.db missing");
				},
				spawn: () => {},
				env: {},
				clock: new ManualClock(),
			},
			l.log,
		);
		expect(result.ok).toBe(false);
		expect(result.degraded).toBe(true);
		expect(result.reason).toContain("kanban.db missing");
	});

	it("disabled via env is a CLEAN skip (not degraded)", async () => {
		const l = lines();
		const { result } = await startKanbanDispatcher(
			{
				openBoard: () => impostorBoard("default"),
				spawn: () => {},
				env: { [KANBAN_DISPATCH_IN_GATEWAY_ENV]: "0" },
				clock: new ManualClock(),
			},
			l.log,
		);
		expect(result.ok).toBe(false);
		expect(result.degraded).toBe(false);
		expect(l.out.join(" ")).toContain(KANBAN_DISPATCH_IN_GATEWAY_ENV);
	});

	it("external singleton holder ⇒ this gateway does NOT dispatch (backstop probe)", async () => {
		const { result } = await startKanbanDispatcher({
			openBoard: () => impostorBoard("default"),
			spawn: () => {},
			env: {},
			clock: new ManualClock(),
			hasSingleton: () => false,
		});
		expect(result.ok).toBe(false);
		expect(result.degraded).toBe(false);
		expect(result.reason).toContain("dispatcher role");
	});
});

describe("running loop — per-tick isolation and stop", () => {
	it("a throwing tick logs loudly and the NEXT tick still runs (parity contract)", async () => {
		const l = lines();
		const clock = new ManualClock(1000);
		let tickCount = 0;
		const failFirstN = 1;
		const board = countingBoard(() => {
			tickCount++;
			if (tickCount <= failFirstN) throw new Error("tick boom");
		});
		const started = await startKanbanDispatcher(
			{
				openBoard: () => board,
				spawn: () => {},
				env: {},
				clock,
				config: { dispatch_interval_seconds: 1 },
			},
			l.log,
		);
		expect(started.result.ok).toBe(true);
		const running = started.running as RunningKanbanDispatcher;
		try {
			// Wait until the SECOND tick happened despite the first throwing.
			const deadline = Date.now() + 5000;
			while (tickCount < 2 && Date.now() < deadline) {
				await clock.sleepMs(10);
			}
			expect(tickCount).toBeGreaterThanOrEqual(2);
			expect(l.out.join("\n")).toContain("tick FAILED loudly");
		} finally {
			await running.stop();
		}
		expect(tickCount).toBe(2); // loop stopped cleanly after stop()
	});
});

// --- minimal fake boards ---------------------------------------------------

function impostorBoard(board: string): BoardClient {
	return {
		board,
		async listReady() {
			return [];
		},
		async countRunning() {
			return 0;
		},
		async reclaimStaleClaims() {
			return [];
		},
		async promoteReady() {
			return [];
		},
		async claimCard(): Promise<KanbanCard | null> {
			return null;
		},
		async recordFailure() {
			return { blocked: false, failures: 0 };
		},
		async completeCard() {
			return true;
		},
		async events() {
			return [];
		},
	};
}

function countingBoard(onTick: () => void): BoardClient {
	const inner = impostorBoard("default");
	return {
		...inner,
		async reclaimStaleClaims(now: number) {
			onTick();
			return inner.reclaimStaleClaims(now);
		},
	};
}
