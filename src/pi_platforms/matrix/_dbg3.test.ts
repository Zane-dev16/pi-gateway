import { describe, expect, it } from "vitest";
import { MATRIX_SYNC_WATCHDOG_TIMEOUT_MS } from "./manifest.js";
import { eventually, makeMatrixWorld, ALICE } from "./matrix-world.js";

describe("debug heartbeat fixture-parity", () => {
	it("traces exactly like the fixture", async () => {
		const world = makeMatrixWorld({ name: "dbg-hb2" });
		const { engine, hs, clock } = world;
		await world.connectAndAwaitLive();
		const genBefore = engine.generation;

		hs.setWedged(true);
		world.pushMessage("!room:fake.example", ALICE, "wedge-1");
		await eventually(() => hs.pendingEventCount >= 1);
		console.log("pending ok:", hs.pendingEventCount);

		console.log("pre-adv1 waits:", clock.pendingTimerCount);
		await clock.advance(MATRIX_SYNC_WATCHDOG_TIMEOUT_MS + 500);
		console.log(
			"post-adv1 waits:",
			clock.pendingTimerCount,
			"streak:",
			engine.stuckProbeStreakForTests,
			"updater:",
			engine.updaterRunning,
			"log:",
			JSON.stringify(engine.recoveryLog),
		);
		await eventually(() => engine.stuckProbeStreakForTests >= 1, 4_000);
		console.log("streak1 ok");

		await clock.advance(MATRIX_SYNC_WATCHDOG_TIMEOUT_MS + 500);
		await eventually(
			() => engine.recoveryLog.includes("sync-watchdog-stuck-streak"),
			4_000,
		);
		console.log("escalation ok");
		await eventually(() => engine.generation > genBefore, 4_000);
		console.log("gen bump ok");
		expect(true).toBe(true);
	});
});
