// 03 §3.3 self-heal + §11 row: kill owner task ⇒ progress + warning; NO owner
// task ⇒ NOT healed (guards installed outside handleMessage stay alone); a
// LIVE owner is never stale. This is the production split-brain tail of
// #11016-class incidents.

import { describe, expect, it } from "vitest";
import { makeFixture } from "./testing/manual-spawner.js";

const KEY = "agent:main:telegram:dm:100";

describe("stale-lock self-heal (§3.3)", () => {
	it("recorded EXITED owner ⇒ healed: warning logged, guard+slot+task+debounce cleared, message proceeds", async () => {
		const f = makeFixture();

		// Produce a real owner, then let it FINISH; its ManualTask handle reads
		// done()===true afterwards — exactly the recorded-exited-owner shape.
		await f.guard.handleMessage(f.text("first"), KEY);
		const frame = f.scheduler.queue.shift()!;
		frame.start();
		await frame.result;
		expect(frame.isDone()).toBe(true);

		// Split-brain simulation: an owner was recorded, then exited WITHOUT
		// clearing its guard (crash between turn end and cleanup).
		f.guard.forceInstallGuardForTests(KEY);
		f.guard.installOwnerForTests(KEY, frame);
		expect(f.guard.isActive(KEY)).toBe(true);
		expect(f.guard.sessionTaskIsStale(KEY)).toBe(true);

		// Next inbound message heals BEFORE the busy check and proceeds.
		await f.guard.handleMessage(f.text("rescue"), KEY);
		expect(
			f.warnings.some((w) => w.includes("Healing stale session lock")),
		).toBe(true);

		await f.scheduler.quiesce();
		expect(f.turns).toContain("rescue"); // NOT trapped behind the dead guard
	}, 30_000);

	it("heal also drops a pending slot and buffered debounce text", async () => {
		const f = makeFixture({ busyTextMode: "queue" });

		await f.guard.handleMessage(f.text("head"), KEY);
		const frame = f.scheduler.queue.shift()!;
		frame.start();
		await frame.result;
		expect(frame.isDone()).toBe(true);

		f.guard.forceInstallGuardForTests(KEY);
		f.guard.installOwnerForTests(KEY, frame);
		// Slot + debounce state left behind by the crashed turn:
		f.guard.slotView.set(KEY, f.text("stale-slot"));
		f.guard.forceBusyQueueTextForTests(KEY, f.text("stale-debounce"));

		const healed = f.guard.healStaleSessionLock(KEY);
		expect(healed).toBe(true);
		expect(f.guard.pendingOf(KEY)).toBeUndefined();
		expect(f.guard.debounceBufferedText(KEY)).toBeNull();
		expect(f.guard.ownerOf(KEY)).toBeUndefined();
	});

	it("NO recorded owner task ⇒ NOT healed: externally installed guards survive", async () => {
		const f = makeFixture();
		// Guard installed OUTSIDE handleMessage (test/other-path parity):
		f.guard.forceInstallGuardForTests(KEY);
		expect(f.guard.sessionTaskIsStale(KEY)).toBe(false);

		const healed = f.guard.healStaleSessionLock(KEY);
		expect(healed).toBe(false);
		expect(f.guard.isActive(KEY)).toBe(true); // untouched
		expect(
			f.warnings.some((w) => w.includes("Healing stale session lock")),
		).toBe(false);

		// And ingress still treats the session as busy (queues behind it):
		await f.guard.handleMessage(f.text("queued"), KEY);
		expect(f.turns).toEqual([]); // no turn started
	});

	it("LIVE owner task ⇒ not stale, never healed", async () => {
		const f = makeFixture();
		f.holdTurns(true);
		await f.guard.handleMessage(f.text("live"), KEY);
		const liveFrame = f.scheduler.queue.shift()!;
		liveFrame.start(); // running (parked)

		expect(f.guard.sessionTaskIsStale(KEY)).toBe(false);
		expect(f.guard.healStaleSessionLock(KEY)).toBe(false);
		expect(f.guard.isActive(KEY)).toBe(true);

		// Cancelled-but-still-unwinding is ALSO not done yet:
		liveFrame.cancel();
		expect(f.guard.sessionTaskIsStale(KEY)).toBe(false);

		f.holdTurns(false);
		await f.scheduler.quiesce();
	});
});
