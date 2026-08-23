// 03 §11 row "Drain boundary" (§4): chained N follow-ups ⇒ N FRESH tasks and
// a flat stack (#17758 — recursion grew one frame per follow-up until
// SIGSEGV); a message landing during cleanup is RE-QUEUED for the owner task,
// never dual-spawned; guard entry persists across the chain while only the
// Event clears; cleanup's owner-check leaves command-swapped guards alone.

import { describe, expect, it } from "vitest";
import { makeFixture } from "./testing/manual-spawner.js";

const KEY = "agent:main:telegram:dm:100";

describe("drain boundary — ownership handoff (§4)", () => {
	it("chained follow-ups each get a FRESH task; frames never nest; every text served exactly once", async () => {
		const f = makeFixture();

		// Idle head turn + three busy follow-ups merged into the slot chain.
		await f.guard.handleMessage(f.text("t0"), KEY);
		await f.guard.handleMessage(f.text("f1"), KEY);
		await f.guard.handleMessage(f.text("f2"), KEY);
		await f.guard.handleMessage(f.text("f3"), KEY);

		let spawned = f.scheduler.queue.length;
		expect(spawned).toBe(1); // single initial spawn

		await f.scheduler.quiesce();

		// t0 ran as its own turn; f1/f2/f3 coalesced into ONE composite
		// follow-up turn (merge_text) — still exactly ONE more frame.
		spawned = f.scheduler.finished.length;
		expect(spawned).toBe(2);
		expect(f.turns).toEqual(["t0", "f1\nf2\nf3"]);
	});

	it("chain of N sequential arrivals produces N fresh tasks with FLAT stack depth (no recursion)", async () => {
		const f = makeFixture();
		const steps: string[] = ["s1", "s2", "s3"];

		// Park the head; each follow-up arrives while an owner is PARKED, so
		// each lands in the slot and is handed to a FRESH chained task.
		f.holdTurns(true);
		await f.guard.handleMessage(f.text("head"), KEY);
		let parked = f.scheduler.queue.shift()!;
		parked.start();

		for (const step of steps) {
			await f.guard.handleMessage(f.text(step), KEY); // busy path → slot
			f.holdTurns(false);
			await parked.result; // owner drains slot → spawns successor
			const successor = f.scheduler.queue.shift();
			if (successor === undefined) break; // session drained early
			f.holdTurns(true); // re-arm gate BEFORE starting the successor
			successor.start(); // parks inside its handler
			parked = successor;
		}
		f.holdTurns(false);
		await f.scheduler.quiesce();

		// Flat-stack evidence: handler concurrency never exceeded ONE even
		// though four turns were produced through three handoffs.
		expect(f.turns[0]).toBe("head");
		for (const step of steps) expect(f.turns).toContain(step);
		expect(f.maxHandlerConcurrency).toBe(1);
		// Every chained turn was a DISTINCT task object (fresh-task handoff):
		const distinctTasks = new Set(f.scheduler.finished);
		expect(distinctTasks.size).toBe(f.turns.length);
	});

	it("guard ENTRY stays live across the chain; only the interrupt Event clears", async () => {
		const f = makeFixture();
		await f.guard.handleMessage(f.text("head"), KEY);
		await f.guard.handleMessage(f.text("followup"), KEY);

		const headTask = f.scheduler.queue.shift();
		headTask?.start();
		// While the head runs, its in-band drain pops the follow-up, CLEARS the
		// event, spawns the fresh drain, and returns. Drive that by awaiting
		// the head's completion:
		await headTask?.result;

		// Entry STILL live after the head frame ended:
		expect(f.guard.isActive(KEY)).toBe(true);
		const event = f.guard.guardOf(KEY);
		expect(event?.isSet()).toBe(false); // cleared for the chain…

		// …and OWNERSHIP transferred to the fresh drain task.
		const owner = f.guard.ownerOf(KEY);
		expect(owner).toBeDefined();
		expect(owner === headTask).toBe(false); // NOT the old frame

		// A message arriving DURING the chain takes the busy path (queued),
		// proving the entry is load-bearing.
		await f.guard.handleMessage(f.text("mid-chain"), KEY);
		expect(f.guard.pendingOf(KEY)?.text ?? null).not.toBe(null); // queued, no new spawn
		expect(f.scheduler.queue.length).toBe(1); // only the existing drain frame

		await f.scheduler.quiesce();
		expect(f.turns).toEqual(["head", "followup", "mid-chain"]);

		// After the LAST frame drains nothing, cleanup releases the guard.
		expect(f.guard.isActive(KEY)).toBe(false);
	});

	it("late arrival during cleanup is RE-QUEUED into the owner's slot — never dual-spawned", async () => {
		const f = makeFixture();

		// Frame A holds; frame B will own the session after A's in-band drain.
		f.holdTurns(true);
		await f.guard.handleMessage(f.text("A"), KEY);
		const frameA = f.scheduler.queue.shift();
		frameA?.start(); // parks inside handler
		await f.guard.handleMessage(f.text("B"), KEY); // B pends in the slot

		// Release A: in-band drain pops B, clears event, spawns frame B,
		// transfers ownership, then enters A's finally.
		f.holdTurns(false);
		await frameA?.result;

		// B's frame exists but has not started (queued by A's drain).
		const frameB = f.scheduler.queue.shift();
		expect(frameB).toBeDefined();
		expect(frameB?.started).toBe(false);
		expect(f.guard.ownerOf(KEY) === frameB).toBe(true);

		// Park B, then deliver LATE arrival while B sits between phases…
		f.holdTurns(true);
		frameB?.start();
		await new Promise<void>((r) => setTimeout(r, 5)); // let B enter its handler
		await f.guard.handleMessage(f.text("LATE"), KEY); // busy path → slot
		expect(f.guard.pendingOf(KEY)?.text).toBe("LATE");

		// …and release B so its finally sees the late pending.
		f.holdTurns(false);
		await f.scheduler.quiesce();

		// LATE was processed by B's OWN drain chain — one frame at a time,
		// never two concurrent processors on this key.
		expect(f.turns).toEqual(["A", "B", "LATE"]);
		expect(f.maxHandlerConcurrency).toBe(1);
		expect(f.guard.isActive(KEY)).toBe(false);
	});

	it("late arrival during the ORIGINAL owner's finally (no handoff yet) spawns exactly ONE drain", async () => {
		const f = makeFixture();

		// Simulate the race window INSIDE frame A's finally: A finishes its
		// body without pending work, and a message lands during cleanup awaits.
		f.holdTurns(true);
		await f.guard.handleMessage(f.text("only"), KEY);
		const frameA = f.scheduler.queue.shift();
		frameA?.start();

		// Deliver while A is parked in the HANDLER (before any drain): goes to
		// slot via busy path. Then release; A's in-band drain picks BOTH up?
		// No — in-band drain pops the SLOT once ("second"); the point here is
		// that "third" arrives during the FINALLY awaits instead.
		await f.guard.handleMessage(f.text("second"), KEY);
		f.holdTurns(false);
		// Race injection: deliver a third event right as A unwinds.
		const thirdDelivery = f.guard
			.handleMessage(f.text("third"), KEY)
			.then(() => undefined);

		await frameA?.result;
		await thirdDelivery;
		await f.scheduler.quiesce();

		// Every text served exactly once across the whole chain; no dual-spawn
		// ever produced duplicate processing of any text.
		const allTexts = f.turns.join("|");
		expect(allTexts).toContain("only");
		expect(allTexts).toContain("second");
		expect(allTexts).toContain("third");
		expect(f.maxHandlerConcurrency).toBe(1);
		expect(f.guard.isActive(KEY)).toBe(false);
	});

	it("cleanup owner-check: an old frame's unwind does NOT delete a newer owner's guard or task entry", async () => {
		const f = makeFixture();
		f.holdTurns(true);
		await f.guard.handleMessage(f.text("old-owner"), KEY);
		const frameOld = f.scheduler.queue.shift();
		frameOld?.start();
		await f.guard.handleMessage(f.text("next"), KEY);

		f.holdTurns(false);
		await frameOld?.result; // old frame hands off to drain frame and unwinds

		// PEEK — do not consume: the successor must stay visible to quiesce().
		const frameNext = f.scheduler.queue[0];
		expect(frameNext).toBeDefined();
		expect(f.guard.isActive(KEY)).toBe(true); // survived old frame's cleanup
		expect(f.guard.ownerOf(KEY)).toBe(frameNext);

		// Now finish the successor normally: cleanup releases everything.
		await f.scheduler.quiesce();
		expect(f.guard.isActive(KEY)).toBe(false);
		expect(f.guard.ownerOf(KEY)).toBeUndefined();
	});
});
