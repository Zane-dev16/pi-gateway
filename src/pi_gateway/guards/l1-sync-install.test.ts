// 03 §11 row "L1 sync-install": two events delivered before either task runs
// ⇒ exactly ONE turn; the second merges/pends. Plus the create_task-stub
// interleave (non-Task sentinel ⇒ rollback, false return) and burst races.
//
// Concurrency is measured INSIDE the handler (in-flight counter), so the
// exactly-one-turn property is asserted against reality, not scheduling luck.

import { describe, expect, it } from "vitest";
import { makeFixture } from "./testing/manual-spawner.js";

const KEY = "agent:main:telegram:dm:100";

describe("L1 adapter-slot guard — sync-install before task spawn", () => {
	it("interleaved events before either frame starts collapse to exactly one turn; second pends", async () => {
		const f = makeFixture();
		// Deliver BOTH events synchronously before any frame is allowed to run.
		await f.guard.handleMessage(f.text("hello"), KEY);
		await f.guard.handleMessage(f.text("world"), KEY);

		// Guard installed synchronously after the FIRST delivery…
		expect(f.guard.isActive(KEY)).toBe(true);
		// …and exactly ONE frame was queued — no duplicate spawn at ingress.
		expect(f.scheduler.queue.length).toBe(1);
		expect(f.turns).toEqual([]); // nothing ran yet

		// Second event took the busy path into the single pending slot.
		expect(f.guard.pendingOf(KEY)?.text).toBe("world");

		// Running the chain: the drain boundary hands the follow-up to a FRESH
		// second frame (§4) — but never concurrently with the first.
		const ran = await f.scheduler.runToEnd();
		expect(ran).toBe(2); // initial + fresh drain frame, NOT recursion
		expect(f.turns).toEqual(["hello", "world"]);
	});

	it("burst of N synchronous deliveries yields one live turn; the chain drains the composite once", async () => {
		const f = makeFixture();
		for (let i = 0; i < 25; i++) {
			await f.guard.handleMessage(f.text(`m${i}`), KEY);
		}
		// One initial turn; everything else merged into the single slot.
		expect(f.scheduler.queue.length).toBe(1);
		const pending = f.guard.pendingOf(KEY);
		expect(pending?.text).toBe(
			Array.from({ length: 24 }, (_, i) => `m${i + 1}`).join("\n"),
		);

		await f.scheduler.runToEnd();
		// Exactly two frames total (head + one drain of the composite), each
		// fragment delivered exactly once across them, ZERO overlap ever.
		expect(f.turns).toEqual([
			"m0",
			Array.from({ length: 24 }, (_, i) => `m${i + 1}`).join("\n"),
		]);
		expect(f.maxHandlerConcurrency).toBe(1);
	});

	it("create_task stubbed with a non-Task sentinel ⇒ rollback, False return, no half-installed lock", async () => {
		let spawnerCalls = 0;
		let broken = true;
		const f = makeFixture({
			spawner: (run) => {
				spawnerCalls++;
				if (broken) {
					return { notATask: true } as unknown as never; // non-Task stub
				}
				return f.scheduler.spawner(run);
			},
		});

		const started = f.guard.startSessionProcessing(f.text("x"), KEY);
		expect(started).toBe(false); // False return per base.py contract
		expect(f.guard.isActive(KEY)).toBe(false); // guard rolled back
		expect(f.guard.ownerOf(KEY)).toBeUndefined(); // no owner recorded
		expect(spawnerCalls).toBeGreaterThan(0);

		// The session is NOT bricked: once the stub clears, ingress works.
		broken = false;
		await f.guard.handleMessage(f.text("after"), KEY);
		await f.scheduler.runToEnd();
		expect(f.turns).toEqual(["after"]);
	});

	it("throwing spawner rolls back identically to the sentinel stub", () => {
		let broken = true;
		const f = makeFixture({
			spawner: (run) => {
				if (broken) throw new TypeError("unhashable sentinel");
				return f.scheduler.spawner(run);
			},
		});
		expect(f.guard.startSessionProcessing(f.text("x"), KEY)).toBe(false);
		expect(f.guard.isActive(KEY)).toBe(false);
		expect(f.guard.ownerOf(KEY)).toBeUndefined();

		broken = false;
		f.guard.startSessionProcessing(f.text("y"), KEY);
		expect(f.guard.isActive(KEY)).toBe(true);
	});

	it("guard AND owner task are installed atomically before the first await returns", async () => {
		const f = makeFixture();
		await f.guard.handleMessage(f.text("first"), KEY);
		// After the synchronous section of handleMessage both maps are populated,
		// even though NO frame has executed yet.
		expect(f.guard.isActive(KEY)).toBe(true);
		expect(f.guard.ownerOf(KEY)?.isDone()).toBe(false);
		expect(f.scheduler.queue[0]?.started).toBe(false); // queued, not yet running
		await f.scheduler.runToEnd();
		expect(f.replies).toEqual(["reply:first"]);
	});

	it("two different session keys never share a guard or a slot", async () => {
		const f = makeFixture();
		await f.guard.handleMessage(f.text("b"), `${KEY}:B`);
		await f.guard.handleMessage(f.text("a"), `${KEY}:A`);
		expect(f.scheduler.queue.length).toBe(2); // independent turns
		await f.scheduler.runToEnd();
		expect([...f.turns].sort((a, b) => a.localeCompare(b))).toEqual(["a", "b"]);
	});
});
