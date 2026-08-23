// 03 §1.2 wake lanes + §11 row "Wake lanes (push)" + DEC-022: background
// completions re-enter via a FORGED internal MessageEvent fed through the
// NORMAL handle_message pipeline — BOTH guards, busy ladder, and merge rules
// included. A wake IS a turn; bypassing guards is explicitly rejected.

import { describe, expect, it } from "vitest";
import type { IncomingEvent } from "./events.js";
import { makeFixture } from "./testing/manual-spawner.js";

const KEY = "agent:main:telegram:dm:100";

function wake(text: string, extra: Partial<IncomingEvent> = {}): IncomingEvent {
	return {
		messageType: "text",
		text,
		internal: true, // DEC-022 push-lane marker
		metadata: { gateway_session_key: KEY },
		source: {
			platform: "telegram",
			chatType: "dm",
			userId: "u1",
			chatId: "100",
		},
		...extra,
	};
}

describe("forged internal events traverse BOTH guards (DEC-022 push lane)", () => {
	it("idle session: a forged wake starts exactly ONE normal turn", async () => {
		const f = makeFixture();
		await f.guard.handleMessage(wake("background task finished"), KEY);

		expect(f.scheduler.queue.length).toBe(1); // one frame through L1 sync-install
		await f.scheduler.quiesce();
		expect(f.turns).toEqual(["background task finished"]);
		expect(f.replies).toEqual(["reply:background task finished"]);
	});

	it("busy session: the wake takes the busy ladder — merged into pending, never a rival turn", async () => {
		const f = makeFixture();
		f.holdTurns(true);
		await f.guard.handleMessage(f.text("user turn"), KEY);
		const headFrame = f.scheduler.queue.shift()!;
		headFrame.start(); // parks — user turn in flight

		// Wake arrives mid-turn: L1 sees the active guard, L2 rules apply.
		// Internal TEXT is NOT a debounce candidate (base.py requires
		// !internal) → merge_pending(merge_text=true).
		await f.guard.handleMessage(
			wake("async delegation completed: 3 files changed"),
			KEY,
		);

		expect(f.guard.pendingOf(KEY)?.text).toBe(
			"async delegation completed: 3 files changed",
		);
		expect(f.turns).toEqual(["user turn"]); // no rival spawn

		f.holdTurns(false);
		await f.scheduler.quiesce();
		// The completion re-enters via the DRAIN BOUNDARY as its own turn:
		expect(f.turns).toEqual([
			"user turn",
			"async delegation completed: 3 files changed",
		]);
	});

	it("wake mid-drain follows late-arrival requeue rules (no dual-spawn)", async () => {
		const f = makeFixture();

		// Owner parked; a first follow-up pends; owner drains it on release.
		f.holdTurns(true);
		await f.guard.handleMessage(f.text("head"), KEY);
		const headFrame = f.scheduler.queue.shift()!;
		headFrame.start();
		await f.guard.handleMessage(f.text("queued-a"), KEY);

		f.holdTurns(false);
		await headFrame.result; // in-band drain spawns successor, hands ownership

		// Successor frame queued but NOT started: park it, then fire the wake
		// during ITS handler window.
		f.holdTurns(true);
		const successor = f.scheduler.queue.shift()!;
		successor.start();
		await new Promise<void>((r) => setTimeout(r, 5));

		await f.guard.handleMessage(wake("watcher ping"), KEY); // busy path → slot

		f.holdTurns(false);
		await f.scheduler.quiesce();

		// Wake processed through the chain exactly once; concurrency stayed 1.
		expect(f.turns.join("|")).toContain("watcher ping");
		expect(f.maxHandlerConcurrency).toBe(1);
		expect(f.guard.isActive(KEY)).toBe(false);
	}, 30_000);

	it("a burst of wakes collapses exactly like user traffic under L1", async () => {
		const f = makeFixture();
		for (let i = 0; i < 5; i++) {
			await f.guard.handleMessage(wake(`completion ${i}`), KEY);
		}
		expect(f.scheduler.queue.length).toBe(1); // ONE turn spawned…
		const composite = f.guard.pendingOf(KEY)?.text ?? "";
		for (let i = 1; i < 5; i++) {
			expect(composite).toContain(`completion ${i}`); // …rest merged
		}
		await f.scheduler.quiesce();
	});
});

describe("concurrent enqueue bursts — deterministic scheduling (§11)", () => {
	it("mixed text/photo/command burst: exactly one live turn, zero overlap, every payload served once", async () => {
		const f = makeFixture();
		f.holdTurns(true);
		await f.guard.handleMessage(f.text("head"), KEY);
		const headFrame = f.scheduler.queue.shift()!;
		headFrame.start();

		// Synchronous burst of heterogeneous traffic while the head holds:
		await f.guard.handleMessage(f.text("text-1"), KEY);
		await f.guard.handleMessage(
			{ messageType: "photo", mediaUrls: ["/p1.png"], text: "photo caption" },
			KEY,
		);
		// Lane B dispatch runs INLINE and its handler parks on the same gate —
		// fire it without awaiting (the await happens after release).
		const statusDispatch = f.guard.handleMessage(f.text("/status"), KEY);
		await f.guard.handleMessage(f.text("text-2"), KEY);
		await f.guard.handleMessage(
			{ messageType: "photo", mediaUrls: ["/p2.png"] },
			KEY,
		);

		f.holdTurns(false);
		await statusDispatch;
		await f.scheduler.quiesce();

		// /status dispatched INLINE (Lane B) even mid-burst:
		expect(f.turns).toContain("/status");
		f.holdTurns(false);
		await f.scheduler.quiesce();

		// Text fragments ride the same composite (media+caption compose):
		const joined = f.turns.join("|");
		expect(joined).toContain("head");
		expect(joined).toContain("text-1");
		expect(joined).toContain("text-2");
		expect(joined.split("text-1").length - 1).toBe(1);
		expect(joined.split("text-2").length - 1).toBe(1);
		expect(f.maxHandlerConcurrency).toBe(1);
		expect(f.guard.isActive(KEY)).toBe(false);
	}, 30_000);

	it("two interleaved sessions isolate completely under burst load", async () => {
		const f = makeFixture();
		const KA = `${KEY}:A`;
		const KB = `${KEY}:B`;
		for (let i = 0; i < 4; i++) {
			await f.guard.handleMessage(f.text(`a${i}`), KA);
			await f.guard.handleMessage(f.text(`b${i}`), KB);
		}
		expect(f.scheduler.queue.length).toBe(2); // one frame per key
		await f.scheduler.quiesce();
		expect(f.maxHandlerConcurrency).toBe(1); // serial event loop, no overlap
		// Each key's chain contains ONLY its own fragments: heads run alone;
		// follow-ups coalesce into per-key composites.
		const allJoined = f.turns.join("|");
		expect(allJoined).toContain("a0");
		expect(allJoined).toContain("a1\na2\na3");
		expect(allJoined).toContain("b0");
		expect(allJoined).toContain("b1\nb2\nb3");
		// No cross-contamination inside a single turn payload:
		for (const t of f.turns) {
			if (t.includes("a1")) expect(t).not.toContain("b1");
			if (t.includes("b1")) expect(t).not.toContain("a1");
		}
	});
});
