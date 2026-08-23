// 03 §3.1 merge table, §3.2 debounce, §11 rows "Pending merge rules" and
// "Debounce": photo+photo extends; media+caption composes; text appends ONLY
// when merge_text; else replace. Debounce: burst within window ⇒ ONE composite
// turn; hard cap bounds delay; sender split flushes separately.
//
// Debounce buffering exists only while a turn is ACTIVE (base.py queues text
// from the busy branch alone), so every scenario holds the head turn open via
// the fixture gate before delivering follow-ups.

import { describe, expect, it } from "vitest";
import type { IncomingEvent } from "./events.js";
import {
	mergeCaption,
	mergePendingEvent,
	canMergeTextDebounceEvents,
} from "./events.js";
import { makeFixture, FakeTimers } from "./testing/manual-spawner.js";

const KEY = "agent:main:telegram:dm:100";

function sender(platform = "telegram", userId = "u1") {
	return { platform, chatType: "group", userId, chatId: "c1" };
}

function textEvent(
	text: string,
	extra: Partial<IncomingEvent> = {},
): IncomingEvent {
	return {
		messageType: "text",
		text,
		// Real arrivals always carry a source; debounce identity needs one.
		source: {
			platform: "telegram",
			chatType: "dm",
			userId: "u1",
			chatId: "100",
		},
		...extra,
	};
}

describe("pending slot merge rules (§3.1)", () => {
	it("photo+photo extends media lists and dedups captions", () => {
		const slots = new Map<string, IncomingEvent>();
		mergePendingEvent(slots, KEY, {
			messageType: "photo",
			text: "caption one",
			mediaUrls: ["/a.png"],
			mediaTypes: ["image/png"],
			source: sender(),
		});
		const second: Partial<IncomingEvent> = {
			messageType: "photo",
			text: "caption two",
			mediaUrls: ["/b.png"],
			mediaTypes: ["image/png"],
		};
		mergePendingEvent(slots, KEY, {
			...second,
			source: sender(),
		} as IncomingEvent);
		const merged = slots.get(KEY);
		expect(merged?.mediaUrls).toEqual(["/a.png", "/b.png"]);
		expect(merged?.text).toBe("caption one\n\ncaption two");

		// Duplicate caption NOT appended twice (block-exact comparison —
		// shorter captions must not vanish as substrings either).
		mergePendingEvent(slots, KEY, {
			messageType: "photo",
			text: "caption two",
			mediaUrls: ["/b.png"],
			mediaTypes: ["image/png"],
		});
		expect(slots.get(KEY)?.text).toBe("caption one\n\ncaption two");
	});

	it("media+caption composes; photo in composite promotes the head slot", () => {
		const slots = new Map<string, IncomingEvent>();
		mergePendingEvent(slots, KEY, {
			messageType: "video",
			mediaUrls: ["/v.mp4"],
		});
		mergePendingEvent(slots, KEY, textEvent("look"));
		let merged = slots.get(KEY);
		expect(merged?.messageType).toBe("video");
		expect(merged?.text).toBe("look");
		expect(merged?.mediaUrls).toEqual(["/v.mp4"]);

		mergePendingEvent(slots, KEY, {
			messageType: "photo",
			mediaUrls: ["/p.png"],
		});
		merged = slots.get(KEY);
		expect(merged?.messageType).toBe("photo");
		expect(merged?.mediaUrls).toEqual(["/v.mp4", "/p.png"]);
	});

	it("text appends with \\n only when merge_text; otherwise REPLACE (newest wins)", () => {
		const slots = new Map<string, IncomingEvent>();
		mergePendingEvent(slots, KEY, textEvent("one"));
		mergePendingEvent(slots, KEY, textEvent("two"), { mergeText: true });
		expect(slots.get(KEY)?.text).toBe("one\ntwo");

		mergePendingEvent(slots, KEY, textEvent("three")); // no merge_text
		expect(slots.get(KEY)?.text).toBe("three");
	});

	it("_merge_caption block-dedup: substring captions are NOT treated as duplicates", () => {
		expect(mergeCaption("Meeting agenda", "Meeting")).toBe(
			"Meeting agenda\n\nMeeting",
		);
		expect(mergeCaption(undefined, "first")).toBe("first");
	});

	it("mixed-sender debounce identity: shared-session senders never coalesce", () => {
		const alice = sender("tg", "alice");
		const bob = sender("tg", "bob");
		const a = textEvent("a", { source: alice });
		const b = textEvent("b", { source: bob });
		const aOtherPlatform = textEvent("a2", {
			source: sender("whatsapp", "alice"),
		});
		expect(canMergeTextDebounceEvents(a, b)).toBe(false); // different sender
		expect(canMergeTextDebounceEvents(a, aOtherPlatform)).toBe(false); // platform differs
		expect(
			canMergeTextDebounceEvents(a, textEvent("a3", { source: alice })),
		).toBe(true);
	});
});

describe("debounced text queueing (§3.2)", () => {
	async function holdHeadTurn(
		f: ReturnType<typeof makeFixture>,
	): Promise<void> {
		f.holdTurns(true);
		await f.guard.handleMessage(textEvent("head"), KEY);
		// Start the head frame but DO NOT await it — its handler parks on the
		// hold gate while the test delivers busy-path traffic.
		f.scheduler.queue.shift()?.start();
	}

	it("burst within the window fuses into ONE composite turn delivered by the drain chain", async () => {
		const f = makeFixture({ busyTextMode: "queue", debounceWindowMs: 350 });
		await holdHeadTurn(f);

		// Busy follow-ups land in the debounce buffer, NOT the slot.
		await f.guard.handleMessage(textEvent("part one"), KEY);
		await f.guard.handleMessage(textEvent("part two"), KEY);
		expect(f.guard.debounceBufferedText(KEY)).toBe("part one\npart two");
		expect(f.guard.pendingOf(KEY)).toBeUndefined();

		// Release the head: its in-band drain OWNS debounce state — the
		// force-flush folds the burst into the slot, then hands off ONCE.
		f.holdTurns(false);
		await f.scheduler.quiesce();

		expect(f.turns).toEqual(["head", "part one\npart two"]); // ONE composite turn
	});

	it("hard cap bounds delay: repeated arrivals postpone within the window but NEVER past first_ts + cap", async () => {
		const clock = new FakeTimers();
		const scheduledDelays: number[] = [];
		const f = makeFixture({
			busyTextMode: "queue",
			debounceWindowMs: 350,
			debounceHardCapMs: 1000,
			nowMs: clock.nowMs,
			scheduleTimer: (delayMs, fn) => {
				scheduledDelays.push(delayMs);
				return clock.scheduleTimer(delayMs, fn);
			},
		});
		await holdHeadTurn(f);

		// Sustained burst: each arrival resets the timer, but every scheduled
		// flush stays bounded by first_ts + cap (1000ms) — a burst cannot
		// postpone a turn forever.
		for (let i = 0; i < 10; i++) {
			clock.nowVal = i * 300; // windows always miss; cap must bind
			await f.guard.handleMessage(textEvent(`frag ${i}`), KEY);
		}
		expect(scheduledDelays.length).toBeGreaterThanOrEqual(9); // reset per arrival
		for (const delay of scheduledDelays) {
			expect(delay).toBeLessThanOrEqual(1000);
			expect(delay).toBeGreaterThanOrEqual(0);
		}
	});

	it("timer expiry flushes the composite into the slot while the turn still runs", async () => {
		const clock = new FakeTimers();
		const f = makeFixture({
			busyTextMode: "queue",
			nowMs: clock.nowMs,
			scheduleTimer: clock.scheduleTimer,
		});
		await holdHeadTurn(f);

		await f.guard.handleMessage(textEvent("early"), KEY);
		clock.advance(350); // window deadline passes → timer fires
		expect(f.guard.pendingOf(KEY)?.text).toBe("early"); // flushed into slot
		expect(f.guard.debounceBufferedText(KEY)).toBeNull();

		// The RUNNING turn picks it up at its drain boundary — not before:
		expect(f.turns).toEqual(["head"]);
		f.holdTurns(false);
		await f.scheduler.quiesce();
		expect(f.turns).toEqual(["head", "early"]);
	});

	it("reply anchor follows the LATEST fragment of the composite", async () => {
		const f = makeFixture({ busyTextMode: "queue", debounceWindowMs: 350 });
		await holdHeadTurn(f);

		await f.guard.handleMessage(
			textEvent("frag one", { messageId: "m-1", replyToMessageId: "r-0" }),
			KEY,
		);
		await f.guard.handleMessage(
			textEvent("frag two", { messageId: "m-2" }),
			KEY,
		);

		// Buffered event anchors updated to the latest fragment…
		const state = f.guard.pendingDebounceStateForTests(KEY);
		expect(state?.event.messageId).toBe("m-2");
		expect(state?.event.replyToMessageId).toBe("m-2"); // anchor = latest id

		f.guard.flushTextDebounceNow(KEY);
		expect(f.guard.pendingOf(KEY)?.messageId).toBe("m-2");
	});

	it("sender split: mixed-sender burst flushes the current buffer FIRST as its own turn", async () => {
		const f = makeFixture({ busyTextMode: "queue" });
		await holdHeadTurn(f);

		await f.guard.handleMessage(
			textEvent("from alice", { source: sender("tg", "alice") }),
			KEY,
		);
		// Bob arrives while alice's buffer holds:
		await f.guard.handleMessage(
			textEvent("from bob", { source: sender("tg", "bob") }),
			KEY,
		);

		// Alice's buffer flushed into the slot; bob started a FRESH buffer.
		expect(f.guard.pendingOf(KEY)?.text).toBe("from alice");
		expect(f.guard.debounceBufferedText(KEY)).toBe("from bob");
	});

	it("control commands discard the buffer (#2170 family): stale text must not replay", async () => {
		const f = makeFixture({ busyTextMode: "queue" });
		await holdHeadTurn(f);

		await f.guard.handleMessage(textEvent("doomed text"), KEY);
		expect(f.guard.debounceBufferedText(KEY)).toBe("doomed text");

		// /new bypass discards debounce BEFORE dispatching Lane A. The command
		// handler itself parks on the hold gate — fire without awaiting it.
		const newDispatch = f.guard.handleMessage(textEvent("/new"), KEY);
		expect(f.guard.debounceBufferedText(KEY)).toBeNull();

		f.holdTurns(false);
		await newDispatch;
		await f.scheduler.quiesce();
		expect(JSON.stringify(f.turns)).not.toContain("doomed text");
	});
});
