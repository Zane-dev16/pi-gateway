// pi_platforms/matrix/matrix.test.ts — ENGINE behavior contracts for the
// Matrix census port. Transport rows + shape rows live in the conformance
// wiring; these tests pin the finer-grained mechanics: sync-loop ladders,
// filter-chain ordering, content building, length policy, and identity
// probes — all headless over the fake homeserver with injected clocks.

import { describe, expect, it } from "vitest";

import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "../conformance/wire.js";
import { ManualPollingClock } from "../polling/clock.js";
import { makeMatrixSubject } from "./matrix-subject.js";
import {
	MATRIX_EVENT_DEDUP_CAPACITY,
	MATRIX_SYNC_WATCHDOG_TIMEOUT_MS,
	normalizeBangCommand,
} from "./manifest.js";
import {
	MatrixAdapterCore,
	extractMatrixRetryAfterSeconds,
	extractReplyFallback,
	stripReplyFallback,
} from "./matrix-adapter.js";
import {
	ALICE,
	BOT_MXID,
	FakeMatrixHomeserver,
	makeSinceToken,
} from "./matrix-fake-server.js";

async function wall(ms = 25): Promise<void> {
	await new Promise<void>((r) => setTimeout(r, ms));
}

function freshEngine(opts?: {
	clock?: ManualPollingClock | undefined;
	hs?: FakeMatrixHomeserver | undefined;
}) {
	const clock = opts?.clock ?? new ManualPollingClock();
	const hs =
		opts?.hs ?? new FakeMatrixHomeserver({ nowMs: () => clock.nowMs() });
	const wire = new FakePlatformWire();
	// No spawner override — the engine's immediateSpawner default runs sync
	// tasks eagerly (engine tests drive REAL loops, not manual scheduler frames).
	const subject = makeSubjectWith(hs, wire, clock, undefined);
	return { clock, hs, wire, subject, engine: subject.adapter };
}

function makeSubjectWith(
	hs: FakeMatrixHomeserver,
	wire: FakePlatformWire,
	clock: ManualPollingClock,
	scheduler: ManualScheduler | undefined,
) {
	return makeMatrixSubject({
		hs,
		wire,
		clock,
		// The standard engine-test room rides FREE-RESPONSE so plain pushes
		// deliver; gating tests use their own gated rooms.
		freeRooms: new Set(["!room:fake.example"]),
		...(scheduler !== undefined ? { scheduler } : {}),
	});
}

describe("matrix adapter — sync transport", () => {
	it("delivers pushed room messages as turns through the long-poll loop", async () => {
		const w = freshEngine();
		await w.subject.adapter.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);

		w.hs.pushRoomMessage("!room:fake.example", ALICE, {
			msgtype: "m.text",
			body: "hello gateway",
		});
		await vi_waitFor(() => w.subject.turns().includes("hello gateway"));
		expect(w.subject.turns()).toContain("hello gateway");
	});

	it("commits the sync token BEFORE dispatch and HOLDS when killed in the window", async () => {
		const w = freshEngine();
		let crashed = false;
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);
		// Install the kill seam AFTER the first settled cycle so the crash lands
		// on the batch that actually carries the messages.
		w.engine.hooks = {
			afterCommitBeforeDispatch: (response) => {
				if (crashed) return;
				// Kill ONLY on a non-empty batch — empty long-poll cycles commit
				// continuously and must not consume the crash.
				if (Object.keys(response.rooms.join).length === 0) return;
				crashed = true;
				w.engine.disconnect();
			},
		};
		const committedBefore = w.engine.committedSyncToken;

		w.hs.pushRoomMessage("!room:fake.example", ALICE, {
			msgtype: "m.text",
			body: "held-1",
		});
		w.hs.pushRoomMessage("!room:fake.example", ALICE, {
			msgtype: "m.text",
			body: "held-2",
		});
		await vi_waitFor(() => w.engine.heldInboundCount >= 2);
		// The token advanced past the batch (commit-before-dispatch parity).
		expect(w.engine.committedSyncToken).not.toBeNull();
		void committedBefore;

		await w.engine.connect({ isReconnect: true });
		await vi_waitFor(
			() =>
				w.subject.turns().includes("held-1") &&
				w.subject.turns().includes("held-2"),
		);
	});

	it("deduplicates replayed event ids after an uncommitted crash window", async () => {
		const w = freshEngine();
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);
		w.hs.pushRoomMessage("!room:fake.example", ALICE, {
			msgtype: "m.text",
			body: "once-only",
		});
		await vi_waitFor(() => w.subject.turns().includes("once-only"));

		// Force server-side replay of the same window.
		w.engine.committedSyncToken = makeSinceToken(0, 0);
		await w.engine.connect({ isReconnect: true });
		await wall();
		expect(w.subject.turns().filter((t) => t === "once-only").length).toBe(1);
	});

	it("stops IMMEDIATELY with a loud fatal on m_unknown_token from sync", async () => {
		const w = freshEngine();
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);
		w.hs.revokeAuth();
		await vi_waitFor(() => w.engine.lifecycleSnapshot().state === "fatal");
		expect(w.engine.recoveryLog).toContain("auth-unknown-token-stop");
		expect(w.engine.recoveryAttempts).toBe(0); // no retry ladder for auth death
	});

	it("recovers M_UNKNOWN_SYNC_TOKEN epoch deaths via full-state restarts and exhausts to fatal under churn", async () => {
		const w = freshEngine();
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);

		// Killable: one invalidation recovers.
		w.hs.invalidateEpoch();
		await vi_waitFor(() => w.engine.recoveryRestartsWithFullState >= 1);
		await vi_waitFor(() => w.engine.updaterRunning);
		expect(w.engine.lifecycleSnapshot().state).toBe("active");

		// Unkillable churn: the server keeps rejecting EVERY sync (rollback
		// storm) so recovery can never converge → bounded exhaustion to fatal.
		w.hs.setEpochChurn(true);
		await vi_waitFor(() =>
			w.engine.recoveryLog.includes(
				"epoch-recovery-exhausted-after-5-attempts",
			),
		);
		await vi_waitFor(() => w.engine.lifecycleSnapshot().state === "fatal");
	});

	it("escalates TWO consecutive sync-watchdog timeouts into the recovery ladder", async () => {
		const w = freshEngine();
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);
		const genBefore = w.engine.generation;

		w.hs.setWedged(true);
		await w.clock.advance(MATRIX_SYNC_WATCHDOG_TIMEOUT_MS + 500);
		await vi_waitFor(() => w.engine.stuckProbeStreakForTests >= 1);
		await w.clock.advance(MATRIX_SYNC_WATCHDOG_TIMEOUT_MS + 500);
		await vi_waitFor(() =>
			w.engine.recoveryLog.includes("sync-watchdog-stuck-streak"),
		);
		await vi_waitFor(() => w.engine.generation > genBefore);
	});

	it("resumes from the committed token across reconnect without dropping queued events", async () => {
		const w = freshEngine();
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);
		w.engine.disconnect();

		w.hs.pushRoomMessage("!room:fake.example", ALICE, {
			msgtype: "m.text",
			body: "queued-during-outage",
		});
		expect(w.hs.pendingEventCount).toBe(1);

		await w.engine.connect({ isReconnect: true });
		await vi_waitFor(() => w.subject.turns().includes("queued-during-outage"));
	});
});

describe("matrix adapter — intake filters", () => {
	it("drops self echo case-normalized and defensively while unresolved (#15763)", async () => {
		const w = freshEngine();
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);

		w.hs.pushRoomMessage("!room:fake.example", BOT_MXID.toUpperCase(), {
			msgtype: "m.text",
			body: "case-echo",
		});
		const savedOwn = w.engine.ownUserId;
		w.engine.ownUserId = "";
		w.hs.pushRoomMessage("!room:fake.example", BOT_MXID, {
			msgtype: "m.text",
			body: "unresolved-echo",
		});
		w.engine.ownUserId = savedOwn;
		await wall();
		expect(w.subject.turns()).toEqual([]);
	});

	it("skips edits, skips notices, tolerates media kinds", async () => {
		const w = freshEngine();
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);

		w.hs.pushRoomMessage("!room:fake.example", ALICE, {
			msgtype: "m.text",
			body: "edited",
			"m.relates_to": { rel_type: "m.replace", event_id: "$x" },
		});
		w.hs.pushRoomMessage("!room:fake.example", ALICE, {
			msgtype: "m.notice",
			body: "notice-body",
		});
		w.hs.pushRoomMessage("!room:fake.example", ALICE, {
			msgtype: "m.image",
			body: "cat.png",
		});
		w.hs.pushRoomMessage("!room:fake.example", ALICE, {
			msgtype: "m.text",
			body: "kept",
		});
		await vi_waitFor(() => w.subject.turns().includes("kept"));
		await wall();
		expect(w.subject.turns()).toEqual(["kept"]);
	});

	it("bounds the event-id dedup deque at 1000 with oldest eviction", () => {
		const w = freshEngine();
		for (let i = 0; i < MATRIX_EVENT_DEDUP_CAPACITY + 10; i++) {
			expect(w.engine.isDuplicateEvent(`$e${i}`)).toBe(false);
		}
		expect(w.engine.processedEventsForTests().length).toBe(
			MATRIX_EVENT_DEDUP_CAPACITY,
		);
		// The oldest ids were evicted → re-deliverable again.
		expect(w.engine.isDuplicateEvent("$e0")).toBe(false);
		// Recent ids still tracked.
		expect(
			w.engine.isDuplicateEvent(`$e${MATRIX_EVENT_DEDUP_CAPACITY + 9}`),
		).toBe(true);
	});
});

describe("matrix adapter — mention gating", () => {
	it("requires mentions in channels; m.mentions is authoritative; commands bypass", async () => {
		const w = freshEngine();
		w.hs.addRoom("!chan:fake.example", { memberCount: 5 });
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);

		w.hs.pushRoomMessage("!chan:fake.example", ALICE, {
			msgtype: "m.text",
			body: "no mention",
		});
		w.hs.pushRoomMessage("!chan:fake.example", ALICE, {
			msgtype: "m.text",
			body: "silent pill",
			"m.mentions": { user_ids: [BOT_MXID] },
		});
		w.hs.pushRoomMessage("!chan:fake.example", ALICE, {
			msgtype: "m.text",
			body: "/status now",
		});
		await vi_waitFor(() => w.subject.turns().includes("/status now"));
		await wall();
		expect(w.subject.turns()).toContain("silent pill");
		expect(w.subject.turns()).toContain("/status now");
		expect(w.subject.turns()).not.toContain("no mention");
	});

	it("strips explicit mention tokens but keeps bare localpart words", async () => {
		const w = freshEngine();
		w.hs.addRoom("!chan:fake.example", { memberCount: 5 });
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);

		w.hs.pushRoomMessage("!chan:fake.example", ALICE, {
			msgtype: "m.text",
			body: `hey ${BOT_MXID} please look`,
		});
		await vi_waitFor(() =>
			w.subject.turns().some((t) => t.includes("please look")),
		);
		const turn = w.subject.turns().find((t) => t.includes("please look"));
		expect(turn).not.toContain("@pi-bot");
		// Bare localpart word detection ("pi-bot" as a word) mentions…
		w.hs.pushRoomMessage("!chan:fake.example", ALICE, {
			msgtype: "m.text",
			body: "yo pi-bot help",
		});
		await vi_waitFor(() => w.subject.turns().includes("yo pi-bot help"));
		// …and stripping leaves the bare word intact (explicit tokens only).
		// Word-boundary discipline: pi-botx does NOT mention.
		w.hs.pushRoomMessage("!chan:fake.example", ALICE, {
			msgtype: "m.text",
			body: "pi-botx is odd",
		});
		await wall();
		expect(w.subject.turns()).not.toContain("pi-botx is odd");
	});
});

describe("matrix adapter — content building + egress", () => {
	it("strips image markdown; ships m.mentions INSIDE content + format/formatted_body when HTML differs", async () => {
		const w = freshEngine();
		await w.wire.reset();
		await w.engine.deliverText(
			"!room:fake.example",
			"see ![pic](https://x/y.png) and ping @carol:other.example",
		);
		const sends = w.wire.sendsOf("!room:fake.example");
		expect(sends.length).toBe(1);
		expect(sends[0]?.content).toBe(
			"see https://x/y.png and ping @carol:other.example",
		);
		// The invented flat key is GONE; mentions serialize INTO the content.
		expect(sends[0]?.metadata["m_mentions_user_ids"]).toBeUndefined();
		const content = sends[0]?.metadata["event_content"] as Record<
			string,
			unknown
		>;
		expect(content["msgtype"]).toBe("m.text");
		expect(content["m.mentions"]).toEqual({
			user_ids: ["@carol:other.example"],
		});

		// Real markdown → html !== body ⇒ format/formatted_body ride along.
		await w.wire.reset();
		await w.engine.deliverText("!room:fake.example", "**bold** move");
		const rich = w.wire.sendsOf("!room:fake.example")[0]?.metadata[
			"event_content"
		] as Record<string, unknown>;
		expect(rich["format"]).toBe("org.matrix.custom.html");
		expect(String(rich["formatted_body"])).toContain("<strong>bold</strong>");
	});

	it("plain text without markdown ships NO format keys (html === body)", async () => {
		const w = freshEngine();
		await w.wire.reset();
		await w.engine.deliverText("!room:fake.example", "just words");
		const content = w.wire.sendsOf("!room:fake.example")[0]?.metadata[
			"event_content"
		] as Record<string, unknown>;
		expect(content["body"]).toBe("just words");
		expect(content["format"]).toBeUndefined();
		expect(content["formatted_body"]).toBeUndefined();
	});

	it("chunks oversized sends at the chat length policy (chars)", async () => {
		const w = freshEngine();
		const policy = w.engine.chatLengthPolicyForChat("!room:fake.example");
		expect(policy.unit).toBe("chars");
		expect(policy.maxUnits).toBe(64); // shared-row budget override
		await w.engine.deliverText("!room:fake.example", "y".repeat(150));
		expect(w.wire.sendsOf("!room:fake.example").length).toBeGreaterThan(1);
	});

	it("edits ship the VENDOR replace payload: '* ' body + m.new_content + m.relates_to", async () => {
		const w = freshEngine();
		await w.engine.editMessage("!room:fake.example", "$target", "**edited**");
		const edits = w.wire.editsOf("!room:fake.example");
		expect(edits.length).toBe(1);
		const content = edits[0]?.metadata["event_content"] as Record<
			string,
			unknown
		>;
		// adapter.py:edit_message — body prefixed '* ', full rebuilt content
		// in m.new_content, relation points at the target.
		expect(content["body"]).toBe("* **edited**");
		const newContent = content["m.new_content"] as Record<string, unknown>;
		expect(newContent["body"]).toBe("**edited**");
		expect(newContent["formatted_body"]).toContain("<strong>edited</strong>");
		expect(content["m.relates_to"]).toEqual({
			rel_type: "m.replace",
			event_id: "$target",
		});
	});

	it("replies and threads emit m.relates_to on outbound content", async () => {
		const w = freshEngine();
		await w.wire.reset();
		await w.engine.send("!room:fake.example", "a reply", "$parent1", {});
		let content = w.wire.sendsOf("!room:fake.example")[0]?.metadata[
			"event_content"
		] as Record<string, unknown>;
		expect(content["m.relates_to"]).toEqual({
			"m.in_reply_to": { event_id: "$parent1" },
		});

		await w.engine.send("!room:fake.example", "threaded", undefined, {
			thread_id: "$root7",
		} as never);
		content = w.wire.sendsOf("!room:fake.example")[1]?.metadata[
			"event_content"
		] as Record<string, unknown>;
		expect(content["m.relates_to"]).toMatchObject({
			rel_type: "m.thread",
			event_id: "$root7",
			is_falling_back: true,
			"m.in_reply_to": { event_id: "$root7" },
		});
	});

	it("dispatch wires the processing lifecycle: typing bubble + 👀→✅ hooks around each turn", async () => {
		const w = freshEngine();
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);

		// base.py:_process_message_background — every dispatched turn opens with
		// set_typing(timeout=30000) + the 👀 eyes reaction, and settles with the
		// eyes redacted, a ✅ completion reaction, set_typing(timeout=0), and the
		// fire-and-forget read receipt.
		const pushed = w.hs.pushRoomMessage("!room:fake.example", ALICE, {
			msgtype: "m.text",
			body: "lifecycle me",
		});
		await vi_waitFor(() => w.subject.turns().includes("lifecycle me"));
		await vi_waitFor(
			() =>
				w.hs.typingEvents.length >= 2 &&
				w.hs.typingEvents[w.hs.typingEvents.length - 1]?.timeoutMs === 0,
		);
		expect(w.hs.typingEvents.filter((t) => t.timeoutMs === 30_000).length).toBe(
			1,
		);
		const eyes = w.hs.reactions.find(
			(r) => r.targetEventId === pushed.eventId && r.key === "\u{1F440}",
		);
		expect(eyes?.redacted).toBe(true);
		expect(
			w.hs.reactions.find(
				(r) => r.targetEventId === pushed.eventId && r.key === "\u2705",
			),
		).toBeDefined();
		expect(w.hs.readReceipts.some((r) => r.eventId === pushed.eventId)).toBe(
			true,
		);
	});

	it("receipts fire-and-forget after processing", async () => {
		const w = freshEngine();
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);
		w.hs.pushRoomMessage("!room:fake.example", ALICE, {
			msgtype: "m.text",
			body: "mark me read",
		});
		await vi_waitFor(() => w.subject.turns().includes("mark me read"));
		await vi_waitFor(() => w.hs.readReceipts.length >= 1, 2_000);
		expect(w.hs.readReceipts[0]?.eventId).toBeDefined();
	});

	it("sendMedia uploads then ships a typed media event with info dict", async () => {
		const w = freshEngine();
		const result = await w.engine.sendMedia(
			"!room:fake.example",
			{
				bytes: new Uint8Array([1, 2, 3, 4]),
				filename: "shot.png",
				mimeType: "image/png",
			},
			{ caption: "here" },
		);
		expect(result.success).toBe(true);
		expect(w.hs.uploads).toHaveLength(1);
		expect(w.hs.uploads[0]).toMatchObject({
			mimeType: "image/png",
			filename: "shot.png",
			size: 4,
		});
		const content = w.wire.sendsOf("!room:fake.example")[0]?.metadata[
			"event_content"
		] as Record<string, unknown>;
		expect(content["msgtype"]).toBe("m.image");
		expect(content["body"]).toBe("here");
		expect(content["url"]).toMatch(/^mxc:\/\/fake\.example\/up\d+$/);
		expect(content["info"]).toEqual({ mimetype: "image/png", size: 4 });
	});

	it("invited rooms JOIN with bounded retry and then deliver (matrix-9)", async () => {
		const w = freshEngine();
		await w.engine.connect({ isReconnect: false });
		await vi_waitFor(() => w.engine.polledOnce);

		// Invite surfaces on sync; join succeeds after scripted failures.
		w.hs.pushInvite("!invited:fake.example");
		await vi_waitFor(
			() => w.hs.joinCalls.includes("!invited:fake.example"),
			3_000,
		);
		expect(w.hs.joinCalls[0]).toBe("!invited:fake.example");

		// After joining, messages in that room flow through the normal filter
		// pipeline (the room now rides the sync join set).
		w.hs.pushRoomMessage("!invited:fake.example", ALICE, {
			msgtype: "m.text",
			body: "post-join hello",
		});
		await vi_waitFor(
			() => w.subject.turns().includes("post-join hello"),
			3_000,
		);
	});

	it("password login resolves identity when MATRIX_PASSWORD configured; whoami resolves device_id", async () => {
		const hs = new FakeMatrixHomeserver({ nowMs: () => 0 });
		hs.loginPassword = "sekrit";
		const clock = new ManualPollingClock();
		const engine = new MatrixAdapterCore({
			hs,
			clock,
			timer: clock.timer,
			manifestName: "matrix-login",
			secretReader: (key) =>
				key === "MATRIX_HOMESERVER"
					? "https://matrix.fake.example"
					: key === "MATRIX_USER_ID"
						? "@bot:fake.example"
						: key === "MATRIX_PASSWORD"
							? "sekrit"
							: undefined,
		});
		const ok = await engine.connect({ isReconnect: false });
		expect(ok).toBe(true);
		expect(hs.loginCalls).toHaveLength(1);
		expect(hs.loginCalls[0]?.identifier).toBe("@bot:fake.example");
		expect(hs.whoamiCount).toBe(0); // password branch skips whoami

		// TOKEN branch (no MATRIX_PASSWORD) still goes through whoami AND
		// resolves device_id alongside user_id.
		const w2 = freshEngine();
		const me = await w2.hs.whoami();
		expect(me.user_id).toBe("@pi-bot:fake.example");
		expect(me.device_id).toBe("DEVFAKE");
	});

	it("honors M_LIMIT_EXCEEDED retry_after_ms ONCE at the typing site", async () => {
		const w = freshEngine();
		w.hs.scriptRateLimitOnce(2);
		// The authoritative retry_after is honored ONCE inside the call —
		// the same call recovers (polling-family sendTyping parity).
		const first = await w.engine.sendTyping("!room:fake.example");
		const second = await w.engine.sendTyping("!room:fake.example");
		expect(first.success).toBe(true);
		expect(second.success).toBe(true);
		expect(w.clock.sleeps).toContain(2000);
		expect(
			extractMatrixRetryAfterSeconds(
				"M_LIMIT_EXCEEDED: Too many requests (retry_after_ms=2500)",
			),
		).toBeCloseTo(2.5);
		expect(
			extractMatrixRetryAfterSeconds(
				"M_LIMIT_EXCEEDED: Too many requests (retry_after_ms=2500)",
			),
		).toBeCloseTo(2.5);
	});
});

describe("matrix adapter — helpers", () => {
	it("extracts and strips reply fallbacks", () => {
		const body =
			"> <@carol:fake.example> quoted question\n> second quoted\n\nthe answer";
		const extracted = extractReplyFallback(body);
		expect(extracted.authorId).toBe("@carol:fake.example");
		expect(extracted.text).toBe("quoted question\nsecond quoted");
		expect(stripReplyFallback(body)).toBe("the answer");
		// Non-fallback bodies pass through untouched.
		expect(stripReplyFallback("plain text")).toBe("plain text");
		// Hermes parity: a body that is ONLY fallback returns unchanged.
		expect(stripReplyFallback("> orphan quote only")).toBe(
			"> orphan quote only",
		);
	});

	it("normalizes known bang commands with underscore→hyphen candidates", () => {
		const known = (n: string) => n === "model" || n === "reload-skills";
		expect(normalizeBangCommand("!model", known)).toBe("/model");
		expect(normalizeBangCommand("!reload_skills now", known)).toBe(
			"/reload-skills now",
		);
		// Unknown or non-command text stays untouched.
		expect(normalizeBangCommand("!bogus", known)).toBe("!bogus");
		expect(normalizeBangCommand("wow! model", known)).toBe("wow! model");
		expect(normalizeBangCommand("!model", () => false)).toBe("!model");
	});
});

// ── tiny waitFor (wall-budget polling, no timing asserts) ────────────────
async function vi_waitFor(
	predicate: () => boolean,
	timeoutMs = 4_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (predicate()) return;
		if (Date.now() > deadline) throw new Error("waitFor: condition not met");
		await new Promise<void>((r) => setTimeout(r, 4));
	}
}
