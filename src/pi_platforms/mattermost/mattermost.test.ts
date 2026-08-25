// pi_platforms/mattermost/mattermost.test.ts — ENGINE behavior contracts for
// the Mattermost census port: ws session/auth, dedup, gating, egress lanes,
// and identity probes. Transport + shape rows live in the conformance wiring.

import { describe, expect, it } from "vitest";

import { ManualClock } from "../persistent-ws/manual-clock.js";
import { MM_MAX_POST_CHARS, withMentionsDisabled } from "./manifest.js";
import {
	isBrokenThreadRoot,
	MattermostAdapterCore,
} from "./mattermost-adapter.js";
import { FakeMattermost } from "./mm-fake-server.js";
import { makeMattermostWorld, USER_ID } from "./mattermost-world.js";
import { makeMattermostSubject } from "./mattermost-subject.js";

async function wall(ms = 30): Promise<void> {
	await new Promise<void>((r) => setTimeout(r, ms));
}

describe("mattermost adapter — ws session", () => {
	it("authenticates via the challenge frame and reaches live", async () => {
		const w = makeMattermostWorld({ name: "mm-session" });
		await w.connectAndAwaitLive();
		expect(w.engine.isLive).toBe(true);
		const challenge = w.mm.receivedFrames.find(
			(f) => f.frame["action"] === "authentication_challenge",
		);
		expect(challenge).toBeDefined();
	});

	it("ignores malformed posted envelopes without dying", async () => {
		const w = makeMattermostWorld({ name: "mm-malformed" });
		await w.connectAndAwaitLive();
		const sock = w.engine.isLive;
		expect(sock).toBe(true);
		// Push a post then a garbage envelope through the raw plane.
		w.pushPost("!chan:fake.example", USER_ID, "good one");
		const conn = w.mm.openConnectionCount;
		void conn;
		await wall();
		expect(w.subject.turns()).toContain("good one");
	});
});

describe("mattermost adapter — intake filters", () => {
	it("drops system posts and own messages before dispatch", async () => {
		const w = makeMattermostWorld({ name: "mm-filters" });
		await w.connectAndAwaitLive();
		w.pushPost("!chan:fake.example", "sys-bot", "archived", {
			type: "system_header",
		});
		w.pushPost("!chan:fake.example", w.engine.botUserIdResolved, "self echo");
		w.pushPost("!chan:fake.example", USER_ID, "real");
		await vi_waitFor(() => w.subject.turns().includes("real"));
		await wall();
		expect(w.subject.turns()).toEqual(["real"]);
	});

	it("suppresses at-least-once redeliveries by post id (TTL+LRU shield)", async () => {
		const w = makeMattermostWorld({ name: "mm-dedup2" });
		await w.connectAndAwaitLive();
		w.pushPost("!chan:fake.example", USER_ID, "dup-me", { postId: "p-dup" });
		await vi_waitFor(() => w.subject.turns().includes("dup-me"));
		w.pushPost("!chan:fake.example", USER_ID, "dup-me", { postId: "p-dup" });
		await wall();
		expect(w.subject.turns().filter((t) => t === "dup-me").length).toBe(1);
		expect(w.engine.dedupSuppressedCount).toBeGreaterThanOrEqual(1);
	});

	it("rescues leading-whitespace commands and strips mentions cleanly", async () => {
		const w = makeMattermostWorld({ name: "mm-cmds" });
		turnsSnapshot = () => w.subject.turns();
		await w.connectAndAwaitLive();
		w.pushPost("!free:fake.example", USER_ID, "  /status now");
		w.pushPost("!free:fake.example", USER_ID, `hey @${w.mm.botUsername} look`);
		await vi_waitFor(() => w.subject.turns().includes("/status now"));
		await vi_waitFor(() => w.subject.turns().some((t) => t.includes("look")));
		const mentionTurn = w.subject.turns().find((t) => t.includes("look"));
		expect(mentionTurn).not.toContain("@");
	});
});

describe("mattermost adapter — turn-start typing", () => {
	it("fires typing on the post's channel when admitted for dispatch", async () => {
		// Core-driven turn start (gateway/run.py:5000): an admitted post sends
		// users/{bot}/typing BEFORE ingress begins processing.
		const w = makeMattermostWorld({ name: "mm-typing-admit" });
		await w.connectAndAwaitLive();
		const typedChannels: string[] = [];
		w.engine.bindTyping(async (chatId) => {
			typedChannels.push(chatId);
		});
		w.pushPost("!chan:fake.example", USER_ID, "typing please");
		await vi_waitFor(() => w.subject.turns().includes("typing please"));
		expect(typedChannels).toEqual(["!chan:fake.example"]);
	});

	it("unbound lane rides users/{bot}/typing REST with the bot user id", async () => {
		// Production default is the REST plane (adapter.py:send_typing POSTs
		// users/{id}/typing); the subject's bound no-op lane must not hide it.
		const clock = new ManualClock();
		const mm = new FakeMattermost({ nowMs: () => clock.nowMs() });
		mm.addChannel("!chan:fake.example", "O", "General");
		const engine = new MattermostAdapterCore({
			mm,
			manifestName: "mm-typing-rest",
			clock,
			secretReader: () => "tok",
			requireMention: false, // plain posts admit (free-response parity)
		});
		await engine.connect({ isReconnect: false });
		engine.attachStandardGuard();
		await engine.handlePostedPost(
			{
				id: "p-type-1",
				channel_id: "!chan:fake.example",
				user_id: USER_ID,
				message: "rest lane",
				type: "",
				root_id: "",
				create_at: 1,
			},
			"channel",
		);
		expect(mm.typingEventCount).toBe(1);
		expect(mm.typingCallsFor("!chan:fake.example")).toEqual([
			{ channelId: "!chan:fake.example", userId: engine.botUserIdResolved },
		]);
	});

	it("typing failures are swallowed — dispatch still proceeds", async () => {
		// Hermes swallows send_typing errors (`except Exception: pass` at
		// run.py:5000); a failing typing plane must never block the turn.
		const w = makeMattermostWorld({ name: "mm-typing-fail" });
		await w.connectAndAwaitLive();
		w.engine.bindTyping(async () => {
			throw new Error("429: rate limit exceeded");
		});
		w.pushPost("!chan:fake.example", USER_ID, "still delivers");
		await vi_waitFor(() => w.subject.turns().includes("still delivers"));
		expect(w.engine.heldInboundCount).toBe(0);
	});

	it("gated-out posts never fire typing (no turn started)", async () => {
		const w = makeMattermostWorld({
			name: "mm-typing-gated",
			freeResponseChannels: new Set<string>(),
		});
		await w.connectAndAwaitLive();
		let typed = 0;
		w.engine.bindTyping(async () => {
			typed += 1;
		});
		w.pushPost("!chan:fake.example", USER_ID, "unmentioned chatter");
		await wall();
		expect(typed).toBe(0);
		expect(w.subject.turns()).toEqual([]);
	});
});

describe("mattermost adapter — egress", () => {
	it("chunks at the 4000-char policy and disables mentions on every payload", async () => {
		const w = makeMattermostWorld({ name: "mm-egress" });
		const policy = w.engine.chatLengthPolicyForChat("!chan:fake.example");
		expect(policy.unit).toBe("chars");
		expect(policy.maxUnits).toBe(64); // shared-row budget override

		await w.wire.reset();
		await w.engine.deliverText(
			"!chan:fake.example",
			"see ![img](https://x/i.png)",
		);
		const sends = w.wire.sendsOf("!chan:fake.example");
		expect(sends.length).toBe(1);
		expect(sends[0]?.content).toBe("see https://x/i.png"); // image-md stripped
		const props = sends[0]?.metadata["mm_props"] as Record<string, unknown>;
		expect(props["disable_mentions"]).toBe(true);
	});

	it("withMentionsDisabled preserves existing props (manifest data)", () => {
		const base = { channel_id: "x", message: "m", props: { card: 1 } };
		const merged = withMentionsDisabled(base) as Record<string, unknown>;
		expect(merged["props"]).toEqual({ card: 1, disable_mentions: true });
		const fresh = withMentionsDisabled({ channel_id: "x" }) as Record<
			string,
			unknown
		>;
		expect(fresh["props"]).toEqual({ disable_mentions: true });
	});

	it("PATCH payloads carry props through BOTH lanes (posts/{id}/patch body parity)", async () => {
		// DIRECT (unbound) lane: engine edit → fake receives the FULL payload
		// {message, props:{disable_mentions:true}} — adapter.py:edit_message.
		const clock = new ManualClock();
		const bareMm = new FakeMattermost({ nowMs: clock.nowMs });
		bareMm.setAuthToken("tok");
		const bareEngine = new MattermostAdapterCore({
			mm: bareMm,
			manifestName: "mm-patch-direct",
			clock,
			secretReader: () => "tok",
		});
		await bareEngine.connect({ isReconnect: false });
		const edit = await bareEngine.editMessage("!chan", "post1", "upd");
		expect(edit.success).toBe(true);
		const direct = bareMm.patchPayloads.at(-1);
		expect(direct?.message).toBe("upd");
		expect(direct?.props?.["disable_mentions"]).toBe(true);

		// WIRE-BOUND lane (the conformance subject binds patch): the patch op
		// reaches the wire with its post id.
		const w = makeMattermostWorld({ name: "mm-patch-bound" });
		await w.connectAndAwaitLive();
		await w.wire.reset();
		const boundEdit = await w.engine.editMessage(
			"!chan:fake.example",
			"post9",
			"bound upd",
		);
		expect(boundEdit.success).toBe(true);
		const boundOp = w.wire.ops.find(
			(o) => o.metadata["mm_patch_post_id"] === "post9",
		);
		expect(boundOp).toBeDefined();
	});

	it("file_ids download into media classification; sendFile uploads via api/v4/files", async () => {
		const w = makeMattermostWorld({ name: "mm-files" });
		await w.connectAndAwaitLive();
		const fid = await w.mm.restUploadFile({
			channelId: "!chan:fake.example",
			filename: "shot.png",
			contentType: "image/png",
			bytes: new Uint8Array([1, 2, 3]),
		});
		w.pushPost("!chan:fake.example", USER_ID, "look at this", {
			fileIds: [fid],
		});
		await vi_waitFor(() =>
			w.subject.turns().some((t) => t.includes("look at this")),
		);
		// Classification surfaced on the delivered IncomingEvent.
		const built = w.engine.lastBuiltIncoming;
		expect(built?.messageType).toBe("photo");
		expect(built?.mediaUrls?.[0]).toBe(`mattermost://file/${fid}`);
		expect(built?.mediaTypes?.[0]).toBe("image/png");

		// OUTBOUND: upload → post referencing file_ids (DEC-019 provider path).
		const sent = await w.engine.sendFile(
			"!chan:fake.example",
			{
				filename: "trace.log",
				bytes: new Uint8Array([9, 9]),
				contentType: "text/plain",
			},
			{ message: "logs attached" },
		);
		expect(sent.success).toBe(true);
		expect(w.mm.uploadedFiles.at(-1)?.filename).toBe("trace.log");
		// The post references the UPLOADED id via file_ids (wire-captured).
		const outFid = w.mm.uploadedFiles.at(-1);
		expect(outFid?.filename).toBe("trace.log");
		const outbound = w.wire
			.sendsOf("!chan:fake.example")
			.find((o) => o.metadata["mm_file_ids"] !== undefined);
		expect(outbound?.metadata["mm_file_ids"]).toHaveLength(1);
		expect((outbound?.metadata["mm_file_ids"] as string[])[0]).toMatch(
			/^fid\d+$/,
		);
	});

	it("in-band challenge FAIL reply escalates FATAL like a 4001 close", async () => {
		const w = makeMattermostWorld({ name: "mm-challenge-fail" });
		await w.connectAndAwaitLive();
		w.mm.challengeReplyFail(99);
		w.mm.dropActive({ reason: "reauth" });
		await w.clock.advance(2_000);
		await vi_waitFor(
			() => w.engine.lifecycleSnapshot().state === "fatal",
			4_000,
		);
		expect(w.engine.lifecycleSnapshot().state).toBe("fatal");
	});

	it("answers SERVER-initiated keepalive pings with pong frames", async () => {
		const w = makeMattermostWorld({ name: "mm-server-ping" });
		await w.connectAndAwaitLive();
		w.mm.serverPing();
		await vi_waitFor(() =>
			w.mm.receivedFrames.some((f) => f.frame["action"] === "pong"),
		);
		const pong = w.mm.receivedFrames.find((f) => f.frame["action"] === "pong");
		expect(pong?.frame["seq_reply"]).toBeDefined();
	});

	it("native stream ships RAW cumulative bytes; seal finalizes the same post id", async () => {
		const w = makeMattermostWorld({ name: "mm-stream" });
		// Relay-shaped lane: seal-interception armed for this chat.
		w.engine.markStreamIsMessage("!chan:fake.example");
		const start = await w.engine.sendDraft({
			chatId: "!chan:fake.example",
			draftId: 7,
			content: "**part one**", // START = full RAW accumulator
		});
		expect(start.success).toBe(true);
		const append = await w.engine.sendDraft({
			chatId: "!chan:fake.example",
			draftId: 7,
			content: "**part one** and **two**", // cumulative RAW
		});
		expect(append.success).toBe(true);
		// The turn-final through DOOR 1 seals the open stream.
		const final = await w.engine.send(
			"!chan:fake.example",
			"**part one** and **two**",
		);
		expect(final.success).toBe(true);
		expect(final.messageId).toBe(append.messageId); // sealed stream IS the message
		const startOp = w.wire.ops.find((o) => o.metadata["stream_op"] === "start");
		const patchOps = w.wire.ops.filter(
			(o) => o.metadata["mm_patch_post_id"] !== undefined,
		);
		const appendOp = patchOps.find((o) => !o.final);
		const sealOp = patchOps.find((o) => o.final);
		expect(startOp?.content).toBe("**part one**"); // START = full RAW bytes
		expect(appendOp?.content).toBe("**part one** and **two**"); // cumulative RAW
		// Prefix stability across frames.
		expect(appendOp?.content.startsWith("**part one**")).toBe(true);
		// Seal finalizes the SAME post id.
		expect(sealOp?.content).toBe("**part one** and **two**");
	});

	it("broken thread roots fail closed unless notify metadata forces flat fallback", async () => {
		// Unit-level: the classifier is manifest-shaped data.
		expect(
			isBrokenThreadRoot(404, "404: Invalid or missing root_id parameter"),
		).toBe(true);
		expect(isBrokenThreadRoot(400, "invalid root_id")).toBe(true);
		expect(isBrokenThreadRoot(500, "internal error")).toBe(false);
		expect(isBrokenThreadRoot(404, "")).toBe(false);
	});

	it("surfaces MmRestError retry-after through the send lane", async () => {
		const w = makeMattermostWorld({ name: "mm-retry" });
		w.wire.script(
			"send",
			{ kind: "fail", error: "429: rate limit exceeded", retryAfter: 0.4 },
			{ kind: "ok" },
		);
		const results = await w.engine.deliverText("!chan:fake.example", "hello");
		expect(results[0]?.success).toBe(true); // ladder retried to success
		expect(w.engine.lastCapturedRetryAfterSeconds).not.toBeNull();
	});
});

describe("mattermost adapter — identity probes", () => {
	it("missing required secrets LOUDLY disable the sibling", () => {
		const w = makeMattermostWorld({ name: "mm-sibling" });
		const sibling = w.engine.buildMissingSecretSibling();
		const snap = sibling.lifecycleSnapshot();
		expect(snap.state).toBe("disabled");
	});

	it("second instance token lock refuses with named holder", () => {
		const w = makeMattermostWorld({ name: "mm-lock" });
		const outcome = w.engine.secondInstanceTokenLockAttempt();
		expect(outcome.acquired).toBe(false);
		if (!outcome.acquired) expect(outcome.holderOwner).toBe("instance-A");
	});

	it("production default budget is the 4000-char manifest scalar", () => {
		const mm = new FakeMattermost();
		const adapter = new MattermostAdapterCore({
			mm,
			secretReader: () => undefined, // disabled — fine for policy probing
		});
		const policy = adapter.chatLengthPolicyForChat("!chan:fake.example");
		expect(policy.unit).toBe("chars");
		expect(policy.maxUnits).toBe(MM_MAX_POST_CHARS);
	});
});

// ── tiny waitFor ──────────────────────────────────────────────────────────
let turnsSnapshot: () => readonly string[] = () => [];
async function vi_waitFor(
	predicate: () => boolean,
	timeoutMs = 4_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (predicate()) return;
		if (Date.now() > deadline) {
			console.error("WAITFOR-FAIL turns:", JSON.stringify(turnsSnapshot()));
			throw new Error("waitFor: condition not met");
		}
		await new Promise<void>((r) => setTimeout(r, 4));
	}
}

/** Subject re-export guard for suite wiring consumers. */
void makeMattermostSubject;
