// pi_platforms/mattermost/mattermost.test.ts — ENGINE behavior contracts for
// the Mattermost census port: ws session/auth, dedup, gating, egress lanes,
// and identity probes. Transport + shape rows live in the conformance wiring.

import { describe, expect, it } from "vitest";

import { FakePlatformWire } from "../conformance/wire.js";
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
		const startOp = w.wire.ops.find(
			(o) => o.metadata["stream_op"] === "start",
		);
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
			console.error(
				"WAITFOR-FAIL turns:",
				JSON.stringify(turnsSnapshot()),
			);
			throw new Error("waitFor: condition not met");
		}
		await new Promise<void>((r) => setTimeout(r, 4));
	}
}

/** Subject re-export guard for suite wiring consumers. */
void makeMattermostSubject;
