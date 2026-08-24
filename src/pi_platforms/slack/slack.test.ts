// pi_platforms/slack/slack.test.ts — behavior contracts for the Slack census
// port. Every contract runs headless against the Socket-Mode fake server and
// the shared harness wire with the INJECTED clock; no network, no wall-time
// assertions. Required families:
//   1. Socket-Mode envelope shapes + retry-flag dedup × cursor replay
//   2. Block Kit round-trip (build → action → resolve) via THE kit grammar
//   3. mrkdwn conversion byte-exact decision matrix vs RAW-native
//   4. Rate-tier gating BEFORE egress (per method class, injected clock)
//   5. Approvals card-first e2e through the pi_embedded DeliveryBridge
//   plus thread_ts session mapping, block-rejection retries, edit
//   reconciliation, and manifest-data declarations.

import { describe, expect, it } from "vitest";

import { makeSlackWorld, eventually } from "./slack-fixture.js";
import { makeSlackSubject } from "./slack-subject.js";
import { FakePlatformWire } from "../conformance/wire.js";
import { SlackAdapter } from "./slack-adapter.js";
import { SLACK_MANIFEST, SLACK_MAX_MESSAGE_UNITS } from "./manifest.js";
import {
	convertMarkdownToSlackMrkdwn,
	renderGfmTableFenced,
} from "./mrkdwn.js";
import { isBlockPayloadRejectionError } from "./block-rejection.js";
import {
	ApprovalCardLedger,
	DeliveryBridge,
	hasExecApprovalCard,
} from "../../pi_embedded/approvals/delivery.js";
import type { GatewayClock } from "../../pi_embedded/approvals/clock.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { SlackSocketModeServer } from "./fake-socket-mode.js";

const bridgeClock: GatewayClock = {
	nowSeconds: () => 1_000,
	sleepMs: async () => {},
};

describe("slack socket-mode envelope shapes", () => {
	it("cold boot subscribes with a null resume cursor; envelopes carry ids/ts/retryAttempt=0", async () => {
		const world = makeSlackWorld({ name: "slack-envelope" });
		const { engine, socketServer } = world;
		await world.connectAndAwaitLive();

		const firstSubscribe = socketServer
			.getFramesReceived()
			.find((f) => f.frame["type"] === "subscribe");
		expect(firstSubscribe?.frame["cursor"]).toBeNull();

		const env = world.socketServer.pushMessage({
			channel: "C1",
			user: "user-1",
			text: "hello",
		});
		world.pushMessage("C1", "second");
		await eventually(() => engine.cursor.value === "e2");
		expect(env.envelopeId).toMatch(/^emv\d+$/);
		expect(env.retryAttempt).toBe(0);
		expect(env.ts).toMatch(/^\d+\.\d+$/);

		// The cursor IS the ack point: advanced past every processed event.
		expect(engine.cursor.value).toBe("e2");
	});

	it("interactivity payloads route through the ONE handler at the socket seam; every payload acks", async () => {
		const world = makeSlackWorld({ name: "slack-interactivity" });
		const { subject, engine } = world;
		await world.connectAndAwaitLive();
		const sent = await subject.adapter.sendClarifyCard({
			chatId: "C-int",
			question: "Pick one",
			choices: ["alpha", "beta"],
			clarifyId: 3,
			sessionKey: "slack:C-int:t1",
		});
		expect(sent.success).toBe(true);

		world.pushInteractive({
			type: "block_actions",
			user: { id: "user-1", name: "U1" },
			channel: { id: "C-int" },
			message: { ts: sent.messageId ?? "", blocks: [] },
			actions: [{ action_id: "hermes_clarify_choice_0", value: "3|0" }],
		});
		await eventually(() => engine.interactiveAudit.length === 1);
		expect(engine.resolvedFamilies.includes("cl")).toBe(true);
		expect(engine.interactiveAudit[0]?.acked).toBe(true);
		// Ack inside the 3-second window — trivially true under the injected
		// clock but the CONTRACT is that the audit records the window.
		expect(engine.interactiveAudit[0]?.ackedWithinMs).toBeLessThanOrEqual(3000);
	});
});

describe("retry-flag dedup × cursor replay (#4777)", () => {
	it("redeliveries carry retry flags; dedup suppresses overlap exactly once; replay covers the disconnect gap", async () => {
		const world = makeSlackWorld({ name: "slack-replay-dedup" });
		const { engine, socketServer, clock, subject } = world;
		await world.connectAndAwaitLive();

		for (const text of ["r1", "r2", "r3"]) world.pushMessage("C1", text);
		await eventually(() => engine.cursor.value === "e3");

		socketServer.dropActive({ reason: "blip" });
		world.pushMessage("C1", "r4"); // buffered during the outage
		world.pushMessage("C1", "r5");
		// Slack also replays UN-ACKED events whose delivery raced the drop.
		expect(socketServer.markUnackedForRedelivery("e3")).toBe(true);

		await clock.advance(5_000); // ladder sleep → resubscribe w/ cursor e3
		await eventually(() => engine.isLive && engine.cursor.value === "e5");

		// Exactly-once downstream despite the flagged overlap redelivery.
		expect([...subject.turns()].sort()).toEqual(["r1", "r2", "r3", "r4", "r5"]);
		expect(engine.dedupSuppressedCount).toBe(1);
		// The resubscribe carried the ACK POINT as its resume cursor.
		const subscribes = socketServer
			.getFramesReceived()
			.filter((f) => f.frame["type"] === "subscribe");
		const last = subscribes[subscribes.length - 1]?.frame;
		expect(last?.["cursor"]).toBe("e3");
		// Redelivered envelopes were flagged with retry metadata.
		expect(
			engine.redeliveryLog.some((r) => r.id === "e3" && r.retryAttempt >= 1),
		).toBe(true);
		expect(engine.cursor.value).toBe("e5");
	});

	it("a dispatch failure leaves the cursor unmoved so the replay window still covers it", async () => {
		const world = makeSlackWorld({ name: "slack-dispatch-contain" });
		const { engine } = world;
		await world.connectAndAwaitLive();

		world.pushMessage("C1", "processed");
		await eventually(() => engine.cursor.value === "e1");

		// Adapter-level dispatch seam fails (disabled lifecycle throws inside
		// handleIngress — BEFORE any guard work): contained, cursor unmoved.
		engine.lifecycle.disable({
			kind: "secret_missing",
			secretKey: "SLACK_BOT_TOKEN",
			manifestName: "slack-dispatch-contain",
		});
		world.pushMessage("C1", "doomed");
		await new Promise<void>((r) => setTimeout(r, 12));
		expect(engine.cursor.value).toBe("e1"); // NOT advanced past e1
		expect(engine.inboundLog.length).toBe(1);
	});
});

describe("thread_ts → session threading", () => {
	it("top-level messages synthesize their ts root; replies key under thread_ts; same thread shares a session", async () => {
		const world = makeSlackWorld({ name: "slack-threads" });
		const { engine, socketServer } = world;
		await world.connectAndAwaitLive();

		socketServer.pushMessage({
			channel: "C1",
			user: "user-1",
			text: "top-level",
			ts: "1700000000.000001",
		});
		socketServer.pushMessage({
			channel: "C1",
			user: "user-1",
			text: "reply A",
			thread_ts: "1700000000.000001",
		});
		socketServer.pushMessage({
			channel: "C1",
			user: "user-1",
			text: "reply B",
			thread_ts: "1700000000.000001",
		});
		socketServer.pushMessage({
			channel: "C1",
			user: "user-1",
			text: "other root",
			ts: "1700000000.000900",
		});
		await eventually(() => engine.sessionKeysSeen.length === 4);

		const keys = engine.sessionKeysSeen;
		expect(keys[0]).toBe("slack-threads:C1:1700000000.000001");
		expect(keys[1]).toBe("slack-threads:C1:1700000000.000001");
		expect(keys[2]).toBe(keys[1]); // same thread ⇒ same session key
		expect(keys[3]).toBe("slack-threads:C1:1700000000.000900");

		socketServer.pushMessage({
			channel: "D123",
			user: "user-1",
			text: "dm words",
			ts: "1700000000.000950",
		});
		await eventually(() => engine.sessionKeysSeen.length === 5);
		expect(engine.sessionKeysSeen[4]).toContain(":D123:");
	});
});

describe("Block Kit round-trip via THE kit grammar (build → action → resolve)", () => {
	it("clarify card: builder → wire (blocks + mrkdwn fallback) → tap → resolver → host edited, buttons stripped; double-tap answers once", async () => {
		const world = makeSlackWorld({ name: "slack-cl-roundtrip" });
		const { subject, engine } = world;
		await world.connectAndAwaitLive();
		const sent = await engine.sendClarifyCard({
			chatId: "C-cl",
			question: "Deploy now?",
			choices: ["yes", "no"],
			clarifyId: 42,
			sessionKey: "slack:C-cl:t1",
		});
		expect(sent.success).toBe(true);
		const cardOp = subject.wire.sendsOf("C-cl")[0];
		expect(Array.isArray(cardOp?.metadata["blocks"])).toBe(true);
		// The accessible mrkdwn fallback ALWAYS ships alongside.
		expect(cardOp?.content.startsWith("❓ Deploy now?")).toBe(true);

		const tap = () =>
			world.pushInteractive({
				type: "block_actions",
				user: { id: "user-1", name: "U" },
				channel: { id: "C-cl" },
				message: { ts: sent.messageId ?? "", blocks: [] },
				actions: [{ action_id: "hermes_clarify_choice_1", value: "42|1" }],
			});
		tap();
		await eventually(() => subject.wire.editsOf("C-cl").length === 1);
		expect(engine.resolvedFamilies.includes("cl")).toBe(true);
		const edits = subject.wire.editsOf("C-cl");
		expect(edits[0]?.metadata["buttons_removed"]).toBe(true);

		tap(); // double-tap: answered, never re-resolved
		await eventually(() => engine.interactiveAudit.length === 2);
		expect(subject.wire.editsOf("C-cl").length).toBe(1); // still ONE host edit
		expect(engine.routerAuditResolved().length).toBe(1);
	});

	it("unauthorized clicker answered ⛔ without resolution or host edit", async () => {
		const world = makeSlackWorld({ name: "slack-unauth" });
		const { subject, engine } = world;
		await world.connectAndAwaitLive();
		engine.setClickerAuthorization(false);
		const sent = await engine.sendClarifyCard({
			chatId: "C-u",
			question: "q?",
			choices: ["a"],
			clarifyId: 9,
			sessionKey: "slack:C-u:t1",
		});
		world.pushInteractive({
			type: "block_actions",
			user: { id: "stranger", name: "S" },
			channel: { id: "C-u" },
			message: { ts: sent.messageId ?? "", blocks: [] },
			actions: [{ action_id: "hermes_clarify_choice_0", value: "9|0" }],
		});
		await eventually(() => engine.interactiveAudit.length === 1);
		expect(engine.resolvedFamilies.includes("cl")).toBe(false);
		expect(subject.wire.editsOf("C-u").length).toBe(0);
	});

	it("'Other…' flips the clarify entry to free-text capture (host prompt rewritten, keyboard stripped)", async () => {
		const world = makeSlackWorld({ name: "slack-cl-other" });
		const { subject, engine } = world;
		await world.connectAndAwaitLive();
		const sent = await engine.sendClarifyCard({
			chatId: "C-o",
			question: "Which env?",
			choices: ["prod", "staging"],
			clarifyId: 11,
			sessionKey: "slack:C-o:t1",
		});
		world.pushInteractive({
			type: "block_actions",
			user: { id: "user-1", name: "U" },
			channel: { id: "C-o" },
			message: { ts: sent.messageId ?? "", blocks: [] },
			actions: [{ action_id: "hermes_clarify_other", value: "11|other" }],
		});
		await eventually(() => engine.interactiveAudit.length === 1);
		const edits = subject.wire.editsOf("C-o");
		expect(edits[0]?.content).toContain("Awaiting typed response");
		expect(edits[0]?.metadata["buttons_removed"]).toBe(true);
	});
});

describe("approvals card-first e2e (pi_embedded DeliveryBridge seam)", () => {
	it("bridge picks CARD-FIRST over the real adapter; tap resolves through ea grammar; ambiguous sends never re-ask", async () => {
		const world = makeSlackWorld({ name: "slack-card-first" });
		const { subject, engine } = world;
		await world.connectAndAwaitLive();
		// Class-level probe parity: only a PROTOTYPE method enables cards.
		expect(hasExecApprovalCard(world.engine)).toBe(true);

		const ledger = new ApprovalCardLedger();
		const bridge = new DeliveryBridge({
			// THE REAL ADAPTER — hasExecApprovalCard walks PROTOTYPES; a
			// structural copy would silently disable the card path.
			target: engine as unknown as ConstructorParameters<typeof DeliveryBridge>[0]["target"],
			chatId: "C-appr",
			ledger,
			clock: bridgeClock,
		});
		await bridge.notify({
			sessionKey: "slack:C-appr:t9",
			command: "rm -rf /tmp/build-cache",
			description: "clean build cache",
			allowPermanent: true,
			allowSession: true,
			smartDenied: false,
		});

		// Card-first: ONE send carrying blocks + fallback text.
		const sends = subject.wire.sendsOf("C-appr");
		expect(sends.length).toBe(1);
		const cardOp = sends[0];
		expect(cardOp?.content.startsWith("⚠️ Command approval required:")).toBe(
			true,
		);
		const blocks = cardOp?.metadata["blocks"] as
			| Array<{ type: string }>
			| undefined;
		expect(blocks?.some((b) => b.type === "section")).toBe(true);
		expect(blocks?.some((b) => b.type === "actions")).toBe(true);
		// Ledger bound AFTER delivery succeeded (adapter parity).
		expect(ledger.size).toBe(1);
		const approvalId = 1;
		expect(ledger.peek(approvalId)).toBe("slack:C-appr:t9");

		// Tap Allow Once on the rendered card.
		world.pushInteractive({
			type: "block_actions",
			user: { id: "user-1", name: "Owner" },
			channel: { id: "C-appr" },
			message: { ts: cardOp?.messageId ?? "", blocks: [] },
			actions: [
				{ action_id: "hermes_approve_once", value: String(approvalId) },
			],
		});
		await eventually(() => subject.wire.editsOf("C-appr").length === 1);
		expect(engine.resolvedFamilies.includes("ea")).toBe(true);
		const edits = subject.wire.editsOf("C-appr");
		expect(edits[0]?.metadata["buttons_removed"]).toBe(true);
		// The ADAPTER's pending entry was popped by the click (one-shot);
		// the BRIDGE's ledger keeps its bind for late-tap accounting.
		expect(engine.approvals.has(approvalId)).toBe(false);
		expect(ledger.peek(approvalId)).toBe("slack:C-appr:t9");

		// Double-tap past resolution: answered, nothing re-resolves.
		world.pushInteractive({
			type: "block_actions",
			user: { id: "user-1", name: "Owner" },
			channel: { id: "C-appr" },
			message: { ts: cardOp?.messageId ?? "", blocks: [] },
			actions: [
				{ action_id: "hermes_approve_once", value: String(approvalId) },
			],
		});
		await eventually(() => engine.interactiveAudit.length === 2);
		expect(engine.routerAuditResolved().length).toBe(1);
		expect(subject.wire.editsOf("C-appr").length).toBe(1);
	});

	it("a block-payload rejection INSIDE the card send drops blocks and keeps the card path", async () => {
		const world = makeSlackWorld({ name: "slack-card-fallback" });
		const { subject, engine, wire } = world;
		await world.connectAndAwaitLive();
		wire.script("send", { kind: "fail", error: "invalid_blocks" });
		const ledger = new ApprovalCardLedger();
		const bridge = new DeliveryBridge({
			target: engine as unknown as ConstructorParameters<typeof DeliveryBridge>[0]["target"],
			chatId: "C-fb",
			ledger,
			clock: bridgeClock,
		});
		await bridge.notify({
			sessionKey: "slack:C-fb:t1",
			command: "deploy.sh",
			description: "deploy",
			allowPermanent: false,
			allowSession: true,
			smartDenied: false,
		});
		// invalid_blocks is block-payload-recoverable INSIDE the adapter's REST
		// boundary (retry dropped blocks), so the FIRST send still succeeds and
		// the bridge stays on the card path — ledger armed, zero text fallback.
		const sends = subject.wire.sendsOf("C-fb");
		expect(sends.length).toBe(2);
		expect(Array.isArray(sends[0]?.metadata["blocks"])).toBe(true);
		expect(sends[1]?.metadata["blocks_dropped_on_retry"]).toBe(true);
		expect(sends[1]?.metadata["blocks"]).toBeUndefined();
	});
});

describe("rate-tier gating before egress (Q17, injected clock)", () => {
	it("tier budget consumed per method class; exhausted tier refuses BEFORE the wire; window rotation admits again", async () => {
		const world = makeSlackWorld({ name: "slack-rate" });
		const { engine, wire, clock } = world;

		// Fill the Tier-2 messaging class (manifest: send, 20/min).
		for (let i = 0; i < 20; i++) {
			const r = await engine.deliverText("C-rate", `m${i}`);
			expect(r[r.length - 1]?.success).toBe(true);
		}
		expect(wire.sendsOf("C-rate").length).toBe(20);
		expect(engine.gateSnapshot("send")).toBe(20);

		// 21st refused WITHOUT touching the wire; horizon reported as data.
		const refused = await engine.deliverText("C-rate", "over-budget");
		expect(refused[refused.length - 1]?.success).toBe(false);
		expect(refused[refused.length - 1]?.error).toBe(
			"rate_limited:tier2-messaging",
		);
		expect(wire.sendsOf("C-rate").length).toBe(20); // NOTHING hit the wire
		expect(refused[refused.length - 1]?.retryAfter).toBeGreaterThanOrEqual(1);

		// The STREAMING class is an INDEPENDENT budget — still admitted.
		const draft = await engine.sendDraft({
			chatId: "C-rate-stream",
			draftId: 1,
			content: "stream bytes",
		});
		expect(draft.success).toBe(true);

		// Rotate the window under the injected clock — messaging admits again.
		await clock.advance(60_000);
		const after = await engine.deliverText("C-rate", "fresh-window");
		expect(after[after.length - 1]?.success).toBe(true);
		expect(wire.sendsOf("C-rate").length).toBe(21);
	});

	it("streaming tier exhaustion refuses drafts/edits while messaging stays independent", async () => {
		const world = makeSlackWorld({ name: "slack-rate-streaming" });
		const { engine, wire, clock } = world;
		// Each NEW draft_id seals the dangling stream (draft-stop) and starts
		// a new stream (draft-start): TWO Tier-2 tokens per segment ⇒ the
		// 20-token budget fills after 10 segments.
		for (let i = 0; i < 10; i++) {
			const r = await engine.sendDraft({
				chatId: "C-s",
				draftId: i + 1,
				content: `seg${i}`,
			});
			expect(r.success).toBe(true);
		}
		expect(engine.nativeStreamLatch.unsupported).toBe(false);
		const refusedDraft = await engine.sendDraft({
			chatId: "C-s",
			draftId: 99,
			content: "nope",
		});
		expect(refusedDraft.success).toBe(false);
		expect(refusedDraft.error).toBe("rate_limited:tier2-streaming");
		expect(wire.ops.filter((o) => o.op === "draft").length).toBe(10);
		expect(wire.ops.filter((o) => o.op === "seal").length).toBe(10);

		// Messaging class untouched by streaming consumption.
		const send = await engine.deliverText("C-s", "still fine");
		expect(send[send.length - 1]?.success).toBe(true);

		await clock.advance(60_000);
		const recovered = await engine.sendDraft({
			chatId: "C-s",
			draftId: 100,
			content: "again",
		});
		expect(recovered.success).toBe(true);
	});
});

describe("mrkdwn conversion decision matrix (byte-exact) vs RAW native path", () => {
	const cases: Array<[string, string, string]> = [
		["bold", "**bold**", "*bold*"],
		["link", "[text](https://x.y)", "<https://x.y|text>"],
		["header", "## Title", "*Title*"],
		["header strips bold", "### **Bold Head**", "*Bold Head*"],
		["bold-italic", "***both***", "*_both_*"],
		["strikethrough", "~~gone~~", "~gone~"],
		["closing-char ZWSP quirk", "**(done)**", "*(done)\u200b*"],
		["entity escape", "<script>", "&lt;script&gt;"],
		["entity stability", "&amp;lt;", "&amp;lt;"],
		[
			"broadcast mention neutralized",
			"<!channel> ping",
			"&lt;!channel&gt; ping",
		],
		[
			"fence tag dropped at line start",
			"```python\nprint(1)\n```",
			"```\nprint(1)\n```",
		],
		["inline code protected", "`code **not** bold`", "`code **not** bold`"],
		["existing entity protected", "<@U123> hi", "<@U123> hi"],
		["blockquote marker preserved", "> quoted line", "> quoted line"],
		["literal asterisks preserved", "a * b * c", "a * b * c"],
	];
	for (const [name, input, expected] of cases) {
		it(`${name}: ${JSON.stringify(input)} → ${JSON.stringify(expected)}`, () => {
			expect(convertMarkdownToSlackMrkdwn(input)).toBe(expected);
		});
	}

	it("GFM table renders as aligned fenced monospace with CJK width awareness", () => {
		expect(
			convertMarkdownToSlackMrkdwn("| a | b |\n|---|---|\n| 1 | 2 |"),
		).toBe("```\na  b\n-  -\n1  2\n```");
		expect(
			renderGfmTableFenced("| 名前 | x |\n|---|---|\n| 太郎 | y |")?.fenced,
		).toBe("```\n名前  x\n----  -\n太郎  y\n```");
	});

	it("mid-line fence spans keep their language tag byte-exactly", () => {
		const inline = "run ```js\nvar x=1;\n``` now";
		expect(convertMarkdownToSlackMrkdwn(inline)).toBe(inline);
	});
});

describe("block-rejection retry drops blocks, never content", () => {
	it("invalid_blocks → retried WITHOUT blocks (flagged); non-block failures take the §6.1 lanes instead", async () => {
		const world = makeSlackWorld({
			name: "slack-block-reject",
			richBlocks: true,
		});
		const { engine, wire } = world;

		wire.script(
			"send",
			{ kind: "fail", error: "invalid_blocks" },
			{ kind: "ok" },
		);
		const results = await engine.deliverText("C-br", "para one\n\npara two");
		expect(results[results.length - 1]?.success).toBe(true);
		const sends = wire.sendsOf("C-br");
		expect(sends.length).toBe(2);
		expect(Array.isArray(sends[0]?.metadata["blocks"])).toBe(true);
		expect(sends[1]?.metadata["blocks_dropped_on_retry"]).toBe(true);
		expect(sends[1]?.metadata["blocks"]).toBeUndefined();
		expect(isBlockPayloadRejectionError("msg_too_long here")).toBe(true);
		expect(isBlockPayloadRejectionError("too_many_blocks")).toBe(true);
		expect(isBlockPayloadRejectionError("socket hang up")).toBe(false);

		// Non-block failure: NO drop-retry flag anywhere; notice delivered.
		wire.script(
			"send",
			{ kind: "fail", error: "socket hang up" },
			{ kind: "ok" },
		);
		const retried = await engine.deliverText("C-br2", "payload");
		expect(retried[retried.length - 1]?.success).toBe(true);
		const all = wire.sendsOf("C-br2");
		expect(
			all.every((o) => o.metadata["blocks_dropped_on_retry"] === undefined),
		).toBe(true);
	});
});

describe("edit reconciliation (chat.update parity)", () => {
	it("REST edits convert; finalize attaches blocks; transient transport keeps the message id retryable", async () => {
		const world = makeSlackWorld({ name: "slack-edits", richBlocks: true });
		const { engine, wire } = world;

		const plain = await engine.editMessage("C-e", "wire-1", "**upd**");
		expect(plain.success).toBe(true);
		const plainOp = wire.editsOf("C-e")[0];
		expect(plainOp?.content).toBe("*upd*");
		expect(plainOp?.metadata["blocks"]).toBeUndefined();

		const final = await engine.editMessage(
			"C-e",
			"wire-1",
			"para A\n\npara B",
			{
				finalize: true,
			},
		);
		expect(final.success).toBe(true);
		const finalOp = wire.editsOf("C-e")[1];
		expect(finalOp?.content).toBe("para A\n\npara B");
		expect(Array.isArray(finalOp?.metadata["blocks"])).toBe(true);

		wire.script("edit", { kind: "fail", error: "connection reset by peer" });
		const transient = await engine.editMessage("C-e", "wire-1", "catch up");
		expect(transient.success).toBe(false);
		expect(transient.retryable).toBe(true);
	});
});

describe("manifest data declarations (Q17 / plugin.yaml transcription)", () => {
	it("required env, capability flags, rate tiers, and the 39k cap are MANIFEST DATA", async () => {
		expect(SLACK_MANIFEST.requiresEnv.map((e) => e.name)).toEqual([
			"SLACK_BOT_TOKEN",
			"SLACK_APP_TOKEN",
		]);
		expect(SLACK_MANifest_capabilities()).toMatchObject({
			splitsLongMessages: true,
			typedCommandPrefix: "!",
			supportsAsyncDelivery: true,
		});
		expect(SLACK_MANIFEST.rateBudget?.tiers.map((t) => t.name)).toEqual([
			"tier2-messaging",
			"tier2-streaming",
		]);
		for (const tier of SLACK_MANIFEST.rateBudget?.tiers ?? []) {
			expect(tier.limit).toBe(20);
			expect(tier.windowSeconds).toBe(60);
		}
		expect(SLACK_MAX_MESSAGE_UNITS).toBe(39_000);

		// The REAL cap resolves through THE one chat-length pair when wired.
		const clock = new ManualClock();
		const server = new SlackSocketModeServer({ nowMs: clock.nowMs });
		const wire = new FakePlatformWire();
		const subject = makeSlackSubject({
			name: "slack-cap",
			wire,
			ws: server,
			clock,
		});
		expect(subject.chatPolicyFor("C-anywhere").maxUnits).toBe(64); // harness budget
		const realCap = new SlackAdapter({
			manifestName: "slack-realcap",
			transport: server,
			rest: {
				transmitSend: async () => ({ success: true }),
				transmitEdit: async () => ({ success: true }),
				transmitDraft: async () => ({ success: true }),
				transmitRich: async () => ({ success: true }),
				hasScript: () => false,
			},
			clock,
			scalarMaxUnits: SLACK_MAX_MESSAGE_UNITS,
			requiresEnv: SLACK_MANIFEST.requiresEnv,
			secretReader: () => undefined,
		});
		void realCap;
		const capPolicy = new SlackAdapter({
			manifestName: "slack-realcap2",
			transport: server,
			rest: {
				transmitSend: async () => ({ success: true }),
				transmitEdit: async () => ({ success: true }),
				transmitDraft: async () => ({ success: true }),
				transmitRich: async () => ({ success: true }),
				hasScript: () => false,
			},
			clock,
			scalarMaxUnits: SLACK_MAX_MESSAGE_UNITS,
		}).chatLengthPolicyForChat("C1");
		expect(capPolicy.maxUnits).toBe(39_000);
	});

	it("missing APP token disables LOUDLY naming SLACK_APP_TOKEN", async () => {
		const clock = new ManualClock();
		const server = new SlackSocketModeServer({ nowMs: clock.nowMs });
		const adapter = new SlackAdapter({
			manifestName: "slack-half-secret",
			transport: server,
			rest: {
				transmitSend: async () => ({ success: true }),
				transmitEdit: async () => ({ success: true }),
				transmitDraft: async () => ({ success: true }),
				transmitRich: async () => ({ success: true }),
				hasScript: () => false,
			},
			clock,
			requiresEnv: [
				{
					name: "SLACK_BOT_TOKEN",
					description: "bot token",
					password: true,
				},
				{
					name: "SLACK_APP_TOKEN",
					description: "app token",
					password: true,
				},
			],
			secretReader: (key) =>
				key === "SLACK_BOT_TOKEN" ? "xoxb-present" : undefined,
		});
		const snap = adapter.lifecycle.statusSnapshot();
		expect(snap.state).toBe("disabled");
		expect((snap.detail ?? "").includes("SLACK_APP_TOKEN")).toBe(true);
	});
});

function SLACK_MANifest_capabilities() {
	return SLACK_MANIFEST.capabilities;
}

/** Structural DeliveryTarget slice over the real adapter (exact-optional
 * narrowing of SendResult → ApprovalSendResult for the bridge seam). */
function asDeliveryTarget(engine: SlackAdapter): {
	typedCommandPrefix: string;
	send(
		chatId: string,
		text: string,
		metadata?: unknown,
	): Promise<{
		success: boolean;
		messageId?: string | null;
		error?: string | null;
	}>;
	sendExecApproval(
		args: Parameters<SlackAdapter["sendExecApproval"]>[0],
	): Promise<{
		success: boolean;
		messageId?: string | null;
		error?: string | null;
	}>;
} {
	return {
		typedCommandPrefix: engine.typedCommandPrefix,
		send: async (chatId, text, metadata) => {
			const r = await engine.send(
				chatId,
				text,
				undefined,
				(metadata ?? {}) as never,
			);
			return {
				success: r.success,
				messageId: r.messageId ?? null,
				error: r.error ?? null,
			};
		},
		sendExecApproval: async (args) => {
			const r = await engine.sendExecApproval(args);
			return {
				success: r.success,
				messageId: r.messageId ?? null,
				error: r.error ?? null,
			};
		},
	};
}
