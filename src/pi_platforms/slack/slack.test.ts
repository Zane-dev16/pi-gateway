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
import { SlackSocketModeServer, type SlackEnvelopeEvent } from "./fake-socket-mode.js";
import {
	isSlackReactionsEnabled,
	SLACK_PROCESSED_MESSAGE_TS_MAX,
} from "./slack-adapter.js";
import type { SlackCapturingWire } from "./slack-subject.js";

const bridgeClock: GatewayClock = {
	nowSeconds: () => 1_000,
	sleepMs: async () => {},
};

describe("slack socket-mode envelope shapes", () => {
	it("cold boot subscribes with a null resume cursor; EVERY envelope acked by its envelope_id", async () => {
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

		// The cursor IS the durable ack point: advanced past every event.
		expect(engine.cursor.value).toBe("e2");
		// Socket-Mode wire truth: EVERY delivered envelope was answered with its
		// own {type:"ack",envelope_id} frame (adapter.py slack_bolt parity).
		const ackIds = socketServer
			.getFramesReceived()
			.filter((f) => f.frame["type"] === "ack")
			.map((f) => f.frame["envelope_id"]);
		expect(ackIds).toContain(env.envelopeId);
		expect(ackIds.length).toBeGreaterThanOrEqual(2);
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

describe("egress thread targeting + status + media (adapter.py send parity)", () => {
	it("reply/thread metadata resolves to a vendor thread_ts on sends and stream START args", async () => {
		const world = makeSlackWorld({ name: "slack-thread-egress" });
		const { engine, wire } = world;
		await engine.deliverText("C-t", "reply", {
			thread_id: "1700000000.000042",
		});
		await engine.deliverText("C-t", "reply2", {
			reply_to_message_id: "1700000000.000099",
		});
		const sends = wire.sendsOf("C-t");
		expect(sends[0]?.metadata["thread_ts"]).toBe("1700000000.000042");
		expect(sends[0]?.metadata["thread_id"]).toBeUndefined();
		expect(sends[1]?.metadata["thread_ts"]).toBe("1700000000.000099");
		// The chokepoint's internal reply stamp never reaches the wire —
		// chat.postMessage carries ONLY thread_ts.
		expect(sends[1]?.metadata["reply_to_message_id"]).toBeUndefined();

		// chat.startStream REQUIRES a thread target: START args carry it.
		await engine
			.sendDraft({
				chatId: "C-t",
				draftId: 7,
				content: "stream head",
				metadata: { thread_id: "1700000000.000777" } as never,
			})
			.catch(() => undefined);
		const startOp = wire
			.draftsOf("C-t")
			.find((d) => d.metadata["stream_op"] === "start");
		if (startOp !== undefined) {
			expect(startOp.metadata["thread_ts"]).toBe("1700000000.000777");
		}
	});

	it("turn start sets assistant.threads.setStatus and finalize clears it; bare channels never activate", async () => {
		const world = makeSlackWorld({ name: "slack-status" });
		const { engine, socketServer } = world;
		const capturing =
			world.wire as unknown as import("./slack-subject.js").SlackCapturingWire;
		await world.connectAndAwaitLive();

		socketServer.pushMessage({
			channel: "C1",
			user: "user-1",
			text: "work on it",
			ts: "1700000000.000500",
		});
		await eventually(() => capturing.statusOps.length >= 2);
		// SET at turn start (is thinking...) then CLEARED at finalize ("").
		expect(capturing.statusOps[0]).toEqual({
			channelId: "C1",
			threadTs: "1700000000.000500",
			status: "is thinking...",
		});
		expect(capturing.statusOps[capturing.statusOps.length - 1]).toEqual({
			channelId: "C1",
			threadTs: "1700000000.000500",
			status: "",
		});
		// Direct public surface mirrors Hermes' send_typing/stop_typing.
		await engine.sendTyping("C1", { thread_id: "1700000000.000600" });
		await engine.stopTyping("C1", { thread_id: "1700000000.000600" });
		expect(capturing.statusOps.at(-2)?.status).toBe("is thinking...");
		expect(capturing.statusOps.at(-1)?.status).toBe("");
		// No thread root ⇒ NO call (bare channel sends stay inert).
		const before = capturing.statusOps.length;
		await engine.sendTyping("C-nothread");
		expect(capturing.statusOps.length).toBe(before);
	});

	it("files_upload_v2-shaped upload with conversations.open DM resolution ahead of send/upload", async () => {
		const world = makeSlackWorld({ name: "slack-upload" });
		const { engine, wire } = world;
		const capturing =
			world.wire as unknown as import("./slack-subject.js").SlackCapturingWire;

		const result = await engine.deliverFile(
			"U999",
			{
				filename: "trace.log",
			},
			{ caption: "here you go", replyTo: "1700000000.001111" },
		);
		expect(result.success).toBe(true);
		expect(capturing.dmOpens[0]?.userId).toBe("U999");
		expect(capturing.uploadOps[0]).toMatchObject({
			channel: "D999", // resolved D conversation, not the raw U id
			filename: "trace.log",
			initialComment: "here you go",
			threadTs: "1700000000.001111",
		});

		// Channel uploads skip conversations.open entirely; no thread target.
		await engine.deliverFile("C-chan", { filename: "shot.png" });
		expect(capturing.dmOpens).toHaveLength(1);
		expect(capturing.uploadOps[1]?.channel).toBe("C-chan");
		expect(capturing.uploadOps[1]?.threadTs).toBeUndefined();

		// Plain sends to U/W targets resolve through conversations.open FIRST
		// (adapter.py:_ensure_dm_conversation ahead of chat.postMessage).
		await engine.deliverText("W888", "dm body");
		expect(capturing.dmOpens.map((o) => o.userId)).toContain("W888");
		expect(wire.sendsOf("D888").at(-1)?.content).toBe("dm body");
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
		// chat.update REPLACES the layout: section(original) + context(decision)
		// — that replacement is HOW buttons disappear (no invented flag).
		const resolvedBlocks = edits[0]?.metadata["blocks"] as Array<{
			type: string;
		}>;
		expect(resolvedBlocks.some((b) => b.type === "section")).toBe(true);
		expect(resolvedBlocks.some((b) => b.type === "context")).toBe(true);
		expect(resolvedBlocks.every((b) => b.type !== "actions")).toBe(true);

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
		const otherBlocks = edits[0]?.metadata["blocks"] as Array<{
			type: string;
		}>;
		expect(otherBlocks.every((b) => b.type !== "actions")).toBe(true);
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
			target: engine as unknown as ConstructorParameters<
				typeof DeliveryBridge
			>[0]["target"],
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
		const apprBlocks = edits[0]?.metadata["blocks"] as Array<{
			type: string;
		}>;
		expect(apprBlocks.some((b) => b.type === "section")).toBe(true);
		expect(apprBlocks.every((b) => b.type !== "actions")).toBe(true);
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
			target: engine as unknown as ConstructorParameters<
				typeof DeliveryBridge
			>[0]["target"],
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
		// The retry ships WITHOUT blocks; the drop is LOCAL audit state only —
		// no invented flag rides the wire.
		expect(sends[1]?.metadata["blocks"]).toBeUndefined();
		expect(engine.blockRetryAudit.droppedOnRetries).toBe(1);
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
			metadata: { thread_id: "1700000000.000100" } as never,
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
				metadata: { thread_id: "1700000000.000200" } as never,
			});
			expect(r.success).toBe(true);
		}
		expect(engine.nativeStreamLatch.unsupported).toBe(false);
		const refusedDraft = await engine.sendDraft({
			chatId: "C-s",
			draftId: 99,
			content: "nope",
			metadata: { thread_id: "1700000000.000200" } as never,
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
			metadata: { thread_id: "1700000000.000200" } as never,
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
		// Retry WITHOUT blocks; drop recorded locally, never as a wire key.
		expect(sends[1]?.metadata["blocks"]).toBeUndefined();
		expect(engine.blockRetryAudit.droppedOnRetries).toBe(1);
		expect(isBlockPayloadRejectionError("msg_too_long here")).toBe(true);
		expect(isBlockPayloadRejectionError("too_many_blocks")).toBe(true);
		expect(isBlockPayloadRejectionError("socket hang up")).toBe(false);

		// Non-block failure: NO drop-retry anywhere; notice delivered.
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
		expect(engine.blockRetryAudit.droppedOnRetries).toBe(1);
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

describe("per-turn emoji lifecycle (ws-6: reactions.add/remove, SLACK_REACTIONS)", () => {
	it("👀 lands on the triggering message at processing start; removed + white_check_mark at completion", async () => {
		const world = makeSlackWorld({ name: "slack-react-success" });
		const { engine, socketServer } = world;
		const capturing = world.wire as unknown as SlackCapturingWire;
		await world.connectAndAwaitLive();

		world.pushMessage("C1", "do the thing");
		await eventually(() => engine.cursor.value === "e1");

		const ops = capturing.reactionOps;
		// 👀 at start → remove 👀 THEN the final mark (:4280-4284).
		expect(ops.map((o) => `${o.action}:${o.name}`)).toEqual([
			"add:eyes",
			"remove:eyes",
			"add:white_check_mark",
		]);
		expect(new Set(ops.map((o) => o.channelId))).toEqual(new Set(["C1"]));
		expect(ops[0]!.ts).toMatch(/^\d+\.\d+$/); // the TRIGGERING message ts
		expect(new Set(ops.map((o) => o.ts)).size).toBe(1); // one anchor message
		expect(engine.reactionAudit.map((r) => r.phase)).toEqual([
			"start",
			"complete",
		]);
	});

	it("a failed dispatch swaps 👀 for x (failure outcome), cursor unmoved", async () => {
		const world = makeSlackWorld({ name: "slack-react-failure" });
		const { engine } = world;
		const capturing = world.wire as unknown as SlackCapturingWire;
		await world.connectAndAwaitLive();

		world.pushMessage("C1", "processed");
		await eventually(() => engine.cursor.value === "e1");
		capturing.reactionOps.length = 0;

		engine.lifecycle.disable({
			kind: "secret_missing",
			secretKey: "SLACK_BOT_TOKEN",
			manifestName: "slack-react-failure",
		});
		world.pushMessage("C1", "doomed");
		await new Promise<void>((r) => setTimeout(r, 12));
		expect(capturing.reactionOps.map((o) => `${o.action}:${o.name}`)).toEqual([
			"add:eyes",
			"remove:eyes",
			"add:x",
		]);
		expect(
			engine.reactionAudit.some(
				(r) => r.phase === "complete" && r.outcome === "failure",
			),
		).toBe(true);
	});

	it("SLACK_REACTIONS=false (env or injected) disables the lifecycle entirely; parser matches :4250", async () => {
		const envWorld = makeSlackWorld({ name: "slack-react-env-off" });
		const envCapturing = envWorld.wire as unknown as SlackCapturingWire;
		process.env["SLACK_REACTIONS"] = "false";
		try {
			await envWorld.connectAndAwaitLive();
			envWorld.pushMessage("C1", "quiet");
			await eventually(() => envWorld.engine.cursor.value === "e1");
			expect(envCapturing.reactionOps).toEqual([]);
			expect(envWorld.engine.reactionAudit).toEqual([]);
		} finally {
			delete process.env["SLACK_REACTIONS"];
		}

		const injected = makeSlackWorld({
			name: "slack-react-injected-off",
			reactionsEnabled: false,
		});
		const injectedCapturing = injected.wire as unknown as SlackCapturingWire;
		await injected.connectAndAwaitLive();
		injected.pushMessage("C1", "also quiet");
		await eventually(() => injected.engine.cursor.value === "e1");
		expect(injectedCapturing.reactionOps).toEqual([]);

		for (const raw of ["false", "0", "no", "FALSE", "No"]) {
			expect(isSlackReactionsEnabled(raw)).toBe(false);
		}
		for (const raw of [undefined, "", "true", "1", "yes"]) {
			expect(isSlackReactionsEnabled(raw)).toBe(true);
		}
	});
});

describe("native stream START args (ws-7: recipients + pre-wire anchor guard)", () => {
	it("START carries recipient_user_id (renamed from user_id/sender_id) and recipient_team_id from the channel→team map; originals stripped", async () => {
		const world = makeSlackWorld({ name: "slack-start-recipients" });
		const { engine, wire } = world;
		await world.connectAndAwaitLive();

		// An inbound event remembers C1→W0 in the channel→team map
		// (_remember_channel_team parity).
		world.socketServer.pushMessage({
			channel: "C1",
			user: "user-1",
			text: "remember this channel",
		});
		await eventually(() => engine.cursor.value === "e1");

		await engine.sendDraft({
			chatId: "C1",
			draftId: 5,
			content: "stream head",
			metadata: {
				thread_id: "1700000000.000500",
				user_id: "user-9",
			} as never,
		});
		const startOp = wire
			.draftsOf("C1")
			.find((d) => d.metadata["stream_op"] === "start");
		expect(startOp).toBeDefined();
		expect(startOp!.metadata["thread_ts"]).toBe("1700000000.000500");
		expect(startOp!.metadata["recipient_user_id"]).toBe("user-9");
		expect(startOp!.metadata["recipient_team_id"]).toBe("W0"); // map, not auth scope
		// The internal stamps never ship on the vendor frame.
		expect(startOp!.metadata["user_id"]).toBeUndefined();
		expect(startOp!.metadata["sender_id"]).toBeUndefined();
		expect(startOp!.metadata["thread_id"]).toBeUndefined();

		// sender_id fallback renames identically.
		await engine.sendDraft({
			chatId: "C1",
			draftId: 6,
			content: "more",
			metadata: {
				thread_id: "1700000000.000500",
				sender_id: "user-8",
			} as never,
		});
		const second = wire
			.draftsOf("C1")
			.filter((d) => d.metadata["stream_op"] === "start")
			.at(-1);
		expect(second!.metadata["recipient_user_id"]).toBe("user-8");

		// Unknown channel falls back to the connect-time workspace team scope.
		await engine.sendDraft({
			chatId: "C-unknown",
			draftId: 7,
			content: "head",
			metadata: { thread_id: "1700000000.000700" } as never,
		});
		const fallback = wire
			.draftsOf("C-unknown")
			.find((d) => d.metadata["stream_op"] === "start");
		expect(fallback!.metadata["recipient_team_id"]).toBe("TWORKSPACE0");
		// Appends/seals never carry recipient stamps (START-only kwargs).
		const appendish = wire
			.draftsOf("C-unknown")
			.filter((d) => d.metadata["stream_op"] !== "start");
		expect(
			appendish.every(
				(d) =>
					d.metadata["recipient_user_id"] === undefined &&
					d.metadata["recipient_team_id"] === undefined,
			),
		).toBe(true);
	});

	it("pre-wire guard: an unanchored START fails BEFORE calling the API (no wire op)", async () => {
		const world = makeSlackWorld({ name: "slack-start-guard" });
		const { engine, wire } = world;

		const result = await engine.sendDraft({
			chatId: "C-guard",
			draftId: 1,
			content: "anchorless",
		});
		expect(result.success).toBe(false);
		expect(result.error).toBe("no thread_ts for native stream");
		// NOTHING reached the wire — the guard precedes the API call.
		expect(wire.draftsOf("C-guard")).toHaveLength(0);
		// A feature-gate latch must NOT engage for an anchor failure.
		expect(engine.nativeStreamLatch.unsupported).toBe(false);
		// Anchored STARTs sail through.
		const anchored = await engine.sendDraft({
			chatId: "C-guard",
			draftId: 2,
			content: "anchored",
			metadata: { thread_ts: "1700000000.000900" } as never,
		});
		expect(anchored.success).toBe(true);
	});
});

describe("auth.test at connect (ws-8: token identity → self-echo + team scope)", () => {
	it("connect resolves selfUserId + team scope from the token; the echo filter uses it", async () => {
		const world = makeSlackWorld({ name: "slack-auth-scope" });
		const { engine, subject } = world;
		const capturing = world.wire as unknown as SlackCapturingWire;
		expect(capturing.authTestCalls).toBe(0);

		await world.connectAndAwaitLive();

		expect(capturing.authTestCalls).toBe(1);
		expect(engine.authProbes).toEqual([
			{ ok: true, userId: "UBOTAUTH0", teamId: "TWORKSPACE0" },
		]);
		// Token identity REPLACED the 'bot-self' seed and feeds the echo filter.
		expect(engine.resolvedSelfUserId).toBe("UBOTAUTH0");
		subject.socketServer.pushMessage({
			channel: "C1",
			user: "UBOTAUTH0",
			text: "echo of myself",
		});
		await new Promise<void>((r) => setTimeout(r, 12));
		expect([...subject.turns()]).toEqual([]); // suppressed as self/echo

		// Real users still dispatch.
		world.pushMessage("C1", "hello bot");
		await eventually(() => subject.turns().includes("hello bot"));
	});

	it("an explicit auth failure fails the connect loudly; the socket never opens", async () => {
		const world = makeSlackWorld({
			name: "slack-auth-fail",
			authIdentity: { ok: false, error: "invalid_auth" },
		});
		const { engine, socketServer } = world;
		const connected = await engine.connect({ isReconnect: false });
		expect(connected).toBe(false);
		expect(engine.isLive).toBe(false);
		expect(socketServer.openConnectionCount).toBe(0);
		expect(engine.authProbes).toEqual([{ ok: false }]);
	});

	it("an explicitly injected botUserId wins over the token identity", async () => {
		const server = new SlackSocketModeServer();
		const wire = new FakePlatformWire();
		const engine = new SlackAdapter({
			manifestName: "slack-injected-id",
			transport: server,
			rest: wire,
			clock: new ManualClock(),
			botUserId: "UBOT-INJECTED",
		});
		// No authTest bound on the plain wire ⇒ identity stays the injection.
		const connected = await engine.connect({ isReconnect: false });
		expect(connected).toBe(true);
		expect(engine.resolvedSelfUserId).toBe("UBOT-INJECTED");
		await engine.disconnect();
	});
});

describe("chat.delete lane (ws-10: opt-in cleanup_progress capability)", () => {
	it("deleteMessage ships chat.delete {channel,ts}; class-level presence is the run.py probe", async () => {
		const world = makeSlackWorld({ name: "slack-delete" });
		const { engine } = world;
		const capturing = world.wire as unknown as SlackCapturingWire;

		// run.py:28580 probes getattr(type(adapter), "delete_message") — the
		// CLASS-LEVEL method is what arms the opt-in cleanup_progress config.
		expect(typeof SlackAdapter.prototype.deleteMessage).toBe("function");

		const ok = await engine.deleteMessage("C1", "1700000000.000555");
		expect(ok).toBe(true);
		expect(capturing.deleteOps).toEqual([
			{ channel: "C1", ts: "1700000000.000555" },
		]);
	});

	it("without a bound delete lane the capability reports false (silently disabled cleanup)", async () => {
		const engine = new SlackAdapter({
			manifestName: "slack-delete-bare",
			transport: new SlackSocketModeServer(),
			rest: new FakePlatformWire(), // no transmitDelete extra bound
			clock: new ManualClock(),
		});
		expect(await engine.deleteMessage("C1", "1700000000.000001")).toBe(false);
	});
});

describe("message_changed envelopes (ws-11: edits normalize onto changed-ts-deduped fresh turns)", () => {
	it("an edit to a NEVER-addressed message re-processes as a fresh turn under the original thread root", async () => {
		const world = makeSlackWorld({ name: "slack-edit-fresh" });
		const { engine, subject } = world;
		await world.connectAndAwaitLive();

		// Backlog edit: the original message never reached the agent.
		const editEvt: SlackEnvelopeEvent = world.pushMessageChanged({
			channel: "C1",
			user: "user-1",
			text: "edited body v2",
			originalTs: "1700000000.000301",
			eventTs: "1700000000.000900",
		});
		await eventually(() => engine.cursor.value === editEvt.id);
		expect([...subject.turns()]).toEqual(["edited body v2"]);
		// Same session/thread root as the original message (its own ts).
		expect(engine.sessionKeysSeen.at(-1)).toBe("slack-edit-fresh:C1:1700000000.000301");
	});

	it("redelivery of a DISPATCHED edit is absorbed by the processed-original-ts guard before any re-dispatch", async () => {
		const world = makeSlackWorld({ name: "slack-edit-dedup" });
		const { engine, subject } = world;
		await world.connectAndAwaitLive();

		const editEvt = world.pushMessageChanged({
			channel: "C1",
			user: "user-1",
			text: "once only",
			originalTs: "1700000000.000401",
			eventTs: "1700000000.000901",
		});
		await eventually(() => subject.turns().includes("once only"));
		const turnsAfterFirst = subject.turns().length;

		// Redelivery (retry-flagged, same envelope): the :5779 guard fires
		// first — routing the edit marked the original ts at :6855 — so the
		// turn is never duplicated and the dedup window stays untouched.
		await engine.handlePlatformEvent({
			...editEvt,
			retryAttempt: 1,
			retryReason: "retry_timeout",
		} as never);
		await new Promise<void>((r) => setTimeout(r, 12));
		expect(subject.turns().length).toBe(turnsAfterFirst);
		expect(engine.dedupSuppressedCount).toBe(0);

		// Malformed message_changed (no nested message) — dropped silently.
		await engine.handlePlatformEvent({
			id: "e-manual",
			type: "message",
			chatId: "C1",
			userId: "user-1",
			text: "",
			subtype: "message_changed",
		} as never);
		await new Promise<void>((r) => setTimeout(r, 12));
		expect(subject.turns().length).toBe(turnsAfterFirst);
	});

	it("the CHANGED-ts dedup key suppresses repeated FILTERED edits exactly once (:5797 window sees every envelope)", async () => {
		const world = makeSlackWorld({ name: "slack-edit-changed-dedup" });
		const { engine, subject } = world;
		await world.connectAndAwaitLive();

		// A bot-authored edited message passes normalization + the processed-ts
		// guard (its original was never routed) and lands in the CHANGED-ts
		// dedup window BEFORE the allow_bots filter drops it.
		const botEdit = world.pushMessageChanged({
			channel: "C1",
			user: "bot-user",
			text: "bot edit",
			originalTs: "1700000000.000451",
			eventTs: "1700000000.000951",
			botId: "B0BOT",
		});
		await new Promise<void>((r) => setTimeout(r, 12));
		expect([...subject.turns()]).toEqual([]);

		// Redelivered under a NEW envelope id but the SAME event_ts — the
		// workspace-scoped CHANGED-ts key is what suppresses it.
		await engine.handlePlatformEvent({
			...botEdit,
			id: `${botEdit.id}-retry`,
			envelopeId: `${botEdit.envelopeId}-retry`,
			retryAttempt: 1,
			retryReason: "retry_timeout",
		} as never);
		await new Promise<void>((r) => setTimeout(r, 12));
		expect([...subject.turns()]).toEqual([]);
		expect(engine.dedupSuppressedCount).toBe(1);
	});

	it("edits to ALREADY-addressed messages are dropped (no duplicate response); a second distinct edit is likewise absorbed", async () => {
		const world = makeSlackWorld({ name: "slack-edit-addressed" });
		const { engine, subject } = world;
		await world.connectAndAwaitLive();

		const originalTs = "1700000000.000501";
		const original = world.socketServer.pushMessage({
			channel: "C1",
			user: "user-1",
			text: "original question",
			ts: originalTs,
		});
		await eventually(() => subject.turns().includes("original question"));

		// Editing the just-answered message NEVER re-triggers (:5779 guard —
		// the original ts was routed into the agent at :6855).
		world.pushMessageChanged({
			channel: "C1",
			user: "user-1",
			text: "original question EDITED",
			originalTs,
			eventTs: "1700000000.000902",
		});
		await new Promise<void>((r) => setTimeout(r, 12));
		expect([...subject.turns()]).toEqual(["original question"]);
		void original;

		// Even processing edit #1 of an UNADDRESSED original absorbs later
		// edits of the same message: routing edit #1 marks the original ts
		// addressed (:6855 records the normalized event's ts).
		const freshOriginalTs = "1700000000.000601";
		const edit1 = world.pushMessageChanged({
			channel: "C1",
			user: "user-1",
			text: "first edit wins",
			originalTs: freshOriginalTs,
			eventTs: "1700000000.000903",
		});
		await eventually(() => subject.turns().includes("first edit wins"));
		world.pushMessageChanged({
			channel: "C1",
			user: "user-1",
			text: "second edit absorbed",
			originalTs: freshOriginalTs,
			eventTs: "1700000000.000904",
		});
		await new Promise<void>((r) => setTimeout(r, 12));
		expect(subject.turns().at(-1)).toBe("first edit wins");
		expect(subject.turns().filter((t) => t.includes("edit"))).toHaveLength(1);
		void edit1;
		expect(SLACK_PROCESSED_MESSAGE_TS_MAX).toBe(5000);
	});
});
