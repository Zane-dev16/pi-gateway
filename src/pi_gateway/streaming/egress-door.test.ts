// EgressChokepoint BEHAVIOR CONTRACTS — the DEC-006 single-audited-chokepoint
// property with BOTH-door coverage (04 §1.1, §5.1; §8 "single chokepoint test").
// Every assertion here targets an effect that vanishes if interception moves
// out of the shared chokepoint into per-door magic.

import { describe, expect, it } from "vitest";
import {
	INTERIM_SEND_MARKER,
	type Metadata,
	type SendResult,
} from "./adapter-seam.js";
import {
	EgressChokepoint,
	turnKey,
	type DoorTransport,
} from "./egress-door.js";
import type { EditOp, SendOp, WireOp } from "./testing/fake-adapters.js";

interface Harness {
	cp: EgressChokepoint;
	ops: WireOp[];
	state: { isMessage: boolean; failSeals: boolean };
}

function harness(opts?: { isMessage?: boolean }): Harness {
	const ops: WireOp[] = [];
	const state = { isMessage: opts?.isMessage ?? true, failSeals: false };
	let messageIdCounter = 0;
	const transport: DoorTransport = {
		streamIsMessageForChat: () => state.isMessage,
		async transmitSend(
			chatId,
			content,
			metadata,
			platform,
		): Promise<SendResult> {
			messageIdCounter += 1;
			ops.push({ op: "send", chatId, content, metadata, platform });
			return { success: true, messageId: `msg_${messageIdCounter}` };
		},
		async transmitEdit(chatId, messageId, content, o, platform) {
			ops.push({
				op: "edit",
				chatId,
				messageId,
				content,
				finalize: o.finalize,
				metadata: undefined,
				platform,
			});
			return { success: true, messageId };
		},
		async transmitSeal(_key, chatId, draftId, content, metadata) {
			if (state.failSeals) {
				return { success: false, error: "forced seal failure" };
			}
			messageIdCounter += 1;
			const messageId = `sealed_${messageIdCounter}`;
			ops.push({
				op: "draft",
				chatId,
				draftId,
				content,
				final: true,
				messageId,
				metadata,
			});
			return { success: true, messageId };
		},
	};
	return { cp: new EgressChokepoint(transport), ops, state };
}

/** Arm an open draft the way a relay adapter's send_draft would. */
function arm(cp: EgressChokepoint, chatId: string, draftId: number): void {
	const v = cp.draftAdmission({ chatId, draftId });
	expect(v.swallow).toBe(false);
	if (v.arm) cp.armOpenDraft(v.key, draftId);
}

describe("single-audited-chokepoint property (DEC-006)", () => {
	it("EVERY admission through EITHER door yields EXACTLY ONE audit entry, in order", async () => {
		const { cp } = harness();
		await cp.admit({ door: "send", chatId: "c", content: "one" });
		// Same identity-less turn lane ⇒ reconciles by EDIT (invariant 4)…
		await cp.admit({
			door: "send_for_platform",
			chatId: "c",
			content: "two",
			platform: "slack",
		});
		// …interim lane has no own id yet ⇒ plain send.
		await cp.admit({
			door: "send",
			chatId: "c",
			content: "three",
			metadata: { [INTERIM_SEND_MARKER]: true },
		});
		expect(cp.audit.map((a) => [a.door, a.action])).toEqual([
			["send", "plain-send"],
			["send_for_platform", "reconcile-edit"],
			["send", "plain-send"],
		]);
		expect(cp.audit.map((a) => a.interim)).toEqual([false, false, true]);
	});

	it("_interim_send is POPPED at BOTH doors — wire metadata never carries it", async () => {
		const { cp, ops } = harness();
		const mdA: Metadata = {
			[INTERIM_SEND_MARKER]: true,
			tag: "x",
			reply_to_message_id: "1",
		};
		const mdB: Metadata = {
			[INTERIM_SEND_MARKER]: true,
			tag: "x",
			reply_to_message_id: "2",
		};
		await cp.admit({
			door: "send",
			chatId: "c",
			content: "interim A",
			metadata: { ...mdA },
		});
		await cp.admit({
			door: "send_for_platform",
			chatId: "c",
			content: "interim B",
			platform: "telegram",
			metadata: { ...mdB },
		});
		for (const op of ops) {
			if (op.op === "send") {
				expect(op.metadata?.[INTERIM_SEND_MARKER]).toBeUndefined();
				expect(op.metadata?.["tag"]).toBe("x"); // rest of metadata survives
			}
		}
		expect(ops.filter((o) => o.op === "send")).toHaveLength(2);
	});

	it("an interim send NEVER seal-intercepts: armed draft stays open, final still seals after", async () => {
		const { cp, ops } = harness();
		arm(cp, "chat", 41);
		expect(cp.isOpenDraft("chat")).toBe(true);

		const sealed = await cp.admit({
			door: "send",
			chatId: "chat",
			content: "commentary beat",
			metadata: { [INTERIM_SEND_MARKER]: true },
		});
		// Delivered as ordinary send — NOT absorbed into the live stream.
		expect(sealed.success).toBe(true);
		const sendOp = ops.find((o): o is SendOp => o.op === "send");
		expect(sendOp?.content).toBe("commentary beat");
		expect(ops.some((o) => o.op === "draft" && o.final)).toBe(false);
		expect(cp.isOpenDraft("chat")).toBe(true);

		// The real turn-final DOES seal.
		const fin = await cp.admit({
			door: "send",
			chatId: "chat",
			content: "true final",
		});
		expect(fin.messageId).toMatch(/^sealed_/);
		const sealAudit = cp.audit.find((a) => a.action === "seal");
		expect(sealAudit?.interim).toBe(false);
	});

	it("replyTo parameter lands in wire turn identity on both doors", async () => {
		const { cp, ops } = harness();
		await cp.admit({ door: "send", chatId: "c", content: "x", replyTo: "42" });
		await cp.admit({
			door: "send_for_platform",
			chatId: "c",
			content: "y",
			replyTo: "43",
			platform: "p",
		});
		const sends = ops.filter((o): o is SendOp => o.op === "send");
		expect(sends[0]?.metadata?.["reply_to_message_id"]).toBe("42");
		expect(sends[1]?.metadata?.["reply_to_message_id"]).toBe("43");
	});
});

describe("§5.1 seal mechanics", () => {
	it("FAILED seal falls through to PLAIN SEND byte-exact — never swallowed; tombstone set first", async () => {
		const h = harness();
		h.state.failSeals = true;
		arm(h.cp, "chat", 7);
		const res = await h.cp.admit({
			door: "send",
			chatId: "chat",
			content: "THE FINAL ✅",
		});

		// Seal attempted and failed…
		expect(h.ops.some((o) => o.op === "draft" && o.final)).toBe(false);
		// …final delivered as ordinary send, BYTE-EXACT.
		const send = h.ops.find((o): o is SendOp => o.op === "send");
		expect(send?.content).toBe("THE FINAL ✅");
		expect(res.success).toBe(true);
		expect(res.messageId).toMatch(/^msg_/);
		expect(h.cp.audit.at(-1)?.action).toBe("seal-failed-plain-send");
		// Tombstone BEFORE the transport call: stragglers can't re-arm.
		expect(h.cp.isSealedDraft("chat")).toBe(true);
		expect(h.cp.isOpenDraft("chat")).toBe(false);
		// The failed attempt's draft_id is swallowed as a post-seal straggler.
		const verdict = h.cp.draftAdmission({ chatId: "chat", draftId: 7 });
		expect(verdict.swallow).toBe(true);
	});

	it("reconcile-by-edit BESIDE the sealed stream: edit into the sealed id, NEVER a second plain send", async () => {
		const { cp, ops } = harness();
		arm(cp, "chat", 9);
		const seal = await cp.admit({
			door: "send",
			chatId: "chat",
			content: "sealed final",
		});
		expect(seal.messageId).toBeDefined();

		// Beside-sealed redelivery through door 2 (delivery-resolver lane).
		await cp.admit({
			door: "send_for_platform",
			chatId: "chat",
			content: "queued follow-up",
			platform: "discord",
		});
		const edit = ops.find((o): o is EditOp => o.op === "edit");
		expect(edit?.messageId).toBe(seal.messageId);
		expect(edit?.finalize).toBe(true);
		expect(edit?.platform).toBe("discord");
		expect(
			ops.some((o) => o.op === "send" && o.content === "queued follow-up"),
		).toBe(false);
		expect(cp.audit.at(-1)?.action).toBe("reconcile-edit");

		// No editable message ⇒ plain send (invariant 4 fallback).
		await cp.admit({
			door: "send",
			chatId: "other-chat",
			content: "fresh message",
		});
		expect(cp.audit.at(-1)?.action).toBe("plain-send");
	});

	it("failed EDIT falls through to plain send (run.py parity)", async () => {
		const ops: WireOp[] = [];
		const state = { isMessage: true, failSeals: false, editFails: false };
		let n = 0;
		const transport: DoorTransport = {
			streamIsMessageForChat: () => state.isMessage,
			async transmitSend(chatId, content, metadata) {
				n += 1;
				ops.push({ op: "send", chatId, content, metadata });
				return { success: true, messageId: `msg_${n}` };
			},
			async transmitEdit() {
				if (state.editFails) return { success: false, error: "edit rejected" };
				throw new Error("unreachable in this test");
			},
			async transmitSeal() {
				return { success: false, error: "no seal here" };
			},
		};
		const cp = new EgressChokepoint(transport);
		await cp.admit({ door: "send", chatId: "c", content: "first" }); // establishes lane
		state.editFails = true;
		const res = await cp.admit({
			door: "send",
			chatId: "c",
			content: "second",
		});
		expect(res.success).toBe(true);
		const sends = ops.filter((o) => o.op === "send");
		expect(sends).toHaveLength(2);
		expect(cp.audit.at(-1)?.action).toBe("plain-send");
	});

	it("interception runs BEFORE any explicit-platform branch (finding #7)", async () => {
		const { cp, ops } = harness();
		arm(cp, "chat", 3);
		const res = await cp.admit({
			door: "send_for_platform",
			chatId: "chat",
			content: "platform final",
			platform: "whatsapp",
		});
		expect(res.messageId).toMatch(/^sealed_/);
		expect(ops.some((o) => o.op === "send" && o.platform === "whatsapp")).toBe(
			false,
		);
	});

	it("identity-carrying callers NEVER fall back to the single-open-stream match", async () => {
		const { cp, ops } = harness();
		arm(cp, "chat", 5);
		const res = await cp.admit({
			door: "send",
			chatId: "chat",
			content: "someone else's turn",
			metadata: { reply_to_message_id: "999" },
		});
		expect(ops.some((o) => o.op === "draft" && o.final)).toBe(false);
		expect(res.messageId).toMatch(/^msg_/);
	});

	it("single-open-stream fallback matches ONLY unambiguous identity-less callers; ambiguous ⇒ edit beside sealed", async () => {
		const { cp, ops } = harness();
		arm(cp, "chat", 11);
		const seal = await cp.admit({
			door: "send",
			chatId: "chat",
			content: "bare caller",
		});
		expect(seal.messageId).toMatch(/^sealed_/); // matched the one open stream

		// Second open streams same chat ⇒ stream match ambiguous → interception
		// skipped, but the sealed turn lane from part one EXISTS ⇒ reconcile by
		// EDIT beside the sealed stream (invariant 4), never a second send.
		const vA = cp.draftAdmission({
			chatId: "chat",
			draftId: 21,
			metadata: { reply_to_message_id: "a" },
		});
		const vB = cp.draftAdmission({
			chatId: "chat",
			draftId: 22,
			metadata: { reply_to_message_id: "b" },
		});
		cp.armOpenDraft(vA.key, 21);
		cp.armOpenDraft(vB.key, 22);
		await cp.admit({ door: "send", chatId: "chat", content: "second bare" });
		const edit = ops.find((o): o is EditOp => o.op === "edit");
		expect(edit?.messageId).toBe(seal.messageId);
		expect(edit?.finalize).toBe(true);
		expect(ops.some((o) => o.op === "send")).toBe(false);
		expect(cp.audit.at(-1)?.action).toBe("reconcile-edit");
		expect(ops.filter((o) => o.op === "draft" && o.final)).toHaveLength(1);
	});
});

describe("draft-frame admission (relay send_draft delegation)", () => {
	it("arming gate: ONLY stream-is-message chats arm interception (review B4)", () => {
		const telegram = harness({ isMessage: false });
		const v1 = telegram.cp.draftAdmission({ chatId: "tg", draftId: 1 });
		expect(v1.arm).toBe(false);

		const relay = harness({ isMessage: true });
		const v2 = relay.cp.draftAdmission({ chatId: "sl", draftId: 2 });
		expect(v2.arm).toBe(true);
		relay.cp.armOpenDraft(v2.key, 2);
		expect(relay.cp.isOpenDraft("sl")).toBe(true);
	});

	it("post-seal STRAGGLER frames are swallowed by draft_id-matched tombstones", async () => {
		const { cp } = harness();
		arm(cp, "chat", 21);
		await cp.admit({ door: "send", chatId: "chat", content: "seal me" });
		// Same draft_id after seal → swallow (content already in sealed msg).
		expect(cp.draftAdmission({ chatId: "chat", draftId: 21 }).swallow).toBe(
			true,
		);
		// A NEW draft id (next segment/turn) is NOT swallowed.
		expect(cp.draftAdmission({ chatId: "chat", draftId: 22 }).swallow).toBe(
			false,
		);
	});

	it("turnKey separates chats and per-turn identities", () => {
		expect(turnKey("c", undefined)).toBe("c|_");
		expect(turnKey("c", { reply_to_message_id: "5" })).toBe("c|5");
		expect(turnKey("c", { message_id: "m1" })).toBe("c|m1");
		expect(turnKey("c", {})).toBe("c|_"); // empty metadata → bare fallback
		expect(turnKey("c1", { reply_to_message_id: "5" })).not.toBe(
			turnKey("c2", { reply_to_message_id: "5" }),
		);
	});
});
