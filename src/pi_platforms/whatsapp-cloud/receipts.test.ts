// Mark-as-read + typing receipt lifecycle (whatsapp_cloud.py:send_typing
// parity): ONE coupled POST — status:"read" + typing_indicator against the
// LATEST inbound wamid per chat; skip when nothing inbound; 131009 stale-wamid
// rejection is info-class, never fatal.

import { describe, expect, it } from "vitest";

import { makeWaCloudFixture } from "./wa-cloud-fixture.js";

describe("mark-as-read receipt lifecycle", () => {
	it("receipt attaches to the LATEST inbound wamid, coupled with the typing indicator", async () => {
		const fx = makeWaCloudFixture();
		try {
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [
						fx.textMessage("wamid.old", "15551234567", "first"),
						fx.textMessage("wamid.new", "15551234567", "second"),
					],
				}),
			);
			await fx.adapter.markReadAndTyping("15551234567");

			const receipts = fx.graph.readReceipts();
			expect(receipts).toHaveLength(1);
			const body = receipts[0]?.body as Record<string, unknown>;
			expect(body["status"]).toBe("read");
			expect(body["message_id"]).toBe("wamid.new"); // latest, not first
			expect(body["messaging_product"]).toBe("whatsapp");
			expect(body["typing_indicator"]).toEqual({ type: "text" });
			expect(fx.adapter.receipts[0]?.ok).toBe(true);
		} finally {
			fx.dispose();
		}
	});

	it("no inbound message yet ⇒ SKIP silently (zero wire calls)", async () => {
		const fx = makeWaCloudFixture();
		try {
			await fx.adapter.markReadAndTyping("15550000000");
			expect(fx.graph.readReceipts()).toHaveLength(0);
			expect(fx.graph.sentMessages).toHaveLength(0);
			expect(fx.adapter.receipts).toHaveLength(0);
		} finally {
			fx.dispose();
		}
	});

	it("per-chat isolation: chat B's receipt targets chat B's wamid", async () => {
		const fx = makeWaCloudFixture();
		try {
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [fx.textMessage("wamid.a", "15551110000", "from a")],
				}),
			);
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [fx.textMessage("wamid.b", "15552220000", "from b")],
				}),
			);
			await fx.adapter.markReadAndTyping("15551110000");
			await fx.adapter.markReadAndTyping("15552220000");

			const receipts = fx.graph.readReceipts();
			expect(receipts).toHaveLength(2);
			const postedWamids = receipts
				.map((r) => String((r.body as Record<string, unknown>)["message_id"]))
				.sort();
			expect(postedWamids).toEqual(["wamid.a", "wamid.b"]);
			// Chat→wamid association held by the adapter's per-chat cache.
			const byChat = new Map(
				fx.adapter.receipts.map((r) => [r.chatId, r.wamid]),
			);
			expect(byChat.get("15551110000")).toBe("wamid.a");
			expect(byChat.get("15552220000")).toBe("wamid.b");
		} finally {
			fx.dispose();
		}
	});

	it("stale-wamid rejection code 131009 is contained: receipt recorded not-ok, lifecycle continues", async () => {
		const fx = makeWaCloudFixture();
		try {
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [fx.textMessage("wamid.stale", "15551234567", "old msg")],
				}),
			);
			fx.graph.script("messages", {
				status: 400,
				error: {
					message: "(#131009) Parameter value is not valid",
					type: "OAuthException",
					code: 131009,
				},
			});
			// MUST NOT throw — best-effort UX polish (send_typing contract).
			await expect(
				fx.adapter.markReadAndTyping("15551234567"),
			).resolves.toBeUndefined();
			expect(fx.adapter.receipts).toHaveLength(1);
			expect(fx.adapter.receipts[0]?.ok).toBe(false);
			expect(fx.adapter.receipts[0]?.rejectedCode).toBe(131009);

			// A subsequent receipt attempt still flows (lifecycle unbroken).
			fx.graph.reset();
			await fx.adapter.markReadAndTyping("15551234567");
			expect(fx.graph.readReceipts()).toHaveLength(1);
		} finally {
			fx.dispose();
		}
	});
});
