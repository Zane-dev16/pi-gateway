// Ingress pipeline contracts: envelope walk, wamid replay dedup/idempotency,
// status-callback handling, group-shaped refusal, containment. Parity:
// whatsapp_cloud.py:_handle_webhook / _dispatch_payload / _dedup_wamid /
// _build_message_event_from_cloud.

import { describe, expect, it } from "vitest";

import { makeWaCloudFixture, type WaCloudFixture } from "./wa-cloud-fixture.js";

async function eventually(
	cond: () => boolean,
	timeoutMs = 2_000,
): Promise<boolean> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (cond()) return true;
		await new Promise<void>((r) => setTimeout(r, 5));
	}
	return cond();
}

describe("webhook ingress — envelope walk + dedup/idempotency", () => {
	it("signed text delivery dispatches ONE turn under the canonical session key", async () => {
		const fx = makeWaCloudFixture();
		try {
			const resp = await fx.postSigned(
				fx.valueEnvelope({
					messages: [
						fx.textMessage("wamid.t1", "15551234567", "hello gateway"),
					],
				}),
			);
			expect(resp.status).toBe(200); // verified ⇒ ALWAYS 200 (Meta retries non-200 for 7 days)
			expect(
				await eventually(() => fx.adapter.turnLog.includes("hello gateway")),
			).toBe(true);
		} finally {
			fx.dispose();
		}
	});

	it("wamid REPLAY is deduped: same id twice → one turn, duplicate counter bumped", async () => {
		const fx = makeWaCloudFixture();
		try {
			const envelope = () =>
				fx.valueEnvelope({
					messages: [fx.textMessage("wamid.dup.1", "15551234567", "once only")],
				});
			await fx.postSigned(envelope());
			await fx.postSigned(envelope()); // Meta redelivery
			await eventually(() => fx.adapter.turnLog.length === 1);
			await new Promise<void>((r) => setTimeout(r, 25));
			expect(fx.adapter.turnLog).toEqual(["once only"]);
			expect(fx.adapter.counters.duplicates).toBe(1);
			expect(fx.adapter.counters.accepted).toBe(1);
		} finally {
			fx.dispose();
		}
	});

	it("FIFO eviction keeps the seen-set bounded (cap honored, oldest forgotten)", () => {
		const fx = makeWaCloudFixture({ dedupCap: 3 });
		try {
			for (let i = 0; i < 5; i++) {
				expect(fx.adapter.dedupWamid(`w${i}`)).toBe(true);
			}
			// Cap 3: w0,w1 evicted; w2 still remembered.
			expect(fx.adapter.dedupWamid("w3")).toBe(false);
			expect(fx.adapter.counters.duplicates).toBe(1);
			expect(fx.adapter.dedupWamid("w1")).toBe(true); // evicted ⇒ passes again
			expect(fx.adapter.dedupWamid("")).toBe(true); // no id ⇒ cannot dedup
		} finally {
			fx.dispose();
		}
	});

	it("statuses[] callbacks are acknowledged and counted but NEVER dispatched as turns", async () => {
		const fx = makeWaCloudFixture();
		try {
			const resp = await fx.postSigned(
				fx.valueEnvelope({
					statuses: [fx.statusUpdate("wamid.s1", "delivered")],
				}),
			);
			expect(resp.status).toBe(200);
			await new Promise<void>((r) => setTimeout(r, 30));
			expect(fx.adapter.counters.statusesSeen).toBe(1);
			expect(fx.adapter.turnLog).toEqual([]);
		} finally {
			fx.dispose();
		}
	});

	it("non-WABA objects and non-messages fields are silently skipped", async () => {
		const fx = makeWaCloudFixture();
		try {
			await fx.postSigned({ object: "something_else", entry: [] });
			await fx.postSigned(
				fx.valueEnvelope({}),
				// valueEnvelope always uses field "messages"; craft a wrong-field one:
			);
			const wrongField = fx.valueEnvelope({});
			(wrongField["entry"] as Array<Record<string, unknown>>)[0] = {
				id: "waba-1",
				changes: [{ field: "account_alerts", value: {} }],
			};
			await fx.postSigned(wrongField);
			await new Promise<void>((r) => setTimeout(r, 20));
			expect(fx.adapter.turnLog).toEqual([]);
			expect(fx.adapter.counters.accepted).toBe(0);
		} finally {
			fx.dispose();
		}
	});

	it("group-shaped payloads are REFUSED (DM-only), never misaddressed", async () => {
		const fx = makeWaCloudFixture();
		try {
			const msg = fx.textMessage("wamid.g1", "15551234567", "group msg");
			msg["chat"] = "120363@g.us";
			await fx.postSigned(fx.valueEnvelope({ messages: [msg] }));
			await new Promise<void>((r) => setTimeout(r, 30));
			expect(fx.adapter.turnLog).toEqual([]);
			expect(fx.adapter.counters.refusedGroupShaped).toBe(1);
		} finally {
			fx.dispose();
		}
	});

	it("oversized body → 413 before parse; malformed JSON after valid signature → 400 contained", async () => {
		const fx = makeWaCloudFixture() as WaCloudFixture & { _parsed?: number };
		try {
			const huge = Buffer.alloc(3 * 1024 * 1024 + 1, 0x78);
			const oversize = await fx.postRaw(
				{ "x-hub-signature-256": fx.sign(huge) },
				huge,
			);
			expect(oversize.status).toBe(413);

			const badJson = '{"object": "whatsapp_business_account"';
			const bad = await fx.postRaw(
				{ "x-hub-signature-256": fx.sign(badJson) },
				badJson,
			);
			expect(bad.status).toBe(400);
			expect(fx.adapter.counters.accepted).toBe(0);
		} finally {
			fx.dispose();
		}
	});

	it("quoted replies resolve OUR prior outbound text via the send index", async () => {
		const fx = makeWaCloudFixture();
		try {
			const sent = await fx.adapter.send("15551234567", "the original answer");
			expect(sent.success).toBe(true);
			const outWamid = sent.messageId as string;

			const reply = fx.textMessage("wamid.r1", "15551234567", "a follow-up");
			reply["context"] = {
				id: outWamid,
				from: "15550001111", // our display phone ⇒ reply to OWN message
			};
			await fx.postSigned(fx.valueEnvelope({ messages: [reply] }));
			expect(await eventually(() => fx.adapter.turnLog.length === 1)).toBe(
				true,
			);

			// The inbound event carried the quote resolution (observable through
			// the metadata stamp built in buildInboundEvent).
			expect(fx.adapter.quotedTextOf("15551234567", outWamid)).toBe(
				"the original answer",
			);
		} finally {
			fx.dispose();
		}
	});

	it("interactive `appr:` tap resolves ONCE through the kit router and CLAIMS the tap", async () => {
		const fx = makeWaCloudFixture();
		try {
			// §9.1 grammar: ids are SHORT NUMERIC ints (64-byte callback cap).
			fx.adapter.appr.register(420042, "sk-appr-fixture");
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [
						fx.interactiveReply(
							"wamid.i1",
							"15551234567",
							"appr:420042:approve",
							"✅ Approve",
						),
					],
				}),
			);
			// Claimed: NO turn dispatched from the tap…
			expect(
				await eventually(() => fx.adapter.resolvedFamilies.includes("appr")),
			).toBe(true);
			await new Promise<void>((r) => setTimeout(r, 30));
			expect(fx.adapter.turnLog).toEqual([]);
			// …but a plain-text confirmation SEND went out (no edit API on Cloud).
			expect(fx.graph.textSendsOf("15551234567").length).toBeGreaterThanOrEqual(
				1,
			);

			// Double-tap: store popped ⇒ stale ⇒ falls through to TEXT dispatch.
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [
						fx.interactiveReply(
							"wamid.i2",
							"15551234567",
							"appr:420042:approve",
							"✅ Approve",
						),
					],
				}),
			);
			expect(
				await eventually(() => fx.adapter.turnLog.includes("✅ Approve")),
			).toBe(true);
		} finally {
			fx.dispose();
		}
	});
});
