// Ingress pipeline contracts: envelope walk, wamid replay dedup/idempotency,
// status-callback handling, group-shaped refusal, containment. Parity:
// whatsapp_cloud.py:_handle_webhook / _dispatch_payload / _dedup_wamid /
// _build_message_event_from_cloud.

import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";

import {
	FIXTURE_APP_SECRET,
	FIXTURE_VERIFY_TOKEN,
	makeWaCloudFixture,
	type WaCloudFixture,
} from "./wa-cloud-fixture.js";
import { WaCloudAdapter, WA_CLOUD_REGISTRY } from "./wa-cloud-adapter.js";
import { FakeGraphServer } from "./graph-wire.js";
import { mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

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

function asRecord(v: unknown): Record<string, unknown> {
	return v !== null && typeof v === "object" && !Array.isArray(v)
		? (v as Record<string, unknown>)
		: {};
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
	describe("outbound conformity probes (send/clarify/approval wire shapes)", () => {
		it("blank/whitespace content short-circuits BEFORE any Graph POST (send parity)", async () => {
			const fx = makeWaCloudFixture();
			try {
				for (const blank of ["", "   ", "\n\t\n"]) {
					const result = await fx.adapter.send("15551234567", blank);
					expect(result.success).toBe(true);
					expect(result.messageId ?? null).toBeNull();
				}
				expect(fx.graph.sentMessages).toHaveLength(0); // NO wire call ever
			} finally {
				fx.dispose();
			}
		});

		it("zero-choice clarify passes reply context through (send_clarify reply_to parity)", async () => {
			const fx = makeWaCloudFixture();
			try {
				await fx.adapter.sendClarifyPrompt(
					"15551234567",
					"What did you mean?",
					[],
					"cl-z0",
					"sk-clz",
					{ replyToMessageId: "wamid.quote.me" },
				);
				const sends = fx.graph.textSendsOf("15551234567");
				expect(sends).toHaveLength(1);
				expect(sends[0]?.textBody).toBe("❓ What did you mean?");
				expect(sends[0]?.body["context"]).toEqual({
					message_id: "wamid.quote.me",
				});
			} finally {
				fx.dispose();
			}
		});

		it("smartDenied appends the owner-override suffix to the approval card body", async () => {
			const fx = makeWaCloudFixture();
			try {
				await fx.adapter.sendWhatsappApproval(
					"15551234567",
					"rm -rf /",
					"appr-1",
					"sk-appr",
					"destructive",
					{ smartDenied: true },
				);
				let body = "";
				for (const m of fx.graph.sentMessages) {
					const interactive = m.body["interactive"] as
						| Record<string, unknown>
						| undefined;
					const b = asRecord(interactive?.["body"])["text"];
					if (typeof b === "string") body = b;
				}
				expect(body).toContain("Reason: destructive");
				expect(body).toContain(
					"Smart DENY: owner override applies to this one operation only.",
				);

				// Without the flag the suffix is absent.
				await fx.adapter.sendWhatsappApproval(
					"15551234567",
					"ls",
					"appr-2",
					"sk-appr",
				);
				let plainBody = "";
				for (const m of fx.graph.sentMessages.slice(1)) {
					const interactive = m.body["interactive"] as
						| Record<string, unknown>
						| undefined;
					const b = asRecord(interactive?.["body"])["text"];
					if (typeof b === "string") plainBody = b;
				}
				expect(plainBody).not.toContain("Smart DENY");
			} finally {
				fx.dispose();
			}
		});
	});
});

// ── stability round 2: document text injection + slash-confirm card + quoted-
// text hydration ───────────────────────────────────────────────────────────────

describe("inbound document text injection (_build_message_event_from_cloud @~2020 parity)", () => {
	const CSV = "col_a,col_b\n1,2\n3,4";

	async function postDocument(
		fx: WaCloudFixture,
		wamid: string,
		mediaId: string,
		mime: string,
		filename?: string,
		caption?: string,
	): Promise<void> {
		const msg = fx.mediaMessage(
			wamid,
			"15551234567",
			"document",
			mediaId,
			mime,
			caption,
		);
		if (filename !== undefined) {
			(msg["document"] as Record<string, unknown>)["filename"] = filename;
		}
		await fx.postSigned(fx.valueEnvelope({ messages: [msg] }));
	}

	it("text-readable content injects INLINE; the [Document:] marker rides AFTER it", async () => {
		const fx = makeWaCloudFixture();
		try {
			const id = fx.graph.seedMedia("text/csv", Buffer.from(CSV, "utf8"));
			await postDocument(fx, "wamid.doc1", id, "text/csv", "report.csv");
			expect(await eventually(() => fx.adapter.turnLog.length === 1)).toBe(
				true,
			);
			// display name is the CACHED artifact basename (media_id + ext).
			expect(fx.adapter.turnLog[0]).toBe(
				`[Content of ${id}.csv]:\n${CSV}\n\n[Document: report.csv]`,
			);
		} finally {
			fx.dispose();
		}
	});

	it("captioned text documents prepend the content ahead of the caption", async () => {
		const fx = makeWaCloudFixture();
		try {
			const md = "# Title\n\nbody line";
			const id = fx.graph.seedMedia("text/plain", Buffer.from(md, "utf8"));
			await postDocument(
				fx,
				"wamid.doc2",
				id,
				"text/plain",
				undefined,
				"see attached",
			);
			expect(await eventually(() => fx.adapter.turnLog.length === 1)).toBe(
				true,
			);
			// No filename ⇒ no '[Document:]' marker; the caption rides after.
			expect(fx.adapter.turnLog[0]).toBe(
				`[Content of ${id}.txt]:\n${md}\n\nsee attached`,
			);
		} finally {
			fx.dispose();
		}
	});

	it("oversize (>100KB) text documents keep the metadata-only body", async () => {
		const fx = makeWaCloudFixture();
		try {
			const big = `${"x".repeat(100 * 1024)}\n`;
			const id = fx.graph.seedMedia("text/plain", Buffer.from(big, "utf8"));
			await postDocument(fx, "wamid.doc3", id, "text/plain", "big.log");
			expect(await eventually(() => fx.adapter.turnLog.length === 1)).toBe(
				true,
			);
			expect(fx.adapter.turnLog[0]).toBe("[Document: big.log]");
		} finally {
			fx.dispose();
		}
	});

	it("non-text extensions keep '[Document: fname]' (no injection)", async () => {
		const fx = makeWaCloudFixture();
		try {
			const id = fx.graph.seedMedia(
				"application/pdf",
				Buffer.from("%PDF-1.4 fake"),
			);
			await postDocument(fx, "wamid.doc4", id, "application/pdf", "file.pdf");
			expect(await eventually(() => fx.adapter.turnLog.length === 1)).toBe(
				true,
			);
			expect(fx.adapter.turnLog[0]).toBe("[Document: file.pdf]");
		} finally {
			fx.dispose();
		}
	});
});

describe("slash-confirm card (send_slash_confirm @~903 parity)", () => {
	it("renders the 3-button sc:{once|always|cancel}:{id} card and registers the pending confirm", async () => {
		const fx = makeWaCloudFixture();
		try {
			const result = await fx.adapter.sendSlashConfirm(
				"15551234567",
				"Run /deploy now?",
				"prod · main @abc123",
				"sk-sc-1",
				5150,
			);
			expect(result.success).toBe(true);

			expect(fx.graph.sentMessages).toHaveLength(1);
			const body = fx.graph.sentMessages[0]?.body as Record<string, unknown>;
			expect(body["to"]).toBe("15551234567"); // verbatim recipient
			expect(body["type"]).toBe("interactive");
			const interactive = body["interactive"] as Record<string, unknown>;
			expect(asRecord(interactive["body"])["text"]).toBe(
				"*Run /deploy now?*\n\nprod · main @abc123",
			);
			const buttons = (
				asRecord(interactive["action"])["buttons"] as Array<{
					type: string;
					reply: { id: string; title: string };
				}>
			).map((b) => b.reply);
			expect(buttons).toEqual([
				{ id: "sc:once:5150", title: "✅ Approve Once" },
				{ id: "sc:always:5150", title: "🔒 Always" },
				{ id: "sc:cancel:5150", title: "❌ Cancel" },
			]);
			expect(fx.adapter.slashConfirms.has(5150)).toBe(true);

			// A tap routes through THE one kit router and CLAIMS the tap
			// (never dispatched as a conversation turn).
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [
						fx.interactiveReply(
							"wamid.sc1",
							"15551234567",
							"sc:always:5150",
							"🔒 Always",
						),
					],
				}),
			);
			expect(
				await eventually(() => fx.adapter.resolvedFamilies.includes("sc")),
			).toBe(true);
			expect(fx.adapter.slashConfirms.has(5150)).toBe(false); // popped once
			await new Promise<void>((r) => setTimeout(r, 25));
			expect(fx.adapter.turnLog).toEqual([]);
		} finally {
			fx.dispose();
		}
	});

	it("body truncates at the 1024 interactive cap; failed POST never registers state", async () => {
		const fx = makeWaCloudFixture();
		try {
			fx.graph.script("messages", {
				status: 400,
				error: { message: "bad", code: 100 },
			});
			const failed = await fx.adapter.sendSlashConfirm(
				"15551234567",
				"T",
				"m".repeat(2000),
				"sk-sc-2",
				5151,
			);
			expect(failed.success).toBe(false);
			expect(fx.adapter.slashConfirms.has(5151)).toBe(false);

			const ok = await fx.adapter.sendSlashConfirm(
				"15551234567",
				"T",
				"m".repeat(2000),
				"sk-sc-3",
				5152,
			);
			expect(ok.success).toBe(true);
			const body = (
				(
					fx.graph.sentMessages[1]?.body["interactive"] as Record<
						string,
						unknown
					>
				)["body"] as Record<string, unknown>
			)["text"] as string;
			expect(body.length).toBeLessThanOrEqual(1024);
			expect(body.startsWith("*T*\n\n")).toBe(true);
			expect(fx.adapter.slashConfirms.has(5152)).toBe(true);
		} finally {
			fx.dispose();
		}
	});
});

describe("quoted replies hydrate reply_to_text onto the dispatched event (@~2067 parity)", () => {
	it("resolved quoted text rides event.metadata['reply_to_text'] for the run loop", async () => {
		const root = mkdtempSync(join(tmpdir(), "wa-cap-"));
		mkdirSync(join(root, "whatsapp-session"), { recursive: true });
		mkdirSync(join(root, "media"), { recursive: true });
		const graph = new FakeGraphServer();
		const capturedMetadata: Array<Record<string, unknown>> = [];
		const adapter = new WaCloudAdapter({
			transport: graph,
			nowMs: () => 1_700_000_000_000,
			whatsappSessionDir: join(root, "whatsapp-session"),
			mediaCacheDir: join(root, "media"),
			secretReader: (name) =>
				name === "WHATSAPP_CLOUD_PHONE_NUMBER_ID"
					? "wa-phone-id"
					: name === "WHATSAPP_CLOUD_ACCESS_TOKEN"
						? "wa-access-token"
						: name === "WHATSAPP_CLOUD_APP_SECRET"
							? FIXTURE_APP_SECRET
							: name === "WHATSAPP_CLOUD_VERIFY_TOKEN"
								? FIXTURE_VERIFY_TOKEN
								: undefined,
		});
		adapter.attachGuard({
			registry: WA_CLOUD_REGISTRY,
			messageHandler: async (event) => {
				capturedMetadata.push({ ...(event.metadata ?? {}) });
				return `reply:${event.text}`;
			},
			sendReply: async () => {},
		});
		try {
			const sent = await adapter.send("15551234567", "the original answer");
			const outWamid = sent.messageId as string;

			const replyBody = JSON.stringify({
				object: "whatsapp_business_account",
				entry: [
					{
						id: "waba-1",
						changes: [
							{
								field: "messages",
								value: {
									metadata: {
										display_phone_number: "15550001111",
										phone_number_id: "wa-phone-id",
									},
									contacts: [],
									messages: [
										{
											id: "wamid.r9",
											from: "15551234567",
											timestamp: "1700000000",
											type: "text",
											text: { body: "a follow-up" },
											context: {
												id: outWamid,
												from: "15550001111", // our display phone ⇒ own message
											},
										},
									],
								},
							},
						],
					},
				],
			});
			const resp = await adapter.handleWebhookPost(
				{
					"x-hub-signature-256": `sha256=${createHmac(
						"sha256",
						FIXTURE_APP_SECRET,
					)
						.update(replyBody)
						.digest("hex")}`,
				},
				Buffer.from(replyBody, "utf8"),
			);
			expect(resp.status).toBe(200);
			expect(await eventually(() => capturedMetadata.length > 0)).toBe(true);
			const md = capturedMetadata[0];
			expect(md?.["reply_to_text"]).toBe("the original answer");
			expect(md?.["reply_to_is_own_message"]).toBe(true);
			expect(adapter.quotedTextOf("15551234567", outWamid)).toBe(
				"the original answer",
			);
		} finally {
			rmSync(root, { recursive: true, force: true });
		}
	});
});
