// Meta X-Hub-Signature-256 NEGATIVE MATRIX through the KIT trust engine
// (verifyHmacSignature/secureCompare) configured with WA scheme data — plus
// the GET subscription-handshake matrix. Parity:
// gateway/platforms/whatsapp_cloud.py:_verify_signature / _handle_verify.
// Behavior contracts, never vendor-error-string snapshots: each negative is a
// DISTINCT wire shape with its own expected verdict.

import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";

import { verifyHmacSignature, secureCompare } from "../kit/trust.js";
import {
	FIXTURE_APP_SECRET,
	FIXTURE_VERIFY_TOKEN,
	makeWaCloudFixture,
} from "./wa-cloud-fixture.js";
import { WaCloudAdapter } from "./wa-cloud-adapter.js";
import { FakeGraphServer } from "./graph-wire.js";

/** Scoped reader supplying ONLY the app secret (+ registration requirements). */
function waSecretsWithAppSecret() {
	return (name: string): string | undefined =>
		name === "WHATSAPP_CLOUD_PHONE_NUMBER_ID"
			? "pid"
			: name === "WHATSAPP_CLOUD_ACCESS_TOKEN"
				? "tok"
				: name === "WHATSAPP_CLOUD_APP_SECRET"
					? FIXTURE_APP_SECRET
					: undefined;
}

describe("Meta signature scheme via kit trust engine (wa scheme data)", () => {
	const body = Buffer.from(JSON.stringify({ ok: true }), "utf8");
	const valid = `sha256=${createHmac("sha256", FIXTURE_APP_SECRET).update(body).digest("hex")}`;

	it("valid signature over RAW body bytes verifies", () => {
		expect(verifyHmacSignature(FIXTURE_APP_SECRET, body, valid)).toBe(true);
	});

	it("tampered body rejects (signature covers exact bytes)", () => {
		const tampered = Buffer.from(
			body.toString("utf8").replace("true", "false"),
		);
		expect(verifyHmacSignature(FIXTURE_APP_SECRET, tampered, valid)).toBe(
			false,
		);
	});

	it("wrong secret rejects", () => {
		expect(verifyHmacSignature("other-secret", body, valid)).toBe(false);
	});

	it("missing/empty header rejects at the ADAPTER gate (prefix required there)", () => {
		// The kit helper is scheme-agnostic: it strips an OPTIONAL sha256= and
		// constant-time compares hex. Enforcing the PREFIX is the adapter's
		// scheme-data job (_verify_signature: "if not header.startswith").
		const adapter = new WaCloudAdapter({
			transport: new FakeGraphServer(),
			secretReader: waSecretsWithAppSecret(),
		});
		expect(adapter.verifySignature(body, "")).toBe(false);
		expect(adapter.verifySignature(body, undefined)).toBe(false);
		expect(adapter.verifySignature(body, valid.slice(7))).toBe(false); // bare hex
		expect(adapter.verifySignature(body, valid)).toBe(true);
	});

	it("uppercase hex ACCEPTS with lowercase prefix (compare lowercases hex, _verify_signature parity)", () => {
		const hex = valid.slice("sha256=".length);
		expect(
			verifyHmacSignature(
				FIXTURE_APP_SECRET,
				body,
				`sha256=${hex.toUpperCase()}`,
			),
		).toBe(true);
	});

	it("non-hex garbage rejects without touching the comparator", () => {
		expect(verifyHmacSignature(FIXTURE_APP_SECRET, body, "sha256=zzzz")).toBe(
			false,
		);
	});

	it("single-byte signature mutation flips the verdict (mutation check)", () => {
		const hex = valid.slice("sha256=".length);
		const flipped = `sha256=${hex[0] === "0" ? "1" : "0"}${hex.slice(1)}`;
		expect(secureCompare(hex, flipped)).toBe(false);
		expect(verifyHmacSignature(FIXTURE_APP_SECRET, body, flipped)).toBe(false);
	});

	it("adapter gate: unsigned POST → 401 + counter; signed POST passes the gate", async () => {
		const fx = makeWaCloudFixture();
		try {
			const envelope = fx.valueEnvelope({
				messages: [fx.textMessage("wamid.sig.1", "15551234567", "hi")],
			});
			const raw = JSON.stringify(envelope);

			const rejected = await fx.postRaw({}, raw);
			expect(rejected.status).toBe(401);
			expect(fx.adapter.counters.rejectedSignature).toBe(1);

			const accepted = await fx.postRaw(
				{ "x-hub-signature-256": fx.sign(raw) },
				raw,
			);
			expect(accepted.status).toBe(200);
			expect(fx.adapter.counters.accepted).toBe(1);
		} finally {
			fx.dispose();
		}
	});

	it("app_secret unset ⇒ fail-closed 503 BEFORE any signature math", async () => {
		const adapter = new WaCloudAdapter({
			transport: new FakeGraphServer(),
			secretReader: (name) =>
				name === "WHATSAPP_CLOUD_PHONE_NUMBER_ID"
					? "pid"
					: name === "WHATSAPP_CLOUD_ACCESS_TOKEN"
						? "tok"
						: undefined, // NO app secret
		});
		const resp = await adapter.handleWebhookPost(
			{ "x-hub-signature-256": "sha256=deadbeef" },
			Buffer.from("{}"),
		);
		expect(resp.status).toBe(503);
	});
});

describe("GET subscription handshake (_handle_verify matrix)", () => {
	it("unset verify_token ⇒ 503 refuse (never accept-any-token)", () => {
		const adapter = new WaCloudAdapter({
			transport: new FakeGraphServer(),
			secretReader: (name) =>
				name === "WHATSAPP_CLOUD_PHONE_NUMBER_ID"
					? "pid"
					: name === "WHATSAPP_CLOUD_ACCESS_TOKEN"
						? "tok"
						: undefined,
		});
		expect(
			adapter.handleVerifyRequest({
				"hub.mode": "subscribe",
				"hub.verify_token": "anything",
				"hub.challenge": "ch",
			}),
		).toEqual({ status: 503, text: "verify_token not configured" });
	});

	it("full handshake matrix", () => {
		const fx = makeWaCloudFixture();
		try {
			expect(
				fx.verify({
					"hub.mode": "other",
					"hub.verify_token": FIXTURE_VERIFY_TOKEN,
					"hub.challenge": "c",
				}).status,
			).toBe(400);
			expect(
				fx.verify({
					"hub.mode": "subscribe",
					"hub.verify_token": "wrong",
					"hub.challenge": "c",
				}).status,
			).toBe(403);
			expect(
				fx.verify({
					"hub.mode": "subscribe",
					"hub.verify_token": FIXTURE_VERIFY_TOKEN,
					"hub.challenge": "",
				}).status,
			).toBe(400);
			const ok = fx.verify({
				"hub.mode": "subscribe",
				"hub.verify_token": FIXTURE_VERIFY_TOKEN,
				"hub.challenge": "CHALLENGE-115599",
			});
			expect(ok.status).toBe(200);
			expect(ok.text).toBe("CHALLENGE-115599");
		} finally {
			fx.dispose();
		}
	});
});
