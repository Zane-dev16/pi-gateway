// pi_platforms/weixin/weixin-vendor-contracts.test.ts — vendor-truth contracts
// for the adjudicated conformity findings, ported from the READ-ONLY Hermes
// reference gateway/platforms/weixin.py:
//
//   cn-3  _api_post/_base_info/_headers — EVERY outgoing iLink POST merges
//         {channel_version:"2.2.0"} base_info and carries the exact header
//         plane (AuthorizationType / Content-Length / X-WECHAT-UIN /
//         iLink-App-Id / iLink-App-ClientVersion / Bearer). Asserted on EVERY
//         postLog record, including the getupdates long poll.
//   cn-2  _send_file — getuploadurl (padded filesize, hex aeskey,
//         no_need_thumb) → ECB ciphertext CDN POST (octet-stream →
//         x-encrypted-param) → sendmessage media item with aes_key =
//         base64(HEX STRING); caption precedes media; failures abort in
//         vendor order.
//   cn-5  send_typing/stop_typing/_ensure_typing_ticket — turns bracket
//         dispatch with status 1|2 signals on getConfig-refreshed tickets;
//         TTL expiry refetches (stuck-indicator guard).
//   cn-9  qr_login — get_bot_qrcode then a get_qrcode_status poll loop:
//         wait/scaned/scaned_but_redirect repoint/expired ≤3 refreshes/
//         confirmed credentials; GETs carry app identity ONLY.

import { describe, expect, it } from "vitest";
import { createHash } from "node:crypto";
import type { ManualClock } from "../persistent-ws/manual-clock.js";
import type { WXWorld } from "./weixin-fixture.js";
import { makeWXWorld } from "./weixin-fixture.js";
import { WeixinAdapter, type WeixinSyncStore } from "./weixin-adapter.js";
import { aes128EcbDecrypt, aesPaddedSize, parseAesKey } from "./wire-crypto.js";
import {
	aesKeyForApi,
	baseInfo,
	buildOutboundMediaItem,
	cdnUploadUrl,
	outboundMediaKind,
} from "./ilink-transport.js";
import {
	BACKOFF_DELAY_SECONDS,
	CHANNEL_VERSION,
	EP_GET_UPLOAD_URL,
	EP_SEND_MESSAGE,
	ILINK_APP_CLIENT_VERSION,
	ILINK_APP_ID,
	ILINK_BASE_URL,
	ITEM_FILE,
	ITEM_IMAGE,
	ITEM_VOICE,
	MEDIA_FILE,
	MEDIA_IMAGE,
	MEDIA_VOICE,
	QR_DEFAULT_BOT_TYPE,
	SEND_CHUNK_RETRIES,
	SEND_CHUNK_RETRY_DELAY_S,
	SESSION_EXPIRED_PAUSE_S,
	TEXT_BATCH_DELAY_S,
	TYPING_START,
	TYPING_STOP,
	TYPING_TICKET_TTL_S,
	WEIXIN_COPY_LINE_WIDTH,
	WX_MAX_MESSAGE_LENGTH,
} from "./manifest.js";
import { WEIXIN_CDN_BASE_URL } from "./manifest.js";
import { FakeILinkServer, type ILinkPostRecord } from "./fake-ilink.js";

// ── world helpers ────────────────────────────────────────────────────────────

async function connectedWorld(name: string): Promise<WXWorld> {
	const w = makeWXWorld({ name });
	await w.connectAndAwaitLive();
	return w;
}

/** Pump the injected clock until `pred` holds (late timers settle per step). */
async function pump(
	w: WXWorld,
	pred: () => boolean,
	rounds = 40,
): Promise<void> {
	for (let i = 0; i < rounds && !pred(); i++) {
		await w.clock.advance(1_000);
		await new Promise<void>((r) => setTimeout(r, 0));
	}
}

function wxText(
	messageId: string,
	from: string,
	text: string,
): Parameters<FakeILinkServer["pushMessage"]>[0] {
	return {
		from_user_id: from,
		message_id: messageId,
		msg_type: 1,
		item_list: [{ type: 1, text_item: { text } }],
	};
}

/**
 * THE cn-3 assertion: one outgoing POST's request shape matches Hermes'
 * _api_post exactly — merged base_info plus the full header plane.
 */
function expectConformingPost(rec: ILinkPostRecord, token: string): void {
	expect(rec.base_info).toEqual({ channel_version: CHANNEL_VERSION });
	expect(rec.headers["Content-Type"]).toBe("application/json");
	expect(rec.headers["AuthorizationType"]).toBe("ilink_bot_token");
	expect(rec.headers["iLink-App-Id"]).toBe(ILINK_APP_ID);
	expect(rec.headers["iLink-App-ClientVersion"]).toBe(
		String(ILINK_APP_CLIENT_VERSION),
	);
	expect(rec.headers["Authorization"]).toBe(`Bearer ${token}`);
	// X-WECHAT-UIN: base64 of a DECIMAL uint32 string.
	const uin = Buffer.from(rec.headers["X-WECHAT-UIN"] ?? "", "base64").toString(
		"ascii",
	);
	expect(uin).toMatch(/^\d+$/);
	expect(Number(uin)).toBeLessThanOrEqual(0xffffffff);
	// Content-Length is the UTF-8 byte length of the serialized body.
	expect(Number(rec.headers["Content-Length"])).toBe(
		Buffer.byteLength(JSON.stringify(rec.payload), "utf8"),
	);
}

// ── cn-3 ─────────────────────────────────────────────────────────────────────

describe("cn-3 — every outgoing iLink POST carries base_info + the header plane", () => {
	it("getupdates long-poll AND sendmessage egress both conform on EVERY postLog record", async () => {
		const w = await connectedWorld("wx-cn3");

		// Ingress turn → getupdates + sendmessage POSTs.
		w.server.pushMessage(wxText("c3-1", "u_c3", "hello"));
		await pump(w, () => w.engine.turnLog.length >= 1);

		// Direct egress chunk too.
		const results = await w.engine.deliverText("u_c3", "egress body");
		expect(results[results.length - 1]?.success).toBe(true);
		await pump(w, () =>
			w.server.postLog.some((r) => r.endpoint === EP_SEND_MESSAGE),
		);

		const endpoints = new Set(w.server.postLog.map((r) => r.endpoint));
		expect(endpoints.has("ilink/bot/getupdates")).toBe(true);
		expect(endpoints.has(EP_SEND_MESSAGE)).toBe(true);
		for (const rec of w.server.postLog) {
			expectConformingPost(rec, w.engine.token);
		}
	});

	it("the ticket-refresh getConfig call conforms as well (base_info + headers recorded)", async () => {
		const w = await connectedWorld("wx-cn3b");
		w.engine.sendTypingIndicator("u_c3c"); // forces getConfig via typingTicketFor
		const cfgPosts = w.server.postLog.filter(
			(r) => r.endpoint === "ilink/bot/getconfig",
		);
		expect(cfgPosts.length).toBeGreaterThanOrEqual(1);
		for (const rec of cfgPosts) expectConformingPost(rec, w.engine.token);
	});
});

// ── cn-2 ─────────────────────────────────────────────────────────────────────

describe("cn-2 — sendFile: getuploadurl → ECB ciphertext CDN POST → media item", () => {
	it("happy path honors every vendor byte shape (padded filesize, hex aeskey, base64(hex) item key)", async () => {
		const w = await connectedWorld("wx-cn2-file");
		w.engine.contextTokens.set("u_file", "ctx-tok");
		const plaintext = Buffer.from("media-bytes-123"); // 15 bytes

		const res = await w.engine.sendFile("u_file", {
			filename: "photo.png",
			plaintext,
		});
		expect(res.success).toBe(true);

		// Leg 1: ilink/bot/getuploadurl payload shapes.
		const up = w.server.getUploadUrlCalls.at(-1)!;
		expect(up.filekey).toMatch(/^[0-9a-f]{32}$/); // token_hex(16)
		expect(up.media_type).toBe(MEDIA_IMAGE);
		expect(up.to_user_id).toBe("u_file");
		expect(up.rawsize).toBe(plaintext.length);
		expect(up.rawfilemd5).toBe(
			createHash("md5").update(plaintext).digest("hex"),
		);
		expect(up.filesize).toBe(aesPaddedSize(plaintext.length)); // PKCS#7-padded
		expect(aesPaddedSize(plaintext.length)).toBeGreaterThan(plaintext.length);
		expect(up.no_need_thumb).toBe(true);
		expect(up.aeskey).toMatch(/^[0-9a-f]{32}$/); // raw hex, not base64
		expect(up.base_info).toEqual(baseInfo());
		expectConformingPost(
			w.server.postLog.find((r) => r.endpoint === EP_GET_UPLOAD_URL)!,
			w.engine.token,
		);

		// Leg 2: CDN POST of the ECB ciphertext (octet-stream).
		const cdn = w.server.cdnUploadCalls.at(-1)!;
		expect(cdn.contentType).toBe("application/octet-stream");
		expect(cdn.status).toBe(200);
		expect(cdn.url).toBe(
			cdnUploadUrl(WEIXIN_CDN_BASE_URL, up.response.upload_param, up.filekey),
		);
		expect(cdn.ciphertextSize).toBe(aesPaddedSize(plaintext.length));
		// The uploaded bytes decrypt to the plaintext under the advertised key.
		const aesKey = Buffer.from(up.aeskey, "hex");
		expect(aes128EcbDecrypt(cdn.ciphertext!, aesKey).equals(plaintext)).toBe(
			true,
		);

		// Leg 3: sendmessage media item — byte-exact vendor shapes.
		const sendRec = w.server.sendCalls.at(-1)!;
		expect(sendRec.to_user_id).toBe("u_file");
		expect(sendRec.context_token).toBe("ctx-tok");
		const msgPost = w.server.postLog
			.filter((r) => r.endpoint === EP_SEND_MESSAGE)
			.at(-1)!;
		const msg = msgPost.payload["msg"] as Record<string, unknown>;
		const item = (msg["item_list"] as Array<Record<string, unknown>>)[0]!;
		expect(item["type"]).toBe(ITEM_IMAGE);
		const imageItem = item["image_item"] as Record<string, unknown>;
		const media = imageItem["media"] as Record<string, unknown>;
		expect(media["encrypt_query_param"]).toBe(cdn.encryptedParam);
		// aes_key = base64(HEX STRING) — NEVER base64(raw bytes).
		expect(media["aes_key"]).toBe(aesKeyForApi(up.aeskey));
		expect(
			Buffer.from(String(media["aes_key"]), "base64").toString("ascii"),
		).toBe(up.aeskey);
		expect(parseAesKey(String(media["aes_key"])).equals(aesKey)).toBe(true);
		expect(media["encrypt_type"]).toBe(1);
		expect(imageItem["mid_size"]).toBe(aesPaddedSize(plaintext.length));
	});

	it("a .silk voice rides MEDIA_VOICE with encode_type 6 @ 24kHz/16-bit; a pdf rides MEDIA_FILE with file_name+len", async () => {
		const w = await connectedWorld("wx-cn2-kinds");
		const silk = await w.engine.sendFile("u_k", {
			filename: "note.silk",
			plaintext: Buffer.from("voice-bytes"),
		});
		expect(silk.success).toBe(true);
		const voiceUp = w.server.getUploadUrlCalls.at(-1)!;
		expect(voiceUp.media_type).toBe(MEDIA_VOICE);
		const voiceItem = (
			(
				w.server.postLog.filter((r) => r.endpoint === EP_SEND_MESSAGE).at(-1)!
					.payload["msg"] as Record<string, unknown>
			)["item_list"] as Array<Record<string, unknown>>
		)[0]!;
		expect(voiceItem["type"]).toBe(ITEM_VOICE);
		const voiceBody = voiceItem["voice_item"] as Record<string, unknown>;
		expect(voiceBody["encode_type"]).toBe(6);
		expect(voiceBody["sample_rate"]).toBe(24000);
		expect(voiceBody["bits_per_sample"]).toBe(16);

		const pdf = await w.engine.sendFile("u_k", {
			filename: "report.pdf",
			plaintext: Buffer.from("%PDF-1.4 fake"),
		});
		expect(pdf.success).toBe(true);
		const fileUp = w.server.getUploadUrlCalls.at(-1)!;
		expect(fileUp.media_type).toBe(MEDIA_FILE);
		const fileItem = (
			(
				w.server.postLog.filter((r) => r.endpoint === EP_SEND_MESSAGE).at(-1)!
					.payload["msg"] as Record<string, unknown>
			)["item_list"] as Array<Record<string, unknown>>
		)[0]!;
		expect(fileItem["type"]).toBe(ITEM_FILE);
		const fileBody = fileItem["file_item"] as Record<string, unknown>;
		expect(fileBody["file_name"]).toBe("report.pdf");
		expect(fileBody["len"]).toBe(String(Buffer.from("%PDF-1.4 fake").length));
	});

	it("an optional caption precedes the media item as its own text message (vendor order)", async () => {
		const w = await connectedWorld("wx-cn2-caption");
		const res = await w.engine.sendFile("u_cap", {
			filename: "pic.jpg",
			plaintext: Buffer.from("bits"),
			caption: "see attached",
		});
		expect(res.success).toBe(true);
		const sends = w.server.postLog.filter(
			(r) => r.endpoint === EP_SEND_MESSAGE,
		);
		expect(sends.length).toBe(2);
		const first = sends[0]!.payload["msg"] as Record<string, unknown>;
		const firstItems = first["item_list"] as Array<Record<string, unknown>>;
		expect(
			(firstItems[0]!["text_item"] as Record<string, unknown>)["text"],
		).toBe("see attached");
		const second = sends[1]!.payload["msg"] as Record<string, unknown>;
		const secondItems = second["item_list"] as Array<Record<string, unknown>>;
		expect(secondItems[0]!["type"]).toBe(ITEM_IMAGE);
	});

	it("getuploadurl vendor error aborts BEFORE any CDN leg or sendmessage", async () => {
		const w = await connectedWorld("wx-cn2-uperr");
		w.server.scriptGetUploadUrl({ ret: -1 });
		const res = await w.engine.sendFile("u_e", {
			filename: "x.png",
			plaintext: Buffer.from("data"),
		});
		expect(res.success).toBe(false);
		expect(res.error).toContain("getuploadurl error: ret=-1");
		expect(w.server.cdnUploadCalls.length).toBe(0);
		expect(w.server.postLog.some((r) => r.endpoint === EP_SEND_MESSAGE)).toBe(
			false,
		);
	});

	it("CDN non-200 fails loudly; a 200 WITHOUT x-encrypted-param fails loudly too", async () => {
		const w1 = await connectedWorld("wx-cn2-cdn403");
		w1.server.scriptCdnUpload({ status: 403 });
		const r403 = await w1.engine.sendFile("u_c1", {
			filename: "x.png",
			plaintext: Buffer.from("data"),
		});
		expect(r403.success).toBe(false);
		expect(r403.error).toContain("CDN upload HTTP 403");
		expect(w1.server.postLog.some((r) => r.endpoint === EP_SEND_MESSAGE)).toBe(
			false,
		);

		const w2 = await connectedWorld("wx-cn2-cdnnoparam");
		w2.server.scriptCdnUpload({ status: 200, encryptedParam: null });
		const rNoParam = await w2.engine.sendFile("u_c2", {
			filename: "x.png",
			plaintext: Buffer.from("data"),
		});
		expect(rNoParam.success).toBe(false);
		expect(rNoParam.error).toContain("x-encrypted-param");
	});
});

describe("cn-2 — outbound media classification matches _outbound_media_builder", () => {
	it("mime-prefix rules (wx-6): image/*→image, video/*→video, .silk→voice, audio/unknown→file", () => {
		expect(outboundMediaKind("a.png")).toBe("image");
		expect(outboundMediaKind("b.JPG")).toBe("image");
		expect(outboundMediaKind("c.mp4")).toBe("video");
		expect(outboundMediaKind("d.silk")).toBe("voice");
		expect(outboundMediaKind("d.silk", true)).toBe("file"); // force_file parity
		expect(outboundMediaKind("e.mp3")).toBe("file"); // audio/* non-.silk ⇒ FILE
		expect(outboundMediaKind("f.bin")).toBe("file");
	});

	it("mimetypes.guess_type parity: svg/tiff/ico/avif are image/* — NOT a fixed extension list", () => {
		// Hermes classifies by mime PREFIX; every image/* extension ships the
		// MEDIA_IMAGE mid_size item upstream even though the old fixed list
		// missed it.
		expect(outboundMediaKind("logo.svg")).toBe("image");
		expect(outboundMediaKind("scan.tiff")).toBe("image");
		expect(outboundMediaKind("scan.tif")).toBe("image");
		expect(outboundMediaKind("favicon.ico")).toBe("image");
		expect(outboundMediaKind("pic.heif")).toBe("image");
		expect(outboundMediaKind("pic.avif")).toBe("image");
		// video/* prefix coverage beyond the old fixed set.
		expect(outboundMediaKind("clip.m4v")).toBe("video");
		expect(outboundMediaKind("clip.3gp")).toBe("video");
		// force_file_attachment gates ONLY the .silk leg (vendor ORDER:
		// image/video checks precede it) — a forced image still ships IMAGE.
		expect(outboundMediaKind("a.png", true)).toBe("image");
		expect(outboundMediaKind("c.mp4", true)).toBe("video");
		// extension match is case-insensitive and suffix-anchored.
		expect(outboundMediaKind("IMG.SVG")).toBe("image");
		expect(outboundMediaKind("archive.svgz")).toBe("file"); // not .svg
	});

	it("item builders carry encrypt_type 1 and vendor size fields", () => {
		const p = {
			encryptQueryParam: "enc-x",
			aesKeyApi: aesKeyForApi("ab".repeat(16)),
			ciphertextSize: 48,
			plaintextSize: 33,
			filename: "n.bin",
			rawfilemd5: "md5hex",
		};
		const video = buildOutboundMediaItem("video", p);
		expect((video["video_item"] as Record<string, unknown>)["video_size"]).toBe(
			48,
		);
		expect((video["video_item"] as Record<string, unknown>)["video_md5"]).toBe(
			"md5hex",
		);
		const file = buildOutboundMediaItem("file", p);
		expect((file["file_item"] as Record<string, unknown>)["len"]).toBe("33");
		for (const kind of ["image", "video", "voice", "file"] as const) {
			const item = buildOutboundMediaItem(kind, p);
			const bodyName = `${kind}_item`;
			const media = (item[bodyName] as Record<string, unknown>)[
				"media"
			] as Record<string, unknown>;
			expect(media["encrypt_type"]).toBe(1);
			expect(media["aes_key"]).toBe(p.aesKeyApi);
			expect(media["encrypt_query_param"]).toBe("enc-x");
		}
	});
});

// ── cn-5 ─────────────────────────────────────────────────────────────────────

describe("cn-5 — turn-scoped typing signals on refreshed tickets", () => {
	it("a dispatched turn brackets with sendtyping status START then STOP", async () => {
		const w = await connectedWorld("wx-cn5");
		w.server.pushMessage(wxText("t-1", "u_t5", "hi"));
		await pump(w, () => w.engine.turnLog.length >= 1);

		const calls = w.server.sendTypingCalls.filter(
			(c) => c.ilink_user_id === "u_t5",
		);
		expect(calls.length).toBeGreaterThanOrEqual(2);
		expect(calls[0]!.status).toBe(TYPING_START);
		expect(calls[1]!.status).toBe(TYPING_STOP);
		for (const c of calls) {
			// Byte shapes: ticket from getConfig, status ∈ {1,2}, conforming plane.
			expect(c.typing_ticket).toBe(w.server.typingTicket);
			expect([TYPING_START, TYPING_STOP]).toContain(c.status);
			expect(c.base_info).toEqual(baseInfo());
			expect(c.headers["AuthorizationType"]).toBe("ilink_bot_token");
			expect(c.headers["Authorization"]).toBe(`Bearer ${w.engine.token}`);
		}
	});

	it("the TTL cache avoids refetch inside 600s; expiry refetches (stuck-indicator guard)", async () => {
		const w = await connectedWorld("wx-cn5b");
		w.engine.sendTypingIndicator("u_ttl"); // fetch #1
		w.engine.stopTypingIndicator("u_ttl"); // cached
		expect(w.server.getConfigCalls.length).toBe(1);
		w.engine.sendTypingIndicator("u_ttl"); // still cached
		expect(w.server.getConfigCalls.length).toBe(1);

		await w.clock.advance((TYPING_TICKET_TTL_S + 5) * 1000);
		w.engine.sendTypingIndicator("u_ttl"); // expired → REFRESH
		expect(w.server.getConfigCalls.length).toBe(2);

		const statuses = w.server.sendTypingCalls.map((c) => c.status);
		expect(statuses).toEqual([
			TYPING_START,
			TYPING_STOP,
			TYPING_START,
			TYPING_START,
		]);
	});
});

// ── cn-9 ─────────────────────────────────────────────────────────────────────

/**
 * Drive a qrLogin promise against the INJECTED clock: each advance(1s)
 * releases one poll-interval sleep, mirroring the vendor 1s cadence.
 */
async function driveQrLogin<T>(
	clock: ManualClock,
	run: () => Promise<T>,
	maxSeconds = 120,
): Promise<T> {
	let done = false;
	const p = run().then((v) => {
		done = true;
		return v;
	});
	for (let i = 0; i < maxSeconds && !done; i++) {
		await clock.advance(1_000);
		await new Promise<void>((r) => setTimeout(r, 0));
	}
	if (!done) throw new Error("qrLogin did not finish within the driven clock");
	return await p;
}

describe("cn-9 — qr_login two-endpoint poll loop", () => {
	it("fetches get_bot_qrcode then polls get_qrcode_status to confirmed credentials", async () => {
		const w = makeWXWorld({ name: "wx-cn9" }); // no poll loop needed
		const { engine, server } = w;
		server.scriptQrStatusResponse(
			{ status: "wait" },
			{ status: "scaned" },
			{
				status: "confirmed",
				ilink_bot_id: "bot-1",
				bot_token: "tok-1",
				baseurl: "https://alt.example",
				ilink_user_id: "u-1",
			},
		);
		const creds = await driveQrLogin(w.clock, () =>
			engine.qrLogin({ botType: QR_DEFAULT_BOT_TYPE, timeoutSeconds: 30 }),
		);
		expect(creds).toEqual({
			account_id: "bot-1",
			token: "tok-1",
			base_url: "https://alt.example",
			user_id: "u-1",
		});
		expect(server.qrCodeRequests.length).toBe(1);
		expect(server.qrCodeRequests[0]!.bot_type).toBe(QR_DEFAULT_BOT_TYPE);
		expect(server.qrCodeRequests[0]!.baseUrl).toBe(ILINK_BASE_URL);
		// _api_get parity: app identity ONLY — never Bearer/body auth headers.
		expect(server.qrCodeRequests[0]!.headers["iLink-App-Id"]).toBe(
			ILINK_APP_ID,
		);
		expect(server.qrCodeRequests[0]!.headers["iLink-App-ClientVersion"]).toBe(
			String(ILINK_APP_CLIENT_VERSION),
		);
		expect(server.qrCodeRequests[0]!.headers["Authorization"]).toBeUndefined();
		expect(server.qrStatusRequests.length).toBeGreaterThanOrEqual(3);
		expect(server.qrStatusRequests[0]!.qrcode).toMatch(/^qr-/);
	});

	it("scaned_but_redirect repoints subsequent status polls at the redirect host", async () => {
		const w = makeWXWorld({ name: "wx-cn9-redirect" });
		const { engine, server } = w;
		server.scriptQrStatusResponse(
			{ status: "scaned_but_redirect", redirect_host: "redirect.example" },
			{
				status: "confirmed",
				ilink_bot_id: "bot-2",
				bot_token: "tok-2",
				ilink_user_id: "u-2",
			},
		);
		const creds = await driveQrLogin(w.clock, () =>
			engine.qrLogin({ timeoutSeconds: 30 }),
		);
		expect(creds?.account_id).toBe("bot-2");
		expect(server.qrStatusRequests[0]!.baseUrl).toBe(ILINK_BASE_URL);
		expect(server.qrStatusRequests.at(-1)!.baseUrl).toBe(
			"https://redirect.example",
		);
		// confirmed without baseurl falls back to the canonical ILINK base URL.
		expect(creds?.base_url).toBe(ILINK_BASE_URL);
	});

	it("expired QR refetches up to THREE times, then fails (refresh cap)", async () => {
		const w = makeWXWorld({ name: "wx-cn9-expired" });
		const { engine, server } = w;
		server.scriptQrStatusResponse(
			{ status: "expired" },
			{ status: "expired" },
			{ status: "expired" },
			{ status: "expired" }, // fourth expiry exceeds the cap
		);
		const result = await driveQrLogin(w.clock, () =>
			engine.qrLogin({ timeoutSeconds: 60 }),
		);
		expect(result).toBeNull();
		// initial fetch + one refetch per tolerated expiry (3) = 4 total.
		expect(server.qrCodeRequests.length).toBe(4);
		expect(server.qrStatusRequests.length).toBe(4);
	});

	it("confirmed WITHOUT complete credentials fails closed; missing qrcode aborts pre-poll", async () => {
		const w1 = makeWXWorld({ name: "wx-cn9-incomplete" });
		w1.server.scriptQrStatusResponse({
			status: "confirmed",
			ilink_bot_id: "bot-3",
			// bot_token MISSING
		});
		const incomplete = await driveQrLogin(w1.clock, () => w1.engine.qrLogin());
		expect(incomplete).toBeNull();

		const w2 = makeWXWorld({ name: "wx-cn9-noqr" });
		w2.server.scriptQrCodeResponse({}); // no qrcode field
		const noQr = await w2.engine.qrLogin();
		expect(noQr).toBeNull();
		expect(w2.server.qrStatusRequests.length).toBe(0);
	});

	it("returns null past the deadline when the code never confirms (all-wait polling)", async () => {
		const w = makeWXWorld({ name: "wx-cn9-timeout" });
		const { engine, server } = w; // unscripted statuses answer {status:"wait"}
		const result = await driveQrLogin(
			w.clock,
			() => engine.qrLogin({ timeoutSeconds: 5 }),
			30,
		);
		expect(result).toBeNull();
		// ~one poll per elapsed second before the deadline.
		expect(server.qrStatusRequests.length).toBeGreaterThanOrEqual(4);
	});
});

// ══ stability round 2 — cluster weixin-r2 (wx-1..wx-7) ══════════════════════

/** Raw adapter with vendor defaults (no subject harness-scale override). */
function rawAdapter(
	opts: Partial<ConstructorParameters<typeof WeixinAdapter>[0]> = {},
): WeixinAdapter {
	return new WeixinAdapter({
		token: "tok",
		server: new FakeILinkServer(),
		syncStore: {
			load: () => "",
			save: () => {},
		} as WeixinSyncStore,
		...opts,
	});
}

const settle = () => new Promise<void>((r) => setTimeout(r, 0));

/**
 * Drive a promise against the INJECTED clock: advances in small steps until
 * the operation settles (retry/pause ladders ride the injected sleeps).
 */
async function driven<T>(
	w: WXWorld,
	run: () => Promise<T>,
	maxMs = 120_000,
): Promise<T> {
	let done = false;
	const p = run().then((v) => {
		done = true;
		return v;
	});
	let walked = 0;
	while (!done && walked < maxMs) {
		await w.clock.advance(250);
		walked += 250;
		await settle();
	}
	if (!done)
		throw new Error("operation did not settle within the driven clock");
	return await p;
}

describe("round-2 wx-1 — generic vendor errors retry with linear backoff", () => {
	it("a scripted ret=-5 retries ONCE more after SEND_CHUNK_RETRY_DELAY_S*(attempt+1) and recovers", async () => {
		const w = await connectedWorld("wx-r2-generic-retry");
		w.server.scriptSendMessage(-5);
		const results = await driven(w, () =>
			w.engine.deliverText("u_g", "retry me"),
		);
		expect(results[results.length - 1]?.success).toBe(true); // Hermes raises into the retry loop — recoverable
		expect(
			w.server.sendCalls.filter((c) => c.to_user_id === "u_g"),
		).toHaveLength(2);
	});

	it("persistent -5 is terminal only AFTER the ladder: 4 retries, delay*(attempt+1) backoff", async () => {
		const w = await connectedWorld("wx-r2-generic-terminal");
		const sleeps: number[] = [];
		const clockSleep = w.clock.sleepMs.bind(w.clock);
		(
			w.engine as unknown as { sleepFn: (ms: number) => Promise<void> }
		).sleepFn = async (ms: number) => {
			sleeps.push(ms);
			await clockSleep(ms);
		};
		for (let i = 0; i <= SEND_CHUNK_RETRIES; i++) {
			w.server.scriptSendMessage(-5, undefined, "boom");
		}
		const failed = await driven(w, () =>
			w.engine.deliverText("u_g2", "doomed"),
		);
		const fail = failed[failed.length - 1] ?? { success: true };
		expect(fail.success).toBe(false);
		expect(fail.error).toBe(
			"iLink sendmessage error: ret=-5 errcode=0 errmsg=boom",
		);
		expect(
			w.server.sendCalls.filter((c) => c.to_user_id === "u_g2"),
		).toHaveLength(SEND_CHUNK_RETRIES + 1);
		expect(sleeps).toEqual([
			SEND_CHUNK_RETRY_DELAY_S * 1000,
			SEND_CHUNK_RETRY_DELAY_S * 2000,
			SEND_CHUNK_RETRY_DELAY_S * 3000,
			SEND_CHUNK_RETRY_DELAY_S * 4000,
		]);
	});
});

describe("round-2 wx-2 — stale-session (-2 + 'unknown error') joins the -14 family", () => {
	it("poll site: pauses 600s like -14 (sessionExpiredStreak), NOT the generic failure ladder", async () => {
		const w = await connectedWorld("wx-r2-stale-poll");
		const genBefore = w.engine.generation;

		w.server.scriptGetUpdates({
			kind: "code",
			ret: -2,
			errmsg: "unknown error",
		});
		for (let i = 0; i < 140 && w.engine.sessionExpiredStreak < 1; i++) {
			await w.clock.advance(5_000);
			await settle();
		}
		// The stale signature took the SESSION-EXPIRED branch (streak + pause),
		// never the consecutive-failures ladder and never a rate-limit path.
		expect(w.engine.sessionExpiredStreak).toBe(1);
		expect(w.engine.generation).toBe(genBefore);

		// Strike 2 escalates per DEC-045 (recycle = generation bump).
		w.server.scriptGetUpdates({
			kind: "code",
			ret: -2,
			errmsg: "unknown error",
		});
		for (let i = 0; i < 140 && w.engine.generation <= genBefore; i++) {
			await w.clock.advance(5_000);
			await settle();
		}
		expect(w.engine.generation).toBeGreaterThan(genBefore);
	});

	it("poll site contrast: a PLAIN -2 rate limit does NOT join the session-expired family", async () => {
		const w = await connectedWorld("wx-r2-plain-limit");
		w.server.scriptGetUpdates({ kind: "code", ret: -2 });
		await w.clock.advance(BACKOFF_DELAY_SECONDS * 1000);
		await settle();
		await w.clock.advance(1_000);
		await settle();
		expect(w.engine.sessionExpiredStreak).toBe(0);
	});

	it("send site: strips context_token and retries tokenless; breaker NOT fed", async () => {
		const w = await connectedWorld("wx-r2-stale-send");
		w.engine.contextTokens.set("u_st", "ctx-tok-9");
		w.server.scriptSendMessage(-2, undefined, "unknown error");
		const results = await driven(w, () =>
			w.engine.deliverText("u_st", "tokenless plz"),
		);
		expect(results[results.length - 1]?.success).toBe(true);
		const calls = w.server.sendCalls.filter((c) => c.to_user_id === "u_st");
		expect(calls.length).toBeGreaterThanOrEqual(2);
		expect(calls[0]?.context_token).toBe("ctx-tok-9");
		expect(calls[1]?.context_token ?? "").toBe("");
		expect(w.engine.contextTokens.has("u_st")).toBe(false);
		// A dead session is NOT a genuine rate limit: no breaker cooldown.
		expect(w.engine.rateLimitCooldownRemaining()).toBe(0);
	});
});

describe("round-2 wx-3 — adaptive long-poll budget; benign timeout cycles", () => {
	it("the server-suggested longpolling_timeout_ms becomes the NEXT pull budget", async () => {
		const w = await connectedWorld("wx-r2-budget");
		expect(w.engine.longPollTimeoutBudgetMs).toBe(35_000); // LONG_POLL_TIMEOUT_MS
		w.server.longPollingTimeoutMsOverride = 50_000;
		w.server.pushMessage(wxText("b-1", "u_b", "budget probe"));
		await pump(w, () => w.engine.longPollTimeoutBudgetMs === 50_000);
		expect(w.engine.longPollTimeoutBudgetMs).toBe(50_000);
	});

	it("an over-budget probe is a BENIGN empty cycle: zero penalty, no recycle, no fatal", async () => {
		const w = await connectedWorld("wx-r3-benign");
		const genBefore = w.engine.generation;
		w.server.holdUpdates();
		for (let i = 0; i < 6; i++) {
			await w.clock.advance(40_000);
			await settle();
		}
		expect(w.engine.pollLog.includes("timeout")).toBe(true);
		expect(w.engine.generation).toBe(genBefore);
		expect(w.engine.lifecycle.statusSnapshot().state).not.toBe("fatal");

		// Recovery: releasing the hold lets messages flow immediately.
		w.server.releaseUpdates();
		w.server.pushMessage(wxText("bn-1", "u_bn", "flows again"));
		await pump(w, () =>
			w.engine.turnLog.some((t) => t.includes("flows again")),
		);
	});
});

describe("round-2 wx-4 — WX_MAX_MESSAGE_LENGTH=2000 default split budget", () => {
	it("the manifest constant is vendor truth; adapters WITHOUT an override resolve it", () => {
		expect(WX_MAX_MESSAGE_LENGTH).toBe(2000); // weixin.py MAX_MESSAGE_LENGTH
		const adapter = rawAdapter();
		expect(adapter.chatLengthPolicyForChat("any-chat").maxUnits).toBe(2000);
	});

	it("an explicit scalarMaxUnits still wins (harness-scale subjects stay at 64)", () => {
		const scaled = rawAdapter({ scalarMaxUnits: 64 });
		expect(scaled.chatLengthPolicyForChat("any-chat").maxUnits).toBe(64);
	});
});

describe("round-2 wx-5 — egress ships format_message parity then delivery-unit splitting", () => {
	it("blank-run collapse rides format_message parity BEFORE splitting", async () => {
		const w = await connectedWorld("wx-r5-format");
		await driven(w, () =>
			w.engine.deliverText("u_f", "**Header**\n\n\n\nBody text here"),
		);
		expect(w.server.sendCalls.at(-1)?.text).toBe(
			"**Header**\n\nBody text here",
		);
	});

	it("chatty multiline under budget splits into UNLABELED bubbles (compact mode)", async () => {
		const w = await connectedWorld("wx-r5-bubbles");
		const before = w.server.sendCalls.length;
		await driven(w, () =>
			w.engine.deliverText("u_fb", "how are you?\nfine thanks\nand you?"),
		);
		const bubbles = w.server.sendCalls.slice(before).map((c) => c.text);
		expect(bubbles).toEqual(["how are you?", "fine thanks", "and you?"]);
	});

	it("oversized single block overflows through THE fence-carry chunker with (i/n)", async () => {
		const w = await connectedWorld("wx-r5-overflow");
		const long = Array.from({ length: 30 }, (_, i) => `line-${i} filler`).join(
			"\n",
		);
		const before = w.server.sendCalls.length;
		const results = await driven(w, () => w.engine.deliverText("u_fo", long));
		expect(results.every((r) => r.success)).toBe(true);
		const units = w.server.sendCalls.slice(before);
		expect(units.length).toBeGreaterThan(1);
		units.forEach((c, idx) => {
			expect(c.text.endsWith(`(${idx + 1}/${units.length})`)).toBe(true);
			expect(c.text.length).toBeLessThanOrEqual(64);
		});
		// Overflow splits prefer newlines but may fall back to spaces
		// (base.truncate_message parity): every TOKEN survives, in order.
		const rebuilt = units
			.map((c) => c.text.replace(/\s*\(\d+\/\d+\)$/, ""))
			.join("\n")
			.replace(/\s+/g, " ");
		for (let i = 0; i < 30; i++) {
			expect(rebuilt).toContain(`line-${i} filler`);
		}
	});

	it("the 120-col copy-friendly wrap applies to egress text (format_message parity)", async () => {
		const srv = new FakeILinkServer();
		const adapter = rawAdapter({ scalarMaxUnits: 500, server: srv });
		const prose = Array.from({ length: 40 }, () => "word").join(" ");
		await adapter.deliverText("chat-w", prose);
		const sent = srv.sendCalls.at(-1)?.text ?? "";
		expect(sent).toContain("\n"); // wrapped, not one long line
		for (const line of sent.split("\n")) {
			expect(line.length).toBeLessThanOrEqual(WEIXIN_COPY_LINE_WIDTH);
		}
	});
});

describe("round-2 wx-7 — open DM intake behind allow-all opt-ins", () => {
	it("dm 'open' denies by default and admits ONLY behind the env opt-ins", () => {
		const env: Record<string, string | undefined> = {};
		const mk = () => rawAdapter({ dmPolicy: "open", readEnv: (k) => env[k] });
		expect(mk().isDmIntakeAllowed("u9")).toBe(false);
		env.GATEWAY_ALLOW_ALL_USERS = "true";
		expect(mk().isDmIntakeAllowed("u9")).toBe(true);
		delete env.GATEWAY_ALLOW_ALL_USERS;
		env.WEIXIN_ALLOW_ALL_USERS = "yes";
		expect(mk().isDmIntakeAllowed("u9")).toBe(true);
		env.WEIXIN_ALLOW_ALL_USERS = "1";
		expect(mk().isDmIntakeAllowed("u9")).toBe(true);
		env.WEIXIN_ALLOW_ALL_USERS = "TRUE";
		expect(mk().isDmIntakeAllowed("u9")).toBe(true);
		env.WEIXIN_ALLOW_ALL_USERS = "no";
		expect(mk().isDmIntakeAllowed("u9")).toBe(false);
	});

	it("pairing/disabled policies are untouched by the opt-ins", () => {
		const env: Record<string, string | undefined> = {
			GATEWAY_ALLOW_ALL_USERS: "true",
		};
		const pairing = rawAdapter({
			dmPolicy: "pairing",
			readEnv: (k) => env[k],
		});
		expect(pairing.isDmIntakeAllowed("u9")).toBe(true); // pairing admits anyway
		const disabled = rawAdapter({
			dmPolicy: "disabled",
			readEnv: (k) => env[k],
		});
		expect(disabled.isDmIntakeAllowed("u9")).toBe(false); // opt-in cannot revive disabled
		const allowlist = rawAdapter({
			dmPolicy: "allowlist",
			allowFrom: ["friend"],
			readEnv: (k) => env[k],
		});
		expect(allowlist.isDmIntakeAllowed("friend")).toBe(true);
		expect(allowlist.isDmIntakeAllowed("stranger")).toBe(false);
	});

	it("integration: an open-policy engine admits an unknown sender once opted in", async () => {
		const w = makeWXWorld({ name: "wx-r2-open", dmPolicy: "open" });
		await w.connectAndAwaitLive();

		// Default (no opt-in): the stranger is dropped silently.
		w.server.pushMessage(wxText("o-0", "u_stranger", "knock once"));
		await w.clock.advance(TEXT_BATCH_DELAY_S * 1000 + 1_000);
		await settle();
		expect(w.engine.turnLog.some((t) => t.includes("knock once"))).toBe(false);

		process.env.GATEWAY_ALLOW_ALL_USERS = "true";
		try {
			w.server.pushMessage(wxText("o-1", "u_stranger", "knock again"));
			await pump(w, () =>
				w.engine.turnLog.some((t) => t.includes("knock again")),
			);
			expect(w.engine.turnLog.some((t) => t.includes("knock again"))).toBe(
				true,
			);
		} finally {
			delete process.env.GATEWAY_ALLOW_ALL_USERS;
		}
	});
});
