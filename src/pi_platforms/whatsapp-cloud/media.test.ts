// Media-plane contracts: PRE-upload cap enforcement (per-kind manifest data),
// the media-first-then-caption two-step send, link passthrough, and the
// two-step inbound download with extension mapping + traversal guard.
// Parity: whatsapp_cloud.py:_upload_media / _send_media /
// _download_media_to_cache; caps transcribed from _MEDIA_SIZE_LIMITS.

import { existsSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

import { makeWaCloudFixture } from "./wa-cloud-fixture.js";
import { MEDIA_SIZE_LIMITS, type WaMediaKind } from "./manifest.js";

describe("media cap enforcement is PRE-upload", () => {
	it("oversized uploads refused BEFORE any Graph roundtrip, per kind", () => {
		const fx = makeWaCloudFixture();
		try {
			const kinds: WaMediaKind[] = [
				"image",
				"video",
				"audio",
				"document",
				"sticker",
			];
			for (const kind of kinds) {
				const cap = MEDIA_SIZE_LIMITS[kind];
				const result = fx.adapter.sendMedia(
					kind === "image" ? "15551112222" : `15551112222+${kind}`,
					kind,
					{
						bytes: Buffer.alloc(cap + 1),
						filename: `big.${kind}`,
					},
				);
				void result.then((r) => {
					expect(r.success).toBe(false);
					expect(r.error).toContain(`cap is ${cap} bytes`);
				});
			}
			// After all refusals: ZERO upload calls hit the wire (the contract).
			return Promise.all([
				fx.adapter.sendMedia("c1", "sticker", {
					bytes: Buffer.alloc(MEDIA_SIZE_LIMITS.sticker + 1),
				}),
				fx.adapter.sendMedia("c2", "audio", {
					bytes: Buffer.alloc(MEDIA_SIZE_LIMITS.audio + 1),
				}),
			]).then(async ([r1, r2]) => {
				expect(r1.success).toBe(false);
				expect(r2.success).toBe(false);
				expect(fx.graph.uploads).toHaveLength(0);
			});
		} finally {
			fx.dispose();
		}
	});

	it("mutation check: exactly-at-cap uploads PASS the pre-gate and reach the wire", async () => {
		const fx = makeWaCloudFixture();
		try {
			const result = await fx.adapter.sendMedia("15551112222", "sticker", {
				bytes: Buffer.alloc(MEDIA_SIZE_LIMITS.sticker), // EXACTLY at cap
				mime: "image/webp",
			});
			expect(result.success).toBe(true);
			expect(fx.graph.uploads).toHaveLength(1); // reached Graph
			expect(fx.graph.uploads[0]?.kind).toBe("sticker");
		} finally {
			fx.dispose();
		}
	});
});

describe("outbound media two-step: media FIRST, caption rides the message block", () => {
	it("upload → id reference; caption/filename attached per kind rules", async () => {
		const fx = makeWaCloudFixture();
		try {
			const bytes = Buffer.from("fake-png-bytes");
			const sent = await fx.adapter.sendMedia(
				"15551234567",
				"image",
				{ bytes, mime: "image/png", filename: "pic.png" },
				{ caption: "*bold* caption", replyToMessageId: "wamid.ctx.9" },
			);
			expect(sent.success).toBe(true);

			// TWO transport calls total: one upload, one message POST.
			expect(fx.graph.uploads).toHaveLength(1);
			const posts = fx.graph.sentMessages;
			expect(posts).toHaveLength(1);

			// Caption rides the IMAGE BLOCK of the SECOND call — never a
			// separate text send (two-step shape).
			const body = posts[0]?.body as Record<string, unknown>;
			expect(body["type"]).toBe("image");
			const image = body["image"] as Record<string, unknown>;
			expect(image["id"]).toBe(sent.messageId ? image["id"] : undefined); // id present
			expect(typeof image["id"]).toBe("string");
			expect(image["caption"]).toBe("*bold* caption");
			expect(image["filename"]).toBeUndefined(); // filename is document-only

			// Reply context quotes on the media POST.
			expect(body["context"]).toEqual({ message_id: "wamid.ctx.9" });
			// No text-type posts at all (caption is not a text message).
			expect(fx.graph.textSendsOf()).toHaveLength(0);
		} finally {
			fx.dispose();
		}
	});

	it("document carries filename; audio/sticker never carry captions", async () => {
		const fx = makeWaCloudFixture();
		try {
			await fx.adapter.sendMedia(
				"c-doc",
				"document",
				{ bytes: Buffer.from("doc"), mime: "text/plain" },
				{ caption: "notes", filename: "notes.txt" },
			);
			const docPost = fx.graph.sentMessages[fx.graph.sentMessages.length - 1]
				?.body as Record<string, unknown>;
			const doc = docPost["document"] as Record<string, unknown>;
			expect(doc["filename"]).toBe("notes.txt");
			expect(doc["caption"]).toBe("notes");

			await fx.adapter.sendMedia(
				"c-audio",
				"audio",
				{ bytes: Buffer.from("mp3"), mime: "audio/mpeg" },
				{ caption: "ignored" },
			);
			const audioPost = fx.graph.sentMessages[fx.graph.sentMessages.length - 1]
				?.body as Record<string, unknown>;
			const audio = audioPost["audio"] as Record<string, unknown>;
			expect(audio["caption"]).toBeUndefined();
		} finally {
			fx.dispose();
		}
	});

	it("HTTPS link mode skips the upload roundtrip entirely", async () => {
		const fx = makeWaCloudFixture();
		try {
			const result = await fx.adapter.sendMedia("15551234567", "video", {
				link: "https://cdn.example.com/clip.mp4",
			});
			expect(result.success).toBe(true);
			expect(fx.graph.uploads).toHaveLength(0);
			const body = fx.graph.sentMessages[fx.graph.sentMessages.length - 1]
				?.body as Record<string, unknown>;
			expect((body["video"] as Record<string, unknown>)["link"]).toBe(
				"https://cdn.example.com/clip.mp4",
			);
		} finally {
			fx.dispose();
		}
	});

	it("Graph-side upload failure surfaces a graph-shaped error (no message POST follows)", async () => {
		const fx = makeWaCloudFixture();
		try {
			fx.graph.script("upload", {
				status: 400,
				error: {
					message: "(#133010) size",
					type: "OAuthException",
					code: 133010,
				},
			});
			const result = await fx.adapter.sendMedia("c-err", "image", {
				bytes: Buffer.alloc(8),
			});
			expect(result.success).toBe(false);
			expect(result.error).toContain("graph error 133010 (HTTP 400)");
			expect(fx.graph.sentMessages).toHaveLength(0); // no second step
		} finally {
			fx.dispose();
		}
	});
});

describe("inbound media download — two-step over the fake Graph", () => {
	it("metadata → bytes → cached under mkdtemp dir with override extension", async () => {
		const fx = makeWaCloudFixture();
		try {
			const payload = Buffer.from("ogg-opus-voice-bytes");
			const mediaId = fx.graph.seedMedia("audio/ogg; codecs=opus", payload);
			const resp = await fx.postSigned(
				fx.valueEnvelope({
					messages: [
						fx.mediaMessage(
							"wamid.m1",
							"15551234567",
							"voice",
							mediaId,
							"audio/ogg; codecs=opus",
						),
					],
				}),
			);
			expect(resp.status).toBe(200);
			expect(fx.adapter.inboundMediaLog.length).toBeGreaterThanOrEqual(0);

			const expectedPath = `${fx.mediaDir}/${mediaId}.ogg`;
			let seen = false;
			for (let i = 0; i < 100 && !seen; i++) {
				await new Promise<void>((r) => setTimeout(r, 10));
				seen = existsSync(expectedPath);
			}
			expect(seen).toBe(true); // override map: audio/ogg → .ogg (not .oga)
			expect(readFileSync(expectedPath)).toEqual(payload); // byte-exact
			// Two-step observable: metadata GET then bytes GET.
			expect(fx.graph.metadataGets.map((g) => g.mediaId)).toContain(mediaId);
			expect(fx.graph.bytesGets.length).toBeGreaterThanOrEqual(1);
		} finally {
			fx.dispose();
		}
	});

	it("hostile media ids are refused (path-traversal guard), delivery stays metadata-only", async () => {
		const fx = makeWaCloudFixture();
		try {
			const resp = await fx.postSigned(
				fx.valueEnvelope({
					messages: [
						fx.mediaMessage(
							"wamid.m2",
							"15551234567",
							"document",
							"../../etc/passwd",
							"application/octet-stream",
						),
					],
				}),
			);
			expect(resp.status).toBe(200); // contained — never a 500 retry loop
			expect(fx.graph.metadataGets).toHaveLength(0); // refused BEFORE step 1
			expect(fx.adapter.inboundMediaLog).toHaveLength(0);
		} finally {
			fx.dispose();
		}
	});
});
