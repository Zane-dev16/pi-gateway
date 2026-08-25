// Media-plane contracts: PRE-upload cap enforcement (per-kind manifest data),
// the media-first-then-caption two-step send, link passthrough, and the
// two-step inbound download with extension mapping + traversal guard.
// Parity: whatsapp_cloud.py:_upload_media / _send_media /
// _download_media_to_cache; caps transcribed from _MEDIA_SIZE_LIMITS.

import { existsSync, readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";

import { makeWaCloudFixture } from "./wa-cloud-fixture.js";
import { MEDIA_SIZE_LIMITS, type WaMediaKind } from "./manifest.js";

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

	it("uploads carry the Meta-REQUIRED multipart fields: messaging_product='whatsapp' + mime-typed `type` (_upload_media parity)", async () => {
		const fx = makeWaCloudFixture();
		try {
			const result = await fx.adapter.sendMedia("15551112222", "image", {
				bytes: Buffer.from("png"),
				mime: "image/png",
				filename: "pic.png",
			});
			expect(result.success).toBe(true);
			const upload = fx.graph.uploads[0];
			expect(upload?.messagingProduct).toBe("whatsapp");
			expect(upload?.type).toBe("image/png");

			// The FAKE SERVER enforces the vendor gate: a transport caller that
			// omits messaging_product/type is rejected BY META before any id.
			const bad = await fx.graph.uploadMedia({
				kind: "document",
				bytes: Buffer.from("x"),
				mime: "application/pdf",
				filename: "doc.pdf",
				messagingProduct: "wrong-product",
				type: "application/pdf",
			});
			expect(bad.status).toBe(400);
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

	it("omitted filename derives a REAL name+extension from the mime (os.path.basename parity)", async () => {
		const fx = makeWaCloudFixture();
		try {
			// Override map: audio/ogg → .ogg.
			await fx.adapter.sendMedia("f-ogg", "audio", {
				bytes: Buffer.from("ogg"),
				mime: "audio/ogg",
			});
			expect(fx.graph.uploads.at(-1)?.filename).toBe("audio.ogg");

			// mimetypes-equivalent fallback: application/pdf → .pdf.
			await fx.adapter.sendMedia("f-pdf", "document", {
				bytes: Buffer.from("pdf"),
				mime: "application/pdf",
			});
			expect(fx.graph.uploads.at(-1)?.filename).toBe("document.pdf");

			// Default-kind mime: image → image/jpeg override → .jpg.
			await fx.adapter.sendMedia("f-img", "image", { bytes: Buffer.from("i") });
			expect(fx.graph.uploads.at(-1)?.filename).toBe("image.jpg");
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

	it("extension chain falls through the mimetypes-equivalent table BEFORE '.bin' (_ext_for_mime parity)", async () => {
		const fx = makeWaCloudFixture();
		try {
			// image/png is NOT in the override map — mimetypes fallback gives .png.
			const pngId = fx.graph.seedMedia("image/png", Buffer.from("png-bytes"));
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [
						fx.mediaMessage(pngId, "15551234567", "image", pngId, "image/png"),
					],
				}),
			);
			await new Promise<void>((r) => setTimeout(r, 40));
			expect(existsSync(`${fx.mediaDir}/${pngId}.png`)).toBe(true);

			// Unknown mime still lands on the '.bin' terminal default.
			const binId = fx.graph.seedMedia(
				"application/x-unknown-format",
				Buffer.from("mystery"),
			);
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [
						fx.mediaMessage(
							binId,
							"15551234567",
							"document",
							binId,
							"application/x-unknown-format",
						),
					],
				}),
			);
			await new Promise<void>((r) => setTimeout(r, 40));
			expect(existsSync(`${fx.mediaDir}/${binId}.bin`)).toBe(true);
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

// ── stability round 2: cached-media extension resolves hintMime-FIRST ────────

describe("cached-media extension resolves hintMime-FIRST (_download_media_to_cache @~1388 parity)", () => {
	it("divergent mimes: the webhook inner mime wins the cached extension, metadata only backfills", async () => {
		const fx = makeWaCloudFixture();
		try {
			// Graph metadata says video/mp4 (.mp4); the webhook inner mime says
			// opus-in-Ogg. The cache must carry .ogg so downstream STT/file
			// handling sees the REAL container (Hermes ext_hint precedence).
			const id = fx.graph.seedMedia("video/mp4", Buffer.from("voice-bytes"));
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [
						fx.mediaMessage(
							"wamid.h1",
							"15551234567",
							"voice",
							id,
							"audio/ogg; codecs=opus",
						),
					],
				}),
			);
			expect(
				await eventually(() => existsSync(`${fx.mediaDir}/${id}.ogg`)),
			).toBe(true);
			expect(existsSync(`${fx.mediaDir}/${id}.mp4`)).toBe(false); // never metadata-derived
		} finally {
			fx.dispose();
		}
	});

	it("unresolvable hint backfills from the Graph-metadata mime; both unknown land '.bin'", async () => {
		const fx = makeWaCloudFixture();
		try {
			// Hint unresolvable → metadata mime decides (_ext_for_mime chain).
			const a = fx.graph.seedMedia("image/png", Buffer.from("aaa"));
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [
						fx.mediaMessage(
							"wamid.h2",
							"15551234567",
							"document",
							a,
							"application/x-unknown-wire-mime",
						),
					],
				}),
			);
			expect(
				await eventually(() => existsSync(`${fx.mediaDir}/${a}.png`)),
			).toBe(true);

			// Neither side resolves → '.bin' terminal default stands.
			const b = fx.graph.seedMedia(
				"application/x-meta-unknown",
				Buffer.from("bbb"),
			);
			await fx.postSigned(
				fx.valueEnvelope({
					messages: [
						fx.mediaMessage(
							"wamid.h3",
							"15551234567",
							"document",
							b,
							"application/x-wire-unknown",
						),
					],
				}),
			);
			expect(
				await eventually(() => existsSync(`${fx.mediaDir}/${b}.bin`)),
			).toBe(true);
		} finally {
			fx.dispose();
		}
	});
});

// ── stability round 2: voice lane MP3→opus transcoder seam ──────────────────

describe("voice lane transcoder seam — MP3→opus pre-upload (send_voice @~1194 parity)", () => {
	/** Converter standing in for the ffmpeg shell-out: deterministic bytes. */
	const fakeConverter = async (bytes: Buffer, filename: string) =>
		filename.toLowerCase().endsWith(".mp3")
			? {
					bytes: Buffer.from(`opus(${bytes.toString("latin1")})`),
					filename: `${filename.slice(0, -4)}.ogg`,
				}
			: null;

	it("declared MP3 converts PRE-upload: converted bytes ride 'audio/ogg; codecs=opus'", async () => {
		const fx = makeWaCloudFixture({ transcodeMp3ToOpus: fakeConverter });
		try {
			const mp3 = Buffer.from("fake-mp3-bytes");
			const sent = await fx.adapter.sendMedia("15551234567", "audio", {
				bytes: mp3,
				filename: "tts.mp3",
				mime: "audio/mpeg",
			});
			expect(sent.success).toBe(true);

			// The UPLOAD carries the CONVERTED artifact with the voice-note mime
			// (multipart file part AND type field) — not the caller's bytes.
			const upload = fx.graph.uploads[0];
			expect(upload?.bytes).toEqual(Buffer.from("opus(fake-mp3-bytes)"));
			expect(upload?.mime).toBe("audio/ogg; codecs=opus");
			expect(upload?.type).toBe("audio/ogg; codecs=opus");
			expect(upload?.filename).toBe("tts.ogg");

			// One message POST references the uploaded id on its audio block.
			expect(fx.graph.sentMessages).toHaveLength(1);
			const body = fx.graph.sentMessages[0]?.body as Record<string, unknown>;
			expect(body["type"]).toBe("audio");
			expect(typeof (body["audio"] as Record<string, unknown>)["id"]).toBe(
				"string",
			);
		} finally {
			fx.dispose();
		}
	});

	it("conversion failure degrades to the MP3 attachment (audio/mpeg), never an error", async () => {
		const fx = makeWaCloudFixture({
			transcodeMp3ToOpus: async () => null, // ffmpeg missing / failed parity
		});
		try {
			const mp3 = Buffer.from("still-mp3");
			const sent = await fx.adapter.sendMedia("15551234567", "audio", {
				bytes: mp3,
				filename: "tts.mp3",
				mime: "audio/mpeg",
			});
			expect(sent.success).toBe(true); // fallback delivers
			const upload = fx.graph.uploads[0];
			expect(upload?.bytes).toEqual(mp3); // ORIGINAL bytes verbatim
			expect(upload?.mime).toBe("audio/mpeg");
			expect(upload?.type).toBe("audio/mpeg");
			expect(upload?.filename).toBe("tts.mp3");
		} finally {
			fx.dispose();
		}
	});

	it("undeclared audio bytes NEVER enter the lane: no seam call without caller-declared MP3", async () => {
		let seamCalls = 0;
		const fx = makeWaCloudFixture({
			transcodeMp3ToOpus: async (bytes, filename) => {
				seamCalls += 1;
				return fakeConverter(bytes, filename);
			},
		});
		try {
			// No filename / no mime → derived defaults look like MP3 but the
			// CALLER declared nothing: ship verbatim exactly as before.
			const ogg = Buffer.from("actual-ogg-opus-bytes");
			await fx.adapter.sendMedia("15551234567", "audio", {
				bytes: ogg,
				mime: "audio/ogg; codecs=opus",
			});
			expect(seamCalls).toBe(0);
			const upload = fx.graph.uploads[0];
			expect(upload?.bytes).toEqual(ogg);
			expect(upload?.type).toBe("audio/ogg; codecs=opus");
		} finally {
			fx.dispose();
		}
	});

	it("non-audio kinds never invoke the seam even with .mp3 names", async () => {
		let seamCalls = 0;
		const fx = makeWaCloudFixture({
			transcodeMp3ToOpus: async (bytes, filename) => {
				seamCalls += 1;
				return fakeConverter(bytes, filename);
			},
		});
		try {
			await fx.adapter.sendMedia("15551234567", "document", {
				bytes: Buffer.from("mp3-as-doc"),
				filename: "song.mp3",
				mime: "audio/mpeg",
			});
			expect(seamCalls).toBe(0); // send_voice is the VOICE lane only
			expect(fx.graph.uploads[0]?.kind).toBe("document");
		} finally {
			fx.dispose();
		}
	});
});
