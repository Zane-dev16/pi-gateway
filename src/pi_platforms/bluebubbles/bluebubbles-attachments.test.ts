// pi_platforms/bluebubbles/bluebubbles-attachments.test — attachment TRANSPORT
// behavior contracts (gateway/platforms/bluebubbles.py:_send_attachment @~470 +
// _download_attachment @~610 parity): the vendor multipart wire shape, the
// byte-exact upload/download round trip, the failure verdicts, and the
// post-stream rescan-lane bindings (DEC-019 explicit-tag delivery).

import { describe, expect, it } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
	type PostStreamAdapter,
	rescanPostStream,
} from "../../pi_gateway/outbound/post-stream-rescan.js";
import { BlueBubblesAdapter } from "./bluebubbles-adapter.js";
import { FakeBlueBubblesServer } from "./fake-server.js";
import {
	FIXTURE_BB_PASSWORD,
	FIXTURE_BB_SERVER_URL,
} from "./fixture-secrets.js";

/** Adapter world: rostered fake server + tmp dir for attachment payloads. */
function makeWorld(): {
	adapter: BlueBubblesAdapter;
	server: FakeBlueBubblesServer;
	mediaDir: string;
	cleanup: () => void;
	writeFile: (name: string, bytes: Uint8Array) => string;
} {
	const server = new FakeBlueBubblesServer();
	server.seedChat({ guid: "iMessage;-;chat-x", chatIdentifier: "chat-x" });
	const adapter = new BlueBubblesAdapter({
		config: {
			server_url: FIXTURE_BB_SERVER_URL,
			password: FIXTURE_BB_PASSWORD,
		},
		secretReader: (name) =>
			name === "BLUEBUBBLES_SERVER_URL"
				? FIXTURE_BB_SERVER_URL
				: name === "BLUEBUBBLES_PASSWORD"
					? FIXTURE_BB_PASSWORD
					: undefined,
		restClient: server,
		nowMs: () => 1_700_000_000_000,
	});
	const mediaDir = mkdtempSync(join(tmpdir(), "bb-attachments-"));
	return {
		adapter,
		server,
		mediaDir,
		cleanup: () => rmSync(mediaDir, { recursive: true, force: true }),
		writeFile: (name, bytes) => {
			const p = join(mediaDir, name);
			writeFileSync(p, bytes);
			return p;
		},
	};
}

const PNG_BYTES = new Uint8Array([
	0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 1, 2, 3, 4,
]);

describe("bluebubbles _send_attachment parity (multipart upload)", () => {
	it("an image rides the vendor multipart wire: {chatGuid,name,tempGuid} fields + ONE octet-stream part", async () => {
		const w = makeWorld();
		try {
			const path = w.writeFile("photo.png", PNG_BYTES);
			const r = await w.adapter.sendAttachmentFile("chat-x", path);
			expect(r.success).toBe(true);
			expect(r.messageId).toBe("bb-att-1");

			const up = w.server.attachmentUploadCalls[0];
			expect(up?.fields["chatGuid"]).toBe("iMessage;-;chat-x");
			expect(up?.fields["name"]).toBe("photo.png");
			expect(typeof up?.fields["tempGuid"]).toBe("string");
			expect((up?.fields["tempGuid"] ?? "").length).toBeGreaterThan(0);
			expect(up?.fields["isAudioMessage"]).toBeUndefined();
			expect(up?.file.field).toBe("attachment");
			expect(up?.file.name).toBe("photo.png");
			expect(up?.file.contentType).toBe("application/octet-stream");
			expect(
				Buffer.from(up?.file.bytes ?? []).equals(Buffer.from(PNG_BYTES)),
			).toBe(true);
			// NO text bubble accompanies a caption-less upload.
			expect(w.server.messageTextCalls).toHaveLength(0);
		} finally {
			w.cleanup();
		}
	});

	it("voice flags isAudioMessage=true; a caption rides a SEPARATE text bubble AFTER the upload", async () => {
		const w = makeWorld();
		try {
			const audio = w.writeFile("note.caf", new Uint8Array([1, 2, 3]));

			await w.adapter.sendVoice("chat-x", audio);
			expect(w.server.attachmentUploadCalls[0]?.fields["isAudioMessage"]).toBe(
				"true",
			);
			expect(w.server.attachmentUploadCalls).toHaveLength(1);
			expect(w.server.messageTextCalls).toHaveLength(0);

			await w.adapter.sendAttachmentFile("chat-x", audio, {
				caption: "listen to this",
			});
			// Caption went out over the TEXT engine, never inside the upload.
			expect(w.server.attachmentUploadCalls).toHaveLength(2);
			expect(w.server.messageTextCalls).toHaveLength(1);
			expect(w.server.messageTextCalls[0]?.payload?.["message"]).toBe(
				"listen to this",
			);
			expect(w.server.messageTextCalls[0]?.payload?.["chatGuid"]).toBe(
				"iMessage;-;chat-x",
			);
		} finally {
			w.cleanup();
		}
	});

	it("sendMultipleImages gives EVERY image its own attachment bubble (iMessage semantics)", async () => {
		const w = makeWorld();
		try {
			const a = w.writeFile("a.png", new Uint8Array([1]));
			const b = w.writeFile("b.jpg", new Uint8Array([2]));
			const c = w.writeFile("c.gif", new Uint8Array([3]));
			const results = await w.adapter.sendMultipleImages("chat-x", [a, b, c]);
			expect(results.map((r) => r.success)).toEqual([true, true, true]);
			expect(
				w.server.attachmentUploadCalls.map((u) => u.fields["name"]),
			).toEqual(["a.png", "b.jpg", "c.gif"]);
			for (const up of w.server.attachmentUploadCalls) {
				expect(up.fields["isAudioMessage"]).toBeUndefined();
			}
		} finally {
			w.cleanup();
		}
	});

	it("missing files and unresolved chats fail with the source verdicts WITHOUT touching the wire", async () => {
		const w = makeWorld();
		try {
			const ghost = join(w.mediaDir, "ghost.png");
			const r1 = await w.adapter.sendAttachmentFile("chat-x", ghost);
			expect(r1.success).toBe(false);
			expect(r1.error).toBe(`File not found: ${ghost}`);

			const real = w.writeFile("real.png", PNG_BYTES);
			const r2 = await w.adapter.sendAttachmentFile("chat-zzz", real);
			expect(r2.success).toBe(false);
			expect(r2.error).toBe("Chat not found: chat-zzz");

			expect(w.server.attachmentUploadCalls).toHaveLength(0);
			expect(w.server.messageTextCalls).toHaveLength(0);
		} finally {
			w.cleanup();
		}
	});

	it("failures surface the vendor verdict: body-status carries its message, transport errors land in the catch", async () => {
		const w = makeWorld();
		try {
			const path = w.writeFile("doc.pdf", new Uint8Array([9, 9]));
			// Body-level failure: HTTP 200 but the envelope says otherwise.
			w.server.scriptAttachmentEnvelope({
				status: 403,
				message: "Private API disabled",
			});
			const r1 = await w.adapter.sendAttachmentFile("chat-x", path);
			expect(r1.success).toBe(false);
			expect(r1.error).toBe("Private API disabled");

			// Transport-level failure: raise_for_status parity throws into the catch.
			w.server.setAttachmentError("connect ECONNREFUSED 127.0.0.1:1234");
			const r2 = await w.adapter.sendAttachmentFile("chat-x", path);
			expect(r2.success).toBe(false);
			expect(r2.error).toContain("ECONNREFUSED");
		} finally {
			w.cleanup();
		}
	});
});

describe("bluebubbles _download_attachment parity (byte round trip)", () => {
	it("upload stores the attachment; download returns the SAME bytes byte-exact", async () => {
		const w = makeWorld();
		try {
			const payload = new Uint8Array([0xde, 0xad, 0xbe, 0xef, 42]);
			const path = w.writeFile("blob.bin", payload);
			const r = await w.adapter.sendAttachmentFile("chat-x", path);
			expect(r.success).toBe(true);

			const guid = r.messageId ?? "";
			const out = await w.adapter.downloadAttachment(guid);
			expect(out).not.toBeNull();
			expect(Buffer.from(out ?? []).equals(Buffer.from(payload))).toBe(true);
			expect(w.server.attachmentDownloadCalls).toEqual([guid]);
		} finally {
			w.cleanup();
		}
	});

	it("an unknown attachment guid downloads null through the raise_for_status ladder", async () => {
		const w = makeWorld();
		try {
			expect(await w.adapter.downloadAttachment("never-uploaded")).toBeNull();
			expect(w.server.attachmentDownloadCalls).toEqual(["never-uploaded"]);
		} finally {
			w.cleanup();
		}
	});
});

describe("post-stream rescan lanes bind the REAL adapter (DEC-019)", () => {
	const accept = (p: string): string | null => p;
	const opts = (adapter: PostStreamAdapter, chatId: string) => ({
		adapter,
		chatId,
		chatPlatform: "bluebubbles",
		validatePath: accept,
	});

	it("the adapter satisfies the PostStreamAdapter dispatch seam structurally", () => {
		const w = makeWorld();
		try {
			const lane: PostStreamAdapter = w.adapter;
			expect(lane.sendMultipleImages).toBeDefined();
			expect(lane.sendVoice).toBeDefined();
			expect(lane.sendVideo).toBeDefined();
			expect(lane.sendDocument).toBeDefined();
		} finally {
			w.cleanup();
		}
	});

	it("MEDIA tags dispatch all four lanes onto the multipart wire with correct shapes", async () => {
		const w = makeWorld();
		try {
			const png = w.writeFile("shot.png", new Uint8Array([1]));
			const ogg = w.writeFile("memo.ogg", new Uint8Array([2]));
			const mov = w.writeFile("clip.mov", new Uint8Array([3]));
			const pdf = w.writeFile("paper.pdf", new Uint8Array([4]));

			const r1 = await rescanPostStream(
				`MEDIA:${png} MEDIA:${ogg}`,
				opts(w.adapter, "chat-x"),
			);
			expect(r1.attempts.map((a) => [a.kind, a.status])).toEqual([
				["image_batch", "sent"],
				["voice_or_audio", "sent"],
			]);

			const r2 = await rescanPostStream(
				`MEDIA:${mov} MEDIA:${pdf}`,
				opts(w.adapter, "chat-x"),
			);
			expect(r2.attempts.map((a) => [a.kind, a.status])).toEqual([
				["video", "sent"],
				["document", "sent"],
			]);

			// Four uploads total: image + voice (flagged) + video + document.
			const ups = w.server.attachmentUploadCalls;
			expect(ups.map((u) => u.file.name)).toEqual([
				"shot.png",
				"memo.ogg",
				"clip.mov",
				"paper.pdf",
			]);
			expect(ups[1]?.fields["isAudioMessage"]).toBe("true");
			expect(ups[0]?.fields["isAudioMessage"]).toBeUndefined();
			expect(ups[2]?.fields["isAudioMessage"]).toBeUndefined();
			expect(ups[3]?.fields["isAudioMessage"]).toBeUndefined();
			for (const up of ups) {
				expect(up.fields["chatGuid"]).toBe("iMessage;-;chat-x");
			}
		} finally {
			w.cleanup();
		}
	});
});
