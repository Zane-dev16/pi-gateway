// pi_platforms/qqbot/qqbot-media.test.ts — the NATIVE MEDIA lane behavior
// contract (adjudication cn-1, Hermes anchor adapter.py:_send_media):
//
//   - HTTP(S) URLs upload via ONE POST /v2/{users|groups}/{id}/files
//     {file_type, srv_send_msg:false, url} then deliver msg_type=7 with body
//     {media:{file_info}, content?, msg_id?, msg_seq} — never a text degrade.
//   - Local files ride the three-step chunked flow (byte seam injected) and
//     land on the SAME msg_type=7 body.
//   - Guild chats refuse native media non-retryably; biz 40093002 surfaces as
//     a NON-retryable typed daily-limit failure; missing file_info fails loud.
//   - Every media transmission admits through the EgressChokepoint (audit).

import { describe, expect, it } from "vitest";
import { makeQQWorld, type QQWorld } from "./qqbot-fixture.js";
import {
	QQ_MEDIA_TYPE_FILE,
	QQ_MEDIA_TYPE_IMAGE,
	QQ_MSG_TYPE_MEDIA,
} from "./manifest.js";

async function liveWorld(name: string): Promise<QQWorld> {
	const world = makeQQWorld({ name });
	await world.connectAndAwaitLive();
	return world;
}

describe("qqbot native media lane (_send_media parity)", () => {
	it("uploads URL images via /files {url} then delivers msg_type=7 with file_info + caption + reply", async () => {
		const world = await liveWorld("qb-media-url");
		const { engine, gateway } = world;
		engine.chatTypeMap.set("u_media", "c2c");
		const auditBefore = engine.doorAudit().length;

		const result = await engine.sendImage(
			"u_media",
			"https://cdn.example.com/photos/cat.png",
			"a cat",
			"mid-9",
		);

		expect(result.success).toBe(true);
		expect(result.messageId).toBe("wmsg-1");

		// Upload leg: single POST /v2/users/{id}/files with srv_send_msg=false.
		const files = gateway.callsOf("files");
		expect(files).toHaveLength(1);
		expect(files[0]!.method).toBe("POST");
		expect(files[0]!.path).toBe("/v2/users/u_media/files");
		expect(files[0]!.body).toEqual({
			file_type: QQ_MEDIA_TYPE_IMAGE,
			srv_send_msg: false,
			url: "https://cdn.example.com/photos/cat.png",
		});

		// Delivery leg: msg_type=7 RichMedia body through the messages path.
		const msgs = gateway.callsOf("messages:c2c");
		expect(msgs.length).toBeGreaterThanOrEqual(1);
		const body = msgs[msgs.length - 1]!.body;
		expect(body["msg_type"]).toBe(QQ_MSG_TYPE_MEDIA);
		expect(body["media"]).toEqual({ file_info: "fi-fake" });
		expect(body["content"]).toBe("a cat");
		expect(body["msg_id"]).toBe("mid-9");
		expect(typeof body["msg_seq"]).toBe("number");

		// The admission rode the audited chokepoint like every other send.
		expect(engine.doorAudit().length).toBe(auditBefore + 1);
		expect(engine.doorAudit()[engine.doorAudit().length - 1]!.action).toBe(
			"plain-send",
		);
	});

	it("delivers group media to /v2/groups/{id}/… with NO content/msg_id when absent", async () => {
		const world = await liveWorld("qb-media-group");
		const { engine, gateway } = world;
		engine.chatTypeMap.set("g_media", "group");

		const result = await engine.sendImage(
			"g_media",
			"https://cdn.example.com/dog.jpg",
		);

		expect(result.success).toBe(true);
		const files = gateway.callsOf("files");
		expect(files[0]!.path).toBe("/v2/groups/g_media/files");
		const msgs = gateway.callsOf("messages:group");
		expect(msgs.length).toBe(1);
		const body = msgs[0]!.body;
		expect(body["msg_type"]).toBe(QQ_MSG_TYPE_MEDIA);
		expect(body["media"]).toEqual({ file_info: "fi-fake" });
		expect(body["content"]).toBeUndefined();
		expect(body["msg_id"]).toBeUndefined();
	});

	it("chunk-uploads LOCAL files through the injected byte seam onto the same msg_type=7 body", async () => {
		const bytes = Buffer.from("local-png-bytes!"); // 16B → two 8B parts
		const world = makeQQWorld({
			name: "qb-media-local",
			readFileBytes: (path) => {
				expect(path).toBe("/fixtures/cat.png");
				return bytes;
			},
		});
		const { engine, gateway } = world;
		await world.connectAndAwaitLive();
		engine.chatTypeMap.set("u_local", "c2c");
		gateway.script("upload_prepare", {
			kind: "ok",
			body: {
				upload_id: "up-media-1",
				block_size: 8,
				concurrency: 1,
				parts: [
					{ part_index: 1, presigned_url: "/cos-part/m1" },
					{ part_index: 2, presigned_url: "/cos-part/m2" },
				],
			},
		});

		const result = await engine.sendDocument("u_local", "/fixtures/cat.png");

		expect(result.success).toBe(true);

		const prepares = gateway.callsOf("upload_prepare");
		expect(prepares).toHaveLength(1);
		const prepareBody = prepares[0]!.body;
		expect(prepareBody["file_type"]).toBe(QQ_MEDIA_TYPE_FILE);
		expect(prepareBody["file_name"]).toBe("cat.png");
		expect(prepareBody["file_size"]).toBe(bytes.length);
		expect(typeof prepareBody["md5_10m"]).toBe("string");

		// Two COS part PUTs sliced at block_size rode the transport seam.
		const puts = gateway.callsOf("cos-part");
		expect(puts.map((c) => c.path)).toEqual(["/cos-part/m1", "/cos-part/m2"]);

		// complete_upload reused /files with an upload_id-only body.
		const completes = gateway
			.callsOf("files")
			.filter((c) => typeof c.body["upload_id"] === "string");
		expect(completes).toHaveLength(1);
		expect(completes[0]!.body["upload_id"]).toBe("up-media-1");

		// …and the delivery body carries the completed file_info token.
		const msgs = gateway.callsOf("messages:c2c");
		expect(msgs.length).toBe(1);
		expect(msgs[0]!.body["msg_type"]).toBe(QQ_MSG_TYPE_MEDIA);
		expect(msgs[0]!.body["media"]).toEqual({ file_info: "fi-fake" });
	});

	it("refuses guild media NON-retryably without any REST traffic", async () => {
		const world = await liveWorld("qb-media-guild");
		const { engine, gateway } = world;
		engine.chatTypeMap.set("ch_guild", "guild");

		const result = await engine.sendVoice("ch_guild", "/tmp/clip.mp3");

		expect(result.success).toBe(false);
		expect(result.retryable).toBe(false);
		expect(result.error).toContain("Guild media send not supported");
		expect(gateway.callsOf("files")).toHaveLength(0);
		expect(gateway.callsOf("messages:guild")).toHaveLength(0);
	});

	it("maps biz 40093002 to a NON-retryable daily-limit failure naming the file", async () => {
		const world = makeQQWorld({
			name: "qb-media-quota",
			readFileBytes: () => Buffer.from("mp4-bytes"),
		});
		const { engine, gateway } = world;
		await world.connectAndAwaitLive();
		engine.chatTypeMap.set("u_quota", "c2c");
		gateway.script("upload_prepare", {
			kind: "fail",
			message:
				"QQ Bot API error [400] /v2/users/u_quota/upload_prepare: code=40093002 daily limit",
		});

		const result = await engine.sendVideo("u_quota", "/tmp/movie.mp4");

		expect(result.success).toBe(false);
		expect(result.retryable).toBe(false);
		expect(result.error).toContain("daily upload limit exceeded");
		expect(result.error).toContain("movie.mp4");
		// Non-retryable quota failures NEVER reach a message send.
		expect(gateway.callsOf("messages:c2c")).toHaveLength(0);
	});

	it("falls back to a '{caption}\n{url}' TEXT send when an URL image upload carries no file_info (send_image parity)", async () => {
		const world = makeQQWorld({ name: "qb-media-noinfo" });
		const { engine, gateway } = world;
		await world.connectAndAwaitLive();
		engine.chatTypeMap.set("u_noinfo", "c2c");
		gateway.script("files", {
			kind: "ok",
			body: { data: {} }, // neither file_info nor data.file_info present
		});

		const result = await engine.sendImage(
			"u_noinfo",
			"https://x.example/x.png",
			"look at this",
		);

		// The MEDIA leg failed loud…
		expect(gateway.callsOf("files")).toHaveLength(1);
		// …and adapter.py:send_image degraded to the text link lane.
		expect(result.success).toBe(true);
		const msgs = gateway.callsOf("messages:c2c");
		expect(msgs.length).toBe(1);
		const body = msgs[0]!.body;
		expect(body["msg_type"]).not.toBe(QQ_MSG_TYPE_MEDIA); // plain/markdown text
		const sentText =
			body["markdown"] !== undefined
				? String((body["markdown"] as Record<string, unknown>)["content"] ?? "")
				: String(body["content"] ?? "");
		expect(sentText).toBe("look at this\nhttps://x.example/x.png");
	});

	it("does NOT fall back for non-image URL sources (send_voice/video/document raw lane)", async () => {
		const world = makeQQWorld({ name: "qb-media-noinfo-doc" });
		const { engine, gateway } = world;
		await world.connectAndAwaitLive();
		engine.chatTypeMap.set("u_docfail", "c2c");
		gateway.script("files", {
			kind: "ok",
			body: { data: {} },
		});

		// adapter.py:send_document/_send_media raw surface — NO text-URL lane.
		const result = await engine.sendMedia({
			chatId: "u_docfail",
			source: "https://x.example/report.pdf",
			fileType: QQ_MEDIA_TYPE_FILE,
		});

		expect(result.success).toBe(false);
		expect(result.retryable).toBe(false);
		expect(result.error).toContain("Upload returned no file_info");
		// The failed upload NEVER degrades onto the message wire.
		expect(gateway.callsOf("messages:c2c")).toHaveLength(0);
	});
});
