// yuanbao-conformity — behavior contracts for adjudicated findings cn-6/cn-7/
// cn-11 (move TOWARD Hermes vendor truth, never away):
//
//   cn-6  MISSING_BEHAVIOR medium — group sends parse @nickname into mixed
//         TIMTextElem + TIMCustomElem(elem_type 1002) bodies from a member
//         cache fed by encode_get_group_member_list (Hermes anchors:
//         yuanbao.py:_build_msg_body_with_mentions +
//         GroupQuery.get_group_member_list_raw + _member_cache).
//   cn-7  MISSING_BEHAVIOR medium — reply_to threads onto ref_msg_id
//         (send_group_message field 7) on the FIRST chunk only (Hermes
//         anchors: send_group_msg_body ref_msg_id=reply_to or ""; sender
//         "Only reply_to the first chunk" shape).
//   cn-11 MISSING_BEHAVIOR low — sign-token POST carries X-AppVersion,
//         X-OperationSystem, X-Instance-Id, X-Bot-Version alongside
//         X-Route-Env (Hermes anchor: yuanbao.py:SignManager.fetch).
//
// Every row drives the REAL adapter over the REAL binary ConnMsg plane of
// FakeYuanbaoGateway — frames are decoded from the wire, never stubbed.

import { describe, expect, it } from "vitest";
import type { Metadata } from "../../pi_gateway/streaming/adapter-seam.js";
import { FakeYuanbaoGateway, type YbWireSend } from "./fake-yuanbao.js";
import { YuanbaoAdapter } from "./yuanbao-adapter.js";
import { makeYBWorld, type YBWorld } from "./yuanbao-fixture.js";
import { SignManager, type SignHttpSeam } from "./sign-manager.js";
import {
	BIZ_PKG,
	decodeBizMsg,
	decodeGetGroupMemberListReq,
	decodeGetGroupMemberListRsp,
	encodeGetGroupMemberList,
	encodeGetGroupMemberListRspFixture,
	HERMES_INSTANCE_ID,
	type GroupMemberListResult,
	resetSeqNo,
} from "./proto.js";
import {
	SEND_TIMEOUT_S,
	MEMBER_CACHE_TTL_S,
	SIGN_APP_VERSION,
	SIGN_BOT_VERSION,
	SIGN_INSTANCE_ID,
	SIGN_OPERATION_SYSTEM,
	YB_MAX_TEXT_CHUNK,
	YB_GROUP_CODE_METADATA_KEY,
} from "./manifest.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";

// ── harness ──────────────────────────────────────────────────────────────────

async function liveWorld(name: string): Promise<YBWorld> {
	const world = makeYBWorld({ name });
	await world.connectAndAwaitLive();
	return world;
}

function groupSends(world: YBWorld): YbWireSend[] {
	return world.gateway.sentMessages.filter(
		(s) => s.cmd === "send_group_message",
	);
}

/** Parse a TIMCustomElem mention payload (elem_type 1002). */
function mentionPayload(data: unknown): {
	elem_type: number;
	text: string;
	user_id: string;
} {
	expect(typeof data).toBe("string");
	return JSON.parse(String(data)) as {
		elem_type: number;
		text: string;
		user_id: string;
	};
}

/** Strip decoder-normalized DEFAULTS (""/0/[]) from a wire-decoded body
 * element so assertions express exactly what rode the wire. */
function slim(
	el: { msg_type: string; msg_content: MsgContentRecord } | undefined,
): { msg_type: string; msg_content: Record<string, unknown> } {
	const content: Record<string, unknown> = {};
	for (const [k, v] of Object.entries(el?.msg_content ?? {})) {
		const meaningful =
			typeof v === "string"
				? v !== ""
				: Array.isArray(v)
					? v.length > 0
					: v !== 0;
		if (meaningful) content[k] = v;
	}
	return { msg_type: el?.msg_type ?? "", msg_content: content };
}
type MsgContentRecord = Record<string, unknown>;

function memberDirectory(): GroupMemberListResult {
	return {
		code: 0,
		message: "ok",
		members: [
			{
				user_id: "u-alice",
				nickname: "Alice",
				role: 2,
				join_time: 1111,
				name_card: "",
			},
			{
				user_id: "u-bob",
				nickname: "Bob",
				role: 0,
				join_time: 2222,
				name_card: "",
			},
		],
		next_offset: 0,
		is_complete: true,
	};
}

// ── cn-11: sign-token identity headers ───────────────────────────────────────

describe("cn-11 — sign-token POST carries the four Hermes identity headers", () => {
	it("SignManager.fetch always sends X-AppVersion/X-OperationSystem/X-Instance-Id/X-Bot-Version; X-Route-Env ONLY when routeEnv is set", async () => {
		const calls: Array<{
			headers: Record<string, string>;
		}> = [];
		const http: SignHttpSeam = {
			postJson: async (_url, _payload, headers) => {
				calls.push({ headers });
				return {
					status: 200,
					body: {
						code: 0,
						data: { token: "t", bot_id: "b", duration: 3600 },
					},
				};
			},
			nowMs: () => 0,
			sleepMs: async () => {},
		};
		const opts = {
			appKey: "k",
			appSecret: "s",
			apiDomain: "https://x.invalid",
		};

		await SignManager.fetch(http, opts);
		await SignManager.fetch(http, { ...opts, routeEnv: "beta" });

		expect(calls).toHaveLength(2);
		for (const [i, call] of calls.entries()) {
			expect(call.headers["Content-Type"]).toBe("application/json");
			expect(call.headers["X-AppVersion"]).toBe(SIGN_APP_VERSION);
			expect(call.headers["X-Bot-Version"]).toBe(SIGN_BOT_VERSION);
			expect(call.headers["X-OperationSystem"]).toBe(SIGN_OPERATION_SYSTEM);
			expect(call.headers["X-Instance-Id"]).toBe(SIGN_INSTANCE_ID);
			if (i === 0) {
				expect(call.headers["X-Route-Env"]).toBeUndefined();
			} else {
				expect(call.headers["X-Route-Env"]).toBe("beta");
			}
		}
	});

	it("manifest header DATA keeps the exact Hermes value shapes", () => {
		// _APP_VERSION == _BOT_VERSION == hermes_cli.__version__.
		expect(SIGN_APP_VERSION).toBe(SIGN_BOT_VERSION);
		expect(SIGN_APP_VERSION.length).toBeGreaterThan(0);
		// _YUANBAO_INSTANCE_ID = str(yuanbao_proto.HERMES_INSTANCE_ID) = "17".
		expect(SIGN_INSTANCE_ID).toBe(String(HERMES_INSTANCE_ID));
		expect(SIGN_INSTANCE_ID).toBe("17");
		// _OPERATION_SYSTEM = sys.platform — host OS token, non-empty.
		expect(SIGN_OPERATION_SYSTEM.length).toBeGreaterThan(0);
	});

	it("the REAL connect flow puts the same headers on the wire POST", async () => {
		const gateway = new FakeYuanbaoGateway();
		const headersSeen: Array<Record<string, string>> = [];
		const adapter = new YuanbaoAdapter({
			appKey: "k",
			appSecret: "s",
			gateway,
			signHttp: {
				postJson: async (_url, _payload, headers) => {
					headersSeen.push(headers);
					return {
						status: 200,
						body: {
							code: 0,
							data: {
								token: "tok",
								bot_id: "bot-self",
								duration: 7200,
							},
						},
					};
				},
			},
		});
		const ok = await adapter.connect({ isReconnect: false });
		expect(ok).toBe(true);
		expect(adapter.connectId).not.toBeNull();
		expect(headersSeen).toHaveLength(1);
		const headers = headersSeen[0] ?? {};
		expect(headers["X-AppVersion"]).toBe(SIGN_APP_VERSION);
		expect(headers["X-Bot-Version"]).toBe(SIGN_BOT_VERSION);
		expect(headers["X-OperationSystem"]).toBe(SIGN_OPERATION_SYSTEM);
		expect(headers["X-Instance-Id"]).toBe(SIGN_INSTANCE_ID);
		await adapter.disconnect();
	});
});

// ── cn-6 (wire layer): get_group_member_list codec ──────────────────────────

describe("cn-6 — get_group_member_list codec round-trips BYTE-FAITHFULLY", () => {
	it("req encodes group_code/offset/limit under biz wrapper get_group_member_list; offset omitted when 0", () => {
		resetSeqNo();
		const biz = decodeBizMsg(encodeGetGroupMemberList("g-9"));
		expect(biz.method).toBe("get_group_member_list");
		expect(biz.service).toBe(BIZ_PKG);
		expect(biz.req_id.startsWith("gml_")).toBe(true);
		expect(biz.is_response).toBe(false);
		expect(decodeGetGroupMemberListReq(biz.body)).toEqual({
			groupCode: "g-9",
			offset: 0,
			limit: 200,
		});

		const bizPaged = decodeBizMsg(encodeGetGroupMemberList("g-9", 40, 50));
		expect(decodeGetGroupMemberListReq(bizPaged.body)).toEqual({
			groupCode: "g-9",
			offset: 40,
			limit: 50,
		});
	});

	it("rsp fixture encoder ↔ decoder preserve members/next_offset/is_complete with reference-normalized defaults", () => {
		const result: GroupMemberListResult = {
			code: 0,
			message: "ok",
			members: [
				{
					user_id: "u-alice",
					nickname: "Alice",
					role: 2,
					join_time: 1111,
					name_card: "",
				},
				{
					user_id: "u-bob",
					nickname: "Bob",
					role: 0,
					join_time: 0,
					name_card: "Bobby",
				},
				{ user_id: "", nickname: "", role: 0, join_time: 0, name_card: "" },
			],
			next_offset: 40,
			is_complete: false,
		};
		const decoded = decodeGetGroupMemberListRsp(
			encodeGetGroupMemberListRspFixture(result),
		);
		expect(decoded).toEqual(result);

		// Malformed wire → null (reference returns None on parse failure).
		expect(decodeGetGroupMemberListRsp(Uint8Array.from([0x0f]))).toBeNull();
	});

	it("manifest constants keep the vendor truth shapes", () => {
		expect(MEMBER_CACHE_TTL_S).toBe(300.0); // MEMBER_CACHE_TTL_S (5 minutes)
		expect(SEND_TIMEOUT_S).toBe(30.0); // DEFAULT_SEND_TIMEOUT
	});
});

// ── cn-6 (behavior): member cache + mixed mention bodies ───────────────────

describe("cn-6 — group sends split @mentions into mixed TIMTextElem/TIMCustomElem bodies", () => {
	it("@Known → TIMCustomElem(elem_type 1002); unknown @name stays plain text; segments/tail trimmed", async () => {
		const world = await liveWorld("yb-cn6-mentions");
		const { engine, gateway } = world;

		gateway.memberDirectory.set("g-77", memberDirectory());
		const listed = await engine.getGroupMemberListRaw("g-77");
		expect(listed?.members.map((m) => m.nickname)).toEqual(["Alice", "Bob"]);
		expect(
			gateway.receivedFrames.filter((f) => f.kind === "get_group_member_list"),
		).toHaveLength(1);

		await engine.deliverText("group:g-77", "@Alice hello @Bob and @Carol!");
		const send = groupSends(world).at(-1);
		expect(send?.cmd).toBe("send_group_message");
		expect(send?.groupCode).toBe("g-77");
		expect(send?.msgBody.map(slim)).toEqual([
			{
				msg_type: "TIMCustomElem",
				msg_content: {
					data: JSON.stringify({
						elem_type: 1002,
						text: "@Alice",
						user_id: "u-alice",
					}),
				},
			},
			{ msg_type: "TIMTextElem", msg_content: { text: "hello" } },
			{
				msg_type: "TIMCustomElem",
				msg_content: {
					data: JSON.stringify({
						elem_type: 1002,
						text: "@Bob",
						user_id: "u-bob",
					}),
				},
			},
			{ msg_type: "TIMTextElem", msg_content: { text: "and" } },
			{ msg_type: "TIMTextElem", msg_content: { text: "@Carol!" } },
		]);
		const first = send?.msgBody[0];
		if (first !== undefined) {
			expect(mentionPayload(first.msg_content["data"])).toEqual({
				elem_type: 1002,
				text: "@Alice",
				user_id: "u-alice",
			});
		}
	});

	it("mention matching is case-insensitive and preserves the DIRECTORY nickname casing", async () => {
		const world = await liveWorld("yb-cn6-case");
		const { engine, gateway } = world;
		gateway.memberDirectory.set("g-case", memberDirectory());
		await engine.getGroupMemberListRaw("g-case");

		await engine.deliverText("group:g-case", "@alice ok");
		const send = groupSends(world).at(-1);
		expect(send?.msgBody.map(slim)).toHaveLength(2);
		const payload = mentionPayload(send?.msgBody[0]?.msg_content["data"]);
		expect(payload).toEqual({
			elem_type: 1002,
			text: "@Alice", // real nickname casing, lowercased lookup
			user_id: "u-alice",
		});
		expect(send?.msgBody[1]?.msg_content["text"]).toBe("ok");
	});

	it("WITHOUT a cached member list the group body stays ONE plain TIMTextElem (legacy byte shape)", async () => {
		const world = await liveWorld("yb-cn6-nocache");
		const { engine } = world;

		await engine.deliverText("group:g-plain", "plain @Nobody text");
		const send = groupSends(world).at(-1);
		expect(send?.msgBody.map(slim)).toEqual([
			{ msg_type: "TIMTextElem", msg_content: { text: "plain @Nobody text" } },
		]);
	});

	it("member cache expires after MEMBER_CACHE_TTL_S under the injected clock; a refresh restores mentions", async () => {
		const world = await liveWorld("yb-cn6-ttl");
		const { engine, gateway, clock } = world;
		gateway.memberDirectory.set("g-ttl", memberDirectory());
		await engine.getGroupMemberListRaw("g-ttl");

		await engine.deliverText("group:g-ttl", "@Alice hi");
		expect(groupSends(world).at(-1)?.msgBody[0]?.msg_type).toBe(
			"TIMCustomElem",
		);

		// Stale past the 300s TTL → plain-text fallback (cache dropped).
		await clock.advance((MEMBER_CACHE_TTL_S + 1) * 1000);
		await engine.deliverText("group:g-ttl", "@Alice hi again");
		const stale = groupSends(world).at(-1);
		expect(stale?.msgBody.map(slim)).toEqual([
			{
				msg_type: "TIMTextElem",
				msg_content: { text: "@Alice hi again" },
			},
		]);

		// Re-fetch repopulates the cache → mentions resume.
		await engine.getGroupMemberListRaw("g-ttl");
		await engine.deliverText("group:g-ttl", "@Bob hi once more");
		const refreshed = groupSends(world).at(-1);
		expect(refreshed?.msgBody[0]?.msg_type).toBe("TIMCustomElem");
		expect(mentionPayload(refreshed?.msgBody[0]?.msg_content["data"])).toEqual({
			elem_type: 1002,
			text: "@Bob",
			user_id: "u-bob",
		});
		expect(
			gateway.receivedFrames.filter((f) => f.kind === "get_group_member_list"),
		).toHaveLength(2);
	});

	it("unknown groups answer a DECODED failure (never cached, never poisons mentions)", async () => {
		const world = await liveWorld("yb-cn6-missing-group");
		const { engine } = world;

		const result = await engine.getGroupMemberListRaw("g-missing");
		expect(result).not.toBeNull();
		expect(result?.code).toBe(50004);
		expect(result?.members).toEqual([]);

		await engine.deliverText("group:g-missing", "@Alice hi");
		expect(groupSends(world).at(-1)?.msgBody.map(slim)).toEqual([
			{ msg_type: "TIMTextElem", msg_content: { text: "@Alice hi" } },
		]);

		// Offline queries answer null without throwing.
		await engine.disconnect();
		expect(await engine.getGroupMemberListRaw("g-missing")).toBeNull();
	});
});

// ── cn-7: reply threading onto ref_msg_id (first chunk only) ────────────────

describe("cn-7 — reply_to threads ref_msg_id on the FIRST chunk only", () => {
	function c2cSendsTo(world: YBWorld, account: string) {
		return world.gateway.sentMessages.filter(
			(s) => s.cmd === "send_c2c_message" && s.toAccount === account,
		);
	}

	const longContent = Array.from(
		{ length: 14 },
		(_, i) => `seg${i} filler words`,
	).join(" "); // far beyond the subject's harness-scale 64-char budget → multi-chunk

	it("multi-chunk GROUP delivery quotes the FIRST frame; every later frame ships flat", async () => {
		const world = await liveWorld("yb-cn7-first-chunk");
		const { engine } = world;

		await engine.deliverText("group:g-reply", longContent, {
			reply_to_message_id: "msg-9",
		} as Metadata);

		const sends = groupSends(world).filter((s) => s.groupCode === "g-reply");
		expect(sends.length).toBeGreaterThanOrEqual(2);
		expect(sends[0]?.refMsgId).toBe("msg-9");
		for (const s of sends.slice(1)) {
			expect(s.refMsgId).toBe("");
		}
	});

	it("c2c legs NEVER carry ref_msg_id (field 7 is group-only on the wire)", async () => {
		const world = await liveWorld("yb-cn7-c2c-flat");
		const { engine } = world;

		await engine.deliverText("direct:utf16-r", "short", {
			reply_to_message_id: "dm-1",
		} as Metadata);
		const sends = c2cSendsTo(world, "utf16-r");
		expect(sends.length).toBe(1);
		for (const s of sends) expect(s.refMsgId).toBe("");
	});

	it("positional door replyTo threads onto a single group frame; later sends stay flat", async () => {
		const world = await liveWorld("yb-cn7-door");
		const { engine } = world;

		await engine.send("group:g-x", "hello there", "ref-42");
		const quoted = groupSends(world).at(-1);
		expect(quoted?.cmd).toBe("send_group_message");
		expect(quoted?.refMsgId).toBe("ref-42");

		await engine.send("group:g-x", "flat follow-up");
		expect(groupSends(world).at(-1)?.refMsgId).toBe("");
	});

	it("first chunk carries BOTH ref_msg_id AND the mixed mention body together", async () => {
		const world = await liveWorld("yb-cn7-reply-plus-mention");
		const { engine, gateway } = world;
		gateway.memberDirectory.set("g-both", memberDirectory());
		await engine.getGroupMemberListRaw("g-both");

		const content = `@Alice ${longContent}`;
		await engine.deliverText("group:g-both", content, {
			reply_to_message_id: "root-8",
		} as Metadata);
		const frames = groupSends(world).filter((s) => s.groupCode === "g-both");
		expect(frames.length).toBeGreaterThanOrEqual(2);
		expect(frames[0]?.refMsgId).toBe("root-8");
		// The quoted FIRST frame opens with the TIMCustomElem mention.
		expect(frames[0]?.msgBody[0]?.msg_type).toBe("TIMCustomElem");
		expect(mentionPayload(frames[0]?.msgBody[0]?.msg_content["data"])).toEqual({
			elem_type: 1002,
			text: "@Alice",
			user_id: "u-alice",
		});
		expect(frames[1]?.refMsgId).toBe("");
		expect(
			frames[1]?.msgBody.every((el) => el.msg_type === "TIMTextElem"),
		).toBe(true);
	});
});

// ── stability round yuanbao-r2 (yb-1 … yb-8) ───────────────────────────────

describe("yb-r2 — auth identity, reconnect ladder, debounce, open policy, C2C origin, bind timeout", () => {
	function signOk(data: Record<string, unknown>): SignHttpSeam {
		return {
			postJson: async () => ({ status: 200, body: { code: 0, data } }),
		};
	}

	it("yb-1: AUTH_BIND carries SIGN versions + token_data.source-or-'bot' + route_env field 5 (adapter routeEnv wins)", async () => {
		const gateway = new FakeYuanbaoGateway();
		const adapter = new YuanbaoAdapter({
			appKey: "k",
			appSecret: "s",
			gateway,
			routeEnv: "prod-env",
			signHttp: signOk({
				token: "tok-1",
				bot_id: "bot-self",
				duration: 7200,
				source: "web",
				route_env: "token-env",
			}),
		});
		expect(await adapter.connect({ isReconnect: false })).toBe(true);
		expect(gateway.authBinds).toHaveLength(1);
		const bind = gateway.authBinds[0]!;
		// _HERMES_VERSION ('0.20.5') — never the hardcoded '1.0.0'.
		expect(bind.appVersion).toBe(SIGN_APP_VERSION);
		expect(bind.botVersion).toBe(SIGN_BOT_VERSION);
		expect(bind.appVersion).toBe("0.20.5");
		// token_data.source threaded verbatim.
		expect(bind.source).toBe("web");
		expect(bind.uid).toBe("bot-self");
		// env_name(field 5): adapter._route_env or token_data.route_env.
		expect(bind.routeEnv).toBe("prod-env");
		await adapter.disconnect();

		// Fallbacks: no adapter routeEnv → token route_env; missing source → 'bot'.
		const gateway2 = new FakeYuanbaoGateway();
		const adapter2 = new YuanbaoAdapter({
			appKey: "k",
			appSecret: "s",
			gateway: gateway2,
			signHttp: signOk({ token: "tok-2", bot_id: "b2", duration: 7200 }),
		});
		expect(await adapter2.connect({ isReconnect: false })).toBe(true);
		const bind2 = gateway2.authBinds[0]!;
		expect(bind2.source).toBe("bot"); // source-or-'bot'
		expect(bind2.routeEnv).toBe("");
		await adapter2.disconnect();

		// yb-4: the ADAPTER-DEFAULT chunk budget is MAX_TEXT_CHUNK=4000
		// (subjects may scale down explicitly; bare construction must not
		// fragment texts into tiny pieces).
		expect(adapter.chatLengthPolicyForChat("direct:x").maxUnits).toBe(
			YB_MAX_TEXT_CHUNK,
		);
		expect(YB_MAX_TEXT_CHUNK).toBe(4000);
	});

	it("yb-2/yb-3: every reconnect RE-SIGNS despite a cache-valid token; backoff grows to the 60s cap and stays there", async () => {
		const clock = new ManualClock();
		const gateway = new FakeYuanbaoGateway();
		let posts = 0;
		const adapter = new YuanbaoAdapter({
			appKey: "k",
			appSecret: "s",
			gateway,
			nowMs: clock.nowMs,
			sleepMs: clock.sleepMs,
			signHttp: {
				postJson: async () => {
					posts += 1;
					return {
						status: 200,
						body: {
							code: 0,
							data: { token: `t${posts}`, bot_id: "b", duration: 7200 },
						},
					};
				},
			},
		});
		expect(await adapter.connect({ isReconnect: false })).toBe(true);
		expect(posts).toBe(1); // initial get()

		// Withhold BIND_ACK: each reconnect dial opens the socket but fails
		// closed at AUTH_TIMEOUT_S (injected) → exactly ONE ladder chain.
		gateway.withholdBindAck = true;
		gateway.dropActive(1001, "going away");
		for (let i = 0; i < 80 && adapter.reconnectSteps.length < 10; i++) {
			await clock.advance(70_000);
			await new Promise<void>((r) => setTimeout(r, 5));
		}
		const delays = adapter.reconnectSteps.map((s) => s.delayMs);
		expect(delays.length).toBeGreaterThanOrEqual(8);
		// min(2**(n-1), 60): 1s, 2s, 4s … reaching the 60s CAP (never 16s forever).
		expect(delays.slice(0, 3)).toEqual([1000, 2000, 4000]);
		expect(Math.max(...delays)).toBe(60_000);
		expect(delays.every((d) => d <= 60_000)).toBe(true);
		expect(delays[delays.length - 1]).toBe(60_000);

		// yb-2: EVERY dial force-refreshed the sign token even though the
		// first token stays cache-valid for 7200s of injected time (a plain
		// cache get() would have answered without ANY further POST).
		expect(posts).toBeGreaterThanOrEqual(delays.length); // 1 initial + per-dial re-signs
		expect(clock.nowMs() / 1000).toBeLessThan(7200);
		await adapter.disconnect();
	}, 30_000);

	it("yb-5: same-sender companion pushes merge into ONE turn with the \\n separator; windows RESET on arrival; other senders stay separate", async () => {
		const world = await liveWorld("yb-r2-debounce");
		const { engine, gateway, clock } = world;
		const push = (id: string, from: string, text: string): void =>
			gateway.pushMessage({
				from_account: from,
				msg_id: id,
				group_code: "",
				callback_command: "",
				msg_body: [{ msg_type: "TIMTextElem", msg_content: { text } }],
			});

		// image + caption arriving as separate pushes become ONE merged run.
		push("m-img", "u1", "img");
		push("m-cap", "u1", "caption");
		for (let i = 0; i < 60 && !engine.turnLog.includes("img\ncaption"); i++) {
			await clock.advance(200);
			await new Promise<void>((r) => setTimeout(r, 2));
		}
		expect(engine.turnLog).toContain("img\ncaption");
		expect(
			engine.turnLog.filter((t) => t === "img" || t === "caption"),
		).toEqual([]);

		// A different sender dispatches as its OWN turn.
		push("m-hello", "u2", "hello");
		for (let i = 0; i < 60 && !engine.turnLog.includes("hello"); i++) {
			await clock.advance(200);
			await new Promise<void>((r) => setTimeout(r, 2));
		}
		expect(engine.turnLog).toContain("hello");

		// A new arrival RESETS the window (existing timer cancelled upstream):
		// 1s alone delivers nothing; a second push at t+1s restarts the 1.5s
		// wait; the pair merges only after the FULL restarted window elapses.
		push("m-d1", "u3", "d1");
		await clock.advance(1_000);
		await new Promise<void>((r) => setTimeout(r, 4));
		expect(engine.turnLog.some((t) => t.startsWith("d1"))).toBe(false);
		push("m-d2", "u3", "d2");
		await clock.advance(1_000); // still inside the RESTARTED window
		await new Promise<void>((r) => setTimeout(r, 4));
		expect(engine.turnLog.some((t) => t.includes("d1"))).toBe(false);
		await clock.advance(500); // completes the restarted window
		await new Promise<void>((r) => setTimeout(r, 4));
		expect(engine.turnLog).toContain("d1\nd2");
	});

	it("yb-6: dm/group 'open' policies are OPT-IN via GATEWAY/YUANBAO_ALLOW_ALL_USERS (either flag, case-insensitive)", () => {
		const adapter = new YuanbaoAdapter({
			appKey: "k",
			appSecret: "s",
			gateway: new FakeYuanbaoGateway(),
			signHttp: { postJson: async () => ({ status: 200, body: {} }) },
			dmPolicy: "open",
			groupPolicy: "open",
		});
		const prev = {
			GATEWAY_ALLOW_ALL_USERS: process.env["GATEWAY_ALLOW_ALL_USERS"],
			YUANBAO_ALLOW_ALL_USERS: process.env["YUANBAO_ALLOW_ALL_USERS"],
		};
		try {
			delete process.env["GATEWAY_ALLOW_ALL_USERS"];
			delete process.env["YUANBAO_ALLOW_ALL_USERS"];
			// No opt-in ⇒ 'open' DENIES both lanes (never default-open).
			expect(adapter.isDmIntakeAllowed("u9")).toBe(false);
			expect(adapter.isGroupAllowed("g9")).toBe(false);

			process.env["GATEWAY_ALLOW_ALL_USERS"] = "1";
			expect(adapter.isDmIntakeAllowed("u9")).toBe(true);
			expect(adapter.isGroupAllowed("g9")).toBe(true);
			delete process.env["GATEWAY_ALLOW_ALL_USERS"];

			process.env["YUANBAO_ALLOW_ALL_USERS"] = "YES";
			expect(adapter.isDmIntakeAllowed("u9")).toBe(true);
			expect(adapter.isGroupAllowed("g9")).toBe(true);
			process.env["YUANBAO_ALLOW_ALL_USERS"] = "false";
			expect(adapter.isDmIntakeAllowed("u9")).toBe(false);

			// pairing/disabled/allowlist semantics untouched.
			process.env["YUANBAO_ALLOW_ALL_USERS"] = "true";
			const paired = new YuanbaoAdapter({
				appKey: "k",
				appSecret: "s",
				gateway: new FakeYuanbaoGateway(),
				signHttp: { postJson: async () => ({ status: 200, body: {} }) },
				dmPolicy: "pairing",
				groupPolicy: "disabled",
			});
			expect(paired.isDmIntakeAllowed("u9")).toBe(true);
			expect(paired.isGroupAllowed("g9")).toBe(false);
		} finally {
			for (const [k, v] of Object.entries(prev)) {
				if (v === undefined) delete process.env[k];
				else process.env[k] = v;
			}
		}
	});

	it("yb-7: metadata group-origin code threads onto SendC2CMessageReq field 6; plain DMs leave it empty", async () => {
		const world = await liveWorld("yb-r2-c2c-origin");
		const { engine, gateway } = world;

		await engine.deliverText("direct:u_origin", "reply body", {
			[YB_GROUP_CODE_METADATA_KEY]: "g-src",
		} as Metadata);
		const origin = gateway.sentMessages.filter(
			(s) => s.cmd === "send_c2c_message" && s.toAccount === "u_origin",
		);
		expect(origin.length).toBeGreaterThanOrEqual(1);
		for (const s of origin) expect(s.groupCode).toBe("g-src");

		await engine.deliverText("direct:u_plain", "plain body");
		const plain = gateway.sentMessages.filter(
			(s) => s.cmd === "send_c2c_message" && s.toAccount === "u_plain",
		);
		expect(plain.length).toBeGreaterThanOrEqual(1);
		for (const s of plain) expect(s.groupCode).toBe("");
	});

	it("yb-8: withheld BIND_ACK fails the handshake CLOSED at AUTH_TIMEOUT_S under the injected clock", async () => {
		const clock = new ManualClock();
		const gateway = new FakeYuanbaoGateway();
		gateway.withholdBindAck = true;
		const adapter = new YuanbaoAdapter({
			appKey: "k",
			appSecret: "s",
			gateway,
			nowMs: clock.nowMs,
			sleepMs: clock.sleepMs,
			signHttp: signOk({ token: "tok", bot_id: "b", duration: 3600 }),
		});
		const connecting = adapter.connect({ isReconnect: false });
		let settled = false;
		void connecting.then(() => {
			settled = true;
		});
		for (let i = 0; i < 80 && !settled; i++) {
			await clock.advance(500);
			await new Promise<void>((r) => setTimeout(r, 3));
		}
		expect(await connecting).toBe(false); // fail-closed, NEVER unbounded
		expect(settled).toBe(true);
		const snapshot = adapter.lifecycle.statusSnapshot();
		expect(snapshot.state).toBe("fatal");
		expect(JSON.stringify(snapshot)).toContain("AUTH_BIND timeout");
		expect(adapter.connectId).toBeNull();
		expect(adapter.running).toBe(false);
		expect(clock.nowMs()).toBeGreaterThanOrEqual(10_000); // honored AUTH_TIMEOUT_S
	}, 20_000);
});
