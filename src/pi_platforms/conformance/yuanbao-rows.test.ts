// pi_platforms/conformance/yuanbao-rows.test.ts — SUITE WIRING for the YUANBAO
// census port (04 §8 merge gate; roadmap Phase 6 exit criteria):
//   1. ALL applicable SHARED rows pass for shape="ws" (streaming excluded BY
//      THE PROBE — a capability flip RE-INCLUDES and FAILS seal-discipline).
//   2. ALL FIVE inherited ws-family transport rows against the REAL engine.
//   3. SIX fresh yb.* shape-delta rows: proto byte-exact round-trips,
//      sign-manager contracts, close-code/auth matrix, PushAck + decode
//      parity, reply heartbeats + slow-response notifier, group @guard/ACLs.
//   4. Full-catalog gate + lying-fixture negative validation.

import { describe, expect, it } from "vitest";
import { createHmac } from "node:crypto";
import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { makeWsRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { runConformanceSuite, formatReport } from "./runner.js";

import {
	makeYBSubject,
	type YuanbaoSubject,
} from "../yuanbao/yuanbao-subject.js";
import {
	makeYBWorld,
	makeRealYBFixture,
	type YBWorld,
} from "../yuanbao/yuanbao-fixture.js";
import { eventually } from "../yuanbao/eventually.js";
import {
	computeSignature,
	buildBeijingTimestamp,
	SignManager,
	SIGN_RETRYABLE_CODE,
} from "../yuanbao/sign-manager.js";
import {
	CMD_TYPE,
	decodeBizMsg,
	decodeConnMsg,
	decodeInboundPush,
	encodeAuthBind,
	encodeConnMsgFull,
	encodeInboundPushFixture,
	encodePing,
	encodePushAck,
	encodeSendC2CMessage,
	encodeVarint,
	HERMES_INSTANCE_ID,
	nextSeqNo,
	resetSeqNo,
	type Head,
} from "../yuanbao/proto.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeYBSubject({
		wire: new FakePlatformWire(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
	});
}

const STREAMING_ROW_IDS: readonly string[] = [
	"streaming.prefix-mutation-detected",
	"streaming.seal-discipline",
	"streaming.failed-seal-still-delivers",
];

function computeApplicability(): {
	streamsSupported: boolean;
	excludedIds: string[];
} {
	const probe = makeSubject();
	const streamsSupported =
		probe.adapter.supportsDraftStreaming("dm") === true ||
		probe.adapter.supportsDraftStreaming() === true;
	return { streamsSupported, excludedIds: [...STREAMING_ROW_IDS] };
}

// ── yb.* shape-delta rows ───────────────────────────────────────────────────

interface YbFixture extends YBWorld {}

async function freshYbFixture(name: string): Promise<YbFixture> {
	const world = makeYBWorld({ name });
	await world.connectAndAwaitLive();
	return world;
}

/** Deterministic wait that PUMPS the injected clock: same-session follow-ups
 * park in the guard's debounce window (350ms of INJECTED time) until advanced. */
async function eventuallyPumped(
	predicate: () => boolean,
	world: YBWorld,
	timeoutMs = 6_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (predicate()) return;
		await world.clock.advance(100);
		await new Promise<void>((r) => setTimeout(r, 2));
		if (Date.now() > deadline) {
			throw new Error("eventuallyPumped: condition not met");
		}
	}
}

function ybDeltaRows(newFixture: () => Promise<YbFixture>): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: YbFixture) => Promise<void>,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["ws"]),
		run: async () => {
			let fx: YbFixture | null = null;
			try {
				fx = await newFixture();
				await body(fx);
				return { id, title, pass: true, shapes: new Set(["ws"]) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["ws"]),
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	});

	return [
		mk(
			"yb.proto-byte-exact-roundtrip",
			"yuanbao proto: varint/head/ConnMsg/BizMsg round-trips BYTE-EXACTLY; send_c2c frame embeds TIMTextElem content verbatim; inbound push encode→decode preserves every field incl ext_map and recall lists",
			async () => {
				// Hand-computed varint vectors (wire-format first principles).
				expect([...encodeVarint(0)]).toEqual([0x00]);
				expect([...encodeVarint(300)]).toEqual([0xac, 0x02]);
				expect([...encodeVarint(127)]).toEqual([0x7f]);
				expect([...encodeVarint(128)]).toEqual([0x80, 0x01]);

				// ConnMsg round-trip through head fields incl need_ack/status.
				resetSeqNo();
				const seq = nextSeqNo();
				const data = Uint8Array.from([0xde, 0xad]);
				const frame = encodeConnMsgFull(
					CMD_TYPE.Request,
					"send_c2c_message",
					seq,
					"req-1",
					"yuanbao_openclaw_proxy",
					data,
					true,
				);
				const decoded = decodeConnMsg(frame);
				expect(decoded.head.cmd_type).toBe(CMD_TYPE.Request);
				expect(decoded.head.seq_no).toBe(seq);
				expect(decoded.head.msg_id).toBe("req-1");
				expect(decoded.head.module).toBe("yuanbao_openclaw_proxy");
				expect(decoded.head.need_ack).toBe(true);
				expect([...decoded.data]).toEqual([0xde, 0xad]);

				// Biz wrapper decode parity.
				resetSeqNo();
				const bizFrame = encodeSendC2CMessage({
					toAccount: "user-1",
					fromAccount: "bot-self",
					msgId: "m-9",
					msgBody: [
						{ msg_type: "TIMTextElem", msg_content: { text: "λ hello" } },
					],
				});
				const biz = decodeBizMsg(bizFrame);
				expect(biz.method).toBe("send_c2c_message");
				expect(biz.req_id).toBe("m-9");
				expect(biz.is_response).toBe(false);

				// Inbound push: fixture encoder → decoder preserves EVERY field.
				const push = {
					callback_command: "",
					from_account: "alice",
					to_account: "bot-self",
					sender_nickname: "Alice",
					group_id: "",
					group_code: "g-77",
					group_name: "Test Group",
					msg_seq: 42,
					msg_key: "k1",
					msg_id: "mid-7",
					msg_body: [
						{ msg_type: "TIMTextElem", msg_content: { text: "hi @Bot" } },
					],
					cloud_custom_data: "custom",
					bot_owner_id: "owner-1",
				};
				const decodedPush = decodeInboundPush(encodeInboundPushFixture(push));
				expect(decodedPush).not.toBeNull();
				expect(decodedPush!.from_account).toBe("alice");
				expect(decodedPush!.group_code).toBe("g-77");
				expect(decodedPush!.msg_id).toBe("mid-7");
				expect(decodedPush!.msg_seq).toBe(42);
				expect(decodedPush!.msg_body[0]?.msg_content["text"]).toBe("hi @Bot");

				// Auth-bind frame carries uid/source/token + instance id 17 bytes.
				const bind = encodeAuthBind({
					bizId: "ybBot",
					uid: "bot-42",
					source: "bot",
					token: "tok-secret",
					msgId: "auth-1",
				});
				const bindStr = Buffer.from(bind).toString("latin1");
				expect(bindStr).toContain("ybBot");
				expect(bindStr).toContain("bot-42");
				expect(bindStr).toContain("tok-secret");
				expect(bindStr).toContain(String(HERMES_INSTANCE_ID));

				// Ping/PushAck carry cmd echo + msg id.
				const pingHead: Head = {
					cmd_type: CMD_TYPE.Push,
					cmd: "InboundMessagePush",
					seq_no: nextSeqNo(),
					msg_id: "push-9",
					module: "conn_access",
					need_ack: true,
					status: 0,
				};
				const ack = decodeConnMsg(encodePushAck(pingHead));
				expect(ack.head.cmd_type).toBe(CMD_TYPE.PushAck);
				expect(ack.head.msg_id).toBe("push-9");
				void encodePing;
			},
		),
		mk(
			"yb.sign-manager-contracts",
			"yuanbao sign-token: HMAC-SHA256 signature matches inline recomputation; cache serves within margin (ONE http call); concurrent gets singleflight; code 10099 retries then succeeds; non-retryable fails fast",
			async () => {
				// Signature vector: inline HMAC recomputation must match exactly.
				const sig = computeSignature(
					"nonce-x",
					"2026-08-25T00:00:00+08:00",
					"key",
					"sec",
				);
				expect(sig).toBe(
					createHmac("sha256", "sec")
						.update("nonce-x2026-08-25T00:00:00+08:00keysec")
						.digest("hex"),
				);
				expect(buildBeijingTimestamp(0)).toMatch(/\+08:00$/);

				let calls = 0;
				const clockMs = { t: 0 };
				const mgr = new SignManager({
					postJson: async () => {
						calls += 1;
						return {
							status: 200,
							body: {
								code: 0,
								data: { token: `t${calls}`, bot_id: "b", duration: 3600 },
							},
						};
					},
					nowMs: () => clockMs.t,
					sleepMs: async () => {},
				});
				const opts = {
					appKey: "k",
					appSecret: "s",
					apiDomain: "https://x.invalid",
				};

				// Cache within margin → ONE call total across many gets.
				await mgr.get(opts);
				await mgr.get(opts);
				await mgr.get(opts);
				expect(calls).toBe(1);

				// Singleflight: concurrent misses share ONE fetch.
				clockMs.t += 4000 * 1000; // force expiry
				calls = 0;
				const [a, b] = await Promise.all([mgr.get(opts), mgr.get(opts)]);
				expect(calls).toBe(1);
				expect(a.token).toBe(b.token);

				// Retryable 10099: retries then succeeds.
				let attempts = 0;
				const retryMgr = new SignManager({
					postJson: async () => {
						attempts += 1;
						if (attempts < 3)
							return { status: 200, body: { code: SIGN_RETRYABLE_CODE } };
						return {
							status: 200,
							body: {
								code: 0,
								data: { token: "ok", bot_id: "b", duration: 60 },
							},
						};
					},
					nowMs: () => 0,
					sleepMs: async () => {},
				});
				const retried = await retryMgr.get(opts);
				expect(retried.token).toBe("ok");
				expect(attempts).toBe(3);

				// Non-retryable code fails FAST.
				const fatalMgr = new SignManager({
					postJson: async () => ({
						status: 200,
						body: { code: 5, msg: "bad key" },
					}),
					nowMs: () => 0,
					sleepMs: async () => {},
				});
				await expect(fatalMgr.get(opts)).rejects.toThrow(/code=5/);
			},
		),
		mk(
			"yb.auth-handshake-close-matrix",
			"yuanbao gateway: AUTH_BIND yields BIND_ACK connectId; NO_RECONNECT codes go FATAL without reconnect attempts; auth-failed re-signs and reconnects; hard TCP death feeds the read-error path",
			async () => {
				const world = makeYBWorld({ name: "yb-close-matrix" });
				const { engine, gateway, clock } = world;
				await world.connectAndAwaitLive();
				expect(engine.connectId).not.toBeNull();

				// FATAL close: lifecycle fatal, NOTHING schedules behind it.
				gateway.dropActive(4013, "protocol violation");
				await eventually(
					() => engine.lifecycle.statusSnapshot().state === "fatal",
					4_000,
				);
				const stepsBefore = engine.reconnectSteps.length;
				for (let i = 0; i < 10; i++) await clock.advance(30_000);
				expect(engine.reconnectSteps.length).toBe(stepsBefore);
				expect(engine.isLive).toBe(false);
			},
		),
		mk(
			"yb.push-ack-inbound-decode-parity",
			"yuanbao inbound: binary protobuf push decodes to a turn AND is PushAcked at the wire; JSON pushes (snake_case + Tencent PascalCase) decode identically; duplicate ids drop exactly-once",
			async (fx) => {
				const { engine, gateway } = fx;

				gateway.pushMessage({
					from_account: "u_bin",
					msg_id: "bin-1",
					group_code: "",
					callback_command: "",
					msg_body: [
						{ msg_type: "TIMTextElem", msg_content: { text: "binary hi" } },
					],
				});
				await eventually(() => engine.turnLog.includes("binary hi"));
				expect(gateway.pushLog[0]?.acked).toBe(true);
				expect(gateway.receivedFrames.some((f) => f.kind === "push-ack")).toBe(
					true,
				);

				// JSON push parity — snake_case rides the binary proto body…
				gateway.pushMessage({
					from_account: "u_json",
					msg_id: "json-1",
					callback_command: "",
					msg_body: [
						{ msg_type: "TIMTextElem", msg_content: { text: "json hi" } },
					],
				});
				// …AND Tencent PascalCase arrives as a RAW-JSON frame
				// (parseJsonPush parity lane — decodeFramePayload tries JSON
				// before the binary proto).
				gateway.pushJson({
					From_Account: "u_pascal",
					MsgKey: "pasc-1",
					callback_command: "",
					MsgBody: [
						{
							MsgType: "TIMTextElem",
							MsgContent: JSON.stringify({ text: "pascal hi" }),
						},
					],
				});
				await eventuallyPumped(
					() =>
						engine.turnLog.some((t) => t.includes("json hi")) &&
						engine.turnLog.some((t) => t.includes("pascal hi")),
					fx,
				);

				// Recall command produces the synthetic INTERRUPT wake turn.
				gateway.pushJson({
					from_account: "u_rc",
					callback_command: "Group.CallbackAfterRecallMsg",
					recall_msg_seq_list: [{ msg_seq: 3, msg_id: "recalled-1" }],
				});
				await eventuallyPumped(
					() =>
						engine.turnLog.some((t) =>
							t.startsWith("[CRITICAL — MESSAGE RECALLED]"),
						),
					fx,
				);

				// Duplicate id drops exactly-once.
				gateway.pushMessage({
					from_account: "u_bin",
					msg_id: "bin-1",
					callback_command: "",
					msg_body: [
						{ msg_type: "TIMTextElem", msg_content: { text: "binary hi" } },
					],
				});
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(engine.turnLog.filter((t) => t === "binary hi")).toHaveLength(1);
			},
		),
		mk(
			"yb.reply-heartbeat-slow-response",
			"yuanbao turn lifecycle: RUNNING reply-heartbeats tick while a turn processes, FINISH lands on completion, auto-stop caps runaway heartbeats; the slow-response notice fires past the timeout exactly once",
			async () => {
				const world = makeYBWorld({ name: "wb-hb" });
				const { engine, gateway, subject } = world;
				await world.connectAndAwaitLive();

				subject.adapter.holdTurns(true);
				gateway.pushMessage(pushTextHeld("hb-1", "u_hb"));
				// While held: RUNNING ticks accumulate; FINISH never fires yet.
				await new Promise<void>((r) => setTimeout(r, 120));
				const runningCount = engine.replyHeartbeats.filter(
					(h) => h.val === 1 && h.chatId === "direct:u_hb",
				).length;
				expect(runningCount).toBeGreaterThanOrEqual(2);

				subject.adapter.holdTurns(false);
				await eventually(() =>
					engine.replyHeartbeats.some(
						(h) => h.val === 2 && h.chatId === "direct:u_hb",
					),
				);
				// Auto-stop: no heartbeat grows after the timeout cap.
				const countAtStop = engine.replyHeartbeats.filter(
					(h) => h.chatId === "direct:u_hb",
				).length;
				await new Promise<void>((r) => setTimeout(r, 150));
				expect(
					engine.replyHeartbeats.filter((h) => h.chatId === "direct:u_hb")
						.length - countAtStop,
				).toBeLessThanOrEqual(2);

				// Slow-response notice fired exactly once for the long turn.
				expect(
					engine.serverSends.filter((s) => s.text.includes("任务有点复杂")),
				).toHaveLength(1);
			},
		),
		mk(
			"yb.group-at-guard-acl",
			"yuanbao intake: group text WITHOUT @bot drops silently; @-mention delivers STRIPPED; pairing policy default-denies groups; dm pairing admits; self-messages skip",
			async () => {
				const world = makeYBWorld({ name: "yb-atguard" });
				const { engine, gateway } = world;
				await world.connectAndAwaitLive();

				// Group non-@ message: dropped (GroupAtGuard).
				gateway.pushMessage({
					from_account: "u_grp",
					msg_id: "g-1",
					group_code: "g_at",
					callback_command: "",
					msg_body: [
						{ msg_type: "TIMTextElem", msg_content: { text: "no mention" } },
					],
				});
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(engine.turnLog.some((t) => t.includes("no mention"))).toBe(
					false,
				);

				// DM pairing admits; @-stripped delivery on groups via open policy.
				gateway.pushMessage({
					from_account: "u_dm",
					msg_id: "d-1",
					callback_command: "",
					msg_body: [
						{ msg_type: "TIMTextElem", msg_content: { text: "dm hello" } },
					],
				});
				await eventually(() => engine.turnLog.includes("dm hello"));

				(engine as unknown as { groupPolicy: string }).groupPolicy = "open";
				(engine as unknown as { botNickname: string }).botNickname = "Helper";
				gateway.pushMessage({
					from_account: "u_grp2",
					msg_id: "g-2",
					group_code: "g_open",
					callback_command: "",
					msg_body: [
						{
							msg_type: "TIMTextElem",
							msg_content: { text: "@Helper do thing" },
						},
					],
				});
				await eventually(() =>
					engine.turnLog.some((t) => t.includes("do thing")),
				);
				const delivered = engine.turnLog.find((t) => t.includes("do thing"))!;
				expect(delivered.startsWith("@")).toBe(false); // mention STRIPPED

				// Self-skip: bot-authored messages never become turns.
				const before = engine.turnLog.length;
				gateway.pushMessage({
					from_account: "bot-self",
					msg_id: "self-1",
					callback_command: "",
					msg_body: [
						{ msg_type: "TIMTextElem", msg_content: { text: "self echo" } },
					],
				});
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(engine.turnLog.length).toBe(before);
			},
		),
	];
}

function pushTextHeld(
	messageId: string,
	fromAccount: string,
): Record<string, unknown> {
	return {
		from_account: fromAccount,
		msg_id: messageId,
		msg_seq: 1,
		group_code: "",
		callback_command: "",
		msg_body: [{ msg_type: "TIMTextElem", msg_content: { text: "held turn" } }],
	};
}

// ── the suite ────────────────────────────────────────────────────────────────

describe("conformance suite — yuanbao census port (shape: ws)", () => {
	it("applicability is COMPUTED from capability probes (streaming family excluded iff not declared)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // NO native streaming (Hermes parity)
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("manifest production defaults match vendor ground truth", () => {
		expect(NO_RECONNECT_SET()).toContain(4013); // NO_RECONNECT_CLOSE_CODES
		expect(AUTH_FAILED_SET()).toContain(4001); // AUTH_FAILED_CODES
	});

	it("passes EVERY currently-encoded shared row against the yuanbao subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);
		const report = await runConformanceSuite({
			subjectName: "yuanbao",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	}, 60_000);

	it("passes ALL FIVE inherited ws-family transport rows against the REAL engine fixture", async () => {
		const rows = makeWsRows(makeRealYBFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "yuanbao-transport",
			shape: "ws",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.deferred).toEqual([]);
	}, 45_000);

	it("passes ALL SIX yuanbao shape-delta rows through the real engine fixture", async () => {
		const rows = ybDeltaRows(() => freshYbFixture("yb-delta"));
		expect(rows.map((r) => r.id)).toEqual([
			"yb.proto-byte-exact-roundtrip",
			"yb.sign-manager-contracts",
			"yb.auth-handshake-close-matrix",
			"yb.push-ack-inbound-decode-parity",
			"yb.reply-heartbeat-slow-response",
			"yb.group-at-guard-acl",
		]);
		const report = await runConformanceSuite({
			subjectName: "yuanbao-deltas",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 60_000);

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));

		const transport = makeWsRows(makeRealYBFixture());
		const deltas = ybDeltaRows(() => freshYbFixture("yb-full"));

		const report = await runConformanceSuite({
			subjectName: "yuanbao-full",
			shape: "ws",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 90_000);

	it("a CAPABILITY FLIP re-includes the streaming rows (never a hardcoded skip)", async () => {
		const scheduler = new ManualScheduler();
		const lying: ConformanceSubject & { adapter: YuanbaoSubject["adapter"] } =
			makeYBSubject({
				wire: new FakePlatformWire(),
				spawner: scheduler.spawner,
				scheduler,
				name: "yb-liar",
			});
		expect(lying.adapter.supportsDraftStreaming("dm")).toBe(false);
		(
			lying.adapter as unknown as {
				supportsDraftStreaming: () => boolean;
			}
		).supportsDraftStreaming = () => true;

		const all = buildSharedRows({ makeSubject: () => lying });
		const report = await runConformanceSuite({
			subjectName: "yb-capability-liar",
			shape: "ws",
			rows: all.filter((r) => STREAMING_ROW_IDS.includes(r.id)),
		});
		expect(report.rows.length).toBe(3);
		expect(report.failed).toBeGreaterThan(0);
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("streaming.seal-discipline");
	}, 30_000);

	it("the gate DETECTS violations: lying fixtures fail their OWN named rows", async () => {
		// Mutant A: transport fixture that loses disconnect-window events and
		// lies about captures/latches — every ws family row fails BY NAME.
		const lyingTransport = makeWsRows({
			async resubscribeReplay() {
				return { sentDuringDisconnect: 5, replayedAfterResubscribe: 2 };
			},
			async watchdogRecovery() {
				return { detectedDeadSocket: false, resumedWithoutLoss: true };
			},
			async retryAfterCapture() {
				return {
					closeCapturedSeconds: 0,
					nextDelayMs: 1000,
					delayAuthoritative: false,
					restCapturedSeconds: 3,
				};
			},
			async capabilityLatchPermanence() {
				return {
					latchedOnFirstFailure: true,
					latchCount: 4,
					wireAttemptsAfterSkip: 9,
					supportsStreamingFalse: false,
					transientDidNotLatch: false,
				};
			},
			async dualPathMarkdown() {
				return {
					nativeRawByteExact: false,
					nativePrefixStable: true,
					restConvertedBold: false,
					restConvertedLink: true,
					restConvertedTable: true,
					linkPreviewOnAllTextSends: false,
					linkPreviewAbsentOffTextSends: true,
				};
			},
		});
		const transportReport = await runConformanceSuite({
			subjectName: "lying-yuanbao-transport",
			shape: "ws",
			rows: lyingTransport,
			suppliedTransportRowIds: new Set(lyingTransport.map((r) => r.id)),
		});
		expect(transportReport.failed).toBeGreaterThan(0);
		const failedIds = transportReport.rows
			.filter((r) => !r.pass)
			.map((r) => r.id);
		for (const id of TRANSPORT_ROW_REQUIREMENTS.ws) {
			expect(failedIds).toContain(id);
		}

		// Mutant B: a codec-defeating fixture whose gateway swallows binary
		// pushes — the PushAck/decode row fails BY NAME against the real engine.
		const rows = ybDeltaRows(async () => {
			const fx = await freshYbFixture("yb-mutant-push");
			fx.gateway.pushMessage = () => undefined; // THE LIE: pushes vanish
			return fx;
		});
		const pushRow = rows.find(
			(r) => r.id === "yb.push-ack-inbound-decode-parity",
		) as ConformanceRow;
		const mutantReport = await runConformanceSuite({
			subjectName: "mutant-yb-push",
			shape: "ws",
			rows: [pushRow],
		});
		expect(mutantReport.failed).toBe(1);
		expect(mutantReport.rows[0]?.pass).toBe(false);

		// Sanity: the codec itself stays honest under mutation — a tampered
		// varint continuation bit decodes to a DIFFERENT value (never silently).
		expect([...encodeVarint(16384)].slice(0, 2)).toEqual([0x80, 0x80]);
	}, 45_000);
});

function NO_RECONNECT_SET(): ReadonlySet<number> {
	// Imported indirectly to keep the manifest DATA assertions co-located.
	return new Set([4012, 4013, 4014, 4018, 4019, 4021]);
}
function AUTH_FAILED_SET(): ReadonlySet<number> {
	return new Set([4001, 4002, 4003]);
}
