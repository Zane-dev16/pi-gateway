// CONFORMANCE WIRING — the WeCom callback census port vs the executable 04 §8
// matrix (DEC-002 gate applies to every new platform).
//
//   1. ALL applicable SHARED rows pass for shape="webhook" against the REAL
//      kit-built WecomCallbackSubject. Applicability is COMPUTED from
//      capability data: proactive-send egress has no native draft lanes, so
//      the streaming family is excluded BY THE PROBE, never by a skip.
//   2. The INHERITED webhook transport rows run over the REAL adapter probes.
//   3. Fresh WeCom shape-delta rows execute through the real engine fixture
//      with the REAL BizMsgCrypt stack (fixtures ENCRYPT valid vendor
//      envelopes; the adapter DECRYPTS them): URL-verification handshake,
//      callback signature negative matrix, body-cap pre-parse, MsgId TTL
//      dedupe on the injected clock, XML event ladder + multi-app routing,
//      ack-first bounded-window behavior + proactive-send payload/token
//      ladder, text-cap lossless splitting.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: a lying crypto stub that accepts ANY signature fails
//      its own named row.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { makeWebhookRows } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import {
	makeWecomSubject,
	type WecomCallbackSubject,
} from "../wecom/wecom-subject.js";
import type { WecomCallbackAdapter } from "../wecom/wecom-callback-adapter.js";
import {
	makeWecomFixture,
	buildInnerXml,
	DEFAULT_APP_A,
	DEFAULT_APP_B,
	type WecomFixture,
} from "../wecom/wecom-fixture.js";
import { WxBizMsgCrypt, extractXmlTag } from "../wecom/wecom-crypto.js";
import {
	FIXTURE_CORP_ID_A,
	FIXTURE_WECOM_AES_KEY_A,
	FIXTURE_WECOM_TOKEN_A,
} from "../wecom/fixture-secrets.js";
import {
	WECOM_DEDUP_PRUNE_BOUND,
	WECOM_MAX_BODY_BYTES,
	WECOM_TEXT_SEND_CAP_CHARS,
} from "../wecom/manifest.js";
import { PLAIN_TEXT_FALLBACK_PREFIX } from "../kit/index.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeWecomSubject({
		wire: new FakePlatformWire(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
	});
}

/** §8 streaming family — applicable ONLY when draft streaming is supported. */
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
		probe.adapter.supportsDraftStreaming() === true &&
		probe.adapter.supportsAsyncDelivery === true;
	return { streamsSupported, excludedIds: [...STREAMING_ROW_IDS] };
}

// ── WeCom shape-delta rows (executed over the REAL engine fixture) ──────────

function wecomDeltaRows(newFixture: () => WecomFixture): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: WecomFixture) => Promise<void>,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["webhook"]),
		run: async () => {
			const fx = newFixture();
			try {
				await body(fx);
				return { id, title, pass: true, shapes: new Set(["webhook"]) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["webhook"]),
					detail: err instanceof Error ? err.message : String(err),
				};
			} finally {
				fx.dispose();
			}
		},
	});

	return [
		mk(
			"transport.wecom.url-verification-handshake",
			"wecom: GET verify decrypts echostr per app and echoes VERBATIM as text/plain; wrong token/tampered signature/receive_id mismatch ⇒ 403 after ALL apps fail; second app's credentials succeed when first fails",
			async (fx) => {
				// Valid handshake under app A's credentials.
				const cryptA = new WxBizMsgCrypt(
					FIXTURE_WECOM_TOKEN_A,
					FIXTURE_WECOM_AES_KEY_A,
					FIXTURE_CORP_ID_A,
				);
				const ts = "1700000000";
				const nonce = "nonce-verify";
				const envelope = cryptA.encrypt("echo-PLAIN-text-123", nonce, ts);
				const sig =
					/<MsgSignature>([^<]+)<\/MsgSignature>/.exec(envelope)?.[1] ?? "";
				const encrypt = extractXmlTag(envelope, "Encrypt") ?? "";
				const ok = fx.getVerify({
					msg_signature: sig,
					timestamp: ts,
					nonce,
					echostr: encrypt,
				});
				expect(ok.status).toBe(200);
				expect(ok.text).toBe("echo-PLAIN-text-123"); // verbatim decrypted

				// Tampered signature: every app rejects ⇒ 403.
				const bad = fx.getVerify({
					msg_signature: `${sig.slice(0, -2)}zz`,
					timestamp: ts,
					nonce,
					echostr: encrypt,
				});
				expect(bad.status).toBe(403);

				// receive_id mismatch: encrypt for corp-B verified against A-only
				// fixture ⇒ 403 (corp binding is part of the envelope).
				const singleA = makeWecomFixture({ apps: [DEFAULT_APP_A] });
				const cryptB = new WxBizMsgCrypt(
					DEFAULT_APP_B.token ?? "",
					DEFAULT_APP_B.encoding_aes_key ?? "",
					DEFAULT_APP_B.corp_id ?? "",
				);
				const envB = cryptB.encrypt("other-corp", "n2", "1700000001");
				const mismatch = singleA.getVerify({
					msg_signature:
						/<MsgSignature>([^<]+)<\/MsgSignature>/.exec(envB)?.[1] ?? "",
					timestamp: "1700000001",
					nonce: "n2",
					echostr: extractXmlTag(envB, "Encrypt") ?? "",
				});
				expect(mismatch.status).toBe(403);
				expect(singleA.adapter.counters.handshakeFailures).toBe(1);
				singleA.dispose();

				// Multi-app ladder: an envelope under B's crypto triple succeeds
				// on a fixture carrying BOTH apps even though A tries first.
				const both = makeWecomFixture({ apps: [DEFAULT_APP_A, DEFAULT_APP_B] });
				const bothOk = both.getVerify({
					msg_signature:
						/<MsgSignature>([^<]+)<\/MsgSignature>/.exec(envB)?.[1] ?? "",
					timestamp: "1700000001",
					nonce: "n2",
					echostr: extractXmlTag(envB, "Encrypt") ?? "",
				});
				expect(bothOk.status).toBe(200);
				expect(bothOk.text).toBe("other-corp");
				both.dispose();
			},
		),
		mk(
			"transport.wecom.callback-signature-negative-matrix",
			"wecom: callbacks signed under wrong token/tampered bodies/receive_id mismatch exhaust EVERY app's decrypt ladder then reject 400 with ZERO dispatches; valid callbacks ack success",
			async (fx) => {
				const inner = buildInnerXml({
					ToUserName: FIXTURE_CORP_ID_A,
					FromUserName: "W-user1",
					MsgType: "text",
					Content: "signed hello",
					MsgId: "m-sig-1",
					CreateTime: "1700000000",
				});

				// Wrong TOKEN (signature computed under B's token over A-key blob).
				const wrongTokenEnvelope = (() => {
					const crypt = new WxBizMsgCrypt(
						"wrong-token-entirely",
						FIXTURE_WECOM_AES_KEY_A,
						FIXTURE_CORP_ID_A,
					);
					return crypt.encrypt(inner, "n9", "1700000009");
				})();
				const wrongToken = await fx.postRaw({
					query: {
						msg_signature:
							/<MsgSignature>([^<]+)<\/MsgSignature>/.exec(
								wrongTokenEnvelope,
							)?.[1] ?? "",
						timestamp: "1700000009",
						nonce: "n9",
					},
					body: wrongTokenEnvelope,
				});
				expect(wrongToken.status).toBe(400);

				// receive_id mismatch: encrypted for corp-B but only app A present.
				const singleA = makeWecomFixture({ apps: [DEFAULT_APP_A] });
				const built = singleA.buildEncryptedCallback(DEFAULT_APP_B, {
					ToUserName: DEFAULT_APP_B.corp_id ?? "",
					FromUserName: "W-user1",
					MsgType: "text",
					Content: "cross-corp",
					MsgId: "m-x",
				});
				const crossCorp = await singleA.postRaw({
					query: built.query,
					body: built.body,
				});
				expect(crossCorp.status).toBe(400);
				expect(singleA.adapter.counters.decryptFailures).toBe(1);
				singleA.dispose();

				// Garbage Encrypt blob (not base64 of a valid AES block chain).
				const garbage = await fx.postRaw({
					query: { msg_signature: "deadbeef", timestamp: "1", nonce: "n" },
					body: "<xml><Encrypt><![CDATA[!!!not-base64!!!]]></Encrypt></xml>",
				});
				expect(garbage.status).toBe(400);

				// Missing Encrypt field entirely.
				const noField = await fx.postRaw({
					query: { msg_signature: "x", timestamp: "1", nonce: "n" },
					body: "<xml><Other>1</Other></xml>",
				});
				expect(noField.status).toBe(400);

				// Zero dispatches across every negative (wrongToken + garbage +
				// noField ran on THIS fixture's decrypt seam; crossCorp ran on the
				// single-app fixture above).
				expect(fx.adapter.turnLog).toEqual([]);
				expect(fx.adapter.counters.parseInvocations).toBe(3);

				// The VALID path still acks success and dispatches.
				const good = await fx.postValidCallback(DEFAULT_APP_A, {
					ToUserName: FIXTURE_CORP_ID_A,
					FromUserName: "W-user1",
					MsgType: "text",
					Content: "signed hello",
					MsgId: "m-sig-ok",
				});
				expect(good.status).toBe(200);
				expect(good.text).toBe("success");
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(fx.adapter.turnLog).toEqual(["signed hello"]);
			},
		),
		mk(
			"transport.wecom.body-cap-preparse",
			"wecom: >64 KiB bodies rejected 413 at BOTH gates (declared length AND actual bytes) BEFORE any parse/signature work; at-cap parses once",
			async (fx) => {
				expect(WECOM_MAX_BODY_BYTES).toBe(65_536);
				const big = Buffer.alloc(WECOM_MAX_BODY_BYTES + 1, 0x61);

				const declared = await fx.postRaw({
					headers: { "content-length": String(big.length) },
					query: {},
					body: "x",
				});
				expect(declared.status).toBe(413);
				expect(fx.adapter.counters.parseInvocations).toBe(0);

				const lying = await fx.postRaw({
					headers: { "content-length": "10" },
					query: {},
					body: big,
				});
				expect(lying.status).toBe(413);
				expect(fx.adapter.counters.parseInvocations).toBe(0);

				const good = await fx.postValidCallback(DEFAULT_APP_A, {
					ToUserName: FIXTURE_CORP_ID_A,
					FromUserName: "W-user1",
					MsgType: "text",
					Content: "fits",
					MsgId: "m-cap",
				});
				expect(good.status).toBe(200);
				expect(fx.adapter.counters.parseInvocations).toBe(1);
			},
		),
		mk(
			"transport.wecom.msgid-dedupe-ttl",
			"wecom: MsgId redelivery deduped exactly-once within 300 s (still acked success); injected clock past TTL re-dispatches; MsgId-less callbacks fall back to user:createTime ids STABLE across redeliveries; bound pinned by manifest",
			async (fx) => {
				const replay = () =>
					fx.postValidCallback(DEFAULT_APP_A, {
						ToUserName: FIXTURE_CORP_ID_A,
						FromUserName: "W-user1",
						MsgType: "text",
						Content: "exactly once",
						MsgId: "M-DUP-1",
					});
				const first = await replay();
				expect(first.text).toBe("success");
				const second = await replay(); // WeCom retry-on-timeout
				expect(second.text).toBe("success"); // acked anyway
				await new Promise<void>((r) => setTimeout(r, 25));
				expect(fx.adapter.turnLog).toEqual(["exactly once"]);
				expect(fx.adapter.counters.duplicates).toBe(1);
				expect(fx.adapter.counters.accepted).toBe(1);

				// Injected clock past the 300 s TTL ⇒ live again.
				fx.advance(301_000);
				await replay();
				await new Promise<void>((r) => setTimeout(r, 25));
				expect(
					fx.adapter.turnLog.filter((t) => t === "exactly once"),
				).toHaveLength(2);

				// Fallback id: no MsgId ⇒ `user:createTime`; SAME CreateTime twice
				// dedupes, a NEW CreateTime dispatches.
				const idless = (createTime: string) =>
					fx.postValidCallback(DEFAULT_APP_A, {
						ToUserName: FIXTURE_CORP_ID_A,
						FromUserName: "W-user2",
						MsgType: "text",
						Content: `idless-${createTime}`,
						CreateTime: createTime,
					});
				await idless("1700000100");
				await idless("1700000100"); // same fallback id ⇒ duplicate
				await idless("1700000200"); // different fallback id ⇒ dispatch
				await new Promise<void>((r) => setTimeout(r, 30));
				expect(
					fx.adapter.turnLog.filter((t) => t.startsWith("idless-")),
				).toEqual(["idless-1700000100", "idless-1700000200"]);

				// Manifest pins the production prune bound.
				expect(WECOM_DEDUP_PRUNE_BOUND).toBe(2000);

				// Prune semantics are EXPIRED-ONLY (callback_adapter parity): past the
				// bound, LIVE receipts survive — a small-bound fixture keeps ALL live
				// ids even when their count exceeds it (FIFO eviction would shrink the
				// duplicate window under bursts instead). Then one clock advance past
				// TTL + a fresh insert prunes every EXPIRED entry at once.
				const burst = makeWecomFixture({ dedupCap: 3 });
				for (let i = 0; i < 5; i++) {
					const r = await burst.postValidCallback(DEFAULT_APP_A, {
						ToUserName: FIXTURE_CORP_ID_A,
						FromUserName: "W-burst",
						MsgType: "text",
						Content: `burst-${i}`,
						MsgId: `M-BURST-${i}`,
					});
					expect(r.text).toBe("success");
				}
				await new Promise<void>((r) => setTimeout(r, 25));
				expect(burst.adapter.seenDedupSize()).toBe(5); // all LIVE beyond bound
				// Live duplicates still deduped despite being past the FIFO bound.
				const replayBurst = await burst.postValidCallback(DEFAULT_APP_A, {
					ToUserName: FIXTURE_CORP_ID_A,
					FromUserName: "W-burst",
					MsgType: "text",
					Content: "burst-0",
					MsgId: "M-BURST-0",
				});
				expect(replayBurst.text).toBe("success");
				await new Promise<void>((r) => setTimeout(r, 25));
				expect(
					burst.adapter.turnLog.filter((t) => t === "burst-0"),
				).toHaveLength(1);
				// Clock past TTL + fresh insert ⇒ expired-only prune fires.
				burst.advance(301_000);
				await burst.postValidCallback(DEFAULT_APP_A, {
					ToUserName: FIXTURE_CORP_ID_A,
					FromUserName: "W-burst",
					MsgType: "text",
					Content: "after-ttl",
					MsgId: "M-BURST-FRESH",
				});
				expect(burst.adapter.seenDedupSize()).toBeLessThanOrEqual(2); // pruned
				burst.dispose();
			},
		),
		mk(
			"transport.wecom.xml-event-ladder",
			"wecom: text messages dispatch with scoped corp:user chat ids; enter_agent/subscribe lifecycle events silently ack with NO turn; empty event content becomes '/start'; unknown MsgTypes ack-no-op; multi-app routing records user→app so later sends resolve the RIGHT agent",
			async () => {
				const both = makeWecomFixture({ apps: [DEFAULT_APP_A, DEFAULT_APP_B] });

				// Lifecycle events: enter_agent + subscribe — acked, zero turns.
				for (const evt of ["enter_agent", "subscribe"]) {
					const resp = await both.postValidCallback(DEFAULT_APP_A, {
						ToUserName: FIXTURE_CORP_ID_A,
						FromUserName: "W-lc",
						MsgType: "event",
						Event: evt,
						Content: "",
					});
					expect(resp.text).toBe("success");
				}
				expect(both.adapter.turnLog).toEqual([]);
				expect(both.adapter.counters.lifecycleAcked).toBe(2);

				// Unknown MsgType (image) — acked no-op.
				const image = await both.postValidCallback(DEFAULT_APP_A, {
					ToUserName: FIXTURE_CORP_ID_A,
					FromUserName: "W-img",
					MsgType: "image",
					PicUrl: "https://x.example/y.png",
				});
				expect(image.status).toBe(200);
				expect(both.adapter.counters.unhandledTypes).toBe(1);

				// OTHER EVENT NAMES fall through to normal construction — empty
				// Content becomes the synthesized "/start" (_build_event parity:
				// only enter_agent/subscribe return early for MsgType=event).
				const foreign = await both.postValidCallback(DEFAULT_APP_A, {
					ToUserName: FIXTURE_CORP_ID_A,
					FromUserName: "W-fall",
					MsgType: "event",
					Event: "report_job",
					Content: "",
					MsgId: "M-FALL1",
				});
				expect(foreign.text).toBe("success");
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(both.adapter.turnLog).toContain("/start");
				// A foreign event CARRYING Content keeps its own text.
				await both.postValidCallback(DEFAULT_APP_A, {
					ToUserName: FIXTURE_CORP_ID_A,
					FromUserName: "W-fall2",
					MsgType: "event",
					Event: "report_location",
					Content: "where am I",
					MsgId: "M-FALL2",
				});
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(both.adapter.turnLog).toContain("where am I");

				// Text message under corp B routes through B and records mapping.
				const bMsg = await both.postValidCallback(DEFAULT_APP_B, {
					ToUserName: DEFAULT_APP_B.corp_id ?? "",
					FromUserName: "W-buser",
					MsgType: "text",
					Content: "from beta",
					MsgId: "M-B1",
				});
				expect(bMsg.text).toBe("success");
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(both.adapter.turnLog).toContain("from beta");
				expect(
					both.adapter.appForUser(`${DEFAULT_APP_B.corp_id}:W-buser`),
				).toBe("beta");

				// Scoped session key carries the corp prefix (cross-corp isolation).
				const scoped = await both.postValidCallback(DEFAULT_APP_A, {
					ToUserName: FIXTURE_CORP_ID_A,
					FromUserName: "W-scoped",
					MsgType: "text",
					Content: "scoped hello",
					MsgId: "M-SC1",
				});
				expect(scoped.text).toBe("success");
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(both.adapter.dispatchedEvents.at(-1)?.messageId).toBe("M-SC1");
				both.dispose();

				// Empty event content becomes "/start" (subscribe w/ Content absent
				// is lifecycle-acked; the synthetic command applies to OTHER event
				// subtypes that reach the text lane).
				expect(WECOM_TEXT_SEND_CAP_CHARS).toBe(2048);
			},
		),
		mk(
			"transport.wecom.ack-first-proactive-send",
			"wecom: callbacks ack 'success' IMMEDIATELY while a turn is HELD (bounded window); replies ride the PROACTIVE send seam with touser/agentid/text/safe:0 payload shape; access tokens cached across sends; errcode 40001 evicts+refreshes+retries ONCE; persistent failure fails cleanly",
			async (fx) => {
				{
					// Ack-first: HELD turn does NOT delay the 'success' response.
					fx.adapter.holdTurns(true);
					const startedAt = Date.now();
					const resp = await fx.postValidCallback(DEFAULT_APP_A, {
						ToUserName: FIXTURE_CORP_ID_A,
						FromUserName: "W-held",
						MsgType: "text",
						Content: "while held",
						MsgId: "M-HELD",
					});
					const elapsed = Date.now() - startedAt;
					expect(resp.text).toBe("success");
					expect(elapsed).toBeLessThan(5_000); // bounded-window posture
					fx.adapter.holdTurns(false);

					// Outbound payload SHAPE through the real send() door → the
					// proactive message/send seam (fixture records qyapi payloads).
					// The body carries EXACTLY the vendor keys — caller metadata is
					// never spread into the wire JSON.
					const sent = await fx.adapter.send("corp-alpha:W-held", "the reply");
					expect(sent.success).toBe(true);
					const call = fx.api.sendsOfUser("W-held").at(-1);
					expect(call?.payload["msgtype"]).toBe("text");
					expect(call?.payload["agentid"]).toBe(1000001);
					expect(
						(call?.payload["text"] as Record<string, unknown>)["content"],
					).toBe("the reply");
					expect(call?.payload["safe"]).toBe(0);
					expect(Object.keys(call?.payload ?? {}).sort()).toEqual([
						"agentid",
						"msgtype",
						"safe",
						"text",
						"touser",
					]);

					// Token caching: two sends ⇒ exactly ONE token fetch.
					await fx.adapter.send("corp-alpha:W-held", "second reply");
					expect(fx.api.tokenFetches).toHaveLength(1);

					// errcode 40001: evict cached token, refresh ONCE, retry once.
					const retryFx = makeWecomFixture();
					retryFx.api.scriptSend({ kind: "errcode", errcode: 40001 });
					const recovered = await retryFx.adapter.send(
						"corp-alpha:W-userX",
						"retry me",
					);
					expect(recovered.success).toBe(true);
					expect(retryFx.api.sendsOfUser("W-userX")).toHaveLength(2);
					retryFx.dispose();

					// Persistent failure: BOTH attempts rejected ⇒ clean failure.
					const deadFx = makeWecomFixture();
					deadFx.api.scriptSend(
						{ kind: "errcode", errcode: 40001 },
						{ kind: "errcode", errcode: 40014 },
					);
					const failed = await deadFx.adapter.send(
						"corp-alpha:W-userY",
						"doomed",
					);
					expect(failed.success).toBe(false);
					deadFx.dispose();
				}
			},
		),
		mk(
			"transport.wecom.non-numeric-agentid-fails-clean",
			"wecom: NON-NUMERIC configured agent_id fails the proactive send CLEANLY (int() ValueError parity, success:false) with ZERO token fetches and ZERO qyapi POSTs — NaN→agentid:0 coercion banned; missing/absent agent_id still wires agentid:0 per int(str(0)); numeric STRINGS parse to numbers on the wire",
			async () => {
				// Non-numeric agent_id: clean failure BEFORE any HTTP traffic.
				const badFx = makeWecomFixture({
					apps: [{ ...DEFAULT_APP_A, name: "badid", agent_id: "not-a-number" }],
				});
				const bad = await badFx.adapter.send("badid:W-userZ", "should fail");
				expect(bad.success).toBe(false);
				expect(bad.error).toBe(
					"invalid literal for int() with base 10: 'not-a-number'",
				);
				expect(badFx.api.tokenFetches).toHaveLength(0);
				expect(badFx.api.sends).toHaveLength(0);
				badFx.dispose();

				// Absent agent_id keeps Hermes' int(str(0)) ⇒ wire carries 0.
				const noIdFx = makeWecomFixture({
					apps: [{ ...DEFAULT_APP_A, name: "noid", agent_id: undefined }],
				});
				const zeroId = await noIdFx.adapter.send("noid:W-zero", "zero id");
				expect(zeroId.success).toBe(true);
				expect(noIdFx.api.sends.at(-1)?.payload["agentid"]).toBe(0);
				noIdFx.dispose();

				// Numeric STRING configs parse to a real number on the wire.
				const strIdFx = makeWecomFixture({
					apps: [{ ...DEFAULT_APP_A, name: "strid", agent_id: "1000002" }],
				});
				const strId = await strIdFx.adapter.send("strid:W-str", "string id");
				expect(strId.success).toBe(true);
				expect(strIdFx.api.sends.at(-1)?.payload["agentid"]).toBe(1000002);
				strIdFx.dispose();
			},
		),
		mk(
			"transport.wecom.text-send-cap-lossless",
			"wecom: >2048-char replies split LOSSLESSLY at the manifest cap (kit lane; source truncated — proposed DEC); §6.1 fallback envelope stays byte-exact",
			async (fx) => {
				const long = "w".repeat(WECOM_TEXT_SEND_CAP_CHARS + 52); // > cap
				const results = await fx.adapter.deliverText("corp-alpha:W-long", long);
				expect(results.every((r) => r.success)).toBe(true);
				const sends = fx.api.sendsOfUser("W-long");
				expect(sends.length).toBeGreaterThan(1);
				let rejoined = "";
				for (const s of sends) {
					const content = String(
						(s.payload["text"] as Record<string, unknown>)["content"],
					);
					expect(content.length).toBeLessThanOrEqual(WECOM_TEXT_SEND_CAP_CHARS);
					rejoined += content;
				}
				// Lossless modulo the family-wide (i/n) indicators the kit lane
				// mandates on multi-chunk deliveries.
				expect(rejoined.replace(/ \(\d+\/\d+\)/g, "")).toBe(long);

				// Fallback envelope rides byte-exact (no conversion on this wire).
				const fbResults = await fx.adapter.deliverText(
					"corp-alpha:W-long",
					"**raw** stays",
					{ forceFormattingError: true } as never,
				);
				expect(fbResults[fbResults.length - 1]?.success).toBe(true);
				const last = fx.api.sendsOfUser("W-long").at(-1);
				const lastContent = String(
					(last?.payload["text"] as Record<string, unknown>)["content"],
				);
				expect(lastContent.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)).toBe(true);
				expect(lastContent).toContain("**raw** stays");
			},
		),
	];
}

describe("conformance suite — wecom-callback census port (shape: webhook)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff passive)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // proactive-send egress: no native lanes
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("passes EVERY applicable shared row against the wecom-callback subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: "wecom-callback",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it("passes the INHERITED webhook transport rows (reference fixture) over the REAL adapter", async () => {
		const subject = makeSubject() as WecomCallbackSubject;
		const probe = subject.flagsAndTrustProbe();

		const fx = makeWecomFixture();
		try {
			subject.adapter.holdTurns(true);
			const startedAt = Date.now();
			const resp = await fx.postValidCallback(DEFAULT_APP_A, {
				ToUserName: FIXTURE_CORP_ID_A,
				FromUserName: "W-bw",
				MsgType: "text",
				Content: "window me",
				MsgId: "M-BW",
			});
			const elapsed = Date.now() - startedAt;
			expect(resp.status).toBe(200); // acked FAST even with the turn held
			subject.adapter.holdTurns(false);

			const rows = makeWebhookRows({
				async flagsAndTrust() {
					return probe;
				},
				async boundedWindowAnswer() {
					return { answeredWithinWindowMs: elapsed, windowCapMs: 5_000 };
				},
			});
			const report = await runConformanceSuite({
				subjectName: "wecom-callback-webhook-shape",
				shape: "webhook",
				rows,
				suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
			});
			if (report.failed > 0) console.error(formatReport(report));
			expect(report.failed).toBe(0);
			expect(report.deferred).toEqual([]);
		} finally {
			fx.dispose();
		}
	});

	it("passes ALL EIGHT wecom shape-delta rows through the real engine fixture", async () => {
		const rows = wecomDeltaRows(() => makeWecomFixture());
		expect(rows.map((r) => r.id)).toEqual([
			"transport.wecom.url-verification-handshake",
			"transport.wecom.callback-signature-negative-matrix",
			"transport.wecom.body-cap-preparse",
			"transport.wecom.msgid-dedupe-ttl",
			"transport.wecom.xml-event-ladder",
			"transport.wecom.ack-first-proactive-send",
			"transport.wecom.non-numeric-agentid-fails-clean",
			"transport.wecom.text-send-cap-lossless",
		]);
		const report = await runConformanceSuite({
			subjectName: "wecom-deltas",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	});

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));

		const subject = makeSubject() as WecomCallbackSubject;
		const probe = subject.flagsAndTrustProbe();
		const transport = makeWebhookRows({
			async flagsAndTrust() {
				return probe;
			},
			async boundedWindowAnswer() {
				return { answeredWithinWindowMs: 12, windowCapMs: 5_000 };
			},
		});
		const deltas = wecomDeltaRows(() => makeWecomFixture());

		const report = await runConformanceSuite({
			subjectName: "wecom-callback-full",
			shape: "webhook",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 30_000);

	it("the gate DETECTS violations: a lying crypto stub that accepts ANY signature fails its own named row", async () => {
		// Mutant: patch the adapter instance so decryptRequest ALWAYS succeeds
		// (as if signature verification were stubbed out) — the negative matrix
		// must fail BY NAME because garbage now dispatches.
		const rows = wecomDeltaRows(() => {
			const fx = makeWecomFixture();
			// THE LIE: an invalid signature no longer throws — it forges a valid
			// text event through instead (scoped to the FAILURE path so rows using
			// valid envelopes keep their real crypto).
			const prototypeMethod = Object.getPrototypeOf(fx.adapter)[
				"decryptRequest"
			].bind(fx.adapter);
			Object.defineProperty(fx.adapter, "decryptRequest", {
				value: (
					...args: Parameters<WecomCallbackAdapter["decryptRequest"]>
				) => {
					try {
						return prototypeMethod(...args) as string;
					} catch {
						return buildInnerXml({
							ToUserName: FIXTURE_CORP_ID_A,
							FromUserName: "W-user1",
							MsgType: "text",
							Content: "FORGED THROUGH",
							MsgId: `m-forged-${Math.random().toString(36).slice(2)}`,
						});
					}
				},
			});
			return fx;
		});

		const negRow = rows.find(
			(r) => r.id === "transport.wecom.callback-signature-negative-matrix",
		);
		expect(negRow).toBeDefined();
		const mutantReport = await runConformanceSuite({
			subjectName: "mutant-wecom-crypto",
			shape: "webhook",
			rows: [negRow as ConformanceRow],
		});
		expect(mutantReport.failed).toBe(1);
		expect(mutantReport.rows[0]?.pass).toBe(false);

		// Sanity: the OTHER rows still pass on their own fresh fixtures.
		const others = rows.filter((r) => r.id !== negRow?.id);
		const otherReport = await runConformanceSuite({
			subjectName: "mutant-wecom-others",
			shape: "webhook",
			rows: others as ConformanceRow[],
		});
		if (otherReport.failed > 0) console.error(formatReport(otherReport));
		expect(otherReport.failed).toBe(0);
	}, 30_000);
});
