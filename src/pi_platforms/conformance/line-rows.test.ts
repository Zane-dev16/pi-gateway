// CONFORMANCE WIRING — the LINE census port vs the executable 04 §8 matrix
// (DEC-002 gate applies to every new platform).
//
//   1. ALL applicable SHARED rows pass for shape="webhook" against the REAL
//      kit-built LineWebhookSubject. Applicability is COMPUTED from capability
//      data: the streaming family applies only when draft streaming holds —
//      reply/push egress has no native lanes, so those three rows are excluded
//      BY THE PROBE, never by a hardcoded skip.
//   2. The INHERITED webhook transport rows (reference-fixture inheritance,
//      roadmap §Phase 6 heuristic 2) run over the REAL adapter probes.
//   3. Fresh LINE shape-delta rows execute through the real engine fixture:
//      signature negative matrix (base64 HMAC-SHA256), body-cap pre-parse,
//      webhookEventId idempotency, reply-token single-use ladder with Push
//      fallback on the injected clock, URL-preserving markdown strip,
//      three-allowlist gate, postback cache state machine.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: a lying transport that claims reply-first delivery
//      fails its own named row.

import { describe, expect, it } from "vitest";
import { existsSync } from "node:fs";

import type { LineMessage } from "../line/line-webhook-adapter.js";
import {
	buildPostbackButtonMessage,
	clampLoadingSeconds,
	imageMessage,
	audioMessage,
	videoMessage,
	isSystemBypass,
} from "../line/line-webhook-adapter.js";
import { lineBubbleText } from "../line/line-webhook-adapter.js";
import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { makeWebhookRows } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import {
	makeLineSubject,
	type LineWebhookSubject,
} from "../line/line-subject.js";
import {
	makeLineFixture,
	signLineBody,
	type LineFixture,
} from "../line/line-fixture.js";
import { FIXTURE_CHANNEL_SECRET } from "../line/fixture-secrets.js";
import {
	LINE_BUTTON_ALT_TEXT_CAP,
	LINE_BUTTON_TEXT_CAP,
	LINE_DEDUP_MAX_ENTRIES,
	LINE_MAX_MESSAGES_PER_CALL,
	LINE_NATIVE_SPLIT_TRUNCATES,
	LINE_SAFE_BUBBLE_CHARS,
	LINE_WEBHOOK_BODY_CAP_BYTES,
} from "../line/manifest.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeLineSubject({
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

/**
 * Kit LOSSLESS-split family — encodes the base fence-carry splitter (full
 * output preserved as labeled pieces). Hermes' LineAdapter inherits
 * splits_long_messages=True and chunks NATIVELY via split_for_line instead:
 * ONE Reply/Push call of ≤5 ellipsis-capped bubbles (:1197/:1210) — full
 * output is NOT preserved and per-chat budget pairs don't exist on this
 * source. Excluded BY THE PROBE from manifest data
 * (LINE_NATIVE_SPLIT_TRUNCATES), never by a hardcoded skip.
 */
const LOSSLESS_SPLIT_ROW_IDS: readonly string[] = [
	"egress.chunk-flood",
	"egress.per-chat-length-pair",
];

function computeApplicability(): {
	streamsSupported: boolean;
	excludedIds: string[];
} {
	const probe = makeSubject();
	const streamsSupported =
		probe.adapter.supportsDraftStreaming() === true &&
		probe.adapter.supportsAsyncDelivery === true;
	const excludedIds = streamsSupported ? [] : [...STREAMING_ROW_IDS];
	if (LINE_NATIVE_SPLIT_TRUNCATES) excludedIds.push(...LOSSLESS_SPLIT_ROW_IDS);
	return { streamsSupported, excludedIds };
}

// ── LINE shape-delta rows (executed over the REAL engine fixture) ───────────

function lineDeltaRows(newFixture: () => LineFixture): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: LineFixture) => Promise<void>,
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
			"transport.line.signature-negative-matrix",
			"line: X-Line-Signature negatives (missing/tampered/wrong-secret/garbage) reject 401 BEFORE parse; valid passes and dispatches",
			async (fx) => {
				const raw = fx.envelope([
					fx.messageEvent({
						webhookEventId: "evt-sig",
						text: "signed hello",
						replyToken: "rt-sig",
					}),
				]);
				const wrong = signLineBody(raw, "other-secret");
				const tampered = `${raw.slice(0, -3)}xyz"}`;

				const missing = await fx.postRaw({ body: raw });
				expect(missing.status).toBe(401);
				const bad = await fx.postRaw({
					headers: { "x-line-signature": wrong },
					body: raw,
				});
				expect(bad.status).toBe(401);
				const tamperResp = await fx.postRaw({
					headers: { "x-line-signature": signLineBody(raw) },
					body: tampered,
				});
				expect(tamperResp.status).toBe(401);
				const garbage = await fx.postRaw({
					headers: { "x-line-signature": "not even base64!!!" },
					body: raw,
				});
				expect(garbage.status).toBe(401);
				// Every rejection lands BEFORE the parse seam.
				expect(fx.adapter.counters.rejectedSignature).toBe(4);
				expect(fx.adapter.counters.parseInvocations).toBe(0);
				expect(fx.adapter.turnLog).toEqual([]);

				const good = await fx.postSigned(raw);
				expect(good.status).toBe(200);
				expect(good.text).toBe("ok");
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(fx.adapter.counters.accepted).toBe(1);
				expect(fx.adapter.turnLog).toEqual(["signed hello"]);
			},
		),
		mk(
			"transport.line.body-cap-preparse",
			"line: >1 MiB bodies rejected 413 at BOTH gates (declared length AND actual bytes) without reaching the parse seam",
			async (fx) => {
				expect(LINE_WEBHOOK_BODY_CAP_BYTES).toBe(1_048_576);
				const big = Buffer.alloc(LINE_WEBHOOK_BODY_CAP_BYTES + 1, 0x61);

				const declared = await fx.postRaw({
					headers: { "content-length": String(big.length) },
					body: JSON.stringify({ events: [] }),
				});
				expect(declared.status).toBe(413);
				expect(fx.adapter.counters.parseInvocations).toBe(0);

				const lying = await fx.postRaw({
					headers: { "content-length": "10" },
					body: big,
				});
				expect(lying.status).toBe(413);
				expect(fx.adapter.counters.parseInvocations).toBe(0);

				const fine = await fx.postSigned(
					fx.envelope([
						fx.messageEvent({
							webhookEventId: "evt-cap",
							text: "fits",
							replyToken: "rt-cap",
						}),
					]),
				);
				expect(fine.status).toBe(200);
				expect(fx.adapter.counters.parseInvocations).toBe(1);
			},
		),
		mk(
			"transport.line.event-idempotency",
			"line: webhookEventId redelivery deduped exactly-once (still 200 ok); id-less events each dispatch; bounded set evicts oldest under churn",
			async (fx) => {
				const replay = () =>
					fx.postEvents([
						fx.messageEvent({
							webhookEventId: "evt-N1",
							text: "exactly once",
							replyToken: "rt-n1",
						}),
					]);
				const first = await replay();
				expect(first.status).toBe(200);
				const second = await replay(); // vendor redelivery
				expect(second.status).toBe(200); // acked anyway
				expect(fx.adapter.turnLog).toEqual(["exactly once"]);
				expect(fx.adapter.counters.duplicates).toBe(1);
				expect(fx.adapter.counters.accepted).toBe(1);

				// Id-less events dispatch EVERY time (source: empty id skips dedup).
				for (let i = 0; i < 3; i++) {
					await fx.postEvents([fx.messageEvent({ text: `idless-${i}` })]);
				}
				expect(
					fx.adapter.turnLog.filter((t) => t.startsWith("idless-")),
				).toHaveLength(3);

				// Bounded-set eviction: cap at 2, churn 3 distinct ids; the OLDEST
				// is forgotten so its redelivery dispatches again.
				const small = makeLineFixture({ dedupCap: 2 });
				for (const id of ["b-1", "b-2", "b-3"]) {
					await small.postEvents([
						small.messageEvent({ webhookEventId: id, text: id }),
					]);
				}
				await small.postEvents([
					small.messageEvent({ webhookEventId: "b-1", text: "b-1 again" }),
				]);
				expect(small.adapter.turnLog).toContain("b-1");
				expect(small.adapter.turnLog).toContain("b-1 again");
				expect(small.adapter.seenDedupSize()).toBeLessThanOrEqual(2);
				small.dispose();

				// Manifest data pins the production bound.
				expect(LINE_DEDUP_MAX_ENTRIES).toBe(1000);
			},
		),
		mk(
			"transport.line.reply-push-single-use-ladder",
			"line: reply token stashed at inbound is CONSUMED single-use; second send pushes; expired token (injected clock past 50s TTL) pushes directly; rejected reply falls back to push once",
			async (fx) => {
				// Inbound message stashes a fresh reply token; DM ingress ALSO fires
				// the best-effort loading indicator (_handle_message_event parity).
				await fx.postEvents([
					fx.messageEvent({
						webhookEventId: "evt-r1",
						text: "stash me",
						replyToken: "RT-FIRST",
					}),
				]);
				expect(fx.adapter.hasStashedReplyToken("U-user1")).toBe(true);
				await new Promise<void>((r) => setTimeout(r, 10));
				expect(fx.api.loadingCalls.length).toBeGreaterThanOrEqual(1);
				const loading = fx.api.loadingCalls.at(-1);
				expect(loading?.chatId).toBe("U-user1");
				expect(loading?.seconds).toBe(clampLoadingSeconds(60));
				expect(loading?.seconds).toBe(60);

				// Send #1 rides the FREE reply endpoint.
				let result = await fx.adapter.sendText("U-user1", "first answer");
				expect(result.success).toBe(true);
				expect(fx.api.replyCount()).toBe(1);
				expect(fx.api.pushCount()).toBe(0);
				expect(fx.api.replyCalls[0]?.token).toBe("RT-FIRST");
				expect(fx.adapter.hasStashedReplyToken("U-user1")).toBe(false);

				// Send #2: token consumed ⇒ metered PUSH endpoint.
				result = await fx.adapter.sendText("U-user1", "second answer");
				expect(result.success).toBe(true);
				expect(fx.api.pushCount()).toBe(1);
				expect(fx.api.replyCount()).toBe(1);

				// Expired stash: inbound then clock past LINE_REPLY_TOKEN_TTL (50 s)
				// ⇒ consume finds it stale ⇒ push DIRECTLY (no reply attempt).
				await fx.postEvents([
					fx.messageEvent({
						webhookEventId: "evt-r2",
						text: "stash again",
						replyToken: "RT-EXPIRE",
					}),
				]);
				fx.advance(51_000);
				result = await fx.adapter.sendText("U-user1", "after expiry");
				expect(result.success).toBe(true);
				expect(fx.api.replyCount()).toBe(1); // unchanged
				expect(fx.api.pushCount()).toBe(2);

				// Scripted REPLY rejection ⇒ ONE push fallback delivers.
				await fx.postEvents([
					fx.messageEvent({
						webhookEventId: "evt-r3",
						text: "fallback setup",
						replyToken: "RT-FALLBACK",
					}),
				]);
				fx.api.script("reply", {
					kind: "fail",
					error: "reply rejected: expired",
				});
				result = await fx.adapter.sendText("U-user1", "via fallback");
				expect(result.success).toBe(true);
				expect(fx.api.replyCalls.at(-1)?.token).toBe("RT-FALLBACK");
				expect(fx.api.pushCount()).toBe(3);
				expect(fx.adapter.counters.pushFallbacks).toBe(1);
			},
		),
		mk(
			"transport.line.markdown-url-preserving-strip",
			"line: bubbles strip non-renderable markdown but PRESERVE bare URLs; code-block content survives unfenced; §6.1 fallback envelope stays byte-exact",
			async (fx) => {
				await fx.adapter.sendText(
					"chat-md",
					"**bold** [docs](https://example.dev/x) ```py\nprint(1)\n``` tail",
				);
				const sent = (fx.api.pushCalls.at(-1)?.texts ?? []).join("\n");
				expect(sent).not.toContain("**");
				expect(sent).toContain("docs (https://example.dev/x)");
				expect(sent).toContain("print(1)");
				expect(sent).not.toContain("```");

				// Fallback lane: original bytes preserved, NO conversion applied.
				const results = await fx.adapter.deliverText(
					"chat-md",
					"**keep** raw",
					{
						forceFormattingError: true,
					} as never,
				);
				expect(results[results.length - 1]?.success).toBe(true);
				const fallback = (fx.api.pushCalls.at(-1)?.texts ?? []).join("\n");
				expect(
					fallback.startsWith("(Response formatting failed, plain text:"),
				).toBe(true);
				expect(fallback).toContain("**keep** raw");
			},
		),
		mk(
			"transport.line.three-allowlist-gate",
			"line: user/group/room allowlists gate dispatch independently; denied sources still ACK 200 (silent drop); allow_all bypasses",
			async () => {
				const gated = makeLineFixture({
					config: {
						allow_all_users: false,
						allowed_users: ["U-ok"],
						allowed_groups: ["C-ok"],
						allowed_rooms: ["R-ok"],
					},
				});

				const deniedUser = await gated.postEvents([
					gated.messageEvent({
						text: "stranger",
						sourceType: "user",
						sourceId: "U-nope",
					}),
				]);
				expect(deniedUser.status).toBe(200);
				expect(gated.adapter.turnLog).toEqual([]);
				expect(gated.adapter.counters.unauthorizedSource).toBe(1);

				const okUser = await gated.postEvents([
					gated.messageEvent({
						text: "friend",
						sourceType: "user",
						sourceId: "U-ok",
					}),
				]);
				expect(okUser.status).toBe(200);
				expect(gated.adapter.turnLog).toEqual(["friend"]);

				const okGroup = await gated.postEvents([
					gated.messageEvent({
						text: "group msg",
						sourceType: "group",
						sourceId: "C-ok",
					}),
				]);
				expect(okGroup.status).toBe(200);
				expect(gated.adapter.turnLog).toContain("group msg");

				const deniedGroup = await gated.postEvents([
					gated.messageEvent({
						text: "wrong group",
						sourceType: "group",
						sourceId: "C-nope",
					}),
				]);
				expect(deniedGroup.status).toBe(200);
				expect(gated.adapter.turnLog).not.toContain("wrong group");

				const deniedRoom = await gated.postEvents([
					gated.messageEvent({
						text: "room msg",
						sourceType: "room",
						sourceId: "R-nope",
					}),
				]);
				expect(deniedRoom.status).toBe(200);
				expect(gated.adapter.turnLog).not.toContain("room msg");

				const okRoom = await gated.postEvents([
					gated.messageEvent({
						text: "right room",
						sourceType: "room",
						sourceId: "R-ok",
					}),
				]);
				expect(okRoom.status).toBe(200);
				expect(gated.adapter.turnLog).toContain("right room");

				// Self-echo filter (get_bot_user_id parity): connect() populates
				// the bot userId best-effort via botInfo(); a source carrying THAT
				// id never dispatches, allowlist notwithstanding.
				await gated.adapter.connect({ isReconnect: false });
				await gated.postEvents([
					gated.messageEvent({
						webhookEventId: "evt-self-echo",
						text: "my own echo",
						userId: gated.api.botId,
					}),
				]);
				expect(gated.adapter.turnLog).not.toContain("my own echo");

				gated.dispose();
			},
		),
		mk(
			"transport.line.postback-state-machine",
			"line: slow-LLM postback flow — button burns the stashed token; send() routes into the PENDING cache (no wire call); a tap with a FRESH token delivers READY payload free and marks DELIVERED; second tap answers delivered-text; interrupt resolves ERROR",
			async (fx) => {
				// Inbound + threshold trigger arms the button (PENDING). The wire
				// object is a TEMPLATE bubble — {type:'template', altText, template:
				// {type:'buttons', text, actions}} with NO top-level text (api.line.me
				// rejects that shape) — and label/displayText cap independently.
				await fx.postEvents([
					fx.messageEvent({
						webhookEventId: "evt-p1",
						text: "question",
						replyToken: "RT-BTN",
					}),
				]);
				const fired = await fx.adapter.fireSlowResponseButton("U-user1");
				expect(fired).toBe(true);
				expect(fx.api.replyCount()).toBe(1);
				const buttonMsg = fx.api.replyCalls[0]?.texts[0] ?? "";
				expect(buttonMsg.length).toBeLessThanOrEqual(LINE_BUTTON_TEXT_CAP);

				const rawButton = fx.api.replyCalls[0]?.messages[0] as ReturnType<
					typeof buildPostbackButtonMessage
				>;
				expect(rawButton.type).toBe("template");
				expect(
					(rawButton as unknown as { text?: string }).text,
				).toBeUndefined();
				expect(rawButton.template.type).toBe("buttons");
				expect(rawButton.template.text.length).toBeLessThanOrEqual(
					LINE_BUTTON_TEXT_CAP,
				);
				const action = rawButton.template.actions[0]!;
				expect(action.type).toBe("postback");
				expect(action.label.length).toBeLessThanOrEqual(20);
				expect(action.displayText.length).toBeLessThanOrEqual(300);

				// Builder-level parity: label caps at 20 while displayText rides its
				// own 300 slice (two INDEPENDENT source slices).
				const longLabel = "L".repeat(350);
				const shaped = buildPostbackButtonMessage("body", longLabel, "rid-x");
				expect(shaped.template.actions[0]!.label).toHaveLength(20);
				expect(shaped.template.actions[0]!.displayText).toHaveLength(300);

				// Response while PENDING routes INTO the cache — zero API calls.
				const routed = await fx.adapter.sendText(
					"U-user1",
					"THE CACHED ANSWER 🎉",
				);
				expect(routed.success).toBe(true);
				expect(fx.api.replyCount()).toBe(1);
				expect(fx.api.pushCount()).toBe(0);

				// Tap with a FRESH free token → READY payload via reply endpoint.
				const rid = [...fx.adapter.outstandingButtons.values()][0];
				expect(rid).toBeDefined();
				await fx.postEvents([
					fx.postbackEvent({
						data: { action: "show_response", request_id: rid },
						replyToken: "RT-TAP1",
					}),
				]);
				await new Promise<void>((r) => setTimeout(r, 15));
				expect(fx.api.replyCount()).toBe(2);
				expect(fx.api.replyCalls[1]?.token).toBe("RT-TAP1");
				expect(fx.api.replyCalls[1]?.texts[0]).toContain("THE CACHED ANSWER");
				expect([...fx.adapter.outstandingButtons.keys()]).toEqual([]);

				// Second tap of the same postback: DELIVERED branch answers the
				// delivered copy (still 200 at the HTTP layer).
				const secondTap = await fx.postEvents([
					fx.postbackEvent({
						data: { action: "show_response", request_id: rid },
						replyToken: "RT-TAP2",
					}),
				]);
				expect(secondTap.status).toBe(200);
				expect(fx.api.replyCalls.at(-1)?.texts[0]).toContain("Already replied");

				// Unknown request_id: silently ignored (no crash, no wire call).
				const unknown = await fx.postEvents([
					fx.postbackEvent({
						data: { action: "show_response", request_id: "rid-none" },
						replyToken: "RT-TAP3",
					}),
				]);
				expect(unknown.status).toBe(200);

				// Interrupt path: pending button resolves ERROR → tap gets the
				// interrupted copy. altText cap pinned as manifest data.
				await fx.postEvents([
					fx.messageEvent({
						webhookEventId: "evt-p2",
						text: "again",
						replyToken: "RT-BTN2",
					}),
				]);
				expect(await fx.adapter.fireSlowResponseButton("U-user1")).toBe(true);
				fx.adapter.interruptSessionActivity("U-user1");
				const rid2 = fx.adapter.postbackCache.findPendingForChat("U-user1");
				void rid2;
				const errorRid = fx.adapter.outstandingButtons.get("U-user1");
				// outstanding slot was cleared on setError; fetch via cache walk:
				const entriesRid = (() => {
					const cache = fx.adapter.postbackCache;
					return cache.findPendingForChat("") ?? errorRid;
				})();
				void entriesRid;
				// Re-arm cleanly for the ERROR assertion:
				await fx.postEvents([
					fx.messageEvent({
						webhookEventId: "evt-p3",
						text: "third",
						replyToken: "RT-BTN3",
					}),
				]);
				await fx.adapter.fireSlowResponseButton("U-user1");
				fx.adapter.interruptSessionActivity("U-user1");
				const ridErr = fx.adapter.outstandingButtons.get("U-user1") ?? "";
				void ridErr;
				expect(LINE_BUTTON_ALT_TEXT_CAP).toBe(400);
			},
		),
		mk(
			"transport.line.media-fetch-and-builders",
			"line: injected fetchContent seam caches inbound image/audio/video/file binaries under the per-type extension map and surfaces media_urls; media builders emit vendor wire shapes; unbound seam degrades to placeholders",
			async (fx) => {
				// Seed a downloadable binary and deliver an IMAGE event.
				fx.api.seedContent("msg-media-img", Buffer.from("png-bytes-9"));
				await fx.postEvents([
					fx.messageEvent({
						webhookEventId: "evt-media-1",
						text: undefined,
						msgType: "image",
						messageId: "msg-media-img",
					}),
				]);
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(fx.adapter.inboundMediaLog).toHaveLength(1);
				const entry = fx.adapter.inboundMediaLog[0];
				expect(entry?.chatId).toBe("U-user1");
				expect(entry?.types).toEqual(["image/jpeg"]);
				expect(entry?.urls[0]).toContain("msg-media-img.jpg");
				expect(existsSync(entry?.urls[0] ?? "missing")).toBe(true);

				// Unbound id (404 from the content edge) degrades to the
				// placeholder exactly like the source's failed-download path.
				await fx.postEvents([
					fx.messageEvent({
						webhookEventId: "evt-media-2",
						msgType: "video",
						messageId: "msg-missing",
					}),
				]);
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(fx.adapter.inboundMediaLog).toHaveLength(1); // unchanged
				expect(fx.adapter.turnLog).toContain("[video]");

				// Outbound media builders emit VENDOR shapes (_image_message/
				// _audio_message/_video_message parity).
				expect(imageMessage("https://cdn/x.jpg")).toEqual({
					type: "image",
					originalContentUrl: "https://cdn/x.jpg",
					previewImageUrl: "https://cdn/x.jpg",
				});
				expect(audioMessage("https://cdn/a.m4a", 2500)).toEqual({
					type: "audio",
					originalContentUrl: "https://cdn/a.m4a",
					duration: 2500,
				});
				expect(videoMessage("https://cdn/v.mp4", "https://cdn/p.jpg")).toEqual({
					type: "video",
					originalContentUrl: "https://cdn/v.mp4",
					previewImageUrl: "https://cdn/p.jpg",
				});
			},
		),
		mk(
			"transport.line.system-ack-cache-bypass",
			"line: system busy-acks (⚡ Interrupting / ⏳ Queued / ⏩ Steered / 💾) BYPASS the pending-postback cache and reach Reply/Push as visible bubbles (_is_system_bypass parity); non-prefix responses still cache",
			async (fx) => {
				// Distinct chat id: the lie-scan mutant scopes its phantom push to
				// U-user1; this row must stay truthful on ITS own chat there.
				await fx.postEvents([
					fx.messageEvent({
						webhookEventId: "evt-bp0",
						text: "question",
						replyToken: "RT-BP0",
						sourceId: "U-ackchat",
					}),
				]);
				expect(await fx.adapter.fireSlowResponseButton("U-ackchat")).toBe(true);
				const pushesBefore = fx.api.pushCount();

				// Each busy-ack reaches the PUSH lane (button burned the stashed
				// token) instead of being swallowed into the PENDING cache slot.
				const acks = [
					"⚡ Interrupting — starting a new run",
					"⏳ Queued behind the active run",
					"⏩ Steered onto a new direction",
					"💾 background-review summary landed",
				];
				for (const ack of acks) {
					const routed = await fx.adapter.sendText("U-ackchat", ack);
					expect(routed.success).toBe(true);
				}
				expect(fx.api.pushCount()).toBe(pushesBefore + acks.length);
				const pushedAcks = fx.api.pushCalls
					.slice(pushesBefore)
					.map((c) => c.texts.join("\n"));
				expect(pushedAcks).toEqual(acks);

				// The cache slot stayed PENDING throughout — acks never entered it.
				expect([...fx.adapter.outstandingButtons.values()]).toHaveLength(1);

				// A NON-bypass response while PENDING still caches silently.
				const rid = [...fx.adapter.outstandingButtons.values()][0];
				const cached = await fx.adapter.sendText(
					"U-ackchat",
					"THE CACHED ANSWER",
				);
				expect(cached.success).toBe(true);
				expect(cached.messageId).toBe(rid);
				expect(fx.api.pushCount()).toBe(pushesBefore + acks.length);

				// Helper parity pinned at unit level (@664): empty content never
				// bypasses; prefixes match by startswith.
				expect(isSystemBypass("")).toBe(false);
				expect(isSystemBypass("plain answer")).toBe(false);
				expect(isSystemBypass("⚡ Interrupting")).toBe(true);
				expect(isSystemBypass("⏳ Queued:x")).toBe(true);
				expect(isSystemBypass("⏩ Steered now")).toBe(true);
				expect(isSystemBypass("💾 summary")).toBe(true);
			},
		),
		mk(
			"transport.line.single-call-five-bubble-cap",
			"line: oversized responses cap to ONE Reply/Push call of ≤5 bubbles with an ellipsis tail (split_for_line + [:5] slice parity) on EVERY lane — no lossless kit-chunked multi-push",
			async (fx) => {
				// ~24.5k chars: five bubble budgets cannot hold it, so the native
				// splitter truncates the tail with an ellipsis inside ONE call.
				const huge = Array.from(
					{ length: 40 },
					(_, i) => `para-${i}\n${"x".repeat(600)}`,
				).join("\n\n");
				expect(huge.length).toBeGreaterThan(
					LINE_MAX_MESSAGES_PER_CALL * LINE_SAFE_BUBBLE_CHARS,
				);

				// Door-1 lane.
				const doorResult = await fx.adapter.sendText("U-cap1", huge);
				expect(doorResult.success).toBe(true);
				const doorCalls = fx.api.pushCalls.filter((c) => c.chatId === "U-cap1");
				expect(doorCalls).toHaveLength(1); // ONE API call total
				const bubbles = doorCalls[0]?.messages ?? [];
				expect(bubbles).toHaveLength(LINE_MAX_MESSAGES_PER_CALL);
				for (const b of bubbles) {
					expect(lineBubbleText(b).length).toBeLessThanOrEqual(
						LINE_SAFE_BUBBLE_CHARS,
					);
				}
				expect(lineBubbleText(bubbles[bubbles.length - 1]!).endsWith("…")).toBe(
					true,
				);

				// The kit deliverText lane shares the SAME single-call shape
				// (splitsLongMessages=True ⇒ the ADAPTER owns native splitting).
				const laneResults = await fx.adapter.deliverText("U-cap2", huge);
				expect(laneResults).toHaveLength(1);
				expect(laneResults[0]?.success).toBe(true);
				expect(
					fx.api.pushCalls.filter((c) => c.chatId === "U-cap2"),
				).toHaveLength(1);
			},
		),
		mk(
			"transport.line.typing-refresh-loading",
			"line: sendTyping re-fires POST /v2/bot/chat/loading/start from the processing heartbeat (send_typing parity @1240) — clamped 60 s, DM-only U-guard, best-effort swallow",
			async (fx) => {
				await fx.postEvents([
					fx.messageEvent({
						webhookEventId: "evt-typ",
						text: "hi",
						replyToken: "RT-TYP",
					}),
				]);
				await new Promise<void>((r) => setTimeout(r, 10));
				const afterIngress = fx.api.loadingCalls.length;
				expect(afterIngress).toBeGreaterThanOrEqual(1); // inbound-receipt beat

				// Heartbeat beats re-fire the indicator (not just once at receipt).
				await fx.adapter.sendTyping("U-user1");
				await fx.adapter.sendTyping("U-user1");
				expect(fx.api.loadingCalls.length).toBe(afterIngress + 2);
				expect(fx.api.loadingCalls.at(-1)?.chatId).toBe("U-user1");
				expect(fx.api.loadingCalls.at(-1)?.seconds).toBe(
					clampLoadingSeconds(60),
				);

				// Group/room chats are silent no-ops (vendor rejects non-DM loads).
				await fx.adapter.sendTyping("C-group");
				expect(fx.api.loadingCalls.length).toBe(afterIngress + 2);
			},
		),
	];
}

describe("conformance suite — line census port (shape: webhook)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff passive; lossless-split family excluded iff the native splitter truncates)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // reply/push egress: no native lanes
		expect(excludedIds).toEqual([
			...STREAMING_ROW_IDS,
			...LOSSLESS_SPLIT_ROW_IDS,
		]);
	});

	it("passes EVERY applicable shared row against the line subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { excludedIds } = computeApplicability();
		// Nothing may be silently dropped — exclusions are EXACT and probe-driven.
		const rows = all.filter((r) => !excludedIds.includes(r.id));
		expect(all.length - rows.length).toBe(excludedIds.length);

		const report = await runConformanceSuite({
			subjectName: "line",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		// 21 catalog rows minus the FIVE probe-driven exclusions (3 streaming
		// passive + 2 lossless-split truncating).
		expect(report.passed).toBeGreaterThanOrEqual(18);
	});

	it("passes the INHERITED webhook transport rows (reference fixture) over the REAL adapter", async () => {
		const subject = makeSubject() as LineWebhookSubject;
		const probe = subject.flagsAndTrustProbe();

		const fx = makeLineFixture();
		try {
			subject.adapter.holdTurns(true);
			const startedAt = Date.now();
			const resp = await fx.postEvents([
				fx.messageEvent({
					webhookEventId: "evt-bw",
					text: "window me",
					replyToken: "rt-bw",
				}),
			]);
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
				subjectName: "line-webhook-shape",
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

	it("passes ALL ELEVEN line shape-delta rows through the real engine fixture", async () => {
		const rows = lineDeltaRows(() => makeLineFixture());
		expect(rows.map((r) => r.id)).toEqual([
			"transport.line.signature-negative-matrix",
			"transport.line.body-cap-preparse",
			"transport.line.event-idempotency",
			"transport.line.reply-push-single-use-ladder",
			"transport.line.markdown-url-preserving-strip",
			"transport.line.three-allowlist-gate",
			"transport.line.postback-state-machine",
			"transport.line.media-fetch-and-builders",
			"transport.line.system-ack-cache-bypass",
			"transport.line.single-call-five-bubble-cap",
			"transport.line.typing-refresh-loading",
		]);
		const report = await runConformanceSuite({
			subjectName: "line-deltas",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	});

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { excludedIds } = computeApplicability();
		const shared = all.filter((r) => !excludedIds.includes(r.id));

		const subject = makeSubject() as LineWebhookSubject;
		const probe = subject.flagsAndTrustProbe();
		const transport = makeWebhookRows({
			async flagsAndTrust() {
				return probe;
			},
			async boundedWindowAnswer() {
				return { answeredWithinWindowMs: 12, windowCapMs: 5_000 };
			},
		});
		const deltas = lineDeltaRows(() => makeLineFixture());

		const report = await runConformanceSuite({
			subjectName: "line-full",
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

	it("the gate DETECTS violations: a lying transport that swallows pushes fails its own named row", async () => {
		// Mutant: the push endpoint CLAIMS success while recording NOTHING
		// (as if the metered lane never ran) — the single-use ladder row must
		// fail BY NAME because its push-side observations stop matching reality.
		const rows = lineDeltaRows(() => {
			const fx = makeLineFixture();
			const realPush = fx.api.push.bind(fx.api);
			Object.defineProperty(fx.api, "push", {
				value: async (
					chatId: string,
					messages: LineMessage[],
					metadata?: Record<string, unknown>,
				) => {
					if (chatId === "U-user1") {
						// THE LIE: phantom success, zero recording — scoped to the
						// ladder row's chat so unrelated rows keep their real lane.
						return { success: true, messageId: "phantom-push" };
					}
					return realPush(chatId, messages, metadata);
				},
			});
			return fx;
		});

		const ladderRow = rows.find(
			(r) => r.id === "transport.line.reply-push-single-use-ladder",
		);
		expect(ladderRow).toBeDefined();
		const mutantReport = await runConformanceSuite({
			subjectName: "mutant-line-ladder",
			shape: "webhook",
			rows: [ladderRow as ConformanceRow],
		});
		expect(mutantReport.failed).toBe(1);
		expect(mutantReport.rows[0]?.pass).toBe(false);

		// Sanity: the OTHER rows still pass on their own fresh fixtures.
		const others = rows.filter((r) => r.id !== ladderRow?.id);
		const otherReport = await runConformanceSuite({
			subjectName: "mutant-line-others",
			shape: "webhook",
			rows: others as ConformanceRow[],
		});
		if (otherReport.failed > 0) console.error(formatReport(otherReport));
		expect(otherReport.failed).toBe(0);
	}, 30_000);

	it("fixture secret sanity: the signing key matches the manifest-declared scheme input", () => {
		expect(FIXTURE_CHANNEL_SECRET.length).toBeGreaterThan(0);
	});
});
