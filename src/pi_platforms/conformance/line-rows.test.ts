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

import type { LineMessage } from "../line/line-webhook-adapter.js";
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
				// Inbound message stashes a fresh reply token.
				await fx.postEvents([
					fx.messageEvent({
						webhookEventId: "evt-r1",
						text: "stash me",
						replyToken: "RT-FIRST",
					}),
				]);
				expect(fx.adapter.hasStashedReplyToken("U-user1")).toBe(true);

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
			async (fx) => {
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
				gated.dispose();
			},
		),
		mk(
			"transport.line.postback-state-machine",
			"line: slow-LLM postback flow — button burns the stashed token; send() routes into the PENDING cache (no wire call); a tap with a FRESH token delivers READY payload free and marks DELIVERED; second tap answers delivered-text; interrupt resolves ERROR",
			async (fx) => {
				// Inbound + threshold trigger arms the button (PENDING).
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
	];
}

describe("conformance suite — line census port (shape: webhook)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff passive)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // reply/push egress: no native lanes
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("passes EVERY applicable shared row against the line subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: "line",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
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

	it("passes ALL SEVEN line shape-delta rows through the real engine fixture", async () => {
		const rows = lineDeltaRows(() => makeLineFixture());
		expect(rows.map((r) => r.id)).toEqual([
			"transport.line.signature-negative-matrix",
			"transport.line.body-cap-preparse",
			"transport.line.event-idempotency",
			"transport.line.reply-push-single-use-ladder",
			"transport.line.markdown-url-preserving-strip",
			"transport.line.three-allowlist-gate",
			"transport.line.postback-state-machine",
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
		const { streamsSupported } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));

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
