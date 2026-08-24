// CONFORMANCE WIRING — the WhatsApp Cloud port vs the executable 04 §8 matrix
// (census-phase merge gate; DEC-002 gate applies to every new platform).
//
//   1. ALL applicable SHARED rows pass for shape="webhook" against the REAL
//      kit-built WaCloudSubject. Applicability is COMPUTED from capability
//      data (04 §8 conditional headers): the streaming family applies only
//      when supports_draft_streaming()/draft_stream_is_message hold — the
//      Cloud API is reply-only egress, so those three rows are excluded BY THE
//      PROBE, never by a hardcoded skip (a capability flip re-includes them).
//   2. The INHERITED webhook transport rows (reference-fixture inheritance,
//      roadmap §Phase 6 heuristic 2) run over the REAL adapter probes.
//   3. Fresh WhatsApp shape-delta rows execute through the real engine fixture
//      (signature negative matrix, status-callback dedup, window
//      classification, media cap pre-upload, LID continuity, read receipts).
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: an inflating mutant fixture fails ITS OWN named row.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { makeWebhookRows } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import {
	makeWaCloudSubject,
	type WaCloudSubject,
} from "../whatsapp-cloud/wa-cloud-subject.js";
import {
	makeWaCloudFixture,
	FIXTURE_APP_SECRET,
	type WaCloudFixture,
} from "../whatsapp-cloud/wa-cloud-fixture.js";
import {
	MESSAGING_WINDOW_MS,
	MEDIA_SIZE_LIMITS,
} from "../whatsapp-cloud/manifest.js";
import { createHmac } from "node:crypto";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeWaCloudSubject({
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
	const streamsSupported = probe.adapter.supportsDraftStreaming() === true;
	return { streamsSupported, excludedIds: [...STREAMING_ROW_IDS] };
}

// ── WhatsApp shape-delta rows (executed over the REAL engine fixture) ───────

function signWith(body: string): string {
	return `sha256=${createHmac("sha256", FIXTURE_APP_SECRET).update(body).digest("hex")}`;
}

/**
 * One delta row factory. Every body drives the REAL ingress/media/receipt
 * paths through WaCloudFixture and asserts OBSERVABLE outcomes; each row gets
 * a FRESH fixture (rows never couple through shared mutable state).
 */
function waDeltaRows(newFixture: () => WaCloudFixture): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: WaCloudFixture) => Promise<void>,
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
			"transport.wa.signature-negative-matrix",
			"wa: Meta X-Hub-Signature-256 negatives (missing/tampered/wrong-secret) reject 401; valid passes; counter audits",
			async (fx) => {
				const envelope = fx.valueEnvelope({
					messages: [fx.textMessage("wamid.sig", "15551234567", "sig row")],
				});
				const raw = JSON.stringify(envelope);
				const tampered = `${raw.slice(0, -3)}alse"}`;
				const wrongSig = `sha256=${createHmac("sha256", "other-secret").update(raw).digest("hex")}`;

				const missing = await fx.postRaw({}, raw);
				expect(missing.status).toBe(401);
				const bad = await fx.postRaw({ "x-hub-signature-256": wrongSig }, raw);
				expect(bad.status).toBe(401);
				const tamperedResp = await fx.postRaw(
					{ "x-hub-signature-256": signWith(raw) },
					tampered,
				);
				expect(tamperedResp.status).toBe(401);
				expect(fx.adapter.counters.rejectedSignature).toBe(3);

				const good = await fx.postRaw(
					{ "x-hub-signature-256": signWith(raw) },
					raw,
				);
				expect(good.status).toBe(200);
				expect(fx.adapter.counters.accepted).toBe(1);
			},
		),
		mk(
			"transport.wa.status-callback-dedup",
			"wa: statuses[] acknowledged but never dispatched; wamid redelivery deduped exactly-once",
			async (fx) => {
				const statusEnv = fx.valueEnvelope({
					statuses: [fx.statusUpdate("wamid.st", "delivered")],
				});
				const acked = await fx.postSigned(statusEnv);
				expect(acked.status).toBe(200);
				await new Promise<void>((r) => setTimeout(r, 25));
				expect(fx.adapter.counters.statusesSeen).toBe(1);
				expect(fx.adapter.turnLog).toEqual([]);

				const replayEnv = () =>
					fx.valueEnvelope({
						messages: [
							fx.textMessage("wamid.rp", "15551234567", "exactly once"),
						],
					});
				await fx.postSigned(replayEnv());
				await fx.postSigned(replayEnv()); // Meta redelivery
				await new Promise<void>((r) => setTimeout(r, 50));
				expect(fx.adapter.turnLog).toEqual(["exactly once"]);
				expect(fx.adapter.counters.duplicates).toBe(1);
				expect(fx.adapter.counters.accepted).toBe(1);
			},
		),
		mk(
			"transport.wa.window-classification-recorded",
			"wa: every outbound RECORDS its session/template routing decision; class flips at exactly 24h (injected clock)",
			async (fx) => {
				const chat = "15554440000";
				// Cold send: no session ever opened ⇒ template class recorded.
				await fx.adapter.send(chat, "cold");
				// Inbound opens the session…
				await fx.postSigned(
					fx.valueEnvelope({
						messages: [fx.textMessage("wamid.w9", chat, "customer hello")],
					}),
				);
				await new Promise<void>((r) => setTimeout(r, 30));
				// …23h later still session…
				fx.advance(23 * 60 * 60 * 1000);
				await fx.adapter.send(chat, "still open");
				// …at EXACTLY windowMs the class flips to template.
				fx.advance(60 * 60 * 1000);
				await fx.adapter.send(chat, "closed now");

				const classes = fx.adapter.classifier
					.decisionsOf(chat)
					.map((d) => d.routeClass);
				expect(classes[0]).toBe("template");
				expect(classes[1]).toBe("session");
				expect(classes[2]).toBe("template");
				expect(fx.adapter.classifier.decisionsOf(chat)[2]?.elapsedMs).toBe(
					MESSAGING_WINDOW_MS,
				);
			},
		),
		mk(
			"transport.wa.media-cap-preupload",
			"wa: per-kind size caps refuse PRE-upload (zero roundtrips); at-cap passes; caption rides the media block, never a text send",
			async (fx) => {
				const oversized = await fx.adapter.sendMedia("m-chat", "sticker", {
					bytes: Buffer.alloc(MEDIA_SIZE_LIMITS.sticker + 1),
				});
				expect(oversized.success).toBe(false);
				expect(fx.graph.uploads).toHaveLength(0); // refused BEFORE Graph

				const atCap = await fx.adapter.sendMedia(
					"15551234567",
					"image",
					{ bytes: Buffer.alloc(MEDIA_SIZE_LIMITS.image), mime: "image/png" },
					{ caption: "the caption" },
				);
				expect(atCap.success).toBe(true);
				expect(fx.graph.uploads).toHaveLength(1); // reached Graph exactly once

				const post = fx.graph.sentMessages[0]?.body as Record<string, unknown>;
				const image = post["image"] as Record<string, unknown>;
				expect(image["caption"]).toBe("the caption");
				// Two-step shape: caption is NOT a separate text message.
				expect(fx.graph.textSendsOf()).toHaveLength(0);
			},
		),
		mk(
			"transport.wa.lid-alias-continuity",
			"wa: phone↔LID aliases collapse to ONE canonical session key AND one stable digits recipient through the REAL identity module",
			async (fx) => {
				const PHONE = "15551234567";
				const LID = "999999999999999";
				fx.writeLidMapping(PHONE, LID);

				await fx.postSigned(
					fx.valueEnvelope({
						messages: [fx.textMessage("wamid.p", PHONE, "via phone")],
					}),
				);
				await fx.postSigned(
					fx.valueEnvelope({
						messages: [fx.textMessage("wamid.l", LID, "via lid")],
					}),
				);
				await new Promise<void>((r) => setTimeout(r, 60));
				expect(fx.adapter.turnLog).toContain("via phone");
				expect(fx.adapter.turnLog).toContain("via lid");

				// Outbound addressing resolves BOTH spellings to the SAME wire
				// recipient, idempotently.
				const r1 = await fx.adapter.send(PHONE, "reply a");
				const r2 = await fx.adapter.send(LID, "reply b");
				expect(r1.success && r2.success).toBe(true);
				const recipients = fx.graph.textSendsOf().map((s) => s.to);
				expect(recipients).toEqual([PHONE, PHONE]);
				expect(fx.adapter.resolveRecipient(LID)).toBe(PHONE);
			},
		),
		mk(
			"transport.wa.read-receipt-lifecycle",
			"wa: mark-as-read couples status:read + typing indicator on the LATEST inbound wamid; skips pre-conversation; stale-wamid 131009 contained",
			async (fx) => {
				// Pre-conversation: SKIP (zero wire calls).
				await fx.adapter.markReadAndTyping("15550000000");
				expect(fx.graph.readReceipts()).toHaveLength(0);

				await fx.postSigned(
					fx.valueEnvelope({
						messages: [
							fx.textMessage("wamid.old", "15551234567", "one"),
							fx.textMessage("wamid.newest", "15551234567", "two"),
						],
					}),
				);
				await new Promise<void>((r) => setTimeout(r, 40));
				await fx.adapter.markReadAndTyping("15551234567");

				const receipts = fx.graph.readReceipts();
				expect(receipts).toHaveLength(1);
				const body = receipts[0]?.body as Record<string, unknown>;
				expect(body["message_id"]).toBe("wamid.newest"); // latest
				expect(body["typing_indicator"]).toEqual({ type: "text" });

				// Stale-wamid rejection (code 131009) is info-class, never fatal.
				fx.graph.script("messages", {
					status: 400,
					error: {
						message: "(#131009) Parameter value is not valid",
						code: 131009,
					},
				});
				await expect(
					fx.adapter.markReadAndTyping("15551234567"),
				).resolves.toBeUndefined();
				expect(fx.adapter.receipts.some((r) => r.rejectedCode === 131009)).toBe(
					true,
				);
			},
		),
	];
}

describe("conformance suite — whatsapp-cloud census port (shape: webhook)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff reply-only)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // Cloud API: reply-only egress (§3)
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("passes EVERY applicable shared row against the whatsapp-cloud subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: "whatsapp-cloud",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it("passes the INHERITED webhook transport rows (reference fixture) over the REAL adapter", async () => {
		// flags/trust probe from the SUBJECT (kit capability getters + DEC-017
		// validation); bounded-window answer measured against the REAL ingress
		// handler while a turn is HELD.
		const subject = makeSubject() as WaCloudSubject;
		const probe = subject.flagsAndTrustProbe();

		const fx = makeWaCloudFixture();
		try {
			subject.adapter.holdTurns(true);
			const envelope = fx.valueEnvelope({
				messages: [fx.textMessage("wamid.bw", "15551234567", "window me")],
			});
			const raw = JSON.stringify(envelope);
			const startedAt = Date.now();
			const resp = await fx.postRaw(
				{ "x-hub-signature-256": signWith(raw) },
				raw,
			);
			const elapsed = Date.now() - startedAt;
			expect(resp.status).toBe(200); // verified deliveries answer FAST
			subject.adapter.holdTurns(false);

			const rows = makeWebhookRows({
				async flagsAndTrust() {
					return probe;
				},
				async boundedWindowAnswer() {
					return {
						answeredWithinWindowMs: elapsed,
						windowCapMs: 5_000,
					};
				},
			});
			const report = await runConformanceSuite({
				subjectName: "whatsapp-cloud-webhook-shape",
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

	it("passes ALL SIX WhatsApp shape-delta rows through the real engine fixture", async () => {
		const rows = waDeltaRows(() => makeWaCloudFixture());
		expect(rows.map((r) => r.id)).toEqual([
			"transport.wa.signature-negative-matrix",
			"transport.wa.status-callback-dedup",
			"transport.wa.window-classification-recorded",
			"transport.wa.media-cap-preupload",
			"transport.wa.lid-alias-continuity",
			"transport.wa.read-receipt-lifecycle",
		]);
		const report = await runConformanceSuite({
			subjectName: "whatsapp-cloud-deltas",
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

		const subject = makeSubject() as WaCloudSubject;
		const probe = subject.flagsAndTrustProbe();
		const transport = makeWebhookRows({
			async flagsAndTrust() {
				return probe;
			},
			async boundedWindowAnswer() {
				return { answeredWithinWindowMs: 12, windowCapMs: 5_000 };
			},
		});
		const deltas = waDeltaRows(() => makeWaCloudFixture());

		const report = await runConformanceSuite({
			subjectName: "whatsapp-cloud-full",
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

	it("the gate DETECTS violations: an inflating mutant fails its own named row", async () => {
		// Mutant: pretend oversize uploads REACHED Graph (cap not enforced
		// pre-upload). The media-cap row must fail BY NAME — rows are detectors.
		const rows = waDeltaRows(() => {
			const fx = makeWaCloudFixture();
			// The mutant's lie: every uploads-length observation reports one
			// PHANTOM upload — as if the oversized refusal had round-tripped.
			Object.defineProperty(fx.graph, "uploads", {
				get: () => ({ length: 1 }),
			});
			return fx;
		});

		const mediaRow = rows.find(
			(r) => r.id === "transport.wa.media-cap-preupload",
		);
		expect(mediaRow).toBeDefined();
		const others = rows.filter((r) => r.id !== mediaRow?.id);
		const mediaReport = await runConformanceSuite({
			subjectName: "mutant-wa-media",
			shape: "webhook",
			rows: [mediaRow as ConformanceRow],
		});
		expect(mediaReport.failed).toBe(1);
		expect(mediaReport.rows[0]?.pass).toBe(false);
		expect(mediaReport.rows[0]?.detail).toContain("expected"); // assertion tripped on the phantom

		// Sanity: the OTHER rows still pass on their own fresh fixtures.
		const otherReport = await runConformanceSuite({
			subjectName: "mutant-wa-others",
			shape: "webhook",
			rows: others as ConformanceRow[],
		});
		expect(otherReport.failed).toBe(0);
	});
});
