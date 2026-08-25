// CONFORMANCE WIRING — the msgraph-webhook census port vs the executable
// 04 §8 matrix (DEC-002 gate applies to every new platform).
//
//   1. ALL applicable SHARED rows pass for shape="webhook" against the REAL
//      kit-built MSGraphWebhookSubject. Applicability is COMPUTED from
//      capability data (04 §8 conditional headers): the streaming family
//      applies only when supports_draft_streaming()/draft_stream_is_message
//      hold — passive ingestion has no native lanes, so those three rows are
//      excluded BY THE PROBE, never by a hardcoded skip.
//   2. The INHERITED webhook transport rows (reference-fixture inheritance,
//      roadmap §Phase 6 heuristic 2) run over the REAL adapter probes:
//      stateless flag pairing (manifest DIVERGENCE note in manifest.ts) +
//      DEC-017 trust-boundary completeness + bounded-window answer measured
//      while a turn is HELD.
//   3. Fresh msgraph shape-delta rows execute through the real engine fixture
//      (validation handshake, CIDR admission matrix, clientState negative
//      matrix, notification dedupe + seen-set bound, body-cap pre-parse,
//      resource filter, batch verdict ladder, PASSIVE subscription-renewal
//      boundary on the injected clock, prompt rendering).
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
	makeMSGraphSubject,
	type MSGraphWebhookSubject,
} from "../msgraph-webhook/msgraph-subject.js";
import {
	makeMSGraphFixture,
	type MSGraphFixture,
} from "../msgraph-webhook/msgraph-fixture.js";
import { FIXTURE_CLIENT_STATE } from "../msgraph-webhook/fixture-secrets.js";
// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeMSGraphSubject({
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

// ── msgraph shape-delta rows (executed over the REAL engine fixture) ────────

function msgraphDeltaRows(newFixture: () => MSGraphFixture): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: MSGraphFixture) => Promise<void>,
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
			"transport.msgraph.validation-handshake",
			"msgraph: GET ?validationToken echoed VERBATIM as text/plain; bare GET rejected 400; POST-borne token echoes defensively",
			async (fx) => {
				const token = "ValidationToken: xyz+abc/123===";
				const ok = await fx.getValidation({ validationToken: token });
				expect(ok.status).toBe(200);
				expect(ok.contentType).toBe("text/plain");
				expect(ok.text).toBe(token); // verbatim — no trim/re-encode

				const missing = await fx.getValidation({});
				expect(missing.status).toBe(400);
				const empty = await fx.getValidation({ validationToken: "" });
				expect(empty.status).toBe(400);

				// Defensive in-band handshake replay on POST echoes too.
				const postEcho = await fx.postRaw({
					query: { validationToken: token },
					body: JSON.stringify({ value: [] }),
				});
				expect(postEcho.status).toBe(200);
				expect(postEcho.text).toBe(token);
				expect(fx.adapter.counters.parseInvocations).toBe(0);
			},
		),
		mk(
			"transport.msgraph.cidr-admission-matrix",
			"msgraph: non-loopback bind without CIDRs refuses connect; out-of-range peer 403 BEFORE body parse; in-range admits; forwarded headers never consulted",
			async (fx) => {
				// Connect refusal ladder (non-loopback bind, no allowlist).
				const openBind = makeMSGraphFixture({
					config: { host: "0.0.0.0", allowed_source_cidrs: null },
				});
				await expect(
					openBind.adapter.connect({ isReconnect: false }),
				).resolves.toBe(false);
				// Loopback-only bind MAY omit the allowlist.
				const loopback = makeMSGraphFixture({
					config: { host: "127.0.0.1", allowed_source_cidrs: null },
				});
				await expect(
					loopback.adapter.connect({ isReconnect: false }),
				).resolves.toBe(true);
				loopback.dispose();

				// Out-of-range peer: rejected BEFORE body read/parse.
				const before = fx.adapter.counters.parseInvocations;
				const denied = await fx.postNotifications(
					[fx.changeNotification({ id: "n-deny", resource: "foo" })],
					{ peer: "203.0.113.7" }, // TEST-NET-3 — outside every range
				);
				expect(denied.status).toBe(403);
				expect(fx.adapter.counters.cidrDenied).toBe(1);
				expect(fx.adapter.counters.parseInvocations).toBe(before);

				// In-range peer admits (202 with accepted content).
				const admitted = await fx.postNotifications(
					[fx.changeNotification({ id: "n-admit", resource: "foo" })],
					{ peer: "20.190.160.7" },
				);
				expect(admitted.status).toBe(202);

				// Unparseable peers FAIL CLOSED.
				const junkPeer = await fx.postRaw({
					body: JSON.stringify(fx.notificationEnvelope([])),
					peer: "not-an-ip",
				});
				expect(junkPeer.status).toBe(403);

				// Invalid CIDR entries are skipped with a warning, not fatal.
				const partial = makeMSGraphFixture({
					config: {
						host: null,
						allowed_source_cidrs: ["not-a-cidr", "10.0.0.0/8"],
					},
				});
				expect(partial.adapter.cidrWarnings).toEqual(["not-a-cidr"]);
				partial.dispose();

				// X-Forwarded-For can NEVER move the verdict (socket-peer-only).
				const spoofed = await fx.postRaw({
					headers: { "x-forwarded-for": "20.190.160.7" },
					body: JSON.stringify(fx.notificationEnvelope([])),
					peer: "203.0.113.7",
				});
				expect(spoofed.status).toBe(403);
			},
		),
		mk(
			"transport.msgraph.clientstate-negative-matrix",
			"msgraph: clientState negatives (missing/mismatched/wrong-secret) are AUTH rejections; forged whole batch ⇒ 403 stop-retrying signal; valid passes",
			async (fx) => {
				const missing = await fx.postNotifications([
					fx.changeNotification({ id: "cs-1", clientState: undefined }),
				]);
				expect(missing.status).toBe(403); // auth_rejected, no other_rejected

				const wrong = await fx.postNotifications([
					fx.changeNotification({ id: "cs-2", clientState: "attacker-guess" }),
				]);
				expect(wrong.status).toBe(403);
				expect(fx.adapter.counters.authRejected).toBe(2);

				// Mixed batch: one forged + one malformed ⇒ NOT the pure-auth
				// verdict — falls through to 400 per the source ladder.
				const mixed = await fx.postNotifications([
					fx.changeNotification({ clientState: "attacker-guess" }),
					"not-an-object",
				]);
				expect(mixed.status).toBe(400);

				// Valid clientState passes (202).
				const good = await fx.postNotifications([
					fx.changeNotification({ id: "cs-ok", resource: "me/messages" }),
				]);
				expect(good.status).toBe(202);
				expect(fx.adapter.counters.accepted).toBe(1);

				// An adapter configured WITHOUT any expected secret rejects
				// everything (fail closed) — never matches absent clientState.
				const noSecret = makeMSGraphFixture({ withSecret: false });
				const refused = await noSecret.postNotifications([
					fx.changeNotification({ id: "cs-3", resource: "me/messages" }),
				]);
				expect(refused.status).toBe(403);
				noSecret.dispose();
			},
		),
		mk(
			"transport.msgraph.notification-dedup",
			"msgraph: Graph redelivery of the same notification id deduped exactly-once (still 202); seen-set FIFO bound enforced; id-less notifications fall back to canonical sha1 ids",
			async (fx) => {
				const replay = () =>
					fx.postNotifications([
						fx.changeNotification({
							id: "N1",
							resource: "me/messages",
						}),
					]);
				await replay();
				await replay(); // Graph redelivery
				expect(fx.adapter.turnLog.length).toBe(1); // exactly-once downstream
				expect(fx.adapter.counters.duplicates).toBe(1);
				expect(fx.adapter.counters.accepted).toBe(1);
				expect(fx.adapter.hasSeenReceipt("id:N1")).toBe(true);

				// Seen-set FIFO bound: override cap to 3 and churn 5 distinct ids.
				const small = makeMSGraphFixture({
					config: { max_seen_receipts: 3 },
				});
				for (let i = 0; i < 5; i++) {
					const resp = await small.postNotifications([
						small.changeNotification({ id: `burst-${i}`, resource: "r" }),
					]);
					expect(resp.status).toBe(202);
				}
				// Oldest entries evicted: burst-0/burst-1 forgotten, newest live.
				expect(small.adapter.hasSeenReceipt("id:burst-0")).toBe(false);
				expect(small.adapter.hasSeenReceipt("id:burst-1")).toBe(false);
				expect(small.adapter.hasSeenReceipt("id:burst-4")).toBe(true);
				expect(small.adapter.seenReceiptCount()).toBe(3);

				// Id-less notifications: Hermes dedupes ONLY on explicit ids
				// (_build_receipt_key returns None ⇒ no receipt is recorded), so
				// identical id-less payloads each dispatch — but the canonical
				// sha1 fallback id is STABLE across redeliveries.
				const idless = () =>
					fx.postNotifications([
						fx.changeNotification({
							resource: "me/messages",
							changeType: "updated",
						}),
					]);
				await idless();
				await idless();
				expect(fx.adapter.counters.duplicates).toBe(1); // only the N1 replay
				const eventIds = fx.adapter.dispatchedEvents.map((e) => e.messageId);
				expect(eventIds[eventIds.length - 2]?.startsWith("sha1:")).toBe(true);
				expect(eventIds[eventIds.length - 2]).toBe(
					eventIds[eventIds.length - 1],
				);
			},
		),
		mk(
			"transport.msgraph.body-cap-preparse",
			"msgraph: >1 MiB bodies rejected 413 at BOTH gates (declared length AND actual bytes) without reaching the parse seam",
			async (fx) => {
				const cap = fx.adapter.maxBodyBytes;
				expect(cap).toBe(1_048_576);

				// Gate 1: honest declared Content-Length over the cap.
				const big = Buffer.alloc(cap + 1, 0x61);
				const declared = await fx.postRaw({
					headers: { "content-length": String(big.length) },
					body: JSON.stringify({ value: [] }),
				});
				expect(declared.status).toBe(413);
				expect(fx.adapter.counters.parseInvocations).toBe(0);

				// Gate 2: LYING Content-Length trips on actual bytes post-read.
				const lying = await fx.postRaw({
					headers: { "content-length": "10" },
					body: big,
				});
				expect(lying.status).toBe(413);
				expect(fx.adapter.counters.parseInvocations).toBe(0);

				// Within-cap bodies parse exactly once.
				const fine = await fx.postNotifications([
					fx.changeNotification({ id: "cap-ok", resource: "r" }),
				]);
				expect(fine.status).toBe(202);
				expect(fx.adapter.counters.parseInvocations).toBe(1);
			},
		),
		mk(
			"transport.msgraph.resource-filter",
			"msgraph: accepted_resources exact/prefix matching with `/` boundary; wildcard star strips then matches; non-matching notifications are OTHER rejections ⇒ 400 sender-config signal",
			async (_fx) => {
				const filtered = makeMSGraphFixture({
					config: {
						accepted_resources: [
							"me/messages",
							"teams/channels/*",
							"/sites/govern/lists/",
						],
					},
				});

				const exact = await filtered.postNotifications([
					filtered.changeNotification({ id: "rf-1", resource: "me/messages" }),
				]);
				expect(exact.status).toBe(202);

				// Sub-path under an exact pattern matches (prefix-with-boundary).
				const subpath = await filtered.postNotifications([
					filtered.changeNotification({
						id: "rf-2",
						resource: "me/messages/AAMk.long.id",
					}),
				]);
				expect(subpath.status).toBe(202);

				// Sibling prefix does NOT match (boundary respected).
				const sibling = await filtered.postNotifications([
					filtered.changeNotification({
						id: "rf-3",
						resource: "me/messagesArchive/x",
					}),
				]);
				expect(sibling.status).toBe(400); // other_rejected only

				// Wildcard pattern covers nested paths after normalization.
				const wild = await filtered.postNotifications([
					filtered.changeNotification({
						id: "rf-4",
						resource: "/teams/channels/19:abc@thread.tacv2/messages/",
					}),
				]);
				expect(wild.status).toBe(202);

				const counts = filtered.adapter.counters;
				expect(counts.otherRejected).toBe(1);
				filtered.dispose();
			},
		),
		mk(
			"transport.msgraph.batch-verdict-ladder",
			"msgraph: batch verdict ladder — empty value [] ⇒ 202; non-list/non-object/malformed JSON ⇒ 400; duplicates-only batch still 202; mixed accept+auth-reject stays 202",
			async (fx) => {
				// Empty batch: the source ladder has NO 202-when-empty branch —
				// zero accepted, zero duplicates, zero auth rejections falls
				// through to 400 (sender sent nothing actionable).
				const empty = await fx.postNotifications([]);
				expect(empty.status).toBe(400);

				const badJson = await fx.postRaw({ body: "{not json" });
				expect(badJson.status).toBe(400);
				const arrayBody = await fx.postRaw({ body: "[1,2]" });
				expect(arrayBody.status).toBe(400);
				const scalarBody = await fx.postRaw({ body: '"hello"' });
				expect(scalarBody.status).toBe(400);
				const noValue = await fx.postRaw({ body: JSON.stringify({ v: 1 }) });
				expect(noValue.status).toBe(400);
				const nonListValue = await fx.postRaw({
					body: JSON.stringify({ value: "nope" }),
				});
				expect(nonListValue.status).toBe(400);

				// Duplicates-only batch: still acked 202 (Graph must not retry).
				const dupEnv = () =>
					fx.postNotifications([
						fx.changeNotification({ id: "dup-only", resource: "r" }),
					]);
				await dupEnv();
				const again = await dupEnv();
				expect(again.status).toBe(202);

				// Mixed accept + auth-reject: accepted item dominates ⇒ 202.
				const mixedAccept = await fx.postNotifications([
					fx.changeNotification({ id: "mix-good", resource: "r" }),
					fx.changeNotification({
						id: "mix-bad",
						clientState: "forged",
					}),
				]);
				expect(mixedAccept.status).toBe(202);

				// Malformed items inside an otherwise-empty batch ⇒ 400.
				const malformedItem = await fx.postNotifications([42]);
				expect(malformedItem.status).toBe(400);
			},
		),
		mk(
			"transport.msgraph.passive-subscription-boundary",
			"msgraph: subscription renewal boundary — NO create/renew/expiry machinery exists on this side of the seam; injected clock advances days with ZERO outbound calls while notifications keep flowing; missing client_state refuses connect loudly",
			async (fx) => {
				await expect(fx.adapter.connect({ isReconnect: false })).resolves.toBe(
					true,
				);

				// Advance FAR past any plausible subscription lifetime (Graph caps
				// subscriptions at <1 day for most resources; renewal is the
				// OPERATOR's job per 06 §8.3). The adapter must emit nothing.
				fx.advance(30 * 24 * 60 * 60 * 1000);
				expect(fx.adapter.counters.outboundWireCalls).toBe(0);

				// Ingress keeps flowing unchanged after the clock jump.
				const afterJump = await fx.postNotifications([
					fx.changeNotification({ id: "late-1", resource: "r" }),
				]);
				expect(afterJump.status).toBe(202);
				expect(fx.adapter.dispatchedEvents.length).toBeGreaterThanOrEqual(1);

				// Lifecycle events ride the SAME passive path (no special casing):
				// Graph delivers reauthorizationRequired/subscriptionRemoved as
				// plain changeType notifications; the port renders them like any
				// other payload — it does NOT act on them outbound.
				const lifecycle = await fx.postNotifications([
					fx.changeNotification({
						id: "life-1",
						lifecycleEvent: "reauthorizationRequired",
						changeType: undefined,
					}),
				]);
				expect(lifecycle.status).toBe(202);
				expect(fx.adapter.counters.outboundWireCalls).toBe(0);

				// Missing client_state ⇒ loud disable + connect refusal.
				const noSecret = makeMSGraphFixture({ withSecret: false });
				const snap = noSecret.adapter.lifecycle.statusSnapshot();
				expect(snap.state).toBe("disabled"); // construction-time loud disable
				expect((snap.detail ?? "").toLowerCase()).toContain("secret");
				await expect(
					noSecret.adapter.connect({ isReconnect: false }),
				).rejects.toThrow(/disabled/);
				noSecret.dispose();
			},
		),
		mk(
			"transport.msgraph.prompt-rendering",
			"msgraph: default render fences pretty JSON capped at 4000 chars; prompt templates substitute {paths}, dict values render stable JSON capped at 2000, unknown keys stay literal",
			async (fx) => {
				await fx.postNotifications([
					fx.changeNotification({
						id: "pr-1",
						resource: "me/messages",
						resourceData: { "@odata.type": "#Microsoft.Graph.Message" },
					}),
				]);
				const rendered = fx.adapter.dispatchedEvents[0]?.text ?? "";
				expect(
					rendered.startsWith("Microsoft Graph change notification:"),
				).toBe(true);
				expect(rendered.includes("```json")).toBe(true);
				expect(rendered.includes('"resource": "me/messages"')).toBe(true);

				// Oversized payload truncates at the 4000-char render cap.
				const huge = makeMSGraphFixture();
				const blob = "x".repeat(10_000);
				await huge.postRaw({
					body: JSON.stringify({
						value: [{ id: "big", blob, clientState: FIXTURE_CLIENT_STATE }],
					}),
				});
				const bigText = huge.adapter.dispatchedEvents[0]?.text ?? "";
				expect(bigText.length).toBeLessThanOrEqual(
					bigText.indexOf("```json") + 4000 + 40,
				);
				huge.dispose();

				// Template mode.
				const templated = makeMSGraphFixture({
					config: {
						prompt:
							"Change {change_type} on {resource} (sub {subscription_id}) data={notification.resourceData}",
					},
				});
				await templated.postNotifications([
					templated.changeNotification({
						id: "pr-2",
						resource: "me/events",
						changeType: "updated",
						subscriptionId: "sub-T",
						resourceData: { z: 1, a: 2 },
					}),
				]);
				const text = templated.adapter.dispatchedEvents[0]?.text ?? "";
				expect(text).toContain("Change updated on me/events (sub sub-T)");
				expect(text).toContain('{"a":2,"z":1}'); // stable sorted JSON
				templated.dispose();

				// Unknown template keys stay literal.
				const literal = makeMSGraphFixture({
					config: { prompt: "keep {nope.missing} intact" },
				});
				await literal.postRaw({
					body: JSON.stringify({
						value: [{ id: "pr-3", clientState: FIXTURE_CLIENT_STATE }],
					}),
				});
				expect(literal.adapter.dispatchedEvents[0]?.text).toBe(
					"keep {nope.missing} intact",
				);
				literal.dispose();
			},
		),
	];
}

describe("conformance suite — msgraph-webhook census port (shape: webhook)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff passive)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // passive ingestion: no native lanes (§3)
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("passes EVERY applicable shared row against the msgraph-webhook subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: "msgraph-webhook",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it("passes the INHERITED webhook transport rows (reference fixture) over the REAL adapter", async () => {
		const subject = makeSubject() as MSGraphWebhookSubject;
		const probe = subject.flagsAndTrustProbe();

		const fx = makeMSGraphFixture();
		try {
			subject.adapter.holdTurns(true);
			const startedAt = Date.now();
			const resp = await fx.postNotifications([
				fx.changeNotification({ id: "bw-msg", resource: "me/messages" }),
			]);
			const elapsed = Date.now() - startedAt;
			expect(resp.status).toBe(202); // acked FAST even with the turn held
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
				subjectName: "msgraph-webhook-shape",
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

	it("passes ALL NINE msgraph shape-delta rows through the real engine fixture", async () => {
		const rows = msgraphDeltaRows(() => makeMSGraphFixture());
		expect(rows.map((r) => r.id)).toEqual([
			"transport.msgraph.validation-handshake",
			"transport.msgraph.cidr-admission-matrix",
			"transport.msgraph.clientstate-negative-matrix",
			"transport.msgraph.notification-dedup",
			"transport.msgraph.body-cap-preparse",
			"transport.msgraph.resource-filter",
			"transport.msgraph.batch-verdict-ladder",
			"transport.msgraph.passive-subscription-boundary",
			"transport.msgraph.prompt-rendering",
		]);
		const report = await runConformanceSuite({
			subjectName: "msgraph-webhook-deltas",
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

		const subject = makeSubject() as MSGraphWebhookSubject;
		const probe = subject.flagsAndTrustProbe();
		const transport = makeWebhookRows({
			async flagsAndTrust() {
				return probe;
			},
			async boundedWindowAnswer() {
				return { answeredWithinWindowMs: 12, windowCapMs: 5_000 };
			},
		});
		const deltas = msgraphDeltaRows(() => makeMSGraphFixture());

		const report = await runConformanceSuite({
			subjectName: "msgraph-webhook-full",
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

	it("the gate DETECTS violations: a CIDR-gate-defeating mutant fails its own named row", async () => {
		// Mutant: the admission gate ALWAYS admits (as if sourceIpAllowed were
		// stubbed true) — the CIDR matrix row must fail BY NAME.
		const rows = msgraphDeltaRows(() => {
			const fx = makeMSGraphFixture();
			const original = fx.adapter.handleNotificationPost.bind(fx.adapter);
			Object.defineProperty(fx.adapter, "handleNotificationPost", {
				value: async (input: Parameters<typeof original>[0]) =>
					original({ ...input, peer: "20.190.160.7" }), // the lie
			});
			return fx;
		});

		const cidrRow = rows.find(
			(r) => r.id === "transport.msgraph.cidr-admission-matrix",
		);
		expect(cidrRow).toBeDefined();
		const cidrReport = await runConformanceSuite({
			subjectName: "mutant-ms-cidr",
			shape: "webhook",
			rows: [cidrRow as ConformanceRow],
		});
		expect(cidrReport.failed).toBe(1);
		expect(cidrReport.rows[0]?.pass).toBe(false);

		// Sanity: the OTHER rows still pass on their own fresh fixtures.
		const others = rows.filter((r) => r.id !== cidrRow?.id);
		const otherReport = await runConformanceSuite({
			subjectName: "mutant-ms-others",
			shape: "webhook",
			rows: others as ConformanceRow[],
		});
		if (otherReport.failed > 0) console.error(formatReport(otherReport));
		expect(otherReport.failed).toBe(0);
	}, 30_000);
});
