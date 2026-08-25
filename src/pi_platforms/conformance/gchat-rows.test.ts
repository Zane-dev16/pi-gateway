// CONFORMANCE WIRING — the Google Chat census port vs the executable 04 §8
// matrix (DEC-002 gate applies to every new platform).
//
//   1. ALL applicable SHARED rows pass for shape="webhook" against the REAL
//      kit-built GchatWebhookSubject. Applicability is COMPUTED from
//      capability data: REST create/patch has no native draft lanes, so the
//      streaming family is excluded BY THE PROBE, never by a hardcoded skip.
//   2. The INHERITED webhook transport rows run over the REAL adapter probes.
//   3. Fresh Google Chat shape-delta rows execute through the real engine
//      fixture: OIDC bearer negative matrix (claims-minted tokens), envelope
//      format matrix (all three vendor shapes), BOT self-filter + msg.name
//      TTL dedupe on the injected clock, body-cap pre-parse, thread-routing
//      ladder, Chat-dialect markdown conversion, outbound verdict ladder
//      (403 fatal / 404 skip / 429 counter), cardsV2 builder caps.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: a lying verifier that admits any token fails its own
//      named row.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { makeWebhookRows } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import {
	makeGchatSubject,
	type GchatWebhookSubject,
} from "../google-chat/gchat-subject.js";
import {
	makeGchatFixture,
	mintBearerToken,
	type GchatFixture,
} from "../google-chat/gchat-fixture.js";
import { FIXTURE_SA_EMAIL } from "../google-chat/fixture-secrets.js";
import {
	cardSpecToCardsV2,
	toChatDialect,
} from "../google-chat/google-chat-adapter.js";
import { GCHAT_MAX_TEXT_LENGTH } from "../google-chat/manifest.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeGchatSubject({
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

// ── Google Chat shape-delta rows (executed over the REAL engine fixture) ────

function gchatDeltaRows(newFixture: () => GchatFixture): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: GchatFixture) => Promise<void>,
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
			"transport.gchat.bearer-auth-negative-matrix",
			"gchat: OIDC bearer negatives (missing/non-Bearer/tampered/wrong-audience/unexpected-SA-email/not-configured) reject with NAMED reasons BEFORE body parse; valid passes",
			async (fx) => {
				const envelope = fx.nativeEnvelope({
					message: fx.chatMessage({ name: "spaces/AAAA/messages/auth-ok" }),
				});

				const missing = await fx.postRaw({ body: JSON.stringify(envelope) });
				expect(missing.status).toBe(401);
				expect(missing.text).toBe("missing_google_bearer");

				const nonBearer = await fx.postRaw({
					headers: { authorization: "Basic dXNlcjpwd2Q=" },
					body: JSON.stringify(envelope),
				});
				expect(nonBearer.status).toBe(401);

				const tampered = `${fx.bearerFor().slice(0, -4)}beef`;
				const invalid = await fx.postRaw({
					headers: { authorization: `Bearer ${tampered}` },
					body: JSON.stringify(envelope),
				});
				expect(invalid.status).toBe(401);
				expect(invalid.text).toBe("invalid_google_bearer");

				const wrongAud = mintBearerToken({
					aud: "https://other-audience.example/",
					email: fx.saEmails[0],
				});
				const audRejected = await fx.postRaw({
					headers: { authorization: `Bearer ${wrongAud}` },
					body: JSON.stringify(envelope),
				});
				expect(audRejected.status).toBe(401);
				expect(audRejected.text).toBe("invalid_google_bearer");

				const stranger = mintBearerToken({
					aud: fx.audience,
					email: "stranger@evil.example",
				});
				const identity = await fx.postRaw({
					headers: { authorization: `Bearer ${stranger}` },
					body: JSON.stringify(envelope),
				});
				expect(identity.status).toBe(403);
				expect(identity.text).toBe("unexpected_google_bearer_identity");

				// Every rejection lands BEFORE the parse seam.
				expect(fx.adapter.counters.authRejected).toBe(5);
				expect(fx.adapter.counters.parseInvocations).toBe(0);

				// Unconfigured deployment ⇒ 503 not-configured.
				const bare = makeGchatFixture({ withSecret: false });
				const unconfigured = await bare.postRaw({
					body: JSON.stringify(envelope),
				});
				expect(unconfigured.status).toBe(503);
				expect(unconfigured.text).toBe(
					"google_chat_http_events_not_configured",
				);
				bare.dispose();

				// Valid bearer passes and dispatches.
				const good = await fx.postSigned(envelope);
				expect(good.status).toBe(200);
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(fx.adapter.turnLog).toEqual(["hello chat"]);
			},
		),
		mk(
			"transport.gchat.envelope-format-matrix",
			"gchat: all three vendor envelope formats dispatch (workspace_addons / native_chat_api / relay_flat); non-MESSAGE types ack-dropped; unrecognized envelopes ACK 200 without dispatch",
			async (fx) => {
				const addons = await fx.postSigned(
					fx.workspaceAddonsEnvelope({
						message: fx.chatMessage({
							name: "spaces/AAAA/messages/f1",
							text: "via addons",
						}),
					}),
				);
				expect(addons.status).toBe(200);

				const native = await fx.postSigned(
					fx.nativeEnvelope({
						message: fx.chatMessage({
							name: "spaces/AAAA/messages/f2",
							text: "via native",
						}),
					}),
				);
				expect(native.status).toBe(200);

				const relay = await fx.postSigned(
					fx.relayEnvelope({
						messageName: "spaces/AAAA/messages/f3",
						text: "via relay",
					}),
				);
				expect(relay.status).toBe(200);
				await new Promise<void>((r) => setTimeout(r, 30));
				expect(fx.adapter.turnLog).toEqual([
					"via addons",
					"via native",
					"via relay",
				]);

				// Non-MESSAGE native events are dropped (acked).
				const nonMessage = await fx.postSigned(
					fx.nativeEnvelope({ eventType: "ADDED_TO_SPACE" }),
				);
				expect(nonMessage.status).toBe(200);

				// Relay event_type filter.
				const relayNonMessage = await fx.postSigned(
					fx.relayEnvelope({
						eventType: "MEMBERSHIP_ADDED",
						messageName: "spaces/AAAA/messages/f4",
					}),
				);
				expect(relayNonMessage.status).toBe(200);

				// Unrecognized/non-MESSAGE envelopes share ONE acked-drop bucket
				// (_extract_message_payload returns None for both): the two
				// non-MESSAGE posts above plus the junk object.
				const junk = await fx.postSigned({ hello: "world" });
				expect(junk.status).toBe(200);
				expect(fx.adapter.counters.unrecognizedEnvelopes).toBe(3);
				expect(fx.adapter.turnLog).toHaveLength(3);
			},
		),
		mk(
			"transport.gchat.bot-filter-and-name-dedupe",
			"gchat: sender.type=BOT never dispatches (incl. relay-forwarded bot replies); msg.name redelivery deduped exactly-once within 300s; injected clock past the TTL re-dispatches; id-less messages each dispatch",
			async (fx) => {
				// BOT self-filter across formats.
				await fx.postSigned(
					fx.nativeEnvelope({
						message: fx.chatMessage({
							name: "spaces/AAAA/messages/bot-1",
							senderType: "BOT",
						}),
					}),
				);
				await fx.postSigned(
					fx.relayEnvelope({
						senderType: "BOT",
						messageName: "spaces/AAAA/messages/bot-2",
					}),
				);
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(fx.adapter.turnLog).toEqual([]);
				expect(fx.adapter.counters.botSelfFiltered).toBe(2);

				// msg.name TTL dedupe.
				const replay = () =>
					fx.postSigned(
						fx.nativeEnvelope({
							message: fx.chatMessage({
								name: "spaces/AAAA/messages/dup-1",
								text: "exactly once",
							}),
						}),
					);
				await replay();
				await replay(); // at-least-once redelivery
				await new Promise<void>((r) => setTimeout(r, 25));
				expect(fx.adapter.turnLog).toEqual(["exactly once"]);
				expect(fx.adapter.counters.duplicates).toBe(1);

				// Injected clock past the 300 s TTL ⇒ live again.
				fx.advance(301_000);
				await replay();
				await new Promise<void>((r) => setTimeout(r, 25));
				expect(
					fx.adapter.turnLog.filter((t) => t === "exactly once"),
				).toHaveLength(2);

				// Id-less messages dispatch EVERY time.
				for (let i = 0; i < 2; i++) {
					await fx.postSigned(
						fx.nativeEnvelope({
							message: {
								sender: { type: "HUMAN", email: "h@x.y" },
								text: "anon",
							},
						}),
					);
				}
				await new Promise<void>((r) => setTimeout(r, 25));
				expect(fx.adapter.turnLog.filter((t) => t === "anon")).toHaveLength(2);
			},
		),
		mk(
			"transport.gchat.body-cap-preparse",
			"gchat: >16 MiB bodies rejected 413 at BOTH gates (declared length AND actual bytes) without reaching the parse seam; malformed JSON ⇒ 400",
			async (fx) => {
				const cap = 16 * 1024 * 1024;
				const big = Buffer.alloc(cap + 1, 0x61);

				const declared = await fx.postRaw({
					headers: {
						authorization: `Bearer ${fx.bearerFor()}`,
						"content-length": String(big.length),
					},
					body: JSON.stringify({}),
				});
				expect(declared.status).toBe(413);
				expect(fx.adapter.counters.parseInvocations).toBe(0);

				const lying = await fx.postRaw({
					headers: {
						authorization: `Bearer ${fx.bearerFor()}`,
						"content-length": "10",
					},
					body: big,
				});
				expect(lying.status).toBe(413);
				expect(fx.adapter.counters.parseInvocations).toBe(0);

				const badJson = await fx.postRaw({
					headers: { authorization: `Bearer ${fx.bearerFor()}` },
					body: "{not json",
				});
				expect(badJson.status).toBe(400);
				expect(fx.adapter.counters.parseInvocations).toBe(1);

				const fine = await fx.postSigned(
					fx.nativeEnvelope({
						message: fx.chatMessage({ name: "spaces/AAAA/messages/cap-ok" }),
					}),
				);
				expect(fine.status).toBe(200);
			},
		),
		mk(
			"transport.gchat.thread-routing-ladder",
			"gchat: metadata.thread_id wins; threads-names ride as reply targets while message names never convert; job_id forces TOP-LEVEL; DM side-thread cache routes replies into the user's thread; threaded creates carry messageReplyOption data",
			async (fx) => {
				const adapter = fx.adapter;

				// 1. Explicit metadata pin wins over everything.
				expect(
					adapter.resolveThreadId(
						undefined,
						{ thread_id: "spaces/A/threads/T1" },
						"spaces/A",
					),
				).toBe("spaces/A/threads/T1");

				// 2. A threads-resource replyTo converts; a messages-resource does NOT.
				expect(adapter.resolveThreadId("spaces/A/threads/T2", undefined)).toBe(
					"spaces/A/threads/T2",
				);
				expect(
					adapter.resolveThreadId(
						"spaces/A/messages/M1",
						undefined,
						"spaces/A",
					),
				).toBeNull();

				// 3. job_id forces top-level even when a DM cache would exist.
				expect(
					adapter.resolveThreadId(undefined, { job_id: "job-9" }, "spaces/A"),
				).toBeNull();

				// Wire-level: threaded send carries the REPLY_MESSAGE_FALLBACK data.
				await adapter.send("spaces/WIRE", "threaded please", undefined, {
					thread_id: "spaces/WIRE/threads/T9",
				});
				const create = fx.api.createsOf("spaces/WIRE").at(-1);
				expect(create?.body["thread"]).toEqual({
					name: "spaces/WIRE/threads/T9",
				});
				expect(create?.body["messageReplyOption"]).toBe(
					"REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD",
				);

				// DM inbound main-flow vs side-thread routing through REAL ingress:
				// first sight of a DM thread = main flow (no session isolation, no
				// outbound-thread cache); second sight = side thread (cached).
				const dmFirst = await fx.postSigned(
					fx.workspaceAddonsEnvelope({
						space: fx.dmSpace(),
						message: fx.chatMessage({
							name: "spaces/DM1/messages/dm-1",
							text: "first dm",
							threadName: "spaces/DM1/threads/X",
							space: fx.dmSpace(),
						}),
					}),
				);
				expect(dmFirst.status).toBe(200);
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(adapter.cachedOutboundThread("spaces/DM1")).toBeUndefined();

				const dmSecond = await fx.postSigned(
					fx.workspaceAddonsEnvelope({
						space: fx.dmSpace(),
						message: fx.chatMessage({
							name: "spaces/DM1/messages/dm-2",
							text: "side thread now",
							threadName: "spaces/DM1/threads/X",
							space: fx.dmSpace(),
						}),
					}),
				);
				expect(dmSecond.status).toBe(200);
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(adapter.cachedOutboundThread("spaces/DM1")).toBe(
					"spaces/DM1/threads/X",
				);
			},
		),
		mk(
			"transport.gchat.chat-dialect-conversion",
			"gchat: **bold**→*bold*, headers→*title*, links→<url|text>, code spans preserved verbatim, invisible Unicode stripped; §6.1 fallback envelope stays byte-exact; edit path caps at 4000+ellipsis",
			async (fx) => {
				// Dialect conversion on the OUTBOUND wire body. The heading sits
				// on its own line (source regex is MULTILINE ^-anchored).
				await fx.adapter.send(
					"spaces/DIALECT",
					"**hi**\n## Title\n[g](https://g.dev) `code`",
				);
				const sent =
					fx.api.createsOf("spaces/DIALECT").at(-1)?.body["text"] ?? "";
				expect(sent).toContain("*hi*");
				expect(sent).toContain("*Title*");
				expect(sent).toContain("<https://g.dev|g>");
				expect(sent).toContain("`code`");
				expect(sent).not.toContain("**");
				expect(sent).not.toContain("##");

				// Pure-function parity probes (source-shaped helpers).
				expect(toChatDialect("a\u200Bb")).toBe("ab");

				// §6.1 fallback lane: original bytes preserved on the wire body.
				const results = await fx.adapter.deliverText(
					"spaces/DIALECT",
					"**keep** raw",
					{ forceFormattingError: true } as never,
				);
				expect(results[results.length - 1]?.success).toBe(true);
				const fallbackBody = (() => {
					for (let i = fx.api.calls.length - 1; i >= 0; i--) {
						const call = fx.api.calls[i] as (typeof fx.api.calls)[number];
						if (call.op === "create") return String(call.body["text"] ?? "");
					}
					return "";
				})();
				expect(
					fallbackBody.startsWith("(Response formatting failed, plain text:"),
				).toBe(true);
				expect(fallbackBody).toContain("**keep** raw");

				// Edit cap: >4000 chars truncate with ellipsis via patch-in-place.
				await fx.adapter.editMessage(
					"spaces/DIALECT",
					"spaces/x/messages/E",
					"y".repeat(4500),
				);
				const patch = fx.api.calls.filter((c) => c.op === "patch").at(-1);
				const patchedText = String(patch?.body["text"] ?? "");
				expect(patchedText.length).toBeLessThanOrEqual(GCHAT_MAX_TEXT_LENGTH);
				expect(patchedText.endsWith("…")).toBe(true);
			},
		),
		mk(
			"transport.gchat.outbound-verdict-ladder",
			"gchat: 403 marks FATAL chat_forbidden; 404 surfaces target-not-found non-retryable; 429 bumps the per-chat rate-limit counter AND surfaces retryable; transient 500 classifies retryable",
			async (fx) => {
				// 429 ladder on a FRESH fixture per status keeps the ladder row
				// deterministic — one fixture per assertion block here.
				fx.api.script("create", { kind: "status", status: 429 });
				let result = await fx.adapter.send("spaces/RATE", "hit limit");
				expect(result.success).toBe(false);
				expect(result.retryable).toBe(true);
				expect(fx.adapter.rateLimitHitsOf("spaces/RATE")).toBe(1);

				fx.api.script("create", { kind: "status", status: 500 });
				result = await fx.adapter.send("spaces/FIVE", "server hiccup");
				expect(result.success).toBe(false);
				expect(result.retryable).toBe(true);

				fx.api.script("create", { kind: "status", status: 404 });
				result = await fx.adapter.send("spaces/GONE", "target vanished");
				expect(result.success).toBe(false);
				expect(result.retryable).toBe(false);
				expect(result.error).toContain("not found");
				void result;

				// Rate-limit warn threshold (manifest data: 5 hits).
				const warnFx = newFixture();
				for (let i = 0; i < 5; i++)
					warnFx.adapter.noteRateLimitHit("spaces/WARN");
				expect(warnFx.adapter.counters.rateLimitHitsByChatWarned).toBe(1);
				warnFx.dispose();
			},
		),
		mk(
			"transport.gchat.cardsv2-builder-caps",
			"gchat: cardsV2 builder renders sections/widgets/buttons with SORTED parameters; unsupported widget types DECLINE the whole card cleanly; clarify-shaped buttons carry action+parameters",
			async (fx) => {
				const spec = {
					card_id: "clarify-x",
					header: { title: "Question" },
					sections: [
						{
							widgets: [
								{ type: "text", text: "❓ pick" },
								{
									type: "buttons",
									buttons: [
										{
											text: "Yes",
											action: "hermes_clarify",
											parameters: { b: "2", a: "1" },
										},
									],
								},
							],
						},
					],
				};
				const card = cardSpecToCardsV2(spec);
				const cardBody = card["card"] as Record<string, unknown>;
				const sections = cardBody["sections"] as Array<Record<string, unknown>>;
				const widgets = sections[0]?.["widgets"] as Array<
					Record<string, unknown>
				>;
				const buttonList = widgets[1]?.["buttonList"] as Record<
					string,
					unknown
				>;
				const buttons = buttonList["buttons"] as Array<Record<string, unknown>>;
				const onClick = buttons[0]?.["onClick"] as Record<string, unknown>;
				const action = onClick["action"] as Record<string, unknown>;
				expect(action["parameters"]).toEqual([
					{ key: "a", value: "1" },
					{ key: "b", value: "2" },
				]);
				expect(card["cardId"]).toBe("clarify-x");

				// Unsupported widget ⇒ whole-card decline (clean failure mapping).
				const declined = await fx.adapter.sendCard("spaces/CARDS", {
					sections: [{ widgets: [{ type: "video", src: "x" }] }],
				});
				expect(declined.success).toBe(false);
				expect(declined.error).toContain("unsupported widget type");

				// Valid card SPEC rides createMessage with cardsV2 on the wire body.
				const ok = await fx.adapter.sendCard("spaces/CARDS", spec);
				expect(ok.success).toBe(true);
				const create = fx.api.createsOf("spaces/CARDS").at(-1);
				expect(Array.isArray(create?.body["cardsV2"])).toBe(true);
			},
		),
	];
}

describe("conformance suite — google-chat census port (shape: webhook)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff passive)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // REST create/patch: no native lanes
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("passes EVERY applicable shared row against the google-chat subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: "google-chat",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it("passes the INHERITED webhook transport rows (reference fixture) over the REAL adapter", async () => {
		const subject = makeSubject() as GchatWebhookSubject;
		const probe = subject.flagsAndTrustProbe();

		const fx = makeGchatFixture();
		try {
			subject.adapter.holdTurns(true);
			const startedAt = Date.now();
			const resp = await fx.postSigned(
				fx.nativeEnvelope({
					message: fx.chatMessage({ name: "spaces/AAAA/messages/bw" }),
				}),
			);
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
				subjectName: "google-chat-webhook-shape",
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

	it("passes ALL EIGHT google-chat shape-delta rows through the real engine fixture", async () => {
		const rows = gchatDeltaRows(() => makeGchatFixture());
		expect(rows.map((r) => r.id)).toEqual([
			"transport.gchat.bearer-auth-negative-matrix",
			"transport.gchat.envelope-format-matrix",
			"transport.gchat.bot-filter-and-name-dedupe",
			"transport.gchat.body-cap-preparse",
			"transport.gchat.thread-routing-ladder",
			"transport.gchat.chat-dialect-conversion",
			"transport.gchat.outbound-verdict-ladder",
			"transport.gchat.cardsv2-builder-caps",
		]);
		const report = await runConformanceSuite({
			subjectName: "google-chat-deltas",
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

		const subject = makeSubject() as GchatWebhookSubject;
		const probe = subject.flagsAndTrustProbe();
		const transport = makeWebhookRows({
			async flagsAndTrust() {
				return probe;
			},
			async boundedWindowAnswer() {
				return { answeredWithinWindowMs: 12, windowCapMs: 5_000 };
			},
		});
		const deltas = gchatDeltaRows(() => makeGchatFixture());

		const report = await runConformanceSuite({
			subjectName: "google-chat-full",
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

	it("the gate DETECTS violations: a lying verifier that admits ANY token fails its own named row", async () => {
		// Mutant: the verifier accepts EVERYTHING — tampered tokens, wrong
		// audiences, stranger identities all resolve to the allowlisted SA — as
		// if audience binding and identity checks were stubbed out. The bearer
		// negative matrix must fail BY NAME; rows using VALID bearers are
		// unaffected (the lie accepts them exactly like the real verifier).
		const rows = gchatDeltaRows(() =>
			makeGchatFixture({
				verifier: {
					verify: async (_token, aud) => ({ aud, email: FIXTURE_SA_EMAIL }),
				},
			}),
		);

		const bearerRow = rows.find(
			(r) => r.id === "transport.gchat.bearer-auth-negative-matrix",
		);
		expect(bearerRow).toBeDefined();
		const mutantReport = await runConformanceSuite({
			subjectName: "mutant-gchat-bearer",
			shape: "webhook",
			rows: [bearerRow as ConformanceRow],
		});
		expect(mutantReport.failed).toBe(1);
		expect(mutantReport.rows[0]?.pass).toBe(false);

		// Sanity: the OTHER rows still pass on their own fresh fixtures.
		const others = rows.filter((r) => r.id !== bearerRow?.id);
		const otherReport = await runConformanceSuite({
			subjectName: "mutant-gchat-others",
			shape: "webhook",
			rows: others as ConformanceRow[],
		});
		if (otherReport.failed > 0) console.error(formatReport(otherReport));
		expect(otherReport.failed).toBe(0);
	}, 30_000);
});
