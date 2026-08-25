// CONFORMANCE WIRING — the SMS (Twilio) census port vs the executable
// 04 §8 matrix (DEC-002 gate applies to every new platform).
//
//   1. ALL applicable SHARED rows pass for shape="webhook" against the REAL
//      kit-built SmsSubject. Applicability is COMPUTED from capability data
//      (04 §8 conditional headers): the streaming family applies only when
//      supportsDraftStreaming()/draft_stream_is_message hold — the Twilio REST
//      Messages API has no edit endpoint in the source, so those three rows
//      are excluded BY THE PROBE, never by a hardcoded skip.
//   2. The INHERITED webhook transport rows (reference-fixture inheritance,
//      roadmap §Phase 6 heuristic 2) run over the REAL adapter probes:
//      stateless flag pairing (manifest DIVERGENCE note) + DEC-017
//      trust-boundary completeness + bounded-window answer MEASURED by posting
//      a validly-signed webhook while a turn is HELD.
//   3. Fresh sms shape-delta rows execute through the real engine fixture
//      (signature matrix with default-port variant + insecure mode, body-cap
//      pre-parse gates, form/field ladder, connect refusal ladders,
//      strip_markdown ladder + 1600-char chunking, REST error mapping,
//      trust-boundary mutation checks).
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: a signature-gate-defeating mutant fails ITS OWN named
//      row; others stay green on fresh fixtures.
//   6. LIE-SCAN: flipping supportsDraftStreaming on a probe subject ADMITS the
//      streaming family — and seal reality fails it BY NAME.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { makeWebhookRows } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { makeSmsSubject, type SmsSubject } from "../sms/sms-subject.js";
import {
	makeSmsFixture,
	formEncode,
	type FixtureResponse,
	type SmsFixture,
} from "../sms/sms-fixture.js";
import {
	checkTwilioSignature,
	stripMarkdownForSms,
	twilioPortVariantUrl,
	verifyTwilioSignature,
	signTwilioParams,
} from "../sms/sms-adapter.js";
import {
	MAX_SMS_LENGTH,
	SMS_PLUGIN_MANIFEST,
	TWILIO_WEBHOOK_MAX_BODY_BYTES,
	declareSmsTrustBoundary,
	validateSmsTrustBoundary,
} from "../sms/manifest.js";
import { FIXTURE_FROM_NUMBER } from "../sms/fixture-secrets.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: {
		withSecret?: boolean | undefined;
		name?: string | undefined;
		declaredMessageEditing?: boolean | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeSmsSubject({
		wire: new FakePlatformWire(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
		...(opts.declaredMessageEditing !== undefined
			? { declaredMessageEditing: opts.declaredMessageEditing }
			: {}),
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

// ── sms shape-delta rows (executed over the REAL engine fixture) ────────────

function smsDeltaRows(newFixture: () => SmsFixture): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: SmsFixture) => Promise<void>,
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
			"transport.sms.signature-matrix",
			"sms: X-Twilio-Signature matrix — valid accepted; tampered param / wrong URL / missing header ⇒ 403; default-port VARIANT accepted both ways; non-standard ports have NO variant; insecure mode admits unsigned LOUDLY",
			async (fx) => {
				expect(fx.adapter.webhookUrl).toBe(
					"https://sms-fixture.example/webhooks/twilio",
				);

				// Pure-function level: the verifier recomputes EXACTLY what the
				// signer produced (sorted flat params over the configured URL).
				const params = {
					From: "+15557654321",
					To: FIXTURE_FROM_NUMBER,
					Body: "sig matrix",
					MessageSid: "SMsig0000000001",
				};
				const good = signTwilioParams(
					fx.adapter.authToken,
					fx.adapter.webhookUrl,
					params,
				);
				expect(
					verifyTwilioSignature({
						authToken: fx.adapter.authToken,
						url: fx.adapter.webhookUrl,
						params,
						signature: good,
					}),
				).toBe(true);
				expect(
					checkTwilioSignature(
						fx.adapter.authToken,
						fx.adapter.webhookUrl,
						{ ...params, Body: "tampered" },
						good,
					),
				).toBe(false);

				// Wire level: a validly-signed form is ACCEPTED.
				const ok = await fx.postSignedSms({ sid: "SMsig0000000002" });
				expect(ok.status).toBe(200);

				// Tampered param value ⇒ 403.
				const honestForm = fx.signedForm({ body: "original text" });
				const tamperedFields = {
					From: "+15557654321",
					To: FIXTURE_FROM_NUMBER,
					Body: "TAMPERED text",
					MessageSid: "SMfixture0000000001",
				};
				const tampered = await fx.postWebhook({
					headers: honestForm.headers,
					body: formEncode(tamperedFields),
				});
				expect(tampered.status).toBe(403);

				// Signed over the WRONG URL ⇒ 403.
				const wrongUrl = await fx.postWebhook(
					fx.signedForm({
						url: "https://attacker.example/webhooks/twilio",
					}),
				);
				expect(wrongUrl.status).toBe(403);

				// MISSING header ⇒ 403.
				const unsigned = fx.signedForm();
				const missingHeader = await fx.postWebhook({
					headers: {
						"content-type": "application/x-www-form-urlencoded",
					},
					body: unsigned.body,
				});
				expect(missingHeader.status).toBe(403);
				expect(fx.adapter.counters.missingSignatureRejected).toBe(1);
				expect(fx.adapter.counters.invalidSignatureRejected).toBe(2);

				// Default-port VARIANT: Twilio may sign EITHER form of the URL.
				// Plain configured URL + signature computed over explicit :443.
				const explicit443 = "https://sms-fixture.example:443/webhooks/twilio";
				const variantSigned = await fx.postSignedSms({
					url: explicit443,
					sid: "SMsig0000000003",
				});
				expect(variantSigned.status).toBe(200);
				// Reverse: configured URL carries explicit :443, signature plain.
				const portedFixture = makeSmsFixture({
					config: { webhook_url: explicit443 },
				});
				try {
					const reverseVariant = await portedFixture.postSignedSms({
						url: "https://sms-fixture.example/webhooks/twilio",
					});
					expect(reverseVariant.status).toBe(200);

					// Port-variant math is pure and toggles ONLY default ports.
					expect(twilioPortVariantUrl("https://x.example/p")).toBe(
						"https://x.example:443/p",
					);
					expect(twilioPortVariantUrl("http://x.example/p")).toBe(
						"http://x.example:80/p",
					);
					expect(twilioPortVariantUrl(explicit443)).toBe(
						"https://sms-fixture.example/webhooks/twilio",
					);
					expect(twilioPortVariantUrl("https://x.example:8443/p")).toBeNull();
					expect(twilioPortVariantUrl("ftp://x.example/p")).toBeNull();

					// Non-standard-port URLs have NO variant — neither direction.
					const oddFixture = makeSmsFixture({
						config: {
							webhook_url: "https://sms-fixture.example:8443/webhooks/twilio",
						},
					});
					try {
						const signedPlain = await oddFixture.postSignedSms({
							url: "https://sms-fixture.example/webhooks/twilio",
						});
						expect(signedPlain.status).toBe(403);
						const signed443 = await oddFixture.postSignedSms({
							url: explicit443,
						});
						expect(signed443.status).toBe(403);
					} finally {
						oddFixture.dispose();
					}
				} finally {
					portedFixture.dispose();
				}

				// Insecure-no-signature mode admits unsigned requests BUT logs the
				// DISABLED-validation warning at connect.
				const insecure = makeSmsFixture({
					config: { webhook_url: "", insecure_no_signature: true },
				});
				try {
					await expect(
						insecure.adapter.connect({ isReconnect: false }),
					).resolves.toBe(true);
					expect(
						insecure.logLines.some(
							(l) => l.level === "warn" && l.message.includes("DISABLED"),
						),
					).toBe(true);
					const bare = insecure.signedForm();
					const admitted = await insecure.postWebhook({
						headers: { "content-type": "application/x-www-form-urlencoded" },
						body: bare.body,
					});
					expect(admitted.status).toBe(200);
				} finally {
					insecure.dispose();
				}
			},
		),
		mk(
			"transport.sms.body-cap-preparse",
			"sms: >64 KiB bodies rejected 413 at BOTH gates (declared Content-Length AND actual bytes) without reaching the form-parse seam",
			async (fx) => {
				const cap = fx.adapter.maxBodyBytes;
				expect(cap).toBe(TWILIO_WEBHOOK_MAX_BODY_BYTES);
				expect(cap).toBe(65_536);

				// Gate 1: honest declared Content-Length over the cap.
				const big = Buffer.alloc(cap + 1, 0x61);
				const declared = await fx.postWebhook({
					headers: { "content-length": String(big.length) },
					body: Buffer.alloc(10, 0x62),
				});
				expect(declared.status).toBe(413);
				expect(fx.adapter.counters.parseInvocations).toBe(0);

				// Gate 2: LYING Content-Length trips on actual bytes post-read.
				const lying = await fx.postWebhook({
					headers: { "content-length": "10" },
					body: big,
				});
				expect(lying.status).toBe(413);
				expect(fx.adapter.counters.parseInvocations).toBe(0);
				expect(fx.adapter.counters.oversizedRejected).toBe(2);

				// Within-cap bodies parse exactly once.
				const fine = await fx.postSignedSms({ sid: "SMcap0000000001" });
				expect(fine.status).toBe(200);
				expect(fine.text).toBe(
					'<?xml version="1.0" encoding="UTF-8"?><Response></Response>',
				);
				expect(fx.adapter.counters.parseInvocations).toBe(1);
			},
		),
		mk(
			"transport.sms.form-and-field-ladder",
			"sms: garbage body ⇒ 400; blank From/Body ⇒ 200 empty TwiML no dispatch; own-number echo ignored; valid message dispatches exactly ONE event keyed chat_id/user_id=From text=Body; get_chat_info trivial dm",
			async (fx) => {
				// Garbage (invalid UTF-8) ⇒ UnicodeDecodeError parity ⇒ 400.
				const garbage = await fx.postWebhook({
					body: Buffer.from([0xff, 0xfe, 0x41, 0x42]),
				});
				expect(garbage.status).toBe(400);
				expect(garbage.text).toBe(
					'<?xml version="1.0" encoding="UTF-8"?><Response></Response>',
				);
				expect(fx.adapter.counters.parseErrors).toBe(1);

				// Blank From AND Body (validly signed, keep_blank_values parity):
				// 200 empty TwiML, NO dispatch.
				const blank = await fx.postSignedSms({ from: "", body: "" });
				expect(blank.status).toBe(200);
				expect(fx.adapter.counters.missingFieldsIgnored).toBe(1);
				expect(fx.adapter.counters.dispatched).toBe(0);

				// Own-number echo prevention: From === TWILIO_PHONE_NUMBER ⇒ 200,
				// ignored.
				const echo = await fx.postSignedSms({
					from: FIXTURE_FROM_NUMBER,
					sid: "SMecho0000000001",
				});
				expect(echo.status).toBe(200);
				expect(fx.adapter.counters.echoIgnored).toBe(1);
				expect(fx.adapter.counters.dispatched).toBe(0);

				// Valid message: exactly ONE event, chat_id/user_id = From,
				// text = Body — observed at the deliverInbound seam.
				const seen: Array<{
					chatId: string | undefined;
					userId: string | undefined;
					text: string | undefined;
				}> = [];
				const original = fx.adapter.deliverInbound.bind(fx.adapter);
				Object.defineProperty(fx.adapter, "deliverInbound", {
					value: async (event: IncomingEvent, sessionKey: string) => {
						seen.push({
							chatId: event.source?.chatId,
							userId: event.source?.userId,
							text: event.text,
						});
						return original(event, sessionKey);
					},
				});
				const valid = await fx.postSignedSms({
					from: "+15559997777",
					body: "the real message",
					sid: "SMvalid000000001",
				});
				expect(valid.status).toBe(200);
				await fx.drainInbound();
				expect(seen.length).toBe(1);
				expect(seen[0]?.chatId).toBe("+15559997777");
				expect(seen[0]?.userId).toBe("+15559997777");
				expect(seen[0]?.text).toBe("the real message");
				expect(fx.adapter.turnLog).toContain("the real message");
				expect(
					fx.adapter.dispatchedEvents.some(
						(e) => e.messageId === "SMvalid000000001",
					),
				).toBe(true);

				// get_chat_info trivial dm identity (source parity).
				expect(fx.adapter.getChatInfo("+15550009999")).toEqual({
					name: "+15550009999",
					type: "dm",
				});
			},
		),
		mk(
			"transport.sms.connect-refusal-ladders",
			"sms connect ladder: missing phone ⇒ FATAL sms_missing_phone_number; missing webhook URL without insecure ⇒ FATAL sms_missing_webhook_url; insecure-without-URL CONNECTS with the DISABLED-validation warning logged",
			async (_fx) => {
				const missingPhone = makeSmsFixture({
					config: { phone_number: "" },
				});
				try {
					await expect(
						missingPhone.adapter.connect({ isReconnect: false }),
					).resolves.toBe(false);
					const snap = missingPhone.adapter.lifecycle.statusSnapshot();
					expect(snap.state).toBe("fatal");
					expect(snap.detail).toContain("sms_missing_phone_number");
				} finally {
					missingPhone.dispose();
				}

				const missingUrl = makeSmsFixture({ config: { webhook_url: "" } });
				try {
					await expect(
						missingUrl.adapter.connect({ isReconnect: false }),
					).resolves.toBe(false);
					const snap = missingUrl.adapter.lifecycle.statusSnapshot();
					expect(snap.state).toBe("fatal");
					expect(snap.detail).toContain("sms_missing_webhook_url");
				} finally {
					missingUrl.dispose();
				}

				const insecureOk = makeSmsFixture({
					config: { webhook_url: "", insecure_no_signature: true },
				});
				try {
					await expect(
						insecureOk.adapter.connect({ isReconnect: false }),
					).resolves.toBe(true);
					expect(insecureOk.adapter.isConnected).toBe(true);
					expect(
						insecureOk.logLines.some(
							(l) =>
								l.level === "warn" &&
								l.message.includes("signature validation") &&
								l.message.includes("DISABLED"),
						),
					).toBe(true);
					// The health route answers plain-text "ok".
					const health = insecureOk.adapter.handleHealthGet();
					expect(health.status).toBe(200);
					expect(health.body).toBe("ok");
				} finally {
					insecureOk.dispose();
				}

				// Manifest shape sanity: webhook transport, 1600-char budget data.
				expect(SMS_PLUGIN_MANIFEST.transportShape).toBe("webhook");
				expect(MAX_SMS_LENGTH).toBe(1600);
			},
		),
		mk(
			"transport.sms.markdown-strip-chunking",
			"sms egress: strip_markdown ladder ports helpers.py EXACTLY (bold/italic star+under, fence markers, inline code, headings, links, newline collapse); oversized bodies split ≤1600-char chunks each POSTed with From/To preserved",
			async (fx) => {
				// The regex ladder, case by case (helpers.py:strip_markdown).
				expect(stripMarkdownForSms("**bold** tail")).toBe("bold tail");
				expect(stripMarkdownForSms("*italic* tail")).toBe("italic tail");
				expect(stripMarkdownForSms("__under-bold__ tail")).toBe(
					"under-bold tail",
				);
				expect(stripMarkdownForSms("_under-italic_ tail")).toBe(
					"under-italic tail",
				);
				// Fence MARKERS are removed; fenced content stays (source parity).
				expect(stripMarkdownForSms("before\n```py\ncode()\n```\nafter")).toBe(
					"before\ncode()\nafter",
				);
				expect(stripMarkdownForSms("`inline` tail")).toBe("inline tail");
				expect(stripMarkdownForSms("## Heading\nbody")).toBe("Heading\nbody");
				expect(stripMarkdownForSms("[text](https://example.com/x) tail")).toBe(
					"text tail",
				);
				expect(stripMarkdownForSms("a\n\n\n\n\nb")).toBe("a\n\nb");
				expect(stripMarkdownForSms("   padded   ")).toBe("padded");

				// Chunking: >1600-char content splits into ≤1600-char chunks, each
				// POSTed as its own Messages.json Body with From/To preserved.
				const filler = `${"lorem ipsum dolor sit amet "} `.repeat(120); // ~2900 chars
				const markdowny = `**urgent** ${filler}`;
				const result = await fx.adapter.send("+15557654321", markdowny);
				expect(result.success).toBe(true);
				const posts = fx.rest.posts;
				expect(posts.length).toBeGreaterThan(1);
				for (const post of posts) {
					expect(post.body.length).toBeLessThanOrEqual(MAX_SMS_LENGTH);
					expect(post.from).toBe(FIXTURE_FROM_NUMBER);
					expect(post.to).toBe("+15557654321");
					expect(post.status).toBe(201);
				}
				// strip_markdown applied at the transport boundary: the FIRST
				// posted body carries stripped text, never literal "**".
				expect(posts[0]?.body.startsWith("urgent")).toBe(true);
				// Every chunk reassembles to the full stripped payload (no loss).
				const rejoined = posts
					.map((p) => p.body.replace(/ \(\d+\/\d+\)$/, ""))
					.join("");
				expect(rejoined.includes("lorem ipsum dolor sit amet")).toBe(true);
			},
		),
		mk(
			"transport.sms.rest-error-mapping",
			"sms REST mapping: scripted 400 {message} ⇒ 'Twilio 400: <message>' CONSTRUCTED from the script; message-less bodies stringify; multi-chunk sends STOP at first failure; success carries the sid",
			async (fx) => {
				// Scripted vendor error → constructed expected string.
				const scriptedMessage = "The 'To' number is not a valid phone number";
				fx.scriptRest({ status: 400, json: { message: scriptedMessage } });
				const failed = await fx.adapter.send("+15551230000", "tiny");
				expect(failed.success).toBe(false);
				expect(failed.error).toBe(`Twilio 400: ${scriptedMessage}`);
				expect(fx.rest.posts.length).toBe(1);

				// Multi-chunk content stops at the FIRST failing chunk.
				const stopper = makeSmsFixture();
				try {
					stopper.scriptRest({ status: 400, json: { message: "boom" } });
					const big = "chunk filler ".repeat(300); // ~3900 chars ⇒ ≥2 chunks
					const stopped = await stopper.adapter.send("+15551230000", big);
					expect(stopped.success).toBe(false);
					expect(stopped.error).toBe("Twilio 400: boom");
					expect(stopper.rest.posts.length).toBe(1); // chunk 2 never POSTed
				} finally {
					stopper.dispose();
				}

				// Message-less error bodies stringify (body.get("message", str(body))
				// parity), still CONSTRUCTED from the script.
				const messageless = makeSmsFixture();
				try {
					messageless.scriptRest({ status: 503, json: { code: 20001 } });
					const mFailed = await messageless.adapter.send(
						"+15551230000",
						"tiny",
					);
					expect(mFailed.success).toBe(false);
					expect(mFailed.error).toBe(
						`Twilio 503: ${JSON.stringify({ code: 20001 })}`,
					);
				} finally {
					messageless.dispose();
				}

				// Success carries the scripted sid as messageId.
				const okFixture = makeSmsFixture();
				try {
					okFixture.scriptRest({
						status: 201,
						json: { sid: "SMexplicit12345" },
					});
					const sent = await okFixture.adapter.send("+15551230000", "tiny");
					expect(sent.success).toBe(true);
					expect(sent.messageId).toBe("SMexplicit12345");
				} finally {
					okFixture.dispose();
				}
			},
		),
		mk(
			"transport.sms.trust-boundary-complete",
			"sms DEC-017 boundary complete when declared; mutating constantTimeCompare / body cap / scheme datum / seen-set bound yields NAMED errors (mutation-checked)",
			async (fx) => {
				expect(validateSmsTrustBoundary(fx.adapter.trustBoundary)).toEqual([]);
				expect(SMS_PLUGIN_MANIFEST.trustBoundary?.signatureSchemes).toEqual([]);
				expect(SMS_PLUGIN_MANIFEST.capabilities.supportsAsyncDelivery).toBe(
					false,
				);
				expect(SMS_PLUGIN_MANIFEST.capabilities.interactiveResume).toBe(false);

				const base = declareSmsTrustBoundary();

				const noCtc = {
					...base,
					constantTimeCompare: false,
				} as unknown as typeof base;
				expect(validateSmsTrustBoundary(noCtc)).toContain(
					"trust boundary must declare constantTimeCompare: true",
				);

				const tinyCap = {
					...base,
					bodySizeCapBytes: 0,
				} as unknown as typeof base;
				const capErrors = validateSmsTrustBoundary(tinyCap);
				expect(capErrors.length).toBeGreaterThan(0);
				expect(capErrors.some((e) => e.includes("bodySizeCapBytes"))).toBe(
					true,
				);

				const noSchemeDatum: Record<string, unknown> = { ...base };
				delete noSchemeDatum["twilioSignatureHmacSha1"];
				const schemeErrors = validateSmsTrustBoundary(
					noSchemeDatum as unknown as typeof base,
				);
				expect(
					schemeErrors.some((e) => e.includes("twilioSignatureHmacSha1")),
				).toBe(true);

				const unbounded = { ...base } as Record<string, unknown>;
				unbounded["idempotency"] = undefined;
				const idemErrors = validateSmsTrustBoundary(
					unbounded as unknown as typeof base,
				);
				expect(idemErrors.some((e) => e.includes("idempotency"))).toBe(true);

				const unboundedWindow = {
					...base,
					backpressureWindow: "unbounded-lifecycle",
				} as unknown as typeof base;
				expect(
					validateSmsTrustBoundary(unboundedWindow).some((e) =>
						e.includes("backpressureWindow"),
					),
				).toBe(true);
			},
		),
	];
}

describe("conformance suite — sms census port (shape: webhook)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff passive)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // no edit API in the Twilio source (§3)
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("passes EVERY applicable shared row against the sms subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: "sms",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it("passes the INHERITED webhook transport rows (reference fixture) over the REAL adapter", async () => {
		const subject = makeSubject() as SmsSubject;
		const probe = subject.flagsAndTrustProbe();
		expect(probe.interactiveResumeFalse).toBe(true);
		expect(probe.supportsAsyncDeliveryFalse).toBe(true);
		expect(probe.trustBoundaryComplete).toBe(true);

		const fx = makeSmsFixture();
		try {
			// Bounded-window answer MEASURED: a validly-signed webhook posted
			// while a turn is HELD still answers FAST (non-blocking dispatch —
			// "Twilio expects a fast response").
			fx.adapter.holdTurns(true);
			const startedAt = Date.now();
			const resp: FixtureResponse = await fx.postSignedSms({
				from: "+15557654321",
				body: "bounded-window hello",
				sid: "SMwindow000000001",
			});
			const elapsed = Date.now() - startedAt;
			expect(resp.status).toBe(200);
			subject.adapter.holdTurns(false);
			fx.adapter.holdTurns(false);
			await fx.drainInbound();
			expect(fx.adapter.turnLog).toContain("bounded-window hello");

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
				subjectName: "sms-shape",
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

	it("passes ALL SEVEN sms shape-delta rows through the real engine fixture", async () => {
		const rows = smsDeltaRows(() => makeSmsFixture());
		expect(rows.map((r) => r.id)).toEqual([
			"transport.sms.signature-matrix",
			"transport.sms.body-cap-preparse",
			"transport.sms.form-and-field-ladder",
			"transport.sms.connect-refusal-ladders",
			"transport.sms.markdown-strip-chunking",
			"transport.sms.rest-error-mapping",
			"transport.sms.trust-boundary-complete",
		]);
		const report = await runConformanceSuite({
			subjectName: "sms-deltas",
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

		const subject = makeSubject() as SmsSubject;
		const probe = subject.flagsAndTrustProbe();
		const transport = makeWebhookRows({
			async flagsAndTrust() {
				return probe;
			},
			async boundedWindowAnswer() {
				return { answeredWithinWindowMs: 12, windowCapMs: 5_000 };
			},
		});
		const deltas = smsDeltaRows(() => makeSmsFixture());

		const report = await runConformanceSuite({
			subjectName: "sms-full",
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

	it("the gate DETECTS violations: a signature-gate-defeating mutant fails ITS OWN named row", async () => {
		// Mutant factory: the signature gate validates NOTHING (as if the
		// verifyTwilioSignature call were patched away). Every request reaches
		// the gate headerless; the honest engine would 403 them — so the
		// matrix row's negative legs (tampered/wrong-URL/missing-header ⇒ 403)
		// can never hold.
		const mutantFactory = (): SmsFixture => {
			const fx = makeSmsFixture();
			const original = fx.adapter.handleWebhookPost.bind(fx.adapter);
			Object.defineProperty(fx.adapter, "handleWebhookPost", {
				value: async (input: Parameters<typeof original>[0]) => {
					const headers = { ...(input.headers ?? {}) };
					delete headers["x-twilio-signature"]; // THE LIE
					return original({ ...input, headers });
				},
			});
			return fx;
		};
		const sigRow = smsDeltaRows(mutantFactory).find(
			(r) => r.id === "transport.sms.signature-matrix",
		);
		expect(sigRow).toBeDefined();
		const sigReport = await runConformanceSuite({
			subjectName: "mutant-sms-sig-gate",
			shape: "webhook",
			rows: [sigRow as ConformanceRow],
		});
		expect(sigReport.failed).toBe(1);
		expect(sigReport.rows[0]?.pass).toBe(false);

		// Sanity: the OTHER rows still pass on their OWN fresh HONEST fixtures.
		const others = smsDeltaRows(() => makeSmsFixture()).filter(
			(r) => r.id !== "transport.sms.signature-matrix",
		);
		const otherReport = await runConformanceSuite({
			subjectName: "mutant-sms-others",
			shape: "webhook",
			rows: others as ConformanceRow[],
		});
		if (otherReport.failed > 0) console.error(formatReport(otherReport));
		expect(otherReport.failed).toBe(0);
	}, 30_000);

	it("the gate DETECTS violations: a LYING capability datum fails the streaming family BY NAME", async () => {
		// Lie-scan mutant: flip THE probe datum that drives the streaming
		// exclusion (supportsDraftStreaming). Applicability then ADMITS the
		// streaming family — and seal reality catches the lie: the adapter has
		// NO native draft/seal machinery (no edit API in the Twilio source), so
		// streaming.seal-discipline can never observe its exactly-one-seal
		// invariant and FAILS by name (graceful degradation may let OTHER
		// family rows pass — the gate needs only ONE deterministic detector).
		const lyingApplicability = (): boolean =>
			makeSubject({
				declaredMessageEditing: true,
			}).adapter.supportsDraftStreaming() === true;
		expect(lyingApplicability()).toBe(true); // the lie FLIPS the probe…

		const all = buildSharedRows({
			makeSubject: (o) => makeSubject({ ...o, declaredMessageEditing: true }),
		});
		const streamingRows = all.filter((r) => STREAMING_ROW_IDS.includes(r.id));
		expect(streamingRows.length).toBe(3);
		const report = await runConformanceSuite({
			subjectName: "mutant-sms-streaming-lie",
			shape: "webhook",
			rows: streamingRows,
		});
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("streaming.seal-discipline");

		// …and the HONEST probe stays closed for every fresh subject.
		expect(computeApplicability().streamsSupported).toBe(false);

		// …and the HONEST deltas stay green (negatives never poison the catalog).
		const honestReport = await runConformanceSuite({
			subjectName: "honest-after-sms-mutant",
			shape: "webhook",
			rows: smsDeltaRows(() => makeSmsFixture()),
		});
		expect(honestReport.failed).toBe(0);
	}, 30_000);
});
