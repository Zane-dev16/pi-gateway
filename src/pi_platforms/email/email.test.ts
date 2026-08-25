// pi_platforms/email/email.test — A19 EMAIL-CLASS SANITIZER contracts with
// FULL MUTATION COVERAGE plus the MIME/threading behavior contracts.
//
// Mutation discipline: every contract runs against the REAL implementation
// (must pass) AND against deliberate single-sanitizer mutants (MUST FAIL).

import { describe, expect, it } from "vitest";

import {
	SANITIZER_CONTRACTS,
	realSanitizers,
	domainsAligned,
	safeDecodeBytes,
	extractAttachments,
	looksLikeImage,
	formatRfc2822Date,
	type SanitizerImpls,
} from "./mime-text.js";
import { FakeImapServer } from "./fake-mail-servers.js";
import { EMAIL_IMAP_ID_ARGUMENT, EMAIL_PLUGIN_MANIFEST } from "./manifest.js";
import { ALICE, makeEmailWorld } from "./email-world.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";

function runContract(name: string, impls: SanitizerImpls): string | null {
	const contract = SANITIZER_CONTRACTS.find((c) => c.name === name);
	if (contract === undefined) throw new Error(`unknown contract ${name}`);
	try {
		contract.run(impls);
		return null;
	} catch (err) {
		return err instanceof Error ? err.message : String(err);
	}
}

function mutant(over: Partial<SanitizerImpls>): SanitizerImpls {
	return { ...realSanitizers, ...over };
}

describe("A19 sanitizer contracts — real implementation", () => {
	for (const contract of SANITIZER_CONTRACTS) {
		it(`real impls pass contract "${contract.name}"`, () => {
			expect(runContract(contract.name, realSanitizers)).toBeNull();
		});
	}
});

describe("A19 sanitizer contracts — MUTANTS MUST FAIL their named contract", () => {
	it("mutant: charset ladder raises on unknown labels → charset-ladder-never-throws FAILS", () => {
		const violation = runContract(
			"charset-ladder-never-throws",
			mutant({
				safeDecodeBytes: (_payload: Buffer, charset?: string | null) => {
					throw new Error(`Unknown codec ${String(charset)}`);
				},
			}),
		);
		expect(violation).not.toBeNull();
	});

	it("mutant: B-words skipped → rfc2047-decodes-without-raising FAILS", () => {
		const violation = runContract(
			"rfc2047-decodes-without-raising",
			mutant({
				decodeHeaderValue: (raw: string) =>
					raw.includes("?B?") ? raw : realSanitizers.decodeHeaderValue(raw),
			}),
		);
		expect(violation).not.toBeNull();
		expect(violation).toContain("B-word decodes");
	});

	it("mutant: script tags kept → html-stripped-naive-and-safe FAILS", () => {
		const violation = runContract(
			"html-stripped-naive-and-safe",
			mutant({
				stripHtml: (html: string) =>
					html.replace(/<p[^>]*>/gi, "").replace(/<\/p>/gi, ""),
			}),
		);
		expect(violation).not.toBeNull();
		expect(violation).toContain("no tags survive");
	});

	it("mutant: html preferred over plain → plain-preferred-html-fallback FAILS", () => {
		const violation = runContract(
			"plain-preferred-html-fallback",
			mutant({
				extractTextBody: (parts) => {
					for (const part of parts) {
						if (
							part.contentType === "text/html" &&
							part.payload !== null &&
							!(part.disposition ?? "").includes("attachment")
						) {
							return realSanitizers.stripHtml(
								realSanitizers.safeDecodeBytes(part.payload, part.charset),
							);
						}
					}
					return realSanitizers.extractTextBody(parts);
				},
			}),
		);
		expect(violation).not.toBeNull();
		expect(violation).toContain("plain wins");
	});

	it("mutant: allowlist pattern inverted → automated-sender-gates FAILS", () => {
		const violation = runContract(
			"automated-sender-gates",
			mutant({
				isAutomatedSender: (
					address: string,
					headers: Record<string, string>,
					patterns?: readonly string[],
					autoHeaders?: Readonly<Record<string, (v: string) => boolean>>,
				) =>
					!realSanitizers.isAutomatedSender(
						address,
						headers,
						patterns,
						autoHeaders,
					),
			}),
		);
		expect(violation).not.toBeNull();
	});

	it("mutant: no-header returns TRUE (fail-open) → sender-authentication FAILS", () => {
		const violation = runContract(
			"sender-authentication-fail-closed",
			mutant({
				verifySenderAuthentication: (
					headers: readonly string[],
					from: string,
					authservId = "",
				) => {
					const verdict = realSanitizers.verifySenderAuthentication(
						headers,
						from,
						authservId,
					);
					return headers.length === 0
						? { authenticated: true, reason: "trust-on-absence" }
						: verdict;
				},
			}),
		);
		expect(violation).not.toBeNull();
	});
});

describe("alignment + decode helpers", () => {
	it("organizational alignment matches relaxed DMARC semantics", () => {
		expect(domainsAligned("mail.example.com", "example.com")).toBe(true);
		expect(domainsAligned("example.com", "example.com")).toBe(true);
		expect(domainsAligned("evilexample.com", "example.com")).toBe(false);
		expect(domainsAligned("", "example.com")).toBe(false);
	});

	it("latin-1 fallback never throws on hostile bytes", () => {
		const bytes = Buffer.from([0xff, 0xfe, 0x00, 0x81]);
		expect(safeDecodeBytes(bytes, "gbk").length).toBeGreaterThan(0);
	});
});

describe("fake IMAP server discipline", () => {
	it("\\Seen flags are server-side and set by RFC822 fetch only", () => {
		const imap = new FakeImapServer();
		const uid = imap.deliver({ from: "a@b.c", textBody: "hi" });
		imap.login("u", "p");
		expect(imap.uidSearch("UNSEEN")).toEqual([uid]);
		imap.selectInbox();
		imap.uidFetchRfc822(uid);
		expect(imap.uidSearch("UNSEEN")).toEqual([]);
		expect(imap.uidSearch("ALL")).toEqual([uid]);
	});

	it("scripted fetch refusals do NOT mark seen", () => {
		const imap = new FakeImapServer();
		const uid = imap.deliver({ from: "a@b.c", textBody: "hi" });
		imap.fetchRefusals.add(uid);
		imap.login("u", "p");
		imap.selectInbox();
		expect(() => imap.uidFetchRfc822(uid)).toThrow(/refused/);
		expect(imap.uidSearch("UNSEEN")).toEqual([uid]);
	});
});

// ══════════════════════════════════════════════════════════════════════
// Conforming-behavior contracts (vendor truth: plugins/platforms/email/
// adapter.py _send_imap_id · _extract_attachments · _dispatch_message ·
// _send_email). Each assertion pins a HERMES behavior, not an implementation
// accident.
// ══════════════════════════════════════════════════════════════════════

/** Independent copy of the vendor identity f-string (adapter.py:_send_imap_id
 * with hermes_cli.__version__ = "0.20.5") — NOT imported from the impl so a
 * regression in the shipped constant fails this contract. */
const VENDOR_IMAP_ID =
	'("name" "hermes-agent" "version" "0.20.5" ' +
	'"vendor" "NousResearch" ' +
	'"support-email" "noreply@nousresearch.com")';

async function eventually(
	predicate: () => boolean,
	timeoutMs = 4_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (predicate()) return;
		if (Date.now() > deadline)
			throw new Error("eventually: condition not met (email.test)");
		await new Promise<void>((r) => setTimeout(r, 2));
	}
}

describe("RFC 2971 IMAP ID after EVERY login (adapter.py:_send_imap_id)", () => {
	it("connect + each fetch cycle issue the byte-identical vendor identity", async () => {
		const w = makeEmailWorld({ name: "em-id" });
		await w.connectAndAwaitLive();
		expect(w.imap.idCommands).toEqual([VENDOR_IMAP_ID]);
		expect(EMAIL_IMAP_ID_ARGUMENT).toBe(VENDOR_IMAP_ID);

		w.imap.deliver({ from: ALICE, textBody: "id body", subject: "i1" });
		await w.runCycles(1);
		// A SECOND login (the poll cycle) carries the SAME identity string.
		expect(w.imap.idCommands).toEqual([VENDOR_IMAP_ID, VENDOR_IMAP_ID]);
	});

	it("ID rejection is swallowed — non-supporting servers keep working", async () => {
		const w = makeEmailWorld({ name: "em-id-reject" });
		w.imap.idRejectionArmed = true;
		await w.connectAndAwaitLive(); // best-effort: connect must NOT fail
		expect(w.imap.idCommands).toEqual([]);
		w.imap.deliver({
			from: ALICE,
			textBody: "still works",
			subject: "i2",
		});
		await w.runCycles(1);
		await eventually(() =>
			w.subject.turns().some((t) => t.includes("still works")),
		);
	});
});

describe("outbound MIME headers (adapter.py:_send_email)", () => {
	it("stamps an RFC 2822 local-time Date pinned by the clock seam", async () => {
		const w = makeEmailWorld({ name: "em-date" });
		await w.connectAndAwaitLive();
		const fixedMs = Date.UTC(2025, 0, 15, 12, 30, 45);
		w.clock.nowMs = () => fixedMs;
		await w.engine.sendEmail("bob@example.com", "dated body");
		const sent = w.smtp.sent[w.smtp.sent.length - 1];
		if (!sent) throw new Error("no mail captured");
		const dateHeader = sent.headers["Date"];
		if (dateHeader === undefined) throw new Error("Date header missing");
		expect(dateHeader).toMatch(
			/^[A-Za-z]{3}, \d{2} [A-Za-z]{3} \d{4} \d{2}:\d{2}:\d{2} [+-]\d{4}$/u,
		);
		// Round-trips to EXACTLY the pinned instant regardless of local zone.
		expect(new Date(dateHeader).getTime()).toBe(fixedMs);
	});

	it('default outbound subject is the vendor string "Re: Hermes Agent"', async () => {
		const w = makeEmailWorld({ name: "em-subject" });
		await w.connectAndAwaitLive();
		await w.engine.sendEmail("bob@example.com", "no thread context");
		const sent = w.smtp.sent[w.smtp.sent.length - 1];
		expect(sent?.subject).toBe("Re: Hermes Agent");

		// Thread-context replies keep the original subject (Re:-prefixed once).
		w.imap.deliver({
			from: ALICE,
			textBody: "quarterly numbers",
			subject: "Quarterly report",
		});
		await w.runCycles(1);
		await w.engine.sendEmail(ALICE, "replying");
		const reply = w.smtp.sent[w.smtp.sent.length - 1];
		expect(reply?.subject).toBe("Re: Quarterly report");
	});
});

describe("inbound attachments → media (adapter.py:_extract_attachments + _dispatch_message)", () => {
	it("mixed image+document mail surfaces mediaUrls/mediaTypes with DOCUMENT-wins typing; non-image .png bytes are SKIPPED", async () => {
		const w = makeEmailWorld({ name: "em-media" });
		await w.connectAndAwaitLive();
		const captured: IncomingEvent[] = [];
		const inner = w.engine.deliverInbound.bind(w.engine);
		w.engine.deliverInbound = async (event, key) => {
			captured.push(structuredClone(event));
			await inner(event, key);
		};

		const uid = w.imap.deliver({
			from: ALICE,
			textBody: "see attached",
			subject: "media",
			attachments: [
				{
					filename: "photo.png",
					contentType: "image/png",
					payload: Buffer.concat([
						Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
						Buffer.from("rest-of-png"),
					]),
				},
				{
					filename: "report.pdf",
					contentType: "application/pdf",
					payload: Buffer.from("%PDF-1.4"),
				},
				// Image EXTENSION but garbage bytes → magic check skips it.
				{
					filename: "fake.png",
					contentType: "image/png",
					payload: Buffer.from("<html>definitely not an image</html>"),
				},
			],
		});
		await w.runCycles(1);
		await eventually(() =>
			w.subject.turns().some((t) => t.includes("see attached")),
		);

		const evt = captured[captured.length - 1];
		if (!evt) throw new Error("no inbound event captured");
		expect(evt.messageType).toBe("document"); // document WINS over photo
		expect(evt.mediaUrls).toEqual([
			`/tmp/email-media/${uid}/img_00.png`,
			`/tmp/email-media/${uid}/doc_01_report.pdf`,
		]);
		expect(evt.mediaTypes).toEqual(["image/png", "application/pdf"]);
	});

	it("image-only mail promotes to PHOTO; attachment-free mail stays TEXT", async () => {
		const w = makeEmailWorld({ name: "em-photo" });
		await w.connectAndAwaitLive();
		const captured: IncomingEvent[] = [];
		const inner = w.engine.deliverInbound.bind(w.engine);
		w.engine.deliverInbound = async (event, key) => {
			captured.push(structuredClone(event));
			await inner(event, key);
		};

		const photoUid = w.imap.deliver({
			from: ALICE,
			textBody: "a picture",
			subject: "pic",
			attachments: [
				{
					filename: "chart.gif",
					contentType: "image/gif",
					payload: Buffer.from("GIF89a\x01\x02\x03"),
				},
			],
		});
		await w.runCycles(1);
		const picEvt = captured.find((e) => e.text?.includes("a picture"));
		if (!picEvt) throw new Error("photo event not captured");
		expect(picEvt.messageType).toBe("photo");
		expect(picEvt.mediaUrls).toEqual([
			`/tmp/email-media/${photoUid}/img_00.gif`,
		]);
		expect(picEvt.mediaTypes).toEqual(["image/gif"]);

		const plainUid = w.imap.deliver({
			from: ALICE,
			textBody: "just words",
			subject: "txt",
		});
		void plainUid;
		await w.runCycles(1);
		const txtEvt = captured.find((e) => e.text?.includes("just words"));
		if (!txtEvt) throw new Error("text event not captured");
		expect(txtEvt.messageType).toBe("text");
		expect(txtEvt.mediaUrls).toBeUndefined();
		expect(txtEvt.mediaTypes).toBeUndefined();
	});
});

describe("GATEWAY_ALLOW_ALL_USERS open-access opt-in (adapter.py:_dispatch_message)", () => {
	it("admits strangers when EMAIL_ALLOWED_USERS is unset and GATEWAY_ALLOW_ALL_USERS is truthy", async () => {
		const gw = makeEmailWorld({
			name: "em-gw-open",
			gatewayAllowAllUsers: true,
		});
		await gw.connectAndAwaitLive();
		gw.imap.deliver({
			from: "stranger@nowhere.test",
			textBody: "gw open body",
			subject: "g1",
		});
		await gw.runCycles(1);
		await eventually(() =>
			gw.subject.turns().some((t) => t.includes("gw open body")),
		);
	});

	it("default-deny holds when the allowlist is unset and NEITHER opt-in is set", async () => {
		const closed = makeEmailWorld({
			name: "em-gw-closed",
			unsetAllowedUsers: true,
		});
		await closed.connectAndAwaitLive();
		closed.imap.deliver({
			from: "stranger@nowhere.test",
			textBody: "denied body",
			subject: "g2",
		});
		await closed.runCycles(1);
		await new Promise<void>((r) => setTimeout(r, 60));
		expect(closed.subject.turns().some((t) => t.includes("denied body"))).toBe(
			false,
		);
	});

	it("the manifest DECLARES GATEWAY_ALLOW_ALL_USERS as optional env", () => {
		expect(
			(EMAIL_PLUGIN_MANIFEST.optionalEnv ?? []).some(
				(entry) => entry.name === "GATEWAY_ALLOW_ALL_USERS",
			),
		).toBe(true);
	});
});

describe("attachment extraction primitives (adapter.py:_extract_attachments)", () => {
	it("magic-byte vectors match base.py:_looks_like_image exactly", () => {
		const png = Buffer.from([
			0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a, 1, 2,
		]);
		expect(looksLikeImage(png)).toBe(true);
		expect(looksLikeImage(Buffer.from([0xff, 0xd8, 0xff, 0xe0]))).toBe(true); // JPEG
		expect(looksLikeImage(Buffer.from("GIF87a\x01"))).toBe(true);
		expect(looksLikeImage(Buffer.from("GIF89a\x01"))).toBe(true);
		expect(looksLikeImage(Buffer.from("BMxx"))).toBe(true); // BMP
		expect(
			looksLikeImage(
				Buffer.concat([
					Buffer.from("RIFF"),
					Buffer.alloc(4, 0),
					Buffer.from("WEBP"),
				]),
			),
		).toBe(true);
		expect(looksLikeImage(Buffer.from("nope"))).toBe(false);
		expect(looksLikeImage(Buffer.from([1, 2, 3]))).toBe(false); // len < 4
	});

	it("inline text bodies stay excluded; document names are path-sanitized; unnamed parts derive attachment.<subtype>", () => {
		const atts = extractAttachments("9", [
			{
				contentType: "text/plain",
				disposition: null,
				payload: Buffer.from("body"),
			},
			{
				contentType: "text/html",
				disposition: "inline",
				payload: Buffer.from("<p>signature block</p>"),
				filename: "sig.html",
			},
			{
				contentType: "application/pdf",
				disposition: "attachment",
				payload: Buffer.from("%PDF"),
				filename: "../../evil.pdf",
			},
			{
				contentType: "application/octet-stream",
				disposition: "attachment",
				payload: Buffer.from([0]),
			},
		]);
		expect(atts).toHaveLength(2);
		expect(atts[0]).toMatchObject({
			kind: "document",
			mediaType: "application/pdf",
			filename: "../../evil.pdf",
		});
		expect(atts[0]?.path.endsWith("_evil.pdf")).toBe(true); // traversal stripped
		expect(atts[1]?.filename).toBe("attachment.octet-stream");
	});

	it("formatRfc2822Date renders formatdate(localtime=True) shape and round-trips", () => {
		const fixedMs = Date.UTC(2025, 10, 5, 6, 7, 8);
		const rendered = formatRfc2822Date(fixedMs);
		expect(rendered).toMatch(/^Wed, 05 Nov 2025 06:07:08 [+-]\d{4}$/u);
		expect(new Date(rendered).getTime()).toBe(fixedMs);
	});
});
