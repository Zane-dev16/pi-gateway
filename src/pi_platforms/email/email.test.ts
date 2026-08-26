// pi_platforms/email/email.test — A19 EMAIL-CLASS SANITIZER contracts with
// FULL MUTATION COVERAGE plus the MIME/threading behavior contracts.
//
// Mutation discipline: every contract runs against the REAL implementation
// (must pass) AND against deliberate single-sanitizer mutants (MUST FAIL).

import { describe, expect, it } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

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
import { FakeImapServer, FakeSmtpServer } from "./fake-mail-servers.js";
import { EMAIL_IMAP_ID_ARGUMENT, EMAIL_PLUGIN_MANIFEST } from "./manifest.js";
import { EmailAdapter } from "./email-adapter.js";
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

// ══════════════════════════════════════════════════════════════════════
// Stability round-2 fix cluster email-r2 — contracts pinned to Hermes truth
// (plugins/platforms/email/adapter.py anchors cited per test).
// ══════════════════════════════════════════════════════════════════════

function adapterWithEnv(env: Record<string, string | undefined>): EmailAdapter {
	return new EmailAdapter({
		imap: new FakeImapServer(),
		smtp: new FakeSmtpServer(),
		secretReader: (key) => env[key],
	});
}

const BASE_ENV: Record<string, string | undefined> = {
	EMAIL_ADDRESS: "agent@fake.example",
	EMAIL_PASSWORD: "pw",
	EMAIL_IMAP_HOST: "imap.fake.example",
	EMAIL_SMTP_HOST: "smtp.fake.example",
};

describe("email-r2: smtp.quit after every lane + connect test (eml-1, adapter.py :771/:1194/:1320/:1398/:1466)", () => {
	it("connect test and every send tear the SMTP session down — zero leaked sessions", async () => {
		const w = makeEmailWorld({ name: "em-r2-quit" });
		await w.connectAndAwaitLive();
		// Connect-test finally already quit its probe session.
		expect(w.smtp.quitCalls).toBeGreaterThanOrEqual(1);
		expect(w.smtp.leakedSessions).toBe(0);

		await w.engine.sendEmail(ALICE, "one");
		await w.engine.sendEmail(ALICE, "two");
		expect(w.smtp.leakedSessions).toBe(0); // per-send teardown held
	});

	it("a raising quit is chased by close — delivery unaffected (finally parity)", async () => {
		const w = makeEmailWorld({ name: "em-r2-quit-fail" });
		await w.connectAndAwaitLive();
		w.smtp.quitFailuresArmed = 1;
		await w.engine.sendEmail(ALICE, "quit-hostile body");
		expect(w.smtp.sent[w.smtp.sent.length - 1]?.bodyText).toBe(
			"quit-hostile body",
		);
		expect(w.smtp.closeCalls).toBeGreaterThanOrEqual(1);
		expect(w.smtp.leakedSessions).toBe(0);
	});

	it("the connect-test quits even when login throws; typed auth death still classified", async () => {
		const w = makeEmailWorld({ name: "em-r2-quit-auth" });
		w.smtp.authBad = true;
		try {
			await w.connectAndAwaitLive();
			throw new Error("expected connect to fail");
		} catch (err) {
			expect(String((err as Error).message)).toContain("not live");
		}
		expect(w.subject.lifecycleSnapshot().detail).toContain("email_auth_error");
		expect(w.smtp.quitCalls).toBe(1); // finally ran despite the throw
		expect(w.smtp.leakedSessions).toBe(0);
	});
});

describe("email-r2: outbound document/image lanes (eml-2, adapter.py :1204-1386)", () => {
	it("sendDocument attaches ONE MIMEBase('application','octet-stream') base64 part preserving bytes", async () => {
		const w = makeEmailWorld({ name: "em-r2-doc" });
		await w.connectAndAwaitLive();
		const dir = mkdtempSync(join(tmpdir(), "email-r2-doc-"));
		try {
			const docPath = join(dir, "report.pdf");
			const bytes = Buffer.from("%PDF-1.4 attachment-bytes-éè");
			writeFileSync(docPath, bytes);
			const result = await w.engine.sendDocument("bob@example.com", docPath);
			expect(result.success).toBe(true);
			const sent = w.smtp.sent[w.smtp.sent.length - 1];
			expect(sent?.subject).toBe("Re: Hermes Agent");
			expect(sent?.attachments).toHaveLength(1);
			const att = sent?.attachments[0];
			expect(att?.contentType).toBe("application/octet-stream");
			expect(att?.filename).toBe("report.pdf");
			expect(
				Buffer.from(att?.payloadBase64 ?? "", "base64").equals(bytes),
			).toBe(true);
			expect(w.smtp.leakedSessions).toBe(0);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("sendMultipleImages batches local files into ONE mail; remote URLs link in the body; missing files skip", async () => {
		const w = makeEmailWorld({ name: "em-r2-multi" });
		await w.connectAndAwaitLive();
		const dir = mkdtempSync(join(tmpdir(), "email-r2-multi-"));
		try {
			const aPath = join(dir, "a.png");
			const bPath = join(dir, "b.png");
			const aBytes = Buffer.concat([
				Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a]),
				Buffer.from("aaa"),
			]);
			writeFileSync(aPath, aBytes);
			writeFileSync(bPath, Buffer.from("GIF89a-b-bytes"));

			const result = await w.engine.sendMultipleImages("bob@example.com", [
				`file://${aPath}`, // Hermes run.py wire format
				bPath, // pi post-stream bare-path format
				"https://cdn.example/cat.png", // remote → body link
				join(dir, "missing.png"), // missing → warn+skip
			]);
			expect(result[0]?.success).toBe(true);
			const sent = w.smtp.sent[w.smtp.sent.length - 1];
			expect(sent?.attachments).toHaveLength(2);
			expect(sent?.attachments.map((a) => a.filename)).toEqual([
				"a.png",
				"b.png",
			]);
			for (const att of sent?.attachments ?? []) {
				expect(att.contentType).toBe("application/octet-stream");
			}
			expect(
				Buffer.from(sent?.attachments[0]?.payloadBase64 ?? "", "base64").equals(
					aBytes,
				),
			).toBe(true);
			expect(sent?.bodyText).toBe("Image: https://cdn.example/cat.png");
			expect(sent?.to).toBe("bob@example.com");
			expect(w.smtp.leakedSessions).toBe(0);
		} finally {
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("sendDocument of an unreadable path fails gracefully with an error result", async () => {
		const w = makeEmailWorld({ name: "em-r2-doc-miss" });
		await w.connectAndAwaitLive();
		const result = await w.engine.sendDocument(
			"bob@example.com",
			"/nonexistent/email-r2/missing.bin",
		);
		expect(result.success).toBe(false);
		expect(result.error?.length ?? 0).toBeGreaterThan(0);
		expect(w.smtp.sent).toHaveLength(0); // nothing half-sent
	});

	it("sendImage links the image URL into a plain body (send_image parity)", async () => {
		const w = makeEmailWorld({ name: "em-r2-image" });
		await w.connectAndAwaitLive();
		await w.engine.sendImage(
			"bob@example.com",
			"https://cdn.example/x.png",
			"look at this",
		);
		const sent = w.wire.sendsOf("bob@example.com");
		expect(sent).toHaveLength(1); // door-1 lane (Hermes self.send parity)
		expect(sent[0]?.content).toContain("look at this");
		expect(sent[0]?.content).toContain("Image: https://cdn.example/x.png");
	});
});

describe("email-r2: skip_attachments operator opt-out (eml-3, adapter.py :565/:969)", () => {
	it("configured short-circuit drops attachment/inline parts from dispatch entirely", async () => {
		const w = makeEmailWorld({ name: "em-r2-skipatt", skipAttachments: true });
		await w.connectAndAwaitLive();
		const captured: IncomingEvent[] = [];
		const inner = w.engine.deliverInbound.bind(w.engine);
		w.engine.deliverInbound = async (event, key) => {
			captured.push(structuredClone(event));
			await inner(event, key);
		};
		w.imap.deliver({
			from: ALICE,
			textBody: "clean body",
			subject: "sa",
			attachments: [
				{
					filename: "evil.exe",
					contentType: "application/x-msdownload",
					payload: Buffer.from("MZ"),
				},
			],
		});
		await w.runCycles(1);
		await eventually(() =>
			w.subject.turns().some((t) => t.includes("clean body")),
		);
		const evt = captured[captured.length - 1];
		expect(evt?.messageType).toBe("text"); // no PHOTO promotion
		expect(evt?.mediaUrls).toBeUndefined();
		expect(evt?.mediaTypes).toBeUndefined();
	});

	it("default stays extract-everything when unset", async () => {
		const w = makeEmailWorld({ name: "em-r2-keepatt" });
		await w.connectAndAwaitLive();
		const captured: IncomingEvent[] = [];
		const inner = w.engine.deliverInbound.bind(w.engine);
		w.engine.deliverInbound = async (event, key) => {
			captured.push(structuredClone(event));
			await inner(event, key);
		};
		w.imap.deliver({
			from: ALICE,
			textBody: "with docs",
			subject: "ka",
			attachments: [
				{
					filename: "notes.txt",
					contentType: "text/plain",
					disposition: "attachment",
					payload: Buffer.from("hi"),
				},
			],
		});
		await w.runCycles(1);
		await eventually(() =>
			w.subject.turns().some((t) => t.includes("with docs")),
		);
		expect(captured[captured.length - 1]?.mediaUrls).toHaveLength(1);
	});
});

describe("email-r2: IMAP teardown outside the failing try (eml-4, adapter.py _close_imap :115/:744/:919)", () => {
	it("an armed logout abort cannot fail a fully-successful fetch or spur escalation", async () => {
		const w = makeEmailWorld({ name: "em-r2-logout" });
		await w.connectAndAwaitLive();
		w.imap.deliver({
			from: ALICE,
			textBody: "survives logout",
			subject: "lo1",
		});
		w.imap.logoutAborts = true; // EVERY logout raises IMAP4.abort
		await w.runCycles(1);
		await eventually(() =>
			w.subject.turns().some((t) => t.includes("survives logout")),
		);
		// Pre-fix: logout INSIDE the failing try marked this fetch failed and
		// escalated a reconnect. Vendor truth: partial success is success.
		expect(w.engine.escalationLog).toEqual([]);
		expect(w.subject.lifecycleSnapshot().state).not.toBe("fatal");
	});

	it("reconnect survives armed logout aborts too (_close_imap eats everything)", async () => {
		const w = makeEmailWorld({ name: "em-r2-logout-reconn" });
		await w.connectAndAwaitLive();
		w.engine.disconnect();
		w.imap.logoutAborts = true;
		expect(await w.engine.connect({ isReconnect: true })).toBe(true);
		expect(w.engine.isConnected).toBe(true);
	});
});

describe("email-r2: scripted Message-ID/In-Reply-To thread through (eml-5)", () => {
	it("parseFetchedMessage consumes ACTUAL headers for threading; replies cite them", async () => {
		const w = makeEmailWorld({ name: "em-r2-msgid" });
		await w.connectAndAwaitLive();
		w.imap.deliver({
			from: ALICE,
			textBody: "parent body",
			subject: "thread",
			headers: {
				"Message-ID": "<real-parent@mx.example>",
				"In-Reply-To": "<grandparent@mx.example>",
			},
		});
		await w.runCycles(1);
		await eventually(() =>
			w.subject.turns().some((t) => t.includes("parent body")),
		);
		await w.engine.sendEmail(ALICE, "threaded reply");
		const sent = w.smtp.sent[w.smtp.sent.length - 1];
		expect(sent?.headers["In-Reply-To"]).toBe("<real-parent@mx.example>");
		expect(sent?.headers["References"]).toBe("<real-parent@mx.example>");
	});

	it("synthesized <fake-{uid}> ID remains ONLY the fallback for header-less mail", async () => {
		const imap = new FakeImapServer();
		const uid = imap.deliver({
			from: ALICE,
			textBody: "anon",
			subject: "anon",
		});
		imap.login("u", "p");
		imap.selectInbox();
		const record = imap.uidFetchRfc822(uid);
		expect(record.messageId).toBe(`<fake-${uid}@mx.fake.example>`);
		expect(record.inReplyTo).toBe("");
	});
});

describe("email-r2: blank numeric env values are unset (eml-6, adapter.py _esecret_int :78-86)", () => {
	it("blank/whitespace ports and poll interval fall back to defaults — never port 0 or 0ms", () => {
		const blank = adapterWithEnv({
			...BASE_ENV,
			EMAIL_IMAP_PORT: "",
			EMAIL_SMTP_PORT: "   ",
			EMAIL_POLL_INTERVAL: "\t ",
		});
		expect(blank.imapPort).toBe(993);
		expect(blank.smtpPort).toBe(587);
		expect(blank.pollIntervalMs).toBe(15_000);
	});

	it("garbage values fall back; valid integers (trimmed) parse", () => {
		const garbage = adapterWithEnv({
			...BASE_ENV,
			EMAIL_IMAP_PORT: "abc",
			EMAIL_POLL_INTERVAL: "soon",
		});
		expect(garbage.imapPort).toBe(993);
		expect(garbage.pollIntervalMs).toBe(15_000);

		const valid = adapterWithEnv({
			...BASE_ENV,
			EMAIL_IMAP_PORT: "10993",
			EMAIL_SMTP_PORT: " 1465 ",
			EMAIL_POLL_INTERVAL: "30",
		});
		expect(valid.imapPort).toBe(10993);
		expect(valid.smtpPort).toBe(1465);
		expect(valid.pollIntervalMs).toBe(30_000);
	});
});

describe("email-r2: repeated Authentication-Results instances (eml-7, GHSA-rxqh get_all parity)", () => {
	it("StoredMail preserves duplicates in wire order; collapsed view keeps dict(msg.items()) semantics", async () => {
		const imap = new FakeImapServer();
		const uid = imap.deliver({
			from: ALICE,
			textBody: "ar dup",
			subject: "dup",
			headers: {
				"Authentication-Results": [
					"attacker.example; dmarc=pass header.from=example.com",
					"mx.fake.example; dmarc=fail policy.dmarc=reject",
				],
			},
		});
		imap.login("u", "p");
		imap.selectInbox();
		const record = imap.uidFetchRfc822(uid);
		const arInstances = record.headerList.filter(
			([k]) => k.toLowerCase() === "authentication-results",
		);
		expect(arInstances).toHaveLength(2); // BOTH survive — first-instance trust representable
		expect(arInstances[0]?.[1]).toContain("attacker.example");
		// Collapsed Record = Python dict(msg.items()): LAST instance wins.
		expect(record.headers["Authentication-Results"]).toContain(
			"mx.fake.example",
		);
	});

	it("unpinned verdict trusts the FIRST instance; authserv-id pin skips injected ones", async () => {
		// Unpinned: first-instance trust — the receiving server PREPENDS.
		const unpinned = makeEmailWorld({
			name: "em-r2-ar-first",
			requireAuthenticatedSender: true,
		});
		await unpinned.connectAndAwaitLive();
		unpinned.imap.deliver({
			from: ALICE,
			textBody: "first instance wins",
			subject: "ar-first",
			headers: {
				"Authentication-Results": [
					"attacker.example; dmarc=pass header.from=example.com",
					"mx.fake.example; dmarc=fail policy.dmarc=reject",
				],
			},
		});
		await unpinned.runCycles(1);
		await eventually(() =>
			unpinned.subject.turns().some((t) => t.includes("first instance wins")),
		);

		// Pinned to the operator's server: the attacker's first instance is
		// skipped and the genuine dmarc=fail decides — fail-closed.
		const pinned = makeEmailWorld({
			name: "em-r2-ar-pin",
			requireAuthenticatedSender: true,
			authservId: "mx.fake.example",
		});
		await pinned.connectAndAwaitLive();
		pinned.imap.deliver({
			from: ALICE,
			textBody: "pin rejects injected",
			subject: "ar-pin",
			headers: {
				"Authentication-Results": [
					"attacker.example; dmarc=pass header.from=example.com",
					"mx.fake.example; dmarc=fail policy.dmarc=reject",
				],
			},
		});
		await pinned.runCycles(1);
		await new Promise<void>((r) => setTimeout(r, 60));
		expect(
			pinned.subject.turns().some((t) => t.includes("pin rejects injected")),
		).toBe(false);

		// Same pin ACCEPTS when the genuine instance passes.
		pinned.imap.deliver({
			from: ALICE,
			textBody: "pin accepts genuine",
			subject: "ar-pass",
			headers: {
				"Authentication-Results": [
					"attacker.example; dmarc=pass header.from=evil.test",
					"mx.fake.example; dmarc=pass header.from=example.com",
				],
			},
		});
		await pinned.runCycles(1);
		await eventually(() =>
			pinned.subject.turns().some((t) => t.includes("pin accepts genuine")),
		);
	});
});

describe("email-r2: connect() rides the A21 ladder (eml-8, adapter.py _connect_smtp :630-648)", () => {
	it("v6-blackhole connect recovers onto IPv4 during the connection TEST itself", async () => {
		const w = makeEmailWorld({ name: "em-r2-connect-ladder" });
		w.smtp.resolverCandidates = [
			{ family: 6, host: "2001:db8::bad", reachable: false },
			{ family: 4, host: "192.0.2.10", reachable: true },
		];
		await w.connectAndAwaitLive(); // pre-fix: fatal email_smtp_connect_error
		expect(w.smtp.lastCandidateFamily).toBe(4);
		expect(w.smtp.leakedSessions).toBe(0);
	});

	it("exhausted resolution retries IPv4-only ONCE inside connect before classifying retryable", async () => {
		const w = makeEmailWorld({ name: "em-r2-connect-dead" });
		w.smtp.resolverCandidates = [
			{ family: 6, host: "2001:db8::bad", reachable: false },
		];
		const ok = await w.engine.connect({ isReconnect: false });
		expect(ok).toBe(false);
		expect(w.engine.fatalCodes[0]?.code).toBe("email_smtp_connect_error");
		expect(w.engine.fatalCodes[0]?.retryable).toBe(true);
		// TWO attempts = the ladder ran inside connect (single shot would be 1).
		expect(w.smtp.connectCalls).toBe(2);
		expect(w.smtp.leakedSessions).toBe(0);
	});
});

describe("email-r2: full-body SMTP send (eml-9, adapter.py _send_email MIMEText(body))", () => {
	it("no SMTP-lane slice — bodies beyond the 50000 metadata cap ship whole", async () => {
		const w = makeEmailWorld({ name: "em-r2-fullbody" });
		await w.connectAndAwaitLive();
		const big = "x".repeat(60_000);
		await w.engine.sendEmail(ALICE, big);
		expect(w.smtp.sent[w.smtp.sent.length - 1]?.bodyText).toBe(big);
	});

	it("the 50000 budget lives in deliverText policy chunking only", async () => {
		const w = makeEmailWorld({ name: "em-r2-policy-cap" });
		await w.connectAndAwaitLive();
		const results = await w.subject.deliverLongText(
			ALICE,
			"word ".repeat(12_500),
		);
		expect(results.length).toBeGreaterThan(1); // chunked upstream
		for (const r of results) expect(r.success).toBe(true);
		const joined = w.smtp.sent
			.slice(-results.length)
			.map((r) => r.bodyText)
			.join(" ");
		for (const word of ["alpha-marker-absent"]) {
			expect(joined.includes(word)).toBe(false);
		}
	});
});

describe("email-r2: EMAIL_AUTHSERV_ID secret fallback (eml-10, adapter.py :591-592)", () => {
	it("config extra wins over the scoped secret; secret strips+lowercases", () => {
		const viaEnv = adapterWithEnv({
			...BASE_ENV,
			EMAIL_AUTHSERV_ID: " MX.Fake.Example ",
		});
		expect(viaEnv.authservId).toBe("mx.fake.example");

		const bothSet = makeEmailWorld({
			name: "em-r2-authserv-both",
			authservId: "Config.Example",
			emailAuthservIdEnv: "env.example",
		});
		expect(bothSet.engine.authservId).toBe("config.example");

		const neither = makeEmailWorld({ name: "em-r2-authserv-none" });
		expect(neither.engine.authservId).toBe("");
	});

	it("an env-configured pin defends against an injected first A-R instance", async () => {
		const w = makeEmailWorld({
			name: "em-r2-authserv-gate",
			requireAuthenticatedSender: true,
			emailAuthservIdEnv: "mx.fake.example",
		});
		await w.connectAndAwaitLive();
		w.imap.deliver({
			from: ALICE,
			textBody: "env pin holds",
			subject: "env-pin",
			headers: {
				"Authentication-Results": [
					"attacker.example; dmarc=pass header.from=example.com",
					"mx.fake.example; dmarc=fail policy.dmarc=reject",
				],
			},
		});
		await w.runCycles(1);
		await new Promise<void>((r) => setTimeout(r, 60));
		expect(w.subject.turns().some((t) => t.includes("env pin holds"))).toBe(
			false,
		);
	});
});
