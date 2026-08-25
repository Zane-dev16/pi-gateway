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
	type SanitizerImpls,
} from "./mime-text.js";
import { FakeImapServer } from "./fake-mail-servers.js";

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
