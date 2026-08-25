// pi_platforms/email/mime-text — A19 EMAIL-CLASS SANITIZERS + MIME text
// plumbing (gap-audit A19 ride-along; roadmap Phase 6 heuristic 3).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/email/adapter.py::_safe_decode        (charset ladder)
//   plugins/platforms/email/adapter.py::_decode_header_value (RFC 2047)
//   plugins/platforms/email/adapter.py::_strip_html          (naive fallback)
//   plugins/platforms/email/adapter.py::_extract_text_body   (plain preferred)
//   plugins/platforms/email/adapter.py::_is_automated_sender (noreply gates)
//   plugins/platforms/email/adapter.py::_send_email          (MIMEText plain
//     ONLY outbound; MIMEMultipart carries text/plain parts exclusively)
//
// Every sanitizer ships with a CONTRACT RUNNER (SANITIZER_CONTRACTS); the test
// suite executes each against the real impl AND deliberate mutants — a mutant
// MUST fail its named contract.

import {
	EMAIL_AUTOMATED_HEADERS,
	EMAIL_CHARSET_ALIASES,
	EMAIL_NOREPLY_PATTERNS,
} from "./manifest.js";

/**
 * adapter.py:_safe_decode — decode bytes without EVER raising: alias table →
 * requested label → utf-8 → latin-1 (which cannot fail). Unknown or
 * attacker-controlled charset labels previously aborted whole fetch batches.
 */
export function safeDecodeBytes(
	payload: Buffer,
	charset?: string | null,
): string {
	let label = (charset ?? "utf-8")
		.trim()
		.replace(/^["']|["']$/g, "")
		.toLowerCase();
	if (label.length === 0) label = "utf-8";
	label = EMAIL_CHARSET_ALIASES[label] ?? label;
	for (const candidate of [label, "utf-8"]) {
		try {
			return new TextDecoder(candidate).decode(payload);
		} catch {}
	}
	return new TextDecoder("latin1").decode(payload);
}

/**
 * adapter.py:_decode_header_value — RFC 2047 encoded-word decoding that never
 * raises. Supports =?charset?B|Q?<data>?= words; malformed structure degrades
 * to the raw string, unknown charsets to replacement characters.
 */
export function decodeHeaderValue(raw: string): string {
	const encodedWord = /=\?([^?]+)\?([bBqQ])\?([^?]*)\?=/;
	if (!encodedWord.test(raw)) return raw;
	const out: string[] = [];
	let lastIndex = 0;
	for (const match of raw.matchAll(new RegExp(encodedWord.source, "gi"))) {
		const idx = match.index ?? 0;
		if (idx > lastIndex) out.push(raw.slice(lastIndex, idx));
		const charset = match[1] ?? "utf-8";
		const encoding = (match[2] ?? "b").toUpperCase();
		const data = match[3] ?? "";
		try {
			const bytes = Buffer.from(data, "base64");
			// Q-encoding: =XX hex escapes decoded manually over the binary copy.
			const decodedQ =
				encoding === "Q"
					? Buffer.from(
							data
								.replace(/_/g, " ")
								.replace(/=([0-9A-Fa-f]{2})/g, (_m, h) =>
									String.fromCharCode(parseInt(h, 16)),
								),
							"binary",
						)
					: bytes;
			out.push(safeDecodeBytes(decodedQ, charset));
		} catch {
			out.push(match[0] ?? "");
		}
		lastIndex = idx + match[0].length;
	}
	if (lastIndex < raw.length) out.push(raw.slice(lastIndex));
	return out.join("");
}

/**
 * adapter.py:_strip_html — NAIVE fallback stripper: br/p to newlines, tags
 * dropped, entity map, blank-line collapse. Security-adjacent: this is the
 * ONLY html handling — no scripts are ever interpreted.
 */
export function stripHtml(html: string): string {
	let text = html.replace(/<br\s*\/?>/gi, "\n");
	text = text.replace(/<p[^>]*>/gi, "\n");
	text = text.replace(/<\/p>/gi, "\n");
	text = text.replace(/<[^>]+>/g, "");
	text = text.replace(/&nbsp;/g, " ");
	text = text.replace(/&amp;/g, "&");
	text = text.replace(/&lt;/g, "<");
	text = text.replace(/&gt;/g, ">");
	text = text.replace(/\n{3,}/g, "\n\n");
	return text.trim();
}

/** adapter.py:_extract_text_body — multipart walk preferring text/plain. */
export function extractTextBody(
	parts: Array<{
		contentType: string;
		disposition: string | null;
		payload: Buffer | null;
		charset?: string | null;
	}>,
): string {
	if (parts.length === 0) return "";
	for (const part of parts) {
		const isAttachment = (part.disposition ?? "").includes("attachment");
		if (isAttachment) continue;
		if (
			part.contentType.toLowerCase() === "text/plain" &&
			part.payload !== null
		) {
			return safeDecodeBytes(part.payload, part.charset);
		}
	}
	for (const part of parts) {
		const isAttachment = (part.disposition ?? "").includes("attachment");
		if (isAttachment) continue;
		if (
			part.contentType.toLowerCase() === "text/html" &&
			part.payload !== null
		) {
			return stripHtml(safeDecodeBytes(part.payload, part.charset));
		}
	}
	return "";
}

/** adapter.py:_extract_email_address — 'Name <addr>' → bare lowercased addr. */
export function extractEmailAddress(raw: string): string {
	const match = /<([^>]+)>/.exec(raw);
	if (match !== null) return (match[1] ?? "").trim().toLowerCase();
	return raw.trim().toLowerCase();
}

/** adapter.py:_is_automated_sender — address patterns then header predicates. */
export function isAutomatedSender(
	address: string,
	headers: Record<string, string>,
	patterns: readonly string[] = EMAIL_NOREPLY_PATTERNS,
	automatedHeaders: Readonly<
		Record<string, (v: string) => boolean>
	> = EMAIL_AUTOMATED_HEADERS,
): boolean {
	const addr = address.toLowerCase();
	for (const pattern of patterns) {
		if (addr.includes(pattern)) return true;
	}
	for (const [header, checkValue] of Object.entries(automatedHeaders)) {
		const value = headers[header];
		if (value !== undefined && value.length > 0 && checkValue(value))
			return true;
	}
	return false;
}

/**
 * adapter.py:_verify_sender_authentication — parse Authentication-Results.
 * Trust order: FIRST header wins (receiving server PREPENDS its verdict);
 * optional authserv-id pin. dmarc=pass ⇒ authenticated; spf=pass requires
 * envelope alignment; dkim=pass requires header.d alignment. Organizational
 * alignment = exact match OR dot-suffix relationship (_domains_aligned).
 */
export function verifySenderAuthentication(
	authResultsHeaders: readonly string[],
	fromAddress: string,
	authservId = "",
): { authenticated: boolean; reason: string } {
	const fromDomain = domainOf(fromAddress);
	if (fromDomain.length === 0)
		return { authenticated: false, reason: "missing From domain" };
	if (authResultsHeaders.length === 0) {
		return { authenticated: false, reason: "no Authentication-Results header" };
	}
	let trusted: string | undefined;
	for (const rawHeader of authResultsHeaders) {
		const value = rawHeader.split(/\s+/).join(" ");
		if (authservId.length > 0) {
			const servId = (value.split(";")[0] ?? "").trim().toLowerCase();
			if (
				!domainsAligned(servId, authservId) &&
				servId !== authservId.toLowerCase()
			) {
				continue;
			}
		}
		trusted = value;
		break;
	}
	if (trusted === undefined) {
		return {
			authenticated: false,
			reason: "no Authentication-Results from trusted authserv-id",
		};
	}

	const methods = new Map<string, string>();
	for (const m of trusted.matchAll(/\b(dmarc|dkim|spf)\s*=\s*([a-z]+)/gi)) {
		methods.set((m[1] ?? "").toLowerCase(), (m[2] ?? "").toLowerCase());
	}
	const props = new Map<string, string>();
	for (const m of trusted.matchAll(
		/\b(header\.from|header\.d|smtp\.mailfrom|smtp\.from|envelope-from)\s*=\s*([^\s;]+)/gi,
	)) {
		props.set((m[1] ?? "").toLowerCase(), (m[2] ?? "").replace(/^"|"$/g, ""));
	}

	const dmarcResult = methods.get("dmarc");
	if (dmarcResult === "pass")
		return { authenticated: true, reason: "dmarc=pass" };

	const spfResult = methods.get("spf");
	if (spfResult === "pass") {
		let spfDomain =
			props.get("smtp.mailfrom") ??
			props.get("smtp.from") ??
			props.get("envelope-from") ??
			"";
		spfDomain = spfDomain.includes("@") ? domainOf(spfDomain) : spfDomain;
		if (domainsAligned(spfDomain, fromDomain)) {
			return { authenticated: true, reason: "spf=pass aligned" };
		}
	}
	const dkimResult = methods.get("dkim");
	if (dkimResult === "pass") {
		const dkimDomain =
			props.get("header.d") ?? domainOf(props.get("header.from") ?? "");
		if (domainsAligned(dkimDomain, fromDomain)) {
			return { authenticated: true, reason: "dkim=pass aligned" };
		}
	}
	return {
		authenticated: false,
		reason: `authentication failed (${trusted.slice(0, 120)})`,
	};
}

/** adapter.py:_domain_of — lowercased domain part, or ''. */
export function domainOf(address: string): string {
	const at = address.lastIndexOf("@");
	if (at < 0) return "";
	return address
		.slice(at + 1)
		.trim()
		.toLowerCase();
}

/** adapter.py:_domains_aligned — relaxed DMARC organizational alignment. */
export function domainsAligned(a: string, b: string): boolean {
	const x = (a ?? "").trim().toLowerCase().replace(/\.+$/u, "");
	const y = (b ?? "").trim().toLowerCase().replace(/\.+$/u, "");
	if (x.length === 0 || y.length === 0) return false;
	if (x === y) return true;
	return x.endsWith(`.${y}`) || y.endsWith(`.${x}`);
}

// ── A19 contract runners ────────────────────────────────────────────────────

export type SanitizerContract = {
	name: string;
	invariant: string;
	run: (impl: typeof realSanitizers) => void;
};

export const realSanitizers = {
	safeDecodeBytes,
	decodeHeaderValue,
	stripHtml,
	extractTextBody,
	extractEmailAddress,
	isAutomatedSender,
	verifySenderAuthentication,
};

export type SanitizerImpls = typeof realSanitizers;

function assert(condition: unknown, message: string): asserts condition {
	if (!condition) throw new Error(message);
}

export const SANITIZER_CONTRACTS: readonly SanitizerContract[] = [
	{
		name: "charset-ladder-never-throws",
		invariant:
			"A19: payload decoding NEVER raises — unknown/hostile charsets fall through alias → utf-8 → latin-1",
		run: (impl) => {
			const bytes = Buffer.from("café", "utf8");
			assert(
				impl.safeDecodeBytes(bytes, "utf-8").includes("café"),
				"utf-8 decodes",
			);
			assert(
				impl.safeDecodeBytes(Buffer.from([0xca, 0xfe]), "latin-1").length > 0,
				"latin-1 decodes",
			);
			assert(
				impl.safeDecodeBytes(bytes, "unknown-8bit").includes("café"),
				"alias table maps unknown-8bit",
			);
			assert(
				impl.safeDecodeBytes(bytes, '"><script>evil').length > 0,
				"hostile charset label degrades safely",
			);
			assert(
				impl.safeDecodeBytes(bytes, null).includes("café"),
				"null charset defaults utf-8",
			);
		},
	},
	{
		name: "rfc2047-decodes-without-raising",
		invariant:
			"A19: RFC 2047 encoded-words decode (B and Q), malformed structures degrade to raw, never raise",
		run: (impl) => {
			const b = impl.decodeHeaderValue("=?utf-8?B?Y2Fmw6k=?=");
			assert(b.includes("café"), `B-word decodes, got ${JSON.stringify(b)}`);
			const q = impl.decodeHeaderValue("=?utf-8?Q?caf=C3=A9?=");
			assert(q.includes("café"), `Q-word decodes, got ${JSON.stringify(q)}`);
			assert(
				impl.decodeHeaderValue("plain subject").includes("plain subject"),
				"raw passthrough",
			);
			assert(
				impl.decodeHeaderValue("=?broken??B???=").length > 0,
				"malformed word never raises",
			);
			assert(
				impl.decodeHeaderValue("=?nosuchcharset?B?aGk=?=").length > 0,
				"unknown charset in header degrades",
			);
		},
	},
	{
		name: "html-stripped-naive-and-safe",
		invariant:
			"A19: naive HTML fallback strips tags/entities; markup NEVER survives as executable text",
		run: (impl) => {
			const out = impl.stripHtml("<p>hello</p><br>bold &amp; <b>brave</b>");
			assert(out.includes("hello"), "text survives");
			assert(!/<[a-z]/i.test(out), "no tags survive");
			assert(out.includes("&") && !out.includes("&amp;"), "entities decoded");
			const hostile = impl.stripHtml("<script>alert(1)</script><p>body</p>");
			assert(!hostile.includes("<script>"), "script tag stripped");
			assert(
				hostile.includes("alert(1)") === false || !/<script/i.test(hostile),
				"no live script markup",
			);
		},
	},
	{
		name: "plain-preferred-html-fallback",
		invariant:
			"A19: multipart extraction PREFERS text/plain; html used only as fallback; attachments skipped",
		run: (impl) => {
			const parts = [
				{
					contentType: "text/html",
					disposition: null,
					payload: Buffer.from("<p>html body</p>"),
					charset: "utf-8",
				},
				{
					contentType: "text/plain",
					disposition: null,
					payload: Buffer.from("plain body"),
					charset: "utf-8",
				},
				{
					contentType: "application/pdf",
					disposition: "attachment",
					payload: Buffer.from("%PDF"),
					charset: null,
				},
			];
			assert(
				impl.extractTextBody(parts) === "plain body",
				"plain wins when present",
			);
			assert(
				impl.extractTextBody([parts[0], parts[2]] as never) === "html body",
				"html fallback strips tags",
			);
			assert(
				impl.extractTextBody([parts[2]] as never) === "",
				"attachment-only mail yields empty body",
			);
		},
	},
	{
		name: "automated-sender-gates",
		invariant:
			"A19: noreply-class addresses AND automated RFC headers gate silently; benign senders pass",
		run: (impl) => {
			assert(
				impl.isAutomatedSender("no-reply@example.com", {}),
				"pattern noreply",
			);
			assert(
				impl.isAutomatedSender("MAILER-DAEMON@mx.example", {}),
				"pattern daemon",
			);
			assert(
				impl.isAutomatedSender("news@example.com", {
					"List-Unsubscribe": "<...>",
				}),
				"automated header",
			);
			assert(
				!impl.isAutomatedSender("alice@example.com", {
					Precedence: "first-class",
				}),
				"benign sender passes (non-bulk Precedence)",
			);
			assert(
				!impl.isAutomatedSender("alice@example.com", {}),
				"clean sender passes",
			);
		},
	},
	{
		name: "sender-authentication-fail-closed",
		invariant:
			"A19/GHSA-rxqh: From-domain authentication FAILS CLOSED — no A-R header ⇒ drop; dmarc pass ⇒ accept; aligned spf/dkim ⇒ accept; misaligned ⇒ drop",
		run: (impl) => {
			const none = impl.verifySenderAuthentication([], "alice@example.com");
			assert(none.authenticated === false, "no header fails closed");
			const dmarc = impl.verifySenderAuthentication(
				["mx.example; dmarc=pass header.from=example.com"],
				"alice@example.com",
			);
			assert(dmarc.authenticated === true, "dmarc pass accepts");
			const spfAligned = impl.verifySenderAuthentication(
				["example.org; spf=pass smtp.mailfrom=bob@example.com"],
				"bob@mail.example.com",
			);
			assert(spfAligned.authenticated === true, "org-aligned spf accepts");
			const spfMisaligned = impl.verifySenderAuthentication(
				["example.org; spf=pass smtp.mailfrom=evil.net"],
				"alice@example.com",
			);
			assert(spfMisaligned.authenticated === false, "misaligned spf drops");
			const dkim = impl.verifySenderAuthentication(
				["x; dkim=pass header.d=example.com"],
				"a@example.com",
			);
			assert(dkim.authenticated === true, "aligned dkim accepts");
		},
	},
];
