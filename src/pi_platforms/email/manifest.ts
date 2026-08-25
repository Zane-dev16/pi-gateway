// pi_platforms/email/manifest — EMAIL MANIFEST DATA (Phase-6 census port).
// Every constant TRANSCRIBED from the READ-ONLY Hermes reference plugin
// (plugins/platforms/email/adapter.py) and cited by file:symbol (Q17/DEC-017).

import type { PluginManifest } from "../kit/index.js";

/** adapter.py:MAX_MESSAGE_LENGTH = 50_000 — "Gmail-safe max length per body". */
export const EMAIL_MAX_BODY_CHARS = 50_000;

/** adapter.py:SMTP_CONNECT_TIMEOUT = 30 (also the IMAP connect/fetch timeout). */
export const EMAIL_IMAP_TIMEOUT_MS = 30_000;
export const EMAIL_SMTP_TIMEOUT_S = 30;

/** adapter.py:__init__ — `_esecret_int("EMAIL_POLL_INTERVAL", 15)` seconds. */
export const EMAIL_POLL_INTERVAL_MS = 15_000;

/**
 * adapter.py:__init__ — `_seen_uids_max: int = 2000` ("cap to prevent
 * unbounded memory growth"); _trim_seen_uids keeps the TOP HALF.
 */
export const EMAIL_SEEN_UIDS_MAX = 2000;

/** Port defaults: IMAP4_SSL 993 / SMTP 587; port 465 ⇒ implicit TLS. */
export const EMAIL_IMAP_PORT_DEFAULT = 993;
export const EMAIL_SMTP_PORT_DEFAULT = 587;
export const EMAIL_SMTP_IMPLICIT_TLS_PORT = 465;

/**
 * adapter.py:_NOREPLY_PATTERNS — automated senders are silently ignored.
 * Substring match against the lowercased envelope address.
 */
export const EMAIL_NOREPLY_PATTERNS: readonly string[] = [
	"noreply",
	"no-reply",
	"no_reply",
	"donotreply",
	"do-not-reply",
	"mailer-daemon",
	"postmaster",
	"bounce",
	"notifications@",
	"automated@",
	"auto-confirm",
	"auto-reply",
	"automailer",
];

/**
 * adapter.py:_AUTOMATED_HEADERS — RFC headers that indicate bulk/automated
 * mail. Predicate semantics transcribed verbatim.
 */
export const EMAIL_AUTOMATED_HEADERS: Readonly<
	Record<string, (value: string) => boolean>
> = {
	"Auto-Submitted": (v) => v.toLowerCase() !== "no",
	Precedence: (v) => ["bulk", "list", "junk"].includes(v.toLowerCase()),
	"X-Auto-Response-Suppress": (v) => v.length > 0,
	"List-Unsubscribe": (v) => v.length > 0,
};

/**
 * adapter.py:_CHARSET_ALIASES — charset labels seen in the wild that codecs
 * reject; mapped before the utf-8 → latin-1 fallback ladder (#35901).
 */
export const EMAIL_CHARSET_ALIASES: Readonly<Record<string, string>> = {
	"unknown-8bit": "utf-8",
	unknown: "utf-8",
	"x-unknown": "utf-8",
	default: "utf-8",
	"ansi_x3.110-1983": "latin-1",
	"cp-850": "cp850",
	gb2312: "gb18030",
	gbk: "gb18030",
	"ks_c_5601-1987": "cp949",
};

// ── the PluginManifest ──────────────────────────────────────────────────────

/**
 * Capability note (mattermost/signal parity): email is a POLLING platform on
 * the IMAP UID cursor with SMTP sends. NO draft streaming, NO edits, NO
 * interactive callbacks on the real surface — declared streaming matches seal
 * reality via probe-computed exclusions. A21 ride-along: the SMTP connector
 * carries the source's IPv4 fallback ladder (_create_ipv4_connection).
 */
export const EMAIL_PLUGIN_MANIFEST: PluginManifest = {
	name: "email",
	description:
		"Email adapter on the polling transport family (IMAP UID cursor in, SMTP MIME plain out)",
	transportShape: "polling",
	requiresEnv: [
		{
			name: "EMAIL_ADDRESS",
			description: "The agent's email address",
			password: false,
		},
		{
			name: "EMAIL_PASSWORD",
			description: "Password or app password",
			password: true,
		},
		{
			name: "EMAIL_IMAP_HOST",
			description: "IMAP server host (e.g. imap.gmail.com)",
			password: false,
		},
		{
			name: "EMAIL_SMTP_HOST",
			description: "SMTP server host (e.g. smtp.gmail.com)",
			password: false,
		},
	],
	optionalEnv: [
		{ name: "EMAIL_IMAP_PORT", description: "IMAP port (default 993)" },
		{ name: "EMAIL_SMTP_PORT", description: "SMTP port (default 587)" },
		{
			name: "EMAIL_POLL_INTERVAL",
			description: "Seconds between mailbox checks (default 15)",
		},
		{
			name: "EMAIL_ALLOWED_USERS",
			description: "Comma-separated allowlist of sender addresses",
			password: false,
		},
		{
			name: "EMAIL_ALLOW_ALL_USERS",
			description: "Accept any sender (dev only)",
			password: false,
		},
		{
			name: "EMAIL_TRUST_FROM_HEADER",
			description:
				"Opt out of Authentication-Results sender verification (risky)",
			password: false,
		},
	],
	capabilities: {
		supportsAsyncDelivery: true,
		splitsLongMessages: true,
		typedCommandPrefix: "/",
		interactiveResume: false,
		supportsInchannelContinuable: false,
		requiresEditFinalize: false,
	},
	// Q17 review note: no client-side rate tiers exist in the Hermes plugin;
	// poll cadence (15s default) and connection timeouts ARE the budget data.
};
