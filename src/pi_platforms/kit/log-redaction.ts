// pi_platforms/kit/log-redaction — DEC-033: the §8 "sensitive ids redacted in
// logs" clause is a GUARD/LOGGER-LEVEL property, not per-adapter code. The
// base adapter wraps whatever StreamLogger it is given with THIS filter, so
// every adapter built on the kit inherits redaction without per-adapter
// logging discipline.
//
// Two scrub passes per emitted field:
//   1. REGISTERED VALUES — exact literals registered by the owning adapter
//      (resolved token values, session keys) replaced wherever they appear,
//      including nested inside structured meta objects.
//   2. CREDENTIAL SHAPES — high-signal token patterns (Bearer schemes,
//      xox*/sk-/ghp_-class prefixes) scrubbed even when never registered:
//      adversarial payloads carry secrets in unexpected fields, and a
//      value-based registry alone cannot catch what it was never told about.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/platforms/base.py logging of send failures / disable reasons —
//     messages may embed error blobs that echo server-side payloads; the
//     logger is the one place redaction can be total.

import type { StreamLogger } from "../../pi_gateway/streaming/adapter-seam.js";

/** Replacement marker for every scrubbed span. */
export const REDACTED_PLACEHOLDER = "[redacted]";

/** Minimum length for a registerable secret (short strings over-redact). */
export const MIN_REDACTABLE_LENGTH = 6;

/**
 * Credential-shaped substrings scrubbed even when unregistered. Each pattern
 * matches `prefix + payload`; only the credential span is replaced so
 * surrounding log text stays readable.
 */
const CREDENTIAL_PATTERNS: readonly RegExp[] = [
	/\b(?:xoxb|xoxp|xoxa|xapp|xoxs)-[A-Za-z0-9-]{8,}/g, // Slack-class tokens
	/\bsk-[A-Za-z0-9_-]{12,}/g, // sk- API keys
	/\bghp_[A-Za-z0-9]{16,}/g, // GitHub PATs
	/\bgithub_pat_[A-Za-z0-9_]{16,}/g,
	/\bBearer\s+[A-Za-z0-9._~+/=-]{12,}/gi, // Bearer scheme + token
	/\bbot\d+:[A-Za-z0-9_-]{20,}/g, // bot<id>:<token> webhook-token shapes
];

export class SecretRedactor {
	private readonly values = new Set<string>();

	/** Register an exact sensitive literal (token value, session key, …). */
	register(value: string): void {
		if (value.length >= MIN_REDACTABLE_LENGTH) this.values.add(value);
	}

	registerAll(values: Iterable<string>): void {
		for (const v of values) this.register(v);
	}

	get registeredCount(): number {
		return this.values.size;
	}

	/** Replace every registered value occurrence with the placeholder. */
	private scrubRegistered(text: string): string {
		let out = text;
		for (const value of this.values) {
			if (out.includes(value)) {
				out = out.split(value).join(REDACTED_PLACEHOLDER);
			}
		}
		return out;
	}

	/** Scrub credential-shaped spans from raw text. */
	scrubCredentialShapes(text: string): string {
		return CREDENTIAL_PATTERNS.reduce(
			(acc, re) => acc.replace(re, REDACTED_PLACEHOLDER),
			text,
		);
	}

	/** Both passes over one string. */
	scrub(text: string): string {
		return this.scrubCredentialShapes(this.scrubRegistered(text));
	}

	/** Deep-scrub structured meta: every string field, at any nesting. */
	scrubMeta(
		meta: Record<string, unknown> | undefined,
	): Record<string, unknown> | undefined {
		if (meta === undefined) return undefined;
		currentRedactor = this;
		try {
			return scrubValue(meta) as Record<string, unknown>;
		} finally {
			currentRedactor = null;
		}
	}
}

// Deep meta scrubbing needs the owning redactor inside the recursion; the
// instance is threaded via this module-local for the duration of one
// synchronous walk (no awaits inside — single-threaded event loop).
let currentRedactor: SecretRedactor | null = null;

function scrubValue(value: unknown): unknown {
	if (typeof value === "string") {
		return currentRedactor !== null ? currentRedactor.scrub(value) : value;
	}
	if (Array.isArray(value)) return value.map(scrubValue);
	if (value !== null && typeof value === "object") {
		const out: Record<string, unknown> = {};
		for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
			out[k] = scrubValue(v);
		}
		return out;
	}
	return value;
}

/**
 * Wrap a logger with the redactor. Every level scrubs BOTH the message and
 * the structured meta before the inner sink ever sees bytes. Undefined inner
 * loggers stay undefined (silent adapters remain silent — the wrapper adds no
 * sink of its own).
 */
export function createRedactingLogger(
	inner: StreamLogger | undefined,
	redactor: SecretRedactor,
): StreamLogger | undefined {
	if (inner === undefined) return undefined;
	const emit =
		(level: keyof StreamLogger) =>
		(message: string, meta?: Record<string, unknown>): void => {
			const cleanMessage = redactor.scrub(message);
			const cleanMeta = redactor.scrubMeta(meta);
			const fn = inner[level] as
				| ((m: string, meta?: Record<string, unknown>) => void)
				| undefined;
			if (fn !== undefined) {
				if (cleanMeta !== undefined) fn.call(inner, cleanMessage, cleanMeta);
				else fn.call(inner, cleanMessage);
			}
		};
	return {
		debug: emit("debug"),
		warn: emit("warn"),
		error: emit("error"),
		info: emit("info"),
	};
}
