// pi_platforms/kit/send-retry — the §6.1 egress retry ladder
// (base.py::_send_with_retry) plus the ONE shared error classifier.
//
// Ladder (04-platform-adapters.md §6.1, verbatim semantics):
//   send()
//    ├─ success → done
//    ├─ timeout-classified error → NOT retried (non-idempotent: message may
//    │     have arrived); returned as-is (no plain-text fallback either)
//    ├─ network-classified (result.retryable OR retryable patterns):
//    │     retry ≤ maxRetries=2, exponential backoff base 2.0s + jitter;
//    │     server retryAfter AUTHORITATIVE over local schedule, honored once;
//    │     └─ exhausted → user-facing "delivery failed, please resend" notice
//    └─ other (formatting/permission) → plain-text fallback:
//          "(Response formatting failed, plain text:)\n\n{content[:3500]}"
//
// Error classification uses ONE shared lowercased blob (str(exc) + class
// name) so the retry classifier and the plain-text-fallback classifier
// CANNOT silently disagree on the same failure.

import type { SendResult } from "../../pi_gateway/streaming/adapter-seam.js";

/** The shared lowercased classification blob (str(exc) + class name). */
export function errorBlob(err: unknown): string {
	const message =
		err instanceof Error
			? err.message
			: typeof err === "object" && err !== null && "message" in err
				? String((err as { message: unknown }).message)
				: String(err);
	const className = err instanceof Error ? err.constructor.name : "";
	return `${message} ${className}`.toLowerCase();
}

export type SendErrorClass =
	| "timeout" // ambiguous — never retried, never fallback-sent
	| "connect-timeout" // connection never established — safe to re-send
	| "flood" // server retry_after present — authoritative
	| "network" // transient transport loss
	| "formatting" // markdown render rejection → plain-text fallback lane
	| "permission"
	| "unknown";

const TIMEOUT_PATTERNS = ["timed out", "timeout"];
const CONNECT_TIMEOUT_PATTERNS = [
	"connecttimeout",
	"connect timeout",
	"connection timeout",
];
const NETWORK_PATTERNS = [
	"connecterror",
	"connectionreset",
	"connection refused",
	"connectionreset",
	"network",
	"broken pipe",
	"econnreset",
	"econnrefused",
	"enotfound",
	"socket hang up",
];
const FORMATTING_PATTERNS = [
	"can't parse entities",
	"parse entities",
	"markdown",
	"bad request: message text is empty", // render produced nothing sendable
];

/**
 * THE classifier — one blob in, one class out. Retry decisions AND the
 * plain-text-fallback decision both read THIS result; no second classifier
 * may exist at a call site.
 */
export function classifySendError(err: unknown): SendErrorClass {
	const blob = errorBlob(err);
	if (CONNECT_TIMEOUT_PATTERNS.some((p) => blob.includes(p)))
		return "connect-timeout";
	// FloodWait carries a server-authoritative delay ("retry after N" /
	// retry_after attr surfaced by the adapter).
	const m = /retry\s+(?:after|in)\s+(\d+)/.exec(blob);
	if (m || blob.includes("flood")) return "flood";
	if (TIMEOUT_PATTERNS.some((p) => blob.includes(p))) return "timeout";
	if (NETWORK_PATTERNS.some((p) => blob.includes(p))) return "network";
	if (FORMATTING_PATTERNS.some((p) => blob.includes(p))) return "formatting";
	if (
		blob.includes("forbidden") ||
		blob.includes("unauthorized") ||
		blob.includes("not enough rights")
	)
		return "permission";
	return "unknown";
}

/** Extract a server-authoritative `retry_after` from an error, if any. */
export function extractRetryAfterSeconds(err: unknown): number | null {
	const attrs = err as { retryAfter?: unknown; retry_after?: unknown } | null;
	if (
		typeof attrs?.retryAfter === "number" &&
		Number.isFinite(attrs.retryAfter)
	)
		return attrs.retryAfter;
	if (
		typeof attrs?.retry_after === "number" &&
		Number.isFinite(attrs.retry_after)
	)
		return attrs.retry_after;
	const m = /retry\s+(?:after|in)\s+(\d+)/.exec(errorBlob(err));
	return m ? Number(m[1]) : null;
}

export const PLAIN_TEXT_FALLBACK_PREFIX =
	"(Response formatting failed, plain text:)";

/** base.py parity cap for the plain-text fallback body. */
export const PLAIN_TEXT_FALLBACK_CAP = 3500;

export const DELIVERY_FAILED_NOTICE =
	"⚠️ Delivery failed after retries — please resend.";

export interface RetryLadderOptions {
	maxRetries?: number | undefined;
	baseDelayMs?: number | undefined;
	/** Jitter fraction (0..1); deterministic when rng injected. */
	jitterFraction?: number | undefined;
	sleep?: ((ms: number) => Promise<void>) | undefined;
	rng?: (() => number) | undefined;
}

interface RetryTransport {
	attempt(
		content: string,
		metadata: Record<string, unknown>,
	): Promise<SendResult>;
}

/**
 * _send_with_retry parity over an attempt seam. Formatting-class failures do
 * NOT loop here — callers consult classifySendError and route to their own
 * plain-text fallback lane; this ladder owns ONLY the retry/flood/exhaustion
 * behavior and returns the classified failure otherwise.
 */
export async function sendWithRetry(
	content: string,
	metadata: Record<string, unknown>,
	attempt: RetryTransport["attempt"],
	options: RetryLadderOptions = {},
): Promise<SendResult> {
	const maxRetries = options.maxRetries ?? 2;
	const baseDelayMs = options.baseDelayMs ?? 2000;
	const sleep =
		options.sleep ?? ((ms) => new Promise<void>((r) => setTimeout(r, ms)));
	const rng = options.rng ?? (() => Math.random());
	const jitterFraction = options.jitterFraction ?? 0;

	let honoredRetryAfterOnce = false;
	for (let tryIndex = 0; ; tryIndex++) {
		let result: SendResult;
		try {
			result = await attempt(content, metadata);
		} catch (err) {
			result = {
				success: false,
				error: String(err instanceof Error ? err.message : err),
			};
			(result as { __err?: unknown }).__err = err;
		}
		if (result.success) return result;

		const err =
			(result as { __err?: unknown }).__err ?? new Error(result.error ?? "");
		const klass = classifySendError(err);

		// Timeout ambiguity: NOT retried (the message may have arrived), and
		// NO plain-text fallback either — returned as-is for the caller.
		if (klass === "timeout") return result;

		const retryAfter =
			result.retryAfter !== undefined && result.retryAfter !== null
				? result.retryAfter
				: extractRetryAfterSeconds(err);

		const networkClassified =
			result.retryable === true ||
			klass === "connect-timeout" ||
			klass === "network" ||
			(klass === "flood" && retryAfter !== null);
		if (!networkClassified) return result; // formatting/permission/unknown → caller lanes

		if (tryIndex >= maxRetries) {
			return {
				success: false,
				error: result.error,
				retryable: true,
				retryAfter,
			};
		}

		if (retryAfter !== null && !honoredRetryAfterOnce) {
			// Server-authoritative delay honored ONCE over the local schedule.
			honoredRetryAfterOnce = true;
			await sleep(retryAfter * 1000);
			continue;
		}
		const jitter = 1 + jitterFraction * rng();
		await sleep(baseDelayMs * 2 ** tryIndex * jitter);
	}
}

/** Build the §6.1 plain-text fallback body for a formatting-rejected chunk. */
export function plainTextFallbackBody(content: string): string {
	const body =
		content.length > PLAIN_TEXT_FALLBACK_CAP
			? content.slice(0, PLAIN_TEXT_FALLBACK_CAP)
			: content;
	return `${PLAIN_TEXT_FALLBACK_PREFIX}\n\n${body}`;
}
