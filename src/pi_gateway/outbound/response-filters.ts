// pi_gateway/outbound/response-filters.ts — delivery-vs-persist response
// filters (03-message-routing.md §9.1).
//
// The gateway boundary first decides whether a completed turn is DELIVERED AT
// ALL, independently of what persists: suppression happens here, AFTER the
// agent core has already flushed the turn's messages, so history stays
// complete for replay/cache while the channel stays quiet. Filtering at the
// wrong layer (pre-persist) corrupts replay.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/response_filters.py:LIVE_GATEWAY_SILENT_MARKERS      → LIVE_GATEWAY_SILENT_MARKERS
//   gateway/response_filters.py:is_intentional_silence_response  → isIntentionalSilenceResponse
//   gateway/response_filters.py:is_autonomous_silence_response   → isAutonomousSilenceResponse
//   gateway/response_filters.py:is_intentional_silence_agent_result → isIntentionalSilenceAgentResult
//   gateway/response_filters.py:is_partial_silence_marker        → isPartialSilenceMarker
//
// The marker set lives ONCE and both lanes (interactive + autonomous) share
// it, so they can never drift apart (03 §9.1 table). All matchers are PURE —
// no I/O, no clock — so the delivered/persisted split is a total function of
// the response text (+ agent-result failure flag).

/** Exact whole-response markers that mean "the agent intentionally chose not to reply". */
export const LIVE_GATEWAY_SILENT_MARKERS: ReadonlySet<string> = new Set([
	"[SILENT]",
	"SILENT",
	"NO_REPLY",
	"NO REPLY",
]);

const MAX_SILENCE_CANDIDATE_LENGTH = 64;

/** Uppercase + whitespace collapse (Python `" ".join(text.upper().split())`). */
function canonicalSilenceCandidate(text: string): string {
	return text.trim().toUpperCase().split(/\s+/).filter(Boolean).join(" ");
}

/**
 * Strip stray edge punctuation without erasing marker structure.
 * Models sometimes emit `.NO_REPLY` or `*NO_REPLY*`; square brackets stay
 * structural so malformed `[SILENT` never canonicalizes to `SILENT`.
 */
function stripEdgeSilencePunctuation(text: string): string {
	let start = 0;
	let end = text.length;
	while (
		start < end &&
		text[start] !== "[" &&
		text[start] !== "]" &&
		isPunctuation(text[start] as string)
	) {
		start++;
	}
	while (
		end > start &&
		text[end - 1] !== "[" &&
		text[end - 1] !== "]" &&
		isPunctuation(text[end - 1] as string)
	) {
		end--;
	}
	return text.slice(start, end).trim();
}

/** Unicode general-category P* test (`\p{P}` ≙ Python `unicodedata.category().startswith("P")`). */
function isPunctuation(ch: string): boolean {
	return /^\p{P}$/u.test(ch);
}

/** Canonical candidates: exact form first, edge-punctuation-stripped fallback second. */
function canonicalSilenceCandidates(text: string): [string, ...string[]] {
	const trimmed = text.trim();
	const exact = canonicalSilenceCandidate(trimmed);
	const stripped = stripEdgeSilencePunctuation(trimmed);
	if (stripped === trimmed) return [exact];
	return [exact, canonicalSilenceCandidate(stripped)];
}

/**
 * True ONLY when the entire response is exactly a silence marker.
 * Prose merely mentioning NO_REPLY delivers normally; blank is NOT silence
 * (that is the empty-response failure path); >64 chars is never a marker.
 */
export function isIntentionalSilenceResponse(response: unknown): boolean {
	if (typeof response !== "string") return false;
	const stripped = response.trim();
	if (!stripped || stripped.length > MAX_SILENCE_CANDIDATE_LENGTH) return false;
	for (const candidate of canonicalSilenceCandidates(stripped)) {
		if (LIVE_GATEWAY_SILENT_MARKERS.has(candidate)) return true;
	}
	return false;
}

/** A non-empty line whose canonical form IS a marker. */
function isWholeLineToken(line: string): boolean {
	return LIVE_GATEWAY_SILENT_MARKERS.has(canonicalSilenceCandidate(line));
}

/**
 * Loose matcher for autonomous lanes (cron ticks, webhook). Suppresses when
 * the marker is the whole response, sits alone on the first or last line,
 * or the bracketed sentinel opens the response (the documented
 * "[SILENT] No changes detected" same-line prefix — restricted to the
 * bracketed form so "Silent retry succeeded" is NOT swallowed). Shares
 * LIVE_GATEWAY_SILENT_MARKERS with the interactive rule.
 */
export function isAutonomousSilenceResponse(response: unknown): boolean {
	if (typeof response !== "string") return false;
	const stripped = response.trim();
	if (!stripped) return false;

	// Whole response is exactly a token.
	if (isWholeLineToken(stripped)) return true;
	// Marker alone on its own first or last line.
	const lines = stripped.split(/\r\n|\n|\r/).filter((ln) => ln.trim());
	if (
		lines.length > 0 &&
		(isWholeLineToken(lines[0] as string) ||
			isWholeLineToken(lines[lines.length - 1] as string))
	) {
		return true;
	}
	// Bracketed sentinel as same-line prefix.
	if (stripped.toUpperCase().startsWith("[SILENT]")) return true;
	return false;
}

/** Minimal agent-turn message shape needed for the dedup scan (runner-shaped). */
export interface SilenceAgentResult {
	/** Failed turns deliver their errors — silence suppresses successful turns only. */
	failed?: boolean;
}

/**
 * Silence markers suppress DELIVERY only for successful agent turns; a failed
 * turn always delivers its error text regardless of any marker in it.
 */
export function isIntentionalSilenceAgentResult(
	agentResult: SilenceAgentResult | null | undefined,
	response: unknown,
): boolean {
	if (agentResult == null || typeof agentResult !== "object") return false;
	if (agentResult.failed) return false;
	return isIntentionalSilenceResponse(response);
}

/**
 * Streaming hold-back: True while `text` could STILL resolve to a marker
 * (its canonical form is a non-empty prefix of some marker), so streaming
 * never edits a raw marker onto screen and then retracts. Anything already
 * diverged from every marker resumes normal streaming immediately.
 */
export function isPartialSilenceMarker(text: unknown): boolean {
	if (typeof text !== "string") return false;
	const stripped = text.trim();
	if (!stripped || stripped.length > MAX_SILENCE_CANDIDATE_LENGTH) return false;
	for (const candidate of canonicalSilenceCandidates(stripped)) {
		if (!candidate) continue;
		for (const marker of LIVE_GATEWAY_SILENT_MARKERS) {
			if (marker.startsWith(candidate)) return true;
		}
	}
	return false;
}

// ---------------------------------------------------------------------------
// Ordered disposition pipeline.
//
// §9.1: the boundary decides DELIVERED-AT-ALL per lane, independently of
// persistence. The pipeline is ORDERED and TOTAL: each filter class runs in a
// fixed sequence and its verdict records WHY delivery did or did not happen.
// Persistence is ALWAYS true here — this module never decides what history
// stores; it only gates egress after persist.
// ---------------------------------------------------------------------------

/** Which consumer lane the completed turn arrived from. */
export type ResponseLane = "interactive" | "cron" | "webhook";

export type DeliverySuppressionReason =
	| "intentional_silence"
	| "autonomous_silence"
	| null;

export interface DeliveryDisposition {
	/** What the platform sees: false ⇒ nothing goes out on the channel. */
	deliver: boolean;
	/** What history stores: ALWAYS true post-persist (persist ≠ deliver). */
	persist: true;
	reason: DeliverySuppressionReason;
	/** Matcher class that produced the verdict ("none" when delivering). */
	matcher:
		| "is_intentional_silence_response"
		| "is_autonomous_silence_response"
		| "none";
}

export interface DeliveryDispositionInput {
	lane: ResponseLane;
	/** Final assistant text (already sealed by the stream consumer when streaming). */
	response: unknown;
	/** Runner's turn result; `.failed === true` forces delivery of errors. */
	agentResult?: SilenceAgentResult | null;
}

/**
 * Ordered delivery-vs-persist evaluation (03 §9.1):
 *   interactive → is_intentional_silence_response (EXACT marker only)
 *   cron/webhook → is_autonomous_silence_response  (loose: line/prefix forms)
 * Both share ONE marker set; failed turns bypass suppression entirely
 * (`is_intentional_silence_agent_result` semantics applied per lane).
 */
export function resolveDeliveryDisposition(
	input: DeliveryDispositionInput,
): DeliveryDisposition {
	const { lane, response } = input;
	const failed = input.agentResult?.failed === true;

	// Failure path: errors always deliver, on every lane.
	if (failed) {
		return { deliver: true, persist: true, reason: null, matcher: "none" };
	}

	if (lane === "interactive") {
		// Parity: is_intentional_silence_agent_result(None, …) ⇒ False — without a
		// turn result there is nothing asserting the turn succeeded, so deliver.
		if (
			input.agentResult != null &&
			isIntentionalSilenceAgentResult(input.agentResult, response)
		) {
			return {
				deliver: false,
				persist: true,
				reason: "intentional_silence",
				matcher: "is_intentional_silence_response",
			};
		}
	} else if (isAutonomousSilenceResponse(response)) {
		return {
			deliver: false,
			persist: true,
			reason: "autonomous_silence",
			matcher: "is_autonomous_silence_response",
		};
	}
	return { deliver: true, persist: true, reason: null, matcher: "none" };
}
