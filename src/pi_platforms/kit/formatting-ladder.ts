// pi_platforms/kit/formatting-ladder — the THREE-TIER per-send ladder for
// markdown-rendering platforms (04-platform-adapters.md §10.1; Telegram ground
// truth) with the rich/capability downgrade machinery adapters bind into their
// SEND PATH (never centralized — a global pre-send converter breaks native
// streaming §10.2 and media sends §10.1).
//
// Ported from the READ-ONLY Hermes reference, semantics only:
//   plugins/platforms/telegram/adapter.py:_try_send_rich
//     (rich FIRST with the RAW payload; capability errors LATCH
//      _rich_send_disabled permanently — probe once per process;
//      BadRequest-class falls back WITHOUT latching; TRANSIENT rich failures
//      are NEVER legacy-resent — duplicate risk — failed SendResult with
//      extracted retry_after)
//   adapter.py:_is_rich_capability_error / _is_rich_fallback_error
//   adapter.py:_strip_mdv2 (parse-failure plain resend strips escape/markup)
//   adapter.py:_separate_chunk_indicator_from_fence (indicator relocation)
//
// Ladder per TEXT send:
//   1. Rich first (skipped when latched off or metadata marks expect-edits).
//   2. MarkdownV2/dialect conversion path.
//   3. Parse-failure fallback: parse-classified send errors resend plain
//      (parse_mode=None) with markup stripped.

import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { extractRetryAfterSeconds } from "./send-retry.js";

export type LadderTier = "rich" | "converted" | "plain";

/** Rich-failure classification (see classifyRichFailure below). */
export type RichErrorClass = "capability" | "fallback" | "transient";

/** Blob token that marks a tier-2 failure as parse/markdown-classified. */
export const CLASSIFY_FORMATTING = "parse entities";

/**
 * The transport seam an adapter supplies — the three wire paths, kept distinct
 * because dialect conversion belongs to the PATH (§10.2 obligation).
 */
export interface FormattingTransport {
	/** Tier 1: platform-native rich message (RAW markdown payload in). */
	tryRich(content: string, metadata: Metadata): Promise<SendResult>;
	/** Tier 2: converted-dialect send (adapter escapes/converts BEFORE wire). */
	sendConverted(content: string, metadata: Metadata): Promise<SendResult>;
	/** Tier 3: parse_mode=None plain resend of STRIPPED content. */
	sendPlain(content: string, metadata: Metadata): Promise<SendResult>;
}

export interface FormattingLadderOptions {
	/**
	 * Skip tier 1 when metadata marks expect-edits (Hermes: rich path skipped
	 * for messages that will be edited later).
	 */
	expectEditsMetadataKey?: string | undefined;
	log?: ((message: string, meta?: Record<string, unknown>) => void) | undefined;
}

export interface LadderOutcome extends SendResult {
	/** Which tier actually delivered (or attempted last). */
	tier: LadderTier;
	/** True when THIS call latched rich mode off permanently. */
	latchedRichOff: boolean;
}

/**
 * The downgrade latch. ONE instance per adapter; persists for the session
 * (probe once per process). Capability errors latch ONCE; BadRequest-class
 * failures fall back WITHOUT latching.
 */
export class FormattingLadder {
	/** _rich_send_disabled parity — permanent for this session once true. */
	richSendDisabled = false;
	/** Times the latch fired (observability; "latches ONCE" is asserted by test). */
	richLatchCount = 0;

	constructor(
		private readonly transport: FormattingTransport,
		private readonly opts: FormattingLadderOptions = {},
	) {}

	get richDisabled(): boolean {
		return this.richSendDisabled;
	}

	async sendText(
		content: string,
		metadata: Metadata = {},
	): Promise<LadderOutcome> {
		const expectKey = this.opts.expectEditsMetadataKey ?? "expect_edits";

		// Tier 1 — rich first unless latched or expect-edits marked.
		if (!this.richSendDisabled && metadata[expectKey] !== true) {
			let rich: SendResult;
			try {
				rich = await this.transport.tryRich(content, metadata);
			} catch (err) {
				rich = {
					success: false,
					error: err instanceof Error ? err.message : String(err),
				};
			}
			if (rich.success) return { ...rich, tier: "rich", latchedRichOff: false };
			const klass = classifyRichFailure(rich.error ?? "");
			if (klass === "capability") {
				// LATCH ONCE — every later send skips the doomed roundtrip.
				this.richSendDisabled = true;
				this.richLatchCount += 1;
				this.opts.log?.("rich endpoint unavailable — latching rich off", {
					error: rich.error,
				});
				// fall through to tier 2 (legacy path owns delivery now)
			} else if (klass === "transient") {
				// NEVER legacy-resend a transient rich failure — the request may
				// have reached the server (duplicate risk). Failed SendResult
				// with extracted retry_after.
				return {
					...rich,
					retryable: true,
					retryAfter:
						rich.retryAfter ??
						extractRetryAfterSeconds(new Error(rich.error ?? "")),
					tier: "rich",
					latchedRichOff: false,
				};
			}
			// klass === "fallback" (BadRequest-class): fall through WITHOUT
			// latching — the next message may be fine.
		}

		// Tier 2 — converted-dialect send.
		const converted = await this.transport.sendConverted(content, metadata);
		if (converted.success)
			return { ...converted, tier: "converted", latchedRichOff: false };

		// Tier 3 — parse/markdown-classified failure ⇒ plain resend of the
		// stripped content. Only parse-classified errors take this lane;
		// anything else surfaces as-is.
		const blob = (converted.error ?? "").toLowerCase();
		if (!blob.includes(CLASSIFY_FORMATTING)) {
			return { ...converted, tier: "converted", latchedRichOff: false };
		}
		const plain = await this.transport.sendPlain(
			stripMarkdownMarkup(content),
			metadata,
		);
		return {
			...plain,
			tier: "plain",
			latchedRichOff: this.richSendDisabled,
		};
	}
}

/**
 * Rich-failure classification (_is_rich_capability_error /
 * _is_rich_fallback_error parity). One lowercased-blob classifier:
 *   capability → method-not-found / 404 / unsupported-endpoint classes → LATCH
 *   fallback   → BadRequest-class / "unsupported" → fall back, NO latch
 *   transient  → everything else → never legacy-resend
 */
export function classifyRichFailure(errorText: string): RichErrorClass {
	const s = errorText.toLowerCase();
	if (
		s.includes("method not found") ||
		s.includes("endpoint not found") ||
		s.includes("no such method") ||
		s.includes("404") ||
		s.includes("does not exist")
	)
		return "capability";
	if (
		s.includes("bad request") ||
		s.includes("unsupported") ||
		s.includes("not implemented")
	)
		return "fallback";
	return "transient";
}

/**
 * Parse-failure plain resend content (base._strip_mdv2 semantics, generic):
 * remove escape backslashes and common markdown markers so the fallback
 * carries clean text. Content stays byte-exact AFTER stripping — deterministic
 * transform, no truncation.
 */
export function stripMarkdownMarkup(text: string): string {
	let cleaned = text.replace(/\\([_*[\]()~`>#+=|{}.!\\])/g, "$1");
	cleaned = cleaned.replace(/\*\*([^*]+)\*\*/g, "$1"); // **bold** before *bold*
	cleaned = cleaned.replace(/\*([^*]+)\*/g, "$1");
	cleaned = cleaned.replace(/(?<!\w)_([^_]+)_(?!\w)/g, "$1"); // snake_case safe
	cleaned = cleaned.replace(/~([^~]+)~/g, "$1");
	cleaned = cleaned.replace(/\|\|([^|]+)\|\|/g, "$1");
	return cleaned;
}
