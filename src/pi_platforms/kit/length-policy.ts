// pi_platforms/kit/length-policy — THE ONE chat length-resolution pair
// (04-platform-adapters.md §6.3, gap-audit A15).
//
// Chunk budgets are measured PER CHAT, not per adapter class. Three members
// move TOGETHER and every chunking/truncation call site must take budget AND
// unit from the SAME chat resolution:
//   - message_len_fn            → ChatLengthPolicy.lenFn      (unit)
//   - max_message_length_for_chat → ChatLengthPolicy.maxUnits (budget)
//   - message_len_fn_for_chat   → folded into the same resolution object
//
// An adapter whose chats can differ in cap OR unit overrides the per-chat PAIR;
// mixing a scalar budget with a per-chat unit splits wrong. The chunker
// (chunking.ts) accepts ONLY this policy object — there is no call site that
// can take a bare scalar, so the §6.3 obligation holds by construction.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/platforms/base.py:max_message_length_for_chat / message_len_fn /
//     message_len_fn_for_chat                       (default scalar 4096+len)
//   gateway/relay/adapter.py:_descriptor_for_chat → _LEN_FNS (_utf16_len =
//     utf-16-le byte length // 2)                   (relay overrides the trio)

export type LengthUnit = "chars" | "utf16";

/** Default scalar cap when an adapter declares none (base.py MAX_MESSAGE_LENGTH). */
export const DEFAULT_MAX_MESSAGE_LENGTH = 4096;

/** Codepoint length (Python len() parity: astral chars count as ONE). */
export function codePointLen(text: string): number {
	let n = 0;
	for (const _ of text) n += 1;
	return n;
}

/**
 * UTF-16 code-unit length — Bot API counts UTF-16, so codepoint math silently
 * over-splits or 400s. utf-16-le byte length // 2 (relay/adapter.py:_utf16_len).
 */
export function utf16Len(text: string): number {
	return Buffer.byteLength(text, "utf16le") / 2;
}

/** Length function for a declared unit. */
export function lenFnForUnit(unit: LengthUnit): (text: string) => number {
	return unit === "utf16" ? utf16Len : codePointLen;
}

/** The resolved per-chat pair. Budget AND unit always travel together. */
export interface ChatLengthPolicy {
	chatId: string;
	unit: LengthUnit;
	lenFn: (text: string) => number;
	maxUnits: number;
}

export interface ChatLengthResolutionInput {
	chatId: string;
	/**
	 * Adapter-wide default unit (message_len_fn property). Defaults to
	 * codepoints; Telegram-class adapters override to utf16.
	 */
	unit?: LengthUnit | undefined;
	/**
	 * Per-chat descriptor override — relay-shaped adapters resolve their
	 * fronted-platform descriptor here. When present it wins over the scalar.
	 */
	descriptor?:
		| {
				maxMessageLength?: number | undefined;
				lenUnit?: LengthUnit | undefined;
		  }
		| undefined;
	/** Scalar fallback cap when no descriptor applies. */
	scalarMaxUnits?: number | undefined;
}

/**
 * THE single chat resolution (§6.3). Every consumer resolves budget AND unit
 * from THIS function's result — never from two independent lookups.
 *
 * Precedence: per-chat descriptor > adapter-wide unit/scalar defaults, with
 * the descriptor's OWN members preferred together so a descriptor supplying
 * only one member still upgrades the pair coherently.
 */
export function resolveChatLengthPolicy(
	input: ChatLengthResolutionInput,
): ChatLengthPolicy {
	const baseUnit = input.unit ?? "chars";
	const unit = input.descriptor?.lenUnit ?? baseUnit;
	const maxUnits =
		input.descriptor?.maxMessageLength ??
		input.scalarMaxUnits ??
		DEFAULT_MAX_MESSAGE_LENGTH;
	return {
		chatId: input.chatId,
		unit,
		lenFn: lenFnForUnit(unit),
		maxUnits: Math.max(1, Math.trunc(maxUnits)),
	};
}
