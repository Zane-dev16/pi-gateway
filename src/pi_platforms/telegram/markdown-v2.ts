// pi_platforms/telegram/markdown-v2 — the Telegram formatting dialect
// (ported from READ-ONLY Hermes reference, semantics only, no code vendored):
//   plugins/platforms/telegram/adapter.py:_MDV2_ESCAPE_RE / _escape_mdv2
//     (every MarkdownV2 special char backslash-escaped OUTSIDE code spans)
//   adapter.py:format_message (dialect conversion bound to the SEND path —
//     never a global pre-send transform; §10.1)
//   adapter.py:_strip_mdv2 → kit stripMarkdownMarkup (tier-3 stripping is
//     OWNED by the kit formatting ladder; this module only converts)
//
// Scope split enforced here (keeps shared conformance rows byte-stable):
//   - SENDS (converted lane): STRUCTURAL markers only (**bold** → *bold*,
//     fences preserved verbatim); NO punctuation escaping — chunk indicators
//     "(i/n)" must survive byte-exact.
//   - FINALIZE EDITS (REQUIRES_EDIT_FINALIZE path, #25710): FULL conversion
//     incl. _escape_mdv2 punctuation escaping outside protected regions.

const MDV2_ESCAPE_RE = /([_*[\]()~`>#+=|{}.!\\])/g;

/** adapter.py:_escape_mdv2 — escape every MarkdownV2 special character. */
export function escapeMarkdownV2(text: string): string {
	return text.replace(MDV2_ESCAPE_RE, "\\$1");
}

/** Regions whose bytes pass through untouched (fenced code blocks). */
const FENCED_BLOCK_RE = /```[^\n]*\n[\s\S]*?(?:```|$)/g;

/**
 * FULL finalize-path conversion (#25710 anchor): structural markdown →
 * MarkdownV2 WITH punctuation escaping outside fenced blocks. Fences are
 * preserved verbatim; prose specials get backslash-escaped.
 */
export function toTelegramMarkdownV2Full(text: string): string {
	if (!text.includes("```")) {
		return fullConvertProse(text);
	}
	let out = "";
	let pos = 0;
	for (const m of text.matchAll(FENCED_BLOCK_RE)) {
		out += fullConvertProse(text.slice(pos, m.index));
		out += m[0];
		pos = (m.index ?? 0) + m[0].length;
	}
	out += fullConvertProse(text.slice(pos));
	return out;
}

/**
 * Structural-only send-path conversion: bold/italic markers collapsed to the
 * MarkdownV2 single-delimiter forms; NO punctuation escaping. This is the
 * tier-2 "converted" lane of the formatting ladder for telegram.
 */
export function toTelegramMarkdownV2(text: string): string {
	return convertStructural(text);
}

/** **x** → *x* (MDV2 bold) and __x__ → _x_ (MDV2 italic), fences untouched. */
function convertStructural(text: string): string {
	return (
		text
			// fenced blocks first: protect by splitting (convert outside only)
			.split(FENCED_BLOCK_PROTECT)
			.map((part) =>
				part.startsWith("```")
					? part
					: part
							.replace(/\*\*([^*]+)\*\*/g, "*$1*")
							.replace(/__([^_]+)__/g, "_$1_"),
			)
			.join("")
	);
}

/**
 * ONE prose segment for the FULL lane: collapse structural spans FIRST, then
 * escape every remaining special OUTSIDE those spans — the emitted single-
 * delimiter markers themselves stay unescaped (MDV2 needs them literal).
 */
function fullConvertProse(prose: string): string {
	const parts = prose.split(/(\*\*[^*]+\*\*|__[^_]+__)/g);
	return parts
		.map((part) => {
			if (part.startsWith("**") && part.endsWith("**") && part.length > 4) {
				return `*${escapeMarkdownV2(part.slice(2, -2))}*`;
			}
			if (part.startsWith("__") && part.endsWith("__") && part.length > 4) {
				return `_${escapeMarkdownV2(part.slice(2, -2))}_`;
			}
			return escapeMarkdownV2(part);
		})
		.join("");
}

const FENCED_BLOCK_PROTECT = /(```[^\n]*\n[\s\S]*?(?:```|$)|```[^`]*```)/g;

/**
 * Plain-lane probe: does this content carry the §6.1 fallback body prefix?
 * The plain-text fallback ships parse_mode=None RAW — never converted.
 */
export const PLAIN_LANE_PREFIX = "(Response formatting failed, plain text:)";

export function isPlainLaneContent(
	content: string,
	metadataParseMode: unknown,
): boolean {
	if (metadataParseMode === "none") return true;
	return content.startsWith(PLAIN_LANE_PREFIX);
}
