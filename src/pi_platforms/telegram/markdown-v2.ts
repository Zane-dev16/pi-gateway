// pi_platforms/telegram/markdown-v2 — the Telegram formatting dialect
// (ported from READ-ONLY Hermes reference, semantics only, no code vendored):
//   plugins/platforms/telegram/adapter.py:_MDV2_ESCAPE_RE / _escape_mdv2
//     (every MarkdownV2 special char backslash-escaped OUTSIDE code spans)
//   adapter.py::send (formatted=self.format_message(content) under
//     parse_mode=MARKDOWN_V2 — chunk markers "(1/2)" escaped "\\(1/2\\)" so
//     Telegram cannot reject the chunk)
//   adapter.py:edit_message finalize branch (parse_mode=MARKDOWN_V2,
//     _strip_mdv2 plain retry; mid-stream edits carry NO parse_mode)
//
// Scope (tg-2 parity): BOTH user-visible text lanes — sends AND finalize
// edits (#25710 REQUIRES_EDIT_FINALIZE) — emit FULL format_message-style
// conversion: structural markdown collapsed AND every remaining MarkdownV2
// special escaped OUTSIDE protected regions, so text always matches its
// parse_mode=MarkdownV2 stamp. Plain lanes (§6.1 fallback body / explicit
// parse_mode "none") ship RAW with no parse_mode.

const MDV2_ESCAPE_RE = /([_*[\]()~`>#+=|{}.!\\])/g;

/** adapter.py:_escape_mdv2 — escape every MarkdownV2 special character. */
export function escapeMarkdownV2(text: string): string {
	return text.replace(MDV2_ESCAPE_RE, "\\$1");
}

/** Regions whose bytes pass through untouched (fenced code blocks). */
const FENCED_BLOCK_RE = /```[^\n]*\n[\s\S]*?(?:```|$)/g;

/**
 * FULL format_message-style conversion (adapter.py::send + edit_message
 * finalize): structural markdown → MarkdownV2 WITH punctuation escaping
 * outside fenced blocks. Fences are preserved verbatim; prose specials get
 * backslash-escaped. This is THE converted lane for sends and finalize edits
 * alike — emitted text always parses under parse_mode=MarkdownV2.
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
