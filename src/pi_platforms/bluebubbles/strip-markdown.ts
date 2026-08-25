// pi_platforms/bluebubbles/strip-markdown — gateway/platforms/helpers.py:strip_markdown
// regex ladder, ported EXACTLY (READ-ONLY reference; semantics ported, no code
// vendored). BlueBubbles format_message = strip_markdown(content)
// (bluebubbles.py:format_message @~600) — iMessage bubbles are plain text.
//
// Ladder order is LOAD-BEARING (helpers.py @~150-165): bold ** → italic * →
// bold __ → italic _ → fenced code blocks → inline code → headings → links →
// 3+ newlines collapsed to paragraph breaks → trim. Each pass consumes the
// output of the previous one; reordering changes results on combined markup.

/**
 * helpers.py:_RE_BOLD — `\*\*(.+?)\*\*` with DOTALL. Non-greedy span between
 * double asterisks across lines.
 */
const RE_BOLD = /\*\*([\s\S]+?)\*\*/g;

/**
 * helpers.py:_RE_ITALIC_STAR — `\*(.+?)\*` with DOTALL. Runs AFTER bold so
 * `**x**` remnants never re-match here.
 */
const RE_ITALIC_STAR = /\*([\s\S]+?)\*/g;

/**
 * helpers.py:_RE_BOLD_UNDER — `\b__(?![\s_])(.+?)(?<![\s_])__\b` with DOTALL.
 * Word-boundary + non-space guards keep snake_case identifiers intact.
 */
const RE_BOLD_UNDER = /\b__(?![\s_])([\s\S]+?)(?<![\s_])__\b/g;

/** helpers.py:_RE_ITALIC_UNDER — `\b_(?![\s_])(.+?)(?<![\s_])_\b` with DOTALL. */
const RE_ITALIC_UNDER = /\b_(?![\s_])([\s\S]+?)(?<![\s_])_\b/g;

/** helpers.py:_RE_CODE_BLOCK — "```[a-zA-Z0-9_+-]*\n?" — fence line INCLUSIVE of tag, content kept. */
const RE_CODE_BLOCK = /```[a-zA-Z0-9_+-]*\n?/g;

/** helpers.py:_RE_INLINE_CODE — '`(.+?)`'. */
const RE_INLINE_CODE = /`([\s\S]+?)`/g;

/** helpers.py:_RE_HEADING — `^#{1,6}\s+` with MULTILINE (`\s` includes newlines). */
const RE_HEADING = /^#{1,6}\s+/gm;

/** helpers.py:_RE_LINK — `\[([^\]]+)\]\([^\)]+\)` keeps the label text. */
const RE_LINK = /\[([^\]]+)\]\([^)]+\)/g;

/** helpers.py:_RE_MULTI_NEWLINE — `\n{3,}` collapses to a paragraph break. */
const RE_MULTI_NEWLINE = /\n{3,}/g;

/**
 * helpers.py:strip_markdown — strip markdown formatting for plain-text
 * platforms (SMS, iMessage, …). Byte-order parity with the Python ladder;
 * Python `$1` backreferences map to capture-group replacement.
 */
export function stripMarkdown(text: string): string {
	let out = text.replace(RE_BOLD, '$1');
	out = out.replace(RE_ITALIC_STAR, '$1');
	out = out.replace(RE_BOLD_UNDER, '$1');
	out = out.replace(RE_ITALIC_UNDER, '$1');
	out = out.replace(RE_CODE_BLOCK, '');
	out = out.replace(RE_INLINE_CODE, '$1');
	out = out.replace(RE_HEADING, '');
	out = out.replace(RE_LINK, '$1');
	out = out.replace(RE_MULTI_NEWLINE, "\n\n");
	return out.trim();
}
