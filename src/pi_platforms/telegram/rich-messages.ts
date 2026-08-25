// pi_platforms/telegram/rich-messages — Bot API 10.1 rich-message eligibility
// + payload builders (tg2-6), ported from the READ-ONLY Hermes reference:
//   plugins/platforms/telegram/adapter.py:_needs_rich_rendering
//   adapter.py:_rich_eligible / _content_fits_rich_limits
//   adapter.py:_has_telegram_desktop_details_math_crash_shape
//   adapter.py:_has_telegram_desktop_cjk_rich_garble_shape
//   adapter.py:_rich_normalize_linebreaks / _rich_message_payload
//
// The rich endpoint carries RAW agent markdown (never format_message —
// MarkdownV2 escaping destroys table pipes); eligibility reserves it for
// constructs the legacy path degrades: pipe tables, GFM task lists,
// collapsible <details> blocks, and block math (#45995).

import { TELEGRAM_RICH_MESSAGE_MAX_CHARS } from "./manifest.js";

/** gateway/platforms/helpers.py:TABLE_SEPARATOR_RE. */
const TABLE_SEPARATOR_RE = /^\s*\|?\s*:?-+:?\s*(?:\|\s*:?-+:?\s*)+\|?\s*$/;

/** GFM task-list item ("- [ ] " / "* [x] ") at line start. */
const TASK_LIST_RE = /^\s*[-*]\s+\[[ xX]\]\s+/;

/** <details>/<summary> structural tags at line start (adapter.py :2036). */
const DETAILS_TAG_RE =
	/(^<details\b)|(^<\/details>)|(^<summary\b)|(^<\/summary>)/;

/** adapter.py:_RICH_DETAILS_RE — collapsible blocks (dot-all). */
const RICH_DETAILS_RE = /<details\b[^>]*>[\s\S]*?<\/details>/gi;

/**
 * adapter.py:_RICH_MATH_IN_DETAILS_RE — block math markers inside a details
 * block ($$…$$, \[…\], \(…\), or TeX control words).
 */
const MATH_IN_DETAILS_SOURCE =
	"\\$\\$[\\s\\S]*?\\$\\$" +
	"|\\\\\\[[\\s\\S]*?\\\\\\]" +
	"|\\\\\\([\\s\\S]*?\\\\\\)" +
	"|\\\\(?:sum|frac|alpha|beta|gamma|delta|theta|lambda|mu|pi|sigma|int|prod|sqrt|lim|infty|begin\\{(?:equation|align|matrix|cases)\\})";
const RICH_MATH_IN_DETAILS_PROBE = new RegExp(MATH_IN_DETAILS_SOURCE, "i");

/** adapter.py:_RICH_CJK_RE — CJK ranges current TDesktop drafts garble. */
const RICH_CJK_RE =
	/[\u3040-\u30ff\u3400-\u4dbf\u4e00-\u9fff\uac00-\ud7af\uf900-\ufaff\u{20000}-\u{323af}]/u;

/** Protected regions keep their bare newlines: fenced code OR pipe tables. */
const RICH_PROTECTED_REGION_RE =
	/```[^\n]*\n[\s\S]*?```|^[^\n]*\|[^\n]*\n[ \t]*\|?[ \t]*:?-+:?[ \t]*(?:\|[ \t]*:?-+:?[ \t]*)+\|?[ \t]*(?:\n[^\n]*\|[^\n]*)*/gm;

/** Single (non-paragraph) newlines in prose become markdown hard breaks. */
const SINGLE_NEWLINE_RE = /(?<!\n)\n(?!\n)/g;

/**
 * adapter.py:_needs_rich_rendering port — true for content whose legacy
 * rendering degrades: table delimiter rows, task lists, details blocks,
 * block math.
 */
export function needsRichRendering(content: string): boolean {
	if (!content) return false;
	const lines = content.split("\n");
	if (lines.some((line) => TABLE_SEPARATOR_RE.test(line))) return true;
	if (lines.some((line) => TASK_LIST_RE.test(line))) return true;
	if (DETAILS_TAG_RE.test(content)) return true;
	return content.includes("$$");
}

/**
 * adapter.py:_has_telegram_desktop_details_math_crash_shape port — math
 * inside a collapsible block crashes Telegram Desktop 6.9.1
 * (telegramdesktop/tdesktop#30808); skip rich for that shape.
 */
export function hasDetailsMathCrashShape(content: string): boolean {
	if (!content) return false;
	for (const m of content.matchAll(RICH_DETAILS_RE)) {
		if (RICH_MATH_IN_DETAILS_PROBE.test(m[0])) return true;
	}
	return false;
}

/**
 * adapter.py:_has_telegram_desktop_cjk_rich_garble_shape port — CJK text
 * garbles in current TDesktop rich drafts (#47653); legacy renders cleanly.
 */
export function hasCjkRichGarbleShape(content: string): boolean {
	return !!content && RICH_CJK_RE.test(content);
}

/**
 * adapter.py:_content_fits_rich_limits port — the one locally countable hard
 * limit: 32,768 characters (code points; Python len semantics). Other Bot API
 * rich limits surface as BadRequest → fallback-classified upstream.
 */
export function fitsRichLimits(content: string): boolean {
	let units = 0;
	for (const ch of content) {
		void ch;
		units += 1; // code-point count
	}
	return units <= TELEGRAM_RICH_MESSAGE_MAX_CHARS;
}

/**
 * adapter.py:_rich_normalize_linebreaks port — single newlines become
 * markdown hard breaks ("  \n") so multi-line prose doesn't collapse into one
 * paragraph; paragraph breaks (\n\n), fenced code, and pipe-table blocks are
 * left untouched.
 */
export function richNormalizeLinebreaks(text: string): string {
	if (!text || !text.includes("\n")) return text;
	const out: string[] = [];
	let pos = 0;
	for (const m of text.matchAll(RICH_PROTECTED_REGION_RE)) {
		const start = m.index ?? 0;
		out.push(text.slice(pos, start).replace(SINGLE_NEWLINE_RE, "  \n"));
		out.push(m[0]); // protected region verbatim
		pos = start + m[0].length;
	}
	out.push(text.slice(pos).replace(SINGLE_NEWLINE_RE, "  \n"));
	return out.join("");
}

/**
 * adapter.py:_rich_message_payload port — the InputRichMessage object from
 * RAW markdown (NEVER format_message output).
 */
export function richMessagePayload(content: string): { markdown: string } {
	return { markdown: richNormalizeLinebreaks(content) };
}

/** Content eligibility for rich delivery, ignoring per-call metadata gates
 * (_rich_eligible minus expect_edits, which the ladder owns). */
export function isRichEligibleContent(content: string): boolean {
	return (
		!!content &&
		content.trim() !== "" &&
		needsRichRendering(content) &&
		!hasDetailsMathCrashShape(content) &&
		!hasCjkRichGarbleShape(content) &&
		fitsRichLimits(content)
	);
}
