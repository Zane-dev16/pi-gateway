// pi_platforms/signal/signal-format — A18 ride-along: markdown → Signal
// native formatting as ONE pure module shared by the live adapter and any
// standalone send path (signal_format.py docstring: "Keep markdown → Signal
// native formatting conversion in one place so both the live Signal adapter
// and standalone send paths emit the same bodyRanges").
//
// Hermes anchors (READ-ONLY; semantics ported, no code vendored):
//   gateway/platforms/signal_format.py::markdown_to_signal — the ENTIRE
//     conversion contract:
//     - collapse \n{3,} → \n\n, trim, normalize Markdown bullet markers to
//       "• " OUTSIDE fenced code blocks (fences preserved byte-for-byte)
//     - ``` fences → inner bytes + a MONOSPACE range over them
//     - ATX headings #{1,6} → marker stripped + BOLD range over the text
//     - inline **b** __b__ ~~s~~ `m` *i* _i_ with FIRST-MATCH-WINS overlap
//       rejection (a span overlapping an occupied region is skipped whole)
//     - style positions are UTF-16 code-unit based ("Positions are measured
//       in UTF-16 code units because that's what the Signal protocol uses")
//     - output strings "start:length:STYLE" — signal-cli textStyle(s) params
//   Supported styles: BOLD, ITALIC, STRIKETHROUGH, MONOSPACE.

/**
 * Length of `s` in UTF-16 code units. JS strings ARE UTF-16, so this is
 * `.length` — kept as a named function so the contract reads explicitly and
 * the codepoint-vs-UTF16 distinction is greppable at call sites.
 */
function utf16Len(s: string): number {
	return s.length;
}

const FENCE_SPLIT_RE = /(```[\s\S]*?```)/;
const BULLET_LINE_RE = /^([ \t]{0,3})[-*+]\s+/gm;

/** signal_format.py::_normalize_bullet_markers — prose bullets become "• ";
 * list-looking lines inside fenced code blocks are code, not prose. */
function normalizeBulletMarkers(source: string): string {
	const parts = source.split(FENCE_SPLIT_RE);
	for (let idx = 0; idx < parts.length; idx++) {
		const part = parts[idx];
		if (part === undefined || idx % 2 === 1) continue; // odd slices ARE fences
		parts[idx] = part.replace(BULLET_LINE_RE, "$1• ");
	}
	return parts.join("");
}

interface StyleRange {
	start: number;
	length: number;
	style: string;
}

interface InlineMatch {
	ms: number;
	me: number;
	g1s: number;
	g1e: number;
	style: string;
}

/** Inline pattern set, in priority order (signal_format.py `patterns`).
 * The `d` flag yields EXACT group-1 offsets (m.indices[1]). */
const INLINE_PATTERNS: Array<{ re: RegExp; style: string }> = [
	{ re: /\*\*([\s\S]+?)\*\*/dg, style: "BOLD" },
	{ re: /__([\s\S]+?)__/dg, style: "BOLD" },
	{ re: /~~([\s\S]+?)~~/dg, style: "STRIKETHROUGH" },
	{ re: /`(.+?)`/dg, style: "MONOSPACE" },
	{ re: /(?<!\*)\*(?!\*| )(.+?)(?<!\*)\*(?!\*)/dg, style: "ITALIC" },
	{ re: /(?<!\w)_(?!_)(.+?)(?<!_)_(?!\w)/dg, style: "ITALIC" },
];

const CODE_BLOCK_RE = /```[a-zA-Z0-9_+-]*\n?([\s\S]*?)```/g;
const HEADING_RE = /^#{1,6}\s+/gm;

/**
 * Convert markdown to plain text + Signal bodyRanges style strings.
 *
 * Returns `[plainText, styleStrings]` where each style is
 * `"u16Start:u16Length:STYLE"`. Positions are UTF-16 code units measured on
 * the FINAL plain text.
 */
export function markdownToSignal(text: string): [string, string[]] {
	text = text.replace(/\n{3,}/g, "\n\n").trim();
	text = normalizeBulletMarkers(text);

	const styles: StyleRange[] = [];

	// ── fenced code blocks ────────────────────────────────────────────────
	CODE_BLOCK_RE.lastIndex = 0;
	for (;;) {
		const match = CODE_BLOCK_RE.exec(text);
		if (match === null) break;
		const inner = (match[1] ?? "").replace(/\n+$/, "");
		const start = match.index;
		text =
			text.slice(0, match.index) +
			inner +
			text.slice(match.index + match[0].length);
		styles.push({ start, length: inner.length, style: "MONOSPACE" });
		CODE_BLOCK_RE.lastIndex = start + inner.length;
	}

	// ── ATX headings → BOLD, markers removed (signal_format.py rebuild) ───
	{
		let newText = "";
		let lastEnd = 0;
		HEADING_RE.lastIndex = 0;
		for (;;) {
			const match = HEADING_RE.exec(text);
			if (match === null) break;
			newText += text.slice(lastEnd, match.index);
			lastEnd = match.index + match[0].length;
			const eolRaw = text.indexOf("\n", lastEnd);
			const eol = eolRaw === -1 ? text.length : eolRaw;
			const headingText = text.slice(lastEnd, eol);
			const start = newText.length;
			newText += headingText;
			styles.push({ start, length: headingText.length, style: "BOLD" });
			lastEnd = eol;
		}
		newText += text.slice(lastEnd);
		text = newText;
	}

	// ── inline spans: collect non-overlapping matches in source order ─────
	const allMatches: InlineMatch[] = [];
	const occupied: Array<[number, number]> = [];
	for (const { re, style } of INLINE_PATTERNS) {
		re.lastIndex = 0;
		for (;;) {
			const m = re.exec(text);
			if (m === null) break;
			const ms = m.index;
			const me = ms + m[0].length;
			const overlaps = occupied.some(([os, oe]) => ms < oe && me > os);
			if (overlaps) continue;
			const indices = m.indices?.[1];
			if (indices === undefined) continue;
			allMatches.push({ ms, me, g1s: indices[0], g1e: indices[1], style });
			occupied.push([ms, me]);
		}
	}
	allMatches.sort(
		(a, b) =>
			a.ms - b.ms ||
			a.me - b.me ||
			a.g1s - b.g1s ||
			a.g1e - b.g1e ||
			(a.style < b.style ? -1 : a.style > b.style ? 1 : 0),
	);

	// Marker-char removal plan (the delimiters around each group-1 span).
	const removals: Array<[number, number]> = [];
	for (const { ms, me, g1s, g1e } of allMatches) {
		if (g1s > ms) removals.push([ms, g1s - ms]);
		if (me > g1e) removals.push([g1e, me - g1e]);
	}
	removals.sort((a, b) => a[0] - b[0]);

	function adjust(pos: number): number {
		let shift = 0;
		for (const [removePos, removeLen] of removals) {
			if (removePos >= pos) break;
			shift += Math.min(removeLen, pos - removePos);
		}
		return pos - shift;
	}

	// Prior ranges (fence/heading) re-anchor AFTER marker removal.
	const adjustedPrior: StyleRange[] = [];
	for (const { start, length, style } of styles) {
		const newStart = adjust(start);
		const newEnd = adjust(start + length);
		if (newEnd > newStart)
			adjustedPrior.push({
				start: newStart,
				length: newEnd - newStart,
				style,
			});
	}

	// Emit the final text without markers, collecting inline ranges.
	let result = "";
	let lastEnd = 0;
	const inlineStyles: StyleRange[] = [];
	for (const { ms, me, g1s, g1e, style } of allMatches) {
		result += text.slice(lastEnd, ms);
		const pos = result.length;
		result += text.slice(g1s, g1e);
		inlineStyles.push({ start: pos, length: g1e - g1s, style });
		lastEnd = me;
	}
	result += text.slice(lastEnd);
	text = result;

	// Merge, sort by position, bounds-check, render UTF-16 "start:len:STYLE".
	const merged = [...adjustedPrior, ...inlineStyles].sort(
		(a, b) =>
			a.start - b.start ||
			a.length - b.length ||
			(a.style < b.style ? -1 : a.style > b.style ? 1 : 0),
	);
	const styleStrings: string[] = [];
	for (const { start, length, style } of merged) {
		if (start < 0 || start + length > text.length) continue;
		const u16Start = utf16Len(text.slice(0, start));
		const u16Len = utf16Len(text.slice(start, start + length));
		styleStrings.push(`${u16Start}:${u16Len}:${style}`);
	}

	return [text, styleStrings];
}
