// pi_platforms/slack/mrkdwn — the REST-path dialect conversion, ported
// faithfully from the READ-ONLY Hermes reference:
//
//   plugins/platforms/slack/adapter.py:format_message  (the whole ladder)
//   plugins/platforms/slack/adapter.py:_wrap_markdown_tables (+_disp_width,
//     _align_table) — GFM pipe tables re-rendered as fenced column-aligned
//     monospace with CJK display-width awareness (mrkdwn has no tables)
//
// DUAL-PATH binding (04 §10.2): this converter belongs to the REST send/edit
// PATH ONLY. The native *Stream path ships RAW standard markdown unconverted —
// converting centrally would violate §5 invariant 1. The adapter routes every
// chat.postMessage/chat.update-shaped transmission through convertMarkdownToSlackMrkdwn
// exactly once (single conversion point at the REST boundary).
//
// Ladder order (format_message parity):
//   wrap GFM tables → escape broadcast special mentions → protect fenced code
//   (dropping the opening-fence language tag) → protect inline code → convert
//   links → protect existing Slack entities → protect blockquote markers →
//   single-pass entity escape → headers → ***bold-italic*** → **bold** (with
//   the zero-width-space closing quirk fix) → *italic* → ~~strike~~ → restore.

// ── display width (adapter.py:_disp_width — CJK-aware alignment) ────────────

/** Wide East-Asian ranges counted as 2 columns for table alignment. */
function isWideCodePoint(cp: number): boolean {
	return (
		(cp >= 0x1100 && cp <= 0x115f) || // Hangul Jamo
		(cp >= 0x2e80 && cp <= 0xa4cf && cp !== 0x303f) || // CJK Radicals..Yi
		(cp >= 0xac00 && cp <= 0xd7a3) || // Hangul Syllables
		(cp >= 0xf900 && cp <= 0xfaff) || // CJK Compatibility Ideographs
		(cp >= 0xfe30 && cp <= 0xfe6f) || // CJK Compatibility Forms
		(cp >= 0xff00 && cp <= 0xff60) || // Fullwidth Forms
		(cp >= 0xffe0 && cp <= 0xffe6) ||
		(cp >= 0x20000 && cp <= 0x3fffd)
	);
}

export function dispWidth(s: string): number {
	let w = 0;
	for (const ch of s) w += isWideCodePoint(ch.codePointAt(0) ?? 0) ? 2 : 1;
	return w;
}

// ── GFM table wrapping (_wrap_markdown_tables / _align_table) ───────────────

function stripPipeEdges(line: string): string {
	return line.trim().replace(/^\|/, "").replace(/\|$/, "");
}

function splitTableRow(line: string): string[] {
	return stripPipeEdges(line)
		.split("|")
		.map((c) => c.trim());
}

function looksLikeSeparatorRow(line: string): boolean {
	const body = stripPipeEdges(line);
	if (!body.includes("-")) return false;
	return /^[\s:|-]+$/.test(body);
}

function padCell(cell: string, width: number): string {
	const gap = width - dispWidth(cell);
	return gap > 0 ? cell + " ".repeat(gap) : cell;
}

export interface TableRenderResult {
	fenced: string;
	rows: number;
	cols: number;
}

/** Column-aligned fenced monospace block for one GFM table (CJK aware). */
export function renderGfmTableFenced(table: string): TableRenderResult | null {
	const lines = table.replace(/\n+$/, "").split("\n");
	if (lines.length < 2) return null;
	const header = splitTableRow(lines[0] as string);
	if (header.length === 0) return null;
	if (!looksLikeSeparatorRow(lines[1] ?? "")) return null;
	const rows = [header];
	for (const line of lines.slice(2)) {
		if (line.trim().length === 0) continue;
		if (looksLikeSeparatorRow(line)) continue;
		rows.push(splitTableRow(line));
	}
	const cols = Math.max(...rows.map((r) => r.length));
	const widths: number[] = [];
	for (let i = 0; i < cols; i++) {
		widths.push(Math.max(...rows.map((r) => dispWidth(r[i] ?? ""))));
	}
	const fmt = (cells: string[]) =>
		cells
			.map((c, i) => padCell(c, widths[i] ?? 0))
			.join("  ")
			.trimEnd();
	const out = ["```", fmt(header), widths.map((w) => "-".repeat(w)).join("  ")];
	for (const row of rows.slice(1)) out.push(fmt(row));
	out.push("```");
	return { fenced: out.join("\n"), rows: rows.length, cols };
}

/**
 * Wrap every GFM pipe-table block in the text with its aligned fenced form.
 * Matches header | separator | body rows exactly like the Hermes regex
 * (_wrap_markdown_tables).
 */
export function wrapMarkdownTables(text: string): string {
	const re = /(?:^\|\s.*\|\s*$\n?\|\s*[-:| ]+\|\s*$\n?(?:^\|.+\|\s*$\n?)*)/gm;
	return text.replace(re, (m) => renderGfmTableFenced(m)?.fenced ?? m);
}

// ── THE converter (format_message parity) ────────────────────────────────────

interface ProtectedRegion {
	token: string;
	original: string;
}

const SPECIAL_MENTION_RE = /<!(?:everyone|channel|here)>/g;

/**
 * Standard markdown → Slack mrkdwn. Fenced code and inline code are PROTECTED
 * via placeholders so their bytes survive verbatim (only a genuine
 * line-starting opening fence loses its language tag — mrkdwn renders the tag
 * literally). Broadcast mentions are neutralized BEFORE entity protection so
 * model output cannot trigger workspace notifications.
 */
export function convertMarkdownToSlackMrkdwn(markdown: string): string {
	if (!markdown) return markdown;

	let text = wrapMarkdownTables(markdown);

	const placeholders: ProtectedRegion[] = [];
	let counter = 0;
	const stash = (value: string): string => {
		const token = `\u0000SL${counter++}\u0000`;
		placeholders.push({ token, original: value });
		return token;
	};

	// Broadcast mentions: escape only the leading angle bracket.
	text = text.replace(SPECIAL_MENTION_RE, (m) => m.replace("<", "&lt;"));

	// 1) Protect fenced blocks; drop the language tag on a genuine opening
	//    fence (start of input or preceded by newline), never mid-line spans.
	text = text.replace(
		/```(?:[^\n]*\n)?[\s\S]*?```/g,
		(block, offset: number) => {
			const atLineStart = offset === 0 || text[offset - 1] === "\n";
			let protectedBlock = block;
			if (atLineStart) {
				// Drop the language tag from a genuine opening fence only.
				const open = /^```([^\s`]+)[ \t]*(\r?\n)/.exec(block);
				if (open) {
					protectedBlock =
						"```" + (open[2] ?? "\n") + block.slice(open[0].length);
				}
			}
			return stash(protectedBlock);
		},
	);

	// 2) Protect inline code.
	text = text.replace(/(`[^`]+`)/g, (m) => stash(m));

	// 3) Convert links [text](url) → <url|text>.
	text = text.replace(
		/(?<!!)\[([^\]]+)\]\(([^()]*(?:\([^()]*\)[^()]*)*)\)/g,
		(_m, label: string, url: string) => {
			const u = url.trim().replace(/^</, "").replace(/>$/, "").trim();
			return stash(`<${u}|${label}>`);
		},
	);

	// 4) Protect existing Slack entities / manual links.
	text = text.replace(/<(?:[@#!]|(?:https?|mailto|tel):)[^>\n]+>/g, (m) =>
		stash(m),
	);

	// 5) Protect blockquote markers.
	text = text.replace(/^(>+\s)/gm, (m) => stash(m));

	// 6) Single-pass entity escape (unescape first so already-escaped input is
	//    stable — sequential str.replace would rescan its own output).
	text = text.replace(
		/&amp;|&lt;|&gt;/g,
		(m) => ({ "&amp;": "&", "&lt;": "<", "&gt;": ">" })[m] ?? m,
	);
	text = text
		.split("&")
		.join("&amp;")
		.split("<")
		.join("&lt;")
		.split(">")
		.join("&gt;");

	// 7) Headers → bold lines; redundant bold markers stripped inside.
	text = text.replace(/^#{1,6}\s+(.+)$/gm, (_m, inner: string) => {
		const stripped = inner.trim().replace(/\*\*(.+?)\*\*/g, "$1");
		return stash(`*${stripped}*`);
	});

	// 8) Bold+italic → *_text_*.
	text = text.replace(/\*\*\*(.+?)\*\*\*/g, (_m, inner: string) =>
		stash(`*_${inner}_*`),
	);

	// 9) Bold → *text*; Slack's parser truncates when the closing * follows a
	//    non-word char — insert a zero-width space before it (format_message
	//    step 9 quirk fix).
	text = text.replace(/\*\*(.+?)\*\*/g, (_m, inner: string) => {
		if (inner.length > 0 && !/[A-Za-z0-9_]/.test(inner.slice(-1))) {
			return stash(`*${inner}\u200b*`);
		}
		return stash(`*${inner}*`);
	});

	// 10) Single-star italic → _italic_, only between non-whitespace edges.
	text = text.replace(
		/(?<!\*)\*(\S(?:[^*\n]*?\S)?)\*(?!\*)/g,
		(_m, inner: string) => stash(`_${inner}_`),
	);

	// 11) Strikethrough.
	text = text.replace(/~~(.+?)~~/g, (_m, inner: string) => stash(`~${inner}~`));

	// 13) Restore placeholders in reverse stash order.
	for (let i = placeholders.length - 1; i >= 0; i--) {
		const region = placeholders[i]!;
		text = text.split(region.token).join(region.original);
	}
	return text;
}
