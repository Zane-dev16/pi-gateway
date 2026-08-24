// pi_platforms/persistent-ws/dual-path-markdown — DUAL-PATH markdown dispatch
// for the persistent-ws shape (04 §10.2, Slack ground truth; gap-audit A17):
//
//   REST path (postMessage/edit):  standard md → platform dialect
//     (`convertMarkdownToMrkdwn`) — fence-protected placeholders; GFM tables
//     re-rendered as fenced column-aligned monospace (mrkdwn has no tables).
//   Native-stream path (*Stream):  sends RAW standard markdown UNCONVERTED —
//     converting centrally would violate §5 invariant 1; conversion belongs
//     to the PATH, never a global pre-send transform.
//
// The seal helper ports `_seal_stream`'s append discipline: the sealing pass
// transmits ONLY the unsent suffix `final_text[len(sent):]`, guarded by
// `startswith` — never a re-append of already-streamed bytes ("stacked
// copies" bug class, §5 invariant 1).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/slack/adapter.py:_convert_markdown_to_mrkdwn
//   plugins/platforms/slack/adapter.py:_seal_stream

/** A sealed-frame decision: append the delta, nothing to send, or rewrite. */
export type SealDecision =
	| { kind: "append"; delta: string }
	| { kind: "none" }
	| { kind: "rewrite" };

/**
 * `_seal_stream` suffix math: the final may only EXTEND what was streamed.
 * Prefix-stable streams always yield append/none; a rewritten accumulator
 * (shouldn't happen within a segment) forces the caller's fallback lane.
 */
export function sealSuffix(sent: string, finalText: string): SealDecision {
	if (finalText === sent) return { kind: "none" };
	if (finalText.startsWith(sent))
		return { kind: "append", delta: finalText.slice(sent.length) };
	return { kind: "rewrite" };
}

// ── the REST-path dialect conversion ────────────────────────────────────────

interface ProtectedRegion {
	token: string;
	original: string;
}

function isTableSeparatorRow(line: string): boolean {
	return /^\s*\|?[\s:|-]+\|[\s:|-]*$/.test(line) && line.includes("-");
}

function splitRow(line: string): string[] {
	return line
		.trim()
		.replace(/^\|/, "")
		.replace(/\|$/, "")
		.split("|")
		.map((c) => c.trim());
}

/** GFM table → fenced column-aligned monospace block (mrkdwn has no tables). */
export function renderTableAsFencedMonospace(table: string): string {
	const lines = table.replace(/\n+$/, "").split("\n");
	const header = splitRow(lines[0] ?? "");
	const bodyRows = lines
		.slice(1)
		.filter((l) => !isTableSeparatorRow(l))
		.map(splitRow);
	const widths = header.map((h, i) =>
		Math.max(h.length, ...bodyRows.map((r) => r[i]?.length ?? 0)),
	);
	const fmt = (cells: string[]) =>
		cells
			.map((c, i) => c + " ".repeat((widths[i] ?? 0) - c.length))
			.join("  ")
			.trimEnd();
	const out = ["```", fmt(header), widths.map((w) => "-".repeat(w)).join("  ")];
	for (const row of bodyRows) out.push(fmt(row));
	out.push("```");
	return out.join("\n");
}

/**
 * Standard markdown → fake-mrkdwn dialect for the REST send path.
 * Fenced code blocks are PROTECTED via placeholder extraction so their bytes
 * survive verbatim; inline conversions apply only outside protection:
 *   **bold** → *bold*, __underline__ → _underline_, ~~strike~~ → ~strike~,
 *   [text](url) → <url|text>, GFM tables → aligned-monospace fences.
 */
export function convertMarkdownToMrkdwn(markdown: string): string {
	const protectedRegions: ProtectedRegion[] = [];
	let counter = 0;

	// Protect fenced code blocks first (bytes must survive verbatim).
	let text = markdown.replace(/```[^\n]*\n[\s\S]*?```/g, (m) => {
		const token = `\u0000PROT${counter++}\u0000`;
		protectedRegions.push({ token, original: m });
		return token;
	});

	// GFM tables (header | separator | rows) → fenced monospace.
	text = text.replace(
		/(?:^\|\s.*\|\s*$\n?\|\s*[-:| ]+\|\s*$\n?(?:^\|.+\|\s*$\n?)*)/gm,
		(m) => renderTableAsFencedMonospace(m),
	);

	// Inline dialect mapping.
	text = text
		.replace(/\*\*([^*]+)\*\*/g, "*$1*")
		.replace(/__([^_]+)__/g, "_$1_")
		.replace(/~~([^~]+)~~/g, "~$1~")
		.replace(/\[([^\]]+)\]\(([^)\s]+)\)/g, "<$2|$1>");

	// Restore protected regions byte-exactly.
	for (const region of protectedRegions)
		text = text.replace(region.token, () => region.original);
	return text;
}
