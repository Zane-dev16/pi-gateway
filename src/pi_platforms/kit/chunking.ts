// pi_platforms/kit/chunking — (i/n) chunk math with fence-carry rendering
// (04-platform-adapters.md §6.2), CONSUMING the pi_gateway/outbound offset-safe
// segmentation primitive. Ladders RENDER from segments — never re-split
// rendered output — so byte offsets stay truthful end-to-end.
//
// Semantics ported from the READ-ONLY Hermes reference, cited as file:symbol:
//   gateway/platforms/base.py:truncate_message   (INDICATOR_RESERVE, fence
//     close/reopen with language tag, inline-code walkback, degenerate budget
//     floor "progress guaranteed, never spin")
//   plugins/platforms/telegram/adapter.py:_separate_chunk_indicator_from_fence
//     ((N/M) markers relocated off closing-fence lines)
//
// Contract:
//   - chunks of at most policy.maxUnits UNITS (unit from THE chat resolution
//     pair, length-policy.ts);
//   - a chunk ending inside a fenced block CLOSES it; the next chunk REOPENS
//     it with the original language tag;
//   - multi-chunk output carries "(i/n)" indicators, INDICATOR_RESERVE=10
//     headroom reserved BEFORE splitting; an indicator never shares a line
//     with a closing fence;
//   - astral (surrogate-pair) content is split-safe: cuts snap off trailing
//     surrogates (base.py:_custom_unit_to_cp maps budgets to CODEPOINT-aligned
//     slice positions — Pi enforces the same alignment at render time);
//   - BYTE-EXACT round trip: stripChunkScaffolding(chunkWithFenceCarry(x))
//     === x. Hermes' truncate_message lstrips the remainder after each cut,
//     dropping the separator byte; Pi's pipeline consumes the byte-preserving
//     offset-safe primitive instead and never drops bytes (proposed DEC).
//   - degenerate budgets floor at one codepoint per chunk (progress
//     guaranteed, never spin); such a chunk may exceed maxUnits by that one
//     codepoint — intentional (dropping = data loss, spinning = hang).

import {
	computeChunkLabel,
	segmentByOffsets,
	type Segment,
} from "../../pi_gateway/outbound/segmentation.js";
import type { ChatLengthPolicy } from "./length-policy.js";

/** Room for " (XX/XX)" (base.py:truncate_message INDICATOR_RESERVE). */
export const INDICATOR_RESERVE = 10;
const FENCE_CLOSE = "\n```";

export interface ChunkPlan {
	chunks: string[];
	/** Chunk count (>1 ⇒ labels were appended). */
	chunkCount: number;
	/** Per-chunk scaffolding record — consumed by stripChunkScaffolding. */
	scaffold: ScaffoldRecord[];
}

/** Exact bytes this module ADDED to one chunk (the inverse-stripping key). */
export interface ScaffoldRecord {
	/** Bytes of SYNTHESIZED reopen prefix prepended (0 when none). */
	prefixLen: number;
	/** True when a SYNTHESIZED "\n```" close was appended. */
	closeAdded: boolean;
	/** Separator bytes inserted between body and the "(i/n)" label (0 or 1). */
	labelJoinLen: number;
}

/**
 * Split `content` for ONE chat's resolved policy. Pure: same input ⇒ same
 * plan. Single-chunk passthrough when the content already fits.
 */
export function chunkWithFenceCarry(
	content: string,
	policy: ChatLengthPolicy,
): ChunkPlan {
	if (policy.lenFn(content) <= policy.maxUnits) {
		return {
			chunks: [content],
			chunkCount: 1,
			scaffold: [{ prefixLen: 0, closeAdded: false, labelJoinLen: 0 }],
		};
	}
	const rendered = renderSegments(content, policy);
	const n = rendered.length;
	if (n <= 1) {
		return {
			chunks: [rendered[0]?.body ?? content],
			chunkCount: 1,
			scaffold: [{ prefixLen: 0, closeAdded: false, labelJoinLen: 0 }],
		};
	}
	const chunks: string[] = [];
	const scaffold: ScaffoldRecord[] = [];
	rendered.forEach((c, idx) => {
		const label = computeChunkLabel(idx + 1, n);
		let text: string;
		let joinLen: number;
		if (c.labelAfterFenceClose) {
			text = `${c.body}\n${label}`;
			joinLen = 1;
		} else if (/\s$/.test(c.body)) {
			// Body already ends in whitespace — bare label, no doubled separator.
			text = `${c.body}${label}`;
			joinLen = 0;
		} else {
			text = `${c.body} ${label}`;
			joinLen = 1;
		}
		chunks.push(text);
		scaffold.push({
			prefixLen: c.prefixLen,
			closeAdded: c.closeAdded,
			labelJoinLen: joinLen,
		});
	});
	return { chunks, chunkCount: n, scaffold };
}

// ── rendering pipeline ───────────────────────────────────────────────────────

/** One wire chunk plus the exact scaffolding this module added to it. */
interface WireChunk {
	/** Rendered body WITHOUT the (i/n) label. */
	body: string;
	labelFreeLen: number;
	/** Bytes of SYNTHESIZED reopen prefix prepended (0 when none). */
	prefixLen: number;
	/** True when a SYNTHESIZED "\n```" close was appended. */
	closeAdded: boolean;
	/**
	 * True when the label must sit on its own line AFTER a trailing fence
	 * close (_separate_chunk_indicator_from_fence parity).
	 */
	labelAfterFenceClose: boolean;
}

function renderSegments(
	content: string,
	policy: ChatLengthPolicy,
): WireChunk[] {
	// Single planning pass at the indicator-reserved budget; fence-carry
	// overhead is accounted LOCALLY per chunk (base.py computes headroom per
	// chunk, not globally).
	const plan = segmentByOffsets(
		content,
		Math.max(1, policy.maxUnits - INDICATOR_RESERVE),
	);
	return expandSegments(plan.segments, policy);
}

function endsWithClosingFenceLine(text: string): boolean {
	return text.endsWith("```") && !text.endsWith("````");
}

function segmentStartsWithOpenerLine(text: string, opener: string): boolean {
	const first = (text.split("\n", 1)[0] ?? "").trim();
	return first === opener.trim();
}

/**
 * Expand primitive segments into wire chunks: sub-split any segment still over
 * budget (the primitive rides whole protected spans — long fenced blocks
 * arrive as one oversized segment and ARE split here with fence carry, exactly
 * base.py's line-walk), then apply fence close/reopen rendering per segment.
 */
function expandSegments(
	segments: Segment[],
	policy: ChatLengthPolicy,
): WireChunk[] {
	const out: WireChunk[] = [];
	for (const seg of segments) {
		// Conservative rendered-size estimate: body + carried-opener prefix +
		// synthesized close + indicator reserve. Over ⇒ sub-split with exact
		// per-piece overhead accounting.
		const startsWithOwnOpener =
			seg.fenceOpener !== undefined &&
			segmentStartsWithOpenerLine(seg.text, seg.fenceOpener);
		const needsPrefix = seg.fenceOpener !== undefined && !startsWithOwnOpener;
		const estimate =
			policy.lenFn(seg.text) +
			(needsPrefix ? policy.lenFn(seg.fenceOpener ?? "") + 1 : 0) +
			(seg.endsInsideFence ? FENCE_CLOSE.length : 0) +
			INDICATOR_RESERVE;
		if (estimate > policy.maxUnits) {
			out.push(...subSplitOversizedSegment(seg, policy));
			continue;
		}

		let text = seg.text;
		let prefixLen = 0;
		// Reopen the carried fence FIRST (with original language tag) — unless
		// the segment text already BEGINS with that opener line (the primitive's
		// unclosed-fence tail span includes it; prepending would double the
		// fence). An in-text opener opens the block naturally and stays content.
		if (
			seg.fenceOpener !== undefined &&
			!segmentStartsWithOpenerLine(seg.text, seg.fenceOpener)
		) {
			const prefix = `${seg.fenceOpener}\n`;
			text = prefix + text;
			prefixLen = prefix.length;
		}
		let closeAdded = false;
		if (seg.endsInsideFence) {
			text += FENCE_CLOSE;
			closeAdded = true;
		}
		out.push({
			body: text,
			labelFreeLen: policy.lenFn(text),
			prefixLen,
			closeAdded,
			labelAfterFenceClose: endsWithClosingFenceLine(text),
		});
	}
	return out;
}

/**
 * Hard-split one oversized segment at safe boundaries (newline ≥ half-budget,
 * else space/tab, else hard cut snapped off surrogate trails), walking fence
 * state so blocks crossing sub-splits close and reopen correctly. This is the
 * base.py:truncate_message inner loop over segment-LOCAL text whose entry
 * fence state is known. Byte-exact: separator bytes ride with the left part.
 */
function subSplitOversizedSegment(
	seg: Segment,
	policy: ChatLengthPolicy,
): WireChunk[] {
	const out: WireChunk[] = [];
	// Entry fence state. A carried opener means we CONTINUE an open block —
	// except when the segment text itself begins with that opener line (the
	// primitive's tail span includes it); then the walk below opens the block
	// naturally and no synthetic prefix is emitted.
	const startsWithOwnOpener =
		seg.fenceOpener !== undefined &&
		segmentStartsWithOpenerLine(seg.text, seg.fenceOpener);
	let inFence = seg.fenceOpener !== undefined && !startsWithOwnOpener;
	let lang = inFence ? (langOf(seg.fenceOpener) ?? "") : "";

	let remaining = seg.text;
	while (remaining.length > 0) {
		const prefix = inFence ? `\`\`\`${lang}\n` : "";
		const bodyBudget = Math.max(
			1,
			policy.maxUnits -
				INDICATOR_RESERVE -
				policy.lenFn(prefix) -
				FENCE_CLOSE.length,
		);
		// Whole-rest fitting reserves the close too — an open block at the
		// end of this chunk still costs FENCE_CLOSE before the indicator.
		const fitsWhole =
			policy.lenFn(prefix) + policy.lenFn(remaining) <=
			policy.maxUnits - INDICATOR_RESERVE - FENCE_CLOSE.length;

		let body: string;
		if (fitsWhole) {
			body = remaining;
			remaining = "";
		} else {
			const cut = chooseSubCut(remaining, bodyBudget);
			body = remaining.slice(0, cut);
			remaining = remaining.slice(cut);
		}

		// Walk ONLY the body to update fence state (prefix excluded — base.py).
		for (const line of body.split("\n")) {
			const stripped = line.trim();
			if (!stripped.startsWith("```")) continue;
			if (inFence && stripped === "```") {
				inFence = false;
				lang = "";
			} else if (!inFence) {
				inFence = true;
				lang = langOf(stripped) ?? "";
			}
		}

		let text = prefix + body;
		let closeAdded = false;
		if (inFence) {
			text += FENCE_CLOSE;
			closeAdded = true;
		}
		out.push({
			body: text,
			labelFreeLen: policy.lenFn(text),
			prefixLen: prefix.length,
			closeAdded,
			labelAfterFenceClose: endsWithClosingFenceLine(text),
		});

		// Degenerate-progress guarantee: chooseSubCut floors at one codepoint,
		// so remaining strictly shrinks every iteration (never spins).
	}
	return out;
}

/**
 * Last soft boundary within budget outside inline-code spans, surrogate-safe:
 * newline ≥ half-budget preferred, else space/tab, else hard cut snapped off a
 * trailing high surrogate (never split a pair). The cut lands AFTER the chosen
 * separator so no byte is lost or duplicated.
 */
function chooseSubCut(text: string, budgetUnits: number): number {
	const hardEnd = Math.min(budgetUnits, text.length);
	const region = text.slice(0, hardEnd);
	let splitAt = region.lastIndexOf("\n");
	if (splitAt < Math.floor(hardEnd / 2)) {
		splitAt = region.lastIndexOf(" ");
	}
	if (splitAt < Math.floor(hardEnd / 2)) splitAt = region.lastIndexOf("\t");
	if (splitAt < 1) splitAt = hardEnd;
	// Inline-code walkback: odd count of unescaped backticks before the cut
	// means the cut lands inside a span — move before the last unescaped
	// backtick (base.py inline-code guard).
	const candidate = text.slice(0, splitAt);
	const unescapedBackticks: number[] = [];
	for (let i = 0; i < candidate.length; i++) {
		if (candidate[i] !== "`" || candidate[i - 1] === "\\") continue;
		unescapedBackticks.push(i);
	}
	if (unescapedBackticks.length % 2 === 1) {
		const lastBt = unescapedBackticks[unescapedBackticks.length - 1] ?? 0;
		if (lastBt > 0) {
			const safeSpace = candidate.lastIndexOf(" ", lastBt);
			const safeNl = candidate.lastIndexOf("\n", lastBt);
			const safe = Math.max(safeSpace, safeNl);
			if (safe > Math.floor(hardEnd / 4)) splitAt = safe;
		}
	}
	// BYTE-EXACT: separator rides with the LEFT part; nothing stripped from
	// the remainder. Hermes' truncate_message lstrips the remainder (dropping
	// the separator byte); the primitive this module consumes is
	// byte-preserving by contract (proposed DEC entry).
	let cut = Math.min(text.length, splitAt + 1);
	cut = snapOffSurrogateTrail(text, cut);
	return Math.max(1, cut);
}

/** Never cut between the halves of a UTF-16 surrogate pair. */
export function snapOffSurrogateTrail(text: string, pos: number): number {
	if (pos <= 0 || pos >= text.length) return pos;
	const prev = text.charCodeAt(pos - 1);
	if (prev >= 0xd800 && prev <= 0xdbff) {
		const next = text.charCodeAt(pos);
		if (next >= 0xdc00 && next <= 0xdfff) return pos - 1;
	}
	return pos;
}

function langOf(fenceLine: string | undefined): string {
	if (!fenceLine) return "";
	const tag = fenceLine.replace(/^\s*```/, "").trim();
	return tag.split(/\s+/)[0] ?? "";
}

// ── labels ───────────────────────────────────────────────────────────────────

/**
 * Append "(i/n)" to every chunk once the count is final. Placement rules:
 *   - an indicator NEVER shares a closing-fence line — after one it moves to
 *     its own line immediately below (_separate_chunk_indicator_from_fence);
 *   - a chunk already ending in whitespace takes the BARE label (no doubled
 *     separator); otherwise exactly one space joins it.
 * (Inlined into chunkWithFenceCarry so each chunk's labelJoinLen lands in the
 * plan's scaffold record — the exact-inverse key.)
 */

/**
 * Exact inverse of chunkWithFenceCarry: remove labels, then exactly the
 * synthesized bytes recorded in the plan's scaffold — restoring the ORIGINAL
 * content bytes. Not a general markdown stripper.
 */
export function stripChunkScaffolding(plan: ChunkPlan): string {
	const { chunks, scaffold } = plan;
	if (chunks.length <= 1) return chunks[0] ?? "";
	const n = chunks.length;
	const parts = chunks.map((chunk, idx) => {
		const rec = scaffold[idx];
		if (rec === undefined)
			throw new Error(`chunk ${idx + 1}/${n} has no scaffold record`);
		const label = computeChunkLabel(idx + 1, n);
		if (!chunk.endsWith(label))
			throw new Error(`chunk ${idx + 1}/${n} is missing its indicator`);
		let text = chunk.slice(0, -label.length - rec.labelJoinLen);
		if (rec.closeAdded) text = text.slice(0, -FENCE_CLOSE.length);
		if (rec.prefixLen > 0) text = text.slice(rec.prefixLen);
		return text;
	});
	return parts.join("");
}
