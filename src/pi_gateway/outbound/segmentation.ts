// pi_gateway/outbound/segmentation.ts — OFFSET-SAFE text segmentation over
// protection spans. This is the PRIMITIVE layer only: it decides WHERE text
// can be split without ever cutting a protected span (fenced code blocks,
// inline code, blockquotes) and carries fence state across boundaries.
//
// PHASE-3 HANDOFF (03 §9 / roadmap item "chunk/fence-carry splitting"):
// the (i/n) chunk math WITH fence-carry rendering — i.e. closing an open
// fence at a chunk end and RE-OPENING it (with its info string) at the next
// chunk start — lives with the formatting ladders in Phase 3. This module
// gives them everything they need to do that correctly:
//   - segments[] with EXACT source offsets [start, end) into the original;
//   - per-segment fence state (opensWith/closesWith + carried info string);
//   - computeChunkLabel(i, n) for the "(i/n)" markers.
// Ladders must RENDER from segments (never re-split rendered output), so
// byte offsets stay truthful end-to-end.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   base.py:_mask_protected_spans span detection → collectProtectedSpans (media-grammar)
//   gateway/delivery.py:MAX_PLATFORM_OUTPUT cap  → callers pass their own cap

import { collectProtectedSpans } from "./media-grammar.js";

export interface Segment {
	/** Exact source slice (text.slice(start, end) === text). */
	text: string;
	start: number;
	end: number;
	/** True when this segment ENDS inside an unclosed fenced block. */
	endsInsideFence: boolean;
	/** The opening fence line (e.g. "```ts") to RE-OPEN at the next segment when endsInsideFence (Phase 3 ladder input). */
	fenceOpener?: string;
}

export interface SegmentationPlan {
	segments: Segment[];
	chunkCount: number;
}

/**
 * Split `text` into segments of at most `maxUnits` UTF-16 code units,
 * preferring whitespace/newline boundaries OUTSIDE protected spans; never
 * splits INSIDE a protected span unless that span alone exceeds maxUnits
 * (then the oversized span is hard-split and its fence state is carried).
 *
 * Pure: same input ⇒ same plan; offsets are exact against `text`.
 */
export function segmentByOffsets(
	text: string,
	maxUnits: number,
): SegmentationPlan {
	if (maxUnits < 1) throw new Error("segmentByOffsets: maxUnits must be >= 1");
	const segments: Segment[] = [];
	if (text.length === 0) {
		segments.push({ text: "", start: 0, end: 0, endsInsideFence: false });
		return { segments, chunkCount: segments.length };
	}

	// Closed protected spans (fences/inline code/quotes) PLUS the unclosed-
	// fence tail as one protected region — a cut may never land inside either.
	const spans = mergeSorted([
		...collectProtectedSpans(text),
		...unclosedFenceTail(text),
	]);

	let cursor = 0;
	while (cursor < text.length) {
		const hardEnd = Math.min(cursor + maxUnits, text.length);
		if (hardEnd >= text.length) {
			emit(cursor, text.length);
			break;
		}
		const cut = chooseCut(text, cursor, hardEnd, spans);
		emit(cursor, cut);
		cursor = cut;
	}

	function emit(start: number, end: number): void {
		const { insideFence, opener } = fenceStateAt(text, end - 1);
		// Any segment ending inside an UNCLOSED fence carries the state — even
		// one that opened its own fence — because the block does NOT close in
		// the source; the rendering ladder must append a closing marker.
		finishSegmentInto(
			segments,
			text,
			start,
			end,
			insideFence ? opener : undefined,
			insideFence,
		);
	}

	return { segments, chunkCount: segments.length };
}

function finishSegmentInto(
	out: Segment[],
	text: string,
	start: number,
	end: number,
	carriedOpener: string | undefined,
	endsInsideFence: boolean,
): void {
	const seg: Segment = {
		text: text.slice(start, end),
		start,
		end,
		endsInsideFence,
	};
	if (carriedOpener !== undefined) seg.fenceOpener = carriedOpener;
	out.push(seg);
}

function mergeSorted(spans: Array<[number, number]>): Array<[number, number]> {
	const sorted = [...spans].sort((a, b) => a[0] - b[0]);
	const merged: Array<[number, number]> = [];
	for (const [s, e] of sorted) {
		const last = merged[merged.length - 1];
		if (last && s <= last[1]) last[1] = Math.max(last[1], e);
		else merged.push([s, e]);
	}
	return merged;
}

/**
 * Choose a cut in (cursor, hardEnd]: the LAST soft boundary outside every
 * protected span. When no such boundary exists AND a span crosses hardEnd,
 * OVERSHOOT to the span's end (the span rides whole into this segment) so a
 * cut never lands mid-span. Overshoot is bounded by the span itself.
 */
function chooseCut(
	text: string,
	cursor: number,
	hardEnd: number,
	spans: Array<[number, number]>,
): number {
	for (let pos = hardEnd; pos > cursor; pos--) {
		if (!isProtectedPosition(pos - 1, spans)) {
			const ch = text[pos - 1];
			if (ch === "\n" || ch === " " || ch === "\t") return pos;
		}
	}
	// No soft boundary outside protection in this window: if a span swallows
	// the boundary, ride to its END (never leave a half-span behind).
	const crossing = spanContaining(hardEnd, spans);
	if (crossing !== null)
		return crossing.end ?? crossingEnd(text, spans, crossing);
	void text;
	return hardEnd;
}

function isProtectedPosition(
	pos: number,
	spans: Array<[number, number]>,
): boolean {
	for (const [s, e] of spans) {
		if (pos >= s && pos < e) return true;
		if (s > pos) break;
	}
	return false;
}

/** Span strictly containing `pos` ([s,e) with s < pos < e), else null. */
function spanContaining(
	pos: number,
	spans: Array<[number, number]>,
): { start: number; end?: number } | null {
	for (const [s, e] of spans) {
		if (pos > s && pos < e) return { start: s, end: e };
		if (s >= pos) break;
	}
	return null;
}

function crossingEnd(
	_text: string,
	_spans: Array<[number, number]>,
	crossing: { start: number; end?: number },
): number {
	return crossing.end ?? _text.length;
}

function unclosedFenceTail(text: string): Array<[number, number]> {
	const lines = text.split("\n");
	let offset = 0;
	let depth = 0;
	let openerStart: number | null = null;
	for (const line of lines) {
		const trimmed = line.trimStart();
		if (trimmed.startsWith("```")) {
			if (depth === 0) {
				depth = 1;
				openerStart = offset;
			} else if (/^```$/.test(trimmed)) {
				depth = 0;
				openerStart = null;
			}
		}
		offset += line.length + 1;
	}
	return depth === 1 && openerStart !== null
		? [[openerStart, text.length]]
		: [];
}

/** Fence context just before `pos`: inside an unclosed ``` block, and what line opened it? */
function fenceStateAt(
	text: string,
	pos: number,
): { insideFence: boolean; opener: string | undefined } {
	const lines = text.split("\n");
	let offset = 0;
	let depth = 0;
	let currentOpener: string | undefined;
	for (const line of lines) {
		const trimmed = line.trimStart();
		const lineEnd = offset + line.length; // exclusive
		if (trimmed.startsWith("```")) {
			if (depth === 0) {
				depth = 1;
				currentOpener = trimmed;
			} else if (/^```$/.test(trimmed)) {
				depth = 0;
				currentOpener = undefined;
			}
		}
		if (pos <= lineEnd) {
			return { insideFence: depth === 1, opener: currentOpener };
		}
		offset += line.length + 1;
	}
	return { insideFence: false, opener: undefined };
}

/** The "(i/n)" label primitive for Phase 3 formatting ladders. */
export function computeChunkLabel(
	index1Based: number,
	chunkCount: number,
): string {
	return `(${index1Based}/${chunkCount})`;
}
