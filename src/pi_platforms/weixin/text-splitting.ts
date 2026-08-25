// pi_platforms/weixin/text-splitting — the WeChat copy-friendly delivery
// splitter, ported from Hermes gateway/platforms/weixin.py (module-level
// helpers). Production send() routes long-form content through THIS pipeline;
// the conformance surface inherits the shared kit chunk pipeline per roadmap
// Phase-6 heuristic 2 (shape deltas written fresh, fixtures inherited).
//
// Hermes anchors:
//   weixin.py:_normalize_markdown_blocks      — blank-run collapse, fence-aware
//   weixin.py:_wrap_copy_friendly_lines_for_weixin — 120-col wrap outside
//     fences/tables (WEIXIN_COPY_LINE_WIDTH)
//   weixin.py:_split_markdown_blocks          — fenced/blank-line block split
//   weixin.py:_split_delivery_units_for_weixin — top-level lines → units,
//     indented continuations attached
//   weixin.py:_looks_like_chatty_line_for_weixin / _looks_like_heading_line_for_weixin /
//     _should_split_short_chat_block_for_weixin — chat-bubble heuristic
//   weixin.py:_pack_markdown_blocks_for_weixin / _split_text_for_weixin_delivery

import { WEIXIN_COPY_LINE_WIDTH } from "./manifest.js";

const HEADER_RE = /^(#{1,6})\s+(.+?)\s*$/;
const TABLE_RULE_RE = /^\s*\|?(?:\s*:?-{3,}:?\s*\|)+\s*:?-{3,}:?\s*\|?\s*$/;
const FENCE_RE = /^```([^\n`]*)\s*$/;

function isFenceLine(stripped: string): boolean {
	return FENCE_RE.test(stripped);
}

/** Blank-run collapse outside code fences (weixin.py:_normalize_markdown_blocks). */
export function normalizeMarkdownBlocks(content: string): string {
	const result: string[] = [];
	let inCodeBlock = false;
	let blankRun = 0;
	for (const rawLine of content.split("\n")) {
		const line = rawLine.replace(/\s+$/, "");
		if (isFenceLine(line.trim())) {
			inCodeBlock = !inCodeBlock;
			result.push(line);
			blankRun = 0;
			continue;
		}
		if (inCodeBlock) {
			result.push(line);
			continue;
		}
		if (line.trim() === "") {
			blankRun += 1;
			if (blankRun <= 1) result.push("");
			continue;
		}
		blankRun = 0;
		result.push(line);
	}
	return result.join("\n").trim();
}

/**
 * Wrap long display lines that are hard to copy in WeChat clients
 * (weixin.py:_wrap_copy_friendly_lines_for_weixin): never inside fences,
 * tables, or empty lines; break_long_words OFF, break_on_hyphens OFF.
 */
export function wrapCopyFriendlyLines(
	content: string,
	width = WEIXIN_COPY_LINE_WIDTH,
): string {
	if (!content) return content;
	const wrapped: string[] = [];
	let inCodeBlock = false;
	for (const rawLine of content.split("\n")) {
		const line = rawLine.replace(/\s+$/, "");
		const stripped = line.trim();
		if (isFenceLine(stripped)) {
			inCodeBlock = !inCodeBlock;
			wrapped.push(line);
			continue;
		}
		if (
			inCodeBlock ||
			line.length <= width ||
			stripped === "" ||
			stripped.startsWith("|") ||
			TABLE_RULE_RE.test(stripped)
		) {
			wrapped.push(line);
			continue;
		}
		// Greedy word wrap preserving whitespace semantics.
		const words = line.split(/(\s+)/);
		let current = "";
		for (const piece of words) {
			if (current.length + piece.length > width && current.trim() !== "") {
				wrapped.push(current);
				current = piece.startsWith(" ") || piece.startsWith("\t") ? "" : piece;
				if (piece.trim() !== "") current = current === "" ? piece : piece;
				continue;
			}
			current += piece;
		}
		wrapped.push(current);
	}
	return wrapped.join("\n").trim();
}

/** Split into markdown blocks on blanks + fence boundaries (_split_markdown_blocks). */
export function splitMarkdownBlocks(content: string): string[] {
	if (!content) return [];
	const blocks: string[] = [];
	const current: string[] = [];
	let inCodeBlock = false;
	for (const rawLine of content.split("\n")) {
		const line = rawLine.replace(/\s+$/, "");
		if (isFenceLine(line.trim())) {
			if (!inCodeBlock && current.length > 0) {
				blocks.push(current.join("\n").trim());
				current.length = 0;
			}
			current.push(line);
			inCodeBlock = !inCodeBlock;
			if (!inCodeBlock) {
				blocks.push(current.join("\n").trim());
				current.length = 0;
			}
			continue;
		}
		if (inCodeBlock) {
			current.push(line);
			continue;
		}
		if (line.trim() === "") {
			if (current.length > 0) {
				blocks.push(current.join("\n").trim());
				current.length = 0;
			}
			continue;
		}
		current.push(line);
	}
	if (current.length > 0) blocks.push(current.join("\n").trim());
	return blocks.filter((b) => b !== "");
}

/**
 * Top-level line breaks become separate delivery units; indented continuation
 * lines stay attached to the previous unit (_split_delivery_units_for_weixin).
 */
export function splitDeliveryUnits(content: string): string[] {
	const units: string[] = [];
	for (const block of splitMarkdownBlocks(content)) {
		if (isFenceLine(block.split("\n")[0]!.trim())) {
			units.push(block);
			continue;
		}
		let current: string[] = [];
		for (const rawLine of block.split("\n")) {
			const line = rawLine.replace(/\s+$/, "");
			if (line.trim() === "") {
				if (current.length > 0) {
					units.push(current.join("\n").trim());
					current = [];
				}
				continue;
			}
			const isContinuation = current.length > 0 && /^[\t ]/.test(rawLine);
			if (isContinuation) {
				current.push(line);
				continue;
			}
			if (current.length > 0) units.push(current.join("\n").trim());
			current = [line];
		}
		if (current.length > 0) units.push(current.join("\n").trim());
	}
	return units.filter((u) => u !== "");
}

/** Chat-like standalone utterance heuristic (weixin.py:_looks_like_chatty_line…). */
export function looksLikeChattyLine(line: string): boolean {
	const stripped = line.trim();
	if (stripped === "") return false;
	if (stripped.length > 48) return false;
	if (/^[\t ]/.test(line)) return false;
	if (/^[>*【#|]/.test(stripped)) return false;
	if (TABLE_RULE_RE.test(stripped)) return false;
	if (/^\*\*[^*]+\*\*$/.test(stripped)) return false;
	if (/^\d+\.\s/.test(stripped)) return false;
	return true;
}

/** Short heading-shaped line heuristic (weixin.py:_looks_like_heading_line…). */
export function looksLikeHeadingLine(line: string): boolean {
	const stripped = line.trim();
	if (stripped === "") return false;
	if (HEADER_RE.test(stripped)) return true;
	return stripped.length <= 24 && /[：:]$/.test(stripped);
}

/** Only chat-like 2..6-line blocks split into bubbles (_should_split_short…). */
export function shouldSplitShortChatBlock(block: string): boolean {
	const lines = block.split("\n").filter((l) => l.trim() !== "");
	if (lines.length < 2 || lines.length > 6) return false;
	const first = lines[0] ?? "";
	if (looksLikeHeadingLine(first)) return false;
	return lines.every((l) => looksLikeChattyLine(l));
}

/** Greedy pack of blocks under a length budget.
 *
 * Hermes anchor (_pack_markdown_blocks_for_weixin): a block that ALONE exceeds
 * the budget goes to the OVERFLOW callable — base.py truncate_message, the
 * newline-preferred fence-carrying chunker with "(i/n)" indicators — never a
 * mid-line hard slice. Callers that omit `overflow` keep the legacy
 * slice-per-maxLength fallback (pure data contracts in tests).
 */
export function packMarkdownBlocks(
	content: string,
	maxLength: number,
	overflow?: ((block: string) => string[]) | undefined,
): string[] {
	if (content.length <= maxLength) return [content];
	const packed: string[] = [];
	let current = "";
	for (const block of splitMarkdownBlocks(content)) {
		if (block.length > maxLength) {
			if (current.trim() !== "") {
				packed.push(current.trimEnd());
				current = "";
			}
			if (overflow !== undefined) {
				packed.push(...overflow(block));
			} else {
				for (let i = 0; i < block.length; i += maxLength) {
					packed.push(block.slice(i, i + maxLength));
				}
			}
			continue;
		}
		if (current === "") current = block;
		else if (current.length + 2 + block.length <= maxLength) {
			current += `\n\n${block}`;
		} else {
			packed.push(current.trimEnd());
			current = block;
		}
	}
	if (current.trim() !== "") packed.push(current.trimEnd());
	return packed;
}

/**
 * THE splitter (weixin.py:_split_text_for_weixin_delivery):
 * compact mode keeps one message under budget UNLESS the content is a short
 * chatty exchange (bubble split); per_line mode makes every top-level unit a
 * separate message; oversized units use block-aware packing.
 */
export function splitTextForWeixinDelivery(
	content: string,
	maxLength: number,
	splitPerLine = false,
	opts?: { overflow?: ((block: string) => string[]) | undefined } | undefined,
): string[] {
	const overflow = opts?.overflow;
	if (!content) return [];
	if (splitPerLine) {
		if (content.length <= maxLength && !content.includes("\n"))
			return [content];
		const chunks: string[] = [];
		for (const unit of splitDeliveryUnits(content)) {
			if (unit.length <= maxLength) {
				chunks.push(unit);
				continue;
			}
			chunks.push(...packMarkdownBlocks(unit, maxLength, overflow));
		}
		const filtered = chunks.filter((c) => c !== "");
		return filtered.length > 0 ? filtered : [content];
	}
	// Compact default: single message under budget unless chatty-multiline.
	if (content.length <= maxLength) {
		if (shouldSplitShortChatBlock(normalizeMarkdownBlocks(content))) {
			return splitDeliveryUnits(content).filter((u) => u !== "");
		}
		return [content];
	}
	const packed = packMarkdownBlocks(content, maxLength, overflow);
	return packed.length > 0 ? packed : [content];
}
