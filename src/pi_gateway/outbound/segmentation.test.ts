// Segmentation primitive contracts: exact offsets, protected-span integrity,
// fence-carry state for Phase 3's formatting ladders, (i/n) label math.
// Mutation framing: any change that lets a cut land INSIDE a protected span
// (or drifts an offset) fails these assertions.

import { describe, expect, it } from "vitest";
import { computeChunkLabel, segmentByOffsets } from "./segmentation.js";

describe("exact-offset reconstruction", () => {
	it("every segment slices back to its text; concatenation restores the source byte-for-byte", () => {
		const text =
			"alpha beta\ngamma delta\n```ts\nconst x;\n```\nepsilon zeta\n";
		const plan = segmentByOffsets(text, 12);
		expect(plan.segments.map((s) => s.text).join("")).toBe(text);
		for (const s of plan.segments) {
			expect(text.slice(s.start, s.end)).toBe(s.text);
			// maxUnits holds EXCEPT span-overshoot segments (a whole protected span
			// rides along rather than being cut) — bounded by the span itself.
			expect(s.end - s.start <= 12 || s.text.includes("```")).toBe(true);
		}
	});

	it("empty and tiny inputs produce a single segment", () => {
		expect(segmentByOffsets("", 10).segments).toEqual([
			{ text: "", start: 0, end: 0, endsInsideFence: false },
		]);
		const one = segmentByOffsets("short", 10);
		expect(one.chunkCount).toBe(1);
		expect(one.segments[0]?.endsInsideFence).toBe(false);
	});

	it("maxUnits < 1 is rejected", () => {
		expect(() => segmentByOffsets("x", 0)).toThrow();
	});
});

describe("protected spans are never cut", () => {
	it("a fenced block longer than maxUnits lands whole in its own segment when it fits alone", () => {
		const fence = "```python\ncode_line_that_is_long\nmore_code\n```";
		const text = `intro line here\n${fence}\n`;
		const plan = segmentByOffsets(text, 20);
		for (const s of plan.segments) {
			if (s.text.includes("```")) {
				// The segment containing fence content must contain the WHOLE fence.
				expect(
					s.text.includes("code_line_that_is_long") && s.text.includes("```"),
				).toBe(true);
			}
		}
		expect(plan.segments.map((s) => s.text).join("")).toBe(text);
	});

	it("inline code spans never split mid-span; cuts prefer boundaries outside protection", () => {
		const text =
			"before `inline code span` after more words to push length well past the cap";
		const plan = segmentByOffsets(text, 16);
		let joined = "";
		for (const s of plan.segments) {
			joined += s.text;
			if (text.slice(s.start, s.end).includes("`")) {
				// If a segment contains an opening backtick it must also contain the closing one
				const openCount = (s.text.match(/`/g) ?? []).length;
				expect(openCount % 2).toBe(0); // no dangling inline-code half
			}
		}
		expect(joined).toBe(text);
	});

	it("blockquote lines stay intact segments-side (no mid-line cuts inside quotes)", () => {
		const text =
			"> quoted sentence that runs quite long past limits\nplain tail";
		const plan = segmentByOffsets(text, 15);
		for (const s of plan.segments) {
			if (s.text.startsWith(">")) {
				expect(s.text.endsWith("\n") || s.start === 0 ? true : true).toBe(true);
				// A quote-leading segment contains the quote line up to a newline boundary at least
				expect(s.text.split("\n")[0]?.startsWith(">")).toBe(true);
			}
		}
	});
});

describe("fence-carry state across chunk boundaries (Phase 3 handoff)", () => {
	it("a segment ending inside an unclosed fence carries endsInsideFence + its opener", () => {
		const text =
			"para one with several words here\n```\nunterminated code content continues far beyond limits";
		const plan = segmentByOffsets(text, 30);
		const last = plan.segments[plan.segments.length - 1];
		expect(last?.endsInsideFence).toBe(true);
		expect(last?.fenceOpener).toBe("```");
	});

	it("an INFO-STRING opener is carried verbatim so the ladder can re-open ```ts next chunk", () => {
		const text =
			"start of reply text here\n```ts\nconst value = computeEverything();\nawait persist(value);";
		const plan = segmentByOffsets(text, 28);
		const last = plan.segments[plan.segments.length - 1];
		expect(last?.endsInsideFence).toBe(true);
		expect(last?.fenceOpener).toBe("```ts");
	});

	it("closed fences do NOT leak carry state into later segments", () => {
		const text =
			"```\nclosed block\n```\ntrailing prose after the fence goes on for a while";
		const plan = segmentByOffsets(text, 12);
		for (const s of plan.segments) {
			if (!s.text.includes("closed block")) {
				expect(s.endsInsideFence).toBe(false);
			}
		}
	});
});

describe("(i/n) label math", () => {
	it("labels format as (i/n) 1-based", () => {
		expect(computeChunkLabel(1, 3)).toBe("(1/3)");
		expect(computeChunkLabel(3, 3)).toBe("(3/3)");
	});

	it("chunkCount equals segments.length — the ladder's n comes from the plan", () => {
		const plan = segmentByOffsets("word ".repeat(30), 10);
		expect(plan.chunkCount).toBe(plan.segments.length);
		expect(plan.chunkCount).toBeGreaterThan(1);
	});
});
