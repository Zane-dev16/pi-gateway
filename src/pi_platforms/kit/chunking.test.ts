// Chunking contracts (04 §6.2): (i/n) math, fence close/reopen carry,
// per-chat UTF-16 budget math via THE chat resolution pair, astral split
// safety, degenerate-budget progress guarantee, indicator/fence separation.
// Round-trips are BYTE-EXACT: stripping scaffolding restores the source.

import { describe, expect, it } from "vitest";
import {
	INDICATOR_RESERVE,
	chunkWithFenceCarry,
	snapOffSurrogateTrail,
	stripChunkScaffolding,
} from "./chunking.js";
import {
	codePointLen,
	resolveChatLengthPolicy,
	utf16Len,
} from "./length-policy.js";

function chars(chatId: string, maxUnits: number) {
	return resolveChatLengthPolicy({
		chatId,
		unit: "chars",
		scalarMaxUnits: maxUnits,
	});
}

describe("(i/n) indicator math", () => {
	it("single-chunk passthrough when content fits — no label ever", () => {
		const text = "x".repeat(200);
		const plan = chunkWithFenceCarry(text, chars("dm1", 4096));
		expect(plan.chunkCount).toBe(1);
		expect(plan.chunks[0]).toBe(text); // byte-identical, untouched
	});

	it("multi-chunk output labels EVERY chunk (i/n) and stays within budget", () => {
		const text = Array.from({ length: 40 }, (_, i) => `line ${i} filler`).join(
			"\n",
		);
		const policy = chars("dm1", 64);
		const plan = chunkWithFenceCarry(text, policy);
		expect(plan.chunkCount).toBeGreaterThan(1);
		plan.chunks.forEach((chunk, i) => {
			expect(chunk.endsWith(`(${i + 1}/${plan.chunkCount})`)).toBe(true);
			expect(utf16Len(chunk)).toBeLessThanOrEqual(policy.maxUnits);
		});
	});

	it(`planning reserves ${INDICATOR_RESERVE} headroom before splitting (labels never overflow the cap)`, () => {
		// Content sized so naive maxUnits-splitting would leave no room for " (1/2)".
		const text = `${"a".repeat(58)} ${"b".repeat(58)}`;
		const policy = chars("c", 64);
		const plan = chunkWithFenceCarry(text, policy);
		expect(plan.chunkCount).toBeGreaterThanOrEqual(2);
		for (const chunk of plan.chunks) {
			expect(utf16Len(chunk)).toBeLessThanOrEqual(policy.maxUnits);
		}
	});
});

describe("fence-carry correctness", () => {
	it("a chunk ending mid-fenced-block closes it; the next reopens with the ORIGINAL language tag", () => {
		const codeLines = Array.from(
			{ length: 12 },
			(_, i) => `console.log(${i});`,
		);
		const text = `\`\`\`ts\n${codeLines.join("\n")}\n\`\`\`\ndone`;
		const policy = chars("dm2", 60);
		const plan = chunkWithFenceCarry(text, policy);
		expect(plan.chunkCount).toBeGreaterThan(1);

		for (const chunk of plan.chunks) {
			let openFences = 0;
			for (const line of chunk.split("\n")) {
				const stripped = line.trim();
				if (!stripped.startsWith("```") || /^\(\d+\/\d+\)$/.test(stripped))
					continue;
				if (stripped === "```") openFences -= 1;
				else openFences += 1;
			}
			// Every wire chunk is FENCE-BALANCED on its own.
			expect(openFences).toBe(0);
		}
		// The carried reopen preserves the language tag somewhere after chunk 1.
		const reopen = plan.chunks.slice(1).find((c) => c.startsWith("```ts"));
		expect(reopen).toBeDefined();
	});

	it("an UNCLOSED fenced tail closes in the final chunk with no dangling opener", () => {
		const text = "intro\n```\nstill inside the block without a closing fence";
		const plan = chunkWithFenceCarry(text, chars("dm3", 30));
		const last = plan.chunks[plan.chunkCount - 1] as string;
		// Fence closes, and the indicator sits on its OWN line below it.
		expect(last).toMatch(/```\n\(\d+\/\d+\)$/);
		for (const chunk of plan.chunks) {
			let inside = false;
			const netOpens = 0;
			for (const line of chunk.split("\n")) {
				const s = line.trim();
				if (!s.startsWith("```") || /^\(\d+\/\d+\)$/.test(s)) continue;
				inside = !inside;
			}
			// Stateful walk must land OUTSIDE every fence at each chunk end.
			expect(inside).toBe(false);
			void netOpens;
		}
	});

	it("a fenced block longer than the budget splits across chunks WITH carry (base.py line-walk parity)", () => {
		const longBlock = [
			"```python",
			...Array.from({ length: 20 }, (_, i) => `x_${i} = ${i}`),
			"```",
		].join("\n");
		const policy = chars("dm4", 48);
		const plan = chunkWithFenceCarry(longBlock, policy);
		expect(plan.chunkCount).toBeGreaterThan(1);
		for (const chunk of plan.chunks) {
			expect(utf16Len(chunk)).toBeLessThanOrEqual(policy.maxUnits);
			let balance = 0;
			for (const line of chunk.split("\n")) {
				const s = line.trim();
				if (!s.startsWith("```") || /^\(\d+\/\d+\)$/.test(s)) continue;
				balance += s === "```" ? -1 : 1;
			}
			expect(balance).toBe(0);
		}
		expect(stripChunkScaffolding(plan)).toBe(longBlock);
	});
});

describe("byte-exact round trip", () => {
	it("stripping scaffolding restores the original bytes for prose + fences + inline code", () => {
		const text = [
			"prose paragraph one with several words to fill budget",
			"prose two with `inline code span` embedded mid-sentence here",
			"```js",
			"const a = 1;",
			"const b = 2;",
			"```",
			"tail prose after the block, long enough to force more chunks.",
		].join("\n");
		for (const budget of [40, 64, 96, 128]) {
			const plan = chunkWithFenceCarry(text, chars("rt", budget));
			if (plan.chunkCount === 1) continue;
			expect(stripChunkScaffolding(plan)).toBe(text);
		}
	});
});

describe("per-chat length pair (§6.3 / A15)", () => {
	it("UTF-16 platform proven by CODE-UNIT math: astral chars count double, chunks respect utf16 units", () => {
		// Each 🎉 is ONE codepoint but TWO UTF-16 units. Codepoint math would
		// over-fill Telegram-style caps; the pair must split by code units.
		const text = "🎉".repeat(50); // 50 cp / 100 utf16 units
		const policy = resolveChatLengthPolicy({
			chatId: "tg-chat",
			unit: "utf16",
			scalarMaxUnits: 30,
		});
		expect(policy.lenFn).toBe(utf16Len);
		const plan = chunkWithFenceCarry(text, policy);
		expect(plan.chunkCount).toBeGreaterThan(1);
		for (const chunk of plan.chunks) {
			expect(utf16Len(chunk)).toBeLessThanOrEqual(30);
		}
		// No surrogate pair may be split: scan every high surrogate — its low
		// half must follow within the same chunk (labels are ASCII so they never
		// confuse the pairing walk).
		const joined = plan.chunks.map((c) => c.replace(/\s?\(\d+\/\d+\)$/, ""));
		for (const chunk of joined) {
			for (let i = 0; i < chunk.length; i++) {
				const code = chunk.charCodeAt(i);
				if (code >= 0xd800 && code <= 0xdbff) {
					const next = chunk.charCodeAt(i + 1);
					expect(next >= 0xdc00 && next <= 0xdfff).toBe(true);
				}
			}
		}
		expect(stripChunkScaffolding(plan)).toBe(text);
	});

	it("budget AND unit come from ONE resolution — an override moves both together", () => {
		// Relay-shaped descriptor: per-chat cap AND unit upgrade together; the
		// chunker only accepts the resolved policy object (no scalar call site).
		const policy = resolveChatLengthPolicy({
			chatId: "relay:telegram:123",
			unit: "chars",
			descriptor: { maxMessageLength: 2000, lenUnit: "utf16" },
		});
		expect(policy.unit).toBe("utf16");
		expect(policy.maxUnits).toBe(2000);
		const slackPolicy = resolveChatLengthPolicy({
			chatId: "relay:slack:C1",
			unit: "chars",
			descriptor: { maxMessageLength: 39000, lenUnit: "chars" },
		});
		expect(slackPolicy.unit).toBe("chars");
		expect(slackPolicy.maxUnits).toBe(39000);
	});
});

describe("astral-char split safety", () => {
	it("hard cuts snap off trailing high surrogates — pairs are never severed", () => {
		const text = "🎉".repeat(10);
		for (let pos = 0; pos <= text.length; pos++) {
			const snapped = snapOffSurrogateTrail(text, pos);
			const left = text.slice(0, snapped);
			// Left side must end on a codepoint boundary.
			expect(left.length * 2).toBe(utf16Len(left) * 2);
			expect(codePointLen(left) * 2).toBe(utf16Len(left));
		}
	});

	it("degenerate tiny budgets still make progress and never spin (floor contract)", () => {
		const text = "🎉".repeat(6) + "tail";
		const plan = chunkWithFenceCarry(text, chars("tiny", 1));
		expect(plan.chunkCount).toBeGreaterThanOrEqual(2);
		expect(plan.chunks.length).toBeLessThan(200); // bounded, not spinning
		expect(stripChunkScaffolding(plan)).toBe(text);
	});
});

describe("indicator placement vs fences", () => {
	it("(i/n) never shares a closing-fence line (indicator relocated below)", () => {
		const text = [
			"lead prose that fills some space for the splitter to work with",
			"```",
			...Array.from({ length: 14 }, (_, i) => `row ${i}`),
			// unclosed on purpose → final chunk ends with a synthesized close
		].join("\n");
		const plan = chunkWithFenceCarry(text, chars("ind", 44));
		expect(plan.chunkCount).toBeGreaterThan(1);
		for (const chunk of plan.chunks) {
			for (const line of chunk.split("\n")) {
				if (/^\(\d+\/\d+\)$/.test(line.trim())) {
					expect(line.trim()).toMatch(/^\(\d+\/\d+\)$/); // own line, bare
					expect(line.startsWith("```")).toBe(false);
				}
			}
			// And no "``` (i/n)" single-line composite anywhere.
			expect(chunk).not.toMatch(/``` \(\d+\/\d+\)/);
		}
	});
});
