// WhatsApp markup conversion (wa_common.py:WhatsAppBehaviorMixin.format_message
// parity): byte-exact dialect swaps with fence/code protection.

import { describe, expect, it } from "vitest";

import { toWhatsappMarkup } from "./wa-markdown.js";

describe("WhatsApp-flavored markdown conversion", () => {
	it("bold/italic/strike dialect swaps", () => {
		expect(toWhatsappMarkup("**bold** text")).toBe("*bold* text");
		expect(toWhatsappMarkup("__bold__ too")).toBe("*bold* too");
		expect(toWhatsappMarkup("~~struck~~ out")).toBe("~struck~ out");
		// _italic_ is ALREADY WhatsApp syntax — untouched.
		expect(toWhatsappMarkup("_calm_ italic")).toBe("_calm_ italic");
	});

	it("single-* emphasis converts to _italic_ BEFORE bold (format_message parity)", () => {
		// *hi* alone would render BOLD on WhatsApp; the italic pass runs first
		// so standard-Markdown emphasis lands as _hi_.
		expect(toWhatsappMarkup("*hi* there")).toBe("_hi_ there");
		expect(toWhatsappMarkup("**bold** and *italic*")).toBe(
			"*bold* and _italic_",
		);
		// Lookarounds: list bullets and bold delimiters never convert.
		expect(toWhatsappMarkup("* item one\n* item two")).toBe(
			"* item one\n* item two",
		);
		expect(toWhatsappMarkup("***both***")).toBe("**both**");
	});

	it("headers become bold lines; pre-wrapped bold collapses (Hermes parity)", () => {
		expect(toWhatsappMarkup("# Title")).toBe("*Title*");
		// Hermes _header_to_bold strips whole-wrap asterisk pairs only:
		// "# **Title**" → inner "**Title**" → strip ×2 → "Title" → "*Title*".
		expect(toWhatsappMarkup("# **Title**")).toBe("*Title*");
		// INNER emphasis is left nested (format_message parity — zero divergence).
		expect(toWhatsappMarkup("### Deep **Title** here")).toBe(
			"*Deep *Title* here*",
		);
	});

	it("markdown links flatten to 'text (url)'", () => {
		expect(toWhatsappMarkup("[docs](https://x.y)")).toBe("docs (https://x.y)");
	});

	it("STEP 0 outbound sanitization: invisible chars stripped, odd spaces normalized (format_message parity)", () => {
		// U+200B/U+2060/U+2063/U+FEFF removed verbatim.
		expect(toWhatsappMarkup("a\u200bb\u2060c\u2063d\ufeffe")).toBe("abcde");
		// U+00A0/U+2003/U+3000 etc. normalize to plain ASCII space.
		expect(toWhatsappMarkup("x\u00a0y\u2003z\u3000w")).toBe("x y z w");
		// Emoji joiners (U+200D) are KEPT — only format chars go.
		expect(toWhatsappMarkup("emoji \u200d joiners kept")).toBe(
			"emoji \u200d joiners kept",
		);
		// Sanitization runs BEFORE protection: dirty bytes inside fences are
		// cleaned too (Hermes sanitizes before the fence placeholder pass).
		expect(toWhatsappMarkup("```\na\u200bb\n```")).toBe("```\nab\n```");
	});

	it("fenced code blocks and inline code survive VERBATIM", () => {
		const fenced = "```\n**not bold** # not header\n```";
		expect(toWhatsappMarkup(fenced)).toBe(fenced);
		expect(toWhatsappMarkup("use `npm **install**` now")).toBe(
			"use `npm **install**` now",
		);
	});

	it("conversion is deterministic (same input → same bytes)", () => {
		const input = "# H\n\n**b** _i_ ~~s~~ [l](u)\n```\ncode\n```";
		expect(toWhatsappMarkup(input)).toEqual(toWhatsappMarkup(input));
	});
});
