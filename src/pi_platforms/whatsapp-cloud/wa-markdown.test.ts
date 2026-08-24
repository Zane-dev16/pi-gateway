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
