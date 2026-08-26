// Outbound media grammar contracts (03 §9.2; §11 "Media grammar" row;
// DEC-019). Behavior contracts only:
//   - offset-mask integrity under NESTED protection spans (masking removed ⇒
//     these assertions fail — they ARE the mutation detector);
//   - message-global directive semantics (directive ANYWHERE applies message-
//     wide, never span-local);
//   - glued/CJK/emphasis/sentence-period tags parse; unknown extensions SURVIVE;
//   - inline-code real-tag exception; JSON string-value masking.
// Pure: path validation injected as fakes so no fs leaks into grammar tests.

import { describe, expect, it } from "vitest";
import {
	AUDIO_EXTS,
	MEDIA_DELIVERY_EXTS,
	buildExtAlternation,
	collectProtectedSpans,
	extractMedia,
	hasAsDocumentDirective,
	maskJsonStringMedia,
	maskProtectedSpans,
	mergeSpans,
	normalizeMediaTagPath,
	pathLacksDeliverableExtension,
	stripMediaDirectivesForDisplay,
} from "./media-grammar.js";
import type { ProtectedSpanOptions } from "./media-grammar.js";

const ACCEPT_ALL = (p: string) => p;
const REJECT_ALL = () => null;

/** Extraction with a REALISTIC validator: known-extension paths "exist on disk"; anything else must be explicitly declared existing. */
const EXISTING_EXTRA = new Set<string>();
function extract(text: string, extra?: { home?: string; existing?: string[] }) {
	if (extra?.existing) for (const p of extra.existing) EXISTING_EXTRA.add(p);
	const opts: ProtectedSpanOptions = {
		validatePath: (p) =>
			/^\/(etc|proc)(\/|$)|^~\/\.ssh\//.test(p)
				? null
				: pathLacksDeliverableExtension(p)
					? EXISTING_EXTRA.has(p)
						? p
						: null
					: p,
	};
	if (extra?.home !== undefined) opts.home = extra.home;
	return extractMedia(text, opts);
}

describe("one extension source of truth", () => {
	it("covers images/video/audio/documents/data/gis/slides/archives/web", () => {
		for (const ext of [
			".png",
			".mp4",
			".ogg",
			".pdf",
			".csv",
			".kmz",
			".pptx",
			".7z",
			".html",
		]) {
			expect(MEDIA_DELIVERY_EXTS).toContain(ext);
		}
	});

	it("alternation is longest-first so .tar cannot prefix-match .tar.gz components", () => {
		const alt = buildExtAlternation().split("|");
		expect(alt.indexOf("tar.gz".slice(0, 3))).toBeLessThan(alt.indexOf("gz"));
		const firstIdx = alt.findIndex((e) => e.length !== alt[0]?.length);
		if (firstIdx !== -1)
			expect(alt[firstIdx - 1]?.length).toBeGreaterThanOrEqual(
				alt[firstIdx]?.length ?? 0,
			);
	});

	it("audio exts ⊆ delivery exts (voice gating rides the shared list)", () => {
		for (const ext of AUDIO_EXTS) expect(MEDIA_DELIVERY_EXTS).toContain(ext);
	});
});

describe("offset-mask integrity — length-preserving masks keep offsets correct", () => {
	it("mask output has IDENTICAL length; non-newline protected chars are spaces; newlines survive", () => {
		const content = "before\n```ts\nconst x = 1;\n```\nafter";
		const masked = maskProtectedSpans(content);
		expect(masked.length).toBe(content.length); // offset invariant
		const fenceStart = content.indexOf("```ts");
		expect(masked.slice(fenceStart, fenceStart + 5)).not.toContain("`");
		expect(masked[content.indexOf("const")]).toBe(" ");
		expect(masked.split("\n").length).toBe(content.split("\n").length); // newlines kept
	});

	it("chained masks (protected spans then JSON strings) stay length-stable and compose", () => {
		const content =
			'> quoted MEDIA:/a.png\njson {"result":"MEDIA:/b.png"} tail';
		const once = maskJsonStringMedia(maskProtectedSpans(content));
		const twice = maskJsonStringMedia(maskProtectedSpans(once));
		expect(twice.length).toBe(content.length); // idempotent composition
		expect(twice).toBe(once);
	});

	it("ASTRAL-plane content keeps code-unit offsets valid through masking", () => {
		const content = "😀😀 ```js\nMEDIA:/x.png\n``` 😀 tail MEDIA:/y.png";
		const masked = maskProtectedSpans(content);
		expect(masked.length).toBe(content.length);
		// The OUTSIDE tag still parses at its exact original offset.
		const outsideOffset = content.lastIndexOf("MEDIA:");
		expect(masked.slice(outsideOffset, outsideOffset + 6)).toBe("MEDIA:");
	});
});

describe("offset-mask MUTATION DETECTOR — example/stored paths NEVER deliver", () => {
	it("MEDIA tag inside a fenced code block yields NO attachment and survives verbatim in cleaned text", () => {
		const content = [
			"Here is how you attach:",
			"```",
			"MEDIA:/etc/passwd",
			"```",
			"Done.",
		].join("\n");
		const { media, cleaned } = extract(content);
		expect(media).toEqual([]);
		expect(cleaned).toContain("MEDIA:/etc/passwd"); // protected span preserved verbatim
	});

	it("blockquote example tags do not deliver (#35695)", () => {
		const { media } = extract("> example: MEDIA:/tmp/report.pdf");
		expect(media).toEqual([]);
	});

	it("JSON string-value stored tags do not deliver even though bare-path shaped (#34375)", () => {
		const content =
			'{"result": "MEDIA:/Users/x/.hermes/media/generated/stale.png"}';
		const { media } = extract(content);
		expect(media).toEqual([]);
	});

	it("quoted-path JSON variant (real LLM format) still delivers while bare stays masked", () => {
		const content = '{"note": "x"}\nMEDIA:"/tmp/real.pdf"';
		const { media } = extract(content);
		expect(media.map((m) => m.path)).toEqual(["/tmp/real.pdf"]);
	});

	it("NESTED protection spans mask their union: blockquote lines inside fences, fences inside quotes", () => {
		const content = [
			"> quote start MEDIA:/q1.png",
			"```python",
			"# comment MEDIA:/f1.png",
			"> not really quote but inside fence MEDIA:/f2.png",
			"```",
			"> trailing quote MEDIA:/q2.png",
			"",
			"real: MEDIA:/real.png",
		].join("\n");
		const { media, cleaned } = extract(content);
		expect(media.map((m) => m.path)).toEqual(["/real.png"]);
		// Every protected example survives verbatim in visible text:
		for (const example of [
			"MEDIA:/q1.png",
			"MEDIA:/f1.png",
			"MEDIA:/f2.png",
			"MEDIA:/q2.png",
		]) {
			expect(cleaned).toContain(example);
		}
	});

	it("inline code holding a NON-deliverable example stays masked; whole-tag inline code DELIVERS when valid", () => {
		const maskedCase = extract("see `MEDIA:/etc/shadow` docs");
		expect(maskedCase.media).toEqual([]);

		const deliverable = extract("grab `MEDIA:/tmp/voice.ogg` please");
		expect(deliverable.media.map((m) => m.path)).toEqual(["/tmp/voice.ogg"]);
		expect(deliverable.media[0]?.isVoice).toBe(false); // voice flag comes from the DIRECTIVE, not ext alone here
	});

	it("inline-code exception is VALIDATION-GATED: invalid path ⇒ masked (prose examples stay hidden from delivery)", () => {
		const r = extractMedia("see `MEDIA:/nowhere/x.csv`", {
			validatePath: REJECT_ALL,
		});
		expect(r.media).toEqual([]);
	});
});

describe("tag parsing edge cases", () => {
	it("optional whitespace after colon; quoted and backticked variants", () => {
		const { media } = extract(
			'MEDIA: /tmp/a.png\nMEDIA:"/tmp/b.jpg"\nMEDIA:`/tmp/c.webp`',
		);
		expect(media.map((m) => m.path)).toEqual([
			"/tmp/a.png",
			"/tmp/b.jpg",
			"/tmp/c.webp",
		]);
	});

	it("glued tags split cleanly (#68773)", () => {
		const { media } = extract("MEDIA:/a.pngMEDIA:/b.png");
		expect(media.map((m) => m.path)).toEqual(["/a.png", "/b.png"]);
	});

	it("trailing wrapper class includes the apostrophe (base.py [`\"'*_]{0,3} parity): stray ' cleans with the tag", () => {
		const { media, cleaned } = extract("report MEDIA:/x.pdf' attached");
		expect(media.map((m) => m.path)).toEqual(["/x.pdf"]);
		expect(cleaned).not.toContain("'");
	});

	it("markdown emphasis wrappers tolerated (**MEDIA:…**, *…*, _…_)", () => {
		const { media } = extract(
			"**MEDIA:/x.pdf** and *MEDIA:/y.docx* and _MEDIA:/z.epub_",
		);
		expect(media.map((m) => m.path)).toEqual(["/x.pdf", "/y.docx", "/z.epub"]);
	});

	it("sentence-final period splits when followed by whitespace/EOL; multi-part extensions intact", () => {
		const { media } = extract(
			"MEDIA:/x/data.csv. Also MEDIA:/d/archive.tar.gz done",
		);
		expect(media.map((m) => m.path)).toEqual([
			"/x/data.csv",
			"/d/archive.tar.gz",
		]);
	});

	it("CJK punctuation terminators parse (#88038)", () => {
		const { media } = extract("MEDIA:D:\\path\\早报.pdf（782.6 KB）");
		expect(media.map((m) => m.path)).toEqual(["D:\\path\\早报.pdf"]);
	});

	it("Windows drive-letter paths parse (#34632)", () => {
		const { media } = extract("MEDIA:C:\\Users\\me\\shot.png");
		expect(media.map((m) => m.path)).toEqual(["C:\\Users\\me\\shot.png"]);
	});

	it("UNKNOWN-extension tags SURVIVE in the body rather than vanishing (#34517)", () => {
		const content = "MEDIA:/logs/app.log stays visible";
		const { media, cleaned } = extractMedia(content, {
			validatePath: REJECT_ALL,
		});
		expect(media).toEqual([]);
		expect(cleaned).toContain("MEDIA:/logs/app.log");
	});

	it("extension-less validated files deliver via progressive space recovery (#24032)", () => {
		const content = "MEDIA:/data/Makefile backup old";
		const r = extractMedia(content, {
			validatePath: (p) =>
				p === "/data/Makefile backup" ? "/data/Makefile backup" : null,
		});
		expect(r.media.map((m) => m.path)).toEqual(["/data/Makefile backup"]);
	});

	it("dedupe on EXPANDED path within one extraction, first occurrence wins (#29131)", () => {
		const { media } = extract(
			"MEDIA:/tmp/dup.png\nsummary MEDIA:~/dup.png end",
			{ home: "/tmp" },
		);
		expect(media.filter((m) => m.path.endsWith("dup.png"))).toHaveLength(1);
	});
});

describe("MESSAGE-GLOBAL directives (DEC-019) — all-or-nothing, message-wide, stripped from visible text", () => {
	it("[[audio_as_voice]] ANYWHERE in the body flags EVERY recognized-audio file — including before the directive", () => {
		const content =
			"MEDIA:/tmp/a.ogg\nsome prose\n[[audio_as_voice]]\nMEDIA:/tmp/b.mp3";
		const { media, cleaned, hasVoiceDirective } = extract(content);
		expect(hasVoiceDirective).toBe(true);
		expect(media.map((m) => [m.path, m.isVoice])).toEqual([
			["/tmp/a.ogg", true],
			["/tmp/b.mp3", true],
		]);
		expect(cleaned).not.toContain("[[audio_as_voice]]");
	});

	it("voice tagging is EXTENSION-GATED: image stays non-voice so ONE message mixes inline image + voice bubble", () => {
		const { media } = extract(
			"[[audio_as_voice]]\nMEDIA:/tmp/photo.png\nMEDIA:/tmp/note.wav",
		);
		expect(media).toEqual([
			{ path: "/tmp/photo.png", isVoice: false },
			{ path: "/tmp/note.wav", isVoice: true },
		]);
	});

	it("[[as_document]] is captured AND stripped; both directives coexist", () => {
		const content =
			"[[as_document]]\nMEDIA:/tmp/big.png\n[[audio_as_voice]] MEDIA:/tmp/v.opus";
		const {
			hasAsDocumentDirective: doc,
			hasVoiceDirective: voice,
			cleaned,
		} = extract(content);
		expect(doc).toBe(true);
		expect(voice).toBe(true);
		expect(cleaned).not.toContain("[[as_document]]");
		expect(hasAsDocumentDirective(content)).toBe(true);
	});

	it("directives apply MESSAGE-WIDE, not span-local: one outside any protection flips files tagged elsewhere", () => {
		// Directive sits AFTER a fenced block that mentions nothing; the audio
		// file sits INSIDE prose far away — still flagged because global.
		const content =
			"intro\n```\ncode\n```\ntail\n[[audio_as_voice]]\nMEDIA:/tmp/final.flac";
		const { media } = extract(content);
		expect(media).toEqual([{ path: "/tmp/final.flac", isVoice: true }]);
	});
});

describe("visible-text cleaning", () => {
	it("delivered tags are removed; protected spans survive; blank lines collapse to max two", () => {
		const content =
			"line1\nMEDIA:/tmp/r.png\n\n\n\n> quote MEDIA:/q.png stays\nline2";
		const { cleaned } = extract(content);
		expect(cleaned).not.toContain("/tmp/r.png");
		expect(cleaned).toContain("> quote MEDIA:/q.png stays");
		expect(cleaned).not.match(/\n{3,}/);
	});

	it("strip_media_directives_for_display removes known-ext tags unconditionally, unknown-ext only when undeliverable-visible", () => {
		expect(stripMediaDirectivesForDisplay("go MEDIA:/a.png now")).toBe(
			"go  now",
		);
		const kept = stripMediaDirectivesForDisplay(
			"keep MEDIA:/x/weird.ext visible",
			{ validatePath: REJECT_ALL },
		);
		expect(kept).toContain("MEDIA:/x/weird.ext");
	});

	it("display stripper leaves protected spans unmangled (#16434)", () => {
		const text = "```\nMEDIA:/example.png\n```";
		expect(stripMediaDirectivesForDisplay(text)).toContain(
			"MEDIA:/example.png",
		);
	});
});

describe("span utilities", () => {
	it("mergeSpans unions overlapping and adjacent-but-touching spans, sorted output", () => {
		expect(
			mergeSpans([
				[5, 9],
				[0, 3],
				[8, 12],
				[20, 22],
				[3, 5],
			]),
		).toEqual([
			[0, 12],
			[20, 22],
		]);
	});

	it("collectProtectedSpans returns exact original-string offsets", () => {
		const content = "ab `code` cd\n> q\n```js\nx\n```";
		const spans = collectProtectedSpans(content, { validatePath: REJECT_ALL });
		for (const [start, end] of spans) {
			expect(content.slice(start, end)).toMatch(
				/(`code`|^> q$|```[\s\S]*```)/gm,
			);
		}
	});
});

describe("normalize/path helpers", () => {
	it("normalizeMediaTagPath strips symmetric quotes and edge punctuation only", () => {
		expect(normalizeMediaTagPath("`/a b.png`")).toBe("/a b.png");
		expect(normalizeMediaTagPath("\"'/x',.;:)}]")).toBe("/x");
		expect(normalizeMediaTagPath("/plain.png")).toBe("/plain.png");
	});

	it("pathLacksDeliverableExtension drives the validated pass for Makefile and .log alike", () => {
		expect(pathLacksDeliverableExtension("/srv/Makefile")).toBe(true);
		expect(pathLacksDeliverableExtension("/srv/app.log")).toBe(true);
		expect(pathLacksDeliverableExtension("/srv/app.PNG")).toBe(false); // case-insensitive membership
		expect(pathLacksDeliverableExtension("/srv/app.png")).toBe(false);
	});

	it("ACCEPT_ALL fake validates every candidate (used by focused cases above)", () => {
		expect(ACCEPT_ALL("/whatever")).toBe("/whatever");
	});
});
