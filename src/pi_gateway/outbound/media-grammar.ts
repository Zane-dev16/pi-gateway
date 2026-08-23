// pi_gateway/outbound/media-grammar.ts — THE outbound file-delivery grammar
// (03-message-routing.md §9.2; DEC-019). Agents attach files over chat ONLY
// through this grammar: `MEDIA:<path>` tags (quoted/backticked/emphasis-
// wrapped variants) plus MESSAGE-GLOBAL `[[audio_as_voice]]` /
// `[[as_document]]` directives.
//
// Offset-mask protection BEFORE scanning (DEC-019): fenced code blocks,
// inline code spans, blockquotes, and JSON string values are blanked
// LENGTH-PRESERVINGLY (chars → spaces, newlines kept) so example/stored paths
// never deliver (#35695, #34375) while every match offset stays valid against
// the original text — masking is chained and idempotent per index, so NESTED
// or overlapping protection spans mask their union without corrupting
// offsets. Inline-code exception: a whole `MEDIA:/real/path` tag in inline
// code DOES deliver when the path validates (models routinely format real
// directives as code); fenced code blocks are always masked regardless.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   base.py:MEDIA_DELIVERY_EXTS / _MEDIA_EXT_ALTERNATION → MEDIA_DELIVERY_EXTS / buildExtAlternation
//   base.py:MEDIA_TAG_CLEANUP_RE / MEDIA_EXTENSIONLESS_TAG_RE → MEDIA_TAG_CLEANUP_RE / MEDIA_EXTENSIONLESS_TAG_RE
//   base.py:_mask_protected_spans           → collectProtectedSpans / maskProtectedSpans
//   base.py:_mask_json_string_media         → maskJsonStringMedia
//   base.py:_normalize_media_tag_path       → normalizeMediaTagPath
//   base.py:_match_extensionless_path       → matchExtensionlessPath
//   base.py:_merge_spans                    → mergeSpans
//   base.py:extract_media                   → extractMedia
//   base.py:strip_media_directives_for_display → stripMediaDirectivesForDisplay
//
// Phase-3 handoff (chunk/fence-carry splitting): segmentation.ts reuses
// collectProtectedSpans for offset-safe segmentation; formatting ladders own
// the (i/n) math on top.

import { extname } from "node:path";
import {
	validateMediaDeliveryPath,
	type PathValidationEnv,
} from "./media-policy.js";

/**
 * SINGLE SOURCE OF TRUTH for deliverable extensions (base.py:MEDIA_DELIVERY_EXTS).
 * Both extraction regexes derive from this tuple — historically divergent lists
 * created a silent black hole (#34517).
 */
export const MEDIA_DELIVERY_EXTS: readonly string[] = [
	// Images (embed inline)
	".png",
	".jpg",
	".jpeg",
	".gif",
	".webp",
	".bmp",
	".tiff",
	".svg",
	// Video (embed inline where supported)
	".mp4",
	".mov",
	".avi",
	".mkv",
	".webm",
	".3gp",
	// Audio (delivered as voice/audio where supported)
	".mp3",
	".m2a",
	".wav",
	".ogg",
	".opus",
	".m4a",
	".flac",
	// Documents (uploaded as file attachments)
	".pdf",
	".docx",
	".doc",
	".odt",
	".rtf",
	".txt",
	".md",
	".epub",
	// Spreadsheets / data
	".xlsx",
	".xls",
	".ods",
	".csv",
	".tsv",
	".json",
	".xml",
	".yaml",
	".yml",
	// Geospatial / GIS (#24032)
	".kmz",
	".kml",
	".geojson",
	".gpx",
	// Presentations
	".pptx",
	".ppt",
	".odp",
	".key",
	// Archives
	".zip",
	".tar",
	".gz",
	".tgz",
	".bz2",
	".xz",
	".7z",
	".rar",
	".apk",
	".ipa",
	// Web / rendered output
	".html",
	".htm",
];

/** Recognized audio extensions (base.py:_AUDIO_MIME_TYPES keys). */
export const AUDIO_EXTS: ReadonlySet<string> = new Set([
	".mp3",
	".m2a",
	".wav",
	".ogg",
	".opus",
	".m4a",
	".flac",
]);

/**
 * Alternation fragment, longest-first so `.tar` never matches as a prefix of
 * `.tar.gz` components (base.py:_MEDIA_EXT_ALTERNATION). Memoized: the set is
 * static and regex construction is hot-path work per message.
 */
let cachedAlternation: string | null = null;
export function buildExtAlternation(): string {
	if (cachedAlternation === null) {
		cachedAlternation = [...MEDIA_DELIVERY_EXTS]
			.map((e) => e.replace(/^\./, ""))
			.sort((a, b) => b.length - a.length)
			.join("|");
	}
	return cachedAlternation;
}

/** CJK full-width punctuation accepted as MEDIA path terminators (#88038). */
const CJK_TERMINATORS =
	"（）〈〉《》：，。；！？、\u201c\u201d\u2018\u2019【】";

/**
 * Anchored MEDIA:<path> pattern. Only strips a tag whose path ends in a known
 * deliverable extension (optionally quoted/backticked) — an unknown-extension
 * tag SURVIVES in the body rather than vanishing. Non-greedy quantifiers keep
 * glued tags (`MEDIA:/a.pngMEDIA:/b.png`) separate (#68773); sentence-final
 * `.` is a boundary only before whitespace/EOL; emphasis wrappers tolerated.
 */
export const MEDIA_TAG_CLEANUP_RE = new RegExp(
	"[`\"'*_]{0,3}MEDIA:\\s*" +
		"(?<path>`[^`\\n]+?`|\"[^\"\\n]+?\"|'[^'\\n]+?'|" +
		"(?:~/|/|[A-Za-z]:[/\\\\])\\S+?(?:[^\\S\\n]+\\S+?)*?\\.(?:" +
		buildExtAlternation() +
		"))" +
		"(?=[\\s`\"'*_,;:)\\]}\\[" +
		CJK_TERMINATORS +
		']|MEDIA:|\\.(?:\\s|$)|$)[`"*_]{0,3}\\.?',
	"gi",
);

/**
 * Paths NOT covered by the alternation above (extension-less files like
 * Makefile, or unknown extensions like .py/.log) route through here and are
 * delivered only when validate_media_delivery_path ACCEPTS them, so unknown
 * paths stay visible instead of silently dropping (#36060). Tempered-greedy
 * token prevents gluing onto the next tag or trailing prose.
 */
export const MEDIA_EXTENSIONLESS_TAG_RE = new RegExp(
	"[`\"'*_]{0,3}MEDIA:\\s*" +
		"(?<path>`[^`\\n]+`|\"[^\"\\n]+\"|'[^'\\n]+'|" +
		"(?:~/|/|[A-Za-z]:[/\\\\])[^\\s\\n`\"']+?)" +
		"(?=[`\"'\\s,;:)}" +
		"\\]" +
		CJK_TERMINATORS +
		"]|MEDIA:|$)" +
		"[`\"'*_]{0,3}\\s*",
	"gi",
);

/** Message-global directive tokens (all-or-nothing per message, stripped from visible text). */
export const AUDIO_AS_VOICE_DIRECTIVE = "[[audio_as_voice]]";
export const AS_DOCUMENT_DIRECTIVE = "[[as_document]]";

export interface ProtectedSpanOptions extends PathValidationEnv {
	/**
	 * Path validator for the inline-code delivery exception. Defaults to the
	 * media-policy ladder; tests inject fakes to keep grammar contracts pure.
	 */
	validatePath?: (path: string) => string | null;
}

function validatorOf(opts: ProtectedSpanOptions): (p: string) => string | null {
	return (
		opts.validatePath ?? ((p: string) => validateMediaDeliveryPath(p, opts))
	);
}

/**
 * Every protected span of `content` as [start, end) code-unit offsets into the
 * ORIGINAL string: fenced code blocks, inline code spans (minus the two
 * scannable exceptions), and blockquote lines. Offsets are exact — consumers
 * may slice/mask by them.
 *
 * The two inline-code exceptions (parity _mask_protected_spans):
 *   1. a backtick opening immediately after `MEDIA:` is a quoted PATH, not
 *      inline code;
 *   2. a whole tag wrapped in inline code (`MEDIA:/x.pdf`) stays SCANNABLE
 *      when the path validates — real directive, not a prose example.
 */
export function collectProtectedSpans(
	content: string,
	opts: ProtectedSpanOptions = {},
): Array<[number, number]> {
	const spans: Array<[number, number]> = [];

	// Fenced code blocks: ```...``` (opening line may carry an info string).
	for (const m of content.matchAll(/```[^\n]*\n[\s\S]*?```/g)) {
		spans.push([m.index, m.index + m[0].length]);
	}

	// Inline code spans.
	for (const m of content.matchAll(/`[^`\n]+`/g)) {
		const start = m.index;
		const prefix = content.slice(Math.max(0, start - 20), start);
		if (/MEDIA:\s*$/.test(prefix)) continue; // backtick-quoted MEDIA path
		const inner = (m[0] as string).slice(1, -1).trim();
		if (inner.toUpperCase().startsWith("MEDIA:")) {
			const candidate = normalizeMediaTagPath(inner.slice(6));
			if (candidate && validatorOf(opts)(candidate)) continue; // deliverable tag — keep scannable
		}
		spans.push([start, start + (m[0] as string).length]);
	}

	// Blockquote lines.
	for (const m of content.matchAll(/^>.*$/gm)) {
		spans.push([m.index, m.index + (m[0] as string).length]);
	}

	return spans;
}

/**
 * Blank every protected span LENGTH-PRESERVINGLY: non-newline chars become
 * spaces so `.length`, match offsets, and byte positions stay valid. Chained
 * masks compose (blanking is idempotent per index), which is exactly how
 * nested/overlapping protection unions behave under DEC-019.
 */
export function applySpansAsMask(
	content: string,
	spans: Array<[number, number]>,
): string {
	// CODE-UNIT array (split(""), not [...str]): spans/offsets are UTF-16
	// code-unit offsets (the regex domain) and .length must survive masking
	// even for astral-plane content.
	const chars = content.split("");
	for (const [start, end] of spans) {
		for (let i = start; i < end && i < chars.length; i++) {
			if (chars[i] !== "\n") chars[i] = " ";
		}
	}
	return chars.join("");
}

export function maskProtectedSpans(
	content: string,
	opts: ProtectedSpanOptions = {},
): string {
	return applySpansAsMask(content, collectProtectedSpans(content, opts));
}

/**
 * Blank `MEDIA:<bare-path>` occurrences inside JSON STRING VALUES so stored/
 * serialized tool-result text never re-delivers (#34375). Precise discriminator:
 * only spans opened by a JSON value-context quote (`:` `,` `{` `[` before the
 * `"`), and only BARE paths (`/`, `~/`, `X:\`) — a quoted-path tag is a real
 * LLM output format and stays untouched. Length-preserving.
 */
export function maskJsonStringMedia(content: string): string {
	if (!content.includes('"') || !content.toUpperCase().includes("MEDIA:"))
		return content;
	const chars = [...content];
	for (const m of content.matchAll(/(?<=[:,{[])\s*"((?:[^"\\\n]|\\.)*)"/g)) {
		const seg = m[1] as string;
		if (/MEDIA:\s*(?:~\/|\/|[A-Za-z]:[/\\])/.test(seg)) {
			// Opening-quote offset: the body holds no bare quotes, so the first
			// `"` inside the match IS the delimiter.
			const segStart =
				(m.index ?? 0) + ((m[0] as string).indexOf('"') as number) + 1;
			for (let i = segStart; i < segStart + seg.length; i++) {
				if (chars[i] !== "\n") chars[i] = " ";
			}
		}
	}
	return chars.join("");
}

/** Merge overlapping/nested spans so multi-pattern matches never double-delete adjacent text. */
export function mergeSpans(
	spans: Array<[number, number]>,
): Array<[number, number]> {
	const sorted = [...spans].sort((a, b) => a[0] - b[0]);
	const merged: Array<[number, number]> = [];
	for (const [s, e] of sorted) {
		const last = merged[merged.length - 1];
		if (last && s <= last[1]) last[1] = Math.max(last[1], e);
		else merged.push([s, e]);
	}
	return merged;
}

/** Strip quote wrappers + edge punctuation from a captured MEDIA path. */
export function normalizeMediaTagPath(raw: string): string {
	let path = String(raw ?? "").trim();
	if (
		path.length >= 2 &&
		path[0] === path[path.length - 1] &&
		(path[0] === "`" || path[0] === '"' || path[0] === "'")
	) {
		path = path.slice(1, -1).trim();
	}
	return path.replace(/^[`"']+/, "").replace(/[`"',.;:)}\]]+$/g, "");
}

export interface ExtractedMediaFile {
	/** Expanded (~/ resolved) path; validation happens downstream via filterMediaDeliveryPaths. */
	path: string;
	isVoice: boolean;
}

export interface ExtractMediaResult {
	media: ExtractedMediaFile[];
	/** Visible text: tags + directives removed, protected spans preserved verbatim. */
	cleaned: string;
	/** MESSAGE-GLOBAL directive flags captured from the ORIGINAL content (§9.2). */
	hasVoiceDirective: boolean;
	hasAsDocumentDirective: boolean;
}

/** True when the basename has no extension OR one outside MEDIA_DELIVERY_EXTS. */
export function pathLacksDeliverableExtension(path: string): boolean {
	const suffix = extname(path).toLowerCase();
	return (
		!suffix || !(MEDIA_DELIVERY_EXTS as readonly string[]).includes(suffix)
	);
}

/** `~` expansion for dedupe keys (extraction skips a crafted ~NUL path rather than aborting). */
function expandForDedupe(path: string, home?: string): string {
	if (path === "~") return home ?? "";
	if (path.startsWith("~/")) return `${home ?? ""}${path.slice(1)}`;
	return path;
}

/**
 * Extract MEDIA tags + message-global directives from response text
 * (base.py:extract_media). Dedupe on the EXPANDED path within THIS extraction,
 * first occurrence wins (#29131). Voice tagging is EXTENSION-GATED so one
 * message can carry an inline image AND a voice bubble together.
 */
export function extractMedia(
	content: string,
	opts: ProtectedSpanOptions = {},
): ExtractMediaResult {
	const hasVoiceDirective = content.includes(AUDIO_AS_VOICE_DIRECTIVE);
	const hasAsDocument = content.includes(AS_DOCUMENT_DIRECTIVE);

	// Directives are stripped FIRST; masking `cleaned` (not `content`) keeps
	// offsets valid after removal.
	let cleaned = content
		.split(AUDIO_AS_VOICE_DIRECTIVE)
		.join("")
		.split(AS_DOCUMENT_DIRECTIVE)
		.join("");

	const scanContent = maskJsonStringMedia(maskProtectedSpans(cleaned, opts));

	const media: ExtractedMediaFile[] = [];
	const seenPaths = new Set<string>();

	const consider = (rawPath: string): void => {
		const path = normalizeMediaTagPath(rawPath);
		if (!path) return;
		const ext = extname(path).toLowerCase();
		const isVoice = hasVoiceDirective && AUDIO_EXTS.has(ext);
		let expanded: string;
		try {
			if (path.includes("\0")) throw new Error("embedded null");
			expanded = expandForDedupe(path, opts.home);
		} catch {
			return; // crafted ~\x00 path skips itself, batch continues
		}
		if (!seenPaths.has(expanded)) {
			seenPaths.add(expanded);
			media.push({ path: expanded, isVoice });
		}
	};

	for (const m of scanContent.matchAll(MEDIA_TAG_CLEANUP_RE)) {
		consider(m.groups?.path ?? "");
	}

	// Extension-less / unknown-extension tags: validated delivery pass with
	// progressive space-extension recovery (#24032).
	for (const m of scanContent.matchAll(MEDIA_EXTENSIONLESS_TAG_RE)) {
		const rawPath = m.groups?.path ?? "";
		const path = normalizeMediaTagPath(rawPath);
		if (!path || !pathLacksDeliverableExtension(path)) continue;
		const resolved = matchExtensionlessPath(scanContent, m, opts);
		if (resolved === null) continue;
		const [safe, endOffset] = resolved;
		if (!seenPaths.has(safe)) {
			const safeExt = extname(safe).toLowerCase();
			media.push({
				path: safe,
				isVoice: hasVoiceDirective && AUDIO_EXTS.has(safeExt),
			});
			seenPaths.add(safe);
		}
		void endOffset;
	}

	// Remove DELIVERED tag spans from the visible text: locate spans on a
	// masked copy (locator only), delete exactly those spans from the
	// UNMASKED cleaned text — protected spans must survive verbatim.
	if (media.length > 0) {
		const maskedCleaned = maskJsonStringMedia(
			maskProtectedSpans(cleaned, opts),
		);
		const spans: Array<[number, number]> = [];
		for (const m of maskedCleaned.matchAll(MEDIA_TAG_CLEANUP_RE)) {
			spans.push([m.index, m.index + (m[0] as string).length]);
		}
		for (const m of maskedCleaned.matchAll(MEDIA_EXTENSIONLESS_TAG_RE)) {
			const path = normalizeMediaTagPath(m.groups?.path ?? "");
			if (!path || !pathLacksDeliverableExtension(path)) continue;
			const resolved = matchExtensionlessPath(maskedCleaned, m, opts);
			if (resolved !== null) spans.push([m.index, resolved[1]]);
		}
		cleaned = deleteSpans(cleaned, mergeSpans(spans));
		cleaned = collapseBlankLines(cleaned).trim();
	}

	return {
		media,
		cleaned,
		hasVoiceDirective,
		hasAsDocumentDirective: hasAsDocument,
	};
}

/** Delete [start,end) spans from `text` (code-unit offsets into the same-length twin). */
function deleteSpans(
	text: string,
	mergedSorted: Array<[number, number]>,
): string {
	const chars = text.split("");
	for (const [start, end] of [...mergedSorted].sort((a, b) => b[0] - a[0])) {
		chars.splice(start, end - start);
	}
	return chars.join("");
}

function collapseBlankLines(text: string): string {
	return text.replace(/\n{3,}/g, "\n\n");
}

/**
 * Resolve an extensionless tag to a VALIDATED on-disk path, progressively
 * extending forward across single spaces when the bare match fails validation
 * (bounded at 8 tokens, never past a newline or a subsequent MEDIA: keyword;
 #24032). Returns [safe_path, end_offset_into_scan_text] or null.
 */
export function matchExtensionlessPath(
	scanText: string,
	match: RegExpMatchArray,
	opts: ProtectedSpanOptions = {},
): [string, number] | null {
	const validate = validatorOf(opts);
	const rawPath = match.groups?.path ?? "";
	const path = normalizeMediaTagPath(rawPath);
	if (!path) return null;
	const safe = validate(path);
	// The raw path appears verbatim inside the full match (the group matched it
	// literally), so its absolute span reproduces Python's match.start/end("path").
	const absPathStart =
		(match.index ?? 0) + (match[0] as string).indexOf(rawPath);
	if (safe) {
		return [safe, absPathStart + rawPath.length];
	}
	// Recovery segment STARTS AT THE PATH GROUP (parity match.start("path")),
	// bounded by newline / next MEDIA: keyword.
	const nl = scanText.indexOf("\n", absPathStart);
	const limit = nl === -1 ? scanText.length : nl;
	const segment = scanText.slice(absPathStart, limit);
	const nxt = segment.indexOf("MEDIA:", 1);
	const boundedSegment = nxt !== -1 ? segment.slice(0, nxt) : segment;

	// pos walks within `segment`; token ends map back via absPathStart+tokEnd.
	let pos = rawPath.length;
	for (let attempt = 0; attempt < 8; attempt++) {
		while (
			pos < boundedSegment.length &&
			(boundedSegment[pos] === " " || boundedSegment[pos] === "\t")
		)
			pos++;
		if (pos >= boundedSegment.length) break;
		let tokEnd = pos;
		while (
			tokEnd < boundedSegment.length &&
			boundedSegment[tokEnd] !== " " &&
			boundedSegment[tokEnd] !== "\t"
		)
			tokEnd++;
		const candidate = normalizeMediaTagPath(boundedSegment.slice(0, tokEnd));
		const safeCandidate = validate(candidate);
		if (safeCandidate) return [safeCandidate, absPathStart + tokEnd];
		pos = tokEnd;
	}
	return null;
}

/**
 * Display-side directive stripper (base.py:strip_media_directives_for_display):
 * removes MEDIA: tags + directive markers from streamed/display text. Known-
 * extension tags go unconditionally; extension-less ones only when the path
 * validates, so undeliverable paths stay VISIBLE for debugging. Protected
 * spans are locator-only — never mangled (#16434).
 */
export function stripMediaDirectivesForDisplay(
	text: string,
	opts: ProtectedSpanOptions = {},
): string {
	if (
		!text.includes("MEDIA:") &&
		!text.includes(AUDIO_AS_VOICE_DIRECTIVE) &&
		!text.includes(AS_DOCUMENT_DIRECTIVE)
	) {
		return text;
	}
	let cleaned = text
		.split(AUDIO_AS_VOICE_DIRECTIVE)
		.join("")
		.split(AS_DOCUMENT_DIRECTIVE)
		.join("");
	const masked = maskJsonStringMedia(maskProtectedSpans(cleaned, opts));
	const spans: Array<[number, number]> = [];
	for (const m of masked.matchAll(MEDIA_TAG_CLEANUP_RE)) {
		spans.push([m.index, m.index + (m[0] as string).length]);
	}
	for (const m of masked.matchAll(MEDIA_EXTENSIONLESS_TAG_RE)) {
		const path = normalizeMediaTagPath(m.groups?.path ?? "");
		if (!path || !pathLacksDeliverableExtension(path)) continue;
		const resolved = matchExtensionlessPath(masked, m, opts);
		if (resolved !== null) spans.push([m.index, resolved[1]]);
	}
	if (spans.length > 0) {
		cleaned = deleteSpans(cleaned, mergeSpans(spans));
		cleaned = collapseBlankLines(cleaned);
	}
	return cleaned.trimEnd();
}

/**
 * Capture `[[as_document]]` BEFORE extraction strips it (post-stream rescan
 * needs the flag to force byte-preserving document delivery — §9.3).
 */
export function hasAsDocumentDirective(content: string): boolean {
	return content.includes(AS_DOCUMENT_DIRECTIVE);
}
