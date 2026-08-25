// pi_platforms/whatsapp-cloud/wa-markdown — WhatsApp-flavored markup
// conversion (the REST-path dialect converter for the Cloud adapter).
//
// Ported from the READ-ONLY Hermes reference
// gateway/platforms/whatsapp_common.py:WhatsAppBehaviorMixin.format_message —
// the behavior mixin shared by the Baileys bridge and THIS cloud adapter:
//   - STEP 0 (_sanitize_outbound_text): U+200B/U+2060/U+2063/U+FEFF stripped
//     and U+00A0/U+1680/U+180E/U+2000-U+200A/U+202F/U+205F/U+3000 normalized
//     to plain ASCII space on EVERY outbound (they render as mojibake on
//     WhatsApp)
//   - fenced code blocks survive verbatim (supports_code_blocks=True: WhatsApp
//     renders monospace fences) and are protected behind placeholders while
//     the surrounding markdown is rewritten
//   - inline `code` protected likewise
//   - *italic* → _italic_ FIRST (lookaround-guarded so list bullets and bold
//     delimiters never convert; single-* left alone would render BOLD on
//     WhatsApp), then **bold** → *bold* ; __bold__ → *bold* ;
//     ~~strike~~ → ~strike~ (_italic_ is ALREADY WhatsApp syntax — untouched)
//   - #{1,6} headers → *Header* (stripping any bold wrapping produced by the
//     previous step so "# **Title**" yields "*Title*", not "**Title**")
//   - [text](url) links → "text (url)"
//
// Pure function, byte-deterministic; unit-tested by conversion round-trips.

const FENCE_PH = "\u0000WAF";
const CODE_PH = "\u0000WAC";

/** _OUTBOUND_INVISIBLE_CHARS_RE parity — zero-width format chars, stripped. */
const OUTBOUND_INVISIBLE_CHARS_RE = /[\u200b\u2060\u2063\ufeff]/g;
/** _OUTBOUND_ODD_SPACE_RE parity — odd unicode spaces normalize to ' '. */
const OUTBOUND_ODD_SPACE_RE =
	/[\u00a0\u1680\u180e\u2000-\u200a\u202f\u205f\u3000]/g;

/**
 * Italic BEFORE bold (format_message step 3 parity): standard Markdown
 * *text* → WhatsApp _text_. The lookarounds avoid list bullets (`* item`) and
 * bold delimiters (`**` never opens an italic span).
 */
const ITALIC_SINGLE_STAR_RE = /(?<!\*)\*(?!\s|\*)([^*\n]*?\S[^*\n]*?)\*(?!\*)/g;

/** _sanitize_outbound_text parity: strip invisible chars, normalize spaces. */
function sanitizeOutboundText(content: string): string {
	if (!content) return content;
	return content
		.replace(OUTBOUND_INVISIBLE_CHARS_RE, "")
		.replace(OUTBOUND_ODD_SPACE_RE, " ");
}

export function toWhatsappMarkup(input: string): string {
	if (!input) return input;

	const fences: string[] = [];
	const codes: string[] = [];

	// 0. Sanitize invisible/odd unicode BEFORE any protection or conversion
	// (format_message sanitizes first, then protects fences).
	let result = sanitizeOutboundText(input);

	// 1. Protect fenced blocks (``` … ```) then inline code.
	result = result.replace(/```[\s\S]*?```/g, (fence) => {
		fences.push(fence);
		return `${FENCE_PH}${fences.length - 1}\u0000`;
	});
	result = result.replace(/`([^`\n]+)`/g, (code) => {
		codes.push(code);
		return `${CODE_PH}${codes.length - 1}\u0000`;
	});

	// 2. Italic FIRST (so **bold** never converts by accident), then
	// bold/strike dialect swaps.
	result = result.replace(ITALIC_SINGLE_STAR_RE, "_$1_");
	result = result.replace(/\*\*(.+?)\*\*/g, "*$1*");
	result = result.replace(/__(.+?)__/g, "*$1*");
	result = result.replace(/~~(.+?)~~/g, "~$1~");

	// 3. Headers → bold lines (# **Title** collapses to *Title*).
	result = result.replace(/^#{1,6}\s+(.+)$/gm, (_m, inner: string) => {
		let text = String(inner).trim();
		while (text.length > 1 && text.startsWith("*") && text.endsWith("*")) {
			text = text.slice(1, -1).trim();
		}
		return `*${text}*`;
	});

	// 4. Markdown links → "text (url)".
	result = result.replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1 ($2)");

	// 5. Restore protected sections.
	result = result.replace(
		/\u0000WAF(\d+)\u0000/g,
		(_m, i: string) => fences[Number(i)] ?? "",
	);
	result = result.replace(
		/\u0000WAC(\d+)\u0000/g,
		(_m, i: string) => codes[Number(i)] ?? "",
	);
	return result;
}
