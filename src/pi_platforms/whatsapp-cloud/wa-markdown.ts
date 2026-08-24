// pi_platforms/whatsapp-cloud/wa-markdown — WhatsApp-flavored markup
// conversion (the REST-path dialect converter for the Cloud adapter).
//
// Ported from the READ-ONLY Hermes reference
// gateway/platforms/whatsapp_common.py:WhatsAppBehaviorMixin.format_message —
// the behavior mixin shared by the Baileys bridge and THIS cloud adapter:
//   - fenced code blocks survive verbatim (supports_code_blocks=True: WhatsApp
//     renders monospace fences) and are protected behind placeholders while
//     the surrounding markdown is rewritten
//   - inline `code` protected likewise
//   - **bold** → *bold* ; __bold__ → *bold* ; ~~strike~~ → ~strike~
//     (_italic_ is ALREADY WhatsApp syntax — left untouched)
//   - #{1,6} headers → *Header* (stripping any bold wrapping produced by the
//     previous step so "# **Title**" yields "*Title*", not "**Title**")
//   - [text](url) links → "text (url)"
//
// Pure function, byte-deterministic; unit-tested by conversion round-trips.

const FENCE_PH = "\u0000WAF";
const CODE_PH = "\u0000WAC";

export function toWhatsappMarkup(input: string): string {
	const fences: string[] = [];
	const codes: string[] = [];

	// 1. Protect fenced blocks (``` … ```) then inline code.
	let result = input.replace(/```[\s\S]*?```/g, (fence) => {
		fences.push(fence);
		return `${FENCE_PH}${fences.length - 1}\u0000`;
	});
	result = result.replace(/`([^`\n]+)`/g, (code) => {
		codes.push(code);
		return `${CODE_PH}${codes.length - 1}\u0000`;
	});

	// 2. Bold/italic/strike dialect swaps.
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
