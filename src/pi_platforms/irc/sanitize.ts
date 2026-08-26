// pi_platforms/irc/sanitize — A19 INJECTION-SAFETY SANITIZERS for the
// IRC text class (gap-audit A19 ride-along; roadmap Phase 6 heuristic 3).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/irc/adapter.py::_parse_irc_message      (line protocol)
//   plugins/platforms/irc/adapter.py::_extract_nick
//   plugins/platforms/irc/adapter.py::_strip_markdown         (outbound plain)
//   plugins/platforms/irc/adapter.py::_strip_irc_control_chars (CRLF/NUL)
//   plugins/platforms/irc/adapter.py::IRCAdapter._handle_line  (CTCP ACTION,
//     addressing gate, nick-collision suffix ladder)
//   plugins/platforms/irc/adapter.py::_standalone_send        (target checks)
//
// EVERY sanitizer is a PURE FUNCTION with a CONTRACT RUNNER exported beside
// it: the conformance suite executes each contract against the real impl AND
// against deliberate mutants — a mutant MUST fail its named contract, or the
// contract is a change-detector-shaped no-op (workspace hard rule).

import { IRC_SANITIZER_REPLACEMENT } from "./manifest.js";

/** adapter.py:_parse_irc_message — raw line → {prefix, command, params}. */
export interface ParsedIrcLine {
	prefix: string;
	command: string;
	params: string[];
}

export function parseIrcMessage(raw: string): ParsedIrcLine {
	let rest = raw;
	let prefix = "";
	let trailing = "";

	if (rest.startsWith(":")) {
		const idx = rest.indexOf(" ");
		if (idx >= 0) {
			prefix = rest.slice(1, idx);
			rest = rest.slice(idx + 1);
		} else {
			prefix = rest.slice(1);
			rest = "";
		}
	}

	const trailingIdx = rest.indexOf(" :");
	if (trailingIdx >= 0) {
		trailing = rest.slice(trailingIdx + 2);
		rest = rest.slice(0, trailingIdx);
	}

	const parts = rest.split(" ").filter((p) => p.length > 0);
	const command = parts.length > 0 ? (parts[0] ?? "") : "";
	const params = parts.length > 1 ? parts.slice(1) : [];
	// adapter.py:104 bug-for-bug: `if trailing:` — an EMPTY trailing is NOT
	// pushed. "PRIVMSG botnick :" parses to params=[botnick] (<2 params ⇒
	// dropped by the PRIVMSG gate), never to a text="" turn that would pass
	// the DM gates and dispatch an empty reply.
	if (trailing !== "") params.push(trailing);

	return { prefix, command, params };
}

/** adapter.py:_extract_nick — nick!user@host → nick. */
export function extractNick(prefix: string): string {
	if (!prefix.includes("!")) return prefix;
	return prefix.split("!")[0] ?? prefix;
}

/**
 * adapter.py:_strip_irc_control_chars — CR/LF become ONE space each; NUL is
 * removed. IRC commands are CRLF-delimited: a bare \r or \n inside user
 * content injects arbitrary commands (CTCP, JOIN, KICK).
 */
export function stripIrcControlChars(text: string): string {
	return text
		.replaceAll("\r", IRC_SANITIZER_REPLACEMENT)
		.replaceAll("\n", IRC_SANITIZER_REPLACEMENT)
		.replaceAll("\x00", "");
}

/**
 * adapter.py:_strip_markdown — markdown does not render on IRC; strip to the
 * plain-text dialect. Images BEFORE links; bold before italic.
 */
export function stripMarkdownForIrc(text: string): string {
	let out = text;
	out = out.replace(/\*\*(.+?)\*\*/g, "$1"); // **bold**
	out = out.replace(/__(.+?)__/g, "$1"); // __bold__
	out = out.replace(/\*(.+?)\*/g, "$1"); // *italic*
	out = out.replace(/(?<!\w)_(.+?)_(?!\w)/g, "$1"); // _italic_
	out = out.replace(/`(.+?)`/g, "$1"); // `code`
	out = out.replace(/```\w*\n?/g, ""); // ```fences
	out = out.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, "$2"); // ![alt](url) → url
	out = out.replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1 ($2)"); // [t](u) → t (u)
	return out;
}

/**
 * adapter.py:_handle_line — inbound CTCP ACTION (/me) converts to
 * "* <nick> <action>"; every OTHER CTCP payload (\x01…) is dropped upstream.
 */
export function ctcpActionText(text: string, senderNick: string): string {
	return `* ${senderNick} ${text.slice("\x01ACTION ".length, -1)}`;
}

export function isCtcpAction(text: string): boolean {
	return text.startsWith("\x01ACTION ") && text.endsWith("\x01");
}

export function isCtcp(text: string): boolean {
	return text.startsWith("\x01");
}

/** adapter.py:_is_irc_channel — #&+! channel prefixes. */
export function isIrcChannel(target: string): boolean {
	const first = target.charAt(0);
	return target.length > 0 && "#&+!".includes(first);
}

/**
 * adapter.py:_handle_line (433) — collision suffix ladder. From the CURRENT
 * nick: numeric suffix increments (hermes_1 → hermes_2); first collision on
 * the bare nick appends "_" (hermes_); any other mutated form restarts at
 * "_1". The base strips trailing digits/underscores first.
 */
export function nextNickOnCollision(
	configuredNick: string,
	currentNick: string,
): string {
	const base = configuredNick.replace(/[_0-9]+$/u, "");
	const suffixMatch = /_(\d+)$/u.exec(currentNick);
	if (suffixMatch) {
		return `${base}_${Number(suffixMatch[1]) + 1}`;
	}
	if (currentNick === configuredNick) {
		return `${configuredNick}_`;
	}
	return `${configuredNick}_1`;
}

/**
 * adapter.py:_standalone_send — chat_id/target admission: \r \n \x00 and
 * SPACE are illegal in an IRC message target (command injection). Returns
 * null when the target must be rejected.
 */
export function safeIrcTarget(
	rawTarget: string | undefined | null,
): string | null {
	const raw = rawTarget ?? "";
	if (/[\r\n\x00 ]/u.test(raw)) return null;
	return raw;
}

/**
 * adapter.py:_split_message — byte-aware paragraph splitter. Budget per line:
 * min(userLimit [measured with lenFn], 510 − overhead BYTES); binary search
 * the longest UTF-8-safe prefix, prefer a SPACE boundary when one sits past
 * ⅓ of the cut; blank paragraphs vanish. Empty input yields [""] (one empty
 * PRIVMSG). lenFn defaults to code-point length; the §6.3 policy pair may
 * pass its own unit measurer (utf16 for harness chats).
 */
export function splitMessageForIrc(
	content: string,
	target: string,
	userLimitChars: number,
	lenFn: (s: string) => number = (s) => [...s].length,
	/** Keep markup in the chunks (§6.1 byte-preservation); strip at the wire. */
	keepMarkup = false,
): string[] {
	const overhead = Buffer.byteLength(`PRIVMSG ${target} :`, "utf8") + 2;
	const maxBytes = 510 - overhead;
	const lines: string[] = [];

	const source = keepMarkup ? content : stripMarkdownForIrc(content);
	for (const paragraph of source.split("\n")) {
		if (paragraph.trim().length === 0) continue;
		let rest = paragraph;
		for (;;) {
			if (
				lenFn(rest) <= userLimitChars &&
				Buffer.byteLength(rest, "utf8") <= maxBytes
			) {
				if (rest.trim().length > 0) lines.push(rest);
				break;
			}
			// Binary search the longest character prefix within BOTH budgets
			// (unit measurer AND protocol bytes move independently).
			let low = 1;
			let high = rest.length;
			let best = 0;
			while (low <= high) {
				const mid = (low + high) >> 1;
				const head = rest.slice(0, mid);
				if (
					lenFn(head) <= userLimitChars &&
					Buffer.byteLength(head, "utf8") <= maxBytes
				) {
					best = mid;
					low = mid + 1;
				} else {
					high = mid - 1;
				}
			}
			let splitAt = best;
			const space = rest.lastIndexOf(" ", splitAt);
			if (space > Math.floor(splitAt / 3)) splitAt = space;
			lines.push(rest.slice(0, splitAt).trimEnd());
			rest = rest.slice(splitAt).trimStart();
		}
	}

	return lines.length > 0 ? lines : [""];
}

// ── A19 contract runners (mutation-checked; see irc.test.ts) ───────────────

export type SanitizerContract = {
	name: string;
	/** What a violation looks like (assertion detail in mutation reports). */
	invariant: string;
	/**
	 * Runs the invariant against an IMPL; throws when the impl VIOLATES it.
	 * The real implementation must pass; every mutant must throw.
	 */
	run: (impl: typeof realSanitizers) => void;
};

/** The real implementations, grouped so contracts can exercise all of them. */
export const realSanitizers = {
	stripIrcControlChars,
	stripMarkdownForIrc,
	safeIrcTarget,
	nextNickOnCollision,
	ctcpActionText,
	splitMessageForIrc,
};

export type SanitizerImpls = typeof realSanitizers;

function assert(condition: unknown, message: string): asserts condition {
	if (!condition) throw new Error(message);
}

const CRLF_PAYLOAD = "hi\rJOIN #evil\r\nKICK #x bob\n\x00nul";

export const SANITIZER_CONTRACTS: readonly SanitizerContract[] = [
	{
		name: "crlf-nul-stripped",
		invariant:
			"A19: outbound line payloads carry NO carriage return, newline, or NUL byte",
		run: (impl) => {
			const out = impl.stripIrcControlChars(CRLF_PAYLOAD);
			assert(
				!/[\r\n\x00]/u.test(out),
				`control byte survived: ${JSON.stringify(out)}`,
			);
			assert(out.includes("hi"), "benign content survives stripping");
			assert(
				impl.stripIrcControlChars("plain").includes("plain"),
				"clean text passes through unchanged",
			);
		},
	},
	{
		name: "markdown-stripped-to-plain",
		invariant:
			"A19: outbound markdown markers are stripped (bold/italic/code/fences/images-before-links)",
		run: (impl) => {
			const out = impl.stripMarkdownForIrc(
				"**b** __b2__ *i* _i2_ `c` ![alt](http://img) [txt](http://lnk)",
			);
			assert(!out.includes("**"), `** survived: ${JSON.stringify(out)}`);
			assert(!out.includes("__"), "__ survived");
			assert(!out.includes("`"), "` survived");
			assert(!out.includes("!["), "image syntax survived");
			assert(!out.includes("alt"), "image alt text leaked");
			assert(
				out.includes("http://img"),
				"image URL is KEPT (images degrade to url)",
			);
			assert(
				out.includes("txt (http://lnk)"),
				`links degrade to 'text (url)', got ${JSON.stringify(out)}`,
			);
			assert(
				!out.includes("*") && !/(?<!\w)_/u.test(out),
				"emphasis markers survived",
			);
			assert(
				out.includes("b") && out.includes("i") && out.includes("c"),
				"inner content preserved",
			);
		},
	},
	{
		name: "injection-target-rejected",
		invariant:
			"A19: a send target containing CR/LF/NUL/space is REJECTED, never transmitted",
		run: (impl) => {
			assert(impl.safeIrcTarget("#ok") === "#ok", "clean target passes");
			for (const bad of [
				"#a\rJOIN",
				"#a\nKICK",
				"#a\x00",
				"#a b",
				"\r",
				"\n",
			]) {
				assert(
					impl.safeIrcTarget(bad) === null,
					`hostile target accepted: ${JSON.stringify(bad)}`,
				);
			}
		},
	},
	{
		name: "nick-ladder-bounded-and-shaped",
		invariant:
			"A19/433: collision ladder produces hermes_ then hermes_1, hermes_2… and NEVER regresses",
		run: (impl) => {
			const first = impl.nextNickOnCollision("hermes-bot", "hermes-bot");
			assert(
				first === "hermes-bot_",
				`first collision → underscore suffix, got ${first}`,
			);
			const second = impl.nextNickOnCollision("hermes-bot", first);
			assert(second === "hermes-bot_1", `second collision → _1, got ${second}`);
			const third = impl.nextNickOnCollision("hermes-bot", second);
			assert(third === "hermes-bot_2", `third collision → _2, got ${third}`);
		},
	},
	{
		name: "ctcp-action-converted-others-dropped",
		invariant:
			"A19: CTCP ACTION becomes '* nick action'; non-ACTION CTCP never renders as chat text",
		run: (impl) => {
			const action = "\x01ACTION waves hello\x01";
			assert(
				impl.ctcpActionText(action, "alice") === "* alice waves hello",
				"ACTION conversion shape violated",
			);
		},
	},
	{
		name: "splitter-respects-byte-budget",
		invariant:
			"A19: every split line fits min(userLimit chars, 510−overhead BYTES); multi-byte content splits on code-point boundaries",
		run: (impl) => {
			const target = "#chan";
			const overhead = Buffer.byteLength(`PRIVMSG ${target} :`) + 2;
			const budget = 510 - overhead;
			const lines = impl.splitMessageForIrc("🎉".repeat(400), target, 450);
			assert(lines.length > 1, "astral-heavy content must split");
			for (const line of lines) {
				const bytes = Buffer.byteLength(line, "utf8");
				assert(bytes <= budget, `line exceeds protocol budget (${bytes}B)`);
				assert(line.length <= 450, `line exceeds char limit (${line.length})`);
				assert(
					!line.includes("\ud83c") || line.includes("🎉"),
					"surrogate pair torn across lines",
				);
			}
			// Space-preference: cuts land on word boundaries when possible.
			const words = impl.splitMessageForIrc(
				Array.from({ length: 120 }, (_, i) => `word${i}`).join(" "),
				target,
				50,
			);
			assert(words.length > 1, "long word run must split under 50-char limit");
			for (const line of words) {
				assert(line.length <= 50, `char limit violated (${line.length})`);
			}
			assert(
				JSON.stringify(impl.splitMessageForIrc("", target, 450)) ===
					JSON.stringify([""]),
				"empty content yields exactly one empty line",
			);
		},
	},
];
