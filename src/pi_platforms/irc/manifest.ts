// pi_platforms/irc/manifest — IRC MANIFEST DATA (Phase-6 census port).
// Every constant TRANSCRIBED from the READ-ONLY Hermes reference plugin
// (plugins/platforms/irc/adapter.py) and cited by file:symbol (Q17/DEC-017).

import type { PluginManifest } from "../kit/index.js";

/**
 * adapter.py:register(max_message_length=450) / IRCAdapter.__init__
 * (`max_msg or 450`) — the USER-VISIBLE per-message character budget.
 * CHARACTERS (Python len() → code points, kit "chars" unit).
 */
export const IRC_MAX_MESSAGE_LENGTH_CHARS = 450;

/**
 * adapter.py:_split_message — "IRC has a ~512 byte line limit"; the splitter
 * reserves protocol overhead `len("PRIVMSG <target> :")` + 2 (CRLF) inside a
 * 510-byte budget (1 byte spare each side of the historic 512 cap).
 */
export const IRC_PROTOCOL_LINE_BUDGET_BYTES = 510;

/**
 * adapter.py:send — `await asyncio.sleep(0.3)` after EVERY PRIVMSG line:
 * "Basic rate limiting to avoid excess flood". THE flood-control budget is
 * this interline PACING (data, not hardcoded runner logic — Q17).
 */
export const IRC_INTERLINE_PACING_MS = 300;

/** adapter.py:connect — open_connection + registration waits use 30s. */
export const IRC_CONNECT_TIMEOUT_MS = 30_000;
export const IRC_REGISTRATION_TIMEOUT_MS = 30_000;

/**
 * adapter.py:connect — NickServ IDENTIFY is followed by a fixed 2s settle
 * sleep ("Give NickServ time to process").
 */
export const IRC_NICKSERV_SETTLE_MS = 2_000;

/**
 * adapter.py:_standalone_send — standalone nick base capped to 24 chars
 * ("so subsequent collision retries do not overflow the 30-char NICKLEN most
 * networks enforce"); full nick capped to 30.
 */
export const IRC_NICKLEN = 30;
export const IRC_STANDALONE_NICK_BASE_CAP = 24;

/** adapter.py:_standalone_send — max_nick_attempts = 5 (432/433 ladder bound). */
export const IRC_MAX_NICK_ATTEMPTS = 5;

/** adapter.py:_standalone_send — JOIN ack wait (366/JOIN) is 5s; then proceed. */
export const IRC_JOIN_ACK_WAIT_MS = 5_000;

/**
 * adapter.py:_standalone_send — JOIN rejection numerics that abort delivery:
 403 ERR_NOSUCHCHANNEL · 405 ERR_TOOMANYCHANNELS · 471 ERR_CHANNELISFULL ·
 473 ERR_INVITEONLYCHAN · 474 ERR_BANNEDFROMCHAN · 475 ERR_BADCHANNELKEY.
 */
export const IRC_JOIN_REJECT_CODES: ReadonlySet<string> = new Set([
	"403",
	"405",
	"471",
	"473",
	"474",
	"475",
]);

/** adapter.py:_standalone_send — server-rejected-client numerics (PASS). */
export const IRC_REGISTRATION_REJECT_CODES: ReadonlySet<string> = new Set([
	"464",
	"465",
]);

/**
 * A19 sanitizer scope (gap-audit A19; adapter.py::_strip_irc_control_chars):
 * CR/LF are IRC command terminators (CTCP/JOIN/KICK injection) and NUL is a
 * protocol-illegal byte. Everything else is legal in PRIVMSG payloads.
 */
export const IRC_SANITIZER_REPLACEMENT = " ";

// ── the PluginManifest ──────────────────────────────────────────────────────

/**
 * Capability mapping note (mattermost-manifest parity): Hermes IRC renders NO
 * markdown — outbound text is stripped to PLAIN TEXT (A19) and split per the
 * 450-char/510-byte budgets. splitsLongMessages=true expresses the plugin's
 * own send()-level splitting; the kit chunker is BYPASSED in favor of the
 * vendor byte-aware paragraph splitter (shape delta, see irc-adapter).
 *
 * Transport note: persistent line socket with server-driven PING keepalive.
 * There is NO native draft streaming, NO edits, NO interactive callbacks on
 * the real surface — declared streaming matches seal reality (probe-computed
 * exclusions; see conformance wiring).
 */
export const IRC_PLUGIN_MANIFEST: PluginManifest = {
	name: "irc",
	description:
		"IRC adapter on the persistent line-socket transport family (RFC 2812 PRIVMSG relay)",
	transportShape: "ws",
	requiresEnv: [
		{
			name: "IRC_SERVER",
			description: "IRC server hostname (e.g. irc.libera.chat)",
			password: false,
		},
		{
			name: "IRC_CHANNEL",
			description: "Channel to join (e.g. #hermes)",
			password: false,
		},
		{
			name: "IRC_NICKNAME",
			description: "Bot nickname (e.g. hermes-bot)",
			password: false,
		},
	],
	optionalEnv: [
		{ name: "IRC_PORT", description: "Server port (default 6697)" },
		{
			name: "IRC_USE_TLS",
			description: "Use TLS (default true)",
			password: false,
		},
		{
			name: "IRC_SERVER_PASSWORD",
			description: "Optional PASS password",
			password: true,
		},
		{
			name: "IRC_NICKSERV_PASSWORD",
			description: "Optional NickServ IDENTIFY password",
			password: true,
		},
		{
			name: "IRC_ALLOWED_USERS",
			description: "Comma-separated allowlist of nicks (empty = allow all)",
			password: false,
		},
	],
	capabilities: {
		supportsAsyncDelivery: true,
		splitsLongMessages: true,
		typedCommandPrefix: "/",
		interactiveResume: false,
		supportsInchannelContinuable: false,
		requiresEditFinalize: false,
	},
	// Q17 review note: the ONLY client-side rate control in the Hermes plugin
	// is the 0.3s interline PRIVMSG pacing (adapter.py:send). No server tiers
	// exist to transcribe; excess-flood enforcement is server-side QUIT.
};
