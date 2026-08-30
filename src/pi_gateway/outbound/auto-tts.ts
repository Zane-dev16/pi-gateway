// pi_gateway/outbound/auto-tts.ts — whether a finished turn is spoken aloud
// is decided ONCE, post-turn, against three axes (03-message-routing.md
// §9.4); mis-specify any one and users hear double audio:
//
//   1. Mode resolution (first match wins): per-chat persisted mode `all` →
//      speak every turn; `voice_only` → speak only replies to VOICE input;
//      `off` → never; NO per-chat mode → global `voice.auto_tts` mirrored on
//      the adapter (adapter._should_auto_tts_for_chat).
//   2. Response eligibility: empty or `Error:`-prefixed responses never speak.
//   3. Dedup axis A — agent-initiated TTS: an assistant `text_to_speech`
//      tool_call within THIS turn ⇒ runner stays silent.
//   4. Dedup axis B — base-adapter ownership: voice-INPUT turns are spoken by
//      the adapter's auto-TTS path so the runner skips — UNLESS streaming
//      already consumed the response (already_sent=True): the adapter gets
//      None and cannot synthesize, so ownership flips to the runner.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   run.py:_should_send_voice_reply        → shouldSendVoiceReply
//   run.py:_load_voice_modes/_save_voice_modes → loadVoiceModes/saveVoiceModes
//   run.py:_sync_voice_mode_state_to_adapter → syncVoiceModeStateToAdapter
//   run.py:_voice_key                      → voiceKey
//   platforms/base.py:_should_auto_tts_for_chat → adapterShouldAutoTtsForChat
//   tools/tts_tool.py:OPUS_VOICE_PLATFORMS → OPUS_VOICE_PLATFORMS

import { readFileSync, writeFileSync, mkdirSync } from "node:fs";
import { dirname, join } from "node:path";
import { tmpdir } from "node:os";
import { randomUUID } from "node:crypto";

/** Per-chat voice modes (gateway_voice_mode.json values). */
export type VoiceMode = "all" | "voice_only" | "off";

export const VOICE_MODES: readonly VoiceMode[] = ["all", "voice_only", "off"];

/** Platform-namespaced key for voice-mode state (run.py:_voice_key). */
export function voiceKey(platform: string, chatId: string): string {
	return `${platform}:${chatId}`;
}

export interface VoiceModeLoadResult {
	modes: Record<string, VoiceMode>;
	/** Legacy UNPREFIXED keys skipped during migration (warn + re-enable to rebuild). */
	skippedLegacyKeys: string[];
	/** Invalid mode values ignored. */
	invalidEntries: number;
}

/**
 * Load per-chat voice modes from `<home>/gateway_voice_mode.json`
 * (run.py:_load_voice_modes). Corrupt/unreadable file ⇒ empty store; invalid
 * mode VALUES are skipped silently-counted; legacy unprefixed keys (pre-
 * namespacing) are SKIPPED with the key recorded for a warning until the chat
 * re-enables.
 */
export function loadVoiceModes(path: string): VoiceModeLoadResult {
	let data: unknown;
	try {
		data = JSON.parse(readFileSync(path, "utf8"));
	} catch {
		return { modes: {}, skippedLegacyKeys: [], invalidEntries: 0 };
	}
	if (data == null || typeof data !== "object" || Array.isArray(data)) {
		return { modes: {}, skippedLegacyKeys: [], invalidEntries: 0 };
	}
	const result: Record<string, VoiceMode> = {};
	const skippedLegacyKeys: string[] = [];
	let invalidEntries = 0;
	for (const [rawKey, rawMode] of Object.entries(
		data as Record<string, unknown>,
	)) {
		if (
			typeof rawMode !== "string" ||
			!(VOICE_MODES as readonly string[]).includes(rawMode)
		) {
			invalidEntries++;
			continue;
		}
		const key = String(rawKey);
		if (!key.includes(":")) {
			skippedLegacyKeys.push(key);
			continue;
		}
		result[key] = rawMode as VoiceMode;
	}
	return { modes: result, skippedLegacyKeys, invalidEntries };
}

/** Persist the mode map (indent=2 parity); best-effort — callers warn on false. */
export function saveVoiceModes(
	path: string,
	modes: Record<string, VoiceMode>,
): boolean {
	try {
		mkdirSync(dirname(path), { recursive: true });
		writeFileSync(path, JSON.stringify(modes, null, 2), "utf8");
		return true;
	} catch {
		return false;
	}
}

/** Adapter-side capability probe shape (base.py:_should_auto_tts_for_chat). */
export interface AutoTtsAdapterState {
	enabledChats: Set<string>;
	disabledChats: Set<string>;
	defaultEnabled: boolean;
}

/**
 * The adapter's own decision layers (#16007): explicit /voice on|tts opt-in
 * beats explicit off beats the global default. The RUNNER consults this only
 * when no per-chat mode exists (axis-1 fallback).
 */
export function adapterShouldAutoTtsForChat(
	state: AutoTtsAdapterState,
	chatId: string,
): boolean {
	if (state.enabledChats.has(chatId)) return true;
	if (state.disabledChats.has(chatId)) return false;
	return state.defaultEnabled;
}

/** Minimal turn-message shape needed for dedup axis A. */
export interface TurnMessage {
	role: string;
	toolCalls?: Array<{ name?: string; function?: { name?: string } }>;
}

export interface ShouldSendVoiceReplyInput {
	/** Final response text (sealed stream output or non-streaming final). */
	response: string | null | undefined;
	/** Per-chat PERSISTED mode; undefined ⇒ no explicit chat choice. */
	voiceMode: VoiceMode | undefined;
	/** Axis-1 fallback: adapter's _should_auto_tts_for_chat(chat_id) verdict. */
	adapterAutoTts: boolean;
	/** This turn replied to VOICE input (event.message_type === voice). */
	isVoiceInput: boolean;
	/** THIS turn's messages (from the last user message forward). */
	turnMessages: TurnMessage[];
	/** Streaming already delivered the text (adapter receives None). */
	alreadySent?: boolean;
}

/** The text_to_speech tool name scanned by dedup axis A. */
export const AGENT_TTS_TOOL_NAME = "text_to_speech";

/**
 * The full ladder, in order (§9.4). Pure — all inputs resolved by the caller.
 */
export function shouldSendVoiceReply(
	input: ShouldSendVoiceReplyInput,
): boolean {
	const { response, voiceMode, adapterAutoTts, isVoiceInput, turnMessages } =
		input;

	// Axis 2 — response eligibility: empty or error responses never speak.
	if (!response || response.startsWith("Error:")) return false;

	// Axis 1 — mode resolution, first match wins.
	const should =
		voiceMode === "all" ||
		(voiceMode === "voice_only" && isVoiceInput) ||
		// Global fallback applies ONLY when the chat has no explicit mode;
		// otherwise the chat-level all/voice_only/off choice takes precedence.
		(voiceMode === undefined && adapterAutoTts);
	if (!should) return false;

	// Dedup axis A — agent already called the TTS tool THIS turn.
	const hasAgentTts = turnMessages.some(
		(msg) =>
			msg.role === "assistant" &&
			(msg.toolCalls ?? []).some(
				(tc) => (tc.function?.name ?? tc.name) === AGENT_TTS_TOOL_NAME,
			),
	);
	if (hasAgentTts) return false;

	// Dedup axis B — voice INPUT is spoken by the adapter's auto-TTS path…
	// …UNLESS streaming already consumed the response: the adapter will
	// receive None and can't synthesize, so ownership flips to the runner.
	if (isVoiceInput && !input.alreadySent) return false;

	return true;
}

/** Slice THIS turn out of the session transcript (last user message forward). */
export function messagesForThisTurn(
	agentMessages: TurnMessage[],
): TurnMessage[] {
	for (let i = agentMessages.length - 1; i >= 0; i--) {
		if (agentMessages[i]?.role === "user") return agentMessages.slice(i);
	}
	return agentMessages;
}

// ---------------------------------------------------------------------------
// Injectable TTS synthesis seam — NO real TTS lives here. Production wires a
// real synthesizer; tests wire fakes.
// ---------------------------------------------------------------------------

export interface TtsSynthesisRequest {
	text: string;
	outputPath: string;
}

export interface TtsSynthesisResult {
	success: boolean;
	filePaths?: string[];
	error?: string;
}

/** The seam every lane (auto-TTS reply, future streaming TTS) synthesizes through. */
export interface TtsSynthesizer {
	synthesize(request: TtsSynthesisRequest): Promise<TtsSynthesisResult>;
}

/** Platforms whose native voice bubbles require Ogg/Opus (tts_tool.py:OPUS_VOICE_PLATFORMS). */
export const OPUS_VOICE_PLATFORMS: ReadonlySet<string> = new Set([
	"telegram",
	"matrix",
	"feishu",
	"signal",
]);
// DEC-070: the "whatsapp" (personal bridge) row left with that adapter;
// whatsapp-cloud's voice path is not Ogg/Opus-class, so the set has no WA row.

/**
 * Platform-aware output path (base.py:build_auto_tts_output_path): opus-class
 * platforms get .ogg so container repair guarantees real Ogg/Opus bytes;
 * others keep the MP3 default.
 */
export function buildAutoTtsOutputPath(
	platform: string,
	opts?: { tempRoot?: string },
): string {
	const ext = OPUS_VOICE_PLATFORMS.has((platform ?? "").toLowerCase())
		? "ogg"
		: "mp3";
	return join(
		opts?.tempRoot ?? tmpdir(),
		`pi_voice_${randomUUID().slice(0, 12)}.${ext}`,
	);
}

export interface SendVoiceReplyDeps {
	synthesizer: TtsSynthesizer;
	/** Delivery callback (adapter.send_voice analogue). */
	sendVoice: (audioPath: string) => Promise<void>;
	platform: string;
	tempRoot?: string;
}

/**
 * Synthesize + deliver a voice reply through the seam. Returns the audio path
 * on success, null on any failure (synthesis failure and empty audio both log
 * via the returned reason instead of throwing into the post-turn path).
 */
export async function sendVoiceReply(
	text: string,
	deps: SendVoiceReplyDeps,
): Promise<string | null> {
	const audioPath =
		deps.tempRoot !== undefined
			? buildAutoTtsOutputPath(deps.platform, { tempRoot: deps.tempRoot })
			: buildAutoTtsOutputPath(deps.platform);
	let result: TtsSynthesisResult;
	try {
		result = await deps.synthesizer.synthesize({ text, outputPath: audioPath });
	} catch (e) {
		return null; // synthesis must never break the post-turn flow
	}
	const paths = (
		result.filePaths ?? (result.success ? [audioPath] : [])
	).filter(Boolean);
	if (!result.success || paths.length === 0) return null;
	for (const p of paths) {
		await deps.sendVoice(p);
	}
	return paths[0] ?? null;
}

/**
 * Restore persisted /voice state onto a live adapter's per-chat sets
 * (run.py:_sync_voice_mode_state_to_adapter) so the GLOBAL fallback reads
 * consistently after mutations.
 */
export function syncVoiceModeStateToAdapter(
	modes: Record<string, VoiceMode>,
	state: AutoTtsAdapterState,
	platform: string,
	globalAutoTts: boolean,
): void {
	state.defaultEnabled = globalAutoTts;
	state.disabledChats.clear();
	state.enabledChats.clear();
	const prefix = `${platform}:`;
	for (const [key, mode] of Object.entries(modes)) {
		if (!key.startsWith(prefix)) continue;
		const chatId = key.slice(prefix.length);
		if (mode === "off") state.disabledChats.add(chatId);
		else state.enabledChats.add(chatId);
	}
}
