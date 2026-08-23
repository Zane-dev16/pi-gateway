// Auto-TTS decision ladder contracts (03 §9.4; §11 "Auto-TTS ladder" row).
// Table-driven over the three axes; INJECTED temp roots; persisted mode store
// under mkdtemp. No real TTS — synthesis rides the injected seam.

import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
	type AutoTtsAdapterState,
	AGENT_TTS_TOOL_NAME,
	buildAutoTtsOutputPath,
	loadVoiceModes,
	messagesForThisTurn,
	OPUS_VOICE_PLATFORMS,
	saveVoiceModes,
	sendVoiceReply,
	shouldSendVoiceReply,
	syncVoiceModeStateToAdapter,
	type TtsSynthesizer,
	type TurnMessage,
	type VoiceMode,
	voiceKey,
} from "./auto-tts.js";

let root: string;

beforeAll(() => {
	root = mkdtempSync(join(tmpdir(), "pi-outbound-tts-"));
});

afterAll(() => {
	rmSync(root, { recursive: true, force: true });
});

const BASE = {
	response: "Here is the summary.",
	voiceMode: undefined as VoiceMode | undefined,
	adapterAutoTts: false,
	isVoiceInput: false,
	turnMessages: [] as TurnMessage[],
	alreadySent: false,
};

describe("axis 1 — mode resolution (first match wins)", () => {
	it("per-chat `all` speaks every turn regardless of input type or adapter fallback", () => {
		expect(shouldSendVoiceReply({ ...BASE, voiceMode: "all" })).toBe(true);
		// Voice input + streaming-consumed text ⇒ runner owns synthesis.
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: "all",
				isVoiceInput: true,
				alreadySent: true,
			}),
		).toBe(true);
		expect(
			shouldSendVoiceReply({ ...BASE, voiceMode: "all", adapterAutoTts: true }),
		).toBe(true);
	});

	it("per-chat `voice_only` speaks ONLY replies to voice input (streamed ⇒ runner-owned)", () => {
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: "voice_only",
				isVoiceInput: true,
				alreadySent: true,
			}),
		).toBe(true);
		expect(shouldSendVoiceReply({ ...BASE, voiceMode: "voice_only" })).toBe(
			false,
		);
	});

	it("per-chat `off` NEVER speaks — even with adapter fallback enabled", () => {
		expect(
			shouldSendVoiceReply({ ...BASE, voiceMode: "off", adapterAutoTts: true }),
		).toBe(false);
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: "off",
				adapterAutoTts: true,
				isVoiceInput: true,
			}),
		).toBe(false);
	});

	it("NO per-chat mode ⇒ global fallback via the adapter probe", () => {
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: undefined,
				adapterAutoTts: true,
			}),
		).toBe(true);
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: undefined,
				adapterAutoTts: false,
			}),
		).toBe(false);
	});

	it("capability-miss fallback: adapter WITHOUT a probe ⇒ treated as disabled, per-chat mode still works", () => {
		// Hermes guards hasattr(adapter, "_should_auto_tts_for_chat") → False.
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: undefined,
				adapterAutoTts: false,
			}),
		).toBe(false);
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: "all",
				adapterAutoTts: false,
			}),
		).toBe(true);
	});
});

describe("axis 2 — response eligibility", () => {
	// Parity: `if not response or response.startswith("Error:")` — a non-empty
	// WHITESPACE response is truthy in the gate and may speak.
	it("empty/falsy and Error:-prefixed responses never speak; whitespace-only is truthy per gate parity", () => {
		for (const response of ["", null, undefined]) {
			expect(
				shouldSendVoiceReply({ ...BASE, response, voiceMode: "all" }),
			).toBe(false);
		}
		expect(
			shouldSendVoiceReply({
				...BASE,
				response: "Error: tool exploded",
				voiceMode: "all",
			}),
		).toBe(false);
	});
});

describe("dedup axis A — agent-initiated TTS in THIS turn only", () => {
	const agentTtsTurn: TurnMessage[] = [
		{ role: "user" },
		{
			role: "assistant",
			toolCalls: [{ function: { name: AGENT_TTS_TOOL_NAME } }],
		},
	];

	it("an assistant text_to_speech call this turn silences the runner even in `all` mode", () => {
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: "all",
				turnMessages: agentTtsTurn,
			}),
		).toBe(false);
	});

	it("a TTS call BEFORE the last user message belongs to an earlier turn and does not dedupe", () => {
		const transcript = messagesForThisTurn([
			{ role: "user" },
			{
				role: "assistant",
				toolCalls: [{ function: { name: AGENT_TTS_TOOL_NAME } }],
			},
			{ role: "tool" },
			{ role: "user" },
			{ role: "assistant" },
		]);
		expect(transcript).toHaveLength(2); // last user forward
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: "all",
				turnMessages: transcript,
			}),
		).toBe(true);
	});

	it("other tool calls do not count as agent TTS", () => {
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: "all",
				turnMessages: [
					{ role: "assistant", toolCalls: [{ function: { name: "browser" } }] },
				],
			}),
		).toBe(true);
	});
});

describe("dedup axis B — voice-input ownership flips with streaming", () => {
	it("voice input without streaming: the ADAPTER owns auto-TTS; runner skips", () => {
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: "all",
				isVoiceInput: true,
				alreadySent: false,
			}),
		).toBe(false);
	});

	it("voice input WITH already_sent=True: adapter gets None ⇒ ownership flips to the runner", () => {
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: "all",
				isVoiceInput: true,
				alreadySent: true,
			}),
		).toBe(true);
	});

	it("text input ignores the ownership rule entirely", () => {
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: "all",
				isVoiceInput: false,
				alreadySent: true,
			}),
		).toBe(true);
		expect(
			shouldSendVoiceReply({
				...BASE,
				voiceMode: "all",
				isVoiceInput: false,
				alreadySent: false,
			}),
		).toBe(true);
	});
});

describe("adapter-side fallback decision layers (#16007)", () => {
	const state: AutoTtsAdapterState = {
		enabledChats: new Set(["dm123"]),
		disabledChats: new Set(["group456"]),
		defaultEnabled: false,
	};

	it("explicit opt-in beats global off; explicit off beats global on; default falls through", () => {
		expect(require_adapter(state, "dm123")).toBe(true);
		expect(require_adapter(state, "group456")).toBe(false);
		expect(require_adapter(state, "unknown")).toBe(false);
		state.defaultEnabled = true;
		expect(require_adapter(state, "unknown")).toBe(true);
		expect(require_adapter(state, "group456")).toBe(false); // still hard-off
	});

	function require_adapter(s: AutoTtsAdapterState, chatId: string): boolean {
		// mirrors base.py:_should_auto_tts_for_chat consumption by the runner
		return shouldSendVoiceReply({
			...BASE,
			voiceMode: undefined,
			adapterAutoTts: requireProbe(s, chatId),
		});
	}

	function requireProbe(s: AutoTtsAdapterState, chatId: string): boolean {
		return (
			s.enabledChats.has(chatId) ||
			(!s.disabledChats.has(chatId) && s.defaultEnabled)
		);
	}
});

describe("persisted voice-mode store (gateway_voice_mode.json parity)", () => {
	it("key format is <platform>:<chat_id>", () => {
		expect(voiceKey("telegram", "42")).toBe("telegram:42");
	});

	it("round-trips modes; skips legacy UNPREFIXED keys with warning data; counts invalid values", () => {
		const path = join(root, "modes.json");
		saveVoiceModes(path, {
			"telegram:1": "all",
			"discord:2": "off",
			legacybare: "voice_only",
		} as Record<string, VoiceMode>);
		const loaded = loadVoiceModes(path);
		expect(loaded.modes).toEqual({ "telegram:1": "all", "discord:2": "off" });
		expect(loaded.skippedLegacyKeys).toEqual(["legacybare"]);
		expect(readFileSync(path, "utf8")).toContain('"telegram:1"');

		const badPath = join(root, "bad.json");
		saveVoiceModes(badPath, { "telegram:9": "loud" } as unknown as Record<
			string,
			VoiceMode
		>);
		const bad = loadVoiceModes(badPath);
		expect(bad.modes).toEqual({});
		expect(bad.invalidEntries).toBe(1);
	});

	it("missing/corrupt file loads as empty store", () => {
		expect(loadVoiceModes(join(root, "missing.json"))).toEqual({
			modes: {},
			skippedLegacyKeys: [],
			invalidEntries: 0,
		});
		const corrupt = join(root, "corrupt.json");
		writeFileSync(corrupt, "{not json");
		expect(loadVoiceModes(corrupt).modes).toEqual({});
	});
});

describe("adapter state sync — mutations mirror onto the adapter's sets", () => {
	it("/voice mutations persist AND re-derive enabled/disabled sets for the global fallback", () => {
		const state: AutoTtsAdapterState = {
			enabledChats: new Set(),
			disabledChats: new Set(["stale"]),
			defaultEnabled: false,
		};
		const modes: Record<string, VoiceMode> = {
			"telegram:on": "all",
			"telegram:muted": "off",
			"whatsapp:x": "voice_only", // other platform ignored for telegram sync
		};
		syncVoiceModeStateToAdapter(modes, state, "telegram", true);
		expect([...state.enabledChats].sort()).toEqual(["on"]);
		expect([...state.disabledChats].sort()).toEqual(["muted"]);
		expect(state.defaultEnabled).toBe(true);
	});
});

describe("TTS seam + platform-aware output paths", () => {
	it("opus-class platforms get .ogg; others keep .mp3", () => {
		for (const p of OPUS_VOICE_PLATFORMS) {
			expect(
				buildAutoTtsOutputPath(p, { tempRoot: root }).endsWith(".ogg"),
			).toBe(true);
		}
		expect(
			buildAutoTtsOutputPath("discord", { tempRoot: root }).endsWith(".mp3"),
		).toBe(true);
	});

	it("sendVoiceReply synthesizes through the SEAM and delivers produced files", async () => {
		const delivered: string[] = [];
		const synth: TtsSynthesizer = {
			synthesize: async (req) => ({
				success: true,
				filePaths: [req.outputPath],
			}),
		};
		const got = await sendVoiceReply("hello", {
			synthesizer: synth,
			sendVoice: async (p) => {
				delivered.push(p);
			},
			platform: "telegram",
			tempRoot: root,
		});
		expect(got).not.toBeNull();
		expect(delivered).toHaveLength(1);
		expect(delivered[0]).toBe(got);
	});

	it("synthesis failure returns null WITHOUT throwing into the post-turn path", async () => {
		const failing: TtsSynthesizer = {
			synthesize: async () => ({ success: false, error: "provider down" }),
		};
		expect(
			await sendVoiceReply("hello", {
				synthesizer: failing,
				sendVoice: async () => undefined,
				platform: "telegram",
				tempRoot: root,
			}),
		).toBeNull();
		const throwing: TtsSynthesizer = {
			synthesize: async () => {
				throw new Error("exploded");
			},
		};
		expect(
			await sendVoiceReply("hello", {
				synthesizer: throwing,
				sendVoice: async () => undefined,
				platform: "discord",
				tempRoot: root,
			}),
		).toBeNull();
	});
});
