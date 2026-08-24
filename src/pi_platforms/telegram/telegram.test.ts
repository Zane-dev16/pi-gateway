// pi_platforms/telegram/telegram.test.ts — behavior contracts for the
// telegram census port's MODULE-LEVEL pieces: manifest data vs citations
// (Q17 tiers, capability mapping), MarkdownV2 dialect lanes, A1/A2 reaction
// normalization, M7 sticker cache mechanics, and the fake Bot API server's
// single-offset-space engine view. Adapter/engine behaviors live in the
// conformance suites (shared + transport + shape rows).

import { describe, expect, it } from "vitest";
import { mkdtempSync } from "node:fs";
import { readFile, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { governingTier, type PluginContext } from "../kit/index.js";
import {
	isValidChatAction,
	TELEGRAM_CHAT_ACTIONS,
	TELEGRAM_MANIFEST,
	TELEGRAM_MAX_MESSAGE_UNITS,
	TELEGRAM_RATE_BUDGET,
	threadIdForSend,
	threadIdForTyping,
} from "./manifest.js";
import {
	escapeMarkdownV2,
	isPlainLaneContent,
	toTelegramMarkdownV2,
	toTelegramMarkdownV2Full,
} from "./markdown-v2.js";
import {
	buildAnimatedStickerInjection,
	buildStickerInjection,
	StickerDescriptionCache,
	STICKER_VISION_PROMPT,
} from "./sticker-cache.js";
import {
	parseReactionsEnabled,
	reactionForOutcome,
	REACTION_FAIL,
	REACTION_IN_PROGRESS,
	REACTION_OK,
	normalizeMessageReactionUpdate,
} from "./reactions.js";
import { TelegramAdapter } from "./telegram-adapter.js";
import { registerTelegramPlatform } from "./telegram-adapter.js";
import { TelegramBotApiFake } from "./telegram-fake-server.js";
import { ManualPollingClock } from "../polling/clock.js";

describe("telegram manifest data (Q17/DEC-017 — vendor ground truth as data)", () => {
	it("rate tiers resolve per method class, first-listed tier wins", () => {
		expect(governingTier(TELEGRAM_RATE_BUDGET, "edit")?.name).toBe(
			"stream-edit-envelope",
		);
		expect(governingTier(TELEGRAM_RATE_BUDGET, "typing")?.name).toBe(
			"typing-refresh",
		);
		expect(governingTier(TELEGRAM_RATE_BUDGET, "send")?.name).toBe(
			"chat-message",
		);
		expect(governingTier(undefined, "send")).toBeNull();
	});

	it("capability flags carry the kit-mapping semantics", () => {
		// REQUIRES_EDIT_FINALIZE=True (#25710 anchor).
		expect(TELEGRAM_MANIFEST.capabilities.requiresEditFinalize).toBe(true);
		// splitsLongMessages stays FALSE: kit BASE owns chunking; declaring it
		// true would suppress deliverText splitting entirely.
		expect(TELEGRAM_MANIFEST.capabilities.splitsLongMessages).toBe(false);
		expect(TELEGRAM_MANIFEST.transportShape).toBe("polling");
		expect(TELEGRAM_MANIFEST.requiresEnv.map((e) => e.name)).toEqual([
			"TELEGRAM_BOT_TOKEN",
		]);
	});

	it("forum thread-id placement is ASYMMETRIC between sends and typing", () => {
		expect(threadIdForSend("1")).toBeNull(); // sends reject General-topic id
		expect(threadIdForTyping("1")).toBe(1); // typing PRESERVES it
		expect(threadIdForTyping(undefined)).toBeNull();
		expect(threadIdForSend("42")).toBe(42);
	});

	it("the chat-action matrix is closed over Bot API action names", () => {
		expect(isValidChatAction("typing")).toBe(true);
		expect(isValidChatAction("upload_voice")).toBe(true);
		expect(isValidChatAction("record_video_note")).toBe(true);
		expect(isValidChatAction("dancing")).toBe(false);
		expect(new Set(TELEGRAM_CHAT_ACTIONS).size).toBe(
			TELEGRAM_CHAT_ACTIONS.length,
		);
	});
});

describe("telegram markdown dialects (format_message/_escape_mdv2 ports)", () => {
	it("full conversion escapes MDV2 specials OUTSIDE fenced blocks only", () => {
		const out = toTelegramMarkdownV2Full(
			"a_b (x) ```py\nraw_untouched (1)\n``` tail!",
		);
		expect(out).toContain("a\\_b \\(x\\)");
		expect(out).toContain("```py\nraw_untouched (1)\n```");
		// Prose specials in the tail ARE escaped (correct MDV2); fence bytes raw.
		expect(out.endsWith("tail\\!")).toBe(true);
	});

	it("structural send conversion collapses bold but NEVER escapes punctuation", () => {
		const out = toTelegramMarkdownV2("**bold** stays (1/2) literal");
		expect(out).toBe("*bold* stays (1/2) literal");
	});

	it("escapeMarkdownV2 covers every reserved character", () => {
		expect(escapeMarkdownV2("a.b!c|d#e")).toBe("a\\.b\\!c\\|d\\#e");
	});

	it("plain-lane detection keys off prefix or explicit parse_mode none", () => {
		expect(
			isPlainLaneContent(
				"(Response formatting failed, plain text:)\n\nx",
				undefined,
			),
		).toBe(true);
		expect(isPlainLaneContent("anything", "none")).toBe(true);
		expect(isPlainLaneContent("normal text", undefined)).toBe(false);
	});

	it("vision prompt is the concise sticker prompt", () => {
		expect(STICKER_VISION_PROMPT.startsWith("Describe this sticker")).toBe(
			true,
		);
	});
});

describe("telegram reactions (A1/A2)", () => {
	it("gate parse is opt-in fail-closed", () => {
		expect(parseReactionsEnabled(undefined)).toBe(false);
		expect(parseReactionsEnabled("")).toBe(false);
		expect(parseReactionsEnabled("false")).toBe(false);
		expect(parseReactionsEnabled("0")).toBe(false);
		expect(parseReactionsEnabled("no")).toBe(false);
		expect(parseReactionsEnabled("true")).toBe(true);
		expect(parseReactionsEnabled("1")).toBe(true);
	});

	it("outcome mapping: success/failure swap emoji set, cancelled clears", () => {
		expect(reactionForOutcome("success")).toEqual({
			kind: "set",
			emoji: REACTION_OK,
		});
		expect(reactionForOutcome("failure")).toEqual({
			kind: "set",
			emoji: REACTION_FAIL,
		});
		expect(reactionForOutcome("cancelled")).toEqual({ kind: "clear" });
		expect(REACTION_IN_PROGRESS).toBe("\u{1F440}");
	});

	it("normalization enforces the hard caps and tolerates invalid shapes", () => {
		const capped = normalizeMessageReactionUpdate({
			message_id: 5,
			chat: { id: 9 },
			new_reaction: Array.from({ length: 70 }, () => ({
				emoji: "e".repeat(80),
				custom_emoji_id: "c".repeat(200),
			})),
		});
		expect(capped?.payload.emojis.length).toBe(64);
		expect(capped?.payload.emojis[0]?.length).toBe(64);
		expect(capped?.payload.customEmojiIds[0]?.length).toBe(128);

		expect(normalizeMessageReactionUpdate(undefined)).toBeNull();
		expect(normalizeMessageReactionUpdate({ new_reaction: [] })).toBeNull(); // no ids
		expect(normalizeMessageReactionUpdate({ message_id: 1 })).toBeNull();
		// Malformed wire data (boolean id) coerces to null — hostile shapes
		// never throw.
		const hostile = { message_id: true as unknown as string, chat: {} };
		expect(normalizeMessageReactionUpdate(hostile)).toBeNull();
	});
});

describe("sticker description cache (M7)", () => {
	it("mkdtemp-isolated file with ATOMIC saves (no temp litter)", async () => {
		const dir = mkdtempSync(join(tmpdir(), "pi-tg-test-"));
		const cache = new StickerDescriptionCache({ dir, nowMs: () => 1000 });
		await cache.cacheStickerDescription("fid-1", "a cat", "😀", "Pack");
		// Injected clock stamped into the JSON file (string assertions — no
		// parse of possibly-corrupt content in tests).
		const raw = await readFile(cache.path, "utf8");
		expect(raw).toContain('"fid-1"');
		expect(raw).toContain('"cached_at": 1000');
		const { readdirSync } = await import("node:fs");
		const files = readdirSync(dir);
		expect(files.every((f) => !f.includes(".tmp"))).toBe(true);
		const hit = await cache.getCachedDescription("fid-1");
		expect(hit?.description).toBe("a cat");
		expect(hit?.setName).toBe("Pack");
	});

	it("corrupt cache files degrade to EMPTY, never throw", async () => {
		const dir = mkdtempSync(join(tmpdir(), "pi-tg-corrupt-"));
		const cache = new StickerDescriptionCache({ dir, nowMs: () => 0 });
		await writeFile(join(dir, "sticker_cache.json"), "{not json!!", "utf8");
		await expect(cache.getCachedDescription("any")).resolves.toBeUndefined();
		// Overwrite-on-save heals the file.
		await cache.cacheStickerDescription("fid", "desc");
		await expect(cache.getCachedDescription("fid")).resolves.toMatchObject({
			description: "desc",
		});
	});

	it("injection formats are byte-exact warm-style strings", () => {
		expect(buildStickerInjection("A cat waving", "😀", "MyPack")).toBe(
			'[The user sent a sticker 😀 from "MyPack"~ It shows: "A cat waving" (=^.w.^=)]',
		);
		expect(buildStickerInjection("A cat", "😀")).toBe(
			'[The user sent a sticker 😀~ It shows: "A cat" (=^.w.^=)]',
		);
		expect(buildAnimatedStickerInjection()).toBe(
			"[The user sent an animated sticker~ I can't see animated ones yet]",
		);
		expect(buildAnimatedStickerInjection("🔥")).toBe(
			"[The user sent an animated sticker 🔥~ I can't see animated ones yet, but the emoji suggests: 🔥]",
		);
	});
});

describe("telegram adapter construction (production defaults + registration)", () => {
	function makeAdapter(): TelegramAdapter {
		return new TelegramAdapter({
			wire: new TelegramBotApiFake(),
			clock: new ManualPollingClock(),
			secretReader: () => "tok",
		});
	}

	it("production scalar budget is the manifest 4096 UTF-16 units", () => {
		const adapter = makeAdapter();
		const policy = adapter.chatLengthPolicyForChat("some-chat");
		expect(policy.maxUnits).toBe(TELEGRAM_MAX_MESSAGE_UNITS);
		expect(policy.lenFn("🎉".repeat(3))).toBe(6); // UTF-16 code units
	});

	it("reactions gate resolves from the OPTIONAL env reader at construction", async () => {
		const off = new TelegramAdapter({
			wire: new TelegramBotApiFake(),
			secretReader: () => "tok",
		});
		expect(off.reactionsEnabled).toBe(false);
		const on = new TelegramAdapter({
			wire: new TelegramBotApiFake(),
			secretReader: () => "tok",
			optionalEnvReader: (k) =>
				k === "TELEGRAM_REACTIONS" ? "true" : undefined,
		});
		expect(on.reactionsEnabled).toBe(true);
	});

	it("registration path registers the telegram platform under its manifest name", () => {
		const registered: string[] = [];
		const ctx = {
			registerPlatform: (manifest: { name: string }) => {
				registered.push(manifest.name);
				return null;
			},
		} as unknown as PluginContext;
		registerTelegramPlatform(ctx, () => makeAdapter());
		expect(registered).toEqual(["telegram"]);
	});
});

describe("fake Bot API server — engine plane over mixed-kind updates", () => {
	it("ONE offset space ACKs every update kind; pending drains to zero", async () => {
		const tg = new TelegramBotApiFake();
		tg.pushTextUpdate(10, "text one");
		tg.pushRawUpdate({
			message_reaction: { message_id: 1, chat: { id: 10 }, new_reaction: [] },
		});
		tg.pushCallbackUpdate({
			hostChatId: 10,
			hostMessageId: 55,
			data: "ea:once:7",
		});
		expect(tg.pendingUpdateCount).toBe(3);

		const token = tg.openSession();
		const batch = await tg.getUpdates({
			sessionToken: token,
			offset: 1,
			timeoutMs: 0,
		});
		expect(batch.updates.length).toBe(3);
		// Engine view flattens non-message kinds to text "" — routing happens
		// adapter-side against the raw registry. Find the CALLBACK kind by its
		// raw payload (reaction updates also flatten to "").
		expect(batch.updates.every((u) => typeof u.updateId === "number")).toBe(
			true,
		);
		const callbackEntry = batch.updates.find(
			(u) => tg.rawUpdateFor(u.updateId)?.callback_query !== undefined,
		);
		expect(callbackEntry).toBeDefined();
		const raw = tg.rawUpdateFor(callbackEntry?.updateId ?? -1);
		expect(raw?.callback_query?.data).toBe("ea:once:7");

		tg.commitOffset(
			token,
			(batch.updates[batch.updates.length - 1]?.updateId ?? 0) + 1,
		);
		expect(tg.pendingUpdateCount).toBe(0);
	});

	it("callback answers / markup strips / reactions capture on the control plane", async () => {
		const tg = new TelegramBotApiFake();
		await tg.answerCallbackQuery({ callback_query_id: "cbq1", text: "ok" });
		await tg.editMessageReplyMarkup({
			chat_id: 1,
			message_id: 2,
			reply_markup: null,
		});
		await tg.setMessageReaction({
			chat_id: 1,
			message_id: 2,
			reaction: "\u{1F440}",
		});
		await tg.setMessageReaction({ chat_id: 1, message_id: 2, reaction: null }); // clear
		expect(tg.callbackAnswers.length).toBe(1);
		expect(tg.replyMarkupEdits[0]?.markup).toBeNull();
		expect(tg.reactionOps.map((o) => o.reaction)).toEqual(["\u{1F440}", null]);
	});
});
