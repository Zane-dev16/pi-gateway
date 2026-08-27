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
import { buildExecApprovalCallback } from "../kit/index.js";
import {
	TELEGRAM_ALLOWED_UPDATES,
	TELEGRAM_CHAT_ACTION,
	TELEGRAM_DEFAULT_ALLOWED_UPDATES,
	TELEGRAM_MANIFEST,
	TELEGRAM_MAX_MESSAGE_UNITS,
	TELEGRAM_RATE_BUDGET,
	TELEGRAM_STICKER_CACHE_TTL_MS,
	notificationKwargs,
	resolveTelegramNotificationsMode,
	threadIdForSend,
	threadIdForTyping,
} from "./manifest.js";
import {
	escapeMarkdownV2,
	isPlainLaneContent,
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
import {
	metadataDirectMessagesTopicId,
	metadataReplyToMessageId,
	metadataThreadId,
	threadKwargsForSend,
	TELEGRAM_DM_TOPIC_MISSING_ANCHOR_ERROR,
} from "./manifest.js";
import { normalizeTelegramChatId } from "./telegram-ids.js";
import {
	fitsRichLimits,
	isRichEligibleContent,
	needsRichRendering,
	richNormalizeLinebreaks,
} from "./rich-messages.js";
import { normalizeMessageEditedEvent } from "./platform-events.js";
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

	it("production typing emits ONLY action='typing' (adapter.py:send_typing)", () => {
		// The baseline adapter sends exactly ONE chat action ever (:8400/:8412).
		expect(TELEGRAM_CHAT_ACTION).toBe("typing");
	});

	it("every poll requests Update.ALL_TYPES (tg-1)", () => {
		// Without allowed_updates real Telegram's default filter EXCLUDES the
		// reaction kinds — the fake models that default explicitly.
		expect(TELEGRAM_ALLOWED_UPDATES).toContain("message");
		expect(TELEGRAM_ALLOWED_UPDATES).toContain("message_reaction");
		expect(TELEGRAM_ALLOWED_UPDATES).toContain("callback_query");
		expect(TELEGRAM_DEFAULT_ALLOWED_UPDATES).not.toContain("message_reaction");
		expect(new Set(TELEGRAM_ALLOWED_UPDATES).size).toBe(
			TELEGRAM_ALLOWED_UPDATES.length,
		);
	});

	it("notification mode resolves important-by-default with all opt-in", () => {
		expect(resolveTelegramNotificationsMode(undefined)).toBe("important");
		expect(resolveTelegramNotificationsMode("")).toBe("important");
		expect(resolveTelegramNotificationsMode("IMPORTANT")).toBe("important");
		expect(resolveTelegramNotificationsMode("all")).toBe("all");
		// Unknown values warn-and-default in Hermes — same landing here.
		expect(resolveTelegramNotificationsMode("bogus")).toBe("important");
	});

	it("notification kwargs silence by default and honor notify metadata", () => {
		const meta = { notify: true } as unknown as Record<string, unknown>;
		expect(notificationKwargs("important", meta)).toEqual({});
		expect(notificationKwargs("important", undefined)).toEqual({
			disable_notification: true,
		});
		expect(notificationKwargs("all", undefined)).toEqual({});
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

	it("converted lane output always parses under parse_mode=MarkdownV2", () => {
		// Send-lane truth (tg-2): chunk markers "(1/2)" ship escaped like the
		// rest of format_message output ('\\(1/2\\)'), so Telegram cannot
		// reject the chunk for unescaped entities.
		expect(toTelegramMarkdownV2Full("body (1/2)")).toBe("body \\(1/2\\)");
		expect(toTelegramMarkdownV2Full("**bold** stays literal")).toBe(
			"*bold* stays literal",
		);
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

	it("sticker cache TTL defaults to EXACT Hermes read semantics (tg-14)", () => {
		// gateway/sticker_cache.py:get_cached_description NEVER expires entries;
		// Infinity is parity, not a deviation (the old "DEC-043" citation was
		// the dingtalk exclusion — unrelated).
		expect(TELEGRAM_STICKER_CACHE_TTL_MS).toBe(Number.POSITIVE_INFINITY);
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

describe("telegram-wire-r2 pure helpers (tg2-x)", () => {
	it("tg2-7 normalizeTelegramChatId coerces numeric ids to ints and preserves usernames", () => {
		expect(normalizeTelegramChatId("123456789")).toBe(123456789);
		expect(normalizeTelegramChatId("-1001234567890")).toBe(-1001234567890);
		expect(normalizeTelegramChatId(" 42 ")).toBe(42);
		expect(normalizeTelegramChatId("@room")).toBe("@room");
		expect(normalizeTelegramChatId("chat-x")).toBe("chat-x");
	});

	it("tg2-4 metadata accessors mirror adapter.py classmethods", () => {
		expect(metadataThreadId({ thread_id: "7" })).toBe("7");
		expect(metadataThreadId({ message_thread_id: 8 })).toBe("8");
		expect(metadataThreadId(undefined)).toBeNull();
		expect(metadataDirectMessagesTopicId({ direct_messages_topic_id: 1 })).toBe(
			"1",
		);
		expect(
			metadataDirectMessagesTopicId({
				telegram_direct_messages_topic_id: 2,
			}),
		).toBe("2");
		expect(
			metadataReplyToMessageId({ telegram_reply_to_message_id: "9" }),
		).toBe(9);
	});

	it("tg2-4 threadKwargsForSend routes forum vs DM-topic lanes", () => {
		// Forum topic id ships as message_thread_id.
		expect(threadKwargsForSend("7", {}, null)).toEqual({ messageThreadId: 7 });
		// General-topic '1' maps away on sends.
		expect(threadKwargsForSend("1", {}, null)).toEqual({
			messageThreadId: null,
		});
		// Explicit DM-topic id pairs with an OMITTED thread id.
		expect(
			threadKwargsForSend(null, { direct_messages_topic_id: 5 }, null),
		).toEqual({
			messageThreadId: null,
			directMessagesTopicId: 5,
		});
		// Anchor-less fallback prefers the topic thread, then the dm topic id.
		expect(
			threadKwargsForSend(
				"7",
				{ telegram_dm_topic_reply_fallback: true },
				null,
			),
		).toEqual({ messageThreadId: 7 });
		expect(
			threadKwargsForSend(
				null,
				{
					telegram_dm_topic_reply_fallback: true,
					direct_messages_topic_id: 5,
				},
				null,
			),
		).toEqual({ messageThreadId: null, directMessagesTopicId: 5 });
		expect(
			threadKwargsForSend(
				null,
				{ telegram_dm_topic_reply_fallback: true },
				null,
			),
		).toEqual({});
	});

	it("tg2-4 the DM-topic missing-anchor error text is EXACT", () => {
		expect(TELEGRAM_DM_TOPIC_MISSING_ANCHOR_ERROR).toBe(
			"Telegram DM topic delivery requires a reply anchor; refusing to send outside the requested topic",
		);
	});

	it("tg2-6 rich eligibility reserves sendRichMessage for degrading constructs", () => {
		expect(needsRichRendering("| a | b |\n| - | - |")).toBe(true);
		expect(needsRichRendering("- [ ] task")).toBe(true);
		expect(needsRichRendering("<details><summary>s</summary>x</details>")).toBe(
			true,
		);
		expect(needsRichRendering("math $$x^2$$")).toBe(true);
		expect(needsRichRendering("plain prose")).toBe(false);
		expect(isRichEligibleContent("plain prose")).toBe(false);
		expect(fitsRichLimits("x".repeat(32768))).toBe(true);
		expect(fitsRichLimits("x".repeat(32769))).toBe(false);
		// Details+math crash shape skips rich (TDesktop #30808).
		expect(isRichEligibleContent("<details>$$x$$</details>")).toBe(false);
	});

	it("tg2-6 rich linebreak normalization protects code fences and tables", () => {
		expect(richNormalizeLinebreaks("a\nb")).toBe("a  \nb");
		expect(richNormalizeLinebreaks("a\n\nb")).toBe("a\n\nb");
		const table = "| a | b |\n| - | - |\n| 1 | 2 |";
		expect(richNormalizeLinebreaks(table)).toBe(table);
		const fenced = "```py\nx = 1\ny = 2\n```";
		expect(richNormalizeLinebreaks(fenced)).toBe(fenced);
	});

	it("tg2-11 edited_message updates normalize to bounded platform events", () => {
		const event = normalizeMessageEditedEvent({
			message_id: 55,
			chat: { id: 100 },
			text: "edited body",
			message_thread_id: 9,
			is_topic_message: true,
			edit_date: 1760000500,
		});
		expect(event?.eventType).toBe("message_edited");
		expect(event?.payload.chatId).toBe("100");
		expect(event?.payload.messageId).toBe("55");
		expect(event?.payload.threadId).toBe("9");
		expect(event?.payload.editedAt).toBe(
			new Date(1760000500 * 1000).toISOString(),
		);
		// Topic-less edits carry NO thread id; caption falls back for media.
		const noTopic = normalizeMessageEditedEvent({
			message_id: 56,
			chat: { id: 100 },
			caption: "media caption",
		});
		expect(noTopic?.payload.threadId).toBeUndefined();
		expect(noTopic?.payload.text).toBe("media caption");
		// Malformed identities return null.
		expect(normalizeMessageEditedEvent({ chat: {} })).toBeNull();
		expect(
			normalizeMessageEditedEvent({
				message_id: true as unknown as number,
				chat: { id: 1 },
			}),
		).toBeNull();
	});
});

describe("fake Bot API server — engine plane over mixed-kind updates", () => {
	it("ONE offset space ACKs every REQUESTED update kind (tg-1 default filter)", async () => {
		const tg = new TelegramBotApiFake();
		tg.pushTextUpdate(10, "text one");
		tg.pushRawUpdate({
			message_reaction: {
				message_id: 1,
				chat: { id: 10 },
				new_reaction: [],
			},
		});
		tg.pushCallbackUpdate({
			hostChatId: 10,
			hostMessageId: 55,
			data: "ea:once:7",
		});
		expect(tg.pendingUpdateCount).toBe(3);

		// REAL-API DEFAULT filter: without allowed_updates the reaction kind
		// is NEVER delivered — a poller that forgets it loses reactions.
		const token = tg.openSession();
		const batch = await tg.getUpdates({
			sessionToken: token,
			offset: 1,
			timeoutMs: 0,
		});
		expect(batch.updates.length).toBe(2); // message + callback only
		expect(
			batch.updates.some(
				(u) => tg.rawUpdateFor(u.updateId)?.message_reaction !== undefined,
			),
		).toBe(false);

		// ALL_TYPES delivers every pushed kind.
		const full = await tg.getUpdates({
			sessionToken: token,
			offset: 1,
			timeoutMs: 0,
			allowedUpdates: TELEGRAM_ALLOWED_UPDATES,
		});
		expect(full.updates.length).toBe(3);
		const callbackEntry = full.updates.find(
			(u) => tg.rawUpdateFor(u.updateId)?.callback_query !== undefined,
		);
		expect(callbackEntry).toBeDefined();
		const raw = tg.rawUpdateFor(callbackEntry?.updateId ?? -1);
		expect(raw?.callback_query?.data).toBe("ea:once:7");
		// The capture log records exactly what each poll requested.
		expect(tg.allowedUpdatesLog).toEqual([undefined, TELEGRAM_ALLOWED_UPDATES]);
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

	it("deleteWebhook captures drop_pending_updates=false and scripts failures (tg-7)", async () => {
		const tg = new TelegramBotApiFake();
		await tg.deleteWebhook({ drop_pending_updates: false });
		expect(tg.webhookDeletes).toEqual([{ dropPendingUpdates: false, seq: 1 }]);

		tg.scriptWebhook({ kind: "fail", error: "getaddrinfo ENOTFOUND" });
		await expect(tg.deleteWebhook()).rejects.toThrow(/ENOTFOUND/);
		expect(tg.webhookDeletes.length).toBe(1); // failed call NOT captured as done
	});

	it("media family captures FULL kwarg sets per method (tg-9)", async () => {
		const tg = new TelegramBotApiFake();
		const ok = await tg.sendPhoto({
			chat_id: 5,
			photo: "file:///tmp/x.png",
			caption: "cap",
			reply_to_message_id: 9,
			message_thread_id: 3,
			disable_notification: true,
		});
		expect(ok.success).toBe(true);
		tg.scriptMedia("sendVoice", { kind: "fail", error: "voice boom" });
		const failed = await tg.sendVoice({ chat_id: 5, voice: "/tmp/a.ogg" });
		expect(failed.success).toBe(false);
		const sent = await tg.sendAudio({ chat_id: 5, audio: "/tmp/a.mp3" });
		expect(sent.success).toBe(true);
		await tg.sendVideo({ chat_id: 5, video: "/tmp/v.mp4" });
		await tg.sendAnimation({ chat_id: 5, animation: "https://x/y.gif" });

		expect(tg.mediaOps.map((o) => o.method)).toEqual([
			"sendPhoto",
			"sendAudio",
			"sendVideo",
			"sendAnimation",
		]);
		expect(tg.mediaOps[0]?.args["disable_notification"]).toBe(true);
		expect(tg.mediaOps[0]?.args["message_thread_id"]).toBe(3);
	});

	it("sendMessage/editMessageText capture their FULL kwargs", async () => {
		const tg = new TelegramBotApiFake();
		await tg.sendMessage({
			chat_id: 7,
			text: "hi",
			parse_mode: "MarkdownV2",
			disable_notification: true,
			reply_to_message_id: 4,
		});
		await tg.editMessageText({
			chat_id: 7,
			message_id: 4,
			text: "edited",
			parse_mode: "MarkdownV2",
		});
		expect(tg.sendKwargs[0]?.["parse_mode"]).toBe("MarkdownV2");
		expect(tg.sendKwargs[0]?.["disable_notification"]).toBe(true);
		expect(tg.editKwargs[0]?.["parse_mode"]).toBe("MarkdownV2");
	});

	it("sendMessageDraft captures FULL kwargs with no message id on success (tg-13)", async () => {
		const tg = new TelegramBotApiFake();
		const ok = await tg.sendMessageDraft({
			chat_id: 42,
			draft_id: 7,
			text: "partial *bold*",
			parse_mode: "MarkdownV2",
		});
		expect(ok.success).toBe(true);
		expect(ok.messageId).toBeUndefined(); // drafts carry NO message id (:6207)
		expect(tg.draftKwargs[0]?.["chat_id"]).toBe(42);
		expect(tg.draftKwargs[0]?.["draft_id"]).toBe(7);
		expect(tg.draftKwargs[0]?.["parse_mode"]).toBe("MarkdownV2");

		tg.scriptDraft({
			kind: "fail",
			error: "Bad Request: can't parse entities",
		});
		const failed = await tg.sendMessageDraft({
			chat_id: 1,
			draft_id: 2,
			text: "x",
		});
		expect(failed.success).toBe(false);
		expect(tg.draftKwargs.length).toBe(2); // failed attempts stay assertable
	});

	it("deleteMessage captures {chat_id, message_id} and scripts failures (tg-12)", async () => {
		const tg = new TelegramBotApiFake();
		await tg.deleteMessage({ chat_id: 4242, message_id: 88 });
		expect(tg.deleteOps).toEqual([{ chatId: "4242", messageId: "88", seq: 1 }]);

		tg.scriptDelete({
			kind: "fail",
			error: "Bad Request: message to delete not found",
		});
		const failed = await tg.deleteMessage({ chat_id: 4242, message_id: 89 });
		expect(failed.success).toBe(false);
		expect(tg.deleteOps.length).toBe(1); // failed call NOT captured
	});
});

describe("telegram closures wave (tg-11/tg-12/tg-13 — Hermes-truth contracts)", () => {
	function makeAdapterHere(): TelegramAdapter {
		return new TelegramAdapter({
			wire: new TelegramBotApiFake(),
			clock: new ManualPollingClock(),
			secretReader: () => "tok",
		});
	}
	function makeAdapterWith(botWire: TelegramBotApiFake): TelegramAdapter {
		return new TelegramAdapter({
			wire: botWire,
			clock: new ManualPollingClock(),
			secretReader: () => "tok",
		});
	}

	/** Env control helper (multiplex OFF ⇒ authz accessors read process.env;
	 * same sanctioned pattern as security/authz/decision.test.ts). ASYNC-AWARE:
	 * the save/restore MUST bracket the ENTIRE awaited body — a sync try/finally
	 * restores env the moment the body hits its first await, so any env read
	 * performed AFTER that suspension (e.g. a SECOND route() in one block)
	 * observes already-emptied vars and fails closed against nothing. */
	async function withAuthzEnv<T>(
		vars: Record<string, string | undefined>,
		fn: () => T,
	): Promise<Awaited<T>> {
		const KEYS = [
			"TELEGRAM_ALLOWED_USERS",
			"TELEGRAM_GROUP_ALLOWED_USERS",
			"TELEGRAM_GROUP_ALLOWED_CHATS",
			"TELEGRAM_ALLOW_ALL_USERS",
			"GATEWAY_ALLOWED_USERS",
			"GATEWAY_ALLOW_ALL_USERS",
		];
		const saved = new Map<string, string | undefined>();
		for (const k of KEYS) saved.set(k, process.env[k]);
		for (const k of KEYS) delete process.env[k];
		try {
			for (const [k, v] of Object.entries(vars)) {
				if (v !== undefined) process.env[k] = v;
			}
			return await fn();
		} finally {
			for (const [k, v] of saved) {
				if (v === undefined) delete process.env[k];
				else process.env[k] = v;
			}
		}
	}

	it("tg-11: NO callback resolution without session authorization (fail-closed default)", async () => {
		await withAuthzEnv({}, async () => {
			const adapter = makeAdapterHere();
			adapter.approvals.register(11, "sk-tg11");
			const answer = await adapter.router.route(
				buildExecApprovalCallback("once", 11),
				{ userId: "31337", chatId: "77", chatType: "private" },
			);
			// Unconfigured gateway ⇒ _is_callback_user_authorized parity DENY:
			// answered ⛔ (spinner clears) but NEVER resolved (#24457).
			expect(answer.kind).toBe("unauthorized");
			expect(answer.answerText).toContain("not authorized");
			expect(answer.hostEdit).toBeNull();
			expect(adapter.approvals.has(11)).toBe(true); // NOT popped
		});
	});

	it("tg-11: TELEGRAM_ALLOWED_USERS / '*' / ALLOW_ALL opt-ins authorize like message ingress", async () => {
		await withAuthzEnv({ TELEGRAM_ALLOWED_USERS: "42,999" }, async () => {
			const adapter = makeAdapterHere();
			adapter.approvals.register(12, "sk-tg11a");
			const ok = await adapter.router.route(
				buildExecApprovalCallback("once", 12),
				{ userId: "42", chatId: "77", chatType: "dm" },
			);
			expect(ok.kind).toBe("resolved");
		});
		await withAuthzEnv({ TELEGRAM_ALLOWED_USERS: "*" }, async () => {
			const adapter = makeAdapterHere();
			adapter.approvals.register(13, "sk-tg11w");
			const wildcard = await adapter.router.route(
				buildExecApprovalCallback("always", 13),
				{ userId: "anyone", chatType: "private" },
			);
			expect(wildcard.kind).toBe("resolved");
		});
		await withAuthzEnv({ GATEWAY_ALLOW_ALL_USERS: "yes" }, async () => {
			const adapter = makeAdapterHere();
			adapter.approvals.register(14, "sk-tg11g");
			const open = await adapter.router.route(
				buildExecApprovalCallback("deny", 14),
				{ userId: "555", chatType: "dm" },
			);
			expect(open.kind).toBe("resolved");
		});
	});

	it("tg-11: empty clicker id fails closed EVEN under '*'; supergroup+thread maps to forum scope", async () => {
		await withAuthzEnv(
			{
				TELEGRAM_ALLOWED_USERS: "*",
				TELEGRAM_GROUP_ALLOWED_CHATS: "-10099",
			},
			async () => {
				const adapter = makeAdapterHere();
				adapter.approvals.register(15, "sk-tg11e");
				const emptyUser = await adapter.router.route(
					buildExecApprovalCallback("session", 15),
					{ userId: "", chatType: "private" },
				);
				expect(emptyUser.kind).toBe("unauthorized");

				// A forum-topic tap in an allowed GROUP chat authorizes through
				// gate 2 (group_chat_allowlist) even though its sender is not in
				// the platform-wide allowlist — SessionSource chat_type mapping.
				adapter.approvals.register(16, "sk-tg11f");
				const forumTap = await adapter.router.route(
					buildExecApprovalCallback("once", 16),
					{
						userId: "not-listed",
						chatId: "-10099",
						chatType: "supergroup",
						threadId: "9",
					},
				);
				expect(forumTap.kind).toBe("resolved");
			},
		);
	});

	it("tg-11: forced fixture override wins over the real chain (shared conformance rows)", async () => {
		await withAuthzEnv({}, async () => {
			const adapter = makeAdapterHere();
			adapter.approvals.register(17, "sk-tg11f");
			adapter.setClickerAuthorization(false);
			const denied = await adapter.router.route(
				buildExecApprovalCallback("once", 17),
				{ userId: "42", chatType: "private" },
			);
			expect(denied.kind).toBe("unauthorized");
		});
	});

	it("tg-12: deleteMessage normalizes ids on the wire and NEVER throws on failure", async () => {
		const tg = new TelegramBotApiFake();
		const adapter = makeAdapterWith(tg);
		await expect(adapter.deleteMessage("4242", "88")).resolves.toBe(true);
		expect(tg.deleteOps[0]?.chatId).toBe("4242");
		expect(tg.deleteOps[0]?.messageId).toBe("88");

		tg.scriptDelete({
			kind: "fail",
			error: "Bad Request: message to delete not found",
		});
		await expect(adapter.deleteMessage("4242", "89")).resolves.toBe(false);
		await expect(adapter.deleteMessage("@channelname", "90")).resolves.toBe(
			true,
		);
		expect(tg.deleteOps.at(-1)?.chatId).toBe("@channelname");
	});

	it("tg-13: legacy draft frames are sendMessageDraft calls, MarkdownV2-first with one plain retry", async () => {
		const tg = new TelegramBotApiFake();
		const adapter = makeAdapterWith(tg);

		// First frame: MarkdownV2 conversion + parse_mode stamp; NO message id.
		const first = await adapter.sendDraft({
			chatId: "4242",
			draftId: 4001,
			content: "Part **one** <b>& raw",
		});
		expect(first.success).toBe(true);
		expect(first.messageId).toBeUndefined();
		expect(tg.draftKwargs[0]?.["chat_id"]).toBe(4242); // normalized int
		expect(tg.draftKwargs[0]?.["draft_id"]).toBe(4001);
		expect(tg.draftKwargs[0]?.["parse_mode"]).toBe("MarkdownV2");
		expect(String(tg.draftKwargs[0]?.["text"])).toContain("*one*");

		// BadRequest parse failure degrades THIS frame to raw text (no flag).
		tg.scriptDraft({
			kind: "fail",
			error: "Bad Request: can't parse entities",
		});
		const retried = await adapter.sendDraft({
			chatId: "4242",
			draftId: 4002,
			content: "plain tail **b**",
		});
		expect(retried.success).toBe(true);
		expect(tg.draftKwargs[1]?.["parse_mode"]).toBe("MarkdownV2"); // failed MD attempt
		expect(tg.draftKwargs[2]?.["parse_mode"]).toBeUndefined(); // plain retry
		expect(tg.draftKwargs[2]?.["text"]).toBe("plain tail **b**"); // RAW bytes

		// Non-BadRequest failures surface immediately — no doomed retry.
		tg.scriptDraft({ kind: "fail", error: "socket hang up" });
		const transient = await adapter.sendDraft({
			chatId: "4242",
			draftId: 4003,
			content: "x",
		});
		expect(transient.success).toBe(false);
		expect(transient.error).toContain("socket hang up");
		expect(tg.draftKwargs.filter((k) => k["draft_id"] === 4003)).toHaveLength(
			1,
		);
	});

	it("tg-13: oversized drafts ship FIRST-CHUNK bytes (utf16 budget, label kept) and stay RAW when rich preview governs", async () => {
		const oversize = "a".repeat(TELEGRAM_MAX_MESSAGE_UNITS + 10);
		const adapter = makeAdapterHere();
		await adapter.sendDraft({
			chatId: "chat-tg13-long",
			draftId: 4004,
			content: oversize,
		});
		const kwargs = adapter.bot.draftKwargs.at(-1) as Record<string, unknown>;
		// :6176 = truncate_message(len_fn=utf16_len)[0] — the first chunk of the
		// SHARED chunker: indicator-reserved body budget (4096−10), fence-aware
		// split, " (1/N)" label kept. Never a plain byte-cut, never split into
		// multiple frames. (The :6175 fit check itself is codepoint-based — an
		// astral-heavy draft under 4096 codepoints would pass through whole.)
		// The shipped bytes are the MARKDOWNV2 ATTEMPT: the conversion escapes
		// the synthesized label's parens exactly like regular chunked sends
		// ("… (1/2)" → "… \\(1/2\\)").
		expect(kwargs["text"]).toBe(
			`${"a".repeat(TELEGRAM_MAX_MESSAGE_UNITS - 10)} \\(1/2\\)`,
		);
		expect(String(kwargs["parse_mode"])).toBe("MarkdownV2"); // MDV2 attempt still stamped

		// A draft whose CODEPOINT count fits passes through UNCHANGED even when
		// its UTF-16 length exceeds the cap (upstream `len(content)` quirk).
		const astralOnly = "🎉".repeat(2048); // 2048 codepoints, 4096 UTF-16 units
		await adapter.sendDraft({
			chatId: "chat-tg13-long",
			draftId: 4005,
			content: astralOnly,
		});
		const passthrough = adapter.bot.draftKwargs.at(-1) as Record<
			string,
			unknown
		>;
		expect(passthrough["text"]).toBe(astralOnly);

		// rich_messages ON but rich_drafts OFF + rich-rendering-needed content:
		// the ephemeral preview stays RAW (plain_rich_preview :6185).
		const envOf = (k: string): string | undefined => {
			if (k === "TELEGRAM_RICH_MESSAGES") return "true";
			if (k === "TELEGRAM_RICH_DRAFTS") return "false";
			return undefined;
		};
		const richPreviewAdapter = new TelegramAdapter({
			wire: new TelegramBotApiFake(),
			secretReader: () => "tok",
			optionalEnvReader: envOf,
		});
		const table = "| a | b |\n| - | - |\n| 1 | 2 |";
		await richPreviewAdapter.sendDraft({
			chatId: "c",
			draftId: 1,
			content: table,
		});
		const last = richPreviewAdapter.bot.draftKwargs.at(-1) as Record<
			string,
			unknown
		>;
		expect(last["text"]).toBe(table); // untouched pipe-table bytes
		expect(last["parse_mode"]).toBeUndefined(); // no MDV2 attempt either
	});
});
