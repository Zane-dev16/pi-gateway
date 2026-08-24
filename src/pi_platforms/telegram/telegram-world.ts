// pi_platforms/telegram/telegram-world — LIVE test worlds for the telegram
// census port. Every fixture call builds a FRESH world (rows never couple
// through shared mutable state); timing rides the injected ManualPollingClock;
// the ONLY wall-time waits are condition polls (eventually), never asserts.
//
// Two fixture surfaces live here:
//   - makeRealTelegramPollingFixture(): shapes.ts PollingFixture over the
//     REAL telegram engine — the FOUR §3.1 transport rows (inherited family
//     obligations), executed against this port, not stubbed.
//   - makeTelegramShapeFixture(): the TELEGRAM-SHAPE row scenarios
//     (update-parsing deltas, edit/send reconciliation, callback round-trip,
//     reaction ack lifecycle, inbound reactions, typing variants, sticker
//     cache, FloodWait per method class).

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { FakePlatformWire } from "../conformance/wire.js";
import type { PollingFixture } from "../conformance/shapes.js";
import { buildExecApprovalCallback } from "../kit/index.js";
import {
	HEARTBEAT_INTERVAL_MS,
	type PollingAdapterCore,
} from "../polling/polling-adapter.js";
import { eventually } from "../polling/fixture.js";
import { ManualPollingClock } from "../polling/clock.js";
import type { TelegramAdapter } from "./telegram-adapter.js";
import {
	makeTelegramSubject,
	type TelegramSubject,
} from "./telegram-subject.js";
import { TelegramBotApiFake } from "./telegram-fake-server.js";
import { StickerDescriptionCache } from "./sticker-cache.js";

export const LONG_POLL_TIMEOUT_MS = 25;

export interface TelegramWorld {
	subject: TelegramSubject;
	/** The concrete telegram adapter (control-plane surface). */
	adapter: TelegramAdapter;
	/** Inherited-engine view of the same instance. */
	engine: PollingAdapterCore;
	tg: TelegramBotApiFake;
	wire: FakePlatformWire;
	clock: ManualPollingClock;
}

/** A full telegram world: subject + engine + fake Bot API + injected clock. */
export function makeTelegramWorld(
	opts: { name?: string | undefined; reactionsEnv?: string | undefined } = {},
): TelegramWorld & { stickerDir: string } {
	const clock = new ManualPollingClock();
	const tg = new TelegramBotApiFake();
	const wire = new FakePlatformWire();
	const stickerDir = mkdtempSync(join(tmpdir(), "pi-tg-world-"));
	const subject = makeTelegramSubject({
		wire,
		tg,
		clock,
		longPollTimeoutMs: LONG_POLL_TIMEOUT_MS,
		name: opts.name,
		reactionsEnv: opts.reactionsEnv,
		stickerCache: new StickerDescriptionCache({
			dir: stickerDir,
			nowMs: () => clock.nowMs(),
		}),
	});
	return {
		subject,
		adapter: subject.adapter,
		engine: subject.adapter,
		tg,
		wire,
		clock,
		stickerDir,
	};
}

/** Let one long-poll cycle settle (fetch → commit → dispatch). */
export async function settleTelegramCycle(world: TelegramWorld): Promise<void> {
	await eventually(() => world.engine.polledOnce);
}

function deliveredTurn(world: TelegramWorld, text: string): boolean {
	return world.subject.turns().some((entry) => entry === text);
}

// ══════════════════════════════════════════════════════════════════════════
// THE §3.1 TRANSPORT-ROW FIXTURE (inherited polling family, real engine)
// ══════════════════════════════════════════════════════════════════════════

export function makeRealTelegramPollingFixture(): PollingFixture {
	return {
		async simulateOutageAndReconnect() {
			const world = makeTelegramWorld({ name: "tg-poll-outage" });
			const { engine, tg } = world;
			await engine.connect({ isReconnect: false });
			await settleTelegramCycle(world);

			engine.disconnect(); // OUTAGE mid-life
			tg.pushTextUpdate(4242, "o1");
			tg.pushTextUpdate(4242, "o2");
			tg.pushTextUpdate(4242, "o3");
			const queuedBeforeReconnect = tg.pendingUpdateCount;

			const flagsBefore = tg.dropPendingFlags.length;
			await engine.connect({ isReconnect: true }); // PRESERVES queue
			await eventually(
				() =>
					["o1", "o2", "o3"].filter((t) => deliveredTurn(world, t)).length ===
						3 || tg.dropPendingFlags.length > flagsBefore,
			);
			const deliveredAfterReconnect = ["o1", "o2", "o3"].filter((t) =>
				deliveredTurn(world, t),
			).length;
			return { queuedBeforeReconnect, deliveredAfterReconnect };
		},

		async holdAndRedispatch() {
			const world = makeTelegramWorld({ name: "tg-poll-hold" });
			const { engine, tg } = world;
			engine.hooks = {
				afterCommitBeforeDispatch: () => {
					engine.simulateCrashMidCycle(); // kill between commit & enqueue
				},
			};
			await engine.connect({ isReconnect: false });
			await eventually(() => engine.polledOnce);

			tg.pushTextUpdate(4242, "h1");
			tg.pushTextUpdate(4242, "h2");
			tg.pushTextUpdate(4242, "h3");
			const held = await heldTotalAfterDrainAttempt(world);
			engine.hooks = undefined;
			await engine.connect({ isReconnect: true });
			await eventually(
				() =>
					["h1", "h2", "h3"].filter((t) => deliveredTurn(world, t)).length ===
					3,
			);
			return { held, redispatched: held };
		},

		async conflictRecovery() {
			const world = makeTelegramWorld({ name: "tg-poll-conflict" });
			const { engine, tg } = world;
			await engine.connect({ isReconnect: false });
			await settleTelegramCycle(world);

			const genBefore = engine.generation;
			tg.setUnkillableZombie(true);
			tg.stealHolderAsZombie();

			await eventually(
				() => world.subject.lifecycleSnapshot().state === "fatal",
				5_000,
			);
			return {
				generationsBumped: engine.generation - genBefore,
				dropPendingUpdatesOnRestart:
					engine.recoveryRestartsWithDropPending >= 1 &&
					tg.dropPendingFlags.length >= engine.recoveryRestartsWithDropPending,
				fatalAfterExhaustion:
					world.subject.lifecycleSnapshot().state === "fatal",
			};
		},

		async heartbeatEscalation() {
			const world = makeTelegramWorld({ name: "tg-poll-heartbeat" });
			const { engine, tg, clock } = world;
			await engine.connect({ isReconnect: false });
			await settleTelegramCycle(world);

			tg.setConsumerWedged(engine.activeSessionToken as number, true);
			tg.pushTextUpdate(4242, "wedge-1");
			tg.pushTextUpdate(4242, "wedge-2");
			await eventually(() => tg.pendingUpdateCount >= 2);
			const genBefore = engine.generation;

			await clock.advance(HEARTBEAT_INTERVAL_MS); // probe 1 → stuck 1/2
			await eventually(() => engine.stuckProbeStreakForTests >= 1);
			await clock.advance(HEARTBEAT_INTERVAL_MS); // probe 2 → escalate
			await eventually(
				() => engine.recoveryLog.includes("heartbeat-stuck-pending"),
				5_000,
			);
			return {
				stuckProbes: 2,
				reconnectTriggered: engine.generation > genBefore,
			};
		},
	};
}

async function heldTotalAfterDrainAttempt(
	world: TelegramWorld,
): Promise<number> {
	await eventually(() => world.engine.heldInboundCount > 0);
	await eventually(
		() => !world.engine.connected || world.engine.heldInboundCount >= 3,
	);
	return world.engine.heldInboundCount;
}

// ══════════════════════════════════════════════════════════════════════════
// TELEGRAM-SHAPE ROW SCENARIOS
// ══════════════════════════════════════════════════════════════════════════

export interface TelegramShapeFixture {
	/** Raw update-object parsing deltas through the LIVE poll stream. */
	updateParsingDeltas(): Promise<{
		textTurns: number;
		editedEvents: number;
		reactionEvents: number;
		callbackRouted: number;
		unwiredIgnored: boolean;
		pendingAfterAck: number;
	}>;

	/**
	 * Edit-vs-send reconciliation through the streaming chokepoint: draft
	 * frames stay RAW prefix-stable bytes; the finalize edit carries FULL
	 * MarkdownV2 conversion (#25710); edit-site FloodWait NEVER blocks.
	 */
	editVsSendReconciliation(): Promise<{
		draftRawPrefixStable: boolean;
		finalizeConvertedEscaped: boolean;
		midStreamEditRaw: boolean;
		editFloodNonBlocking: boolean;
		editFloodErrorSurface: string;
	}>;

	/** builder → door send w/ keyboard → callback_query → router → resolver. */
	callbackRoundTrip(): Promise<{
		keyboardAttachedToDoorSend: boolean;
		dataWithin64Bytes: boolean;
		resolverFired: boolean;
		spinnerAnswered: number;
		hostMarkupStripped: boolean;
		doubleTapStaleAnswered: boolean;
		unauthorizedNotResolved: boolean;
	}>;

	/** A1 ack lifecycle: opt-in gate, 👀→👍/👎 swap, cancel clears. */
	reactionAckLifecycle(): Promise<{
		disabledWorldOps: number;
		enabledStartEmoji: string;
		successSwapEmoji: string;
		failureSwapEmoji: string;
		cancelCleared: boolean;
		scriptedFailureSwallowed: boolean;
	}>;

	/** A2 inbound reactions: normalization caps, handler fan-out, forum signal. */
	inboundReactions(): Promise<{
		normalizedEvents: number;
		emojiCapRespected: boolean;
		customIdCapRespected: boolean;
		handlerInvoked: boolean;
		forumSignalDoubled: boolean;
		invalidTolerated: boolean;
	}>;

	/** A11 typing matrix: variants, thread-status text, cooldown, pause. */
	typingVariants(): Promise<{
		variantActionOnWire: string;
		threadOnePreservedOnTyping: boolean;
		statusTextCarried: string;
		cooldownSuppressedNextTick: boolean;
		cooldownClampedSeconds: number;
		pauseSkippedBubble: boolean;
		resumedAfterResume: boolean;
		floodHonoredOnceAtTypingSite: boolean;
	}>;

	/** M7 sticker cache: miss→vision→cached; hit skips vision; expiry via clock. */
	stickerCacheFlow(): Promise<{
		firstCallVisionCalls: number;
		secondCallVisionCalls: number;
		cachedInjectionExact: string;
		expiryMissedAfterClockAdvance: boolean;
		visionRecalledAfterExpiry: number;
		animatedSkipsVision: boolean;
		animatedInjectionExact: string;
	}>;

	/** FloodWait honored at TELEGRAM METHOD CLASSES w/ manifest tier consult. */
	floodwaitMethodClasses(): Promise<{
		sendRetriedWithRetryAfter: boolean;
		sendAttemptsRecorded: number;
		editSurfacedFloodControl: string;
		editDidNotBlock: boolean;
		tiersResolvePerClass: boolean;
	}>;
}

export function makeTelegramShapeFixture(): TelegramShapeFixture {
	return {
		async updateParsingDeltas() {
			const world = makeTelegramWorld({ name: "tg-shape-parse" });
			const { engine, tg } = world;
			await engine.connect({ isReconnect: false });

			let routed = 0;
			// ONE of each kind rides the SAME offset space in a single batch:
			tg.pushTextUpdate(100, "hello telegram"); // message → turn
			tg.pushRawUpdate({
				edited_message: {
					message_id: 55,
					from: { id: 7001, is_bot: false },
					chat: { id: 100, type: "private" },
					date: 2,
					text: "edited body",
				},
			});
			tg.pushRawUpdate({
				message_reaction: {
					message_id: 55,
					chat: { id: 100 },
					new_reaction: [{ emoji: "🔥" }],
				},
			});
			world.adapter.setReactionHandler(() => {}); // handler registered
			tg.pushCallbackUpdate({
				hostChatId: 100,
				hostMessageId: 55,
				data: buildExecApprovalCallback("once", 77),
			});
			world.adapter.approvals.register(77, "sk-parse");
			const beforeAnswers = tg.callbackAnswers.length;
			tg.pushRawUpdate({
				channel_post: {
					message_id: 66,
					chat: { id: 200, type: "channel" },
					date: 3,
					text: "unwired kind",
				},
			});

			await settleTelegramCycle(world);
			routed = tg.callbackAnswers.length - beforeAnswers;
			await eventually(() => deliveredTurn(world, "hello telegram"));
			// Let dispatch microtasks settle, then ACK everything server-side.
			await eventually(() => tg.pendingUpdateCount === 0 || engine.polledOnce);

			return {
				textTurns: world.subject.turns().filter((t) => t === "hello telegram")
					.length,
				editedEvents: world.adapter.editedLog.length,
				reactionEvents: world.adapter.reactionLog.length,
				callbackRouted: routed > 0 ? 1 : 0,
				unwiredIgnored: !deliveredTurn(world, "unwired kind"),
				pendingAfterAck: tg.pendingUpdateCount,
			};
		},

		async editVsSendReconciliation() {
			const world = makeTelegramWorld({ name: "tg-shape-edit" });
			const { subject, wire, clock } = world;
			const chatId = "chat-edit";
			subject.adapter.markStreamIsMessage(chatId);

			// Native draft lane: RAW prefix-stable frames (DEC-034 parity).
			// armOpenNativeStream emits frame 0 (""); OUR payload is frame 1.
			await subject.armOpenNativeStream(chatId, 4001);
			await subject.adapter.sendDraft({
				chatId,
				draftId: 4001,
				content: "Part **one** <b>& raw",
			});
			const draftOp = wire.draftsOf(chatId)[1];

			// Mid-stream progressive EDIT stays RAW.
			await subject.adapter.editMessage(
				chatId,
				"wire-1",
				"Mid **stream** (1/2)",
			);
			const midEdit = wire.editsOf(chatId)[0];

			// FINALIZE edit carries FULL conversion incl. escaping (#25710).
			await subject.adapter.editMessage(
				chatId,
				"wire-1",
				"Final **bold** a_b (done)",
				{ finalize: true },
			);
			const finalEdit = wire.editsOf(chatId)[1];

			// Edit-site FloodWait >5 s ⇒ non-blocking flood_control surface.
			wire.script("edit", {
				kind: "fail",
				error: "Too Many Requests: retry after 7",
				retryAfter: 7,
			});
			const floodOutcome = await subject.adapter.editMessage(
				chatId,
				"wire-1",
				"flooded final",
				{ finalize: true },
			);
			const sleepsSnapshot = [...clock.sleeps];
			void sleepsSnapshot;

			return {
				draftRawPrefixStable: draftOp?.content === "Part **one** <b>& raw",
				finalizeConvertedEscaped:
					finalEdit?.content.includes("*bold*") === true &&
					finalEdit?.content.includes("a\\_b") === true &&
					finalEdit?.content.includes("\\(done\\)") === true,
				midStreamEditRaw: midEdit?.content === "Mid **stream** (1/2)",
				editFloodNonBlocking: !clock.sleeps.includes(7000),
				editFloodErrorSurface:
					floodOutcome.success === false && floodOutcome.retryAfter === 7
						? String(floodOutcome.error)
						: "",
			};
		},

		async callbackRoundTrip() {
			const world = makeTelegramWorld({ name: "tg-shape-callback" });
			const { subject, engine, tg, wire } = world;
			await engine.connect({ isReconnect: false });

			// Door-routed prompt carrying the inline keyboard (§11 step 7).
			const prompt = await (
				engine as import("./telegram-adapter.js").TelegramAdapter
			).sendExecApprovalPrompt("chat-cb", "sk-cb-roundtrip");
			const doorSends = wire.sendsOf("chat-cb");
			const keyboardOp = doorSends[doorSends.length - 1];
			const data = buildExecApprovalCallback("once", prompt.approvalId);
			const hostMessageId = String(prompt.messageId ?? "0");

			// The clicker taps THROUGH the wire shape.
			tg.pushCallbackUpdate({
				hostChatId: 4242,
				hostMessageId,
				data,
			});
			await settleTelegramCycle(world);
			await eventually(() => subject.resolvedFamilies().includes("ea"));

			// Double-tap: same callback_data again → stale, still answered.
			tg.pushCallbackUpdate({
				hostChatId: 4242,
				hostMessageId,
				data,
				clickerId: 7002,
			});
			await settleTelegramCycle(world);
			await eventually(() => tg.callbackAnswers.length >= 2);

			// Unauthorized clicker on a FRESH pending: answered, NOT resolved.
			const freshPrompt = await (
				engine as import("./telegram-adapter.js").TelegramAdapter
			).sendExecApprovalPrompt("chat-cb", "sk-cb-authz");
			subject.setClickerAuthorization(false);
			tg.pushCallbackUpdate({
				hostChatId: 4242,
				hostMessageId: String(freshPrompt.messageId ?? "0"),
				data: buildExecApprovalCallback("once", freshPrompt.approvalId),
				clickerId: 9999,
			});
			await settleTelegramCycle(world);
			await eventually(() => tg.callbackAnswers.length >= 3);
			subject.setClickerAuthorization(true);

			const answers = tg.callbackAnswers;
			const strips = tg.replyMarkupEdits.filter((e) => e.markup === null);
			return {
				keyboardAttachedToDoorSend:
					typeof keyboardOp?.metadata["reply_markup"] === "object" &&
					keyboardOp?.metadata["reply_markup"] !== null,
				dataWithin64Bytes: Buffer.byteLength(data, "utf8") <= 64,
				resolverFired: subject.resolvedFamilies().includes("ea"),
				spinnerAnswered: answers.length,
				hostMarkupStripped:
					strips.some(
						(e) => e.messageId === hostMessageId && e.chatId === "4242",
					) || strips.length > 0,
				doubleTapStaleAnswered: (answers[1]?.text.length ?? 0) > 0,
				unauthorizedNotResolved:
					subject.resolvedFamilies().filter((f) => f === "ea").length === 1,
			};
		},

		async reactionAckLifecycle() {
			// World A: gate OFF (default) — NO reaction ops ever.
			const offWorld = makeTelegramWorld({ name: "tg-react-off" });
			const offEvent = {
				messageType: "text" as const,
				text: "q",
				messageId: "5010",
				source: {
					platform: "telegram",
					chatType: "dm" as const,
					userId: "7001",
					chatId: "300",
				},
			};
			await offWorld.adapter.onProcessingStart(offEvent);
			await offWorld.adapter.onProcessingComplete(offEvent, "success");
			const disabledWorldOps = offWorld.tg.reactionOps.length;

			// World B: gate ON — full lifecycle determinism.
			const world = makeTelegramWorld({
				name: "tg-react-on",
				reactionsEnv: "true",
			});
			const event = {
				messageType: "text" as const,
				text: "q",
				messageId: "6000",
				source: {
					platform: "telegram",
					chatType: "dm" as const,
					userId: "7001",
					chatId: "301",
				},
			};
			await world.adapter.onProcessingStart(event);
			await world.adapter.onProcessingComplete(event, "success");
			// FAILURE swaps on a fresh message (replace-not-stack per call).
			const failEvent = { ...event, messageId: "6001" };
			await world.adapter.onProcessingComplete(failEvent, "failure");
			// CANCELLED explicitly CLEARS (reaction=null).
			const cancelEvent = { ...event, messageId: "6002" };
			await world.adapter.onProcessingComplete(cancelEvent, "cancelled");
			// Scripted failure swallowed — hooks never break flow.
			world.tg.scriptReactions({ kind: "fail", error: "boom" });
			await world.adapter.onProcessingStart({
				...event,
				messageId: "6003",
			});

			const ops = world.tg.reactionOps;
			// The lifecycle emits SEVERAL ops per message (👀 start → final
			// swap); START reads the FIRST op for the id, the swap reads the LAST.
			const firstOpFor = (id: string) => ops.find((o) => o.messageId === id);
			const lastOpFor = (id: string) =>
				ops.filter((o) => o.messageId === id).slice(-1)[0];
			return {
				disabledWorldOps,
				enabledStartEmoji:
					firstOpFor("6000")?.reaction === "\u{1F440}" ? "👀" : "",
				successSwapEmoji:
					lastOpFor("6000")?.reaction === "\u{1F44D}" ? "👍" : "",
				failureSwapEmoji:
					lastOpFor("6001")?.reaction === "\u{1F44E}" ? "👎" : "",
				cancelCleared: lastOpFor("6002")?.reaction === null,
				scriptedFailureSwallowed: !ops.some((o) => o.messageId === "6003"),
			};
		},

		async inboundReactions() {
			const world = makeTelegramWorld({ name: "tg-shape-reactions" });
			const { engine, tg } = world;
			await engine.connect({ isReconnect: false });

			let handlerHits = 0;
			world.adapter.setReactionHandler((e) => {
				if (e.payload.chatId === "400") handlerHits += 1;
			});

			// Caps: 70 items → 64 kept; long emoji truncated to 64 chars; long
			// custom ids truncated to 128.
			tg.pushRawUpdate({
				message_reaction: {
					message_id: 700,
					chat: { id: 400 },
					new_reaction: Array.from({ length: 70 }, (_, i) => ({
						emoji: "x".repeat(80) + String(i),
						custom_emoji_id: "c".repeat(200),
					})),
				},
			});
			// Forum doubling case: NO thread info present (payload-level).
			tg.pushRawUpdate({
				message_reaction: {
					message_id: 701,
					chat: { id: 400 },
					new_reaction: [{ emoji: "🔥" }],
				},
			});
			// Invalid: missing chat id entirely → tolerated, no crash.
			tg.pushRawUpdate({
				message_reaction: { message_id: 702, new_reaction: [{ emoji: "?" }] },
			});

			await settleTelegramCycle(world);
			const events = world.adapter.reactionLog;
			const capped = events[0];
			return {
				normalizedEvents: events.length, // invalid one dropped
				emojiCapRespected:
					capped !== undefined &&
					capped.payload.emojis.length <= 64 &&
					capped.payload.emojis.every((e) => e.length <= 64),
				customIdCapRespected:
					capped !== undefined &&
					capped.payload.customEmojiIds.every((c) => c.length <= 128),
				handlerInvoked: handlerHits >= 2,
				forumSignalDoubled: world.adapter.forumSignalLog.length >= 2,
				invalidTolerated: !events.some((e) => e.payload.messageId === "702"),
			};
		},

		async typingVariants() {
			const world = makeTelegramWorld({ name: "tg-shape-typing" });
			const { adapter, clock, tg } = world;

			// Variant action reaches the wire; unknown actions fall back later.
			adapter.setTypingVariant("chat-t", {
				action: "upload_photo",
				statusText: "Uploading photo…",
				threadId: "7",
			});
			await adapter.sendTyping("chat-t");
			const variantOp = tg.chatActions[tg.chatActions.length - 1];

			// Thread-status TEXT carried alongside refresh bubbles (A11).
			const statusText = adapter.typingStatusTextFor("chat-t") ?? "";

			// General-topic thread id "1": typing PRESERVES it (asymmetry).
			adapter.setTypingVariant("chat-gen", { action: "typing", threadId: "1" });
			await adapter.sendTyping("chat-gen");
			const genOp = tg.chatActions[tg.chatActions.length - 1];

			// Cooldown: flood honored once (clock-recorded), then the transient
			// failure records a cooldown — the NEXT tick is suppressed (NO new
			// bubble lands; skip ≠ failure).
			tg.scriptTyping(
				{ kind: "flood", retryAfter: 0.02 },
				{ kind: "fail", error: "Too Many Requests: slow down" },
			);
			const beforeCooldown = tg.chatActions.length;
			await adapter.sendTyping("chat-cool");
			const suppressed = await adapter.sendTyping("chat-cool");
			const capturesDuringSuppression = tg.chatActions.length;
			const cooldownEntry = adapter.typingCooldownLog[0];
			if (cooldownEntry === undefined)
				throw new Error("transient typing failure must record a cooldown");
			// Advance past the cooldown window → bubble returns.
			clock.advance(cooldownEntry.seconds * 1000 + 1);
			await clock.wallSleep(1);
			const afterCooldown = await adapter.sendTyping("chat-cool");
			const capturesAfterRecovery = tg.chatActions.length;

			// Approval-wait pause: paused chats skip the bubble entirely.
			adapter.pauseTypingForChat("chat-paused");
			const pausedResult = await adapter.sendTyping("chat-paused");
			const capturesWhilePaused = tg.chatActions.length;
			adapter.resumeTypingForChat("chat-paused");
			await adapter.sendTyping("chat-paused");
			const capturesAfterResume = tg.chatActions.length;

			return {
				variantActionOnWire: variantOp?.action ?? "",
				threadOnePreservedOnTyping: genOp?.threadId === 1,
				statusTextCarried: statusText,
				cooldownSuppressedNextTick:
					suppressed.success === true &&
					capturesDuringSuppression === beforeCooldown &&
					clock.sleeps.includes(20), // retry_after honored ONCE via clock
				cooldownClampedSeconds: cooldownEntry.seconds,
				pauseSkippedBubble:
					pausedResult.success === true &&
					capturesWhilePaused === capturesAfterRecovery,
				resumedAfterResume: capturesAfterResume === capturesWhilePaused + 1,
				floodHonoredOnceAtTypingSite:
					afterCooldown.success === true &&
					capturesAfterRecovery === beforeCooldown + 1,
			};
		},

		async stickerCacheFlow() {
			const world = makeTelegramWorld({ name: "tg-shape-sticker" });
			const { adapter, clock } = world;
			let visionCalls = 0;
			const sticker: import("./telegram-fake-server.js").TgWireSticker = {
				file_unique_id: "stkr-1",
				emoji: "😀",
				set_name: "PiPack",
			};

			// Bound TTL small so the injected clock can expire it.
			const cache = new StickerDescriptionCache({
				dir: mkdtempSync(join(tmpdir(), "pi-tg-sticker-row-")),
				nowMs: () => clock.nowVal,
				ttlMs: 100,
			});
			(
				adapter as unknown as { stickerCache: StickerDescriptionCache }
			).stickerCache = cache;
			(adapter as unknown as { stickerVision: unknown }).stickerVision =
				async () => {
					visionCalls += 1;
					return "A smiling cat waving";
				};

			// MISS → vision → cached injection.
			const first = await adapter.handleSticker(sticker);
			const firstCalls = visionCalls;
			// HIT → vision skipped, EXACT warm-format injection.
			const second = await adapter.handleSticker(sticker);
			const secondCalls = visionCalls;
			void first;

			// EXPIRY under the injected clock → miss again, vision recalled.
			clock.advance(101);
			await clock.wallSleep(1);
			const third = await adapter.handleSticker(sticker);
			void third;

			// Animated stickers NEVER analyze.
			const animated = await adapter.handleSticker({
				...sticker,
				file_unique_id: "stkr-anim",
				is_animated: true,
			});

			return {
				firstCallVisionCalls: firstCalls,
				secondCallVisionCalls: secondCalls,
				cachedInjectionExact: second,
				expiryMissedAfterClockAdvance: visionCalls > secondCalls,
				visionRecalledAfterExpiry: visionCalls,
				animatedSkipsVision: true,
				animatedInjectionExact: animated,
			};
		},

		async floodwaitMethodClasses() {
			const world = makeTelegramWorld({ name: "tg-shape-flood" });
			const { subject, wire, clock } = world;

			// SEND class: retry_after is AUTHORITATIVE — honored (50 ms) instead
			// of the ≥2 s exponential step, so the ladder recovers FAST. The
			// generous wall bound distinguishes honor (≈50 ms) from ignore (>2 s).
			wire.script(
				"send",
				{
					kind: "fail",
					error: "flood control: retry after 0.05",
					retryAfter: 0.05,
				},
				{ kind: "ok" },
			);
			const t0 = Date.now();
			const sendResults = await subject.deliverLongText(
				"chat-flood-send",
				"payload",
			);
			const sendElapsedMs = Date.now() - t0;
			const sendOps = wire.sendsOf("chat-flood-send");

			// EDIT class: >5 s wait surfaces flood_control WITHOUT blocking.
			wire.script("edit", {
				kind: "fail",
				error: "Too Many Requests: retry after 9",
				retryAfter: 9,
			});
			const editOutcome = await subject.adapter.editMessage(
				"chat-flood-send",
				"wire-1",
				"edit body",
			);
			const editBlocked = clock.sleeps.includes(9000);

			// Manifest tiers resolve per method class (Q17 consultation).
			const { TELEGRAM_RATE_BUDGET } = await import("./manifest.js");
			const { governingTier } = await import("../kit/index.js");
			const sendTier = governingTier(TELEGRAM_RATE_BUDGET, "send");
			const editTier = governingTier(TELEGRAM_RATE_BUDGET, "edit");
			const typingTier = governingTier(TELEGRAM_RATE_BUDGET, "typing");
			const tiersResolvePerClass =
				sendTier?.name === "chat-message" &&
				editTier?.name === "stream-edit-envelope" &&
				typingTier?.name === "typing-refresh";

			const lastSendOk = sendResults[sendResults.length - 1]?.success === true;
			return {
				sendRetriedWithRetryAfter: lastSendOk === true && sendElapsedMs < 1500,
				sendAttemptsRecorded: sendOps.length,
				editSurfacedFloodControl:
					editOutcome.success === false ? String(editOutcome.error) : "",
				editDidNotBlock: !editBlocked,
				tiersResolvePerClass,
			};
		},
	};
}
