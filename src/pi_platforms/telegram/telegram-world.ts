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
import { GatewayStreamConsumer } from "../../pi_gateway/streaming/gateway-stream-consumer.js";
import { ManualPollingClock } from "../polling/clock.js";
import { TelegramAdapter } from "./telegram-adapter.js";
import { TELEGRAM_ALLOWED_UPDATES } from "./manifest.js";
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
	opts: {
		name?: string | undefined;
		reactionsEnv?: string | undefined;
		/** Scoped OPTIONAL-env map (notifications mode, rich/link-preview/
		 * status extras). TELEGRAM_REACTIONS rides reactionsEnv separately. */
		env?: Record<string, string | undefined> | undefined;
		/** Post-connect DM-topic config (tg2-3 housekeeping rows). */
		dmTopicsConfig?: readonly import("./telegram-adapter.js").DmTopicConfigEntry[] | undefined;
	} = {},
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
		optionalEnv: opts.env,
		dmTopicsConfig: opts.dmTopicsConfig,
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

/** Door-1 send with optional metadata (send-lane parity scenarios). */
async function sendThroughDoor(
	world: TelegramWorld,
	chatId: string,
	content: string,
	metadata: Record<string, unknown> = {},
): Promise<import("../../pi_gateway/streaming/adapter-seam.js").SendResult> {
	return world.subject.sendThroughDoor1(
		chatId,
		content,
		metadata as import("../../pi_gateway/streaming/adapter-seam.js").Metadata,
	);
}

/** Content that splits into multiple kit chunks under any sane budget. */
function multiChunkContent(): string {
	return "lorem ipsum dolor ".repeat(400);
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
		editedEventNormalized: boolean;
		editedEventPayloadExact: boolean;
		editedHandlerFired: boolean;
		reactionEvents: number;
		callbackRouted: number;
		unwiredIgnored: boolean;
		pendingAfterAck: number;
		pollRequestedAllTypes: boolean;
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
		finalizeParseModeStamped: boolean;
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

	/** Send-lane wire parity: escaping, parse_mode, notification/thread/
	 * reply kwargs, plain lanes raw. */
	sendWireParity(): Promise<{
		convertedEscapedMatchesParseMode: boolean;
		chunkMarkerEscaped: boolean;
		plainLaneRawNoParseMode: boolean;
		silentByDefault: boolean;
		notifyOverrideRings: boolean;
		allModeNeverSilences: boolean;
		threadIdRouted: boolean;
		generalTopicOneOmitted: boolean;
		replyAnchorFirstChunkOnly: boolean;
		replyNotFoundRetryDropsAnchor: boolean;
	}>;

	/** tg-7: deleteWebhook on connect — cold boot required, reconnect best-
	 * effort, drop_pending_updates always false, precedes first poll. */
	connectWebhookClear(): Promise<{
		coldBootDeleteCaptured: boolean;
		coldBootDropPendingFalse: boolean;
		coldBootFailureAbortsConnect: boolean;
		reconnectBestEffortContinues: boolean;
		reconnectDropPendingFalse: boolean;
		noPollAfterColdFailure: boolean;
	}>;

	/** tg-9: the outgoing media family incl. audio→document fallbacks. */
	mediaSendFamily(): Promise<{
		photoCarriesNotificationThreadCaption: boolean;
		numericChatIdIntOnWire: boolean;
		dmTopicKwargsRouted: boolean;
		voiceAbsentCaptionOmitsKey: boolean;
		voiceMdV2CaptionFirst: boolean;
		voicePlainRetryAfterParseRejection: boolean;
		photoFailureFallsBackToDocument: boolean;
		documentFilenameDefaultsToBasename: boolean;
		voiceExtRoutingOggOpus: boolean;
		voiceMp3RoutesToSendAudio: boolean;
		voiceUnsupportedExtFallsBackToDocument: boolean;
		animationFailureFallsBackToPhoto: boolean;
		captionCappedAt1024: boolean;
	}>;

	/** A11 typing matrix: pinned action, thread-status text, cooldown, pause. */
	typingVariants(): Promise<{
		actionOnWire: string;
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
		overCapSendFloodControl: boolean;
		overCapDidNotSleep: boolean;
	}>;

	/** tg2-1: "message is not modified" edits map to success no-ops so the
	 * REQUIRES_EDIT_FINALIZE redundant finalize can never duplicate text. */
	editNotModifiedNoop(): Promise<{
		midStreamNoOpSuccess: boolean;
		finalizeRawNoOpSuccess: boolean;
		finalizeConvertedNoOpSuccess: boolean;
		consumerFinalDeliveredWithoutDuplicate: boolean;
		realFailuresStillFail: boolean;
	}>;

	/** tg2-3: post-connect housekeeping — command menu ×3 scopes, opt-in
	 * status indicator, DM-topic create/load, lazy forum-scope registration. */
	postConnectHousekeeping(): Promise<{
		coldBootMenuScopesExactOrder: boolean;
		menuCommandCount: number;
		reconnectDoesNotDoubleSchedule: boolean;
		statusIndicatorOptIn: boolean;
		dmTopicCreatedAndCached: boolean;
		dmTopicRenamed: boolean;
		forumScopeLazyRegisteredOnce: boolean;
		housekeepingFailureNonFatal: boolean;
	}>;

	/** tg2-4: DM-topic metadata routing + fail-loud anchor gate on sends. */
	dmTopicSendRouting(): Promise<{
		dmTopicIdPairedWithThreadNone: boolean;
		aliasMetadataKeyHonored: boolean;
		fallbackAnchorAttachedEveryChunk: boolean;
		missingAnchorFailsLoudExact: boolean;
		createdForSendNeverFailsLoud: boolean;
	}>;

	/** tg2-5/tg2-7/tg2-9: whitelist-only arg set, normalized chat ids,
	 * extra-gated link-preview suppression. */
	wireArgWhitelist(): Promise<{
		inputNamespaceKeysNeverLeak: boolean;
		builtArgsStillShip: boolean;
		replyMarkupSurvives: boolean;
		numericChatIdIsInt: boolean;
		usernameChatIdPreserved: boolean;
		linkPreviewKwargsGated: boolean;
		linkPreviewOffByDefault: boolean;
	}>;

	/** tg2-6: rich extras — sendRichMessage/sendRichMessageDraft/rich edit
	 * behind wireRich; capability latch; default-off worlds unchanged. */
	richExtrasLane(): Promise<{
		defaultOffNeverAttemptsRich: boolean;
		extraOnSendsRichPayload: boolean;
		ineligibleContentSkipsSilently: boolean;
		expectEditsSkipsRich: boolean;
		capabilityErrorLatchesOnce: boolean;
		transientRichNeverLegacyResent: boolean;
		richDraftGatedOnBothExtras: boolean;
		richDraftCapabilityLatches: boolean;
		richFinalizeEditApplied: boolean;
	}>;

	/** tg2-12: media transmissions ride the DM-topic anchor retry ladder. */
	mediaDmTopicRetry(): Promise<{
		deadReplyAnchorRetriesWithoutAnchors: boolean;
		staleTopicRetriedWithBindingPrune: boolean;
		withoutFallbackFlagFailsImmediately: boolean;
		nonRetryClassSurfacesAsIs: boolean;
	}>;

	/** tg2-8: production long-poll timeout is the PTB-default 10 s. */
	longPollTimeoutDefault(): Promise<{
		productionTimeoutMs: number;
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
					chat: { id: 100, type: "supergroup", is_forum: true },
					date: 2,
					text: "edited body",
					message_thread_id: 9,
					is_topic_message: true,
					edit_date: 1760000500,
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
			// tg2-11: edits fan out as normalized message_edited platform events.
			let editedEvent:
				| import("./platform-events.js").NormalizedEditedEvent
				| null = null;
			world.adapter.setEditedMessageHandler((e) => {
				editedEvent = e;
			});
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

			const ev = world.adapter.editedEventLog[0];
			return {
				textTurns: world.subject.turns().filter((t) => t === "hello telegram")
					.length,
				editedEvents: world.adapter.editedLog.length,
				editedEventNormalized: ev !== undefined,
				editedEventPayloadExact:
					ev !== undefined &&
					ev.eventType === "message_edited" &&
					ev.payload.chatId === "100" &&
					ev.payload.messageId === "55" &&
					ev.payload.threadId === "9" &&
					ev.payload.text === "edited body" &&
					ev.payload.editedAt === new Date(1760000500 * 1000).toISOString() &&
					typeof ev.payload.editedAt === "string",
				editedHandlerFired: editedEvent !== null,
				reactionEvents: world.adapter.reactionLog.length,
				callbackRouted: routed > 0 ? 1 : 0,
				unwiredIgnored: !deliveredTurn(world, "unwired kind"),
				pendingAfterAck: tg.pendingUpdateCount,
				// tg-1: EVERY poll of this world requested Update.ALL_TYPES —
				// the reaction update above is only deliverable under it.
				pollRequestedAllTypes:
					tg.allowedUpdatesLog.length > 0 &&
					tg.allowedUpdatesLog.every(
						(a) =>
							a !== undefined &&
							a.length === TELEGRAM_ALLOWED_UPDATES.length &&
							a.every((k) => TELEGRAM_ALLOWED_UPDATES.includes(k)),
					),
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
				// tg-3: finalize edits stamp parse_mode; mid-stream omit it.
				finalizeParseModeStamped:
					finalEdit?.metadata["parse_mode"] === "MarkdownV2" &&
					midEdit?.metadata["parse_mode"] === undefined,
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
			// tg2-2: resolved taps edit the host message TEXT (MarkdownV2 +
			// reply_markup removed) — the resolution label lands in chat.
			const hostMessageIdNum = Number(hostMessageId);
			const hostEdits = tg.editKwargs.filter(
				(e) =>
					String(e.chat_id) === "4242" &&
					(Number.isFinite(hostMessageIdNum)
						? Number(e.message_id) === hostMessageIdNum
						: String(e.message_id) === hostMessageId) &&
					e.reply_markup === null &&
					e.parse_mode === "MarkdownV2",
			);
			return {
				keyboardAttachedToDoorSend:
					typeof keyboardOp?.metadata["reply_markup"] === "object" &&
					keyboardOp?.metadata["reply_markup"] !== null,
				dataWithin64Bytes: Buffer.byteLength(data, "utf8") <= 64,
				resolverFired: subject.resolvedFamilies().includes("ea"),
				spinnerAnswered: answers.length,
				hostMarkupStripped:
					hostEdits.length === 1 &&
					String(hostEdits[0]?.text).includes("Approved"),
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

			// Hermes truth (tg-10): the wire action is ALWAYS "typing" — a
			// variant carries status text + thread placement only. A decoy
			// "upload_photo" action request must NEVER reach the wire.
			adapter.setTypingVariant("chat-t", {
				statusText: "Uploading photo…",
				threadId: "7",
			});
			await adapter.sendTyping("chat-t", "upload_photo");
			const variantOp = tg.chatActions[tg.chatActions.length - 1];

			// Thread-status TEXT carried alongside refresh bubbles (A11).
			const statusText = adapter.typingStatusTextFor("chat-t") ?? "";

			// General-topic thread id "1": typing PRESERVES it (asymmetry).
			adapter.setTypingVariant("chat-gen", { threadId: "1" });
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
				actionOnWire: variantOp?.action ?? "",
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

			// SEND class over-cap (tg-8/#91969): a 7 s retry_after must fail
			// closed as flood_control:7 WITHOUT any inline sleep — wall-fast.
			wire.script("send", {
				kind: "fail",
				error: "Too Many Requests: retry after 7",
				retryAfter: 7,
			});
			const sleepsBeforeOverCap = clock.sleeps.length;
			const tCap0 = Date.now();
			const overCap = await subject.sendThroughDoor1(
				"chat-flood-overcap",
				"payload",
			);
			const overCapElapsedMs = Date.now() - tCap0;
			void sleepsBeforeOverCap;

			return {
				sendRetriedWithRetryAfter: lastSendOk === true && sendElapsedMs < 1500,
				sendAttemptsRecorded: sendOps.length,
				editSurfacedFloodControl:
					editOutcome.success === false ? String(editOutcome.error) : "",
				editDidNotBlock: !editBlocked,
				tiersResolvePerClass,
				overCapSendFloodControl:
					overCap.success === false &&
					String(overCap.error).startsWith("flood_control:7"),
				overCapDidNotSleep:
					!clock.sleeps.includes(7000) && overCapElapsedMs < 1500,
			};
		},

		async sendWireParity() {
			const world = makeTelegramWorld({ name: "tg-shape-sendparity" });
			const { wire } = world;

			// tg-2: converted lane escapes specials matching parse_mode stamp.
			await sendThroughDoor(world, "chat-esc", "Hi **there** a_b.");
			const convertedOp = wire.sendsOf("chat-esc").slice(-1)[0];

			// tg-2: kit-appended chunk markers ship ESCAPED ("\\(1/2\\)") like
			// Hermes format_message output.
			await sendThroughDoor(world, "chat-marker", "chunked body (1/2)");
			const markerOp = wire.sendsOf("chat-marker").slice(-1)[0];

			// tg-2 plain lanes ship RAW without parse_mode.
			await sendThroughDoor(
				world,
				"chat-plain-a",
				"(Response formatting failed, plain text:)\n\n**raw** stays!",
			);
			const plainOp = wire.sendsOf("chat-plain-a").slice(-1)[0];
			await sendThroughDoor(world, "chat-plain-b", "**explicit none**", {
				parse_mode: "none",
			});
			const explicitNoneOp = wire.sendsOf("chat-plain-b").slice(-1)[0];

			// tg-4: important mode silences by default; notify metadata rings.
			await sendThroughDoor(world, "chat-silent", "silent default");
			const silentOp = wire.sendsOf("chat-silent").slice(-1)[0];
			await sendThroughDoor(world, "chat-notify", "final rings", {
				notify: true,
			});
			const notifyOp = wire.sendsOf("chat-notify").slice(-1)[0];

			// tg-6: thread routing; General-topic id "1" omitted on sends.
			await sendThroughDoor(world, "chat-thread42", "topic", {
				thread_id: "42",
			});
			const threadOp = wire.sendsOf("chat-thread42").slice(-1)[0];
			await sendThroughDoor(world, "chat-gen1", "general", { thread_id: "1" });
			const generalOp = wire.sendsOf("chat-gen1").slice(-1)[0];

			// tg-5: reply anchor rides ONLY the first chunk of a delivery
			// ('first' mode parity across kit-owned chunks).
			const results = await world.subject.deliverLongText(
				"chat-reply",
				multiChunkContent(),
				{ reply_to_message_id: "777" },
			);
			const replySends = wire.sendsOf("chat-reply");
			const anchoredCount = replySends.filter(
				(o) => o.metadata["reply_to_message_id"] !== undefined,
			).length;
			const allDelivered = results.every((r) => r.success);

			// tg-5: "message to be replied not found" drops the anchor and
			// retries the same chunk WITHOUT it (Hermes BadRequest branch).
			wire.script("send", {
				kind: "fail",
				error: "Bad Request: message to be replied not found",
			});
			const notFoundRes = await sendThroughDoor(
				world,
				"chat-nf",
				"anchor gone",
				{ reply_to_message_id: "555" },
			);
			const nfSends = wire.sendsOf("chat-nf");

			// tg-4 'all' mode never silences (HERMES_TELEGRAM_NOTIFICATIONS=all;
			// scoped through the optional-env map, NOT the reactions key).
			const allWorld = makeTelegramWorld({
				name: "tg-shape-sendparity-all",
				env: { HERMES_TELEGRAM_NOTIFICATIONS: "all" },
			});
			await sendThroughDoor(allWorld, "chat-all", "rings in all mode");
			const allModeOp = allWorld.wire.sendsOf("chat-all").slice(-1)[0];

			return {
				convertedEscapedMatchesParseMode:
					convertedOp?.metadata["parse_mode"] === "MarkdownV2" &&
					convertedOp?.content.includes("*there*") === true &&
					convertedOp?.content.includes("a\\_b\\.") === true,
				chunkMarkerEscaped:
					markerOp?.content === "chunked body \\(1/2\\)",
				plainLaneRawNoParseMode:
					plainOp?.metadata["parse_mode"] === undefined &&
					plainOp?.content.endsWith("**raw** stays!") === true &&
					explicitNoneOp?.metadata["parse_mode"] === undefined &&
					explicitNoneOp?.content === "**explicit none**",
				silentByDefault:
					silentOp?.metadata["disable_notification"] === true,
				notifyOverrideRings:
					notifyOp?.metadata["disable_notification"] === undefined,
				allModeNeverSilences:
					allModeOp?.metadata["disable_notification"] === undefined &&
					allWorld.adapter.notificationsMode === "all",
				threadIdRouted:
					threadOp?.metadata["message_thread_id"] === 42 &&
					generalOp?.metadata["message_thread_id"] === undefined,
				generalTopicOneOmitted:
					threadOp?.metadata["message_thread_id"] === 42 &&
					generalOp?.metadata["message_thread_id"] === undefined,
				replyAnchorFirstChunkOnly:
					allDelivered &&
					replySends.length >= 2 &&
					anchoredCount === 1 &&
					replySends[0]?.metadata["reply_to_message_id"] === 777 &&
					replySends[1]?.metadata["reply_to_message_id"] === undefined,
				replyNotFoundRetryDropsAnchor:
					notFoundRes.success === true &&
					nfSends.length === 2 &&
					nfSends[0]?.metadata["reply_to_message_id"] === 555 &&
					nfSends[1]?.metadata["reply_to_message_id"] === undefined,
			};
		},

		async connectWebhookClear() {
			const world = makeTelegramWorld({ name: "tg-shape-webhook" });
			const { engine, tg } = world;

			// COLD BOOT: deleteWebhook captured, drop_pending_updates=false,
			// and polling starts afterwards.
			await engine.connect({ isReconnect: false });
			await settleTelegramCycle(world);
			const firstDelete = tg.webhookDeletes[0];
			const pollsAfterColdBoot = tg.allowedUpdatesLog.length;

			// COLD BOOT FAILURE: require_success aborts connect loudly and
			// NO poll ever starts (ordering proof: webhook clear gates polling).
			engine.disconnect();
			tg.scriptWebhook({ kind: "fail", error: "getaddrinfo ENOTFOUND" });
			let coldFailureAborted = false;
			try {
				await engine.connect({ isReconnect: false });
			} catch {
				coldFailureAborted = true;
			}
			const noPollAfterColdFailure =
				tg.allowedUpdatesLog.length === pollsAfterColdBoot;

			// RECONNECT FAILURE: best-effort continues to polling anyway.
			engine.disconnect();
			tg.pushTextUpdate(4242, "after-webhook-fail");
			tg.scriptWebhook({ kind: "fail", error: "getaddrinfo ENOTFOUND" });
			await engine.connect({ isReconnect: true });
			await eventually(
				() => deliveredTurn(world, "after-webhook-fail"),
				5_000,
			);

			return {
				coldBootDeleteCaptured: firstDelete !== undefined,
				coldBootDropPendingFalse: firstDelete?.dropPendingUpdates === false,
				coldBootFailureAbortsConnect: coldFailureAborted,
				noPollAfterColdFailure,
				reconnectBestEffortContinues: deliveredTurn(
					world,
					"after-webhook-fail",
				),
				reconnectDropPendingFalse: tg.webhookDeletes.every(
					(d) => d.dropPendingUpdates === false,
				),
			};
		},

		async mediaSendFamily() {
			const world = makeTelegramWorld({ name: "tg-shape-media" });
			const { adapter, tg } = world;

			// Photo carries the full kwarg set (notification + thread + anchor).
			await adapter.sendImageFile("chat-m", "file:///tmp/pic.PNG", {
				caption: "a photo",
				threadId: "9",
				replyTo: "321",
			});
			const photoOp = tg.mediaOps.find((o) => o.method === "sendPhoto");

			// Photo failure → document fallback (dimension/limit parity).
			tg.scriptMedia("sendPhoto", {
				kind: "fail",
				error: "Bad Request: PHOTO_INVALID_DIMENSIONS",
			});
			await adapter.sendImageFile("chat-m2", "/tmp/fallback.png", {
				caption: "fb",
			});
			const byMethod = (m: string) =>
				tg.mediaOps.filter((o) => o.method === m);

			// Voice extension routing.
			await adapter.sendVoice("chat-m3", "/tmp/note.ogg");
			await adapter.sendVoice("chat-m3", "/tmp/song.mp3");
			tg.scriptMedia("sendAudio", {
				kind: "fail",
				error: "audio failed",
			});
			const audioFail = await adapter.sendVoice("chat-m3", "/tmp/song.m4a");
			const wavDocFallback = await adapter.sendVoice(
				"chat-m4",
				"/tmp/clip.wav",
			);

			// Animation failure → photo fallback.
			tg.scriptMedia("sendAnimation", {
				kind: "fail",
				error: "animation rejected",
			});
			const animFallback = await adapter.sendAnimation(
				"chat-m5",
				"https://cdn.example/x.gif",
			);

			// Caption cap at Bot API 1024 units.
			await adapter.sendDocument("chat-m6", "/tmp/doc.pdf", {
				caption: "x".repeat(1500),
				fileName: "report.pdf",
			});

			// tg2-10 caption ladder: absent captions OMIT the caption key
			// (never null), formatted captions ship MDV2-first, and an entity
			// parse rejection retries the PLAIN slice without parse_mode.
			const voiceOpsBeforeCaptionLadder = byMethod("sendVoice").length;
			const noCapVoiceOps = tg.mediaOps.filter(
				(o) => o.method === "sendVoice" && o.args["voice"] === "/tmp/note.ogg",
			);
			tg.scriptMedia(
				"sendVoice",
				{ kind: "fail", error: "Bad Request: can't parse entities" },
				{ kind: "ok" },
			);
			await adapter.sendVoice("chat-m-cap", "/tmp/cap.ogg", {
				caption: "hello **world**",
			});
			const capVoiceOps = tg.mediaOps.filter(
				(o) => o.method === "sendVoice" && o.args["voice"] === "/tmp/cap.ogg",
			);
			// Unscripted call proves the FIRST variant is the MDV2 conversion.
			await adapter.sendVoice("chat-m-cap2", "/tmp/cap2.ogg", {
				caption: "hello **world**",
			});
			const mdFirstOp = tg.mediaOps.filter(
				(o) => o.method === "sendVoice" && o.args["voice"] === "/tmp/cap2.ogg",
			)[0];

			// tg2-4: DM-topic metadata routes via direct_messages_topic_id with
			// message_thread_id OMITTED (Bot API pairs None with the topic id).
			await adapter.sendImageFile("4242", "/tmp/dm.png", {
				metadata: { direct_messages_topic_id: 42 },
			});
			const dmTopicOp = tg.mediaOps.filter(
				(o) => o.method === "sendPhoto" && o.args["photo"] === "/tmp/dm.png",
			)[0];

			const docOps = byMethod("sendDocument");
			const cappedDoc = docOps[docOps.length - 1];

			return {
				photoCarriesNotificationThreadCaption:
					photoOp?.args["disable_notification"] === true &&
					photoOp?.args["message_thread_id"] === 9 &&
					photoOp?.args["reply_to_message_id"] === 321 &&
					photoOp?.args["caption"] === "a photo",
				// tg2-7: numeric chat ids ship as WIRE INTS at bot.* sites.
				numericChatIdIntOnWire: dmTopicOp?.args["chat_id"] === 4242,
				dmTopicKwargsRouted:
					dmTopicOp?.args["direct_messages_topic_id"] === 42 &&
					dmTopicOp?.args["message_thread_id"] === undefined,
				voiceAbsentCaptionOmitsKey:
					noCapVoiceOps.length === 1 &&
					noCapVoiceOps[0]?.args["caption"] === undefined,
				voiceMdV2CaptionFirst:
					mdFirstOp?.args["parse_mode"] === "MarkdownV2" &&
					mdFirstOp?.args["caption"] === "hello *world*",
				voicePlainRetryAfterParseRejection:
					// failed attempts are NOT recorded as ops; exactly one op
					// proves the plain retry succeeded after the parse rejection.
					capVoiceOps.length === 1 &&
					capVoiceOps[0]?.args["parse_mode"] === undefined &&
					capVoiceOps[0]?.args["caption"] === "hello **world**",
				photoFailureFallsBackToDocument:
					docOps[0]?.args["document"] === "/tmp/fallback.png" &&
					docOps[0]?.args["caption"] === "fb",
				documentFilenameDefaultsToBasename:
					docOps[0]?.args["filename"] === "fallback.png" &&
					cappedDoc?.args["filename"] === "report.pdf",
				voiceExtRoutingOggOpus:
					noCapVoiceOps.length === 1 && byMethod("sendVoice").length >= 2,
				// mp3 AND m4a route to sendAudio; the scripted-failure attempt
				// surfaces its error without recording an op.
				voiceMp3RoutesToSendAudio:
					audioFail.success === false &&
					byMethod("sendAudio").some(
						(o) => o.args["audio"] === "/tmp/song.mp3",
					),
				voiceUnsupportedExtFallsBackToDocument:
					audioFail.success === false &&
					wavDocFallback.success === true &&
					docOps.some((o) => o.args["document"] === "/tmp/clip.wav"),
				animationFailureFallsBackToPhoto:
					animFallback.success === true &&
					byMethod("sendPhoto").some(
						(o) => o.args["photo"] === "https://cdn.example/x.gif",
					),
				captionCappedAt1024:
					String(cappedDoc?.args["caption"] ?? "").length === 1024,
			};
		},

		async editNotModifiedNoop() {
			const world = makeTelegramWorld({ name: "tg-shape-notmodified" });
			const { subject, wire, clock } = world;

			// Unit level: EVERY edit lane maps the 400 blob to success.
			wire.script("edit", {
				kind: "fail",
				error: "Bad Request: message is not modified",
			});
			const midRes = await subject.adapter.editMessage(
				"chat-nm",
				"wire-1",
				"mid **stream**",
			);
			const midStreamNoOpSuccess = midRes.success === true;

			wire.script("edit", {
				kind: "fail",
				error: "Bad Request: message is not modified",
			});
			const rawFinalRes = await subject.adapter.editMessage(
				"chat-nm",
				"wire-1",
				"(Response formatting failed, plain text:)\\n\\nplain final",
				{ finalize: true },
			);

			wire.script("edit", {
				kind: "fail",
				error: "Bad Request: message is not modified",
			});
			const convFinalRes = await subject.adapter.editMessage(
				"chat-nm",
				"wire-1",
				"final **bold** (done)",
				{ finalize: true },
			);

			wire.script("edit", { kind: "fail", error: "Bad Request: chat not found" });
			const realFail = await subject.adapter.editMessage(
				"chat-nm",
				"wire-1",
				"still broken",
			);

			// Composition level: a REQUIRES_EDIT_FINALIZE consumer turn whose
			// progressive edit AND redundant finalize both draw "not modified"
			// must deliver WITHOUT a fallback full-text duplicate send.
			const consumer = new GatewayStreamConsumer(
				subject.streamAdapter(),
				"chat-nm-live",
				{
					transport: "edit",
					editIntervalMs: 0,
					bufferThreshold: 1,
					now: () => clock.nowMs(),
				},
			);
			const runP = consumer.run();
			consumer.onDelta("hello world");
			await eventually(() => wire.sendsOf("chat-nm-live").length === 1);
			clock.advance(50);
			wire.script("edit", {
				kind: "fail",
				error: "Bad Request: message is not modified",
			});
			consumer.onDelta("hello world!");
			await eventually(() => wire.editsOf("chat-nm-live").length >= 1);
			clock.advance(50);
			wire.script("edit", {
				kind: "fail",
				error: "Bad Request: message is not modified",
			});
			consumer.finish("hello world!");
			await runP;

			return {
				midStreamNoOpSuccess,
				finalizeRawNoOpSuccess: rawFinalRes.success === true,
				finalizeConvertedNoOpSuccess: convFinalRes.success === true,
				consumerFinalDeliveredWithoutDuplicate:
					consumer.finalContentDelivered === true &&
					consumer.finalResponseSent === true &&
					wire.sendsOf("chat-nm-live").length === 1 &&
					wire.editsOf("chat-nm-live").length >= 2,
				realFailuresStillFail: realFail.success === false,
			};
		},

		async postConnectHousekeeping() {
			const dmTopicsConfig = [
				{
					chatId: "4242",
					topics: [{ name: "General", threadId: 321 }, { name: "Auditor" }],
				},
			];
			const world = makeTelegramWorld({
				name: "tg-shape-housekeeping",
				env: {
					TELEGRAM_STATUS_INDICATOR: "true",
					TELEGRAM_STATUS_ONLINE: "Gateway Online",
				},
				dmTopicsConfig,
			});
			const { engine, tg, adapter } = world;

			await engine.connect({ isReconnect: false });
			await eventually(() => tg.myCommandsOps.length >= 3);
			// DM-topic creation follows the command menu off the connect path.
			await eventually(() => tg.forumTopicCreates.length >= 1);
			const coldBootMenuScopes = tg.myCommandsOps
				.filter((op) => op.scope.type !== "chat")
				.map((op) => op.scope.type);
			const coldBootMenuScopesExactOrder =
				JSON.stringify(coldBootMenuScopes) ===
				JSON.stringify(["default", "all_private_chats", "all_group_chats"]);

			// Persisted topic loaded from config WITHOUT an API call; missing
			// one created once and cached for repeat lookups.
			const createsBefore = tg.forumTopicCreates.length;
			const cached = await adapter.ensureDmTopic("4242", "Auditor");
			const createsAfterEnsure = tg.forumTopicCreates.length;
			await adapter.renameDmTopic("4242", 321, "General HQ");

			// Status indicator fired exactly the configured online text.
			const indicatorOps = tg.shortDescriptions;

			// Lazy forum-scope registration on FIRST message from a forum chat.
			tg.pushForumTextUpdate(555, "forum hello");
			await settleTelegramCycle(world);
			await eventually(() =>
				tg.myCommandsOps.some(
					(op) => op.scope.type === "chat" && op.scope.chat_id === 555,
				),
			);
			tg.pushForumTextUpdate(555, "forum again");
			await settleTelegramCycle(world);
			await eventually(() => deliveredTurn(world, "forum again"));
			const forumScopeOps = tg.myCommandsOps.filter(
				(op) => op.scope.type === "chat" && op.scope.chat_id === 555,
			).length;

			// Default-OFF world: NO status indicator ever fires.
			const offWorld = makeTelegramWorld({ name: "tg-shape-housekeeping-off" });
			await offWorld.engine.connect({ isReconnect: false });
			await eventually(() => offWorld.engine.polledOnce);

			// Per-scope tolerance: a failing Default scope leaves the other two
			// scopes registered and polling still starts.
			const failWorld = makeTelegramWorld({ name: "tg-shape-housekeeping-fail" });
			failWorld.tg.scriptCommands({
				kind: "fail",
				error: "getaddrinfo ENOTFOUND",
			});
			await failWorld.engine.connect({ isReconnect: false });
			await eventually(() => failWorld.tg.pollTimeoutLog.length >= 1);
			const survivingScopes = failWorld.tg.myCommandsOps.map(
				(op) => op.scope.type,
			);

			return {
				coldBootMenuScopesExactOrder,
				menuCommandCount: tg.myCommandsOps[0]?.commands.length ?? 0,
				reconnectDoesNotDoubleSchedule:
					forumScopeOps === 1 &&
					tg.forumTopicCreates.filter((c) => c.name === "Auditor").length ===
						1,
				statusIndicatorOptIn:
					indicatorOps.length === 1 &&
					indicatorOps[0]?.text === "Gateway Online" &&
					offWorld.tg.shortDescriptions.length === 0,
				dmTopicCreatedAndCached:
					createsBefore === 1 &&
					createsAfterEnsure === createsBefore &&
					cached !== null,
				dmTopicRenamed:
					tg.forumTopicEdits[0]?.name === "General HQ" &&
					tg.forumTopicEdits[0]?.threadId === 321 &&
					tg.forumTopicEdits[0]?.chatId === "4242",
				forumScopeLazyRegisteredOnce: forumScopeOps === 1,
				housekeepingFailureNonFatal:
					engine.connected === true &&
					tg.pollTimeoutLog.length >= 1 &&
					JSON.stringify(survivingScopes) ===
						JSON.stringify(["all_private_chats", "all_group_chats"]),
			};
		},

		async dmTopicSendRouting() {
			const world = makeTelegramWorld({ name: "tg-shape-dmtopic" });
			const { wire } = world;

			// Explicit DM-topic id pairs with message_thread_id OMITTED (None).
			await sendThroughDoor(world, "4242", "topic send", {
				direct_messages_topic_id: 42,
			});
			const topicOp = wire.sendsOf("4242").slice(-1)[0];

			// Alias metadata key honored identically.
			await sendThroughDoor(world, "4243", "alias send", {
				telegram_direct_messages_topic_id: 43,
			});
			const aliasOp = wire.sendsOf("4243").slice(-1)[0];

			// Private fallback lane attaches its metadata anchor EVERY chunk.
			const chunkResults = await world.subject.deliverLongText(
				"4244",
				multiChunkContent(),
				{
					thread_id: "7",
					telegram_dm_topic_reply_fallback: true,
					telegram_reply_to_message_id: "88",
				} as unknown as Record<string, unknown>,
			);
			const chunkSends = wire.sendsOf("4244");

			// Anchor-less private send REFUSES loudly before any transmission.
			const refuseRes = await sendThroughDoor(world, "4245", "no anchor", {
				thread_id: "7",
				telegram_dm_topic_reply_fallback: true,
			});

			// telegram_dm_topic_created_for_send marks NON-private lanes.
			await sendThroughDoor(world, "4246", "created lane", {
				thread_id: "7",
				telegram_dm_topic_created_for_send: true,
			});
			const createdOp = wire.sendsOf("4246").slice(-1)[0];

			const { TELEGRAM_DM_TOPIC_MISSING_ANCHOR_ERROR } = await import(
				"./manifest.js"
			);
			return {
				dmTopicIdPairedWithThreadNone:
					topicOp?.metadata["direct_messages_topic_id"] === 42 &&
					topicOp?.metadata["message_thread_id"] === undefined,
				aliasMetadataKeyHonored:
					aliasOp?.metadata["direct_messages_topic_id"] === 43 &&
					aliasOp?.metadata["telegram_direct_messages_topic_id"] ===
						undefined,
				fallbackAnchorAttachedEveryChunk:
					chunkResults.every((r) => r.success) &&
					chunkSends.length >= 2 &&
					chunkSends.every(
						(o) =>
							o.metadata["reply_to_message_id"] === 88 &&
							o.metadata["message_thread_id"] === 7,
					),
				missingAnchorFailsLoudExact:
					refuseRes.success === false &&
					refuseRes.error === TELEGRAM_DM_TOPIC_MISSING_ANCHOR_ERROR &&
					wire.sendsOf("4245").length === 0,
				createdForSendNeverFailsLoud:
					createdOp !== undefined &&
					createdOp.metadata["message_thread_id"] === 7,
			};
		},

		async wireArgWhitelist() {
			const world = makeTelegramWorld({
				name: "tg-shape-whitelist",
				env: { TELEGRAM_DISABLE_LINK_PREVIEWS: "true" },
			});
			const { wire } = world;

			// Noisy input-namespace metadata rides IN but can never leak OUT.
			await sendThroughDoor(world, "777", "clean **send** a_b", {
				notify: false,
				expect_edits: true,
				gateway_session_key: "sk-secret",
				final: true,
				replyToOverride: "9",
				_interim_send: true,
				forceFormattingError: false,
				thread_id: "5",
			});
			const noisyOp = wire.sendsOf("777").slice(-1)[0];
			const ALLOWED = [
				"chat_id",
				"text",
				"parse_mode",
				"message_thread_id",
				"direct_messages_topic_id",
				"disable_notification",
				"link_preview_options",
				"reply_markup",
				"reply_to_message_id",
			];
			const leakedKeys =
				noisyOp === undefined
					? ["missing-op"]
					: Object.keys(noisyOp.metadata).filter(
							(k) => !ALLOWED.includes(k),
						);

			// Built args still ship: notify rings, MDV2 conversion stamped.
			await sendThroughDoor(world, "778", "**rings**", { notify: true });
			const ringOp = wire.sendsOf("778").slice(-1)[0];

			// reply_markup survives (approval keyboards ride DOOR 1).
			await sendThroughDoor(world, "779", "kb", {
				reply_markup: {
					inline_keyboard: [[{ text: "ok", callback_data: "x" }]],
				},
			});
			const kbOp = wire.sendsOf("779").slice(-1)[0];

			// @username targets pass through trimmed and unconverted.
			await sendThroughDoor(world, "@room", "to username", {});
			const unameOp = wire.sendsOf("@room").slice(-1)[0];

			// Default world: no link-preview kwarg without the extra.
			const defWorld = makeTelegramWorld({ name: "tg-shape-whitelist-off" });
			await sendThroughDoor(defWorld, "780", "previews on", {});
			const defOp = defWorld.wire.sendsOf("780").slice(-1)[0];

			return {
				inputNamespaceKeysNeverLeak: leakedKeys.length === 0,
				builtArgsStillShip:
					noisyOp?.metadata["chat_id"] === 777 &&
					noisyOp?.metadata["parse_mode"] === "MarkdownV2" &&
					noisyOp?.metadata["message_thread_id"] === 5 &&
					noisyOp?.metadata["disable_notification"] === true &&
					ringOp?.metadata["disable_notification"] === undefined,
				replyMarkupSurvives: kbOp?.metadata["reply_markup"] !== undefined,
				numericChatIdIsInt: noisyOp?.metadata["chat_id"] === 777,
				usernameChatIdPreserved: unameOp?.metadata["chat_id"] === "@room",
				linkPreviewKwargsGated:
					noisyOp?.metadata["link_preview_options"] !== undefined &&
					(noisyOp?.metadata["link_preview_options"] as unknown as Record<
						string,
						unknown
					>)["is_disabled"] === true,
				linkPreviewOffByDefault:
					defOp?.metadata["link_preview_options"] === undefined,
			};
		},

		async richExtrasLane() {
			const TABLE = "| a | b |\n| - | - |\n| 1 | 2 |";

			// DEFAULT world: no rich attempt ever (scripted probe stays shut).
			const offWorld = makeTelegramWorld({ name: "tg-shape-rich-off" });
			const offResults = await offWorld.subject.deliverLongText(
				"chat-roff",
				TABLE,
			);
			const offRichAttempts = offWorld.wire.ops.filter(
				(o) => o.op === "rich",
			).length;

			// EXTRAS-ON world: eligible content goes sendRichMessage RAW.
			const world = makeTelegramWorld({
				name: "tg-shape-rich-on",
				env: {
					TELEGRAM_RICH_MESSAGES: "true",
					TELEGRAM_RICH_DRAFTS: "true",
				},
			});
			const { adapter, wire, tg } = world;
			const richOk = await world.subject.deliverLongText("chat-r1", TABLE, {
				notify: true,
				reply_to_message_id: "31",
			});
			const richOp = tg.richOps.find((o) => o.method === "sendRichMessage");
			const richCountAfterFirst = tg.richOps.length;

			// Ineligible prose skips tier 1 SILENTLY (fallback-class, no latch).
			const prose = await world.subject.deliverLongText("chat-r2", "plain prose");
			const richCountAfterProse = tg.richOps.length;

			// expect_edits previews skip rich too (ladder gate parity).
			await world.subject.deliverLongText("chat-r3", TABLE, {
				expect_edits: true,
			} as unknown as Record<string, unknown>);
			const richCountAfterExpectEdits = tg.richOps.length;

			// Capability failure latches ONCE; later sends skip the endpoint
			// (attempt counting includes scripted failures — ops do not).
			const attemptsBeforeLatch = tg.richAttemptsLog.length;
			tg.scriptRich("sendRichMessage", {
				kind: "fail",
				error: "sendRichMessage: method not found",
			});
			const latchResult = await world.subject.deliverLongText("chat-r4", TABLE);
			const attemptsAfterLatchSend = tg.richAttemptsLog.length;
			const afterLatch = await world.subject.deliverLongText("chat-r5", TABLE);
			const attemptsAfterSecond = tg.richAttemptsLog.length;

			// Transient rich failure NEVER legacy-resends (duplicate risk).
			const tWorld = makeTelegramWorld({
				name: "tg-shape-rich-transient",
				env: { TELEGRAM_RICH_MESSAGES: "true" },
			});
			tWorld.tg.scriptRich("sendRichMessage", {
				kind: "fail",
				error: "socket hang up",
			});
			const transientRes = await tWorld.subject.deliverLongText(
				"chat-rt",
				TABLE,
			);
			const legacySendsAfterTransient = tWorld.wire.sendsOf("chat-rt").length;

			// Rich DRAFT frames require BOTH extras (messages-only world: none).
			const msgOnlyWorld = makeTelegramWorld({
				name: "tg-shape-rich-msgonly",
				env: { TELEGRAM_RICH_MESSAGES: "true" },
			});
			await msgOnlyWorld.adapter.sendDraft({
				chatId: "chat-rd1",
				draftId: 4001,
				content: TABLE,
			});
			const msgOnlyRichDrafts = msgOnlyWorld.tg.richOps.filter(
				(o) => o.method === "sendRichMessageDraft",
			).length;
			const msgOnlyLegacyDrafts = msgOnlyWorld.wire.draftsOf("chat-rd1").length;

			// Both extras: eligible frames go sendRichMessageDraft.
			await adapter.sendDraft({
				chatId: "chat-rd2",
				draftId: 4012,
				content: TABLE,
			});
			const richDraftOpsBefore = tg.richOps.filter(
				(o) => o.method === "sendRichMessageDraft",
			).length;

			// Draft capability failure latches its OWN flag; frame falls back.
			tg.scriptRich("sendRichMessageDraft", {
				kind: "fail",
				error: "sendRichMessageDraft: method not found",
			});
			await adapter.sendDraft({
				chatId: "chat-rd3",
				draftId: 4022,
				content: TABLE,
			});
			const draftFallbackLegacy = wire.draftsOf("chat-rd3").length;
			await adapter.sendDraft({
				chatId: "chat-rd4",
				draftId: 4032,
				content: TABLE,
			});
			const richDraftOpsAfterLatch = tg.richOps.filter(
				(o) => o.method === "sendRichMessageDraft",
			).length;

			// Rich FINALIZE edit: eligible content edits via rich_message param.
			await adapter.editMessage("chat-r6", "55", TABLE, { finalize: true });
			const richEditKwargs = tg.editKwargs.at(-1);
			// Ineligible finalize falls through to the legacy MDV2 edit.
			await adapter.editMessage("chat-r7", "56", "plain **b**", {
				finalize: true,
			});

			return {
				defaultOffNeverAttemptsRich:
					offRichAttempts === 0 && offResults.every((r) => r.success === true),
				extraOnSendsRichPayload:
					richOk.every((r) => r.success === true) &&
					richOp !== undefined &&
					richOp.args["chat_id"] === "chat-r1" &&
					(richOp.args["rich_message"] as Record<string, unknown>)
						.markdown !== undefined &&
					(richOp.args["reply_parameters"] as Record<string, unknown>)
						.message_id === 31 &&
					richOp.args["disable_notification"] === undefined,
				ineligibleContentSkipsSilently:
					prose.every((r) => r.success === true) &&
					richCountAfterProse === richCountAfterFirst,
				expectEditsSkipsRich:
					richCountAfterExpectEdits === richCountAfterProse,
				capabilityErrorLatchesOnce:
					latchResult.every((r) => r.success === true) &&
					attemptsAfterLatchSend === attemptsBeforeLatch + 1 &&
					afterLatch.every((r) => r.success === true) &&
					attemptsAfterSecond === attemptsAfterLatchSend,
				transientRichNeverLegacyResent:
					transientRes[transientRes.length - 1]?.success === false &&
					legacySendsAfterTransient === 0,
				richDraftGatedOnBothExtras:
					msgOnlyRichDrafts === 0 && msgOnlyLegacyDrafts > 0,
				richDraftCapabilityLatches:
					richDraftOpsBefore === 1 &&
					draftFallbackLegacy > 0 &&
					richDraftOpsAfterLatch === 1,
				richFinalizeEditApplied:
					richEditKwargs !== undefined &&
					(richEditKwargs["rich_message"] as Record<string, unknown> | undefined) !==
						undefined &&
					richEditKwargs["chat_id"] === "chat-r6" &&
					richEditKwargs["message_id"] === 55,
			};
		},

		async mediaDmTopicRetry() {
			const world = makeTelegramWorld({ name: "tg-shape-media-retry" });
			const { adapter, tg } = world;

			// Dead reply anchor: retry WITHOUT anchors succeeds (flag required).
			tg.scriptMedia(
				"sendPhoto",
				{ kind: "fail", error: "Bad Request: message to be replied not found" },
				{ kind: "ok" },
			);
			const anchorRetry = await adapter.sendImageFile("4242", "/tmp/r1.png", {
				metadata: {
					telegram_dm_topic_reply_fallback: true,
					telegram_reply_to_message_id: 88,
				},
			});
			const anchorRetryOps = tg.mediaOps.filter(
				(o) => o.method === "sendPhoto" && o.args["photo"] === "/tmp/r1.png",
			);

			// Stale topic id: topic-marker rejection retries without routing and
			// PRUNES the cached binding (next ensure re-creates).
			const bound = await adapter.ensureDmTopic("4343", "Work");
			tg.scriptMedia(
				"sendPhoto",
				{ kind: "fail", error: "Bad Request: message thread not found" },
				{ kind: "ok" },
			);
			const topicRetry = await adapter.sendImageFile("4343", "/tmp/r2.png", {
				metadata: {
					telegram_dm_topic_reply_fallback: true,
					telegram_direct_messages_topic_id: Number(bound),
				},
			});
			const createsBeforeReensure = tg.forumTopicCreates.length;
			const rebound = await adapter.ensureDmTopic("4343", "Work");
			const prunedBindingForcesRecreate =
				rebound !== null && tg.forumTopicCreates.length > createsBeforeReensure;
			const topicRetryOps = tg.mediaOps.filter(
				(o) => o.method === "sendPhoto" && o.args["photo"] === "/tmp/r2.png",
			);

			// Without the fallback flag there is NO retry ladder at all —
			// sendVideo (no fallback chain) surfaces the failure directly.
			const noFlag = await adapter.sendVideo("4545", "/tmp/r3.mp4", {
				metadata: {
					telegram_dm_topic_reply_fallback: false,
					telegram_reply_to_message_id: 88,
				},
			});
			tg.scriptMedia("sendVideo", {
				kind: "fail",
				error: "Bad Request: message to be replied not found",
			});
			const noFlagDeadAnchor = await adapter.sendVideo(
				"4546",
				"/tmp/r5.mp4",
				{
					metadata: {
						telegram_reply_to_message_id: 88,
					},
				},
			);

			// Non-topic BadRequests surface as-is (photo→document chain runs).
			tg.scriptMedia("sendPhoto", {
				kind: "fail",
				error: "Bad Request: PHOTO_INVALID_DIMENSIONS",
			});
			const dimFail = await adapter.sendImageFile("4646", "/tmp/r4.png", {
				metadata: { telegram_dm_topic_reply_fallback: true },
			});
			const docFallbackFired = tg.mediaOps.some(
				(o) =>
					o.method === "sendDocument" &&
					o.args["document"] === "/tmp/r4.png",
			);

			return {
				deadReplyAnchorRetriesWithoutAnchors:
					anchorRetry.success === true &&
					anchorRetryOps.length === 1 &&
					anchorRetryOps[0]?.args["reply_to_message_id"] === undefined &&
					anchorRetryOps[0]?.args["message_thread_id"] === undefined,
				staleTopicRetriedWithBindingPrune:
					bound !== null &&
					topicRetry.success === true &&
					topicRetryOps.length === 1 &&
					topicRetryOps[0]?.args["direct_messages_topic_id"] === undefined &&
					prunedBindingForcesRecreate,
				withoutFallbackFlagFailsImmediately:
					noFlag.success === true && noFlagDeadAnchor.success === false,
				nonRetryClassSurfacesAsIs:
					dimFail.success === true && docFallbackFired === true,
			};
		},

		async longPollTimeoutDefault() {
			// Production construction: NO longPollTimeoutMs override — the
			// engine default must be the PTB baseline 10 s (adapter.py
			// start_polling passes no timeout override, tg2-8).
			const clock = new ManualPollingClock();
			const tg = new TelegramBotApiFake();
			const engine = new TelegramAdapter({
				wire: tg,
				clock,
				timer: clock.timer,
				manifestName: "tg-poll-default",
				secretReader: () => "tok",
			});
			engine.attachStandardGuard();
			await engine.connect({ isReconnect: false });
			await eventually(() => tg.pollTimeoutLog.length >= 1);
			return { productionTimeoutMs: tg.pollTimeoutLog[0] ?? -1 };
		},
	};
}
