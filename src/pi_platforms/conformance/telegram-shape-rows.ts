// pi_platforms/conformance/telegram-shape-rows — TELEGRAM-SHAPE conformance
// rows (Phase-6 census port; DEC-024). These are the SHAPE DELTAS beyond the
// inherited polling transport family: update-object parsing, edit-vs-send
// reconciliation through the chokepoint, callback_query round-trips within
// the 64-byte cap, reaction ack lifecycle + inbound reactions (A1/A2), typing
// variant matrix incl. thread-status text (A11), sticker description cache
// hit/miss/expiry (M7, injected clock), and FloodWait honored at telegram
// METHOD CLASSES with manifest-declared tiers (Q17).
//
// Row bodies run against the REAL engine via makeTelegramShapeFixture() —
// no stubbed return values; each call builds fresh worlds.

import type { ConformanceRow } from "./rows.js";
import { makeTelegramShapeFixture } from "../telegram/telegram-world.js";

const SHAPE = new Set(["polling"] as const);

function mk(
	id: string,
	title: string,
	body: () => Promise<Record<string, unknown>>,
	asserts: (r: Record<string, unknown>) => string | null,
): ConformanceRow {
	return {
		id,
		title,
		shapes: SHAPE,
		run: async () => {
			try {
				const result = await body();
				const problem = asserts(result);
				if (problem !== null) {
					return { id, title, pass: false, shapes: SHAPE, detail: problem };
				}
				return { id, title, pass: true, shapes: SHAPE };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: SHAPE,
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	};
}

export function makeTelegramShapeRows(): ConformanceRow[] {
	const f = () => makeTelegramShapeFixture();
	return [
		mk(
			"tg.update-parsing-deltas",
			"telegram: raw update-object parsing deltas — message→turn, edited/reaction/callback routed, unwired kinds tolerated, ONE offset space ACKs every kind, EVERY poll carries allowed_updates=ALL_TYPES",
			() => f().updateParsingDeltas(),
			(r) => {
				if (Number(r.textTurns) !== 1)
					return `message update must yield exactly one turn, got ${String(r.textTurns)}`;
				if (Number(r.editedEvents) !== 1)
					return "edited_message must normalize to a platform event (no turn)";
				if (r.editedEventNormalized !== true)
					return "edited_message must normalize to a message_edited platform event (tg2-11)";
				if (r.editedEventPayloadExact !== true)
					return "message_edited payload must carry chatId/messageId/threadId/text/editedAt bounded (:4284)";
				if (r.editedHandlerFired !== true)
					return "normalized edited events must fan out through the handler seam";
				if (Number(r.reactionEvents) !== 1)
					return "message_reaction must normalize to ONE event";
				if (Number(r.callbackRouted) !== 1)
					return "callback_query must route through the query handler";
				if (r.unwiredIgnored !== true)
					return "channel_post (unwired kind) must be tolerated without becoming a turn";
				if (Number(r.pendingAfterAck) !== 0)
					return "every kind shares ONE offset space — all ACKed, none re-delivered";
				if (r.pollRequestedAllTypes !== true)
					return "every getUpdates must carry allowed_updates=ALL_TYPES (tg-1) — without it real Telegram never delivers reaction updates";
				return null;
			},
		),
		mk(
			"tg.send-wire-parity",
			"telegram send lane: FULL MarkdownV2 escaping matching parse_mode (chunk markers \\(i/n\\)), plain lanes raw, important-mode silence w/ notify override + all-mode opt-out, threadIdForSend routing (General '1' omitted), reply anchor on FIRST chunk only + not-found retry drops it",
			() => f().sendWireParity(),
			(r) => {
				if (r.convertedEscapedMatchesParseMode !== true)
					return "converted sends must escape MDV2 specials matching parse_mode=MarkdownV2 (tg-2)";
				if (r.chunkMarkerEscaped !== true)
					return "chunk markers must ship escaped '\\(1/2\\)' like Hermes format_message output (tg-2)";
				if (r.plainLaneRawNoParseMode !== true)
					return "plain lanes (§6.1 fallback / parse_mode none) must ship RAW without parse_mode";
				if (r.silentByDefault !== true)
					return "important notification mode must ship disable_notification=true by default (tg-4)";
				if (r.notifyOverrideRings !== true)
					return "metadata notify=true must suppress disable_notification (tg-4)";
				if (r.allModeNeverSilences !== true)
					return "HERMES_TELEGRAM_NOTIFICATIONS=all must never silence sends (tg-4)";
				if (r.threadIdRouted !== true || r.generalTopicOneOmitted !== true)
					return "thread_id metadata must route through threadIdForSend with General-topic '1' omitted (tg-6)";
				if (r.replyAnchorFirstChunkOnly !== true)
					return "reply_to_message_id must ride ONLY the first chunk of a delivery (tg-5 'first' mode)";
				if (r.replyNotFoundRetryDropsAnchor !== true)
					return "'message to be replied not found' must drop the anchor and retry without it (tg-5)";
				return null;
			},
		),
		mk(
			"tg.edit-send-reconciliation",
			"telegram: edit-vs-send reconciliation through the chokepoint — draft frames are REAL sendMessageDraft calls with MarkdownV2-first text (tg-13), mid-stream edits RAW with NO parse_mode, finalize edits FULL MarkdownV2 + parse_mode stamp (#25710/tg-3), edit FloodWait never blocks",
			() => f().editVsSendReconciliation(),
			(r) => {
				if (r.draftLaneSendMessageDraftMarkdownV2 !== true)
					return "native draft frames must be sendMessageDraft calls carrying {chat_id, draft_id, text, parse_mode:MarkdownV2} with the send-path conversion applied (tg-13, adapter.py:send_draft :6116)";
				if (r.midStreamEditRaw !== true)
					return "mid-stream progressive edits must stay RAW prefix-stable";
				if (r.finalizeConvertedEscaped !== true)
					return "finalize edit must carry FULL MarkdownV2 conversion + escaping (REQUIRES_EDIT_FINALIZE #25710)";
				if (r.finalizeParseModeStamped !== true)
					return "finalize edit must stamp parse_mode=MarkdownV2 while mid-stream edits omit it (tg-3)";
				if (r.editFloodNonBlocking !== true)
					return "edit-site FloodWait must NOT block the caller";
				if (!String(r.editFloodErrorSurface).includes("flood_control"))
					return `edit flood must surface flood_control:<wait> with retryAfter, got ${JSON.stringify(r.editFloodErrorSurface)}`;
				return null;
			},
		),
		mk(
			"tg.callback-roundtrip-64b",
			"telegram: callback_data builder→door-keyboard→callback_query→router→resolver round-trip within 64 bytes; spinner always answered; consumed keyboard stripped; unauthorized ignored; production-default clicker authz FAILS CLOSED (tg-11)",
			() => f().callbackRoundTrip(),
			(r) => {
				if (r.keyboardAttachedToDoorSend !== true)
					return "approval prompt through DOOR 1 must carry the inline keyboard";
				if (r.dataWithin64Bytes !== true)
					return "callback_data exceeds Telegram's 64-byte cap";
				if (r.resolverFired !== true)
					return "authorized tap must fire the approval resolver";
				if (Number(r.spinnerAnswered) < 3)
					return "EVERY tap answers (spinner clears) — authorized, double-tap, unauthorized";
				if (r.hostMarkupStripped !== true)
					return "resolved taps edit the host TEXT under MarkdownV2 with reply_markup removed (tg2-2 :7280)";
				if (r.doubleTapStaleAnswered !== true)
					return "double-tap answered stale, resolved exactly once";
				if (r.unauthorizedNotResolved !== true)
					return "unauthorized clicker answered but NEVER resolved";
				if (r.defaultClosedUnauthorizedNotResolved !== true)
					return "with no forced override and no authz env, a fresh subject must DENY taps through the real chain (_is_callback_user_authorized parity, tg-11)";
				return null;
			},
		),
		mk(
			"tg.reaction-ack-lifecycle",
			"telegram A1: reaction-ack lifecycle — opt-in gate off by default; 👀 start swaps to 👍 success / 👎 failure / cleared on cancel; scripted failures swallowed",
			() => f().reactionAckLifecycle(),
			(r) => {
				if (Number(r.disabledWorldOps) !== 0)
					return "reactions gate OFF ⇒ zero reaction ops (opt-in feature)";
				if (r.enabledStartEmoji !== "👀")
					return `processing start must set 👀, got ${JSON.stringify(r.enabledStartEmoji)}`;
				if (r.successSwapEmoji !== "👍")
					return `success must swap to 👍 (replace-not-stack), got ${JSON.stringify(r.successSwapEmoji)}`;
				if (r.failureSwapEmoji !== "👎")
					return `failure must swap to 👎, got ${JSON.stringify(r.failureSwapEmoji)}`;
				if (r.cancelCleared !== true)
					return "CANCELLED must CLEAR the in-progress reaction (set_message_reaction null)";
				if (r.scriptedFailureSwallowed !== true)
					return "scripted reaction failures are swallowed — hooks never break flow";
				return null;
			},
		),
		mk(
			"tg.inbound-reactions",
			"telegram A2: inbound message_reaction events normalized with hard caps (64 items / 64-char emoji / 128-char custom id); handler fan-out; forum-signal doubling; invalid shapes tolerated",
			() => f().inboundReactions(),
			(r) => {
				if (Number(r.normalizedEvents) !== 2)
					return `valid reactions normalize (${String(r.normalizedEvents)} ≠ 2 — invalid dropped, valid kept)`;
				if (r.emojiCapRespected !== true)
					return "emoji list caps at 64 items of ≤64 chars";
				if (r.customIdCapRespected !== true)
					return "custom_emoji_id truncates at 128 chars";
				if (r.handlerInvoked !== true)
					return "registered reaction handler fans out";
				if (r.forumSignalDoubled !== true)
					return "thread-less reactions double as forum signals (A2)";
				if (r.invalidTolerated !== true)
					return "invalid shapes tolerated (never thrown, never dispatched)";
				return null;
			},
		),
		mk(
			"tg.typing-variant-matrix",
			"telegram A11: typing — wire action PINNED to 'typing' even when a variant/decoy action is configured (adapter.py:send_typing :8400/:8412 truth, tg-10), General-topic thread '1' PRESERVED, thread-status text carried, transient-failure cooldown suppresses then expires (injected clock), approval-wait pause skips",
			() => f().typingVariants(),
			(r) => {
				if (r.actionOnWire !== "typing")
					return `production typing must ALWAYS send action='typing', got ${JSON.stringify(r.actionOnWire)} (tg-10)`;
				if (r.threadOnePreservedOnTyping !== true)
					return "typing PRESERVES thread id '1' (_message_thread_id_for_typing asymmetry)";
				if (r.statusTextCarried !== "Uploading photo…")
					return "thread-status text must be carried alongside refresh bubbles";
				if (r.cooldownSuppressedNextTick !== true)
					return "transient failure cooldown must suppress the next bubble";
				if (
					Number(r.cooldownClampedSeconds) < 1 ||
					Number(r.cooldownClampedSeconds) > 300
				)
					return "cooldown clamps to [1,300] seconds";
				if (r.pauseSkippedBubble !== true)
					return "approval-wait pause must skip bubbles entirely";
				if (r.resumedAfterResume !== true)
					return "resume restores the refresh loop";
				if (r.floodHonoredOnceAtTypingSite !== true)
					return "post-cooldown typing recovers (FloodWait honored once before)";
				return null;
			},
		),
		mk(
			"tg.sticker-cache-hit-miss-expiry",
			"telegram M7: sticker description cache — miss→vision→cached injection; hit skips vision; TTL expiry under INJECTED clock recalls vision; animated stickers never analyze",
			() => f().stickerCacheFlow(),
			(r) => {
				if (Number(r.firstCallVisionCalls) !== 1)
					return "cache MISS must analyze exactly once";
				if (Number(r.secondCallVisionCalls) !== 1)
					return "cache HIT must skip vision entirely";
				if (
					!String(r.cachedInjectionExact).startsWith(
						'[The user sent a sticker 😀 from "PiPack"~ It shows:',
					)
				)
					return `warm-format injection exact, got ${JSON.stringify(r.cachedInjectionExact)}`;
				if (r.expiryMissedAfterClockAdvance !== true)
					return "TTL expiry under the injected clock must turn hits back to misses";
				if (Number(r.visionRecalledAfterExpiry) !== 2)
					return "expired entry re-analyzed exactly once more";
				if (r.animatedSkipsVision !== true)
					return "animated stickers never reach vision";
				if (
					r.animatedInjectionExact !==
					"[The user sent an animated sticker 😀~ I can't see animated ones yet, but the emoji suggests: 😀]"
				)
					return `animated injection format exact, got ${JSON.stringify(r.animatedInjectionExact)}`;
				return null;
			},
		),
		mk(
			"tg.floodwait-method-classes",
			"telegram Q17: FloodWait honored per METHOD CLASS — send ladder honors small retry_after inline and recovers, over-cap (>5s) waits fail closed as flood_control:<wait> WITHOUT sleeping (tg-8/#91969); edit class surfaces flood_control WITHOUT blocking; manifest tiers resolve per op",
			() => f().floodwaitMethodClasses(),
			(r) => {
				if (r.sendRetriedWithRetryAfter !== true)
					return "send-class small FloodWait must honor retry_after over the injected clock and recover";
				if (Number(r.sendAttemptsRecorded) < 2)
					return "send ladder records the failed attempt + successful retry";
				if (r.overCapSendFloodControl !== true)
					return "send-class wait >5s must fail closed with error=flood_control:<wait> (tg-8)";
				if (r.overCapDidNotSleep !== true)
					return "send-class over-cap flood must NOT sleep inline (tg-8 #91969)";
				if (!String(r.editSurfacedFloodControl).startsWith("flood_control:"))
					return "edit-class long wait surfaces error=flood_control:<wait>";
				if (r.editDidNotBlock !== true)
					return "edit-class waits >5 s must NOT sleep the caller";
				if (r.tiersResolvePerClass !== true)
					return "manifest rate tiers resolve send/edit/typing to their declared classes";
				return null;
			},
		),
		mk(
			"tg.connect-webhook-clear",
			"telegram connect bootstrap (tg-7): deleteWebhook(drop_pending_updates=false) precedes polling — cold boot REQUIRES success (failure aborts connect before any poll), reconnect is best-effort (polling continues through failure)",
			() => f().connectWebhookClear(),
			(r) => {
				if (r.coldBootDeleteCaptured !== true)
					return "cold boot must call deleteWebhook before start_polling (tg-7)";
				if (r.coldBootDropPendingFalse !== true)
					return "deleteWebhook must always carry drop_pending_updates=false (_delete_webhook_best_effort)";
				if (r.coldBootFailureAbortsConnect !== true)
					return "cold-boot deleteWebhook network failure must abort connect (require_success parity)";
				if (r.noPollAfterColdFailure !== true)
					return "no getUpdates may fire after a failed cold-boot webhook clear (ordering proof)";
				if (r.reconnectBestEffortContinues !== true)
					return "reconnect deleteWebhook failures are best-effort — polling continues";
				if (r.reconnectDropPendingFalse !== true)
					return "every deleteWebhook carries drop_pending_updates=false";
				return null;
			},
		),
		mk(
			"tg.media-send-family",
			"telegram outgoing media family (tg-9): sendPhoto/sendDocument/sendVoice/sendAudio/sendVideo/sendAnimation carry notification+thread+anchor kwargs; photo→document fallback on dimension errors; voice extension routing (.ogg/.opus→voice, .mp3/.m4a→audio, else document); animation→photo fallback; captions capped at 1024",
			() => f().mediaSendFamily(),
			(r) => {
				if (r.photoCarriesNotificationThreadCaption !== true)
					return "media sends must carry disable_notification + message_thread_id + reply anchor + caption (tg-9)";
				if (r.numericChatIdIntOnWire !== true)
					return "numeric chat ids must ship as wire ints at bot.* call sites (tg2-7 telegram_ids.py:23)";
				if (r.dmTopicKwargsRouted !== true)
					return "direct_messages_topic_id metadata must pair with an OMITTED message_thread_id on media (tg2-4)";
				if (r.voiceAbsentCaptionOmitsKey !== true)
					return "absent voice captions must OMIT the caption key, never ship null (tg2-10)";
				if (r.voiceMdV2CaptionFirst !== true)
					return "#32029: fitting captions prefer the MarkdownV2 variant first";
				if (r.voicePlainRetryAfterParseRejection !== true)
					return "parse-entity rejection must retry the PLAIN caption slice without parse_mode (tg2-10 send_voice ladder)";
				if (r.photoFailureFallsBackToDocument !== true)
					return "photo failures must fall back to sendDocument (dimension/limit parity)";
				if (r.documentFilenameDefaultsToBasename !== true)
					return "documents default filename to the source basename";
				if (r.voiceExtRoutingOggOpus !== true)
					return ".ogg/.opus audio must route to sendVoice";
				if (r.voiceMp3RoutesToSendAudio !== true)
					return ".mp3/.m4a audio must route to sendAudio (Bot API mp3/m4a-only)";
				if (r.voiceUnsupportedExtFallsBackToDocument !== true)
					return "unsupported audio extensions must fall back to document delivery, never raise";
				if (r.animationFailureFallsBackToPhoto !== true)
					return "animation failures must fall back to a regular photo send";
				if (r.captionCappedAt1024 !== true)
					return "media captions cap at Bot API 1024 units";
				return null;
			},
		),
		mk(
			"tg.edit-not-modified-noop",
			'telegram tg2-1: "message is not modified" edits map to SendResult(success=true) no-ops on EVERY lane (mid-stream raw, finalize raw, finalize converted) so REQUIRES_EDIT_FINALIZE redundant finalize edits never route into sendFallbackContinuation full-text duplicates (adapter.py:edit_message :5737/:5757/:5929)',
			() => f().editNotModifiedNoop(),
			(r) => {
				if (r.midStreamNoOpSuccess !== true)
					return "mid-stream not-modified edits must succeed as no-ops";
				if (
					r.finalizeRawNoOpSuccess !== true ||
					r.finalizeConvertedNoOpSuccess !== true
				)
					return "finalize not-modified edits must succeed on both raw and converted lanes";
				if (r.consumerFinalDeliveredWithoutDuplicate !== true)
					return "a consumer turn whose finalize edit draws not-modified must deliver WITHOUT a duplicate send";
				if (r.realFailuresStillFail !== true)
					return "real edit failures must still fail — only the not-modified blob maps to success";
				return null;
			},
		),
		mk(
			"tg.stream-delete-retraction",
			"telegram tg-12: stream-consumer retraction rides the REAL Bot API deleteMessage lane — intentional-silence finals and stale edit-path previews delete their preview through adapter.deleteMessage (best-effort, non-fatal; stream_consumer.py:_suppress_silence_marker / abandon :6064 port), and no silence-marker text ever ships",
			() => f().streamDeleteRetraction(),
			(r) => {
				if (r.silenceMarkerPreviewDeleted !== true)
					return "the silence-marker final must retract its streamed preview via a REAL {chat_id, message_id} deleteMessage capture (tg-12)";
				if (r.stalePreviewAbandonDeleted !== true)
					return "a run gone stale must abandon its edit-path preview through the same best-effort delete seam";
				if (r.noSilenceMarkerTextOnWire !== true)
					return "retraction deletes the preview instead of delivering any marker text";
				return null;
			},
		),
		mk(
			"tg.post-connect-housekeeping",
			"telegram tg2-3: post-connect housekeeping OFF the connect path — set_my_commands for Default/AllPrivateChats/AllGroupChats (per-scope failure tolerated), opt-in set_my_short_description status indicator with the :5177 offline stamp on clean disconnect, DM-topic create/load/rename with cache, lazy BotCommandScopeChat(chat_id) for forum chats (:4078/:4110/:4953/:3759/:3873/:9645)",
			() => f().postConnectHousekeeping(),
			(r) => {
				if (r.coldBootMenuScopesExactOrder !== true)
					return "housekeeping must register Default, AllPrivateChats, and AllGroupChats scopes in order (:4110)";
				if (Number(r.menuCommandCount) <= 0)
					return "the command menu derives from the builtin registry and must be non-empty";
				if (r.reconnectDoesNotDoubleSchedule !== true)
					return "forum-scope registration is once-per-chat and DM-topic creation once-per-name";
				if (r.statusIndicatorOptIn !== true)
					return "status indicator fires only when opted in, with the configured online text (default off)";
				if (r.offlineStampOnDisconnect !== true)
					return "disconnect must stamp the Offline short description when opted in and NOTHING in a default-off world (:5172-5184)";
				if (r.dmTopicCreatedAndCached !== true)
					return "persisted topics load without API calls; missing ones create exactly once and cache";
				if (r.dmTopicRenamed !== true)
					return "rename_dm_topic routes editForumTopic with chat/thread/name";
				if (r.forumScopeLazyRegisteredOnce !== true)
					return "first forum message lazily registers BotCommandScopeChat(chat_id) exactly once (:9645)";
				if (r.housekeepingFailureNonFatal !== true)
					return "housekeeping step failures are non-fatal — polling still starts";
				return null;
			},
		),
		mk(
			"tg.dm-topic-send-routing",
			"telegram tg2-4: DM-topic metadata routing — direct_messages_topic_id/telegram_direct_messages_topic_id pair with an OMITTED message_thread_id, private fallback lanes anchor EVERY chunk from telegram_reply_to_message_id, anchor-less sends FAIL LOUD with the exact refuse error before transmission, created_for_send lanes never do (_thread_kwargs_for_send :1552 family)",
			() => f().dmTopicSendRouting(),
			(r) => {
				if (r.dmTopicIdPairedWithThreadNone !== true)
					return "direct_messages_topic_id must ship with message_thread_id omitted (Bot API None pairing)";
				if (r.aliasMetadataKeyHonored !== true)
					return "telegram_direct_messages_topic_id alias metadata must be honored identically";
				if (r.fallbackAnchorAttachedEveryChunk !== true)
					return "private fallback-lane sends attach their metadata reply anchor on EVERY chunk";
				if (r.missingAnchorFailsLoudExact !== true)
					return "anchor-less private DM-topic sends fail loud BEFORE transmitting with the exact Hermes refuse error";
				if (r.createdForSendNeverFailsLoud !== true)
					return "telegram_dm_topic_created_for_send marks non-private lanes — normal thread routing, no refuse";
				return null;
			},
		),
		mk(
			"tg.wire-arg-whitelist",
			"telegram tg2-5/tg2-7/tg2-9: the sendMessage payload WHITELISTS built args (+reply_markup) — input-namespace metadata (notify, expect_edits, gateway_session_key, final, replyToOverride, _interim_send, raw thread_id strings) can never leak; chat_id ships normalized (numeric int / trimmed @username); disable_link_previews extra gates link_preview_options={is_disabled:true} (send :5397 exact-fields parity)",
			() => f().wireArgWhitelist(),
			(r) => {
				if (r.inputNamespaceKeysNeverLeak !== true)
					return "input-namespace metadata keys must NEVER appear in the transmitted arg set";
				if (r.builtArgsStillShip !== true)
					return "built args (chat_id/text/parse_mode/message_thread_id/disable_notification) must still ship";
				if (r.replyMarkupSurvives !== true)
					return "explicit reply_markup rides the whitelist (approval keyboards)";
				if (r.numericChatIdIsInt !== true || r.usernameChatIdPreserved !== true)
					return "normalizeTelegramChatId: numeric ids ship as ints, @username strings pass through trimmed";
				if (r.linkPreviewKwargsGated !== true)
					return "disable_link_previews extra must attach link_preview_options={is_disabled:true} (tg2-9 :1945)";
				if (r.linkPreviewOffByDefault !== true)
					return "without the extra NO link_preview_options kwarg may ship (default-off parity)";
				return null;
			},
		),
		mk(
			"tg.rich-extras-lane",
			"telegram tg2-6: Bot API 10.1 rich extras behind the wireRich seam — default worlds NEVER attempt rich; rich_messages opt-in drives sendRichMessage {chat_id, rich_message, reply_parameters…} on eligible RAW markdown; ineligible content skips silently; expect_edits skips; capability errors latch ONCE; transient failures never legacy-resend; sendRichMessageDraft needs BOTH extras with its own latch, falling back to REAL sendMessageDraft calls (tg-13); eligible finalize edits carry rich_message (:2229/:2336/:2430)",
			() => f().richExtrasLane(),
			(r) => {
				if (r.defaultOffNeverAttemptsRich !== true)
					return "with no extras configured, zero rich attempts and legacy delivery stands";
				if (r.extraOnSendsRichPayload !== true)
					return "rich_messages opt-in must sendRichMessage with normalized chat_id, rich_message payload, reply_parameters anchor, notification kwargs";
				if (r.ineligibleContentSkipsSilently !== true)
					return "ineligible content skips tier 1 silently WITHOUT latching";
				if (r.expectEditsSkipsRich !== true)
					return "expect_edits previews skip the rich tier (_should_attempt_rich parity)";
				if (r.capabilityErrorLatchesOnce !== true)
					return "capability errors latch rich off exactly once; later sends skip the endpoint but still deliver";
				if (r.transientRichNeverLegacyResent !== true)
					return "transient rich failures surface as failed results with NO legacy resend (duplicate risk)";
				if (r.richDraftGatedOnBothExtras !== true)
					return "sendRichMessageDraft requires BOTH rich_messages and rich_drafts; otherwise legacy drafts stand";
				if (r.richDraftCapabilityLatches !== true)
					return "draft capability failures fall back this frame and latch drafts-only";
				if (r.richFinalizeEditApplied !== true)
					return "eligible finalize edits ride editMessageText with a rich_message param (_try_edit_rich :2336)";
				return null;
			},
		),
		mk(
			"tg.media-dm-topic-retry",
			"telegram tg2-12: media transmissions wrap the DM-topic anchor retry ladder (_send_with_dm_topic_reply_anchor_retry :1753) — dead anchors retry once WITHOUT reply/topic routing, stale topic markers retry AND prune the cached binding, non-fallback lanes and non-retry BadRequests surface as-is",
			() => f().mediaDmTopicRetry(),
			(r) => {
				if (r.deadReplyAnchorRetriesWithoutAnchors !== true)
					return "'message to be replied not found' under the fallback flag must retry once without any anchor kwargs";
				if (r.staleTopicRetriedWithBindingPrune !== true)
					return "topic/thread rejections must retry un-routed and prune the stale binding (next ensure re-creates)";
				if (r.withoutFallbackFlagFailsImmediately !== true)
					return "WITHOUT telegram_dm_topic_reply_fallback there is no retry ladder — the failure surfaces as-is";
				if (r.nonRetryClassSurfacesAsIs !== true)
					return "non-topic BadRequests skip the ladder (photo→document fallback chain owns them)";
				return null;
			},
		),
		mk(
			"tg.long-poll-timeout-default",
			"telegram tg2-8: production long-poll timeout defaults to the PTB baseline 10 s (adapter.py:start_polling passes no override, :2668/:2884)",
			() => f().longPollTimeoutDefault(),
			(r) => {
				if (Number(r.productionTimeoutMs) !== 10_000)
					return `production longPollTimeoutMs must be 10_000 (PTB timeout=10), got ${String(r.productionTimeoutMs)}`;
				return null;
			},
		),
	];
}
