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
			"telegram: raw update-object parsing deltas — message→turn, edited/reaction/callback routed, unwired kinds tolerated, ONE offset space ACKs every kind",
			() => f().updateParsingDeltas(),
			(r) => {
				if (Number(r.textTurns) !== 1)
					return `message update must yield exactly one turn, got ${String(r.textTurns)}`;
				if (Number(r.editedEvents) !== 1)
					return "edited_message must normalize to a platform event (no turn)";
				if (Number(r.reactionEvents) !== 1)
					return "message_reaction must normalize to ONE event";
				if (Number(r.callbackRouted) !== 1)
					return "callback_query must route through the query handler";
				if (r.unwiredIgnored !== true)
					return "channel_post (unwired kind) must be tolerated without becoming a turn";
				if (Number(r.pendingAfterAck) !== 0)
					return "every kind shares ONE offset space — all ACKed, none re-delivered";
				return null;
			},
		),
		mk(
			"tg.edit-send-reconciliation",
			"telegram: edit-vs-send reconciliation through the chokepoint — drafts RAW prefix-stable, mid-stream edits RAW, finalize edits FULL MarkdownV2 (#25710), edit FloodWait never blocks",
			() => f().editVsSendReconciliation(),
			(r) => {
				if (r.draftRawPrefixStable !== true)
					return "native draft frames must ship RAW bytes (§10.2 native-stream rule)";
				if (r.midStreamEditRaw !== true)
					return "mid-stream progressive edits must stay RAW prefix-stable";
				if (r.finalizeConvertedEscaped !== true)
					return "finalize edit must carry FULL MarkdownV2 conversion + escaping (REQUIRES_EDIT_FINALIZE #25710)";
				if (r.editFloodNonBlocking !== true)
					return "edit-site FloodWait must NOT block the caller";
				if (!String(r.editFloodErrorSurface).includes("flood_control"))
					return `edit flood must surface flood_control:<wait> with retryAfter, got ${JSON.stringify(r.editFloodErrorSurface)}`;
				return null;
			},
		),
		mk(
			"tg.callback-roundtrip-64b",
			"telegram: callback_data builder→door-keyboard→callback_query→router→resolver round-trip within 64 bytes; spinner always answered; consumed keyboard stripped; unauthorized ignored",
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
					return "resolved taps strip the host reply markup (consumed state visible)";
				if (r.doubleTapStaleAnswered !== true)
					return "double-tap answered stale, resolved exactly once";
				if (r.unauthorizedNotResolved !== true)
					return "unauthorized clicker answered but NEVER resolved";
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
			"telegram A11: typing variants — action matrix on wire, General-topic thread '1' PRESERVED on typing bubbles, thread-status text carried, transient-failure cooldown suppresses then expires (injected clock), approval-wait pause skips",
			() => f().typingVariants(),
			(r) => {
				if (r.variantActionOnWire !== "upload_photo")
					return `configured action variant must reach sendChatAction, got ${JSON.stringify(r.variantActionOnWire)}`;
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
			"telegram Q17: FloodWait honored per METHOD CLASS — send ladder honors retry_after once and recovers; edit class surfaces flood_control WITHOUT blocking; manifest tiers resolve per op",
			() => f().floodwaitMethodClasses(),
			(r) => {
				if (r.sendRetriedWithRetryAfter !== true)
					return "send-class FloodWait must honor retry_after over the injected clock and recover";
				if (Number(r.sendAttemptsRecorded) < 2)
					return "send ladder records the failed attempt + successful retry";
				if (!String(r.editSurfacedFloodControl).startsWith("flood_control:"))
					return "edit-class long wait surfaces error=flood_control:<wait>";
				if (r.editDidNotBlock !== true)
					return "edit-class waits >5 s must NOT sleep the caller";
				if (r.tiersResolvePerClass !== true)
					return "manifest rate tiers resolve send/edit/typing to their declared classes";
				return null;
			},
		),
	];
}
