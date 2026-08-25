// pi_platforms/matrix/matrix-shape-rows — MATRIX-SHAPE conformance rows
// (Phase-6 census port). These are the SHAPE DELTAS beyond the inherited §3.1
// polling transport family: sync-token exactly-once replay, auth/epoch
// ladders, the ordered intake filter chain, startup grace, mention gating,
// reply-fallback + bang aliases, channel directory + alias overlay (A9), and
// the reaction-ack/typing ride-alongs (A1/A11).
//
// Row bodies run against the REAL engine via makeMatrixShapeFixture() — no
// stubbed return values; each call builds fresh worlds.

import type { ConformanceRow } from "../conformance/rows.js";
import { makeMatrixShapeFixture } from "./matrix-world.js";

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

export const MATRIX_SHAPE_ROW_IDS = [
	"mx.sync-token-exactly-once",
	"mx.auth-and-epoch-ladders",
	"mx.ingress-filter-chain",
	"mx.startup-grace-window",
	"mx.mention-gating-matrix",
	"mx.reply-fallback-and-bang",
	"mx.directory-alias-overlay",
	"mx.reaction-typing-variants",
] as const;

export function makeMatrixShapeRows(): ConformanceRow[] {
	const f = () => makeMatrixShapeFixture();
	return [
		mk(
			"mx.sync-token-exactly-once",
			"matrix sync-token exactly-once: rewinding the committed token replays the same window gap-free; event-id dedup keeps downstream delivery exactly once while fresh events keep flowing",
			() => f().syncTokenExactlyOnce(),
			(r) => {
				if (Number(r.r1TurnCopies) !== 1)
					return `r1 must turn exactly once, got ${String(r.r1TurnCopies)} copies`;
				if (Number(r.r2TurnCopies) !== 1)
					return `r2 must turn exactly once, got ${String(r.r2TurnCopies)} copies`;
				if (Number(r.replayedWindowCount) !== 2)
					return "the rewound window must cover both events";
				if (r.freshAfterRedeliveryDelivered !== true)
					return "fresh events after the redelivery must still deliver";
				return null;
			},
		),
		mk(
			"mx.auth-and-epoch-ladders",
			"matrix auth ladders: m_unknown_token from sync STOPS immediately with a loud fatal and NO retry ladder; M_UNKNOWN_SYNC_TOKEN epoch death recovers via full-state restarts that abandon the dead stream",
			() => f().authLadders(),
			(r) => {
				if (r.unknownTokenFatalImmediately !== true)
					return "m_unknown_token must stop the loop with an immediate fatal";
				if (r.noRetryLadderOnAuthDeath !== true)
					return "auth death must NOT run a retry ladder";
				if (r.epochRecoveredByFullState !== true)
					return "epoch death must recover via full-state restart";
				if (r.postRecoveryStreamLive !== true)
					return "post-recovery stream must be live for fresh events";
				return null;
			},
		),
		mk(
			"mx.ingress-filter-chain",
			"matrix intake filter chain in Hermes order: self/echo (case-normalized, defensive on unresolved own id), appservice bridge senders, m.notice skip, m.replace edits skip, media tolerated, dedup deque bounded",
			() => f().filterChain(),
			(r) => {
				const turns = r.turnTexts as readonly string[] | undefined;
				void turns;
				for (const leg of [
					"selfEchoTurns",
					"caseVariantSelfTurns",
					"bridgeSenderTurns",
				] as const) {
					if (Number(r[leg]) !== 0) return `${leg} must be filtered`;
				}
				if (r.unresolvedOwnIdDefensiveDrop !== true)
					return "unresolved own user id must drop defensively (#15763)";
				for (const leg of [
					"noticeSkipped",
					"editSkipped",
					"mediaTolerated",
					"realTextDelivered",
					"dedupDequeBounded",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				return null;
			},
		),
		mk(
			"mx.startup-grace-window",
			"matrix startup grace: initial-sync backlog older than startup−5s drops, backlog inside the grace is kept, live events pass, and the boundary is exact (strict <)",
			() => f().startupGrace(),
			(r) => {
				for (const leg of [
					"oldBacklogDropped",
					"insideGraceKeptOrHeldThenKept",
					"liveKept",
					"boundaryExact",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				return null;
			},
		),
		mk(
			"mx.mention-gating-matrix",
			"matrix mention gating: MSC3952 m.mentions authoritative, mxid-body fallback strips cleanly, localpart word-boundary mentions but bare words are KEPT, word boundary respected, free rooms + commands + DMs bypass, whitelists silently drop",
			() => f().mentionGating(),
			(r) => {
				for (const leg of [
					"unmentionedChannelDropped",
					"msc3952Authoritative",
					"bodyFallbackStrippedToCleanText",
					"localpartWordBoundaryMentionsButBareWordKept",
					"wordBoundaryRespectedNotMentioned",
					"freeRoomBypass",
					"commandBypass",
					"dmExempt",
					"whitelistSilentlyDrops",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				return null;
			},
		),
		mk(
			"mx.reply-fallback-and-bang",
			"matrix reply-fallback + bang aliases: quoted '> <@author> text' extracted to reply_to_text/author and stripped; !known → /known incl. underscore→hyphen; unknown bangs stay chat text",
			() => f().replyFallbackAndBang(),
			(r) => {
				if (r.replyToTextExtracted !== "what model are you\nsecond line")
					return `quoted text extraction wrong: ${JSON.stringify(r.replyToTextExtracted)}`;
				if (r.authorIdResolved !== "@carol:fake.example")
					return `pill author not resolved: ${JSON.stringify(r.authorIdResolved)}`;
				for (const leg of [
					"bodyStrippedToReplyOnly",
					"nonFallbackUntouched",
					"bangNormalized",
					"underscoreBangNormalized",
					"unknownBangLeftAlone",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				return null;
			},
		),
		mk(
			"mx.directory-alias-overlay",
			"matrix channel directory + alias overlay (A9): display name prefers room_name → canonical_alias → room_id; member_count ≤2 wins DM; explicit name beats stale m.direct flagged as conflict; identity cache honors its TTL",
			() => f().directoryOverlay(),
			(r) => {
				if (r.displayNamePrefersName !== "The Named Room")
					return "explicit name must win display resolution";
				if (r.fallsBackToAlias !== "#alias:fake.example")
					return "alias overlay must back a missing name";
				if (r.fallsBackToRoomId !== "!bare:fake.example")
					return "room id is the last display fallback";
				for (const leg of [
					"memberCountDmWins",
					"explicitNameBeatsStaleDirectConflictFlagged",
					"cacheHitWithinTtl",
					"ttlExpiryResolvesAgain",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				return null;
			},
		),
		mk(
			"mx.reaction-typing-variants",
			"matrix reaction-ack lifecycle (A1: 👀 start → ✅/❌ swap with eyes redaction, cancel clears only) + typing variants (A11: 30s bubbles, 0 stop, M_LIMIT_EXCEEDED retry_after honored once at the typing site)",
			() => f().reactionAckAndTyping(),
			(r) => {
				if (r.startEmoji !== "\u{1F440}")
					return `processing start must set 👀, got ${JSON.stringify(r.startEmoji)}`;
				if (r.successSwappedEmoji !== "\u2705")
					return `success must swap to ✅, got ${JSON.stringify(r.successSwappedEmoji)}`;
				for (const leg of [
					"eyesRedactedOnComplete",
					"cancelClearedEyesOnly",
					"rateLimitHonoredOnceThenRecovers",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				if (Number(r.typingTimeoutMs) !== 30000)
					return "typing bubble timeout must be 30000ms";
				if (Number(r.stopTypingTimeoutMs) !== 0)
					return "stop typing must clear with timeout 0";
				return null;
			},
		),
	];
}
