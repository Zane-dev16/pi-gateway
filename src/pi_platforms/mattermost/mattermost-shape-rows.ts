// pi_platforms/mattermost/mattermost-shape-rows — MATTERMOST-SHAPE
// conformance rows (Phase-6 census port): ws event dedup + markdown dual-path
// (the port's named deltas), mention gating, thread-root discipline,
// reconnect backfill window, and the auth/ladder ride-along.
//
// Row bodies run against the REAL engine via makeMattermostShapeFixture().

import type { ConformanceRow } from "../conformance/rows.js";
import {
	makeMattermostShapeFixture,
	makeRealMattermostFixture,
} from "./mattermost-world.js";

const SHAPE = new Set(["ws"] as const);

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

export const MATTERMOST_SHAPE_ROW_IDS = [
	"mm.ws-event-dedup",
	"mm.dual-path-markdown",
	"mm.mention-gating-matrix",
	"mm.thread-root-discipline",
	"mm.rest-backfill-window",
	"mm.reconnect-auth-ladder",
] as const;

export function makeMattermostShapeRows(): ConformanceRow[] {
	const f = () => makeMattermostShapeFixture();
	// The dual-path markdown scenario IS the family ws row — reuse its REAL
	// fixture body for the named shape delta.
	const dual = () => makeRealMattermostFixture().dualPathMarkdown();
	return [
		mk(
			"mm.ws-event-dedup",
			"mattermost ws event dedup: at-least-once redelivery of a post id turns exactly once while fresh posts still deliver; system posts and own messages filtered",
			() => f().wsEventDedup(),
			(r) => {
				if (Number(r.deliveredOnceIds) !== 1)
					return `the post must turn exactly once, got ${String(r.deliveredOnceIds)}`;
				for (const leg of [
					"duplicateSuppressed",
					"systemPostFiltered",
					"ownPostFiltered",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				return null;
			},
		),
		mk(
			"mm.dual-path-markdown",
			"mattermost markdown dual-path (DEC-034 vendor realization): native edit-stream ships RAW cumulative prefix-stable bytes; REST path preserves native markdown verbatim (bold/link/tables render natively); image markdown strips to URLs; link-preview flag rides TEXT sends only",
			() => dual(),
			(r) => {
				for (const leg of [
					"nativeRawByteExact",
					"nativePrefixStable",
					"restConvertedBold",
					"restConvertedLink",
					"restConvertedTable",
					"linkPreviewOnAllTextSends",
					"linkPreviewAbsentOffTextSends",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				return null;
			},
		),
		mk(
			"mm.mention-gating-matrix",
			"mattermost mention gating: THE require_mention gate runs BEFORE any command detection — unmentioned slash text is dropped; @username/@userid match case-insensitively and strip cleanly ('gi'); free-response channels and DMs bypass; allowed-channels whitelist silently drops",
			() => f().mentionGating(),
			(r) => {
				for (const leg of [
					"unmentionedChannelDropped",
					"usernameMentionStripped",
					"userIdMentionAccepted",
					"caseInsensitiveMatch",
					"freeChannelBypass",
					"unmentionedCommandDroppedAtGate",
					"dmExempt",
					"whitelistSilentlyDrops",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				return null;
			},
		),
		mk(
			"mm.thread-root-discipline",
			"mattermost thread roots: reply_mode=off ignores threads; thread mode carries prospective roots; a REPLY's own root wins via posts/{id} lookup; broken-root notify content falls back FLAT with the warning notice while non-notify keeps failing",
			() => f().threadRootDiscipline(),
			(r) => {
				for (const leg of [
					"replyModeOffIgnoresThreads",
					"threadModeUsesProspectiveRoot",
					"brokenThreadNotifyFallsBackFlat",
					"brokenThreadNonNotifyKeepsFailure",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				if (r.replyRootResolvedViaLookup !== "rootpost1")
					return `reply root must resolve to rootpost1 via posts lookup, got ${JSON.stringify(r.replyRootResolvedViaLookup)}`;
				return null;
			},
		),
		mk(
			"mm.rest-backfill-window",
			"mattermost reconnect REST backfill: posts posted during the disconnect deliver after reconnect across tracked channels; re-fetching already-seen posts stays exactly-once via the dedup shield",
			() => f().backfillWindow(),
			(r) => {
				if (Number(r.missedDuringOutageDelivered) !== 2)
					return "both missed posts must deliver after reconnect";
				for (const leg of [
					"exactlyOnceAcrossOverlap",
					"trackedChannelsHonored",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				return null;
			},
		),
		mk(
			"mm.reconnect-auth-ladder",
			"mattermost reconnect ladder: exponential steps GROW (2s→4s→8s manifest constants) capped at the 60s ceiling; authentication rejection escalates LOUD fatal instead of a silent healthy loop (OOF-156)",
			() => f().reconnectAuthLadder(),
			(r) => {
				for (const leg of [
					"ladderStepsGrow",
					"authRejectedFatalNotSilentLoop",
					"capsAtMaxDelay",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				return null;
			},
		),
	];
}
