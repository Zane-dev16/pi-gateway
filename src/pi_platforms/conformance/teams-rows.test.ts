// CONFORMANCE WIRING — the Microsoft Teams census port vs the executable
// 04 §8 matrix (DEC-002 gate applies to every new platform).
//
//   1. ALL applicable SHARED rows pass for shape="webhook" against the REAL
//      kit-built TeamsSubject. Applicability is COMPUTED from capability
//      data: the streaming family applies only when draft streaming holds —
//      Teams has no native draft lanes, so those three rows are excluded BY
//      THE PROBE, never by a hardcoded skip.
//   2. The INHERITED webhook transport rows run over the REAL adapter probes,
//      with ONE SHAPE DELTA (roadmap §Phase 6 heuristic 2): Teams is
//      ASYNC-CAPABLE (supports_async_delivery=True — proactive sends are
//      real), so the flags row asserts the pairing FROM THE DECLARED DATA
//      instead of the stateless both-false clause. Flip the capability and
//      the strict pairing requirement re-includes — computed, never skipped.
//   3. Fresh Teams shape-delta rows execute through the real engine fixture:
//      activity-ingress pipeline (self filter / @mention strip / TTL dedupe),
//      attachment classification precedence, approval-card lifecycle with
//      DEFAULT-DENY clicker authz, and the Bot Framework outbound REST shapes
//      (token dance, raw-markdown textFormat, threaded-reply digit gate with
//      flat fallback, service-host allowlist, conversation-id charset).
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: an inflating mutant fixture fails ITS OWN named row.

import { writeFileSync } from "node:fs";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { makeWebhookRows } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { makeTeamsSubject, type TeamsSubject } from "../teams/teams-subject.js";
import { makeTeamsFixture, type TeamsFixture } from "../teams/teams-fixture.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeTeamsSubject({
		wire: new FakePlatformWire(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
	});
}

/** §8 streaming family — applicable ONLY when draft streaming is supported. */
const STREAMING_ROW_IDS: readonly string[] = [
	"streaming.prefix-mutation-detected",
	"streaming.seal-discipline",
	"streaming.failed-seal-still-delivers",
];

function computeApplicability(): {
	streamsSupported: boolean;
	excludedIds: string[];
} {
	const probe = makeSubject();
	const streamsSupported =
		probe.adapter.supportsDraftStreaming() === true &&
		probe.adapter.supportsAsyncDelivery === true;
	return { streamsSupported, excludedIds: [...STREAMING_ROW_IDS] };
}

/**
 * SHAPE DELTA of the inherited flags row: the stateless both-false clause is
 * required IFF the declared capability data is stateless. Teams declares
 * supports_async_delivery=True (proactive ConversationReference sends), so the
 * probe asserts async-capable posture + DEC-017 completeness instead. A
 * capability flip re-includes the strict pairing — no hardcoded skip.
 */
function makeTeamsFlagsRow(subject: TeamsSubject): ConformanceRow {
	const id = "transport.webhook.flags-and-trust-boundary";
	const title =
		"webhook: flag pairing matches DECLARED capability data (stateless pairing enforced iff async-capable off); DEC-017 trust boundary complete";
	const run = async () => {
		try {
			const probe = subject.flagsAndTrustProbe();
			if (!probe.pairingSatisfied) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["webhook"]) as Set<"webhook">,
					detail: "flag pairing contradicts the declared capability data",
				};
			}
			if (!probe.trustBoundaryComplete) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["webhook"]) as Set<"webhook">,
					detail: "trust boundary manifest incomplete",
				};
			}
			if (!probe.bearerAuthDeclared) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["webhook"]) as Set<"webhook">,
					detail: "delegated inbound-Bearer boundary not declared",
				};
			}
			if (subject.adapter.wakeLane !== "forged-event") {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["webhook"]) as Set<"webhook">,
					detail: "async-capable shape must declare the forged-event wake lane",
				};
			}
			return {
				id,
				title,
				pass: true,
				shapes: new Set(["webhook"]) as Set<"webhook">,
			};
		} catch (err) {
			return {
				id,
				title,
				pass: false,
				shapes: new Set(["webhook"]) as Set<"webhook">,
				detail: err instanceof Error ? err.message : String(err),
			};
		}
	};
	return { id, title, shapes: new Set(["webhook"]), run };
}

// ── Teams shape-delta rows (executed over the REAL engine fixture) ──────────

function teamsDeltaRows(newFixture: () => TeamsFixture): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: TeamsFixture) => Promise<void>,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["webhook"]),
		run: async () => {
			const fx = newFixture();
			try {
				await body(fx);
				return { id, title, pass: true, shapes: new Set(["webhook"]) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["webhook"]),
					detail: err instanceof Error ? err.message : String(err),
				};
			} finally {
				fx.dispose();
			}
		},
	});

	return [
		mk(
			"transport.teams.activity-ingress-pipeline",
			"teams: _on_message pipeline — bot-id self filter skips silently; <at>@mention</at> stripped; TTL dedupe kills redelivery and EXPIRES on the injected clock; conversation_type maps to dm/group/channel; activity POSTs ack 200 with EMPTY bodies (accepted and skipped alike); non-message activities (conversationUpdate/typing pings) are IGNORED per the SDK's per-activity handler registration; source.chatName rides the CONVERSATION (sender name is the DM-only fallback)",
			async (fx) => {
				// Self-authored echo never becomes a turn.
				const self = await fx.postMessageActivity(
					fx.messageActivity({
						id: "act-self",
						text: "echo",
						botAuthored: true,
						conversationType: "personal",
					}),
				);
				expect(self.status).toBe(200);
				// webhook-41: skipped POSTs ack 200 with an EMPTY body too —
				// skip reasons live in counters, never on the wire.
				expect(self.json).toBeUndefined();
				expect(fx.adapter.counters.selfMessagesSkipped).toBe(1);
				expect(fx.adapter.turnLog).toEqual([]);

				// @mention strip + chat-type mapping (channel).
				const mentioned = fx.messageActivity({
					id: "act-m1",
					text: "<at>Hermes</at> deploy the gateway",
				});
				const mentionedResp = await fx.postMessageActivity(mentioned);
				// webhook-41: accepted activity POSTs ack 200 with NO body
				// (_AiohttpBridgeAdapter answers web.Response(status) bare).
				expect(mentionedResp.json).toBeUndefined();
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(fx.adapter.turnLog).toEqual(["deploy the gateway"]);

				// Redelivery inside the TTL window dies on the deduplicator…
				await fx.postMessageActivity(mentioned);
				expect(fx.adapter.counters.duplicatesSkipped).toBe(1);
				expect(fx.adapter.turnLog.length).toBe(1);

				// …but EXPIRES once the injected clock passes 300 s (TTL parity of
				// helpers.py MessageDeduplicator — not a permanent blocklist).
				fx.advance(301_000);
				await fx.postMessageActivity(mentioned);
				expect(fx.adapter.counters.duplicatesSkipped).toBe(1); // unchanged
				expect(fx.adapter.turnLog.length).toBe(2);

				// groupChat → group; unknown → dm fallback.
				const group = await fx.postMessageActivity(
					fx.messageActivity({
						id: "act-g1",
						text: "group hello",
						conversationType: "groupChat",
					}),
				);
				expect(group.status).toBe(200);
				const personal = await fx.postMessageActivity(
					fx.messageActivity({
						id: "act-p1",
						text: "dm hello",
						conversationType: "personal",
					}),
				);
				expect(personal.status).toBe(200);
				await new Promise<void>((r) => setTimeout(r, 30));
				expect(fx.adapter.turnLog).toContain("group hello");
				expect(fx.adapter.turnLog).toContain("dm hello");

				// webhook-39: source.chatName is the CONVERSATION's name — a channel
				// session is never named after whoever spoke.
				const namedChannel = await fx.postMessageActivity(
					fx.messageActivity({
						id: "act-name",
						text: "who goes there",
						fromName: "Alice Sender",
						conversationName: "Deploy Channel",
					}),
				);
				expect(namedChannel.status).toBe(200);
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(fx.adapter.sourceChatNames.at(-1)).toBe("Deploy Channel");

				// Nameless channel: chatName stays EMPTY (the sender name never
				// leaks into it).
				const unnamedChannel = await fx.postMessageActivity(
					fx.messageActivity({
						id: "act-chan-unnamed",
						text: "unnamed channel",
						fromName: "Carol Sender",
					}),
				);
				expect(unnamedChannel.status).toBe(200);
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(fx.adapter.sourceChatNames.at(-1)).toBe("");

				// DM fallback ONLY: a nameless personal conversation borrows the
				// sender's name (Hermes builds chat_name and user_name apart).
				const namedDm = await fx.postMessageActivity(
					fx.messageActivity({
						id: "act-dmname",
						text: "dm naming",
						fromName: "Bob Sender",
						conversationType: "personal",
					}),
				);
				expect(namedDm.status).toBe(200);
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(fx.adapter.sourceChatNames.at(-1)).toBe("Bob Sender");

				// Malformed bodies answer 400 (never dispatched).
				const badJson = await fx.postActivityBody("{nope");
				expect(badJson.status).toBe(400);
				const scalar = await fx.postActivityBody("42");
				expect(scalar.status).toBe(400);

				// Oversized bodies answer 413 pre-parse (client_max_size posture).
				const big = Buffer.alloc(1_048_577, 0x20);
				const oversized = await fx.postActivityBody(big);
				expect(oversized.status).toBe(413);

				// Non-message activities NEVER become pipeline traffic (the SDK
				// registers on_message for MessageActivity ONLY @850-859):
				// conversationUpdate/typing pings ack 200 EMPTY and are dropped.
				const turnsBeforePings = fx.adapter.turnLog.length;
				const acceptedBeforePings = fx.adapter.counters.activitiesAccepted;
				const convUpdate = await fx.postActivityBody(
					JSON.stringify({
						type: "conversationUpdate",
						membersAdded: [{ id: "bot-id" }],
						conversation: {
							id: "19:meeting123@thread.tacv2",
							conversation_type: "channel",
						},
					}),
				);
				expect(convUpdate.status).toBe(200);
				expect(convUpdate.json).toBeUndefined();
				const typingPing = await fx.postActivityBody(
					JSON.stringify({
						type: "typing",
						from: { id: "user-8" },
						conversation: {
							id: "19:meeting123@thread.tacv2",
							conversation_type: "channel",
						},
					}),
				);
				expect(typingPing.status).toBe(200);
				expect(fx.adapter.turnLog.length).toBe(turnsBeforePings);
				expect(fx.adapter.counters.activitiesAccepted).toBe(
					acceptedBeforePings,
				);
				expect(fx.adapter.counters.duplicatesSkipped).toBe(1);
			},
		),
		mk(
			"transport.teams.attachment-classification",
			"teams: mirrored text/html skipped; card attachments skipped; fileDownload.info documents cached through the SSRF-guarded seam; mixed attachments classify DOCUMENT > PHOTO > VIDEO > AUDIO",
			async (fx) => {
				// Mirrored body + card payload only ⇒ text message, zero fetches.
				const mirrorsOnly = await fx.postMessageActivity(
					fx.messageActivity({
						id: "att-1",
						text: "just text",
						attachments: [
							{ contentType: "text/html", content: "<b>just text</b>" },
							{
								contentType: "application/vnd.microsoft.card.adaptive",
								content: {},
							},
						],
					}),
				);
				expect(mirrorsOnly.status).toBe(200);
				expect(fx.bf.attachmentFetches).toHaveLength(0);
				await new Promise<void>((r) => setTimeout(r, 15));
				expect(fx.adapter.turnLog).toContain("just text");

				// fileDownload.info document: fetched from the downloadUrl, cached
				// into the mkdtemp media dir as DOCUMENT kind.
				const doc = await fx.postMessageActivity(
					fx.messageActivity({
						id: "att-2",
						text: "",
						attachments: [
							{
								contentType:
									"application/vnd.microsoft.teams.file.download.info",
								name: "report.pdf",
								content: {
									downloadUrl: "https://sharepoint.example/dl/report.pdf",
									fileType: "pdf",
								},
							},
						],
					}),
				);
				expect(doc.status).toBe(200);
				expect(fx.bf.attachmentFetches).toEqual([
					"https://sharepoint.example/dl/report.pdf",
				]);
				expect(fx.adapter.counters.attachmentsCached).toBe(1);
				await new Promise<void>((r) => setTimeout(r, 15));
				expect(fx.adapter.dispatchedMediaKinds()).toContain("document");

				// Mixed image + document ⇒ DOCUMENT wins (precedence contract).
				await fx.postMessageActivity(
					fx.messageActivity({
						id: "att-3",
						text: "mixed",
						attachments: [
							{
								contentType: "image/png",
								contentUrl: "https://cdn.example/pic.png",
							},
							{
								contentType:
									"application/vnd.microsoft.teams.file.download.info",
								name: "data.csv",
								content: {
									downloadUrl: "https://sharepoint.example/dl/data.csv",
									fileType: "csv",
								},
							},
						],
					}),
				);
				await new Promise<void>((r) => setTimeout(r, 15));
				const kinds = fx.adapter.dispatchedMediaKinds();
				expect(kinds[kinds.length - 1]).toBe("document"); // document > photo

				// Non-http(s) URLs refuse BEFORE any fetch (SSRF guard seam).
				await fx.postMessageActivity(
					fx.messageActivity({
						id: "att-4",
						text: "sneaky",
						attachments: [
							{ contentType: "image/png", contentUrl: "file:///etc/passwd" },
						],
					}),
				);
				expect(
					fx.bf.attachmentFetches.every((u) => u.startsWith("https://")),
				).toBe(true);
			},
		),
		mk(
			"transport.teams.approval-card-lifecycle",
			"teams: card actions DEFAULT-DENY (unconfigured denies w/ guidance; allowlist admits named clicker; '*' admits all); card button data carries NO approval_id — clicks resolve by SESSION KEY through THE kit router; resolved taps echo the FULL confirmation card (title/cmd fence/Reason/bold label); double-tap answers the explicit expiry CARD; AdaptiveCard invoke activities route through handleActivityPost",
			async (_fx) => {
				const sessionKey = "sk-approval-1";

				// Unconfigured authz: guidance denial, nothing resolved.
				const unconfigured = makeTeamsFixture(); // no allowedUsers seed
				const deniedGuidance = await unconfigured.adapter.handleCardAction(
					{ hermes_action: "approve_once", session_key: sessionKey },
					{ aadObjectId: "clicker-A" },
				);
				expect(deniedGuidance.value).toBe(
					"⛔ Approval buttons require TEAMS_ALLOWED_USERS to be configured.",
				);
				unconfigured.dispose();

				// Allowlisted clicker resolves ONCE; second tap expires.
				const gated = makeTeamsFixture({ allowedUsers: ["clicker-B"] });
				const sendResult = await gated.adapter.sendApprovalCard(
					"19:chat@thread.tacv2",
					"rm -rf /tmp/stale",
					sessionKey,
					"dangerous cleanup",
				);
				expect(sendResult.success).toBe(true);

				// teams-1 btn_data_base parity (@1198-1210): button data carries NO
				// approval_id — exactly {session_key, cmd, desc} + hermes_action.
				// The sequential id bookkeeping stays ADAPTER-SIDE (one pending
				// registered in the approvals store).
				const postedCard = gated.bf.activities.at(-1);
				const cardAttachment = (
					(postedCard?.activity["attachments"] ?? []) as Array<
						Record<string, unknown>
					>
				)[0];
				const cardContent = cardAttachment?.["content"] as Record<
					string,
					unknown
				>;
				const cardActions = cardContent["actions"] as Array<
					Record<string, unknown>
				>;
				expect(cardActions).toHaveLength(4); // smart_denied=false ⇒ 4 buttons
				for (const action of cardActions) {
					expect(
						Object.keys(action["data"] as Record<string, unknown>).sort(),
					).toEqual(["cmd", "desc", "hermes_action", "session_key"]);
				}
				expect(gated.adapter.pendingApprovalIds()).toHaveLength(1);

				// HERMES-SHAPED click (approval_id absent-on-wire) resolves by
				// session key through THE kit router.
				const approved = await gated.adapter.handleCardAction(
					{
						hermes_action: "approve_once",
						session_key: sessionKey,
						cmd: "rm -rf /tmp/stale",
						desc: "dangerous cleanup",
					},
					{ aadObjectId: "clicker-B" },
				);
				// teams-4: the FULL confirmation card — title, fenced cmd, Reason
				// line, bold label (@1160-1181), never a label-only string.
				expect(approved.kind).toBe("card");
				if (approved.kind === "card") {
					expect(approved.value).toEqual({
						type: "AdaptiveCard",
						version: "1.4",
						body: [
							{
								type: "TextBlock",
								text: "⚠️ Command Approval Required",
								wrap: true,
								weight: "Bolder",
							},
							{
								type: "TextBlock",
								text: "```\nrm -rf /tmp/stale\n```",
								wrap: true,
							},
							{
								type: "TextBlock",
								text: "Reason: dangerous cleanup",
								wrap: true,
								isSubtle: true,
							},
							{
								type: "TextBlock",
								text: "✅ Allowed (once)",
								wrap: true,
								weight: "Bolder",
							},
						],
					});
				}
				expect(gated.adapter.counters.cardActionsResolved).toBe(1);

				// Double-tap: consumed pending ⇒ explicit expiry CARD.
				const replay = await gated.adapter.handleCardAction(
					{ hermes_action: "deny", session_key: sessionKey },
					{ aadObjectId: "clicker-B" },
				);
				expect(replay).toEqual({
					statusCode: 200,
					kind: "card",
					value: {
						type: "AdaptiveCard",
						version: "1.4",
						body: [
							{
								type: "TextBlock",
								text: "⚠️ Approval already resolved or expired.",
								wrap: true,
							},
						],
					},
				});

				// Never-registered session key answers the SAME expiry card
				// (has_blocking_approval parity).
				const stale = await gated.adapter.handleCardAction(
					{ hermes_action: "deny", session_key: "sk-never-registered" },
					{ aadObjectId: "clicker-B" },
				);
				expect(stale.kind).toBe("card");

				// Unauthorized clicker (not on the list): ⛔ Not authorized.
				const stranger = await gated.adapter.handleCardAction(
					{ hermes_action: "deny", session_key: sessionKey },
					{ aadObjectId: "clicker-EVIL" },
				);
				expect(stranger.value).toBe("⛔ Not authorized.");

				// Wildcard allowlist admits everyone; deny with no cmd/desc on the
				// click data renders the bold label ONLY (Hermes body build parity).
				const wildcard = makeTeamsFixture({ allowedUsers: ["*"] });
				wildcard.adapter.approvals.register(424242, "sk-any");
				const anyClicker = await wildcard.adapter.handleCardAction(
					{ hermes_action: "deny", session_key: "sk-any" },
					{ aadObjectId: "whoever" },
				);
				expect(anyClicker.kind).toBe("card");
				if (anyClicker.kind === "card") {
					expect(anyClicker.value.body).toEqual([
						{
							type: "TextBlock",
							text: "❌ Denied",
							wrap: true,
							weight: "Bolder",
						},
					]);
				}
				wildcard.dispose();

				// Unknown hermes_action answers "Unknown action." (both shapes).
				const gated2 = makeTeamsFixture({ allowedUsers: ["c1"] });
				const unknownAction = await gated2.adapter.handleCardAction(
					{ hermes_action: "explode", session_key: sessionKey },
					{ aadObjectId: "c1" },
				);
				expect(unknownAction.value).toBe("Unknown action.");
				const missingFields = await gated2.adapter.handleCardAction({}, {});
				expect(missingFields.value).toBe("Unknown action.");
				gated2.dispose();

				// teams-3: AdaptiveCard invoke activities route THROUGH
				// handleActivityPost to the card-action handler (SDK on_card_action
				// registration parity) — HTTP 200 WITH the modeled InvokeResponse
				// body, resolution observed, NEVER dispatched as a turn.
				const invoked = makeTeamsFixture({ allowedUsers: ["clicker-C"] });
				const cardSend = await invoked.adapter.sendApprovalCard(
					"19:chat@thread.tacv2",
					"systemctl restart gateway",
					"sk-invoke-1",
					"restart service",
				);
				expect(cardSend.success).toBe(true);
				const turnsBeforeInvoke = invoked.adapter.turnLog.length;
				const acceptedBeforeInvoke =
					invoked.adapter.counters.activitiesAccepted;
				const invokeResp = await invoked.postActivityBody(
					JSON.stringify({
						type: "invoke",
						name: "adaptiveCard/action",
						from: { id: "clicker-C" },
						conversation: {
							id: "19:chat@thread.tacv2",
							conversation_type: "personal",
						},
						value: {
							action: {
								type: "Action.Execute",
								verb: "hermes_approve",
								data: {
									hermes_action: "approve_once",
									session_key: "sk-invoke-1",
									cmd: "systemctl restart gateway",
									desc: "restart service",
								},
							},
						},
					}),
				);
				expect(invokeResp.status).toBe(200);
				expect(invokeResp.json).toEqual({
					value: {
						type: "AdaptiveCard",
						version: "1.4",
						body: [
							{
								type: "TextBlock",
								text: "⚠️ Command Approval Required",
								wrap: true,
								weight: "Bolder",
							},
							{
								type: "TextBlock",
								text: "```\nsystemctl restart gateway\n```",
								wrap: true,
							},
							{
								type: "TextBlock",
								text: "Reason: restart service",
								wrap: true,
								isSubtle: true,
							},
							{
								type: "TextBlock",
								text: "✅ Allowed (once)",
								wrap: true,
								weight: "Bolder",
							},
						],
					},
				});
				expect(invoked.adapter.counters.cardActionsResolved).toBe(1);
				expect(invoked.adapter.turnLog.length).toBe(turnsBeforeInvoke);
				expect(invoked.adapter.counters.activitiesAccepted).toBe(
					acceptedBeforeInvoke,
				);
				invoked.dispose();
			},
		),
		mk(
			"transport.teams.outbound-rest-shapes",
			'teams: token dance precedes every activity POST; activities ship RAW markdown (textFormat) byte-exact; threaded replies ONLY for digit reply_to≠0 with FLAT fallback on scripted 400 AND on THROWN transport errors; typing ships a REAL {type:"typing"} activity POST (errors swallowed); per-kind default media contentTypes (image/png, video/mp4, audio/mpeg, application/octet-stream); service-host allowlist + conv-id charset refuse pre-wire',
			async (fx) => {
				// Flat send shape: token first, then {"type":"message","text":…,
				// "textFormat":"markdown"}.
				const sent = await fx.adapter.send(
					"19:chat@thread.tacv2",
					"**bold** and `code`",
				);
				expect(sent.success).toBe(true);
				expect(fx.bf.tokenRequests).toHaveLength(1);
				expect(fx.bf.tokenRequests[0]?.scope).toBe(
					"https://api.botframework.com/.default",
				);
				const flat = fx.bf.textSendsOf()[0];
				expect(flat?.activity["text"]).toBe("**bold** and `code`"); // RAW bytes
				expect(flat?.activity["textFormat"]).toBe("markdown");
				expect(flat?.kind).toBe("send");

				// Digit reply_to threads; "0" and non-digits stay flat.
				await fx.adapter.send("19:chat@thread.tacv2", "threaded", undefined, {
					reply_to_message_id: "1777",
				} as never);
				const threaded = fx.bf.activities.at(-1);
				expect(threaded?.kind).toBe("reply");

				await fx.adapter.send("19:chat@thread.tacv2", "zero-gate", undefined, {
					reply_to_message_id: "0",
				} as never);
				expect(fx.bf.activities.at(-1)?.kind).toBe("send");

				// Scripted threaded 400 (group-chat shape) falls back to FLAT send
				// and still delivers.
				fx.bf.scriptReplyFail({ status: 400, json: {} });
				const fellBack = await fx.adapter.send(
					"19:chat@thread.tacv2",
					"fallback please",
					undefined,
					{ reply_to_message_id: "1888" } as never,
				);
				expect(fellBack.success).toBe(true);
				const lastTwo = fx.bf.activities.slice(-2);
				expect(lastTwo[0]?.kind).toBe("reply"); // attempted thread failed
				expect(lastTwo[1]?.kind).toBe("send"); // flat fallback delivered
				expect(lastTwo[1]?.activity["text"]).toBe("fallback please");

				// webhook-40: a THROWN transport error on the threaded lane takes
				// the SAME flat fallback — the chunk is never skipped just because
				// postReply raised instead of answering non-200.
				fx.bf.scriptReplyThrow(new Error("socket hang up"));
				const threwThenFellBack = await fx.adapter.send(
					"19:chat@thread.tacv2",
					"throw fallback please",
					undefined,
					{ reply_to_message_id: "1999" } as never,
				);
				expect(threwThenFellBack.success).toBe(true);
				expect(fx.bf.activities.at(-1)?.kind).toBe("send");
				expect(fx.bf.activities.at(-1)?.activity["text"]).toBe(
					"throw fallback please",
				);

				// Both lanes down ⇒ genuine failure surfaces (outer-catch parity:
				// Hermes returns SendResult(success=false) only when flat ALSO fails).
				fx.bf.scriptReplyThrow(new Error("threaded lane down"));
				fx.bf.scriptActivityThrow(new Error("flat lane down"));
				const bothDown = await fx.adapter.send(
					"19:chat@thread.tacv2",
					"doomed chunk",
					undefined,
					{ reply_to_message_id: "2001" } as never,
				);
				expect(bothDown.success).toBe(false);
				expect(bothDown.retryable).toBe(true);

				// Token failure surfaces the source's error shape, retryable.
				fx.bf.scriptToken({ status: 401, json: { error: "invalid_client" } });
				const tokenFail = await fx.adapter.send("19:chat@thread.tacv2", "nope");
				expect(tokenFail.success).toBe(false);
				expect(tokenFail.retryable).toBe(true);
				expect(tokenFail.error?.includes("token request failed (401)")).toBe(
					true,
				);

				// Typing is best-effort polish: a REAL Typing activity POST — the
				// {type:"typing"} body rides the wire (@1277-1283); errors swallowed.
				await fx.adapter.sendTyping("19:chat@thread.tacv2");
				expect(fx.bf.typingActivities).toHaveLength(1);
				expect(fx.bf.typingActivities[0]?.conversationId).toBe(
					"19:chat@thread.tacv2",
				);
				expect(fx.bf.typingActivities[0]?.activity).toEqual({
					type: "typing",
				});

				// Service-host allowlist refuses pre-wire (SSRF posture).
				const offAllowlist = makeTeamsFixture({
					serviceUrl: "https://evil.example/teams/",
				});
				const refusedUrl = await offAllowlist.adapter.send(
					"19:chat@thread.tacv2",
					"x",
				);
				expect(refusedUrl.success).toBe(false);
				expect(refusedUrl.error?.includes("allowlist")).toBe(true);
				expect(offAllowlist.bf.activities).toHaveLength(0);
				offAllowlist.dispose();

				// Conversation-id charset refuses pre-wire.
				const hostileChat = await fx.adapter.send("bad/id!escape", "x");
				expect(hostileChat.success).toBe(false);
				expect(
					hostileChat.error?.includes(
						"outside the Bot Framework conversation ID set",
					),
				).toBe(true);

				// teams-6 per-kind default contentTypes (@1335/@1366/@1383/@1400):
				// extension-less sources fall through guessMime to the KIND default.
				await fx.adapter.sendImage("19:chat@thread.tacv2", {
					url: "https://cdn.example/pic",
				});
				await fx.adapter.sendVideo("19:chat@thread.tacv2", {
					url: "https://cdn.example/clip",
				});
				await fx.adapter.sendVoice("19:chat@thread.tacv2", {
					url: "https://cdn.example/note",
				});
				await fx.adapter.sendDocument("19:chat@thread.tacv2", {
					url: "https://cdn.example/blob",
				});
				const mediaPosts = fx.bf.activities.slice(-4);
				expect(
					mediaPosts.map(
						(p) =>
							((
								p.activity["attachments"] as Array<Record<string, unknown>>
							)[0] ?? {})["contentType"],
					),
				).toEqual([
					"image/png",
					"video/mp4",
					"audio/mpeg",
					"application/octet-stream",
				]);

				// Extension-derived mime still WINS over the per-kind default.
				await fx.adapter.sendVideo("19:chat@thread.tacv2", {
					url: "https://cdn.example/movie.mov",
				});
				const movPost = fx.bf.activities.at(-1);
				expect(
					((
						movPost?.activity["attachments"] as Array<Record<string, unknown>>
					)[0] ?? {})["contentType"],
				).toBe("video/quicktime");

				// Local-path variant rides the SAME default (data URI carries it).
				writeFileSync(join(fx.mediaDir, "voice-note"), Buffer.from("abc"));
				const voiceLocal = await fx.adapter.sendVoice("19:chat@thread.tacv2", {
					path: join(fx.mediaDir, "voice-note"),
				});
				expect(voiceLocal.success).toBe(true);
				const voiceAttachment =
					(
						fx.bf.activities.at(-1)?.activity["attachments"] as Array<
							Record<string, unknown>
						>
					)[0] ?? {};
				expect(voiceAttachment["contentType"]).toBe("audio/mpeg");
				expect(String(voiceAttachment["contentUrl"])).toMatch(
					/^data:audio\/mpeg;base64,/,
				);

				// Chunking at the native budget: 28 KB budget splits long content
				// into multiple activity POSTs, each markdown-shaped.
				const longText = "chunk ".repeat(6000); // 36k chars > 28000 budget
				const chunked = await fx.adapter.send("19:chat@thread.tacv2", longText);
				expect(chunked.success).toBe(true);
				const chunkPosts = fx.bf
					.textSendsOf()
					.filter((a) => String(a.activity["text"]).startsWith("chunk "));
				expect(chunkPosts.length).toBeGreaterThan(1);
				for (const post of chunkPosts) {
					expect(post.activity["textFormat"]).toBe("markdown");
				}
			},
		),
	];
}

describe("conformance suite — teams census port (shape: webhook)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff no native drafts)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false);
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("passes EVERY applicable shared row against the teams subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: "teams",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it("passes the INHERITED webhook transport rows over the REAL adapter (computed flags variant)", async () => {
		const subject = makeSubject() as TeamsSubject;

		const fx = makeTeamsFixture();
		try {
			subject.adapter.holdTurns(true);
			const startedAt = Date.now();
			const resp = await fx.postMessageActivity(
				fx.messageActivity({ id: "bw-act", text: "bounded window" }),
			);
			const elapsed = Date.now() - startedAt;
			expect(resp.status).toBe(200); // acked FAST even with the turn held
			subject.adapter.holdTurns(false);

			const flagsRow = makeTeamsFlagsRow(subject);
			// Bounded-window leg comes from the family factory; the flags leg is
			// the COMPUTED teams variant (shape delta documented above).
			const boundedRow = makeWebhookRows({
				flagsAndTrust: async () => ({
					interactiveResumeFalse: true,
					supportsAsyncDeliveryFalse: true,
					trustBoundaryComplete: true,
				}),
				boundedWindowAnswer: async () => ({
					answeredWithinWindowMs: elapsed,
					windowCapMs: 5_000,
				}),
			}).find((r) => r.id === "transport.webhook.bounded-window-answer");
			const rows = [flagsRow, boundedRow as ConformanceRow];
			const report = await runConformanceSuite({
				subjectName: "teams-webhook-shape",
				shape: "webhook",
				rows,
				suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
			});
			if (report.failed > 0) console.error(formatReport(report));
			expect(report.failed).toBe(0);
			expect(report.deferred).toEqual([]);
		} finally {
			fx.dispose();
		}
	});

	it("passes ALL FOUR Teams shape-delta rows through the real engine fixture", async () => {
		const rows = teamsDeltaRows(() => makeTeamsFixture());
		expect(rows.map((r) => r.id)).toEqual([
			"transport.teams.activity-ingress-pipeline",
			"transport.teams.attachment-classification",
			"transport.teams.approval-card-lifecycle",
			"transport.teams.outbound-rest-shapes",
		]);
		const report = await runConformanceSuite({
			subjectName: "teams-deltas",
			shape: "webhook",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	});

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));

		const subject = makeSubject() as TeamsSubject;
		const flagsRow = makeTeamsFlagsRow(subject);
		const boundedRow = makeWebhookRows({
			flagsAndTrust: async () => ({
				interactiveResumeFalse: true,
				supportsAsyncDeliveryFalse: true,
				trustBoundaryComplete: true,
			}),
			boundedWindowAnswer: async () => ({
				answeredWithinWindowMs: 12,
				windowCapMs: 5_000,
			}),
		}).find(
			(r) => r.id === "transport.webhook.bounded-window-answer",
		) as ConformanceRow;
		const deltas = teamsDeltaRows(() => makeTeamsFixture());

		const transport = [flagsRow, boundedRow];
		const report = await runConformanceSuite({
			subjectName: "teams-full",
			shape: "webhook",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 30_000);

	it("the gate DETECTS violations: a wire-shape-defeating mutant fails its own named row", async () => {
		// Mutant: the fake server records activities WITHOUT their textFormat —
		// as if raw-markdown shaping were lost. The outbound-rest row must fail
		// BY NAME while its siblings stay green on fresh fixtures.
		const rows = teamsDeltaRows(() => {
			const fx = makeTeamsFixture();
			const original = fx.bf.postActivity.bind(fx.bf);
			Object.defineProperty(fx.bf, "postActivity", {
				value: async (
					conversationId: string,
					activity: Record<string, unknown>,
					bearer: string,
				) => {
					const stripped = { ...activity };
					delete stripped["textFormat"]; // the lie
					return original(conversationId, stripped, bearer);
				},
			});
			return fx;
		});

		const restRow = rows.find(
			(r) => r.id === "transport.teams.outbound-rest-shapes",
		);
		expect(restRow).toBeDefined();
		const restReport = await runConformanceSuite({
			subjectName: "mutant-teams-wire",
			shape: "webhook",
			rows: [restRow as ConformanceRow],
		});
		expect(restReport.failed).toBe(1);
		expect(restReport.rows[0]?.pass).toBe(false);

		// Sanity: the OTHER rows still pass on their own fresh fixtures.
		const others = rows.filter((r) => r.id !== restRow?.id);
		const otherReport = await runConformanceSuite({
			subjectName: "mutant-teams-others",
			shape: "webhook",
			rows: others as ConformanceRow[],
		});
		if (otherReport.failed > 0) console.error(formatReport(otherReport));
		expect(otherReport.failed).toBe(0);
	}, 30_000);
});
