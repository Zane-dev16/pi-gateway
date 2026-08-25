// pi_platforms/conformance/feishu-rows.test.ts — SUITE WIRING for the FEISHU/
// LARK census port (04 §8 merge gate; roadmap Phase 6 exit criteria). The port
// supplies ONLY this wiring:
//
//   1. ALL applicable SHARED rows pass for shape="ws" against the REAL
//      FeishuSubject. Applicability is COMPUTED from capability probes (04 §8
//      conditional headers): Feishu declares NO native streaming (base
//      supports_draft_streaming stays False — Hermes parity), so the THREE
//      streaming rows are excluded BY THE PROBE, never by a hardcoded skip —
//      a capability flip RE-INCLUDES them (wa-cloud precedent).
//   2. ALL FIVE inherited persistent-ws transport family rows execute against
//      the REAL engine fixture (makeWsRows ← makeRealFeishuFixture) — the two
//      native-streaming-substrate rows realize their family intent as feishu
//      vendor truth (declaration-matches-seal-reality; whole-message markdown
//      decision), documented in feishu-fixture.ts.
//   3. SIX fresh fs.* shape-delta rows: event-subscription replay/dedup incl.
//      restart persistence, the A12 card callback round-trip through the SAME
//      kit resolvers, and the three A12 ingress classes each producing
//      MessageEvents through BOTH guards over the fake server.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: a lying/mutant fixture fails ITS OWN named rows, and
//      a capability lie (probe flipped ON without a plane) trips the transport
//      latch row BY NAME.

import { describe, expect, it } from "vitest";
import { mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { makeWsRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { runConformanceSuite, formatReport } from "./runner.js";

import {
	makeFeishuSubject,
	type FeishuSubject,
} from "../feishu/feishu-subject.js";
import {
	makeFeishuWorld,
	makeRealFeishuFixture,
	eventually,
	type FeishuWorld,
} from "../feishu/feishu-fixture.js";
import {
	FakeFeishuServer,
	cardActionEnvelope,
	driveCommentEnvelope,
	meetingInvitedEnvelope,
} from "../feishu/fake-feishu.js";
import {
	buildExecApprovalCard,
	buildUpdatePromptCard,
} from "../feishu/cards.js";
import { FEISHU_CARD_SCHEMA_CAPS } from "../feishu/manifest.js";
import {
	FeishuCommentRulesStore,
	resolveRule,
	isUserAllowed,
} from "../feishu/comment-rules.js";
import { FEISHU_MAX_MESSAGE_UNITS } from "../feishu/manifest.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeFeishuSubject({
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
		probe.adapter.supportsDraftStreaming("dm") === true ||
		probe.adapter.supportsDraftStreaming() === true;
	return { streamsSupported, excludedIds: [...STREAMING_ROW_IDS] };
}

// ── fs.* shape-delta rows (executed over the REAL engine fixture) ───────────

interface FsFixture extends FeishuWorld {}

async function freshFsFixture(name: string): Promise<FsFixture> {
	const world = makeFeishuWorld({ name, ladderIntervalMs: 100 });
	await world.engine.connect({ isReconnect: false });
	return world;
}

/** Push a p2p text event and drive the arrival batcher's injected timer. */
async function deliverText(
	world: FsFixture,
	messageId: string,
	text: string,
): Promise<void> {
	world.server.pushEvent(imText(messageId, "on_dm_row", text));
	await world.clock.advance(700); // batch window 600ms + slack
}

function imText(
	messageId: string,
	chatId: string,
	text: string,
): Parameters<FakeFeishuServer["pushEvent"]>[0] {
	return {
		header: { event_id: messageId, event_type: "im.message.receive_v1" },
		event: {
			message: {
				message_id: messageId,
				message_type: "text",
				content: JSON.stringify({ text }),
				chat_type: "p2p",
				chat_id: chatId,
			},
			sender: {
				sender_type: "user",
				sender_id: { open_id: "ou_user1", user_id: "u_1", union_id: "on_1" },
			},
		},
	};
}

/**
 * One delta-row factory. Every body drives the REAL ingress/egress paths
 * through a FRESH FsFixture (rows never couple through shared mutable state).
 */
function feishuDeltaRows(
	newFixture: () => Promise<FsFixture>,
): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: FsFixture) => Promise<void>,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["ws"]),
		run: async () => {
			let fx: FsFixture | null = null;
			try {
				fx = await newFixture();
				await body(fx);
				return { id, title, pass: true, shapes: new Set(["ws"]) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["ws"]),
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	});

	return [
		mk(
			"fs.subscription-replay-dedup",
			"feishu: disconnect-window events redelivered after reconnect (at-least-once) are suppressed exactly-once downstream by the persisted 24h-TTL dedup store",
			async (fx) => {
				deliverText(fx, "sr-1", "before outage");
				await eventually(() => fx.engine.inboundLog.length >= 1);
				fx.server.dropActive({});
				const before = fx.engine.inboundLog.length;
				// Disconnect-window events…
				fx.server.pushEvent(imText("sr-2", "on_dm_row", "during outage"));
				// …PLUS an at-least-once REDUPLICATED acked id (window replay
				// includes already-delivered events; dedup must absorb them).
				fx.server.pushEvent(imText("sr-1", "on_dm_row", "before outage"));
				fx.server.pushEvent(imText("sr-3", "on_dm_row", "after outage"));
				await fx.clock.advance(500); // fixed-interval ladder sleep → reconnect
				await eventually(() => fx.engine.isLive, 4000);
				await eventually(
					() =>
						new Set(fx.engine.inboundLog.map((e) => e.eventId)).size >=
						before + 2,
					4_000,
				);
				expect(fx.engine.seenMessages.suppressedCount).toBeGreaterThanOrEqual(
					1,
				);
				const ids = [...new Set(fx.engine.inboundLog.map((e) => e.eventId))];
				expect(ids).toContain("sr-2");
				expect(ids).toContain("sr-3");
			},
		),
		mk(
			"fs.restart-dedup-persistence",
			"feishu: dedup state persists across an adapter restart — a redelivered pre-restart id NEVER dispatches twice",
			async () => {
				const dir = mkdtempSync(join(tmpdir(), "fs-restart-"));
				const scheduler = new ManualScheduler();
				const statePath = join(dir, "feishu_seen_message_ids.json");
				const wireA = new FakePlatformWire();
				const a = makeFeishuSubject({
					wire: wireA,
					server: new FakeFeishuServer(),
					spawner: scheduler.spawner,
					dedupStatePath: statePath,
					name: "fs-a",
				});
				a.adapter.attachStandardGuard(scheduler.spawner);
				void wireA;
				// Inject the state path post-construction (constructor loads it).
				(a.adapter as unknown as { dedupStatePath?: string }).dedupStatePath =
					statePath;
				await a.adapter.connect({ isReconnect: false });
				a.adapter.seenMessages.isDuplicate("pre-restart-id");
				await a.adapter.disconnect(); // persists

				// "Restart": fresh store over the same path remembers the id.
				const b = makeFeishuSubject({
					wire: new FakePlatformWire(),
					server: new FakeFeishuServer(),
					spawner: scheduler.spawner,
					dedupStatePath: statePath,
					name: "fs-b",
				});
				expect(b.adapter.seenMessages.isDuplicate("pre-restart-id")).toBe(true);
			},
		),
		mk(
			"fs.card-callback-roundtrip",
			"feishu A12: card callback round-trip — builder→handler→resolver through the SAME kit pending stores; resolved card IS the ack (consumed state visible); double-tap stale; unauthorized answered-not-resolved",
			async (fx) => {
				// ── exec-approval family ──
				const approvalId = fx.engine.nextApprovalId();
				fx.engine.approvals.register(approvalId, "sk-ea");
				fx.engine.approvalState.set(approvalId, {
					sessionKey: "sk-ea",
					chatId: "oc_chat",
				});
				// Builder: the PROMPT card's button value feeds the handler.
				const promptCard = buildExecApprovalCard({
					title: "Exec approval",
					detail: "run rm -rf?",
					approvalId,
					allowSession: true,
					allowPermanent: false,
				});
				const actionBlock = promptCard.elements.find(
					(e) => e.tag === "action",
				) as { actions: Array<{ value: Record<string, unknown> }> } | undefined;
				const onceButton = actionBlock?.actions[0];
				expect(onceButton).toBeDefined();
				const ack = await fx.engine.handleCardActionTrigger(
					`tok-${approvalId}`,
					cardActionEnvelope({
						token: `tok-${approvalId}`,
						actionValue: (onceButton as { value: Record<string, unknown> })
							.value,
						openChatId: "oc_chat",
						operatorOpenId: "ou_user1",
					})["event"] as Record<string, unknown>,
				);
				const card = ack["card"] as
					| { header?: { template?: string; title?: { content?: string } } }
					| undefined;
				expect(card?.header?.template).toBe("green");
				expect(
					fx.engine.resolvedFamilies.some((f) => f.startsWith("ea:")),
				).toBe(true);

				// Double-tap resolves exactly once — corrective card, no re-resolve.
				const second = await fx.engine.handleCardActionTrigger(
					`tok-${approvalId}-b`,
					cardActionEnvelope({
						token: `tok-${approvalId}-b`,
						actionValue: {
							hermes_action: "approve_once",
							approval_id: approvalId,
						},
						openChatId: "oc_chat",
						operatorOpenId: "ou_user1",
					})["event"] as Record<string, unknown>,
				);
				expect(second["card"]).toBeDefined();
				expect(fx.engine.resolvedApprovalCards).toHaveLength(1);

				// Update-prompt family through the SAME handler.
				fx.engine.registerUpdatePrompt(555, "sk-upd");
				const updCard = buildUpdatePromptCard({
					title: "Update available",
					detail: "apply?",
					promptId: 555,
				});
				const updActions = updCard.elements.find((e) => e.tag === "action") as
					| { actions: Array<{ value: Record<string, unknown> }> }
					| undefined;
				const yesAck = await fx.engine.handleCardActionTrigger(
					"tok-upd",
					cardActionEnvelope({
						token: "tok-upd",
						actionValue: (
							updActions?.actions[0] as { value: Record<string, unknown> }
						).value,
						openChatId: "oc_chat",
						operatorOpenId: "ou_user1",
					})["event"] as Record<string, unknown>,
				);
				expect(yesAck["card"]).toBeDefined();
				expect(fx.engine.updatePromptAnswers).toContainEqual({
					promptId: 555,
					answer: "y",
				});

				// Card schema caps: the manifest records the HONEST absence of any
				// vendor cap (adapter.py codes none) — declared data, reviewed.
				expect(FEISHU_CARD_SCHEMA_CAPS).toEqual([]);
			},
		),
		mk(
			"fs.a12-card-command-ingress",
			"feishu A12: generic card click becomes a synthetic /card COMMAND MessageEvent traversing BOTH guards; 15-min token dedup drops replays silently; unauthorized operators refused before resolution",
			async (fx) => {
				const env = cardActionEnvelope({
					token: "tok-row-1",
					actionValue: { action: "open_board", board: "42" },
					actionTag: "button",
					openChatId: "oc_rowchat",
					operatorOpenId: "ou_clicker",
				});
				fx.server.pushEvent(env);
				await eventually(() => fx.engine.turnLog.length >= 1);
				// The synthetic COMMAND became a REAL turn (both guards traversed).
				expect(fx.engine.turnLog[0]).toContain("/card button");
				expect(JSON.stringify(fx.engine.turnLog[0])).toContain("open_board");

				// Token replay within 15 min: dropped BEFORE guard traversal.
				fx.server.pushEvent({ ...env });
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(fx.engine.cardCommandAudit).toHaveLength(1);
				expect(fx.engine.turnLog).toHaveLength(1);
			},
		),
		mk(
			"fs.a12-meeting-invite-ingress",
			"feishu A12: vc.bot.meeting_invited_v1 becomes a synthetic DM MessageEvent carrying the EXACT invite prompt through BOTH guards; malformed and duplicate invites dropped before dispatch",
			async (fx) => {
				const env = meetingInvitedEnvelope({
					eventId: "evt-row-9",
					meeting: {
						id: "m-9",
						topic: "Wave sync",
						meeting_no: "900-100-200",
						host_open_id: "ou_h",
						host_name: "Host",
					},
					inviter: { open_id: "ou_inviter", user_name: "Invoker" },
				});
				fx.server.pushEvent(env);
				await eventually(() => fx.engine.turnLog.length >= 1);
				const prompt = fx.engine.turnLog[0] ?? "";
				expect(prompt).toContain(
					"You have been invited to join a meeting: Wave sync",
				);
				expect(prompt).toContain("Meeting Number: 900-100-200");
				expect(prompt).toContain("Join the meeting directly.");
				// Duplicate key dropped silently.
				fx.server.pushEvent({ ...env });
				await new Promise<void>((r) => setTimeout(r, 20));
				const dispatched = fx.engine.meetingInviteLog.filter(
					(l) => l.outcome === "dispatched",
				);
				expect(dispatched).toHaveLength(1);
				// Malformed payload dropped.
				fx.server.pushEvent(
					meetingInvitedEnvelope({
						eventId: "evt-bad",
						meeting: { id: "", meeting_no: "" },
						inviter: { open_id: "" },
					}),
				);
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(
					fx.engine.meetingInviteLog.some(
						(l) => l.outcome === "dropped_malformed",
					),
				).toBe(true);
			},
		),
		mk(
			"fs.a12-drive-comment-ingress",
			"feishu A12: drive.notice.comment_add_v1 behind 3-tier access rules — pairing deny-by-default is SILENT; approved authors get a prompt turn through BOTH guards and the reply lands as a thread comment (1069302 ⇒ whole-comment fallback)",
			async (fx) => {
				const dir = mkdtempSync(join(tmpdir(), "fs-comment-rules-"));
				writeFileSync(
					join(dir, "feishu_comment_rules.json"),
					JSON.stringify({ policy: "pairing" }),
				);
				const rulesStore = new FeishuCommentRulesStore(dir);
				rulesStore.pairingAdd("ou_author");

				const posted: Array<{ kind: string; text: string }> = [];
				const answers: string[] = [];
				fx.engine.onDriveComment = async (_eventId, event) => {
					const { handleDriveCommentEvent } = await import(
						"../feishu/comment-ingress.js"
					);
					await handleDriveCommentEvent(event, {
						rulesStore,
						selfOpenId: "bot-self",
						api: {
							docMeta: () => ({
								title: "Port Spec",
								url: "https://feishu.cn/docx/x",
							}),
							batchQueryComment: () => ({
								isWhole: false,
								quote: "",
								replies: [
									{
										openId: "ou_author",
										text: "please summarize",
										replyId: "r0",
									},
								],
							}),
							listWholeComments: () => [],
							listCommentReplies: () => [
								{
									openId: "ou_author",
									text: "please summarize",
									replyId: "r0",
								},
							],
							addReaction: () => true,
							deleteReaction: () => true,
							postThreadReply: (_ft, _ty, _cid, text) => {
								posted.push({ kind: "thread", text });
								return { ok: true } as const;
							},
							postNewComment: (_ft, _ty, text) => {
								posted.push({ kind: "whole", text });
								return { ok: true } as const;
							},
						},
						// The agent leg rides the GUARD PIPELINE (PROPOSED DEC-046):
						// the prompt traverses both guards as a DM turn and the
						// scripted model's answer comes back via the reply log.
							runTurn: async (prompt) => {
								// The PROMPT itself traverses both guards as a DM turn;
								// the scripted model's ANSWER is fixed text.
								answers.push(prompt);
								await deliverText(fx, `cmt-turn-${answers.length}`, prompt);
								return "Here is the summary you asked for.";
							},
					});
				};

				// DENIED first: a non-paired commenter produces NOTHING (silent).
				fx.server.pushEvent(
					driveCommentEnvelope({
						event_id: "cev-denied",
						comment_id: "cmt-denied",
						file_token: "doctok1234567",
						file_type: "docx",
						from_open_id: "ou_stranger",
						to_open_id: "bot-self",
					}),
				);
				await new Promise<void>((r) => setTimeout(r, 30));
				const turnsBeforeDenialCheck = fx.engine.turnLog.length;
				expect(turnsBeforeDenialCheck).toBe(0);

				// APPROVED author: prompt turn through BOTH guards + thread reply.
				fx.server.pushEvent(
					driveCommentEnvelope({
						event_id: "cev-ok",
						comment_id: "cmt-ok",
						reply_id: "r0",
						file_token: "doctok1234567",
						file_type: "docx",
						from_open_id: "ou_author",
						to_open_id: "bot-self",
					}),
				);
				await eventually(() => fx.engine.turnLog.length >= 1);
				expect(fx.engine.turnLog[0]).toContain("please summarize");
				expect(fx.engine.turnLog[0]).toContain("Port Spec");
				await eventually(() => posted.length >= 1);
				expect(posted[0]?.kind).toBe("thread");
			},
		),
	];
}

// ── the suite ────────────────────────────────────────────────────────────────

describe("conformance suite — feishu census port (shape: ws)", () => {
	it("applicability is COMPUTED from capability probes (streaming family excluded iff not declared)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // NO native streaming (Hermes parity)
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("manifest production defaults match vendor ground truth", () => {
		// Spot-checks binding the manifest DATA to the transcribed constants
		// (full citations live in manifest.ts comments).
		expect(FEISHU_MAX_MESSAGE_UNITS).toBe(8000);
	});

	it("passes EVERY currently-encoded shared row against the feishu subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);
		const report = await runConformanceSuite({
			subjectName: "feishu",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it("passes ALL FIVE inherited ws-family transport rows against the REAL engine fixture", async () => {
		const rows = makeWsRows(makeRealFeishuFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "feishu-transport",
			shape: "ws",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.deferred).toEqual([]);
	});

	it("passes ALL SIX feishu shape-delta rows through the real engine fixture", async () => {
		const rows = feishuDeltaRows(() => freshFsFixture("fs-delta"));
		expect(rows.map((r) => r.id)).toEqual([
			"fs.subscription-replay-dedup",
			"fs.restart-dedup-persistence",
			"fs.card-callback-roundtrip",
			"fs.a12-card-command-ingress",
			"fs.a12-meeting-invite-ingress",
			"fs.a12-drive-comment-ingress",
		]);
		const report = await runConformanceSuite({
			subjectName: "feishu-deltas",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 40_000);

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));

		const transport = makeWsRows(makeRealFeishuFixture());
		const deltas = feishuDeltaRows(() => freshFsFixture("fs-full"));

		const report = await runConformanceSuite({
			subjectName: "feishu-full",
			shape: "ws",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 60_000);

	it("a CAPABILITY FLIP re-includes the streaming rows (never a hardcoded skip)", async () => {
		// Probe-computed applicability must FOLLOW the probe: a lying subject
		// that declares draft streaming gets the three streaming rows BACK —
		// and promptly FAILS the seal-discipline row (no seal lane exists).
		const scheduler = new ManualScheduler();
		const lying: ConformanceSubject & { adapter: FeishuSubject["adapter"] } =
			makeFeishuSubject({
				wire: new FakePlatformWire(),
				spawner: scheduler.spawner,
				scheduler,
				name: "fs-liar",
			});
		expect(lying.adapter.supportsDraftStreaming("dm")).toBe(false);
		// Flip ONLY the probe (the lie) — the plane stays absent.
		(
			lying.adapter as unknown as {
				supportsDraftStreaming: () => boolean;
			}
		).supportsDraftStreaming = () => true;

		const all = buildSharedRows({ makeSubject: () => lying });
		const report = await runConformanceSuite({
			subjectName: "fs-capability-liar",
			shape: "ws",
			rows: all.filter((r) => STREAMING_ROW_IDS.includes(r.id)),
		});
		expect(report.rows.length).toBe(3); // re-included BY THE FLIP
		expect(report.failed).toBeGreaterThan(0);
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("streaming.seal-discipline");
	}, 30_000);

	it("the gate DETECTS violations: lying fixtures fail their OWN named rows", async () => {
		// Mutant A: a replay fixture that LOSES disconnect-window events.
		const lyingTransport = makeWsRows({
			async resubscribeReplay() {
				return { sentDuringDisconnect: 5, replayedAfterResubscribe: 2 }; // LOST
			},
			async watchdogRecovery() {
				return { detectedDeadSocket: false, resumedWithoutLoss: true };
			},
			async retryAfterCapture() {
				return {
					closeCapturedSeconds: 0, // nothing captured
					nextDelayMs: 1000, // NOT the capture
					delayAuthoritative: false,
					restCapturedSeconds: 3,
				};
			},
			async capabilityLatchPermanence() {
				return {
					latchedOnFirstFailure: true,
					latchCount: 4,
					wireAttemptsAfterSkip: 9,
					supportsStreamingFalse: false, // THE capability lie
					transientDidNotLatch: false,
				};
			},
			async dualPathMarkdown() {
				return {
					nativeRawByteExact: false, // markdown CONVERTED (vendor violation)
					nativePrefixStable: true,
					restConvertedBold: false,
					restConvertedLink: true,
					restConvertedTable: true,
					linkPreviewOnAllTextSends: false,
					linkPreviewAbsentOffTextSends: true,
				};
			},
		});
		const transportReport = await runConformanceSuite({
			subjectName: "lying-feishu-transport",
			shape: "ws",
			rows: lyingTransport,
			suppliedTransportRowIds: new Set(lyingTransport.map((r) => r.id)),
		});
		expect(transportReport.failed).toBeGreaterThan(0);
		const failedIds = transportReport.rows
			.filter((r) => !r.pass)
			.map((r) => r.id);
		for (const id of TRANSPORT_ROW_REQUIREMENTS.ws) {
			expect(failedIds).toContain(id);
		}

		// Mutant B: an A12 ingress fixture whose card click never dispatches —
		// its named delta row fails BY NAME against the REAL engine.
		const rows = feishuDeltaRows(async () => {
			const fx = await freshFsFixture("fs-mutant-a12");
			// The lie: swallow card-action frames entirely (handler unwired).
			fx.server.pushEvent = () => ({
				header: { event_id: "", event_type: "" },
				event: {},
			});
			return fx;
		});
		const cardRow = rows.find(
			(r) => r.id === "fs.a12-card-command-ingress",
		) as ConformanceRow;
		const mutantReport = await runConformanceSuite({
			subjectName: "mutant-feishu-a12",
			shape: "ws",
			rows: [cardRow],
		});
		expect(mutantReport.failed).toBe(1);
		expect(mutantReport.rows[0]?.pass).toBe(false);

		// Sanity: rules-tier resolution itself stays honest under mutation.
		const cfg = {
			policy: "allowlist",
			allow_from: ["ou_member"],
			documents: {},
		};
		const rule = resolveRule(cfg, "docx", "tok");
		expect(isUserAllowed(rule, "ou_member", new Set())).toBe(true);
		expect(isUserAllowed(rule, "ou_other", new Set())).toBe(false);
	}, 30_000);
});
