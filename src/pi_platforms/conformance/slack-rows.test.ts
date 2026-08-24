// pi_platforms/conformance/slack-rows.test.ts — SUITE WIRING for the Slack
// census port (04 §8 merge gate; roadmap Phase 6 exit criteria). The port
// INHERITS the persistent-ws transport family: all 23 shared rows run against
// the slack subject, and ALL FIVE `transport.ws.*` family rows execute against
// the REAL SlackAdapter over the Socket-Mode fake server (makeWsRows ←
// makeRealSlackFixture — same named rows, slack-backed fixture, no stubs).
// Six SLACK-SPECIFIC required contracts are appended as first-class rows:
// socket-mode envelope shapes, retry-flag dedup × cursor replay, Block Kit
// round-trip via THE kit grammar, mrkdwn byte-exact decision matrix vs
// RAW-native, rate-tier gating before egress, and the approvals card-first
// e2e through the pi_embedded DeliveryBridge.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import { makeWsRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceRow } from "./rows.js";
import type { ConformanceSubject } from "./harness.js";
import {
	runConformanceSuite,
	formatReport,
	type SuiteReport,
} from "./runner.js";
import {
	makeSlackSubject,
	type SlackSubjectOptions,
} from "../slack/slack-subject.js";
import {
	makeSlackWorld,
	makeRealSlackFixture,
	eventually,
} from "../slack/slack-fixture.js";
import type { SlackEnvelopeEvent } from "../slack/fake-socket-mode.js";
import { convertMarkdownToSlackMrkdwn } from "../slack/mrkdwn.js";
import {
	ApprovalCardLedger,
	DeliveryBridge,
	hasExecApprovalCard,
} from "../../pi_embedded/approvals/delivery.js";
import type { GatewayClock } from "../../pi_embedded/approvals/clock.js";

function makeSubject(
	opts: {
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		withSecret?: boolean | undefined;
		name?: string | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	const subjectOpts: SlackSubjectOptions = {
		wire: new FakePlatformWire(),
		spawner: scheduler.spawner,
		scheduler,
		name: opts.name,
	};
	void opts.withSecret; // slack subjects resolve both manifest tokens
	const subject = makeSlackSubject(subjectOpts);
	if (opts.streamIsMessageChatIds !== undefined) {
		for (const id of opts.streamIsMessageChatIds) {
			subject.adapter.markStreamIsMessage(id);
		}
	}
	return subject;
}

// ── slack-specific rows (required contracts, executed like any other) ──────

const bridgeClock: GatewayClock = {
	nowSeconds: () => 1_000,
	sleepMs: async () => {},
};

function makeSlackSpecificRows(): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		run: () => Promise<void>,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["ws"]),
		run: async () => {
			try {
				await run();
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
			"slack.transport.socket-mode-envelopes",
			"socket-mode envelopes: hello/subscribed handshake, envelope ids + retry flags on redelivery, cursor advance IS the ack point",
			async () => {
				const world = makeSlackWorld({ name: "slack-row-envelopes" });
				const { engine, socketServer } = world;
				await world.connectAndAwaitLive();
				const subs = socketServer
					.getFramesReceived()
					.filter((f) => f.frame["type"] === "subscribe");
				if (subs.length !== 1 || subs[0]?.frame["cursor"] !== null) {
					throw new Error(
						"cold boot must subscribe exactly once with null cursor",
					);
				}
				world.pushMessage("C1", "one");
				world.pushMessage("C1", "two");
				await eventually(() => engine.cursor.value === "e2");
				const inbound =
					engine.inboundLog as unknown as readonly SlackEnvelopeEvent[];
				if (
					inbound[0]?.envelopeId === undefined ||
					inbound[0]?.retryAttempt !== 0
				) {
					throw new Error(
						"fresh envelopes must carry envelopeId + retryAttempt=0",
					);
				}
			},
		),
		mk(
			"slack.transport.retry-flag-dedup-cursor-replay",
			"retry-flag dedup × cursor replay (#4777): flagged overlap redelivered once, suppressed by the workspace-scoped window, gap covered exactly-once",
			async () => {
				const world = makeSlackWorld({ name: "slack-row-replay" });
				const { engine, socketServer, clock, subject } = world;
				await world.connectAndAwaitLive();
				for (const t of ["r1", "r2", "r3"]) world.pushMessage("C1", t);
				await eventually(() => engine.cursor.value === "e3");
				socketServer.dropActive({ reason: "blip" });
				world.pushMessage("C1", "r4");
				world.pushMessage("C1", "r5");
				socketServer.markUnackedForRedelivery("e3");
				await clock.advance(5_000);
				await eventually(() => engine.isLive && engine.cursor.value === "e5");
				const sorted = [...subject.turns()].sort();
				if (
					JSON.stringify(sorted) !==
					JSON.stringify(["r1", "r2", "r3", "r4", "r5"])
				)
					throw new Error(`exactly-once violated: ${JSON.stringify(sorted)}`);
				if (engine.dedupSuppressedCount < 1)
					throw new Error("overlap redelivery was NOT suppressed");
				if (
					!engine.redeliveryLog.some(
						(r) => r.id === "e3" && r.retryAttempt >= 1,
					)
				)
					throw new Error("redelivery lacked retry flags");
			},
		),
		mk(
			"slack.interactive.block-kit-roundtrip",
			"Block Kit round-trip via THE kit grammar: build → action → resolve for approval AND clarify families; consumed buttons stripped; double-tap resolves once",
			async () => {
				const world = makeSlackWorld({ name: "slack-row-roundtrip" });
				const { engine, subject } = world;
				await world.connectAndAwaitLive();
				const sent = await engine.sendClarifyCard({
					chatId: "C-rt",
					question: "Proceed?",
					choices: ["yes", "no"],
					clarifyId: 7,
					sessionKey: "slack:C-rt:t1",
				});
				const cardOp = subject.wire.sendsOf("C-rt")[0];
				if (!Array.isArray(cardOp?.metadata["blocks"]))
					throw new Error("card shipped WITHOUT blocks");
				world.pushInteractive({
					type: "block_actions",
					user: { id: "user-1", name: "U" },
					channel: { id: "C-rt" },
					message: { ts: sent.messageId ?? "", blocks: [] },
					actions: [{ action_id: "hermes_clarify_choice_0", value: "7|0" }],
				});
				await eventually(() => subject.wire.editsOf("C-rt").length === 1);
				const edit = subject.wire.editsOf("C-rt")[0];
				if (edit?.metadata["buttons_removed"] !== true)
					throw new Error("consumed buttons NOT stripped from host message");
				// Double-tap: answered, never re-resolved.
				world.pushInteractive({
					type: "block_actions",
					user: { id: "user-1", name: "U" },
					channel: { id: "C-rt" },
					message: { ts: sent.messageId ?? "", blocks: [] },
					actions: [{ action_id: "hermes_clarify_choice_0", value: "7|0" }],
				});
				await eventually(() => engine.interactiveAudit.length === 2);
				if (engine.routerAuditResolved().length !== 1)
					throw new Error("double-tap resolved MORE than once");
			},
		),
		mk(
			"slack.formatting.mrkdwn-decision-matrix",
			"mrkdwn decision matrix byte-exact on the REST path; native stream bytes untouched (§10.2 dual-path)",
			async () => {
				const matrix: Array<[string, string]> = [
					["**b** v **w**", "*b* v *w*"],
					["[x](https://q.r)", "<https://q.r|x>"],
					["## Head", "*Head*"],
					["***bi***", "*_bi_*"],
					["~~s~~", "~s~"],
					["**(done)**", "*(done)\u200b*"],
					["```py\ncode\n```", "```\ncode\n```"],
					["`keep **raw**`", "`keep **raw**`"],
					["<script>", "&lt;script&gt;"],
				];
				for (const [input, expected] of matrix) {
					const got = convertMarkdownToSlackMrkdwn(input);
					if (got !== expected) {
						throw new Error(
							`${JSON.stringify(input)} → ${JSON.stringify(got)}, want ${JSON.stringify(expected)}`,
						);
					}
				}
				// Native path RAW: the same constructs ship UNCONVERTED on drafts.
				const world = makeSlackWorld({ name: "slack-row-matrix-native" });
				const raw = "**not** [converted](https://x.y)";
				await world.engine.sendDraft({
					chatId: "C-n",
					draftId: 1,
					content: raw,
				});
				const draft = world.wire.draftsOf("C-n")[0];
				if (draft?.content !== raw)
					throw new Error(
						"native draft bytes were converted (§10.2 violation)",
					);
			},
		),
		mk(
			"slack.egress.rate-tier-gating",
			"rate tiers consulted BEFORE egress per method class (Q17 manifest budgets); exhausted tier refuses without wire ops; injected clock rotates the window",
			async () => {
				const world = makeSlackWorld({ name: "slack-row-rate" });
				const { engine, wire, clock } = world;
				for (let i = 0; i < 20; i++) {
					const r = await engine.deliverText("C-rate", `m${i}`);
					if (!r[r.length - 1]?.success) throw new Error(`send ${i} failed`);
				}
				const refused = await engine.deliverText("C-rate", "over");
				const last = refused[refused.length - 1];
				if (
					last?.success !== false ||
					last.error !== "rate_limited:tier2-messaging"
				)
					throw new Error("exhausted tier did not refuse with tier name");
				if (wire.sendsOf("C-rate").length !== 20)
					throw new Error("refusal LEAKED to the wire");
				if ((last.retryAfter ?? 0) < 1)
					throw new Error("retry horizon not surfaced");
				// Independent streaming class still admitted.
				const draft = await engine.sendDraft({
					chatId: "C-other",
					draftId: 1,
					content: "stream",
				});
				if (!draft.success)
					throw new Error("classes must budget independently");
				await clock.advance(60_000);
				const after = await engine.deliverText("C-rate", "rotated");
				if (!after[after.length - 1]?.success)
					throw new Error("window rotation under the injected clock failed");
			},
		),
		mk(
			"slack.approvals.card-first-e2e",
			"approvals bridge seam: DeliveryBridge picks CARD-FIRST over the real adapter (class probe); tap resolves through ea grammar; buttons stripped; double-tap answered",
			async () => {
				const world = makeSlackWorld({ name: "slack-row-cardfirst" });
				const { engine, subject } = world;
				await world.connectAndAwaitLive();
				if (!hasExecApprovalCard(engine))
					throw new Error(
						"class-level sendExecApproval missing — no card path",
					);
				const ledger = new ApprovalCardLedger();
				const bridge = new DeliveryBridge({
					target: engine as unknown as ConstructorParameters<
						typeof DeliveryBridge
					>[0]["target"],
					chatId: "C-e2e",
					ledger,
					clock: bridgeClock,
				});
				await bridge.notify({
					sessionKey: "slack:C-e2e:t1",
					command: "deploy.sh --env prod",
					description: "production deploy",
					allowPermanent: true,
					allowSession: true,
					smartDenied: false,
				});
				const sends = subject.wire.sendsOf("C-e2e");
				if (sends.length !== 1) throw new Error("card-first must be ONE send");
				if (!sends[0]?.content.startsWith("⚠️ Command approval required:"))
					throw new Error("fallback text shipped instead of the card");
				if (!Array.isArray(sends[0]?.metadata["blocks"]))
					throw new Error("card shipped without blocks");
				if (ledger.size !== 1)
					throw new Error("ledger not bound after delivery");
				world.pushInteractive({
					type: "block_actions",
					user: { id: "user-1", name: "Owner" },
					channel: { id: "C-e2e" },
					message: { ts: sends[0]?.messageId ?? "", blocks: [] },
					actions: [{ action_id: "hermes_approve_once", value: "1" }],
				});
				await eventually(() => subject.wire.editsOf("C-e2e").length === 1);
				if (!engine.resolvedFamilies.includes("ea"))
					throw new Error("approval resolver never fired");
				if (
					subject.wire.editsOf("C-e2e")[0]?.metadata["buttons_removed"] !== true
				)
					throw new Error("buttons not stripped after resolution");
			},
		),
	];
}

describe("conformance suite — Slack census port (Phase 6 gate)", () => {
	it("passes EVERY shared row for the slack subject", async () => {
		const rows = buildSharedRows({ makeSubject });
		expect(rows.length).toBeGreaterThanOrEqual(22);
		const report = await runConformanceSuite({
			subjectName: "slack",
			shape: "ws",
			rows,
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.passed).toBe(rows.length);
	});

	it("passes ALL FIVE inherited ws-family transport rows against the REAL slack fixture", async () => {
		const rows = makeWsRows(makeRealSlackFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "slack-transport",
			shape: "ws",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
	});

	it("passes all SIX slack-specific required rows", async () => {
		const rows = makeSlackSpecificRows();
		const report = await runConformanceSuite({
			subjectName: "slack-specific",
			shape: "ws",
			rows,
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.passed).toBe(6);
	});

	it("full applicable catalog is GREEN — shared + ws-family + slack-specific, zero deferred", async () => {
		const shared = buildSharedRows({ makeSubject });
		const transport = makeWsRows(makeRealSlackFixture());
		const slackSpecific = makeSlackSpecificRows();
		const report: SuiteReport = await runConformanceSuite({
			subjectName: "slack-full",
			shape: "ws",
			rows: [...shared, ...transport, ...slackSpecific],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred.length).toBe(0);
		expect(report.allApplicablePassed).toBe(true);
	});

	it("the gate still DETECTS violations (negative validation of the gate)", async () => {
		const lying = makeWsRows({
			async resubscribeReplay() {
				return { sentDuringDisconnect: 5, replayedAfterResubscribe: 2 };
			},
			async watchdogRecovery() {
				return { detectedDeadSocket: false, resumedWithoutLoss: true };
			},
			async retryAfterCapture() {
				return {
					closeCapturedSeconds: 0,
					nextDelayMs: 1000,
					delayAuthoritative: false,
					restCapturedSeconds: 3,
				};
			},
			async capabilityLatchPermanence() {
				return {
					latchedOnFirstFailure: true,
					latchCount: 4,
					wireAttemptsAfterSkip: 9,
					supportsStreamingFalse: false,
					transientDidNotLatch: false,
				};
			},
			async dualPathMarkdown() {
				return {
					nativeRawByteExact: false,
					nativePrefixStable: true,
					restConvertedBold: false,
					restConvertedLink: true,
					restConvertedTable: true,
					linkPreviewOnAllTextSends: false,
					linkPreviewAbsentOffTextSends: true,
				};
			},
		});
		const report = await runConformanceSuite({
			subjectName: "lying-slack",
			shape: "ws",
			rows: lying,
			suppliedTransportRowIds: new Set(lying.map((r) => r.id)),
		});
		expect(report.failed).toBeGreaterThan(0);
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		for (const required of TRANSPORT_ROW_REQUIREMENTS.ws) {
			expect(failedIds).toContain(required);
		}
	});
});
