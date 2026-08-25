// pi_platforms/conformance/irc-rows.test.ts — SUITE WIRING for the IRC census
// port (04 §8 merge gate; roadmap Phase 6). The port supplies ONLY this
// wiring:
//   1. ALL applicable SHARED rows pass for shape="ws" against the REAL
//      kit-built IrcSubject. Applicability is COMPUTED from capability data:
//      native draft streaming is excluded BY THE PROBE (IRC has no edit/draft
//      machinery), so the three streaming rows drop out via the probe, never
//      a hardcoded skip. The LIE-SCAN at the bottom flips THE datum and shows
//      the streaming family then RUN and FAIL against seal reality.
//   2. ALL FIVE inherited ws transport rows run over the REAL engine fixture
//      (makeRealIrcFixture) with documented leg mappings — PROPOSED DEC texts
//      in irc-world.ts cover the replay-window, death-detection, and
//      retry-after-capture mappings.
//   3. FOUR fresh IRC shape-delta rows execute through the real engine.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: lying fixtures fail their own named rows.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import { makeWsRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import type { ConformanceRow as Row } from "./rows.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { FakeIrcServer } from "../irc/fake-irc-server.js";
import { makeIrcSubject, type IrcSubject } from "../irc/irc-subject.js";
import { makeIrcShapeRows, makeRealIrcFixture } from "../irc/irc-world.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: {
		withSecret?: boolean | undefined;
		name?: string | undefined;
		declaredDraftStreaming?: boolean | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeIrcSubject({
		wire: new FakePlatformWire(),
		server: new FakeIrcServer(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
		...(opts.declaredDraftStreaming !== undefined
			? { declaredDraftStreaming: opts.declaredDraftStreaming }
			: {}),
	});
}

/** §8 streaming family — applicable ONLY when the probe admits drafts. */
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
	const streamsSupported = probe.adapter.supportsDraftStreaming() === true;
	return { streamsSupported, excludedIds: [...STREAMING_ROW_IDS] };
}

// ── lying shape rows (negative validation) ──────────────────────────────────

function makeLyingIrcShapeRows(): Row[] {
	const mk = (id: string, detail: string): Row => ({
		id,
		title: "lying",
		shapes: new Set(["ws"]),
		run: async () => ({
			id,
			title: "lying",
			pass: false,
			shapes: new Set(["ws"]),
			detail,
		}),
	});
	return [
		mk(
			"transport.irc.line-protocol-gates",
			"lying fixture: unaddressedChannelTurns=3",
		),
		mk(
			"transport.irc.nick-collision-ladder",
			"lying fixture: collisionCount=0",
		),
		mk(
			"transport.irc.outbound-sanitizer-wire-truth",
			"lying fixture: CRLF reached the wire",
		),
		mk("transport.irc.rate-paced-burst", "lying fixture: pacing gaps=0"),
	];
}

// ── suite wiring ────────────────────────────────────────────────────────────

describe("conformance suite — IRC census port (shape: ws)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff the no-draft probe closes)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // no draft/edit machinery on the line protocol
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("passes EVERY applicable shared row against the IRC subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows: Row[] = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: "irc",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it("passes ALL FIVE inherited ws transport rows against the REAL engine fixture (documented leg mappings)", async () => {
		const fixtureRows = makeWsRows(makeRealIrcFixture());
		expect(fixtureRows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "irc-transport-inherited",
			shape: "ws",
			rows: fixtureRows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 30_000);

	it("passes ALL FOUR IRC shape-delta rows through the real engine fixture", async () => {
		const rows: Row[] = makeIrcShapeRows();
		expect(rows.map((r) => r.id)).toEqual([
			"transport.irc.line-protocol-gates",
			"transport.irc.nick-collision-ladder",
			"transport.irc.outbound-sanitizer-wire-truth",
			"transport.irc.rate-paced-burst",
		]);
		const report = await runConformanceSuite({
			subjectName: "irc-shape",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	});

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const shared: Row[] = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));

		const transport = makeWsRows(makeRealIrcFixture());
		const suppliedTransportRowIds = new Set(transport.map((r) => r.id));
		for (const requiredId of TRANSPORT_ROW_REQUIREMENTS.ws) {
			expect(suppliedTransportRowIds.has(requiredId)).toBe(true);
		}
		const deltas: Row[] = makeIrcShapeRows();

		const report = await runConformanceSuite({
			subjectName: "irc-full",
			shape: "ws",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds,
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 45_000);

	it("the gate DETECTS violations: a LYING capability datum fails the streaming family BY NAME", async () => {
		// Lie-scan mutant: flip THE manifest datum that drives the exclusion
		// probe (declaredDraftStreaming=true arms stream-is-message chats). The
		// adapter still has NO draft/seal wire lanes, so seal-discipline can
		// never observe its exactly-one-seal invariant and FAILS by name.
		const all = buildSharedRows({
			makeSubject: (o) => makeSubject({ ...o, declaredDraftStreaming: true }),
		});
		const streamingRows = all.filter((r) => STREAMING_ROW_IDS.includes(r.id));
		expect(streamingRows.length).toBe(3);
		const report = await runConformanceSuite({
			subjectName: "mutant-irc-streaming-lie",
			shape: "ws",
			rows: streamingRows,
		});
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("streaming.seal-discipline");

		// …and the HONEST probe stays closed for every fresh subject.
		expect(computeApplicability().streamsSupported).toBe(false);
	});

	it("the gate DETECTS violations: a lying transport fixture fails ITS OWN named rows", async () => {
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
			subjectName: "mutant-ws-fixture-irc",
			shape: "ws",
			rows: lying,
		});
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("transport.ws.resubscribe-replay");
		expect(failedIds).toContain("transport.ws.heartbeat-watchdog-recovery");
		expect(failedIds).toContain("transport.ws.retry-after-capture");

		// …and a lying SHAPE fixture fails every named shape row too, while the
		// honest shape catalog stays green (control).
		const lyingShape = makeLyingIrcShapeRows();
		const shapeReport = await runConformanceSuite({
			subjectName: "mutant-irc-shape",
			shape: "ws",
			rows: lyingShape,
		});
		expect(shapeReport.failed).toBe(lyingShape.length);

		const honestShape = await runConformanceSuite({
			subjectName: "honest-after-mutant",
			shape: "ws",
			rows: makeIrcShapeRows(),
		});
		expect(honestShape.failed).toBe(0);
	}, 30_000);
});

export type { IrcSubject };
