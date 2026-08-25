// pi_platforms/conformance/ntfy-rows.test.ts — SUITE WIRING for the NTFY
// census port (04 §8 merge gate; roadmap Phase 6). The port supplies ONLY
// this wiring:
//   1. ALL applicable SHARED rows pass for shape="ws" against the REAL
//      kit-built NtfySubject — streaming family excluded BY THE PROBE; lie-
//      scan proves flipping the datum FAILS seal reality BY NAME.
//   2. ALL FIVE inherited ws transport rows run over the REAL engine fixture
//      with documented leg mappings — PROPOSED DEC texts in ntfy-world.ts.
//   3. THREE fresh ntfy shape-delta rows execute through the real engine.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: lying fixtures fail their own named rows.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows, type ConformanceRow as Row } from "./rows.js";
import type { ConformanceSubject } from "./harness.js";
import { makeWsRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { FakeNtfyServer } from "../ntfy/fake-ntfy-server.js";
import { makeNtfySubject } from "../ntfy/ntfy-subject.js";
import { makeNtfyShapeRows, makeRealNtfyFixture } from "../ntfy/ntfy-world.js";

const STREAMING_ROW_IDS: readonly string[] = [
	"streaming.prefix-mutation-detected",
	"streaming.seal-discipline",
	"streaming.failed-seal-still-delivers",
];

function makeSubject(
	opts: {
		withSecret?: boolean | undefined;
		name?: string | undefined;
		declaredDraftStreaming?: boolean | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeNtfySubject({
		wire: new FakePlatformWire(),
		server: new FakeNtfyServer(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
		...(opts.declaredDraftStreaming !== undefined
			? { declaredDraftStreaming: opts.declaredDraftStreaming }
			: {}),
	});
}

function computeApplicability(): {
	streamsSupported: boolean;
	excludedIds: string[];
} {
	const probe = makeSubject();
	return {
		streamsSupported: probe.adapter.supportsDraftStreaming() === true,
		excludedIds: [...STREAMING_ROW_IDS],
	};
}

function makeLyingNtfyShapeRows(): Row[] {
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
			"transport.ntfy.stream-dedup-window",
			"lying fixture: duplicate id produced a second turn",
		),
		mk(
			"transport.ntfy.backoff-ladder",
			"lying fixture: ladder reset while alive <60s",
		),
		mk(
			"transport.ntfy.publish-shapes",
			"lying fixture: body chunked past 4096",
		),
	];
}

describe("conformance suite — NTFY census port (shape: ws)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff the no-draft probe closes)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false);
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("passes EVERY applicable shared row against the NTFY subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows: Row[] = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: "ntfy",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it("passes ALL FIVE inherited ws transport rows against the REAL engine fixture (documented leg mappings)", async () => {
		const fixtureRows = makeWsRows(makeRealNtfyFixture());
		expect(fixtureRows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "ntfy-transport-inherited",
			shape: "ws",
			rows: fixtureRows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 30_000);

	it("passes ALL THREE ntfy shape-delta rows through the real engine fixture", async () => {
		const rows: Row[] = makeNtfyShapeRows();
		expect(rows.map((r) => r.id)).toEqual([
			"transport.ntfy.stream-dedup-window",
			"transport.ntfy.backoff-ladder",
			"transport.ntfy.publish-shapes",
		]);
		const report = await runConformanceSuite({
			subjectName: "ntfy-shape",
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

		const transport = makeWsRows(makeRealNtfyFixture());
		const suppliedTransportRowIds = new Set(transport.map((r) => r.id));
		for (const requiredId of TRANSPORT_ROW_REQUIREMENTS.ws) {
			expect(suppliedTransportRowIds.has(requiredId)).toBe(true);
		}
		const deltas: Row[] = makeNtfyShapeRows();

		const report = await runConformanceSuite({
			subjectName: "ntfy-full",
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
		const all = buildSharedRows({
			makeSubject: (o) => makeSubject({ ...o, declaredDraftStreaming: true }),
		});
		const streamingRows = all.filter((r) => STREAMING_ROW_IDS.includes(r.id));
		expect(streamingRows.length).toBe(3);
		const report = await runConformanceSuite({
			subjectName: "mutant-ntfy-streaming-lie",
			shape: "ws",
			rows: streamingRows,
		});
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("streaming.seal-discipline");
		expect(computeApplicability().streamsSupported).toBe(false);
	});

	it("the gate DETECTS violations: LYING transport + shape fixtures fail THEIR OWN named rows", async () => {
		const lyingTransport = makeWsRows({
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
		const transportReport = await runConformanceSuite({
			subjectName: "mutant-ws-fixture-ntfy",
			shape: "ws",
			rows: lyingTransport,
		});
		const failedIds = transportReport.rows
			.filter((r) => !r.pass)
			.map((r) => r.id);
		for (const requiredId of TRANSPORT_ROW_REQUIREMENTS.ws) {
			expect(failedIds).toContain(requiredId);
		}

		const lyingShape = makeLyingNtfyShapeRows();
		const shapeReport = await runConformanceSuite({
			subjectName: "mutant-ntfy-shape",
			shape: "ws",
			rows: lyingShape,
		});
		expect(shapeReport.failed).toBe(lyingShape.length);

		const honestReport = await runConformanceSuite({
			subjectName: "honest-after-mutant",
			shape: "ws",
			rows: makeNtfyShapeRows(),
		});
		expect(honestReport.failed).toBe(0);
	}, 30_000);
});
