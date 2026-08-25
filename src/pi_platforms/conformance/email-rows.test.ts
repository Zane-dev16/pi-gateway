// pi_platforms/conformance/email-rows.test.ts — SUITE WIRING for the EMAIL
// census port (04 §8 merge gate; roadmap Phase 6). The port supplies ONLY
// this wiring:
//   1. ALL applicable SHARED rows pass for shape="polling" against the REAL
//      kit-built EmailSubject — streaming family excluded BY THE PROBE
//      (email has no draft/edit machinery); lie-scan proves flipping the datum
//      FAILS seal reality BY NAME.
//   2. ALL FOUR inherited polling transport rows run over the REAL engine
//      fixture with documented vendor-true leg mappings — PROPOSED DEC texts
//      in email-world.ts cover the conflict and heartbeat mappings.
//   3. FOUR fresh email shape-delta rows execute through the real engine.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: lying fixtures fail their own named rows.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows, type ConformanceRow as Row } from "./rows.js";
import type { ConformanceSubject } from "./harness.js";
import { makePollingRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { FakeImapServer, FakeSmtpServer } from "../email/fake-mail-servers.js";
import { makeEmailSubject } from "../email/email-subject.js";
import {
	makeEmailShapeRows,
	makeRealEmailFixture,
} from "../email/email-world.js";

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
	return makeEmailSubject({
		wire: new FakePlatformWire(),
		imap: new FakeImapServer(),
		smtp: new FakeSmtpServer(),
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

function makeLyingEmailShapeRows(): Row[] {
	const mk = (id: string, detail: string): Row => ({
		id,
		title: "lying",
		shapes: new Set(["polling"]),
		run: async () => ({
			id,
			title: "lying",
			pass: false,
			shapes: new Set(["polling"]),
			detail,
		}),
	});
	return [
		mk(
			"transport.email.uid-cursor-discipline",
			"lying fixture: refused UID marked seen",
		),
		mk(
			"transport.email.sender-authz-ladder",
			"lying fixture: spoofed From admitted",
		),
		mk(
			"transport.email.mime-sanitizer-wire-truth",
			"lying fixture: html shipped as text/plain part",
		),
		mk(
			"transport.email.ipv4-fallback-ladder",
			"lying fixture: TLS failure retried",
		),
	];
}

describe("conformance suite — EMAIL census port (shape: polling)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff the no-draft probe closes)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false);
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("passes EVERY applicable shared row against the EMAIL subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows: Row[] = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: "email",
			shape: "polling",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it("passes ALL FOUR inherited polling transport rows against the REAL engine fixture (documented leg mappings)", async () => {
		const fixtureRows = makePollingRows(makeRealEmailFixture());
		expect(fixtureRows.map((r) => r.id)).toEqual(
			TRANSPORT_ROW_REQUIREMENTS.polling,
		);
		const report = await runConformanceSuite({
			subjectName: "email-transport-inherited",
			shape: "polling",
			rows: fixtureRows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 30_000);

	it("passes ALL FOUR email shape-delta rows through the real engine fixture", async () => {
		const rows: Row[] = makeEmailShapeRows();
		expect(rows.map((r) => r.id)).toEqual([
			"transport.email.uid-cursor-discipline",
			"transport.email.sender-authz-ladder",
			"transport.email.mime-sanitizer-wire-truth",
			"transport.email.ipv4-fallback-ladder",
		]);
		const report = await runConformanceSuite({
			subjectName: "email-shape",
			shape: "polling",
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

		const transport = makePollingRows(makeRealEmailFixture());
		const suppliedTransportRowIds = new Set(transport.map((r) => r.id));
		for (const requiredId of TRANSPORT_ROW_REQUIREMENTS.polling) {
			expect(suppliedTransportRowIds.has(requiredId)).toBe(true);
		}
		const deltas: Row[] = makeEmailShapeRows();

		const report = await runConformanceSuite({
			subjectName: "email-full",
			shape: "polling",
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
			subjectName: "mutant-email-streaming-lie",
			shape: "polling",
			rows: streamingRows,
		});
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("streaming.seal-discipline");
		expect(computeApplicability().streamsSupported).toBe(false);
	});

	it("the gate DETECTS violations: a LYING transport fixture fails ITS OWN named rows", async () => {
		const lying = makePollingRows({
			async simulateOutageAndReconnect() {
				return { queuedBeforeReconnect: 3, deliveredAfterReconnect: 0 };
			},
			async holdAndRedispatch() {
				return { held: 3, redispatched: 1 }; // LOST events
			},
			async conflictRecovery() {
				return {
					generationsBumped: 0,
					dropPendingUpdatesOnRestart: false,
					fatalAfterExhaustion: false,
				};
			},
			async heartbeatEscalation() {
				return { stuckProbes: 2, reconnectTriggered: false };
			},
		});
		const report = await runConformanceSuite({
			subjectName: "mutant-polling-fixture-email",
			shape: "polling",
			rows: lying,
		});
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		for (const requiredId of TRANSPORT_ROW_REQUIREMENTS.polling) {
			expect(failedIds).toContain(requiredId);
		}

		const lyingShape = makeLyingEmailShapeRows();
		const shapeReport = await runConformanceSuite({
			subjectName: "mutant-email-shape",
			shape: "polling",
			rows: lyingShape,
		});
		expect(shapeReport.failed).toBe(lyingShape.length);

		const honestReport = await runConformanceSuite({
			subjectName: "honest-after-mutant",
			shape: "polling",
			rows: makeEmailShapeRows(),
		});
		expect(honestReport.failed).toBe(0);
	});
});
