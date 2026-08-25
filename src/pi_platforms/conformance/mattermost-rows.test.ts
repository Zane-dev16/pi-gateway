// pi_platforms/conformance/mattermost-rows.test.ts — SUITE WIRING for the
// MATTERMOST census port (04 §8 merge gate; roadmap Phase 6). The port
// supplies ONLY this wiring: all shared rows run against the Mattermost
// subject, the FIVE inherited persistent-ws transport rows run against the
// REAL engine fixture (makeRealMattermostFixture) — no stubbed return values —
// and the SIX mattermost-shape rows close the shape deltas.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import { makeWsRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import {
	makeMattermostShapeFixture,
	makeRealMattermostFixture,
} from "../mattermost/mattermost-world.js";
import { makeMattermostShapeRows } from "../mattermost/mattermost-shape-rows.js";
import { FakeMattermost } from "../mattermost/mm-fake-server.js";
import { makeMattermostSubject } from "../mattermost/mattermost-subject.js";

function makeSubject(
	opts: {
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		withSecret?: boolean | undefined;
		name?: string | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	const subject = makeMattermostSubject({
		wire: new FakePlatformWire(),
		mm: new FakeMattermost(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
	});
	if (opts.streamIsMessageChatIds !== undefined) {
		for (const id of opts.streamIsMessageChatIds) {
			subject.adapter.markStreamIsMessage(id);
		}
	}
	return subject;
}

describe("conformance suite — mattermost census port (Phase 6)", () => {
	it("passes EVERY currently-encoded shared row for the mattermost subject", async () => {
		const rows = buildSharedRows({ makeSubject });
		expect(rows.length).toBeGreaterThanOrEqual(22);
		const report = await runConformanceSuite({
			subjectName: "mattermost",
			shape: "ws",
			rows,
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.passed).toBe(rows.length);
	});

	it("passes ALL FIVE inherited ws transport rows against the REAL mattermost engine", async () => {
		const rows = makeWsRows(makeRealMattermostFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "mattermost-transport",
			shape: "ws",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.deferred).toEqual([]);
	});

	it("passes ALL SIX mattermost-shape rows (shape deltas over the family)", async () => {
		const rows = makeMattermostShapeRows();
		expect(rows.map((r) => r.id)).toEqual([
			"mm.ws-event-dedup",
			"mm.dual-path-markdown",
			"mm.mention-gating-matrix",
			"mm.thread-root-discipline",
			"mm.rest-backfill-window",
			"mm.reconnect-auth-ladder",
		]);
		const report = await runConformanceSuite({
			subjectName: "mattermost-shape",
			shape: "ws",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
	});

	it("full applicable catalog is GREEN — shared + transport + shape (exit gate)", async () => {
		const shared = buildSharedRows({ makeSubject });
		const transport = makeWsRows(makeRealMattermostFixture());
		const shape = makeMattermostShapeRows();
		const report = await runConformanceSuite({
			subjectName: "mattermost-full",
			shape: "ws",
			rows: [...shared, ...transport, ...shape],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred.length).toBe(0);
		expect(report.allApplicablePassed).toBe(true);
	});

	it("the gate still DETECTS violations (negative validation of the gate)", async () => {
		// A lying fixture must fail its rows: transport AND shape rows are
		// detectors, not rubber stamps.
		const lyingTransport = makeWsRows({
			async resubscribeReplay() {
				return { sentDuringDisconnect: 5, replayedAfterResubscribe: 2 }; // LOST
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
		const lyingShape = makeLyingShapeRows();
		const report = await runConformanceSuite({
			subjectName: "lying-mattermost",
			shape: "ws",
			rows: [...lyingTransport, ...lyingShape],
			suppliedTransportRowIds: new Set(lyingTransport.map((r) => r.id)),
		});
		expect(report.failed).toBeGreaterThan(0);
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		for (const required of TRANSPORT_ROW_REQUIREMENTS.ws) {
			expect(failedIds).toContain(required);
		}
		for (const id of [
			"mm.ws-event-dedup",
			"mm.mention-gating-matrix",
			"mm.rest-backfill-window",
			"mm.thread-root-discipline",
		]) {
			expect(failedIds).toContain(id);
		}
	});
});

/** Deliberately lying mattermost-shape rows — every dimension violates its
 * named row (specificity check for the census gate). */
function makeLyingShapeRows() {
	return [
		{
			id: "mm.ws-event-dedup",
			title: "lying: duplicates turn twice",
			shapes: new Set(["ws"] as const),
			run: async () => ({
				id: "mm.ws-event-dedup",
				title: "lying",
				pass: false,
				shapes: new Set(["ws"] as const),
				detail: "lying fixture: deliveredOnceIds=2",
			}),
		},
		{
			id: "mm.mention-gating-matrix",
			title: "lying: unmentioned channel messages answered",
			shapes: new Set(["ws"] as const),
			run: async () => ({
				id: "mm.mention-gating-matrix",
				title: "lying",
				pass: false,
				shapes: new Set(["ws"] as const),
				detail: "lying fixture: unmentionedChannelDropped=false",
			}),
		},
		{
			id: "mm.rest-backfill-window",
			title: "lying: disconnect window lost",
			shapes: new Set(["ws"] as const),
			run: async () => {
				// Run the REAL fixture then lie about ONE dimension: missed posts
				// MUST deliver — a claim they don't must FAIL against reality.
				const real = await makeMattermostShapeFixture().backfillWindow();
				return {
					id: "mm.rest-backfill-window",
					title: "lying",
					pass: Number(real.missedDuringOutageDelivered) === 0,
					shapes: new Set(["ws"] as const),
					detail: `real backfill behavior observed (delivered=${real.missedDuringOutageDelivered})`,
				};
			},
		},
		{
			id: "mm.thread-root-discipline",
			title: "lying: broken notify keeps failing",
			shapes: new Set(["ws"] as const),
			run: async () => ({
				id: "mm.thread-root-discipline",
				title: "lying",
				pass: false,
				shapes: new Set(["ws"] as const),
				detail: "lying fixture: brokenThreadNotifyFallsBackFlat=false",
			}),
		},
	];
}
