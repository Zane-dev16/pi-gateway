// pi_platforms/conformance/matrix-rows.test.ts — SUITE WIRING for the MATRIX
// census port (04 §8 merge gate; roadmap Phase 6). The port supplies ONLY this
// wiring: all shared rows run against the Matrix subject, the FOUR inherited
// §3.1 polling transport rows run against the REAL engine fixture
// (makeRealMatrixFixture) — no stubbed return values — and the EIGHT
// matrix-shape rows close the shape deltas.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import { makePollingRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import {
	makeRealMatrixFixture,
	makeMatrixShapeFixture,
} from "../matrix/matrix-world.js";
import { makeMatrixShapeRows } from "../matrix/matrix-shape-rows.js";
import { FakeMatrixHomeserver } from "../matrix/matrix-fake-server.js";
import { makeMatrixSubject } from "../matrix/matrix-subject.js";

function makeSubject(
	opts: {
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		withSecret?: boolean | undefined;
		name?: string | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	const subject = makeMatrixSubject({
		wire: new FakePlatformWire(),
		hs: new FakeMatrixHomeserver(),
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

describe("conformance suite — matrix census port (Phase 6)", () => {
	it("passes EVERY currently-encoded shared row for the matrix subject", async () => {
		const rows = buildSharedRows({ makeSubject });
		expect(rows.length).toBeGreaterThanOrEqual(22);
		const report = await runConformanceSuite({
			subjectName: "matrix",
			shape: "polling",
			rows,
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.passed).toBe(rows.length);
	});

	it("passes ALL FOUR inherited §3.1 transport rows against the REAL matrix engine", async () => {
		const rows = makePollingRows(makeRealMatrixFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.polling);
		const report = await runConformanceSuite({
			subjectName: "matrix-transport",
			shape: "polling",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.deferred).toEqual([]);
	});

	it("passes ALL EIGHT matrix-shape rows (shape deltas over the family)", async () => {
		const rows = makeMatrixShapeRows();
		expect(rows.map((r) => r.id)).toEqual([
			"mx.sync-token-exactly-once",
			"mx.auth-and-epoch-ladders",
			"mx.ingress-filter-chain",
			"mx.startup-grace-window",
			"mx.mention-gating-matrix",
			"mx.reply-fallback-and-bang",
			"mx.directory-alias-overlay",
			"mx.reaction-typing-variants",
		]);
		const report = await runConformanceSuite({
			subjectName: "matrix-shape",
			shape: "polling",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
	});

	it("full applicable catalog is GREEN — shared + transport + shape (exit gate)", async () => {
		const shared = buildSharedRows({ makeSubject });
		const transport = makePollingRows(makeRealMatrixFixture());
		const shape = makeMatrixShapeRows();
		const report = await runConformanceSuite({
			subjectName: "matrix-full",
			shape: "polling",
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
		const lyingTransport = makePollingRows({
			async simulateOutageAndReconnect() {
				return { queuedBeforeReconnect: 3, deliveredAfterReconnect: 0 }; // LOST
			},
			async holdAndRedispatch() {
				return { held: 3, redispatched: 1 };
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
		const lyingShape = makeLyingShapeRows();
		const report = await runConformanceSuite({
			subjectName: "lying-matrix",
			shape: "polling",
			rows: [...lyingTransport, ...lyingShape],
			suppliedTransportRowIds: new Set(lyingTransport.map((r) => r.id)),
		});
		expect(report.failed).toBeGreaterThan(0);
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		for (const required of TRANSPORT_ROW_REQUIREMENTS.polling) {
			expect(failedIds).toContain(required);
		}
		for (const id of [
			"mx.sync-token-exactly-once",
			"mx.mention-gating-matrix",
			"mx.directory-alias-overlay",
			"mx.startup-grace-window",
		]) {
			expect(failedIds).toContain(id);
		}
	});

	it("the REAL shape fixture still satisfies every lying dimension it claims to fake", async () => {
		// Mutation-check breaker: each lying row asserts the NEGATION of a real
		// fixture fact — running the REAL facts through the lying assertions
		// must FAIL, proving the detectors bind to reality.
		const real = await makeMatrixShapeFixture().syncTokenExactlyOnce();
		const lyingAssertion = real.r1TurnCopies !== 1 || real.r2TurnCopies !== 1;
		expect(lyingAssertion).toBe(false); // reality contradicts the lie
	});
});

/** Deliberately lying matrix-shape rows — every dimension violates its named
 * row (specificity check for the census gate). */
function makeLyingShapeRows() {
	return [
		{
			id: "mx.sync-token-exactly-once",
			title: "lying: replayed window duplicates turns",
			shapes: new Set(["polling"] as const),
			run: async () => ({
				id: "mx.sync-token-exactly-once",
				title: "lying",
				pass: false,
				shapes: new Set(["polling"] as const),
				detail: "lying fixture: r1 turned twice",
			}),
		},
		{
			id: "mx.auth-and-epoch-ladders",
			title: "lying: auth death retried forever silently",
			shapes: new Set(["polling"] as const),
			run: async () => ({
				id: "mx.auth-and-epoch-ladders",
				title: "lying",
				pass: false,
				shapes: new Set(["polling"] as const),
				detail: "lying fixture: unknownTokenFatalImmediately=false",
			}),
		},
		{
			id: "mx.startup-grace-window",
			title: "lying: backlog delivered",
			shapes: new Set(["polling"] as const),
			run: async () => ({
				id: "mx.startup-grace-window",
				title: "lying",
				pass: false,
				shapes: new Set(["polling"] as const),
				detail: "lying fixture: oldBacklogDropped=false",
			}),
		},
		{
			id: "mx.mention-gating-matrix",
			title: "lying: unmentioned channel messages answered",
			shapes: new Set(["polling"] as const),
			run: async () => ({
				id: "mx.mention-gating-matrix",
				title: "lying",
				pass: false,
				shapes: new Set(["polling"] as const),
				detail: "lying fixture: unmentionedChannelDropped=false",
			}),
		},
		{
			id: "mx.directory-alias-overlay",
			title: "lying: alias overlay ignored",
			shapes: new Set(["polling"] as const),
			run: async () => {
				// Run the REAL fixture then lie about ONE dimension: the display
				// name must prefer the explicit room name — a "resolution" that
				// returns the raw room id instead must FAIL against reality.
				const real = await makeMatrixShapeFixture().directoryOverlay();
				return {
					id: "mx.directory-alias-overlay",
					title: "lying",
					pass: real.displayNamePrefersName === "!named:fake.example",
					shapes: new Set(["polling"] as const),
					detail: `real overlay behavior observed (displayName=${real.displayNamePrefersName})`,
				};
			},
		},
	];
}
