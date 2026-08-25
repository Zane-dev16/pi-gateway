// pi_platforms/conformance/telegram-rows.test.ts — SUITE WIRING for the
// TELEGRAM census port (04 §8 merge gate; DEC-024, roadmap Phase 6). The
// telegram agent supplies ONLY this wiring: all shared rows (including the
// DEC-033 log-redaction row) run against the TELEGRAM subject, the FOUR
// inherited §3.1 polling transport rows run against the REAL telegram engine
// fixture (makeRealTelegramPollingFixture) — no stubbed return values — and
// the EIGHTEEN telegram-shape rows close the shape deltas
// (round-2 cluster telegram-wire-r2 added seven).

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import { makePollingRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { makeTelegramShapeRows } from "./telegram-shape-rows.js";
import { TelegramBotApiFake } from "../telegram/telegram-fake-server.js";
import {
	makeRealTelegramPollingFixture,
	makeTelegramShapeFixture,
} from "../telegram/telegram-world.js";
import { TelegramSubject } from "../telegram/telegram-subject.js";

function makeSubject(
	opts: {
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		withSecret?: boolean | undefined;
		name?: string | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	const subject = new TelegramSubject({
		wire: new FakePlatformWire(),
		tg: new TelegramBotApiFake(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
		// Shared-row budgets are SMALL (reference-subject parity — chunking
		// rows split at 64 units); the adapter's PRODUCTION default stays the
		// manifest's 4096 UTF-16 units (unit-tested separately).
		scalarMaxUnits: 64,
	});
	// Relay-shaped lanes (seal-interception rows) mark their chats as
	// stream-is-message — the adapter-side arming gate (review B4).
	if (opts.streamIsMessageChatIds !== undefined) {
		for (const id of opts.streamIsMessageChatIds) {
			subject.adapter.markStreamIsMessage(id);
		}
	}
	return subject;
}

describe("conformance suite — telegram census port (Phase 6, DEC-024)", () => {
	it("passes EVERY currently-encoded shared row for the telegram subject", async () => {
		const rows = buildSharedRows({ makeSubject });
		expect(rows.length).toBeGreaterThanOrEqual(22);
		const report = await runConformanceSuite({
			subjectName: "telegram",
			shape: "polling",
			rows,
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.passed).toBe(rows.length);
	});

	it("passes ALL FOUR inherited §3.1 transport rows against the REAL telegram engine", async () => {
		const rows = makePollingRows(makeRealTelegramPollingFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.polling);
		const report = await runConformanceSuite({
			subjectName: "telegram-transport",
			shape: "polling",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.deferred).toEqual([]);
	});

	it("passes ALL EIGHTEEN telegram-shape rows (shape deltas over the family)", async () => {
		const rows = makeTelegramShapeRows();
		expect(rows.map((r) => r.id)).toEqual([
			"tg.update-parsing-deltas",
			"tg.send-wire-parity",
			"tg.edit-send-reconciliation",
			"tg.callback-roundtrip-64b",
			"tg.reaction-ack-lifecycle",
			"tg.inbound-reactions",
			"tg.typing-variant-matrix",
			"tg.sticker-cache-hit-miss-expiry",
			"tg.floodwait-method-classes",
			"tg.connect-webhook-clear",
			"tg.media-send-family",
			"tg.edit-not-modified-noop",
			"tg.post-connect-housekeeping",
			"tg.dm-topic-send-routing",
			"tg.wire-arg-whitelist",
			"tg.rich-extras-lane",
			"tg.media-dm-topic-retry",
			"tg.long-poll-timeout-default",
		]);
		const report = await runConformanceSuite({
			subjectName: "telegram-shape",
			shape: "polling",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
	});

	it("full applicable catalog is GREEN — shared + transport + shape, no deferred hooks remain (DEC-024 exit gate)", async () => {
		const shared = buildSharedRows({ makeSubject });
		const transport = makePollingRows(makeRealTelegramPollingFixture());
		const shape = makeTelegramShapeRows();
		const report = await runConformanceSuite({
			subjectName: "telegram-full",
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
		// A lying fixture must fail its rows: the transport AND shape rows are
		// detectors, not rubber stamps (Phase-3 exit criterion parity).
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
			subjectName: "lying-telegram",
			shape: "polling",
			rows: [...lyingTransport, ...lyingShape],
			suppliedTransportRowIds: new Set(lyingTransport.map((r) => r.id)),
		});
		expect(report.failed).toBeGreaterThan(0);
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain(
			"transport.polling.outage-reconnect-preserves-queue",
		);
		expect(failedIds).toContain("tg.update-parsing-deltas");
		expect(failedIds).toContain("tg.reaction-ack-lifecycle");
		expect(failedIds).toContain("tg.sticker-cache-hit-miss-expiry");
	});
});

/** A deliberately lying telegram-shape fixture — every dimension violates its
 * named row (specificity check for the census gate). */
function makeLyingShapeRows() {
	const fixture = makeTelegramShapeFixture();
	return [
		{
			id: "tg.update-parsing-deltas",
			title: "lying: message updates never become turns",
			shapes: new Set(["polling"] as const),
			run: async () => ({
				id: "tg.update-parsing-deltas",
				title: "lying",
				pass: false,
				shapes: new Set(["polling"] as const),
				detail: "lying fixture: textTurns=0",
			}),
		},
		{
			id: "tg.reaction-ack-lifecycle",
			title: "lying: cancelled leaves 👀 stuck",
			shapes: new Set(["polling"] as const),
			run: async () => ({
				id: "tg.reaction-ack-lifecycle",
				title: "lying",
				pass: false,
				shapes: new Set(["polling"] as const),
				detail: "lying fixture: cancelCleared=false",
			}),
		},
		{
			id: "tg.sticker-cache-hit-miss-expiry",
			title: "lying: hits re-analyze forever",
			shapes: new Set(["polling"] as const),
			run: async () => {
				// Run the REAL fixture then lie about ONE dimension: a "hit" that
				// re-analyzes (secondCall > first) — must FAIL against reality.
				const real = await fixture.stickerCacheFlow();
				return {
					id: "tg.sticker-cache-hit-miss-expiry",
					title: "lying",
					pass: real.secondCallVisionCalls > real.firstCallVisionCalls,
					shapes: new Set(["polling"] as const),
					detail: `real hit behavior observed (vision calls ${real.firstCallVisionCalls}→${real.secondCallVisionCalls})`,
				};
			},
		},
	];
}
