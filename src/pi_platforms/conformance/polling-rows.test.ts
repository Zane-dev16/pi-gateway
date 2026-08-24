// pi_platforms/conformance/polling-rows.test.ts — SUITE WIRING for the
// polling reference adapter (04 §8 merge gate; DEC-002). The polling agent
// supplies ONLY this wiring: shared rows run against the polling subject,
// and the four §3.1 transport rows run against the REAL engine fixture
// (makePollingRows → makeRealPollingFixture) — no stubbed return values.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import { makePollingRows } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { runConformanceSuite } from "./runner.js";
import { TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import { FakeTelegramServer } from "../polling/fake-server.js";
import { makeRealPollingFixture } from "../polling/fixture.js";
import { makePollingSubject } from "../polling/subject.js";

function makeSubject(
	opts: {
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		withSecret?: boolean | undefined;
		name?: string | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	const subject = makePollingSubject({
		wire: new FakePlatformWire(),
		tg: new FakeTelegramServer(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
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

describe("conformance suite — polling reference adapter (Phase 3)", () => {
	it("passes EVERY currently-encoded shared row for the polling shape", async () => {
		const rows = buildSharedRows({ makeSubject });
		expect(rows.length).toBeGreaterThanOrEqual(20);
		const report = await runConformanceSuite({
			subjectName: "polling-reference",
			shape: "polling",
			rows,
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.passed).toBe(rows.length);
	});

	it("passes ALL FOUR §3.1 transport rows against the REAL engine fixture", async () => {
		const rows = makePollingRows(makeRealPollingFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.polling);
		const report = await runConformanceSuite({
			subjectName: "polling-reference-transport",
			shape: "polling",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.deferred).toEqual([]);
	});

	it("full applicable catalog is GREEN — no deferred hooks remain", async () => {
		const shared = buildSharedRows({ makeSubject });
		const transport = makePollingRows(makeRealPollingFixture());
		const report = await runConformanceSuite({
			subjectName: "polling-reference-full",
			shape: "polling",
			rows: [...shared, ...transport],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		expect(report.failed).toBe(0);
		expect(report.deferred.length).toBe(0);
		expect(report.allApplicablePassed).toBe(true);
	});

	it("the gate still DETECTS violations (negative validation of the gate)", async () => {
		// A lying fixture must fail its row: the transport rows are detectors,
		// not rubber stamps (Phase-3 exit criterion parity).
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
			subjectName: "lying-polling",
			shape: "polling",
			rows: lying,
			suppliedTransportRowIds: new Set(lying.map((r) => r.id)),
		});
		expect(report.failed).toBeGreaterThan(0);
	});
});
