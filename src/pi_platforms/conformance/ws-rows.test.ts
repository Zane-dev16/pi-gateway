// pi_platforms/conformance/ws-rows.test.ts — SUITE WIRING for the
// persistent-ws reference adapter (04 §8 merge gate; DEC-032). The ws agent
// supplies ONLY this wiring: all shared rows (including the DEC-033
// log-redaction row) run against the ws subject, and the FIVE transport rows
// (two §3 hooks + Retry-After capture + capability latch + DEC-034 dual-path)
// run against the REAL engine fixture (makeWsRows → makeRealWsFixture) — no
// stubbed return values.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import { makeWsRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { makeRealWsFixture } from "../persistent-ws/ws-fixture.js";
import { makeWsSubject } from "../persistent-ws/ws-subject.js";

function makeSubject(
	opts: {
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		withSecret?: boolean | undefined;
		name?: string | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	const subject = makeWsSubject({
		wire: new FakePlatformWire(),
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

describe("conformance suite — persistent-ws reference adapter (Phase 3 close)", () => {
	it("passes EVERY currently-encoded shared row for the ws shape", async () => {
		const rows = buildSharedRows({ makeSubject });
		expect(rows.length).toBeGreaterThanOrEqual(22);
		const report = await runConformanceSuite({
			subjectName: "ws-reference",
			shape: "ws",
			rows,
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.passed).toBe(rows.length);
	});

	it("passes ALL FIVE transport rows against the REAL engine fixture", async () => {
		const rows = makeWsRows(makeRealWsFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "ws-reference-transport",
			shape: "ws",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.deferred).toEqual([]);
	});

	it("full applicable catalog is GREEN — no deferred hooks remain (DEC-032 gate)", async () => {
		const shared = buildSharedRows({ makeSubject });
		const transport = makeWsRows(makeRealWsFixture());
		const report = await runConformanceSuite({
			subjectName: "ws-reference-full",
			shape: "ws",
			rows: [...shared, ...transport],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred.length).toBe(0);
		expect(report.allApplicablePassed).toBe(true);
	});

	it("the gate still DETECTS violations (negative validation of the gate)", async () => {
		// A lying fixture must fail its rows: the transport rows are detectors,
		// not rubber stamps (Phase-3 exit criterion parity).
		const lying = makeWsRows({
			async resubscribeReplay() {
				return { sentDuringDisconnect: 5, replayedAfterResubscribe: 2 }; // LOST events
			},
			async watchdogRecovery() {
				return { detectedDeadSocket: false, resumedWithoutLoss: true };
			},
			async retryAfterCapture() {
				return {
					closeCapturedSeconds: 0, // nothing captured
					nextDelayMs: 1000, // exponential default, NOT the capture
					delayAuthoritative: false,
					restCapturedSeconds: 3,
				};
			},
			async capabilityLatchPermanence() {
				return {
					latchedOnFirstFailure: true,
					latchCount: 4, // re-latching every attempt
					wireAttemptsAfterSkip: 9, // wire still hammered post-latch
					supportsStreamingFalse: false,
					transientDidNotLatch: false,
				};
			},
			async dualPathMarkdown() {
				return {
					nativeRawByteExact: false, // native path CONVERTED (§10.2 violation)
					nativePrefixStable: true,
					restConvertedBold: false,
					restConvertedLink: true,
					restConvertedTable: true,
					linkPreviewOnAllTextSends: false, // flag missing on text sends
					linkPreviewAbsentOffTextSends: true,
				};
			},
		});
		const report = await runConformanceSuite({
			subjectName: "lying-ws",
			shape: "ws",
			rows: lying,
			suppliedTransportRowIds: new Set(lying.map((r) => r.id)),
		});
		expect(report.failed).toBeGreaterThan(0);
		// Each lying dimension is caught by its OWN named row (specificity).
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("transport.ws.resubscribe-replay");
		expect(failedIds).toContain("transport.ws.heartbeat-watchdog-recovery");
		expect(failedIds).toContain("transport.ws.retry-after-capture");
		expect(failedIds).toContain("transport.ws.capability-latch-permanent");
		expect(failedIds).toContain("transport.ws.dual-path-markdown");
	});
});
