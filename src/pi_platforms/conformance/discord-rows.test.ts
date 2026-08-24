// pi_platforms/conformance/discord-rows.test.ts — SUITE WIRING for the
// Discord census port (04 §8 merge gate; roadmap Phase 6). The port supplies
// ONLY this wiring: all shared rows run against the Discord subject, and the
// FIVE inherited persistent-ws transport rows run against the REAL engine
// fixture (makeRealDiscordFixture) — no stubbed return values.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import { makeWsRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import { makeRealDiscordFixture } from "../discord/discord-fixture.js";
import { makeDiscordSubject } from "../discord/discord-subject.js";

function makeSubject(
	opts: {
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		withSecret?: boolean | undefined;
		name?: string | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	const subject = makeDiscordSubject({
		wire: new FakePlatformWire(),
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

describe("conformance suite — Discord adapter (Phase 6 census port)", () => {
	it("passes EVERY currently-encoded shared row for the discord subject", async () => {
		const rows = buildSharedRows({ makeSubject });
		expect(rows.length).toBeGreaterThanOrEqual(23);
		const report = await runConformanceSuite({
			subjectName: "discord",
			shape: "ws",
			rows,
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.passed).toBe(rows.length);
	});

	it("passes ALL FIVE inherited ws transport rows against the REAL engine fixture", async () => {
		const rows = makeWsRows(makeRealDiscordFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "discord-transport",
			shape: "ws",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.deferred).toEqual([]);
	});

	it("full applicable catalog is GREEN — no deferred hooks remain (exit gate)", async () => {
		const shared = buildSharedRows({ makeSubject });
		const transport = makeWsRows(makeRealDiscordFixture());
		const report = await runConformanceSuite({
			subjectName: "discord-full",
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
			subjectName: "lying-discord",
			shape: "ws",
			rows: lying,
			suppliedTransportRowIds: new Set(lying.map((r) => r.id)),
		});
		expect(report.failed).toBeGreaterThan(0);
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("transport.ws.resubscribe-replay");
		expect(failedIds).toContain("transport.ws.heartbeat-watchdog-recovery");
		expect(failedIds).toContain("transport.ws.retry-after-capture");
		expect(failedIds).toContain("transport.ws.capability-latch-permanent");
		expect(failedIds).toContain("transport.ws.dual-path-markdown");
	});
});
