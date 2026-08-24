// Conformance harness SELF-TEST (required contract): a REFERENCE-CORRECT fake
// adapter passes ALL currently-encoded rows. This proves the suite itself;
// real adapters (Phase 3/4) are held to the identical bar. Also asserts the
// shape-requirement catalog reports deferred transport hooks honestly.

import { describe, expect, it } from "vitest";
import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import {
	makeReferenceSubject,
	type ReferenceSubject,
} from "./reference/reference-adapter.js";
import type { ConformanceSubject } from "./harness.js";
import { SCHEDULER_SYMBOL } from "./harness.js";
import { buildSharedRows } from "./rows.js";
import { runConformanceSuite } from "./runner.js";
import { makePollingRows, makeWsRows, makeWebhookRows } from "./shapes.js";

function makeSubject(
	opts: {
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		withSecret?: boolean | undefined;
		name?: string | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	const subject = makeReferenceSubject({
		wire: new FakePlatformWire(),
		streamIsMessageChatIds: opts.streamIsMessageChatIds,
		withSecret: opts.withSecret,
		name: opts.name,
		spawner: scheduler.spawner,
		scheduler,
	});
	return subject;
}

describe("conformance self-test — reference-correct adapter", () => {
	it("passes EVERY currently-encoded shared row", async () => {
		const rows = buildSharedRows({ makeSubject });
		expect(rows.length).toBeGreaterThanOrEqual(20);
		const report = await runConformanceSuite({
			subjectName: "reference",
			shape: "polling",
			rows,
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.passed).toBe(rows.length);
	});

	it("runs green for the webhook shape too (shape filtering holds)", async () => {
		const rows = buildSharedRows({ makeSubject });
		const report = await runConformanceSuite({
			subjectName: "reference-webhook",
			shape: "webhook",
			rows,
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
	});
});

describe("transport-specific named hooks", () => {
	it("deferred hooks are reported honestly until adapter agents supply fixtures", async () => {
		const rows = buildSharedRows({ makeSubject });
		const report = await runConformanceSuite({
			subjectName: "reference",
			shape: "polling",
			rows,
		});
		expect(report.deferred.length).toBe(4); // polling requirements not yet supplied
		expect(report.deferred.map((d) => d.id)).toContain(
			"transport.polling.outage-reconnect-preserves-queue",
		);
	});

	it("fixture-backed polling rows pass against a REFERENCE fixture implementation", async () => {
		const rows = makePollingRows({
			async simulateOutageAndReconnect() {
				return { queuedBeforeReconnect: 5, deliveredAfterReconnect: 5 };
			},
			async holdAndRedispatch() {
				return { held: 3, redispatched: 3 };
			},
			async conflictRecovery() {
				return {
					generationsBumped: 1,
					dropPendingUpdatesOnRestart: true,
					fatalAfterExhaustion: true,
				};
			},
			async heartbeatEscalation() {
				return { stuckProbes: 2, reconnectTriggered: true };
			},
		});
		const report = await runConformanceSuite({
			subjectName: "reference-polling",
			shape: "polling",
			rows,
		});
		expect(report.failed).toBe(0);
	});

	it("ws + webhook hook fixtures encode their contracts and catch violations", async () => {
		const goodWs = makeWsRows({
			async resubscribeReplay() {
				return { sentDuringDisconnect: 7, replayedAfterResubscribe: 7 };
			},
			async watchdogRecovery() {
				return { detectedDeadSocket: true, resumedWithoutLoss: true };
			},
			async retryAfterCapture() {
				return {
					closeCapturedSeconds: 5,
					nextDelayMs: 5000,
					delayAuthoritative: true,
					restCapturedSeconds: 3,
				};
			},
			async capabilityLatchPermanence() {
				return {
					latchedOnFirstFailure: true,
					latchCount: 1,
					wireAttemptsAfterSkip: 1,
					supportsStreamingFalse: true,
					transientDidNotLatch: true,
				};
			},
			async dualPathMarkdown() {
				return {
					nativeRawByteExact: true,
					nativePrefixStable: true,
					restConvertedBold: true,
					restConvertedLink: true,
					restConvertedTable: true,
					linkPreviewOnAllTextSends: true,
					linkPreviewAbsentOffTextSends: true,
				};
			},
		});
		const wsReport = await runConformanceSuite({
			subjectName: "ref-ws",
			shape: "ws",
			rows: goodWs,
		});
		expect(wsReport.failed).toBe(0);

		// A VIOLATING fixture must fail its row (the rows are real detectors).
		const badWebhook = makeWebhookRows({
			async boundedWindowAnswer() {
				return { answeredWithinWindowMs: 9000, windowCapMs: 5000 };
			},
			async flagsAndTrust() {
				return {
					interactiveResumeFalse: true,
					supportsAsyncDeliveryFalse: false, // violation
					trustBoundaryComplete: true,
				};
			},
		});
		const badReport = await runConformanceSuite({
			subjectName: "bad-webhook",
			shape: "webhook",
			rows: badWebhook,
		});
		expect(badReport.failed).toBe(2);
	});
});

describe("fresh-subject isolation", () => {
	it("rows receive independent subjects (no cross-row latch leakage)", () => {
		const a = makeSubject();
		const b = makeSubject();
		expect(a).not.toBe(b);
		expect(a.wire.ops.length).toBe(0);
		expect(b.wire.ops.length).toBe(0);
	});

	it("subjects carry the deterministic scheduler under the harness symbol", () => {
		const s = makeSubject();
		const sched = (s as unknown as Record<symbol, ManualScheduler>)[
			SCHEDULER_SYMBOL
		];
		expect(sched).toBeInstanceOf(ManualScheduler);
		void (s as ReferenceSubject);
	});
});
