// Behavior contracts for cron delivery (07 §5.2 delivery-isolation +
// mirror carve-out rows): wrap ON by default with the exact frame; isolation
// default = NO transcript writes anywhere; mirror strictly opt-in and only
// for the ORIGIN conversation, carrying the CLEANED output.

import { describe, expect, it } from "vitest";

import {
	applyWrap,
	cleanedCronOutput,
	deliverCronResult,
	matchesOrigin,
	resolveMirrorEnabled,
	wrapCronResponse,
	type DeliverySink,
	type MirrorAppender,
} from "./delivery.js";
import type { CronDeliveryTarget } from "./store.js";

const TARGET: CronDeliveryTarget = { platform: "telegram", chatId: "42" };
const ORIGIN: CronDeliveryTarget = {
	platform: "telegram",
	chatId: "42",
	threadId: "7",
};
const TARGET_WITH_THREAD: CronDeliveryTarget = {
	platform: "telegram",
	chatId: "42",
	threadId: "7",
};

function recordingSink(errors: string[] = []): {
	sink: DeliverySink;
	sent: Array<{ target: CronDeliveryTarget; content: string }>;
} {
	const sent: Array<{ target: CronDeliveryTarget; content: string }> = [];
	return {
		sent,
		sink: {
			async deliver(target, content) {
				sent.push({ target, content });
				return errors.shift() ?? null;
			},
		},
	};
}

function recordingAppender(): {
	appender: MirrorAppender;
	appends: Array<{ sessionId: string; text: string }>;
} {
	const appends: Array<{ sessionId: string; text: string }> = [];
	return {
		appends,
		appender: {
			async appendAssistantTurn(sessionId, text) {
				appends.push({ sessionId, text });
				return true;
			},
		},
	};
}

describe("wrapCronResponse — exact frame parity", () => {
	it("produces the byte-exact Hermes header/footer frame", () => {
		expect(wrapCronResponse("daily brief", "abc123", "All good.")).toBe(
			"Cronjob Response: daily brief\n" +
				"(job_id: abc123)\n" +
				"-------------\n\n" +
				"All good.\n\n" +
				'To stop or manage this job, send me a new message (e.g. "stop reminder daily brief").',
		);
	});

	it("wrap defaults ON; cron.wrap_response:false opts out cleanly", () => {
		expect(applyWrap("j", "id", "body")).toContain("Cronjob Response:");
		expect(applyWrap("j", "id", "body", { wrapResponse: false })).toBe("body");
	});
});

describe("resolveMirrorEnabled precedence (first decisive value wins)", () => {
	it("per-job attach_to_session wins over the global knob, both directions", () => {
		expect(
			resolveMirrorEnabled(
				{ attachToSession: true },
				{ mirrorDelivery: false },
			),
		).toBe(true);
		expect(
			resolveMirrorEnabled(
				{ attachToSession: false },
				{ mirrorDelivery: true },
			),
		).toBe(false);
	});

	it("global cron.mirror_delivery applies when per-job unset; default is OFF", () => {
		expect(resolveMirrorEnabled({}, { mirrorDelivery: true })).toBe(true);
		expect(resolveMirrorEnabled({})).toBe(false);
	});
});

describe("delivery isolation default vs mirror opt-in", () => {
	const job = { id: "job1", name: "nightly" };

	it("DEFAULT: wrapped output reaches targets; NO transcript append anywhere (silent)", async () => {
		const { sink, sent } = recordingSink();
		const { appender, appends } = recordingAppender();
		const report = await deliverCronResult({
			job,
			outputText: "the payload",
			targets: [TARGET],
			sink,
			appender, // present but NOT opted in — must stay unused
			origin: ORIGIN,
			originSessionId: "sess-origin",
		});
		expect(sent).toHaveLength(1);
		expect(sent[0]!.content).toContain("Cronjob Response: nightly");
		expect(report.deliveryErrors).toEqual([]);
		expect(appends).toEqual([]); // isolation default: nothing mirrored
		expect(report.mirrored).toBe(false);
	});

	it("MIRROR OPT-IN (attach_to_session): CLEANED copy appended to origin exactly once", async () => {
		const { sink, sent } = recordingSink();
		const { appender, appends } = recordingAppender();
		const report = await deliverCronResult({
			job: { ...job, attachToSession: true },
			outputText: "  clean payload  ",
			targets: [TARGET_WITH_THREAD],
			sink,
			appender,
			origin: ORIGIN,
			originSessionId: "sess-origin",
		});
		// Platform lane still gets the WRAPPED content.
		expect(sent[0]!.content).toContain("Cronjob Response:");
		// Transcript gets the CLEANED output only.
		expect(report.mirrored).toBe(true);
		expect(appends).toHaveLength(1);
		expect(appends[0]).toEqual({
			sessionId: "sess-origin",
			text: "clean payload",
		});
	});

	it("mirror scope is ORIGIN-only: fan-out targets are never mirrored", async () => {
		const { sink } = recordingSink();
		const { appender, appends } = recordingAppender();
		await deliverCronResult({
			job: { ...job, attachToSession: true },
			outputText: "payload",
			targets: [{ platform: "slack", chatId: "other-chat" }],
			sink,
			appender,
			origin: ORIGIN,
			originSessionId: "sess-origin",
		});
		expect(appends).toEqual([]);
	});

	it("missing appender/origin degrades to NOT mirroring (never misdelivery)", async () => {
		const { sink, sent } = recordingSink();
		const report = await deliverCronResult({
			job: { ...job, attachToSession: true }, // opted in…
			outputText: "payload",
			targets: [TARGET],
			sink, // …but no appender wired
			origin: ORIGIN,
			originSessionId: "sess-origin",
		});
		expect(sent).toHaveLength(1); // delivery itself unaffected
		expect(report.mirrored).toBe(false);
	});

	it("sink failures surface as delivery errors, distinct from run success", async () => {
		const { sink } = recordingSink(["telegram 502"]);
		const report = await deliverCronResult({
			job,
			outputText: "p",
			targets: [TARGET],
			sink,
		});
		expect(report.deliveryErrors).toEqual(["telegram 502"]);
	});
});

describe("origin matching + cleaned output helpers", () => {
	it("matchesOrigin compares platform+chat+thread with thread-null tolerance", () => {
		expect(matchesOrigin(ORIGIN, TARGET_WITH_THREAD)).toBe(true);
		expect(matchesOrigin(ORIGIN, TARGET)).toBe(false); // thread differs
		expect(matchesOrigin(null, TARGET)).toBe(false);
	});

	it("cleanedCronOutput trims (mirror text parity)", () => {
		expect(cleanedCronOutput("\n  hi \n")).toBe("hi");
	});
});
