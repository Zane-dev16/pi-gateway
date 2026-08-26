// Telegram TEXT follow-up grace on the busy ladder (03-message-routing §6;
// gateway/run.py:_handle_message HERMES_TELEGRAM_FOLLOWUP_GRACE_SECONDS block):
// arrivals within the grace window of RUN START queue/merge WITHOUT
// interrupting — ahead of the interrupt demotion and even ahead of steer.

import { describe, expect, it } from "vitest";
import type { IncomingEvent } from "./events.js";
import type { CommandDef } from "./busy-policy.js";
import {
	DEFAULT_FOLLOWUP_GRACE_SECONDS,
	TELEGRAM_FOLLOWUP_GRACE_SECONDS_ENV,
	RunnerBusyGuard,
	resolveFollowupGraceSeconds,
	type RunnerBusyOptions,
} from "./runner-busy.js";

const REGISTRY: CommandDef[] = [
	{ name: "stop", busyPolicy: "interrupt_then_dispatch", busyHandler: "stop" },
];

const TELEGRAM_KEY = "agent:main:telegram:dm:100";
const SLACK_KEY = "agent:main:slack:dm:200";

function telegramText(text: string): IncomingEvent {
	return {
		messageType: "text",
		text,
		source: { platform: "telegram", chatType: "dm" },
	};
}

function makeRunner(extra: Partial<RunnerBusyOptions> = {}): {
	runner: RunnerBusyGuard;
	slots: Map<string, IncomingEvent>;
} {
	const slots = new Map<string, IncomingEvent>();
	const runner = new RunnerBusyGuard({
		registry: REGISTRY,
		slots,
		...extra,
	});
	return { runner, slots };
}

describe("env bridge (HERMES_* names ported verbatim)", () => {
	it("defaults to 3.0s; parses overrides; garbage fails safe to the default", () => {
		expect(TELEGRAM_FOLLOWUP_GRACE_SECONDS_ENV).toBe(
			"HERMES_TELEGRAM_FOLLOWUP_GRACE_SECONDS",
		);
		expect(DEFAULT_FOLLOWUP_GRACE_SECONDS).toBe(3.0);
		expect(resolveFollowupGraceSeconds({})).toBe(3.0);
		expect(
			resolveFollowupGraceSeconds({
				[TELEGRAM_FOLLOWUP_GRACE_SECONDS_ENV]: "1.5",
			}),
		).toBe(1.5);
		expect(
			resolveFollowupGraceSeconds({
				[TELEGRAM_FOLLOWUP_GRACE_SECONDS_ENV]: "0",
			}),
		).toBe(0);
		// Python float() would raise; the gate fails safe instead (deviation).
		expect(
			resolveFollowupGraceSeconds({
				[TELEGRAM_FOLLOWUP_GRACE_SECONDS_ENV]: "banana",
			}),
		).toBe(3.0);
	});
});

describe("Telegram TEXT follow-up grace (run.py _handle_message)", () => {
	it("arrival within grace of run start MERGES into the head slot without interrupting, even in interrupt mode", () => {
		const now = 1_000_000;
		const { runner, slots } = makeRunner({ now: () => now });
		runner.markTurnStarted(TELEGRAM_KEY, now - 10);
		slots.set(TELEGRAM_KEY, telegramText("first half"));

		const disposition = runner.handlePlainTextFollowUp(
			TELEGRAM_KEY,
			telegramText("second half"),
		);

		expect(disposition).toBe("queued"); // queued WITHOUT interrupt demotion noise
		// merge_text parity: appended to the head slot with "\n", not replaced.
		expect(slots.get(TELEGRAM_KEY)?.text).toBe("first half\nsecond half");
	});

	it("steer mode does NOT steer during the grace window (grace precedes steer)", () => {
		const now = 1_000_000;
		let steered: string | null = null;
		const { runner, slots } = makeRunner({
			busyInputMode: "steer",
			now: () => now,
			steer: (text) => {
				steered = text;
				return true;
			},
		});
		runner.markTurnStarted(TELEGRAM_KEY, now - 500);
		slots.set(TELEGRAM_KEY, telegramText("head"));

		expect(
			runner.handlePlainTextFollowUp(TELEGRAM_KEY, telegramText("graced")),
		).toBe("queued");
		expect(steered).toBeNull();
		expect(slots.get(TELEGRAM_KEY)?.text).toBe("head\ngraced");
	});

	it("queue mode enqueues a DISTINCT turn within the grace window (no merge)", () => {
		const now = 1_000_000;
		const { runner, slots } = makeRunner({
			busyInputMode: "queue",
			now: () => now,
		});
		runner.markTurnStarted(TELEGRAM_KEY, now - 2_900); // 2.9s < 3.0s
		runner.handlePlainTextFollowUp(TELEGRAM_KEY, telegramText("own-turn-1"));
		runner.handlePlainTextFollowUp(TELEGRAM_KEY, telegramText("own-turn-2"));

		// FIFO parity of _enqueue_fifo inside the grace branch:
		expect(slots.get(TELEGRAM_KEY)?.text).toBe("own-turn-1");
		expect(runner.overflowOf(TELEGRAM_KEY).map((e) => e.text)).toEqual([
			"own-turn-2",
		]);
	});

	it("arrivals BEYOND the grace window take the normal ladder (FIFO queue, not merge)", () => {
		const now = 1_000_000;
		const { runner, slots } = makeRunner({ now: () => now });
		runner.markTurnStarted(TELEGRAM_KEY, now - 3_500); // > 3.0s

		slots.set(TELEGRAM_KEY, telegramText("head"));
		runner.handlePlainTextFollowUp(TELEGRAM_KEY, telegramText("late"));

		// Not graced → the slot is NOT text-merged; the event FIFOs behind it.
		expect(slots.get(TELEGRAM_KEY)?.text).toBe("head");
		expect(runner.overflowOf(TELEGRAM_KEY).map((e) => e.text)).toEqual([
			"late",
		]);
	});

	it("markTurnFinished closes the window", () => {
		const now = 1_000_000;
		const { runner, slots } = makeRunner({ now: () => now });
		runner.markTurnStarted(TELEGRAM_KEY, now - 10);
		runner.markTurnFinished(TELEGRAM_KEY);

		slots.set(TELEGRAM_KEY, telegramText("head"));
		runner.handlePlainTextFollowUp(TELEGRAM_KEY, telegramText("after end"));
		expect(slots.get(TELEGRAM_KEY)?.text).toBe("head"); // not merged
		expect(runner.overflowOf(TELEGRAM_KEY).map((e) => e.text)).toEqual([
			"after end",
		]);
	});

	it("only TELEGRAM text events are graced; slack or photos are not", () => {
		const now = 1_000_000;
		const { runner, slots } = makeRunner({ now: () => now });
		runner.markTurnStarted(SLACK_KEY, now - 5);

		// Slack TEXT at +5ms: not the Telegram lane → normal ladder.
		slots.set(SLACK_KEY, { messageType: "text", text: "head" });
		runner.handlePlainTextFollowUp(SLACK_KEY, {
			messageType: "text",
			text: "slack follow-up",
			source: { platform: "slack", chatType: "dm" },
		});
		expect(slots.get(SLACK_KEY)?.text).toBe("head"); // not merged
		expect(runner.overflowOf(SLACK_KEY).map((e) => e.text)).toEqual([
			"slack follow-up",
		]);

		// Telegram PHOTO at +5ms: photo priority path is separate (§3.1).
		runner.markTurnStarted(TELEGRAM_KEY, now - 5);
		slots.set(TELEGRAM_KEY, telegramText("head"));
		runner.handlePlainTextFollowUp(TELEGRAM_KEY, {
			messageType: "photo",
			mediaUrls: ["/x.png"],
			source: { platform: "telegram", chatType: "dm" },
		});
		// Not merged as text-grace; replace/merge table handled it instead.
		expect(slots.get(TELEGRAM_KEY)?.messageType).toBe("photo");
	});

	it("a session with no recorded run start is never graced (falsy started_at parity)", () => {
		const now = 1_000_000;
		const { runner, slots } = makeRunner({ now: () => now });
		slots.set(TELEGRAM_KEY, telegramText("head"));
		runner.handlePlainTextFollowUp(TELEGRAM_KEY, telegramText("cold"));
		expect(slots.get(TELEGRAM_KEY)?.text).toBe("head"); // not merged
		expect(runner.overflowOf(TELEGRAM_KEY).map((e) => e.text)).toEqual([
			"cold",
		]);
	});

	it("grace <= 0 disables the window entirely", () => {
		const now = 1_000_000;
		const { runner, slots } = makeRunner({
			now: () => now,
			followupGraceSeconds: 0,
		});
		runner.markTurnStarted(TELEGRAM_KEY, now - 1);
		slots.set(TELEGRAM_KEY, telegramText("head"));
		runner.handlePlainTextFollowUp(TELEGRAM_KEY, telegramText("instant"));
		expect(slots.get(TELEGRAM_KEY)?.text).toBe("head"); // not merged
		expect(runner.overflowOf(TELEGRAM_KEY).map((e) => e.text)).toEqual([
			"instant",
		]);
	});

	it("the guard resolves the default grace from the env bridge when unset", () => {
		const now = 1_000_000;
		const { runner, slots } = makeRunner({
			now: () => now,
			env: { [TELEGRAM_FOLLOWUP_GRACE_SECONDS_ENV]: "0.05" },
		});
		runner.markTurnStarted(TELEGRAM_KEY, now - 60); // 60ms > 50ms env grace
		slots.set(TELEGRAM_KEY, telegramText("head"));
		runner.handlePlainTextFollowUp(TELEGRAM_KEY, telegramText("expired"));
		expect(runner.overflowOf(TELEGRAM_KEY).map((e) => e.text)).toEqual([
			"expired",
		]);

		runner.markTurnStarted(TELEGRAM_KEY, now - 10); // within 50ms
		runner.handlePlainTextFollowUp(TELEGRAM_KEY, telegramText("graced"));
		expect(slots.get(TELEGRAM_KEY)?.text).toBe("head\ngraced");
	});
});
