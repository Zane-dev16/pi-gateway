// Behavior contracts for the schedule engine (07 §5.1 formats; 07 §5.2
// catchup clamp + one-shot grace). All time via fixed instants — no wall
// clock anywhere.

import { describe, expect, it } from "vitest";

import {
	CATCHUP_GRACE_MAX_SECONDS,
	CATCHUP_GRACE_MIN_SECONDS,
	ONESHOT_GRACE_SECONDS,
	catchupGraceSeconds,
	computeNextRun,
	cronNextAfter,
	parseDuration,
	parseSchedule,
	recoverableOneshotRunAt,
	scheduleCadenceSeconds,
	setNowAnchorForTests,
} from "./schedule.js";

const T0 = 1_770_000_000; // arbitrary fixed epoch second

describe("parseDuration", () => {
	it("parses m/h/d and aliases (parity of parse_duration)", () => {
		expect(parseDuration("30m")).toBe(30);
		expect(parseDuration("2h")).toBe(120);
		expect(parseDuration("1d")).toBe(1440);
		expect(parseDuration("45 min")).toBe(45);
		expect(parseDuration("2 hours")).toBe(120);
		expect(parseDuration("3days")).toBe(4320);
	});

	it("rejects garbage with an actionable message", () => {
		expect(() => parseDuration("weekly")).toThrow(/Invalid duration/);
	});
});

describe("parseSchedule", () => {
	it("maps 'every X' to a recurring interval", () => {
		const s = parseSchedule("every 30m");
		expect(s).toEqual({ kind: "interval", minutes: 30 });
	});

	it("maps a 5-field cron expression", () => {
		const s = parseSchedule("0 9 * * *");
		expect(s).toEqual({ kind: "cron", expr: "0 9 * * *" });
	});

	it("rejects malformed cron expressions at parse time", () => {
		expect(() => parseSchedule("99 9 * * *")).toThrow(/[Ii]nvalid cron/);
	});

	it("maps ISO timestamps to one-shots", () => {
		const s = parseSchedule("2026-02-03T14:00:00Z");
		expect(s.kind).toBe("once");
		if (s.kind === "once")
			expect(s.runAtSeconds).toBe(Date.parse("2026-02-03T14:00:00Z") / 1000);
	});

	it("anchors bare durations as one-shots from the injected anchor", () => {
		setNowAnchorForTests(() => T0);
		try {
			const s = parseSchedule("30m");
			expect(s).toEqual({ kind: "once", runAtSeconds: T0 + 30 * 60 });
		} finally {
			setNowAnchorForTests(() => Date.now() / 1000);
		}
	});

	it("rejects weekday phrases with the duration-parse error (Hermes parity)", () => {
		// Hermes' parse_schedule hits the same shape: "every monday 9am" →
		// parse_duration raises ValueError('Invalid duration: ...') directly.
		expect(() => parseSchedule("every monday 9am")).toThrow(/Invalid duration/);
	});
});

describe("computeNextRun — once grace semantics", () => {
	it("returns the ORIGINAL run_at while within the 120s grace window", () => {
		const runAt = T0 - 119;
		expect(recoverableOneshotRunAt(runAt, T0)).toBe(runAt);
	});

	it("returns null once past the grace boundary (just-outside skips)", () => {
		const runAt = T0 - (ONESHOT_GRACE_SECONDS + 1);
		expect(recoverableOneshotRunAt(runAt, T0)).toBeNull();
	});

	it("is permanently ineligible after it has run", () => {
		expect(recoverableOneshotRunAt(T0 - 10, T0, T0 - 5)).toBeNull();
	});

	it("interval chains from last run; first fire is now+period", () => {
		const schedule = { kind: "interval" as const, minutes: 15 };
		expect(computeNextRun(schedule, T0, null)).toBe(T0 + 900);
		expect(computeNextRun(schedule, T0, T0 - 100)).toBe(T0 + 800);
	});

	it("cron anchors from last run when present (crash-safe re-anchor)", () => {
		const schedule = { kind: "cron" as const, expr: "0 9 * * *" };
		const base = Date.parse("2026-02-03T10:00:00Z") / 1000;
		const nextFromBase = computeNextRun(schedule, T0, base);
		expect(nextFromBase).toBe(Date.parse("2026-02-04T09:00:00Z") / 1000);
	});
});

describe("cron expression engine", () => {
	it("advances to the next matching minute same-day when still ahead", () => {
		const now = Date.parse("2026-02-03T08:30:00Z") / 1000;
		expect(cronNextAfter("0 9 * * *", now)).toBe(
			Date.parse("2026-02-03T09:00:00Z") / 1000,
		);
	});

	it("rolls across day/month/year boundaries", () => {
		const dec31 = Date.parse("2026-12-31T23:05:00Z") / 1000;
		expect(cronNextAfter("0 9 * * *", dec31)).toBe(
			Date.parse("2027-01-01T09:00:00Z") / 1000,
		);
	});

	it("supports steps, lists, ranges", () => {
		const now = Date.parse("2026-02-03T00:00:00Z") / 1000;
		expect(cronNextAfter("*/15 * * * *", now)).toBe(now + 900);
		expect(cronNextAfter("0,30 * * * *", now)).toBe(now + 1800);
		// weekday-only expr: Sat → Mon
		const sat = Date.parse("2026-02-07T12:00:00Z") / 1000;
		expect(cronNextAfter("0 8 * * 1-5", sat)).toBe(
			Date.parse("2026-02-09T08:00:00Z") / 1000,
		);
	});

	it("applies the Vixie dom/dow OR rule when both are restricted", () => {
		// "0 0 13 * 5": 13th OR any Friday. Feb 13 2026 is a Friday.
		const feb12 = Date.parse("2026-02-12T12:00:00Z") / 1000;
		const next = cronNextAfter("0 0 13 * 5", feb12);
		expect(next).toBe(Date.parse("2026-02-13T00:00:00Z") / 1000);
		// Feb 20 2026 is a Friday too — fires even though dom≠13.
		const feb19 = Date.parse("2026-02-19T12:00:00Z") / 1000;
		expect(cronNextAfter("0 0 13 * 5", feb19)).toBe(
			Date.parse("2026-02-20T00:00:00Z") / 1000,
		);
	});

	it("treats dow=7 as Sunday", () => {
		const sat = Date.parse("2026-02-07T12:00:00Z") / 1000;
		expect(cronNextAfter("0 8 * * 7", sat)).toBe(
			Date.parse("2026-02-08T08:00:00Z") / 1000,
		);
	});
});

describe("catchup grace clamp [120s, 7200s] — both bounds (07 §5.2)", () => {
	it("frequent jobs hit the MIN floor: every 2m ⇒ half=60 clamped to 120", () => {
		const schedule = { kind: "interval" as const, minutes: 2 };
		expect(scheduleCadenceSeconds(schedule, T0)).toBe(120);
		expect(catchupGraceSeconds(schedule, T0)).toBe(CATCHUP_GRACE_MIN_SECONDS);
	});

	it("infrequent jobs hit the MAX ceiling: every 8h ⇒ half=14400 clamped to 7200", () => {
		const schedule = { kind: "interval" as const, minutes: 480 };
		expect(scheduleCadenceSeconds(schedule, T0)).toBe(28_800);
		expect(catchupGraceSeconds(schedule, T0)).toBe(CATCHUP_GRACE_MAX_SECONDS);
	});

	it("mid-range periods pass through unclamped: every 4h ⇒ exactly 7200", () => {
		const schedule = { kind: "interval" as const, minutes: 240 };
		expect(catchupGraceSeconds(schedule, T0)).toBe(7200);
		expect(catchupGraceSeconds({ kind: "interval", minutes: 10 }, T0)).toBe(
			300,
		);
	});

	it("unknown cadence degrades to the 120s floor", () => {
		expect(catchupGraceSeconds({ kind: "once", runAtSeconds: T0 }, T0)).toBe(
			120,
		);
	});

	it("measures cron cadence between the next two fires", () => {
		const schedule = { kind: "cron" as const, expr: "*/10 * * * *" };
		expect(scheduleCadenceSeconds(schedule, T0)).toBe(600);
	});
});
