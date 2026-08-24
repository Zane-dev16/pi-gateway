// pi_embedded/cron/schedule.ts — schedule model + next-run computation.
//
// Ported semantics (Hermes anchors, READ-ONLY reference; no code vendored):
//   cron/jobs.py:parse_duration            → parseDuration
//   cron/jobs.py:parse_schedule            → parseSchedule
//   cron/jobs.py:compute_next_run          → computeNextRun
//   cron/jobs.py:_recoverable_oneshot_run_at → recoverableOneshotRunAt
//   cron/jobs.py:_schedule_cadence_seconds → scheduleCadenceSeconds
//   cron/jobs.py:_compute_grace_seconds    → catchupGraceSeconds
//
// Binding invariants (07 §5.1–§5.2):
//   - Schedule formats: duration one-shot ("30m"/"2h"/"1d"), recurring
//     interval ("every 30m"), 5-field cron ("0 9 * * *"), ISO timestamp.
//   - Catchup grace = HALF the schedule period clamped to [120s, 7200s];
//     unknown cadence degrades to the 120s floor. Late-but-within-grace
//     catches up (fires the stale slot); past grace fast-forwards (skips
//     accumulated missed runs, still executes once now).
//   - One-shot grace ONESHOT_GRACE_SECONDS = 120: a one-shot created up to
//     120s after its requested minute still fires on the next tick; past
//     that it is REJECTED at create/update time with an explicit error and
//     never scheduled.

export const ONESHOT_GRACE_SECONDS = 120;
export const CATCHUP_GRACE_MIN_SECONDS = 120;
export const CATCHUP_GRACE_MAX_SECONDS = 7200;

export type CronSchedule =
	| { kind: "once"; runAtSeconds: number }
	| { kind: "interval"; minutes: number }
	| { kind: "cron"; expr: string };

/** Epoch-seconds → ISO-8601 UTC (deterministic; no local-TZ dependence). */
export function epochToIso(seconds: number): string {
	return new Date(seconds * 1000).toISOString();
}

/** ISO-8601 → epoch seconds; null when unparsable. Accepts a trailing Z. */
export function isoToEpoch(iso: string): number | null {
	const ms = Date.parse(iso);
	return Number.isFinite(ms) ? ms / 1000 : null;
}

/**
 * Parse a duration string into minutes (parity of parse_duration).
 * "30m"→30, "2h"→120, "1d"→1440; unit aliases m/min(s)/hour(s)/day(s).
 */
export function parseDuration(raw: string): number {
	const s = raw.trim().toLowerCase();
	const match =
		/^(\d+)\s*(m|min|mins|minute|minutes|h|hr|hrs|hour|hours|d|day|days)$/.exec(
			s,
		);
	if (match === null) {
		throw new Error(
			`Invalid duration: '${raw}'. Use format like '30m', '2h', or '1d'`,
		);
	}
	const value = Number(match[1]);
	const unit = (match[2] ?? "")[0]; // first char disambiguates m/h/d
	if (unit === undefined) {
		throw new Error(`Invalid duration unit in '${raw}'`);
	}
	const multipliers: Record<string, number> = { m: 1, h: 60, d: 1440 };
	const multiplier = multipliers[unit];
	if (multiplier === undefined) {
		throw new Error(`Invalid duration unit in '${raw}'`);
	}
	return value * multiplier;
}

/**
 * Parse a schedule string (parity of parse_schedule). Returns the structured
 * schedule. Raises with an actionable message on anything unparsable —
 * including weekday phrases ("every monday 9am"), which Hermes' parser also
 * rejects at this layer (they are blueprint/CLI sugar upstream, not ticker
 * grammar); Pi ports the ticker grammar only.
 */
export function parseSchedule(schedule: string): CronSchedule {
	const trimmed = schedule.trim();
	const lower = trimmed.toLowerCase();

	// "every X" → recurring interval.
	if (lower.startsWith("every ")) {
		const minutes = parseDuration(trimmed.slice(6));
		return { kind: "interval", minutes };
	}

	// Cron expression: >=5 whitespace-separated fields of [\d*,\-/]+.
	const parts = trimmed.split(/\s+/);
	if (
		parts.length >= 5 &&
		parts.slice(0, 5).every((p) => /^[\d*\-,/]+$/.test(p))
	) {
		validateCronExpression(parts.slice(0, 5).join(" "));
		return { kind: "cron", expr: parts.slice(0, 5).join(" ") };
	}

	// ISO timestamp → one-shot at that time (naive strings are read as UTC —
	// deterministic storage; local-wall-clock intent is a config-layer concern).
	if (trimmed.includes("T") || /^\d{4}-\d{2}-\d{2}/.test(trimmed)) {
		const seconds = isoToEpoch(trimmed.replace(/(Z|\+00:00)$/i, "Z"));
		if (seconds === null) {
			throw new Error(`Invalid timestamp '${schedule}'`);
		}
		return { kind: "once", runAtSeconds: seconds };
	}

	// Bare duration → one-shot relative to `nowSeconds`.
	try {
		const minutes = parseDuration(trimmed);
		return { kind: "once", runAtSeconds: nowAnchorSeconds() + minutes * 60 };
	} catch {
		/* fall through to the rejection below */
	}

	throw new Error(
		`Invalid schedule '${schedule}'. Use:\n` +
			`  - Duration: '30m', '2h', '1d' (one-shot)\n` +
			`  - Interval: 'every 30m', 'every 2h' (recurring)\n` +
			`  - Cron: '0 9 * * *' (cron expression)\n` +
			`  - Timestamp: '2026-02-03T14:00:00Z' (one-shot at time)`,
	);
}

/** Anchor for bare-duration one-shots; injectable for tests via setNowAnchor. */
let nowAnchor: () => number = () => Date.now() / 1000;
function nowAnchorSeconds(): number {
	return nowAnchor();
}
export function setNowAnchorForTests(fn: () => number): void {
	nowAnchor = fn;
}

// ---------------------------------------------------------------------------
// 5-field cron engine (croniter-equivalent subset)
// ---------------------------------------------------------------------------

interface CronFields {
	minutes: Set<number>;
	hours: Set<number>;
	doms: Set<number>;
	months: Set<number>;
	dowSet: Set<number>;
	/** Vixie rule: when BOTH dom and dow are restricted, either may match. */
	domRestricted: boolean;
	dowRestricted: boolean;
}

const FIELD_BOUNDS: Array<{ min: number; max: number }> = [
	{ min: 0, max: 59 }, // minute
	{ min: 0, max: 23 }, // hour
	{ min: 1, max: 31 }, // day of month
	{ min: 1, max: 12 }, // month
	{ min: 0, max: 7 }, // day of week (0 and 7 both Sunday)
];

function parseField(
	field: string,
	bounds: { min: number; max: number },
): Set<number> {
	const out = new Set<number>();
	for (const piece of field.split(",")) {
		let body = piece;
		let step = 1;
		const slash = piece.indexOf("/");
		if (slash !== -1) {
			body = piece.slice(0, slash);
			step = Number(piece.slice(slash + 1));
			if (!Number.isInteger(step) || step <= 0) {
				throw new Error(`Invalid cron step in '${piece}'`);
			}
		}
		let lo: number;
		let hi: number;
		if (body === "*" || body === "") {
			lo = bounds.min;
			hi = bounds.max;
		} else if (body.includes("-")) {
			const [a, b] = body.split("-");
			lo = Number(a);
			hi = Number(b);
		} else {
			lo = Number(body);
			hi = slash === -1 ? lo : bounds.max; // "5/10" ≙ "5-max/10"
		}
		if (
			!Number.isInteger(lo) ||
			!Number.isInteger(hi) ||
			lo < bounds.min ||
			hi > bounds.max ||
			lo > hi
		) {
			throw new Error(`Invalid cron field '${field}'`);
		}
		for (let v = lo; v <= hi; v += step) out.add(v);
	}
	return out;
}

function parseCronFields(expr: string): CronFields {
	const parts = expr.trim().split(/\s+/);
	if (parts.length !== 5) throw new Error(`Invalid cron expression '${expr}'`);
	const parsed = parts.map((p, i) => parseField(p, FIELD_BOUNDS[i]!));
	const [minutes, hours, doms, months, dowRaw] = parsed as [
		Set<number>,
		Set<number>,
		Set<number>,
		Set<number>,
		Set<number>,
	];
	// Normalize 7 → 0 (Sunday).
	const normalizedDowSet = new Set<number>();
	for (const d of dowRaw) normalizedDowSet.add(d === 7 ? 0 : d);
	return {
		minutes,
		hours,
		doms,
		months,
		dowSet: normalizedDowSet,
		domRestricted: parts[2] !== "*",
		dowRestricted: parts[4] !== "*",
	};
}

function matchesDay(fields: CronFields, date: Date): boolean {
	const dom = date.getUTCDate();
	const dow = date.getUTCDay();
	const month = date.getUTCMonth() + 1;
	if (!fields.months.has(month)) return false;
	const domOk = fields.doms.has(dom);
	const dowOk = fields.dowSet.has(dow);
	if (fields.domRestricted && fields.dowRestricted) return domOk || dowOk;
	if (fields.domRestricted) return domOk;
	if (fields.dowRestricted) return dowOk;
	return true;
}

/** Next epoch second STRICTLY after `afterSeconds` matching the expression,
 * aligned to whole minutes (croniter get_next parity). null after 5 years. */
export function cronNextAfter(
	expr: string,
	afterSeconds: number,
): number | null {
	let fields: CronFields;
	try {
		fields = parseCronFields(expr);
	} catch {
		return null;
	}
	// Round UP to the next whole minute boundary strictly after `after`.
	const afterMs = afterSeconds * 1000;
	let cursor = Math.floor(afterMs / 60_000) * 60_000;
	if (cursor <= afterMs) cursor += 60_000;
	const deadline = afterMs + 5 * 366 * 24 * 3600 * 1000;
	while (cursor < deadline) {
		const date = new Date(cursor);
		if (
			matchesDay(fields, date) &&
			fields.hours.has(date.getUTCHours()) &&
			fields.minutes.has(date.getUTCMinutes())
		) {
			return cursor / 1000;
		}
		cursor += 60_000;
	}
	return null;
}

function validateCronExpression(expr: string): void {
	const parts = expr.trim().split(/\s+/);
	if (parts.length !== 5) {
		throw new Error(`Invalid cron expression '${expr}': expected 5 fields`);
	}
	parts.forEach((p, i) => {
		parseField(p, FIELD_BOUNDS[i]!); // throws with a specific message
	});
}

/**
 * Compute the next run time (epoch seconds) for a schedule, or null when the
 * schedule has no more runs (parity of compute_next_run).
 *
 * - once: eligible ONLY while unrun AND within the one-shot grace window —
 *   returns the ORIGINAL run_at (it fires late-but-within-grace on the next
 *   tick); past grace or already run ⇒ null (permanently ineligible).
 * - interval: lastRun+minutes when re-anchoring, else now+minutes.
 * - cron: next matching minute after lastRun (crash-safe anchor), else now.
 */
export function computeNextRun(
	schedule: CronSchedule,
	nowSeconds: number,
	lastRunSeconds?: number | null,
): number | null {
	const lastRun = lastRunSeconds ?? null;
	switch (schedule.kind) {
		case "once":
			return recoverableOneshotRunAt(
				schedule.runAtSeconds,
				nowSeconds,
				lastRun,
			);
		case "interval": {
			if (lastRun === null) return nowSeconds + schedule.minutes * 60;
			return lastRun + schedule.minutes * 60;
		}
		case "cron":
			return cronNextAfter(
				schedule.expr,
				lastRun === null ? nowSeconds : lastRun,
			);
		default:
			return null; // unreachable: CronSchedule is a closed union
	}
}

/** Parity of _recoverable_oneshot_run_at: run_at while unrun and within the
 * 120s grace window (measured BACKWARD from now), else null. */
export function recoverableOneshotRunAt(
	runAtSeconds: number,
	nowSeconds: number,
	lastRunSeconds?: number | null,
): number | null {
	if (lastRunSeconds !== null && lastRunSeconds !== undefined) return null;
	return runAtSeconds >= nowSeconds - ONESHOT_GRACE_SECONDS
		? runAtSeconds
		: null;
}

/**
 * Approximate the natural period of a schedule in seconds (parity of
 * _schedule_cadence_seconds): interval = minutes*60; cron = gap between the
 * next two fire times measured from `nowSeconds`; once/unknown = null.
 */
export function scheduleCadenceSeconds(
	schedule: CronSchedule,
	nowSeconds: number,
): number | null {
	switch (schedule.kind) {
		case "once":
			return null;
		case "interval":
			return schedule.minutes > 0 ? schedule.minutes * 60 : null;
		case "cron": {
			const first = cronNextAfter(schedule.expr, nowSeconds);
			if (first === null) return null;
			const second = cronNextAfter(schedule.expr, first);
			if (second === null) return null;
			const gap = second - first;
			return gap > 0 ? gap : null;
		}
		default:
			return null; // unreachable: CronSchedule is a closed union
	}
}

/**
 * How late a recurring job can be and still CATCH UP instead of
 * fast-forwarding: half the schedule period clamped to [120s, 7200s];
 * unknown cadence degrades to the 120s floor (parity of
 * _compute_grace_seconds — daily jobs tolerate a 2h miss, frequent jobs
 * fast-forward quickly, 07 §5.2 binding clamp).
 */
export function catchupGraceSeconds(
	schedule: CronSchedule,
	nowSeconds: number,
): number {
	const period = scheduleCadenceSeconds(schedule, nowSeconds);
	if (period === null) return CATCHUP_GRACE_MIN_SECONDS;
	const grace = Math.floor(period / 2);
	return Math.max(
		CATCHUP_GRACE_MIN_SECONDS,
		Math.min(grace, CATCHUP_GRACE_MAX_SECONDS),
	);
}
