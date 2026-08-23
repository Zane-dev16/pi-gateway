// pi_gateway/lifecycle/markers.ts — takeover + planned-stop marker files.
//
// Spec: /root/pi-gateway/08-operations.md §1.2 (signal classification),
// §1.1 stage 4; 01-architecture.md §3.2. Hermes anchors (READ-ONLY reference;
// semantics ported, no code vendored — gateway/status.py):
//   _TAKEOVER_MARKER_FILENAME / _PLANNED_STOP_MARKER_FILENAME (+ 60s TTLs)
//                                              → MARKER_TTL_SECONDS
//   write_takeover_marker                       → writeTakeoverMarker
//   consume_takeover_marker_for_self            → consumeTakeoverMarkerForSelf
//   clear_takeover_marker                       → clearTakeoverMarker
//   write_planned_stop_marker                   → writePlannedStopMarker
//   planned_stop_marker_targets_self            → plannedStopMarkerTargetsSelf
//     (non-destructive probe)                     (never unlinks a match)
//   consume_planned_stop_marker_for_self        → consumePlannedStopMarkerForSelf
//   _consume_pid_marker_for_self                → shared consumePidMarkerForSelf
//   _marker_is_stale                            → isMarkerStale
//
// Binding semantics ported verbatim:
// - Markers are short-lived (60s TTL); stale/malformed markers are unlinked so
//   a leftover file can never wedge or misclassify a future shutdown.
// - PID-reuse guard (#34597): when BOTH start times are known they must match;
//   when either is unknown, PID equality alone decides (bounded by the TTL).
// - Cross-home guard (#29092): a takeover marker naming a target_pi_home that
//   is not ours is ignored (legacy markers without the field fall back to the
//   replacer-home rule).
// - The AUTHORITATIVE consume always unlinks a non-stale, home-applicable
//   marker and returns whether it named us; the non-destructive PLANNED-STOP
//   probe leaves matching markers for the handler to consume.

import {
	mkdirSync,
	readFileSync,
	renameSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { randomUUID } from "node:crypto";
import { dirname, join } from "node:path";
import { processStartTime, startTimeMatches } from "./process-info.js";

export const TAKEOVER_MARKER_FILENAME = ".gateway-takeover.json";
export const PLANNED_STOP_MARKER_FILENAME = ".gateway-planned-stop.json";
/** Marker older than this is treated as stale (status.py:_TAKEOVER_MARKER_TTL_S). */
export const MARKER_TTL_SECONDS = 60;

export interface MarkerOptions {
	/** Override self PID (tests). Default process.pid. */
	selfPid?: number;
	/**
	 * Override OUR start time (tests). Default probed live; null off-Linux.
	 */
	selfStartTime?: number | null;
	/** Injected wall clock in ms (tests). Default Date.now(). */
	nowMs?: () => number;
}

export interface TakeoverMarkerRecord {
	target_pid: number;
	target_start_time: number | null;
	target_pi_home: string;
	replacer_pid: number;
	replacer_pi_home: string;
	written_at: string;
}

export interface PlannedStopMarkerRecord {
	target_pid: number;
	target_start_time: number | null;
	stopper_pid: number;
	written_at: string;
}

export function takeoverMarkerPath(home: string): string {
	return join(home, TAKEOVER_MARKER_FILENAME);
}

export function plannedStopMarkerPath(home: string): string {
	return join(home, PLANNED_STOP_MARKER_FILENAME);
}

function utcNowIso(nowMs: () => number): string {
	return new Date(nowMs()).toISOString();
}

function isMarkerStale(writtenAt: string, nowMs: () => number): boolean {
	const written = Date.parse(writtenAt);
	if (!Number.isFinite(written)) return true; // unparseable ⇒ stale
	return nowMs() - written > MARKER_TTL_SECONDS * 1000;
}

function readJsonFile(path: string): Record<string, unknown> | null {
	try {
		const raw = readFileSync(path, "utf8");
		const parsed: unknown = JSON.parse(raw);
		if (
			typeof parsed === "object" &&
			parsed !== null &&
			!Array.isArray(parsed)
		) {
			return parsed as Record<string, unknown>;
		}
		return null;
	} catch {
		return null;
	}
}

/** Atomic JSON write: temp file + rename (markers must never be torn). */
function writeJsonAtomic(path: string, payload: unknown): void {
	mkdirSync(dirname(path), { recursive: true });
	const tmp = `${path}.${randomUUID()}.tmp`;
	writeFileSync(tmp, JSON.stringify(payload), { mode: 0o600 });
	renameSync(tmp, path);
}

function unlinkQuiet(path: string): void {
	try {
		rmSync(path, { force: true });
	} catch {
		/* best-effort — never raise from marker hygiene */
	}
}

interface ParsedTarget {
	targetPid: number;
	targetStartTime: number | null;
}

function parseTargetFields(
	record: Record<string, unknown>,
	pidField: string,
	startTimeField: string,
): ParsedTarget | null {
	const rawPid = record[pidField];
	let pid: number;
	if (typeof rawPid === "number") {
		pid = rawPid;
	} else {
		pid = Number.parseInt(String(rawPid ?? ""), 10);
	}
	if (!Number.isFinite(pid)) return null;
	const rawStart = record[startTimeField];
	let targetStartTime: number | null = null;
	if (typeof rawStart === "number") {
		targetStartTime = rawStart;
	} else if (rawStart !== undefined && rawStart !== null) {
		const parsed = Number(rawStart);
		targetStartTime = Number.isFinite(parsed) ? parsed : null;
	}
	return { targetPid: pid, targetStartTime };
}

/**
 * Shared consume core (status.py:_consume_pid_marker_for_self). Returns
 * "self" | "other" | "absent". "absent" covers missing/malformed/stale/
 * cross-home markers (stale + unparsable get cleaned up in passing).
 */
function consumePidMarkerForSelf(
	path: string,
	pidField: string,
	startTimeField: string,
	opts: MarkerOptions & { home: string; homeField?: string },
): "self" | "other" | "absent" {
	const nowMs = opts.nowMs ?? (() => Date.now());
	const selfPid = opts.selfPid ?? process.pid;
	const selfStartTime =
		opts.selfStartTime !== undefined
			? opts.selfStartTime
			: processStartTime(selfPid);

	const record = readJsonFile(path);
	if (!record) return "absent";

	let target: ParsedTarget | null = null;
	try {
		target = parseTargetFields(record, pidField, startTimeField);
	} catch {
		target = null;
	}
	const writtenAt =
		typeof record["written_at"] === "string" ? record["written_at"] : "";
	if (target === null || writtenAt === "") {
		unlinkQuiet(path); // malformed markers can never match anyone — drop
		return "absent";
	}
	if (isMarkerStale(writtenAt, nowMs)) {
		unlinkQuiet(path);
		return "absent";
	}

	// Cross-home guard (#29092): new markers name the verified TARGET home;
	// legacy markers fall back to the replacer-home rule.
	const ourHome = opts.home;
	if (opts.homeField !== undefined) {
		const namedHome = record[opts.homeField];
		if (
			namedHome !== undefined &&
			namedHome !== null &&
			namedHome !== ourHome
		) {
			return "absent"; // not ours, and NOT unlinkable — another profile owns it
		}
	}

	const matches = startTimeMatches(
		target.targetStartTime,
		selfStartTime,
		target.targetPid === selfPid,
	);

	// Authoritative consume: ALWAYS unlink once non-stale + home-applicable
	// so subsequent unrelated signals cannot re-trigger (status.py parity).
	unlinkQuiet(path);
	return matches ? "self" : "other";
}

// ---------------------------------------------------------------------------
// Takeover marker (--replace handshake, 08 §1.1 stage 4)
// ---------------------------------------------------------------------------

/**
 * Record that `targetPid` is being replaced by the current process. Captures
 * the target's start_time so PID reuse after it exits can never match later.
 * BEST-EFFORT by spec (08 §1.1 stage 4): returns false on any failure and the
 * caller proceeds anyway.
 */
export function writeTakeoverMarker(
	home: string,
	targetPid: number,
	options: MarkerOptions & { targetStartTime?: number | null } = {},
): boolean {
	try {
		const nowMs = options.nowMs ?? (() => Date.now());
		const selfPid = options.selfPid ?? process.pid;
		const targetStartTime =
			options.targetStartTime !== undefined
				? options.targetStartTime
				: processStartTime(targetPid);
		const record: TakeoverMarkerRecord = {
			target_pid: targetPid,
			target_start_time: targetStartTime,
			target_pi_home: home,
			replacer_pid: selfPid,
			replacer_pi_home: home,
			written_at: utcNowIso(nowMs),
		};
		writeJsonAtomic(takeoverMarkerPath(home), record);
		return true;
	} catch {
		return false;
	}
}

/**
 * True only when a valid (non-stale) takeover marker names THIS process
 * (pid + start-time rule). Unlinks the marker on every non-absent verdict —
 * a returning true means the SIGTERM is a planned --replace takeover and the
 * gateway exits 0 instead of recording an unexpected-signal death.
 */
export function consumeTakeoverMarkerForSelf(
	home: string,
	options: MarkerOptions = {},
): boolean {
	return (
		consumePidMarkerForSelf(
			takeoverMarkerPath(home),
			"target_pid",
			"target_start_time",
			{
				...options,
				home,
				homeField: "target_pi_home",
			},
		) === "self"
	);
}

/** Remove the takeover marker unconditionally. Safe to call repeatedly. */
export function clearTakeoverMarker(home: string): void {
	unlinkQuiet(takeoverMarkerPath(home));
}

// ---------------------------------------------------------------------------
// Planned-stop marker (`gateway stop` / service-manager stop)
// ---------------------------------------------------------------------------

/** Service stop commands write this BEFORE signalling (08 §1.2). Best-effort. */
export function writePlannedStopMarker(
	home: string,
	targetPid: number,
	options: MarkerOptions & { targetStartTime?: number | null } = {},
): boolean {
	try {
		const nowMs = options.nowMs ?? (() => Date.now());
		const selfPid = options.selfPid ?? process.pid;
		const targetStartTime =
			options.targetStartTime !== undefined
				? options.targetStartTime
				: processStartTime(targetPid);
		const record: PlannedStopMarkerRecord = {
			target_pid: targetPid,
			target_start_time: targetStartTime,
			stopper_pid: selfPid,
			written_at: utcNowIso(nowMs),
		};
		writeJsonAtomic(plannedStopMarkerPath(home), record);
		return true;
	} catch {
		return false;
	}
}

/**
 * AUTHORITATIVE consume for the signal handler: true when the current process
 * is being intentionally stopped. Unlinks on match and on staleness.
 */
export function consumePlannedStopMarkerForSelf(
	home: string,
	options: MarkerOptions = {},
): boolean {
	return (
		consumePidMarkerForSelf(
			plannedStopMarkerPath(home),
			"target_pid",
			"target_start_time",
			{
				...options,
				home,
			},
		) === "self"
	);
}

/**
 * NON-destructive probe (status.py:planned_stop_marker_targets_self): used by
 * watchers to decide whether a stop was requested. Never unlinks a marker that
 * matches us; DOES clean up malformed/stale markers so a leftover file cannot
 * crash-loop a fresh boot. Markers naming another process are left alone.
 */
export function plannedStopMarkerTargetsSelf(
	home: string,
	options: MarkerOptions = {},
): boolean {
	const path = plannedStopMarkerPath(home);
	const nowMs = options.nowMs ?? (() => Date.now());
	const selfPid = options.selfPid ?? process.pid;
	const selfStartTime =
		options.selfStartTime !== undefined
			? options.selfStartTime
			: processStartTime(selfPid);

	const record = readJsonFile(path);
	if (!record) return false;
	const target = parseTargetFields(record, "target_pid", "target_start_time");
	const writtenAt =
		typeof record["written_at"] === "string" ? record["written_at"] : "";
	if (target === null || writtenAt === "") {
		unlinkQuiet(path); // malformed — drop
		return false;
	}
	if (isMarkerStale(writtenAt, nowMs)) {
		unlinkQuiet(path); // past its useful life regardless of target — clean up
		return false;
	}
	if (target.targetPid !== selfPid) return false;
	return startTimeMatches(target.targetStartTime, selfStartTime, true);
}

/** Remove the planned-stop marker unconditionally. Safe to call repeatedly. */
export function clearPlannedStopMarker(home: string): void {
	unlinkQuiet(plannedStopMarkerPath(home));
}
