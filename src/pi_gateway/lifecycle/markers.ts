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
//
// Drain-request marker (external begin/cancel-drain contract, 08 §1.2;
// gateway/drain_control.py): presence of `.drain_request.json` stamped with
// the CURRENT instantiation epoch flips the gateway to externally-draining
// and stops new turns; a PRIOR-epoch marker (survived a machine restart on a
// durable home volume, NS-570) or one older than DRAIN_REQUEST_MAX_AGE_SECONDS
// = 3600s (#85433) is ignored leniently. Reading NEVER raises: malformed /
// half-written files read as present-but-contentless ({}) which still counts
// as drain-active — fail-safe toward quiescing. Only a DEFINITE epoch mismatch
// or a parseable, definitely-too-old timestamp is ignored; anything ambiguous
// honours the marker.

import {
	existsSync,
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

// ---------------------------------------------------------------------------
// Drain-request marker (.drain_request.json — drain_control.py port)
// ---------------------------------------------------------------------------

export const DRAIN_REQUEST_FILENAME = ".drain_request.json";
/** Same-epoch orphan max-age in seconds (drain_control.py:#85433). */
export const DRAIN_REQUEST_MAX_AGE_SECONDS = 3600;

export function drainRequestPath(home: string): string {
	return join(home, DRAIN_REQUEST_FILENAME);
}

export interface DrainRequestRecord {
	action: "drain";
	requested_at: string;
	principal: string;
	epoch: string;
	suppress_notification: boolean;
}

/** Injectable /proc reader for epoch computation (tests). */
type ProcTextReader = (path: string) => string | null;

function defaultProcText(path: string): string | null {
	try {
		if (!existsSync(path)) return null;
		return readFileSync(path, "utf8");
	} catch {
		return null;
	}
}

/**
 * Identity of THIS container/VM instantiation (drain_control.py:
 * current_instantiation_epoch): kernel boot id + PID 1's start time. Stable
 * for the life of the PID-1 init (an s6 respawn of just the gateway keeps the
 * epoch and an in-flight drain is honoured), fresh when the machine/container
 * is recreated. Returns "" when neither source is readable — an empty epoch
 * DISABLES the staleness check downstream (presence-only behaviour).
 */
export function computeInstantiationEpoch(
	readProcText: ProcTextReader = defaultProcText,
): string {
	const bootId = (readProcText("/proc/sys/kernel/random/boot_id") ?? "").trim();
	let pid1Start = "";
	try {
		// /proc/1/stat: comm can contain spaces/parens — split after the LAST
		// ')'; starttime is field 22, i.e. tail index 19 (drain_control.py).
		const stat = readProcText("/proc/1/stat") ?? "";
		const close = stat.lastIndexOf(")");
		if (close >= 0) {
			pid1Start =
				stat
					.slice(close + 1)
					.trim()
					.split(/\s+/)[19] ?? "";
		}
	} catch {
		pid1Start = "";
	}
	if (!bootId && !pid1Start) return "";
	return `${bootId}:${pid1Start}`;
}

let cachedEpoch: string | undefined | null = null;

/** Memoised epoch for THIS process (the value cannot change mid-life). */
export function currentInstantiationEpoch(): string {
	if (cachedEpoch !== null) return cachedEpoch as string;
	cachedEpoch = computeInstantiationEpoch();
	return cachedEpoch;
}

/** Test hook: drop the memoised epoch so injected readers re-compute. */
export function resetInstantiationEpochCache(): void {
	cachedEpoch = null;
}

/** Write the begin-drain marker (drain_control.py:write_drain_request).
 *  Idempotent: re-writing refreshes requested_at — the sanctioned keep-alive
 *  for drains longer than the max-age. Atomic; best-effort. */
export function writeDrainRequest(
	home: string,
	options: {
		principal?: string;
		suppressNotification?: boolean;
		epoch?: string;
		nowMs?: () => number;
	} = {},
): boolean {
	try {
		const record: DrainRequestRecord = {
			action: "drain",
			requested_at: new Date((options.nowMs ?? Date.now)()).toISOString(),
			principal: options.principal ?? "drain-control",
			epoch:
				options.epoch !== undefined
					? options.epoch
					: currentInstantiationEpoch(),
			suppress_notification: options.suppressNotification === true,
		};
		writeJsonAtomic(drainRequestPath(home), record);
		return true;
	} catch {
		return false;
	}
}

/** Remove the drain marker (cancel-drain). True when one existed. Best-effort. */
export function clearDrainRequest(home: string): boolean {
	try {
		rmSync(drainRequestPath(home), { force: false });
		return true;
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === "ENOENT") return false;
		try {
			rmSync(drainRequestPath(home), { force: true });
		} catch {
			/* cancel is idempotent — never raise */
		}
		return false;
	}
}

/**
 * Marker body or null when absent. A present-but-unparseable marker returns
 * `{}` (truthy-presence preserved; never raises — drain_control.py:
 * read_drain_request).
 */
export function readDrainRequest(home: string): Record<string, unknown> | null {
	try {
		if (!existsSync(drainRequestPath(home))) return null;
		const parsed: unknown = JSON.parse(
			readFileSync(drainRequestPath(home), "utf8"),
		);
		if (
			typeof parsed === "object" &&
			parsed !== null &&
			!Array.isArray(parsed)
		) {
			return parsed as Record<string, unknown>;
		}
		return {};
	} catch {
		return {};
	}
}

interface DrainStalenessOptions {
	/** Override epoch (tests). Default currentInstantiationEpoch(). */
	epoch?: string;
	nowMs?: () => number;
}

function drainMarkerEpochIsStale(
	body: Record<string, unknown>,
	options: DrainStalenessOptions,
): boolean {
	const current = options.epoch ?? currentInstantiationEpoch();
	if (!current) return false; // epoch unavailable ⇒ lenient: honour presence
	const markerEpoch = body["epoch"];
	if (typeof markerEpoch !== "string" || markerEpoch === "") return false; // legacy/corrupt ⇒ honour
	return markerEpoch !== current; // definite mismatch only
}

function drainMarkerIsExpired(
	body: Record<string, unknown>,
	options: DrainStalenessOptions,
): boolean {
	const raw = body["requested_at"];
	if (typeof raw !== "string" || raw === "") return false;
	const requestedAt = Date.parse(raw);
	if (!Number.isFinite(requestedAt)) return false; // unparseable ⇒ honour
	const nowMs = options.nowMs ?? Date.now;
	const ageS = (nowMs() - requestedAt) / 1000;
	return ageS > DRAIN_REQUEST_MAX_AGE_SECONDS; // future-dated ⇒ honoured
}

/** Either lenient signal suffices: prior epoch OR past max-age. */
export function drainMarkerIsStale(
	body: Record<string, unknown>,
	options: DrainStalenessOptions = {},
): boolean {
	return (
		drainMarkerEpochIsStale(body, options) ||
		drainMarkerIsExpired(body, options)
	);
}

/**
 * True iff a begin-drain marker for THIS instantiation is present and not
 * definitely stale (drain_control.py:drain_requested). Malformed/empty bodies
 * count as ACTIVE (fail-safe toward quiescing).
 */
export function drainRequested(
	home: string,
	options: DrainStalenessOptions = {},
): boolean {
	const body = readDrainRequest(home);
	if (body === null) return false;
	return !drainMarkerIsStale(body, options);
}

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
