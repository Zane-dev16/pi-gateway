// pi_gateway/lifecycle/status-stamp.ts — persisted runtime status snapshot.
//
// Spec: /root/pi-gateway/08-operations.md §4 (verified field set of
// `gateway_state.json`, "written on every runtime-status transition");
// 01-architecture.md §3.1 stage 10. Hermes anchors (READ-ONLY reference;
// semantics ported, no code vendored — gateway/status.py):
//   write_runtime_status            → writeRuntimeStatus (read-modify-write patch)
//   read_runtime_status             → readRuntimeStatus
//   normalize_updated_at            → RFC3339 updated_at on every write
//   _get_code_identity_fields       → code_sha / code_version stamps that
//                                     "degrade to absent fields rather than
//                                     failing the write"
//   _RUNTIME_STATUS_STALE_TTL_S     → RUNTIME_STATUS_STALE_TTL_S (120s)
//   runtime_status_is_stale         → runtimeStatusIsStale
//   runtime_status_pid_is_live      → runtimeStatusPidIsLive
//
// DEC-086 single-writer discipline: only the true gateway boss writes
// `gateway_state.json` (lifecycle stage 10, drain flips, exit stamps — all
// boss-owned). Workers never write it and never stamp the shared file.
// Workers needing to publish facts use their own owned files via
// workerStatusPath plus writeWorkerStatus, merged only at the read or
// reporting boundary. Readers never trust the file alone: interpret with
// interpretRuntimeStatus (pid alive plus heartbeat freshness) and report
// stale file plus dead pulse as crashed, never live. Missing or
// unreadable reads as absent, never as stopped.
//
// The gateway_state vocabulary used by this skeleton: starting | running |
// draining | stopped (#42675: an UNEXPECTED signal must never persist
// "stopped" — enforced by the shutdown controller, not here). The
// interpreted liveness vocabulary is live | crashed | absent (DEC-086) —
// synthesized by readers, never persisted.

import {
	existsSync,
	mkdirSync,
	readFileSync,
	renameSync,
	writeFileSync,
} from "node:fs";
import { randomUUID } from "node:crypto";
import { dirname, join } from "node:path";
import { uptime as osUptime } from "node:os";
import { probeProcess } from "./process-info.js";

export const RUNTIME_STATUS_FILENAME = "gateway_state.json";

export type GatewayRuntimeState =
	| "starting"
	| "running"
	| "draining"
	| "stopped";

export interface RuntimeStatusRecord {
	pid: number;
	kind: string;
	argv: string[];
	/** Process start epoch seconds (clock-change-safe checks). */
	start_time: number;
	/** Owning profile home (scoped-lock placement). */
	pi_home: string;
	gateway_state: GatewayRuntimeState;
	exit_reason: string | null;
	restart_requested: boolean;
	active_agents: number;
	platforms: Record<string, unknown>;
	/** RFC3339 UTC; normalized on every write (08 §4). */
	updated_at: string;
	code_sha: string | null;
	code_version: string | null;
}

export interface StatusIdentity {
	pid?: number;
	startTimeSec?: number;
	argv?: string[];
	home: string;
}

export interface RuntimeStatusPatch {
	gateway_state?: GatewayRuntimeState;
	exit_reason?: string | null;
	restart_requested?: boolean;
	active_agents?: number;
	platforms?: Record<string, unknown>;
	code_sha?: string | null;
	code_version?: string | null;
}

export function runtimeStatusPath(home: string): string {
	return join(home, RUNTIME_STATUS_FILENAME);
}

function baseRecord(identity: StatusIdentity): RuntimeStatusRecord {
	const pid = identity.pid ?? process.pid;
	return {
		pid,
		kind: "pi-gateway",
		argv: identity.argv ?? [...process.argv],
		start_time: identity.startTimeSec ?? defaultStartTimeSec(pid),
		pi_home: identity.home,
		gateway_state: "starting",
		exit_reason: null,
		restart_requested: false,
		active_agents: 0,
		platforms: {},
		updated_at: new Date().toISOString(),
		code_sha: null,
		code_version: null,
	};
}

/**
 * Start-time in SECONDS for the status record (08 §4 field semantics). The
 * raw /proc tick value is converted with USER_HZ=100 anchored to the host
 * boot wall clock so cross-life comparisons stay plausible; off Linux (no
 * source) it degrades to boot wall clock — consumers compare start_time for
 * EQUALITY within one life, which both forms satisfy.
 */
function defaultStartTimeSec(pid: number): number {
	let ticks: number | null = null;
	if (process.platform === "linux") {
		try {
			const raw = readFileSync(`/proc/${pid}/stat`, "utf8");
			const close = raw.lastIndexOf(")");
			const rest =
				close >= 0
					? raw
							.slice(close + 1)
							.trim()
							.split(/\s+/)
					: [];
			const parsed = Number.parseInt(rest[19] ?? "", 10);
			ticks = Number.isFinite(parsed) ? parsed : null;
		} catch {
			ticks = null;
		}
	}
	if (ticks === null) return Math.floor(Date.now() / 1000);
	const hz = 100; // USER_HZ is 100 on every mainstream Linux config
	const bootSec = Math.floor(Date.now() / 1000) - Math.floor(osUptime());
	return Math.floor(bootSec + ticks / hz);
}

function readJson(path: string): Record<string, unknown> | null {
	try {
		const parsed: unknown = JSON.parse(readFileSync(path, "utf8"));
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

function writeAtomic(path: string, payload: unknown): void {
	mkdirSync(dirname(path), { recursive: true });
	const tmp = `${path}.${randomUUID()}.tmp`;
	writeFileSync(tmp, JSON.stringify(payload, null, 2), { mode: 0o600 });
	renameSync(tmp, path);
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return typeof value === "object" && value !== null && !Array.isArray(value);
}

/**
 * Read-modify-write patch (parity of write_runtime_status): missing file ⇒
 * fresh base record first. Never throws for identity-degradation reasons —
 * code stamps arrive pre-degraded (null) from the caller.
 *
 * DEC-086 ownership: only the true gateway boss calls this (lifecycle
 * stage 10, drain flips, exit stamps). Workers never call it — they own
 * separate files via workerStatusPath plus writeWorkerStatus, or nothing.
 */
export function writeRuntimeStatus(
	home: string,
	patch: RuntimeStatusPatch,
	identity: StatusIdentity,
): RuntimeStatusRecord {
	const path = runtimeStatusPath(home);
	const existing = existsSync(path) ? readJson(path) : null;
	const base =
		existing !== null
			? (existing as Partial<RuntimeStatusRecord>)
			: baseRecord(identity);
	// Process identity belongs to the LIVE process, not the file: a bounce
	// leaves a dead pid behind, so a changed pid reconverges pid/argv/
	// start_time onto the live identity instead of preserving the stale row.
	const livePid = identity.pid ?? base.pid ?? process.pid;
	const sameProcess = base.pid !== undefined && base.pid === livePid;
	const next: RuntimeStatusRecord = {
		pid: livePid,
		kind: base.kind ?? "pi-gateway",
		argv: sameProcess
			? (base.argv ?? identity.argv ?? [...process.argv])
			: (identity.argv ?? [...process.argv]),
		start_time: sameProcess
			? (base.start_time ??
					identity.startTimeSec ??
					defaultStartTimeSec(livePid))
			: (identity.startTimeSec ?? defaultStartTimeSec(livePid)),
		pi_home: base.pi_home ?? identity.home,
		gateway_state:
			patch.gateway_state ??
			(base.gateway_state as GatewayRuntimeState) ??
			"starting",
		exit_reason:
			patch.exit_reason !== undefined
				? patch.exit_reason
				: (base.exit_reason ?? null),
		restart_requested:
			patch.restart_requested ?? base.restart_requested ?? false,
		active_agents: patch.active_agents ?? base.active_agents ?? 0,
		platforms:
			patch.platforms ?? (isRecord(base.platforms) ? base.platforms : {}),
		updated_at: new Date().toISOString(),
		code_sha:
			patch.code_sha !== undefined ? patch.code_sha : (base.code_sha ?? null),
		code_version:
			patch.code_version !== undefined
				? patch.code_version
				: (base.code_version ?? null),
	};
	writeAtomic(path, next);
	return next;
}

/** Read the persisted snapshot, or null when absent/unreadable. */
export function readRuntimeStatus(home: string): RuntimeStatusRecord | null {
	const raw = readJson(runtimeStatusPath(home));
	if (raw === null) return null;
	// SAFETY: readJson guarantees a parsed non-array object; the full record
	// shape is trusted because writeRuntimeStatus is the sole atomic writer.
	return raw as unknown as RuntimeStatusRecord;
}

/** Max age of a snapshot before its liveness claim is suspect (Hermes
 * `_RUNTIME_STATUS_STALE_TTL_S`: 2x the 60s housekeeping interval). */
export const RUNTIME_STATUS_STALE_TTL_S = 120;

/** Tolerance for wall-clock start_time equality (boot-second rounding). */
export const RUNTIME_STATUS_START_TIME_TOLERANCE_S = 2;

/** Whole seconds since the snapshot updated_at. Null when missing. */
export function runtimeStatusHeartbeatAgeS(
	record: Pick<RuntimeStatusRecord, "updated_at"> | null | undefined,
	nowMs: () => number = Date.now,
): number | null {
	if (record === null || record === undefined) return null;
	const parsed = Date.parse(record.updated_at);
	if (!Number.isFinite(parsed)) return null;
	return Math.max(0, Math.floor((nowMs() - parsed) / 1000));
}

/** True when the snapshot updated_at is older than ttl (or missing). */
export function runtimeStatusIsStale(
	record: Pick<RuntimeStatusRecord, "updated_at"> | null | undefined,
	ttlS: number = RUNTIME_STATUS_STALE_TTL_S,
	nowMs: () => number = Date.now,
): boolean {
	const age = runtimeStatusHeartbeatAgeS(record, nowMs);
	if (age === null) return true;
	return age > ttlS;
}

/**
 * Live wall-clock start_time for a pid in the status-stamp domain
 * (boot wall clock plus ticks over USER_HZ — the same construction as
 * defaultStartTimeSec). Null when unknown: off Linux or unreadable proc.
 * Callers fall back to pid equality alone when null (08 §1.2 rule).
 */
export function liveStatusStartTimeSec(pid: number): number | null {
	if (process.platform !== "linux") return null;
	try {
		const raw = readFileSync(`/proc/${pid}/stat`, "utf8");
		const close = raw.lastIndexOf(")");
		const rest =
			close >= 0
				? raw
						.slice(close + 1)
						.trim()
						.split(/\s+/)
				: [];
		const parsed = Number.parseInt(rest[19] ?? "", 10);
		if (!Number.isFinite(parsed)) return null;
		const hz = 100;
		const bootSec = Math.floor(Date.now() / 1000) - Math.floor(osUptime());
		return Math.floor(bootSec + parsed / hz);
	} catch {
		return null;
	}
}

export interface RuntimePidProbes {
	pidAlive?: ((pid: number) => boolean) | undefined;
	liveStartTimeSec?: ((pid: number) => number | null) | undefined;
}

/**
 * True when the snapshot pid is alive and passes the start_time reuse
 * guard. Both known but apart beyond tolerance means the holder exited
 * and the OS recycled the pid — never signal it. Either unknown falls
 * back to pid aliveness alone (08 §1.2 rule).
 */
export function runtimeStatusPidIsLive(
	record: Pick<RuntimeStatusRecord, "pid" | "start_time"> | null | undefined,
	probes: RuntimePidProbes = {},
): boolean {
	if (record === null || record === undefined) return false;
	const pid = record.pid;
	if (!Number.isInteger(pid) || pid <= 0) return false;
	const alive = (probes.pidAlive ?? ((p: number) => probeProcess(p).alive))(pid);
	if (!alive) return false;
	const live =
		probes.liveStartTimeSec !== undefined
			? probes.liveStartTimeSec(pid)
			: liveStatusStartTimeSec(pid);
	const recorded = record.start_time;
	if (live === null || live === undefined) return true;
	if (typeof recorded !== "number" || !Number.isFinite(recorded)) return true;
	return Math.abs(live - recorded) <= RUNTIME_STATUS_START_TIME_TOLERANCE_S;
}

/** Interpreted liveness for one home (DEC-086). Synthesized, never stored. */
export type RuntimeLiveness = "live" | "crashed" | "absent";

export interface RuntimeStatusView {
	outcome: RuntimeLiveness;
	record: RuntimeStatusRecord | null;
	pidAlive: boolean;
	stale: boolean;
	startTimeMatches: boolean;
}

export interface InterpretRuntimeStatusOptions extends RuntimePidProbes {
	ttlS?: number | undefined;
	nowMs?: (() => number) | undefined;
}

/**
 * Interpret the shared file with liveness alongside it. Missing or
 * unreadable reads as absent, never as stopped. A live pid with a
 * matching start_time reads live (stale heartbeat rides alongside as a
 * health warning, never as death — Hermes parity). A dead pulse reads
 * crashed, never live — including stale file plus dead pulse.
 */
export function interpretRuntimeStatus(
	home: string,
	opts: InterpretRuntimeStatusOptions = {},
): RuntimeStatusView {
	const record = readRuntimeStatus(home);
	if (record === null) {
		return {
			outcome: "absent",
			record: null,
			pidAlive: false,
			stale: false,
			startTimeMatches: false,
		};
	}
	const nowMs = opts.nowMs ?? Date.now;
	const ttlS = opts.ttlS ?? RUNTIME_STATUS_STALE_TTL_S;
	const stale = runtimeStatusIsStale(record, ttlS, nowMs);
	const probes: RuntimePidProbes = {};
	if (opts.pidAlive !== undefined) probes.pidAlive = opts.pidAlive;
	if (opts.liveStartTimeSec !== undefined) {
		probes.liveStartTimeSec = opts.liveStartTimeSec;
	}
	const pidAlive = runtimeStatusPidIsLive(record, probes);
	const live =
		probes.liveStartTimeSec !== undefined
			? probes.liveStartTimeSec(record.pid)
			: liveStatusStartTimeSec(record.pid);
	const startTimeMatches =
		live === null ||
		live === undefined ||
		typeof record.start_time !== "number" ||
		!Number.isFinite(record.start_time)
			? pidAlive
			: Math.abs(live - record.start_time) <=
					RUNTIME_STATUS_START_TIME_TOLERANCE_S;
	return {
		outcome: pidAlive ? "live" : "crashed",
		record,
		pidAlive,
		stale,
		startTimeMatches,
	};
}

/** Owned worker state path: <home>/workers/<id>.json (DEC-086). */
export function workerStatusPath(home: string, workerId: string): string {
	const safe = workerId.replace(/[^a-zA-Z0-9_-]/g, "_").slice(0, 64) || "worker";
	return join(home, "workers", `${safe}.json`);
}

/**
 * Worker-owned scratch write. Never touches `gateway_state.json` — the
 * boss stays the sole writer of the shared file. Merged only at the read
 * or reporting boundary by whoever consumes the worker files.
 */
export function writeWorkerStatus(
	home: string,
	workerId: string,
	payload: unknown,
): string {
	const path = workerStatusPath(home, workerId);
	writeAtomic(path, payload);
	return path;
}
