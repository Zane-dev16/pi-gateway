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
//
// The gateway_state vocabulary used by this skeleton: starting | running |
// draining | stopped (#42675: an UNEXPECTED signal must never persist
// "stopped" — enforced by the shutdown controller, not here).

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
	const next: RuntimeStatusRecord = {
		pid: base.pid ?? identity.pid ?? process.pid,
		kind: base.kind ?? "pi-gateway",
		argv: base.argv ?? identity.argv ?? [...process.argv],
		start_time:
			base.start_time ?? defaultStartTimeSec(identity.pid ?? process.pid),
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
	return raw as unknown as RuntimeStatusRecord;
}
