// verify.ts — verify stage (08 §8): settled window after ACTUAL restarts,
// then a fleet-wide code-identity sweep. A provably-stale gateway FAILS the
// update — outcome `partial`, exit 1 — because automation must never treat a
// mixed-version fleet as healthy (#88654, #69754).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   update_receipt.py:collect_fleet_versions    → collectFleetVersions
//       (one entry per home with a LIVE-PID gateway_state.json; stale =
//       stamped sha ≠ updated checkout HEAD; unknown = no stamp to compare —
//       reported but NOT failing)
//   update_receipt.py:print_fleet_version_matrix
//                                               → fleetHasStaleGateway +
//                                                 formatFleetVersionMatrix
//
// All timing flows through the injected GatewayClock: the ~2s settle window
// runs ONLY when something was actually restarted (08 §8 verified condition),
// and tests drive it deterministically.

import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { GatewayClock } from "./clock.js";
import { systemClock } from "./clock.js";

export const DEFAULT_SETTLE_WINDOW_MS = 2_000;

export type FleetIdentityState = "current" | "stale" | "unknown";

export interface FleetEntry {
	profile: string;
	pid: number;
	codeSha: string | null;
	codeVersion: string | null;
	state: FleetIdentityState;
}

/** Minimal view of the identity fields verify consumes from a status record. */
export interface StatusView {
	pid: unknown;
	code_sha: unknown;
	code_version: unknown;
}

/** Read-only view of one profile's runtime-status snapshot (08 §4 stamps). */
export interface FleetStatusSource {
	profile: string;
	home: string;
}

/**
 * Read ONE profile's runtime-status snapshot by parsing the gateway_state.json
 * FILE FORMAT documented in 08 §4 ("verified field set") directly.
 *
 * LAYERING NOTE (01 §5.3): pi_embedded must never import runner internals
 * (pi_gateway/lifecycle), so this module owns a reader for the DOCUMENTED
 * file schema instead of importing the writer's parser — the shared contract
 * is the SPEC's field set, not a TS symbol. Only these three identity fields
 * are consumed (08 §8 verify); any failure degrades to null (never raises).
 */
export function defaultReadStatus(home: string): StatusView | null {
	try {
		const parsed: unknown = JSON.parse(
			readFileSync(join(home, "gateway_state.json"), "utf8"),
		);
		if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) {
			return null;
		}
		const record = parsed as Record<string, unknown>;
		return {
			pid: record["pid"],
			code_sha: record["code_sha"],
			code_version: record["code_version"],
		};
	} catch {
		return null;
	}
}

export interface FleetProbePorts {
	expectedSha: string | null;
	liveness?(pid: number): boolean;
	/** Read the persisted runtime status for a home. */
	readStatus?(
		home: string,
	): { pid: unknown; code_sha: unknown; code_version: unknown } | null;
}

function defaultLiveness(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

/**
 * Snapshot every profile's gateway code identity vs. the updated tree's HEAD.
 * Never raises; a probe failure yields an empty list (parity docstring).
 */
export function collectFleetVersions(
	homes: readonly FleetStatusSource[],
	ports: FleetProbePorts,
): FleetEntry[] {
	const results: FleetEntry[] = [];
	const liveness = ports.liveness ?? defaultLiveness;
	const readStatus = ports.readStatus ?? defaultReadStatus;
	for (const source of homes) {
		let record: {
			pid: unknown;
			code_sha: unknown;
			code_version: unknown;
		} | null = null;
		try {
			record = readStatus(source.home);
		} catch {
			record = null;
		}
		if (record === null || typeof record !== "object") continue;
		const pid = Number(record.pid);
		if (!Number.isInteger(pid) || pid <= 0 || !liveness(pid)) continue;
		const codeSha =
			typeof record.code_sha === "string" ? record.code_sha : null;
		let state: FleetIdentityState;
		if (!codeSha || !ports.expectedSha) {
			state = "unknown";
		} else if (codeSha === ports.expectedSha) {
			state = "current";
		} else {
			state = "stale";
		}
		results.push({
			profile: source.profile,
			pid,
			codeSha,
			codeVersion:
				typeof record.code_version === "string" ? record.code_version : null,
			state,
		});
	}
	return results;
}

/**
 * True when at least one gateway is PROVABLY stale (still serving pre-update
 * code) — the escalation trigger. `unknown` never fails the update (parity:
 * gateways predating identity stamping have no sha to compare; failing on
 * them would turn this feature's own rollout into a false-positive storm).
 */
export function fleetHasStaleGateway(fleet: readonly FleetEntry[]): boolean {
	return fleet.some((entry) => entry.state === "stale");
}

/** Human-readable matrix (display truncation ONLY — matching uses full argv/shas). */
export function formatFleetVersionMatrix(
	fleet: readonly FleetEntry[],
): string[] {
	const lines: string[] = ["Fleet version check:"];
	for (const entry of fleet) {
		const short =
			entry.codeSha !== null && entry.codeSha.length >= 8
				? entry.codeSha.slice(0, 8)
				: "?";
		if (entry.state === "current") {
			lines.push(
				`✓ ${entry.profile} (pid ${entry.pid}) @ ${short} — up to date`,
			);
		} else if (entry.state === "stale") {
			lines.push(
				`✗ ${entry.profile} (pid ${entry.pid}) @ ${short} — STALE (pre-update code)`,
			);
		} else {
			lines.push(`? ${entry.profile} (pid ${entry.pid}) — version unknown`);
		}
	}
	if (fleetHasStaleGateway(fleet)) {
		lines.push("⚠ Stale gateways keep serving pre-update code until restarted");
	}
	return lines;
}

export interface VerifyStageResult {
	fleet: FleetEntry[];
	anyStale: boolean;
	settledMs: number;
	matrixLines: string[];
}

/**
 * Verify stage: settle ONLY after actual restarts, then sweep the fleet.
 * `expectedSha` comes from the plan's post-pull HEAD re-read by the caller.
 */
export async function verifyStage(options: {
	homes: readonly FleetStatusSource[];
	expectedSha: string | null;
	restartedSomething: boolean;
	clock?: GatewayClock;
	settleWindowMs?: number;
	liveness?(pid: number): boolean;
	readStatus?(
		home: string,
	): { pid: unknown; code_sha: unknown; code_version: unknown } | null;
}): Promise<VerifyStageResult> {
	const clock = options.clock ?? systemClock;
	const settleMs =
		options.settleWindowMs === undefined
			? DEFAULT_SETTLE_WINDOW_MS
			: options.settleWindowMs;
	let settledMs = 0;
	if (options.restartedSomething && settleMs > 0) {
		await clock.sleepMs(settleMs);
		settledMs = settleMs;
	}
	const probePorts: FleetProbePorts = { expectedSha: options.expectedSha };
	if (options.liveness !== undefined) probePorts.liveness = options.liveness;
	if (options.readStatus !== undefined)
		probePorts.readStatus = options.readStatus;
	const fleet = collectFleetVersions(options.homes, probePorts);
	return {
		fleet,
		anyStale: fleetHasStaleGateway(fleet),
		settledMs,
		matrixLines: formatFleetVersionMatrix(fleet),
	};
}
