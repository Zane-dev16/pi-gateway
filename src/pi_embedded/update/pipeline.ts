// pipeline.ts — THE transactional update pipeline (08 §8):
//
//   plan ──▶ snapshot ──▶ apply ──▶ restart-per-kind ──▶ verify ──▶ receipt
//
// Stage contracts realized here:
//   - refusal gates (non-in-place installs) exit 1 BEFORE mutating anything,
//     yet still persist a receipt ("a begun-but-unwritten receipt is a bug");
//   - dirty-tree refusals at BOTH apply sites are terminal+receipted;
//   - an incomplete restart phase fails CLOSED (⇒ failed);
//   - a provably-stale gateway after verify ⇒ outcome `partial`, exit 1;
//   - every terminal path finalizes exactly one receipt with the real exit
//     code; exceptions land in the same guarantee via the outer guard.
//
// Hermes anchors: hermes_cli/main.py:cmd_update command-boundary ownership,
// hermes_cli/update_cmd.py:_cmd_update_impl stage order + hangup wrap.

import type { GatewayClock } from "./clock.js";
import { systemClock } from "./clock.js";
import {
	buildUpdatePlan,
	headShaViaFiles,
	type GitIdentityProbe,
	type PlanStageInputs,
	type RuntimeRecord,
	type UpdatePlan,
} from "./plan.js";
import {
	createPreUpdateSnapshotsAllProfiles,
	type ProfileSnapshotResult,
} from "./snapshot.js";
import {
	applyStage,
	DEFAULT_PULL_TARGET,
	type ApplyStageOutcome,
	type PullTarget,
} from "./apply.js";
import { nodeUpdateCommandRunner, type UpdateCommandRunner } from "./run.js";
import {
	restartFleet,
	type RestartStageResult,
	type RestartUnit,
} from "./restart.js";
import { verifyStage, type FleetEntry } from "./verify.js";
import { installHangupProtection } from "./hangup.js";
import {
	UpdateReceiptWriter,
	exitCodeForOutcome,
	receiptsDirFor,
	type UpdateOutcome,
} from "./receipt.js";

/** Read-only view of one profile home for fleet discovery. */
export interface ProfileHome {
	profile: string;
	home: string;
}

export interface StatusView {
	pid: unknown;
	code_sha: unknown;
	code_version: unknown;
}

export interface UpdatePipelineOptions {
	/** Install tree root the updater would mutate. */
	treeRoot: string;
	/** EVERY profile home on this host — the fleet scope (#66140). */
	homes: readonly ProfileHome[];
	clock?: GatewayClock;
	runner?: UpdateCommandRunner;
	pullTarget?: PullTarget;
	/** Extracted new-source dir enabling the ZIP overlay fallback. */
	zipSourceDir?: string;
	platform?: NodeJS.Platform;
	drainTimeoutMs?: number;
	/**
	 * Live gateway units to restart. Default derives them from each home's
	 * gateway_state.json live PID (canonical stamp vocabulary, 08 §4).
	 */
	units?: readonly RestartUnit[];
	gitProbe?: GitIdentityProbe;
	readStatus?(home: string): StatusView | null;
	liveness?(pid: number): boolean;
}

export interface UpdatePipelineResult {
	outcome: UpdateOutcome;
	exitCode: 0 | 1;
	receiptPath: string | null;
	plan: UpdatePlan | null;
	snapshots: ProfileSnapshotResult[];
	apply: ApplyStageOutcome | null;
	restart: RestartStageResult | null;
	fleet: FleetEntry[];
	error: string | null;
}

import { defaultReadStatus } from "./verify.js";
// ONE file-format reader for the whole subsystem (08 §4 documented schema;
// layering-safe, 01 §5.3) — re-exported for pipeline consumers.
export { defaultReadStatus };

function defaultLiveness(pid: number): boolean {
	try {
		process.kill(pid, 0);
		return true;
	} catch {
		return false;
	}
}

function discoverDefaultUnits(
	homes: readonly ProfileHome[],
	readStatus: (home: string) => StatusView | null,
	liveness: (pid: number) => boolean,
): RestartUnit[] {
	const units: RestartUnit[] = [];
	for (const entry of homes) {
		let record: StatusView | null = null;
		try {
			record = readStatus(entry.home);
		} catch {
			record = null;
		}
		if (record === null) continue;
		const pid = Number(record.pid);
		if (!Number.isInteger(pid) || pid <= 0 || !liveness(pid)) continue;
		units.push({ profile: entry.profile, supervisor: "manual", pid });
	}
	return units;
}

function runtimeRecordsFromUnits(
	units: readonly RestartUnit[],
): RuntimeRecord[] {
	return units.map((unit) => ({
		kind: "gateway",
		profile: unit.profile,
		pid: unit.pid,
		supervisor: unit.supervisor,
		codeSha: null,
		codeVersion: null,
		restartVia: `${unit.supervisor} restart (${unit.profile})`,
		detail: {},
	}));
}

/**
 * Run the full pipeline under hangup protection (DEC-042), returning the
 * outcome + exit code. NEVER throws: any escaped exception finalizes the
 * receipt as `failed` first.
 */
export async function runUpdatePipeline(
	options: UpdatePipelineOptions,
): Promise<UpdatePipelineResult> {
	const clock = options.clock ?? systemClock;
	const guard = installHangupProtection();
	const writer = new UpdateReceiptWriter(
		receiptsDirFor(options.homes[0]?.home ?? "."),
		clock,
	);
	const result: UpdatePipelineResult = {
		outcome: "failed",
		exitCode: 1,
		receiptPath: null,
		plan: null,
		snapshots: [],
		apply: null,
		restart: null,
		fleet: [],
		error: null,
	};
	try {
		const readStatus = options.readStatus ?? defaultReadStatus;
		const liveness = options.liveness ?? defaultLiveness;

		// --- Plan (read-only; refuses nothing itself) ---
		const units =
			options.units ??
			discoverDefaultUnits(options.homes, readStatus, liveness);
		// ONE git-identity seam for plan + verify: injected probes win (tests),
		// otherwise the production file-based HEAD reader serves BOTH stages.
		const gitProbe: GitIdentityProbe =
			options.gitProbe ?? { headSha: headShaViaFiles };
		const planInputs: PlanStageInputs = {
			treeRoot: options.treeRoot,
			profiles: options.homes.map((h) => h.profile),
			runtimes: runtimeRecordsFromUnits(units),
			gitProbe,
		};
		const plan = buildUpdatePlan(planInputs);
		result.plan = plan;
		writer.setPlan(plan);
		writer.recordStep("plan", true, {
			install_method: plan.installMethod,
			updatable_in_place: plan.updatableInPlace,
			expected_sha: plan.expectedSha,
			runtimes: plan.runtimes.length,
		});

		// --- Refusal gate (BEFORE mutating anything, 08 §5/§8) ---
		if (!plan.updatableInPlace) {
			result.error = `update refused: ${plan.installMethod} installs are not updatable in place — ${plan.updateMechanism}`;
			writer.recordStep("refusal-gate", false, { reason: result.error });
			return finishAs(result, writer, "failed");
		}

		// --- Snapshot (best-effort; skips recorded WITH reasons) ---
		const snapshots = createPreUpdateSnapshotsAllProfiles({
			profiles: options.homes.map((h) => ({
				profile: h.profile,
				home: h.home,
			})),
			clockSeconds: clock.nowSeconds(),
		});
		result.snapshots = snapshots.perProfile;
		for (const snap of snapshots.perProfile) {
			writer.recordStep(`snapshot:${snap.profile}`, snap.ok, {
				snapshot_id: snap.snapshotId,
				copied: snap.copied,
				error: snap.error,
			});
			writer.recordSkips(`snapshot:${snap.profile}`, snap.skips);
		}
		writer.recordStep("snapshot-prune-suppressed", snapshots.pruningSuppressed);

		// --- Apply ---
		const applyInputs = {
			root: options.treeRoot,
			runner: options.runner ?? nodeUpdateCommandRunner,
			pullTarget: options.pullTarget ?? DEFAULT_PULL_TARGET,
			platform: options.platform ?? process.platform,
		};
		if (options.zipSourceDir !== undefined) {
			(applyInputs as { zipSourceDir?: string }).zipSourceDir =
				options.zipSourceDir;
		}
		const apply = applyStage(applyInputs);
		result.apply = apply;
		switch (apply.kind) {
			case "pulled":
			case "already-current":
				writer.recordStep("apply", true, { mode: apply.kind });
				break;
			case "overlay-applied":
				writer.recordStep("apply", true, {
					mode: "zip-overlay",
					swapped: apply.swapped.length,
				});
				break;
			case "refused-dirty-tree":
				// BOTH refusal sites land here — each its own receipted failure.
				result.error = `ZIP overlay refused at ${apply.phase}: ${apply.reason}`;
				writer.recordStep("apply", false, {
					refused: apply.phase,
					class: apply.class,
					reason: apply.reason,
				});
				return finishAs(result, writer, "failed");
			case "failed":
				result.error =
					`update step failed (${apply.failureClass}): ${apply.failure.stderr}`.trim();
				writer.recordStep("apply", false, {
					failure_class: apply.failureClass,
					logical_argv: apply.failure.logicalArgv,
					status: apply.failure.status,
					stderr_tail: apply.failure.stderr.split("\n").slice(-12),
					zip_fallback_considered: apply.zipFallbackConsidered,
				});
				return finishAs(result, writer, "failed");
		}

		// Post-pull identity: RE-READ HEAD so verify compares against what the
		// TREE now holds (parity: get_code_identity(refresh=True)) — never the
		// pre-pull sha the plan recorded.
		const postPullSha =
			plan.installMethod === "git"
				? gitProbe.headSha(options.treeRoot)
				: plan.expectedSha;

		// --- Restart-per-kind (only when code actually changed) ---
		const codeChanged = apply.kind !== "already-current";
		let anyDrained = false;
		let restart: RestartStageResult | null = null;
		if (codeChanged && units.length > 0) {
			const ports: Parameters<typeof restartFleet>[1] = { clock };
			if (options.drainTimeoutMs !== undefined) {
				ports.drainTimeoutMs = options.drainTimeoutMs;
			}
			ports.liveness = liveness;
			restart = await restartFleet(units, ports);
			result.restart = restart;
			writer.setRestart({
				outcome: restart.outcome,
				reason: restart.reason,
				pre_restart_pids: restart.preRestartPids,
				surviving_pids: restart.survivingPids,
				units: restart.units,
			});
			anyDrained = restart.units.some((u) => u.drainSignaled);
			if (restart.outcome !== "completed") {
				// Escaped phase w/ unknown survivors ⇒ fail closed (#78574).
				result.error = `restart phase incomplete: ${restart.reason ?? "unknown state"}`;
				writer.recordStep("restart", false, { reason: restart.reason });
				return finishAs(result, writer, "failed");
			}
			writer.recordStep("restart", true, { units: restart.units.length });
		}

		// --- Verify (settle window only after ACTUAL restarts) ---
		const verified = await verifyStage({
			homes: options.homes,
			expectedSha: postPullSha,
			restartedSomething: anyDrained,
			clock,
			liveness,
			readStatus,
		});
		result.fleet = verified.fleet;
		writer.setFleet(verified.fleet);
		writer.recordStep("verify", !verified.anyStale, {
			fleet: verified.fleet,
			settled_ms: verified.settledMs,
			matrix: verified.matrixLines,
		});

		// --- Outcome / receipt ---
		const outcome: UpdateOutcome = verified.anyStale ? "partial" : "success";
		if (verified.anyStale) {
			result.error = "stale gateway(s) still serving pre-update code";
		}
		return finishAs(result, writer, outcome);
	} catch (error) {
		result.error = error instanceof Error ? error.message : String(error);
		writer.recordStep("pipeline-exception", false, { error: result.error });
		return finishAs(result, writer, "failed");
	} finally {
		guard.restore();
	}
}

function finishAs(
	target: UpdatePipelineResult,
	writer: UpdateReceiptWriter,
	outcome: UpdateOutcome,
): UpdatePipelineResult {
	target.outcome = outcome;
	target.exitCode = exitCodeForOutcome(outcome);
	const finalized = writer.finalize(target.outcome, target.error);
	target.receiptPath = finalized.path;
	return target;
}
