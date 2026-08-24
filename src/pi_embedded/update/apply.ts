// apply.ts — apply stage: git pull, or the ZIP overlay fallback gated
// STRICTLY on argv/git-CLASSIFIED failures; a dependency-install failure
// must NEVER trigger tree clobbering (08 §8).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   update_cmd.py:_called_process_error_is_git        → isGitCommand
//   update_cmd.py:_called_process_error_is_python_dep_install
//                                                    → isDependencyInstallCommand
//   update_cmd.py:_should_zip_fallback_on_update_error
//                                                    → shouldZipFallbackOnUpdateError
//       (git-classified failure AND Windows-only — a locked .exe breaks git
//       file I/O there; everywhere else the fallback has no reason to fire)
//   update_cmd.py:_zip_overlay_block_reason           → zipOverlayBlockReason
//       (-uall defeats a user-level status.showUntrackedFiles=no that would
//       blind the guard; nonzero git status ⇒ fail closed: unknown dirtiness
//       is not a license to clobber)
//   update_cmd.py:_abort_zip_update_if_dirty_tree     → first refusal site
//   update_cmd.py pre-swap re-check (#87304 TOCTOU)   → second refusal site
//   update_cmd.py:_ZIP_STAGING_ARTIFACT_SUFFIXES      → staging-artifact filter
//   update_cmd.py built-artifact grafting             → graftBuiltArtifacts
//
// Both refusal sites are terminal pipeline paths — each produces its own
// receipted outcome via ApplyStageOutcome ("dirty tree refusal twice, BOTH
// receipted").

import {
	cpSync,
	existsSync,
	mkdirSync,
	readdirSync,
	renameSync,
	rmSync,
} from "node:fs";
import { join } from "node:path";
import type { UpdateCommandRunner } from "./run.js";

/** Classified child-command failure (parity of CalledProcessError + classifier). */
export interface UpdateCommandFailure {
	logicalArgv: readonly string[];
	status: number | null;
	stderr: string;
}

const GIT_EXEC_NAMES = new Set(["git", "git.exe"]);

function basenameNormalized(arg0: string): string {
	// Windows argv may use backslashes; normalize before taking the name
	// (update_cmd.py:_called_process_error_cmd_parts comment).
	return arg0.replace(/\\/g, "/").split("/").pop()!.toLowerCase();
}

/** True when the failed subprocess was git itself. */
export function isGitCommand(
	failure: Pick<UpdateCommandFailure, "logicalArgv">,
): boolean {
	const arg0 = failure.logicalArgv[0];
	if (arg0 === undefined) return false;
	return GIT_EXEC_NAMES.has(basenameNormalized(arg0));
}

const DEP_INSTALL_EXE_NAMES = new Set([
	"pip",
	"pip.exe",
	"pip3",
	"pip3.exe",
	"uv",
	"uv.exe",
	"npm",
	"npm.exe",
	"npm.cmd",
	"yarn",
	"yarn.cmd",
	"pnpm",
	"pnpm.cmd",
]);

/**
 * True when the failed subprocess was a package-manager install (parity of
 * _called_process_error_is_python_dep_install, widened to the JS ecosystem's
 * installers). The pull has ALREADY succeeded when this fires downstream —
 * re-downloading source cannot fix an install and would clobber uncommitted
 * work (#87304 rationale).
 */
export function isDependencyInstallCommand(
	failure: Pick<UpdateCommandFailure, "logicalArgv">,
): boolean {
	const parts = failure.logicalArgv.map((part) => part.toLowerCase());
	if (parts.length === 0) return false;
	if (parts.includes("ensurepip")) return true;
	const exe = basenameNormalized(parts[0] as string);
	return parts.includes("install") && DEP_INSTALL_EXE_NAMES.has(exe);
}

export type FailureClass = "git" | "dependency-install" | "other";

export function classifyFailure(failure: UpdateCommandFailure): FailureClass {
	if (isGitCommand(failure)) return "git";
	if (isDependencyInstallCommand(failure)) return "dependency-install";
	return "other";
}

/**
 * ZIP fallback fires ONLY for a git-classified failure on Windows — never
 * for dependency-install or other stages (update_cmd.py anchor verbatim).
 * `platform` is injected so the gate is testable cross-platform.
 */
export function shouldZipFallbackOnUpdateError(
	failure: UpdateCommandFailure,
	platform: NodeJS.Platform = process.platform,
): boolean {
	return classifyFailure(failure) === "git" && platform === "win32";
}

// --- Dirty-tree guard ------------------------------------------------------

const STAGING_ARTIFACT_SUFFIXES = [".pi-update-staging", ".pi-update-old"];

/** True when a porcelain status line names OUR two-phase-swap artifact. */
export function isStagingArtifactStatusLine(line: string): boolean {
	const payload = line.length >= 3 ? line.slice(3) : line;
	const topLevel = payload
		.trim()
		.replace(/^"/, "")
		.replace(/"$/, "")
		.replace(/\\/g, "/")
		.replace(/\/+$/, "")
		.split("/", 1)[0]!;
	return STAGING_ARTIFACT_SUFFIXES.some((suffix) => topLevel.endsWith(suffix));
}

export interface OverlayBlock {
	reason: string;
	/** "unverifiable" blocks FAIL CLOSED like dirtiness (never a license to clobber). */
	class: "dirty" | "unverifiable";
}

/**
 * Why overlaying onto `root` would destroy work, or null when safe. Runs
 * `git status --porcelain --untracked-files=all` — the -uall override means
 * a user-level status.showUntrackedFiles=no cannot hide untracked files from
 * this guard (update_cmd.py:_zip_overlay_block_reason).
 */
export function zipOverlayBlockReason(
	root: string,
	runner: UpdateCommandRunner,
	options?: { ignoreStagingArtifacts?: boolean },
): OverlayBlock | null {
	if (!existsSync(join(root, ".git"))) return null;
	const result = runner(
		["git", "status", "--porcelain", "--untracked-files=all"],
		root,
	);
	if (result.spawnError !== null || result.status !== 0) {
		const detail = (result.stderr || result.stdout)
			.split("\n")
			.map((line) => line.trim())
			.filter(Boolean)[0];
		return {
			reason: detail
				? `could not check the working tree (${detail})`
				: "could not check the working tree",
			class: "unverifiable",
		};
	}
	let lines = result.stdout
		.split("\n")
		.filter((line) => line.trim().length > 0);
	if (options?.ignoreStagingArtifacts === true) {
		lines = lines.filter((line) => !isStagingArtifactStatusLine(line));
	}
	if (lines.length > 0) {
		return {
			reason: "the working tree has uncommitted changes or untracked files",
			class: "dirty",
		};
	}
	return null;
}

// --- Two-phase overlay ------------------------------------------------------

/** Top-level entries never touched by the overlay (preserve set). */
export const OVERLAY_PRESERVE_SET: ReadonlySet<string> = new Set([
	".git",
	"node_modules",
	".env",
]);

/** Built artifact dirs the source archive lacks but the live tree has. */
export const BUILT_ARTIFACT_DIRS: readonly string[] = ["dist"];

export interface ZipOverlayOptions {
	/** The checkout root being replaced (must be a .git checkout). */
	root: string;
	/** Directory holding the EXTRACTED new-source tree. */
	sourceDir: string;
	runner: UpdateCommandRunner;
}

export type ZipOverlayOutcome =
	| { kind: "applied"; swapped: string[] }
	| { kind: "refused"; phase: "up-front" | "toctou"; block: OverlayBlock }
	| { kind: "failed"; error: string; rolledBack: boolean };

/**
 * Graft built artifacts the extracted source lacks from the live tree
 * BEFORE staging, so they ride the normal swap (update_cmd.py graft parity:
# apps/desktop/release/ in Hermes; dist/ here).
 */
export function graftBuiltArtifacts(
	sourceDir: string,
	liveRoot: string,
	builtDirs: readonly string[] = BUILT_ARTIFACT_DIRS,
): string[] {
	const grafted: string[] = [];
	for (const dir of builtDirs) {
		const src = join(liveRoot, dir);
		const dest = join(sourceDir, dir);
		if (!existsSync(src) || existsSync(dest)) continue;
		cpSync(src, dest, { recursive: true });
		grafted.push(dir);
	}
	return grafted;
}

function topLevelEntries(dir: string): string[] {
	try {
		return readdirSync(dir).filter((name) => !OVERLAY_PRESERVE_SET.has(name));
	} catch {
		return [];
	}
}

/**
 * The ZIP path REFUSES a dirty tree up front AND re-checks immediately
 * pre-swap (#87304 TOCTOU): phase 1 stages every swappable top-level entry
 * into `<entry>.pi-update-staging` siblings WITHOUT touching live files,
 * then the re-check runs with the staging-artifact filter so our own
 * siblings don't read as user dirtiness. Swap = renames only. Mid-swap
 * failure rolls completed entries back from `.pi-update-old`.
 */
export function zipOverlay(options: ZipOverlayOptions): ZipOverlayOutcome {
	const { root, sourceDir, runner } = options;

	// Refusal site 1: up-front (update_cmd.py:_abort_zip_update_if_dirty_tree).
	const upFront = zipOverlayBlockReason(root, runner);
	if (upFront !== null)
		return { kind: "refused", phase: "up-front", block: upFront };

	graftBuiltArtifacts(sourceDir, root);

	// Phase 1 — stage the INCOMING tree without touching live entries
	// (parity: phase 1 extracts/copies the NEW content into *.update-staging
	// siblings; the live tree is only renamed aside at swap time).
	const staged: Array<{ staging: string; old: string; live: string }> = [];
	try {
		for (const entry of topLevelEntries(sourceDir)) {
			const live = join(root, entry);
			const staging = `${live}.pi-update-staging`;
			const old = `${live}.pi-update-old`;
			for (const leftover of [staging, old]) {
				rmSync(leftover, { recursive: true, force: true });
			}
			cpSync(join(sourceDir, entry), staging, { recursive: true });
			staged.push({ staging, old, live });
		}
	} catch (error) {
		for (const item of staged) {
			rmSync(item.staging, { recursive: true, force: true });
		}
		return {
			kind: "failed",
			error: `staging failed: ${error instanceof Error ? error.message : String(error)}`,
			rolledBack: false,
		};
	}

	// Refusal site 2: TOCTOU re-check IMMEDIATELY pre-swap (#87304), with our
	// own staging siblings filtered out of the verdict.
	const toctou = zipOverlayBlockReason(root, runner, {
		ignoreStagingArtifacts: true,
	});
	if (toctou !== null) {
		for (const item of staged) {
			rmSync(item.staging, { recursive: true, force: true });
		}
		return { kind: "refused", phase: "toctou", block: toctou };
	}

	// Phase 2 — atomic renames, rollback on partial failure.
	const swapped: string[] = [];
	const completedOlds: Array<{ old: string; live: string }> = [];
	try {
		for (const item of staged) {
			if (existsSync(item.live)) {
				renameSync(item.live, item.old);
				completedOlds.push({ old: item.old, live: item.live });
			}
			renameSync(item.staging, item.live);
			swapped.push(item.live);
		}
	} catch (error) {
		let rolledBack = false;
		try {
			for (const item of completedOlds) {
				if (existsSync(item.live))
					rmSync(item.live, { recursive: true, force: true });
				renameSync(item.old, item.live);
			}
			rolledBack = true;
		} catch {
			rolledBack = false;
		}
		return {
			kind: "failed",
			error: `swap failed: ${error instanceof Error ? error.message : String(error)}`,
			rolledBack,
		};
	}

	for (const item of completedOlds) {
		rmSync(item.old, { recursive: true, force: true });
	}
	mkdirSync(root, { recursive: true }); // no-op sanity; keeps fs import honest
	return { kind: "applied", swapped };
}

// --- Stage entry ------------------------------------------------------------

export type ApplyStageOutcome =
	| { kind: "pulled"; mode: "git" }
	| { kind: "already-current"; mode: "git" }
	| { kind: "overlay-applied"; swapped: string[] }
	| ({
			kind: "refused-dirty-tree";
			phase: "up-front" | "toctou";
	  } & OverlayBlock)
	| {
			kind: "failed";
			failure: UpdateCommandFailure;
			failureClass: FailureClass;
			zipFallbackConsidered: boolean;
	  };

/** Pull target (parity: `git pull --ff-only upstream main`). */
export interface PullTarget {
	remote: string;
	branch: string;
}

export const DEFAULT_PULL_TARGET: PullTarget = {
	remote: "origin",
	branch: "main",
};

/**
 * Apply stage for a git deployment: fast-forward pull; on a git-classified
 * WINDOWS failure take the ZIP overlay; anything else fails WITHOUT touching
 * the tree. `platform` injection mirrors shouldZipFallbackOnUpdateError.
 */
export function applyStage(options: {
	root: string;
	runner: UpdateCommandRunner;
	pullTarget?: PullTarget;
	/** Extracted new-source dir enabling the ZIP overlay path. */
	zipSourceDir?: string;
	platform?: NodeJS.Platform;
}): ApplyStageOutcome {
	const pullTarget = options.pullTarget ?? DEFAULT_PULL_TARGET;
	const result = options.runner(
		["git", "pull", "--ff-only", pullTarget.remote, pullTarget.branch],
		options.root,
	);
	if (result.spawnError === null && result.status === 0) {
		const upToDate = /already up to date/i.test(result.stdout);
		return upToDate
			? { kind: "already-current", mode: "git" }
			: { kind: "pulled", mode: "git" };
	}
	const failure: UpdateCommandFailure = {
		logicalArgv: result.logicalArgv,
		status: result.status,
		stderr: result.stderr || result.spawnError || "",
	};
	const failureClass = classifyFailure(failure);
	if (
		failureClass === "git" &&
		shouldZipFallbackOnUpdateError(
			failure,
			options.platform ?? process.platform,
		) &&
		options.zipSourceDir !== undefined
	) {
		const outcome = zipOverlay({
			root: options.root,
			sourceDir: options.zipSourceDir,
			runner: options.runner,
		});
		switch (outcome.kind) {
			case "applied":
				return { kind: "overlay-applied", swapped: outcome.swapped };
			case "refused":
				return {
					kind: "refused-dirty-tree",
					phase: outcome.phase,
					reason: outcome.block.reason,
					class: outcome.block.class,
				};
			case "failed":
				return {
					kind: "failed",
					failure,
					failureClass,
					zipFallbackConsidered: true,
				};
		}
	}
	return {
		kind: "failed",
		failure,
		failureClass,
		zipFallbackConsidered: false,
	};
}
