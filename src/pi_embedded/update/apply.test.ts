// Apply-stage behavior contracts (08 §8): argv/git-CLASSIFIED failure gates;
// #87304 double dirty-tree refusal; staging-artifact TOCTOU filter; grafting.
// Real git end-to-end lives in apply-two-process.test.ts.

import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	BUILT_ARTIFACT_DIRS,
	classifyFailure,
	graftBuiltArtifacts,
	isDependencyInstallCommand,
	isGitCommand,
	shouldZipFallbackOnUpdateError,
	zipOverlay,
	zipOverlayBlockReason,
	type UpdateCommandFailure,
} from "./apply.js";
import type { CommandResult, UpdateCommandRunner } from "./run.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-update-apply-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

function failure(argv: string[], status = 1): UpdateCommandFailure {
	return { logicalArgv: argv, status, stderr: "boom" };
}

function runnerOf(
	responses: Array<Partial<CommandResult>>,
	log: string[][] = [],
): UpdateCommandRunner {
	let i = 0;
	return (argv, cwd) => {
		log.push([cwd, ...argv]);
		const response = responses[
			Math.min(i, responses.length - 1)
		] as Partial<CommandResult>;
		i += 1;
		return {
			status: 0,
			signal: null,
			stdout: "",
			stderr: "",
			spawnError: null,
			logicalArgv: argv,
			...response,
		};
	};
}

describe("failure classification (update_cmd.py:_called_process_error_* parity)", () => {
	it("classifies git by the BASENAME of argv[0], slash-normalized", () => {
		expect(isGitCommand(failure(["git", "pull"]))).toBe(true);
		expect(isGitCommand(failure(["/usr/bin/git", "pull"]))).toBe(true);
		expect(
			isGitCommand(failure(["C:\\Program Files\\Git\\cmd\\git.exe", "pull"])),
		).toBe(true);
		expect(isGitCommand(failure(["gitk"]))).toBe(false);
		expect(isGitCommand(failure(["npm", "install"]))).toBe(false);
		expect(isGitCommand(failure([]))).toBe(false);
	});

	it("classifies dependency installs — which must NEVER clobber the tree", () => {
		expect(
			isDependencyInstallCommand(failure(["pip", "install", "-r", "req.txt"])),
		).toBe(true);
		expect(
			isDependencyInstallCommand(
				failure(["/usr/bin/uv", "pip", "install", "x"]),
			),
		).toBe(true);
		expect(
			isDependencyInstallCommand(failure(["python", "-m", "ensurepip"])),
		).toBe(true);
		expect(isDependencyInstallCommand(failure(["npm", "install"]))).toBe(true);
		expect(isDependencyInstallCommand(failure(["npm", "ci"]))).toBe(false);
		expect(isDependencyInstallCommand(failure(["git", "install"]))).toBe(false);
	});

	it("gates ZIP fallback on git-classified failures AND Windows only", () => {
		const gitFailure = failure(["git", "pull", "--ff-only"]);
		const depFailure = failure([
			"uv",
			"pip",
			"install",
			"-r",
			"requirements.txt",
		]);
		expect(shouldZipFallbackOnUpdateError(gitFailure, "win32")).toBe(true);
		// A dependency-install failure is NOT a git failure — the pull already
		// succeeded by then (#87304 rationale): no fallback, no clobber.
		expect(shouldZipFallbackOnUpdateError(depFailure, "win32")).toBe(false);
		expect(classifyFailure(depFailure)).toBe("dependency-install");
		// Everywhere else the fallback has no reason to fire.
		expect(shouldZipFallbackOnUpdateError(gitFailure, "linux")).toBe(false);
		expect(shouldZipFallbackOnUpdateError(gitFailure, "darwin")).toBe(false);
	});
});

describe("zipOverlayBlockReason (-uall + fail-closed)", () => {
	it("runs status with --untracked-files=all so showUntrackedFiles=no cannot blind it", () => {
		mkdirSync(join(dir, ".git"));
		const log: string[][] = [];
		const runner = runnerOf([{ stdout: "" }], log);
		expect(zipOverlayBlockReason(dir, runner)).toBeNull();
		expect(log[0]?.slice(1)).toEqual([
			"git",
			"status",
			"--porcelain",
			"--untracked-files=all",
		]);
	});

	it("reports dirtiness from ANY porcelain line — edits or untracked files", () => {
		mkdirSync(join(dir, ".git"));
		const runner = runnerOf([{ stdout: " M src/x.ts\n?? notes.txt\n" }]);
		const block = zipOverlayBlockReason(dir, runner);
		expect(block?.class).toBe("dirty");
		expect(block?.reason).toMatch(/uncommitted changes or untracked files/);
	});

	it("fails CLOSED when git status cannot run — unknown dirtiness is not a license to clobber", () => {
		mkdirSync(join(dir, ".git"));
		const runner = runnerOf([
			{ status: 128, stderr: "fatal: not a git repository" },
		]);
		const block = zipOverlayBlockReason(dir, runner);
		expect(block?.class).toBe("unverifiable");
		expect(block?.reason).toContain("could not check the working tree");
	});

	it("filters OUR staging siblings at the TOCTOU re-check but keeps user files", () => {
		mkdirSync(join(dir, ".git"));
		const stagedStatus =
			"?? dist.pi-update-staging/\n?? src.pi-update-old/\n M config.yaml\n";
		const filtered = zipOverlayBlockReason(
			dir,
			runnerOf([{ stdout: stagedStatus }]),
			{
				ignoreStagingArtifacts: true,
			},
		);
		expect(filtered?.class).toBe("dirty"); // real edit still visible
		const cleanAfterFilter = zipOverlayBlockReason(
			dir,
			runnerOf([
				{ stdout: "?? dist.pi-update-staging/\n?? src.pi-update-old/\n" },
			]),
			{ ignoreStagingArtifacts: true },
		);
		expect(cleanAfterFilter).toBeNull();
	});

	it("returns safe for non-checkout roots without running git", () => {
		const log: string[][] = [];
		const runner = runnerOf([{ stdout: "" }], log);
		expect(zipOverlayBlockReason(join(dir, "no-git-here"), runner)).toBeNull();
		expect(log).toEqual([]);
	});
});

describe("zipOverlay — refusal twice (#87304), graft, two-phase swap", () => {
	function makeCheckout(withDirtyFile: boolean): string {
		const root = join(dir, `checkout-${Math.random().toString(36).slice(2)}`);
		mkdirSync(join(root, ".git"), { recursive: true });
		writeFileSync(join(root, "pkg.ts"), "export {};\n");
		if (withDirtyFile)
			writeFileSync(join(root, "uncommitted.txt"), "user work\n");
		return root;
	}

	function makeSource(): string {
		const source = join(dir, `source-${Math.random().toString(36).slice(2)}`);
		mkdirSync(source, { recursive: true });
		writeFileSync(join(source, "pkg.ts"), "export const fresh = true;\n");
		return source;
	}

	it("refuses UP FRONT on a dirty tree WITHOUT staging anything", () => {
		const root = makeCheckout(true);
		const outcome = zipOverlay({
			root,
			sourceDir: makeSource(),
			runner: runnerOf([{ stdout: "?? uncommitted.txt\n" }]),
		});
		expect(outcome.kind === "refused" && outcome.phase).toBe("up-front");
		expect(readdirSync(root).filter((n) => n.includes("pi-update"))).toEqual(
			[],
		);
	});

	it("refuses at the TOCTOU re-check when dirtiness appears AFTER staging", () => {
		const root = makeCheckout(false);
		let call = 0;
		const runner: UpdateCommandRunner = (argv, _cwd) => {
			call += 1;
			// Call 1 = up-front check: clean. Call 2 = TOCTOU re-check: someone
			// dropped an untracked file into the checkout mid-overlay.
			return {
				status: 0,
				signal: null,
				stdout: call >= 2 ? "?? race.txt\n" : "",
				stderr: "",
				spawnError: null,
				logicalArgv: argv,
			};
		};
		const outcome = zipOverlay({ root, sourceDir: makeSource(), runner });
		expect(outcome.kind === "refused" && outcome.phase).toBe("toctou");
		// Nothing swapped: the live file still holds pre-overlay content.
		expect(existsSync(join(root, "pkg.pi-update-old"))).toBe(false);
	});

	it("applies cleanly on a clean tree via staging renames, preserving .git/.env/node_modules", () => {
		const root = makeCheckout(false);
		writeFileSync(join(root, ".env"), "SECRET=keep\n");
		mkdirSync(join(root, "node_modules"), { recursive: true });
		writeFileSync(join(root, "node_modules", "dep.js"), "module.exports=1;\n");
		const source = makeSource();
		const outcome = zipOverlay({
			root,
			sourceDir: source,
			runner: runnerOf([{ stdout: "" }]),
		});
		expect(outcome.kind === "applied" && outcome.swapped.length > 0).toBe(true);
		expect(readFileSync(join(root, "pkg.ts"), "utf8")).toContain("fresh");
		expect(readFileSync(join(root, ".env"), "utf8")).toBe("SECRET=keep\n");
		expect(existsSync(join(root, "node_modules", "dep.js"))).toBe(true);
		// Staging siblings cleaned after success.
		expect(readdirSync(root).filter((n) => n.includes("pi-update"))).toEqual(
			[],
		);
	});

	it("grafts built artifacts the extracted source lacks BEFORE the swap rides along", () => {
		const root = makeCheckout(false);
		mkdirSync(join(root, BUILT_ARTIFACT_DIRS[0] as string), {
			recursive: true,
		});
		writeFileSync(
			join(root, BUILT_ARTIFACT_DIRS[0] as string, "bundle.js"),
			"built();\n",
		);
		const source = makeSource();
		const grafted = graftBuiltArtifacts(source, root);
		expect(grafted).toEqual(BUILT_ARTIFACT_DIRS);
		const outcome = zipOverlay({
			root,
			sourceDir: source,
			runner: runnerOf([{ stdout: "" }]),
		});
		expect(outcome.kind).toBe("applied");
		// The built artifact SURVIVED the overlay that replaced everything else.
		expect(
			readFileSync(
				join(root, BUILT_ARTIFACT_DIRS[0] as string, "bundle.js"),
				"utf8",
			),
		).toBe("built();\n");
	});
});
