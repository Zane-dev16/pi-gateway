// Real-git apply contracts (08 §8): fast-forward pull against a REAL
// upstream; non-fast-forward failures classify as git WITHOUT fallback on
// POSIX; the ZIP overlay refuses a dirty checkout up front and applies with
// graft preservation end-to-end.

import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readFileSync,
	readdirSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { DEFAULT_PULL_TARGET, applyStage, zipOverlay } from "./apply.js";
import { nodeUpdateCommandRunner } from "./run.js";
import { headShaViaFiles } from "./plan.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-update-git-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

const GIT = "git";

function runGit(cwd: string, args: string[]): string {
	const result = nodeUpdateCommandRunner([GIT, ...args], cwd);
	if (result.status !== 0) {
		throw new Error(`git ${args.join(" ")} failed: ${result.stderr}`);
	}
	return result.stdout;
}

/** seed repo → bare `origin` → cloned checkout, one commit on main. */
function makeFixture(): { seed: string; checkout: string } {
	const seed = join(dir, "seed");
	const bare = join(dir, "upstream.git");
	const checkout = join(dir, "checkout");
	runGit(dir, ["init", "--initial-branch=main", seed]);
	writeFileSync(
		join(seed, "package.json"),
		'{"name":"pi-gateway","version":"1.0.0"}',
	);
	writeFileSync(join(seed, "lib.ts"), "export const v1 = 1;\n");
	// Real checkouts IGNORE generated/secret paths — the overlay guard must
	// not read them as user dirtiness (parity: real trees gitignore these).
	writeFileSync(join(seed, ".gitignore"), "node_modules/\ndist/\n.env\n");
	runGit(seed, ["add", "."]);
	runGit(seed, [
		"-c",
		"user.email=t@t",
		"-c",
		"user.name=t",
		"commit",
		"--no-gpg-sign",
		"-m",
		"v1",
	]);
	runGit(dir, ["clone", "--bare", seed, bare]);
	// `git init` creates no remote — wire seed → bare so pushes work.
	runGit(seed, ["remote", "add", "origin", bare]);
	runGit(dir, ["clone", bare, checkout]);
	return { seed, checkout };
}

function pushCommit(seed: string, version: string): void {
	writeFileSync(
		join(seed, "package.json"),
		JSON.stringify({ name: "pi-gateway", version }),
	);
	writeFileSync(join(seed, "lib.ts"), `export const v2 = "${version}";\n`);
	runGit(seed, ["add", "."]);
	runGit(seed, [
		"-c",
		"user.email=t@t",
		"-c",
		"user.name=t",
		"commit",
		"--no-gpg-sign",
		"-m",
		version,
	]);
	runGit(seed, ["push", "origin", "main"]);
}

describe("git pull path (real children)", () => {
	it("fast-forwards a behind checkout — applyStage reports pulled + new HEAD visible", () => {
		const { seed, checkout } = makeFixture();
		const before = headShaViaFiles(checkout);
		pushCommit(seed, "2.0.0");

		const outcome = applyStage({
			root: checkout,
			runner: nodeUpdateCommandRunner,
			pullTarget: DEFAULT_PULL_TARGET,
			platform: process.platform,
		});
		expect(outcome.kind).toBe("pulled");
		expect(readFileSync(join(checkout, "lib.ts"), "utf8")).toContain("2.0.0");
		expect(headShaViaFiles(checkout)).not.toBeNull();
		expect(headShaViaFiles(checkout)).not.toBe(before);
	}, 30_000);

	it("an up-to-date checkout reports already-current without mutating anything", () => {
		const { checkout } = makeFixture();
		const shaBefore = headShaViaFiles(checkout);
		const outcome = applyStage({
			root: checkout,
			runner: nodeUpdateCommandRunner,
			pullTarget: DEFAULT_PULL_TARGET,
		});
		expect(outcome.kind).toBe("already-current");
		expect(headShaViaFiles(checkout)).toBe(shaBefore);
	}, 30_000);

	it("a diverged (non-fast-forward) checkout fails GIT-classified with NO fallback on POSIX", () => {
		const { seed, checkout } = makeFixture();
		pushCommit(seed, "2.0.0"); // upstream moves ahead…
		// …while the checkout commits locally: histories DIVERGE.
		writeFileSync(join(checkout, "local.txt"), "diverged\n");
		runGit(checkout, ["add", "."]);
		runGit(checkout, [
			"-c",
			"user.email=t@t",
			"-c",
			"user.name=t",
			"commit",
			"--no-gpg-sign",
			"-m",
			"local",
		]);
		const outcome = applyStage({
			root: checkout,
			runner: nodeUpdateCommandRunner,
			pullTarget: DEFAULT_PULL_TARGET,
			zipSourceDir: join(dir, "unused-source"),
			platform: "linux",
		});
		if (outcome.kind !== "failed") {
			throw new Error(`expected failed, got ${outcome.kind}`);
		}
		expect(outcome.failureClass).toBe("git");
		expect(outcome.zipFallbackConsidered).toBe(false); // linux ⇒ no overlay
		// The local commit SURVIVES — nothing clobbered the tree.
		expect(existsSync(join(checkout, "local.txt"))).toBe(true);
	}, 30_000);
});

describe("ZIP overlay end-to-end (real git status under the guard)", () => {
	function makeSource(): string {
		const source = join(dir, "extracted-zip");
		mkdirSync(source, { recursive: true });
		writeFileSync(
			join(source, "package.json"),
			'{"name":"pi-gateway","version":"9.9.9"}',
		);
		writeFileSync(join(source, "lib.ts"), "export const fresh = true;\n");
		return source;
	}

	it("refuses UP FRONT on a dirty checkout — receipted refusal, zero mutation", () => {
		const { checkout } = makeFixture();
		writeFileSync(join(checkout, "uncommitted.txt"), "user work\n"); // dirty!
		const outcome = applyStage({
			root: checkout,
			runner: nodeUpdateCommandRunner,
			pullTarget: { remote: "nonexistent", branch: "nope" }, // force git-classified failure
			zipSourceDir: makeSource(),
			platform: "win32", // gate open
		});
		expect(outcome.kind === "refused-dirty-tree" && outcome.phase).toBe(
			"up-front",
		);
		// The user's uncommitted file is untouched; no staging siblings exist.
		expect(existsSync(join(checkout, "uncommitted.txt"))).toBe(true);
	}, 30_000);

	it("applies cleanly via real renames when the tree is clean, preserving .git/.env/node_modules", () => {
		const { checkout } = makeFixture();
		writeFileSync(join(checkout, ".env"), "SECRET=keep\n");
		mkdirSync(join(checkout, "node_modules"), { recursive: true });
		writeFileSync(join(checkout, "node_modules", "dep.js"), "m\n");
		const source = makeSource();

		// Direct overlay invocation (the win32-gated entry into this path).
		const outcome = zipOverlay({
			root: checkout,
			sourceDir: source,
			runner: nodeUpdateCommandRunner,
		});
		if (outcome.kind !== "applied") {
			throw new Error(`expected applied, got ${JSON.stringify(outcome)}`);
		}
		expect(readFileSync(join(checkout, "lib.ts"), "utf8")).toContain("fresh");
		expect(readFileSync(join(checkout, ".env"), "utf8")).toBe("SECRET=keep\n");
		expect(existsSync(join(checkout, "node_modules", "dep.js"))).toBe(true);
		expect(existsSync(join(checkout, ".git"))).toBe(true);
		// Staging artifacts cleaned post-swap.
		const leftovers = readdirSync(checkout) as string[];
		expect(leftovers.filter((n) => n.includes("pi-update"))).toEqual([]);
	}, 30_000);

	it("grafts built artifacts (dist/) the extracted source lacks before swapping", () => {
		const { checkout } = makeFixture();
		mkdirSync(join(checkout, "dist"), { recursive: true });
		writeFileSync(join(checkout, "dist", "bundle.js"), "built();\n");
		const source = makeSource();
		const outcome = zipOverlay({
			root: checkout,
			sourceDir: source,
			runner: nodeUpdateCommandRunner,
		});
		expect(outcome.kind).toBe("applied");
		// The graft rode the swap: built artifact survives full replacement.
		expect(readFileSync(join(checkout, "dist", "bundle.js"), "utf8")).toBe(
			"built();\n",
		);
	}, 30_000);
});
