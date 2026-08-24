// Plan-stage behavior contracts (08 §5): deployment-kind classification from
// REAL tree state, plan schema fidelity, version/HEAD reads. Real git-checkout
// classification end-to-end lives in the two-process suites; here fixtures are
// exact on-disk states (stamp file, .git layout, package.json).

import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	INSTALL_METHOD_STAMP,
	buildUpdatePlan,
	classifyDeploymentKind,
	headShaViaFiles,
	readTreeVersion,
} from "./plan.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-update-plan-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

describe("classifyDeploymentKind", () => {
	it("classifies a .git checkout as the only in-place kind (08 §5)", () => {
		mkdirSync(join(dir, ".git"), { recursive: true });
		const c = classifyDeploymentKind(dir);
		expect(c.kind).toBe("git");
		expect(c.updatableInPlace).toBe(true);
	});

	it("treats a .git FILE (worktree) as a checkout, not an external install", () => {
		writeFileSync(join(dir, ".git"), "gitdir: /somewhere/else\n");
		expect(classifyDeploymentKind(dir).kind).toBe("git");
	});

	it("honors the authoritative code-scoped zip-package stamp over structure", () => {
		// A stamped package install may still carry a stale package.json —
		// the stamp describes THE RUNNING TREE (config.py:detect_install_method).
		writeFileSync(join(dir, "package.json"), '{"name":"x","version":"1.0.0"}');
		writeFileSync(join(dir, INSTALL_METHOD_STAMP), "zip-package\n");
		const c = classifyDeploymentKind(dir);
		expect(c.kind).toBe("zip-package");
		expect(c.updatableInPlace).toBe(false);
		expect(c.updateMechanism).toMatch(/reinstall/i);
	});

	it("classifies npm-class installs (package.json without .git) as external", () => {
		writeFileSync(
			join(dir, "package.json"),
			'{"name":"pi-gateway","version":"0.1.0"}',
		);
		const c = classifyDeploymentKind(dir);
		expect(c.kind).toBe("npm-class");
		expect(c.updatableInPlace).toBe(false);
		expect(c.updateMechanism).toMatch(/npm/i);
	});

	it("ignores an unknown stamp value and falls through to structural detection", () => {
		writeFileSync(join(dir, INSTALL_METHOD_STAMP), "carrier-pigeon\n");
		mkdirSync(join(dir, ".git"), { recursive: true });
		expect(classifyDeploymentKind(dir).kind).toBe("git");
	});

	it("returns unknown (not updatable) for an unrecognizable directory", () => {
		const c = classifyDeploymentKind(dir);
		expect(c.kind).toBe("unknown");
		expect(c.updatableInPlace).toBe(false);
	});
});

describe("headShaViaFiles", () => {
	it("resolves detached HEAD shas directly", () => {
		mkdirSync(join(dir, ".git"), { recursive: true });
		const sha = "1234567890abcdef1234567890abcdef12345678";
		writeFileSync(join(dir, ".git", "HEAD"), `${sha}\n`);
		expect(headShaViaFiles(dir)).toBe(sha);
	});

	it("resolves symbolic HEAD through its ref file", () => {
		mkdirSync(join(dir, ".git", "refs", "heads"), { recursive: true });
		const sha = "aabbccdd00112233445566778899aabbccddeeff";
		writeFileSync(join(dir, ".git", "HEAD"), "ref: refs/heads/main\n");
		writeFileSync(join(dir, ".git", "refs", "heads", "main"), `${sha}\n`);
		expect(headShaViaFiles(dir)).toBe(sha);
	});

	it("returns null for absent or malformed git dirs — never raises", () => {
		expect(headShaViaFiles(dir)).toBeNull();
		mkdirSync(join(dir, ".git"), { recursive: true });
		writeFileSync(join(dir, ".git", "HEAD"), "garbage\n");
		expect(headShaViaFiles(dir)).toBeNull();
	});
});

describe("buildUpdatePlan", () => {
	it("records expectedSha only for in-place kinds and versions from the tree", () => {
		mkdirSync(join(dir, ".git"), { recursive: true });
		const sha = "f".repeat(40);
		writeFileSync(join(dir, ".git", "HEAD"), `ref: refs/heads/main\n`);
		mkdirSync(join(dir, ".git", "refs", "heads"), { recursive: true });
		writeFileSync(join(dir, ".git", "refs", "heads", "main"), `${sha}\n`);
		writeFileSync(
			join(dir, "package.json"),
			JSON.stringify({ name: "pi-gateway", version: "2.3.4" }),
		);
		const plan = buildUpdatePlan({
			treeRoot: dir,
			profiles: ["default", "work"],
			runtimes: [
				{
					kind: "gateway",
					profile: "default",
					pid: 4242,
					supervisor: "manual",
					codeSha: null,
					codeVersion: null,
					restartVia: "manual",
					detail: {},
				},
			],
		});
		expect(plan.installMethod).toBe("git");
		expect(plan.expectedSha).toBe(sha);
		// On-disk manifest, not installed metadata (_read_project_version parity).
		expect(plan.expectedVersion).toBe("2.3.4");
		expect(plan.profiles).toEqual(["default", "work"]);
		expect(plan.runtimes).toHaveLength(1);
		expect(plan.runtimes[0]?.pid).toBe(4242);
	});

	it("withholds expectedSha for non-in-place kinds (nothing to compare yet)", () => {
		writeFileSync(join(dir, INSTALL_METHOD_STAMP), "npm-class\n");
		const plan = buildUpdatePlan({ treeRoot: dir, profiles: ["default"] });
		expect(plan.expectedSha).toBeNull();
		expect(readTreeVersion(dir)).toBeNull();
	});
});
