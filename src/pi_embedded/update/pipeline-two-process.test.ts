// The Phase-5 exit-criterion drill (roadmap (c)/(f)): a TWO-PROFILE host,
// REAL git tree, REAL unit processes — the fleet version matrix must FAIL
// the run when any gateway still serves pre-update code (outcome `partial`,
// exit 1); succeed only when every profile is current; and REFUSALS still
// write receipts before mutating anything.

import {
	existsSync,
	mkdirSync,
	mkdtempSync,
	readdirSync,
	readFileSync,
	rmSync,
	writeFileSync,
	renameSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { runUpdatePipeline } from "./pipeline.js";
import { nodeUpdateCommandRunner } from "./run.js";
import { INSTALL_METHOD_STAMP } from "./plan.js";
import { SNAPSHOTS_DIRNAME } from "./snapshot.js";
import { UPDATE_RECEIPTS_DIRNAME, readLatestPointer } from "./receipt.js";

const DRIVER_TS = fileURLToPath(
	new URL("./testing/unit-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("../../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let dir: string;
const children: ChildProcess[] = [];

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-update-drill-"));
});

afterEach(() => {
	for (const child of children.splice(0)) {
		try {
			child.kill("SIGKILL");
		} catch {
			/* already gone */
		}
	}
	killDetachedReplacements(dir);
	rmSync(dir, { recursive: true, force: true });
});

/** Replacements are detached on purpose (supervisor semantics) — reap by record. */
function killDetachedReplacements(root: string): void {
	const stack = [root];
	while (stack.length > 0) {
		const current = stack.pop() as string;
		let entries: string[];
		try {
			entries = readdirSync(current);
		} catch {
			continue;
		}
		for (const name of entries) {
			const path = join(current, name);
			if (name === "gateway_state.json") {
				try {
					const record = JSON.parse(readFileSync(path, "utf8")) as {
						pid?: number;
					};
					if (typeof record.pid === "number" && record.pid !== process.pid) {
						try {
							process.kill(record.pid, "SIGKILL");
						} catch {
							/* gone */
						}
					}
				} catch {
					/* unreadable */
				}
			} else {
				stack.push(path);
			}
		}
	}
}

// --- Real git fixture --------------------------------------------------------

function runGit(cwd: string, args: string[]): string {
	const result = nodeUpdateCommandRunner(["git", ...args], cwd);
	if (result.status !== 0) {
		throw new Error(`git ${args.join(" ")} failed: ${result.stderr}`);
	}
	return result.stdout;
}

interface TreeFixture {
	checkout: string;
	oldSha: string;
	newSha: string;
}

let fixtureSeq = 0;

function makeTreeFixture(): TreeFixture {
	fixtureSeq += 1;
	const token = `f${fixtureSeq}`;
	const seed = join(dir, `${token}-seed`);
	const bare = join(dir, `${token}-upstream.git`);
	const checkout = join(dir, `${token}-install-tree`);
	runGit(dir, ["init", "--initial-branch=main", seed]);
	writeFileSync(
		join(seed, "package.json"),
		'{"name":"pi-gateway","version":"1.0.0"}',
	);
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
	const oldSha = runGit(seed, ["rev-parse", "HEAD"]).trim();
	// Post-pull identity: one commit pushed upstream while gateways run OLD code.
	writeFileSync(
		join(seed, "package.json"),
		'{"name":"pi-gateway","version":"2.0.0"}',
	);
	writeFileSync(join(seed, "lib.ts"), 'export const v2 = "two";\n');
	runGit(seed, ["add", "."]);
	runGit(seed, [
		"-c",
		"user.email=t@t",
		"-c",
		"user.name=t",
		"commit",
		"--no-gpg-sign",
		"-m",
		"v2",
	]);
	runGit(seed, ["push", "origin", "main"]);
	return {
		checkout,
		oldSha,
		newSha: runGit(seed, ["rev-parse", "HEAD"]).trim(),
	};
}

// --- Unit helpers ------------------------------------------------------------

async function launchUnit(mode: string): Promise<{ pid: number }> {
	const home = join(dir, `unit-home-${Math.random().toString(36).slice(2)}`);
	return launchUnitWithHome(mode, home);
}

async function launchUnitWithHome(
	mode: string,
	home: string,
): Promise<{ pid: number }> {
	mkdirSync(home, { recursive: true });
	const child = spawn(
		process.execPath,
		["--import", RESOLVE_MJS, DRIVER_TS, mode, home],
		{ stdio: ["ignore", "pipe", "pipe"] },
	);
	children.push(child);
	let stderr = "";
	child.stderr?.on("data", (d: Buffer) => {
		stderr += d.toString("utf8");
	});
	for (let i = 0; i < 100; i++) {
		try {
			return {
				pid: Number.parseInt(
					readFileSync(`${home}.unit.pid`, "utf8").trim(),
					10,
				),
			};
		} catch {
			await new Promise<void>((resolvePromise) => {
				setTimeout(resolvePromise, 50);
			});
		}
	}
	throw new Error(`unit never started (${mode}): ${stderr}`);
}

/**
 * Stamp a home with PRE-UPDATE identity by writing gateway_state.json in the
 * EXACT 08 §4 verified field set (layering forbids importing the runner's
 * writer from pi_embedded specs — same seam as testing/unit-driver.ts).
 */
function stampHomeWithIdentity(
	home: string,
	pid: number,
	codeSha: string,
): void {
	const path = join(home, "gateway_state.json");
	const record = {
		pid,
		kind: "pi-gateway",
		argv: ["pi", "gateway", "run"],
		start_time: Math.floor(Date.now() / 1000),
		pi_home: home,
		gateway_state: "running",
		exit_reason: null,
		restart_requested: false,
		active_agents: 0,
		platforms: {},
		updated_at: new Date().toISOString(),
		code_sha: codeSha,
		code_version: "1.0.0",
	};
	const tmp = `${path}.tmp`;
	writeFileSync(tmp, JSON.stringify(record, null, 2));
	renameSync(tmp, path);
}

interface DrillFixture {
	tree: TreeFixture;
	homes: Array<{ profile: string; home: string }>;
}

/**
 * Two-profile host: each unit respawns stamped with the sha CONTENTS of its
 * own sha FILE (read at drain time). Both homes are stamped PRE-UPDATE
 * (oldSha) through THE canonical status writer.
 */
/**
 * Two-profile host: each unit respawns stamped with the sha CONTENTS of its
 * own sha FILE (read at drain time). `arm` picks WHICH identity a profile's
 * replacement serves — "current" (post-pull sha) or "stale" (pre-update
 * sha, the failing arm). Both homes are stamped PRE-UPDATE through THE
 * canonical status writer.
 */
async function makeDrill(
	defaultArm: "current" | "stale",
	workArm: "current" | "stale",
): Promise<DrillFixture> {
	const tree = makeTreeFixture();
	const shaFor = (arm: "current" | "stale") =>
		arm === "current" ? tree.newSha : tree.oldSha;
	const defaultShaFile = join(dir, "default-respawn.sha");
	const workShaFile = join(dir, "work-respawn.sha");
	writeFileSync(defaultShaFile, shaFor(defaultArm));
	writeFileSync(workShaFile, shaFor(workArm));
	const defaultHome = join(dir, "homes-default");
	const workHome = join(dir, "homes-work");
	const a = await launchUnitWithHome(
		`drain-current:${defaultShaFile}`,
		defaultHome,
	);
	const b = await launchUnitWithHome(`drain-current:${workShaFile}`, workHome);
	stampHomeWithIdentity(defaultHome, a.pid, tree.oldSha);
	stampHomeWithIdentity(workHome, b.pid, tree.oldSha);
	return {
		tree,
		homes: [
			{ profile: "default", home: defaultHome },
			{ profile: "work", home: workHome },
		],
	};
}

describe("two-profile update drill (roadmap exit criteria c+f)", () => {
	it("a STALE gateway fails the run: partial outcome, exit 1, matrix recorded in the receipt", async () => {
		if (process.platform === "win32") return; // no SIGHUP semantics there
		// default → CURRENT after respawn; work → STILL SERVING PRE-UPDATE CODE.
		const drill = await makeDrill("current", "stale");

		const result = await runUpdatePipeline({
			treeRoot: drill.tree.checkout,
			homes: drill.homes,
		});
		expect(result.outcome).toBe("partial");
		expect(result.exitCode).toBe(1);
		expect(result.error ?? "").toMatch(/stale/);

		// The fleet version matrix names BOTH profiles with the right verdicts.
		const byProfile = new Map(result.fleet.map((e) => [e.profile, e.state]));
		expect(byProfile.get("default")).toBe("current");
		expect(byProfile.get("work")).toBe("stale");

		// Criterion (f): partial terminal path persists its receipt + pointer.
		expect(result.receiptPath).not.toBeNull();
		const defaultEntry = drill.homes.find((h) => h.profile === "default");
		if (!defaultEntry)
			throw new Error("drill fixture lost its default profile");
		const pointer = readLatestPointer(
			join(defaultEntry.home, UPDATE_RECEIPTS_DIRNAME),
		);
		expect(pointer?.outcome).toBe("partial");
	}, 60_000);

	it("an ALL-CURRENT fleet succeeds with exit 0 and a success receipt", async () => {
		if (process.platform === "win32") return;
		const drill = await makeDrill("current", "current");

		const result = await runUpdatePipeline({
			treeRoot: drill.tree.checkout,
			homes: drill.homes,
		});
		expect(result.outcome).toBe("success");
		expect(result.exitCode).toBe(0);
		expect(result.error).toBeNull();
		for (const entry of result.fleet) expect(entry.state).toBe("current");
		expect(
			readLatestPointer(join(drill.homes[0]!.home, UPDATE_RECEIPTS_DIRNAME))
				?.outcome,
		).toBe("success");
		// The pulled code really landed in the install tree.
		expect(readFileSync(join(drill.tree.checkout, "lib.ts"), "utf8")).toContain(
			"two",
		);
	}, 60_000);

	it("a zip-package install is REFUSED before mutating anything — with a receipt", async () => {
		const tree = makeTreeFixture();
		writeFileSync(join(tree.checkout, INSTALL_METHOD_STAMP), "zip-package\n");
		const home = join(dir, "homes", "only");
		mkdirSync(home, { recursive: true });
		const result = await runUpdatePipeline({
			treeRoot: tree.checkout,
			homes: [{ profile: "default", home }],
		});
		// Exit-2 preflight-refusal convention: a DECLINED run is not a failed one.
		expect(result.outcome).toBe("refused");
		expect(result.exitCode).toBe(2);
		expect(result.error ?? "").toMatch(/not updatable in place/);
		// Nothing mutated BEFORE the gate: no snapshots anywhere.
		expect(existsSync(join(home, SNAPSHOTS_DIRNAME))).toBe(false);
		// Criterion (f): the refusal path STILL wrote its receipt.
		expect(result.receiptPath).not.toBeNull();
		expect(
			readLatestPointer(join(home, UPDATE_RECEIPTS_DIRNAME))?.outcome,
		).toBe("refused");
	}, 60_000);

	it("a dirty-tree ZIP-overlay refusal is receipted at the pipeline boundary", async () => {
		const tree = makeTreeFixture();
		writeFileSync(join(tree.checkout, "uncommitted.txt"), "user work\n"); // dirty
		const source = join(dir, "extracted-zip");
		mkdirSync(source, { recursive: true });
		writeFileSync(join(source, "lib.ts"), "export const fresh = true;\n");
		const home = join(dir, "homes", "dirty");
		mkdirSync(home, { recursive: true });
		const result = await runUpdatePipeline({
			treeRoot: tree.checkout,
			homes: [{ profile: "default", home }],
			pullTarget: { remote: "nonexistent", branch: "nope" }, // force git-classified failure
			zipSourceDir: source,
			platform: "win32",
		});
		expect(result.outcome).toBe("refused");
		expect(result.exitCode).toBe(2);
		expect(result.error ?? "").toMatch(/refused at up-front/);
		expect(existsSync(join(tree.checkout, "uncommitted.txt"))).toBe(true); // no clobber
		const receiptDirs = join(home, UPDATE_RECEIPTS_DIRNAME);
		expect(readdirSync(receiptDirs).some((n) => n.startsWith("receipt-"))).toBe(
			true,
		);
	}, 60_000);

	it("an escaped runner exception still finalizes exactly one failed receipt", async () => {
		const tree = makeTreeFixture();
		const home = join(dir, "homes", "boom");
		mkdirSync(home, { recursive: true });
		const result = await runUpdatePipeline({
			treeRoot: tree.checkout,
			homes: [{ profile: "default", home }],
			runner: () => {
				throw new Error("runner detonated mid-update");
			},
		});
		expect(result.outcome).toBe("failed");
		expect(result.receiptPath).not.toBeNull();
		const receipts = readdirSync(join(home, UPDATE_RECEIPTS_DIRNAME)).filter(
			(n) => n.startsWith("receipt-"),
		);
		expect(receipts).toHaveLength(1);
	}, 60_000);
});
