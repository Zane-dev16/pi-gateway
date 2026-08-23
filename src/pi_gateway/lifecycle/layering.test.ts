// Behavior contracts for the layering gate (01 §5.3 downward-only rule):
// the script must pass on the CURRENT tree (spawned as a real child process,
// exit 0) and FAIL on synthesized violating fixtures — proving the gate
// detects upward imports rather than merely exiting successfully.

import { execFile } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

const exec = promisify(execFile);
const SCRIPT = new URL("../../../scripts/check-layering.mjs", import.meta.url)
	.pathname;

let fixtureRoot: string;

beforeEach(() => {
	fixtureRoot = mkdtempSync(join(tmpdir(), "pi-layering-fixture-"));
});

afterEach(() => {
	rmSync(fixtureRoot, { recursive: true, force: true });
});

function runScript(root?: string): Promise<{ code: number; output: string }> {
	return new Promise((resolvePromise) => {
		exec("node", root === undefined ? [SCRIPT] : [SCRIPT, root]).then(
			({ stdout }) => resolvePromise({ code: 0, output: stdout }),
			(err: { code?: number; stdout?: string; stderr?: string }) =>
				resolvePromise({
					code: err.code ?? -1,
					output: `${err.stdout ?? ""}${err.stderr ?? ""}`,
				}),
		);
	});
}

function writeTree(files: Record<string, string>): void {
	for (const [rel, content] of Object.entries(files)) {
		const path = join(fixtureRoot, rel);
		mkdirSync(join(path, ".."), { recursive: true });
		writeFileSync(path, content);
	}
}

describe("check-layering script (npm run check:layering)", () => {
	it("PASSES on the current production tree (exit 0)", async () => {
		const result = await runScript();
		expect(result.code).toBe(0);
		expect(result.output).toContain("layering OK");
	}, 20_000);

	it("FAILS when pi_state imports pi_gateway (UPWARD_IMPORT)", async () => {
		writeTree({
			"src/pi_gateway/thing.ts": "export const thing = 1;\n",
			"src/pi_state/mutant.ts":
				'import { thing } from "../pi_gateway/thing.js";\nvoid thing;\n',
		});
		const result = await runScript(fixtureRoot);
		expect(result.code).toBe(1);
		expect(result.output).toContain("pi_state/mutant.ts");
		expect(result.output).toContain("UPWARD_IMPORT");
		expect(result.output).toContain("DOWNWARD only");
	});

	it("FAILS when pi_agent_core imports pi_gateway (banned sibling direction)", async () => {
		writeTree({
			"src/pi_gateway/runner.ts": "export const runner = 1;\n",
			"src/pi_agent_core/mutant.ts":
				'import { runner } from "../pi_gateway/runner.js";\nvoid runner;\n',
		});
		const result = await runScript(fixtureRoot);
		expect(result.code).toBe(1);
		expect(result.output).toContain("pi_agent_core/mutant.ts");
		expect(result.output).toContain("UPWARD_IMPORT");
	});

	it("FAILS when pi_home imports anything above it", async () => {
		writeTree({
			"src/pi_state/store.ts": "export const s = 1;\n",
			"src/pi_home.ts": 'import { s } from "./pi_state/store.js";\nvoid s;\n',
		});
		const result = await runScript(fixtureRoot);
		expect(result.code).toBe(1);
		expect(result.output).toContain("pi_home.ts");
	});

	it("ALLOWS the sanctioned runner edge: pi_gateway importing pi_agent_core and pi_state", async () => {
		writeTree({
			"src/pi_agent_core/loop.ts": "export const loop = 1;\n",
			"src/pi_state/substrate.ts": "export const substrate = 1;\n",
			"src/pi_gateway/runner.ts":
				'import { loop } from "../pi_agent_core/loop.js";\n' +
				'import { substrate } from "../pi_state/substrate.js";\n' +
				"void loop; void substrate;\n",
		});
		const result = await runScript(fixtureRoot);
		expect(result.code).toBe(0);
	});

	it("FAILS when pi_platforms imports runner internals (RUNNER_INTERNAL)", async () => {
		writeTree({
			"src/pi_gateway/lifecycle/index.ts": "export const x = 1;\n",
			"src/pi_platforms/base.ts":
				'import { x } from "../pi_gateway/lifecycle/index.js";\nvoid x;\n',
		});
		const result = await runScript(fixtureRoot);
		expect(result.code).toBe(1);
		expect(result.output).toContain("RUNNER_INTERNAL");
	});

	it("external (bare) specifiers never trip the gate", async () => {
		writeTree({
			"src/pi_state/store.ts":
				'import Database from "better-sqlite3";\nimport { join } from "node:path";\nvoid Database; void join;\n',
		});
		const result = await runScript(fixtureRoot);
		expect(result.code).toBe(0);
	});
});
