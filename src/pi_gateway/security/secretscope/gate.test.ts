// Behavior contracts for the CI grep gate itself
// (scripts/check-secret-scope.mjs — npm run check:secrets). The gate must
// PASS on the current production tree and FAIL on each seeded banned-shape
// class — proving it detects the DEC-009 forbidden fallback shapes rather
// than merely exiting successfully. Fixture trees live in mkdtemp temp dirs;
// src/ is never polluted. Precedent: src/pi_gateway/lifecycle/layering.test.ts.

import { execFile } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { promisify } from "node:util";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

const exec = promisify(execFile);
const SCRIPT = new URL(
	"../../../../scripts/check-secret-scope.mjs",
	import.meta.url,
).pathname;

let fixtureRoot: string;

beforeEach(() => {
	fixtureRoot = mkdtempSync(join(tmpdir(), "secretscope-gate-fixture-"));
	mkdirSync(join(fixtureRoot, "src"), { recursive: true });
});

afterEach(() => {
	rmSync(fixtureRoot, { recursive: true, force: true });
});

function writeTree(files: Record<string, string>): void {
	for (const [rel, content] of Object.entries(files)) {
		const path = join(fixtureRoot, rel);
		mkdirSync(join(path, ".."), { recursive: true });
		writeFileSync(path, content);
	}
}

function runGate(root?: string): Promise<{ code: number; output: string }> {
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

const WRAPPER = "src/pi_gateway/security/secretscope/wrapper.ts";
const SANCTIONED_WRAPPER =
	"export function getScopedSecret(name: string): string | undefined {\n" +
	"\ttry {\n" +
	"\t\treturn undefined;\n" +
	"\t} catch (err) {\n" +
	"\t\tif (err instanceof UnscopedSecretError) {\n" +
	"\t\t\treturn process.env[name];\n" +
	"\t\t}\n" +
	"\t\tthrow err;\n" +
	"\t}\n" +
	"}\n";

describe("check-secret-scope.mjs (npm run check:secrets)", () => {
	it("PASSES on the current production tree (exit 0)", async () => {
		const result = await runGate();
		expect(result.code).toBe(0);
		expect(result.output).toContain("secret-scope OK");
	}, 20_000);

	it("detects the banned catch→env fallback OUTSIDE the canonical wrapper (DEC-009)", async () => {
		writeTree({
			"src/pi_platforms/rogue.ts":
				"function f() {\n" +
				"\ttry {\n" +
				"\t\treturn undefined;\n" +
				"\t} catch (err) {\n" +
				"\t\tif (err instanceof UnscopedSecretError) {\n" +
				"\t\t\treturn process.env.TOKEN;\n" +
				"\t\t}\n" +
				"\t\tthrow err;\n" +
				"\t}\n" +
				"}\n" +
				"void f;\n",
		});
		const result = await runGate(fixtureRoot);
		expect(result.code).toBe(1);
		expect(result.output).toContain("UNSCOPED_CATCH_FALLBACK");
		expect(result.output).toContain("src/pi_platforms/rogue.ts");
	});

	it("the SAME shape at THE canonical wrapper path is EXEMPT (sanctioned copy)", async () => {
		writeTree({ [WRAPPER]: SANCTIONED_WRAPPER });
		const result = await runGate(fixtureRoot);
		expect(result.code).toBe(0);
		expect(result.output).toContain("secret-scope OK");
	});

	it("a SECOND copy of the sanctioned fallback is flagged even when hand-rolled elsewhere", async () => {
		// 06 §3.2: the wrapper is canonicalized ONCE; adapters copying the
		// pattern into their own modules are exactly the #86905 regression.
		writeTree({
			[WRAPPER]: SANCTIONED_WRAPPER,
			"src/pi_platforms/feishu-copy.ts": SANCTIONED_WRAPPER,
		});
		const result = await runGate(fixtureRoot);
		expect(result.code).toBe(1);
		expect(result.output).toContain("UNSCOPED_CATCH_FALLBACK");
		expect(result.output).toContain("feishu-copy.ts");
	});

	it("detects the coalesced after-a-scoped-miss fallback (`getScopedSecret(...) ?? process.env…`)", async () => {
		writeTree({
			"src/pi_gateway/coalesce.ts":
				'import { getScopedSecret } from "./security/secretscope/index.js";\n' +
				'export const K = getScopedSecret("K") ?? process.env.K;\n',
		});
		const result = await runGate(fixtureRoot);
		expect(result.code).toBe(1);
		expect(result.output).toContain("COALESCED_SCOPED_MISS_FALLBACK");
	});

	it("detects raw env reads beside scope resolution (RAW_ENV_BESIDE_SCOPE)", async () => {
		writeTree({
			"src/pi_gateway/mixed.ts":
				'import { getSecret } from "./security/secretscope/index.js";\n' +
				"export function read(): string | undefined {\n" +
				'\treturn getSecret("K") ?? undefined;\n' +
				"}\n" +
				"export const OTHER = process.env.OTHER;\n" +
				"void OTHER;\n",
		});
		const result = await runGate(fixtureRoot);
		expect(result.code).toBe(1);
		expect(result.output).toContain("RAW_ENV_BESIDE_SCOPE");
	});

	it("engine internals reading process env do NOT trip the gate (exempt implementation dir)", async () => {
		writeTree({
			[WRAPPER]: SANCTIONED_WRAPPER,
			"src/pi_gateway/security/secretscope/resolve.ts":
				'import { isGlobalEnv } from "./global-env.js";\n' +
				"export function getSecret(n: string): string | undefined {\n" +
				"\tif (isGlobalEnv(n)) return process.env[n];\n" +
				"\treturn undefined;\n" +
				"}\n",
		});
		const result = await runGate(fixtureRoot);
		expect(result.code).toBe(0);
	});

	it("test files are outside the gate's domain (detectors simulate banned shapes)", async () => {
		writeTree({
			"src/pi_platforms/detector.test.ts":
				"// a behavior contract simulating the banned shape\n" +
				"it('proves env is NOT read', () => {});\n" +
				"const bad = getScopedSecret('K') ?? process.env.K;\n" +
				"void bad;\n",
		});
		const result = await runGate(fixtureRoot);
		expect(result.code).toBe(0);
	});

	it("clean tree without any scope references passes (plain process.env use is legal)", async () => {
		writeTree({
			"src/pi_home.ts":
				"export const HOME = process.env.PI_HOME;\nvoid HOME;\n",
		});
		const result = await runGate(fixtureRoot);
		expect(result.code).toBe(0);
	});

	it("usage error exits 2", async () => {
		const result = await runGate("   ");
		expect(result.code).toBe(2);
	});
});
