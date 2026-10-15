// pi_gateway/setup/writer.test — Owner B behavior contracts for the sole
// env writer: 0600 merge writes, 0700 home dir, newline stripping, quoting,
// process.env publish, and env-first read precedence. Every case drives the
// writer and asserts literal file bytes, modes, or read results.

import {
	chmodSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	statSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { readSetupVar, writeSetupVars } from "./writer.js";

let home: string;
let savedEnv: Map<string, string | undefined>;

const PROBE_TOKEN = "PI_GATEWAY_SETUP_TEST_TOKEN";
const PROBE_ALLOW = "PI_GATEWAY_SETUP_TEST_ALLOW";

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "setup-writer-"));
	savedEnv = new Map([
		[PROBE_TOKEN, process.env[PROBE_TOKEN]],
		[PROBE_ALLOW, process.env[PROBE_ALLOW]],
	]);
	delete process.env[PROBE_TOKEN];
	delete process.env[PROBE_ALLOW];
});

afterEach(() => {
	for (const [name, value] of savedEnv) {
		if (value === undefined) delete process.env[name];
		else process.env[name] = value;
	}
	rmSync(home, { recursive: true, force: true });
});

describe("0600 merge writes", () => {
	it("merges in place, preserving comments, unrelated keys, and export lines", async () => {
		const envPath = join(home, ".env");
		writeFileSync(
			envPath,
			"# operator config\nUNRELATED_KEEP=1\nexport ROTATE_ME=old\n",
		);
		await writeSetupVars(
			home,
			new Map([
				[PROBE_TOKEN, "token-abc-123"],
				["ROTATE_ME", "new"],
			]),
		);
		const body = readFileSync(envPath, "utf8");
		expect(body).toContain("# operator config");
		expect(body).toContain("UNRELATED_KEEP=1");
		expect(body).toContain(`${PROBE_TOKEN}=token-abc-123`);
		expect(body).toContain("ROTATE_ME=new");
		expect(body).not.toContain("old");
		expect(statSync(envPath).mode & 0o777).toBe(0o600);
	});

	it("reruns converge: rotation replaces without duplicating the key", async () => {
		await writeSetupVars(home, new Map([[PROBE_TOKEN, "first"]]));
		await writeSetupVars(home, new Map([[PROBE_TOKEN, "second"]]));
		const body = readFileSync(join(home, ".env"), "utf8");
		expect(body).toContain(`${PROBE_TOKEN}=second`);
		expect(body).not.toContain("first");
		expect(body.split("\n").filter((l) => l.startsWith(`${PROBE_TOKEN}=`))).toHaveLength(1);
	});

	it("quotes values with spaces and strips raw newlines to one line", async () => {
		await writeSetupVars(
			home,
			new Map([[PROBE_ALLOW, "hello world\ninjected=line"]]),
		);
		const body = readFileSync(join(home, ".env"), "utf8");
		const hit = body.split("\n").filter((l) => l.startsWith(`${PROBE_ALLOW}=`));
		expect(hit).toHaveLength(1);
		expect(hit[0]).toBe(`${PROBE_ALLOW}="hello worldinjected=line"`);
	});

	it("brings the home dir to 0700 and publishes vars to process.env", async () => {
		chmodSync(home, 0o755);
		await writeSetupVars(home, new Map([[PROBE_TOKEN, "live-value-9"]]));
		expect(statSync(home).mode & 0o777).toBe(0o700);
		expect(process.env[PROBE_TOKEN]).toBe("live-value-9");
	});
});

describe("process env first, .env fallback", () => {
	it("exported var wins for the process; unset restores the file value", async () => {
		await writeSetupVars(home, new Map([[PROBE_TOKEN, "file-value-4"]]));
		delete process.env[PROBE_TOKEN];
		expect(readSetupVar(home, PROBE_TOKEN)).toBe("file-value-4");

		process.env[PROBE_TOKEN] = "exported-value-7";
		expect(readSetupVar(home, PROBE_TOKEN)).toBe("exported-value-7");

		delete process.env[PROBE_TOKEN];
		expect(readSetupVar(home, PROBE_TOKEN)).toBe("file-value-4");
	});

	it("missing everywhere reads undefined", async () => {
		expect(readSetupVar(home, PROBE_ALLOW)).toBeUndefined();
	});
});
