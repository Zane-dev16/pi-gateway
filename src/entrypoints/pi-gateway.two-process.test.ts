// pi-gateway.two-process.test.ts — REAL two-process dual-boot contract
// (DEC-080): a live standalone instance holds the runtime lock; a second
// OS process running `pi-gateway run` against the same home exits 0 with
// the lock-held message instead of double-polling, and `status` observes
// the live instance without starting anything.
//
// The child runs the CLI under bare node (the repo's spawned-child TS
// path). It reaches the lock-held exit through the PID-file pre-check,
// which needs no composition import; the parent boots in-process under
// vitest. Full spawned boot of the composed stack is blocked by
// builtins.ts pulling registry parameter properties (see blockers file).

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	composeGatewayLifecycle,
	type AdapterConnectSurface,
} from "./gateway-run.js";
import { LOCK_HELD_MESSAGE } from "./pi-gateway.js";

const CLI_TS = fileURLToPath(new URL("./pi-gateway.ts", import.meta.url));
const RESOLVE_MJS = fileURLToPath(
	new URL("../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-gateway-cli-2proc-home-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

interface ChildRun {
	code: number | null;
	stdout: string;
	stderr: string;
}

function runChild(args: string[]): Promise<ChildRun> {
	return new Promise((resolve, reject) => {
		const child: ChildProcess = spawn(
			process.execPath,
			["--import", RESOLVE_MJS, CLI_TS, ...args],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		let stdout = "";
		let stderr = "";
		child.stdout?.on("data", (chunk: Buffer) => {
			stdout += String(chunk);
		});
		child.stderr?.on("data", (chunk: Buffer) => {
			stderr += String(chunk);
		});
		const timer = setTimeout(() => {
			child.kill("SIGKILL");
			reject(new Error(`child timed out: ${args.join(" ")}`));
		}, 60_000);
		child.once("exit", (code) => {
			clearTimeout(timer);
			resolve({ code, stdout, stderr });
		});
		child.once("error", (err) => {
			clearTimeout(timer);
			reject(err);
		});
	});
}

describe("pi-gateway — two-process dual boot", () => {
	it("a second `run` against a live home exits 0 lock-held; `status` observes it", async () => {
		let connects = 0;
		const adapter: AdapterConnectSurface = {
			connect: () => {
				connects++;
				return true;
			},
			disconnect: () => undefined,
		};
		const composed = composeGatewayLifecycle({
			home,
			installSignals: false,
			platforms: [
				{
					platform: "driver",
					manifest: {
						name: "driver",
						description: "dual-boot driver platform",
						transportShape: "polling",
						requiresEnv: [{ name: "DRIVER_TOKEN" }],
						capabilities: {},
					},
					factory: () => adapter,
				},
			],
			secretReader: () => "tok",
		});
		const startup = await composed.lifecycle.startup();
		expect(startup.ok).toBe(true);
		try {
			expect([...composed.connectedPlatforms()]).toEqual(["driver"]);

			const second = await runChild(["run", "--home", home]);
			expect(second.code).toBe(0);
			expect(second.stdout).toContain(LOCK_HELD_MESSAGE);

			const status = await runChild(["status", "--home", home]);
			expect(status.code).toBe(0);
			expect(status.stdout).toContain("gateway: running");
			expect(status.stdout).toContain(`pid=${process.pid}`);

			// The loser never reached the adapter stage: exactly one connect.
			expect(connects).toBe(1);
		} finally {
			await composed.lifecycle.requestShutdown("planned_stop");
			await composed.lifecycle.waitShutdown();
		}
	});
});
