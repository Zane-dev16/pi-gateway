// pi-gateway.test.ts — standalone CLI behavior contracts (DEC-080).
//
// Calls the CLI the way its users do (argv in, exit code plus printed
// lines out) and asserts literal observable effects: a real boot connects
// a platform and runs a turn, logs land under the profile home, status
// reports without starting anything, and a contended lock exits 0 with
// the lock-held message. Spawned double-boot plus live stop live in
// pi-gateway.two-process.test.ts.

import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MatrixAdapterCore } from "../pi_platforms/matrix/matrix-adapter.js";
import { FakeMatrixHomeserver } from "../pi_platforms/matrix/matrix-fake-server.js";
import { MATRIX_MANIFEST } from "../pi_platforms/matrix/manifest.js";
import { buildSessionKey } from "../pi_gateway/resolution/session-key.js";
import type { IncomingEvent } from "../pi_gateway/guards/index.js";
import type { Logger } from "../pi_gateway/lifecycle/shutdown.js";
import { RuntimeLock, buildPidRecord, pidFilePath } from "../pi_gateway/lifecycle/instance-guard.js";
import { plannedStopMarkerPath } from "../pi_gateway/lifecycle/markers.js";
import {
	createScriptedModelEnv,
	fauxAssistantMessage,
	type ScriptedModelEnv,
} from "../pi_agent_core/testing/faux-model.js";
import { matrixHosting } from "./platform-hosting.js";
import { buildProductionTurnRunnerFactory } from "./production-runner.js";
import {
	LOCK_HELD_MESSAGE,
	piGatewayMain,
	statusCommand,
	stopCommand,
} from "./pi-gateway.js";

const HUMAN = "@human:fake.example";
const ROOM = "!room:fake.example";

function matrixSecrets(): (name: string) => string | undefined {
	const required = new Set(MATRIX_MANIFEST.requiresEnv.map((s) => s.name));
	return (name: string) => (required.has(name) ? "cred" : undefined);
}

function matrixEvent(sender: string, text: string): IncomingEvent {
	return {
		messageType: "text",
		text,
		source: {
			platform: "matrix",
			chatType: "dm",
			userId: sender,
			chatId: ROOM,
		},
	};
}

function matrixKey(sender: string): string {
	return buildSessionKey({
		platform: "matrix",
		chatType: "dm",
		userId: sender,
		chatId: ROOM,
	});
}

function spyLogger(): {
	log: Logger;
	calls: Array<{ level: string; message: string }>;
} {
	const calls: Array<{ level: string; message: string }> = [];
	return {
		calls,
		log: {
			info: (m: string) => {
				calls.push({ level: "info", message: m });
			},
			warn: (m: string) => {
				calls.push({ level: "warn", message: m });
			},
			error: (m: string) => {
				calls.push({ level: "error", message: m });
			},
		},
	};
}

async function waitFor(cond: () => boolean, ms = 15000): Promise<void> {
	const start = Date.now();
	for (;;) {
		if (cond()) return;
		if (Date.now() - start > ms)
			throw new Error("timed out waiting for condition");
		await new Promise((r) => setTimeout(r, 25));
	}
}

let home: string;
let env: ScriptedModelEnv | null = null;
const savedEnv = new Map<string, string | undefined>();

function setEnv(vars: Record<string, string | undefined>): void {
	for (const [k, v] of Object.entries(vars)) {
		if (!savedEnv.has(k)) savedEnv.set(k, process.env[k]);
		if (v === undefined) delete process.env[k];
		else process.env[k] = v;
	}
}

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-gateway-cli-home-"));
});

afterEach(async () => {
	for (const [k, v] of savedEnv) {
		if (v === undefined) delete process.env[k];
		else process.env[k] = v;
	}
	savedEnv.clear();
	if (env !== null) {
		rmSync(env.home, { recursive: true, force: true });
		env = null;
	}
	rmSync(home, { recursive: true, force: true });
});

describe("pi-gateway run — standalone boot proof", () => {
	it("boots the composed stack, connects a platform, runs a turn, and lands logs under the profile home", async () => {
		setEnv({
			MATRIX_ALLOWED_USERS: HUMAN,
			PI_GATEWAY_MODEL: undefined,
			PI_GATEWAY_PROVIDER: undefined,
		});
		env = await createScriptedModelEnv();
		const e = env;
		e.faux.setResponses([fauxAssistantMessage("STANDALONE-OK")]);
		const model = e.faux.getModel();
		if (!model) throw new Error("faux provider exposed no model");

		let adapter: MatrixAdapterCore | null = null;
		const out: string[] = [];
		const spy = spyLogger();
		const connected: string[] = [];
		const result = await piGatewayMain(["run", "--home", home], {
			out: () => undefined,
			overrides: {
				logger: spy.log,
				installSignals: false,
				secretReader: matrixSecrets(),
				turnRunnerFactory: buildProductionTurnRunnerFactory({
					home,
					modelRuntime: e.modelRuntime,
					model,
				}),
				platforms: [
					matrixHosting(() => {
						const a = new MatrixAdapterCore({
							hs: new FakeMatrixHomeserver(),
							secretReader: matrixSecrets(),
						});
						a.wireTransmitSend = async (_chatId, content) => {
							out.push(String(content));
							return { success: true };
						};
						adapter = a;
						return a;
					}),
				],
				onStartupOk: async (gateway) => {
					connected.push(...gateway.connectedPlatforms());
					const a =
						adapter ??
						(() => {
							throw new Error("matrix factory never ran");
						})();
					await a.deliverInbound(
						matrixEvent(HUMAN, "hello-standalone"),
						matrixKey(HUMAN),
					);
					await waitFor(() => out.length > 0);
					await gateway.lifecycle.requestShutdown("planned_stop");
				},
			},
		});

		expect(result.exitCode).toBe(0);
		expect(connected).toEqual(["matrix"]);
		expect(out.some((t) => t.includes("STANDALONE-OK"))).toBe(true);
		expect(result.output.some((l) => l.includes("platforms=[matrix]"))).toBe(
			true,
		);

		const logPath = join(home, "logs", "gateway.log");
		expect(existsSync(logPath)).toBe(true);
		expect(readFileSync(logPath, "utf8")).toContain("guard wired");
	});
});

describe("pi-gateway run — dual boot", () => {
	it("a contended runtime lock exits 0 with the lock-held message", async () => {
		const holder = new RuntimeLock(home);
		expect(holder.acquire()).toBe(true);
		try {
			const result = await piGatewayMain(["run", "--home", home], {
				out: () => undefined,
				overrides: { installSignals: false },
			});
			expect(result.exitCode).toBe(0);
			expect(result.output.some((l) => l.includes(LOCK_HELD_MESSAGE))).toBe(
				true,
			);
		} finally {
			holder.release();
		}
	});
});

describe("pi-gateway status/stop", () => {
	it("status reports not-running without starting anything", async () => {
		const lines: string[] = [];
		const result = statusCommand({
			home,
			out: (l) => lines.push(l),
		});
		expect(result.exitCode).toBe(0);
		expect(lines).toEqual(["gateway: not running"]);
		expect(existsSync(join(home, "state.db"))).toBe(false);
	});

	it("stop with no instance exits 0 as not-running", async () => {
		const result = await piGatewayMain(["stop", "--home", home], {
			out: () => undefined,
		});
		expect(result.exitCode).toBe(0);
		expect(result.output).toEqual(["gateway: not running"]);
	});

	it("unknown command prints usage with exit 2", async () => {
		const result = await piGatewayMain(["frobnicate"], {
			out: () => undefined,
		});
		expect(result.exitCode).toBe(2);
		expect(result.output[0]).toContain("usage:");
	});

	it("stop marks the stop planned then signals the recorded live PID", async () => {
		const holder = new RuntimeLock(home);
		expect(holder.acquire()).toBe(true);
		const sleeper: ChildProcess = spawn("sleep", ["300"], {
			stdio: "ignore",
		});
		try {
			const pid = sleeper.pid;
			if (pid === undefined) throw new Error("sleeper spawned without a pid");
			writeFileSync(pidFilePath(home), JSON.stringify(buildPidRecord(home, pid)));
			const result = stopCommand({ home, out: () => undefined });
			expect(result.exitCode).toBe(0);
			expect(result.output).toEqual([`gateway: stop signalled — pid=${pid}`]);
			expect(existsSync(plannedStopMarkerPath(home))).toBe(true);
			await new Promise<void>((resolve) => {
				if (sleeper.exitCode !== null || sleeper.signalCode !== null) resolve();
				else sleeper.once("exit", () => resolve());
			});
			expect(sleeper.signalCode).toBe("SIGTERM");
		} finally {
			try {
				sleeper.kill("SIGKILL");
			} catch {
				/* already exited */
			}
			holder.release();
		}
	});
});
