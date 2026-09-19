// production-runner.test.ts — DEC-075 extension runner-factory behavior contracts.
//
// The gap this closes: commit 285fbcd landed the stage-9 factory seam, but
// the only production caller (extensions/pi-gateway.ts) passed NO factory,
// so boot logged guard_unwired and every ingress died at handleIngress.
// These contracts run REAL chains — REAL composition, the REAL production
// factory builder over an injected faux-backed host runtime, the REAL
// stage-8 registry, the REAL authz decision, temp homes — hermetic,
// no network, no source-regex.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MatrixAdapterCore } from "../pi_platforms/matrix/matrix-adapter.js";
import { FakeMatrixHomeserver } from "../pi_platforms/matrix/matrix-fake-server.js";
import { MATRIX_MANIFEST } from "../pi_platforms/matrix/manifest.js";
import { buildSessionKey } from "../pi_gateway/resolution/session-key.js";
import type { IncomingEvent } from "../pi_gateway/guards/index.js";
import type { Logger } from "../pi_gateway/lifecycle/shutdown.js";
import {
	createScriptedModelEnv,
	fauxAssistantMessage,
	type ScriptedModelEnv,
} from "../pi_agent_core/testing/faux-model.js";
import { ModelRuntime } from "../pi_agent_core/host.js";
import { composeGatewayLifecycle } from "./gateway-run.js";
import { matrixHosting } from "./platform-hosting.js";
import { buildProductionTurnRunnerFactory } from "./production-runner.js";

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
	calls: Array<{
		level: "info" | "warn" | "error";
		message: string;
		meta?: Record<string, unknown> | undefined;
	}>;
} {
	const calls: Array<{
		level: "info" | "warn" | "error";
		message: string;
		meta?: Record<string, unknown> | undefined;
	}> = [];
	return {
		calls,
		log: {
			info: (m: string, meta?: Record<string, unknown>) => {
				calls.push({ level: "info", message: m, meta });
			},
			warn: (m: string, meta?: Record<string, unknown>) => {
				calls.push({ level: "warn", message: m, meta });
			},
			error: (m: string, meta?: Record<string, unknown>) => {
				calls.push({ level: "error", message: m, meta });
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
	home = mkdtempSync(join(tmpdir(), "pi-gateway-prod-runner-"));
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

describe("production factory through REAL composition (DEC-075)", () => {
	it("factory attaches a live guard: ingress dispatches, no guard_unwired", async () => {
		setEnv({
			MATRIX_ALLOWED_USERS: HUMAN,
			PI_GATEWAY_MODEL: undefined,
			PI_GATEWAY_PROVIDER: undefined,
		});
		env = await createScriptedModelEnv();
		const e = env;
		e.faux.setResponses([fauxAssistantMessage("PROD-FACTORY-OK")]);
		const model = e.faux.getModel();
		if (!model) throw new Error("faux provider exposed no model");

		let adapter: MatrixAdapterCore | null = null;
		const out: string[] = [];
		const spy = spyLogger();
		const composed = composeGatewayLifecycle({
			home,
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
		});
		const result = await composed.lifecycle.startup();
		try {
			expect(result.ok).toBe(true);
			expect([...composed.connectedPlatforms()]).toEqual(["matrix"]);
			// Live guard wired; the loud unwired fallback stayed silent.
			expect(
				spy.calls.filter((c) => c.message.includes("guard wired")),
			).toHaveLength(1);
			expect(
				spy.calls.filter((c) => c.meta?.["reason_code"] === "guard_unwired"),
			).toEqual([]);

			const a: MatrixAdapterCore =
				adapter ??
				(() => {
					throw new Error("matrix factory never ran");
				})();
			const key = matrixKey(HUMAN);
			await a.deliverInbound(matrixEvent(HUMAN, "hello-production"), key);
			await waitFor(() => out.length > 0);
			expect(out.some((t) => t.includes("PROD-FACTORY-OK"))).toBe(true);
		} finally {
			await composed.lifecycle.requestShutdown("planned_stop");
			await composed.lifecycle.waitShutdown();
		}
	});

	it("composition WITHOUT the factory keeps the loud guard_unwired fallback", async () => {
		setEnv({ MATRIX_ALLOWED_USERS: HUMAN });
		const spy = spyLogger();
		let adapter: MatrixAdapterCore | null = null;
		const composed = composeGatewayLifecycle({
			home,
			logger: spy.log,
			installSignals: false,
			secretReader: matrixSecrets(),
			platforms: [
				matrixHosting(() => {
					const a = new MatrixAdapterCore({
						hs: new FakeMatrixHomeserver(),
						secretReader: matrixSecrets(),
					});
					adapter = a;
					return a;
				}),
			],
		});
		const result = await composed.lifecycle.startup();
		try {
			expect(result.ok).toBe(true);
			// Connect proceeds, but loudly unwired.
			expect([...composed.connectedPlatforms()]).toEqual(["matrix"]);
			const unwired = spy.calls.filter(
				(c) => c.meta?.["reason_code"] === "guard_unwired",
			);
			expect(unwired.length).toBeGreaterThan(0);

			const a: MatrixAdapterCore =
				adapter ??
				(() => {
					throw new Error("matrix factory never ran");
				})();
			await expect(
				a.deliverInbound(matrixEvent(HUMAN, "hello"), matrixKey(HUMAN)),
			).rejects.toThrow(/no guard attached — wire the runner first/);
		} finally {
			await composed.lifecycle.requestShutdown("planned_stop");
			await composed.lifecycle.waitShutdown();
		}
	});

	it("factory with no resolvable model degrades to the loud unwired path (never bricks boot)", async () => {
		setEnv({
			MATRIX_ALLOWED_USERS: HUMAN,
			PI_GATEWAY_MODEL: undefined,
			PI_GATEWAY_PROVIDER: undefined,
		});
		const agentDir = mkdtempSync(join(tmpdir(), "pi-gw-no-model-"));
		try {
			const emptyRuntime = await ModelRuntime.create({
				authPath: join(agentDir, "auth.json"),
				modelsPath: null,
			});
			expect(emptyRuntime.getAvailableSnapshot()).toEqual([]);
			const spy = spyLogger();
			let adapter: MatrixAdapterCore | null = null;
			const composed = composeGatewayLifecycle({
				home,
				logger: spy.log,
				installSignals: false,
				secretReader: matrixSecrets(),
				turnRunnerFactory: buildProductionTurnRunnerFactory({
					home,
					modelRuntime: emptyRuntime,
				}),
				platforms: [
					matrixHosting(() => {
						const a = new MatrixAdapterCore({
							hs: new FakeMatrixHomeserver(),
							secretReader: matrixSecrets(),
						});
						adapter = a;
						return a;
					}),
				],
			});
			const result = await composed.lifecycle.startup();
			try {
				expect(result.ok).toBe(true);
				expect([...composed.connectedPlatforms()]).toEqual(["matrix"]);
				const unwired = spy.calls.filter(
					(c) => c.meta?.["reason_code"] === "guard_unwired",
				);
				expect(unwired.length).toBeGreaterThan(0);
				const a: MatrixAdapterCore =
					adapter ??
					(() => {
						throw new Error("matrix factory never ran");
					})();
				await expect(
					a.deliverInbound(matrixEvent(HUMAN, "hello"), matrixKey(HUMAN)),
				).rejects.toThrow(/no guard attached — wire the runner first/);
			} finally {
				await composed.lifecycle.requestShutdown("planned_stop");
				await composed.lifecycle.waitShutdown();
			}
		} finally {
			rmSync(agentDir, { recursive: true, force: true });
		}
	});
});
