// guard-wiring.test.ts — DEC-074 production guard-wiring behavior contracts.
//
// The gap: stage-9 entries constructed adapters and connect()ed them, but no
// production path ever attached the runner guard, so every ingress threw
// "<platform>: no guard attached — wire the runner first". These contracts
// run REAL chains — real hosted adapters (MatrixAdapterCore over the fake
// homeserver seam; TelegramAdapter over the fake Bot API), the real
// stage-8-built builtin registry, the real GatewayAgentRunner on the
// scripted faux model, the real authz decision, temp StateStores — hermetic
// in temp homes. No source-regex, no frozen lists.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { MatrixAdapterCore } from "../pi_platforms/matrix/matrix-adapter.js";
import { FakeMatrixHomeserver } from "../pi_platforms/matrix/matrix-fake-server.js";
import { TelegramAdapter } from "../pi_platforms/telegram/telegram-adapter.js";
import { TelegramBotApiFake } from "../pi_platforms/telegram/telegram-fake-server.js";
import { buildSessionKey } from "../pi_gateway/resolution/session-key.js";
import { createBuiltinCommandRegistry } from "../pi_gateway/commands/builtins.js";
import type { IncomingEvent } from "../pi_gateway/guards/index.js";
import type { Logger } from "../pi_gateway/lifecycle/shutdown.js";
import {
	createRunnerHarness,
	type RunnerHarness,
} from "../pi_agent_core/testing/runner-harness.js";
import { GatewayAgentRunner } from "../pi_agent_core/runner.js";
import { fauxAssistantMessage } from "../pi_agent_core/testing/faux-model.js";
import {
	buildAdapterSendReply,
	buildProductionMessageHandler,
	toGuardRegistry,
	tryAttachProductionGuard,
} from "./guard-wiring.js";
import {
	composeGatewayLifecycle,
	defaultReconnectBackend,
} from "./gateway-run.js";
import { matrixHosting, telegramHosting } from "./platform-hosting.js";
import { MATRIX_MANIFEST } from "../pi_platforms/matrix/manifest.js";
import { TELEGRAM_MANIFEST } from "../pi_platforms/telegram/manifest.js";

/** Stage-9 secret gate reads exactly the manifests' requiresEnv (no frozen list). */
function hostedSecrets(): (name: string) => string | undefined {
	const required = new Set<string>([
		...MATRIX_MANIFEST.requiresEnv.map((s) => s.name),
		...TELEGRAM_MANIFEST.requiresEnv.map((s) => s.name),
	]);
	return (name: string) => (required.has(name) ? "cred" : undefined);
}

let home: string;
let harness: RunnerHarness | null = null;
const savedEnv = new Map<string, string | undefined>();

function setEnv(vars: Record<string, string | undefined>): void {
	for (const [k, v] of Object.entries(vars)) {
		if (!savedEnv.has(k)) savedEnv.set(k, process.env[k]);
		if (v === undefined) delete process.env[k];
		else process.env[k] = v;
	}
}

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-gateway-guard-home-"));
});

afterEach(async () => {
	for (const [k, v] of savedEnv) {
		if (v === undefined) delete process.env[k];
		else process.env[k] = v;
	}
	savedEnv.clear();
	if (harness !== null) {
		await harness.close();
		harness = null;
	}
	rmSync(home, { recursive: true, force: true });
});

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

/** Poll until cond() holds or the budget expires (guard frames spawn async). */
async function waitFor(cond: () => boolean, ms = 15000): Promise<void> {
	const start = Date.now();
	for (;;) {
		if (cond()) return;
		if (Date.now() - start > ms)
			throw new Error("timed out waiting for condition");
		await new Promise((r) => setTimeout(r, 25));
	}
}

function makeMatrixAdapter(
	secretReader: (name: string) => string | undefined = hostedSecrets(),
): MatrixAdapterCore {
	return new MatrixAdapterCore({
		hs: new FakeMatrixHomeserver(),
		secretReader,
	});
}

const HUMAN = "@human:fake.example";
const OUTSIDER = "@outsider:fake.example";
const ROOM = "!room:fake.example";

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

describe("production guard attach — hosted matrix adapter (DEC-074)", () => {
	it("attached production-style guard dispatches ingress to the handler (no throw)", async () => {
		setEnv({ MATRIX_ALLOWED_USERS: HUMAN });
		harness = await createRunnerHarness({ withTurnLeases: true });
		const h = harness;
		h.faux.setResponses([fauxAssistantMessage("GUARD-WIRED-OK")]);

		const adapter = makeMatrixAdapter();
		const replies: string[] = [];
		const registry = toGuardRegistry(createBuiltinCommandRegistry().rows());
		const wired = tryAttachProductionGuard(adapter, {
			registry,
			messageHandler: buildProductionMessageHandler({
				runner: h.runner,
				store: h.store,
			}),
			sendReply: async (_chatId, text) => {
				replies.push(text);
			},
		});
		expect(wired).toBe(true);

		const key = matrixKey(HUMAN);
		await adapter.deliverInbound(matrixEvent(HUMAN, "hello-turn"), key);
		await waitFor(() => replies.length > 0);
		expect(replies).toEqual(["GUARD-WIRED-OK"]);

		// The handler ensured the durable session row, engaging the runner's
		// DB turn-lease layer (a fresh id would have skipped it).
		const row = h.store.db
			.prepare("SELECT id, source FROM sessions WHERE id = ?")
			.get(key) as { id: string; source: string } | undefined;
		expect(row?.id).toBe(key);
		expect(row?.source).toBe("gateway");
	});

	it("unattached adapter still throws the explicit no-guard error (contract preserved)", async () => {
		const adapter = makeMatrixAdapter();
		const key = matrixKey(HUMAN);
		await expect(
			adapter.deliverInbound(matrixEvent(HUMAN, "hello-turn"), key),
		).rejects.toThrow(/no guard attached — wire the runner first/);
	});

	it("outsider drops silently with a logged reason_code (no turn, no reply)", async () => {
		setEnv({ MATRIX_ALLOWED_USERS: HUMAN });
		harness = await createRunnerHarness({ withTurnLeases: true });
		const h = harness;
		h.faux.setResponses([fauxAssistantMessage("MUST-NEVER-SEND")]);

		const adapter = makeMatrixAdapter();
		const replies: string[] = [];
		const denials: Array<Record<string, unknown>> = [];
		const wired = tryAttachProductionGuard(adapter, {
			registry: toGuardRegistry(createBuiltinCommandRegistry().rows()),
			messageHandler: buildProductionMessageHandler({
				runner: h.runner,
				store: h.store,
				log: {
					info: () => {},
					error: () => {},
					warn: (message, meta) => {
						denials.push({ message, ...(meta ?? {}) });
					},
				},
			}),
			sendReply: async (_chatId, text) => {
				replies.push(text);
			},
		});
		expect(wired).toBe(true);

		await adapter.deliverInbound(
			matrixEvent(OUTSIDER, "let me in"),
			matrixKey(OUTSIDER),
		);
		await waitFor(() => denials.length > 0);
		// Silent in-chat (Hermes _handle_message returns None): settle window,
		// then prove no turn ran and nothing was sent.
		await new Promise((r) => setTimeout(r, 500));
		expect(replies).toEqual([]);
		expect(h.faux.state.callCount).toBe(0);
		const denial = denials[0] as { reason_code?: unknown; gate?: unknown };
		expect(typeof denial.reason_code).toBe("string");
		expect(denial.gate).not.toBeNull();
	});
});

describe("structural wiring skips (DEC-074 loud-unwired contract)", () => {
	it("non-kit surfaces expose no guard slot and no text egress", () => {
		const plain = {
			async connect() {
				return true;
			},
		};
		expect(
			tryAttachProductionGuard(plain, {
				registry: toGuardRegistry(createBuiltinCommandRegistry().rows()),
				messageHandler: async () => null,
				sendReply: async () => {},
			}),
		).toBe(false);
		expect(buildAdapterSendReply(plain)).toBeNull();
		expect(buildAdapterSendReply(null)).toBeNull();
	});
});

describe("stage-9 wires every hosted adapter generically (DEC-074)", () => {
	it("matrix + telegram share the one wiring path: ingress dispatches, replies land on the wires", async () => {
		setEnv({ MATRIX_ALLOWED_USERS: HUMAN, TELEGRAM_ALLOWED_USERS: "111" });
		harness = await createRunnerHarness({ withTurnLeases: true });
		const h = harness;
		h.faux.setResponses([
			fauxAssistantMessage("STAGE9-WIRED-OK"),
			fauxAssistantMessage("STAGE9-WIRED-OK"),
		]);

		let matrixAdapter: MatrixAdapterCore | null = null;
		let telegramAdapter: TelegramAdapter | null = null;
		const matrixOut: string[] = [];
		const telegramOut: string[] = [];
		// The factory MUST adapt the lifecycle-provided store (production
		// recipe) — a runner bound to any other store breaks the durable
		// session/lease chain with FK violations.
		let factoryStore: unknown = null;
		const spy = spyLogger();
		const composed = composeGatewayLifecycle({
			home,
			logger: spy.log,
			installSignals: false,
			secretReader: hostedSecrets(),
			turnRunnerFactory: ({ store }) => {
				factoryStore = store;
				if (store === null) throw new Error("stage-9 provided no store");
				const model = h.env.faux.getModel();
				if (!model) throw new Error("faux provider exposed no model");
				return new GatewayAgentRunner({
					store: {
						db: store.db,
						appendMessage: (m) => store.appendMessage(m),
						queueTokenCounts: (sessionId, delta) =>
							store.queueTokenCounts(sessionId, delta),
						touchSessionActivity: (sessionId, opts) =>
							store.touchSessionActivity(sessionId, opts),
						leases: store.leases,
					},
					systemPrompt: h.env.systemPrompt,
					model,
					modelRuntime: h.env.modelRuntime,
				});
			},
			platforms: [
				matrixHosting(() => {
					const a = makeMatrixAdapter();
					a.wireTransmitSend = async (_chatId, content) => {
						matrixOut.push(String(content));
						return { success: true };
					};
					matrixAdapter = a;
					return a;
				}),
				telegramHosting(() => {
					const t = new TelegramAdapter({
						wire: new TelegramBotApiFake(),
						secretReader: hostedSecrets(),
					});
					t.wireTransmitSend = async (_chatId, content) => {
						telegramOut.push(String(content));
						return { success: true };
					};
					telegramAdapter = t;
					return t;
				}),
			],
		});
		const result = await composed.lifecycle.startup();
		try {
			expect(result.ok).toBe(true);
			expect([...composed.connectedPlatforms()].sort()).toEqual([
				"matrix",
				"telegram",
			]);
			// The generic path wired both platforms (not a matrix special-case).
			const wiredLines = spy.calls.filter((c) =>
				c.message.includes("guard wired"),
			);
			expect(wiredLines.map((c) => c.meta?.["platform"]).sort()).toEqual([
				"matrix",
				"telegram",
			]);

			// Matrix ingress → production handler → faux-model text → matrix wire.
			const m: MatrixAdapterCore =
				matrixAdapter ??
				(() => {
					throw new Error("matrix factory never ran");
				})();
			const mkey = matrixKey(HUMAN);
			await m.deliverInbound(matrixEvent(HUMAN, "stage-nine"), mkey);
			await waitFor(() => matrixOut.length > 0);

			// Telegram ingress (harness-injected event, no raw registry entry →
			// the standard guard lane) → same handler → telegram wire.
			const t: TelegramAdapter =
				telegramAdapter ??
				(() => {
					throw new Error("telegram factory never ran");
				})();
			const tkey = "telegram:dm:111";
			await t.handleIngress(
				{
					messageType: "text",
					text: "stage-nine-tg",
					source: {
						platform: "telegram",
						chatType: "dm",
						userId: "111",
						chatId: "111",
					},
					metadata: { gateway_session_key: tkey },
				},
				tkey,
			);
			await waitFor(() => telegramOut.length > 0);
		} finally {
			await composed.lifecycle.requestShutdown("planned_stop");
			await composed.lifecycle.waitShutdown();
		}
		expect(factoryStore).not.toBeNull();
		expect(matrixOut.some((t) => t.includes("STAGE9"))).toBe(true);
		expect(telegramOut.some((t) => t.includes("STAGE9"))).toBe(true);
	});
});

describe("default reconnect backend re-wires before connect (DEC-074)", () => {
	it("rewire runs before connect; unknown platforms stay false", async () => {
		const order: string[] = [];
		const rewired: Array<{ platform: string }> = [];
		const doubles: Array<{ connects: number; guardWired: boolean }> = [];
		const hosting = matrixHosting(() => {
			const rec = { connects: 0, guardWired: false };
			doubles.push(rec);
			return {
				attachGuard: () => {
					rec.guardWired = true;
				},
				deliverText: async () => [{ success: true }],
				connect: async () => {
					order.push("connect");
					rec.connects += 1;
					return true;
				},
				disconnect: async () => {},
			};
		});
		const backend = defaultReconnectBackend(
			{ platforms: [hosting] },
			(platform, adapter) => {
				order.push("rewire");
				rewired.push({ platform });
				return tryAttachProductionGuard(adapter, {
					registry: toGuardRegistry(createBuiltinCommandRegistry().rows()),
					messageHandler: async () => null,
					sendReply: buildAdapterSendReply(adapter) ?? (async () => {}),
				});
			},
		);
		const ok = await backend.reconnect("matrix", {
			platform: "matrix",
			attempts: 1,
			queuedAt: 0,
			nextRetryAt: 0,
		});
		expect(ok).toBe(true);
		expect(order).toEqual(["rewire", "connect"]);
		expect(rewired).toEqual([{ platform: "matrix" }]);
		expect(doubles[0]?.guardWired).toBe(true);
		expect(
			await backend.reconnect("unknown-platform", {
				platform: "unknown-platform",
				attempts: 1,
				queuedAt: 0,
				nextRetryAt: 0,
			}),
		).toBe(false);
	});
});
