// entrypoints/production-runner.ts — production turnRunnerFactory (DEC-075).
//
// Closes the DEC-074 seam: the extension passes
// buildProductionTurnRunnerFactory({home}) into composeGatewayLifecycle, so
// stage-9 connect attaches a LIVE guard and ingress dispatches to the host
// agent loop instead of dying at handleIngress ("no guard attached").
//
// Host-loop reuse (DEC-023): the runner IS GatewayAgentRunner driven by the
// REAL host ModelRuntime — the same construction the subject harnesses use
// (pi_agent_core/testing/runner-harness.ts:createRunnerHarness), adapted for
// the production lifecycle. Nothing here re-implements the loop.
//
// Laziness: ModelRuntime.create + model resolution + runner construction all
// happen on the FIRST factory call, when the stage-6 store exists (the
// factory receives it), and memoize — Hermes one-runner parity. A factory
// throw (no model/auth, no store) flows through stage-9's loud
// guard_unwired degrade; boot never bricks on model config.

import { existsSync } from "node:fs";
import { join } from "node:path";
import {
	GatewayAgentRunner,
	type RunnerStore,
} from "../pi_agent_core/runner.js";
import { ModelRuntime, resolveCliModel } from "../pi_agent_core/host.js";
import type { Api, Model } from "../pi_agent_core/host.js";
import type { StateStore } from "../pi_state/index.js";
import type { TurnRunnerFactory } from "./guard-wiring.js";

/** Explicit model pattern override (resolveCliModel `provider/model` shape). */
export const PI_GATEWAY_MODEL_ENV = "PI_GATEWAY_MODEL";
/** Explicit provider override paired with PI_GATEWAY_MODEL. */
export const PI_GATEWAY_PROVIDER_ENV = "PI_GATEWAY_PROVIDER";

/**
 * Agent-dir env the host resolves through (host config.ts:ENV_AGENT_DIR =
 * `${APP_NAME}_CODING_AGENT_DIR`, APP_NAME "pi"). Unset ⇒ <home>/agent.
 */
const AGENT_DIR_ENV = "PI_CODING_AGENT_DIR";

/** Frozen production system prompt (no operator override in this commit). */
export const GATEWAY_SYSTEM_PROMPT =
	"You are the pi messaging-gateway agent. Answer helpfully and concisely; " +
	"replies travel over chat transports with length limits.";

export interface ProductionRunnerFactoryOptions {
	/** Profile home (the same home composition receives). */
	home: string;
	systemPrompt?: string;
	/** Injectable runtime (tests pass the faux-backed ModelRuntime). */
	modelRuntime?: ModelRuntime;
	/** Injectable resolved model (tests pass the faux model). */
	model?: Model<Api>;
}

function readEnv(name: string): string | undefined {
	const raw = (process.env[name] ?? "").trim();
	return raw !== "" ? raw : undefined;
}

function profileAgentDir(home: string): string {
	const override = readEnv(AGENT_DIR_ENV);
	if (override !== undefined) return override;
	return join(home, "agent");
}

async function createProfileRuntime(home: string): Promise<ModelRuntime> {
	const agentDir = profileAgentDir(home);
	const authPath = join(agentDir, "auth.json");
	const modelsFile = join(agentDir, "models.json");
	return ModelRuntime.create({
		authPath,
		...(existsSync(modelsFile)
			? { modelsPath: modelsFile }
			: { modelsPath: null }),
	});
}

/**
 * Host-faithful model pick: explicit PI_GATEWAY_PROVIDER/PI_GATEWAY_MODEL
 * via resolveCliModel, else the first available model with valid auth
 * (findInitialModel step-4 parity). Anything else is a LOUD throw the
 * stage-9 guard_unwired degrade carries.
 */
function resolveProfileModel(modelRuntime: ModelRuntime): Model<Api> {
	const pattern = readEnv(PI_GATEWAY_MODEL_ENV);
	if (pattern !== undefined) {
		const provider = readEnv(PI_GATEWAY_PROVIDER_ENV);
		const resolved = resolveCliModel({
			...(provider !== undefined ? { cliProvider: provider } : {}),
			cliModel: pattern,
			modelRuntime,
		});
		if (resolved.model !== undefined) return resolved.model;
		throw new Error(
			`gateway turn runner: no model matches ${JSON.stringify(pattern)}` +
				(resolved.error !== undefined ? ` — ${resolved.error}` : ""),
		);
	}
	const first = modelRuntime.getAvailableSnapshot()[0];
	if (first === undefined) {
		throw new Error(
			"gateway turn runner: no model with valid auth — configure profile " +
				"auth.json or set PI_GATEWAY_MODEL",
		);
	}
	return first;
}

/**
 * Build the production TurnRunnerFactory closing over the host agent loop.
 * The RunnerStore adapts the stage-6 store with the harness recipe (db +
 * appendMessage + queueTokenCounts + touchSessionActivity + leases) so the
 * durable session/lease chain stays on the lifecycle-owned database.
 */
export function buildProductionTurnRunnerFactory(
	options: ProductionRunnerFactoryOptions,
): TurnRunnerFactory {
	const systemPrompt = options.systemPrompt ?? GATEWAY_SYSTEM_PROMPT;
	let built: Promise<GatewayAgentRunner> | null = null;
	return ({ store }: { store: StateStore | null }) => {
		if (store === null) {
			throw new Error("gateway turn runner: stage-9 provided no store");
		}
		if (built === null) {
			const stageStore: StateStore = store;
			built = (async () => {
				const modelRuntime =
					options.modelRuntime ?? (await createProfileRuntime(options.home));
				const model = options.model ?? resolveProfileModel(modelRuntime);
				const runnerStore: RunnerStore = {
					db: stageStore.db,
					appendMessage: (m) => stageStore.appendMessage(m),
					queueTokenCounts: (sessionId, delta) =>
						stageStore.queueTokenCounts(sessionId, delta),
					touchSessionActivity: (sessionId, opts) =>
						stageStore.touchSessionActivity(sessionId, opts),
					leases: stageStore.leases,
				};
				return new GatewayAgentRunner({
					store: runnerStore,
					systemPrompt,
					model,
					modelRuntime,
				});
			})();
		}
		return built;
	};
}
