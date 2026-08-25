// pi_agent_core/testing/runner-harness.ts — composition harness for runner
// behavior tests: an isolated StateStore (mkdtemp) + a scripted-model env
// (faux provider through the REAL ModelRuntime) + a GatewayAgentRunner wired
// to both. Tests compose the agent core with pi_state directly (downward
// layering); pi_gateway composition lands in that layer's own suites.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { StateStore } from "../../pi_state/index.js";
import type {
	GatewayAgentRunnerOptions,
	MemoryTurnHooks,
	RunnerStore,
	RunnerTurnLeaseRegistry,
} from "../runner.js";
import { GatewayAgentRunner } from "../runner.js";
import {
	createScriptedModelEnv,
	fauxProvider,
	type FauxProviderHandle,
	type ScriptedModelEnv,
} from "./faux-model.js";

export interface RunnerHarness {
	dir: string;
	store: StateStore;
	env: ScriptedModelEnv;
	faux: FauxProviderHandle;
	runner: GatewayAgentRunner;
	/**
	 * Mint the canonical session row. STAND-IN for the Phase-1 §4/§9 resolution
	 * chain (routing key → adopt-before-mint), which lives in pi_gateway; the
	 * runner itself never mints sessions.
	 */
	ensureSession(sessionId: string): void;
	close(): Promise<void>;
}

export interface RunnerHarnessOptions {
	systemPrompt?: string;
	maxIterations?: number;
	customTools?: GatewayAgentRunnerOptions["customTools"];
	memoryHooks?: MemoryTurnHooks;
	cacheOptions?: GatewayAgentRunnerOptions["cacheOptions"];
	poolMaxWorkers?: number;
	/** Wire the durable turn-lease layer (StateStore.leases) into the runner. */
	withTurnLeases?: boolean;
	/** L1 in-process registry passed straight through to the runner. */
	turnLeaseRegistry?: RunnerTurnLeaseRegistry;
	leaseTtlSeconds?: number;
	leaseWaitSeconds?: number;
	leasePollIntervalSeconds?: number;
	leaseRefreshIntervalMs?: number;
	cacheSweepIntervalMs?: number;
	startInterval?: GatewayAgentRunnerOptions["startInterval"];
}

export async function createRunnerHarness(
	options: RunnerHarnessOptions = {},
): Promise<RunnerHarness> {
	const dir = mkdtempSync(join(tmpdir(), "pi-gw-runner-"));
	const store = await StateStore.open(join(dir, "state.db"));
	const env = await createScriptedModelEnv({
		...(options.systemPrompt !== undefined
			? { systemPrompt: options.systemPrompt }
			: {}),
	});
	const model = env.faux.getModel();
	if (!model) throw new Error("faux provider exposed no model");
	const storeAdapter: RunnerStore = {
		db: store.db,
		appendMessage: (m) => store.appendMessage(m),
		queueTokenCounts: (sessionId, delta) =>
			store.queueTokenCounts(sessionId, delta),
		...(options.withTurnLeases === true ? { leases: store.leases } : {}),
	};
	const runner = new GatewayAgentRunner({
		store: storeAdapter,
		systemPrompt: env.systemPrompt,
		model,
		modelRuntime: env.modelRuntime,
		...(options.maxIterations !== undefined
			? { maxIterations: options.maxIterations }
			: {}),
		...(options.customTools ? { customTools: options.customTools } : {}),
		...(options.memoryHooks ? { memoryHooks: options.memoryHooks } : {}),
		...(options.cacheOptions ? { cacheOptions: options.cacheOptions } : {}),
		...(options.poolMaxWorkers !== undefined
			? { poolMaxWorkers: options.poolMaxWorkers }
			: {}),
		...(options.turnLeaseRegistry !== undefined
			? { turnLeaseRegistry: options.turnLeaseRegistry }
			: {}),
		...(options.leaseTtlSeconds !== undefined
			? { leaseTtlSeconds: options.leaseTtlSeconds }
			: {}),
		...(options.leaseWaitSeconds !== undefined
			? { leaseWaitSeconds: options.leaseWaitSeconds }
			: {}),
		...(options.leasePollIntervalSeconds !== undefined
			? { leasePollIntervalSeconds: options.leasePollIntervalSeconds }
			: {}),
		...(options.leaseRefreshIntervalMs !== undefined
			? { leaseRefreshIntervalMs: options.leaseRefreshIntervalMs }
			: {}),
		...(options.cacheSweepIntervalMs !== undefined
			? { cacheSweepIntervalMs: options.cacheSweepIntervalMs }
			: {}),
		...(options.startInterval !== undefined
			? { startInterval: options.startInterval }
			: {}),
	});
	return {
		dir,
		store,
		env,
		faux: env.faux,
		runner,
		ensureSession(sessionId: string): void {
			void store.withWrite((db) => {
				db.prepare(
					"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'gateway', ?)",
				).run(sessionId, Math.floor(Date.now() / 1000));
			});
		},
		async close() {
			await runner.close();
			await store.close();
		},
	};
}

// Re-export for tests that build custom tooling around the faux provider.
export { fauxProvider };
