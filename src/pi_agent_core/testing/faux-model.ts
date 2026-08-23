// pi_agent_core/testing/faux-model.ts — scripted MODEL injection for behavior
// tests (sanctioned by the phase brief: "scripted/fake MODEL injection is
// allowed — it is not shimming the loop").
//
// Built on the pi-ai FAUX provider (a real Provider implementation that the
// real ModelRuntime dispatches to through the real host loop): every request
// the host loop makes lands in `FauxResponseFactory(context, …)`, which gives
// tests a byte-level observation point on systemPrompt/messages/tools — the
// exact wire context — without intercepting anything.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import {
	DefaultResourceLoader,
	ModelRuntime,
	SettingsManager,
} from "../host.js";

// pi-ai faux provider — test-only seam shipped inside the host runtime.
import {
	createFauxCore,
	fauxAssistantMessage,
	fauxText,
	fauxThinking,
	fauxToolCall,
	fauxProvider,
} from "/usr/local/lib/node_modules/@earendil-works/pi-coding-agent/node_modules/@earendil-works/pi-ai/dist/index.js";
import type {
	FauxProviderHandle,
	FauxResponseFactory,
} from "/usr/local/lib/node_modules/@earendil-works/pi-coding-agent/node_modules/@earendil-works/pi-ai/dist/index.js";

export {
	createFauxCore,
	fauxAssistantMessage,
	fauxText,
	fauxThinking,
	fauxToolCall,
	fauxProvider,
};
export type { FauxProviderHandle, FauxResponseFactory };

export interface ScriptedModelEnv {
	/** Isolated temp home (mkdtemp) used as cwd AND agentDir. */
	home: string;
	faux: FauxProviderHandle;
	modelRuntime: ModelRuntime;
	resourceLoader: DefaultResourceLoader;
	settingsManager: ReturnType<typeof SettingsManager.inMemory>;
	/** The stable system prompt bytes configured via systemPromptOverride. */
	systemPrompt: string;
}

export interface ScriptedModelEnvOptions {
	systemPrompt?: string;
}

/**
 * Build one isolated scripted-model environment. Every piece that could reach
 * the real filesystem/network is pinned to the temp home:
 *   - SettingsManager.inMemory → no settings.json reads, no auto-compaction,
 *     no auto-retry (turn semantics stay runner-owned).
 *   - ModelRuntime.create({ authPath, modelsPath: null }) → no real auth or
 *     model catalog; allowModelNetwork defaults false.
 *   - fauxProvider() registered natively; auth resolve() always configured.
 */
export async function createScriptedModelEnv(
	options: ScriptedModelEnvOptions = {},
): Promise<ScriptedModelEnv> {
	const home = mkdtempSync(join(tmpdir(), "pi-gw-agent-core-"));
	const systemPrompt =
		options.systemPrompt ?? "You are a scripted gateway agent.";
	const settingsManager = SettingsManager.inMemory({
		compaction: { enabled: false },
		retry: { enabled: false },
	});
	const resourceLoader = new DefaultResourceLoader({
		cwd: home,
		agentDir: home,
		systemPromptOverride: () => systemPrompt,
		// Deterministic prompt bytes (see runner.buildHostSession).
		agentsFilesOverride: () => ({ agentsFiles: [] }),
		settingsManager,
	});
	await resourceLoader.reload();
	const modelRuntime = await ModelRuntime.create({
		authPath: join(home, "auth.json"),
		modelsPath: null,
	});
	const faux = fauxProvider();
	modelRuntime.registerNativeProvider(faux.provider);
	return {
		home,
		faux,
		modelRuntime,
		resourceLoader,
		settingsManager,
		systemPrompt,
	};
}
