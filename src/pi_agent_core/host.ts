// pi_agent_core/host.ts — THE single import seam onto the HOST pi coding-agent
// SDK (DEC-023 walk-away rule: drive the real host agent loop directly, never
// re-implement or shim it).
//
// The gateway repo deliberately does not add @earendil-works/pi-coding-agent to
// package.json dependencies (orchestrator-owned file): the host runtime is the
// globally installed package this process runs under, imported here by its
// installed absolute path. Every other module in pi_agent_core imports the SDK
// surface ONLY through this module, so relocating/relinking the SDK touches
// exactly one line pair.
//
// Real seams used (docs/sdk.md, docs/custom-provider.md):
//   createAgentSession() / AgentSession.prompt()/steer()/abort()/subscribe()
//   SessionManager.inMemory / SettingsManager.inMemory / DefaultResourceLoader
//     (systemPromptOverride — stable system-prompt bytes per session)
//   ModelRuntime (+ registerNativeProvider) — provider injection point; tests
//     inject the pi-ai FAUX provider (scripted MODEL injection, sanctioned by
//     the phase brief; it is a real provider implementation behind the real
//     loop, not a loop shim).
//
// Layering: absolute specifiers resolve OUTSIDE src/, so scripts/
// check-layering.mjs ignores them (pi_agent_core keeps rank-2 deps:
// pi_home + pi_state + node builtins + the external host runtime).

export {
	createAgentSession,
	DefaultResourceLoader,
	ModelRuntime,
	SessionManager,
	SettingsManager,
	defineTool,
} from "/usr/local/lib/node_modules/@earendil-works/pi-coding-agent/dist/index.js";

// TypeBox lives under pi-ai (coding-agent re-exports only its types, not the
// value) — custom tool schemas import `Type` from here.
import { Type as _Type } from "/usr/local/lib/node_modules/@earendil-works/pi-coding-agent/node_modules/@earendil-works/pi-ai/dist/index.js";
export const Type = _Type;
export type { Static } from "/usr/local/lib/node_modules/@earendil-works/pi-coding-agent/node_modules/@earendil-works/pi-ai/dist/index.js";

export type {
	AgentSession,
	AgentSessionEvent,
	CreateAgentSessionOptions,
	CreateAgentSessionResult,
	InputSource,
	PromptOptions,
	SettingsManager as SettingsManagerType,
	ToolDefinition,
} from "/usr/local/lib/node_modules/@earendil-works/pi-coding-agent/dist/index.js";

// Type-level surface of the wire protocol the loop drives (pi-ai).
export type {
	Api,
	AssistantMessage,
	Context,
	Message,
	Model,
	StopReason,
	TextContent,
	ThinkingContent,
	ToolCall,
	ToolResultMessage,
	UserMessage,
	Usage,
} from "/usr/local/lib/node_modules/@earendil-works/pi-coding-agent/node_modules/@earendil-works/pi-ai/dist/index.js";
