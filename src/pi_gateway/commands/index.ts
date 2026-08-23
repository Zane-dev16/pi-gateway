// pi_gateway/commands — the ONE slash-command registry + derived consumers
// (07-integrations.md §1–§2; DEC-005). Layer rank 3 (pi_gateway): may import
// pi_state, never upward; sibling guards consume this registry's output — no
// reverse dependency in src.
//
// Review checklist (07 §9) mapping:
//   - No hand-built command lists anywhere → every consumer derives from
//     CommandRegistry rows (derived.ts, busy-resolver.ts).
//   - Busy policies cover ALL resolvable commands; queueing a recognized
//     slash command is impossible → busy-resolver.ts + slash-intake.ts
//     (unknown "/foo" is plain text and MAY queue; recognized commands never).
//   - Config-gated commands routable while hidden; gate read failure degrades
//     closed-safe → config-gates.ts.

// Schema (07 §1.1 field-for-field).
export {
	BUSY_POLICIES,
	CommandDefValidationError,
	CommandDef,
	DEFAULT_BUSY_POLICY,
	TRUTHY_STRINGS,
	aliasesOf,
	argsHintOf,
	effectiveBusyPolicy,
	isTruthyValue,
	isValidCommandToken,
	subcommandsOf,
	validateCommandDef,
	type BusyPolicy,
} from "./command-def.js";

// The frozen central registry store.
export {
	CommandRegistry,
	RegistryCollisionError,
	RegistryFrozenError,
	type RegisterOptions,
} from "./registry.js";

// Config gates (07 §1.3) — pure evaluation, injectable reader.
export {
	isGatewayAvailable,
	resolveConfigGates,
	walkConfigDotPath,
	type RawConfigReader,
} from "./config-gates.js";

// Derived consumers (07 §1.2) — zero per-surface lists.
export {
	buildCliDescription,
	cliCommandDescriptions,
	cliCommandsByCategory,
	completionCatalog,
	gatewayHelpLines,
	gatewayKnownCommands,
	isGatewayKnownCommand,
	requiresArgument,
	sanitizeTelegramName,
	subcommandsFor,
	telegramMenuModel,
	type CompletionCatalog,
	type CompletionSurface,
	type MenuCommand,
	type PluginMenuEntry,
} from "./derived.js";

// L2 busy-policy resolver feeding the guard machinery (DEC-005).
export {
	BusyResolver,
	buildBusyLookup,
	toGuardRows,
	type GuardCommandRow,
} from "./busy-resolver.js";

// Arrival classification: unknown commands queue as TEXT (03 §11, 07 §1.4).
export {
	classifySlashIntake,
	extractSlashArgs,
	extractSlashToken,
	type SlashIntake,
	type SlashToken,
} from "./slash-intake.js";

// §1.6 injection path: user-message injection + deferred cache invalidation.
export {
	DeferredInvalidationBuffer,
	NOW_FLAG,
	runSlashInjection,
	type CacheInvalidationHooks,
	type DeferredCacheInvalidation,
	type ImmediateCacheInvalidation,
	type InjectedUserMessage,
	type InvalidationDisposition,
	type SlashInjectionOptions,
	type SlashInjectionResult,
} from "./inject.js";
