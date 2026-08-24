// secretscope — the fail-closed secret scope engine (06 §3; DEC-003/DEC-009).
//
// This barrel is the SANCTIONED import surface: every consumer (adapters,
// runner, kit wiring) imports from here so the canonical wrapper stays the
// single copy of the sanctioned except-shape and the grep gate can police
// copies (06 §10 "wrapper copy fidelity" row).

export {
	UnscopedSecretError,
	currentSecretScope,
	isMultiplexActive,
	resetSecretScope,
	runInSecretScope,
	secretMappingFromRecord,
	setMultiplexActive,
	setSecretScope,
	type SecretMapping,
	type SecretScopeToken,
} from "./scope.js";
export {
	GLOBAL_ENV_EXACT,
	GLOBAL_ENV_PREFIXES,
	isGlobalEnv,
} from "./global-env.js";
export { getSecret } from "./resolve.js";
export {
	buildProfileSecretScope,
	loadEnvFile,
	parseEnvValue,
	stripInlineComment,
} from "./env-file.js";
export {
	getScopedSecret,
	kitScopedSecretReader,
	type KitScopedSecretReader,
} from "./wrapper.js";
export { withProfileRuntimeScope } from "./runtime-scope.js";
