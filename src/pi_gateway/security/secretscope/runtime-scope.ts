// secretscope/runtime-scope — the per-profile runtime scope combinator
// (06 §3 sketch; port of gateway/run.py::_profile_runtime_scope).
//
// Installs the TWO per-turn tokens in order — home override first, secret
// scope second — and resets BOTH in reverse order on unwind, INCLUDING when
// the scoped body throws (06 §9 error-handling table row "Scope reset
// hygiene: exception inside _profile_runtime_scope body still resets BOTH
// tokens"). Applied ONLY around multiplexed inbound paths (secondary-profile
// adapter dispatch and turns) — never globally, never around unrelated tasks.
//
// KNOWN GAP (recorded): Hermes hydrates external profile secret sources
// between the home override and the scope install
// (gateway/run.py::_profile_runtime_scope → hydrate_profile_secret_sources).
// Pi Gateway's external source registry is not built yet; callers may pass
// `hydrate` explicitly until then.

import { runWithPiHomeOverride } from "../../../pi_home.js";
import type { SecretMapping } from "./scope.js";
import { setSecretScope, resetSecretScope } from "./scope.js";

export interface ProfileRuntimeScopeOptions {
	/** Hydrate external profile secret sources BEFORE the scope installs. */
	hydrate?: ((home: string) => void) | undefined;
}

export function withProfileRuntimeScope<T>(
	profileHome: string,
	secrets: SecretMapping | undefined,
	fn: () => T,
	opts: ProfileRuntimeScopeOptions = {},
): T {
	return runWithPiHomeOverride(profileHome, () => {
		opts.hydrate?.(profileHome);
		const secretToken = setSecretScope(secrets);
		try {
			return fn();
		} finally {
			resetSecretScope(secretToken); // reverse order of install
			// home token restored by runWithPiHomeOverride's unwinding
		}
	});
}
