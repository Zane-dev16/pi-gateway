// secretscope/resolve — the fail-closed resolution table (06 §3, verified
// resolution order of agent/secret_scope.py::get_secret).
//
//   name ∈ global-env carve-out?  → process env always (deployment settings,
//                                   NOT profile secrets; see global-env.ts)
//   scope installed?
//       hit                       → scope value
//       miss + multiplex ACTIVE   → declared default    ← FAIL CLOSED (DEC-003)
//       miss + multiplex OFF      → process env (scope is a .env OVERLAY here,
//                                   not a boundary — cron installs one per job;
//                                   blindfolding it broke cron creds)
//   no scope + multiplex ACTIVE   → RAISE UnscopedSecretError
//   no scope + multiplex OFF      → process env (legacy single-profile path)
//
// Presence semantics parity: a scope entry mapped to "" is a HIT (returns ""),
// mirroring Python's `val is not None` checks in agent/secret_scope.py:get_secret.

import {
	currentSecretScope,
	isMultiplexActive,
	UnscopedSecretError,
} from "./scope.js";
import { isGlobalEnv } from "./global-env.js";

export function getSecret(
	name: string,
	defaultValue?: string,
): string | undefined {
	// 1. Genuinely-global vars always read process env regardless of scope.
	if (isGlobalEnv(name)) {
		return process.env[name] ?? defaultValue;
	}

	const scope = currentSecretScope();
	if (scope !== undefined) {
		const hit = scope.get(name);
		if (hit !== undefined) return hit;
		if (isMultiplexActive()) {
			// FAIL CLOSED: the scope is authoritative under multiplex. A scoped
			// miss returns the declared default and MUST NOT fall through to
			// process env — under multiplex that borrows another profile's
			// credential or allowlist (#86905/#72348 class; DEC-003/DEC-009).
			return defaultValue;
		}
		// Multiplex off: the scope is an overlay over the process environment,
		// not an isolation boundary — there is no other profile to leak from.
		// Without this fallthrough credentials injected only into the process
		// environment vanish inside any setSecretScope block (the cron
		// scheduler installs one around every job).
		return process.env[name] ?? defaultValue;
	}

	if (isMultiplexActive()) {
		throw new UnscopedSecretError(name);
	}
	return process.env[name] ?? defaultValue;
}
