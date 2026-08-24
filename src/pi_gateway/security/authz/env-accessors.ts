// authz/env-accessors — the two scoped authz env accessors (06 §2 header).
//
// Hermes parity anchors (READ-ONLY reference):
//   gateway/authz_mixin.py:_auth_env          → authEnv
//   gateway/authz_mixin.py:_platform_gate_env → platformGateEnv
//
// DELIBERATE COLLAPSE (proposed DEC text in the phase report): in Hermes the
// two accessors differ ONLY in what happens after a scoped read misses —
// _platform_gate_env returns the declared default (#72348 fix) while
// _auth_env falls through to os.getenv. That fallthrough is precisely the
// AFTER-A-SCOPED-MISS fallback shape DEC-003 bans — "applies to authz reads
// with zero exceptions" — and 06 §2.2 names the leaked default-profile
// allow-all skipping step 5 as the sharpest edge ("which is why §3 has no
// exceptions for authz reads"). Pi Gateway therefore routes BOTH accessors
// through the canonical wrapper (secretscope/wrapper.getScopedSecret), whose
// sanctioned catch serves process env ONLY on the unscoped default-profile
// path. Under multiplex a scoped miss NEVER borrows process env, on either
// accessor; single-profile deployments behave exactly like the legacy
// os.getenv reads.
//
// GREP-GATE NOTE: this file intentionally contains no raw process-environment
// — every env observation funnels through the secretscope engine, which owns
// the carve-out and overlay branches (scripts/check-secret-scope.mjs
// RAW_ENV_BESIDE_SCOPE).

import { getScopedSecret } from "../secretscope/index.js";

function readScoped(name: string, defaultValue: string): string {
	if (!name) return defaultValue;
	const value = getScopedSecret(name);
	const trimmed =
		value === undefined || value === null ? "" : String(value).trim();
	// Presence semantics: an empty-string scoped hit carries NO authorization
	// signal (parity of Hermes' `str(val).strip()` truthiness check) — the
	// declared default applies, never another source's value.
	return trimmed !== "" ? trimmed : defaultValue;
}

/**
 * Read an allowlist/auth env var preferring the profile secret scope under
 * multiplex. Fail-closed per DEC-003/DEC-009: after a scoped miss the
 * declared default is returned — process env is consulted only by the
 * canonical wrapper's sanctioned unscoped-default-profile path.
 */
export function authEnv(name: string, defaultValue = ""): string {
	return readScoped(name, defaultValue);
}

/**
 * Read a platform allow/deny gate env var with per-profile isolation.
 * Authoritative under multiplex exactly like {@link authEnv} (see the
 * collapse note above): a key absent from the installed scope yields the
 * declared default, never another profile's first-writer-bridged value
 * (#72348/#86905 class).
 */
export function platformGateEnv(name: string, defaultValue = ""): string {
	return readScoped(name, defaultValue);
}
