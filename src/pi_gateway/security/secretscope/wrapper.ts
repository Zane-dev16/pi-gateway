// secretscope/wrapper — THE canonical scope-aware credential read (06 §3.2).
//
// Port of plugins/platforms/feishu/adapter.py:_get_scoped_secret (the
// canonical adapter copy; same pattern as the Slack SLACK_APP_TOKEN read
// #59739 and gateway/platforms/whatsapp_common.py::_get_wsecret).
//
// ── Sanctioned vs banned except-shapes (DEC-003 as amended by DEC-009) ──
//
// This file contains the ONLY sanctioned `catch UnscopedSecretError →
// process env` clause in the repository. UnscopedSecretError fires only on
// the UNSCOPED default-profile path: the default profile constructs and sends
// unscoped under multiplexing, and there process env IS that profile's own
// value (it was written at startup). Catching the error to serve process env
// for such an unscoped call site is CORRECT.
//
// BANNED (grep-gated by scripts/check-secret-scope.mjs): any fallback to
// process env AFTER A SCOPED MISS — reading a scoped value, getting the
// declared default because the key is absent FROM THE SCOPE, then consulting
// process env. Under multiplex that borrows another profile's credential or
// allowlist (#86905 class). Adapters must import THIS wrapper verbatim —
// never hand-roll variants (06 §3.2: "reviewed once, canonicalized, copied").
// The gate exempts exactly this file and nothing else.

import { getSecret } from "./resolve.js";
import { UnscopedSecretError } from "./scope.js";

/**
 * Scope-aware credential read with the default-profile startup fallback.
 * Secondary profiles construct their adapters UNDER a profile secret scope —
 * the scope is authoritative and a scoped miss returns the declared default
 * (no cross-profile borrow from process env). The DEFAULT profile's adapter
 * constructs and sends UNSCOPED under multiplexing, where a bare getSecret
 * would raise UnscopedSecretError; there process env is that profile's own
 * value, so fall back to it.
 */
export function getScopedSecret(
	name: string,
	defaultValue?: string,
): string | undefined {
	try {
		return getSecret(name, defaultValue);
	} catch (err) {
		if (err instanceof UnscopedSecretError) {
			// SANCTIONED (DEC-009): this handler runs ONLY on the unscoped
			// default-profile path — multiplex active, NO scope installed.
			const fromEnv = process.env[name];
			return fromEnv !== undefined ? fromEnv : defaultValue;
		}
		throw err;
	}
}

/**
 * The kit secret-reader seam shape consumed by plugin registration
 * (src/pi_platforms/kit/registration.ts — `export type ScopedSecretReader =
 * (name: string) => string | undefined`). Declared here as a structural
 * mirror so pi_gateway stays downward-only (01 §5.3: rank 3 may not import
 * rank 4); TypeScript structural typing makes the real kit type accept every
 * value produced below — asserted at type level in wrapper.test.ts.
 */
export type KitScopedSecretReader = (name: string) => string | undefined;

/**
 * Produce the ScopedSecretReader handed to PluginContext/resolveEnablement at
 * registration time (04 §4.2). Routing:
 * - secondary profiles register under their installed scope → scoped hit
 *   resolves the profile's own value; a SCOPED MISS returns undefined ⇒ the
 *   kit LOUDLY disables the adapter (`secret_missing`), never borrowing
 *   process env (DEC-003/009);
 * - the default profile registers UNSCOPED under multiplex → the sanctioned
 *   wrapper fallback serves process env (its OWN values);
 * - multiplex OFF → overlay semantics (scoped miss falls through to env).
 *
 * WIRING NOTE (Phase 4): construction sites for PluginContext do not exist
 * yet; when the runner grows them, pass `kitScopedSecretReader()` here so
 * every adapter enablement read routes through this engine. No kit-side edit
 * is required — the seam already accepts this function structurally.
 */
export function kitScopedSecretReader(): KitScopedSecretReader {
	return (name: string) => getScopedSecret(name);
}
