// multiplex/profile-env — PROFILE-SCOPED environment reads that FAIL CLOSED
// (06 §4 build item 1; roadmap Phase-4 item 5).
//
// Incident classes: #86905 / #72348 — under a profile multiplexer, resolving a
// profile's env value by falling back to `os.environ` borrows ANOTHER
// profile's credential or allowlist. DEC-003 as amended by DEC-009 bans that
// shape on every SCOPED path ("applies to authz reads with zero exceptions").
//
// Relationship to the secretscope engine (deliberate split, not duplication):
//   secretscope/resolve.getSecret      — the FULL 06 §3 resolution table
//     (global-env carve-out, sanctioned unscoped-default-profile fallback via
//     wrapper.ts, and the cron OVERLAY fallthrough when multiplex is OFF);
//   multiplex/profile-env (THIS file)  — the STRICTER primitive for callers
//     that demand profile-only resolution: reads consult ONLY the installed
//     profile mapping, in EVERY mode. No carve-out branch (when in doubt a
//     value is a profile secret — 06 §3.1), no overlay branch, no process-env
//     read of any kind. A missing key yields the declared default; `require`
//     raises instead. Callers needing deployment-level knobs (PI_* runtime,
//     listener settings) read getSecret directly so the §3.1 carve-out keeps
//     applying.
//
// No-scope discipline: this reader exists to serve MULTIPLEXED paths, so a
// read with NO profile scope installed raises UnscopedSecretError regardless
// of the multiplex flag — there is no profile mapping to resolve from, and
// borrowing process env here is precisely the banned shape.

import {
	currentSecretScope,
	UnscopedSecretError,
	type SecretMapping,
} from "../secretscope/index.js";

/** Raised when `require` cannot find the name IN THE PROFILE'S OWN mapping. */
export class ProfileEnvMissingError extends Error {
	constructor(varName: string) {
		super(
			`profileEnv.require(${JSON.stringify(varName)}): the variable is not set ` +
				`in the active profile's secret scope. Under multiplex this read must ` +
				`be satisfied from the profile's OWN .env — process env is never ` +
				`consulted (#86905/#72348 class). Set the value in the profile home.`,
		);
		this.name = "ProfileEnvMissingError";
	}
}

/** Where a reader obtains its (isolated) mapping. May return undefined only
 * when NO scope is installed — every read then fails closed. */
export type ProfileMappingSource = () => SecretMapping | undefined;

export interface ProfileEnvReader {
	/**
	 * Resolve `name` from the profile's OWN mapping only. Present keys win —
	 * including values mapped to "" (presence parity of Python's
	 * `val is not None`). Absent keys yield `defaultValue` (undefined when not
	 * provided). NEVER reads process env, in any mode (DEC-003/DEC-009).
	 */
	get(name: string, defaultValue?: string): string | undefined;
	/**
	 * Like {@link get} but raises {@link ProfileEnvMissingError} when the key
	 * is absent from the profile mapping — for credentials whose absence must
	 * stop construction loudly instead of resolving to a default.
	 */
	require(name: string): string;
}

function makeReader(source: ProfileMappingSource): ProfileEnvReader {
	const resolveOrThrow = (name: string): { hit: boolean; value?: string } => {
		const scope = source();
		if (scope === undefined) throw new UnscopedSecretError(name);
		const hit = scope.get(name);
		return hit === undefined ? { hit: false } : { hit: true, value: hit };
	};
	return {
		get(name, defaultValue) {
			const r = resolveOrThrow(name);
			return r.hit ? (r.value as string) : defaultValue;
		},
		require(name) {
			const r = resolveOrThrow(name);
			if (!r.hit) throw new ProfileEnvMissingError(name);
			return r.value as string;
		},
	};
}

/**
 * Reader pinned to ONE explicit mapping (adapter construction that already
 * holds the profile scope dict; tests). The mapping is captured as-is — an
 * empty mapping is legal and simply denies every read its values (defaults
 * still apply).
 */
export function profileEnvFor(mapping: SecretMapping): ProfileEnvReader {
	return makeReader(() => mapping);
}

/**
 * Reader over the CURRENTLY INSTALLED secret scope (AsyncLocalStorage), for
 * call sites running inside `withProfileIsolation` / `setSecretScope` blocks.
 * No scope installed ⇒ UnscopedSecretError on every read — fail closed even
 * when multiplex is off, because THIS reader's contract is profile-only
 * resolution (the overlay/cron semantics live in secretscope's resolver).
 */
export function currentProfileEnv(): ProfileEnvReader {
	return makeReader(() => currentSecretScope());
}
