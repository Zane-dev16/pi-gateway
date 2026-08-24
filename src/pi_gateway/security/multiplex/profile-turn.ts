// multiplex/profile-turn — the per-turn isolation combinator (06 §4).
//
// "Multiple isolated instances share one gateway process... State isolation
// checklist per turn: home override (config, skills, memory, sessions, SOUL)
// · secret scope (.env + external sources) · per-profile pairing stores ·
// per-profile adapter registries." (06 §4; execution order parity of
// gateway/run.py::_profile_runtime_scope — 01 §6 step 6.)
//
// Applied ONLY around multiplexed inbound paths (secondary-profile adapter
// dispatch and turns) — never globally, never around unrelated tasks. The
// combinator:
//
//   1. stamps the PROFILE TURN CONTEXT (which profile + resolved home) for
//      the current async context — the stable isolation boundary consumers
//      like the check_fn cache key on (parity: Hermes keys that cache on
//      get_hermes_home_override(); pi_home's override storage is private to
//      pi_home.ts, so this module carries the turn's identity explicitly
//      instead of forking a second home primitive);
//   2. installs the home override (runWithPiHomeOverride via secretscope's
//      withProfileRuntimeScope);
//   3. installs the profile secret scope built from <home>/.env WITHOUT
//      touching process env (buildProfileSecretScope), defaulting when the
//      caller does not supply a mapping;
//   4. resets BOTH tokens in REVERSE order on unwind EVEN WHEN fn THROWS
//      (06 §9 scope-reset-hygiene row), restoring the outer context exactly.
//
// Fail-closed validation: an unnamed profile or blank home REFUSES to run —
// an unidentifiable turn is the aliasing hazard every consumer below guards
// against.

import { AsyncLocalStorage } from "node:async_hooks";
import { resolve } from "node:path";

import {
	buildProfileSecretScope,
	withProfileRuntimeScope,
	type SecretMapping,
} from "../secretscope/index.js";

/** Identity of the profile whose turn is executing on this async context. */
export interface ProfileTurnContext {
	readonly profile: string;
	/** Resolved ABSOLUTE profile home — the cache-scope key (stable across turns). */
	readonly home: string;
}

const turnStorage = new AsyncLocalStorage<ProfileTurnContext>();

/** The profile turn executing on this async context, or undefined. */
export function currentProfileTurn(): ProfileTurnContext | undefined {
	return turnStorage.getStore();
}

export interface ProfileIsolationOptions {
	/** Profile name as stamped on inbound sources ("" refuses). */
	profile: string;
	/** Profile home directory (blank refuses; normalized via path.resolve). */
	home: string;
	/**
	 * Explicit secret mapping. Default: buildProfileSecretScope(home) — the
	 * <home>/.env parse that never touches process env (06 §3 sketch).
	 * Pass an empty mapping (`secretMappingFromRecord({})`) for profiles with
	 * no .env; pass `hydrate` for external secret sources.
	 */
	secrets?: SecretMapping | undefined;
	/** Hydrate external profile secret sources BEFORE the scope installs. */
	hydrate?: ((home: string) => void) | undefined;
}

function requireContext(opts: ProfileIsolationOptions): ProfileTurnContext {
	const profile = opts.profile.trim();
	if (profile === "") {
		throw new Error(
			"withProfileIsolation: refusing to run a multiplexed turn without a " +
				"profile name — an unidentifiable turn would alias into whichever " +
				"context is ambient (the #86905 incident class).",
		);
	}
	if (opts.home.trim() === "") {
		throw new Error(
			`withProfileIsolation: profile ${JSON.stringify(profile)} has no home ` +
				"directory — refusing to install an empty home override.",
		);
	}
	return { profile, home: resolve(opts.home) };
}

/**
 * Run `fn` as ONE fully isolated profile turn: profile-context stamp → home
 * override → secret scope, reset in reverse order even on throw. NESTED turns
 * compose correctly: an inner profile-B boundary restores profile A's home,
 * scope, and stamp when B unwinds (hygiene tests cover exception paths).
 */
export function withProfileIsolation<T>(
	opts: ProfileIsolationOptions,
	fn: () => T,
): T {
	const ctx = requireContext(opts);
	// Hydrate is forwarded (not called here) so it lands BETWEEN the home
	// override and the scope install — gateway/run.py::_profile_runtime_scope
	// order: home token → hydrate external sources → secret-scope token.
	return turnStorage.run(ctx, () =>
		withProfileRuntimeScope(
			ctx.home,
			opts.secrets ?? buildProfileSecretScope(ctx.home),
			fn,
			{ hydrate: opts.hydrate },
		),
	);
}
