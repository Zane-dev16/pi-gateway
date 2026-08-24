// secretscope/scope — the context-local secret scope primitive (06 §3).
//
// Port of agent/secret_scope.py (READ-ONLY Hermes reference; semantics ported,
// no code vendored):
//   agent/secret_scope.py:_SECRET_SCOPE          → scopeStorage (ctxvar ≙ AsyncLocalStorage)
//   agent/secret_scope.py:set_secret_scope       → setSecretScope
//   agent/secret_scope.py:reset_secret_scope     → resetSecretScope (token discipline)
//   agent/secret_scope.py:current_secret_scope   → currentSecretScope
//   agent/secret_scope.py:_MULTIPLEX_ACTIVE      → multiplex flag (plain module global —
//                                                  it describes the DEPLOYMENT MODE, not a
//                                                  per-task value, exactly as in Hermes)
//   agent/secret_scope.py:UnscopedSecretError    → UnscopedSecretError
//
// Fail-closed rule (DEC-003 as amended by DEC-009): under an active multiplexer
// a credential read with NO scope installed RAISES instead of borrowing
// os.environ — an un-migrated call site fails loud at that exact line rather
// than leaking another profile's value (#86905 incident class).

import { AsyncLocalStorage } from "node:async_hooks";

/** A profile's secret mapping (isolated dict — never process.env itself). */
export type SecretMapping = ReadonlyMap<string, string>;

/** Build a SecretMapping from a plain record (tests, YAML→scope bridges). */
export function secretMappingFromRecord(
	record: Record<string, string>,
): SecretMapping {
	return new Map(Object.entries(record));
}

/**
 * Raised when a secret is read in multiplex mode with no scope installed.
 * The fail-closed signal (06 §3): wrap the call path in set_secret_scope(...)
 * — the per-turn / per-adapter profile scope — never widen any allowlist.
 */
export class UnscopedSecretError extends Error {
	constructor(name: string) {
		super(
			`get_secret(${JSON.stringify(name)}) called with no profile secret scope active ` +
				`while multiplexing is on. This credential read must run inside a ` +
				`setSecretScope(...) block (the per-turn / per-adapter profile scope). ` +
				`Reading process env here would risk leaking another profile's value ` +
				`(the #86905 incident class).`,
		);
		this.name = "UnscopedSecretError";
	}
}

// `undefined` is a legal outer store: entering with undefined CLEARS the
// scope on the current async context (parity: set_secret_scope(None)). The
// store carries a PER-CONTEXT version stamp so token misuse is detectable:
// Python's ContextVar.reset raises when a surpassed/stale token is reset —
// the stamp reproduces that without cross-context interference (each async
// context versions independently).
interface ScopeStore {
	readonly mapping: SecretMapping | undefined;
	readonly version: number;
}
const scopeStorage = new AsyncLocalStorage<ScopeStore | undefined>();

/**
 * Token returned by setSecretScope, consumed by resetSecretScope. Parity of
 * contextvars.Token: captures the PREVIOUS store so nested scopes unwind in
 * reverse order, plus the version stamp for staleness detection.
 */
export interface SecretScopeToken {
	readonly previous: ScopeStore | undefined;
	/** @internal per-context install stamp at creation time. */
	readonly version: number;
	/** @internal set on first reset; a second reset is caller misuse. */
	consumed: boolean;
}

function nextVersion(previous: ScopeStore | undefined): number {
	return (previous?.version ?? 0) + 1;
}

/**
 * Install the active profile's secret mapping for the CURRENT async context
 * and everything spawned from it. Pass undefined to clear. Returns the token
 * for resetSecretScope.
 */
export function setSecretScope(
	secrets: SecretMapping | undefined,
): SecretScopeToken {
	const previous = scopeStorage.getStore();
	const token: SecretScopeToken = {
		previous,
		version: nextVersion(previous),
		consumed: false,
	};
	scopeStorage.enterWith({ mapping: secrets, version: token.version });
	return token;
}

/**
 * Restore the previous secret scope. Misuse throws instead of silently
 * corrupting state: double reset, or an OUT-OF-ORDER reset whose token was
 * surpassed by a newer install on this context (parity of ContextVar.reset
 * raising ValueError on stale tokens).
 */
export function resetSecretScope(token: SecretScopeToken): void {
	if (token.consumed) {
		throw new Error("resetSecretScope: token already consumed (double reset)");
	}
	const current = scopeStorage.getStore();
	if ((current?.version ?? 0) !== token.version) {
		throw new Error(
			"resetSecretScope: stale token — scopes must reset in reverse install order",
		);
	}
	token.consumed = true;
	scopeStorage.enterWith(token.previous);
}

/** The active secret mapping, or undefined when no scope is installed. */
export function currentSecretScope(): SecretMapping | undefined {
	return scopeStorage.getStore()?.mapping;
}

/**
 * Run `fn` inside a secret scope, restoring the outer state on unwind EVEN
 * WHEN fn THROWS (AsyncLocalStorage.run restores the previous store in a
 * finally — the parity of gateway/run.py::_profile_runtime_scope's
 * try/finally around reset_secret_scope). Prefer this over manual
 * set/reset whenever no home override is involved.
 */
export function runInSecretScope<T>(
	secrets: SecretMapping | undefined,
	fn: () => T,
): T {
	const previous = scopeStorage.getStore();
	return scopeStorage.run(
		{ mapping: secrets, version: nextVersion(previous) },
		fn,
	);
}

// ── multiplex-active flag ────────────────────────────────────────────────

let multiplexActive = false;

/**
 * Mark whether this process runs as a profile multiplexer. Called ONCE at
 * gateway startup (parity agent/secret_scope.py:set_multiplex_active). When
 * true, getSecret fails closed on unscoped reads.
 */
export function setMultiplexActive(active: boolean): void {
	multiplexActive = active;
}

export function isMultiplexActive(): boolean {
	return multiplexActive;
}
