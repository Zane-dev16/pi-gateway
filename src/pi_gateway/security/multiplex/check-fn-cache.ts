// multiplex/check-fn-cache — availability-probe cache keyed (fn, scope)
// (05 §3.2, verified semantics of tools/registry.py::_check_fn_cached and
// friends; READ-ONLY Hermes reference — semantics ported, no code vendored).
//
//   tools/registry.py:_CHECK_FN_TTL_SECONDS          → CHECK_FN_TTL_SECONDS (30 s)
//   tools/registry.py:_CHECK_FN_FAILURE_GRACE_SECONDS→ CHECK_FN_FAILURE_GRACE_SECONDS (60 s)
//   tools/registry.py:_CHECK_FN_CACHE_MAX            → CHECK_FN_CACHE_MAX (512, FIFO prune)
//   tools/registry.py:_check_fn_cache / _last_good    → entry/lastGood maps
//   tools/registry.py:CHECK_FN_CACHE_BYPASS ("")     → CHECK_FN_CACHE_BYPASS
//   tools/registry.py:no_cache_check_fn              → CheckFnCache.markUncached
//   tools/registry.py:invalidate_check_fn_cache      → CheckFnCache.invalidate
//   tools/registry.py:get_cached_check_fn_result     → CheckFnCache.peek (NEVER probes)
//
// Cache scope ladder — check_fn_cache_scope() parity:
//   1. request-bound session identity (session id + browser-control principal
//      + transport family ALL set) → BYPASS. Live browser control changes on
//      every attach/detach; one session's tools must never leak into another.
//      (Minimal in-house slice of Hermes gateway/session_context.py ctxvars;
//      wiring gap recorded for Phase 4+ when the full session context lands.)
//   2. multiplex OFF → null: single-profile deployments keep the historical
//      PROCESS-WIDE entry (scope component absent — the "multiplex-OFF
//      bypass" shorthand; all callers share one verdict per fn).
//   3. multiplex ON + profile turn context → the resolved profile home
//      (stable isolation boundary across turns for that profile).
//   4. multiplex ON + NO resolvable profile identity → BYPASS, fail closed:
//      never alias requests whose identity is unknown into a shared entry.
//      Flipping setMultiplexActive(true) mid-process therefore disables the
//      process-wide sharing WITHOUT a restart: lookups re-key immediately and
//      stale null-scope entries become unreachable.
//
// Failure semantics (#21658/#5304): a probe failure within 60 s of the last
// success is served as last-good True WITHOUT caching the failure (next call
// re-probes); a failure past the grace window is honored and cached so a
// genuinely-down backend stops advertising its tools. Raised probes count as
// failures and warn with the unresolved-scope flag on bypass paths.

import { AsyncLocalStorage } from "node:async_hooks";
import { resolve as resolvePath } from "node:path";

import { currentProfileTurn, type ProfileTurnContext } from "./profile-turn.js";
import { isMultiplexActive } from "../secretscope/index.js";

/** TTL for a cached verdict (seconds). */
export const CHECK_FN_TTL_SECONDS = 30;
/** How long after a success a fresh failure is treated as a flake (seconds). */
export const CHECK_FN_FAILURE_GRACE_SECONDS = 60;
/** Cache cap; oldest-inserted entries pruned first (insertion-order Map). */
export const CHECK_FN_CACHE_MAX = 512;
/**
 * Scope sentinel meaning "consult NO shared cache" — request-bound sessions
 * and unresolvable profile identities probe fresh EVERY call.
 */
export const CHECK_FN_CACHE_BYPASS = "";

/** Scope key component: null = historical process-wide entry; "" = BYPASS. */
export type CheckFnScope = string | null;

// ── request-bound session identity (session_context minimal slice) ────────

/**
 * The three fields that mark a session REQUEST-BOUND (live browser control).
 * All three must be non-blank to trigger the bypass — parity of the
 * `all(str(value or "").strip() ...)` guard in check_fn_cache_scope().
 */
export interface RequestBoundIdentity {
	sessionId: string;
	principal: string;
	transportFamily: string;
}

const identityStorage = new AsyncLocalStorage<RequestBoundIdentity>();

/** Stamp the request-bound identity for this async context (tests + the
 * future browser-control wiring). Undefined store clears nothing here — use
 * nesting scoping instead. */
export function runWithRequestIdentity<T>(
	identity: RequestBoundIdentity | undefined,
	fn: () => T,
): T {
	return identityStorage.run(identity as RequestBoundIdentity, fn);
}

export function currentRequestIdentity(): RequestBoundIdentity | undefined {
	return identityStorage.getStore();
}

function isRequestBound(
	identity: RequestBoundIdentity | undefined,
): identity is RequestBoundIdentity {
	if (identity === undefined) return false;
	return (
		identity.sessionId.trim() !== "" &&
		identity.principal.trim() !== "" &&
		identity.transportFamily.trim() !== ""
	);
}

// ── warnings (logger.warning port sites) ──────────────────────────────────

export type CheckFnWarning =
	| { kind: "probe_raised"; unresolvedScope: boolean }
	| { kind: "transient_failure_served_last_good"; withinSeconds: number }
	| { kind: "probe_failed"; raised: boolean };

export type CheckFnWarningSink = (warning: CheckFnWarning) => void;

export interface CheckFnCacheOptions {
	/** Monotonic seconds. Default performance.now()/1000 (time.monotonic parity). */
	nowSeconds?: (() => number) | undefined;
	/** Warning sink (runner wiring); default discards. */
	onWarning?: CheckFnWarningSink | undefined;
}

interface CacheEntry {
	ts: number;
	value: boolean;
}

function monotonicSeconds(): number {
	return performance.now() / 1000;
}

/**
 * The (fn, scope) TTL cache. One instance per gateway process (exported
 * singleton `checkFnCache`); tests construct isolated instances with injected
 * clocks. All methods are synchronous — JS's single thread stands in for
 * Hermes' `_check_fn_cache_lock`.
 */
export class CheckFnCache {
	private readonly entries = new Map<string, CacheEntry>();
	private readonly lastGood = new Map<string, number>();
	private readonly uncachedFns = new Set<() => boolean>();
	private readonly fnIds = new WeakMap<() => boolean, number>();
	private nextFnId = 1;

	private readonly now: () => number;
	private readonly onWarning: CheckFnWarningSink | undefined;

	constructor(opts: CheckFnCacheOptions = {}) {
		this.now = opts.nowSeconds ?? monotonicSeconds;
		this.onWarning = opts.onWarning;
	}

	// ── scope resolution (check_fn_cache_scope parity) ─────────────────────

	/**
	 * The active cache scope: BYPASS sentinel, null (process-wide), or the
	 * resolved profile home. See the module ladder above.
	 */
	cacheScope(): CheckFnScope {
		try {
			// 1. Fully bound browser-control requests bypass both cache layers.
			const identity = currentRequestIdentity();
			if (isRequestBound(identity)) return CHECK_FN_CACHE_BYPASS;

			// 2. Single-profile processes keep the historical process-wide cache.
			if (!isMultiplexActive()) return null;

			// 3./4. Under multiplex the canonical key is the stable per-profile
			// boundary; an unresolvable identity FAILS CLOSED to bypass rather
			// than aliasing requests (parity of the except-clause).
			const turn: ProfileTurnContext | undefined = currentProfileTurn();
			if (turn === undefined || turn.home.trim() === "") {
				return CHECK_FN_CACHE_BYPASS;
			}
			return resolvePath(turn.home);
		} catch {
			return CHECK_FN_CACHE_BYPASS;
		}
	}

	// ── internals ───────────────────────────────────────────────────────────

	private keyOf(fn: () => boolean, scope: Exclude<CheckFnScope, "">): string {
		let id = this.fnIds.get(fn);
		if (id === undefined) {
			id = this.nextFnId++;
			this.fnIds.set(fn, id);
		}
		// Unambiguous composite: JSON keeps null distinct from any string and
		// makes separator collisions impossible.
		return JSON.stringify([id, scope]);
	}

	/** Expire stale entries and cap growth. Caller discipline mirrors Hermes:
	 * run under the (implicit JS) lock before reads AND before writes. */
	private prune(now: number): void {
		for (const [key, entry] of this.entries) {
			if (now - entry.ts >= CHECK_FN_TTL_SECONDS) this.entries.delete(key);
		}
		for (const [key, ts] of this.lastGood) {
			if (now - ts >= CHECK_FN_FAILURE_GRACE_SECONDS) this.lastGood.delete(key);
		}
		while (this.entries.size >= CHECK_FN_CACHE_MAX) {
			this.entries.delete(this.entries.keys().next().value as string);
		}
		while (this.lastGood.size >= CHECK_FN_CACHE_MAX) {
			this.lastGood.delete(this.lastGood.keys().next().value as string);
		}
	}

	private runUncached(fn: () => boolean, unresolvedScope: boolean): boolean {
		try {
			return Boolean(fn());
		} catch {
			this.onWarning?.({ kind: "probe_raised", unresolvedScope });
			return false;
		}
	}

	// ── public surface ───────────────────────────────────────────────────────

	/**
	 * Run an availability probe with TTL caching across calls. Verdicts key on
	 * (fn identity, current cache scope) — profiles never collide, and BYPASS
	 * scopes never touch the maps at all.
	 */
	run(fn: () => boolean): boolean {
		if (this.uncachedFns.has(fn)) return this.runUncached(fn, false);

		const scope = this.cacheScope();
		if (scope === CHECK_FN_CACHE_BYPASS) {
			return this.runUncached(fn, true);
		}
		const key = this.keyOf(fn, scope);

		let now = this.now();
		this.prune(now);
		const cached = this.entries.get(key);
		if (cached !== undefined && now - cached.ts < CHECK_FN_TTL_SECONDS) {
			return cached.value;
		}

		let raised = false;
		let value: boolean;
		try {
			value = Boolean(fn());
		} catch {
			value = false;
			raised = true;
		}

		now = this.now();
		this.prune(now);
		if (value) {
			this.lastGood.set(key, now);
			this.entries.set(key, { ts: now, value: true }); // keeps insertion order
			return true;
		}

		const lastGoodTs = this.lastGood.get(key);
		if (
			lastGoodTs !== undefined &&
			now - lastGoodTs < CHECK_FN_FAILURE_GRACE_SECONDS
		) {
			// Recent success → treat as a flake. Serve last-good True and do NOT
			// cache the failure, so the next call re-probes instead of pinning a
			// stale verdict for the full TTL.
			this.onWarning?.({
				kind: "transient_failure_served_last_good",
				withinSeconds: now - lastGoodTs,
			});
			return true;
		}

		this.onWarning?.({ kind: "probe_failed", raised });
		this.entries.set(key, { ts: now, value: false });
		return false;
	}

	/**
	 * Current cached verdict WITHOUT ever executing the probe (read-only
	 * surfaces). Null when bypassed, uncached, or past TTL — there is no
	 * trustworthy verdict to report.
	 */
	peek(fn: () => boolean): boolean | null {
		const scope = this.cacheScope();
		if (scope === CHECK_FN_CACHE_BYPASS) return null;
		const entry = this.entries.get(this.keyOf(fn, scope));
		if (entry === undefined) return null;
		return this.now() - entry.ts < CHECK_FN_TTL_SECONDS ? entry.value : null;
	}

	/** Mark a local/config-backed check as permanently uncached (@no_cache_check_fn). */
	markUncached(fn: () => boolean): void {
		this.uncachedFns.add(fn);
	}

	/** Drop every cached verdict (config changes affecting availability). */
	invalidate(): void {
		this.entries.clear();
		this.lastGood.clear();
	}

	/** Diagnostic size (entries map; tests + status panels). */
	get size(): number {
		return this.entries.size;
	}
}

/**
 * Process-wide instance for registry wiring (parity of the module-global
 * Hermes cache). Tests prefer isolated instances with injected clocks.
 */
export const checkFnCache = new CheckFnCache();
