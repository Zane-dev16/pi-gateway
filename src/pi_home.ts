// pi_home — the single home accessor every gateway path resolves through.
//
// Spec: /root/pi-gateway/01-architecture.md §6 "PI_HOME Override Discipline"
// (step-by-step execution order is binding) and §5.3 (zero-dep bottom layer).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_constants.py:get_hermes_home            → resolvePiHome
//   hermes_constants.py:get_hermes_home_override   → context-local override (ctxvar ≙ AsyncLocalStorage)
//   hermes_constants.py:display_hermes_home        → displayPiHome
//   hermes_constants.py:_warn_profile_fallback_once→ the loud one-shot fallback warning
//
// Resolution order inside the accessor (01 §6 step 2):
//   context-local override → PI_HOME env var → platform-native default (~/.pi).
//
// Per the phase directive, the env var is read ONCE at the first resolution and
// cached for the process lifetime — entrypoints must install overrides before any
// project import (01 §6 step 1), which is exactly what makes this cache safe.
// Tests reset the cache via resetPiHomeCacheForTests().

import { existsSync, readFileSync } from "node:fs";
import { homedir } from "node:os";
import { AsyncLocalStorage } from "node:async_hooks";

const ENV_VAR = "PI_HOME";
const HOME_DIRNAME = ".pi";
const ACTIVE_PROFILE_MARKER = "active_profile";

interface HomeOverride {
	readonly home: string;
}

// `undefined` is a legal store here: entering with undefined clears the
// override on the current async context (resetPiHomeOverride).
const overrideStorage = new AsyncLocalStorage<HomeOverride | undefined>();

/** Cached PI_HOME env read. `undefined` = not read yet; `null` = read, unset. */
let cachedEnvHome: string | null | undefined;
let warnedProfileFallback = false;

function platformDefaultHome(): string {
	// Parity of hermes_constants.py:_get_platform_default_hermes_home (win32 branch
	// under LOCALAPPDATA, POSIX under the user home).
	if (process.platform === "win32") {
		const localAppData = (process.env.LOCALAPPDATA ?? "").trim();
		const base = localAppData !== "" ? localAppData : homedir();
		return joinPath(base, "pi");
	}
	return joinPath(homedir(), HOME_DIRNAME);
}

/** Path.join without importing node:path wholesale — keep this module zero-dep-ish. */
function joinPath(base: string, leaf: string): string {
	return base.endsWith("/") || base.endsWith("\\")
		? `${base}${leaf}`
		: `${base}/${leaf}`;
}

/**
 * Loud ONE-SHOT warning when PI_HOME is unset while an `active_profile` marker
 * names a non-default profile (01 §6 step 2; hermes_constants.py:
 * _warn_profile_fallback_once). Never raises — raising would brick module-level
 * callers. Writes straight to stderr because this fires before logging config.
 */
function warnProfileFallbackOnce(defaultHome: string): void {
	if (warnedProfileFallback) return;
	let active = "";
	try {
		const marker = joinPath(defaultHome, ACTIVE_PROFILE_MARKER);
		if (existsSync(marker)) {
			active = readFileSync(marker, "utf8").trim();
		}
	} catch {
		active = "";
	}
	if (active !== "" && active !== "default") {
		warnedProfileFallback = true;
		const msg =
			`[PI_HOME fallback] ${ENV_VAR} is unset but active profile is ${JSON.stringify(active)}. ` +
			`Falling back to ${defaultHome}, which is the DEFAULT profile — not ${JSON.stringify(active)}. ` +
			`Any data this process writes will land in the wrong profile. The subprocess spawner must pass ${ENV_VAR} explicitly.`;
		try {
			process.stderr.write(`${msg}\n`);
		} catch {
			/* stderr unavailable — nothing more we can do */
		}
	}
}

/**
 * The single source of truth for the gateway home directory (01 §6).
 * Resolution: context-local override → PI_HOME (read once per process) →
 * platform-native default. All state paths MUST flow through here; no
 * hardcoded ~/.pi anywhere (#3575 bug class).
 */
export function resolvePiHome(): string {
	const override = overrideStorage.getStore();
	if (override !== undefined && override.home !== "") {
		return override.home;
	}
	if (cachedEnvHome === undefined) {
		const raw = (process.env[ENV_VAR] ?? "").trim();
		cachedEnvHome = raw !== "" ? raw : null;
	}
	if (cachedEnvHome !== null) {
		return cachedEnvHome;
	}
	const defaultHome = platformDefaultHome();
	warnProfileFallbackOnce(defaultHome);
	return defaultHome;
}

/** User-facing display form: collapse the user-home prefix to `~`. */
export function displayPiHome(): string {
	const home = resolvePiHome();
	const userHome = homedir();
	if (userHome !== "" && (home === userHome || home.startsWith(userHome))) {
		return `~${home.slice(userHome.length)}`;
	}
	return home;
}

/**
 * Install a context-local home override for the CURRENT synchronous execution
 * context and any promises spawned from it (AsyncLocalStorage ≙ Hermes ctxvar,
 * hermes_constants.py:set_hermes_home_override). Used by entrypoints before
 * imports and by multiplex per-turn scopes (01 §6 steps 1 and 6).
 */
export function setPiHomeOverride(home: string): void {
	overrideStorage.enterWith({ home });
}

/** Run `fn` under a context-local home override, restoring afterwards. */
export function runWithPiHomeOverride<T>(home: string, fn: () => T): T {
	return overrideStorage.run({ home }, fn);
}

/** Remove any context-local override installed on the current context. */
export function resetPiHomeOverride(): void {
	overrideStorage.enterWith(undefined);
}

/**
 * The process-scoped (env/default) home ignoring context overrides — parity of
 * hermes_constants.py:get_process_hermes_home. Spawners use this to propagate
 * PI_HOME explicitly to children (01 §6 step 4).
 */
export function processScopedPiHome(): string {
	if (cachedEnvHome === undefined) {
		const raw = (process.env[ENV_VAR] ?? "").trim();
		cachedEnvHome = raw !== "" ? raw : null;
	}
	return cachedEnvHome ?? platformDefaultHome();
}

/** Test hook: forget the cached env read and the fallback-warning latch. */
export function resetPiHomeCacheForTests(): void {
	cachedEnvHome = undefined;
	warnedProfileFallback = false;
}
