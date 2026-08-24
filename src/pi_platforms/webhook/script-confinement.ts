// pi_platforms/webhook/script-confinement — DEC-017: "script transforms
// confined under the home directory (relative_to check)".
//
// Ported from the READ-ONLY Hermes reference:
//   gateway/platforms/webhook_filters.py:_resolve_script_path — a route's
//   script path must resolve INSIDE the configured root; absolute paths,
//   traversal escapes, and symlink-free lexical checks fail closed.

import { isAbsolute, join, normalize, sep } from "node:path";

export type ScriptPathResolution =
	| { ok: true; resolvedPath: string }
	| { ok: false; reason: string };

/**
 * Resolve a route script path against the home root, failing closed on any
 * escape. Empty paths reject (a script route without a script is a config
 * error, not a pass-through).
 */
export function resolveScriptPath(
	homeRoot: string,
	scriptPath: string,
): ScriptPathResolution {
	if (scriptPath.length === 0) {
		return { ok: false, reason: "script path is empty" };
	}
	if (isAbsolute(scriptPath)) {
		return { ok: false, reason: "script path must be relative to home" };
	}
	const root = normalize(homeRoot);
	const candidate = normalize(join(root, scriptPath));
	// relative_to parity: the candidate MUST stay under the normalized root
	// (prefix + separator boundary — "/home/x-evil" must not match "/home/x").
	if (candidate !== root && !candidate.startsWith(root + sep)) {
		return { ok: false, reason: "script path escapes home confinement" };
	}
	return { ok: true, resolvedPath: candidate };
}
