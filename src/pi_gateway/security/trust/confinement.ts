// pi_gateway/security/trust/confinement — script-transform path confinement
// under <home>/scripts via `relative_to` semantics (06 §8.2; DEC-017).
//
// Ported from the READ-ONLY Hermes reference:
//   gateway/platforms/webhook_filters.py:_resolve_profile_path (@30) —
//     expandvars → ~/.hermes maps to the active profile home.
//   gateway/platforms/webhook_filters.py:_resolve_script_path (@49) —
//     expandvars/expanduser → resolve() → candidate.relative_to(scripts_root)
//     else REFUSE → exists + is_file checks. Escape attempts (`..`, absolute
//     paths outside home, `~/.hermes` aliases) refuse at config load.
//
// Exact semantics worth pinning (all covered by the escape matrix):
//   * "~/.hermes" ALONE resolves to the profile home, NOT under scripts/ ⇒
//     relative_to refuses it (it is listed among the escape attempts).
//   * Absolute paths are RESOLVED then confined — an absolute path that
//     genuinely lands under scripts_root is legal (Hermes parity); only
//     resolution OUTSIDE the root refuses.
//   * resolve() collapses symlinked segments, so a lexically-inside /
//     really-outside path refuses too (the "symlink-ish" encoding row).

import { isAbsolute, join, sep } from "node:path";

/** Filesystem seam — injected so tests fake resolution without real trees. */
export interface ConfinementFs {
	/**
	 * Python Path.resolve(strict=False) parity: make absolute, collapse
	 * `.`/`..` AND resolve symlinks where the target exists (non-strict).
	 */
	resolve(path: string): string;
	exists(path: string): boolean;
	isFileSync(path: string): boolean;
}

export interface ScriptConfinementConfig {
	/** The confinement root (<home>/scripts analog). */
	scriptsRoot: string;
	/** Where the `~/.hermes` alias maps (active profile home). */
	profileHome: string;
	/** expandvars source — injected; engines never read process.env directly. */
	env: Record<string, string>;
	/** `~` expansion target (defaults to profileHome). */
	userHome?: string | undefined;
	fs: ConfinementFs;
}

export type ScriptPathResolution =
	| { ok: true; resolvedPath: string }
	| { ok: false; reason: string };

/** os.path.expandvars parity: $VAR / ${VAR} expand when present, else stay literal. */
export function expandVars(value: string, env: Record<string, string>): string {
	return value.replace(/\$\{(\w+)\}|\$(\w+)/g, (whole, braced, plain) => {
		const name = (braced as string | undefined) ?? (plain as string);
		const v = env[name];
		return v !== undefined ? v : whole;
	});
}

/** Path.expanduser parity for leading ~ (bare ~ and ~/…). */
export function expandUser(value: string, userHome: string): string {
	if (value === "~") return userHome;
	if (value.startsWith("~/")) return join(userHome, value.slice(2));
	return value;
}

function mapHermesAlias(raw: string, profileHome: string): string | null {
	if (raw === "~/.hermes") return profileHome;
	if (raw.startsWith("~/.hermes/")) {
		return join(profileHome, raw.slice("~/.hermes/".length));
	}
	return null;
}

/**
 * Resolve a route script under the confinement root; refuse (ok:false) on
 * any escape, missing file, or non-file target. Non-string input refuses
 * (a config type error is a refusal, never a pass-through).
 */
export function resolveScriptPath(
	config: ScriptConfinementConfig,
	scriptValue: unknown,
): ScriptPathResolution {
	if (typeof scriptValue !== "string" || scriptValue.trim().length === 0) {
		return { ok: false, reason: "script path is empty" };
	}
	const rootResolved = config.fs.resolve(config.scriptsRoot);
	const rawText = expandVars(scriptValue.trim(), config.env);

	let candidateRaw: string;
	const alias = mapHermesAlias(rawText, config.profileHome);
	if (alias !== null) {
		candidateRaw = alias;
	} else {
		const userHome = config.userHome ?? config.profileHome;
		candidateRaw = expandUser(rawText, userHome);
	}

	const candidate = isAbsolute(candidateRaw)
		? config.fs.resolve(candidateRaw)
		: config.fs.resolve(join(rootResolved, candidateRaw));

	// relative_to(scripts_root) semantics: the resolved candidate must be the
	// root itself or live strictly beneath it (prefix WITH separator boundary
	// so "/home/x-evil" never matches "/home/x").
	if (candidate !== rootResolved && !candidate.startsWith(rootResolved + sep)) {
		return {
			ok: false,
			reason: `script path resolves outside ${rootResolved}`,
		};
	}
	if (!config.fs.exists(candidate)) {
		return { ok: false, reason: `script not found: ${candidate}` };
	}
	if (!config.fs.isFileSync(candidate)) {
		return { ok: false, reason: `script path is not a file: ${candidate}` };
	}
	return { ok: true, resolvedPath: candidate };
}
