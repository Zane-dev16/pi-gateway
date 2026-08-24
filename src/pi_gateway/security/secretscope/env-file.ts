// secretscope/env-file — profile .env loading WITHOUT touching process env
// (06 §3: "parses <home>/.env WITHOUT touching os.environ").
//
// Ports of:
//   agent/secret_scope.py:load_env_file           → loadEnvFile
//   agent/secret_scope.py:_strip_inline_comment   → stripInlineComment
//   hermes_cli/config.py:_parse_env_value         → parseEnvValue
//   agent/secret_scope.py:build_profile_secret_scope → buildProfileSecretScope
//
// The parser handles the small KEY=VALUE subset the gateway writes itself
// (`export` prefix, full-line comments, dotenv-compatible inline comments,
// matching quotes with `\"`/`\\` escapes reversed). Encoding is UTF-8 with a
// leading BOM stripped so Windows-authored files don't prefix the first key.
//
// KNOWN GAP (recorded, not silently dropped): Hermes' build_profile_secret_scope
// also folds in external secret sources (1Password/bitwarden via
// hermes_cli/env_loader.get_secret_source_values). Pi Gateway's external
// source registry is not built yet; this port loads <home>/.env only. Wiring
// hydrateExternalSecretSources is a Phase-4+ item.

import { readFileSync } from "node:fs";
import { join } from "node:path";
import type { SecretMapping } from "./scope.js";
import { isGlobalEnv } from "./global-env.js";

/**
 * Strip a dotenv-style inline comment from a raw .env value. Parity of
 * agent/secret_scope.py:_strip_inline_comment (python-dotenv 1.2.2 semantics):
 * - Quoted values keep everything through the matching close quote (backslash-
 *   escape-aware for double quotes); a trailing `# ...` after it is discarded;
 *   non-comment trailing junk leaves the value untouched.
 * - Unquoted values truncate only at a `#` PRECEDED BY WHITESPACE, so
 *   `KEY=foo#bar` keeps `foo#bar` while `KEY=value # comment` keeps `value`.
 * - A value that STARTS with `#` (KEY=#leading) is kept.
 */
export function stripInlineComment(value: string): string {
	const trimmed = value.trim();
	if (trimmed === "") return trimmed;
	const quote = trimmed[0] as string;
	if (quote === "'" || quote === '"') {
		let i = 1;
		while (i < trimmed.length) {
			const ch = trimmed[i] as string;
			if (quote === '"' && ch === "\\") {
				i += 2; // skip the escaped character
				continue;
			}
			if (ch === quote) {
				const remainder = trimmed.slice(i + 1).trimStart();
				return remainder.startsWith("#") ? trimmed.slice(0, i + 1) : trimmed;
			}
			i += 1;
		}
		return trimmed; // unterminated quote: leave as-is
	}
	return trimmed.split(/\s+#/, 1)[0]?.trim() ?? "";
}

/** Parse the small .env value subset the gateway writes itself. Port of
 * hermes_cli/config.py:_parse_env_value — reverses the writer's `\"`/`\\`
 * escapes inside double quotes so credentials containing " or \ survive
 * scoped resolution intact. */
export function parseEnvValue(rawValue: string): string {
	const value = rawValue.trim();
	if (value.length >= 2 && value.startsWith('"') && value.endsWith('"')) {
		const quoted = value.slice(1, -1);
		let out = "";
		let i = 0;
		while (i < quoted.length) {
			const ch = quoted[i] as string;
			if (ch === "\\" && i + 1 < quoted.length) {
				const nextCh = quoted[i + 1] as string;
				if (nextCh === '"' || nextCh === "\\") {
					out += nextCh;
					i += 2;
					continue;
				}
			}
			out += ch;
			i += 1;
		}
		return out;
	}
	if (value.length >= 2 && value.startsWith("'") && value.endsWith("'")) {
		return value.slice(1, -1);
	}
	return value;
}

/**
 * Parse a .env file into an isolated mapping WITHOUT mutating process.env —
 * that isolation is the whole point (06 §3). Missing/unreadable/undecodable
 * file ⇒ empty mapping (parity of the FileNotFoundError/OSError swallow).
 */
export function loadEnvFile(envPath: string): Map<string, string> {
	const secrets = new Map<string, string>();
	let text: string;
	try {
		text = readFileSync(envPath, "utf8");
	} catch {
		return secrets;
	}
	if (text.startsWith("\uFEFF")) text = text.slice(1); // utf-8-sig parity

	for (const rawLine of text.split(/\r?\n/)) {
		let line = rawLine.trim();
		if (line === "" || line.startsWith("#")) continue;
		if (line.startsWith("export "))
			line = line.slice("export ".length).trimStart();
		const eq = line.indexOf("=");
		if (eq === -1) continue;
		const key = line.slice(0, eq).trim();
		if (key === "") continue;
		const rawValue = line.slice(eq + 1);
		secrets.set(key, parseEnvValue(stripInlineComment(rawValue)));
	}
	return secrets;
}

/**
 * Build a profile's secret mapping from `<home>/.env`. Returns a FRESH map
 * (safe to install via setSecretScope). Genuinely-global names are
 * intentionally NOT copied in — getSecret reads those from process env
 * directly, so the scope holds only profile secrets (agent/
 * secret_scope.py:build_profile_secret_scope).
 */
export function buildProfileSecretScope(home: string): SecretMapping {
	const secrets = loadEnvFile(join(home, ".env"));
	for (const key of [...secrets.keys()]) {
		if (isGlobalEnv(key)) secrets.delete(key);
	}
	return secrets;
}
