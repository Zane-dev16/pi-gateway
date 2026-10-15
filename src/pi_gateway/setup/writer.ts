// pi_gateway/setup/writer — secretscope-owned profile .env writer (DEC-081).
//
// Reuses the allowlist mirror's atomic 0600 merge-preserving posture instead
// of a second writer: temp-file plus atomic replace, chmod 0600, unrelated
// keys preserved, re-runnable for rotation. Values are never logged here.
//
// Setup CLI contract: writes merge in place under <home>/.env, match
// existing KEY= and export KEY= lines (mirror-owned), quote values with
// spaces, strip raw newlines (a literal newline would corrupt the dotenv
// line layout), publish every written var to process.env so the current
// process sees the durable default, and keep the home dir at 0700.
// Reads are process-env-first with .env fallback: an exported var wins for
// that process, the file stays the durable default.

import { chmodSync, mkdirSync } from "node:fs";
import { defaultAllowlistMirrorForHome } from "../security/authz/env-mirror.js";

/** Current value of a setup var: live process env wins, .env is fallback. */
export function readSetupVar(
	home: string,
	name: string,
): string | undefined {
	const live = process.env[name];
	if (live !== undefined) return live;
	return defaultAllowlistMirrorForHome(home).readVar(name);
}

function ensureHomeDir(home: string): void {
	try {
		mkdirSync(home, { recursive: true, mode: 0o700 });
	} catch {
		/* managed installs and containers own the dir already — best-effort */
	}
	try {
		chmodSync(home, 0o700);
	} catch {
		/* read-only roots and containers refuse chmod — the 0600 file still holds */
	}
}

export async function writeSetupVars(
	home: string,
	values: ReadonlyMap<string, string>,
): Promise<void> {
	ensureHomeDir(home);
	const mirror = defaultAllowlistMirrorForHome(home);
	for (const [name, value] of values) {
		const clean = value.replace(/[\r\n]+/g, "");
		mirror.writeVar(name, clean);
		try {
			process.env[name] = clean;
		} catch {
			/* publish is best-effort — the file stays the durable default */
		}
	}
}
