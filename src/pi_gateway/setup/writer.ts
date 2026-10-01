// pi_gateway/setup/writer — secretscope-owned profile .env writer (DEC-081).
//
// Reuses the allowlist mirror's atomic 0600 merge-preserving posture instead
// of a second writer: temp-file plus atomic replace, chmod 0600, unrelated
// keys preserved, re-runnable for rotation. Values are never logged here.

import { defaultAllowlistMirrorForHome } from "../security/authz/env-mirror.js";

export async function writeSetupVars(
	home: string,
	values: ReadonlyMap<string, string>,
): Promise<void> {
	const mirror = defaultAllowlistMirrorForHome(home);
	for (const [name, value] of values) mirror.writeVar(name, value);
}
