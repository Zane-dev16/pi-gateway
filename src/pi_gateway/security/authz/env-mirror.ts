// authz/env-mirror — the operator-allowlist read/write seam behind the
// pairing grant mirror and the revocation cascade.
//
// Hermes anchors (READ-ONLY reference):
//   gateway/pairing.py:_read_allowlist_env    → AllowlistMirror.readVar
//   gateway/pairing.py:_sync_allowlist_add    → PairingStore grant mirror
//   gateway/pairing.py:_sync_allowlist_remove → PairingStore revocation cascade
//   hermes_cli/config.py:save_env_value/remove_env_value → writeVar/removeVar
//
// Pi Gateway shape: the operator's allowlist lives in the profile `.env` file
// — the SAME file secretscope's buildProfileSecretScope loads scopes from at
// startup — so one on-disk source of truth feeds both scoped authz reads and
// CLI/admin approvals. Writes NEVER touch the live process environment (grep-gate
// every mutation is a temp-file + atomic replace at mode 0600, mirroring the
// pairing store's _secure_write posture. Grant mirroring stays BEST-EFFORT:
// a failed mirror write degrades to "grant recorded but not mirrored" — the
// §2.1 step-7 union still authorizes from the store.

import {
	chmodSync,
	existsSync,
	mkdirSync,
	readFileSync,
	renameSync,
	statSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import { loadEnvFile } from "../secretscope/index.js";

export interface AllowlistMirror {
	/** Current raw value of an allowlist var (undefined when not configured). */
	readVar(name: string): string | undefined;
	/** Create or replace `name=value`, preserving every other line verbatim. */
	writeVar(name: string, value: string): void;
	/** Remove every `name=...` line; no-op when absent. */
	removeVar(name: string): void;
}

function renderValue(value: string): string {
	// Quote values containing characters dotenv parsing treats specially so a
	// round-trip through parseEnvValue/stripInlineComment is byte-exact.
	if (/^[A-Za-z0-9_@.,:*+/-]+$/.test(value)) return value;
	return `"${value.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
}

function keyLineName(line: string): string | null {
	const trimmed = line.trim();
	const candidate = trimmed.startsWith("export ")
		? trimmed.slice("export ".length).trimStart()
		: trimmed;
	const eq = candidate.indexOf("=");
	if (eq <= 0) return null;
	return candidate.slice(0, eq).trim() || null;
}

/**
 * File-backed mirror over `<home>/.env`. Reads parse the file in isolation;
 * writes rewrite it atomically. Missing file ⇒ reads are undefined and writes
 * create it (0600, parents included).
 */
export function fileAllowlistMirror(envPath: string): AllowlistMirror {
	function atomicWrite(content: string): void {
		mkdirSync(dirname(envPath), { recursive: true });
		const tmp = `${envPath}.tmp-${process.pid}-${Date.now()}`;
		try {
			writeFileSync(tmp, content, { encoding: "utf8", mode: 0o600 });
			renameSync(tmp, envPath);
			try {
				chmodSync(envPath, 0o600);
			} catch {
				/* best-effort parity of _secure_write */
			}
		} catch (err) {
			try {
				unlinkSync(tmp);
			} catch {
				/* already gone */
			}
			throw err;
		}
	}

	return {
		readVar(name: string): string | undefined {
			return loadEnvFile(envPath).get(name);
		},
		writeVar(name: string, value: string): void {
			const lines = existsSync(envPath)
				? (readFileSync(envPath, "utf8").split(/\r?\n/) as string[])
				: [];
			const rendered = `${name}=${renderValue(value)}`;
			// Last occurrence wins (dotenv semantics); earlier duplicates of the
			// same key are dropped so the key appears exactly once afterwards.
			let lastAt = -1;
			for (let i = 0; i < lines.length; i++) {
				if (keyLineName(lines[i] as string) === name) lastAt = i;
			}
			let out: string[];
			if (lastAt === -1) {
				out = [...lines];
				while (out.length > 0 && out[out.length - 1] === "") out.pop();
				out.push(rendered);
			} else {
				out = [];
				for (let i = 0; i < lines.length; i++) {
					if (keyLineName(lines[i] as string) === name) {
						if (i !== lastAt) continue; // drop duplicate/stale occurrences
						out.push(rendered);
						continue;
					}
					out.push(lines[i] as string);
				}
			}
			atomicWrite(out.join("\n") + "\n");
		},
		removeVar(name: string): void {
			if (!existsSync(envPath)) return;
			const kept = (
				readFileSync(envPath, "utf8").split(/\r?\n/) as string[]
			).filter((line) => keyLineName(line) !== name);
			atomicWrite(kept.join("\n"));
		},
	};
}

/** Default mirror location: `<home>/.env` for the given pi home root. */
export function defaultAllowlistMirrorForHome(home: string): AllowlistMirror {
	return fileAllowlistMirror(join(home, ".env"));
}

/** Stat helper used by tests to assert 0600 persistence without lying. */
export function envFileMode(envPath: string): number | null {
	try {
		return statSync(envPath).mode & 0o777;
	} catch {
		return null;
	}
}
