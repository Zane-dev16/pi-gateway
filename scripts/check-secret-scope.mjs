#!/usr/bin/env node
// check-secret-scope.mjs — CI grep gate for the fail-closed secret scope
// (06 §3.2; DEC-003 as amended by DEC-009; README invariant 6: "Grep-gate the
// forbidden fallback shape in CI").
//
// What is banned (and policed here):
//   UNSCOPED_CATCH_FALLBACK      any `catch` clause naming UnscopedSecretError
//                                whose handler touches process env — EXCEPT
//                                inside THE canonical wrapper
//                                (src/pi_gateway/security/secretscope/wrapper.ts),
//                                where the sanctioned default-profile fallback
//                                lives and must stay the ONLY copy (06 §3.2:
//                                adapters do NOT hand-roll variants).
//   COALESCED_SCOPED_MISS_...    a scoped read (getSecret/getScopedSecret/
//                                currentSecretScope) coalesced directly into
//                                process env (`getSecret("K") ?? process.env.K`)
//                                — the literal after-a-scoped-miss fallback
//                                shape (#86905 class).
//   RAW_ENV_BESIDE_SCOPE         any file OUTSIDE the engine directory that
//                                references the scope engine yet also reads
//                                process env directly (06 §11 checklist row 1:
//                                no raw env reads on multiplex-reachable paths;
//                                all env reads route through the engine, which
//                                owns the carve-out and overlay branches).
//
// Scope of the scan: <root>/src/**/*.ts EXCLUDING *.test.ts — production
// paths are gated; test files legitimately simulate banned shapes as
// detectors (behavior contracts), and fixtures live outside src/.
// The gate policeS the banned shapes, never the sanctioned catch itself
// (DEC-009: "the grep gates police the banned shape — never the catch").
//
// Usage: node scripts/check-secret-scope.mjs [projectRoot]
// Exit codes: 0 = clean; 1 = violations found; 2 = usage error.

import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const DEFAULT_ROOT = resolve(SCRIPT_DIR, "..");

const rootArg = process.argv[2];
if (rootArg !== undefined && /^\s*$/.test(rootArg)) {
	console.error("usage: node scripts/check-secret-scope.mjs [projectRoot]");
	process.exit(2);
}
const projectRoot = rootArg !== undefined ? resolve(rootArg) : DEFAULT_ROOT;
const srcRoot = join(projectRoot, "src");

const WRAPPER_REL = "src/pi_gateway/security/secretscope/wrapper.ts";
const ENGINE_DIR_REL = "src/pi_gateway/security/secretscope/";

/** Identifiers that mark a file as secret-scope-reachable (for RAW_ENV_BESIDE_SCOPE). */
const SCOPE_IDENTIFIER_RE =
	/\b(getSecret|getScopedSecret|currentSecretScope|setSecretScope|resetSecretScope|runInSecretScope|kitScopedSecretReader|buildProfileSecretScope|withProfileRuntimeScope|UnscopedSecretError)\b/;
const PROCESS_ENV_RE = /\bprocess\s*\.\s*env\b/;
const SCOPED_READ_RE =
	/\b(?:getSecret|getScopedSecret|currentSecretScope)\s*\((?:[^()]|\([^()]*\))*\)\s*(?:\?\?|\|\|)\s*process\s*\.\s*env/;
const CATCH_OPEN_RE = /\bcatch\s*\(/g;

function listTsFiles(dir) {
	const out = [];
	let entries;
	try {
		entries = readdirSync(dir);
	} catch {
		return out;
	}
	for (const entry of entries) {
		const full = join(dir, entry);
		let st;
		try {
			st = statSync(full);
		} catch {
			continue;
		}
		if (st.isDirectory()) out.push(...listTsFiles(full));
		else if (/\.ts$/.test(entry) && !/\.test\.ts$/.test(entry)) out.push(full);
	}
	return out;
}

/**
 * Extract the handler body of a catch clause starting at `openParenIdx`,
 * returning { params, body } or null when unbalanced (documented limitation:
 * brace-counting, same fidelity class as any grep gate).
 */
function extractCatchClause(text, openParenIdx) {
	let i = openParenIdx + 1;
	let parenDepth = 1;
	const paramsStart = i;
	while (i < text.length && parenDepth > 0) {
		const ch = text[i];
		if (ch === "(") parenDepth += 1;
		else if (ch === ")") parenDepth -= 1;
		i += 1;
	}
	if (parenDepth !== 0) return null;
	const params = text.slice(paramsStart, i - 1);
	while (i < text.length && /\s/.test(text[i])) i += 1;
	if (text[i] !== "{") return null;
	let braceDepth = 0;
	const bodyStart = i;
	while (i < text.length) {
		const ch = text[i];
		if (ch === "{") braceDepth += 1;
		else if (ch === "}") {
			braceDepth -= 1;
			if (braceDepth === 0)
				return { params, body: text.slice(bodyStart, i + 1) };
		}
		i += 1;
	}
	return null;
}

function lineOf(text, idx) {
	return text.slice(0, idx).split("\n").length;
}

const violations = [];
const files = listTsFiles(srcRoot);

for (const file of files.sort()) {
	const rel = relative(projectRoot, file).replace(/\\/g, "/");
	const text = readFileSync(file, "utf8");
	const isEngineDir = rel.startsWith(ENGINE_DIR_REL);
	const isCanonicalWrapper = rel === WRAPPER_REL;

	// R1 — banned except-shape: a catch handler FOR UnscopedSecretError that
	// touches process env, anywhere except THE canonical wrapper (DEC-003/
	// DEC-009; 06 §3.2). TypeScript catches carry no type in the parameter
	// (unlike Python's `except UnscopedSecretError:`), so the association is
	// either a typed parameter OR an `instanceof UnscopedSecretError` guard in
	// the handler body — both shapes are policed.
	if (!isCanonicalWrapper) {
		CATCH_OPEN_RE.lastIndex = 0;
		let m;
		while ((m = CATCH_OPEN_RE.exec(text)) !== null) {
			const openParen = text.indexOf("(", m.index);
			const clause =
				openParen === -1 ? null : extractCatchClause(text, openParen);
			CATCH_OPEN_RE.lastIndex = m.index + 5; // keep scanning past this clause
			if (clause === null) continue;
			const targetsUnscoped =
				/\bUnscopedSecretError\b/.test(clause.params) ||
				/\bUnscopedSecretError\b/.test(clause.body);
			if (!targetsUnscoped) continue;
			if (PROCESS_ENV_RE.test(clause.body)) {
				violations.push({
					rule: "UNSCOPED_CATCH_FALLBACK",
					file: rel,
					line: lineOf(text, m.index),
					detail:
						"catch(UnscopedSecretError) falls back to process env outside the canonical wrapper " +
						`(sanctioned copy lives ONLY at ${WRAPPER_REL}; DEC-009)`,
				});
			}
		}
	}

	// R2 — the literal after-a-scoped-miss fallback: a scoped read coalesced
	// straight into process env. Applies everywhere, engine included.
	const coalesce = SCOPED_READ_RE.exec(text);
	if (coalesce !== null) {
		violations.push({
			rule: "COALESCED_SCOPED_MISS_FALLBACK",
			file: rel,
			line: lineOf(text, coalesce.index),
			detail:
				"scoped read coalesced into process env (`scopedRead(...) ?? process.env…`) — " +
				"a scoped miss must return the declared default, never fall through (#86905; DEC-003)",
		});
	}

	// R3 — raw env reads beside scope resolution: once a file participates in
	// secret resolution, EVERY env read routes through the engine (which owns
	// the carve-out/overlay branches). The engine directory itself implements
	// those branches and is exempt; the canonical wrapper sits inside it.
	if (!isEngineDir && SCOPE_IDENTIFIER_RE.test(text)) {
		const re = new RegExp(PROCESS_ENV_RE.source, "g");
		let p;
		while ((p = re.exec(text)) !== null) {
			violations.push({
				rule: "RAW_ENV_BESIDE_SCOPE",
				file: rel,
				line: lineOf(text, p.index),
				detail:
					"direct process-env read in a file that references the secret scope engine — " +
					"route ALL env reads through the engine (06 §11 checklist row 1)",
			});
		}
	}
}

if (violations.length > 0) {
	console.error(`secret-scope violations (${violations.length}):`);
	for (const v of violations) {
		console.error(`  ${v.rule} ${v.file}:${v.line} — ${v.detail}`);
	}
	process.exit(1);
}
console.log(
	`secret-scope OK (no forbidden fallback shapes across ${files.length} files under src/, root=${projectRoot})`,
);
