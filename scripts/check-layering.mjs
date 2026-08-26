#!/usr/bin/env node
// check-layering.mjs — downward-only dependency lint (roadmap exit criterion:
// "no module violates the downward-dependency rule (lint-level check)").
//
// Normative basis: /root/pi-gateway/01-architecture.md §5.3 (dependency graph
// of proposed Pi modules; "dependencies flow downward only; nothing below the
// runner may import it") and 10-implementation-roadmap.md §0 ("A phase's code
// may not reach upward past its own layer even when a shortcut would
// compile").
//
// Layer ranks (lower = lower in the stack; X may import only ranks <= X):
//   pi_home       0   zero deps (env + ctxvar resolution only)
//   pi_state      1   deps: pi_home, sqlite driver, node builtins
//   pi_agent_core 2   deps: pi_home (+pi_state)
//   pi_gateway    3   deps: pi_state, pi_agent_core (the sanctioned runner
//                     edge in the §5.3 diagram: pi_agent_core ◄── pi_gateway)
//   pi_platforms      4   deps: base.py/registry shapes; NEVER the runner
//   pi_embedded       4   deps: pi_state, pi_home; started BY the runner but
//                         never importing it
//   pi_server         5   client surfaces; the gateway NEVER imports these
//   entrypoints       6   'pi main / pi gateway run' composition roots — the
//                         ONE rank allowed to import every other layer and
//                         bind it into a running gateway (DEC-058; 01 §5.3
//                         top row: entrypoints sit ABOVE the whole graph)
//
// Hard failures:
//   UPWARD_IMPORT     importer reaches a strictly higher-ranked layer
//                     (covers: pi_state → pi_gateway/pi_agent_core;
//                     pi_agent_core → pi_gateway — i.e. the banned direction
//                     of any sibling cross-import; pi_home → anything).
//   RUNNER_INTERNAL   pi_platforms / pi_embedded import runner internals
//                     (pi_gateway/lifecycle/** or a future run/guards module) —
//                     adapters see the runner only through registries.
//   UNPLACED_LAYER    an import touches src/<dir>/ that is not a declared
//                     layer directory (unknown top-level module placement).
//
// Files outside the known layers are IGNORED (out of lint domain) so the gate
// stays stable while phases land new directories behind their own DECs.
// Root-level files under src/ map to layer `pi_home` when named pi_home*.ts.
//
// Usage: node scripts/check-layering.mjs [projectRoot]
// Exit codes: 0 = layering holds; 1 = violations found; 2 = usage error.

import { readdirSync, readFileSync, statSync } from "node:fs";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const SCRIPT_DIR = dirname(fileURLToPath(import.meta.url));
const DEFAULT_ROOT = resolve(SCRIPT_DIR, "..");

const rootArg = process.argv[2];
if (rootArg !== undefined && /^\s*$/.test(rootArg)) {
	console.error("usage: node scripts/check-layering.mjs [projectRoot]");
	process.exit(2);
}
const projectRoot = rootArg !== undefined ? resolve(rootArg) : DEFAULT_ROOT;
const srcRoot = join(projectRoot, "src");

const LAYER_RANK = {
	pi_home: 0,
	pi_state: 1,
	pi_agent_core: 2,
	pi_gateway: 3,
	pi_platforms: 4,
	pi_embedded: 4,
	pi_server: 5,
	entrypoints: 6,
};

/** Runner internals that platform/embedded code must never import. */
const RUNNER_INTERNAL_PREFIXES = ["pi_gateway/lifecycle", "pi_gateway/run"];

const IMPORT_PATTERNS = [
	/(?:^|\n)\s*import\s+(?:type\s+)?[^;'"]*?from\s*["']([^"']+)["']/g,
	/(?:^|\n)\s*export\s+(?:type\s+)?[^;'"]*?from\s*["']([^"']+)["']/g,
	/\bimport\s*\(\s*["']([^"']+)["']\s*\)/g,
	/(?:^|\n)\s*(?:import|export)\s+["']([^"']+)["']/g,
];

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
		else if (/\.(ts|mts|cts)$/.test(entry)) out.push(full);
	}
	return out;
}

/** Map a source file path under src/ to its layer name, or null if unplaced. */
function layerOf(relFromSrc) {
	const segments = relFromSrc.split(/[\\/]/);
	if (segments.length === 1) {
		// Root-level file: pi_home*.ts IS the pi_home layer; anything else is
		// an unplaced module (layers are directories per 01 §5.3).
		return /^pi_home\b/.test(segments[0]) ? "pi_home" : null;
	}
	const top = segments[0];
	return Object.hasOwn(LAYER_RANK, top) ? top : null;
}

/** Resolve a specifier to a repo-internal layer key ("pi_state" | null). */
function resolveImportLayer(fromFile, specifier) {
	if (!specifier.startsWith(".") && !specifier.startsWith("/"))
		return undefined; // external/bare/node:
	const base = specifier.endsWith(".js")
		? specifier.slice(0, -3)
		: specifier.endsWith(".mjs") || specifier.endsWith(".cjs")
			? specifier.slice(0, -4)
			: specifier;
	const abs = resolve(dirname(fromFile), base);
	for (const candidate of [`${abs}.ts`, `${abs}.tsx`, `${abs}.d.ts`, abs]) {
		const rel = relative(srcRoot, candidate);
		if (rel.startsWith("..")) continue; // outside src (tests/fixtures)
		return layerOf(rel);
	}
	return undefined; // resolves nowhere inside src — ignore (e.g. .js data)
}

const violations = [];

for (const file of listTsFiles(srcRoot)) {
	const relFile = relative(srcRoot, file);
	const importerLayer = layerOf(relFile);
	if (importerLayer === null) {
		violations.push({
			kind: "UNPLACED_LAYER",
			file: relFile,
			line: 1,
			detail:
				"source file lives outside every declared 01 §5.3 layer directory",
		});
	}
	const text = readFileSync(file, "utf8");
	const lines = text.split("\n");
	for (const pattern of IMPORT_PATTERNS) {
		pattern.lastIndex = 0;
		let match;
		while ((match = pattern.exec(text)) !== null) {
			const specifier = match[1];
			const lineNo = text.slice(0, match.index).split("\n").length;
			void lines;
			const targetLayer = resolveImportLayer(file, specifier);
			if (targetLayer === undefined || targetLayer === null) {
				if (targetLayer === null) {
					violations.push({
						kind: "UNPLACED_LAYER",
						file: relFile,
						line: lineNo,
						detail: `${specifier} lands outside every declared layer directory`,
					});
				}
				continue;
			}
			const fromRank = LAYER_RANK[importerLayer];
			const toRank = LAYER_RANK[targetLayer];
			if (fromRank !== undefined && toRank > fromRank) {
				violations.push({
					kind: "UPWARD_IMPORT",
					file: relFile,
					line: lineNo,
					detail: `${importerLayer} (rank ${fromRank}) imports ${targetLayer} (rank ${toRank}) via ${specifier} — dependencies flow DOWNWARD only (01 §5.3)`,
				});
			}
			if (
				(importerLayer === "pi_platforms" || importerLayer === "pi_embedded") &&
				RUNNER_INTERNAL_PREFIXES.some((p) =>
					resolve(dirname(file), specifier.replace(/\.js$/, ""))
						.replace(/\\/g, "/")
						.includes(p),
				)
			) {
				violations.push({
					kind: "RUNNER_INTERNAL",
					file: relFile,
					line: lineNo,
					detail: `${importerLayer} imports runner internals (${specifier}) — adapters/services never import the runner (01 §5.3)`,
				});
			}
		}
	}
}

if (violations.length > 0) {
	console.error(`layering violations (${violations.length}):`);
	for (const v of violations.sort((a, b) => a.file.localeCompare(b.file))) {
		console.error(`  ${v.kind} ${v.file}:${v.line} — ${v.detail}`);
	}
	process.exit(1);
}
console.log(
	`layering OK (downward-only holds across src/, root=${projectRoot})`,
);
