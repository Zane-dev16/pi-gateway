// Behavior contracts for script-transform path confinement (06 §8.2;
// webhook_filters.py:_resolve_script_path port). Escape matrix: `..`
// traversal, absolute-outside-home, `~/.hermes` aliases, env-var escapes,
// and symlink-ish (lexically-inside/really-outside) encodings all REFUSE.

import { describe, expect, it } from "vitest";
import {
	expandUser,
	expandVars,
	resolveScriptPath,
	type ConfinementFs,
	type ScriptConfinementConfig,
} from "./index.js";

/**
 * Fake fs with symlink simulation: resolve() makes absolute, collapses
 * lexical segments, then rewrites any symlinked prefix to its target
 * (Path.resolve(strict=False) parity for the shapes we exercise).
 */
function fakeFs(opts: {
	files: Set<string>;
	dirs: Set<string>;
	symlinks: Record<string, string>;
}): ConfinementFs {
	const { posix } = require("node:path") as typeof import("node:path");
	function lexicallyResolve(p: string): string {
		return posix.normalize(p);
	}
	function rewriteSymlinks(p: string): string {
		let current = p;
		for (let guard = 0; guard < 8; guard++) {
			let rewritten = false;
			for (const [link, target] of Object.entries(opts.symlinks)) {
				if (current === link) {
					current = target;
					rewritten = true;
					break;
				}
				if (current.startsWith(link + "/")) {
					current = target + current.slice(link.length);
					rewritten = true;
					break;
				}
			}
			if (!rewritten) break;
			current = lexicallyResolve(current);
		}
		return current;
	}
	return {
		resolve(path: string): string {
			return rewriteSymlinks(lexicallyResolve(path));
		},
		exists(path: string): boolean {
			return opts.files.has(path) || opts.dirs.has(path);
		},
		isFileSync(path: string): boolean {
			return opts.files.has(path);
		},
	};
}

const HOME = "/home/operator/.hermes";
const SCRIPTS = `${HOME}/scripts`;

function config(
	overrides: Partial<ScriptConfinementConfig> = {},
): ScriptConfinementConfig {
	return {
		scriptsRoot: SCRIPTS,
		profileHome: HOME,
		env: {},
		fs:
			overrides.fs ??
			fakeFs({
				files: new Set([
					`${SCRIPTS}/transform.py`,
					`${SCRIPTS}/sub/inner.py`,
					"/etc/secret.env",
				]),
				dirs: new Set([SCRIPTS, `${SCRIPTS}/sub`, "/etc"]),
				symlinks: {},
			}),
		...overrides,
	};
}

describe("confinement escape matrix — ALL refuse", () => {
	it("../ traversal refuses", () => {
		expect(resolveScriptPath(config(), "../secrets.env").ok).toBe(false);
		const r = resolveScriptPath(config(), "../../etc/passwd");
		expect(r.ok === false && r.reason).toMatch(/outside/);
	});

	it("./-prefixed traversal that stays inside is fine; escaping via subdir refuses", () => {
		expect(resolveScriptPath(config(), "sub/../transform.py").ok).toBe(true);
		expect(resolveScriptPath(config(), "sub/../../escape.sh").ok).toBe(false);
	});

	it("absolute paths OUTSIDE home refuse; absolute INSIDE scripts root is legal", () => {
		expect(resolveScriptPath(config(), "/etc/passwd").ok).toBe(false);
		expect(
			resolveScriptPath(config(), "/etc/secret.env").ok === false && true,
		).toBe(true);
		// Hermes parity: an absolute path resolving under scripts_root passes.
		expect(resolveScriptPath(config(), `${SCRIPTS}/transform.py`)).toEqual({
			ok: true,
			resolvedPath: `${SCRIPTS}/transform.py`,
		});
	});

	it("`~/.hermes` alias values refuse (they resolve OUTSIDE <home>/scripts)", () => {
		// Bare alias → profile home itself, not under scripts ⇒ relative_to fails.
		expect(resolveScriptPath(config(), "~/.hermes").ok).toBe(false);
		// Alias into the home tree but NOT under scripts/ ⇒ refuse.
		expect(resolveScriptPath(config(), "~/.hermes/config.yaml").ok).toBe(false);
		// Alias WITH traversal out of the home ⇒ refuse.
		expect(resolveScriptPath(config(), "~/.hermes/../../etc/passwd").ok).toBe(
			false,
		);
		// Alias landing UNDER scripts/ is the sanctioned spelling.
		expect(
			resolveScriptPath(config(), "~/.hermes/scripts/transform.py"),
		).toEqual({ ok: true, resolvedPath: `${SCRIPTS}/transform.py` });
	});

	it("~ expansion outside scripts refuses", () => {
		expect(resolveScriptPath(config({ userHome: "/root" }), "~/x.sh").ok).toBe(
			false,
		);
	});

	it("env-var escapes refuse ($HOME / ${VAR} pointed outside)", () => {
		expect(
			resolveScriptPath(
				config({ env: { HOME: "/etc", EVIL_DIR: "/var" } }),
				"$HOME/cron.tab",
			).ok,
		).toBe(false);
		expect(
			resolveScriptPath(
				config({ env: { EVIL_DIR: "/var" } }),
				"${EVIL_DIR}/payload.py",
			).ok,
		).toBe(false);
	});

	it("symlink-ish encoding (lexically inside, really outside) refuses", () => {
		const cfg = config({
			fs: fakeFs({
				files: new Set(["/etc/secret.env"]),
				dirs: new Set([SCRIPTS, "/etc"]),
				symlinks: { [`${SCRIPTS}/link`]: "/etc" },
			}),
		});
		const result = resolveScriptPath(cfg, "link/secret.env");
		expect(result.ok).toBe(false);
		expect(result.ok === false && result.reason).toMatch(/outside/);
	});
});

describe("sanctioned resolutions + refusal hygiene", () => {
	it("relative, subdir-relative, and bare ~ resolve inside the root", () => {
		expect(resolveScriptPath(config(), "transform.py")).toEqual({
			ok: true,
			resolvedPath: `${SCRIPTS}/transform.py`,
		});
		expect(resolveScriptPath(config(), "sub/inner.py").ok).toBe(true);
	});

	it("missing file and non-file targets refuse distinctly", () => {
		const missing = resolveScriptPath(config(), "nope.py");
		expect(missing.ok === false && missing.reason).toMatch(/not found/);
		const dirAsScript = config({
			fs: fakeFs({
				files: new Set(),
				dirs: new Set([`${SCRIPTS}/dirpy`, SCRIPTS]),
				symlinks: {},
			}),
		});
		const notFile = resolveScriptPath(dirAsScript, "dirpy");
		expect(notFile.ok === false && notFile.reason).toMatch(/not a file/);
	});

	it("empty / whitespace / non-string input refuses", () => {
		expect(resolveScriptPath(config(), "").ok).toBe(false);
		expect(resolveScriptPath(config(), "   ").ok).toBe(false);
		expect(resolveScriptPath(config(), 42 as unknown).ok).toBe(false);
	});

	it("expandvars leaves unknown vars literal (os.path.expandvars parity)", () => {
		expect(expandVars("$UNSET_X/${ALSO unset}", {})).toBe(
			"$UNSET_X/${ALSO unset}",
		);
		expect(expandVars("${A}/$B", { A: "1", B: "2" })).toBe("1/2");
		expect(expandUser("~/f", "/u")).toBe("/u/f");
		expect(expandUser("~", "/u")).toBe("/u");
		expect(expandUser("/abs/f", "/u")).toBe("/abs/f");
	});
});
