// pi_gateway/lifecycle/boot-fingerprint.ts — boot code fingerprint + skew
// detection.
//
// Spec: /root/pi-gateway/08-operations.md §1.1 stage 5 ("record boot code
// fingerprint while sys.modules still matches disk — later `git pull` under
// this long-lived process becomes DETECTABLE instead of crash-on-stale");
// 01-architecture.md §3.3. Hermes anchors (READ-ONLY reference; semantics
// ported, no code vendored):
//   gateway/code_skew.py:record_boot_fingerprint → recordBootFingerprint (memoized once)
//   gateway/code_skew.py:_fingerprint            → defaultGitFingerprintReader (`git:<ref>:<sha>`)
//   gateway/code_skew.py:detect_code_skew        → detectCodeSkew
//   gateway/code_skew.py:_short                  → shortFingerprintLabel
//
// If the revision can't be read (non-git install, IO error, no git binary),
// the boot snapshot stays null and skew detection NO-OPS — never a false
// positive. The recorded fingerprint is PERSISTED at stage 10 by stamping
// code_sha into gateway_state.json (08 §4), which is what makes the update
// pipeline's verify stage provable instead of assumed.

import { execFile } from "node:child_process";
import { existsSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

export type FingerprintReader = () => Promise<string | null>;

const THIS_DIR = dirname(fileURLToPath(import.meta.url));

/**
 * Walk upward to the checkout root (the .git boundary). Hermes fingerprints
 * ITS project root (code_skew.py:_PROJECT_ROOT = repo root); ours is the
 * enclosing checkout whether that is pi-gateway/ or a parent workspace.
 */
export function findProjectRoot(startDir = THIS_DIR): string {
	let current = startDir;
	for (;;) {
		if (existsSync(join(current, ".git"))) return current;
		const parent = dirname(current);
		if (parent === current) return startDir; // no .git anywhere — non-git install
		current = parent;
	}
}

function execFileText(
	cmd: string,
	args: string[],
	cwd: string,
): Promise<string> {
	return new Promise((resolvePromise, rejectPromise) => {
		execFile(cmd, args, { cwd, timeout: 5000 }, (err, stdout) => {
			if (err) rejectPromise(err);
			else resolvePromise(String(stdout).trim());
		});
	});
}

/**
 * Worktree-aware-ish reader producing `git:<ref>:<sha>` (parity of the CLI's
 * _read_git_revision_fingerprint shape). Any failure ⇒ null — never raises.
 */
export function defaultGitFingerprintReader(
	projectRoot?: string,
): FingerprintReader {
	return async () => {
		const root = projectRoot ?? findProjectRoot();
		try {
			const ref = await execFileText(
				"git",
				["rev-parse", "--abbrev-ref", "HEAD"],
				root,
			);
			const sha = await execFileText("git", ["rev-parse", "HEAD"], root);
			if (!sha) return null;
			return `git:${ref || "detached"}:${sha}`;
		} catch {
			return null;
		}
	};
}

/** `undefined` = not yet recorded; `null` = recorded as unavailable. */
let bootFingerprint: string | null | undefined;

/**
 * Snapshot the checkout revision at gateway startup (idempotent memoization
 * parity of record_boot_fingerprint). MUST run early — while imports still
 * match disk (08 §1.1 stage 3 ordering).
 */
export async function recordBootFingerprint(
	reader: FingerprintReader = defaultGitFingerprintReader(),
): Promise<string | null> {
	if (bootFingerprint === undefined) {
		bootFingerprint = await reader();
	}
	return bootFingerprint;
}

/** Raw boot value for status stamping (null when unavailable). */
export function bootFingerprintValue(): string | null {
	return bootFingerprint === undefined ? null : bootFingerprint;
}

/** Compact `git:<ref>:<sha>` rendering (code_skew.py:_short parity). */
export function shortFingerprintLabel(fingerprint: string): string {
	const sha = fingerprint.split(":").pop() ?? "";
	if (sha && sha !== "unresolved" && sha.length > 10) return sha.slice(0, 10);
	return sha || fingerprint;
}

/**
 * Return {boot, disk} short labels when the checkout drifted since boot,
 * else null. Null boot snapshot ⇒ always null (never a false positive).
 */
export async function detectCodeSkew(
	reader: FingerprintReader = defaultGitFingerprintReader(),
): Promise<{ boot: string; disk: string } | null> {
	if (bootFingerprint === undefined || bootFingerprint === null) return null;
	const current = await reader();
	if (current === null || current === bootFingerprint) return null;
	return {
		boot: shortFingerprintLabel(bootFingerprint),
		disk: shortFingerprintLabel(current),
	};
}

/** Test hook: forget the recorded boot snapshot (code_skew has module state too). */
export function resetBootFingerprintForTests(): void {
	bootFingerprint = undefined;
}
