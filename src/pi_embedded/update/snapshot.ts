// snapshot.ts — pre-update quick-snapshot stage (08 §6, FILE-LOSS RECOVERY,
// NOT code-rollback insurance).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/backup.py:create_pre_update_snapshots_all_profiles
//       → createPreUpdateSnapshotsAllProfiles — same snapshot set, same cap,
//         same keep policy for EVERY sibling profile (#66140); each lands
//         under its OWN <home>/state-snapshots/ so per-profile restore
//         tooling finds it where it expects.
//   hermes_cli/backup.py:_prune_quick_snapshots / update_cmd keep floor→1
//       → pruneSnapshotDirs — pre-update keep policy floors to 1.
//   hermes_cli/update_cmd.py:_PRE_UPDATE_SNAPSHOT_MAX_FILE_SIZE (= 1 GiB)
//       → PRE_UPDATE_SNAPSHOT_MAX_FILE_SIZE — oversized files skipped WITH
//         warning; pruning suppressed when a protected file was skipped
//         (incompleteness must never evict the last complete snapshot).
//   hermes_cli/backup.py:_safe_copy_db
//       → safeCopyDb — SQLite copies go through zeroed-file detection +
//         post-copy verification, never raw copies.
//
// Never raises: the pipeline records skips WITH reasons and continues
// ("snapshot failure ⇒ loud skip-with-reason, never silent", 08 §8).

import {
	copyFileSync,
	existsSync,
	mkdirSync,
	openSync,
	readSync,
	closeSync,
	readdirSync,
	rmSync,
	statSync,
	writeFileSync,
} from "node:fs";
import { dirname, join } from "node:path";
import Database from "better-sqlite3";

/** Per-file cap parity: 1 GiB (update_cmd.py:_PRE_UPDATE_SNAPSHOT_MAX_FILE_SIZE). */
export const PRE_UPDATE_SNAPSHOT_MAX_FILE_SIZE = 1 << 30;

/** Pre-update keep policy = 1 (manual quick snapshots default 20 — different knob). */
export const PRE_UPDATE_KEEP = 1;

export const SNAPSHOTS_DIRNAME = "state-snapshots";
export const PRE_UPDATE_LABEL = "pre-update";

/**
 * THE critical-file set — identical for every profile (08 §6: "an identical
 * critical-file set for EVERY profile ... never add partial/tiered snapshot
 * sets"). Pi home layout parity of backup.py:_QUICK_STATE_FILES: small,
 * hard-to-regenerate state (config, secrets store, cron jobs, runtime
 * status), NEVER multi-GB payloads — those are capped below.
 */
export const QUICK_STATE_FILES: readonly string[] = [
	"state.db",
	"config.yaml",
	".env",
	"cron/jobs.json",
	"gateway_state.json",
];

/** Files that go through the SQLite safe-copy path (backup.py:_safe_copy_db). */
const SQLITE_SUFFIXES = [".db", ".sqlite", ".sqlite3"];

export interface SnapshotSkip {
	path: string;
	reason: string;
}

export interface ProfileSnapshotResult {
	profile: string;
	home: string;
	ok: boolean;
	snapshotId: string | null;
	dir: string | null;
	copied: number;
	/** Oversized protected skips — these SUPPRESS pruning downstream. */
	skippedOversized: string[];
	/** Every skip WITH its reason (receipt payload, 08 §8). */
	skips: SnapshotSkip[];
	error: string | null;
}

export interface SnapshotStageResult {
	perProfile: ProfileSnapshotResult[];
	/** True when any skip suppressed the keep-prune this run. */
	pruningSuppressed: boolean;
	idsByProfile: Record<string, string>;
}

function isSqlitePath(path: string): boolean {
	return SQLITE_SUFFIXES.some((suffix) => path.endsWith(suffix));
}

/**
 * Zeroed-file detection + post-copy verification wrapper (parity of
 * backup.py:_safe_copy_db). A zeroed source (observed corruption class) is
 * refused up front; the copy is verified by reopening it read-only and
 * running PRAGMA quick_check. Copies -wal/-shm siblings so a live writer's
 * frames survive. Returns the refusal reason instead of throwing.
 */
export function safeCopyDb(
	src: string,
	dest: string,
): { ok: true } | { ok: false; reason: string } {
	try {
		mkdirSync(dirname(dest), { recursive: true });
		const header = Buffer.alloc(16);
		const fd = openSync(src, "r");
		try {
			readSync(fd, header, 0, 16, 0);
		} finally {
			closeSync(fd);
		}
		if (header.every((byte) => byte === 0)) {
			return {
				ok: false,
				reason: "database file is zeroed (corruption guard)",
			};
		}
		for (const suffix of ["", "-wal", "-shm"]) {
			const srcSibling = `${src}${suffix}`;
			if (!existsSync(srcSibling)) continue;
			copyFileSync(srcSibling, `${dest}${suffix}`);
		}
		// Post-copy verification: the copy must open and pass quick_check.
		const verify = new Database(dest, { readonly: true });
		try {
			const verdict = verify.pragma("quick_check", { simple: true });
			if (verdict !== "ok") {
				return {
					ok: false,
					reason: `post-copy quick_check reported ${String(verdict)}`,
				};
			}
		} finally {
			verify.close();
		}
		return { ok: true };
	} catch (error) {
		return {
			ok: false,
			reason: `safe copy failed: ${error instanceof Error ? error.message : String(error)}`,
		};
	}
}

function snapshotDirName(clockSeconds: number, label: string): string {
	const iso = new Date(clockSeconds * 1000).toISOString();
	// Lexicographic order == chronological order (prune relies on this).
	return `${iso.replace(/[:.]/g, "-")}-${label}`;
}

/**
 * Snapshot ONE profile home: identical critical set, per-file cap, best-
 * effort with recorded skips. Parity of backup.py:create_quick_snapshot
 * invoked with the pre-update label.
 */
export function snapshotProfileHome(options: {
	profile: string;
	home: string;
	clockSeconds: number;
	label?: string;
	maxFileSize?: number;
	fileSet?: readonly string[];
}): ProfileSnapshotResult {
	const maxFileSize =
		options.maxFileSize === undefined
			? PRE_UPDATE_SNAPSHOT_MAX_FILE_SIZE
			: options.maxFileSize;
	const fileSet =
		options.fileSet === undefined ? QUICK_STATE_FILES : options.fileSet;
	const label = options.label ?? PRE_UPDATE_LABEL;
	const { profile, home, clockSeconds } = options;
	const result: ProfileSnapshotResult = {
		profile,
		home,
		ok: false,
		snapshotId: null,
		dir: null,
		copied: 0,
		skippedOversized: [],
		skips: [],
		error: null,
	};
	try {
		const name = snapshotDirName(clockSeconds, label);
		const dir = join(home, SNAPSHOTS_DIRNAME, name);
		mkdirSync(dir, { recursive: true });
		result.dir = dir;
		result.snapshotId = name;
		let protectedSkips = 0;
		for (const rel of fileSet) {
			const src = join(home, rel);
			if (!existsSync(src)) continue; // fresh-home absence is normal
			let stats;
			try {
				stats = statSync(src);
			} catch (error) {
				result.skips.push({
					path: rel,
					reason: `stat failed: ${error instanceof Error ? error.message : String(error)}`,
				});
				continue;
			}
			if (!stats.isFile()) continue;
			if (stats.size > maxFileSize) {
				// Oversized ⇒ skipped WITH warning (update_cmd.py anchor comment:
				// the snapshot protects small hard-to-regenerate state, never a
				// multi-GB state.db).
				result.skips.push({
					path: rel,
					reason: `file exceeds the ${maxFileSize}-byte snapshot cap (${stats.size} bytes)`,
				});
				result.skippedOversized.push(rel);
				if (isSqlitePath(rel)) protectedSkips += 1;
				continue;
			}
			const dest = join(dir, rel);
			mkdirSync(dirname(dest), { recursive: true });
			if (isSqlitePath(rel)) {
				const copied = safeCopyDb(src, dest);
				if (!copied.ok) {
					result.skips.push({ path: rel, reason: copied.reason });
					protectedSkips += 1;
					continue;
				}
			} else {
				try {
					copyFileSync(src, dest);
				} catch (error) {
					result.skips.push({
						path: rel,
						reason: `copy failed: ${error instanceof Error ? error.message : String(error)}`,
					});
					continue;
				}
			}
			result.copied += 1;
		}
		result.ok = true;
		if (protectedSkips > 0) {
			// Incompleteness marker: restore tooling can distinguish a partial
			// snapshot from a complete one before trusting it.
			writeIncompleteMarker(dir);
		}
		return result;
	} catch (error) {
		result.ok = false;
		result.error = error instanceof Error ? error.message : String(error);
		return result;
	}
}

const INCOMPLETE_MARKER = ".incomplete";

function writeIncompleteMarker(dir: string): void {
	try {
		writeFileSync(join(dir, INCOMPLETE_MARKER), "protected skips present\n");
	} catch {
		/* best-effort */
	}
}

/**
 * Remove oldest pre-update snapshot dirs beyond `keep`, floored to 1
 * (parity: "_prune_pre_update_backups floors keep to 1 because this helper
 * is only called immediately after a fresh backup is written"). Only touches
 * `*-<label>` dirs; hand-made directories are never touched. Returns count
 * deleted.
 */
export function pruneSnapshotDirs(
	root: string,
	keep: number,
	label = PRE_UPDATE_LABEL,
): number {
	const effectiveKeep = Math.max(keep, 1);
	let deleted = 0;
	let entries: string[];
	try {
		entries = readdirSync(root);
	} catch {
		return 0;
	}
	const ours = entries
		.filter((name) => !name.startsWith(".") && name.endsWith(`-${label}`))
		.sort()
		.reverse();
	for (const name of ours.slice(effectiveKeep)) {
		try {
			rmSync(join(root, name), { recursive: true, force: true });
			deleted += 1;
		} catch {
			// Unlink failure is caller-log territory; never raise.
		}
	}
	return deleted;
}

/**
 * Pre-update quick snapshots for EVERY sibling profile (#66140 parity):
 * same set, same cap, same policy per profile; pruning runs at keep=1 ONLY
 * when no file was skipped anywhere (suppression rule). Never raises.
 */
export function createPreUpdateSnapshotsAllProfiles(options: {
	profiles: ReadonlyArray<{ profile: string; home: string }>;
	clockSeconds: number;
	keep?: number | null;
	maxFileSize?: number;
}): SnapshotStageResult {
	const perProfile = options.profiles.map((entry) =>
		snapshotProfileHome({
			profile: entry.profile,
			home: entry.home,
			clockSeconds: options.clockSeconds,
			maxFileSize: options.maxFileSize ?? PRE_UPDATE_SNAPSHOT_MAX_FILE_SIZE,
		}),
	);
	const pruningSuppressed = perProfile.some(
		(entry) => entry.skippedOversized.length > 0 || entry.skips.length > 0,
	);
	if (!pruningSuppressed && options.keep !== null) {
		const keep = options.keep === undefined ? PRE_UPDATE_KEEP : options.keep;
		for (const entry of perProfile) {
			if (!entry.ok) continue;
			pruneSnapshotDirs(join(entry.home, SNAPSHOTS_DIRNAME), keep);
		}
	}
	const idsByProfile: Record<string, string> = {};
	for (const entry of perProfile) {
		if (entry.ok && entry.snapshotId !== null) {
			idsByProfile[entry.profile] = entry.snapshotId;
		}
	}
	return { perProfile, pruningSuppressed, idsByProfile };
}
