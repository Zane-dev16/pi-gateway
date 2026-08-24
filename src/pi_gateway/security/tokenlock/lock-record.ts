// tokenlock/lock-record — the on-disk scoped-lock record (06 §5).
//
// Port of gateway/status.py lock plumbing (READ-ONLY Hermes reference;
// semantics ported, no code vendored):
//   status.py:_scope_hash           → hashIdentity        (sha256[:16]; the RAW
//                                     credential is NEVER written to disk)
//   status.py:_get_scope_lock_path  → scopedLockPath      ({scope}-{hash16}.lock)
//   status.py:_get_lock_dir         → defaultLockDir      (machine-local dir —
//                                     deliberately NOT per-profile home: the
//                                     whole point is cross-profile contention)
//   status.py:_build_pid_record     → buildLockRecord     (pid + kind + argv +
//                                     start_time PID-reuse guard)
//   OOF-3 profile label             → record.profile      (operators must tell
//                                     WHICH profile holds the credential)
//
// Write discipline (tightened, observable behavior identical):
// - Creation publishes a FULLY-WRITTEN file via write-temp + link(2). link()
//   fails with EEXIST when the lock already exists — atomic O_EXCL semantics
//   over COMPLETE content. Hermes noted the crash window between its
//   O_CREAT|O_EXCL create and json.dump() (status.py acquire_scoped_lock,
//   empty/invalid-JSON comment); readers here can never observe a torn record
//   and a crashed creator leaves nothing behind to clean up.
// - Updates (same-holder self-reacquire refresh / explicit replace steal)
//   publish via write-temp + rename(2) — atomic replacement, no clobbered
//   partial states.

import { createHash } from "node:crypto";
import {
	closeSync,
	constants,
	openSync,
	writeSync,
	existsSync,
	linkSync,
	mkdirSync,
	readFileSync,
	renameSync,
	unlinkSync,
} from "node:fs";
import { homedir } from "node:os";
import { join } from "node:path";
import { getProcessStartTime, readProcessCmdline } from "./process-identity.js";

/** Machine-global marker stamped into every record (status.py `kind` parity). */
export const TOKEN_LOCK_KIND = "pi-gateway-scoped-lock";

/** The JSON record persisted at `{dir}/{scope}-{identity_hash}.lock`. */
export interface ScopedLockRecord {
	/** Holder OS pid. */
	pid: number;
	kind: string;
	/** Holder argv at acquisition (identity oracle when cmdline is unreadable). */
	argv: string[];
	/**
	 * PID-reuse fingerprint (/proc stat field 22 clock ticks) captured for the
	 * holder AT WRITE TIME; null only when the platform cannot produce one.
	 */
	start_time: number | null;
	scope: string;
	/** sha256(identity)[:16] — raw credential NEVER on disk (06 §5). */
	identity_hash: string;
	metadata: Record<string, unknown>;
	/** RFC3339 UTC stamp of the last write (refreshes on self-reacquire). */
	updated_at: string;
	/** Injected-clock ms when the CURRENT hold began (seam heldSinceMs). */
	held_since_ms: number;
	/** Logical owner id within the process (instance id); see DEC-035 note. */
	owner: string;
	/** Optional human-readable profile label (OOF-3 cross-profile diagnosis). */
	profile?: string | undefined;
}

/** Resolve the machine-local lock directory (env-overridable for operators). */
export function defaultLockDir(env: NodeJS.ProcessEnv = process.env): string {
	const override = env["PI_GATEWAY_LOCK_DIR"];
	if (override !== undefined && override.trim() !== "") return override;
	const stateHome =
		env["XDG_STATE_HOME"] !== undefined && env["XDG_STATE_HOME"].trim() !== ""
			? env["XDG_STATE_HOME"]
			: join(homedir(), ".local", "state");
	return join(stateHome, "pi-gateway", "token-locks");
}





export function hashIdentity(identity: string): string {
	return createHash("sha256")
		.update(identity, "utf8")
		.digest("hex")
		.slice(0, 16);
}

export function scopedLockPath(
	dir: string,
	scope: string,
	identity: string,
): string {
	return join(dir, `${scope}-${hashIdentity(identity)}.lock`);
}

export function buildLockRecord(fields: {
	scope: string;
	identity: string;
	owner: string;
	metadata?: Record<string, unknown> | undefined;
	profileLabel?: string | undefined;
	nowMs: number;
}): ScopedLockRecord {
	const nowIso = new Date(fields.nowMs).toISOString();
	const record: ScopedLockRecord = {
		pid: process.pid,
		kind: TOKEN_LOCK_KIND,
		argv: [...process.argv],
		start_time: getProcessStartTime(process.pid),
		scope: fields.scope,
		identity_hash: hashIdentity(fields.identity),
		metadata: fields.metadata ?? {},
		updated_at: nowIso,
		held_since_ms: fields.nowMs,
		owner: fields.owner,
	};
	if (fields.profileLabel !== undefined) record.profile = fields.profileLabel;
	return record;
}

/** Refresh the mutable stamps of an existing record for self-reacquire. */
export function refreshLockRecord(
	record: ScopedLockRecord,
	nowMs: number,
): ScopedLockRecord {
	return {
		...record,
		pid: process.pid,
		argv: [...process.argv],
		start_time: getProcessStartTime(process.pid),
		updated_at: new Date(nowMs).toISOString(),
		held_since_ms: nowMs,
	};
}

export type LockReadResult =
	| { found: true; record: ScopedLockRecord }
	| { found: false; reason: "missing" | "corrupt" };

export function readLockRecord(path: string): LockReadResult {
	let raw: string;
	try {
		raw = readFileSync(path, "utf8");
	} catch {
		return { found: false, reason: "missing" };
	}
	try {
		const parsed = JSON.parse(raw) as unknown;
		if (
			parsed !== null &&
			typeof parsed === "object" &&
			typeof (parsed as ScopedLockRecord).pid === "number"
		) {
			return { found: true, record: parsed as ScopedLockRecord };
		}
		return { found: false, reason: "corrupt" };
	} catch {
		return { found: false, reason: "corrupt" };
	}
}

function serialize(record: ScopedLockRecord): string {
	return `${JSON.stringify(record)}\n`;
}

function writeTempFile(dir: string, payload: string): string {
	const tmp = join(
		dir,
		`.tmp-${process.pid}-${Math.random().toString(36).slice(2)}-${Date.now()}`,
	);
	const fd = openSync(
		tmp,
		constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY,
		0o600,
	);
	try {
		writeSync(fd, payload);
	} finally {
		closeSync(fd);
	}
	return tmp;
}

/**
 * Atomic CREATE: publish a fully-written record only if no lock exists.
 * Returns false when someone else holds/created it (EEXIST from link(2)).
 */
export function createLockFile(
	path: string,
	record: ScopedLockRecord,
): boolean {
	const dir = join(path, "..");
	mkdirSync(dir, { recursive: true });
	const tmp = writeTempFile(dir, serialize(record));
	try {
		try {
			linkSync(tmp, path);
			return true;
		} catch (err) {
			if ((err as NodeJS.ErrnoException).code === "EEXIST") return false;
			throw err;
		}
	} finally {
		try {
			unlinkSync(tmp);
		} catch {
			/* best-effort temp cleanup */
		}
	}
}

/** Atomic REPLACE of an existing record (self-reacquire refresh / replace). */
export function replaceLockFile(path: string, record: ScopedLockRecord): void {
	const dir = join(path, "..");
	mkdirSync(dir, { recursive: true });
	const tmp = writeTempFile(dir, serialize(record));
	try {
		renameSync(tmp, path);
	} catch (err) {
		try {
			unlinkSync(tmp);
		} catch {
			/* nothing more we can do */
		}
		throw err;
	}
}

/** Remove the lock file if it still exists (release / corrupt cleanup). */
export function removeLockFile(path: string): void {
	try {
		unlinkSync(path);
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code !== "ENOENT") throw err;
	}
}

/** True when the path exists but yields no readable record (crash debris or
 * hand corruption) — the stale ladder's degenerate first rung. */
export function isCorruptLockFile(path: string): boolean {
	if (!existsSync(path)) return false;
	const read = readLockRecord(path);
	return !read.found && read.reason === "corrupt";
}

/** Identity oracle fallback: does the RECORD itself look like one of ours? */
export function recordLooksLikeTokenLockHolder(
	record: ScopedLockRecord,
): boolean {
	if (record.kind !== TOKEN_LOCK_KIND) return false;
	return Array.isArray(record.argv) && record.argv.length > 0;
}

/** Live-process identity oracle: does the holder's cmdline still name our argv[0]? */
export function liveCmdlineMatchesHolder(
	pid: number,
	record: ScopedLockRecord,
): boolean {
	const cmdline = readProcessCmdline(pid);
	if (cmdline === null || cmdline === "") return false; // unreadable ⇒ no verdict
	const argv0 = Array.isArray(record.argv) ? record.argv[0] : undefined;
	if (typeof argv0 === "string" && argv0 !== "" && cmdline.includes(argv0)) {
		return true;
	}
	return cmdline.includes("pi") && cmdline.includes("gateway");
}
