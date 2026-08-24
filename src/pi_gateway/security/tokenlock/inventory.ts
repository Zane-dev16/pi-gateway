// tokenlock/inventory — the §5.1 lock inventory: WHO holds WHAT, since WHEN.
//
// Port of the Hermes scoped-lock observability surface (READ-ONLY reference;
// semantics ported, no code vendored):
//   status.py:release_all_scoped_locks glob  → listScopedLocks (*.lock scan)
//   status.py:scoped_lock_owner_label        → scopedLockOwnerLabel (validated
//                                              profile label; OOF-3)
//   acquire_scoped_lock record               → inventory rows (scope, identity
//                                              HASH, pid, start_time, stamps)
//
// Hygiene invariants: raw credentials NEVER appear (records carry only
// sha256[:16] identity hashes); liveness is reported per row so operators can
// distinguish a live holder from crash debris at a glance.

import { readdirSync, readFileSync } from "node:fs";
import { basename, join } from "node:path";
import { isCorruptLockFile, type ScopedLockRecord } from "./lock-record.js";
import {
	getProcessStartTime,
	isProcessAlive,
	isProcessStopped,
} from "./process-identity.js";
import { classifyExistingRecord } from "./token-lock.js";

export interface ScopedLockInventoryRow {
	/** Lock file name ({scope}-{identity_hash}.lock). */
	file: string;
	scope: string;
	/** sha256(identity)[:16] — never the raw credential. */
	identityHash: string;
	/** Validated profile label when stamped; null otherwise. */
	profile: string | null;
	pid: number | null;
	owner: string;
	/** PID-reuse fingerprint of the recorded holder. */
	startTime: number | null;
	/** Injected-clock ms when the current hold began. */
	heldSinceMs: number;
	updated_at: string | null;
	metadata: Record<string, unknown>;
	alive: boolean;
	stopped: boolean;
	/** True when THIS process would win the key on its next acquire. */
	reclaimableByUs: boolean;
}

export interface InventoryOptions {
	dir?: string | undefined;
	/** Logical owner id used for the reclaimable verdict (default "inventory"). */
	observerOwner?: string | undefined;
}

function parseScopeFromFilename(file: string): string {
	// {scope}-{16 hex}.lock — scope ids never contain '-' followed by exactly
	// 16 hex chars + ".lock", so split on the LAST such boundary.
	const name = basename(file);
	const m = /^(.*)-([0-9a-f]{16})\.lock$/.exec(name);
	return m?.[1] ?? name;
}

/**
 * Enumerate every lock record in the machine-local lock dir with liveness and
 * staleness verdicts per row (06 §5.1 inventory).
 */
export function listScopedLocks(
	options: InventoryOptions = {},
): ScopedLockInventoryRow[] {
	const dir = options.dir ?? process.env["PI_GATEWAY_LOCK_DIR"] ?? "";
	let files: string[] = [];
	try {
		files = readdirSync(dir).filter((f) => f.endsWith(".lock"));
	} catch {
		return [];
	}
	const observer = options.observerOwner ?? "__inventory__";
	const rows: ScopedLockInventoryRow[] = [];
	for (const file of files.sort()) {
		const path = join(dir, file);
		if (isCorruptLockFile(path)) {
			rows.push({
				file,
				scope: parseScopeFromFilename(file),
				identityHash: /-([0-9a-f]{16})\.lock$/.exec(file)?.[1] ?? "",
				profile: null,
				pid: null,
				owner: "",
				startTime: null,
				heldSinceMs: Number.NaN,
				updated_at: null,
				metadata: {},
				alive: false,
				stopped: false,
				reclaimableByUs: true,
			});
			continue;
		}
		let record: ScopedLockRecord;
		try {
			record = JSON.parse(readFileSync(path, "utf8")) as ScopedLockRecord;
		} catch {
			continue;
		}
		const pid = typeof record.pid === "number" ? record.pid : null;
		const alive = pid !== null && isProcessAlive(pid);
		const stopped = pid !== null && isProcessStopped(pid);
		const startTime =
			typeof record.start_time === "number" ? record.start_time : null;
		const stale =
			classifyExistingRecord(record, { owner: observer, replace: false })
				.action === "stale";
		rows.push({
			file,
			scope:
				typeof record.scope === "string" && record.scope !== ""
					? record.scope
					: parseScopeFromFilename(file),
			identityHash:
				typeof record.identity_hash === "string" ? record.identity_hash : "",
			profile:
				typeof record.profile === "string" &&
				/^[A-Za-z0-9_.-]{1,64}$/.test(record.profile.trim())
					? record.profile.trim()
					: null,
			pid,
			owner: typeof record.owner === "string" ? record.owner : "",
			startTime,
			heldSinceMs:
				typeof record.held_since_ms === "number"
					? record.held_since_ms
					: Number.NaN,
			updated_at:
				typeof record.updated_at === "string" ? record.updated_at : null,
			metadata:
				record.metadata !== null && typeof record.metadata === "object"
					? (record.metadata as Record<string, unknown>)
					: {},
			alive,
			stopped,
			reclaimableByUs: stale,
		});
	}
	return rows;
}

/** Current start-time fingerprint for OUR pid — inventory/debug helper. */
export function ownProcessFingerprint(): number | null {
	return getProcessStartTime(process.pid);
}
