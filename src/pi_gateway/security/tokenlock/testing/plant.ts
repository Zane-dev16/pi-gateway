// TEST INFRASTRUCTURE — record-planting helpers for the token-lock contracts.
// Records are written through the PRODUCTION writer (createLockFile) so the
// on-disk format under test is the real serialization, never a parallel one.

import { createHash } from "node:crypto";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	createLockFile,
	scopedLockPath,
	TOKEN_LOCK_KIND,
	type ScopedLockRecord,
} from "../lock-record.js";

export function makeScratchDir(prefix: string): string {
	return mkdtempSync(join(tmpdir(), prefix));
}

export interface PlantOverrides {
	pid?: number | undefined;
	owner?: string | undefined;
	start_time?: number | null | undefined;
	scope?: string | undefined;
	held_since_ms?: number | undefined;
	profile?: string | undefined;
	metadata?: Record<string, unknown> | undefined;
}

/** Build a full record literal with test-controlled identity fields. */
export function makeRecord(
	identity: string,
	overrides: PlantOverrides = {},
): ScopedLockRecord {
	const now = overrides.held_since_ms ?? 1_700_000_000_000;
	const record: ScopedLockRecord = {
		pid: overrides.pid ?? process.pid,
		kind: TOKEN_LOCK_KIND,
		argv: [process.execPath, "planted-holder"],
		start_time:
			overrides.start_time === undefined ? 12345 : overrides.start_time,
		scope: overrides.scope ?? "test-scope",
		identity_hash: "", // filled by the writer path below via scopedLockPath parity
		metadata: overrides.metadata ?? {},
		updated_at: new Date(now).toISOString(),
		held_since_ms: now,
		owner: overrides.owner ?? "planted",
	};
	if (overrides.profile !== undefined) record.profile = overrides.profile;
	// Hash parity with the engine (sha256[:16] of the raw identity).
	record.identity_hash = hashOf(identity);
	return record;
}

function hashOf(identity: string): string {
	return createHash("sha256")
		.update(identity, "utf8")
		.digest("hex")
		.slice(0, 16);
}

/** Plant a record at its canonical lock path through the production writer. */
export function plantRecord(
	dir: string,
	scope: string,
	identity: string,
	record: ScopedLockRecord,
): string {
	const path = scopedLockPath(dir, scope, identity);
	const created = createLockFile(path, { ...record, scope });
	if (!created) throw new Error(`plant failed: lock already exists at ${path}`);
	return path;
}
