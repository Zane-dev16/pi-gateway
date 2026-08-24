// tokenlock/token-lock — the REAL machine-local scoped token lock engine
// (06-security-and-profiles.md §5; DEC-003/DEC-009 adjacent hygiene).
//
// Port of the verified Hermes lock machinery (READ-ONLY reference; semantics
// ported, no code vendored):
//   gateway/status.py:acquire_scoped_lock   → ScopedTokenLockManager.tryAcquire
//                                             (SYNCHRONOUS, tuple-shaped result —
//                                             never a promise, never an exception
//                                             on the refusal path)
//   gateway/status.py:release_scoped_lock   → releaseScopedLock (same-PID +
//                                             same-owner; foreign release no-op)
//   gateway/platforms/base.py:_acquire_platform_lock → acquirePlatformLock /
//                                             requireScopedLock (refusal ⇒ FATAL
//                                             TokenLockConflictError naming the
//                                             holder — base.py `_set_fatal_error`)
//
// STALE-DETECTION LADDER (06 §5, verified order):
//   1. corrupt/empty file                      → stale
//   2. same live PID + same owner              → self-reacquire (refresh record)
//   3. dead PID (ESRCH-proven or zombie)       → stale  — NO TTL wait anywhere:
//   4. start_time mismatch (PID reuse)         → stale     liveness IS the staleness
//   5. stopped process (state T/t)             → stale     test (#81468 reconnects)
//   6. cmdline oracle rung                     → see below
//   Stale removal is ATOMIC via rename(2) onto a tombstone so two racing
//   starters cannot both win; exactly one racer claims the stale file and the
//   loser's O_EXCL-equivalent create decides the winner.
//   Release is SAME-PID (+same-owner) ownership with NO start_time requirement
//   so reconnects never wedge on fingerprint timing (#81468).
//
// RUNG 6 (cmdline oracles): Hermes consults "does this look like a gateway"
// when start_time comparison is unavailable on either side (macOS/Windows) and
// as a secondary boot-time-collision defence. The pi-gateway CLI entrypoint
// does not exist yet, so the default identity predicates are CONSERVATIVE: an
// unreadable cmdline yields no verdict and both-oracle rungs stay inert unless
// the evidence is affirmative. Inject `identityProbe` to sharpen once the CLI
// shape lands. Recorded for ratification in the phase report (proposed DEC).
//
// SEAM COMPATIBILITY: this engine implements the minimal Phase-3 preview seam
// (src/pi_platforms/kit/token-lock.ts) behind identical call shapes —
// tryAcquire(scope, credentialId, owner, {replace}) → LockAcquisition,
// holderOf(scope, credentialId), AcquiredTokenLock.release(). pi_gateway may
// not import pi_platforms (01 §5.3 downward-only), so the shapes are declared
// structurally here; adapters consume this engine through the kit types.

import { renameSync, unlinkSync } from "node:fs";
import {
	buildLockRecord,
	createLockFile,
	defaultLockDir,
	hashIdentity,
	isCorruptLockFile,
	liveCmdlineMatchesHolder,
	readLockRecord,
	recordLooksLikeTokenLockHolder,
	refreshLockRecord,
	replaceLockFile,
	scopedLockPath,
	removeLockFile,
	type ScopedLockRecord,
} from "./lock-record.js";

import {
	getProcessStartTime,
	isProcessAlive,
	isProcessStopped,
} from "./process-identity.js";

/** Who holds a lock — seam-compatible holder info (kit/token-lock.ts). */
export interface LockHolderInfo {
	/** Logical owner id recorded at acquisition (instance id). */
	owner: string;
	/** When the current hold began (ms since injected clock epoch). */
	heldSinceMs: number;
}

/** Seam-compatible acquisition result: TUPLE-shaped, synchronous, refusal is a
 * VALUE (never an exception). */
export type LockAcquisition =
	| { acquired: true; lock: AcquiredTokenLock }
	| { acquired: false; holder: LockHolderInfo };

export interface AcquiredTokenLock {
	scope: string;
	credentialId: string;
	release(): void;
}

/**
 * Named-holder refusal surfaced as a FATAL adapter error (06 §5 shape;
 * structural parity with kit lifecycle-state.TokenLockConflictError — pi_gateway
 * cannot import pi_platforms upward, so the class lives here and carries the
 * same name/kind contract). Message NAMES the holder (base.py OOF-3 wording).
 */
export class TokenLockConflictError extends Error {
	readonly scope: string;
	readonly credentialId: string;
	readonly holder: string;
	readonly fatal = true as const;
	/** Hermes stamps the conflict retryable=True on the adapter fatal state. */
	readonly retryable = true as const;

	constructor(scope: string, credentialId: string, holder: string) {
		super(
			`${credentialId} in scope ${scope} already in use by ${holder}. ` +
				`Stop the other gateway first.`,
		);
		this.name = "TokenLockConflictError";
		this.scope = scope;
		this.credentialId = credentialId;
		this.holder = holder;
	}
}

export interface ScopedTokenLockManagerOptions {
	/** Lock directory override (tests mkdtemp-isolate; ops use env). */
	dir?: string | undefined;
	/** Monotonic-ish clock seam (tests inject). Default Date.now. */
	nowMs?: (() => number) | undefined;
	/** Human-readable profile label stamped into records (OOF-3). */
	profileLabel?: string | undefined;
}

interface InternalOptions {
	dir: string;
	now: () => number;
	profileLabel?: string | undefined;
}

function normalizeOptions(
	opts: ScopedTokenLockManagerOptions | undefined,
): InternalOptions {
	return {
		dir: opts?.dir ?? defaultLockDir(),
		now: opts?.nowMs ?? (() => Date.now()),
		profileLabel: opts?.profileLabel,
	};
}

/**
 * THE canonical acquisition primitive (status.py:acquire_scoped_lock port).
 * Synchronous; returns {acquired:true, lock} or {acquired:false, holder}.
 *
 * `options.replace` is the EXPLICIT takeover flag (04 §7 "takeover only via
 * explicit replace flag; no silent theft"): within THIS process it steals the
 * key from another logical owner and invalidates the prior handle's release().
 * Against a LIVE FOREIGN PROCESS it still refuses — cross-process theft is the
 * runner-phase --replace handoff (bounded termination before reacquire),
 * outside this layer by design.
 */
export function acquireScopedLock(
	scope: string,
	identity: string,
	owner: string,
	options: ScopedTokenLockManagerOptions & {
		metadata?: Record<string, unknown> | undefined;
		replace?: boolean | undefined;
	} = {},
): LockAcquisition {
	const mgr = new ScopedTokenLockManager(options);
	const acquisition = mgr.tryAcquire(scope, identity, owner, options);
	return acquisition;
}

/** Same-PID + same-owner release; any other releaser is silently ignored. */
export function releaseScopedLock(
	scope: string,
	identity: string,
	owner: string,
	options: Pick<ScopedTokenLockManagerOptions, "dir"> = {},
): void {
	const dir = options.dir ?? defaultLockDir();
	const path = scopedLockPath(dir, scope, identity);
	const existing = readLockRecord(path);
	if (!existing.found) return;
	// Same live pid + same owner ⇒ we own the lock. Do NOT demand start_time
	// equality (#81468); any other releaser — foreign pid OR displaced owner —
	// is silently ignored (06 §10 release-ownership row).
	if (existing.record.pid !== process.pid) return;
	if (existing.record.owner !== owner) return;
	removeLockFile(path);
}

/**
 * Holder lookup WITHOUT acquiring — inventory-grade view of one key.
 * Returns null when no readable record exists.
 */
export function scopedLockHolder(
	scope: string,
	identity: string,
	options: Pick<ScopedTokenLockManagerOptions, "dir" | "nowMs"> = {},
): LockHolderInfo | null {
	const dir = options.dir ?? defaultLockDir();
	const read = readLockRecord(scopedLockPath(dir, scope, identity));
	if (!read.found) return null;
	return holderInfoOf(read.record);
}

function holderInfoOf(record: ScopedLockRecord): LockHolderInfo {
	return { owner: record.owner, heldSinceMs: record.held_since_ms };
}

/**
 * Human-facing holder description for refusals/inventory (base.py OOF-3 port):
 * prefers the validated profile label; falls back to PID-only wording.
 */
export function scopedLockOwnerDescription(record: ScopedLockRecord): string {
	const profile =
		typeof record.profile === "string" &&
		/^[A-Za-z0-9_.-]{1,64}$/.test(record.profile.trim())
			? record.profile.trim()
			: null;
	const pid = typeof record.pid === "number" ? record.pid : null;
	if (profile !== null) {
		const suffix = pid !== null ? ` (PID ${pid})` : "";
		return `the '${profile}' profile gateway${suffix}`;
	}
	return `another gateway (PID ${pid ?? "unknown"})`;
}

/**
 * The REAL engine behind the Phase-3 kit seam. Two cooperating layers keyed on
 * (scope, credentialId) — DEC-004's two-layer lease shape applied to locks:
 * - cross-process: the machine-local lock FILE (this module's whole protocol);
 * - intra-process: owner-aware records — two logical owners inside ONE pid
 *   contend through the same file protocol, which also makes exclusion hold
 *   across WORKER THREADS (separate module registries would not).
 */
export class ScopedTokenLockManager {
	private readonly opts: InternalOptions;

	constructor(opts: ScopedTokenLockManagerOptions = {}) {
		this.opts = normalizeOptions(opts);
	}

	private pathOf(scope: string, credentialId: string): string {
		return scopedLockPath(this.opts.dir, scope, credentialId);
	}

	private now(): number {
		return this.opts.now();
	}

	tryAcquire(
		scope: string,
		credentialId: string,
		owner: string,
		options: {
			metadata?: Record<string, unknown> | undefined;
			replace?: boolean | undefined;
		} = {},
	): LockAcquisition {
		const path = this.pathOf(scope, credentialId);

		// Rung 0 — crash debris / corruption: an unwritable-record lock file is
		// BY DEFINITION not a live claim (Hermes: empty/invalid JSON → stale).
		if (isCorruptLockFile(path)) {
			removeTombstoning(path);
		}

		const existing = readLockRecord(path);

		if (existing.found) {
			const verdict = classifyExistingRecord(existing.record, {
				owner,
				replace: options.replace === true,
			});
			switch (verdict.action) {
				case "self-reacquire": {
					// Same pid + same owner: refresh stamps, keep holding
					// (#81468 — never demand start_time equality of ourselves).
					replaceLockFile(path, refreshLockRecord(existing.record, this.now()));
					return {
						acquired: true,
						lock: this.makeHandle(scope, credentialId, owner),
					};
				}
				case "steal-intra-process": {
					// Explicit replace authority INSIDE this process: rewrite the
					// record to us; the displaced handle's release() checks owner
					// equality and becomes a no-op (seam parity).
					replaceLockFile(
						path,
						refreshLockRecord({ ...existing.record, owner }, this.now()),
					);
					return {
						acquired: true,
						lock: this.makeHandle(scope, credentialId, owner),
					};
				}
				case "refuse": {
					return { acquired: false, holder: holderInfoOf(existing.record) };
				}
				case "stale": {
					// Atomic stale removal: rename(2) onto a tombstone. With
					// unlink()+create, two racers could BOTH observe removal and
					// both win; rename lets exactly one racer claim the stale
					// file and the loser falls through to the create below.
					removeTombstoning(path);
					break;
				}
			}
		}

		const record = buildLockRecord({
			scope,
			identity: credentialId,
			owner,
			metadata: options.metadata,
			profileLabel: this.opts.profileLabel,
			nowMs: this.now(),
		});
		let created = createLockFile(path, record);
		for (let races = 0; !created && races < 3; races++) {
			// Someone raced us between stale-removal and create: re-read THEIR
			// record and refuse against it (never fabricate holder info). A
			// racing RELEASE may also have vanished the winner meanwhile — the
			// bounded loop lands on a stable state either way.
			const winner = readLockRecord(path);
			if (winner.found) {
				return { acquired: false, holder: holderInfoOf(winner.record) };
			}
			created = createLockFile(path, record);
		}
		if (!created) {
			throw new Error(
				`token lock ${scope}/${hashIdentity(credentialId)} kept vanishing under contention`,
			);
		}
		return {
			acquired: true,
			lock: this.makeHandle(scope, credentialId, owner),
		};
	}

	holderOf(scope: string, credentialId: string): LockHolderInfo | null {
		return scopedLockHolder(scope, credentialId, {
			dir: this.opts.dir,
			nowMs: this.opts.now,
		});
	}

	private makeHandle(
		scope: string,
		credentialId: string,
		owner: string,
	): AcquiredTokenLock {
		let released = false;
		return {
			scope,
			credentialId,
			release: () => {
				if (released) return;
				released = true;
				releaseScopedLock(scope, credentialId, owner, { dir: this.opts.dir });
			},
		};
	}
}

type Verdict =
	| { action: "self-reacquire" }
	| { action: "steal-intra-process" }
	| { action: "refuse" }
	| { action: "stale" };

/**
 * The staleness ladder for an EXISTING readable record (06 §5 verified order).
 */
export function classifyExistingRecord(
	record: ScopedLockRecord,
	ctx: { owner: string; replace: boolean },
): Verdict {
	const existingPid =
		typeof record.pid === "number" &&
		Number.isInteger(record.pid) &&
		record.pid > 0
			? record.pid
			: null;

	// Rung 2 — our own pid. start_time CANNOT distinguish processes sharing the
	// caller's pid (impossible while we are alive); demanding equality falsely
	// rejects reconnects when fingerprints are missing (#81468).
	if (existingPid === process.pid) {
		if (record.owner === ctx.owner) return { action: "self-reacquire" };
		return ctx.replace
			? { action: "steal-intra-process" }
			: { action: "refuse" };
	}

	// Rung 3 — dead pid (kernel-proven ESRCH or zombie): stale. This is the
	// KILL-HOLDER REACQUISITION path: reclaimable IMMEDIATELY, no TTL.
	if (existingPid === null || !isProcessAlive(existingPid)) {
		return { action: "stale" };
	}

	// Rung 4 — PID-reuse guard: a live pid whose CURRENT start time differs
	// from the recorded fingerprint is a DIFFERENT process ⇒ stale.
	const recordedStart =
		typeof record.start_time === "number" ? record.start_time : null;
	const currentStart = getProcessStartTime(existingPid);
	if (
		recordedStart !== null &&
		currentStart !== null &&
		currentStart !== recordedStart
	) {
		return { action: "stale" };
	}

	// Rung 5 — stopped (SIGSTOP/tracer stop) holders are treated stale so an
	// explicit replace works (they pass liveness but hold nothing runnable).
	if (isProcessStopped(existingPid)) {
		return { action: "stale" };
	}

	// Rung 6 — cmdline oracles (conservative defaults; see module header):
	// only when BOTH the live cmdline AND the record say "not ours" do we
	// declare stale — the boot-time PID+start_time collision defence.
	if (
		recordedStart !== null &&
		currentStart !== null &&
		currentStart === recordedStart &&
		!liveCmdlineMatchesHolder(existingPid, record) &&
		!recordLooksLikeTokenLockHolder(record)
	) {
		return { action: "stale" };
	}

	// Live, matching-fingerprint holder: REFUSE, naming it.
	return { action: "refuse" };
}

/**
 * Stale removal ATOMIC via tombstone rename (status.py parity): exactly one
 * racer wins the rename; losers observe ENOENT and fall through to the
 * create, where link(2)'s EEXIST picks the single winner. The tombstone is
 * deleted right after — it exists only to make the claim atomic.
 */
function removeTombstoning(path: string): void {
	const tombstone = `${path}.stale`;
	try {
		renameSync(path, tombstone);
	} catch (err) {
		// Another racer already claimed the stale lock (and may have created
		// a fresh one) — fall through and let the create below decide the
		// single winner (status.py: "let O_EXCL below decide the winner").
		if ((err as NodeJS.ErrnoException).code === "ENOENT") return;
		throw err;
	}
	try {
		unlinkSync(tombstone);
	} catch {
		/* best-effort; a leftover .stale never gates acquisition */
	}
}

// ═══════════════════════════════════════════════════════════════════════════
// Canonical caller wrapper — gateway/platforms/base.py:_acquire_platform_lock
// port. Adapters connect through THIS, never raw acquire calls; refusal is a
// LOUD fatal adapter error naming the holder (OOF-3), never a silent drop.
// ═══════════════════════════════════════════════════════════════════════════

export interface PlatformLockRequest {
	scope: string;
	identity: string;
	/** Human-readable resource description ("Telegram bot token"). */
	resourceDesc: string;
	owner: string;
	metadata?: Record<string, unknown> | undefined;
}

/**
 * Acquire the scoped lock for an adapter connect. Returns the acquired handle,
 * or throws the FATAL TokenLockConflictError whose message names the holder —
 * base.py's `fatal_error("lock_conflict", f"{desc} in use by {holder}")` with
 * the kit-mandated exception surface.
 */
export function requireScopedLock(
	request: PlatformLockRequest,
	options: ScopedTokenLockManagerOptions = {},
): AcquiredTokenLock {
	const manager = new ScopedTokenLockManager(options);
	const acquisition = manager.tryAcquire(
		request.scope,
		request.identity,
		request.owner,
		{
			metadata: request.metadata,
		},
	);
	if (!acquisition.acquired) {
		const read = readRecordQuiet(options, request);
		const description =
			read !== null
				? scopedLockOwnerDescription(read)
				: `another instance ('${acquisition.holder.owner}')`;
		throw new TokenLockConflictError(
			request.scope,
			request.identity,
			description,
		);
	}
	return acquisition.lock;
}

function readRecordQuiet(
	options: ScopedTokenLockManagerOptions | undefined,
	request: PlatformLockRequest,
): ScopedLockRecord | null {
	const dir = options?.dir ?? defaultLockDir();
	const read = readLockRecord(
		scopedLockPath(dir, request.scope, request.identity),
	);
	return read.found ? read.record : null;
}
