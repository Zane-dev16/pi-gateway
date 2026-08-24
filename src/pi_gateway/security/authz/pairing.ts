// authz/pairing — the DM pairing handshake machinery producing the grants §2.1
// step 7 consumes (06 §2.4; gap-audit A5).
//
// Ported semantics, Hermes anchors (READ-ONLY reference; no code vendored):
//   gateway/pairing.py::PairingStore           → PairingStore
//   CODE_ALPHABET/CODE_LENGTH/CODE_TTL_SECONDS → same constants (8-char codes,
//       unambiguous alphabet ABCDEFGHJKLMNPQRSTUVWXYZ23456789, 3600 s TTL)
//   RATE_LIMIT_SECONDS / LOCKOUT_SECONDS /     → 600 s / 3600 s / 3 / 5
//   MAX_PENDING_PER_PLATFORM / MAX_FAILED_ATTEMPTS
//   generate_code / approve_code / approve_request / revoke / list_pending /
//       list_approved / clear_pending / looks_like_request_id → same names
//   _hash_code (salted SHA-256) + secrets.compare_digest → hashCode +
//       constant-time digest compare
//
// DELIBERATE STORAGE DIVERGENCE (proposed DEC text in the phase report):
// Hermes persists per-platform JSON files under ~/.hermes/pairing written via
// _secure_write; Pi Gateway persists THE SAME FIELDS in pi_state's state.db
// (tables `pairing_pending` / `pairing_approved` / `pairing_rate_limits`,
// created idempotently by this module — additive migration into
// pi_state/schema.ts SCHEMA_TABLES_SQL proposed). Rationale: one durable,
// crash-safe, cross-process substrate already exists (02 §1); JSON files would
// fork gateway state across two storage systems. All mutations run inside
// BEGIN IMMEDIATE transactions (pi_state/wal.executeWrite) so two gateway
// processes serialize exactly like Hermes' threading.RLock did in-process.
// File-permission hygiene carries over as mode-0600 on the db's parent dir
// contract (state.db is already operator-private) and on mirrored .env writes.
//
// Grant semantics (#23778, option i): approval admits via the §2.1 union AND
// mirrors the user into the platform's env-backed allowlist WHEN ONE IS
// CONFIGURED; revocation removes from BOTH the store and the allowlists.
// Mirroring is best-effort — a failed write degrades to "grant recorded but
// not mirrored", never blocks approval.

import {
	createHash,
	randomBytes as nodeRandomBytes,
	timingSafeEqual,
} from "node:crypto";
import type Database from "better-sqlite3";

import { executeWrite } from "../../../pi_state/wal.js";
import {
	expandWhatsappAliases,
	normalizeWhatsappIdentifier,
} from "../../resolution/whatsapp-identity.js";
import {
	defaultAllowlistMirrorForHome,
	type AllowlistMirror,
} from "./env-mirror.js";
import { PAIRING_ALLOWLIST_ENV } from "./platform-tables.js";
import { resolvePiHome } from "../../../pi_home.js";

// ── Verified constants (gateway/pairing.py L44-57) ──────────────────────────

/** Unambiguous alphabet — excludes 0/O and 1/I to prevent confusion. */
export const CODE_ALPHABET = "ABCDEFGHJKLMNPQRSTUVWXYZ23456789";
export const CODE_LENGTH = 8;
/** Codes expire after 1 hour. */
export const CODE_TTL_SECONDS = 3600;
/** 1 code request per user per platform per 10 minutes. */
export const RATE_LIMIT_SECONDS = 600;
/** Lockout duration after too many failed approvals. */
export const LOCKOUT_SECONDS = 3600;
/** Max pending codes per platform. */
export const MAX_PENDING_PER_PLATFORM = 3;
/** Failed approvals before lockout. */
export const MAX_FAILED_ATTEMPTS = 5;

export function hashCode(code: string, salt: Buffer): string {
	return createHash("sha256").update(salt).update(code, "utf8").digest("hex");
}

/**
 * Constant-time comparison with the kit/trust.secureCompare length discipline:
 * unequal lengths return false WITHOUT any early-exit content divergence.
 */
export function compareDigest(a: string, b: string): boolean {
	const bufA = Buffer.from(a, "utf8");
	const bufB = Buffer.from(b, "utf8");
	if (bufA.length !== bufB.length) return false;
	return timingSafeEqual(bufA, bufB);
}

export interface PairingClock {
	/** Epoch seconds (parity time.time()). */
	nowSeconds(): number;
}

export const systemPairingClock: PairingClock = {
	nowSeconds: () => Date.now() / 1000,
};

export interface PairingStoreOptions {
	/** Injected clock — behavior contracts drive TTL/rate-limit deterministically. */
	clock?: PairingClock | undefined;
	/**
	 * Randomness seam. Default node:crypto.randomBytes. The alphabet is exactly
	 * 32 chars (= 2^5) so one random byte yields an UNBIASED code character via
	 * the top 5 bits — no modulo bias, parity of secrets.choice uniformity.
	 */
	randomBytes?: ((n: number) => Buffer) | undefined;
	/** WhatsApp bridge session dir for alias expansion (tests inject temp dirs). */
	whatsappSessionDir?: string | undefined;
	/**
	 * Operator-allowlist mirror behind the grant mirror + revocation cascade.
	 * Default: `<pi-home>/.env` resolved per call (respects pi-home overrides).
	 */
	allowlistMirror?: AllowlistMirror | undefined;
	/** BEGIN IMMEDIATE retry patience (cross-process busy handling). */
	patienceMs?: number | undefined;
	/** Lockout observer (tests assert without reading console noise). */
	onLockout?: ((platform: string, untilSeconds: number) => void) | undefined;
}

export interface ApprovedUser {
	platform: string;
	user_id: string;
	user_name: string;
	approved_at: number;
}

export interface PendingRequest {
	platform: string;
	request_id: string;
	user_id: string;
	user_name: string;
	age_minutes: number;
}

export interface ApprovalResult {
	user_id: string;
	user_name: string;
}

function platformUsesWhatsappIdentity(platform: string): boolean {
	const p = platform.trim().toLowerCase();
	return p === "whatsapp" || p === "whatsapp_cloud";
}

export class PairingStore {
	private readonly db: Database.Database;
	private readonly clock: PairingClock;
	private readonly randomBytesFn: (n: number) => Buffer;
	private readonly opts: PairingStoreOptions;

	constructor(db: Database.Database, opts: PairingStoreOptions = {}) {
		this.db = db;
		this.opts = opts;
		this.clock = opts.clock ?? systemPairingClock;
		this.randomBytesFn = opts.randomBytes ?? nodeRandomBytes;
		this.ensureTables();
	}

	// ── storage (additive, idempotent — see module docstring) ────────────────

	private ensureTables(): void {
		this.db.exec(`
CREATE TABLE IF NOT EXISTS pairing_pending (
  platform TEXT NOT NULL,
  entry_id TEXT NOT NULL,
  code_hash TEXT NOT NULL,
  salt TEXT NOT NULL,
  user_id TEXT NOT NULL,
  user_name TEXT NOT NULL DEFAULT '',
  created_at REAL NOT NULL,
  PRIMARY KEY (platform, entry_id)
);
CREATE TABLE IF NOT EXISTS pairing_approved (
  platform TEXT NOT NULL,
  user_id TEXT NOT NULL,
  user_name TEXT NOT NULL DEFAULT '',
  approved_at REAL NOT NULL,
  PRIMARY KEY (platform, user_id)
);
CREATE TABLE IF NOT EXISTS pairing_rate_limits (
  key TEXT PRIMARY KEY,
  value REAL NOT NULL
);
`);
	}

	/** exactOptionalPropertyTypes-safe executeWrite options. */
	private writeOpts(): { patienceMs: number } | undefined {
		return this.opts.patienceMs !== undefined
			? { patienceMs: this.opts.patienceMs }
			: undefined;
	}

	private get pendingStmts() {
		return {
			insert: this.db.prepare(
				`INSERT INTO pairing_pending (platform, entry_id, code_hash, salt, user_id, user_name, created_at)
				 VALUES (?, ?, ?, ?, ?, ?, ?)`,
			),
			all: this.db.prepare(
				`SELECT entry_id, code_hash, salt, user_id, user_name, created_at
				 FROM pairing_pending WHERE platform = ? ORDER BY created_at`,
			),
			count: this.db.prepare(
				`SELECT COUNT(*) AS n FROM pairing_pending WHERE platform = ?`,
			),
			delete: this.db.prepare(
				`DELETE FROM pairing_pending WHERE platform = ? AND entry_id = ?`,
			),
			deleteAll: this.db.prepare(
				`DELETE FROM pairing_pending WHERE platform = ?`,
			),
			expired: this.db.prepare(
				`DELETE FROM pairing_pending
				 WHERE platform = ? AND (? - created_at) > ${CODE_TTL_SECONDS}`,
			),
			platforms: this.db.prepare(
				`SELECT DISTINCT platform FROM pairing_pending`,
			),
		};
	}

	private get approvedStmts() {
		return {
			upsert: this.db.prepare(
				`INSERT INTO pairing_approved (platform, user_id, user_name, approved_at)
				 VALUES (?, ?, ?, ?)
				 ON CONFLICT(platform, user_id) DO UPDATE SET
				   user_name = excluded.user_name, approved_at = excluded.approved_at`,
			),
			all: this.db.prepare(
				`SELECT user_id, user_name, approved_at FROM pairing_approved WHERE platform = ?`,
			),
		};
	}

	private get limitsStmts() {
		return {
			get: this.db.prepare(
				`SELECT value FROM pairing_rate_limits WHERE key = ?`,
			),
			set: this.db.prepare(
				`INSERT INTO pairing_rate_limits (key, value) VALUES (?, ?)
				 ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
			),
		};
	}

	private limitValue(key: string): number {
		const row = this.limitsStmts.get.get(key) as { value: number } | undefined;
		return row?.value ?? 0;
	}

	private setLimitValue(key: string, value: number): void {
		this.limitsStmts.set.run(key, value);
	}

	// ── identity matching (pairing.py:_normalize_user_id/_user_id_aliases) ──

	normalizeUserId(platform: string, userId: string): string {
		const raw = String(userId ?? "").trim();
		if (!platformUsesWhatsappIdentity(platform)) return raw;
		return normalizeWhatsappIdentifier(raw) || raw;
	}

	userIdAliases(platform: string, userId: string): Set<string> {
		const raw = String(userId ?? "").trim();
		if (raw === "") return new Set();
		const aliases = new Set<string>([raw, this.normalizeUserId(platform, raw)]);
		if (platformUsesWhatsappIdentity(platform)) {
			const opts =
				this.opts.whatsappSessionDir !== undefined
					? { sessionDir: this.opts.whatsappSessionDir }
					: {};
			for (const alias of expandWhatsappAliases(raw, opts)) aliases.add(alias);
		}
		aliases.delete("");
		return aliases;
	}

	userIdsMatch(platform: string, left: string, right: string): boolean {
		const leftAliases = this.userIdAliases(platform, left);
		const rightAliases = this.userIdAliases(platform, right);
		if (leftAliases.size === 0 || rightAliases.size === 0) return false;
		for (const alias of leftAliases) {
			if (rightAliases.has(alias)) return true;
		}
		return false;
	}

	// ── approved users ────────────────────────────────────────────────────────

	isApproved(platform: string, userId: string): boolean {
		const rows = this.approvedStmts.all.all(platform) as Array<{
			user_id: string;
		}>;
		for (const row of rows) {
			if (this.userIdsMatch(platform, row.user_id, userId)) return true;
		}
		return false;
	}

	listApproved(platform?: string): ApprovedUser[] {
		const platforms =
			platform !== undefined ? [platform] : this.knownApprovedPlatforms();
		const results: ApprovedUser[] = [];
		for (const p of platforms) {
			const rows = this.approvedStmts.all.all(p) as Array<{
				user_id: string;
				user_name: string;
				approved_at: number;
			}>;
			for (const row of rows) {
				results.push({
					platform: p,
					user_id: row.user_id,
					user_name: row.user_name,
					approved_at: row.approved_at,
				});
			}
		}
		return results;
	}

	private knownApprovedPlatforms(): string[] {
		const rows = this.db
			.prepare(`SELECT DISTINCT platform FROM pairing_approved`)
			.all() as Array<{ platform: string }>;
		return rows.map((r) => r.platform);
	}

	/** Must run INSIDE the write transaction. */
	private approveUserInsideTx(
		platform: string,
		userId: string,
		userName: string,
	): void {
		const normalized = this.normalizeUserId(platform, userId);
		this.approvedStmts.upsert.run(
			platform,
			normalized,
			userName,
			this.clock.nowSeconds(),
		);
	}

	// ── grant mirror + revocation cascade (best-effort, #23778 option i) ────

	private mirror(): AllowlistMirror {
		return (
			this.opts.allowlistMirror ??
			defaultAllowlistMirrorForHome(resolvePiHome())
		);
	}

	private allowlistEnvVar(platform: string): string | null {
		return PAIRING_ALLOWLIST_ENV[platform.trim().toLowerCase()] ?? null;
	}

	private splitAllowlist(raw: string): string[] {
		return raw
			.split(",")
			.map((part) => part.trim())
			.filter((part) => part !== "");
	}

	/**
	 * Add the user to the platform allowlist IF one is configured (option i).
	 * On an open gateway (no allowlist) do NOTHING — never silently convert an
	 * open gateway into a locked one on first pairing.
	 */
	syncAllowlistAdd(platform: string, userId: string): void {
		const envVar = this.allowlistEnvVar(platform);
		if (!envVar) return;
		let current: string | undefined;
		try {
			current = this.mirror().readVar(envVar);
		} catch {
			return; // best-effort: the store grant still authorizes via the union
		}
		if (current === undefined || current.trim() === "") return;
		const ids = this.splitAllowlist(current);
		if (ids.includes("*") || ids.includes(String(userId))) return; // covered
		try {
			this.mirror().writeVar(envVar, [...ids, String(userId)].join(","));
		} catch {
			/* degrade to "grant recorded but not mirrored" */
		}
	}

	/**
	 * Remove the user (and alias equivalents) from the allowlist. Matching
	 * mirrors the store's alias rules: approval mirrors a NORMALIZED phone into
	 * WHATSAPP_ALLOWED_USERS while revocation is often invoked with a JID or
	 * device-suffix form — exact-string delete would leave the sender authorized.
	 */
	syncAllowlistRemove(platform: string, userId: string): void {
		const envVar = this.allowlistEnvVar(platform);
		if (!envVar) return;
		let current: string | undefined;
		try {
			current = this.mirror().readVar(envVar);
		} catch {
			return;
		}
		if (current === undefined || current.trim() === "") return;
		const ids = this.splitAllowlist(current);
		const remaining = ids.filter(
			(id) => id === "*" || !this.userIdsMatch(platform, id, String(userId)),
		);
		if (remaining.length === ids.length) return; // not present
		try {
			if (remaining.length > 0) {
				this.mirror().writeVar(envVar, remaining.join(","));
			} else {
				this.mirror().removeVar(envVar);
			}
		} catch {
			/* best-effort cascade */
		}
	}

	// ── pending codes ────────────────────────────────────────────────────────

	/**
	 * Generate a pairing code for a new user. Returns the PLAINTEXT code (it is
	 * delivered to the user's DM and shown NOWHERE else), or null when:
	 * rate-limited / capacity cap reached / platform locked out.
	 * Storage holds ONLY a salted SHA-256 hash — reading pending rows never
	 * reveals codes.
	 *
	 * Order parity of generate_code: cleanup → lockout → rate limit → capacity.
	 */
	async generateCode(
		platform: string,
		userId: string,
		userName = "",
	): Promise<string | null> {
		return executeWrite(
			this.db,
			() => {
				const p = platform.trim().toLowerCase();
				this.cleanupExpiredInsideTx(p);

				if (this.isLockedOut(p)) return null;

				if (this.isRateLimited(p, userId)) return null;

				const countRow = this.pendingStmts.count.get(p) as { n: number };
				if (countRow.n >= MAX_PENDING_PER_PLATFORM) return null;

				// Unbiased pick: |alphabet| == 32 == 2^5 ⇒ top 5 bits of one byte.
				let code = "";
				const raw = this.randomBytesFn(CODE_LENGTH);
				for (let i = 0; i < CODE_LENGTH; i++) {
					code += CODE_ALPHABET[(raw[i] as number) >> 3];
				}

				const salt = this.randomBytesFn(16);
				const entryId = this.randomBytesFn(8).toString("hex");
				this.pendingStmts.insert.run(
					p,
					entryId,
					hashCode(code, salt),
					salt.toString("hex"),
					this.normalizeUserId(p, userId),
					String(userName ?? ""),
					this.clock.nowSeconds(),
				);
				this.recordRateLimitInsideTx(p, userId);
				return code;
			},
			this.writeOpts(),
		);
	}

	/**
	 * Approve a pending code. Lockout runs BEFORE the pending lookup (#10195):
	 * a valid sitting code cannot be accepted once locked out. A miss records a
	 * failed attempt; a SUCCESS resets the consecutive-failure streak so one
	 * fresh typo cannot trip a stale lockout. Returns null for invalid/expired/
	 * locked-out; callers disambiguate with {@link isLockedOut}.
	 */
	async approveCode(
		platform: string,
		code: string,
	): Promise<ApprovalResult | null> {
		return executeWrite(
			this.db,
			() => {
				const p = platform.trim().toLowerCase();
				this.cleanupExpiredInsideTx(p);
				const candidate = String(code ?? "")
					.toUpperCase()
					.trim();

				// #10195: must run before the pending lookup — otherwise the lockout
				// only blocks generate_code, nullifying brute-force protection for
				// any code already issued.
				if (this.isLockedOut(p)) return null;

				const rows = this.pendingStmts.all.all(p) as Array<{
					entry_id: string;
					code_hash: string;
					salt: string;
					user_id: string;
					user_name: string;
				}>;
				for (const row of rows) {
					let salt: Buffer;
					try {
						salt = Buffer.from(row.salt, "hex");
					} catch {
						continue; // malformed entry — skipped, pruned at TTL
					}
					if (compareDigest(hashCode(candidate, salt), row.code_hash)) {
						return this.finishApprovalInsideTx(p, row.entry_id, row);
					}
				}
				this.recordFailedAttemptInsideTx(p);
				return null;
			},
			this.writeOpts(),
		);
	}

	/** True when `value` has the shape of a list_pending request id (16 hex). */
	static looksLikeRequestId(value: string): boolean {
		const v = String(value ?? "").trim();
		return v.length === 16 && /^[0-9a-fA-F]+$/.test(v);
	}

	/**
	 * Approve by server-side request id — the admin-surface grant path that
	 * must never reveal the DM'd code. Unlike approve_code this is NOT gated by
	 * the lockout and does NOT count misses toward it: a request id is only
	 * obtainable by an already-authenticated admin; a stale id means "the row
	 * you clicked expired", not an attack (counting GUI clicks let a stale list
	 * lock the operator's CLI code path too).
	 */
	async approveRequest(
		platform: string,
		requestId: string,
	): Promise<ApprovalResult | null> {
		return executeWrite(
			this.db,
			() => {
				const p = platform.trim().toLowerCase();
				this.cleanupExpiredInsideTx(p);
				const wanted = String(requestId ?? "")
					.trim()
					.toLowerCase();
				if (wanted === "") return null;
				const rows = this.pendingStmts.all.all(p) as Array<{
					entry_id: string;
					code_hash: string;
					salt: string;
					user_id: string;
					user_name: string;
				}>;
				for (const row of rows) {
					if (compareDigest(row.entry_id.toLowerCase(), wanted)) {
						return this.finishApprovalInsideTx(p, row.entry_id, row);
					}
				}
				return null;
			},
			this.writeOpts(),
		);
	}

	/** Must run INSIDE the write transaction holding the matched row. */
	private finishApprovalInsideTx(
		platform: string,
		entryId: string,
		row: { user_id: string; user_name: string },
	): ApprovalResult {
		this.pendingStmts.delete.run(platform, entryId);
		// Success proves legitimacy: the brute-force streak must not carry over
		// (counter tracks CONSECUTIVE failures, persisted until reset).
		this.resetFailedAttemptsInsideTx(platform);
		this.approveUserInsideTx(platform, row.user_id, row.user_name);
		// Best-effort grant mirror AFTER the durable grant exists.
		this.syncAllowlistAdd(
			platform,
			this.normalizeUserId(platform, row.user_id),
		);
		return { user_id: row.user_id, user_name: row.user_name };
	}

	listPending(platform?: string): PendingRequest[] {
		const platforms =
			platform !== undefined
				? [platform]
				: (
						this.pendingStmts.platforms.all() as Array<{ platform: string }>
					).map((r) => r.platform);
		const results: PendingRequest[] = [];
		const now = this.clock.nowSeconds();
		for (const p of platforms) {
			const rows = this.pendingStmts.all.all(p) as Array<{
				entry_id: string;
				user_id: string;
				user_name: string;
				created_at: number;
			}>;
			for (const row of rows) {
				results.push({
					platform: p,
					request_id: row.entry_id,
					user_id: row.user_id,
					user_name: row.user_name,
					age_minutes: Math.floor((now - row.created_at) / 60),
				});
			}
		}
		return results;
	}

	async clearPending(platform?: string): Promise<number> {
		return executeWrite(
			this.db,
			() => {
				const platforms =
					platform !== undefined
						? [platform]
						: (
								this.pendingStmts.platforms.all() as Array<{ platform: string }>
							).map((r) => r.platform);
				let count = 0;
				for (const p of platforms) {
					const row = this.pendingStmts.count.get(p) as { n: number };
					count += row.n;
					this.pendingStmts.deleteAll.run(p);
				}
				return count;
			},
			this.writeOpts(),
		);
	}

	// ── revocation (cascade to allowlists) ───────────────────────────────────

	/** Remove a paired user. Returns true when a grant was found and removed. */
	async revoke(platform: string, userId: string): Promise<boolean> {
		const removed = await executeWrite(
			this.db,
			() => {
				const p = platform.trim().toLowerCase();
				const rows = this.approvedStmts.all.all(p) as Array<{
					user_id: string;
				}>;
				const matching = rows
					.filter((row) => this.userIdsMatch(p, row.user_id, String(userId)))
					.map((row) => row.user_id);
				if (matching.length === 0) return false;
				const del = this.db.prepare(
					`DELETE FROM pairing_approved WHERE platform = ? AND user_id = ?`,
				);
				for (const id of matching) del.run(p, id);
				return true;
			},
			this.writeOpts(),
		);
		if (!removed) return false;
		// Cascade AFTER the durable removal: drop every alias-matching allowlist
		// entry the approval (or the operator) added. Never strips "*".
		this.syncAllowlistRemove(platform, userId);
		return true;
	}

	// ── rate limiting + lockout ──────────────────────────────────────────────

	isRateLimited(platform: string, userId: string): boolean {
		const now = this.clock.nowSeconds();
		for (const alias of this.userIdAliases(platform, userId)) {
			const last = this.limitValue(`${platform}:${alias}`);
			if (now - last < RATE_LIMIT_SECONDS) return true;
		}
		return false;
	}

	recordRateLimit(platform: string, userId: string): void {
		void executeWrite(
			this.db,
			() => {
				this.recordRateLimitInsideTx(platform, userId);
			},
			this.writeOpts(),
		);
	}

	private recordRateLimitInsideTx(platform: string, userId: string): void {
		const now = this.clock.nowSeconds();
		for (const alias of this.userIdAliases(platform, userId)) {
			this.setLimitValue(`${platform}:${alias}`, now);
		}
	}

	isLockedOut(platform: string): boolean {
		const p = platform.trim().toLowerCase();
		const until = this.limitValue(`_lockout:${p}`);
		return this.clock.nowSeconds() < until;
	}

	lockoutUntil(platform: string): number {
		return this.limitValue(`_lockout:${platform.trim().toLowerCase()}`);
	}

	private recordFailedAttemptInsideTx(platform: string): void {
		const key = `_failures:${platform}`;
		const fails = this.limitValue(key) + 1;
		if (fails >= MAX_FAILED_ATTEMPTS) {
			const until = this.clock.nowSeconds() + LOCKOUT_SECONDS;
			this.setLimitValue(`_lockout:${platform}`, until);
			this.setLimitValue(key, 0); // reset counter — lockout window governs now
			this.opts.onLockout?.(platform, until);
		} else {
			this.setLimitValue(key, fails);
		}
	}

	private resetFailedAttemptsInsideTx(platform: string): void {
		const key = `_failures:${platform}`;
		if (this.limitValue(key) !== 0) this.setLimitValue(key, 0);
	}

	// ── expiry ────────────────────────────────────────────────────────────────

	private cleanupExpiredInsideTx(platform: string): void {
		this.pendingStmts.expired.run(platform, this.clock.nowSeconds());
	}

	/** Public sweep for operators/tests; returns rows removed. */
	async cleanupExpired(platform: string): Promise<number> {
		return executeWrite(
			this.db,
			() => {
				const before = (this.pendingStmts.count.get(platform) as { n: number })
					.n;
				this.cleanupExpiredInsideTx(platform);
				const after = (this.pendingStmts.count.get(platform) as { n: number })
					.n;
				return before - after;
			},
			this.writeOpts(),
		);
	}
}
