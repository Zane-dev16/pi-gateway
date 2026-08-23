// pi_state/wal.ts — SQLite journal-mode ladder, open helper, and the
// contended-write ladder (BEGIN IMMEDIATE + jittered patience).
//
// Spec: /root/pi-gateway/02-session-and-state.md §1.1 "Journal mode ladder"
// and §12 row "Write-lock contention" (app-level retry, jittered patience).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored —
// production-quality port of the proven Phase-0 spike shape):
//   hermes_state.py:apply_wal_with_fallback             → applyWalWithFallback
//   hermes_state.py:resolve_journal_mode                → resolveJournalMode (invalid fails safe to wal)
//   hermes_state.py:is_sqlite_wal_reset_vulnerable      → isSqliteWalResetVulnerable (#70055 gate)
//   hermes_state.py:_on_disk_journal_mode               → onDiskJournalMode (read-only header probe)
//   hermes_state.py:_set_journal_mode_no_wait           → setJournalModeNoWait (busy_timeout=0 flip)
//   hermes_state.py:_WAL_INCOMPAT_MARKERS               → WAL_INCOMPAT_MARKERS
//   hermes_state.py:_apply_wal_size_limit               → applyWalSizeLimit (journal_size_limit=64MiB)
//   hermes_state.py:_log_wal_fallback_once              → per-label ERROR dedup
//   SessionDB._connect_and_init_with_lock_patience      → runWithJitteredPatience consumers
//   SessionDB._execute_write                            → executeWrite (BEGIN IMMEDIATE + two-band jitter)

import Database from "better-sqlite3";

/** Upper bound for the write-ahead log. hermes_state.py:_WAL_SIZE_LIMIT_BYTES */
const WAL_SIZE_LIMIT_BYTES = 64 * 1024 * 1024;
/** hermes_state.py:_WAL_INCOMPAT_MARKERS */
const WAL_INCOMPAT_MARKERS = [
	"locking protocol", // SQLITE_PROTOCOL on NFS/SMB
	"not authorized", // some FUSE mounts block the WAL pragma outright
	"disk i/o error", // ZFS SHM corruption under concurrent connections
] as const;

export type JournalMode = "wal" | "delete";

export interface PragmaPort {
	/** Read a pragma (e.g. "journal_mode", "busy_timeout"). null when no row. */
	get(name: string): string | number | null;
	/** Execute `PRAGMA <expr>`. Returns resulting row value. */
	set(expr: string): string | number | null;
	/** Linked SQLite library version string, e.g. "3.53.4". */
	sqliteVersion(): string;
}

/** Real port over a better-sqlite3 connection. */
export class RealPragmaPort implements PragmaPort {
	private readonly db: Database.Database;
	constructor(db: Database.Database) {
		this.db = db;
	}
	get(name: string): string | number | null {
		const v = this.db.pragma(name, { simple: true }) as
			| string
			| number
			| undefined
			| null;
		return v === undefined ? null : v;
	}
	set(expr: string): string | number | null {
		const v = this.db.pragma(expr, { simple: true }) as
			| string
			| number
			| undefined
			| null;
		return v === undefined ? null : v;
	}
	sqliteVersion(): string {
		const row = this.db.prepare("SELECT sqlite_version() AS v").get() as {
			v: string;
		};
		return row.v;
	}
}

export function normalizePragmaValue(
	v: string | number | null | undefined,
): string {
	if (v === null || v === undefined) return "";
	return String(v).trim().toLowerCase();
}

function errMessage(err: unknown): string {
	if (err instanceof Error) return err.message;
	return String(err);
}

function errCode(err: unknown): string {
	if (
		err instanceof Error &&
		"code" in err &&
		typeof (err as { code?: unknown }).code === "string"
	) {
		return (err as { code: string }).code;
	}
	return "";
}

/** Retryable lock classes (locked/busy + transient engine errors). */
export function isLockedOrBusy(err: unknown): boolean {
	const code = errCode(err);
	if (
		code === "SQLITE_BUSY" ||
		code === "SQLITE_BUSY_SNAPSHOT" ||
		code === "SQLITE_LOCKED" ||
		code === "SQLITE_LOCKED_SHAREDCACHE"
	) {
		return true;
	}
	return /locked|busy/i.test(errMessage(err));
}

/** Marker match for WAL-incompatible filesystem failures. */
export function isWalIncompatError(err: unknown): boolean {
	if (errCode(err) === "SQLITE_PROTOCOL") return true; // message text varies across builds
	const msg = errMessage(err).toLowerCase();
	return WAL_INCOMPAT_MARKERS.some((m) => msg.includes(m));
}

/** Raised when requireWal is set and the filesystem cannot do WAL. */
export class WalUnsupportedError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "WalUnsupportedError";
	}
}
export function isWalUnsupportedError(err: unknown): boolean {
	return err instanceof WalUnsupportedError;
}

/**
 * Operator setting ∈ {wal, delete}; anything else fails safe to wal
 * (hermes_state.py:resolve_journal_mode).
 */
export function resolveJournalMode(raw: unknown): JournalMode {
	if (raw === undefined || raw === null) return "wal";
	if (typeof raw !== "string") return "wal";
	const mode = raw.trim().toLowerCase();
	return mode === "wal" || mode === "delete" ? mode : "wal";
}

export function parseSqliteVersion(s: string): [number, number, number] | null {
	const m = /^(\d+)\.(\d+)\.(\d+)/.exec(s.trim());
	if (!m) return null;
	const maj = Number(m[1]);
	const min = Number(m[2]);
	const pat = Number(m[3]);
	if (Number.isNaN(maj) || Number.isNaN(min) || Number.isNaN(pat)) return null;
	return [maj, min, pat];
}

/**
 * True for SQLite builds with the WAL-reset bug (#70055,
 * https://sqlite.org/wal.html#walresetbug): documented through 3.51.2, fixed
 * in 3.51.3+ with backports 3.50.7 and 3.44.6. Pre-3.7.0 builds cannot hit
 * the race and are treated as safe.
 */
export function isSqliteWalResetVulnerable(
	v: [number, number, number],
): boolean {
	const [maj, min, pat] = v;
	if (maj !== 3) return false; // pre-WAL (<3.7.0) safe; future majors treated fixed
	if (min < 7) return false;
	if (min > 51) return false;
	if (min === 51) return pat < 3; // fixed in 3.51.3+
	if (min === 50) return pat < 7; // backport 3.50.7
	if (min === 44) return pat < 6; // backport 3.44.6
	return true;
}

const defaultSleep = (ms: number): Promise<void> =>
	new Promise<void>((resolve) => setTimeout(resolve, ms));

export interface LadderOptions {
	dbLabel?: string;
	/** Raw `database.journal_mode` operator setting (undefined = default wal). */
	operatorMode?: unknown;
	requireWal?: boolean;
	trace?: string[];
	sleep?: (ms: number) => Promise<void> | void;
	/** Override linked-SQLite version for tests (default: queried from port). */
	versionString?: string;
}

// Per-label WARNING/ERROR dedup (hermes_state.py:_wal_fallback_warned_paths).
const warnedLabels = new Set<string>();

export function resetWalWarningsForTests(): void {
	warnedLabels.clear();
}

export function walWarningCount(): number {
	return warnedLabels.size;
}

function logWalFallbackOnce(label: string, kind: string, detail: string): void {
	const key = `${label}:${kind}`;
	if (warnedLabels.has(key)) return;
	warnedLabels.add(key);
	// Hermes logs at ERROR here — degradation to DELETE is a real concurrency loss.
	console.error(`[wal-ladder:${label}] ${kind}: ${detail}`);
}

/**
 * Read-only probe of the on-disk header BEFORE any journal_mode write
 * (hermes_state.py:_on_disk_journal_mode). Retries transient "disk i/o error"
 * reads (4 attempts); returns null when the mode cannot be determined.
 */
export async function onDiskJournalMode(
	port: PragmaPort,
	sleep: (ms: number) => Promise<void> | void = defaultSleep,
): Promise<string | null> {
	for (let attempt = 0; attempt < 4; attempt++) {
		try {
			const v = port.get("journal_mode");
			if (v === null) return null;
			const mode = normalizePragmaValue(v);
			return mode === "" ? null : mode;
		} catch (err) {
			if (!errMessage(err).toLowerCase().includes("disk i/o error")) {
				return null;
			}
			await sleep(50);
		}
	}
	return null;
}

/** Best-effort journal_size_limit=64MiB after WAL success; never raises. */
export function applyWalSizeLimit(port: PragmaPort): void {
	try {
		port.set(`journal_size_limit = ${WAL_SIZE_LIMIT_BYTES}`);
	} catch {
		/* disk-slack only; must not break open */
	}
}

/**
 * Darwin-only synchronous=FULL / checkpoint_fullfsync hardening
 * (_apply_macos_checkpoint_barrier + _enforce_macos_synchronous_full).
 * No-op elsewhere (platform-guarded exactly like the reference).
 */
function applyMacosPragmas(port: PragmaPort): void {
	if (process.platform !== "darwin") return;
	try {
		port.set("synchronous = FULL");
	} catch {
		/* best-effort */
	}
	try {
		port.set("checkpoint_fullfsync = 1");
	} catch {
		/* best-effort */
	}
}

/**
 * The ONLY place a journal-mode switch to a non-WAL target may be issued
 * (hermes_state.py:_set_journal_mode_no_wait). Forces busy_timeout=0 so any
 * concurrent opener makes the pragma fail immediately instead of waiting out
 * the timeout and sneaking a flip between another writer's transactions.
 * Callers treat a raised error as "not exclusively owned: leave mode alone".
 */
export async function setJournalModeNoWait(
	port: PragmaPort,
	mode: "DELETE",
): Promise<string> {
	let previousTimeout: number | null = null;
	try {
		const prev = port.get("busy_timeout");
		previousTimeout = typeof prev === "number" ? prev : null;
	} catch {
		previousTimeout = null;
	}
	port.set("busy_timeout = 0");
	try {
		const row = port.set(`journal_mode = ${mode}`);
		return normalizePragmaValue(row);
	} finally {
		try {
			port.set(`busy_timeout = ${previousTimeout ?? 0}`);
		} catch {
			/* restore is best-effort */
		}
	}
}

/**
 * The ladder itself — port of hermes_state.py:apply_wal_with_fallback
 * (02 §1.1). Returns the journal mode actually in effect ("wal"/"delete").
 *
 * Step order: operator setting → #70055 vulnerable-SQLite gate → read-only
 * probe (already-WAL ⇒ keep without issuing the set-pragma) → configured
 * delete (verified via no-wait setter) → WAL attempt trusting the RETURNED ROW
 * (silent refusal shape) → guarded DELETE fallback that never downgrades a DB
 * it does not exclusively own.
 */
export async function applyWalWithFallback(
	port: PragmaPort,
	opts: LadderOptions = {},
): Promise<JournalMode> {
	const dbLabel = opts.dbLabel ?? "state.db";
	const trace = opts.trace;
	const sleep = opts.sleep ?? defaultSleep;
	const step = (s: string): void => {
		trace?.push(s);
	};

	// Step 1 — operator setting first (resolve_journal_mode).
	const configured = resolveJournalMode(opts.operatorMode);
	step(`operator:${configured}`);

	const version =
		opts.versionString !== undefined
			? parseSqliteVersion(opts.versionString)
			: parseSqliteVersion(port.sqliteVersion());

	// Step 2 — WAL-reset corruption bug gate (#70055). Vulnerable runtimes never
	// get NEW WAL databases; an already-WAL on-disk DB is never live-downgraded.
	if (version && isSqliteWalResetVulnerable(version)) {
		step("vuln-gate:hit");
		const requireDelete = configured === "delete";
		const current = await onDiskJournalMode(port, sleep);
		if (current === "wal") {
			step("vuln:kept-wal");
			logWalFallbackOnce(
				dbLabel,
				"wal-reset-vuln",
				"vulnerable SQLite but DB already WAL — keeping WAL (never live-downgrade)",
			);
			applyWalSizeLimit(port);
			applyMacosPragmas(port);
			return "wal";
		}
		if (current === null) {
			// Never flip a journal mode we cannot even read (indeterminate).
			if (requireDelete) {
				throw new Error(
					"could not verify journal mode before applying configured journal_mode=delete " +
						"(database is locked — possible concurrent openers); refusing to downgrade " +
						"a database this process does not exclusively own",
				);
			}
			step("vuln:indeterminate");
			logWalFallbackOnce(
				dbLabel,
				"wal-reset-vuln",
				"journal mode unreadable under vulnerable SQLite — leaving as-is",
			);
			return "wal";
		}
		let actual = "";
		try {
			actual = await setJournalModeNoWait(port, "DELETE");
		} catch (err) {
			if (requireDelete) throw err;
			if (isLockedOrBusy(err)) {
				// A concurrent opener appeared between probe and flip: leave mode alone.
				step("vuln:indeterminate");
				logWalFallbackOnce(
					dbLabel,
					"wal-reset-vuln",
					`DELETE blocked by concurrent opener (${errMessage(err)}) — keeping ${current}`,
				);
				return current === "wal" ? "wal" : "delete";
			}
			// Best-effort: fresh file-backed DBs are usually already DELETE.
		}
		if (requireDelete && actual !== "delete") {
			throw new Error(
				`could not set configured journal_mode=delete (got ${actual || "no result"})`,
			);
		}
		step("vuln:delete");
		logWalFallbackOnce(
			dbLabel,
			"wal-reset-vuln",
			"vulnerable SQLite — fresh/non-WAL database kept on DELETE",
		);
		return "delete";
	}
	step("vuln-gate:clear");

	// Step 3 — read-only probe before any journal_mode write. If already WAL,
	// keep WAL without issuing the set-pragma (no sidecar unlink under live openers).
	const currentMode = await onDiskJournalMode(port, sleep);
	step(`probe:${currentMode ?? "unknown"}`);
	if (currentMode === "wal") {
		step("kept-wal-by-probe");
		applyWalSizeLimit(port);
		applyMacosPragmas(port);
		return "wal";
	}

	// Canonical database.journal_mode=delete request: existing WAL DBs were
	// returned above and are never live-downgraded. An unreadable probe means we
	// cannot prove exclusive ownership — refuse rather than downgrade.
	if (configured === "delete") {
		if (currentMode === null) {
			throw new Error(
				"could not verify journal mode before applying configured journal_mode=delete " +
					"(database is locked — possible concurrent openers); refusing to downgrade " +
					"a database this process does not exclusively own",
			);
		}
		const actual = await setJournalModeNoWait(port, "DELETE");
		step("operator-delete:set");
		if (actual !== "delete") {
			throw new Error(
				`could not set configured journal_mode=delete (got ${actual || "no result"})`,
			);
		}
		return "delete";
	}

	// Step 4 — attempt WAL; TRUST THE RETURNED ROW (silent-refusal shape on
	// macOS NFS/SMB/FUSE returns the still-effective mode without raising).
	try {
		step("wal:attempt");
		const row = port.set("journal_mode = WAL");
		const mode = normalizePragmaValue(row);
		if (mode === "wal") {
			step("wal:ok");
			applyWalSizeLimit(port);
			applyMacosPragmas(port);
			return "wal";
		}
		const silent = new WalUnsupportedError(
			`journal_mode=WAL refused without raising (still ${mode || "unknown"})`,
		);
		if (opts.requireWal) throw silent;
		step(`wal:silent-refusal:${mode}`);
		logWalFallbackOnce(dbLabel, "wal-fallback", silent.message);
		return mode === "delete" ? "delete" : ((mode || "delete") as JournalMode);
	} catch (err) {
		// A requireWal silent-refusal raise propagates unchanged.
		if (isWalUnsupportedError(err)) throw err;
		if (!isWalIncompatError(err)) throw err; // unrelated OperationalError — re-raise

		// "disk i/o error" is ambiguous: deterministic WAL-incompatibility (ZFS SHM)
		// vs one-shot transient EIO. Retry twice; transient clears, deterministic
		// keeps failing into the guarded DELETE fallback.
		if (errMessage(err).toLowerCase().includes("disk i/o error")) {
			for (let retry = 1; retry <= 2; retry++) {
				await sleep(50);
				try {
					const row = port.set("journal_mode = WAL");
					const mode = normalizePragmaValue(row);
					if (mode === "wal") {
						step(`wal:ioerr-retry:${retry}`);
						step("wal:ok");
						applyWalSizeLimit(port);
						applyMacosPragmas(port);
						return "wal";
					}
					break;
				} catch (retryErr) {
					if (!errMessage(retryErr).toLowerCase().includes("disk i/o error")) {
						throw retryErr;
					}
				}
			}
		}

		// Guarded DELETE fallback: never downgrade if another process already set
		// WAL, or the mode cannot be verified (probe blocked by concurrent locks).
		const existing = await onDiskJournalMode(port, sleep);
		step(`probe:${existing ?? "unknown"}`);
		if (existing === "wal" || existing === null) throw err;
		if (opts.requireWal) {
			throw new WalUnsupportedError(errMessage(err));
		}
		step("fallback:guarded-delete");
		logWalFallbackOnce(dbLabel, "wal-fallback", errMessage(err));
		await setJournalModeNoWait(port, "DELETE");
		return "delete";
	}
}

export interface OpenDatabaseOptions {
	path: string;
	readOnly?: boolean;
	/** Default 5000ms — busy/timeout contention surfaces to app-level retry. */
	busyTimeoutMs?: number;
	operatorJournalMode?: unknown;
	requireWal?: boolean;
	trace?: string[];
}

/**
 * Open helper implementing 02 §1.1: WAL journal mode with the documented
 * fallback ladder, busy_timeout set, manual transaction control (callers issue
 * BEGIN IMMEDIATE explicitly — parity of isolation_level=None). Read-only
 * opens never DDL and never flip journal modes (02 §3).
 */
export async function openDatabase(opts: OpenDatabaseOptions): Promise<{
	db: Database.Database;
	journalMode: JournalMode;
}> {
	const db = new Database(opts.path, {
		readonly: opts.readOnly === true,
	});
	db.pragma(`busy_timeout = ${opts.busyTimeoutMs ?? 5000}`);
	if (opts.readOnly === true) {
		return { db, journalMode: "wal" };
	}
	const port = new RealPragmaPort(db);
	const ladderOpts: LadderOptions = { dbLabel: "state.db" };
	if (opts.operatorJournalMode !== undefined) {
		ladderOpts.operatorMode = opts.operatorJournalMode;
	}
	if (opts.requireWal !== undefined) ladderOpts.requireWal = opts.requireWal;
	if (opts.trace !== undefined) ladderOpts.trace = opts.trace;
	const journalMode = await applyWalWithFallback(port, ladderOpts);
	return { db, journalMode };
}

export interface PatienceOptions {
	/** Total time budget for lock retries (default 20s, hermes routine-write patience). */
	patienceMs?: number;
	retryMinMs?: number; // 20
	retryMaxMs?: number; // 150
	slowAfterMs?: number; // 2000
	slowMinMs?: number; // 250
	slowMaxMs?: number; // 1000
	now?: () => number;
	sleep?: (ms: number) => Promise<void> | void;
	random?: () => number;
	onRetry?: (err: unknown, attempt: number, elapsedMs: number) => void;
}

/**
 * Shared two-band jittered-patience retry around ANY body that throws the
 * busy/locked class (hermes_state.py:SessionDB._execute_write schedule).
 * Contention surfaces immediately because BEGIN IMMEDIATE takes the WAL write
 * lock at START; on retry the caller sleeps random jitter, breaking SQLite's
 * deterministic-backoff convoy. Patience is TIME-based, never attempt-counted.
 * Terminal failure rethrows the last busy error.
 */
export async function runWithJitteredPatience<T>(
	body: () => T | Promise<T>,
	opts: PatienceOptions = {},
): Promise<T> {
	const now = opts.now ?? Date.now;
	const sleep = opts.sleep ?? defaultSleep;
	const rand = opts.random ?? Math.random;
	const patienceMs = opts.patienceMs ?? 20_000;
	const retryMinMs = opts.retryMinMs ?? 20;
	const retryMaxMs = opts.retryMaxMs ?? 150;
	const slowAfterMs = opts.slowAfterMs ?? 2_000;
	const slowMinMs = opts.slowMinMs ?? 250;
	const slowMaxMs = opts.slowMaxMs ?? 1_000;

	const start = now();
	let attempt = 0;
	for (;;) {
		attempt++;
		try {
			return await body();
		} catch (err) {
			const retryable =
				isLockedOrBusy(err) ||
				errMessage(err).toLowerCase().includes("no more rows available");
			if (!retryable) throw err;
			opts.onRetry?.(err, attempt, now() - start);
			const elapsed = now() - start;
			if (elapsed >= patienceMs) throw err; // terminal: patience exhausted
			const chosen =
				elapsed < slowAfterMs
					? retryMinMs + (retryMaxMs - retryMinMs) * rand()
					: slowMinMs + (slowMaxMs - slowMinMs) * rand();
			const remaining = start + patienceMs - now();
			await sleep(Math.max(1, Math.min(chosen, remaining)));
		}
	}
}

export interface ExecuteWriteOptions extends PatienceOptions {}

/**
 * BEGIN IMMEDIATE transaction with the jittered-patience retry ladder.
 * `fn` runs inside the transaction; any thrown error rolls back and either
 * retried (busy class) or propagated. Nested transactions are NOT supported —
 * compose multiple statements into one fn instead.
 */
export async function executeWrite<T>(
	db: Database.Database,
	fn: (db: Database.Database) => T,
	opts: ExecuteWriteOptions = {},
): Promise<T> {
	return runWithJitteredPatience(() => {
		db.exec("BEGIN IMMEDIATE");
		try {
			const result = fn(db);
			db.exec("COMMIT");
			return result;
		} catch (err) {
			try {
				db.exec("ROLLBACK");
			} catch {
				/* no active tx — BEGIN itself failed */
			}
			throw err;
		}
	}, opts);
}
