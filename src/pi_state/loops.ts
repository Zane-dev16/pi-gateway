// pi_state/loops.ts — persisted /loop rows: recurring in-session wakeups
// stored ONE PER SESSION in state_meta under `loop:<session_id>`.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/loops.py:LoopState                    → LoopState (+JSON codec)
//   hermes_cli/loops.py:_META_PREFIX/_meta_key       → LOOP_META_PREFIX/loopMetaKey
//   hermes_cli/loops.py:load_loop/save_loop          → loadLoopRow/saveLoopRow
//   hermes_cli/loops.py:clear_loop                   → clearLoopRow (audit-preserving)
//   hermes_cli/loops.py:list_active_loops            → listActiveLoopRows
//   hermes_state.py:SessionDB.list_meta_prefix       → listMetaPrefix
//   hermes_cli/loops.py:migrate_loop_to_session      → migrateLoopRowToSession
//
// Storage contract (hermes_cli/loops.py "Persistence (SessionDB state_meta)"):
// rows live in state_meta keyed `loop:<session_id>`; the value is the full
// serialized LoopState (snake_case field names — BYTE-FORMAT PARITY with
// Hermes rows so a shared state.db round-trips across both runtimes).
// `/resume` picks the loop back up after restarts; `cleared` rows are kept
// for audit. Writes ride the standard BEGIN IMMEDIATE ladder (wal.ts); reads
// are synchronous probes that degrade to null/[] when the row is absent or
// unparseable — never throw into the caller.

import type Database from "better-sqlite3";

import { getMeta, setMeta } from "./reconcile.js";
import { executeWrite, type ExecuteWriteOptions } from "./wal.js";

/** hermes_cli/loops.py:_META_PREFIX. */
export const LOOP_META_PREFIX = "loop:";

/** hermes_cli/loops.py:DEFAULT_MAX_TICKS — config backstop baked into rows. */
export const LOOP_DEFAULT_MAX_TICKS = 100;

/** Loop cadence mode (Claude Code parity): fixed interval | self-paced. */
export type LoopMode = "interval" | "self_paced";

/** Loop lifecycle status. */
export type LoopStatus = "active" | "paused" | "done" | "cleared";

/**
 * Gateway routing captured at creation (platform/chat/thread/user), so the
 * idle wakeup watcher can inject ticks back into the right chat after a
 * restart. Empty route = CLI/TUI-owned loop (their own surfaces drive it).
 * Keys are Hermes snake_case wire names — this dict round-trips verbatim.
 */
export type LoopRoute = Record<string, string>;

/** Serializable /loop state stored per session (loops.py:LoopState parity). */
export interface LoopState {
	prompt: string;
	status: LoopStatus;
	mode: LoopMode;
	/** Fixed cadence seconds (mode === "interval"). */
	intervalSeconds: number;
	/** Live cadence seconds (self-paced backoff; fixed mode mirrors interval). */
	currentDelay: number;
	/** User cap (--times N); 0 = none. */
	times: number;
	/** Judged stop condition (--until); "" = none. */
	until: string;
	/** Config backstop at creation; 0 = unlimited. */
	maxTicks: number;
	ticksFired: number;
	createdAt: number;
	lastFiredAt: number;
	nextDueAt: number;
	/**
	 * True between "wakeup injected" and "that turn's response evaluated".
	 * Keeps a tick from double-firing while its turn runs.
	 */
	awaitingResponse: boolean;
	/** Self-paced change detection: digest of the previous wakeup's reply. */
	lastResponseDigest: string;
	pausedReason: string | null;
	lastStopReason: string | null;
	route: LoopRoute;
}

/** hermes_cli/loops.py:_meta_key. */
export function loopMetaKey(sessionId: string): string {
	return `${LOOP_META_PREFIX}${sessionId}`;
}

/**
 * Serialize to the EXACT Hermes wire format: snake_case keys, route verbatim,
 * numeric floats as-is. loops.py:LoopState.to_json parity (json.dumps of the
 * dataclass asdict).
 */
export function loopStateToJson(state: LoopState): string {
	return JSON.stringify({
		prompt: state.prompt,
		status: state.status,
		mode: state.mode,
		interval_seconds: state.intervalSeconds,
		current_delay: state.currentDelay,
		times: state.times,
		until: state.until,
		max_ticks: state.maxTicks,
		ticks_fired: state.ticksFired,
		created_at: state.createdAt,
		last_fired_at: state.lastFiredAt,
		next_due_at: state.nextDueAt,
		awaiting_response: state.awaitingResponse,
		last_response_digest: state.lastResponseDigest,
		paused_reason: state.pausedReason,
		last_stop_reason: state.lastStopReason,
		route: state.route,
	});
}

/**
 * Python `float(data.get(key, d)) or 0.0` / `int(... or 0)` chain parity:
 * MISSING key substitutes its default first, then any FALSY value (0, "",
 * null, false) collapses to 0, and every other value is coerced like
 * float()/int() — garbage THROWS so the caller's corrupt-row arm degrades the
 * whole row to absent (load_loop warns + returns None; it never resurrects a
 * partial loop).
 */
function pyNumberField(
	raw: unknown,
	missingDefault: number,
	label: string,
): number {
	const v = raw === undefined ? missingDefault : raw;
	if (!v) return 0;
	if (typeof v === "number") {
		if (!Number.isFinite(v)) {
			throw new TypeError(`${label}: not a finite number`);
		}
		return v;
	}
	if (typeof v === "boolean") return v ? 1 : 0;
	if (typeof v === "string") {
		const n = Number(v.trim());
		if (v.trim() === "" || !Number.isFinite(n)) {
			throw new TypeError(`${label}: could not convert ${JSON.stringify(v)}`);
		}
		return n;
	}
	throw new TypeError(`${label}: unsupported value ${String(v)}`);
}

/**
 * Tolerant decode (loops.py:LoopState.from_json parity): every field carries
 * its default; mistyped values fall back like Python's float()/int()/bool()
 * coercions over `or 0` chains. A non-dict route degrades to {}.
 */
export function loopStateFromJson(raw: string): LoopState {
	const data = JSON.parse(raw) as Record<string, unknown>;
	const routeRaw = data["route"];
	const route: LoopRoute = {};
	if (
		routeRaw !== null &&
		typeof routeRaw === "object" &&
		!Array.isArray(routeRaw)
	) {
		for (const [k, v] of Object.entries(routeRaw as Record<string, unknown>)) {
			route[k] = String(v);
		}
	}
	const pausedReason = data["paused_reason"];
	const lastStopReason = data["last_stop_reason"];
	const rawStatus = data["status"];
	const rawMode = data["mode"];
	return {
		prompt: String(data["prompt"] ?? ""),
		status:
			rawStatus === undefined || rawStatus === null
				? "active"
				: (String(rawStatus) as LoopStatus),
		mode:
			rawMode === undefined || rawMode === null
				? "interval"
				: (String(rawMode) as LoopMode),
		intervalSeconds: pyNumberField(
			data["interval_seconds"],
			0,
			"interval_seconds",
		),
		currentDelay: pyNumberField(data["current_delay"], 0, "current_delay"),
		times: Math.trunc(pyNumberField(data["times"], 0, "times")),
		until: String(data["until"] ?? ""),
		maxTicks: Math.trunc(
			pyNumberField(data["max_ticks"], LOOP_DEFAULT_MAX_TICKS, "max_ticks"),
		),
		ticksFired: Math.trunc(
			pyNumberField(data["ticks_fired"], 0, "ticks_fired"),
		),
		createdAt: pyNumberField(data["created_at"], 0, "created_at"),
		lastFiredAt: pyNumberField(data["last_fired_at"], 0, "last_fired_at"),
		nextDueAt: pyNumberField(data["next_due_at"], 0, "next_due_at"),
		awaitingResponse: Boolean(data["awaiting_response"]),
		lastResponseDigest: String(data["last_response_digest"] ?? ""),
		pausedReason:
			pausedReason === null || pausedReason === undefined
				? null
				: String(pausedReason),
		lastStopReason:
			lastStopReason === null || lastStopReason === undefined
				? null
				: String(lastStopReason),
		route,
	};
}

// ---------------------------------------------------------------------------
// Row access (loops.py:load_loop / save_loop / clear_loop / list_active_loops)
// ---------------------------------------------------------------------------

/**
 * Load the loop for a session, or null when none exists / the row is
 * unparseable (load_loop parity: corrupt rows warn and read as absent).
 */
export function loadLoopRow(
	db: Database.Database,
	sessionId: string,
): LoopState | null {
	if (!sessionId) return null;
	let raw: string | null = null;
	try {
		raw = getMeta(db, loopMetaKey(sessionId));
	} catch {
		return null; // get_meta failed parity
	}
	if (!raw) return null;
	try {
		return loopStateFromJson(raw);
	} catch (err) {
		console.warn(
			`[pi_state] could not parse stored loop for ${sessionId}: ${String(err)}`,
		);
		return null;
	}
}

/**
 * Persist a loop row. No-op when sessionId is empty (save_loop parity).
 * Rides the standard contended-write ladder.
 */
export function saveLoopRow(
	db: Database.Database,
	sessionId: string,
	state: LoopState,
	opts?: ExecuteWriteOptions,
): Promise<void> {
	if (!sessionId) return Promise.resolve();
	return executeWrite(
		db,
		(conn) =>
			void setMeta(conn, loopMetaKey(sessionId), loopStateToJson(state)),
		opts,
	);
}

/**
 * Mark a loop cleared in the DB (preserved for audit, status='cleared').
 * Returns false when there was no live row to clear (clear_loop returns
 * nothing; the boolean is pi's honest-ack extension for dispatchers).
 */
export async function clearLoopRow(
	db: Database.Database,
	sessionId: string,
	opts?: ExecuteWriteOptions,
): Promise<boolean> {
	const state = loadLoopRow(db, sessionId);
	if (state === null) return false;
	state.status = "cleared";
	await saveLoopRow(db, sessionId, state, opts);
	return true;
}

/**
 * hermes_state.py:SessionDB.list_meta_prefix — [(key, value), …] for
 * state_meta keys under a literal prefix. LIKE wildcards in the prefix are
 * escaped (parity: `%`/`_`/`\` in session ids must match literally).
 */
export function listMetaPrefix(
	db: Database.Database,
	prefix: string,
): Array<[string, string]> {
	if (!prefix) return [];
	const escaped = prefix
		.replace(/\\/g, "\\\\")
		.replace(/%/g, "\\%")
		.replace(/_/g, "\\_");
	const rows = db
		.prepare("SELECT key, value FROM state_meta WHERE key LIKE ? ESCAPE '\\'")
		.all(`${escaped}%`) as Array<{ key: string; value: string | null }>;
	return rows.map((row) => [row.key, row.value ?? ""]);
}

/**
 * [(sessionId, LoopState)] for every ACTIVE loop row — the gateway wakeup
 * watcher's scan input. Best-effort: unreadable/corrupt rows are skipped,
 * DB failure yields [] (list_active_loops parity).
 */
export function listActiveLoopRows(
	db: Database.Database,
): Array<[string, LoopState]> {
	let rows: Array<[string, string]>;
	try {
		rows = listMetaPrefix(db, LOOP_META_PREFIX);
	} catch {
		return [];
	}
	const out: Array<[string, LoopState]> = [];
	for (const [key, raw] of rows) {
		const sessionId = key.slice(LOOP_META_PREFIX.length);
		if (!sessionId || !raw) continue;
		let state: LoopState;
		try {
			state = loopStateFromJson(raw);
		} catch {
			continue;
		}
		if (state.status === "active") out.push([sessionId, state]);
	}
	return out;
}

/**
 * Carry a persistent /loop from a parent session to its continuation
 * (#33618 class: context compression rotates session_id — without this the
 * loop silently dies at the compaction boundary). Copies the row onto the
 * new session and archives the old one as `cleared` so exactly ONE active
 * loop row exists per logical conversation. Best-effort and never throws.
 */
export async function migrateLoopRowToSession(
	db: Database.Database,
	oldSessionId: string,
	newSessionId: string,
	reason = "",
): Promise<boolean> {
	if (!oldSessionId || !newSessionId || oldSessionId === newSessionId) {
		return false;
	}
	try {
		const state = loadLoopRow(db, oldSessionId);
		if (state === null || state.status === "cleared") return false;
		if (loadLoopRow(db, newSessionId) !== null) return false;
		await saveLoopRow(db, newSessionId, state);
		await clearLoopRow(db, oldSessionId);
		console.debug?.(
			`[pi_state] migrated loop ${oldSessionId} -> ${newSessionId} (${reason || "rotation"})`,
		);
		return true;
	} catch {
		return false;
	}
}
