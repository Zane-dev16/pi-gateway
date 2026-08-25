// pi_gateway/lifecycle/shutdown.ts — graceful shutdown: signal classes,
// ordered drain, flush-before-clear backstop, recovery files.
//
// Spec: /root/pi-gateway/08-operations.md §1.2 (ordered drain + signal
// semantics), §1.3(a) (flush → data-loss net), 01-architecture.md §3.4.
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/run.py:shutdown_signal_handler
//     → classifySignalForSelf (takeover consume FIRST, then SIGINT /
//       planned-stop marker; unexpected ⇒ flag mirrored BEFORE teardown)
//   run.py:_signal_initiated_shutdown / #42675
//     → unexpected signals must NOT persist gateway_state="stopped"
//   gateway/shutdown_flush.py:flush_pending_to_file / recover_pending_to_db
//     → flushPendingToFiles / recoverPendingToDb (atomic per-message JSON,
//       mode 0600, dir fsync'd on POSIX, invalid files PRESERVED never deleted)
//   shutdown_flush.py:flush_agent_history_to_file
//     → flushAgentHistoryToFile (operator-recovery schema; auto-recovery skips)
//   §1.2 "Second signal escalates to fast-exit" + §1.3(c) watchdog ordering
//     → escalateFastExit releases PID file + runtime lock BEFORE hard exit
//   run.py:_stop_impl_body
//     → notify_active_sessions + pre-drain mark_resume_pending run BEFORE
//       teardown completes (adapters still connected); .clean_shutdown marker
//       written ONLY on non-timed-out drains; .restart_failure_counts
//       incremented for sessions active at shutdown (#7536 stuck-loop input)
//
// Binding drain order (08 §1.2): stop ingress → active turns within grace →
// release leases → flush delivery obligations + token rollups + pending
// messages → close DB → exit non-zero only on failed flushes. The notify /
// pre-drain resume-pending phases PRECEDE stop_ingress (Hermes notifies while
// adapters are still connected); the clean-shutdown marker and restart-failure
// counts follow close_database exactly as _stop_impl_body orders them.
// Binding backstop order (§1.3(a)): pending-message flush (and its recovery-
// file fallback) runs BEFORE any in-memory slot clear.

import {
	closeSync,
	existsSync,
	fsyncSync,
	mkdirSync,
	openSync,
	readFileSync,
	readdirSync,
	renameSync,
	rmSync,
	unlinkSync,
	writeFileSync,
} from "node:fs";
import { randomUUID } from "node:crypto";
import { join } from "node:path";
import { insertMessageInTx } from "../../pi_state/messages.js";
import type Database from "better-sqlite3";
import {
	clearPlannedStopMarker,
	consumePlannedStopMarkerForSelf,
	consumeTakeoverMarkerForSelf,
} from "./markers.js";

/**
 * Shutdown classes (08 §1.2 + gateway/restart.py supervisor contract):
 * - takeover / planned_stop exit 0 (systemd Restart=on-failure must not
 *   flap-fight a replacer or an operator stop);
 * - service_restart exits 75 (EX_TEMPFAIL — SIGUSR1-initiated in-band
 *   restart, loop-liveness hard-exit; asks the supervisor to REPLACE us,
 *   run.py:restart_signal_handler → request_restart(via_service=True));
 * - fatal_config exits 78 (EX_CONFIG — s6 RestartPreventExitStatus stops
 *   restarting a fatally misconfigured gateway, #51228);
 * - unexpected_signal exits 1 so the supervisor revives us.
 */
export type ShutdownClass =
	| "takeover"
	| "planned_stop"
	| "service_restart"
	| "fatal_config"
	| "unexpected_signal";

/**
 * Exit-code discipline (08 §1.2): planned takeover exits 0 so systemd's
 * Restart=on-failure doesn't flap-fight the replacer; planned stop exits 0;
 * an unexpected external signal exits non-zero so the supervisor revives us.
 */
export const SHUTDOWN_EXIT_CODES: Record<ShutdownClass, number> = {
	takeover: 0,
	planned_stop: 0,
	service_restart: 75,
	fatal_config: 78,
	unexpected_signal: 1,
};

/** True only when the class may persist gateway_state="stopped" (#42675). */
export function persistsStopped(klass: ShutdownClass): boolean {
	return klass !== "unexpected_signal";
}

/**
 * Signal classification parity of run.py:shutdown_signal_handler — the
 * takeover marker is consumed FIRST regardless of signal name; SIGINT is an
 * intentional foreground stop by definition; otherwise the planned-stop
 * marker decides; a bare unmarked signal is UNEXPECTED.
 *
 * `signalName` null means a programmatic stop (treated like a marked stop).
 */
/**
 * Signal classification parity of run.py:shutdown_signal_handler (+ its
 * sibling restart_signal_handler): the takeover marker is consumed FIRST
 * regardless of signal name; SIGINT is an intentional foreground stop by
 * definition; SIGUSR1 IS the in-band restart signal by definition (the pi
 * fleet updater drains gateways via SIGUSR1 first — 08 §7 — so it classifies
 * as service_restart and exits 75); otherwise the planned-stop marker
 * decides; a bare unmarked signal is UNEXPECTED.
 *
 * `signalName` null means a programmatic stop (treated like a marked stop).
 */
export function classifySignalForSelf(
	home: string,
	signalName: string | null,
	options: { selfPid?: number } = {},
): ShutdownClass {
	const opts = { selfPid: options.selfPid ?? process.pid };
	if (consumeTakeoverMarkerForSelf(home, opts)) return "takeover";
	if (signalName === "SIGINT") return "planned_stop";
	if (signalName === "SIGUSR1") return "service_restart";
	if (signalName === null || consumePlannedStopMarkerForSelf(home, opts)) {
		return "planned_stop";
	}
	return "unexpected_signal";
}

// ---------------------------------------------------------------------------
// Drain sequence
// ---------------------------------------------------------------------------

export interface DrainStepTrace {
	step: string;
	ok: boolean;
	error?: string;
	detail?: Record<string, unknown>;
}

export type DrainPhase =
	| "notify_active_sessions"
	| "pre_drain_resume_pending"
	| "stop_ingress"
	| "active_turns_grace"
	| "release_leases"
	| "flush_delivery_obligations"
	| "flush_token_rollups"
	| "finalize_agent_histories"
	| "flush_pending_messages"
	| "clear_pending_slots"
	| "close_database"
	| "write_clean_shutdown_marker"
	| "increment_restart_failure_counts"
	| "persist_exit_status";

export const DRAIN_PHASE_ORDER: readonly DrainPhase[] = [
	"notify_active_sessions",
	"pre_drain_resume_pending",
	"stop_ingress",
	"active_turns_grace",
	"release_leases",
	"flush_delivery_obligations",
	"flush_token_rollups",
	"finalize_agent_histories",
	"flush_pending_messages",
	"clear_pending_slots",
	"close_database",
	"write_clean_shutdown_marker",
	"increment_restart_failure_counts",
	"persist_exit_status",
];

export interface DrainHooks {
	/** Notify chats with active agents BEFORE anything tears down — adapters
	 *  are still connected here, so messages can be sent (run.py:
	 *  _stop_impl_body notify phase). */
	notifyActiveSessions?: () => Promise<void>;
	/** Durable pre-drain resume-pending marks (#27856): if the process is
	 *  killed mid-drain, the marker is already written. Returns the marked
	 *  session keys (cleared again when the drain completes gracefully). */
	markResumePendingPreDrain?: () => Promise<readonly string[]>;
	/** Adapters stop polling/reading (08 §1.2 step 1). Embedded background
	 *  services join here until Phase 5 gives them cooperative-drain contracts. */
	stopIngress?: () => Promise<void>;
	/** Active turns finish within a grace window. Returns TRUE when the window
	 *  expired with turns still active (drain TIMED OUT) — void/false is a
	 *  graceful completion. */
	awaitActiveTurns?: (
		graceMs: number,
	) => Promise<boolean | void> | boolean | void;
	releaseLeases?: () => Promise<void>;
	flushDeliveryObligations?: () => Promise<void>;
	/** Token-rollup flush barrier (02 §7.2) — callers MUST run before model
	 *  switches; at shutdown it drains the coalescing writer. */
	flushTokenRollups?: () => Promise<void>;
	/** May RAISE when persistence is broken — the controller snapshots agent
	 *  transcripts to operator-recovery files and continues (§1.3a). */
	flushAgentHistories?: () => Promise<void>;
	/** Final DB close. */
	closeDatabase?: () => Promise<void>;
	/** Sessions still active at teardown time — restart-failure counting input
	 *  (#7536 stuck-loop detection across restarts). */
	activeSessionKeys?: () => readonly string[];
	/** Graceful-completion hook: clear the pre-drain resume-pending marks for
	 *  sessions that finished inside the window (only runs when !timedOut). */
	clearResumePending?: (keys: readonly string[]) => Promise<void>;
	/** Status persist for the exit transition. */
	persistExitStatus?: (outcome: DrainOutcome) => Promise<void>;
}

export interface DrainOutcome {
	klass: ShutdownClass;
	exitCode: number;
	/** Whether gateway_state="stopped" was persisted (false for unexpected). */
	persistedStopped: boolean;
	/** True ONLY when data could reach neither the DB nor a recovery file. */
	flushesFailed: boolean;
	/** True when active turns were still running when the grace window closed.
	 *  Gates the clean-shutdown marker (timed-out drains skip it so the next
	 *  boot suspends half-finished tool loops, _stop_impl_body parity). */
	timedOut: boolean;
	/** Whether `.clean_shutdown` was written (non-timed-out drains only). */
	cleanShutdownWritten: boolean;
	trace: DrainStepTrace[];
	escalated: boolean;
}

interface DrainOptions {
	home: string;
	klass: ShutdownClass;
	graceMs: number;
	hooks: DrainHooks;
	/** In-memory pending-message slots owned by the lifecycle. */
	takePendingSlots: () => unknown[];
	log: Logger;
	nowMs?: () => number;
}

export interface Logger {
	info(message: string, meta?: Record<string, unknown>): void;
	warn(message: string, meta?: Record<string, unknown>): void;
	error(message: string, meta?: Record<string, unknown>): void;
}

const STEP_FOR_PHASE: Record<DrainPhase, string> = {
	notify_active_sessions: "notify_active_sessions",
	pre_drain_resume_pending: "pre_drain_resume_pending",
	stop_ingress: "stop_ingress",
	active_turns_grace: "active_turns_grace",
	release_leases: "release_leases",
	flush_delivery_obligations: "flush_delivery_obligations",
	flush_token_rollups: "flush_token_rollups",
	finalize_agent_histories: "finalize_agent_histories",
	flush_pending_messages: "flush_pending_messages",
	clear_pending_slots: "clear_pending_slots",
	close_database: "close_database",
	write_clean_shutdown_marker: "write_clean_shutdown_marker",
	increment_restart_failure_counts: "increment_restart_failure_counts",
	persist_exit_status: "persist_exit_status",
};

async function runStep(
	trace: DrainStepTrace[],
	step: string,
	body: () => Promise<void>,
): Promise<boolean> {
	try {
		await body();
		trace.push({ step, ok: true });
		return true;
	} catch (err) {
		trace.push({ step, ok: false, error: String(err) });
		return false;
	}
}

/**
 * The ordered drain (08 §1.2 + §1.3(a) binding order). Every step runs even
 * after an earlier failure (best-effort teardown); ordering is what carries
 * the correctness load — flushes strictly precede the slot clear.
 */
export async function executeDrain(
	options: DrainOptions,
): Promise<DrainOutcome> {
	const trace: DrainStepTrace[] = [];
	let pendingFlushFailed = false;
	let agentHistoryFlushFailed = false;

	const hooks = options.hooks;

	// Notify active sessions FIRST — adapters are still connected so shutdown
	// notices actually reach chats (run.py:_notify_active_sessions_of_shutdown).
	await runStep(trace, STEP_FOR_PHASE.notify_active_sessions, async () => {
		await hooks.notifyActiveSessions?.();
	});

	// Pre-drain resume-pending marks (#27856): durable BEFORE the grace wait,
	// so a kill mid-drain still leaves recoverable sessions behind.
	let preDrainKeys: readonly string[] = [];
	await runStep(trace, STEP_FOR_PHASE.pre_drain_resume_pending, async () => {
		preDrainKeys = (await hooks.markResumePendingPreDrain?.()) ?? [];
	});

	await runStep(trace, STEP_FOR_PHASE.stop_ingress, async () => {
		await hooks.stopIngress?.();
	});
	let timedOut = false;
	await runStep(trace, STEP_FOR_PHASE.active_turns_grace, async () => {
		timedOut = (await hooks.awaitActiveTurns?.(options.graceMs)) === true;
		// Graceful completion: sessions that finished INSIDE the window must not
		// carry a stale resume-pending flag (_stop_impl_body clear_resume_pending).
		if (
			!timedOut &&
			preDrainKeys.length > 0 &&
			hooks.clearResumePending !== undefined
		) {
			await hooks.clearResumePending(preDrainKeys);
		}
	});
	await runStep(trace, STEP_FOR_PHASE.release_leases, async () => {
		await hooks.releaseLeases?.();
	});
	await runStep(trace, STEP_FOR_PHASE.flush_delivery_obligations, async () => {
		await hooks.flushDeliveryObligations?.();
	});
	await runStep(trace, STEP_FOR_PHASE.flush_token_rollups, async () => {
		await hooks.flushTokenRollups?.();
	});

	// §1.3(a) first bullet: agent-history flush raises? → best-effort snapshot
	// to an operator-recovery file (auto-recovery skips this schema).
	await runStep(trace, STEP_FOR_PHASE.finalize_agent_histories, async () => {
		try {
			await hooks.flushAgentHistories?.();
		} catch (err) {
			const snapshot = (err as { agentHistorySnapshot?: AgentHistorySnapshot })
				.agentHistorySnapshot;
			if (snapshot !== undefined) {
				flushAgentHistoryToFile(
					options.home,
					snapshot.sessionId,
					snapshot.messages,
					{
						reason: "shutdown-with-unpersisted-agent-history",
					},
				);
				options.log.error(
					"agent history failed to persist; serialized operator-recovery snapshot",
					{ session_id: snapshot.sessionId },
				);
				return; // data preserved — not a failed flush
			}
			agentHistoryFlushFailed = true; // nothing survived ⇒ failed flush
			throw err;
		}
	});

	// §1.3(a) second bullet: pending messages serialize to atomic recovery
	// files BEFORE ANY .clear() — memory holds the only surviving copy when
	// persistence is broken (#72680).
	await runStep(trace, STEP_FOR_PHASE.flush_pending_messages, async () => {
		const slots = options.takePendingSlots();
		const written = flushPendingToFiles(options.home, slots, {
			reason: "shutdown",
		});
		pendingFlushFailed = written.failed > 0;
		if (written.files.length > 0) {
			options.log.warn("pending messages flushed to recovery files", {
				count: written.files.length,
				failed: written.failed,
			});
		}
	});

	// Clear in-memory slots ONLY AFTER the flush attempts (§1.3(a)).
	await runStep(trace, STEP_FOR_PHASE.clear_pending_slots, async () => {
		/* slots were taken above — clearing is the take itself; this step pins
		   the ORDERING in the trace so contracts can assert flush < clear. */
	});

	await runStep(trace, STEP_FOR_PHASE.close_database, async () => {
		await hooks.closeDatabase?.();
	});

	// Graceful completion writes the clean-shutdown marker; a TIMED-OUT drain
	// skips it so the next boot suspends half-finished tool loops instead of
	// resuming them (_stop_impl_body: `if not timed_out: (.clean_shutdown).touch()`).
	if (!timedOut) {
		await runStep(
			trace,
			STEP_FOR_PHASE.write_clean_shutdown_marker,
			async () => {
				writeCleanShutdownMarker(options.home);
			},
		);
	}

	// Restart-failure counting (#7536): sessions active across 3+ consecutive
	// restarts get auto-suspended at next boot. Only recorded when something
	// was actually running at teardown (parity of the active_agents gate).
	await runStep(
		trace,
		STEP_FOR_PHASE.increment_restart_failure_counts,
		async () => {
			const keys = hooks.activeSessionKeys?.() ?? [];
			if (keys.length > 0) incrementRestartFailureCounts(options.home, keys);
		},
	);

	const klass = options.klass;
	const outcome: DrainOutcome = {
		klass,
		exitCode: SHUTDOWN_EXIT_CODES[klass],
		persistedStopped: false,
		flushesFailed: pendingFlushFailed || agentHistoryFlushFailed,
		timedOut,
		cleanShutdownWritten: !timedOut,
		trace,
		escalated: false,
	};
	await runStep(trace, STEP_FOR_PHASE.persist_exit_status, async () => {
		await hooks.persistExitStatus?.(outcome);
		// Mark AFTER the persist hook actually ran.
		outcome.persistedStopped = persistsStopped(klass);
	});
	return outcome;
}

// ---------------------------------------------------------------------------
// Recovery-file IO (shutdown_flush.py port)
// ---------------------------------------------------------------------------

export function recoveryDir(home: string): string {
	return join(home, "pending_messages");
}

export interface NormalizedPayload {
	sessionId: string | null;
	text: string;
	raw: unknown;
}

/**
 * Payload discipline (08 §1.3(a)): MessageEvent-shaped objects use their text
 * (+ session binding); raw strings pass through; dicts keep their JSON shape;
 * anything else degrades to its string form.
 */
export function normalizePendingPayload(raw: unknown): NormalizedPayload {
	if (typeof raw === "string") return { sessionId: null, text: raw, raw };
	if (typeof raw === "object" && raw !== null) {
		const rec = raw as Record<string, unknown>;
		const sessionId =
			typeof rec["session_id"] === "string"
				? (rec["session_id"] as string)
				: null;
		const text = typeof rec["text"] === "string" ? (rec["text"] as string) : "";
		if (sessionId !== null || text !== "") {
			return { sessionId, text, raw };
		}
		return { sessionId: null, text: JSON.stringify(raw), raw }; // dict form
	}
	return { sessionId: null, text: String(raw), raw };
}

function fsyncDirQuiet(path: string): void {
	if (process.platform === "win32") return; // no directory fsync on Windows
	try {
		const fd = openSync(path, "r");
		try {
			fsyncSync(fd);
		} finally {
			closeSync(fd);
		}
	} catch {
		/* best-effort — fsync of a read-only dir fd fails on some filesystems */
	}
}

export interface FlushResult {
	files: string[];
	failed: number;
}

/**
 * One ATOMIC JSON file per message under `<home>/pending_messages/`
 * (pending-<uuid>.json, mode 0600, directory fsync'd on POSIX).
 */
export function flushPendingToFiles(
	home: string,
	payloads: readonly unknown[],
	options: { reason: string },
): FlushResult {
	const dir = recoveryDir(home);
	const files: string[] = [];
	let failed = 0;
	for (const payload of payloads) {
		const normalized = normalizePendingPayload(payload);
		const fileName = `pending-${randomUUID()}.json`;
		const tmpPath = `${dir}/${fileName}.${randomUUID()}.tmp`;
		const finalPath = `${dir}/${fileName}`;
		try {
			mkdirSync(dir, { recursive: true });
			const body = JSON.stringify({
				kind: "pending_message",
				reason: options.reason,
				written_at: new Date().toISOString(),
				message: {
					session_id: normalized.sessionId,
					text: normalized.text,
				},
			});
			writeFileSync(tmpPath, body, { mode: 0o600 });
			renameSync(tmpPath, finalPath); // atomic on POSIX and Windows
			fsyncDirQuiet(dir);
			files.push(finalPath);
		} catch {
			failed++;
			try {
				rmSync(tmpPath, { force: true });
			} catch {
				/* best-effort */
			}
		}
	}
	return { files, failed };
}

export interface AgentHistorySnapshot {
	sessionId: string;
	messages: readonly unknown[];
}

/**
 * Operator-recovery snapshot with a DISTINCT schema (kind="agent_history") —
 * auto-recovery deliberately skips these (08 §1.3(a)); they exist so a human
 * can resurrect transcripts persistence corrupted.
 */
export function flushAgentHistoryToFile(
	home: string,
	sessionId: string,
	messages: readonly unknown[],
	options: { reason: string },
): string | null {
	const dir = recoveryDir(home);
	const fileName = `agent-history-${sessionId.replace(/[^A-Za-z0-9_-]/g, "_")}-${randomUUID()}.json`;
	const finalPath = `${dir}/${fileName}`;
	const tmpPath = `${finalPath}.${randomUUID()}.tmp`;
	try {
		mkdirSync(dir, { recursive: true });
		writeFileSync(
			tmpPath,
			JSON.stringify({
				kind: "agent_history",
				reason: options.reason,
				written_at: new Date().toISOString(),
				session_id: sessionId,
				messages: [...messages],
			}),
			{ mode: 0o600 },
		);
		renameSync(tmpPath, finalPath);
		fsyncDirQuiet(dir);
		return finalPath;
	} catch {
		try {
			rmSync(tmpPath, { force: true });
		} catch {
			/* best-effort */
		}
		return null;
	}
}

export interface RecoveryReport {
	recovered: number;
	/** Files kept on disk (invalid or insert-failed) — NEVER deleted. */
	preserved: string[];
}

/** Minimal StateStore-shaped seam (structural typing over pi_state facade). */
export interface RecoverStore {
	withWrite<T>(fn: (db: Database.Database) => T): Promise<T>;
	appendMessage(m: {
		sessionId: string;
		role: string;
		content?: string;
	}): Promise<number>;
}

/**
 * Boot-time replay (parity of recover_pending_to_db): each valid pending
 * payload inserts through the store's message surface (FTS triggers + session
 * metadata stay correct — never raw SQL), file unlinked on success, KEPT for
 * retry on failure. Structurally invalid / agent-history files are PRESERVED
 * with a warning and never deleted (08 §1.3(a)).
 */
export async function recoverPendingToDb(
	store: RecoverStore,
	home: string,
	log?: Logger,
): Promise<RecoveryReport> {
	const dir = recoveryDir(home);
	const report: RecoveryReport = { recovered: 0, preserved: [] };
	if (!existsSync(dir)) return report;
	let entries: string[];
	try {
		entries = readdirSync(dir).filter((f) => f.endsWith(".json"));
	} catch {
		return report;
	}
	for (const entry of entries) {
		const path = join(dir, entry);
		let parsed: unknown;
		try {
			parsed = JSON.parse(readFileSync(path, "utf8"));
		} catch {
			report.preserved.push(entry);
			log?.warn("preserved structurally invalid recovery file", {
				file: entry,
			});
			continue;
		}
		const record = (parsed ?? {}) as Record<string, unknown>;
		if (record["kind"] === "agent_history") {
			report.preserved.push(entry); // operator recovery only — never auto-inserted
			continue;
		}
		if (record["kind"] !== "pending_message") {
			report.preserved.push(entry);
			log?.warn("preserved unrecognized recovery file", { file: entry });
			continue;
		}
		const message = record["message"] as
			| { session_id?: unknown; text?: unknown }
			| undefined;
		const sessionId =
			typeof message?.session_id === "string" ? message.session_id : null;
		const text = typeof message?.text === "string" ? message.text : "";
		if (sessionId === null || (!sessionId && text === "")) {
			report.preserved.push(entry);
			log?.warn("preserved recovery file without resolvable session", {
				file: entry,
			});
			continue;
		}
		try {
			// SessionDB.append_message parity: the session context is ensured
			// before the message insert, both in ONE write txn (better-sqlite3
			// enforces messages.session_id → sessions(id) FKs by default).
			const nowSec = Math.floor(Date.now() / 1000);
			await store.withWrite((db) => {
				db.prepare(
					"INSERT OR IGNORE INTO sessions (id, source, started_at, last_activity_at) VALUES (?, 'shutdown_flush_recovery', ?, ?)",
				).run(sessionId, nowSec, nowSec);
				insertMessageInTx(db, { sessionId, role: "user", content: text });
			});
			unlinkSync(path); // replayed exactly once
			report.recovered++;
		} catch (err) {
			report.preserved.push(entry); // DB still unhealthy — keep + retry next boot
			log?.warn("recovery replay failed; file kept for retry", {
				file: entry,
				error: String(err),
			});
		}
	}
	return report;
}

/** Remove leftover planned-stop markers at boot (stale-guard hygiene). */
export function clearShutdownMarkersAtBoot(home: string): void {
	clearPlannedStopMarker(home);
}

// ---------------------------------------------------------------------------
// Clean-shutdown receipt + restart-failure counts (#7536 / _stop_impl_body)
// ---------------------------------------------------------------------------

export const CLEAN_SHUTDOWN_MARKER_FILENAME = ".clean_shutdown";
export const RESTART_FAILURE_COUNTS_FILENAME = ".restart_failure_counts";
/** Restarts-while-active before a session is auto-suspended (#7536). */
export const STUCK_LOOP_THRESHOLD = 3;

export function cleanShutdownMarkerPath(home: string): string {
	return join(home, CLEAN_SHUTDOWN_MARKER_FILENAME);
}

/** Touch-parity receipt: "this drain completed gracefully" (never raises). */
export function writeCleanShutdownMarker(home: string): boolean {
	try {
		mkdirSync(home, { recursive: true });
		writeFileSync(cleanShutdownMarkerPath(home), "", { mode: 0o600 });
		return true;
	} catch {
		return false;
	}
}

export function cleanShutdownMarkerExists(home: string): boolean {
	try {
		return existsSync(cleanShutdownMarkerPath(home));
	} catch {
		return false;
	}
}

/** Consume the receipt exactly once (boot side unlinks BEFORE acting). */
export function consumeCleanShutdownMarker(home: string): boolean {
	const existed = cleanShutdownMarkerExists(home);
	if (!existed) return false;
	try {
		unlinkSync(cleanShutdownMarkerPath(home));
	} catch {
		/* best-effort */
	}
	return true;
}

export function restartFailureCountsPath(home: string): string {
	return join(home, RESTART_FAILURE_COUNTS_FILENAME);
}

/** Read the persisted counters; unreadable/absent ⇒ {} (never raises). */
export function readRestartFailureCounts(home: string): Record<string, number> {
	try {
		if (!existsSync(restartFailureCountsPath(home))) return {};
		const parsed: unknown = JSON.parse(
			readFileSync(restartFailureCountsPath(home), "utf8"),
		);
		if (
			typeof parsed !== "object" ||
			parsed === null ||
			Array.isArray(parsed)
		) {
			return {};
		}
		const out: Record<string, number> = {};
		for (const [k, v] of Object.entries(parsed as Record<string, unknown>)) {
			const n = typeof v === "number" ? v : Number(v);
			if (Number.isFinite(n)) out[k] = n;
		}
		return out;
	} catch {
		return {};
	}
}

/**
 * Increment counters for sessions ACTIVE at shutdown; sessions NOT active are
 * dropped entirely — their loop is broken (run.py:
 * _increment_restart_failure_counts writes exactly the active set).
 */
export function incrementRestartFailureCounts(
	home: string,
	activeSessionKeys: readonly string[],
): void {
	try {
		const counts = readRestartFailureCounts(home);
		const next: Record<string, number> = {};
		for (const key of activeSessionKeys) {
			next[key] = (counts[key] ?? 0) + 1;
		}
		mkdirSync(home, { recursive: true });
		const path = restartFailureCountsPath(home);
		const tmp = `${path}.${randomUUID()}.tmp`;
		writeFileSync(tmp, JSON.stringify(next), { mode: 0o600 });
		renameSync(tmp, path);
	} catch {
		/* best-effort — counting must never break teardown */
	}
}
