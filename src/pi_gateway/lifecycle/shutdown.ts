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
//
// Binding drain order (08 §1.2): stop ingress → active turns within grace →
// release leases → flush delivery obligations + token rollups + pending
// messages → close DB → exit non-zero only on failed flushes.
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

export type ShutdownClass = "takeover" | "planned_stop" | "unexpected_signal";

/**
 * Exit-code discipline (08 §1.2): planned takeover exits 0 so systemd's
 * Restart=on-failure doesn't flap-fight the replacer; planned stop exits 0;
 * an unexpected external signal exits non-zero so the supervisor revives us.
 */
export const SHUTDOWN_EXIT_CODES: Record<ShutdownClass, number> = {
	takeover: 0,
	planned_stop: 0,
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
export function classifySignalForSelf(
	home: string,
	signalName: string | null,
	options: { selfPid?: number } = {},
): ShutdownClass {
	const opts = { selfPid: options.selfPid ?? process.pid };
	if (consumeTakeoverMarkerForSelf(home, opts)) return "takeover";
	if (signalName === "SIGINT") return "planned_stop";
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
	| "stop_ingress"
	| "active_turns_grace"
	| "release_leases"
	| "flush_delivery_obligations"
	| "flush_token_rollups"
	| "finalize_agent_histories"
	| "flush_pending_messages"
	| "clear_pending_slots"
	| "close_database"
	| "persist_exit_status";

export const DRAIN_PHASE_ORDER: readonly DrainPhase[] = [
	"stop_ingress",
	"active_turns_grace",
	"release_leases",
	"flush_delivery_obligations",
	"flush_token_rollups",
	"finalize_agent_histories",
	"flush_pending_messages",
	"clear_pending_slots",
	"close_database",
	"persist_exit_status",
];

export interface DrainHooks {
	/** Adapters stop polling/reading (08 §1.2 step 1). Embedded background
	 *  services join here until Phase 5 gives them cooperative-drain contracts. */
	stopIngress?: () => Promise<void>;
	/** Active turns finish within a grace window. */
	awaitActiveTurns?: (graceMs: number) => Promise<void>;
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
	stop_ingress: "stop_ingress",
	active_turns_grace: "active_turns_grace",
	release_leases: "release_leases",
	flush_delivery_obligations: "flush_delivery_obligations",
	flush_token_rollups: "flush_token_rollups",
	finalize_agent_histories: "finalize_agent_histories",
	flush_pending_messages: "flush_pending_messages",
	clear_pending_slots: "clear_pending_slots",
	close_database: "close_database",
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
	await runStep(trace, STEP_FOR_PHASE.stop_ingress, async () => {
		await hooks.stopIngress?.();
	});
	await runStep(trace, STEP_FOR_PHASE.active_turns_grace, async () => {
		await hooks.awaitActiveTurns?.(options.graceMs);
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

	const klass = options.klass;
	const outcome: DrainOutcome = {
		klass,
		exitCode: SHUTDOWN_EXIT_CODES[klass],
		persistedStopped: false,
		flushesFailed: pendingFlushFailed || agentHistoryFlushFailed,
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
