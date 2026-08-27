// notifier.ts — the embedded kanban NOTIFIER: poll subscription cursors,
// deliver terminal events to their chat surface, advance/rewind cursors,
// unsubscribe on archive, and GC stale done-subscriptions.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/kanban_watchers.py:_kanban_notifier_watcher → notifyOnce + loop
//     * one tick every 5s (the watcher's interval parameter default);
//     * TERMINAL_KINDS claimed per sub: completed, blocked, gave_up,
//       crashed, timed_out, status, archived, unblocked, block_loop_detected,
//       review_requested — archived/unblocked are CLAIMED (cursor hygiene:
//       they can never wedge a later terminal event behind an unclaimed row)
//       but intentionally SILENT;
//     * subscriptions survive `done` (reversible via review/reopen) and are
//       removed ONLY when the task reaches irreversible `archived`;
//     * delivery failure rewinds the claim (CAS-guarded) so the next tick
//       retries; after MAX_SEND_FAILURES=12 consecutive failures the
//       subscription is dropped instead of spinning against a dead chat;
//     * stale done-sub GC once at startup then at most hourly
//       (_GC_INTERVAL_SECONDS = 3600), retention kanban.done_sub_retention_days
//       default 30, <=0 disables;
//     * failures in one tick NEVER stop subsequent ticks;
//     * initial delay so the host finishes wiring adapters before the first
//       poll.
//   hermes_cli/kanban_db.py:claim_unseen_events_for_sub → cursor discipline.
//
// Legacy-row ownership (DEC-057): Hermes' notifier tick sets
// include_unowned = _owns_kanban_dispatcher_lock() — subscription rows with
// NO profile stamp are visible ONLY while THIS process holds the machine-
// global dispatcher singleton. Pi's subscription schema carries no profile
// dimension at all, so EVERY row is legacy-equivalent and ownership gates
// the whole tick: when the injected `ownsSingleton` probe reports false,
// the tick claims/delivers nothing (silently — parity of an empty claim
// sweep) and retries next tick. The probe is optional: without it (single-
// gateway wiring, tests) delivery is unconditional, exactly the pre-DEC-057
// behavior.
//
// Divergence / PROPOSED DEC text: Hermes re-reads
// kanban.done_sub_retention_days from config.yaml at every GC sweep ("a
// config change applies without a restart"). Pi Gateway reads config ONCE at
// boot for every watcher knob (DEC-013: no live reload) — the retention
// setting is resolved once at start() like every other service setting, so a
// mid-flight change applies on RESTART here. The GC CADENCE itself (first
// tick + hourly gate) is ported unchanged.

import { systemClock, type GatewayClock } from "./clock.js";
import { resolveBoardSlug } from "./board.js";
import { GATEWAY_SECRET_PATTERNS } from "../approvals/redact.js";
import {
	subKeyOf,
	DEFAULT_DONE_SUB_RETENTION_DAYS,
	type ClaimedEvents,
	type NotifyEvent,
	type NotifySubStore,
	type NotifySubscription,
	type NotifyTaskView,
	type SubKey,
} from "./notify-store.js";

/**
 * Terminal event kinds CLAIMED by every tick (parity TERMINAL_KINDS).
 * archived/unblocked are silent — see SILENT_EVENT_KINDS.
 */
export const NOTIFY_TERMINAL_KINDS = [
	"completed",
	"blocked",
	"gave_up",
	"crashed",
	"timed_out",
	"status",
	"archived",
	"unblocked",
	"block_loop_detected",
	"review_requested",
	// kanban_watchers.py:TERMINAL_KINDS parity — a reviewer BLOCK in the
	// review lane was never claimed, so such events pinged nobody.
	"changes_requested",
] as const;

export type NotifyTerminalKind = (typeof NOTIFY_TERMINAL_KINDS)[number];

/** Claimed but intentionally SILENT kinds (archive needs no ping; unblocked
 * is an internal transition). They still advance the cursor. */
export const SILENT_EVENT_KINDS: ReadonlySet<string> = new Set([
	"archived",
	"unblocked",
]);

/**
 * Per-subscription consecutive send-failure budget (parity MAX_SEND_FAILURES
 * = 12 ≈ 60s at the 5s tick): a transient platform outage must NOT drop a
 * live subscription, but a genuinely dead chat must stop being spun against.
 */
export const MAX_SEND_FAILURES = 12;

/** One tick every 5s (parity of the watcher's interval default). */
export const DEFAULT_NOTIFIER_INTERVAL_SECONDS = 5;
/** Warm-up before the first poll (gateway finishes wiring adapters). */
export const DEFAULT_NOTIFIER_INITIAL_DELAY_SECONDS = 5;
/** Stale-done-sub GC cadence: first tick + at most once per hour. */
export const NOTIFIER_GC_INTERVAL_SECONDS = 3600;

/**
 * Delivery seam: send ONE rendered message for ONE event to the
 * subscription's destination. A throw (or a rejected promise) counts as a
 * FAILED delivery — the caller rewinds the claim so the next tick retries
 * (adapters reporting success=false throw or reject with that error).
 */
export type NotifyDeliverFn = (
	sub: NotifySubscription,
	message: string,
	event: NotifyEvent,
) => Promise<void> | void;

// ── rendering (message-shape parity, no i18n layer) ───────────────────────

function truncate(text: string, max: number): string {
	return text.length > max ? text.slice(0, max) : text;
}

/** Raw [:max] slice of a string — parity Python str(x)[:max]. */
function rawSlice(value: string, max: number): string {
	return value.length > max ? value.slice(0, max) : value;
}

/** Python str.splitlines() separator set (UTF-8 source parity of the
 * watcher's `.strip().splitlines()` handoff-line selection). */
const SPLITLINES_RE = /\r\n|\r|\n|\v|\f|\x1c|\x1d|\x1e|\u0085|\u2028|\u2029/;

/** First line of the stripped text clamped to max — but when stripping
 * yields NO lines at all (whitespace-only input), upstream falls back to the
 * RAW unstripped text [:max], so this does too. */
function firstLine(text: string, max: number): string {
	const lines = text.trim().split(SPLITLINES_RE);
	return truncate(lines[0] ?? "", max) || rawSlice(text, max);
}

function payloadString(
	payload: Record<string, unknown> | null,
	key: string,
): string {
	if (!payload) return "";
	const value = payload[key];
	return typeof value === "string" ? value : "";
}

function boardTagFor(board: string | null | undefined): string {
	return board ? `[${board}] ` : "";
}

function whoTag(task: NotifyTaskView | null): string {
	return task?.assignee ? `@${task.assignee} ` : "";
}

/** Render ONE terminal event into its user-facing message.
 *
 * Byte-parity of the current kanban_watchers.py notification text (drift
 * re-audit vs /tmp/hermes-upstream@77001a6b): per-kind glyph prefixes
 * (✔⏸✖✖⏱🔄👀🛑), RAW free-text slices where Hermes does not clamp
 * (completed/blocked/gave_up/review_requested/block_loop reasons ride plain
 * [:N] slices), clamped fields exactly where Hermes clamps (_safe_review_reason
 * rides ONLY the changes_requested reason/reviewer/implementer triple), and the
 * changes_requested composition WITHOUT the @assignee identity prefix.
 */
export function renderNotifyMessage(
	event: NotifyEvent,
	task: NotifyTaskView | null,
	board: string | null | undefined,
): string {
	const tag = boardTagFor(board);
	const who = whoTag(task);
	const title = truncate(task?.title ?? event.taskId, 120);
	const head = `${tag}${who}Kanban ${event.taskId} `;
	const headNoWho = `${tag}Kanban ${event.taskId} `;
	switch (event.kind) {
		case "completed": {
			// Worker handoff first (payload summary, [:200]), legacy task.result
			// fallback carries the SHORTER [:160] slice (watcher parity).
			const summary = payloadString(event.payload, "summary");
			const handoffSource = summary || task?.result || "";
			const handoffLimit = summary ? 200 : 160;
			const handoff = handoffSource
				? `\n${truncate(firstLine(handoffSource, handoffLimit), handoffLimit)}`
				: "";
			return `✔ ${head}done — ${title}${handoff}`;
		}
		case "blocked": {
			const reason = payloadString(event.payload, "reason");
			return `⏸ ${head}blocked${reason ? `: ${rawSlice(reason, 160)}` : ""}`;
		}
		case "gave_up": {
			const err = payloadString(event.payload, "error");
			return `✖ ${head}gave up after repeated spawn failures${err ? `\n${truncate(err, 200)}` : ""}`;
		}
		case "crashed":
			return `✖ ${head}worker crashed (pid gone); dispatcher will retry`;
		case "timed_out": {
			const rawLimit = event.payload?.["limit_seconds"];
			const limit =
				typeof rawLimit === "number" && Number.isFinite(rawLimit)
					? Math.trunc(rawLimit)
					: 0;
			return `⏱ ${head}timed out (max_runtime=${limit}s); will retry`;
		}
		case "status": {
			const status = payloadString(event.payload, "status");
			return `🔄 ${head}\u2192 ${status}`;
		}
		case "review_requested": {
			// RAW multi-line summary slice — NOT whitespace-collapsed and not
			// run through the external-delivery clamp (Hermes sends it raw).
			const summary = payloadString(event.payload, "summary");
			return `👀 ${head}ready for review — ${title}${summary ? `\n${rawSlice(summary, 200)}` : ""}`;
		}
		case "changes_requested": {
			// kanban_watchers.py:changes_requested branch — a reviewer BLOCKed
			// (or requested changes on) work still under review. The ONLY kind
			// whose free-text fields ride _safe_review_reason (reason clamped at
			// the default 160, identities at 48); the composed string itself is
			// NOT re-clamped. The @assignee prefix is deliberately absent here.
			const reason = payloadString(event.payload, "reason");
			const reviewer = safeReviewReason(
				payloadString(event.payload, "reviewer") || null,
				48,
			);
			const implementer = safeReviewReason(
				payloadString(event.payload, "implementer") || null,
				48,
			);
			const reasonText =
				safeReviewReason(reason) || "reviewer feedback requires changes";
			let provenance = "";
			if (reviewer) provenance += ` — reviewer @${reviewer}`;
			if (implementer) provenance += ` → implementer @${implementer}`;
			return `🛑 ${headNoWho}review requested changes/BLOCK: ${reasonText}${provenance}`;
		}
		case "block_loop_detected": {
			const reason = payloadString(event.payload, "reason");
			const recurrences = event.payload?.["recurrences"];
			const rc = recurrences
				? ` (blocked ${String(recurrences)}x for the same cause)`
				: "";
			return `🛑 ${head}routed to TRIAGE — needs a human decision${rc}${reason ? `: ${rawSlice(reason, 160)}` : ""}`;
		}
		default:
			// Silent kinds never render (guarded by SILENT_EVENT_KINDS upstream);
			// unknown kinds fall back to a bare transition line rather than
			// crashing the tick.
			return `${head}${event.kind}`;
	}
}

// ── external-delivery hygiene (kanban_watchers.py:_safe_review_reason) ───────────

/** kanban_watchers.py:_LOCAL_PATH_RE — absolute machine paths must not ride
 * notifications to a chat surface (they leak filesystem topology). */
const LOCAL_PATH_RE =
	/(?<![\w:/])(?:\/(?:Users|home|private|tmp|var|etc|workspace)\/[^\s,;]+|[A-Za-z]:\\[^\s,;]+)/g;
const URL_CREDENTIALS_RE = /(https?:\/\/)([^\s:@/]+):([^\s@/]+)@/gi;

/** Redact + clamp free-text meant for EXTERNAL delivery (review reasons,
 * reviewer handles). force-style secret scrub (gateway pattern belt) first —
 * an error or miss here is the failure direction that leaks; then URL
 * credentials ([REDACTED]@), local paths ([local path]), whitespace collapse,
 * and the ellipsis clamp. Never throws. */
export function safeReviewReason(value: unknown, limit = 160): string {
	let reason = value === null || value === undefined ? "" : String(value);
	try {
		for (const pattern of GATEWAY_SECRET_PATTERNS) {
			reason = reason.replace(pattern, (_match, group1?: string) =>
				typeof group1 === "string" ? `${group1}[REDACTED]` : "[REDACTED]",
			);
		}
	} catch {
		/* pattern pass is belt-and-suspenders — degrade to raw tail handling */
	}
	reason = reason.replace(URL_CREDENTIALS_RE, "$1[REDACTED]@");
	reason = reason.replace(LOCAL_PATH_RE, "[local path]");
	reason = reason.split(/\s+/).filter(Boolean).join(" ");
	if (reason.length > limit) {
		reason = `${reason.slice(0, limit - 1).replace(/\s+$/, "")}\u2026`;
	}
	return reason;
}

// ── one deterministic tick ────────────────────────────────────────────────

export interface NotifyTickOptions {
	/** Board slug for message tags (null ⇒ untagged). */
	board?: string | null;
	/** Run the stale-done-sub sweep this tick (service gates it hourly). */
	gcDue?: boolean;
	/** kanban.done_sub_retention_days parity (default 30; <=0 disables). */
	gcRetentionDays?: number;
	/** Epoch seconds backing the purge cutoff (injected clock read). */
	nowSeconds: number;
	deliver: NotifyDeliverFn;
	/**
	 * Cross-tick consecutive-failure counters keyed by subscription identity
	 * (owned by the CALLER so budgets persist across ticks, parity of the
	 * watcher's _kanban_sub_fail_counts). A fresh map is used when omitted.
	 */
	failCounts?: Map<string, number>;
	log?: { warn?(line: string): void };
}

export interface NotifyTickResult {
	subsScanned: number;
	delivered: Array<{
		taskId: string;
		platform: string;
		chatId: string;
		threadId: string;
		kind: string;
		eventId: number;
	}>;
	/** Subscriptions removed because their task reached `archived`. */
	unsubscribedArchived: Array<SubKey>;
	/** Subscriptions dropped after MAX_SEND_FAILURES consecutive failures. */
	droppedAfterFailures: Array<SubKey>;
	/** Claims rewound for retry after a transient delivery failure. */
	rewound: Array<SubKey>;
	/** Rows removed by the GC sweep this tick (0 when not due/disabled). */
	gcPurged: number;
}

function keyOf(sub: SubKey): SubKey {
	return {
		taskId: sub.taskId,
		platform: sub.platform,
		chatId: sub.chatId,
		threadId: sub.threadId,
	};
}

/**
 * Run ONE notifier tick over the store: list subs, claim unseen terminal
 * events per sub, deliver, advance/rewind cursors, unsubscribe on archive,
 * optionally GC. Per-subscription failures are isolated — one bad sub can
 * never block delivery for the others (parity of the per-sub try/except).
 * NEVER throws for delivery/store failures: they land in the result.
 */
export async function runNotifierTick(
	store: NotifySubStore,
	opts: NotifyTickOptions,
): Promise<NotifyTickResult> {
	const result: NotifyTickResult = {
		subsScanned: 0,
		delivered: [],
		unsubscribedArchived: [],
		droppedAfterFailures: [],
		rewound: [],
		gcPurged: 0,
	};
	const failCounts = opts.failCounts ?? new Map<string, number>();

	// Stale-done-sub GC first (cheap single DELETE, best-effort parity: a
	// failed sweep must never block delivery — errors propagate as tick
	// failures only from here).
	if (opts.gcDue === true) {
		try {
			result.gcPurged = await store.purgeStaleDoneSubs({
				maxAgeDays: opts.gcRetentionDays ?? DEFAULT_DONE_SUB_RETENTION_DAYS,
				nowSeconds: opts.nowSeconds,
			});
			if (result.gcPurged > 0) {
				opts.log?.warn?.(
					`[kanban] notifier: purged ${result.gcPurged} stale done-task subscription(s) (retention ${opts.gcRetentionDays ?? DEFAULT_DONE_SUB_RETENTION_DAYS}d)`,
				);
			}
		} catch (err) {
			opts.log?.warn?.(
				`[kanban] notifier: stale-sub GC failed: ${err instanceof Error ? err.message : String(err)}`,
			);
		}
	}

	const subs = await store.listSubs();
	result.subsScanned = subs.length;
	for (const sub of subs) {
		let claimed: ClaimedEvents | null;
		try {
			claimed = await store.claimUnseenEvents(sub, NOTIFY_TERMINAL_KINDS);
		} catch (err) {
			opts.log?.warn?.(
				`[kanban] notifier: subscription claim failed for ${sub.taskId}: ` +
					(err instanceof Error ? err.message : String(err)),
			);
			continue; // isolate per-subscription failures
		}
		if (claimed === null) continue; // subscription vanished mid-listing
		if (claimed.events.length === 0) continue;

		let task: NotifyTaskView | null = null;
		try {
			task = await store.getTask(sub.taskId);
		} catch {
			task = null; // render falls back to ids
		}

		let deliveredAll = true;
		for (const event of claimed.events) {
			if (SILENT_EVENT_KINDS.has(event.kind)) continue; // cursor hygiene only
			const message = renderNotifyMessage(event, task, opts.board ?? null);
			try {
				await opts.deliver(sub, message, event);
			} catch (err) {
				const key = subKeyOf(sub);
				const fails = (failCounts.get(key) ?? 0) + 1;
				failCounts.set(key, fails);
				opts.log?.warn?.(
					`[kanban] notifier: send failed for ${sub.taskId} on ${sub.platform}/${sub.chatId} ` +
						`(attempt ${fails}/${MAX_SEND_FAILURES}): ` +
						(err instanceof Error ? err.message : String(err)),
				);
				if (fails >= MAX_SEND_FAILURES) {
					// Dead chat: drop the subscription instead of spinning forever.
					await store.removeSub(sub);
					failCounts.delete(key);
					result.droppedAfterFailures.push(keyOf(sub));
				} else {
					// Transient: rewind the claim so the next tick retries.
					try {
						await store.rewindCursor(sub, claimed.newCursor, claimed.oldCursor);
						result.rewound.push(keyOf(sub));
					} catch (rewindErr) {
						opts.log?.warn?.(
							`[kanban] notifier: cursor rewind failed for ${sub.taskId}: ` +
								(rewindErr instanceof Error
									? rewindErr.message
									: String(rewindErr)),
						);
					}
				}
				deliveredAll = false;
				break; // stop processing THIS sub's remaining events this tick
			}
			failCounts.delete(subKeyOf(sub)); // success resets the counter
			result.delivered.push({
				taskId: sub.taskId,
				platform: sub.platform,
				chatId: sub.chatId,
				threadId: sub.threadId,
				kind: event.kind,
				eventId: event.id,
			});
		}

		if (!deliveredAll) continue; // retry semantics handled above
		try {
			// Parity _kanban_advance: pin the cursor at the claimed boundary
			// (idempotent after the claim's own advance).
			await store.advanceCursor(sub, claimed.newCursor);
			// Unsubscribe ONLY on archive: `done` stays reversible (reopen /
			// review cycles keep notifying), and the retained cursor prevents
			// replay of events already delivered.
			if (task !== null && task.status === "archived") {
				await store.removeSub(sub);
				result.unsubscribedArchived.push(keyOf(sub));
			}
		} catch (err) {
			opts.log?.warn?.(
				`[kanban] notifier: post-delivery bookkeeping failed for ${sub.taskId}: ` +
					(err instanceof Error ? err.message : String(err)),
			);
		}
	}
	return result;
}

// ── service wiring ────────────────────────────────────────────────────────

const FALSEY_ENV = new Set(["0", "false", "no", "off"]);

/**
 * The gateway-hosted-notifier gate env var. The DISPATCHER's
 * HERMES_KANBAN_DISPATCH_IN_GATEWAY does not cover delivery (dispatch and
 * notification have separate ownership in Hermes too), so the notifier gets
 * its own false-y escape hatch following the same naming convention.
 */
export const KANBAN_NOTIFY_IN_GATEWAY_ENV = "HERMES_KANBAN_NOTIFY_IN_GATEWAY";

export interface KanbanNotifierConfig {
	board: string;
	boardSource: "pinned" | "env" | "default";
	intervalSeconds: number;
	initialDelaySeconds: number;
	doneSubRetentionDays: number;
	enabled: boolean;
	warnings: string[];
}

function positiveIntOrDefault(
	raw: unknown,
	dflt: number,
	label: string,
	warn: (message: string) => void,
	floor = 0,
): number {
	if (raw === undefined || raw === null || raw === "") return dflt;
	const n = typeof raw === "number" ? raw : Number(raw);
	if (!Number.isFinite(n) || n < floor) {
		warn(
			`kanban notifier: invalid ${label}=${JSON.stringify(raw)}, using default ${dflt}`,
		);
		return dflt;
	}
	return Math.trunc(n);
}

/**
 * Resolve ALL notifier config up front (DEC-013: read once at boot — see the
 * module-header divergence note on per-sweep retention re-reads).
 */
export function resolveNotifierServiceConfig(input: {
	pinnedBoard?: string | null | undefined;
	env?: Record<string, string | undefined> | undefined;
	config?: Record<string, unknown> | undefined;
}): KanbanNotifierConfig {
	const cfg = input.config ?? {};
	const warnings: string[] = [];

	let enabled = true;
	const envOverride = (input.env?.[KANBAN_NOTIFY_IN_GATEWAY_ENV] ?? "")
		.trim()
		.toLowerCase();
	if (FALSEY_ENV.has(envOverride)) enabled = false;

	// Same HARD board-boundary resolution the dispatcher uses: an invalid or
	// fell-through pinned/env slug returns an UNUSABLE config (board: "") that
	// start() refuses loudly — delivering another board's cards' notifications
	// would breach the exact boundary HERMES_KANBAN_BOARD enforces.
	const resolved = resolveBoardSlug({
		pinned: input.pinnedBoard,
		env: input.env,
	});
	if (resolved.fellThrough) {
		return {
			board: "",
			boardSource: "pinned",
			intervalSeconds: DEFAULT_NOTIFIER_INTERVAL_SECONDS,
			initialDelaySeconds: DEFAULT_NOTIFIER_INITIAL_DELAY_SECONDS,
			doneSubRetentionDays: DEFAULT_DONE_SUB_RETENTION_DAYS,
			enabled,
			warnings: [
				`kanban notifier: ${resolved.reason ?? "board resolution fell through"}; ` +
					`DEGRADED LOUDLY — refusing to deliver this boot (hard board boundary; fix and RESTART, DEC-013)`,
			],
		};
	}

	const intervalSeconds = positiveIntOrDefault(
		cfg.notify_interval_seconds,
		DEFAULT_NOTIFIER_INTERVAL_SECONDS,
		"notify_interval_seconds",
		(w) => warnings.push(w),
		1,
	);
	const initialDelaySeconds = positiveIntOrDefault(
		cfg.notify_initial_delay_seconds,
		DEFAULT_NOTIFIER_INITIAL_DELAY_SECONDS,
		"notify_initial_delay_seconds",
		(w) => warnings.push(w),
		0,
	);
	const doneSubRetentionDays = positiveIntOrDefault(
		cfg.done_sub_retention_days,
		DEFAULT_DONE_SUB_RETENTION_DAYS,
		"done_sub_retention_days",
		(w) => warnings.push(w),
		0,
	);

	return {
		board: resolved.board,
		boardSource: resolved.source,
		intervalSeconds,
		initialDelaySeconds,
		doneSubRetentionDays,
		enabled,
		warnings,
	};
}

export interface NotifierStartResult {
	ok: boolean;
	degraded: boolean;
	reason?: string;
	warnings: string[];
}

export interface StartKanbanNotifierOptions {
	/** Factory producing the subscription store for the RESOLVED board. */
	openStore: (board: string) => Promise<NotifySubStore> | NotifySubStore;
	pinnedBoard?: string | null;
	env?: Record<string, string | undefined>;
	config?: Record<string, unknown>;
	/** Delivery bridge (chat-platform adapter seam; tests use recorders). */
	deliver: NotifyDeliverFn;
	clock?: GatewayClock;
	hasSingleton?: () => boolean;
	/**
	 * Per-tick dispatcher-lock ownership probe (DEC-057 legacy-row gating):
	 * false ⇒ this tick claims/delivers NOTHING (legacy rows are lock-owner-
	 * only). Omit on single-gateway wiring where delivery is unconditional.
	 */
	ownsSingleton?: () => boolean;
}

export interface RunningKanbanNotifier {
	stop(): Promise<void>;
}

/**
 * Start the embedded notifier. Never throws: every failure collapses into a
 * classified NotifierStartResult (optional-stage contract). The loop ticks
 * every `intervalSeconds` (default 5s), sweeps stale done-subs on the FIRST
 * tick and at most hourly thereafter, and a THROWING tick is logged loudly
 * while the loop continues (parity: failures in one tick don't stop
 * subsequent ticks).
 */
export async function startKanbanNotifier(
	opts: StartKanbanNotifierOptions,
	log: (line: string) => void = console.error,
): Promise<{ result: NotifierStartResult; running?: RunningKanbanNotifier }> {
	const clock = opts.clock ?? systemClock;
	const cfg = resolveNotifierServiceConfig({
		pinnedBoard: opts.pinnedBoard,
		env: opts.env,
		config: opts.config,
	});
	for (const warning of cfg.warnings) log(`[kanban] WARNING ${warning}`);

	if (!cfg.enabled) {
		const reason = `disabled via ${KANBAN_NOTIFY_IN_GATEWAY_ENV} env`;
		log(`[kanban] notifier: ${reason}`);
		return { result: { ok: false, degraded: false, reason, warnings: [] } };
	}
	if (!cfg.board) {
		const reason =
			cfg.warnings.find((w) => w.includes("board")) ??
			"invalid pinned board slug";
		log(`[kanban] notifier: DEGRADED — ${reason}`);
		return {
			result: { ok: false, degraded: true, reason, warnings: cfg.warnings },
		};
	}
	if (opts.hasSingleton && !opts.hasSingleton()) {
		const reason = "another gateway holds the machine-global notifier role";
		log(`[kanban] notifier: ${reason}; this gateway will NOT deliver.`);
		return { result: { ok: false, degraded: false, reason, warnings: [] } };
	}

	let store: NotifySubStore;
	try {
		store = await opts.openStore(cfg.board);
	} catch (err) {
		const reason = err instanceof Error ? err.message : String(err);
		const full = `cannot open subscription store for ${JSON.stringify(cfg.board)}: ${reason}`;
		log(`[kanban] notifier: DEGRADED — ${full}`);
		return {
			result: { ok: false, degraded: true, reason: full, warnings: [] },
		};
	}
	if (store.board !== cfg.board) {
		const full =
			`subscription store resolved to ${JSON.stringify(store.board)} but ` +
			`notifier pinned ${JSON.stringify(cfg.board)} — refusing (hard board boundary)`;
		log(`[kanban] notifier: DEGRADED — ${full}`);
		return {
			result: { ok: false, degraded: true, reason: full, warnings: [] },
		};
	}

	log(
		`[kanban] notifier: polling subscriptions for board ${JSON.stringify(cfg.board)} ` +
			`(interval=${cfg.intervalSeconds}s, gc=${NOTIFIER_GC_INTERVAL_SECONDS}s, retention=${cfg.doneSubRetentionDays}d)`,
	);

	const failCounts = new Map<string, number>();
	let running = true;
	const loopDone = (async () => {
		// Initial delay so the gateway finishes wiring adapters (parity of the
		// watcher's startup sleep).
		await clock.sleepMs(cfg.initialDelaySeconds * 1000);
		// 0 ⇒ sweep on the first tick after startup (parity _gc_next_at = 0.0).
		let gcNextAt = 0;
		while (running) {
			try {
				const nowSeconds = clock.nowSeconds();
				const gcDue = nowSeconds >= gcNextAt;
				if (opts.ownsSingleton && !opts.ownsSingleton()) {
					// Legacy rows are lock-owner-only (include_unowned parity):
					// without the machine-global dispatcher role nothing here is
					// ours to claim — skip silently, retry next tick.
				} else {
					if (gcDue) gcNextAt = nowSeconds + NOTIFIER_GC_INTERVAL_SECONDS;
					await runNotifierTick(store, {
						board: cfg.board,
						gcDue,
						gcRetentionDays: cfg.doneSubRetentionDays,
						nowSeconds,
						deliver: opts.deliver,
						failCounts,
						log: { warn: log },
					});
				}
			} catch (err) {
				log(
					`[kanban] notifier: tick FAILED loudly, continuing: ` +
						(err instanceof Error ? err.message : String(err)),
				);
			}
			await clock.sleepMs(cfg.intervalSeconds * 1000);
		}
	})();

	return {
		result: { ok: true, degraded: false, warnings: [] },
		running: {
			stop: async () => {
				running = false;
				await loopDone;
			},
		},
	};
}
