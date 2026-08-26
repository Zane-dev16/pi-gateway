// pi_gateway/guards/l1-adapter-guard.ts — the ADAPTER-side guard state machine
// (03-message-routing.md §1–§5): sync-install before task spawn, the single
// pending slot, text debounce, stale-lock self-heal, cancel-handoff ordering,
// and the drain boundary with fresh-task ownership handoff.
//
// Fidelity rule (03 preamble): the two-guard queue, its ordering, and the
// inline bypass lanes are copied EXACTLY — every clause traces to a real
// incident (duplicate turns under bursts, stack exhaustion from recursive
// drains, approval deadlocks, dropped /new confirmations).
//
// Runtime mapping (DEC-023 idiom-level native):
//   asyncio.Event            → InterruptEvent
//   asyncio.create_task      → injectable TaskSpawner (default: immediate
//                              start; tests inject a MANUAL QUEUE for
//                              deterministic interleaving)
//   asyncio.current_task()   → the GatewayTask handle threaded through spawn
//   asyncio.wait_for(…, 5.0) → bounded await on cancel (cancelWaitTimeoutMs)
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/platforms/base.py:BasePlatformAdapter.__init__   → guard state maps
//   base.py:_start_session_processing                        → startSessionProcessing
//   base.py:_release_session_guard                           → releaseSessionGuard
//   base.py:_session_task_is_stale / _heal_stale_session_lock → sessionTaskIsStale / healStaleSessionLock
//   base.py:cancel_session_processing                        → cancelSessionProcessing
//   base.py:_dispatch_active_session_command                 → dispatchActiveSessionCommand
//   base.py:_drain_pending_after_session_command             → drainPendingAfterSessionCommand
//   base.py:_process_message_background (+ finally)          → processMessageBackground
//   base.py:_cleanup_finished_session_task (#48300)          → cleanupFinishedSessionTask
//   base.py:_queue_text_debounce / _flush_text_debounce_now / _discard_text_debounce
//                                                            → debounce family
//   base.py:handle_message                                   → handleMessage

import type { CommandRegistry } from "./busy-policy.js";
import {
	coercePlaintextGatewayCommand,
	getCommand,
	isCommand,
	type IncomingEvent,
	mergePendingEvent,
	type TextDebounceState,
	canMergeTextDebounceEvents,
} from "./events.js";
import type { SessionSource } from "../resolution/session-key.js";
import {
	buildCommandLookup,
	isInterruptThenDispatch,
	resolveCommand,
	shouldBypassActiveSession,
	type CommandDef,
} from "./busy-policy.js";

/** Default busy-text debounce window (HERMES_GATEWAY_BUSY_TEXT_DEBOUNCE_SECONDS). */
export const DEFAULT_DEBOUNCE_WINDOW_MS = 350;
/** Hard cap (HERMES_GATEWAY_BUSY_TEXT_HARD_CAP_SECONDS) — a burst cannot postpone forever. */
export const DEFAULT_DEBOUNCE_HARD_CAP_MS = 1000;
/** base.py:cancel_session_processing bounded wait. */
export const CANCEL_WAIT_TIMEOUT_MS = 5000;

/** Per-session interrupt Event analogue (asyncio.Event). */
export class InterruptEvent {
	private flag = false;
	setInterrupt(): void {
		this.flag = true;
	}
	clear(): void {
		this.flag = false;
	}
	isSet(): boolean {
		return this.flag;
	}
}

/**
 * Owner-task handle. `isDone()` mirrors asyncio.Task.done() (done OR
 * cancelled) — the stale-lock predicate reads exactly this.
 * `cancelRequested()` exposes the cooperative-cancel flag the turn frame
 * polls between phases.
 */
export interface GatewayTask {
	readonly result: Promise<void>;
	isDone(): boolean;
	cancel(): void;
	cancelRequested(): boolean;
}

/**
 * Spawn seam. The returned GatewayTask must invoke `run(task)` — with the
 * task handle itself — immediately (production) or later (manual queues in
 * tests). Returning null/sentinel models the non-Task stub: the caller rolls
 * back the half-installed lock (03 §11 row).
 */
export type TaskSpawner = (
	run: (task: GatewayTask) => Promise<void>,
) => GatewayTask | null | undefined;

/** Cooperative cancellation signal handed to the message handler. */
export interface TurnContext {
	readonly task: GatewayTask;
	/** Throws TaskCancelledError once this frame's task has been cancelled. */
	throwIfCancelled(): void;
}

export class TaskCancelledError extends Error {
	constructor() {
		super("turn task cancelled");
		this.name = "TaskCancelledError";
	}
}

export type ProcessingOutcome = "success" | "failure" | "cancelled";

export type MessageHandler = (
	event: IncomingEvent,
	ctx: TurnContext,
) => Promise<string | null | undefined>;

export interface AdapterGuardDeps {
	/**
	 * The runner's message handler (base.py:self._message_handler): used BOTH
	 * by background turn frames and by the inline bypass lanes.
	 */
	messageHandler: MessageHandler;
	/** Reply sink (chat_id → text). */
	sendReply: (chatId: string, text: string) => Promise<void>;
	registry: CommandRegistry;
	spawner?: TaskSpawner;
	/** Warning sink (stale heals, suppressed responses, errors). Default silent. */
	onWarning?: (message: string) => void;
	debounceWindowMs?: number;
	debounceHardCapMs?: number;
	/** Monotonic ms clock for debounce timing (injected in tests). */
	nowMs?: () => number;
	/**
	 * Timer seam: schedule(fn) after delayMs, return canceller. Default
	 * setTimeout(unref). Tests may drive flushes manually instead.
	 */
	scheduleTimer?: (delayMs: number, fn: () => void) => () => void;
	cancelWaitTimeoutMs?: number;
	/**
	 * Adapter mirror of display.busy_input_mode (run.py syncs it onto adapters
	 * as busy_text_mode). Only "queue" arms the debounce candidate gate.
	 */
	busyTextMode?: "queue" | "interrupt";
	/**
	 * Clarify intercept predicate (Lane C, §5.3): when it returns true for a
	 * NON-command message, the message routes inline to the resolver instead
	 * of becoming a follow-up turn. Absent ⇒ lane disabled (Phase-2 tool).
	 */
	hasPendingClarify?: (sessionKey: string) => boolean;
	/** Optional per-key busy-session handler (base.py:_busy_session_handler). */
	busySessionHandler?: (
		event: IncomingEvent,
		sessionKey: string,
	) => Promise<boolean>;
	/**
	 * Telegram DM topic-recovery hook (base.py:set_topic_recovery_fn — the
	 * runner installs run.py:_recover_telegram_topic_thread_id). Called with
	 * event.source BEFORE any keying/matching; a non-null return rewrites
	 * source.threadId so a lobby-shaped DM reply (''/General "1") pins to the
	 * user's last-active topic lane. Absent ⇒ hook disabled.
	 */
	topicThreadRecovery?: (source: SessionSource) => string | null | undefined;
	/**
	 * Key rebuilder applied to the REWRITTEN source after a recovery hit
	 * (build_session_key parity: Hermes derives the session key AFTER the
	 * rewrite, so routing follows the recovered lane). Required together with
	 * topicThreadRecovery; without it a rewrite cannot move the key.
	 */
	rebuildSessionKey?: (source: SessionSource) => string;
}

function defaultSchedule(delayMs: number, fn: () => void): () => void {
	const t = setTimeout(fn, delayMs);
	t.unref?.();
	return () => clearTimeout(t);
}

/** Production spawner: starts the frame immediately (create_task semantics). */
export function immediateSpawner(): TaskSpawner {
	return (run) => new ImmediateTask(run);
}

class ImmediateTask implements GatewayTask {
	readonly result: Promise<void>;
	private settled = false;
	private cancelled = false;
	constructor(run: (task: GatewayTask) => Promise<void>) {
		this.result = (async () => {
			try {
				await run(this);
			} catch (err) {
				// Frames contain their own errors; this net only prevents
				// unhandledRejection noise from fire-and-forget tasks.
			} finally {
				this.settled = true;
			}
		})();
	}
	isDone(): boolean {
		return this.settled; // cancelled-but-unwinding is NOT done (stale-lock parity)
	}
	cancel(): void {
		this.cancelled = true; // cooperative: frames read cancelRequested()
	}
	cancelRequested(): boolean {
		return this.cancelled;
	}
}

/**
 * The L1 guard. One instance per adapter. State maps mirror
 * base.py:BasePlatformAdapter.__init__ §2.1 exactly:
 *   activeSessions ≙ _active_sessions   pendingMessages ≙ _pending_messages
 *   sessionTasks   ≙ _session_tasks     textDebounce   ≙ _text_debounce
 *
 * INVARIANT (sync-install, §2.1): activeSessions[key] is installed
 * SYNCHRONOUSLY, BEFORE the turn task spawns — a burst delivering two events
 * before either task runs collapses to exactly ONE turn.
 */
export class AdapterSessionGuard {
	private readonly activeSessions = new Map<string, InterruptEvent>();
	private readonly pendingMessages = new Map<string, IncomingEvent>();
	private readonly sessionTasks = new Map<string, GatewayTask>();
	private readonly textDebounce = new Map<string, TextDebounceState<unknown>>();
	private readonly lookup: ReadonlyMap<string, CommandDef>;

	private readonly messageHandler: MessageHandler;
	private readonly sendReply: (chatId: string, text: string) => Promise<void>;
	private readonly spawner: TaskSpawner;
	private readonly onWarning: (message: string) => void;
	private readonly debounceWindowMs: number;
	private readonly debounceHardCapMs: number;
	private readonly nowMs: () => number;
	private readonly scheduleTimer: (
		delayMs: number,
		fn: () => void,
	) => () => void;
	private readonly cancelWaitTimeoutMs: number;
	private readonly busyTextMode: "queue" | "interrupt";
	private readonly hasPendingClarify: ((sessionKey: string) => boolean) | null;
	private readonly busySessionHandler:
		| ((event: IncomingEvent, sessionKey: string) => Promise<boolean>)
		| null;
	private readonly topicThreadRecovery:
		| ((source: SessionSource) => string | null | undefined)
		| null;
	private readonly rebuildSessionKey:
		| ((source: SessionSource) => string)
		| null;

	constructor(deps: AdapterGuardDeps) {
		this.lookup = buildCommandLookup(deps.registry);
		this.messageHandler = deps.messageHandler;
		this.sendReply = deps.sendReply;
		this.spawner = deps.spawner ?? immediateSpawner();
		this.onWarning = deps.onWarning ?? (() => {});
		this.debounceWindowMs = deps.debounceWindowMs ?? DEFAULT_DEBOUNCE_WINDOW_MS;
		this.debounceHardCapMs =
			deps.debounceHardCapMs ?? DEFAULT_DEBOUNCE_HARD_CAP_MS;
		this.nowMs = deps.nowMs ?? (() => performance.now());
		this.scheduleTimer = deps.scheduleTimer ?? defaultSchedule;
		this.cancelWaitTimeoutMs =
			deps.cancelWaitTimeoutMs ?? CANCEL_WAIT_TIMEOUT_MS;
		this.busyTextMode = deps.busyTextMode ?? "interrupt";
		this.hasPendingClarify = deps.hasPendingClarify ?? null;
		this.busySessionHandler = deps.busySessionHandler ?? null;
		// The pair is only meaningful together (rewrite + re-key).
		this.topicThreadRecovery =
			deps.topicThreadRecovery !== undefined &&
			deps.rebuildSessionKey !== undefined
				? deps.topicThreadRecovery
				: null;
		this.rebuildSessionKey =
			deps.topicThreadRecovery !== undefined &&
			deps.rebuildSessionKey !== undefined
				? deps.rebuildSessionKey
				: null;
	}

	// -- observability --------------------------------------------------------

	isActive(sessionKey: string): boolean {
		return this.activeSessions.has(sessionKey);
	}

	guardOf(sessionKey: string): InterruptEvent | undefined {
		return this.activeSessions.get(sessionKey);
	}

	ownerOf(sessionKey: string): GatewayTask | undefined {
		return this.sessionTasks.get(sessionKey);
	}

	pendingOf(sessionKey: string): IncomingEvent | undefined {
		return this.pendingMessages.get(sessionKey);
	}

	/** Narrow view over the single pending slot (runner FIFO helpers consume this). */
	get slotView(): Map<string, IncomingEvent> {
		return this.pendingMessages;
	}

	debounceBufferedText(sessionKey: string): string | null {
		return this.textDebounce.get(sessionKey)?.event.text ?? null;
	}

	/** Test/diagnostic view of one session's debounce state (anchors, timing). */
	pendingDebounceStateForTests(
		sessionKey: string,
	): Readonly<TextDebounceState> | null {
		const state = this.textDebounce.get(sessionKey);
		if (state === undefined) return null;
		// Freeze a shallow copy so tests cannot mutate live buffer state.
		return {
			event: { ...state.event },
			task: null,
			firstTs: state.firstTs,
			lastTs: state.lastTs,
		};
	}

	// -- TEST-ONLY state seams --------------------------------------------------
	// Parity of tests installing guards directly into _active_sessions
	// (base.py explicitly leaves such entries alone in stale detection).

	/** Install a guard WITHOUT an owner task (external-install parity). */
	forceInstallGuardForTests(sessionKey: string): void {
		this.activeSessions.set(sessionKey, new InterruptEvent());
	}

	/** Re-record an owner task for an existing guard (split-brain simulation). */
	installOwnerForTests(sessionKey: string, task: GatewayTask): void {
		this.sessionTasks.set(sessionKey, task);
	}

	/** Seed debounce buffer state directly (crashed-turn leftovers). */
	forceBusyQueueTextForTests(sessionKey: string, event: IncomingEvent): void {
		const now = this.nowMs();
		this.textDebounce.set(sessionKey, {
			event,
			task: null,
			firstTs: now,
			lastTs: now,
		});
	}

	// -- guard ownership helpers ----------------------------------------------

	/**
	 * base.py:_release_session_guard — identity-checked release. When `guard`
	 * is given, only releases if the entry STILL points at that exact Event:
	 * a reset-like command's swap-in must survive an old task's cleanup.
	 */
	releaseSessionGuard(sessionKey: string, guard?: InterruptEvent): boolean {
		const currentGuard = this.activeSessions.get(sessionKey);
		if (currentGuard === undefined) return false;
		if (guard !== undefined && currentGuard !== guard) return false;
		this.activeSessions.delete(sessionKey);
		return true;
	}

	/**
	 * base.py:_session_task_is_stale — stale ONLY when a recorded owner task
	 * exists and has exited (done/cancelled). No owner task at all ≠ stale:
	 * guards installed outside handleMessage (tests, other paths) stay alone.
	 */
	sessionTaskIsStale(sessionKey: string): boolean {
		const task = this.sessionTasks.get(sessionKey);
		if (task === undefined) return false;
		return task.isDone();
	}

	/**
	 * base.py:_heal_stale_session_lock — on-entry split-brain recovery
	 * (#11016 tail): pops guard + pending slot + owner task + debounce, then
	 * falls through to normal dispatch. Returns true iff healed.
	 */
	healStaleSessionLock(sessionKey: string): boolean {
		if (!this.activeSessions.has(sessionKey)) return false;
		if (!this.sessionTaskIsStale(sessionKey)) return false;
		this.onWarning(
			`Healing stale session lock for ${sessionKey} (owner task is done/absent)`,
		);
		this.activeSessions.delete(sessionKey);
		this.pendingMessages.delete(sessionKey);
		this.sessionTasks.delete(sessionKey);
		this.discardTextDebounce(sessionKey);
		return true;
	}

	/**
	 * base.py:_start_session_processing — installs guard AND owner task
	 * atomically. Returns false (with full rollback) when the spawner yields a
	 * non-task sentinel, so the caller never holds a half-installed lock.
	 */
	startSessionProcessing(event: IncomingEvent, sessionKey: string): boolean {
		const guard = new InterruptEvent();
		this.activeSessions.set(sessionKey, guard); // SYNC-INSTALL, before any spawn

		let task: GatewayTask | null | undefined;
		try {
			task = this.spawner((self) =>
				this.processMessageBackground(event, sessionKey, guard, self),
			);
		} catch {
			task = null; // spawner blew up ≙ non-Task sentinel
		}
		if (
			task === null ||
			task === undefined ||
			typeof task.isDone !== "function" ||
			typeof task.cancel !== "function"
		) {
			// Rollback: never leave a guard installed without an owner.
			this.sessionTasks.delete(sessionKey);
			this.releaseSessionGuard(sessionKey, guard);
			return false;
		}
		this.sessionTasks.set(sessionKey, task);
		return true;
	}

	/**
	 * base.py:cancel_session_processing — cancel the owner task, bounded by a
	 * wait so a wedged finally cannot stall dispatch. `release_guard=false`
	 * lets reset-like commands finish atomically before follow-ups dispatch.
	 */
	async cancelSessionProcessing(
		sessionKey: string,
		options: { releaseGuard?: boolean; discardPending?: boolean } = {},
	): Promise<"cancelled" | "not-running"> {
		const releaseGuard = options.releaseGuard ?? true;
		const discardPending = options.discardPending ?? true;
		const task = this.sessionTasks.get(sessionKey);
		this.sessionTasks.delete(sessionKey);
		let outcome: "cancelled" | "not-running" = "not-running";
		if (task !== undefined && !task.isDone()) {
			outcome = "cancelled";
			task.cancel();
			await Promise.race([
				task.result.catch(() => {}),
				new Promise<void>((resolve) => {
					const t = setTimeout(resolve, this.cancelWaitTimeoutMs);
					t.unref?.();
				}),
			]);
		}
		if (discardPending) {
			this.pendingMessages.delete(sessionKey);
			this.discardTextDebounce(sessionKey);
		}
		if (releaseGuard) this.releaseSessionGuard(sessionKey);
		return outcome;
	}

	// -- ingress --------------------------------------------------------------

	/**
	 * base.py:handle_message — returns quickly; heavy work always rides a
	 * spawned frame. Busy input takes the bypass lanes (§5) or the queue paths
	 * (§3); idle input sync-installs and spawns.
	 */
	async handleMessage(event: IncomingEvent, sessionKey: string): Promise<void> {
		// base.py handle_message entry (base.py:6137 call shape): coerce exact DM
		// restart phrases to "/restart" FIRST — before topic recovery, key
		// derivation, or any classification — so a self-restart ask can never
		// ride the LLM path inside a running turn.
		if (allowsControl(event)) {
			coercePlaintextGatewayCommand(event);
		}

		// base.py runs Telegram DM topic recovery BEFORE deriving/using any key
		// (base.py:_apply_topic_recovery ahead of build_session_key): a
		// lobby-shaped reply (''/General "1") must pin to the user's bound lane
		// or history splits across session keys.
		sessionKey = this.applyPreKeyTopicRecovery(event, sessionKey);

		// Internally routed events carry the key they were forged for; a
		// mismatch means misrouting — drop loudly rather than cross transcripts.
		const expectedKey = String(
			(event.metadata ?? {})["gateway_session_key"] ?? "",
		).trim();
		if (expectedKey !== "" && expectedKey !== sessionKey) {
			this.onWarning(
				`Dropping internally routed event: expected session=${expectedKey} derived=${sessionKey}`,
			);
			return;
		}

		// On-entry self-heal (§3.3) BEFORE the busy check.
		if (this.activeSessions.has(sessionKey)) {
			this.healStaleSessionLock(sessionKey);
		}

		if (this.activeSessions.has(sessionKey)) {
			const cmd = getCommand(event);

			// Bypass lanes (§5): commands NEVER queue (#4926/#5057).
			if (shouldBypassActiveSession(this.lookup, cmd)) {
				if (cmd !== null && isInterruptThenDispatch(this.lookup, cmd)) {
					// Lane A: cancel-handoff (/stop, /new, /reset).
					this.discardTextDebounce(sessionKey);
					try {
						await this.dispatchActiveSessionCommand(event, sessionKey, cmd);
					} catch (err) {
						this.onWarning(`Command '/${cmd}' dispatch failed: ${String(err)}`);
					}
					return;
				}
				// Lane B: direct dispatch (/approve, /deny, /status, …) — the
				// running task keeps running.
				await this.dispatchInline(event);
				return;
			}

			// Lane C: clarify intercept — agent blocked on the resolver must see
			// the next non-command message first (same shape as /approve).
			if (
				cmd === null &&
				allowsControl(event) &&
				this.hasPendingClarify !== null &&
				this.hasPendingClarify(sessionKey)
			) {
				await this.dispatchInline(event);
				return;
			}

			if (this.busySessionHandler !== null) {
				const handled = await this.busySessionHandler(event, sessionKey);
				if (handled) return;
			}

			// Photo bursts/albums merge into the head slot, never interrupt.
			if (event.messageType === "photo") {
				mergePendingEvent(this.pendingMessages, sessionKey, event);
				return;
			}

			if (this.isQueueTextDebounceCandidate(event)) {
				this.queueTextDebounce(sessionKey, event);
			} else {
				mergePendingEvent(this.pendingMessages, sessionKey, event, {
					mergeText: event.messageType === "text",
				});
			}
			return; // Don't process now — cascades after the current turn finishes
		}

		// Idle path. Mark active BEFORE spawning so a second message arriving
		// before the task starts cannot also pass the check (grammY
		// sequentialize pattern — set the guard synchronously, not in the task).
		this.startSessionProcessing(event, sessionKey);
	}

	/**
	 * base.py:_apply_topic_recovery — rewrite event.source.threadId in place
	 * when the recovery hook returns one, then RE-DERIVE the session key from
	 * the rewritten source. Runs ONLY for telegram DM arrivals (the exact
	 * base.py needs_topic_recovery gate: group/forum/channel traffic never
	 * touches the store-backed hook); hook failures degrade to the original
	 * key. A no-rewrite result keeps both the source object and the key
	 * untouched (identity preserved, parity of dataclasses.replace skipping).
	 */
	private applyPreKeyTopicRecovery(
		event: IncomingEvent,
		sessionKey: string,
	): string {
		const recover = this.topicThreadRecovery;
		const rebuild = this.rebuildSessionKey;
		if (recover === null || rebuild === null) return sessionKey;
		const source = event.source;
		if (source === undefined || source === null) return sessionKey;
		if (source.platform !== "telegram" || source.chatType !== "dm") {
			return sessionKey;
		}
		let recovered: string | null | undefined;
		try {
			recovered = recover(source);
		} catch (err) {
			this.onWarning(`Topic recovery hook failed: ${String(err)}`);
			return sessionKey;
		}
		if (recovered === null || recovered === undefined) return sessionKey;
		const currentThreadId = String(source.threadId ?? "");
		if (String(recovered) === currentThreadId) return sessionKey;
		event.source = { ...source, threadId: String(recovered) };
		return rebuild(event.source);
	}

	/** Inline dispatch shared by Lane B / Lane C: run handler, send reply. */
	private async dispatchInline(event: IncomingEvent): Promise<void> {
		try {
			const response = await this.messageHandler(event, {
				task: DETACHED_TASK,
				throwIfCancelled: () => {},
			});
			if (typeof response === "string" && response.length > 0) {
				await this.sendReply(chatIdOf(event), response);
			}
		} catch (err) {
			this.onWarning(`Inline dispatch failed: ${String(err)}`);
		}
	}

	// -- Lane A: cancel-handoff -------------------------------------------------

	/**
	 * base.py:_dispatch_active_session_command — serialize /stop,/new,/reset:
	 * (1) swap a command-scoped guard so racing follow-ups stay queued;
	 * (2) run the runner handler INLINE;
	 * (3) send the response BEFORE cancelling the old task (#18912 — the
	 *     "/new" confirmation must not be dropped by cancellation side effects);
	 * (4) cancel the old task (bounded), preserving guard and pending;
	 * (5) drain once via drainPendingAfterSessionCommand.
	 */
	async dispatchActiveSessionCommand(
		event: IncomingEvent,
		sessionKey: string,
		_cmd: string, // parity of the '/%s' debug log label; warnings carry it at the call site
	): Promise<void> {
		const currentGuard = this.activeSessions.get(sessionKey);
		const commandGuard = new InterruptEvent();
		this.activeSessions.set(sessionKey, commandGuard);

		try {
			const response = await this.messageHandler(event, {
				task: DETACHED_TASK,
				throwIfCancelled: () => {},
			});
			// Response BEFORE cancellation — deterministic ordering (#18912).
			if (typeof response === "string" && response.length > 0) {
				await this.sendReply(chatIdOf(event), response);
			}
			await this.cancelSessionProcessing(sessionKey, {
				releaseGuard: false,
				discardPending: false,
			});
		} catch (err) {
			// On failure restore the original guard if the entry still points at
			// the command-scoped one — never leave a half-reset session.
			if (this.activeSessions.get(sessionKey) === commandGuard) {
				if (this.sessionTasks.has(sessionKey) && currentGuard !== undefined) {
					this.activeSessions.set(sessionKey, currentGuard);
				} else {
					this.releaseSessionGuard(sessionKey, commandGuard);
				}
			}
			throw err;
		}

		await this.drainPendingAfterSessionCommand(sessionKey, commandGuard);
	}

	/**
	 * base.py:_drain_pending_after_session_command — resume the latest queued
	 * follow-up once a session command completes: flush debounce → pop slot →
	 * release the command-scoped guard → fresh processing task for the head.
	 */
	async drainPendingAfterSessionCommand(
		sessionKey: string,
		commandGuard: InterruptEvent,
	): Promise<void> {
		await this.flushTextDebounceNow(sessionKey);
		const pendingEvent = this.pendingMessages.get(sessionKey);
		this.pendingMessages.delete(sessionKey);
		this.releaseSessionGuard(sessionKey, commandGuard);
		if (pendingEvent === undefined) return;
		this.startSessionProcessing(pendingEvent, sessionKey);
	}

	// -- the turn frame ---------------------------------------------------------

	/**
	 * base.py:_process_message_background — one turn of the pipeline ending in
	 * the DRAIN BOUNDARY (§4). A finishing turn hands the session to the next
	 * owner; it NEVER loops on pending work inside its own frame.
	 */
	async processMessageBackground(
		event: IncomingEvent,
		sessionKey: string,
		guardAtEntry: InterruptEvent,
		currentTask: GatewayTask,
	): Promise<void> {
		// Reuse the interrupt event installed by startSessionProcessing (fall
		// back only if the entry was removed externally).
		const interruptEvent = this.activeSessions.get(sessionKey) ?? guardAtEntry;
		this.activeSessions.set(sessionKey, interruptEvent);

		const ctx: TurnContext = {
			task: currentTask,
			throwIfCancelled: () => {
				if (currentTask.cancelRequested()) throw new TaskCancelledError();
			},
		};

		try {
			// Handler (can take a while with tool calls).
			let response: string | null | undefined;
			try {
				response = await this.messageHandler(event, ctx);
			} catch (err) {
				if (
					err instanceof TaskCancelledError ||
					currentTask.cancelRequested()
				) {
					throw err;
				}
				response = await this.handleTurnError(event, sessionKey, err);
			}
			ctx.throwIfCancelled();

			// Suppress stale response when interrupted by a not-yet-consumed
			// follow-up (#8221/#2483) — the pending handler owns the reply now.
			if (
				response &&
				interruptEvent.isSet() &&
				this.pendingMessages.has(sessionKey)
			) {
				this.onWarning(
					`Suppressing stale response for interrupted session ${sessionKey}`,
				);
				response = null;
			}
			if (response) {
				await this.sendReply(chatIdOf(event), response);
			}

			// The active drain owns debounce state: force-flush a queue-mode
			// timer that hasn't fired so THIS task hands off the follow-up.
			await this.flushTextDebounceNow(sessionKey);

			// IN-BAND DRAIN (§4): pop the slot; CLEAR the Event only — the ENTRY
			// stays live across the chain (deleting it re-opens the duplicate-
			// turn race); spawn a FRESH task (#17758: recursion grew one stack
			// frame per chained follow-up until SIGSEGV); hand OWNERSHIP over.
			if (this.pendingMessages.has(sessionKey)) {
				const pendingEvent = this.pendingMessages.get(sessionKey);
				this.pendingMessages.delete(sessionKey);
				interruptEvent.clear();
				const drainTask = this.trySpawnDrain(pendingEvent, sessionKey);
				if (drainTask !== null) {
					this.sessionTasks.set(sessionKey, drainTask);
				}
				return; // Drain task owns the session now.
			}
		} catch (err) {
			if (err instanceof TaskCancelledError || currentTask.cancelRequested()) {
				// Cancelled turns unwind silently (no user-facing error notice).
				return;
			}
			await this.handleTurnError(event, sessionKey, err);
		} finally {
			// Final force-flush before deciding whether the guard can clear.
			await this.flushTextDebounceNow(sessionKey);

			// LATE-ARRIVAL DRAIN (finally block): a message may land in the slot
			// during cleanup awaits. If another task already owns the key,
			// RE-QUEUE the event for that owner (never dual-spawn); otherwise
			// this frame spawns the fresh drain itself.
			const latePending = this.pendingMessages.get(sessionKey);
			this.pendingMessages.delete(sessionKey);
			if (latePending !== undefined) {
				const existingTask = this.sessionTasks.get(sessionKey);
				if (existingTask !== undefined && existingTask !== currentTask) {
					// In-band drain (or earlier late-arrival drain) owns this
					// session — hand the event back to ITS slot.
					this.pendingMessages.set(sessionKey, latePending);
				} else {
					interruptEvent.clear();
					const drainTask = this.trySpawnDrain(latePending, sessionKey);
					if (drainTask !== null) {
						this.sessionTasks.set(sessionKey, drainTask);
					}
				}
				// Leave activeSessions populated — the drain task cleans up.
			} else if (this.sessionTasks.get(sessionKey) === currentTask) {
				// Owner-check covers the in-band handoff above: when we spawned a
				// drain task, the map names IT, so we leave everything populated.
				this.cleanupFinishedSessionTask(sessionKey, interruptEvent);
			}
		}
	}

	/** Send the radio-silence-breaking error notice; failures never raise. */
	private async handleTurnError(
		event: IncomingEvent,
		_sessionKey: string,
		err: unknown,
	): Promise<null> {
		const errorType = err instanceof Error ? err.constructor.name : typeof err;
		const detail = String(err instanceof Error ? err.message : err).slice(
			0,
			300,
		);
		this.onWarning(`Error handling message: ${String(err)}`);
		try {
			await this.sendReply(
				chatIdOf(event),
				`Sorry, I encountered an error (${errorType}).\n${detail}\nTry again or use /reset to start a fresh session.`,
			);
		} catch (notifyErr) {
			this.onWarning(`Failed to send error notification: ${String(notifyErr)}`);
		}
		return null;
	}

	/**
	 * base.py:_cleanup_finished_session_task — release-then-conditional-delete
	 * (#48300): when a concurrent path swapped in a different guard, the
	 * identity-checked release skips and the task entry survives.
	 */
	private cleanupFinishedSessionTask(
		sessionKey: string,
		guard: InterruptEvent,
	): void {
		const released = this.releaseSessionGuard(sessionKey, guard);
		if (released) this.sessionTasks.delete(sessionKey);
	}

	/** Spawn a drain frame; sentinel/failed spawns degrade to null. */
	private trySpawnDrain(
		pendingEvent: IncomingEvent | undefined,
		sessionKey: string,
	): GatewayTask | null {
		if (pendingEvent === undefined) return null;
		const guard = this.activeSessions.get(sessionKey) ?? new InterruptEvent();
		try {
			const task = this.spawner((self) =>
				this.processMessageBackground(pendingEvent, sessionKey, guard, self),
			);
			if (
				task === null ||
				task === undefined ||
				typeof task.isDone !== "function"
			) {
				return null;
			}
			return task;
		} catch {
			return null;
		}
	}

	// -- text debounce (§3.2) ----------------------------------------------------

	/** base.py:_is_queue_text_debounce_candidate. */
	isQueueTextDebounceCandidate(event: IncomingEvent): boolean {
		return (
			this.busyTextMode === "queue" &&
			event.messageType === "text" &&
			event.internal !== true &&
			!isCommand(event) &&
			Boolean((event.text ?? "").trim())
		);
	}

	/** base.py:_text_debounce_delay — min(window deadline, hard-cap deadline). */
	textDebounceDelayMs(state: TextDebounceState<unknown>): number {
		const now = this.nowMs();
		const windowDeadline = state.lastTs + this.debounceWindowMs;
		const hardCapDeadline = state.firstTs + this.debounceHardCapMs;
		return Math.max(0, Math.min(windowDeadline, hardCapDeadline) - now);
	}

	/**
	 * base.py:_queue_text_debounce — buffer busy text; schedule a bounded
	 * flush; each arrival resets the timer within the hard cap; mixed-sender
	 * bursts flush the current buffer as its own turn FIRST.
	 */
	queueTextDebounce(sessionKey: string, event: IncomingEvent): void {
		let state = this.textDebounce.get(sessionKey);
		if (
			state !== undefined &&
			!canMergeTextDebounceEvents(state.event, event)
		) {
			// Preserve sender attribution in shared sessions: the current buffer
			// becomes the next pending turn; the new sender starts fresh.
			this.flushTextDebounceNow(sessionKey);
			state = this.textDebounce.get(sessionKey);
			if (
				state !== undefined &&
				!canMergeTextDebounceEvents(state.event, event)
			) {
				const existingPending = this.pendingMessages.get(sessionKey);
				if (
					existingPending !== undefined &&
					canMergeTextDebounceEvents(existingPending, event)
				) {
					mergePendingEvent(this.pendingMessages, sessionKey, event, {
						mergeText: true,
					});
				}
				return;
			}
		}

		const now = this.nowMs();
		if (state === undefined) {
			state = { event, task: null, firstTs: now, lastTs: now };
			this.textDebounce.set(sessionKey, state);
		} else {
			if (event.text) {
				state.event.text = state.event.text
					? `${state.event.text}\n${event.text}`
					: event.text;
			}
			const latestMessageId = event.messageId;
			const latestAnchor = latestMessageId ?? event.replyToMessageId;
			if (latestMessageId !== undefined) {
				state.event.messageId = latestMessageId;
			}
			if (latestAnchor !== undefined) {
				state.event.replyToMessageId = latestAnchor;
			}
			state.lastTs = now;
		}

		if (state.task !== null) {
			(state.task as () => void)(); // cancel previous timer
		}
		const delay = this.textDebounceDelayMs(state);
		const cancel = this.scheduleTimer(delay, () => {
			void this.flushTextDebounceNow(sessionKey);
		});
		state.task = cancel;
	}

	/**
	 * base.py:_flush_text_debounce_now — force-flush one burst into the
	 * pending slot (merge_text=True). Returns true when something flushed.
	 */
	flushTextDebounceNow(sessionKey: string): boolean {
		const state = this.textDebounce.get(sessionKey);
		if (state === undefined) return false;
		if (state.task !== null) {
			(state.task as () => void)(); // cancel the pending timer
		}
		state.task = null;

		const existingPending = this.pendingMessages.get(sessionKey);
		if (
			existingPending !== undefined &&
			!canMergeTextDebounceEvents(existingPending, state.event)
		) {
			return false; // different speaker owns the slot — buffer waits
		}
		this.textDebounce.delete(sessionKey);
		mergePendingEvent(this.pendingMessages, sessionKey, state.event, {
			mergeText: true,
		});
		return true;
	}

	/** base.py:_discard_text_debounce — control commands drop buffered text. */
	discardTextDebounce(sessionKey: string): void {
		const state = this.textDebounce.get(sessionKey);
		if (state === undefined) return;
		this.textDebounce.delete(sessionKey);
		if (state.task !== null) {
			(state.task as () => void)();
		}
	}
}

// -- small helpers -------------------------------------------------------------

/** Task handle for inline (non-frame) dispatch contexts — never cancellable. */
const DETACHED_TASK: GatewayTask = {
	result: Promise.resolve(),
	isDone: () => true,
	cancel: () => {},
	cancelRequested: () => false,
};

function allowsControl(event: IncomingEvent): boolean {
	return event.allowGatewayControl !== false;
}

function chatIdOf(event: IncomingEvent): string {
	return String(event.source?.chatId ?? "");
}

// Re-export for composition convenience (single import site for adapters).
export { resolveCommand, getCommand };
