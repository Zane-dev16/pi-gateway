// pi_platforms/webhook/runs — the api_server-class /v1/runs lane: UNBOUNDED
// windows held open under lifecycle management, cooperative interruption
// reachable (approval / steer / stop), C5-corrected split sibling of the
// bounded webhook routes.
//
// Ported from the READ-ONLY Hermes reference (api_server.py):
//   _handle_runs (@7495)        — run_id allocation, 202 {run_id,status},
//                                 approval namespace IS the run_id so
//                                 concurrent runs can never cross-resolve
//   _set_run_status (@7386)     — queued→running→waiting_for_approval→
//                                 running→completed/failed/cancelled/stopping
//   _make_run_event_callback (@7401) — typed SSE event vocabulary
//   _handle_run_approval (@7989)— pop-or-409 (approval_not_active /
//                                 approval_not_pending), choices
//                                 once/session/always/deny (+aliases)
//   _handle_steer_run (@8077)   — only status=="running" steerable, else 409
//   _handle_stop_run (@8137)    — cooperative: sets stopping + interrupts;
//                                 the executor may outlive the HTTP request
//   _RUN_STREAM_TTL=300s / _RUN_STATUS_TTL=3600s (@7388)

import { OneShotPendingStore } from "../kit/callback-router.js";

export const RUN_STREAM_TTL_MS = 300_000;
export const RUN_STATUS_TTL_MS = 3_600_000;

export const APPROVAL_CHOICES = ["once", "session", "always", "deny"] as const;
export type ApprovalChoice = (typeof APPROVAL_CHOICES)[number];

const CHOICE_ALIASES: Record<string, ApprovalChoice> = {
	approve: "once",
	approved: "once",
	allow: "once",
};

export function normalizeApprovalChoice(raw: string): ApprovalChoice | null {
	const trimmed = raw.trim().toLowerCase();
	const aliased = CHOICE_ALIASES[trimmed];
	if (aliased !== undefined) return aliased;
	return (APPROVAL_CHOICES as readonly string[]).includes(trimmed)
		? (trimmed as ApprovalChoice)
		: null;
}

export type RunStatus =
	| "queued"
	| "running"
	| "waiting_for_approval"
	| "stopping"
	| "completed"
	| "failed"
	| "cancelled";

/**
 * Typed SSE event vocabulary (api_server.py event names). The wire mapper in
 * server.ts renders these as `event: <type>` frames whose data payloads carry
 * snake_case run_id/session_id/seq/ts (webhook-46 conformance).
 */
export type RunEvent =
	| { type: "assistant.delta"; runId: string; text: string }
	| {
			type: "tool.started";
			runId: string;
			toolName: string;
			preview?: string | undefined;
	  }
	| {
			type: "tool.completed";
			runId: string;
			toolName: string;
			duration?: number | undefined;
			isError?: boolean | undefined;
	  }
	| {
			type: "tool.failed";
			runId: string;
			toolName: string;
			preview?: string | undefined;
	  }
	| {
			type: "approval.request";
			runId: string;
			approvalId: number;
			command: string;
			choices: readonly ApprovalChoice[];
	  }
	| {
			type: "approval.responded";
			runId: string;
			approvalId: number;
			choice: ApprovalChoice;
			/** Approvals resolved by this response (_handle_run_approval @8147). */
			resolved: number;
	  }
	| { type: "run.steered"; runId: string; text: string }
	| {
			type: "run.completed";
			runId: string;
			output: string;
			/** Undelivered steer text rides the terminal event (@7926). */
			pendingSteer?: string | undefined;
	  }
	| { type: "run.failed"; runId: string; error: string }
	| { type: "run.cancelled"; runId: string };

/** Token usage block surfaced on the completed status view (:7926-7936). */
export interface RunUsage {
	promptTokens: number;
	completionTokens: number;
	totalTokens: number;
}

/**
 * Executor completion payload. A bare string stays legal (usage omitted) so
 * pre-existing executors compile unchanged.
 */
export interface RunCompletion {
	output: string;
	usage?: RunUsage | undefined;
}

/**
 * The executor seam: production wires the real agent loop; tests script the
 * turn. Cooperative interruption rides `shouldStop()` polls plus the
 * interrupt latch (stop parity of request_hard_interrupt).
 */
export interface RunExecutorControls {
	runId: string;
	emitDelta(text: string): void;
	/** api_server.py tool_progress_callback parity (typed SSE vocabulary). */
	emitToolProgress(
		event:
			| { name: "tool.started"; toolName: string; preview?: string }
			| {
					name: "tool.completed";
					toolName: string;
					duration?: number;
					isError?: boolean;
			  }
			| { name: "tool.failed"; toolName: string; preview?: string },
	): void;
	shouldStop(): boolean;
	/**
	 * Hold the run open for a human decision. Resolves with the approved
	 * choice; REJECTS when the run is stopped/cancelled while waiting.
	 */
	requestApproval(
		command: string,
		choices?: readonly ApprovalChoice[],
	): Promise<ApprovalChoice>;
}

export type RunExecutor = (
	controls: RunExecutorControls,
	input: string,
) => Promise<string | RunCompletion>;

/**
 * Pollable status view — api_server.py:_set_run_status shape
 * ({'object':'hermes.run','run_id','status','created_at','updated_at',...}).
 */
export interface RunView {
	object: "hermes.run";
	runId: string;
	status: RunStatus;
	/** Epoch SECONDS (Hermes time.time() parity). */
	createdAt: number;
	updatedAt: number;
	sessionId: string;
	/** Requested model — stored at QUEUED (body.get("model") @7690 parity). */
	model?: string | undefined;
	usage?: RunUsage | undefined;
	/** Undelivered steer text retained on the completed view (@7926-7936). */
	pendingSteer?: string | undefined;
	output?: string | undefined;
	error?: string | undefined;
	lastEvent?: string | undefined;
}

interface RunRecord {
	runId: string;
	/** Hermes defaults session_id to the run id when the caller omits one. */
	sessionId: string;
	status: RunStatus;
	events: RunEvent[];
	listener: ((event: RunEvent) => void) | null;
	pendingSteer: string | null;
	stopRequested: boolean;
	interruptListeners: Array<() => void>;
	createdAtMs: number;
	updatedAtMs: number;
	completedAtMs: number | null;
	/** Last emitted event name (status view `last_event`). */
	lastEventName: string | null;
	/** Monotonic per-run SSE sequence (wire field `seq`). */
	seqCounter: number;
	/** The currently-open approval for THIS run (namespace = run_id parity). */
	currentApprovalId: number | null;
	/** Requested model, fixed at start (queued-status field parity). */
	model: string | null;
	/** Usage reported by the executor on completion. */
	usage: RunUsage | null;
}

export class RunRegistry {
	private readonly runs = new Map<string, RunRecord>();
	private readonly approvals = new OneShotPendingStore();
	private seq = 0;
	private approvalSeq = 0;
	private readonly nowMs: () => number;
	private readonly spawn: (task: () => Promise<void>) => void;

	constructor(
		opts: {
			nowMs?: (() => number) | undefined;
			spawn?: ((task: () => Promise<void>) => void) | undefined;
		} = {},
	) {
		this.nowMs = opts.nowMs ?? (() => Date.now());
		this.spawn =
			opts.spawn ??
			((task) => {
				void task().catch(() => {});
			});
	}

	/**
	 * Start a run. The executor spawns AFTER the run record exists (Hermes
	 * registers the queue before the task), so events emitted immediately are
	 * captured, never raced.
	 */
	start(
		input: string,
		executor: RunExecutor,
		opts: { sessionId?: string | undefined; model?: string | undefined } = {},
	): string {
		this.seq += 1;
		const runId = `run_${String(this.seq).padStart(6, "0")}`;
		const nowMsValue = this.nowMs();
		const record: RunRecord = {
			runId,
			sessionId: opts.sessionId ?? runId,
			status: "running",
			events: [],
			listener: null,
			pendingSteer: null,
			stopRequested: false,
			interruptListeners: [],
			createdAtMs: nowMsValue,
			updatedAtMs: nowMsValue,
			completedAtMs: null,
			lastEventName: null,
			seqCounter: 0,
			currentApprovalId: null,
			// Hermes stores body.get("model", self._model_name) at QUEUED; the
			// caller (HTTP lane) supplies the default-model fallback.
			model: opts.model ?? null,
			usage: null,
		};
		this.runs.set(runId, record);

		this.spawn(async () => {
			const controls: RunExecutorControls = {
				runId,
				emitDelta: (text) => {
					this.pushEvent(record, { type: "assistant.delta", runId, text });
				},
				emitToolProgress: (event) => {
					if (event.name === "tool.started") {
						this.pushEvent(record, {
							type: "tool.started",
							runId,
							toolName: event.toolName,
							preview: event.preview,
						});
					} else if (event.name === "tool.completed") {
						this.pushEvent(record, {
							type: "tool.completed",
							runId,
							toolName: event.toolName,
							duration: event.duration,
							isError: event.isError,
						});
					} else {
						this.pushEvent(record, {
							type: "tool.failed",
							runId,
							toolName: event.toolName,
							preview: event.preview,
						});
					}
				},
				shouldStop: () => record.stopRequested,
				requestApproval: (command, choices = APPROVAL_CHOICES) =>
					this.openApproval(record, command, choices),
			};
			try {
				const result = await executor(controls, input);
				const outcome =
					typeof result === "string" ? { output: result } : result;
				if (record.status === "stopping") {
					this.finishCancelled(record);
				} else {
					record.status = "completed";
					record.completedAtMs = this.nowMs();
					record.usage = outcome.usage ?? null;
					// Undelivered steer text rides the terminal event + status
					// (api_server.py turn_finalizer parity @7889-7936).
					const pendingSteer =
						record.pendingSteer !== null && record.pendingSteer.length > 0
							? record.pendingSteer
							: undefined;
					this.pushEvent(record, {
						type: "run.completed",
						runId,
						output: outcome.output,
						pendingSteer,
					});
				}
			} catch (err) {
				if (
					record.status === "stopping" ||
					record.stopRequested ||
					err instanceof RunStoppedError
				) {
					this.finishCancelled(record);
				} else {
					record.status = "failed";
					record.completedAtMs = this.nowMs();
					this.pushEvent(record, {
						type: "run.failed",
						runId,
						error: err instanceof Error ? err.message : String(err),
					});
				}
			}
		});
		return runId;
	}

	/** THE approval gate: namespace = run-scoped id; hold-open via deferred. */
	private openApproval(
		record: RunRecord,
		command: string,
		choices: readonly ApprovalChoice[],
	): Promise<ApprovalChoice> {
		return new Promise<ApprovalChoice>((resolve, reject) => {
			if (record.stopRequested) {
				reject(new RunStoppedError());
				return;
			}
			this.approvalSeq += 1;
			const approvalId = this.approvalSeq;
			// Atomic POP store ⇒ double-respond dedup (DEC-016 seam parity);
			// the entry carries the RUN id as its session key.
			this.approvals.register(approvalId, record.runId);
			record.currentApprovalId = approvalId;
			record.status = "waiting_for_approval";
			this.pushEvent(record, {
				type: "approval.request",
				runId: record.runId,
				approvalId,
				command,
				choices,
			});
			const onInterrupt = (): void => {
				reject(new RunStoppedError());
			};
			record.interruptListeners.push(onInterrupt);
			const poll = setInterval(() => {
				if (record.stopRequested) {
					clearInterval(poll);
					this.approvals.pop(approvalId, Number.MAX_SAFE_INTEGER);
					reject(new RunStoppedError());
				}
			}, 5);
			poll.unref?.();
			this.approvalWaiters.set(approvalId, (choice) => {
				clearInterval(poll);
				if (record.currentApprovalId === approvalId) {
					record.currentApprovalId = null;
				}
				const idx = record.interruptListeners.indexOf(onInterrupt);
				if (idx >= 0) record.interruptListeners.splice(idx, 1);
				resolve(choice);
			});
		});
	}

	private readonly approvalWaiters = new Map<
		number,
		(c: ApprovalChoice) => void
	>();

	/**
	 * POST /v1/runs/:id/approvals — BY-RUN resolution (approval namespace is
	 * the run_id): the run's open approval pops atomically; none open ⇒
	 * approval_not_active, already consumed ⇒ approval_not_pending. With
	 * `resolveAll` (body all/resolve_all booleans, _handle_run_approval @8121
	 * parity) EVERY live approval registered under THIS run resolves FIFO and
	 * `resolved` carries the drained count; ONE SSE frame reports it (@8147).
	 */
	respondApprovalForRun(
		runId: string,
		rawChoice: string,
		opts: { resolveAll?: boolean | undefined } = {},
	):
		| { ok: true; choice: ApprovalChoice; resolved: number }
		| {
				ok: false;
				code:
					| "unknown_run"
					| "approval_not_active"
					| "approval_not_pending"
					| "invalid_choice";
		  } {
		const record = this.runs.get(runId);
		if (record === undefined) return { ok: false, code: "unknown_run" };
		// No OPEN approval on this run ⇒ approval_not_active (Hermes parity:
		// _handle_run_approval distinguishes never/finished-open from consumed).
		if (record.currentApprovalId === null) {
			return { ok: false, code: "approval_not_active" };
		}
		const choice = normalizeApprovalChoice(rawChoice);
		if (choice === null) return { ok: false, code: "invalid_choice" };

		if (opts.resolveAll === true) {
			// resolve_gateway_approval(resolve_all=True): drain the whole
			// session queue FIFO (tools/approval.py:2850). The store keys each
			// entry by sessionKey=run_id, so oldestIdForSession IS the queue.
			let resolvedCount = 0;
			let firstApprovalId = record.currentApprovalId;
			for (;;) {
				const oldest = this.approvals.oldestIdForSession(runId);
				if (oldest === null) break;
				const id = Number(oldest);
				if (resolvedCount === 0) firstApprovalId = id;
				const outcome = this.resolveViaStore(id, choice);
				if (outcome.state === "resolved") resolvedCount += 1;
			}
			if (resolvedCount <= 0) {
				return { ok: false, code: "approval_not_pending" };
			}
			this.pushEvent(record, {
				type: "approval.responded",
				runId,
				approvalId: firstApprovalId ?? 0,
				choice,
				resolved: resolvedCount,
			});
			return { ok: true, choice, resolved: resolvedCount };
		}

		const outcome = this.resolveViaStore(record.currentApprovalId, choice);
		if (outcome.state !== "resolved") {
			return {
				ok: false,
				code:
					outcome.state === "absent"
						? "approval_not_pending"
						: "approval_not_active",
			};
		}
		this.pushEvent(record, {
			type: "approval.responded",
			runId,
			approvalId: record.currentApprovalId,
			choice,
			resolved: 1,
		});
		return { ok: true, choice, resolved: 1 };
	}

	/** Numeric-id variant (the wire grammar's shared resolution seam). */
	respondApproval(
		approvalId: number,
		rawChoice: string,
	):
		| { ok: true; runId: string; choice: ApprovalChoice; resolved: number }
		| {
				ok: false;
				code: "approval_not_active" | "approval_not_pending" | "invalid_choice";
		  } {
		const choice = normalizeApprovalChoice(rawChoice);
		if (choice === null) return { ok: false, code: "invalid_choice" };
		const outcome = this.resolveViaStore(approvalId, choice);
		if (outcome.state !== "resolved") {
			return {
				ok: false,
				code:
					outcome.state === "absent"
						? "approval_not_pending"
						: "approval_not_active",
			};
		}
		const record =
			outcome.runId === null ? undefined : this.runs.get(outcome.runId);
		if (record !== undefined) {
			this.pushEvent(record, {
				type: "approval.responded",
				runId: record.runId,
				approvalId,
				choice,
				resolved: 1,
			});
		}
		return { ok: true, runId: outcome.runId ?? "", choice, resolved: 1 };
	}

	/**
	 * Atomic POP + waiter resolution WITHOUT event emission — the two entry
	 * points above own their SSE frames (Hermes emits one approval.responded
	 * frame per HTTP response, carrying the total resolved count). Also flips
	 * a waiting_for_approval run back to running (_handle_run_approval @8153).
	 */
	private resolveViaStore(
		approvalId: number,
		choice: ApprovalChoice,
	):
		| { state: "resolved"; runId: string | null }
		| { state: "absent" | "no_waiter" } {
		const pop = this.approvals.pop(approvalId, Number.MAX_SAFE_INTEGER);
		if (pop.state === "absent") return { state: "absent" };
		const waiter = this.approvalWaiters.get(approvalId);
		this.approvalWaiters.delete(approvalId);
		if (waiter === undefined) return { state: "no_waiter" };
		const runId = pop.state === "live" ? pop.sessionKey : null;
		const record = runId === null ? undefined : this.runs.get(runId);
		waiter(choice);
		if (record !== undefined && record.status === "waiting_for_approval") {
			record.status = "running";
		}
		return { state: "resolved", runId };
	}

	/** POST /v1/runs/:id/steer — only RUNNING runs accept steer text. */
	steer(
		runId: string,
		text: string,
	):
		| { ok: true }
		| { ok: false; code: "unknown_run" | "run_not_accepting_steer" } {
		const record = this.runs.get(runId);
		if (record === undefined) return { ok: false, code: "unknown_run" };
		if (record.status !== "running") {
			return { ok: false, code: "run_not_accepting_steer" };
		}
		record.pendingSteer = text;
		this.pushEvent(record, { type: "run.steered", runId, text });
		return { ok: true };
	}

	/** Consumed by the executor between phases (pendingSteer handoff). */
	consumeSteer(runId: string): string | null {
		const record = this.runs.get(runId);
		if (record === undefined || record.pendingSteer === null) return null;
		const text = record.pendingSteer;
		record.pendingSteer = null;
		return text;
	}

	/**
	 * POST /v1/runs/:id/stop — COOPERATIVE interruption. Once a run is
	 * TERMINAL its agent/task refs are gone (api_server.py pops them in the
	 * _run_and_close finally-block @7975), so a late stop answers 404
	 * run_not_found exactly like an unknown id (_handle_stop_run @8199).
	 */
	stop(runId: string): { ok: boolean; code?: string | undefined } {
		const record = this.runs.get(runId);
		if (record === undefined || isTerminal(record.status)) {
			return { ok: false, code: "unknown_run" };
		}
		record.stopRequested = true;
		record.status = "stopping";
		for (const listener of [...record.interruptListeners]) {
			listener();
		}
		return { ok: true };
	}

	/** Subscribe: replays buffered events then streams live ones. */
	subscribe(runId: string, listener: (event: RunEvent) => void): void {
		const record = this.runs.get(runId);
		if (record === undefined) return;
		for (const event of record.events) listener(event);
		record.listener = listener;
	}

	detach(runId: string): void {
		const record = this.runs.get(runId);
		if (record !== undefined) record.listener = null;
	}

	status(runId: string): RunView | null {
		const record = this.runs.get(runId);
		if (record === undefined) return null;
		const view: RunView = {
			object: "hermes.run",
			runId,
			status: record.status,
			createdAt: record.createdAtMs / 1000,
			updatedAt: Math.max(record.updatedAtMs, record.createdAtMs) / 1000,
			sessionId: record.sessionId,
		};
		if (record.model !== null) view.model = record.model;
		if (record.usage !== null) view.usage = record.usage;
		if (record.pendingSteer !== null && record.pendingSteer.length > 0) {
			view.pendingSteer = record.pendingSteer;
		}
		for (const event of [...record.events].reverse()) {
			if (event.type === "run.completed") view.output = event.output;
			if (event.type === "run.failed") view.error = event.error;
			break;
		}
		if (record.lastEventName !== null) view.lastEvent = record.lastEventName;
		return view;
	}

	/** All run ids (observability/tests; oldest first). */
	runIds(): string[] {
		return [...this.runs.keys()];
	}

	/** Terminal-status retention sweep (injected clock; tests drive). */
	pruneExpired(): number {
		const now = this.nowMs();
		let pruned = 0;
		for (const [runId, record] of this.runs) {
			if (
				isTerminal(record.status) &&
				record.completedAtMs !== null &&
				now - record.completedAtMs > RUN_STATUS_TTL_MS
			) {
				this.runs.delete(runId);
				pruned += 1;
			}
		}
		return pruned;
	}

	private pushEvent(record: RunRecord, event: RunEvent): void {
		record.events.push(event);
		record.updatedAtMs = this.nowMs();
		record.lastEventName = event.type;
		record.seqCounter += 1;
		record.listener?.(event);
	}

	private finishCancelled(record: RunRecord): void {
		record.status = "cancelled";
		record.completedAtMs = this.nowMs();
		this.pushEvent(record, { type: "run.cancelled", runId: record.runId });
	}
}

export class RunStoppedError extends Error {
	constructor() {
		super("run stopped while waiting");
		this.name = "RunStoppedError";
	}
}

function isTerminal(status: RunStatus): boolean {
	return (
		status === "completed" || status === "failed" || status === "cancelled"
	);
}
