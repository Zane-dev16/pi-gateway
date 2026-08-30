// bridge.ts — the exec-approval bridge facade: registration lifecycle +
// delivery wiring + surface entry points (07 §8; 03 §5.2 owns how /approve
// REACH the runner — this module owns everything after dispatch).
//
// Hermes anchors (READ-ONLY reference):
//   gateway/run.py::_run_agent_inner register/unregister around run_conversation
//                                    → registerSession / unregisterSession
//   gateway/run.py::_approval_notify_sync        → DeliveryBridge.notify wiring
//   tools/approval.py:clear_session              → clearSession (deny + release)
//
// Registration lifecycle (§8.1): the runner registers the session's notify
// immediately before driving the agent loop; the finally block unregisters —
// which signals ALL blocked waits for the session so interrupt / completion /
// shutdown can NEVER strand a wait — then clears the answer binding.
//
// Fail-loud rule: awaiting a decision on a session WITHOUT a live
// registration collapses into the notify_failed path (entry dropped, post
// hook choice="notify_failed", fail-closed result) — a down bridge must never
// leave requests silently hanging (§8.6).
//
// No live config reload (DEC-013): approvals.timeout is read ONCE at
// construction and fixed for the process lifetime.

import { systemClock, type GatewayClock } from "./clock.js";
import { ApprovalQueues, type NormalizedApprovalData } from "./queue.js";
import { HumanWaitAccounting, humanWaitCeiling } from "./human-wait.js";
import {
	DEFAULT_APPROVAL_TIMEOUT_SECONDS,
	ActivityHeartbeat,
	type AwaitDecisionDeps,
	type AwaitDecisionOptions,
	type AwaitDecisionRequest,
	type DecisionResult,
	type ObserverEmit,
	awaitGatewayDecision,
} from "./gate.js";
import {
	ApprovalCardLedger,
	DeliveryBridge,
	type DeliveryBridgeDeps,
	type DeliveryTarget,
} from "./delivery.js";
import {
	bindingMatches,
	handleApprovalClick,
	handleApprove,
	handleDeny,
	resolveByRequestId,
	routeBareAnswer,
	type AnswerSource,
	type BareRouteResult,
	type ClickResult,
	type IsAnswerAuthorized,
	type SlashAnswerResult,
	type SurfaceDeps,
} from "./surfaces.js";

export interface RegisterSessionOptions {
	/** Delivery target for THIS session's prompts (adapter slice). */
	target: DeliveryTarget;
	chatId: string;
	metadata?: unknown;
	/** The surface/chat that started the session — the ONLY authorized answerer. */
	source?: AnswerSource;
	pauseTyping?(): void;
	resumeTyping?(): void;
}

export interface ApprovalBridgeOptions {
	clock?: GatewayClock;
	/** approvals.timeout in seconds (config default 300); fixed at construction. */
	timeoutSeconds?: number;
	/** HookRegistry-compatible observer emit (pre/post approval hooks). */
	hooks?: { emit?: ObserverEmit };
	isInterrupted?: () => boolean;
	/** Activity sink (human thinking time reported as agent liveness). */
	onActivity?: (note: string) => void;
	/**
	 * Answer-authorization override (bot-level ACL parity of
	 * `_is_callback_user_authorized`); defaults to the registered-binding match.
	 */
	answerPolicyOverride?: IsAnswerAuthorized;
	log?: {
		info?(message: string): void;
		warn?(message: string): void;
		error?(message: string): void;
	};
}

/**
 * One bridge per gateway process. All state is process-local by design
 * (§8.6): a restart loses pending queues and boot-time resume replays the
 * interrupted TURN, not the approval.
 */
export class ExecApprovalBridge {
	readonly queues = new ApprovalQueues();
	readonly ledger = new ApprovalCardLedger();
	private readonly expiredApprovals = new Set<string>();
	private readonly bindings = new Map<string, RegisterSessionOptions>();
	private readonly humanWait: HumanWaitAccounting;
	private gateDeps: AwaitDecisionDeps | null = null;

	constructor(private readonly options: ApprovalBridgeOptions = {}) {
		const timeoutSeconds =
			options.timeoutSeconds ?? DEFAULT_APPROVAL_TIMEOUT_SECONDS;
		this.humanWait = new HumanWaitAccounting(
			options.clock ?? systemClock,
			humanWaitCeiling(timeoutSeconds),
		);
	}

	get timeoutSeconds(): number {
		return this.options.timeoutSeconds ?? DEFAULT_APPROVAL_TIMEOUT_SECONDS;
	}

	get clock(): GatewayClock {
		return this.options.clock ?? systemClock;
	}

	private heartbeat(): ActivityHeartbeat | undefined {
		if (!this.options.onActivity) return undefined;
		return new ActivityHeartbeat(this.clock, this.options.onActivity);
	}

	/**
	 * Default answer policy: caller-IDENTITY vs registered-session bindings
	 * (bot-ACL parity of `_is_callback_user_authorized`) — deliberately NOT
	 * keyed on the peeked approval state, so a duplicate/stale tap still
	 * reaches the honest "already resolved" / "expired" renders instead of a
	 * misleading "not authorized". Inject `answerPolicyOverride` for richer ACLs.
	 */
	private readonly defaultPolicy: IsAnswerAuthorized = (
		source: AnswerSource,
	): boolean => {
		for (const options of this.bindings.values()) {
			if (bindingMatches(options.source, source)) return true;
		}
		return false;
	};

	private surfaceDeps(): SurfaceDeps {
		const deps: SurfaceDeps = {
			queues: this.queues,
			ledger: this.ledger,
			expiredApprovals: this.expiredApprovals,
			isAnswerAuthorized:
				this.options.answerPolicyOverride ?? this.defaultPolicy,
		};
		deps.resumeTyping = (sessionKey: string) => {
			this.bindings.get(sessionKey)?.resumeTyping?.();
		};
		return deps;
	}

	// ── registration lifecycle ──────────────────────────────────────────────

	/**
	 * Register the session's notify binding immediately before the agent run
	 * (`register_gateway_notify` parity). Re-registration overwrites (dict
	 * assign parity). The queue-level callback slot mirrors the reference
	 * registry so introspection (`isNotifyRegistered`) stays honest.
	 */
	registerSession(sessionKey: string, options: RegisterSessionOptions): void {
		this.bindings.set(sessionKey, options);
		if (!this.queues.isNotifyRegistered(sessionKey)) {
			this.queues.registerNotify(sessionKey, () => {});
		}
		this.ensureGateDeps();
	}

	/**
	 * Unregister after the run's finally block: signals ALL blocked waits for
	 * the session so they can never hang, and clears the answer binding.
	 */
	unregisterSession(sessionKey: string): void {
		this.queues.unregisterNotify(sessionKey);
		this.bindings.delete(sessionKey);
	}

	/**
	 * Session-boundary cleanup (`clear_session` queue half): deny + release
	 * blocked waits IMMEDIATELY instead of idling to timeout.
	 */
	clearSession(sessionKey: string): void {
		this.queues.clearSession(sessionKey);
		this.bindings.delete(sessionKey);
		this.expiredApprovals.delete(sessionKey);
	}

	isRegistered(sessionKey: string): boolean {
		return this.bindings.has(sessionKey);
	}

	/**
	 * Record that this session's blocking approval expired so a later
	 * /approve //deny reports "approval expired" rather than "nothing
	 * pending" (runner `_pending_approvals` marker parity, §8.5 stale row).
	 */
	markApprovalExpired(sessionKey: string): void {
		this.expiredApprovals.add(sessionKey);
	}

	// ── gate ────────────────────────────────────────────────────────────────

	/**
	 * Build the gate deps once; the notify closure fails LOUDLY when the
	 * session has no live registration (bridge down ⇒ notify_failed, not a hang).
	 */
	private ensureGateDeps(): AwaitDecisionDeps {
		if (this.gateDeps) return this.gateDeps;

		const deliver = async (
			sessionKey: string,
			data: NormalizedApprovalData,
		): Promise<void> => {
			const binding = this.bindings.get(sessionKey);
			if (!binding) {
				throw new Error(
					`approval bridge down: no delivery registered for session ${sessionKey}`,
				);
			}
			const deliveryDeps: DeliveryBridgeDeps = {
				target: binding.target,
				chatId: binding.chatId,
				ledger: this.ledger,
				clock: this.clock,
			};
			if (binding.metadata !== undefined) {
				deliveryDeps.metadata = binding.metadata;
			}
			if (binding.pauseTyping !== undefined) {
				deliveryDeps.pauseTyping = binding.pauseTyping;
			}
			if (this.options.log !== undefined) {
				deliveryDeps.log = this.options.log;
			}
			const bridge = new DeliveryBridge(deliveryDeps);
			await bridge.notify({
				sessionKey,
				command: data.command,
				description: data.description,
				allowPermanent: data.allowPermanent,
				allowSession: data.allowSession,
				smartDenied: data.smartDenied,
			});
		};

		const deps: AwaitDecisionDeps = {
			queues: this.queues,
			clock: this.clock,
			timeoutSeconds: this.timeoutSeconds,
			humanWait: this.humanWait,
			notify: deliver,
		};
		if (this.options.hooks !== undefined) deps.hooks = this.options.hooks;
		const heartbeatInstance = this.heartbeat();
		if (heartbeatInstance !== undefined) deps.heartbeat = heartbeatInstance;
		if (this.options.isInterrupted !== undefined) {
			deps.isInterrupted = this.options.isInterrupted;
		}
		if (this.options.log !== undefined) deps.log = this.options.log;

		this.gateDeps = deps;
		return deps;
	}

	/**
	 * Block the calling agent turn until the approval resolves or times out.
	 * On an unresolved deadline the session is marked expired for the
	 * stale-answer lane before the result returns.
	 */
	async awaitDecision(
		sessionKey: string,
		request: AwaitDecisionRequest,
		options: AwaitDecisionOptions = {},
	): Promise<DecisionResult> {
		const deps = this.ensureGateDeps();
		const result = await awaitGatewayDecision(
			deps,
			sessionKey,
			request,
			options,
		);
		if (!result.resolved || result.choice === null) {
			this.markApprovalExpired(sessionKey);
		}
		return result;
	}

	/** Total human-wait seconds recorded for a session (#79719 accounting). */
	humanWaitSeconds(sessionKey: string): number {
		return this.humanWait.seconds(sessionKey);
	}

	// ── answer surfaces ─────────────────────────────────────────────────────

	/** `/approve [/all] [session|always]` — L1 bypass lands here. */
	approve(input: {
		sessionKey: string;
		rawArgs: string;
		source?: AnswerSource;
	}): SlashAnswerResult {
		return handleApprove(this.surfaceDeps(), input);
	}

	/** `/deny [/all] [<reason>]` — reason ≤280 chars relayed verbatim. */
	deny(input: {
		sessionKey: string;
		rawArgs: string;
		source?: AnswerSource;
	}): SlashAnswerResult {
		return handleDeny(this.surfaceDeps(), input);
	}

	/** Inline-button click (`ea:<choice>:<id>`). */
	async click(input: {
		callbackData: string;
		source: AnswerSource;
		userDisplay?: string;
	}): Promise<ClickResult> {
		return handleApprovalClick(this.surfaceDeps(), input);
	}

	/** Bare-word routing ("yes"/"no"/… while blocked, control-gated). */
	bareWord(input: {
		sessionKey: string;
		text: string;
		controlAllowed: boolean;
	}): BareRouteResult {
		return routeBareAnswer(this.surfaceDeps(), input);
	}

	/** api_server request_id-targeted resolution (/v1/runs lane). */
	apiResolve(input: {
		sessionKey: string;
		requestId: string;
		choice: string;
		source?: AnswerSource;
	}): SlashAnswerResult {
		return resolveByRequestId(this.surfaceDeps(), input);
	}
}
