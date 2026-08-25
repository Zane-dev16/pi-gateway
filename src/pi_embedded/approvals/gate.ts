// gate.ts — THE blocking approval gate: enqueue → notify → block the agent
// turn exactly like the CLI's synchronous input(), with coalescing, bounded
// wait slices, activity heartbeats, interrupt-first deny, and outcome
// normalization (07 §8 prologue, §8.2 binding, §8.4).
//
// Hermes anchors (READ-ONLY reference):
//   tools/approval.py:_await_gateway_decision    → awaitGatewayDecision
//   tools/approval.py:_await_coalesced_leader    → awaitCoalescedLeader
//   tools/approval.py:_get_approval_timeout      → timeoutSeconds dep (default 300)
//   tools/environments/base.py:touch_activity_if_due
//                                                → ActivityHeartbeat.maybeDue
//   tools/approval.py:human_wait_window          → HumanWaitAccounting.begin
//
// Binding clauses realized here:
//   • Deadline = approvals.timeout (config default 300 s), fixed at
//     construction — DEC-013 forbids live config reload.
//   • The wait polls in slices ≤1 s firing activity heartbeats every ~10 s so
//     inactivity monitors see liveness while a human thinks; otherwise the
//     watchdog kills a healthy agent mid-wait.
//   • Interrupt-first: an interrupt signal resolves DENY so the loop unwinds
//     via the normal denial path (#8697). A follower's interrupt denies ONLY
//     the follower — the leader thread handles its own signal.
//   • Coalescing adoption keeps consent strict: session/always ⇒ adopted
//     approval; deny ⇒ adopted refusal (re-asking what the user just declined
//     is prompt spam AND an evasion path); timeout ⇒ unresolved like a direct
//     timeout; `once` ⇒ single-use consent covering ONLY the leader, so the
//     follower falls through to a FRESH prompt (own notify + hooks cycle).
//   • Outcome normalization: an unresolved deadline reports choice "timeout"
//     to the post hook. Timeout ≠ explicit deny for telemetry/plugins — but
//     BOTH leave the command blocked (fail-closed).
//   • Notify failure (raise OR no registered notify) ⇒ entry dropped, post
//     hook choice="notify_failed", gate returns notifyFailed — the agent sees
//     a failed-guard result, NEVER a hang (§8.6).
//   • Unregister mid-wait settles waiters without a result ⇒ normalized to
//     "timeout" (event-set-without-result parity) — loud release, not a hang.

import type { GatewayClock } from "./clock.js";
import {
	ApprovalEntry,
	type ApprovalChoice,
	type ApprovalQueues,
	type NormalizedApprovalData,
} from "./queue.js";
import type { HumanWaitAccounting } from "./human-wait.js";

/** Default approvals.timeout (config `approvals.timeout`, read once — DEC-013). */
export const DEFAULT_APPROVAL_TIMEOUT_SECONDS = 300;

/**
 * Cron approval modes (parity tools/approval.py:_get_cron_approval_mode).
 * Default DENY: cron jobs run without a user present to answer prompts.
 */
export type CronApprovalMode = "deny" | "approve";

/**
 * Normalize a raw `approvals.cron_mode` config value (parity vocabulary:
 * approve/off/allow/yes ⇒ approve; anything else — including absent/garbage
 * — fails safe to deny).
 */
export function normalizeCronApprovalMode(raw: unknown): CronApprovalMode {
	const mode = String(raw ?? "")
		.trim()
		.toLowerCase();
	return mode === "approve" ||
		mode === "off" ||
		mode === "allow" ||
		mode === "yes"
		? "approve"
		: "deny";
}

/**
 * The BLOCKED reason returned to a denied cron request (parity of the
 * cron_deny_message shapes in tools/approval.py).
 */
export function cronDenyReason(data: NormalizedApprovalData): string {
	return (
		`BLOCKED: ${data.description || data.command} requires approval, but ` +
		"cron jobs run without a user present to approve it. Find an alternative " +
		"approach that avoids this command. To allow commands like this in cron " +
		"jobs, set approvals.cron_mode: approve in config.yaml."
	);
}

/** Activity-heartbeat cadence (~10 s; touch_activity_if_due default interval). */
export const ACTIVITY_HEARTBEAT_INTERVAL_S = 10;

/** Wait-slice cap (Hermes polls in slices ≤1 s to keep the loop responsive). */
export const WAIT_SLICE_MS = 1000;

/**
 * Observer emit seam (HookRegistry.emit shape — emit-and-log, never throws
 * into the gate). Event names mirror Hermes `_fire_approval_hook` calls:
 * `pre_approval_request` / `post_approval_response` with camelCase payload.
 */
export type ObserverEmit = (
	eventType: string,
	context?: Record<string, unknown>,
) => Promise<void>;

export interface ApprovalHooks {
	emit?: ObserverEmit;
}

export interface ApprovalHookPayload {
	command: string;
	description: string;
	patternKey: string;
	patternKeys: string[];
	sessionKey: string;
	surface: string;
	choice?: string;
	coalesced?: boolean;
}

/** Fire an observer approval hook; failures never reach the agent thread. */
export async function fireApprovalHook(
	hooks: ApprovalHooks | undefined,
	eventName: "pre_approval_request" | "post_approval_response",
	payload: ApprovalHookPayload,
	log?: { warn?(message: string): void },
): Promise<void> {
	if (!hooks?.emit) return;
	try {
		await hooks.emit(eventName, { ...payload });
	} catch (err) {
		log?.warn?.(
			`[approvals] hook '${eventName}' emission failed: ${String(err)}`,
		);
	}
}

interface ActivityState {
	lastTouchSeconds: number;
	startSeconds: number;
}

/**
 * touch_activity_if_due parity: fires at most once per interval (default
 * 10 s), swallowing sink exceptions so liveness reporting can never break
 * the wait.
 */
export class ActivityHeartbeat {
	constructor(
		private readonly clock: GatewayClock,
		private readonly sink: ((note: string) => void) | undefined,
		private readonly intervalSeconds: number = ACTIVITY_HEARTBEAT_INTERVAL_S,
	) {}

	maybeDue(state: ActivityState, label: string): void {
		const now = this.clock.nowSeconds();
		if (now - state.lastTouchSeconds < this.intervalSeconds) return;
		state.lastTouchSeconds = now;
		try {
			this.sink?.(
				`${label} (${Math.trunc(now - state.startSeconds)}s elapsed)`,
			);
		} catch {
			/* heartbeat failure must never break the wait */
		}
	}
}

export interface DecisionResult {
	resolved: boolean;
	choice: ApprovalChoice | null;
	reason?: string | null;
	coalesced?: boolean;
	/** Notify bridge failed loudly — the command did NOT run (fail-closed). */
	notifyFailed?: boolean;
}

export interface AwaitDecisionRequest {
	command: string;
	description: string;
	patternKey?: string;
	patternKeys?: readonly string[];
	allowPermanent?: boolean;
	allowSession?: boolean;
	smartDenied?: boolean;
}

export interface AwaitDecisionOptions {
	/** Answer surface requesting the gate (hook observability only). */
	surface?: string;
}

/**
 * Sends the prompt for one session. Implementations either resolve (prompt
 * delivered through the delivery bridge) or THROW — both a throw and a
 * missing registration collapse into the loud `notify_failed` path.
 */
export type SessionNotify = (
	sessionKey: string,
	data: NormalizedApprovalData,
) => Promise<void>;

export interface AwaitDecisionDeps {
	queues: ApprovalQueues;
	hooks?: ApprovalHooks;
	clock: GatewayClock;
	timeoutSeconds: number;
	humanWait: HumanWaitAccounting;
	heartbeat?: ActivityHeartbeat;
	isInterrupted?: () => boolean;
	notify: SessionNotify;
	/**
	 * Cron-session detection (parity tools/approval.py:
	 * _is_cron_approval_context). Wired by the composition root to the cron
	 * module's ambient probe. When TRUE the gate resolves IMMEDIATELY from
	 * `cronMode` below WITHOUT enqueueing — a cron job must never leave a
	 * pending approval with no listener blocking the job for the full timeout.
	 */
	isCronSession?: (() => boolean) | undefined;
	/**
	 * `approvals.cron_mode`: raw config string, resolved mode, or a resolver
	 * for any of those. DEFAULTS TO DENY (fail-closed parity) however it is
	 * provided; only consulted when `isCronSession` fires.
	 */
	cronMode?:
		| string
		| CronApprovalMode
		| (() => string | CronApprovalMode)
		| undefined;
	log?: { warn?(message: string): void; info?(message: string): void };
}

function resolveCronMode(deps: AwaitDecisionDeps): CronApprovalMode {
	try {
		const raw =
			typeof deps.cronMode === "function" ? deps.cronMode() : deps.cronMode;
		return normalizeCronApprovalMode(raw);
	} catch {
		// A failing config read fails SAFE to deny (parity of _get_cron_approval_mode's
		// except-deny guard) — never let a broken resolver widen cron authority.
		return "deny";
	}
}

function hookBase(
	data: NormalizedApprovalData,
	sessionKey: string,
	surface: string,
): ApprovalHookPayload {
	return {
		command: data.command,
		description: data.description,
		patternKey: data.patternKey,
		patternKeys: [...data.patternKeys],
		sessionKey,
		surface,
	};
}

function remainingSliceMs(
	clock: GatewayClock,
	deadlineSeconds: number,
): number {
	const remainingMs = (deadlineSeconds - clock.nowSeconds()) * 1000;
	return Math.max(0, Math.min(WAIT_SLICE_MS, Math.floor(remainingMs)));
}

type WaitWinner = "entry" | "slice";

/**
 * One bounded poll slice: settle as soon as the entry resolves, else after
 * ≤1 s so interrupts/deadlines/heartbeats stay responsive.
 */
async function waitSlice(
	wait: Promise<void>,
	clock: GatewayClock,
	sliceMs: number,
): Promise<WaitWinner> {
	void wait.catch(() => {}); // shadow late rejection after a slice win
	return Promise.race([
		wait.then(() => "entry" as WaitWinner),
		clock.sleepMs(sliceMs).then(() => "slice" as WaitWinner),
	]);
}

/**
 * Wait on an already-pending identical approval instead of re-prompting
 * (`_await_coalesced_leader`). Returns null ONLY when the leader resolved
 * `once` — single-use consent covers only the leader's execution and the
 * caller must issue a fresh prompt.
 */
export async function awaitCoalescedLeader(
	deps: AwaitDecisionDeps,
	sessionKey: string,
	leader: ApprovalEntry,
	data: NormalizedApprovalData,
	options: AwaitDecisionOptions = {},
): Promise<DecisionResult | null> {
	const surface = options.surface ?? "gateway";
	await fireApprovalHook(
		deps.hooks,
		"pre_approval_request",
		{ ...hookBase(data, sessionKey, surface), coalesced: true },
		deps.log,
	);

	const deadline = deps.clock.nowSeconds() + deps.timeoutSeconds;
	let choice: ApprovalChoice | null = null;
	let resolved = false;
	let interrupted = false;

	const closeWait = deps.humanWait.begin(sessionKey);
	try {
		for (;;) {
			if (deps.isInterrupted?.() === true) {
				// Deny only OUR follower; the leader thread handles its own signal.
				choice = "deny";
				resolved = true;
				interrupted = true;
				break;
			}
			const sliceMs = remainingSliceMs(deps.clock, deadline);
			if (sliceMs <= 0) break;
			const winner = await waitSlice(leader.wait, deps.clock, sliceMs);
			if (winner === "entry") {
				choice = leader.result;
				resolved = choice !== null;
				break;
			}
		}
	} finally {
		closeWait();
	}

	if (interrupted) {
		deps.log?.info?.(
			`Coalesced approval wait interrupted by user signal — returning deny for session ${sessionKey}`,
		);
	}

	if (choice === "once") {
		// Single-use consent — the caller re-prompts. The post hook fires for
		// the fresh prompt's own lifecycle, not here.
		return null;
	}

	await fireApprovalHook(
		deps.hooks,
		"post_approval_response",
		{
			...hookBase(data, sessionKey, surface),
			choice: !resolved || choice === null ? "timeout" : choice,
			coalesced: true,
		},
		deps.log,
	);

	return {
		resolved,
		choice,
		reason: leader.reason,
		coalesced: true,
	};
}

/**
 * Enqueue *request*, notify the user, and block until resolution or the
 * approval timeout — firing pre/post hooks and cleaning up the queue entry
 * (`_await_gateway_decision`). Persistence of an approved choice and building
 * the final tool-facing BLOCKED result remain the CALLER's responsibility.
 */
export async function awaitGatewayDecision(
	deps: AwaitDecisionDeps,
	sessionKey: string,
	request: AwaitDecisionRequest,
	options: AwaitDecisionOptions = {},
): Promise<DecisionResult> {
	const surface = options.surface ?? "gateway";

	// ── Cron sessions are NEVER gateway-approval contexts ────────────────
	// Parity tools/approval.py:_is_cron_approval_context + _get_cron_approval_mode
	// (enforced first inside _is_gateway_approval_context): a cron job has no
	// human listening on any chat surface. Falling through to the normal path
	// would enqueue a pending approval with NO listener and block the job for
	// the full approvals.timeout. Resolve IMMEDIATELY from cron_mode (default
	// deny) WITHOUT enqueueing anything.
	if (deps.isCronSession?.() === true) {
		const data = new ApprovalEntry(request).data;
		await fireApprovalHook(
			deps.hooks,
			"pre_approval_request",
			hookBase(data, sessionKey, surface),
			deps.log,
		);
		const mode = resolveCronMode(deps);
		const choice: ApprovalChoice = mode === "approve" ? "once" : "deny";
		if (choice === "deny") {
			deps.log?.info?.(
				`Cron approval denied (approvals.cron_mode: deny) for session ${sessionKey}: ${data.command}`,
			);
		}
		await fireApprovalHook(
			deps.hooks,
			"post_approval_response",
			{ ...hookBase(data, sessionKey, surface), choice },
			deps.log,
		);
		return {
			resolved: true,
			choice,
			...(choice === "deny" ? { reason: cronDenyReason(data) } : {}),
		};
	}

	// ── Coalesce identical concurrent approvals (one prompt, one answer) ──
	// Parallel tool calls can hit the same dangerous-command gate at the same
	// time; without coalescing every caller enqueues its own entry and fires
	// its own notify — N identical prompts, /approve needed N times. Identity
	// is (command, pattern_keys) EXACTLY (§8.4).
	const candidateData = new ApprovalEntry(request).data;
	const leader = deps.queues.findIdenticalPending(
		sessionKey,
		candidateData.command,
		candidateData.patternKeys,
	);
	if (leader && leader.data.requestId !== candidateData.requestId) {
		const adopted = await awaitCoalescedLeader(
			deps,
			sessionKey,
			leader,
			candidateData,
			options,
		);
		if (adopted !== null) return adopted;
		// Leader resolved "once" — fall through to a fresh prompt below.
	}

	const entry = new ApprovalEntry(request);
	deps.queues.enqueue(sessionKey, entry);

	await fireApprovalHook(
		deps.hooks,
		"pre_approval_request",
		hookBase(entry.data, sessionKey, surface),
		deps.log,
	);

	// Notify the user (bridges the agent turn → the chat platform). A raise OR
	// an unregistered session lands here: drop the entry, fire the post hook
	// with choice="notify_failed", fail closed — never hang (§8.6).
	try {
		await deps.notify(sessionKey, { ...entry.data });
	} catch (err) {
		deps.log?.warn?.(`Gateway approval notify failed: ${String(err)}`);
		deps.queues.dropEntry(sessionKey, entry);
		await fireApprovalHook(
			deps.hooks,
			"post_approval_response",
			{ ...hookBase(entry.data, sessionKey, surface), choice: "notify_failed" },
			deps.log,
		);
		return { resolved: false, choice: null, notifyFailed: true };
	}

	// Block until the user responds or the canonical approval timeout elapses.
	// Poll in short slices firing activity heartbeats every ~10 s; respect
	// interrupts FIRST (#8697). The whole window counts as human-wait time so
	// concurrent batch deadlines exclude it (#79719).
	const deadline = deps.clock.nowSeconds() + deps.timeoutSeconds;
	const state: ActivityState = {
		lastTouchSeconds: deps.clock.nowSeconds(),
		startSeconds: deps.clock.nowSeconds(),
	};
	let resolved = false;

	const closeWait = deps.humanWait.begin(sessionKey);
	try {
		for (;;) {
			if (deps.isInterrupted?.() === true) {
				deps.log?.info?.(
					`Approval wait interrupted by user signal — returning deny for session ${sessionKey}`,
				);
				entry.result = "deny";
				entry.settle();
				resolved = true;
				break;
			}
			const sliceMs = remainingSliceMs(deps.clock, deadline);
			if (sliceMs <= 0) break;
			const winner = await waitSlice(entry.wait, deps.clock, sliceMs);
			if (winner === "entry") {
				resolved = true;
				break;
			}
			deps.heartbeat?.maybeDue(state, "waiting for user approval");
		}
	} finally {
		closeWait();
	}

	deps.queues.dropEntry(sessionKey, entry);

	const choice = entry.result;
	// Normalize the outcome for the post hook: unresolved (timeout) and null
	// both mean the user never responded — report that explicitly so plugins
	// can distinguish timeout from explicit deny.
	const outcome: string = !resolved || choice === null ? "timeout" : choice;
	await fireApprovalHook(
		deps.hooks,
		"post_approval_response",
		{ ...hookBase(entry.data, sessionKey, surface), choice: outcome },
		deps.log,
	);

	return { resolved, choice, reason: entry.reason };
}
