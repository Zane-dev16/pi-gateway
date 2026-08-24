// delivery.ts — the notify bridge: CARD-FIRST / TEXT-FALLBACK delivery of an
// approval prompt, with the send-outcome classifier whose boundary rule is
// BINDING (07 §8.3): an ambiguous (timed-out) send is NEVER re-sent and NEVER
// falls back — only a DEFINITIVE failure re-asks.
//
// Hermes anchors (READ-ONLY reference):
//   gateway/run.py:_approval_notify_sync            → DeliveryBridge.notify
//   gateway/run.py:_approval_send_outcome           → classifySendOutcome
//   gateway/run.py:safe_schedule_threadsend seam    → DeliveryBridgeDeps.schedule
//     (safe_schedule_threadsafe; null future ⇒ "loop unavailable" ⇒ failed)
//   gateway/run.py:class-level getattr(send_exec_approval)
//                                                   → hasExecApprovalCard
//   telegram/adapter.py:_approval_counter/_approval_state
//                                                   → ApprovalCardLedger
//
// The class-level probe is deliberate: checking the INSTANCE would
// false-positive on test doubles that auto-create attributes (MagicMock
// parity note in run.py). Instance-own properties are ignored; only a method
// declared on the prototype chain enables the card path.

import type { GatewayClock } from "./clock.js";
import { redactApprovalCommand } from "./redact.js";
import { formatExecApprovalFallback } from "./render.js";

/** 04 §1 verbatim send shape (structural import by value here is avoided). */
export interface ApprovalSendResult {
	success: boolean;
	messageId?: string | null;
	error?: string | null;
}

export type SendOutcome = "sent" | "ambiguous" | "failed";

/** Scheduling-future classification window (run.py `_approval_send_outcome(fut, timeout=15)`). */
export const SEND_CLASSIFY_TIMEOUT_MS = 15_000;

export interface SendClassifierLog {
	warn?(message: string): void;
}

/**
 * Classify an approval prompt send as `sent` / `failed` / `ambiguous`.
 *
 * `ambiguous` == the scheduling future did NOT settle within `timeoutMs`.
 * The card may well have posted (slow platform API call, transient
 * backpressure, event-loop stall); treating that timeout as failure has been
 * observed to re-send cards repeatedly and orphan "/approve: nothing pending"
 * replies. Callers must keep the registration armed and NOT re-send/fall back.
 * Only a DEFINITIVE failure (error result / rejection / no future) re-asks.
 */
export async function classifySendOutcome(
	future: Promise<ApprovalSendResult> | null,
	clock: GatewayClock,
	timeoutMs: number,
	log?: SendClassifierLog,
): Promise<SendOutcome> {
	if (future === null) {
		log?.warn?.("Prompt send failed: no scheduling future (loop unavailable)");
		return "failed";
	}
	// Shadow settlement so a late rejection after `ambiguous` wins the race
	// cannot become an unhandled rejection.
	void future.catch(() => {});
	const settled = future.then(
		(result) =>
			result && result.success === true ? "sent" : ("failed" as SendOutcome),
		(reason: unknown) => {
			log?.warn?.(`Prompt send failed: ${String(reason)}`);
			return "failed" as SendOutcome;
		},
	);
	const deadline = clock
		.sleepMs(timeoutMs)
		.then(() => "ambiguous" as SendOutcome);
	return Promise.race([settled, deadline]);
}

/**
 * CLASS-level capability probe (`getattr(type(adapter), "send_exec_approval",
 * None) is not None` parity). Walks the prototype chain for a DECLARED
 * `sendExecApproval`; instance-assigned functions do not count.
 */
export function hasExecApprovalCard(target: object): boolean {
	let proto = Object.getPrototypeOf(target);
	while (proto !== null && proto !== Object.prototype) {
		if (Object.hasOwn(proto, "sendExecApproval")) {
			const method = (proto as Record<string, unknown>).sendExecApproval;
			return typeof method === "function";
		}
		proto = Object.getPrototypeOf(proto);
	}
	return false;
}

/**
 * Monotonic approval-id ledger (adapter `_approval_counter` + `_approval_state`
 * parity) owned by the bridge so ONE namespaced `ea:` grammar serves every
 * adapter (DEC-016): bind(id → sessionKey) at card-send time, pop() at click time.
 */
export class ApprovalCardLedger {
	private counter = 0;
	private readonly state = new Map<number, string>();

	nextId(): number {
		this.counter += 1;
		return this.counter;
	}

	bind(approvalId: number, sessionKey: string): void {
		this.state.set(approvalId, sessionKey);
	}

	/** Missing id ⇒ already resolved / stale tap. */
	pop(approvalId: number): string | null {
		const sessionKey = this.state.get(approvalId);
		if (sessionKey === undefined) return null;
		this.state.delete(approvalId);
		return sessionKey;
	}

	/** Non-destructive read — authorization checks must NOT touch state. */
	peek(approvalId: number): string | null {
		return this.state.get(approvalId) ?? null;
	}

	get size(): number {
		return this.state.size;
	}
}

export interface ExecApprovalSendArgs {
	chatId: string;
	command: string;
	description: string;
	sessionKey: string;
	metadata?: unknown;
	allowPermanent: boolean;
	allowSession: boolean;
	smartDenied: boolean;
	/** Bridge-assigned ledger id (see ApprovalCardLedger). */
	approvalId: number;
}

/**
 * Structural slice a delivery target must satisfy. Adapters implement
 * `sendExecApproval` on the CLASS to enable the card path; `typedCommandPrefix`
 * selects the fallback verb form ("/" vs "!").
 */
export interface DeliveryTarget {
	typedCommandPrefix?: string | undefined;
	send?(
		chatId: string,
		text: string,
		metadata?: unknown,
	): Promise<ApprovalSendResult>;
	sendExecApproval?(args: ExecApprovalSendArgs): Promise<ApprovalSendResult>;
}

export interface NotifyRequest {
	sessionKey: string;
	command: string;
	description: string;
	allowPermanent: boolean;
	allowSession: boolean;
	smartDenied: boolean;
}

export interface DeliveryBridgeDeps {
	target: DeliveryTarget;
	chatId: string;
	ledger: ApprovalCardLedger;
	clock: GatewayClock;
	metadata?: unknown;
	/** Shared redaction point injection (defaults to the built-in pattern pass). */
	redact?: (command: string) => string;
	/**
	 * Schedules a send onto the turn loop; null ⇒ loop unavailable ⇒ definitive
	 * failure. Defaults to invoking the op directly.
	 */
	schedule?<T>(op: () => Promise<T>): Promise<T> | null;
	pauseTyping?(): void;
	log?: {
		info?(message: string): void;
		warn?(message: string): void;
		error?(message: string): void;
	};
}

/**
 * THE notify bridge (`_approval_notify_sync` port). Never throws — a failed
 * prompt send must not crash the agent thread; the gate's bounded wait plus
 * text-command resolution remain armed (§8.6).
 *
 *   pause typing → redact ONCE (shared point feeds both paths) →
 *   card path when the CLASS defines sendExecApproval, classified:
 *     sent      → done
 *     ambiguous → NO re-send, NO fallback; registration stays armed
 *     failed    → fall through to text fallback
 *   text fallback via the adapter's typed_command_prefix; if THAT also fails
 *   the wait continues (text commands can still resolve it).
 */
export class DeliveryBridge {
	constructor(private readonly deps: DeliveryBridgeDeps) {}

	async notify(request: NotifyRequest): Promise<void> {
		const { deps } = this;
		deps.pauseTyping?.();

		// Redact BEFORE any rendering — one shared point for BOTH paths (#48456).
		const redact = deps.redact ?? ((cmd: string) => redactApprovalCommand(cmd));
		const command = redact(request.command);

		if (hasExecApprovalCard(deps.target)) {
			try {
				const args: ExecApprovalSendArgs = {
					chatId: deps.chatId,
					command,
					description: request.description,
					sessionKey: request.sessionKey,
					allowPermanent: request.allowPermanent,
					allowSession: request.allowSession,
					smartDenied: request.smartDenied,
					approvalId: deps.ledger.nextId(),
				};
				if (deps.metadata !== undefined) {
					args.metadata = deps.metadata;
				}
				const schedule =
					deps.schedule ?? ((op: () => Promise<ApprovalSendResult>) => op());
				const future = schedule(() =>
					(
						deps.target.sendExecApproval as (
							a: ExecApprovalSendArgs,
						) => Promise<ApprovalSendResult>
					)(args),
				);
				if (future === null) {
					throw new Error("send_exec_approval: loop unavailable");
				}
				const outcome = await classifySendOutcome(
					future,
					deps.clock,
					SEND_CLASSIFY_TIMEOUT_MS,
					deps.log,
				);
				if (outcome === "sent") {
					// Bind id → session AFTER delivery succeeds (adapter
					// `_approval_state[id] = session_key` parity).
					deps.ledger.bind(args.approvalId, request.sessionKey);
					return;
				}
				if (outcome === "ambiguous") {
					// Timeout ≠ failure: the card may have posted with a late ack.
					// Re-sending produced duplicate cards + orphaned replies in live
					// relay testing; skip the text fallback and stay armed — the id
					// binds anyway so a late tap on the rendered card still resolves.
					deps.ledger.bind(args.approvalId, request.sessionKey);
					deps.log?.warn?.(
						"Button-based approval send timed out — treating as possibly-delivered " +
							"(no re-send; the prompt stays armed for a late tap)",
					);
					return;
				}
				deps.log?.warn?.(
					"Button-based approval failed (send returned error), falling back to text",
				);
			} catch (err) {
				deps.log?.warn?.(
					`Button-based approval failed, falling back to text: ${String(err)}`,
				);
			}
		}

		// Text fallback: use the ADAPTER's typed prefix so Slack/Matrix users are
		// told the form they can actually type ("!" vs "/").
		const prefix = deps.target.typedCommandPrefix ?? "/";
		const message = formatExecApprovalFallback({
			command,
			description: request.description,
			commandPrefix: prefix,
			allowPermanent: request.allowPermanent,
			allowSession: request.allowSession,
			smartDenied: request.smartDenied,
		});
		if (!deps.target.send) {
			deps.log?.error?.("Failed to send approval request: target has no send");
			return;
		}
		try {
			// bind(): the callback must not lose the receiver when scheduled onto
			// the turn loop (member-call form elsewhere keeps `this` intact).
			const send = deps.target.send.bind(deps.target);
			const schedule =
				deps.schedule ?? ((op: () => Promise<ApprovalSendResult>) => op());
			const future = schedule(() => send(deps.chatId, message, deps.metadata));
			if (future !== null) {
				await classifySendOutcome(
					future,
					deps.clock,
					SEND_CLASSIFY_TIMEOUT_MS,
					deps.log,
				);
			}
		} catch (err) {
			// Definitive fallback failure: log and CONTINUE — the wait stays armed
			// because typed `/approve …` answers can still resolve it (§8.6).
			deps.log?.error?.(`Failed to send approval request: ${String(err)}`);
		}
	}
}
