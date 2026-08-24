// surfaces.ts — every answer surface above THE one resolution primitive
// (07 §8.4 resolution primitive, §8.5 answer-surface table):
//
//   • inline-button clicks  → handleApprovalClick (authorize FIRST, pop state,
//                             resolve FIRST / render SECOND — #63501)
//   • /approve [/all] [session|always] → handleApprove
//   • /deny [/all] [<reason>]          → handleDeny (reason ≤280 relayed verbatim)
//   • bare words while blocked         → classifyBareWord + routeBareAnswer,
//                             gated on hasBlockingApproval + control permission;
//                             synthesis ALWAYS uses a literal "/" prefix
//   • api request_id lane   → resolveByRequestId (request_id targeting)
//
// Stale answers are answered HONESTLY: no live queue + expired-approval marker
// ⇒ "approval expired" (distinct from "nothing pending"); a resolved-count of
// 0 renders the expired label — never fabricated success.
//
// Hermes anchors (READ-ONLY reference):
//   gateway/slash_commands.py:_handle_approve_command → handleApprove
//   gateway/slash_commands.py:_handle_deny_command    → handleDeny
//   gateway/run.py bare-word block (~line 10221)      → BARE_APPROVE_WORDS /
//                                                       BARE_DENY_WORDS / routeBareAnswer
//   plugins/platforms/telegram/adapter.py:_handle_callback_query `ea:` arm
//                                                     → handleApprovalClick

import type { ApprovalQueues, ApprovalChoice } from "./queue.js";
import { isApprovalChoice } from "./queue.js";
import type { ApprovalCardLedger } from "./delivery.js";
import {
	APPROVAL_EXPIRED,
	ALREADY_RESOLVED_TEXT,
	DENY_REASON_MAX_CHARS,
	DENY_STALE,
	INVALID_CALLBACK_TEXT,
	NOT_AUTHORIZED_TEXT,
	approveConfirmation,
	clickOutcomeLabel,
	denyConfirmation,
	parseApprovalCallbackData,
} from "./render.js";

export type AnswerRejectionCode =
	| "unauthorized_source"
	| "invalid_callback"
	| "already_resolved"
	| "no_pending"
	| "stale_expired";

export class AnswerRejected extends Error {
	constructor(
		readonly code: AnswerRejectionCode,
		message: string,
	) {
		super(message);
		this.name = "AnswerRejected";
	}
}

/**
 * Authorization policy for ANSWER sources (§8.3: authorize the clicker FIRST
 * — unauthorized ⇒ rejection with reason code, NO approval state touched).
 * The bridge binds the originating surface at registration; policy overrides
 * keep bot-level ACLs injectable.
 */
export type IsAnswerAuthorized = (
	source: AnswerSource,
	sessionKey: string | null,
) => boolean;

export interface AnswerSource {
	surface: string;
	chatId?: string;
	userId?: string;
}

/** Default fail-closed policy: only the bound source may answer. */
export function bindingMatches(
	bound: AnswerSource | undefined,
	source: AnswerSource,
): boolean {
	if (!bound) return false;
	return bound.surface === source.surface && bound.chatId === source.chatId;
}

export interface SurfaceDeps {
	queues: ApprovalQueues;
	ledger: ApprovalCardLedger;
	/**
	 * Expired-approval markers (`runner._pending_approvals` parity): sessions
	 * whose blocking approval timed out; consumed by a later /approve //deny.
	 */
	expiredApprovals: Set<string>;
	isAnswerAuthorized: IsAnswerAuthorized;
	resumeTyping?(sessionKey: string): void;
}

// ── slash parsing ──────────────────────────────────────────────────────────────

export interface ParsedApproveArgs {
	resolveAll: boolean;
	choice: Exclude<ApprovalChoice, "deny">;
}

/**
 * `/approve` args: support "all", "all session", "all always", "session",
 * "always". Args parse LOWERCASED (slash_commands parity).
 */
export function parseApproveArgs(rawArgs: string): ParsedApproveArgs {
	const tokens = rawArgs.trim().toLowerCase().split(/\s+/).filter(Boolean);
	const resolveAll = tokens.includes("all");
	const remaining = tokens.filter((token) => token !== "all");
	let choice: Exclude<ApprovalChoice, "deny"> = "once";
	if (
		remaining.some((a) => ["always", "permanent", "permanently"].includes(a))
	) {
		choice = "always";
	} else if (remaining.some((a) => ["session", "ses"].includes(a))) {
		choice = "session";
	}
	return { resolveAll, choice };
}

export interface ParsedDenyArgs {
	resolveAll: boolean;
	/** Verbatim (NOT lowercased), capped at 280 chars. */
	reason: string | null;
}

/**
 * `/deny` args: a leading "all" token denies every pending command; anything
 * after it (or the whole arg string when "all" is absent) is captured
 * verbatim as the optional deny reason, capped to a sane one-liner.
 */
export function parseDenyArgs(rawArgs: string): ParsedDenyArgs {
	const trimmed = rawArgs.trim();
	const tokens = trimmed.split(/\s+/).filter(Boolean);
	const resolveAll = tokens.length > 0 && tokens[0] === "all";
	let reason: string;
	if (resolveAll) {
		reason = trimmed.slice((tokens[0] as string).length).trim();
	} else {
		reason = trimmed;
	}
	if (Array.from(reason).length > DENY_REASON_MAX_CHARS) {
		reason = Array.from(reason).slice(0, DENY_REASON_MAX_CHARS).join("").trim();
	}
	return { resolveAll, reason: reason ? reason : null };
}

function staleCheck(
	deps: SurfaceDeps,
	sessionKey: string,
): "live" | "expired" | "none" {
	if (deps.queues.hasBlocking(sessionKey)) return "live";
	if (deps.expiredApprovals.has(sessionKey)) {
		deps.expiredApprovals.delete(sessionKey);
		return "expired";
	}
	return "none";
}

export interface SlashAnswerResult {
	count: number;
	reply: string;
}

/**
 * Targeting gate for surface answers that CARRY a source identity: an
 * unauthorized source is reason-code rejected BEFORE any queue or ledger
 * mutation. Runners-internal callers (already permission-checked upstream by
 * the L1 bypass) omit `source` and pass straight through.
 */
function assertSourceAuthorized(
	deps: SurfaceDeps,
	sessionKey: string,
	source?: AnswerSource,
): void {
	if (source !== undefined && !deps.isAnswerAuthorized(source, sessionKey)) {
		throw new AnswerRejected("unauthorized_source", NOT_AUTHORIZED_TEXT);
	}
}

/** `_handle_approve_command` port. Throws AnswerRejected(unauthorized_source|no_pending|stale_expired). */
export function handleApprove(
	deps: SurfaceDeps,
	args: {
		sessionKey: string;
		rawArgs: string;
		source?: AnswerSource;
	},
): SlashAnswerResult {
	assertSourceAuthorized(deps, args.sessionKey, args.source);
	const liveness = staleCheck(deps, args.sessionKey);
	if (liveness !== "live") {
		throw new AnswerRejected(
			liveness === "expired" ? "stale_expired" : "no_pending",
			liveness === "expired" ? APPROVAL_EXPIRED : APPROVE_NO_PENDING_MSG,
		);
	}
	const parsed = parseApproveArgs(args.rawArgs);
	const count = deps.queues.resolve(args.sessionKey, parsed.choice, {
		resolveAll: parsed.resolveAll,
	});
	if (!count) throw new AnswerRejected("no_pending", APPROVE_NO_PENDING_MSG);
	deps.resumeTyping?.(args.sessionKey);
	return { count, reply: approveConfirmation(parsed.choice, count) };
}

const APPROVE_NO_PENDING_MSG = "No pending command to approve.";

/** `_handle_deny_command` port. Throws AnswerRejected(unauthorized_source|no_pending|stale_expired). */
export function handleDeny(
	deps: SurfaceDeps,
	args: {
		sessionKey: string;
		rawArgs: string;
		source?: AnswerSource;
	},
): SlashAnswerResult {
	assertSourceAuthorized(deps, args.sessionKey, args.source);
	const liveness = staleCheck(deps, args.sessionKey);
	if (liveness !== "live") {
		throw new AnswerRejected(
			liveness === "expired" ? "stale_expired" : "no_pending",
			liveness === "expired" ? DENY_STALE : "No pending command to deny.",
		);
	}
	const parsed = parseDenyArgs(args.rawArgs);
	const count = deps.queues.resolve(args.sessionKey, "deny", {
		resolveAll: parsed.resolveAll,
		reason: parsed.reason,
	});
	if (!count)
		throw new AnswerRejected("no_pending", "No pending command to deny.");
	deps.resumeTyping?.(args.sessionKey);
	return { count, reply: denyConfirmation(count, parsed.reason ?? undefined) };
}

// ── bare words while blocked (run.py plain-text approval routing) ────────────

export const BARE_APPROVE_WORDS: ReadonlySet<string> = new Set([
	"approve",
	"yes",
	"ok",
	"okay",
	"confirm",
	"y",
	"👍",
]);

export const BARE_DENY_WORDS: ReadonlySet<string> = new Set([
	"deny",
	"no",
	"reject",
	"cancel",
	"n",
	"👎",
]);

const BARE_ALWAYS_FORMS: ReadonlySet<string> = new Set([
	"always",
	"approve always",
	"always approve",
]);

const BARE_SESSION_FORMS: ReadonlySet<string> = new Set([
	"session",
	"approve session",
	"session approve",
]);

export interface BareWordRoute {
	verb: "approve" | "deny";
	/** Canonical modifier args ("always"/"session") or "". */
	args: string;
	/** The synthesized canonical command text (ALWAYS a literal "/"). */
	synthesizedCommand: string;
}

/**
 * Classify stripped/lowercased bare text into an approve/deny route.
 * Returns null for conversational text — the disambiguator that keeps a
 * conversational "yes" from triggering anything is hasBlockingApproval,
 * enforced by routeBareAnswer, not by this table.
 */
export function classifyBareWord(text: string): BareWordRoute | null {
	const normalized = text.trim().toLowerCase();
	if (BARE_APPROVE_WORDS.has(normalized)) {
		return route("approve", "");
	}
	if (BARE_DENY_WORDS.has(normalized)) {
		return route("deny", "");
	}
	if (BARE_ALWAYS_FORMS.has(normalized)) return route("approve", "always");
	if (BARE_SESSION_FORMS.has(normalized)) return route("approve", "session");
	return null;
}

function route(verb: "approve" | "deny", args: string): BareWordRoute {
	let command = `/${verb}`;
	if (args) command = `${command} ${args}`;
	return { verb, args, synthesizedCommand: command };
}

export interface BareRouteResult {
	routed: boolean;
	/** Why not routed (only when routed=false). */
	skipReason?: "not_a_bare_word" | "control_denied" | "nothing_blocking";
	reply?: string;
}

/**
 * Route a bare-word answer through the SAME handlers as the slash forms.
 * Gated on control permission AND a live blocking approval so a
 * conversational "yes" stays conversational when nothing is pending.
 */
export function routeBareAnswer(
	deps: SurfaceDeps,
	input: {
		sessionKey: string;
		text: string;
		controlAllowed: boolean;
		source?: AnswerSource;
	},
): BareRouteResult {
	const classified = classifyBareWord(input.text);
	if (!classified) return { routed: false, skipReason: "not_a_bare_word" };
	if (!input.controlAllowed || !deps.queues.hasBlocking(input.sessionKey)) {
		return {
			routed: false,
			skipReason: input.controlAllowed ? "nothing_blocking" : "control_denied",
		};
	}
	const result =
		classified.verb === "approve"
			? handleApprove(deps, {
					sessionKey: input.sessionKey,
					rawArgs: classified.args,
				})
			: handleDeny(deps, {
					sessionKey: input.sessionKey,
					rawArgs: classified.args,
				});
	return { routed: true, reply: result.reply };
}

// ── inline-button click handling (adapter callback parity) ───────────────────

export type ClickOutcome =
	| "resolved"
	| "expired"
	| "unauthorized"
	| "already_resolved"
	| "invalid";

export interface ClickResult {
	outcome: ClickOutcome;
	reasonCode?: AnswerRejectionCode;
	/** Query-answer text (toast). */
	answerText: string;
	/** Message edit label when something actually happened (undefined otherwise). */
	editText?: string;
	/** Strip the inline keyboard (after ANY definitive outcome render). */
	stripKeyboard: boolean;
	resolvedCount: number;
	sessionKey?: string;
}

/**
 * The `ea:` click arm of `_handle_callback_query`, in binding order:
 *
 *   1. parse the wire grammar            → invalid ⇒ "Invalid approval data."
 *   2. authorize the clicker FIRST       → unauthorized ⇒ "⛔ …", NO state touched
 *   3. pop the ledger binding            → missing ⇒ "already been resolved."
 *   4. resolve FIRST via THE primitive
 *   5. render SECOND from the ACTUAL count — count=0 ⇒ honest "expired"
 *      (#63501: a stale tap must NEVER claim "Approved" for a command that
 *      will not run); resume typing only when something actually resolved.
 */
export async function handleApprovalClick(
	deps: SurfaceDeps,
	input: {
		callbackData: string;
		source: AnswerSource;
		userDisplay?: string;
	},
): Promise<ClickResult> {
	const parsed = parseApprovalCallbackData(input.callbackData);
	if (!parsed) {
		return {
			outcome: "invalid",
			reasonCode: "invalid_callback",
			answerText: INVALID_CALLBACK_TEXT,
			stripKeyboard: false,
			resolvedCount: 0,
		};
	}

	const sessionKey = deps.ledger.peek(parsed.approvalId);
	if (!deps.isAnswerAuthorized(input.source, sessionKey)) {
		return {
			outcome: "unauthorized",
			reasonCode: "unauthorized_source",
			answerText: NOT_AUTHORIZED_TEXT,
			stripKeyboard: false,
			resolvedCount: 0,
		};
	}

	const popped = deps.ledger.pop(parsed.approvalId);
	if (popped === null) {
		return {
			outcome: "already_resolved",
			reasonCode: "already_resolved",
			answerText: ALREADY_RESOLVED_TEXT,
			stripKeyboard: true,
			resolvedCount: 0,
		};
	}

	// Resolve FIRST — unblocks the agent thread. Rendering happens AFTER so the
	// message reflects what actually occurred.
	const count = deps.queues.resolve(popped, parsed.choice);

	const label = clickOutcomeLabel(parsed.choice, count);
	const who = input.userDisplay ? ` by ${input.userDisplay}` : "";
	return {
		outcome: count > 0 ? "resolved" : "expired",
		answerText: count > 0 ? `${label}${who}` : label,
		editText:
			count > 0
				? `${label}${who}`
				: "⌛ Approval expired — no command was waiting. It already timed out (and was denied) or was resolved elsewhere.",
		stripKeyboard: true,
		resolvedCount: count,
		sessionKey: popped,
	};
}

// ── api request_id lane (/v1/runs approvals) ────────────────────────────────

/**
 * Targeted resolution: ONLY entries carrying this requestId resolve. An
 * unauthorized source is rejected BEFORE any queue mutation; an unknown
 * request id resolves nothing and reports no_pending (authoritative 0).
 */
export function resolveByRequestId(
	deps: SurfaceDeps,
	input: {
		sessionKey: string;
		requestId: string;
		choice: string;
		source?: AnswerSource;
	},
): SlashAnswerResult {
	if (
		input.source !== undefined &&
		!deps.isAnswerAuthorized(input.source, input.sessionKey)
	) {
		throw new AnswerRejected("unauthorized_source", NOT_AUTHORIZED_TEXT);
	}
	if (!isApprovalChoice(input.choice)) {
		throw new AnswerRejected("no_pending", `invalid choice '${input.choice}'`);
	}
	const count = deps.queues.resolve(input.sessionKey, input.choice, {
		requestId: input.requestId,
	});
	if (!count) {
		throw new AnswerRejected(
			"no_pending",
			`no pending approval with request_id ${input.requestId}`,
		);
	}
	deps.resumeTyping?.(input.sessionKey);
	return {
		count,
		reply:
			input.choice === "deny"
				? denyConfirmation(count)
				: approveConfirmation(
						input.choice as Exclude<ApprovalChoice, "deny">,
						count,
					),
	};
}
