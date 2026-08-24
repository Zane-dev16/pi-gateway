// render.ts — pure rendering for the exec-approval bridge: the byte-stable
// text-fallback template, the DEC-016 callback wire grammar, the 2×2 card
// keyboard, and outcome labels (07 §8.3, §8.5; DEC-016).
//
// Hermes anchors (READ-ONLY reference):
//   gateway/run.py:_format_exec_approval_fallback   → formatExecApprovalFallback
//   plugins/platforms/telegram/adapter.py:send_exec_approval button block
//                                                   → buildApprovalKeyboard
//   plugins/platforms/telegram/adapter.py:_handle_callback_query `ea:` arm
//                                                   → parseApprovalCallbackData
//                                                   + outcome label map
//   agent/i18n.py catalog keys gateway.approve.* / gateway.deny.* /
//   gateway.approval_expired        → slash confirmation strings below

import { isApprovalChoice, type ApprovalChoice } from "./queue.js";

export const FALLBACK_HEADING = "⚠️ **Dangerous command requires approval:**";
export const SMART_DENIED_HEADING =
	"⚠️ **Smart DENY — owner override for one operation:**";

/** Command preview cap in CODE POINTS (Python len() parity). */
const COMMAND_PREVIEW_CHARS = 200;

function commandPreview(command: string): string {
	const chars = Array.from(command);
	return chars.length > COMMAND_PREVIEW_CHARS
		? `${chars.slice(0, COMMAND_PREVIEW_CHARS).join("")}...`
		: command;
}

/**
 * The text fallback rendered from approval CAPABILITIES, not platform names.
 * BYTE-STABLE port of `_format_exec_approval_fallback`: choices are gated on
 * allow_session / allow_permanent / smart_denied and joined as
 * `", ".join(choices[:-1]) + f", or {choices[-1]}."`.
 */
export function formatExecApprovalFallback(args: {
	command: string;
	description: string;
	commandPrefix: string;
	allowPermanent?: boolean;
	allowSession?: boolean;
	smartDenied?: boolean;
}): string {
	const {
		command,
		description,
		commandPrefix,
		allowPermanent = true,
		allowSession = true,
		smartDenied = false,
	} = args;
	const preview = commandPreview(command);
	const heading = smartDenied ? SMART_DENIED_HEADING : FALLBACK_HEADING;

	const choices: string[] = [
		`Reply \`${commandPrefix}approve\` to execute this one operation`,
	];
	if (!smartDenied && allowSession) {
		choices.push(
			`\`${commandPrefix}approve session\` to approve this pattern for the session`,
		);
		if (allowPermanent) {
			choices.push(`\`${commandPrefix}approve always\` to approve permanently`);
		}
	}
	choices.push(`\`${commandPrefix}deny\` to cancel`);

	const head = choices.slice(0, -1).join(", ");
	const tail = choices[choices.length - 1] as string;
	return `${heading}\n\`\`\`\n${preview}\n\`\`\`\nReason: ${description}\n\n${head}, or ${tail}.`;
}

// ── DEC-016 callback wire grammar: ea:<choice>:<approval_id> ─────────────────

/** Monotonic per-ledger id → short `ea:<choice>:<id>` payload (64-byte cap safe). */
export function execApprovalCallbackData(
	choice: ApprovalChoice,
	approvalId: number,
): string {
	return `ea:${choice}:${approvalId}`;
}

export interface ParsedApprovalCallback {
	choice: ApprovalChoice;
	approvalId: number;
}

/**
 * Parse `ea:<choice>:<id>`; anything else (other prefixes, bad choice,
 * non-numeric id) parses to null → "Invalid approval data." answer.
 */
export function parseApprovalCallbackData(
	data: string,
): ParsedApprovalCallback | null {
	if (!data.startsWith("ea:")) return null;
	const rest = data.slice("ea:".length);
	const sep = rest.indexOf(":");
	if (sep <= 0) return null;
	const choice = rest.slice(0, sep);
	const rawId = rest.slice(sep + 1);
	if (!isApprovalChoice(choice)) return null;
	const approvalId = Number.parseInt(rawId, 10);
	if (!Number.isInteger(approvalId) || !/^\d+$/.test(rawId)) return null;
	return { choice, approvalId };
}

// ── card keyboard (Telegram parity set, paired into 2-per-row) ────────────────

export interface CardButton {
	label: string;
	callbackData: string;
}

/**
 * Button set gated on allow_session / allow_permanent / smart_denied, paired
 * into rows of two — a single 4-button row truncates on mobile.
 */
export function buildApprovalKeyboard(args: {
	approvalId: number;
	allowPermanent?: boolean;
	allowSession?: boolean;
	smartDenied?: boolean;
}): CardButton[][] {
	const {
		approvalId,
		allowPermanent = true,
		allowSession = true,
		smartDenied = false,
	} = args;
	const buttons: CardButton[] = [
		{
			label: "✅ Allow Once",
			callbackData: execApprovalCallbackData("once", approvalId),
		},
	];
	if (!smartDenied && allowSession) {
		buttons.push({
			label: "✅ Session",
			callbackData: execApprovalCallbackData("session", approvalId),
		});
		if (allowPermanent) {
			buttons.push({
				label: "✅ Always",
				callbackData: execApprovalCallbackData("always", approvalId),
			});
		}
	}
	buttons.push({
		label: "❌ Deny",
		callbackData: execApprovalCallbackData("deny", approvalId),
	});
	const rows: CardButton[][] = [];
	for (let i = 0; i < buttons.length; i += 2) {
		rows.push(buttons.slice(i, i + 2));
	}
	return rows;
}

// ── outcome labels + honest stale-tap texts (#63501) ─────────────────────────

export const OUTCOME_LABELS: Readonly<Record<ApprovalChoice, string>> =
	Object.freeze({
		once: "✅ Approved once",
		session: "✅ Approved for session",
		always: "✅ Approved permanently",
		deny: "❌ Denied",
	});

export const RESOLVED_FALLBACK_LABEL = "Resolved";
export const EXPIRED_LABEL = "⌛ Approval expired";
export const EXPIRED_EDIT_TEXT =
	`${EXPIRED_LABEL} — no command was waiting. ` +
	`It already timed out (and was denied) or was resolved elsewhere.`;
export const ALREADY_RESOLVED_TEXT = "This approval has already been resolved.";
export const NOT_AUTHORIZED_TEXT =
	"⛔ You are not authorized to approve commands.";
export const INVALID_CALLBACK_TEXT = "Invalid approval data.";

/** Outcome label derives from the ACTUAL resolved count (resolve-first-render-second). */
export function clickOutcomeLabel(
	choice: ApprovalChoice,
	count: number,
): string {
	if (count <= 0) return EXPIRED_LABEL;
	return OUTCOME_LABELS[choice] ?? RESOLVED_FALLBACK_LABEL;
}

// ── slash-handler confirmation strings (en catalog parity) ────────────────────

export const APPROVE_NO_PENDING = "No pending command to approve.";
export const DENY_NO_PENDING = "No pending command to deny.";
export const APPROVAL_EXPIRED =
	"⚠️ Approval expired (agent is no longer waiting). Ask the agent to try again.";
export const DENY_STALE = "❌ Command denied (approval was stale).";

const APPROVE_SINGULAR: Readonly<
	Record<Exclude<ApprovalChoice, "deny">, string>
> = Object.freeze({
	once: "✅ Command approved. The agent is resuming...",
	session:
		"✅ Command approved (pattern approved for this session). The agent is resuming...",
	always:
		"✅ Command approved (pattern approved permanently). The agent is resuming...",
});

const APPROVE_PLURAL: Readonly<
	Record<Exclude<ApprovalChoice, "deny">, string>
> = Object.freeze({
	once: "✅ Commands approved ({count} commands). The agent is resuming...",
	session:
		"✅ Commands approved (pattern approved for this session) ({count} commands). The agent is resuming...",
	always:
		"✅ Commands approved (pattern approved permanently) ({count} commands). The agent is resuming...",
});

export function approveConfirmation(
	choice: Exclude<ApprovalChoice, "deny">,
	count: number,
): string {
	if (count > 1) {
		return (APPROVE_PLURAL[choice] ?? APPROVE_SINGULAR.once).replace(
			"{count}",
			String(count),
		);
	}
	return APPROVE_SINGULAR[choice];
}

export function denyConfirmation(count: number, reason?: string): string {
	const trimmedReason = reason?.trim();
	if (trimmedReason) {
		return count > 1
			? `❌ Commands denied (${count} commands). Reason relayed to the agent: "${trimmedReason}"`
			: `❌ Command denied. Reason relayed to the agent: "${trimmedReason}"`;
	}
	return count > 1
		? `❌ Commands denied (${count} commands).`
		: "❌ Command denied.";
}

/** `/deny <reason>` free-text cap (§8.1: capped 280 chars, relayed verbatim). */
export const DENY_REASON_MAX_CHARS = 280;
