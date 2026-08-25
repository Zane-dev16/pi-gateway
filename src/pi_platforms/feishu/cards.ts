// pi_platforms/feishu/cards — the A12 CARD ACTION machinery
// (adapter.py:send_exec_approval :2057, :_build_update_prompt_card :2131,
// :_on_card_action_trigger :2715, :_handle_card_action_event :3062).
//
// Feishu's native interactive mechanism is the interactive-card action value:
// a JSON dict carried on the button. Three families exist in Hermes ground
// truth, and ALL resolve through ONE entry (_on_card_action_trigger):
//   {"hermes_action": <choice>, "approval_id": <int>}      → exec approvals
//   {"hermes_update_prompt_action": "y"|"n", "update_prompt_id": <int>}
//                                                          → update prompts
//   anything else                                          → generic click,
//     synthetic COMMAND text `/card {tag} {json}` through the full pipeline.
//
// DEC-016 parallel-mechanism parity: the kit callback grammar (ea:/sc:/cl:)
// stays THE resolution seam — feishu-native values map onto the SAME pending
// stores + resolvers, never a divergent second state machine.

import type { ExecApprovalChoice } from "../kit/index.js";
import {
	FEISHU_APPROVAL_CHOICE_MAP,
	FEISHU_RESOLVED_LABELS,
} from "./manifest.js";

/** Card action value as delivered inside p2_card_action_trigger. */
export type CardActionValue = Record<string, unknown>;

export interface ParsedCardAction {
	family: "approval" | "update_prompt" | "generic";
	choice?: ExecApprovalChoice | undefined;
	approvalId?: number | undefined;
	answer?: "y" | "n" | undefined;
	updatePromptId?: number | undefined;
	tag?: string | undefined;
	value?: CardActionValue | undefined;
}

/**
 * adapter.py:_APPROVAL_CHOICE_MAP (:245) with fail-closed default (:2791):
 * an UNKNOWN choice value resolves to `deny` — a malformed approval tap can
 * never approve anything.
 */
export function parseApprovalChoice(raw: unknown): ExecApprovalChoice {
	const mapped = FEISHU_APPROVAL_CHOICE_MAP[String(raw ?? "")];
	return (mapped ?? "deny") as ExecApprovalChoice;
}

/**
 * The ONE dispatch table `_on_card_action_trigger` walks (sync entry :2715).
 * Generic clicks carry their action tag (`action.tag`, default "button").
 */
export function parseCardAction(
	actionValue: CardActionValue | undefined,
	actionTag: string | undefined,
): ParsedCardAction {
	const value = actionValue ?? {};
	if ("hermes_action" in value) {
		return {
			family: "approval",
			choice: parseApprovalChoice(value["hermes_action"]),
			approvalId: Number(value["approval_id"] ?? 0),
		};
	}
	if ("hermes_update_prompt_action" in value) {
		const raw = String(value["hermes_update_prompt_action"] ?? "")
			.toLowerCase()
			.trim();
		return {
			family: "update_prompt",
			answer: raw === "y" || raw === "n" ? raw : undefined,
			updatePromptId: Number(value["update_prompt_id"] ?? 0),
		};
	}
	return {
		family: "generic",
		tag: actionTag && actionTag.length > 0 ? actionTag : "button",
		value,
	};
}

// ── builders (adapter.py:_btn :2057 / _build_approval_card) ─────────────────

export interface CardButton {
	tag: "button";
	text: { tag: "plain_text"; content: string };
	type: "primary" | "danger" | "default";
	value: CardActionValue;
}

function btn(
	label: string,
	type: "primary" | "danger" | "default",
	value: CardActionValue,
): CardButton {
	return {
		tag: "button",
		text: { tag: "plain_text", content: label },
		type,
		value,
	};
}

/**
 * Card JSON shape (adapter.py send_exec_approval :2057 / _build_update_prompt_card
 * :2131 / _build_resolved_approval_card :2199): header titles are PLAIN_TEXT,
 * prompt templates are ORANGE, body content rides native `markdown`
 * elements — never div/lark_md re-encodings.
 */
export interface ApprovalCard {
	config: { wide_screen_mode: boolean };
	header: {
		title: { tag: "plain_text"; content: string };
		template: string;
	};
	elements: Array<
		| { tag: "markdown"; content: string }
		| { tag: "action"; actions: CardButton[] }
	>;
}

/**
 * Exec-approval prompt card. Button SET depends on allow_session/allow_permanent
 * exactly like send_exec_approval (:2057): session omitted when denied or
 * disallowed; always only when allow_permanent also holds.
 */
export function buildExecApprovalCard(opts: {
	title: string;
	detail: string;
	approvalId: number;
	allowSession: boolean;
	allowPermanent: boolean;
	smartDenied?: boolean | undefined;
}): ApprovalCard {
	const buttons: CardButton[] = [
		btn("✅ Allow Once", "primary", {
			// Button types (:2057 _btn default): Allow Once is EXPLICITLY primary;
			// Session/Always ride the DEFAULT type; Deny is danger.
			hermes_action: "approve_once",
			approval_id: opts.approvalId,
		}),
	];
	if (!opts.smartDenied && opts.allowSession) {
		buttons.push(
			btn("✅ Session", "default", {
				hermes_action: "approve_session",
				approval_id: opts.approvalId,
			}),
		);
	}
	if (!opts.smartDenied && opts.allowSession && opts.allowPermanent) {
		buttons.push(
			btn("✅ Always", "default", {
				hermes_action: "approve_always",
				approval_id: opts.approvalId,
			}),
		);
	}
	buttons.push(
		btn("❌ Deny", "danger", {
			hermes_action: "deny",
			approval_id: opts.approvalId,
		}),
	);
	return {
		config: { wide_screen_mode: true },
		header: {
			title: { tag: "plain_text", content: opts.title },
			template: "orange",
		},
		elements: [
			{ tag: "markdown", content: opts.detail },
			{ tag: "action", actions: buttons },
		],
	};
}

/**
 * Resolved-state replacement card (:2199 _build_resolved_approval_card):
 * header "{icon} {label}" (plain_text), template green/red, and THE
 * attribution line "{icon} **{label}** by {user_name}" as a markdown
 * element. This IS the ack payload for an approval tap (CallBackCard
 * type=raw; :2808).
 */
export function buildResolvedApprovalCard(
	choice: ExecApprovalChoice,
	userName: string,
): ApprovalCard {
	const icon = choice === "deny" ? "❌" : "✅";
	const label = FEISHU_RESOLVED_LABELS[choice] ?? "Resolved";
	return {
		config: { wide_screen_mode: true },
		header: {
			title: { tag: "plain_text", content: `${icon} ${label}` },
			template: choice === "deny" ? "red" : "green",
		},
		elements: [
			{
				tag: "markdown",
				content: `${icon} **${label}** by ${userName}`,
			},
		],
	};
}

/** Update-prompt card (:2131): ✓ Yes primary / ✗ No danger, orange template. */
export function buildUpdatePromptCard(opts: {
	title: string;
	detail: string;
	promptId: number;
}): ApprovalCard {
	return {
		config: { wide_screen_mode: true },
		header: {
			title: { tag: "plain_text", content: opts.title },
			template: "orange",
		},
		elements: [
			{ tag: "markdown", content: opts.detail },
			{
				tag: "action",
				actions: [
					btn("✓ Yes", "primary", {
						hermes_update_prompt_action: "y",
						update_prompt_id: opts.promptId,
					}),
					btn("✗ No", "danger", {
						hermes_update_prompt_action: "n",
						update_prompt_id: opts.promptId,
					}),
				],
			},
		],
	};
}

/**
 * Resolved update-prompt card (:2224 _build_resolved_update_prompt_card):
 * title "✅/❌ Update prompt answered: Yes/No", template green/red, and the
 * "Answered by **{user_name}**" markdown element.
 */
export function buildResolvedUpdatePromptCard(
	answer: "y" | "n",
	userName: string,
): ApprovalCard {
	const yes = answer === "y";
	return {
		config: { wide_screen_mode: true },
		header: {
			title: {
				tag: "plain_text",
				content: `${yes ? "✅" : "❌"} Update prompt answered: ${yes ? "Yes" : "No"}`,
			},
			template: yes ? "green" : "red",
		},
		elements: [{ tag: "markdown", content: `Answered by **${userName}**` }],
	};
}

/**
 * Generic click → synthetic COMMAND text (:3077–3082): `/card {tag} {json}`.
 * message_id = event token when present (dedup interplay avoided because the
 * token store is checked FIRST), else a fresh uuid-shaped fallback supplied
 * by the caller.
 */
export function buildGenericCardCommandText(
	tag: string,
	value: CardActionValue,
): string {
	return `/card ${tag} ${JSON.stringify(value)}`;
}

/**
 * Operator authorization (:2771 _is_interactive_operator_authorized):
 * allowed = admins ∪ FEISHU_ALLOWED_USERS; EMPTY set ⇒ everyone; "*" is a
 * wildcard member. Fail-closed for empty operator ids.
 */
export function isInteractiveOperatorAuthorized(
	openId: string,
	opts: { admins: ReadonlySet<string>; allowedUsers: ReadonlySet<string> },
): boolean {
	if (!openId) return false;
	if (opts.admins.has(openId)) return true;
	if (opts.allowedUsers.size === 0) return true; // empty ⇒ allow everyone
	if (opts.allowedUsers.has("*")) return true;
	return opts.allowedUsers.has(openId);
}
