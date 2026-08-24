// pi_platforms/slack/block-cards — Block Kit card BUILDING through the kit's
// parallel interactive mechanism (04 §9.2; DEC-016). The kit owns the render
// caps (renderBlocks DECLINES whole past 50 blocks / 3000-char sections) and
// the action-handler registry (acks even when handlers raise, inside Slack's
// 3-second ack window); this module builds the Hermes card SHAPES on top:
//
//   plugins/platforms/slack/adapter.py:send_exec_approval — warning header,
//     budgeted command preview (3000-char section cap), Allow Once/Session/
//     Always + Deny buttons with stable hermes_* action_ids
//   plugins/platforms/slack/adapter.py:send_clarify — indexed
//     hermes_clarify_choice_<idx> ids (unique within an actions block),
//     "Other…" free-text flip button, ≤5 elements per actions block,
//     mrkdwn-escaped question clamped to the section budget
//   plugins/platforms/slack/adapter.py:_is_block_payload_rejection — the
//     recoverable codes {invalid_blocks, msg_too_long, too_many_blocks} whose
//     failure mode is a retry WITHOUT blocks (never a dropped response)
//   plugins/platforms/slack/block_kit.py:render_blocks — decline-whole caps;
//     plain mrkdwn fallback ALWAYS ships alongside

import {
	assembleInteractiveMessage,
	clarifyChoiceActionId,
	CLARIFY_OTHER_ACTION_ID,
	renderBlocks,
	slashConfirmActionId,
	APPROVAL_ACTION_IDS,
	type InteractiveMessage,
	type KitBlock,
	type SlashConfirmChoice,
} from "../kit/index.js";
import type { ExecApprovalSendArgs } from "../../pi_embedded/approvals/delivery.js";

export { isBlockPayloadRejectionError } from "./block-rejection.js";

/** Section-block text hard cap (Slack invalid_blocks past it). */
const SECTION_TEXT_CAP = 3000;

function escapeMrkdwnControlChars(s: string): string {
	return s
		.split("&")
		.join("&amp;")
		.split("<")
		.join("&lt;")
		.split(">")
		.join("&gt;");
}

/**
 * Build THE exec-approval card. `value` carries the bridge-assigned approval
 * id so one tap maps onto THE namespaced `ea:` grammar via the router (the
 * parallel mechanism implements the SAME resolution seam — DEC-016).
 * Command preview budget math ports send_exec_approval: fixed parts are
 * subtracted from the 3000 cap instead of flat-truncating into overflow.
 */
export function buildExecApprovalCard(
	args: ExecApprovalSendArgs,
): InteractiveMessage {
	const header =
		":warning: *Command Approval Required*\n" +
		(args.smartDenied
			? "*Smart DENY:* owner override applies to this one operation only.\n"
			: "");
	const reason = `Reason: ${args.description.slice(0, 500)}`;
	const budget =
		SECTION_TEXT_CAP -
		header.length -
		reason.length -
		"``````\n".length -
		"...".length;
	const command = args.command;
	const preview =
		command.length > budget
			? `${command.slice(0, Math.max(0, budget))}...`
			: command;

	const actions: KitBlock[] = [
		{
			type: "button",
			text: { type: "plain_text", text: "Allow Once" },
			style: "primary",
			action_id: APPROVAL_ACTION_IDS.once,
			value: String(args.approvalId),
		},
	] as unknown as KitBlock[];
	if (!args.smartDenied && args.allowSession) {
		actions.push({
			type: "button",
			text: { type: "plain_text", text: "Allow Session" },
			action_id: APPROVAL_ACTION_IDS.session,
			value: String(args.approvalId),
		} as unknown as KitBlock);
		if (args.allowPermanent) {
			actions.push({
				type: "button",
				text: { type: "plain_text", text: "Always Allow" },
				action_id: APPROVAL_ACTION_IDS.always,
				value: String(args.approvalId),
			} as unknown as KitBlock);
		}
	}
	actions.push({
		type: "button",
		text: { type: "plain_text", text: "Deny" },
		style: "danger",
		action_id: APPROVAL_ACTION_IDS.deny,
		value: String(args.approvalId),
	} as unknown as KitBlock);

	const blocks: KitBlock[] = [
		{
			type: "section",
			text: {
				type: "mrkdwn",
				text: `${header}\`\`\`${preview}\`\`\`\n${reason}`,
			},
		},
		{ type: "actions", elements: actions },
	];

	return assembleInteractiveMessage(
		blocks,
		`⚠️ Command approval required: ${preview.slice(0, 100)}`,
	);
}

/**
 * Build THE clarify card: one indexed choice button per option + an "Other…"
 * free-text flip button; actions chunked at 5 elements per block.
 */
export function buildClarifyCard(opts: {
	question: string;
	choices: readonly string[];
	clarifyId: number;
}): InteractiveMessage {
	let body = `❓ ${escapeMrkdwnControlChars(opts.question ?? "")}`;
	if (body.length > SECTION_TEXT_CAP - "...".length) {
		body = body.slice(0, SECTION_TEXT_CAP - "...".length) + "...";
	}
	interface ButtonShape {
		type: string;
		text: { type: string; text: string; emoji?: boolean };
		action_id: string;
		value: string;
	}
	const elements: ButtonShape[] = [];
	opts.choices.forEach((choice, idx) => {
		const label = (String(choice).trim() || `Option ${idx + 1}`).slice(0, 75);
		elements.push({
			type: "button",
			text: { type: "plain_text", text: label, emoji: true },
			action_id: clarifyChoiceActionId(idx),
			value: `${opts.clarifyId}|${idx}`,
		});
	});
	elements.push({
		type: "button",
		text: { type: "plain_text", text: "✏️ Other…", emoji: true },
		action_id: CLARIFY_OTHER_ACTION_ID,
		value: `${opts.clarifyId}|other`,
	});
	const blocks: KitBlock[] = [
		{ type: "section", text: { type: "mrkdwn", text: body } },
	];
	for (let start = 0; start < elements.length; start += 5) {
		blocks.push({
			type: "actions",
			elements: elements.slice(start, start + 5) as unknown as KitBlock[],
		});
	}
	return assembleInteractiveMessage(blocks, body);
}

/**
 * Build THE slash-confirmation card (hermes_confirm_once/_always/_cancel
 * family).
 */
export function buildSlashConfirmCard(opts: {
	promptText: string;
	confirmId: number;
}): InteractiveMessage {
	interface ButtonShape {
		type: string;
		text: { type: string; text: string };
		style?: string;
		action_id: string;
		value: string;
	}
	const defs: Array<{
		choice: SlashConfirmChoice;
		label: string;
		style?: string;
	}> = [
		{ choice: "once", label: "Run once", style: "primary" },
		{ choice: "always", label: "Always run" },
		{ choice: "cancel", label: "Cancel", style: "danger" },
	];
	const elements = defs.map(({ choice, label, style }) => {
		const b: ButtonShape = {
			type: "button",
			text: { type: "plain_text", text: label },
			action_id: slashConfirmActionId(choice),
			value: String(opts.confirmId),
		};
		if (style !== undefined) b.style = style;
		return b;
	});
	const blocks: KitBlock[] = [
		{ type: "section", text: { type: "mrkdwn", text: opts.promptText } },
		{ type: "actions", elements: elements as unknown as KitBlock[] },
	];
	return assembleInteractiveMessage(blocks, opts.promptText);
}

/**
 * Opt-in rich content blocks (`platforms.slack.extra.rich_blocks` parity):
 * paragraphs → section blocks; whole render declines past kit caps and the
 * caller falls back to plain converted text (which ALWAYS ships anyway as the
 * notification/accessibility `text` field). Returns null when declined or
 * nothing structural remains.
 */
export function buildContentBlocks(content: string): KitBlock[] | null {
	const paragraphs = content
		.split(/\n{2,}/)
		.map((p) => p.trim())
		.filter((p) => p.length > 0);
	if (paragraphs.length === 0) return null;
	const blocks: KitBlock[] = paragraphs.map((p) => ({
		type: "section",
		text: { type: "mrkdwn", text: p },
	}));
	const rendered = renderBlocks(blocks);
	return rendered.ok ? rendered.blocks : null;
}
