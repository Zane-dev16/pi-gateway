// pi_platforms/qqbot/keyboards — QQ Bot v2 inline keyboards + approval /
// update-prompt button grammar, ported from Hermes
// gateway/platforms/qqbot/keyboards.py (itself from WideLee's qqbot-agent-sdk
// v1.2.2; authorship preserved upstream).
//
// Hermes anchors:
//   keyboards.py:InlineKeyboard/.to_dict — serialized into the outbound body's
//     `keyboard` field.
//   keyboards.py:build_approval_keyboard — 3-button ✅ once / ⭐ always / ❌
//     deny layout, all sharing group_id='approval' (mutually exclusive);
//     allow_permanent=false hides the ⭐ button.
//   keyboards.py:build_update_prompt_keyboard — ✓ confirm / ✗ cancel pair.
//   keyboards.py:parse_approval_button_data / parse_update_prompt_button_data
//     — `approve:<session_key>:<decision>` and `update_prompt:<y|n>`.
//   keyboards.py:parse_interaction_event / InteractionEvent.operator_openid.

export const APPROVAL_BUTTON_PREFIX = "approve:";
export const UPDATE_PROMPT_PREFIX = "update_prompt:";

export type ApprovalDecision = "allow-once" | "allow-always" | "deny";
export type UpdatePromptAnswer = "y" | "n";

/**
 * Parse approval `button_data` into [sessionKey, decision]
 * (keyboards.py:parse_approval_button_data). The session key itself contains
 * colons (agent:main:qqbot:c2c:OPENID) so the session group is GREEDY and the
 * decision trails. Returns null when not an approval button.
 */
export function parseApprovalButtonData(
	buttonData: string,
): [sessionKey: string, decision: ApprovalDecision] | null {
	const m = /^approve:(.+):(allow-once|allow-always|deny)$/.exec(
		buttonData ?? "",
	);
	if (m === null) return null;
	return [m[1] as string, m[2] as ApprovalDecision];
}

/** Parse `update_prompt:y|n` (keyboards.py:parse_update_prompt_button_data). */
export function parseUpdatePromptButtonData(
	buttonData: string,
): UpdatePromptAnswer | null {
	const m = /^update_prompt:(y|n)$/.exec(buttonData ?? "");
	return m === null ? null : (m[1] as UpdatePromptAnswer);
}

// ── keyboard payload shapes (keyboards.py dataclasses → wire dicts) ─────────

export interface KeyboardButtonPermission {
	readonly type: number; // 2 = all users can click
}
export interface KeyboardButtonAction {
	readonly type: number; // 1 = callback (INTERACTION_CREATE), 2 = link
	readonly data: string;
	readonly permission: KeyboardButtonPermission;
	readonly click_limit: number; // 1 = single-use per user
}
export interface KeyboardButtonRenderData {
	readonly label: string;
	readonly visited_label: string;
	readonly style: number; // 0 grey, 1 blue
}
export interface KeyboardButton {
	readonly id: string;
	readonly render_data: KeyboardButtonRenderData;
	readonly action: KeyboardButtonAction;
	readonly group_id: string;
}
export interface KeyboardRowWire {
	readonly buttons: readonly KeyboardButton[];
}
export interface KeyboardContentWire {
	readonly rows: readonly KeyboardRowWire[];
}
export interface InlineKeyboardWire {
	readonly content: KeyboardContentWire;
}

function makeCallbackButton(
	btnId: string,
	label: string,
	visitedLabel: string,
	data: string,
	style: number,
	groupId: string,
): KeyboardButton {
	return {
		id: btnId,
		render_data: { label, visited_label: visitedLabel, style },
		action: {
			type: 1,
			data,
			permission: { type: 2 },
			click_limit: 1,
		},
		group_id: groupId,
	};
}

/**
 * The approval keyboard (keyboards.py:build_approval_keyboard). Buttons share
 * group_id='approval' so clicking one greys the rest. allowPermanent=false
 * omits the ⭐ 始终允许 button entirely.
 */
export function buildApprovalKeyboard(
	sessionKey: string,
	opts: { allowPermanent?: boolean | undefined } = {},
): InlineKeyboardWire {
	const allowPermanent = opts.allowPermanent !== false;
	const buttons: KeyboardButton[] = [
		makeCallbackButton(
			"allow",
			"✅ 允许一次",
			"已允许",
			`${APPROVAL_BUTTON_PREFIX}${sessionKey}:allow-once`,
			1,
			"approval",
		),
	];
	if (allowPermanent) {
		buttons.push(
			makeCallbackButton(
				"always",
				"⭐ 始终允许",
				"已始终允许",
				`${APPROVAL_BUTTON_PREFIX}${sessionKey}:allow-always`,
				1,
				"approval",
			),
		);
	}
	buttons.push(
		makeCallbackButton(
			"deny",
			"❌ 拒绝",
			"已拒绝",
			`${APPROVAL_BUTTON_PREFIX}${sessionKey}:deny`,
			0,
			"approval",
		),
	);
	return { content: { rows: [{ buttons }] } };
}

/** Yes/No update-confirmation keyboard (keyboards.py:build_update_prompt_keyboard). */
export function buildUpdatePromptKeyboard(): InlineKeyboardWire {
	return {
		content: {
			rows: [
				{
					buttons: [
						makeCallbackButton(
							"yes",
							"✓ 确认",
							"已确认",
							`${UPDATE_PROMPT_PREFIX}y`,
							1,
							"update_prompt",
						),
						makeCallbackButton(
							"no",
							"✗ 取消",
							"已取消",
							`${UPDATE_PROMPT_PREFIX}n`,
							0,
							"update_prompt",
						),
					],
				},
			],
		},
	};
}

// ── INTERACTION_CREATE parsing (keyboards.py:InteractionEvent) ───────────────

export type InteractionScene = "" | "guild" | "group" | "c2c";

export class InteractionEvent {
	constructor(
		readonly id: string,
		readonly type: number,
		readonly chatType: number,
		readonly scene: InteractionScene,
		readonly groupOpenid: string,
		readonly groupMemberOpenid: string,
		readonly userOpenid: string,
		readonly channelId: string,
		readonly guildId: string,
		readonly buttonData: string,
		readonly buttonId: string,
		readonly resolverUserId: string,
	) {}

	/** Best available operator openid (group → member; c2c → user). */
	get operatorOpenid(): string {
		return this.groupMemberOpenid || this.userOpenid || this.resolverUserId;
	}
}

const SCENE_BY_CODE: Record<number, InteractionScene> = {
	0: "guild",
	1: "group",
	2: "c2c",
};

/** Parse a raw INTERACTION_CREATE dispatch payload (keyboards.py:parse_interaction_event). */
export function parseInteractionEvent(
	raw: Record<string, unknown>,
): InteractionEvent {
	const dataRaw =
		(raw["data"] as Record<string, unknown> | undefined) ?? undefined;
	const resolved =
		(dataRaw?.["resolved"] as Record<string, unknown> | undefined) ?? undefined;
	const sceneCode = Number(raw["chat_type"] ?? 0) || 0;
	return new InteractionEvent(
		String(raw["id"] ?? ""),
		Number(dataRaw?.["type"] ?? 0) || 0,
		sceneCode,
		SCENE_BY_CODE[sceneCode] ?? "",
		String(raw["group_openid"] ?? ""),
		String(raw["group_member_openid"] ?? ""),
		String(raw["user_openid"] ?? ""),
		String(raw["channel_id"] ?? ""),
		String(raw["guild_id"] ?? ""),
		String(resolved?.["button_data"] ?? ""),
		String(resolved?.["button_id"] ?? ""),
		String(resolved?.["user_id"] ?? ""),
	);
}
