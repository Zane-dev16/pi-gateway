// pi_platforms/kit/block-kit — the Slack-class PARALLEL interactive mechanism
// (04-platform-adapters.md §9.2; DEC-016). Platforms with richer native
// mechanisms implement the SAME resolution seam via stable action_ids plus a
// registered action-handler registry — a parallel mechanism, NOT a divergent
// wire grammar.
//
// Ported from the READ-ONLY Hermes reference, semantics only:
//   plugins/platforms/slack/adapter.py — hermes_approve_once/_session/_always,
//     hermes_deny → _handle_approval_action; hermes_confirm_*/cancel →
//     _handle_slash_confirm_action; ^hermes_clarify_choice_\d+$ +
//     hermes_clarify_other → _handle_clarify_action (indexed because action_ids
//     must be unique within an actions block)
//   hermes_cli/plugins.py — register_slack_action_handler(string|regex|
//     constraint dict); drained at connect; wrapped so plugin exceptions are
//     caught AND still acked inside Slack's 3-second ack window
//   plugins/platforms/slack/block_kit.py:render_blocks — pure function that
//     DECLINES whole (>50 blocks, >3000-char section text, unexpected shape)
//     rather than truncating; plain mrkdwn ALWAYS ships alongside as the
//     accessible/notification fallback

import type { ExecApprovalChoice } from "./callback-grammar.js";
import type { SlashConfirmChoice } from "./callback-grammar.js";

/** Slack's 3-second ack window (plugins must ack even while raising). */
export const SLACK_ACK_WINDOW_MS = 3_000;

/** Render caps — a declining renderer never truncates. */
export const MAX_BLOCKS = 50;
export const MAX_SECTION_TEXT_CHARS = 3_000;
/** Native table caps: 100 rows × 20 cols × 10k cell chars, else fallback. */
export const MAX_TABLE_ROWS = 100;
export const MAX_TABLE_COLS = 20;
export const MAX_TABLE_CELL_CHARS = 10_000;

// ── block shapes (minimal, for the decliner contract) ────────────────────────

export interface SectionBlock {
	type: "section";
	text: { type: "mrkdwn" | "plain_text"; text: string };
	accessory?: unknown;
}

export interface ActionsBlock {
	type: "actions";
	/** Stable action_ids — unique WITHIN the actions block (indexed clarify). */
	elements: Array<{ type: string; action_id?: string }>;
}

export type KitBlock =
	| SectionBlock
	| ActionsBlock
	| { type: string; [k: string]: unknown };

export type RenderBlocksResult =
	| { ok: true; blocks: KitBlock[] }
	| { ok: false; reason: string };

/**
 * Pure function that DECLINES WHOLE past caps (>50 blocks, >3000-char section
 * text, unexpected shape) rather than truncating (block_kit.py:render_blocks).
 * Callers ship plain mrkdwn fallback alongside on decline.
 */
export function renderBlocks(blocks: unknown): RenderBlocksResult {
	if (!Array.isArray(blocks))
		return { ok: false, reason: "blocks must be an array" };
	if (blocks.length > MAX_BLOCKS) {
		return {
			ok: false,
			reason: `too many blocks (${blocks.length} > ${MAX_BLOCKS})`,
		};
	}
	for (const block of blocks) {
		if (block === null || typeof block !== "object" || !("type" in block)) {
			return { ok: false, reason: "unexpected block shape" };
		}
		const b = block as { type: unknown; text?: unknown };
		if (b.type === "section") {
			const text = (b.text as { text?: unknown } | undefined)?.text;
			if (typeof text === "string" && text.length > MAX_SECTION_TEXT_CHARS) {
				return {
					ok: false,
					reason: `section text exceeds ${MAX_SECTION_TEXT_CHARS} chars`,
				};
			}
		}
	}
	return { ok: true, blocks: blocks as KitBlock[] };
}

/** An interactive message always carries its accessible mrkdwn fallback. */
export interface InteractiveMessage {
	blocks?: KitBlock[] | undefined;
	/** Plain mrkdwn — ALWAYS shipped alongside blocks (notification/accessibility). */
	mrkdwnText: string;
}

/**
 * Assemble an interactive message: blocks attached only when they render
 * whole; mrkdwn fallback unconditionally present.
 */
export function assembleInteractiveMessage(
	blocks: unknown,
	mrkdwnText: string,
): InteractiveMessage {
	const rendered = renderBlocks(blocks);
	return rendered.ok ? { blocks: rendered.blocks, mrkdwnText } : { mrkdwnText }; // whole-render decline → mrkdwn only
}

// ── action-handler registry ───────────────────────────────────────────────────

/** Constraint-dict registration shape (hermes_cli/plugins.py parity). */
export interface ActionConstraint {
	prefix?: string | undefined;
	regex?: string | undefined;
}

export type ActionMatcher = string | RegExp | ActionConstraint;

export interface ActionDispatch {
	actionId: string;
	payload: Record<string, unknown>;
	/** Clicker authorization result — handlers see pre-authorized calls only
	 * when the adapter gates before dispatch; registry passes it through. */
	authorized?: boolean | undefined;
}

export interface ActionAck {
	acked: boolean;
	/** The handler raised — acked anyway inside the 3-second window. */
	handlerError?: string | undefined;
}

function matches(matcher: ActionMatcher, actionId: string): boolean {
	if (typeof matcher === "string") return matcher === actionId;
	if (matcher instanceof RegExp) return matcher.test(actionId);
	if (matcher.prefix !== undefined && !actionId.startsWith(matcher.prefix))
		return false;
	if (matcher.regex !== undefined && !new RegExp(matcher.regex).test(actionId))
		return false;
	return true;
}

/**
 * Registered action handlers, drained at connect (plugins.py parity).
 * Dispatch wraps EVERY handler so plugin exceptions are caught AND still
 * acked inside Slack's 3-second ack window.
 */
export class ActionHandlerRegistry {
	private readonly handlers: Array<{
		matcher: ActionMatcher;
		handler: (d: ActionDispatch) => Promise<void> | void;
	}> = [];
	private drained = false;

	register(
		matcher: ActionMatcher,
		handler: (d: ActionDispatch) => Promise<void> | void,
	): void {
		this.handlers.push({ matcher, handler });
	}

	/** Connect-time drain parity: after connect, new registrations are late —
	 * Hermes drains at connect; we keep accepting but flag it. */
	drainAtConnect(): void {
		this.drained = true;
	}

	get isDrained(): boolean {
		return this.drained;
	}

	get size(): number {
		return this.handlers.length;
	}

	/**
	 * Route ONE action through matching handlers. Exceptions are caught and
	 * reported ON the ack — the ack itself never fails past the window.
	 */
	async dispatch(d: ActionDispatch): Promise<ActionAck> {
		for (const { matcher, handler } of this.handlers) {
			if (!matches(matcher, d.actionId)) continue;
			try {
				await handler(d);
			} catch (err) {
				return {
					acked: true,
					handlerError: err instanceof Error ? err.message : String(err),
				};
			}
			return { acked: true };
		}
		// No handler matched — still ack (Slack requires it).
		return { acked: true };
	}
}

// ── stable action_id vocabulary (DEC-016 shared semantics) ────────────────────

export const APPROVAL_ACTION_IDS = {
	once: "hermes_approve_once",
	session: "hermes_approve_session",
	always: "hermes_approve_always",
	deny: "hermes_deny",
} as const;

export function approvalActionId(choice: ExecApprovalChoice): string {
	return APPROVAL_ACTION_IDS[choice];
}

export const SLASH_CONFIRM_ACTION_IDS = {
	once: "hermes_confirm_once",
	always: "hermes_confirm_always",
	cancel: "hermes_confirm_cancel",
} as const;

export function slashConfirmActionId(choice: SlashConfirmChoice): string {
	return SLASH_CONFIRM_ACTION_IDS[choice];
}

/**
 * Clarify action_ids are INDEXED (^hermes_clarify_choice_\d+$) because
 * action_ids must be unique within an actions block.
 */
export const CLARIFY_CHOICE_ACTION_RE = /^hermes_clarify_choice_(\d+)$/;
export const CLARIFY_OTHER_ACTION_ID = "hermes_clarify_other";

export function clarifyChoiceActionId(idx: number): string {
	return `hermes_clarify_choice_${idx}`;
}

/** Extract the choice index from a clarify action_id (null when not matching). */
export function parseClarifyChoiceActionId(actionId: string): number | null {
	const m = CLARIFY_CHOICE_ACTION_RE.exec(actionId);
	return m ? Number(m[1]) : null;
}
