// Block Kit parallel-mechanism contracts (04 §9.2; DEC-016): whole-render
// decline past caps (never truncation), mrkdwn accessibility fallback always
// shipped, handler registry acks even when raising, indexed clarify action_ids.

import { describe, expect, it } from "vitest";
import {
	ActionHandlerRegistry,
	CLARIFY_CHOICE_ACTION_RE,
	MAX_BLOCKS,
	MAX_SECTION_TEXT_CHARS,
	assembleInteractiveMessage,
	approvalActionId,
	clarifyChoiceActionId,
	parseClarifyChoiceActionId,
	renderBlocks,
	slashConfirmActionId,
} from "./block-kit.js";

describe("renderBlocks declines WHOLE past caps", () => {
	it("declines >50 blocks with a reason — never truncates", () => {
		const blocks = Array.from({ length: MAX_BLOCKS + 1 }, () => ({
			type: "section",
			text: { type: "mrkdwn", text: "x" },
		}));
		const result = renderBlocks(blocks);
		expect(result.ok).toBe(false);
		if (!result.ok) expect(result.reason).toContain("50");
	});

	it("declines a section over 3000 chars — never truncates", () => {
		const result = renderBlocks([
			{
				type: "section",
				text: { type: "mrkdwn", text: "x".repeat(MAX_SECTION_TEXT_CHARS + 1) },
			},
		]);
		expect(result.ok).toBe(false);
		if (!result.ok) expect(result.reason).toContain("3000");
	});

	it("accepts exactly-at-cap renders", () => {
		const ok = renderBlocks([
			{
				type: "section",
				text: { type: "mrkdwn", text: "x".repeat(MAX_SECTION_TEXT_CHARS) },
			},
		]);
		expect(ok.ok).toBe(true);
	});

	it("declines unexpected shapes", () => {
		expect(renderBlocks("not-an-array").ok).toBe(false);
		expect(renderBlocks([42]).ok).toBe(false);
	});
});

describe("mrkdwn fallback ALWAYS ships alongside", () => {
	it("blocks attach only when they render whole; mrkdwn present either way", () => {
		const good = assembleInteractiveMessage(
			[{ type: "section", text: { type: "mrkdwn", text: "hello" } }],
			"plain hello",
		);
		expect(good.blocks).toHaveLength(1);
		expect(good.mrkdwnText).toBe("plain hello");

		const declined = assembleInteractiveMessage(
			Array.from({ length: MAX_BLOCKS + 1 }, () => ({
				type: "section",
				text: { type: "mrkdwn", text: "y" },
			})),
			"fallback text",
		);
		expect(declined.blocks).toBeUndefined(); // whole-render decline
		expect(declined.mrkdwnText).toBe("fallback text"); // accessible fallback
	});
});

describe("action-handler registry acks even when raising", () => {
	it("matched handler exceptions are caught AND still acked (3-second window)", async () => {
		const registry = new ActionHandlerRegistry();
		registry.register(approvalActionId("deny"), () => {
			throw new Error("plugin exploded");
		});
		const ack = await registry.dispatch({
			actionId: approvalActionId("deny"),
			payload: {},
		});
		expect(ack.acked).toBe(true); // ack inside Slack's window regardless
		expect(ack.handlerError).toContain("plugin exploded");
	});

	it("string, regex, and constraint matchers all route", async () => {
		const registry = new ActionHandlerRegistry();
		const seen: string[] = [];
		registry.register(slashConfirmActionId("cancel"), ({ actionId }) => {
			seen.push(`exact:${actionId}`);
		});
		registry.register(CLARIFY_CHOICE_ACTION_RE, ({ actionId }) => {
			seen.push(`regex:${actionId}`);
		});
		registry.register({ prefix: "hermes_approve" }, ({ actionId }) => {
			seen.push(`prefix:${actionId}`);
		});
		await registry.dispatch({
			actionId: slashConfirmActionId("cancel"),
			payload: {},
		});
		await registry.dispatch({
			actionId: clarifyChoiceActionId(2),
			payload: {},
		});
		await registry.dispatch({
			actionId: approvalActionId("once"),
			payload: {},
		});
		expect(seen).toEqual([
			`exact:${slashConfirmActionId("cancel")}`,
			`regex:${clarifyChoiceActionId(2)}`,
			`prefix:${approvalActionId("once")}`,
		]);
	});

	it("unmatched actions still ack (Slack requires it)", async () => {
		const registry = new ActionHandlerRegistry();
		const ack = await registry.dispatch({
			actionId: "nobody_home",
			payload: {},
		});
		expect(ack.acked).toBe(true);
		expect(ack.handlerError).toBeUndefined();
	});
});

describe("clarify action_ids are INDEXED (unique within an actions block)", () => {
	it("indexed ids parse back to their choice index; non-members rejected", () => {
		expect(parseClarifyChoiceActionId(clarifyChoiceActionId(0))).toBe(0);
		expect(parseClarifyChoiceActionId(clarifyChoiceActionId(17))).toBe(17);
		expect(parseClarifyChoiceActionId("hermes_clarify_other")).toBeNull();
		expect(parseClarifyChoiceActionId("hermes_clarify_choice_x")).toBeNull();
	});
});
