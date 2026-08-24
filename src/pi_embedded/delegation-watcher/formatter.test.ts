// Behavior contracts for the re-injection FORMATTER (port of
// tools/process_registry.py:_format_async_delegation + run.py
// _format_coalesced_async_delegations). The block must stand entirely on its
// own: full task source, status, and result — the receiving agent may be deep
// in unrelated context and won't remember why the subagent existed.

import { describe, expect, it } from "vitest";

import {
	delegationAttributionLine,
	formatAge,
	formatAsyncDelegation,
	formatCoalescedAsyncDelegations,
} from "./formatter.js";

const BASE = {
	type: "async_delegation",
	delegation_id: "dlg-fmt",
	session_key: "agent:main:telegram:dm:100",
	parent_session_id: "parent",
	goal: "sweep the logs",
	context: "prod incident 4412",
	toolsets: ["web search", "extract"],
	role: "leaf",
	model: "pi-main",
	status: "completed",
	summary: "found 3 anomalies",
	api_calls: 12,
	duration_seconds: 95,
	dispatched_at: 1_775_000_000,
	completed_at: 1_775_000_095,
};

describe("single completion block", () => {
	it("carries the FULL original task source plus the result", () => {
		const text = formatAsyncDelegation(BASE);
		expect(text).toContain("[ASYNC DELEGATION COMPLETE — dlg-fmt]");
		expect(text).toContain("Original goal: sweep the logs");
		expect(text).toContain("Context you provided: prod incident 4412");
		expect(text).toContain("Toolsets: web search, extract");
		expect(text).toContain("Role: leaf   Model: pi-main");
		expect(text).toContain("Status: completed   API calls: 12   Duration: 95s");
		expect(text).toContain("--- RESULT ---\nfound 3 anomalies");
	});

	it("marks truncated work loudly", () => {
		const text = formatAsyncDelegation({
			...BASE,
			exit_reason: "max_iterations",
			summary: "partial view",
		});
		expect(text).toContain(
			"TRUNCATED: hit max_iterations — work may be incomplete",
		);
		expect(text).toContain("[TRUNCATED — subagent hit its iteration cap;");
		expect(text).toContain("partial view");
	});

	it("failure statuses surface the error, summary as partial output", () => {
		const text = formatAsyncDelegation({
			...BASE,
			status: "failed",
			error: "provider socket reset",
		});
		expect(text).toContain(
			"The subagent did not complete successfully (status=failed).",
		);
		expect(text).toContain("provider socket reset");
		expect(text).toContain("Partial output:");
	});

	it("interrupted status explains the interruption", () => {
		const text = formatAsyncDelegation({ ...BASE, status: "interrupted" });
		expect(text).toContain("The subagent was interrupted before completing.");
	});
});

describe("fan-out batch block", () => {
	it("renders every subagent's per-task summary in ONE consolidated block", () => {
		const text = formatAsyncDelegation({
			...BASE,
			is_batch: true,
			goals: ["task zero", "task one"],
			results: [
				{
					task_index: 1,
					status: "completed",
					summary: "second done",
				},
				{
					task_index: 0,
					status: "failed",
					error: "boom",
					summary: "first partial",
				},
			],
			total_duration_seconds: 300,
		});
		expect(text).toContain("[ASYNC DELEGATION BATCH COMPLETE — dlg-fmt]");
		expect(text).toContain("A background fan-out of 2 subagent(s)");
		expect(text.indexOf("TASK 1/2")).toBeLessThan(text.indexOf("TASK 2/2")); // sorted by index
		expect(text).toContain(": task zero");
		expect(text).toContain("(failed: boom)");
		expect(text).toContain("Partial output:\nfirst partial");
		expect(text).toContain("✓ TASK 2/2"); // success icon on the completed task
		expect(text).toContain("second done");
	});

	it("a batch with an error and no results renders the batch error arm", () => {
		const text = formatAsyncDelegation({
			...BASE,
			is_batch: true,
			goals: ["a", "b"],
			error: "whole fan-out failed to start",
		});
		expect(text).toContain("--- ERROR ---");
		expect(text).toContain(
			"The batch did not complete successfully: whole fan-out failed to start",
		);
	});
});

describe("attribution + age helpers", () => {
	it("sa-* task ids attribute generically; others attribute nothing", () => {
		expect(delegationAttributionLine({ task_id: "sa-abc123" })).toContain(
			"Started by subagent sa-abc123 (delegate_task).",
		);
		expect(delegationAttributionLine({ task_id: "proc_99" })).toBeNull();
		expect(delegationAttributionLine({})).toBeNull();
	});

	it("formatAge parity ('45s', '18m', '2h3m')", () => {
		expect(formatAge(45)).toBe("45s");
		expect(formatAge(60)).toBe("1m");
		expect(formatAge(18 * 60 + 5)).toBe("18m5s");
		expect(formatAge(2 * 3600 + 3 * 60)).toBe("2h3m");
		expect(formatAge(2 * 3600)).toBe("2h");
		expect(formatAge("junk")).toBe("?");
	});
});

describe("coalesced multi-delegation turn (#70300)", () => {
	it("joins blocks under one IMPORTANT header demanding a single response", () => {
		const text = formatCoalescedAsyncDelegations(["BLOCK A", "BLOCK B"]);
		expect(
			text.startsWith("[IMPORTANT: 2 background subagent delegations"),
		).toBe(true);
		expect(text.replace(/\s+/g, " ")).toContain(
			"Treat these results as one completion batch",
		);
		expect(text.replace(/\s+/g, " ")).toContain(
			"send at most one consolidated user-facing response",
		);
		expect(text).toContain("BLOCK A\n\nBLOCK B");
	});
});
