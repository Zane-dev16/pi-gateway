// Behavior contracts for the answer surfaces above THE one resolution
// primitive (07 §8.4, §8.5): slash parsing matrices, stale-answer honesty,
// bare-word gating, and the authorize-first click flow.

import { describe, expect, it } from "vitest";

import {
	ApprovalEntry,
	ApprovalQueues,
	type ApprovalRequestData,
} from "./queue.js";
import { ApprovalCardLedger } from "./delivery.js";
import {
	AnswerRejected,
	classifyBareWord,
	handleApprovalClick,
	handleApprove,
	handleDeny,
	parseApproveArgs,
	parseDenyArgs,
	resolveByRequestId,
	routeBareAnswer,
	type AnswerSource,
	type SurfaceDeps,
} from "./surfaces.js";
import {
	ALREADY_RESOLVED_TEXT,
	APPROVAL_EXPIRED,
	DENY_STALE,
	NOT_AUTHORIZED_TEXT,
} from "./render.js";

const BOUND: AnswerSource = { surface: "telegram", chatId: "42" };
const STRANGER: AnswerSource = { surface: "discord", chatId: "99" };

function makeDeps(overrides: Partial<SurfaceDeps> = {}): SurfaceDeps & {
	queues: ApprovalQueues;
	ledger: ApprovalCardLedger;
} {
	const queues = new ApprovalQueues();
	const ledger = new ApprovalCardLedger();
	const deps: SurfaceDeps = {
		queues,
		ledger,
		expiredApprovals: new Set<string>(),
		// Bot-ACL parity: authorization is CALLER-IDENTITY based (the bound
		// surface), not queue-state based — an answerer stays authorized even
		// when the binding has been popped, so stale taps reach the honest
		// "already resolved" / "expired" renders.
		isAnswerAuthorized: (source) =>
			source.surface === BOUND.surface && source.chatId === BOUND.chatId,
	};
	return Object.assign(deps, overrides, { queues, ledger });
}

function pending(
	deps: ReturnType<typeof makeDeps>,
	requestId?: string,
	command = "rm -rf /tmp/x",
): ApprovalEntry {
	const request: ApprovalRequestData = {
		command,
		description: "delete",
		patternKey: "rm_rf",
		patternKeys: ["rm_rf"],
	};
	if (requestId !== undefined) {
		request.requestId = requestId;
	}
	const entry = new ApprovalEntry(request);
	deps.queues.enqueue("s", entry);
	return entry;
}

describe("slash arg parsing", () => {
	it("/approve parses all/session/always combos (lowercased)", () => {
		expect(parseApproveArgs("")).toEqual({ resolveAll: false, choice: "once" });
		expect(parseApproveArgs("all")).toEqual({
			resolveAll: true,
			choice: "once",
		});
		expect(parseApproveArgs("SESSION")).toEqual({
			resolveAll: false,
			choice: "session",
		});
		expect(parseApproveArgs("always")).toEqual({
			resolveAll: false,
			choice: "always",
		});
		expect(parseApproveArgs("all always")).toEqual({
			resolveAll: true,
			choice: "always",
		});
		expect(parseApproveArgs("permanent")).toEqual({
			resolveAll: false,
			choice: "always",
		});
	});

	it("/deny keeps the reason VERBATIM (not lowercased) and caps at 280 chars", () => {
		expect(parseDenyArgs("")).toEqual({ resolveAll: false, reason: null });
		expect(parseDenyArgs("all")).toEqual({ resolveAll: true, reason: null });
		const parsed = parseDenyArgs("This Deletes PRODUCTION — do NOT re-run");
		expect(parsed.resolveAll).toBe(false);
		expect(parsed.reason).toBe("This Deletes PRODUCTION — do NOT re-run");
		expect(parseDenyArgs("all  cleanup first").reason).toBe("cleanup first");

		const long = "y".repeat(400);
		const capped = parseDenyArgs(long);
		expect(capped.reason?.length).toBe(280);
	});
});

describe("handleApprove / handleDeny", () => {
	it("approve resolves FIFO oldest with the chosen scope and resumes typing", () => {
		const deps = makeDeps();
		let resumed = "";
		deps.resumeTyping = (key) => {
			resumed = key;
		};
		pending(deps);
		pending(deps, undefined, "sudo reboot");

		const result = handleApprove(deps, { sessionKey: "s", rawArgs: "" });
		expect(result.count).toBe(1);
		expect(result.reply).toContain("✅ Command approved");
		expect(resumed).toBe("s");

		const second = handleApprove(deps, { sessionKey: "s", rawArgs: "all" });
		expect(second.count).toBe(1); // only "sudo reboot" remained
		expect(second.reply).toBe("✅ Command approved. The agent is resuming...");
	});

	it("deny relays a capped reason into the entry verbatim", () => {
		const deps = makeDeps();
		const entry = pending(deps);
		const result = handleDeny(deps, {
			sessionKey: "s",
			rawArgs: "staging DB is live",
		});
		expect(result.count).toBe(1);
		expect(entry.result).toBe("deny");
		expect(entry.reason).toBe("staging DB is live");
		expect(result.reply).toContain(
			'Reason relayed to the agent: "staging DB is live"',
		);
	});

	it("nothing pending ⇒ no_pending rejection; expired marker ⇒ honest 'approval expired'", () => {
		const deps = makeDeps();
		expect(() =>
			handleApprove(deps, { sessionKey: "s", rawArgs: "" }),
		).toThrowError(AnswerRejected);

		deps.expiredApprovals.add("s");
		try {
			handleApprove(deps, { sessionKey: "s", rawArgs: "" });
			expect.unreachable("must throw");
		} catch (err) {
			expect((err as AnswerRejected).code).toBe("stale_expired");
			expect((err as Error).message).toBe(APPROVAL_EXPIRED);
		}
		// Marker consumed by the attempt.
		expect(deps.expiredApprovals.has("s")).toBe(false);
	});

	it("/deny distinguishes its own stale form ('approval was stale')", () => {
		const deps = makeDeps();
		deps.expiredApprovals.add("s");
		try {
			handleDeny(deps, { sessionKey: "s", rawArgs: "" });
			expect.unreachable("must throw");
		} catch (err) {
			expect((err as Error).message).toBe(DENY_STALE);
		}
	});
});

describe("bare words while blocked (§8.5)", () => {
	it("classifies the full parity word set including emoji", () => {
		for (const word of ["approve", "yes", "ok", "okay", "confirm", "y", "👍"]) {
			expect(classifyBareWord(word)?.verb).toBe("approve");
		}
		for (const word of ["deny", "no", "reject", "cancel", "n", "👎"]) {
			expect(classifyBareWord(word)?.verb).toBe("deny");
		}
		expect(classifyBareWord("Always")?.args).toBe("always");
		expect(classifyBareWord("approve session")?.args).toBe("session");
		expect(classifyBareWord("yes please ship it")).toBeNull(); // conversational
	});

	it("synthesis ALWAYS uses a literal '/' regardless of display prefix", () => {
		expect(classifyBareWord("yes")?.synthesizedCommand).toBe("/approve");
		expect(classifyBareWord("no")?.synthesizedCommand).toBe("/deny");
		expect(classifyBareWord("always")?.synthesizedCommand).toBe(
			"/approve always",
		);
	});

	it("routes through the SAME handlers only when control-allowed AND blocking", () => {
		const deps = makeDeps();
		pending(deps);

		// Control permission missing → conversational text stays untouched.
		const denied = routeBareAnswer(deps, {
			sessionKey: "s",
			text: "yes",
			controlAllowed: false,
		});
		expect(denied.routed).toBe(false);
		expect(denied.skipReason).toBe("control_denied");
		expect(deps.queues.hasBlocking("s")).toBe(true); // nothing resolved

		// Control allowed + live blocking approval → resolves.
		const ok = routeBareAnswer(deps, {
			sessionKey: "s",
			text: "yes",
			controlAllowed: true,
		});
		expect(ok.routed).toBe(true);
		expect(ok.reply).toContain("✅");
		expect(deps.queues.hasBlocking("s")).toBe(false);
	});

	it("a conversational 'yes' with NOTHING pending stays conversational", () => {
		const deps = makeDeps();
		const result = routeBareAnswer(deps, {
			sessionKey: "s",
			text: "yes",
			controlAllowed: true,
		});
		expect(result.routed).toBe(false);
		expect(result.skipReason).toBe("nothing_blocking");
	});
});

describe("inline-button clicks — authorize FIRST, pop, resolve FIRST render SECOND", () => {
	it("full happy path: resolves via THE primitive, label from actual count", async () => {
		const deps = makeDeps();
		pending(deps);
		deps.ledger.bind(7, "s");

		const result = await handleApprovalClick(deps, {
			callbackData: "ea:once:7",
			source: BOUND,
			userDisplay: "Ada",
		});
		expect(result.outcome).toBe("resolved");
		expect(result.resolvedCount).toBe(1);
		expect(result.answerText).toBe("✅ Approved once by Ada");
		expect(result.editText).toContain("by Ada");
		expect(result.stripKeyboard).toBe(true);
		expect(deps.queues.hasBlocking("s")).toBe(false);
	});

	it("UNAUTHORIZED source rejected BEFORE state is touched (no ledger pop)", async () => {
		const deps = makeDeps();
		pending(deps);
		deps.ledger.bind(7, "s");

		const result = await handleApprovalClick(deps, {
			callbackData: "ea:once:7",
			source: STRANGER,
		});
		expect(result.outcome).toBe("unauthorized");
		expect(result.answerText).toBe(NOT_AUTHORIZED_TEXT);
		// NO state touched: binding intact, queue intact.
		expect(deps.ledger.peek(7)).toBe("s");
		expect(deps.queues.hasBlocking("s")).toBe(true);
	});

	it("duplicate/stale tap after pop ⇒ 'already been resolved'", async () => {
		const deps = makeDeps();
		pending(deps);
		deps.ledger.bind(7, "s");

		await handleApprovalClick(deps, {
			callbackData: "ea:deny:7",
			source: BOUND,
		});
		const second = await handleApprovalClick(deps, {
			callbackData: "ea:deny:7",
			source: BOUND,
		});
		expect(second.outcome).toBe("already_resolved");
		expect(second.answerText).toBe(ALREADY_RESOLVED_TEXT);
	});

	it("STALE tap after expiry answers honestly — never fabricated success (#63501)", async () => {
		const deps = makeDeps();
		pending(deps);
		deps.ledger.bind(9, "s");

		// The wait timed out server-side; the queue drained but the card's
		// ledger binding survived (registration stayed armed).
		deps.queues.clearSession("s");

		const result = await handleApprovalClick(deps, {
			callbackData: "ea:always:9",
			source: BOUND,
		});
		expect(result.outcome).toBe("expired");
		expect(result.resolvedCount).toBe(0);
		expect(result.answerText).toBe("⌛ Approval expired");
		expect(result.editText).toContain("no command was waiting");
	});

	it("malformed callback data answers 'Invalid approval data.'", async () => {
		const deps = makeDeps();
		const result = await handleApprovalClick(deps, {
			callbackData: "ea:huh:zzz",
			source: BOUND,
		});
		expect(result.outcome).toBe("invalid");
	});
});

describe("api request_id lane", () => {
	it("targeted resolution touches ONLY the matching entry", () => {
		const deps = makeDeps();
		const a = pending(deps, "req-a", "first");
		const b = pending(deps, "req-b", "second");

		const result = resolveByRequestId(deps, {
			sessionKey: "s",
			requestId: "req-b",
			choice: "once",
			source: BOUND,
		});
		expect(result.count).toBe(1);
		expect(b.result).toBe("once");
		expect(a.result).toBeNull();

		// Unknown id resolves nothing.
		expect(() =>
			resolveByRequestId(deps, {
				sessionKey: "s",
				requestId: "ghost",
				choice: "once",
				source: BOUND,
			}),
		).toThrowError(expect.objectContaining({ code: "no_pending" }));
	});

	it("unauthorized api answers are rejected before any mutation", () => {
		const deps = makeDeps();
		pending(deps, "req-a");
		expect(() =>
			resolveByRequestId(deps, {
				sessionKey: "s",
				requestId: "req-a",
				choice: "once",
				source: STRANGER,
			}),
		).toThrowError(expect.objectContaining({ code: "unauthorized_source" }));
		expect(deps.queues.hasBlocking("s")).toBe(true);
	});
});
