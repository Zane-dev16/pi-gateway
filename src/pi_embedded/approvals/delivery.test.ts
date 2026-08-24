// Behavior contracts for card-first/text-fallback delivery (07 §8.3) and the
// byte-stable fallback renderer. The boundary rule is binding: an ambiguous
// send is NEVER re-sent and NEVER falls back; only a DEFINITIVE failure re-asks.

import { describe, expect, it } from "vitest";

import {
	ApprovalCardLedger,
	DeliveryBridge,
	classifySendOutcome,
	hasExecApprovalCard,
	type DeliveryTarget,
	type ExecApprovalSendArgs,
} from "./delivery.js";
import {
	buildApprovalKeyboard,
	execApprovalCallbackData,
	formatExecApprovalFallback,
	parseApprovalCallbackData,
	clickOutcomeLabel,
} from "./render.js";
import { redactApprovalCommand } from "./redact.js";
import { ManualClock } from "./testing/manual-clock.js";

class CardAdapter implements DeliveryTarget {
	cardCalls: ExecApprovalSendArgs[] = [];
	textCalls: Array<{ chatId: string; text: string }> = [];
	// Scripted by test:
	private readonly responder: (args: ExecApprovalSendArgs) => Promise<{
		success: boolean;
		error?: string;
	}>;

	constructor(
		responder: (args: ExecApprovalSendArgs) => Promise<{
			success: boolean;
			error?: string;
		}>,
	) {
		this.responder = responder;
	}

	async sendExecApproval(args: ExecApprovalSendArgs) {
		this.cardCalls.push(args);
		return this.responder(args);
	}

	async send(chatId: string, text: string) {
		this.textCalls.push({ chatId, text });
		return { success: true };
	}
}

class TextOnlyAdapter implements DeliveryTarget {
	textCalls: string[] = [];

	async send(_chatId: string, text: string) {
		this.textCalls.push(text);
		return { success: true };
	}
}

const REQUEST = {
	sessionKey: "telegram:chat:42",
	command: "rm -rf /srv/data",
	description: "hardline delete",
	allowPermanent: true,
	allowSession: true,
	smartDenied: false,
};

describe("hasExecApprovalCard — CLASS-level probe", () => {
	it("true when the PROTOTYPE declares sendExecApproval", () => {
		expect(
			hasExecApprovalCard(new CardAdapter(async () => ({ success: true }))),
		).toBe(true);
	});

	it("false for instance-assigned functions (MagicMock false-positive guard)", () => {
		const fake = new TextOnlyAdapter() as unknown as Record<string, unknown>;
		fake["sendExecApproval"] = async () => ({ success: true }); // own property
		expect(hasExecApprovalCard(fake)).toBe(false);
	});

	it("false when the adapter has no card method at all", () => {
		expect(hasExecApprovalCard(new TextOnlyAdapter())).toBe(false);
	});
});

describe("classifySendOutcome — sent / ambiguous / failed", () => {
	it("success result ⇒ sent; error result ⇒ failed; null future ⇒ failed", async () => {
		const clock = new ManualClock();
		await expect(
			classifySendOutcome(Promise.resolve({ success: true }), clock, 15_000),
		).resolves.toBe("sent");
		await expect(
			classifySendOutcome(
				Promise.resolve({ success: false, error: "boom" }),
				clock,
				15_000,
			),
		).resolves.toBe("failed");
		await expect(classifySendOutcome(null, clock, 15_000)).resolves.toBe(
			"failed",
		);
	});

	it("rejection ⇒ failed (with detail logged)", async () => {
		const clock = new ManualClock();
		const warnings: string[] = [];
		const outcome = await classifySendOutcome(
			Promise.reject(new Error("connector died")),
			clock,
			15_000,
			{ warn: (m) => warnings.push(m) },
		);
		expect(outcome).toBe("failed");
		expect(warnings[0]).toContain("connector died");
	});

	it("unsettled future at the deadline ⇒ ambiguous", async () => {
		const clock = new ManualClock();
		let resolveLater: (r: { success: boolean }) => void = () => {};
		const future = new Promise<{ success: boolean }>((resolve) => {
			resolveLater = resolve;
		});
		const pending = classifySendOutcome(future, clock, 15_000);
		void pending.then(() => resolveLater({ success: true })); // late ack shadow
		await expect(pending).resolves.toBe("ambiguous");
	});
});

describe("DeliveryBridge — card first, text fallback, boundary rule", () => {
	it("successful card send ⇒ NO text fallback, ledger binds id → session", async () => {
		const clock = new ManualClock();
		const ledger = new ApprovalCardLedger();
		const adapter = new CardAdapter(async () => ({ success: true }));
		const bridge = new DeliveryBridge({
			target: adapter,
			chatId: "42",
			ledger,
			clock,
		});

		await bridge.notify(REQUEST);

		expect(adapter.cardCalls.length).toBe(1);
		expect(adapter.textCalls.length).toBe(0);
		const args = adapter.cardCalls[0];
		expect(args?.chatId).toBe("42");
		expect(args?.sessionKey).toBe(REQUEST.sessionKey);
		expect(args?.approvalId).toBeGreaterThan(0);
		expect(ledger.peek(args?.approvalId ?? -1)).toBe(REQUEST.sessionKey);
	});

	it("AMBIGUOUS card send ⇒ no re-send, no fallback; registration stays armed", async () => {
		const clock = new ManualClock();
		const ledger = new ApprovalCardLedger();
		let settleCard: ((r: { success: boolean }) => void) | undefined;
		const adapter = new CardAdapter(
			() =>
				new Promise<{ success: boolean }>((resolve) => {
					settleCard = resolve; // never settles before the deadline
				}),
		);
		const warnings: string[] = [];
		const bridge = new DeliveryBridge({
			target: adapter,
			chatId: "42",
			ledger,
			clock,
			log: { warn: (m) => warnings.push(m) },
		});

		await bridge.notify(REQUEST);

		// Exactly ONE card attempt; the possibly-posted prompt stays armed.
		expect(adapter.cardCalls.length).toBe(1);
		expect(adapter.textCalls.length).toBe(0);
		expect(warnings.join("\n")).toContain("possibly-delivered");
		expect(settleCard).toBeDefined(); // late ack would still land safely
	});

	it("DEFINITIVE card failure ⇒ exactly one byte-stable text fallback", async () => {
		const clock = new ManualClock();
		const ledger = new ApprovalCardLedger();
		const adapter = new CardAdapter(async () => ({
			success: false,
			error: "Not connected",
		}));
		const bridge = new DeliveryBridge({
			target: adapter,
			chatId: "42",
			ledger,
			clock,
		});

		await bridge.notify({ ...REQUEST, allowPermanent: false });

		expect(adapter.cardCalls.length).toBe(1);
		expect(adapter.textCalls.length).toBe(1);
		expect(adapter.textCalls[0]?.text).toBe(
			formatExecApprovalFallback({
				command: REQUEST.command,
				description: REQUEST.description,
				commandPrefix: "/",
				allowPermanent: false,
				allowSession: true,
				smartDenied: false,
			}),
		);
	});

	it("card-capability miss goes STRAIGHT to text (no probe delay)", async () => {
		const clock = new ManualClock();
		const ledger = new ApprovalCardLedger();
		const adapter = new TextOnlyAdapter();
		const bridge = new DeliveryBridge({
			target: adapter,
			chatId: "42",
			ledger,
			clock,
		});

		await bridge.notify(REQUEST);

		expect(adapter.textCalls.length).toBe(1);
		expect(adapter.textCalls[0]).toContain("/approve");
	});

	it("text-fallback ALSO failing leaves the wait armed (typed answers can still resolve)", async () => {
		const clock = new ManualClock();
		const ledger = new ApprovalCardLedger();
		const logs: string[] = [];
		const failingText = new TextOnlyAdapter();
		failingText.send = async () => {
			throw new Error("flood-wait exhausted");
		};
		const bridge = new DeliveryBridge({
			target: failingText,
			chatId: "42",
			ledger,
			clock,
			log: {
				warn: (m) => logs.push(m),
				error: (m) => logs.push(m),
			},
		});

		// notify NEVER throws — a failed prompt must not crash the agent thread.
		await expect(bridge.notify(REQUEST)).resolves.toBeUndefined();
		expect(logs.join("\n")).toContain("flood-wait exhausted");
	});

	it("typing pauses BEFORE any render path runs", async () => {
		const clock = new ManualClock();
		const ledger = new ApprovalCardLedger();
		const order: string[] = [];
		const adapter = new TextOnlyAdapter();
		adapter.send = async (_chat, text) => {
			order.push(`send:${text.slice(0, 10)}`);
			return { success: true };
		};
		const bridge = new DeliveryBridge({
			target: adapter,
			chatId: "42",
			ledger,
			clock,
			pauseTyping: () => order.push("pause"),
		});

		await bridge.notify(REQUEST);
		expect(order[0]).toBe("pause");
	});

	it("redaction runs ONCE through the shared point feeding BOTH paths (#48456)", async () => {
		const clock = new ManualClock();
		const ledger = new ApprovalCardLedger();
		const secretCommand =
			"curl -H 'Authorization: Bearer sk-abcdefghijklmnopqrstuvwx' https://x";
		const seen: string[] = [];
		let redactCalls = 0;

		const adapter = new CardAdapter(async () => ({ success: false }));
		const bridge = new DeliveryBridge({
			target: adapter,
			chatId: "42",
			ledger,
			clock,
			redact: (cmd) => {
				redactCalls += 1;
				return cmd.replace(/sk-[A-Za-z0-9]+/, "[REDACTED]");
			},
		});
		await bridge.notify({ ...REQUEST, command: secretCommand });
		expect(redactCalls).toBe(1);
		expect(adapter.cardCalls[0]?.command).not.toContain("sk-");
		seen.push(adapter.cardCalls[0]?.command ?? "");

		const textOnly = new TextOnlyAdapter();
		const bridge2 = new DeliveryBridge({
			target: textOnly,
			chatId: "42",
			ledger,
			clock,
			redact: (cmd) => {
				redactCalls += 1;
				return cmd.replace(/sk-[A-Za-z0-9]+/, "[REDACTED]");
			},
		});
		await bridge2.notify({ ...REQUEST, command: secretCommand });
		expect(textOnly.textCalls[0]).not.toContain("sk-");
		seen.push(textOnly.textCalls[0] ?? "");
		expect(seen.every((s) => s.includes("[REDACTED]"))).toBe(true);
	});
});

describe("formatExecApprovalFallback — byte-stable template", () => {
	it("matches the reference template exactly (default flags, '/' prefix)", () => {
		expect(
			formatExecApprovalFallback({
				command: "rm -rf /srv/data",
				description: "hardline delete",
				commandPrefix: "/",
			}),
		).toBe(
			"⚠️ **Dangerous command requires approval:**\n" +
				"```\nrm -rf /srv/data\n```\n" +
				"Reason: hardline delete\n\n" +
				"Reply `/approve` to execute this one operation, " +
				"`/approve session` to approve this pattern for the session, " +
				"`/approve always` to approve permanently, or `/deny` to cancel.",
		);
	});

	it("'!' prefix swaps the verb form for Slack/Matrix-class surfaces", () => {
		const rendered = formatExecApprovalFallback({
			command: "kubectl delete ns prod",
			description: "cluster mutation",
			commandPrefix: "!",
		});
		expect(rendered).toContain("`!approve`");
		expect(rendered).toContain("`!deny` to cancel.");
		expect(rendered).not.toContain("`/approve`");
	});

	it("smart_denied swaps the heading AND drops session/always choices", () => {
		const rendered = formatExecApprovalFallback({
			command: "git push --force origin main",
			description: "force push",
			commandPrefix: "/",
			smartDenied: true,
		});
		expect(
			rendered.startsWith(
				"⚠️ **Smart DENY — owner override for one operation:**",
			),
		).toBe(true);
		expect(rendered).not.toContain("approve session");
		expect(rendered).not.toContain("approve always");
		expect(rendered).toContain(", or `/deny` to cancel.");
	});

	it("allow_session=false omits both persistence choices", () => {
		const rendered = formatExecApprovalFallback({
			command: "cmd",
			description: "d",
			commandPrefix: "/",
			allowSession: false,
		});
		expect(rendered).not.toContain("approve session");
		expect(rendered).not.toContain("approve always");
	});

	it("commands over 200 CODE POINTS truncate with an ellipsis", () => {
		const long = "x".repeat(250);
		const rendered = formatExecApprovalFallback({
			command: long,
			description: "d",
			commandPrefix: "/",
		});
		expect(rendered).toContain(`${"x".repeat(200)}...\n`);
		// Code-point parity: a 250-astral-char command truncates at 200 chars.
		const astral = "😀".repeat(250);
		const renderedAstral = formatExecApprovalFallback({
			command: astral,
			description: "d",
			commandPrefix: "/",
		});
		expect(renderedAstral).toContain(`${"😀".repeat(200)}...`);
	});
});

describe("card grammar + keyboard (DEC-016 ea: prefix)", () => {
	it("callback data round-trips through the parser", () => {
		for (const choice of ["once", "session", "always", "deny"] as const) {
			const data = execApprovalCallbackData(choice, 17);
			expect(data).toBe(`ea:${choice}:17`);
			expect(parseApprovalCallbackData(data)).toEqual({
				choice,
				approvalId: 17,
			});
		}
	});

	it("foreign prefixes / malformed ids parse to null", () => {
		expect(parseApprovalCallbackData("sc:once:17")).toBeNull();
		expect(parseApprovalCallbackData("ea:bogus:17")).toBeNull();
		expect(parseApprovalCallbackData("ea:once:notanumber")).toBeNull();
		expect(parseApprovalCallbackData("ea:once")).toBeNull();
		expect(parseApprovalCallbackData("")).toBeNull();
	});

	it("full button set pairs into rows of two (mobile truncation guard)", () => {
		const rows = buildApprovalKeyboard({ approvalId: 5 });
		expect(rows.map((row) => row.map((b) => b.label))).toEqual([
			["✅ Allow Once", "✅ Session"],
			["✅ Always", "❌ Deny"],
		]);
	});

	it("flag gating mirrors the reference set", () => {
		const noPermanent = buildApprovalKeyboard({
			approvalId: 5,
			allowPermanent: false,
		});
		expect(noPermanent.map((row) => row.map((b) => b.label))).toEqual([
			["✅ Allow Once", "✅ Session"],
			["❌ Deny"],
		]);

		const smartDenied = buildApprovalKeyboard({
			approvalId: 5,
			smartDenied: true,
		});
		// Remaining pair shares one row ([b[i:i+2]] pairing parity).
		expect(smartDenied.map((row) => row.map((b) => b.label))).toEqual([
			["✅ Allow Once", "❌ Deny"],
		]);

		const noSession = buildApprovalKeyboard({
			approvalId: 5,
			allowSession: false,
		});
		expect(noSession.map((row) => row.map((b) => b.label))).toEqual([
			["✅ Allow Once", "❌ Deny"],
		]);
	});
});

describe("click outcome labels + built-in redaction pass", () => {
	it("labels derive from the ACTUAL resolved count — stale taps render 'expired'", () => {
		expect(clickOutcomeLabel("once", 1)).toBe("✅ Approved once");
		expect(clickOutcomeLabel("session", 1)).toBe("✅ Approved for session");
		expect(clickOutcomeLabel("always", 1)).toBe("✅ Approved permanently");
		expect(clickOutcomeLabel("deny", 1)).toBe("❌ Denied");
		// #63501: count=0 must NEVER claim approval for a command that won't run.
		expect(clickOutcomeLabel("once", 0)).toBe("⌛ Approval expired");
		expect(clickOutcomeLabel("always", 0)).toBe("⌛ Approval expired");
	});

	it("built-in gateway secret patterns redact without a primary scrubber", () => {
		expect(redactApprovalCommand("deploy sk-abcdefgh1234567890 now")).toBe(
			"deploy [REDACTED] now",
		);
		expect(
			redactApprovalCommand(
				"curl -H 'Authorization: Bearer abcdefghijklmnopqrstuvwxyz'",
			),
		).toContain("Bearer [REDACTED]");
		expect(redactApprovalCommand("safe command")).toBe("safe command");
	});
});
