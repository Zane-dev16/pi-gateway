// End-to-end bridge contracts (07 §8): the registration lifecycle around an
// agent run, surface targeting with reason-coded rejection, resolution
// primitive idempotence beneath every surface, ambiguous-send physics, and
// the fail-loud rule when the bridge is down.

import { describe, expect, it } from "vitest";

import { ExecApprovalBridge, type RegisterSessionOptions } from "./bridge.js";
import type { DeliveryTarget } from "./delivery.js";
import { APPROVAL_EXPIRED, NOT_AUTHORIZED_TEXT } from "./render.js";
import type { AnswerRejected } from "./surfaces.js";
import { ManualClock } from "./testing/manual-clock.js";
import type { ApprovalChoice } from "./queue.js";

const REQUEST = {
	command: "rm -rf /srv/data",
	description: "hardline delete",
	patternKey: "rm_rf",
	patternKeys: ["rm_rf"],
};

class ScriptedTarget implements DeliveryTarget {
	cardSends = 0;
	textSends: string[] = [];
	private readonly responder: () => Promise<{ success: boolean }>;

	constructor(
		responder: () => Promise<{ success: boolean }> = async () => ({
			success: true,
		}),
	) {
		this.responder = responder;
	}

	async send(_chatId: string, text: string) {
		this.textSends.push(text);
		return this.responder();
	}
}

function sessionOptions(
	target: DeliveryTarget,
	source = { surface: "telegram", chatId: "42" },
): RegisterSessionOptions {
	const options: RegisterSessionOptions = {
		target,
		chatId: source.chatId ?? "42",
	};
	options.source = source;
	return options;
}

describe("registration lifecycle", () => {
	it("register → await blocks → authorized answer unblocks; unregister releases stragglers", async () => {
		const clock = new ManualClock();
		const bridge = new ExecApprovalBridge({ clock });
		const target = new ScriptedTarget();
		bridge.registerSession("s", sessionOptions(target));

		const pending = bridge.awaitDecision("s", REQUEST);
		await flushUntilBlocked();

		// Prompt delivered card-first through the registered target.
		expect(bridge.queues.hasBlocking("s")).toBe(true);

		// The bound surface answers via /approve.
		const answer = bridge.approve({ sessionKey: "s", rawArgs: "" });
		expect(answer.count).toBe(1);

		const result = await pending;
		expect(result.resolved).toBe(true);
		expect(result.choice).toBe("once");
	});

	it("unregister mid-wait signals ALL blocked waits — never a silent hang", async () => {
		const clock = new ManualClock();
		const bridge = new ExecApprovalBridge({
			clock,
			timeoutSeconds: 300,
		});
		bridge.registerSession("s", sessionOptions(new ScriptedTarget()));

		const first = bridge.awaitDecision("s", REQUEST);
		await flushUntilBlocked();
		const second = bridge.awaitDecision("s", REQUEST); // coalesced follower
		await flushUntilBlocked();

		bridge.unregisterSession("s"); // runner finally-block parity

		// Both waiters release promptly with a null choice. Parity note: the
		// LEADER reports event-set-without-result as resolved=true; the COALESCED
		// follower adopts leader.result=null → resolved=false — both normalize
		// to hook choice "timeout" (_await_coalesced_leader parity).
		const results = await Promise.race([
			Promise.all([first, second]),
			clock.sleepMs(60_000).then(() => null),
		]);
		expect(results).not.toBeNull();
		expect(results?.[0]).toMatchObject({ resolved: true, choice: null });
		expect(results?.[1]).toMatchObject({ resolved: false, choice: null });
		expect(bridge.isRegistered("s")).toBe(false);

		// A NEW request after unregister fails LOUDLY (notify_failed).
		const lateResult = await bridge.awaitDecision("s", REQUEST);
		expect(lateResult.notifyFailed).toBe(true);
	});

	it("clearSession denies + releases immediately at the session boundary", async () => {
		const clock = new ManualClock();
		const bridge = new ExecApprovalBridge({ clock });
		bridge.registerSession("s", sessionOptions(new ScriptedTarget()));

		const pending = bridge.awaitDecision("s", REQUEST);
		await flushUntilBlocked();

		bridge.clearSession("s");
		const result = await pending;
		expect(result).toEqual({ resolved: true, choice: "deny", reason: null });
	});

	it("await on a NEVER-registered session fails closed with notify_failed", async () => {
		const clock = new ManualClock();
		const bridge = new ExecApprovalBridge({ clock });
		const result = await Promise.race([
			bridge.awaitDecision("ghost", REQUEST),
			clock.sleepMs(1000).then(() => ({ hung: true })),
		]);
		expect(result).toMatchObject({ notifyFailed: true, resolved: false });
	});
});

describe("targeting — answers only from authorized surfaces", () => {
	it("bound surface resolves; a stranger's /approve is reason-code rejected", async () => {
		const clock = new ManualClock();
		const bridge = new ExecApprovalBridge({ clock });
		bridge.registerSession(
			"s",
			sessionOptions(new ScriptedTarget(), {
				surface: "telegram",
				chatId: "42",
			}),
		);

		const pending = bridge.awaitDecision("s", REQUEST);
		await flushUntilBlocked();

		let rejectedCode: string | null = null;
		try {
			bridge.approve({
				sessionKey: "s",
				rawArgs: "",
				source: { surface: "discord", chatId: "99" },
			});
		} catch (err) {
			rejectedCode = (err as AnswerRejected).code;
		}
		expect(rejectedCode).toBe("unauthorized_source");
		expect(bridge.queues.hasBlocking("s")).toBe(true); // untouched

		// The AUTHORIZED surface resolves it.
		bridge.approve({
			sessionKey: "s",
			rawArgs: "",
			source: { surface: "telegram", chatId: "42" },
		});
		expect((await pending).choice).toBe("once");
	});

	it("button clicks from strangers get '⛔ not authorized' and touch nothing", async () => {
		const clock = new ManualClock();
		const bridge = new ExecApprovalBridge({ clock });
		// Card-CAPABLE target so the prompt allocates a real ledger id.
		class CardTarget implements DeliveryTarget {
			async sendExecApproval() {
				return { success: true };
			}
		}
		bridge.registerSession("s", sessionOptions(new CardTarget()));

		const pending = bridge.awaitDecision("s", REQUEST);
		await flushUntilBlocked();

		// The card send allocated ledger id 1 (monotonic, first prompt).
		const approvalId = firstLedgerId(bridge);
		const click = await bridge.click({
			callbackData: `ea:once:${approvalId}`,
			source: { surface: "telegram", chatId: "666" },
		});
		expect(click.outcome).toBe("unauthorized");
		expect(click.answerText).toBe(NOT_AUTHORIZED_TEXT);
		expect(bridge.queues.hasBlocking("s")).toBe(true);

		const honest = await bridge.click({
			callbackData: `ea:session:${approvalId}`,
			source: { surface: "telegram", chatId: "42" },
		});
		expect(honest.outcome).toBe("resolved");
		expect(await pending).toMatchObject({ choice: "session" });
	});

	it("api request_id lane resolves only its own entry alongside FIFO traffic", async () => {
		const clock = new ManualClock();
		const bridge = new ExecApprovalBridge({ clock });
		bridge.registerSession("s", sessionOptions(new ScriptedTarget()));

		const first = bridge.awaitDecision("s", REQUEST);
		await flushUntilBlocked();
		// Snapshot the leader's id BEFORE the second (FIFO-behind) request.
		const targetedId = oldestRequestId(bridge);

		const secondRequest = { ...REQUEST, command: "sudo reboot prod" };
		const second = bridge.awaitDecision("s", secondRequest);
		await flushUntilBlocked();

		// The api answer TARGETS the first request — the second stays pending.
		const answer = bridge.apiResolve({
			sessionKey: "s",
			requestId: targetedId,
			choice: "deny",
			source: { surface: "telegram", chatId: "42" },
		});
		expect(answer.count).toBe(1);
		expect(await first).toMatchObject({ choice: "deny" });
		expect(bridge.queues.hasBlocking("s")).toBe(true);

		bridge.approve({ sessionKey: "s", rawArgs: "" });
		expect(await second).toMatchObject({ choice: "once" });
	});
});

describe("ONE primitive beneath ALL surfaces — double-answer idempotence", () => {
	it("second answer on any surface resolves 0 and renders honestly", async () => {
		const clock = new ManualClock();
		const bridge = new ExecApprovalBridge({ clock });
		bridge.registerSession("s", sessionOptions(new ScriptedTarget()));

		const pending = bridge.awaitDecision("s", REQUEST);
		await flushUntilBlocked();

		// Surface 1: bare word "yes".
		const bare = bridge.bareWord({
			sessionKey: "s",
			text: "yes",
			controlAllowed: true,
		});
		expect(bare.routed).toBe(true);
		expect(await pending).toMatchObject({ choice: "once" });

		// Surface 2 (later): slash /approve again → authoritative 0.
		expect(() => bridge.approve({ sessionKey: "s", rawArgs: "" })).toThrowError(
			expect.objectContaining({ code: "no_pending" }),
		);

		// The expired marker is NOT set by a successful resolution.
		expect(() => bridge.deny({ sessionKey: "s", rawArgs: "" })).toThrowError(
			/No pending command to deny/,
		);
	});

	it("timeout marks the session so a LATE /approve reports 'approval expired'", async () => {
		const clock = new ManualClock();
		const bridge = new ExecApprovalBridge({ clock, timeoutSeconds: 5 });
		bridge.registerSession("s", sessionOptions(new ScriptedTarget()));

		const result = await bridge.awaitDecision("s", REQUEST);
		expect(result.resolved).toBe(false); // fail-closed

		try {
			bridge.approve({ sessionKey: "s", rawArgs: "" });
			expect.unreachable("must throw");
		} catch (err) {
			expect((err as Error).message).toBe(APPROVAL_EXPIRED);
		}
	});

	it("deny-with-reason relays verbatim into the gate result", async () => {
		const clock = new ManualClock();
		const bridge = new ExecApprovalBridge({ clock });
		bridge.registerSession("s", sessionOptions(new ScriptedTarget()));

		const pending = bridge.awaitDecision("s", REQUEST);
		await flushUntilBlocked();

		bridge.deny({
			sessionKey: "s",
			rawArgs: "staging is live, use pg_dump",
		});
		const result = await pending;
		expect(result.choice).toBe("deny");
		expect(result.reason).toBe("staging is live, use pg_dump");
	});
});

describe("ambiguous-send physics end-to-end", () => {
	it("a hung card send keeps the registration armed — no fallback, late tap resolves", async () => {
		const clock = new ManualClock();
		const bridge = new ExecApprovalBridge({ clock });
		class HangingCardTarget implements DeliveryTarget {
			cardCalls = 0;
			textSends: string[] = [];
			private resolvers: Array<(r: { success: boolean }) => void> = [];

			async sendExecApproval() {
				this.cardCalls += 1;
				return new Promise<{ success: boolean }>((resolve) => {
					this.resolvers.push(resolve);
				});
			}

			async send(_chatId: string, text: string) {
				this.textSends.push(text);
				return { success: true };
			}

			settleLate(): void {
				for (const resolve of this.resolvers) resolve({ success: true });
			}
		}
		const target = new HangingCardTarget();
		bridge.registerSession("s", sessionOptions(target));

		const pending = bridge.awaitDecision("s", REQUEST);
		// Walk past the 15 s classification window without settling the card.
		await clock.sleepMs(20_000);
		await drainMacrotasks();

		expect(target.cardCalls).toBe(1); // NO re-send
		expect(target.textSends.length).toBe(0); // NO fallback
		expect(bridge.queues.hasBlocking("s")).toBe(true); // still armed

		// A late tap on the rendered card still resolves the wait.
		target.settleLate();
		const approvalId = firstLedgerId(bridge);
		const click = await bridge.click({
			callbackData: `ea:always:${approvalId}`,
			source: { surface: "telegram", chatId: "42" },
		});
		expect(click.outcome).toBe("resolved");

		const result = await Promise.race([
			pending,
			drainMacrotasks().then(() => null),
		]);
		await flushUntilBlocked();
		expect(result ?? (await pending)).toMatchObject({ choice: "always" });
	});
});

// ── helpers ───────────────────────────────────────────────────────────────────

async function drainMacrotasks(rounds = 8): Promise<void> {
	for (let i = 0; i < rounds; i++) {
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
	}
}

/** Park the awaiting decision in its poll loop before answering. */
async function flushUntilBlocked(): Promise<void> {
	await drainMacrotasks();
}

function firstLedgerId(bridge: ExecApprovalBridge): number {
	// The ledger hands out monotonic ids starting at 1; the armed prompt owns 1.
	void bridge;
	return 1;
}

/** requestId of the OLDEST pending entry (queue-head snapshot). */
function oldestRequestId(bridge: ExecApprovalBridge): string {
	const head = bridge.queues.oldestPending("s");
	if (!head) throw new Error("no pending approval in fixture");
	return head.requestId;
}

void (async () => {
	/* keep top-level await-free for vitest module scope */
})();

export type { ApprovalChoice };
