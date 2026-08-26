// DM plaintext gateway-command coercion (base.py:coerce_plaintext_gateway_command
// + _PLAINTEXT_GATEWAY_RESTART_PATTERNS): exact restart-style phrases rewrite to
// "/restart" AT INTAKE — before any command classification — so a self-restart
// ask can never ride the LLM path inside a running turn and wedge the gateway
// in draining. Scope is intentionally narrow: TEXT events, non-slash text,
// chat_type === "dm" STRICTLY, control-enabled events only.

import { describe, expect, it } from "vitest";
import type { IncomingEvent } from "./events.js";
import {
	allowsGatewayControl,
	coercePlaintextGatewayCommand,
	PLAINTEXT_GATEWAY_RESTART_PATTERNS,
} from "./events.js";
import {
	AdapterSessionGuard,
	type AdapterGuardDeps,
} from "./l1-adapter-guard.js";

function dmText(
	text: string,
	extra: Partial<IncomingEvent> = {},
): IncomingEvent {
	return {
		messageType: "text",
		text,
		source: { platform: "telegram", chatType: "dm", userId: "u1" },
		...extra,
	};
}

describe("pattern matrix (exact phrases only; anchored both ends)", () => {
	it.each([
		"restart gateway",
		"restart the gateway",
		"please restart gateway",
		"please restart the gateway",
		"RESTART THE GATEWAY.",
		"Restart Gateway!",
		"restart gateway?   ",
		"restart hermes",
		"please restart hermes.",
		"restart hermes gateway",
		"please restart the hermes gateway?",
		"  restart the gateway  ",
	])("coerces %j", (phrase) => {
		const event = dmText(phrase);
		coercePlaintextGatewayCommand(event);
		expect(event.text).toBe("/restart");
	});

	it.each([
		"restart the gateway now",
		"can you restart the gateway",
		"gateway restart",
		"restart gateway please",
		"please restart the gateway right now!",
		"restart the production gateway",
		"/restart the gateway",
		"",
		"   ",
	])("does NOT coerce %j", (phrase) => {
		const event = dmText(phrase);
		const before = event.text;
		coercePlaintextGatewayCommand(event);
		expect(event.text).toBe(before);
	});
});

describe("scope guards (base.py narrowing)", () => {
	it("rewrites IN PLACE only for chat_type === 'dm' (strict)", () => {
		for (const chatType of [
			"group",
			"channel",
			"thread",
			"private",
			"direct",
		]) {
			const event = dmText("restart the gateway");
			if (event.source) event.source.chatType = chatType;
			coercePlaintextGatewayCommand(event);
			expect(event.text).toBe("restart the gateway"); // untouched
		}
	});

	it("missing source is not a dm → untouched", () => {
		const event: IncomingEvent = {
			messageType: "text",
			text: "restart gateway",
		};
		coercePlaintextGatewayCommand(event);
		expect(event.text).toBe("restart gateway");
	});

	it("non-TEXT events are never touched", () => {
		const event: IncomingEvent = {
			messageType: "photo",
			text: "restart gateway",
			mediaUrls: ["/x.png"],
			source: { platform: "telegram", chatType: "dm" },
		};
		coercePlaintextGatewayCommand(event);
		expect(event.text).toBe("restart gateway");
	});

	it("already-slash text passes through unmodified", () => {
		const event = dmText("/restart");
		coercePlaintextGatewayCommand(event);
		expect(event.text).toBe("/restart");
	});

	it("the pattern table carries exactly the three Hermes regexes", () => {
		expect(PLAINTEXT_GATEWAY_RESTART_PATTERNS.length).toBe(3);
	});
});

describe("intake wiring: L1 handleMessage coerces BEFORE classification", () => {
	function guardWithRestart(): {
		guard: AdapterSessionGuard;
		turns: string[];
		replies: string[];
		deps: AdapterGuardDeps;
		KEY: string;
	} {
		const KEY = "agent:main:telegram:dm:100";
		const turns: string[] = [];
		const replies: string[] = [];
		const deps: AdapterGuardDeps = {
			messageHandler: async (event) => {
				turns.push(event.text ?? "");
				return `reply:${event.text ?? ""}`;
			},
			sendReply: async (_chatId, text) => {
				replies.push(text);
			},
			registry: [
				// /restart registered like the real row: dispatches mid-run.
				{ name: "restart", busyPolicy: "dispatch" },
				{
					name: "stop",
					busyPolicy: "interrupt_then_dispatch",
					busyHandler: "stop",
				},
			],
		};
		return { guard: new AdapterSessionGuard(deps), turns, replies, deps, KEY };
	}

	it("busy session: the phrase takes the COMMAND bypass lane as /restart — never queued as agent text", async () => {
		const f = guardWithRestart();
		await f.guard.handleMessage(dmText("head"), f.KEY);
		expect(f.guard.isActive(f.KEY)).toBe(true);

		await f.guard.handleMessage(dmText("please restart the gateway"), f.KEY);

		// The coerced text dispatched INLINE as a recognized command; nothing
		// merged into pending.
		expect(f.turns).toContain("/restart");
		expect(f.guard.pendingOf(f.KEY)).toBeUndefined();
		expect(f.replies).toContain("reply:/restart");
	});

	it("allowGatewayControl:false keeps natural-language semantics (no coercion)", async () => {
		const f = guardWithRestart();
		await f.guard.handleMessage(dmText("head"), f.KEY);

		await f.guard.handleMessage(
			dmText("restart the gateway", { allowGatewayControl: false }),
			f.KEY,
		);
		// Not a command → busy ladder: queued/merged as plain agent text.
		expect(f.turns).toEqual(["head"]);
		expect(f.guard.pendingOf(f.KEY)?.text).toBe("restart the gateway");
		expect(
			allowsGatewayControl(dmText("x", { allowGatewayControl: false })),
		).toBe(false);
	});

	it("group traffic is never coerced", async () => {
		const f = guardWithRestart();
		await f.guard.handleMessage(dmText("head"), f.KEY);
		await f.guard.handleMessage(
			dmText("restart the gateway", {
				source: { platform: "telegram", chatType: "group", userId: "u1" },
			}),
			f.KEY,
		);
		expect(f.turns).toEqual(["head"]);
		expect(f.guard.pendingOf(f.KEY)?.text).toBe("restart the gateway");
	});
});
