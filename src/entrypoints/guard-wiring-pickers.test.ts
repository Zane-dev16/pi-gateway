// entrypoints/guard-wiring-pickers.test — composition contracts for picker
// wiring at the guard seam. Fake runner plus fake adapter slots: every test
// drives the production messageHandler/attach helpers and asserts effects
// (consumed text never turns, chat ids flow, predicates forward).

import { describe, expect, it } from "vitest";
import {
	buildProductionMessageHandler,
	hasPickerSenderSurface,
	tryAttachPickerHooks,
	tryAttachProductionGuard,
} from "./guard-wiring.js";
import type { IncomingEvent } from "../pi_gateway/guards/index.js";

function textEvent(text: string, chatId = "42"): IncomingEvent {
	return {
		messageType: "text",
		text,
		metadata: { gateway_session_key: "sess-1" },
		source: {
			platform: "telegram",
			userId: "7001",
			chatId,
			chatType: "dm",
		},
	};
}

describe("production messageHandler picker consumption", () => {
	it("consumed picker text returns the reply and never turns", async () => {
		let turns = 0;
		const handler = buildProductionMessageHandler({
			runner: {
				handleTurn: async () => {
					turns += 1;
					return {
						exitReason: "finalized" as const,
						finalText: "turn",
						iterations: 1,
						repairs: 0,
						userRowId: null,
						assistantRowId: null,
						usage: null,
					};
				},
			},
			isAuthorized: () => ({
				allowed: true,
				platform: "telegram",
				userId: "7001",
				chatId: "42",
				reasonCode: "test",
				gate: 7,
			}),
			consumePendingText: (sessionKey, text) =>
				sessionKey === "sess-1" && text === "2" ? "✓ Green" : null,
		});
		const reply = await handler(textEvent("2"), { sessionKey: "sess-1" } as never);
		expect(reply).toBe("✓ Green");
		expect(turns).toBe(0);
	});

	it("unconsumed text turns with the event chat id attached", async () => {
		const seen: Array<Record<string, unknown>> = [];
		const handler = buildProductionMessageHandler({
			runner: {
				handleTurn: async (request) => {
					seen.push({ ...request });
					return {
						exitReason: "finalized" as const,
						finalText: "turn",
						iterations: 1,
						repairs: 0,
						userRowId: null,
						assistantRowId: null,
						usage: null,
					};
				},
			},
			isAuthorized: () => ({
				allowed: true,
				platform: "telegram",
				userId: "7001",
				chatId: "42",
				reasonCode: "test",
				gate: 7,
			}),
			consumePendingText: () => null,
		});
		await handler(textEvent("hello"), { sessionKey: "sess-1" } as never);
		expect(seen).toEqual([
			{ sessionId: "sess-1", routingKey: "sess-1", text: "hello", chatId: "42" },
		]);
	});
});

describe("guard attach picker options", () => {
	it("hasPendingClarify forwards into the guard slot", () => {
		const captured: Array<Record<string, unknown>> = [];
		const adapter = {
			attachGuard: (
				_deps: unknown,
				opts: { hasPendingClarify?: (key: string) => boolean },
			) => {
				captured.push({ hasPredicate: typeof opts.hasPendingClarify === "function" });
			},
		};
		const ok = tryAttachProductionGuard(
			adapter,
			{ registry: [], messageHandler: async () => null, sendReply: async () => {} },
			{ hasPendingClarify: (key) => key === "sess-1" },
		);
		expect(ok).toBe(true);
		expect(captured).toEqual([{ hasPredicate: true }]);
	});

	it("picker hooks attach only on picker slots", () => {
		const calls: string[] = [];
		const telegramLike = {
			setPickerHooks: (_hooks: unknown) => {
				calls.push("hooked");
			},
		};
		const hooks = {
			choiceTap: () => ({ status: "stale" as const }),
			confirmTap: () => ({ label: null }),
			clarifyChoice: () => ({ status: "stale" as const }),
			clarifyOther: () => ({ status: "stale" as const }),
			modelTap: async () => ({ answer: "x", stale: true as const }),
		};
		expect(tryAttachPickerHooks(telegramLike, hooks)).toBe(true);
		expect(calls).toEqual(["hooked"]);
		expect(tryAttachPickerHooks({}, hooks)).toBe(false);
	});

	it("sender surface detection keys off the keyboard senders", () => {
		const sender = {
			sendChoicePicker: async () => ({}),
			sendSlashConfirm: async () => ({}),
			sendClarify: async () => ({}),
			sendModelPicker: async () => ({}),
			sendPickerText: async () => ({}),
		};
		expect(hasPickerSenderSurface(sender)).toBe(true);
		expect(hasPickerSenderSurface({ send: async () => ({}) })).toBe(false);
		expect(hasPickerSenderSurface(null)).toBe(false);
	});
});
