// pi_platforms/telegram/telegram-pickers.test — picker behavior contracts
// over the fake Bot API server. Every test drives the REAL adapter plus the
// REAL picker service the way chat does: service ask → sendMessage keyboard
// → callback_query tap → answerCallbackQuery + editMessageText. Assertions
// are wire bytes (callback_data literals, 64-byte cap, answer-every-tap)
// plus service outcomes — never source shapes.

import { describe, expect, it } from "vitest";
import { ManualPollingClock } from "../polling/clock.js";
import { PickerService } from "../../pi_gateway/pickers/service.js";
import { TelegramAdapter } from "./telegram-adapter.js";
import { TelegramBotApiFake } from "./telegram-fake-server.js";
import { CALLBACK_DATA_MAX_BYTES } from "../kit/index.js";

function makeHookedWorld(): {
	adapter: TelegramAdapter;
	tg: TelegramBotApiFake;
	service: PickerService;
	responses: Array<{ uiId: string; body: Record<string, unknown> }>;
} {
	const tg = new TelegramBotApiFake();
	const adapter = new TelegramAdapter({
		wire: tg,
		clock: new ManualPollingClock(),
		secretReader: () => "tok",
	});
	adapter.setClickerAuthorization(true);
	// Subject-level binding (telegram-subject parity): the send path lands
	// on the fake Bot API with its FULL wire arg set (text, parse_mode,
	// reply_markup) so keyboards are byte-assertable.
	adapter.wireTransmitSend = async (chatId, content, metadata) =>
		bindSend(tg, chatId, content, metadata);
	const responses: Array<{ uiId: string; body: Record<string, unknown> }> = [];
	// The adapter satisfies the sender seam structurally (keyboard senders
	// plus sendPickerText through DOOR 1); taps resolve back through hooks.
	let service: PickerService;
	const hooks = {
		choiceTap: (id: number, index: number) => service.choiceTap(id, index),
		choicePageTap: (id: number, page: number) => service.choicePageTap(id, page),
		confirmTap: (id: number, choice: "once" | "always" | "cancel") =>
			service.confirmTap(id, choice),
		clarifyChoice: (id: number, idx: number) => service.clarifyTap(id, idx),
		clarifyOther: (id: number) => service.clarifyTap(id, "other"),
		modelTap: async () => ({
			answer: "This picker has expired — run /model again.",
			stale: true as const,
		}),
	};
	adapter.setPickerHooks(hooks);
	service = new PickerService({
		sender: adapter,
	});
	return { adapter, tg, service, responses };
}

async function bindSend(
	tg: TelegramBotApiFake,
	chatId: string,
	content: string,
	metadata: Record<string, unknown>,
): Promise<{ success: true; messageId: string }> {
	const message = await tg.sendMessage({
		chat_id: chatId,
		text: content,
		...(typeof metadata["parse_mode"] === "string" ? { parse_mode: metadata["parse_mode"] } : {}),
		...(metadata["reply_markup"] !== undefined ? { reply_markup: metadata["reply_markup"] } : {}),
	});
	return { success: true, messageId: String(message.message_id) };
}

function keyboardData(tg: TelegramBotApiFake, sendIndex: number): string[] {
	const kwargs = tg.sendKwargs[sendIndex] as { reply_markup?: unknown };
	const markup = kwargs.reply_markup as {
		inline_keyboard: Array<Array<{ text: string; callback_data: string }>>;
	};
	return markup.inline_keyboard.flatMap((row) => row.map((button) => button.callback_data));
}

function editKeyboardData(tg: TelegramBotApiFake, editIndex: number): string[] {
	const kwargs = tg.editKwargs[editIndex] as { reply_markup?: unknown };
	const markup = kwargs.reply_markup as {
		inline_keyboard: Array<Array<{ text: string; callback_data: string }>>;
	};
	return markup.inline_keyboard.flatMap((row) => row.map((button) => button.callback_data));
}

async function flushSends(tg: TelegramBotApiFake, count: number): Promise<void> {
	for (let i = 0; i < 50 && tg.sendKwargs.length < count; i += 1) {
		await new Promise((resolve) => setImmediate(resolve));
	}
}

async function tap(
	adapter: TelegramAdapter,
	tg: TelegramBotApiFake,
	opts: { hostChatId: number; hostMessageId: string | number; data: string },
): Promise<void> {
	const update = tg.pushCallbackUpdate({
		hostChatId: opts.hostChatId,
		hostMessageId: opts.hostMessageId,
		data: opts.data,
	});
	await adapter.handleIngress(
		{ messageId: String(update.update_id), messageType: "text" },
		"tg:7001",
	);
}

describe("choice picker round-trip over the fake server", () => {
	it("send → tap → resolve → strip; second tap gets expiry, never a second resolve", async () => {
		const { adapter, tg, service, responses } = makeHookedWorld();
		const asked = service.askSelect({
			chatId: "42",
			sessionKey: "sess-1",
			uiId: "ui-1",
			title: "Pick a color",
			options: ["Red", "Green"],
			respond: async (uiId, body) => {
				responses.push({ uiId, body });
			},
		});
		await flushSends(tg, 1);
		// Byte-exact keyboard: cp:<i> payloads under the 64-byte cap.
		const data = keyboardData(tg, 0);
		expect(data).toEqual(["cp:0", "cp:1"]);
		for (const payload of data) {
			expect(Buffer.byteLength(payload, "utf8")).toBeLessThanOrEqual(CALLBACK_DATA_MAX_BYTES);
		}
		const hostMessageId = tg.sentMessages[0]?.message_id ?? 1;

		await tap(adapter, tg, { hostChatId: 42, hostMessageId, data: "cp:1" });
		expect(await asked).toEqual({ kind: "value", value: "Green" });
		expect(responses).toEqual([{ uiId: "ui-1", body: { value: "Green" } }]);
		// Spinner cleared plus host message stripped to the resolution.
		expect(tg.callbackAnswers.length).toBe(1);
		expect(tg.callbackAnswers[0]?.text).toContain("Green");
		expect(tg.editKwargs.length).toBe(1);
		expect(tg.editKwargs[0]).toMatchObject({ reply_markup: null });
		expect(String((tg.editKwargs[0] as { text: string }).text)).toContain("Green");

		// Second tap: expiry toast (resolved pickers pop their binding —
		// Hermes expired_rerun parity), no edit, no second resolution.
		await tap(adapter, tg, { hostChatId: 42, hostMessageId, data: "cp:1" });
		expect(tg.callbackAnswers.length).toBe(2);
		expect(tg.callbackAnswers[1]?.text).toContain("expired");
		expect(tg.editKwargs.length).toBe(1);
	});
});

describe("slash-confirm round-trip over the fake server", () => {
	it("sc:once approves with the static toast and strips; second tap is stale", async () => {
		const { adapter, tg, service, responses } = makeHookedWorld();
		const asked = service.askConfirm({
			chatId: "42",
			sessionKey: "sess-1",
			uiId: "ui-2",
			title: "Clear session?",
			message: "All messages will be lost.",
			respond: async (uiId, body) => {
				responses.push({ uiId, body });
			},
		});
		await flushSends(tg, 1);
		const data = keyboardData(tg, 0);
		// Byte-exact sc: payloads (approve-once / always / cancel rows).
		expect(data).toEqual(["sc:once:1", "sc:always:1", "sc:cancel:1"]);
		const hostMessageId = tg.sentMessages[0]?.message_id ?? 1;

		await tap(adapter, tg, { hostChatId: 42, hostMessageId, data: "sc:once:1" });
		expect(await asked).toEqual({ kind: "confirmed", confirmed: true });
		expect(responses).toEqual([{ uiId: "ui-2", body: { confirmed: true } }]);
		expect(tg.callbackAnswers[0]?.text).toBe("✅ Approved once");
		expect(tg.editKwargs[0]).toMatchObject({ reply_markup: null });

		await tap(adapter, tg, { hostChatId: 42, hostMessageId, data: "sc:once:1" });
		expect(tg.callbackAnswers[1]?.text).toContain("already been resolved");
		expect(tg.editKwargs.length).toBe(1);
	});
});

describe("clarify round-trip over the fake server", () => {
	it("numeric tap resolves and strips; other flips and text completes", async () => {
		const { adapter, tg, service, responses } = makeHookedWorld();
		const asked = service.askClarify({
			chatId: "42",
			sessionKey: "sess-9",
			uiId: "ui-9",
			question: "Favorite fruit?",
			options: ["Apple", "Mango"],
			respond: async (uiId, body) => {
				responses.push({ uiId, body });
			},
		});
		await flushSends(tg, 1);
		const data = keyboardData(tg, 0);
		expect(data).toEqual(["cl:1:0", "cl:1:1", "cl:1:other"]);
		const hostMessageId = tg.sentMessages[0]?.message_id ?? 1;

		// Other flips WITHOUT resolving: keyboard stripped, state kept.
		await tap(adapter, tg, { hostChatId: 42, hostMessageId, data: "cl:1:other" });
		expect(tg.callbackAnswers[0]?.text).toContain("Type your answer");
		expect(tg.editKwargs[0]).toMatchObject({ reply_markup: null });
		expect(service.hasPendingTextCapture("sess-9")).toBe(true);

		// The next message resolves through the text intercept.
		expect(service.takeText("sess-9", "mango")).toBe("✓ mango");
		expect(await asked).toEqual({ kind: "value", value: "mango" });
		expect(responses).toEqual([{ uiId: "ui-9", body: { value: "mango" } }]);
	});

	it("expired clarify tap toasts expiry AND strips the dead keyboard", async () => {
		const tg = new TelegramBotApiFake();
		const adapter = new TelegramAdapter({
			wire: tg,
			clock: new ManualPollingClock(),
			secretReader: () => "tok",
		});
		adapter.setClickerAuthorization(true);
		const host = tg.pushCallbackUpdate({
			hostChatId: 42,
			hostMessageId: 9,
			data: "cl:99:0",
		});
		await adapter.handleIngress(
			{ messageId: String(host.update_id), messageType: "text" },
			"tg:7001",
		);
		expect(tg.callbackAnswers.length).toBe(1);
		expect(tg.callbackAnswers[0]?.text).toContain("expired");
		expect(tg.editKwargs.length).toBe(1);
		expect(tg.editKwargs[0]).toMatchObject({ reply_markup: null });
	});
});

describe("answer-every-tap over the fake server", () => {
	it("garbage, unauthorized, and stale taps all clear the spinner and never turn", async () => {
		const tg = new TelegramBotApiFake();
		const adapter = new TelegramAdapter({
			wire: tg,
			clock: new ManualPollingClock(),
			secretReader: () => "tok",
		});
		// No forced authorization: the real chain denies the unknown clicker.
		for (const data of ["zz:garbage:stuff", "ea:once:4242", "mx:noop"]) {
			const update = tg.pushCallbackUpdate({
				hostChatId: 42,
				hostMessageId: 5,
				data,
				clickerId: 31337,
			});
			await adapter.handleIngress(
				{ messageId: String(update.update_id), messageType: "text" },
				"tg:7001",
			);
		}
		expect(tg.callbackAnswers.length).toBe(3);
		for (const answer of tg.callbackAnswers) {
			expect(answer.text.length).toBeGreaterThan(0);
		}
		expect(tg.editKwargs.length).toBe(0);
	});
});

describe("choice pagination over the fake server (DEC-089)", () => {
	it("25-option select pages 10 per screen; nav re-renders in place; absolute tap commits once", async () => {
		const { adapter, tg, service, responses } = makeHookedWorld();
		const options = Array.from({ length: 25 }, (_, i) => `Option ${i + 1}`);
		const asked = service.askSelect({
			chatId: "42",
			sessionKey: "sess-p",
			uiId: "ui-p",
			title: "Pick one",
			options,
			respond: async (uiId, body) => {
				responses.push({ uiId, body });
			},
		});
		await flushSends(tg, 1);
		const page0 = keyboardData(tg, 0);
		// 10 cp: + counter + Next (no Cancel — flat shape preserved).
		expect(page0.filter((d) => d.startsWith("cp:")).length).toBe(10);
		expect(page0).toContain("mx:noop");
		expect(page0).toContain("cpv:1");
		expect(page0).not.toContain("cp:10");
		for (const payload of page0) {
			expect(Buffer.byteLength(payload, "utf8")).toBeLessThanOrEqual(CALLBACK_DATA_MAX_BYTES);
		}
		const sendText = String((tg.sendKwargs[0] as { text: string }).text);
		expect(sendText).toContain("1–10 of 25");
		const hostMessageId = tg.sentMessages[0]?.message_id ?? 1;

		// Forward to page 1: in-place edit with absolute cp:10–19.
		await tap(adapter, tg, { hostChatId: 42, hostMessageId, data: "cpv:1" });
		expect(tg.editKwargs.length).toBe(1);
		const page1 = editKeyboardData(tg, 0);
		expect(page1.filter((d) => d.startsWith("cp:")).length).toBe(10);
		expect(page1).toContain("cp:10");
		expect(page1).toContain("cp:19");
		expect(page1).toContain("cpv:0");
		expect(page1).toContain("cpv:2");
		expect(String((tg.editKwargs[0] as { text: string }).text)).toContain("11–20 of 25");

		// Back to page 0 round-trips byte-identical.
		await tap(adapter, tg, { hostChatId: 42, hostMessageId, data: "cpv:0" });
		expect(editKeyboardData(tg, 1)).toEqual(page0);

		// Last page has 5 buttons.
		await tap(adapter, tg, { hostChatId: 42, hostMessageId, data: "cpv:2" });
		expect(editKeyboardData(tg, 2).filter((d) => d.startsWith("cp:")).length).toBe(5);

		// Absolute tap on the last row commits exactly once.
		await tap(adapter, tg, { hostChatId: 42, hostMessageId, data: "cp:24" });
		expect(await asked).toEqual({ kind: "value", value: "Option 25" });
		expect(responses).toEqual([{ uiId: "ui-p", body: { value: "Option 25" } }]);
		expect(tg.editKwargs.length).toBe(4);
		expect(tg.editKwargs[3]).toMatchObject({ reply_markup: null });

		// Double-tap: expiry, no second resolve, no extra edit.
		await tap(adapter, tg, { hostChatId: 42, hostMessageId, data: "cp:24" });
		expect(tg.callbackAnswers[tg.callbackAnswers.length - 1]?.text).toMatch(/expired|already been resolved/i);
		expect(tg.editKwargs.length).toBe(4);
	});

	it("single-page select keeps the flat keyboard (no nav, no counter)", async () => {
		const { adapter, tg, service } = makeHookedWorld();
		void adapter;
		const asked = service.askSelect({
			chatId: "42",
			sessionKey: "sess-1",
			uiId: "ui-1",
			title: "Pick",
			options: ["A", "B"],
			respond: async () => {},
		});
		await flushSends(tg, 1);
		expect(keyboardData(tg, 0)).toEqual(["cp:0", "cp:1"]);
		service.cancelSession("sess-1");
		await asked;
	});

});

describe("choice keyboard keeps the numbered body without the keyboard", () => {
	it("single-page send carries the absolute numbered list plus the typed hint", async () => {
		const { tg, service } = makeHookedWorld();
		const asked = service.askSelect({
			chatId: "42",
			sessionKey: "sess-body-1",
			uiId: "ui-body-1",
			title: "Pick a color",
			options: ["Red", "Green"],
			respond: async () => {},
		});
		await flushSends(tg, 1);
		// Keyboard still rides the wire.
		expect(keyboardData(tg, 0)).toEqual(["cp:0", "cp:1"]);
		// Body stays usable when the client never renders that keyboard:
		// title plus absolute numbers plus the typed-number hint.
		// Backslashes stripped: sends ride the clarify double-escape lane
		// (pre-convert plus wireSend), nav edits single — assert the raw
		// numbered shape either way.
		const text = String((tg.sendKwargs[0] as { text: string }).text);
		const clean = text.replace(/\\/g, "");
		expect(clean).toContain("Pick a color");
		expect(clean).toContain("1. Red");
		expect(clean).toContain("2. Green");
		expect(clean).toContain("Reply with the number");
		service.cancelSession("sess-body-1");
		await asked;
	});

	it("paged send and nav edits keep the page slice numbered absolutely", async () => {
		const { adapter, tg, service } = makeHookedWorld();
		const options = Array.from({ length: 25 }, (_, i) => `Option ${i + 1}`);
		const asked = service.askSelect({
			chatId: "42",
			sessionKey: "sess-body-p",
			uiId: "ui-body-p",
			title: "Pick one",
			options,
			respond: async () => {},
		});
		await flushSends(tg, 1);
		const sendText = String((tg.sendKwargs[0] as { text: string }).text);
		const sendClean = sendText.replace(/\\/g, "");
		expect(sendClean).toContain("1–10 of 25");
		expect(sendClean).toContain("1. Option 1");
		expect(sendClean).toContain("10. Option 10");
		expect(sendClean).not.toContain("11. Option 11");
		expect(sendClean).toContain("Reply with the number");
		expect(sendClean).toContain("next");
		const hostMessageId = tg.sentMessages[0]?.message_id ?? 1;
		await tap(adapter, tg, { hostChatId: 42, hostMessageId, data: "cpv:1" });
		expect(tg.editKwargs.length).toBe(1);
		const editText = String((tg.editKwargs[0] as { text: string }).text);
		const editClean = editText.replace(/\\/g, "");
		expect(editClean).toContain("11–20 of 25");
		expect(editClean).toContain("11. Option 11");
		expect(editClean).toContain("20. Option 20");
		expect(editClean).not.toContain("10. Option 10");
		expect(editClean).toContain("Reply with the number");
		// Typed numbers stay live while the keyboard body is on screen.
		expect(service.takeText("sess-body-p", "25")).toBe("✓ Option 25");
		await asked;
	});
});

describe("picker reconcile keeps the keyboard on the edit (both lanes bound)", () => {
	it("second choice send on the same chat edits with the replacement keyboard", async () => {
		const tg = new TelegramBotApiFake();
		const adapter = new TelegramAdapter({
			wire: tg,
			clock: new ManualPollingClock(),
			secretReader: () => "tok",
		});
		adapter.setClickerAuthorization(true);
		// Production-shaped binding: BOTH lanes reach the fake Bot API with
		// their full wire arg sets (text plus reply_markup when present).
		adapter.wireTransmitSend = async (chatId, content, metadata) =>
			bindSend(tg, chatId, content, metadata);
		adapter.editTransmit = async (chatId, messageId, content, metadata) => {
			const record = (metadata ?? {}) as Record<string, unknown>;
			await tg.editMessageText({
				chat_id: Number(chatId),
				message_id: Number(messageId),
				text: content,
				...(typeof record["parse_mode"] === "string"
					? { parse_mode: record["parse_mode"] as string }
					: {}),
				...(record["reply_markup"] !== undefined
					? { reply_markup: record["reply_markup"] }
					: {}),
			});
			return { success: true, messageId: String(messageId) };
		};
		await adapter.sendChoicePicker(
			"42",
			"Pick one",
			[
				{ value: "Red", label: "Red" },
				{ value: "Green", label: "Green" },
			],
			"sess-reconcile",
			101,
		);
		expect(tg.sendKwargs.length).toBe(1);
		expect(tg.editKwargs.length).toBe(0);
		expect(adapter.doorAudit()[0]?.action).toBe("plain-send");
		await adapter.sendChoicePicker(
			"42",
			"Pick two",
			[
				{ value: "Blue", label: "Blue" },
				{ value: "Yellow", label: "Yellow" },
			],
			"sess-reconcile",
			102,
		);
		// Second send reconciles by edit instead of duplicating.
		expect(tg.sendKwargs.length).toBe(1);
		expect(tg.editKwargs.length).toBe(1);
		expect(adapter.doorAudit()[1]?.action).toBe("reconcile-edit");
		// The replacement keyboard rides the edit lane.
		const edit = tg.editKwargs[0] as { text: string; reply_markup?: unknown };
		expect(String(edit.text).replace(/\\/g, "")).toContain("Pick two");
		const markup = edit.reply_markup as {
			inline_keyboard: Array<Array<{ text: string; callback_data: string }>>;
		};
		expect(markup).toBeDefined();
		expect(
			markup.inline_keyboard.flatMap((row) => row.map((button) => button.callback_data)),
		).toEqual(["cp:0", "cp:1"]);
	});
});
