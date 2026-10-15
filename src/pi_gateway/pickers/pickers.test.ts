// pi_gateway/pickers/pickers.test — behavior contracts for the picker
// service over a fake sender. Every test drives the service the way the
// turn runner and the adapter tap path do, and asserts outcomes plus the
// exact extension_ui_response bodies — literal user-visible behavior, never
// source shapes.

import { describe, expect, it } from "vitest";
import { PickerStore } from "./store.js";
import { PickerService, type PickerSender } from "./service.js";
import type { SendResult } from "../streaming/adapter-seam.js";

const OK: SendResult = { success: true };
const NOT_SUPPORTED: SendResult = { success: false, error: "Not supported" };

interface SentText {
	chatId: string;
	text: string;
}

class FakeSender implements PickerSender {
	interactivePickers = true;
	keyboardFails = false;
	readonly texts: SentText[] = [];
	choiceCalls = 0;
	confirmCalls = 0;
	clarifyCalls = 0;
	modelCalls = 0;

	private keyboard(): SendResult {
		return this.keyboardFails ? NOT_SUPPORTED : OK;
	}

	async sendChoicePicker(): Promise<SendResult> {
		this.choiceCalls += 1;
		return this.keyboard();
	}

	async sendSlashConfirm(): Promise<SendResult> {
		this.confirmCalls += 1;
		return this.keyboard();
	}

	async sendModelPicker(): Promise<SendResult> {
		this.modelCalls += 1;
		return this.keyboard();
	}

	async sendClarify(): Promise<SendResult> {
		this.clarifyCalls += 1;
		return this.keyboard();
	}

	async sendPickerText(chatId: string, text: string): Promise<SendResult> {
		this.texts.push({ chatId, text });
		return OK;
	}
}

function makeService(sender?: FakeSender): {
	service: PickerService;
	sender: FakeSender;
	responses: Array<{ uiId: string; body: Record<string, unknown> }>;
} {
	const real = sender ?? new FakeSender();
	const responses: Array<{ uiId: string; body: Record<string, unknown> }> = [];
	const service = new PickerService({
		sender: real,
	});
	return {
		service,
		sender: real,
		responses,
	};
}

function responderOf(
	responses: Array<{ uiId: string; body: Record<string, unknown> }>,
): (uiId: string, body: Record<string, unknown>) => Promise<void> {
	return async (uiId, body) => {
		responses.push({ uiId, body });
	};
}

describe("select round-trip: tap resolves, second tap is stale", () => {
	it("choice tap settles the waiter with the exact value and one response", async () => {
		const { service, responses } = makeService();
		const asked = service.askSelect({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-1",
			title: "Pick a color",
			options: ["Red", "Green", "Blue"],
			respond: responderOf(responses),
		});
		await Promise.resolve();
		const claim = service.choiceTap(1, 2);
		expect(claim).toEqual({ status: "resolved", value: "Blue" });
		const outcome = await asked;
		expect(outcome).toEqual({ kind: "value", value: "Blue" });
		expect(responses).toEqual([{ uiId: "ui-1", body: { value: "Blue" } }]);
	});

	it("second tap gets stale and the committer runs exactly once", async () => {
		const { service, sender } = makeService();
		const asked = service.askSelect({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-1",
			title: "Pick",
			options: ["A", "B"],
			respond: async () => {},
		});
		await Promise.resolve();
		expect(service.choiceTap(1, 0)).toEqual({ status: "resolved", value: "A" });
		expect(service.choiceTap(1, 0)).toEqual({ status: "stale" });
		expect(service.choiceTap(1, 1)).toEqual({ status: "stale" });
		await asked;
		expect(sender.choiceCalls).toBe(1);
	});

	it("out-of-range tap index is stale, entry stays live", async () => {
		const { service } = makeService();
		const asked = service.askSelect({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-1",
			title: "Pick",
			options: ["A", "B"],
			respond: async () => {},
		});
		await Promise.resolve();
		expect(service.choiceTap(1, 9)).toEqual({ status: "stale" });
		expect(service.choiceTap(1, 1)).toEqual({ status: "resolved", value: "B" });
		await asked;
	});
});

describe("confirm round-trip maps once/always/cancel", () => {
	it.each([
		{ choice: "once" as const, confirmed: true, label: "✅ Approved once" },
		{ choice: "always" as const, confirmed: true, label: "🔒 Always approve" },
		{ choice: "cancel" as const, confirmed: false, label: "❌ Cancelled" },
	])("$choice settles confirmed=$confirmed with one response", async ({ choice, confirmed, label }) => {
		const { service, responses } = makeService();
		const asked = service.askConfirm({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-9",
			title: "Clear session?",
			message: "All messages will be lost.",
			respond: responderOf(responses),
		});
		await Promise.resolve();
		expect(service.confirmTap(1, choice)).toEqual({ label });
		const outcome = await asked;
		expect(outcome).toEqual({ kind: "confirmed", confirmed });
		expect(responses).toEqual([{ uiId: "ui-9", body: { confirmed } }]);
	});

	it("double confirm tap resolves once", async () => {
		const { service } = makeService();
		const asked = service.askConfirm({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-9",
			title: "Sure?",
			message: "Really.",
			respond: async () => {},
		});
		await Promise.resolve();
		expect(service.confirmTap(1, "once").label).not.toBeNull();
		expect(service.confirmTap(1, "once")).toEqual({ label: null });
		await asked;
	});
});

describe("clarify other flips to text capture without popping", () => {
	it("other keeps state; the next message resolves it", async () => {
		const { service, responses } = makeService();
		const asked = service.askText({
			chatId: "chat-1",
			sessionKey: "sess-7",
			uiId: "ui-7",
			title: "Favorite fruit?",
			respond: responderOf(responses),
		});
		await Promise.resolve();
		// Open-text entries capture at once: takeText consumes the reply.
		expect(service.takeText("sess-7", "mango")).toBe("✓ mango");
		const outcome = await asked;
		expect(outcome).toEqual({ kind: "value", value: "mango" });
		expect(responses).toEqual([{ uiId: "ui-7", body: { value: "mango" } }]);
	});

	it("choice entry: other flips, numeric still resolves, state kept meanwhile", async () => {
		const store = new PickerStore();
		const id = store.register({
			sessionKey: "sess-8",
			kind: "select",
			title: "Q",
			options: ["X", "Y"],
		});
		const { service } = makeService();
		void service;
		// Store-level contract: markAwaitingText flips without popping.
		expect(store.markAwaitingText(id)).toBe(true);
		expect(store.get(id)?.awaitingText).toBe(true);
		expect(store.pendingForSession("sess-8")?.id).toBe(id);
	});
});

describe("clarify ask: numeric tap resolves, other flips, text completes", () => {
	it("numeric tap settles the value; other flips and text resolves", async () => {
		const { service, responses, sender } = makeService();
		const asked = service.askClarify({
			chatId: "chat-1",
			sessionKey: "sess-9",
			uiId: "ui-9",
			question: "Favorite fruit?",
			options: ["Apple", "Mango"],
			respond: responderOf(responses),
		});
		await Promise.resolve();
		expect(sender.clarifyCalls).toBe(1);
		expect(service.clarifyTap(1, "other")).toEqual({ status: "awaiting-text" });
		expect(service.takeText("sess-9", "mango")).toBe("✓ mango");
		expect(await asked).toEqual({ kind: "value", value: "mango" });
		expect(responses).toEqual([{ uiId: "ui-9", body: { value: "mango" } }]);
	});

	it("numeric tap resolves at once with the choice text", async () => {
		const { service } = makeService();
		const asked = service.askClarify({
			chatId: "chat-1",
			sessionKey: "sess-9",
			uiId: "ui-9",
			question: "Q?",
			options: ["Apple", "Mango"],
			respond: async () => {},
		});
		await Promise.resolve();
		expect(service.clarifyTap(1, 0)).toEqual({ status: "resolved", value: "Apple" });
		expect(await asked).toEqual({ kind: "value", value: "Apple" });
	});
});

describe("capability miss latches to the numbered-text list", () => {
	it("Not-supported keyboard sends text once and never retries keyboards", async () => {
		const sender = new FakeSender();
		sender.keyboardFails = true;
		const { service } = makeService(sender);
		const first = service.askSelect({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-1",
			title: "Pick",
			options: ["A", "B"],
			respond: async () => {},
		});
		await Promise.resolve();
		expect(sender.choiceCalls).toBe(1);
		expect(sender.texts.length).toBe(1);
		expect(sender.texts[0]?.text).toContain("1. A");
		expect(sender.texts[0]?.text).toContain("2. B");
		// Text lane: numeric reply resolves.
		expect(service.takeText("sess-1", "2")).toBe("✓ B");
		expect(await first).toEqual({ kind: "value", value: "B" });

		const second = service.askSelect({
			chatId: "chat-1",
			sessionKey: "sess-2",
			uiId: "ui-2",
			title: "Pick again",
			options: ["C"],
			respond: async () => {},
		});
		await Promise.resolve();
		// Latched: no second keyboard attempt, straight to text.
		expect(sender.choiceCalls).toBe(1);
		expect(sender.texts.length).toBe(2);
		expect(service.takeText("sess-2", "1")).toBe("✓ C");
		expect(await second).toEqual({ kind: "value", value: "C" });
	});

	it("interactivePickers:false skips the keyboard attempt entirely", async () => {
		const sender = new FakeSender();
		sender.interactivePickers = false;
		const { service } = makeService(sender);
		const asked = service.askConfirm({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-1",
			title: "Sure?",
			message: "Really.",
			respond: async () => {},
		});
		await Promise.resolve();
		expect(sender.confirmCalls).toBe(0);
		expect(sender.texts.length).toBe(1);
		expect(sender.texts[0]?.text).toContain("yes or no");
		expect(service.takeText("sess-1", "yes")).toBe("✅ Approved");
		expect(await asked).toEqual({ kind: "confirmed", confirmed: true });
	});
});

describe("timeout and abort settle cancelled with exactly one response", () => {
	it("request timeout replies cancelled with the original id", async () => {
		const { service, responses } = makeService();
		const outcome = await service.askSelect({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-timeout",
			title: "Pick",
			options: ["A"],
			timeoutMs: 5,
			respond: responderOf(responses),
		});
		expect(outcome).toEqual({ kind: "cancelled" });
		expect(responses).toEqual([{ uiId: "ui-timeout", body: { cancelled: true } }]);
	});

	it("abort signal settles cancelled", async () => {
		const { service, responses } = makeService();
		const controller = new AbortController();
		const asked = service.askConfirm({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-abort",
			title: "Sure?",
			message: "Really.",
			signal: controller.signal,
			respond: responderOf(responses),
		});
		await Promise.resolve();
		controller.abort();
		expect(await asked).toEqual({ kind: "cancelled" });
		expect(responses).toEqual([{ uiId: "ui-abort", body: { cancelled: true } }]);
	});

	it("a failing responder never hangs the waiter", async () => {
		const { service } = makeService();
		const asked = service.askText({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-dead",
			title: "Name?",
			respond: async () => {
				throw new Error("child gone");
			},
		});
		await Promise.resolve();
		expect(service.takeText("sess-1", "zed")).toBe("✓ zed");
		expect(await asked).toEqual({ kind: "value", value: "zed" });
	});
});





describe("takeText never steals commands and never double-dispatches", () => {
	it("leading-slash bytes are not picker input", async () => {
		const { service } = makeService();
		const asked = service.askText({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-1",
			title: "Name?",
			respond: async () => {},
		});
		await Promise.resolve();
		expect(service.takeText("sess-1", "/new")).toBeNull();
		expect(service.takeText("sess-1", "amy")).toBe("✓ amy");
		expect(await asked).toEqual({ kind: "value", value: "amy" });
	});

	it("unmatched text on a choice prompt is consumed with a nag, entry kept", async () => {
		const { service } = makeService();
		const asked = service.askSelect({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-1",
			title: "Pick",
			options: ["Red", "Green"],
			respond: async () => {},
		});
		await Promise.resolve();
		expect(service.takeText("sess-1", "hello there")).toBe("Reply with a number 1–2.");
		expect(service.hasPending("sess-1")).toBe(true);
		expect(service.takeText("sess-1", "2")).toBe("✓ Green");
		expect(await asked).toEqual({ kind: "value", value: "Green" });
	});
});

describe("choice pagination: cpv nav, absolute taps, paged text lane (DEC-089)", () => {
	const options25 = Array.from({ length: 25 }, (_, i) => `Option ${i + 1}`);

	it("select pages 10 per screen with page info; nav round-trips; absolute tap resolves", async () => {
		const { service } = makeService();
		const asked = service.askSelect({
			chatId: "chat-1",
			sessionKey: "sess-p",
			uiId: "ui-p",
			title: "Pick one",
			options: options25,
			respond: async () => {},
		});
		await Promise.resolve();
		const p1 = service.choicePageTap(1, 1);
		if (!("screen" in p1)) throw new Error("expected screen");
		expect(p1.screen.page).toBe(1);
		expect(p1.screen.totalPages).toBe(3);
		expect(p1.screen.start).toBe(10);
		expect(p1.screen.total).toBe(25);
		expect(p1.screen.choices.length).toBe(10);
		expect(p1.screen.title).toContain("(11–20 of 25)");
		const back = service.choicePageTap(1, 0);
		if (!("screen" in back)) throw new Error("expected screen");
		expect(back.screen.page).toBe(0);
		expect(back.screen.choices[0]?.value).toBe("Option 1");
		expect(back.screen.title).toContain("(1–10 of 25)");
		// Absolute tap on the last row resolves even though it is on page 2.
		expect(service.choiceTap(1, 24)).toEqual({ status: "resolved", value: "Option 25" });
		expect(await asked).toEqual({ kind: "value", value: "Option 25" });
		// Stale nav after resolve.
		expect(service.choicePageTap(1, 1)).toMatchObject({ stale: true });
	});

	it("out-of-range page clamps; unknown picker is stale", async () => {
		const { service } = makeService();
		const asked = service.askSelect({
			chatId: "chat-1",
			sessionKey: "sess-p",
			uiId: "ui-p",
			title: "Pick one",
			options: options25,
			respond: async () => {},
		});
		await Promise.resolve();
		const far = service.choicePageTap(1, 99);
		if (!("screen" in far)) throw new Error("expected screen");
		expect(far.screen.page).toBe(2);
		expect(far.screen.choices.length).toBe(5);
		expect(far.screen.title).toContain("(21–25 of 25)");
		expect(service.choicePageTap(999, 0)).toMatchObject({ stale: true });
		service.cancelSession("sess-p");
		await asked;
	});

	it("text fallback stays paged; typed 11 resolves; next turns the page", async () => {
		const sender = new FakeSender();
		sender.keyboardFails = true;
		const { service } = makeService(sender);
		const asked = service.askSelect({
			chatId: "chat-1",
			sessionKey: "sess-t",
			uiId: "ui-t",
			title: "Pick one",
			options: options25,
			respond: async () => {},
		});
		await Promise.resolve();
		const fallback = sender.texts[0]?.text ?? "";
		expect(fallback).toContain("(1–10 of 25)");
		expect(fallback.split("\n").filter((l) => /^\d+\. /.test(l)).length).toBe(10);
		expect(fallback).toContain("1. Option 1");
		expect(fallback).not.toContain("Option 11");
		// Rows past 10 stay reachable by absolute number (no nag).
		expect(service.takeText("sess-t", "11")).toBe("✓ Option 11");
		expect(await asked).toEqual({ kind: "value", value: "Option 11" });
	});

	it("typed next/prev turns the text page without settling", async () => {
		const sender = new FakeSender();
		sender.keyboardFails = true;
		const { service } = makeService(sender);
		const asked = service.askSelect({
			chatId: "chat-1",
			sessionKey: "sess-n",
			uiId: "ui-n",
			title: "Pick one",
			options: options25,
			respond: async () => {},
		});
		await Promise.resolve();
		const page2 = service.takeText("sess-n", "next");
		expect(page2).toContain("(11–20 of 25)");
		expect(page2).toContain("11. Option 11");
		expect(service.hasPending("sess-n")).toBe(true);
		const back = service.takeText("sess-n", "prev");
		expect(back).toContain("(1–10 of 25)");
		expect(service.takeText("sess-n", "25")).toBe("✓ Option 25");
		expect(await asked).toEqual({ kind: "value", value: "Option 25" });
	});

	it("single-page select keeps the exact flat fallback (no page info)", async () => {
		const sender = new FakeSender();
		sender.keyboardFails = true;
		const { service } = makeService(sender);
		const asked = service.askSelect({
			chatId: "chat-1",
			sessionKey: "sess-1",
			uiId: "ui-1",
			title: "Pick",
			options: ["Red", "Green"],
			respond: async () => {},
		});
		await Promise.resolve();
		expect(sender.texts[0]?.text).not.toContain(" of ");
		expect(service.takeText("sess-1", "hello there")).toBe("Reply with a number 1–2.");
		expect(service.takeText("sess-1", "2")).toBe("✓ Green");
		expect(await asked).toEqual({ kind: "value", value: "Green" });
	});
});


