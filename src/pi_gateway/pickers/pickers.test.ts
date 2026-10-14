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
	commits: Array<{ kind: string; sessionKey: string; ref: string }>;
} {
	const real = sender ?? new FakeSender();
	const responses: Array<{ uiId: string; body: Record<string, unknown> }> = [];
	const commits: Array<{ kind: string; sessionKey: string; ref: string }> = [];
	const service = new PickerService({
		sender: real,
		commit: {
			commitModel: async (sessionKey, provider, id) => {
				commits.push({ kind: "model", sessionKey, ref: `${provider}/${id}` });
				return `Model: ${provider}/${id}`;
			},
			commitResume: async (sessionKey, file) => {
				commits.push({ kind: "resume", sessionKey, ref: file });
				return `Resumed session (${file}).`;
			},
		},
	});
	return {
		service,
		sender: real,
		responses,
		commits,
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

describe("model drill-down: providers, warn/confirm, commit, abort", () => {
	const catalog = [
		{ provider: "acme", id: "model-a" },
		{ provider: "acme", id: "model-b" },
		{ provider: "other", id: "model-c" },
	];

	it("provider tap shows models; model tap commits and resolves the waiter", async () => {
		const { service, commits } = makeService();
		const asked = service.askModel({
			chatId: "chat-1",
			sessionKey: "sess-1",
			catalog,
			current: { provider: "acme", id: "model-a" },
		});
		await Promise.resolve();
		const providers = await service.modelTap(1, { kind: "provider", slug: "other" });
		expect(providers).toMatchObject({ answer: " " });
		if (!("screen" in providers) || providers.screen.kind !== "models") {
			throw new Error("expected models screen");
		}
		expect(providers.screen.models).toEqual(["model-c"]);
		const done = await service.modelTap(1, { kind: "model", index: 0 });
		if (!("screen" in done) || done.screen.kind !== "result") {
			throw new Error("expected result screen");
		}
		expect(done.screen.text).toBe("Model: other/model-c");
		expect(await asked).toEqual({
			status: "switched",
			provider: "other",
			id: "model-c",
			text: "Model: other/model-c",
		});
		expect(commits).toEqual([{ kind: "model", sessionKey: "sess-1", ref: "other/model-c" }]);
		// Second tap after commit is stale and commits nothing more.
		expect(await service.modelTap(1, { kind: "model", index: 0 })).toMatchObject({ stale: true });
		expect(commits.length).toBe(1);
	});

	it("expensive-model warning routes mm to the mc confirm screen", async () => {
		const sender = new FakeSender();
		const { responses } = makeService(sender);
		void responses;
		const service = new PickerService({
			sender,
			commit: {
				commitModel: async (_s, p, id) => `Model: ${p}/${id}`,
				commitResume: async () => "resumed",
			},
			warnForModel: (provider, id) =>
				provider === "acme" && id === "model-b" ? "This model is expensive." : null,
		});
		const asked = service.askModel({
			chatId: "chat-1",
			sessionKey: "sess-1",
			catalog,
			current: null,
		});
		await Promise.resolve();
		await service.modelTap(1, { kind: "provider", slug: "acme" });
		const warned = await service.modelTap(1, { kind: "model", index: 1 });
		if (!("screen" in warned) || warned.screen.kind !== "warn") {
			throw new Error("expected warn screen");
		}
		expect(warned.screen.text).toContain("expensive");
		expect(warned.screen.index).toBe(1);
		const done = await service.modelTap(1, { kind: "confirm", index: 1 });
		if (!("screen" in done) || done.screen.kind !== "result") {
			throw new Error("expected result screen");
		}
		expect(await asked).toMatchObject({ status: "switched", id: "model-b" });
	});

	it("cancel is abort-cache-safe: no commit, waiter settles cancelled", async () => {
		const { service, commits } = makeService();
		const asked = service.askModel({
			chatId: "chat-1",
			sessionKey: "sess-1",
			catalog,
			current: null,
		});
		await Promise.resolve();
		const cancelled = await service.modelTap(1, { kind: "cancel" });
		if (!("screen" in cancelled)) throw new Error("expected screen");
		expect(cancelled.screen).toEqual({ kind: "result", text: "Model picker cancelled." });
		expect(await asked).toEqual({ status: "cancelled" });
		expect(commits).toEqual([]);
	});

	it("text lane accepts provider/id and commits", async () => {
		const sender = new FakeSender();
		sender.keyboardFails = true;
		const { service } = makeService(sender);
		const asked = service.askModel({
			chatId: "chat-1",
			sessionKey: "sess-1",
			catalog,
			current: { provider: "acme", id: "model-a" },
		});
		await Promise.resolve();
		expect(sender.texts[0]?.text).toContain("acme/model-a");
		expect(service.takeText("sess-1", "other/model-c")).toBe("✓ other/model-c");
		expect(await asked).toMatchObject({ status: "switched", id: "model-c" });
	});
});

describe("resume picker lists at most ten and tap clears scope", () => {
	const sessions = Array.from({ length: 12 }, (_, index) => ({
		id: `sess-${index + 1}`,
		file: `/agent/sessions/sess-${index + 1}.jsonl`,
		label: `/work/dir-${index + 1} — sess-${index + 1}`,
	}));

	it("only the first ten rows are offered", async () => {
		const sender = new FakeSender();
		sender.keyboardFails = true;
		const { service } = makeService(sender);
		const asked = service.askResume({ chatId: "chat-1", sessionKey: "sess-0", sessions });
		await Promise.resolve();
		const text = sender.texts[0]?.text ?? "";
		expect(text).toContain("sess-10");
		expect(text).not.toContain("sess-11");
		expect(text).not.toContain("sess-12");
		expect(service.takeText("sess-0", "3")).toBe("✓ /work/dir-3 — sess-3");
		expect(await asked).toMatchObject({ status: "resumed", id: "sess-3" });
	});

	it("tap switches and clears every other pending picker for the session", async () => {
		const { service, commits } = makeService();
		const stale = service.askText({
			chatId: "chat-1",
			sessionKey: "sess-0",
			uiId: "ui-stale",
			title: "Old question?",
			respond: async () => {},
		});
		await Promise.resolve();
		const asked = service.askResume({ chatId: "chat-1", sessionKey: "sess-0", sessions });
		await Promise.resolve();
		expect(service.choiceTap(2, 0)).toEqual({
			status: "resolved",
			value: "/work/dir-1 — sess-1",
		});
		expect(await asked).toMatchObject({ status: "resumed", id: "sess-1" });
		expect(commits[0]).toMatchObject({ kind: "resume", ref: "/agent/sessions/sess-1.jsonl" });
		// Scope cleared: the older picker settled cancelled, not left armed.
		expect(await stale).toEqual({ kind: "cancelled" });
		expect(service.hasPending("sess-0")).toBe(false);
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

describe("resume pagination: all rows kept, 10 per page (DEC-089)", () => {
	const sessions = Array.from({ length: 22 }, (_, index) => ({
		id: `sess-${index + 1}`,
		file: `/agent/sessions/sess-${index + 1}.jsonl`,
		label: `/work/dir-${index + 1} — sess-${index + 1}`,
	}));

	it("keyboard pages; absolute tap resolves row 22; typed 11 resolves (no nag)", async () => {
		const { service, commits } = makeService();
		const asked = service.askResume({ chatId: "chat-1", sessionKey: "sess-r", sessions });
		await Promise.resolve();
		const p1 = service.choicePageTap(1, 1);
		if (!("screen" in p1)) throw new Error("expected screen");
		expect(p1.screen.total).toBe(22);
		expect(p1.screen.totalPages).toBe(3);
		expect(p1.screen.title).toContain("(11–20 of 22)");
		const p2 = service.choicePageTap(1, 2);
		if (!("screen" in p2)) throw new Error("expected screen");
		expect(p2.screen.choices.length).toBe(2);
		expect(service.choiceTap(1, 21)).toEqual({
			status: "resolved",
			value: "/work/dir-22 — sess-22",
		});
		expect(await asked).toMatchObject({ status: "resumed", id: "sess-22" });
		expect(commits[0]).toMatchObject({ kind: "resume", ref: "/agent/sessions/sess-22.jsonl" });
	});

	it("text fallback lists the first 10 with page info; next pages the text", async () => {
		const sender = new FakeSender();
		sender.keyboardFails = true;
		const { service } = makeService(sender);
		const asked = service.askResume({ chatId: "chat-1", sessionKey: "sess-r", sessions });
		await Promise.resolve();
		const text = sender.texts[0]?.text ?? "";
		expect(text).toContain("(1–10 of 22)");
		expect(text.split("\n").filter((l) => /^\d+\. /.test(l)).length).toBe(10);
		expect(text).not.toContain("sess-11");
		const page2 = service.takeText("sess-r", "next");
		expect(page2).toContain("(11–20 of 22)");
		expect(page2).toContain("11. /work/dir-11");
		// Absolute number reaches past the first page without paging first.
		expect(service.takeText("sess-r", "11")).toBe("✓ /work/dir-11 — sess-11");
		expect(await asked).toMatchObject({ status: "resumed", id: "sess-11" });
	});
});
