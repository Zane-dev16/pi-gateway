// pi_agent_core/rpc-turn-runner-pickers.test — picker behavior contracts
// for turns. Fake turn clients that emit extension_ui_request mid-turn plus
// the REAL picker service over a fake sender: every test drives handleTurn
// the way the guard does and asserts the extension_ui_response bytes (with
// the ORIGINAL id — DEC-088), the turn outcome, and the message rows.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it, vi } from "vitest";
import { ChatProcRegistry } from "../../pi_agent_core/chat-proc-registry.js";
import {
	RpcTurnRunner,
	type TurnClient,
	type TurnPickerService,
	type TurnRpcEvent,
} from "../../pi_agent_core/rpc-turn-runner.js";
import { PickerService, type PickerSender } from "./service.js";
import {
	BARE_CATALOG_TIMEOUT_MS,
	BARE_SELECT_TIMEOUT_MS,
} from "../../pi_agent_core/rpc-turn-runner.js";
import type { SendResult } from "../streaming/adapter-seam.js";
import { StateStore } from "../../pi_state/index.js";

const OK: SendResult = { success: true };
const NOT_SUPPORTED: SendResult = { success: false, error: "Not supported" };

class FakeUiClient implements TurnClient {
	promptCalls: string[] = [];
	listeners: Array<(event: TurnRpcEvent) => void> = [];
	private idleWaiters: Array<() => void> = [];
	private idleOpen = false;
	finalText = "turn done";
	currentModel = { provider: "acme", id: "model-a" };
	catalog: Array<{ provider: string; id: string }> = [
		{ provider: "acme", id: "model-a" },
		{ provider: "acme", id: "model-b" },
	];
	setModelCalls: Array<{ provider: string; id: string }> = [];
	switchCalls: string[] = [];
	hostSessionId = "host-session-1";
	started = false;

	async start(): Promise<void> {
		this.started = true;
	}

	async stop(): Promise<void> {
		this.started = false;
	}

	async getState(): Promise<{ sessionId: string; isStreaming: boolean; model?: { provider: string; id: string } }> {
		return { sessionId: this.hostSessionId, isStreaming: false, model: { ...this.currentModel } };
	}

	async prompt(message: string): Promise<string> {
		this.promptCalls.push(message);
		return "started";
	}

	async waitForIdle(): Promise<void> {
		if (this.idleOpen) return;
		await new Promise<void>((resolve) => {
			this.idleWaiters.push(resolve);
		});
	}

	releaseIdle(): void {
		this.idleOpen = true;
		for (const wake of this.idleWaiters.splice(0)) wake();
	}

	nullText = false;

	async getLastAssistantText(): Promise<string> {
		if (this.nullText) return null as unknown as string;
		return this.finalText;
	}

	onEvent(listener: (event: TurnRpcEvent) => void): () => void {
		this.listeners.push(listener);
		return () => {};
	}

	emit(event: TurnRpcEvent): void {
		for (const listener of [...this.listeners]) listener(event);
	}

	async newSession(): Promise<unknown> {
		return { cancelled: false };
	}

	async switchSession(sessionPath: string): Promise<unknown> {
		this.switchCalls.push(sessionPath);
		return { switched: true };
	}

	async setModel(provider: string, modelId: string): Promise<{ provider: string; id: string }> {
		this.setModelCalls.push({ provider, id: modelId });
		this.currentModel = { provider, id: modelId };
		return { provider, id: modelId };
	}

	async getAvailableModels(): Promise<Array<{ provider: string; id: string }>> {
		return this.catalog.map((row) => ({ ...row }));
	}

	async compact(): Promise<{ summary: string; tokensBefore: number }> {
		return { summary: "s", tokensBefore: 1 };
	}

	async getEntries(): Promise<{ entries: Array<unknown>; leafId: unknown }> {
		return { entries: [], leafId: null };
	}

	async getCommands(): Promise<Array<{ name: string; description: string }>> {
		return [];
	}
}

class FakeSender implements PickerSender {
	interactivePickers = true;
	keyboardFails = false;
	readonly texts: string[] = [];
	choiceCalls = 0;

	private keyboard(): SendResult {
		return this.keyboardFails ? NOT_SUPPORTED : OK;
	}

	async sendChoicePicker(): Promise<SendResult> {
		this.choiceCalls += 1;
		return this.keyboard();
	}

	async sendSlashConfirm(): Promise<SendResult> {
		return this.keyboard();
	}

	async sendClarify(): Promise<SendResult> {
		return this.keyboard();
	}

	async sendModelPicker(): Promise<SendResult> {
		return this.keyboard();
	}

	async sendPickerText(_chatId: string, text: string): Promise<SendResult> {
		this.texts.push(text);
		return OK;
	}
}

let dir = "";
let store: StateStore | null = null;

afterEach(async () => {
	if (store !== null) {
		await store.close();
		store = null;
	}
	if (dir !== "") {
		rmSync(dir, { recursive: true, force: true });
		dir = "";
	}
});

async function openStore(): Promise<StateStore> {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-turn-pickers-"));
	store = await StateStore.open(join(dir, "state.db"));
	return store;
}

async function ensureSession(s: StateStore, id: string): Promise<void> {
	await s.withWrite((db) => {
		(db as { prepare(sql: string): { run(...args: unknown[]): void } })
			.prepare(
				"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'gateway', ?)",
			)
			.run(id, Math.floor(Date.now() / 1000));
	});
}

function makePickerRunner(
	fakes: FakeUiClient[],
	s: StateStore | null,
	opts: {
		sender?: FakeSender | undefined;
		responses?: Array<Record<string, unknown>> | undefined;
		recentSessions?: Array<{ id: string; file: string; label: string }> | undefined;
	} = {},
): {
	runner: RpcTurnRunner;
	service: PickerService;
	sender: FakeSender;
	fakes: FakeUiClient[];
} {
	let calls = 0;
	const registry = new ChatProcRegistry({
		makeClient: () => {
			const fake = fakes[calls];
			calls += 1;
			if (fake === undefined) throw new Error("no fake left");
			return fake;
		},
	});
	const sender = opts.sender ?? new FakeSender();
	const captured = opts.responses ?? [];
	let service: PickerService;
	const pickers: TurnPickerService = {
		askSelect: (o) => service.askSelect(o),
		askConfirm: (o) => service.askConfirm(o),
		askText: (o) => service.askText(o),
		cancelSession: (sessionKey) => service.storeRef.cancelSession(sessionKey),
	};
	const runner = new RpcTurnRunner({
		registry,
		resolveHome: () => "/tmp/fake-home",
		store: s,
		pickers,
		extensionUIResponder: async (_client, response) => {
			captured.push(response);
		},
		...(opts.recentSessions !== undefined
			? { listRecentSessions: () => opts.recentSessions ?? [] }
			: {}),
	});
	service = new PickerService({
		sender,
	});
	return { runner, service, sender, fakes };
}

async function flush(count = 30): Promise<void> {
	for (let i = 0; i < count; i += 1) {
		await new Promise((resolve) => setImmediate(resolve));
	}
}

describe("extension_ui select over a live turn", () => {
	it("text fallback resolves by number; response keeps the original id; turn completes", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const sender = new FakeSender();
		sender.keyboardFails = true;
		const responses: Array<Record<string, unknown>> = [];
		const fake = new FakeUiClient();
		
		const { runner, service } = makePickerRunner([fake], s, { sender, responses });

		const turned = runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			chatId: "42",
			text: "run the demo",
		});
		await flush();
		expect(fake.promptCalls).toEqual(["run the demo"]);
		// The child blocks on a select dialog mid-turn.
		fake.emit({
			type: "extension_ui_request",
			id: "uuid-select-1",
			method: "select",
			title: "Pick a color",
			options: ["Red", "Green"],
		});
		await flush();
		// Numbered-text fallback rendered (capability miss), no keyboard.
		expect(sender.choiceCalls).toBe(1);
		expect(sender.texts.length).toBe(1);
		expect(sender.texts[0]).toContain("1. Red");
		// Typed reply resolves; the response preserves the host id.
		expect(service.takeText("chat-1", "2")).toBe("✓ Green");
		await flush();
		expect(responses).toEqual([
			{ type: "extension_ui_response", id: "uuid-select-1", value: "Green" },
		]);
		fake.releaseIdle();
		const outcome = await turned;
		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("turn done");
		const rows = s.listMessages("sess-1");
		expect(rows.map((r) => [r.role, r.content])).toEqual([
			["user", "run the demo"],
			["assistant", "turn done"],
		]);
	});

	it("confirm resolves confirmed:false on cancel; timeout replies cancelled", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const sender = new FakeSender();
		sender.keyboardFails = true;
		const responses: Array<Record<string, unknown>> = [];
		const fake = new FakeUiClient();
		
		const { runner, service } = makePickerRunner([fake], s, { sender, responses });

		const turned = runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			chatId: "42",
			text: "wipe it",
		});
		await flush();
		fake.emit({
			type: "extension_ui_request",
			id: "uuid-confirm-1",
			method: "confirm",
			title: "Sure?",
			message: "Really wipe?",
		});
		await flush();
		expect(service.takeText("chat-1", "no")).toBe("❌ Cancelled");
		await flush();
		expect(responses).toEqual([
			{ type: "extension_ui_response", id: "uuid-confirm-1", confirmed: false },
		]);
		fake.releaseIdle();

		// A second dialog with a tight timeout settles cancelled untouched.
		const fake2 = new FakeUiClient();
		
		const { runner: runner2, service: _service2 } = makePickerRunner([fake2], s, {
			sender: new FakeSender(),
			responses,
		});
		void _service2;
		const turned2 = runner2.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-2",
			chatId: "42",
			text: "again",
		});
		await flush();
		fake2.emit({
			type: "extension_ui_request",
			id: "uuid-timeout-1",
			method: "select",
			title: "Pick",
			options: ["A"],
			timeout: 5,
		});
		const outcome2 = await Promise.race([
			turned2.then(() => "turned" as const),
			flush(60).then(() => "flushed" as const),
		]);
		void outcome2;
		fake2.releaseIdle();
		const final2 = await turned2;
		expect(final2.exitReason).toBe("finalized");
		expect(responses).toContainEqual({
			type: "extension_ui_response",
			id: "uuid-timeout-1",
			cancelled: true,
		});
		const outcome = await turned;
		expect(outcome.exitReason).toBe("finalized");
	});

	it("unknown custom payload falls back to numbered text and the turn completes", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const sender = new FakeSender();
		sender.keyboardFails = true;
		const responses: Array<Record<string, unknown>> = [];
		const fake = new FakeUiClient();
		
		const { runner, service } = makePickerRunner([fake], s, { sender, responses });

		const turned = runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			chatId: "42",
			text: "custom flow",
		});
		await flush();
		// custom() has no wire form today; a future payload with options
		// still lands in the numbered-text lane, never hangs.
		fake.emit({
			type: "extension_ui_request",
			id: "uuid-custom-1",
			method: "custom",
			title: "Pick one",
			options: ["Alpha", "Beta"],
		});
		await flush();
		expect(sender.texts.length).toBe(1);
		expect(sender.texts[0]).toContain("1. Alpha");
		expect(service.takeText("chat-1", "1")).toBe("✓ Alpha");
		await flush();
		expect(responses).toEqual([
			{ type: "extension_ui_response", id: "uuid-custom-1", value: "Alpha" },
		]);
		fake.releaseIdle();
		expect((await turned).exitReason).toBe("finalized");
	});
});

describe("handled turns with no assistant text", () => {
	it("null text coerces to empty at the boundary, never a null persist", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeUiClient();
		fake.nullText = true;
		fake.releaseIdle();
		const { runner } = makePickerRunner([fake], s, {});
		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			chatId: "42",
			text: "handled with no text",
		});
		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("");
		const rows = s.listMessages("sess-1");
		expect(rows.map((r) => [r.role, r.content])).toEqual([
			["user", "handled with no text"],
			["assistant", ""],
		]);
	});
});

describe("fire-and-forget extension UI is ignored", () => {
	it("notify/setStatus/setWidget/setTitle/set_editor_text never prompt nor respond", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const sender = new FakeSender();
		const responses: Array<Record<string, unknown>> = [];
		const fake = new FakeUiClient();
		const { runner, service } = makePickerRunner([fake], s, { sender, responses });
		fake.releaseIdle();

		const turned = runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			chatId: "42",
			text: "go",
		});
		await flush();
		for (const method of ["notify", "setStatus", "setWidget", "setTitle", "set_editor_text"]) {
			fake.emit({ type: "extension_ui_request", id: `ff-${method}`, method });
		}
		await flush();
		expect(sender.texts).toEqual([]);
		expect(responses).toEqual([]);
		expect(service.hasPending("chat-1")).toBe(false);
		expect((await turned).exitReason).toBe("finalized");
	});
});

describe("bare /model routes to the picker", () => {
	it("tap commits through setModel and the override sticks for respawns", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const sender = new FakeSender();
		const fake = new FakeUiClient();
		const { runner, service } = makePickerRunner([fake], s, { sender });

		const turned = runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			chatId: "42",
			text: "/model",
		});
		await flush();
		// First generic select lists providers (single acme row here).
		expect(sender.choiceCalls).toBe(1);
		expect(service.choiceTap(1, 0)).toEqual({ status: "resolved", value: "acme" });
		await flush();
		// Second generic select lists models of the chosen provider.
		expect(sender.choiceCalls).toBe(2);
		expect(service.choiceTap(2, 1)).toEqual({ status: "resolved", value: "model-b" });
		const outcome = await turned;
		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("Model: acme/model-b");
		expect(fake.setModelCalls).toEqual([{ provider: "acme", id: "model-b" }]);
	});

	it("without a picker service bare /model keeps the text catalog", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeUiClient();
		let calls = 0;
		const registry = new ChatProcRegistry({
			makeClient: () => {
				calls += 1;
				return fake;
			},
		});
		const runner = new RpcTurnRunner({ registry, resolveHome: () => "/tmp/x", store: s });
		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/model",
		});
		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toContain("Current model: acme/model-a");
		expect(calls).toBe(1);
	});
});

describe("bare /resume routes to the session picker", () => {
	const sessions = [
		{ id: "aaa", file: "/agent/sessions/aaa.jsonl", label: "/work/a — aaa" },
		{ id: "bbb", file: "/agent/sessions/bbb.jsonl", label: "/work/b — bbb" },
	];

	it("tap switches and the reply names the session", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const sender = new FakeSender();
		const fake = new FakeUiClient();
		const { runner, service } = makePickerRunner([fake], s, {
			sender,
			recentSessions: sessions,
		});

		const turned = runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			chatId: "42",
			text: "/resume",
		});
		await flush();
		expect(service.choiceTap(1, 1)).toEqual({ status: "resolved", value: "/work/b — bbb" });
		const outcome = await turned;
		expect(outcome.exitReason).toBe("finalized");
		expect(fake.switchCalls).toEqual(["/agent/sessions/bbb.jsonl"]);
		expect(outcome.finalText).toContain("Resumed session");
	});
});

describe("parked-picker takeover never wedges the chat", () => {
	it("a second turn aborts the parked picker and takes over", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const sender = new FakeSender();
		const fake = new FakeUiClient();
		const { runner } = makePickerRunner([fake], s, {
			sender,
			recentSessions: [{ id: "aaa", file: "/a.jsonl", label: "a" }],
		});

		const parked = runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			chatId: "42",
			text: "/model",
		});
		await flush();
		// Chat is parked on an untapped model picker; a new turn takes over.
		const taking = runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			chatId: "42",
			text: "hello instead",
		});
		await flush();
		fake.releaseIdle();
		const [parkedOutcome, takingOutcome] = await Promise.all([parked, taking]);
		expect(parkedOutcome.exitReason).toBe("finalized");
		expect(parkedOutcome.finalText).toBe("Model picker cancelled.");
		expect(takingOutcome.exitReason).toBe("finalized");
		expect(takingOutcome.finalText).toBe("turn done");
		expect(fake.setModelCalls).toEqual([]);
	});
});

describe("untapped bare pickers expire on their own", () => {
	const sessions = [{ id: "aaa", file: "/agent/sessions/aaa.jsonl", label: "/work/a — aaa" }];

	async function pump(rounds: number): Promise<void> {
		for (let i = 0; i < rounds; i += 1) {
			await vi.advanceTimersByTimeAsync(0);
		}
	}

	it("bare /resume settles cancelled and frees the chat with no tap", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const sender = new FakeSender();
		const fake = new FakeUiClient();
		const { runner, service } = makePickerRunner([fake], s, { sender, recentSessions: sessions });
		vi.useFakeTimers();
		try {
			const turned = runner.handleTurn({
				sessionId: "sess-1",
				routingKey: "chat-1",
				chatId: "42",
				text: "/resume",
			});
			await pump(20);
			expect(service.hasPending("chat-1")).toBe(true);
			await vi.advanceTimersByTimeAsync(BARE_SELECT_TIMEOUT_MS);
			expect(service.hasPending("chat-1")).toBe(false);
			const outcome = await turned;
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("Resume cancelled.");
			const rows = s.listMessages("sess-1");
			expect(rows.map((r) => [r.role, r.content])).toEqual([
				["user", "/resume"],
				["assistant", "Resume cancelled."],
			]);
		} finally {
			vi.useRealTimers();
		}
	});

	it("bare /model settles cancelled and frees the chat with no tap", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const sender = new FakeSender();
		const fake = new FakeUiClient();
		const { runner, service } = makePickerRunner([fake], s, { sender });
		vi.useFakeTimers();
		try {
			const turned = runner.handleTurn({
				sessionId: "sess-1",
				routingKey: "chat-1",
				chatId: "42",
				text: "/model",
			});
			await pump(20);
			expect(service.hasPending("chat-1")).toBe(true);
			await vi.advanceTimersByTimeAsync(BARE_SELECT_TIMEOUT_MS);
			expect(service.hasPending("chat-1")).toBe(false);
			const outcome = await turned;
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("Model picker cancelled.");
			expect(fake.setModelCalls).toEqual([]);
		} finally {
			vi.useRealTimers();
		}
	});
});

describe("bare /model catalog fetch is bounded", () => {
	it("hanging catalog settles cancelled fast with zero egress", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const sender = new FakeSender();
		const fake = new FakeUiClient();
		fake.getAvailableModels = () =>
			new Promise<Array<{ provider: string; id: string }>>(() => {});
		const { runner, service } = makePickerRunner([fake], s, { sender });

		const started = Date.now();
		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			chatId: "42",
			text: "/model",
		});
		const elapsed = Date.now() - started;

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("Model picker cancelled.");
		expect(sender.choiceCalls).toBe(0);
		expect(service.hasPending("chat-1")).toBe(false);
		expect(fake.setModelCalls).toEqual([]);
		expect(elapsed).toBeLessThan(BARE_CATALOG_TIMEOUT_MS + 10_000);
		const rows = s.listMessages("sess-1");
		expect(rows.map((r) => [r.role, r.content])).toEqual([
			["user", "/model"],
			["assistant", "Model picker cancelled."],
		]);
	});

	it("empty catalog cancels without rendering a picker", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const sender = new FakeSender();
		const fake = new FakeUiClient();
		fake.catalog = [];
		const { runner, service } = makePickerRunner([fake], s, { sender });

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			chatId: "42",
			text: "/model",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("Model picker cancelled.");
		expect(sender.choiceCalls).toBe(0);
		expect(service.hasPending("chat-1")).toBe(false);
		expect(fake.setModelCalls).toEqual([]);
	});

	it("healthy catalog still renders the provider picker", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const sender = new FakeSender();
		const fake = new FakeUiClient();
		const { runner, service } = makePickerRunner([fake], s, { sender });

		const turned = runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			chatId: "42",
			text: "/model",
		});
		await flush();
		expect(sender.choiceCalls).toBe(1);
		expect(service.hasPending("chat-1")).toBe(true);
		expect(service.choiceTap(1, 0)).toEqual({ status: "resolved", value: "acme" });
		await flush();
		expect(sender.choiceCalls).toBe(2);
		expect(service.choiceTap(2, 1)).toEqual({ status: "resolved", value: "model-b" });
		const outcome = await turned;
		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("Model: acme/model-b");
		expect(fake.setModelCalls).toEqual([{ provider: "acme", id: "model-b" }]);
	});
});
