// pi_agent_core/rpc-turn-runner.test.ts — behavior contracts for per-chat
// turns. Fake turn clients plus one real StateStore on a temp db: every test
// drives handleTurn the way the guard does and asserts the outcome plus the
// message rows against literal values.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { ChatProcRegistry } from "./chat-proc-registry.js";
import { RpcTurnRunner } from "./rpc-turn-runner.js";
import type { TurnClient, TurnRpcEvent } from "./rpc-turn-runner.js";
import { StateStore } from "../pi_state/index.js";

class FakeTurnClient implements TurnClient {
	started = false;
	promptCalls: string[] = [];
	waitCalls = 0;
	disposition = "started";
	finalText = "hello back";
	failPrompt: string | null = null;
	failIdle: string | null = null;
	failText: string | null = null;
	failProbe = false;
	seen: TurnRpcEvent[] = [];
	gate: (() => void) | null = null;

	async start(): Promise<void> {
		this.started = true;
	}

	async stop(): Promise<void> {
		this.started = false;
	}

	async getState(): Promise<{ sessionId: string; isStreaming: boolean }> {
		if (this.failProbe) throw new Error("boom-probe");
		if (!this.started) throw new Error("not started");
		return { sessionId: "host-session-1", isStreaming: false };
	}

	async prompt(message: string): Promise<string> {
		this.promptCalls.push(message);
		if (this.failPrompt !== null) throw new Error(this.failPrompt);
		return this.disposition;
	}

	async waitForIdle(_timeoutMs?: number): Promise<void> {
		this.waitCalls += 1;
		if (this.failIdle !== null) throw new Error(this.failIdle);
		if (this.gate !== null) {
			await new Promise<void>((resolve) => {
				this.gate = resolve;
			});
		}
	}

	async getLastAssistantText(): Promise<string> {
		if (this.failText !== null) throw new Error(this.failText);
		return this.finalText;
	}

	onEvent(listener: (event: TurnRpcEvent) => void): () => void {
		this.seen.push({ type: "subscribed" });
		void listener;
		return () => {};
	}
}

/** Non-turn client: satisfies ProcClient but cannot drive a turn. */
class StartOnlyClient {
	started = false;

	async start(): Promise<void> {
		this.started = true;
	}

	async stop(): Promise<void> {
		this.started = false;
	}

	async getState(): Promise<{ sessionId: string; isStreaming: boolean }> {
		if (!this.started) throw new Error("not started");
		return { sessionId: "host-session-1", isStreaming: false };
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
	dir = mkdtempSync(join(tmpdir(), "pi-turn-runner-"));
	store = await StateStore.open(join(dir, "state.db"));
	return store;
}

/** Production session-ensure mirror: the guard inserts the row first. */
async function ensureSession(s: StateStore, id: string): Promise<void> {
	await s.withWrite((db) => {
		(db as { prepare(sql: string): { run(...args: unknown[]): void } })
			.prepare(
				"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'gateway', ?)",
			)
			.run(id, Math.floor(Date.now() / 1000));
	});
}

function makeRunner(fakes: FakeTurnClient[], s: StateStore | null) {
	let calls = 0;
	const registry = new ChatProcRegistry({
		makeClient: () => {
			const fake = fakes[calls];
			calls += 1;
			if (fake === undefined) throw new Error("no fake left");
			return fake;
		},
	});
	const runner = new RpcTurnRunner({
		registry,
		resolveHome: () => "/tmp/fake-home",
		store: s,
	});
	return { registry, runner, clientCalls: () => calls };
}

describe("RpcTurnRunner plain turn", () => {
	it("returns the child text and persists both rows", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const { runner } = makeRunner([new FakeTurnClient()], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "say hi",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("hello back");
		expect(outcome.iterations).toBe(1);
		expect(outcome.repairs).toBe(0);
		expect(typeof outcome.userRowId).toBe("number");
		expect(typeof outcome.assistantRowId).toBe("number");

		const rows = s.listMessages("sess-1");
		expect(rows.map((r) => [r.role, r.content])).toEqual([
			["user", "say hi"],
			["assistant", "hello back"],
		]);
	});

	it("fails the second concurrent turn fast without a second prompt", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		fake.gate = () => {};
		const { runner } = makeRunner([fake], s);

		const first = runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "one",
		});
		await new Promise((resolve) => setTimeout(resolve, 10));
		const second = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "two",
		});
		fake.gate?.();
		const done = await first;

		expect(second.exitReason).toBe("error");
		expect(second.errorMessage).toBe("turn already in progress for this chat");
		expect(fake.promptCalls).toEqual(["one"]);
		expect(done.exitReason).toBe("finalized");
	});

	it("keeps the child ready when the prompt fails on a live child", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		fake.failPrompt = "boom-prompt";
		const { registry, runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "say hi",
		});

		expect(outcome.exitReason).toBe("error");
		expect(outcome.errorMessage).toBe("prompt failed: boom-prompt");
		expect(outcome.userRowId).not.toBeNull();
		expect(outcome.assistantRowId).toBeNull();
		expect(registry.get("chat-1")?.state).toBe("ready");

		const rows = s.listMessages("sess-1");
		expect(rows.map((r) => r.role)).toEqual(["user"]);
	});

	it("marks the child dead when the prompt fails on a dead child, then heals", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const dead = new FakeTurnClient();
		dead.failPrompt = "boom-prompt";
		dead.failProbe = true;
		const healed = new FakeTurnClient();
		healed.finalText = "healed hi";
		const { registry, runner } = makeRunner([dead, healed], s);

		const failed = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "say hi",
		});
		expect(failed.exitReason).toBe("error");
		expect(registry.get("chat-1")?.state).toBe("dead");

		const next = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "again",
		});
		expect(next.exitReason).toBe("finalized");
		expect(next.finalText).toBe("healed hi");
		expect(registry.get("chat-1")?.generation).toBe(1);
		expect(healed.promptCalls).toEqual(["again"]);
	});

	it("skips the idle wait on handled dispositions", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		fake.disposition = "handled";
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/help",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("hello back");
		expect(fake.waitCalls).toBe(0);
	});

	it("reports spawn failure as an error outcome with null rows", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const registry = new ChatProcRegistry({
			makeClient: () => {
				throw new Error("no child left");
			},
		});
		const runner = new RpcTurnRunner({
			registry,
			resolveHome: () => "/tmp/fake-home",
			store: s,
		});

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "say hi",
		});

		expect(outcome.exitReason).toBe("error");
		expect(outcome.errorMessage).toBe("spawn failed: no child left");
		expect(outcome.userRowId).toBeNull();
		expect(outcome.assistantRowId).toBeNull();
	});

	it("reports a non-turn client as an error outcome", async () => {
		const registry = new ChatProcRegistry({
			makeClient: () => new StartOnlyClient(),
		});
		const runner = new RpcTurnRunner({
			registry,
			resolveHome: () => "/tmp/fake-home",
			store: null,
		});

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "say hi",
		});

		expect(outcome.exitReason).toBe("error");
		expect(outcome.errorMessage).toBe("chat child cannot drive a turn");
	});
});
