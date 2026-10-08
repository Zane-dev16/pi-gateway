// pi_agent_core/rpc-turn-runner.test.ts — behavior contracts for per-chat
// turns. Fake turn clients plus one real StateStore on a temp db: every test
// drives handleTurn the way the guard does and asserts the outcome plus the
// message rows against literal values.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, expect, it } from "vitest";
import { ChatProcRegistry } from "./chat-proc-registry.js";
import {
	parseSlashCommand,
	renderSessionExportHtml,
	RpcTurnRunner,
} from "./rpc-turn-runner.js";
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
	hostSessionId = "host-session-1";
	currentModel = { provider: "acme", id: "model-a" };
	catalog: Array<{ provider: string; id: string }> = [
		{ provider: "acme", id: "model-a" },
		{ provider: "acme", id: "model-b" },
	];
	compactSummary = "compacted summary";
	compactTokens = 1234;
	branchEntries: Array<unknown> = [
		{ id: "e1", role: "user", content: "hi" },
		{ id: "e2", role: "assistant", content: "hello" },
	];
	newSessionCalls = 0;
	switchCalls: string[] = [];
	setModelCalls: Array<{ provider: string; id: string }> = [];
	compactCalls: Array<string | undefined> = [];
	entriesCalls = 0;
	commandsCalls = 0;
	hostCommands: Array<{ name: string; description: string }> = [
		{ name: "model", description: "Select model" },
		{ name: "compact", description: "Compact context" },
		{ name: "skill:notes", description: "Skill notes" },
	];
	failCommands: string | null = null;
	failNew: string | null = null;
	failSwitch: string | null = null;
	failSetModel: string | null = null;
	failCompact: string | null = null;
	failEntries: string | null = null;
	failAvailable: string | null = null;
	probeCalls = 0;
	failProbeAfterCalls: number | null = null;

	async start(): Promise<void> {
		this.started = true;
	}

	async stop(): Promise<void> {
		this.started = false;
	}

	async getState(): Promise<{
		sessionId: string;
		isStreaming: boolean;
		model?: { provider: string; id: string };
	}> {
		if (this.failProbe) throw new Error("boom-probe");
		if (!this.started) throw new Error("not started");
		this.probeCalls += 1;
		if (
			this.failProbeAfterCalls !== null &&
			this.probeCalls > this.failProbeAfterCalls
		) {
			throw new Error("boom-probe");
		}
		return {
			sessionId: this.hostSessionId,
			isStreaming: false,
			model: { ...this.currentModel },
		};
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

	async newSession(): Promise<{ cancelled: false }> {
		this.newSessionCalls += 1;
		if (this.failNew !== null) throw new Error(this.failNew);
		this.hostSessionId = "host-session-2";
		return { cancelled: false as const };
	}

	async switchSession(sessionPath: string): Promise<{ cancelled: false }> {
		this.switchCalls.push(sessionPath);
		if (this.failSwitch !== null) throw new Error(this.failSwitch);
		this.hostSessionId = `resumed-${sessionPath}`;
		return { cancelled: false as const };
	}

	async setModel(
		provider: string,
		modelId: string,
	): Promise<{ provider: string; id: string }> {
		this.setModelCalls.push({ provider, id: modelId });
		if (this.failSetModel !== null) throw new Error(this.failSetModel);
		this.currentModel = { provider, id: modelId };
		return { ...this.currentModel };
	}

	async getAvailableModels(): Promise<
		Array<{ provider: string; id: string }>
	> {
		if (this.failAvailable !== null) throw new Error(this.failAvailable);
		return this.catalog.map((m) => ({ ...m }));
	}

	async compact(
		customInstructions?: string,
	): Promise<{ summary: string; tokensBefore: number }> {
		this.compactCalls.push(customInstructions);
		if (this.failCompact !== null) throw new Error(this.failCompact);
		return {
			summary: this.compactSummary,
			tokensBefore: this.compactTokens,
		};
	}

	async getEntries(): Promise<{
		entries: Array<unknown>;
		leafId: string | null;
	}> {
		this.entriesCalls += 1;
		if (this.failEntries !== null) throw new Error(this.failEntries);
		return { entries: [...this.branchEntries], leafId: "leaf-1" };
	}

	async getCommands(): Promise<
		Array<{ name: string; description: string }>
	> {
		this.commandsCalls += 1;
		if (this.failCommands !== null) throw new Error(this.failCommands);
		return this.hostCommands.map((c) => ({ ...c }));
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
			text: "/session",
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

describe("parseSlashCommand", () => {
	it("returns null for plain text", () => {
		expect(parseSlashCommand("say hi")).toBeNull();
	});

	it("parses the command word plus the args tail", () => {
		expect(parseSlashCommand("/resume abc123")).toEqual({
			name: "resume",
			args: "abc123",
		});
	});

	it("lowercases the name and trims the args", () => {
		expect(parseSlashCommand("  /MODEL  acme/model-b  ")).toEqual({
			name: "model",
			args: "acme/model-b",
		});
	});

	it("returns null for a bare slash", () => {
		expect(parseSlashCommand("/")).toBeNull();
	});
});

describe("RpcTurnRunner native sessions", () => {
	it("/new starts a fresh host session without a prompt", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { registry, runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/new",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("Started a new session (host-session-2).");
		expect(outcome.iterations).toBe(0);
		expect(fake.promptCalls).toEqual([]);
		expect(fake.newSessionCalls).toBe(1);
		expect(registry.get("chat-1")?.state).toBe("ready");

		const rows = s.listMessages("sess-1");
		expect(rows.map((r) => [r.role, r.content])).toEqual([
			["user", "/new"],
			["assistant", "Started a new session (host-session-2)."],
		]);
	});

	it("/resume with no id renders usage without a prompt", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/resume",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe(
			"Usage: /resume <session-id> — rebinds this chat onto that session's history.",
		);
		expect(fake.promptCalls).toEqual([]);
		expect(fake.switchCalls).toEqual([]);

		const rows = s.listMessages("sess-1");
		expect(rows.map((r) => r.role)).toEqual(["user", "assistant"]);
	});

	it("/resume rebinds through switchSession without a prompt", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/resume sess-file-9",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe(
			"Resumed session (resumed-sess-file-9). The next turn replays its history.",
		);
		expect(fake.promptCalls).toEqual([]);
		expect(fake.switchCalls).toEqual(["sess-file-9"]);
	});

	it("bare /model lists current plus catalog without a prompt", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/model",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe(
			"Current model: acme/model-a\nAvailable: acme/model-a, acme/model-b",
		);
		expect(fake.promptCalls).toEqual([]);
		expect(fake.setModelCalls).toEqual([]);
	});

	it("/model with a qualified ref switches without a prompt", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/model acme/model-b",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("Model: acme/model-b");
		expect(fake.promptCalls).toEqual([]);
		expect(fake.setModelCalls).toEqual([
			{ provider: "acme", id: "model-b" },
		]);

		const rows = s.listMessages("sess-1");
		expect(rows.map((r) => [r.role, r.content])).toEqual([
			["user", "/model acme/model-b"],
			["assistant", "Model: acme/model-b"],
		]);
	});

	it("/model with a bare id resolves against the catalog", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/model model-b",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("Model: acme/model-b");
		expect(fake.promptCalls).toEqual([]);
	});

	it("/model with an unknown ref replies failed without a prompt", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/model nope/nope",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toContain("Model switch failed");
		expect(fake.promptCalls).toEqual([]);
	});

	it("/compact renders the host summary without a prompt", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/compact",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe(
			"Compacted 1234 tokens of context.\n\ncompacted summary",
		);
		expect(fake.promptCalls).toEqual([]);
		expect(fake.compactCalls).toEqual([undefined]);
	});

	it("/compact refusals render as reply text", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		fake.failCompact = "Nothing to compact (session too small)";
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/compact focus on the API",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe(
			"Compaction failed: Nothing to compact (session too small)",
		);
		expect(fake.promptCalls).toEqual([]);
	});

	it("/export renders branch entries as inline JSONL", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/export",
		});

		const expected =
			`${JSON.stringify({ id: "e1", role: "user", content: "hi" })}\n` +
			`${JSON.stringify({ id: "e2", role: "assistant", content: "hello" })}\n`;
		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe(expected);
		expect(fake.promptCalls).toEqual([]);
		expect(fake.entriesCalls).toBe(1);
	});

	it("/export html renders the minimal transcript", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/export html",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toContain("<title>Session export</title>");
		expect(outcome.finalText).toContain("hello");
		expect(fake.promptCalls).toEqual([]);
	});

	it("/help lists host commands without a prompt", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/help",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe(
			"`/model` -- Select model\n`/compact` -- Compact context\n`/skill:notes` -- Skill notes",
		);
		expect(fake.promptCalls).toEqual([]);
		expect(fake.commandsCalls).toBe(1);
	});

	it("/help skills lists skill rows only", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/help skills",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("`/skill:notes` -- Skill notes");
		expect(fake.promptCalls).toEqual([]);
	});

	it("/help with no match renders the empty notice", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fake = new FakeTurnClient();
		const { runner } = makeRunner([fake], s);

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/help zzz-no-such",
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("No commands match.");
		expect(fake.promptCalls).toEqual([]);
	});

	it("a session command while busy fails fast with no native call", async () => {
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
			text: "/new",
		});
		fake.gate?.();
		await first;

		expect(second.exitReason).toBe("error");
		expect(second.errorMessage).toBe("turn already in progress for this chat");
		expect(fake.newSessionCalls).toBe(0);
		expect(fake.promptCalls).toEqual(["one"]);
	});

	it("a session transport failure marks dead then heals", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const dead = new FakeTurnClient();
		dead.failNew = "boom-new";
		dead.failProbeAfterCalls = 1;
		const healed = new FakeTurnClient();
		healed.hostSessionId = "host-session-9";
		const { registry, runner } = makeRunner([dead, healed], s);

		const failed = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/new",
		});
		expect(failed.exitReason).toBe("error");
		expect(failed.errorMessage).toBe("new failed: boom-new");
		expect(registry.get("chat-1")?.state).toBe("dead");

		const next = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/new",
		});
		expect(next.exitReason).toBe("finalized");
		expect(next.finalText).toContain("host-session-2");
		expect(registry.get("chat-1")?.generation).toBe(1);
	});

	it("renderSessionExportHtml escapes entry text", () => {
		const jsonl = `${JSON.stringify({ role: "user", content: "<b>hi</b>" })}\n`;
		const html = renderSessionExportHtml(jsonl);
		expect(html).toContain("&lt;b&gt;hi&lt;/b&gt;");
		expect(html).not.toContain("<b>hi</b>");
	});
});

describe("RpcTurnRunner path commands (DEC-085)", () => {
	function makePathRunner(
		fakes: FakeTurnClient[],
		s: StateStore | null,
		seams: { paths?: string[]; sessionFile?: string | null } = {},
	) {
		const spawnedCwd: Array<string | undefined> = [];
		let calls = 0;
		const registry = new ChatProcRegistry({
			makeClient: (opts) => {
				spawnedCwd.push(opts.cwd);
				const fake = fakes[calls];
				calls += 1;
				if (fake === undefined) throw new Error("no fake left");
				return fake;
			},
		});
		const runner = new RpcTurnRunner({
			registry,
			resolveHome: () => "/tmp/fake-home",
			listDiscoveryPaths: () => seams.paths ?? [],
			findSessionFileAtPath: () => seams.sessionFile ?? null,
			store: s,
		});
		return { registry, runner, spawnedCwd };
	}

	it("/switch-path stops the old child, spawns at the target, and rebinds", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const first = new FakeTurnClient();
		const second = new FakeTurnClient();
		const { registry, runner, spawnedCwd } = makePathRunner(
			[first, second],
			s,
			{
				paths: ["/proj/a"],
				sessionFile: "/home/sessions/x/s1.jsonl",
			},
		);

		const plain = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "hi",
		});
		expect(plain.exitReason).toBe("finalized");
		expect(spawnedCwd).toEqual(["/tmp/fake-home"]);
		expect(first.started).toBe(true);

		const moved = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/switch-path /proj/a",
		});
		expect(moved.exitReason).toBe("finalized");
		expect(moved.finalText).toBe(
			"Switched to /proj/a. Resumed session (resumed-/home/sessions/x/s1.jsonl).",
		);
		expect(first.started).toBe(false);
		expect(spawnedCwd).toEqual(["/tmp/fake-home", "/proj/a"]);
		expect(registry.get("chat-1")?.spawnOpts.cwd).toBe("/proj/a");
		expect(second.switchCalls).toEqual(["/home/sessions/x/s1.jsonl"]);
		expect(second.promptCalls).toEqual([]);
		expect(registry.get("chat-1")?.state).toBe("ready");

		const again = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "hi again",
		});
		expect(again.finalText).toBe("hello back");
		expect(spawnedCwd).toEqual(["/tmp/fake-home", "/proj/a"]);
		expect(second.promptCalls).toEqual(["hi again"]);
	});

	it("/new_path spawns at the target with a fresh session and no rebind", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const fresh = new FakeTurnClient();
		const { runner, spawnedCwd } = makePathRunner([fresh], s, {
			sessionFile: "/home/sessions/x/s9.jsonl",
		});

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/new_path /proj/b",
		});
		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe(
			"Started a fresh session (host-session-2) under /proj/b.",
		);
		expect(spawnedCwd).toEqual(["/proj/b"]);
		expect(fresh.newSessionCalls).toBe(1);
		expect(fresh.switchCalls).toEqual([]);
		expect(fresh.promptCalls).toEqual([]);
	});

	it("bare /switch-path lists paths without spawning a child", async () => {
		const s = await openStore();
		await ensureSession(s, "sess-1");
		const { registry, runner, spawnedCwd } = makePathRunner([], s, {
			paths: ["/proj/a", "/proj/b"],
		});

		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/switch-path",
		});
		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe(
			"Paths holding pi sessions:\n/proj/a\n/proj/b",
		);
		expect(spawnedCwd).toEqual([]);
		expect(registry.size).toBe(0);

		const rows = s.listMessages("sess-1");
		expect(rows.map((r) => [r.role, r.content])).toEqual([
			["user", "/switch-path"],
			["assistant", "Paths holding pi sessions:\n/proj/a\n/proj/b"],
		]);
	});

	it("bare /switch-path with no paths reports none", async () => {
		const { runner, spawnedCwd } = makePathRunner([], null, { paths: [] });
		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/switch-path",
		});
		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("No paths holding pi sessions.");
		expect(spawnedCwd).toEqual([]);
	});

	it("bare /new-path renders usage without spawning", async () => {
		const { runner, spawnedCwd } = makePathRunner([], null);
		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/new-path",
		});
		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe(
			"Usage: /new-path <path> — starts a fresh session under that path.",
		);
		expect(spawnedCwd).toEqual([]);
	});

	it("/switch-path with no session at the target still moves and starts fresh", async () => {
		const second = new FakeTurnClient();
		const { runner, spawnedCwd } = makePathRunner([second], null, {
			sessionFile: null,
		});
		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/switch-path /proj/empty",
		});
		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe(
			"Switched to /proj/empty. No sessions there yet — starting fresh.",
		);
		expect(spawnedCwd).toEqual(["/proj/empty"]);
		expect(second.switchCalls).toEqual([]);
		expect(second.newSessionCalls).toBe(0);
	});

	it("relative targets resolve against the current chat cwd", async () => {
		const a = new FakeTurnClient();
		const b = new FakeTurnClient();
		const { runner, spawnedCwd } = makePathRunner([a, b], null, {
			sessionFile: null,
		});
		await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/switch-path /proj/a",
		});
		const outcome = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/switch-path sub",
		});
		expect(outcome.finalText).toBe(
			"Switched to /proj/a/sub. No sessions there yet — starting fresh.",
		);
		expect(spawnedCwd).toEqual(["/proj/a", "/proj/a/sub"]);
	});

	it("a path switch while busy fails fast without stopping the child", async () => {
		const fake = new FakeTurnClient();
		fake.gate = () => {};
		const { runner } = makePathRunner([fake], null);

		const first = runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "one",
		});
		await new Promise((resolve) => setTimeout(resolve, 10));
		const second = await runner.handleTurn({
			sessionId: "sess-1",
			routingKey: "chat-1",
			text: "/new-path /proj/b",
		});
		fake.gate?.();
		await first;

		expect(second.exitReason).toBe("error");
		expect(second.errorMessage).toBe("turn already in progress for this chat");
		expect(fake.started).toBe(true);
	});
});
