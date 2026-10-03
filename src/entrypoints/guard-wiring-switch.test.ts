// guard-wiring-switch.test.ts — DEC-079 session-hopping behavior
// contracts. The production handler drives REAL seams throughout: a real
// RoutingBinder over a temp state.db, real discovery over a temp agentDir,
// real adoption from real session files, the real drive-lock file, and (for
// the closing e2e) the REAL GatewayAgentRunner with a scripted model. Only
// the chat surface is stubbed — assertions observe bindings, rows, and wire
// context, never internals.

import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import type { TurnOutcome } from "../pi_agent_core/runner-types.js";
import { createRunnerHarness } from "../pi_agent_core/testing/runner-harness.js";
import { fauxAssistantMessage } from "../pi_agent_core/testing/faux-model.js";
import type { IncomingEvent } from "../pi_gateway/guards/index.js";
import { RoutingBinder } from "../pi_embedded/handoff/binder.js";
import { SessionDriverLock } from "../pi_embedded/handoff/session-lock.js";
import type { AdoptionStore } from "../pi_embedded/handoff/adoption.js";
import { StateStore } from "../pi_state/index.js";
import {
	buildProductionMessageHandler,
	type ChatTurnRunner,
	type SessionHopDeps,
} from "./guard-wiring.js";

const CHAT = "agent:main:test:dm:c1";

function event(text: string): IncomingEvent {
	return {
		messageType: "text",
		text,
		source: {
			platform: "test",
			chatType: "dm",
			userId: "pleb",
			chatId: "c1",
		},
		metadata: { gateway_session_key: CHAT },
	};
}

const ALLOW_ALL = () => ({
	allowed: true,
	gate: 0,
	reasonCode: "allowlisted_test",
	platform: "test",
	userId: "pleb",
	chatId: "c1",
});

const CTX = {
	task: {
		result: Promise.resolve(),
		isDone: () => true,
		cancel: () => {},
		cancelRequested: () => false,
	},
	throwIfCancelled: () => {},
};

interface RecordedTurn {
	sessionId: string;
	routingKey: string;
	text: string;
}

function stubRunner() {
	const texts: RecordedTurn[] = [];
	const dropCalls: string[] = [];
	const runner: ChatTurnRunner = {
		handleTurn: async (request: {
			sessionId: string;
			routingKey: string;
			text: string;
		}): Promise<TurnOutcome> => {
			texts.push({ ...request });
			return {
				exitReason: "finalized",
				finalText: `TURN:${request.text}`,
				iterations: 1,
				repairs: 0,
				userRowId: 1,
				assistantRowId: 2,
				usage: null,
			};
		},
		dropCachedSession: (sessionId: string): void => {
			dropCalls.push(sessionId);
		},
	};
	return { texts, dropCalls, runner };
}

function sessionFileLines(id: string, cwd: string, pairs: string[][]): string {
	const header =
		JSON.stringify({
			type: "session",
			version: 3,
			id,
			timestamp: new Date().toISOString(),
			cwd,
		}) + "\n";
	let n = 0;
	const lines = [header];
	for (const [user, assistant] of pairs) {
		const u = `e${++n}`;
		const a = `e${++n}`;
		lines.push(
			JSON.stringify({
				type: "message",
				id: u,
				parentId: null,
				timestamp: new Date().toISOString(),
				message: {
					role: "user",
					content: [{ type: "text", text: user }],
				},
			}) + "\n",
			JSON.stringify({
				type: "message",
				id: a,
				parentId: u,
				timestamp: new Date().toISOString(),
				message: {
					role: "assistant",
					content: [{ type: "text", text: assistant }],
				},
			}) + "\n",
		);
	}
	return lines.join("");
}

let dir: string;
let state: StateStore;
let binder: RoutingBinder;
let agentDir: string;
let adoptStore: AdoptionStore;
let mintCount: number;

beforeEach(async () => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-switch-"));
	state = await StateStore.open(join(dir, "state.db"));
	binder = new RoutingBinder(state.db);
	agentDir = mkdtempSync(join(tmpdir(), "pi-gw-switch-agent-"));
	mkdirSync(join(agentDir, "sessions", "--a--"), { recursive: true });
	mkdirSync(join(agentDir, "sessions", "--b--"), { recursive: true });
	writeFileSync(
		join(agentDir, "sessions", "--a--", "hist-1.jsonl"),
		sessionFileLines("hist-1", "/proj/a", [
			["deploy steps?", "freeze, migrate, verify"],
		]),
	);
	writeFileSync(
		join(agentDir, "sessions", "--b--", "hist-2.jsonl"),
		sessionFileLines("hist-2", "/proj/b", [["other topic", "other answer"]]),
	);
	adoptStore = {
		ensureSession: async (sessionId) => {
			await state.withWrite((db) => {
				db.prepare(
					"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'gateway', ?)",
				).run(sessionId, Math.floor(Date.now() / 1000));
			});
		},
		appendMessage: (m) =>
			state.appendMessage({
				sessionId: m.sessionId,
				role: m.role,
				content: m.content,
			}),
		messageCount: async (sessionId) =>
			(
				state.db
					.prepare("SELECT COUNT(*) AS n FROM messages WHERE session_id = ?")
					.get(sessionId) as { n: number }
			).n,
	};
	mintCount = 0;
});

afterEach(async () => {
	await state.close();
	rmSync(dir, { recursive: true, force: true });
	rmSync(agentDir, { recursive: true, force: true });
});

function hop(): SessionHopDeps {
	return {
		binder,
		agentDir,
		lockDir: dir,
		adoptStore,
		newSessionId: () => `fresh-${++mintCount}`,
	};
}

function rowContents(sessionId: string): string[] {
	return (
		state.db
			.prepare("SELECT content FROM messages WHERE session_id = ? ORDER BY id")
			.all(sessionId) as { content: string }[]
	).map((r) => r.content);
}

describe("switch contracts over real seams", () => {
	it("/sessions lists via passthrough bytes (no new code)", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			store: state,
			isAuthorized: ALLOW_ALL,
			sessionHop: hop(),
		});
		const reply = await handler(event("/sessions"), CTX);
		expect(reply).toBe("TURN:/sessions");
		const bound = binder.entryOf(CHAT)?.session_id ?? "";
		expect(bound).not.toBe("");
		expect(stub.texts).toEqual([
			{ sessionId: bound, routingKey: CHAT, text: "/sessions" },
		]);
	});

	it("/new repoints the chat binding; the next turn drives the fresh id", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			store: state,
			isAuthorized: ALLOW_ALL,
			sessionHop: hop(),
		});
		await handler(event("before"), CTX);
		const before = binder.entryOf(CHAT)?.session_id ?? "";
		const reply = await handler(event("/new"), CTX);
		expect(reply).toContain("Started a new session (fresh-1).");
		expect(binder.entryOf(CHAT)?.session_id).toBe("fresh-1");
		expect(binder.entryOf(CHAT)?.session_id).not.toBe(before);
		await handler(event("after"), CTX);
		expect(stub.texts.at(-1)).toEqual({
			sessionId: "fresh-1",
			routingKey: CHAT,
			text: "after",
		});
		// The repoint itself runs no turn.
		expect(stub.texts.map((t) => t.text)).toEqual(["before", "after"]);
	});

	it("/resume lands on history via binder switchSession", async () => {
		await state.withWrite((db) => {
			db.prepare(
				"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'gateway', ?)",
			).run("hist-9", Math.floor(Date.now() / 1000));
		});
		await state.appendMessage({
			sessionId: "hist-9",
			role: "user",
			content: "original question",
		});
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			store: state,
			isAuthorized: ALLOW_ALL,
			sessionHop: hop(),
		});
		const reply = await handler(event("/resume hist-9"), CTX);
		expect(reply).toContain("Resumed session (hist-9).");
		expect(binder.entryOf(CHAT)?.session_id).toBe("hist-9");
		expect(stub.dropCalls).toContain("hist-9");
		await handler(event("follow-up"), CTX);
		expect(stub.texts.at(-1)?.sessionId).toBe("hist-9");
		expect(rowContents("hist-9")[0]).toBe("original question");
	});

	it("/resume without an id answers usage and holds the binding", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			store: state,
			isAuthorized: ALLOW_ALL,
			sessionHop: hop(),
		});
		await handler(event("before"), CTX);
		const before = binder.entryOf(CHAT)?.session_id ?? "";
		const reply = await handler(event("/resume"), CTX);
		expect(reply).toContain("Usage: /resume <session-id>");
		expect(binder.entryOf(CHAT)?.session_id).toBe(before);
		expect(stub.texts.map((t) => t.text)).toEqual(["before"]);
	});
});

describe("path switching over real discovery + adoption", () => {
	it("/switch-path lists every path holding pi sessions", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			store: state,
			isAuthorized: ALLOW_ALL,
			sessionHop: hop(),
		});
		const reply = String(await handler(event("/switch-path"), CTX));
		expect(reply).toContain("- /proj/a");
		expect(reply).toContain("- /proj/b");
		expect(reply).toContain("/switch-path <path>");
		expect(stub.texts).toEqual([]);
	});

	it("/switch-path re-roots onto adopted history under the drive lock", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			store: state,
			isAuthorized: ALLOW_ALL,
			sessionHop: hop(),
		});
		const reply = String(await handler(event("/switch-path /proj/a"), CTX));
		expect(reply).toContain("re-rooted onto /proj/a");
		expect(reply).toContain("adopted 2 entries from session hist-1");
		expect(binder.entryOf(CHAT)?.session_id).toBe("hist-1");
		expect(stub.dropCalls).toContain("hist-1");
		expect(rowContents("hist-1")).toEqual([
			"deploy steps?",
			"freeze, migrate, verify",
		]);
		// The gateway now drives hist-1: a contender loses while we hold it.
		expect(new SessionDriverLock(dir, "hist-1").acquire()).toBe(false);
		await handler(event("next"), CTX);
		expect(stub.texts.at(-1)?.sessionId).toBe("hist-1");
		// Leaving releases the drive lock for the next owner.
		await handler(event("/new"), CTX);
		expect(new SessionDriverLock(dir, "hist-1").acquire()).toBe(true);
	});

	it("/switch-path on an unknown path holds the binding", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			store: state,
			isAuthorized: ALLOW_ALL,
			sessionHop: hop(),
		});
		await handler(event("before"), CTX);
		const before = binder.entryOf(CHAT)?.session_id ?? "";
		const reply = String(
			await handler(event("/switch-path /proj/nowhere"), CTX),
		);
		expect(reply).toContain("No pi sessions under /proj/nowhere");
		expect(binder.entryOf(CHAT)?.session_id).toBe(before);
	});

	it("a live foreign session stays read-only plus takeover, zero rows", async () => {
		const holder = new SessionDriverLock(dir, "hist-1");
		expect(holder.acquire()).toBe(true);
		try {
			const stub = stubRunner();
			const handler = buildProductionMessageHandler({
				runner: stub.runner,
				store: state,
				isAuthorized: ALLOW_ALL,
				sessionHop: hop(),
			});
			await handler(event("before"), CTX);
			const before = binder.entryOf(CHAT)?.session_id ?? "";
			const reply = String(await handler(event("/switch-path /proj/a"), CTX));
			expect(reply).toContain("live in another process");
			expect(reply).toContain("take over");
			expect(binder.entryOf(CHAT)?.session_id).toBe(before);
			expect(rowContents("hist-1")).toEqual([]);
		} finally {
			holder.release();
		}
	});

	it("/new-path starts a fresh session under a real directory", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			store: state,
			isAuthorized: ALLOW_ALL,
			sessionHop: hop(),
		});
		const target = join(agentDir, "sessions", "--a--");
		const reply = String(await handler(event(`/new-path ${target}`), CTX));
		expect(reply).toContain(`Fresh session (fresh-1) under ${target}.`);
		expect(binder.entryOf(CHAT)?.session_id).toBe("fresh-1");
		await handler(event("hello"), CTX);
		expect(stub.texts.at(-1)?.sessionId).toBe("fresh-1");
	});

	describe("path linkage (blocker 2 slice)", () => {
		it("/switch-path persists the adopted session's path", async () => {
			const stub = stubRunner();
			const handler = buildProductionMessageHandler({
				runner: stub.runner,
				store: state,
				isAuthorized: ALLOW_ALL,
				sessionHop: hop(),
			});
			expect(binder.entryOf(CHAT)).toBeNull();
			const reply = String(await handler(event("/switch-path /proj/a"), CTX));
			expect(reply).toContain("re-rooted onto /proj/a");
			expect(binder.entryOf(CHAT)?.session_id).toBe("hist-1");
			expect(binder.entryOf(CHAT)?.cwd).toBe("/proj/a");
		});

		it("/new-path persists the fresh target", async () => {
			const stub = stubRunner();
			const handler = buildProductionMessageHandler({
				runner: stub.runner,
				store: state,
				isAuthorized: ALLOW_ALL,
				sessionHop: hop(),
			});
			const target = join(agentDir, "sessions", "--a--");
			const reply = String(await handler(event(`/new-path ${target}`), CTX));
			expect(reply).toContain(`Fresh session (fresh-1) under ${target}.`);
			expect(binder.entryOf(CHAT)?.session_id).toBe("fresh-1");
			expect(binder.entryOf(CHAT)?.cwd).toBe(resolve(target));
		});

		it("/new keeps the current root on its fresh binding", async () => {
			const stub = stubRunner();
			const handler = buildProductionMessageHandler({
				runner: stub.runner,
				store: state,
				isAuthorized: ALLOW_ALL,
				sessionHop: hop(),
			});
			await handler(event("/switch-path /proj/a"), CTX);
			const reply = String(await handler(event("/new"), CTX));
			expect(reply).toContain("Started a new session (fresh-1).");
			expect(binder.entryOf(CHAT)?.session_id).toBe("fresh-1");
			expect(binder.entryOf(CHAT)?.cwd).toBe("/proj/a");
		});

		it("re-root restores the earlier path", async () => {
			const stub = stubRunner();
			const handler = buildProductionMessageHandler({
				runner: stub.runner,
				store: state,
				isAuthorized: ALLOW_ALL,
				sessionHop: hop(),
			});
			await handler(event("/switch-path /proj/a"), CTX);
			expect(binder.entryOf(CHAT)?.cwd).toBe("/proj/a");
			await handler(event("/switch-path /proj/b"), CTX);
			expect(binder.entryOf(CHAT)?.session_id).toBe("hist-2");
			expect(binder.entryOf(CHAT)?.cwd).toBe("/proj/b");
			const back = String(await handler(event("/switch-path /proj/a"), CTX));
			expect(back).toContain("re-rooted onto /proj/a");
			expect(binder.entryOf(CHAT)?.session_id).toBe("hist-1");
			expect(binder.entryOf(CHAT)?.cwd).toBe("/proj/a");
			await handler(event("next"), CTX);
			expect(stub.texts.at(-1)?.sessionId).toBe("hist-1");
		});

		it("an unknown path holds the binding and the path", async () => {
			const stub = stubRunner();
			const handler = buildProductionMessageHandler({
				runner: stub.runner,
				store: state,
				isAuthorized: ALLOW_ALL,
				sessionHop: hop(),
			});
			await handler(event("/switch-path /proj/a"), CTX);
			const reply = String(
				await handler(event("/switch-path /proj/nowhere"), CTX),
			);
			expect(reply).toContain("No pi sessions under /proj/nowhere");
			expect(binder.entryOf(CHAT)?.session_id).toBe("hist-1");
			expect(binder.entryOf(CHAT)?.cwd).toBe("/proj/a");
		});
	});

	it("/new-path refuses missing args and missing directories", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			store: state,
			isAuthorized: ALLOW_ALL,
			sessionHop: hop(),
		});
		await handler(event("before"), CTX);
		const before = binder.entryOf(CHAT)?.session_id ?? "";
		expect(String(await handler(event("/new-path"), CTX))).toContain(
			"Usage: /new-path <path>",
		);
		expect(
			String(await handler(event("/new-path /no/such/dir"), CTX)),
		).toContain("No such directory");
		expect(binder.entryOf(CHAT)?.session_id).toBe(before);
		expect(stub.texts.map((t) => t.text)).toEqual(["before"]);
	});
});

describe("e2e through the real host loop", () => {
	it("/new repoints and the next turn sees fresh history", async () => {
		const h = await createRunnerHarness();
		const hBinder = new RoutingBinder(h.store.db);
		const hAdopt: AdoptionStore = {
			ensureSession: async (sessionId) => {
				await h.store.withWrite((db) => {
					db.prepare(
						"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'gateway', ?)",
					).run(sessionId, Math.floor(Date.now() / 1000));
				});
			},
			appendMessage: (m) =>
				h.store.appendMessage({
					sessionId: m.sessionId,
					role: m.role,
					content: m.content,
				}),
			messageCount: async (sessionId) =>
				(
					h.store.db
						.prepare("SELECT COUNT(*) AS n FROM messages WHERE session_id = ?")
						.get(sessionId) as { n: number }
				).n,
		};
		let n = 0;
		const handler = buildProductionMessageHandler({
			runner: h.runner,
			store: h.store,
			isAuthorized: ALLOW_ALL,
			sessionHop: {
				binder: hBinder,
				agentDir,
				lockDir: dir,
				adoptStore: hAdopt,
				newSessionId: () => `live-fresh-${++n}`,
			},
		});
		try {
			h.faux.setResponses([fauxAssistantMessage("first answer")]);
			const first = await handler(event("first question"), CTX);
			expect(first).toBe("first answer");
			const oldId = hBinder.entryOf(CHAT)?.session_id ?? "";
			expect(oldId).not.toBe("");
			const switched = await handler(event("/new"), CTX);
			expect(String(switched)).toContain(
				`Started a new session (live-fresh-1).`,
			);
			const freshId = hBinder.entryOf(CHAT)?.session_id ?? "";
			expect(freshId).toBe("live-fresh-1");
			h.faux.setResponses([fauxAssistantMessage("second answer")]);
			const second = await handler(event("second question"), CTX);
			expect(second).toBe("second answer");
			const freshRows = h.store.listMessages(freshId);
			expect(freshRows.map((r) => r.role)).toEqual(["user", "assistant"]);
			expect(freshRows[0]?.content).toBe("second question");
			const oldRows = h.store.listMessages(oldId);
			expect(oldRows.map((r) => r.content)).toContain("first question");
			expect(freshRows.map((r) => r.content)).not.toContain("first question");
		} finally {
			await h.close();
		}
	});

	it("/resume lands history on the model wire", async () => {
		const h = await createRunnerHarness();
		const hBinder = new RoutingBinder(h.store.db);
		const hAdopt: AdoptionStore = {
			ensureSession: async (sessionId) => {
				await h.store.withWrite((db) => {
					db.prepare(
						"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'gateway', ?)",
					).run(sessionId, Math.floor(Date.now() / 1000));
				});
			},
			appendMessage: (m) =>
				h.store.appendMessage({
					sessionId: m.sessionId,
					role: m.role,
					content: m.content,
				}),
			messageCount: async (sessionId) =>
				(
					h.store.db
						.prepare("SELECT COUNT(*) AS n FROM messages WHERE session_id = ?")
						.get(sessionId) as { n: number }
				).n,
		};
		let n = 0;
		const handler = buildProductionMessageHandler({
			runner: h.runner,
			store: h.store,
			isAuthorized: ALLOW_ALL,
			sessionHop: {
				binder: hBinder,
				agentDir,
				lockDir: dir,
				adoptStore: hAdopt,
				newSessionId: () => `live-fresh-${++n}`,
			},
		});
		try {
			h.faux.setResponses([fauxAssistantMessage("blue noted")]);
			await handler(event("remember blue"), CTX);
			const rememberedId = hBinder.entryOf(CHAT)?.session_id ?? "";
			await handler(event("/new"), CTX);
			h.faux.setResponses([fauxAssistantMessage("other noted")]);
			await handler(event("unrelated"), CTX);
			const resumed = await handler(event(`/resume ${rememberedId}`), CTX);
			expect(String(resumed)).toContain(`Resumed session (${rememberedId}).`);
			const seenWire: string[] = [];
			h.faux.setResponses([
				(context) => {
					seenWire.push(JSON.stringify(context.messages ?? context));
					return fauxAssistantMessage("recall done");
				},
			]);
			await handler(event("what did I ask first"), CTX);
			expect(seenWire).toHaveLength(1);
			expect(seenWire[0]).toContain("remember blue");
			expect(seenWire[0]).not.toContain("unrelated");
		} finally {
			await h.close();
		}
	});
});

describe("bound chat root threads into handleTurn", () => {
	it("a chat rooted at /proj/a builds its turn under /proj/a", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			store: state,
			isAuthorized: ALLOW_ALL,
			sessionHop: hop(),
		});
		await handler(event("before"), CTX);
		await binder.setEntryCwd(CHAT, "/proj/a");
		await handler(event("after root"), CTX);
		expect(stub.texts.at(-1)).toEqual({
			sessionId: binder.entryOf(CHAT)?.session_id ?? "",
			routingKey: CHAT,
			text: "after root",
			cwd: "/proj/a",
		});
	});

	it("an unrooted chat sends no cwd key (runner falls back)", async () => {
		const stub = stubRunner();
		const handler = buildProductionMessageHandler({
			runner: stub.runner,
			store: state,
			isAuthorized: ALLOW_ALL,
			sessionHop: hop(),
		});
		await handler(event("plain"), CTX);
		expect(binder.entryOf(CHAT)?.cwd ?? null).toBeNull();
		expect(stub.texts).toEqual([
			{
				sessionId: binder.entryOf(CHAT)?.session_id ?? "",
				routingKey: CHAT,
				text: "plain",
			},
		]);
	});
});
