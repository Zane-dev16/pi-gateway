// Behavior contracts for HandoffPipeline.process — the _process_handoff port
// (DEC-008 steps): destination resolution errors, thread capability fallback,
// SessionSource keying rules (Telegram DM-topic / Discord thread), session_key
// computation through THE shared builder, re-bind, and synthetic-event
// construction with the DEC-022 forged-event markers.

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	createHandoffHarness,
	type HandoffHarness,
} from "./testing/harness.js";
import {
	HandoffPipeline,
	deriveDestinationSource,
	looksLikeTelegramPrivateChatId,
	type HandoffRow,
	type SyntheticTurnDispatcher,
} from "./index.js";

let h: HandoffHarness;

beforeEach(async () => {
	h = await createHandoffHarness();
});

afterEach(async () => {
	await h.close();
});

function rowOf(
	id: string,
	platform = "telegram",
	title: string | null = "Port scan",
): HandoffRow {
	return {
		id,
		source: "cli",
		title,
		startedAt: 1,
		handoffPlatform: platform,
	};
}

const KEY_HOME_DM_THREAD = "agent:main:telegram:dm:100"; // no-thread fallback

describe("HandoffPipeline — destination derivation (pure rows)", () => {
	it("looksLikeTelegramPrivateChatId matches Python int(chat_id) > 0 semantics", () => {
		expect(looksLikeTelegramPrivateChatId("100")).toBe(true);
		expect(looksLikeTelegramPrivateChatId("+12")).toBe(true);
		expect(looksLikeTelegramPrivateChatId(" 7 ")).toBe(true);
		expect(looksLikeTelegramPrivateChatId("-500")).toBe(false);
		expect(looksLikeTelegramPrivateChatId("0")).toBe(false);
		expect(looksLikeTelegramPrivateChatId("12abc")).toBe(false); // int() raises
		expect(looksLikeTelegramPrivateChatId("")).toBe(false);
		expect(looksLikeTelegramPrivateChatId(null)).toBe(false);
		expect(looksLikeTelegramPrivateChatId(undefined)).toBe(false);
	});

	it("created thread + Telegram PRIVATE chat ⇒ DM-topic shape keyed like inbound replies", () => {
		const d = deriveDestinationSource({
			platform: "telegram",
			newThreadId: "t9",
			home: { platform: "telegram", chatId: "100" },
		});
		// A generic thread source would strand real replies on a dm key
		// (#handoff_thread_session_key parity row).
		expect(d.destChatType).toBe("dm");
		expect(d.destUserId).toBe("100"); // real user id, not system:handoff
		expect(d.effectiveThreadId).toBe("t9");
	});

	it("created thread + group chat ⇒ thread/system:handoff shape", () => {
		const d = deriveDestinationSource({
			platform: "slack",
			newThreadId: "t9",
			home: { platform: "slack", chatId: "-500" },
		});
		expect(d.destChatType).toBe("thread");
		expect(d.destUserId).toBe("system:handoff");
	});

	it("no created thread ⇒ dm/system:handoff (home thread fallback carried)", () => {
		const d = deriveDestinationSource({
			platform: "slack",
			newThreadId: null,
			home: { platform: "slack", chatId: "-500", threadId: "home-t" },
		});
		expect(d.destChatType).toBe("dm");
		expect(d.destUserId).toBe("system:handoff");
		expect(d.effectiveThreadId).toBe("home-t");
	});

	it("DISCORD thread keys on the THREAD's own id, never the parent channel", () => {
		const d = deriveDestinationSource({
			platform: "discord",
			newThreadId: "thr-77",
			home: { platform: "discord", chatId: "parent-1" },
		});
		expect(d.destChatType).toBe("thread");
		expect(d.destChatId).toBe("thr-77");
	});
});

describe("HandoffPipeline — happy path through the composed rig", () => {
	it("re-binds the destination key onto the CLI session and dispatches the forged internal event", async () => {
		h.seedCliSession("cli-1", [["hello", "hi there"]]);
		await h.queue.requestHandoff("cli-1", "telegram");

		await h.pipeline.process(rowOf("cli-1"));

		// Created thread + Telegram private chat ⇒ DM-topic continuity key.
		const expectedKey = "agent:main:telegram:dm:100:topic-1";
		expect(h.binder.entryOf(expectedKey)?.session_id).toBe("cli-1");

		// The synthetic turn RAN through the normal pipeline against the
		// RE-BOUND session id.
		expect(h.turns).toHaveLength(1);
		expect(h.turns[0]?.sessionKey).toBe(expectedKey);
		expect(h.turns[0]?.resolvedSessionId).toBe("cli-1");
		expect(h.turns[0]?.text).toContain("[Session was just handed off from CLI");
		expect(h.turns[0]?.text).toContain('("Port scan")');

		// Reply egressed through the guard wiring onto the destination.
		expect(h.replies.length).toBeGreaterThanOrEqual(1);
		expect(h.transport.sends[0]?.chatId).toBe("100");
	});

	it("no-thread-capability transport falls back to the plain home channel key", async () => {
		const bare = await createHandoffHarness({
			transport: { createThreads: false },
		});
		try {
			bare.seedCliSession("cli-2", []);
			await bare.queue.requestHandoff("cli-2", "telegram");
			await bare.pipeline.process(rowOf("cli-2"));
			expect(bare.transport.threadsCreated).toHaveLength(0);
			expect(bare.binder.entryOf(KEY_HOME_DM_THREAD)?.session_id).toBe("cli-2");
		} finally {
			await bare.close();
		}
	});

	it("createHandoffThread raising is CONTAINED — handoff proceeds without a thread", async () => {
		const exploding = await createHandoffHarness({
			transport: { throwOnCreate: true },
		});
		try {
			exploding.seedCliSession("cli-3", []);
			await exploding.queue.requestHandoff("cli-3", "telegram");
			await exploding.pipeline.process(rowOf("cli-3")); // must not throw
			expect(exploding.binder.entryOf(KEY_HOME_DM_THREAD)?.session_id).toBe(
				"cli-3",
			);
			expect(exploding.turns).toHaveLength(1);
		} finally {
			await exploding.close();
		}
	});
});

describe("HandoffPipeline — error contract (watcher records these)", () => {
	it("empty platform ⇒ 'handoff_platform is empty'", async () => {
		await expect(h.pipeline.process(rowOf("cli-1", ""))).rejects.toThrow(
			"handoff_platform is empty",
		);
	});

	it("inactive platform ⇒ 'not active in this gateway'", async () => {
		await expect(
			h.pipeline.process(rowOf("cli-1", "carrier-pigeon")),
		).rejects.toThrow(
			"platform 'carrier-pigeon' is not active in this gateway",
		);
	});

	it.each([
		[
			"missing home channel",
			{ threadId: undefined, chatId: undefined as string | undefined },
		],
	])("unconfigured/empty home ⇒ '/sethome' guidance (%s)", async (_label) => {
		const noHome = await createHandoffHarness();
		try {
			// Strip the chatId so resolveHomeChannel returns an unusable home.
			const broken = new HandoffPipeline({
				resolveTransport: () => noHome.transport,
				resolveHomeChannel: () => ({ platform: "telegram", chatId: "" }),
				binder: noHome.binder,
				dispatcher: { dispatch: async () => undefined },
				clock: noHome.clock,
			});
			await expect(broken.process(rowOf("cli-x"))).rejects.toThrow(
				/no home channel configured for telegram/,
			);
		} finally {
			await noHome.close();
		}
	});

	it("switch_session failure ⇒ 'could not switch session key …'", async () => {
		const broken = new HandoffPipeline({
			resolveTransport: () => h.transport,
			resolveHomeChannel: () => h.home,
			binder: {
				// ensureEntry succeeds (entry exists) but the swap fails — models
				// the corrupt/missing-entry race the null return guards.
				ensureEntry: async () => ({
					session_key: "agent:main:telegram:dm:100:topic-1",
					session_id: "pre-existing",
					created_at: 1,
					updated_at: 1,
				}),
				entryOf: () => null,
				switchSession: async () => null,
			},
			dispatcher: { dispatch: async () => undefined },
			clock: h.clock,
		});
		await expect(broken.process(rowOf("cli-1"))).rejects.toThrow(
			/could not switch session key .* → cli-1/,
		);
	});

	it("dispatcher rejection propagates (turn failure ⇒ failed row payload)", async () => {
		const failing = new HandoffPipeline({
			resolveTransport: () => h.transport,
			resolveHomeChannel: () => h.home,
			binder: h.binder,
			dispatcher: {
				dispatch: async () => {
					throw new Error("model provider exploded");
				},
			} satisfies SyntheticTurnDispatcher,
			clock: h.clock,
		});
		await expect(failing.process(rowOf("cli-1"))).rejects.toThrow(
			"model provider exploded",
		);
	});
});

describe("HandoffPipeline — isolation-flag seam", () => {
	it("thread-isolation OFF (default): shared thread key without a participant slot", async () => {
		const flagged = await createHandoffHarness({
			platform: "slack",
			home: { chatId: "-500" },
		});
		try {
			flagged.seedCliSession("cli-s", []);
			await flagged.queue.requestHandoff("cli-s", "slack");
			await flagged.pipeline.process({
				id: "cli-s",
				source: "cli",
				title: null,
				startedAt: 1,
				handoffPlatform: "slack",
			});
			// Created thread over a group home: thread/system:handoff shape;
			// threads SHARED by default (thread_sessions_per_user=False) ⇒ no
			// participant slot.
			const expected = "agent:main:slack:thread:-500:topic-1";
			expect(flagged.binder.entryOf(expected)?.session_id).toBe("cli-s");
		} finally {
			await flagged.close();
		}
	});

	it("thread-isolation ON: participant slot flows through THE shared builder", async () => {
		const flagged = await createHandoffHarness({
			platform: "slack",
			home: { chatId: "-500" },
			isolationFlags: {
				threadSessionsPerUser: true,
			},
		});
		try {
			flagged.seedCliSession("cli-s2", []);
			await flagged.queue.requestHandoff("cli-s2", "slack");
			await flagged.pipeline.process({
				id: "cli-s2",
				source: "cli",
				title: null,
				startedAt: 1,
				handoffPlatform: "slack",
			});
			const expected = "agent:main:slack:thread:-500:topic-1:system:handoff";
			expect(flagged.binder.entryOf(expected)?.session_id).toBe("cli-s2");
		} finally {
			await flagged.close();
		}
	});
});
