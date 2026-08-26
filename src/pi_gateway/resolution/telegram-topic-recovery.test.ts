// Telegram DM topic-lane pinning (fix cluster "telegram-topic-bindings").
//
// Unit contracts for recoverTelegramTopicThreadId — the port of
// gateway/run.py:_recover_telegram_topic_thread_id — over a REAL StateStore,
// plus the guard-hook factory wiring. Hermes test parity:
// tests/gateway/test_telegram_topic_mode.py (recovery matrix rows) and
// test_session_override_thread_recovery.py's pre-key ordering invariant.

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { StateStore } from "../../pi_state/index.js";
import { buildSessionKey, type SessionSource } from "./session-key.js";
import {
	isTelegramTopicLobbyThread,
	recoverTelegramTopicThreadId,
	telegramTopicGuardHooks,
} from "./telegram-topic-recovery.js";
import { makeTempDir, removeTempDir } from "../../pi_state/testing/harness.js";

let dir: string;

beforeEach(() => {
	dir = makeTempDir("pi-gw-tgrecover-");
});

afterEach(() => {
	removeTempDir(dir);
});

const CHAT = "208214988";
const USER = "208214988";

function dmSource(threadId?: string): SessionSource {
	return {
		platform: "telegram",
		chatType: "dm",
		userId: USER,
		chatId: CHAT,
		...(threadId !== undefined ? { threadId } : {}),
	};
}

async function storeWithBindings(): Promise<StateStore> {
	const store = await StateStore.open(`${dir}/state.db`);
	await store.enableTelegramTopicMode({ chatId: CHAT, userId: USER });
	await seedBinding(store, "111", "sA"); // older lane
	await seedBinding(store, "222", "sB"); // NEWEST lane (larger updated_at)
	return store;
}

let clockTick = 0;
async function seedBinding(
	store: StateStore,
	threadId: string,
	sessionId: string,
	userId = USER,
): Promise<void> {
	clockTick += 10;
	const sessionKey = "agent:main:telegram:dm:" + CHAT + ":" + threadId;
	store.db
		.prepare(
			"INSERT INTO sessions (id, source, session_key, started_at) VALUES (?, 'telegram', ?, ?)",
		)
		.run(sessionId, sessionKey, clockTick);
	await store.bindTelegramTopic(
		{
			chatId: CHAT,
			threadId,
			userId,
			sessionKey,
			sessionId,
		},
		{ nowSeconds: () => 1_000_000 + clockTick },
	);
}

function depsOf(store: StateStore) {
	return {
		topicModeEnabled: (chatId: string, userId: string) =>
			store.isTelegramTopicModeEnabled({ chatId, userId }),
		listTelegramTopicBindingsForChat: ({ chatId }: { chatId: string }) =>
			store.listTelegramTopicBindingsForChat({ chatId }),
	};
}

describe("recoverTelegramTopicThreadId — run.py:_recover_telegram_topic_thread_id matrix", () => {
	it("lobby reply ('' or General '1') pins to the user's newest bound topic", async () => {
		const store = await storeWithBindings();
		try {
			const deps = depsOf(store);
			expect(recoverTelegramTopicThreadId(dmSource(), deps)).toBe("222");
			expect(recoverTelegramTopicThreadId(dmSource("1"), deps)).toBe("222");
		} finally {
			await store.close();
		}
	});

	it("non-lobby unknown thread id is NEVER rewritten — brand-new topic keeps its own lane (#31086)", async () => {
		const store = await storeWithBindings();
		try {
			expect(
				recoverTelegramTopicThreadId(dmSource("99999"), depsOf(store)),
			).toBeNull();
			expect(
				recoverTelegramTopicThreadId(dmSource("9999"), depsOf(store)),
			).toBeNull();
		} finally {
			await store.close();
		}
	});

	it("a known non-lobby thread id is left alone too (only LOBBY shapes recover)", async () => {
		const store = await storeWithBindings();
		try {
			expect(
				recoverTelegramTopicThreadId(dmSource("111"), depsOf(store)),
			).toBeNull();
		} finally {
			await store.close();
		}
	});

	it("newest-first scan skips OTHER users' bindings and stops at the user's most recent lane", async () => {
		const store = await StateStore.open(`${dir}/state.db`);
		try {
			await store.enableTelegramTopicMode({ chatId: CHAT, userId: USER });
			await seedBinding(store, "700", "sOther", "someone-else"); // newer but foreign
			await seedBinding(store, "555", "sMine"); // mine, older than theirs
			expect(recoverTelegramTopicThreadId(dmSource(), depsOf(store))).toBe(
				"555",
			);
		} finally {
			await store.close();
		}
	});

	it("gates: mode disabled, group traffic, missing ids, empty bindings ⇒ null", async () => {
		const store = await StateStore.open(`${dir}/state.db`);
		try {
			const deps = depsOf(store);
			// Topic mode never enabled here.
			expect(recoverTelegramTopicThreadId(dmSource(), deps)).toBeNull();

			// Enabled, but no bindings yet.
			await store.enableTelegramTopicMode({ chatId: CHAT, userId: USER });
			expect(recoverTelegramTopicThreadId(dmSource(), deps)).toBeNull();

			// Non-DM / non-telegram arrivals are out of scope entirely.
			const group: SessionSource = { ...dmSource(), chatType: "group" };
			expect(recoverTelegramTopicThreadId(group, deps)).toBeNull();
			const slackDm: SessionSource = { ...dmSource(), platform: "slack" };
			expect(recoverTelegramTopicThreadId(slackDm, deps)).toBeNull();

			// Missing chat/user identifiers.
			expect(
				recoverTelegramTopicThreadId(
					{ platform: "telegram", chatType: "dm", userId: USER },
					deps,
				),
			).toBeNull();
			expect(
				recoverTelegramTopicThreadId(
					{ platform: "telegram", chatType: "dm", chatId: CHAT },
					deps,
				),
			).toBeNull();

			// Disabled AFTER bindings exist ⇒ still null.
			await seedBinding(store, "888", "sLate");
			await store.disableTelegramTopicMode({ chatId: CHAT });
			expect(recoverTelegramTopicThreadId(dmSource(), deps)).toBeNull();
		} finally {
			await store.close();
		}
	});

	it("recovered == inbound lobby id ⇒ null (no-op rewrite)", async () => {
		const store = await StateStore.open(`${dir}/state.db`);
		try {
			await store.enableTelegramTopicMode({ chatId: CHAT, userId: USER });
			await seedBinding(store, "1", "sGeneral"); // General bound as a lane
			expect(
				recoverTelegramTopicThreadId(dmSource("1"), depsOf(store)),
			).toBeNull();
		} finally {
			await store.close();
		}
	});
});

describe("guard-hook factory over the real store", () => {
	it("hooks rewrite the lobby thread and re-derive the canonical key; non-lobby passes through", async () => {
		const store = await storeWithBindings();
		try {
			const hooks = telegramTopicGuardHooks(store);
			const source = dmSource();

			expect(hooks.topicThreadRecovery(source)).toBe("222");
			expect(hooks.rebuildSessionKey({ ...source, threadId: "222" })).toBe(
				buildSessionKey({ ...source, threadId: "222" }),
			);

			// A fresh topic id passes through untouched.
			expect(hooks.topicThreadRecovery(dmSource("424242"))).toBeNull();
		} finally {
			await store.close();
		}
	});
});

describe("lobby classification", () => {
	it("'' and '1' are the only lobby shapes", () => {
		expect(isTelegramTopicLobbyThread(undefined)).toBe(true);
		expect(isTelegramTopicLobbyThread("")).toBe(true);
		expect(isTelegramTopicLobbyThread("1")).toBe(true);
		expect(isTelegramTopicLobbyThread("0")).toBe(false);
		expect(isTelegramTopicLobbyThread("17585")).toBe(false);
	});
});
