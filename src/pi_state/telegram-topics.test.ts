// Behavior contracts: the Telegram DM topic-bindings subsystem (explicit
// opt-in migration, mode enable/disable, binding upsert/prune). Hermes parity:
// tests/gateway/test_telegram_topic_mode.py (store-level rows) +
// hermes_state.py:apply_telegram_topic_migration semantics.
//
// Contracts under test (races and mutations, never snapshots):
//   - startup reconcile NEVER creates the side tables; only explicit
//     migration/enable does; reads on absent tables degrade to false/[]/0.
//   - v1→v2 version-gated rebuild preserves rows and lands the CASCADE FK.
//   - bind idempotence + one-topic-per-session rule + linked_at preservation.
//   - #31501 prune flips topic mode off when the LAST binding goes, in ONE
//     transaction.

import Database from "better-sqlite3";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { initStore, getMeta } from "./reconcile.js";
import { StateStore } from "./store.js";
import {
	applyTelegramTopicMigration,
	bindTelegramTopic,
	deleteTelegramTopicBinding,
	disableTelegramTopicMode,
	enableTelegramTopicMode,
	getTelegramTopicBinding,
	getTelegramTopicBindingBySession,
	isTelegramSessionLinkedToTopic,
	isTelegramTopicModeEnabled,
	listTelegramTopicBindingsForChat,
	TELEGRAM_TOPIC_SCHEMA_VERSION_KEY,
} from "./telegram-topics.js";
import { openDatabase } from "./wal.js";
import { makeTempDir, removeTempDir } from "./testing/harness.js";

let dir: string;

beforeEach(() => {
	dir = makeTempDir("pi-gw-tgtopics-");
});

afterEach(() => {
	removeTempDir(dir);
});

function dbPath(name = "state.db"): string {
	return `${dir}/${name}`;
}

function topicTables(raw: Database.Database): string[] {
	return (
		raw
			.prepare(
				"SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'telegram_dm%' ORDER BY name",
			)
			.all() as Array<{ name: string }>
	).map((r) => r.name);
}

/** Minimal session row so bindings' FK resolves (sessions(id) must exist). */
function seedSession(
	raw: Database.Database,
	id: string,
	sessionKey = `agent:main:telegram:dm:208214988:${id}`,
): void {
	raw
		.prepare(
			"INSERT INTO sessions (id, source, session_key, started_at) VALUES (?, 'telegram', ?, ?)",
		)
		.run(id, sessionKey, Date.now() / 1000);
}

async function openRaw(path: string): Promise<Database.Database> {
	const { db } = await openDatabase({ path });
	initStore(db);
	return db;
}

describe("explicit migration walk", () => {
	it("startup NEVER creates the side tables — only explicit migration does", async () => {
		const p = dbPath();
		const store = await StateStore.open(p);
		try {
			expect(topicTables(store.db)).toEqual([]);
			await store.applyTelegramTopicMigration();
			expect(topicTables(store.db)).toEqual([
				"telegram_dm_topic_bindings",
				"telegram_dm_topic_mode",
			]);
			expect(getMeta(store.db, TELEGRAM_TOPIC_SCHEMA_VERSION_KEY)).toBe("2");
		} finally {
			await store.close();
		}
	});

	it("is idempotent: re-running keeps tables singular and version stamped", async () => {
		const raw = await openRaw(dbPath());
		try {
			await applyTelegramTopicMigration(raw);
			await applyTelegramTopicMigration(raw);
			await applyTelegramTopicMigration(raw);
			expect(topicTables(raw)).toHaveLength(2);
			const idx = raw
				.prepare(
					"SELECT name FROM sqlite_master WHERE type='index' AND name LIKE 'idx_telegram_dm%'",
				)
				.all();
			expect(idx).toHaveLength(2);
			expect(getMeta(raw, TELEGRAM_TOPIC_SCHEMA_VERSION_KEY)).toBe("2");
		} finally {
			raw.close();
		}
	});

	it("v1→v2 rebuild: pre-CASCADE bindings table is rebuilt row-preserving with ON DELETE CASCADE", async () => {
		const p = dbPath();
		const raw = await openRaw(p);
		try {
			// Simulate a v1 store: table exists WITHOUT the CASCADE FK, no
			// version stamp. FK enforcement off while seeding (legacy shape).
			raw.pragma("foreign_keys = OFF");
			raw.exec(`
				CREATE TABLE telegram_dm_topic_bindings (
					chat_id TEXT NOT NULL,
					thread_id TEXT NOT NULL,
					user_id TEXT NOT NULL,
					session_key TEXT NOT NULL,
					session_id TEXT NOT NULL REFERENCES sessions(id),
					managed_mode TEXT NOT NULL DEFAULT 'auto',
					linked_at REAL NOT NULL,
					updated_at REAL NOT NULL,
					PRIMARY KEY (chat_id, thread_id)
				);
			`);
			seedSession(raw, "sess-A");
			raw
				.prepare(
					"INSERT INTO telegram_dm_topic_bindings VALUES ('208214988','111','208214988','k','sess-A','auto',1,1)",
				)
				.run();
			raw.pragma("foreign_keys = ON");

			await applyTelegramTopicMigration(raw);

			const fk = raw
				.prepare("PRAGMA foreign_key_list('telegram_dm_topic_bindings')")
				.all() as Array<Record<string, unknown>>;
			const sessionsFk = fk.find((r) => String(r["table"]) === "sessions");
			expect(sessionsFk).toBeDefined();
			expect(String(sessionsFk!["on_delete"])).toBe("CASCADE");
			const rows = raw
				.prepare(
					"SELECT chat_id, thread_id, session_id FROM telegram_dm_topic_bindings",
				)
				.all();
			expect(rows).toEqual([
				{ chat_id: "208214988", thread_id: "111", session_id: "sess-A" },
			]);
			expect(getMeta(raw, TELEGRAM_TOPIC_SCHEMA_VERSION_KEY)).toBe("2");

			// The point of v2: pruning the session auto-clears its binding.
			raw.prepare("DELETE FROM sessions WHERE id = 'sess-A'").run();
			expect(
				raw
					.prepare("SELECT COUNT(*) AS n FROM telegram_dm_topic_bindings")
					.get(),
			).toEqual({ n: 0 });
		} finally {
			raw.close();
		}
	});
});

describe("mode enable/disable/read", () => {
	it("enable upserts per chat: conflict refreshes user/enabled/capabilities, keeps activated_at", async () => {
		const raw = await openRaw(dbPath());
		let clock = 1000;
		const nowSeconds = (): number => clock;
		try {
			await enableTelegramTopicMode(
				raw,
				{
					chatId: "208214988",
					userId: "u1",
					hasTopicsEnabled: true,
					allowsUsersToCreateTopics: false,
				},
				{ nowSeconds },
			);
			clock = 2000; // re-enable later by another actor row update
			await enableTelegramTopicMode(
				raw,
				{
					chatId: "208214988",
					userId: "u1",
					hasTopicsEnabled: null,
					allowsUsersToCreateTopics: true,
				},
				{ nowSeconds },
			);

			const row = raw
				.prepare(
					"SELECT * FROM telegram_dm_topic_mode WHERE chat_id = '208214988'",
				)
				.get() as Record<string, unknown>;
			expect(row["user_id"]).toBe("u1");
			expect(row["enabled"]).toBe(1);
			expect(row["activated_at"]).toBe(1000); // first activation preserved
			expect(row["updated_at"]).toBe(2000);
			expect(row["has_topics_enabled"]).toBe(null); // latest write wins (NULL)
			expect(row["allows_users_to_create_topics"]).toBe(1);
			expect(row["capability_checked_at"]).toBe(2000);
			expect(
				isTelegramTopicModeEnabled(raw, { chatId: "208214988", userId: "u1" }),
			).toBe(true);
		} finally {
			raw.close();
		}
	});

	it("enabled flag is per (chat, user); disabled or foreign user ⇒ false", async () => {
		const raw = await openRaw(dbPath());
		try {
			await enableTelegramTopicMode(raw, { chatId: "c1", userId: "u1" });
			await disableTelegramTopicMode(raw, { chatId: "c1" });
			expect(
				isTelegramTopicModeEnabled(raw, { chatId: "c1", userId: "u1" }),
			).toBe(false);
			await enableTelegramTopicMode(raw, { chatId: "c1", userId: "u1" });
			expect(
				isTelegramTopicModeEnabled(raw, { chatId: "c1", userId: "OTHER" }),
			).toBe(false);
			expect(
				isTelegramTopicModeEnabled(raw, { chatId: "cX", userId: "u1" }),
			).toBe(false);
		} finally {
			raw.close();
		}
	});

	it("disable clears bindings by default; clearBindings=false preserves them; absent tables no-op without creating", async () => {
		const p = dbPath();
		const raw = await openRaw(p);
		try {
			await enableTelegramTopicMode(raw, { chatId: "c1", userId: "u1" });
			seedSession(raw, "s1");
			seedSession(raw, "s2");
			await bindTelegramTopic(raw, {
				chatId: "c1",
				threadId: "11",
				userId: "u1",
				sessionKey: "k1",
				sessionId: "s1",
			});
			await bindTelegramTopic(raw, {
				chatId: "c1",
				threadId: "22",
				userId: "u1",
				sessionKey: "k2",
				sessionId: "s2",
			});

			await disableTelegramTopicMode(raw, {
				chatId: "c1",
				clearBindings: false,
			});
			expect(
				isTelegramTopicModeEnabled(raw, { chatId: "c1", userId: "u1" }),
			).toBe(false);
			expect(
				listTelegramTopicBindingsForChat(raw, { chatId: "c1" }),
			).toHaveLength(2);

			await enableTelegramTopicMode(raw, { chatId: "c1", userId: "u1" });
			await disableTelegramTopicMode(raw, { chatId: "c1" });
			expect(listTelegramTopicBindingsForChat(raw, { chatId: "c1" })).toEqual(
				[],
			);
		} finally {
			raw.close();
		}

		// Absent tables: silent no-op, nothing created.
		const fresh = new Database(dbPath("fresh.db"));
		try {
			await disableTelegramTopicMode(fresh, { chatId: "c1" });
			expect(topicTables(fresh)).toEqual([]);
		} finally {
			fresh.close();
		}
	});
});

describe("bindings CRUD", () => {
	async function seeded(): Promise<Database.Database> {
		const raw = await openRaw(dbPath());
		await enableTelegramTopicMode(raw, { chatId: "c1", userId: "u1" });
		seedSession(raw, "sA", "agent:main:telegram:dm:c1:11");
		seedSession(raw, "sB", "agent:main:telegram:dm:c1:22");
		seedSession(raw, "sC", "agent:main:telegram:dm:c1:33");
		return raw;
	}

	it("bind requires an existing session row (FK), upserts, and is idempotent for the same topic", async () => {
		const raw = await seeded();
		try {
			let clock = 10;
			const nowSeconds = (): number => clock;
			await bindTelegramTopic(
				raw,
				{
					chatId: "c1",
					threadId: "11",
					userId: "u1",
					sessionKey: "kA",
					sessionId: "sA",
				},
				{ nowSeconds },
			);
			clock = 20;
			// Same topic rebind: idempotent fields move, linked_at stays ORIGINAL.
			await bindTelegramTopic(
				raw,
				{
					chatId: "c1",
					threadId: "11",
					userId: "u1",
					sessionKey: "kA",
					sessionId: "sB",
					managedMode: "restored",
				},
				{ nowSeconds },
			);
			const row = getTelegramTopicBinding(raw, {
				chatId: "c1",
				threadId: "11",
			});
			expect(row).toBeDefined();
			expect(row!.session_id).toBe("sB"); // rebound to newer session
			expect(row!.managed_mode).toBe("restored");
			expect(row!.linked_at).toBe(10); // NOT excluded.linked_at
			expect(row!.updated_at).toBe(20);

			// A brand-new topic for a NEW session binds fine…
			await bindTelegramTopic(raw, {
				chatId: "c1",
				threadId: "22",
				userId: "u1",
				sessionKey: "kC",
				sessionId: "sC",
			});
			// …but sC cannot ALSO claim yet another topic.
			await expect(
				bindTelegramTopic(raw, {
					chatId: "c1",
					threadId: "33",
					userId: "u1",
					sessionKey: "kC",
					sessionId: "sC",
				}),
			).rejects.toThrow("session is already linked to another Telegram topic");
		} finally {
			raw.close();
		}
	});

	it("list returns bindings newest-first and NEVER migrates on a feature-absent store", async () => {
		const p = dbPath();
		const raw = await openRaw(p);
		try {
			expect(listTelegramTopicBindingsForChat(raw, { chatId: "c1" })).toEqual(
				[],
			);
			// hermes parity: the read must not create the tables either.
			expect(topicTables(raw)).toEqual([]);

			await enableTelegramTopicMode(raw, { chatId: "c1", userId: "u1" });
			seedSession(raw, "old");
			seedSession(raw, "new");
			let clock = 1;
			const nowSeconds = (): number => clock;
			await bindTelegramTopic(
				raw,
				{
					chatId: "c1",
					threadId: "11",
					userId: "u1",
					sessionKey: "ko",
					sessionId: "old",
				},
				{ nowSeconds },
			);
			clock = 999;
			await bindTelegramTopic(
				raw,
				{
					chatId: "c1",
					threadId: "22",
					userId: "u1",
					sessionKey: "kn",
					sessionId: "new",
				},
				{ nowSeconds },
			);
			// Other-chat rows are filtered out entirely.
			seedSession(raw, "other");
			await bindTelegramTopic(raw, {
				chatId: "c2",
				threadId: "99",
				userId: "u1",
				sessionKey: "kx",
				sessionId: "other",
			});

			const listed = listTelegramTopicBindingsForChat(raw, { chatId: "c1" });
			expect(listed.map((r) => r.thread_id)).toEqual(["22", "11"]);
			expect(listed[0]!.session_key).toBe("kn");
		} finally {
			raw.close();
		}
	});

	it("reverse lookup by session_id; linked predicate degrades to false on absent tables", async () => {
		const raw = await seeded();
		try {
			await bindTelegramTopic(raw, {
				chatId: "c1",
				threadId: "17585",
				userId: "u1",
				sessionKey: "agent:main:telegram:dm:c1:17585",
				sessionId: "sB",
			});
			const row = getTelegramTopicBindingBySession(raw, { sessionId: "sB" });
			expect(row).toBeDefined();
			expect(row!.chat_id).toBe("c1");
			expect(row!.thread_id).toBe("17585");
			expect(
				getTelegramTopicBindingBySession(raw, { sessionId: "unbound" }),
			).toBeUndefined();
			expect(isTelegramSessionLinkedToTopic(raw, { sessionId: "sB" })).toBe(
				true,
			);
			expect(isTelegramSessionLinkedToTopic(raw, { sessionId: "sA" })).toBe(
				false,
			);
		} finally {
			raw.close();
		}
	});

	it("#31501 prune: deleting the LAST binding flips the chat's mode off in the same transaction; counts returned", async () => {
		const raw = await seeded();
		try {
			await bindTelegramTopic(raw, {
				chatId: "c1",
				threadId: "11",
				userId: "u1",
				sessionKey: "kA",
				sessionId: "sA",
			});
			await bindTelegramTopic(raw, {
				chatId: "c1",
				threadId: "22",
				userId: "u1",
				sessionKey: "kB",
				sessionId: "sB",
			});

			expect(
				await deleteTelegramTopicBinding(raw, { chatId: "c1", threadId: "11" }),
			).toBe(1);
			// Still one lane left → mode stays on.
			expect(
				isTelegramTopicModeEnabled(raw, { chatId: "c1", userId: "u1" }),
			).toBe(true);

			expect(
				await deleteTelegramTopicBinding(raw, { chatId: "c1", threadId: "22" }),
			).toBe(1);
			// Last lane gone → recovery must stand down.
			expect(
				isTelegramTopicModeEnabled(raw, { chatId: "c1", userId: "u1" }),
			).toBe(false);
			expect(
				await deleteTelegramTopicBinding(raw, { chatId: "c1", threadId: "22" }),
			).toBe(0);
		} finally {
			raw.close();
		}
	});
});
