// Behavior contracts: participant-isolation flag flips re-key ONLY new
// messages — previously persisted rows keep their original session keys
// (02-session-and-state.md §4.4; 02 §13 test-matrix row "flag flipped between
// turns re-keys only NEW messages (old sessions untouched)"). Roadmap Phase 1
// required contract #4.
//
// Shape: a real temp state.db IS the history. Each arrival resolves its key
// through the ONE shared predicate via buildSessionKey; the message persists
// into the sessions row that key names. Flipping group_sessions_per_user
// between turns must move SUBSEQUENT arrivals to a different key while the
// old rows' linkage stays byte-stable — and flipping BACK must converge onto
// the ORIGINAL session again (identity is the resolved row, not the flags).

import { describe, expect, it } from "vitest";
import type Database from "better-sqlite3";

import { StateStore } from "../../pi_state/store.js";
import { makeTempDir, removeTempDir } from "../../pi_state/testing/harness.js";
import {
	buildSessionKey,
	type IsolationFlags,
	type SessionSource,
} from "./session-key.js";

const T0 = 1_750_000_000;

describe("participant-isolation flips re-key only NEW messages", () => {
	it("group flag flip moves subsequent arrivals; previously persisted rows keep their original keys; flip-back converges", async () => {
		const dir = makeTempDir("pi-gw-iso-");
		const store = await StateStore.open(`${dir}/state.db`);
		try {
			const db = store.db;
			const sender: SessionSource = {
				platform: "telegram",
				chatType: "group",
				chatId: "chat-1",
				userIdAlt: "user-9",
			};
			const isolated: IsolationFlags = {}; // defaults: per-sender isolation
			const shared: IsolationFlags = { groupSessionsPerUser: false };

			let clock = T0;

			/** Resolve-and-persist one arrival under the CURRENT policy flags. */
			const arrive = async (
				flags: IsolationFlags,
			): Promise<{ key: string; messageId: number }> => {
				clock += 1;
				const key = buildSessionKey(sender, flags);
				await store.withWrite((conn) => {
					const found = conn
						.prepare("SELECT id FROM sessions WHERE session_key = ?")
						.get(key) as { id: string } | undefined;
					let sessionId = found?.id;
					if (!sessionId) {
						sessionId = [
							"20260823_130000_i",
							String(clock).padStart(5, "0"),
						].join("");
						conn
							.prepare(
								"INSERT INTO sessions (id, source, session_key, chat_id, chat_type, user_id, started_at) VALUES (?, 'telegram', ?, 'chat-1', 'group', 'user-9', ?)",
							)
							.run(sessionId, key, clock);
					}
					const info = conn
						.prepare(
							"INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'user', ?, ?)",
						)
						.run(sessionId, ["arrival @ t=", clock].join(""), clock);
					return { key, messageId: Number(info.lastInsertRowid) };
				});
				return {
					key,
					messageId: (
						db
							.prepare("SELECT id FROM messages ORDER BY id DESC LIMIT 1")
							.get() as { id: number }
					).id,
				};
			};

			const keyOfMessage = db.prepare(
				`SELECT s.session_key AS key FROM messages m
				 JOIN sessions s ON s.id = m.session_id WHERE m.id = ?`,
			);

			// --- Phase A: isolated-per-sender (default flags) ---
			const a1 = await arrive(isolated);
			const a2 = await arrive(isolated);
			expect(a1.key).toBe("agent:main:telegram:group:chat-1:user-9");
			expect(a2.key).toBe(a1.key); // same sender → same session

			// --- Phase B: operator flips the flag → SHARED session ---
			const b1 = await arrive(shared);
			expect(b1.key).toBe("agent:main:telegram:group:chat-1");
			expect(b1.key).not.toBe(a1.key);

			// Old rows are UNTOUCHED: their stored linkage still points at the
			// original isolated session.
			expect((keyOfMessage.get(a1.messageId) as { key: string }).key).toBe(
				a1.key,
			);
			expect((keyOfMessage.get(a2.messageId) as { key: string }).key).toBe(
				a1.key,
			);

			// --- Phase C: flag flips back → continuity with the ORIGINAL row ---
			const c1 = await arrive(isolated);
			expect(c1.key).toBe(a1.key);
			expect((keyOfMessage.get(c1.messageId) as { key: string }).key).toBe(
				a1.key,
			);

			// Exactly TWO rows exist for this chat (isolated + shared): the
			// flip created no fork spam and reused the original session on
			// return rather than minting a third identity.
			const rowsForChat = db
				.prepare("SELECT COUNT(*) AS n FROM sessions WHERE chat_id = 'chat-1'")
				.get() as { n: number };
			expect(rowsForChat.n).toBe(2);

			// Message-to-session distribution proves the re-key boundary:
			// two arrivals pre-flip + one post-flip-back in session A, exactly
			// the mid-window arrival in session B.
			const counts = db
				.prepare(
					`SELECT s.session_key AS key, COUNT(m.id) AS msgs FROM sessions s
					 LEFT JOIN messages m ON m.session_id = s.id
					 WHERE s.chat_id = 'chat-1' GROUP BY s.id`,
				)
				.all() as Array<{ key: string; msgs: number }>;
			expect(counts).toContainEqual({ key: a1.key, msgs: 3 });
			expect(counts).toContainEqual({ key: b1.key, msgs: 1 });
		} finally {
			await store.close();
			removeTempDir(dir);
		}
	});

	it("thread flag flip behaves identically at the persistence level (shared thread → isolated threads)", async () => {
		const dir = makeTempDir("pi-gw-iso-t-");
		const store = await StateStore.open(`${dir}/state.db`);
		try {
			const db = store.db;
			const arrival: SessionSource = {
				platform: "slack",
				chatType: "thread",
				scopeId: "W1",
				chatId: "C1",
				threadId: "T9",
				userId: "U7",
			};
			const sharedThread: IsolationFlags = {}; // default: threads shared
			const isolatedThread: IsolationFlags = { threadSessionsPerUser: true };

			const keyFor = (flags: IsolationFlags) => buildSessionKey(arrival, flags);

			const beforeFlip = keyFor(sharedThread);
			const afterFlip = keyFor(isolatedThread);
			expect(beforeFlip).toBe("agent:main:slack:thread:W1:C1:T9");
			expect(afterFlip).toBe("agent:main:slack:thread:W1:C1:T9:U7");
			expect(afterFlip.startsWith(beforeFlip)).toBe(true); // same chat, +slot

			// Persist one message per side; each keeps its own key.
			const persist = (key: string, label: string): number => {
				let sessionId = (
					db
						.prepare("SELECT id FROM sessions WHERE session_key = ?")
						.get(key) as { id: string } | undefined
				)?.id;
				if (!sessionId) {
					sessionId = ["20260823_140000_t", label].join("");
					db.prepare(
						"INSERT INTO sessions (id, source, session_key, started_at) VALUES (?, 'slack', ?, ?)",
					).run(sessionId, key, T0);
				}
				return Number(
					db
						.prepare(
							"INSERT INTO messages (session_id, role, content, timestamp) VALUES (?, 'user', ?, ?)",
						)
						.run(sessionId, label, T0).lastInsertRowid,
				);
			};

			const preFlipMsg = persist(beforeFlip, "pre");
			const postFlipMsg = persist(afterFlip, "post");
			const keyOf = db.prepare(
				`SELECT s.session_key AS key FROM messages m JOIN sessions s ON s.id = m.session_id WHERE m.id = ?`,
			);
			expect((keyOf.get(preFlipMsg) as { key: string }).key).toBe(beforeFlip);
			expect((keyOf.get(postFlipMsg) as { key: string }).key).toBe(afterFlip);
		} finally {
			await store.close();
			removeTempDir(dir);
		}
	});
});
