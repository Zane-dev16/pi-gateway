// Behavior contracts: byte-exact api_content sidecar discipline
// (02-session-and-state.md §7; DEC-007; roadmap Phase 1 list: "replay
// fidelity round-trips api_content byte-exactly including rewrite-drops-
// sidecar"). Mutation rows over hostile Unicode corpora; no snapshots.

import Database from "better-sqlite3";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { substituteApiContent, scrubSurrogates } from "./messages.js";
import { StateStore } from "./store.js";
import { makeTempDir, removeTempDir } from "./testing/harness.js";

let dir: string;

beforeEach(() => {
	dir = makeTempDir("pi-gw-replay-");
});

afterEach(() => {
	removeTempDir(dir);
});

function dbPath(): string {
	return `${dir}/state.db`;
}

function expectByteExact(written: string, stored: string): void {
	const want = Buffer.from(written, "utf8");
	const got = Buffer.from(stored, "utf8");
	expect(got.equals(want)).toBe(true);
	expect(got.byteLength).toBe(want.byteLength);
	expect(stored.length).toBe(written.length); // UTF-16 code-unit identity too
}

async function openSeeded(): Promise<StateStore> {
	const store = await StateStore.open(dbPath());
	store.db
		.prepare(
			"INSERT INTO sessions (id, source, started_at) VALUES ('s1', 'cli', 1)",
		)
		.run();
	return store;
}

describe("02 §7.1 replay fidelity — persist-what-you-send", () => {
	it("multi-byte emoji, combining marks, astral chars, CJK/RTL mix, NUL bytes survive write→read BYTE-EXACTLY", async () => {
		const store = await openSeeded();
		try {
			// Hostile corpus: ZWJ families, flags, NFD forms kept UNNORMALIZED,
			// max scalar values, CJK+RTL+Indic+Thai, embedded NUL + control bytes.
			const values = [
				"hello 🚀🎉 — 👨‍👩‍👧‍👦 👨‍👩‍👦 🇯🇵🇩🇪 café",
				"combining: e\u0301\u0302 a\u0328 o\u0308 f\u0300 q̇", // NFC vs NFD matters below
				"astral edge: \u{10FFFF}\u{10FFFE}\u{10000}",
				"scripts: 中文漢字 日本語 العربية الْعَرَبِيَّة עברית हिन्दी ไทย",
				"binary-ish: a\u0000b\u0007c\td\ne 🚀",
				"kitchen sink: 🚀café 中文 \u{10FFFF} e\u0301 مرحبا 👨‍👩‍👧‍👦 🇯🇵 — ✅",
			];
			const ids: number[] = [];
			for (const v of values) {
				ids.push(
					await store.appendMessage({
						sessionId: "s1",
						role: "user",
						content: v,
						apiContent: v,
					}),
				);
			}
			// Read back through an INDEPENDENT connection (cross-connection visibility).
			const reader = new Database(dbPath());
			try {
				reader.pragma("busy_timeout = 5000");
				for (let i = 0; i < values.length; i++) {
					const row = reader
						.prepare("SELECT content, api_content FROM messages WHERE id = ?")
						.get(ids[i]!) as { content: string; api_content: string };
					expectByteExact(values[i]!, row.api_content); // persist-what-you-send
					expectByteExact(values[i]!, row.content);
				}
				// And via the store's fixed projection.
				expectByteExact(values[3]!, store.getApiContent(ids[3]!)!);
			} finally {
				reader.close();
			}
		} finally {
			await store.close();
		}
	});

	it("NFC and NFD forms stay DISTINCT — no normalization anywhere in the persist path", async () => {
		const store = await openSeeded();
		try {
			const nfc = "é"; // U+00E9 precomposed
			const nfd = "e\u0301"; // decomposed — DIFFERENT bytes
			expect(Buffer.byteLength(nfc, "utf8")).not.toBe(
				Buffer.byteLength(nfd, "utf8"),
			);
			const idA = await store.appendMessage({
				sessionId: "s1",
				role: "user",
				apiContent: nfc,
			});
			const idB = await store.appendMessage({
				sessionId: "s1",
				role: "user",
				apiContent: nfd,
			});
			expectByteExact(nfc, store.getApiContent(idA)!);
			expectByteExact(nfd, store.getApiContent(idB)!);
			expect(store.getApiContent(idA)).not.toBe(store.getApiContent(idB));
		} finally {
			await store.close();
		}
	});

	it("~200KB mixed-script value round-trips byte-exactly across connections", async () => {
		const store = await openSeeded();
		try {
			const unit = "ab🚀中\u0301العربية👨‍👩‍👧‍👦\u{10FFFF}x";
			const big = unit.repeat(6000);
			expect(Buffer.byteLength(big, "utf8")).toBeGreaterThan(100_000);
			const id = await store.appendMessage({
				sessionId: "s1",
				role: "assistant",
				apiContent: big,
			});
			const reader = new Database(dbPath());
			try {
				reader.pragma("busy_timeout = 5000");
				const row = reader
					.prepare("SELECT api_content FROM messages WHERE id = ?")
					.get(id) as { api_content: string };
				expectByteExact(big, row.api_content);
			} finally {
				reader.close();
			}
		} finally {
			await store.close();
		}
	});

	it("lone surrogates map to the DOCUMENTED U+FFFD form (explicit scrub, never truncation)", () => {
		// Driver boundary mapping made explicit by scrubSurrogates (parity of
		// hermes _scrub_surrogates): paired pairs survive, singles → U+FFFD.
		expect(scrubSurrogates("\uD800abc\uDFFF")).toBe("\uFFFDabc\uFFFD");
		expect(scrubSurrogates("🚀 ok 👨‍👩‍👧")).toBe("🚀 ok 👨‍👩‍👧"); // valid text untouched
		const scrubbed = scrubSurrogates("x\uD800y");
		expectByteExact("x\uFFFDy", scrubbed);
		expect(Buffer.byteLength(scrubbed, "utf8")).toBe(5);
	});

	it("sidecar survives committed lifecycle with rolled-back neighbor leaving ZERO residue", async () => {
		const store = await openSeeded();
		try {
			await store.withWrite((conn) => {
				conn
					.prepare(
						"INSERT INTO messages (session_id, role, api_content, timestamp) VALUES ('s1','user',?,1)",
					)
					.run("kept 🚀 \u{10FFFF}");
			});
			await store
				.withWrite((conn) => {
					conn
						.prepare(
							"INSERT INTO messages (session_id, role, api_content, timestamp) VALUES ('s1','assistant',?,2)",
						)
						.run("rolled-back \uD83D\uDE00-lone-\uD800");
					conn.exec("SELECT 1"); // touch inside tx before rollback
					throw Object.assign(new Error("force rollback"), {
						code: "TEST_ROLLBACK",
					});
				})
				.catch((err: unknown) => {
					expect((err as Error).message).toMatch(/force rollback/);
				});
			const count = store.db
				.prepare("SELECT COUNT(*) AS n FROM messages")
				.get() as { n: number };
			expect(count.n).toBe(1); // zero residue from the rolled-back neighbor
			expectByteExact("kept 🚀 \u{10FFFF}", store.getApiContent(1)!);
			expect(store.db.pragma("integrity_check", { simple: true })).toBe("ok");
		} finally {
			await store.close();
		}
	});
});

describe("sidecar lifecycle — backfill stamp, drop-on-rewrite, substitution", () => {
	it("setLatestUserApiContent stamps ONLY the newest ACTIVE user row; guard mismatch writes nothing", async () => {
		const store = await openSeeded();
		try {
			await store.appendMessage({
				sessionId: "s1",
				role: "user",
				content: "older user msg",
			});
			await store.appendMessage({
				sessionId: "s1",
				role: "assistant",
				content: "reply",
			});
			const newestUser = await store.appendMessage({
				sessionId: "s1",
				role: "user",
				content: "newest user msg",
			});

			const stamped = await store.setLatestUserApiContent(
				"s1",
				"wire bytes 🚀",
				"newest user msg",
			);
			expect(stamped).toBe(1);
			expect(store.getApiContent(newestUser)).toBe("wire bytes 🚀");

			// Defensive guard: wrong expected content ⇒ nothing written anywhere.
			const stamped2 = await store.setLatestUserApiContent(
				"s1",
				"other",
				"not-the-tail",
			);
			expect(stamped2).toBe(0);
			expect(store.getApiContent(newestUser)).toBe("wire bytes 🚀");

			// Inactive user row is never stamped: compact it away first.
			store.db
				.prepare("UPDATE messages SET active = 0 WHERE id = ?")
				.run(newestUser);
			await store.appendMessage({
				sessionId: "s1",
				role: "user",
				content: "post-compaction turn",
			});
			const stamped3 = await store.setLatestUserApiContent(
				"s1",
				"fresh bytes",
				"post-compaction turn",
			);
			expect(stamped3).toBe(1);
		} finally {
			await store.close();
		}
	});

	it("rewrite drops the sidecar (drop_stale_api_content): replay never resends removed content", async () => {
		const store = await openSeeded();
		try {
			const id = await store.appendMessage({
				sessionId: "s1",
				role: "user",
				content: "cleaned",
				apiContent: "with [image data] attached 🚀",
			});
			expect(store.getApiContent(id)).toBe("with [image data] attached 🚀");
			await store.dropStaleApiContent(id);
			expect(store.getApiContent(id)).toBeNull(); // cost: one cache-boundary miss
			// Display content survives untouched.
			expect(store.getMessage(id)?.content).toBe("cleaned");
		} finally {
			await store.close();
		}
	});

	it("substituteApiContent prefers the sidecar verbatim at every build site", () => {
		expect(
			substituteApiContent({ content: "display", api_content: "wire 🚀" }),
		).toBe("wire 🚀");
		// No sidecar ⇒ clean display content passes through.
		expect(
			substituteApiContent({ content: "display", api_content: null }),
		).toBe("display");
		expect(
			substituteApiContent({ content: null, api_content: null }),
		).toBeNull();
	});
});

describe("02 §7.3 replay-read path", () => {
	async function openLineage(): Promise<StateStore> {
		const store = await StateStore.open(dbPath());
		const seedSession = (
			id: string,
			parent: string | null,
			endReason: string | null,
		): void => {
			store.db
				.prepare(
					"INSERT INTO sessions (id, parent_session_id, source, started_at, end_reason) VALUES (?, ?, 'cli', 1, ?)",
				)
				.run(id, parent, endReason);
		};
		seedSession("root", null, "compression");
		seedSession("child", "root", null);
		// Deliberately-ended ancestor must NOT contribute history.
		seedSession("reset-parent", null, "session_reset");
		seedSession("reset-child", "reset-parent", null);
		return store;
	}

	it("replay reads self + compression ancestors in insertion order with sidecars verbatim", async () => {
		const store = await openLineage();
		try {
			await store.appendMessage({
				sessionId: "root",
				role: "user",
				content: "q1",
				apiContent: "WIRE-q1",
			});
			await store.appendMessage({
				sessionId: "root",
				role: "assistant",
				content: "a1",
				apiContent: "WIRE-a1",
			});
			await store.appendMessage({
				sessionId: "child",
				role: "user",
				content: "q2",
				apiContent: "WIRE-q2",
			});

			const replay = store.readReplayMessages("child");
			expect(replay.map((r) => r.api_content)).toEqual([
				"WIRE-q1",
				"WIRE-a1",
				"WIRE-q2",
			]);
			expect(replay.map((r) => r.id)).toEqual(
				[...replay.map((r) => r.id)].sort((a, b) => a - b),
			);

			// Deliberate reset ancestor contributes NOTHING to its child's replay.
			await store.appendMessage({
				sessionId: "reset-child",
				role: "user",
				content: "fresh",
			});
			expect(store.readReplayMessages("reset-child")).toHaveLength(1);
		} finally {
			await store.close();
		}
	});

	it("active-only head: compacted prefix excluded; dedupeReplayedUserRows keeps the LAST duplicate", async () => {
		const store = await openLineage();
		try {
			await store.appendMessage({
				sessionId: "root",
				role: "user",
				content: "dup question",
			});
			await store.appendMessage({
				sessionId: "root",
				role: "user",
				content: "dup question",
			});
			const tailClone = await store.appendMessage({
				sessionId: "child",
				role: "user",
				content: "dup question",
			});

			// Compacted prefix (active=0) drops out of the replay head…
			store.db
				.prepare(
					"UPDATE messages SET active = 0 WHERE session_id = 'root' AND id < 2",
				)
				.run();
			const activeRows = store.readReplayMessages("child");
			expect(activeRows.map((r) => r.session_id)).toContain("root");

			// …and the cloned-user-row defense keeps exactly ONE copy (the newest).
			const deduped = store.readReplayMessages("child", {
				dedupeReplayedUserRows: true,
			});
			const dupRows = deduped.filter((r) => r.content === "dup question");
			expect(dupRows).toHaveLength(1);
			expect(dupRows[0]!.id).toBe(tailClone); // last occurrence wins
		} finally {
			await store.close();
		}
	});
});
