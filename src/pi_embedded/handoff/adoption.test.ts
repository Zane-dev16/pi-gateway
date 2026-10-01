// adoption.test.ts — adoption contracts from a REAL session-file
// fixture (DEC-079): the host loader parses the file, adoptable messages
// land as the gateway store's own rows under the host id, the drive lock is
// held on return, and a contended lock yields readonly-plus-takeover with
// zero rows written. The fixture is committed; live files are never touched.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { StateStore } from "../../pi_state/index.js";
import { adoptSessionFile, type AdoptionStore } from "./adoption.js";
import { isSessionDriveLocked, SessionDriverLock } from "./session-lock.js";

const FIXTURE = fileURLToPath(
	new URL("./testing/adoption-fixture.jsonl", import.meta.url),
);

let dir: string;
let state: StateStore;
let store: AdoptionStore;

beforeEach(async () => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-adoption-"));
	state = await StateStore.open(join(dir, "state.db"));
	store = {
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
});

afterEach(async () => {
	await state.close();
	rmSync(dir, { recursive: true, force: true });
});

function roles(sessionId: string): string[] {
	return state.db
		.prepare("SELECT role FROM messages WHERE session_id = ? ORDER BY id")
		.all(sessionId)
		.map((r) => (r as { role: string }).role);
}

describe("adoptSessionFile", () => {
	it("loads fixture entries into gateway rows under the host id, lock held", async () => {
		const result = await adoptSessionFile({
			sessionFile: FIXTURE,
			sessionId: "fixture-adopt-1",
			lockDir: dir,
			store,
		});
		try {
			expect(result.kind).toBe("adopted-live-under-lock");
			if (result.kind !== "adopted-live-under-lock") return;
			expect(result.entryCount).toBe(4);
			expect(result.lock.isHeld()).toBe(true);
			expect(roles("fixture-adopt-1")).toEqual([
				"user",
				"assistant",
				"user",
				"assistant",
			]);
			const texts = state.db
				.prepare(
					"SELECT content FROM messages WHERE session_id = ? ORDER BY id",
				)
				.all("fixture-adopt-1")
				.map((r) => (r as { content: string }).content);
			expect(texts[0]).toContain("deploy checklist");
			expect(texts[2]).toBe("plain string content still adopts");
		} finally {
			if (result.kind === "adopted-live-under-lock") result.lock.release();
		}
	});

	it("repeat adoption is idempotent — no duplicate rows", async () => {
		const first = await adoptSessionFile({
			sessionFile: FIXTURE,
			sessionId: "fixture-adopt-1",
			lockDir: dir,
			store,
		});
		if (first.kind !== "adopted-live-under-lock")
			throw new Error("first adopt lost");
		first.lock.release();
		const second = await adoptSessionFile({
			sessionFile: FIXTURE,
			sessionId: "fixture-adopt-1",
			lockDir: dir,
			store,
		});
		try {
			expect(second.kind).toBe("adopted-live-under-lock");
			expect(roles("fixture-adopt-1")).toHaveLength(4);
		} finally {
			if (second.kind === "adopted-live-under-lock") second.lock.release();
		}
	});

	it("lock contention yields readonly-plus-takeover with zero rows written", async () => {
		const holder = new SessionDriverLock(dir, "fixture-adopt-1");
		expect(holder.acquire()).toBe(true);
		try {
			const result = await adoptSessionFile({
				sessionFile: FIXTURE,
				sessionId: "fixture-adopt-1",
				lockDir: dir,
				store,
			});
			expect(result).toEqual({
				kind: "readonly-plus-takeover",
				sessionId: "fixture-adopt-1",
				sourceFile: FIXTURE,
				reason: "drive-lock-held",
			});
			expect(roles("fixture-adopt-1")).toEqual([]);
		} finally {
			holder.release();
		}
	});

	it("concurrent adopters produce one winner and no partial copy", async () => {
		const [a, b] = await Promise.all([
			adoptSessionFile({
				sessionFile: FIXTURE,
				sessionId: "fixture-adopt-1",
				lockDir: dir,
				store,
			}),
			adoptSessionFile({
				sessionFile: FIXTURE,
				sessionId: "fixture-adopt-1",
				lockDir: dir,
				store,
			}),
		]);
		const kinds = [a.kind, b.kind].sort();
		expect(kinds).toEqual([
			"adopted-live-under-lock",
			"readonly-plus-takeover",
		]);
		expect(roles("fixture-adopt-1")).toHaveLength(4);
		for (const r of [a, b]) {
			if (r.kind === "adopted-live-under-lock") r.lock.release();
		}
	});

	it("header id mismatch throws loudly and frees the lock", async () => {
		await expect(
			adoptSessionFile({
				sessionFile: FIXTURE,
				sessionId: "some-other-id",
				lockDir: dir,
				store,
			}),
		).rejects.toThrow("does not match");
		expect(isSessionDriveLocked(dir, "some-other-id")).toBe(false);
		expect(roles("some-other-id")).toEqual([]);
	});

	it("a missing file throws loudly and frees the lock", async () => {
		await expect(
			adoptSessionFile({
				sessionFile: join(dir, "nope.jsonl"),
				sessionId: "missing-1",
				lockDir: dir,
				store,
			}),
		).rejects.toThrow();
		expect(isSessionDriveLocked(dir, "missing-1")).toBe(false);
	});
});
