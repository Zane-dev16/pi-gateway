// runner-cwd.test.ts — DEC-079 runner cwd plumbing behavior contracts.
//
// A turn carrying the chat's bound path builds (or reuses) the host session
// under THAT root; the root persists on sessions.cwd so later cwd-less turns
// (compact/model/export included) resolve it back; sessions never given a
// path keep running under process.cwd() untouched.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { createRunnerHarness } from "./testing/runner-harness.js";
import { fauxAssistantMessage } from "./testing/faux-model.js";

let boundA: string;
let boundB: string;
let dirs: string[];

beforeEach(() => {
	boundA = mkdtempSync(join(tmpdir(), "pi-gw-cwd-a-"));
	boundB = mkdtempSync(join(tmpdir(), "pi-gw-cwd-b-"));
	dirs = [boundA, boundB];
});

afterEach(() => {
	for (const dir of dirs) rmSync(dir, { recursive: true, force: true });
});

function storedCwd(
	db: { prepare(sql: string): { get(...args: unknown[]): unknown } },
	sessionId: string,
): string | null {
	const row = db
		.prepare("SELECT cwd FROM sessions WHERE id = ? LIMIT 1")
		.get(sessionId) as { cwd: string | null } | undefined;
	return row?.cwd ?? null;
}

describe("runner cwd plumbing (DEC-079)", () => {
	it("a turn under a bound path builds the host session under that root", async () => {
		const h = await createRunnerHarness();
		try {
			h.ensureSession("bound-sess");
			h.faux.setResponses([fauxAssistantMessage("rooted reply")]);
			const outcome = await h.runner.handleTurn({
				sessionId: "bound-sess",
				routingKey: "agent:main:test:dm:c1",
				text: "hello from the bound chat",
				cwd: boundA,
			});
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("rooted reply");
			expect(h.runner.cachedSessionCwd("bound-sess")).toBe(boundA);
			expect(storedCwd(h.store.db, "bound-sess")).toBe(boundA);
		} finally {
			await h.close();
		}
	});

	it("a follow-up turn without cwd reuses the bound root via sessions.cwd", async () => {
		const h = await createRunnerHarness();
		try {
			h.ensureSession("reuse-sess");
			h.faux.setResponses([
				fauxAssistantMessage("first"),
				fauxAssistantMessage("second"),
			]);
			await h.runner.handleTurn({
				sessionId: "reuse-sess",
				routingKey: "rk",
				text: "one",
				cwd: boundA,
			});
			const outcome = await h.runner.handleTurn({
				sessionId: "reuse-sess",
				routingKey: "rk",
				text: "two",
			});
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("second");
			expect(h.runner.cachedSessionCwd("reuse-sess")).toBe(boundA);
			expect(h.runner.cacheStats.entries).toBe(1);
		} finally {
			await h.close();
		}
	});

	it("sessions under process cwd are unaffected by a bound sibling", async () => {
		const h = await createRunnerHarness();
		try {
			h.ensureSession("bound-sess");
			h.ensureSession("plain-sess");
			h.faux.setResponses([
				fauxAssistantMessage("bound reply"),
				fauxAssistantMessage("plain reply"),
			]);
			await h.runner.handleTurn({
				sessionId: "bound-sess",
				routingKey: "rk",
				text: "bound ask",
				cwd: boundA,
			});
			const outcome = await h.runner.handleTurn({
				sessionId: "plain-sess",
				routingKey: "rk",
				text: "plain ask",
			});
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("plain reply");
			expect(h.runner.cachedSessionCwd("plain-sess")).toBe(process.cwd());
			expect(storedCwd(h.store.db, "plain-sess")).toBeNull();
			expect(h.runner.cachedSessionCwd("bound-sess")).toBe(boundA);
			expect(h.runner.cacheStats.entries).toBe(2);
		} finally {
			await h.close();
		}
	});

	it("a re-rooted chat rebuilds under the new root and restamps sessions.cwd", async () => {
		const h = await createRunnerHarness();
		try {
			h.ensureSession("moving-sess");
			h.faux.setResponses([
				fauxAssistantMessage("at a"),
				fauxAssistantMessage("at b"),
			]);
			await h.runner.handleTurn({
				sessionId: "moving-sess",
				routingKey: "rk",
				text: "ask at a",
				cwd: boundA,
			});
			expect(h.runner.cachedSessionCwd("moving-sess")).toBe(boundA);
			const outcome = await h.runner.handleTurn({
				sessionId: "moving-sess",
				routingKey: "rk",
				text: "ask at b",
				cwd: boundB,
			});
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("at b");
			expect(h.runner.cachedSessionCwd("moving-sess")).toBe(boundB);
			expect(storedCwd(h.store.db, "moving-sess")).toBe(boundB);
			// Drop-plus-rebuild leaks no entry; history still replays.
			expect(h.runner.cacheStats.entries).toBe(1);
			expect(
				h.store.listMessages("moving-sess").map((r) => r.content),
			).toEqual(["ask at a", "at a", "ask at b", "at b"]);
		} finally {
			await h.close();
		}
	});
});
