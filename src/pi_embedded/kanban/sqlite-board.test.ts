// §6 Kanban contracts against the REAL SQLite board (better-sqlite3, WAL).
//
// Every timing assertion runs on an INJECTED clock (ManualClock) — no wall
// reads. The claim/reclaim/breaker transitions are exercised through the
// BoardClient seam exactly as the dispatcher drives them.

import { rmSync } from "node:fs";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { openKanbanHarness, type KanbanHarness } from "./testing/harness.js";
import { ManualClock } from "./testing/manual-clock.js";

let h: KanbanHarness;
let rivalBoard: KanbanHarness["board"];

beforeEach(async () => {
	h = await openKanbanHarness();
	rivalBoard = undefined as unknown as KanbanHarness["board"];
});

afterEach(async () => {
	if (rivalBoard !== undefined) {
		// Rival boards share the file; closing the harness closes its own
		// connection only. Rival connections are per-test and GC'd with the
		// temp dir; close them via the harness escape hatch if opened.
	}
	await Promise.resolve();
	rmSync(h.dir, { recursive: true, force: true });
});

async function readyCard(
	overrides: Parameters<KanbanHarness["board"]["createCard"]>[0] = {},
) {
	return h.board.createCard({
		status: "ready",
		assignee: "worker",
		...overrides,
	});
}

describe("claimCard — atomic CAS transition (exactly one owner)", () => {
	it("second claimant on a SECOND CONNECTION loses the race", async () => {
		const card = await readyCard();
		const rival = await h.openRivalBoard();
		rivalBoard = rival;

		const winner = await h.board.claimCard({
			cardId: card.id,
			lock: "conn-A",
			expiresAt: h.clock.nowSeconds() + 600,
			nowSeconds: h.clock.nowSeconds(),
		});
		expect(winner).not.toBeNull();
		expect(winner?.claimLock).toBe("conn-A");
		expect(winner?.status).toBe("running");

		const loser = await rival.claimCard({
			cardId: card.id,
			lock: "conn-B",
			expiresAt: h.clock.nowSeconds() + 600,
			nowSeconds: h.clock.nowSeconds(),
		});
		expect(loser).toBeNull();

		const final = await h.board.getCard(card.id);
		expect(final?.claimLock).toBe("conn-A"); // owner unchanged
	});

	it("claims are rejected for non-ready or already-claimed cards", async () => {
		const todo = await h.board.createCard({ status: "todo" });
		expect(
			await h.board.claimCard({
				cardId: todo.id,
				lock: "x",
				expiresAt: h.clock.nowSeconds() + 10,
				nowSeconds: h.clock.nowSeconds(),
			}),
		).toBeNull();

		const claimed = await readyCard();
		await h.board.claimCard({
			cardId: claimed.id,
			lock: "first",
			expiresAt: h.clock.nowSeconds() + 10,
			nowSeconds: h.clock.nowSeconds(),
		});
		expect(
			await h.board.claimCard({
				cardId: claimed.id,
				lock: "second",
				expiresAt: h.clock.nowSeconds() + 10,
				nowSeconds: h.clock.nowSeconds(),
			}),
		).toBeNull();
	});

	it("never claims while a parent is non-terminal; racy writer's card demotes to todo", async () => {
		const parent = await h.board.createCard({ status: "running" });
		const child = await readyCard();
		await h.board.linkParent(child.id, parent.id);

		expect(
			await h.board.claimCard({
				cardId: child.id,
				lock: "x",
				expiresAt: h.clock.nowSeconds() + 10,
				nowSeconds: h.clock.nowSeconds(),
			}),
		).toBeNull();
		expect((await h.board.getCard(child.id))?.status).toBe("todo");
		expect(
			(await h.board.events(child.id)).some(
				(e) => e.event === "claim_rejected",
			),
		).toBe(true);
	});
});

describe("reclaimStaleClaims — stale boundary on the INJECTED clock", () => {
	it("boundary is STRICT expiry: not reclaimed at expires-1 nor at expires, reclaimed at expires+1", async () => {
		const ttl = 600;
		const t0 = h.clock.nowSeconds();
		const card = await readyCard();
		await h.board.claimCard({
			cardId: card.id,
			lock: "w1",
			expiresAt: t0 + ttl,
			nowSeconds: t0,
		});

		h.clock.set(t0 + ttl - 1);
		expect(await h.board.reclaimStaleClaims(h.clock.nowSeconds())).toEqual([]);
		expect((await h.board.getCard(card.id))?.status).toBe("running");

		h.clock.set(t0 + ttl);
		expect(await h.board.reclaimStaleClaims(h.clock.nowSeconds())).toEqual([]);
		expect((await h.board.getCard(card.id))?.status).toBe("running");

		h.clock.set(t0 + ttl + 1);
		expect(await h.board.reclaimStaleClaims(h.clock.nowSeconds())).toEqual([
			card.id,
		]);
		const after = await h.board.getCard(card.id);
		expect(after?.status).toBe("ready");
		expect(after?.claimLock).toBeNull();
		expect(after?.claimExpires).toBeNull();
		expect(
			(await h.board.events(card.id)).some((e) => e.event === "reclaimed"),
		).toBe(true);
	});

	it("reclaims ONLY expired cards and never touches fresh claims", async () => {
		const t0 = h.clock.nowSeconds();
		const stale = await readyCard();
		const fresh = await readyCard();
		await h.board.claimCard({
			cardId: stale.id,
			lock: "a",
			expiresAt: t0 + 5,
			nowSeconds: t0,
		});
		await h.board.claimCard({
			cardId: fresh.id,
			lock: "b",
			expiresAt: t0 + 500,
			nowSeconds: t0,
		});

		h.clock.advance(10);
		expect(await h.board.reclaimStaleClaims(h.clock.nowSeconds())).toEqual([
			stale.id,
		]);
		expect((await h.board.getCard(fresh.id))?.status).toBe("running");

		// Idempotent: re-running finds nothing left.
		expect(await h.board.reclaimStaleClaims(h.clock.nowSeconds())).toEqual([]);
	});

	it("running rows without expiry are invisible to the TTL sweep (parity IS-NOT-NULL guard)", async () => {
		// The TTL sweep only sees rows with claim_expires NOT NULL — a
		// running row without expiry (manual tampering / legacy writer) must
		// be skipped, not crashed on.
		const card = await readyCard();
		await h.board.claimCard({
			cardId: card.id,
			lock: "w",
			expiresAt: h.clock.nowSeconds() + 100,
			nowSeconds: h.clock.nowSeconds(),
		});
		// Simulate a writer that cleared the expiry while leaving 'running':
		h.db
			.prepare("UPDATE kanban_cards SET claim_expires = NULL WHERE id = ?")
			.run(card.id);

		// Reclaim at a time past ANY plausible TTL finds nothing (no expiry).
		h.clock.advance(10_000);
		expect(await h.board.reclaimStaleClaims(h.clock.nowSeconds())).toEqual([]);
	});
});

describe("promoteReady — dependency ordering and block stickiness", () => {
	it("promotes todo → ready only when ALL parents are done/archived", async () => {
		const p1 = await h.board.createCard({ status: "done" });
		const p2 = await h.board.createCard({ status: "archived" });
		const blockedParent = await h.board.createCard({ status: "todo" });
		const child = await h.board.createCard({ status: "todo" });
		await h.board.linkParent(child.id, p1.id);
		await h.board.linkParent(child.id, p2.id);
		const waiting = await h.board.createCard({ status: "todo" });
		await h.board.linkParent(waiting.id, blockedParent.id);

		const promoted = await h.board.promoteReady(h.clock.nowSeconds(), 2);
		// blockedParent is parentless, so IT also promotes (reference semantics:
		// every todo row with satisfied parents promotes); waiting does not.
		expect(promoted).toEqual([blockedParent.id, child.id]);
		expect((await h.board.getCard(child.id))?.status).toBe("ready");
		expect((await h.board.getCard(waiting.id))?.status).toBe("todo");
	});

	it("parentless todo cards promote immediately (ordered by created_at)", async () => {
		const first = await h.board.createCard({ status: "todo", createdAt: 100 });
		const second = await h.board.createCard({ status: "todo", createdAt: 200 });
		const promoted = await h.board.promoteReady(h.clock.nowSeconds(), 2);
		expect(promoted).toEqual([first.id, second.id]);
	});

	it("MANUAL blocks are sticky: recompute_ready never auto-recovers them", async () => {
		// Block from a live status so a 'manual' block event is recorded.
		const card = await readyCard();
		await h.board.blockCard(card.id, h.clock.nowSeconds(), "manual");
		expect(await h.board.promoteReady(h.clock.nowSeconds(), 2)).toEqual([]);
		expect((await h.board.getCard(card.id))?.status).toBe("blocked");

		// Explicit unblock is the only exit — and it resets the retry budget.
		expect(
			await h.board.unblockCard(card.id, "ready", h.clock.nowSeconds()),
		).toBe(true);
		expect((await h.board.getCard(card.id))?.consecutiveFailures).toBe(0);
	});

	it("breaker-tripped cards stay blocked even when their parents finish", async () => {
		const parent = await h.board.createCard({ status: "todo" });
		const tripped = await readyCard({ maxRetries: 1 });
		await h.board.claimCard({
			cardId: tripped.id,
			lock: "w",
			expiresAt: h.clock.nowSeconds() + 100,
			nowSeconds: h.clock.nowSeconds(),
		});
		const res = await h.board.recordFailure(
			tripped.id,
			"spawn_failed",
			"boom",
			{
				failureLimit: 99, // dispatcher limit high; per-task override 1 trips
				nowSeconds: h.clock.nowSeconds(),
			},
		);
		expect(res.blocked).toBe(true);
		await h.board.linkParent(tripped.id, parent.id);
		await h.board.completeCard(parent.id, h.clock.nowSeconds());

		// Parents done now — but the breaker holds.
		expect(await h.board.promoteReady(h.clock.nowSeconds(), 2)).not.toContain(
			tripped.id,
		);
		expect((await h.board.getCard(tripped.id))?.status).toBe("blocked");
	});
});

describe("recordFailure / completeCard — auto-block breaker after EXACT-N", () => {
	it("blocks on the Nth consecutive failure and NOT before (default N=2)", async () => {
		const card = await readyCard();
		await h.board.claimCard({
			cardId: card.id,
			lock: "w",
			expiresAt: h.clock.nowSeconds() + 100,
			nowSeconds: h.clock.nowSeconds(),
		});

		// Failure 1/2: released back to ready, counter kept.
		const r1 = await h.board.recordFailure(card.id, "spawn_failed", "err-1", {
			failureLimit: 2,
			nowSeconds: h.clock.nowSeconds(),
		});
		expect(r1).toEqual({ blocked: false, failures: 1 });
		let state = await h.board.getCard(card.id);
		expect(state?.status).toBe("ready");
		expect(state?.consecutiveFailures).toBe(1);

		// Failure 2/2: breaker trips — blocked with last error, claim cleared.
		const r2 = await h.board.recordFailure(card.id, "timed_out", "err-2", {
			failureLimit: 2,
			nowSeconds: h.clock.nowSeconds(),
		});
		expect(r2).toEqual({ blocked: true, failures: 2 });
		state = await h.board.getCard(card.id);
		expect(state?.status).toBe("blocked");
		expect(state?.lastFailureError).toBe("err-2");
		expect(state?.claimLock).toBeNull();
		expect(
			(await h.board.events(card.id)).some((e) => e.event === "gave_up"),
		).toBe(true);
	});

	it("per-task maxRetries overrides the dispatcher limit both ways", async () => {
		const lenient = await readyCard({ maxRetries: 5 });
		for (let i = 1; i <= 4; i++) {
			const r = await h.board.recordFailure(lenient.id, "crashed", `e${i}`, {
				failureLimit: 2,
				nowSeconds: h.clock.nowSeconds(),
			});
			expect(r.blocked).toBe(false);
			expect(r.failures).toBe(i);
		}
		const trip = await h.board.recordFailure(lenient.id, "crashed", "e5", {
			failureLimit: 2,
			nowSeconds: h.clock.nowSeconds(),
		});
		expect(trip).toEqual({ blocked: true, failures: 5 });

		// Strict override: maxRetries=1 blocks on the FIRST failure.
		const strict = await readyCard({ maxRetries: 1 });
		const rStrict = await h.board.recordFailure(
			strict.id,
			"spawn_failed",
			"e1",
			{
				failureLimit: 99,
				nowSeconds: h.clock.nowSeconds(),
			},
		);
		expect(rStrict.blocked).toBe(true);
	});

	it("successful completion RESETS the consecutive-failure counter (breaker measures streaks)", async () => {
		const card = await readyCard();
		await h.board.claimCard({
			cardId: card.id,
			lock: "w",
			expiresAt: h.clock.nowSeconds() + 100,
			nowSeconds: h.clock.nowSeconds(),
		});
		await h.board.recordFailure(card.id, "spawn_failed", "e1", {
			failureLimit: 2,
			nowSeconds: h.clock.nowSeconds(),
		});
		// Worker completes successfully on the next attempt:
		await h.board.claimCard({
			cardId: card.id,
			lock: "w2",
			expiresAt: h.clock.nowSeconds() + 100,
			nowSeconds: h.clock.nowSeconds(),
		});
		expect(await h.board.completeCard(card.id, h.clock.nowSeconds())).toBe(
			true,
		);
		const done = await h.board.getCard(card.id);
		expect(done?.status).toBe("done");
		expect(done?.consecutiveFailures).toBe(0);
		expect(done?.lastFailureError).toBeNull();

		// A later failure streak therefore starts from zero again.
		const recycled = await h.board.createCard({
			status: "ready",
			assignee: "worker",
		});
		await h.board.claimCard({
			cardId: recycled.id,
			lock: "w3",
			expiresAt: h.clock.nowSeconds() + 100,
			nowSeconds: h.clock.nowSeconds(),
		});
		const r = await h.board.recordFailure(
			recycled.id,
			"spawn_failed",
			"fresh-e1",
			{
				failureLimit: 2,
				nowSeconds: h.clock.nowSeconds(),
			},
		);
		expect(r).toEqual({ blocked: false, failures: 1 });
	});
});

describe("listReady — dispatch ordering (priority DESC, created_at ASC)", () => {
	it("orders exactly like the reference SELECT", async () => {
		const lowOld = await readyCard({
			priority: 1,
			createdAt: 100,
			id: "t_low_old",
		});
		const lowNew = await readyCard({
			priority: 1,
			createdAt: 200,
			id: "t_low_new",
		});
		const highNew = await readyCard({
			priority: 9,
			createdAt: 300,
			id: "t_high_new",
		});
		const highOld = await readyCard({
			priority: 9,
			createdAt: 50,
			id: "t_high_old",
		});

		const order = (await h.board.listReady()).map((c) => c.id);
		expect(order).toEqual([highOld.id, highNew.id, lowOld.id, lowNew.id]);
	});

	it("excludes claimed cards and other columns", async () => {
		await readyCard({ id: "t_visible" });
		const claimed = await readyCard({ id: "t_hidden_running" });
		await h.board.claimCard({
			cardId: claimed.id,
			lock: "w",
			expiresAt: h.clock.nowSeconds() + 100,
			nowSeconds: h.clock.nowSeconds(),
		});
		await readyCard({ id: "t_hidden_todo", status: "todo" });
		const ids = (await h.board.listReady()).map((c) => c.id);
		expect(ids).toEqual(["t_visible"]);
	});
});
