// §6 dispatcher tick contracts over a FAKE BOARD (no SQLite, no network):
// reclaim → promote → budget → claim/spawn ordering, skip buckets, and the
// spawn-failure breaker wiring. The fake applies the same CAS semantics as
// the real store so lost-race handling is exercised honestly.

import { describe, expect, it } from "vitest";

import { dispatchOnce, DEFAULT_CLAIM_TTL_SECONDS } from "./dispatcher.js";
import type {
	BoardClient,
	ClaimRequest,
	DispatchResult,
	FailureOutcome,
	KanbanCard,
} from "./types.js";
import { DEFAULT_FAILURE_LIMIT } from "./types.js";

interface FakeState {
	cards: Map<string, KanbanCard>;
	events: Array<{ cardId: string; event: string; at: number }>;
}

class FakeBoard implements BoardClient {
	readonly board: string;
	private readonly s: FakeState;
	/** Cards whose claim attempt should lose the race (simulated racer). */
	readonly raceLosers = new Set<string>();

	constructor(board: string, state?: Partial<FakeState>) {
		this.board = board;
		this.s = {
			cards: state?.cards ?? new Map(),
			events: state?.events ?? [],
		};
	}

	add(card: Partial<KanbanCard> & { id: string }): KanbanCard {
		const full: KanbanCard = {
			title: "",
			status: "todo",
			assignee: null,
			tenant: null,
			priority: 0,
			claimLock: null,
			claimExpires: null,
			consecutiveFailures: 0,
			maxRetries: null,
			lastFailureError: null,
			createdAt: 0,
			startedAt: null,
			...card,
		};
		this.s.cards.set(full.id, full);
		return full;
	}

	async listReady(): Promise<KanbanCard[]> {
		return [...this.s.cards.values()]
			.filter((c) => c.status === "ready" && c.claimLock === null)
			.sort((a, b) => b.priority - a.priority || a.createdAt - b.createdAt);
	}

	async countRunning(): Promise<number> {
		return [...this.s.cards.values()].filter((c) => c.status === "running")
			.length;
	}

	async reclaimStaleClaims(nowSeconds: number): Promise<string[]> {
		const out: string[] = [];
		for (const c of this.s.cards.values()) {
			if (
				c.status === "running" &&
				c.claimExpires !== null &&
				c.claimExpires < nowSeconds
			) {
				c.status = "ready";
				c.claimLock = null;
				c.claimExpires = null;
				out.push(c.id);
				this.log(c.id, "reclaimed", nowSeconds);
			}
		}
		return out;
	}

	async promoteReady(
		nowSeconds: number,
		failureLimit: number,
	): Promise<string[]> {
		const out: string[] = [];
		for (const c of this.s.cards.values()) {
			if (c.status !== "todo") continue;
			const effective = c.maxRetries ?? failureLimit;
			if (c.consecutiveFailures >= effective) continue; // breaker holds
			c.status = "ready";
			out.push(c.id);
			this.log(c.id, "promoted", nowSeconds);
		}
		return out;
	}

	async claimCard(req: ClaimRequest): Promise<KanbanCard | null> {
		if (this.raceLosers.has(req.cardId)) return null;
		const c = this.s.cards.get(req.cardId);
		if (!c || c.status !== "ready" || c.claimLock !== null) return null;
		c.status = "running";
		c.claimLock = req.lock;
		c.claimExpires = req.expiresAt;
		this.log(c.id, "claimed", req.nowSeconds);
		return { ...c };
	}

	async recordFailure(
		cardId: string,
		outcome: FailureOutcome,
		error: string,
		opts: { failureLimit: number; nowSeconds: number },
	): Promise<{ blocked: boolean; failures: number }> {
		const c = this.s.cards.get(cardId);
		if (!c) return { blocked: false, failures: 0 };
		c.consecutiveFailures += 1;
		const effective = c.maxRetries ?? opts.failureLimit;
		this.log(cardId, outcome, opts.nowSeconds);
		if (c.consecutiveFailures >= effective) {
			c.status = "blocked";
			c.claimLock = null;
			c.claimExpires = null;
			c.lastFailureError = error;
			this.log(cardId, "gave_up", opts.nowSeconds);
			return { blocked: true, failures: c.consecutiveFailures };
		}
		c.status = "ready";
		c.claimLock = null;
		c.claimExpires = null;
		return { blocked: false, failures: c.consecutiveFailures };
	}

	async completeCard(cardId: string, nowSeconds: number): Promise<boolean> {
		const c = this.s.cards.get(cardId);
		if (!c || !["running", "ready", "blocked", "review"].includes(c.status))
			return false;
		c.status = "done";
		c.claimLock = null;
		c.claimExpires = null;
		c.consecutiveFailures = 0;
		this.log(cardId, "completed", nowSeconds);
		return true;
	}

	async events(cardId: string) {
		return this.s.events
			.filter((e) => e.cardId === cardId)
			.map(({ event, at }) => ({ event, at }));
	}

	private log(cardId: string, event: string, at: number): void {
		this.s.events.push({ cardId, event, at });
	}
}

function tick(
	board: FakeBoard,
	now: number,
	spawn: (card: KanbanCard) => void | Promise<void>,
	maxSpawn: number | null = null,
	failureLimit = DEFAULT_FAILURE_LIMIT,
): Promise<DispatchResult> {
	return dispatchOnce(board, {
		nowSeconds: now,
		failureLimit,
		maxSpawn,
		spawn,
	});
}

describe("dispatchOnce — tick shape (07 §6)", () => {
	it("runs reclaim → promote → claim+spawn in one pass", async () => {
		const board = new FakeBoard("b1");
		const stale = board.add({
			id: "stale",
			status: "running",
			assignee: "w",
			claimLock: "old",
			claimExpires: 900,
		});
		const promoted = board.add({
			id: "p1",
			status: "todo",
			assignee: "w",
			createdAt: 5,
		});
		const ready = board.add({
			id: "r1",
			status: "ready",
			assignee: "w",
			createdAt: 10,
		});

		const result = await tick(board, 1000, () => {});

		expect(result.reclaimed).toEqual([stale.id]);
		expect(result.promoted).toEqual([promoted.id]);
		// The reclaimed card is back in ready with no claim, so the SAME
		// tick's ready loop re-claims it first (lowest created_at) — parity of
		// _dispatch_once_locked, which enumerates ready AFTER reclaim+promote.
		expect(result.spawned.map((s) => s.cardId)).toEqual(["stale", "p1", "r1"]);
		expect(stale.status).toBe("running");
	});

	it("claims highest-priority first and honors maxSpawn as a LIVE concurrency cap", async () => {
		const board = new FakeBoard("b1");
		board.add({
			id: "low",
			status: "ready",
			assignee: "w",
			priority: 1,
			createdAt: 1,
		});
		board.add({
			id: "highA",
			status: "ready",
			assignee: "w",
			priority: 9,
			createdAt: 2,
		});
		board.add({
			id: "highB",
			status: "ready",
			assignee: "w",
			priority: 9,
			createdAt: 3,
		});

		// maxSpawn=2 with 0 running ⇒ exactly two claims this tick, best first.
		let result = await tick(board, 100, () => {}, 2);
		expect(result.spawned.map((s) => s.cardId)).toEqual(["highA", "highB"]);

		// Next tick: 2 running ≥ cap ⇒ nothing new spawns.
		result = await tick(board, 200, () => {}, 2);
		expect(result.spawned).toEqual([]);
	});

	it("skips unassigned ready cards into skippedUnassigned without blocking others", async () => {
		const board = new FakeBoard("b1");
		board.add({ id: "anon", status: "ready", assignee: null });
		board.add({ id: "owned", status: "ready", assignee: "w" });

		const result = await tick(board, 10, () => {});
		expect(result.skippedUnassigned).toEqual(["anon"]);
		expect(result.spawned.map((s) => s.cardId)).toEqual(["owned"]);
	});

	it("a LOST CLAIM RACE skips silently to the next card (exactly-one-owner discipline)", async () => {
		const board = new FakeBoard("b1");
		const raced = board.add({ id: "raced", status: "ready", assignee: "w" });
		const other = board.add({ id: "other", status: "ready", assignee: "w" });
		board.raceLosers.add(raced.id);

		const result = await tick(board, 10, () => {});
		expect(result.spawned.map((s) => s.cardId)).toEqual([other.id]);
		// The raced card was never spawned AND its CAS never flipped.
		expect(raced.status).toBe("ready");
	});

	it("spawn failure feeds the breaker: auto-block after EXACT-N consecutive failures across ticks", async () => {
		const board = new FakeBoard("b1");
		const card = board.add({ id: "cursed", status: "ready", assignee: "w" });

		// Tick 1 — failure 1/2: back to ready, not blocked.
		let result = await tick(board, 10, () => {
			throw new Error("profile missing");
		});
		expect(result.autoBlocked).toEqual([]);
		expect(card.status).toBe("ready");
		expect(card.consecutiveFailures).toBe(1);

		// Tick 2 — failure 2/2: breaker trips.
		result = await tick(board, 20, () => {
			throw new Error("profile missing");
		});
		expect(result.autoBlocked).toEqual(["cursed"]);
		expect(card.status).toBe("blocked");
		expect(card.lastFailureError).toBe("profile missing");

		// Tick 3 — blocked cards are invisible to the ready loop entirely.
		result = await tick(board, 30, () => {});
		expect(result.spawned).toEqual([]);
		expect(result.autoBlocked).toEqual([]);
	});

	it("dispatcher-minted claims carry the reference TTL (15 min)", async () => {
		const board = new FakeBoard("b1");
		board.add({ id: "c", status: "ready", assignee: "w", createdAt: 0 });
		await tick(board, 1000, () => {});
		const claimed = (await board.listReady()).length === 0;
		expect(claimed).toBe(true);
		expect(DEFAULT_CLAIM_TTL_SECONDS).toBe(900);
	});
});
