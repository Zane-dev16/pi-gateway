// Behavior contracts: coalescing background token writer (02 §7.2; DEC-011;
// roadmap Phase 1 list: "token-writer coalesce / absolute-delta / flush-
// barrier"). Injected monotonic clocks; ordering asserted by relationship.

import type Database from "better-sqlite3";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	coalesceTokenDeltas,
	TokenWriter,
	updateTokenCounts,
	type QueuedDelta,
	type TokenDelta,
} from "./usage.js";
import { StateStore } from "./store.js";
import { makeTempDir, removeTempDir } from "./testing/harness.js";

let dir: string;

beforeEach(() => {
	dir = makeTempDir("pi-gw-usage-");
});

afterEach(() => {
	removeTempDir(dir);
});

function dbPath(): string {
	return `${dir}/state.db`;
}

function q(sessionId: string, delta: TokenDelta): QueuedDelta {
	return { sessionId, delta };
}

async function openStore(): Promise<StateStore> {
	const store = await StateStore.open(dbPath());
	store.db
		.prepare(
			"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES ('s1', 'cli', 1)",
		)
		.run();
	store.db
		.prepare(
			"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES ('s2', 'cli', 1)",
		)
		.run();
	return store;
}

interface SessionsRow {
	input_tokens: number;
	output_tokens: number;
	cache_read_tokens: number;
	reasoning_tokens: number;
	api_call_count: number;
	model: string | null;
	billing_provider: string | null;
	estimated_cost_usd: number | null;
	actual_cost_usd: number | null;
}

function readSessions(db: Database.Database, id: string): SessionsRow {
	return db
		.prepare(
			"SELECT input_tokens, output_tokens, cache_read_tokens, reasoning_tokens, api_call_count, model, billing_provider, estimated_cost_usd, actual_cost_usd FROM sessions WHERE id = ?",
		)
		.get(id) as SessionsRow;
}

describe("coalescing rules (hermes _coalesce_token_deltas)", () => {
	it("merges ADJACENT same-route deltas; sums add; costs stay None-preserving", () => {
		const out = coalesceTokenDeltas([
			q("s1", { inputTokens: 10, model: "m1" }),
			q("s1", { inputTokens: 5, outputTokens: 2, model: "m1" }),
			q("s2", { inputTokens: 7, model: "m1" }), // different session → boundary
			q("s2", { inputTokens: 1, model: "m1" }),
		]);
		// s1's two adjacent deltas merge into one group; s2's two likewise.
		expect(out).toHaveLength(2);
		expect(out[0]!.sessionId).toBe("s1");
		expect(out[0]!.delta.inputTokens).toBe(15);
		expect(out[0]!.delta.outputTokens).toBe(2);
		expect(out[1]!.sessionId).toBe("s2");
		expect(out[1]!.delta.inputTokens).toBe(8);
	});

	it("cost fields sum None-preserving — an all-None run stays undefined so COALESCE keeps stored values", () => {
		// Omitted cost fields are the None case (kwargs.get(f) → None parity).
		const out = coalesceTokenDeltas([
			q("s1", { inputTokens: 1 }),
			q("s1", { inputTokens: 1 }),
		]);
		expect(out[0]!.delta.actualCostUsd).toBeUndefined();

		const mixed = coalesceTokenDeltas([
			q("s1", { actualCostUsd: 0.5 }),
			q("s1", {}), // None in the middle
			q("s1", { actualCostUsd: 0.25 }),
		]);
		expect(mixed[0]!.delta.actualCostUsd).toBeCloseTo(0.75);
	});

	it("route change splits adjacency; mid-session model switch preserves ORDER across the switch", () => {
		const out = coalesceTokenDeltas([
			q("s1", { inputTokens: 10, model: "old" }),
			q("s1", { inputTokens: 4, model: "new" }),
			q("s1", { inputTokens: 6, model: "new" }),
			q("s1", { inputTokens: 1, model: "old" }), // non-adjacent to first old
		]);
		expect(out.map((x) => x.delta.model)).toEqual(["old", "new", "old"]);
		expect(out.map((x) => x.delta.inputTokens)).toEqual([10, 10, 1]);
	});

	it("absolute deltas NEVER merge — not with each other, not with incrementals", () => {
		const out = coalesceTokenDeltas([
			q("s1", { inputTokens: 100, absolute: true }),
			q("s1", { inputTokens: 200, absolute: true }),
			q("s1", { inputTokens: 3, absolute: true, model: "x" }),
			q("s1", { inputTokens: 4, absolute: true, model: "x" }),
		]);
		expect(out).toHaveLength(4);
	});
});

describe("writer queue/flush semantics (02 §7.2)", () => {
	it("queued deltas apply in enqueue order; totals exact; flush barrier drains everything", async () => {
		const store = await openStore();
		try {
			const appliedOrder: string[] = [];
			const writer = new TokenWriter(store.db, {
				applyHook: (batch: readonly QueuedDelta[]) => {
					for (const item of batch) appliedOrder.push(item.sessionId);
				},
			});
			writer.queueTokenCounts("s1", {
				inputTokens: 10,
				outputTokens: 4,
				apiCallCount: 1,
			});
			writer.queueTokenCounts("s2", { inputTokens: 100 });
			writer.queueTokenCounts("s1", { inputTokens: 1 });
			expect(writer.pendingCount()).toBeGreaterThanOrEqual(1);

			const drained = await writer.flushTokenCounts();
			expect(drained).toBe(true);
			// Per-session relative order preserved through the interleaving.
			const s1Idx = appliedOrder.indexOf("s1");
			const s1Last = appliedOrder.lastIndexOf("s1");
			expect(appliedOrder.length).toBeGreaterThanOrEqual(2);
			expect(s1Idx).toBeLessThanOrEqual(s1Last);

			const a = readSessions(store.db, "s1");
			expect(a.input_tokens).toBe(11);
			expect(a.output_tokens).toBe(4);
			expect(a.api_call_count).toBe(1);
			expect(readSessions(store.db, "s2").input_tokens).toBe(100);

			await writer.stop();
		} finally {
			await store.close();
		}
	});

	it("flush barrier BEFORE route switch: queued old-route deltas land before an absolute new-route overwrite", async () => {
		const store = await openStore();
		try {
			const writer = new TokenWriter(store.db);
			writer.queueTokenCounts("s1", {
				inputTokens: 30,
				model: "gpt-old",
				billingProvider: "p1",
			});
			writer.queueTokenCounts("s1", {
				inputTokens: 12,
				model: "gpt-old",
				billingProvider: "p1",
			});

			// THE BARRIER (§7.2): flush before the route-field UPDATE.
			expect(await writer.flushTokenCounts()).toBe(true);
			await updateTokenCounts(store.db, "s1", {
				inputTokens: 999,
				model: "gpt-new",
				billingProvider: "p2",
				billingBaseUrl: "https://new",
				billingMode: "subscription",
				absolute: true,
			});
			// Any post-barrier straggler must NOT be re-applied on top of absolutes:
			await writer.flushTokenCounts();

			const row = readSessions(store.db, "s1");
			expect(row.input_tokens).toBe(999); // absolute won, no reordered deltas
			expect(row.model).toBe("gpt-new");

			// Per-model rows keep the accurate breakdown per live route (DEC-011).
			const routes = store.db
				.prepare(
					"SELECT model, SUM(input_tokens) AS t FROM session_model_usage WHERE session_id='s1' GROUP BY model ORDER BY model",
				)
				.all() as Array<{ model: string; t: number }>;
			expect(routes).toHaveLength(1); // absolute path records NO per-model rows
			expect(routes[0]!.t).toBe(42); // only the pre-switch incremental deltas
			await writer.stop();
		} finally {
			await store.close();
		}
	});

	it("mid-session /model switch attributes per-call deltas to the LIVE route (#51607)", async () => {
		const store = await openStore();
		try {
			const writer = new TokenWriter(store.db);
			writer.queueTokenCounts("s1", {
				inputTokens: 10,
				apiCallCount: 1,
				model: "modelA",
				billingProvider: "provA",
				billingBaseUrl: "https://a",
				billingMode: "api",
			});
			await writer.flushTokenCounts();
			writer.queueTokenCounts("s1", {
				inputTokens: 20,
				apiCallCount: 2,
				model: "modelB",
				billingProvider: "provB",
				billingBaseUrl: "https://b",
				billingMode: "api",
			});
			await writer.flushTokenCounts();
			await writer.stop();

			const routes = store.db
				.prepare(
					"SELECT model, billing_provider, api_call_count, input_tokens FROM session_model_usage WHERE session_id='s1' ORDER BY model",
				)
				.all() as Array<{
				model: string;
				billing_provider: string;
				api_call_count: number;
				input_tokens: number;
			}>;
			expect(routes).toHaveLength(2);
			expect(routes[0]).toMatchObject({
				model: "modelA",
				billing_provider: "provA",
				api_call_count: 1,
				input_tokens: 10,
			});
			expect(routes[1]).toMatchObject({
				model: "modelB",
				billing_provider: "provB",
				api_call_count: 2,
				input_tokens: 20,
			});

			// First ACCOUNTED route stamped the summary row; later switches don't clobber.
			const sess = readSessions(store.db, "s1");
			expect(sess.model).toBe("modelA");
			expect(sess.billing_provider).toBe("provA");
			expect(sess.api_call_count).toBe(3);
		} finally {
			await store.close();
		}
	});

	it("omitted model falls back to the session-row route for per-model attribution", async () => {
		const store = await openStore();
		try {
			// Session already carries its primary route (resolution sets it at create).
			store.db
				.prepare(
					"UPDATE sessions SET model='sessModel', billing_provider='sessProv' WHERE id='s1'",
				)
				.run();
			await updateTokenCounts(store.db, "s1", { inputTokens: 5 }); // no model
			const routes = store.db
				.prepare(
					"SELECT model, billing_provider, input_tokens FROM session_model_usage WHERE session_id='s1'",
				)
				.all() as Array<{
				model: string;
				billing_provider: string;
				input_tokens: number;
			}>;
			expect(routes).toHaveLength(1);
			expect(routes[0]).toMatchObject({
				model: "sessModel",
				billing_provider: "sessProv",
				input_tokens: 5,
			});
		} finally {
			await store.close();
		}
	});

	it("batch-hook failures are tolerated (observability-only) and deltas still land", async () => {
		const store = await openStore();
		try {
			const writer = new TokenWriter(store.db, {
				applyHook: () => {
					throw new Error("injected hook failure");
				},
			});
			writer.queueTokenCounts("s1", { inputTokens: 5 });
			await expect(writer.flushTokenCounts()).resolves.toBe(true); // never raises
			// Hook failure did NOT lose the delta — application continued.
			expect(readSessions(store.db, "s1").input_tokens).toBe(5);
			await writer.stop();
		} finally {
			await store.close();
		}
	});

	it("per-delta apply failures are logged, never raised; later deltas land once the fault clears", async () => {
		const store = await openStore();
		try {
			const writer = new TokenWriter(store.db);
			// Poison the apply path at the SQL layer (real failure class), not a mock.
			store.db.exec("DROP TABLE sessions");
			writer.queueTokenCounts("s1", { inputTokens: 5 });
			await expect(writer.flushTokenCounts()).resolves.toBe(true); // swallow+log

			// Fault clears (schema restored): subsequent deltas land.
			store.db.exec(
				"CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, source TEXT NOT NULL, started_at REAL NOT NULL, input_tokens INTEGER DEFAULT 0, output_tokens INTEGER DEFAULT 0, cache_read_tokens INTEGER DEFAULT 0, cache_write_tokens INTEGER DEFAULT 0, reasoning_tokens INTEGER DEFAULT 0, api_call_count INTEGER DEFAULT 0, estimated_cost_usd REAL, actual_cost_usd REAL, cost_status TEXT, cost_source TEXT, pricing_version TEXT, billing_provider TEXT, billing_base_url TEXT, billing_mode TEXT, model TEXT)",
			);
			store.db
				.prepare(
					"INSERT INTO sessions (id, source, started_at) VALUES ('s1', 'cli', 1)",
				)
				.run();
			writer.queueTokenCounts("s1", { inputTokens: 7 });
			await expect(writer.flushTokenCounts()).resolves.toBe(true);
			expect(readSessions(store.db, "s1").input_tokens).toBe(7); // failed delta = accounting loss; this one landed
			await writer.stop();
		} finally {
			await store.close().catch(() => undefined);
		}
	});

	it("stop() drains; afterwards queueTokenCounts falls back to SYNCHRONOUS apply", async () => {
		const store = await openStore();
		try {
			const writer = new TokenWriter(store.db);
			writer.queueTokenCounts("s1", { inputTokens: 21 });
			await writer.stop(); // drain-on-stop
			expect(readSessions(store.db, "s1").input_tokens).toBe(21);

			// Post-stop synchronous path (raises like the direct path on failure).
			writer.queueTokenCounts("s1", { inputTokens: 9 });
			expect(readSessions(store.db, "s1").input_tokens).toBe(30);
		} finally {
			await store.close();
		}
	});

	it("idle retirement retires the lazy worker; a fresh enqueue respawns it (injected clock)", async () => {
		const store = await openStore();
		try {
			let mono = 0;
			const writer = new TokenWriter(store.db, {
				monotonicSeconds: () => mono,
				idleRetireSeconds: 30,
			});
			expect(writer.isWriterActive()).toBe(false); // lazily spawned
			writer.queueTokenCounts("s1", { inputTokens: 1 });
			expect(writer.isWriterActive()).toBe(true);
			await writer.flushTokenCounts();
			mono += 31; // idle past retirement window
			for (let i = 0; i < 40 && writer.isWriterActive(); i++) {
				await new Promise((r) => setTimeout(r, 10));
			}
			expect(writer.isWriterActive()).toBe(false); // retired
			writer.queueTokenCounts("s1", { inputTokens: 2 }); // respawn on demand
			expect(writer.isWriterActive()).toBe(true);
			await writer.flushTokenCounts();
			expect(readSessions(store.db, "s1").input_tokens).toBe(3);
			await writer.stop();
		} finally {
			await store.close();
		}
	});
});

describe("direct apply path parity (update_token_counts)", () => {
	it("incremental vs absolute SQL semantics incl NULL-preserving actual cost", async () => {
		const store = await openStore();
		try {
			await updateTokenCounts(store.db, "s1", {
				inputTokens: 10,
				actualCostUsd: 0.25,
			});
			await updateTokenCounts(store.db, "s1", {
				inputTokens: 5,
				actualCostUsd: null,
			});
			let row = readSessions(store.db, "s1");
			expect(row.input_tokens).toBe(15);
			expect(row.actual_cost_usd).toBeCloseTo(0.25); // NULL delta preserved stored value

			await updateTokenCounts(store.db, "s1", {
				inputTokens: 100,
				outputTokens: 50,
				estimatedCostUsd: 2.5,
				absolute: true,
			});
			row = readSessions(store.db, "s1");
			expect(row.input_tokens).toBe(100); // SET, not incremented
			expect(row.output_tokens).toBe(50);
			expect(row.estimated_cost_usd).toBeCloseTo(2.5);
			expect(row.actual_cost_usd).toBeCloseTo(0.25); // untouched when absent
		} finally {
			await store.close();
		}
	});

	it("defensive session-row ensure: deltas for UNKNOWN sessions create bare rows instead of vanishing", async () => {
		const store = await openStore();
		try {
			await updateTokenCounts(store.db, "ghost-session", { inputTokens: 3 });
			const row = readSessions(store.db, "ghost-session");
			expect(row.input_tokens).toBe(3);
		} finally {
			await store.close();
		}
	});
});
