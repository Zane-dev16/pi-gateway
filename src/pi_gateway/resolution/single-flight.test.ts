// Behavior contracts: resolution single-flight (02-session-and-state.md §4.1)
// and the §9 adopt-before-mint canonical registry built on it.
// Roadmap Phase 1 required contracts #1 and #2.
//
// Asserted by relationship:
//   - N concurrent cold-key resolves against a REAL temp DB mint exactly ONE
//     canonical row; every caller receives the SAME row (id equality + row
//     count via DB query + body-execution count).
//   - Different keys stay concurrent; same-key waiters share the owner's
//     settled value/error INSTANCE.
//   - Double-click registration ADOPTS the existing titled row rather than
//     minting twice → ONE row; adoption opens the LINEAGE TIP while the
//     registry row stays identity (§9 pseudocode).

import { describe, expect, it } from "vitest";
import type Database from "better-sqlite3";

import { StateStore } from "../../pi_state/store.js";
import { makeTempDir, removeTempDir } from "../../pi_state/testing/harness.js";
import { getCompressionTip } from "./compression-tip.js";
import { SingleFlightMap } from "./single-flight.js";

const TURN_TS = 1_750_000_000;

function deferred<T = void>(): {
	promise: Promise<T>;
	resolve: (value?: T) => void;
} {
	let resolve!: (value?: T) => void;
	const promise = new Promise<T>((res) => {
		resolve = (value?: T) => {
			res(value as T);
		};
	});
	return { promise, resolve };
}

describe("resolution single-flight (gateway/session.py:_SessionFlight)", () => {
	it("N→1: 24 concurrent resolves of one cold key against a real temp DB mint exactly ONE canonical row, shared by every caller", async () => {
		const dir = makeTempDir("pi-gw-sflight-");
		const store = await StateStore.open(`${dir}/state.db`);
		try {
			const KEY = "agent:whatsapp:dm:15551234567";
			const CANONICAL_ID = "20260823_101010_c0ffee01";
			const CALLERS = 24;

			let bodyRuns = 0;
			const allCallersArrived = deferred();
			const sf = new SingleFlightMap();

			// The production resolution body: mint the canonical sessions row
			// for a cold routing key (identity lands atomically in the INSERT).
			const resolveColdKey = () =>
				sf.run(KEY, async () => {
					bodyRuns += 1;
					// Hold the flight open until every caller has registered, so
					// the N→1 claim is deterministic rather than timing-lucky.
					await allCallersArrived.promise;
					await store.withWrite((db) => {
						db.prepare(
							`INSERT INTO sessions (id, source, session_key, chat_id, chat_type, started_at)
							 VALUES (?, 'whatsapp', ?, '15551234567', 'dm', ?)`,
						).run(CANONICAL_ID, KEY, TURN_TS);
					});
					return CANONICAL_ID;
				});

			const pending = Array.from({ length: CALLERS }, () => resolveColdKey());
			// Every caller after the first joined the in-flight slot synchronously.
			expect(bodyRuns).toBe(1);
			expect(sf.inFlightKeys()).toEqual([KEY]);

			allCallersArrived.resolve();
			const results = await Promise.all(pending);

			expect(new Set(results).size).toBe(1); // same row id for ALL callers
			expect(results[0]).toBe(CANONICAL_ID);
			expect(bodyRuns).toBe(1); // owner ran exactly once

			// DB is truth: exactly one canonical row minted for the key.
			const rowCount = store.db
				.prepare("SELECT COUNT(*) AS n FROM sessions WHERE session_key = ?")
				.get(KEY) as { n: number } | undefined;
			expect(rowCount?.n).toBe(1);

			// Slot clears only after settle.
			expect(sf.inFlightKeys()).toEqual([]);
		} finally {
			await store.close();
			removeTempDir(dir);
		}
	});

	it("different keys stay concurrent: neither body waits for the other", async () => {
		const sf = new SingleFlightMap();
		const gateA = deferred();
		const gateB = deferred();
		let enteredA = 0;
		let enteredB = 0;

		const runA = sf.run("key:A", async () => {
			enteredA += 1;
			await gateA.promise;
			return "A";
		});
		const runB = sf.run("key:B", async () => {
			enteredB += 1;
			await gateB.promise;
			return "B";
		});

		// Both bodies entered before either gate released → no cross-key lock.
		expect(enteredA).toBe(1);
		expect(enteredB).toBe(1);

		gateB.resolve();
		await expect(runB).resolves.toBe("B");
		expect(enteredA).toBe(1); // A still parked — B never blocked on it
		gateA.resolve();
		await expect(runA).resolves.toBe("A");
	});

	it("same-key waiters share the owner's settled VALUE instance; the slot is reusable afterwards", async () => {
		const sf = new SingleFlightMap();
		const gate = deferred<Record<string, unknown>>();
		const shared = { marker: true };

		const owner = sf.run("k", async () => {
			await gate.promise;
			return shared;
		});
		const waiter = sf.run("k", async () => "never-run");

		gate.resolve(shared);
		const [a, b] = await Promise.all([owner, waiter]);
		expect(b).toBe(a);
		expect(b).toBe(shared); // instance identity, not just deep equality
		expect(sf.inFlightKeys()).toEqual([]);

		// After settle a NEW call starts a FRESH flight (no stale result reuse).
		await expect(sf.run("k", async () => "fresh")).resolves.toBe("fresh");
	});

	it("owner failure rejects EVERY waiter with the SAME error instance; retry starts a fresh body", async () => {
		const sf = new SingleFlightMap();
		const CALLERS = 12;
		const boom = new Error("mint failed");
		let bodyRuns = 0;
		const allArrived = deferred();

		const attempt = () =>
			sf.run("cold:key", async () => {
				bodyRuns += 1;
				await allArrived.promise;
				throw boom;
			});

		const pending = Array.from({ length: CALLERS }, () => attempt());
		allArrived.resolve();

		const rejections = await Promise.all(
			pending.map((p) =>
				p.then(
					() => null,
					(e: unknown) => e,
				),
			),
		);
		for (const err of rejections) expect(err).toBe(boom); // same instance
		expect(bodyRuns).toBe(1);
		expect(sf.inFlightKeys()).toEqual([]);

		/** Failure is not memoized: next resolve starts a FRESH flight and runs. */
		await expect(sf.run("cold:key", async () => "recovered")).resolves.toBe(
			"recovered",
		);
		expect(sf.inFlightKeys()).toEqual([]);
	});
});

describe("adopt-before-mint canonical registry (02 §9)", () => {
	/** §9 registry entry shape: identity row + resolved lineage tip. */
	interface RegistryEntry {
		id: string;
		resolvedId: string;
	}

	interface Fixture {
		db: Database.Database;
		register(title: string): Promise<RegistryEntry>;
		mints(): number;
		/** Gates parking CURRENTLY-pending mint bodies (slow-create sim). */
		releasePendingMints(): void;
		close(): Promise<void>;
	}

	async function openFixture(): Promise<Fixture> {
		const dir = makeTempDir("pi-gw-adopt-");
		const store = await StateStore.open(`${dir}/state.db`);
		const db = store.db;
		const registry = new SingleFlightMap();
		let mintCount = 0;
		const pendingMintGates = new Map<
			string,
			{ promise: Promise<void>; resolve: () => void }
		>();

		const register = (title: string): Promise<RegistryEntry> => {
			// Non-interpolated key keeps this call site free of any SQL-adjacent
			// templating; titles never reach a query string (see parameterized
			// lookups below).
			const flightKey = ["title", title].join(":");
			return registry.run(flightKey, async (): Promise<RegistryEntry> => {
				// Adopt-before-mint: indexed name lookup INSIDE the flight, so a
				// racing click joins this execution instead of minting a fork.
				const found = db
					.prepare(
						"SELECT id FROM sessions WHERE title = ? ORDER BY rowid DESC LIMIT 1",
					)
					.get(title) as { id: string } | undefined;
				if (found) {
					const id = String(found.id);
					// §9: open THAT row — resolved_id is the lineage tip; the
					// registry row itself stays the identity.
					return { id, resolvedId: getCompressionTip(db, id) };
				}
				const gate = deferred();
				pendingMintGates.set(flightKey, gate);
				mintCount += 1;
				const seq = mintCount; // captured BEFORE parking — ids never collide
				await gate.promise; // simulate slow create under concurrent clicks
				const id = `20260823_101011_${String(seq).padStart(8, "0")}`;
				await store.withWrite((conn) => {
					conn
						.prepare(
							`INSERT INTO sessions (id, source, title, hidden, started_at)
						 VALUES (?, 'cli', ?, 1, ?)`,
						)
						.run(id, title, TURN_TS);
				});
				return { id, resolvedId: id };
			});
		};

		return {
			db,
			register,
			mints: () => mintCount,
			releasePendingMints: () => {
				for (const gate of pendingMintGates.values()) gate.resolve();
				pendingMintGates.clear();
			},
			close: async () => {
				await store.close();
				removeTempDir(dir);
			},
		};
	}

	it("double-click adopts rather than minting twice → ONE row, both clicks hold it", async () => {
		const fx = await openFixture();
		try {
			const first = fx.register("Bot Chat");
			const second = fx.register("Bot Chat"); // rapid second click

			fx.releasePendingMints(); // complete THE one creation body
			const [a, b] = await Promise.all([first, second]);

			expect(a).toBe(b); // same entry INSTANCE — one flight, two holders
			expect(a.id).toBe(b.id);
			expect(fx.mints()).toBe(1);

			const rows = fx.db
				.prepare("SELECT COUNT(*) AS n FROM sessions WHERE title = 'Bot Chat'")
				.get() as { n: number } | undefined;
			expect(rows?.n).toBe(1);

			// Canonical bot chats are minted hidden:true WITH eager title —
			// the untitled-window race is closed by construction.
			const row = fx.db
				.prepare("SELECT hidden, title FROM sessions WHERE id = ?")
				.get(a.id) as { hidden: number; title: string | null } | undefined;
			expect(row?.hidden).toBe(1);
			expect(row?.title).toBe("Bot Chat");
		} finally {
			await fx.close();
		}
	});

	it("sequential re-registration adopts the canonical row — names cannot dangle, no fork", async () => {
		const fx = await openFixture();
		try {
			const minting = fx.register("Support Bot");
			fx.releasePendingMints(); // let the first registration mint
			const first = await minting;
			const second = await fx.register("Support Bot"); // pure adopt path
			expect(second.id).toBe(first.id);
			expect(fx.mints()).toBe(1);
			const rows = fx.db.prepare("SELECT COUNT(*) AS n FROM sessions").get() as
				| { n: number }
				| undefined;
			expect(rows?.n).toBe(1);
		} finally {
			await fx.close();
		}
	});

	it("adoption opens the LINEAGE TIP of the found row; the registry row stays identity", async () => {
		const fx = await openFixture();
		try {
			// The canonical row compressed since the last click: parent closed
			// with end_reason='compression', continuation child exists.
			const parent = "20260822_090000_ada00001";
			const child = "20260822_090500_ada00002";
			fx.db
				.prepare(
					`INSERT INTO sessions (id, source, title, hidden, started_at, ended_at, end_reason)
					 VALUES (?, 'cli', 'Long Bot', 1, ?, ?, 'compression')`,
				)
				.run(parent, TURN_TS, TURN_TS + 100);
			fx.db
				.prepare(
					"INSERT INTO sessions (id, source, parent_session_id, started_at) VALUES (?, 'cli', ?, ?)",
				)
				.run(child, parent, TURN_TS + 101);

			const entry = await fx.register("Long Bot");
			expect(entry.id).toBe(parent); // identity = the registry row
			expect(entry.resolvedId).toBe(child); // opened at the lineage tip
			expect(fx.mints()).toBe(0); // adopted, never minted
		} finally {
			await fx.close();
		}
	});

	it("different titles stay independent: separate flights mint separately", async () => {
		const fx = await openFixture();
		try {
			const minting = Promise.all([
				fx.register("Bot One"),
				fx.register("Bot Two"),
			]);
			fx.releasePendingMints(); // both mint bodies parked on their own gates
			const [a, b] = await minting;
			expect(a.id).not.toBe(b.id);
			expect(fx.mints()).toBe(2);
			const rows = fx.db.prepare("SELECT COUNT(*) AS n FROM sessions").get() as
				| { n: number }
				| undefined;
			expect(rows?.n).toBe(2);
		} finally {
			await fx.close();
		}
	});
});
