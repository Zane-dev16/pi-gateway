// Behavior contracts: declarative reconcile, storage-version tracking, title
// uniqueness, structural heals, read-probes (02-session-and-state.md §2.2,
// §3, §9; roadmap Phase 1 test list). Races and mutations only — no snapshots.
//
// Every test isolates its DB under an mkdtemp temp path.

import Database from "better-sqlite3";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	FTS_STORAGE_VERSION,
	SCHEMA_VERSION,
	FTS_REBUILD_HIGH_WATER_KEY,
	FTS_REBUILD_PROGRESS_KEY,
	FTS_STORAGE_VERSION_KEY,
} from "./schema.js";
import {
	assertStoreMatchesSchema,
	connectAndInitWithPatience,
	deleteMeta,
	getMeta,
	healGatewayRoutingPk,
	initStore,
	readProbeStatements,
	reconcileColumns,
	StoreBehindSchemaError,
} from "./reconcile.js";
import { StateStore } from "./store.js";
import {
	boundActivityDescription,
	SESSION_ACTIVITY_HEARTBEAT_MIN_INTERVAL_SECONDS,
} from "./store.js";
import { SessionTurnLeaseLostError, structuredHolder } from "./leases.js";
import { executeWrite } from "./wal.js";
import { makeTempDir, removeTempDir } from "./testing/harness.js";

let dir: string;

beforeEach(() => {
	dir = makeTempDir("pi-gw-state-");
});

afterEach(() => {
	removeTempDir(dir);
});

function dbPath(name = "state.db"): string {
	return `${dir}/${name}`;
}

/** Legacy-shaped store: sessions table missing later SCHEMA columns. */
function seedLegacySessionsDb(path: string): void {
	const raw = new Database(path);
	raw.pragma("journal_mode = WAL");
	raw.exec(`
		CREATE TABLE IF NOT EXISTS sessions (
		  id TEXT PRIMARY KEY,
		  source TEXT NOT NULL,
		  started_at REAL NOT NULL
		);
	`);
	raw
		.prepare(
			"INSERT INTO sessions (id, source, started_at) VALUES ('legacy-1', 'cli', 1)",
		)
		.run();
	raw.close();
}

describe("02 §3 reconcile — additive columns", () => {
	it("adds missing declared columns to a legacy DB without a migration block; data preserved", async () => {
		const p = dbPath();
		seedLegacySessionsDb(p);
		const store = await StateStore.open(p);
		try {
			expect(store.initReport?.reconciled.added.map((a) => a.column)).toContain(
				"session_key",
			);
			expect(store.initReport?.reconciled.added.map((a) => a.column)).toContain(
				"handoff_state",
			);
			// Legacy row survived; new column is NULL there.
			const row = store.db
				.prepare(
					"SELECT id, source, session_key FROM sessions WHERE id = 'legacy-1'",
				)
				.get() as { id: string; source: string; session_key: string | null };
			expect(row.id).toBe("legacy-1");
			expect(row.session_key).toBeNull();
			// Reopen is idempotent — nothing "added" the second time.
			await store.close();
			const again = await StateStore.open(p);
			try {
				expect(again.initReport?.reconciled.added).toHaveLength(0);
			} finally {
				await again.close();
			}
			return;
		} finally {
			await store.close().catch(() => undefined);
		}
	});

	it("sibling ADD COLUMN race: both connections racing migration succeed", async () => {
		const p = dbPath();
		seedLegacySessionsDb(p);
		// Two independent connections race the FULL init sequence concurrently;
		// 'duplicate column' losers continue, busy losers retry via whole-init
		// patience — both must exit success with a complete schema.
		const [reportA, reportB] = await Promise.all([
			connectAndInitWithPatience(async () => {
				const { db } = await import("./wal.js").then((w) =>
					w.openDatabase({ path: p }),
				);
				try {
					return initStore(db);
				} finally {
					db.close();
				}
			}),
			connectAndInitWithPatience(async () => {
				const { db } = await import("./wal.js").then((w) =>
					w.openDatabase({ path: p }),
				);
				try {
					return initStore(db);
				} finally {
					db.close();
				}
			}),
		]);
		expect(reportA.reconciled).toBeDefined();
		expect(reportB.reconciled).toBeDefined();
		// Final schema is complete either way (probes prepare cleanly).
		const check = new Database(p);
		try {
			expect(() => assertStoreMatchesSchema(check)).not.toThrow();
		} finally {
			check.close();
		}
	});

	it("locked ALTER re-raises so whole-init retries; half-reconciled stores never served", async () => {
		const p = dbPath();
		seedLegacySessionsDb(p);
		const holder = new Database(p);
		holder.pragma("busy_timeout = 50");
		holder.exec("BEGIN IMMEDIATE"); // foreign writer holds the WAL write lock
		try {
			const contender = new Database(p);
			try {
				contender.pragma("busy_timeout = 60");
				let raisedBusy = false;
				try {
					reconcileColumns(contender);
				} catch (err) {
					raisedBusy = true;
					expect((err as { code?: string }).code ?? "").toMatch(/BUSY|LOCKED/);
				}
				expect(raisedBusy).toBe(true); // NOT swallowed into a stale schema

				// Release; whole-init retry then lands everything.
				holder.exec("COMMIT");
				await connectAndInitWithPatience(
					async () => {
						const { db } = await import("./wal.js").then((w) =>
							w.openDatabase({ path: p }),
						);
						try {
							return initStore(db);
						} finally {
							db.close();
						}
					},
					{ patienceMs: 10_000 },
				);
				expect(() => assertStoreMatchesSchema(contender)).not.toThrow();
			} finally {
				contender.close();
			}
		} finally {
			holder.close();
		}
	});
});

describe("02 §9 title uniqueness — partial index + dedup repair", () => {
	it("second same-title row impossible; NULL titles unlimited", async () => {
		const store = await StateStore.open(dbPath());
		try {
			const ins = (id: string, title: string | null): void => {
				store.db
					.prepare(
						"INSERT INTO sessions (id, source, started_at, title) VALUES (?, 'cli', 1, ?)",
					)
					.run(id, title);
			};
			ins("a", "My Chat");
			expect(() => ins("b", "My Chat")).toThrow(/UNIQUE/);
			ins("n1", null);
			ins("n2", null);
			ins("n3", null);
			const nulls = store.db
				.prepare("SELECT COUNT(*) AS n FROM sessions WHERE title IS NULL")
				.get() as { n: number };
			expect(nulls.n).toBe(3);
		} finally {
			await store.close();
		}
	});

	it("dedup repair keeps the NEWEST duplicate; older rows get title=NULL; index ensured after repair", async () => {
		const p = dbPath();
		// Seed duplicates BEFORE any index exists (raw table, no reconcile).
		const raw = new Database(p);
		raw.pragma("journal_mode = WAL");
		raw.exec(
			"CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, source TEXT NOT NULL, started_at REAL NOT NULL, title TEXT)",
		);
		const ins = raw.prepare(
			"INSERT INTO sessions (id, source, started_at, title) VALUES (?, 'cli', 1, ?)",
		);
		ins.run("old-1", "Shared Alias");
		ins.run("old-2", "Shared Alias");
		ins.run("newest", "Shared Alias");
		raw.close();

		const store = await StateStore.open(p);
		try {
			const rows = store.db
				.prepare(
					"SELECT id, title FROM sessions ORDER BY CASE WHEN id='newest' THEN 0 ELSE 1 END",
				)
				.all() as Array<{ id: string; title: string | null }>;
			const kept = rows.filter((r) => r.title !== null);
			expect(kept).toHaveLength(1);
			expect(kept[0]!.id).toBe("newest"); // newest keeps the alias
			// Index now enforces uniqueness.
			expect(() =>
				store.db
					.prepare(
						"INSERT INTO sessions (id, source, started_at, title) VALUES ('x', 'cli', 1, 'Shared Alias')",
					)
					.run(),
			).toThrow(/UNIQUE/);
		} finally {
			await store.close();
		}
	});
});

describe("02 §3 read-probes — RO opens never DDL", () => {
	it("probe statements are derived from SCHEMA and detect behind-schema stores on RO opens", async () => {
		const probes = readProbeStatements();
		const byTable = new Map(probes.map((p) => [p.table, p]));
		expect(byTable.get("messages")?.sql).toContain("api_content");

		const p = dbPath();
		seedLegacySessionsDb(p); // behind schema (missing many columns, no tables)
		const ro = new Database(p, { readonly: true });
		try {
			let failed = false;
			try {
				assertStoreMatchesSchema(ro);
			} catch (err) {
				failed = true;
				expect(err).toBeInstanceOf(StoreBehindSchemaError);
				const be = err as StoreBehindSchemaError;
				expect(be.failedProbes.length).toBeGreaterThan(0);
			}
			expect(failed).toBe(true); // probe caught the stale store pre-use
		} finally {
			ro.close();
		}

		// Writable open heals; RO open then succeeds.
		const healer = await StateStore.open(p);
		await healer.close();
		const roAfter = new Database(p, { readonly: true });
		try {
			expect(() => assertStoreMatchesSchema(roAfter)).not.toThrow();
		} finally {
			roAfter.close();
		}
	});
});

describe("02 §2.2 two-tier storage-version tracking + FTS backfill gate", () => {
	it("fresh store stamps schema_version and fts_storage_version; triggers exclude tool rows", async () => {
		const store = await StateStore.open(dbPath());
		try {
			const v = store.db
				.prepare("SELECT version FROM schema_version")
				.get() as {
				version: number;
			};
			expect(Number(v.version)).toBe(SCHEMA_VERSION);
			expect(getMeta(store.db, FTS_STORAGE_VERSION_KEY)).toBe(
				String(FTS_STORAGE_VERSION),
			);

			store.db
				.prepare(
					"INSERT INTO sessions (id, source, started_at) VALUES ('s1', 'cli', 1)",
				)
				.run();
			store.db
				.prepare(
					"INSERT INTO messages (session_id, role, content, timestamp) VALUES ('s1', 'user', 'findable needle 🚀', 1)",
				)
				.run();
			store.db
				.prepare(
					"INSERT INTO messages (session_id, role, content, timestamp) VALUES ('s1', 'tool', 'tool output never indexed', 2)",
				)
				.run();
			const hits = store.db
				.prepare(
					"SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'needle'",
				)
				.all() as Array<{ rowid: number }>;
			expect(hits).toHaveLength(1);
			const toolHits = store.db
				.prepare(
					"SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'indexed'",
				)
				.all() as unknown[];
			expect(toolHits).toHaveLength(0); // role <> 'tool' exclusion held
		} finally {
			await store.close();
		}
	});

	it("legacy store (no fts_storage_version): bounded chunked backfill completes across opens; version stamped ONLY when done; no double-indexing", async () => {
		const p = dbPath();
		{
			// Build a CURRENT-schema store, seed rows, then regress it to legacy
			// FTS state: clear index contents + remove the layout-version stamp.
			const store = await StateStore.open(p);
			const insMsg = store.db.prepare(
				"INSERT INTO messages (session_id, role, content, timestamp) VALUES ('sX', 'user', ?, ?)",
			);
			store.db
				.prepare(
					"INSERT INTO sessions (id, source, started_at) VALUES ('sX','cli',1)",
				)
				.run();
			for (let i = 0; i < 25; i++) {
				const body = "backfill target " + String(i); // bound parameter
				insMsg.run(body, i);
			}
			store.db.exec(
				"INSERT INTO messages_fts(messages_fts) VALUES('delete-all')",
			);
			store.db.exec(
				"INSERT INTO messages_fts_trigram(messages_fts_trigram) VALUES('delete-all')",
			);
			deleteMeta(store.db, FTS_STORAGE_VERSION_KEY);
			await store.close();
		}

		// First open: budget ONE small chunk ⇒ backfill INCOMPLETE, version NOT bumped.
		{
			const store = await StateStore.open(p, {
				init: { ftsChunkRows: 10, maxFtsChunksPerOpen: 1 },
			});
			try {
				expect(store.initReport?.fts.complete).toBe(false);
				expect(store.initReport?.versionBumped).toBe(false);
				expect(getMeta(store.db, FTS_STORAGE_VERSION_KEY)).toBeNull();
				const progRaw = getMeta(store.db, FTS_REBUILD_PROGRESS_KEY);
				expect(progRaw).not.toBeNull(); // bookkeeping persisted for next open
				// Live traffic above high-water still indexed DURING backfill (gating).
				store.db
					.prepare(
						"INSERT INTO messages (session_id, role, content, timestamp) VALUES ('sX', 'user', 'live during rebuild zebra', 99)",
					)
					.run();
				const live = store.db
					.prepare(
						"SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'zebra'",
					)
					.all() as Array<{ rowid: number }>;
				expect(live).toHaveLength(1);
			} finally {
				await store.close();
			}
		}

		// Second open: finishes remaining chunks, deletes keys, stamps version.
		{
			const store = await StateStore.open(p, {
				init: { ftsChunkRows: 1000 },
			});
			try {
				expect(store.initReport?.fts.complete).toBe(true);
				expect(store.initReport?.versionBumped).toBe(true);
				expect(getMeta(store.db, FTS_STORAGE_VERSION_KEY)).toBe(
					String(FTS_STORAGE_VERSION),
				);
				expect(getMeta(store.db, FTS_REBUILD_HIGH_WATER_KEY)).toBeNull();
				expect(getMeta(store.db, FTS_REBUILD_PROGRESS_KEY)).toBeNull();

				// Every seeded message indexed EXACTLY once (no double-indexing
				// of the partially-backfilled range, no corruption).
				const expected = Number(
					(
						store.db
							.prepare(
								"SELECT COUNT(*) AS n FROM messages WHERE role <> 'tool' AND content LIKE '%target%'",
							)
							.get() as { n: number }
					).n,
				);
				const hits = store.db
					.prepare(
						"SELECT rowid FROM messages_fts WHERE messages_fts MATCH 'target'",
					)
					.all() as Array<{ rowid: number }>;
				expect(hits).toHaveLength(expected);
				const integrity = store.db.pragma("integrity_check", {
					simple: true,
				}) as unknown as string;
				expect(integrity).toBe("ok");
			} finally {
				await store.close();
			}
		}
	});
});

describe("02 §3 step 7 — gateway_routing PK heal", () => {
	function buildLegacyRouting(path: string): void {
		const raw = new Database(path);
		raw.pragma("journal_mode = WAL");
		raw.exec(`
			CREATE TABLE IF NOT EXISTS gateway_routing (
			  session_key TEXT PRIMARY KEY,
			  entry_json TEXT NOT NULL,
			  updated_at REAL NOT NULL
			);
		`);
		raw
			.prepare(
				"INSERT INTO gateway_routing (session_key, entry_json, updated_at) VALUES (?, ?, ?)",
			)
			.run("agent:main:telegram:dm:111", '{"v":1}', 100);
		raw.close();
	}

	it("heals legacy session_key-only PK preserving rows; composite scope isolation enforced after", async () => {
		const p = dbPath();
		buildLegacyRouting(p);
		const raw = new Database(p);
		// The heal needs the scope column present (reconcile adds it first).
		expect(healGatewayRoutingPk(raw)).toBe(true);
		const cols = raw
			.prepare('PRAGMA table_info("gateway_routing")')
			.all() as Array<Record<string, unknown>>;
		const pkCols = cols
			.filter((c) => Number(c["pk"]) > 0)
			.sort((a, b) => Number(a["pk"]) - Number(b["pk"]))
			.map((c) => String(c["name"]));
		expect(pkCols).toEqual(["scope", "session_key"]);
		const preserved = raw
			.prepare("SELECT session_key FROM gateway_routing")
			.all() as Array<{ session_key: string }>;
		expect(preserved.map((r) => r.session_key)).toContain(
			"agent:main:telegram:dm:111",
		);
		// Composite PK allows same key under a DIFFERENT scope (the isolation the
		// old shape made impossible).
		raw
			.prepare(
				"INSERT INTO gateway_routing (scope, session_key, entry_json, updated_at) VALUES ('ws2', 'agent:main:telegram:dm:111', '{}', 200)",
			)
			.run();
		// …and idempotent: correct shape reports nothing to heal.
		expect(healGatewayRoutingPk(raw)).toBe(false);
		raw.close();
	});

	it("full init heals end-to-end through StateStore.open", async () => {
		const p = dbPath();
		buildLegacyRouting(p);
		// Pre-add scope column like reconcile would (legacy DB mid-upgrade).
		const raw = new Database(p);
		raw.exec(
			"ALTER TABLE gateway_routing ADD COLUMN scope TEXT NOT NULL DEFAULT ''",
		);
		raw.close();
		const store = await StateStore.open(p);
		try {
			expect(store.initReport?.routingPkHealed).toBe(true);
		} finally {
			await store.close();
		}
	});
});

describe("write ladder integration through the facade", () => {
	it("withWrite retries busy and lands both sides' commits", async () => {
		const p = dbPath();
		const store = await StateStore.open(p);
		try {
			const other = new Database(p);
			other.pragma("journal_mode = WAL");
			other.exec(
				"CREATE TABLE IF NOT EXISTS ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, writer TEXT NOT NULL, seq INTEGER NOT NULL, payload TEXT NOT NULL)",
			);
			other.pragma("busy_timeout = 100");
			other.exec("BEGIN IMMEDIATE");
			other
				.prepare(
					"INSERT INTO ledger (writer, seq, payload) VALUES ('A', 1, 'held')",
				)
				.run();
			// Release A's lock ~2.4s in; the facade's write ladder rides it out.
			setTimeout(() => other.exec("COMMIT"), 2400);
			const landed = await store.withWrite(
				(conn) => {
					conn
						.prepare(
							"INSERT INTO ledger (writer, seq, payload) VALUES ('B', 1, 'landed 🚀')",
						)
						.run();
					return true;
				},
				{ patienceMs: 15_000 },
			);
			expect(landed).toBe(true);
			const rows = store.db
				.prepare("SELECT writer FROM ledger ORDER BY rowid")
				.all() as Array<{ writer: string }>;
			expect(rows.map((r) => r.writer)).toEqual(["A", "B"]); // zero lost commits
			other.close();
		} finally {
			await store.close();
		}
	});

	it("executeWrite terminal failure surfaces SQLITE_BUSY after patience exhausts", async () => {
		const p = dbPath();
		const store = await StateStore.open(p);
		try {
			store.db
				.prepare(
					"CREATE TABLE ledger (id INTEGER PRIMARY KEY AUTOINCREMENT, writer TEXT NOT NULL, seq INTEGER NOT NULL, payload TEXT NOT NULL)",
				)
				.run();
			const other = new Database(p);
			other.pragma("busy_timeout = 100");
			other.exec("BEGIN IMMEDIATE");
			let retries = 0;
			await expect(
				executeWrite(
					store.db,
					(conn) => {
						conn
							.prepare(
								"INSERT INTO ledger (writer, seq, payload) VALUES ('X', 1, 'x')",
							)
							.run();
						return true;
					},
					{ patienceMs: 500, onRetry: () => retries++ },
				),
			).rejects.toMatchObject({ code: "SQLITE_BUSY" });
			expect(retries).toBeGreaterThanOrEqual(1);
			other.exec("ROLLBACK");
			other.close();
		} finally {
			await store.close();
		}
	});
});

// ---------------------------------------------------------------------------
// Transcript-write lease admission (hermes_state.py:
// _check_transcript_write_guards turn-lease leg via insertMessageInTx).
// ---------------------------------------------------------------------------

describe("transcript-write lease admission inside the message write txn", () => {
	async function openWithSession(sessionId: string) {
		const store = await StateStore.open(dbPath());
		store.db
			.prepare(
				"INSERT INTO sessions (id, source, started_at) VALUES (?, 'telegram', ?)",
			)
			.run(sessionId, Date.now() / 1000);
		return store;
	}

	function leaseOf(store: StateStore, sessionId: string) {
		return store.db
			.prepare(
				"SELECT holder, expires_at FROM session_turn_leases WHERE conversation_id = ?",
			)
			.get(sessionId) as { holder: string; expires_at: number } | undefined;
	}

	it("owner-matching append lands; the guard is silent while the lease is fresh", async () => {
		const store = await openWithSession("adm-1");
		try {
			const holder = "gw:pid=1";
			expect(store.leases.tryAcquire("adm-1", holder)).toBe(true);
			const rowId = await store.appendMessage({
				sessionId: "adm-1",
				role: "user",
				content: "held write",
				turnLeaseHolder: holder,
			});
			expect(rowId).toBeGreaterThan(0);
			expect(store.listMessages("adm-1")).toHaveLength(1);
		} finally {
			await store.close();
		}
	});

	it("expired-but-matching lease RENEWS in the same txn — a starved refresher recovers", async () => {
		const store = await openWithSession("adm-2");
		try {
			const holder = "gw:pid=2";
			expect(store.leases.tryAcquire("adm-2", holder, 300)).toBe(true);
			// Force expiry behind the writer's back (>TTL stall).
			store.db
				.prepare(
					"UPDATE session_turn_leases SET expires_at = ? WHERE conversation_id = 'adm-2'",
				)
				.run(Date.now() / 1000 - 10);
			await store.appendMessage({
				sessionId: "adm-2",
				role: "user",
				content: "stalled but still ours",
				turnLeaseHolder: holder,
				turnLeaseTtlSeconds: 300,
			});
			const lease = leaseOf(store, "adm-2");
			expect(lease?.holder).toBe(holder);
			expect(lease!.expires_at).toBeGreaterThan(Date.now() / 1000 + 200); // renewed ~+300s
		} finally {
			await store.close();
		}
	});

	it("FOREIGN holder ⇒ SessionTurnLeaseLostError; nothing lands, thief keeps the slot", async () => {
		const store = await openWithSession("adm-3");
		try {
			const thief = structuredHolder("thief", process.pid);
			expect(store.leases.tryAcquire("adm-3", thief)).toBe(true);
			await expect(
				store.appendMessage({
					sessionId: "adm-3",
					role: "assistant",
					content: "stale flush after takeover",
					turnLeaseHolder: "gw:pid=999999",
				}),
			).rejects.toBeInstanceOf(SessionTurnLeaseLostError);
			// Fail-fast fencing: no partial row survived the refused txn.
			expect(
				store.listMessages("adm-3", { includeInactive: true }),
			).toHaveLength(0);
			expect(leaseOf(store, "adm-3")?.holder).toBe(thief);
		} finally {
			await store.close();
		}
	});

	it("missing lease row ⇒ refusal (released mid-turn means someone else owns the lineage now)", async () => {
		const store = await openWithSession("adm-4");
		try {
			await expect(
				store.appendMessage({
					sessionId: "adm-4",
					role: "user",
					content: "ghost write",
					turnLeaseHolder: "gw:pid=1",
				}),
			).rejects.toBeInstanceOf(SessionTurnLeaseLostError);
			expect(store.listMessages("adm-4")).toHaveLength(0);
		} finally {
			await store.close();
		}
	});

	it("holder-less appends skip the guard entirely (recovery replays keep working)", async () => {
		const store = await openWithSession("adm-5");
		try {
			// No lease row exists AT ALL — an unguarded write must still land.
			const rowId = await store.appendMessage({
				sessionId: "adm-5",
				role: "user",
				content: "unguarded recovery replay",
			});
			expect(rowId).toBeGreaterThan(0);
		} finally {
			await store.close();
		}
	});

	it("admission keys on the LINEAGE ROOT: a child-segment append contends with the root holder", async () => {
		const store = await StateStore.open(dbPath());
		try {
			const db = store.db;
			db.prepare(
				"INSERT INTO sessions (id, source, parent_session_id, started_at, end_reason) VALUES ('lin', 'telegram', NULL, ?, 'compression')",
			).run(Date.now() / 1000);
			db.prepare(
				"INSERT INTO sessions (id, source, parent_session_id, started_at) VALUES ('lin_child', 'telegram', 'lin', ?)",
			).run(Date.now() / 1000);
			const owner = structuredHolder("root-owner", process.pid);
			expect(store.leases.tryAcquire("lin", owner)).toBe(true);

			// Writing through the CHILD segment hits the SAME root-keyed row.
			await expect(
				store.appendMessage({
					sessionId: "lin_child",
					role: "user",
					content: "segment write without ownership",
					turnLeaseHolder: "gw:pid=424242",
				}),
			).rejects.toBeInstanceOf(SessionTurnLeaseLostError);

			// The rightful owner writing through the child segment lands.
			await store.appendMessage({
				sessionId: "lin_child",
				role: "user",
				content: "segment write as owner",
				turnLeaseHolder: owner,
			});
			expect(store.listMessages("lin_child")).toHaveLength(1);
		} finally {
			await store.close();
		}
	});
});

// ---------------------------------------------------------------------------
// Mid-turn activity heartbeat (hermes_state.py:touch_session_activity).
// ---------------------------------------------------------------------------

describe("touchSessionActivity — observation-only durable heartbeat", () => {
	async function openWithSession(id: string, lastActivityAt: number | null) {
		const store = await StateStore.open(dbPath());
		store.db
			.prepare(
				"INSERT INTO sessions (id, source, started_at, last_activity_at) VALUES (?, 'telegram', ?, ?)",
			)
			.run(id, 1_750_000_000, lastActivityAt);
		return store;
	}

	async function activityOf(store: StateStore, id: string) {
		return store.db
			.prepare(
				"SELECT last_activity_at, last_activity_description FROM sessions WHERE id = ?",
			)
			.get(id) as {
			last_activity_at: number | null;
			last_activity_description: string | null;
		};
	}

	it("advances last_activity_at and stamps the bounded description", async () => {
		const store = await openWithSession("act-1", null);
		try {
			await store.touchSessionActivity("act-1", {
				ts: 1_750_000_100,
				description: "x".repeat(300), // over budget → clamped with …
			});
			const row = await activityOf(store, "act-1");
			expect(row.last_activity_at).toBe(1_750_000_100);
			expect(row.last_activity_description).toHaveLength(120);
			expect(row.last_activity_description!.endsWith("…")).toBe(true);
		} finally {
			await store.close();
		}
	});

	it("NEVER moves last_activity_at backwards; nothing is written on a stale stamp", async () => {
		const store = await openWithSession("act-2", 1_750_000_500);
		try {
			store.db
				.prepare(
					"UPDATE sessions SET last_activity_description = 'keep' WHERE id = 'act-2'",
				)
				.run();
			await store.touchSessionActivity("act-2", {
				ts: 1_750_000_400, // older than the current stamp
				description: "stale",
			});
			const row = await activityOf(store, "act-2");
			expect(row.last_activity_at).toBe(1_750_000_500); // untouched
			expect(row.last_activity_description).toBe("keep"); // label rides the timestamp
		} finally {
			await store.close();
		}
	});

	it("no-ops on an unknown session or empty id without raising", async () => {
		const store = await StateStore.open(dbPath());
		try {
			await expect(
				store.touchSessionActivity("missing-row", { ts: 1 }),
			).resolves.toBeUndefined();
			await expect(store.touchSessionActivity("")).resolves.toBeUndefined();
		} finally {
			await store.close();
		}
	});

	it("heartbeat cadence constant honors the ≥30s write-pressure contract (ships 60s)", () => {
		expect(SESSION_ACTIVITY_HEARTBEAT_MIN_INTERVAL_SECONDS).toBe(60);
		expect(boundActivityDescription(null)).toBe("");
	});
});
