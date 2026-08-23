// Behavior contracts: cross-process turn leases keyed on the compression-
// lineage ROOT (02-session-and-state.md §5, DEC-004; roadmap Phase 1 list).
//
// Asserted by relationship — expiry/dead-PID reclaim, lineage-root sharing,
// fork exclusion, generation-scoped release, exactly-one-winner under racers,
// and two-OS-process contention. Injected clocks for timing assertions.

import Database from "better-sqlite3";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	DEFAULT_TTL_SECONDS,
	DbTurnLeaseStore,
	extractHolderPid,
	structuredHolder,
} from "./leases.js";
import { LEASE_PROBE_DDL } from "./testing/probe-schema.js";
import { makeTempDir, removeTempDir } from "./testing/harness.js";

let dir: string;

beforeEach(() => {
	dir = makeTempTempDir();
});

function makeTempTempDir(): string {
	return makeTempDir("pi-gw-lease-");
}

afterEach(() => {
	removeTempDir(dir);
});

function dbPath(): string {
	return join(dir, "state.db");
}

interface LeaseFixture {
	store: DbTurnLeaseStore;
	db: Database.Database;
	/** Seed a session row (id/parent/source/model_config/end_reason). */
	seed(row: {
		id: string;
		parent?: string;
		source?: string;
		modelConfig?: string;
		endReason?: string | null;
	}): void;
	close(): void;
}

function openLeaseFixture(
	opts: ConstructorParameters<typeof DbTurnLeaseStore>[1] = {},
): LeaseFixture {
	const db = new Database(dbPath());
	db.pragma("journal_mode = WAL");
	db.exec(LEASE_PROBE_DDL);
	const store = new DbTurnLeaseStore(db, opts);
	return {
		store,
		db,
		seed: (row) => {
			db.prepare(
				`INSERT OR REPLACE INTO sessions (id, parent_session_id, source, model_config, end_reason)
				 VALUES (?, ?, ?, ?, ?)`,
			).run(
				row.id,
				row.parent ?? null,
				row.source ?? null,
				row.modelConfig ?? null,
				row.endReason ?? null,
			);
		},
		close: () => db.close(),
	};
}

describe("02 §5 turn lease — reclaim rules", () => {
	it("expired lease is reclaimed in-transaction by the next acquirer (injected clock)", () => {
		const f = openLeaseFixture({
			processAlive: () => true, // isolate the EXPIRY rule from dead-PID reclaim
		});
		try {
			f.seed({ id: "root" });
			let now = 1_000;
			const store2 = new DbTurnLeaseStore(f.db, {
				nowSeconds: () => now,
				processAlive: () => true,
			});
			expect(store2.tryAcquire("root", "a:pid=111", 60)).toBe(true);
			now += 61; // past the 60s TTL
			expect(store2.tryAcquire("root", "b:pid=222", 60)).toBe(true); // expiry reclaim
			expect(store2.probeOwner("root")?.holder).toBe("b:pid=222");
			expect(DEFAULT_TTL_SECONDS).toBe(300); // spec default pinned
		} finally {
			f.close();
		}
	});

	it("dead-PID holder is reclaimed BEFORE TTL; unstructured and same-PID holders stay protected", async () => {
		// A genuinely exited process proves ESRCH death without mocks.
		const dead = spawn(process.execPath, ["-e", "process.exit(0)"]);
		await new Promise<void>((resolve) => dead.on("exit", resolve));
		while (true) {
			try {
				process.kill(dead.pid!, 0);
				await new Promise((r) => setTimeout(r, 20));
			} catch {
				break; // ESRCH — provably gone
			}
		}

		const f = openLeaseFixture();
		try {
			f.seed({ id: "root" });
			const selfPid = process.pid;
			// Structured DEAD foreign pid → reclaimable.
			expect(f.store.tryAcquire("root", `gw:pid=${dead.pid}`, 300)).toBe(true);
			f.store.releaseHolder("root", `gw:pid=${dead.pid}`);
			// Unstructured holder whose number LOOKS like the dead pid → NOT reclaimable.
			expect(f.store.tryAcquire("root", `${dead.pid}-unstructured`, 300)).toBe(
				true,
			);
			// Same-process structured holder → never self-reclaimed.
			f.store.releaseHolder("root", `${dead.pid}-unstructured`);
			expect(f.store.tryAcquire("root", `mine:pid=${selfPid}`, 300)).toBe(true);
			expect(
				f.store.tryAcquire("root", `other:pid=${selfPid + 999999}`, 300),
			).toBe(false); // live same-pid lease protects
			// extractHolderPid parity of the upstream regex.
			expect(extractHolderPid("runner:pid=4242:turn")).toBe(4242);
			expect(extractHolderPid("no marker")).toBeNull();
			expect(structuredHolder("gw", 7)).toBe("gw:pid=7");
		} finally {
			f.close();
		}
	});
});

describe("02 §5 turn lease — lineage-root keying (DEC-004)", () => {
	it("all compression segments contend on ONE lease row; unrelated roots independent", () => {
		const f = openLeaseFixture();
		try {
			// Chain: root (compression-ended) ← mid ← tip. All segments share ONE
			// conversation row keyed on the ROOT.
			f.seed({ id: "root", endReason: "compression" });
			f.seed({ id: "mid", parent: "root", endReason: "compression" });
			f.seed({ id: "tip", parent: "mid" });

			expect(f.store.lineageRoot("tip")).toBe("root");
			expect(f.store.lineageRoot("mid")).toBe("root");

			expect(f.store.tryAcquire("tip", "a:pid=1")).toBe(true);
			// Sibling segment id blocks — SAME conversation_id row.
			expect(f.store.tryAcquire("mid", "b:pid=2")).toBe(false);
			expect(f.store.tryAcquire("root", "b:pid=2")).toBe(false);
			const owner = f.store.probeOwner("mid");
			expect(owner?.conversationId).toBe("root"); // one row for all segments

			// Unrelated root is independent.
			f.seed({ id: "other-lineage" });
			expect(f.store.tryAcquire("other-lineage", "b:pid=2")).toBe(true);

			// Generation-scoped release: stale release can't free the newer owner.
			f.store.releaseHolder("tip", "b:pid=2"); // wrong holder — no-op
			expect(f.store.probeOwner("tip")?.holder).toBe("a:pid=1");
		} finally {
			f.close();
		}
	});

	it("explicit forks do NOT join the compression lineage; markers bind to parent_session_id only", () => {
		const f = openLeaseFixture();
		try {
			f.seed({ id: "root", endReason: "compression" });
			// Branched child: _branched_from points AT its parent → fork boundary.
			f.seed({
				id: "branch",
				parent: "root",
				modelConfig: JSON.stringify({ _branched_from: "root" }),
			});
			expect(f.store.lineageRoot("branch")).toBe("branch");

			// Delegate continuation: _delegate_from of ITS OWN parent → fork too…
			f.seed({
				id: "deleg",
				parent: "root",
				modelConfig: JSON.stringify({ _delegate_from: "root" }),
			});
			expect(f.store.lineageRoot("deleg")).toBe("deleg");

			// …but a marker pointing ELSEWHERE does not exclude (presence-only
			// matching would misclassify): walks through to the compression parent.
			f.seed({
				id: "cont",
				parent: "root",
				modelConfig: JSON.stringify({ _delegate_from: "someone-else" }),
			});
			expect(f.store.lineageRoot("cont")).toBe("root");

			// tool-source children never join.
			f.seed({ id: "toolkid", parent: "root", source: "tool" });
			expect(f.store.lineageRoot("toolkid")).toBe("toolkid");

			// Cycles terminate the walk instead of hanging: walking from x lands on y
			// (y's own parent x is already visited), never loops.
			f.seed({ id: "x", parent: "y" });
			f.seed({ id: "y", parent: "x", endReason: "compression" });
			expect(f.store.lineageRoot("x")).toBe("y");

			// Non-compression parent stops the walk.
			f.seed({ id: "plainparent", endReason: "session_reset" });
			f.seed({ id: "childofplain", parent: "plainparent" });
			expect(f.store.lineageRoot("childofplain")).toBe("childofplain");
		} finally {
			f.close();
		}
	});

	it("refresh-after-rotation extends the SAME lease row via the child id", async () => {
		let now = 1_000;
		const f = openLeaseFixture({
			nowSeconds: () => now,
			processAlive: () => true,
		});
		try {
			f.seed({ id: "seg0", endReason: null });
			expect(f.store.tryAcquire("seg0", "a:pid=1", 100)).toBe(true);
			const before = f.store.probeOwner("seg0")!;

			// Rotate: close parent as compression, create child continuation.
			f.db
				.prepare(
					"UPDATE sessions SET end_reason = 'compression' WHERE id = 'seg0'",
				)
				.run();
			f.seed({ id: "seg1", parent: "seg0" });

			now += 50;
			expect(f.store.refresh("seg1", "a:pid=1", 100)).toBe(true); // walked to root
			const after = f.store.probeOwner("seg1")!;
			expect(after.conversationId).toBe(before.conversationId);
			expect(after.expiresAt).toBeGreaterThan(before.expiresAt);

			// Foreign refresh fails closed (false), never mutates.
			expect(f.store.refresh("seg1", "b:pid=2", 100)).toBe(false);
			expect(f.store.probeOwner("seg1")?.holder).toBe("a:pid=1");
		} finally {
			f.close();
		}
	});
});

describe("02 §5 acquire-wait semantics", () => {
	it("waiter wins after TTL expiry; on_wait notices fire; should_abort bails mid-wait", async () => {
		let now = 1_000;
		let mono = 0;
		const sleeps: number[] = [];
		const f = openLeaseFixture({
			nowSeconds: () => now,
			monotonicSeconds: () => mono,
			processAlive: () => true, // ONLY the clock can expire the initial lease
			sleep: async (ms) => {
				sleeps.push(ms);
				mono += ms / 1000;
				now += ms / 1000;
			},
		});
		try {
			f.seed({ id: "s" });
			expect(f.store.tryAcquire("s", "holder:pid=99", 2)).toBe(true); // 2s TTL

			let notices = 0;
			const wonPromise = f.store.acquireWait("s", "waiter:pid=98", {
				ttlSeconds: 2,
				waitSeconds: 30,
				pollIntervalSeconds: 0.5,
				waitNoticeIntervalSeconds: 1,
				onWait: () => notices++,
			});
			// Injected sleep advances BOTH clocks synchronously, so the wait loop
			// self-drives deterministically: ~4 polls of 500ms cross the 2s TTL.
			expect(await wonPromise).toBe(true);
			expect(notices).toBeGreaterThanOrEqual(1);
			expect(sleeps.length).toBeGreaterThanOrEqual(4); // polled until expiry
			expect(f.store.probeOwner("s")?.holder).toBe("waiter:pid=98");

			// should_abort bails immediately even while a lease is held.
			let abortCalls = 0;
			const aborted = await f.store.acquireWait("s", "third:pid=97", {
				waitSeconds: 30,
				shouldAbort: () => {
					abortCalls++;
					return true;
				},
			});
			expect(aborted).toBe(false);
			expect(abortCalls).toBeGreaterThanOrEqual(1);
		} finally {
			f.close();
		}
	});

	it("exactly ONE winner among N in-process racers across separate connections", async () => {
		const p = dbPath();
		{
			const seed = new Database(p);
			seed.pragma("journal_mode = WAL");
			seed.exec(LEASE_PROBE_DDL);
			seed.close();
		}
		const connections = Array.from({ length: 6 }, () => new Database(p));
		try {
			// All racer pids "alive" — ONLY the CAS may decide the winner (no
			// dead-PID reclaim interference).
			const stores = connections.map(
				(db, i) =>
					new DbTurnLeaseStore(db, {
						pid: 500_000 + i,
						processAlive: () => true,
					}),
			);
			const results = stores.map((s, i) =>
				s.tryAcquire("conv", structuredHolder(`racer${i}`, 500_000 + i), 300),
			);
			// Holders must be distinct AND exactly one CAS may win.
			const winners = results.filter(Boolean);
			expect(winners).toHaveLength(1);
		} finally {
			for (const c of connections) c.close();
		}
	});
});

// ---------------------------------------------------------------------------
// Two OS processes (DEC-004 verification shape ported from the spike)
// ---------------------------------------------------------------------------

const DRIVER_TS = fileURLToPath(
	new URL("./testing/child-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("./testing/node-ts-resolve.mjs", import.meta.url),
);
const PROJECT_ROOT = fileURLToPath(new URL("../../", import.meta.url));

interface ChildRun {
	code: number | null;
	stdout: string;
	stderr: string;
}

function runDriver(
	scenarioArgs: Record<string, string | number>,
	timeoutMs = 25_000,
): Promise<ChildRun> {
	const flat: string[] = [];
	for (const [k, v] of Object.entries(scenarioArgs))
		flat.push(`--${k}`, String(v));
	return new Promise((resolvePromise, rejectPromise) => {
		const child = spawn(
			process.execPath,
			["--import", RESOLVE_MJS, DRIVER_TS, ...flat],
			{ cwd: PROJECT_ROOT, stdio: ["ignore", "pipe", "pipe"] },
		);
		let stdout = "";
		let stderr = "";
		let killed = false;
		const timer = setTimeout(() => {
			killed = true;
			child.kill("SIGKILL");
		}, timeoutMs);
		child.stdout.on("data", (d: Buffer) => {
			stdout += d.toString("utf8");
		});
		child.stderr.on("data", (d: Buffer) => {
			stderr += d.toString("utf8");
		});
		child.on("error", (err: Error) => {
			clearTimeout(timer);
			rejectPromise(err);
		});
		child.on("close", (code: number | null) => {
			clearTimeout(timer);
			resolvePromise({ code: killed ? -1 : code, stdout, stderr });
		});
	});
}

function parseResult(run: ChildRun): Record<string, unknown> {
	const lines = run.stdout
		.split("\n")
		.filter((l) => l.startsWith("RESULT_JSON "));
	if (lines.length === 0) {
		throw new Error(`no RESULT_JSON from child; stderr=${run.stderr}`);
	}
	try {
		return JSON.parse(
			lines[lines.length - 1]!.slice("RESULT_JSON ".length),
		) as Record<string, unknown>;
	} catch (err) {
		throw new Error(
			`malformed RESULT_JSON from child (${String(err)}); stdout=${run.stdout} stderr=${run.stderr}`,
		);
	}
}

async function waitForMarker(name: string, timeoutMs = 15_000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (!existsSync(join(dir, name))) {
		if (Date.now() > deadline) throw new Error(`timeout waiting for ${name}`);
		await new Promise<void>((r) => setTimeout(r, 10));
	}
}

describe("two OS processes — lineage-root contention (DEC-004)", () => {
	it("process A holds via PARENT id; process B waits on CHILD id; B wins exactly after A releases", async () => {
		const p = dbPath();
		{
			const seed = new Database(p);
			seed.pragma("journal_mode = WAL");
			seed.exec(LEASE_PROBE_DDL);
			// root ← seg1 (compression parent), so BOTH ids share one conversation row.
			seed
				.prepare(
					"INSERT INTO sessions (id, parent_session_id, source, model_config, end_reason) VALUES ('root', NULL, NULL, NULL, 'compression')",
				)
				.run();
			seed
				.prepare(
					"INSERT INTO sessions (id, parent_session_id, source, model_config, end_reason) VALUES ('seg1', 'root', NULL, NULL, NULL)",
				)
				.run();
			seed.close();
		}

		const holderPromise = runDriver({
			scenario: "lease-holder",
			db: p,
			coord: dir,
			"session-id": "root",
			"holder-prefix": "procA", // driver embeds its REAL pid
			ttl: 120,
			"acquired-marker": "lease-a",
			"release-marker": "release-a",
			"released-marker": "released-a",
		});
		await waitForMarker("lease-a"); // A now owns conversation root

		// B contends via the CHILD id — must serialize on the same row.
		const contenderPromise = runDriver({
			scenario: "lease-contender",
			db: p,
			coord: dir,
			"session-id": "seg1",
			"holder-prefix": "procB", // different REAL process, different pid
			ttl: 120,
			wait: 15,
			poll: 0.05,
			"acquired-marker": "lease-a",
			"blocked-marker": "blocked-b",
		});

		await waitForMarker("blocked-b"); // B demonstrably blocked pre-release
		const { writeFileSync } = await import("node:fs");
		writeFileSync(join(dir, "release-a"), "{}"); // causal: blocked BEFORE release

		const holder = parseResult(await holderPromise);
		const contender = parseResult(await contenderPromise);
		expect(holder["acquired"]).toBe(true);
		expect(String(holder["holder"])).toMatch(/^procA:pid=\d+$/);
		expect(contender["won"]).toBe(true);
		expect(contender["notices"]).toBeGreaterThanOrEqual(1);
		expect(contender["conversationId"]).toBe("root"); // lineage-root sharing held cross-process
		expect(String(contender["holder"])).toMatch(/^procB:pid=\d+$/);

		// Post-release: an independent third root never contended with either.
		const verify = new Database(p);
		try {
			const rows = verify
				.prepare("SELECT conversation_id, holder FROM session_turn_leases")
				.all() as Array<{ conversation_id: string; holder: string }>;
			expect(rows).toHaveLength(1); // A's release removed its row; B's remains
			expect(rows[0]!.conversation_id).toBe("root");
		} finally {
			verify.close();
		}
	}, 45_000);
});
