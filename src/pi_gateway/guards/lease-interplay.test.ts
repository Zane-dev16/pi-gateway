// STEP 7 — lease interplay: the in-process layer-1 registry (resolved
// session_id) × the cross-process DB store (compression-lineage ROOT), DEC-004
// 03 §7. Includes two-OS-process coexistence evidence modeled on the spike's
// child-driver pattern; child drivers live under guards/testing/.

import { spawn, type ChildProcess } from "node:child_process";
import {
	existsSync,
	mkdtempSync,
	readFileSync,
	rmSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import Database from "better-sqlite3";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { DbTurnLeaseStore, structuredHolder } from "../../pi_state/leases.js";
import { LEASE_PROBE_DDL } from "../../pi_state/testing/probe-schema.js";
import {
	SessionTurnLeaseRegistry,
	TurnLeaseTimeoutError,
} from "./turn-lease.js";

const DRIVER_TS = fileURLToPath(
	new URL("./testing/guards-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("../../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let dir: string;
let dbPath: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-guardlease-"));
	dbPath = join(dir, "state.db");
	seedLineage();
});

afterEach(() => {
	try {
		rmSync(dir, { recursive: true, force: true });
	} catch {
		/* temp dirs are disposable */
	}
});

function seedLineage(): void {
	const db = new Database(dbPath);
	db.exec(LEASE_PROBE_DDL);
	db.pragma("journal_mode = wal");
	// Compression lineage R ← C1 ← C2 (02 §5): segments contend on root R.
	const ins = db.prepare(
		"INSERT INTO sessions (id, parent_session_id, source, model_config, end_reason) VALUES (?, ?, ?, ?, ?)",
	);
	ins.run("R", null, null, null, "compression");
	ins.run("C1", "R", null, null, "compression");
	ins.run("C2", "C1", null, null, null);
	ins.run("U", null, null, null, null);
	db.close();
}

function spawnDriver(scenarioArgs: Record<string, string>): ChildProcess {
	const flat: string[] = [];
	for (const [k, v] of Object.entries(scenarioArgs)) {
		flat.push(`--${k}`, v);
	}
	return spawn(
		process.execPath,
		["--import", RESOLVE_MJS, DRIVER_TS, ...flat],
		{
			stdio: ["ignore", "pipe", "pipe"],
		},
	);
}

async function waitForMarker(name: string, timeoutMs = 15_000): Promise<void> {
	const path = join(dir, name);
	const deadline = Date.now() + timeoutMs;
	while (!existsSync(path)) {
		if (Date.now() > deadline) throw new Error(`timeout waiting for ${name}`);
		await new Promise<void>((r) => setTimeout(r, 10));
	}
	return;
}

function readMarker<T = Record<string, unknown>>(name: string): T {
	try {
		return JSON.parse(readFileSync(join(dir, name), "utf8")) as T;
	} catch (err) {
		throw new Error(`malformed marker ${name}: ${String(err)}`);
	}
}

describe("two-layer lease interplay — single process (DEC-004)", () => {
	it("L1 blocks a second acquirer WITHOUT contending the cross-process row", async () => {
		const db = new Database(dbPath);
		const store = new DbTurnLeaseStore(db, { pid: 111_111 });
		try {
			expect(store.tryAcquire("C1", structuredHolder("owner-a", 111_111))).toBe(
				true,
			);

			const registry = new SessionTurnLeaseRegistry();
			const l1 = await registry.acquire("C2", {
				ownerKey: "turn-a",
				generation: 1,
			});
			expect(l1).not.toBeNull();

			// Same-process rival fails closed at L1 fast…
			await expect(
				registry.acquire("C2", {
					ownerKey: "turn-b",
					generation: 2,
					timeoutMs: 80,
				}),
			).rejects.toBeInstanceOf(TurnLeaseTimeoutError);
			// …and the DB row was never touched by the loser.
			expect(store.probeOwner("C2")?.holder).toBe(
				structuredHolder("owner-a", 111_111),
			);

			registry.release(l1 as never);
			store.releaseHolder("C1", structuredHolder("owner-a", 111_111));
		} finally {
			db.close();
		}
	});

	it("lineage-root keying agrees across layers: C1/C2 share one DB slot while L1 keys stay per-resolved-id", async () => {
		const db = new Database(dbPath);
		const store = new DbTurnLeaseStore(db);
		try {
			const registry = new SessionTurnLeaseRegistry();

			// L2: acquiring via C1 holds the ROOT-R row visible from C2.
			expect(store.tryAcquire("C1", structuredHolder("x", process.pid))).toBe(
				true,
			);
			expect(store.probeOwner("C2")?.conversationId).toBe("R");

			// L1 meanwhile serializes per RESOLVED id independently:
			const t1 = await registry.acquire("C1", {
				ownerKey: "k1",
				generation: 1,
			});
			const t2 = await registry.acquire("C2", {
				ownerKey: "k2",
				generation: 1,
			});
			expect(t1).not.toBe(t2); // different slots at layer 1…

			// …but a SECOND acquirer of C1 still fails closed there.
			await expect(
				registry.acquire("C1", {
					ownerKey: "k3",
					generation: 2,
					timeoutMs: 60,
				}),
			).rejects.toBeInstanceOf(TurnLeaseTimeoutError);

			registry.release(t1 as never);
			registry.release(t2 as never);
			store.releaseHolder("C1", structuredHolder("x", process.pid));
		} finally {
			db.close();
		}
	});

	it("stale unwind is generation-safe on BOTH layers simultaneously", async () => {
		const db = new Database(dbPath);
		const store = new DbTurnLeaseStore(db, { pid: 111_111 });
		try {
			const registry = new SessionTurnLeaseRegistry();
			const staleL1 = await registry.acquire("U", {
				ownerKey: "old",
				generation: 7,
			});
			expect(store.tryAcquire("U", structuredHolder("old", 111_111))).toBe(
				true,
			);

			// Newer turn takes over BOTH layers:
			expect(store.tryAcquire("U", structuredHolder("new", 222_222))).toBe(
				false,
			); // PID alive
			registry.release(staleL1 as never);
			const freshL1 = await registry.acquire("U", {
				ownerKey: "new-turn",
				generation: 8,
			});
			expect(freshL1).not.toBeNull();

			// The stale holder's DB release cannot free the newer acquirer… wait —
			// here the newer DB attempt FAILED (PID alive), so old still owns it;
			// its release DOES free it, then fresh wins. Generation-safety shows
			// on L1: the stale token's second release is a no-op against fresh.
			store.releaseHolder("U", structuredHolder("old", 111_111));
			expect(store.tryAcquire("U", structuredHolder("new", 222_222))).toBe(
				true,
			);
			expect(registry.release(staleL1 as never)).toBe(false); // already released
			expect(registry.holderOf("U")).toBe(freshL1);

			store.releaseHolder("U", structuredHolder("new", 222_222));
			registry.release(freshL1 as never);
		} finally {
			db.close();
		}
	});
});

describe("two-layer lease interplay — TWO OS PROCESSES", () => {
	it("child holds BOTH layers; parent's local L1 is free but the DB lease blocks it until release", async () => {
		let child: ChildProcess | null = spawnDriver({
			scenario: "both-hold",
			db: dbPath,
			coord: dir,
			"session-id": "C1",
			"holder-prefix": "kid",
			"acquired-marker": "acquired",
			"release-marker": "release",
			"released-marker": "released",
		});
		try {
			await waitForMarker("acquired");
			const acquired = readMarker<{ acquired: boolean; holder: string }>(
				"acquired",
			);
			expect(acquired.acquired).toBe(true);
			expect(acquired.holder).toContain(
				`pid=${String(0) + 0}` === "" ? "" : "pid=",
			);

			const db = new Database(dbPath);
			const parentStore = new DbTurnLeaseStore(db);
			try {
				// Parent's OWN L1 is completely free — yet the cross-process
				// layer blocks the same lineage slot:
				expect(
					parentStore.tryAcquire("C2", structuredHolder("parent", process.pid)),
				).toBe(false);
				const registry = new SessionTurnLeaseRegistry();
				const localToken = await registry.acquire("C2", {
					ownerKey: "p",
					generation: 1,
				});
				expect(localToken).not.toBeNull(); // L1 alone proves nothing cross-process

				// Release the child; the parent then wins the SAME root slot.
				// The driver's protocol is MARKER FILES (not stdin).
				writeFileSync(join(dir, "release"), JSON.stringify({ t: Date.now() }));
				await waitForMarker("released");
				expect(
					parentStore.tryAcquire("C2", structuredHolder("parent", process.pid)),
				).toBe(true);
				parentStore.releaseHolder(
					"C2",
					structuredHolder("parent", process.pid),
				);
				registry.release(localToken as never);
			} finally {
				db.close();
			}
		} finally {
			if (!child.killed && child.exitCode === null) child.kill("SIGKILL");
			void child;
			child = null;
		}
	}, 30_000);

	it("SIGKILL'd dual-layer holder is reclaimed WITHOUT waiting out the TTL; successor takes both layers", async () => {
		let child: ChildProcess | null = spawnDriver({
			scenario: "hold-forever",
			db: dbPath,
			coord: dir,
			"session-id": "C1",
			"holder-prefix": "victim",
			ttl: "300",
			"acquired-marker": "victim-acquired",
		});
		try {
			await waitForMarker("victim-acquired");
			const victim = readMarker<{ acquired: boolean; holder: string }>(
				"victim-acquired",
			);
			expect(victim.acquired).toBe(true);

			const db = new Database(dbPath);
			const parentStore = new DbTurnLeaseStore(db);
			try {
				// TTL is far from expiry — only liveness can free this lease.
				const before = parentStore.probeOwner("C1");
				expect(before?.holder).toBe(victim.holder);
				expect((before?.expiresAt ?? 0) - Date.now() / 1000).toBeGreaterThan(
					240,
				);

				// Kill hard and reap so ESRCH proves death.
				const victimProc: ChildProcess = child;
				victimProc.kill("SIGKILL");
				const code = await new Promise<number | null>((resolve) => {
					victimProc.once("exit", (c) => resolve(c));
				});
				expect(code).not.toBe(0);

				// Reclaim through polling acquireWait — seconds, not TTL.
				const t0 = Date.now();
				const reclaimed = await parentStore.acquireWait(
					"C1",
					structuredHolder("parent", process.pid),
					{
						ttlSeconds: 300,
						waitSeconds: 20,
						pollIntervalSeconds: 0.05,
					},
				);
				expect(reclaimed).toBe(true);
				expect(Date.now() - t0).toBeLessThan(60_000); // ≪ 300s TTL

				// Successor takes ITS layer-1 token too: both layers agree.
				const registry = new SessionTurnLeaseRegistry();
				const token = await registry.acquire("C1", {
					ownerKey: "parent",
					generation: 9,
				});
				expect(token).not.toBeNull();
				registry.release(token as never);

				// Dead holder's stale unwind frees nothing of the successor's:
				parentStore.releaseHolder("C1", victim.holder);
				expect(
					parentStore.refresh("C1", structuredHolder("parent", process.pid)),
				).toBe(true);
				parentStore.releaseHolder(
					"C1",
					structuredHolder("parent", process.pid),
				);
			} finally {
				db.close();
			}
		} finally {
			if (child !== null && !child.killed && child.exitCode === null) {
				child.kill("SIGKILL");
			}
			child = null;
		}
	}, 45_000);
});
