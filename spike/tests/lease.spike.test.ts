// Turn lease spike proof — two-layer lease per 02-session-and-state.md §5 and
// DEC-004 (09-open-questions.md). Behavior contracts only: cross-process
// contention on one SQLite DB, SIGKILL mid-hold reclaim, generation-scoped
// stale unwind, N-way races with exactly-one winner, lineage-root keying, TTL
// expiry. Every DB/state path is isolated under mkdtemp temp dirs.
//
// Hermes anchors under test:
//   gateway/turn_lease.py:SessionTurnLeaseRegistry (layer 1)
//   hermes_state.py:try_acquire_session_turn_lease / acquire_session_turn_lease /
//   _session_turn_lease_key_on_conn / _compression_lock_holder_process_is_dead (layer 2)

import { spawn, type ChildProcess } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { Worker } from "node:worker_threads";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { DbTurnLeaseStore, structuredHolder } from "../lease/db-turn-lease.js";
import {
	SessionTurnLeaseRegistry,
	TurnLeaseTimeoutError,
	type TurnLeaseToken,
} from "../lease/turn-lease-registry.js";

const DRIVER_PATH = fileURLToPath(
	new URL("../lease/child-driver.mjs", import.meta.url),
);
const STORE_PATH = fileURLToPath(
	new URL("../lease/db-turn-lease.ts", import.meta.url),
);

let dir: string;
let dbPath: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-lease-"));
	dbPath = join(dir, "state.db");
});

afterEach(() => {
	try {
		rmSync(dir, { recursive: true, force: true });
	} catch {
		/* temp dirs are disposable */
	}
});

// ---------------------------------------------------------------------------
// Cross-process driver harness: child OS processes and worker threads run the
// REAL DbTurnLeaseStore via spike/lease/child-driver.mjs (Node type-strips the
// .ts module). Parent syncs on stdout JSON lines / worker messages.
// ---------------------------------------------------------------------------

type DriverEvent = { event?: string } & Record<string, unknown>;

interface ContenderHandle {
	kind: "process" | "thread";
	index: number;
	nextEvent(timeoutMs?: number): Promise<DriverEvent>;
	release(): void;
	/** Hard-stop: SIGKILL for OS processes, terminate() for threads. */
	kill(): void;
	waitExit(timeoutMs?: number): Promise<number | null>;
	stop(): Promise<void>;
	stderrText(): string;
}

function encodeCmd(cmd: Record<string, unknown>): string {
	return Buffer.from(JSON.stringify(cmd)).toString("base64url");
}

function makeCollector(diag: () => string) {
	const queue: DriverEvent[] = [];
	const waiters: Array<(ev: DriverEvent) => void> = [];
	return {
		push(ev: DriverEvent): void {
			const wake = waiters.shift();
			if (wake) wake(ev);
			else queue.push(ev);
		},
		next(timeoutMs: number): Promise<DriverEvent> {
			const immediate = queue.shift();
			if (immediate) return Promise.resolve(immediate);
			return new Promise<DriverEvent>((resolve, reject) => {
				const timer = setTimeout(
					() => reject(new Error(`driver event timeout: ${diag()}`)),
					timeoutMs,
				);
				waiters.push((ev) => {
					clearTimeout(timer);
					resolve(ev);
				});
			});
		},
	};
}

function spawnProcessContender(
	index: number,
	cmd: Record<string, unknown>,
): ContenderHandle {
	const diag = () => `proc#${index} stderr=${stderrBuf}`;
	const collector = makeCollector(diag);
	let proc: ChildProcess | null = spawn(
		process.execPath,
		[DRIVER_PATH, dbPath, STORE_PATH, encodeCmd(cmd)],
		{ stdio: ["pipe", "pipe", "pipe"] },
	);
	let stderrBuf = "";
	let buf = "";

	proc.stdout!.setEncoding("utf8");
	proc.stdout!.on("data", (chunk: string) => {
		buf += chunk;
		for (;;) {
			const nl = buf.indexOf("\n");
			if (nl < 0) break;
			const line = buf.slice(0, nl);
			buf = buf.slice(nl + 1);
			if (!line.trim()) continue;
			try {
				collector.push(JSON.parse(line) as DriverEvent);
			} catch {
				/* non-JSON noise → ignore */
			}
		}
	});
	proc.stderr!.on("data", (c: string) => {
		stderrBuf += c;
	});

	let exitPromise: Promise<number | null> | null = null;
	return {
		kind: "process",
		index,
		nextEvent(timeoutMs = 10_000) {
			return collector.next(timeoutMs);
		},
		release() {
			proc?.stdin?.write("release\n");
		},
		kill() {
			proc?.kill("SIGKILL");
		},
		waitExit(timeoutMs = 10_000) {
			exitPromise ??= new Promise<number | null>((resolve, reject) => {
				const timer = setTimeout(
					() => reject(new Error(`proc#${index} exit timeout; ${diag()}`)),
					timeoutMs,
				);
				proc!.once("exit", (code) => {
					clearTimeout(timer);
					resolve(code);
				});
			});
			return exitPromise;
		},
		async stop() {
			if (!proc) return;
			this.release();
			await this.waitExit().catch(() => proc?.kill("SIGKILL"));
			proc = null;
		},
		stderrText: () => stderrBuf,
	};
}

function spawnThreadContender(
	index: number,
	cmd: Record<string, unknown>,
): ContenderHandle {
	let stopped = false;
	const worker = new Worker(DRIVER_PATH, {
		workerData: { dbPath, storePath: STORE_PATH, cmd },
	});
	const collector = makeCollector(() => `thread#${index}`);
	worker.on("message", (ev: DriverEvent) => collector.push(ev));
	let exitPromise: Promise<number | null> | null = null;
	return {
		kind: "thread",
		index,
		nextEvent(timeoutMs = 10_000) {
			return collector.next(timeoutMs);
		},
		release() {
			worker.postMessage({ type: "release" });
		},
		kill() {
			void worker.terminate();
		},
		waitExit(timeoutMs = 10_000) {
			exitPromise ??= new Promise<number | null>((resolve, reject) => {
				const timer = setTimeout(
					() => reject(new Error(`thread#${index} exit timeout`)),
					timeoutMs,
				);
				worker.once("exit", (code) => {
					clearTimeout(timer);
					resolve(code);
				});
			});
			return exitPromise;
		},
		async stop() {
			if (stopped) return;
			stopped = true;
			this.release();
			await this.waitExit().catch(() => worker.terminate());
		},
		stderrText: () => "",
	};
}

/** Wait for a contender to either win ('held') or lose ('result ok:false'). */
async function settleContender(
	c: ContenderHandle,
): Promise<{
	handle: ContenderHandle;
	held?: DriverEvent;
	result?: DriverEvent;
}> {
	for (;;) {
		const ev = await c.nextEvent(20_000);
		if (ev.event === "held") return { handle: c, held: ev };
		if (ev.event === "result") return { handle: c, result: ev };
		if (ev.event === "error") {
			throw new Error(
				`contender ${c.kind}#${c.index} crashed: ${String(ev.message)} ${c.stderrText()}`,
			);
		}
		// "ready" etc. → keep waiting
	}
}

function seedSessions(store: DbTurnLeaseStore): void {
	// Compression lineage R ← C1 ← C2: a rotated-away parent carries
	// end_reason 'compression' (hermes publish_compression_child shape); C2 is
	// the live tip. U unrelated; F an explicit fork child of C1
	// (_branched_from marker bound to its parent ⇒ walk stops at F).
	store.insertSession({ id: "R", endReason: "compression" });
	store.insertSession({
		id: "C1",
		parentSessionId: "R",
		endReason: "compression",
	});
	store.insertSession({ id: "C2", parentSessionId: "C1" });
	store.insertSession({ id: "U" });
	store.insertSession({
		id: "F",
		parentSessionId: "C1",
		modelConfig: JSON.stringify({ _branched_from: "C1" }),
	});
}

// ---------------------------------------------------------------------------
// Layer 1 — in-process registry (gateway/turn_lease.py parity)
// ---------------------------------------------------------------------------

describe("in-process turn lease registry (layer 1)", () => {
	it("generation check: stale unwind carrying an old token cannot release a newer lease (#28686 rule)", async () => {
		const registry = new SessionTurnLeaseRegistry();
		const stale = (await registry.acquire("sess", {
			ownerKey: "k-old",
			generation: 7,
		}))!;
		expect(stale.generation).toBe(7);
		expect(registry.release(stale)).toBe(true);

		// A newer turn takes over the same session…
		const fresh = (await registry.acquire("sess", {
			ownerKey: "k-new",
			generation: 8,
		}))!;
		// …and the OLD holder's late unwind replays release with its stale token:
		expect(registry.release(stale)).toBe(false);
		expect(registry.holderOf("sess")).toBe(fresh);

		// The newer lease is still exclusive — a third acquirer fails closed.
		await expect(
			registry.acquire("sess", {
				ownerKey: "k-third",
				generation: 9,
				timeoutMs: 80,
			}),
		).rejects.toBeInstanceOf(TurnLeaseTimeoutError);
		expect(registry.holderOf("sess")).toBe(fresh);

		// Release is idempotent and ownership-checked.
		expect(registry.release(fresh)).toBe(true);
		expect(registry.release(fresh)).toBe(false);
		expect(registry.holderOf("sess")).toBeNull();
	});

	it("timed-out waiter fails closed while FIFO waiter acquires after release; contention reported", async () => {
		const contended: string[] = [];
		const registry = new SessionTurnLeaseRegistry({
			onContended: (info) =>
				contended.push(`${info.waitingOwnerKey}<-${info.holderOwnerKey}`),
		});
		const holder = (await registry.acquire("s", {
			ownerKey: "h0",
			generation: 1,
		}))!;

		let grantedOrder: string[] = [];
		const slowWaiterP = registry
			.acquire("s", { ownerKey: "w-slow", generation: 2 })
			.then((t) => {
				grantedOrder = ["slow"];
				return t as TurnLeaseToken;
			});
		const impatientP = registry.acquire("s", {
			ownerKey: "w-fast",
			generation: 3,
			timeoutMs: 100,
		});

		await expect(impatientP).rejects.toBeInstanceOf(TurnLeaseTimeoutError);
		expect(contended).toEqual(["w-slow<-h0", "w-fast<-h0"]);

		registry.release(holder);
		const slowToken = await slowWaiterP;
		expect(grantedOrder).toEqual(["slow"]);
		expect(slowToken.ownerKey).toBe("w-slow");

		// Old holder's second release is a no-op once ownership moved on.
		expect(registry.release(holder)).toBe(false);
		expect(registry.release(slowToken)).toBe(true);
	});

	it("bounded registry: eviction drops only idle entries, never a live lease", async () => {
		const registry = new SessionTurnLeaseRegistry({ maxEntries: 3 });
		const live = (await registry.acquire("live", {
			ownerKey: "keep",
			generation: 1,
		}))!;
		for (const sid of ["a", "b", "c", "d", "e"]) {
			const t = (await registry.acquire(sid, {
				ownerKey: sid,
				generation: 1,
			}))!;
			registry.release(t);
		}
		expect(registry.size).toBeLessThanOrEqual(4); // cap holds (transient +live allowed)
		expect(registry.holderOf("live")).toBe(live); // correctness beats the cap
	});

	it("rebind aliases a HELD lease onto the rotated session id; blocked when target is live", async () => {
		const registry = new SessionTurnLeaseRegistry();

		const t = (await registry.acquire("old-id", {
			ownerKey: "turn",
			generation: 4,
		}))!;
		expect(registry.rebind(t, "new-id")).toBe(true);
		expect(registry.holderOf("new-id")).toBe(t);
		expect(registry.holderOf("old-id")).toBe(t); // same slot under both ids
		await expect(
			registry.acquire("new-id", {
				ownerKey: "alias-key",
				generation: 5,
				timeoutMs: 60,
			}),
		).rejects.toBeInstanceOf(TurnLeaseTimeoutError);
		expect(registry.release(t)).toBe(true);
		expect(registry.holderOf("new-id")).toBeNull();

		const ta = (await registry.acquire("a", { ownerKey: "a", generation: 1 }))!;
		const tb = (await registry.acquire("b", { ownerKey: "b", generation: 1 }))!;
		expect(registry.rebind(ta, "b")).toBe(false); // target live → blocked
		registry.release(ta);
		registry.release(tb);
	});
});

// ---------------------------------------------------------------------------
// Layer 2 — SQLite store semantics in a single process (hermes_state.py)
// ---------------------------------------------------------------------------

describe("SQLite turn lease store (layer 2)", () => {
	it("lineage-root keying: compression segments share one slot; forks and unrelated roots do not", () => {
		const store = DbTurnLeaseStore.open(dbPath);
		try {
			seedSessions(store);
			expect(store.lineageRoot("C1")).toBe("R");
			expect(store.lineageRoot("C2")).toBe("R");
			expect(store.lineageRoot("F")).toBe("F"); // explicit fork stops the walk
			expect(store.lineageRoot("U")).toBe("U");
			expect(store.lineageRoot("R")).toBe("R");

			// C1 and C2 contend on the SAME slot (root R).
			expect(store.tryAcquire("C1", "holder-A", 300)).toBe(true);
			expect(store.tryAcquire("C2", "holder-B", 300)).toBe(false);
			const probe = store.probeOwner("C2"); // resolved from C2 → same row
			expect(probe?.conversationId).toBe("R");
			expect(probe?.holder).toBe("holder-A");

			// Unrelated root and explicit fork are independent slots, concurrently.
			expect(store.tryAcquire("U", "holder-B", 300)).toBe(true);
			expect(store.tryAcquire("F", "holder-B", 300)).toBe(true);

			// Release is holder-scoped and visible through any segment of the lineage.
			store.releaseHolder("C2", "holder-A"); // releases the ROOT row via C2
			expect(store.probeOwner("C1")).toBeNull();
			expect(store.tryAcquire("C1", "holder-B", 300)).toBe(true);
		} finally {
			store.close();
		}
	});

	it("TTL expiry: a live-but-silent holder past TTL loses the lease; refresh stays holder-scoped", () => {
		let nowSec = 10_000;
		const clock = (): number => nowSec;
		// Fake PIDs are probed by the OTHER store — keep them "alive" so this
		// test isolates pure TTL expiry from the dead-PID reclaim path.
		const storeA = DbTurnLeaseStore.open(dbPath, {
			nowSeconds: clock,
			pid: 111_111,
			processAlive: () => true,
		});
		const storeB = DbTurnLeaseStore.open(dbPath, {
			nowSeconds: clock,
			pid: 222_222,
			processAlive: () => true,
		});
		try {
			seedSessions(storeA);
			expect(storeA.tryAcquire("C1", structuredHolder("A", 111_111), 300)).toBe(
				true,
			);
			const held = storeA.probeOwner("C2");
			expect(held?.expiresAt).toBe(nowSec + 300);

			// Unexpired → B cannot take it even though A is silent.
			expect(storeB.tryAcquire("C2", structuredHolder("B", 222_222), 300)).toBe(
				false,
			);
			// Refresh extends only for the current holder…
			expect(storeA.refresh("C2", structuredHolder("A", 111_111), 300)).toBe(
				true,
			);
			// …and fails for everyone else.
			expect(storeB.refresh("C2", structuredHolder("B", 222_222), 300)).toBe(
				false,
			);

			nowSec += 301; // past TTL; holder A's PID is alive but silent
			expect(storeB.tryAcquire("C2", structuredHolder("B", 222_222), 300)).toBe(
				true,
			);
			expect(storeA.refresh("C1", structuredHolder("A", 111_111), 300)).toBe(
				false,
			); // A lost it
			// A's stale unwind deletes only WHERE holder = A → no-op against B.
			storeA.releaseHolder("C1", structuredHolder("A", 111_111));
			expect(storeB.refresh("C1", structuredHolder("B", 222_222), 300)).toBe(
				true,
			);
			expect(storeB.probeOwner("C2")?.holder).toBe(
				structuredHolder("B", 222_222),
			);
		} finally {
			storeA.close();
			storeB.close();
		}
	});

	it("dead-PID reclaim: provably-dead structured holder reclaimed before TTL; doubt stays protected", () => {
		const DEAD_PID = 999_999;
		const store = DbTurnLeaseStore.open(dbPath, {
			pid: 1,
			processAlive: (pid) => pid !== DEAD_PID,
		});
		try {
			seedSessions(store);
			expect(
				store.tryAcquire("C1", structuredHolder("ghost", DEAD_PID), 300),
			).toBe(true);
			// Holder PID is dead → reclaimed immediately although unexpired.
			expect(store.tryAcquire("C1", "successor", 300)).toBe(true);

			// Unstructured (legacy) holder: no pid= marker → protected until TTL.
			expect(store.tryAcquire("C1", "legacy-no-pid", 300)).toBe(false);
			// Same-process holder: never self-reclaimed (another thread may own it).
			expect(
				store.tryAcquire("C1", structuredHolder("selfish", process.pid), 300),
			).toBe(false);
			// Live foreign PID: protected until TTL too.
			expect(store.tryAcquire("C1", structuredHolder("alive", 1234), 300)).toBe(
				false,
			);
			// Stale release from the ghost cannot free the successor's lease.
			store.releaseHolder("C1", structuredHolder("ghost", DEAD_PID));
			expect(store.refresh("C1", "successor", 300)).toBe(true);
		} finally {
			store.close();
		}
	});

	it("acquireWait: should_abort bails mid-wait without consuming the budget; on_wait fires", async () => {
		const storeA = DbTurnLeaseStore.open(dbPath);
		const storeB = DbTurnLeaseStore.open(dbPath);
		try {
			seedSessions(storeA);
			expect(storeA.tryAcquire("C1", "holder-A", 300)).toBe(true);

			const notices: number[] = [];
			let abort = false;
			setTimeout(() => {
				abort = true;
			}, 250).unref();
			const t0 = Date.now();
			const ok = await storeB.acquireWait("C2", "holder-B", {
				waitSeconds: 30,
				pollIntervalSeconds: 0.05,
				waitNoticeIntervalSeconds: 0.1,
				onWait: (elapsed) => notices.push(elapsed),
				shouldAbort: () => abort,
			});
			const elapsedMs = Date.now() - t0;
			expect(ok).toBe(false); // aborted, not acquired
			expect(elapsedMs).toBeLessThan(5000); // far under the 30s budget
			expect(notices.length).toBeGreaterThanOrEqual(1);
			expect(storeA.probeOwner("C1")?.holder).toBe("holder-A"); // never double-owned
		} finally {
			storeA.close();
			storeB.close();
		}
	});
});

// ---------------------------------------------------------------------------
// Layer 3 — cross-process contracts on ONE SQLite DB
// ---------------------------------------------------------------------------

describe("cross-process turn leases (layer 3)", () => {
	it("two OS processes acquire/release the same lease slot through one DB", async () => {
		const parentStore = DbTurnLeaseStore.open(dbPath);
		try {
			seedSessions(parentStore);

			// Child #1: acquire then immediately release ("once").
			const child1 = spawnProcessContender(1, {
				op: "once",
				session: "C1",
				holder: "kid-one",
				ttlSeconds: 300,
			});
			await child1.nextEvent(); // ready
			const r1 = await child1.nextEvent();
			expect(r1.event).toBe("result");
			expect(r1.ok).toBe(true);
			expect(r1.released).toBe(true);
			expect(String(r1.holder)).toContain("pid="); // structured holder embeds its PID
			await child1.waitExit();
			expect(parentStore.probeOwner("C1")).toBeNull(); // released on exit

			// Parent holds; child #2 single-shot acquire must fail cleanly.
			expect(
				parentStore.tryAcquire(
					"C1",
					structuredHolder("parent", process.pid),
					300,
				),
			).toBe(true);
			const child2 = spawnProcessContender(2, {
				op: "once",
				session: "C2",
				holder: "kid-two",
				ttlSeconds: 300,
			});
			await child2.nextEvent();
			const r2 = await child2.nextEvent();
			expect(r2.event).toBe("result");
			expect(r2.ok).toBe(false); // contended, no wait budget → fail closed
			await child2.waitExit();

			// After the parent releases, the next child acquires.
			parentStore.releaseHolder("C1", structuredHolder("parent", process.pid));
			const child3 = spawnProcessContender(3, {
				op: "once",
				session: "C1",
				holder: "kid-three",
				ttlSeconds: 300,
			});
			await child3.nextEvent();
			const r3 = await child3.nextEvent();
			expect(r3.event).toBe("result");
			expect(r3.ok).toBe(true);
			await child3.waitExit();
			child3.release();
		} finally {
			parentStore.close();
		}
	});

	it("SIGKILL mid-hold: dead holder reclaimed by another process WITHOUT waiting out the TTL", async () => {
		const parentStore = DbTurnLeaseStore.open(dbPath);
		try {
			seedSessions(parentStore);
			const child = spawnProcessContender(1, {
				op: "hold",
				session: "C1",
				holder: "victim",
				ttlSeconds: 300, // long TTL: only liveness can free this lease
				waitSeconds: 5,
				pollIntervalSeconds: 0.05,
				maxHoldMs: 15_000,
			});
			await child.nextEvent(); // ready
			const held = await child.nextEvent();
			expect(held.event).toBe("held");
			expect(Number(held.pid)).toBeGreaterThan(0); // real OS process reported its PID
			const victimHolder = String(held.holder);

			// Control: while the victim lives and the lease is fresh, nobody else wins.
			const beforeKill = parentStore.probeOwner("C1")!;
			expect(beforeKill.holder).toBe(victimHolder);
			expect(beforeKill.expiresAt - Date.now() / 1000).toBeGreaterThan(240); // TTL far from expiry
			expect(parentStore.tryAcquire("C1", "parent", 300)).toBe(false);

			// Kill hard — no graceful release possible — and reap so the kernel
			// proves the PID is gone (ESRCH).
			child.kill();
			const code = await child.waitExit();
			expect(code).not.toBe(0);

			// Reclaim happens through the polling acquire path, in seconds, NOT
			// after the 300s TTL: liveness-based reclaim.
			const t0 = Date.now();
			const reclaimed = await parentStore.acquireWait(
				"C1",
				structuredHolder("parent", process.pid),
				{
					ttlSeconds: 300,
					waitSeconds: 30,
					pollIntervalSeconds: 0.05,
				},
			);
			const reclaimMs = Date.now() - t0;
			expect(reclaimed).toBe(true);
			expect(reclaimMs).toBeLessThan(60_000); // ≪ TTL; loose wall-clock bound

			// The dead holder's stale unwind cannot free the new owner's lease.
			parentStore.releaseHolder("C1", victimHolder);
			expect(
				parentStore.refresh("C1", structuredHolder("parent", process.pid), 300),
			).toBe(true);
			expect(parentStore.probeOwner("C2")?.holder).toBe(
				structuredHolder("parent", process.pid),
			);
		} finally {
			parentStore.close();
		}
	});

	it("race: 8 concurrent acquirers (4 threads + 4 processes) yield exactly one winner with mutual exclusion throughout", async () => {
		const parentStore = DbTurnLeaseStore.open(dbPath);
		const contenders: ContenderHandle[] = [];
		try {
			seedSessions(parentStore);
			const h0 = structuredHolder("H0", process.pid);
			expect(parentStore.tryAcquire("ROOT", h0, 300)).toBe(true);

			// Probe samples ownership ~every 20ms across the whole race.
			const samples: Array<{ t: number; holder: string | null }> = [];
			const sampler = setInterval(() => {
				samples.push({
					t: Date.now(),
					holder: parentStore.probeOwner("ROOT")?.holder ?? null,
				});
			}, 20);
			sampler.unref();

			const contendersCmd = (i: number): Record<string, unknown> => ({
				op: "hold",
				session: "ROOT",
				holder: i % 2 === 0 ? `t${i}` : `p${i}`,
				ttlSeconds: 300,
				waitSeconds: 6, // losers time out well inside the test timeout
				pollIntervalSeconds: 0.03,
				maxHoldMs: 15_000,
			});
			for (let i = 0; i < 8; i++) {
				contenders.push(
					i % 2 === 0
						? spawnThreadContender(i, contendersCmd(i))
						: spawnProcessContender(i, contendersCmd(i)),
				);
			}

			// H0 holds for ~800ms after everyone starts, then releases.
			await new Promise((r) => setTimeout(r, 800));
			const releaseAt = Date.now();
			parentStore.releaseHolder("ROOT", h0);

			const settled = await Promise.all(contenders.map(settleContender));
			const winners = settled.filter((s) => s.held !== undefined);
			const losers = settled.filter((s) => s.result !== undefined);

			clearInterval(sampler);

			// Exactly one winner; every loser timed out as a waiter.
			expect(winners).toHaveLength(1);
			expect(losers).toHaveLength(7);
			for (const loser of losers) {
				expect(loser.result?.ok).toBe(false);
			}
			const winnerHolder = String(winners[0]!.held!.holder);

			// Mutual exclusion timeline from the probe:
			//  - while H0 held (up to releaseAt): H0 was the ONLY owner ever seen;
			//  - no loser's holder string EVER appears in any sample;
			//  - once the winner appeared, it stayed sole owner until released.
			const duringH0 = samples.filter((s) => s.t <= releaseAt);
			expect(duringH0.length).toBeGreaterThan(10); // sampled throughout the hold
			for (const s of duringH0) {
				expect(s.holder === null || s.holder === h0).toBe(true);
			}
			const loserHolders = new Set(losers.map((l) => String(l.result!.holder)));
			for (const s of samples) {
				expect(loserHolders.has(s.holder ?? "")).toBe(false);
			}
			const firstWinIdx = samples.findIndex((s) => s.holder === winnerHolder);
			expect(firstWinIdx).toBeGreaterThanOrEqual(0);
			for (const s of samples.slice(firstWinIdx)) {
				expect(s.holder === winnerHolder || s.holder === null).toBe(true);
			}

			// Release the winner; the slot drains to empty.
			winners[0]!.handle.release();
			await winners[0]!.handle.nextEvent(); // released event
			const drained = parentStore.probeOwner("ROOT");
			expect(drained === null || Date.now() / 1000 > drained.expiresAt).toBe(
				true,
			);

			// Everyone exits cleanly.
			await Promise.all(contenders.map((c) => c.stop()));
		} finally {
			for (const c of contenders) await c.stop().catch(() => undefined);
			parentStore.close();
		}
	});

	it("cross-process lineage-root keying: segments contend on one slot across processes", async () => {
		const parentStore = DbTurnLeaseStore.open(dbPath);
		let child: ContenderHandle | null = null;
		try {
			seedSessions(parentStore);

			// Child acquires via segment C1…
			child = spawnProcessContender(1, {
				op: "hold",
				session: "C1",
				holder: "seg-kid",
				ttlSeconds: 300,
				waitSeconds: 5,
				pollIntervalSeconds: 0.05,
				maxHoldMs: 10_000,
			});
			await child.nextEvent(); // ready
			const held = await child.nextEvent();
			expect(held.event).toBe("held");

			// …the parent resolving through C2 hits the SAME root-R slot.
			expect(
				parentStore.tryAcquire(
					"C2",
					structuredHolder("parent", process.pid),
					300,
				),
			).toBe(false);
			// An unrelated root stays independent.
			expect(
				parentStore.tryAcquire(
					"U",
					structuredHolder("parent", process.pid),
					300,
				),
			).toBe(true);
			parentStore.releaseHolder("U", structuredHolder("parent", process.pid));

			// After the child releases, the parent wins the lineage slot via C2.
			await child.stop();
			expect(
				parentStore.tryAcquire(
					"C2",
					structuredHolder("parent", process.pid),
					300,
				),
			).toBe(true);
			expect(parentStore.probeOwner("C1")?.conversationId).toBe("R");
		} finally {
			if (child) await child.stop().catch(() => undefined);
			parentStore.close();
		}
	});
});
