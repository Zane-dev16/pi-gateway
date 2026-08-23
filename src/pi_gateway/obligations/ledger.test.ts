// Behavior contracts for the delivery-obligations ledger (DEC-007; Q16;
// 02-session-and-state.md §11 GC table). Ported from Hermes
// gateway/delivery_ledger.py + its tests (tests/gateway/test_delivery_ledger.py).
// Races, mutations, state machines, byte-exact ids — no change detectors.
//
// Every time observation flows through ManualClock; every engine gets an
// injected OwnerStamp so liveness never depends on the host PID space.

import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { StateStore } from "../../pi_state/index.js";
import { ManualClock, ScriptedSender } from "./testing/manual-clock.js";
import {
	DeliveryLedger,
	IllegalTransitionError,
	MAX_ATTEMPTS,
	MAX_ROWS,
	ObligationNotFoundError,
	RETRY_BASE_SECONDS,
	STALE_AFTER_SECONDS,
	composeDeliveryContent,
	computeObligationId,
	nextRetryDelaySeconds,
	readProcessStartTime,
	type DeliveryLedgerOptions,
	type NewObligation,
	type ObligationState,
	type OwnerStamp,
} from "./index.js";
import { RECOVERED_MARKER } from "./sender.js";

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-obligations-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

// Fixed identity space so liveness is fully deterministic per test.
const DEAD_PID = 990_001;
const ALIVE_A_PID = 990_002;
const ALIVE_B_PID = 990_003;
const START_TIMES = new Map<number, number>([
	[DEAD_PID, 4_242],
	[ALIVE_A_PID, 111],
	[ALIVE_B_PID, 222],
]);
const DEAD_STAMP: OwnerStamp = { pid: DEAD_PID, startedAt: 4_242 };
const ALIVE_A_STAMP: OwnerStamp = { pid: ALIVE_A_PID, startedAt: 111 };
const ALIVE_B_STAMP: OwnerStamp = { pid: ALIVE_B_PID, startedAt: 222 };

function fakeEnv(): Pick<
	DeliveryLedgerOptions,
	"processAlive" | "processStartTime"
> {
	return {
		processAlive: (pid) => START_TIMES.has(pid) && pid !== DEAD_PID,
		// A dead process's /proc entry is unreadable → null (falls back to probe).
		processStartTime: (pid) =>
			pid === DEAD_PID ? null : (START_TIMES.get(pid) ?? null),
	};
}

async function makeLedger(opts?: {
	selfStamp?: OwnerStamp;
	clock?: ManualClock;
}): Promise<{ ledger: DeliveryLedger; store: StateStore; clock: ManualClock }> {
	const store = await StateStore.open(
		join(dir, `db-${Math.random().toString(36).slice(2)}.db`),
	);
	const clock = opts?.clock ?? new ManualClock();
	const ledger = new DeliveryLedger(store.db, {
		clock,
		selfStamp: opts?.selfStamp ?? ALIVE_A_STAMP,
		...fakeEnv(),
	});
	return { ledger, store, clock };
}

function sample(overrides: Partial<NewObligation> = {}): NewObligation {
	return {
		sessionKey: "telegram|chat|100",
		platform: "telegram",
		chatId: "100",
		threadId: null,
		content: "the final answer",
		messageRef: "msg-1",
		...overrides,
	};
}

/** Direct seed with full control over timestamps/owner/state. */
function seedRow(
	db: StateStore["db"],
	spec: {
		id: string;
		state?: string;
		attempts?: number;
		created_at: number;
		updated_at: number;
		owner?: OwnerStamp | null;
		platform?: string;
		content?: string;
		last_error?: string | null;
	},
): void {
	db.prepare(
		`INSERT OR REPLACE INTO delivery_obligations
		   (obligation_id, session_key, platform, chat_id, thread_id, content,
		    state, attempts, created_at, updated_at, owner_pid, owner_started_at, last_error)
		 VALUES (?, 'sk', ?, 'chat', NULL, ?, ?, ?, ?, ?, ?, ?, ?)`,
	).run(
		spec.id,
		spec.platform ?? "telegram",
		spec.content ?? "seeded",
		spec.state ?? "pending",
		spec.attempts ?? 0,
		spec.created_at,
		spec.updated_at,
		spec.owner?.pid ?? null,
		spec.owner?.startedAt ?? null,
		spec.last_error ?? null,
	);
}

describe("obligation id (parity compute_obligation_id)", () => {
	it("is byte-stable and content-distinct", async () => {
		const a = computeObligationId("sk1", "msg1", "hello");
		expect(a).toBe(computeObligationId("sk1", "msg1", "hello"));
		expect(a).not.toBe(computeObligationId("sk1:threadB", "msg1", "hello"));
		expect(a).not.toBe(computeObligationId("sk1", "msg2", "hello"));
		expect(a).not.toBe(computeObligationId("sk1", "msg1", "other"));
		expect(a).toHaveLength(24);
		// Independent recomputation of the documented formula.
		const expected = createHash("sha256")
			.update("sk1|msg1|hello", "utf8")
			.digest("hex")
			.slice(0, 24);
		expect(a).toBe(expected);
	});

	it("record() re-uses a supplied id idempotently (REPLACE resets to pending)", async () => {
		const { ledger, store } = await makeLedger();
		const id = await ledger.record(sample({ obligationId: "fixed-id" }), {
			nowSeconds: 5,
		});
		await ledger.record(sample({ obligationId: "fixed-id" }), {
			nowSeconds: 9,
		});
		const row = store.db
			.prepare(
				"SELECT * FROM delivery_obligations WHERE obligation_id='fixed-id'",
			)
			.get() as { state: string; attempts: number; updated_at: number };
		expect(id).toBe("fixed-id");
		expect(row.state).toBe("pending");
		expect(row.attempts).toBe(0);
		expect(row.updated_at).toBe(9);
		await store.close();
	});
});

describe("state machine transitions", () => {
	it("accepts exactly the legal edge set", async () => {
		const { ledger, store } = await makeLedger();
		const mk = async (start: ObligationState): Promise<string> => {
			seedRow(store.db, {
				id: `row-${start}`,
				state: start,
				created_at: 1,
				updated_at: 1,
			});
			return `row-${start}`;
		};
		const ops: Record<
			"beginAttempt" | "markDelivered" | "markFailed",
			(id: string) => Promise<boolean>
		> = {
			beginAttempt: (id) => ledger.beginAttempt(id),
			markDelivered: (id) => ledger.markDelivered(id),
			markFailed: (id) => ledger.markFailed(id, ""),
		};
		const ok = async (
			from: ObligationState,
			op: "beginAttempt" | "markDelivered" | "markFailed",
			to: string,
		): Promise<void> => {
			const id = await mk(from);
			await ops[op](id);
			expect(ledger.stateOf(id)).toBe(to);
		};

		// pending edges
		await ok("pending", "beginAttempt", "attempting");
		await ok("pending", "markDelivered", "delivered");
		await ok("pending", "markFailed", "failed");
		// attempting edges
		await ok("attempting", "markDelivered", "delivered");
		await ok("attempting", "markFailed", "failed");
		// failed edges (retry boundary)
		await ok("failed", "beginAttempt", "attempting");
		await ok("failed", "markDelivered", "delivered");
		await store.close();
	});

	it("rejects illegal transitions and leaves the row untouched", async () => {
		const { ledger, store } = await makeLedger();
		const seeds: Array<[string, ObligationState]> = [
			["att-1", "attempting"],
			["del-1", "delivered"],
			["fail-1", "failed"],
			["aban-1", "abandoned"],
		];
		for (const [id, state] of seeds) {
			seedRow(store.db, { id, state, created_at: 1, updated_at: 1 });
		}
		const expectIllegal = async (
			run: () => Promise<unknown>,
			id: string,
			from: string,
			to: string,
		): Promise<void> => {
			await expect(run).rejects.toMatchObject({
				name: "IllegalTransitionError",
				obligationId: id,
				from,
				to,
			});
		};

		// terminal states are immutable
		await expectIllegal(
			() => ledger.markDelivered("del-1"),
			"del-1",
			"delivered",
			"delivered",
		);
		await expectIllegal(
			() => ledger.markFailed("del-1", "x"),
			"del-1",
			"delivered",
			"failed",
		);
		await expectIllegal(
			() => ledger.beginAttempt("del-1"),
			"del-1",
			"delivered",
			"attempting",
		);
		for (const [op, to] of [
			["beginAttempt", "attempting"],
			["markDelivered", "delivered"],
			["markFailed", "failed"],
		] as const) {
			const run =
				op === "beginAttempt"
					? (): Promise<boolean> => ledger.beginAttempt("aban-1")
					: op === "markDelivered"
						? (): Promise<boolean> => ledger.markDelivered("aban-1")
						: (): Promise<boolean> => ledger.markFailed("aban-1", "e");
			await expectIllegal(run, "aban-1", "abandoned", to);
		}
		// no self-edge for failed, and attempting cannot restart
		await expectIllegal(
			() => ledger.markFailed("fail-1", "again"),
			"fail-1",
			"failed",
			"failed",
		);
		await expectIllegal(
			() => ledger.beginAttempt("att-1"),
			"att-1",
			"attempting",
			"attempting",
		);

		// rows unchanged after all rejections
		const states = store.db
			.prepare(
				"SELECT obligation_id, state FROM delivery_obligations ORDER BY obligation_id",
			)
			.all() as unknown as Array<{ obligation_id: string; state: string }>;
		expect(
			Object.fromEntries(states.map((r) => [r.obligation_id, r.state])),
		).toEqual({
			"aban-1": "abandoned",
			"att-1": "attempting",
			"del-1": "delivered",
			"fail-1": "failed",
		});
		// The typed error is the exported class (structural + identity check).
		await expect(ledger.markDelivered("del-1")).rejects.toBeInstanceOf(
			IllegalTransitionError,
		);
		await store.close();
	});

	it("throws NotFound for unknown ids", async () => {
		const { ledger, store } = await makeLedger();
		await expect(ledger.markDelivered("nope")).rejects.toBeInstanceOf(
			ObligationNotFoundError,
		);
		await store.close();
	});

	it("truncates last_error to 500 chars and stores NULL when empty (parity _update_state)", async () => {
		const { ledger, store } = await makeLedger();
		const longError = "x".repeat(600);
		await ledger.record(sample({ obligationId: "err-1" }));
		await ledger.beginAttempt("err-1");
		await ledger.markFailed("err-1", longError);
		let row = ledger.row("err-1");
		expect(row?.last_error).toHaveLength(500);
		expect(row?.state).toBe("failed");

		seedRow(store.db, { id: "err-2", created_at: 1, updated_at: 1 });
		await ledger.markFailed("err-2", "");
		row = ledger.row("err-2");
		expect(row?.last_error).toBeNull();
		await store.close();
	});
});

describe("crash-recovery sweep (parity sweep_recoverable)", () => {
	it("claims dead-owned pending rows plainly and dead-owned attempting/failed rows with marker", async () => {
		const { ledger, store } = await makeLedger();
		seedRow(store.db, {
			id: "p1",
			created_at: 10,
			updated_at: 10,
			owner: DEAD_STAMP,
			state: "pending",
		});
		seedRow(store.db, {
			id: "a1",
			created_at: 10,
			updated_at: 10,
			owner: DEAD_STAMP,
			state: "attempting",
			attempts: 1,
		});
		seedRow(store.db, {
			id: "f1",
			created_at: 10,
			updated_at: 10,
			owner: DEAD_STAMP,
			state: "failed",
			attempts: 2,
		});
		const claimed = await ledger.sweepRecoverable({ nowSeconds: 20 });
		const byId = new Map(claimed.map((c) => [c.obligationId, c]));
		expect(claimed).toHaveLength(3);
		expect(byId.get("p1")?.needsMarker).toBe(false);
		expect(byId.get("a1")?.needsMarker).toBe(true);
		expect(byId.get("f1")?.needsMarker).toBe(true);
		expect(byId.get("p1")?.attempts).toBe(1); // attempts incremented on claim
		expect(byId.get("f1")?.attempts).toBe(3);
		// ownership re-stamped to the sweeping engine
		expect(ledger.row("p1")?.owner_pid).toBe(ALIVE_A_STAMP.pid);
		// a second sweep claims nothing: live owner now holds the rows
		expect(await ledger.sweepRecoverable({ nowSeconds: 21 })).toEqual([]);
		await store.close();
	});

	it("never claims rows owned by a live process", async () => {
		const { ledger, store } = await makeLedger({ selfStamp: ALIVE_B_STAMP });
		seedRow(store.db, {
			id: "live-1",
			created_at: 10,
			updated_at: 10,
			owner: ALIVE_A_STAMP,
		});
		expect(await ledger.sweepRecoverable({ nowSeconds: 20 })).toEqual([]);
		expect(ledger.row("live-1")?.attempts).toBe(0);
		await store.close();
	});

	it("treats NULL-owner rows as claimable (dead parity)", async () => {
		const { ledger, store } = await makeLedger();
		seedRow(store.db, {
			id: "orphan-1",
			created_at: 10,
			updated_at: 10,
			owner: null,
		});
		const claimed = await ledger.sweepRecoverable({ nowSeconds: 20 });
		expect(claimed.map((c) => c.obligationId)).toEqual(["orphan-1"]);
		await store.close();
	});

	it("abandons rows past the 3-attempt cap at claim time — 4th attempt refused", async () => {
		const { store } = await makeLedger();
		const sender = new ScriptedSender().alwaysFail("platform down");
		seedRow(store.db, {
			id: "cap-1",
			created_at: 10,
			updated_at: 10,
			owner: DEAD_STAMP,
		});
		// Each redelivery belongs to a FRESH boot whose process then dies — the
		// restart boundary is Hermes' only retry trigger, so a doomed boot per
		// round is the faithful model. Its owner probe reads everything dead.
		const doomedBoot = (boot: number): DeliveryLedger =>
			new DeliveryLedger(store.db, {
				clock: new ManualClock(10 + boot),
				selfStamp: { pid: 990_100 + boot, startedAt: 5_000 + boot },
				processAlive: () => false,
				processStartTime: () => null,
			});
		for (let round = 1; round <= MAX_ATTEMPTS; round++) {
			const boot = doomedBoot(round);
			const claimed = await boot.sweepRecoverable({ nowSeconds: 10 + round });
			expect(claimed.map((c) => c.attempts)).toEqual([round]);
			const reports = await boot.driveClaimed(claimed, sender.bind(), {
				nowSeconds: 10 + round,
			});
			expect(reports.every((r) => !r.ok)).toBe(true);
			expect(boot.stateOf("cap-1")).toBe("failed");
		}
		// 4th boot: claim refused → abandoned, sender NOT invoked again.
		const fourth = await doomedBoot(4).sweepRecoverable({ nowSeconds: 50 });
		expect(fourth).toEqual([]);
		expect(
			(
				store.db
					.prepare(
						"SELECT state FROM delivery_obligations WHERE obligation_id='cap-1'",
					)
					.get() as { state: string }
			).state,
		).toBe("abandoned");
		expect(sender.callCount).toBe(MAX_ATTEMPTS);
		await store.close();
	});

	it("abandons rows older than the 24h stale window (injected clock, strict >)", async () => {
		const { ledger, store } = await makeLedger();
		seedRow(store.db, {
			id: "stale-1",
			created_at: 0,
			updated_at: 86_400 - 1,
			owner: DEAD_STAMP,
		});
		// Boundary: age == STALE_AFTER_SECONDS is NOT stale (strict > in Hermes).
		const boundary = await ledger.sweepRecoverable({
			nowSeconds: STALE_AFTER_SECONDS,
		});
		expect(boundary.map((c) => c.obligationId)).toEqual(["stale-1"]);
		await ledger.markFailed("stale-1", "reset for next case");

		seedRow(store.db, {
			id: "stale-2",
			created_at: 0,
			updated_at: 10,
			owner: DEAD_STAMP,
		});
		const past = await ledger.sweepRecoverable({
			nowSeconds: STALE_AFTER_SECONDS + 1,
		});
		expect(past).toEqual([]); // stale-2 abandoned, not claimed
		expect(ledger.stateOf("stale-2")).toBe("abandoned");
		// stale-1 was claimed+failed earlier; it ages on ITS OWN created_at.
		expect(ledger.stateOf("stale-1")).toBe("failed");
		await store.close();
	});

	it("does not burn the redelivery budget when the platform is absent this boot", async () => {
		const { ledger, store } = await makeLedger();
		seedRow(store.db, {
			id: "absent-1",
			created_at: 10,
			updated_at: 10,
			owner: DEAD_STAMP,
			platform: "discord",
		});
		const telegramOnly = new Set(["telegram"]);
		expect(
			await ledger.sweepRecoverable({
				nowSeconds: 20,
				deliverablePlatforms: telegramOnly,
			}),
		).toEqual([]);
		const row = ledger.row("absent-1");
		expect(row?.attempts).toBe(0); // budget untouched
		expect(row?.state).toBe("pending"); // left untouched for a later boot
		expect(row?.owner_pid).toBe(DEAD_STAMP.pid);

		const claimed = await ledger.sweepRecoverable({ nowSeconds: 21 });
		expect(claimed.map((c) => c.obligationId)).toEqual(["absent-1"]); // platform returns → delivers
		await store.close();
	});
});

describe("retention GC (parity _prune)", () => {
	it("prunes delivered rows after the 7d confirmation window; undelivered retained", async () => {
		const { ledger, store, clock } = await makeLedger();
		await ledger.record(sample({ obligationId: "will-deliver" }), {
			nowSeconds: 100,
		});
		await ledger.beginAttempt("will-deliver", { nowSeconds: 100 });
		await ledger.markDelivered("will-deliver", { nowSeconds: 100 });
		await ledger.record(
			sample({ obligationId: "still-owed", messageRef: "m2" }),
			{
				nowSeconds: 101,
			},
		);

		// Before the window: nothing pruned even though record() ran prune().
		await ledger.record(
			sample({ obligationId: "trigger-1", messageRef: "m3" }),
			{
				nowSeconds: 100 + 7 * 24 * 3600 - 1,
			},
		);
		expect(ledger.stateOf("will-deliver")).toBe("delivered");

		// After the window: the next record()'s prune removes ONLY the delivered row.
		await ledger.record(
			sample({ obligationId: "trigger-2", messageRef: "m4" }),
			{
				nowSeconds: 100 + 7 * 24 * 3600 + 1,
			},
		);
		expect(ledger.row("will-deliver")).toBeNull();
		expect(ledger.stateOf("still-owed")).toBe("pending"); // undelivered retained
		expect(ledger.stateOf("trigger-1")).toBe("pending");
		expect(ledger.stateOf("trigger-2")).toBe("pending");
		void clock;
		await store.close();
	});

	it("prunes abandoned rows after their own 7d inspection window", async () => {
		const { ledger, store } = await makeLedger();
		seedRow(store.db, {
			id: "poison",
			created_at: 0,
			updated_at: 50,
			owner: DEAD_STAMP,
		});
		await ledger.sweepRecoverable({ nowSeconds: STALE_AFTER_SECONDS + 1 }); // abandoned @ that now
		const abandonedAt = ledger.row("poison")?.updated_at ?? 0;
		expect(ledger.stateOf("poison")).toBe("abandoned");

		await ledger.record(
			sample({ obligationId: "gc-trigger", messageRef: "gx" }),
			{
				nowSeconds: abandonedAt + 7 * 24 * 3600 + 1,
			},
		);
		expect(ledger.row("poison")).toBeNull();
		await store.close();
	});
});

describe("500-row cap admission (parity _prune eviction)", () => {
	function countRows(db: StateStore["db"]): number {
		return (
			db.prepare("SELECT COUNT(*) AS n FROM delivery_obligations").get() as {
				n: number;
			}
		).n;
	}

	it("evicts delivered first, then abandoned, then oldest active — Hermes rank order", async () => {
		const { ledger, store } = await makeLedger();
		const t0 = 1_000_000;
		// 498 active pending rows...
		for (let i = 0; i < 498; i++) {
			seedRow(store.db, {
				id: `act-${i}`,
				created_at: t0 + i,
				updated_at: t0 + i,
				owner: ALIVE_A_STAMP,
			});
		}
		// ...3 delivered (oldest updates), 4 abandoned (newer updates) → 505 total.
		for (let i = 0; i < 3; i++) {
			seedRow(store.db, {
				id: `del-${i}`,
				state: "delivered",
				created_at: t0,
				updated_at: t0 + 10 + i,
			});
		}
		for (let i = 0; i < 4; i++) {
			seedRow(store.db, {
				id: `aban-${i}`,
				state: "abandoned",
				created_at: t0,
				updated_at: t0 + 20 + i,
			});
		}
		expect(countRows(store.db)).toBe(505);

		const deleted = await ledger.prune({ nowSeconds: t0 + 999 });
		expect(deleted).toBe(5); // excess = 505 - 500
		expect(countRows(store.db)).toBe(MAX_ROWS);

		const survivors = (id: string): string | null =>
			(store.db
				.prepare("SELECT state FROM delivery_obligations WHERE obligation_id=?")
				.get(id) as { state: string } | null | undefined)
				? (
						store.db
							.prepare(
								"SELECT state FROM delivery_obligations WHERE obligation_id=?",
							)
							.get(id) as { state: string }
					).state
				: null;

		// All delivered evicted (rank 0, oldest-first would take del-0..2 before any aban).
		for (let i = 0; i < 3; i++) expect(survivors(`del-${i}`)).toBeNull();
		// Spill took the two OLDEST abandoned only.
		expect(survivors("aban-0")).toBeNull();
		expect(survivors("aban-1")).toBeNull();
		expect(survivors("aban-2")).not.toBeNull();
		expect(survivors("aban-3")).not.toBeNull();
		// No active row touched while lower ranks could absorb the excess.
		for (let i = 0; i < 498; i++) expect(survivors(`act-${i}`)).not.toBeNull();
		await store.close();
	});

	it("evicts OLDEST active rows once delivered/abandoned ranks are exhausted", async () => {
		const { ledger, store } = await makeLedger();
		const t0 = 2_000_000;
		for (let i = 0; i < MAX_ROWS + 5; i++) {
			seedRow(store.db, {
				id: `only-active-${String(i).padStart(3, "0")}`,
				created_at: t0 + i,
				updated_at: t0 + i,
				owner: ALIVE_A_STAMP,
			});
		}
		await ledger.prune({ nowSeconds: t0 + 10_000 });
		const remaining = store.db
			.prepare(
				"SELECT obligation_id FROM delivery_obligations ORDER BY obligation_id",
			)
			.all() as unknown as Array<{ obligation_id: string }>;
		expect(remaining).toHaveLength(MAX_ROWS);
		const ids = remaining.map((r) => r.obligation_id);
		expect(ids.includes("only-active-000")).toBe(false); // oldest evicted
		expect(ids.includes("only-active-004")).toBe(false);
		expect(ids.includes("only-active-005")).toBe(true); // newest kept
		await store.close();
	});

	it("record() keeps total ≤ 500 continuously (admission path)", async () => {
		const { ledger, store } = await makeLedger();
		for (let i = 0; i < MAX_ROWS + 12; i++) {
			await ledger.record(
				sample({ obligationId: `bulk-${i}`, messageRef: `bm-${i}` }),
			);
		}
		expect(countRows(store.db)).toBe(MAX_ROWS);
		// Newest survive; the earliest were recycled.
		expect(ledger.row("bulk-0")).toBeNull();
		expect(ledger.row(`bulk-${MAX_ROWS + 11}`)).not.toBeNull();
		await store.close();
	});
});

describe("concurrent engines (two ledgers, one DB) never double-send", () => {
	it("exactly one racing sweep wins the guarded claim", async () => {
		const path = join(dir, "shared.db");
		const storeA = await StateStore.open(path);
		const storeB = await StateStore.open(path);
		const clockA = new ManualClock(500);
		const ledgerA = new DeliveryLedger(storeA.db, {
			clock: clockA,
			selfStamp: ALIVE_A_STAMP,
			...fakeEnv(),
		});
		const ledgerB = new DeliveryLedger(storeB.db, {
			clock: clockA,
			selfStamp: ALIVE_B_STAMP,
			...fakeEnv(),
		});

		seedRow(storeA.db, {
			id: "race-1",
			created_at: 100,
			updated_at: 100,
			owner: DEAD_STAMP,
			content: "one copy only",
		});

		const senderA = new ScriptedSender().alwaysFail("unused"); // outcomes irrelevant here
		const senderB = new ScriptedSender();
		const [claimedA, claimedB] = await Promise.all([
			ledgerA.sweepRecoverable({ nowSeconds: 200 }),
			ledgerB.sweepRecoverable({ nowSeconds: 200 }),
		]);
		const winners = [...claimedA, ...claimedB];
		expect(winners.map((c) => c.obligationId).sort()).toEqual(["race-1"]);

		// Drive through whichever engine won; total sends across BOTH === 1.
		const winnerLedger = claimedA.length > 0 ? ledgerA : ledgerB;
		const winnerSender = claimedA.length > 0 ? senderA : senderB;
		const claimed = claimedA.length > 0 ? claimedA : claimedB;
		await winnerLedger.driveClaimed(claimed, winnerSender.bind(), {
			nowSeconds: 201,
		});
		expect(senderA.callCount + senderB.callCount).toBe(1);

		// Repeated sweeps by both engines find nothing further.
		const [againA, againB] = await Promise.all([
			ledgerA.sweepRecoverable({ nowSeconds: 300 }),
			ledgerB.sweepRecoverable({ nowSeconds: 300 }),
		]);
		expect(againA.length + againB.length).toBe(0);
		await storeA.close();
		await storeB.close();
	});

	it("a live foreign engine's rows are invisible to another engine's due-retry pass", async () => {
		const path = join(dir, "shared2.db");
		const storeA = await StateStore.open(path);
		const storeB = await StateStore.open(path);
		const clock = new ManualClock(1_000);
		const ledgerA = new DeliveryLedger(storeA.db, {
			clock,
			selfStamp: ALIVE_A_STAMP,
			...fakeEnv(),
		});
		const ledgerB = new DeliveryLedger(storeB.db, {
			clock,
			selfStamp: ALIVE_B_STAMP,
			...fakeEnv(),
		});
		// A records + begins a send (attempting, owned by A, alive).
		const id = await ledgerA.record(sample({ obligationId: "inflight" }), {
			nowSeconds: 1_000,
		});
		await ledgerA.beginAttempt(id, { nowSeconds: 1_000 });
		// B's due-retry pass must not touch A's row even far past any backoff.
		expect(
			await ledgerB.claimDueRetries({ nowSeconds: 1_000 + 40_000 }),
		).toEqual([]);
		expect(
			ledgerB.sweepRecoverable({ nowSeconds: 1_000 + 40_000 }),
		).resolves.toEqual([]);
		await storeA.close();
		await storeB.close();
	});
});

describe("inline delivery (parity platforms/base.py record→attempting→send→settle)", () => {
	it("happy path lands delivered without marker", async () => {
		const { ledger, store } = await makeLedger();
		const sender = new ScriptedSender();
		const report = await ledger.deliverNew(sample(), sender.bind());
		expect(report.outcome?.ok).toBe(true);
		expect(sender.calls[0]?.needsMarker).toBe(false);
		expect(sender.calls[0]?.content).toBe("the final answer");
		expect(sender.calls[0]?.attempts).toBe(0);
		expect(sender.calls[0]?.chatId).toBe("100");
		expect(ledger.stateOf(report.obligationId)).toBe("delivered");
		await store.close();
	});

	it("definitive rejection settles failed with the sender error", async () => {
		const { ledger, store } = await makeLedger();
		const sender = new ScriptedSender().queue("fail");
		const report = await ledger.deliverNew(sample(), sender.bind());
		expect(report.outcome).toMatchObject({
			ok: false,
			error: "scripted failure",
		});
		const row = ledger.row(report.obligationId);
		expect(row?.state).toBe("failed");
		expect(row?.last_error).toBe("scripted failure");
		expect(row?.attempts).toBe(0); // inline sends never spend the redelivery budget
		await store.close();
	});

	it("thrown sends become ok:false ('send failed' parity)", async () => {
		const { ledger, store } = await makeLedger();
		const boom = async (): Promise<never> => {
			throw new Error("socket exploded");
		};
		const report = await ledger.deliverNew(sample(), boom);
		expect(report.outcome).toMatchObject({
			ok: false,
			error: "socket exploded",
		});
		expect(ledger.row(report.obligationId)?.last_error).toBe("socket exploded");
		await store.close();
	});

	it("recovery drives compose the visible recovered-reply marker (run.py driver parity)", async () => {
		const { ledger, store } = await makeLedger();
		seedRow(store.db, {
			id: "marker-1",
			created_at: 10,
			updated_at: 10,
			owner: DEAD_STAMP,
			state: "attempting",
			content: "body text",
		});
		const claimed = await ledger.sweepRecoverable({ nowSeconds: 20 });
		const sender = new ScriptedSender();
		await ledger.driveClaimed(claimed, sender.bind(), { nowSeconds: 20 });
		expect(sender.calls[0]?.content.startsWith(RECOVERED_MARKER)).toBe(true);
		expect(sender.calls[0]?.content.endsWith("body text")).toBe(true);
		expect(sender.calls[0]?.needsMarker).toBe(true);
		await store.close();
	});
});

describe("backoff schedule primitives", () => {
	it("grows monotonically and caps at one hour", () => {
		expect(nextRetryDelaySeconds(0)).toBe(RETRY_BASE_SECONDS);
		expect(nextRetryDelaySeconds(1)).toBe(240);
		expect(nextRetryDelaySeconds(2)).toBe(960);
		expect(nextRetryDelaySeconds(3)).toBe(3600);
		expect(nextRetryDelaySeconds(99)).toBe(3600);
		for (let a = 0; a < 20; a++) {
			expect(nextRetryDelaySeconds(a + 1)).toBeGreaterThanOrEqual(
				nextRetryDelaySeconds(a),
			);
		}
	});

	it("composeDeliveryContent prepends iff needed", () => {
		expect(composeDeliveryContent("hi", false)).toBe("hi");
		expect(composeDeliveryContent("hi", true)).toBe(`${RECOVERED_MARKER}hi`);
	});
});

describe("process stamp helpers", () => {
	it("readProcessStartTime parses this live process or degrades to null off-Linux", () => {
		if (process.platform !== "linux") {
			expect(readProcessStartTime(process.pid)).toBeNull();
			return;
		}
		const start = readProcessStartTime(process.pid);
		expect(start).not.toBeNull();
		expect(start).toBe(readProcessStartTime(process.pid)); // stable across reads
	});
});
