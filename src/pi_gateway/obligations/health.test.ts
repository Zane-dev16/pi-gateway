// Behavior contracts for the /status-bound obligations health snapshot
// (Q16: undelivered obligations surface in health). Pure query + shape —
// every assertion runs against seeded rows at an explicit injected instant.

import Database from "better-sqlite3";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { StateStore } from "../../pi_state/index.js";
import {
	MAX_ATTEMPTS,
	MAX_ROWS,
	STALE_AFTER_SECONDS,
	obligationHealthSnapshot,
	type OwnerStamp,
} from "./index.js";

let dir: string;
let store: StateStore;

beforeEach(async () => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-obligations-health-"));
	store = await StateStore.open(join(dir, "state.db"));
});

afterEach(async () => {
	await store.close();
	rmSync(dir, { recursive: true, force: true });
});

const OWNER: OwnerStamp = { pid: 960_001, startedAt: 5 };

function seed(
	id: string,
	spec: {
		state?: string;
		attempts?: number;
		created_at: number;
		updated_at?: number;
	},
): void {
	store.db
		.prepare(
			`INSERT OR REPLACE INTO delivery_obligations
			   (obligation_id, session_key, platform, chat_id, thread_id, content,
			    state, attempts, created_at, updated_at, owner_pid, owner_started_at)
			 VALUES (?, 'sk', 'telegram', 'c', NULL, 'body', ?, ?, ?, ?, ?, ?)`,
		)
		.run(
			id,
			spec.state ?? "pending",
			spec.attempts ?? 0,
			spec.created_at,
			spec.updated_at ?? spec.created_at,
			OWNER.pid,
			OWNER.startedAt,
		);
}

describe("obligationHealthSnapshot", () => {
	it("reports an empty ledger as all-zero with null ages and null next-retry", () => {
		const snap = obligationHealthSnapshot(store.db, 1_000_000);
		expect(snap.total).toBe(0);
		expect(snap.byState).toEqual({
			pending: 0,
			attempting: 0,
			delivered: 0,
			failed: 0,
			abandoned: 0,
		});
		expect(snap.undelivered).toBe(0);
		expect(snap.oldestUndeliveredAgeSeconds).toBeNull();
		expect(snap.nextRetryInSeconds).toBeNull();
		expect(snap.capacity).toEqual({ maxRows: MAX_ROWS, utilization: 0 });
	});

	it("counts by state, derives undelivered, oldest age, stale, exhausted, capacity", () => {
		const now = 2_000_000;
		seed("p-new", { created_at: now - 30 });
		seed("p-old", { created_at: now - STALE_AFTER_SECONDS + 60 }); // inside window
		seed("p-stale", { created_at: now - STALE_AFTER_SECONDS - 10 }); // past window
		seed("a-1", { state: "attempting", created_at: now - 120 });
		seed("f-1", {
			state: "failed",
			attempts: MAX_ATTEMPTS,
			created_at: now - 50,
		});
		seed("d-1", { state: "delivered", created_at: now - 10 });
		seed("x-1", { state: "abandoned", created_at: now - 10 });

		const snap = obligationHealthSnapshot(store.db, now);
		expect(snap.total).toBe(7);
		expect(snap.byState).toEqual({
			pending: 3,
			attempting: 1,
			delivered: 1,
			failed: 1,
			abandoned: 1,
		});
		expect(snap.undelivered).toBe(5); // pending+attempting+failed
		expect(snap.oldestUndeliveredAgeSeconds).toBe(STALE_AFTER_SECONDS + 10);
		expect(snap.staleUndelivered).toBe(1); // only p-stale is past the window
		expect(snap.exhaustedUndelivered).toBe(1); // f-1 spent the whole budget
		expect(snap.capacity.utilization).toBeCloseTo(7 / MAX_ROWS, 6);
	});

	it("nextRetryInSeconds is the minimum remaining backoff across pending/failed rows", () => {
		const now = 3_000_000;
		// updated_at = now-100, attempts=1 → due in 240-100 = 140s.
		seed("r-240", {
			state: "failed",
			attempts: 1,
			created_at: now - 500,
			updated_at: now - 100,
		});
		// updated_at = now-59, attempts=0 → due in 60-59 = 1s (the soonest).
		seed("r-60", { attempts: 0, created_at: now - 500, updated_at: now - 59 });
		seed("r-attempting", { state: "attempting", created_at: now - 500 }); // never scheduled

		const snap = obligationHealthSnapshot(store.db, now);
		expect(snap.nextRetryInSeconds).toBe(1);

		// Once everything is already due the value clamps to 0, not negative.
		const later = obligationHealthSnapshot(store.db, now + 500);
		expect(later.nextRetryInSeconds).toBe(0);
	});

	it("ignores terminal rows for scheduling but includes them in totals/capacity", () => {
		const now = 4_000_000;
		for (let i = 0; i < 3; i++) {
			seed(`del-${i}`, { state: "delivered", created_at: now });
		}
		seed("ab-1", { state: "abandoned", created_at: now });
		const snap = obligationHealthSnapshot(store.db, now);
		expect(snap.total).toBe(4);
		expect(snap.undelivered).toBe(0);
		expect(snap.nextRetryInSeconds).toBeNull(); // nothing schedulable
		expect(snap.capacity.utilization).toBeCloseTo(4 / MAX_ROWS, 6);
	});
});
