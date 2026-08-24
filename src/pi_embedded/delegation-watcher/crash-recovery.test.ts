// CRASH-RECOVERY contracts (06 §7.1 boot restore × §7.2 watcher; 06 §10
// "Async rail crash recovery" row at the WATCHER layer):
//
//   Generation 1 dispatches work and dies holding claims / mid-dispatch.
//   Generation 2 boots: restore_undelivered runs recover_abandoned FIRST
//   (dead owner ⇒ synthesized status:'unknown' completion), prunes
//   replay-stale pendings, then every undelivered row is claimed through the
//   NORMAL handshake (stale takeover after >300s) and delivered EXACTLY ONCE.
//   Generation 3 finds nothing left — total deliveries across all restarts
//   is one per delegation.
//
// The cross-process SIGKILL/WAL durability of the ROWS themselves is proven
// by pi_gateway/delegation/two-process.test.ts; these contracts drive the
// watcher layer over the same store file with deterministic owner-liveness
// injection (no child processes needed here).

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { describe, expect, it } from "vitest";

import { DelegationRail } from "../../pi_gateway/delegation/index.js";
import { StateStore } from "../../pi_state/index.js";
import { DelegationWatcher } from "./index.js";
import {
	buildHarnessOn,
	FakeLiveness,
	RecordingDispatcher,
	seedRouting,
	seedSession,
	type WatcherHarness,
} from "./testing/harness.js";
import { ManualClock } from "./testing/manual-clock.js";

const T0 = 1_775_000_000;
const KEY_A = "agent:main:telegram:dm:100";
const KEY_B = "agent:main:telegram:dm:200";

/** Generation 1: dispatch work, publish one completion, claim it, "die". */
async function generationOne(): Promise<{
	dir: string;
	dbPath: string;
	harness: WatcherHarness;
}> {
	const dir = mkdtempSync(join(tmpdir(), "pi-gw-delegation-crash-"));
	const dbPath = join(dir, "state.db");
	const clock = new ManualClock();
	clock.setSeconds(T0);
	const store = await StateStore.open(dbPath);
	const h = await buildHarnessOn(dir, dbPath, store, clock);

	await seedSession(h, "a-parent", { sessionKey: KEY_A });
	await seedRouting(h, KEY_A, "a-parent");
	await seedSession(h, "b-parent", { sessionKey: KEY_B });
	await seedRouting(h, KEY_B, "b-parent");

	// Row A: dispatched, NEVER completed — owner died mid-work.
	await h.rail.recordDispatch({
		delegationId: "dlg-abandoned",
		originSession: KEY_A,
		parentSessionId: "a-parent",
		task: { goal: "long research" },
	});

	// Row B: completed durably, CLAIMED — killed between claim-write and ack.
	await h.rail.recordDispatch({
		delegationId: "dlg-killed-mid-delivery",
		originSession: KEY_B,
		parentSessionId: "b-parent",
		task: { goal: "build artifact" },
	});
	await h.rail.publishCompletion({
		delegationId: "dlg-killed-mid-delivery",
		event: {
			type: "async_delegation",
			delegation_id: "dlg-killed-mid-delivery",
			session_key: KEY_B,
			parent_session_id: "b-parent",
			goal: "build artifact",
			status: "completed",
			summary: "artifact built",
			completed_at: T0,
		},
		result: { status: "completed", summary: "artifact built" },
	});
	expect(
		await h.rail.claimCompletion(
			"dlg-killed-mid-delivery",
			h.rail.makeClaimId("gateway"),
		),
	).toBe(true);

	await h.close(); // WAL-committed rows survive the "crash"
	return { dir, dbPath, harness: h };
}

describe("watcher crash-recovery interplay with rail restore-on-boot", () => {
	it("dead generation's rows are recovered + delivered EXACTLY once across restarts", async () => {
		const gen1 = await generationOne();

		// ---- GENERATION 2: boot 301s later; the old owner is provably dead --
		const clock2 = new ManualClock();
		clock2.setSeconds(T0 + 301); // > CLAIM_STALE_SECONDS ⇒ stale takeover wins
		const store2 = await StateStore.open(gen1.dbPath);
		try {
			const rail2 = new DelegationRail(store2.db, {
				clock: clock2,
				processAlive: () => false, // injected liveness truth: owner is gone
			});
			const dispatcher = new RecordingDispatcher();
			const liveness = new FakeLiveness(); // targets idle post-restart
			const watcher = new DelegationWatcher({
				db: store2.db,
				liveness,
				dispatcher,
				clock: clock2,
				rail: rail2, // boot() must run the SAME liveness-injected rail
			});

			// Boot restore: abandoned dispatch synthesizes 'unknown'; both rows listed.
			const boot = await watcher.boot();
			expect(boot.restored).toBe(2);

			// One drain cycle delivers BOTH rows — each EXACTLY once.
			const tick = await watcher.tick();
			expect(tick.delivered).toBe(2);
			expect(dispatcher.events).toHaveLength(2);

			const texts = dispatcher.texts().join("\n---\n");
			expect(texts).toContain("did not complete successfully"); // synthesized unknown
			expect(texts).toContain("status=unknown");
			expect(texts).toContain("artifact built"); // crashed-mid-delivery payload

			const stateOf = (id: string): Record<string, unknown> =>
				store2.db
					.prepare(
						"SELECT delivery_state, delivery_attempts FROM async_delegations WHERE delegation_id = ?",
					)
					.get(id) as Record<string, unknown>;
			expect(stateOf("dlg-abandoned")).toMatchObject({
				delivery_state: "delivered",
				delivery_attempts: 1, // gen2's claim only
			});
			expect(stateOf("dlg-killed-mid-delivery")).toMatchObject({
				delivery_state: "delivered",
				delivery_attempts: 2, // doomed gen1 claim + gen2 stale takeover
			});
		} finally {
			store2.close(false);
		}

		// ---- GENERATION 3: nothing undelivered remains ----------------------
		const clock3 = new ManualClock();
		clock3.setSeconds(T0 + 400);
		const store3 = await StateStore.open(gen1.dbPath);
		try {
			const d3 = new RecordingDispatcher();
			const w3 = new DelegationWatcher({
				db: store3.db,
				liveness: { isBusy: (): boolean => false },
				dispatcher: d3,
				clock: clock3,
				rail: new DelegationRail(store3.db, { clock: clock3 }),
			});
			const boot3 = await w3.boot();
			expect(boot3.restored).toBe(0);
			const tick3 = await w3.tick();
			expect(tick3.pending).toBe(0);
			expect(d3.events).toHaveLength(0);
		} finally {
			store3.close(false);
			rmSync(gen1.dir, { recursive: true, force: true });
		}
	});
});
