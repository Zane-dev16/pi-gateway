// Behavior contracts for the true-inactivity runaway bound (07 §5.2 runaway
// row; roadmap "inactivity interrupt fires only on TRUE inactivity"). Every
// timing assertion runs on ManualClock — a wall-clock reading anywhere would
// make these tests lie, so none exists.

import { describe, expect, it } from "vitest";

import {
	TimestampActivityLog,
	INACTIVITY_POLL_SECONDS,
	resolveInactivityLimitSeconds,
	runWithInactivityBound,
} from "./inactivity.js";
import { Gate, ManualClock } from "./testing/manual-clock.js";

describe("resolveInactivityLimitSeconds (HERMES_CRON_TIMEOUT parse)", () => {
	it("defaults to 600 when unset or blank", () => {
		expect(resolveInactivityLimitSeconds({})).toBe(600);
		expect(resolveInactivityLimitSeconds({ HERMES_CRON_TIMEOUT: "  " })).toBe(
			600,
		);
	});

	it("0 means UNLIMITED; explicit values pass through", () => {
		expect(resolveInactivityLimitSeconds({ HERMES_CRON_TIMEOUT: "0" })).toBe(0);
		expect(resolveInactivityLimitSeconds({ HERMES_CRON_TIMEOUT: "900" })).toBe(
			900,
		);
	});

	it("invalid input warns and falls back to 600", () => {
		const warned: string[] = [];
		const limit = resolveInactivityLimitSeconds(
			{ HERMES_CRON_TIMEOUT: "banana" },
			(raw) => {
				warned.push(raw);
			},
		);
		expect(limit).toBe(600);
		expect(warned).toEqual(["banana"]);
	});
});

describe("runWithInactivityBound — TRUE inactivity vs wall clock", () => {
	it("an IDLE job trips exactly at the limit and issues the hard interrupt", async () => {
		const clock = new ManualClock(1_000_000);
		const start = clock.nowSeconds();
		const probe = new TimestampActivityLog(start); // never touched again
		let interrupts = 0;
		const interruptedGate = new Gate();

		const result = await runWithInactivityBound({
			exec: () => interruptedGate.wait.then(() => "aborted-run"), // hangs until abort
			interrupt: () => {
				interrupts++;
				interruptedGate.open();
			},
			probe,
			limitSeconds: 600,
			clock,
			pollSeconds: INACTIVITY_POLL_SECONDS,
		}); // monitor polls drive ALL logical time here

		expect(interrupts).toBe(1); // hard interrupt issued exactly once
		expect(result.timedOut).toBe(true);
		expect(result.idleAtBreach).toBe(600); // EXACTLY at the bound
	});

	it("an ACTIVE job may run for HOURS of logical time without tripping", async () => {
		const clock = new ManualClock(2_000_000);
		const start = clock.nowSeconds();
		const probe = new TimestampActivityLog(start);
		let workIterations = 0;

		const result = await runWithInactivityBound({
			exec: async () => {
				// Simulates continuous tool/API/stream activity: each poll cycle
				// the job reports progress. Runs 3600 simulated seconds.
				for (let i = 0; i < 720; i++) {
					await clock.sleepMs(5000);
					probe.touch(clock.nowSeconds());
					workIterations++;
				}
				return "long-but-healthy";
			},
			interrupt: () => {
				throw new Error("MUST NOT interrupt an active job");
			},
			probe,
			limitSeconds: 600,
			clock,
			pollSeconds: INACTIVITY_POLL_SECONDS,
		});

		expect(workIterations).toBe(720);
		expect(clock.nowSeconds() - start).toBeGreaterThanOrEqual(3600);
		expect(result.timedOut).toBe(false);
		expect(result.result).toBe("long-but-healthy");
	});

	it("the runaway bound fires on idleness AFTER activity stops (not from start)", async () => {
		const clock = new ManualClock(3_000_000);
		const probe = new TimestampActivityLog(clock.nowSeconds());
		let interrupts = 0;
		const wedgedGate = new Gate();

		const result = await runWithInactivityBound({
			exec: async () => {
				// Active for 30 simulated minutes, then wedges silently.
				for (let i = 0; i < 360; i++) {
					await clock.sleepMs(5000);
					probe.touch(clock.nowSeconds());
				}
				await wedgedGate.wait; // hang until the bound interrupts
				return "interrupted-after-idle";
			},
			interrupt: () => {
				interrupts++;
				wedgedGate.open();
			},
			probe,
			limitSeconds: 600,
			clock,
		});

		expect(interrupts).toBe(1);
		expect(result.timedOut).toBe(true);
		// Breach measured from LAST activity (~+1800s), not job start.
		expect(result.idleAtBreach).toBe(600);
	});

	it("limit 0 (HERMES_CRON_TIMEOUT=0) disables the watchdog but keeps claim-loss polling", async () => {
		const clock = new ManualClock(4_000_000);
		const probe = new TimestampActivityLog(clock.nowSeconds()); // idle forever
		const start = clock.nowSeconds();
		let lostClaimSeen = false;
		let interrupted = false;

		const result = await runWithInactivityBound({
			exec: async () => {
				while (!interrupted) {
					await new Promise<void>((r) => setTimeout(r, 0)); // real yield, NO logical time
				}
				return "claim-lost-settled";
			},
			interrupt: () => {
				interrupted = true;
			},
			probe,
			limitSeconds: 0,
			clock,
			pollSeconds: 5,
			shouldAbort: () => {
				if (clock.nowSeconds() >= start + 60) lostClaimSeen = true;
				return lostClaimSeen;
			},
		});

		expect(result.timedOut).toBe(false); // unlimited: idle NEVER trips
		expect(result.aborted).toBe(true); // …but claim-loss still aborts
	});

	it("null probe behaves as unlimited regardless of the limit", async () => {
		const clock = new ManualClock(5_000_000);
		const result = await runWithInactivityBound({
			exec: async () => {
				await clock.sleepMs(60_000);
				return "done";
			},
			interrupt: () => {
				throw new Error("no probe ⇒ no watchdog");
			},
			probe: null,
			limitSeconds: 600,
			clock,
			pollSeconds: 5,
		});
		expect(result.timedOut).toBe(false);
		expect(result.result).toBe("done");
	});

	it("awaits executor settlement AFTER issuing the interrupt (abort lands first)", async () => {
		const clock = new ManualClock(6_000_000);
		const probe = new TimestampActivityLog(clock.nowSeconds());
		let settledAfterInterrupt = false;
		let interrupted = false;

		const result = await runWithInactivityBound({
			exec: async () => {
				await new Promise<void>((r) => setTimeout(r, 5)); // real yield
				settledAfterInterrupt = interrupted;
				return "settled-late";
			},
			interrupt: () => {
				interrupted = true;
			},
			probe,
			limitSeconds: 1,
			clock,
			pollSeconds: 1,
		});
		void clock;

		expect(result.timedOut).toBe(true);
		expect(settledAfterInterrupt).toBe(true);
	});
});
