// Behavior contracts for the boot choreography (run.py:start parity):
// clean-shutdown receipt branch vs unclean recovery, fail-closed receipt
// consumption, stuck-loop suspension at 3 restarts (#7536), and the inbound
// restore gate that serializes dispatch during boot restore.

import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	selectStuckSessions,
	suspendStuckLoopSessions,
	runBootChoreography,
} from "./boot-recovery.js";
import {
	STUCK_LOOP_THRESHOLD,
	consumeCleanShutdownMarker,
	incrementRestartFailureCounts,
	readRestartFailureCounts,
	restartFailureCountsPath,
	writeCleanShutdownMarker,
} from "./shutdown.js";
import { createRestoreGate } from "./restore-gate.js";
import { existsSync } from "node:fs";

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-lifecycle-bootrec-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

const QUIET = { info() {}, warn() {}, error() {} } as const;

describe("clean-receipt vs unclean-recovery branch", () => {
	it("clean receipt ⇒ recovery hooks SKIPPED, orphan markers discarded", async () => {
		const calls: string[] = [];
		const report = await runBootChoreography(
			home,
			{
				discardActiveTurnMarkers: () => {
					calls.push("discard");
					return Promise.resolve(2);
				},
				recoverInterruptedTurns: () => {
					calls.push("exact");
					return Promise.resolve(5);
				},
				suspendRecentlyActive: () => {
					calls.push("fallback");
					return Promise.resolve(1);
				},
			},
			{ cleanShutdownMarkerExists: true, log: QUIET },
		);
		expect(report.cleanShutdown).toBe(true);
		expect(report.discardedMarkers).toBe(2);
		expect(report.exactRecovered).toBe(0); // unclean path never ran
		expect(report.fallbackRecovered).toBe(0);
		expect(calls).toEqual(["discard"]);
	});

	it("UNCLEAN exit (no receipt) ⇒ exact + legacy recovery run; receipt branch skipped", async () => {
		const calls: string[] = [];
		const report = await runBootChoreography(
			home,
			{
				discardActiveTurnMarkers: () => {
					calls.push("discard");
					return Promise.resolve(0);
				},
				recoverInterruptedTurns: () => {
					calls.push("exact");
					return Promise.resolve(3);
				},
				suspendRecentlyActive: () => {
					calls.push("fallback");
					return Promise.resolve(4);
				},
			},
			{ cleanShutdownMarkerExists: false, log: QUIET },
		);
		expect(report.cleanShutdown).toBe(false);
		expect(report.exactRecovered).toBe(3);
		expect(report.fallbackRecovered).toBe(4);
		expect(calls).toEqual(["exact", "fallback"]);
	});

	it("FAIL-CLOSED: a failing receipt consumption RAISES (never masks an unclean exit)", async () => {
		await expect(
			runBootChoreography(
				home,
				{
					discardActiveTurnMarkers: () =>
						Promise.reject(new Error("marker unlink failed")),
				},
				{ cleanShutdownMarkerExists: true, log: QUIET },
			),
		).rejects.toThrow(/clean-start recovery cleanup failed/);
	});
});

describe("stuck-loop suspension (#7536)", () => {
	it("selects sessions at/over the threshold of 3", () => {
		expect(STUCK_LOOP_THRESHOLD).toBe(3);
		expect(selectStuckSessions({ a: 3, b: 2, c: 5 })).toEqual(["a", "c"]);
	});

	it("suspend hook runs per stuck key and the counters file is cleared after", async () => {
		incrementRestartFailureCounts(home, ["looper"]);
		incrementRestartFailureCounts(home, ["looper", "fresh"]);
		incrementRestartFailureCounts(home, ["looper"]);
		expect(readRestartFailureCounts(home)["looper"]).toBe(3);
		// "fresh" was active once — its count survives below the threshold.

		const suspended: string[] = [];
		const result = await suspendStuckLoopSessions(
			home,
			{
				suspendSession: (key) => {
					suspended.push(key);
					return true;
				},
			},
			QUIET,
		);
		expect(result).toEqual(["looper"]);
		expect(suspended).toEqual(["looper"]);
		expect(existsSync(restartFailureCountsPath(home))).toBe(false); // counters start fresh after a pass
	});

	it("WITHOUT a suspension backend the counts are PRESERVED (evidence kept)", async () => {
		incrementRestartFailureCounts(home, ["stuck-one"]);
		incrementRestartFailureCounts(home, ["stuck-one"]);
		incrementRestartFailureCounts(home, ["stuck-one"]);
		const result = await suspendStuckLoopSessions(home, {}, QUIET);
		expect(result).toEqual([]);
		expect(readRestartFailureCounts(home)["stuck-one"]).toBe(3);
	});

	it("sessions that completed OK are dropped from the counts (loop broken)", () => {
		writeFileSync(restartFailureCountsPath(home), JSON.stringify({ gone: 2 }));
		incrementRestartFailureCounts(home, []); // nothing active this shutdown
		expect(readRestartFailureCounts(home)).toEqual({}); // dropped entirely
	});
});

describe("inbound restore gate (run.py:_startup_restore_queue parity)", () => {
	it("queues arrivals while closed and flushes them to the consumer IN ORDER on finish", async () => {
		const gate = createRestoreGate();
		gate.begin();
		expect(gate.closed).toBe(true);

		const delivered: string[] = [];
		gate.setConsumer((item) => {
			delivered.push(String(item));
		});
		gate.enqueueInbound("first");
		gate.enqueueInbound("second");
		expect(gate.queuedCount()).toBe(2);

		let opened = false;
		void gate.whenOpen().then(() => {
			opened = true;
		});
		await gate.finish();
		expect(gate.closed).toBe(false);
		expect(opened).toBe(true);
		expect(delivered).toEqual(["first", "second"]); // arrival order preserved
	});

	it("whenOpen() resolves immediately when the gate was never closed", async () => {
		const gate = createRestoreGate();
		await gate.whenOpen();
		expect(gate.closed).toBe(false);
	});
});

describe("receipt file mechanics", () => {
	it("write/consume exactly once", () => {
		writeCleanShutdownMarker(home);
		expect(consumeCleanShutdownMarker(home)).toBe(true);
		expect(consumeCleanShutdownMarker(home)).toBe(false);
	});
});
