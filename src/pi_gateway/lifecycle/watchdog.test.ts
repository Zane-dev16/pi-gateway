// Behavior contracts for the liveness backstops (08 §1.3(c);
// gateway/shutdown_watchdog.py port): the unref'd OS-level shutdown watchdog
// hard-exits ≤ drain+60s with a metadata snapshot, the loop-liveness guard
// escalates after 3 MISSED probes and resets on responses, and the heartbeat
// file refreshes immediately + on cadence. All clocks are injected.

import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	DEFAULT_SHUTDOWN_WATCHDOG_GRACE_MS,
	LOOP_LIVENESS_MAX_STRIKES,
	armShutdownWatchdog,
	loopHeartbeatPath,
	resolveShutdownWatchdogDelayMs,
	shutdownWatchdogDumpPath,
	startLoopHeartbeat,
	startLoopLivenessGuard,
	type TimerPort,
} from "./watchdog.js";

interface FakeTimers extends TimerPort {
	run(ms: number): void;
	/** Zero-delay witnesses are scheduled but NEVER run (frozen loop). */
	freezeWitnesses(on: boolean): void;
}

/** Deterministic timer port: tests advance the clock by hand. */
function fakeTimers(): FakeTimers {
	let now = 0;
	let seq = 0;
	let swallowZero = false;
	type Job = { at: number; fn: () => void };
	const jobs = new Map<unknown, Job>();
	return {
		setTimeout(fn: () => void, ms: number): unknown {
			const handle = ++seq;
			const frozen = swallowZero && ms === 0;
			jobs.set(handle, {
				at: frozen ? Number.POSITIVE_INFINITY : now + Math.max(0, ms),
				fn,
			});
			return handle;
		},
		clearTimeout(handle: unknown): void {
			jobs.delete(handle);
		},
		nowMs(): number {
			return now;
		},
		freezeWitnesses(on: boolean): void {
			swallowZero = on;
		},
		run(ms: number): void {
			const target = now + ms;
			for (;;) {
				let due: { handle: unknown; job: Job } | null = null;
				for (const [handle, job] of jobs) {
					if (job.at > target) continue;
					if (due === null || job.at < due.job.at) due = { handle, job };
				}
				if (due === null) break;
				jobs.delete(due.handle);
				now = Math.max(now, due.job.at);
				due.job.fn();
			}
			now = target;
		},
	};
}

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-lifecycle-watchdog-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

describe("shutdown watchdog (wedged-drain hard-exit ≤ drain+60s)", () => {
	it("delay resolves to drain budget + 60s grace", () => {
		expect(DEFAULT_SHUTDOWN_WATCHDOG_GRACE_MS).toBe(60_000);
		expect(resolveShutdownWatchdogDelayMs(0)).toBe(60_000);
		expect(resolveShutdownWatchdogDelayMs(15_000)).toBe(75_000);
	});

	it("fires past the leash: metadata snapshot appended to the dump log THEN hard-exit", () => {
		const timers = fakeTimers();
		const exits: number[] = [];
		const watch = armShutdownWatchdog({
			delayMs: 60_000,
			exitCode: 1,
			dumpPath: shutdownWatchdogDumpPath(home),
			timer: timers,
			snapshotFn: () => ({ klass: "planned_stop", pending_messages: 3 }),
			hardExit: (code) => exits.push(code),
		});
		timers.run(59_999);
		expect(exits).toEqual([]); // grace not exhausted — still armed
		timers.run(1);
		expect(exits).toEqual([1]);
		expect(watch.fired()).toBe(true);
		// The dump line carries the snapshot for post-mortem diagnosis.
		const raw = readFileSync(shutdownWatchdogDumpPath(home), "utf8");
		const record = JSON.parse(raw.trim()) as Record<string, unknown>;
		expect(record["event"]).toBe("shutdown_watchdog_fired");
		expect((record["snapshot"] as Record<string, unknown>)["klass"]).toBe(
			"planned_stop",
		);
	});

	it("disarm before the leash exits quietly (normal completion)", () => {
		const timers = fakeTimers();
		let exited = false;
		const watch = armShutdownWatchdog({
			delayMs: 60_000,
			timer: timers,
			hardExit: () => {
				exited = true;
			},
		});
		timers.run(30_000);
		watch.disarm();
		timers.run(60_000); // far past the original deadline
		expect(exited).toBe(false);
		expect(watch.fired()).toBe(false);
	});

	it("non-positive delay never arms anything", () => {
		let exited = false;
		const watch = armShutdownWatchdog({
			delayMs: 0,
			timer: fakeTimers(),
			hardExit: () => {
				exited = true;
			},
		});
		watch.disarm();
		expect(exited).toBe(false);
	});

	it("snapshotFn throwing degrades to an error marker instead of skipping the exit", () => {
		const timers = fakeTimers();
		const exits: number[] = [];
		armShutdownWatchdog({
			delayMs: 1000,
			dumpPath: join(home, "wdump.log"),
			timer: timers,
			snapshotFn: () => {
				throw new Error("snapshot blew up");
			},
			hardExit: (code) => exits.push(code),
		});
		timers.run(2000);
		expect(exits).toEqual([1]);
		const raw = readFileSync(join(home, "wdump.log"), "utf8");
		expect(raw).toContain("snapshot blew up");
	});
});

describe("loop-liveness guard (3 missed probes ⇒ exit 75)", () => {
	function makeGuard(
		timers: FakeTimers,
		extra?: { onBreach?: (strikes: number) => void },
	) {
		const exits: number[] = [];
		const guard = startLoopLivenessGuard({
			probeIntervalMs: 30_000,
			probeTimeoutMs: 10_000,
			maxStrikes: LOOP_LIVENESS_MAX_STRIKES,
			timer: timers,
			...extra,
			hardExit: (code) => exits.push(code),
		});
		return { guard, exits };
	}

	it("responsive loop: witness answers inside every timeout — no strikes, no exit", () => {
		const timers = fakeTimers();
		const { guard, exits } = makeGuard(timers);
		for (let i = 0; i < 6; i++) timers.run(5_000); // witnesses always win the race
		expect(guard.strikes()).toBe(0);
		expect(exits).toEqual([]);
		guard.stop();
	});

	it("frozen loop: three consecutive missed probes escalate to hard-exit 75", () => {
		const timers = fakeTimers();
		timers.freezeWitnesses(true); // event loop wedged: witnesses never run
		const breaches: number[] = [];
		const { guard, exits } = makeGuard(timers, {
			onBreach: (s) => breaches.push(s),
		});
		// Interval ticks land at t=30k/60k/90k; each round's judge lands 10s
		// later and counts one strike. The third strike fires at t=100k.
		timers.run(100_000);
		expect(guard.strikes()).toBe(3);
		expect(breaches).toEqual([3]);
		expect(exits).toEqual([75]); // SERVICE_RESTART_EXIT_CODE — supervisor recycles
		guard.stop();
	});

	it("a responding probe RESETS the strike count (slow blip ≠ dead loop)", () => {
		const timers = fakeTimers();
		const { guard, exits } = makeGuard(timers);
		timers.freezeWitnesses(true);
		// Judges fire at t=40k and t=70k — two missed probes.
		timers.run(70_000);
		expect(guard.strikes()).toBe(2);
		// …the loop recovers; the next judge (t=100k) sees its witness ran…
		timers.freezeWitnesses(false);
		timers.run(30_000);
		expect(guard.strikes()).toBe(0);
		expect(exits).toEqual([]);
		guard.stop();
	});

	it("stop() cancels everything — no further escalation", () => {
		const timers = fakeTimers();
		timers.freezeWitnesses(true);
		const { guard, exits } = makeGuard(timers);
		guard.stop();
		timers.run(300_000);
		expect(exits).toEqual([]);
	});
});

describe("loop heartbeat (<home>/state/gateway.heartbeat)", () => {
	it("writes immediately, then on cadence, until stopped", () => {
		const timers = fakeTimers();
		const hb = startLoopHeartbeat(home, {
			intervalMs: 30_000,
			startTimeSec: 1234,
			timer: timers,
			pid: 777,
		});
		const path = hb.path();
		expect(path).toBe(loopHeartbeatPath(home));
		expect(existsSync(path)).toBe(true); // immediate first write
		const first = JSON.parse(readFileSync(path, "utf8")) as Record<
			string,
			unknown
		>;
		expect(first["pid"]).toBe(777);
		expect(first["start_time"]).toBe(1234);
		expect(first["updated_at"]).toBeTypeOf("string");

		timers.run(30_000);
		const second = JSON.parse(readFileSync(path, "utf8")) as Record<
			string,
			unknown
		>;
		expect(
			(second["monotonic"] as number) >= (first["monotonic"] as number),
		).toBe(true);

		hb.stop();
	});
});
