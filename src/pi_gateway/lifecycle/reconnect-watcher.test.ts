// Behavior contracts for the supervised reconnect watcher (08 §1.1 step 7 /
// structure parity of run.py:_failed_platforms + _platform_reconnect_watcher +
// _spawn_supervised): exponential backoff 30s→300s cap, retry-until-healed,
// success removes from queue, and a crashing pass RESPAWNS the watcher
// instead of killing it. The external drain-request watcher (se-8) is
// contracted alongside: marker presence flips draining, staleness never does.

import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	RECONNECT_BACKOFF_CAP_MS,
	RECONNECT_BASE_DELAY_MS,
	FailedPlatformQueue,
	createReconnectWatcherService,
	reconnectBackoffDelayMs,
} from "./reconnect-watcher.js";
import { createDrainRequestWatcherService } from "./drain-request-watcher.js";
import { writeDrainRequest, clearDrainRequest } from "./markers.js";
import type { TimerPort } from "./watchdog.js";

function fakeTimers(): TimerPort & { run(ms: number): void; now(): number } {
	let now = 0;
	let seq = 0;
	const jobs = new Map<number, { at: number; fn: () => void }>();
	return {
		setTimeout(fn: () => void, ms: number): unknown {
			const id = ++seq;
			jobs.set(id, { at: now + Math.max(0, ms), fn });
			return id;
		},
		clearTimeout(handle: unknown): void {
			jobs.delete(handle as number);
		},
		nowMs(): number {
			return now;
		},
		now(): number {
			return now;
		},
		run(ms: number): void {
			const target = now + ms;
			for (;;) {
				let due: { id: number; at: number; fn: () => void } | null = null;
				for (const [id, job] of jobs) {
					if (job.at > target) continue;
					if (due === null || job.at < due.at) due = { id, ...job };
				}
				if (due === null) break;
				jobs.delete(due.id);
				now = Math.max(now, due.at);
				due.fn();
			}
			now = target;
		},
	};
}

function quietLogger() {
	return {
		info() {},
		warn() {},
		error() {},
	};
}

/** Settle every pending microtask chain deterministically. */
const flush = (): Promise<void> => new Promise((r) => setImmediate(r));

describe("reconnect backoff math (run.py:_reconnect_backoff)", () => {
	it("doubles from 30s and caps at 300s", () => {
		expect(RECONNECT_BASE_DELAY_MS).toBe(30_000);
		expect(RECONNECT_BACKOFF_CAP_MS).toBe(300_000);
		expect(reconnectBackoffDelayMs(1)).toBe(30_000);
		expect(reconnectBackoffDelayMs(2)).toBe(60_000);
		expect(reconnectBackoffDelayMs(3)).toBe(120_000);
		expect(reconnectBackoffDelayMs(4)).toBe(240_000);
		expect(reconnectBackoffDelayMs(5)).toBe(300_000);
		expect(reconnectBackoffDelayMs(50)).toBe(300_000);
	});
});

describe("supervised reconnect watcher service", () => {
	let home: string;
	beforeEach(() => {
		home = mkdtempSync(join(tmpdir(), "pi-lifecycle-reconnect-"));
	});
	afterEach(() => {
		rmSync(home, { recursive: true, force: true });
	});

	/** Minimal StageContext shape for service starts in these tests. */
	function makeCtx(homeDir: string) {
		return {
			home: homeDir,
			selfPid: process.pid,
			log: quietLogger(),
			config: null,
			fingerprint: null,
			existingInstance: null,
			takeover: null,
			lock: null,
			store: null,
			services: { cron: [], watchers: [] },
			adapters: [],
			commands: null,
		};
	}

	async function makeService(
		timers: ReturnType<typeof fakeTimers>,
		hooks: Parameters<typeof createReconnectWatcherService>[0]["hooks"],
	) {
		const queue = new FailedPlatformQueue(() => timers.nowMs());
		const entry = createReconnectWatcherService({
			queue,
			hooks,
			logger: quietLogger(),
			timer: timers,
			initialDelayMs: 10_000,
			pollIntervalMs: 30_000,
		});
		const outcome = await entry.start(makeCtx(home));
		if (!outcome.ok) throw new Error("watcher failed to start");
		return { queue, outcome };
	}

	it("startup-failed platform is retried on the backoff cadence; SUCCESS removes it", async () => {
		const timers = fakeTimers();
		let attempts = 0;
		const { queue } = await makeService(timers, {
			reconnect: () => {
				attempts++;
				return attempts >= 3; // heals on the third try
			},
		});
		// Stage-9 enqueue stamps the FIRST retry one base backoff away.
		queue.enqueue("telegram", {});

		timers.run(40_000); // initial 10s pass skips (nextRetryAt=+30s); 30s cadence lands the first attempt
		await flush();
		expect(attempts).toBe(1);
		expect(queue.get("telegram")?.attempts).toBe(2); // bumped for the next pass

		// Backoff doubled (60s): nothing before +59_999ms…
		timers.run(59_999);
		await flush();
		expect(attempts).toBe(1);
		timers.run(1); // …then attempt #2 fails and doubles again
		await flush();
		expect(attempts).toBe(2);

		// Third retry (+120s) heals → removed from the queue.
		timers.run(120_000);
		await flush();
		expect(attempts).toBe(3);
		expect(queue.size).toBe(0);

		// A healed platform stops consuming retries.
		timers.run(600_000);
		await flush();
		expect(attempts).toBe(3);
	});

	it("a THROWING reconnect hook is contained per platform: entry retained, retries continue", async () => {
		const timers = fakeTimers();
		let attempts = 0;
		const { queue } = await makeService(timers, {
			reconnect: () => {
				attempts++;
				throw new Error("adapter exploded");
			},
		});
		queue.enqueue("discord", {});
		timers.run(40_000);
		await flush();
		expect(attempts).toBe(1);
		expect(queue.size).toBe(1); // retained — the failure was this unit's alone
		timers.run(60_000); // next backoff lands at t=100k
		await flush();
		expect(attempts).toBeGreaterThanOrEqual(2); // retries continue unchanged
	});

	it("a crash ANYWHERE in the pass machinery respawns the watcher (#71758)", async () => {
		const timers = fakeTimers();
		// The log sink itself is dead — the pass cannot even report. The
		// supervision boundary must contain it and respawn the loop.
		const dyingLogger = {
			info() {
				throw new Error("log sink destroyed");
			},
			warn() {},
			error() {},
		};
		let attempts = 0;
		const queue = new FailedPlatformQueue(() => timers.nowMs());
		const service = createReconnectWatcherService({
			queue,
			hooks: {
				reconnect: () => {
					attempts++;
					return true;
				},
			},
			logger: dyingLogger,
			timer: timers,
			initialDelayMs: 10_000,
			pollIntervalMs: 30_000,
		});
		const outcome = await service.start(makeCtx(home));
		if (!outcome.ok) throw new Error("watcher did not start");
		queue.enqueue("slack", {});
		timers.run(100_000);
		await flush();
		// The hook never even ran (the pass died logging about it), yet the
		// watcher is STILL ALIVE and respawning — supervision contained it.
		expect(attempts).toBe(0);
		expect(service.runtime()?.respawns()).toBeGreaterThanOrEqual(1);
	});

	it("stop() halts all future retries", async () => {
		const timers = fakeTimers();
		let attempts = 0;
		const { outcome } = await makeService(timers, {
			reconnect: () => {
				attempts++;
				return false;
			},
		});
		await outcome.handle?.stop?.();
		timers.run(600_000);
		expect(attempts).toBe(0);
	});
});

describe("external drain-request watcher (dashboard begin/cancel-drain)", () => {
	let home: string;
	beforeEach(() => {
		home = mkdtempSync(join(tmpdir(), "pi-lifecycle-drainwatch-"));
	});
	afterEach(() => {
		rmSync(home, { recursive: true, force: true });
	});

	interface CtxLike {
		home: string;
		selfPid: number;
		log: ReturnType<typeof quietLogger>;
	}

	function ctx(homeDir: string): CtxLike {
		return { home: homeDir, selfPid: process.pid, log: quietLogger() };
	}

	it("marker presence flips to draining; removal reverts; status file follows", async () => {
		const timers = fakeTimers();
		const events: string[] = [];
		const service = createDrainRequestWatcherService({
			intervalMs: 1000,
			timer: timers,
			epoch: "epoch-1",
			nowMs: () => timers.now(),
			onDrainRequested: () => events.push("requested"),
			onDrainReleased: () => events.push("released"),
		});
		const startOutcome = await service.start(ctx(home) as never);
		if (!startOutcome.ok) throw new Error("watcher did not start");

		// No marker: stays running through several polls.
		timers.run(5000);
		expect(service.watcher()?.isExternallyDraining()).toBe(false);

		writeDrainRequest(home, { epoch: "epoch-1", nowMs: () => timers.now() });
		timers.run(2000);
		await Promise.resolve();
		expect(service.watcher()?.isExternallyDraining()).toBe(true);
		expect(events).toEqual(["requested"]);
		const stamped = JSON.parse(
			readFileSync(join(home, "gateway_state.json"), "utf8"),
		) as Record<string, unknown>;
		expect(stamped["gateway_state"]).toBe("draining");

		clearDrainRequest(home);
		timers.run(2000);
		await Promise.resolve();
		expect(service.watcher()?.isExternallyDraining()).toBe(false);
		expect(events).toEqual(["requested", "released"]);
		const reverted = JSON.parse(
			readFileSync(join(home, "gateway_state.json"), "utf8"),
		) as Record<string, unknown>;
		expect(reverted["gateway_state"]).toBe("running");
	});

	it("a PRIOR-EPOCH orphan marker NEVER flips the fresh gateway into draining (NS-570)", () => {
		const timers = fakeTimers();
		writeDrainRequest(home, {
			epoch: "old-machine:old-init",
			nowMs: () => 0,
		});
		const service = createDrainRequestWatcherService({
			intervalMs: 1000,
			timer: timers,
			epoch: "new-machine:new-init",
			nowMs: () => timers.now(),
		});
		service.start(ctx(home) as never);
		timers.run(10_000);
		expect(service.watcher()?.isExternallyDraining()).toBe(false);
	});
});
