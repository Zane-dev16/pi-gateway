// Staleness eviction (run.py:_handle_message ~17208): leaked running-agent
// entries from hung-but-live handlers wedge a session until process restart —
// follow-ups queue behind a guard that will never answer. The sweep evicts an
// entry when the agent has been IDLE ≥ HERMES_AGENT_TIMEOUT (default 1800s)
// or wall age exceeds max(10× timeout, 7200s), INVALIDATING the session's run
// generation and RELEASING the running-agent state. Pending sentinels are
// never evicted; a disabled timeout (≤0) disables eviction entirely.

import { describe, expect, it } from "vitest";
import type { IncomingEvent } from "./events.js";
import {
	AGENT_TIMEOUT_SECONDS_ENV,
	DEFAULT_AGENT_TIMEOUT_SECONDS,
	RunnerBusyGuard,
	isStaleRunningEntry,
	resolveAgentTimeoutSeconds,
	staleRunningAgentWallTtlSeconds,
	type RunnerBusyOptions,
} from "./runner-busy.js";

const KEY = "agent:main:telegram:dm:100";

function makeGuard(extra: Partial<RunnerBusyOptions> = {}): {
	guard: RunnerBusyGuard;
	generations: Array<{ key: string; reason: string }>;
	released: string[];
	warnings: string[];
	slots: Map<string, IncomingEvent>;
} {
	const generations: Array<{ key: string; reason: string }> = [];
	const released: string[] = [];
	const warnings: string[] = [];
	const slots = new Map<string, IncomingEvent>();
	const guard = new RunnerBusyGuard({
		registry: [
			{
				name: "stop",
				busyPolicy: "interrupt_then_dispatch",
				busyHandler: "stop",
			},
		],
		slots,
		onWarning: (m) => warnings.push(m),
		invalidateRunGeneration: (key, reason) => generations.push({ key, reason }),
		releaseRunningAgentState: (key) => released.push(key),
		specialHandlers: { stop: () => "stopped" },
		...extra,
	});
	return { guard, generations, released, warnings, slots };
}

describe("env bridge (HERMES_AGENT_TIMEOUT ported verbatim)", () => {
	it("default 1800; parses overrides; garbage fails safe to the default", () => {
		expect(AGENT_TIMEOUT_SECONDS_ENV).toBe("HERMES_AGENT_TIMEOUT");
		expect(DEFAULT_AGENT_TIMEOUT_SECONDS).toBe(1800);
		expect(resolveAgentTimeoutSeconds({})).toBe(1800);
		expect(
			resolveAgentTimeoutSeconds({ [AGENT_TIMEOUT_SECONDS_ENV]: "900" }),
		).toBe(900);
		expect(
			resolveAgentTimeoutSeconds({ [AGENT_TIMEOUT_SECONDS_ENV]: "banana" }),
		).toBe(1800); // Python float() would raise; the gate fails safe instead
		// ≤0 values pass through — they DISABLE eviction downstream (parity).
		expect(
			resolveAgentTimeoutSeconds({ [AGENT_TIMEOUT_SECONDS_ENV]: "-5" }),
		).toBe(-5);
	});
});

describe("wall-TTL and stale-entry predicate boundaries", () => {
	it("wall TTL = max(10×timeout, 7200); ∞ when disabled", () => {
		expect(staleRunningAgentWallTtlSeconds(1800)).toBe(18000);
		expect(staleRunningAgentWallTtlSeconds(700)).toBe(7200);
		expect(staleRunningAgentWallTtlSeconds(0)).toBe(Number.POSITIVE_INFINITY);
		expect(staleRunningAgentWallTtlSeconds(-5)).toBe(Number.POSITIVE_INFINITY);
	});

	it("idle ≥ timeout evicts (equality included); below does not", () => {
		expect(
			isStaleRunningEntry({
				ageSeconds: 5,
				idleSeconds: 1800,
				timeoutSeconds: 1800,
			}),
		).toBe(true);
		expect(
			isStaleRunningEntry({
				ageSeconds: 5,
				idleSeconds: 1799.9,
				timeoutSeconds: 1800,
			}),
		).toBe(false);
	});

	it("extreme WALL AGE evicts even while actively working", () => {
		expect(
			isStaleRunningEntry({
				ageSeconds: 18000.1, // > max(10×1800, 7200)
				idleSeconds: 1, // active seconds ago
				timeoutSeconds: 1800,
			}),
		).toBe(true);
		// Strictly greater: exactly at the bound survives.
		expect(
			isStaleRunningEntry({
				ageSeconds: 18000,
				idleSeconds: 1,
				timeoutSeconds: 1800,
			}),
		).toBe(false);
	});

	it("a disabled timeout (≤0) disables BOTH clauses — nothing ever evicts", () => {
		expect(
			isStaleRunningEntry({
				ageSeconds: Number.MAX_SAFE_INTEGER,
				idleSeconds: Number.POSITIVE_INFINITY,
				timeoutSeconds: 0,
			}),
		).toBe(false);
	});

	it("unknowable idleness (∞) counts as idle beyond any positive timeout", () => {
		expect(
			isStaleRunningEntry({
				ageSeconds: 10,
				idleSeconds: Number.POSITIVE_INFINITY,
				timeoutSeconds: 60,
			}),
		).toBe(true);
	});
});

describe("RunnerBusyGuard sweep", () => {
	it("no recorded turn ⇒ no eviction, no callbacks", () => {
		const f = makeGuard();
		expect(f.guard.maybeEvictStaleRunningAgent(KEY)).toBe(false);
		expect(f.generations).toEqual([]);
		expect(f.released).toEqual([]);
	});

	it("a fresh live turn is never evicted", () => {
		const now = 1_000_000_000;
		const f = makeGuard({ now: () => now });
		f.guard.markTurnStarted(KEY, now - 500);
		expect(f.guard.maybeEvictStaleRunningAgent(KEY)).toBe(false);
	});

	it("idle ≥ HERMES_AGENT_TIMEOUT ⇒ evict + invalidate generation + release state", () => {
		const now = 1_000_000_000;
		const f = makeGuard({ now: () => now });
		f.guard.markTurnStarted(KEY, now - 1_800_500); // started ~1800.5s ago…

		expect(f.guard.maybeEvictStaleRunningAgent(KEY)).toBe(true);
		expect(f.generations).toEqual([
			{ key: KEY, reason: "stale_running_agent_eviction" },
		]);
		expect(f.released).toEqual([KEY]);
		// Local release: the running record is gone.
		expect(f.guard.hasRunningTurn(KEY)).toBe(false);
		expect(f.guard.turnStartOf(KEY)).toBeUndefined();
		// Hermes warning shape preserved.
		expect(f.warnings[0]).toContain(
			`Evicting stale _running_agents entry for ${KEY}`,
		);
		expect(f.warnings[0]).toContain(
			"(age: 1801s, idle: 1801s, timeout: 1800s)",
		);
	});

	it("recent agent ACTIVITY keeps an ancient turn alive until wall TTL trips", () => {
		const now = 2_000_000_000;
		const f = makeGuard({ now: () => now });
		// Started 19,000s ago (> 18,000s wall TTL)…
		f.guard.markTurnStarted(KEY, now - 19_000_000);
		// …but streamed a tool call 2s ago: NOT idle-stale yet.
		f.guard.markAgentActivity(KEY, now - 2_000);
		expect(f.guard.maybeEvictStaleRunningAgent(KEY)).toBe(true); // age wins

		// Exactly at the wall-TTL boundary (≤ survives, > evicts).
		const g = makeGuard({ now: () => now });
		g.guard.markTurnStarted(KEY, now - 18_000_000);
		g.guard.markAgentActivity(KEY, now - 1_000);
		expect(g.guard.maybeEvictStaleRunningAgent(KEY)).toBe(false);
	});

	it("pending sentinels are NEVER evicted (async-setup race guard)", () => {
		const now = 50_000_000_000;
		const f = makeGuard({ now: () => now });
		f.guard.markPendingTurnStart(KEY, now - 999_999_999);
		expect(f.guard.maybeEvictStaleRunningAgent(KEY)).toBe(false);
		expect(f.generations).toEqual([]);
		expect(f.guard.hasRunningTurn(KEY)).toBe(false); // sentinel isn't a real run either
	});

	it("agentTimeoutSeconds ≤ 0 disables eviction entirely", () => {
		const now = 1_000_000_000;
		const f = makeGuard({ now: () => now, agentTimeoutSeconds: 0 });
		f.guard.markTurnStarted(KEY, 0); // infinitely old, infinitely idle
		expect(f.guard.maybeEvictStaleRunningAgent(KEY)).toBe(false);
	});

	it("the guard resolves the timeout from the env bridge when unset", () => {
		const now = 1_000_000_000;
		const f = makeGuard({
			now: () => now,
			env: { [AGENT_TIMEOUT_SECONDS_ENV]: "60" },
		});
		f.guard.markTurnStarted(KEY, now - 61_000); // idle 61s ≥ 60s env override
		expect(f.guard.maybeEvictStaleRunningAgent(KEY)).toBe(true);
	});
});

describe("busy-entry integration (eviction precedes the ladder, run.py order)", () => {
	const textEvent = (): IncomingEvent => ({
		messageType: "text",
		text: "follow-up",
		source: { platform: "telegram", chatType: "dm" },
	});

	it("handlePlainTextFollowUp reports 'evicted' and queues NOTHING for a stale session", () => {
		const now = 1_000_000_000;
		const f = makeGuard({ now: () => now });
		f.guard.markTurnStarted(KEY, now - 2_000_000);

		expect(f.guard.handlePlainTextFollowUp(KEY, textEvent())).toBe("evicted");
		expect(f.slots.size).toBe(0);
		expect(f.generations.map((g) => g.reason)).toEqual([
			"stale_running_agent_eviction",
		]);
		expect(f.released).toEqual([KEY]);
	});

	it("a LIVE turn still takes the grace ladder (sweep is transparent)", () => {
		const now = 1_000_000_000;
		const f = makeGuard({ now: () => now });
		f.guard.markTurnStarted(KEY, now - 10);
		f.slots.set(KEY, {
			messageType: "text",
			text: "head",
			source: { platform: "telegram", chatType: "dm" },
		});

		expect(f.guard.handlePlainTextFollowUp(KEY, textEvent())).toBe("queued");
		expect(f.slots.get(KEY)?.text).toBe("head\nfollow-up");
		expect(f.generations).toEqual([]);
	});

	it("steer mode does not steer into an evicted (released) session", () => {
		const now = 1_000_000_000;
		let steered: string | null = null;
		const f = makeGuard({
			now: () => now,
			busyInputMode: "steer",
			steer: (t) => {
				steered = t;
				return true;
			},
		});
		f.guard.markTurnStarted(KEY, now - 2_000_000);
		expect(f.guard.handlePlainTextFollowUp(KEY, textEvent())).toBe("evicted");
		expect(steered).toBeNull();
	});

	it("dispatchBusySlashCommand returns null on eviction — no mid-run dispatch into a dead entry", async () => {
		const now = 1_000_000_000;
		let stopRan = false;
		const f = makeGuard({
			now: () => now,
			specialHandlers: {
				stop: () => {
					stopRan = true;
					return "stopped";
				},
			},
		});
		f.guard.markTurnStarted(KEY, now - 2_000_000);

		await expect(
			f.guard.dispatchBusySlashCommand("stop", textEvent(), KEY),
		).resolves.toBeNull();
		expect(stopRan).toBe(false);
		expect(f.generations.length).toBe(1);
	});

	it("a LIVE entry still dispatches /stop normally (regression guard)", async () => {
		const now = 1_000_000_000;
		let stopRan = false;
		const f = makeGuard({
			now: () => now,
			specialHandlers: {
				stop: () => {
					stopRan = true;
					return "stopped";
				},
			},
		});
		f.guard.markTurnStarted(KEY, now - 100);
		await expect(
			f.guard.dispatchBusySlashCommand("stop", textEvent(), KEY),
		).resolves.toBe("stopped");
		expect(stopRan).toBe(true);
		expect(f.generations).toEqual([]);
	});
});
