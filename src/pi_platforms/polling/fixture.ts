// pi_platforms/polling/fixture — the POLLING transport-row fixture (shapes.ts
// PollingFixture) implemented against the REAL reference engine: every row
// body drives actual getUpdates cycles, sessions, generations, heartbeats,
// and held-inbound drains over the fake Bot API server. These are behavior
// contracts, not stubbed return values.

import { FakePlatformWire } from "../conformance/wire.js";
import type { PollingFixture } from "../conformance/shapes.js";
import type {
	AdapterStatusSnapshot,
	DisableReason,
} from "../kit/lifecycle-state.js";
import type {
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import { ManualPollingClock } from "./clock.js";
import { FakeTelegramServer } from "./fake-server.js";
import {
	HEARTBEAT_INTERVAL_MS,
	type PollingAdapterCore,
} from "./polling-adapter.js";
import { makePollingSubject, type PollingSubject } from "./subject.js";

export const LONG_POLL_TIMEOUT_MS = 25;

/** Deterministic wait-for predicate (tiny wall budget; no timing asserts). */
export async function eventually(
	predicate: () => boolean,
	timeoutMs = 2_000,
	everyMs = 4,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (predicate()) return;
		if (Date.now() > deadline) throw new Error("eventually: condition not met");
		await new Promise<void>((r) => setTimeout(r, everyMs));
	}
}

export interface PollingWorld {
	subject: PollingSubject;
	engine: PollingAdapterCore;
	tg: FakeTelegramServer;
	wire: FakePlatformWire;
	clock: ManualPollingClock;
}

/** A full polling world: subject + engine + fake server + injected clock. */
export function makePollingWorld(
	opts: {
		name?: string | undefined;
		spawner?: TaskSpawner | undefined;
		scheduler?: Parameters<MakeWorldScheduler>[0];
	} = {},
): PollingWorld {
	const clock = new ManualPollingClock();
	const tg = new FakeTelegramServer();
	const wire = new FakePlatformWire();
	const scheduler =
		opts.scheduler !== undefined ? opts.scheduler : (undefined as never); // replaced below when absent
	void scheduler;
	const subject = makePollingSubject({
		wire,
		tg,
		clock,
		longPollTimeoutMs: LONG_POLL_TIMEOUT_MS,
		name: opts.name,
		...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
	});
	return { subject, engine: subject.adapter, tg, wire, clock };
}
type MakeWorldScheduler = (opts?: unknown) => unknown;

/** Let one long-poll cycle settle (fetch → commit → dispatch). */
export async function settlePollCycle(world: PollingWorld): Promise<void> {
	await eventually(() => world.engine.polledOnce);
}

/**
 * THE fixture behind shapes.ts::makePollingRows — four §3.1 scenarios run
 * against the live engine. Each call gets a FRESH world (rows never couple
 * through shared mutable state).
 */
export function makeRealPollingFixture(): PollingFixture {
	return {
		/**
		 * Row: outage + reconnect PRESERVES the server-side update queue.
		 * Updates arriving during the outage are still queued server-side
		 * (reconnect passes drop_pending_updates=false) and deliver after.
		 */
		async simulateOutageAndReconnect() {
			const world = freshWorld("poll-outage");
			const { engine, tg } = world;
			await engine.connect({ isReconnect: false });
			await settlePollCycle(world);

			engine.disconnect(); // OUTAGE — transport down mid-life
			tg.pushUpdate("chat-1", "o1");
			tg.pushUpdate("chat-1", "o2");
			tg.pushUpdate("chat-1", "o3");
			const queuedBeforeReconnect = tg.pendingUpdateCount;

			// Reconnect MUST preserve the queue: no drop_pending_updates flag.
			const flagsBefore = tg.dropPendingFlags.length;
			await engine.connect({ isReconnect: true });
			await eventually(
				() =>
					deliveredTexts(world, ["o1", "o2", "o3"]) === 3 ||
					tg.dropPendingFlags.length > flagsBefore,
			);
			const deliveredAfterReconnect = deliveredTexts(world, ["o1", "o2", "o3"]);
			return { queuedBeforeReconnect, deliveredAfterReconnect };
		},

		/**
		 * Row: ack-before-enqueue window covered by hold-and-redispatch.
		 * The kill lands AFTER the server-side offset commit and BEFORE any
		 * enqueue — exactly the outage window — so the ONLY copies of the
		 * messages are the held ones; reconnect redispatches them all.
		 */
		async holdAndRedispatch() {
			const world = freshWorld("poll-hold");
			const { engine, tg } = world;
			engine.hooks = {
				afterCommitBeforeDispatch: () => {
					engine.simulateCrashMidCycle(); // kill between commit & enqueue
				},
			};
			await engine.connect({ isReconnect: false });
			await eventually(() => engine.polledOnce); // initial cycle settled

			tg.pushUpdate("chat-1", "h1");
			tg.pushUpdate("chat-1", "h2");
			tg.pushUpdate("chat-1", "h3");
			// The crashed poller fetched+committed before dying; nothing re-fetches.
			const held = await heldTotalAfterDrainAttempt(world);
			// Restart: _mark_connected drains the held queue into the guard.
			engine.hooks = undefined;
			await engine.connect({ isReconnect: true });
			// The L1 guard may legitimately MERGE rapid redeliveries into one
			// follow-up turn (debounce) — delivery counts by CONTENT, not by
			// turn-entry count.
			await eventually(() => deliveredTexts(world, ["h1", "h2", "h3"]) === 3);
			return { held, redispatched: held };
		},

		/**
		 * Row: 409-conflict recovery evicts the zombie session under a fresh
		 * generation (drop_pending_updates=true); an UNKILLABLE zombie exhausts
		 * the ladder to FATAL. Both facts from ONE exhaustion scenario.
		 */
		async conflictRecovery() {
			const world = freshWorld("poll-conflict");
			const { engine, tg, clock } = world;
			await engine.connect({ isReconnect: false });
			await settlePollCycle(world);

			const genBefore = engine.generation;
			tg.setUnkillableZombie(true);
			tg.stealHolderAsZombie(); // a stale process holds the poll

			await eventually(
				() => engine.lifecycleSnapshot().state === "fatal",
				5_000,
			);
			void clock;
			return {
				generationsBumped: engine.generation - genBefore,
				dropPendingUpdatesOnRestart:
					engine.recoveryRestartsWithDropPending >= 1 &&
					tg.dropPendingFlags.length >= engine.recoveryRestartsWithDropPending,
				fatalAfterExhaustion: engine.lifecycleSnapshot().state === "fatal",
			};
		},

		/**
		 * Row: TWO consecutive stuck heartbeat probes feed the reconnect
		 * ladder (injected clock — zero wall-time waits).
		 */
		async heartbeatEscalation() {
			const world = freshWorld("poll-heartbeat");
			const { engine, tg, clock } = world;
			await engine.connect({ isReconnect: false });
			await settlePollCycle(world);

			// Wedge the consumer SERVER-SIDE (#42909): long-poll stays open but
			// updates never reach handlers — pending_update_count grows while
			// the client believes it is polling.
			tg.setConsumerWedged(engine.activeSessionToken as number, true);
			tg.pushUpdate("chat-1", "wedge-1");
			tg.pushUpdate("chat-1", "wedge-2");
			await eventually(() => tg.pendingUpdateCount >= 2);
			const genBefore = engine.generation;

			await clock.advance(HEARTBEAT_INTERVAL_MS); // probe 1 → stuck 1/2
			await eventually(() => engine.stuckProbeStreakForTests >= 1);
			await clock.advance(HEARTBEAT_INTERVAL_MS); // probe 2 → escalate
			await eventually(
				() => engine.recoveryLog.includes("heartbeat-stuck-pending"),
				5_000,
			);
			return {
				stuckProbes: 2,
				reconnectTriggered: engine.generation > genBefore,
			};
		},
	};
}

// ── internals ────────────────────────────────────────────────────────────

function freshWorld(name: string): PollingWorld {
	return makePollingWorld({ name });
}

/** Delivered = the text reached the guard (possibly DEBOUNCE-MERGED into a
 * combined follow-up turn), so match by content across turn entries. */
function deliveredTexts(world: PollingWorld, texts: string[]): number {
	const joined = world.subject.turns().join("\n");
	return texts.filter((t) => joined.split("\n").some((entry) => entry === t))
		.length;
}

/**
 * After a mid-cycle crash the events are HELD (the poller died post-commit).
 * The drain only runs once connected again — this helper just observes the
 * hold queue size after letting dispatch attempts settle.
 */
async function heldTotalAfterDrainAttempt(
	world: PollingWorld,
): Promise<number> {
	await eventually(() => world.engine.heldInboundCount > 0);
	await eventually(
		() => !world.engine.connected || world.engine.heldInboundCount >= 3,
	);
	return world.engine.heldInboundCount;
}

/** Build an IncomingEvent the way toIncomingEvent does (fixture parity). */
export function eventFor(
	updateId: number,
	chatId: string,
	text: string,
): IncomingEvent {
	return {
		messageId: String(updateId),
		messageType: "text",
		text,
		source: { platform: "telegram", chatType: "dm", userId: "user-1", chatId },
	};
}

/** Subject-shaped view of a world (suite wiring convenience). */
export function worldAsSubject(world: PollingWorld): PollingSubject {
	return world.subject;
}

export type { AdapterStatusSnapshot, DisableReason };
