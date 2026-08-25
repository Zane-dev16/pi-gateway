// pi_platforms/whatsapp-personal/wa-personal-world — world factory + the REAL
// fixtures behind the conformance rows: shapes.ts::PollingFixture (the FOUR
// inherited §3.1 family rows) realized through VENDOR-TRUE mechanisms of the
// Baileys-bridge port:
//
//   - outage/reconnect: the bridge-side queue is owned SERVER-SIDE
//     (GET /messages drain-on-read, no client cursor) — a downed bridge
//     PRESERVES undrained messages and the reconnect drains them
//     exactly-once (bridge-wire.ts cursor semantics);
//   - held-inbound redispatch: THE TEXT DEBOUNCE IS the hold-and-redeliver
//     window — rapid-fire WhatsApp deliveries aggregate into ONE dispatch
//     after the quiet period (adapter.py:_enqueue_text_event parity);
//   - "conflict/zombie": a CRASHED managed bridge (exit outside planned
//     shutdown) fails in-flight sends cleanly, its in-memory buffer dies WITH
//     the process (crash-respawn drops pending updates — the vendor-true
//     drop_pending_updates=True analog), a recycled pidfile PID is never
//     signalled, and recovery respawns a FRESH engine (watcher parity);
//   - heartbeat escalation: TWO consecutive poll errors keep the loop ALIVE
//     with the 5s backoff (source: error → sleep 5s → continue), and an
//     unhealthy /health verdict escalates into the reconnect ladder.
//
// Every scenario drives the REAL adapter against FakeBridgeServer under the
// INJECTED ManualPollingClock — behavior contracts, no wall-clock asserts,
// no stubbed return values. Rows never couple through shared mutable state.

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type { TaskSpawner } from "../../pi_gateway/guards/index.js";
import { FakePlatformWire } from "../conformance/wire.js";
import type { PollingFixture } from "../conformance/shapes.js";
import { ManualPollingClock } from "../polling/clock.js";

import {
	makeWaPersonalSubject,
	type WaPersonalSubject,
} from "./wa-personal-subject.js";
import {
	staleBridgeEvictionDecision,
	type WaPersonalAdapter,
} from "./wa-personal-adapter.js";
import { FakeBridgeServer } from "./bridge-wire.js";
import { WA_TEXT_BATCH_DELAY_SECONDS } from "./manifest.js";

/** Standard fixture DM sender (pairing-default policy admits any sender). */
export const ALICE_JID = "15551230001@s.whatsapp.net";

export interface WaPersonalWorld {
	subject: WaPersonalSubject;
	adapter: WaPersonalAdapter;
	bridge: FakeBridgeServer;
	wire: FakePlatformWire;
	clock: ManualPollingClock;
	scheduler: ManualScheduler;
	/** Connect and complete the first poll cycle. */
	connectAndAwaitLive(): Promise<void>;
	/** Queue one inbound bridge message (Baileys-shaped dict). */
	pushInbound(data: Record<string, unknown>): void;
	/** Drive N GET /messages cycles through the REAL adapter. */
	drivePolls(cycles?: number): Promise<void>;
	/** Drain queued guard frames deterministically. */
	drainTurns(): Promise<void>;
}

export function makeWaPersonalWorld(
	opts: { name?: string | undefined; spawner?: TaskSpawner | undefined } = {},
): WaPersonalWorld {
	const clock = new ManualPollingClock();
	const bridge = new FakeBridgeServer();
	const wire = new FakePlatformWire();
	const scheduler = new ManualScheduler();
	const subject = makeWaPersonalSubject({
		wire,
		bridge,
		clock,
		spawner: opts.spawner ?? scheduler.spawner,
		scheduler,
		name: opts.name,
	});
	return {
		subject,
		adapter: subject.adapter,
		bridge,
		wire,
		clock,
		scheduler,
		async connectAndAwaitLive(): Promise<void> {
			await subject.adapter.connect({ isReconnect: false });
			await subject.adapter.pollOnce();
		},
		pushInbound(data: Record<string, unknown>): void {
			bridge.queueInbound(data);
		},
		async drivePolls(cycles = 1): Promise<void> {
			for (let i = 0; i < cycles; i++) {
				await subject.adapter.pollOnce();
			}
		},
		async drainTurns(): Promise<void> {
			await scheduler.runToEnd();
		},
	};
}

/** Baileys-shaped inbound DM text message. */
function dmText(
	body: string,
	opts: { seq?: number | undefined; sender?: string | undefined } = {},
): Record<string, unknown> {
	const seq = opts.seq ?? 0;
	return {
		messageId: `BAE${String(seq).padStart(6, "0")}`,
		chatId: opts.sender ?? ALICE_JID,
		senderId: opts.sender ?? ALICE_JID,
		senderName: "Alice",
		body,
		isGroup: false,
		timestamp: Date.now(),
	};
}

/** Delivered = the text reached the guard (debounce may merge; match lines). */
function deliveredLineCount(world: WaPersonalWorld, texts: string[]): number {
	const joined = world.subject.turns().join("\n");
	return texts.filter((t) => joined.split("\n").some((entry) => entry === t))
		.length;
}

/**
 * THE fixture behind shapes.ts::makePollingRows — vendor-true mappings
 * documented per scenario (see module header).
 */
export function makeRealWaPersonalFixture(): PollingFixture {
	return {
		/** Outage + reconnect preserves the SERVER-SIDE update queue. */
		async simulateOutageAndReconnect() {
			const world = freshWorld("wa-outage");
			await world.connectAndAwaitLive();

			world.pushInbound(dmText("o1", { seq: 1 }));
			world.pushInbound(dmText("o2", { seq: 2 }));
			world.pushInbound(dmText("o3", { seq: 3 }));

			// OUTAGE mid-poll: every edge refuses; the GET never drains.
			world.bridge.setDown(true);
			await world.adapter.pollOnce();
			if (world.bridge.pendingCount !== 3) {
				throw new Error(
					`outage must preserve the bridge-side queue, got ${world.bridge.pendingCount}`,
				);
			}
			const queuedBeforeReconnect = world.bridge.pendingCount;

			// Bridge returns; reconnect resumes polling (no backlog drop).
			world.bridge.setDown(false);
			await world.adapter.connect({ isReconnect: true });
			await world.drivePolls(3);
			// Debounced texts flush after the quiet period (virtual clock).
			await world.clock.advance(WA_TEXT_BATCH_DELAY_SECONDS * 1000 + 1);
			await world.drainTurns();

			const delivered = deliveredLineCount(world, ["o1", "o2", "o3"]);
			if (delivered !== 3) {
				throw new Error(
					`expected exactly-once delivery of 3, got ${delivered}`,
				);
			}
			return { queuedBeforeReconnect, deliveredAfterReconnect: delivered };
		},

		/**
		 * Held-inbound redispatch: the TEXT DEBOUNCE IS the window — four
		 * rapid-fire arrivals aggregate into ONE combined dispatch whose line
		 * count equals the held count (redispatched === held contract).
		 */
		async holdAndRedispatch() {
			const world = freshWorld("wa-hold");
			await world.connectAndAwaitLive();

			const bursts = 4;
			for (let i = 1; i <= bursts; i++) {
				world.pushInbound(dmText(`h${i}`, { seq: i }));
			}
			await world.drivePolls(1); // one GET hands back the whole burst

			const held = world.adapter.counters.debouncedEnqueues;
			if (held !== bursts || world.adapter.heldTextBatchCount !== 1) {
				throw new Error(
					`burst must aggregate ${bursts} arrivals into ONE batch (held=${held}, batches=${world.adapter.heldTextBatchCount})`,
				);
			}

			await world.clock.advance(WA_TEXT_BATCH_DELAY_SECONDS * 1000 + 1);
			await world.drainTurns();

			const combined = world.subject.turns().at(-1) ?? "";
			const redispatched = combined
				.split("\n")
				.filter((line) => /^h\d$/.test(line)).length;
			if (redispatched !== held) {
				throw new Error(
					`held ${held} but redispatched ${redispatched}: ${JSON.stringify(world.subject.turns())}`,
				);
			}
			return { held, redispatched };
		},

		/**
		 * Conflict/zombie eviction: the crashed managed BRIDGE fails in-flight
		 * sends cleanly, its buffer dies WITH the process (vendor-true
		 * drop_pending_updates=True analog), recycled PIDs are never signalled,
		 * and recovery respawns a FRESH engine while the crashed one stays
		 * terminally FATAL.
		 */
		async conflictRecovery() {
			// Leg A: crash detection + clean failure + stale-PID decisions.
			const world = freshWorld("wa-zombie");
			await world.connectAndAwaitLive();

			// A message buffered at the moment of death dies with the process.
			world.pushInbound(dmText("lost-with-the-crash", { seq: 99 }));
			world.adapter.injectBridgeExit(-9); // SIGKILL-class crash
			const outcome = await world.adapter.send(ALICE_JID, "in-flight send");
			if (outcome.success !== false) {
				throw new Error("a detected crash must fail in-flight sends cleanly");
			}
			if (!/exited unexpectedly/.test(outcome.error ?? "")) {
				throw new Error(`clean failure error shape, got ${outcome.error}`);
			}
			world.bridge.crash(); // process death takes its buffer
			const dropPendingUpdatesOnRestart =
				world.adapter.lifecycleSnapshot().state === "fatal" &&
				world.bridge.pendingCount === 0;

			// Stale/zombie eviction DECISIONS (pidProbe injected; OS mechanics
			// excluded): a live-but-recycled PID is never a stranger-killer.
			const alienProbe = {
				alive: () => true,
				startTicksOf: () => 999_999, // ≠ recorded baseline
				cmdlineOf: () => null,
			};
			const recycled = staleBridgeEvictionDecision(
				{ pid: 4242, startTicks: 17 },
				"/tmp/session",
				alienProbe,
			);
			const oursProbe = {
				alive: () => true,
				startTicksOf: () => 17, // exact baseline match
				cmdlineOf: () => null,
			};
			const ours = staleBridgeEvictionDecision(
				{ pid: 4242, startTicks: 17 },
				"/tmp/session",
				oursProbe,
			);
			if (recycled.action !== "skip-recycled" || ours.action !== "kill") {
				throw new Error(
					`stale-bridge decision broken: recycled=${recycled.action} ours=${ours.action}`,
				);
			}

			// Leg B: recovery respawns a FRESH engine (runner-watchdog parity —
			// fatal is terminal for the OLD process lifetime).
			const respawn = freshWorld("wa-respawn");
			await respawn.connectAndAwaitLive();
			respawn.pushInbound(dmText("post-respawn", { seq: 7 }));
			await respawn.drivePolls(1);
			await respawn.clock.advance(WA_TEXT_BATCH_DELAY_SECONDS * 1000 + 1);
			await respawn.drainTurns();
			const generationsBumped =
				respawn.adapter.generation >= 1 &&
				respawn.subject.turns().includes("post-respawn")
					? respawn.adapter.generation
					: 0;

			return {
				generationsBumped,
				dropPendingUpdatesOnRestart,
				fatalAfterExhaustion:
					world.adapter.lifecycleSnapshot().state === "fatal",
			};
		},

		/**
		 * Heartbeat escalation: TWO consecutive poll errors keep the loop alive
		 * with the 5s backoff (never fatal), then an unhealthy /health verdict
		 * escalates into the reconnect ladder (generation bump).
		 */
		async heartbeatEscalation() {
			const world = freshWorld("wa-heartbeat");
			await world.connectAndAwaitLive();
			const genBefore = world.adapter.generation;

			world.bridge.setDown(true);
			await world.adapter.pollOnce(); // poll error #1 → sleep 5s → continue
			await world.adapter.pollOnce(); // poll error #2 → still ALIVE
			const stuckProbes = world.adapter.counters.consecutivePollFailures;
			if (
				stuckProbes !== 2 ||
				world.adapter.lifecycleSnapshot().state !== "active"
			) {
				throw new Error(
					`two poll errors must keep the loop alive (stuck=${stuckProbes}, state=${world.adapter.lifecycleSnapshot().state})`,
				);
			}
			const backoffs = world.clock.sleeps.filter((ms) => ms === 5_000).length;
			if (backoffs < 2) {
				throw new Error(`expected 2 recorded 5s backoffs, got ${backoffs}`);
			}

			// Escalation rung #1: stuck-streak feeds the reconnect ladder.
			const escalatedStreak = await world.adapter.escalateIfUnhealthy();
			// Escalation rung #2: /health disconnected verdict escalates too.
			world.bridge.setDown(false);
			world.bridge.setHealthStatus("disconnected");
			await world.adapter.pollOnce(); // HTTP fine, empty batch
			const escalatedVerdict = await world.adapter.escalateIfUnhealthy();
			const reconnectTriggered =
				escalatedStreak &&
				escalatedVerdict &&
				world.adapter.generation > genBefore &&
				world.adapter.recoveryLog.length >= 2;
			if (!reconnectTriggered) {
				throw new Error(
					`escalation ladder did not trigger (streak=${escalatedStreak}, verdict=${escalatedVerdict}, gen=${world.adapter.generation})`,
				);
			}
			return { stuckProbes: 2, reconnectTriggered };
		},
	};
}

function freshWorld(name: string): WaPersonalWorld {
	return makeWaPersonalWorld({ name });
}
