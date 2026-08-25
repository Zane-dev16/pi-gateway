// pi_platforms/buzz/buzz-world — world factory + the REAL fixtures behind the
// conformance rows: shapes.ts::PollingFixture (the FOUR inherited §3.1 family
// ids) realized through vendor-true mechanisms, every row title carrying the
// vendor-class mapping. Every scenario builds a fresh world; the injected
// ManualPollingClock and the FakeBuzzCli executor seam drive everything —
// behavior contracts, no stubbed return values.
//
// Vendor mappings (documented per shapes.ts contract):
//   outage-reconnect-preserves-queue → CLI failures mid-sweep (persistent
//     executor failure window); fake-relay state persists server-side;
//     resumed sweeps deliver held events exactly-once via seen-set dedupe;
//     a full reconnect re-seeds watermarks without replaying duplicates.
//   held-inbound-redispatch → SOURCE-PINNED ack-window: _handle_event commits
//     seen BEFORE dispatch. The fetched-but-uncommitted window redispatches
//     exactly once on the next inclusive-since sweep (modeled by the
//     hooks.beforeCommit fault point); the committed-but-undispatched window
//     is pinned at-most-once in the shape-delta ack-window row.
//   conflict-zombie-eviction → HONEST CLASS DELTA: a stateless request/response
//     CLI cannot hold a server-side polling session ⇒ no 409/zombie exists.
//     The REAL bounds are pinned instead: duplicate consumers refuse via the
//     scoped identity lock (fatal), and reconnect re-seeding drops backlog
//     (drop_pending_updates parity) while fresh flow continues.
//   heartbeat-escalation → HONEST CLASS DELTA: Buzz has NO heartbeats; the
//     escalation analog is the CLI timeout ladder (rc 124 {"error":"timeout"}
//     stderr contract): repeated timeouts never latch fatal and the sweep loop
//     SURVIVES, resuming delivery afterwards.

import { ManualPollingClock } from "../polling/clock.js";
import { FakePlatformWire } from "../conformance/wire.js";
import type { PollingFixture } from "../conformance/shapes.js";
import type { TaskSpawner } from "../../pi_gateway/guards/index.js";
import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type { TokenLockManagerSeam } from "../kit/index.js";

import { FakeBuzzCli } from "./cli-wire.js";
import { BuzzAdapter } from "./buzz-adapter.js";
import {
	BuzzSubject,
	FIXTURE_BUZZ_NSEC,
	FIXTURE_BUZZ_RELAY,
} from "./buzz-subject.js";
import { FIXED_PUBKEY_HEX } from "./vectors.js";

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

/** Fixture community identities. */
export const ALICE =
	"1111111111111111111111111111111111111111111111111111111111111111";
export const BOB =
	"2222222222222222222222222222222222222222222222222222222222222222";
export const MAIN_CHANNEL = "ch-main-ccc2bc1a";
export const DM_CHANNEL = "dm-alice-6f9d01";

export interface BuzzWorld {
	subject: BuzzSubject;
	/** Alias of subject.adapter (engine under test). */
	engine: BuzzAdapter;
	cli: FakeBuzzCli;
	wire: FakePlatformWire;
	clock: ManualPollingClock;
	scheduler: import("../../pi_gateway/guards/testing/manual-spawner.js").ManualScheduler;
	/** Shared lock registry for contention fixtures. */
	makeRivalAdapter(owner: string): BuzzAdapter;
	/** Connect + one settled sweep (the ready-to-poll posture). */
	connectAndAwaitLive(): Promise<void> /** ONE manual poll sweep + guard-frame drain (deterministic). */;
	sweep(): Promise<void>;
	pushChannelMessage(channelId: string, pubkey: string, content: string): void;
}

let worldCounter = 0;

export function makeBuzzWorld(
	opts: { name?: string | undefined; spawner?: TaskSpawner | undefined } = {},
): BuzzWorld {
	worldCounter += 1;
	const clock = new ManualPollingClock();
	const cli = new FakeBuzzCli({
		relayUrl: FIXTURE_BUZZ_RELAY,
		selfPubkey: FIXED_PUBKEY_HEX, // identity derived from FIXTURE_BUZZ_NSEC
		selfDisplayName: "PiBot",
	});
	cli.addChannel(MAIN_CHANNEL, "General", "the main room");
	const wire = new FakePlatformWire();
	const scheduler = new ManualScheduler();
	const subject = new BuzzSubject({
		name: opts.name ?? `buzz-${worldCounter}`,
		wire,
		cli,
		spawner: opts.spawner ?? scheduler.spawner,
		scheduler,
	});
	return {
		subject,
		engine: subject.adapter,
		cli,
		wire,
		clock,
		scheduler,
		makeRivalAdapter(owner: string): BuzzAdapter {
			return new BuzzAdapter({
				config: { cli_path: "/usr/local/bin/buzz" },
				pathProbes: { fileExists: () => true },
				secretReader: (key) =>
					key === "BUZZ_PRIVATE_KEY"
						? FIXTURE_BUZZ_NSEC
						: key === "BUZZ_RELAY_URL"
							? FIXTURE_BUZZ_RELAY
							: undefined,
				executor: cli.executor(),
				nowMs: () => cli.nowSeconds * 1000,
				lockManager: subject.getLockManager(),
				lockOwner: owner,
			});
		},
		async connectAndAwaitLive(): Promise<void> {
			await subject.adapter.connect({ isReconnect: false });
			await subject.adapter.pollSweep();
			await scheduler.runToEnd();
		},
		async sweep(): Promise<void> {
			await subject.adapter.pollSweep();
			await scheduler.runToEnd();
		},
		pushChannelMessage(channelId, pubkey, content): void {
			cli.pushEvent(channelId, { pubkey, content });
		},
	};
}

// ── THE fixture behind shapes.ts::makePollingRows ─────────────────────────────

function freshWorld(name: string): BuzzWorld {
	return makeBuzzWorld({ name });
}

/** Let async dispatch pipelines drain (wall-budget yield, no asserts). */
async function settleWall(): Promise<void> {
	await new Promise<void>((r) => setTimeout(r, 20));
}

function turnsIncluding(world: BuzzWorld, text: string): number {
	return world.subject.turns().filter((t) => t === text).length;
}

export function makeRealBuzzFixture(): PollingFixture {
	/**
	 * Dispatch-level delivery counter: dispatchMessage enqueues EXACTLY one
	 * IncomingEvent per dispatched chat event (guard-side turn merging is
	 * shared-row territory), so exactly-once downstream is counted HERE.
	 */
	function dispatchCount(world: BuzzWorld, text: string): number {
		return world.engine.inboundEventLog.filter((e) => e.text === text).length;
	}

	return {
		/**
		 * MAPPING (row title carries it): outage = persistent CLI failure
		 * window; queue preservation = server-side fake-relay state + resumed
		 * sweeps; exactly-once downstream = seen-set dedupe across inclusive
		 * refetches AND across a full reconnect re-seed.
		 */
		async simulateOutageAndReconnect() {
			const world = freshWorld("buzz-outage");
			const { engine, cli } = world;
			await world.connectAndAwaitLive();

			// One consumed marker event before the outage.
			world.pushChannelMessage(MAIN_CHANNEL, ALICE, "@PiBot pre1");
			await world.sweep();
			if (dispatchCount(world, "pre1") !== 1) {
				throw new Error("fixture setup: pre1 not delivered once");
			}

			// OUTAGE: every CLI call fails mid-life (rc≠0, JSON error contract).
			let outage = true;
			cli.setPersistentFailure(() =>
				outage
					? {
							code: 1,
							stdout: "",
							stderr: JSON.stringify({
								error: "unreachable",
								message: "relay unreachable",
							}),
						}
					: undefined,
			);
			cli.advanceClock(10);
			world.pushChannelMessage(MAIN_CHANNEL, ALICE, "@PiBot o1");
			world.pushChannelMessage(MAIN_CHANNEL, ALICE, "@PiBot o2");
			world.pushChannelMessage(MAIN_CHANNEL, ALICE, "@PiBot o3");
			const queuedBeforeReconnect = 3; // held server-side during the outage

			await world.sweep(); // fails mid-sweep — CONTAINED (loop survives)
			if (engine.pollFailures < 1 || !engine.pollLoopActive) {
				throw new Error(
					"outage sweep must be contained without killing the loop",
				);
			}
			if (
				dispatchCount(world, "o1") +
					dispatchCount(world, "o2") +
					dispatchCount(world, "o3") >
				0
			) {
				throw new Error("no event may dispatch during a full outage");
			}

			// RECONNECT: clear the outage; sweeps resume and drain the queue.
			outage = false;
			await world.sweep();
			await settleWall();
			const deliveredAfterReconnect = ["o1", "o2", "o3"].filter(
				(t) => dispatchCount(world, t) === 1,
			).length;

			// Full reconnect cycle: re-seed suppresses history WITHOUT
			// redelivering anything (exactly-once downstream), fresh flows on.
			await engine.connect({ isReconnect: true });
			cli.advanceClock(5);
			world.pushChannelMessage(MAIN_CHANNEL, ALICE, "@PiBot fresh-post");
			await world.sweep();
			await settleWall();
			if (dispatchCount(world, "fresh-post") !== 1) {
				throw new Error("post-reconnect fresh event must dispatch exactly once");
			}
			for (const t of ["pre1", "o1", "o2", "o3"]) {
				if (dispatchCount(world, t) !== 1) {
					throw new Error(
						`delivery count ${dispatchCount(world, t)} for ${t}; log=${JSON.stringify(engine.inboundEventLog.map((e) => ({ t: e.text, u: e.source?.userId })))}`,
					);
				}
			}
			return { queuedBeforeReconnect, deliveredAfterReconnect };
		},

		/**
		 * MAPPING: source pins SEEN-COMMIT-BEFORE-DISPATCH. The fault point
		 * models a crash AFTER fetch but BEFORE commit — the uncommitted batch
		 * is HELD server-side and the next inclusive-since sweep REDISPATCHES
		 * it exactly once (dedupe keeps single delivery).
		 */
		async holdAndRedispatch() {
			const world = freshWorld("buzz-hold");
			const { engine } = world;
			await world.connectAndAwaitLive();

			let crashedOnce = false;
			engine.hooks = {
				beforeCommit: (_channelId, event) => {
					if (crashedOnce) return;
					if (event["content"] === "@PiBot h1") {
						crashedOnce = true;
						throw new Error("simulated crash between fetch and commit");
					}
				},
			};
			cliAdvanceAndPush(world, ["@PiBot h1", "@PiBot h2", "@PiBot h3"]);
			await world.sweep(); // h1 crashes pre-commit; whole batch aborted UNCOMMITTED
			const held = 3;
			for (const t of ["h1", "h2", "h3"]) {
				if (dispatchCount(world, t) !== 0) {
					throw new Error(`${t} dispatched despite the pre-commit crash`);
				}
			}
			if (engine.sweepErrors < 1) throw new Error("crash must be contained");

			engine.hooks = undefined; // recovery
			await world.sweep(); // inclusive refetch re-presents the held batch
			await settleWall();
			const redispatched = ["h1", "h2", "h3"].filter(
				(t) => dispatchCount(world, t) === 1,
			).length;
			return { held, redispatched };
		},

		/**
		 * CLASS DELTA mapping: no server-side polling session exists to 409-
		 * evict — the REAL contention bounds are asserted: rival consumer
		 * refuses FATAL via the scoped identity lock; the survivor's reconnect
		 * RESEEDS (backlog dropped = drop_pending_updates parity); fresh flow
		 * resumes under the SAME single identity.
		 */
		async conflictRecovery() {
			const world = freshWorld("buzz-conflict");
			const { engine, cli } = world;
			await world.connectAndAwaitLive();
			const genBefore = engine.connectLog.length;

			// Duplicate consumer on the SAME identity refuses FATAL (named holder).
			const rival = world.makeRivalAdapter("rival-instance");
			const rivalConnected = await rival.connect({ isReconnect: false });
			if (rivalConnected) throw new Error("rival must NOT acquire the identity");
			const snap = rival.lifecycle.statusSnapshot();
			if (snap.state !== "fatal" || snap.reason?.kind !== "token_lock_conflict") {
				throw new Error(`rival must end fatal lock-conflict, got ${snap.state}`);
			}

			// Survivor reconnects: re-seed DROPS the backlog that accumulated
			// while it was away (never replays into the agent).
			cli.advanceClock(30);
			world.pushChannelMessage(MAIN_CHANNEL, BOB, "@PiBot z-backlog");
			await engine.connect({ isReconnect: true });
			cli.advanceClock(5);
			world.pushChannelMessage(MAIN_CHANNEL, BOB, "@PiBot post-recovery");
			await world.sweep();
			await settleWall();

			const fatalAfterExhaustion =
				rival.lifecycle.statusSnapshot().state === "fatal";
			const dropPendingUpdatesOnRestart =
				dispatchCount(world, "z-backlog") === 0 &&
				dispatchCount(world, "post-recovery") === 1;
			return {
				generationsBumped: engine.connectLog.length - genBefore,
				dropPendingUpdatesOnRestart,
				fatalAfterExhaustion,
			};
		},

		/**
		 * CLASS DELTA mapping: no heartbeats exist; the escalation analog is
		 * the CLI TIMEOUT LADDER — two consecutive rc-124 timeouts surface the
		 * documented error-contract shape, NEVER latch fatal, and the loop
		 * survives to resume delivery ("reconnectTriggered" := loop alive and
		 * delivering again after the stuck streak).
		 */
		async heartbeatEscalation() {
			const world = freshWorld("buzz-timeouts");
			const { engine, cli } = world;
			await world.connectAndAwaitLive();

			cli.scriptTimeout("messages"); // stuck probe #1
			await world.sweep();
			if (engine.pollFailures < 1 || engine.lifecycle.statusSnapshot().state !== "active") {
				throw new Error("timeout #1 must not latch or kill the loop");
			}
			cli.scriptTimeout("messages"); // stuck probe #2
			await world.sweep();
			if (engine.lifecycle.statusSnapshot().state !== "active") {
				throw new Error("timeout #2 must not latch fatal");
			}
			if (engine.pollFailures !== 2) {
				throw new Error(`expected 2 stuck probes, got ${engine.pollFailures}`);
			}
			// The rc-124 error CONTRACT surfaces with its classified shape:
			// "timeout: buzz messages timed out after 30s (exit 124)".
			if (!/\(exit 124\)/.test(engine.lastCliError)) {
				throw new Error(
					`timeout error contract missing exit code, got ${JSON.stringify(engine.lastCliError)}`,
				);
			}

			cli.advanceClock(3);
			world.pushChannelMessage(MAIN_CHANNEL, ALICE, "@PiBot after-timeouts");
			await world.sweep();
			await settleWall();
			const reconnectTriggered =
				dispatchCount(world, "after-timeouts") === 1 && engine.pollLoopActive;
			return { stuckProbes: 2, reconnectTriggered };
		},
	};
}

/** Advance the fixture clock and push messages at distinct seconds. */
function cliAdvanceAndPush(world: BuzzWorld, contents: string[]): void {
	for (const content of contents) {
		world.cli.advanceClock(1);
		world.pushChannelMessage(MAIN_CHANNEL, ALICE, content);
	}
}
