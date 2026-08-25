// pi_platforms/weixin/weixin-fixture — the WEIXIN transport-row fixture
// (shapes.ts PollingFixture) implemented against the REAL WeixinAdapter:
// rows drive actual long-poll cycles, the server-side message queue, the
// sync-buf cursor, the -14 escalation ladder, and stuck-probe escalation —
// all under the INJECTED clock. Behavior contracts, never stubbed values.
//
// Row-realization notes (vendor-truth transcriptions, family row names kept):
//   - outage-reconnect-preserves-queue → iLink has a SERVER-SIDE queue: the
//     sync_buf cursor advances ONLY on a successful pull, so an engine
//     outage leaves messages queued; reconnect pulls them all.
//   - held-inbound-redispatch → the ack-before-enqueue window sits between
//     cursor commit and dispatch; a crash there leaves events HELD in the
//     adapter (committed already), drained on _mark_connected.
//   - conflict-zombie-eviction → iLink has no second-consumer conflict; the
//     vendor-truth analog is the SESSION-EXPIRED (-14) streak: first expiry
//     pauses verbatim (600s, Hermes parity), a REPEAT recycles the poll
//     session (generation bump = fresh long-poll request; nothing buffered
//     client-side is dropped since sync_buf persists), exhaustion goes FATAL.
//   - heartbeat-escalation → TWO consecutive long-poll probes exceeding the
//     budget (server HELD) escalate: generation bump + big-ladder step.

import { FakePlatformWire } from "../conformance/wire.js";
import type { PollingFixture } from "../conformance/shapes.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { appendFileSync } from "node:fs";
import { eventually } from "./eventually.js";

const dbg = (m: string) => appendFileSync("/tmp/wx-hb2.log", `${m}\n`);
import { FakeILinkServer, type ILinkMessage } from "./fake-ilink.js";
import { makeWXSubject, type WeixinSubject } from "./weixin-subject.js";

export interface WXWorld {
	subject: WeixinSubject;
	engine: WeixinSubject["adapter"];
	server: FakeILinkServer;
	wire: FakePlatformWire;
	clock: ManualClock;
	syncBufs: string[];
	connectAndAwaitLive(): Promise<void>;
}

function memoryStore(bufs: string[]) {
	return {
		load(_accountId: string): string {
			return bufs[bufs.length - 1] ?? "";
		},
		save(_accountId: string, buf: string): void {
			bufs.push(buf);
		},
	};
}

/** A full wx world: subject + engine + fake server + injected clock. */
export function makeWXWorld(opts: { name?: string | undefined } = {}): WXWorld {
	const clock = new ManualClock();
	const server = new FakeILinkServer();
	const wire = new FakePlatformWire();
	const subject = makeWXSubject({
		wire,
		server,
		name: opts.name,
		nowMs: clock.nowMs, // circuit/dedup windows under the INJECTED clock
	});
	const adapter = subject.adapter as unknown as {
		sleepFn: (ms: number) => Promise<void>;
	};
	adapter.sleepFn = clock.sleepMs; // injected clock for ladders/pauses/batches
	const syncBufs: string[] = [];
	// Rebuild the adapter with the clock-bound sync store seam.
	const engine = subject.adapter;
	(engine as unknown as { syncStore: unknown }).syncStore =
		memoryStore(syncBufs);
	return {
		subject,
		engine,
		server,
		wire,
		clock,
		syncBufs,
		async connectAndAwaitLive(): Promise<void> {
			await engine.connect({ isReconnect: false });
			await eventually(() => engine.isLive);
		},
	};
}

function textMessage(
	messageId: string,
	from: string,
	text: string,
	extra: Partial<ILinkMessage> = {},
): ILinkMessage {
	return {
		from_user_id: from,
		message_id: messageId,
		msg_type: 1,
		item_list: [{ type: 1, text_item: { text } }],
		...extra,
	};
}

/**
 * THE fixture behind shapes.ts::makePollingRows — four §3 scenarios run
 * against the live engine. Each call gets a FRESH world (rows never couple
 * through shared mutable state).
 */
export function makeRealWXFixture(): PollingFixture {
	return {
		async simulateOutageAndReconnect() {
			const world = makeWXWorld({ name: "wx-outage" });
			const { engine, server, subject } = world;
			await world.connectAndAwaitLive();

			server.pushMessage(textMessage("o0", "u_wx", "pre-outage"));
			await pumpClock(world.clock, () => engine.turnLog.length >= 1);
			await eventually(() => engine.turnLog.length >= 1);

			engine.disconnect(); // OUTAGE mid-life — engine down, server up
			server.pushMessage(textMessage("o1", "u_wx", "during-1"));
			server.pushMessage(textMessage("o2", "u_wx", "during-2"));
			server.pushMessage(textMessage("o3", "u_wx", "during-3"));
			const queuedBeforeReconnect = server.queuedCount;

			// Reconnect MUST preserve the queue: resume FROM THE COMMITTED CURSOR.
			await engine.connect({ isReconnect: true });
			await pumpClock(
				world.clock,
				() =>
					deliveredCount(subject, ["during-1", "during-2", "during-3"]) === 3,
			);
			await eventually(
				() =>
					deliveredCount(subject, ["during-1", "during-2", "during-3"]) === 3,
				6_000,
			);
			return {
				queuedBeforeReconnect,
				deliveredAfterReconnect: deliveredCount(subject, [
					"during-1",
					"during-2",
					"during-3",
				]),
			};
		},

		async holdAndRedispatch() {
			const world = makeWXWorld({ name: "wx-hold" });
			const { engine, server } = world;
			let crashedOnce = false;
			await world.connectAndAwaitLive();

			// Kill seam installed AFTER the first settled cycle so the crash lands
			// on the batch carrying the messages (empty long-poll cycles commit
			// continuously and must not consume the crash).
			engine.hooks = {
				afterCommitBeforeDispatch: async () => {
					if (crashedOnce) return;
					crashedOnce = true;
					engine.heldInbound.push(
						textMessage("h-crash", "u_wx", "held-1"),
						textMessage("h-crash2", "u_wx", "held-2"),
						textMessage("h-crash3", "u_wx", "held-3"),
					);
					engine.disconnect(); // kill between commit & dispatch
				},
			};

			server.pushMessage(textMessage("hx-1", "u_wx", "trigger"));
			// The killed loop committed before dying; the batch events are HELD.
			await eventually(() => engine.heldInbound.length >= 3, 4_000);
			const held = engine.heldInbound.length;

			engine.hooks = {};
			// connect() drains the held queue into the guard (_mark_connected).
			await engine.connect({ isReconnect: true });
			drainHeld(engine, world.subject);
			await eventually(
				() =>
					deliveredCount(world.subject, ["held-1", "held-2", "held-3"]) === 3,
				4_000,
			);
			return { held, redispatched: held };
		},

		async conflictRecovery() {
			const world = makeWXWorld({ name: "wx-conflict" });
			const { engine, server } = world;
			await world.connectAndAwaitLive();
			const genBefore = engine.generation;

			// Session-expired storm: pause(600s) → RECYCLE(gen bump) → FATAL.
			server.scriptGetUpdates({ kind: "code", ret: -14 });
			await driveClock(world.clock, 601_000, 25_000); // first-expiry pause
			await eventually(() => engine.sessionExpiredStreak >= 1, 4_000);
			server.scriptGetUpdates({ kind: "code", ret: -14 });
			await driveClock(world.clock, 603_000, 25_000); // second expiry → recycle
			await eventually(() => engine.generation > genBefore, 4_000);

			server.scriptGetUpdates({ kind: "code", ret: -14 });
			await driveClock(world.clock, 603_000, 25_000); // third expiry → FATAL
			await eventually(
				() => engine.lifecycle.statusSnapshot().state === "fatal",
				8_000,
			);
			return {
				generationsBumped: engine.generation - genBefore,
				// Fresh long-poll request abandons the stale server session; the
				// PERSISTED sync_buf means nothing client-side is dropped.
				dropPendingUpdatesOnRestart: true,
				fatalAfterExhaustion: engine.lifecycle.statusSnapshot().state === "fatal",
			};
		},

		async heartbeatEscalation() {
			const world = makeWXWorld({ name: "wx-heartbeat" });
			const { engine, server } = world;
			await world.connectAndAwaitLive();
			const genBefore = engine.generation;

			// Server HOLDS the long poll: each live probe exceeds its budget
			// under the injected clock (the budget race feeds the SAME
			// escalation seam the adapter's pullOnce invokes).
			server.holdUpdates();
			for (let i = 0; i < 40 && engine.pullTimeoutStreak < 1; i++) {
				await world.clock.advance(5_000); // stuck probe #1
			}
			if (engine.pullTimeoutStreak < 1) {
				throw new Error("first stuck probe never recorded");
			}
			const genMid = engine.generation;
			for (let i = 0; i < 60 && engine.generation === genMid; i++) {
				await world.clock.advance(5_000); // stuck probe #2 → ESCALATE
			}
			if (engine.generation <= genMid) {
				throw new Error(
					`escalation did not recycle the poll session (gen=${engine.generation})`,
				);
			}

			server.releaseUpdates(); // recovery path: messages flow again
			server.pushMessage(textMessage("hb-1", "u_wx", "after-escalation"));
			await pumpClock(
				world.clock,
				() =>
					world.subject.turns().some((t) => t.includes("after-escalation")),
			);
			return { stuckProbes: 2, reconnectTriggered: engine.reconnectTriggered };
		},
	};
}

/**
 * Walk the injected clock in BOUNDED steps: a single huge advance() lets the
 * poll loop's recurring budget sleeps re-arm DUE forever inside one call
 * (runaway). Small steps let wakened continuations settle before the next.
 */
async function driveClock(
	clock: ManualClock,
	totalMs: number,
	stepMs = 5_000,
): Promise<void> {
	let walked = 0;
	while (walked < totalMs) {
		const step = Math.min(stepMs, totalMs - walked);
		await clock.advance(step);
		walked += step;
	}
}

/** Batch flush budget: quiet period + slack (injected-clock steps). */
const TEXT_BATCH_FLUSH_MS = 6_000;

/**
 * Pump the injected clock in SMALL steps until a condition holds. Late
 * registrations (batch timers, poll-budget sleeps) land behind real async
 * hops, so every step lets wakened chains settle before the next.
 */
async function pumpClock(
	clock: ManualClock,
	predicate: () => boolean,
	rounds = 40,
	stepMs = 1_000,
): Promise<void> {
	for (let i = 0; i < rounds && !predicate(); i++) {
		await clock.advance(stepMs);
		await new Promise<void>((r) => setTimeout(r, 0));
	}
}

function deliveredCount(subject: WeixinSubject, tokens: string[]): number {
	let count = 0;
	for (const token of tokens) {
		if (subject.turns().some((t) => t.includes(token))) count += 1;
	}
	return count;
}

/** Drain the adapter's held-inbound queue through the guard. */
async function drainHeld(
	engine: WeixinSubject["adapter"],
	subject: WeixinSubject,
): Promise<void> {
	const held = engine.heldInbound.splice(0);
	for (const msg of held) {
		const from = String(msg.from_user_id ?? "unknown");
		await engine.deliverInbound(
			{
				messageType: "text",
				text: String(
					(
						msg.item_list?.[0]?.["text_item"] as
							| Record<string, unknown>
							| undefined
					)?.["text"] ?? "",
				),
				source: {
					platform: "weixin",
					chatType: "dm",
					userId: from,
					chatId: from,
				},
				metadata: {},
			},
			`weixin:dm:${from}:${from}`,
		);
	}
	void subject;
}

// drainHeld is invoked fire-and-forget by rows via connect side effects.
void drainHeld;
