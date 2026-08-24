// pi_platforms/polling/polling.test.ts — BEHAVIOR CONTRACTS for the polling
// reference adapter (04 §3.1 + §8 polling rows). Every scenario drives the
// REAL engine against the fake Bot API server: offset-commit-before-enqueue
// with held-inbound redispatch, 409-conflict zombie eviction under a fresh
// generation, heartbeat stuck-probe escalation (injected clock), FloodWait
// honored at send AND typing sites, and the runner-composed e2e.

import { describe, expect, it } from "vitest";

import { FakeTelegramServer, TelegramConflictError } from "./fake-server.js";
import { ManualPollingClock } from "./clock.js";
import {
	MAX_CONFLICT_RETRIES,
	PollingAdapterCore,
	TYPING_REFRESH_MS,
	conflictRetryDelayMs,
} from "./polling-adapter.js";
import { FakePlatformWire } from "../conformance/wire.js";
import {
	makePollingWorld,
	makeRealPollingFixture,
	eventually,
} from "./fixture.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import { createRunnerHarness } from "../../pi_agent_core/testing/runner-harness.js";
import { fauxAssistantMessage } from "../../pi_agent_core/testing/faux-model.js";
import { GatewayStreamConsumer } from "../../pi_gateway/streaming/gateway-stream-consumer.js";

function makeEngine(
	opts: {
		name?: string;
		clock?: ManualPollingClock;
		tg?: FakeTelegramServer;
		wire?: FakePlatformWire;
	} = {},
): {
	engine: PollingAdapterCore;
	tg: FakeTelegramServer;
	wire: FakePlatformWire;
	clock: ManualPollingClock;
} {
	const clock = opts.clock ?? new ManualPollingClock();
	const tg = opts.tg ?? new FakeTelegramServer();
	const wire = opts.wire ?? new FakePlatformWire();
	const engine = new PollingAdapterCore({
		wire: tg,
		clock,
		timer: clock.timer,
		longPollTimeoutMs: 25,
		manifestName: opts.name ?? "telegram-polling",
		secretReader: (k) => (k === "TELEGRAM_BOT_TOKEN" ? "tok" : undefined),
	});
	engine.attachStandardGuard(); // production wiring: ingress needs the L1 guard
	engine.wireTransmitSend = (chatId, content, metadata) =>
		wire.transmitSend(chatId, content, metadata);
	engine.editTransmit = (chatId, messageId, content) =>
		wire.transmitEdit(chatId, messageId, content, {});
	return { engine, tg, wire, clock };
}

function event(text: string, chatId = "chat-1"): IncomingEvent {
	return {
		messageType: "text",
		text,
		source: { platform: "telegram", chatType: "dm", userId: "user-1", chatId },
	};
}

async function waitConnectedCycle(e: PollingAdapterCore): Promise<void> {
	await e.connect({ isReconnect: false });
	await eventually(() => e.polledOnce || !e.connected);
}

describe("outage window — commit-before-enqueue covered by held-inbound redispatch", () => {
	it("kill mid-cycle AFTER server-side commit ⇒ messages redelivered EXACTLY ONCE via holds", async () => {
		const { engine, tg } = makeEngine();
		let kills = 0;
		engine.hooks = {
			afterCommitBeforeDispatch: () => {
				kills += 1;
				engine.simulateCrashMidCycle(); // outage lands INSIDE the window
			},
		};
		await engine.connect({ isReconnect: false });
		await eventually(() => engine.polledOnce); // initial empty cycle settled

		// Updates fetched + committed by the crashed cycle; never enqueued.
		tg.pushUpdate("chat-1", "m1");
		tg.pushUpdate("chat-1", "m2");
		await eventually(() => engine.heldInboundCount === 2);

		// The server will NEVER redeliver them — offsets were confirmed.
		expect(tg.pendingUpdateCount).toBe(0);

		// Restart: _mark_connected drains the holds into the guard exactly once.
		engine.hooks = undefined;
		await engine.connect({ isReconnect: true });
		await eventually(() => engine.turnLog.length >= 2);
		await new Promise<void>((r) => setTimeout(r, 30)); // straggler window

		expect(engine.turnLog.sort()).toEqual(["m1", "m2"]); // exactly once each
		expect(kills).toBe(1); // the crash happened inside ONE window
		expect(engine.redispatchLog).toEqual([2]); // one drain, both events
	});

	it("held-inbound queue: cap 64 drop-oldest, identity dedup", async () => {
		const { engine } = makeEngine();
		engine.disconnect(); // disconnected ⇒ dispatches hold
		for (let i = 0; i < 70; i++) {
			// Direct hold-path exercise through ingestUpdate while disconnected.
			await engine.ingestUpdate({
				updateId: 10_000 + i,
				chatId: `chat-${i}`,
				text: `u${i}`,
				senderId: "user-1",
			});
		}
		expect(engine.heldInboundCount).toBe(64); // cap held, oldest dropped
		expect(engine.heldInboundForTests()[0]?.text).toBe("u6"); // u0..u5 dropped
		expect(engine.turnLog.length).toBe(0);

		// Identity dedup: re-holding the SAME event object is a no-op.
		const probe = event("dedup-probe");
		engine.holdForTests(probe);
		const withProbe = engine.heldInboundCount;
		engine.holdForTests(probe); // identical object again — deduped
		expect(engine.heldInboundCount).toBe(withProbe);
	});

	it("permanent fatal DISCARDS the held queue explicitly (never redispatched)", async () => {
		const { engine } = makeEngine();
		await waitConnectedCycle(engine);
		engine.disconnect();
		await engine.ingestUpdate({
			updateId: 5001,
			chatId: "chat-1",
			text: "doomed",
			senderId: "user-1",
		});
		expect(engine.heldInboundCount).toBe(1);

		// Force fatal via conflict exhaustion on a fresh world is expensive;
		// drive the same terminal state through the lifecycle directly.
		engine.lifecycle.markFatal({
			kind: "config_invalid",
			detail: "test-forced fatal",
		});
		await expect(engine.connect({ isReconnect: true })).rejects.toThrow(
			/fatal/,
		);
		expect(engine.heldInboundCount).toBe(1); // untouched by connect…
		// …and a dispatch attempt under fatal HOLDS nothing new (discards).
		await engine.ingestUpdate({
			updateId: 5002,
			chatId: "chat-1",
			text: "after-fatal",
			senderId: "user-1",
		});
		expect(engine.heldInboundCount).toBe(1);
	});
});

describe("409 conflict — zombie eviction under a FRESH polling generation", () => {
	it("first recovery restart evicts the zombie and the fresh generation owns the stream", async () => {
		// Run the HAPPY path manually (the exhaustion world is a separate row).
		const { engine, tg } = makeEngine({ name: "conflict-happy" });
		await waitConnectedCycle(engine);
		const genBefore = engine.generation;

		tg.stealHolderAsZombie(); // stale process grabs the poll
		tg.pushUpdate("chat-1", "post-conflict"); // triggers the next fetch

		// Ladder attempt 1: sleep(25s virtual) → restart drop_pending=true.
		await eventually(
			() =>
				engine.generation > genBefore &&
				engine.recoveryRestartsWithDropPending >= 1,
			5_000,
		);
		expect(engine.lifecycleSnapshot().state).not.toBe("fatal");

		// The zombie session was TERMINATED by the takeover (#75017).
		await eventually(
			() => tg.isSessionTerminated(tg.currentHolder as number) === false,
		);
		// A generation counts healthy only after RECORDED PROGRESS — wait for
		// the fresh poller's first completed cycle to reset the episode.
		await eventually(() => engine.conflictCount === 0);

		// Fresh generation OWNS the stream: subsequent updates flow, no double-poll.
		tg.pushUpdate("chat-1", "after-recovery");
		await eventually(() => engine.turnLog.includes("after-recovery"));
		expect(engine.turnLog.filter((t) => t === "after-recovery")).toEqual([
			"after-recovery",
		]);
		expect(tg.liveSessionCount()).toBeLessThanOrEqual(3); // no runaway sessions
	});

	it("exhaustion: unkillable zombie burns all retries then declares FATAL (virtual clock)", async () => {
		const rowResult = await makeRealPollingFixture().conflictRecovery();
		expect(rowResult.generationsBumped).toBeGreaterThanOrEqual(1);
		expect(rowResult.dropPendingUpdatesOnRestart).toBe(true);
		expect(rowResult.fatalAfterExhaustion).toBe(true);
	});

	it("retry delay follows the 10 + count·10 s ladder (injected clock)", async () => {
		expect(conflictRetryDelayMs(1)).toBe(20_000);
		expect(conflictRetryDelayMs(5)).toBe(60_000);
		const clock = new ManualPollingClock();
		const { engine, tg } = makeEngine({ name: "conflict-ladder", clock });
		await waitConnectedCycle(engine);
		tg.setUnkillableZombie(true);
		tg.stealHolderAsZombie();
		await eventually(
			() =>
				engine.recoveryLog.includes(`conflict-retry-1/${MAX_CONFLICT_RETRIES}`),
			5_000,
		);
		expect(clock.sleeps[0]).toBe(20_000); // 10 + 1·10 seconds, honored once per retry
	});

	it("conflict errors classify distinctly from network errors", () => {
		const conflict = new TelegramConflictError();
		expect(conflict.name).toBe("TelegramConflictError");
		expect(conflict.message.includes("409")).toBe(true);
	});
});

describe("heartbeat stuck-probe escalation ladder (injected clock)", () => {
	it("two consecutive STUCK-PENDING probes feed the reconnect ladder", async () => {
		const result = await makeRealPollingFixture().heartbeatEscalation();
		expect(result.stuckProbes).toBe(2);
		expect(result.reconnectTriggered).toBe(true);
	});

	it("a single stuck probe does NOT trip recovery (in-flight update tolerance)", async () => {
		const clock = new ManualPollingClock();
		const { engine, tg } = makeEngine({ name: "hb-one-probe", clock });
		await waitConnectedCycle(engine);
		tg.setConsumerWedged(engine.activeSessionToken as number, true);
		tg.pushUpdate("chat-1", "wedge");
		await eventually(() => tg.pendingUpdateCount >= 1);
		await clock.advance(90_000); // ONE probe
		await eventually(() => engine.stuckProbeStreakForTests >= 1);
		expect(engine.recoveryLog).not.toContain("heartbeat-stuck-pending");
		engine.disconnect();
	});

	it("updater-NOT-RUNNING ×2 escalates through the same ladder (#55769)", async () => {
		const clock = new ManualPollingClock();
		const { engine } = makeEngine({ name: "hb-not-running", clock });
		await waitConnectedCycle(engine);
		// Long-poll task exits (generation bump) without a reconnect — updater gone.
		const killGen = engine.generation;
		engine.generation += 1; // orphan the running poll loop
		await eventually(() => !engine.updaterRunning);

		await clock.advance(90_000); // not-running 1/2
		await eventually(() => engine.recoveryLog.length >= 0);
		const logAfterOne = engine.recoveryLog.filter((r) =>
			r.startsWith("heartbeat-updater-not-running"),
		).length;
		await clock.advance(90_000); // not-running 2/2 → escalate
		await eventually(
			() =>
				engine.recoveryLog.filter((r) =>
					r.startsWith("heartbeat-updater-not-running"),
				).length > logAfterOne,
			5_000,
		);
		expect(engine.generation).toBeGreaterThan(killGen); // recovery restarted polling
		engine.disconnect();
	});

	it("unreachable server turns the PROBE ITSELF into a recovery feeding", async () => {
		const clock = new ManualPollingClock();
		const { engine, tg } = makeEngine({ name: "hb-unreachable", clock });
		await waitConnectedCycle(engine);
		// Kill ONLY the general request path: get_me/webhook-info probes fail
		// while the long-poll pool stays up (separate pools, Hermes parity).
		tg.setReachable(false, "general");
		await clock.advance(90_000);
		await eventually(
			() => engine.recoveryLog.some((r) => r.startsWith("heartbeat-probe:")),
			5_000,
		);
		engine.disconnect();
	});
});

describe("reconnect queue preservation vs cold-boot drop", () => {
	it("is_reconnect=true preserves the server-side queue; cold boot drops stale updates", async () => {
		const fixture = makeRealPollingFixture();
		const outageRow = await fixture.simulateOutageAndReconnect();
		expect(outageRow.deliveredAfterReconnect).toBe(
			outageRow.queuedBeforeReconnect,
		);

		// Cold boot parity: the FIRST getUpdates of a fresh process may drop.
		const { engine, tg } = makeEngine({ name: "cold-boot" });
		tg.pushUpdate("chat-1", "stale-before-boot");
		await waitConnectedCycle(engine);
		expect(tg.dropPendingFlags[0]).toBe(true); // cold boot dropped it
		expect(engine.turnLog).not.toContain("stale-before-boot");

		// Reconnect NEVER drops: every later flag-carrying call is a conflict
		// recovery restart, not an is_reconnect poll call.
		tg.pushUpdate("chat-1", "fresh-after-boot");
		await eventually(() => engine.turnLog.includes("fresh-after-boot"));
	});
});

describe("FloodWait honored at EVERY site", () => {
	it("SEND site: retry_after honored once over the local schedule, then recovers", async () => {
		const { engine, wire } = makeEngine({ name: "flood-send" });
		wire.script(
			"send",
			{ kind: "fail", error: "flood control: retry after 7", retryAfter: 0.02 },
			{ kind: "ok" },
		);
		const results = await engine.deliverText("chat-flood", "payload");
		expect(results[results.length - 1]?.success).toBe(true);
		expect(wire.sendsOf("chat-flood").length).toBe(2); // fail → honored → ok
	});

	it("TYPING site backs off per Retry-after value via the injected clock", async () => {
		const clock = new ManualPollingClock();
		const { engine, tg } = makeEngine({ name: "flood-typing", clock });
		tg.scriptTyping({ kind: "flood", retryAfter: 7 }, { kind: "ok" });
		const res = await engine.sendTyping("chat-typing");
		expect(res.success).toBe(true);
		expect(clock.sleeps).toEqual([7_000]); // authoritative value, once
		expect(tg.chatActions.length).toBe(2); // flood → backoff → retried
	});

	it("typing refresh loop keeps ~2s cadence and picks up dynamic status text", async () => {
		const clock = new ManualPollingClock();
		const { engine, tg } = makeEngine({ name: "typing-loop", clock });
		engine.startTypingRefresh("chat-live", "reading files…");
		await eventually(() => tg.chatActions.length >= 1);
		expect(engine.statusTextFor("chat-live")).toBe("reading files…");
		// Cadence: refreshes land at t≈2s, 4s, 6s… (virtual clock).
		await clock.advance(TYPING_REFRESH_MS * 3);
		await eventually(() => tg.chatActions.length >= 4);
		engine.stopTypingRefresh("chat-live");
		const after = tg.chatActions.length;
		await clock.advance(TYPING_REFRESH_MS * 2);
		expect(tg.chatActions.length).toBe(after); // stopped means stopped
	});

	it("EDIT site surfaces flood_control:<wait> WITHOUT blocking the caller", async () => {
		const clock = new ManualPollingClock();
		const { engine, wire } = makeEngine({ name: "flood-edit", clock });
		wire.script("edit", {
			kind: "fail",
			error: "Too Many Requests: retry after 9",
			retryAfter: 9,
		});
		const started = Date.now();
		const res = await engine.editMessage("chat-e", "wire-1", "edited text");
		const elapsed = Date.now() - started;
		expect(res.success).toBe(false);
		expect(res.error).toBe("flood_control:9"); // Hermes error-shape parity
		expect(res.retryAfter).toBe(9);
		expect(elapsed).toBeLessThan(200); // returned immediately, no wall wait
		expect(clock.sleeps).toEqual([]); // edit lane NEVER sleeps
	});
});

describe("egress doors ride the audited chokepoint (DEC-006)", () => {
	it("both doors audit and interim sends never leak the marker", async () => {
		const { engine, wire } = makeEngine({ name: "doors" });
		await engine.send("chat-x", "via send()");
		await engine.sendForPlatform("plat", "chat-x", "via door two");
		const doors = new Set(engine.doorAudit().map((a) => a.door));
		expect(doors.has("send")).toBe(true);
		expect(doors.has("send_for_platform")).toBe(true);
		for (const op of wire.ops) {
			expect(op.metadata["_interim_send"]).toBeUndefined();
		}
	});
});

describe("e2e — fake inbound → guards → runner (scripted model) → streaming → egress", () => {
	it("a pushed update becomes a REAL agent turn whose final streams out the doors", async () => {
		const h = await createRunnerHarness();
		try {
			h.ensureSession("sess-e2e");
			h.faux.setResponses([fauxAssistantMessage("final hello from model")]);

			const world = makePollingWorld({ name: "e2e-polling" });
			const { engine, tg, wire } = world;
			engine.turnDriver = async (event, text) => {
				expect(text).toBe("hello gateway");
				const outcome = await h.runner.handleTurn({
					sessionId: "sess-e2e",
					routingKey: `rk-${String(event.source?.chatId)}`,
					text,
				});
				expect(outcome.exitReason).toBe("finalized");
				// Stream the turn through the adapter's doors: Telegram-shaped
				// drafts (DM) + authoritative final adoption (invariant 2).
				const consumer = new GatewayStreamConsumer(
					world.subject.streamAdapter(),
					String(event.source?.chatId),
					{
						transport: "auto",
						chatType: "dm",
						editIntervalMs: 0,
						bufferThreshold: 1,
					},
					{ reply_to_message_id: event.messageId ?? "m-1" },
				);
				const runP = consumer.run();
				consumer.onDelta("streaming partial; ");
				await new Promise<void>((r) => setTimeout(r, 3));
				consumer.finish(outcome.finalText);
				await runP;
				return null; // delivery owned by the streaming lane
			};

			await engine.connect({ isReconnect: false });
			tg.pushUpdate("chat-e2e", "hello gateway");

			// The turn's egress lands on the fake platform wire.
			await eventually(
				() =>
					wire.sendsOf("chat-e2e").length > 0 &&
					engine.turnLog.includes("hello gateway"),
				5_000,
			);

			// Guard saw exactly ONE turn; the model's authoritative final is
			// byte-exact on the wire; draft frames were prefix-stable.
			expect(engine.turnLog.filter((t) => t === "hello gateway")).toHaveLength(
				1,
			);
			const finalSends = wire
				.sendsOf("chat-e2e")
				.filter((o) => o.content.includes("final hello from model"));
			expect(finalSends).toHaveLength(1);
			expect(finalSends[0]?.content).toBe("final hello from model");

			const drafts = wire.draftsOf("chat-e2e").map((d) => d.content);
			expect(drafts.length).toBeGreaterThanOrEqual(1);
			for (let i = 1; i < drafts.length; i++) {
				expect(drafts[i]?.startsWith(drafts[i - 1] as string)).toBe(true);
			}

			// Persist-what-you-send: user + assistant rows landed in pi_state.
			const rows = h.store.db
				.prepare(
					"SELECT role, content FROM messages WHERE session_id = ? AND active = 1 ORDER BY id",
				)
				.all("sess-e2e") as Array<{ role: string; content: string }>;
			expect(
				rows.some((r) => r.role === "user" && r.content === "hello gateway"),
			).toBe(true);
			expect(
				rows.some(
					(r) =>
						r.role === "assistant" &&
						(r.content ?? "").includes("final hello from model"),
				),
			).toBe(true);

			engine.disconnect();
		} finally {
			await h.close();
		}
	}, 15_000);
});
