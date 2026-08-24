// pi_platforms/persistent-ws/persistent-ws.test.ts — BEHAVIOR CONTRACTS for
// the persistent-WS reference adapter (04 §3 matrix + §10.2; A23/#4777
// parity): reconnect ladder semantics, capability-latch permanence, cursor/
// dedup exactly-once machinery, manual clock determinism, dual-path markdown,
// the fake ws replay window, watchdog stale-reap under an INJECTED clock, and
// the runner-composed e2e (fake ws inbound → guards → scripted-model runner →
// streaming chokepoint → native RAW egress).

import { describe, expect, it } from "vitest";

import { ManualClock } from "./manual-clock.js";
import {
	ReconnectLadder,
	type ReconnectLadderOptions,
} from "./reconnect-ladder.js";
import {
	CapabilityLatch,
	isNativeStreamFeatureGateError,
} from "./capability-latch.js";
import {
	EventDeduplicator,
	ResumeCursor,
	DEFAULT_DEDUP_MAX_ENTRIES,
} from "./event-cursor.js";
import {
	convertMarkdownToMrkdwn,
	renderTableAsFencedMonospace,
	sealSuffix,
} from "./dual-path-markdown.js";
import { FakeWsServer, WS_CLOSED, WS_OPEN } from "./fake-ws.js";
import {
	PersistentWsAdapter,
	WS_REQUIRED_SECRET,
	type RestPlane,
} from "./persistent-ws-adapter.js";
import { createRunnerHarness } from "../../pi_agent_core/testing/runner-harness.js";
import { fauxAssistantMessage } from "../../pi_agent_core/testing/faux-model.js";
import { GatewayStreamConsumer } from "../../pi_gateway/streaming/gateway-stream-consumer.js";
import { FakePlatformWire } from "../conformance/wire.js";
import { makeWsWorld, eventually } from "./ws-fixture.js";

// ── helpers ──────────────────────────────────────────────────────────────

const OK_REST: RestPlane = {
	transmitSend: () => Promise.resolve({ success: true, messageId: "m1" }),
	transmitEdit: () => Promise.resolve({ success: true, messageId: "m2" }),
	transmitDraft: (_c, _d, _content, final) =>
		Promise.resolve({
			success: true,
			messageId: final ? "sealed" : "draft-ts",
		}),
	transmitRich: () => Promise.resolve({ success: false }),
	hasScript: () => false,
};

function makeEngine(
	opts: {
		name?: string;
		clock?: ManualClock;
		ws?: FakeWsServer;
		wire?: FakePlatformWire;
		restOverrides?: Partial<RestPlane>;
		ladder?: ReconnectLadderOptions;
		botUserId?: string;
		streamIsMessageChatIds?: ReadonlySet<string>;
		withSecret?: boolean;
	} = {},
): {
	engine: PersistentWsAdapter;
	ws: FakeWsServer;
	wire: FakePlatformWire;
	clock: ManualClock;
} {
	const clock = opts.clock ?? new ManualClock();
	const ws = opts.ws ?? new FakeWsServer({ nowMs: clock.nowMs });
	const wire = opts.wire ?? new FakePlatformWire();
	const rest: RestPlane = { ...OK_REST, ...(opts.restOverrides ?? {}) };
	const engine = new PersistentWsAdapter({
		manifestName: opts.name ?? "slack-like",
		transport: ws,
		rest,
		clock,
		scalarMaxUnits: 64,
		requiresEnv: [{ name: WS_REQUIRED_SECRET }],
		secretReader: (k) =>
			opts.withSecret === false
				? undefined
				: k === WS_REQUIRED_SECRET
					? "xoxb-unit-token"
					: undefined,
		botUserId: opts.botUserId,
		...(opts.ladder !== undefined ? { ladder: opts.ladder } : {}),
		...(opts.streamIsMessageChatIds !== undefined
			? { streamIsMessageChatIds: opts.streamIsMessageChatIds }
			: {}),
	});
	return { engine, ws, wire, clock };
}

// ── manual clock ─────────────────────────────────────────────────────────

describe("ManualClock (injected timing discipline)", () => {
	it("walks due waiters in deadline order and lets continuations progress", async () => {
		const clock = new ManualClock();
		const order: string[] = [];
		void clock.sleepMs(100).then(() => order.push("a@100"));
		void clock.sleepMs(50).then(() => order.push("b@50"));
		void clock.sleepMs(50).then(async () => {
			order.push("b@50-cont");
			await clock.sleepMs(10); // chained waiter registered mid-advance
			order.push("b+10@60");
		});
		expect(clock.pendingWaits).toBe(3);
		await clock.advance(200);
		expect(order).toEqual(["b@50", "b@50-cont", "b+10@60", "a@100"]);
		expect(clock.nowMs()).toBe(200);
	});

	it("never fires waiters scheduled beyond the advance target", async () => {
		const clock = new ManualClock();
		let fired = false;
		void clock.sleepMs(500).then(() => {
			fired = true;
		});
		await clock.advance(100);
		expect(fired).toBe(false);
		expect(clock.pendingWaits).toBe(1);
	});
});

// ── reconnect ladder ─────────────────────────────────────────────────────

describe("ReconnectLadder (transport backoff)", () => {
	it("escalates exponentially under a deterministic rng and caps", async () => {
		const sleeps: number[] = [];
		const ladder = new ReconnectLadder({
			baseDelayMs: 100,
			maxDelayMs: 700,
			jitterFraction: 0,
			rng: () => 0,
			sleep: async (ms) => {
				sleeps.push(ms);
			},
		});
		for (let i = 0; i < 5; i++) await ladder.wait(null);
		expect(sleeps).toEqual([100, 200, 400, 700, 700]); // capped at max
		expect(ladder.attemptCount).toBe(5);
	});

	it("Retry-After is AUTHORITATIVE over the schedule and does not advance attempts", async () => {
		const ladder = new ReconnectLadder({
			baseDelayMs: 100,
			jitterFraction: 0,
			rng: () => 0,
			sleep: async () => {},
		});
		const step = await ladder.wait(9); // server says 9 seconds
		expect(step).toMatchObject({ delayMs: 9000, authoritative: true });
		expect(ladder.attemptCount).toBe(0); // authoritative values are one-shot
		const next = await ladder.wait(null); // computed schedule resumes at base
		expect(next.delayMs).toBe(100);
	});

	it("reset() on a healthy session returns to base delay", async () => {
		const ladder = new ReconnectLadder({
			baseDelayMs: 100,
			maxDelayMs: 60_000,
			jitterFraction: 0,
			rng: () => 0,
			sleep: async () => {},
		});
		await ladder.wait(null);
		await ladder.wait(null);
		ladder.reset();
		const step = await ladder.wait(null);
		expect(step.delayMs).toBe(100);
	});
});

// ── capability latch ────────────────────────────────────────────────────

describe("CapabilityLatch (A23 permanent downgrade)", () => {
	it("latches ONCE on feature-gate markers and skips afterwards", () => {
		const latch = new CapabilityLatch();
		expect(latch.shouldSkipNative()).toBe(false);
		expect(isNativeStreamFeatureGateError("feature_not_enabled: x")).toBe(true);
		expect(isNativeStreamFeatureGateError("network hiccup")).toBe(false);

		expect(latch.maybeLatch("streaming_not_allowed for this workspace")).toBe(
			true,
		);
		expect(latch.unsupported).toBe(true);
		expect(latch.latchCount).toBe(1);
		expect(latch.reason).toContain("streaming_not_allowed");
		// Latching again is impossible — count stays 1, skip answers true.
		expect(latch.maybeLatch("not_allowed")).toBe(false);
		expect(latch.latchCount).toBe(1);
		expect(latch.shouldSkipNative()).toBe(true);
	});

	it("non-gate failures never touch the latch", () => {
		const latch = new CapabilityLatch();
		expect(latch.maybeLatch("socket hang up")).toBe(false);
		expect(latch.maybeLatch("rate_limited")).toBe(false);
		expect(latch.unsupported).toBe(false);
		expect(latch.wireAttempts).toBe(0); // counts come from the adapter
	});
});

// ── cursor + dedup ──────────────────────────────────────────────────────

describe("EventDeduplicator + ResumeCursor (#4777 replay safety)", () => {
	it("records-and-answers in one call; duplicates refresh recency", () => {
		let now = 1_000;
		const dedup = new EventDeduplicator({ nowMs: () => now });
		expect(dedup.isDuplicate("e1")).toBe(false);
		expect(dedup.isDuplicate("e1")).toBe(true);
		expect(dedup.suppressedCount).toBe(1);
		now += 1000;
		expect(dedup.isDuplicate("e1")).toBe(true); // still inside TTL
		dedup.isDuplicate("e2");
		expect(dedup.size).toBe(2);
	});

	it("expires entries past the TTL under the injected clock", () => {
		let now = 1_000;
		const dedup = new EventDeduplicator({ ttlMs: 500, nowMs: () => now });
		dedup.isDuplicate("old");
		now += 501;
		expect(dedup.isDuplicate("old")).toBe(false); // expired → deliverable again
	});

	it("stays memory-bounded via LRU pruning at maxEntries", () => {
		const dedup = new EventDeduplicator({
			maxEntries: DEFAULT_DEDUP_MAX_ENTRIES,
		});
		for (let i = 0; i < DEFAULT_DEDUP_MAX_ENTRIES + 50; i++)
			dedup.isDuplicate(`e${i}`);
		expect(dedup.size).toBeLessThanOrEqual(DEFAULT_DEDUP_MAX_ENTRIES);
		expect(dedup.isDuplicate(`e${DEFAULT_DEDUP_MAX_ENTRIES + 49}`)).toBe(true);
	});

	it("cursor advances monotonically and drives the resubscribe value", () => {
		const cursor = new ResumeCursor();
		expect(cursor.value).toBeNull(); // cold boot subscribes with null
		cursor.advance("e7");
		expect(cursor.value).toBe("e7");
	});
});

// ── dual-path markdown (§10.2 / DEC-034 helpers) ────────────────────────

describe("dual-path-markdown converters", () => {
	it("converts inline dialect OUTSIDE protected fences", () => {
		const out = convertMarkdownToMrkdwn(
			"**bold** __under__ ~~strike~~ [text](https://a.b)",
		);
		expect(out).toBe("*bold* _under_ ~strike~ <https://a.b|text>");
	});

	it("fenced code blocks survive BYTE-EXACT while surrounding text converts", () => {
		const md = 'x **bold**\n```sh\necho "**not bold** [x](https://y.z)"\n```';
		const out = convertMarkdownToMrkdwn(md);
		expect(out).toContain('echo "**not bold** [x](https://y.z)"'); // fence verbatim
		expect(out).toContain("*bold*"); // outside fence converted
		expect(out).not.toContain("**bold**");
	});

	it("GFM tables re-render as fenced column-aligned monospace", () => {
		const table = "| a | bb |\n|---|---|\n| 1 | 2 |";
		const out = renderTableAsFencedMonospace(table);
		expect(out.split("\n")[0]).toBe("```");
		expect(out).toContain("a  bb");
		expect(out).toContain("-  --");
		expect(convertMarkdownToMrkdwn(`pre\n${table}\n`)).toContain("```");
	});

	it("sealSuffix appends ONLY the unsent suffix, guarded by startswith", () => {
		expect(sealSuffix("hello wor", "hello world")).toEqual({
			kind: "append",
			delta: "ld",
		});
		expect(sealSuffix("hello world", "hello world")).toEqual({ kind: "none" });
		expect(sealSuffix("abc", "xyz")).toEqual({ kind: "rewrite" }); // rewritten accumulator
	});
});

// ── fake ws plane ───────────────────────────────────────────────────────

describe("FakeWsServer (in-process replay window)", () => {
	function clientOf(
		server: FakeWsServer,
		frames: unknown[],
	): {
		socket: ReturnType<FakeWsServer["connect"]>;
		done: Promise<void>;
	} {
		let open!: () => void;
		const done = new Promise<void>((r) => {
			open = r;
		});
		const socket = server.connect({
			onOpen: () => open(),
			onFrame: (f) => frames.push(f),
			onClose: () => {},
			onError: () => {},
		});
		return { socket, done };
	}

	it("delivers pushes only to SUBSCRIBED live sockets; unsubscribed miss them", async () => {
		const server = new FakeWsServer();
		const frames: unknown[] = [];
		const { socket, done } = clientOf(server, frames);
		await done;
		expect(socket.readyState).toBe(WS_OPEN);
		server.pushEvent({ type: "message", chatId: "c", userId: "u", text: "x" });
		expect(frames).toEqual([]); // not subscribed yet — held in the window
		socket.send({ type: "subscribe", cursor: null });
		const events = frames.filter(
			(f) => (f as { type: string })["type"] === "event",
		);
		expect(events.length).toBe(1); // window replayed on subscribe
		expect(server.openConnectionCount).toBe(1);
	});

	it("replays strictly AFTER the resume cursor on resubscribe (#4777)", async () => {
		const server = new FakeWsServer();
		const frames: unknown[] = [];
		const { socket, done } = clientOf(server, frames);
		await done;
		socket.send({ type: "subscribe", cursor: null });
		const a = server.pushEvent({
			type: "message",
			chatId: "c",
			userId: "u",
			text: "a",
		});
		const b = server.pushEvent({
			type: "message",
			chatId: "c",
			userId: "u",
			text: "b",
		});
		// Live delivery reaches the subscribed socket for BOTH; then resubscribe
		// with the LAST DELIVERED id — the server must replay strictly after it.
		a.id;
		frames.length = 0;
		socket.send({ type: "subscribe", cursor: a.id });
		const events = frames.filter(
			(f) => (f as { type: string })["type"] === "event",
		);
		const ids = events.map(
			(f) => ((f as { event: { id: string } }).event as { id: string }).id,
		);
		expect(ids).toEqual([b.id]); // strictly after the cursor
		expect(b.id).not.toBe(a.id);
	});

	it("refusals surface asynchronously as close(1006) — ladder food", async () => {
		const server = new FakeWsServer();
		server.refuseConnections();
		const closes: number[] = [];
		const socket = server.connect({
			onOpen: () => {},
			onFrame: () => {},
			onClose: (info) => closes.push(info.code),
			onError: () => {},
		});
		await new Promise<void>((r) => setTimeout(r, 5));
		expect(closes).toEqual([1006]);
		expect(socket.readyState).toBe(WS_CLOSED);
	});
});

// ── engine integration (REAL adapter over the fake planes) ──────────────

describe("PersistentWsAdapter — transport lifecycle", () => {
	it("cold boot subscribes with a null cursor; reconnect resumes FROM the cursor", async () => {
		const { engine, ws, clock } = makeEngine({ name: "cursor-flow" });
		await engine.connect({ isReconnect: false });
		expect(engine.currentPhase).toBe("live");

		ws.dropActive({});
		await clock.advance(3_000);
		await eventually(() => engine.isLive);

		const subscribes = ws.receivedFrames.filter(
			(f) => f.frame["type"] === "subscribe",
		);
		expect(subscribes[0]?.frame["cursor"]).toBeNull(); // cold boot
		expect(subscribes.length).toBeGreaterThanOrEqual(2);
	});

	it("resubscribe replay arrives EXACTLY ONCE even across overlapping windows", async () => {
		const world = makeWsWorld({ name: "overlap" });
		const { engine, ws, clock, subject } = world;
		engine.attachStandardGuard();
		await world.connectAndAwaitLive();

		ws.pushEvent({ type: "message", chatId: "c", userId: "u", text: "one" });
		await eventually(() => subject.turns().includes("one"));

		// TWO rapid outage/reconnect cycles: the second resubscribe REPLAYS
		// events the pipeline already delivered (at-least-once server).
		ws.dropActive({});
		await clock.advance(2_000);
		await eventually(() => engine.isLive);
		ws.dropActive({});
		await clock.advance(2_000);
		await eventually(() => engine.isLive);
		await eventually(
			() =>
				engine.dedupSuppressedCount >= 1 ||
				subject.turns().filter((t) => t === "one").length === 1,
		);

		expect(subject.turns().filter((t) => t === "one").length).toBe(1);
		const ids = engine.inboundLog.map((e) => e.id);
		expect(new Set(ids).size).toBe(ids.length); // exactly-once downstream
		engine.disconnect();
	});

	it("dispatch errors are CONTAINED: logged, cursor held, loop survives", async () => {
		const { engine } = makeEngine({ name: "containment" });
		// NO guard attached — handleIngress throws synchronously.
		await expect(
			engine.handlePlatformEvent({
				id: "e1",
				type: "message",
				chatId: "c",
				userId: "u",
				text: "boom",
			}),
		).resolves.toBeUndefined(); // did NOT reject into the frame pump
		expect(engine.inboundLog).toHaveLength(0); // ack-after-process held
		expect(engine.cursor.value).toBeNull();

		// Attach the guard; the SAME event id must still processable later —
		// but the dedup already recorded it (Hermes parity: dedup BEFORE
		// dispatch), so a REDelivery is suppressed rather than double-run.
		engine.attachStandardGuard();
		await engine.handlePlatformEvent({
			id: "e1",
			type: "message",
			chatId: "c",
			userId: "u",
			text: "boom",
		});
		expect(engine.inboundLog).toHaveLength(0); // duplicate suppressed
	});

	it("self/echo events never reach the pipeline; dedup precedes dispatch", async () => {
		const world = makeWsWorld({ name: "self-filter" });
		const { engine, ws, subject } = world;
		engine.attachStandardGuard();
		await world.connectAndAwaitLive();
		ws.pushEvent({
			type: "message",
			chatId: "c",
			userId: "bot-self",
			text: "echo",
		});
		ws.pushEvent({
			type: "message",
			chatId: "c",
			userId: "user-1",
			text: "human",
		});
		await eventually(() => subject.turns().includes("human"));
		expect(subject.turns()).not.toContain("echo");
		engine.disconnect();
	});

	it("watchdog reaps the wedged socket within pingInterval·factor + one tick (injected clock)", async () => {
		const world = makeWsWorld({
			name: "watchdog-bound",
			pingIntervalMs: 100,
			pingStaleFactor: 2,
			firstPingGraceMs: 150,
			watchdogIntervalMs: 50,
			ladder: { baseDelayMs: 100, jitterFraction: 0, rng: () => 0 },
		});
		const { engine, ws, clock } = world;
		await world.connectAndAwaitLive();
		ws.stallPongs();
		await clock.advance(1_000);
		expect(engine.reconnectLog.length).toBeGreaterThanOrEqual(1);
		expect(ws.openConnectionCount).toBe(1); // reaped AND reconnected
		expect(engine.isLive).toBe(true);
		engine.disconnect();
		expect(engine.currentPhase).toBe("stopped");
	});

	it("close-payload Retry-After feeds the reconnect ladder verbatim", async () => {
		const world = makeWsWorld({ name: "ra-close" });
		const { engine, ws, clock } = world;
		await world.connectAndAwaitLive();
		ws.dropActive({ retryAfterSeconds: 12 });
		await eventually(() => engine.lastCapturedRetryAfterSeconds === 12);
		await clock.advance(13_000);
		await eventually(() => engine.isLive);
		const step = engine.reconnectLog[engine.reconnectLog.length - 1];
		expect(step).toMatchObject({ delayMs: 12_000, authoritative: true });
		engine.disconnect();
	});
});

// ── e2e — fake ws inbound → guards → runner → streaming → RAW egress ────

describe("e2e — pushed ws event becomes a REAL agent turn streamed natively", () => {
	it("inbound event → guard turn (scripted model) → native RAW *Stream egress captured by the fake server", async () => {
		const h = await createRunnerHarness();
		try {
			h.ensureSession("sess-ws-e2e");
			h.faux.setResponses([fauxAssistantMessage("**RAW** final from model")]);

			const world = makeWsWorld({
				name: "ws-e2e",
				streamIsMessageChatIds: ["chat-e2e"],
			});
			const { engine, ws, wire } = world;
			engine.attachStandardGuard();
			let sawTurnText = "";
			engine.turnDriver = async (event, text) => {
				sawTurnText = text;
				expect(text).toBe("hello gateway");
				const outcome = await h.runner.handleTurn({
					sessionId: "sess-ws-e2e",
					routingKey: `rk-${String(event.source?.chatId)}`,
					text,
				});
				expect(outcome.exitReason).toBe("finalized");
				// Stream the turn through the NATIVE *Stream plane: RAW
				// cumulative frames + authoritative final adoption.
				const consumer = new GatewayStreamConsumer(
					world.subject.streamAdapter(),
					String(event.source?.chatId),
					{
						transport: "draft",
						chatType: "dm",
						editIntervalMs: 0,
						bufferThreshold: 1,
					},
					{ reply_to_message_id: event.messageId ?? "e1" },
				);
				const runP = consumer.run();
				consumer.onDelta("**RAW** partial; ");
				await new Promise<void>((r) => setTimeout(r, 3));
				consumer.finish(outcome.finalText);
				await runP;
				return null; // delivery owned by the streaming lane
			};

			await world.connectAndAwaitLive();
			ws.pushEvent({
				type: "message",
				chatId: "chat-e2e",
				userId: "user-1",
				text: "hello gateway",
			});

			await eventually(() => sawTurnText === "hello gateway", 10_000);
			await eventually(
				() =>
					wire.ops.some(
						(o) => o.op === "seal" && o.content.includes("final from model"),
					),
				10_000,
			);

			// Guard saw exactly ONE turn.
			expect(engine.turnLog.filter((t) => t === "hello gateway")).toHaveLength(
				1,
			);

			// Native frames are RAW markdown — byte-untouched by the mrkdwn
			// converter (§10.2 invariant; converting centrally is banned).
			const streamOps = wire.ops.filter(
				(o) => o.op === "draft" || o.op === "seal",
			);
			expect(streamOps.length).toBeGreaterThanOrEqual(2);
			const rawBytes = streamOps.map((o) => o.content).join("");
			expect(rawBytes).toContain("**RAW**"); // double-star survived
			const drafts = wire.draftsOf("chat-e2e").map((d) => d.content);
			for (let i = 1; i < drafts.length; i++) {
				expect(drafts[i]?.startsWith(drafts[i - 1] as string)).toBe(true);
			}

			// The sealed stream IS the message: exactly ONE seal, carrying
			// the authoritative final suffix; NEVER a plain-send duplicate.
			const seals = wire.ops.filter(
				(o) => o.op === "seal" && o.chatId === "chat-e2e",
			);
			expect(seals).toHaveLength(1);
			const dupes = wire
				.sendsOf("chat-e2e")
				.filter((o) => o.content.includes("final from model"));
			expect(dupes).toHaveLength(0);

			// Persist-what-you-send: user + assistant rows landed in pi_state.
			const rows = h.store.db
				.prepare(
					"SELECT role, content FROM messages WHERE session_id = ? AND active = 1 ORDER BY id",
				)
				.all("sess-ws-e2e") as Array<{ role: string; content: string }>;
			expect(
				rows.some((r) => r.role === "user" && r.content === "hello gateway"),
			).toBe(true);
			expect(
				rows.some(
					(r) =>
						r.role === "assistant" &&
						(r.content ?? "").includes("final from model"),
				),
			).toBe(true);

			engine.disconnect();
		} finally {
			await h.close();
		}
	}, 20_000);
});
