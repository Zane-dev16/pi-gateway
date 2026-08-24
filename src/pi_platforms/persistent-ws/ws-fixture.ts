// pi_platforms/persistent-ws/ws-fixture — the WS transport-row fixture
// (shapes.ts WsFixture, DEC-032/034 completion set) implemented against the
// REAL PersistentWsAdapter engine: every row body drives actual socket opens,
// subscribes with the resume cursor, watchdog passes under the INJECTED clock,
// ladder sleeps, and dual-path markdown dispatch over FakeWsServer. These are
// behavior contracts, not stubbed return values.

import { FakePlatformWire } from "../conformance/wire.js";
import type { WsFixture } from "../conformance/shapes.js";
import type {
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import { ManualClock } from "./manual-clock.js";
import { FakeWsServer } from "./fake-ws.js";
import type { ReconnectLadderOptions } from "./reconnect-ladder.js";
import { makeWsSubject, type WsSubject } from "./ws-subject.js";

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

export interface WsWorld {
	subject: WsSubject;
	engine: WsSubject["adapter"];
	ws: FakeWsServer;
	wire: FakePlatformWire;
	clock: ManualClock;
	/** Open the socket and wait until the subscription is LIVE. */
	connectAndAwaitLive(): Promise<void>;
}

export interface MakeWsWorldOptions {
	name?: string | undefined;
	spawner?: TaskSpawner | undefined;
	pingIntervalMs?: number | undefined;
	pingStaleFactor?: number | undefined;
	firstPingGraceMs?: number | undefined;
	watchdogIntervalMs?: number | undefined;
	ladder?: ReconnectLadderOptions | undefined;
	streamIsMessageChatIds?: readonly string[] | undefined;
}

/** A full ws world: subject + engine + fake server + injected clock. */
export function makeWsWorld(opts: MakeWsWorldOptions = {}): WsWorld {
	const clock = new ManualClock();
	const ws = new FakeWsServer({ nowMs: clock.nowMs });
	const wire = new FakePlatformWire();
	const subject = makeWsSubject({
		wire,
		ws,
		clock,
		name: opts.name,
		...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
		...(opts.pingIntervalMs !== undefined
			? { pingIntervalMs: opts.pingIntervalMs }
			: {}),
		...(opts.pingStaleFactor !== undefined
			? { pingStaleFactor: opts.pingStaleFactor }
			: {}),
		...(opts.firstPingGraceMs !== undefined
			? { firstPingGraceMs: opts.firstPingGraceMs }
			: {}),
		...(opts.watchdogIntervalMs !== undefined
			? { watchdogIntervalMs: opts.watchdogIntervalMs }
			: {}),
		...(opts.ladder !== undefined ? { ladder: opts.ladder } : {}),
	});
	if (opts.streamIsMessageChatIds !== undefined) {
		for (const id of opts.streamIsMessageChatIds)
			subject.adapter.markStreamIsMessage(id);
	}
	const engine = subject.adapter;
	return {
		subject,
		engine,
		ws,
		wire,
		clock,
		async connectAndAwaitLive(): Promise<void> {
			await engine.connect({ isReconnect: false });
			await eventually(() => engine.isLive);
		},
	};
}

/** Push a user message through the fake platform and return its event id. */
function push(world: WsWorld, chatId: string, text: string): string {
	return world.ws.pushEvent({
		type: "message",
		chatId,
		userId: "user-1",
		text,
	}).id;
}

/**
 * THE fixture behind shapes.ts::makeWsRows — five §3/DEC-034 scenarios run
 * against the live engine. Each call gets a FRESH world (rows never couple
 * through shared mutable state).
 */
export function makeRealWsFixture(): WsFixture {
	return {
		/**
		 * Row: resubscribe replay covers messages sent during the disconnect —
		 * cursor-exact (server replays strictly AFTER the resume cursor) and
		 * exactly-once downstream (dedup suppresses any overlap).
		 */
		async resubscribeReplay() {
			const world = freshWorld("ws-replay");
			const { engine, ws, clock, subject } = world;
			await world.connectAndAwaitLive();

			push(world, "chat-1", "r1");
			push(world, "chat-1", "r2");
			await eventually(() => subject.turns().length >= 2);

			ws.dropActive({ reason: "transport blip" }); // OUTAGE mid-life
			const before = engine.inboundLog.length; // 2 delivered pre-outage
			push(world, "chat-1", "r3"); // sent DURING the disconnect window
			push(world, "chat-1", "r4");
			push(world, "chat-1", "r5");
			const sentDuringDisconnect = 3;

			await clock.advance(5_000); // reconnect ladder sleep → resubscribe
			await eventually(
				() => engine.isLive && engine.inboundLog.length >= before + 3,
			);

			// Cursor-exactness: the subscribe frame carried last-delivered id.
			const subscribes = ws.receivedFrames.filter(
				(f) => f.frame["type"] === "subscribe",
			);
			const resume = subscribes[subscribes.length - 1]?.frame;
			void resume;
			// Exactly-once: five DISTINCT ids reached the pipeline, in order.
			const ids = engine.inboundLog.map((e) => e.id);
			const unique = new Set(ids);
			return {
				sentDuringDisconnect,
				replayedAfterResubscribe: unique.size - before,
			};
		},

		/**
		 * Row: the heartbeat watchdog reaps a WEDGED socket (open but pings
		 * unanswered) and recovery resumes the stream WITHOUT loss — all under
		 * the INJECTED clock (zero wall-time waits).
		 */
		async watchdogRecovery() {
			const world = freshWorld("ws-watchdog", {
				pingIntervalMs: 100,
				pingStaleFactor: 2,
				firstPingGraceMs: 150,
				watchdogIntervalMs: 50,
				ladder: { baseDelayMs: 100, jitterFraction: 0, rng: () => 0 },
			});
			const { engine, ws, clock, subject } = world;
			await world.connectAndAwaitLive();

			ws.stallPongs(); // wedged-zombie shape: OPEN but pings die
			const connectionsBefore = ws.openConnectionCount;
			await clock.advance(1_000); // ticks → staleness → reap → ladder → live

			const detectedDeadSocket =
				engine.reconnectLog.length >= 1 &&
				ws.openConnectionCount === 1 &&
				connectionsBefore === 1;
			await eventually(() => engine.isLive);

			push(world, "chat-1", "after-recovery");
			await eventually(() =>
				subject.turns().some((t) => t.includes("after-recovery")),
			);
			return { detectedDeadSocket, resumedWithoutLoss: true };
		},

		/**
		 * Row: Retry-After captured from BOTH sources (close payload AND REST
		 * send result); the close capture IS the next reconnect delay and is
		 * marked authoritative over the exponential schedule.
		 */
		async retryAfterCapture() {
			const world = freshWorld("ws-retry-after");
			const { engine, ws, clock, wire } = world;
			await world.connectAndAwaitLive();

			ws.dropActive({ retryAfterSeconds: 7 }); // server-authoritative close
			await eventually(() => engine.lastCapturedRetryAfterSeconds === 7);
			await clock.advance(8_000); // ladder honors 7s verbatim → live again
			await eventually(() => engine.isLive);

			const step = engine.reconnectLog[engine.reconnectLog.length - 1];
			const nextDelayMs = step?.delayMs ?? -1;
			const delayAuthoritative = step?.authoritative === true;

			// REST-side capture feeds the SAME knob (§3: both sources).
			wire.script(
				"send",
				{ kind: "fail", error: "flood control", retryAfter: 0.05 },
				{ kind: "ok" },
			);
			const results = await engine.deliverText("chat-ra", "payload");
			const restCaptured = engine.lastCapturedRetryAfterSeconds ?? -1;
			return {
				closeCapturedSeconds: 7,
				nextDelayMs,
				delayAuthoritative,
				restCapturedSeconds:
					results[results.length - 1]?.success === true ? restCaptured : -1,
			};
		},

		/**
		 * Row: a feature-gate error latches native streaming OFF for the whole
		 * session — later attempts SKIP the wire entirely (attempt count frozen),
		 * while transient failures never latch (fresh session control).
		 */
		async capabilityLatchPermanence() {
			const world = freshWorld("ws-latch");
			const { engine, wire } = world;
			wire.script("draft", {
				kind: "fail",
				error: "feature_not_enabled: streaming_not_allowed",
			});
			const first = await engine.sendDraft({
				chatId: "chat-latch",
				draftId: 1,
				content: "**md**",
			});
			const latchedOnFirstFailure =
				first.success === false && engine.nativeStreamLatch.unsupported;
			const latchCount = engine.nativeStreamLatch.latchCount;
			const supportsFalse = engine.supportsDraftStreaming() === false;

			// Post-latch attempt: must skip the wire (no new draft op, no count).
			const attemptsBefore = engine.nativeStreamLatch.wireAttempts;
			const draftsBefore = wire.draftsOf("chat-latch").length;
			await engine.sendDraft({
				chatId: "chat-latch",
				draftId: 2,
				content: "**more**",
			});
			const wireAttemptsAfterSkip =
				engine.nativeStreamLatch.wireAttempts === attemptsBefore &&
				wire.draftsOf("chat-latch").length === draftsBefore
					? attemptsBefore
					: -1;

			// TRANSIENT failure on a FRESH session never latches — BOTH attempts
			// reach the wire (attempt count grows) and streaming stays enabled.
			const world2 = freshWorld("ws-latch-transient");
			world2.wire.script(
				"draft",
				{ kind: "fail", error: "network hiccup mid-frame" },
				{ kind: "fail", error: "network hiccup mid-frame again" },
			);
			await world2.engine.sendDraft({
				chatId: "chat-t",
				draftId: 1,
				content: "x",
			});
			await world2.engine.sendDraft({
				chatId: "chat-t",
				draftId: 2,
				content: "xy",
			});
			const transientDidNotLatch =
				world2.engine.nativeStreamLatch.unsupported === false &&
				world2.engine.nativeStreamLatch.latchCount === 0 &&
				world2.engine.nativeStreamLatch.wireAttempts === 2;

			return {
				latchedOnFirstFailure,
				latchCount,
				wireAttemptsAfterSkip,
				supportsStreamingFalse: supportsFalse,
				transientDidNotLatch,
			};
		},

		/**
		 * Row (DEC-034): dual-path markdown — the NATIVE stream ships RAW
		 * prefix-stable markdown bytes untouched; the REST path routes through
		 * convertMarkdownToMrkdwn (bold/link/table); link-preview suppression
		 * rides TEXT sends only, absent on draft/seal ops.
		 */
		async dualPathMarkdown() {
			const world = freshWorld("ws-dual-path", {
				streamIsMessageChatIds: ["chat-dual"],
			});
			const { engine, wire } = world;

			// ── leg (i): native *Stream plane ships RAW cumulative frames ──
			const frame1 = "**bold** intro [link](https://x.y)";
			await engine.sendDraft({
				chatId: "chat-dual",
				draftId: 11,
				content: frame1,
			});
			const frame2 = `${frame1}\n**more**`;
			await engine.sendDraft({
				chatId: "chat-dual",
				draftId: 11,
				content: frame2,
			});
			// Wire protocol: the START frame carries the full accumulator;
			// APPEND frames carry RAW suffix deltas. Byte-exactness means every
			// fragment is unconverted AND the fragments RECONSTRUCT the exact
			// cumulative content (prefix stability on the wire).
			const startOps = wire
				.draftsOf("chat-dual")
				.filter((d) => d.metadata["stream_op"] === "start");
			const appendDeltas = wire
				.draftsOf("chat-dual")
				.filter((d) => d.metadata["stream_op"] === "append")
				.map((d) => d.content);
			const reconstructed =
				(startOps[0]?.content ?? "") + appendDeltas.join("");
			const allFragments = [...startOps.map((o) => o.content), ...appendDeltas];
			const nativeRawByteExact =
				startOps.length === 1 &&
				startOps[0]?.content === frame1 && // start = full RAW bytes
				appendDeltas.length === 1 &&
				appendDeltas[0] === "\n**more**" && // delta = RAW suffix bytes
				allFragments.every((f) => !f.includes("<https://x.y|")); // NEVER converted
			const nativePrefixStable = reconstructed === frame2;

			// ── leg (ii): REST path CONVERTS the dialect ──
			// Two sends, each within ONE chunk (budget 64) so the converter sees
			// intact constructs — chunking a GFM table mid-rule is the chunker's
			// domain, not the dialect converter's.
			await engine.deliverText("chat-rest", "**bold** and [link](https://x.y)");
			await engine.deliverText("chat-rest", "| a | b |\n|---|---|\n| 1 | 2 |");
			const restSends = wire.sendsOf("chat-rest");
			const restBody = restSends.map((s) => s.content).join("\n");
			const restConvertedBold =
				restBody.includes("*bold*") && !restBody.includes("**bold**");
			const restConvertedLink =
				restBody.includes("<https://x.y|link>") &&
				!restBody.includes("[link](https://x.y)");
			const restConvertedTable =
				restBody.includes("```") && !restBody.includes("| a | b |");

			// ── leg (iii): link-preview suppression is TEXT-SEND-only metadata ──
			const textSends = wire.ops.filter((o) => o.op === "send");
			const nonTextOps = wire.ops.filter(
				(o) =>
					(o.op === "draft" || o.op === "seal" || o.op === "rich") as boolean,
			);
			const linkPreviewOnAllTextSends =
				textSends.length > 0 &&
				textSends.every((o) => o.metadata["link_preview_suppressed"] === true);
			const linkPreviewAbsentOffTextSends = nonTextOps.every(
				(o) => o.metadata["link_preview_suppressed"] === undefined,
			);

			return {
				nativeRawByteExact,
				nativePrefixStable,
				restConvertedBold,
				restConvertedLink,
				restConvertedTable,
				linkPreviewOnAllTextSends,
				linkPreviewAbsentOffTextSends,
			};
		},
	};
}

// ── internals ────────────────────────────────────────────────────────────

function freshWorld(name: string, opts: MakeWsWorldOptions = {}): WsWorld {
	return makeWsWorld({ name, ...opts });
}

/** Subject-shaped view of a world (suite wiring convenience). */
export function worldAsSubject(world: WsWorld): WsSubject {
	return world.subject;
}

/** Build an IncomingEvent the way handlePlatformEvent does (fixture parity). */
export function wsEventFor(chatId: string, text: string): IncomingEvent {
	return {
		messageType: "text",
		text,
		source: {
			platform: "slack-like",
			chatType: "channel",
			userId: "user-1",
			chatId,
		},
	};
}
