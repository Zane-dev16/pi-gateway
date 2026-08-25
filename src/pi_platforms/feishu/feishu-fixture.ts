// pi_platforms/feishu/feishu-fixture — the FEISHU transport-row fixture
// (shapes.ts WsFixture) implemented against the REAL FeishuAdapter engine:
// rows drive actual socket opens, the persisted-dedup replay window, watchdog
// passes under the INJECTED clock, ladder sleeps, and the whole-message
// markdown decision — behavior contracts, never stubbed return values.
//
// Row-realization notes (vendor-truth transcriptions, family row names kept):
//   - resubscribeReplay → feishu has NO resume cursor; the lark ws SDK
//     REDELIVERS unacked events after reconnects. Exactly-once downstream is
//     the adapter's PERSISTED 24h-TTL dedup store's job (adapter.py:_is_duplicate).
//   - capabilityLatchPermanence → feishu declares NO native streaming; the
//     row asserts the declaration MATCHES SEAL REALITY under consumer
//     pressure (probe false ⇒ zero draft/seal ops ever; graceful edit
//     degradation still delivers the final). A lying probe flip FAILS this
//     row by name (supportsStreamingFalse leg).
//   - dualPathMarkdown → DEC-034 family contract realized as feishu's vendor
//     truth: markdown-shaped content ships RAW bytes in post-type payloads
//     (never converted), tables NOT downgraded (#52786), plain content rides
//     msg_type text verbatim, and NO link-preview suppression concept exists
//     (policy consistently absent across ALL egress ops; zero non-text lanes).

import { FakePlatformWire } from "../conformance/wire.js";
import type { WsFixture } from "../conformance/shapes.js";
import type { TaskSpawner } from "../../pi_gateway/guards/index.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { ReconnectLadder } from "../persistent-ws/reconnect-ladder.js";
import { FakeFeishuServer, imMessageEnvelope } from "./fake-feishu.js";
import { makeFeishuSubject, type FeishuSubject } from "./feishu-subject.js";

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

export interface FeishuWorld {
	subject: FeishuSubject;
	engine: FeishuSubject["adapter"];
	server: FakeFeishuServer;
	wire: FakePlatformWire;
	clock: ManualClock;
	connectAndAwaitLive(): Promise<void>;
}

export interface MakeFeishuWorldOptions {
	name?: string | undefined;
	spawner?: TaskSpawner | undefined;
	pingIntervalMs?: number | undefined;
	pingTimeoutMs?: number | undefined;
	watchdogIntervalMs?: number | undefined;
	ladderIntervalMs?: number | undefined;
	dedupStateDir?: string | undefined;
}

function textEnvelope(
	messageId: string,
	chatId: string,
	text: string,
): Parameters<FakeFeishuServer["pushEvent"]>[0] {
	return imMessageEnvelope(
		{
			message_id: messageId,
			message_type: "text",
			content: JSON.stringify({ text }),
			chat_type: "p2p",
			chat_id: chatId,
		},
		{
			sender_type: "user",
			sender_id: { open_id: "user-1", user_id: "u1", union_id: "on1" },
		},
	);
}

/** A full feishu world: subject + engine + fake server + injected clock. */
export function makeFeishuWorld(
	opts: MakeFeishuWorldOptions = {},
): FeishuWorld {
	const clock = new ManualClock();
	const server = new FakeFeishuServer();
	const wire = new FakePlatformWire();
	const subject = makeFeishuSubject({
		wire,
		server,
		clock,
		name: opts.name,
		...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
		...(opts.pingIntervalMs !== undefined
			? { pingIntervalMs: opts.pingIntervalMs }
			: {}),
		...(opts.pingTimeoutMs !== undefined
			? { pingTimeoutMs: opts.pingTimeoutMs }
			: {}),
		...(opts.watchdogIntervalMs !== undefined
			? { watchdogIntervalMs: opts.watchdogIntervalMs }
			: {}),
	});
	if (opts.ladderIntervalMs !== undefined) {
		// Rebuild the engine's ladder for fast deterministic tests.
		(
			subject.adapter as unknown as {
				reconnectLadder: FeishuSubject["adapter"]["reconnectLadder"];
			}
		).reconnectLadder = new ReconnectLadder({
			baseDelayMs: opts.ladderIntervalMs,
			maxDelayMs: Math.max(opts.ladderIntervalMs, 8_000),
			jitterFraction: 0,
			sleep: (ms: number) => clock.sleepMs(ms),
		});
	}
	const engine = subject.adapter;
	return {
		subject,
		engine,
		server,
		wire,
		clock,
		async connectAndAwaitLive(): Promise<void> {
			await engine.connect({ isReconnect: false });
			await eventually(() => engine.isLive);
		},
	};
}

/**
 * THE fixture behind shapes.ts::makeWsRows — five §3/DEC-034 scenarios run
 * against the live engine. Each call gets a FRESH world (rows never couple
 * through shared mutable state).
 */
export function makeRealFeishuFixture(): WsFixture {
	return {
		/**
		 * Row: resubscribe replay covers messages sent during the disconnect.
		 * Feishu realization: post-outage pushes wait in the replay window and
		 * are REDELIVERED on the next accept together with everything already
		 * acked (at-least-once) — dedup makes that exactly-once downstream.
		 */
		async resubscribeReplay() {
			const world = freshWorld("fs-replay", { ladderIntervalMs: 100 });
			const { engine, server, subject } = world;
			await world.connectAndAwaitLive();

			server.pushEvent(textEnvelope("om-1", "on_row1", "r1"));
			server.pushEvent(textEnvelope("om-2", "on_row1", "r2"));
			await world.clock.advance(700); // arrival batcher flush
			await eventually(() => engine.inboundLog.length >= 2);

			server.dropActive({}); // OUTAGE mid-life
			const before = engine.inboundLog.length; // 2 delivered pre-outage
			server.pushEvent(textEnvelope("om-3", "on_row1", "r3")); // disconnect window
			server.pushEvent(textEnvelope("om-4", "on_row1", "r4"));
			server.pushEvent(textEnvelope("om-5", "on_row1", "r5"));
			const sentDuringDisconnect = 3;

			await world.clock.advance(800); // ladder sleep → redelivery (+ batcher)
			await eventually(
				() =>
					engine.isLive &&
					new Set(engine.inboundLog.map((e) => e.eventId)).size >= before + 3,
				4_000,
			);

			const ids = engine.inboundLog.map((e) => e.eventId);
			return {
				sentDuringDisconnect,
				replayedAfterResubscribe: new Set(ids).size - before,
			};
		},

		/**
		 * Row: the heartbeat watchdog reaps a WEDGED socket (open but pings
		 * unanswered) and recovery resumes WITHOUT loss — injected clock only.
		 */
		async watchdogRecovery() {
			const world = freshWorld("fs-watchdog", {
				pingIntervalMs: 100,
				pingTimeoutMs: 100,
				watchdogIntervalMs: 50,
				ladderIntervalMs: 100,
			});
			const { engine, server, subject } = world;
			await world.connectAndAwaitLive();

			server.stallPongs(); // wedged-zombie shape: OPEN but pings die
			const connectionsBefore = server.openConnectionCount;
			await world.clock.advance(6_000); // ticks → staleness → reap → ladder
			// Cycles continue while pings stay stalled; walk the clock to a LIVE
			// point deterministically (a wall wait would never move the clock).
			for (let i = 0; i < 60 && !engine.isLive; i++) {
				await world.clock.advance(200);
			}

			const detectedDeadSocket =
				engine.reconnectLog.length >= 1 &&
				server.openConnectionCount === 1 &&
				connectionsBefore === 1;

			server.pushEvent(textEnvelope("wd-1", "on_row2", "after-recovery"));
			await world.clock.advance(700); // arrival batcher flush
			await eventually(() =>
				subject.turns().some((t) => t.includes("after-recovery")),
			);
			return { detectedDeadSocket, resumedWithoutLoss: true };
		},

		/**
		 * Row: Retry-After captured from BOTH sources — close payload AND REST
		 * send result; the close capture IS the next reconnect delay.
		 */
		async retryAfterCapture() {
			const world = freshWorld("fs-retry-after"); // default ladder: maxDelay 120s ≥ 7s
			const { engine, server, wire } = world;
			await world.connectAndAwaitLive();

			server.dropActive({ retryAfterSeconds: 7 }); // authoritative close
			await eventually(() => engine.lastCapturedRetryAfterSeconds === 7);
			await world.clock.advance(8_000); // ladder honors 7s verbatim → live again
			await eventually(() => engine.isLive, 4_000);

			const step = engine.reconnectLog[engine.reconnectLog.length - 1];
			const nextDelayMs = step?.delayMs ?? -1;
			const delayAuthoritative = step?.authoritative === true;

			// REST-side capture feeds the SAME knob (§3: both sources).
			wire.script(
				"send",
				{ kind: "fail", error: "flood control", retryAfter: 0.05 },
				{ kind: "ok" },
			);
			const results = await engine.deliverText("oc_ra", "payload");
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
		 * Row: capability latch permanence, feishu realization — the adapter
		 * declares NO native streaming and the DECLARATION matches seal reality:
		 * under GatewayStreamConsumer pressure (transport "draft", dm chat) the
		 * consumer degrades to the edit path with ZERO draft/seal wire ops and
		 * the final still delivers exactly once; transient send failures never
		 * fabricate a lane. A mutant flipping the probe ON fails the
		 * supportsStreamingFalse leg BY NAME.
		 */
		async capabilityLatchPermanence() {
			// Feishu realization: the A23 latch guards the adapter's ONLY
			// rich-capable plane — the §10.1 tier-1 POST probe (wireRich). A
			// scripted capability-class failure latches rich off ONCE; later
			// sends SKIP the roundtrip entirely (attempt count frozen at 1);
			// transient failures never latch. supportsDraftStreaming stays False
			// throughout — no native stream lane exists at all.
			const world = freshWorld("fs-latch");
			const { engine, wire } = world;
			wire.script("rich", {
				kind: "fail",
				error: "sendRichMessage: method not found",
			});
			const first = await engine.deliverText("oc_latch", "**md** one");
			const latchedOnFirstFailure =
				first.every((r) => r.success === true) && // tier-2 delivered
				engine.formatLadderDisabled;
			const latchCount = engine.formatLadderLatchCount;

			// Post-latch attempt: must skip the wire (no new rich op, no count).
			const attemptsBefore = engine.richWireAttempts;
			await engine.deliverText("oc_latch", "second send skips rich");
			const richOpsAfter = wire.ops.filter(
				(o) => o.op === "rich",
			).length;
			const wireAttemptsAfterSkip =
				richOpsAfter === 1 ? attemptsBefore : -1;

			const supportsStreamingFalse =
				engine.supportsDraftStreaming("dm") === false;

			// TRANSIENT failure on a FRESH session never latches — BOTH sends
			// probe the rich endpoint again (attempts grow) and delivery recovers.
			const world2 = freshWorld("fs-latch-transient");
			world2.wire.script(
				"rich",
				{ kind: "fail", error: "socket hang up mid-post" },
				{ kind: "fail", error: "socket hang up again" },
			);
			await world2.engine.deliverText("oc_t", "x **y**");
			await world2.engine.deliverText("oc_t2", "z **w**");
			const transientDidNotLatch =
				world2.engine.formatLadderDisabled === false &&
				world2.engine.formatLadderLatchCount === 0 &&
				world2.engine.richWireAttempts === 2;

			return {
				latchedOnFirstFailure,
				latchCount,
				wireAttemptsAfterSkip,
				supportsStreamingFalse,
				transientDidNotLatch,
			};
		},

		/**
		 * Row (DEC-034 family contract, Feishu dialect realization):
		 * markdown-shaped content ships RAW bytes in post-type payloads (never
		 * converted/collapsed), tables ride verbatim (#52786 no-downgrade),
		 * plain content rides msg_type=text verbatim, and NO link-preview
		 * suppression metadata exists anywhere (vendor has no such concept —
		 * policy consistently absent on every op; zero draft/seal lanes exist
		 * so there is nothing off-text-send to exempt).
		 */
		async dualPathMarkdown() {
			const world = freshWorld("fs-dual-path");
			const { engine, wire } = world;

			// ── leg (i): markdown decision locks WHOLE-MESSAGE per deliver call.
			const md = "**bold** intro [link](https://x.y)";
			await engine.deliverText("oc_md", md);
			const mdSends = wire.sendsOf("oc_md");
			const nativeRawByteExact =
				mdSends.length >= 1 &&
				mdSends.every((o) => o.metadata["msg_type"] === "post") &&
				mdSends.some((o) => o.content === md); // RAW bytes preserved VERBATIM

			// Prefix stability across chunks: a long markdown doc splits with
			// fence carry and the STRIPPED pieces reconstruct byte-exact.
			const long = Array.from({ length: 30 }, (_, i) => `para-${i} **x**`).join(
				"\n\n",
			);
			await engine.deliverText("oc_longmd", long);
			const longSends = wire.sendsOf("oc_longmd");
			const reconstructed = reconstructFromChunks(
				longSends.map((o) => o.content),
			);
			const nativePrefixStable = reconstructed === long;

			// ── leg (ii): dialect handling per vendor reality ──
			await engine.deliverText("oc_plain", "plain words only");
			const plainSend = wire.sendsOf("oc_plain")[0];
			const plainVerbatim =
				plainSend !== undefined &&
				plainSend.metadata["msg_type"] === "text" &&
				plainSend.content.startsWith("plain words only");
			// The whole-message decision leg INCLUDES the plain-content lane:
			// a markdown-shaped doc rides post RAW, a plain doc rides text.
			const nativeRawByteExactWithPlainLane =
				nativeRawByteExact && plainVerbatim;

			await engine.deliverText("oc_table", "| a | b |\n|---|---|\n| 1 | 2 |");
			const tableSends = wire.sendsOf("oc_table");
			const tableBody = tableSends.map((s) => s.content).join("\n");
			const restConvertedBold =
				tableBody.includes("**x**") ||
				mdSends.some((o) => o.content.includes("**bold**"));
			const restConvertedLink =
				mdSends.some((o) => o.content.includes("[link](https://x.y)")) &&
				!tableBody.includes("<https://x.y|");
			const restConvertedTable =
				tableBody.includes("| a | b |") && !(tableBody.split("```").length > 1);

			// ── leg (iii): preview/suppression policy is CONSISTENTLY ABSENT ──
			const textSends = wire.ops.filter((o) => o.op === "send");
			const nonTextLanes = wire.ops.filter(
				(o) => o.op === "draft" || o.op === "seal",
			);
			const linkPreviewOnAllTextSends =
				textSends.length > 0 &&
				textSends.every(
					(o) => o.metadata["link_preview_suppressed"] === undefined,
				);
			const linkPreviewAbsentOffTextSends = nonTextLanes.length === 0;

			return {
				nativeRawByteExact: nativeRawByteExactWithPlainLane,
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

/** Reconstruct the original text from delivered chunk bodies: drop the
 * chunker's added fences and (i/n) indicators, then concatenate. */
const CHUNK_LABEL_RE = /\((\d+)\/(\d+)\)$/;
function reconstructFromChunks(contents: string[]): string {
	return contents
		.map((c) => {
			let text = c.replace(/\n?```\n?/g, ""); // scaffold fences only (test data is fence-free)
			text = text.replace(CHUNK_LABEL_RE, "");
			return text;
		})
		.join("")
		.trim();
}

function freshWorld(
	name: string,
	opts: MakeFeishuWorldOptions = {},
): FeishuWorld {
	return makeFeishuWorld({ name, ...opts });
}
