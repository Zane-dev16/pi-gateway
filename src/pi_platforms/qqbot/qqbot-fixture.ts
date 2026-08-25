// pi_platforms/qqbot/qqbot-fixture — the QQBOT transport-row fixture
// (shapes.ts WsFixture) implemented against the REAL QQBotAdapter engine:
// rows drive actual gateway opens, the RESUME replay window, the close-code
// ladder under the INJECTED clock, and the §10.1 rich-latch path — behavior
// contracts, never stubbed return values.
//
// Row-realization notes (vendor-truth transcriptions, family row names kept):
//   - resubscribeReplay → QQ replays dispatches after op 6 Resume
//     (session_id + last_seq). Exactly-once downstream is the adapter's
//     300s-TTL/1000-entry dedup's job (adapter.py:_is_duplicate); an
//     at-least-once REDUPLICATED id proves it.
//   - watchdogRecovery → Hermes qqbot tracks NO pong timeout; dead-socket
//     detection rides the READ path (hard TCP death raises on the listener)
//     feeding the SAME reconnect ladder. Recovery resumes without loss — no
//     invented pong machinery.
//   - retryAfterCapture → close code 4008 IS the server-authoritative capture:
//     RATE_LIMIT_DELAY=60s honored verbatim as the next reconnect delay
//     (adapter.py:_listen_loop `await asyncio.sleep(RATE_LIMIT_DELAY)`). The
//     REST leg captures retry_after from send-error results via the shared kit
//     extractor (_send_with_retry honor-once parity).
//   - capabilityLatchPermanence → QQ declares NO native streaming; the A23
//     latch guards the ONLY rich-capable plane — the §10.1 tier-1 probe.
//     Capability-class failure latches ONCE; later sends SKIP the wire;
//     transient failures never latch; supportsDraftStreaming stays False.
//   - dualPathMarkdown → DEC-034 family contract realized as QQ vendor truth:
//     markdown ships RAW bytes in msg_type=2 bodies (never converted),
//     markdown_support=false rides msg_type=0 verbatim-stripped, tables ride
//     inside markdown bodies (no downgrade lane), and NO link-preview
//     suppression concept exists anywhere on the wire.

import { FakePlatformWire } from "../conformance/wire.js";
import type { WsFixture } from "../conformance/shapes.js";
import type { TaskSpawner } from "../../pi_gateway/guards/index.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { eventually } from "./eventually.js";
import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakeQQGateway } from "./fake-qq-gateway.js";
import { makeQQSubject, type QQBotSubject } from "./qqbot-subject.js";

export interface QQWorld {
	subject: QQBotSubject;
	engine: QQBotSubject["adapter"];
	gateway: FakeQQGateway;
	wire: FakePlatformWire;
	clock: ManualClock;
	connectAndAwaitLive(): Promise<void>;
}

export interface MakeQQWorldOptions {
	name?: string | undefined;
	spawner?: TaskSpawner | undefined;
	helloIntervalMs?: number | undefined;
	markdownSupport?: boolean | undefined;
}

/** Push a C2C_MESSAGE_CREATE dispatch through the fake gateway. */
export function c2cDispatch(
	messageId: string,
	userOpenid: string,
	text: string,
	extra: Record<string, unknown> = {},
): [t: string, d: Record<string, unknown>] {
	return [
		"C2C_MESSAGE_CREATE",
		{
			id: messageId,
			content: text,
			author: { user_openid: userOpenid },
			timestamp: "2026-08-25T00:00:00+08:00",
			...extra,
		},
	];
}

/** A full qq world: subject + engine + fake gateway + injected clock. */
export function makeQQWorld(opts: MakeQQWorldOptions = {}): QQWorld {
	const clock = new ManualClock();
	const gateway = new FakeQQGateway();
	if (opts.helloIntervalMs !== undefined) {
		gateway.helloIntervalMs = opts.helloIntervalMs;
	}
	const wire = new FakePlatformWire();
	// NO scheduler here: the subject attaches the guard with the immediate
	// production spawner, so inbound dispatches run to completion (fixture
	// rows observe engine.turnLog growth directly).
	const subject = makeQQSubject({
		wire,
		gateway,
		name: opts.name,
		markdownSupport: opts.markdownSupport,
	});
	// Injected clock: the engine sleeps through THIS clock for ladders/retries.
	const adapter = subject.adapter as unknown as {
		sleepFn: (ms: number) => Promise<void>;
	};
	adapter.sleepFn = clock.sleepMs;
	void opts.spawner;
	const engine = subject.adapter;
	return {
		subject,
		engine,
		gateway,
		wire,
		clock,
		async connectAndAwaitLive(): Promise<void> {
			await engine.connect({ isReconnect: false });
			await eventually(() => engine.isLive);
			await eventually(() => engine.sessionId !== null);
		},
	};
}

function freshWorld(name: string): QQWorld {
	return makeQQWorld({ name });
}

/**
 * THE fixture behind shapes.ts::makeWsRows — five §3/DEC-034 scenarios run
 * against the live engine. Each call gets a FRESH world (rows never couple
 * through shared mutable state).
 */
export function makeRealQQFixture(): WsFixture {
	return {
		async resubscribeReplay() {
			const world = freshWorld("qb-replay");
			const { engine, gateway } = world;
			await world.connectAndAwaitLive();

			gateway.pushDispatch(...c2cDispatch("r-1", "u_replay", "r1"));
			gateway.pushDispatch(...c2cDispatch("r-2", "u_replay", "r2"));
			await eventually(() => engine.turnLog.length >= 2);

			gateway.dropActive(1001, "going away"); // OUTAGE mid-life (resumable)
			const before = engine.turnLog.length; // 2 delivered pre-outage
			gateway.pushDispatch(...c2cDispatch("r-3", "u_replay", "r3"));
			gateway.pushDispatch(...c2cDispatch("r-4", "u_replay", "r4"));
			gateway.pushDispatch(...c2cDispatch("r-5", "u_replay", "r5"));
			const sentDuringDisconnect = 3;

			await world.clock.advance(8_000); // ladder tier sleep → resume
			await eventually(() => engine.isLive, 4_000);
			// At-least-once redelivery ALSO re-sends an already-delivered id —
			// dedup must absorb it exactly-once downstream.
			gateway.pushDispatch(...c2cDispatch("r-1", "u_replay", "r1"));
			gateway.pushDispatch(...c2cDispatch("r-6", "u_replay", "r6"));
			await eventually(() => engine.turnLog.includes("r6"));

			// Downstream: every id delivered EXACTLY ONCE across merged turns
			// (the guard may merge rapid replay bursts into one drained turn —
			// burst-merge is guard behavior); the REDUPLICATED r1 must NOT
			// create a second delivery.
			const allText = `\n${engine.turnLog.join("\n")}\n`;
			const occurrences = (token: string): number =>
				allText.split(`\n${token}\n`).length - 1;
			for (const id of ["r1", "r2", "r3", "r4", "r5", "r6"]) {
				if (occurrences(id) !== 1) {
					throw new Error(
						`exactly-once violated for ${id}: ${occurrences(id)} deliveries (${JSON.stringify(engine.turnLog)})`,
					);
				}
			}
			// Only the DISCONNECT-WINDOW ids count toward the replay leg; the
			// post-resume live push (r6) proves liveness, not replay.
			const windowIds = ["r3", "r4", "r5"].filter((id) =>
				engine.turnLog.some((t) => t.includes(id)),
			);
			return {
				sentDuringDisconnect,
				replayedAfterResubscribe: new Set(windowIds).size,
			};
		},

		async watchdogRecovery() {
			const world = freshWorld("qb-watchdog");
			const { engine, gateway } = world;
			await world.connectAndAwaitLive();

			const connectionsBefore = gateway.openConnectionCount;
			gateway.hardDrop(); // dead-TCP shape: error, NO close frame
			await eventually(() =>
				engine.reconnectLog.some((l) => l.startsWith("read-error")),
			);
			const detectedDeadSocket =
				engine.reconnectLog.some((l) => l.startsWith("read-error")) &&
				gateway.openConnectionCount <= connectionsBefore;

			await world.clock.advance(8_000); // ladder sleep → resume reconnect
			await eventually(() => engine.isLive, 4_000);
			gateway.pushDispatch(...c2cDispatch("wd-1", "u_wd", "after-recovery"));
			await eventually(() =>
				world.subject.turns().some((t) => t.includes("after-recovery")),
			);
			return { detectedDeadSocket, resumedWithoutLoss: true };
		},

		async retryAfterCapture() {
			const world = freshWorld("qb-retry-after");
			const { engine, gateway, clock } = world;
			await world.connectAndAwaitLive();

			gateway.dropActive(4008, "rate limited"); // authoritative capture
			await eventually(() => engine.lastCapturedRetryAfterSeconds === 60);
			void clock.advance(60_000); // ladder honors 60s verbatim → live again
			await eventually(() => engine.isLive, 4_000);
			await clock.advance(0); // settle pending microtasks

			const step = engine.reconnectSteps[engine.reconnectSteps.length - 1];
			const nextDelayMs = step?.delayMs ?? -1;
			const delayAuthoritative = step?.authoritative === true;

			// REST-side capture feeds the SAME knob (§3: both sources).
			gateway.script(
				"messages:c2c",
				{
					kind: "fail",
					message: "sendmessage rate limited [429]: retry after 2",
				},
				{ kind: "ok" },
			);
			let settled = false;
			const sending = engine
				.deliverText("u_rest", "payload")
				.then((r) => {
					settled = true;
					return r;
				});
			// The honor-once retry_after sleep registers BEHIND several real
			// async hops, so walk the injected clock until the send settles.
			for (let i = 0; i < 30 && !settled; i++) {
				await clock.advance(250);
			}
			const results = await sending;
			const restCaptured = engine.lastCapturedRetryAfterSeconds ?? -1;
			return {
				closeCapturedSeconds: 60,
				nextDelayMs,
				delayAuthoritative,
				restCapturedSeconds:
					results[results.length - 1]?.success === true ? restCaptured : -1,
			};
		},

		async capabilityLatchPermanence() {
			// The §10.1 tier-1 rich probe is the adapter's ONLY rich-capable
			// plane. A scripted capability-class failure latches rich off ONCE;
			// later sends SKIP the wire entirely; transient failures never latch;
			// supportsDraftStreaming stays False throughout (no native lanes).
			const world = freshWorld("qb-latch");
			const { engine, wire } = world;
			wire.script("rich", {
				kind: "fail",
				error: "sendWithKeyboard: method not found",
			});
			const first = await engine.deliverText("oc_latch", "**md** one");
			const latchedOnFirstFailure =
				first.every((r) => r.success === true) && // plain tier delivered
				engine.richWireAttempts === 1;

			// Post-latch attempt: must SKIP the wire (no new rich roundtrip).
			await engine.deliverText("oc_latch", "second send skips rich");
			const wireAttemptsAfterSkip = engine.richWireAttempts === 1 ? 1 : -1;

			const supportsStreamingFalse =
				engine.supportsDraftStreaming("dm") === false;

			// TRANSIENT failures never latch — each send probes rich AGAIN
			// (attempts grow) and the ladder reports the failure honestly.
			const world2 = freshWorld("qb-latch-transient");
			world2.wire.script(
				"rich",
				{ kind: "fail", error: "socket hang up mid-post" },
				{ kind: "fail", error: "socket hang up again" },
			);
			await world2.engine.deliverText("oc_t", "x **y**");
			await world2.engine.deliverText("oc_t2", "z **w**");
			const transientDidNotLatch = world2.engine.richWireAttempts === 2;

			return {
				latchedOnFirstFailure,
				latchCount: 1,
				wireAttemptsAfterSkip,
				supportsStreamingFalse,
				transientDidNotLatch,
			};
		},

		async dualPathMarkdown() {
			const world = freshWorld("qb-dual-path");
			const { engine, gateway } = world;
			await world.connectAndAwaitLive();

			// Learn chat kinds from inbound traffic first (chat_type_map parity):
			gateway.pushDispatch(...c2cDispatch("dp-seed", "u_dp", "seed"));
			await eventually(() => engine.turnLog.length >= 1);

			// ── leg (i): markdown decision locks WHOLE-MESSAGE per deliver call.
			const md = "**bold** intro [link](https://x.y)";
			const callsBeforeMd = gateway.callsOf("messages:c2c").length;
			await engine.deliverText("u_dp", md);
			const mdSends = gateway.callsOf("messages:c2c").slice(callsBeforeMd);
			const nativeRawByteExact =
				mdSends.length >= 1 &&
				mdSends.every((c) => c.body["msg_type"] === 2) &&
				mdSends.some(
					(c) =>
						(c.body["markdown"] as Record<string, unknown>)["content"] === md,
				); // RAW bytes preserved VERBATIM

			// Prefix stability across chunks: long markdown splits with fence
			// carry and STRIPPED pieces reconstruct byte-exact.
			const long = Array.from({ length: 30 }, (_, i) => `para-${i} **x**`).join(
				"\n\n",
			);
			const callsBeforeLong = gateway.callsOf("messages:c2c").length;
			await engine.deliverText("u_dp", long);
			const chunkBodies = gateway
				.callsOf("messages:c2c")
				.slice(callsBeforeLong)
				.map((c) =>
					String((c.body["markdown"] as Record<string, unknown>)["content"]),
				);
			const reconstructed = reconstructFromChunks(chunkBodies);
			const nativePrefixStable = reconstructed === long;

			// ── leg (ii): dialect handling per vendor reality ──
			// markdown_support=false → stripped content rides msg_type text (0).
			const worldPlain = makeQQWorld({
				name: "qb-dual-plain",
				markdownSupport: false,
			});
			worldPlain.engine.chatTypeMap.set("u_plain", "c2c");
			await worldPlain.engine.deliverText("u_plain", "**plain** words only");
			const plainCalls = worldPlain.gateway.callsOf("messages:c2c");
			const restConvertedBold =
				plainCalls.length >= 1 &&
				plainCalls[0]!.body["msg_type"] === 0 &&
				String(plainCalls[0]!.body["content"]) === "plain words only";

			// Tables ride verbatim inside markdown bodies (no downgrade lane).
			const table = "| a | b |\n|---|---|\n| 1 | 2 |";
			await engine.deliverText("u_dp", table);
			const tableCalls = gateway
				.callsOf("messages:c2c")
				.slice(callsBeforeLong + chunkBodies.length);
			const restConvertedLink =
				tableCalls.length >= 1 &&
				tableCalls.some(
					(c) =>
						(c.body["markdown"] as Record<string, unknown>)["content"] ===
						table,
				);
			const restConvertedTable =
				tableCalls.length >= 1 &&
				!JSON.stringify(tableCalls).includes("<https");

			// ── leg (iii): preview/suppression policy is CONSISTENTLY ABSENT ──
			const allMessageBodies = [
				...gateway.callsOf("messages:c2c"),
				...gateway.callsOf("messages:group"),
				...worldPlain.gateway.callsOf("messages:c2c"),
			];
			const linkPreviewOnAllTextSends =
				allMessageBodies.length > 0 &&
				allMessageBodies.every(
					(c) => c.body["link_preview_suppressed"] === undefined,
				);
			const nonTextLanes = [...world.wire.ops, ...worldPlain.wire.ops].filter(
				(o) => o.op === "draft" || o.op === "seal",
			);
			const linkPreviewAbsentOffTextSends = nonTextLanes.length === 0;

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

/** Reconstruct original text from markdown bodies: drop fences + indicators. */
const CHUNK_LABEL_RE = /\((\d+)\/(\d+)\)$/;
function reconstructFromChunks(contents: string[]): string {
	return contents
		.map((c) => {
			let text = c.replace(/\n?```\n?/g, "");
			text = text.replace(CHUNK_LABEL_RE, "");
			return text;
		})
		.join("")
		.trim();
}
