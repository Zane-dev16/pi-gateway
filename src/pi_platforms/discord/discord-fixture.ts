// pi_platforms/discord/discord-fixture — THE Discord transport-row fixture
// (shapes.ts WsFixture, inherited persistent-ws family contract) implemented
// against the REAL DiscordAdapter engine: every row drives actual gateway
// opens, HELLO→IDENTIFY/READY or RESUME handshakes, application heartbeats,
// watchdog passes and ladder sleeps under the INJECTED clock, and dual-path
// markdown dispatch over the fake planes. Behavior contracts, not stubbed
// return values.
//
// Dual-path leg mapping (§10.2 realization for the vendor dialect — proposed
// DEC entry in the port report): Discord renders standard markdown natively
// on BOTH lanes, so emphasis/link bytes are preserved verbatim on the REST
// lane (the family row's "converted" legs map to the constructs the Discord
// dialect genuinely cannot render: GFM tables → fenced monospace), and
// link-preview suppression rides text sends as the SUPPRESS_EMBEDS flag.

import { FakePlatformWire } from "../conformance/wire.js";
import type { WsFixture } from "../conformance/shapes.js";
import type { TaskSpawner } from "../../pi_gateway/guards/index.js";
import { ManualClock } from "./clock.js";
import { DiscordGatewayFake } from "./gateway-fake.js";
import type { HistoryProvider } from "./discord-adapter.js";
import type { ReconnectLadderOptions } from "../persistent-ws/reconnect-ladder.js";
import { makeDiscordSubject, type DiscordSubject } from "./discord-subject.js";

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

export interface DiscordWorld {
	subject: DiscordSubject;
	engine: DiscordSubject["adapter"];
	gateway: DiscordGatewayFake;
	wire: FakePlatformWire;
	clock: ManualClock;
	/** Open the gateway and wait until the session is LIVE (post READY/RESUMED). */
	connectAndAwaitLive(): Promise<void>;
}

export interface MakeDiscordWorldOptions {
	name?: string | undefined;
	spawner?: TaskSpawner | undefined;
	heartbeatIntervalMs?: number | undefined;
	livenessIntervalMs?: number | undefined;
	ackMaxAgeMs?: number | undefined;
	livenessFailureThreshold?: number | undefined;
	ladder?: ReconnectLadderOptions | undefined;
	rateGate?: boolean | undefined;
	historyProvider?: HistoryProvider | undefined;
}

/** A full discord world: subject + engine + gateway fake + injected clock. */
export function makeDiscordWorld(
	opts: MakeDiscordWorldOptions = {},
): DiscordWorld {
	const clock = new ManualClock();
	const gateway = new DiscordGatewayFake({ nowMs: clock.nowMs });
	if (opts.heartbeatIntervalMs !== undefined)
		gateway.heartbeatIntervalMs = opts.heartbeatIntervalMs;
	const wire = new FakePlatformWire();
	const subject = makeDiscordSubject({
		wire,
		gateway,
		clock,
		name: opts.name,
		...(opts.spawner === undefined ? {} : { spawner: opts.spawner }),
		...(opts.rateGate === undefined ? {} : { rateGate: opts.rateGate }),
		...(opts.historyProvider === undefined
			? {}
			: { historyProvider: opts.historyProvider }),
		ladder: opts.ladder ?? {
			baseDelayMs: 100,
			jitterFraction: 0,
			rng: () => 0,
		},
	});
	// Watchdog tuning passthrough (fixture determinism; defaults port A13).
	const engine = subject.adapter as unknown as Record<string, unknown>;
	if (opts.livenessIntervalMs !== undefined)
		engine["livenessIntervalMs"] = opts.livenessIntervalMs;
	if (opts.ackMaxAgeMs !== undefined) engine["ackMaxAgeMs"] = opts.ackMaxAgeMs;
	if (opts.livenessFailureThreshold !== undefined)
		engine["livenessThreshold"] = opts.livenessFailureThreshold;

	return {
		subject,
		engine: subject.adapter,
		gateway,
		wire,
		clock,
		async connectAndAwaitLive(): Promise<void> {
			void engine;
			await subject.adapter.connect({ isReconnect: false });
			await eventually(() => subject.adapter.isLive);
		},
	};
}

/**
 * THE fixture behind shapes.ts::makeWsRows — five §3/DEC-032/034 scenarios
 * run against the LIVE Discord engine. Each call gets a FRESH world.
 */
export function makeRealDiscordFixture(): WsFixture {
	return {
		/**
		 * Row: resume replay covers messages sent during the disconnect —
		 * SEQ-exact (server replays strictly s > resume seq) and exactly-once
		 * downstream (dedup suppresses overlap). THE shape delta vs the family
		 * reference: the subscribe carries session_id+seq, not an event cursor.
		 */
		async resubscribeReplay() {
			const world = makeDiscordWorld({ name: "discord-replay" });
			const { engine, gateway, clock, subject } = world;
			await world.connectAndAwaitLive();

			gateway.pushMessage({
				id: "m1",
				channelId: "chat-1",
				authorId: "user-1",
				content: "r1",
			});
			gateway.pushMessage({
				id: "m2",
				channelId: "chat-1",
				authorId: "user-1",
				content: "r2",
			});
			await eventually(() => subject.turns().length >= 2);

			gateway.dropActive({ reason: "transport blip" }); // OUTAGE mid-life
			await eventually(() => !engine.isLive);
			const deliveredBefore = engine.inboundLog.length; // 2 pre-outage
			// Sent DURING the disconnect window (seq-buffered server-side).
			gateway.pushMessage({
				id: "m3",
				channelId: "chat-1",
				authorId: "user-1",
				content: "r3",
			});
			gateway.pushMessage({
				id: "m4",
				channelId: "chat-1",
				authorId: "user-1",
				content: "r4",
			});
			gateway.pushMessage({
				id: "m5",
				channelId: "chat-1",
				authorId: "user-1",
				content: "r5",
			});
			const sentDuringDisconnect = 3;

			await clock.advance(5_000); // ladder sleep → reopen → RESUME
			await eventually(
				() => engine.isLive && engine.inboundLog.length >= deliveredBefore + 3,
			);
			await eventually(() => subject.turns().length >= 3);

			// Shape-delta evidence: the reconnect handshake was a RESUME carrying
			// session_id + the last acked SEQUENCE (not an event-id cursor).
			const resumes = gateway.receivedFrames.filter((f) => f.frame.op === 6);
			const lastResume = resumes[resumes.length - 1]?.frame.d as
				| { session_id?: string; seq?: number }
				| undefined;
			if (
				lastResume === undefined ||
				lastResume.session_id !== engine.sessionId ||
				typeof lastResume.seq !== "number"
			) {
				throw new Error(
					`reconnect did not RESUME with session_id+seq: ${JSON.stringify(lastResume)}`,
				);
			}

			// Exactly-once: five DISTINCT message ids reached the pipeline, in
			// order — burst merge may coalesce TURNS, never drop/redup events.
			const ids = engine.inboundLog;
			const unique = new Set(ids);
			if (unique.size !== ids.length)
				throw new Error(`duplicate deliveries: ${JSON.stringify(ids)}`);
			return {
				sentDuringDisconnect,
				replayedAfterResubscribe: unique.size - deliveredBefore,
			};
		},

		/**
		 * Row: the heartbeat-ACK watchdog reaps a WEDGED socket (open but acks
		 * dead) and recovery resumes the stream WITHOUT loss — all under the
		 * INJECTED clock. Resume (not re-identify) proves zero-loss recovery.
		 */
		async watchdogRecovery() {
			const world = makeDiscordWorld({
				name: "discord-watchdog",
				heartbeatIntervalMs: 100,
				livenessIntervalMs: 40,
				ackMaxAgeMs: 120,
				livenessFailureThreshold: 2,
			});
			const { engine, gateway, clock, subject } = world;
			await world.connectAndAwaitLive();
			await clock.advance(300); // healthy heartbeats ACKed first
			const identifyBefore = gateway.identifyCount;

			gateway.stallHeartbeatAcks(); // wedged-zombie: OPEN, acks die
			await clock.advance(600); // ticks → 2 stale probes → reap → RESUME

			const detectedDeadSocket =
				engine.reconnectLog.length >= 1 && identifyBefore === 1;
			await eventually(() => engine.isLive);
			// Recovery resumed the SAME session — nothing lost, no re-identify.
			if (gateway.identifyCount !== identifyBefore)
				throw new Error("watchdog recovery re-identified (loss!)");

			gateway.pushMessage({
				id: "after-rec",
				channelId: "chat-1",
				authorId: "user-1",
				content: "after-recovery",
			});
			await eventually(() =>
				subject.turns().some((t) => t.includes("after-recovery")),
			);
			return { detectedDeadSocket, resumedWithoutLoss: true };
		},

		/**
		 * Row: Retry-After captured from BOTH sources (rate-limited CLOSE
		 * payload AND REST send result); the close capture IS the next reconnect
		 * delay and is marked authoritative over the exponential schedule.
		 */
		async retryAfterCapture() {
			const world = makeDiscordWorld({ name: "discord-retry-after" });
			const { engine, gateway, clock, wire } = world;
			await world.connectAndAwaitLive();

			gateway.dropActive({ retryAfterSeconds: 7 }); // authoritative close
			await eventually(() => engine.lastCapturedRetryAfterSeconds === 7);
			await clock.advance(8_000); // ladder honors 7s verbatim → live again
			await eventually(() => engine.isLive);

			const step = engine.reconnectLog[engine.reconnectLog.length - 1] ?? null;
			const nextDelayMs = step?.delayMs ?? -1;
			const delayAuthoritative = step?.authoritative === true;

			// REST-side capture feeds the SAME knob (both sources, §3 matrix).
			wire.script(
				"send",
				{
					kind: "fail",
					error: "rate limit 429",
					retryable: true,
					retryAfter: 0.05,
				},
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
		 * Row: a feature-gate error (vendor permission/access class) latches
		 * native streaming OFF for the whole session — later attempts SKIP the
		 * wire entirely (attempt count frozen); transient failures never latch.
		 */
		async capabilityLatchPermanence() {
			const world = makeDiscordWorld({ name: "discord-latch" });
			const { engine, wire } = world;
			wire.script("draft", {
				kind: "fail",
				error: "Missing Permissions (50013)",
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

			const world2 = makeDiscordWorld({ name: "discord-latch-transient" });
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
		 * Row (DEC-034 family contract, Discord dialect realization): the
		 * NATIVE streaming-edit plane ships RAW prefix-stable markdown bytes
		 * untouched; the REST lane preserves native emphasis/link bytes EXACTLY
		 * (the Discord dialect renders them natively) and converts the construct
		 * it cannot render — GFM tables → fenced monospace; SUPPRESS_EMBEDS
		 * rides TEXT sends only, never draft/seal/rich ops.
		 */
		async dualPathMarkdown() {
			const world = makeDiscordWorld({ name: "discord-dual-path" });
			world.subject.adapter.markStreamIsMessage("chat-dual");
			const { engine, wire } = world;

			// ── leg (i): native plane ships RAW cumulative frames ──
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
				startOps[0]?.content === frame1 &&
				appendDeltas.length === 1 &&
				appendDeltas[0] === "\n**more**" &&
				allFragments.every((f) => !f.includes("<https://x.y|"));
			const nativePrefixStable = reconstructed === frame2;

			// ── leg (ii): REST lane — dialect handling per vendor reality ──
			await engine.deliverText("chat-rest", "**bold** and [link](https://x.y)");
			await engine.deliverText("chat-rest", "| a | b |\n|---|---|\n| 1 | 2 |");
			const restSends = wire.sendsOf("chat-rest");
			const restBody = restSends.map((s) => s.content).join("\n");
			// Discord-native emphasis/link bytes preserved VERBATIM (no lossy
			// mrkdwn-style collapse — that would corrupt rendering).
			const restConvertedBold =
				restBody.includes("**bold**") &&
				!/\*bold\*(?!\*)/.test(restBody.replace(/\*\*/g, ""));
			const restConvertedLink =
				restBody.includes("[link](https://x.y)") &&
				!restBody.includes("<https://x.y|");
			// GFM tables CANNOT render in Discord → fenced aligned monospace.
			const restConvertedTable =
				restBody.includes("```") && !restBody.includes("| a | b |");

			// ── leg (iii): SUPPRESS_EMBEDS is TEXT-send-only metadata ──
			const textSends = wire.ops.filter((o) => o.op === "send");
			const nonTextOps = wire.ops.filter(
				(o) => o.op === "draft" || o.op === "seal" || o.op === "rich",
			);
			const linkPreviewOnAllTextSends =
				textSends.length > 0 &&
				textSends.every((o) => o.metadata["suppress_embeds"] === 4);
			const linkPreviewAbsentOffTextSends = nonTextOps.every(
				(o) => o.metadata["suppress_embeds"] === undefined,
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

export type { DiscordSubject };
