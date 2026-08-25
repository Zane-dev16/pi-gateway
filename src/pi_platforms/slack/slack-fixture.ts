// pi_platforms/slack/slack-fixture — the Slack transport-row fixture
// (shapes.ts WsFixture contract — the port INHERITS the persistent-ws family
// rows and executes them against the REAL SlackAdapter over the Socket-Mode
// fake server). Every row body drives actual socket opens, hello/subscribed
// handshakes, watchdog passes under the INJECTED clock, ladder sleeps, and
// dual-path markdown dispatch. Behavior contracts, not stubbed values.

import type { FakePlatformWire } from "../conformance/wire.js";
import type { WsFixture } from "../conformance/shapes.js";
import type { TaskSpawner } from "../../pi_gateway/guards/index.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import type { ReconnectLadderOptions } from "../persistent-ws/reconnect-ladder.js";
import {
	SlackSocketModeServer,
	type SlackInteractivePayload,
} from "./fake-socket-mode.js";
import {
	makeSlackSubject,
	SlackCapturingWire,
	type SlackSubject,
} from "./slack-subject.js";

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

export interface SlackWorld {
	subject: SlackSubject;
	engine: SlackSubject["adapter"];
	socketServer: SlackSocketModeServer;
	wire: FakePlatformWire;
	clock: ManualClock;
	/** Open the socket and wait until hello/subscribed completes. */
	connectAndAwaitLive(): Promise<void>;
	/** Push a user message through Socket Mode and return its envelope id. */
	pushMessage(chatId: string, text: string): string;
	/** Push a message_changed envelope (user edit) and return the event. */
	pushMessageChanged(evt: {
		channel: string;
		user?: string | undefined;
		text: string;
		originalTs: string;
		editedTs?: string | undefined;
		eventTs?: string | undefined;
		thread_ts?: string | undefined;
		botId?: string | undefined;
	}): ReturnType<SlackSocketModeServer["pushMessageChanged"]>;
	/** Push an interactivity payload to every live connection. */
	pushInteractive(payload: SlackInteractivePayload): void;
}

export interface MakeSlackWorldOptions {
	name?: string | undefined;
	spawner?: TaskSpawner | undefined;
	pingIntervalMs?: number | undefined;
	pingStaleFactor?: number | undefined;
	firstPingGraceMs?: number | undefined;
	watchdogIntervalMs?: number | undefined;
	ladder?: ReconnectLadderOptions | undefined;
	streamIsMessageChatIds?: readonly string[] | undefined;
	richBlocks?: boolean | undefined;
	scalarMaxUnits?: number | undefined;
	/** SLACK_REACTIONS override (default env-driven true). */
	reactionsEnabled?: boolean | undefined;
	/** Deterministic auth.test identity override. */
	authIdentity?: import("./slack-subject.js").SlackSubjectOptions["authIdentity"];
}

/** A full slack world: subject + engine + Socket-Mode server + injected clock. */
export function makeSlackWorld(opts: MakeSlackWorldOptions = {}): SlackWorld {
	const clock = new ManualClock();
	const socketServer = new SlackSocketModeServer({ nowMs: clock.nowMs });
	const wire = new SlackCapturingWire();
	const subject = makeSlackSubject({
		wire,
		ws: socketServer,
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
		...(opts.richBlocks !== undefined ? { richBlocks: opts.richBlocks } : {}),
		...(opts.scalarMaxUnits !== undefined
			? { scalarMaxUnits: opts.scalarMaxUnits }
			: {}),
		...(opts.reactionsEnabled !== undefined
			? { reactionsEnabled: opts.reactionsEnabled }
			: {}),
		...(opts.authIdentity !== undefined
			? { authIdentity: opts.authIdentity }
			: {}),
	});
	if (opts.streamIsMessageChatIds !== undefined) {
		for (const id of opts.streamIsMessageChatIds)
			subject.adapter.markStreamIsMessage(id);
	}
	const engine = subject.adapter;
	return {
		subject,
		engine,
		socketServer,
		wire,
		clock,
		async connectAndAwaitLive(): Promise<void> {
			await engine.connect({ isReconnect: false });
			await eventually(() => engine.isLive);
		},
		pushMessage(chatId: string, text: string): string {
			return socketServer.pushMessage({
				channel: chatId,
				user: "user-1",
				text,
			}).id;
		},
		pushMessageChanged(evt) {
			const { botId, ...rest } = evt;
			return socketServer.pushMessageChanged(
				{ user: "user-1", ...rest },
				botId !== undefined ? { botId } : {},
			);
		},
		pushInteractive(payload: SlackInteractivePayload): void {
			socketServer.pushInteractive(payload);
		},
	};
}

/**
 * THE fixture behind shapes.ts::makeWsRows for the SLACK port — the five
 * inherited ws-family scenarios executed against the live slack world.
 * Each call gets a FRESH world (rows never couple through shared state).
 */
export function makeRealSlackFixture(): WsFixture {
	return {
		/**
		 * Resubscribe replay covers messages sent during the disconnect —
		 * cursor-exact (the resubscribe carried the last-acked id) and
		 * exactly-once downstream (workspace-scoped dedup suppresses overlap).
		 */
		async resubscribeReplay() {
			const world = freshWorld("slack-replay");
			const { engine, socketServer, clock, subject } = world;
			await world.connectAndAwaitLive();

			world.pushMessage("C1", "r1");
			world.pushMessage("C1", "r2");
			await eventually(() => engine.cursor.value === "e2"); // ack point

			socketServer.dropActive({ reason: "transport blip" });
			const before = engine.inboundLog.length; // 2 delivered pre-outage
			world.pushMessage("C1", "r3"); // sent DURING the disconnect window
			world.pushMessage("C1", "r4");
			world.pushMessage("C1", "r5");
			const sentDuringDisconnect = 3;

			await clock.advance(5_000); // reconnect ladder sleep → resubscribe
			await eventually(
				() => engine.isLive && engine.inboundLog.length >= before + 3,
			);

			const ids = engine.inboundLog.map((e) => e.id);
			const unique = new Set(ids);
			return {
				sentDuringDisconnect,
				replayedAfterResubscribe: unique.size - before,
			};
		},

		/**
		 * The heartbeat watchdog reaps a WEDGED socket (open but pings
		 * unanswered) and recovery resumes WITHOUT loss — injected clock only.
		 */
		async watchdogRecovery() {
			const world = freshWorld("slack-watchdog", {
				pingIntervalMs: 100,
				pingStaleFactor: 2,
				firstPingGraceMs: 150,
				watchdogIntervalMs: 50,
				ladder: { baseDelayMs: 100, jitterFraction: 0, rng: () => 0 },
			});
			const { engine, socketServer, clock, subject } = world;
			await world.connectAndAwaitLive();

			socketServer.stallPongs();
			await clock.advance(1_000); // ticks → staleness → reap → ladder → live

			await eventually(() => engine.isLive);

			world.pushMessage("C1", "after-recovery");
			await eventually(() =>
				subject.turns().some((t) => t.includes("after-recovery")),
			);
			return {
				detectedDeadSocket: engine.reconnectLog.length >= 1,
				resumedWithoutLoss: true,
			};
		},

		/**
		 * Retry-After captured from BOTH sources (close payload AND REST send
		 * result); the close capture IS the next reconnect delay (authoritative).
		 */
		async retryAfterCapture() {
			const world = freshWorld("slack-retry-after");
			const { engine, socketServer, clock, wire } = world;
			await world.connectAndAwaitLive();

			socketServer.dropActive({ retryAfterSeconds: 7 });
			await eventually(() => engine.lastCapturedRetryAfterSeconds === 7);
			await clock.advance(8_000); // ladder honors 7s verbatim → live again
			await eventually(() => engine.isLive);

			const step = engine.reconnectLog[engine.reconnectLog.length - 1];
			const nextDelayMs = step?.delayMs ?? -1;
			const delayAuthoritative = step?.authoritative === true;

			wire.script(
				"send",
				{ kind: "fail", error: "flood control", retryAfter: 0.05 },
				{ kind: "ok" },
			);
			const results = await engine.deliverText("C-ra", "payload");
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
		 * A feature-gate error latches native streaming OFF for the session —
		 * later attempts SKIP the wire entirely; transient failures never latch.
		 */
		async capabilityLatchPermanence() {
			const world = freshWorld("slack-latch");
			const { engine, wire } = world;
			wire.script("draft", {
				kind: "fail",
				error: "feature_not_enabled: streaming_not_allowed",
			});
			const first = await engine.sendDraft({
				chatId: "C-latch",
				draftId: 1,
				content: "**md**",
				metadata: { thread_id: "1700000000.000001" } as never,
			});
			const latchedOnFirstFailure =
				first.success === false && engine.nativeStreamLatch.unsupported;
			const latchCount = engine.nativeStreamLatch.latchCount;
			const supportsFalse = engine.supportsDraftStreaming() === false;

			const attemptsBefore = engine.nativeStreamLatch.wireAttempts;
			const draftsBefore = wire.draftsOf("C-latch").length;
			await engine.sendDraft({
				chatId: "C-latch",
				draftId: 2,
				content: "**more**",
			});
			const wireAttemptsAfterSkip =
				engine.nativeStreamLatch.wireAttempts === attemptsBefore &&
				wire.draftsOf("C-latch").length === draftsBefore
					? attemptsBefore
					: -1;

			const world2 = freshWorld("slack-latch-transient");
			world2.wire.script(
				"draft",
				{ kind: "fail", error: "network hiccup mid-frame" },
				{ kind: "fail", error: "network hiccup mid-frame again" },
			);
			await world2.engine.sendDraft({
				chatId: "C-t",
				draftId: 1,
				content: "x",
				metadata: { thread_id: "1700000000.000002" } as never,
			});
			await world2.engine.sendDraft({
				chatId: "C-t",
				draftId: 2,
				content: "xy",
				metadata: { thread_id: "1700000000.000002" } as never,
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
		 * Dual-path markdown (DEC-034) on the SLACK converter: native stream
		 * ships RAW prefix-stable bytes untouched; REST path converts through
		 * convertMarkdownToSlackMrkdwn (bold/link/table); link-preview
		 * suppression rides TEXT sends only.
		 */
		async dualPathMarkdown() {
			const world = freshWorld("slack-dual-path", {
				streamIsMessageChatIds: ["C-dual"],
			});
			const { engine, wire } = world;

			// ── leg (i): native stream plane ships RAW cumulative frames ──
			// (drafts carry the turn identity the production gateway always
			// stamps — chat.startStream requires a thread anchor.)
			const frame1 = "**bold** intro [link](https://x.y)";
			const streamMd = { thread_id: "1700000000.000011" };
			await engine.sendDraft({
				chatId: "C-dual",
				draftId: 11,
				content: frame1,
				metadata: streamMd as never,
			});
			const frame2 = `${frame1}\n**more**`;
			await engine.sendDraft({
				chatId: "C-dual",
				draftId: 11,
				content: frame2,
				metadata: streamMd as never,
			});
			const startOps = wire
				.draftsOf("C-dual")
				.filter((d) => d.metadata["stream_op"] === "start");
			const appendDeltas = wire
				.draftsOf("C-dual")
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

			// ── leg (ii): REST path CONVERTS via THE slack mrkdwn ladder ──
			await engine.deliverText("C-rest", "**bold** and [link](https://x.y)");
			await engine.deliverText("C-rest", "| a | b |\n|---|---|\n| 1 | 2 |");
			const restSends = wire.sendsOf("C-rest");
			const restBody = restSends.map((s) => s.content).join("\n");
			const restConvertedBold =
				restBody.includes("*bold*") && !restBody.includes("**bold**");
			const restConvertedLink =
				restBody.includes("<https://x.y|link>") &&
				!restBody.includes("[link](https://x.y)");
			const restConvertedTable =
				restBody.includes("```") && !restBody.includes("| a | b |");

			// ── leg (iii): link-preview suppression is TEXT-send-only metadata ──
			const textSends = wire.ops.filter((o) => o.op === "send");
			const nonTextOps = wire.ops.filter(
				(o) => o.op === "draft" || o.op === "seal" || o.op === "rich",
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

function freshWorld(
	name: string,
	opts: MakeSlackWorldOptions = {},
): SlackWorld {
	return makeSlackWorld({ name, ...opts });
}
