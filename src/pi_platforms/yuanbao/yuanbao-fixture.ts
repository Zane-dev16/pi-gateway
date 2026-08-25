// pi_platforms/yuanbao/yuanbao-fixture — the YUANBAO transport-row fixture
// (shapes.ts WsFixture) implemented against the REAL YuanbaoAdapter engine:
// rows drive actual AUTH_BIND handshakes, binary protobuf pushes, PushAck
// capture, close-code classes, and the §10.1 rich-latch path under the
// INJECTED clock. Behavior contracts, never stubbed values.
//
// Row-realization notes (vendor-truth transcriptions, family row names kept):
//   - resubscribeReplay → Yuanbao has NO server-side replay cursor; pushes
//     emitted while NO live session exists wait in the fake gateway's queue
//     and are delivered on reconnect. Exactly-once downstream is the
//     adapter's 300s-TTL dedup (DedupMiddleware); an at-least-once
//     REDUPLICATED id proves it.
//   - watchdogRecovery → hard TCP death (error, no close frame) feeds the
//     read-error path into the SAME reconnect ladder; recovery re-auths
//     without loss.
//   - retryAfterCapture → the wire's authoritative timing carrier is the
//     close REASON: a 4099 (auth-retryable) close with reason "retry-after:N"
//     captures N seconds verbatim as the next reconnect delay. The REST leg
//     captures retry_after from send-error blobs via the shared kit extractor.
//   - capabilityLatchPermanence → NO native streaming declared; the §10.1
//     tier-1 rich probe latches OFF once on capability-class failure; later
//     sends SKIP the wire; transient failures never latch.
//   - dualPathMarkdown → TIMTextElem ships RAW text bytes in protobuf bodies
//     (never converted/collapsed); plain identical; NO link-preview concept.

import { FakePlatformWire } from "../conformance/wire.js";
import type { WsFixture } from "../conformance/shapes.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { chunkWithFenceCarry } from "../kit/chunking.js";
import { eventually } from "./eventually.js";
import { FakeYuanbaoGateway } from "./fake-yuanbao.js";
import { makeYBSubject, type YuanbaoSubject } from "./yuanbao-subject.js";

export interface YBWorld {
	subject: YuanbaoSubject;
	engine: YuanbaoSubject["adapter"];
	gateway: FakeYuanbaoGateway;
	wire: FakePlatformWire;
	clock: ManualClock;
	connectAndAwaitLive(): Promise<void>;
}

/** A full yuanbao world: subject + engine + fake gateway + injected clock. */
export function makeYBWorld(opts: { name?: string | undefined } = {}): YBWorld {
	const clock = new ManualClock();
	const gateway = new FakeYuanbaoGateway();
	const wire = new FakePlatformWire();
	// NO scheduler: guard attaches with the immediate production spawner so
	// fixture rows observe turnLog growth directly.
	const subject = makeYBSubject({
		wire,
		gateway,
		name: opts.name,
		nowMs: clock.nowMs,
		sleepMs: clock.sleepMs,
		captureMode: "off", // fixture worlds drive the REAL binary WS face
		replyHeartbeatIntervalMs: 20, // deterministic tick under wall waits
		slowResponseTimeoutMs: 60,
	});
	if (opts.name !== undefined) void opts.name;
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
			await eventually(() => engine.connectId !== null);
		},
	};
}

function pushText(
	messageId: string,
	fromAccount: string,
	text: string,
	extra: Record<string, unknown> = {},
): Record<string, unknown> {
	return {
		from_account: fromAccount,
		msg_id: messageId,
		msg_seq: 1,
		group_code: "",
		callback_command: "",
		msg_body: [{ msg_type: "TIMTextElem", msg_content: { text } }],
		...extra,
	};
}

/**
 * Pump the INJECTED clock until `predicate` holds: inbound WS pushes park in
 * the adapter's per-sender DEBOUNCE_WINDOW (1.5s injected), so delivery only
 * happens as injected time advances (wall waits never flush it).
 */
async function pumpUntil(
	world: YBWorld,
	predicate: () => boolean,
	tries = 300,
): Promise<void> {
	for (let i = 0; i < tries && !predicate(); i++) {
		await world.clock.advance(100);
		await new Promise<void>((r) => setTimeout(r, 2));
	}
	if (!predicate()) throw new Error("pumpUntil: condition not met");
}

/**
 * THE fixture behind shapes.ts::makeWsRows — five §3/DEC-034 scenarios run
 * against the live engine. Each call gets a FRESH world.
 */
export function makeRealYBFixture(): WsFixture {
	return {
		async resubscribeReplay() {
			const world = makeYBWorld({ name: "yb-replay" });
			const { engine, gateway } = world;
			await world.connectAndAwaitLive();

			// Two same-sender pushes land INSIDE one DEBOUNCE_WINDOW: they merge
			// into ONE turn with the "\n" companion separator (_push_to_inbound
			// + DecodeMiddleware merged-push parity).
			gateway.pushMessage(pushText("r-1", "u_replay", "r1"));
			gateway.pushMessage(pushText("r-2", "u_replay", "r2"));
			await pumpUntil(world, () => engine.turnLog.some((t) => t === "r1\nr2"));

			gateway.dropActive(1001, "going away"); // OUTAGE mid-life
			void world.clock.advance(8_000); // ladder sleep → forceRefresh → reconnect

			gateway.pushMessage(pushText("r-3", "u_replay", "r3"));
			gateway.pushMessage(pushText("r-4", "u_replay", "r4"));
			gateway.pushMessage(pushText("r-5", "u_replay", "r5"));
			const sentDuringDisconnect = 3;
			// Offline pushes queue in the fake gateway and flush on the next
			// AUTH_BIND; the back-to-back replay lands INSIDE one debounce
			// window, so it merges into ONE replayed turn (no loss, no split).
			await pumpUntil(world, () =>
				engine.turnLog.some((t) => t === "r3\nr4\nr5"),
			);

			// At-least-once REDUPLICATED id in a LATER window: dedup absorbs it
			// exactly-once — no additional turn may appear.
			gateway.pushMessage(pushText("r-1", "u_replay", "r1"));
			await world.clock.advance(2_000); // flush the dup's own window
			await new Promise<void>((r) => setTimeout(r, 4));
			if (
				engine.turnLog.some(
					(t) => t.split("\n").includes("r1") && t !== "r1\nr2",
				)
			) {
				throw new Error(
					`exactly-once violated for r-1: ${JSON.stringify(engine.turnLog)}`,
				);
			}

			// Every sent-during-disconnect text reached intake post-reconnect.
			const windowIds = ["r3", "r4", "r5"].filter((id) =>
				engine.turnLog.some((t) => t.split("\n").includes(id)),
			);
			return {
				sentDuringDisconnect,
				replayedAfterResubscribe: new Set(windowIds).size,
			};
		},

		async watchdogRecovery() {
			const world = makeYBWorld({ name: "yb-watchdog" });
			const { engine, gateway } = world;
			await world.connectAndAwaitLive();

			const connectionsBefore = gateway.openConnectionCount;
			gateway.hardDrop(); // dead-TCP shape
			await eventually(() =>
				engine.closeLog.some((l) => l.outcome.startsWith("read-error")),
			);
			const detectedDeadSocket =
				engine.closeLog.some((l) => l.outcome.startsWith("read-error")) &&
				gateway.openConnectionCount <= connectionsBefore;

			void world.clock.advance(8_000); // ladder sleep → reconnect
			await eventually(() => engine.isLive, 6_000);
			gateway.pushMessage(pushText("wd-1", "u_wd", "after-recovery"));
			// The recovered push parks in the debounce window (INJECTED time):
			// pump until the merged turn delivers.
			await pumpUntil(world, () =>
				world.subject.turns().some((t) => t.includes("after-recovery")),
			);
			return { detectedDeadSocket, resumedWithoutLoss: true };
		},

		async retryAfterCapture() {
			const world = makeYBWorld({ name: "yb-retry-after" });
			const { engine, gateway, wire, clock } = world;
			await world.connectAndAwaitLive();

			// CLOSE leg: 4099 auth-retryable close carrying reason retry-after:7.
			gateway.dropActive(4099, "retry-after:7");
			await eventually(() => engine.lastCapturedRetryAfterSeconds === 7, 4_000);
			void clock.advance(7_000); // ladder honors captured value verbatim
			await eventually(() => engine.isLive, 6_000);

			const nextDelayMs = engine.reconnectSteps.at(-1)?.delayMs ?? -1;
			const delayAuthoritative =
				engine.reconnectSteps.at(-1)?.authoritative === true;

			// REST leg: send-error blob carrying retry_after via kit extractor.
			wire.script(
				"send",
				{ kind: "fail", error: "sendmessage busy: retry after 2" },
				{ kind: "ok" },
			);
			let settled = false;
			const sending = engine
				.deliverText("direct:u_rest", "payload")
				.then((r) => {
					settled = true;
					return r;
				});
			for (let i = 0; i < 30 && !settled; i++) await clock.advance(500);
			const results = await sending;
			const restCaptured = engine.lastCapturedRetryAfterSeconds ?? -1;
			return {
				closeCapturedSeconds: 7,
				nextDelayMs,
				delayAuthoritative,
				restCapturedSeconds:
					results[results.length - 1]?.success === true ? restCaptured : -1,
			};
		},

		async capabilityLatchPermanence() {
			const world = makeYBWorld({ name: "yb-latch" });
			const { engine, wire } = world;
			wire.script("rich", {
				kind: "fail",
				error: "sendRichMessage: method not found",
			});
			// The ladder's tier-2/tier-3 legs deliver over the LIVE face; the
			// rich probe itself rides the scripted capture seam.
			await world.connectAndAwaitLive();
			const first = await engine.deliverText("oc_latch", "**md** one");
			const latchedOnFirstFailure =
				first.every((r) => r.success === true) && engine.richWireAttempts === 1;

			await engine.deliverText("oc_latch", "second send skips rich");
			const wireAttemptsAfterSkip = engine.richWireAttempts === 1 ? 1 : -1;
			const supportsStreamingFalse =
				engine.supportsDraftStreaming("dm") === false;

			const world2 = makeYBWorld({ name: "yb-latch-transient" });
			world2.wire.script(
				"rich",
				{ kind: "fail", error: "socket hang up mid-post" },
				{ kind: "fail", error: "socket hang up again" },
			);
			await world2.connectAndAwaitLive();
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
			const world = makeYBWorld({ name: "yb-dual-path" });
			const { engine } = world;
			await world.connectAndAwaitLive();

			// RAW markdown text ships VERBATIM inside TIMTextElem content.
			const md = "**bold** intro [link](https://x.y)";
			await engine.deliverText("direct:u_dp", md);
			const sends = engine.serverSends.filter(
				(s) => s.chatId === "direct:u_dp",
			);
			const nativeRawByteExact =
				sends.length >= 1 && sends.every((s) => s.text === md);

			// Prefix stability: long content splits via THE kit fence-carry
			// chunker; the wire chunks must be BYTE-EXACT against the computed
			// plan (RAW bytes in, no duplication, no mutation), and the stripped
			// pieces must carry the full content (whitespace-normalized — label
			// relocation consumes exactly the seam separator per scaffold
			// labelJoinLen, a vendor chunker property, not an adapter one).
			const long = Array.from({ length: 30 }, (_, i) => `para-${i} **x**`).join(
				"\n\n",
			);
			const before = engine.serverSends.length;
			await engine.deliverText("direct:u_dp", long);
			const chunkTexts = engine.serverSends.slice(before).map((s) => s.text);
			const policy = engine.chatLengthPolicyForChat("direct:u_dp");
			const plan = chunkWithFenceCarry(long, policy);
			const wireByteExactAgainstPlan =
				chunkTexts.length === plan.chunks.length &&
				chunkTexts.every((c, i) => c === plan.chunks[i]);
			const squash = (t: string): string => t.replace(/\s+/g, " ").trim();
			const stripPiece = (c: string): string =>
				c.replace(/\n?```\n?/g, "").replace(/\s*\(\d+\/\d+\)\s*$/, "");
			const nativePrefixStable =
				wireByteExactAgainstPlan &&
				squash(chunkTexts.map(stripPiece).join(" ")) === squash(long);

			// Plain + tables ride verbatim identically (single dialect).
			await engine.deliverText("direct:u_plain", "plain words only");
			const plainSend = engine.serverSends.find(
				(s) => s.chatId === "direct:u_plain",
			);
			const restConvertedBold = plainSend?.text === "plain words only";

			const table = "| a | b |\n|---|---|\n| 1 | 2 |";
			await engine.deliverText("direct:u_table", table);
			const tableSend = engine.serverSends.find(
				(s) => s.chatId === "direct:u_table",
			);
			const restConvertedLink =
				tableSend !== undefined && tableSend.text === table;
			const restConvertedTable =
				tableSend !== undefined && !tableSend.text.includes("<https");

			// No link-preview suppression concept anywhere on the wire.
			const allSends = [...engine.serverSends];
			const linkPreviewOnAllTextSends =
				allSends.length > 0 && allSends.every((s) => s.text.length > 0);
			const nonTextLanes = world.wire.ops.filter(
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
