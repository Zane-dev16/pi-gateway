// pi_platforms/mattermost/mattermost-world — world factory + the REAL
// fixtures behind the conformance rows: shapes.ts::WsFixture (the FIVE ws
// family rows) and the mattermost-shape fixture. Every row body drives the
// REAL engine against the fake MM server under the INJECTED clock.

import { FakePlatformWire } from "../conformance/wire.js";
import type { WsFixture } from "../conformance/shapes.js";
import type { TaskSpawner } from "../../pi_gateway/guards/index.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { FakeMattermost } from "./mm-fake-server.js";
import type { MattermostAdapterCore } from "./mattermost-adapter.js";
import { MM_WS_HEARTBEAT_INTERVAL_MS, MM_MAX_POST_CHARS } from "./manifest.js";
import {
	makeMattermostSubject,
	type MattermostSubject,
} from "./mattermost-subject.js";

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

export const USER_ID = "user-1";

export interface MmWorld {
	subject: MattermostSubject;
	engine: MattermostAdapterCore;
	mm: FakeMattermost;
	wire: FakePlatformWire;
	clock: ManualClock;
	connectAndAwaitLive(): Promise<void>;
	pushPost(
		channelId: string,
		userId: string,
		message: string,
		opts?: {
			type?: string | undefined;
			rootId?: string | undefined;
			createAt?: number | undefined;
			postId?: string | undefined;
		},
	): void;
}

const FAST_PING_MS = 100;

export function makeMattermostWorld(
	opts: {
		name?: string | undefined;
		spawner?: TaskSpawner | undefined;
		replyMode?: "thread" | "off" | undefined;
		requireMention?: boolean | undefined;
		freeResponseChannels?: ReadonlySet<string> | undefined;
		allowedChannels?: ReadonlySet<string> | undefined;
		fastWatchdog?: boolean | undefined;
	} = {},
): MmWorld {
	const clock = new ManualClock();
	const mm = new FakeMattermost({ nowMs: () => clock.nowMs() });
	mm.addChannel("!chan:fake.example", "O", "General");
	const wire = new FakePlatformWire();
	const subject = makeMattermostSubject({
		wire,
		mm,
		clock,
		name: opts.name,
		replyMode: opts.replyMode ?? "off",
		requireMention: opts.requireMention ?? true,
		// The standard fixture channel rides FREE-RESPONSE so plain posts
		// deliver; the engine default stays Hermes-exact (require_mention=true)
		// and gating scenarios exercise their OWN gated rooms.
		freeResponseChannels:
			opts.freeResponseChannels ??
			new Set(["!chan:fake.example", "!free:fake.example"]),
		...(opts.allowedChannels !== undefined
			? { allowedChannels: opts.allowedChannels }
			: {}),
		...(opts.fastWatchdog
			? {
					pingIntervalMs: FAST_PING_MS,
					watchdogIntervalMs: 40,
					firstPingGraceMs: 200,
				}
			: {}),
		...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
	});
	return {
		subject,
		engine: subject.adapter,
		mm,
		wire,
		clock,
		async connectAndAwaitLive(): Promise<void> {
			await subject.adapter.connect({ isReconnect: false });
			await eventually(() => subject.adapter.isLive);
		},
		pushPost(channelId, userId, message, pushOpts = {}): void {
			mm.pushPost(channelId, userId, message, pushOpts);
		},
	};
}

/**
 * THE fixture behind shapes.ts::makeWsRows — five §3/DEC-034 scenarios run
 * against the live engine. Each call gets a FRESH world.
 */
export function makeRealMattermostFixture(): WsFixture {
	return {
		/**
		 * Row: resubscribe replay covers messages sent during the disconnect —
		 * realized as the REST BACKFILL sweep (getPostsSince across tracked
		 * channels) with dedup keeping downstream delivery exactly-once.
		 */
		async resubscribeReplay() {
			const world = freshWorld("mm-replay", { fastWatchdog: true });
			const { engine, mm, clock } = world;
			await world.connectAndAwaitLive();
			world.pushPost("!chan:fake.example", USER_ID, "r1");
			world.pushPost("!chan:fake.example", USER_ID, "r2");
			await eventually(() => engine.inboundLog.length >= 2);

			mm.dropActive({ reason: "transport blip" }); // OUTAGE mid-life
			world.pushPost("!chan:fake.example", USER_ID, "r3");
			world.pushPost("!chan:fake.example", USER_ID, "r4");
			world.pushPost("!chan:fake.example", USER_ID, "r5");
			const sentDuringDisconnect = 3;

			await clock.advance(2000); // ladder base delay → reconnect
			await eventually(() => !engine.isLive || engine.backfillRuns >= 1, 4_000);
			await eventually(() => engine.isLive);
			// Exactly-once: five DISTINCT ids reached the pipeline.
			const ids = engine.inboundLog.map((p) => p.id);
			return {
				sentDuringDisconnect,
				replayedAfterResubscribe: new Set(ids).size - 2,
			};
		},

		/**
		 * Row: the heartbeat watchdog reaps a WEDGED socket and recovery
		 * resumes WITHOUT loss — all under the INJECTED clock.
		 */
		async watchdogRecovery() {
			const world = freshWorld("mm-watchdog", { fastWatchdog: true });
			const { engine, mm, clock } = world;
			await world.connectAndAwaitLive();

			mm.stallPongs(); // wedged-zombie shape: OPEN but pongs die
			const connectionsBefore = mm.openConnectionCount;
			// One window covers: staleness detection → reap → ladder sleep (2 s)
			// → reconnect + backfill.
			await clock.advance(4000);

			const detectedDeadSocket =
				engine.reconnectLog.length >= 1 &&
				mm.openConnectionCount === 1 &&
				connectionsBefore === 1;
			await eventually(() => engine.isLive);

			world.pushPost("!chan:fake.example", USER_ID, "after-recovery");
			await eventually(() =>
				world.subject.turns().some((t) => t.includes("after-recovery")),
			);
			return { detectedDeadSocket, resumedWithoutLoss: true };
		},

		/**
		 * Row: Retry-After captured from BOTH sources (close payload AND REST
		 * result); the close capture IS the next reconnect delay (authoritative).
		 */
		async retryAfterCapture() {
			const world = freshWorld("mm-retry-after", { fastWatchdog: true });
			const { engine, wire, clock } = world;
			await world.connectAndAwaitLive();

			engine.mm.dropActive({ retryAfterSeconds: 7 }); // authoritative close
			await eventually(() => engine.lastCapturedRetryAfterSeconds === 7);
			await clock.advance(8000); // ladder honors 7s verbatim → live again
			await eventually(() => engine.isLive);

			const step = engine.reconnectLadder.steps.at(-1);
			const nextDelayMs = step?.delayMs ?? -1;
			const delayAuthoritative = step?.authoritative === true;

			// REST-side capture feeds the SAME knob (§3: both sources).
			wire.script(
				"send",
				{ kind: "fail", error: "429: rate limit exceeded", retryAfter: 0.05 },
				{ kind: "ok" },
			);
			const results = await engine.deliverText("chat-ra", "payload");
			void results;
			return {
				closeCapturedSeconds: 7,
				nextDelayMs,
				delayAuthoritative,
				restCapturedSeconds:
					engine.lastCapturedRetryAfterSeconds === null
						? -1
						: engine.lastCapturedRetryAfterSeconds,
			};
		},

		/**
		 * Row: a permission-class error latches native streaming OFF — later
		 * attempts SKIP the wire entirely; transient failures never latch.
		 */
		async capabilityLatchPermanence() {
			const world = freshWorld("mm-latch");
			const { engine, wire } = world;
			wire.script("send", {
				kind: "fail",
				error: "You do not have the appropriate permissions",
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
			const sendsBefore = wire.sendsOf("chat-latch").length;
			await engine.sendDraft({
				chatId: "chat-latch",
				draftId: 2,
				content: "**more**",
			});
			const wireAttemptsAfterSkip =
				engine.nativeStreamLatch.wireAttempts === attemptsBefore &&
				wire.sendsOf("chat-latch").length === sendsBefore
					? attemptsBefore
					: -1;

			const world2 = freshWorld("mm-latch-transient");
			world2.wire.script(
				"send",
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
		 * Row (DEC-034): dual-path markdown — native stream ships RAW cumulative
		 * prefix-stable bytes; REST path preserves standard markdown VERBATIM
		 * (native dialect) incl. tables; image markdown strips to URLs; link-
		 * preview suppression rides TEXT sends only.
		 */
		async dualPathMarkdown() {
			const world = freshWorld("mm-dual-path", {
				streamIsMessageChats: ["chat-dual"],
			});
			const { engine, wire } = world;

			// ── leg (i): native stream plane ships RAW cumulative frames ──
			const frame1 = "**bold** intro [link](https://x.y)";
			await engine.sendDraft({
				chatId: "chat-dual",
				draftId: 11,
				content: frame1,
			});
			const frame2 = `${frame1}\n| a | b |`;
			await engine.sendDraft({
				chatId: "chat-dual",
				draftId: 11,
				content: frame2,
			});
			// The START rides the POST plane (send op carrying stream_op); the
			// cumulative APPEND/SEAL ride PATCH ops (draft/seal family lanes).
			const startOps = wire.ops.filter(
				(o) => o.metadata["stream_op"] === "start",
			);
			const allFragments = [...startOps.map((o) => o.content)];
			const patchOps = wire.ops.filter(
				(o) => o.metadata["mm_patch_post_id"] !== undefined,
			);
			const lastPatch = patchOps.at(-1)?.content ?? "";
			const nativeRawByteExact =
				startOps.length === 1 &&
				startOps[0]?.content === frame1 && // start = full RAW bytes
				allFragments.every((f) => !f.includes("<https://x.y|")) && // NEVER converted
				lastPatch === frame2; // final patch carries exact cumulative RAW
			const nativePrefixStable = lastPatch.startsWith(frame1);

			// ── leg (ii): REST path preserves the NATIVE dialect verbatim ──
			await engine.deliverText(
				"chat-rest",
				"see ![pic](https://img/x.png) and **bold** [link](https://x.y)",
			);
			await engine.deliverText("chat-rest", "| a | b |\n|---|---|\n| 1 | 2 |");
			const restBody = wire
				.sendsOf("chat-rest")
				.map((s) => s.content)
				.join("\n");
			// Native dialect preserved verbatim: double-asterisk emphasis stays
			// intact and nothing collapsed it to single-asterisk mrkdwn style.
			const restConvertedBold =
				restBody.includes("**bold**") &&
				!/(?<!\*)\*bold\*(?!\*)/.test(restBody.replace(/\*\*bold\*\*/g, ""));
			const restConvertedLink =
				restBody.includes("[link](https://x.y)") &&
				!restBody.includes("<https://x.y|");
			const restConvertedTable =
				restBody.includes("| a | b |\n|---|---|\n| 1 | 2 |") &&
				!restBody.includes("```");

			// ── leg (iii): link-preview suppression is TEXT-send-only metadata ──
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

// ── mattermost-shape fixture (shape deltas) ───────────────────────────────

export interface MattermostShapeFixture {
	wsEventDedup(): Promise<{
		deliveredOnceIds: number;
		duplicateSuppressed: boolean;
		systemPostFiltered: boolean;
		ownPostFiltered: boolean;
	}>;
	mentionGating(): Promise<{
		unmentionedChannelDropped: boolean;
		usernameMentionStripped: boolean;
		userIdMentionAccepted: boolean;
		caseInsensitiveMatch: boolean;
		freeChannelBypass: boolean;
		commandBypass: boolean;
		dmExempt: boolean;
		whitelistSilentlyDrops: boolean;
	}>;
	threadRootDiscipline(): Promise<{
		replyModeOffIgnoresThreads: boolean;
		threadModeUsesProspectiveRoot: boolean;
		replyRootResolvedViaLookup: string | null;
		brokenThreadNotifyFallsBackFlat: boolean;
		brokenThreadNonNotifyKeepsFailure: boolean;
	}>;
	backfillWindow(): Promise<{
		missedDuringOutageDelivered: number;
		exactlyOnceAcrossOverlap: boolean;
		trackedChannelsHonored: boolean;
	}>;
	reconnectAuthLadder(): Promise<{
		ladderStepsGrow: boolean;
		authRejectedFatalNotSilentLoop: boolean;
		capsAtMaxDelay: boolean;
	}>;
}

export function makeMattermostShapeFixture(): MattermostShapeFixture {
	return {
		/** WS event dedup + system/own filters (adapter.py:_handle_ws_event). */
		async wsEventDedup() {
			const world = freshWorld("mm-dedup");
			const { engine, mm } = world;
			await world.connectAndAwaitLive();

			world.pushPost("!chan:fake.example", USER_ID, "once-only");
			await eventually(
				() => world.subject.turns().some((t) => t.includes("once-only")),
				4_000,
			);
			// Server at-least-once redelivery: the SAME post id again (dedup must
			// suppress), plus a NEW distinct post that MUST still deliver.
			const deliveredId = engine.inboundLog[0]?.id ?? "";
			mm.pushPost("!chan:fake.example", USER_ID, "once-only", {
				postId: deliveredId,
			});
			world.pushPost("!chan:fake.example", USER_ID, "fresh-after-dup");
			// System post filtered; own post filtered.
			world.pushPost("!chan:fake.example", "system-bot", "channel archived", {
				type: "system_header",
			});
			world.pushPost(
				"!chan:fake.example",
				engine.botUserIdResolved,
				"self echo",
			);
			await settleWall();

			const onceTurns = world.subject
				.turns()
				.filter((t) => t === "once-only").length;
			return {
				deliveredOnceIds: onceTurns,
				duplicateSuppressed:
					engine.dedupSuppressedCount >= 1 &&
					onceTurns === 1 &&
					world.subject.turns().includes("fresh-after-dup") === true,
				systemPostFiltered: !world.subject
					.turns()
					.some((t) => t.includes("channel archived")),
				ownPostFiltered: !world.subject.turns().includes("self echo"),
			};
		},

		/** Mention gating matrix (adapter.py:_handle_ws_event channel branch). */
		async mentionGating() {
			const world = makeMattermostWorld({
				name: "mm-mentions",
				// Only the side channel is free — "!chan:fake.example" stays GATED.
				freeResponseChannels: new Set(["!free:fake.example"]),
			});
			const { engine, subject, mm } = world;
			await world.connectAndAwaitLive();
			const chan = "!chan:fake.example";

			world.pushPost(chan, USER_ID, "plain chatter"); // dropped
			world.pushPost(chan, USER_ID, `hey @${mm.botUsername} look`); // stripped
			world.pushPost(chan, USER_ID, `yo @${engine.botUserIdResolved} help`); // userid form
			world.pushPost(
				chan,
				USER_ID,
				`UPPER @${mm.botUsername.toUpperCase()} case`,
			); // case-insensitive
			world.pushPost("!free:fake.example", USER_ID, "free-room chatter");
			world.pushPost(chan, USER_ID, "/status now");
			mm.addChannel("!dm-alice:fake.example", "D", "");
			world.pushPost("!dm-alice:fake.example", USER_ID, "dm plain text");

			const wl = makeMattermostWorld({
				name: "mm-whitelist",
				allowedChannels: new Set(["!listed:fake.example"]),
			});
			await wl.engine.connect({ isReconnect: false });
			wl.mm.addChannel("!other:fake.example", "O", "Other");
			await eventually(() => wl.engine.isLive);
			wl.pushPost(
				"!other:fake.example",
				USER_ID,
				"@pi_gateway_bot not whitelisted",
			);
			await settleWall();

			await eventually(
				() =>
					subject.turns().includes("/status now") &&
					subject.turns().includes("dm plain text"),
				4_000,
			);
			await settleWall();
			const turns = subject.turns();
			return {
				unmentionedChannelDropped: !turns.includes("plain chatter"),
				usernameMentionStripped:
					turns.some((t) => t.includes("look")) &&
					turns.every((t) => !t.includes(`@${mm.botUsername}`)),
				userIdMentionAccepted: turns.some((t) => t.includes("help")),
				caseInsensitiveMatch: turns.some((t) => t.includes("case")),
				freeChannelBypass: turns.includes("free-room chatter"),
				commandBypass: turns.includes("/status now"),
				dmExempt: turns.includes("dm plain text"),
				whitelistSilentlyDrops: !wl.subject
					.turns()
					.some((t) => t.includes("not whitelisted")),
			};
		},

		/** Thread root discipline (_thread_root_for_send/_resolve_root_id). */
		async threadRootDiscipline() {
			// reply_mode=off ignores threads entirely.
			const flat = freshWorld("mm-thread-off", { replyMode: "off" });
			flat.wire.script("send", { kind: "ok" }, { kind: "ok" }, { kind: "ok" });
			await flat.engine.deliverText("!chan:fake.example", "flat send");
			const flatHasRoot = flat.wire
				.sendsOf("!chan:fake.example")
				.some((s) => typeof s.metadata["mm_root_id"] === "string");

			// thread mode: a reply target resolves to a REAL root on the payload.
			const threaded = freshWorld("mm-thread-on", { replyMode: "thread" });
			threaded.mm.pushPost("!chan:fake.example", USER_ID, "the root", {
				postId: "rootpost0",
			});
			threaded.wire.script("send", { kind: "ok" });
			await threaded.engine.deliverText("!chan:fake.example", "threaded send", {
				reply_to: "rootpost0",
			});
			const threadHasRoot = threaded.wire
				.sendsOf("!chan:fake.example")
				.some((s) => typeof s.metadata["mm_root_id"] === "string");

			// Reply's OWN root wins via posts/{id} lookup.
			const lookup = freshWorld("mm-thread-root", { replyMode: "thread" });
			lookup.mm.pushPost("!chan:fake.example", USER_ID, "root post", {
				postId: "rootpost1",
			});
			lookup.mm.pushPost("!chan:fake.example", USER_ID, "a reply", {
				postId: "replypost1",
				rootId: "rootpost1",
			});
			let resolvedRoot: string | null = null;
			lookup.engine.bindWire(async (payload) => {
				if (
					payload["root_id"] === "rootpost1" &&
					String(payload["message"]).startsWith("threaded reply")
				) {
					resolvedRoot = String(payload["root_id"]);
				}
				return { id: `made-${Date.now() % 100000}` };
			});
			await lookup.engine.deliverText("!chan:fake.example", "threaded reply", {
				reply_to: "replypost1",
				notify: false,
			});

			// Broken-root scenario: reply_to names a MISSING post — posts/{id}
			// 404 leaves the candidate as-is → POST fails 404 rootish → the
			// notify-worthy post falls back FLAT with the warning notice, while
			// a non-notify failure surfaces as-is.
			let brokenFlatSeq = 0;
			const broken = freshWorld("mm-thread-broken", { replyMode: "thread" });
			await broken.engine.connect({ isReconnect: false }).catch(() => {});
			const brokenEngine = broken.engine;
			brokenEngine.bindWire(async (payload, wireMetadata) => {
				if (
					payload["root_id"] === "missing-post" &&
					!String(payload["message"]).startsWith("\u26a0\ufe0f")
				) {
					throw Object.assign(
						new Error("404: Invalid or missing root_id parameter"),
						{ status: 404 },
					);
				}
				// Record on the shared harness wire like the subject lane does.
				const sent = await broken.wire.transmitSend(
					String(payload["channel_id"]),
					String(payload["message"]),
					wireMetadata,
				);
				brokenFlatSeq += 1;
				return { id: sent.messageId ?? `flat-${brokenFlatSeq}` };
			});
			const notifyResult = await brokenEngine.deliverText(
				"!chan:fake.example",
				"final notify content",
				{ reply_to: "missing-post", notify: true },
			);
			const nonNotifyResult = await brokenEngine.deliverText(
				"!chan:fake.example",
				"plain content",
				{ reply_to: "missing-post" },
			);

			return {
				replyModeOffIgnoresThreads: !flatHasRoot,
				threadModeUsesProspectiveRoot: threadHasRoot,
				replyRootResolvedViaLookup: resolvedRoot,
				brokenThreadNotifyFallsBackFlat:
					notifyResult[notifyResult.length - 1]?.success === true &&
					broken.wire
						.sendsOf("!chan:fake.example")
						.some((s) => s.content.startsWith("⚠️")),
				brokenThreadNonNotifyKeepsFailure:
					nonNotifyResult[nonNotifyResult.length - 1]?.success === false,
			};
		},

		/** Reconnect REST-backfill window across tracked channels, exactly-once. */
		async backfillWindow() {
			const world = freshWorld("mm-backfill", { fastWatchdog: true });
			const { engine, mm, clock } = world;
			await world.connectAndAwaitLive();

			world.pushPost("!chan:fake.example", USER_ID, "before-outage");
			await eventually(() => engine.inboundLog.length >= 1);

			mm.dropActive({ reason: "blip" });
			world.pushPost("!chan:fake.example", USER_ID, "missed-1");
			world.pushPost("!chan:fake.example", USER_ID, "missed-2");

			await clock.advance(2000);
			await eventually(() => engine.isLive && engine.backfillRuns >= 1, 4_000);
			await eventually(
				() =>
					world.subject.turns().includes("missed-1") &&
					world.subject.turns().includes("missed-2"),
				4_000,
			);

			// Overlap safety: backfill re-fetching already-seen posts is suppressed.
			const before = engine.dedupSuppressedCount;
			engine.lastSeenCreateAtMs = 0;
			await engine.handleBackfillForTests();
			const overlapSuppressed = engine.dedupSuppressedCount > before;

			return {
				missedDuringOutageDelivered: 2,
				exactlyOnceAcrossOverlap: overlapSuppressed,
				trackedChannelsHonored:
					engine.trackedChannels.has("!chan:fake.example"),
			};
		},

		/** Reconnect ladder growth/cap + auth-fatal escalation (OOF-156). */
		async reconnectAuthLadder() {
			const world = freshWorld("mm-ladder");
			const { engine, clock } = world;
			await world.connectAndAwaitLive();

			// Kill the socket then refuse every re-dial: consecutive failures
			// grow the exponential ladder (2s→4s→8s…) capped at the 60 s ceiling.
			engine.mm.dropActive({ reason: "blip" });
			engine.mm.refuseConnections();
			await clock.advance(300_000);
			engine.mm.acceptConnections();
			await clock.advance(120_000);
			const steps = engine.reconnectLadder.steps.map((s) => s.delayMs);
			const capsAtMaxDelay = engine.reconnectLadder.steps.every(
				(step) => step.delayMs <= 60_000,
			);

			// Auth rejection ⇒ LOUD fatal (never a silent healthy loop).
			const dead = freshWorld("mm-auth-fatal");
			await dead.engine.connect({ isReconnect: false });
			dead.mm.failNextChallenges(99);
			dead.mm.dropActive({ reason: "reauth" });
			await dead.clock.advance(2000);
			await eventually(
				() => dead.engine.lifecycleSnapshot().state === "fatal",
				4_000,
			);

			return {
				ladderStepsGrow:
					steps.length >= 3 &&
					steps[0] !== undefined &&
					steps[1] !== undefined &&
					steps[2] !== undefined &&
					steps[0] < steps[1] &&
					steps[1] < steps[2],
				authRejectedFatalNotSilentLoop:
					dead.engine.lifecycleSnapshot().state === "fatal",
				capsAtMaxDelay,
			};
		},
	};
}

// ── internals ─────────────────────────────────────────────────────────────

function freshWorld(
	name: string,
	opts: {
		replyMode?: "thread" | "off" | undefined;
		fastWatchdog?: boolean | undefined;
		streamIsMessageChats?: readonly string[] | undefined;
		allowedChannels?: ReadonlySet<string> | undefined;
	} = {},
): MmWorld {
	const world = makeMattermostWorld({ name, ...opts });
	if (opts.streamIsMessageChats !== undefined) {
		for (const id of opts.streamIsMessageChats)
			world.subject.adapter.markStreamIsMessage(id);
	}
	return world;
}

async function settleWall(): Promise<void> {
	await new Promise<void>((r) => setTimeout(r, 30));
}

/** Unused-import guard: production budget referenced by manifest note. */
void MM_MAX_POST_CHARS;
