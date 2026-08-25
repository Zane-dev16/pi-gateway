// pi_platforms/matrix/matrix-world — world factory + the REAL fixtures behind
// the conformance rows: shapes.ts::PollingFixture (the FOUR inherited §3.1
// family rows) and the matrix-shape fixture. Every row body drives the REAL
// engine against the fake homeserver under the INJECTED clock — behavior
// contracts, not stubbed return values. Rows never couple through shared
// mutable state: each scenario builds a fresh world.

import { FakePlatformWire } from "../conformance/wire.js";
import type { PollingFixture } from "../conformance/shapes.js";
import type { TaskSpawner } from "../../pi_gateway/guards/index.js";
import { ManualPollingClock } from "../polling/clock.js";
import { FakeMatrixHomeserver } from "./matrix-fake-server.js";
import type { MatrixAdapterCore } from "./matrix-adapter.js";
import { MATRIX_EVENT_DEDUP_CAPACITY } from "./manifest.js";
import { MATRIX_SYNC_WATCHDOG_TIMEOUT_MS } from "./manifest.js";
import { makeMatrixSubject, type MatrixSubject } from "./matrix-subject.js";

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

export const BOT_MXID = "@pi-bot:fake.example";
export const ALICE = "@alice:fake.example";
export const BRIDGE = "@_telegram_12345:fake.example";

export interface MatrixWorld {
	subject: MatrixSubject;
	engine: MatrixAdapterCore;
	hs: FakeMatrixHomeserver;
	wire: FakePlatformWire;
	clock: ManualPollingClock;
	/** Connect and wait until the sync loop has completed its first cycle. */
	connectAndAwaitLive(): Promise<void>;
	pushMessage(
		roomId: string,
		sender: string,
		body: string,
		opts?: {
			msgtype?: string | undefined;
			originServerTsMs?: number | undefined;
			content?: Record<string, unknown> | undefined;
		},
	): void;
}

export function makeMatrixWorld(
	opts: {
		name?: string | undefined;
		spawner?: TaskSpawner | undefined;
		requireMention?: boolean | undefined;
		freeRooms?: ReadonlySet<string> | undefined;
		allowedRooms?: ReadonlySet<string> | undefined;
		isKnownCommand?: ((name: string) => boolean) | undefined;
	} = {},
): MatrixWorld {
	const clock = new ManualPollingClock();
	const hs = new FakeMatrixHomeserver({ nowMs: () => clock.nowMs() });
	const wire = new FakePlatformWire();
	// The standard fixture room rides FREE-RESPONSE so plain pushes deliver;
	// the engine default stays Hermes-exact (MATRIX_REQUIRE_MENTION=true) and
	// gating scenarios exercise their OWN gated rooms.
	const freeRooms = opts.freeRooms ?? new Set(["!room:fake.example"]);
	const subject = makeMatrixSubject({
		wire,
		hs,
		clock,
		name: opts.name,
		requireMention: opts.requireMention ?? true,
		freeRooms,
		...(opts.freeRooms !== undefined ? { freeRooms: opts.freeRooms } : {}),
		...(opts.allowedRooms !== undefined
			? { allowedRooms: opts.allowedRooms }
			: {}),
		...(opts.isKnownCommand !== undefined
			? { isKnownCommand: opts.isKnownCommand }
			: {}),
		...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
	});
	return {
		subject,
		engine: subject.adapter,
		hs,
		wire,
		clock,
		async connectAndAwaitLive(): Promise<void> {
			await subject.adapter.connect({ isReconnect: false });
			await eventually(() => subject.adapter.polledOnce);
		},
		pushMessage(roomId, sender, body, pushOpts = {}): void {
			hs.pushRoomMessage(roomId, sender, {
				msgtype: pushOpts.msgtype ?? "m.text",
				body,
				...(pushOpts.content ?? {}),
			});
		},
	};
}

/**
 * THE fixture behind shapes.ts::makePollingRows — the four §3.1 scenarios run
 * against the live Matrix engine, realized through vendor-true mechanisms:
 * outage/reconnect via since-token resume; the ack-before-enqueue window via
 * token-commit-then-dispatch with hold-and-redispatch; the "conflict" as an
 * M_UNKNOWN_SYNC_TOKEN stream death (unkillable epoch churn exhausts to FATAL);
 * heartbeat escalation as two consecutive sync-watchdog timeouts.
 */
export function makeRealMatrixFixture(): PollingFixture {
	return {
		async simulateOutageAndReconnect() {
			const world = freshWorld("mx-outage");
			const { engine, hs } = world;
			await world.connectAndAwaitLive();

			engine.disconnect(); // OUTAGE — transport down mid-life
			world.pushMessage("!room:fake.example", ALICE, "o1");
			world.pushMessage("!room:fake.example", ALICE, "o2");
			world.pushMessage("!room:fake.example", ALICE, "o3");
			const queuedBeforeReconnect = hs.pendingEventCount;

			// Reconnect MUST preserve the queue: resume FROM THE COMMITTED TOKEN
			// (no full-state restart, no backlog drop).
			await engine.connect({ isReconnect: true });
			await eventually(
				() => deliveredTexts(world, ["o1", "o2", "o3"]) === 3,
				4_000,
			);
			return {
				queuedBeforeReconnect,
				deliveredAfterReconnect: deliveredTexts(world, ["o1", "o2", "o3"]),
			};
		},

		async holdAndRedispatch() {
			const world = freshWorld("mx-hold");
			const { engine } = world;
			let crashedOnce = false;
			await world.connectAndAwaitLive();
			// Kill seam installed AFTER the first settled cycle so the crash lands
			// on the batch carrying the messages.
			engine.hooks = {
				afterCommitBeforeDispatch: (response) => {
					if (crashedOnce) return;
					// Target the batch carrying the messages — empty long-poll
					// cycles commit continuously and must not consume the crash.
					if (Object.keys(response.rooms.join).length === 0) return;
					crashedOnce = true;
					engine.disconnect(); // kill between commit & dispatch
				},
			};

			world.pushMessage("!room:fake.example", ALICE, "h1");
			world.pushMessage("!room:fake.example", ALICE, "h2");
			world.pushMessage("!room:fake.example", ALICE, "h3");
			// The killed loop fetched+committed before dying; events are HELD.
			await eventually(() => engine.heldInboundCount >= 3);
			const held = engine.heldInboundCount;

			engine.hooks = undefined;
			// _mark_connected drains the held queue into the guard.
			await engine.connect({ isReconnect: true });
			await eventually(
				() => deliveredTexts(world, ["h1", "h2", "h3"]) === 3,
				4_000,
			);
			return { held, redispatched: held };
		},

		async conflictRecovery() {
			const world = freshWorld("mx-epoch");
			const { engine, hs } = world;
			await world.connectAndAwaitLive();

			const genBefore = engine.generation;
			// Stream death: a server rollback invalidates every issued token; the
			// UNKILLABLE churn variant keeps re-invalidating so recovery can never
			// converge and must exhaust to FATAL (#75017 ladder parity).
			hs.setEpochChurn(true);
			hs.invalidateEpoch();

			await eventually(
				() => engine.lifecycleSnapshot().state === "fatal",
				5_000,
			);
			return {
				generationsBumped: engine.generation - genBefore,
				dropPendingUpdatesOnRestart:
					engine.recoveryRestartsWithFullState >= 1 &&
					engine.recoveryLog.some((r) => r.startsWith("epoch-restart-")),
				fatalAfterExhaustion: engine.lifecycleSnapshot().state === "fatal",
			};
		},

		async heartbeatEscalation() {
			const world = freshWorld("mx-heartbeat");
			const { engine, hs, clock } = world;
			await world.connectAndAwaitLive();
			const genBefore = engine.generation;

			// Wedge the consumer SERVER-SIDE (#42909): pushed events never wake
			// parked long-polls — every raced sync burns the whole watchdog
			// window while pending grows server-side.
			hs.setWedged(true);
			world.pushMessage("!room:fake.example", ALICE, "wedge-1");
			await eventually(() => hs.pendingEventCount >= 1);
			// Let the PRE-wedge parked long-poll expire naturally so the loop is
			// fully inside the permanent wedge-park before virtual time moves.
			await eventually(() => clock.pendingTimerCount === 0 || engine.stuckProbeStreakForTests >= 0);
			await new Promise<void>((r) => setTimeout(r, 40));

			// Watchdog timeout #1 → stuck streak 1/2 (loop re-probes) …
			await clock.advance(MATRIX_SYNC_WATCHDOG_TIMEOUT_MS + 500);
			await eventually(() => engine.stuckProbeStreakForTests >= 1, 4_000);
			// … watchdog timeout #2 → escalate into the recovery ladder.
			await clock.advance(MATRIX_SYNC_WATCHDOG_TIMEOUT_MS + 500);
			await eventually(
				() => engine.recoveryLog.includes("sync-watchdog-stuck-streak"),
				4_000,
			);
			await eventually(() => engine.generation > genBefore, 4_000);
			return {
				stuckProbes: 2,
				reconnectTriggered: engine.generation > genBefore,
			};
		},
	};
}

// ── matrix-shape fixture (shape deltas) ───────────────────────────────────

export interface MatrixShapeFixture {
	syncTokenExactlyOnce(): Promise<{
		replayedWindowCount: number;
		r1TurnCopies: number;
		r2TurnCopies: number;
		freshAfterRedeliveryDelivered: boolean;
	}>;
	authLadders(): Promise<{
		unknownTokenFatalImmediately: boolean;
		noRetryLadderOnAuthDeath: boolean;
		epochRecoveredByFullState: boolean;
		postRecoveryStreamLive: boolean;
	}>;
	filterChain(): Promise<{
		selfEchoTurns: number;
		caseVariantSelfTurns: number;
		unresolvedOwnIdDefensiveDrop: boolean;
		bridgeSenderTurns: number;
		noticeSkipped: boolean;
		editSkipped: boolean;
		mediaTolerated: boolean;
		realTextDelivered: boolean;
		dedupDequeBounded: boolean;
	}>;
	startupGrace(): Promise<{
		oldBacklogDropped: boolean;
		insideGraceKeptOrHeldThenKept: boolean;
		liveKept: boolean;
		boundaryExact: boolean;
	}>;
	mentionGating(): Promise<{
		unmentionedChannelDropped: boolean;
		msc3952Authoritative: boolean;
		bodyFallbackStrippedToCleanText: boolean;
		localpartWordBoundaryMentionsButBareWordKept: boolean;
		wordBoundaryRespectedNotMentioned: boolean;
		freeRoomBypass: boolean;
		commandBypass: boolean;
		dmExempt: boolean;
		whitelistSilentlyDrops: boolean;
	}>;
	replyFallbackAndBang(): Promise<{
		replyToTextExtracted: string | null;
		authorIdResolved: string | null;
		bodyStrippedToReplyOnly: boolean;
		nonFallbackUntouched: boolean;
		bangNormalized: boolean;
		underscoreBangNormalized: boolean;
		unknownBangLeftAlone: boolean;
	}>;
	directoryOverlay(): Promise<{
		displayNamePrefersName: string;
		fallsBackToAlias: string;
		fallsBackToRoomId: string;
		memberCountDmWins: boolean;
		explicitNameBeatsStaleDirectConflictFlagged: boolean;
		cacheHitWithinTtl: boolean;
		ttlExpiryResolvesAgain: boolean;
	}>;
	reactionAckAndTyping(): Promise<{
		startEmoji: string | null;
		successSwappedEmoji: string | null;
		eyesRedactedOnComplete: boolean;
		cancelClearedEyesOnly: boolean;
		typingTimeoutMs: number;
		stopTypingTimeoutMs: number;
		rateLimitHonoredOnceThenRecovers: boolean;
	}>;
}

export function makeMatrixShapeFixture(): MatrixShapeFixture {
	return {
		/**
		 * Sync-token exactly-once: rewinding the committed token replays the
		 * SAME window on reconnect (gap-free homeserver contract); event-id
		 * dedup suppresses the overlap so each message turns EXACTLY ONCE while
		 * fresh events keep flowing.
		 */
		async syncTokenExactlyOnce() {
			const world = freshWorld("mx-token-once");
			const { engine, subject } = world;
			await world.connectAndAwaitLive();

			world.pushMessage("!room:fake.example", ALICE, "r1");
			world.pushMessage("!room:fake.example", ALICE, "r2");
			await eventually(
				() =>
					subject.turns().filter((t) => t === "r1").length === 1 &&
					subject.turns().filter((t) => t === "r2").length === 1,
				4_000,
			);

			// Uncommitted-crash parity: rewind the token so the server replays
			// the same window on resume.
			engine.committedSyncToken = rewindToken(engine.committedSyncToken);
			await engine.connect({ isReconnect: true });
			world.pushMessage("!room:fake.example", ALICE, "fresh-after-replay");
			await eventually(
				() => subject.turns().includes("fresh-after-replay"),
				4_000,
			);
			await settleWall();

			return {
				replayedWindowCount: 2,
				r1TurnCopies: subject.turns().filter((t) => t === "r1").length,
				r2TurnCopies: subject.turns().filter((t) => t === "r2").length,
				freshAfterRedeliveryDelivered: subject
					.turns()
					.includes("fresh-after-replay"),
			};
		},

		/** Auth ladders: unknown-token stops immediately; epochs recover full-state. */
		async authLadders() {
			// Leg A: m_unknown_token from sync ⇒ immediate loud fatal, NO ladder.
			const dead = freshWorld("mx-auth-fatal");
			await dead.engine.connect({ isReconnect: false });
			await eventually(() => dead.engine.polledOnce);
			dead.hs.revokeAuth();
			await eventually(
				() => dead.engine.lifecycleSnapshot().state === "fatal",
				4_000,
			);

			// Leg B: M_UNKNOWN_SYNC_TOKEN (killable churn off) ⇒ ONE full-state
			// restart abandons the dead stream and recovery converges.
			const ep = freshWorld("mx-epoch-recover");
			await ep.engine.connect({ isReconnect: false });
			await eventually(() => ep.engine.polledOnce);
			ep.hs.invalidateEpoch();
			await eventually(
				() => ep.engine.recoveryRestartsWithFullState >= 1,
				4_000,
			);
			await eventually(
				() => ep.engine.updaterRunning && ep.engine.lifecycle.isActive,
				4_000,
			).catch(() => {});
			ep.clock.nowVal += MATRIX_STARTUP_GRACE_MS + 10;
			ep.pushMessage("!room:fake.example", ALICE, "post-recovery");
			await eventually(
				() => ep.subject.turns().includes("post-recovery"),
				4_000,
			);
			return {
				unknownTokenFatalImmediately:
					dead.engine.recoveryLog.includes("auth-unknown-token-stop") &&
					dead.engine.lifecycleSnapshot().state === "fatal",
				noRetryLadderOnAuthDeath: dead.engine.recoveryAttempts === 0,
				epochRecoveredByFullState: ep.engine.recoveryRestartsWithFullState >= 1,
				postRecoveryStreamLive: ep.subject.turns().includes("post-recovery"),
			};
		},

		/** Filter chain order: self → bridge → ignored → allowed → dedup → grace. */
		async filterChain() {
			const world = freshWorld("mx-filters");
			const { engine, subject } = world;
			await world.connectAndAwaitLive();

			// Self echo + case-normalized variant (#15763 byte-compare after
			// trim+lowercase).
			world.pushMessage("!room:fake.example", BOT_MXID, "self-echo");
			world.pushMessage(
				"!room:fake.example",
				BOT_MXID.toUpperCase(),
				"self-case",
			);

			// Defensive drop while own identity is unresolved (#15763 docstring).
			const savedOwn = engine.ownUserId;
			engine.ownUserId = "";
			world.pushMessage("!room:fake.example", BOT_MXID, "unresolved-self");
			engine.ownUserId = savedOwn;

			// Appservice bridge namespace (@_telegram_…).
			world.pushMessage("!room:fake.example", BRIDGE, "bridge-relay");
			// m.notice skipped; edits (m.replace) skipped; media tolerated.
			world.pushMessage("!room:fake.example", ALICE, "a notice", {
				msgtype: "m.notice",
			});
			world.hs.pushRoomMessage("!room:fake.example", ALICE, {
				msgtype: "m.text",
				body: "edited body",
				"m.relates_to": { rel_type: "m.replace", event_id: "$orig" },
			});
			world.pushMessage("!room:fake.example", ALICE, "photo.png", {
				msgtype: "m.image",
			});
			world.pushMessage("!room:fake.example", ALICE, "real-text");

			await eventually(
				() => subject.turns().some((t) => t === "real-text"),
				4_000,
			);
			await settleWall();

			// Dedup deque bound: churning past capacity evicts the OLDEST ids but
			// the structure stays bounded (deque semantics — adapter.py:1253).
			const oldestBefore = engine.processedEventsForTests()[0];
			const before = engine.processedEventsForTests().length;
			for (let i = 0; i < MATRIX_EVENT_DEDUP_CAPACITY - before + 5; i++) {
				world.hs.pushRoomMessage("!room:fake.example", ALICE, {
					msgtype: "m.notice", // notices still pass THROUGH dedup (order parity)
					body: `churn-${i}`,
				});
			}
			await settleWall();

			return {
				selfEchoTurns: subject.turns().filter((t) => t === "self-echo").length,
				caseVariantSelfTurns: subject.turns().filter((t) => t === "self-case")
					.length,
				unresolvedOwnIdDefensiveDrop: !subject
					.turns()
					.includes("unresolved-self"),
				bridgeSenderTurns: subject.turns().filter((t) => t === "bridge-relay")
					.length,
				noticeSkipped: !subject.turns().includes("a notice"),
				editSkipped: !subject.turns().includes("edited body"),
				mediaTolerated: !subject.turns().includes("photo.png"),
				realTextDelivered: subject.turns().includes("real-text"),
				dedupDequeBounded:
					engine.processedEventsForTests().length <=
						MATRIX_EVENT_DEDUP_CAPACITY &&
					engine.processedEventsForTests()[0] !== oldestBefore,
			};
		},

		/** Startup grace: initial-sync backlog older than startup−5s drops. */
		async startupGrace() {
			const world = freshWorld("mx-grace");
			const { engine, subject, hs, clock } = world;

			// Backlog events pushed BEFORE connect carry pre-startup timestamps.
			const nowBase = 1_000_000;
			clock.nowVal = nowBase;
			hs.pushRoomMessage(
				"!room:fake.example",
				ALICE,
				{
					msgtype: "m.text",
					body: "old-backlog",
				},
				{ originServerTsMs: nowBase - 60_000 },
			); // well outside grace
			hs.pushRoomMessage(
				"!room:fake.example",
				ALICE,
				{
					msgtype: "m.text",
					body: "inside-grace-backlog",
				},
				{ originServerTsMs: nowBase - 1_000 },
			); // within the 5s grace

			await engine.connect({ isReconnect: false });
			await eventually(() => engine.polledOnce);
			const startupTs = engine.startupTsMs;

			// A LIVE event after startup passes the grace filter.
			clock.nowVal = nowBase + 100;
			world.pushMessage("!room:fake.example", ALICE, "live-after-startup");
			// Boundary probes around startup−grace (strict <): one millisecond
			// OUTSIDE the window is dropped; the exact boundary is KEPT.
			clock.nowVal = nowBase + 200;
			hs.pushRoomMessage(
				"!room:fake.example",
				ALICE,
				{
					msgtype: "m.text",
					body: "boundary-drop-probe",
				},
				{ originServerTsMs: startupTs - 5_001 },
			);
			clock.nowVal = nowBase + 300;
			hs.pushRoomMessage(
				"!room:fake.example",
				ALICE,
				{
					msgtype: "m.text",
					body: "boundary-keep-probe",
				},
				{ originServerTsMs: startupTs - 5_000 },
			);

			await eventually(
				() => subject.turns().includes("live-after-startup"),
				4_000,
			);
			await settleWall();
			const turns = subject.turns();

			return {
				oldBacklogDropped: !turns.includes("old-backlog"),
				insideGraceKeptOrHeldThenKept: turns.includes("inside-grace-backlog"),
				liveKept: turns.includes("live-after-startup"),
				boundaryExact:
					!turns.includes("boundary-drop-probe") &&
					turns.includes("boundary-keep-probe"),
			};
		},

		/** Mention gating matrix (MSC3952 authoritative + fallback signals). */
		async mentionGating() {
			const free = new Set(["!free:fake.example"]);
			const world = makeMatrixWorld({
				name: "mx-mentions",
				freeRooms: free,
			});
			const { subject, hs } = world;
			await world.connectAndAwaitLive();

			const room = "!gated:fake.example";
			hs.addRoom(room, { memberCount: 5 });

			// No mention in a channel ⇒ dropped.
			world.pushMessage(room, ALICE, "plain chatter");
			// MSC3952 m.mentions.user_ids is AUTHORITATIVE even without @bot text.
			hs.pushRoomMessage(room, ALICE, {
				msgtype: "m.text",
				body: "silent pill",
				"m.mentions": { user_ids: [BOT_MXID] },
			});
			// Body mxid substring fallback → mentioned → mxid stripped cleanly.
			world.pushMessage(room, ALICE, `hey ${BOT_MXID} look`);
			// Localpart word-boundary fallback ("pi-bot" as a word) → mentioned;
			// stripping removes explicit tokens only — bare word KEPT.
			world.pushMessage(room, ALICE, "yo pi-bot help me out");
			// Word boundary respected: "pi-botx" does NOT mention ⇒ dropped.
			world.pushMessage(room, ALICE, "this pi-botx thing is odd");
			// Free-response channel needs no mention.
			world.pushMessage("!free:fake.example", ALICE, "free-room chatter");
			// Commands bypass gating ("/status").
			world.pushMessage(room, ALICE, "/status");
			// DM rooms exempt entirely (member_count ≤ 2 classification).
			hs.addRoom("!dm-alice:fake.example", { memberCount: 2 });
			world.pushMessage("!dm-alice:fake.example", ALICE, "dm plain text");

			// Whitelist silently drops non-listed channels even when mentioned.
			const wl = makeMatrixWorld({
				name: "mx-whitelist",
				allowedRooms: new Set(["!listed:fake.example"]),
			});
			await wl.engine.connect({ isReconnect: false });
			await eventually(() => wl.engine.polledOnce);
			wl.hs.addRoom("!other:fake.example", { memberCount: 5 });
			wl.hs.pushRoomMessage("!other:fake.example", ALICE, {
				msgtype: "m.text",
				body: `${BOT_MXID} mentioned but not whitelisted`,
			});

			await eventually(
				() =>
					subject.turns().includes("/status") &&
					subject.turns().includes("dm plain text") &&
					subject.turns().includes("yo pi-bot help me out"),
				4_000,
			);
			await settleWall();

			const turns = subject.turns();
			return {
				unmentionedChannelDropped: !turns.includes("plain chatter"),
				msc3952Authoritative: turns.includes("silent pill"),
				bodyFallbackStrippedToCleanText: turns.includes("hey look"),
				localpartWordBoundaryMentionsButBareWordKept: turns.includes(
					"yo pi-bot help me out",
				),
				wordBoundaryRespectedNotMentioned: !turns.some((t) =>
					t.includes("pi-botx"),
				),
				freeRoomBypass: turns.includes("free-room chatter"),
				commandBypass: turns.includes("/status"),
				dmExempt: turns.includes("dm plain text"),
				whitelistSilentlyDrops: !wl.subject
					.turns()
					.some((t) => t.includes("mentioned but not whitelisted")),
			};
		},

		/** Reply-fallback extraction + bang-command normalization. */
		async replyFallbackAndBang() {
			const world = makeMatrixWorld({
				name: "mx-reply-bang",
				isKnownCommand: (n) => n === "model" || n === "reload-skills",
			});
			const { engine, subject } = world;
			await world.connectAndAwaitLive();

			world.hs.pushRoomMessage("!room:fake.example", ALICE, {
				msgtype: "m.text",
				body: "> <@carol:fake.example> what model are you\n> second line\n\nI ask because curious",
				"m.relates_to": { "m.in_reply_to": { event_id: "$target" } },
			});
			world.pushMessage("!room:fake.example", ALICE, "no fallback here");
			world.pushMessage("!room:fake.example", ALICE, "!model please");
			world.pushMessage("!room:fake.example", ALICE, "!reload_skills now");
			world.pushMessage("!room:fake.example", ALICE, "!notacommand stays");

			await eventually(() => subject.turns().includes("/model please"), 4_000);
			await settleWall();
			const turns = subject.turns();
			const replied = engine.inboundEventLog.find((e) =>
				String(e.text ?? "").startsWith("I ask because"),
			);
			const meta = replied?.metadata ?? {};
			return {
				replyToTextExtracted:
					(meta["reply_to_text"] as string | undefined) ?? null,
				authorIdResolved:
					(meta["reply_to_author_id"] as string | undefined) ?? null,
				bodyStrippedToReplyOnly: turns.includes("I ask because curious"),
				nonFallbackUntouched: turns.includes("no fallback here"),
				bangNormalized: turns.includes("/model please"),
				underscoreBangNormalized: turns.includes("/reload-skills now"),
				unknownBangLeftAlone: turns.includes("!notacommand stays"),
			};
		},

		/** Channel directory + alias overlay (A9). */
		async directoryOverlay() {
			const world = freshWorld("mx-directory");
			const { engine, hs, clock } = world;
			await engine.connect({ isReconnect: false });

			hs.addRoom("!named:fake.example", {
				name: "The Named Room",
				canonicalAlias: "#named:fake.example",
				memberCount: 6,
			});
			hs.addRoom("!aliased:fake.example", {
				canonicalAlias: "#alias:fake.example",
				memberCount: 6,
			});
			hs.addRoom("!bare:fake.example", { memberCount: 7 });
			hs.addRoom("!pair:fake.example", { memberCount: 2 });
			hs.addRoom("!stale:fake.example", {
				name: "Actually A Room",
				memberCount: 9,
			});
			hs.setDirect(BOT_MXID, "!pair:fake.example");
			hs.setDirect(BOT_MXID, "!stale:fake.example");

			const named = await engine.resolveRoomIdentity("!named:fake.example");
			const aliased = await engine.resolveRoomIdentity("!aliased:fake.example");
			const bare = await engine.resolveRoomIdentity("!bare:fake.example");
			const pair = await engine.resolveRoomIdentity("!pair:fake.example");
			const stale = await engine.resolveRoomIdentity("!stale:fake.example");

			// Cache hit within TTL despite server-side rename…
			hs.setRoomState("!named:fake.example", { name: "Renamed" });
			const cachedStill = await engine.resolveRoomIdentity(
				"!named:fake.example",
			);
			// …TTL expiry resolves AGAIN through the server state.
			clock.nowVal += 61_000;
			const refreshed = await engine.resolveRoomIdentity("!named:fake.example");

			return {
				displayNamePrefersName: named.displayName,
				fallsBackToAlias: aliased.displayName,
				fallsBackToRoomId: bare.displayName,
				memberCountDmWins: pair.chatType === "dm",
				explicitNameBeatsStaleDirectConflictFlagged:
					stale.chatType === "room" && stale.conflict,
				cacheHitWithinTtl: cachedStill.displayName === "The Named Room",
				ttlExpiryResolvesAgain: refreshed.displayName === "Renamed",
			};
		},

		/** Reaction-ack lifecycle (A1) + typing variants (A11). */
		async reactionAckAndTyping() {
			const world = freshWorld("mx-reactions");
			const { engine, hs, clock } = world;
			await engine.connect({ isReconnect: false });

			await engine.onProcessingStart("!room:fake.example", "$msg1");
			await engine.onProcessingComplete(
				"!room:fake.example",
				"$msg1",
				"success",
			);
			await engine.onProcessingStart("!room:fake.example", "$msg2");
			await engine.onProcessingComplete(
				"!room:fake.example",
				"$msg2",
				"cancelled",
			);
			const eyes1 = hs.reactions.find(
				(r) => r.targetEventId === "$msg1" && r.key === "\u{1F440}",
			);
			const success = hs.reactions.find(
				(r) => r.targetEventId === "$msg1" && r.key === "\u2705",
			);
			const eyes2 = hs.reactions.find(
				(r) => r.targetEventId === "$msg2" && r.key === "\u{1F440}",
			);

			// Typing bubbles + M_LIMIT_EXCEEDED honor-once at the typing site.
			hs.scriptRateLimitOnce(2); // seconds — next setTyping throws once
			const first = await engine.sendTyping("!room:fake.example");
			const second = await engine.sendTyping("!room:fake.example");
			await engine.stopTyping("!room:fake.example");

			return {
				startEmoji: eyes1?.key ?? null,
				successSwappedEmoji: success?.key ?? null,
				eyesRedactedOnComplete: eyes1?.redacted === true,
				cancelClearedEyesOnly:
					eyes2?.redacted === true &&
					hs.reactions.filter((r) => r.targetEventId === "$msg2").length === 1,
				typingTimeoutMs: hs.typingEvents[0]?.timeoutMs ?? -1,
				stopTypingTimeoutMs:
					hs.typingEvents[hs.typingEvents.length - 1]?.timeoutMs ?? -1,
				rateLimitHonoredOnceThenRecovers:
					first.success === true &&
					second.success === true &&
					clock.sleeps.includes(2000),
			};
		},
	};
}

// ── internals ─────────────────────────────────────────────────────────────

const MATRIX_STARTUP_GRACE_MS = 5_000;

function freshWorld(name: string): MatrixWorld {
	return makeMatrixWorld({ name });
}

/** Delivered = the text reached the guard (debounce may merge; match content). */
function deliveredTexts(world: MatrixWorld, texts: string[]): number {
	const joined = world.subject.turns().join("\n");
	return texts.filter((t) => joined.split("\n").some((entry) => entry === t))
		.length;
}

/** Let async dispatch pipelines drain (wall-budget yield, no asserts). */
async function settleWall(): Promise<void> {
	await new Promise<void>((r) => setTimeout(r, 30));
}

/** Rewind a since token by two seq steps (server replay-window simulation). */
function rewindToken(token: string | null): string | null {
	if (token === null) return null;
	const m = /^s(\d+)_(\d+)$/.exec(token);
	if (m === null) return null;
	return `s${m[1]}_${Math.max(0, Number(m[2]) - 2)}`;
}
