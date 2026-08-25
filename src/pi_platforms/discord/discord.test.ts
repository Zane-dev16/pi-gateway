// pi_platforms/discord/discord.test.ts — the Discord port's behavior
// contracts beyond the shared conformance rows (roadmap Phase 6 exit gate).
// Every test runs headless against the Gateway v10 fake + harness wire under
// INJECTED clocks; timing is asserted by clock advancement, never wall time.
//
// Contracts covered here:
//   - gateway protocol: HELLO→heartbeat cadence, IDENTIFY payload, READY
//     session capture, RECONNECT(op:7) → RESUME ladder
//   - RESUME-vs-reidentify ladder incl. seq-gap sweep EXACTLY-ONCE (A13)
//   - heartbeat-ACK watchdog interplay: dropped acks reap at threshold 2 (A13)
//   - Q17 rate-bucket gating per route (data-driven over manifest table)
//   - components round-trip through kit machinery (DEC-016)
//   - auto-thread continuity: DEC-028 effective-thread-slot predicate at the
//     wire level (+ #51057 starter dedup, #20243 abort-on-failure)
//   - typing refresh-loop variants (A11): immediate entry fire, cadence,
//     duplicate suppression, 429 survival, stop + turn-admission wiring
//   - stability round r2: READY d.user.id identity grounding (ws-1),
//     forum-parent type-15 post lane (ws-9)
//   - manifest data transcription (limits/caps with Hermes anchors)

import { describe, expect, it } from "vitest";

import {
	buildExecApprovalCallback,
	buildClarifyCallback,
	CALLBACK_DATA_MAX_BYTES,
} from "../kit/index.js";
import { isSharedMultiUserSession } from "../../pi_gateway/resolution/session-key.js";
import { eventually, makeDiscordWorld } from "./discord-fixture.js";
import { DiscordGatewayFake } from "./gateway-fake.js";
import { RateBucketLedger } from "./rate-buckets.js";
import {
	DiscordRecoveryLedger,
	isDownNoticeContent,
} from "./recovery-ledger.js";
import {
	ALLOWED_MENTIONS_DEFAULTS,
	BUTTON_LABEL_LIMIT,
	CHANNEL_TYPE_FORUM,
	CLARIFY_CHOICES_MAX,
	DISCORD_IDENTIFY_INTENTS,
	DISCORD_INTENT_GUILD_MESSAGES,
	DISCORD_INTENT_MESSAGE_CONTENT,
	GATEWAY_OPCODES as OP,
	MAX_SPLIT_MESSAGES,
	MESSAGE_LENGTH_MAX,
	RATE_BUCKETS,
	SELECT_MAX_OPTIONS,
	THREAD_NAME_FALLBACK,
	TYPING_INTERVAL_SECONDS,
} from "./manifest.js";
import {
	DiscordAdapter,
	buildComponentRows,
	buildSelectOptions,
	deriveForumThreadName,
	deriveThreadName,
	truncateToMessageCap,
	convertGfmToDiscordMarkdown,
	type DiscordRestPlane,
} from "./discord-adapter.js";
import { ManualClock } from "./clock.js";
import { FakePlatformWire } from "../conformance/wire.js";

function push(
	world: ReturnType<typeof makeDiscordWorld>,
	body: {
		id: string;
		channelId: string;
		guildId?: string | undefined;
		authorId?: string | undefined;
		content: string;
		isThread?: boolean | undefined;
		threadId?: string | undefined;
		messageType?: number | undefined;
		referencedMessageId?: string | undefined;
	},
): void {
	world.gateway.pushMessage({
		id: body.id,
		channelId: body.channelId,
		...(body.guildId !== undefined ? { guildId: body.guildId } : {}),
		authorId: body.authorId ?? "user-1",
		content: body.content,
		...(body.isThread === true ? { isThread: true } : {}),
		...(body.threadId !== undefined ? { threadId: body.threadId } : {}),
		...(body.messageType !== undefined
			? { messageType: body.messageType }
			: {}),
		...(body.referencedMessageId !== undefined
			? { referencedMessageId: body.referencedMessageId }
			: {}),
	});
}

describe("gateway protocol contracts", () => {
	it("IDENTIFY carries token + INTEGER intents bitmask incl MESSAGE_CONTENT; READY captures session_id; heartbeats fire on HELLO cadence carrying last seq", async () => {
		const world = makeDiscordWorld({
			name: "proto",
			heartbeatIntervalMs: 1_000,
		});
		const { engine, gateway, clock } = world;
		await engine.connect({ isReconnect: false });
		await eventually(() => engine.isLive);

		const identify = gateway.receivedFrames.find(
			(f) => f.frame.op === OP.IDENTIFY,
		)?.frame.d as { token?: string; intents?: number };
		expect(identify.token).toBe("discord-fake-token");
		// VENDOR WIRE FORM: an integer bitmask — string arrays never come online
		// and MESSAGE_CONTENT is required or inbound content arrives empty.
		expect(typeof identify.intents).toBe("number");
		expect(identify.intents).toBe(DISCORD_IDENTIFY_INTENTS);
		expect(identify.intents! & DISCORD_INTENT_MESSAGE_CONTENT).toBeTruthy();
		expect(identify.intents! & DISCORD_INTENT_GUILD_MESSAGES).toBeTruthy();
		expect(engine.sessionId).toMatch(/^sess-/);

		await clock.advance(2_500); // two heartbeat intervals
		const beats = gateway.receivedFrames.filter(
			(f) => f.frame.op === OP.HEARTBEAT,
		);
		expect(beats.length).toBe(2);
		// Heartbeat payload carries the LAST SEEN sequence (vendor shape delta).
		expect(beats[0]?.frame.d).toBeTypeOf("number");
	});

	it("RECONNECT(op:7) closes into a RESUME ladder — same session survives", async () => {
		const world = makeDiscordWorld({ name: "reconnect-op" });
		const { engine, gateway, clock } = world;
		await world.connectAndAwaitLive();
		const sessionIdBefore = engine.sessionId;

		gateway.forceReconnect();
		await clock.advance(5_000);
		await eventually(() => engine.isLive);

		expect(gateway.identifyCount).toBe(1); // NO fresh identify
		expect(gateway.resumeCount).toBeGreaterThanOrEqual(1);
		expect(engine.sessionId).toBe(sessionIdBefore);
	});

	it("INVALID_SESSION(d:false) ladder: RESUME refused → re-IDENTIFY → sweep flagged", async () => {
		const world = makeDiscordWorld({ name: "invalid-session" });
		const { engine, gateway, clock } = world;
		await world.connectAndAwaitLive();

		gateway.expireSessions();
		gateway.dropActive({ reason: "server restart" });
		await clock.advance(5_000);
		await eventually(() => engine.isLive);

		expect(gateway.resumeCount).toBe(1); // attempted and REFUSED
		expect(gateway.invalidSessionCount).toBe(1);
		expect(gateway.identifyCount).toBe(2); // fresh IDENTIFY
		expect(engine.sessionId).not.toBeNull();
	});
});

describe("RESUME-vs-reidentify ladder with seq-gap recovery sweep (A13)", () => {
	it("unresumable outage → fresh IDENTIFY → missed-dispatch sweep re-admits EXACTLY ONCE through the normal pipeline", async () => {
		let sweepFetches = 0;
		const world = makeDiscordWorld({
			name: "sweep",
			historyProvider: {
				async fetchRecent(channelId, opts) {
					sweepFetches += 1;
					if (channelId !== "chan-1") return [];
					// History AFTER the acknowledged cursor: one missed message.
					return opts.afterMessageId === "m1"
						? [
								{
									id: "miss-1",
									channelId: "chan-1",
									authorId: "user-9",
									text: "lost during outage",
								},
							]
						: [];
				},
			},
		});
		const { engine, gateway, clock, subject } = world;
		await world.connectAndAwaitLive();

		push(world, { id: "m1", channelId: "chan-1", content: "before outage" });
		await eventually(() => subject.turns().length >= 1);

		gateway.expireSessions(); // kill resumability server-side
		gateway.dropActive({ reason: "outage" });
		await clock.advance(5_000);
		await eventually(() => engine.isLive && engine.sweepLog.length > 0);

		// The ladder went RESUME(refused) → INVALID_SESSION → IDENTIFY(fresh).
		expect(gateway.resumeCount).toBe(1);
		expect(gateway.invalidSessionCount).toBe(1);
		expect(gateway.identifyCount).toBe(2);
		expect(sweepFetches).toBeGreaterThanOrEqual(1);

		// Exactly-once: the missed message became ONE turn via the NORMAL
		// pipeline, ledger marked responded, cursor advanced.
		const recoveredTurns = subject
			.turns()
			.filter((t) => t.includes("lost during outage"));
		expect(recoveredTurns).toHaveLength(1);
		expect(
			subject.adapter.inboundLog.filter((id) => id === "miss-1"),
		).toHaveLength(1);
		expect(engine.ledger.get("miss-1")?.status).toBe("responded");
		expect(engine.ledger.cursorFor("chan-1")).toBe("miss-1");
		const sweep = engine.sweepLog[engine.sweepLog.length - 1];
		expect(sweep?.dispatched).toBe(1);

		// A REPLAY of the sweep (or a duplicate delivery) cannot dispatch twice:
		const turnsSnapshot = subject.turns().length;
		await engine.runMissedDispatchSweep("idempotent-rerun");
		push(world, {
			id: "miss-1",
			channelId: "chan-1",
			content: "lost during outage",
		});
		await new Promise((r) => setTimeout(r, 10));
		expect(subject.turns().length).toBe(turnsSnapshot);
		expect(subject.adapter.dedupSuppressedCount).toBeGreaterThanOrEqual(1);
	});

	it("sweep skips persistently-complete, actively-claimed, own, and down-noticed candidates; maxDispatches bounds", async () => {
		const now = { ms: 0 };
		const ledger = new DiscordRecoveryLedger({ nowMs: () => now.ms });
		ledger.recordDiscovered("done", { channelId: "c", authorId: "u1" });
		ledger.markResponded("done", "resp-1");
		ledger.recordDiscovered("stale-claim", { channelId: "c", authorId: "u3" });
		ledger.markStatus("stale-claim", "processing"); // claimed at t=0
		now.ms = 601_000; // past the 10-minute active-claim window…
		ledger.recordDiscovered("fresh-claim", { channelId: "c", authorId: "u2" });
		ledger.markStatus("fresh-claim", "processing"); // …claimed NOW (active)
		ledger.recordDiscovered("own", { channelId: "c", authorId: "bot-self" });
		for (let i = 0; i < 12; i++)
			ledger.recordDiscovered(`bulk-${i}`, {
				channelId: "c",
				authorId: `u${i}`,
			});

		const candidates = ledger.candidatesForSweep({
			botAuthorId: "bot-self",
			maxDispatches: 10,
		});
		const ids = candidates.map((c) => c.messageId);
		expect(ids).not.toContain("done"); // persistently complete
		expect(ids).not.toContain("fresh-claim"); // actively claimed
		expect(ids).not.toContain("own"); // bot-authored
		expect(ids).toContain("stale-claim"); // expired claim is eligible again
		expect(ids.length).toBeLessThanOrEqual(10); // bounded dispatches
	});

	it("down-notice content NEVER masks or dispatches as pending work (ping safety)", () => {
		expect(isDownNoticeContent("the agent is down")).toBe(true);
		expect(isDownNoticeContent("gateway was offline")).toBe(true);
		expect(isDownNoticeContent("deploy finished, all green")).toBe(false);
	});

	it("emoji-only acks are recorded but NOT completion (ledger status machine)", () => {
		const ledger = new DiscordRecoveryLedger({});
		ledger.recordDiscovered("e1", { channelId: "c", authorId: "u1" });
		ledger.markEmojiAck("e1");
		expect(ledger.get("e1")?.emojiAck).toBe(true);
		expect(ledger.isPersistentlyComplete("e1")).toBe(false);
		ledger.markResponded("e1", "r1");
		expect(ledger.isPersistentlyComplete("e1")).toBe(true);
	});
});

describe("heartbeat-ACK watchdog interplay (A13)", () => {
	it("fake drops acks → TWO stale probes reap the socket → RESUME without loss", async () => {
		const world = makeDiscordWorld({
			name: "ack-watchdog",
			heartbeatIntervalMs: 100,
			livenessIntervalMs: 30,
			// Ack-age budget comfortably ABOVE one heartbeat interval (vendor
			// ratio parity: 60s ack budget vs ~41s cadence), so only a DEAD
			// socket trips the two-probe threshold.
			ackMaxAgeMs: 250,
			livenessFailureThreshold: 2,
		});
		const { engine, gateway, clock, subject } = world;
		await world.connectAndAwaitLive();
		await clock.advance(300); // healthy ACKed heartbeats first

		gateway.stallHeartbeatAcks();
		await clock.advance(700); // stale probe ×2 → reap → ladder → RESUME
		await eventually(() => engine.isLive);

		expect(gateway.identifyCount).toBe(1); // resumed, not re-identified
		expect(gateway.resumeCount).toBeGreaterThanOrEqual(1);
		expect(engine.reconnectLog.length).toBeGreaterThanOrEqual(1);

		push(world, {
			id: "post-reap",
			channelId: "chat-1",
			content: "still here",
		});
		await eventually(() =>
			subject.turns().some((t) => t.includes("still here")),
		);

		// Recovery heals liveness: unstalled acks reset the failure counter.
		gateway.resumeHeartbeatAcks();
		const reconnectsAfterHeal = engine.reconnectLog.length;
		await clock.advance(400);
		expect(engine.reconnectLog.length).toBe(reconnectsAfterHeal);
	});

	it("probe DISABLED when knobs ≤ 0 (ping-safety rule)", async () => {
		const world = makeDiscordWorld({
			name: "watchdog-off",
			livenessIntervalMs: 40,
		});
		const { engine, clock, gateway } = world;
		engine.setWatchdogAckMaxAgeMs(0); // disabled probe
		await world.connectAndAwaitLive();
		gateway.stallHeartbeatAcks();
		await clock.advance(2_000);
		expect(engine.reconnectLog.length).toBe(0); // never reaped
		expect(engine.isLive).toBe(true);
	});
});

describe("Q17 rate-bucket gating (data-driven over the manifest table)", () => {
	it("every bucket entry gates its routes at limit/window with scoped independence", () => {
		const now = { ms: 0 };
		for (const spec of RATE_BUCKETS) {
			// Each bucket in ISOLATION (in the full table the channel bucket and
			// global bucket consume together — see the cross-channel test below).
			const ledger = new RateBucketLedger({
				nowMs: () => now.ms,
				buckets: [spec],
			});
			for (const route of spec.routes) {
				now.ms += spec.windowSeconds * 1000 + 1; // fresh window per route
				const scope = spec.scope === "global" ? "" : "chan-a";
				for (let i = 0; i < spec.limit; i++) {
					expect(ledger.consume(route, scope).allowed).toBe(true);
				}
				const blocked = ledger.consume(route, scope);
				expect(blocked.allowed).toBe(false);
				if (!blocked.allowed) {
					expect(blocked.bucketId).toBe(spec.id);
					expect(blocked.retryAfterSeconds).toBeGreaterThan(0);
				}
				// Window slide frees the bucket at the boundary.
				now.ms += spec.windowSeconds * 1000;
				expect(ledger.consume(route, scope).allowed).toBe(true);
			}
		}
	});

	it("channel-scoped buckets are per-channel; global caps across channels", () => {
		const now = { ms: 0 };
		const ledger = new RateBucketLedger({ nowMs: () => now.ms });
		// Exhaust chan-a's channel bucket.
		for (let i = 0; i < 5; i++)
			expect(ledger.consume("send", "chan-a").allowed).toBe(true);
		expect(ledger.consume("send", "chan-a").allowed).toBe(false);
		// A DIFFERENT channel still has its own channel-bucket budget…
		expect(ledger.consume("send", "chan-b").allowed).toBe(true);
		// …but the global bucket counts both.
		for (let i = 0; i < 49; i++) ledger.consume("send", `chan-${i}`);
		expect(ledger.consume("send", "chan-z").allowed).toBe(false);
	});

	it("server-authoritative 429 freezes buckets with the ≥1s floor clamp", () => {
		const now = { ms: 0 };
		const ledger = new RateBucketLedger({ nowMs: () => now.ms });
		ledger.recordAuthority("send", "chan-x", 0.3); // below floor
		expect(ledger.consume("send", "chan-x").allowed).toBe(false);
		now.ms += 900; // still inside clamped 1s freeze
		expect(ledger.consume("send", "chan-x").allowed).toBe(false);
		now.ms += 200; // past the clamp
		expect(ledger.consume("send", "chan-x").allowed).toBe(true);
	});

	it("adapter egress consults the gate BEFORE the wire: 6th send in-window blocks retryable", async () => {
		const world = makeDiscordWorld({ name: "gate-send", rateGate: true });
		const { wire, subject } = world;
		await world.connectAndAwaitLive();
		for (let i = 0; i < 5; i++) {
			const r = await subject.sendThroughDoor1("gated-chat", `msg ${i}`);
			expect(r.success).toBe(true);
		}
		const blocked = await subject.sendThroughDoor1("gated-chat", "msg 5");
		expect(blocked.success).toBe(false);
		expect(blocked.retryable).toBe(true);
		expect(blocked.retryAfter ?? 0).toBeGreaterThan(0);
		// The blocked message NEVER hit the wire — op count unchanged. (Earlier
		// same-chat sends reconcile-by-edit under §5 invariant 4, so the five
		// deliveries appear as one send + four edits; the GATE sees them all.)
		const chatOps = wire.ops.filter((o) => o.chatId === "gated-chat");
		expect(chatOps).toHaveLength(5);
	});

	it("typing shares the channel-messages bucket (data-driven route mapping)", async () => {
		const world = makeDiscordWorld({ name: "gate-typing", rateGate: true });
		const { engine, subject } = world;
		await world.connectAndAwaitLive();
		// Spend the channel-messages bucket on egress first…
		for (let i = 0; i < 5; i++) await subject.sendThroughDoor1("tc", `s${i}`);
		// …then typing maps onto the SAME exhausted bucket inside its window.
		const verdict = engine.rateLedger.consume("typing", "tc");
		expect(verdict.allowed).toBe(false);
		if (!verdict.allowed) expect(verdict.bucketId).toBe("channel-messages");
		// The manifest routes arrays are the DATA tying them together.
		const channelBucket = RATE_BUCKETS.find((b) => b.id === "channel-messages");
		expect(channelBucket?.routes).toContain("typing");
		expect(channelBucket?.routes).toContain("send");
	});
});

describe("components round-trip through kit machinery (DEC-016)", () => {
	it("approval card: builders → component rows ≤ vendor caps → interaction tap resolves once → host keyboard stripped → ack always", async () => {
		const world = makeDiscordWorld({ name: "components" });
		const { engine, wire, subject } = world;
		subject.registerApprovalPending(301, "sk-comp");

		const choices = [
			["once", "Allow Once"],
			["session", "Allow Session"],
			["always", "Always Allow"],
			["deny", "Deny"],
		] as const;
		const builders = choices.map(([choice, label]) => ({
			customId: buildExecApprovalCallback(choice, 301),
			label,
		}));
		await engine.sendExecApprovalCard("chat-comp", {
			approvalId: 301,
			builders,
		});

		// Wire shape: components ride the SEND metadata under vendor caps.
		const cardOp = wire.sendsOf("chat-comp")[0];
		expect(cardOp).toBeDefined();
		const components = cardOp?.metadata["components"] as ReturnType<
			typeof buildComponentRows
		>;
		expect(components.length).toBeLessThanOrEqual(5);
		const flatCustomIds = components.flatMap((row) =>
			row.components.map((c) => c.custom_id),
		);
		expect(flatCustomIds).toEqual(builders.map((b) => b.customId));
		for (const id of flatCustomIds)
			expect(Buffer.byteLength(id, "utf8")).toBeLessThanOrEqual(
				Math.min(CALLBACK_DATA_MAX_BYTES, 100),
			);
		// Plain-content mirror ships alongside (accessibility fallback).
		expect(cardOp?.content).toContain("Allow Once");

		const firstBuilder = builders[0];
		expect(firstBuilder).toBeDefined();

		// Unauthorized clicker: acked ⛔, never resolved.
		subject.setClickerAuthorization(false);
		await engine.handleInteraction({
			interactionId: "i-unauth",
			customId: firstBuilder?.customId ?? "",
			clickerId: "stranger",
		});
		expect(subject.resolvedFamilies()).not.toContain("ea");

		// Authorized tap: registry + router resolve; host keyboard stripped.
		subject.setClickerAuthorization(true);
		// FakePlatformWire assigns wire identity `wire-${seq}` to each op — the
		// host message id the interaction lands on.
		const cardMessageId = cardOp === undefined ? "" : `wire-${cardOp.seq}`;
		await engine.handleInteraction({
			interactionId: "i-1",
			customId: firstBuilder?.customId ?? "",
			clickerId: "user-1",
			...(cardMessageId.length === 0 ? {} : { messageId: cardMessageId }),
		});
		expect(subject.resolvedFamilies()).toContain("ea");
		const hostEdits = wire.editsOf(cardMessageId);
		expect(
			hostEdits.some((e) => e.metadata["components_removed"] === true),
		).toBe(true);

		// Double-tap: pop-once store answers STALE, resolves exactly once.
		await engine.handleInteraction({
			interactionId: "i-2",
			customId: firstBuilder?.customId ?? "",
			clickerId: "user-1",
		});
		expect(subject.resolvedFamilies().filter((f) => f === "ea")).toHaveLength(
			1,
		);
		expect(engine.interactionAudit.at(-1)?.answerKind).toBe("stale");

		// EVERY tap got an ack (spinner clears), including raising handlers.
		expect(subject.interactionAcks.map((a) => a.interactionId)).toEqual([
			"i-unauth",
			"i-1",
			"i-2",
		]);
	});

	it("clarify select: indexed custom_ids resolve choices; caps decline whole-render", async () => {
		const world = makeDiscordWorld({ name: "select" });
		const { engine, subject } = world;
		subject.registerClarifyPending(302, "sk-cl");

		const select = buildSelectOptions(
			Array.from({ length: CLARIFY_CHOICES_MAX }, (_, i) => ({
				label: `option ${i}`,
				value: buildClarifyCallback(302, i),
			})),
			"cl-select",
		);
		expect(select).not.toBeNull();
		expect(select?.options.length).toBe(CLARIFY_CHOICES_MAX);

		const firstValue = select?.options[0]?.value ?? "";
		await engine.handleInteraction({
			interactionId: "i-cl",
			customId: firstValue,
			clickerId: "user-1",
		});
		expect(subject.resolvedFamilies()).toContain("cl");

		expect(
			buildSelectOptions(
				Array.from({ length: SELECT_MAX_OPTIONS + 1 }, (_, i) => ({
					label: `o${i}`,
					value: `v${i}`,
				})),
				"s",
			),
		).toBeNull();
		expect(
			buildComponentRows(
				Array.from({ length: 26 }, (_, i) => ({
					customId: `c${i}`,
					label: `b${i}`,
				})),
			),
		).toEqual([]);
	});
});

describe("auto-thread continuity — DEC-028 effective-thread-slot predicate at the wire level", () => {
	it("initiator keys via prospective thread slot; in-thread follow-ups BYTE-MATCH; predicate agrees by construction", async () => {
		const world = makeDiscordWorld({ name: "autothread" });
		const { engine, subject, wire } = world;
		await world.connectAndAwaitLive();

		push(world, {
			id: "init-1",
			channelId: "chan-9",
			guildId: "g1",
			authorId: "user-2",
			content: "<@bot-self> please investigate the flaky pipeline",
		});
		await eventually(() => subject.turns().length >= 1);
		await eventually(() => engine.threadCreations.length >= 1);

		const creation = engine.threadCreations[0];
		expect(creation?.channelId).toBe("chan-9");
		expect(creation?.name).toBe("please investigate the flaky pipeline");
		// Thread creation rode the REST plane as data (op identity wire-${seq}).
		expect(
			wire.ops.some(
				(o) =>
					o.metadata["thread_create"] === true &&
					creation !== undefined &&
					`wire-${o.seq}` === creation.threadId,
			),
		).toBe(true);

		// Follow-up arriving INSIDE the thread keys to the SAME session.
		push(world, {
			id: "follow-1",
			channelId: creation?.threadId ?? "",
			guildId: "g1",
			authorId: "user-3",
			content: "<@bot-self> logs attached",
			isThread: true,
			threadId: creation?.threadId,
		});
		await eventually(() => subject.turns().length >= 2);

		// THE DEC-028 property: key construction and THE shared predicate read
		// the SAME effective thread slot — initiator (prospective) and follow-up
		// (real) classify identically.
		const threadId = creation?.threadId ?? "";
		expect(threadId.length).toBeGreaterThan(0);
		const prospectiveSource = {
			platform: "discord",
			chatType: "channel",
			userId: "user-2",
			chatId: "chan-9",
			prospectiveThreadId: threadId,
		};
		const threadSource = {
			platform: "discord",
			chatType: "thread",
			userId: "user-3",
			chatId: threadId,
			threadId,
		};
		expect(isSharedMultiUserSession(prospectiveSource)).toBe(
			isSharedMultiUserSession(threadSource),
		);
		// Both are SHARED sessions (threads shared under default flags)…
		expect(isSharedMultiUserSession(threadSource)).toBe(true);
		// …so neither carries a participant slot — keys converge on the slot.
		expect(JSON.stringify(prospectiveSource.prospectiveThreadId)).toBe(
			JSON.stringify(threadSource.threadId),
		);

		// Isolation-flag flips do NOT re-key thread continuity (predicate reads
		// the effective slot, group flag only governs non-thread channels).
		expect(
			isSharedMultiUserSession(threadSource, { groupSessionsPerUser: false }),
		).toBe(true);
	});

	it("starter-message echo after thread creation is DEDUPED (#51057 pre-seed)", async () => {
		const world = makeDiscordWorld({ name: "starter-echo" });
		const { engine, subject } = world;
		await world.connectAndAwaitLive();

		push(world, {
			id: "init-starter",
			channelId: "chan-5",
			guildId: "g1",
			authorId: "user-2",
			content: "<@bot-self> start a topic",
		});
		await eventually(() => engine.threadCreations.length >= 1);
		const turnsAfterInit = subject.turns().length;
		const threadId = engine.threadCreations[0]?.threadId;

		// Vendor echo: a second MESSAGE_CREATE keyed by the THREAD/starter id.
		push(world, {
			id: threadId ?? "seed",
			channelId: "chan-5",
			guildId: "g1",
			authorId: "user-2",
			content: "<@bot-self> start a topic",
		});
		await new Promise((r) => setTimeout(r, 10));
		expect(subject.turns().length).toBe(turnsAfterInit);
	});

	it("thread-creation failure ABORTS processing — no inline fallback (#20243)", async () => {
		const world = makeDiscordWorld({ name: "thread-fail" });
		world.wire.script(
			"send",
			{ kind: "fail", error: "Missing Permissions (50013)" },
			{ kind: "fail", error: "Missing Permissions (50013)" },
		);
		const { engine, subject, clock, wire } = world;
		await world.connectAndAwaitLive();

		push(world, {
			id: "init-fail",
			channelId: "chan-7",
			guildId: "g1",
			authorId: "user-2",
			content: "<@bot-self> hello",
		});
		// Let ingress reach its first injected-clock await, THEN unwind the
		// 750ms retry backoff (advance() only fires already-registered waits).
		await new Promise((r) => setImmediate(r));
		await clock.advance(2_000);

		expect(engine.threadCreations).toHaveLength(0); // retried once, gave up
		expect(subject.turns()).toHaveLength(0); // message ABORTED, not inlined
		expect(engine.ledger.get("init-fail")?.status).toBe("failed");
		// The abort pairs a VISIBLE channel notice so users know to retry
		// (adapter.py :8196-8206) — delivered AFTER both create attempts failed.
		const ops = wire.ops.filter((o) => o.op === "send");
		const notice = ops[ops.length - 1];
		expect(notice?.content).toContain("could not create");
		expect(notice?.content).toContain("Please retry");
	});

	it("per-turn emoji acks: eyes on dispatch, remove-then-add success/failure on completion; ledger fed", async () => {
		const world = makeDiscordWorld({ name: "emoji-acks" });
		const { engine, subject } = world;
		await world.connectAndAwaitLive();

		push(world, {
			id: "emo-1",
			channelId: "chan-e",
			guildId: "g1",
			authorId: "user-2",
			content: "<@bot-self> react to me",
		});
		await eventually(() => subject.turns().length >= 1);
		await eventually(() => engine.ledger.get("emo-1")?.emojiAck === true);

		const EYES = "\u{1F440}";
		const CHECK = "\u2705";
		const reactions = subject.reactionOps.filter(
			(r) => r.messageId === "emo-1",
		);
		// First op: eyes ADDED on dispatch.
		expect(reactions[0]).toMatchObject({
			channelId: "chan-e",
			messageId: "emo-1",
			emoji: EYES,
			action: "add",
		});
		// Completion swap: eyes REMOVED then check ADDED.
		expect(
			reactions.some((r) => r.action === "remove" && r.emoji === EYES),
		).toBe(true);
		expect(reactions.at(-1)).toMatchObject({
			emoji: CHECK,
			action: "add",
		});
	});

	it("split continuations + replies build message_reference (grouping parity)", async () => {
		const world = makeDiscordWorld({ name: "references" });
		const { subject, wire } = world;
		// Oversized content splits at the harness budget (64 units).
		const results = await subject.adapter.deliverText(
			"ref-chat",
			"x".repeat(150),
		);
		const sends = wire.sendsOf("ref-chat");
		expect(sends.length).toBeGreaterThanOrEqual(2);
		const firstId = results[0]?.messageId;
		expect(sends[1]?.metadata["message_reference"]).toEqual({
			message_id: firstId,
			fail_if_not_exists: false,
		});
		expect(sends[0]?.metadata["message_reference"]).toBeUndefined();
		// Continuations chain: chunk 3 references chunk 2.
		if (sends.length >= 3) {
			expect(sends[2]?.metadata["message_reference"]).toEqual({
				message_id: results[1]?.messageId,
				fail_if_not_exists: false,
			});
		}

		// Reply lane: reply_to_message_id converts into the vendor body key.
		await subject.sendThroughDoor1("ref-reply", "a reply body", {
			reply_to_message_id: "parent-9",
		});
		const replySend = wire.sendsOf("ref-reply")[0];
		expect(replySend?.metadata["message_reference"]).toMatchObject({
			message_id: "parent-9",
		});
		expect(replySend?.metadata["reply_to_message_id"]).toBeUndefined();
	});
});

describe("typing refresh-loop variants (A11)", () => {
	it("IMMEDIATE first fire on entry, 12s refresh cadence, duplicate suppression, stop cancels — injected clock only", async () => {
		const world = makeDiscordWorld({ name: "typing" });
		const { engine, clock, subject } = world;
		await world.connectAndAwaitLive();

		engine.startTyping("chat-typing");
		await clock.advance(0); // the entry post lands at t+0 — no interval elapses
		// Fires ON ENTRY (request-first loop, :5605-5637) — a sub-interval
		// turn still shows the indicator.
		expect(subject.typingOps).toHaveLength(1);
		engine.startTyping("chat-typing"); // duplicate suppressed — still ONE loop
		await clock.advance(TYPING_INTERVAL_SECONDS * 1000);
		expect(subject.typingOps).toHaveLength(2); // entry + t=12 refresh
		expect(engine.typingActive("chat-typing")).toBe(true);

		engine.stopTyping("chat-typing");
		await clock.advance(TYPING_INTERVAL_SECONDS * 1000 * 2);
		expect(subject.typingOps).toHaveLength(2);
		expect(engine.typingActive("chat-typing")).toBe(false);
	});

	it("429 survival: authoritative delay alone leads to the next post, KEEP looping; non-retryable ends the loop", async () => {
		const world = makeDiscordWorld({ name: "typing-429" });
		const { engine, clock, subject } = world;
		await world.connectAndAwaitLive();

		// The IMMEDIATE entry fire draws a scripted 429 (retryable + retry_after).
		world.wire.script("send", {
			kind: "fail",
			error: "429 rate limited",
			retryable: true,
			retryAfter: 2,
		});
		engine.startTyping("chat-tp");
		await clock.advance(0);
		expect(subject.typingOps).toHaveLength(1); // fired, got 429

		// Loop survived: slept ONLY the authoritative 2s, next refresh succeeds
		// (:5626 sleep(retry_after) → straight back to request).
		await clock.advance(3_000);
		expect(subject.typingOps).toHaveLength(2);
		// Back on the plain 12s cadence from the last fire.
		await clock.advance(TYPING_INTERVAL_SECONDS * 1000);
		expect(subject.typingOps).toHaveLength(3);

		// A NON-retryable failure ENDS the loop.
		world.wire.script("send", { kind: "timeout" });
		await clock.advance(TYPING_INTERVAL_SECONDS * 1000 + 1_000);
		const countAfterTimeout = subject.typingOps.length;
		await clock.advance(TYPING_INTERVAL_SECONDS * 1000 * 2);
		expect(subject.typingOps.length).toBe(countAfterTimeout);
		expect(engine.typingActive("chat-tp")).toBe(false);
	});
});

describe("stability round r2 — identity grounding, turn-admission typing, forum-post lane", () => {
	/** Direct engine with a forum-capable REST plane (subject wire lacks the
	 * vendor channel-type probe; conformance rows cover the legacy lane). */
	function makeForumEngine(opts: {
		clock: ManualClock;
		wire: FakePlatformWire;
		gateway?: DiscordGatewayFake | undefined;
		forumTypes?: ReadonlySet<string> | undefined;
		posts?: Array<{ forumId: string; name: string; starter: string }>;
		failFirstPost?: boolean | undefined;
	}): DiscordAdapter {
		const posts = opts.posts ?? [];
		let postAttempts = 0;
		const rest: DiscordRestPlane = {
			transmitSend: (c, content, m) => opts.wire.transmitSend(c, content, m),
			transmitEdit: (c, mid, content, m) =>
				opts.wire.transmitEdit(c, mid, content, m),
			transmitDraft: (c, d, content, final, m) =>
				opts.wire.transmitDraft(c, d, content, final, m),
			transmitRich: (c, content, m) => opts.wire.transmitRich(c, content, m),
			transmitThreadCreate: (c, name, m) =>
				opts.wire.transmitSend(c, name, { ...m, thread_create: true }),
			transmitTyping: async () => ({ success: true }),
			transmitInteractionAck: async () => ({ success: true }),
			hasScript: (k) => opts.wire.hasScript(k),
		};
		if (opts.forumTypes !== undefined) {
			rest.resolveChannelType = async (chatId) =>
				opts.forumTypes?.has(chatId) === true ? CHANNEL_TYPE_FORUM : 0;
			rest.transmitForumThreadCreate = async (forumId, name, starter) => {
				postAttempts += 1;
				if (opts.failFirstPost === true && postAttempts === 1) {
					return { success: false, error: "forced forum-post failure" };
				}
				posts.push({ forumId, name, starter });
				return {
					success: true,
					messageId: `starter-${posts.length}`,
					threadId: `thread-${posts.length}`,
				};
			};
		}
		const gateway =
			opts.gateway ?? new DiscordGatewayFake({ nowMs: opts.clock.nowMs });
		return new DiscordAdapter({
			transport: gateway,
			rest,
			clock: { nowMs: opts.clock.nowMs, sleepMs: opts.clock.sleepMs },
			scalarMaxUnits: MESSAGE_LENGTH_MAX,
		});
	}

	it("ws-1: READY d.user.id grounds the bot identity — require_mention matches the REAL id and self-echo filters on it", async () => {
		const clock = new ManualClock();
		const wire = new FakePlatformWire();
		// The gateway fake echoes a DIFFERENT id than the 'bot-self' fallback.
		const gateway = new DiscordGatewayFake({
			nowMs: clock.nowMs,
			botUserId: "gateway-bot-42",
		});
		const engine = makeForumEngine({ clock, wire, gateway });
		engine.attachStandardGuard();
		await engine.connect({ isReconnect: false });
		await eventually(() => engine.isLive);

		// Identity ADOPTED from READY d.user.id (adapter.py:on_ready :1391 —
		// Hermes grounds everything in client.user).
		expect(engine.resolvedBotUserId).toBe("gateway-bot-42");

		// A guild message mentioning <@gateway-bot-42> DISPATCHES — with the
		// unfixed fallback ('bot-self') require_mention would silently drop it.
		gateway.pushMessage({
			id: "rm-1",
			channelId: "guild-chan",
			guildId: "g1",
			authorId: "user-7",
			content: "<@gateway-bot-42> status please",
		});
		await eventually(() => engine.turnLog.length >= 1);
		expect(engine.turnLog[0]).toBe("status please"); // stripSelfMention consumed the mention

		// Unmentioned guild messages stay gated (require_mention default holds).
		gateway.pushMessage({
			id: "rm-2",
			channelId: "guild-chan",
			guildId: "g1",
			authorId: "user-8",
			content: "no mention here",
		});
		await new Promise((r) => setTimeout(r, 10));
		expect(engine.turnLog).toHaveLength(1);

		// Self-echo filter grounds in the adopted id too.
		gateway.pushMessage({
			id: "rm-3",
			channelId: "guild-chan",
			guildId: "g1",
			authorId: "gateway-bot-42",
			content: "<@gateway-bot-42> echo of myself",
		});
		await new Promise((r) => setTimeout(r, 10));
		expect(engine.turnLog).toHaveLength(1);
	});

	it("ws-1: a payload-less READY never downgrades the grounded identity (injected value stays fallback)", async () => {
		const clock = new ManualClock();
		const wire = new FakePlatformWire();
		const gateway = new DiscordGatewayFake({
			nowMs: clock.nowMs,
			botUserId: "gateway-bot-42",
		});
		const engine = makeForumEngine({ clock, wire, gateway });
		await engine.connect({ isReconnect: false });
		await eventually(() => engine.isLive);

		// Synthetic re-READY WITHOUT a user payload: identity must survive.
		gateway.dispatch("READY", { session_id: "sess-anon" });
		await new Promise((r) => setTimeout(r, 10));
		expect(engine.resolvedBotUserId).toBe("gateway-bot-42");
	});

	it("ws-2: turn admission starts the indicator; failure stops it; finalize stops it", async () => {
		// Direct engine, NO guard: handleIngress rejects after admission — a
		// deterministic FAILURE window. A gated transmitReaction holds the
		// adapter BETWEEN startTyping and the turn so the ACTIVE state (and
		// the t+0 entry post) is observable.
		const clock = new ManualClock();
		const wire = new FakePlatformWire();
		let releaseReaction: () => void = () => {};
		const reactionGate = new Promise<void>((r) => {
			releaseReaction = r;
		});
		const typingPosts: string[] = [];
		const gateway = new DiscordGatewayFake({ nowMs: clock.nowMs });
		const engine = new DiscordAdapter({
			transport: gateway,
			rest: {
				transmitSend: (c, content, m) => wire.transmitSend(c, content, m),
				transmitEdit: (c, mid, content, m) =>
					wire.transmitEdit(c, mid, content, m),
				transmitDraft: (c, d, content, final, m) =>
					wire.transmitDraft(c, d, content, final, m),
				transmitRich: (c, content, m) => wire.transmitRich(c, content, m),
				transmitThreadCreate: (c, name, m) =>
					wire.transmitSend(c, name, { ...m, thread_create: true }),
				transmitTyping: async (chatId) => {
					typingPosts.push(chatId);
					return { success: true };
				},
				transmitInteractionAck: async () => ({ success: true }),
				async transmitReaction() {
					await reactionGate;
					return { success: true };
				},
				hasScript: (k) => wire.hasScript(k),
			},
			clock: { nowMs: clock.nowMs, sleepMs: clock.sleepMs },
			scalarMaxUnits: MESSAGE_LENGTH_MAX,
		});
		await engine.connect({ isReconnect: false });
		await eventually(() => engine.isLive);

		gateway.pushMessage({
			id: "tw-1",
			channelId: "chan-tw",
			guildId: "g1",
			authorId: "user-2",
			content: "<@bot-self> work on this",
		});
		// Admission STARTED the indicator (run.py:5000 parity)…
		await eventually(() => engine.typingActive("chan-tw"));
		await clock.advance(0); // …and the entry post fires at t+0 MID-TURN
		expect(typingPosts).toEqual(["chan-tw"]);

		releaseReaction();
		// handleIngress now rejects (no guard) → ❌ failure path → STOPPED.
		await eventually(
			() =>
				engine.ledger.get("tw-1")?.status === "failed" &&
				!engine.typingActive("chan-tw"),
		);

		// Finalize side: a GUARDED engine completes dispatch — the indicator
		// is stopped once handleIngress settles (✅ success branch).
		const clock2 = new ManualClock();
		const wire2 = new FakePlatformWire();
		const gateway2 = new DiscordGatewayFake({ nowMs: clock2.nowMs });
		const engine2 = new DiscordAdapter({
			transport: gateway2,
			rest: {
				transmitSend: (c, content, m) => wire2.transmitSend(c, content, m),
				transmitEdit: (c, mid, content, m) =>
					wire2.transmitEdit(c, mid, content, m),
				transmitDraft: (c, d, content, final, m) =>
					wire2.transmitDraft(c, d, content, final, m),
				transmitRich: (c, content, m) => wire2.transmitRich(c, content, m),
				transmitThreadCreate: (c, name, m) =>
					wire2.transmitSend(c, name, { ...m, thread_create: true }),
				transmitTyping: async () => ({ success: true }),
				transmitInteractionAck: async () => ({ success: true }),
				hasScript: (k) => wire2.hasScript(k),
			},
			clock: { nowMs: clock2.nowMs, sleepMs: clock2.sleepMs },
			scalarMaxUnits: MESSAGE_LENGTH_MAX,
		});
		engine2.attachStandardGuard();
		await engine2.connect({ isReconnect: false });
		await eventually(() => engine2.isLive);
		gateway2.pushMessage({
			id: "tw-2",
			channelId: "chan-done",
			guildId: "g1",
			authorId: "user-3",
			content: "<@bot-self> finish clean",
		});
		await eventually(() => engine2.inboundLog.includes("tw-2"));
		expect(engine2.typingActive("chan-done")).toBe(false);
	});

	it("ws-9: type-15 targets route sends through thread-create starter + follow-ups INTO the new thread", async () => {
		const clock = new ManualClock();
		const wire = new FakePlatformWire();
		const posts: Array<{ forumId: string; name: string; starter: string }> = [];
		const engine = makeForumEngine({
			clock,
			wire,
			forumTypes: new Set(["forum-1"]),
			posts,
		});

		// Oversized delivery ⇒ starter + follow-up chunks (_send_to_forum :3593:
		// create_thread(name=derived, content=starter), then thread sends).
		const body = Array.from(
			{ length: 240 },
			(_, i) => `line-${i} padding text`,
		).join("\n");
		const results = await engine.deliverText(
			"forum-1",
			`Forum report\n${body}`,
		);

		// EXACTLY ONE post op carrying the derived FIRST-LINE name + starter.
		expect(posts).toHaveLength(1);
		expect(posts[0]?.forumId).toBe("forum-1");
		expect(posts[0]?.name).toBe("Forum report");
		expect(posts[0]?.starter.startsWith("Forum report")).toBe(true);

		// Follow-up chunks went INTO the created thread — never as plain sends
		// to the forum parent (Discord rejects those).
		const followUps = wire.sendsOf("thread-1");
		expect(followUps.length).toBe(results.length - 1);
		expect(wire.sendsOf("forum-1")).toHaveLength(0);
		expect(results.every((r) => r.success)).toBe(true);
		expect(results[0]?.messageId).toBe("starter-1");
	});

	it("ws-9: single door sends create the post; non-forum chats probe ONCE then ride the plain lane", async () => {
		const clock = new ManualClock();
		const wire = new FakePlatformWire();
		const posts: Array<{ forumId: string; name: string; starter: string }> = [];
		let probes = 0;
		const engine = makeForumEngine({
			clock,
			wire,
			posts,
			forumTypes: new Set(["forum-x"]),
		});
		// Wrap to count channel-type lookups (probe-cache evidence).
		const rest = (engine as unknown as { rest: DiscordRestPlane }).rest;
		const innerResolve = rest.resolveChannelType?.bind(rest);
		if (innerResolve !== undefined) {
			rest.resolveChannelType = async (chatId) => {
				probes += 1;
				return innerResolve(chatId);
			};
		}

		const sent = await engine.send("forum-x", "Door body", undefined, {});
		expect(sent.success).toBe(true);
		expect(sent.messageId).toBe("starter-1"); // starter message id surfaces
		expect(posts).toHaveLength(1);
		expect(posts[0]?.name).toBe("Door body");

		// Plain chats never create posts. (The SECOND bare send to the SAME
		// chat reconciles by edit under §5 invariant 4 — chokepoint lane
		// behavior, orthogonal to the forum routing.)
		await engine.send("plain-chat", "normal", undefined, {});
		await engine.send("plain-chat", "again", undefined, {});
		const plainOps = wire.ops.filter((o) => o.chatId === "plain-chat");
		expect(plainOps.some((o) => o.op === "send")).toBe(true);
		expect(plainOps.every((o) => o.metadata["forum_post"] === undefined)).toBe(
			true,
		);
		expect(posts).toHaveLength(1);
		// Verdicts CACHE per chat: forum-x + plain-chat probed exactly once.
		expect(probes).toBe(2);
	});

	it("ws-9: failed starter surfaces the error and a later chunk falls back to creating its own post", async () => {
		const clock = new ManualClock();
		const wire = new FakePlatformWire();
		const posts: Array<{ forumId: string; name: string; starter: string }> = [];
		const engine = makeForumEngine({
			clock,
			wire,
			forumTypes: new Set(["forum-f"]),
			posts,
			failFirstPost: true,
		});
		const results = await engine.deliverText(
			"forum-f",
			`first line\n${"x".repeat(4600)}`,
		);
		// Post #1 failed; chunk 2 had no open thread so it created ITS OWN post;
		// chunk 3 followed into that new thread. No doomed plain send ever hit
		// the forum parent.
		expect(results[0]?.success).toBe(false);
		expect(results.slice(1).every((r) => r.success)).toBe(true);
		expect(wire.sendsOf("forum-f")).toHaveLength(0);
		expect(wire.sendsOf("thread-1").length).toBeGreaterThanOrEqual(1);
	});

	it("ws-9: forum-thread names derive from the first line (heading strip, 100-char cap, New Post fallback)", () => {
		expect(deriveForumThreadName("# Bug report\nbody below")).toBe(
			"Bug report",
		);
		expect(deriveForumThreadName("### ### heading")).toBe("### heading");
		expect(deriveForumThreadName("x".repeat(250))).toHaveLength(100);
		expect(deriveForumThreadName("#\nreal title")).toBe("New Post");
		expect(deriveForumThreadName("")).toBe("New Post");
	});
});

describe("manifest data transcription (vendor ground truth)", () => {
	function directAdapter(
		clock: ManualClock,
		wire: FakePlatformWire,
	): DiscordAdapter {
		return new DiscordAdapter({
			transport: new DiscordGatewayFake({ nowMs: clock.nowMs }),
			rest: {
				transmitSend: async (c, content, m) => wire.transmitSend(c, content, m),
				transmitEdit: async (c, mid, content, m) =>
					wire.transmitEdit(c, mid, content, m),
				transmitDraft: async (c, d, content, final, m) =>
					wire.transmitDraft(c, d, content, final, m),
				transmitRich: async (c, content, m) => wire.transmitRich(c, content, m),
				transmitThreadCreate: async (c, name, m) =>
					wire.transmitSend(c, name, { ...m, thread_create: true }),
				transmitTyping: async () => ({ success: true }),
				transmitInteractionAck: async () => ({ success: true }),
				hasScript: (k) => wire.hasScript(k),
			},
			clock: { nowMs: clock.nowMs, sleepMs: clock.sleepMs },
			scalarMaxUnits: MESSAGE_LENGTH_MAX,
		});
	}

	it("2000-char cap chunks with fence carry + (i/n); MAX_SPLIT_MESSAGES bound honored", async () => {
		const clock = new ManualClock();
		const wire = new FakePlatformWire();
		const adapter = directAdapter(clock, wire);
		const long = Array.from(
			{ length: 260 },
			(_, i) => `line-${i} padding text`,
		).join("\n");
		const results = await adapter.deliverText("cap-chat", long);
		expect(results.every((r) => r.success)).toBe(true);
		const sends = wire.sendsOf("cap-chat");
		expect(sends.length).toBeGreaterThan(1);
		expect(sends.length).toBeLessThanOrEqual(MAX_SPLIT_MESSAGES);
		for (const [idx, op] of sends.entries()) {
			expect(op.content.endsWith(`(${idx + 1}/${sends.length})`)).toBe(true);
		}
	});

	it("thread-name derivation: mention strip, collapse, UTF-16 cap 80 with … fallback", () => {
		expect(deriveThreadName("<@123> <@!456> fix   the   bug")).toBe(
			"fix the bug",
		);
		expect(deriveThreadName("   ")).toBe(THREAD_NAME_FALLBACK);
		const long = "x".repeat(120);
		const named = deriveThreadName(long);
		expect(named.length).toBe(80);
		expect(named.endsWith("...")).toBe(true);
		// UTF-16 code-unit math: astral chars count DOUBLE.
		const astral = "🎉".repeat(50); // 100 utf16 units / 50 codepoints
		expect(deriveThreadName(astral).length).toBe(80);
	});

	it("streaming-edit truncation caps at 2000 codepoints with saturated marker", () => {
		const huge = "y".repeat(2500);
		const cut = truncateToMessageCap(huge);
		expect(cut.startsWith("y".repeat(MESSAGE_LENGTH_MAX))).toBe(true);
		expect(cut.endsWith("…")).toBe(true);
		expect(truncateToMessageCap("small")).toBe("small");
	});

	it("component caps ellipsize labels by UTF-16 units and decline over-cap renders", () => {
		const astralLabel = "🎉".repeat(60); // 120 utf16 units
		const rows = buildComponentRows([{ customId: "x", label: astralLabel }]);
		const label = rows[0]?.components[0]?.label ?? "";
		expect([...label].length).toBeLessThanOrEqual(BUTTON_LABEL_LIMIT);

		const options = buildSelectOptions(
			Array.from({ length: SELECT_MAX_OPTIONS }, (_, i) => ({
				label: `l${i}`,
				value: `v${i}`,
			})),
			"sel",
		);
		expect(options?.options.every((o) => o.value.length <= 100)).toBe(true);
	});

	it("ping-safety allowed_mentions ship VENDOR shape and ride EVERY lane (send/edit/draft)", async () => {
		const world = makeDiscordWorld({ name: "ping-safety" });
		const { subject, wire } = world;
		await subject.sendThroughDoor1("ps-chat", "@everyone check this");
		const op = wire.sendsOf("ps-chat")[0];
		// Vendor wire form (discord.py AllowedMentions.to_dict) — camelCase
		// booleans would be DROPPED by the vendor ⇒ parse=[] ⇒ no pings at all.
		expect(op?.metadata["allowed_mentions"]).toEqual({
			parse: ["users"],
			replied_user: true,
		});
		expect(ALLOWED_MENTIONS_DEFAULTS.parse).not.toContain("everyone");
		expect(ALLOWED_MENTIONS_DEFAULTS.parse).not.toContain("roles");

		// Edit + streaming-edit lanes PATCH with the same safe default
		// (discord.py Message.edit fills the client-wide default :3798).
		await subject.adapter.editMessage("ps-chat", "wire-1", "updated");
		const editOp = wire.editsOf("ps-chat")[0];
		expect(editOp?.metadata["allowed_mentions"]).toEqual({
			parse: ["users"],
			replied_user: true,
		});
		await subject.streamAdapter().sendDraft?.({
			chatId: "ps-chat",
			draftId: 3,
			content: "stream",
		});
		const draftOp = wire.draftsOf("ps-chat")[0];
		expect(draftOp?.metadata["allowed_mentions"]).toEqual({
			parse: ["users"],
			replied_user: true,
		});
	});

	it("REST lane converts ONLY GFM tables; emphasis/link bytes stay native; fences survive byte-exact", () => {
		const md =
			"**bold** [link](https://x.y)\n\n| a | b |\n|---|---|\n| 1 | 2 |\n\n```\n| raw | pipe |\n```";
		const out = convertGfmToDiscordMarkdown(md);
		expect(out).toContain("**bold** [link](https://x.y)");
		expect(out).not.toContain("| a | b |");
		expect(out).toContain("```"); // table fenced…
		expect(out).toContain("| raw | pipe |"); // …fence contents byte-exact
	});

	it("gateway opcode constants match the vendor table (transcription guard)", () => {
		expect(OP.DISPATCH).toBe(0);
		expect(OP.HEARTBEAT).toBe(1);
		expect(OP.IDENTIFY).toBe(2);
		expect(OP.RESUME).toBe(6);
		expect(OP.RECONNECT).toBe(7);
		expect(OP.INVALID_SESSION).toBe(9);
		expect(OP.HELLO).toBe(10);
		expect(OP.HEARTBEAT_ACK).toBe(11);
	});
});
