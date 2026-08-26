// CONFORMANCE WIRING — the Home Assistant census port vs the executable
// 04 §8 matrix (roadmap Phase 6 wave 2; DEC-002 gate applies per adapter).
//
//   1. ALL applicable SHARED rows pass for shape="ws" against the REAL
//      kit-built HomeAssistantSubject. Applicability is COMPUTED from
//      capability data: native draft streaming is excluded BY THE PROBE
//      because HA persistent notifications have no edit API at all
//      (HA_SUPPORTS_MESSAGE_EDITING=false — manifest datum feeding the probe,
//      signal/manifest.ts precedent), so the three streaming rows drop out
//      via the probe, never a hardcoded skip. The LIE-SCAN at the bottom
//      flips THE datum and shows the streaming family rows then RUN and FAIL.
//   2. ALL FIVE inherited ws transport rows (makeWsRows) run over the REAL
//      engine fixture with documented HA leg mappings (proposed DEC text in
//      the port report):
//        - resubscribe-replay: BACKLOG MAPPING — HA redelivers nothing across
//          reconnects (subscribe_events has no resume cursor); loss-free ws
//          coverage is modeled SERVER-SIDE: events pushed during a disconnect
//          window flush BEFORE new ones after reconnect+resubscribe;
//          exactly-once downstream holds.
//        - heartbeat-watchdog: REAL fit — aiohttp ws_connect(heartbeat=30)
//          parity: stale-ping detection under the injected clock forces the
//          reconnect; recovery without loss.
//        - retry-after-capture: NO rate-limit guidance exists on this wire
//          (honest absence). The CLOSE feeds the backoff ladder whose CURRENT
//          STEP IS the authoritative next delay (closeCapturedSeconds =
//          ladder step at close, nextDelayMs = slept-ms on the injected
//          clock); the second source is an AUTH-FAILURE-class failed
//          reconnect feeding the SAME ladder.
//        - capability-latch-permanence: the exclusion IS the immutable
//          manifest datum — first draft attempt fails with ZERO wire draft/
//          seal ops; later attempts skip the wire; transient failures never
//          flip it; exactly ONE datum drives the verdict.
//        - dual-path-markdown: SINGLE-PATH plain-text platform — markdown
//          bytes ship VERBATIM inside the notification message; every
//          conversion leg degenerates to identity/ABSENCE-UNIFORMITY.
//   3. Fresh HA shape-delta rows (ids prefixed "transport.ha.") execute
//      through the REAL engine fixture (handshake ladder, filter-chain order,
//      cooldowns, formatter matrix, backoff ladder, REST send shape, ingress
//      dispatch shape). Trust/posture notes are folded into the manifest
//      comments per the signal precedent: NO HTTP ingress exists ⇒ no trust
//      boundary is declared.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: lying fixtures fail their own named rows with
//      specificity; real fixture facts contradict the lies.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { makeWsRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { WsFixture } from "./shapes.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import type { ConformanceSubject } from "./harness.js";

import {
	formatStateChange,
	HA_SEND_TRUNCATES,
	HA_WS_HEARTBEAT_MS,
	type HaWatchConfig,
} from "../homeassistant/manifest.js";
import {
	type HomeAssistantAdapter,
	HA_BACKOFF_STEPS_SECONDS,
} from "../homeassistant/homeassistant-adapter.js";
import {
	HomeAssistantSubject,
	FakeHaServer,
} from "../homeassistant/homeassistant-subject.js";
import { ManualClock } from "../homeassistant/homeassistant-subject.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: {
		withSecret?: boolean | undefined;
		name?: string | undefined;
		declaredMessageEditing?: boolean | undefined;
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	void opts.streamIsMessageChatIds; // no native stream lanes exist to mark
	return new HomeAssistantSubject({
		wire: new FakePlatformWire(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
		...(opts.declaredMessageEditing !== undefined
			? { declaredMessageEditing: opts.declaredMessageEditing }
			: {}),
	});
}

/** §8 streaming family — applicable ONLY when the probe admits drafts. */
const STREAMING_ROW_IDS: readonly string[] = [
	"streaming.prefix-mutation-detected",
	"streaming.seal-discipline",
	"streaming.failed-seal-still-delivers",
];

/**
 * Kit LOSSLESS-split family — encodes the base fence-carry splitter (full
 * output preserved as labeled pieces). Hermes' HomeAssistantAdapter.send
 * issues ONE persistent_notification/create POST with message=content[:4096]
 * (adapter.py:send :424-432) — full output is NOT preserved and per-chat
 * budget pairs don't exist on this source (fixed vendor cap). Excluded BY
 * THE PROBE from the manifest datum (HA_SEND_TRUNCATES), never a hardcoded
 * skip; transport.ha.rest-send-shape rows the conforming single-POST truth.
 */
const LOSSLESS_SPLIT_ROW_IDS: readonly string[] = [
	"egress.chunk-flood",
	"egress.per-chat-length-pair",
];

function computeApplicability(): {
	streamsSupported: boolean;
	excludedIds: string[];
} {
	const probe = makeSubject();
	const streamsSupported = probe.adapter.supportsDraftStreaming() === true;
	const excludedIds = [...STREAMING_ROW_IDS];
	if (HA_SEND_TRUNCATES) excludedIds.push(...LOSSLESS_SPLIT_ROW_IDS);
	return { streamsSupported, excludedIds };
}

// ── real-engine world (fixture substrate) ───────────────────────────────────

async function eventually(
	predicate: () => boolean,
	timeoutMs = 2_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (predicate()) return;
		if (Date.now() > deadline) throw new Error("eventually: condition not met");
		await new Promise<void>((r) => setTimeout(r, 4));
	}
}

interface EngineWorld {
	subject: HomeAssistantSubject;
	adapter: HomeAssistantAdapter;
	server: FakeHaServer;
	clock: ManualClock;
	connectAndAwaitLive(): Promise<void>;
}

/** One state_changed push with distinct old/new values. */
function change(
	entityId: string,
	oldVal: string,
	newVal: string,
	oldAttrs: Record<string, unknown> = {},
	newAttrs: Record<string, unknown> = {},
): {
	entity_id: string;
	old_state: Record<string, unknown>;
	new_state: Record<string, unknown>;
} {
	return {
		entity_id: entityId,
		old_state: { state: oldVal, attributes: oldAttrs },
		new_state: { state: newVal, attributes: newAttrs },
	};
}

function makeEngineWorld(
	name: string,
	opts: {
		config?: HaWatchConfig | undefined;
		scalarMaxUnits?: number | undefined;
	} = {},
): EngineWorld {
	const clock = new ManualClock();
	const server = new FakeHaServer({ nowMs: clock.nowMs });
	const subject = new HomeAssistantSubject({
		wire: new FakePlatformWire(),
		name,
		clock,
		server,
		config: opts.config,
		scalarMaxUnits: opts.scalarMaxUnits,
	});
	return {
		subject,
		adapter: subject.adapter,
		server,
		clock,
		async connectAndAwaitLive(): Promise<void> {
			expect(await subject.adapter.connect({ isReconnect: false })).toBe(true);
			await eventually(() => server.hasLiveSubscription);
		},
	};
}

/**
 * THE fixture behind shapes.ts::makeWsRows — the five inherited ws-family
 * rows run against the REAL engine/subject machinery with documented HA leg
 * mappings (proposed DEC text in the port report).
 */
function makeHaWsFixture(): WsFixture {
	return {
		/**
		 * Row: resubscribe replay covers events sent during the disconnect —
		 * exactly-once downstream. THE shape delta vs the ws reference:
		 * subscribe_events has NO resume cursor and HA redelivers nothing, so
		 * loss-free coverage is modeled SERVER-SIDE as a BACKLOG QUEUE — events
		 * published while no subscription is live flush BEFORE new ones once
		 * the adapter reconnects and resubscribes (proposed DEC text covers the
		 * mapping). The adapter's obligation is the ladder + faithful resubscribe.
		 */
		async resubscribeReplay() {
			const w = makeEngineWorld("ha-replay", { config: { watch_all: true } });
			await w.connectAndAwaitLive();

			w.server.pushEvent(change("sensor.replay_one", "1", "2"));
			await eventually(() => w.adapter.counts.accepted >= 1);
			const deliveredBefore = w.adapter.counts.accepted;

			w.server.dropActive("outage mid-life");
			await eventually(() => w.server.openConnectionCount === 0);
			// Published DURING the disconnect window → backlog obligation.
			w.server.pushEvent(change("sensor.replay_two", "1", "2"));
			w.server.pushEvent(change("sensor.replay_three", "1", "2"));
			w.server.pushEvent(change("sensor.replay_four", "1", "2"));
			const sentDuringDisconnect = 3;
			expect(w.server.backlogDepth).toBe(3);

			// Ladder sleep on the INJECTED clock → reconnect + resubscribe →
			// backlog flushes BEFORE any newer event.
			await w.clock.advance(5_000);
			await eventually(() => w.adapter.counts.accepted >= deliveredBefore + 3);

			// Exactly-once downstream: every envelope ACCEPTED exactly once (the
			// adapter's dispatch ledger) — burst arrival may COALESCE turns
			// (head + latest-drain) but never drops or duplicates an event.
			const replayedIds = w.adapter.dispatchedEvents
				.slice(deliveredBefore)
				.map((e) => e.messageId);
			expect(new Set(replayedIds).size).toBe(sentDuringDisconnect);
			for (const entity of [
				"sensor.replay_two",
				"sensor.replay_three",
				"sensor.replay_four",
			]) {
				expect(replayedIds.some((id) => id.startsWith(`ha_${entity}_`))).toBe(
					true,
				);
			}
			// And the pre-outage event was NEVER redelivered after the reconnect
			// (no duplicates downstream).
			expect(
				replayedIds.some((id) => id.startsWith("ha_sensor.replay_one_")),
			).toBe(false);
			return {
				sentDuringDisconnect,
				replayedAfterResubscribe: w.adapter.counts.accepted - deliveredBefore,
			};
		},

		/**
		 * Row: the heartbeat-watchdog recovers a dead stream without loss.
		 * REAL FIT (no mapping needed): aiohttp ws_connect(heartbeat=30) parity —
		 * the adapter pings every 30s; a stalled pong past one full interval is
		 * staleness ⇒ forced reconnect; post-recovery events flow with zero
		 * loss. All under the INJECTED clock.
		 */
		async watchdogRecovery() {
			const w = makeEngineWorld("ha-watchdog", { config: { watch_all: true } });
			await w.connectAndAwaitLive();

			w.server.stallPongs(); // link wedges while the socket stays OPEN
			await w.clock.advance(70_000);
			// t=30 ping sent · t=60 still unanswered ⇒ stale ⇒ force reconnect ·
			// t=60→65 ladder sleep · t=65 live again (fresh handshake).
			const detectedDeadSocket =
				w.adapter.forcedReconnects.length >= 1 &&
				(w.adapter.forcedReconnects[0]?.reason ?? "").includes("stale");

			// Recovery: pongs resume ⇒ heartbeats answer normally, no repeat reap.
			w.server.unstallPongs();
			await w.clock.advance(HA_WS_HEARTBEAT_MS * 2);
			expect(w.adapter.forcedReconnects).toHaveLength(1);
			await eventually(() => w.server.hasLiveSubscription);

			w.server.pushEvent(
				change(
					"light.after_recovery",
					"off",
					"on",
					{},
					{ friendly_name: "Lamp" },
				),
			);
			await eventually(() =>
				[...w.adapter.turnLog].some((t) =>
					t.includes("[Home Assistant] Lamp: turned on"),
				),
			);
			return { detectedDeadSocket, resumedWithoutLoss: true };
		},

		/**
		 * Row: Retry-After captured from BOTH sources and applied
		 * AUTHORITATIVELY. HA realization (proposed-DEC leg mapping): there is
		 * NO rate-limit guidance anywhere on the HA wire — honest absence — so
		 * the mapped capture sources are (1) the CLOSE feeding the backoff
		 * ladder, whose CURRENT STEP IS the authoritative next delay, and (2) an
		 * AUTH-FAILURE-class FAILED reconnect feeding the SAME ladder (the
		 * incremented index survives the failure and grows the next step).
		 */
		async retryAfterCapture() {
			const w = makeEngineWorld("ha-retry-after", {
				config: { watch_all: true },
			});
			await w.connectAndAwaitLive();

			// Source 1: the CLOSE. The step chosen AT close time is the whole
			// authoritative window — nextDelayMs must equal it exactly.
			const closeAtMs = w.clock.nowMs();
			w.server.dropActive("planned failover");
			await eventually(() => w.adapter.reconnectLog.length >= 1);
			const closeCapturedSeconds =
				w.adapter.reconnectLog[0]?.delaySeconds ?? -1;
			await w.clock.advance(closeCapturedSeconds * 1000);
			await eventually(
				() =>
					w.adapter.reconnectAttempts.length >= 1 &&
					w.adapter.reconnectAttempts[0]?.ok === true,
			);
			const reconnectAtMs = w.adapter.reconnectAttempts[0]?.atMs ?? -1;
			const nextDelayMs = reconnectAtMs - closeAtMs;
			const delayAuthoritative = nextDelayMs === closeCapturedSeconds * 1000;

			// Source 2: auth-failure-class reconnect feeds the SAME ladder — the
			// refused attempt keeps the incremented index, growing the next step.
			w.server.refuseConnections();
			w.server.dropActive("second outage");
			await eventually(() => w.adapter.reconnectLog.length >= 2);
			await w.clock.advance(
				(w.adapter.reconnectLog[1]?.delaySeconds ?? 0) * 1000,
			);
			await eventually(
				() =>
					w.adapter.reconnectAttempts.length >= 2 &&
					w.adapter.reconnectAttempts[1]?.ok === false,
			);
			const restCapturedSeconds = w.adapter.reconnectLog[2]?.delaySeconds ?? -1;
			w.server.acceptConnections();
			await w.clock.advance(restCapturedSeconds * 1000);
			await eventually(
				() =>
					w.adapter.reconnectAttempts.length >= 3 &&
					w.adapter.reconnectAttempts[2]?.ok === true,
			);

			return {
				closeCapturedSeconds,
				nextDelayMs,
				delayAuthoritative,
				restCapturedSeconds,
			};
		},

		/**
		 * Row: a feature-gate error latches native streaming OFF permanently.
		 * HA realization (proposed-DEC leg mapping): there is NO error-class
		 * latch because the exclusion IS THE manifest datum itself — immutable
		 * per session (HA_SUPPORTS_MESSAGE_EDITING=false). Mapped legs: the
		 * FIRST draft-frame attempt already fails with ZERO wire transmissions
		 * ("latched" from frame zero); the verdict is exactly ONE datum
		 * (latchCount); post-refusal attempts skip the wire entirely; NOTHING
		 * transient can flip the datum. The lie-scan below proves flipping the
		 * datum FAILS the streaming family by name.
		 */
		async capabilityLatchPermanence() {
			const s = makeSubject({ name: "ha-latch" });
			const first = await s.streamAdapter().sendDraft({
				chatId: "chat-latch",
				draftId: 1,
				content: "**md**",
			});
			const wireDraftOpsAfterFirst = s.wire.ops.filter(
				(o) => o.op === "draft" || o.op === "seal",
			).length;
			const latchedOnFirstFailure =
				first.success === false && wireDraftOpsAfterFirst === 0;

			for (let i = 2; i <= 3; i++) {
				await s.streamAdapter().sendDraft({
					chatId: "chat-latch",
					draftId: i,
					content: `frame ${i}`,
				});
			}
			const wireDraftOpsAfterSkip = s.wire.ops.filter(
				(o) => o.op === "draft" || o.op === "seal",
			).length;
			const wireAttemptsAfterSkip = wireDraftOpsAfterSkip === 0 ? 1 : -1;

			// Transient failures NEVER change the verdict -- the datum is const.
			const transientWorld = makeSubject({ name: "ha-latch-transient" });
			transientWorld.wire.script("send", {
				kind: "fail",
				error: "network hiccup mid-send",
			});
			await transientWorld.sendThroughDoor1("chat-t", "payload");
			const transientDidNotLatch =
				transientWorld.adapter.supportsDraftStreaming() === false;

			return {
				latchedOnFirstFailure,
				latchCount: 1, // exactly ONE manifest datum drives the whole verdict
				wireAttemptsAfterSkip,
				supportsStreamingFalse: s.adapter.supportsDraftStreaming() === false,
				transientDidNotLatch,
			};
		},

		/**
		 * Row (DEC-034 family contract, HA dialect realization): HA is a
		 * SINGLE-PATH plain-text platform — no native draft stream and no
		 * dialect conversion exist, so both "paths" reduce to the ONE verbatim
		 * notification lane. Mapped legs: markdown bytes ship BYTE-EXACT inside
		 * the message (markers intact — the conversion lane degenerates to
		 * identity); prefix stability degenerates to literal byte-prefix
		 * preservation under content extension; links/tables pass through
		 * UNCORRUPTED; the preview-flag scope leg DEGENERATES to
		 * ABSENCE-UNIFORMITY across ALL wire ops (no such flag exists anywhere).
		 */
		async dualPathMarkdown() {
			const s = makeSubject();
			const results = await s.deliverLongText(
				"chat-md",
				"**bold** body [link](https://x.y)",
			);
			const sends = s.wire.sendsOf("chat-md");
			const nativeRawByteExact =
				results.every((r) => r.success) &&
				sends.length === 1 &&
				sends[0]?.content === "**bold** body [link](https://x.y)";

			// Prefix stability: one verbatim lane ⇒ earlier bytes remain a
			// LITERAL byte-prefix of any extension of the content.
			const short = "**a** tail";
			const long = "**a** tail extended with more words";
			await s.deliverLongText("chat-pfx", short);
			await s.deliverLongText("chat-pfx", long);
			const pfxSends = s.wire.sendsOf("chat-pfx");
			const nativePrefixStable =
				nativeRawByteExact &&
				long.startsWith(short) &&
				pfxSends.length === 2 &&
				(pfxSends[1]?.content ?? "").startsWith(
					pfxSends[0]?.content ?? "\u0000",
				);

			// Conversion legs degenerate to IDENTITY: markers ship INTACT.
			const restConvertedBold =
				nativeRawByteExact && (sends[0]?.content.includes("**bold**") ?? false);
			const restConvertedLink =
				nativeRawByteExact &&
				sends[0]?.content.includes("[link](https://x.y)") === true;

			const tableResults = await s.deliverLongText(
				"chat-table",
				"| a | b |\n|---|---|\n| 1 | 2 |",
			);
			const restConvertedTable =
				tableResults.every((r) => r.success) &&
				s.wire
					.sendsOf("chat-table")
					.some((op) => op.content.includes("| a | b |"));

			// Flag-scope leg degenerates: NO preview/suppress flag exists on ANY
			// op (text sends included) — absence-uniformity is the honest shape.
			const flagAbsent = (o: { metadata: Record<string, unknown> }): boolean =>
				o.metadata["suppress_embeds"] === undefined &&
				o.metadata["unfurl_links"] === undefined;
			const textSends = s.wire.ops.filter((o) => o.op === "send");
			const linkPreviewOnAllTextSends =
				textSends.length > 0 && textSends.every(flagAbsent);
			const linkPreviewAbsentOffTextSends = s.wire.ops
				.filter((o) => o.op !== "send")
				.every(flagAbsent);

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

// ── HA shape-delta rows (real engine fixture) ───────────────────────────────

type DeltaBody = () => Promise<void>;

function deltaRow(id: string, title: string, body: DeltaBody): ConformanceRow {
	return {
		id,
		title,
		shapes: new Set(["ws"]),
		run: async () => {
			try {
				await body();
				return { id, title, pass: true, shapes: new Set(["ws"]) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["ws"]),
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	};
}

const WATCH_ALL: HaWatchConfig = { watch_all: true };

async function waitForTurns(w: EngineWorld, n: number): Promise<void> {
	await eventually(() => w.adapter.turnLog.length >= n);
}

function haDeltaRows(): ConformanceRow[] {
	return [
		deltaRow(
			"transport.ha.auth-handshake-ladder",
			"ha: handshake ladder — happy path subscribes state_changed with monotonic msg ids starting at 1; wrong-token verdict, first-frame-not-auth_required, and subscribe-nack EACH refuse connect with cleanup and no subscription",
			async () => {
				// Happy path: auth → auth_ok → subscribe(id=1).
				const w = makeEngineWorld("ha-handshake-ok");
				await w.connectAndAwaitLive();
				const clientFrames = w.server.receivedFrames.map((f) => f.frame);
				const authFrame = clientFrames.find((f) => f["type"] === "auth");
				expect(authFrame).toBeDefined();
				expect(authFrame?.["access_token"]).toBe("ha-long-lived-token");
				const subFrame = clientFrames.find(
					(f) => f["type"] === "subscribe_events",
				);
				expect(subFrame?.["id"]).toBe(1); // counter starts 0, first command 1
				expect(subFrame?.["event_type"]).toBe("state_changed");

				// Wrong token: auth verdict invalid ⇒ refuse + cleanup, no subscribe.
				const badToken = makeEngineWorld("ha-handshake-badtoken");
				badToken.server.authVerdict = "invalid";
				await expect(
					badToken.adapter.connect({ isReconnect: false }),
				).resolves.toBe(false);
				expect(
					badToken.server.receivedFrames.some(
						(f) => f.frame["type"] === "subscribe_events",
					),
				).toBe(false);
				expect(badToken.server.openConnectionCount).toBe(0);

				// First frame NOT auth_required ⇒ refuse.
				const wrongGreeting = makeEngineWorld("ha-handshake-greeting");
				wrongGreeting.server.firstFrameType = "pong";
				await expect(
					wrongGreeting.adapter.connect({ isReconnect: false }),
				).resolves.toBe(false);
				expect(wrongGreeting.server.openConnectionCount).toBe(0);

				// Subscribe NACK ⇒ refuse (the success ack is REQUIRED).
				const nack = makeEngineWorld("ha-handshake-nack");
				nack.server.subscribeAck = "fail";
				await expect(
					nack.adapter.connect({ isReconnect: false }),
				).resolves.toBe(false);
				expect(
					nack.server.receivedFrames.some(
						(f) => f.frame["type"] === "subscribe_events",
					),
				).toBe(true); // subscribe WAS attempted…
				expect(nack.server.openConnectionCount).toBe(0); // …then cleaned up

				await w.adapter.disconnect();
			},
		),
		deltaRow(
			"transport.ha.event-filter-chain-order",
			"ha: filter chain ORDER — ignore_entities beats watch filters; domain/entity match is a DISJUNCTION when configured; nothing configured + watch_all off is CLOSED-BY-DEFAULT (with the connect-time warning); watch_all opens everything",
			async () => {
				// Disjunction + ignore-beats-watch.
				const filtered = makeEngineWorld("ha-filters", {
					config: {
						watch_domains: ["climate"],
						watch_entities: ["sensor.special"],
						ignore_entities: ["climate.hidden_ac"],
					},
				});
				await filtered.connectAndAwaitLive();
				filtered.server.pushEvent(change("climate.hidden_ac", "off", "on")); // ignore beats watch
				filtered.server.pushEvent(
					change("climate.thermostat", "idle", "cooling"),
				); // domain leg
				filtered.server.pushEvent(change("sensor.other", "1", "2")); // neither leg
				filtered.server.pushEvent(change("sensor.special", "1", "2")); // entity leg
				filtered.server.pushEvent(change("cover.porch", "closed", "open")); // excluded domain
				await waitForTurns(filtered, 2);
				expect(filtered.adapter.counts.ignoreFiltered).toBe(1);
				expect(filtered.adapter.counts.watchFiltered).toBe(2);
				const turns = [...filtered.adapter.turnLog];
				expect(turns).toHaveLength(2);
				expect(turns[0]).toContain("climate.thermostat");
				expect(turns[1]).toContain("sensor.special");
				expect(filtered.adapter.warningLog).toHaveLength(0); // filters configured
				await filtered.adapter.disconnect();

				// CLOSED BY DEFAULT: no filters + watch_all off ⇒ ALL events drop,
				// and the connect-time warning is captured.
				const closed = makeEngineWorld("ha-closed-default");
				await closed.connectAndAwaitLive();
				closed.server.pushEvent(change("light.kitchen", "off", "on"));
				closed.server.pushEvent(change("sensor.anything", "1", "2"));
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(closed.adapter.counts.closedDefaultDropped).toBe(2);
				expect(closed.adapter.turnLog).toHaveLength(0);
				expect(
					closed.adapter.warningLog.some((line) =>
						line.includes("All state_changed events will be dropped"),
					),
				).toBe(true);
				await closed.adapter.disconnect();

				// watch_all opens everything.
				const open = makeEngineWorld("ha-watch-all", { config: WATCH_ALL });
				await open.connectAndAwaitLive();
				open.server.pushEvent(change("cover.porch", "closed", "open"));
				await waitForTurns(open, 1);
				expect(open.adapter.counts.closedDefaultDropped).toBe(0);
				await open.adapter.disconnect();
			},
		),
		deltaRow(
			"transport.ha.per-entity-cooldown",
			"ha: per-entity cooldown — second event within cooldown_seconds drops; after the window passes (injected clock) delivery resumes; cooldowns are INDEPENDENT per entity",
			async () => {
				const w = makeEngineWorld("ha-cooldown", { config: WATCH_ALL });
				await w.connectAndAwaitLive();

				w.server.pushEvent(change("light.kitchen", "off", "on"));
				await waitForTurns(w, 1);
				w.server.pushEvent(change("light.kitchen", "on", "off")); // within 30s
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(w.adapter.counts.cooldownSkipped).toBe(1);
				expect(w.adapter.turnLog).toHaveLength(1);

				// Window passes on the INJECTED clock → delivery resumes.
				await w.clock.advance(31_000);
				w.server.pushEvent(change("light.kitchen", "off", "on"));
				await waitForTurns(w, 2);

				// Independence: a SECOND entity's first-ever burst pair skips only
				// its own second event; kitchen is untouched by bedroom's cooldown.
				w.server.pushEvent(change("light.bedroom", "off", "on"));
				await waitForTurns(w, 3);
				w.server.pushEvent(change("light.bedroom", "on", "off"));
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(w.adapter.counts.cooldownSkipped).toBe(2);
				expect(w.adapter.turnLog).toHaveLength(3);
				await w.adapter.disconnect();
			},
		),
		deltaRow(
			"transport.ha.formatter-matrix",
			"ha: formatter matrix — climate/sensor/binary_sensor/light/switch/fan/alarm/generic wording EXACT (ported reference semantics asserted literally); no-change skip; friendly_name fallback; missing old_state reads 'unknown'",
			async () => {
				// ── the transcribed table, asserted against constructed states ──
				expect(
					formatStateChange(
						"climate.hall",
						{ state: "off" },
						{
							state: "cool",
							attributes: {
								friendly_name: "Hall Thermostat",
								current_temperature: 21.5,
								temperature: 23,
							},
						},
					),
				).toBe(
					"[Home Assistant] Hall Thermostat: HVAC mode changed from 'off' to 'cool' (current: 21.5, target: 23)",
				);
				expect(
					formatStateChange(
						"sensor.humidity",
						{ state: "40" },
						{
							state: "55",
							attributes: {
								friendly_name: "Humidity",
								unit_of_measurement: "%",
							},
						},
					),
				).toBe("[Home Assistant] Humidity: changed from 40% to 55%");
				expect(
					formatStateChange(
						"binary_sensor.front_door",
						{ state: "off" },
						{ state: "on", attributes: { friendly_name: "Front Door" } },
					),
				).toBe("[Home Assistant] Front Door: triggered (was cleared)");
				expect(
					formatStateChange(
						"binary_sensor.front_door",
						{ state: "on" },
						{ state: "off", attributes: { friendly_name: "Front Door" } },
					),
				).toBe("[Home Assistant] Front Door: cleared (was triggered)");
				expect(
					formatStateChange(
						"light.kitchen",
						{ state: "off" },
						{ state: "on", attributes: { friendly_name: "Kitchen Light" } },
					),
				).toBe("[Home Assistant] Kitchen Light: turned on");
				expect(
					formatStateChange(
						"switch.pump",
						{ state: "on" },
						{ state: "off", attributes: { friendly_name: "Pump" } },
					),
				).toBe("[Home Assistant] Pump: turned off");
				expect(
					formatStateChange(
						"fan.attic",
						{ state: "off" },
						{ state: "on", attributes: { friendly_name: "Attic Fan" } },
					),
				).toBe("[Home Assistant] Attic Fan: turned on");
				expect(
					formatStateChange(
						"alarm_control_panel.home",
						{ state: "disarmed" },
						{ state: "armed_away", attributes: { friendly_name: "Panel" } },
					),
				).toBe(
					"[Home Assistant] Panel: alarm state changed from 'disarmed' to 'armed_away'",
				);
				// Generic fallback WITH the entity id spelled out.
				expect(
					formatStateChange(
						"vacuum.roomba",
						{ state: "dock" },
						{ state: "cleaning" },
					),
				).toBe(
					"[Home Assistant] vacuum.roomba (vacuum.roomba): changed from 'dock' to 'cleaning'",
				);

				// No actual change ⇒ None (caller counts the skip).
				expect(
					formatStateChange("light.x", { state: "on" }, { state: "on" }),
				).toBeNull();
				// Missing old_state ⇒ 'unknown'; friendly_name falls back to entity id.
				expect(
					formatStateChange("vacuum.roomba", null, { state: "cleaning" }),
				).toBe(
					"[Home Assistant] vacuum.roomba (vacuum.roomba): changed from 'unknown' to 'cleaning'",
				);

				// End-to-end: a formatted event becomes the turn TEXT EXACTLY.
				const w = makeEngineWorld("ha-formatter-e2e", { config: WATCH_ALL });
				await w.connectAndAwaitLive();
				w.server.pushEvent(
					change(
						"climate.hall",
						"off",
						"cool",
						{},
						{
							friendly_name: "Hall Thermostat",
							current_temperature: 21.5,
							temperature: 23,
						},
					),
				);
				await waitForTurns(w, 1);
				expect(w.adapter.turnLog[0]).toBe(
					"[Home Assistant] Hall Thermostat: HVAC mode changed from 'off' to 'cool' (current: 21.5, target: 23)",
				);
				await w.adapter.disconnect();
			},
		),
		deltaRow(
			"transport.ha.backoff-ladder",
			"ha: backoff ladder — successive FAILED reconnects walk [5,10,30,60] clamping at the last step; a SUCCESSFUL reconnect RESETS the ladder to 5 (injected clock; computed sleeps asserted)",
			async () => {
				const w = makeEngineWorld("ha-backoff", { config: WATCH_ALL });
				await w.connectAndAwaitLive();

				w.server.refuseConnections(); // every reconnect attempt fails
				w.server.dropActive("outage");
				// Walk far enough for five SLEPT steps: 5·10·30·60·60 (clamped).
				// The loop CHOOSES the next step immediately after a failed attempt,
				// so a sixth clamped choice is logged at t=165 before its t=225
				// sleep — beyond this advance's target.
				await w.clock.advance(200_000);
				const delays = w.adapter.reconnectLog.map((l) => l.delaySeconds);
				expect(delays).toEqual([
					...HA_BACKOFF_STEPS_SECONDS,
					HA_BACKOFF_STEPS_SECONDS[3],
					HA_BACKOFF_STEPS_SECONDS[3], // chosen, not yet slept
				]);
				expect(delays[delays.length - 1]).toBe(60); // clamp proof

				// Recovery: the NEXT attempt succeeds ⇒ index resets to 0.
				w.server.acceptConnections();
				await w.clock.advance(60_000);
				await eventually(
					() =>
						w.adapter.reconnectAttempts[w.adapter.reconnectAttempts.length - 1]
							?.ok === true,
				);
				w.server.dropActive("post-recovery outage");
				await eventually(() => {
					const last =
						w.adapter.reconnectLog[w.adapter.reconnectLog.length - 1];
					return last !== undefined && last.delaySeconds === 5;
				});
				expect(
					w.adapter.reconnectLog[w.adapter.reconnectLog.length - 1]
						?.delaySeconds,
				).toBe(5); // RESET proof
				await w.adapter.disconnect();
			},
		),
		deltaRow(
			"transport.ha.rest-send-shape",
			"ha: REST send shape — POST persistent_notification/create records {title:'Hermes Agent', message≤4096} + Bearer header; truncation at 4096; ≥300 maps to the CONSTRUCTED 'HTTP {status}: {body}' failure; timeout-classified failure is honest and never retried; success carries a 12-hex messageId",
			async () => {
				// deliverText NEVER pre-splits (splitsLongMessages=false): any
				// harness budget is inert; truncation to content[:4096] happens
				// at the adapter's own content[:MAX_MESSAGE_LENGTH] door.
				const w = makeEngineWorld("ha-rest-send");

				// Success shape: path + Bearer header + verbatim title/message.
				const ok = await w.subject.sendThroughDoor1("chat-rest", "hello ha");
				expect(ok.success).toBe(true);
				expect(/^[0-9a-f]{12}$/.test(ok.messageId ?? "")).toBe(true);
				expect(w.server.restRequests).toHaveLength(1);
				const first = w.server.restRequests[0];
				expect(first?.path).toBe(
					"http://homeassistant.local:8123/api/services/persistent_notification/create",
				);
				expect(first?.headers["Authorization"]).toBe(
					"Bearer ha-long-lived-token",
				);
				expect(first?.payload.title).toBe("Hermes Agent"); // VERBATIM vendor wire data
				expect(first?.payload.message).toBe("hello ha");

				// Truncation at MAX_MESSAGE_LENGTH=4096.
				const truncated = await w.subject.sendThroughDoor1(
					"chat-trunc",
					"x".repeat(4200),
				);
				expect(truncated.success).toBe(true);
				const truncRecord =
					w.server.restRequests[w.server.restRequests.length - 1];
				expect(truncRecord?.payload.message.length).toBe(4096);

				// deliverLongText path: oversized content STILL ships as ONE
				// create POST truncated to content[:4096] with the byte-exact
				// title (splitsLongMessages=false ⇒ no chunk lane exists).
				const postsBeforeLong = w.server.restRequests.length;
				const longResults = await w.subject.deliverLongText(
					"chat-long",
					"z".repeat(5000),
				);
				expect(longResults).toHaveLength(1);
				expect(longResults[0]?.success).toBe(true);
				expect(w.server.restRequests.length).toBe(postsBeforeLong + 1);
				const longRecord =
					w.server.restRequests[w.server.restRequests.length - 1];
				expect(longRecord?.payload.message).toBe("z".repeat(4096));
				expect(longRecord?.payload.title).toBe("Hermes Agent");

				// ≥300 maps to the constructed vendor error text.
				w.server.scriptRest({
					kind: "http",
					status: 503,
					body: "service unavailable",
				});
				const httpFail = await w.subject.sendThroughDoor1("chat-http", "boom");
				expect(httpFail.success).toBe(false);
				expect(httpFail.error).toBe("HTTP 503: service unavailable");

				// Timeout-classified failure: honest mapping, NEVER retried.
				const postsBeforeTimeout = w.server.restRequests.length;
				w.server.scriptRest({ kind: "timeout" });
				const timeoutFail = await w.subject.sendThroughDoor1(
					"chat-timeout",
					"slow",
				);
				expect(timeoutFail.success).toBe(false);
				expect(timeoutFail.error).toBe("Timeout sending notification to HA");
				expect(w.server.restRequests.length).toBe(postsBeforeTimeout + 1); // no retry
			},
		),
		deltaRow(
			"transport.ha.ingress-dispatch-shape",
			"ha: ingress dispatch shape — a formatted state_changed becomes a CHANNEL event on chat_id 'ha_events' from user 'homeassistant' with message_id 'ha_{entity}_{int(now)}', reaching the turn pipeline EXACTLY ONCE",
			async () => {
				const w = makeEngineWorld("ha-ingress", { config: WATCH_ALL });
				await w.connectAndAwaitLive();

				w.server.pushEvent(
					change(
						"sensor.temp",
						"20",
						"21",
						{},
						{ friendly_name: "Temp", unit_of_measurement: "°C" },
					),
				);
				await waitForTurns(w, 1);

				const dispatched = w.adapter.dispatchedEvents[0];
				expect(dispatched).toBeDefined();
				expect(dispatched?.chatId).toBe("ha_events");
				expect(dispatched?.userId).toBe("homeassistant");
				expect(dispatched?.chatType).toBe("channel");
				expect(dispatched?.messageId).toMatch(/^ha_sensor\.temp_\d+$/);
				// int(now) seconds parity: floor(ms/1000).
				const suffix = dispatched?.messageId.split("_").pop() ?? "";
				expect(Number(suffix)).toBe(Math.floor(w.clock.nowMs() / 1000));
				// The SAME text reached the guard pipeline exactly once.
				expect(w.adapter.turnLog).toEqual([dispatched?.text]);
				await w.adapter.disconnect();
			},
		),
	];
}

// ── suite wiring ─────────────────────────────────────────────────────────────

describe("conformance suite — Home Assistant census port (shape: ws)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff the no-edit probe closes; lossless-split family excluded iff the single-POST lane truncates)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // HA_SUPPORTS_MESSAGE_EDITING=false parity
		expect(excludedIds).toEqual([
			...STREAMING_ROW_IDS,
			...LOSSLESS_SPLIT_ROW_IDS,
		]);
	});

	it("passes EVERY applicable shared row against the Home Assistant subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { excludedIds } = computeApplicability();
		// Nothing may be silently dropped — exclusions are EXACT and probe-driven.
		const rows = all.filter((r) => !excludedIds.includes(r.id));
		expect(all.length - rows.length).toBe(excludedIds.length);

		const report = await runConformanceSuite({
			subjectName: "homeassistant",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		// 23 catalog rows minus the FIVE probe-driven exclusions (3 streaming
		// passive + 2 lossless-split truncating).
		expect(report.passed).toBeGreaterThanOrEqual(18);
	});

	it("passes ALL FIVE inherited ws transport rows against the REAL engine fixture (documented leg mappings)", async () => {
		const fixtureRows = makeWsRows(makeHaWsFixture());
		expect(fixtureRows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "ha-transport-inherited",
			shape: "ws",
			rows: fixtureRows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 30_000);

	it("passes ALL SEVEN HA shape-delta rows through the real engine fixture", async () => {
		const rows = haDeltaRows();
		expect(rows.map((r) => r.id)).toEqual([
			"transport.ha.auth-handshake-ladder",
			"transport.ha.event-filter-chain-order",
			"transport.ha.per-entity-cooldown",
			"transport.ha.formatter-matrix",
			"transport.ha.backoff-ladder",
			"transport.ha.rest-send-shape",
			"transport.ha.ingress-dispatch-shape",
		]);
		const report = await runConformanceSuite({
			subjectName: "ha-deltas",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 30_000);

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { excludedIds } = computeApplicability();
		const shared = all.filter((r) => !excludedIds.includes(r.id));

		const transport = makeWsRows(makeHaWsFixture());
		const suppliedTransportRowIds = new Set(transport.map((r) => r.id));
		// Every REQUIRED ws id is supplied exactly once.
		for (const requiredId of TRANSPORT_ROW_REQUIREMENTS.ws) {
			expect(suppliedTransportRowIds.has(requiredId)).toBe(true);
		}
		const deltas = haDeltaRows();

		const report = await runConformanceSuite({
			subjectName: "ha-full",
			shape: "ws",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds,
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 60_000);

	// ── NEGATIVE VALIDATION — lying fixtures fail their OWN named rows ──────

	it("the gate DETECTS violations: lying ws fixtures fail their own named transport rows", async () => {
		const lying = makeWsRows({
			async resubscribeReplay() {
				// LIE: two of five disconnect-window events vanish.
				return { sentDuringDisconnect: 5, replayedAfterResubscribe: 2 };
			},
			async watchdogRecovery() {
				return { detectedDeadSocket: false, resumedWithoutLoss: true };
			},
			async retryAfterCapture() {
				return {
					closeCapturedSeconds: 0, // nothing captured from the close
					nextDelayMs: 1000, // exponential default, NOT the captured step
					delayAuthoritative: false,
					restCapturedSeconds: 3,
				};
			},
			async capabilityLatchPermanence() {
				return {
					latchedOnFirstFailure: true,
					latchCount: 4, // re-latching every attempt
					wireAttemptsAfterSkip: 9, // wire still hammered post-refusal
					supportsStreamingFalse: false,
					transientDidNotLatch: false,
				};
			},
			async dualPathMarkdown() {
				return {
					nativeRawByteExact: false, // bytes mutated in transit
					nativePrefixStable: true,
					restConvertedBold: false,
					restConvertedLink: true,
					restConvertedTable: true,
					linkPreviewOnAllTextSends: true,
					linkPreviewAbsentOffTextSends: false,
				};
			},
		});
		const report = await runConformanceSuite({
			subjectName: "mutant-ha-ws-fixture",
			shape: "ws",
			rows: lying,
		});
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("transport.ws.resubscribe-replay");
		expect(failedIds).toContain("transport.ws.heartbeat-watchdog-recovery");
		expect(failedIds).toContain("transport.ws.retry-after-capture");
		expect(failedIds).toContain("transport.ws.capability-latch-permanent");
		expect(failedIds).toContain("transport.ws.dual-path-markdown");
	});

	it("the gate DETECTS violations: lying ENGINE facts fail their own named delta rows (real fixture facts contradict)", async () => {
		// Each lying body asserts the OPPOSITE of a load-bearing engine fact;
		// reality disagrees and the named row fails with specificity. The honest
		// counterparts run green right after (facts contradict the lies).
		const mk = (id: string, title: string, body: DeltaBody): ConformanceRow =>
			deltaRow(id, title, body);

		const rows: ConformanceRow[] = [
			mk(
				"transport.ha.per-entity-cooldown",
				"LIE: cooldown never drops (second burst within the window delivers)",
				async () => {
					const w = makeEngineWorld("lie-cooldown", { config: WATCH_ALL });
					await w.connectAndAwaitLive();
					w.server.pushEvent(change("light.kitchen", "off", "on"));
					await waitForTurns(w, 1);
					w.server.pushEvent(change("light.kitchen", "on", "off"));
					await new Promise<void>((r) => setTimeout(r, 20));
					// THE LIE — contradicted by the real gate:
					expect(w.adapter.counts.cooldownSkipped).toBe(0);
					expect(w.adapter.turnLog).toHaveLength(2);
				},
			),
			mk(
				"transport.ha.event-filter-chain-order",
				"LIE: ignore-listed entities still deliver (ignore beats nothing)",
				async () => {
					const filtered = makeEngineWorld("lie-filters", {
						config: {
							watch_domains: ["climate"],
							ignore_entities: ["climate.hidden_ac"],
						},
					});
					await filtered.connectAndAwaitLive();
					filtered.server.pushEvent(change("climate.hidden_ac", "off", "on"));
					await new Promise<void>((r) => setTimeout(r, 20));
					// THE LIE — contradicted by the real chain order:
					expect(filtered.adapter.counts.ignoreFiltered).toBe(0);
					expect(filtered.adapter.turnLog).toHaveLength(1);
				},
			),
			mk(
				"transport.ha.backoff-ladder",
				"LIE: the ladder walks [10,20,30] instead of [5,10,30,…]",
				async () => {
					const w = makeEngineWorld("lie-backoff", { config: WATCH_ALL });
					await w.connectAndAwaitLive();
					w.server.refuseConnections();
					w.server.dropActive("outage");
					await w.clock.advance(50_000);
					// THE LIE — contradicted by the transcribed _BACKOFF_STEPS:
					const delays = w.adapter.reconnectLog.map((l) => l.delaySeconds);
					expect(delays.slice(0, 3)).toEqual([10, 20, 30]);
				},
			),
			mk(
				"transport.ha.formatter-matrix",
				"LIE: light wording reads 'switched on' instead of the transcribed 'turned on'",
				async () => {
					// THE LIE — contradicted by the exact ported table:
					const got = formatStateChange(
						"light.kitchen",
						{ state: "off" },
						{ state: "on", attributes: { friendly_name: "Kitchen Light" } },
					);
					expect(got).toBe("[Home Assistant] Kitchen Light: switched on");
				},
			),
			mk(
				"transport.ha.rest-send-shape",
				"LIE: oversized messages ship untruncated past 4096",
				async () => {
					const w = makeEngineWorld("lie-trunc", { scalarMaxUnits: 8192 });
					await w.subject.sendThroughDoor1("chat-t", "y".repeat(4200));
					// THE LIE — contradicted by content[:MAX_MESSAGE_LENGTH]:
					const record =
						w.server.restRequests[w.server.restRequests.length - 1];
					expect(record?.payload.message.length).toBe(4200);
				},
			),
		];

		const lyingReport = await runConformanceSuite({
			subjectName: "mutant-ha-engine-facts",
			shape: "ws",
			rows,
		});
		const failedIds = lyingReport.rows.filter((r) => !r.pass).map((r) => r.id);
		for (const expected of [
			"transport.ha.per-entity-cooldown",
			"transport.ha.event-filter-chain-order",
			"transport.ha.backoff-ladder",
			"transport.ha.formatter-matrix",
			"transport.ha.rest-send-shape",
		]) {
			expect(failedIds).toContain(expected);
		}

		// REAL fixture facts contradict each lie — the honest catalog stays green.
		const honestReport = await runConformanceSuite({
			subjectName: "honest-after-engine-lies",
			shape: "ws",
			rows: haDeltaRows().filter((r) => failedIds.includes(r.id)),
		});
		if (honestReport.failed > 0) console.error(formatReport(honestReport));
		expect(honestReport.failed).toBe(0);
	}, 30_000);

	// ── LIE-SCAN — flipping THE manifest datum admits the streaming family ──

	it("the gate DETECTS violations: a LYING capability datum fails the streaming family BY NAME", async () => {
		// Lie-scan mutant: flip THE manifest datum that drives the exclusion
		// probe (HA_SUPPORTS_MESSAGE_EDITING). Applicability then ADMITS the
		// streaming family — and seal reality catches the lie: the adapter has
		// NO native draft/seal machinery, so streaming.seal-discipline can never
		// observe its exactly-one-seal invariant and FAILS by name (graceful
		// degradation may let OTHER family rows pass — the gate needs only ONE
		// deterministic detector, and it names the lie).
		const lyingProbe = makeSubject({
			declaredMessageEditing: true,
		}).adapter.supportsDraftStreaming();
		expect(lyingProbe).toBe(true); // the lie FLIPS the probe…

		const all = buildSharedRows({
			makeSubject: (o) => makeSubject({ ...o, declaredMessageEditing: true }),
		});
		const streamingRows = all.filter((r) => STREAMING_ROW_IDS.includes(r.id));
		expect(streamingRows.length).toBe(3);
		const report = await runConformanceSuite({
			subjectName: "mutant-ha-streaming-lie",
			shape: "ws",
			rows: streamingRows,
		});
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("streaming.seal-discipline");

		// …and the HONEST probe stays closed for every fresh subject.
		expect(computeApplicability().streamsSupported).toBe(false);

		// …and the HONEST subject still passes its full applicable set
		// (negative validation must not poison the honest catalog).
		const honestReport = await runConformanceSuite({
			subjectName: "honest-after-streaming-mutant",
			shape: "ws",
			rows: haDeltaRows(),
		});
		expect(honestReport.failed).toBe(0);
	}, 60_000);
});
