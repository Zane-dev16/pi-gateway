// CONFORMANCE WIRING — the Photon Spectrum (iMessage) census port vs the
// executable 04 §8 matrix (DEC-002 gate applies to every new platform).
//
//   1. ALL applicable SHARED rows pass for shape="ws" against the REAL
//      kit-built PhotonSubject. Applicability is COMPUTED from capability
//      data: supportsDraftStreaming() is false BY METHOD (iMessage has no
//      draft lanes), so the streaming family is excluded BY THE PROBE, never
//      by a hardcoded skip.
//   2. The FIVE inherited ws transport rows run against the REAL engine
//      fixture (makeRealPhotonFixture) with PHOTON'S OWN vendor-class mapping
//      documented IN each row title. transport.ws.retry-after-capture is an
//      HONEST CLASS DELTA: its row BODY differs from shapes.ts::makeWsRows
//      because this wire carries NO Retry-After field anywhere — the negative
//      is pinned behaviorally (source truth per DEC-026; noted in the report).
//   3. Eleven photon shape-delta rows close the census: mention gating,
//      allowlist, dedupe window, reaction lifecycle, markdown dual path,
//      typing cooldown, watchdog decision matrix, error classification,
//      no-edit posture, clarify/poll surfaces, fatal self-cancel detachment.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: a mutant that fabricates a numeric retry hint onto
//      failed sends fails ITS OWN named row (retry-after-capture) alone.

import { describe, expect, it } from "vitest";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type { SendResult } from "../../pi_gateway/streaming/adapter-seam.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import type { ConformanceSubject } from "./harness.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";

import { makePhotonWorld, type PhotonWorld } from "../photon/photon-world.js";
import {
	makeRealPhotonFixture,
	type PhotonTransportFixture,
} from "../photon/photon-world.js";
import { makePhotonSubject } from "../photon/photon-subject.js";
import {
	PhotonAdapter,
	matchesMentionPatterns,
} from "../photon/photon-adapter.js";
import { DedupeWindow, PHOTON_DEDUP_TTL_MS } from "../photon/dedupe.js";
import {
	FakeSidecarServer,
	photonDmEvent,
	photonGroupEvent,
	photonPollOptionEvent,
	photonReactionEvent,
} from "../photon/sidecar-wire.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { PHOTON_TARGET_NOT_ALLOWED_MESSAGE } from "../photon/manifest.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makePhotonSubject({
		wire: new FakePlatformWire(),
		sidecar: new FakeSidecarServer(),
		clock: new ManualClock(),
		spawner: scheduler.spawner,
		scheduler,
		scalarMaxUnits: 64, // harness-scale budget mirrors the reference subjects
		withSecret: opts.withSecret,
		name: opts.name,
	});
}

/** §8 streaming family — applicable ONLY when draft streaming is supported. */
const STREAMING_ROW_IDS: readonly string[] = [
	"streaming.prefix-mutation-detected",
	"streaming.seal-discipline",
	"streaming.failed-seal-still-delivers",
];

function computeApplicability(): {
	streamsSupported: boolean;
	excludedIds: string[];
} {
	const probe = makeSubject();
	const streamsSupported =
		probe.adapter.supportsDraftStreaming() === true &&
		probe.adapter.supportsAsyncDelivery === true;
	return { streamsSupported, excludedIds: [...STREAMING_ROW_IDS] };
}

/** Fresh BARE adapter for delta rows (no guard needed — dispatchedEvents). */
function deltaEngine(
	opts: {
		env?: Record<string, string | undefined>;
		config?: Record<string, unknown>;
		world?: PhotonWorld;
		sleepFn?: (ms: number) => Promise<void>;
		onRespawn?: (reason: string) => void | Promise<void>;
		notifyFatalError?: () => Promise<void>;
	} = {},
): { engine: PhotonAdapter; sidecar: FakeSidecarServer; clock: ManualClock } {
	if (opts.world !== undefined) {
		return {
			engine: opts.world.engine,
			sidecar: opts.world.sidecar,
			clock: opts.world.clock,
		};
	}
	const clock = new ManualClock();
	const sidecar = new FakeSidecarServer();
	const scopedEnv: Record<string, string | undefined> = {
		PHOTON_PROJECT_ID: "delta-project-id",
		PHOTON_PROJECT_SECRET: "delta-project-secret",
		...(opts.env ?? {}),
	};
	const engine = new PhotonAdapter({
		sidecar,
		nowMs: () => clock.nowMs(),
		envReader: (name) => scopedEnv[name],
		...(opts.config === undefined ? {} : { config: opts.config }),
		...(opts.sleepFn === undefined ? {} : { sleepFn: opts.sleepFn }),
		...(opts.onRespawn === undefined ? {} : { onRespawn: opts.onRespawn }),
		...(opts.notifyFatalError === undefined
			? {}
			: { notifyFatalError: opts.notifyFatalError }),
	});
	return { engine, sidecar, clock };
}

/** Scoped credential-only env reader (nested-ternary-free). */
function photonCredEnv(
	extra?: Record<string, string | undefined>,
): (name: string) => string | undefined {
	const map: Record<string, string | undefined> = {
		PHOTON_PROJECT_ID: "delta-project-id",
		PHOTON_PROJECT_SECRET: "delta-project-secret",
		...(extra ?? {}),
	};
	return (name) => map[name];
}

async function connect(engine: PhotonAdapter): Promise<void> {
	expect(await engine.connect({ isReconnect: false })).toBe(true);
}

// ── photon transport rows (shapes.ts WsFixture contract, REAL bodies) ──────

/**
 * The five ws-family rows with PHOTON'S vendor-class mapping in each title.
 * Row bodies assert the observables returned by the REAL fixture legs.
 */
export function makePhotonWsRows(
	fixture: PhotonTransportFixture,
): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: () => Promise<Record<string, unknown>>,
		asserts: (r: Record<string, unknown>) => string | null,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["ws"]),
		run: async () => {
			try {
				const result = await body();
				const problem = asserts(result);
				if (problem)
					return {
						id,
						title,
						pass: false,
						shapes: new Set(["ws"]),
						detail: problem,
					};
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
	});

	return [
		mk(
			"transport.ws.resubscribe-replay",
			"ws resubscribe replay [vendor mapping: the spectrum-ts gRPC stream is AT-LEAST-ONCE and REPLAYS after reconnects — no resume cursor exists; exactly-once downstream is the DEDUPE window's job (48h TTL / 4000-entry bound)]",
			async () => {
				return { ...(await fixture.resubscribeReplay()) };
			},
			(r) => {
				if (r.replayedAfterResubscribe !== r.sentDuringDisconnect)
					return `replay gap (${String(r.sentDuringDisconnect)} sent vs ${String(r.replayedAfterResubscribe)} delivered post-replay)`;
				if (r.uniqueIdsDownstream !== 5)
					return `downstream saw ${String(r.uniqueIdsDownstream)} unique ids, expected 5`;
				if (r.duplicateDeliveries !== 0)
					return `redelivered duplicates must be DEDUPED (${String(r.duplicateDeliveries)} leaked)`;
				return null;
			},
		),
		mk(
			"transport.ws.heartbeat-watchdog-recovery",
			"ws heartbeat watchdog recovery [vendor mapping: THE presence WATCHDOG itself — spectrum-ts cannot see a half-open zombie socket, so injected-clock probes classify alive/hung/inconclusive and N consecutive HUNG probes fire EXACTLY ONE respawn signal; live traffic suppresses probing and resets failures]",
			async () => {
				return { ...(await fixture.watchdogRecovery()) };
			},
			(r) => {
				if (r.detectedDeadSocket !== true || r.resumedWithoutLoss !== true) {
					return "watchdog must detect death AND resume cleanly";
				}
				if (r.respawnSignalCount !== 1)
					return `threshold crossing must signal respawn EXACTLY ONCE (got ${String(r.respawnSignalCount)})`;
				if (r.liveTrafficTick !== "skipped-idle")
					return `live traffic must suppress the next probe (got ${String(r.liveTrafficTick)})`;
				return null;
			},
		),
		mk(
			"transport.ws.retry-after-capture",
			"ws retry-after capture [HONEST CLASS DELTA vs the family row: this wire carries NO Retry-After field anywhere — retryable classification is PATTERN-based (_PHOTON_RETRYABLE_PATTERNS); permanent-vs-retryable verdicts drive soft-fail vs respawn-ladder; a numeric hint embedded in an error body is NEVER surfaced or honored — mutation-detectable by fabricating one]",
			async () => {
				return { ...(await fixture.retryAfterCapture()) };
			},
			(r) => {
				if (r.wireCarriedFabricatedHint !== true)
					return "fixture must PROVE the fabricated hint rode the error body";
				if (r.hintSurfacedAnywhere !== false)
					return "a numeric retry hint surfaced on a SendResult — this wire has NO Retry-After channel";
				if (r.patternRetryableRetriedAndRecovered !== true)
					return "pattern-retryable failure must retry and recover";
				if (r.permanentFailureNotRetried !== true)
					return "permanent failure must return as-is WITHOUT retries or fallback resends";
				for (const [k, v] of Object.entries(
					r.classificationTable as Record<string, boolean>,
				)) {
					if (v !== true) return `classification table leg ${k} violated`;
				}
				return null;
			},
		),
		mk(
			"transport.ws.capability-latch-permanent",
			"ws capability latch permanent [vendor mapping: SUPPORTS_MESSAGE_EDITING=False (no iMessage edit API ⇒ streaming cursor suppressed) realized as a STATIC capability — edit/draft attempts answer Not supported with ZERO sidecar calls even after repeated attempts; never probed on the wire]",
			async () => {
				return { ...(await fixture.capabilityLatchPermanence()) };
			},
			(r) => {
				if (r.supportsStreamingFalse !== true)
					return "supportsDraftStreaming() must be false";
				if (r.allAttemptsNotSupported !== true)
					return "every edit/draft attempt must answer Not supported";
				if (r.repeatedAttemptsStillZero !== true)
					return "repeated attempts must still make ZERO wire calls";
				if (Number(r.sidecarCallsDuringAttempts) !== 0)
					return `static capability probed the wire ${String(r.sidecarCallsDuringAttempts)} times`;
				return null;
			},
		),
		mk(
			"transport.ws.dual-path-markdown",
			'ws dual-path markdown [vendor mapping: markdown ships BYTE-EXACT via /send with format:"markdown" (iMessage renders natively); PHOTON_MARKDOWN=false strips markup via the shared equivalent and OMITS the flag; URL-only candidates divert to /send-richlink while prose stays on /send; preview artifacts trailing a link are suppressed inside the 30s window]',
			async () => {
				return { ...(await fixture.dualPathMarkdown()) };
			},
			(r) => {
				for (const leg of [
					"markdownByteExactWithFlag",
					"plainModeStrippedNoFlag",
					"urlOnlyRoutedToRichlink",
					"proseUrlStayedOnSend",
					"markdownOffKillsRichlinkLane",
					"previewSuppressedInsideWindow",
					"previewPassedOutsideWindow",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				return null;
			},
		),
	];
}

// ── photon shape-delta rows ─────────────────────────────────────────────────

function photonDeltaRows(): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: () => Promise<void>,
	): ConformanceRow => ({
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
	});

	const lastDispatched = (engine: PhotonAdapter): IncomingEvent | undefined =>
		engine.dispatchedEvents.at(-1);

	return [
		mk(
			"photon.mention-gating-matrix",
			"photon mention gating: require_mention DEFAULTS FALSE compiling exactly TWO Hermes wake-word patterns; group messages drop without a wake word when enabled; DMs are NEVER gated; custom config/env patterns replace defaults (comma-separated, JSON list); invalid regexes skip keeping good ones; leading wake words strip before dispatch",
			async () => {
				// Defaults: OFF + two Hermes wake patterns.
				const defaults = deltaEngine().engine;
				expect(defaults.requireMention).toBe(false);
				expect(defaults.mentionPatterns.length).toBe(2);

				// Group drops without a wake word; DM never gated.
				const gated = deltaEngine({
					config: { require_mention: true },
				}).engine;
				await gated.dispatchInbound(
					photonGroupEvent("just chatting, no wake word", "grp-1"),
				);
				expect(gated.dispatchedEvents).toHaveLength(0);
				await gated.dispatchInbound(photonDmEvent("no wake word here", "dm-1"));
				expect(gated.dispatchedEvents).toHaveLength(1);
				expect(lastDispatched(gated)?.text).toBe("no wake word here");

				// Wake word passes AND strips (leading match only).
				await gated.dispatchInbound(
					photonGroupEvent("hermes help me", "grp-2"),
				);
				expect(gated.dispatchedEvents).toHaveLength(2);
				expect(lastDispatched(gated)?.text).toBe("help me");

				// Custom patterns from config.extra.
				const amos = deltaEngine({
					config: {
						require_mention: true,
						mention_patterns: ["(?<![\\w@])@?amos\\b[,:\\-]?"],
					},
				}).engine;
				expect(amos.requireMention).toBe(true);
				expect(amos.mentionPatterns.length).toBe(1);
				expect(
					matchesMentionPatterns(amos.mentionPatterns, "amos help me"),
				).toBe(true);
				expect(
					matchesMentionPatterns(amos.mentionPatterns, "hermes help me"),
				).toBe(false);

				// Env comma-separated.
				const envBased = deltaEngine({
					env: {
						PHOTON_REQUIRE_MENTION: "true",
						PHOTON_MENTION_PATTERNS: "bot\\b, assistant\\b",
					},
				}).engine;
				expect(envBased.requireMention).toBe(true);
				expect(envBased.mentionPatterns.length).toBe(2);
				expect(
					matchesMentionPatterns(envBased.mentionPatterns, "hey bot"),
				).toBe(true);

				// Invalid regex dropped, good one kept.
				const mixed = deltaEngine({
					config: {
						require_mention: true,
						mention_patterns: ["(unclosed", "good\\b"],
					},
				}).engine;
				expect(mixed.mentionPatterns.length).toBe(1);
				expect(
					matchesMentionPatterns(mixed.mentionPatterns, "a good thing"),
				).toBe(true);

				// The second default pattern also matches bare "hermes".
				expect(
					matchesMentionPatterns(defaults.mentionPatterns, "hermes status"),
				).toBe(true);
			},
		),
		mk(
			"photon.allowlist-matrix",
			"photon allowlist gate: PHOTON_ALLOWED_USERS csv admits known E.164 senders and DENIES unknown ones; PHOTON_ALLOW_ALL_USERS (dev flag) overrides the list; with NEITHER configured the gate is unconstrained at adapter layer (gateway authz owns default-deny)",
			async () => {
				const restricted = deltaEngine({
					env: { PHOTON_ALLOWED_USERS: "+15551234567, +15559876543" },
				}).engine;
				await restricted.dispatchInbound(
					photonDmEvent("known sender", "allow-1"),
				);
				expect(restricted.dispatchedEvents).toHaveLength(1);
				await restricted.dispatchInbound(
					photonDmEvent("stranger danger", "allow-2", {
						senderId: "+19998887777",
					}),
				);
				expect(restricted.dispatchedEvents).toHaveLength(1);

				const openDev = deltaEngine({
					env: {
						PHOTON_ALLOW_ALL_USERS: "true",
						PHOTON_ALLOWED_USERS: "+15551234567",
					},
				}).engine;
				await openDev.dispatchInbound(
					photonDmEvent("anyone in dev mode", "allow-3", {
						senderId: "+10000000000",
					}),
				);
				expect(openDev.dispatchedEvents).toHaveLength(1);

				const unconstrained = deltaEngine().engine;
				await unconstrained.dispatchInbound(
					photonDmEvent("no list configured", "allow-4", {
						senderId: "+12223334444",
					}),
				);
				expect(unconstrained.dispatchedEvents).toHaveLength(1);
			},
		),
		mk(
			"photon.dedupe-window",
			"photon dedupe window (_is_duplicate semantics): duplicate messageId within the window drops at LINE level; distinct ids pass; HARD capacity eviction at 4000 entries frees the OLDEST; TTL expiry at exactly 48h under the injected clock releases the id",
			async () => {
				const clock = new ManualClock();
				const { engine } = deltaEngine();
				const line = JSON.stringify(photonDmEvent("ping", "dup-1"));
				await engine.onInboundLine(line);
				await engine.onInboundLine(line); // same messageId → deduped
				expect(engine.dispatchedEvents).toHaveLength(1);

				// Unit-level window semantics.
				const window = new DedupeWindow({ nowMs: () => clock.nowMs() });
				expect(window.isDuplicate("id-1")).toBe(false);
				expect(window.isDuplicate("id-1")).toBe(true);
				expect(window.isDuplicate("id-2")).toBe(false);
				expect(window.isDuplicate("id-1")).toBe(true); // still dup

				// Capacity eviction: hard bound 4000, oldest insertion evicted.
				for (let i = 0; i < 4000; i += 1) {
					expect(window.isDuplicate(`burst-${i}`)).toBe(false);
				}
				expect(window.size).toBe(4000);
				window.isDuplicate("burst-4000"); // evicts burst-0 … to fit
				expect(window.size).toBeLessThanOrEqual(4000);
				expect(window.isDuplicate("burst-0")).toBe(false); // forgotten

				// TTL expiry: within the window dup; past 48h released.
				const ttlWindow = new DedupeWindow({ nowMs: () => clock.nowMs() });
				expect(ttlWindow.isDuplicate("x")).toBe(false);
				clock.advance(PHOTON_DEDUP_TTL_MS - 1);
				expect(ttlWindow.isDuplicate("x")).toBe(true);
				clock.advance(1);
				expect(ttlWindow.isDuplicate("x")).toBe(false);
			},
		),
		mk(
			"photon.reaction-lifecycle",
			"photon reaction lifecycle: PHOTON_REACTIONS gates EVERYTHING (disabled ⇒ zero calls); enabled start taps 👀 eyes on the inbound message; completion swaps remove-then-add 👍/👎 by outcome (CANCELLED leaves unreacted); every call soft-fails false on sidecar throw; inbound tapbacks route ONLY on our own outbound targets carrying reply_to triple + null targetText hydration",
			async () => {
				const chatId = "+15551234567";

				// Disabled: zero calls.
				const offEvent: IncomingEvent = {
					messageType: "text",
					text: "hi",
					source: {
						platform: "photon",
						chatType: "dm",
						userId: chatId,
						chatId,
					},
					messageId: "target-msg-1",
				};
				const offParts = deltaEngine();
				expect(await offParts.engine.onProcessingStart(offEvent)).toBe(false);
				expect(
					await offParts.engine.onProcessingComplete(offEvent, "success"),
				).toEqual([]);
				expect(offParts.sidecar.calls).toHaveLength(0);

				// Enabled: eyes start; thumbs by outcome.
				const on = deltaEngine({
					env: { PHOTON_REACTIONS: "true" },
				});
				expect(await on.engine.onProcessingStart(offEvent)).toBe(true);
				let reactCalls = reactionCallsOf(on.sidecar);
				expect(reactCalls).toHaveLength(1);
				expect(reactCalls[0]).toEqual({
					path: "/react",
					body: { spaceId: chatId, messageId: "target-msg-1", emoji: "👀" },
				});
				const upshot = await on.engine.onProcessingComplete(
					offEvent,
					"success",
				);
				expect(upshot).toEqual([true, true]);
				reactCalls = reactionCallsOf(on.sidecar);
				expect(reactCalls[1]?.path).toBe("/unreact");
				expect(reactCalls[2]).toEqual({
					path: "/react",
					body: { spaceId: chatId, messageId: "target-msg-1", emoji: "👍" },
				});
				const downshot = await on.engine.onProcessingComplete(
					{ ...offEvent, messageId: "target-msg-2" },
					"failure",
				);
				expect(downshot).toEqual([true, true]);
				expect(reactionCallsOf(on.sidecar).at(-1)?.body["emoji"]).toBe("👎");
				const cancelled = await on.engine.onProcessingComplete(
					{ ...offEvent, messageId: "target-msg-3" },
					"cancelled",
				);
				expect(cancelled).toEqual([true]); // unreact ONLY, no re-add

				// Soft failure on throw.
				const brittle = deltaEngine({
					env: { PHOTON_REACTIONS: "true" },
				});
				brittle.sidecar.script(
					"*",
					{ kind: "transport-error", message: "sidecar down" },
					{ kind: "transport-error", message: "sidecar down again" },
				);
				expect(await brittle.engine.addReaction(chatId, "m", "👀")).toBe(false);
				expect(await brittle.engine.removeReaction(chatId, "m")).toBe(false);

				// Inbound routing: tapback on OUR message dispatches the triple.
				const router = deltaEngine().engine;
				router.recordSentMessage("bot-msg-1"); // we sent this earlier
				await router.dispatchInbound(photonReactionEvent({ emoji: "❤️" }));
				expect(router.dispatchedEvents).toHaveLength(1);
				const reactionEvent = lastDispatchOf(router);
				expect(reactionEvent?.text).toBe("reaction:added:❤️");
				expect(reactionEvent?.replyToMessageId).toBe("bot-msg-1");
				expect(reactionEvent?.metadata?.["reply_to_text"]).toBe(
					"the bot's earlier reply",
				);
				expect(reactionEvent?.metadata?.["reply_to_is_own_message"]).toBe(true);
				expect(reactionEvent?.source?.chatId).toBe(chatId);

				// Null targetText hydration (attachment-only target).
				await router.dispatchInbound(
					photonReactionEvent({
						emoji: "👍",
						targetText: null,
						messageId: "reaction-evt-2",
					}),
				);
				expect(lastDispatchOf(router)?.metadata?.["reply_to_text"]).toBeNull();

				// Human↔human tapbacks are not for us.
				const quiet = deltaEngine().engine;
				await quiet.dispatchInbound(
					photonReactionEvent({
						targetDirection: "inbound",
						targetId: "human-msg-9",
					}),
				);
				expect(quiet.dispatchedEvents).toHaveLength(0);
			},
		),
		mk(
			"photon.markdown-dual-path-detail",
			'photon markdown dual path detail: format_message passes through VERBATIM by default (supports_code_blocks mirrors the switch); PHOTON_MARKDOWN=false strips via the shared equivalent; the /send body carries format:"markdown" only when enabled (older-sidecar compat omits the key); oversized text truncates at exactly 8000 codepoints',
			async () => {
				const md = "**bold** and `code`";
				const def = deltaEngine();
				expect(def.engine.formatMessage(md)).toBe(md);
				expect(def.engine.supportsCodeBlocks).toBe(true);
				await connect(def.engine);
				await def.engine.photonSend("+1555", md);
				const body = def.sidecar.callsOf("/send")[0]?.body;
				expect(body?.["text"]).toBe(md);
				expect(body?.["format"]).toBe("markdown");

				const plain = deltaEngine({
					env: { PHOTON_MARKDOWN: "false" },
				});
				expect(plain.engine.supportsCodeBlocks).toBe(false);
				expect(plain.engine.formatMessage("**bold** and _soft_")).toBe(
					"bold and soft",
				);
				await connect(plain.engine);
				await plain.engine.photonSend("+1555", "**bold** and _soft_");
				const plainBody = plain.sidecar.callsOf("/send")[0]?.body;
				expect(plainBody?.["text"]).toBe("bold and soft");
				expect(plainBody?.["format"]).toBeUndefined();

				// Truncation at 8000 codepoints (astral chars count as ONE).
				const long = "🎉".repeat(9000); // 9000 codepoints
				await plain.engine.photonSend("+1555", long);
				const truncated = plain.sidecar.callsOf("/send").at(-1)?.body[
					"text"
				] as string;
				let cp = 0;
				for (const _ of truncated) cp += 1;
				expect(cp).toBe(8000);
			},
		),
		mk(
			"photon.typing-cooldown",
			"photon typing cooldown: first start passes; a repeat within 5s (injected clock) is suppressed without a wire call; after the window it passes again; stop ALWAYS passes and clears the window; scripted failures soft-fail",
			async () => {
				const world = makePhotonWorld({ name: "typing-cooldown" });
				const { engine, sidecar, clock } = world;
				await connect(engine);

				expect(await engine.sendTyping("+1555")).toBe(true);
				expect(sidecar.callsOf("/typing")).toHaveLength(1);
				expect(await engine.sendTyping("+1555")).toBe(false);
				expect(sidecar.callsOf("/typing")).toHaveLength(1);

				clock.advance(5_001);
				expect(await engine.sendTyping("+1555")).toBe(true);
				expect(sidecar.callsOf("/typing")).toHaveLength(2);

				// stop ALWAYS passes and clears the cooldown.
				clock.advance(10);
				expect(await engine.stopTyping("+1555")).toBe(true);
				expect(sidecar.callsOf("/typing").at(-1)?.body["state"]).toBe("stop");
				expect(await engine.sendTyping("+1555")).toBe(true);
				expect(sidecar.callsOf("/typing").at(-1)?.body["state"]).toBe("start");

				// Per-chat isolation + soft failure.
				expect(await engine.sendTyping("+1666")).toBe(true);
				sidecar.script("/typing", {
					kind: "transport-error",
					message: "refused",
				});
				expect(await engine.stopTyping("+1666")).toBe(false);
			},
		),
		mk(
			"photon.watchdog-decision-matrix",
			"photon watchdog decision matrix: defaults are 600s interval / 10s timeout / 3 max failures / ENABLED; explicit zero interval disables entirely; _note_upstream_activity resets failures AND stamps activity; inconclusive verdicts NEVER count either direction; interleaved live probes prevent respawn; three consecutive hung ticks fire EXACTLY ONE respawn then reset",
			async () => {
				// Config defaults.
				const def = deltaEngine().engine;
				expect(def.probeIntervalMs).toBe(600_000);
				expect(def.probeTimeoutMs).toBe(10_000);
				expect(def.probeMaxFailures).toBe(3);
				expect(def.probeEnabled).toBe(true);

				// Explicit zero disables (first_set honors explicit 0).
				const disabled = deltaEngine({
					env: { PHOTON_PROBE_INTERVAL_SECONDS: "0" },
				}).engine;
				expect(disabled.probeEnabled).toBe(false);
				expect(await disabled.watchdogTick()).toBe("disabled");

				// Activity reset + idle-skip.
				const world = makePhotonWorld({
					name: "watchdog-matrix",
					fastWatchdog: true,
				});
				const { engine, sidecar, clock } = world;
				await connect(engine);
				engine.noteUpstreamActivity();
				expect(engine.currentProbeFailures).toBe(0);
				expect(await engine.watchdogTick()).toBe("skipped-idle");

				// Inconclusive: strictly no action either direction.
				clock.advance(250);
				sidecar.script(
					"/probe",
					{ kind: "transport-error", message: "connection refused" },
					{ kind: "transport-error", message: "connection refused" },
				);
				expect(await engine.watchdogTick()).toBe("inconclusive");
				expect(await engine.watchdogTick()).toBe("inconclusive");
				expect(engine.currentProbeFailures).toBe(0);
				expect(engine.respawnSignals).toHaveLength(0);

				// Interleaved ALIVE prevents respawn (failures reset between).
				clock.advance(250);
				sidecar.script(
					"/probe",
					{ kind: "hung" },
					{ kind: "ok" },
					{ kind: "hung" },
					{ kind: "hung" },
					{ kind: "hung" },
				);
				expect(await engine.watchdogTick()).toBe("hung");
				expect(engine.currentProbeFailures).toBe(1);
				clock.advance(250);
				expect(await engine.watchdogTick()).toBe("alive");
				expect(engine.currentProbeFailures).toBe(0);
				clock.advance(250);
				expect(await engine.watchdogTick()).toBe("hung");
				clock.advance(250);
				expect(await engine.watchdogTick()).toBe("hung");
				expect(engine.respawnSignals).toHaveLength(0); // never hit 3 in a row

				// Three consecutive ⇒ EXACTLY ONE respawn, then reset.
				engine.noteUpstreamActivity(); // fresh liveness proof (f = 0)
				clock.advance(250);
				sidecar.script(
					"/probe",
					{ kind: "hung" },
					{ kind: "hung" },
					{ kind: "hung" },
				);
				expect(await engine.watchdogTick()).toBe("hung");
				expect(await engine.watchdogTick()).toBe("hung");
				expect(await engine.watchdogTick()).toBe("respawned");
				expect(engine.respawnSignals).toEqual(["3 consecutive hung probes"]);
				expect(engine.currentProbeFailures).toBe(0); // reset by respawn
			},
		),
		mk(
			"photon.error-classification-table",
			"photon error classification: the seven _PHOTON_RETRYABLE_PATTERNS all classify retryable; explicit retryable=false and auth_or_config veto wins; empty/null errors are not retryable; structured SidecarHttpErrors carry raw_response classification with the target_not_allowed canonical message replacing raw upstream text; permanent failures NEVER double-send; timeout-shaped errors never retry; missing credentials fail fatal non-retryable",
			async () => {
				const { engine, sidecar } = deltaEngine();
				for (const [leg, ok] of Object.entries(classificationTable(engine))) {
					expect(ok).toBe(true);
					void leg;
				}

				await connect(engine);

				// Structured retryable failure → raw_response carries the class.
				sidecar.script("*", {
					kind: "error",
					status: 503,
					error: "upstream_unavailable",
					retryable: true,
				});
				const retriableResult = await engine.sidecarSend("+1555", "x");
				expect(retriableResult.success).toBe(false);
				expect(retriableResult.retryable).toBe(true);
				const rawRetryable = (
					retriableResult as { rawResponse?: Record<string, unknown> }
				).rawResponse;
				expect(rawRetryable?.["error_class"]).toBe("sidecar_error");

				// Permanent auth failure → classified permanent, canonical veto.
				sidecar.script("*", {
					kind: "error",
					status: 403,
					error: "auth_or_config: invalid project secret",
					errorClass: "auth_or_config",
					retryable: false,
				});
				const permanent = await engine.sidecarSend("+1555", "y");
				expect(engine.isPermanentSidecarFailure(permanent)).toBe(true);
				expect((permanent as { rawResponse?: unknown }).rawResponse).toEqual({
					error_class: "auth_or_config",
					retryable: false,
				});

				// target_not_allowed gets the CANONICAL explanation.
				sidecar.script("*", {
					kind: "error",
					status: 403,
					error: "Target not allowed for this project",
					errorClass: "target_not_allowed",
					retryable: false,
				});
				const targetBlocked = await engine.sidecarSend("+1555", "z");
				expect(targetBlocked.error).toContain(
					PHOTON_TARGET_NOT_ALLOWED_MESSAGE,
				);
				expect(engine.isPermanentSidecarFailure(targetBlocked)).toBe(true);

				// Permanent failures never double-send: sendWithRetryPhoton makes
				// exactly ONE attempt.
				sidecar.clearScripts();
				sidecar.script("*", {
					kind: "error",
					status: 403,
					error: "auth_or_config: nope",
					errorClass: "auth_or_config",
					retryable: false,
				});
				const sendsBeforePermanent = sidecar.callsOf("/send").length;
				const onceOnly = await engine.sendWithRetryPhoton("+1555", "p");
				expect(onceOnly.success).toBe(false);
				expect(sidecar.callsOf("/send").length - sendsBeforePermanent).toBe(1);

				// Timeout-shaped errors never retry NOR fall back.
				sidecar.clearScripts();
				sidecar.script("*", {
					kind: "error",
					status: 504,
					error: "request timed out",
					retryable: true,
				});
				const sendsBeforeTimeout = sidecar.callsOf("/send").length;
				const timedOut = await engine.sendWithRetryPhoton("+1555", "q");
				expect(timedOut.success).toBe(false);
				expect(sidecar.callsOf("/send").length - sendsBeforeTimeout).toBe(1);

				// Network-pattern failure retries ONCE, then plain-text fallback
				// (richlink+markdown flags suppressed on the fallback).
				sidecar.clearScripts();
				const sendsBeforeFallback = sidecar.callsOf("/send").length;
				sidecar.script(
					"*",
					{
						kind: "error",
						status: 503,
						error: "upstream_unavailable",
						retryable: true,
					},
					{
						kind: "error",
						status: 503,
						error: "upstream_unavailable",
						retryable: true,
					},
				);
				const fellBack = await engine.sendWithRetryPhoton("+1555", "r");
				const sendsAfterFallback = sidecar.callsOf("/send").length;
				expect(sendsAfterFallback - sendsBeforeFallback).toBe(3); // initial + retry + fallback
				const fallbackBody = sidecar.callsOf("/send").at(-1)?.body as Record<
					string,
					unknown
				>;
				expect(fellBack.success).toBe(true);
				expect(fallbackBody["format"]).toBeUndefined(); // fallback is PLAIN

				// Missing credentials: connect()'s own credential guard fires fatal
				// MISSING_CREDENTIALS non-retryable (env resolves EMPTY STRINGS so
				// enablement still passes — the undefined case is the kit's separate
				// loud-disable path, proven in identity.missing-secret-loud-disable).
				const credless = deltaEngine({
					env: {
						PHOTON_PROJECT_ID: "",
						PHOTON_PROJECT_SECRET: "",
					},
				}).engine;
				expect(await credless.connect({ isReconnect: false })).toBe(false);
				expect(credless.hasFatalError).toBe(true);
				expect(credless.fatalErrorCode).toBe("MISSING_CREDENTIALS");
				expect(credless.fatalErrorRetryable).toBe(false);
			},
		),
		mk(
			"photon.no-edit-posture",
			"photon no-edit posture: SUPPORTS_MESSAGE_EDITING=false as DATA — door-level editMessage answers Not supported with ZERO sidecar calls; native drafts identical; supportsDraftStreaming() false by METHOD probe (the streaming cursor stays suppressed)",
			async () => {
				const world = makePhotonWorld({ name: "no-edit-posture" });
				const { engine, sidecar, subject } = world;
				await connect(engine);
				const callsAtStart = sidecar.calls.length; // healthz ping only

				const result = await engine.editMessage(
					"+1555",
					"spc-msg-9",
					"**edited**",
				);
				expect(result.success).toBe(false);
				expect(result.error).toBe("Not supported");
				expect(sidecar.calls.length).toBe(callsAtStart);
				// Static capability answers WITHOUT even consuming an egress-door
				// admission (base.editMessage bypasses the chokepoint entirely).
				expect(subject.doorAudit().length).toBe(0);

				const draft = await engine.sendDraft({
					chatId: "+1555",
					draftId: 1,
					content: "draft",
				});
				expect(draft.success).toBe(false);
				expect(draft.error).toBe("Not supported");
				expect(sidecar.calls.length).toBe(callsAtStart);
				expect(engine.supportsDraftStreaming()).toBe(false);
			},
		),
		mk(
			"photon.clarify-poll-surfaces",
			"photon clarify/poll surfaces: multiple-choice clarify POSTs /send-poll {spaceId,title,options} and arms text-capture; <2 options or empty title refuse WITHOUT a call; poll wire-failure falls back to the numbered-text clarify; open-ended clarifies stay plain text; inbound poll votes dispatch the chosen option as TEXT while deselections and empty titles drop",
			async () => {
				const { engine, sidecar } = deltaEngine();
				await connect(engine);

				// Choices → native poll + text-capture armed.
				const poll = await engine.sendClarify(
					"+155****4567",
					"Pick one",
					["A", "B", "C"],
					"clar-1",
				);
				expect(poll.success).toBe(true);
				const pollBody = sidecar.callsOf("/send-poll")[0]?.body;
				expect(pollBody).toEqual({
					spaceId: "+155****4567",
					title: "Pick one",
					options: ["A", "B", "C"],
				});
				expect(engine.clarifyArmed.has("clar-1")).toBe(true);

				// Validation refusals never reach the wire — but per source truth
				// (send_clarify falls back on ANY poll failure) the numbered-text
				// clarify still delivers, so the USER-FACING result succeeds.
				sidecar.clearScripts();
				const pollsBeforeRefusals = sidecar.callsOf("/send-poll").length;
				const sendsBeforeRefusals = sidecar.callsOf("/send").length;
				const tooFew = await engine.sendClarify(
					"+155****4567",
					"Pick one",
					["only"],
					"clar-2",
				);
				expect(tooFew.success).toBe(true); // text fallback answered
				expect(tooFew.error).toBeUndefined();
				const noTitle = await engine.sendClarify(
					"+155****4567",
					"",
					["A", "B"],
					"clar-3",
				);
				expect(noTitle.success).toBe(true);
				expect(noTitle.error).toBeUndefined();
				// ZERO /send-poll attempts were made for either refusal…
				expect(sidecar.callsOf("/send-poll").length).toBe(pollsBeforeRefusals);
				// …and BOTH fell back to numbered text on /send.
				expect(sidecar.callsOf("/send").length - sendsBeforeRefusals).toBe(2);
				expect(String(sidecar.callsOf("/send").at(-2)?.body["text"])).toContain(
					"1. only",
				);

				// Wire failure falls back to numbered text on /send.
				sidecar.script("*", {
					kind: "error",
					status: 500,
					error: "old sidecar without /send-poll",
				});
				const sendsBeforeFallback = sidecar.callsOf("/send").length;
				const fellBack = await engine.sendClarify(
					"+155****4567",
					"Pick one",
					["A", "B"],
					"clar-4",
				);
				expect(fellBack.success).toBe(true);
				const textBody = sidecar.callsOf("/send")[sendsBeforeFallback]?.body;
				expect(String(textBody?.["text"])).toContain("1. A");
				expect(String(textBody?.["text"])).toContain("2. B");

				// Open-ended clarify stays plain text (no poll endpoint).
				const pollsBeforeOpen = sidecar.callsOf("/send-poll").length;
				const openEnded = await engine.sendClarify(
					"+155****4567",
					"What do you mean?",
					undefined,
					"clar-5",
				);
				expect(openEnded.success).toBe(true);
				expect(sidecar.callsOf("/send-poll").length).toBe(pollsBeforeOpen);
				expect(sidecar.callsOf("/send").at(-1)?.body["text"]).toBe(
					"What do you mean?",
				);

				// Inbound votes: selection forwards TEXT; deselection/empty drop.
				await engine.dispatchInbound(
					photonPollOptionEvent({ title: "Yes — native tappable buttons" }),
				);
				expect(engine.dispatchedEvents).toHaveLength(1);
				const vote = lastDispatchOf(engine);
				expect(vote?.messageType).toBe("text");
				expect(vote?.text).toBe("Yes — native tappable buttons");
				expect(vote?.source?.chatId).toBe("+155****4567");

				await engine.dispatchInbound(
					photonPollOptionEvent({
						title: "No",
						selected: false,
						messageId: "vote-2",
					}),
				);
				await engine.dispatchInbound(
					photonPollOptionEvent({
						title: "   ",
						messageId: "vote-3",
					}),
				);
				expect(engine.dispatchedEvents).toHaveLength(1);
			},
		),
		mk(
			"photon.fatal-notify-self-cancel",
			"photon fatal notify self-cancel: a degraded /healthz stream promotes to FATAL UPSTREAM_STREAM_DEGRADED (retryable) and DETACHES the notification — dispatch returns BEFORE the handoff completes so teardown cancelling the detecting task can never cancel its own caller; notification failures warn instead of throwing; unreachable sidecars skip a beat without fatals",
			async () => {
				// Slow notification held until WE release it.
				let releaseNotify: (() => void) | undefined;
				const gate = new Promise<void>((resolve) => {
					releaseNotify = resolve;
				});
				let completed = false;
				const notifications: string[] = [];
				const sidecar = new FakeSidecarServer();
				const slowAdapter = new PhotonAdapter({
					sidecar,
					envReader: photonCredEnv(),
					notifyFatalError: async () => {
						notifications.push("started");
						await gate;
						completed = true;
					},
				});
				await slowAdapter.connect({ isReconnect: false });

				sidecar.healthzStream = {
					ok: false,
					state: "degraded",
					degradedForMs: 4000,
					lastIssue: "stream persistently failing",
				};
				const verdict = await slowAdapter.runHealthCheckOnce();
				expect(verdict).toBe("degraded-fatal");
				expect(slowAdapter.hasFatalError).toBe(true);
				expect(slowAdapter.fatalErrorCode).toBe("UPSTREAM_STREAM_DEGRADED");
				expect(slowAdapter.fatalErrorRetryable).toBe(true);
				// Dispatch DETACHED: returns before the notification finishes…
				expect(slowAdapter.fatalNotificationsDispatched).toHaveLength(1);
				expect(completed).toBe(false);
				// …and the macrotask hop delivers the handoff anyway.
				releaseNotify?.();
				await eventually(() => completed && notifications.length === 1);

				// Failing notification warns, never surfaces.
				const boomSidecar = new FakeSidecarServer();
				const boom = new PhotonAdapter({
					sidecar: boomSidecar,
					envReader: photonCredEnv(),
					notifyFatalError: async () => {
						throw new Error("gateway unreachable");
					},
				});
				await boom.connect({ isReconnect: false });
				boomSidecar.healthzStream = {
					ok: false,
					state: "degraded",
					degradedForMs: 4000,
					lastIssue: "stream persistently failing",
				};
				const boomVerdict = await boom.runHealthCheckOnce();
				expect(boomVerdict).toBe("degraded-fatal");
				await eventually(() => boom.fatalNotificationWarnings.length === 1);
				expect(boom.fatalNotificationWarnings[0]).toContain(
					"fatal-error notification failed",
				);
				expect(boom.fatalNotificationWarnings[0]).toContain(
					"gateway unreachable",
				);

				// Unreachable sidecar: skip a beat, NO fatal.
				const flaky = new FakeSidecarServer();
				const calm = new PhotonAdapter({
					sidecar: flaky,
					envReader: photonCredEnv(),
				});
				await calm.connect({ isReconnect: false });
				flaky.script("/healthz", {
					kind: "transport-error",
					message: "connection refused",
				});
				expect(await calm.runHealthCheckOnce()).toBe("unreachable");
				expect(calm.hasFatalError).toBe(false);
			},
		),
	];
}

// ── helpers ──────────────────────────────────────────────────────────────────

function lastDispatchOf(engine: PhotonAdapter): IncomingEvent | undefined {
	return engine.dispatchedEvents.at(-1);
}

/** Reaction-plane probe: /react + /unreact calls in order. */
function reactionCallsOf(sidecar: FakeSidecarServer): Array<{
	path: string;
	body: Record<string, unknown>;
}> {
	return sidecar.calls
		.filter((c) => c.path === "/react" || c.path === "/unreact")
		.map((c) => ({ path: c.path, body: c.body }));
}

function classificationTable(engine: PhotonAdapter): Record<string, boolean> {
	return {
		internalSidecarError: engine.isRetryableError("Internal sidecar error"),
		upstreamConnectError: engine.isRetryableError("upstream connect error"),
		upstreamUnavailable: engine.isRetryableError("upstream unavailable"),
		connectionDropped: engine.isRetryableError("connection dropped"),
		overflowReset: engine.isRetryableError("reset reason: overflow"),
		upstreamOverflowSnake: engine.isRetryableError("boom: upstream_overflow"),
		quotaExceededNotRetryable:
			engine.isRetryableError("quota exceeded") === false,
		explicitFalseVetoWins:
			engine.isRetryableError("connection dropped (retryable=false)") === false,
		authOrConfigVetoWins:
			engine.isRetryableError("auth_or_config: bad secret") === false,
		emptyErrorNotRetryable: engine.isRetryableError("") === false,
		nullErrorNotRetryable: engine.isRetryableError(null) === false,
	};
}

async function eventually(
	predicate: () => boolean,
	timeoutMs = 2_000,
	everyMs = 4,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (predicate()) return;
		if (Date.now() > deadline) {
			throw new Error("eventually: condition not met");
		}
		await new Promise<void>((r) => setTimeout(r, everyMs));
	}
}

// ── the suites ───────────────────────────────────────────────────────────────

describe("photon conformance (04 §8 merge gate)", () => {
	it("SHARED applicable rows pass for shape=ws (streaming family excluded BY THE PROBE)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported, excludedIds } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !excludedIds.includes(r.id));
		// The probe must genuinely exclude on this no-native-stream lane.
		expect(streamsSupported).toBe(false);

		const report = await runConformanceSuite({
			subjectName: "photon",
			shape: "ws",
			rows: shared,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		// Every non-excluded shared row actually RAN (no silent skip).
		expect(report.rows.length).toBe(all.length - excludedIds.length);
	}, 60_000);

	it("INHERITED ws transport rows pass against the REAL photon engine fixture (vendor mappings documented in-row)", async () => {
		const rows = makePhotonWsRows(makeRealPhotonFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "photon-transport",
			shape: "ws",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
	});

	it("photon SHAPE DELTA rows pass through the REAL engine", async () => {
		const rows = photonDeltaRows();
		const report = await runConformanceSuite({
			subjectName: "photon-deltas",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	});

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported, excludedIds } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !excludedIds.includes(r.id));

		const transport = makePhotonWsRows(makeRealPhotonFixture());
		const deltas = photonDeltaRows();

		const report = await runConformanceSuite({
			subjectName: "photon-full",
			shape: "ws",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 120_000);

	it("the gate DETECTS violations: a retry-hint-FABRICATING mutant fails its own named row alone", async () => {
		// Mutant: sidecarSend copies a numeric hint embedded in a FAKE error
		// body onto the failed SendResult (as if this wire carried a
		// Retry-After channel — IT DOES NOT). Every other input flows
		// UNTOUCHED, so ONLY the retry-after-capture row may fail.
		function mutate(world: PhotonWorld): PhotonWorld {
			const original = world.engine.sidecarSend.bind(world.engine);
			Object.defineProperty(world.engine, "sidecarSend", {
				value: async (
					spaceId: string,
					text: string,
					o?: { richlink?: boolean; markdown?: boolean },
				): Promise<SendResult> => {
					const result = await original(spaceId, text, o);
					if (!result.success) {
						const failed = [...world.sidecar.calls]
							.reverse()
							.find((c) => c.outcome === "error");
						const hint = failed?.errorBody?.["retry_after"];
						if (typeof hint === "number") {
							// THE LIE: surface the fabricated hint.
							return { ...result, retryAfter: hint };
						}
					}
					return result;
				},
			});
			return world;
		}

		const rows = makePhotonWsRows(
			makeRealPhotonFixture({ mutateWorld: mutate }),
		);
		const target = rows.find(
			(r) => r.id === "transport.ws.retry-after-capture",
		);
		expect(target).toBeDefined();
		const mutantReport = await runConformanceSuite({
			subjectName: "mutant-photon-retry-hint",
			shape: "ws",
			rows: [target as ConformanceRow],
		});
		expect(mutantReport.failed).toBe(1);
		expect(mutantReport.rows[0]?.pass).toBe(false);
		expect(mutantReport.rows[0]?.detail).toContain("NO Retry-After channel");

		// Sanity: the OTHER transport rows still pass on their own worlds.
		const others = rows.filter((r) => r.id !== target?.id);
		const otherReport = await runConformanceSuite({
			subjectName: "mutant-photon-others",
			shape: "ws",
			rows: others as ConformanceRow[],
		});
		if (otherReport.failed > 0) console.error(formatReport(otherReport));
		expect(otherReport.failed).toBe(0);
	}, 60_000);
});
