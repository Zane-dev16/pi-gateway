// CONFORMANCE WIRING — the SimpleX port vs the executable 04 §8 matrix
// (census-phase merge gate; roadmap Phase 6 wave 2; DEC-002 gate applies).
//
//   1. ALL applicable SHARED rows pass for shape="ws" against the REAL
//      kit-built SimplexSubject. Applicability is COMPUTED from capability
//      data: native draft streaming is excluded BY THE PROBE because SimpleX
//      exposes NO message-edit API (SIMPLEX_SUPPORTS_MESSAGE_EDITING=false —
//      manifest datum feeding the probe like signal/manifest.ts), so the
//      three streaming rows drop out via the probe, never a hardcoded skip.
//      The LIE-SCAN at the bottom flips THE datum and shows the streaming
//      family rows then RUN and FAIL against seal reality.
//   2. ALL FIVE inherited ws transport rows run over the REAL engine fixture
//      (FakeSimplexDaemon + ManualClock) with documented leg mappings
//      (proposed DEC text in the port report):
//        - resubscribe-replay: simplex has NO cursor/replay protocol; loss-free
//          coverage is modeled as DAEMON-SIDE BACKLOG flushed before new events
//          after reconnect (Signal precedent mapping) — exactly-once downstream
//          accounting.
//        - heartbeat-watchdog: the mapped watchdog legs are LISTENER CLOSE
//          DETECTION + RECONNECT LADDER; the health monitor itself is
//          deliberately LOG-ONLY (never reconnects healthy quiet links).
//        - retry-after-capture: NO rate-limit guidance exists on this wire —
//          honest absence. Mapped legs: a CLOSE feeds the ladder whose CURRENT
//          computed step IS the authoritative next delay (captured seconds =
//          ladder seconds at close; nextDelayMs = slept ms under the injected
//          clock); the SECOND capture source is the FAILED REACHABILITY CHECK
//          feeding the same ladder (next rung doubles).
//        - capability-latch-permanence: the exclusion IS the immutable manifest
//          datum — first draft attempt fails with ZERO wire draft/seal ops;
//          later attempts skip the wire frozen at 1; transient failures never
//          flip; latchCount=1.
//        - dual-path-markdown: SINGLE-PATH platform — text bytes ship VERBATIM
//          inside the JSON-composed command payload (byte-exact through the
//          json round-trip incl. newlines/quotes); the preview-flag legs
//          degenerate to ABSENCE-UNIFORMITY across all recorded ops.
//   3. Fresh SimpleX shape-delta rows execute through the real engine fixture
//      (corr machinery, auto-accept, chat-item pipeline, group allowlist, text
//      batching, send-command shapes, media routing, file-transfer deferral,
//      reconnect ladder, health posture, image/video doors, channel directory,
//      env config resolution, probe open-timeout, ping/pong keepalive).
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: lying fixtures fail their OWN named rows, and the
//      REAL fixture facts contradict every lie (matrix bottom pattern).

import { describe, expect, it } from "vitest";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type { StreamLogger } from "../../pi_gateway/streaming/adapter-seam.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { makeWsRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { WsFixture } from "./shapes.js";
import { runConformanceSuite, formatReport } from "./runner.js";
import type { ConformanceSubject } from "./harness.js";
import { makeSimplexSubject } from "../simplex/simplex-subject.js";
import {
	FakeSimplexDaemon,
	SX_CONNECTING,
	SX_OPEN,
	type SimplexConnectionFactory,
	type SimplexReadyState,
} from "../simplex/simplex-wire.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import {
	SimplexAdapter,
	type SimplexAdapterOptions,
} from "../simplex/simplex-adapter.js";
import {
	SIMPLEX_CORR_PREFIX,
	SIMPLEX_LIST_CHANNELS_COMMAND_TIMEOUT_MS,
	SIMPLEX_PLUGIN_MANIFEST,
	SIMPLEX_SUPPORTS_MESSAGE_EDITING,
} from "../simplex/manifest.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: {
		withSecret?: boolean | undefined;
		name?: string | undefined;
		declaredMessageEditing?: boolean | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeSimplexSubject({
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

function computeApplicability(): {
	streamsSupported: boolean;
	excludedIds: string[];
} {
	const probe = makeSubject();
	const streamsSupported = probe.adapter.supportsDraftStreaming() === true;
	return { streamsSupported, excludedIds: [...STREAMING_ROW_IDS] };
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

interface EngineWorldOptions {
	autoAccept?: boolean | undefined;
	groupAllowFrom?: readonly string[] | undefined;
	maxPendingCorr?: number | undefined;
	rng?: (() => number) | undefined;
	/** _prepare_image seam injection (deterministic PNG/thumb fixture). */
	imagePreparer?: SimplexAdapterOptions["imagePreparer"] | undefined;
	/** cache_image_from_url seam injection (deterministic fetch fixture). */
	imageUrlFetcher?: SimplexAdapterOptions["imageUrlFetcher"] | undefined;
}

interface EngineWorld {
	adapter: SimplexAdapter;
	daemon: FakeSimplexDaemon;
	clock: ManualClock;
	logLines: string[];
	connectAndAwaitLive(): Promise<void>;
}

function capturingLogger(sink: string[]): StreamLogger {
	return {
		debug: (m) => sink.push(`debug:${m}`),
		info: (m) => sink.push(`info:${m}`),
		warn: (m) => sink.push(`warn:${m}`),
		error: (m) => sink.push(`error:${m}`),
	};
}

function makeEngineWorld(
	name: string,
	opts: EngineWorldOptions = {},
): EngineWorld {
	const clock = new ManualClock();
	const daemon = new FakeSimplexDaemon();
	daemon.setClock(clock.nowMs);
	const logLines: string[] = [];
	const adapter = new SimplexAdapter({
		wsFactory: daemon,
		logger: capturingLogger(logLines),
		scalarMaxUnits: 64,
		nowMs: clock.nowMs,
		sleepMs: clock.sleepMs,
		rng: opts.rng ?? (() => 0),
		autoAccept: opts.autoAccept,
		groupAllowFrom: opts.groupAllowFrom,
		maxPendingCorr: opts.maxPendingCorr,
		...(opts.imagePreparer !== undefined
			? { imagePreparer: opts.imagePreparer }
			: {}),
		...(opts.imageUrlFetcher !== undefined
			? { imageUrlFetcher: opts.imageUrlFetcher }
			: {}),
		textBatchDelayMs: 800,
		secretReader: (key) =>
			key === "SIMPLEX_WS_URL" ? "ws://127.0.0.1:5225" : undefined,
	});
	adapter.attachStandardGuard();
	return {
		adapter,
		daemon,
		clock,
		logLines,
		async connectAndAwaitLive(): Promise<void> {
			expect(await adapter.connect({ isReconnect: false })).toBe(true);
			await eventually(() => daemon.hasLiveConnection);
		},
	};
}

// ── envelope / chat-item builders (rcvMsgContent shapes) ────────────────────

interface DirectItemOpts {
	text?: string | undefined;
	contactId?: string | number | undefined;
	displayName?: string | undefined;
	msgType?: string | undefined;
	direction?: string | undefined;
	contentType?: string | undefined;
	filePath?: string | undefined;
	fileName?: string | undefined;
	fileId?: number | undefined;
	itemTs?: string | undefined;
}

/** One direct-chat chat item carrying rcvMsgContent (source shape parity). */
function directItem(opts: DirectItemOpts): Record<string, unknown> {
	const displayName = opts.displayName ?? "Alice";
	const msgContent: Record<string, unknown> = { type: opts.msgType ?? "text" };
	if (opts.text !== undefined) msgContent["text"] = opts.text;
	const itemData: Record<string, unknown> = {
		chatDir: { type: opts.direction ?? "directRcv" },
		content: {
			type: opts.contentType ?? "rcvMsgContent",
			msgContent,
		},
		meta: { itemTs: opts.itemTs ?? "2026-01-01T00:00:00Z" },
	};
	if (
		opts.filePath !== undefined ||
		opts.fileName !== undefined ||
		opts.fileId !== undefined
	) {
		itemData["file"] = {
			...(opts.filePath !== undefined
				? { fileSource: { filePath: opts.filePath } }
				: {}),
			...(opts.fileName !== undefined ? { fileName: opts.fileName } : {}),
			...(opts.fileId !== undefined ? { fileId: opts.fileId } : {}),
		};
	}
	return {
		chatInfo: {
			type: "direct",
			contact: {
				contactId: opts.contactId ?? 42,
				localDisplayName: displayName,
				profile: { displayName: `${displayName} P` },
			},
		},
		chatItem: itemData,
	};
}

interface GroupItemOpts {
	text?: string | undefined;
	groupId: string;
	memberId?: string | number | undefined;
	memberName?: string | undefined;
	direction?: string | undefined;
}

/** One group-chat chat item (member attribution via chatDir.groupMember). */
function groupItem(opts: GroupItemOpts): Record<string, unknown> {
	const memberId = opts.memberId ?? 7;
	const memberName = opts.memberName ?? "Bob";
	return {
		chatInfo: {
			type: "group",
			groupInfo: {
				groupId: opts.groupId,
				localDisplayName: "Team X",
				groupProfile: { displayName: "Team X profile" },
			},
		},
		chatItem: {
			chatDir: {
				type: opts.direction ?? "groupRcv",
				groupMember: {
					memberId,
					localDisplayName: memberName,
					memberProfile: { displayName: `${memberName} P` },
				},
			},
			content: {
				type: "rcvMsgContent",
				msgContent: {
					type: "text",
					...(opts.text !== undefined ? { text: opts.text } : {}),
				},
			},
			meta: { itemTs: "2026-01-01T00:00:00Z" },
		},
	};
}

/** newChatItems array envelope (daemon → adapter inbound shape). */
function itemsEnvelope(
	items: Record<string, unknown>[],
): Record<string, unknown> {
	return { resp: { type: "newChatItems", chatItems: items } };
}

/** Parse the composed JSON payload segment of a "/_send … json …" command. */
function composedPayloadOf(cmd: string): Array<Record<string, unknown>> {
	const marker = " json ";
	const idx = cmd.indexOf(marker);
	if (idx < 0) throw new Error(`not a structured /_send command: ${cmd}`);
	const parsed: unknown = JSON.parse(cmd.slice(idx + marker.length));
	if (!Array.isArray(parsed)) throw new Error(`payload not an array: ${cmd}`);
	return parsed as Array<Record<string, unknown>>;
}

/** Extract the minted counter from a hermes-<counter>-<ts> correlation id. */
function counterOf(corrId: string): number {
	const m = /^hermes-(\d+)-/.exec(corrId);
	if (m === null || m[1] === undefined)
		throw new Error(`bad corr id: ${corrId}`);
	return Number(m[1]);
}

// ── THE fixture behind shapes.ts::makeWsRows (documented leg mappings) ──────

function makeSimplexWsFixture(rng: () => number = () => 0): WsFixture {
	void rng;
	return {
		/**
		 * Row: resubscribe replay covers messages sent during disconnect —
		 * exactly-once downstream. THE shape delta vs the ws reference: the
		 * simplex wire has NO cursor/replay protocol (nothing to resume WITH);
		 * loss-free coverage is modeled as DAEMON-SIDE BACKLOG — events pushed
		 * while no consumer holds the connection queue server-side and flush
		 * BEFORE newer events on reconnect (Signal precedent mapping, see
		 * FakeSimplexDaemon backlog semantics). The adapter's obligation is the
		 * reconnect ladder + faithful resume of parsing.
		 */
		async resubscribeReplay() {
			const w = makeEngineWorld("simplex-replay");
			await w.connectAndAwaitLive();

			w.daemon.pushEvent(itemsEnvelope([directItem({ text: "replay-r1" })]));
			await eventually(() => w.adapter.counts.accepted >= 1);
			const deliveredBefore = w.adapter.counts.accepted;

			w.daemon.dropActive("outage mid-life");
			await eventually(() => w.adapter.reconnectLog.length >= 1);
			// Sent DURING the disconnect window → daemon-side backlog.
			w.daemon.pushEvent(itemsEnvelope([directItem({ text: "replay-r2" })]));
			w.daemon.pushEvent(itemsEnvelope([directItem({ text: "replay-r3" })]));
			w.daemon.pushEvent(itemsEnvelope([directItem({ text: "replay-r4" })]));
			const sentDuringDisconnect = 3;
			expect(w.daemon.backlogDepth).toBe(3);

			await w.clock.advance(2_000); // ladder sleep → reopen → backlog FIRST
			await eventually(() => w.daemon.hasLiveConnection);
			await eventually(() => w.adapter.counts.accepted >= deliveredBefore + 3);
			// Replay texts arrive BATCHED — flush the quiet-period window under
			// the injected clock, then assert downstream visibility.
			await w.clock.advance(800);
			await eventually(() =>
				["replay-r2", "replay-r3", "replay-r4"].every((token) =>
					w.adapter.turnLog.some((t) => t.includes(token)),
				),
			);

			// Exactly-once downstream: every envelope ACCEPTED exactly once and
			// visible downstream (burst coalescing may merge TURNS, never drop
			// or duplicate events — discord-fixture parity for this leg).
			const joined = w.adapter.turnLog.join("\n");
			for (const token of [
				"replay-r1",
				"replay-r2",
				"replay-r3",
				"replay-r4",
			]) {
				if (!joined.includes(token))
					throw new Error(`envelope lost across reconnect: ${token}`);
			}
			return {
				sentDuringDisconnect,
				replayedAfterResubscribe: w.adapter.counts.accepted - deliveredBefore,
			};
		},

		/**
		 * Row: the heartbeat-watchdog recovers a dead stream without loss.
		 * SimpleX realization (proposed-DEC leg mapping): the WATCHDOG legs are
		 * played by LISTENER CLOSE DETECTION + the reconnect ladder — kill the
		 * socket ⇒ detected + laddered + resumed without loss. The HEALTH
		 * MONITOR itself is deliberately LOG-ONLY (adapter.py:_health_monitor:
		 * protocol pings own liveness; application silence NEVER tears a
		 * healthy quiet link) — the posture leg below proves deep idle causes
		 * ZERO reconnects, then the close legs prove recovery works.
		 */
		async watchdogRecovery() {
			const w = makeEngineWorld("simplex-watchdog");
			await w.connectAndAwaitLive();

			// Posture leg: deep quiet idle NEVER reconnects a healthy link.
			const connectionsBeforeIdle = w.daemon.connectionLog.length;
			await w.clock.advance(600_000); // 2× past the 300s stale bar
			const stayedConnectedThroughIdle =
				w.adapter.idleLogs.length > 0 &&
				w.adapter.reconnectLog.length === 0 &&
				w.daemon.connectionLog.length === connectionsBeforeIdle;

			// Mapped watchdog legs: kill socket ⇒ detected + laddered.
			w.daemon.dropActive("daemon restarted");
			await eventually(() => w.adapter.reconnectLog.length >= 1);
			// Event sent DURING the outage window → backlog (loss-free claim).
			w.daemon.pushEvent(
				itemsEnvelope([directItem({ text: "wd-during-outage" })]),
			);
			await w.clock.advance(2_000); // ladder sleep → reopen → backlog flush
			await eventually(() => w.daemon.hasLiveConnection);
			// Post-recovery traffic flows.
			w.daemon.pushEvent(
				itemsEnvelope([directItem({ text: "wd-after-recovery" })]),
			);
			await eventually(() => w.adapter.counts.accepted >= 2);
			await w.clock.advance(800); // flush the coalesced text batch
			await eventually(() =>
				w.adapter.turnLog.some((t) => t.includes("wd-after-recovery")),
			);
			await eventually(() =>
				w.adapter.turnLog.some((t) => t.includes("wd-during-outage")),
			);
			// Backlog-first ordering held (during-outage BEFORE after-recovery).
			// NOTE: same-chat texts COALESCE into one "\n"-joined turn — order is
			// asserted on byte position within the joined log, not line indices.
			const joinedTurns = w.adapter.turnLog.join("\n");
			const duringPos = joinedTurns.indexOf("wd-during-outage");
			const afterPos = joinedTurns.indexOf("wd-after-recovery");
			const resumedWithoutLoss = duringPos >= 0 && afterPos > duringPos;

			return {
				detectedDeadSocket:
					stayedConnectedThroughIdle &&
					w.adapter.closeEvents.length >= 1 &&
					w.adapter.reconnectLog.length >= 1,
				resumedWithoutLoss,
			};
		},

		/**
		 * Row: Retry-After captured from BOTH sources and applied
		 * AUTHORITATIVELY. SimpleX realization (proposed-DEC leg mapping, HONEST
		 * ABSENCE): this wire carries NO rate-limit guidance whatsoever — there
		 * is no Retry-After header, close field, or error class to capture. The
		 * mapped capture sources are (1) a CONNECTION CLOSE feeding the ladder
		 * whose CURRENT computed step IS the authoritative next delay —
		 * closeCapturedSeconds = ladder seconds at close, nextDelayMs = the
		 * slept-ms under the injected clock, authoritative iff equal — and
		 * (2) a FAILED REACHABILITY CHECK feeding the SAME ladder (the next
		 * computed rung doubles). The ladder IS the retry-after authority.
		 */
		async retryAfterCapture() {
			const w = makeEngineWorld("simplex-retry-after");
			await w.connectAndAwaitLive();

			// Source 1 — CLOSE: captured seconds = ladder rung AT close (initial).
			w.daemon.dropActive("server close carries no retry guidance");
			await eventually(() => w.adapter.reconnectLog.length >= 1);
			const closeEntry = w.adapter.reconnectLog[0];
			const closeCapturedSeconds = 2; // WS_RETRY_DELAY_INITIAL_MS / 1000

			// Advance EXACTLY the captured window: reopen happens IFF the ladder
			// honors the captured value as THE next delay.
			await w.clock.advance(closeCapturedSeconds * 1000);
			await eventually(() => w.daemon.hasLiveConnection);
			const nextDelayMs = closeEntry?.delayMs ?? -1;
			const delayAuthoritative = nextDelayMs === closeCapturedSeconds * 1000;

			// Source 2 — FAILED REACHABILITY: refused handshake feeds the same
			// ladder; the NEXT computed rung doubles (4s).
			w.daemon.refuseConnections();
			w.daemon.dropActive("second outage, refuses now");
			await eventually(() => w.adapter.reconnectLog.length >= 2);
			await w.clock.advance(2_000); // close-triggered sleep → refused attempt
			await eventually(() => w.adapter.reconnectLog.length >= 3);
			const restEntry = w.adapter.reconnectLog[2];
			const restCapturedSeconds = Math.round((restEntry?.delayMs ?? 0) / 1000);
			w.daemon.acceptConnections();

			return {
				closeCapturedSeconds,
				nextDelayMs,
				delayAuthoritative,
				restCapturedSeconds,
			};
		},

		/**
		 * Row: a feature-gate error latches native streaming OFF permanently.
		 * SimpleX realization (proposed-DEC leg mapping): there is NO error-class
		 * latch because the exclusion IS THE manifest datum itself — immutable
		 * per session (SIMPLEX_SUPPORTS_MESSAGE_EDITING=false). Mapped legs: the
		 * FIRST draft-frame attempt already fails with ZERO wire transmissions
		 * ("latched" from frame zero); the verdict is exactly ONE datum
		 * (latchCount); post-refusal attempts skip the wire entirely; and
		 * NOTHING transient can flip the datum. The lie-scan test below proves
		 * flipping the datum FAILS the streaming family by name.
		 */
		async capabilityLatchPermanence() {
			const s = makeSubject();
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

			// Transient failures NEVER change the verdict — the datum is const.
			const transientWorld = makeSubject({ name: "simplex-latch-transient" });
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
		 * Row (DEC-034 family contract, SimpleX dialect realization): SimpleX is
		 * a SINGLE-PATH platform — no native draft stream exists, so both
		 * "paths" reduce to the ONE verbatim lane: text bytes ship UNCONVERTED
		 * inside the JSON-composed "/_send … json" command payload, byte-exact
		 * through the json round-trip including newlines and quotes. Mapped
		 * legs: converted-bold/link/table markers survive BYTE-EXACT (there is
		 * no conversion to check); prefix stability is IDENTITY (content
		 * extension shifts nothing); the preview-flag scope leg DEGENERATES to
		 * ABSENCE-UNIFORMITY — no preview/suppress flag exists anywhere on any
		 * recorded op.
		 */
		async dualPathMarkdown() {
			const w = makeEngineWorld("simplex-dual-path");
			await w.connectAndAwaitLive();

			// Leg 1: raw bytes verbatim through the composed-json round-trip
			// (quotes + newline escaping proven byte-exact).
			const sample = 'line1 "quoted"\nline2 **bold**';
			const r1 = await w.adapter.send("c-md", sample);
			const dmCommands = w.daemon.commandsStartingWith("/_send @c-md json ");
			const dmPayload = composedPayloadOf(dmCommands[0]?.cmd ?? "");
			const nativeRawByteExact =
				r1.success === true &&
				dmCommands.length === 1 &&
				dmPayload.length === 1 &&
				dmPayload[0]?.msgContent !== null &&
				typeof dmPayload[0]?.msgContent === "object" &&
				(dmPayload[0]?.msgContent as Record<string, unknown>)["type"] ===
					"text" &&
				(dmPayload[0]?.msgContent as Record<string, unknown>)["text"] ===
					sample;

			// Prefix-stability leg: content extension shifts NOTHING (identity —
			// there is no prefix computation to destabilize).
			const shortSample = "**a** tail";
			const longSample = "**a** tail extended with more words";
			await w.adapter.send("c-pfx", shortSample);
			await w.adapter.send("c-pfx", longSample);
			const pfxCommands = w.daemon.commandsStartingWith("/_send @c-pfx json ");
			const pfxShort = composedPayloadOf(pfxCommands[0]?.cmd ?? "");
			const pfxLong = composedPayloadOf(pfxCommands[1]?.cmd ?? "");
			const nativePrefixStable =
				(pfxShort[0]?.msgContent as Record<string, unknown>)["text"] ===
					shortSample &&
				(pfxLong[0]?.msgContent as Record<string, unknown>)["text"] ===
					longSample;

			// Converted-tier legs degenerate to the SAME verbatim lane: bold /
			// link / table markers survive byte-exact.
			await w.adapter.send("c-bold", "**bold** stays");
			const boldText = String(
				(
					composedPayloadOf(
						w.daemon.commandsStartingWith("/_send @c-bold json ")[0]?.cmd ?? "",
					)[0]?.msgContent as Record<string, unknown>
				)["text"],
			);
			const restConvertedBold = boldText === "**bold** stays";

			const linkSample = "see [link](https://x.y) here";
			await w.adapter.send("c-link", linkSample);
			const linkText = String(
				(
					composedPayloadOf(
						w.daemon.commandsStartingWith("/_send @c-link json ")[0]?.cmd ?? "",
					)[0]?.msgContent as Record<string, unknown>
				)["text"],
			);
			const restConvertedLink = linkText === linkSample;

			const tableSample = "| a | b |\n|---|---|\n| 1 | 2 |";
			await w.adapter.send("c-table", tableSample);
			const tableText = String(
				(
					composedPayloadOf(
						w.daemon.commandsStartingWith("/_send @c-table json ")[0]?.cmd ??
							"",
					)[0]?.msgContent as Record<string, unknown>
				)["text"],
			);
			const restConvertedTable = tableText === tableSample;

			// Flag-scope legs DEGENERATE to ABSENCE-UNIFORMITY: NO preview /
			// suppress flag exists on ANY recorded op — neither on text sends
			// ("on") nor anywhere else in the command stream ("off").
			const allCommands = w.daemon.commands.filter((c) =>
				c.cmd.startsWith("/_send "),
			);
			const flagAbsentEverywhere = allCommands.every(
				(c) =>
					!c.cmd.includes("suppress_embeds") && !c.cmd.includes("unfurl_links"),
			);
			const payloadsWellFormed = allCommands.every((c) => {
				const payload = composedPayloadOf(c.cmd);
				return payload.every((part) =>
					Object.keys(part).every(
						(k) =>
							k !== "suppress_embeds" &&
							k !== "unfurl_links" &&
							k !== "preview",
					),
				);
			});
			const linkPreviewOnAllTextSends =
				allCommands.length >= 5 && flagAbsentEverywhere;
			const linkPreviewAbsentOffTextSends =
				flagAbsentEverywhere && payloadsWellFormed;

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

// ── SimpleX shape-delta rows (real engine fixture) ──────────────────────────

function simplexDeltaRows(): ConformanceRow[] {
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

	return [
		mk(
			"transport.simplex.corr-id-machinery",
			"simplex: corrId machinery — correlated response resolves the future, timeout pops + null, prefixed-corrId echoes discarded WITHOUT chat-item processing, pending bound evicts oldest in a single sweep",
			async () => {
				const w = makeEngineWorld("simplex-corr", { maxPendingCorr: 3 });
				await w.connectAndAwaitLive();

				// Correlated response resolves the future (scripted reply).
				w.daemon.scriptCommandResponse("/probe", { type: "pong", n: 1 });
				const resolved = await w.adapter.sendCommand("/probe x");
				expect(resolved?.["type"]).toBe("pong");
				expect(w.adapter.pendingResponseCount()).toBe(0);

				// Timeout pops the entry and returns null (injected clock).
				const pendingTimeout = w.adapter.sendCommand("/lonely", 30_000);
				await w.clock.advance(30_000);
				expect(await pendingTimeout).toBeNull();
				expect(w.adapter.pendingResponseCount()).toBe(0);

				// Prefixed-corrId ECHO discarded WITHOUT chat-item processing —
				// even when the payload would otherwise be a valid chat-item batch.
				const acceptedBefore = w.adapter.counts.accepted;
				w.daemon.pushEvent({
					corrId: `${SIMPLEX_CORR_PREFIX}777-123`,
					resp: {
						type: "newChatItems",
						chatItems: [directItem({ text: "echo-must-not-turn" })],
					},
				});
				await eventually(() => w.adapter.counts.echoesDiscarded >= 1);
				expect(w.adapter.counts.accepted).toBe(acceptedBefore);
				expect(
					w.adapter.turnLog.some((t) => t.includes("echo-must-not-turn")),
				).toBe(false);

				// Pending bound evicts the OLDEST entries in a SINGLE sweep
				// (cap injected = 3; five fire-and-forget mints follow the two
				// command mints above ⇒ survivors are mints #5..#7).
				for (let i = 0; i < 5; i++) {
					await w.adapter.sendFireAndForget(`/ff ${i}`);
				}
				const snap = w.adapter.pendingCorrSnapshot();
				expect(snap.map(counterOf)).toEqual([5, 6, 7]);
			},
		),
		mk(
			"transport.simplex.auto-accept",
			"simplex: contactRequest + auto_accept ⇒ correlated '/accept {contactRequestId}'; disabled ⇒ nothing sent",
			async () => {
				const w = makeEngineWorld("simplex-autoaccept", { autoAccept: true });
				await w.connectAndAwaitLive();
				w.daemon.pushEvent({
					resp: {
						type: "contactRequest",
						contactRequest: { contactRequestId: 77 },
					},
				});
				await eventually(
					() => w.daemon.commandsStartingWith("/accept ").length >= 1,
				);
				expect(w.daemon.hasCommand("/accept 77")).toBe(true);
				expect(w.adapter.counts.contactRequestsAutoAccepted).toBe(1);
				// The awaited accept resolved via the fake's built-in responder.
				expect(w.adapter.pendingResponseCount()).toBe(0);

				const w2 = makeEngineWorld("simplex-noautoaccept", {
					autoAccept: false,
				});
				await w2.connectAndAwaitLive();
				w2.daemon.pushEvent({
					resp: {
						type: "contactRequest",
						contactRequest: { contactRequestId: 78 },
					},
				});
				await eventually(() => w2.adapter.counts.unhandledEventTypes >= 1);
				expect(w2.daemon.commandsStartingWith("/accept").length).toBe(0);
				expect(w2.adapter.counts.contactRequestsAutoAccepted).toBe(0);
			},
		),
		mk(
			"transport.simplex.chat-item-pipeline",
			"simplex: chat-item walk ORDER — own-direction drops, non-rcvMsgContent drops, direct id/name extraction, group:{id} + member naming, unhandled types dropped",
			async () => {
				const w = makeEngineWorld("simplex-pipeline", {
					groupAllowFrom: ["55"],
				});
				await w.connectAndAwaitLive();

				// Own messages (both direction flavors) never become turns.
				w.daemon.pushEvent(
					itemsEnvelope([
						directItem({ text: "mine-sent", direction: "directSnd" }),
						groupItem({
							text: "ours-sent",
							groupId: "55",
							direction: "groupSnd",
						}),
					]),
				);
				// Non-rcvMsgContent dropped.
				w.daemon.pushEvent(
					itemsEnvelope([
						directItem({ text: "snd-content", contentType: "sndMsgContent" }),
					]),
				);
				// Unhandled msgContent type + empty extracted text ⇒ dropped.
				w.daemon.pushEvent(
					itemsEnvelope([directItem({ text: "?", msgType: "unknown_type" })]),
				);
				expect(w.adapter.turnLog.length).toBe(0);

				// Direct chat: sender contactId + localDisplayName, chat_id = id.
				w.daemon.pushEvent(
					itemsEnvelope([directItem({ text: "hello direct", contactId: 42 })]),
				);
				// Group: chat_id group:{groupId}, member memberId + display name.
				w.daemon.pushEvent(
					itemsEnvelope([
						groupItem({ text: "hi team", groupId: "55", memberId: 7 }),
					]),
				);
				await eventually(() => w.adapter.counts.accepted >= 2);
				await w.clock.advance(800); // flush the two text batches
				await eventually(() =>
					w.adapter.turnLog.some((t) => t.includes("hello direct")),
				);
				await eventually(() =>
					w.adapter.turnLog.some((t) => t.includes("hi team")),
				);

				expect(w.adapter.counts.ownMessagesDropped).toBe(2);
				expect(w.adapter.counts.nonRcvContent).toBe(1);
				expect(w.adapter.counts.emptyContentDropped).toBe(1);
				expect(w.adapter.counts.accepted).toBe(2);
				const direct = w.adapter.acceptedLog.find((a) => a.chatType === "dm");
				expect(direct?.chatId).toBe("42");
				expect(direct?.userId).toBe("42");
				expect(direct?.chatName).toBe("Alice");
				const group = w.adapter.acceptedLog.find((a) => a.chatType === "group");
				expect(group?.chatId).toBe("group:55");
				expect(group?.userId).toBe("7");
				expect(group?.chatName).toBe("Team X");
			},
		),
		mk(
			"transport.simplex.group-allowlist",
			"simplex: group allowlist — DEFAULT-EMPTY disables ALL groups (logged, safer default), '*' opens any, named membership decides, non-members dropped",
			async () => {
				// Default-empty: groups disabled ENTIRELY, logged.
				const off = makeEngineWorld("simplex-groups-off");
				await off.connectAndAwaitLive();
				off.daemon.pushEvent(
					itemsEnvelope([groupItem({ text: "stray", groupId: "55" })]),
				);
				await eventually(() => off.adapter.counts.groupsDisabled >= 1);
				expect(off.adapter.counts.accepted).toBe(0);
				expect(
					off.logLines.some((l) => l.includes("SIMPLEX_GROUP_ALLOWED")),
				).toBe(true);

				// Named id passes; a different id is dropped as not-allowed.
				const named = makeEngineWorld("simplex-groups-named", {
					groupAllowFrom: ["55"],
				});
				await named.connectAndAwaitLive();
				named.daemon.pushEvent(
					itemsEnvelope([groupItem({ text: "in-list", groupId: "55" })]),
				);
				named.daemon.pushEvent(
					itemsEnvelope([groupItem({ text: "not-listed", groupId: "66" })]),
				);
				await eventually(() => named.adapter.counts.accepted >= 1);
				await named.clock.advance(800);
				await eventually(() => named.adapter.counts.accepted >= 1);
				expect(named.adapter.counts.groupsNotAllowed).toBe(1);
				expect(named.adapter.counts.groupsDisabled).toBe(0);
				expect(named.adapter.turnLog.some((t) => t.includes("in-list"))).toBe(
					true,
				);

				// '*' opens ALL groups.
				const star = makeEngineWorld("simplex-groups-star", {
					groupAllowFrom: ["*"],
				});
				await star.connectAndAwaitLive();
				star.daemon.pushEvent(
					itemsEnvelope([groupItem({ text: "anyone", groupId: "999" })]),
				);
				await eventually(() => star.adapter.counts.accepted >= 1);
				await star.clock.advance(800);
				await eventually(() => star.adapter.counts.accepted >= 1);
				expect(star.adapter.counts.groupsNotAllowed).toBe(0);
			},
		),
		mk(
			"transport.simplex.text-batching",
			"simplex: three rapid texts coalesce '\\n'-joined into ONE event after the quiet period (injected timer); late arrivals RESTART the window (prior flush cancelled); media bypasses batching",
			async () => {
				const w = makeEngineWorld("simplex-batching");
				await w.connectAndAwaitLive();

				w.daemon.pushEvent(itemsEnvelope([directItem({ text: "b1" })]));
				w.daemon.pushEvent(itemsEnvelope([directItem({ text: "b2" })]));
				w.daemon.pushEvent(itemsEnvelope([directItem({ text: "b3" })]));
				await eventually(() => w.adapter.counts.accepted >= 3);
				expect(w.adapter.turnLog.length).toBe(0); // quiet period pending
				await w.clock.advance(800);
				await eventually(() => w.adapter.turnLog.length >= 1);
				expect(w.adapter.turnLog[0]).toBe("b1\nb2\nb3");
				expect(w.adapter.counts.batchesFlushed).toBe(1);

				// Late arrival restarts the window: prior flush task CANCELLED.
				w.daemon.pushEvent(itemsEnvelope([directItem({ text: "late1" })]));
				await eventually(() => w.adapter.counts.accepted >= 4);
				await w.clock.advance(500); // < delay — still pending
				expect(w.adapter.turnLog.length).toBe(1);
				w.daemon.pushEvent(itemsEnvelope([directItem({ text: "late2" })]));
				await eventually(() => w.adapter.counts.accepted >= 5);
				await w.clock.advance(799); // 500+799=1299 since late1 — restarted!
				expect(w.adapter.turnLog.length).toBe(1);
				await w.clock.advance(1); // 800ms since late2 → flush
				await eventually(() => w.adapter.turnLog.length >= 2);
				expect(w.adapter.turnLog[1]).toBe("late1\nlate2");

				// Media bypasses batching entirely — dispatched IMMEDIATELY with
				// zero clock movement. (The pending late-batch stays parked.)
				w.daemon.pushEvent(
					itemsEnvelope([
						directItem({
							msgType: "image",
							text: "pic caption",
							filePath: "/tmp/inbound/pic.png",
						}),
					]),
				);
				await eventually(() =>
					w.adapter.turnLog.some((t) => t.includes("pic caption")),
				);
				const pic = w.adapter.acceptedLog.find((a) => a.text === "pic caption");
				expect(pic?.messageType).toBe("photo");
				expect(pic?.mediaUrls).toEqual(["/tmp/inbound/pic.png"]);
			},
		),
		mk(
			"transport.simplex.send-command-shapes",
			"simplex: outbound command shapes — DM '@{id} json' vs group '#{id} json' BYTE-EXACT (json escapes newlines/quotes); disconnected socket tolerated without throw",
			async () => {
				const w = makeEngineWorld("simplex-send-shapes");
				await w.connectAndAwaitLive();

				const dmText = 'say "hi"\nline2 **b**';
				const rDm = await w.adapter.send("c-42", dmText);
				expect(rDm.success).toBe(true);
				const dmExpected = `/_send @c-42 json ${JSON.stringify([
					{ msgContent: { type: "text", text: dmText } },
				])}`;
				const dmCmd = w.daemon.commandsStartingWith("/_send @c-42 ")[0];
				expect(dmCmd?.cmd).toBe(dmExpected);

				const rGroup = await w.adapter.send("group:g9", "team hello");
				expect(rGroup.success).toBe(true);
				const groupExpected = `/_send #g9 json ${JSON.stringify([
					{ msgContent: { type: "text", text: "team hello" } },
				])}`;
				const groupCmd = w.daemon.commandsStartingWith("/_send #g9 ")[0];
				expect(groupCmd?.cmd).toBe(groupExpected);

				// Disconnected socket: dropped-write tolerated, success off the WS
				// write attempt (listener owns reconnection).
				w.daemon.refuseConnections();
				w.daemon.dropActive("socket gone");
				await eventually(() => w.adapter.reconnectLog.length >= 1);
				const rVoid = await w.adapter.send("c-43", "into the void");
				expect(rVoid.success).toBe(true);
				expect(w.daemon.commandsStartingWith("/_send @c-43 ").length).toBe(0);
				expect(w.adapter.counts.droppedWrites).toBeGreaterThanOrEqual(1);
			},
		),
		mk(
			"transport.simplex.media-routing",
			"simplex: MEDIA:(\\S+) tags stripped; .ogg routes the voice msgContent shape; other ext routes the document shape; failed media short-circuits ITS SendResult",
			async () => {
				const w = makeEngineWorld("simplex-media");
				await w.connectAndAwaitLive();
				const dir = mkdtempSync(join(tmpdir(), "simplex-media-"));
				try {
					const voicePath = join(dir, "note.ogg");
					const docPath = join(dir, "spec.pdf");
					writeFileSync(voicePath, Buffer.alloc(8));
					writeFileSync(docPath, Buffer.alloc(8));

					// Voice leg: awaited structured command — script replies for BOTH
					// the stripped-text command and the voice command (FIFO).
					w.daemon.scriptCommandResponse("/_send", { type: "ok-text" });
					w.daemon.scriptCommandResponse("/_send", { type: "ok-voice" });
					const r1 = await w.adapter.send(
						"c-m",
						`listen up MEDIA:${voicePath}`,
					);
					expect(r1.success).toBe(true);
					// MEDIA tag stripped from the shipped text body…
					const textCmd = w.daemon.commandsStartingWith("/_send @c-m json ")[0];
					const textPayload = composedPayloadOf(textCmd?.cmd ?? "");
					expect(
						(textPayload[0]?.msgContent as Record<string, unknown>)["text"],
					).toBe("listen up");
					// …and .ogg routed to the INLINE VOICE shape.
					const cmds = w.daemon.commandsStartingWith("/_send @c-m json ");
					const voicePayloads = cmds
						.map((c) => composedPayloadOf(c.cmd)[0])
						.filter(
							(p) =>
								(p?.msgContent as Record<string, unknown>)["type"] === "voice",
						);
					expect(voicePayloads.length).toBe(1);
					expect(voicePayloads[0]?.["fileSource"]).toEqual({
						filePath: voicePath,
					});

					// Document leg: non-voice ext routes the file/document shape.
					w.daemon.scriptCommandResponse("/_send", { type: "ok-text2" });
					w.daemon.scriptCommandResponse("/_send", { type: "ok-doc" });
					const r2 = await w.adapter.send("c-m", `see MEDIA:${docPath}`);
					expect(r2.success).toBe(true);
					const docPayloads = w.daemon
						.commandsStartingWith("/_send @c-m json ")
						.map((c) => composedPayloadOf(c.cmd)[0])
						.filter(
							(p) =>
								(p?.msgContent as Record<string, unknown>)["type"] === "file",
						);
					expect(docPayloads.length).toBe(1);
					expect(docPayloads[0]?.["filePath"]).toBe(docPath);

					// Failed media short-circuits ITS SendResult (dead socket ⇒ the
					// correlated voice command returns null fast ⇒ failure surfaces).
					w.daemon.refuseConnections();
					w.daemon.dropActive("gone before media");
					await eventually(() => w.adapter.reconnectLog.length >= 1);
					const rFail = await w.adapter.send("c-m", `MEDIA:${voicePath}`);
					expect(rFail.success).toBe(false);
					expect(rFail.error).toBe("Failed to send voice message");
				} finally {
					rmSync(dir, { recursive: true, force: true });
				}
			},
		),
		mk(
			"transport.simplex.file-transfer-deferral",
			"simplex: audio item without filePath parks + fires fire-and-forget '/freceive {fileId}'; rcvFileComplete injects filePath and dispatches ONCE; non-audio without filePath does NOT park",
			async () => {
				const w = makeEngineWorld("simplex-deferral");
				await w.connectAndAwaitLive();

				// Voice note arriving before its download finished: park + freceive.
				w.daemon.pushEvent(
					itemsEnvelope([
						directItem({
							msgType: "voice",
							text: "",
							fileName: "vn.ogg",
							fileId: 501,
						}),
					]),
				);
				await eventually(
					() => w.daemon.commandsStartingWith("/freceive").length >= 1,
				);
				expect(w.daemon.hasCommand("/freceive 501")).toBe(true);
				expect(w.adapter.pendingTransferCount()).toBe(1);
				expect(w.adapter.counts.transfersDeferred).toBe(1);
				expect(w.adapter.counts.accepted).toBe(0); // parked, not dispatched

				// rcvFileComplete injects the downloaded path into the parked item
				// and runs the NORMAL pipeline on it — exactly once.
				w.daemon.pushEvent({
					resp: {
						type: "rcvFileComplete",
						chatItem: {
							chatInfo: { type: "direct", contact: { contactId: 42 } },
							chatItem: {
								file: {
									fileId: 501,
									fileSource: { filePath: "/tmp/done.ogg" },
								},
							},
						},
					},
				});
				await eventually(() => w.adapter.counts.transfersCompleted >= 1);
				expect(w.adapter.pendingTransferCount()).toBe(0);
				expect(w.adapter.counts.accepted).toBe(1);
				const delivered = w.adapter.acceptedLog[0];
				expect(delivered?.messageType).toBe("voice");
				expect(delivered?.mediaUrls).toEqual(["/tmp/done.ogg"]);
				// The freceive was FIRE-AND-FORGET: nothing awaits a reply.
				expect(w.adapter.pendingResponseCount()).toBe(0);

				// Non-audio without filePath does NOT park (per source: falls
				// through the normal pipeline with whatever text it carried).
				w.daemon.pushEvent(
					itemsEnvelope([
						directItem({
							msgType: "file",
							text: "pdf caption",
							fileName: "doc.pdf",
							fileId: 502,
						}),
					]),
				);
				await eventually(() => w.adapter.counts.accepted >= 2);
				await w.clock.advance(800);
				await eventually(() =>
					w.adapter.turnLog.some((t) => t.includes("pdf caption")),
				);
				expect(w.adapter.pendingTransferCount()).toBe(0);
				expect(w.adapter.counts.transfersDeferred).toBe(1); // unchanged
				expect(w.daemon.hasCommand("/freceive 502")).toBe(false);
			},
		),
		mk(
			"transport.simplex.reconnect-ladder",
			"simplex: ladder doubles 2s→…→60s cap with ≤20% bounded jitter, RESETS to initial on successful connect (computed sleeps ride the injected clock)",
			async () => {
				const w = makeEngineWorld("simplex-ladder", { rng: () => 0.25 });
				await w.connectAndAwaitLive();
				w.daemon.refuseConnections();
				w.daemon.dropActive("outage series");
				await eventually(() => w.adapter.reconnectLog.length >= 1);

				// Walk deep enough to cross the cap: 1.05×(2+4+8+16+32+60+60)s.
				await w.clock.advance(400_000);
				const delays = w.adapter.reconnectLog.map((e) => e.delayMs);
				expect(delays.length).toBeGreaterThanOrEqual(7);
				for (const [i, d] of delays.entries()) {
					const base = Math.min(2_000 * 2 ** i, 60_000);
					// Jitter BOUNDED, not exact: ≤20% of the current backoff.
					expect(d).toBeGreaterThanOrEqual(base);
					expect(d).toBeLessThanOrEqual(base * 1.2 + 1e-6);
				}
				expect(delays.some((d) => d >= 60_000)).toBe(true); // cap reached
				expect(w.adapter.reconnectLog[0]?.trigger).toBe("connection-closed");
				expect(
					w.adapter.reconnectLog
						.slice(1)
						.every((e) => e.trigger === "connect-failed"),
				).toBe(true);

				// Success RESETS the ladder to the initial rung.
				w.daemon.acceptConnections();
				await w.clock.advance(120_000);
				await eventually(() => w.daemon.hasLiveConnection);
				expect(w.adapter.reconnectResets.length).toBeGreaterThanOrEqual(1);

				// Post-reset outage: next delay back at ~initial (bounded jitter).
				const idxAtReset = w.adapter.reconnectLog.length;
				w.daemon.refuseConnections();
				w.daemon.dropActive("post-reset outage");
				await eventually(() => w.adapter.reconnectLog.length >= idxAtReset + 1);
				await w.clock.advance(70_000);
				const postReset = w.adapter.reconnectLog[idxAtReset];
				expect(postReset?.trigger).toBe("connection-closed");
				expect(postReset?.delayMs).toBeGreaterThanOrEqual(2_000);
				expect(postReset?.delayMs).toBeLessThanOrEqual(2_400);
			},
		),
		mk(
			"transport.simplex.health-posture",
			"simplex: health monitor deliberately LOG-ONLY — deep quiet idle NEVER reconnects a healthy link; no HTTP ingress ⇒ NO trust boundary declared (manifest posture)",
			async () => {
				const w = makeEngineWorld("simplex-posture");
				await w.connectAndAwaitLive();
				const connectionsBefore = w.daemon.connectionLog.length;
				await w.clock.advance(900_000); // 30 ticks; 3× past the 300s bar
				expect(w.adapter.idleLogs.length).toBeGreaterThan(0);
				expect(w.adapter.reconnectLog.length).toBe(0);
				expect(w.daemon.connectionLog.length).toBe(connectionsBefore);
				expect(w.daemon.liveConnectionCount).toBe(1);

				// Manifest posture: ws shape, outbound-only surface ⇒ no trust
				// boundary (DEC-017 applies to HTTP-ingress planes only; same
				// precedent as signal/manifest.ts).
				expect(SIMPLEX_PLUGIN_MANIFEST.transportShape).toBe("ws");
				expect(SIMPLEX_PLUGIN_MANIFEST.trustBoundary).toBeUndefined();
			},
		),
		mk(
			"transport.simplex.image-video-doors",
			"simplex: native media doors — send_image ships the correlated '/_send @id|#gid json [{filePath,msgContent:{type:\"image\",image:<thumb-data-uri>,text}}]' shape byte-exact over the PNG-prepared path (file:// + http(s) sources); missing-file/download-failure/dead-link failures never half-send; send_video routes the document shape; sendMultipleImages fans out per entry",
			async () => {
				const dir = mkdtempSync(join(tmpdir(), "simplex-img-"));
				try {
					const webpPath = join(dir, "pic.webp");
					const pngPath = join(dir, "native.png");
					const cachedPath = join(dir, "cached.jpg");
					const vidPath = join(dir, "clip.mp4");
					writeFileSync(webpPath, Buffer.alloc(4));
					writeFileSync(pngPath, Buffer.from([0x89, 0x50, 0x4e, 0x47]));
					writeFileSync(cachedPath, Buffer.alloc(2));
					writeFileSync(vidPath, Buffer.alloc(4));

					const fetchedUrls: string[] = [];
					// _prepare_image parity fixture: png/jpg/jpeg stay UNCONVERTED;
					// anything else becomes the sibling .png + fixed thumb data URI.
					const w = makeEngineWorld("simplex-image", {
						imagePreparer: async (filePath) => ({
							pngPath: /\.(png|jpe?g)$/.test(filePath)
								? filePath
								: filePath.replace(/\.[^.]+$/, ".png"),
							thumbDataUri: "data:image/jpg;base64,QUJDREVG",
						}),
						imageUrlFetcher: async (url) => {
							fetchedUrls.push(url);
							if (url.includes("broken")) {
								throw new Error("DNS resolution failed");
							}
							return cachedPath;
						},
					});
					await w.connectAndAwaitLive();

					// Local image via file:// → PNG-prepared path + inline thumb
					// ride the EXACT composed Hermes shape; correlated reply.
					w.daemon.scriptCommandResponse("/_send", { type: "ok-image" });
					const r1 = await w.adapter.sendImageFile(
						"c-i",
						webpPath,
						"a caption",
					);
					expect(r1.success).toBe(true);
					const dmExpected = `/_send @c-i json ${JSON.stringify([
						{
							filePath: join(dir, "pic.png"),
							msgContent: {
								type: "image",
								image: "data:image/jpg;base64,QUJDREVG",
								text: "a caption",
							},
						},
					])}`;
					expect(
						w.daemon.commandsStartingWith("/_send @c-i json ")[0]?.cmd,
					).toBe(dmExpected);

					// Group addressing uses '#<gid>'; absent caption ⇒ empty text.
					w.daemon.scriptCommandResponse("/_send", { type: "ok-image2" });
					const r2 = await w.adapter.sendImage("group:g7", `file://${pngPath}`);
					expect(r2.success).toBe(true);
					const groupExpected = `/_send #g7 json ${JSON.stringify([
						{
							filePath: pngPath,
							msgContent: {
								type: "image",
								image: "data:image/jpg;base64,QUJDREVG",
								text: "",
							},
						},
					])}`;
					expect(
						w.daemon.commandsStartingWith("/_send #g7 json ")[0]?.cmd,
					).toBe(groupExpected);

					// http(s) source flows through THE cache seam then the SAME
					// door with the caption intact.
					w.daemon.scriptCommandResponse("/_send", { type: "ok-image3" });
					const r3 = await w.adapter.sendImage(
						"c-i",
						"https://cdn.example/x/img.jpeg",
						"from web",
					);
					expect(r3.success).toBe(true);
					expect(fetchedUrls).toEqual(["https://cdn.example/x/img.jpeg"]);
					const webExpected = `/_send @c-i json ${JSON.stringify([
						{
							filePath: cachedPath,
							msgContent: {
								type: "image",
								image: "data:image/jpg;base64,QUJDREVG",
								text: "from web",
							},
						},
					])}`;
					expect(
						w.daemon.commandsStartingWith("/_send @c-i json ")[1]?.cmd,
					).toBe(webExpected);

					// Missing local file → 'Image file not found', NO command.
					const rMiss = await w.adapter.sendImageFile(
						"c-i",
						join(dir, "nope.png"),
					);
					expect(rMiss).toEqual({
						success: false,
						error: "Image file not found",
					});

					// Download failure surfaces THE fetch error verbatim
					// (Hermes str(e)); still zero commands.
					const rDl = await w.adapter.sendImage(
						"c-i",
						"https://cdn.example/broken.webp",
					);
					expect(rDl.success).toBe(false);
					expect(rDl.error).toBe("DNS resolution failed");
					expect(w.daemon.commandsStartingWith("/_send @c-i ").length).toBe(2);

					// Video routes the DOCUMENT shape (adapter.py:send_video →
					// send_document); missing video inherits 'File not found'.
					w.daemon.scriptCommandResponse("/_send", { type: "ok-video" });
					const rv = await w.adapter.sendVideo("c-v", vidPath, "clip cap");
					expect(rv.success).toBe(true);
					expect(
						w.daemon.commandsStartingWith("/_send @c-v json ")[0]?.cmd,
					).toBe(
						`/_send @c-v json ${JSON.stringify([
							{
								filePath: vidPath,
								msgContent: { type: "file", text: "clip cap" },
							},
						])}`,
					);
					const rvMiss = await w.adapter.sendVideo("c-v", join(dir, "no.mp4"));
					expect(rvMiss.error).toBe("File not found");

					// Batch fan-out: every entry its own image command, in order.
					for (let i = 0; i < 3; i++) {
						w.daemon.scriptCommandResponse("/_send", {
							type: `ok-b${i}`,
						});
					}
					const rb = await w.adapter.sendMultipleImages("c-b", [
						webpPath,
						pngPath,
						webpPath,
					]);
					expect(rb.every((r) => r.success)).toBe(true);
					const batchCmds = w.daemon.commandsStartingWith("/_send @c-b json ");
					expect(batchCmds.length).toBe(3);
					expect(
						composedPayloadOf(batchCmds[0]?.cmd ?? "")[0]?.["filePath"],
					).toBe(join(dir, "pic.png"));
					expect(
						composedPayloadOf(batchCmds[1]?.cmd ?? "")[0]?.["filePath"],
					).toBe(pngPath);

					// Dead link: the correlated image command resolves null fast
					// ⇒ failure verdict, never a silent half-send.
					w.daemon.refuseConnections();
					w.daemon.dropActive("gone before image");
					await eventually(() => w.adapter.reconnectLog.length >= 1);
					const rFail = await w.adapter.sendImage("c-i", `file://${pngPath}`);
					expect(rFail.success).toBe(false);
					expect(rFail.error).toBe("Failed to send image");
				} finally {
					rmSync(dir, { recursive: true, force: true });
				}
			},
		),
		mk(
			"transport.simplex.channel-directory",
			"simplex: listChannels issues '/contacts'+'/groups' (10s bounds) and parses display-name/id/pair forms into directory entries; disconnected ⇒ null WITHOUT sending; unresponsive daemon ⇒ null (directory keeps its cache); failed /groups tolerated with contacts kept",
			async () => {
				const w = makeEngineWorld("simplex-channels");
				await w.connectAndAwaitLive();

				w.daemon.scriptCommandResponse("/contacts", {
					type: "contacts",
					contacts: [
						{
							contactId: 42,
							localDisplayName: "Alice",
							profile: { displayName: "Alice P" },
						},
						{ profile: { displayName: "NoId" } },
						{ contactId: 43 },
						"junk-non-dict",
						{},
					],
				});
				w.daemon.scriptCommandResponse("/groups", {
					type: "groups",
					groups: [
						{
							groupId: "g1",
							localDisplayName: "Team X",
							groupProfile: { displayName: "TX" },
						},
						[
							{ groupId: "g2", groupProfile: { displayName: "Pair Form" } },
							{ summary: true },
						],
						{ groupId: "g3" },
						{ noGroupId: 1 },
						42,
					],
				});
				expect(await w.adapter.listChannels()).toEqual([
					{ id: "Alice", name: "Alice", type: "dm" },
					{ id: "NoId", name: "NoId", type: "dm" },
					{ id: "43", name: "43", type: "dm" },
					{ id: "group:g1", name: "Team X", type: "group" },
					{ id: "group:g2", name: "Pair Form", type: "group" },
					{ id: "group:g3", name: "g3", type: "group" },
				]);
				expect(w.daemon.hasCommand("/contacts")).toBe(true);
				expect(w.daemon.hasCommand("/groups")).toBe(true);
				expect(SIMPLEX_LIST_CHANNELS_COMMAND_TIMEOUT_MS).toBe(10_000);

				// Disconnected: NULL without any command leaving.
				const cold = makeEngineWorld("simplex-channels-cold");
				expect(await cold.adapter.listChannels()).toBeNull();
				expect(cold.daemon.commands.length).toBe(0);

				// Unresponsive daemon (/contacts never answered): the 10s
				// correlated bound expires ⇒ null.
				const mute = makeEngineWorld("simplex-channels-mute");
				await mute.connectAndAwaitLive();
				const pendingMute = mute.adapter.listChannels();
				await mute.clock.advance(10_000);
				expect(await pendingMute).toBeNull();

				// Failed /groups tolerated — contacts kept for the directory.
				const half = makeEngineWorld("simplex-channels-half");
				await half.connectAndAwaitLive();
				half.daemon.scriptCommandResponse("/contacts", {
					contacts: [{ contactId: 9, localDisplayName: "Dee" }],
				});
				const pendingHalf = half.adapter.listChannels();
				await half.clock.advance(20_000); // /groups silence expires
				expect(await pendingHalf).toEqual([
					{ id: "Dee", name: "Dee", type: "dm" },
				]);
			},
		),
		mk(
			"transport.simplex.env-config-resolution",
			"simplex: __init__ env resolution via the scoped reader — SIMPLEX_AUTO_ACCEPT parsed ('0'/'false'/'no'/empty ⇒ false, case-insensitive, env BEATS injected opt), SIMPLEX_GROUP_ALLOWED comma-split with empty-env fallback, HERMES_SIMPLEX_TEXT_BATCH_DELAY seconds wired through the batch-delay helper into the ACTUAL flush window",
			async () => {
				const readerWith =
					(vals: Record<string, string | undefined>) =>
					(key: string): string | undefined =>
						key === "SIMPLEX_WS_URL" ? "ws://127.0.0.1:5225" : vals[key];
				const envWorld = (
					vals: Record<string, string | undefined>,
					opts: Partial<SimplexAdapterOptions> = {},
				): {
					adapter: SimplexAdapter;
					daemon: FakeSimplexDaemon;
					clock: ManualClock;
				} => {
					const clock = new ManualClock();
					const daemon = new FakeSimplexDaemon();
					daemon.setClock(clock.nowMs);
					const adapter = new SimplexAdapter({
						wsFactory: daemon,
						scalarMaxUnits: 64,
						nowMs: clock.nowMs,
						sleepMs: clock.sleepMs,
						rng: () => 0,
						textBatchDelayMs: 800,
						secretReader: readerWith(vals),
						...opts,
					});
					adapter.attachStandardGuard();
					return { adapter, daemon, clock };
				};

				// SIMPLEX_AUTO_ACCEPT matrix.
				const autoOf = (raw: string | undefined, optAuto?: boolean): boolean =>
					envWorld(
						{ SIMPLEX_AUTO_ACCEPT: raw },
						optAuto === undefined ? {} : { autoAccept: optAuto },
					).adapter.autoAccept;
				expect(autoOf(undefined)).toBe(true); // unset ⇒ default true
				expect(autoOf(undefined, false)).toBe(false); // unset ⇒ option honored
				expect(autoOf("", true)).toBe(false); // set-but-EMPTY disables even over true
				expect(autoOf("0")).toBe(false);
				expect(autoOf("False")).toBe(false); // case-insensitive
				expect(autoOf("no")).toBe(false);
				expect(autoOf("true")).toBe(true);
				expect(autoOf("1", false)).toBe(true); // env BEATS injected option
				expect(autoOf("YES", false)).toBe(true);

				// SIMPLEX_GROUP_ALLOWED comma-split + trim, empties dropped;
				// unset AND set-but-empty fall back to the injected option.
				const groupsOf = (
					raw: string | undefined,
					opt?: readonly string[],
				): readonly string[] => [
					...envWorld(
						{ SIMPLEX_GROUP_ALLOWED: raw },
						opt === undefined ? {} : { groupAllowFrom: opt },
					).adapter.groupAllowSet,
				];
				expect(groupsOf("a, b,,c")).toEqual(["a", "b", "c"]);
				expect(groupsOf("*")).toEqual(["*"]);
				expect(groupsOf(undefined, ["z"])).toEqual(["z"]);
				expect(groupsOf("", ["z"])).toEqual(["z"]);
				expect(groupsOf(undefined)).toEqual([]);

				// HERMES_SIMPLEX_TEXT_BATCH_DELAY: env seconds WIN over the
				// injected option; invalid values fall back to the default.
				const delayed = envWorld({
					HERMES_SIMPLEX_TEXT_BATCH_DELAY: "1.5",
				});
				expect(delayed.adapter.textBatchDelayMs).toBe(1500);
				expect(
					envWorld({ HERMES_SIMPLEX_TEXT_BATCH_DELAY: "abc" }).adapter
						.textBatchDelayMs,
				).toBe(800);
				expect(envWorld({}).adapter.textBatchDelayMs).toBe(800);

				// The env-derived delay WIRES the real flush window: past the old
				// 800ms mark the batch stays parked; it flushes at 1500ms.
				await delayed.adapter.connect({ isReconnect: false });
				await eventually(() => delayed.daemon.hasLiveConnection);
				delayed.daemon.pushEvent(
					itemsEnvelope([directItem({ text: "env-d1" })]),
				);
				delayed.daemon.pushEvent(
					itemsEnvelope([directItem({ text: "env-d2" })]),
				);
				await eventually(() => delayed.adapter.counts.accepted >= 2);
				expect(delayed.adapter.turnLog.length).toBe(0);
				await delayed.clock.advance(800);
				expect(delayed.adapter.turnLog.length).toBe(0); // NOT yet
				await delayed.clock.advance(700); // 1500ms total ⇒ flush
				await eventually(() => delayed.adapter.turnLog.length >= 1);
				expect(delayed.adapter.turnLog[0]).toBe("env-d1\nenv-d2");
			},
		),
		mk(
			"transport.simplex.probe-open-timeout",
			"simplex: reachability probe raced against open_timeout=10 — a daemon accepting TCP but stalling the handshake resolves FALSE at the 10s bound (injected clock) instead of hanging connect(), abandoning the probe socket; promptly-opening daemons still probe TRUE",
			async () => {
				const clock = new ManualClock();
				let probeSockets = 0;
				let lastClose: { code: number; reason: string } | null = null;
				const stallingFactory: SimplexConnectionFactory = {
					connect(_listener) {
						probeSockets += 1;
						return {
							readyState: SX_CONNECTING,
							send: () => undefined as void,
							close: (code = 1000, reason = "") => {
								lastClose = { code, reason };
							},
						};
					},
				};
				const adapter = new SimplexAdapter({
					wsFactory: stallingFactory,
					scalarMaxUnits: 64,
					nowMs: clock.nowMs,
					sleepMs: clock.sleepMs,
					rng: () => 0,
					secretReader: (k) =>
						k === "SIMPLEX_WS_URL" ? "ws://stall.example" : undefined,
				});
				adapter.attachStandardGuard();

				const pending = adapter.connect({ isReconnect: false });
				await clock.advance(9_999);
				let settledEarly = false;
				void pending.then(() => {
					settledEarly = true;
				});
				await eventually(() => true); // yield macrotasks
				expect(settledEarly).toBe(false); // unresolved BEFORE the bound…
				expect(adapter.isRunning).toBe(false); // …and nothing started

				await clock.advance(1); // cross open_timeout=10 ⇒ FALSE
				expect(await pending).toBe(false);
				expect(adapter.isRunning).toBe(false);
				expect(probeSockets).toBe(1);
				expect(lastClose).toEqual({
					code: 1000,
					reason: "reachability probe timed out",
				});

				// Contrast leg: promptly-opening daemon probes TRUE and starts.
				const ok = makeEngineWorld("simplex-probe-ok");
				await expect(ok.adapter.connect({ isReconnect: false })).resolves.toBe(
					true,
				);
				await eventually(() => ok.daemon.hasLiveConnection);
			},
		),
		mk(
			"transport.simplex.ping-pong-keepalive",
			"simplex: seam keepalive (_ws_listener ping_interval=20/ping_timeout=20 parity) — protocol pings ride the live socket every 20s; answered pongs NEVER reconnect a quiet link (log-only posture intact); a stalled ping expires at +20s ⇒ link closed 1011 'ping timeout' into the SAME ladder, then recovery answers again; factories without ping capability are skipped safely",
			async () => {
				const w = makeEngineWorld("simplex-keepalive");
				await w.connectAndAwaitLive();

				// Quiet link, pongs flowing: 120s of application silence = one
				// ping per 20s answered 1:1, ZERO reconnects.
				await w.clock.advance(120_000);
				const quietPings = w.daemon.pingFrames.length;
				expect(quietPings).toBe(6);
				expect(w.adapter.counts.pingsSent).toBe(quietPings);
				expect(w.adapter.counts.pongsReceived).toBe(quietPings);
				expect(w.adapter.reconnectLog.length).toBe(0);

				// Stall: pings sent at t=140k get NO pong; at t=160k the 20s
				// timeout expires the link — closed 1011 'ping timeout', ladder.
				w.daemon.stallPongs();
				const timeoutsBefore = w.adapter.counts.pingTimeouts;
				await w.clock.advance(40_000);
				expect(w.adapter.counts.pingTimeouts).toBe(timeoutsBefore + 1);
				await eventually(() => w.adapter.reconnectLog.length >= 1);
				expect(
					w.adapter.closeEvents.some((c) => c.reason.startsWith("1011:")),
				).toBe(true);

				// Recovery: answers resume on the reopened link — no FURTHER
				// timeouts, connection stays up, pongs keep counting.
				w.daemon.resumePongs();
				await w.clock.advance(60_000);
				expect(w.adapter.counts.pingTimeouts).toBe(timeoutsBefore + 1);
				expect(w.daemon.hasLiveConnection).toBe(true);
				expect(w.adapter.counts.pongsReceived).toBeGreaterThan(quietPings);

				// Legacy factory WITHOUT ping capability: keepalive skips
				// silently — no crash, no counters, link untouched.
				const legacyClock = new ManualClock();
				const legacyFactory: SimplexConnectionFactory = {
					connect(listener) {
						const sock = {
							readyState: SX_OPEN as SimplexReadyState,
							send: () => undefined as void,
							close: () => {
								// A real socket terminates its pump on close — required
								// for disconnect()'s cooperative task drain.
								listener.onClose({ code: 1000, reason: "closed" });
							},
							// NO ping member — pre-seam factories stay compatible.
						};
						queueMicrotask(() => listener.onOpen());
						return sock;
					},
				};
				const legacy = new SimplexAdapter({
					wsFactory: legacyFactory,
					scalarMaxUnits: 64,
					nowMs: legacyClock.nowMs,
					sleepMs: legacyClock.sleepMs,
					rng: () => 0,
					secretReader: (k) =>
						k === "SIMPLEX_WS_URL" ? "ws://legacy.example" : undefined,
				});
				legacy.attachStandardGuard();
				expect(await legacy.connect({ isReconnect: false })).toBe(true);
				await legacyClock.advance(60_000); // three keepalive windows
				expect(legacy.counts.pingsSent).toBe(0);
				expect(legacy.counts.pingTimeouts).toBe(0);
				expect(legacy.isRunning).toBe(true);
				await legacy.disconnect();
			},
		),
	];
}

// ── suite wiring ─────────────────────────────────────────────────────────────

describe("conformance suite — SimpleX census port (shape: ws)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff the no-edit probe closes)", () => {
		expect(SIMPLEX_SUPPORTS_MESSAGE_EDITING).toBe(false);
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false);
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("passes EVERY applicable shared row against the SimpleX subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: "simplex",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it("passes ALL FIVE inherited ws transport rows against the REAL engine fixture (documented leg mappings)", async () => {
		const fixtureRows = makeWsRows(makeSimplexWsFixture());
		expect(fixtureRows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "simplex-transport-inherited",
			shape: "ws",
			rows: fixtureRows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	});

	it("passes ALL SimpleX shape-delta rows through the real engine fixture", async () => {
		const rows = simplexDeltaRows();
		expect(rows.map((r) => r.id)).toEqual([
			"transport.simplex.corr-id-machinery",
			"transport.simplex.auto-accept",
			"transport.simplex.chat-item-pipeline",
			"transport.simplex.group-allowlist",
			"transport.simplex.text-batching",
			"transport.simplex.send-command-shapes",
			"transport.simplex.media-routing",
			"transport.simplex.file-transfer-deferral",
			"transport.simplex.reconnect-ladder",
			"transport.simplex.health-posture",
			"transport.simplex.image-video-doors",
			"transport.simplex.channel-directory",
			"transport.simplex.env-config-resolution",
			"transport.simplex.probe-open-timeout",
			"transport.simplex.ping-pong-keepalive",
		]);
		const report = await runConformanceSuite({
			subjectName: "simplex-deltas",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 30_000);

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));

		const transport = makeWsRows(makeSimplexWsFixture());
		const suppliedTransportRowIds = new Set(transport.map((r) => r.id));
		// Every REQUIRED ws id is supplied exactly once.
		for (const requiredId of TRANSPORT_ROW_REQUIREMENTS.ws) {
			expect(suppliedTransportRowIds.has(requiredId)).toBe(true);
		}
		const deltas = simplexDeltaRows();

		const report = await runConformanceSuite({
			subjectName: "simplex-full",
			shape: "ws",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds,
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 45_000);

	it("the gate DETECTS violations: LYING fixtures fail their OWN named rows", async () => {
		// Lie 1 — replay LOSS: events sent during disconnect silently vanish.
		const lyingTransport = makeWsRows({
			async resubscribeReplay() {
				return { sentDuringDisconnect: 5, replayedAfterResubscribe: 2 }; // LOST
			},
			async watchdogRecovery() {
				return { detectedDeadSocket: true, resumedWithoutLoss: true };
			},
			async retryAfterCapture() {
				return {
					closeCapturedSeconds: 7,
					nextDelayMs: 7000,
					delayAuthoritative: true,
					restCapturedSeconds: 4,
				};
			},
			async capabilityLatchPermanence() {
				return {
					latchedOnFirstFailure: true,
					latchCount: 1,
					wireAttemptsAfterSkip: 1,
					supportsStreamingFalse: true,
					transientDidNotLatch: true,
				};
			},
			async dualPathMarkdown() {
				return {
					nativeRawByteExact: true,
					nativePrefixStable: true,
					restConvertedBold: true,
					restConvertedLink: true,
					restConvertedTable: true,
					linkPreviewOnAllTextSends: true,
					linkPreviewAbsentOffTextSends: true,
				};
			},
		});

		// Lies 2–5 — delta-row detectors run the REAL machinery then assert the
		// NEGATION of reality (matrix-rows bottom pattern); reality contradicts.
		const mkLying = (
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
		const lyingDeltas: ConformanceRow[] = [
			mkLying(
				"transport.simplex.group-allowlist",
				"lying: groups admit by default",
				async () => {
					const w = makeEngineWorld("lying-groups");
					await w.connectAndAwaitLive();
					w.daemon.pushEvent(
						itemsEnvelope([groupItem({ text: "stray", groupId: "any" })]),
					);
					await eventually(() => w.adapter.counts.groupsDisabled >= 1);
					// THE LIE — contradicted: default-empty DISABLES groups.
					expect(w.adapter.counts.groupsDisabled).toBe(0);
				},
			),
			mkLying(
				"transport.simplex.text-batching",
				"lying: rapid texts stay separate turns",
				async () => {
					const w = makeEngineWorld("lying-batching");
					await w.connectAndAwaitLive();
					for (const t of ["l1", "l2", "l3"]) {
						w.daemon.pushEvent(itemsEnvelope([directItem({ text: t })]));
					}
					await eventually(() => w.adapter.counts.accepted >= 3);
					await w.clock.advance(800);
					await eventually(() => w.adapter.turnLog.length >= 1);
					// THE LIE — contradicted: THREE coalesced into ONE "\n"-joined turn.
					expect(w.adapter.turnLog.length).toBe(3);
				},
			),
			mkLying(
				"transport.simplex.reconnect-ladder",
				"lying: ladder never doubles",
				async () => {
					const w = makeEngineWorld("lying-ladder");
					await w.connectAndAwaitLive();
					w.daemon.refuseConnections();
					w.daemon.dropActive("lie probe");
					await eventually(() => w.adapter.reconnectLog.length >= 1);
					await w.clock.advance(30_000);
					await eventually(() => w.adapter.reconnectLog.length >= 2);
					// THE LIE — contradicted: rung 2 doubled beyond rung 1.
					const d1 = w.adapter.reconnectLog[0]?.delayMs ?? 0;
					const d2 = w.adapter.reconnectLog[1]?.delayMs ?? 0;
					expect(d2).toBeLessThanOrEqual(d1 * 1.2);
				},
			),
			mkLying(
				"transport.simplex.send-command-shapes",
				"lying: groups address via '@'",
				async () => {
					const w = makeEngineWorld("lying-shapes");
					await w.connectAndAwaitLive();
					await w.adapter.send("group:g9", "hello");
					// THE LIE — contradicted: groups use the '#<id>' structured form.
					const cmd = w.daemon.commandsStartingWith("/_send @g9 ")[0];
					expect(cmd?.cmd.startsWith("/_send @g9 json ")).toBe(true);
				},
			),
		];

		const report = await runConformanceSuite({
			subjectName: "mutant-simplex-fixtures",
			shape: "ws",
			rows: [...lyingTransport, ...lyingDeltas],
			suppliedTransportRowIds: new Set(lyingTransport.map((r) => r.id)),
		});
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		for (const required of [
			...TRANSPORT_ROW_REQUIREMENTS.ws.slice(0, 1),
			"transport.simplex.group-allowlist",
			"transport.simplex.text-batching",
			"transport.simplex.reconnect-ladder",
			"transport.simplex.send-command-shapes",
		]) {
			expect(failedIds).toContain(required);
		}
		// The honest-looking stub dims must NOT fail — only the lies do.
		expect(failedIds).not.toContain("transport.ws.heartbeat-watchdog-recovery");
	}, 30_000);

	it("REAL fixture facts contradict EVERY lying claim (detector binding)", async () => {
		// replay preserves every envelope across a disconnect window.
		const rW = makeEngineWorld("fact-replay");
		await rW.connectAndAwaitLive();
		rW.daemon.pushEvent(itemsEnvelope([directItem({ text: "f1" })]));
		await eventually(() => rW.adapter.counts.accepted >= 1);
		rW.daemon.dropActive("fact outage");
		await eventually(() => rW.adapter.reconnectLog.length >= 1);
		rW.daemon.pushEvent(itemsEnvelope([directItem({ text: "f2" })]));
		rW.daemon.pushEvent(itemsEnvelope([directItem({ text: "f3" })]));
		expect(rW.daemon.backlogDepth).toBe(2); // the daemon HELD them
		await rW.clock.advance(2_000);
		await eventually(() => rW.adapter.counts.accepted >= 3);
		const replayPreservesEveryEnvelope = rW.adapter.counts.accepted === 3;

		// Default-empty allowlist DISABLES groups.
		const gW = makeEngineWorld("fact-groups");
		await gW.connectAndAwaitLive();
		gW.daemon.pushEvent(
			itemsEnvelope([groupItem({ text: "g", groupId: "zz" })]),
		);
		await eventually(() => gW.adapter.counts.groupsDisabled >= 1);
		const groupsDisabledByDefault =
			gW.adapter.counts.groupsDisabled >= 1 && gW.adapter.counts.accepted === 0;

		// Rapid texts coalesce into ONE turn.
		const bW = makeEngineWorld("fact-batching");
		await bW.connectAndAwaitLive();
		for (const t of ["c1", "c2", "c3"]) {
			bW.daemon.pushEvent(itemsEnvelope([directItem({ text: t })]));
		}
		await eventually(() => bW.adapter.counts.accepted >= 3);
		await bW.clock.advance(800);
		await eventually(() => bW.adapter.turnLog.length >= 1);
		const batchCoalescedIntoOneTurn =
			bW.adapter.turnLog.length === 1 && bW.adapter.turnLog[0] === "c1\nc2\nc3";

		// Ladder rung 2 doubles beyond rung 1.
		const lW = makeEngineWorld("fact-ladder");
		await lW.connectAndAwaitLive();
		lW.daemon.refuseConnections();
		lW.daemon.dropActive("fact outage");
		await eventually(() => lW.adapter.reconnectLog.length >= 1);
		await lW.clock.advance(10_000);
		await eventually(() => lW.adapter.reconnectLog.length >= 2);
		const ladderSecondRungDoubles =
			(lW.adapter.reconnectLog[1]?.delayMs ?? 0) >
			(lW.adapter.reconnectLog[0]?.delayMs ?? 0) * 1.5;

		// Group sends use the '#<id>' structured form.
		const sW = makeEngineWorld("fact-shapes");
		await sW.connectAndAwaitLive();
		await sW.adapter.send("group:gxx", "x");
		const groupSendUsesHashPrefix =
			sW.daemon.commandsStartingWith("/_send #gxx json ").length === 1;

		expect(replayPreservesEveryEnvelope).toBe(true);
		expect(groupsDisabledByDefault).toBe(true);
		expect(batchCoalescedIntoOneTurn).toBe(true);
		expect(ladderSecondRungDoubles).toBe(true);
		expect(groupSendUsesHashPrefix).toBe(true);
	}, 30_000);

	it("LIE-SCAN: flipping SIMPLEX_SUPPORTS_MESSAGE_EDITING makes the streaming family RUN and FAIL", async () => {
		// Lie-scan mutant: flip THE manifest datum that drives the exclusion
		// probe. Applicability then ADMITS the streaming family — and seal
		// reality catches the lie: the adapter has NO native draft/seal
		// machinery, so streaming.seal-discipline can never observe its
		// exactly-one-seal invariant and FAILS by name.
		const lyingApplicability = (): boolean =>
			makeSubject({
				declaredMessageEditing: true,
			}).adapter.supportsDraftStreaming() === true;
		expect(lyingApplicability()).toBe(true); // the lie FLIPS the probe…

		const all = buildSharedRows({
			makeSubject: (o) => makeSubject({ ...o, declaredMessageEditing: true }),
		});
		const streamingRows = all.filter((r) => STREAMING_ROW_IDS.includes(r.id));
		expect(streamingRows.length).toBe(3);
		const report = await runConformanceSuite({
			subjectName: "mutant-simplex-streaming-lie",
			shape: "ws",
			rows: streamingRows,
		});
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("streaming.seal-discipline");

		// …and the HONEST probe stays closed for every fresh subject.
		expect(computeApplicability().streamsSupported).toBe(false);

		// …and the HONEST subject still passes its delta set (negative
		// validation must not poison the honest catalog).
		const honestReport = await runConformanceSuite({
			subjectName: "honest-simplex-after-mutant",
			shape: "ws",
			rows: simplexDeltaRows(),
		});
		expect(honestReport.failed).toBe(0);
	}, 45_000);
});
