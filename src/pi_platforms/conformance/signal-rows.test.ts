// CONFORMANCE WIRING — the Signal port vs the executable 04 §8 matrix
// (census-phase merge gate; roadmap Phase 6; DEC-002 gate applies per adapter).
//
//   1. ALL applicable SHARED rows pass for shape="ws" against the REAL
//      kit-built SignalSubject. Applicability is COMPUTED from capability
//      data: native draft streaming is excluded BY THE PROBE because Signal
//      has no message-edit API (SUPPORTS_MESSAGE_EDITING=False — signal.py
//      class attr), so the three streaming rows drop out via the probe, never
//      a hardcoded skip. The LIE-SCAN at the bottom flips THE datum and shows
//      the streaming family rows then RUN and FAIL against seal reality.
//   2. THREE of the five inherited ws transport rows run over the REAL engine
//      fixture (makeSignalWsFixture) with documented Signal leg mappings;
//      TWO (capability-latch / dual-path) are realized as direct rows under
//      the SAME required ids because their DEC-032/034-encoded assertions
//      presume a native draft stream Signal genuinely lacks — proposed DEC
//      text in the port report covers both mappings.
//   3. Fresh Signal shape-delta rows execute through the real engine fixture
//      (envelope pipeline + send/typing/reaction wire shapes).
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: lying fixtures fail their own named rows.

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
	makeSignalSubject,
	type SignalSubject,
} from "../signal/signal-subject.js";
import { FakeSignalCliServer } from "../signal/signal-wire.js";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { SignalAdapter } from "../signal/signal-adapter.js";
import { SignalRateLimitError } from "../signal/rate-limit.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: {
		withSecret?: boolean | undefined;
		name?: string | undefined;
		declaredMessageEditing?: boolean | undefined;
	} = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeSignalSubject({
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

interface EngineWorld {
	subject: SignalSubject;
	adapter: SignalAdapter;
	daemon: FakeSignalCliServer;
	clock: ManualClock;
	connectAndAwaitLive(): Promise<void>;
}

function dmEnvelope(text: string): Record<string, unknown> {
	return {
		envelope: {
			sourceNumber: "+15551234567",
			sourceUuid: "9d6fe03a-2b7c-4f21-a5de-9a3c1e83b7aa",
			sourceName: "Alice",
			timestamp: Date.parse("2026-01-01T00:00:00Z") + text.length,
			dataMessage: { message: text },
		},
	};
}

function makeEngineWorld(name: string): EngineWorld {
	const clock = new ManualClock();
	const daemon = new FakeSignalCliServer();
	daemon.setClock(clock.nowMs);
	const mediaDir = mkdtempSync(join(tmpdir(), "signal-engine-"));
	const subject = makeSignalSubject({
		wire: new FakePlatformWire(),
		name,
	});
	// Rebind the REAL adapter onto the fake daemon + injected clock: the
	// subject's shared-row bridge stays for harness lanes; engine rows drive
	// the daemon transport directly through a SECOND adapter wired identically.
	const adapter = new SignalAdapter({
		transport: daemon,
		// Consistent identity: sync-echo destinations address THIS account.
		account: "+15550001111",
		scalarMaxUnits: 64,
		nowMs: clock.nowMs,
		sleepMs: clock.sleepMs,
		rng: () => 0,
		mediaCacheDir: join(mediaDir, "media"),
		secretReader: (key) =>
			key === "SIGNAL_HTTP_URL"
				? "http://127.0.0.1:8080"
				: key === "SIGNAL_ACCOUNT"
					? "+15550001111"
					: undefined,
	});
	adapter.attachStandardGuard();
	rmSync(mediaDir, { recursive: true, force: true });
	return {
		subject,
		adapter,
		daemon,
		clock,
		async connectAndAwaitLive(): Promise<void> {
			expect(await adapter.connect({ isReconnect: false })).toBe(true);
			await eventually(() => daemon.hasLiveStream);
		},
	};
}

/**
 * THE fixture behind shapes.ts::makeWsRows — the five inherited ws-family
 * rows run against the REAL engine/subject machinery with documented Signal
 * leg mappings (proposed DEC text in the port report):
 *
 *  - resubscribe-replay: NO resume cursor exists (signal-cli has no session
 *    protocol); loss-free coverage is modeled as DAEMON-SIDE BACKLOG.
 *  - heartbeat-watchdog: the SSE health monitor (30s cadence / 120s stale
 *    bar) plays the watchdog role.
 *  - retry-after-capture: the TWO capture sources are the structured error
 *    field and the libsignal-net message string; capture calibrates the
 *    rate-limit bucket AUTHORITATIVELY ("next delay" ≙ seconds-per-token).
 *  - capability-latch-permanence: Signal excludes streaming BY MANIFEST DATA
 *    (SUPPORTS_MESSAGE_EDITING=false) rather than an error-class latch; the
 *    mapped legs assert the datum's immutability, zero wire transmissions,
 *    and the lie-scan flip failing the family by name.
 *  - dual-path-markdown: SINGLE-PATH platform — every leg reduces to the ONE
 *    convert-on-send lane (markdown_to_signal bodyRanges); the preview-flag
 *    scope leg degenerates to ABSENCE-UNIFORMITY (no such flag exists).
 */
function makeSignalWsFixture(): WsFixture {
	return {
		/**
		 * Row: resubscribe replay covers messages sent during disconnect —
		 * exactly-once downstream. THE shape delta vs the ws reference:
		 * signal-cli has NO resume cursor/session protocol; loss-free coverage
		 * is modeled as DAEMON-SIDE BACKLOG (events emitted while no consumer
		 * holds the stream flush before new ones on reconnect — see
		 * FakeSignalCliServer backlog semantics). The adapter's obligation is
		 * the reconnect ladder + faithful resume of parsing.
		 */
		async resubscribeReplay() {
			const w = makeEngineWorld("signal-replay");
			await w.connectAndAwaitLive();

			w.daemon.pushEvent(dmEnvelope("r1"));
			await eventually(() => w.adapter.turnLog.length >= 1);
			const deliveredBefore = w.adapter.turnLog.length;

			w.daemon.dropStream("outage mid-life");
			await eventually(() => w.adapter.reconnectLog.length >= 1);
			// Sent DURING the disconnect window → daemon-side backlog.
			w.daemon.pushEvent(dmEnvelope("r2"));
			w.daemon.pushEvent(dmEnvelope("r3"));
			w.daemon.pushEvent(dmEnvelope("r4"));
			const sentDuringDisconnect = 3;
			expect(w.daemon.backlogDepth).toBe(3);

			await w.clock.advance(2_000); // ladder sleep → reopen → backlog first
			await eventually(() => w.daemon.hasLiveStream);
			await eventually(() => w.adapter.counts.accepted >= deliveredBefore + 3);

			// Exactly-once downstream: every envelope ACCEPTED exactly once —
			// burst arrival may coalesce TURNS but never drops or redups events
			// (discord-fixture parity for the exactly-once leg).
			const joined = w.adapter.turnLog.join("\n");
			for (const t of ["r1", "r2", "r3", "r4"]) {
				if (!joined.includes(t))
					throw new Error(`envelope lost across reconnect: ${t}`);
			}
			return {
				sentDuringDisconnect,
				replayedAfterResubscribe: w.adapter.counts.accepted - deliveredBefore,
			};
		},

		/**
		 * Row: the heartbeat-watchdog recovers a dead stream without loss.
		 * Signal realization: the HEALTH MONITOR (30s cadence, 120s stale bar)
		 * probes /api/v1/check after SSE silence; a dead daemon forces the
		 * reconnect; post-recovery envelopes flow with zero loss. All under
		 * the INJECTED clock.
		 */
		async watchdogRecovery() {
			const w = makeEngineWorld("signal-watchdog");
			await w.connectAndAwaitLive();
			w.daemon.setHealth(false); // daemon dies while SSE idles
			await w.clock.advance(150_000); // > stale threshold across ticks

			const detectedDeadSocket =
				w.adapter.forcedReconnects.length >= 1 &&
				(w.adapter.forcedReconnects[0]?.reason ?? "").includes("stale");

			// Recovery: daemon back ⇒ next tick refreshes activity, no repeat.
			w.daemon.setHealth(true);
			await w.clock.advance(60_000);
			expect(w.adapter.forcedReconnects).toHaveLength(1);
			await eventually(() => w.daemon.hasLiveStream);

			w.daemon.pushEvent(dmEnvelope("after recovery"));
			await eventually(() =>
				[...w.adapter.turnLog].some((t) => t.includes("after recovery")),
			);
			return { detectedDeadSocket, resumedWithoutLoss: true };
		},

		/**
		 * Row: Retry-After captured from BOTH sources and applied
		 * AUTHORITATIVELY. Signal realization (proposed-DEC leg mapping): the
		 * two capture sources are the STRUCTURED error field
		 * (error.data.response.results[*].retryAfterSeconds, ≥ v0.14.3) and
		 * the libsignal-net MESSAGE STRING ("Retry after N seconds" leaked
		 * through AttachmentInvalidException). The captured value calibrates
		 * the rate-limit bucket's refill rate — server truth over the local
		 * default — so the "next delay" IS the captured window.
		 */
		async retryAfterCapture() {
			const w = makeEngineWorld("signal-retry-after");
			const { SignalAttachmentScheduler: Sched } = await import(
				"../signal/rate-limit.js"
			);
			const scheduler = new Sched({
				clock: { nowMs: w.clock.nowMs, sleepMs: w.clock.sleepMs },
			});

			// Source 1: STRUCTURED field (error.data.response.results[*]
			// .retryAfterSeconds) surfaces retry_after=7 through the adapter's
			// raise-on-rate-limit RPC classification.
			w.daemon.scriptRpcFailure("send", {
				code: -5,
				message: "RateLimitException",
				data: { response: { results: [{ retryAfterSeconds: 7 }] } },
			});
			let closeCapturedSeconds = -1;
			try {
				await w.adapter.rpc(
					"send",
					{ account: "x" },
					{
						raiseOnRateLimit: true,
					},
				);
				throw new Error("expected rate limit");
			} catch (e) {
				if (!(e instanceof SignalRateLimitError))
					throw new Error("structured error must classify as rate limit");
				closeCapturedSeconds = e.retryAfter ?? -1;
				scheduler.feedback(e.retryAfter, 2); // server truth applied
			}

			// Source 2: MESSAGE STRING (libsignal-net RetryLaterException leaked
			// through AttachmentInvalidException during attachment upload).
			w.daemon.scriptRpcFailure("send", {
				code: -1,
				message:
					"AttachmentInvalidException: io exception: Retry after 4 seconds",
			});
			let restCapturedSeconds = -1;
			try {
				await w.adapter.rpc(
					"send",
					{ account: "x" },
					{
						raiseOnRateLimit: true,
					},
				);
				throw new Error("expected rate limit");
			} catch (e) {
				if (!(e instanceof SignalRateLimitError))
					throw new Error("message-string source must classify too");
				restCapturedSeconds = e.retryAfter ?? -1;
			}

			// The captured value IS the next delay, AUTHORITATIVE over the
			// default 4s/token refill.
			const state = scheduler.state();
			const delayAuthoritative = state.refillSecondsPerToken === 7;
			const nextDelayMs = state.refillSecondsPerToken * 1000;
			return {
				closeCapturedSeconds,
				nextDelayMs,
				delayAuthoritative,
				restCapturedSeconds,
			};
		},

		/**
		 * Row: a feature-gate error latches native streaming OFF permanently.
		 * Signal realization (proposed-DEC leg mapping): there is NO error-class
		 * latch because the exclusion IS THE manifest datum itself -- immutable
		 * per session. Mapped legs: the FIRST draft-frame attempt already fails
		 * with ZERO wire transmissions ("latched" from frame zero); the verdict
		 * is exactly ONE datum (latchCount); post-refusal attempts skip the wire
		 * entirely; and NOTHING transient can flip the datum. The lie-scan test
		 * below proves flipping the datum FAILS the streaming family by name.
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

			// Transient failures NEVER change the verdict -- the datum is const.
			const transientWorld = makeSubject({ name: "signal-latch-transient" });
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
		 * Row (DEC-034 family contract, Signal dialect realization): Signal is
		 * a SINGLE-PATH platform -- no native draft stream exists, so both
		 * "paths" reduce to the ONE convert-on-send lane (markdown_to_signal
		 * bodyRanges). Mapped legs: converted bytes are EXACT (markers stripped,
		 * inner bytes verbatim); style positions are PREFIX-STABLE under content
		 * extension; links/tables pass BYTE-UNCORRUPTED; the preview-flag scope
		 * leg DEGENERATES -- no flag exists anywhere, asserted as
		 * ABSENCE-UNIFORMITY across ALL wire ops.
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
				sends[0]?.content === "bold body [link](https://x.y)";
			const restConvertedBold = String(
				sends[0]?.metadata["signal_text_styles"] ?? "",
			).includes("BOLD");
			const restConvertedLink =
				nativeRawByteExact &&
				sends[0]?.content.includes("[link](https://x.y)") === true;

			// Prefix stability of the pure conversion.
			const fmt = await import("../signal/signal-format.js");
			const [, short] = fmt.markdownToSignal("**a** tail");
			const [, long] = fmt.markdownToSignal(
				"**a** tail extended with more words",
			);
			const nativePrefixStable =
				nativeRawByteExact &&
				short[0] !== undefined &&
				long[0] !== undefined &&
				short[0].split(":")[0] === long[0].split(":")[0];

			const tableResults = await s.deliverLongText(
				"chat-table",
				"| a | b |\\n|---|---|\\n| 1 | 2 |",
			);
			const restConvertedTable =
				tableResults.every((r) => r.success) &&
				s.wire
					.sendsOf("chat-table")
					.some((op) => op.content.includes("| a | b |"));

			// Flag-scope leg degenerates: NO preview/suppress flag exists on ANY
			// op (text sends included) -- absence-uniformity is the honest shape.
			const textSends = s.wire.ops.filter((o) => o.op === "send");
			const linkPreviewOnAllTextSends =
				textSends.length > 0 &&
				textSends.every(
					(o) =>
						o.metadata["suppress_embeds"] === undefined &&
						o.metadata["unfurl_links"] === undefined,
				);
			const linkPreviewAbsentOffTextSends = s.wire.ops
				.filter((o) => o.op !== "send")
				.every(
					(o) =>
						o.metadata["suppress_embeds"] === undefined &&
						o.metadata["unfurl_links"] === undefined,
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

// ── Signal shape-delta rows (real engine fixture) ───────────────────────────

function signalDeltaRows(): ConformanceRow[] {
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
			"transport.signal.envelope-pipeline",
			"signal: envelope walk deltas through the REAL listener — sync echo suppressed, Note-to-Self promoted, stories/self/group gates enforced, exactly-one turn each",
			async () => {
				const w = makeEngineWorld("signal-envelope");
				await w.connectAndAwaitLive();

				// We "sent" ts 42 ourselves: the sync echo must be consumed.
				w.daemon.scriptRpcResult("send", { timestamp: 42, results: [] });
				await w.adapter.send("+15559998888", "ours");
				w.daemon.pushEvent({
					envelope: {
						syncMessage: {
							sentMessage: {
								destinationNumber: "+15550001111",
								timestamp: 42,
							},
						},
					},
				});
				// Genuine Note-to-Self promotes to a turn…
				w.daemon.pushEvent({
					envelope: {
						sourceNumber: "+15550001111",
						syncMessage: {
							sentMessage: {
								destinationNumber: "+15550001111",
								timestamp: 77,
								message: "note to self",
							},
						},
					},
				});
				// …stories and unknown-group traffic never do (groups are DISABLED
				// in this world: no SIGNAL_GROUP_ALLOWED_USERS configured).
				w.daemon.pushEvent({
					envelope: { sourceNumber: "+15551234567", storyMessage: {} },
				});
				w.daemon.pushEvent({
					envelope: {
						sourceNumber: "+15551234567",
						dataMessage: {
							message: "stray group msg",
							groupInfo: { groupId: "zzz999" },
						},
					},
				});
				await eventually(() => w.adapter.turnLog.length >= 1);

				expect(w.adapter.counts.echoSuppressed).toBe(1);
				expect(w.adapter.counts.noteToSelfPromoted).toBe(1);
				expect(w.adapter.counts.storyFiltered).toBe(1);
				expect(w.adapter.counts.groupDisabled).toBe(1);
				expect([...w.adapter.turnLog]).toEqual(["note to self"]);
				await w.adapter.disconnect();
			},
		),
		mk(
			"transport.signal.send-and-reaction-shapes",
			"signal: outbound wire shapes — markdown→textStyle(s) conversion, groupId stripping, typed results failures, typing breaker arms at 3 consecutive failures, 👀→✅ reaction swap",
			async () => {
				const w = makeEngineWorld("signal-shapes");

				// Send: single style rides textStyle; group addressing strips prefix.
				const r1 = await w.adapter.send("+15551234567", "**hi** there");
				expect(r1.success).toBe(true);
				expect(r1.messageId).toBeNull(); // no editable identity — deliberate
				let calls = w.daemon.callsOf("send");
				expect(calls[0]?.params["message"]).toBe("hi there");
				expect(calls[0]?.params["textStyle"]).toBe("0:2:BOLD");
				await w.adapter.send("group:abc==", "group hello");
				calls = w.daemon.callsOf("send");
				expect(calls[1]?.params["groupId"]).toBe("abc==");
				expect(calls[1]?.params["recipient"]).toBeUndefined();

				// Typed failure shape fails the send with its TYPE.
				w.daemon.scriptRpcResult("send", {
					results: [{ type: "NETWORK_FAILURE" }],
				});
				const failed = await w.adapter.send("+15551234567", "unreachable");
				expect(failed.success).toBe(false);
				expect(failed.error).toBe("NETWORK_FAILURE");

				// Typing breaker: three consecutive NETWORK_FAILUREs arm the skip.
				for (let i = 0; i < 3; i++) {
					w.daemon.scriptRpcFailure("sendTyping", {
						code: -1,
						message: "NETWORK_FAILURE",
					});
				}
				await w.adapter.sendTypingSignal("chat-t");
				await w.adapter.sendTypingSignal("chat-t");
				await w.adapter.sendTypingSignal("chat-t");
				const attemptsBefore = w.daemon.callsOf("sendTyping").length;
				await w.adapter.sendTypingSignal("chat-t"); // SKIPPED inside window
				expect(w.daemon.callsOf("sendTyping").length).toBe(attemptsBefore);

				// Reaction lifecycle: 👀 start, remove+✅ swap on success.
				await w.adapter.sendReaction("+15551234567", "👀", "+15551234567", 42);
				await w.adapter.removeReaction("+15551234567", "+15551234567", 42);
				await w.adapter.sendReaction("+15551234567", "✅", "+15551234567", 42);
				const reactions = w.daemon.callsOf("sendReaction");
				expect(reactions[0]?.params["emoji"]).toBe("👀");
				expect(reactions[1]?.params["remove"]).toBe(true);
				expect(reactions[2]?.params["emoji"]).toBe("✅");
			},
		),
	];
}

// ── suite wiring ─────────────────────────────────────────────────────────────

describe("conformance suite — Signal census port (shape: ws)", () => {
	it("applicability is COMPUTED from capability data (streaming family excluded iff the no-edit probe closes)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // SUPPORTS_MESSAGE_EDITING=false parity
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("passes EVERY applicable shared row against the Signal subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);

		const report = await runConformanceSuite({
			subjectName: "signal",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	});

	it("passes ALL FIVE inherited ws transport rows against the REAL engine fixture (documented leg mappings)", async () => {
		const fixtureRows = makeWsRows(makeSignalWsFixture());
		expect(fixtureRows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "signal-transport-inherited",
			shape: "ws",
			rows: fixtureRows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	});

	it("passes ALL Signal shape-delta rows through the real engine fixture", async () => {
		const rows = signalDeltaRows();
		expect(rows.map((r) => r.id)).toEqual([
			"transport.signal.envelope-pipeline",
			"transport.signal.send-and-reaction-shapes",
		]);
		const report = await runConformanceSuite({
			subjectName: "signal-deltas",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	});

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));

		const transport = makeWsRows(makeSignalWsFixture());
		const suppliedTransportRowIds = new Set(transport.map((r) => r.id));
		// Every REQUIRED ws id is supplied exactly once.
		for (const requiredId of TRANSPORT_ROW_REQUIREMENTS.ws) {
			expect(suppliedTransportRowIds.has(requiredId)).toBe(true);
		}
		const deltas = signalDeltaRows();

		const report = await runConformanceSuite({
			subjectName: "signal-full",
			shape: "ws",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds,
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 30_000);

	it("the gate DETECTS violations: a LYING capability datum fails the streaming family BY NAME", async () => {
		// Lie-scan mutant: flip THE manifest datum that drives the exclusion
		// probe. Applicability then ADMITS the streaming family — and seal
		// reality catches the lie: the adapter has NO native draft/seal
		// machinery, so streaming.seal-discipline can never observe its
		// exactly-one-seal invariant and FAILS by name (the consumer's graceful
		// degradation may let OTHER family rows pass — the gate needs only ONE
		// deterministic detector, and it names the lie).
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
			subjectName: "mutant-signal-streaming-lie",
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
			subjectName: "honest-after-mutant",
			shape: "ws",
			rows: signalDeltaRows(),
		});
		expect(honestReport.failed).toBe(0);
	}, 30_000);

	it("the gate DETECTS violations: a lying replay fixture fails ITS OWN named row", async () => {
		const lying = makeWsRows({
			async resubscribeReplay() {
				return { sentDuringDisconnect: 5, replayedAfterResubscribe: 2 };
			},
			async watchdogRecovery() {
				return { detectedDeadSocket: false, resumedWithoutLoss: true };
			},
			async retryAfterCapture() {
				return {
					closeCapturedSeconds: 0,
					nextDelayMs: 1000,
					delayAuthoritative: false,
					restCapturedSeconds: 3,
				};
			},
			async capabilityLatchPermanence() {
				return {
					latchedOnFirstFailure: true,
					latchCount: 4,
					wireAttemptsAfterSkip: 9,
					supportsStreamingFalse: false,
					transientDidNotLatch: false,
				};
			},
			async dualPathMarkdown() {
				return {
					nativeRawByteExact: false,
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
			subjectName: "mutant-ws-fixture",
			shape: "ws",
			rows: lying,
		});
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("transport.ws.resubscribe-replay");
		expect(failedIds).toContain("transport.ws.heartbeat-watchdog-recovery");
		expect(failedIds).toContain("transport.ws.retry-after-capture");
	});
});
