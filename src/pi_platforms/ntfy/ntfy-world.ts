// pi_platforms/ntfy/ntfy-world — the REAL-engine fixture substrate for the
// ntfy census port: world construction, the inherited ws transport-row fixture
// (documented leg mappings), and PROPOSED-DEC notes (DEC-026).
//
// ── PROPOSED DEC (ntfy-replay-window) ───────────────────────────────────────
// transport.ws.resubscribe-replay maps to ntfy's vendor-true surfaces: the
// in-flight window (events pulled off the stream while the session was down
// are HELD and redispatched losslessly on reconnect). The 300s dedup window
// makes any SERVER-side redelivery of a seen id exactly-once WITHIN one
// connection generation; disconnect() CLEARS the map (adapter.py:disconnect
// :327), so a NEW generation re-dispatches redeliveries of ids seen under the
// previous one — Hermes truth, never faked green. Gap traffic outside the
// in-flight window is lost by protocol (no since= cursor exists in the
// reference client) — documented, never faked green.
//
// ── PROPOSED DEC (ntfy-retry-after-leg) ────────────────────────────────────
// The reconnect ladder is the FIXED manifest array [2,5,10,30,60]s; the only
// authoritative-capture path is the shared §6.1 ladder honoring scripted
// retry_after through the injected clock (send-retry.ts), mirroring the IRC
// realization.

import { AutoAdvanceClock } from "./clock.js";
import type { PacingClockLike } from "./clock.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import type { WsFixture } from "../conformance/shapes.js";
import { FakePlatformWire } from "../conformance/wire.js";
import type { ConformanceRow } from "../conformance/rows.js";

import { FakeNtfyServer } from "./fake-ntfy-server.js";
import { FatalStreamError } from "./ntfy-adapter.js";
import type { SendResult } from "../../pi_gateway/streaming/adapter-seam.js";
import { makeNtfySubject, type NtfySubject } from "./ntfy-subject.js";
import { NTFY_ECHO_TAG, NTFY_MAX_MESSAGE_CHARS } from "./manifest.js";

export const TOPIC = "hermes-in";

/**
 * Drives `step()` on the parked clock until `p` settles — the poll must run
 * CONCURRENTLY with the advancing, or a parked ladder sleep can never fire.
 */
async function driveUntilSettled(
	p: Promise<unknown>,
	step: () => Promise<void>,
): Promise<void> {
	let settled = false;
	void p.then(
		() => {
			settled = true;
		},
		() => {
			settled = true;
		},
	);
	while (!settled) {
		await step();
		await new Promise<void>((r) => setImmediate(r));
	}
	await p;
}

async function eventually(
	predicate: () => boolean,
	timeoutMs = 4_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (predicate()) return;
		if (Date.now() > deadline)
			throw new Error("eventually: condition not met (ntfy world)");
		await new Promise<void>((r) => setTimeout(r, 2));
	}
}

export interface NtfyWorld {
	subject: NtfySubject;
	engine: NtfySubject["adapter"];
	server: FakeNtfyServer;
	wire: FakePlatformWire;
	clock: PacingClockLike;
	connectAndAwaitLive(): Promise<void>;
	/** Drive the live stream's queued events into the adapter. */
	pumpStreamEvents(n?: number): Promise<void>;
	adapterRunUntilReconnect(): Promise<boolean>;
	deliveredTexts(texts: readonly string[]): number;
}

export function makeNtfyWorld(
	opts: {
		name?: string | undefined;
		clock?: PacingClockLike | undefined;
		withSecret?: boolean | undefined;
		scheduler?:
			| import("../../pi_gateway/guards/testing/manual-spawner.js").ManualScheduler
			| undefined;
		/** Vendor-cap subjects pass NTFY_MAX_MESSAGE_CHARS here. */
		scalarMaxUnits?: number | undefined;
	} = {},
): NtfyWorld {
	// Parked-time control: sleeps register on the manual clock and fire when
	// scenarios advance() — deterministic without wall time.
	const clock = opts.clock ?? new ManualClock();
	const server = new FakeNtfyServer();
	const wire = new FakePlatformWire();
	const subject = makeNtfySubject({
		wire,
		server,
		clock,
		name: opts.name,
		...(opts.withSecret !== undefined ? { withSecret: opts.withSecret } : {}),
		...(opts.scheduler !== undefined ? { scheduler: opts.scheduler } : {}),
		...(opts.scalarMaxUnits !== undefined
			? { scalarMaxUnits: opts.scalarMaxUnits }
			: {}),
	});

	return {
		subject,
		engine: subject.adapter,
		server,
		wire,
		clock,
		async connectAndAwaitLive(): Promise<void> {
			const ok = await subject.adapter.connect({ isReconnect: false });
			if (!ok || !subject.adapter.isConnected) {
				throw new Error(
					`connectAndAwaitLive: not live (${subject.lifecycleSnapshot().detail})`,
				);
			}
			// Let the consume loop attach.
			await new Promise<void>((r) => setImmediate(r));
		},
		async pumpStreamEvents(n = 8): Promise<void> {
			for (let i = 0; i < n; i++) {
				await new Promise<void>((r) => setImmediate(r));
			}
		},
		async adapterRunUntilReconnect(): Promise<boolean> {
			// One ladder cycle, driving its PARKED ladder sleep via the clock.
			let result = false;
			const cycle = subject.adapter.runReconnectCycle().then((r) => {
				result = r;
			});
			await driveUntilSettled(cycle, () => clock.advance(1000));
			await new Promise<void>((r) => setImmediate(r));
			return result;
		},
		deliveredTexts(texts) {
			const turns = [...subject.turns()];
			return texts.filter((t) => turns.includes(t)).length;
		},
	};
}

/**
 * THE fixture behind shapes.ts::makeWsRows — the FIVE inherited ws scenarios
 * against the live ntfy engine with documented leg mappings above.
 */
export function makeRealNtfyFixture(): WsFixture {
	return {
		/**
		 * Resubscribe replay: held-inbound window + dedup exactly-once on
		 * redelivery (PROPOSED DEC ntfy-replay-window).
		 */
		async resubscribeReplay() {
			const w = makeNtfyWorld({ name: "ntfy-replay" });
			await w.connectAndAwaitLive();
			const stream = w.engine.activeStreamForTests();
			if (stream === null) throw new Error("fixture setup: no live stream");

			// Arm the in-flight hold window: pulled events park instead of
			// dispatching (the reconnect-window discipline, deterministically).
			w.engine.setInboundHoldGate(true);
			stream.pushMessage("o1");
			stream.pushMessage("o2");
			stream.pushMessage("o3");
			await w.pumpStreamEvents(15);
			const sentDuringDisconnect = w.engine.heldInboundCount;
			check(
				sentDuringDisconnect === 3,
				`3 events held (${sentDuringDisconnect})`,
			);

			w.engine.disconnect(); // session DOWN mid-life (dedup map cleared)
			let settled = false;
			const reconnecting = w.engine
				.connect({ isReconnect: true })
				.finally(() => {
					settled = true;
				});
			while (!settled) await new Promise<void>((r) => setImmediate(r));
			await reconnecting;
			w.engine.setInboundHoldGate(false);

			// Drain the held window — every text lands (burst-tolerant: the guard
			// may coalesce same-chat arrivals into one newline-joined drain turn;
			// that discipline is the burst rows').
			await eventually(() => {
				const blob = [...w.subject.turns()].join("\n");
				return (
					blob.includes("o1") && blob.includes("o2") && blob.includes("o3")
				);
			});

			// Within THIS generation the dedup window still shields exactly-once:
			// a same-id redelivery on the LIVE stream is suppressed by ID even
			// with different payload bytes. (Blob containment — the guard may
			// coalesce same-chat arrivals into one newline-joined turn.)
			const live = w.engine.activeStreamForTests();
			live?.pushMessage("fresh after reconnect", { id: "gen2-fresh" });
			live?.pushMessage("dup-A", { id: "fixed-dup" });
			live?.pushMessage("dup-B (same id, different bytes)", {
				id: "fixed-dup",
			});
			await w.pumpStreamEvents(20);
			await eventually(() =>
				[...w.subject.turns()].join("\n").includes("dup-A"),
			);
			const postReconnectBlob = [...w.subject.turns()].join("\n");
			check(
				postReconnectBlob.includes("dup-A") &&
					!postReconnectBlob.includes("dup-B (same id, different bytes)"),
				"same-generation same-id redelivery suppressed exactly once",
			);
			check(w.engine.dedupHits.duplicates >= 1, "duplicate counter ticked");

			// Replay verdict: all three texts sent during the disconnect landed
			// after the resubscribe (held-window losslessness).
			const replayedAfterResubscribe = ["o1", "o2", "o3"].filter((t) =>
				postReconnectBlob.includes(t),
			).length;
			return {
				sentDuringDisconnect,
				replayedAfterResubscribe,
			};
		},

		/**
		 * Watchdog: the 90s READ timeout detects a silently-wedged stream (no
		 * keepalives, no messages) and the ladder reconnects without loss —
		 * the ONLY death detector that exists on this transport.
		 */
		async watchdogRecovery() {
			const mc = new ManualClock();
			const w = makeNtfyWorld({ name: "ntfy-watchdog", clock: mc });
			await w.connectAndAwaitLive();
			w.server.streams[0]?.pushMessage("before wedge");
			await w.pumpStreamEvents(10);
			await eventually(() => w.subject.turns().includes("before wedge"));

			// WEDGE: no keepalives, no messages. The 90s READ timeout is the
			// only detector — drive virtual time past it.
			w.server.streams[0]?.wedgeSilent();
			await mc.advance(91_000);
			await eventually(() =>
				w.engine.reconnectLog.some((e) => e.includes("reason:read-timeout")),
			);
			const reconnected = await w.adapterRunUntilReconnect();
			const detectedDeadSocket =
				reconnected &&
				w.engine.reconnectLog.some((entry) =>
					entry.includes("reason:read-timeout"),
				);
			check(detectedDeadSocket, "read-timeout fed the reconnect ladder");

			w.server.streams.at(-1)?.pushMessage("after recovery");
			await w.pumpStreamEvents(15);
			await eventually(() => w.subject.turns().includes("after recovery"));
			return {
				detectedDeadSocket: true,
				resumedWithoutLoss:
					w.subject.turns().includes("before wedge") &&
					w.subject.turns().includes("after recovery"),
			};
		},

		/**
		 * Retry-After captured from BOTH sources applied authoritatively: the
		 * two "sources" are wire-lane SendResults at door 1 and door 2; the
		 * §6.1 ladder honors the captured value VERBATIM through the injected
		 * clock (kit send-retry.ts) — PROPOSED DEC ntfy-retry-after-leg.
		 */
		async retryAfterCapture() {
			const mkLane = (): NtfySubject =>
				makeNtfySubject({
					wire: new FakePlatformWire(),
					server: new FakeNtfyServer(),
					name: `ntfy-retry-${Math.floor(Math.random() * 1e6)}`,
				});

			const s1 = mkLane();
			s1.wire.script("send", {
				kind: "fail",
				error: "HTTP 429: rate limited, retry after 7",
				retryAfter: 7,
			});
			await s1.sendThroughDoor1(TOPIC, "first payload");
			const closeCapturedSeconds =
				s1.adapter.lastCapturedRetryAfterSeconds ?? -1;

			const s2 = mkLane();
			s2.wire.script("send", {
				kind: "fail",
				error: "HTTP 429: slow down (retry after 3)",
				retryable: true,
				retryAfter: 3,
			});
			await s2.sendThroughDoor2("ntfy", TOPIC, "second payload");
			const restCapturedSeconds =
				s2.adapter.lastCapturedRetryAfterSeconds ?? -1;

			// Authoritative application via the shared ladder + clock seam.
			const { sendWithRetry } = await import("../kit/index.js");
			const { AutoAdvanceClock } = await import("./clock.js");
			const clock = new AutoAdvanceClock();
			const attempts: Array<SendResult> = [
				{ success: false, error: "retry after 7", retryAfter: 7 },
				{ success: true, messageId: "recovered" },
			];
			let calls = 0;
			const retried = await sendWithRetry(
				"authority payload",
				{},
				async () => {
					const r = attempts[calls];
					calls += 1;
					return r ?? ({ success: true } as SendResult);
				},
				{ maxRetries: 2, sleep: (ms) => clock.sleepMs(ms) },
			);
			const appliedStep = clock.sleepLog.find((s) => s.ms === 7000);

			return {
				closeCapturedSeconds,
				nextDelayMs: appliedStep !== undefined ? 7000 : -1,
				delayAuthoritative:
					appliedStep !== undefined &&
					closeCapturedSeconds === 7 &&
					retried.success === true,
				restCapturedSeconds,
			};
		},

		/**
		 * Feature-gate latch (signal-port leg mapping): NO draft/seal machinery
		 * exists — first draft attempt fails with ZERO wire transmissions; the
		 * verdict is ONE immutable datum; lie-scan proves flipping it FAILS
		 * seal reality BY NAME.
		 */
		async capabilityLatchPermanence() {
			const s = makeNtfySubject({
				wire: new FakePlatformWire(),
				server: new FakeNtfyServer(),
				name: "ntfy-latch",
			});
			const first = await s.streamAdapter().sendDraft({
				chatId: TOPIC,
				draftId: 1,
				content: "**md**",
			});
			const draftOps = s.wire.ops.filter(
				(o) => o.op === "draft" || o.op === "seal",
			).length;
			const latchedOnFirstFailure = first.success === false && draftOps === 0;

			for (let i = 2; i <= 3; i++) {
				await s.streamAdapter().sendDraft({
					chatId: TOPIC,
					draftId: i,
					content: `frame ${i}`,
				});
			}
			const afterSkip = s.wire.ops.filter(
				(o) => o.op === "draft" || o.op === "seal",
			).length;
			const wireAttemptsAfterSkip = afterSkip === 0 ? 1 : -1;

			const transientWorld = makeNtfyWorld({ name: "ntfy-latch-t" });
			transientWorld.wire.script("send", {
				kind: "fail",
				error: "network hiccup mid-send",
			});
			await transientWorld.subject.sendThroughDoor1(TOPIC, "payload");
			const transientDidNotLatch =
				transientWorld.engine.supportsDraftStreaming() === false;

			return {
				latchedOnFirstFailure,
				latchCount: 1,
				wireAttemptsAfterSkip,
				supportsStreamingFalse: s.adapter.supportsDraftStreaming() === false,
				transientDidNotLatch,
			};
		},

		/**
		 * DEC-034 family contract, ntfy dialect: markdown mode ships content
		 * RAW byte-exact under X-Markdown=true; plain mode passes text through
		 * unchanged; the flag scope leg = X-Markdown present ONLY when enabled
		 * and ABSENT-uniform otherwise.
		 */
		async dualPathMarkdown() {
			const plain = makeNtfySubject({
				wire: new FakePlatformWire(),
				server: new FakeNtfyServer(),
				name: "ntfy-md-plain",
			});
			const plainResults = await plain.deliverLongText(
				TOPIC,
				"**bold** body [link](https://x.y)",
			);
			const plainSends = plain.wire.sendsOf(TOPIC);
			const nativeRawByteExact =
				plainResults.every((r) => r.success) &&
				plainSends.length === 1 &&
				plainSends[0]?.content === "**bold** body [link](https://x.y)";

			// Markdown lane: RAW bytes preserved verbatim (server renders).
			const md = makeNtfySubject({
				wire: new FakePlatformWire(),
				server: new FakeNtfyServer(),
				name: "ntfy-md-on",
			});
			md.adapter.setMarkdownEnabledForTests(true);
			const mdResults = await md.deliverLongText(
				TOPIC,
				"**bold** body [link](https://x.y)",
			);
			const mdSends = md.wire.sendsOf(TOPIC);
			const restConvertedBold =
				mdResults.every((r) => r.success) &&
				mdSends.length === 1 &&
				mdSends[0]?.content.includes("**bold**") === true;
			const restConvertedLink =
				mdSends[0]?.content.includes("[link](https://x.y)") === true;

			// Prefix stability: pass-through lanes are trivially stable.
			const nativePrefixStable = nativeRawByteExact && restConvertedBold;

			const tableResults = await md.deliverLongText(
				TOPIC,
				"| a | b |\n|---|---|\n| 1 | 2 |",
			);
			const restConvertedTable =
				tableResults.every((r) => r.success) &&
				md.wire.sendsOf(TOPIC).some((op) => op.content.includes("| a | b |"));

			// Flag scope: X-Markdown ONLY on the markdown-enabled lane's sends;
			// absent-uniform everywhere else. Echo tag rides EVERY publish.
			const mdHeaders = md.server.published.map((p) => p.headers);
			const plainHeaders = plain.server.published.map((p) => p.headers);
			const linkPreviewOnAllTextSends =
				mdHeaders.length > 0 &&
				mdHeaders.every(
					(h) => h["X-Markdown"] === "true" && h["X-Tags"] === NTFY_ECHO_TAG,
				);
			const linkPreviewAbsentOffTextSends =
				plainHeaders.length > 0 &&
				plainHeaders.every(
					(h) => h["X-Markdown"] === undefined && h["X-Tags"] === NTFY_ECHO_TAG,
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

// ── ntfy shape-delta rows ───────────────────────────────────────────────────

function deltaRow(
	id: string,
	title: string,
	body: () => Promise<void>,
): ConformanceRow {
	return {
		id,
		title,
		shapes: new Set(["ws"]),
		run: async () => {
			try {
				await body();
				return { id, title, pass: true, shapes: new Set(["ws"]) as Set<"ws"> };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["ws"]) as Set<"ws">,
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	};
}

function check(condition: unknown, what: string): asserts condition {
	if (!condition) throw new Error(what);
}

export function makeNtfyShapeRows(): ConformanceRow[] {
	return [
		deltaRow(
			"transport.ntfy.stream-dedup-window",
			"ntfy: message-id dedup window (300s TTL, 1000 cap, cutoff eviction), echo-tag self-skip, empty-body skip, topic-as-user identity (title NEVER trusted)",
			async () => {
				const w = makeNtfyWorld({ name: "ntfy-dedup" });
				await w.connectAndAwaitLive();
				const engine = w.engine;

				// Dedup: same id twice → one turn, duplicate counter ticks.
				check(!engine.isDuplicate("dup-1"), "first sight is fresh");
				check(engine.isDuplicate("dup-1"), "second sight is duplicate");

				// Window expiry + cap: the lazy rebuild arms itself once the map
				// passes DEDUP_MAX_SIZE; after the 300s TTL elapses the NEXT
				// insert's rebuild drops EVERY stale id (vendor semantics keep
				// only v > cutoff — nothing expires below the cap).
				const mc = new (
					await import("../persistent-ws/manual-clock.js")
				).ManualClock();
				const expEngine = makeNtfyWorld({
					name: "ntfy-dedup-exp",
					clock: mc,
				}).engine;
				check(!expEngine.isDuplicate("old-1"), "old-1 fresh");
				for (let i = 0; i < 1005; i++) expEngine.isDuplicate(`bulk-${i}`);
				check(expEngine.seenCount > 1000, "cap crossed");
				mc.advance(301_000); // every timestamp now stale
				check(expEngine.isDuplicate("newcomer") === false, "newcomer fresh");
				check(
					expEngine.isDuplicate("old-1") === false,
					`stale ids evicted by the armed rebuild (seen=${expEngine.seenCount})`,
				);
				check(expEngine.seenCount <= 3, `map bounded (${expEngine.seenCount})`);

				// Stream-level gates: echo-tag skip · empty-body skip · title ignored.
				const stream = engine.activeStreamForTests();
				check(stream !== null, "live stream attached");
				stream?.pushMessage("echo me", { tags: ["hermes-agent"] });
				stream?.pushMessage("", {});
				stream?.pushMessage("real payload", { title: "alice (spoof)" });
				await w.pumpStreamEvents(30);
				check(engine.dedupHits.echoSkips >= 1, "echo tag skipped");
				const turns = [...w.subject.turns()];
				check(turns.includes("real payload"), "real message dispatched");
				check(
					turns.every((t) => t !== "alice (spoof)"),
					"publisher-controlled title NEVER becomes identity/text",
				);
			},
		),
		deltaRow(
			"transport.ntfy.backoff-ladder",
			"ntfy: data-driven fixed ladder [2,5,10,30,60]s — steps advance per cycle, index resets after ≥60s alive, delays honored via injected clock",
			async () => {
				const clock = new ManualClock();
				const w = makeNtfyWorld({ name: "ntfy-ladder", clock });
				await w.connectAndAwaitLive();

				// Cycle 1 → 2s, cycle 2 → 5s (index advances while alive <60s).
				await driveUntilSettled(w.engine.runReconnectCycle(), () =>
					clock.advance(1000),
				);
				await driveUntilSettled(w.engine.runReconnectCycle(), () =>
					clock.advance(1000),
				);
				const log = w.engine.reconnectLog;
				check(
					log.some((e) => e.includes("[2s@0]")),
					`step1=2s@0 (${JSON.stringify(log)})`,
				);
				check(
					log.some((e) => e.includes("[5s@1]")),
					"step2=5s@1",
				);

				// Alive ≥60s resets the index to 0.
				await clock.advance(61_000);
				await driveUntilSettled(w.engine.runReconnectCycle(), () =>
					clock.advance(1000),
				);
				check(
					w.engine.reconnectLog.some((e) => e.includes("[2s@0]")),
					`ladder reset after ≥60s alive (${JSON.stringify(w.engine.reconnectLog)})`,
				);
			},
		),
		deltaRow(
			"transport.ntfy.publish-shapes",
			"ntfy: publish wire shapes — ONE POST per delivery, oversized bodies TRUNCATED to content[:4096] with the vendor truncation warning (splitsLongMessages=false ⇒ no split lane); echo tag on EVERY publish; auth headers ride the stream GET with 401/404 classified from MODELED statuses; publish_topic chain metadata→configured→chat_id",
			async () => {
				const w = makeNtfyWorld({ name: "ntfy-publish" });
				await w.connectAndAwaitLive();

				// Within the 4096 vendor cap: EXACTLY ONE op, body verbatim.
				const within = "x".repeat(4000);
				const r1 = await w.subject.deliverLongText(TOPIC, within);
				check(r1.length === 1, `within-cap ships as one op (${r1.length})`);
				check(r1[0]?.success === true, "publish succeeds");
				const pub = w.server.published.at(-1);
				check(pub?.body.length === 4000, `body verbatim (${pub?.body.length})`);
				check(pub?.headers["X-Tags"] === NTFY_ECHO_TAG, "echo tag present");

				// Oversized (>4096): STILL ONE POST, truncated to content[:4096],
				// with the vendor truncation WARNING (adapter.py:send :429-439).
				// MUTANT: a labeled multi-publish lane ships bodies Hermes never
				// sends despite splitsLongMessages=false.
				const big = "y".repeat(9000);
				const results = await w.subject.deliverLongText(TOPIC, big);
				check(
					results.length === 1,
					`oversized ships as ONE op (${results.length})`,
				);
				check(results[0]?.success === true, "truncated publish succeeds");
				check(
					w.server.published.at(-1)?.body ===
						big.slice(0, NTFY_MAX_MESSAGE_CHARS),
					"body is content[:4096]",
				);
				check(
					w.engine.warningLog.some((l) =>
						l.includes("truncated from 9000 to 4096 chars"),
					),
					"truncation warning recorded",
				);

				// publish_topic chain: metadata wins over configured over chat_id.
				const r2 = await w.subject.sendThroughDoor1(
					"fallback-topic",
					"meta wins",
					{
						publish_topic: "meta-topic",
					} as never,
				);
				check(r2.success === true, "metadata publish_topic honored");
				check(
					w.server.published.at(-1)?.topic === "meta-topic",
					"metadata topic used",
				);

				// Stream GET carries _auth_headers() (:233-234); the fake models
				// the vendor RESPONSE STATUS and the adapter classifies fatality
				// from it — never from error strings or harness knobs.
				const guarded = makeNtfyWorld({ name: "ntfy-401", clock: w.clock });
				guarded.server.requiredAuthHeader = "Bearer right-token";
				let unauthorized = false;
				try {
					await guarded.connectAndAwaitLive();
				} catch (err) {
					unauthorized = err instanceof FatalStreamError;
				}
				check(unauthorized, "modeled 401 surfaces as a fatal stream error");
				check(
					guarded.subject.lifecycleSnapshot().state === "fatal" &&
						guarded.subject
							.lifecycleSnapshot()
							.detail.includes("ntfy_unauthorized"),
					"401 ⇒ FATAL ntfy_unauthorized",
				);
				check(
					(guarded.server.subscribeLog.at(-1)?.authHeaders.Authorization ??
						undefined) === undefined,
					"credential-less adapter presented NO Authorization header",
				);
				check(
					guarded.server.streams.length === 0,
					"refused reader NEVER admitted",
				);

				// A MATCHING credential IS admitted, with buildAuthHeader output
				// riding the stream GET.
				const admitted = new FakeNtfyServer();
				admitted.requiredAuthHeader = "Bearer tok-1";
				const s = makeNtfySubject({
					wire: new FakePlatformWire(),
					server: admitted,
					name: "ntfy-auth-ok",
					clock: w.clock,
					token: "tok-1",
				});
				check(
					(await s.adapter.connect({ isReconnect: false })) === true,
					"matching credential admits the reader",
				);
				check(
					admitted.subscribeLog.at(-1)?.authHeaders.Authorization ===
						"Bearer tok-1",
					"buildAuthHeader output rode the stream GET",
				);

				// 404 likewise (modeled status → fatal classification).
				const missing = makeNtfyWorld({ name: "ntfy-404", clock: w.clock });
				missing.server.topicNotFound = true;
				let notFound = false;
				try {
					await missing.connectAndAwaitLive();
				} catch (err) {
					notFound = err instanceof FatalStreamError;
				}
				check(notFound, "modeled 404 surfaces as a fatal stream error");
				check(
					missing.subject.lifecycleSnapshot().state === "fatal" &&
						missing.subject
							.lifecycleSnapshot()
							.detail.includes("ntfy_topic_not_found"),
					"404 ⇒ FATAL ntfy_topic_not_found",
				);
			},
		),
	];
}
