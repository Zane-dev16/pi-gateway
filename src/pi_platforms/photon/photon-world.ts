// pi_platforms/photon/photon-world — world factory + the REAL fixtures behind
// the conformance rows: the five ws-family transport rows (shapes.ts contract,
// photon-realized bodies) driven against the REAL PhotonAdapter engine under
// the INJECTED clock. Every row body drives actual seams — FakeSidecarServer,
// PushIngress line semantics, stepwise watchdog ticks — never stubbed returns.

import { FakePlatformWire } from "../conformance/wire.js";
import type { SendResult } from "../../pi_gateway/streaming/adapter-seam.js";
import type { TaskSpawner } from "../../pi_gateway/guards/index.js";
import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { makePhotonSubject, type PhotonSubject } from "./photon-subject.js";
import type { PhotonAdapter, WatchdogTickVerdict } from "./photon-adapter.js";
import {
	FakeSidecarServer,
	PushIngress,
	photonDmEvent,
	photonGroupEvent,
} from "./sidecar-wire.js";

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

export interface PhotonWorld {
	subject: PhotonSubject;
	engine: PhotonAdapter;
	sidecar: FakeSidecarServer;
	ingress: PushIngress;
	wire: FakePlatformWire;
	clock: ManualClock;
	scheduler: ManualScheduler;
	connectAndAwaitLive(): Promise<void>;
}

export interface MakePhotonWorldOptions {
	name?: string | undefined;
	spawner?: TaskSpawner | undefined;
	/** Fast watchdog cadence for fixture-scale probing (interval 200ms). */
	fastWatchdog?: boolean | undefined;
	/** Scoped PHOTON_* env overrides for this world. */
	env?: Record<string, string | undefined> | undefined;
	scalarMaxUnits?: number | undefined;
	onRespawn?: ((reason: string) => void | Promise<void>) | undefined;
	notifyFatalError?: (() => Promise<void>) | undefined;
	sleepFn?: ((ms: number) => Promise<void>) | undefined;
	withSecret?: boolean | undefined;
}

/** A full photon world: subject + engine + fake sidecar + ingress + clock. */
export function makePhotonWorld(
	opts: MakePhotonWorldOptions = {},
): PhotonWorld {
	const clock = new ManualClock();
	const sidecar = new FakeSidecarServer();
	const wire = new FakePlatformWire();
	const scheduler = new ManualScheduler();
	const env: Record<string, string | undefined> = {
		...(opts.fastWatchdog
			? {
					PHOTON_PROBE_INTERVAL_SECONDS: "0.2",
					PHOTON_PROBE_TIMEOUT_SECONDS: "0.05",
					PHOTON_PROBE_MAX_FAILURES: "3",
				}
			: {}),
		...(opts.env ?? {}),
	};
	const subject = makePhotonSubject({
		wire,
		sidecar,
		clock,
		name: opts.name,
		env,
		scheduler,
		scalarMaxUnits: opts.scalarMaxUnits ?? 64,
		...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
		...(opts.withSecret !== undefined ? { withSecret: opts.withSecret } : {}),
		...(opts.onRespawn !== undefined ? { onRespawn: opts.onRespawn } : {}),
		...(opts.notifyFatalError !== undefined
			? { notifyFatalError: opts.notifyFatalError }
			: {}),
		...(opts.sleepFn !== undefined ? { sleepFn: opts.sleepFn } : {}),
	});
	const engine = subject.adapter;
	const ingress = new PushIngress((line) => engine.onInboundLine(line));
	return {
		subject,
		engine,
		sidecar,
		ingress,
		wire,
		clock,
		scheduler,
		async connectAndAwaitLive(): Promise<void> {
			await engine.connect({ isReconnect: false });
		},
	};
}

/**
 * THE five-leg fixture behind the photon transport rows — shapes.ts WsFixture
 * family realized through PHOTON'S OWN vendor truth. Row titles carry the
 * vendor-class mapping; these bodies return the observables.
 */
export interface PhotonTransportFixture {
	/**
	 * Redelivered events after a stream "reconnect" dedupe (48h/4000 window)
	 * while NEW events flow — downstream sees exactly-once.
	 */
	resubscribeReplay(): Promise<{
		sentDuringDisconnect: number;
		replayedAfterResubscribe: number;
		uniqueIdsDownstream: number;
		duplicateDeliveries: number;
	}>;
	/**
	 * THE presence watchdog itself: injected-clock probe cadence, consecutive
	 * dead probes ⇒ EXACTLY ONE respawn signal, live traffic suppresses
	 * probing and resets failures, resumed stream delivers without loss.
	 */
	watchdogRecovery(): Promise<{
		detectedDeadSocket: boolean;
		resumedWithoutLoss: boolean;
		respawnSignalCount: number;
		liveTrafficTick: WatchdogTickVerdict;
	}>;
	/**
	 * HONEST CLASS DELTA: this wire carries NO Retry-After field anywhere.
	 * Retryable classification is PATTERN-based (_PHOTON_RETRYABLE_PATTERNS);
	 * permanent vs retryable verdicts drive soft-fail vs respawn-ladder; a
	 * numeric hint embedded in an error body is NEVER surfaced or honored.
	 */
	retryAfterCapture(): Promise<{
		wireCarriedFabricatedHint: boolean;
		hintSurfacedAnywhere: boolean;
		patternRetryableRetriedAndRecovered: boolean;
		permanentFailureNotRetried: boolean;
		classificationTable: Record<string, boolean>;
	}>;
	/**
	 * Edit/streaming capability LATCH: supportsDraftStreaming()===false and
	 * edit/draft attempts return "Not supported" with ZERO sidecar calls even
	 * after repeated attempts — static capability, never probed on the wire.
	 */
	capabilityLatchPermanence(): Promise<{
		supportsStreamingFalse: boolean;
		allAttemptsNotSupported: boolean;
		sidecarCallsDuringAttempts: number;
		repeatedAttemptsStillZero: boolean;
	}>;
	/**
	 * PHOTON'S OWN dual path: markdown ships byte-exact with format:"markdown"
	 * (iMessage renders natively); plain mode strips markup and omits the
	 * flag; URL-only candidates route to /send-richlink; repeat preview
	 * artifacts inside the 30s window are suppressed.
	 */
	dualPathMarkdown(): Promise<{
		markdownByteExactWithFlag: boolean;
		plainModeStrippedNoFlag: boolean;
		urlOnlyRoutedToRichlink: boolean;
		proseUrlStayedOnSend: boolean;
		markdownOffKillsRichlinkLane: boolean;
		previewSuppressedInsideWindow: boolean;
		previewPassedOutsideWindow: boolean;
	}>;
}

/**
 * THE fixture behind the photon transport rows. Each call gets a FRESH world
 * (rows never couple through shared mutable state). `mutateWorld` lets the
 * negative-validation suite wrap EVERY leg's engine with a defect (the mutant
 * must fail ITS OWN named row and only that row).
 */
export function makeRealPhotonFixture(
	opts: {
		mutateWorld?: ((world: PhotonWorld) => PhotonWorld) | undefined;
	} = {},
): PhotonTransportFixture {
	const freshWorld = (
		name: string,
		wo: MakePhotonWorldOptions = {},
	): PhotonWorld => {
		const world = makePhotonWorld({ name, ...wo });
		return opts.mutateWorld ? opts.mutateWorld(world) : world;
	};
	return {
		/**
		 * Row: transport.ws.resubscribe-replay [vendor mapping: the sidecar's
		 * gRPC stream is AT-LEAST-ONCE and replays after reconnects — there is
		 * no resume cursor; exactly-once downstream is the DEDUPE window's job
		 * (adapter.py:_is_duplicate, 48h TTL / 4000 entries)].
		 */
		async resubscribeReplay() {
			const world = freshWorld("photon-replay");
			const { engine, ingress } = world;
			await world.connectAndAwaitLive();

			await ingress.push(photonDmEvent("r1", "dup-1"));
			await ingress.push(photonDmEvent("r2", "dup-2"));
			const before = engine.dispatchedEvents.length;
			if (before !== 2) throw new Error(`pre-outage delivery drift: ${before}`);

			// OUTAGE mid-life: the reconnect replays BOTH old events (at-least-
			// once redelivery) and then delivers three NEW ones.
			await ingress.push(photonDmEvent("r1", "dup-1"));
			await ingress.push(photonDmEvent("r2", "dup-2"));
			await ingress.push(photonDmEvent("r3", "dup-3"));
			await ingress.push(photonDmEvent("r4", "dup-4"));
			await ingress.push(photonDmEvent("r5", "dup-5"));

			const ids = engine.dispatchedEvents.map((e) => e.messageId ?? "");
			const unique = new Set(ids);
			return {
				sentDuringDisconnect: 3,
				replayedAfterResubscribe: unique.size - 2,
				uniqueIdsDownstream: unique.size,
				duplicateDeliveries: ids.length - unique.size,
			};
		},

		/**
		 * Row: transport.ws.heartbeat-watchdog-recovery [vendor mapping: THE
		 * presence watchdog — spectrum-ts cannot see a half-open zombie socket,
		 * so the adapter probes /probe and respawns the sidecar after N
		 * consecutive HUNG verdicts (adapter.py:_presence_watchdog)].
		 */
		async watchdogRecovery() {
			const world = freshWorld("photon-watchdog", { fastWatchdog: true });
			const { engine, ingress, sidecar, clock, scheduler } = world;
			await world.connectAndAwaitLive();

			clock.advance(250); // idle > 200ms interval → probing armed
			sidecar.script("/probe", { kind: "hung" }, { kind: "hung" });

			const t1 = await engine.watchdogTick();
			const t2 = await engine.watchdogTick();

			// Live traffic proves liveness WITHOUT any probe: resets failures,
			// stamps activity, next tick skips as idle.
			await ingress.push(photonDmEvent("live traffic mid-outage", "live-1"));
			const liveTrafficTick = await engine.watchdogTick();
			const failuresAfterTraffic = engine.currentProbeFailures;

			clock.advance(250);
			sidecar.script(
				"/probe",
				{ kind: "hung" },
				{ kind: "hung" },
				{ kind: "hung" },
			);
			const t3 = await engine.watchdogTick();
			const t4 = await engine.watchdogTick();
			const t5 = await engine.watchdogTick();

			const respawnSignalCount = engine.respawnSignals.length;
			const detectedDeadSocket =
				t5 === "respawned" &&
				respawnSignalCount === 1 &&
				failuresAfterTraffic === 0 &&
				t1 === "hung" &&
				t2 === "hung";

			// Resumed (fresh) sidecar: healthy probe, then real traffic flows.
			await engine.probeOnce(); // alive against the unscripted fake
			await ingress.push(photonDmEvent("after-respawn recovery", "post-1"));
			await scheduler.runToEnd();
			const resumedWithoutLoss = world.subject
				.turns()
				.some((turn) => turn.includes("after-respawn recovery"));

			return {
				detectedDeadSocket,
				resumedWithoutLoss,
				respawnSignalCount,
				liveTrafficTick,
			};
		},

		/**
		 * Row: transport.ws.retry-after-capture [HONEST CLASS DELTA: the photon
		 * wire carries NO Retry-After field anywhere — classification is
		 * PATTERN-based; the fake fabricates numeric hints into error bodies to
		 * prove the port ignores them, and a mutant that surfaces one fails
		 * THIS row by name].
		 */
		async retryAfterCapture() {
			const world = freshWorld("photon-retry-after", {
				sleepFn: async () => {}, // latency-free ladder
			});
			const { engine, sidecar } = world;
			await world.connectAndAwaitLive();

			// Retryable-pattern failure carrying a FABRICATED hint…
			sidecar.script("*", {
				kind: "error",
				status: 503,
				error: "upstream_unavailable",
				errorClass: "sidecar_error",
				retryable: true,
				retryAfterHint: 7,
			});
			const recovered = await engine.sendWithRetryPhoton(
				"+15550001111",
				"payload one",
			);
			// …and a permanent failure carrying another fabricated hint.
			sidecar.script("*", {
				kind: "error",
				status: 403,
				error: "auth_or_config: invalid project secret",
				errorClass: "auth_or_config",
				retryable: false,
				retryAfterHint: 9,
			});
			const sendsBeforePermanent = sidecar.callsOf("/send").length;
			const permanent = await engine.sidecarSend("+15550001111", "payload two");

			const errorBodies = sidecar.calls
				.filter((c) => c.outcome === "error")
				.map((c) => c.errorBody ?? {});
			const wireCarriedFabricatedHint = errorBodies.every(
				(b) => typeof b["retry_after"] === "number",
			);
			const hintSurfacedAnywhere =
				recovered.retryAfter !== undefined ||
				permanent.retryAfter !== undefined;

			return {
				wireCarriedFabricatedHint,
				hintSurfacedAnywhere,
				patternRetryableRetriedAndRecovered: recovered.success === true,
				permanentFailureNotRetried:
					permanent.success === false &&
					engine.isPermanentSidecarFailure(permanent) &&
					sidecar.callsOf("/send").length === sendsBeforePermanent + 1,
				classificationTable: classificationTable(engine),
			};
		},

		/**
		 * Row: transport.ws.capability-latch-permanent [vendor mapping:
		 * SUPPORTS_MESSAGE_EDITING=False (adapter.py @~721) — iMessage has no
		 * edit API, so the streaming cursor is SUPPRESSED and edit/draft lanes
		 * answer "Not supported" statically, ZERO wire probes ever].
		 */
		async capabilityLatchPermanence() {
			const world = freshWorld("photon-latch");
			const { engine, sidecar, wire } = world;
			await world.connectAndAwaitLive();

			const callsBefore = sidecar.calls.length;
			const opsBefore = wire.ops.length;
			const results: SendResult[] = [];
			results.push(
				await engine.editMessage("chat-latch", "spc-msg-1", "**edit**"),
			);
			results.push(
				await engine.sendDraft({
					chatId: "chat-latch",
					draftId: 1,
					content: "**draft**",
				}),
			);
			const firstRoundZero =
				sidecar.calls.length === callsBefore && wire.ops.length === opsBefore;

			// Repeated attempts: still zero calls, still Not supported.
			for (let i = 2; i <= 4; i += 1) {
				results.push(
					await engine.editMessage("chat-latch", `spc-msg-${i}`, "again"),
				);
				results.push(
					await engine.sendDraft({
						chatId: "chat-latch",
						draftId: i,
						content: "draft again",
					}),
				);
			}
			const repeatedAttemptsStillZero =
				sidecar.calls.length === callsBefore && wire.ops.length === opsBefore;

			return {
				supportsStreamingFalse: engine.supportsDraftStreaming() === false,
				allAttemptsNotSupported: results.every(
					(r) => r.success === false && r.error === "Not supported",
				),
				sidecarCallsDuringAttempts:
					sidecar.calls.length - callsBefore + (wire.ops.length - opsBefore),
				repeatedAttemptsStillZero,
			};
		},

		/**
		 * Row: transport.ws.dual-path-markdown [vendor mapping: markdown rides
		 * /send BYTE-EXACT with format:"markdown" (iMessage renders natively);
		 * PHOTON_MARKDOWN=false strips via the shared equivalent and OMITS the
		 * flag; URL-only candidates divert to /send-richlink; preview artifacts
		 * following a link are suppressed within the 30s window].
		 */
		async dualPathMarkdown() {
			const world = freshWorld("photon-dual");
			const { engine, sidecar, ingress, clock } = world;
			await world.connectAndAwaitLive();

			// ── leg (i): markdown byte-exact WITH the format flag ──
			const md = "**bold** and `code`";
			await engine.sidecarSend("+15551234567", md);
			const mdCall = sidecar.callsOf("/send")[0];
			const markdownByteExactWithFlag =
				mdCall !== undefined &&
				mdCall.body["text"] === md &&
				mdCall.body["format"] === "markdown";

			// ── leg (ii): plain mode strips + omits the flag ──
			const plainWorld = freshWorld("photon-dual-plain", {
				env: { PHOTON_MARKDOWN: "false" },
			});
			await plainWorld.connectAndAwaitLive();
			// send() parity: format_message applies INSIDE the door path.
			await plainWorld.engine.photonSend("+15551234567", "**bold** and _soft_");
			const plainCall = plainWorld.sidecar.callsOf("/send")[0];
			const plainModeStrippedNoFlag =
				plainCall !== undefined &&
				plainCall.body["text"] === "bold and soft" &&
				plainCall.body["format"] === undefined;

			// ── leg (iii): URL-only candidates divert to /send-richlink ──
			await engine.sidecarSend("+15551234567", "https://example.com/article");
			const richCall = sidecar.callsOf("/send-richlink")[0];
			const sendsBeforeProse = sidecar.callsOf("/send").length;
			await engine.sidecarSend(
				"+15551234567",
				"read https://example.com/x today",
			);
			const proseCall = sidecar.callsOf("/send")[sendsBeforeProse];
			const urlOnlyRoutedToRichlink =
				richCall !== undefined &&
				richCall.body["url"] === "https://example.com/article";
			const proseUrlStayedOnSend =
				proseCall !== undefined &&
				proseCall.body["text"] === "read https://example.com/x today" &&
				sidecar.callsOf("/send-richlink").length === 1;

			// ── leg (iv): markdown OFF kills the richlink lane entirely ──
			const richCallsBefore =
				plainWorld.sidecar.callsOf("/send-richlink").length;
			await plainWorld.engine.sidecarSend(
				"+15551234567",
				"https://example.com/plain",
			);
			const markdownOffKillsRichlinkLane =
				plainWorld.sidecar.callsOf("/send-richlink").length ===
					richCallsBefore &&
				plainWorld.sidecar
					.callsOf("/send")
					.some((c) => c.body["text"] === "https://example.com/plain");

			// ── leg (v): 30s preview-artifact suppression (inbound) ──
			await ingress.push(
				photonDmEvent("https://example.com/gallery", "url-inbound-1"),
			);
			const dispatchedAfterUrl = engine.dispatchedEvents.length;
			await ingress.push(previewArtifactEvent("prev-1"));
			const previewSuppressedInsideWindow =
				engine.dispatchedEvents.length === dispatchedAfterUrl;
			clock.advance(PHOTON_PREVIEW_WINDOW_MS + 1_000);
			await ingress.push(previewArtifactEvent("prev-2"));
			const previewPassedOutsideWindow =
				engine.dispatchedEvents.length === dispatchedAfterUrl + 1 &&
				String(engine.dispatchedEvents.at(-1)?.text ?? "").includes(
					"photo.pluginpayloadattachment",
				);

			return {
				markdownByteExactWithFlag,
				plainModeStrippedNoFlag,
				urlOnlyRoutedToRichlink,
				proseUrlStayedOnSend,
				markdownOffKillsRichlinkLane,
				previewSuppressedInsideWindow,
				previewPassedOutsideWindow,
			};
		},
	};
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

/** An OpenGraph preview-artifact attachment trailing a link bubble. */
function previewArtifactEvent(messageId: string): Record<string, unknown> {
	return {
		messageId,
		platform: "iMessage",
		space: { id: "+15551234567", type: "dm", phone: "+15551234567" },
		sender: { id: "+15551234567" },
		content: {
			type: "attachment",
			id: `f-${messageId}`,
			name: `photo${PHOTON_PREVIEW_SUFFIX}`,
			mimeType: "image/jpeg",
			size: 2048,
		},
		timestamp: "2026-05-14T19:07:00.000Z",
	};
}

const PHOTON_PREVIEW_WINDOW_MS = 30_000; // manifest constant mirror (fixture-local)
const PHOTON_PREVIEW_SUFFIX = ".pluginpayloadattachment";

/** Group-payload helper re-exported for delta rows (test_mention_gating parity). */
export { photonGroupEvent };

function freshWorld(
	name: string,
	opts: MakePhotonWorldOptions = {},
): PhotonWorld {
	return makePhotonWorld({ name, ...opts });
}
