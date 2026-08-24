// pi_platforms/conformance/shapes — transport-shape row requirements
// (DEC-002: polling / persistent-ws / webhook) plus the NAMED HOOKS where
// transport-specific §8 rows attach. Shared rows are fully encoded in
// rows.ts; the hook factories below define the fixture CONTRACTS that Phase-3
// adapter agents fill with real outage/replay/window scenarios.

import type { Shape } from "./harness.js";
import type { ConformanceRow } from "./rows.js";

/** Rows REQUIRED per shape beyond the shared set (04 §8 per-shape rows). */
export const TRANSPORT_ROW_REQUIREMENTS: Record<Shape, readonly string[]> = {
	polling: [
		"transport.polling.outage-reconnect-preserves-queue",
		"transport.polling.held-inbound-redispatch",
		"transport.polling.conflict-zombie-eviction",
		"transport.polling.heartbeat-escalation",
	],
	ws: [
		"transport.ws.resubscribe-replay",
		"transport.ws.heartbeat-watchdog-recovery",
		"transport.ws.retry-after-capture",
		"transport.ws.capability-latch-permanent",
		"transport.ws.dual-path-markdown",
	],
	webhook: [
		"transport.webhook.flags-and-trust-boundary",
		"transport.webhook.bounded-window-answer",
	],
};

/**
 * Named hook: POLLING fixture contract (§3.1 / extended §8 polling row).
 * Adapter agents implement PollingFixture against their adapter; the returned
 * row bodies assert observable behavior:
 *   - outage + reconnect PRESERVES the server-side update queue
 *     (is_reconnect ⇒ drop_pending_updates=false parity);
 *   - held-inbound redispatch covers the ack-before-enqueue window;
 *   - 409-conflict recovery evicts the zombie session under a fresh
 *     polling generation (drop_pending_updates=true);
 *   - heartbeat stuck-probe escalation feeds the reconnect ladder.
 */
export interface PollingFixture {
	/** Simulate a transport outage mid-stream and reconnect with queue preservation. */
	simulateOutageAndReconnect(): Promise<{
		queuedBeforeReconnect: number;
		deliveredAfterReconnect: number;
	}>;
	/** Hold an inbound event during a disconnect window, then redispatch on _mark_connected. */
	holdAndRedispatch(): Promise<{ held: number; redispatched: number }>;
	/** Second consumer steals the poll; recovery evicts the zombie session. */
	conflictRecovery(): Promise<{
		generationsBumped: number;
		dropPendingUpdatesOnRestart: boolean;
		fatalAfterExhaustion: boolean;
	}>;
	/** Two stuck heartbeats escalate to the reconnect ladder. */
	heartbeatEscalation(): Promise<{
		stuckProbes: number;
		reconnectTriggered: boolean;
	}>;
}

export function makePollingRows(fixture: PollingFixture): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: () => Promise<Record<string, unknown>>,
		asserts: (r: Record<string, unknown>) => string | null,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["polling"]),
		run: async () => {
			try {
				const result = await body();
				const problem = asserts(result);
				if (problem)
					return {
						id,
						title,
						pass: false,
						shapes: new Set(["polling"]),
						detail: problem,
					};
				return { id, title, pass: true, shapes: new Set(["polling"]) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["polling"]),
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	});
	return [
		mk(
			"transport.polling.outage-reconnect-preserves-queue",
			"polling: outage + reconnect preserves server-side update queue",
			() => fixture.simulateOutageAndReconnect(),
			(r) => {
				const before = Number(r.queuedBeforeReconnect ?? 0);
				const after = Number(r.deliveredAfterReconnect ?? 0);
				return after >= before
					? null
					: `updates lost across reconnect (${before} → ${after})`;
			},
		),
		mk(
			"transport.polling.held-inbound-redispatch",
			"polling: ack-before-enqueue window covered by hold-and-redispatch (cap 64, never drop)",
			() => fixture.holdAndRedispatch(),
			(r) =>
				r.redispatched === r.held
					? null
					: `held ${String(r.held)} but redispatched ${String(r.redispatched)}`,
		),
		mk(
			"transport.polling.conflict-zombie-eviction",
			"polling: 409-conflict recovery evicts zombie session under fresh generation (drop_pending_updates=true), fatal on exhaustion",
			() => fixture.conflictRecovery(),
			(r) => {
				if (r.dropPendingUpdatesOnRestart !== true)
					return "restart must pass drop_pending_updates=True to kill the stale server session";
				if (r.fatalAfterExhaustion !== true)
					return "exhausted conflict retries must end FATAL";
				return Number(r.generationsBumped ?? 0) >= 1
					? null
					: "polling generation must bump on recovery restart";
			},
		),
		mk(
			"transport.polling.heartbeat-escalation",
			"polling: TWO consecutive stuck probes feed the reconnect ladder",
			() => fixture.heartbeatEscalation(),
			(r) =>
				r.stuckProbes === 2 && r.reconnectTriggered === true
					? null
					: "escalation requires 2 stuck probes triggering reconnect",
		),
	];
}

/**
 * Named hook: WS fixture contract (§8 ws row + DEC-032/034 completion set).
 * Resubscribe replay must cover messages sent during the disconnect; the
 * heartbeat watchdog recovers a dead socket without dropping in-flight turns;
 * Retry-After captured from close payloads AND REST results shapes the next
 * ladder delay; feature-gate errors latch native streaming off permanently;
 * markdown dispatch is DUAL-PATH (native RAW / REST converted / link-preview
 * suppression on text sends only).
 */
export interface WsFixture {
	resubscribeReplay(): Promise<{
		sentDuringDisconnect: number;
		replayedAfterResubscribe: number;
	}>;
	watchdogRecovery(): Promise<{
		detectedDeadSocket: boolean;
		resumedWithoutLoss: boolean;
	}>;
	/** Close-payload AND REST-result Retry-After capture shapes next delay. */
	retryAfterCapture(): Promise<{
		closeCapturedSeconds: number;
		nextDelayMs: number;
		delayAuthoritative: boolean;
		restCapturedSeconds: number;
	}>;
	/** A23: feature-gate failure latches native streaming OFF for the session. */
	capabilityLatchPermanence(): Promise<{
		latchedOnFirstFailure: boolean;
		latchCount: number;
		wireAttemptsAfterSkip: number;
		supportsStreamingFalse: boolean;
		transientDidNotLatch: boolean;
	}>;
	/** DEC-034 dual-path evidence: native RAW / REST converts / flag scope. */
	dualPathMarkdown(): Promise<{
		nativeRawByteExact: boolean;
		nativePrefixStable: boolean;
		restConvertedBold: boolean;
		restConvertedLink: boolean;
		restConvertedTable: boolean;
		linkPreviewOnAllTextSends: boolean;
		linkPreviewAbsentOffTextSends: boolean;
	}>;
}

export function makeWsRows(fixture: WsFixture): ConformanceRow[] {
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
			"ws: resubscribe replay covers messages sent during disconnect — cursor-exact, exactly-once downstream",
			() => fixture.resubscribeReplay(),
			(r) =>
				r.replayedAfterResubscribe === r.sentDuringDisconnect
					? null
					: `replay gap (${String(r.sentDuringDisconnect)} sent vs ${String(r.replayedAfterResubscribe)} replayed)`,
		),
		mk(
			"transport.ws.heartbeat-watchdog-recovery",
			"ws: heartbeat watchdog detects dead socket and resumes without loss",
			() => fixture.watchdogRecovery(),
			(r) =>
				r.detectedDeadSocket === true && r.resumedWithoutLoss === true
					? null
					: "watchdog must detect death AND resume cleanly",
		),
		mk(
			"transport.ws.retry-after-capture",
			"ws: Retry-After captured from close payload AND REST result; captured value IS the next reconnect delay (authoritative)",
			() => fixture.retryAfterCapture(),
			(r) => {
				if (
					Number(r.closeCapturedSeconds) <= 0 ||
					Number(r.restCapturedSeconds) <= 0
				)
					return "Retry-After must be captured from BOTH sources";
				if (r.delayAuthoritative !== true)
					return "captured value must drive an AUTHORITATIVE ladder step";
				return Number(r.nextDelayMs) === Number(r.closeCapturedSeconds) * 1000
					? null
					: `next delay ${String(r.nextDelayMs)}ms does not honor captured ${String(r.closeCapturedSeconds)}s`;
			},
		),
		mk(
			"transport.ws.capability-latch-permanent",
			"ws: feature-gate error latches native streaming OFF permanently; later attempts skip the wire entirely; transient failures never latch",
			() => fixture.capabilityLatchPermanence(),
			(r) => {
				if (
					r.latchedOnFirstFailure !== true ||
					r.supportsStreamingFalse !== true
				)
					return "feature-gate error must latch streaming off immediately";
				if (Number(r.wireAttemptsAfterSkip) !== 1)
					return "post-latch attempts must SKIP the wire (attempt count frozen at 1)";
				if (r.transientDidNotLatch !== true)
					return "transient failures must NOT latch";
				return Number(r.latchCount) === 1
					? null
					: "latch fires at most ONCE per session";
			},
		),
		mk(
			"transport.ws.dual-path-markdown",
			"ws dual-path markdown (DEC-034): native stream ships RAW prefix-stable bytes; REST path converts to mrkdwn; link-preview suppression is a text-send-only flag",
			() => fixture.dualPathMarkdown(),
			(r) => {
				for (const leg of [
					"nativeRawByteExact",
					"nativePrefixStable",
					"restConvertedBold",
					"restConvertedLink",
					"restConvertedTable",
					"linkPreviewOnAllTextSends",
					"linkPreviewAbsentOffTextSends",
				] as const) {
					if (r[leg] !== true) return `${leg} violated`;
				}
				return null;
			},
		),
	];
}

/**
 * Named hook: WEBHOOK fixture contract (§3 C5 split + DEC-017).
 * Bounded shapes answer within the provider window; flag pairing asserted as
 * DATA (interactive_resume=False + supports_async_delivery=False); api_server-
 * class unbounded windows expose cooperative interruption.
 */
export interface WebhookFixture {
	/** Answer within the bounded sync window (WhatsApp-Cloud-class). */
	boundedWindowAnswer(): Promise<{
		answeredWithinWindowMs: number;
		windowCapMs: number;
	}>;
	/** Flag pairing + trust-boundary manifest completeness. */
	flagsAndTrust(): Promise<{
		interactiveResumeFalse: boolean;
		supportsAsyncDeliveryFalse: boolean;
		trustBoundaryComplete: boolean;
	}>;
}

export function makeWebhookRows(fixture: WebhookFixture): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: () => Promise<Record<string, unknown>>,
		asserts: (r: Record<string, unknown>) => string | null,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["webhook"]),
		run: async () => {
			try {
				const result = await body();
				const problem = asserts(result);
				if (problem)
					return {
						id,
						title,
						pass: false,
						shapes: new Set(["webhook"]),
						detail: problem,
					};
				return { id, title, pass: true, shapes: new Set(["webhook"]) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["webhook"]),
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	});
	return [
		mk(
			"transport.webhook.bounded-window-answer",
			"webhook: bounded-window shape answers within the provider request window",
			() => fixture.boundedWindowAnswer(),
			(r) => {
				const answered = Number(r.answeredWithinWindowMs ?? Infinity);
				const cap = Number(r.windowCapMs ?? 0);
				return answered <= cap
					? null
					: `answered in ${answered}ms > ${cap}ms window`;
			},
		),
		mk(
			"transport.webhook.flags-and-trust-boundary",
			"webhook: interactive_resume=False + supports_async_delivery=False set; DEC-017 trust boundary complete",
			() => fixture.flagsAndTrust(),
			(r) => {
				if (
					r.interactiveResumeFalse !== true ||
					r.supportsAsyncDeliveryFalse !== true
				)
					return "stateless shapes MUST set both flags False";
				return r.trustBoundaryComplete === true
					? null
					: "trust boundary manifest incomplete";
			},
		),
	];
}
