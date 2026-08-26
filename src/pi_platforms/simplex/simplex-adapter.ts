// pi_platforms/simplex/simplex-adapter — THE SimpleX Chat adapter, ported
// from the READ-ONLY Hermes plugin plugins/platforms/simplex/adapter.py onto
// the kit base. Everything policy-shaped is inherited or manifest DATA; this
// module supplies TRANSPORT (ONE persistent WebSocket to the local
// simplex-chat daemon carrying inbound events AND outbound JSON commands,
// correlated by corrId) and the chat-item pipeline.
//
// Shape (DEC-002 ws family — persistent daemon socket, bidirectional):
//   - inbound: {"corrId","resp"} envelopes pushed by the daemon; reconnect
//     ladder initial 2s doubling ×2 capped 60s with ≤20% jitter, RESET on
//     successful connect; ConnectionClosed vs unexpected error feed the SAME
//     ladder while running (adapter.py:_ws_listener parity)
//   - outbound: JSON commands on the SAME socket — structured "/_send
//     #<gid>|@<cid> json <composed>" (bracket chat-command syntax parses as a
//     display-name lookup and silently drops unresolved names — ID-addressed
//     json form avoids that), "/accept <reqId>", fire-and-forget "/freceive"
//     (the daemon never acks it — awaiting would stall a full timeout)
//   - health monitor DELIBERATELY LOG-ONLY (adapter.py:_health_monitor):
//     liveness is carried by PROTOCOL PING/PONG on the seam — Hermes got it
//     from the websockets client (ping_interval=20/ping_timeout=20); this
//     port expresses the SAME keepalive itself (adapter-side loop), so
//     application silence NEVER reconnects a healthy quiet link while a
//     genuinely dead one times out its ping into the SAME ladder
//   - NO message-edit API exists on this wire ⇒ native draft streaming
//     excluded BY THE PROBE from SIMPLEX_SUPPORTS_MESSAGE_EDITING=false; NO
//     typing indicator (send_typing deliberate no-op); channel directory via
//     listChannels (/contacts + /groups correlated commands)
//
// Layering: imports pi_gateway downward + kit same-layer ONLY; no adapter
// cross-imports.

import { existsSync } from "node:fs";
import { mkdtemp, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { extname, join } from "node:path";

import {
	BasePlatformAdapter,
	CallbackQueryRouter,
	ActionHandlerRegistry,
	ClarifyPendingStore,
	OneShotPendingStore,
	chunkWithFenceCarry,
	classifySendError,
	plainTextFallbackBody,
	resolveEnablement,
	FormattingLadder,
	sendWithRetry,
	DELIVERY_FAILED_NOTICE,
} from "../kit/index.js";
import type { ChunkPlan } from "../kit/chunking.js";
import type {
	Metadata,
	SendResult,
	StreamLogger,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	MessageType,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type {
	AdapterStatusSnapshot,
	DisableReason,
} from "../kit/lifecycle-state.js";
import type { LengthUnit } from "../kit/length-policy.js";
import { buildSessionKey } from "../../pi_gateway/resolution/session-key.js";

import {
	HEALTH_CHECK_INTERVAL_MS,
	HEALTH_CHECK_STALE_THRESHOLD_MS,
	SIMPLEX_AUTO_ACCEPT_ENV,
	simplexAutoAcceptFromEnv,
	SIMPLEX_CAPABILITIES,
	SIMPLEX_COMMAND_TIMEOUT_MS,
	SIMPLEX_CONNECT_OPEN_TIMEOUT_MS,
	SIMPLEX_CORR_PREFIX,
	SIMPLEX_GROUP_ALLOWED_ENV,
	isAudioExt,
	isImageExt,
	SIMPLEX_LIST_CHANNELS_COMMAND_TIMEOUT_MS,
	SIMPLEX_MAX_MESSAGE_LENGTH,
	SIMPLEX_MAX_PENDING_CORR,
	SIMPLEX_PLUGIN_MANIFEST,
	parseCommaList,
	SIMPLEX_SUPPORTS_MESSAGE_EDITING,
	SIMPLEX_TEXT_BATCH_DELAY_DEFAULT_S,
	SIMPLEX_TEXT_BATCH_DELAY_ENV,
	simplexTextBatchDelayMs,
	SIMPLEX_VOICE_EXTS,
	SIMPLEX_WS_PING_CLOSE_CODE,
	SIMPLEX_WS_PING_INTERVAL_MS,
	SIMPLEX_WS_PING_TIMEOUT_MS,
	WS_JITTER_FRACTION,
	WS_RETRY_DELAY_INITIAL_MS,
	WS_RETRY_DELAY_MAX_MS,
} from "./manifest.js";
import type {
	SimplexCloseInfo,
	SimplexConnection,
	SimplexConnectionFactory,
	SimplexSocketListener,
} from "./simplex-wire.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const SIMPLEX_REGISTRY: CommandRegistry = [
	{
		name: "new",
		aliases: ["reset"],
		busyPolicy: "interrupt_then_dispatch" as const,
		busyHandler: "new",
	},
	{
		name: "stop",
		busyPolicy: "interrupt_then_dispatch" as const,
		busyHandler: "stop",
	},
	{ name: "model", busyPolicy: "reject" as const, busyHandler: "model" },
	{ name: "approve", busyPolicy: "dispatch" as const },
	{ name: "status", busyPolicy: "dispatch" as const },
];

/**
 * Subject/harness egress CAPTURE seam (msgraph-webhook captureWire precedent):
 * production construction leaves it unset — the composed command rides the
 * daemon socket and send() succeeds off the WS write attempt. A conformance
 * subject supplies it so the SAME door records every user-visible text onto
 * the in-memory capture instrument for the shared egress rows.
 */
export interface SimplexEgressCapture {
	transmitSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult>;
	hasRichScript(opKind: string): boolean;
	transmitRich(chatId: string, content: string): Promise<SendResult>;
}

/**
 * Prepared image artifact riding the /_send image payload
 * (adapter.py:_prepare_image parity): the PNG-converted path plus a small
 * inline-thumbnail data URI for the msgContent.image field.
 */
export interface SimplexPreparedImage {
	pngPath: string;
	thumbDataUri: string;
}

/**
 * _prepare_image SEAM — PNG conversion + thumbnail generation. Production
 * default mirrors Hermes' no-converter fallback posture (original bytes,
 * empty thumbnail, logged); deployments with an encoder wire one here.
 */
export type SimplexImagePreparer = (
	filePath: string,
) => Promise<SimplexPreparedImage>;

/**
 * cache_image_from_url SEAM (base.py:883) — download an http(s) image to a
 * local cache path. Default uses global fetch + a temp dir; fixtures inject
 * deterministic fetchers.
 */
export type SimplexImageUrlFetcher = (url: string) => Promise<string>;

export interface SimplexAdapterOptions {
	/** Injected WS connection factory (fake daemon in tests). */
	wsFactory: SimplexConnectionFactory;
	/** Scoped reader over SIMPLEX_* names (fail-closed; DEC-003/009). */
	secretReader?: ScopedSecretReader | undefined;
	scalarMaxUnits?: number | undefined;
	nowMs?: (() => number) | undefined;
	sleepMs?: ((ms: number) => Promise<void>) | undefined;
	rng?: (() => number) | undefined;
	logger?: StreamLogger | undefined;
	spawner?: TaskSpawner | undefined;
	/** plugin.yaml SIMPLEX_AUTO_ACCEPT (default true; env overrides). */
	autoAccept?: boolean | undefined;
	/** Parsed SIMPLEX_GROUP_ALLOWED entries ('*' opens all; empty disables). */
	groupAllowFrom?: readonly string[] | undefined;
	/** Quiet-period batch delay ms (HERMES_SIMPLEX_TEXT_BATCH_DELAY wiring). */
	textBatchDelayMs?: number | undefined;
	/** Pending-corr bound (fixture seam injects smaller caps). */
	maxPendingCorr?: number | undefined;
	/** _prepare_image seam (PNG conversion + inline thumbnail data URI). */
	imagePreparer?: SimplexImagePreparer | undefined;
	/** cache_image_from_url seam (http(s) download → local cache path). */
	imageUrlFetcher?: SimplexImageUrlFetcher | undefined;
	captureWire?: SimplexEgressCapture | undefined;
	/**
	 * Lie-scan injection point: THE manifest datum that drives the
	 * streaming-exclusion probe. Production defaults to the frozen constant;
	 * only the lying-mutant fixture overrides it.
	 */
	declaredMessageEditing?: boolean | undefined;
}

export interface SimplexCounters {
	events: number;
	correlatedResponses: number;
	echoesDiscarded: number;
	contactRequestsAutoAccepted: number;
	fileDescriptorsReady: number;
	unhandledEventTypes: number;
	ownMessagesDropped: number;
	nonRcvContent: number;
	emptyContentDropped: number;
	unhandledChatTypes: number;
	noSender: number;
	groupsDisabled: number;
	groupsNotAllowed: number;
	transfersDeferred: number;
	transfersCompleted: number;
	commandTimeouts: number;
	droppedWrites: number;
	pingsSent: number;
	pongsReceived: number;
	pingTimeouts: number;
	accepted: number;
	batchesFlushed: number;
}

interface PendingResponse {
	resolve(value: Record<string, unknown> | null): void;
	promise: Promise<Record<string, unknown> | null>;
	settled: boolean;
}

interface BatchFlushState {
	cancelled: boolean;
	wake: (() => void) | null;
}

/**
 * The kit-built SimpleX adapter. Hermes anchors:
 * plugins/platforms/simplex/adapter.py:SimplexAdapter (all method-level
 * anchors cited inline below).
 */
export class SimplexAdapter extends BasePlatformAdapter {
	readonly pluginManifest = SIMPLEX_PLUGIN_MANIFEST;
	readonly transport: SimplexConnectionFactory;

	private readonly secretReader: ScopedSecretReader;
	private readonly nowFn: () => number;
	private readonly sleepFn: (ms: number) => Promise<void>;
	private readonly rngFn: () => number;
	private readonly declaredMessageEditing: boolean;
	private readonly captureWire: SimplexEgressCapture | undefined;
	private readonly imagePreparer: SimplexImagePreparer | undefined;
	private readonly imageUrlFetcher: SimplexImageUrlFetcher | undefined;
	private readonly cp: EgressChokepoint;

	// ── config (__init__ parity, injected as data) ──────────────────────────
	readonly wsUrl: string;
	readonly autoAccept: boolean;
	readonly groupAllowSet: ReadonlySet<string>;
	readonly textBatchDelayMs: number;
	readonly maxPendingCorr: number;

	// ── listener state (_ws_listener/_health_monitor) ────────────────────────
	private running = false;
	private lastWsActivityMs = 0;
	private conn: SimplexConnection | null = null;
	private listenerTask: Promise<void> | null = null;
	private healthTask: Promise<void> | null = null;
	private keepaliveTask: Promise<void> | null = null;
	/** Outstanding keepalive ping's send time (null = none outstanding). */
	private awaitingPongAtMs: number | null = null;
	/** Reconnect ladder steps chosen so far (observability). */
	readonly reconnectLog: Array<{
		delayMs: number;
		jittered: boolean;
		trigger: "connection-closed" | "connect-failed";
	}> = [];
	/** Ladder resets observed on successful connects. */
	readonly reconnectResets: Array<{ atMs: number }> = [];
	/** LOG-ONLY idle observations (posture evidence; NEVER a reconnect). */
	readonly idleLogs: Array<{ atMs: number; elapsedMs: number }> = [];
	readonly closeEvents: Array<{ atMs: number; reason: string }> = [];

	// ── corr machinery (_pending_corr_ids / _pending_responses) ─────────────
	private readonly pendingCorrIds = new Set<string>();
	private readonly pendingResponses = new Map<string, PendingResponse>();
	private corrCounter = 0;

	// ── deferred file transfers (_pending_file_transfers) ────────────────────
	private readonly pendingFileTransfers = new Map<
		number,
		Record<string, unknown>
	>();

	// ── text batching (_pending_text_batches/_pending_text_batch_tasks) ─────
	private readonly textBatches = new Map<string, IncomingEvent>();
	private readonly textBatchFlushes = new Map<string, BatchFlushState>();

	readonly counts: SimplexCounters = {
		events: 0,
		correlatedResponses: 0,
		echoesDiscarded: 0,
		contactRequestsAutoAccepted: 0,
		fileDescriptorsReady: 0,
		unhandledEventTypes: 0,
		ownMessagesDropped: 0,
		nonRcvContent: 0,
		emptyContentDropped: 0,
		unhandledChatTypes: 0,
		noSender: 0,
		groupsDisabled: 0,
		groupsNotAllowed: 0,
		transfersDeferred: 0,
		transfersCompleted: 0,
		commandTimeouts: 0,
		droppedWrites: 0,
		pingsSent: 0,
		pongsReceived: 0,
		pingTimeouts: 0,
		accepted: 0,
		batchesFlushed: 0,
	};

	/** Every ACCEPTED inbound item's routing identity (row observability). */
	readonly acceptedLog: Array<{
		chatId: string;
		chatType: "dm" | "group";
		userId: string;
		chatName: string;
		messageType: MessageType;
		mediaUrls: readonly string[];
		text: string;
	}> = [];

	// Interactive surfaces (kit-owned resolution seam — no native buttons).
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	// Subject/test observability lanes.
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly resolvedFamilies: string[] = [];
	private allowAllClickers = true;
	private readonly clarifyArmedSet = new Set<string>();
	private holding = false;
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};

	/** Interruptible-sleep registry: disconnect() flushes in-flight waits. */
	private readonly pendingSleepWakes = new Set<() => void>();

	constructor(opts: SimplexAdapterOptions) {
		super({
			manifestName: SIMPLEX_PLUGIN_MANIFEST.name,
			capabilities: SIMPLEX_CAPABILITIES,
			scalarMaxUnits: opts.scalarMaxUnits ?? SIMPLEX_MAX_MESSAGE_LENGTH,
			...(opts.logger !== undefined ? { logger: opts.logger } : {}),
		});
		this.transport = opts.wsFactory;
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.sleepFn =
			opts.sleepMs ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
		this.rngFn = opts.rng ?? (() => Math.random());
		this.captureWire = opts.captureWire;
		this.imagePreparer = opts.imagePreparer;
		this.imageUrlFetcher = opts.imageUrlFetcher;
		this.wsUrl = (this.secretReader("SIMPLEX_WS_URL") ?? "").trim();
		// __init__ env resolution parity (adapter.py:__init__): operator-set
		// env WINS over injected config, which wins over defaults.
		//   SIMPLEX_AUTO_ACCEPT — '0'/'false'/'no'/empty ⇒ false (any case);
		//     set-but-empty DISABLES even when an option says true.
		const envAutoAccept = simplexAutoAcceptFromEnv(
			this.secretReader(SIMPLEX_AUTO_ACCEPT_ENV),
		);
		this.autoAccept = envAutoAccept ?? opts.autoAccept ?? true;
		//   SIMPLEX_GROUP_ALLOWED — comma-split; set-but-EMPTY falls back to
		//     the option (Python `os.getenv(K, "") or extra.get(...)`).
		const envGroups = this.secretReader(SIMPLEX_GROUP_ALLOWED_ENV);
		this.groupAllowSet = new Set(
			envGroups !== undefined && envGroups.trim() !== ""
				? parseCommaList(envGroups)
				: (opts.groupAllowFrom ?? []),
		);
		//   HERMES_SIMPLEX_TEXT_BATCH_DELAY — quiet-period seconds through THE
		//   batch-delay helper (invalid values fall back to the default).
		const envBatchDelay = this.secretReader(SIMPLEX_TEXT_BATCH_DELAY_ENV);
		this.textBatchDelayMs =
			envBatchDelay !== undefined && envBatchDelay.trim() !== ""
				? simplexTextBatchDelayMs(envBatchDelay)
				: (opts.textBatchDelayMs ?? SIMPLEX_TEXT_BATCH_DELAY_DEFAULT_S * 1000);
		this.maxPendingCorr = opts.maxPendingCorr ?? SIMPLEX_MAX_PENDING_CORR;
		this.declaredMessageEditing =
			opts.declaredMessageEditing ?? SIMPLEX_SUPPORTS_MESSAGE_EDITING;

		// §11 step 3/4: missing required secret ⇒ LOUD disable (status-visible).
		// Hermes gates via check_requirements() (SIMPLEX_WS_URL required);
		// the kit expresses the same posture at construction so /status shows
		// the reason instead of a silent skip.
		const enablement = resolveEnablement(
			SIMPLEX_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // no edit API ⇒ no native draft lanes
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			transmitEdit: async () => ({ success: false, error: "Not supported" }),
			transmitSeal: async () => ({ success: false, error: "Not supported" }),
		});

		this.router = new CallbackQueryRouter({
			stores: {
				approvals: this.approvals,
				slashConfirms: this.slashConfirms,
				appr: this.appr,
				clarify: this.clarify,
			},
			authorizer: () => this.allowAllClickers,
			onExecApproval: async () => {
				this.resolvedFamilies.push("ea");
				return "ok";
			},
			onSlashConfirm: async () => {
				this.resolvedFamilies.push("sc");
				return "ok";
			},
			onClarifyChoice: async (_k, _id, idx) => {
				this.resolvedFamilies.push("cl");
				return `answer-${idx}`;
			},
			onWhatsappApproval: async () => {
				this.resolvedFamilies.push("appr");
				return "ok";
			},
			onPickerNav: async (parsed) => ({ answerText: `nav:${parsed.family}` }),
		});
	}

	// ── capabilities: THE probe-computed streaming exclusion ────────────────

	/**
	 * Native draft streaming is excluded BY THE PROBE from the manifest datum
	 * SIMPLEX_SUPPORTS_MESSAGE_EDITING=false — the source exposes NO edit API
	 * anywhere (no edit method, no edit wire shape), so a draft cursor could
	 * never be sealed or edited away. Flip the datum and this flips — the
	 * lie-scan mutant proves the flip FAILS the streaming rows.
	 */
	override supportsDraftStreaming(_chatType?: string): boolean {
		return this.declaredMessageEditing;
	}

	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.lifecycle.statusSnapshot();
	}

	// ── connect/disconnect (adapter.py connect/disconnect parity) ───────────

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (!this.wsUrl) {
			this.logger?.error?.("SimpleX: SIMPLEX_WS_URL is required");
			return false;
		}
		// Quick connectivity check — open a probe connection and immediately
		// close it (adapter.py connect: `async with connect(open_timeout=10)`).
		const reachable = await this.probeReachability();
		if (!reachable) {
			this.logger?.error?.(`SimpleX: cannot reach daemon at ${this.wsUrl}`);
			return false;
		}
		if (!this.running) {
			this.running = true;
			this.lastWsActivityMs = this.nowFn();
			this.listenerTask = this.wsListener();
			this.healthTask = this.healthMonitor();
			this.keepaliveTask = this.wsKeepaliveLoop();
		}
		return true;
	}

	override async disconnect(): Promise<void> {
		this.running = false;
		try {
			this.conn?.close(1000, "adapter shutting down");
		} catch {
			/* best-effort teardown */
		}
		this.conn = null;
		// Flush any in-flight ladder/batch waits so both loops observe the stop
		// flag promptly instead of hanging on an un-advanced clock.
		for (const wake of [...this.pendingSleepWakes]) wake();
		this.pendingSleepWakes.clear();
		// Cancel pending text-batch flush timers + buffers.
		for (const state of this.textBatchFlushes.values()) {
			state.cancelled = true;
			state.wake?.();
		}
		this.textBatchFlushes.clear();
		this.textBatches.clear();
		// Cancel pending command futures (CancelledError parity → null).
		for (const entry of this.pendingResponses.values()) entry.resolve(null);
		this.pendingResponses.clear();
		for (const task of [
			this.listenerTask,
			this.healthTask,
			this.keepaliveTask,
		]) {
			await task?.catch(() => undefined);
		}
		this.listenerTask = null;
		this.healthTask = null;
		this.keepaliveTask = null;
	}

	get isRunning(): boolean {
		return this.running;
	}

	private probeReachability(): Promise<boolean> {
		return new Promise<boolean>((resolve) => {
			let settled = false;
			const done = (v: boolean): void => {
				if (settled) return;
				settled = true;
				resolve(v);
			};
			const conn = this.transport.connect({
				onOpen: () => {
					done(true);
					try {
						conn.close(1000, "reachability probe");
					} catch {
						/* already gone */
					}
				},
				onClose: () => done(false),
				onError: () => done(false),
				onText: () => undefined,
			});
			// open_timeout=10 parity (adapter.py:connect): a daemon accepting
			// TCP but STALLING the handshake must not hang connect() forever —
			// expiry resolves FALSE and abandons the probe socket.
			void this.interruptibleSleep(SIMPLEX_CONNECT_OPEN_TIMEOUT_MS).then(() => {
				if (settled) return;
				done(false);
				try {
					conn.close(1000, "reachability probe timed out");
				} catch {
					/* already gone */
				}
			});
		});
	}

	/** Injected-clock sleep that disconnect()/cancel can interrupt. */
	private interruptibleSleep(
		ms: number,
		state?: BatchFlushState | undefined,
	): Promise<void> {
		return new Promise<void>((resolve) => {
			let settled = false;
			const settle = () => {
				if (settled) return;
				settled = true;
				this.pendingSleepWakes.delete(settle);
				resolve();
			};
			this.pendingSleepWakes.add(settle);
			if (state !== undefined) {
				state.wake = () => {
					state.cancelled = true;
					settle();
				};
			}
			void this.sleepFn(ms).then(() => settle());
		});
	}

	// ── WS listener (adapter.py:_ws_listener parity) ─────────────────────────

	private async wsListener(): Promise<void> {
		let backoffMs = WS_RETRY_DELAY_INITIAL_MS;
		while (this.running) {
			const result = await this.connectAndPump();
			if (result.connected) {
				backoffMs = WS_RETRY_DELAY_INITIAL_MS; // RESET on successful connect
				this.reconnectResets.push({ atMs: this.nowFn() });
			}
			if (!this.running) break;
			// ConnectionClosed vs unexpected error BOTH feed the SAME ladder
			// while running (_ws_listener except legs share the sleep+double).
			const jitter = backoffMs * WS_JITTER_FRACTION * this.rngFn();
			const delayMs = backoffMs + jitter;
			this.reconnectLog.push({
				delayMs,
				jittered: jitter > 0,
				trigger: result.trigger,
			});
			await this.interruptibleSleep(delayMs);
			backoffMs = Math.min(backoffMs * 2, WS_RETRY_DELAY_MAX_MS);
		}
	}

	/** One connect+pump generation: resolves when the connection ENDS. */
	private connectAndPump(): Promise<{
		connected: boolean;
		trigger: "connection-closed" | "connect-failed";
	}> {
		return new Promise((resolve) => {
			let opened = false;
			let settled = false;
			let socket: SimplexConnection | null = null;
			const finish = (
				trigger: "connection-closed" | "connect-failed",
			): void => {
				if (settled) return;
				settled = true;
				this.conn = null;
				resolve({ connected: opened, trigger });
			};
			socket = this.transport.connect({
				onOpen: () => {
					opened = true;
					this.conn = socket;
					this.awaitingPongAtMs = null; // fresh link: keepalive resets
					this.lastWsActivityMs = this.nowFn();
				},
				onText: (text) => {
					this.lastWsActivityMs = this.nowFn();
					void this.handleRawText(text);
				},
				onPong: () => {
					// Protocol pong: answers the keepalive ping. Deliberately does
					// NOT touch lastWsActivityMs — the health monitor measures
					// APPLICATION silence, and pongs would mask it.
					this.awaitingPongAtMs = null;
					this.counts.pongsReceived += 1;
				},
				onClose: (info: SimplexCloseInfo) => {
					this.closeEvents.push({
						atMs: this.nowFn(),
						reason: `${info.code}:${info.reason}`,
					});
					finish(opened ? "connection-closed" : "connect-failed");
				},
				onError: () => finish(opened ? "connection-closed" : "connect-failed"),
			});
		});
	}

	// ── keepalive (adapter.py:_ws_listener ping_interval=20/ping_timeout=20) ──

	/**
	 * Client protocol keepalive — the carrier Hermes got for free from the
	 * websockets library. Every PING_INTERVAL a ping rides the socket UNLESS
	 * the previous one is still unanswered past PING_TIMEOUT, in which case
	 * the link is DEAD: abort it (1011) so onClose feeds the SAME reconnect
	 * ladder. Factories whose connections cannot ping are skipped silently.
	 */
	private async wsKeepaliveLoop(): Promise<void> {
		while (this.running) {
			await this.interruptibleSleep(SIMPLEX_WS_PING_INTERVAL_MS);
			if (!this.running) break;
			const conn = this.conn;
			if (conn === null || conn.ping === undefined) continue;
			const now = this.nowFn();
			if (
				this.awaitingPongAtMs !== null &&
				now - this.awaitingPongAtMs >= SIMPLEX_WS_PING_TIMEOUT_MS
			) {
				this.counts.pingTimeouts += 1;
				this.logger?.warn?.("SimpleX: WS ping timeout — closing stale link");
				this.awaitingPongAtMs = null;
				try {
					conn.close(SIMPLEX_WS_PING_CLOSE_CODE, "ping timeout");
				} catch {
					/* already gone */
				}
				continue;
			}
			this.awaitingPongAtMs = now;
			this.counts.pingsSent += 1;
			try {
				conn.ping();
			} catch (err) {
				this.logger?.warn?.(
					`SimpleX: WS ping failed: ${err instanceof Error ? err.message : String(err)}`,
				);
			}
		}
	}

	// ── health monitor (adapter.py:_health_monitor parity — LOG-ONLY) ───────

	private async healthMonitor(): Promise<void> {
		while (this.running) {
			await this.interruptibleSleep(HEALTH_CHECK_INTERVAL_MS);
			if (!this.running) break;
			const elapsed = this.nowFn() - this.lastWsActivityMs;
			if (elapsed > HEALTH_CHECK_STALE_THRESHOLD_MS) {
				// DELIBERATELY LOG-ONLY: liveness is carried by the seam-level
				// ping/pong keepalive above; treating application silence as
				// staleness causes needless reconnect churn.
				this.idleLogs.push({ atMs: this.nowFn(), elapsedMs: elapsed });
				this.logger?.debug?.(
					`SimpleX: WS application-idle for ${Math.floor(elapsed / 1000)}s`,
				);
			}
		}
	}

	// ── inbound frames (adapter.py:_handle_event parity) ─────────────────────

	private async handleRawText(text: string): Promise<void> {
		let parsed: unknown;
		try {
			parsed = JSON.parse(text);
		} catch {
			this.logger?.debug?.(`SimpleX WS: invalid JSON: ${text.slice(0, 100)}`);
			return;
		}
		if (
			parsed !== null &&
			typeof parsed === "object" &&
			!Array.isArray(parsed)
		) {
			try {
				await this.handleEvent(parsed as Record<string, unknown>);
			} catch {
				// Contained per event: handler errors never kill the listener.
				this.logger?.error?.("SimpleX WS: error handling event");
			}
		}
	}

	/**
	 * Dispatch one daemon event. Normalizes BOTH wire forms — {"corrId","resp"}
	 * envelopes and older top-level payloads — before routing
	 * (adapter.py:_handle_event step 1).
	 */
	async handleEvent(event: Record<string, unknown>): Promise<void> {
		this.counts.events += 1;
		const respRaw = event["resp"];
		const resp =
			respRaw !== null && typeof respRaw === "object" && !Array.isArray(respRaw)
				? (respRaw as Record<string, unknown>)
				: event;
		const corrIdValue = event["corrId"];
		const corrId =
			corrIdValue === null || corrIdValue === undefined
				? null
				: String(corrIdValue);

		// Correlated responses — replies to our OWN commands resolve futures.
		if (corrId !== null) {
			const pending = this.pendingResponses.get(corrId);
			if (pending !== undefined) {
				this.pendingResponses.delete(corrId);
				this.counts.correlatedResponses += 1;
				pending.resolve(resp);
				return;
			}
		}
		// Cosmetic echo filter: prefixed corrIds are ours but didn't make it
		// into _pending_responses (fire-and-forget) — discarded WITHOUT any
		// chat-item processing (adapter.py:@386-390).
		if (corrId !== null && corrId.startsWith(SIMPLEX_CORR_PREFIX)) {
			this.pendingCorrIds.delete(corrId);
			this.counts.echoesDiscarded += 1;
			return;
		}

		const respType = stringOf(resp["type"]) || stringOf(event["type"]);

		// Auto-accept contact requests (@392-407).
		if (respType === "contactRequest" && this.autoAccept) {
			const contactReq = asRec(resp["contactRequest"]);
			const contactRequestId = contactReq["contactRequestId"];
			if (contactRequestId !== null && contactRequestId !== undefined) {
				this.counts.contactRequestsAutoAccepted += 1;
				await this.sendCommand(`/accept ${String(contactRequestId)}`);
			}
			return;
		}

		// Early XFTP file-descriptor ready (@409-423): fires BEFORE newChatItems
		// for large files/voice — /freceive starts the download immediately.
		if (respType === "rcvFileDescrReady") {
			const rcvFile = asRec(resp["rcvFileTransfer"]);
			const fileId = rcvFile["fileId"];
			if (fileId !== null && fileId !== undefined) {
				this.counts.fileDescriptorsReady += 1;
				await this.sendFireAndForget(`/freceive ${String(fileId)}`);
			}
			return;
		}

		// New messages — newChatItems array (@425-440)…
		if (respType === "newChatItems") {
			const itemsRaw = resp["chatItems"];
			const items = Array.isArray(itemsRaw) ? itemsRaw : [itemsRaw];
			for (const item of items) {
				if (item === null || typeof item !== "object") continue;
				try {
					await this.handleChatItem(item as Record<string, unknown>);
				} catch {
					// Containment parity: one poisoned item never kills the batch.
					this.logger?.error?.("SimpleX: error processing chat item");
				}
			}
			return;
		}

		// …or the singular variant some daemon versions emit (@442-449).
		if (respType === "newChatItem") {
			try {
				await this.handleChatItem(resp);
			} catch {
				this.logger?.error?.("SimpleX: error processing chat item");
			}
			return;
		}

		// File transfer completion — deliver any deferred chat item (@451-475).
		if (respType === "rcvFileComplete") {
			const envelope = asRec(resp["chatItem"]);
			const itemData = asRec(envelope["chatItem"]);
			const fileInfo = asRec(itemData["file"]);
			const fileId = numberOrNull(fileInfo["fileId"]);
			if (fileId !== null && this.pendingFileTransfers.has(fileId)) {
				const pending = this.pendingFileTransfers.get(fileId);
				this.pendingFileTransfers.delete(fileId);
				const filePath = stringOf(asRec(fileInfo["fileSource"])["filePath"]);
				if (pending !== undefined && filePath !== "") {
					// Inject the completed download's filePath into the parked
					// chat item, then run the NORMAL pipeline on it.
					const pendEnvelope = pending;
					const pendData = asRec(pendEnvelope["chatItem"]);
					const pendFile = asRec(pendData["file"]);
					pendFile["fileSource"] = { filePath };
					pendData["file"] = pendFile;
					pendEnvelope["chatItem"] = pendData;
					this.counts.transfersCompleted += 1;
					try {
						await this.handleChatItem(pending);
					} catch {
						this.logger?.error?.(
							"SimpleX: error processing deferred file message",
						);
					}
				}
			}
			return;
		}

		if (respType !== "") this.counts.unhandledEventTypes += 1;
	}

	// ── chat-item pipeline (adapter.py:_handle_chat_item parity) ────────────

	/**
	 * ORDER MATTERS — mirrors the source walk exactly: own-direction drop →
	 * rcvMsgContent gate → text extraction → empty-content drop → chat
	 * identity (direct/group) → group allowlist gate → sender presence →
	 * file/deferral lane → type classification → timestamp → batch-or-dispatch.
	 */
	async handleChatItem(chatItem: Record<string, unknown>): Promise<void> {
		const chatInfo = asRec(chatItem["chatInfo"]);
		const itemData = asRec(chatItem["chatItem"]);

		const chatType = stringOf(chatInfo["type"]);
		const meta = asRec(itemData["meta"]);
		const content = asRec(itemData["content"]);
		const msgContent = asRec(content["msgContent"]);

		// Own messages (we sent them) never become turns (@486-489).
		const directionType = stringOf(asRec(itemData["chatDir"])["type"]);
		if (directionType === "directSnd" || directionType === "groupSnd") {
			this.counts.ownMessagesDropped += 1;
			return;
		}
		// Only RECEIVED messages are processed (@491-493).
		if (stringOf(content["type"]) !== "rcvMsgContent") {
			this.counts.nonRcvContent += 1;
			return;
		}

		// Text content (@495-501).
		const msgTypeStr = stringOf(msgContent["type"]);
		const text =
			msgTypeStr === "text" ||
			msgTypeStr === "file" ||
			msgTypeStr === "image" ||
			msgTypeStr === "voice" ||
			msgTypeStr === "link" ||
			msgTypeStr === "video"
				? stringOf(msgContent["text"])
				: "";
		if (
			text === "" &&
			msgTypeStr !== "image" &&
			msgTypeStr !== "file" &&
			msgTypeStr !== "voice"
		) {
			this.counts.emptyContentDropped += 1;
			return;
		}

		// Sender + chat IDs (@504-552).
		let senderId = "";
		let senderName = "";
		let chatId = "";
		let isGroup = false;
		if (chatType === "direct") {
			const contact = asRec(chatInfo["contact"]);
			senderId =
				stringOf(contact["contactId"]) || String(contact["contactId"] ?? "");
			const profileName = stringOf(asRec(contact["profile"])["displayName"]);
			senderName = stringOf(contact["localDisplayName"]) || profileName;
			chatId = senderId;
		} else if (chatType === "group") {
			const groupInfo = asRec(chatInfo["groupInfo"]);
			const groupId =
				stringOf(groupInfo["groupId"]) || String(groupInfo["groupId"] ?? "");
			chatId = `group:${groupId}`;
			isGroup = true;

			const member = asRec(asRec(itemData["chatDir"])["groupMember"]);
			senderId =
				stringOf(member["memberId"]) || String(member["memberId"] ?? "");
			const memberProfileName = stringOf(
				asRec(member["memberProfile"])["displayName"],
			);
			senderName = stringOf(member["localDisplayName"]) || memberProfileName;

			// Group allowlist (@543-552): EMPTY disables groups entirely (safer
			// default, logged); '*' opens all; otherwise membership decides.
			if (this.groupAllowSet.size > 0) {
				if (!this.groupAllowSet.has("*") && !this.groupAllowSet.has(groupId)) {
					this.counts.groupsNotAllowed += 1;
					return;
				}
			} else {
				this.counts.groupsDisabled += 1;
				this.logger?.debug?.(
					"SimpleX: ignoring group message (no SIMPLEX_GROUP_ALLOWED)",
				);
				return;
			}
		} else {
			this.counts.unhandledChatTypes += 1;
			return;
		}

		if (senderId === "") {
			this.counts.noSender += 1;
			return;
		}

		// File / image / voice attachments. File info lives at
		// chatItem.chatItem.file — sibling of meta/content/chatDir (@558+).
		const mediaUrls: string[] = [];
		const mediaTypes: string[] = [];
		const fileInfoRaw = itemData["file"];
		if (
			fileInfoRaw !== null &&
			typeof fileInfoRaw === "object" &&
			!Array.isArray(fileInfoRaw)
		) {
			const fileInfo = fileInfoRaw as Record<string, unknown>;
			const fileSource = asRec(fileInfo["fileSource"]);
			const filePath = stringOf(fileSource["filePath"]);
			const fileName = stringOf(fileInfo["fileName"]);
			const fileId = fileInfo["fileId"];

			let ext = filePath !== "" ? unixExtname(filePath) : "";
			if (ext === "" && fileName !== "") ext = unixExtname(fileName);

			// Voice notes typically arrive BEFORE the file finishes downloading:
			// defer until rcvFileComplete fires (@579-591).
			if (
				filePath === "" &&
				isAudioExt(ext) &&
				fileId !== null &&
				fileId !== undefined
			) {
				const key = Number(fileId);
				if (Number.isFinite(key)) {
					this.pendingFileTransfers.set(key, chatItem);
					this.counts.transfersDeferred += 1;
					// Fire-and-forget: the daemon never returns a corrId reply for
					// /freceive — awaiting one would block a full command timeout.
					await this.sendFireAndForget(`/freceive ${String(fileId)}`);
				}
				return;
			}

			if (filePath !== "") {
				const classifiedExt =
					unixExtname(filePath) ||
					(fileName !== "" ? unixExtname(fileName) : "");
				if (isImageExt(classifiedExt)) {
					mediaUrls.push(filePath);
					mediaTypes.push(`image/${classifiedExt.replace(/^\./, "")}`);
				} else if (isAudioExt(classifiedExt)) {
					mediaUrls.push(filePath);
					mediaTypes.push(`audio/${classifiedExt.replace(/^\./, "")}`);
				} else {
					mediaUrls.push(filePath);
					mediaTypes.push("application/octet-stream");
				}
			}
		}

		// Chat display name (@600-606).
		let chatName = senderName;
		if (isGroup) {
			const groupInfo = asRec(chatInfo["groupInfo"]);
			chatName =
				stringOf(groupInfo["localDisplayName"]) ||
				stringOf(asRec(groupInfo["groupProfile"])["displayName"]) ||
				chatId;
		}

		// Message type (@608-622): TEXT default; VOICE any audio; PHOTO any
		// image; DOCUMENT otherwise (octet-stream files surface to the agent).
		let messageType: MessageType = "text";
		if (mediaTypes.length > 0) {
			if (mediaTypes.some((mt) => mt.startsWith("audio/")))
				messageType = "voice";
			else if (mediaTypes.some((mt) => mt.startsWith("image/")))
				messageType = "photo";
			else messageType = "document";
		}

		// Timestamp (@624-634): meta.itemTs|createdAt ISO, fallback now.
		// (Python needs the Z→+00:00 dance for pre-3.11 fromisoformat; JS
		// Date.parse accepts both forms natively.)
		const tsRaw = stringOf(meta["itemTs"]) || stringOf(meta["createdAt"]);
		const parsedTs = tsRaw !== "" ? Date.parse(tsRaw) : Number.NaN;
		const timestampIso = Number.isNaN(parsedTs)
			? new Date(this.nowFn()).toISOString()
			: new Date(parsedTs).toISOString();

		const source = {
			platform: "simplex",
			chatType: isGroup ? ("group" as const) : ("dm" as const),
			userId: senderId,
			chatId,
			...(chatName !== "" ? { chatName } : {}),
		};

		this.counts.accepted += 1;
		this.acceptedLog.push({
			chatId,
			chatType: isGroup ? "group" : "dm",
			userId: senderId,
			chatName,
			messageType,
			mediaUrls: [...mediaUrls],
			text,
		});
		const sessionKey = buildSessionKey(
			{
				platform: source.platform,
				chatType: source.chatType,
				...(source.userId ? { userId: source.userId } : {}),
				...(source.chatId ? { chatId: source.chatId } : {}),
				...(source.chatName !== undefined ? { chatName: source.chatName } : {}),
			},
			{},
			undefined,
		);

		const event: IncomingEvent = {
			messageType,
			text,
			...(mediaUrls.length > 0 ? { mediaUrls } : {}),
			...(mediaTypes.length > 0 ? { mediaTypes } : {}),
			metadata: {
				gateway_session_key: sessionKey,
				simplex_timestamp: timestampIso,
			},
			source,
		};

		// Batch consecutive texts so rapid-fire pastes coalesce into ONE event
		// (@648-653); everything else dispatches immediately.
		if (messageType === "text" && text !== "") {
			this.enqueueTextEvent(sessionKey, event);
		} else {
			await this.handleIngress(event, sessionKey);
		}
	}

	// ── text batching (adapter.py:_enqueue_text_event/_flush_text_batch) ────

	/**
	 * Buffer a text event under its platform:chat_id key and RESET the flush
	 * timer: the PRIOR flush task is CANCELLED and replaced, so late arrivals
	 * restart the quiet-period window. Flush timers ride the INJECTED clock.
	 */
	private enqueueTextEvent(_key: string, event: IncomingEvent): void {
		const key = textBatchKey(event);
		const existing = this.textBatches.get(key);
		if (existing === undefined) {
			this.textBatches.set(key, event);
		} else {
			// "\n"-joined append ONLY when the arrival carries text (source:
			// `if event.text:` guard — empty bodies never add dangling newlines).
			if ((event.text ?? "") !== "") {
				existing.text = existing.text
					? `${existing.text}\n${event.text ?? ""}`
					: (event.text ?? "");
			}
			if ((event.mediaUrls?.length ?? 0) > 0) {
				existing.mediaUrls = [
					...(existing.mediaUrls ?? []),
					...(event.mediaUrls ?? []),
				];
				existing.mediaTypes = [
					...(existing.mediaTypes ?? []),
					...(event.mediaTypes ?? []),
				];
			}
		}

		const prior = this.textBatchFlushes.get(key);
		if (prior !== undefined && !prior.cancelled) {
			prior.wake?.(); // cancels + wakes the prior flush task
		}
		const state: BatchFlushState = { cancelled: false, wake: null };
		this.textBatchFlushes.set(key, state);
		void this.flushTextBatch(key, state);
	}

	private async flushTextBatch(
		key: string,
		state: BatchFlushState,
	): Promise<void> {
		try {
			await this.interruptibleSleep(this.textBatchDelayMs, state);
			if (state.cancelled) return; // replaced by a newer arrival
			if (this.textBatchFlushes.get(key) === state) {
				this.textBatchFlushes.delete(key);
			}
			const agg = this.textBatches.get(key);
			if (agg === undefined) return; // empty-batch guard
			this.textBatches.delete(key);
			this.counts.batchesFlushed += 1;
			const sessionKey = String(agg.metadata?.["gateway_session_key"] ?? key);
			await this.handleIngress(agg, sessionKey);
		} catch {
			/* containment parity: a poisoned flush never kills batching */
		}
	}

	// ── corr machinery (adapter.py:_make_corr_id/_send_command parity) ──────

	/**
	 * Mint a correlation id and remember it for echo-filtering. EVERY minted
	 * id enters the bounded pending set — past `_max_pending_corr` the OLDEST
	 * entries evict in a SINGLE sweep (insertion-order iteration).
	 */
	makeCorrId(): string {
		this.corrCounter += 1;
		const corrId = `${SIMPLEX_CORR_PREFIX}${this.corrCounter}-${this.nowFn()}`;
		this.pendingCorrIds.add(corrId);
		if (this.pendingCorrIds.size > this.maxPendingCorr) {
			const overflow = this.pendingCorrIds.size - this.maxPendingCorr;
			let removed = 0;
			for (const existing of this.pendingCorrIds) {
				if (removed >= overflow) break;
				this.pendingCorrIds.delete(existing);
				removed += 1;
			}
		}
		return corrId;
	}

	pendingCorrSnapshot(): readonly string[] {
		return [...this.pendingCorrIds];
	}

	pendingResponseCount(): number {
		return this.pendingResponses.size;
	}

	pendingTransferCount(): number {
		return this.pendingFileTransfers.size;
	}

	/** Raw JSON payload write; DROPPED-WRITE tolerated when disconnected. */
	private async sendWsJson(payload: Record<string, unknown>): Promise<void> {
		const conn = this.conn;
		if (conn === null) {
			this.counts.droppedWrites += 1;
			this.logger?.debug?.("SimpleX: WS send dropped (not connected)");
			return;
		}
		try {
			conn.send(JSON.stringify(payload));
		} catch (err) {
			this.counts.droppedWrites += 1;
			this.logger?.warn?.(
				`SimpleX: WS send error: ${err instanceof Error ? err.message : String(err)}`,
			);
		}
	}

	/**
	 * Send a command and AWAIT the correlated response (30s timeout, popped on
	 * expiry; a LATE reply then lands in the echo filter instead).
	 */
	async sendCommand(
		command: string,
		timeoutMs: number = SIMPLEX_COMMAND_TIMEOUT_MS,
	): Promise<Record<string, unknown> | null> {
		const conn = this.conn;
		if (conn === null) {
			this.logger?.warn?.("SimpleX: command sent but WebSocket not connected");
			return null;
		}
		const corrId = this.makeCorrId();
		const deferred = this.createPendingResponse();
		this.pendingResponses.set(corrId, deferred);
		try {
			conn.send(JSON.stringify({ corrId, cmd: command }));
		} catch (err) {
			this.counts.droppedWrites += 1;
			this.pendingResponses.delete(corrId);
			this.logger?.warn?.(
				`SimpleX: command failed: ${command.slice(0, 50)} — ${err instanceof Error ? err.message : String(err)}`,
			);
			return null;
		}
		const raced = await Promise.race([
			deferred.promise.then((value) => ({ kind: "response" as const, value })),
			this.sleepFn(timeoutMs).then(() => ({ kind: "timeout" as const })),
		]);
		if (raced.kind === "timeout") {
			this.counts.commandTimeouts += 1;
			this.pendingResponses.delete(corrId);
			this.logger?.warn?.(
				`SimpleX: command timed out: ${command.slice(0, 50)}`,
			);
			return null;
		}
		return raced.value;
	}

	private createPendingResponse(): PendingResponse {
		// JS promises ignore repeat resolve() calls — the settled flag stays
		// descriptive (timeout vs response racing is decided by Promise.race).
		let resolve!: (value: Record<string, unknown> | null) => void;
		const promise = new Promise<Record<string, unknown> | null>((res) => {
			resolve = res;
		});
		return { promise, resolve, settled: false };
	}

	/**
	 * Send a command WITHOUT waiting for a correlated response — for commands
	 * the daemon never acks, e.g. /freceive (adapter.py:_send_fire_and_forget).
	 */
	async sendFireAndForget(command: string): Promise<void> {
		const corrId = this.makeCorrId();
		await this.sendWsJson({ corrId, cmd: command });
	}

	// ── channel directory (adapter.py:list_channels parity) ───────────────

	/**
	 * Enumerate contacts and allowed groups for the channel directory.
	 * Issues correlated '/contacts' then '/groups' (10s timeouts each) over
	 * the live socket. Returns NULL (never []) when the WebSocket is down or
	 * the daemon unresponsive so the directory falls back to session-history
	 * discovery instead of wiping previously known targets. Entry ids match
	 * the send-target formats the adapter accepts: display name for DMs,
	 * 'group:<groupId>' for groups.
	 */
	async listChannels(): Promise<SimplexChannelEntry[] | null> {
		if (this.conn === null) return null;
		const channels: SimplexChannelEntry[] = [];

		const contactsResp = await this.sendCommand(
			"/contacts",
			SIMPLEX_LIST_CHANNELS_COMMAND_TIMEOUT_MS,
		);
		if (contactsResp === null) {
			// Daemon unresponsive — keep whatever the directory already has.
			return null;
		}
		for (const contact of arrayOrEmpty(contactsResp["contacts"])) {
			if (!isRecord(contact)) continue;
			const contactId = contact["contactId"];
			const name =
				stringOf(contact["localDisplayName"]) ||
				stringOf(asRec(contact["profile"])["displayName"]);
			if ((contactId === null || contactId === undefined) && name === "") {
				continue;
			}
			// Display name is what the DM send path actually addresses; fall
			// back to the numeric contactId.
			const label = name !== "" ? name : String(contactId);
			channels.push({ id: label, name: label, type: "dm" });
		}

		const groupsResp = await this.sendCommand(
			"/groups",
			SIMPLEX_LIST_CHANNELS_COMMAND_TIMEOUT_MS,
		);
		if (groupsResp !== null) {
			for (const group of arrayOrEmpty(groupsResp["groups"])) {
				// The daemon returns each group as either a groupInfo dict or a
				// [groupInfo, groupSummary] pair depending on version.
				const info = Array.isArray(group) ? (group[0] ?? null) : group;
				if (!isRecord(info)) continue;
				const groupId = info["groupId"];
				if (groupId === null || groupId === undefined) continue;
				const name =
					stringOf(info["localDisplayName"]) ||
					stringOf(asRec(info["groupProfile"])["displayName"]) ||
					String(groupId);
				channels.push({ id: `group:${String(groupId)}`, name, type: "group" });
			}
		}

		return channels;
	}

	// ── egress doors ─────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * DOOR transport — text egress (adapter.py:send parity). MEDIA:<path>
	 * attachment tags are extracted + stripped; the remaining body composes
	 * the structured json command addressed by ID — groups "/_send #<gid>",
	 * DMs "/_send @<cid>" (the bracket chat-command syntax parses as a
	 * display-name lookup which silently drops unresolved names; the bare
	 * "@id text" form is unreliable for the same reason — the ID-addressed
	 * json form avoids both). FIRE-AND-FORGET at the WS level: the daemon
	 * returns no reliable corrId reply for chat commands, and awaiting one
	 * would serialize ALL egress behind the 30-second timeout. Media paths
	 * route voice-vs-document with PER-MEDIA failure short-circuit.
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		// Fresh regex literals per call — no shared /g lastIndex state across
		// concurrent sends.
		const mediaPaths = [...content.matchAll(/MEDIA:(\S+)/g)]
			.map((m) => m[1] ?? "")
			.filter((p) => p !== "");
		const body = content.replace(/MEDIA:\S+/g, "").trim();

		if (body !== "") {
			// json.dumps escapes newlines + special chars correctly; the composed
			// array carries exactly ONE text msgContent.
			const composed = JSON.stringify([
				{ msgContent: { type: "text", text: body } },
			]);
			const cmdStr = addressCommand(chatId, composed);
			// Fire-and-forget at WS level; a dropped write while disconnected is
			// TOLERATED — the listener owns reconnection.
			await this.sendFireAndForget(cmdStr);
		}

		for (const path of mediaPaths) {
			const ext = extname(path).toLowerCase();
			const result = SIMPLEX_VOICE_EXTS.has(ext)
				? await this.sendVoice(chatId, path)
				: await this.sendDocument(chatId, path);
			if (!result.success) return result; // short-circuit THAT media's verdict
		}

		if (this.captureWire !== undefined) {
			return this.captureWire.transmitSend(chatId, body, metadata);
		}
		return { success: true };
	}

	/** Rich lane ABSENT on this wire: capability-error shape, no roundtrip. */
	protected override async wireRich(
		content: string,
		_metadata: Metadata,
	): Promise<SendResult> {
		if (
			this.captureWire === undefined ||
			!this.captureWire.hasRichScript("rich")
		) {
			return { success: false, error: "method not found" };
		}
		return this.captureWire.transmitRich("__rich__", content);
	}

	// ── outbound media (adapter.py:send_voice/send_document parity) ─────────

	/**
	 * Voice note: msgContent.type "voice" plays INLINE (a plain file
	 * attachment would render as a downloadable document instead). Uses the
	 * STRUCTURED correlated-command path (awaited reply parity).
	 */
	async sendVoice(
		chatId: string,
		audioPath: string,
		caption = "",
		duration = 0,
	): Promise<SendResult> {
		if (!existsSync(audioPath)) {
			return { success: false, error: "Voice file not found" };
		}
		const composed = JSON.stringify([
			{
				msgContent: { type: "voice", text: caption, duration },
				fileSource: { filePath: audioPath },
			},
		]);
		const result = await this.sendCommand(addressCommand(chatId, composed));
		if (result !== null) return { success: true };
		return { success: false, error: "Failed to send voice message" };
	}

	/** Document/file attachment (msgContent.type "file"). */
	async sendDocument(
		chatId: string,
		filePath: string,
		caption = "",
	): Promise<SendResult> {
		if (!existsSync(filePath)) {
			return { success: false, error: "File not found" };
		}
		const composed = JSON.stringify([
			{
				filePath,
				msgContent: { type: "file", text: caption },
			},
		]);
		const result = await this.sendCommand(addressCommand(chatId, composed));
		if (result !== null) return { success: true };
		return { success: false, error: "Failed to send document" };
	}

	// ── outbound images/video (adapter.py:send_image family parity) ──────

	/**
	 * Send an image natively (adapter.py:send_image). Supports 'file://' URLs
	 * and http(s) URLs (downloaded through THE cache seam first). The file is
	 * PNG-prepared via _prepare_image parity, then shipped as the correlated
	 * '/_send @id|#gid json [{filePath, msgContent:{type:"image",
	 * image:<thumb-data-uri>, text}}]' command — /_send addresses by ID; '/f'
	 * only accepts display names which breaks for group IDs.
	 */
	async sendImage(
		chatId: string,
		imageUrl: string,
		caption = "",
	): Promise<SendResult> {
		let filePath: string;
		if (imageUrl.startsWith("file://")) {
			try {
				filePath = decodeURIComponent(imageUrl.slice("file://".length));
			} catch {
				filePath = imageUrl.slice("file://".length); // raw fallback
			}
		} else {
			try {
				filePath = await this.cacheImageFromUrl(imageUrl);
			} catch (err) {
				const message = err instanceof Error ? err.message : String(err);
				this.logger?.warn?.(`SimpleX: failed to download image: ${message}`);
				return { success: false, error: message };
			}
		}

		if (filePath === "" || !existsSync(filePath)) {
			return { success: false, error: "Image file not found" };
		}

		const prepared = await this.prepareImage(filePath);
		const composed = JSON.stringify([
			{
				filePath: prepared.pngPath,
				msgContent: {
					type: "image",
					image: prepared.thumbDataUri,
					text: caption,
				},
			},
		]);
		const result = await this.sendCommand(addressCommand(chatId, composed));
		if (result !== null) return { success: true };
		return { success: false, error: "Failed to send image" };
	}

	/** Send a local image file via SimpleX (adapter.py:send_image_file). */
	async sendImageFile(
		chatId: string,
		imagePath: string,
		caption = "",
	): Promise<SendResult> {
		return this.sendImage(chatId, `file://${imagePath}`, caption);
	}

	/** Video ships as a plain file attachment (adapter.py:send_video). */
	async sendVideo(
		chatId: string,
		videoPath: string,
		caption = "",
	): Promise<SendResult> {
		return this.sendDocument(chatId, videoPath, caption);
	}

	/**
	 * Image batch for the post-stream rescan lane — every entry gets its own
	 * native image send (base.py:send_multiple_images default shape).
	 */
	async sendMultipleImages(
		chatId: string,
		images: readonly string[],
	): Promise<SendResult[]> {
		const results: SendResult[] = [];
		for (const image of images) {
			results.push(await this.sendImageFile(chatId, image));
		}
		return results;
	}

	/** _prepare_image dispatch: injected encoder or the fallback posture. */
	private prepareImage(filePath: string): Promise<SimplexPreparedImage> {
		if (this.imagePreparer !== undefined) return this.imagePreparer(filePath);
		// No converter wired: adapter.py:_prepare_image fallback posture —
		// keep the original bytes and ship WITHOUT an inline thumbnail
		// (conversion unavailable is logged, never thrown).
		this.logger?.debug?.(`SimpleX: image conversion unavailable: ${filePath}`);
		return Promise.resolve({ pngPath: filePath, thumbDataUri: "" });
	}

	/** cache_image_from_url dispatch: injected fetcher or default download. */
	private cacheImageFromUrl(url: string): Promise<string> {
		if (this.imageUrlFetcher !== undefined) return this.imageUrlFetcher(url);
		return defaultCacheImageFromUrl(url);
	}

	/**
	 * adapter.py:send_typing — SimpleX has NO typing-indicator API: a
	 * deliberate no-op kept as a named surface for parity visibility.
	 */
	async sendTypingIndicator(_chatId: string): Promise<void> {
		/* deliberate no-op */
	}

	// ── multi-chat-safe delivery pipeline (signal-precedent override) ───────

	/**
	 * ONE session-scoped formatting ladder whose door closures bind the
	 * CURRENT chatId dynamically (base's lazy ladder captures the FIRST
	 * call's chatId — a latent mis-addressing for multi-chat sessions).
	 * Single-path platform: converted and plain tiers hit the SAME verbatim
	 * lane (no dialect conversion exists on this wire).
	 */
	private sessionLadder: FormattingLadder | null = null;
	private activeChatId = "";

	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		const policy = this.chatLengthPolicyForChat(chatId);
		const plan: ChunkPlan =
			this.splitsLongMessages || policy.lenFn(content) <= policy.maxUnits
				? {
						chunks: [content],
						chunkCount: 1,
						scaffold: [{ prefixLen: 0, closeAdded: false, labelJoinLen: 0 }],
					}
				: chunkWithFenceCarry(content, policy);

		if (this.sessionLadder === null) {
			this.sessionLadder = new FormattingLadder(
				{
					tryRich: (c, md) => this.wireRich(c, md),
					sendConverted: (c, md) => this.wireSend(this.activeChatId, c, md),
					sendPlain: (c, md) => this.wireSend(this.activeChatId, c, md),
				},
				{
					log: this.logger?.warn
						? (m, meta) => this.logger?.warn?.(m, meta)
						: undefined,
				},
			);
		}
		const ladder = this.sessionLadder;

		const results: SendResult[] = [];
		for (const chunk of plan.chunks) {
			this.activeChatId = chatId; // per-chunk door target binding
			results.push(await this.deliverChunkOn(ladder, chunk, metadata));
		}
		return results;
	}

	/** Base deliverChunk parity over the SHARED session ladder. */
	private async deliverChunkOn(
		ladder: FormattingLadder,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const outcome = await ladder.sendText(chunk, metadata);
		if (outcome.success) return outcome;
		if (outcome.tier === "rich") return outcome; // transient rich: never resent

		const failureClass = classifySendError(new Error(outcome.error ?? ""));
		const networkClassified =
			outcome.retryable === true ||
			failureClass === "connect-timeout" ||
			failureClass === "network" ||
			failureClass === "flood";
		if (networkClassified) {
			const retried = await sendWithRetry(
				chunk,
				metadata,
				(c, md) => this.wireSend(this.activeChatId, c, md),
				{ maxRetries: 2 },
			);
			if (retried.success) return retried;
			return this.wireSend(this.activeChatId, DELIVERY_FAILED_NOTICE, metadata);
		}

		// Formatting-classified final failure → §6.1 plain-text fallback lane
		// (original chunk bytes ride the fallback body; the capture bridge
		// accepts that envelope without re-failing).
		if (failureClass === "formatting") {
			return this.wireSend(
				this.activeChatId,
				plainTextFallbackBody(chunk),
				metadata,
			);
		}
		return outcome;
	}

	// ── relay-shaped descriptor seeding (§6.3/A15 conformance lane) ──────────

	protected override chatDescriptorFor(chatId: string):
		| {
				maxMessageLength?: number | undefined;
				lenUnit?: LengthUnit | undefined;
		  }
		| undefined {
		if (chatId.includes("utf16")) {
			return { maxMessageLength: 30, lenUnit: "utf16" };
		}
		return undefined;
	}

	// ── guard wiring (reference-fixture inheritance; copied verbatim) ────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: SIMPLEX_REGISTRY,
				messageHandler: async (event, ctx) => {
					const text = event.text ?? `[${String(event.messageType)}]`;
					const sessionKey = String(
						event.metadata?.["gateway_session_key"] ?? "",
					);
					if (this.clarifyArmedSet.has(sessionKey) && !text.startsWith("/")) {
						this.clarifyCaptures.push(text);
						return null;
					}
					this.turnLog.push(text);
					const isInlineDispatch =
						text.startsWith("/") ||
						(ctx.task.cancelRequested() === false && ctx.task.isDone());
					if (!isInlineDispatch) {
						while (this.holding && !ctx.task.cancelRequested()) {
							await Promise.race([
								this.holdGate.then(() => undefined),
								new Promise<void>((r) => setTimeout(r, 1)),
							]);
						}
					}
					ctx.throwIfCancelled();
					const reply = `reply:${text}`;
					this.replyLog.push(reply);
					return reply;
				},
				sendReply: async (_chatId, text) => {
					this.replyLog.push(text);
				},
			},
			{
				...(spawner !== undefined ? { spawner } : {}),
				hasPendingClarify: (key) => this.clarifyArmedSet.has(key),
			},
		);
	}

	get clarifyArmed(): Set<string> {
		return this.clarifyArmedSet;
	}

	setClarifyIntercept(sessionKey: string, on: boolean): void {
		if (on) this.clarifyArmedSet.add(sessionKey);
		else this.clarifyArmedSet.delete(sessionKey);
	}

	holdTurns(on: boolean): void {
		if (on && !this.holding) {
			this.holdGate = new Promise<void>((resolve) => {
				this.releaseHold = resolve;
			});
		}
		this.holding = on;
		if (!on) this.releaseHold();
	}

	async deliverInbound(
		event: IncomingEvent,
		sessionKey: string,
	): Promise<void> {
		// Self/echo filter parity (shared row contract): bot-authored echoes
		// never become turns.
		if (String(event.source?.userId ?? "") === "bot-self") return;
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

/** One channel-directory entry (adapter.py:list_channels row shape). */
export interface SimplexChannelEntry {
	id: string;
	name: string;
	type: "dm" | "group";
}

function isRecord(v: unknown): v is Record<string, unknown> {
	return v !== null && typeof v === "object" && !Array.isArray(v);
}

/** Python `resp.get("x") or []` parity — null/undefined/non-array ⇒ []. */
function arrayOrEmpty(v: unknown): unknown[] {
	return Array.isArray(v) ? v : [];
}

/**
 * Default cache_image_from_url posture: download via global fetch (30s
 * timeout, redirect-following) into a fresh temp dir. Extension guessed
 * from the URL pathname, '.jpg' fallback (base.py:883 default ext).
 */
async function defaultCacheImageFromUrl(url: string): Promise<string> {
	const res = await fetch(url, {
		signal: AbortSignal.timeout(30_000),
		redirect: "follow",
		headers: { Accept: "image/*,*/*;q=0.8" },
	});
	if (!res.ok) throw new Error(`HTTP ${res.status} for ${url}`);
	const bytes = new Uint8Array(await res.arrayBuffer());
	let ext = extname(new URL(url).pathname).toLowerCase();
	if (!/^\.(png|jpe?g|gif|webp)$/.test(ext)) ext = ".jpg";
	const dir = await mkdtemp(join(tmpdir(), "simplex-img-"));
	const path = join(dir, `image${ext}`);
	await writeFile(path, bytes);
	return path;
}

function asRec(v: unknown): Record<string, unknown> {
	return v !== null && typeof v === "object" && !Array.isArray(v)
		? (v as Record<string, unknown>)
		: {};
}

function stringOf(v: unknown): string {
	return typeof v === "string"
		? v.trim()
		: v === undefined
			? ""
			: String(v).trim();
}

function numberOrNull(v: unknown): number | null {
	if (v === null || v === undefined) return null;
	const n = Number(v);
	return Number.isFinite(n) ? n : null;
}

/** Python pathlib-style suffix (lowercased): ".ogg"; dotfiles yield "". */
function unixExtname(pathLike: string): string {
	const ext = extname(pathLike);
	return ext.toLowerCase();
}

/**
 * Structured /_send addressing: "group:"-prefixed ids use "#<gid>"; bare ids
 * use "@<cid>". Comment WHY (adapter.py:send docstring): the bracket
 * chat-command syntax ("#[<id>] text") is parsed by the daemon as a
 * display-name lookup which silently drops unresolved names, and the bare
 * "@<id> text" syntax is unreliable for DMs for the same reason — the
 * ID-addressed json form avoids both.
 */
function addressCommand(chatId: string, composed: string): string {
	return chatId.startsWith("group:")
		? `/_send #${chatId.slice(6)} json ${composed}`
		: `/_send @${chatId} json ${composed}`;
}

/** Session-scoped text-batch key: platform:chat_id (adapter.py parity). */
function textBatchKey(event: IncomingEvent): string {
	return `${String(event.source?.platform ?? "simplex")}:${String(event.source?.chatId ?? "")}`;
}
