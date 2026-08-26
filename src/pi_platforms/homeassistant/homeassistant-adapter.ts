// pi_platforms/homeassistant/homeassistant-adapter — THE Home Assistant
// adapter, ported from the READ-ONLY Hermes plugin
// plugins/platforms/homeassistant/adapter.py onto the kit base. Everything
// policy-shaped is inherited or manifest DATA; this module supplies
// TRANSPORT (HA WebSocket API inbound state_changed events + outbound REST
// persistent_notification/create) and the event pipeline.
//
// Shape (DEC-002 ws family — persistent-push inbound stream):
//   - connect(): handshake ladder against /api/websocket (auth_required →
//     auth → auth_ok → subscribe_events(state_changed) + result-success ack);
//     failure at ANY step ⇒ cleanup + false (_ws_connect parity)
//   - listen loop: frames of type "event" route into the pipeline; malformed
//     JSON is debug-dropped; CLOSED/ERROR ends the read loop ⇒ backoff-ladder
//     sleep([5,10,30,60][min(idx,last)]) then reconnect; successful reconnect
//     RESETS the ladder index (_listen_loop parity) — sleeps ride the
//     INJECTED clock
//   - heartbeat: aiohttp ws_connect(heartbeat=30) parity — app-level ping/
//     pong every 30s; a stalled pong forces reconnect (watchdog row)
//   - event pipeline ORDER MATTERS (_handle_ha_event parity): entity_id
//     required → ignore_entities → domain/entity watch disjunction (closed
//     by default) → per-entity cooldown (injected clock) → format → channel
//     dispatch on ha_events
//   - send(): REST POST persistent_notification/create with Bearer token;
//     ONE POST per delivery with message truncated at 4096 (no chunk lane —
//     splitsLongMessages=false); title "Hermes Agent" (verbatim vendor wire
//     data — see manifest proposed-DEC note); ≥300 ⇒ "HTTP {status}: {body}"
//   - send_typing: deliberate NO-OP ("No typing indicator for Home
//     Assistant") — absence datum, rowed in the conformance suite
//
// Layering: imports pi_gateway downward + kit same-layer ONLY; no adapter
// cross-imports (clock types declared locally; ManualClock satisfies them
// structurally).

import { randomBytes } from "node:crypto";

import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	classifySendError,
	DELIVERY_FAILED_NOTICE,
	FormattingLadder,
	OneShotPendingStore,
	plainTextFallbackBody,
	resolveEnablement,
	sendWithRetry,
} from "../kit/index.js";
import type {
	Metadata,
	SendResult,
	StreamLogger,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	MessageHandler,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type {
	AdapterStatusSnapshot,
	DisableReason,
} from "../kit/lifecycle-state.js";

import {
	formatStateChange,
	HA_BACKOFF_STEPS_SECONDS,
	haBackoffStepSeconds,
	HA_CAPABILITIES,
	HA_DEFAULT_COOLDOWN_SECONDS,
	HA_DEFAULT_URL,
	HA_EVENTS_CHAT_ID,
	HA_EVENTS_USER_ID,
	HA_MAX_MESSAGE_LENGTH,
	HA_NOTIFICATION_TITLE,
	HA_PLUGIN_MANIFEST,
	HA_REST_NOTIFICATION_CREATE,
	HA_SUPPORTS_MESSAGE_EDITING,
	HA_WS_HEARTBEAT_MS,
	type HaEntityState,
	type HaWatchConfig,
} from "./manifest.js";
import {
	HA_WS_CLOSED,
	HA_WS_OPEN,
	type HaClientSocket,
	type HaCloseInfo,
	type HaConnectionFactory,
	type HaSocketListener,
} from "./ha-fake-server.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const HA_REGISTRY: CommandRegistry = [
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

// ── injected timing seam (structurally satisfied by ManualClock) ─────────────

export type HaNowFn = () => number;
export type HaSleepFn = (ms: number) => Promise<void>;

export interface HaClock {
	nowMs: HaNowFn;
	sleepMs: HaSleepFn;
}

// ── REST session seam (the dedicated send plane; adapter.py:_rest_session) ──

/**
 * Outcome of one notification POST, mapped by the injected session:
 *   ok                — HTTP < 300
 *   http              — any status (engine constructs the vendor error text)
 *   timeout           — timeout-classified transport death (mapped honestly)
 *   transport-failure — scripted/transport error surfaced verbatim
 */
export type HaRestOutcome =
	| { kind: "ok" }
	| { kind: "http"; status: number; body: string }
	| { kind: "timeout" }
	| { kind: "transport-failure"; error: string };

export interface HaRestRequest {
	/** Absolute URL (default world: {hass_url}/api/services/persistent_notification/create). */
	path: string;
	payload: { title: string; message: string };
	headers: Record<string, string>;
}

export interface HaRestSession {
	post(
		req: HaRestRequest,
		chatId: string,
		metadata: Metadata,
	): Promise<HaRestOutcome>;
	/** Rich-probe passthrough (§10.1 latch row) — reference-subject parity. */
	hasRichScript(opKind: string): boolean;
	transmitRich(chatId: string, content: string): Promise<SendResult>;
}

export interface HomeAssistantAdapterOptions {
	/** Event-plane factory (FakeHaServer in tests; a real ws client elsewhere). */
	ws: HaConnectionFactory;
	/** Dedicated REST "session" for sends (adapter.py:_rest_session parity). */
	rest: HaRestSession;
	clock?: HaClock | undefined;
	config?: HaWatchConfig | undefined;
	secretReader?: ScopedSecretReader | undefined;
	scalarMaxUnits?: number | undefined;
	logger?: StreamLogger | undefined;
	spawner?: TaskSpawner | undefined;
	/**
	 * Lie-scan injection point: THE manifest datum that drives the
	 * streaming-exclusion probe (HA_SUPPORTS_MESSAGE_EDITING=false). Only the
	 * lying-mutant fixture overrides it.
	 */
	declaredMessageEditing?: boolean | undefined;
}

export interface HaEventCounters {
	eventsSeen: number;
	malformedDropped: number;
	noEntity: number;
	ignoreFiltered: number;
	watchFiltered: number;
	closedDefaultDropped: number;
	cooldownSkipped: number;
	noChangeSkipped: number;
	accepted: number;
}

export interface HaDispatchedEvent {
	messageId: string;
	text: string;
	chatId: string;
	userId: string;
	chatType: "channel";
}

/**
 * The kit-built Home Assistant adapter. Hermes anchors: plugins/platforms/
 * homeassistant/adapter.py:HomeAssistantAdapter (method-level anchors cited
 * inline below).
 */
export class HomeAssistantAdapter extends BasePlatformAdapter {
	readonly pluginManifest = HA_PLUGIN_MANIFEST;

	private readonly secretReader: ScopedSecretReader;
	private readonly clock: HaClock;
	private readonly wsFactory: HaConnectionFactory;
	readonly rest: HaRestSession;
	private readonly declaredMessageEditing: boolean;

	// ── config (__init__ parity; extra keys verbatim) ────────────────────────
	readonly hassUrl: string;
	readonly watchDomains: ReadonlySet<string>;
	readonly watchEntities: ReadonlySet<string>;
	readonly ignoreEntities: ReadonlySet<string>;
	readonly watchAll: boolean;
	readonly cooldownSeconds: number;

	// ── connection state (__init__/_next_id parity) ──────────────────────────
	private socket: HaClientSocket | null = null;
	private running = false;
	/** True once THIS connection's subscribe frame is sent (live routing gate). */
	private handshaken = false;
	private msgId = 0; // counter STARTS AT 0, incremented per command
	private readonly lastEventTime = new Map<string, number>();

	// ── loop plumbing ────────────────────────────────────────────────────────
	private listenTask: Promise<void> | null = null;
	private loopGeneration = 0;
	private readLoopResolve: (() => void) | null = null;
	private awaitingPong = false;
	private readonly pendingSleepWakes = new Set<() => void>();
	/** Frames received before a handshake reader awaited them. */
	private readonly frameQueue: Array<Record<string, unknown>> = [];
	private readonly frameWaiters: Array<
		(frame: Record<string, unknown>) => void
	> = [];

	// ── observability lanes ──────────────────────────────────────────────────
	readonly counts: HaEventCounters = {
		eventsSeen: 0,
		malformedDropped: 0,
		noEntity: 0,
		ignoreFiltered: 0,
		watchFiltered: 0,
		closedDefaultDropped: 0,
		cooldownSkipped: 0,
		noChangeSkipped: 0,
		accepted: 0,
	};
	readonly dispatchedEvents: HaDispatchedEvent[] = [];
	readonly reconnectLog: Array<{
		stepIndex: number;
		delaySeconds: number;
		delayMs: number;
		atMs: number;
	}> = [];
	readonly reconnectAttempts: Array<{ atMs: number; ok: boolean }> = [];
	readonly forcedReconnects: Array<{ reason: string; atMs: number }> = [];
	readonly warningLog: string[] = [];
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly resolvedFamilies: string[] = [];
	private routerResolved: string[] = [];

	// Interactive surfaces (kit-owned; shared rows drive them).
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	private allowAllClickers = true;
	private readonly clarifyArmedSet = new Set<string>();
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};
	private holding = false;

	private readonly cp: EgressChokepoint;

	constructor(opts: HomeAssistantAdapterOptions) {
		super({
			manifestName: HA_PLUGIN_MANIFEST.name,
			capabilities: HA_CAPABILITIES,
			scalarMaxUnits: opts.scalarMaxUnits ?? HA_MAX_MESSAGE_LENGTH,
			logger: opts.logger,
		});
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.clock = opts.clock ?? {
			nowMs: () => Date.now(),
			sleepMs: (ms) => new Promise<void>((r) => setTimeout(r, ms)),
		};
		this.wsFactory = opts.ws;
		this.rest = opts.rest;
		this.declaredMessageEditing =
			opts.declaredMessageEditing ?? HA_SUPPORTS_MESSAGE_EDITING;
		const config = opts.config ?? {};
		const url = config.url ?? this.secretReader("HASS_URL") ?? HA_DEFAULT_URL;
		this.hassUrl = url.replace(/\/+$/, "");
		this.watchDomains = new Set(config.watch_domains ?? []);
		this.watchEntities = new Set(config.watch_entities ?? []);
		this.ignoreEntities = new Set(config.ignore_entities ?? []);
		this.watchAll = config.watch_all === true;
		this.cooldownSeconds =
			config.cooldown_seconds ?? HA_DEFAULT_COOLDOWN_SECONDS;

		// §11 step 3/4: missing required secret ⇒ LOUD disable (status-visible).
		// Hermes refuses at connect() ("No HASS_TOKEN configured"); the kit
		// expresses the same posture at construction so /status shows why.
		const enablement = resolveEnablement(HA_PLUGIN_MANIFEST, this.secretReader);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		} else {
			// DEC-033 inheritance seam: resolved secret VALUES are registered
			// with the base redactor so they never leak through log emissions.
			const token = this.secretReader("HASS_TOKEN");
			if (token !== undefined) this.registerLogSecret(token);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // no native draft lanes (no edits)
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
			onExecApproval: async (sessionKey) => {
				this.resolvedFamilies.push("ea");
				this.routerResolved.push(`ea:${sessionKey}`);
				return "ok";
			},
			onSlashConfirm: async (sessionKey, _id, choice) => {
				this.resolvedFamilies.push("sc");
				this.routerResolved.push(`sc:${sessionKey}:${choice}`);
				return "ok";
			},
			onClarifyChoice: async (sessionKey, _id, idx) => {
				this.resolvedFamilies.push("cl");
				this.routerResolved.push(`cl:${sessionKey}:${idx}`);
				return `answer-${idx}`;
			},
			onWhatsappApproval: async (sessionKey, _id, approve) => {
				this.resolvedFamilies.push("appr");
				this.routerResolved.push(`appr:${sessionKey}:${approve}`);
				return "ok";
			},
			onPickerNav: async (parsed) => ({
				answerText: `nav:${parsed.family}`,
			}),
		});
	}

	/** Resolved credential (subject/test observability; never logged raw). */
	get hassToken(): string {
		return this.secretReader("HASS_TOKEN") ?? "";
	}

	// ── capabilities: THE probe-computed streaming exclusion ────────────────

	/**
	 * Native draft streaming is excluded BY THE PROBE from the manifest datum
	 * HA_SUPPORTS_MESSAGE_EDITING=false — persistent notifications have no
	 * edit API in the source (no seal/reconcile surface could ever exist).
	 * Flip the datum and this flips — the lie-scan mutant proves the flip
	 * FAILS the streaming rows against seal reality.
	 */
	override supportsDraftStreaming(_chatType?: string): boolean {
		return this.declaredMessageEditing;
	}

	// ── message-id counter (adapter.py:_next_id parity) ──────────────────────

	/** Return the next WebSocket message ID (counter starts 0 ⇒ first id 1). */
	nextId(): number {
		this.msgId += 1;
		return this.msgId;
	}

	// ── connection lifecycle (adapter.py:connect parity) ─────────────────────

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (this.running && this.socket !== null) return true;
		if (!this.hassToken) {
			this.logger?.warn?.(`[${this.manifestName}] No HASS_TOKEN configured`);
			return false;
		}
		const success = await this.wsConnect();
		if (!success) return false;

		// Dedicated REST session for send() calls exists by construction here
		// (injected seam); the source builds aiohttp.ClientSession(total=30).

		// Warn if no event filters are configured (_handle-ha-event drops ALL
		// events in that posture — closed-by-default).
		if (
			this.watchDomains.size === 0 &&
			this.watchEntities.size === 0 &&
			!this.watchAll
		) {
			const message =
				`[${this.manifestName}] No watch_domains, watch_entities, or watch_all configured. ` +
				"All state_changed events will be dropped. Configure filters in " +
				"your HA platform config to receive events.";
			this.warningLog.push(message);
			this.logger?.warn?.(message);
		}

		this.running = true;
		this.startListenLoop();
		this.startHeartbeat();
		this.logger?.info?.(`[${this.manifestName}] Connected to ${this.hassUrl}`);
		return true;
	}

	/**
	 * Establish WebSocket connection and authenticate (adapter.py:_ws_connect
	 * parity — auth_required → auth → auth_ok → subscribe_events + success
	 * ack; ANY deviation ⇒ cleanup + false).
	 */
	async wsConnect(): Promise<boolean> {
		const opened = await this.openSocket();
		if (!opened) return false;

		// Step 1: Receive auth_required.
		const greeting = await this.receiveFrame();
		if (greeting["type"] !== "auth_required") {
			this.logger?.error?.(
				`Expected auth_required, got: ${String(greeting["type"])}`,
			);
			await this.cleanupWs();
			return false;
		}

		// Step 2: Send auth.
		this.socket?.sendText(
			JSON.stringify({ type: "auth", access_token: this.hassToken }),
		);

		// Step 3: Wait for auth_ok.
		const verdict = await this.receiveFrame();
		if (verdict["type"] !== "auth_ok") {
			this.logger?.error?.(`Auth failed: ${JSON.stringify(verdict)}`);
			await this.cleanupWs();
			return false;
		}

		// Step 4: Subscribe to state_changed events.
		const subId = this.nextId();
		this.socket?.sendText(
			JSON.stringify({
				id: subId,
				type: "subscribe_events",
				event_type: "state_changed",
			}),
		);

		// From here, frames other than the ack are live traffic (events may
		// arrive the moment the subscription lands).
		this.handshaken = true;

		// Step 5: Verify subscription acknowledgement — SKIP any live traffic
		// that interleaves before the result frame arrives (backlog flushes;
		// DEC-061 interleave-tolerance delta over strict next-frame abort).
		let ack = await this.receiveFrame();
		while (ack["type"] !== "result" && ack["success"] === undefined) {
			void this.handleLiveFrame(ack);
			ack = await this.receiveFrame();
		}
		if (ack["success"] !== true) {
			this.logger?.error?.(
				`Failed to subscribe to events: ${JSON.stringify(ack)}`,
			);
			await this.cleanupWs();
			return false;
		}

		this.awaitingPong = false; // fresh link: staleness state resets

		// Drain frames that queued during the handshake (events flushed between
		// the ack and the flag flip land here) — ordering preserved.
		while (this.frameQueue.length > 0) {
			const held = this.frameQueue.shift();
			if (held !== undefined) void this.handleLiveFrame(held);
		}
		return true;
	}

	/** Close WebSocket (adapter.py:_cleanup_ws parity). */
	async cleanupWs(): Promise<void> {
		this.handshaken = false;
		this.frameQueue.length = 0; // stale handshake frames never survive a reset
		const sock = this.socket;
		this.socket = null;
		if (sock !== null && sock.readyState !== HA_WS_CLOSED) {
			sock.close(1000, "adapter cleanup");
		}
	}

	override async disconnect(): Promise<void> {
		this.running = false;
		this.loopGeneration += 1;
		// Flush in-flight ladder/heartbeat waits so both loops observe the stop
		// flag instead of hanging on an un-advanced injected clock.
		for (const wake of [...this.pendingSleepWakes]) wake();
		this.pendingSleepWakes.clear();
		// End the read loop BEFORE awaiting the task: the loop parks in
		// readEvents() until the socket closes, so close it first.
		const done = this.readLoopResolve;
		this.readLoopResolve = null;
		done?.();
		await this.cleanupWs();
		const task = this.listenTask;
		this.listenTask = null;
		await task?.catch(() => undefined);
		this.logger?.info?.(`[${this.manifestName}] Disconnected`);
	}

	get isConnected(): boolean {
		return this.running && this.socket !== null;
	}

	// ── internals: socket plumbing ──────────────────────────────────────────

	private openSocket(): Promise<boolean> {
		if (this.socket !== null && this.socket.readyState !== HA_WS_CLOSED) {
			return Promise.resolve(true);
		}
		return new Promise<boolean>((resolve) => {
			let settled = false;
			const settleOnce = (v: boolean) => {
				if (settled) return;
				settled = true;
				resolve(v);
			};
			const listener: HaSocketListener = {
				onOpen: () => settleOnce(true),
				onText: (text) => this.onText(text),
				onClose: (info) => {
					this.onSocketClose(info);
					settleOnce(false);
				},
				onError: () => {
					/* close always follows an error on this plane */
				},
			};
			this.socket = this.wsFactory.connect(listener);
		});
	}

	/**
	 * Frame intake: JSON parse happens HERE (aiohttp receive_json/TEXT
	 * boundary parity). Malformed payloads are debug-dropped; well-formed
	 * frames resolve a handshake waiter FIRST, else route into the live
	 * pipeline.
	 */
	private onText(text: string): void {
		let frame: Record<string, unknown> | null = null;
		try {
			const parsed: unknown = JSON.parse(text);
			if (
				parsed !== null &&
				typeof parsed === "object" &&
				!Array.isArray(parsed)
			) {
				frame = parsed as Record<string, unknown>;
			}
		} catch {
			this.counts.malformedDropped += 1;
			this.logger?.debug?.(`Invalid JSON from HA WS: ${text.slice(0, 200)}`);
			return;
		}
		if (frame === null) return;
		const waiter = this.frameWaiters.shift();
		if (waiter !== undefined) {
			waiter(frame);
			return;
		}
		if (!this.handshaken) {
			// Pre-handshake traffic (the greeting, early events) queues for the
			// handshake reader instead of the live pipeline.
			this.frameQueue.push(frame);
			return;
		}
		void this.handleLiveFrame(frame);
	}

	/** Handshake reader: FIFO over early frames, else park until one arrives. */
	private receiveFrame(): Promise<Record<string, unknown>> {
		const queued = this.frameQueue.shift();
		if (queued !== undefined) return Promise.resolve(queued);
		return new Promise<Record<string, unknown>>((resolve) => {
			this.frameWaiters.push(resolve);
		});
	}

	private async handleLiveFrame(frame: Record<string, unknown>): Promise<void> {
		const type = frame["type"];
		if (type === "pong") {
			this.awaitingPong = false;
			return;
		}
		if (type === "event") {
			const evt = frame["event"];
			if (evt !== null && typeof evt === "object" && !Array.isArray(evt)) {
				await this.handleHaEvent(evt as Record<string, unknown>);
			}
		}
	}

	private onSocketClose(_info: HaCloseInfo): void {
		this.socket = null;
		// End the read loop; the listen loop owns the ladder from here.
		const done = this.readLoopResolve;
		this.readLoopResolve = null;
		done?.();
	}

	// ── listen loop (adapter.py:_listen_loop parity) ─────────────────────────

	private startListenLoop(): void {
		const gen = ++this.loopGeneration;
		this.listenTask = (async () => {
			let backoffIdx = 0;
			while (this.running && gen === this.loopGeneration) {
				await this.readEvents();
				if (!this.running || gen !== this.loopGeneration) return;

				// Reconnect with backoff: steps clamp at the LAST entry; the
				// ladder index resets to 0 on a SUCCESSFUL reconnect.
				const delaySeconds = haBackoffStepSeconds(backoffIdx);
				this.reconnectLog.push({
					stepIndex: backoffIdx,
					delaySeconds,
					delayMs: delaySeconds * 1000,
					atMs: this.clock.nowMs(),
				});
				await this.interruptibleSleep(delaySeconds * 1000);
				backoffIdx += 1;

				await this.cleanupWs();
				let success = false;
				try {
					success = await this.wsConnect();
				} catch {
					success = false;
				}
				this.reconnectAttempts.push({ atMs: this.clock.nowMs(), ok: success });
				if (success) backoffIdx = 0; // Reset on successful reconnect
			}
		})();
	}

	/** Blocks until the socket closes/errors (or returns immediately if dead). */
	private readEvents(): Promise<void> {
		if (this.socket === null || this.socket.readyState === HA_WS_CLOSED) {
			return Promise.resolve();
		}
		return new Promise<void>((resolve) => {
			this.readLoopResolve = resolve;
		});
	}

	/** Injected-clock sleep that disconnect() can interrupt. */
	private interruptibleSleep(ms: number): Promise<void> {
		return new Promise<void>((resolve) => {
			let settled = false;
			const settle = () => {
				if (settled) return;
				settled = true;
				this.pendingSleepWakes.delete(settle);
				resolve();
			};
			this.pendingSleepWakes.add(settle);
			void this.clock.sleepMs(ms).then(() => settle());
		});
	}

	// ── heartbeat (ws_connect(heartbeat=30) parity) ──────────────────────────

	private startHeartbeat(): void {
		const gen = this.loopGeneration;
		void (async () => {
			while (this.running && gen === this.loopGeneration) {
				await this.interruptibleSleep(HA_WS_HEARTBEAT_MS);
				if (!this.running || gen !== this.loopGeneration) return;
				this.heartbeatTick();
			}
		})();
	}

	/**
	 * One heartbeat pass: ping unless the PREVIOUS ping is still unanswered —
	 * a stalled pong past one full interval means the link is wedged; force
	 * the reconnect (close feeds the ladder through onClose).
	 */
	private heartbeatTick(): void {
		const sock = this.socket;
		if (sock === null || sock.readyState !== HA_WS_OPEN) return;
		if (this.awaitingPong) {
			this.forcedReconnects.push({
				reason: "ping/pong stale",
				atMs: this.clock.nowMs(),
			});
			this.logger?.warn?.(
				`${this.manifestName}: reaping stale socket — ping/pong stale`,
			);
			sock.close(4000, "ping/pong stale");
			return;
		}
		this.awaitingPong = true;
		sock.sendText(JSON.stringify({ id: this.nextId(), type: "ping" }));
	}

	// ── event pipeline (adapter.py:_handle_ha_event parity; ORDER MATTERS) ──

	async handleHaEvent(event: Record<string, unknown>): Promise<void> {
		this.counts.eventsSeen += 1;
		const dataRaw = event["data"];
		const data: Record<string, unknown> =
			dataRaw !== null && typeof dataRaw === "object" && !Array.isArray(dataRaw)
				? (dataRaw as Record<string, unknown>)
				: {};
		const entityId =
			typeof data["entity_id"] === "string" ? data["entity_id"] : "";

		// entity_id required.
		if (!entityId) {
			this.counts.noEntity += 1;
			return;
		}

		// Apply ignore filter (beats every watch filter).
		if (this.ignoreEntities.has(entityId)) {
			this.counts.ignoreFiltered += 1;
			return;
		}

		// Apply domain/entity watch filters (closed by default — require
		// explicit watch_domains, watch_entities, or watch_all to forward).
		const domain = entityId.includes(".") ? (entityId.split(".")[0] ?? "") : "";
		if (this.watchDomains.size > 0 || this.watchEntities.size > 0) {
			const domainMatch =
				this.watchDomains.size > 0 && this.watchDomains.has(domain);
			const entityMatch =
				this.watchEntities.size > 0 && this.watchEntities.has(entityId);
			if (!domainMatch && !entityMatch) {
				this.counts.watchFiltered += 1;
				return;
			}
		} else if (!this.watchAll) {
			// No filters configured and watch_all is off — drop the event.
			this.counts.closedDefaultDropped += 1;
			return;
		}

		// Apply cooldown (per-entity; INJECTED clock). NOTE: the source reads
		// self._last_event_time.get(entity_id, 0) against WALL-EPOCH seconds,
		// where the 0 default means "no prior event"; the port keeps that
		// semantics with a -Infinity baseline so zero-based injected clocks
		// behave identically (first observation always passes).
		const nowSeconds = this.clock.nowMs() / 1000;
		const last = this.lastEventTime.get(entityId) ?? Number.NEGATIVE_INFINITY;
		if (nowSeconds - last < this.cooldownSeconds) {
			this.counts.cooldownSkipped += 1;
			return;
		}
		this.lastEventTime.set(entityId, nowSeconds);

		// Build human-readable message.
		const oldState = asState(data["old_state"]);
		const newState = asState(data["new_state"]);
		const message = formatStateChange(entityId, oldState, newState);
		if (message === null) {
			this.counts.noChangeSkipped += 1;
			return;
		}

		// Build IncomingEvent and forward through the guard pipeline.
		const messageId = `ha_${entityId}_${Math.floor(nowSeconds)}`;
		this.counts.accepted += 1;
		this.dispatchedEvents.push({
			messageId,
			text: message,
			chatId: HA_EVENTS_CHAT_ID,
			userId: HA_EVENTS_USER_ID,
			chatType: "channel",
		});
		const sessionKey = `${this.manifestName}:${HA_EVENTS_CHAT_ID}`;
		const incoming: IncomingEvent = {
			messageType: "text",
			text: message,
			messageId,
			source: {
				platform: this.manifestName,
				chatType: "channel",
				userId: HA_EVENTS_USER_ID,
				chatId: HA_EVENTS_CHAT_ID,
				chatName: "Home Assistant Events",
			},
		};
		try {
			await this.handleIngress(incoming, sessionKey);
		} catch {
			/* containment parity: one poisoned event never kills the read loop */
		}
	}

	// ── multi-chat-safe delivery pipeline (base parity, per-call binding) ────

	/**
	 * ONE session-scoped formatting ladder (the §10.1 rich-downgrade latch
	 * must persist across chunks AND sends) whose door closures bind the
	 * CURRENT chatId dynamically (signal-precedent override; Discord precedent
	 * allows deliverText overrides). HA converts NOTHING: converted == plain
	 * == the verbatim notification body.
	 */
	private sessionLadder: FormattingLadder | null = null;
	private activeChatId = "";

	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		// ONE persistent_notification/create POST with message=content[:4096]
		// (adapter.py:send :424-432; title per the DEC-056 branding datum —
		// byte-exact until that DEC lands): splitsLongMessages=false ⇒ NO
		// chunk lane exists on this source — the door-level slice below IS
		// the vendor cap, never a split into labeled pieces.
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

		this.activeChatId = chatId; // door target binding for THIS delivery
		return [await this.deliverChunkOn(ladder, content, metadata)];
	}

	/** Base deliverChunk parity over the SHARED session ladder. */
	private async deliverChunkOn(
		ladder: FormattingLadder,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const outcome = await ladder.sendText(chunk, metadata);
		if (outcome.success) return outcome;

		// A transient RICH failure is NEVER legacy-resent (§10.1 duplicate risk).
		if (outcome.tier === "rich") return outcome;

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

		if (failureClass === "formatting") {
			return this.wireSend(
				this.activeChatId,
				plainTextFallbackBody(chunk),
				metadata,
			);
		}
		return outcome;
	}

	// ── egress doors ─────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * DOOR transport — REST POST persistent_notification/create
	 * (adapter.py:send parity): Bearer token header, message truncated at
	 * MAX_MESSAGE_LENGTH=4096, title "Hermes Agent" (VERBATIM vendor wire
	 * data — see manifest proposed-DEC note). status<300 ⇒ success with a
	 * generated 12-hex messageId; ≥300 ⇒ fail "HTTP {status}: {body}";
	 * timeout-classified failures map honestly (never retried upstream).
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const payload = {
			title: HA_NOTIFICATION_TITLE,
			message: content.slice(0, HA_MAX_MESSAGE_LENGTH),
		};
		const headers = {
			Authorization: `Bearer ${this.hassToken}`,
			"Content-Type": "application/json",
		};
		try {
			const outcome = await this.rest.post(
				{
					path: `${this.hassUrl}${HA_REST_NOTIFICATION_CREATE}`,
					payload,
					headers,
				},
				chatId,
				metadata,
			);
			switch (outcome.kind) {
				case "ok":
					return { success: true, messageId: randomHex12() };
				case "http":
					if (outcome.status < 300) {
						return { success: true, messageId: randomHex12() };
					}
					return {
						success: false,
						error: `HTTP ${outcome.status}: ${outcome.body}`,
					};
				case "timeout":
					return {
						success: false,
						error: "Timeout sending notification to HA",
					};
				case "transport-failure":
					return { success: false, error: outcome.error };
			}
		} catch (err) {
			return {
				success: false,
				error: err instanceof Error ? err.message : String(err),
			};
		}
	}

	/**
	 * Rich lane ABSENT on the real HA REST surface: unless the harness
	 * explicitly scripted a rich probe, answer the capability-error shape
	 * WITHOUT burning a roundtrip (§10.1 latch path probes once then never
	 * again — reference-subject parity).
	 */
	protected override async wireRich(
		content: string,
		_metadata: Metadata,
	): Promise<SendResult> {
		if (!this.rest.hasRichScript("rich")) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return this.rest.transmitRich("__rich__", content);
	}

	/**
	 * send_typing: deliberate NO-OP (adapter.py:send_typing — "No typing
	 * indicator for Home Assistant"). ABSENCE DATUM: zero wire ops forever.
	 */
	async sendTyping(_chatId: string): Promise<void> {
		void _chatId;
	}

	/** get_chat_info parity — basic info about the HA event channel. */
	getChatInfo(): { name: string; type: string; url: string } {
		return {
			name: "Home Assistant Events",
			type: "channel",
			url: this.hassUrl,
		};
	}

	// ── guard wiring (reference-fixture inheritance; copied verbatim) ────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: HA_REGISTRY,
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
			} satisfies {
				registry: CommandRegistry;
				messageHandler: MessageHandler;
				sendReply: (chatId: string, text: string) => Promise<void>;
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

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	routerAuditResolved(): readonly string[] {
		return this.routerResolved;
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

	// ── identity probes (token lock; missing-secret sibling) ─────────────────

	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.lifecycle.statusSnapshot();
	}

	buildMissingSecretSibling(): HomeAssistantAdapter {
		return new HomeAssistantAdapter({
			ws: this.wsFactory,
			rest: this.rest,
			clock: this.clock,
			secretReader: () => undefined,
		});
	}

	/**
	 * Per-chat length pair (§6.3/A15): REMOVED — the source has no per-chat
	 * budgets (LINE_NATIVE_SPLIT_TRUNCATES precedent). HA's vendor cap is the
	 * FIXED MAX_MESSAGE_LENGTH=4096 on every notification (adapter.py
	 * MAX_MESSAGE_LENGTH class attr), enforced at the door.
	 */
}

// ── helpers ──────────────────────────────────────────────────────────────────────

function asState(v: unknown): HaEntityState | null {
	if (v === null || v === undefined) return null;
	if (typeof v !== "object" || Array.isArray(v)) return null;
	return v as HaEntityState;
}

/** uuid.uuid4().hex[:12] parity — 12 lowercase hex chars. */
function randomHex12(): string {
	return randomBytes(6).toString("hex");
}

/** Re-exported ladder bounds for fixture assertions. */
export { HA_BACKOFF_STEPS_SECONDS };

export type { DisableReason };
