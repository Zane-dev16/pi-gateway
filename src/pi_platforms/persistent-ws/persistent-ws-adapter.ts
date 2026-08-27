// pi_platforms/persistent-ws/persistent-ws-adapter — THE persistent-WebSocket
// reference adapter (Slack/Discord-like shape, DEC-002). Built ENTIRELY on the
// pi_platforms kit + pi_gateway seams; the fake ws plane stands in for the
// vendor socket (04 §8 headless rule — no external network).
//
// Shape obligations (04 §3 matrix, Persistent WS column):
//   - Reconnect: RESUBSCRIBE + REPLAY WINDOW — the client resumes from its
//     last-delivered event-id cursor so the server replays everything sent
//     during the disconnect, exactly once downstream (dedup makes the
//     server's at-least-once redelivery safe, #4777 parity).
//   - Backpressure: socket heartbeat/watchdog with stale detection; the
//     resume cursor IS the backlog commitment.
//   - Rate limits: Retry-After CAPTURED from REST results AND close payloads,
//     feeding BOTH the send-retry ladder and the reconnect ladder (A23);
//     feature-gate errors latch the native-stream capability OFF permanently
//     for the session (`_native_stream_unsupported` parity).
//   - Draft streaming: NATIVE streaming edits over the *Stream plane sending
//     RAW markdown; DUAL-PATH markdown — the REST postMessage/edit path
//     converts the dialect, the native path never touches the bytes (§10.2).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/slack/adapter.py:_socket_watchdog_loop        (watchdog)
//   plugins/platforms/slack/adapter.py:_socket_ping_pong_stale      (stale =
//     no recent pong; factor 4, first-ping grace 60 s defaults here)
//   plugins/platforms/slack/adapter.py:send_draft/_active_streams  (RAW
//     cumulative frames; append-delta discipline; prefix mismatch fails the
//     frame so the consumer degrades to the edit path)
//   plugins/platforms/slack/adapter.py:_native_stream_unsupported   (latch)

import type {
	DraftFrameArgs,
	EditOptions,
	Metadata,
	SendResult,
	StreamEgressAdapter,
	StreamLogger,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	IncomingEvent,
	TaskSpawner,
	CommandRegistry,
	MessageHandler,
} from "../../pi_gateway/guards/index.js";
import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	TokenLockManagerSeam,
} from "../kit/index.js";
import { FormattingLadder } from "../kit/formatting-ladder.js";
import type { FormattingTransport } from "../kit/formatting-ladder.js";
import {
	classifySendError,
	sendWithRetry,
	plainTextFallbackBody,
	DELIVERY_FAILED_NOTICE,
} from "../kit/send-retry.js";
import { chunkWithFenceCarry } from "../kit/chunking.js";
import type { CapabilityManifest } from "../kit/capabilities.js";
import {
	resolveEnablement,
	type ScopedSecretReader,
	type EnvVarSpec,
	type TransportShape,
} from "../kit/registration.js";
import type { LengthUnit } from "../kit/length-policy.js";

import type { SleepFn, NowFn } from "./manual-clock.js";
import {
	FakeWsServer,
	WS_CLOSED,
	WS_OPEN,
	type WsClientSocket,
	type WsCloseInfo,
	type WsConnectionFactory,
	type WsFrame,
	type WsPlatformEvent,
	type WsSocketListener,
} from "./fake-ws.js";
import {
	ReconnectLadder,
	type ReconnectLadderOptions,
} from "./reconnect-ladder.js";
import { EventDeduplicator, ResumeCursor } from "./event-cursor.js";
import { CapabilityLatch } from "./capability-latch.js";
import { convertMarkdownToMrkdwn, sealSuffix } from "./dual-path-markdown.js";

/**
 * Structural REST plane — byte-compatible with the conformance harness's
 * FakePlatformWire (declared locally so the adapter source imports NOTHING
 * from conformance; the test passes the real wire object in).
 */
export interface RestPlane {
	transmitSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult>;
	transmitEdit(
		chatId: string,
		messageId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult>;
	transmitDraft(
		chatId: string,
		draftId: number,
		content: string,
		final: boolean,
		metadata: Metadata,
	): Promise<SendResult>;
	transmitRich(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult>;
	hasScript(opKind: "send" | "edit" | "draft" | "seal" | "rich"): boolean;
}

/** Injected timing seam (structurally satisfied by ManualClock). */
export interface AdapterClock {
	nowMs: NowFn;
	sleepMs: SleepFn;
}

/** Command registry — same five-command conformance registry as the kit base. */
export const WS_REGISTRY: CommandRegistry = [
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

export interface PersistentWsAdapterDeps {
	manifestName: string;
	capabilities?: Partial<CapabilityManifest> | undefined;
	lengthUnit?: LengthUnit | undefined;
	scalarMaxUnits?: number | undefined;
	logger?: StreamLogger | undefined;

	/** Event-plane factory (FakeWsServer in tests; a real ws client elsewhere). */
	transport: WsConnectionFactory;
	/** REST plane for sends/edits/stream ops (scripted fake in tests). */
	rest: RestPlane;
	clock: AdapterClock;

	/** Required-secret enablement — missing secret ⇒ LOUD disable (§8). */
	requiresEnv?: readonly EnvVarSpec[] | undefined;
	secretReader?: ScopedSecretReader | undefined;
	transportShape?: TransportShape | undefined;

	/** Our own identity for self/echo filtering. */
	botUserId?: string | undefined;

	/**
	 * Chats whose ONE native stream IS the message (relay-shaped lanes;
	 * draft_stream_is_message instance-level parity, DEC-006 review B4).
	 * Seal-interception arms ONLY for these.
	 */
	streamIsMessageChatIds?: ReadonlySet<string> | undefined;

	/** Watchdog tuning (defaults port A23: interval 10s, factor 4, grace 60s). */
	pingIntervalMs?: number | undefined;
	pingStaleFactor?: number | undefined;
	firstPingGraceMs?: number | undefined;
	watchdogIntervalMs?: number | undefined;

	/** Reconnect ladder tuning. */
	ladder?: ReconnectLadderOptions | undefined;

	/** Dedup window tuning (Slack default TTL 3600s, LRU-bounded). */
	dedupTtlMs?: number | undefined;
}

interface OpenNativeStream {
	ts: string;
	draftId: number;
	/** Cumulative bytes ALREADY transmitted on this stream (RAW dialect). */
	sent: string;
}

/** Required secret for the persistent-ws reference adapter (loud-disable row). */
export const WS_REQUIRED_SECRET = "WS_BOT_TOKEN";

const DEFAULT_PING_INTERVAL_MS = 10_000;
const DEFAULT_PING_STALE_FACTOR = 4;
const DEFAULT_FIRST_PING_GRACE_MS = 60_000;
const DEFAULT_WATCHDOG_INTERVAL_MS = 5_000;

export type WsRunPhase =
	| "new"
	| "connecting"
	| "live"
	| "reconnect-scheduled"
	| "stopped";

/**
 * The persistent-ws shape adapter. Transport + formatting + capabilities;
 * ALL policy-shaped machinery comes from the kit/base.
 */
export class PersistentWsAdapter
	extends BasePlatformAdapter
	implements StreamEgressAdapter
{
	protected readonly rest: RestPlane;
	protected readonly clock: AdapterClock;
	private readonly transportFactory: WsConnectionFactory;

	// ── ws session state ───────────────────────────────────────────────────
	private socket: WsClientSocket | null = null;
	private connectedAtMs: number | null = null;
	private phase: WsRunPhase = "new";
	private running = false;
	private reconnectPending = false;
	private watchdogGeneration = 0;

	readonly cursor = new ResumeCursor();
	private readonly dedup: EventDeduplicator;
	readonly nativeStreamLatch = new CapabilityLatch();
	readonly reconnectLadder: ReconnectLadder;

	/** Delivered (post-dedup, post-filter) inbound event ids, in order. */
	readonly inboundLog: WsPlatformEvent[] = [];
	/** Redeliveries suppressed by the dedup window (#4777 exactly-once audit). */
	get dedupSuppressedCount(): number {
		return this.dedup.suppressedCount;
	}
	/** Reconnect steps taken (delay + authoritative flag) — escalation audit. */
	readonly reconnectLog: { delayMs: number; authoritative: boolean }[] = [];
	/** Last server-authoritative retry-after captured from ANY source. */
	lastCapturedRetryAfterSeconds: number | null = null;

	private readonly pingIntervalMs: number;
	private readonly pingStaleFactor: number;
	private readonly firstPingGraceMs: number;
	private readonly watchdogIntervalMs: number;
	/** Chats whose ONE native stream IS the message (seal-arming set). */
	private readonly sealChats: ReadonlySet<string>;
	private readonly botUserId: string;

	// ── egress machinery ────────────────────────────────────────────────────
	private readonly cp: EgressChokepoint;
	private formatLadder: FormattingLadder | null = null;
	/** Chat the NEXT ladder call transmits against (set per deliver call). */
	private ladderChatId = "";
	/** chatId → open native *Stream connector state. */
	private readonly openStreams = new Map<string, OpenNativeStream>();

	// ── interactive surfaces (§9; DEC-016 — kit-owned, shape-agnostic) ──────
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly resolvedFamilies: string[] = [];
	private routerResolved: string[] = [];
	private readonly clarifyArmedSet = new Set<string>();
	private allowAllClickers = true;

	/** Optional e2e turn driver: replaces the scripted echo handler body. */
	turnDriver:
		| ((event: IncomingEvent, ctxText: string) => Promise<string | null>)
		| null = null;

	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};
	private holding = false;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(deps: PersistentWsAdapterDeps) {
		super({
			manifestName: deps.manifestName,
			capabilities: {
				splitsLongMessages: true,
				typedCommandPrefix: "!",
				...(deps.capabilities ?? {}),
			},
			lengthUnit: deps.lengthUnit,
			scalarMaxUnits: deps.scalarMaxUnits ?? 4096,
			logger: deps.logger,
		});
		this.transportFactory = deps.transport;
		this.rest = deps.rest;
		this.clock = deps.clock;
		this.botUserId = deps.botUserId ?? "bot-self";
		this.sealChats = deps.streamIsMessageChatIds ?? new Set<string>();
		this.pingIntervalMs = deps.pingIntervalMs ?? DEFAULT_PING_INTERVAL_MS;
		this.pingStaleFactor = deps.pingStaleFactor ?? DEFAULT_PING_STALE_FACTOR;
		this.firstPingGraceMs =
			deps.firstPingGraceMs ?? DEFAULT_FIRST_PING_GRACE_MS;
		this.watchdogIntervalMs =
			deps.watchdogIntervalMs ?? DEFAULT_WATCHDOG_INTERVAL_MS;
		this.dedup = new EventDeduplicator({
			...(deps.dedupTtlMs !== undefined ? { ttlMs: deps.dedupTtlMs } : {}),
			nowMs: deps.clock.nowMs,
		});
		// BUGFIX (ws-completion): the ladder MUST sleep on the INJECTED clock —
		// the default wall-clock setTimeout made reconnects untestable under
		// virtual time and violated the workspace injected-clock rule.
		this.reconnectLadder = new ReconnectLadder({
			...(deps.ladder ?? {}),
			sleep: deps.ladder?.sleep ?? ((ms) => this.clock.sleepMs(ms)),
		});

		// §11 step 3: required secrets enablement — missing ⇒ LOUD disable.
		if (deps.requiresEnv && deps.secretReader) {
			const enablement = resolveEnablement(
				{
					name: deps.manifestName,
					description: "persistent-ws reference adapter",
					transportShape: deps.transportShape ?? "ws",
					requiresEnv: deps.requiresEnv,
					capabilities: {},
				},
				deps.secretReader,
			);
			if (!enablement.enabled && enablement.reason) {
				this.lifecycle.disable(enablement.reason);
			} else {
				// DEC-033 inheritance seam in action: resolved secret VALUES are
				// registered with the base redactor so they can never leak through
				// this adapter's log emissions.
				for (const spec of deps.requiresEnv) {
					const value = deps.secretReader(spec.name);
					if (value !== undefined) this.registerLogSecret(value);
				}
			}
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: (chatId) =>
				(this.sealChats as ReadonlySet<string>).has(String(chatId)),
			transmitSend: async (chatId, content, metadata) =>
				this.restSendWithLinkPreviewPolicy(chatId, content, metadata),
			transmitEdit: async (chatId, messageId, content, opts) =>
				this.rest.transmitEdit(
					chatId,
					messageId,
					convertMarkdownToMrkdwn(content), // REST edit lane CONVERTS (§10.2)
					opts,
				),
			transmitSeal: async (_key, chatId, draftId, content, metadata) =>
				this.sealNativeStream(chatId, draftId, content, metadata),
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
				hostEditText: JSON.stringify(parsed),
			}),
		});
	}

	// ── runner wiring passthrough (subject/test attaches the guard) ────────

	// Pure passthrough — the widened deps shape (incl. the optional
	// best-effort onTurnFailure failure-report hook) stays in lockstep with
	// BasePlatformAdapter.attachGuard so subject wiring keeps compiling.
	attachGuard(
		deps: {
			registry: CommandRegistry;
			messageHandler: MessageHandler;
			sendReply: (chatId: string, text: string) => Promise<void>;
			onTurnFailure?:
				| ((event: IncomingEvent) => void | Promise<void>)
				| undefined;
		},
		opts: {
			spawner?: TaskSpawner | undefined;
			hasPendingClarify?: ((sessionKey: string) => boolean) | undefined;
		} = {},
	): void {
		super.attachGuard(deps, opts);
	}

	// ── conformance-subject plumbing (parity with polling/webhook shapes) ──

	private readonly isMessageChats = new Set<string>(["__none__"]);
	/** Relay-shaped lanes mark their chats AFTER construction (review B4). */
	markStreamIsMessage(chatId: string): void {
		(this.sealChats as Set<string>).add(chatId);
		this.isMessageChats.add(chatId);
	}

	get clarifyArmed(): Set<string> {
		return this.clarifyArmedSet;
	}
	routerAuditResolved(): readonly string[] {
		return this.routerResolved;
	}
	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	/** Production wiring: ingress needs the L1 guard (subject/test attaches). */
	attachStandardGuard(spawner?: TaskSpawner): void {
		this.attachGuard(
			{
				registry: WS_REGISTRY,
				messageHandler: async (event, ctx) => {
					const text = event.text ?? `[${String(event.messageType)}]`;
					// Lane C clarify intercept resolves BEFORE any turn work.
					const sessionKey = String(
						event.metadata?.["gateway_session_key"] ?? "",
					);
					if (this.clarifyArmedSet.has(sessionKey) && !text.startsWith("/")) {
						this.clarifyCaptures.push(text);
						return null; // consumed by the clarify resolver
					}
					this.turnLog.push(text);
					if (this.turnDriver !== null) {
						return this.turnDriver(event, text);
					}
					// The hold gate models a BUSY TURN. Inline lanes (control
					// commands, clarify answers under DETACHED_TASK) never block —
					// they run while the old turn unwinds.
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
					return `reply:${text}`;
				},
				sendReply: async (_chatId, text) => {
					this.replyLog.push(text);
				},
			},
			{
				spawner,
				hasPendingClarify: (key) => this.clarifyArmedSet.has(key),
			},
		);
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

	/** Subject-level inbound lane: self/echo filter + routing-key stamping. */
	async deliverInbound(
		event: IncomingEvent,
		sessionKey: string,
	): Promise<void> {
		const senderId = String(event.source?.userId ?? "");
		if (senderId === this.botUserId) return; // self/echo filter (§8 ingress)
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	doorAudit() {
		return this.cp.audit;
	}

	/** Relay lanes arm seal-interception via one emitted draft frame. */
	async armNativeStream(chatId: string, draftId: number): Promise<void> {
		await this.sendDraft({ chatId, draftId, content: "" });
	}

	/**
	 * Formatting ladder probe lane (§10.1 row): transient rich failure is
	 * NEVER legacy-resent — a failed retryable SendResult and NO send.
	 */
	async transientRichOutcome(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		void chatId;
		const ladder = new FormattingLadder({
			tryRich: async () => ({ success: false, error: "socket hang up" }),
			sendConverted: async (_c, md) => {
				void md;
				return { success: false, error: "SHOULD-NOT-HAPPEN" };
			},
			sendPlain: async () => ({ success: false, error: "SHOULD-NOT-HAPPEN" }),
		});
		return ladder.sendText(content, {});
	}

	// ── transport lifecycle ────────────────────────────────────────────────

	get currentPhase(): WsRunPhase {
		return this.phase;
	}

	get isLive(): boolean {
		return (
			this.socket !== null &&
			this.socket.readyState === WS_OPEN &&
			this.phase === "live"
		);
	}

	/**
	 * Open the socket and subscribe. `isReconnect` PRESERVES server-side state
	 * by resubscribing WITH the cursor (server replays the disconnect window);
	 * a cold boot subscribes with whatever cursor exists (null initially).
	 */
	async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		this.running = true;
		this.phase = "connecting";
		const opened = await this.openSocket();
		if (opened) {
			this.startWatchdog();
			return true;
		}
		return false;
	}

	async disconnect(): Promise<void> {
		this.running = false;
		this.phase = "stopped";
		this.watchdogGeneration += 1; // orphan any pending watchdog sleep
		const sock = this.socket;
		this.socket = null;
		sock?.close(1000, "adapter shutdown");
	}

	/** Drive one manual watchdog pass (tests invoke this via the clock). */
	runWatchdogTick(): void {
		this.watchdogTick();
	}

	// ── internals: socket lifecycle ────────────────────────────────────────

	private openSocket(): Promise<boolean> {
		if (this.socket !== null && this.socket.readyState !== WS_CLOSED) {
			return Promise.resolve(true);
		}
		this.phase = "connecting";
		return new Promise<boolean>((resolve) => {
			let settled = false;
			const settleOnce = (v: boolean) => {
				if (settled) return;
				settled = true;
				resolve(v);
			};
			const listener: WsSocketListener = {
				onOpen: () => {
					this.onSocketOpen();
					settleOnce(true);
				},
				onFrame: (frame) => void this.onSocketFrame(frame),
				onClose: (info) => {
					this.onSocketClose(info);
					settleOnce(false);
				},
				onError: () => {
					/* close always follows an error on this plane */
				},
			};
			this.socket = this.transportFactory.connect(listener);
		});
	}

	private onSocketOpen(): void {
		this.connectedAtMs = this.clock.nowMs();
		// RESUBSCRIBE with the resume cursor — null on cold boot, last
		// delivered id across reconnects (the replay-window contract).
		this.socket?.send({
			type: "subscribe",
			subscription: "gateway-events",
			cursor: this.cursor.value,
			isReconnectResume: this.cursor.value !== null,
		});
	}

	private async onSocketFrame(frame: WsFrame): Promise<void> {
		const type = frame["type"];
		if (type === "subscribed") {
			// Session healthy: exponential component resets (Retry-After-shaped
			// delays were one-shot authoritative values, never a baseline).
			this.reconnectLadder.reset();
			this.phase = "live";
			return;
		}
		if (type === "event") {
			const evt = frame["event"] as WsPlatformEvent | undefined;
			if (evt) await this.handlePlatformEvent(evt);
		}
	}

	private onSocketClose(info: WsCloseInfo): void {
		this.socket = null;
		this.connectedAtMs = null;
		if (this.phase !== "stopped") this.phase = "reconnect-scheduled";
		// Retry-After captured FROM THE CLOSE PAYLOAD feeds the ladder (§3).
		if (
			info.retryAfterSeconds !== undefined &&
			info.retryAfterSeconds !== null
		) {
			this.lastCapturedRetryAfterSeconds = info.retryAfterSeconds;
		}
		if (this.running)
			void this.scheduleReconnect(info.retryAfterSeconds ?? null);
	}

	private async scheduleReconnect(
		retryAfterSeconds: number | null,
	): Promise<void> {
		if (!this.running || this.reconnectPending) return;
		this.reconnectPending = true;
		const step = await this.reconnectLadder.wait(retryAfterSeconds);
		this.reconnectLog.push({
			delayMs: step.delayMs,
			authoritative: step.authoritative,
		});
		this.reconnectPending = false;
		if (!this.running) return;
		await this.openSocket();
	}

	private startWatchdog(): void {
		const gen = ++this.watchdogGeneration;
		void (async () => {
			while (this.running && gen === this.watchdogGeneration) {
				await this.clock.sleepMs(this.watchdogIntervalMs);
				if (!this.running || gen !== this.watchdogGeneration) return;
				this.watchdogTick();
			}
		})();
	}

	/**
	 * One watchdog pass: heartbeat when due, then STALENESS — the wedged-zombie
	 * detector (is_connected can lie; missing PONGS cannot, A23 parity).
	 */
	private watchdogTick(): void {
		const sock = this.socket;
		if (sock === null || sock.readyState !== WS_OPEN) {
			if (this.running && !this.reconnectPending) {
				void this.scheduleReconnect(null);
			}
			return;
		}
		const now = this.clock.nowMs();
		const pingImpl = sock as WsClientSocket & {
			lastPingSentAt: number | null;
			lastPongAt: number | null;
		};
		if (
			pingImpl.lastPingSentAt === null ||
			now - pingImpl.lastPingSentAt >= this.pingIntervalMs
		) {
			sock.ping();
		}
		const lastPong = pingImpl.lastPongAt;
		if (lastPong === null) {
			// No pong YET — only suspicious once the first-ping grace elapses.
			const grace = Math.max(this.firstPingGraceMs, this.pingIntervalMs * 2);
			if (this.connectedAtMs !== null && now - this.connectedAtMs > grace) {
				this.reapStaleSocket("ping/pong stale (no first pong)");
			}
			return;
		}
		if (now - lastPong > this.pingIntervalMs * this.pingStaleFactor) {
			this.reapStaleSocket("ping/pong stale");
		}
	}

	/**
	 * Stale-socket reap: close the dead socket locally; the close handler
	 * feeds the reconnect ladder. Bound: detection happens within
	 * pingInterval·factor + one watchdog interval.
	 */
	private reapStaleSocket(reason: string): void {
		const sock = this.socket;
		this.logger?.warn?.(
			`${this.manifestName}: reaping stale socket — ${reason}`,
		);
		sock?.close(4000, reason);
	}

	// ── ingress pipeline ───────────────────────────────────────────────────

	/**
	 * Platform event → guards. Self/echo filtered, redeliveries deduplicated,
	 * cursor advanced ONLY after the handler accepts (ack-after-process).
	 *
	 * Dispatch errors are CONTAINED (logged, cursor NOT advanced) — an
	 * exception escaping into the socket frame pump would surface as an
	 * unhandled rejection while the session stays up.
	 */
	async handlePlatformEvent(evt: WsPlatformEvent): Promise<void> {
		if (evt.userId === this.botUserId) return; // self/echo filter (§8)
		if (this.dedup.isDuplicate(evt.id)) return; // #4777 redelivery suppression
		try {
			await this.dispatchIncoming(
				{
					messageId: evt.id,
					messageType: "text",
					text: evt.text,
					source: {
						platform: this.manifestName,
						chatType: "channel",
						userId: evt.userId,
						chatId: evt.chatId,
					},
				},
				`${this.manifestName}:${evt.chatId}`,
			);
		} catch (err) {
			// Ack-after-process: the cursor does NOT advance — a healthy server
			// replay window can still redeliver after a reconnect. Contain the
			// failure so the transport loop survives.
			this.logger?.error?.(
				`${this.manifestName}: dispatch failed for event ${evt.id}: ${err instanceof Error ? err.message : String(err)}`,
			);
			return;
		}
		this.inboundLog.push(evt);
		this.cursor.advance(evt.id);
	}

	protected async dispatchIncoming(
		event: IncomingEvent,
		sessionKey: string,
	): Promise<void> {
		event.metadata = { ...(event.metadata ?? {}) };
		await this.handleIngress(event, sessionKey);
	}

	// ── egress doors (base doors route through OUR chokepoint) ─────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	/**
	 * Native-splitting delivery (splits_long_messages=True parity): the
	 * adapter chunks natively with THE kit chunker (fence-carry + (i/n)),
	 * preserving full output, then runs each chunk through the session
	 * formatting ladder wrapped in the §6.1 retry/fallback lanes.
	 */
	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		const policy = this.chatLengthPolicyForChat(chatId);
		const plan = chunkWithFenceCarry(content, policy);
		const results: SendResult[] = [];
		for (const chunk of plan.chunks) {
			this.ladderChatId = chatId;
			results.push(await this.deliverWiredChunk(chatId, chunk, metadata));
		}
		return results;
	}

	/** Per-chunk pipeline (§6.1 semantics over the dual-path transports). */
	private async deliverWiredChunk(
		chatId: string,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const outcome = await this.ensureFormatLadder().sendText(chunk, metadata);
		if (outcome.success) return outcome;

		// Transient RICH failures are NEVER legacy-resent (§10.1 duplicate risk).
		if (outcome.tier === "rich") return outcome;

		const failureClass = classifySendError(new Error(outcome.error ?? ""));
		const networkClassified =
			outcome.retryable === true ||
			failureClass === "connect-timeout" ||
			failureClass === "network" ||
			failureClass === "flood";
		if (networkClassified) {
			if (outcome.retryAfter != null)
				this.lastCapturedRetryAfterSeconds = outcome.retryAfter;
			const retried = await sendWithRetry(
				chunk,
				metadata,
				(c: string, md: Metadata) =>
					this.restSendWithLinkPreviewPolicy(chatId, c, md),
				{ maxRetries: 2 },
			);
			if (retried.success) return retried;
			return this.restSendWithLinkPreviewPolicy(
				chatId,
				DELIVERY_FAILED_NOTICE,
				metadata,
			);
		}
		if (failureClass === "formatting") {
			return this.restSendWithLinkPreviewPolicy(
				chatId,
				plainTextFallbackBody(chunk),
				metadata,
			);
		}
		return outcome;
	}

	/**
	 * THE session-scoped formatting ladder — ONE instance so the rich
	 * downgrade latch persists across chunks/sends (§10.1 probe-once). Tier
	 * bindings ARE the dual-path split: tier 2 (REST) CONVERTS; tier 3 plain
	 * resends stripped content; the native-stream tier never enters this
	 * ladder at all.
	 */
	private ensureFormatLadder(): FormattingLadder {
		if (this.formatLadder === null) {
			const transports: FormattingTransport = {
				tryRich: (content, metadata) => this.wireRich(content, metadata),
				sendConverted: (content, metadata) =>
					this.restSendWithLinkPreviewPolicy(
						this.ladderChatId,
						convertMarkdownToMrkdwn(content),
						metadata,
					),
				sendPlain: (content, metadata) =>
					this.restSendWithLinkPreviewPolicy(
						this.ladderChatId,
						content,
						metadata,
					),
			};
			this.formatLadder = new FormattingLadder(transports, {
				log: (m, meta) => this.logger?.warn?.(m, meta),
			});
		}
		return this.formatLadder;
	}

	/** Raw REST send lane (ladder/chokepoint callers pre-apply transforms). */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		return this.restSendWithLinkPreviewPolicy(chatId, content, metadata);
	}

	/**
	 * THE text-send REST lane. DEC-034(iii): link-preview suppression is a
	 * TEXT-SEND-only metadata flag (chat.postMessage unfurl parity) — applied
	 * on every send here and at the chokepoint's send binding; native draft/
	 * seal frames and rich payloads are NOT text sends and never carry it.
	 */
	private restSendWithLinkPreviewPolicy(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const md: Metadata = { link_preview_suppressed: true, ...metadata };
		return this.rest.transmitSend(chatId, content, md);
	}

	protected override wireEdit(
		chatId: string,
		_messageId: string,
		content: string,
		opts: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		// REST edit lane CONVERTS the dialect (§10.2 chat.update parity).
		return this.rest.transmitEdit(
			chatId,
			_messageId,
			convertMarkdownToMrkdwn(content),
			{ finalize_edit: opts.finalize },
		);
	}

	protected override async wireRich(
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		if (!this.rest.hasScript("rich")) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return this.rest.transmitRich("__rich__", content, metadata);
	}

	// ── native *Stream plane (RAW markdown — §10.2) ────────────────────────

	protected override async wireDraft(
		args: DraftFrameArgs,
	): Promise<SendResult> {
		// A23 latch: latched ⇒ fall back WITHOUT touching the wire (skip the
		// doomed roundtrip entirely — attempt counts must prove it).
		if (this.nativeStreamLatch.shouldSkipNative()) {
			return { success: false, error: "native streaming unsupported" };
		}
		this.nativeStreamLatch.wireAttempts += 1;

		// RAW standard markdown UNCONVERTED — converting centrally violates
		// §5 invariant 1; the dialect conversion belongs to the REST path.
		const text = args.content;
		const meta: Metadata = { ...(args.metadata ?? {}) };
		let stream = this.openStreams.get(args.chatId);

		if (stream !== undefined && stream.draftId !== args.draftId) {
			// New segment while a prior stream dangles — seal the old one so it
			// doesn't hang with a live-typing indicator (Hermes parity).
			await this.sealNativeStream(args.chatId, stream.draftId, stream.sent, {});
			stream = undefined;
		}

		if (stream === undefined) {
			const started = await this.rest.transmitDraft(
				args.chatId,
				args.draftId,
				text,
				false,
				{ ...meta, stream_op: "start" },
			);
			if (!started.success) {
				this.nativeStreamLatch.maybeLatch(started.error ?? "");
				return started;
			}
			this.openStreams.set(args.chatId, {
				ts: started.messageId ?? `stream-${args.draftId}`,
				draftId: args.draftId,
				sent: text,
			});
			return started;
		}

		if (text === stream.sent) return { success: true, messageId: stream.ts };
		if (!text.startsWith(stream.sent)) {
			// Accumulator rewritten mid-segment: fail the frame so the consumer
			// degrades to the edit path; seal the dangling stream first.
			await this.sealNativeStream(args.chatId, stream.draftId, stream.sent, {});
			this.openStreams.delete(args.chatId);
			return { success: false, error: "stream prefix mismatch" };
		}
		const delta = text.slice(stream.sent.length);
		const appended = await this.rest.transmitDraft(
			args.chatId,
			args.draftId,
			delta,
			false,
			{ ...meta, stream_op: "append" },
		);
		if (!appended.success) {
			this.nativeStreamLatch.maybeLatch(appended.error ?? "");
			this.openStreams.delete(args.chatId);
			return appended;
		}
		stream.sent = text;
		return { success: true, messageId: stream.ts };
	}

	/**
	 * `_seal_stream` parity: append ONLY the unsent suffix guarded by
	 * startswith; the sealed stream IS the message (its ts becomes the wire
	 * message identity). Records the seal op with the DELTA when appending
	 * (suffix-only transmission is observable), else the full final.
	 */
	private async sealNativeStream(
		chatId: string,
		draftId: number,
		finalText: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const stream = this.openStreams.get(chatId);
		this.openStreams.delete(chatId);
		const decision =
			stream !== undefined && stream.draftId === draftId
				? sealSuffix(stream.sent, finalText)
				: ({ kind: "rewrite" } as const);
		const sealContent =
			decision.kind === "append"
				? decision.delta
				: decision.kind === "none"
					? ""
					: finalText;
		const sealed = await this.rest.transmitDraft(
			chatId,
			draftId,
			sealContent.length > 0 ? sealContent : finalText,
			true,
			metadata,
		);
		if (!sealed.success) {
			this.nativeStreamLatch.maybeLatch(sealed.error ?? "");
			return sealed;
		}
		return {
			success: true,
			messageId: sealed.messageId ?? stream?.ts ?? `sealed-${draftId}`,
		};
	}

	// ── capability probes ───────────────────────────────────────────────────────

	override supportsDraftStreaming(chatType?: string | undefined): boolean {
		// A23: the latch answers FIRST — a latched session never streams.
		if (this.nativeStreamLatch.unsupported) return false;
		return chatType === undefined || chatType === "dm";
	}

	// ── identity probes (token lock; missing-secret sibling) ──────────

	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		if (!this.lockHeld) {
			const first = this.acquireCredentialLock(
				this.lockManager,
				"bot-token",
				"cred-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.acquireCredentialLock(
				this.lockManager,
				"bot-token",
				"cred-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf("bot-token", "cred-1");
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}

	buildMissingSecretSibling(): PersistentWsAdapter {
		return new PersistentWsAdapter({
			manifestName: `${this.manifestName}-no-secret`,
			transport: this.transportFactory,
			rest: this.rest,
			clock: this.clock,
			requiresEnv: [{ name: WS_REQUIRED_SECRET }],
			secretReader: () => undefined,
		});
	}

	lifecycleSnapshot() {
		return this.lifecycle.statusSnapshot();
	}

	/**
	 * Per-chat length pair (§6.3/A15): chats whose id names "utf16" front a
	 * Slack-class platform — budget 30 CODE UNITS. Budget AND unit move
	 * together through THE one chat resolution.
	 */
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
}

/** Re-export for subject wiring convenience. */
export { FakeWsServer };
