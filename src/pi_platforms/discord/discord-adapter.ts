// pi_platforms/discord/discord-adapter — THE Discord platform adapter on the
// persistent-ws transport family (DEC-002 ws column; roadmap Phase 6 census
// port). Built on the pi_platforms kit + pi_gateway seams; the Gateway v10
// fake stands in for the vendor socket headlessly.
//
// SHAPE DELTAS realized here (vs the persistent-ws reference adapter):
//   - Resume is SESSION+SEQ based: RESUME{op:6, session_id, seq} replays every
//     dispatch with s greater than the acked sequence — there is NO event-id
//     cursor. A dead/unresumable session draws INVALID_SESSION(op:9,d:false)
//     and the client re-IDENTIFYs; the outage window is then recovered by the
//     A13 missed-dispatch sweep (recovery-ledger.ts), not by gateway replay.
//   - Application-level heartbeats (op:1 → op:11 ACK) with the A13 liveness
//     watchdog (ack-age/liveness knobs, threshold-2 reap, ping-safety rule).
//   - Native "streaming" = cumulative EDITS of one message (Hermes streams
//     Discord turns by editing in place, truncating at the length cap with a
//     saturated preview, splitting on finalize — adapter.py:3760-3847).
//   - Q17 rate buckets gate BEFORE egress (rate-buckets.ts manifest data).
//
// Family machinery reused per Phase-6 heuristic 2 (ports inherit the
// reference; deltas written fresh): ReconnectLadder, EventDeduplicator,
// ManualClock, sealSuffix + GFM-table fence rendering. Slack-specific dialect
// conversion is NOT ported — Discord renders standard markdown natively; see
// convertGfmToDiscordMarkdown for the scoped exception (tables).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/discord/adapter.py — admission order (:1553-1646),
//     auto-thread continuity (:8160-8206, #51057 pre-seed, #20243 abort),
//     thread naming (:7200-7216), typing loop (:5582-5644), liveness knobs
//     (:1130-1146), recovery/backfill (:2478-2940), ping safety (:2849-2860,
//     :519-552), length caps (:1045-1057), streaming-edit overflow
//     (:3760-3847), component caps (:87-91, :7667-7670).
//   plugins/platforms/discord/recovery.py — ledger status machine.

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
import type { SessionSource } from "../../pi_gateway/resolution/session-key.js";
import { buildSessionKey } from "../../pi_gateway/resolution/session-key.js";
import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	TokenLockManagerSeam,
	chunkWithFenceCarry,
	classifySendError,
	sendWithRetry,
	plainTextFallbackBody,
	DELIVERY_FAILED_NOTICE,
	resolveEnablement,
	codePointLen,
	utf16Len,
	assembleInteractiveMessage,
	type CapabilityManifest,
	type EnvVarSpec,
	type ScopedSecretReader,
	type TransportShape,
	type LengthUnit,
} from "../kit/index.js";
import { FormattingLadder } from "../kit/formatting-ladder.js";
import type { FormattingTransport } from "../kit/formatting-ladder.js";

import { EventDeduplicator } from "../persistent-ws/event-cursor.js";
import {
	ReconnectLadder,
	type ReconnectLadderOptions,
} from "../persistent-ws/reconnect-ladder.js";
// Family-shared GFM-table fence rendering (pure function; Phase-6 heuristic 2).
import {
	sealSuffix,
	renderTableAsFencedMonospace,
} from "../persistent-ws/dual-path-markdown.js";

import type { SleepFn, NowFn } from "./clock.js";
import type {
	GatewayClientSocket,
	GatewayCloseInfo,
	GatewayConnectionFactory,
	GatewayFrame,
	GatewaySocketListener,
} from "./gateway-fake.js";
import { RateBucketLedger } from "./rate-buckets.js";
import {
	DiscordRecoveryLedger,
	isDownNoticeContent,
} from "./recovery-ledger.js";
import {
	ADMISSIBLE_MESSAGE_TYPES,
	AUTO_THREAD_DEFAULT,
	BUTTON_LABEL_LIMIT,
	BUTTONS_PER_ROW,
	COMPONENT_MAX_ROWS,
	DISCORD_MANIFEST,
	GATEWAY_OPCODES as OP,
	HEARTBEAT_ACK_MAX_AGE_SECONDS,
	LIVENESS_FAILURE_THRESHOLD,
	LIVENESS_INTERVAL_SECONDS,
	ALLOWED_MENTIONS_DEFAULTS,
	CHANNEL_TYPE_FORUM,
	DISCORD_IDENTIFY_INTENTS,
	MAX_SPLIT_MESSAGES,
	MESSAGE_LENGTH_MAX,
	NATIVE_STREAM_GATE_MARKERS,
	FORUM_THREAD_NAME_FALLBACK,
	FORUM_THREAD_NAME_MAX_CHARS,
	REQUIRE_MENTION_DEFAULT,
	RETRY_AFTER_FLOOR_SECONDS,
	RATE_LIMIT_SLEEP_CAP_SECONDS,
	SELECT_FIELD_LIMIT,
	SELECT_MAX_OPTIONS,
	SPLIT_THRESHOLD,
	TYPING_INTERVAL_SECONDS,
	THREAD_AUTO_ARCHIVE_DEFAULT,
	THREAD_NAME_FALLBACK,
	THREAD_NAME_MAX_UTF16_UNITS,
	type AllowBots,
	type RateBucketSpec,
	type RateRouteOp,
} from "./manifest.js";

/** Structural REST plane (locally declared — adapter imports NOTHING from
 * conformance; the test wraps the harness wire in the subject). */
export interface DiscordRestPlane {
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
	/**
	 * POST /channels/{starter_message_id}/threads — the STARTER ID IS THE PATH
	 * PARAMETER (adapter.py:_auto_create_thread :7242); body carries name +
	 * auto_archive_duration only. Returns the new thread id.
	 */
	transmitThreadCreate(
		starterMessageId: string,
		name: string,
		metadata: Metadata,
	): Promise<SendResult>;
	/**
	 * PUT/DELETE /channels/{id}/messages/{mid}/reactions/{emoji}/@me —
	 * per-turn processing/result emojis (adapter.py:_add_reaction :3328 /
	 * _remove_reaction :3339).
	 */
	transmitReaction?(
		channelId: string,
		messageId: string,
		emoji: string,
		action: "add" | "remove",
	): Promise<SendResult>;
	/**
	 * GET /channels/{id} — resolve the vendor channel-type integer; the input
	 * to forum-parent detection (adapter.py:_is_forum_parent :7892 — type 15).
	 * OPTIONAL: planes that cannot answer it simply never route forum lanes
	 * (every target treated as a normal channel).
	 */
	resolveChannelType?(chatId: string): Promise<number | null>;
	/**
	 * POST /channels/{forum_channel_id}/threads — create a forum POST whose
	 * starter message carries the content (adapter.py:_send_to_forum :3593,
	 * forum_channel.create_thread). Returns the STARTER-message SendResult;
	 * `threadId` carries the new thread id the follow-up chunks are sent into.
	 */
	transmitForumThreadCreate?(
		forumChannelId: string,
		name: string,
		starterContent: string,
		metadata: Metadata,
	): Promise<SendResult & { threadId?: string | undefined }>;
	transmitTyping(chatId: string, metadata: Metadata): Promise<SendResult>;
	/** Interaction callback ack (deferred-update parity). */
	transmitInteractionAck(
		interactionId: string,
		kind: string,
		metadata: Metadata,
	): Promise<SendResult>;
	hasScript(opKind: "send" | "edit" | "draft" | "seal" | "rich"): boolean;
}

/** REST history fetch seam behind the A13 missed-dispatch sweep. */
export interface HistoryProvider {
	fetchRecent(
		channelId: string,
		opts: { afterMessageId: string | null; limit: number },
	): Promise<
		Array<{ id: string; channelId: string; authorId: string; text: string }>
	>;
}

/** Injected timing seam (structurally satisfied by ManualClock). */
export interface AdapterClock {
	nowMs: NowFn;
	sleepMs: SleepFn;
}

/** Command registry — the shared five-command conformance registry. */
export const DISCORD_REGISTRY: CommandRegistry = [
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

export const DISCORD_REQUIRED_SECRET = "DISCORD_BOT_TOKEN";

/** Auto-thread abort notice — visible channel warning paired with the
 * abort (adapter.py :8196-8206; #20243). */
const AUTO_THREAD_FAILURE_NOTICE =
	"⚠︁ could not create a thread for this message, so it was not " +
	"processed. Please retry.";

const DEFAULT_ACK_MAX_AGE_MS = HEARTBEAT_ACK_MAX_AGE_SECONDS * 1000;
const DEFAULT_LIVENESS_INTERVAL_MS = LIVENESS_INTERVAL_SECONDS * 1000;

export type DiscordRunPhase =
	| "new"
	| "connecting"
	| "identifying"
	| "live"
	| "reconnect-scheduled"
	| "stopped";

export interface DiscordAdapterDeps {
	manifestName?: string | undefined;
	capabilities?: Partial<CapabilityManifest> | undefined;
	logger?: StreamLogger | undefined;
	transport: GatewayConnectionFactory;
	rest: DiscordRestPlane;
	clock: AdapterClock;
	requiresEnv?: readonly EnvVarSpec[] | undefined;
	secretReader?: ScopedSecretReader | undefined;
	botUserId?: string | undefined;
	scalarMaxUnits?: number | undefined;
	/** Relay-shaped per-chat descriptors (§6.3 pair travels together). */
	perChatDescriptors?: ReadonlyMap<
		string,
		{ maxMessageLength?: number | undefined; lenUnit?: LengthUnit | undefined }
	>;
	streamIsMessageChatIds?: ReadonlySet<string> | undefined;
	/** Q17 gate-before-egress toggle (default ON; production table). */
	rateGate?: boolean | undefined;
	rateBuckets?: readonly RateBucketSpec[] | undefined;
	ladder?: ReconnectLadderOptions | undefined;
	dedupTtlMs?: number | undefined;
	autoThread?: boolean | undefined;
	requireMention?: boolean | undefined;
	ignoreNoMention?: boolean | undefined;
	freeResponseChannels?: ReadonlySet<string> | undefined;
	noThreadChannels?: ReadonlySet<string> | undefined;
	allowBots?: AllowBots | undefined;
	historyProvider?: HistoryProvider | undefined;
	ackMaxAgeMs?: number | undefined;
	livenessIntervalMs?: number | undefined;
	livenessFailureThreshold?: number | undefined;
}

interface OpenStreamState {
	messageId: string;
	draftId: number;
	/** Cumulative RAW bytes streamed so far (§5 invariant 1). */
	sent: string;
}

/**
 * The Discord adapter. Transport + formatting + capabilities; ALL
 * policy-shaped machinery comes from the kit/base.
 */
export class DiscordAdapter
	extends BasePlatformAdapter
	implements StreamEgressAdapter
{
	protected readonly rest: DiscordRestPlane;
	protected readonly clock: AdapterClock;
	private readonly transportFactory: GatewayConnectionFactory;
	/**
	 * Ground bot identity. Injected deps value is only the FALLBACK — Hermes
	 * grounds self-filter/mention detection in client.user, which the gateway
	 * populates at READY (adapter.py:on_ready :1391, _is_mentioned :6746); the
	 * READY dispatch adopts d.user.id before any MESSAGE_CREATE can arrive.
	 */
	private botUserId: string;
	private readonly sealChats: ReadonlySet<string>;

	// ── gateway session state ──────────────────────────────────────────────
	private socket: GatewayClientSocket | null = null;
	private phase: DiscordRunPhase = "new";
	private running = false;
	private reconnectPending = false;
	private watchdogGeneration = 0;
	private heartbeatGeneration = 0;
	/** Session identity for RESUME (the shape-delta counterpart of a cursor). */
	sessionId: string | null = null;
	/** Highest sequence ACCEPTED downstream (advanced ack-after-process). */
	lastAckedSeq: number | null = null;
	/** Highest sequence OBSERVED on the wire (heartbeat payload). */
	private lastSeenSeq: number | null = null;
	private lastHeartbeatAckAtMs: number | null = null;
	private livenessFailures = 0;
	/** Set when delivery observed a seq jump or an unresumable reconnect. */
	private pendingRecoverySweep = false;

	readonly reconnectLadder: ReconnectLadder;
	private readonly dedup: EventDeduplicator;
	readonly ledger: DiscordRecoveryLedger;
	readonly rateLedger: RateBucketLedger;
	readonly nativeStreamLatch: DiscordNativeStreamLatch;

	get dedupSuppressedCount(): number {
		return this.dedup.suppressedCount;
	}
	/** Delivered (post-dedup, post-filter) inbound message ids, in order. */
	readonly inboundLog: string[] = [];
	readonly reconnectLog: { delayMs: number; authoritative: boolean }[] = [];
	lastCapturedRetryAfterSeconds: number | null = null;
	readonly sweepLog: Array<{
		scanned: number;
		dispatched: number;
		trigger: string;
	}> = [];
	readonly threadCreations: Array<{
		channelId: string;
		threadId: string;
		name: string;
		initiatingMessageId: string;
	}> = [];

	// ── config (manifest defaults; deps override) ───────────────────────────
	private readonly autoThread: boolean;
	private readonly requireMention: boolean;
	private readonly ignoreNoMention: boolean;
	private readonly freeResponseChannels: ReadonlySet<string>;
	private readonly noThreadChannels: ReadonlySet<string>;
	private readonly allowBotsMode: AllowBots;
	private readonly rateGateEnabled: boolean;
	private readonly ackMaxAgeMs: number;
	private readonly livenessIntervalMs: number;
	private readonly livenessThreshold: number;
	/** Threads the bot participated in (mention-gate bypass parity). */
	private readonly participatedThreads = new Set<string>();
	private readonly seenChannelIds = new Set<string>();

	// ── egress machinery ────────────────────────────────────────────────────
	private readonly cp: EgressChokepoint;
	private formatLadder: FormattingLadder | null = null;
	private ladderChatId = "";
	/** Continuation anchor for the NEXT ladder send (split-chunk threading). */
	private ladderContinuationOf: string | null = null;
	/** chatId → forum verdict cache (_is_forum_parent probes once per chat). */
	private readonly forumLaneCache = new Map<string, boolean>();
	/** chatId → thread of the most recent forum post (follow-up chunk lane). */
	private readonly forumOpenPosts = new Map<string, string>();
	private readonly openStreams = new Map<string, OpenStreamState>();
	private readonly descriptors: ReadonlyMap<
		string,
		{ maxMessageLength?: number | undefined; lenUnit?: LengthUnit | undefined }
	>;

	// ── interactive surfaces (§9; DEC-016) ─────────────────────────────────
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
	readonly interactionAudit: Array<{
		interactionId: string;
		actionId: string;
		answerKind: string;
		ackLatencyMs: number;
	}> = [];

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
	private readonly historyProvider: HistoryProvider | undefined;

	constructor(deps: DiscordAdapterDeps) {
		super({
			manifestName: deps.manifestName ?? DISCORD_MANIFEST.name,
			capabilities: {
				// 04 §2: slack, discord → splits_long_messages True (native chunking).
				splitsLongMessages: true,
				typedCommandPrefix: "/",
				...(deps.capabilities ?? {}),
			},
			scalarMaxUnits: deps.scalarMaxUnits ?? MESSAGE_LENGTH_MAX,
			logger: deps.logger,
		});
		this.transportFactory = deps.transport;
		this.rest = deps.rest;
		this.clock = deps.clock;
		this.botUserId = deps.botUserId ?? "bot-self";
		this.sealChats = deps.streamIsMessageChatIds ?? new Set<string>();
		this.descriptors = deps.perChatDescriptors ?? new Map();
		this.autoThread = deps.autoThread ?? AUTO_THREAD_DEFAULT;
		this.requireMention = deps.requireMention ?? REQUIRE_MENTION_DEFAULT;
		this.ignoreNoMention = deps.ignoreNoMention ?? true;
		this.freeResponseChannels = deps.freeResponseChannels ?? new Set<string>();
		this.noThreadChannels = deps.noThreadChannels ?? new Set<string>();
		this.allowBotsMode = deps.allowBots ?? "none";
		this.rateGateEnabled = deps.rateGate ?? true;
		this.ackMaxAgeMs = deps.ackMaxAgeMs ?? DEFAULT_ACK_MAX_AGE_MS;
		this.livenessIntervalMs =
			deps.livenessIntervalMs ?? DEFAULT_LIVENESS_INTERVAL_MS;
		this.livenessThreshold =
			deps.livenessFailureThreshold ?? LIVENESS_FAILURE_THRESHOLD;
		this.historyProvider = deps.historyProvider;
		this.rateLedger = new RateBucketLedger({
			nowMs: deps.clock.nowMs,
			...(deps.rateBuckets !== undefined ? { buckets: deps.rateBuckets } : {}),
		});
		this.dedup = new EventDeduplicator({
			...(deps.dedupTtlMs !== undefined ? { ttlMs: deps.dedupTtlMs } : {}),
			nowMs: deps.clock.nowMs,
		});
		this.ledger = new DiscordRecoveryLedger({ nowMs: deps.clock.nowMs });
		this.nativeStreamLatch = new DiscordNativeStreamLatch();
		this.reconnectLadder = new ReconnectLadder({
			...(deps.ladder ?? {}),
			sleep: deps.ladder?.sleep ?? ((ms) => this.clock.sleepMs(ms)),
		});

		if (deps.requiresEnv && deps.secretReader) {
			const enablement = resolveEnablement(
				{
					name: this.manifestName,
					description: DISCORD_MANIFEST.description,
					transportShape: "ws" satisfies TransportShape,
					requiresEnv: deps.requiresEnv,
					capabilities: {},
				},
				deps.secretReader,
			);
			if (!enablement.enabled && enablement.reason) {
				this.lifecycle.disable(enablement.reason);
			} else {
				for (const spec of deps.requiresEnv) {
					const value = deps.secretReader(spec.name);
					if (value !== undefined) this.registerLogSecret(value);
				}
			}
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: (chatId) => this.sealChats.has(String(chatId)),
			transmitSend: async (chatId, content, metadata) =>
				this.restSendDiscord(chatId, content, metadata),
			transmitEdit: async (chatId, messageId, content, opts) =>
				this.gatedEdit(chatId, messageId, content, {
					finalize_edit: opts.finalize,
				}),
			transmitSeal: async (_key, chatId, draftId, content, metadata) =>
				this.sealStreamingEdit(chatId, draftId, content, metadata),
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

	// ── runner wiring passthrough (reference-subject parity) ───────────────

	/** The GROUNDED bot id (READY adoption wins over the injected fallback). */
	get resolvedBotUserId(): string {
		return this.botUserId;
	}

	override attachGuard(
		deps: {
			registry: CommandRegistry;
			messageHandler: MessageHandler;
			sendReply: (chatId: string, text: string) => Promise<void>;
		},
		opts: {
			spawner?: TaskSpawner | undefined;
			hasPendingClarify?: ((sessionKey: string) => boolean) | undefined;
		} = {},
	): void {
		super.attachGuard(deps, opts);
	}

	private readonly isMessageChats = new Set<string>(["__none__"]);
	markStreamIsMessage(chatId: string): void {
		(this.sealChats as Set<string>).add(chatId);
		this.isMessageChats.add(chatId);
	}
	streamIsMessageForChat(chatId: string): boolean {
		return this.sealChats.has(String(chatId));
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

	async armNativeStream(chatId: string, draftId: number): Promise<void> {
		await this.sendDraft({ chatId, draftId, content: "" });
	}

	async transientRichOutcome(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		void chatId;
		const ladder = new FormattingLadder({
			tryRich: async () => ({
				success: false,
				error: "socket hang up",
				retryable: true,
			}),
			sendConverted: async (_c, md) => {
				void md;
				return { success: false, error: "SHOULD-NOT-HAPPEN" };
			},
			sendPlain: async () => ({ success: false, error: "SHOULD-NOT-HAPPEN" }),
		});
		return ladder.sendText(content, {});
	}

	attachStandardGuard(spawner?: TaskSpawner): void {
		this.attachGuard(
			{
				registry: DISCORD_REGISTRY,
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
					if (this.turnDriver !== null) {
						return this.turnDriver(event, text);
					}
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
		if (senderId === this.botUserId) return;
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	doorAudit() {
		return this.cp.audit;
	}

	// ── gateway lifecycle ──────────────────────────────────────────────────

	get currentPhase(): DiscordRunPhase {
		return this.phase;
	}

	get isLive(): boolean {
		return (
			this.socket !== null &&
			this.socket.readyState === 1 &&
			this.phase === "live"
		);
	}

	async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		this.running = true;
		this.phase = "connecting";
		const opened = await this.openSocket();
		if (opened) this.startWatchdog();
		return opened;
	}

	async disconnect(): Promise<void> {
		this.running = false;
		this.phase = "stopped";
		this.watchdogGeneration += 1;
		this.heartbeatGeneration += 1;
		for (const chatId of [...this.typingLoops.keys()]) this.stopTyping(chatId);
		const sock = this.socket;
		this.socket = null;
		sock?.close(1000, "adapter shutdown");
	}

	runWatchdogTick(): void {
		this.watchdogTick();
	}

	private openSocket(): Promise<boolean> {
		if (this.socket !== null && this.socket.readyState === 1) {
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
			const listener: GatewaySocketListener = {
				onOpen: () => settleOnce(true),
				onFrame: (frame) => void this.onGatewayFrame(frame),
				onClose: (info: GatewayCloseInfo) => {
					this.onSocketClose(info);
					settleOnce(false);
				},
				onError: () => {
					/* close follows an error on this plane */
				},
			};
			this.socket = this.transportFactory.connect(listener);
		});
	}

	/**
	 * One gateway frame. HELLO arms the heartbeat loop and fires the
	 * IDENTIFY-or-RESUME decision (resume whenever a session_id survives —
	 * the seq-based shape delta).
	 */
	private async onGatewayFrame(frame: GatewayFrame): Promise<void> {
		if (frame.op === OP.HELLO) {
			this.phase = "identifying";
			// Fresh-connection health baseline: the liveness probe judges THIS
			// connection, not the dead one's history (A13 first-probe grace).
			this.lastHeartbeatAckAtMs = this.clock.nowMs();
			this.livenessFailures = 0;
			this.startHeartbeatLoop(
				Number(
					(frame.d as { heartbeat_interval?: number })?.heartbeat_interval ??
						45_000,
				),
			);
			this.identifyOrResume();
			return;
		}
		if (frame.op === OP.HEARTBEAT_ACK) {
			this.lastHeartbeatAckAtMs = this.clock.nowMs();
			this.livenessFailures = 0;
			return;
		}
		if (frame.op === OP.RECONNECT) {
			// Server-requested reconnect: session stays resumable — close and
			// ladder into a RESUME.
			this.socket?.close(4000, "server requested reconnect");
			return;
		}
		if (frame.op === OP.INVALID_SESSION) {
			// Unresumable: discard the session, re-IDENTIFY after the ladder,
			// and plan the A13 sweep for the outage window.
			this.sessionId = null;
			this.pendingRecoverySweep = true;
			const sock = this.socket;
			this.socket = null;
			sock?.close(4000, "invalid session — fresh identify");
			return;
		}
		if (frame.op !== OP.DISPATCH) return;
		const t = frame.t ?? "";
		if (t === "READY") {
			const d = (frame.d ?? {}) as {
				session_id?: string;
				user?: { id?: string };
			};
			this.sessionId = d.session_id ?? null;
			// Ground identity BEFORE any MESSAGE_CREATE can dispatch (Hermes
			// grounds self-filter + mention matching in client.user, populated
			// by READY d.user.id — adapter.py:on_ready :1391, _is_mentioned
			// :6746). The injected deps value remains the fallback and is never
			// downgraded by a payload-less READY.
			const readyUserId = d.user?.id;
			if (typeof readyUserId === "string" && readyUserId.length > 0) {
				this.botUserId = readyUserId;
			}
			// Fresh session ⇒ baseline resets to the READY sequence itself.
			this.lastSeenSeq = frame.s ?? null;
			this.lastAckedSeq = frame.s ?? null;
			this.phase = "live";
			this.reconnectLadder.reset();
			if (this.pendingRecoverySweep) {
				this.pendingRecoverySweep = false;
				void this.runMissedDispatchSweep("fresh-identify-after-gap");
			}
			return;
		}
		if (t === "RESUMED") {
			this.phase = "live";
			this.reconnectLadder.reset();
			return;
		}
		if (t === "MESSAGE_CREATE") {
			await this.handleMessageCreate(frame);
			return;
		}
		// Other dispatch types advance the observed sequence only.
		if (typeof frame.s === "number") this.lastSeenSeq = frame.s;
	}

	private identifyOrResume(): void {
		const sock = this.socket;
		if (sock === null) return;
		const token = this.resolveToken();
		if (
			this.sessionId !== null &&
			this.lastAckedSeq !== null &&
			this.phaseWasReconnect()
		) {
			sock.send({
				op: OP.RESUME,
				t: null,
				s: null,
				d: {
					token,
					session_id: this.sessionId,
					seq: this.lastAckedSeq,
				},
			});
			return;
		}
		sock.send({
			op: OP.IDENTIFY,
			t: null,
			s: null,
			d: {
				token,
				// VENDOR WIRE FORM: integer bitmask (discord.py Intents value).
				// A string array never comes online; MESSAGE_CONTENT is REQUIRED
				// or inbound content arrives empty (adapter.py:connect :1345-1353).
				intents: DISCORD_IDENTIFY_INTENTS,
				properties: {
					os: "linux",
					browser: "pi-gateway",
					device: "pi-gateway",
				},
			},
		});
	}

	private resumingSinceClose = false;
	private phaseWasReconnect(): boolean {
		return this.resumingSinceClose;
	}

	private resolveToken(): string {
		return this.secretToken ?? "fake-token";
	}
	private secretToken: string | null = null;
	/** Subject/production wiring registers the resolved token value. */
	registerToken(token: string): void {
		this.secretToken = token;
		this.registerLogSecret(token);
	}

	private async onSocketClose(info: GatewayCloseInfo): Promise<void> {
		this.socket = null;
		this.heartbeatGeneration += 1;
		if (this.phase !== "stopped") this.phase = "reconnect-scheduled";
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
		// A surviving session_id means the next HELLO must RESUME (seq replay);
		// a cleared one re-IDENTIFYs (INVALID_SESSION already flagged the sweep).
		this.resumingSinceClose = this.sessionId !== null;
		const step = await this.reconnectLadder.wait(retryAfterSeconds);
		this.reconnectLog.push({
			delayMs: step.delayMs,
			authoritative: step.authoritative,
		});
		this.reconnectPending = false;
		if (!this.running) return;
		await this.openSocket();
	}

	// ── application-level heartbeat + A13 liveness watchdog ────────────────

	private startHeartbeatLoop(intervalMs: number): void {
		const gen = ++this.heartbeatGeneration;
		void (async () => {
			while (this.running && gen === this.heartbeatGeneration) {
				await this.clock.sleepMs(Math.max(250, intervalMs));
				if (!this.running || gen !== this.heartbeatGeneration) return;
				const sock = this.socket;
				if (sock === null || sock.readyState !== 1) return;
				sock.send({
					op: OP.HEARTBEAT,
					t: null,
					s: null,
					d: this.lastSeenSeq,
				});
			}
		})();
	}

	private startWatchdog(): void {
		const gen = ++this.watchdogGeneration;
		void (async () => {
			while (this.running && gen === this.watchdogGeneration) {
				await this.clock.sleepMs(this.livenessIntervalMs);
				if (!this.running || gen !== this.watchdogGeneration) return;
				this.watchdogTick();
			}
		})();
	}

	/**
	 * ONE liveness pass (A13 `_read_websocket_health` parity): stale heartbeat
	 * ACK age accumulates failures; threshold consecutive failures reap the
	 * wedged socket into the reconnect ladder. Knobs ≤ 0 DISABLE the probe
	 * (ping-safety rule, adapter.py:1903-1918).
	 */
	private watchdogTick(): void {
		if (this.ackMaxAgeMs <= 0 || this.livenessIntervalMs <= 0) return;
		const sock = this.socket;
		if (sock === null || sock.readyState !== 1) {
			if (this.running && !this.reconnectPending)
				void this.scheduleReconnect(null);
			return;
		}
		const now = this.clock.nowMs();
		const stale =
			this.lastHeartbeatAckAtMs === null ||
			now - this.lastHeartbeatAckAtMs > this.ackMaxAgeMs;
		if (stale) {
			this.livenessFailures += 1;
			if (this.livenessFailures >= this.livenessThreshold) {
				this.reapStaleSocket(
					`heartbeat ack stale ${now - (this.lastHeartbeatAckAtMs ?? 0)}ms`,
				);
			}
			return;
		}
		this.livenessFailures = 0;
	}

	private reapStaleSocket(reason: string): void {
		const sock = this.socket;
		this.logger?.warn?.(
			`${this.manifestName}: reaping stale socket — ${reason}`,
		);
		sock?.close(4000, reason);
	}

	// ── MESSAGE_CREATE admission (Hermes order :1553-1646) ─────────────────

	private async handleMessageCreate(frame: GatewayFrame): Promise<void> {
		const s = typeof frame.s === "number" ? frame.s : null;
		const d = (frame.d ?? {}) as Record<string, unknown>;
		const type = Number(d["type"] ?? 0);
		if (!(ADMISSIBLE_MESSAGE_TYPES as readonly number[]).includes(type)) return;
		const author = (d["author"] ?? {}) as { id?: string; bot?: boolean };
		if (author.id === this.botUserId) return; // self/echo filter
		const content = String(d["content"] ?? "");

		// Bot-author policy (allow_bots none|mentions|all, :6697-6700).
		if (author.bot === true) {
			const botMentioned =
				((d["mentions"] ?? []) as Array<{ id?: string }>).some(
					(m) => m.id === this.botUserId,
				) || content.includes(`<@${this.botUserId}>`);
			if (this.allowBotsMode === "none") return;
			if (this.allowBotsMode === "mentions" && !botMentioned) return;
		}

		// In-session SEQ-GAP detection: a jumped sequence means dropped
		// dispatches — flag the sweep (A13 seq-gap recovery).
		if (
			s !== null &&
			this.lastSeenSeq !== null &&
			s > this.lastSeenSeq + 1 &&
			this.phase === "live"
		) {
			this.pendingRecoverySweep = true;
		}
		if (s !== null) this.lastSeenSeq = s;

		const messageId = String(d["id"] ?? "");
		const channelId = String(d["channel_id"] ?? "");
		this.seenChannelIds.add(channelId); // sweep channel universe (A13)
		const threadIdRaw = d["thread_id"];
		const isThread = typeof threadIdRaw === "string" && threadIdRaw.length > 0;
		const mentions = (d["mentions"] ?? []) as Array<{ id?: string }>;
		const mentionedBot = mentions.some((m) => m.id === this.botUserId);
		const referenced = (d["referenced_message"] ?? null) as {
			id?: string;
		} | null;
		const guildId = d["guild_id"];

		// Mention gating (require_mention / participation / DM bypass).
		const inParticipatingThread =
			isThread && this.participatedThreads.has(channelId);
		const explicitlyMentioned =
			mentionedBot || content.includes(`<@${this.botUserId}>`);
		if (!explicitlyMentioned) {
			const dmBypass = guildId === undefined;
			const freeResponse = this.freeResponseChannels.has(channelId);
			if (
				this.ignoreNoMention &&
				!dmBypass &&
				!freeResponse &&
				!inParticipatingThread &&
				this.requireMention
			)
				return;
		}

		// Dedup claim BEFORE dispatch (#4777/#51057 replay safety).
		if (this.dedup.isDuplicate(messageId)) return;
		this.ledger.recordDiscovered(messageId, {
			channelId,
			authorId: String(author.id ?? ""),
			...(isThread ? { threadId: channelId } : {}),
			text: content,
		});
		this.ledger.markStatus(messageId, "queued");

		// ── Auto-thread continuity (DEC-028 end-to-end) ──
		let prospectiveThreadId: string | undefined;
		if (
			this.autoThread &&
			guildId !== undefined &&
			!isThread &&
			type !== 19 &&
			!this.noThreadChannels.has(channelId) &&
			!this.freeResponseChannels.has(channelId)
		) {
			const created = await this.createAutoThread(
				channelId,
				content,
				messageId,
			);
			if (created === null) {
				// Failure pairs a VISIBLE channel notice with the abort — users
				// must know to retry (adapter.py :8196-8206, #20243).
				try {
					await this.restSendDiscord(channelId, AUTO_THREAD_FAILURE_NOTICE, {});
				} catch {
					/* notice is best-effort; the abort stands either way */
				}
				this.ledger.markFailed(messageId);
				return;
			}
			prospectiveThreadId = created.threadId;
			// Pre-seed dedup: thread creation echoes the starter (#51057).
			this.dedup.isDuplicate(created.threadId);
			this.participatedThreads.add(created.threadId);
		}
		if (isThread) this.participatedThreads.add(channelId);

		// SessionSource + THE effective-thread-slot keying (DEC-028).
		const source: SessionSource = {
			platform: this.manifestName,
			chatType: guildId === undefined ? "dm" : isThread ? "thread" : "channel",
			userId: String(author.id ?? ""),
			chatId: channelId,
			...(isThread ? { threadId: channelId } : {}),
			...(prospectiveThreadId !== undefined ? { prospectiveThreadId } : {}),
			...(guildId !== undefined ? { scopeId: String(guildId) } : {}),
		};
		const sessionKey = buildSessionKey(source);

		const event: IncomingEvent = {
			messageType: "text",
			text: this.stripSelfMention(content),
			source: {
				platform: this.manifestName,
				chatType: source.chatType,
				userId: source.userId ?? "",
				chatId: channelId,
				...(isThread ? { threadId: channelId } : {}),
			},
			metadata: {
				gateway_session_key: sessionKey,
				...(prospectiveThreadId !== undefined
					? { prospective_thread_id: prospectiveThreadId }
					: {}),
				...(referenced !== null && referenced.id !== undefined
					? { reply_to_message_id: referenced.id }
					: {}),
			},
		};

		this.ledger.markStatus(messageId, "processing");
		// Turn-admission typing indicator (run.py:5000 send_typing parity) —
		// UP before the emoji REST round-trip so even instant handoffs show
		// life; refreshes every 12s while the turn runs; stopTyping lands at
		// finalize/failure (:20444/:21065).
		this.startTyping(channelId);
		// 👀 processing-start emoji (adapter.py on_processing_start parity).
		await this.onProcessingStart(channelId, messageId);
		try {
			await this.handleIngress(event, sessionKey);
		} catch (err) {
			// Contained: ledger marks failed; seq NOT acked — replay retries.
			this.logger?.error?.(
				`${this.manifestName}: dispatch failed for ${messageId}: ${err instanceof Error ? err.message : String(err)}`,
			);
			await this.onProcessingComplete(messageId, "failure");
			this.ledger.markFailed(messageId);
			this.stopTyping(channelId);
			return;
		}
		this.inboundLog.push(messageId);
		this.ledger.advanceCursor(channelId, messageId);
		// ✅ completion swap (adapter.py :3376-3381).
		await this.onProcessingComplete(messageId, "success");
		this.stopTyping(channelId);
		if (s !== null) this.lastAckedSeq = Math.max(this.lastAckedSeq ?? s, s);
		if (this.pendingRecoverySweep && s !== null) {
			// Process the CURRENT frame fully before sweeping the gap behind it.
			this.pendingRecoverySweep = false;
			void this.runMissedDispatchSweep("seq-gap-in-session");
		}
	}

	// ── per-turn emoji acks (adapter.py:_add_reaction :3328 / :3376-3381) ──

	/** 👀 on dispatch → removed, then ✅/❌ on completion. */
	static readonly REACTION_PROCESSING = "👀";
	static readonly REACTION_SUCCESS = "✅";
	static readonly REACTION_FAILURE = "❌";

	/** messageId → channel that received the 👀 (swap anchor). */
	private readonly pendingReactions = new Map<string, string>();

	/** Processing-start emoji: PUT 👀 on the triggering message. */
	async onProcessingStart(channelId: string, messageId: string): Promise<void> {
		if (!channelId || !messageId) return;
		try {
			const fired = await this.rest.transmitReaction?.(
				channelId,
				messageId,
				DiscordAdapter.REACTION_PROCESSING,
				"add",
			);
			if (fired?.success) {
				this.pendingReactions.set(messageId, channelId);
				// The recovery ledger models emoji acks — feed it (:2849-2852:
				// emoji-only acks recorded, never completion).
				this.ledger.markEmojiAck(messageId);
			}
		} catch {
			/* reaction failures never break processing */
		}
	}

	/** Completion swap: remove 👀, then ✅ (success) or ❌ (failure). */
	async onProcessingComplete(
		messageId: string,
		outcome: "success" | "failure",
	): Promise<void> {
		const channelId = this.pendingReactions.get(messageId);
		if (channelId === undefined) return;
		this.pendingReactions.delete(messageId);
		try {
			await this.rest.transmitReaction?.(
				channelId,
				messageId,
				DiscordAdapter.REACTION_PROCESSING,
				"remove",
			);
		} catch {
			/* swallowed */
		}
		try {
			await this.rest.transmitReaction?.(
				channelId,
				messageId,
				outcome === "success"
					? DiscordAdapter.REACTION_SUCCESS
					: DiscordAdapter.REACTION_FAILURE,
				"add",
			);
		} catch {
			/* swallowed */
		}
	}

	private stripSelfMention(content: string): string {
		return content
			.replaceAll(`<@${this.botUserId}>`, "")
			.replaceAll(`<@!${this.botUserId}>`, "")
			.trim();
	}

	// ── auto-thread creation ───────────────────────────────────────────────

	private async createAutoThread(
		channelId: string,
		content: string,
		initiatingMessageId: string,
	): Promise<{ threadId: string; name: string } | null> {
		for (let attempt = 0; attempt < 2; attempt++) {
			if (attempt > 0) await this.clock.sleepMs(750);
			const verdict = this.gateRoute("thread-create", channelId);
			if (!verdict.allowed) continue;
			const created = await this.rest.transmitThreadCreate(
				initiatingMessageId, // vendor puts the starter id IN THE PATH
				deriveThreadName(content),
				{ auto_archive_duration: THREAD_AUTO_ARCHIVE_DEFAULT },
			);
			if (created.success && created.messageId) {
				this.threadCreations.push({
					channelId,
					threadId: created.messageId,
					name: deriveThreadName(content),
					initiatingMessageId,
				});
				return { threadId: created.messageId, name: deriveThreadName(content) };
			}
		}
		return null;
	}

	// ── A13 missed-dispatch sweep ──────────────────────────────────────────

	/**
	 * Fresh-IDENTIFY recovery: the gateway will NOT replay an unresumable
	 * session's window, so the ledger + REST history sweep re-admits missed
	 * messages through the NORMAL pipeline, exactly-once (dedup claims +
	 * persistently-complete + active-claim skips; max_dispatches bounded).
	 */
	async runMissedDispatchSweep(trigger: string): Promise<void> {
		if (this.historyProvider === undefined) return;
		const channels = [...this.seenChannelIds];
		let scanned = 0;
		let dispatched = 0;
		for (const channelId of channels) {
			const history = await this.historyProvider.fetchRecent(channelId, {
				afterMessageId: this.ledger.cursorFor(channelId),
				limit: 100,
			});
			for (const item of history) {
				scanned += 1;
				this.ledger.recordDiscovered(item.id, {
					channelId: item.channelId,
					authorId: item.authorId,
					text: item.text,
				});
			}
		}
		const eligible = this.ledger.candidatesForSweep({
			botAuthorId: this.botUserId,
			maxDispatches: 10,
		});
		for (const candidate of eligible) {
			if (candidate.text !== undefined && isDownNoticeContent(candidate.text))
				continue; // ping safety: down-notices never mask/dispatch
			if (this.dedup.isDuplicate(candidate.messageId)) continue; // exactly-once
			this.ledger.markStatus(candidate.messageId, "processing");
			try {
				await this.handleIngress(
					{
						messageType: "text",
						text: candidate.text ?? "[recovered attachment-only message]",
						source: {
							platform: this.manifestName,
							chatType: "channel",
							userId: "recovered-author",
							chatId: candidate.channelId,
						},
						metadata: { recovered: true },
					},
					`${this.manifestName}:${candidate.channelId}`,
				);
				this.ledger.markResponded(
					candidate.messageId,
					`${candidate.messageId}-resp`,
				);
				this.inboundLog.push(candidate.messageId);
				dispatched += 1;
			} catch {
				this.ledger.markFailed(candidate.messageId);
			}
		}
		this.sweepLog.push({ scanned, dispatched, trigger });
	}

	noteChannelSeen(channelId: string): void {
		this.seenChannelIds.add(channelId);
	}

	// ── interactions (components; DEC-016 registered-handler seam) ─────────

	/**
	 * INTERACTION_CREATE tap: authorize → registry dispatch (parallel
	 * mechanism) + THE single router route (grammar families) → ack INSIDE the
	 * window even when handlers raise; resolved host messages lose their
	 * keyboards.
	 */
	async handleInteraction(input: {
		interactionId: string;
		customId: string;
		clickerId: string;
		messageId?: string | undefined;
	}): Promise<void> {
		const startedAt = Date.now();
		const authorized = this.allowAllClickers;
		let answerKind = "unknown";
		if (authorized) {
			this.actionRegistry.dispatch({
				actionId: input.customId,
				payload: { clicker: input.clickerId },
			});
			const answer = await this.router.route(input.customId, {
				userId: input.clickerId,
				...(input.messageId !== undefined ? { chatId: input.messageId } : {}),
			});
			answerKind = answer.kind;
			if (answer.kind === "resolved" && input.messageId !== undefined) {
				await this.gatedEdit(
					input.messageId,
					input.messageId,
					answer.answerText,
					{ components_removed: true },
				);
			}
		} else {
			answerKind = "unauthorized";
		}
		await this.rest.transmitInteractionAck(input.interactionId, answerKind, {});
		this.interactionAudit.push({
			interactionId: input.interactionId,
			actionId: input.customId,
			answerKind,
			ackLatencyMs: Date.now() - startedAt,
		});
	}

	// ── typing loop (A11 refresh-ping variant) ─────────────────────────────

	private readonly typingLoops = new Map<
		string,
		{ gen: number; active: boolean }
	>();

	startTyping(chatId: string): void {
		const existing = this.typingLoops.get(chatId);
		if (existing?.active === true) return; // duplicate-loop suppression
		const gen = (existing?.gen ?? 0) + 1;
		const loop = { gen, active: true };
		this.typingLoops.set(chatId, loop);
		void (async () => {
			// Hermes' _typing_loop (:5605-5637) posts IMMEDIATELY on entry, THEN
			// refreshes every 12s — a sub-interval turn must still show the
			// indicator. The zero-length first delay realizes "request first,
			// sleep after" while keeping the cadence schedulable on the INJECTED
			// clock (registration stays synchronous with startTyping).
			let delayMs = 0;
			while (loop.active && this.running) {
				await this.clock.sleepMs(delayMs);
				if (!loop.active) return;
				delayMs = TYPING_INTERVAL_SECONDS * 1000;
				const verdict = this.gateRoute("typing", chatId);
				if (!verdict.allowed) {
					delayMs = verdict.retryAfterSeconds * 1000;
					continue;
				}
				const fired = await this.rest.transmitTyping(chatId, {
					typing_op: "refresh",
				});
				if (!fired.success) {
					if (fired.retryable === true && fired.retryAfter != null) {
						// 429 survival: the AUTHORITATIVE delay alone leads to the
						// next post (:5626 sleep(retry_after); continue), KEEP looping.
						delayMs =
							Math.max(
								RETRY_AFTER_FLOOR_SECONDS,
								Math.min(fired.retryAfter, RATE_LIMIT_SLEEP_CAP_SECONDS),
							) * 1000;
						continue;
					}
					loop.active = false; // other failures END the loop
					return;
				}
			}
		})();
	}

	stopTyping(chatId: string): void {
		const loop = this.typingLoops.get(chatId);
		if (loop !== undefined) loop.active = false;
	}

	typingActive(chatId: string): boolean {
		return this.typingLoops.get(chatId)?.active === true;
	}

	// ── rate gating (Q17 gate-before-egress) ───────────────────────────────

	private gateRoute(
		route: RateRouteOp,
		channelId: string,
	): { allowed: true } | { allowed: false; retryAfterSeconds: number } {
		if (!this.rateGateEnabled) return { allowed: true };
		const verdict = this.rateLedger.consume(route, channelId);
		if (verdict.allowed) return { allowed: true };
		return {
			allowed: false,
			retryAfterSeconds: Math.max(
				RETRY_AFTER_FLOOR_SECONDS,
				verdict.retryAfterSeconds,
			),
		};
	}

	recordRateAuthority(
		route: RateRouteOp,
		channelId: string,
		seconds: number,
	): void {
		this.rateLedger.recordAuthority(route, channelId, seconds);
		this.lastCapturedRetryAfterSeconds = Math.max(
			RETRY_AFTER_FLOOR_SECONDS,
			seconds,
		);
	}

	private async restSendDiscord(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		return this.restSendDiscordWithReference(chatId, content, metadata, null);
	}

	/**
	 * THE text-send entry. Forum-parent detection FIRST (Hermes checks
	 * _is_forum_parent at the top of send, adapter.py:3501): type-15 targets
	 * route through the thread-create post lane — Discord REJECTS plain
	 * channel sends into forums.
	 */
	private async restSendDiscordWithReference(
		chatId: string,
		content: string,
		metadata: Metadata,
		continuationOf: string | null,
	): Promise<SendResult> {
		if (await this.isForumParent(chatId)) {
			return this.sendViaForumLane(chatId, content, metadata, continuationOf);
		}
		return this.restSendPlain(chatId, content, metadata, continuationOf);
	}

	/**
	 * Forum-parent probe (adapter.py:_is_forum_parent :7892 — vendor channel
	 * type 15). The verdict caches per chat (forums don't stop being forums);
	 * probe FAILURES are not cached — the next send retries the lookup.
	 */
	private async isForumParent(chatId: string): Promise<boolean> {
		const cached = this.forumLaneCache.get(chatId);
		if (cached !== undefined) return cached;
		if (this.rest.resolveChannelType === undefined) return false;
		let type: number | null;
		try {
			type = await this.rest.resolveChannelType(chatId);
		} catch {
			return false; // transient lookup failure never reroutes a send
		}
		const isForum = type === CHANNEL_TYPE_FORUM;
		this.forumLaneCache.set(chatId, isForum);
		return isForum;
	}

	/**
	 * THE forum-post lane (_send_to_forum :3593 parity). The FIRST chunk of a
	 * delivery CREATES the post: POST /channels/{forum}/threads with the derived
	 * name + starter content; follow-up chunks (continuationOf set) send INTO
	 * the newly created thread, addressed by thread id with no reply reference
	 * (:3634-3640 thread_channel.send). A follow-up with no open post falls
	 * back to creating one — plain sends would be rejected anyway.
	 */
	private async sendViaForumLane(
		chatId: string,
		content: string,
		metadata: Metadata,
		continuationOf: string | null,
	): Promise<SendResult> {
		const openThread =
			continuationOf !== null ? this.forumOpenPosts.get(chatId) : undefined;
		if (openThread !== undefined) {
			return this.restSendPlain(openThread, content, metadata, null);
		}
		const createPost = this.rest.transmitForumThreadCreate;
		if (createPost === undefined) {
			// Fail honestly instead of a doomed plain send Discord rejects.
			return {
				success: false,
				error:
					"forum channel requires thread-create support (type 15 rejects plain sends)",
			};
		}
		const name = deriveForumThreadName(content);
		const gate = this.gateRoute("thread-create", chatId);
		if (!gate.allowed) {
			return {
				success: false,
				error: `rate limited (${gate.retryAfterSeconds.toFixed(2)}s)`,
				retryable: true,
				retryAfter: gate.retryAfterSeconds,
			};
		}
		const created = await createPost.call(this.rest, chatId, name, content, {
			allowed_mentions: { ...ALLOWED_MENTIONS_DEFAULTS },
			link_preview_suppressed: true,
			...metadata,
			forum_post: true,
			thread_name: name,
		});
		if (created.success) {
			const threadId = created.threadId ?? created.messageId ?? null;
			if (threadId !== null) this.forumOpenPosts.set(chatId, threadId);
		}
		return created;
	}

	/**
	 * THE plain-channel text lane. Builds vendor body keys from gateway stamps:
	 * reply_to_message_id → message_reference {message_id,
	 * fail_if_not_exists:false} (_reply_reference_for_send parity :3394-3407);
	 * continuation chunks pass an explicit reference that wins over the reply
	 * stamp (_edit_overflow_split parity :3910-3912). Ping safety rides EVERY
	 * send as vendor-shaped DATA (:519-552); link previews are NOT suppressed —
	 * Hermes never sets suppress_embeds on any text path.
	 */
	private async restSendPlain(
		chatId: string,
		content: string,
		metadata: Metadata,
		continuationOf: string | null,
	): Promise<SendResult> {
		const gate = this.gateRoute("send", chatId);
		if (!gate.allowed) {
			return {
				success: false,
				error: `rate limited (${gate.retryAfterSeconds.toFixed(2)}s)`,
				retryable: true,
				retryAfter: gate.retryAfterSeconds,
			};
		}
		const md: Metadata = {
			allowed_mentions: { ...ALLOWED_MENTIONS_DEFAULTS },
			link_preview_suppressed: true,
			...metadata,
		};
		const replyTo = md["reply_to_message_id"];
		const referenceId =
			continuationOf ?? (typeof replyTo === "string" ? replyTo : undefined);
		if (referenceId !== undefined && referenceId !== "") {
			md["message_reference"] = {
				message_id: referenceId,
				fail_if_not_exists: false,
			};
		}
		delete md["reply_to_message_id"]; // converted to message_reference
		return this.rest.transmitSend(chatId, content, md);
	}

	private async gatedEdit(
		chatId: string,
		messageId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const gate = this.gateRoute("edit", chatId);
		if (!gate.allowed) {
			return {
				success: false,
				error: `rate limited (${gate.retryAfterSeconds.toFixed(2)}s)`,
				retryable: true,
				retryAfter: gate.retryAfterSeconds,
			};
		}
		// The REST edit lane CONVERTS the scoped delta (tables → fenced);
		// emphasis/link bytes stay Discord-native. discord.py Message.edit fills
		// the CLIENT-WIDE safe allowed_mentions default into every PATCH —
		// streamed/edited LLM output can NEVER ping everyone/roles (:3798).
		return this.rest.transmitEdit(
			chatId,
			messageId,
			convertGfmToDiscordMarkdown(content),
			{
				allowed_mentions: { ...ALLOWED_MENTIONS_DEFAULTS },
				...metadata,
			},
		);
	}

	// ── base doors ─────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	/** Native-splitting delivery (splits_long_messages=True parity). */
	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		const policy = this.chatLengthPolicyForChat(chatId);
		const plan = chunkWithFenceCarry(content, policy);
		const results: SendResult[] = [];
		let continuationOf: string | null = null;
		for (let i = 0; i < plan.chunks.length; i++) {
			const chunk = plan.chunks[i] as string;
			this.ladderChatId = chatId;
			// Split continuations thread as REPLIES to the prior chunk so
			// Discord groups long answers (_edit_overflow_split :3910-3912).
			this.ladderContinuationOf = i === 0 ? null : continuationOf;
			const sent = await this.deliverWiredChunk(chatId, chunk, metadata);
			results.push(sent);
			if (sent.success && typeof sent.messageId === "string") {
				continuationOf = sent.messageId;
			}
		}
		this.ladderContinuationOf = null;
		return results;
	}

	private async deliverWiredChunk(
		chatId: string,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const outcome = await this.ensureFormatLadder().sendText(chunk, metadata);
		if (outcome.success) return outcome;
		if (outcome.tier === "rich") return outcome; // transient rich NEVER resent

		const failureClass = classifySendError(new Error(outcome.error ?? ""));
		const networkClassified =
			outcome.retryable === true ||
			failureClass === "connect-timeout" ||
			failureClass === "network" ||
			failureClass === "flood";
		if (networkClassified) {
			if (outcome.retryAfter != null)
				this.lastCapturedRetryAfterSeconds = Math.max(
					RETRY_AFTER_FLOOR_SECONDS,
					outcome.retryAfter,
				);
			const retried = await sendWithRetry(
				chunk,
				metadata,
				(c: string, md: Metadata) => this.restSendDiscord(chatId, c, md),
				{ maxRetries: 2 },
			);
			if (retried.success) return retried;
			return this.restSendDiscord(chatId, DELIVERY_FAILED_NOTICE, metadata);
		}
		if (failureClass === "formatting") {
			return this.restSendDiscord(
				chatId,
				plainTextFallbackBody(chunk),
				metadata,
			);
		}
		return outcome;
	}

	private ensureFormatLadder(): FormattingLadder {
		if (this.formatLadder === null) {
			const transports: FormattingTransport = {
				tryRich: (content, metadata) => this.wireRich(content, metadata),
				sendConverted: (content, metadata) =>
					this.restSendDiscordWithReference(
						this.ladderChatId,
						convertGfmToDiscordMarkdown(content),
						metadata,
						this.ladderContinuationOf,
					),
				sendPlain: (content, metadata) =>
					this.restSendDiscordWithReference(
						this.ladderChatId,
						content,
						metadata,
						this.ladderContinuationOf,
					),
			};
			this.formatLadder = new FormattingLadder(transports, {
				log: (m, meta) => this.logger?.warn?.(m, meta),
			});
		}
		return this.formatLadder;
	}

	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		return this.restSendDiscord(chatId, content, metadata);
	}

	protected override wireEdit(
		chatId: string,
		messageId: string,
		content: string,
		opts: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		return this.gatedEdit(chatId, messageId, content, {
			finalize_edit: opts.finalize,
		});
	}

	protected override async wireRich(
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		if (!this.rest.hasScript("rich")) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return this.rest.transmitRich("__embed__", content, metadata);
	}

	// ── native stream plane: STREAMING EDITS ───────────────────────────────

	protected override async wireDraft(
		args: DraftFrameArgs,
	): Promise<SendResult> {
		if (this.nativeStreamLatch.shouldSkipNative()) {
			return { success: false, error: "native streaming unsupported" };
		}
		this.nativeStreamLatch.wireAttempts += 1;

		let stream = this.openStreams.get(args.chatId);
		if (stream !== undefined && stream.draftId !== args.draftId) {
			await this.sealStreamingEdit(
				args.chatId,
				stream.draftId,
				stream.sent,
				{},
			);
			stream = undefined;
		}

		if (stream === undefined) {
			const gate = this.gateRoute("edit", args.chatId);
			if (!gate.allowed) {
				return {
					success: false,
					error: `rate limited (${gate.retryAfterSeconds.toFixed(2)}s)`,
					retryable: true,
					retryAfter: gate.retryAfterSeconds,
				};
			}
			// START frame carries the FULL RAW accumulator (family wire shape).
			// Every streaming-edit PATCH carries the client-wide safe
			// allowed_mentions default (discord.py Message.edit parity :3798).
			const started = await this.rest.transmitDraft(
				args.chatId,
				args.draftId,
				args.content,
				false,
				{
					allowed_mentions: { ...ALLOWED_MENTIONS_DEFAULTS },
					...(args.metadata ?? {}),
					stream_op: "start",
				},
			);
			if (!started.success) {
				this.nativeStreamLatch.maybeLatch(started.error ?? "");
				return started;
			}
			this.openStreams.set(args.chatId, {
				messageId: started.messageId ?? `stream-${args.draftId}`,
				draftId: args.draftId,
				sent: args.content,
			});
			return started;
		}

		if (args.content === stream.sent)
			return { success: true, messageId: stream.messageId };
		if (!args.content.startsWith(stream.sent)) {
			await this.sealStreamingEdit(
				args.chatId,
				stream.draftId,
				stream.sent,
				{},
			);
			this.openStreams.delete(args.chatId);
			return { success: false, error: "stream prefix mismatch" };
		}
		// APPEND frame carries the RAW suffix delta (prefix-stable discipline).
		const delta = args.content.slice(stream.sent.length);
		stream.sent = args.content;
		const appended = await this.rest.transmitDraft(
			args.chatId,
			args.draftId,
			delta,
			false,
			{
				allowed_mentions: { ...ALLOWED_MENTIONS_DEFAULTS },
				...(args.metadata ?? {}),
				stream_op: "append",
			},
		);
		if (!appended.success) {
			this.nativeStreamLatch.maybeLatch(appended.error ?? "");
			this.openStreams.delete(args.chatId);
			return appended;
		}
		return { success: true, messageId: stream.messageId };
	}

	/**
	 * Seal = the family seal op over the streaming-edit stream: append ONLY the
	 * unsent suffix guarded by startswith; overflow beyond the message cap
	 * splits-and-delivers the tail as fresh messages (`_edit_overflow_split`
	 * parity, adapter.py:3847+).
	 */
	private async sealStreamingEdit(
		chatId: string,
		draftId: number,
		finalText: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const stream = this.openStreams.get(chatId);
		this.openStreams.delete(chatId);
		const messageId =
			stream?.draftId === draftId ? stream.messageId : `draft-${draftId}`;
		const decision =
			stream !== undefined && stream.draftId === draftId
				? sealSuffix(stream.sent, finalText)
				: ({ kind: "rewrite" } as const);

		const gate = this.gateRoute("edit", chatId);
		if (!gate.allowed) {
			return {
				success: false,
				error: `rate limited (${gate.retryAfterSeconds.toFixed(2)}s)`,
				retryable: true,
				retryAfter: gate.retryAfterSeconds,
			};
		}
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
			{
				allowed_mentions: { ...ALLOWED_MENTIONS_DEFAULTS },
				...metadata,
				stream_op: "seal",
			},
		);
		if (!sealed.success) {
			this.nativeStreamLatch.maybeLatch(sealed.error ?? "");
			return sealed;
		}
		// Overflow tail: when the FULL final exceeds the per-chat budget the
		// sealed head cannot hold it — split-and-deliver the remaining chunks,
		// each REPLYING to its predecessor so Discord groups the answer
		// (_edit_overflow_split :3910-3912).
		if (decision.kind !== "append") {
			const plan = chunkWithFenceCarry(
				finalText,
				this.chatLengthPolicyForChat(chatId),
			);
			let continuationOf: string | null = sealed.messageId ?? messageId;
			for (let i = 1; i < plan.chunks.length && i < MAX_SPLIT_MESSAGES; i++) {
				const sent = await this.restSendDiscordWithReference(
					chatId,
					plan.chunks[i] ?? "",
					{
						...metadata,
						split_part: `${i + 1}/${plan.chunks.length}`,
					},
					continuationOf,
				);
				if (sent.success && typeof sent.messageId === "string") {
					continuationOf = sent.messageId;
				}
			}
		}
		return { success: true, messageId: sealed.messageId ?? messageId };
	}

	override supportsDraftStreaming(_chatType?: string | undefined): boolean {
		if (this.nativeStreamLatch.unsupported) return false;
		return true; // streaming edits work in ANY channel class
	}

	// ── §6.3/A15: per-chat descriptor pair moves TOGETHER ──────────────────

	/** Relay-shaped descriptor seeding (production: negotiated descriptors). */
	setChatDescriptor(
		chatId: string,
		descriptor: {
			maxMessageLength?: number | undefined;
			lenUnit?: LengthUnit | undefined;
		},
	): void {
		(
			this.descriptors as Map<
				string,
				{
					maxMessageLength?: number | undefined;
					lenUnit?: LengthUnit | undefined;
				}
			>
		).set(chatId, descriptor);
	}

	/** A13 ping-safety knob: ≤0 DISABLES the liveness probe. */
	setWatchdogAckMaxAgeMs(ms: number): void {
		(this as unknown as { ackMaxAgeMs: number }).ackMaxAgeMs = ms;
	}

	protected override chatDescriptorFor(chatId: string):
		| {
				maxMessageLength?: number | undefined;
				lenUnit?: LengthUnit | undefined;
		  }
		| undefined {
		return this.descriptors.get(chatId);
	}

	// ── interactive card builders (components via kit vocabulary) ──────────

	/**
	 * Exec-approval card: kit grammar builders → Discord component rows under
	 * the manifest caps; plain-content mirror ships alongside (accessibility
	 * fallback, embed-invisibility parity :7489-7494).
	 */
	async sendExecApprovalCard(
		chatId: string,
		opts: {
			approvalId: number;
			builders: Array<{ customId: string; label: string }>;
		},
	): Promise<SendResult> {
		const components = buildComponentRows(opts.builders);
		const mirror =
			opts.builders.map((b) => `• ${b.label}`).join("\n") || "(no choices)";
		return this.send(chatId, mirror, undefined, {
			components,
			interactive_family: "ea",
		});
	}

	// ── identity probes (token lock / missing-secret sibling) ─────────────

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

	buildMissingSecretSibling(): DiscordAdapter {
		return new DiscordAdapter({
			manifestName: `${this.manifestName}-no-secret`,
			transport: this.transportFactory,
			rest: this.rest,
			clock: this.clock,
			requiresEnv: [{ name: DISCORD_REQUIRED_SECRET }],
			secretReader: () => undefined,
		});
	}

	lifecycleSnapshot() {
		return this.lifecycle.statusSnapshot();
	}
}

// ── Discord-native-stream feature-gate latch (A23 family shape) ──────────────

/**
 * Vendor feature-gate markers latch native streaming OFF ONCE per session;
 * later attempts SKIP the wire entirely. Transient failures never latch.
 */
export class DiscordNativeStreamLatch {
	private latched = false;
	private latchReason: string | null = null;
	latchCount = 0;
	wireAttempts = 0;

	get unsupported(): boolean {
		return this.latched;
	}
	get reason(): string | null {
		return this.latchReason;
	}
	shouldSkipNative(): boolean {
		return this.latched;
	}
	maybeLatch(errorText: string): boolean {
		if (this.latched) return false;
		const s = errorText.toLowerCase();
		if (!NATIVE_STREAM_GATE_MARKERS.some((m) => s.includes(m))) return false;
		this.latched = true;
		this.latchReason = errorText;
		this.latchCount += 1;
		return true;
	}
}

// ── pure helpers ─────────────────────────────────────────────────────────────

/**
 * Thread-name derivation (`_derive_auto_thread_name`, adapter.py:7200-7216).
 * BYTE-PARITY NOTES (closed under the discord-8 defer sweep):
 *   - Mention/channel strips are NUMERIC-ID ONLY — <@123>, <@!123>, <@&123>,
 *     <#123>. Upstream leaves non-numeric text like `<@team>` alone; a broader
 *     [^>]+ strip eats real content Hermes keeps.
 *   - The [:80] / [:77]+"..." slices are PYTHON code-point semantics (str[n]
 *     never splits a surrogate pair). An emoji-heavy opener stays WHOLE even
 *     though its UTF-16 budget overruns 80 units — that is upstream's own
 *     accepted derive-path behavior, and exactly why the RENAME path exists:
 *     rename_thread (:7287-7290) + run.py:_sanitize_discord_thread_title
 *     re-truncate with utf16 helpers before any semantic rename lands. That
 *     consumer rides the deferred session-title lane (see XREF-REPORT §6 /
 *     proposed DEC for discord-8); this function keeps derive-path fidelity.
 */
export function deriveThreadName(content: string): string {
	const stripped = content
		.trim()
		.replace(/<@[!&]?\d+>/g, "")
		.replace(/<#\d+>/g, "")
		.replace(/\s+/g, " ")
		.trim();
	if (stripped.length === 0) return THREAD_NAME_FALLBACK;
	const codePoints = Array.from(stripped);
	if (codePoints.length <= THREAD_NAME_MAX_UTF16_UNITS) return stripped;
	return `${codePoints.slice(0, THREAD_NAME_MAX_UTF16_UNITS - 3).join("")}...`;
}

/**
 * Forum-post name derivation (`_derive_forum_thread_name` :9879-9888): FIRST
 * line of the message, leading markdown heading prefixes stripped, capped at
 * 100 chars, "New Post" when empty.
 */
export function deriveForumThreadName(content: string): string {
	const firstLine = (content.trim().split("\n", 1)[0] ?? "").trim();
	const stripped = firstLine.replace(/^#+/, "").trim();
	if (stripped.length === 0) return FORUM_THREAD_NAME_FALLBACK;
	return stripped.slice(0, FORUM_THREAD_NAME_MAX_CHARS);
}

/** Streaming-edit transmission cap (truncate IN PLACE, :3833-3845 parity). */
export function truncateToMessageCap(content: string): string {
	if (codePointLen(content) <= MESSAGE_LENGTH_MAX) return content;
	const cut = [...content].slice(0, MESSAGE_LENGTH_MAX).join("");
	return `${cut}\n…`;
}

/**
 * Discord-dialect conversion — the SCOPED delta only. Discord renders standard
 * markdown (bold/link/strike) natively on BOTH lanes, so those bytes are
 * preserved verbatim; the one construct the vendor dialect cannot render is a
 * GFM pipe TABLE, which re-renders as fenced column-aligned monospace
 * (family-shared algorithm). Fenced code blocks survive byte-exactly.
 * See the port report's proposed DEC entry for the §10.2 mapping rationale.
 */
export function convertGfmToDiscordMarkdown(markdown: string): string {
	const protectedRegions: Array<{ token: string; original: string }> = [];
	let counter = 0;
	let text = markdown.replace(/```[^\n]*\n[\s\S]*?```/g, (m) => {
		const token = `\u0000PROT${counter++}\u0000`;
		protectedRegions.push({ token, original: m });
		return token;
	});
	text = text.replace(
		/(?:^\|\s.*\|\s*$\n?\|\s*[-:| ]+\|\s*$\n?(?:^\|.+\|\s*$\n?)*)/gm,
		(m) => renderTableAsFencedMonospace(m),
	);
	for (const region of protectedRegions)
		text = text.replace(region.token, () => region.original);
	return text;
}

/**
 * Component-row assembly under the vendor caps: ≤5 rows, ≤5 buttons/row,
 * labels ellipsized to BUTTON_LABEL_LIMIT. Overflow past ROW×BUTTON capacity
 * declines whole-render (caller ships the plain mirror).
 */
export function buildComponentRows(
	buttons: Array<{ customId: string; label: string }>,
): Array<{
	type: number;
	components: Array<{ type: number; label: string; custom_id: string }>;
}> {
	const capacity = COMPONENT_MAX_ROWS * BUTTONS_PER_ROW;
	if (buttons.length > capacity) return []; // whole-render decline
	const rows = [];
	for (let i = 0; i < buttons.length; i += BUTTONS_PER_ROW) {
		rows.push({
			type: 1,
			components: buttons.slice(i, i + BUTTONS_PER_ROW).map((b) => ({
				type: 2,
				label: ellipsizeUtf16(b.label, BUTTON_LABEL_LIMIT),
				custom_id: b.customId,
			})),
		});
	}
	return rows;
}

function ellipsizeUtf16(label: string, cap: number): string {
	if (utf16Len(label) <= cap) return label;
	return `${label.slice(0, cap - 1)}…`;
}

/** Select-option assembly under SELECT_MAX_OPTIONS/SELECT_FIELD_LIMIT. */
export function buildSelectOptions(
	options: Array<{ label: string; value: string }>,
	customId: string,
): {
	type: number;
	custom_id: string;
	options: Array<{ label: string; value: string }>;
} | null {
	if (options.length > SELECT_MAX_OPTIONS) return null; // decline whole-render
	return {
		type: 3,
		custom_id: customId,
		options: options.map((o) => ({
			label: ellipsizeUtf16(o.label, SELECT_FIELD_LIMIT),
			value: o.value.slice(0, SELECT_FIELD_LIMIT),
		})),
	};
}

/** Re-export for subject wiring convenience. */
export { assembleInteractiveMessage, SPLIT_THRESHOLD };
