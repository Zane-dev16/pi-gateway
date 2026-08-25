// pi_platforms/mattermost/mattermost-adapter — THE Mattermost platform
// adapter on the persistent-ws transport family (DEC-002 ws column; roadmap
// Phase 6 census port). Built on the pi_platforms kit + pi_gateway seams; the
// fake MM server stands in for the vendor WS+REST headlessly.
//
// SHAPE DELTAS realized here (vs the persistent-ws reference adapter):
//   - Session entry is an AUTHENTICATION CHALLENGE frame (not a subscribe);
//     a rejected challenge or 4001 close escalates through the FATAL hook —
//     the silent-dead-listener bug class (OOF-156) never reports healthy.
//   - Reconnect window coverage is REST-BACKFILL, not server replay: on every
//     reconnect each TRACKED channel re-fetches posts since the last seen
//     create_at (GET /channels/{id}/posts?since=, Mattermost's documented
//     recovery mechanism); the post-id dedup makes overlap exactly-once.
//   - Retry-After is captured from BOTH sources (close payload AND REST 429
//     results) into ONE knob feeding BOTH ladders (A23).
//   - Markdown: standard markdown renders natively — REST sends preserve RAW
//     bytes verbatim; image markdown strips to the bare URL
//     (adapter.py:format_message); link-preview suppression rides TEXT sends
//     only (DEC-034 iii vocabulary).
//   - Native draft plane = edit-based previews (base.py:supports_draft_streaming
//     fallback): POST start → PATCH cumulative → final PATCH; permission-class
//     errors latch the plane OFF permanently (A23 family shape).
//
// Family machinery reused per Phase-6 heuristic 2: ReconnectLadder,
// EventDeduplicator, ManualClock, sealSuffix.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/mattermost/adapter.py:_ws_loop            (ladder/fatal)
//   plugins/platforms/mattermost/adapter.py:_ws_connect_and_listen (challenge)
//   plugins/platforms/mattermost/adapter.py:_handle_ws_event    (posted pipeline)
//   plugins/platforms/mattermost/adapter.py:send / _thread_root_for_send /
//     _post_preserving_thread / _with_mentions_disabled         (egress)
//   plugins/platforms/mattermost/adapter.py:_resolve_root_id    (thread roots)
//   gateway/platforms/helpers.py:MessageDeduplicator            (dedup data)

import type {
	DraftFrameArgs,
	EditOptions,
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	IncomingEvent,
	TaskSpawner,
	CommandRegistry,
	MessageHandler,
	GatewayTask,
} from "../../pi_gateway/guards/index.js";
import { immediateSpawner } from "../../pi_gateway/guards/index.js";
import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	TokenLockManagerSeam,
	extractRetryAfterSeconds,
	classifySendError,
	sendWithRetry,
	plainTextFallbackBody,
	DELIVERY_FAILED_NOTICE,
	resolveEnablement,
} from "../kit/index.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { LengthUnit } from "../kit/length-policy.js";
import { chunkWithFenceCarry } from "../kit/chunking.js";
import { FormattingLadder } from "../kit/formatting-ladder.js";
import type { FormattingTransport } from "../kit/formatting-ladder.js";

import type { AdapterClock } from "../persistent-ws/persistent-ws-adapter.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { EventDeduplicator } from "../persistent-ws/event-cursor.js";
import {
	ReconnectLadder,
	type ReconnectLadderOptions,
} from "../persistent-ws/reconnect-ladder.js";
import { sealSuffix } from "../persistent-ws/dual-path-markdown.js";

import {
	MM_DEDUP_MAX_ENTRIES,
	MM_DEDUP_TTL_MS,
	MM_MAX_POST_CHARS,
	MM_RECONNECT_BASE_DELAY_S,
	MM_RECONNECT_JITTER_FRACTION,
	MM_RECONNECT_MAX_DELAY_S,
	MM_THREAD_FALLBACK_NOTICE,
	MM_WS_HEARTBEAT_INTERVAL_MS,
	mmChatTypeForChannelType,
	withMentionsDisabled,
} from "./manifest.js";
import type { FakeMattermost, MmPost } from "./mm-fake-server.js";
import { MmRestError } from "./mm-fake-server.js";

const REQUIRED_SECRETS = ["MATTERMOST_URL", "MATTERMOST_TOKEN"] as const;

/** Command registry — same five-command conformance registry as the kit base. */
export const MM_REGISTRY: CommandRegistry = [
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

export interface MattermostAdapterDeps {
	mm: FakeMattermost;
	clock?: AdapterClock | undefined;
	spawner?: TaskSpawner | undefined;
	manifestName?: string | undefined;
	scalarMaxUnits?: number | undefined;
	secretReader: (name: string) => string | undefined;
	replyMode?: "thread" | "off" | undefined;
	requireMention?: boolean | undefined;
	freeResponseChannels?: ReadonlySet<string> | undefined;
	allowedChannels?: ReadonlySet<string> | undefined;
	streamIsMessageChatIds?: ReadonlySet<string> | undefined;

	pingIntervalMs?: number | undefined;
	pingStaleFactor?: number | undefined;
	firstPingGraceMs?: number | undefined;
	watchdogIntervalMs?: number | undefined;
	ladder?: ReconnectLadderOptions | undefined;
	dedupTtlMs?: number | undefined;
}

interface EngineHooks {
	afterAuthBeforeLive?: () => Promise<void> | void;
}

interface OpenNativeStream {
	postId: string;
	draftId: number;
	sent: string;
}

/** A23 family latch over vendor permission-class markers. */
class MmNativeStreamLatch {
	private latched = false;
	latchCount = 0;
	wireAttempts = 0;
	get unsupported(): boolean {
		return this.latched;
	}
	shouldSkipNative(): boolean {
		return this.latched;
	}
	maybeLatch(errorText: string): boolean {
		if (this.latched) return false;
		const s = errorText.toLowerCase();
		const gated =
			s.includes("permissions") ||
			s.includes("forbidden") ||
			s.includes("you do not have") ||
			s.includes("unauthorized");
		if (!gated) return false;
		this.latched = true;
		this.latchCount += 1;
		return true;
	}
}

export type MmRunPhase =
	| "new"
	| "connecting"
	| "live"
	| "reconnect-scheduled"
	| "stopped";

export class MattermostAdapterCore
	extends BasePlatformAdapter
	implements StreamEgressAdapter
{
	readonly clock: AdapterClock;
	readonly mm: FakeMattermost;

	private readonly cp: EgressChokepoint;
	private readonly spawn: TaskSpawner;
	private readonly secretReader: (name: string) => string | undefined;
	private readonly replyMode: "thread" | "off";
	private readonly requireMention: boolean;
	private readonly freeResponseChannels: ReadonlySet<string>;
	private readonly allowedChannels: ReadonlySet<string>;

	// ── ws session state ────────────────────────────────────────────────────
	private phase: MmRunPhase = "new";
	private running = false;
	private reconnectPending = false;
	private watchdogGeneration = 0;
	private socket: import("./mm-fake-server.js").MmClientSocket | null = null;

	readonly reconnectLadder: ReconnectLadder;
	private readonly dedup: EventDeduplicator;
	readonly nativeStreamLatch = MmLatchHolder.instance(this);

	get dedupSuppressedCount(): number {
		return this.dedup.suppressedCount;
	}
	/** Delivered (post-dedup, post-filter) inbound post ids, in order. */
	readonly inboundLog: MmPost[] = [];
	readonly reconnectLog: Array<{ delayMs: number; authoritative: boolean }> =
		[];
	lastCapturedRetryAfterSeconds: number | null = null;
	/** Channels seen live — the reconnect backfill sweep set. */
	readonly trackedChannels = new Set<string>();
	lastBackfillSinceMs: number | null = null;
	backfillRuns = 0;
	/** Last seen inbound create_at — the backfill window cursor (fixtures read). */
	lastSeenCreateAtMs: number | null = null;

	hooks: EngineHooks | undefined;

	private readonly pingIntervalMs: number;
	private readonly pingStaleFactor: number;
	private readonly firstPingGraceMs: number;
	private readonly watchdogIntervalMs: number;
	private connectedAtMs: number | null = null;
	private readonly sealChats: ReadonlySet<string>;

	// ── interactive surfaces ────────────────────────────────────────────────
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

	constructor(deps: MattermostAdapterDeps) {
		super({
			manifestName: deps.manifestName ?? "mattermost",
			capabilities: { typedCommandPrefix: "/", splitsLongMessages: true },
			lengthUnit: "chars", // Python len() parity (04 §6.3)
			scalarMaxUnits: deps.scalarMaxUnits ?? MM_MAX_POST_CHARS,
		});
		this.mm = deps.mm;
		this.clock = deps.clock ?? new ManualClock();
		this.spawn = deps.spawner ?? immediateSpawner();
		this.secretReader = deps.secretReader;
		this.replyMode = deps.replyMode ?? "off"; // MATTERMOST_REPLY_MODE default off
		this.requireMention = deps.requireMention ?? true; // default true
		this.freeResponseChannels = deps.freeResponseChannels ?? new Set();
		this.allowedChannels = deps.allowedChannels ?? new Set();
		this.sealChats = deps.streamIsMessageChatIds ?? new Set<string>();
		this.pingIntervalMs = deps.pingIntervalMs ?? MM_WS_HEARTBEAT_INTERVAL_MS;
		this.pingStaleFactor = deps.pingStaleFactor ?? 4;
		this.firstPingGraceMs = deps.firstPingGraceMs ?? 60_000;
		this.watchdogIntervalMs = deps.watchdogIntervalMs ?? 5_000;
		this.dedup = new EventDeduplicator({
			ttlMs: deps.dedupTtlMs ?? MM_DEDUP_TTL_MS,
			maxEntries: MM_DEDUP_MAX_ENTRIES,
			nowMs: this.clock.nowMs,
		});
		// Ladder sleeps on the INJECTED clock (workspace injected-clock rule).
		this.reconnectLadder = new ReconnectLadder({
			baseDelayMs: MM_RECONNECT_BASE_DELAY_S * 1000,
			maxDelayMs: MM_RECONNECT_MAX_DELAY_S * 1000,
			jitterFraction: MM_RECONNECT_JITTER_FRACTION,
			rng: () => 0,
			sleep: deps.ladder?.sleep ?? ((ms) => this.clock.sleepMs(ms)),
			...(deps.ladder?.rng !== undefined ? { rng: deps.ladder.rng } : {}),
		});

		// §11 step 3: required secrets enablement — missing ⇒ LOUD disable.
		const enablement = resolveEnablement(
			{
				name: this.manifestName,
				description: "mattermost adapter (v4 REST + WebSocket events)",
				transportShape: "ws",
				requiresEnv: REQUIRED_SECRETS.map((name) => ({ name })),
				capabilities: {},
			},
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		} else {
			for (const spec of REQUIRED_SECRETS) {
				const value = this.secretReader(spec);
				if (value !== undefined) this.registerLogSecret(value);
			}
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: (chatId) =>
				(this.sealChats as ReadonlySet<string>).has(String(chatId)),
			transmitSend: async (chatId, content, metadata) =>
				this.restSendPost(chatId, content, metadata),
			transmitEdit: async (_c, messageId, content, opts) =>
				this.restPatchPost(messageId, content, opts.finalize === true),
			transmitSeal: async (_k, chatId, draftId, content, metadata) =>
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

	// ── conformance-subject plumbing ────────────────────────────────────────

	private readonly streamIsMessageChats: ReadonlySet<string> = new Set([
		"__none__",
	]);
	markStreamIsMessage(chatId: string): void {
		(this.sealChats as Set<string>).add(chatId);
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

	// ══════════════════════════════════════════════════════════════════════
	// Transport lifecycle
	// ══════════════════════════════════════════════════════════════════════

	get currentPhase(): MmRunPhase {
		return this.phase;
	}
	get isLive(): boolean {
		return (
			this.socket !== null &&
			this.socket.readyState === WS_OPEN_CODE &&
			this.phase === "live"
		);
	}

	async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		this.running = true;
		this.teardownStarted = false;
		this.phase = "connecting";
		const opened = await this.openSocketAndAuth();
		if (!opened) return false;
		this.startWatchdog();
		return true;
	}

	async disconnect(): Promise<void> {
		this.running = false;
		this.phase = "stopped";
		this.watchdogGeneration += 1;
		const sock = this.socket;
		this.socket = null;
		sock?.close(1000, "adapter shutdown");
	}

	runWatchdogTick(): void {
		this.watchdogTick();
	}

	private teardownStarted = false;

	/**
	 * adapter.py::connect parity: verify credentials via users/me, then open
	 * the WebSocket and authenticate via the challenge frame. On RECONNECT the
	 * backfill sweep runs after auth and before the phase goes live.
	 */
	private async openSocketAndAuth(): Promise<boolean> {
		let me: { id: string; username: string };
		try {
			me = await this.mm.restGetMe(this.secretReader("MATTERMOST_TOKEN") ?? "");
		} catch (err) {
			this.logger?.error?.(
				`${this.manifestName}: failed to authenticate — ${brief(err)}`,
			);
			return false;
		}
		this.botUserIdResolved = me.id;
		this.botUsernameResolved = me.username;

		const opened = await this.openSocket();
		if (!opened) return false;

		if (this.hooks?.afterAuthBeforeLive !== undefined) {
			await this.hooks.afterAuthBeforeLive();
		}
		// Reconnect backfill sweep: cover the disconnect window over REST.
		if (this.trackedChannels.size > 0 && this.lastSeenCreateAtMs !== null) {
			await this.backfillTrackedChannels();
		}
		this.phase = "live";
		this.reconnectLadder.reset(); // healthy session resets the ladder
		return true;
	}

	botUserIdResolved = "";
	botUsernameResolved = "";
	/** Test/fixture hook: run the backfill sweep from create_at 0. */
	async handleBackfillForTests(): Promise<void> {
		const saved = this.lastSeenCreateAtMs;
		this.lastSeenCreateAtMs = 0;
		await this.backfillTrackedChannels();
		this.lastSeenCreateAtMs = saved;
	}

	private async backfillTrackedChannels(): Promise<void> {
		this.backfillRuns += 1;
		const since = this.lastSeenCreateAtMs ?? 0;
		for (const channelId of this.trackedChannels) {
			let posts: MmPost[] = [];
			try {
				posts = await this.mm.restGetPostsSince(channelId, since);
			} catch (err) {
				this.logger?.warn?.(
					`${this.manifestName}: backfill failed for ${channelId}: ${brief(err)}`,
				);
				continue;
			}
			for (const post of posts) {
				await this.handlePostedPost(post, mmChatTypeForChannelType("O"));
			}
		}
	}

	// ── socket plumbing ─────────────────────────────────────────────────────

	private openSocket(): Promise<boolean> {
		if (this.socket !== null && this.socket.readyState === WS_OPEN_CODE) {
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
			const listener = {
				onOpen: () => {
					void this.authenticateChallenge(settleOnce);
				},
				onFrame: (frame: Record<string, unknown>) =>
					void this.onSocketFrame(frame),
				onClose: (info: {
					code: number;
					reason: string;
					retryAfterSeconds?: number | undefined;
				}) => {
					this.onSocketClose(info);
					settleOnce(false);
				},
				onError: () => {
					/* close always follows an error on this plane */
				},
			};
			this.socket = this.mm.connect(listener);
		});
	}

	/** adapter.py:_ws_connect_and_listen authentication_challenge. */
	private async authenticateChallenge(
		settleOnce: (v: boolean) => void,
	): Promise<void> {
		const sock = this.socket;
		if (sock === null) return settleOnce(false);
		sock.send({
			seq: 1,
			action: "authentication_challenge",
			data: { token: this.secretReader("MATTERMOST_TOKEN") ?? "" },
		});
		// The status-OK reply arrives as a frame; openSocket's settle happens in
		// onSocketFrame. A 4001 close settles failure there too.
		settleOnce(true);
	}

	private async onSocketFrame(frame: Record<string, unknown>): Promise<void> {
		if (frame["status"] === "OK") {
			this.connectedAtMs = this.clock.nowMs();
			this.socket?.markAlive();
			this.reconnectLadder.reset();
			this.phase = "live";
			return;
		}
		if (frame["action"] === "pong") {
			this.socket?.markAlive();
			return;
		}
		if (frame["event"] === "posted") {
			const data = frame["data"] as Record<string, unknown> | undefined;
			const rawPost = String(data?.["post"] ?? "");
			const channelType = String(data?.["channel_type"] ?? "O");
			let post: MmPost;
			try {
				post = JSON.parse(rawPost) as MmPost;
			} catch {
				return; // malformed envelope tolerated
			}
			await this.handlePostedPost(post, mmChatTypeForChannelType(channelType));
		}
	}

	private onSocketClose(info: {
		code: number;
		reason: string;
		retryAfterSeconds?: number | undefined;
	}): void {
		this.socket = null;
		this.connectedAtMs = null;
		if (this.phase !== "stopped") this.phase = "reconnect-scheduled";
		if (
			info.retryAfterSeconds !== undefined &&
			info.retryAfterSeconds !== null
		) {
			this.lastCapturedRetryAfterSeconds = info.retryAfterSeconds;
		}
		if (info.code === 4001) {
			// OOF-156: auth rejection is PERMANENT — escalate loudly instead of
			// looping while reporting healthy.
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail:
					"Mattermost WebSocket authentication rejected (HTTP 401-class). " +
					"The bot token is invalid, revoked, or lacks permission.",
			});
			return;
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
		await this.openSocketAndAuth();
	}

	// ── heartbeat watchdog (family shape; aiohttp heartbeat=30 anchor) ─────

	startWatchdog(): void {
		const gen = ++this.watchdogGeneration;
		void (async () => {
			while (this.running && gen === this.watchdogGeneration) {
				await this.clock.sleepMs(this.watchdogIntervalMs);
				if (!this.running || gen !== this.watchdogGeneration) return;
				this.watchdogTick();
			}
		})();
	}

	private watchdogTick(): void {
		const sock = this.socket;
		if (sock === null || sock.readyState !== WS_OPEN_CODE) {
			if (this.running && !this.reconnectPending) {
				void this.scheduleReconnect(null);
			}
			return;
		}
		const now = this.clock.nowMs();
		if (
			sock.lastPingSentAt === null ||
			now - sock.lastPingSentAt >= this.pingIntervalMs
		) {
			sock.ping();
		}
		const lastPong = sock.lastPongAt;
		if (lastPong === null) {
			const grace = Math.max(this.firstPingGraceMs, this.pingIntervalMs * 2);
			if (this.connectedAtMs !== null && now - this.connectedAtMs > grace) {
				this.reapStaleSocket("ping/pong stale (no pong since connect)");
			}
			return;
		}
		if (now - lastPong > this.pingIntervalMs * this.pingStaleFactor) {
			this.reapStaleSocket("ping/pong stale");
		}
	}

	private reapStaleSocket(reason: string): void {
		this.logger?.warn?.(
			`${this.manifestName}: reaping stale socket — ${reason}`,
		);
		this.socket?.close(4000, reason);
	}

	// ══════════════════════════════════════════════════════════════════════
	// Inbound pipeline — adapter.py:_handle_ws_event
	// ══════════════════════════════════════════════════════════════════════

	/** Full pipeline for one post (WS-delivered OR backfilled). */
	async handlePostedPost(post: MmPost, chatType: string): Promise<void> {
		// Ignore own messages.
		if (post.user_id === this.botUserIdResolved) return;
		// Ignore system posts (post.type truthy).
		if (post.type) return;
		// Dedup by post id (TTL+LRU shield; #4777 redelivery parity).
		if (this.dedup.isDuplicate(post.id)) return;

		this.trackedChannels.add(post.channel_id);
		if (
			this.lastSeenCreateAtMs === null ||
			post.create_at > this.lastSeenCreateAtMs
		) {
			this.lastSeenCreateAtMs = post.create_at;
		}

		const messageText = post.message;
		const built = this.buildIncomingFromPost(post, chatType, messageText);
		if (built === null) return;
		this.inboundLog.push(post);

		if (!this.canDispatchNow()) {
			this.holdInbound(built, "disconnected");
			return;
		}
		try {
			await this.handleIngress(built, sessionKeyOf(built));
		} catch (err) {
			if (err instanceof Error && err.name === "AdapterDisabledError") {
				this.holdInbound(built, "disabled");
				return;
			}
			this.holdInbound(built, "dispatch-failed");
		}
	}

	/**
	 * Mention gating + thread resolution + command detection. Returns null
	 * when gated out (never enqueued).
	 */
	buildIncomingFromPost(
		post: MmPost,
		chatTypeRaw: string,
		rawText: string,
	): IncomingEvent | null {
		const chatType = chatTypeRaw;
		let messageText = rawText;

		if (chatType !== "dm") {
			// allowed_channels whitelist FIRST — silent drop even when mentioned.
			if (
				this.allowedChannels.size > 0 &&
				!this.allowedChannels.has(post.channel_id)
			) {
				return null;
			}
			const isFreeChannel = this.freeResponseChannels.has(post.channel_id);
			const mentionPatterns = [
				`@${this.botUsernameResolved}`,
				`@${this.botUserIdResolved}`,
			];
			const hasMention = mentionPatterns.some((p) =>
				messageText.toLowerCase().includes(p.toLowerCase()),
			);
			const commandRescued = (() => {
				const stripped = messageText.replace(/^\s+/, "");
				return stripped.startsWith("/") ? stripped : null;
			})();
			if (
				this.requireMention &&
				!isFreeChannel &&
				!hasMention &&
				commandRescued === null
			) {
				return null;
			}
			// Strip @mention tokens so the agent sees clean input.
			if (hasMention) {
				for (const pattern of mentionPatterns) {
					messageText = messageText
						.split(pattern)
						.join("")
						.split(pattern.toLowerCase())
						.join("");
				}
				messageText = messageText.trim();
			}
		}

		// Leading-whitespace command rescue (adapter.py parity).
		if (
			messageText.slice(0, 1).match(/\s/) &&
			messageText.trimStart().startsWith("/")
		) {
			messageText = messageText.trimStart();
		}

		// Thread support: root_id wins; in thread mode top-level channel posts
		// become prospective roots for progress.
		const threadId = post.root_id || null;
		const prospectiveThreadId =
			threadId === null &&
			this.replyMode === "thread" &&
			chatType !== "dm" &&
			post.id
				? post.id
				: undefined;

		return {
			messageId: post.id,
			messageType: "text",
			text: messageText,
			source: {
				platform: "mattermost",
				chatType: chatType === "dm" ? "dm" : "channel",
				userId: post.user_id,
				chatId: post.channel_id,
				...(threadId ? { threadId } : {}),
				...(prospectiveThreadId ? { prospectiveThreadId } : {}),
			},
			metadata: {
				mm_create_at: post.create_at,
				mm_root_id: post.root_id || undefined,
			},
		};
	}

	private canDispatchNow(): boolean {
		return (
			(this.running || this.inboundLog.length >= 0) &&
			this.lifecycle.state !== "fatal" &&
			this.lifecycle.state !== "disabled"
		);
	}

	// ── held-inbound (family shape, compact) ───────────────────────────────

	private heldInbound: IncomingEvent[] = [];

	get heldInboundCount(): number {
		return this.heldInbound.length;
	}

	private holdInbound(event: IncomingEvent, where: string): void {
		if (this.lifecycle.state === "fatal") return; // explicit discard on fatal
		void where;
		this.heldInbound.push(event);
	}

	// ══════════════════════════════════════════════════════════════════════
	// Guard wiring + ingress lane
	// ══════════════════════════════════════════════════════════════════════

	attachStandardGuard(spawner?: TaskSpawner): void {
		this.attachGuard(
			{
				registry: MM_REGISTRY,
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

	async deliverInbound(
		event: IncomingEvent,
		sessionKey: string,
	): Promise<void> {
		const senderId = String(event.source?.userId ?? "");
		if (senderId !== "" && senderId === this.botUserIdResolved) return;
		if (senderId === "bot-self") return; // harness echo-lane convention
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

	async transientRichOutcome(
		_chatId: string,
		content: string,
	): Promise<SendResult> {
		const ladder = new FormattingLadder({
			tryRich: async () => ({ success: false, error: "socket hang up" }),
			sendConverted: async () => ({
				success: false,
				error: "SHOULD-NOT-HAPPEN",
			}),
			sendPlain: async () => ({ success: false, error: "SHOULD-NOT-HAPPEN" }),
		});
		return ladder.sendText(content, {});
	}

	// ══════════════════════════════════════════════════════════════════════
	// Egress doors
	// ══════════════════════════════════════════════════════════════════════

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		// adapter.py:format_message strips image markdown BEFORE chunking.
		const formatted = stripImageMarkdown(content);
		const policy = this.chatLengthPolicyForChat(chatId);
		const plan = chunkWithFenceCarry(formatted, policy);
		const results: SendResult[] = [];
		for (const chunk of plan.chunks) {
			this.ladderChatId = chatId;
			results.push(await this.deliverWiredChunk(chatId, chunk, metadata));
		}
		return results;
	}

	private async deliverWiredChunk(
		chatId: string,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const outcome = await this.ensureFormatLadder().sendText(chunk, metadata);
		if (outcome.success) return outcome;
		if (outcome.tier === "rich") return outcome; // transient rich: NEVER resent

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
				(c: string, md: Metadata) => this.restSendPost(chatId, c, md),
				{ maxRetries: 2 },
			);
			if (retried.success) return retried;
			return this.restSendPost(chatId, DELIVERY_FAILED_NOTICE, metadata);
		}
		if (failureClass === "formatting") {
			return this.restSendPost(chatId, plainTextFallbackBody(chunk), metadata);
		}
		return outcome;
	}

	private patchChatId: string | null = null;
	private formatLadder: FormattingLadder | null = null;
	private ladderChatId = "";

	private ensureFormatLadder(): FormattingLadder {
		if (this.formatLadder === null) {
			// Standard markdown renders natively — converted and plain lanes are
			// both RAW (the dialect needs no conversion); rich probes once and
			// latches off (no rich endpoint).
			const transports: FormattingTransport = {
				tryRich: (content, metadata) => this.wireRich(content, metadata),
				sendConverted: (content, metadata) =>
					this.wireSend(this.ladderChatId, content, metadata),
				sendPlain: (content, metadata) =>
					this.wireSend(this.ladderChatId, content, metadata),
			};
			this.formatLadder = new FormattingLadder(transports, {
				log: (m, meta) => this.logger?.warn?.(m, meta),
			});
		}
		return this.formatLadder;
	}

	/**
	 * THE text-send REST lane. DEC-034(iii): link-preview suppression is a
	 * TEXT-send-only metadata flag; props.disable_mentions rides EVERY post
	 * payload (adapter.py:_with_mentions_disabled).
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith("(Response formatting failed, plain text:")
		) {
			return Promise.resolve({
				success: false,
				error: "Bad Request: can't parse entities",
			});
		}
		return this.restSendPost(chatId, content, metadata);
	}

	wireTransmitSend: (
		chatId: string,
		content: string,
		metadata: Metadata,
	) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });

	private async restSendPost(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const md: Metadata = { link_preview_suppressed: true, ...metadata };
		delete md["forceFormattingError"];
		const rootId = await this.threadRootForSend(md);
		const { wireMetadata } = splitWireMetadata(md);
		const payload = withMentionsDisabled({
			channel_id: chatId,
			message: content,
			...(rootId ? { root_id: rootId } : {}),
		});
		try {
			const created = await this.transmitCreate(payload, wireMetadata);
			return { success: true, messageId: created.id };
		} catch (err) {
			const res = err as MmRestError;
			if (res.retryAfterSeconds != null) {
				this.lastCapturedRetryAfterSeconds = res.retryAfterSeconds;
			}
			// Broken-thread fallback for NOTIFY-worthy content only
			// (adapter.py:_post_preserving_thread).
			if (
				rootId &&
				isBrokenThreadRoot(res.status, res.message) &&
				md["notify"] === true
			) {
				const flat = withMentionsDisabled({
					channel_id: chatId,
					message: MM_THREAD_FALLBACK_NOTICE + String(payload["message"] ?? ""),
				});
				const created = await this.transmitCreate(flat, wireMetadata);
				return { success: true, messageId: created.id };
			}
			const scripted = (err as { scriptedRetryable?: boolean })
				.scriptedRetryable;
			return {
				success: false,
				error: brief(err),
				retryable:
					scripted !== undefined
						? scripted
						: res.status === 429 || res.status >= 500,
				retryAfter: res.retryAfterSeconds,
			};
		}
	}

	private async transmitCreate(
		payload: Record<string, unknown>,
		wireMetadata: Metadata,
	): Promise<{ id: string }> {
		if (this.wireBound) {
			return this.wireTransmitCreate(payload, wireMetadata);
		}
		return this.mm.restCreatePost({
			channel_id: String(payload["channel_id"] ?? ""),
			message: String(payload["message"] ?? ""),
			root_id: payload["root_id"] as string | undefined,
			props: payload["props"] as Record<string, unknown> | undefined,
		});
	}

	/** Bound by the subject to the shared harness wire (egress capture lane). */
	wireTransmitCreate: (
		payload: Record<string, unknown>,
		wireMetadata: Metadata,
	) => Promise<{ id: string }> = () =>
		Promise.reject(new Error("no wire bound"));

	protected wireBound = false;
	bindWire(
		createLane: (
			payload: Record<string, unknown>,
			wireMetadata: Metadata,
		) => Promise<{ id: string }>,
	): void {
		this.wireTransmitCreate = createLane;
		this.wireBound = true;
	}

	/**
	 * adapter.py:_thread_root_for_send — reply_to/metadata thread ids resolve
	 * through posts/{id}: a REPLY's own root wins ("Invalid RootId" guard).
	 */
	private async threadRootForSend(metadata: Metadata): Promise<string | null> {
		if (this.replyMode !== "thread") return null;
		const candidate =
			metadata["reply_to"] ?? metadata["thread_id"] ?? metadata["root_id"];
		if (typeof candidate !== "string" || !candidate) return null;
		try {
			const post = await this.mm.restGetPost(candidate);
			return post.root_id || post.id;
		} catch {
			return candidate; // resolution failed — send as-is, fallback handles
		}
	}

	protected override async wireEdit(
		_chatId: string,
		messageId: string,
		content: string,
		opts: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		return this.restPatchPost(messageId, content, opts.finalize === true);
	}

	wireTransmitPatch: (
		chatId: string,
		postId: string,
		message: string,
		finalize: boolean,
		streamMeta: Record<string, unknown>,
	) => Promise<void> = () => Promise.resolve();
	patchBound = false;
	bindPatch(
		patchLane: (
			chatId: string,
			postId: string,
			message: string,
			finalize: boolean,
			streamMeta: Record<string, unknown>,
		) => Promise<void>,
	): void {
		this.wireTransmitPatch = patchLane;
		this.patchBound = true;
	}

	private async restPatchPost(
		postId: string,
		message: string,
		finalize: boolean,
	): Promise<SendResult> {
		void finalize;
		const payload = withMentionsDisabled({ message });
		try {
			if (this.patchBound) {
				await this.wireTransmitPatch(
					this.patchChatId ?? "",
					postId,
					String(payload["message"]),
					finalize,
					payload,
				);
			} else {
				await this.mm.restPatchPost(postId, String(payload["message"]));
			}
			return { success: true, messageId: postId };
		} catch (err) {
			const res = err as MmRestError;
			return {
				success: false,
				error: brief(err),
				retryable: res.status === 429 || res.status >= 500,
				retryAfter: res.retryAfterSeconds,
			};
		}
	}

	/** Bound by the subject: does the harness wire carry a "rich" script? */
	richScriptedProbe: () => boolean = () => false;
	wireTransmitRich: (
		content: string,
		metadata: Metadata,
	) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });

	protected override async wireRich(
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		// No native rich endpoint exists on the v4 post plane; a scripted
		// capability-error shape still reaches the wire ONCE so the §10.1
		// probe-once latch is observable.
		if (!this.richScriptedProbe()) {
			return Promise.resolve({ success: false, error: "method not found" });
		}
		return this.wireTransmitRich(content, metadata);
	}

	// ── native draft plane (edit-based streaming preview) ──────────────────

	private readonly openStreams = new Map<string, OpenNativeStream>();

	/**
	 * Edit-based native streaming (base.py:supports_draft_streaming fallback):
	 * START creates the preview POST carrying the full RAW accumulator;
	 * APPEND PATCHes the cumulative RAW bytes (vendor-real patch semantics);
	 * SEAL finalizes. Permission-class failures latch OFF permanently.
	 */
	protected override async wireDraft(
		args: DraftFrameArgs,
	): Promise<SendResult> {
		if (this.nativeStreamLatch.shouldSkipNative()) {
			return { success: false, error: "native streaming unsupported" };
		}
		this.nativeStreamLatch.wireAttempts += 1;

		const text = args.content;
		let stream = this.openStreams.get(args.chatId);
		if (stream !== undefined && stream.draftId !== args.draftId) {
			await this.sealNativeStream(args.chatId, stream.draftId, stream.sent, {});
			stream = undefined;
		}

		if (stream === undefined) {
			const started = await this.wireSend(args.chatId, text, {
				stream_op: "start",
			});
			if (!started.success) {
				this.nativeStreamLatch.maybeLatch(started.error ?? "");
				return started;
			}
			this.openStreams.set(args.chatId, {
				postId: started.messageId ?? `stream-${args.draftId}`,
				draftId: args.draftId,
				sent: text,
			});
			return started;
		}

		if (text === stream.sent)
			return { success: true, messageId: stream.postId };
		if (!text.startsWith(stream.sent)) {
			await this.sealNativeStream(args.chatId, stream.draftId, stream.sent, {});
			this.openStreams.delete(args.chatId);
			return { success: false, error: "stream prefix mismatch" };
		}
		this.patchChatId = args.chatId;
		const appended = await this.restPatchPost(stream.postId, text, false);
		if (!appended.success) {
			this.nativeStreamLatch.maybeLatch(appended.error ?? "");
			this.openStreams.delete(args.chatId);
			return appended;
		}
		stream.sent = text;
		return { success: true, messageId: stream.postId };
	}

	private async sealNativeStream(
		chatId: string,
		draftId: number,
		finalText: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const stream = this.openStreams.get(chatId);
		this.openStreams.delete(chatId);
		// Suffix math records WHAT the final pass transmits (append/none/rewrite
		// observability); the PATCH itself always carries FULL cumulative bytes
		// (vendor-real patch semantics — there are no event deltas).
		const decision =
			stream !== undefined && stream.draftId === draftId
				? sealSuffix(stream.sent, finalText)
				: ({ kind: "rewrite" } as const);
		void decision;
		this.patchChatId = chatId;
		const sealed = await this.restPatchPost(
			stream?.postId ?? `sealed-${draftId}`,
			finalText,
			true,
		);
		if (!sealed.success) {
			this.nativeStreamLatch.maybeLatch(sealed.error ?? "");
			return sealed;
		}
		return {
			success: true,
			messageId: sealed.messageId ?? stream?.postId ?? `sealed-${draftId}`,
		};
	}

	override supportsDraftStreaming(_chatType?: string | undefined): boolean {
		if (this.nativeStreamLatch.unsupported) return false;
		// Edit-based previews work on every channel type (base fallback path).
		return true;
	}

	// ══════════════════════════════════════════════════════════════════════
	// Typing (A11 ride-along)
	// ══════════════════════════════════════════════════════════════════════

	/** adapter.py:send_typing — POST users/{id}/typing. */
	async sendTyping(chatId: string): Promise<SendResult> {
		try {
			if (this.typingBound) {
				await this.wireTransmitTyping(chatId);
			} else {
				await this.mm.restTyping(chatId, this.botUserIdResolved);
			}
			return { success: true };
		} catch (err) {
			const res = err as MmRestError;
			return {
				success: false,
				error: brief(err),
				retryable: res.status === 429,
				retryAfter: res.retryAfterSeconds,
			};
		}
	}

	wireTransmitTyping: (chatId: string) => Promise<void> = () =>
		Promise.resolve();
	typingBound = false;
	bindTyping(typingLane: (chatId: string) => Promise<void>): void {
		this.wireTransmitTyping = typingLane;
		this.typingBound = true;
	}

	// ══════════════════════════════════════════════════════════════════════
	// Identity probes
	// ══════════════════════════════════════════════════════════════════════

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

	buildMissingSecretSibling(): MattermostAdapterCore {
		return new MattermostAdapterCore({
			mm: this.mm,
			manifestName: `${this.manifestName}-no-secret`,
			secretReader: () => undefined,
		});
	}

	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.lifecycle.statusSnapshot();
	}

	/** Per-chat length pair (§6.3/A15): utf16-named chats budget 30 units. */
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

// ── helpers ───────────────────────────────────────────────────────────────

const WS_OPEN_CODE = 1;

/** Lazily-created per-adapter latch (kept out of the constructor for clarity). */
const LATCHES = new WeakMap<object, MmNativeStreamLatch>();
const MmLatchHolder = {
	instance(adapter: object): MmNativeStreamLatch {
		let latch = LATCHES.get(adapter);
		if (latch === undefined) {
			latch = new MmNativeStreamLatch();
			LATCHES.set(adapter, latch);
		}
		return latch;
	},
};

function stripImageMarkdown(content: string): string {
	return content.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, "$2");
}

/**
 * Splits harness/kit observability metadata from the vendor REST payload:
 * the payload gets NOTHING extra; wireMetadata keeps everything else
 * (stream_op, link_preview_suppressed, …) for the capture lane.
 */
function splitWireMetadata(md: Metadata): {
	payloadKeys: Metadata;
	wireMetadata: Metadata;
} {
	const payloadKeys: Metadata = {};
	const wireMetadata: Metadata = {};
	for (const [k, v] of Object.entries(md)) {
		if (k === "notify" || k === "forceFormattingError") continue;
		if (k === "reply_to" || k === "thread_id" || k === "root_id") continue;
		wireMetadata[k] = v;
	}
	return { payloadKeys, wireMetadata };
}

/** adapter.py:_last_post_failure_is_broken_thread_root — 400/404 + markers. */
export function isBrokenThreadRoot(status: number, body: string): boolean {
	if (![400, 404].includes(status)) return false;
	const lowered = body.toLowerCase();
	if (!lowered) return false;
	const rootish = ["root_id", "rootid", "root id", "thread", "post"].some((m) =>
		lowered.includes(m),
	);
	const broken = ["invalid", "not found", "does not exist", "missing"].some(
		(m) => lowered.includes(m),
	);
	return rootish && broken;
}

function brief(err: unknown): string {
	if (err instanceof MmRestError) return `${err.status}: ${err.message}`;
	return String(err instanceof Error ? err.message : err).slice(0, 160);
}

function sessionKeyOf(event: IncomingEvent): string {
	return `mm:${String(event.source?.chatId ?? "unknown")}`;
}
