// pi_platforms/qqbot/qqbot-adapter — the QQ Bot official-gateway adapter
// (WebSocket shape), ported from Hermes gateway/platforms/qqbot/adapter.py.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   adapter.py:QQAdapter.__init__        — config, policies, dedup, token cache
//   adapter.py:connect/_ensure_token     — token flow w/ 60s refresh margin
//   adapter.py:_dispatch_payload         — op-code routing (10/0/11/7/9)
//   adapter.py:_listen_loop              — close-code classes, quick-disconnect,
//                                          fixed backoff tiers [2,5,10,30,60]
//   adapter.py:_handle_c2c/group/guild/dm_message — intake ACLs, @-strip,
//                                          quoted-context merge (msg_type 103)
//   adapter.py:send/_send_chunk          — markdown v2 body, msg_seq, retry
//   adapter.py:send_with_keyboard        — keyboard attach (c2c/group only)
//   adapter.py:_on_interaction           — prompt ACK then dispatch
//   adapter.py:_is_duplicate             — 300s window / 1000-entry bound
//
// Probe-computed exclusions (documented honestly, never faked green):
//   • Voice STT / silk→wav conversion (Hermes delegates to external ffmpeg +
//     whisper daemons) — voice attachments surface an attachment-info line.
//   • Local-file reads ride an injected byte seam in fixtures; the COS PUT
//     plane is exercised against the fake server's scripted REST face.

import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import { BasePlatformAdapter } from "../kit/index.js";
import { BoundedSeenSet } from "../../pi_gateway/security/trust/replay-seen-set.js";
import {
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	type ClickAuthorizer,
} from "../kit/index.js";
import {
	classifySendError,
	extractRetryAfterSeconds,
	PLAIN_TEXT_FALLBACK_PREFIX,
} from "../kit/send-retry.js";
import {
	QQ_IDENTIFY_INTENTS,
	QQBOT_MAX_QUICK_DISCONNECT_COUNT,
	QQ_MSG_TYPE_MARKDOWN,
	QQ_MSG_TYPE_TEXT,
	QQBOT_API_BASE,
	QQBOT_DEDUP_MAX_SIZE,
	QQBOT_DEDUP_WINDOW_SECONDS,
	QQBOT_FILE_UPLOAD_TIMEOUT_S,
	QQBOT_GATEWAY_URL_PATH,
	QQ_HEARTBEAT_FRACTION_OF_INTERVAL,
	QQBOT_MAX_MESSAGE_LENGTH,
	QQBOT_PLUGIN_MANIFEST,
	QQBOT_QUICK_DISCONNECT_THRESHOLD_S,
	QQBOT_RATE_LIMIT_DELAY_S,
	QQBOT_RECONNECT_BACKOFF_S,
	QQ_SEND_MAX_ATTEMPTS,
	QQ_SEND_RETRY_BASE_DELAY_S,
	QQ_TOKEN_DEFAULT_EXPIRES_IN_S,
	QQ_TOKEN_REFRESH_MARGIN_S,
	QQBOT_TOKEN_URL,
} from "./manifest.js";
import {
	buildApprovalKeyboard,
	buildUpdatePromptKeyboard,
	parseApprovalButtonData,
	parseInteractionEvent,
	parseUpdatePromptButtonData,
	type InteractionEvent,
	type InlineKeyboardWire,
} from "./keyboards.js";
import { ChunkedUploader } from "./chunked-uploader.js";
import type {
	FakeQQGateway,
	QQClientSocket,
	QQGatewayPayload,
	QQSocketListener,
} from "./fake-qq-gateway.js";

export type QQChatType = "c2c" | "group" | "guild" | "dm";

export interface QQRestTransport {
	request(
		method: "POST" | "GET" | "PUT",
		path: string,
		body: Record<string, unknown> | Buffer,
		headers?: Record<string, string> | undefined,
	): Promise<{ status: number; body: Record<string, unknown> }>;
}

export interface QQAdapterOptions {
	appId?: string | undefined;
	clientSecret?: string | undefined;
	markdownSupport?: boolean | undefined;
	dmPolicy?: string | undefined;
	groupPolicy?: string | undefined;
	allowFrom?: readonly string[] | undefined;
	groupAllowFrom?: readonly string[] | undefined;
	scalarMaxUnits?: number | undefined;
	rest: QQRestTransport;
	wsFactory: FakeQQGateway;
	sleepMs?: ((ms: number) => Promise<void>) | undefined;
	nowMs?: (() => number) | undefined;
	/** Scripted §10.1 tier-1 rich probe (fixture seam; production: absent). */
	richProbe?: ((content: string) => Promise<SendResult>) | undefined;
	/** Whether a rich script was deliberately programmed (probe gating). */
	richHasScript?: (() => boolean) | undefined;
}

/** One authoritative-or-computed reconnect wait (ladder observability). */
export interface QQReconnectStep {
	delayMs: number;
	authoritative: boolean;
	attempt: number;
}

const PERMANENT_SEND_PATTERNS = [
	"invalid",
	"forbidden",
	"not found",
	"bad request",
];
/** Fatal close codes stop reconnection (adapter.py:_listen_loop FATAL set). */
const FATAL_CLOSE_CODES: ReadonlySet<number> = new Set([
	4001, 4002, 4010, 4011, 4012, 4013, 4014, 4914, 4915,
]);
/** Session-invalid close codes clear state for a fresh Identify (NOT 4009). */
const SESSION_INVALID_CLOSE_CODES: ReadonlySet<number> = new Set([
	4006, 4007, 4900, 4901, 4902, 4903, 4904, 4905, 4906, 4907, 4908, 4909, 4910,
	4911, 4912, 4913,
]);

/**
 * THE QQBot adapter. Transport-only surface on top of the kit base: guard
 * composition, chunking, formatting ladder and egress doors are inherited.
 */
export class QQBotAdapter extends BasePlatformAdapter {
	readonly pluginManifest = QQBOT_PLUGIN_MANIFEST;

	// ── config (__init__ parity) ─────────────────────────────────────────────
	readonly appId: string;
	readonly clientSecret: string;
	readonly markdownSupport: boolean;
	readonly dmPolicy: string;
	readonly groupPolicy: string;
	readonly allowFrom: readonly string[];
	readonly groupAllowFrom: readonly string[];

	private readonly rest: QQRestTransport;
	private readonly gateway: FakeQQGateway;
	private readonly sleepFn: (ms: number) => Promise<void>;
	private readonly nowFn: () => number;
	private readonly richProbe:
		| ((content: string) => Promise<SendResult>)
		| undefined;
	private readonly richHasScriptFn: (() => boolean) | undefined;
	private richWireAttemptsCount = 0;

	// ── gateway state ────────────────────────────────────────────────────────
	private socket: QQClientSocket | null = null;
	sessionId: string | null = null;
	lastSeq: number | null = null;
	heartbeatIntervalS = 30.0;

	// Token cache (_ensure_token parity).
	private accessToken: string | null = null;
	private tokenExpiresAtMs = 0;
	private tokenFetch: Promise<string> | null = null;

	/** chat_id → chat kind, learned from inbound traffic (_chat_type_map). */
	readonly chatTypeMap = new Map<string, QQChatType>();
	/** Last inbound message id per chat — passive reply_to context (_last_msg_id). */
	readonly lastMsgIdByChat = new Map<string, string>();

	private readonly seenMessages: BoundedSeenSet;

	// ── reconnect machinery (_listen_loop parity) ────────────────────────────
	private backoffIdx = 0;
	private quickDisconnectCount = 0;
	private connectStartedAtMs: number | null = null;
	running = false;
	isLive = false;

	readonly reconnectSteps: QQReconnectStep[] = [];
	reconnectLog: string[] = [];
	/** Server-authoritative capture: close 4008 ⇒ RATE_LIMIT_DELAY (60s). */
	lastCapturedRetryAfterSeconds: number | null = null;
	private pendingRetryAfterS: number | null = null;

	// ── interaction audit ────────────────────────────────────────────────────
	readonly interactionAcks: Array<{ id: string; code: number }> = [];
	readonly resolvedFamilies: string[] = [];
	readonly approvalDecisions: Array<{
		sessionKey: string;
		decision: "once" | "always" | "deny" | "session";
	}> = [];
	readonly updatePromptAnswers: Array<{ answer: "y" | "n"; operator: string }> =
		[];

	// ── subject-support plumbing (reference-fixture inheritance) ────────────
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	private readonly cp: EgressChokepoint;
	private allowAllClickers = true;
	private readonly clarifyArmedSet = new Set<string>();
	private holding = false;
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};

	constructor(opts: QQAdapterOptions) {
		super({
			manifestName: QQBOT_PLUGIN_MANIFEST.name,
			capabilities: QQBOT_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? QQBOT_MAX_MESSAGE_LENGTH,
		});
		this.appId = (opts.appId ?? "").trim();
		this.clientSecret = (opts.clientSecret ?? "").trim();
		this.markdownSupport = opts.markdownSupport !== false;
		this.dmPolicy = (opts.dmPolicy ?? "pairing").toLowerCase();
		this.groupPolicy = (opts.groupPolicy ?? "pairing").toLowerCase();
		this.allowFrom = opts.allowFrom ?? [];
		this.groupAllowFrom = opts.groupAllowFrom ?? [];
		this.rest = opts.rest;
		this.gateway = opts.wsFactory;
		this.sleepFn =
			opts.sleepMs ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.richProbe = opts.richProbe;
		this.richHasScriptFn = opts.richHasScript;

		if (this.appId === "" || this.clientSecret === "") {
			// Loud-disable parity: connect() refuses without credentials; the
			// lifecycle records the reason so /status shows it.
			this.lifecycle.disable({
				kind: "secret_missing",
				secretKey: this.clientSecret === "" ? "QQ_CLIENT_SECRET" : "QQ_APP_ID",
				manifestName: QQBOT_PLUGIN_MANIFEST.name,
			});
		}
		this.registerLogSecret(this.clientSecret);

		this.seenMessages = new BoundedSeenSet({
			maxEntries: QQBOT_DEDUP_MAX_SIZE,
			ttlMs: QQBOT_DEDUP_WINDOW_SECONDS * 1000,
			nowMs: this.nowFn,
		});

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // no native draft lanes on QQ v2 wire
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			transmitEdit: async () => ({ success: false, error: "Not supported" }),
			transmitSeal: async () => ({ success: false, error: "Not supported" }),
		});

		const authorizer: ClickAuthorizer = () => this.allowAllClickers;
		this.router = new CallbackQueryRouter({
			stores: {
				approvals: this.approvals,
				slashConfirms: this.slashConfirms,
				appr: this.appr,
				clarify: this.clarify,
			},
			authorizer,
			onExecApproval: async (sessionKey, choice) => {
				this.resolvedFamilies.push("ea");
				this.approvalDecisions.push({ sessionKey, decision: choice });
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

	get isConnected(): boolean {
		return this.isLive;
	}

	/**
	 * Per-chat descriptor override point (§6.3/A15): budget AND unit resolve
	 * TOGETHER here — the harness's utf16-marked chats prove code-unit math.
	 */
	protected override chatDescriptorFor(chatId: string):
		| {
				maxMessageLength?: number | undefined;
				lenUnit?: import("../kit/length-policy.js").LengthUnit | undefined;
		  }
		| undefined {
		if (chatId.includes("utf16")) {
			return { maxMessageLength: 30, lenUnit: "utf16" };
		}
		return undefined;
	}

	// ── guard wiring (reference-fixture inheritance) ──────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		const spawnerOpts = spawner === undefined ? {} : { spawner };
		this.attachGuard(
			{
				registry: QQ_REGISTRY,
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
				...spawnerOpts,
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

	// ── egress doors ──────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * Wire transport for ONE delivered chunk (base deliverChunk lane). Routes
	 * to c2c/group/guild REST sends by learned chat kind (_guess_chat_type
	 * fallback "c2c"), wrapped in the _send_chunk retry ladder.
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		// Capture-seam interception (reference-fixture parity): the shared rows'
		// formatting-rejection script fails markdown-shaped bodies with a
		// parse-classified vendor error; the PLAIN fallback body succeeds.
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
		) {
			return {
				success: false,
				error: "Bad Request: can't parse entities",
			};
		}
		const replyToRaw = metadata["reply_to"];
		const replyTo = typeof replyToRaw === "string" ? replyToRaw : undefined;
		return this.sendChunkWithRetry(chatId, content, replyTo);
	}

	/** Rich lane ABSENT natively; scripted probes feed the §10.1 latch path. */
	protected override async wireRich(content: string): Promise<SendResult> {
		const scripted =
			this.richProbe !== undefined &&
			(this.richHasScriptFn === undefined || this.richHasScriptFn());
		if (!scripted) {
			// Capability-error shape WITHOUT burning a roundtrip (latch path).
			return { success: false, error: "sendWithKeyboard: method not found" };
		}
		this.richWireAttemptsCount += 1;
		return this.richProbe(content);
	}

	/** Observability: how many REAL rich roundtrips left the adapter. */
	get richWireAttempts(): number {
		return this.richWireAttemptsCount;
	}

	// ── connection lifecycle (connect/disconnect parity) ─────────────────────

	async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		if (this.appId === "" || this.clientSecret === "") {
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail:
					"QQ startup failed: QQ_APP_ID and QQ_CLIENT_SECRET are required",
			});
			return false;
		}
		try {
			await this.ensureToken();
			await this.openWs();
			this.running = true;
			this.isLive = true;
			this.connectStartedAtMs = this.nowFn();
			void this.heartbeatLoop();
			return true;
		} catch (err) {
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail: `QQ startup failed: ${err instanceof Error ? err.message : String(err)}`,
			});
			return false;
		}
	}

	async disconnect(): Promise<void> {
		this.running = false;
		this.isLive = false;
		this.socket?.close(1000);
		this.socket = null;
		this.sessionId = null;
		this.lastSeq = null;
	}

	private async openWs(): Promise<void> {
		const gatewayUrl = await this.getGatewayUrl();
		void gatewayUrl;
		const listener: QQSocketListener = {
			onOpen: () => {},
			onPayload: (payload) => this.dispatchPayload(payload),
			onClose: (info) => void this.handleClose(info.code, info.reason ?? ""),
			onError: (err) => void this.handleReadError(err),
		};
		this.socket = this.gateway.connect(listener);
		// Hello arrives synchronously via serverAccept → onPayload; yield once
		// so the Identify/READY round-trip settles before connect() returns.
		await Promise.resolve();
	}

	// ── token management (_ensure_token parity) ─────────────────────────────

	async ensureToken(): Promise<string> {
		const cached = this.accessToken;
		if (
			cached !== null &&
			this.nowFn() < this.tokenExpiresAtMs - QQ_TOKEN_REFRESH_MARGIN_S * 1000
		) {
			return cached;
		}
		// Singleflight: concurrent callers share one fetch.
		if (this.tokenFetch !== null) return this.tokenFetch;
		this.tokenFetch = (async () => {
			const resp = await this.rest.request("POST", QQBOT_TOKEN_URL, {
				appId: this.appId,
				clientSecret: this.clientSecret,
			});
			if (resp.status >= 400) {
				throw new Error(
					`Failed to get QQ Bot access token: HTTP ${resp.status}`,
				);
			}
			const token = resp.body["access_token"];
			if (typeof token !== "string" || token === "") {
				throw new Error("QQ Bot token response missing access_token");
			}
			const expiresIn = Number(
				resp.body["expires_in"] ?? QQ_TOKEN_DEFAULT_EXPIRES_IN_S,
			);
			this.accessToken = token;
			this.tokenExpiresAtMs = this.nowFn() + expiresIn * 1000;
			return token;
		})();
		try {
			return await this.tokenFetch;
		} finally {
			this.tokenFetch = null;
		}
	}

	private async getGatewayUrl(): Promise<string> {
		const token = await this.ensureToken();
		const resp = await this.rest.request(
			"GET",
			`${QQBOT_API_BASE}${QQBOT_GATEWAY_URL_PATH}`,
			{},
			{ Authorization: `QQBot ${token}` },
		);
		if (resp.status >= 400) {
			throw new Error(`Failed to get QQ Bot gateway URL: HTTP ${resp.status}`);
		}
		const url = resp.body["url"];
		if (typeof url !== "string" || url === "") {
			throw new Error("QQ Bot gateway response missing url");
		}
		return url;
	}

	// ── op-code routing (_dispatch_payload parity) ───────────────────────────

	dispatchPayload(payload: QQGatewayPayload): void {
		if (typeof payload.s === "number") {
			const s = payload.s;
			this.lastSeq =
				this.lastSeq === null || s > this.lastSeq ? s : this.lastSeq;
		}
		const op = payload.op;

		if (op === 10) {
			// Hello — heartbeat at 80% of the server interval; resume when we
			// hold a session, identify otherwise.
			const d = (payload.d ?? {}) as Record<string, unknown>;
			const intervalMs = Number(d["heartbeat_interval"] ?? 30_000);
			this.heartbeatIntervalS =
				(intervalMs / 1000) * QQ_HEARTBEAT_FRACTION_OF_INTERVAL;
			if (this.sessionId !== null && this.lastSeq !== null) {
				this.sendResume();
			} else {
				this.sendIdentify();
			}
			return;
		}
		if (op === 0 && payload.t !== undefined) {
			switch (payload.t) {
				case "READY":
					this.handleReady(payload.d);
					return;
				case "RESUMED":
					this.reconnectLog.push("resumed");
					return;
				case "C2C_MESSAGE_CREATE":
				case "GROUP_AT_MESSAGE_CREATE":
				case "DIRECT_MESSAGE_CREATE":
				case "GUILD_MESSAGE_CREATE":
				case "GUILD_AT_MESSAGE_CREATE":
					void this.onMessage(
						payload.t,
						(payload.d ?? {}) as Record<string, unknown>,
					);
					return;
				case "INTERACTION_CREATE":
					void this.onInteraction((payload.d ?? {}) as Record<string, unknown>);
					return;
				default:
					return;
			}
		}
		if (op === 11) return; // heartbeat ACK
		if (op === 7) {
			// Server-requested reconnect → close so the close path resumes.
			this.socket?.close(4000);
			return;
		}
		if (op === 9) {
			// d=true resumable, d=false fresh-identify.
			if (payload.d !== true) {
				this.sessionId = null;
				this.lastSeq = null;
			}
			this.socket?.close(4000);
			return;
		}
	}

	private handleReady(d: unknown): void {
		if (d !== null && typeof d === "object") {
			const sid = (d as Record<string, unknown>)["session_id"];
			if (typeof sid === "string") this.sessionId = sid;
		}
	}

	private sendIdentify(): void {
		this.socket?.sendPayload({
			op: 2,
			d: {
				token: `QQBot ${this.accessToken ?? ""}`,
				intents: QQ_IDENTIFY_INTENTS,
				shard: [0, 1],
				properties: {
					$os: "linux",
					$browser: "pi-gateway",
					$device: "pi-gateway",
				},
			},
		});
	}

	private sendResume(): void {
		this.socket?.sendPayload({
			op: 6,
			d: {
				token: `QQBot ${this.accessToken ?? ""}`,
				session_id: this.sessionId,
				seq: this.lastSeq,
			},
		});
	}

	// ── heartbeat loop (_heartbeat_loop parity) ─────────────────────────────

	protected async heartbeatLoop(): Promise<void> {
		while (this.running) {
			await this.sleepFn(this.heartbeatIntervalS * 1000);
			if (!this.running || this.socket === null) continue;
			try {
				this.socket.sendPayload({ op: 1, d: this.lastSeq });
			} catch {
				// heartbeat failures are non-fatal; the read path owns liveness
			}
		}
	}

	// ── close-code classes (_listen_loop parity) ─────────────────────────────

	async handleClose(code: number, reason: string): Promise<void> {
		if (code === 1000) {
			// deliberate disconnect()
			return;
		}
		this.isLive = false;

		// Quick-disconnect detection (permission/misconfiguration shape).
		const lifetimeS =
			this.connectStartedAtMs === null
				? Number.POSITIVE_INFINITY
				: (this.nowFn() - this.connectStartedAtMs) / 1000;
		if (lifetimeS < QQBOT_QUICK_DISCONNECT_THRESHOLD_S) {
			this.quickDisconnectCount += 1;
		} else {
			this.quickDisconnectCount = 0;
		}
		if (this.quickDisconnectCount >= QQBOT_MAX_QUICK_DISCONNECT_COUNT) {
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail: "Too many quick disconnects — check bot permissions",
			});
			this.reconnectLog.push(`fatal:quick-disconnect(${reason})`);
			return;
		}

		if (FATAL_CLOSE_CODES.has(code)) {
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail: `Bot closed fatally (${code}) — not reconnecting`,
			});
			this.reconnectLog.push(`fatal:close-${code}`);
			return;
		}

		if (code === 4008) {
			// Rate limited: RATE_LIMIT_DELAY IS the server-authoritative capture.
			this.lastCapturedRetryAfterSeconds = QQBOT_RATE_LIMIT_DELAY_S;
			this.pendingRetryAfterS = QQBOT_RATE_LIMIT_DELAY_S;
			this.reconnectLog.push("rate-limited-4008");
		} else if (code === 4004) {
			// Invalid token: clear the cache so ensureToken refreshes.
			this.accessToken = null;
			this.tokenExpiresAtMs = 0;
			this.reconnectLog.push("invalid-token-4004");
		} else if (SESSION_INVALID_CLOSE_CODES.has(code)) {
			// Session invalid → fresh Identify next Hello (4009 stays resumable).
			this.sessionId = null;
			this.lastSeq = null;
			this.reconnectLog.push(`session-invalid-${code}`);
		} else if (code === 1006) {
			this.reconnectLog.push("refused");
		} else {
			this.reconnectLog.push(`close-${code}`);
		}

		await this.reconnectAfterClose();
	}

	/** Hard transport death without a close frame (dead-TCP shape). */
	async handleReadError(err: Error): Promise<void> {
		this.isLive = false;
		this.reconnectLog.push(`read-error:${err.message.slice(0, 40)}`);
		if (this.running) await this.reconnectAfterClose();
	}

	private async reconnectAfterClose(): Promise<void> {
		if (this.backoffIdx >= QQBOT_RECONNECT_BACKOFF_S.length) {
			this.lifecycle.disable({
				kind: "config_invalid",
				detail: "Max reconnect attempts reached (backoff tiers exhausted)",
			});
			return;
		}
		const tierDelayS = QQBOT_RECONNECT_BACKOFF_S[this.backoffIdx]!;
		const captured = this.pendingRetryAfterS;
		let delayS: number;
		let authoritative: boolean;
		if (captured === null) {
			delayS = tierDelayS;
			authoritative = false;
		} else {
			delayS = captured; // server-authoritative verbatim
			this.pendingRetryAfterS = null;
			authoritative = true;
		}
		this.reconnectSteps.push({
			delayMs: delayS * 1000,
			authoritative,
			attempt: this.backoffIdx + 1,
		});
		await this.sleepFn(delayS * 1000);
		this.backoffIdx += 1;
		this.connectStartedAtMs = this.nowFn();

		try {
			if (this.accessToken === null) await this.ensureToken();
			await this.openWs();
			this.isLive = true;
			this.backoffIdx = 0;
			this.quickDisconnectCount = 0;
			this.reconnectLog.push("reconnected");
		} catch {
			this.isLive = false;
			await this.reconnectAfterClose();
		}
	}

	// ── inbound messages (_on_message + per-kind handlers) ──────────────────

	async onMessage(
		eventType: string,
		d: Record<string, unknown>,
	): Promise<void> {
		const msgId = String(d["id"] ?? "");
		if (msgId === "" || this.isDuplicate(msgId)) return;
		const content = String(d["content"] ?? "").trim();
		const authorRaw = d["author"];
		const author =
			authorRaw !== null && typeof authorRaw === "object"
				? (authorRaw as Record<string, unknown>)
				: {};

		switch (eventType) {
			case "C2C_MESSAGE_CREATE":
				this.handleC2CMessage(d, msgId, content, author);
				return;
			case "GROUP_AT_MESSAGE_CREATE":
				this.handleGroupMessage(d, msgId, content, author);
				return;
			case "GUILD_MESSAGE_CREATE":
			case "GUILD_AT_MESSAGE_CREATE":
				this.handleGuildMessage(d, msgId, content, author);
				return;
			case "DIRECT_MESSAGE_CREATE":
				this.handleGuildDmMessage(d, msgId, content, author);
				return;
		}
	}

	private handleC2CMessage(
		d: Record<string, unknown>,
		msgId: string,
		content: string,
		author: Record<string, unknown>,
	): void {
		const userOpenid = String(author["user_openid"] ?? "");
		if (userOpenid === "") return;
		if (!this.isDmIntakeAllowed(userOpenid)) return;
		const imageUrls = collectAttachmentUrls(d);
		const text = mergeQuote(content, extractQuoteBlock(d));
		if (text.trim() === "" && imageUrls.length === 0) return;
		this.chatTypeMap.set(userOpenid, "c2c");
		this.lastMsgIdByChat.set(userOpenid, msgId);
		const event = buildTextEvent({
			chatType: "dm",
			chatId: userOpenid,
			userId: userOpenid,
			text,
			messageId: msgId,
		});
		void this.deliverInbound(event, `qqbot:dm:${userOpenid}:${userOpenid}`);
	}

	private handleGroupMessage(
		d: Record<string, unknown>,
		msgId: string,
		content: string,
		author: Record<string, unknown>,
	): void {
		const groupOpenid = String(d["group_openid"] ?? "");
		if (groupOpenid === "") return;
		const memberOpenid = String(author["member_openid"] ?? "");
		if (!this.isGroupAllowed(groupOpenid, memberOpenid)) return;
		const imageUrls = collectAttachmentUrls(d);
		const text = mergeQuote(stripAtMention(content), extractQuoteBlock(d));
		if (text.trim() === "" && imageUrls.length === 0) return;
		this.chatTypeMap.set(groupOpenid, "group");
		this.lastMsgIdByChat.set(groupOpenid, msgId);
		const event = buildTextEvent({
			chatType: "group",
			chatId: groupOpenid,
			userId: memberOpenid,
			text,
			messageId: msgId,
		});
		void this.deliverInbound(
			event,
			`qqbot:group:${groupOpenid}:${memberOpenid}`,
		);
	}

	private handleGuildMessage(
		d: Record<string, unknown>,
		msgId: string,
		content: string,
		author: Record<string, unknown>,
	): void {
		const channelId = String(d["channel_id"] ?? "");
		if (channelId === "") return;
		const guildId = String(d["guild_id"] ?? "");
		const authorId = String(author["id"] ?? "");
		// Guild channels are group-like contexts: group ACL applies
		// (any-member-of-any-guild bypass prevention).
		if (!this.isGroupAllowed(guildId === "" ? channelId : guildId, authorId)) {
			return;
		}
		const imageUrls = collectAttachmentUrls(d);
		const text = mergeQuote(content, extractQuoteBlock(d));
		if (text.trim() === "" && imageUrls.length === 0) return;
		this.chatTypeMap.set(channelId, "guild");
		this.lastMsgIdByChat.set(channelId, msgId);
		const event = buildTextEvent({
			chatType: "group",
			chatId: channelId,
			userId: authorId,
			text,
			messageId: msgId,
		});
		void this.deliverInbound(event, `qqbot:guild:${channelId}:${authorId}`);
	}

	private handleGuildDmMessage(
		d: Record<string, unknown>,
		msgId: string,
		content: string,
		author: Record<string, unknown>,
	): void {
		const guildId = String(d["guild_id"] ?? "");
		if (guildId === "") return;
		const authorId = String(author["id"] ?? "");
		if (!this.isDmIntakeAllowed(authorId)) return;
		const imageUrls = collectAttachmentUrls(d);
		const text = mergeQuote(content, extractQuoteBlock(d));
		if (text.trim() === "" && imageUrls.length === 0) return;
		this.chatTypeMap.set(guildId, "dm");
		this.lastMsgIdByChat.set(guildId, msgId);
		const event = buildTextEvent({
			chatType: "dm",
			chatId: guildId,
			userId: authorId,
			text,
			messageId: msgId,
		});
		void this.deliverInbound(event, `qqbot:dm:${guildId}:${authorId}`);
	}

	// ── ACL intake gates (_is_dm_intake_allowed / _is_group_allowed) ────────

	openDmOptedIn(): boolean {
		return ["true", "1", "yes"].includes(
			(process.env["PI_QQ_ALLOW_ALL_USERS"] ?? "").toLowerCase(),
		);
	}

	isDmAllowed(userId: string): boolean {
		if (this.dmPolicy === "disabled") return false;
		if (this.dmPolicy === "allowlist")
			return entryMatches(this.allowFrom, userId);
		if (this.dmPolicy === "open") return this.openDmOptedIn();
		return false;
	}

	isDmIntakeAllowed(userId: string): boolean {
		const principal = String(userId ?? "").trim();
		if (principal === "") return false;
		if (this.dmPolicy === "disabled") return false;
		if (this.dmPolicy === "allowlist")
			return entryMatches(this.allowFrom, principal);
		if (this.dmPolicy === "pairing") return true;
		if (this.dmPolicy === "open") return this.openDmOptedIn();
		return false;
	}

	isGroupAllowed(groupId: string, _userId: string): boolean {
		if (this.groupPolicy === "disabled") return false;
		if (this.groupPolicy === "allowlist")
			return entryMatches(this.groupAllowFrom, groupId);
		if (this.groupPolicy === "pairing") return false;
		if (this.groupPolicy === "open") return true;
		return false;
	}

	// ── dedup (_is_duplicate parity via BoundedSeenSet TTL+cap) ─────────────

	isDuplicate(msgId: string): boolean {
		// add() returns false when a LIVE entry exists (replay).
		return !this.seenMessages.add(msgId);
	}

	// ── interactions (_on_interaction parity) ────────────────────────────────

	async onInteraction(raw: Record<string, unknown>): Promise<void> {
		let event: InteractionEvent;
		try {
			event = parseInteractionEvent(raw);
		} catch {
			return;
		}
		if (event.id === "") return;

		// ACK promptly — PUT /interactions/{id} {code:0}; the client shows an
		// error icon otherwise. ACK happens BEFORE dispatch.
		try {
			const token = await this.ensureToken();
			const resp = await this.rest.request(
				"PUT",
				`${QQBOT_API_BASE}/interactions/${event.id}`,
				{ code: 0 },
				{ Authorization: `QQBot ${token}` },
			);
			this.interactionAcks.push({
				id: event.id,
				code: resp.status < 400 ? 0 : resp.status,
			});
		} catch {
			this.interactionAcks.push({ id: event.id, code: -1 });
		}

		const approval = parseApprovalButtonData(event.buttonData);
		if (approval !== null) {
			const [sessionKey, rawDecision] = approval;
			if (!this.isAuthorizedInteractionForSession(event, sessionKey)) {
				this.reconnectLog.push(
					`unauthorized-approval-click(operator=${event.operatorOpenid})`,
				);
				return;
			}
			const choice =
				rawDecision === "allow-once"
					? ("once" as const)
					: rawDecision === "allow-always"
						? ("always" as const)
						: ("deny" as const);
			this.resolvedFamilies.push("ea");
			this.approvalDecisions.push({ sessionKey, decision: choice });
			return;
		}
		const updateAnswer = parseUpdatePromptButtonData(event.buttonData);
		if (updateAnswer !== null) {
			const updateSessionKey = `agent:main:qqbot:${event.scene}:${
				event.groupOpenid || event.guildId || event.userOpenid
			}`;
			if (!this.isAuthorizedInteractionForSession(event, updateSessionKey)) {
				this.reconnectLog.push("unauthorized-update-prompt-click");
				return;
			}
			this.updatePromptAnswers.push({
				answer: updateAnswer,
				operator: event.operatorOpenid,
			});
			return;
		}
		// Unrecognised button data: logged-and-dropped (never a turn).
		this.resolvedFamilies.push("unknown-button");
	}

	/**
	 * Session+operator authorization for button clicks
	 * (adapter.py:_is_authorized_interaction_for_session): c2c requires
	 * operator==chat_id; group/guild require event-chat match AND operator ==
	 * the session's trailing user segment.
	 */
	isAuthorizedInteractionForSession(
		event: InteractionEvent,
		sessionKey: string,
	): boolean {
		const parts = String(sessionKey ?? "").split(":");
		if (parts.length < 5 || parts[0] !== "agent" || parts[1] !== "main") {
			return false;
		}
		const platform = parts[2];
		const chatType = parts[3];
		const chatId = parts[4];
		const operator = String(event.operatorOpenid ?? "").trim();
		if (platform !== "qqbot" || operator === "") return false;
		if (chatType === "c2c") {
			return chatId !== undefined && chatId !== "" && operator === chatId;
		}
		if (chatType === "group" || chatType === "guild") {
			const eventChat = String(event.groupOpenid || event.guildId || "").trim();
			if (eventChat === "" || eventChat !== chatId) return false;
			const sessionUser = String(parts[5] ?? "").trim();
			return sessionUser !== "" && operator === sessionUser;
		}
		return false;
	}

	// ── outbound: keyboards (send_with_keyboard parity) ─────────────────────

	/**
	 * Send one message with an inline keyboard attached. NO splitting — a
	 * keyboard message has exactly one interactive surface. Guild channels do
	 * not support inline keyboards (non-retryable failure).
	 */
	async sendWithKeyboard(
		chatId: string,
		content: string,
		keyboard: InlineKeyboardWire,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		const chatType = this.guessChatType(chatId);
		const truncated = this.formatMessage(content).slice(
			0,
			QQBOT_MAX_MESSAGE_LENGTH,
		);
		if (chatType === "c2c" || chatType === "group") {
			return this.sendTextRest(chatId, truncated, chatType, {
				replyTo,
				keyboard,
			});
		}
		return {
			success: false,
			error: `Inline keyboards not supported for chat_type '${chatType}'`,
			retryable: false,
		};
	}

	async sendApprovalRequest(opts: {
		chatId: string;
		sessionKey: string;
		title: string;
		description?: string | undefined;
		commandPreview?: string | undefined;
		timeoutSec?: number | undefined;
		allowPermanent?: boolean | undefined;
		replyTo?: string | undefined;
	}): Promise<SendResult> {
		return this.sendWithKeyboard(
			opts.chatId,
			renderApprovalText(opts),
			buildApprovalKeyboard(opts.sessionKey, {
				allowPermanent: opts.allowPermanent,
			}),
			opts.replyTo,
		);
	}

	async sendUpdatePrompt(opts: {
		chatId: string;
		prompt: string;
		fallbackDefault?: string | undefined;
	}): Promise<SendResult> {
		const hint =
			opts.fallbackDefault === undefined || opts.fallbackDefault === ""
				? ""
				: ` (default: ${opts.fallbackDefault})`;
		const content = `⚕ **Update Needs Your Input**\n\n${opts.prompt}${hint}`;
		const msgId = this.lastMsgIdByChat.get(opts.chatId);
		return this.sendWithKeyboard(
			opts.chatId,
			content,
			buildUpdatePromptKeyboard(),
			msgId,
		);
	}

	// ── outbound: media upload (_upload_media parity) ────────────────────────

	async uploadMedia(opts: {
		chatType: "c2c" | "group";
		targetId: string;
		fileType: number;
		url?: string | undefined;
		fileData?: string | undefined;
		fileName?: string | undefined;
		srvSendMsg?: boolean | undefined;
	}): Promise<Record<string, unknown>> {
		const path =
			opts.chatType === "c2c"
				? `/v2/users/${opts.targetId}/files`
				: `/v2/groups/${opts.targetId}/files`;
		const body: Record<string, unknown> = {
			file_type: opts.fileType,
			srv_send_msg: opts.srvSendMsg === true,
		};
		if (opts.url !== undefined) body["url"] = opts.url;
		else if (opts.fileData !== undefined) body["file_data"] = opts.fileData;
		if (opts.fileName !== undefined) body["file_name"] = opts.fileName;
		for (let attempt = 0; attempt < 3; attempt++) {
			try {
				return await this.apiRequest(
					"POST",
					path,
					body,
					QQBOT_FILE_UPLOAD_TIMEOUT_S,
				);
			} catch (err) {
				const msg = err instanceof Error ? err.message : String(err);
				if (/400|401|Invalid|timeout|Timeout/.test(msg)) throw err;
				if (attempt < 2) await this.sleepFn(1500 * (attempt + 1));
				else throw err;
			}
		}
		throw new Error("upload retries exhausted");
	}

	/** Chunked-upload driver over THIS adapter's API seams. */
	chunkedUploader(): ChunkedUploader {
		return new ChunkedUploader({
			apiRequest: (method, path, body, timeoutS) =>
				this.apiRequest(method, path, body, timeoutS),
			httpPut: async (url, data, headers) => {
				// COS part PUTs route through the fake's scripted face; the bytes
				// ride the transport seam verbatim (Content-Length recorded).
				const resp = await this.rest.request("PUT", url, data, {
					...headers,
				});
				return { status: resp.status, text: JSON.stringify(resp.body ?? {}) };
			},
			sleep: this.sleepFn,
			monotonicMs: this.nowFn,
		});
	}

	// ── outbound: REST text sends (_build_text_body + per-type paths) ───────

	guessChatType(chatId: string): QQChatType {
		return this.chatTypeMap.get(chatId) ?? "c2c";
	}

	formatMessage(content: string): string {
		// markdown_support=true → verbatim (QQ renders it natively).
		if (this.markdownSupport) return content;
		return stripMarkdownLite(content);
	}

	buildTextBody(
		content: string,
		replyTo?: string | undefined,
	): Record<string, unknown> {
		const msgSeq = nextMsgSeq(this.nowFn);
		if (this.markdownSupport) {
			return {
				markdown: { content: content.slice(0, QQBOT_MAX_MESSAGE_LENGTH) },
				msg_type: QQ_MSG_TYPE_MARKDOWN,
				msg_seq: msgSeq,
			};
		}
		const reference =
			replyTo === undefined
				? {}
				: { message_reference: { message_id: replyTo } };
		return {
			content: content.slice(0, QQBOT_MAX_MESSAGE_LENGTH),
			msg_type: QQ_MSG_TYPE_TEXT,
			msg_seq: msgSeq,
			...reference,
		};
	}

	async apiRequest(
		method: "POST" | "GET" | "PUT",
		path: string,
		body?: Record<string, unknown> | Buffer | undefined,
		timeoutS: number = 30,
	): Promise<Record<string, unknown>> {
		void timeoutS;
		const token = await this.ensureToken();
		const payload: Record<string, unknown> | Buffer = body ?? {};
		const resp = await this.rest.request(
			method,
			`${QQBOT_API_BASE}${path}`,
			payload,
			{
				Authorization: `QQBot ${token}`,
				"Content-Type": "application/json",
			},
		);
		if (resp.status >= 400) {
			const vendorMessage = String(
				resp.body["message"] ?? JSON.stringify(resp.body),
			);
			// Hermes embeds status + path + vendor message into ONE blob so
			// numeric biz_code matching stays possible downstream.
			throw new Error(
				`QQ Bot API error [${resp.status}] ${path}: ${vendorMessage}`,
			);
		}
		return resp.body;
	}

	/** Capture knob: REST failure results carrying retryAfter feed the ladder. */
	noteRestRetryAfter(seconds: number): void {
		this.lastCapturedRetryAfterSeconds = seconds;
	}

	private async sendChunkWithRetry(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		let lastError: unknown = null;
		let honoredRetryAfterOnce = false;
		for (let attempt = 0; attempt < QQ_SEND_MAX_ATTEMPTS; attempt++) {
			try {
				const result = await this.sendChunkOnce(chatId, content, replyTo);
				if (result.success) return result;
				lastError = new Error(result.error ?? "unknown send failure");
				const errBlob = String(result.error ?? "").toLowerCase();
				const permanent = PERMANENT_SEND_PATTERNS.some((k) =>
					errBlob.includes(k),
				);
				if (permanent) break;
				// Timeout-ambiguous results are never retried either.
				if (classifySendError(lastError) === "timeout") break;
				const ra = result.retryAfter;
				if (ra !== undefined && ra !== null) this.noteRestRetryAfter(ra);
			} catch (err) {
				lastError = err;
				const errBlob = err instanceof Error ? err.message.toLowerCase() : "";
				const permanent = PERMANENT_SEND_PATTERNS.some((k) =>
					errBlob.includes(k),
				);
				if (permanent) break;
				// Timeout-AMBIGUOUS failures are never retried (the send may have
				// landed — duplicate risk, §6.1 base ladder parity).
				if (classifySendError(err) === "timeout") break;
				// Server-authoritative retry_after honored ONCE over the local
				// schedule (_send_with_retry parity) and captured for the ladder.
				if (!honoredRetryAfterOnce) {
					const raErr = extractRetryAfterSeconds(err);
					if (raErr !== null && raErr >= 0) {
						honoredRetryAfterOnce = true;
						this.noteRestRetryAfter(raErr);
						await this.sleepFn(raErr * 1000);
					}
				}
			}
			if (attempt < QQ_SEND_MAX_ATTEMPTS - 1) {
				await this.sleepFn(QQ_SEND_RETRY_BASE_DELAY_S * 1000 * 2 ** attempt);
			}
		}
		const message =
			lastError instanceof Error
				? lastError.message
				: String(lastError ?? "Unknown error");
		const lower = message.toLowerCase();
		const permanentMiss = !PERMANENT_SEND_PATTERNS.slice(0, 3).some((k) =>
			lower.includes(k),
		);
		// Timeout-ambiguous failures surface NON-retryable (§6.1: ambiguous
		// sends are never re-driven — neither here nor by the caller).
		const isTimeout = classifySendError(new Error(message)) === "timeout";
		return {
			success: false,
			error: message,
			retryable: permanentMiss && !isTimeout,
		};
	}

	private async sendChunkOnce(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		// format_message parity: applied INSIDE the send path (markdown
		// support rides verbatim; disabled mode strips before the wire).
		const formatted = this.formatMessage(content);
		const chatType = this.guessChatType(chatId);
		if (chatType === "guild") {
			const guildBody: Record<string, unknown> = {
				content: formatted.slice(0, QQBOT_MAX_MESSAGE_LENGTH),
			};
			if (replyTo !== undefined) guildBody["msg_id"] = replyTo;
			const data = await this.apiRequest(
				"POST",
				`/channels/${chatId}/messages`,
				guildBody,
			);
			return { success: true, messageId: String(data["id"] ?? "unknown") };
		}
		return this.sendTextRest(chatId, formatted, chatType as "c2c" | "group", {
			replyTo,
		});
	}

	private async sendTextRest(
		id: string,
		content: string,
		chatType: "c2c" | "group",
		opts: {
			replyTo?: string | undefined;
			keyboard?: InlineKeyboardWire | undefined;
		} = {},
	): Promise<SendResult> {
		const body = this.buildTextBody(content, opts.replyTo);
		if (opts.replyTo !== undefined) body["msg_id"] = opts.replyTo;
		if (opts.keyboard !== undefined) body["keyboard"] = opts.keyboard;
		const path =
			chatType === "c2c"
				? `/v2/users/${id}/messages`
				: `/v2/groups/${id}/messages`;
		const data = await this.apiRequest("POST", path, body);
		return { success: true, messageId: String(data["id"] ?? "unknown") };
	}
}

// ── module-level helpers ────────────────────────────────────────────────────

function entryMatches(entries: readonly string[], target: string): boolean {
	const normalizedTarget = String(target).trim().toLowerCase();
	for (const entry of entries) {
		const normalized = entry.trim().toLowerCase();
		if (normalized === "*" || normalized === normalizedTarget) return true;
	}
	return false;
}

function stripAtMention(content: string): string {
	return content.trim().replace(/^@\S+\s*/, "");
}

function stripMarkdownLite(content: string): string {
	return content
		.replace(/\*\*([^*]+)\*\*/g, "$1")
		.replace(/__([^_]+)__/g, "$1")
		.replace(/`([^`]*)`/g, "$1")
		.replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1");
}

/** Deterministic 0..65535 seq (adapter.py:_next_msg_seq semantics). */
export function nextMsgSeq(nowMs: () => number): number {
	const timePart = Math.floor(nowMs() / 1000) % 100000000;
	const rand = Math.floor(Math.random() * 0x10000);
	return (timePart ^ rand) % 65536;
}

/** Attachment URL extraction (attachments[].url carries media URLs). */
function collectAttachmentUrls(d: Record<string, unknown>): string[] {
	const atts = d["attachments"];
	if (!Array.isArray(atts)) return [];
	const urls: string[] = [];
	for (const a of atts) {
		if (a !== null && typeof a === "object") {
			const url = (a as Record<string, unknown>)["url"];
			if (typeof url === "string" && url !== "") urls.push(url);
		}
	}
	return urls;
}

/**
 * Quoted-context extraction (adapter.py:_process_quoted_context):
 * message_type=103 → msg_elements carry the referenced content; the quote
 * block PREPENDS with a blank-line separator (_merge_quote_into).
 */
function extractQuoteBlock(d: Record<string, unknown>): string {
	const messageType = Number(d["message_type"] ?? 0) || 0;
	if (messageType !== 103) return "";
	const elements = d["msg_elements"];
	if (!Array.isArray(elements) || elements.length === 0) return "";
	const parts: string[] = [];
	for (const elem of elements) {
		if (elem !== null && typeof elem === "object") {
			const etext = String(
				(elem as Record<string, unknown>)["content"] ?? "",
			).trim();
			if (etext !== "") parts.push(etext);
		}
	}
	if (parts.length === 0) return "[Quoted message]: (image)";
	return `[Quoted message]:\n${parts.join("\n")}`;
}

function mergeQuote(text: string, quoteBlock: string): string {
	if (quoteBlock === "") return text;
	const emptyBody = text.trim() === "";
	return emptyBody ? quoteBlock : `${quoteBlock}\n\n${text}`;
}

function renderApprovalText(opts: {
	title: string;
	description?: string | undefined;
	commandPreview?: string | undefined;
	timeoutSec?: number | undefined;
}): string {
	const lines: string[] = ["🔐 **命令执行审批**", ""];
	if (opts.commandPreview !== undefined && opts.commandPreview !== "") {
		lines.push(`\`\`\`\n${opts.commandPreview.slice(0, 300)}\n\`\`\``);
	}
	if (opts.title !== "" && opts.title !== opts.commandPreview) {
		lines.push(`📋 ${opts.title}`);
	}
	if (opts.description !== undefined && opts.description !== "") {
		lines.push(`📝 ${opts.description}`);
	}
	lines.push("");
	lines.push(`⏱️ 超时: ${opts.timeoutSec ?? 300} 秒`);
	return lines.join("\n");
}

function buildTextEvent(o: {
	chatType: "dm" | "group";
	chatId: string;
	userId: string;
	text: string;
	messageId: string;
}): IncomingEvent {
	const hasText = o.text !== "";
	const kind = hasText ? ("text" as const) : ("other" as const);
	return {
		messageType: kind,
		text: o.text,
		source: {
			platform: "qqbot",
			chatType: o.chatType,
			userId: o.userId,
			chatId: o.chatId,
		},
		messageId: o.messageId,
		mediaUrls: [],
		metadata: {},
	};
}

// ── command registry for guard wiring (07 §1: derived, minimal here) ──────

const QQ_REGISTRY: CommandRegistry = [
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
