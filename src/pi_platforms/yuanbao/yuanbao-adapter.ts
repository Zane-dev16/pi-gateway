// pi_platforms/yuanbao/yuanbao-adapter — the Tencent Yuanbao bot adapter
// (WebSocket shape + binary ConnMsg protobuf wire), ported from Hermes
// gateway/platforms/yuanbao.py.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   yuanbao.py:ConnectionManager.open        — sign-token → WS → AUTH_BIND →
//                                              BIND_ACK(connectId) → loops
//   yuanbao.py:ConnectionManager._heartbeat_loop — ping each HEARTBEAT_INTERVAL;
//                                              2 missed pongs ⇒ reconnect
//   yuanbao.py:NO_RECONNECT_CLOSE_CODES {4012,4013,4014,4018,4019,4021}
//   yuanbao.py:AUTH_FAILED_CODES {4001,4002,4003} / AUTH_RETRYABLE {4010,4011,4099}
//   yuanbao.py:ConnectionManager._handle_frame — Response matching by msg_id;
//     Push ⇒ need_ack ⇒ PushAck; genuine pushes dispatch to AI
//   yuanbao.py:DecodeMiddleware.parse_json_push — JSON push parity (PascalCase)
//   yuanbao.py:HeartbeatManager — RUNNING reply-heartbeat every 2s while a
//     turn processes; auto-stop after 30s
//   yuanbao.py:SlowResponseNotifier — 120s notice "任务有点复杂…"
//   yuanbao.py:MessageSender.send_text_chunk — retry ladder 2^attempt (1s,2s,4s)
//
// Probe-computed exclusions (documented honestly, never faked green):
//   • COS media uploads (ImageUrl/FileUrl handlers) — media send paths surface
//     typed failures; inbound media refs resolve to attachment-info lines.
//   • Transcript-patching recall redaction (session-store rewrite) — recall
//     callbacks produce the synthetic INTERRUPT event (Branch C) only.

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
import {
	ActionHandlerRegistry,
	classifySendError,
	extractRetryAfterSeconds,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	type ClickAuthorizer,
} from "../kit/index.js";
import { BoundedSeenSet } from "../../pi_gateway/security/trust/replay-seen-set.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import { utf16Len } from "../kit/length-policy.js";
import {
	AUTH_FAILED_CLOSE_CODES,
	HEARTBEAT_INTERVAL_S,
	NO_RECONNECT_CLOSE_CODES,
	REPLY_HEARTBEAT_INTERVAL_S,
	REPLY_HEARTBEAT_TIMEOUT_S,
	SLOW_RESPONSE_TIMEOUT_S,
	SLOW_RESPONSE_MESSAGE,
	YUANBAO_PLUGIN_MANIFEST,
} from "./manifest.js";
import {
	CMD_TYPE,
	decodeConnMsg,
	decodeInboundPush,
	encodeAuthBind,
	encodePing,
	encodePushAck,
	encodeSendC2CMessage,
	encodeSendGroupHeartbeat,
	encodeSendGroupMessage,
	encodeSendPrivateHeartbeat,
	parseFields,
	WS_HEARTBEAT_FINISH,
	WS_HEARTBEAT_RUNNING,
	type DecodedPush,
	type MsgBodyElement,
} from "./proto.js";
import { SignManager } from "./sign-manager.js";
import type {
	FakeYuanbaoGateway,
	YbClientSocket,
	YbSocketListener,
} from "./fake-yuanbao.js";

export interface YuanbaoAdapterOptions {
	appKey?: string | undefined;
	appSecret?: string | undefined;
	botNickname?: string | undefined;
	dmPolicy?: string | undefined;
	groupPolicy?: string | undefined;
	scalarMaxUnits?: number | undefined;
	gateway: FakeYuanbaoGateway;
	signHttp: ConstructorParameters<typeof SignManager>[0];
	sleepMs?: ((ms: number) => Promise<void>) | undefined;
	nowMs?: (() => number) | undefined;
	/** Test knobs (defaults = manifest constants). */
	replyHeartbeatIntervalMs?: number | undefined;
	slowResponseTimeoutMs?: number | undefined;
	/** Scripted egress capture (fixture seam; production: absent). */
	egressCapture?:
		| ((
				chatId: string,
				content: string,
				metadata: Metadata,
		  ) => Promise<SendResult>)
		| undefined;
	/** Scripted §10.1 tier-1 rich probe (fixture seam; production: absent). */
	richProbe?: ((content: string) => Promise<SendResult>) | undefined;
	/** Whether a rich script was deliberately programmed (probe gating). */
	richHasScript?: (() => boolean) | undefined;
}

const AUTH_RETRYABLE_CODES = new Set([4010, 4011, 4099]);

/**
 * THE Yuanbao adapter. Binary protobuf WS wire over the kit base.
 */
export class YuanbaoAdapter extends BasePlatformAdapter {
	readonly pluginManifest = YUANBAO_PLUGIN_MANIFEST;

	readonly appKey: string;
	readonly appSecret: string;
	readonly botNickname: string;
	readonly dmPolicy: string;
	readonly groupPolicy: string;

	private readonly gateway: FakeYuanbaoGateway;
	private readonly signManager: SignManager;
	private readonly sleepFn: (ms: number) => Promise<void>;
	private readonly nowFn: () => number;
	private readonly hbIntervalMs: number;
	private readonly slowResponseTimeoutMs: number;
	private readonly egressCapture:
		| ((
				chatId: string,
				content: string,
				metadata: Metadata,
		  ) => Promise<SendResult>)
		| undefined;
	private readonly richProbe:
		| ((content: string) => Promise<SendResult>)
		| undefined;
	private readonly richHasScriptFn: (() => boolean) | undefined;
	private richProbeAttempts = 0;

	private socket: YbClientSocket | null = null;
	connectId: string | null = null;
	botId = "";
	running = false;
	isLive = false;
	reconnectAttempts = 0;
	consecutiveHbTimeouts = 0;

	/** Close-code audit (row observability). */
	readonly closeLog: Array<{ code: number; outcome: string }> = [];
	/** Server-authoritative Retry-After captures (family-row knob). */
	lastCapturedRetryAfterSeconds: number | null = null;
	private pendingRetryAfterS: number | null = null;
	readonly reconnectSteps: Array<{
		delayMs: number;
		authoritative: boolean;
		attempt: number;
	}> = [];
	readonly pushAcks: Array<{ msgId: string }> = [];
	readonly replyHeartbeats: Array<{ chatId: string; val: number }> = [];

	private readonly seenMessages: BoundedSeenSet;
	/** Per-chat outbound locks (MessageSender.get_chat_lock parity). */
	private readonly chatLocks = new Map<string, Promise<void>>();

	// ── subject-support plumbing (reference-fixture inheritance) ────────────
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly resolvedFamilies: string[] = [];
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

	constructor(opts: YuanbaoAdapterOptions) {
		super({
			manifestName: YUANBAO_PLUGIN_MANIFEST.name,
			capabilities: YUANBAO_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
		});
		this.appKey = (opts.appKey ?? "").trim();
		this.appSecret = (opts.appSecret ?? "").trim();
		this.botNickname = opts.botNickname ?? "Bot";
		this.dmPolicy = opts.dmPolicy ?? "pairing";
		this.groupPolicy = opts.groupPolicy ?? "pairing";
		this.gateway = opts.gateway;
		this.signManager = new SignManager(opts.signHttp);
		this.sleepFn =
			opts.sleepMs ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.hbIntervalMs =
			opts.replyHeartbeatIntervalMs ?? REPLY_HEARTBEAT_INTERVAL_S * 1000;
		this.slowResponseTimeoutMs =
			opts.slowResponseTimeoutMs ?? SLOW_RESPONSE_TIMEOUT_S * 1000;
		this.egressCapture = opts.egressCapture;
		this.richProbe = opts.richProbe;
		this.richHasScriptFn = opts.richHasScript;

		if (this.appKey === "" || this.appSecret === "") {
			this.lifecycle.disable({
				kind: "secret_missing",
				secretKey:
					this.appSecret === "" ? "YUANBAO_APP_SECRET" : "YUANBAO_APP_ID",
				manifestName: YUANBAO_PLUGIN_MANIFEST.name,
			});
		}
		this.registerLogSecret(this.appSecret);

		this.seenMessages = new BoundedSeenSet({
			maxEntries: 4096,
			ttlMs: 300_000,
			nowMs: this.nowFn,
		});

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // no native draft lanes
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

	get isConnected(): boolean {
		return this.isLive;
	}

	/** §6.3/A15 descriptor override point. */
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

	override chatLengthPolicyForChat(chatId: string): ChatLengthPolicy {
		const policy = super.chatLengthPolicyForChat(chatId);
		if (chatId.includes("utf16")) return { ...policy, lenFn: utf16Len };
		return policy;
	}

	// ── guard wiring ──────────────────────────────────────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		const spawnerOpts = spawner === undefined ? {} : { spawner };
		this.attachGuard(
			{
				registry: YB_REGISTRY,
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
					// Reply-heartbeat rides the TURN lifecycle (HeartbeatManager
					// parity): RUNNING ticks while the turn processes (incl. held
					// turns), FINISH on completion, auto-stop caps runaway loops,
					// and the slow-response notice fires past the timeout exactly
					// once. The turn runs BACKGROUND (the guard spawns fire-and-
					// forget per base.py:_start_session_processing), so the lifecycle
					// MUST live inside the handler — ingress dispatch never spans
					// the turn.
					const src = event.source;
					const chatId =
						src?.chatType === "group"
							? `group:${String(src.chatId ?? "")}`
							: `direct:${String(src?.chatId ?? "")}`;
					const stopHeartbeat = this.startReplyHeartbeat(chatId);
					const slowTimer = setTimeout(() => {
						void this.platformSendRaw(chatId, SLOW_RESPONSE_MESSAGE);
					}, this.slowResponseTimeoutMs);
					try {
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
					} finally {
						clearTimeout(slowTimer);
						stopHeartbeat();
					}
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

	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith("(Response formatting failed")
		) {
			return { success: false, error: "Bad Request: can't parse entities" };
		}
		// Scripted capture behaviors GOVERN the result; unscripted sends
		// RECORD and SUCCEED via the capture (shared-row subjects have no live
		// WS face by design — falling through to the binary plane would
		// fabricate "Not connected" failures the capture seam exists to avoid).
		if (this.egressCapture !== undefined) {
			return this.egressCapture(chatId, content, metadata);
		}
		return this.sendTextChunk(chatId, content);
	}

	get richWireAttempts(): number {
		return this.richProbeAttempts;
	}

	/** Rich lane ABSENT natively; scripted probes feed the §10.1 latch path. */
	protected override async wireRich(content: string): Promise<SendResult> {
		const scripted =
			this.richProbe !== undefined &&
			(this.richHasScriptFn === undefined || this.richHasScriptFn());
		if (!scripted) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		this.richProbeAttempts += 1;
		return this.richProbe(content);
	}

	// ── connection lifecycle (ConnectionManager.open parity) ────────────────

	async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		if (this.appKey === "" || this.appSecret === "") {
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail:
					"Yuanbao startup failed: YUANBAO_APP_ID and YUANBAO_APP_SECRET are required",
			});
			return false;
		}
		try {
			const tokenData = await this.signManager.get({
				appKey: this.appKey,
				appSecret: this.appSecret,
				apiDomain: "https://fake-yuanbao.invalid",
			});
			if (tokenData.bot_id !== "") this.botId = tokenData.bot_id;
			await this.openAndAuth(tokenData.token);
			this.running = true;
			this.isLive = true;
			this.reconnectAttempts = 0;
			void this.heartbeatLoop();
			return true;
		} catch (err) {
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail: `Yuanbao startup failed: ${err instanceof Error ? err.message : String(err)}`,
			});
			return false;
		}
	}

	async disconnect(): Promise<void> {
		this.running = false;
		this.isLive = false;
		this.socket?.close(1000);
		this.socket = null;
		this.connectId = null;
		this.signManager.clearLocks();
	}

	private openAndAuth(token: string): Promise<void> {
		return new Promise<void>((resolve, reject) => {
			const listener: YbSocketListener = {
				onOpen: () => {},
				onFrame: (frame) => void this.handleFrame(frame),
				onClose: (info) => void this.handleClose(info.code, info.reason),
				onError: (err) => void this.handleReadError(err),
			};
			this.socket = this.gateway.connect(listener);
			void this.waitForOpen()
				.then(() => {
					// Socket OPEN: drive AUTH_BIND now; BIND_ACK resolves.
					const msgId = `auth-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
					const bindFrame = encodeAuthBindRaw(msgId, token, this);
					this.pendingBind = { msgId, resolve, reject };
					this.socket?.sendFrame(bindFrame);
				})
				.catch((err: unknown) =>
					reject(err instanceof Error ? err : new Error(String(err))),
				);
		});
	}

	private async waitForOpen(): Promise<void> {
		for (let i = 0; i < 100 && this.socket?.readyState !== "open"; i++) {
			await new Promise<void>((r) => setTimeout(r, 2));
		}
		if (this.socket?.readyState !== "open") {
			throw new Error("socket never opened");
		}
	}

	private pendingBind: {
		msgId: string;
		resolve: () => void;
		reject: (err: Error) => void;
	} | null = null;

	// ── heartbeat loop (ConnectionManager._heartbeat_loop parity) ──────────

	protected async heartbeatLoop(): Promise<void> {
		while (this.running) {
			await this.sleepFn(HEARTBEAT_INTERVAL_S * 1000);
			if (!this.running || this.socket === null) continue;
			const pongReceived = await this.pingOnce();
			if (pongReceived) {
				this.consecutiveHbTimeouts = 0;
			} else {
				this.consecutiveHbTimeouts += 1;
				// Threshold 2: two missed pongs trigger a reconnect.
				if (this.consecutiveHbTimeouts >= 2) {
					this.consecutiveHbTimeouts = 0;
					await this.scheduleReconnect();
					return;
				}
			}
		}
	}

	private pingOnce(): Promise<boolean> {
		return new Promise<boolean>((resolve) => {
			const msgId = `ping-${Date.now()}-${Math.floor(Math.random() * 1e6)}`;
			this.pendingPongs.set(msgId, () => resolve(true));
			setTimeout(() => {
				if (this.pendingPongs.has(msgId)) {
					this.pendingPongs.delete(msgId);
					resolve(false);
				}
			}, 10_000);
			try {
				this.socket?.sendFrame(encodePing(msgId));
			} catch {
				this.pendingPongs.delete(msgId);
				resolve(false);
			}
		});
	}

	private readonly pendingPongs = new Map<string, () => void>();

	// ── frame handling (_handle_frame parity) ───────────────────────────────

	private async handleFrame(frame: Uint8Array): Promise<void> {
		let decoded;
		try {
			decoded = decodeConnMsg(frame);
		} catch {
			return;
		}
		const head = decoded.head;

		if (head.cmd_type === CMD_TYPE.Response && head.cmd === "auth-bind") {
			// BIND_ACK: extract connectId from AuthBindRsp field 3.
			const connectId = extractConnectId(decoded.data);
			if (connectId !== null && this.pendingBind !== null) {
				this.connectId = connectId;
				const pending = this.pendingBind;
				this.pendingBind = null;
				pending.resolve();
			} else if (this.pendingBind !== null) {
				const pending = this.pendingBind;
				this.pendingBind = null;
				pending.reject(new Error("BIND_ACK missing connectId"));
			}
			return;
		}
		if (head.cmd_type === CMD_TYPE.Response && head.cmd === "ping") {
			const cb = this.pendingPongs.get(head.msg_id);
			if (cb !== undefined) {
				this.pendingPongs.delete(head.msg_id);
				cb();
			}
			return;
		}
		if (head.cmd_type === CMD_TYPE.Response) {
			// Heartbeat ACKs and biz responses: matched-and-discarded here.
			return;
		}
		if (head.cmd_type === CMD_TYPE.Push) {
			if (head.need_ack) {
				try {
					this.socket?.sendFrame(encodePushAck(head));
					this.pushAcks.push({ msgId: head.msg_id });
				} catch {
					/* ack best-effort */
				}
			}
			// JSON-parity lane first: some pushes ship RAW-JSON payloads
			// (parse_json_push parity) before the binary proto decode.
			const push = this.decodeFramePayload(decoded.data);
			if (push !== null) await this.dispatchPush(push);
		}
	}

	// ── close-code classes ───────────────────────────────────────────────────

	async handleClose(code: number, reason: string): Promise<void> {
		if (code === 1000) return; // deliberate disconnect()
		this.isLive = false;

		// Close-REASON Retry-After carrier: "retry-after:N" captures N seconds
		// verbatim as the next reconnect delay (authoritative).
		const m = /retry-after:(\d+)/.exec(reason);
		if (m !== null) {
			this.lastCapturedRetryAfterSeconds = Number(m[1]);
			this.pendingRetryAfterS = Number(m[1]);
		}

		if (NO_RECONNECT_CLOSE_CODES.has(code)) {
			// Fatal ⇒ NOTHING schedules behind it: stop the heartbeat loop and
			// bar scheduleReconnect (ConnectionManager fatal parity).
			this.running = false;
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail: `Close code ${code} is non-recoverable, NOT reconnecting`,
			});
			this.closeLog.push({ code, outcome: "fatal" });
			return;
		}
		if (AUTH_FAILED_CLOSE_CODES.has(code)) {
			this.closeLog.push({ code, outcome: "re-sign" });
			// Re-sign the token and retry once.
			try {
				await this.signManager.forceRefresh({
					appKey: this.appKey,
					appSecret: this.appSecret,
					apiDomain: "https://fake-yuanbao.invalid",
				});
				await this.connect({ isReconnect: true });
				this.closeLog.push({ code, outcome: "reconnected-after-resign" });
			} catch {
				this.closeLog.push({ code, outcome: "resign-failed-fatal" });
				// Fatal ⇒ nothing schedules behind it.
				this.running = false;
				this.lifecycle.markFatal({
					kind: "config_invalid",
					detail: `Re-sign after close ${code} failed`,
				});
			}
			return;
		}
		if (AUTH_RETRYABLE_CODES.has(code)) {
			this.closeLog.push({ code, outcome: "retry-same-token" });
		} else {
			this.closeLog.push({ code, outcome: "ladder-reconnect" });
		}
		await this.scheduleReconnect();
	}

	async handleReadError(err: Error): Promise<void> {
		this.isLive = false;
		this.closeLog.push({
			code: -1,
			outcome: `read-error:${err.message.slice(0, 30)}`,
		});
		if (this.running) await this.scheduleReconnect();
	}

	reconnectLog: string[] = [];

	private async scheduleReconnect(): Promise<void> {
		if (!this.running) return;
		this.reconnectAttempts += 1;
		this.reconnectLog.push(`reconnect#${this.reconnectAttempts}`);
		const captured = this.pendingRetryAfterS;
		let delayS: number;
		let authoritative: boolean;
		if (captured === null) {
			delayS = 2 ** Math.min(this.reconnectAttempts - 1, 4);
			authoritative = false;
		} else {
			delayS = captured; // server-authoritative verbatim
			this.pendingRetryAfterS = null;
			authoritative = true;
		}
		this.reconnectSteps.push({
			delayMs: delayS * 1000,
			authoritative,
			attempt: this.reconnectAttempts,
		});
		await this.sleepFn(delayS * 1000);
		// Exponential ladder capped per MAX_RECONNECT_ATTEMPTS upstream.
		if (this.reconnectAttempts > 100) {
			// Fatal ⇒ nothing schedules behind it.
			this.running = false;
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail: "Max reconnect attempts reached",
			});
			return;
		}
		try {
			const tokenData = await this.signManager.get({
				appKey: this.appKey,
				appSecret: this.appSecret,
				apiDomain: "https://fake-yuanbao.invalid",
			});
			await this.openAndAuth(tokenData.token);
			this.isLive = true;
			this.reconnectAttempts = 0;
			this.reconnectLog.push("reconnected");
			void this.heartbeatLoop();
		} catch {
			this.isLive = false;
			await this.scheduleReconnect();
		}
	}

	// ── inbound decode (JSON push parity + binary proto) ────────────────────

	/** parse_json_push parity: snake_case AND Tencent PascalCase accepted. */
	static parseJsonPush(raw: Record<string, unknown>): DecodedPush | null {
		const rawRec = raw ?? {};
		const fromAccount = String(
			rawRec["from_account"] ?? rawRec["From_Account"] ?? "",
		);
		const groupCode = String(
			rawRec["group_code"] ?? rawRec["GroupId"] ?? rawRec["group_id"] ?? "",
		);
		const bodyRaw = (rawRec["msg_body"] ?? rawRec["MsgBody"] ?? []) as Array<
			Record<string, unknown>
		>;
		const msgBody = bodyRaw.map((item) => ({
			msg_type: String(item["msg_type"] ?? item["MsgType"] ?? ""),
			msg_content: normalizeContent(item["msg_content"] ?? item["MsgContent"]),
		}));
		if (fromAccount === "" && msgBody.length === 0) {
			const cmd = String(rawRec["callback_command"] ?? "");
			if (cmd === "") return null;
		}
		return {
			callback_command: String(rawRec["callback_command"] ?? ""),
			from_account: fromAccount,
			to_account: String(rawRec["to_account"] ?? rawRec["To_Account"] ?? ""),
			sender_nickname: String(
				rawRec["sender_nickname"] ?? rawRec["nick_name"] ?? "",
			),
			group_id: "",
			group_code: groupCode,
			group_name: String(rawRec["group_name"] ?? ""),
			msg_seq: Number(rawRec["msg_seq"] ?? rawRec["MsgSeq"] ?? 0),
			msg_key: "",
			msg_id: String(
				rawRec["msg_id"] ?? rawRec["msg_key"] ?? rawRec["MsgKey"] ?? "",
			),
			msg_body: msgBody.map((el) => ({
				msg_type: el.msg_type,
				msg_content: el.msg_content,
			})),
			cloud_custom_data: String(
				rawRec["cloud_custom_data"] ?? rawRec["CloudCustomData"] ?? "",
			),
			bot_owner_id: String(
				rawRec["bot_owner_id"] ?? rawRec["botOwnerId"] ?? "",
			),
			trace_id: "",
			recall_msg_seq_list: decodeRecallList(
				rawRec["recall_msg_seq_list"] ?? rawRec["RecallMsgSeqList"],
			),
		};
	}

	decodeFramePayload(data: Uint8Array): DecodedPush | null {
		try {
			const asJson = JSON.parse(Buffer.from(data).toString("utf8")) as unknown;
			if (asJson !== null && typeof asJson === "object") {
				return YuanbaoAdapter.parseJsonPush(asJson as Record<string, unknown>);
			}
		} catch {
			/* binary path */
		}
		return decodeInboundPush(data);
	}

	// ── push dispatch (middleware chain semantic core) ──────────────────────

	private async dispatchPush(push: DecodedPush): Promise<void> {
		// Recall guard first (Group.CallbackAfterRecallMsg / C2C withdraw).
		if (
			push.callback_command === "Group.CallbackAfterRecallMsg" ||
			push.callback_command === "C2C.CallbackAfterMsgWithDraw"
		) {
			this.handleRecall(push);
			return;
		}

		const senderId = push.from_account;
		if (senderId === "" || senderId === this.botId) return; // SkipSelf

		const messageId = push.msg_id !== "" ? push.msg_id : `seq:${push.msg_seq}`;
		if (!this.seenMessages.add(messageId)) return; // Dedup

		const isGroup = push.group_code !== "";
		if (isGroup) {
			if (!this.isGroupAllowed(push.group_code)) return;
			// GroupAtGuard: group text must @-mention the bot.
			const text = concatPushText(push);
			if (!text.includes(`@${this.botNickname}`)) return;
		} else if (!this.isDmIntakeAllowed(senderId)) {
			return;
		}

		const text = stripAtMention(concatPushText(push));
		if (text.trim() === "") return;

		const chatId = isGroup ? `group:${push.group_code}` : `direct:${senderId}`;
		const event: IncomingEvent = {
			messageType: "text",
			text,
			source: {
				platform: "yuanbao",
				chatType: isGroup ? "group" : "dm",
				userId: senderId,
				chatId: isGroup ? push.group_code : senderId,
			},
			metadata: {},
		};
		// The turn lifecycle (reply heartbeat + slow-response notice) is owned
		// INSIDE the guard messageHandler — see attachStandardGuard — because
		// the turn runs background and ingress dispatch never spans it.
		await this.deliverInbound(event, `yuanbao:${chatId}:${senderId}`);
	}

	private handleRecall(push: DecodedPush): void {
		// Branch C (processing-session interrupt): synthesize the CRITICAL
		// recall event through the normal pipeline as an internal wake.
		const recalled = (push.recall_msg_seq_list ?? [])[0]?.msg_id ?? "";
		const source = {
			platform: "yuanbao",
			chatType: push.group_code !== "" ? ("group" as const) : ("dm" as const),
			userId: push.from_account,
			chatId: push.group_code !== "" ? push.group_code : push.from_account,
		};
		const synth: IncomingEvent = {
			messageType: "text",
			text: `[CRITICAL — MESSAGE RECALLED] The triggering message (${recalled}) was recalled.`,
			source,
			internal: true,
			metadata: {},
		};
		void this.deliverInbound(
			synth,
			`yuanbao:${push.group_code !== "" ? `group:${push.group_code}` : `direct:${push.from_account}`}:${push.from_account}`,
		);
	}

	/** Reply-heartbeat lifecycle for ONE processing turn. */
	startReplyHeartbeat(chatId: string): () => void {
		let running = true;
		let elapsed = 0;
		const tick = (): void => {
			if (!running) return;
			this.replyHeartbeats.push({ chatId, val: WS_HEARTBEAT_RUNNING });
			this.sendHeartbeat(chatId, WS_HEARTBEAT_RUNNING);
			elapsed += this.hbIntervalMs;
			// Auto-stop at the timeout cap (elapsed is ms; the manifest
			// constant is seconds).
			if (elapsed >= REPLY_HEARTBEAT_TIMEOUT_S * 1000) {
				running = false;
				return; // auto-stop after 30s
			}
			setTimeout(tick, this.hbIntervalMs);
		};
		tick();
		return () => {
			running = false;
			this.replyHeartbeats.push({ chatId, val: WS_HEARTBEAT_FINISH });
			this.sendHeartbeat(chatId, WS_HEARTBEAT_FINISH);
		};
	}

	private sendHeartbeat(chatId: string, val: number): void {
		try {
			const frame = chatId.startsWith("group:")
				? encodeSendGroupHeartbeat(
						this.botId,
						chatId.slice("group:".length),
						val,
					)
				: encodeSendPrivateHeartbeat(
						this.botId,
						chatId.slice("direct:".length),
						val,
					);
			this.socket?.sendFrame(frame);
		} catch {
			/* heartbeat best-effort */
		}
	}

	// ── ACL intake gates ────────────────────────────────────────────────────

	isDmIntakeAllowed(senderId: string): boolean {
		const principal = senderId.trim();
		if (principal === "") return false;
		if (this.dmPolicy === "disabled") return false;
		if (this.dmPolicy === "allowlist") return this.allowListHas(principal);
		if (this.dmPolicy === "pairing") return true;
		return false;
	}

	isGroupAllowed(groupCode: string): boolean {
		if (this.groupPolicy === "disabled") return false;
		if (this.groupPolicy === "open") return true;
		if (this.groupPolicy === "pairing") return false;
		if (this.groupPolicy === "allowlist") return this.groupAllowHas(groupCode);
		return false;
	}

	groupAllowFrom: readonly string[] = [];
	allowFrom: readonly string[] = [];

	private allowListHas(target: string): boolean {
		for (const entry of this.allowFrom) {
			if (
				entry.trim() === "*" ||
				entry.trim().toLowerCase() === target.toLowerCase()
			)
				return true;
		}
		return false;
	}

	private groupAllowHas(target: string): boolean {
		for (const entry of this.groupAllowFrom) {
			if (
				entry.trim() === "*" ||
				entry.trim().toLowerCase() === target.toLowerCase()
			)
				return true;
		}
		return false;
	}

	// ── sender (MessageSender.send_text_chunk parity) ───────────────────────

	private async withChatLock<T>(
		chatId: string,
		fn: () => Promise<T>,
	): Promise<T> {
		const prior = this.chatLocks.get(chatId) ?? Promise.resolve();
		const gate = prior.catch(() => undefined);
		const run = gate.then(fn);
		this.chatLocks.set(
			chatId,
			run.then(
				() => undefined,
				() => undefined,
			),
		);
		return run;
	}

	async sendTextChunk(chatId: string, text: string): Promise<SendResult> {
		return this.withChatLock(chatId, () =>
			this.sendTextChunkLocked(chatId, text),
		);
	}

	private async sendTextChunkLocked(
		chatId: string,
		text: string,
	): Promise<SendResult> {
		if (!this.isLive || this.socket === null) {
			return { success: false, error: "Not connected", retryable: true };
		}
		let lastError = "Unknown error";
		let honoredRetryAfterOnce = false;
		for (let attempt = 0; attempt < 3; attempt++) {
			try {
				const ok = this.rawSendFrame(chatId, text);
				if (ok) return { success: true, messageId: `yb-${this.nowFn()}` };
				lastError = "send failed";
			} catch (err) {
				lastError = err instanceof Error ? err.message : String(err);
			}
			// Server-authoritative retry_after honored ONCE and captured (kit
			// _send_with_retry parity); timeout-AMBIGUOUS failures never re-drive.
			if (classifySendError(new Error(lastError)) === "timeout") {
				return { success: false, error: lastError };
			}
			const ra = !honoredRetryAfterOnce
				? extractRetryAfterSeconds(new Error(lastError))
				: null;
			if (ra !== null && ra >= 0) {
				honoredRetryAfterOnce = true;
				this.lastCapturedRetryAfterSeconds = ra;
				await this.sleepFn(ra * 1000);
			} else if (attempt < 2) {
				await this.sleepFn(2 ** attempt * 1000);
			}
		}
		return {
			success: false,
			error: `Max retries exceeded: ${lastError}`,
			retryable: true,
		};
	}

	private rawSendFrame(chatId: string, text: string): boolean {
		if (this.socket === null) throw new Error("Not connected");
		const body: MsgBodyElement[] = [
			{ msg_type: "TIMTextElem", msg_content: { text } },
		];
		const frame = chatId.startsWith("group:")
			? encodeSendGroupMessage({
					groupCode: chatId.slice("group:".length),
					fromAccount: this.botId,
					toAccount: "",
					msgBody: body,
				})
			: encodeSendC2CMessage({
					toAccount: chatId.slice("direct:".length),
					fromAccount: this.botId,
					msgBody: body,
				});
		this.socket.sendFrame(frame);
		this.serverSends.push({ chatId, text });
		return true;
	}

	/** Outbound sends observed at the WIRE level (row observability). */
	readonly serverSends: Array<{ chatId: string; text: string }> = [];

	/** Direct platform-level send used by the slow-response notifier. */
	private async platformSendRaw(
		chatId: string,
		text: string,
	): Promise<SendResult> {
		return this.sendTextChunkLocked(chatId, text);
	}
}

function concatPushText(push: DecodedPush): string {
	return push.msg_body
		.map((el) => String(el.msg_content["text"] ?? ""))
		.join("");
}

function stripAtMention(text: string): string {
	return text.replace(/^@\S+\s*/, "");
}

function extractConnectId(data: Uint8Array): string | null {
	try {
		const first = parseFields(data).find((f) => f.fieldNumber === 3);
		if (first === undefined || !(first.value instanceof Uint8Array))
			return null;
		return Buffer.from(first.value).toString("utf8") || null;
	} catch {
		return null;
	}
}

function encodeAuthBindRaw(
	msgId: string,
	token: string,
	adapter: YuanbaoAdapter,
): Uint8Array {
	return encodeAuthBind({
		bizId: "ybBot",
		uid: adapter.botId,
		source: "bot",
		token,
		msgId,
		appVersion: "1.0.0",
		operationSystem: process.platform,
		botVersion: "1.0.0",
	});
}

/** JSON-push recall list parity: snake_case AND Tencent PascalCase. */
function decodeRecallList(
	raw: unknown,
): Array<{ msg_seq: number; msg_id: string }> | null {
	if (!Array.isArray(raw)) return null;
	const list = raw
		.map((entry) => {
			const rec = (entry ?? {}) as Record<string, unknown>;
			return {
				msg_seq: Number(rec["msg_seq"] ?? rec["MsgSeq"] ?? 0),
				msg_id: String(rec["msg_id"] ?? rec["MsgId"] ?? ""),
			};
		})
		.filter((e) => e.msg_id !== "");
	return list.length > 0 ? list : null;
}

function normalizeContent(raw: unknown): Record<string, unknown> {
	if (typeof raw === "string") {
		try {
			const parsed = JSON.parse(raw) as unknown;
			if (parsed !== null && typeof parsed === "object") {
				return parsed as Record<string, unknown>;
			}
		} catch {
			return { text: raw };
		}
	}
	if (raw !== null && typeof raw === "object")
		return raw as Record<string, unknown>;
	return {};
}

// ── command registry ────────────────────────────────────────────────────────

const YB_REGISTRY: CommandRegistry = [
	{
		name: "new",
		aliases: ["reset"],
		busyPolicy: "interrupt_then_dispatch" as const,
		busyHandler: "new",
	},
	{ name: "approve", busyPolicy: "dispatch" as const },
	{ name: "status", busyPolicy: "dispatch" as const },
];
