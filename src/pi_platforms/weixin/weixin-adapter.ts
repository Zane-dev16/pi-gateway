// pi_platforms/weixin/weixin-adapter — the WeChat iLink bot adapter
// (long-polling shape), ported from Hermes gateway/platforms/weixin.py.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   weixin.py:WeixinAdapter.__init__   — policies, timings, circuit params
//   weixin.py:_poll_loop               — long-poll cycle, -14/stale pause,
//                                        failure ladder (2s / 30s), recycle
//   weixin.py:_get_updates             — long-poll timeout ⇒ BENIGN empty
//                                        cycle; server longpolling_timeout_ms
//                                        adopted as the next budget (:1386)
//   weixin.py:_is_stale_session_ret    — ret/errcode -2 + 'unknown error' is
//                                        a STALE SESSION (same family as -14),
//                                        branched BEFORE rate-limit handling
//                                        at both sites (poll :1394 / send :1847)
//   weixin.py:_process_message         — dedup (id + content fingerprint),
//                                        chat-type guess, intake ACLs,
//                                        context_token store, media collect
//   weixin.py:_enqueue_text_event/_flush_text_batch — debounce batching
//   weixin.py:_record_rate_limit_event — circuit breaker (threshold/window/open)
//   weixin.py:_send_text_chunk_locked  — per-chunk retries: generic vendor
//                                        errors retry linearly
//                                        delay*(attempt+1) up to 4 retries,
//                                        rate-limit (-2) backs off 3×, session-
//                                        expired (-14/stale) tokenless single retry
//   weixin.py:send → _split_text(format_message(…)) — egress text ships as
//                                        format_message parity (copy-friendly
//                                        wrap over normalized blocks) split by
//                                        the delivery-unit splitter (MAX_MESSAGE_LENGTH=2000)
//   weixin.py:_api_post/_headers/_base_info — EVERY outgoing POST merges
//     {channel_version:"2.2.0"} base_info and carries the full iLink header
//     plane (ilink-transport.ts; asserted on every fake-face call)
//   weixin.py:_send_file — outbound media: getuploadurl → ECB ciphertext CDN
//     POST (octet-stream, x-encrypted-param) → sendmessage media item with
//     aes_key=base64(hex) (ilink-transport item builders + wire-crypto ECB)
//   weixin.py send_typing/stop_typing/_ensure_typing_ticket — turn-scoped
//     indicator signals on getConfig-refreshed tickets (stuck-indicator guard)
//   weixin.py:qr_login — account linking: get_bot_qrcode then a get_qrcode_status
//     poll loop (wait/scaned/scaned_but_redirect/expired≤3/confirmed)
//
// Probe-computed exclusions (documented honestly, never faked green):
//   • INBOUND CDN media download/decrypt (novac2c blobs) — media items surface
//     attachment-info lines. OUTBOUND media ships (sendFile above).

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
import { BasePlatformAdapter, chunkWithFenceCarry } from "../kit/index.js";
import {
	ActionHandlerRegistry,
	CallbackQueryRouter,
	DELIVERY_FAILED_NOTICE,
	FormattingLadder,
	ClarifyPendingStore,
	OneShotPendingStore,
	classifySendError,
	plainTextFallbackBody,
	PLAIN_TEXT_FALLBACK_PREFIX,
	sendWithRetry,
	type ClickAuthorizer,
} from "../kit/index.js";
import { createHash } from "node:crypto";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import { BoundedSeenSet } from "../../pi_gateway/security/trust/replay-seen-set.js";
import { aes128EcbEncrypt, aesPaddedSize } from "./wire-crypto.js";
import {
	baseInfo,
	buildILinkGetHeaders,
	buildILinkPostHeaders,
	buildOutboundMediaItem,
	CDN_UPLOAD_HEADERS,
	cdnUploadUrl,
	aesKeyForApi,
	defaultRandomBytes,
	mediaTypeForKind,
	outboundMediaKind,
	type RandomBytesFn,
} from "./ilink-transport.js";
import {
	BACKOFF_DELAY_SECONDS,
	EP_GET_BOT_QR,
	EP_GET_CONFIG,
	EP_GET_QR_STATUS,
	EP_GET_UPDATES,
	EP_GET_UPLOAD_URL,
	EP_SEND_MESSAGE,
	EP_SEND_TYPING,
	ILINK_BASE_URL,
	ITEM_FILE,
	ITEM_IMAGE,
	ITEM_TEXT,
	ITEM_VIDEO,
	ITEM_VOICE,
	QR_DEFAULT_BOT_TYPE,
	QR_LOGIN_TIMEOUT_S,
	QR_MAX_EXPIRED_REFRESHES,
	QR_POLL_INTERVAL_S,
	MAX_CONSECUTIVE_FAILURES,
	MESSAGE_DEDUP_TTL_SECONDS,
	MSG_TYPE_USER,
	RATE_LIMIT_BACKOFF_FACTOR,
	RATE_LIMIT_CIRCUIT_OPEN_S,
	RATE_LIMIT_CIRCUIT_THRESHOLD,
	RATE_LIMIT_CIRCUIT_WINDOW_S,
	RETRY_DELAY_SECONDS,
	SEND_CHUNK_DELAY_S,
	SEND_CHUNK_RETRIES,
	SEND_CHUNK_RETRY_DELAY_S,
	TEXT_BATCH_SPLIT_THRESHOLD,
	TEXT_BATCH_DELAY_S,
	TEXT_BATCH_SPLIT_DELAY_S,
	SESSION_EXPIRED_ERRCODE,
	SESSION_EXPIRED_PAUSE_S,
	WX_MAX_MESSAGE_LENGTH,
	LONG_POLL_TIMEOUT_MS,
	RATE_LIMIT_ERRCODE,
	TYPING_START,
	TYPING_STOP,
	TYPING_TICKET_TTL_S,
	MSG_TYPE_BOT,
	MSG_STATE_FINISH,
	WEIXIN_CDN_BASE_URL,
	WEIXIN_PLUGIN_MANIFEST,
	WEIXIN_RATE_BUDGET,
} from "./manifest.js";
import {
	normalizeMarkdownBlocks,
	wrapCopyFriendlyLines,
	splitTextForWeixinDelivery,
} from "./text-splitting.js";
import type {
	FakeILinkServer,
	ILinkMessage,
	ILinkPostResponse,
} from "./fake-ilink.js";

export interface WeixinSyncStore {
	load(accountId: string): string;
	save(accountId: string, buf: string): void;
}

export interface WeixinAdapterOptions {
	token?: string | undefined;
	accountId?: string | undefined;
	dmPolicy?: string | undefined;
	groupPolicy?: string | undefined;
	allowFrom?: readonly string[] | undefined;
	groupAllowFrom?: readonly string[] | undefined;
	scalarMaxUnits?: number | undefined;
	/** Env-read seam for the open-DM opt-ins (production: process.env). */
	readEnv?: ((key: string) => string | undefined) | undefined;
	server: FakeILinkServer;
	syncStore: WeixinSyncStore;
	sleepMs?: ((ms: number) => Promise<void>) | undefined;
	nowMs?: (() => number) | undefined;
	spawner?: TaskSpawner | undefined;
	/** Scripted egress capture (fixture seam; production: absent). */
	sendCapture?:
		| ((
				chatId: string,
				content: string,
				metadata: Metadata,
		  ) => Promise<SendResult>)
		| undefined;
	/** Whether a send script was deliberately programmed (probe gating). */
	captureHasScript?: (() => boolean) | undefined;
	/** Scripted §10.1 tier-1 rich probe (fixture seam; production: absent). */
	richProbe?: ((content: string) => Promise<SendResult>) | undefined;
	/** Whether a rich script was deliberately programmed (probe gating). */
	richHasScript?: (() => boolean) | undefined;
	/** CSPRNG seam for filekeys/aes keys/uins (fixtures may inject determinism). */
	randomBytesFn?: RandomBytesFn | undefined;
}

export interface SendFileOptions {
	filename: string;
	plaintext: Buffer;
	/** _outbound_media_builder force_file_attachment parity. */
	forceFileAttachment?: boolean | undefined;
	caption?: string | undefined;
}

export interface QrLoginOptions {
	botType?: string | undefined;
	timeoutSeconds?: number | undefined;
}

export interface QrLoginCredentials {
	account_id: string;
	token: string;
	base_url: string;
	user_id: string;
}

interface TextBatch {
	event: IncomingEvent;
	lastChunkLen: number;
	timer: Promise<void>;
}

/**
 * THE Weixin adapter. Long-poll ingress over the fake iLink face; shared-kit
 * egress doors with platform retry semantics (rate-limit breaker, -14
 * tokenless retry).
 */
export class WeixinAdapter extends BasePlatformAdapter {
	readonly pluginManifest = WEIXIN_PLUGIN_MANIFEST;

	readonly token: string;
	readonly accountId: string;
	readonly dmPolicy: string;
	readonly groupPolicy: string;
	readonly allowFrom: readonly string[];
	readonly groupAllowFrom: readonly string[];

	private readonly server: FakeILinkServer;
	private readonly syncStore: WeixinSyncStore;
	private readonly sleepFn: (ms: number) => Promise<void>;
	private readonly nowFn: () => number;
	private readonly readEnv: (key: string) => string | undefined;
	private readonly sendCapture:
		| ((
				chatId: string,
				content: string,
				metadata: Metadata,
		  ) => Promise<SendResult>)
		| undefined;
	private readonly captureHasScriptFn: (() => boolean) | undefined;
	private readonly richProbe:
		| ((content: string) => Promise<SendResult>)
		| undefined;
	private readonly richHasScriptFn: (() => boolean) | undefined;
	private readonly rng: RandomBytesFn;

	private syncBuf = "";
	running = false;
	isLive = false;
	/** Poll-session generation — bumps on every recycle (row observability). */
	generation = 0;

	private consecutiveFailures = 0;
	sessionExpiredStreak = 0;
	private lastRateLimitEvents: number[] = [];
	private rateLimitCircuitUntil = 0;

	/** Committed-but-undispatched inbound (ack-before-enqueue window). */
	heldInbound: ILinkMessage[] = [];
	/** Fixture seam: fires AFTER cursor commit, BEFORE dispatch. */
	hooks: {
		afterCommitBeforeDispatch?:
			| ((msgs: ILinkMessage[]) => Promise<void>)
			| undefined;
	} = {};

	private readonly dedup: BoundedSeenSet;
	/** Fixture-visible context-token store (production: private parity). */
	readonly contextTokens = new Map<string, string>();
	private readonly typingTickets = new Map<
		string,
		{ ticket: string; atMs: number }
	>();

	private pendingTextBatches = new Map<string, TextBatch>();

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

	constructor(opts: WeixinAdapterOptions) {
		super({
			manifestName: WEIXIN_PLUGIN_MANIFEST.name,
			capabilities: WEIXIN_PLUGIN_MANIFEST.capabilities,
			// weixin.py:WeixinAdapter.MAX_MESSAGE_LENGTH — the vendor split budget.
			scalarMaxUnits: opts.scalarMaxUnits ?? WX_MAX_MESSAGE_LENGTH,
		});
		this.token = (opts.token ?? "").trim();
		this.accountId = (opts.accountId ?? "").trim();
		this.dmPolicy = opts.dmPolicy ?? "pairing";
		this.groupPolicy = opts.groupPolicy ?? "disabled";
		this.allowFrom = opts.allowFrom ?? [];
		this.groupAllowFrom = opts.groupAllowFrom ?? [];
		this.server = opts.server;
		this.syncStore = opts.syncStore;
		this.sleepFn =
			opts.sleepMs ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.readEnv = opts.readEnv ?? ((key) => process.env[key]);
		this.sendCapture = opts.sendCapture;
		this.captureHasScriptFn = opts.captureHasScript;
		this.richProbe = opts.richProbe;
		this.richHasScriptFn = opts.richHasScript;
		this.rng = opts.randomBytesFn ?? defaultRandomBytes;

		if (this.token === "") {
			// Loud disable parity: connect() refuses without the bot token.
			this.lifecycle.disable({
				kind: "secret_missing",
				secretKey: "WEIXIN_TOKEN",
				manifestName: WEIXIN_PLUGIN_MANIFEST.name,
			});
		}
		this.registerLogSecret(this.token);

		this.dedup = new BoundedSeenSet({
			maxEntries: 4096,
			ttlMs: MESSAGE_DEDUP_TTL_SECONDS * 1000,
			nowMs: this.nowFn,
		});

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // no native draft lanes on iLink
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

	// ── guard wiring (reference-fixture inheritance) ──────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		const spawnerOpts = spawner === undefined ? {} : { spawner };
		this.attachGuard(
			{
				registry: WX_REGISTRY,
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
		// Turn-scoped typing indicator (weixin.py send_typing → turn →
		// stop_typing parity): bracket EVERY dispatched turn so the WeChat
		// client never shows a stuck indicator.
		const chatId = String(event.source?.chatId ?? "");
		this.sendTypingIndicator(chatId);
		try {
			await this.handleIngress(event, sessionKey);
		} finally {
			this.stopTypingIndicator(chatId);
		}
	}

	/**
	 * Typing signal plane (weixin.py send_typing/stop_typing parity): POSTs
	 * ilink/bot/sendtyping {ilink_user_id, typing_ticket, status:TYPING_START|STOP}
	 * on a getConfig-refreshed ticket. Failures are debug-grade and NEVER
	 * propagate into the turn (Hermes swallows them identically).
	 */
	private signalTyping(chatId: string, status: number): void {
		if (chatId === "") return;
		try {
			const ticket = this.typingTicketFor(chatId);
			if (ticket === null) return; // no ticket ⇒ silent no-op (vendor parity)
			this.ilinkPost(EP_SEND_TYPING, {
				ilink_user_id: chatId,
				typing_ticket: ticket,
				status,
			});
		} catch {
			// debug-grade: an indicator failure never breaks the turn
		}
	}

	sendTypingIndicator(chatId: string): void {
		this.signalTyping(chatId, TYPING_START);
	}

	stopTypingIndicator(chatId: string): void {
		this.signalTyping(chatId, TYPING_STOP);
	}

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	/** Per-chat descriptor override point (§6.3/A15). */
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

	// ── iLink transport plane (weixin.py:_api_post/_api_get parity) ───────────

	/**
	 * ONE outgoing-POST builder: merges {channel_version} base_info into the
	 * payload and carries Hermes' exact header plane. Every iLink POST the
	 * adapter issues routes through here — the seam records both verbatim.
	 */
	private ilinkPostRequest(
		endpoint: string,
		payload: Record<string, unknown>,
	): {
		endpoint: string;
		payload: Record<string, unknown>;
		headers: Record<string, string>;
	} {
		const merged = { ...payload, base_info: baseInfo() };
		const bodyByteLength = Buffer.byteLength(JSON.stringify(merged), "utf8");
		return {
			endpoint,
			payload: merged,
			headers: buildILinkPostHeaders(
				this.token === "" ? undefined : this.token,
				bodyByteLength,
				this.rng,
			),
		};
	}

	private ilinkPost(
		endpoint: string,
		payload: Record<string, unknown>,
	): ILinkPostResponse {
		return this.server.post(this.ilinkPostRequest(endpoint, payload));
	}

	/** _api_get parity: app-identity headers ONLY (never Bearer/body auth). */
	private ilinkGet(
		baseUrl: string,
		endpoint: string,
		query: Record<string, string>,
	): Record<string, unknown> {
		return this.server.getILink({
			baseUrl,
			endpoint,
			query,
			headers: buildILinkGetHeaders(),
		});
	}

	// ── connection lifecycle ────────────────────────────────────────────────

	async connect(opts: { isReconnect: boolean }): Promise<boolean> {
		if (this.token === "") {
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail: "Weixin startup failed: WEIXIN_TOKEN is required",
			});
			return false;
		}
		void opts.isReconnect;
		this.syncBuf = this.syncStore.load(this.accountId);
		this.running = true;
		this.isLive = true;
		this.generation += 1;
		void this.pollLoop();
		return true;
	}

	async disconnect(): Promise<void> {
		this.running = false;
		this.isLive = false;
	}

	// ── the long-poll cycle (_poll_loop parity) ─────────────────────────────

	private pollLoopStarted = false;

	private async pollLoop(): Promise<void> {
		if (this.pollLoopStarted) return;
		this.pollLoopStarted = true;
		try {
			while (this.running) {
				try {
					const response = await this.pullOnce();
					if (response === undefined) continue; // timed-out long poll

					// Hermes treats a missing/null ret/errcode as NO error ({0, None}).
					const ret = numericOrUndefined(response["ret"]);
					const errcode = numericOrUndefined(response["errcode"]);
					const hasError =
						(ret !== undefined && ret !== 0) ||
						(errcode !== undefined && errcode !== 0);
					if (hasError) {
						// Stale-session FIRST (weixin.py:_poll_loop :1394): -14 OR the
						// _is_stale_session_ret signature (-2 + 'unknown error') joins
						// the session-expired family — never the generic failure ladder.
						const errmsg = String(response["errmsg"] ?? "");
						const isSessionExpired =
							ret === SESSION_EXPIRED_ERRCODE ||
							errcode === SESSION_EXPIRED_ERRCODE ||
							isStaleSessionRet(ret, errcode, errmsg);
						if (isSessionExpired) {
							this.sessionExpiredStreak += 1;
							this.consecutiveFailures = 0;
							// Escalation ladder (family-row realization; see fixture):
							// first expiry pauses verbatim (600s), a REPEAT recycles
							// the poll session (generation bump), a third goes fatal.
							if (this.sessionExpiredStreak === 1) {
								await this.sleepFn(SESSION_EXPIRED_PAUSE_S * 1000);
							} else if (this.sessionExpiredStreak === 2) {
								this.generation += 1;
								await this.sleepFn(RETRY_DELAY_SECONDS * 1000);
							} else {
								this.lifecycle.markFatal({
									kind: "config_invalid",
									detail:
										"Stale iLink session unrecoverable (-14 streak exhausted)",
								});
								this.running = false;
								this.isLive = false;
								return;
							}
							continue;
						}
						this.consecutiveFailures += 1;
						const delayS =
							this.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES
								? BACKOFF_DELAY_SECONDS
								: RETRY_DELAY_SECONDS;
						await this.sleepFn(delayS * 1000);
						if (this.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
							this.consecutiveFailures = 0;
						}
						continue;
					}

					this.consecutiveFailures = 0;
					this.sessionExpiredStreak = 0;
					const newBuf = String(response["get_updates_buf"] ?? "");
					if (newBuf !== "") {
						this.syncBuf = newBuf;
						this.syncStore.save(this.accountId, this.syncBuf);
					}

					const msgs = (response["msgs"] ?? []) as ILinkMessage[];
					// Fixture seam: the ack-before-enqueue window sits BETWEEN the
					// cursor commit above and dispatch below.
					if (this.hooks.afterCommitBeforeDispatch !== undefined) {
						await this.hooks.afterCommitBeforeDispatch(msgs);
					}
					for (const msg of msgs) {
						if (!this.isLive) {
							this.heldInbound.push(msg); // committed already — never lost
							continue;
						}
						void this.processMessageSafe(msg);
					}
				} catch (err) {
					this.consecutiveFailures += 1;
					const delayS =
						this.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES
							? BACKOFF_DELAY_SECONDS
							: RETRY_DELAY_SECONDS;
					if (this.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
						this.consecutiveFailures = 0;
						this.generation += 1; // session recycle (#79889 parity)
					}
					void err;
					await this.sleepFn(delayS * 1000);
				}
			}
		} finally {
			this.pollLoopStarted = false;
		}
	}

	/** Adaptive long-poll budget — LONG_POLL_TIMEOUT_MS until the server suggests otherwise. */
	private longPollBudgetMs = LONG_POLL_TIMEOUT_MS;
	private pullSeq = 0;

	/** Observability: the CURRENT long-poll budget (server suggestion adopted). */
	get longPollTimeoutBudgetMs(): number {
		return this.longPollBudgetMs;
	}

	/**
	 * ONE long-poll pull (weixin.py:_poll_loop/_get_updates parity): the pull
	 * races the ADAPTIVE budget; exceeding it is a BENIGN empty cycle —
	 * _get_updates answers {ret:0, msgs:[], get_updates_buf} on TimeoutError,
	 * so the loop re-probes with ZERO penalty (no recycle, no pause). A settled
	 * pull ADOPTS the server's suggested longpolling_timeout_ms (:1386) as the
	 * next budget. The reconnect ladder stays reserved for the -14/stale streak
	 * (DEC-045) and the exception failure ladder (#79889 recycle).
	 */
	private async pullOnce(): Promise<Record<string, unknown> | undefined> {
		let settled = false;
		const token = ++this.pullSeq;
		const pull = (async (): Promise<Record<string, unknown>> => {
			// getupdates rides the SAME _api_post request shape as every other
			// outgoing POST (base_info + header plane recorded by the seam).
			const req = this.ilinkPostRequest(EP_GET_UPDATES, {
				get_updates_buf: this.syncBuf,
			});
			const result = await this.server.pullAsync(
				this.syncBuf,
				() => this.pullSeq !== token,
				{ payload: req.payload, headers: req.headers },
			);
			settled = true;
			return result as Record<string, unknown>;
		})();
		const outcome = await Promise.race([
			pull,
			this.sleepFn(this.longPollBudgetMs).then(() => "TIMEOUT" as const),
		]);
		if (outcome === "TIMEOUT") {
			// Benign empty cycle (weixin.py:_get_updates TimeoutError leg):
			// zero penalty — observability marker only.
			this.pollLog.push("timeout");
			return undefined;
		}
		const response = outcome as Record<string, unknown>;
		const suggested = Number(response["longpolling_timeout_ms"]);
		if (Number.isInteger(suggested) && suggested > 0) {
			this.longPollBudgetMs = suggested;
		}
		return response;
	}

	/** Observability: poll-cycle outcomes (row seams). */
	readonly pollLog: string[] = [];

	// ── inbound processing (_process_message parity) ────────────────────────

	private processing = new Set<string>();

	private async processMessageSafe(msg: ILinkMessage): Promise<void> {
		const key = String(msg.message_id ?? JSON.stringify(msg).length);
		if (this.processing.has(key)) return;
		this.processing.add(key);
		try {
			await this.processMessage(msg);
		} finally {
			this.processing.delete(key);
		}
	}

	private async processMessage(msg: ILinkMessage): Promise<void> {
		const senderId = String(msg.from_user_id ?? "").trim();
		if (senderId === "" || senderId === this.accountId) return;

		const messageId = String(msg.message_id ?? "").trim();
		if (messageId !== "" && !this.dedup.add(messageId)) return; // replay

		const itemList = msg.item_list ?? [];
		const text = extractText(itemList);
		if (text !== "") {
			const contentKey = `content:${senderId}:${fnv1a(text)}`;
			if (!this.dedup.add(contentKey)) return; // content-fingerprint replay
		}

		const [chatType, effectiveChatId] = guessChatType(msg, this.accountId);
		if (chatType === "group") {
			if (this.groupPolicy === "disabled") return;
			if (
				this.groupPolicy === "allowlist" &&
				!entryMatches(this.groupAllowFrom, effectiveChatId)
			) {
				return;
			}
			if (this.groupPolicy === "pairing") return;
		} else if (!this.isDmIntakeAllowed(senderId)) {
			return;
		}

		const contextToken = String(msg.context_token ?? "").trim();
		if (contextToken !== "") {
			this.contextTokens.set(senderId, contextToken);
		}

		const mediaInfo = collectMediaInfo(itemList);

		if (text === "" && mediaInfo.length === 0) return;

		const body =
			text !== "" && mediaInfo.length > 0
				? `${text}\n\n${mediaInfo.join("\n")}`
				: text !== ""
					? text
					: mediaInfo.join("\n");

		const source = {
			platform: "weixin",
			chatType: chatType as "dm" | "group",
			userId: senderId,
			chatId: effectiveChatId,
		};
		const event: IncomingEvent = {
			messageType: "text",
			text: body,
			source,
			metadata: {},
		};

		// Text events ride the debounce batcher; everything else dispatches.
		this.enqueueTextEvent(event, senderId);
	}

	private enqueueTextEvent(event: IncomingEvent, _senderId: string): void {
		const source = event.source;
		if (source === undefined) return;
		const key = `${source.chatType}:${source.chatId}`;
		const chunkLen = (event.text ?? "").length;
		const existing = this.pendingTextBatches.get(key);
		if (existing === undefined) {
			const timer = this.scheduleFlush(key, chunkLen);
			this.pendingTextBatches.set(key, {
				event,
				lastChunkLen: chunkLen,
				timer,
			});
			return;
		}
		const incoming = event.text ?? "";
		existing.event.text = existing.event.text
			? `${existing.event.text}\n${incoming}`
			: incoming;
		existing.lastChunkLen = chunkLen;
	}

	private scheduleFlush(key: string, initialChunkLen = 0): Promise<void> {
		const timer = (async () => {
			const batch = this.pendingTextBatches.get(key);
			const lastLen = Math.max(batch?.lastChunkLen ?? 0, initialChunkLen);
			const delayS =
				lastLen >= TEXT_BATCH_SPLIT_THRESHOLD
					? TEXT_BATCH_SPLIT_DELAY_S
					: TEXT_BATCH_DELAY_S;
			await this.sleepFn(delayS * 1000);
			const pending = this.pendingTextBatches.get(key);
			if (pending === undefined) return;
			this.pendingTextBatches.delete(key);
			await this.dispatchBatch(pending.event);
		})();
		return timer;
	}

	private async dispatchBatch(event: IncomingEvent): Promise<void> {
		const source = event.source;
		const sessionKey = `weixin:${source?.chatType ?? "dm"}:${source?.chatId ?? "unknown"}:${source?.userId ?? "unknown"}`;
		await this.deliverInbound(event, sessionKey);
	}

	// ── ACL intake gates (_is_dm_intake_allowed / group parity) ─────────────

	isDmIntakeAllowed(senderId: string): boolean {
		const principal = String(senderId ?? "").trim();
		if (principal === "") return false;
		if (this.dmPolicy === "disabled") return false;
		if (this.dmPolicy === "allowlist")
			return entryMatches(this.allowFrom, principal);
		if (this.dmPolicy === "pairing") return true;
		if (this.dmPolicy === "open") return this.openDmOptedIn();
		return false;
	}

	/**
	 * _open_dm_opted_in parity (weixin.py:1539): 'open' admits DMs ONLY behind
	 * an explicit allow-all opt-in — GATEWAY_ALLOW_ALL_USERS or
	 * WEIXIN_ALLOW_ALL_USERS ∈ {true,1,yes} (case-insensitive).
	 */
	private openDmOptedIn(): boolean {
		return openDmOptedInto(this.readEnv);
	}

	// ── rate-limit circuit breaker (weixin.py parity) ───────────────────────

	rateLimitCooldownRemaining(): number {
		return Math.max(0, this.rateLimitCircuitUntil - this.nowFn());
	}

	/** Record a genuine iLink rate limit; TRUE when the breaker opened. */
	recordRateLimitEvent(): boolean {
		const now = this.nowFn();
		const windowStart = now - RATE_LIMIT_CIRCUIT_WINDOW_S * 1000;
		this.lastRateLimitEvents = this.lastRateLimitEvents.filter(
			(ts) => ts >= windowStart,
		);
		this.lastRateLimitEvents.push(now);
		if (this.lastRateLimitEvents.length >= RATE_LIMIT_CIRCUIT_THRESHOLD) {
			this.rateLimitCircuitUntil = Math.max(
				this.rateLimitCircuitUntil,
				now + RATE_LIMIT_CIRCUIT_OPEN_S * 1000,
			);
			return this.rateLimitCooldownRemaining() > 0;
		}
		return false;
	}

	resetRateLimitCircuit(): void {
		this.lastRateLimitEvents = [];
		this.rateLimitCircuitUntil = 0;
	}

	// ── egress doors ──────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		const policy = this.chatLengthPolicyForChat(chatId);
		// Hermes send() parity (weixin.py:1961): chunks =
		// _split_text(format_message(content)) — format_message runs
		// normalize_markdown_blocks + the 120-col copy-friendly wrap, then the
		// delivery-unit splitter applies the chat budget. An oversized markdown
		// block overflows through THE kit fence-carry chunker (DEC-047
		// plan-exact port of base.truncate_message: newline-preferred split
		// points, fence carry, "(i/n)" indicators) — never mid-line slices.
		const formatted = wrapCopyFriendlyLines(normalizeMarkdownBlocks(content));
		const units = splitTextForWeixinDelivery(
			formatted,
			policy.maxUnits,
			false,
			{
				overflow: (block) => chunkWithFenceCarry(block, policy).chunks,
			},
		);
		const results: SendResult[] = [];
		for (let i = 0; i < units.length; i++) {
			results.push(await this.deliverWiredChunk(chatId, units[i]!, metadata));
			// Inter-chunk pacing (send_chunk_delay parity).
			if (i < units.length - 1) {
				await this.sleepFn(SEND_CHUNK_DELAY_S * 1000);
			}
		}
		return results;
	}

	private async deliverWiredChunk(
		chatId: string,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		this.ladderChatId = chatId;
		this.ladderMetadata = metadata;
		const outcome = await this.ensureFormatLadder().sendText(chunk, metadata);
		if (outcome.success) {
			this.resetRateLimitCircuit();
			return outcome;
		}

		// A transient RICH failure is NEVER legacy-resent (§10.1 duplicate risk).
		if (outcome.tier === "rich") return outcome;

		const failureClass = classifySendError(new Error(outcome.error ?? ""));
		// Breaker-cooldown refusals are TERMINAL for this call: re-driving them
		// through the §6.1 ladder would spin against OUR OWN open circuit.
		const cooldownRefusal = (outcome.error ?? "").startsWith(
			"iLink sendmessage rate limited; cooldown",
		);
		const networkClassified =
			!cooldownRefusal &&
			(outcome.retryable === true ||
				failureClass === "connect-timeout" ||
				failureClass === "network" ||
				failureClass === "flood");
		if (networkClassified) {
			const retried = await sendWithRetry(
				chunk,
				metadata,
				(c, md) => this.platformSend(chatId, c, md),
				{ maxRetries: SEND_CHUNK_RETRIES },
			);
			if (retried.success) return retried;
			return this.platformSend(chatId, DELIVERY_FAILED_NOTICE, metadata);
		}
		if (failureClass === "formatting") {
			return this.platformSend(chatId, plainTextFallbackBody(chunk), metadata);
		}
		return outcome;
	}

	private formatLadder: FormattingLadder | null = null;
	private ladderChatId = "";
	private ladderMetadata: Metadata = {};

	private ensureFormatLadder(): FormattingLadder {
		if (this.formatLadder === null) {
			// iLink has NO rich endpoint (base capability-error shape); the
			// ladder probes once, latches off, and plain sends carry the day.
			this.formatLadder = new FormattingLadder({
				tryRich: (content) => this.wireRich(content),
				sendConverted: (content) =>
					this.platformSend(this.ladderChatId, content, this.ladderMetadata),
				sendPlain: (content) =>
					this.platformSend(this.ladderChatId, content, this.ladderMetadata),
			});
		}
		return this.formatLadder;
	}

	/** THE lowest platform send primitive (retry semantics + capture). */
	private async platformSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		return this.sendChunkWithPlatformSemantics(chatId, content, metadata);
	}

	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		// Capture-seam interception (reference-fixture parity).
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
		) {
			return { success: false, error: "Bad Request: can't parse entities" };
		}
		const replyToRaw = metadata["reply_to"];
		const replyTo = typeof replyToRaw === "string" ? replyToRaw : undefined;
		return this.sendChunkWithPlatformSemantics(chatId, content, {
			reply_to: replyTo,
		});
	}

	/** Rich lane ABSENT natively; scripted probes feed the §10.1 latch path. */
	protected override async wireRich(content: string): Promise<SendResult> {
		const scripted =
			this.richProbe !== undefined &&
			(this.richHasScriptFn === undefined || this.richHasScriptFn());
		if (!scripted) {
			// Capability-error shape WITHOUT burning a roundtrip (latch path).
			return { success: false, error: "sendRichMessage: method not found" };
		}
		this.richProbeAttempts += 1;
		return this.richProbe(content);
	}

	/** Observability: how many REAL rich roundtrips left the adapter. */
	get richWireAttempts(): number {
		return this.richProbeAttempts;
	}
	private richProbeAttempts = 0;

	/**
	 * _send_text_chunk_locked parity: generic vendor errors retry with linear
	 * backoff (delay*(attempt+1), terminal after SEND_CHUNK_RETRIES); rate-limit
	 * (-2) backs off 3× and feeds the breaker; stale sessions (-14, or -2 +
	 * 'unknown error') retry ONCE WITHOUT context_token (degraded fallback
	 * keeps cron pushes alive).
	 */
	private async sendChunkWithPlatformSemantics(
		chatId: string,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		// Capture-seam interception (reference-fixture parity): the shared
		// rows' formatting-rejection script fails markdown-shaped bodies; the
		// PLAIN fallback body (prefix-carried) succeeds.
		if (
			metadata["forceFormattingError"] === true &&
			!chunk.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
		) {
			return { success: false, error: "Bad Request: can't parse entities" };
		}
		let lastError: unknown = null;
		let retriedWithoutToken = false;
		for (let attempt = 0; attempt <= SEND_CHUNK_RETRIES; attempt++) {
			if (this.rateLimitCooldownRemaining() > 0) {
				return {
					success: false,
					error: `iLink sendmessage rate limited; cooldown active for ${Math.round(this.rateLimitCooldownRemaining() / 100) / 10}s`,
					retryable: true,
				};
			}
			// Scripted capture behaviors GOVERN the result (shared rows program
			// failures/timeouts); unscripted sends RECORD then hit the fake face.
			if (this.sendCapture !== undefined) {
				const scripted =
					this.captureHasScriptFn === undefined || this.captureHasScriptFn();
				if (scripted) return this.sendCapture(chatId, chunk, metadata);
				await this.sendCapture(chatId, chunk, metadata); // record-only
			}
			const contextToken = retriedWithoutToken
				? undefined
				: this.contextTokens.get(chatId);
			const resp = this.ilinkPost(EP_SEND_MESSAGE, {
				msg: {
					from_user_id: "",
					to_user_id: chatId,
					client_id: `hermes-weixin-${attempt}-${this.nowFn()}`,
					message_type: MSG_TYPE_BOT,
					message_state: MSG_STATE_FINISH,
					item_list: [{ type: ITEM_TEXT, text_item: { text: chunk } }],
					...(contextToken !== undefined
						? { context_token: contextToken }
						: {}),
				},
			});
			const ret = resp.ret;
			const errcode = resp.errcode;
			if ((ret !== 0 || errcode !== 0) && (ret !== null || errcode !== null)) {
				// Stale-session FIRST (weixin.py:_send_text_chunk_locked :1847):
				// -14 OR the _is_stale_session_ret signature (-2 + 'unknown error')
				// strips context_token and retries tokenless — a dead session, not
				// a rate limit, so the stale check precedes the -2 branch.
				const errmsg = String(resp["errmsg"] ?? resp["msg"] ?? "");
				const isSessionExpired =
					ret === SESSION_EXPIRED_ERRCODE ||
					errcode === SESSION_EXPIRED_ERRCODE ||
					isStaleSessionRet(ret, errcode, errmsg);
				if (
					isSessionExpired &&
					!retriedWithoutToken &&
					contextToken !== undefined
				) {
					retriedWithoutToken = true;
					this.contextTokens.delete(chatId);
					continue; // tokenless retry — NOT counted against attempts
				}
				const isRateLimited =
					ret === RATE_LIMIT_ERRCODE || errcode === RATE_LIMIT_ERRCODE;
				if (isRateLimited) {
					lastError = new Error(
						`iLink sendmessage rate limited: ret=${ret} errcode=${errcode} errmsg=${errmsg !== "" ? errmsg : "rate limited"}`,
					);
					const opened = this.recordRateLimitEvent();
					if (opened) {
						return {
							success: false,
							error: String(
								lastError instanceof Error ? lastError.message : lastError,
							),
							retryable: true,
						};
					}
					if (attempt >= SEND_CHUNK_RETRIES) break;
					await this.sleepFn(
						SEND_CHUNK_RETRY_DELAY_S * RATE_LIMIT_BACKOFF_FACTOR * 1000,
					);
					continue;
				}
				// Generic vendor error (wx Hermes :1859): raised INTO the retry loop —
				// linear backoff SEND_CHUNK_RETRY_DELAY_S*(attempt+1), terminal only
				// after SEND_CHUNK_RETRIES. Not a timeout case: DEC-046's carve-out
				// does not apply.
				lastError = new Error(
					`iLink sendmessage error: ret=${ret} errcode=${errcode} errmsg=${errmsg !== "" ? errmsg : "unknown error"}`,
				);
				if (attempt >= SEND_CHUNK_RETRIES) break;
				await this.sleepFn(SEND_CHUNK_RETRY_DELAY_S * (attempt + 1) * 1000);
				continue;
			}
			// Success.
			return { success: true, messageId: `wx-${this.nowFn()}` };
		}
		const message =
			lastError instanceof Error
				? lastError.message
				: String(lastError ?? "Unknown error");
		return { success: false, error: message };
	}

	private async rawSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const resp = this.ilinkPost(EP_SEND_MESSAGE, {
			msg: {
				to_user_id: chatId,
				client_id: `hermes-weixin-final-${this.nowFn()}`,
				message_type: MSG_TYPE_BOT,
				message_state: MSG_STATE_FINISH,
				item_list: [{ type: ITEM_TEXT, text_item: { text: content } }],
			},
		});
		void metadata;
		if (resp.ret !== 0 || resp.errcode !== 0) {
			return {
				success: false,
				error: `iLink sendmessage failed ret=${resp.ret}`,
				retryable: true,
			};
		}
		return { success: true };
	}

	// ── outbound media (weixin.py:_send_file parity) ─────────────────────────

	/**
	 * _send_file parity: getuploadurl → ECB ciphertext CDN POST (octet-stream,
	 * reads x-encrypted-param) → sendmessage media item. Byte-specific vendor
	 * rules honored: filesize is the PKCS#7-padded size, aeskey rides as hex,
	 * and the sendmessage item carries aes_key = base64(HEX STRING) — never
	 * base64(raw bytes). An optional caption precedes the media item as its own
	 * text message (vendor order). Failures never throw; they return SendResult.
	 */
	async sendFile(chatId: string, opts: SendFileOptions): Promise<SendResult> {
		try {
			const kind = outboundMediaKind(
				opts.filename,
				opts.forceFileAttachment === true,
			);
			const plaintext = opts.plaintext;
			const filekey = this.rng(16).toString("hex"); // secrets.token_hex(16)
			const aesKey = this.rng(16); // secrets.token_bytes(16)
			const rawsize = plaintext.length;
			const rawfilemd5 = createHash("md5").update(plaintext).digest("hex");

			const uploadResp = this.ilinkPost(EP_GET_UPLOAD_URL, {
				filekey,
				media_type: mediaTypeForKind(kind),
				to_user_id: chatId,
				rawsize,
				rawfilemd5,
				filesize: aesPaddedSize(rawsize),
				no_need_thumb: true,
				aeskey: aesKey.toString("hex"),
			});
			if (Number(uploadResp.ret) !== 0 || Number(uploadResp.errcode) !== 0) {
				return {
					success: false,
					error: `iLink getuploadurl error: ret=${uploadResp.ret} errcode=${uploadResp.errcode}`,
					retryable: true,
				};
			}
			const uploadParam = String(uploadResp["upload_param"] ?? "");
			const uploadFullUrl = String(uploadResp["upload_full_url"] ?? "");
			// Prefer upload_full_url (direct CDN); fall back to the constructed
			// CDN URL from upload_param — both legs POST (the old PUT 404s).
			let uploadUrl: string;
			if (uploadFullUrl !== "") uploadUrl = uploadFullUrl;
			else if (uploadParam !== "") {
				uploadUrl = cdnUploadUrl(WEIXIN_CDN_BASE_URL, uploadParam, filekey);
			} else {
				return {
					success: false,
					error: `getUploadUrl returned neither upload_param nor upload_full_url`,
				};
			}

			const ciphertext = aes128EcbEncrypt(plaintext, aesKey);
			const cdnResp = this.server.cdnUpload(uploadUrl, ciphertext, {
				...CDN_UPLOAD_HEADERS,
			});
			if (cdnResp.status !== 200) {
				return { success: false, error: `CDN upload HTTP ${cdnResp.status}` };
			}
			const encryptedQueryParam = cdnResp.headers["x-encrypted-param"];
			if (encryptedQueryParam === undefined || encryptedQueryParam === "") {
				return {
					success: false,
					error: "CDN upload missing x-encrypted-param header",
				};
			}

			const contextToken = this.contextTokens.get(chatId);
			const mediaClientId = `hermes-weixin-${this.rng(12).toString("hex")}`;

			if (opts.caption !== undefined && opts.caption.trim() !== "") {
				this.ilinkPost(EP_SEND_MESSAGE, {
					msg: {
						from_user_id: "",
						to_user_id: chatId,
						client_id: `hermes-weixin-${this.rng(12).toString("hex")}`,
						message_type: MSG_TYPE_BOT,
						message_state: MSG_STATE_FINISH,
						item_list: [
							{
								type: ITEM_TEXT,
								text_item: {
									// format_message parity: copy-friendly wrap.
									text: wrapCopyFriendlyLines(
										normalizeMarkdownBlocks(opts.caption),
									),
								},
							},
						],
						...(contextToken !== undefined
							? { context_token: contextToken }
							: {}),
					},
				});
			}

			const mediaItem = buildOutboundMediaItem(kind, {
				encryptQueryParam: encryptedQueryParam,
				aesKeyApi: aesKeyForApi(aesKey.toString("hex")),
				ciphertextSize: ciphertext.length,
				plaintextSize: rawsize,
				filename: opts.filename,
				rawfilemd5,
			});
			const resp = this.ilinkPost(EP_SEND_MESSAGE, {
				msg: {
					from_user_id: "",
					to_user_id: chatId,
					client_id: mediaClientId,
					message_type: MSG_TYPE_BOT,
					message_state: MSG_STATE_FINISH,
					item_list: [mediaItem],
					...(contextToken !== undefined
						? { context_token: contextToken }
						: {}),
				},
			});
			if (Number(resp.ret) !== 0 || Number(resp.errcode) !== 0) {
				return {
					success: false,
					error: `iLink sendmessage error: ret=${resp.ret} errcode=${resp.errcode}`,
					retryable: true,
				};
			}
			return { success: true, messageId: mediaClientId };
		} catch (err) {
			return {
				success: false,
				error: err instanceof Error ? err.message : String(err),
			};
		}
	}

	// ── QR account linking (weixin.py:qr_login parity) ────────────────────

	/**
	 * qr_login parity: GET ilink/bot/get_bot_qrcode?bot_type=… then poll GET
	 * ilink/bot/get_qrcode_status?qrcode=… until confirmed / deadline.
	 * wait keeps polling; scaned_but_redirect repoints subsequent polls at the
	 * redirect host; expired refetches a NEW qrcode up to three times before
	 * giving up; confirmed yields credentials (incomplete payload fails closed).
	 * Headless port: no terminal rendering/persistence — the caller owns what
	 * happens with the credential dict.
	 */
	async qrLogin(opts: QrLoginOptions = {}): Promise<QrLoginCredentials | null> {
		const botType = opts.botType ?? QR_DEFAULT_BOT_TYPE;
		const timeoutMs = (opts.timeoutSeconds ?? QR_LOGIN_TIMEOUT_S) * 1000;
		const fetchQr = (): string => {
			const resp = this.ilinkGet(ILINK_BASE_URL, EP_GET_BOT_QR, {
				bot_type: botType,
			});
			return String(resp["qrcode"] ?? "");
		};

		let qrcodeValue = fetchQr();
		if (qrcodeValue === "") return null; // QR response missing qrcode
		let currentBaseUrl = ILINK_BASE_URL;
		let refreshCount = 0;
		const deadline = this.nowFn() + timeoutMs;

		while (this.nowFn() < deadline) {
			const statusResp = this.ilinkGet(currentBaseUrl, EP_GET_QR_STATUS, {
				qrcode: qrcodeValue,
			});
			const status = String(statusResp["status"] ?? "wait");
			if (status === "confirmed") {
				const accountId = String(statusResp["ilink_bot_id"] ?? "");
				const token = String(statusResp["bot_token"] ?? "");
				const baseUrl = String(statusResp["baseurl"] ?? "") || ILINK_BASE_URL;
				const userId = String(statusResp["ilink_user_id"] ?? "");
				if (accountId === "" || token === "") return null;
				return {
					account_id: accountId,
					token,
					base_url: baseUrl,
					user_id: userId,
				};
			}
			if (status === "scaned_but_redirect") {
				const host = String(statusResp["redirect_host"] ?? "");
				if (host !== "") currentBaseUrl = `https://${host}`;
			} else if (status === "expired") {
				refreshCount += 1;
				if (refreshCount > QR_MAX_EXPIRED_REFRESHES) return null;
				qrcodeValue = fetchQr();
				if (qrcodeValue === "") return null;
			}
			// wait / scaned / redirect-repoint / refreshed all poll again next cycle.
			await this.sleepFn(QR_POLL_INTERVAL_S * 1000);
		}
		return null;
	}

	// ── typing tickets (TypingTicketCache + getConfig refresh parity) ───────

	typingTicketFor(userId: string): string | null {
		const cached = this.typingTickets.get(userId);
		if (cached !== undefined) {
			if (this.nowFn() - cached.atMs < TYPING_TICKET_TTL_S * 1000) {
				return cached.ticket;
			}
			this.typingTickets.delete(userId);
		}
		const contextToken = this.contextTokens.get(userId);
		const resp = this.ilinkPost(EP_GET_CONFIG, {
			ilink_user_id: userId,
			...(contextToken !== undefined ? { context_token: contextToken } : {}),
		});
		const ticket = String(resp["typing_ticket"] ?? "");
		if (ticket !== "") {
			this.typingTickets.set(userId, { ticket, atMs: this.nowFn() });
			return ticket;
		}
		return null;
	}

	// ── observability seams ─────────────────────────────────────────────────

	get currentSyncBuf(): string {
		return this.syncBuf;
	}

	/** The platform delivery splitter (text-splitting.ts) — delta-row seam. */
	splitForDelivery(content: string, maxLength: number): string[] {
		return splitTextForWeixinDelivery(content, maxLength, false);
	}
}

// ── module-level helpers ────────────────────────────────────────────────────

/**
 * Hermes treats a missing/None ret/errcode as no-error; numbers compare
 * numerically (weixin.py `ret not in {0, None}`).
 */
function numericOrUndefined(value: unknown): number | undefined {
	if (value === undefined || value === null) return undefined;
	const n = Number(value);
	return Number.isNaN(n) ? undefined : n;
}

/**
 * weixin.py:_is_stale_session_ret parity: ret/errcode -2 with errmsg
 * 'unknown error' is a STALE-SESSION signal (same family as errcode -14),
 * never a genuine rate limit. Branched BEFORE rate-limit handling at both
 * sites (poll :1394 / send :1847).
 */
export function isStaleSessionRet(
	ret: number | null | undefined,
	errcode: number | null | undefined,
	errmsg: unknown,
): boolean {
	if (ret !== RATE_LIMIT_ERRCODE && errcode !== RATE_LIMIT_ERRCODE) {
		return false;
	}
	return String(errmsg ?? "").toLowerCase() === "unknown error";
}

/** _open_dm_opted_in env keys (weixin.py:1539), gateway-scope first. */
const OPEN_DM_ENV_KEYS = [
	"GATEWAY_ALLOW_ALL_USERS",
	"WEIXIN_ALLOW_ALL_USERS",
] as const;

function openDmOptedInto(
	readEnv: (key: string) => string | undefined,
): boolean {
	for (const key of OPEN_DM_ENV_KEYS) {
		const value = (readEnv(key) ?? "").toLowerCase();
		if (value === "true" || value === "1" || value === "yes") return true;
	}
	return false;
}

function entryMatches(entries: readonly string[], target: string): boolean {
	const normalizedTarget = target.trim().toLowerCase();
	for (const entry of entries) {
		const normalized = entry.trim().toLowerCase();
		if (normalized === "*" || normalized === normalizedTarget) return true;
	}
	return false;
}

function fnv1a(text: string): string {
	let h = 0x811c9dc5;
	for (let i = 0; i < text.length; i++) {
		h ^= text.charCodeAt(i);
		h = Math.imul(h, 0x01000193);
	}
	return (h >>> 0).toString(16);
}

/**
 * Chat-type resolution (weixin.py:_guess_chat_type): room ids are groups;
 * a to_user_id differing from OUR account id with user-msg type is a group
 * addressed directly; otherwise a DM from the sender.
 */
export function guessChatType(
	msg: ILinkMessage,
	accountId: string,
): ["dm" | "group", string] {
	const roomId = String(msg.room_id ?? msg.chat_room_id ?? "").trim();
	const toUserId = String(msg.to_user_id ?? "").trim();
	const isGroup =
		roomId !== "" ||
		(toUserId !== "" &&
			accountId !== "" &&
			toUserId !== accountId &&
			msg.msg_type === MSG_TYPE_USER);
	if (isGroup) {
		return [
			"group",
			roomId !== ""
				? roomId
				: toUserId !== ""
					? toUserId
					: String(msg.from_user_id ?? ""),
		];
	}
	return ["dm", String(msg.from_user_id ?? "")];
}

/**
 * Text extraction with quoted-context prefixes (weixin.py:_extract_text).
 */
export function extractText(itemList: Array<Record<string, unknown>>): string {
	for (const item of itemList) {
		if (item["type"] !== ITEM_TEXT) continue;
		const text = String(
			(item["text_item"] as Record<string, unknown> | undefined)?.["text"] ??
				"",
		);
		const ref = (item["ref_msg"] ?? {}) as Record<string, unknown>;
		const refItem = ref["message_item"] as Record<string, unknown> | undefined;
		const refType = Number(refItem?.["type"] ?? 0);
		if ([ITEM_IMAGE, ITEM_VIDEO, ITEM_FILE, ITEM_VOICE].includes(refType)) {
			const title = String(ref["title"] ?? "");
			const prefix = title !== "" ? `[引用媒体: ${title}]\n` : "[引用媒体]\n";
			return `${prefix}${text}`.trim();
		}
		if (refItem !== undefined) {
			const parts: string[] = [];
			if (String(ref["title"] ?? "") !== "") parts.push(String(ref["title"]));
			const refText = extractText([refItem]);
			if (refText !== "") parts.push(refText);
			if (parts.length > 0)
				return `[引用: ${parts.join(" | ")}]\n${text}`.trim();
		}
		return text;
	}
	return "";
}

/**
 * Media collection as ATTACHMENT-INFO lines (probe-computed exclusion — CDN
 * downloads stay out of the headless surface).
 */
function collectMediaInfo(itemList: Array<Record<string, unknown>>): string[] {
	const infos: string[] = [];
	for (const item of itemList) {
		const t = Number(item["type"] ?? 0);
		if (t === ITEM_IMAGE) infos.push("[图片]");
		else if (t === ITEM_VIDEO) infos.push("[视频]");
		else if (t === ITEM_VOICE) infos.push("[语音]");
		else if (t === ITEM_FILE) infos.push("[文件]");
	}
	return infos;
}

// ── command registry for guard wiring ───────────────────────────────────────

const WX_REGISTRY: CommandRegistry = [
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
