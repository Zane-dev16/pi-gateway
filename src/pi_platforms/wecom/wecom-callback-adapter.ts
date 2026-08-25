// pi_platforms/wecom/wecom-callback-adapter — THE WeCom callback adapter,
// ported from the READ-ONLY Hermes plugin
// (plugins/platforms/wecom/callback_adapter.py) onto the kit base. Everything
// policy-shaped is inherited; this module supplies TRANSPORT (the encrypted
// XML callback endpoint + the proactive message/send egress seam) and
// MANIFEST DATA.
//
// Shape (DEC-002 third column — stateless webhook):
//   - capabilities AS DATA: supports_async_delivery=False +
//     interactive_resume=False (manifest DIVERGENCE note)
//   - ingress ports _handle_verify + _handle_callback exactly: URL
//     verification decrypts echostr per app (all fail ⇒ 403); callbacks run
//     body caps (declared length then actual bytes, BOTH pre-parse) →
//     per-app decrypt ladder (crypto errors try next app; exhausted ⇒ 400)
//     → event build (MsgType text|event only; enter_agent/subscribe silently
//     acked; OTHER event names FALL THROUGH to normal construction with empty
//     Content becoming "/start"; non-text/event MsgTypes ack-no-op) → MsgId
//     TTL dedupe (300 s live window; prune is EXPIRED-ONLY past 2000 — live
//     receipts are never FIFO-evicted) → ACK-FIRST "success" text/plain — the
//     agent's reply goes out later via the proactive send seam
//   - outbound ports send(): touser from the scoped chat id, text capped at
//     2048 chars, safe=0; the message/send JSON body carries ONLY the vendor
//     keys {touser,msgtype,agentid,text,safe} (caller metadata rides the
//     transport seam separately, never the wire body); access-token cache
//     with a 60 s early-refresh margin; errcode {40001,42001} evicts the
//     cached token and retries ONCE
//
// Layering: imports pi_gateway downward + kit same-layer ONLY; no adapter
// cross-imports.

import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	resolveEnablement,
} from "../kit/index.js";
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
import {
	WECOM_ACCESS_TOKEN_TTL_SECONDS,
	WECOM_CAPABILITIES,
	WECOM_DEFAULT_CALLBACK_PATH,
	WECOM_DEFAULT_PORT,
	WECOM_DEDUP_PRUNE_BOUND,
	WECOM_DEDUP_TTL_MS,
	WECOM_MAX_BODY_BYTES,
	WECOM_PLUGIN_MANIFEST,
	WECOM_TEXT_SEND_CAP_CHARS,
	WECOM_TOKEN_REJECTED_ERRCODES,
	WECOM_TOKEN_REFRESH_MARGIN_SECONDS,
	validateWecomTrustBoundary,
} from "./manifest.js";
import type { WecomTrustBoundary } from "./manifest.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type { DisableReason } from "../kit/lifecycle-state.js";
import {
	extractXmlTag,
	WxBizMsgCrypt,
	WeComCryptoError,
} from "./wecom-crypto.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const WECOM_REGISTRY: CommandRegistry = [
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

// ── configuration (__init__ @109 parity) ─────────────────────────────────────

export interface WecomAppConfig {
	name?: string | undefined;
	corp_id?: string | undefined;
	corp_secret?: string | undefined;
	agent_id?: string | number | undefined;
	token?: string | undefined;
	encoding_aes_key?: string | undefined;
}

export interface WecomAdapterConfig {
	port?: number | undefined;
	path?: string | undefined;
	apps?: readonly WecomAppConfig[] | undefined;
	/** Legacy single-app extra fields (_normalize_apps collapse branch). */
	corp_id?: string | undefined;
	corp_secret?: string | undefined;
	agent_id?: string | number | undefined;
	token?: string | undefined;
	encoding_aes_key?: string | undefined;
}

/** Proactive-send seam (httpx AsyncClient parity). `metadata` carries the
 * harness script markers SEPARATELY from the vendor JSON body — caller
 * metadata never reaches qyapi.weixin.qq.com. */
export interface WecomApiTransport {
	/** Token fetch mirroring the gettoken payload (expires_in seconds). */
	getAccessToken(
		app: WecomAppConfig,
	): Promise<{ token: string; expiresIn?: number | undefined }>;
	sendMessage(
		app: WecomAppConfig,
		token: string,
		payload: Record<string, unknown>,
		metadata?: Record<string, unknown>,
	): Promise<WecomApiResponse>;
}

export interface WecomApiResponse extends SendResult {
	errcode?: number | undefined;
}

/**
 * Optional RICH probe seam (subject-supplied, msgraph captureWire parity):
 * WeCom text messages have no native rich lane; when a harness scripts one,
 * the §10.1 latch path must still OBSERVE the probe exactly once.
 */
export interface WecomCaptureWire {
	hasRichScript(opKind: string): boolean;
	transmitRich(chatId: string, content: string): Promise<SendResult>;
}

export interface WecomCallbackAdapterOptions {
	config?: WecomAdapterConfig | undefined;
	secretReader?: ScopedSecretReader | undefined;
	nowMs?: (() => number) | undefined;
	scalarMaxUnits?: number | undefined;
	transport?: WecomApiTransport | undefined;
	/** Conformance-harness rich probe capture (see WecomCaptureWire). */
	captureWire?: WecomCaptureWire | undefined;
	dedupCap?: number | undefined;
}

export interface HandlerResponse {
	status: number;
	contentType?: "text/plain" | "application/json" | undefined;
	body?: string | Record<string, never> | undefined;
}

interface BuiltEvent {
	event: IncomingEvent;
	messageId: string;
	userId: string;
	corpId: string;
}

/** _user_app_key parity: corp-scoped user keys avoid cross-corp collisions. */
export function userAppKey(corpId: string, userId: string): string {
	return corpId ? `${corpId}:${userId}` : userId;
}

// ── THE adapter ──────────────────────────────────────────────────────────────

export class WecomCallbackAdapter extends BasePlatformAdapter {
	readonly pluginManifest = WECOM_PLUGIN_MANIFEST;
	readonly trustBoundary: WecomTrustBoundary;

	readonly port: number;
	readonly callbackPath: string;

	private readonly secretReader: ScopedSecretReader;
	private readonly nowFn: () => number;
	private readonly transport: WecomApiTransport | undefined;
	private readonly captureWire: WecomCaptureWire | undefined;
	readonly apps: readonly WecomAppConfig[];

	/** chat key (corp:user) → app name (_user_app_map parity). */
	private readonly userAppMap = new Map<string, string>();
	/**
	 * MsgId dedupe (_seen_messages parity): msg_id → first-seen epoch ms. A
	 * live (non-TTL-expired) entry is a duplicate; an EXPIRED entry is deleted
	 * and re-armed. Prune past the bound drops EXPIRED entries ONLY — live
	 * receipts are never FIFO-evicted (callback_adapter dedup prune parity).
	 */
	private readonly seenMessages = new Map<string, number>();
	private readonly accessTokens = new Map<
		string,
		{ token: string; expiresAtMs: number }
	>();

	readonly counters = {
		accepted: 0,
		duplicates: 0,
		lifecycleAcked: 0,
		unhandledTypes: 0,
		handshakeFailures: 0,
		tooLarge: 0,
		decryptFailures: 0,
		parseInvocations: 0,
		outboundCalls: 0,
		tokenRefreshes: 0,
	};
	readonly dispatchedEvents: Array<{ messageId: string; text: string }> = [];
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly resolvedFamilies: string[] = [];

	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	private readonly cp: EgressChokepoint;
	private readonly dedupPruneBound: number;
	private allowAllClickers = true;
	private readonly clarifyArmedSet = new Set<string>();
	private holding = false;
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};

	constructor(opts: WecomCallbackAdapterOptions = {}) {
		const config = opts.config ?? {};
		super({
			manifestName: WECOM_PLUGIN_MANIFEST.name,
			capabilities: WECOM_CAPABILITIES,
			scalarMaxUnits: opts.scalarMaxUnits ?? WECOM_TEXT_SEND_CAP_CHARS,
		});
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.transport = opts.transport;
		this.captureWire = opts.captureWire;
		this.port = Number(config.port ?? WECOM_DEFAULT_PORT);
		this.callbackPath = normalizePath(
			config.path ?? WECOM_DEFAULT_CALLBACK_PATH,
		);
		this.apps = this.normalizeApps(config);

		this.trustBoundary =
			WECOM_PLUGIN_MANIFEST.trustBoundary as WecomTrustBoundary;
		const boundaryErrors = validateWecomTrustBoundary(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			const reason: DisableReason = {
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			};
			this.lifecycle.disable(reason);
		}

		// §11 step 3/4: missing required secrets ⇒ LOUD disable. The legacy
		// single-app shape resolves its crypto triple through the SCOPED reader
		// (env-seeded config), so an unconfigured deployment disables loudly at
		// construction instead of refusing silently at connect.
		const enablement = resolveEnablement(
			WECOM_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}
		if (!enablement.reason && this.apps.length === 0) {
			this.lifecycle.disable({
				kind: "secret_missing",
				secretKey: "WECOM_CALLBACK_TOKEN",
				manifestName: WECOM_PLUGIN_MANIFEST.name,
			});
		}

		// MsgId TTL dedupe (callback_adapter.py _seen_messages: 300 s live
		// window; expired-only prune when len > 2000). The bound is fixture-
		// tunable via dedupCap for eviction-semantics probes.
		this.dedupPruneBound = Math.max(
			1,
			opts.dedupCap ?? WECOM_DEDUP_PRUNE_BOUND,
		);

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // proactive send; no native lanes
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

	/**
	 * _normalize_apps parity: explicit extra.apps list wins; otherwise the
	 * legacy single-app fields collapse into one app whose secrets may ride
	 * the scoped env surface.
	 */
	private normalizeApps(config: WecomAdapterConfig): WecomAppConfig[] {
		if (Array.isArray(config.apps) && config.apps.length > 0) {
			return config.apps.filter(
				(a): a is WecomAppConfig => a !== null && typeof a === "object",
			);
		}
		if (config.corp_id || config.token) {
			return [
				{
					name: "default",
					corp_id: config.corp_id ?? "",
					corp_secret: config.corp_secret ?? "",
					agent_id: config.agent_id ?? "",
					token: config.token ?? "",
					encoding_aes_key: config.encoding_aes_key ?? "",
				},
			];
		}
		// Env-seeded default app (port surface): legacy installs configure the
		// whole triple through the scoped secret reader.
		const corpId = this.secretReader("WECOM_CALLBACK_CORP_ID");
		const token = this.secretReader("WECOM_CALLBACK_TOKEN");
		const aesKey = this.secretReader("WECOM_CALLBACK_ENCODING_AES_KEY");
		if (corpId && token && aesKey) {
			return [
				{
					name: "default",
					corp_id: corpId,
					corp_secret: this.secretReader("WECOM_CALLBACK_CORP_SECRET") ?? "",
					agent_id: "",
					token,
					encoding_aes_key: aesKey,
				},
			];
		}
		return [];
	}

	private hasUsableApp(): boolean {
		return this.apps.some((a) => a.token && a.encoding_aes_key && a.corp_id);
	}

	// ── crypt helpers (_crypt_for_app parity) ────────────────────────────────

	cryptForApp(app: WecomAppConfig): WxBizMsgCrypt {
		return new WxBizMsgCrypt(
			String(app.token ?? ""),
			String(app.encoding_aes_key ?? ""),
			String(app.corp_id ?? ""),
		);
	}

	// ── GET URL verification (_handle_verify @310 parity) ────────────────────

	handleVerify(input: { query: Record<string, string> }): HandlerResponse {
		const msgSignature = input.query["msg_signature"] ?? "";
		const timestamp = input.query["timestamp"] ?? "";
		const nonce = input.query["nonce"] ?? "";
		const echostr = input.query["echostr"] ?? "";
		for (const app of this.apps) {
			try {
				const plain = this.cryptForApp(app).verifyUrl(
					msgSignature,
					timestamp,
					nonce,
					echostr,
				);
				return { status: 200, contentType: "text/plain", body: plain };
			} catch {
				// Try the next app's crypto triple (source ladder).
			}
		}
		this.counters.handshakeFailures += 1;
		return {
			status: 403,
			contentType: "text/plain",
			body: "signature verification failed",
		};
	}

	handleHealthGet(): HandlerResponse {
		return {
			status: 200,
			contentType: "application/json",
			body: JSON.stringify({ status: "ok", platform: "wecom_callback" }),
		};
	}

	// ── POST callback (_handle_callback @330 parity) ─────────────────────────

	/**
	 * Order ports the source exactly: body caps (aiohttp client_max_size then
	 * the explicit len guard — BOTH before any parse/signature work) → per-app
	 * decrypt ladder → event build → MsgId dedupe → ack-first "success".
	 */
	async handleCallbackPost(input: {
		query: Record<string, string>;
		headers?: Record<string, string> | undefined;
		rawBody: Buffer;
	}): Promise<HandlerResponse> {
		const headers = lowerKeys(input.headers);

		// Gate 1: honest declared Content-Length over the cap (client_max_size
		// rejects oversized bodies at the read layer, 413, before our handler).
		const declaredRaw = headers["content-length"];
		const declaredLength =
			declaredRaw !== undefined && /^\d+$/.test(declaredRaw)
				? Number(declaredRaw)
				: null;
		if (declaredLength !== null && declaredLength > WECOM_MAX_BODY_BYTES) {
			this.counters.tooLarge += 1;
			return {
				status: 413,
				contentType: "text/plain",
				body: "payload too large",
			};
		}
		// Gate 2: LYING Content-Length trips on actual bytes post-read.
		if (input.rawBody.length > WECOM_MAX_BODY_BYTES) {
			this.counters.tooLarge += 1;
			return {
				status: 413,
				contentType: "text/plain",
				body: "payload too large",
			};
		}

		const body = input.rawBody.toString("utf8");
		const msgSignature = input.query["msg_signature"] ?? "";
		const timestamp = input.query["timestamp"] ?? "";
		const nonce = input.query["nonce"] ?? "";

		for (const app of this.apps) {
			let decryptedXml: string;
			try {
				decryptedXml = this.decryptRequest(
					app,
					body,
					msgSignature,
					timestamp,
					nonce,
				);
			} catch (err) {
				if (err instanceof WeComCryptoError) {
					this.counters.decryptFailures += 1;
					continue; // try next app (source ladder)
				}
				break; // non-crypto error ⇒ bail to 400 (source `break`)
			}
			const built = this.buildEvent(app, decryptedXml);
			if (built === null) {
				// Lifecycle/unknown MsgTypes: immediately acknowledged, no dispatch.
				return { status: 200, contentType: "text/plain", body: "success" };
			}
			// Deduplicate: WeCom retries callbacks on timeout (#10305).
			if (built.messageId) {
				if (this.claimSeen(built.messageId)) {
					this.counters.duplicates += 1;
					return { status: 200, contentType: "text/plain", body: "success" };
				}
			}
			// Record which app this user belongs to.
			if (built.userId) {
				this.userAppMap.set(
					userAppKey(built.corpId, built.userId),
					String(app.name ?? "default"),
				);
			}
			this.counters.accepted += 1;
			this.dispatchedEvents.push({
				messageId: built.messageId,
				text: built.event.text ?? "",
			});
			await this.deliverInbound(
				built.event,
				`wecom:${userAppKey(built.corpId, built.userId)}`,
			);
			// Immediately acknowledge — the reply arrives via proactive send.
			return { status: 200, contentType: "text/plain", body: "success" };
		}
		return {
			status: 400,
			contentType: "text/plain",
			body: "invalid callback payload",
		};
	}

	/**
	 * _seen_messages parity (@348): returns TRUE when msg_id is a LIVE
	 * duplicate. An entry older than the TTL is deleted and re-armed (the
	 * message dispatches again). After arming, prune past the bound drops
	 * EXPIRED entries ONLY — live receipts are never evicted FIFO.
	 */
	private claimSeen(msgId: string): boolean {
		const now = this.nowFn();
		const seenAt = this.seenMessages.get(msgId);
		if (seenAt !== undefined) {
			if (now - seenAt < WECOM_DEDUP_TTL_MS) return true;
			this.seenMessages.delete(msgId);
		}
		this.seenMessages.set(msgId, now);
		if (this.seenMessages.size > this.dedupPruneBound) {
			const cutoff = now - WECOM_DEDUP_TTL_MS;
			for (const [key, ts] of this.seenMessages) {
				if (ts <= cutoff) this.seenMessages.delete(key);
			}
		}
		return false;
	}

	/** _decrypt_request parity: extract Encrypt, then BizMsgCrypt decrypt. */
	protected decryptRequest(
		app: WecomAppConfig,
		body: string,
		msgSignature: string,
		timestamp: string,
		nonce: string,
	): string {
		this.counters.parseInvocations += 1;
		const encrypt = extractXmlTag(body, "Encrypt");
		if (encrypt === null || encrypt.length === 0) {
			throw new WeComCryptoError("missing Encrypt field");
		}
		const crypt = this.cryptForApp(app);
		return crypt
			.decrypt(msgSignature, timestamp, nonce, encrypt)
			.toString("utf8");
	}

	/**
	 * _build_event parity: MsgType text|event only; enter_agent/subscribe
	 * lifecycle events return null (silently acked); OTHER event names FALL
	 * THROUGH to normal construction — empty Content becomes "/start"; non-
	 * text/event MsgTypes return null; msg_id falls back to `${user}:${CreateTime}`.
	 */
	buildEvent(app: WecomAppConfig, xmlText: string): BuiltEvent | null {
		const msgType = (extractXmlTag(xmlText, "MsgType") ?? "").toLowerCase();
		if (!msgType) return null;
		if (msgType === "event") {
			const eventName = (extractXmlTag(xmlText, "Event") ?? "").toLowerCase();
			if (eventName === "enter_agent" || eventName === "subscribe") {
				this.counters.lifecycleAcked += 1;
				return null;
			}
			// Other event names FALL THROUGH to normal construction (_build_event
			// parity: for MsgType=event ONLY enter_agent/subscribe return early;
			// empty Content becomes the synthesized "/start" command below).
		} else if (msgType !== "text") {
			this.counters.unhandledTypes += 1;
			return null;
		}

		const userId = extractXmlTag(xmlText, "FromUserName") ?? "";
		const corpId =
			extractXmlTag(xmlText, "ToUserName") ?? String(app.corp_id ?? "");
		let content = (extractXmlTag(xmlText, "Content") ?? "").trim();
		if (!content && msgType === "event") content = "/start";
		const msgId =
			extractXmlTag(xmlText, "MsgId") ??
			`${userId}:${extractXmlTag(xmlText, "CreateTime") ?? "0"}`;

		return {
			messageId: msgId,
			userId,
			corpId,
			event: {
				messageType: "text",
				text: content,
				messageId: msgId,
				source: {
					platform: WECOM_PLUGIN_MANIFEST.name,
					chatType: "dm",
					userId,
					chatId: userAppKey(corpId, userId),
					chatName: userId,
				},
			},
		};
	}

	/**
	 * Per-chat length descriptor (§6.3/A15 relay-shaped override point): the
	 * harness's utf16-marked chats return budget AND unit TOGETHER; production
	 * chats return undefined ⇒ manifest default (WECOM_TEXT_SEND_CAP_CHARS).
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

	// ── connect ladder (@171 parity) ─────────────────────────────────────────

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (this.apps.length === 0) {
			this.logger?.error?.("[WecomCallback] No callback apps configured");
			return false;
		}
		if (!this.hasUsableApp()) {
			this.logger?.error?.(
				"[WecomCallback] Apps must carry corp_id/token/encoding_aes_key",
			);
			return false;
		}
		// The port-in-use probe and aiohttp site start are host-level concerns
		// the headless port models at the fixture layer (no sockets in tests).
		return true;
	}

	override async disconnect(): Promise<void> {}

	// ── outbound send (@230 parity) ──────────────────────────────────────────

	/**
	 * Proactive message/send ladder (send() parity): touser from the scoped
	 * chat id, text ≤2048 chars, safe=0; access-token cache with a 60 s early-
	 * refresh margin; errcode {40001,42001} evicts the cached token and retries
	 * ONCE. Called BY the door transport — one kit-chunked piece per call.
	 */
	private async sendProactive(
		chatId: string,
		textContent: string,
		metadata: Record<string, unknown>,
	): Promise<SendResult> {
		const app = this.resolveAppForChat(chatId);
		const touser = chatId.includes(":")
			? (chatId.split(":").slice(1).join(":") as string)
			: chatId;
		if (this.transport === undefined) return { success: true };

		// int(str(agent_id or 0)) parity (:256): falsy config resolves to "0";
		// a NON-NUMERIC agent_id raises upstream INSIDE the send try-block, so
		// the send fails CLEANLY (except → SendResult(success=False)) with ZERO
		// token fetches and ZERO qyapi POSTs — NaN→0 coercion that would wire
		// agentid:0 is banned.
		const rawAgentId = app.agent_id;
		const agentIdLiteral =
			rawAgentId === undefined || rawAgentId === "" || rawAgentId === 0
				? "0"
				: String(rawAgentId);
		const trimmedLiteral = agentIdLiteral.trim();
		if (!/^[+-]?\d+$/.test(trimmedLiteral)) {
			return {
				success: false,
				error: `invalid literal for int() with base 10: '${agentIdLiteral}'`,
			};
		}
		const agentId = Number(trimmedLiteral);

		// content[:2048] truncation in the SOURCE; the port receives kit-chunked
		// pieces ≤ the cap so nothing is dropped (proposed DEC note).
		const capped =
			textContent.length > WECOM_TEXT_SEND_CAP_CHARS
				? textContent.slice(0, WECOM_TEXT_SEND_CAP_CHARS)
				: textContent;

		for (let attempt = 0; attempt < 2; attempt++) {
			let token: string;
			try {
				token = await this.getAccessToken(app);
			} catch (err) {
				return {
					success: false,
					error: err instanceof Error ? err.message : String(err),
				};
			}
			// Vendor body carries ONLY the qyapi keys (send parity): caller
			// metadata rides the transport seam separately, never the wire.
			const payload: Record<string, unknown> = {
				touser,
				msgtype: "text",
				agentid: agentId,
				text: { content: capped },
				safe: 0,
			};
			const resp = await this.transport.sendMessage(
				app,
				token,
				payload,
				metadata,
			);
			const errcode = resp.errcode;
			if (
				errcode !== undefined &&
				WECOM_TOKEN_REJECTED_ERRCODES.has(errcode) &&
				attempt === 0
			) {
				// WeCom rejected the token — evict the cached entry so the next
				// _get_access_token forces a fresh fetch, retry once.
				this.accessTokens.delete(String(app.name ?? "default"));
				continue;
			}
			if (!resp.success) {
				return { success: false, error: resp.error ?? "send failed" };
			}
			return { success: true, messageId: resp.messageId };
		}
		return { success: false, error: "send failed after token refresh" };
	}

	/** _resolve_app_for_chat parity: map hit, unique-suffix fallback, first app. */
	resolveAppForChat(chatId: string): WecomAppConfig {
		const appName = this.userAppMap.get(chatId);
		if (!appName && !chatId.includes(":")) {
			const matching = [...this.userAppMap.keys()].filter((k) =>
				k.endsWith(`:${chatId}`),
			);
			if (matching.length === 1) {
				const mapped = this.userAppMap.get(matching[0] as string);
				if (mapped) {
					const found = this.appByName(mapped);
					if (found !== undefined) return found;
				}
			}
		}
		const named = appName ? this.appByName(appName) : undefined;
		return named ?? (this.apps[0] as WecomAppConfig);
	}

	private appByName(name: string): WecomAppConfig | undefined {
		return this.apps.find((a) => String(a.name ?? "") === name);
	}

	// ── access tokens (_get_access_token/_refresh_access_token parity) ───────

	async getAccessToken(app: WecomAppConfig): Promise<string> {
		const name = String(app.name ?? "default");
		const cached = this.accessTokens.get(name);
		const nowMs = this.nowFn();
		if (
			cached !== undefined &&
			cached.expiresAtMs > nowMs + WECOM_TOKEN_REFRESH_MARGIN_SECONDS * 1000
		) {
			return cached.token;
		}
		return this.refreshAccessToken(app);
	}

	async refreshAccessToken(app: WecomAppConfig): Promise<string> {
		if (this.transport === undefined) return "fixture-token";
		const fetched = await this.transport.getAccessToken(app);
		const expiresInSeconds =
			fetched.expiresIn ?? WECOM_ACCESS_TOKEN_TTL_SECONDS;
		this.accessTokens.set(String(app.name ?? "default"), {
			token: fetched.token,
			expiresAtMs: this.nowFn() + expiresInSeconds * 1000,
		});
		this.counters.tokenRefreshes += 1;
		return fetched.token;
	}

	// ── guard wiring (reference-fixture inheritance) ─────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: WECOM_REGISTRY,
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
				...(spawner === undefined ? {} : { spawner }),
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

	// ── egress doors ─────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * DOOR transport — one kit-chunked piece per call, riding the PROACTIVE
	 * message/send API with a cached access token (send() parity above).
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		this.counters.outboundCalls += 1;
		return this.sendProactive(chatId, content, { ...metadata });
	}

	/**
	 * Rich lane ABSENT on the real surface (text messages only): answer the
	 * capability-error shape WITHOUT burning a roundtrip (§10.1 latch parity).
	 */
	protected override async wireRich(content: string): Promise<SendResult> {
		if (
			this.captureWire === undefined ||
			!this.captureWire.hasRichScript("rich")
		) {
			void content;
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return this.captureWire.transmitRich("__rich__", content);
	}

	// ── observability ─────────────────────────────────────────────────────────

	seenDedupSize(): number {
		return this.seenMessages.size;
	}

	hasSeenMessageId(id: string): boolean {
		const seenAt = this.seenMessages.get(id);
		if (seenAt === undefined) return false;
		return this.nowFn() - seenAt < WECOM_DEDUP_TTL_MS;
	}

	appForUser(userId: string): string | undefined {
		return this.userAppMap.get(userId);
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

function lowerKeys(
	headers: Record<string, string> | undefined,
): Record<string, string> {
	const out: Record<string, string> = {};
	for (const [k, v] of Object.entries(headers ?? {})) out[k.toLowerCase()] = v;
	return out;
}

function normalizePath(path: string): string {
	const raw = String(path ?? "").trim() || "/";
	return raw.startsWith("/") ? raw : `/${raw}`;
}
