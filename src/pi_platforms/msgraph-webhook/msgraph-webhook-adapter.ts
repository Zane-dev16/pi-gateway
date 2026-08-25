// pi_platforms/msgraph-webhook/msgraph-webhook-adapter — THE Microsoft Graph
// change-notification webhook adapter, ported from the READ-ONLY Hermes
// built-in gateway/platforms/msgraph_webhook.py onto the kit base. Everything
// policy-shaped is inherited; this module supplies TRANSPORT (the notification
// endpoint surface) and MANIFEST DATA.
//
// Shape (DEC-002 third column — stateless webhook):
//   - capabilities AS DATA: supports_async_delivery=False +
//     interactive_resume=False (see manifest DIVERGENCE note — Hermes inherits
//     base True/True for this adapter; 04 §8 stateless pairing + the log-only
//     send() make False the honest data)
//   - NO draft streaming / NO edits / NO media plane: passive ingestion only
//   - DEC-017 trust boundary as manifest data: NO HMAC scheme exists on this
//     wire — authenticity = constant-time clientState compare + CIDR source
//     allowlist consulted BEFORE body parse (forwarded headers never trusted)
//   - ingress pipeline ports _handle_notification exactly: CIDR gate →
//     defensive in-band handshake echo → body cap (declared-length then
//     actual-bytes, both pre-parse) → JSON shape → per-notification walk
//     (resource filter → clientState → receipt dedupe) → verdict ladder
//     (202 accepted/deduped · 403 all-auth-rejected · 400 otherwise)
//   - subscription lifecycle stays with Graph/operator (06 §8.3): the adapter
//     NEVER creates or renews subscriptions — connect refusal ladder +
//     zero-outbound-call posture are pinned by conformance rows
//
// Layering: imports pi_gateway downward + kit same-layer ONLY; no adapter
// cross-imports.

import { createHash, timingSafeEqual as nodeTimingSafe } from "node:crypto";

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
	MSGRAPH_BODY_CAP,
	MSGRAPH_DEFAULT_PORT,
	MSGRAPH_DEFAULT_WEBHOOK_PATH,
	MSGRAPH_HEALTH_PATH,
	MSGRAPH_MAX_SEEN_RECEIPTS,
	MSGRAPH_PROMPT_RENDER_CAP_CHARS,
	MSGRAPH_TEMPLATE_VALUE_CAP_CHARS,
	MSGRAPH_WEBHOOK_PLUGIN_MANIFEST,
	declareMSGraphTrustBoundary,
	validateMsGraphTrustBoundary,
} from "./manifest.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type { DisableReason } from "../kit/lifecycle-state.js";
import {
	BoundedSeenSet,
	allowlistRequiredButMissing,
	parseCidrAllowlist,
	sourceIpAllowed,
} from "../../pi_gateway/security/trust/index.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const MSGRAPH_REGISTRY: CommandRegistry = [
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
 * Adapter configuration — Hermes reads these from `extra` (msgraph_webhook.py
 * __init__). Key names mirror the source's extra keys verbatim.
 */
export interface MSGraphWebhookConfig {
	/** Bind host; null/undefined ⇒ dual-stack all-interfaces (DEFAULT_HOST=None). */
	host?: string | null | undefined;
	port?: number | undefined;
	webhook_path?: string | undefined;
	health_path?: string | undefined;
	accepted_resources?: readonly string[] | undefined;
	client_state?: string | undefined;
	max_seen_receipts?: number | undefined;
	max_body_bytes?: number | undefined;
	/** CSV string or list — parseCidrAllowlist handles both (parity). */
	allowed_source_cidrs?: string | readonly string[] | null | undefined;
	/** Prompt template with {notification.resource}-style placeholders. */
	prompt?: string | undefined;
}

export interface MSGraphWebhookAdapterOptions {
	config?: MSGraphWebhookConfig | undefined;
	/** Scoped reader over MSGRAPH_* names (fail-closed; DEC-003/009). */
	secretReader?: ScopedSecretReader | undefined;
	nowMs?: (() => number) | undefined;
	scalarMaxUnits?: number | undefined;
	spawner?: TaskSpawner | undefined;
	/**
	 * Conformance-harness egress CAPTURE wire. Production/fixture construction
	 * leaves this unset: send() stays the source's log-only stub and no rich
	 * lane exists. When a subject supplies it, the SAME door records every
	 * user-visible transmission onto the in-memory capture instrument so shared
	 * egress rows can observe chunking/fallback behavior (FakePlatformWire
	 * makes zero network calls — outboundWireCalls stays 0 by construction).
	 */
	captureWire?: MSGraphCaptureWire | undefined;
}

/** Capture seam (subject-supplied; family FakePlatformWire shape). */
export interface MSGraphCaptureWire {
	transmitSend(
		chatId: string,
		content: string,
		metadata: Record<string, unknown>,
	): Promise<SendResult>;
	hasRichScript(opKind: string): boolean;
	transmitRich(chatId: string, content: string): Promise<SendResult>;
}

export type NotificationRecord = Record<string, unknown>;

export interface HandlerResponse {
	status: number;
	contentType?: "application/json" | "text/plain" | undefined;
	body?: string | Record<string, never> | undefined;
}

interface NotificationVerdict {
	accepted: NotificationRecord[];
	duplicates: number;
	authRejected: number;
	otherRejected: number;
}

export class MSGraphWebhookAdapter extends BasePlatformAdapter {
	readonly pluginManifest = MSGRAPH_WEBHOOK_PLUGIN_MANIFEST;
	readonly trustBoundary;

	// ── config (__init__ parity) ──────────────────────────────────────────────
	readonly host: string | null;
	readonly port: number;
	readonly webhookPath: string;
	readonly healthPath: string;
	readonly acceptedResources: readonly string[];
	readonly maxSeenReceipts: number;
	readonly maxBodyBytes: number;
	readonly promptTemplate: string;
	/** Invalid CIDR entries skipped with a warning (_parse_allowed_source_cidrs). */
	readonly cidrWarnings: readonly string[];

	private readonly networks: ReturnType<typeof parseCidrAllowlist>["networks"];
	private readonly clientStateConfig: string | undefined;
	private readonly secretReader: ScopedSecretReader;

	// ── runtime state ─────────────────────────────────────────────────────────
	private readonly seenReceipts: BoundedSeenSet;
	private readonly nowFn: () => number;
	private connectedOnce = false;
	/** Observability: the parse seam must NEVER run after a gate rejection. */
	readonly counters = {
		accepted: 0,
		duplicates: 0,
		authRejected: 0,
		otherRejected: 0,
		cidrDenied: 0,
		parseInvocations: 0,
		outboundWireCalls: 0, // MUST stay zero forever (passive posture)
		validationHandshakes: 0,
	};

	/** Accepted notification events dispatched downstream (row observability). */
	readonly dispatchedEvents: Array<{
		messageId: string;
		text: string;
		internal: true;
	}> = [];
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly resolvedFamilies: string[] = [];

	// Interactive surfaces (kit-owned; shared rows drive them).
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	private readonly cp: EgressChokepoint;
	private readonly captureWire: MSGraphCaptureWire | undefined;
	private allowAllClickers = true;
	private readonly clarifyArmedSet = new Set<string>();
	private holding = false;
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};

	constructor(opts: MSGraphWebhookAdapterOptions = {}) {
		const config = opts.config ?? {};
		super({
			manifestName: MSGRAPH_WEBHOOK_PLUGIN_MANIFEST.name,
			capabilities: MSGRAPH_WEBHOOK_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? 4096,
		});
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.captureWire = opts.captureWire;
		this.host =
			config.host === undefined || config.host === null || config.host === ""
				? null
				: String(config.host);
		this.port = Number(config.port ?? MSGRAPH_DEFAULT_PORT);
		this.webhookPath = MSGraphWebhookAdapter.normalizePath(
			config.webhook_path ?? MSGRAPH_DEFAULT_WEBHOOK_PATH,
		);
		this.healthPath = MSGraphWebhookAdapter.normalizePath(
			config.health_path ?? MSGRAPH_HEALTH_PATH,
		);
		this.acceptedResources = (config.accepted_resources ?? [])
			.map((v) => String(v).trim())
			.filter((v) => v.length > 0);
		this.maxSeenReceipts = Math.max(
			1,
			Number(config.max_seen_receipts ?? MSGRAPH_MAX_SEEN_RECEIPTS),
		);
		this.maxBodyBytes = Math.max(
			1,
			Number(config.max_body_bytes ?? MSGRAPH_BODY_CAP),
		);
		this.promptTemplate = config.prompt ?? "";

		// client_state resolution: extra config first (Hermes parity), scoped
		// secret second (port surface — MSGRAPH_CLIENT_STATE).
		const fromConfig =
			typeof config.client_state === "string" && config.client_state.trim()
				? config.client_state.trim()
				: undefined;
		const fromSecret = this.secretReader("MSGRAPH_CLIENT_STATE");
		this.clientStateConfig = fromConfig ?? fromSecret;

		// CIDR allowlist (trust engine parity of _parse_allowed_source_cidrs:
		// invalid entries SKIPPED with a warning, empty list ⇒ loopback-only
		// bind may omit them entirely).
		const parsed = parseCidrAllowlist(config.allowed_source_cidrs ?? null);
		this.networks = parsed.networks;
		this.cidrWarnings = parsed.invalid;
		const rawCidrs = normalizeCidrList(config.allowed_source_cidrs ?? null);
		this.trustBoundary = declareMSGraphTrustBoundary({
			allowedSourceCidrs: rawCidrs.valid,
		});
		for (const invalid of rawCidrs.invalid) {
			this.logger?.warn?.(
				`[msgraph_webhook] Ignoring invalid allowed_source_cidrs entry: ${JSON.stringify(invalid)}`,
			);
		}

		// Receipt dedupe: pure FIFO bound, NO TTL (msgraph_webhook.py
		// _remember_receipt is a set + insertion-order deque; BoundedSeenSet
		// ttlMs:null is exactly that shape).
		this.seenReceipts = new BoundedSeenSet({
			maxEntries: this.maxSeenReceipts,
			ttlMs: null,
			nowMs: this.nowFn,
		});

		// DEC-017: an incomplete trust boundary is a CONSTRUCTION-TIME error.
		const boundaryErrors = validateMsGraphTrustBoundary(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			const reason: DisableReason = {
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			};
			this.lifecycle.disable(reason);
		}

		// §11 step 3/4: missing required secret ⇒ LOUD disable (status-visible).
		// Hermes refuses at connect() ("Refusing to start without
		// extra.client_state configured"); the kit expresses the same posture at
		// construction so /status shows the reason instead of a silent skip.
		const enablement = resolveEnablement(
			MSGRAPH_WEBHOOK_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}
		if (!this.clientStateConfig && !enablement.reason) {
			this.lifecycle.disable({
				kind: "secret_missing",
				secretKey: "MSGRAPH_CLIENT_STATE",
				manifestName: MSGRAPH_WEBHOOK_PLUGIN_MANIFEST.name,
			});
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // passive ingestion; no native lanes
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

	get clientState(): string | undefined {
		return this.clientStateConfig;
	}

	/**
	 * Per-chat length descriptor (§6.3/A15 relay-shaped override point): the
	 * harness's utf16-marked chats return budget AND unit TOGETHER; production
	 * chats return undefined ⇒ manifest default.
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

	/** _normalize_path parity: empty ⇒ "/", leading slash added when missing. */
	static normalizePath(path: string): string {
		const raw = String(path ?? "").trim() || "/";
		return raw.startsWith("/") ? raw : `/${raw}`;
	}

	// ── connect ladder (@~160 parity) ─────────────────────────────────────────

	/**
	 * Refuses to start without extra.client_state configured, and refuses
	 * network-accessible binds without allowed_source_cidrs (guidance message).
	 * PASSIVE posture: connecting starts NO subscription machinery — zero
	 * outbound calls, ever.
	 */
	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (!this.clientStateConfig) {
			this.logger?.error?.(
				"[msgraph_webhook] Refusing to start without extra.client_state configured",
			);
			return false;
		}
		if (allowlistRequiredButMissing(this.host, this.networks)) {
			this.logger?.error?.(
				`[msgraph_webhook] Refusing to start: binding to ${String(this.host)} requires ` +
					"extra.allowed_source_cidrs. Configure the Microsoft Graph source CIDRs or bind to loopback (127.0.0.1/::1) behind a tunnel or reverse proxy.",
			);
			return false;
		}
		this.connectedOnce = true;
		return true;
	}

	override async disconnect(): Promise<void> {
		this.connectedOnce = false;
	}

	get isConnected(): boolean {
		return this.connectedOnce;
	}

	// ── GET validation handshake (_handle_validation @~230 parity) ────────────

	/**
	 * Graph validates a subscription endpoint by sending a GET with
	 * `validationToken` in the query string; the service must echo the token
	 * VERBATIM as text/plain within 10 seconds. Anything else (bare GET, missing
	 * token) is rejected so the endpoint can't be enumerated or mistakenly used
	 * for data exfiltration.
	 */
	handleValidationGet(
		query: Record<string, string>,
		peer: string,
	): HandlerResponse {
		if (!this.admitPeer(peer)) return { status: 403 };
		const token = query["validationToken"] ?? "";
		if (!token) return { status: 400 };
		this.counters.validationHandshakes += 1;
		return { status: 200, contentType: "text/plain", body: token };
	}

	handleHealthGet(peer: string): HandlerResponse {
		if (!this.admitPeer(peer)) return { status: 403 };
		return {
			status: 200,
			contentType: "application/json",
			body: JSON.stringify({
				status: "ok",
				platform: MSGRAPH_WEBHOOK_PLUGIN_MANIFEST.name,
				webhook_path: this.webhookPath,
				accepted: this.counters.accepted,
				duplicates: this.counters.duplicates,
			}),
		};
	}

	// ── POST notifications (_handle_notification @~250 parity) ───────────────

	/**
	 * Order ports the source exactly: CIDR gate (BEFORE body read/parse) →
	 * defensive in-band handshake echo → declared-length cap → read →
	 * actual-bytes cap → JSON shape checks (`dict` body, `value` list) →
	 * per-notification walk → verdict ladder.
	 */
	async handleNotificationPost(input: {
		query?: Record<string, string> | undefined;
		headers?: Record<string, string> | undefined;
		rawBody: Buffer;
		peer: string;
	}): Promise<HandlerResponse> {
		const query = input.query ?? {};
		const headers = normalizeHeaders(input.headers);

		if (!this.admitPeer(input.peer)) {
			this.counters.cidrDenied += 1;
			return { status: 403 };
		}

		// Graph never sends validationToken on POST, but tolerate it for
		// defensive clients that replay the handshake in-band.
		const validationToken = query["validationToken"] ?? "";
		if (validationToken) {
			return { status: 200, contentType: "text/plain", body: validationToken };
		}

		// Body cap at BOTH gates (declared Content-Length, then actual bytes) —
		// oversized bodies are rejected WITHOUT reaching the parse seam.
		const declaredRaw = headers["content-length"];
		const declaredLength =
			declaredRaw !== undefined && /^\d+$/.test(declaredRaw)
				? Number(declaredRaw)
				: null;
		if (declaredLength !== null && declaredLength > this.maxBodyBytes) {
			return { status: 413 };
		}
		if (input.rawBody.length > this.maxBodyBytes) {
			return { status: 413 };
		}

		const parsed = this.parseJsonBody(input.rawBody);
		if (!parsed.ok) {
			return { status: 400 };
		}
		const payload = parsed.value;
		if (
			payload === null ||
			typeof payload !== "object" ||
			Array.isArray(payload)
		) {
			return { status: 400 };
		}
		const value = (payload as Record<string, unknown>)["value"];
		if (!Array.isArray(value)) {
			return { status: 400 };
		}

		const verdict = await this.walkNotifications(value);
		this.counters.accepted += verdict.accepted.length;
		this.counters.duplicates += verdict.duplicates;
		this.counters.authRejected += verdict.authRejected;
		this.counters.otherRejected += verdict.otherRejected;

		// Verdict ladder (@~270): if anything ingested OR deduped, 202 with an
		// empty body so Graph acks and we don't leak internal counters. If every
		// item failed auth, 403 so an attacker POSTing fake notifications gets a
		// clear reject. Other failures are the sender's configuration problem ⇒
		// 400.
		for (const notification of verdict.accepted) {
			await this.dispatchNotification(notification);
		}
		if (verdict.accepted.length > 0 || verdict.duplicates > 0) {
			return { status: 202, contentType: "application/json", body: {} };
		}
		if (verdict.authRejected > 0 && verdict.otherRejected === 0) {
			return { status: 403 };
		}
		return { status: 400 };
	}

	private async walkNotifications(
		value: unknown[],
	): Promise<NotificationVerdict> {
		const verdict: NotificationVerdict = {
			accepted: [],
			duplicates: 0,
			authRejected: 0,
			otherRejected: 0,
		};
		for (const raw of value) {
			if (raw === null || typeof raw !== "object" || Array.isArray(raw)) {
				verdict.otherRejected += 1;
				continue;
			}
			const notification = raw as NotificationRecord;
			if (!this.resourceAccepted(stringOf(notification["resource"]))) {
				verdict.otherRejected += 1;
				continue;
			}
			if (!this.verifyClientState(notification)) {
				// Treat bad clientState as an auth failure: if the whole batch is
				// forged we want to signal 403 so the sender stops retrying.
				verdict.authRejected += 1;
				continue;
			}
			const receiptKey = buildReceiptKey(notification);
			if (receiptKey !== null && !this.seenReceipts.add(receiptKey)) {
				verdict.duplicates += 1;
				continue;
			}
			verdict.accepted.push(notification);
		}
		return verdict;
	}

	/**
	 * Per-request admission (_source_ip_allowed parity via the trust engine):
	 * required-but-missing fails closed; empty allowlist on a loopback-only
	 * bind admits; otherwise the SOCKET PEER decides — forwarded headers are
	 * not part of the request surface and can never move the verdict.
	 */
	private admitPeer(peer: string): boolean {
		return sourceIpAllowed({ remoteAddr: peer }, this.host, this.networks);
	}

	// ── resource filter (_resource_accepted @~330 parity) ─────────────────────

	resourceAccepted(resource: string): boolean {
		if (this.acceptedResources.length === 0) return true;
		const normalizedResource = normalizeResource(resource);
		for (const pattern of this.acceptedResources) {
			const normalizedPattern = normalizeResource(pattern);
			if (!normalizedPattern) continue;
			if (normalizedPattern.endsWith("*")) {
				const prefix = normalizedPattern.slice(0, -1).replace(/\/+$/, "");
				if (
					normalizedResource === prefix ||
					normalizedResource.startsWith(`${prefix}/`)
				) {
					return true;
				}
				continue;
			}
			if (
				normalizedResource === normalizedPattern ||
				normalizedResource.startsWith(`${normalizedPattern}/`)
			) {
				return true;
			}
		}
		return false;
	}

	/**
	 * clientState echo verification (_verify_client_state parity): constant-time
	 * byte compare (kit secureCompare over UTF-8 bytes); expected unset or
	 * provided missing ⇒ reject.
	 */
	verifyClientState(notification: NotificationRecord): boolean {
		const expected = this.clientStateConfig;
		if (expected === undefined) return false;
		const provided = stringOrNone(notification["clientState"]);
		if (provided === null) return false;
		const a = Buffer.from(provided, "utf8");
		const b = Buffer.from(expected, "utf8");
		if (a.length !== b.length) return false;
		return timingSafeEqual(a, b);
	}

	// ── event construction (_build_message_event + _render_prompt parity) ────

	buildMessageId(notification: NotificationRecord): string {
		const explicit = buildReceiptKey(notification);
		if (explicit !== null) return explicit;
		return `sha1:${createHash("sha1")
			.update(canonicalJson(notification))
			.digest("hex")}`;
	}

	renderPrompt(notification: NotificationRecord): string {
		if (this.promptTemplate) {
			return renderTemplate(this.promptTemplate, {
				notification,
				resource: notification["resource"] ?? "",
				change_type: notification["changeType"] ?? "",
				subscription_id: notification["subscriptionId"] ?? "",
			});
		}
		const rendered = stablePrettyJson(notification).slice(
			0,
			MSGRAPH_PROMPT_RENDER_CAP_CHARS,
		);
		return `Microsoft Graph change notification:\n\n\`\`\`json\n${rendered}\n\`\`\``;
	}

	private async dispatchNotification(
		notification: NotificationRecord,
	): Promise<void> {
		const messageId = this.buildMessageId(notification);
		const text = this.renderPrompt(notification);
		this.dispatchedEvents.push({ messageId, text, internal: true });

		const subscriptionId =
			stringOf(notification["subscriptionId"]) || "unknown";
		const event: IncomingEvent = {
			messageType: "text",
			text,
			internal: true,
			messageId,
			source: {
				platform: MSGRAPH_WEBHOOK_PLUGIN_MANIFEST.name,
				chatType: "webhook",
				userId: "msgraph",
				chatId: `msgraph:${subscriptionId}`,
				chatName: "msgraph/webhook",
			},
		};
		const sessionKey = `msgraph:${subscriptionId}`;
		try {
			await this.deliverInbound(event, sessionKey);
		} catch {
			/* containment parity: one poisoned notification never rejects the batch */
		}
	}

	// ── guard wiring (reference-fixture inheritance) ──────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: MSGRAPH_REGISTRY,
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

	// ── egress doors ──────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * send() parity (msgraph_webhook.py send @~200): responses have nowhere to
	 * go — LOG ONLY, success:true. There is deliberately NO transport behind
	 * this door: any outbound call would break the passive-ingress posture
	 * (outboundWireCalls stays zero — the capture wire is in-memory observation,
	 * not a transport). When a conformance subject supplies a capture wire, the
	 * SAME log-only door records the rendered response there so shared egress
	 * rows can observe chunk/fallback behavior.
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		this.logger?.info?.(
			`[msgraph_webhook] Response for ${chatId}: ${content.slice(0, 200)}`,
		);
		if (this.captureWire !== undefined) {
			return this.captureWire.transmitSend(chatId, content, metadata);
		}
		return { success: true };
	}

	/**
	 * Rich lane ABSENT on the real surface (passive ingestion): unless a
	 * capture wire scripted a rich probe, answer the capability-error shape
	 * WITHOUT burning a roundtrip (§10.1 latch path probes once then never
	 * again — webhook reference adapter parity).
	 */
	protected override async wireRich(content: string): Promise<SendResult> {
		if (
			this.captureWire === undefined ||
			!this.captureWire.hasRichScript("rich")
		) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return this.captureWire.transmitRich("__rich__", content);
	}

	/**
	 * THE parse seam — the ONLY place a request body is decoded. Admission
	 * gates (CIDR, handshake echo, body caps) run strictly BEFORE this method,
	 * so `parseInvocations` staying at zero after a rejection is an OBSERVABLE
	 * contract ("rejected before body parse" per 06 §8.3). Malformed input is
	 * a RESULT, never a throw: the verdict ladder maps it to 400.
	 */
	private parseJsonBody(
		rawBody: Buffer,
	): { ok: true; value: unknown } | { ok: false } {
		this.counters.parseInvocations += 1;
		try {
			return { ok: true, value: JSON.parse(rawBody.toString("utf8")) };
		} catch {
			return { ok: false };
		}
	}

	/** Receipt-set observability (dedupe rows probe the live bound). */
	seenReceiptCount(): number {
		return this.seenReceipts.size();
	}
	hasSeenReceipt(receiptKey: string): boolean {
		return this.seenReceipts.has(receiptKey);
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

function normalizeHeaders(
	headers: Record<string, string> | undefined,
): Record<string, string> {
	const out: Record<string, string> = {};
	for (const [k, v] of Object.entries(headers ?? {})) out[k.toLowerCase()] = v;
	return out;
}

function normalizeCidrList(
	raw: string | readonly string[] | null | undefined,
): { valid: string[]; invalid: string[] } {
	if (raw === null || raw === undefined) return { valid: [], invalid: [] };
	const candidates =
		typeof raw === "string"
			? raw.split(",").map((c) => c.trim())
			: raw.map((c) => String(c).trim());
	const valid: string[] = [];
	const invalid: string[] = [];
	for (const c of candidates) {
		if (!c) continue;
		if (parseCidrAllowlist(c).networks.length > 0) valid.push(c);
		else invalid.push(c);
	}
	return { valid, invalid };
}

function stringOf(v: unknown): string {
	return typeof v === "string" ? v : "";
}

function stringOrNone(v: unknown): string | null {
	if (v === null || v === undefined) return null;
	const text = String(v).trim();
	return text.length > 0 ? text : null;
}

function normalizeResource(resource: string): string {
	return String(resource ?? "")
		.trim()
		.replace(/^\/+|\/+$/g, "");
}

/** msgraph_webhook.py:_build_receipt_key — `id:<id>` when id non-empty, else None. */
function buildReceiptKey(notification: NotificationRecord): string | null {
	const explicitId = stringOf(notification["id"]).trim();
	if (explicitId) return `id:${explicitId}`;
	return null;
}

/**
 * Constant-time byte compare (hmac.compare_digest parity): unequal lengths
 * fail WITHOUT content comparison (node timingSafeEqual throws on length
 * mismatch), equal lengths compare via the node primitive.
 */
function timingSafeEqual(a: Buffer, b: Buffer): boolean {
	if (a.length !== b.length) return false;
	return nodeTimingSafe(a, b);
}

/**
 * Canonical JSON for the sha1 fallback id. Python anchor:
 * sha1(json.dumps(notification, sort_keys=True)). The port canonicalizes with
 * sorted keys and compact separators — deterministic within the port (ids
 * never cross the language boundary), which is the property the dedupe needs.
 */
function canonicalJson(value: unknown): string {
	return JSON.stringify(sortKeysDeep(value));
}

function sortKeysDeep(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(sortKeysDeep);
	if (value !== null && typeof value === "object") {
		const out: Record<string, unknown> = {};
		for (const k of Object.keys(value as Record<string, unknown>).sort()) {
			out[k] = sortKeysDeep((value as Record<string, unknown>)[k]);
		}
		return out;
	}
	return value;
}

/** json.dumps(value, indent=2, sort_keys=True) shape for the default render. */
function stablePrettyJson(value: unknown): string {
	const render = (v: unknown, indent: number): string => {
		const pad = "  ".repeat(indent);
		const padInner = "  ".repeat(indent + 1);
		if (Array.isArray(v)) {
			if (v.length === 0) return "[]";
			const items = v.map((item) => `${padInner}${render(item, indent + 1)}`);
			return `[\n${items.join(",\n")}\n${pad}]`;
		}
		if (v !== null && typeof v === "object") {
			const rec = v as Record<string, unknown>;
			const keys = Object.keys(rec).sort();
			if (keys.length === 0) return "{}";
			const entries = keys.map(
				(k) => `${padInner}${JSON.stringify(k)}: ${render(rec[k], indent + 1)}`,
			);
			return `{\n${entries.join(",\n")}\n${pad}}`;
		}
		return JSON.stringify(v) ?? "null";
	};
	return render(value, 0);
}

/**
 * Template interpolation (_render_template parity): `{a.b.c}` placeholders
 * resolve against the payload dict-path-wise; dict/list values render as
 * stable JSON capped at 2000 chars; unresolved keys keep the literal
 * `{a.b.c}` text.
 */
function renderTemplate(
	template: string,
	payload: Record<string, unknown>,
): string {
	return template.replace(/\{([a-zA-Z0-9_.]+)\}/g, (_m, key: string) => {
		let value: unknown = payload;
		for (const part of key.split(".")) {
			if (
				value !== null &&
				typeof value === "object" &&
				!Array.isArray(value)
			) {
				value = (value as Record<string, unknown>)[part];
			} else {
				return `{${key}}`;
			}
		}
		if (value !== null && typeof value === "object") {
			return JSON.stringify(sortKeysDeep(value)).slice(
				0,
				MSGRAPH_TEMPLATE_VALUE_CAP_CHARS,
			);
		}
		return String(value);
	});
}
