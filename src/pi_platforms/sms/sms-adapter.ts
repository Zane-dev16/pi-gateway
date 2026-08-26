// pi_platforms/sms/sms-adapter — THE SMS (Twilio) platform adapter, ported
// from the READ-ONLY Hermes plugins/platforms/sms/adapter.py onto the kit
// base. Everything policy-shaped is inherited; this module supplies TRANSPORT
// (the Twilio inbound webhook surface + the outbound Messages.json REST lane)
// and MANIFEST DATA.
//
// Shape (DEC-002 third column — stateless webhook):
//   - capabilities AS DATA: supports_async_delivery=False +
//     interactive_resume=False (manifest DIVERGENCE note — Hermes inherits
//     base True/True; the fast-TwiML-ack/out-of-band-REST shape makes False
//     the honest data)
//   - NO draft streaming / NO edits: the Twilio REST Messages API has no edit
//     endpoint in the source, so a draft cursor could never be sealed or
//     edited away (supportsDraftStreaming false BY PROBE; lie-scan flips it)
//   - DEC-017 trust boundary as manifest data: X-Twilio-Signature =
//     base64(HMAC-SHA1(authToken, url + concat(sorted param key+values))),
//     validated CONSTANT-TIME via kit secureCompare (_check_signature parity),
//     with the default-port VARIANT fallback (_port_variant_url toggles 443/80
//     only — non-standard ports untouched)
//   - ingress ports _handle_webhook exactly: declared Content-Length cap →
//     actual-bytes cap (both 413 BEFORE parse) → form parse (parse_qs
//     keep_blank_values semantics, first value wins) → X-Twilio-Signature gate
//     (only when the public webhook URL is configured) → From+Body required →
//     own-number echo prevention → NON-blocking dispatch → ALWAYS empty TwiML
//   - connect refusal ladder ports the source exactly: missing phone number ⇒
//     FATAL sms_missing_phone_number; missing webhook URL without the insecure
//     flag ⇒ FATAL sms_missing_webhook_url; insecure mode logs the
//     DISABLED-validation warning and runs unsigned
//
// Layering: imports pi_gateway downward + kit same-layer ONLY; no adapter
// cross-imports.

import { createHmac } from "node:crypto";

import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	chunkWithFenceCarry,
	secureCompare,
	resolveEnablement,
} from "../kit/index.js";
import { PLAIN_TEXT_FALLBACK_PREFIX } from "../kit/send-retry.js";
import type {
	Metadata,
	SendResult,
	StreamLogger,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type { DisableReason } from "../kit/lifecycle-state.js";
import {
	DEFAULT_WEBHOOK_HOST,
	DEFAULT_WEBHOOK_PORT,
	MAX_SMS_LENGTH,
	SMS_HEALTH_PATH,
	SMS_PLUGIN_MANIFEST,
	SMS_WEBHOOK_PATH,
	TWILIO_API_BASE,
	TWILIO_WEBHOOK_MAX_BODY_BYTES,
	declareSmsTrustBoundary,
	validateSmsTrustBoundary,
} from "./manifest.js";

/**
 * The one command registry (07 §1 derivation — mirrors the reference set).
 */
export const SMS_REGISTRY: CommandRegistry = [
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

/** The empty-TwiML body EVERY webhook response carries (source literal). */
export const EMPTY_TWIML =
	'<?xml version="1.0" encoding="UTF-8"?><Response></Response>';

/** HTTP handler response surface (web.Response parity). */
export interface SmsHttpResponse {
	status: number;
	contentType?: "application/xml" | "text/plain" | undefined;
	body?: string | undefined;
}

function twiml(status: number): SmsHttpResponse {
	return { status, contentType: "application/xml", body: EMPTY_TWIML };
}

/**
 * Adapter configuration — Hermes reads these from env (sms/adapter.py
 * __init__). Key names mirror the source env names snake-cased.
 */
export interface SmsAdapterConfig {
	/** TWILIO_ACCOUNT_SID (REST basic-auth user). */
	account_sid?: string | undefined;
	/** TWILIO_AUTH_TOKEN — ALSO the HMAC-SHA1 signature key. */
	auth_token?: string | undefined;
	/** TWILIO_PHONE_NUMBER — E.164 from-number (connect refusal when empty). */
	phone_number?: string | undefined;
	/** SMS_WEBHOOK_PORT (default 8080). */
	port?: number | undefined;
	/** SMS_WEBHOOK_HOST (default 127.0.0.1). */
	host?: string | undefined;
	/** SMS_WEBHOOK_URL — public URL Twilio signs against (required). */
	webhook_url?: string | undefined;
	/** SMS_INSECURE_NO_SIGNATURE=true — dev-only validation disable. */
	insecure_no_signature?: boolean | undefined;
	max_body_bytes?: number | undefined;
}

/** Outbound REST edge: ONE Messages.json POST per chunk (send() loop parity). */
export interface SmsRestTransport {
	postMessages(input: {
		/**
		 * Composed REST URL — adapter.py:194 parity:
		 * `{TWILIO_API_BASE}/{account_sid}/Messages.json`.
		 */
		url: string;
		/**
		 * Authorization header value — adapter.py:_basic_auth_header parity:
		 * `Basic base64(account_sid:auth_token)`.
		 */
		authorization: string;
		from: string;
		to: string;
		body: string;
		/**
		 * Capture-seam control metadata (forceFormattingError script et al) —
		 * NEVER transmitted; only From/To/Body hit the wire form.
		 */
		metadata: Metadata;
	}): Promise<SmsRestResponse>;
}

export interface SmsRestResponse {
	status: number;
	json: Record<string, unknown>;
	retryable?: boolean | undefined;
	retryAfter?: number | undefined;
}

/** Rich-probe scripting seam (§10.1 latch rows; no native rich lane exists). */
export interface SmsRichProbe {
	hasRichScript(opKind: string): boolean;
	transmitRich(chatId: string, content: string): Promise<SendResult>;
}

export interface SmsAdapterOptions {
	config?: SmsAdapterConfig | undefined;
	/** Scoped reader over TWILIO_* names (fail-closed; DEC-003/009). */
	secretReader?: ScopedSecretReader | undefined;
	nowMs?: (() => number) | undefined;
	scalarMaxUnits?: number | undefined;
	spawner?: TaskSpawner | undefined;
	/** Injected logger — the warning/error ladders are OBSERVABLE contracts. */
	logger?: StreamLogger | undefined;
	/** Outbound Messages.json seam (production fetch impl; fixtures bind fakes). */
	rest?: SmsRestTransport | undefined;
	/** Rich-probe scripting seam (conformance subject supplies it). */
	richProbe?: SmsRichProbe | undefined;
	/**
	 * Lie-scan fixture seam ONLY: flips THE probe datum behind
	 * supportsDraftStreaming() so the negative gate can prove a lying
	 * capability claim FAILS the streaming family rows. Never set in production.
	 */
	declaredMessageEditing?: boolean | undefined;
}

// ── pure Twilio signature functions (adapter.py:@~215 parity) ────────────────

/**
 * sms/adapter.py:_check_signature — data-to-sign is the URL followed by each
 * param's key+value concatenated in sorted-key order; HMAC-SHA1 keyed by the
 * auth token, base64-encoded.
 */
export function signTwilioParams(
	authToken: string,
	url: string,
	params: Record<string, string>,
): string {
	let dataToSign = url;
	for (const key of Object.keys(params).sort()) {
		dataToSign += key + params[key];
	}
	return createHmac("sha1", authToken)
		.update(dataToSign, "utf8")
		.digest("base64");
}

/** One signature compare — CONSTANT-TIME via kit secureCompare. */
export function checkTwilioSignature(
	authToken: string,
	url: string,
	params: Record<string, string>,
	signature: string,
): boolean {
	return secureCompare(signTwilioParams(authToken, url, params), signature);
}

/**
 * sms/adapter.py:_basic_auth_header — HTTP Basic credentials for the Twilio
 * REST edge: `Basic base64("{account_sid}:{auth_token}")` (ASCII).
 */
export function twilioBasicAuthHeader(
	accountSid: string,
	authToken: string,
): string {
	const creds = `${accountSid}:${authToken}`;
	return `Basic ${Buffer.from(creds, "ascii").toString("base64")}`;
}

/**
 * sms/adapter.py:_port_variant_url — the URL with the DEFAULT port toggled,
 * or null. Only default ports toggle (443 https / 80 http); non-standard
 * ports are never modified.
 *
 * NOTE: the authority is parsed MANUALLY — WHATWG URL normalizes default
 * ports AWAY (`new URL("https://x:443/p").port === ""`), while the source's
 * urllib.parse.urlparse keeps ":443" EXPLICIT and both toggle branches
 * depend on exactly that distinction.
 */
export function twilioPortVariantUrl(url: string): string | null {
	const parsed = /^([a-zA-Z][a-zA-Z0-9+.-]*):\/\/([^/?#]*)([/?#].*)?$/.exec(
		url,
	);
	if (parsed === null) return null;
	const scheme = (parsed[1] ?? "").toLowerCase();
	const defaultPort = scheme === "https" ? 443 : scheme === "http" ? 80 : null;
	if (defaultPort === null) return null;
	const authority = parsed[2] ?? "";
	const rest = parsed[3] ?? "";
	// Split [userinfo@]host[:port] (bracketed IPv6 literals respected).
	const bracketEnd = authority.indexOf("]");
	const atSplit =
		bracketEnd === -1
			? authority.indexOf("@")
			: authority.indexOf("@", bracketEnd);
	const userinfo = atSplit === -1 ? "" : authority.slice(0, atSplit + 1);
	const hostPart = atSplit === -1 ? authority : authority.slice(atSplit + 1);
	let host = hostPart;
	let port: string | null = null;
	if (hostPart.startsWith("[") && hostPart.endsWith("]")) {
		host = hostPart; // bare IPv6 literal — no port segment
	} else if (hostPart.startsWith("[")) {
		const close = hostPart.indexOf("]");
		host = hostPart.slice(0, close + 1);
		const after = hostPart.slice(close + 1);
		port = after.startsWith(":") ? after.slice(1) : null;
	} else {
		const colon = hostPart.indexOf(":");
		if (colon !== -1) {
			host = hostPart.slice(0, colon);
			port = hostPart.slice(colon + 1);
		}
	}
	if (!host) return null;
	if (
		port !== null &&
		port !== "" &&
		/^\d+$/.test(port) &&
		Number(port) === defaultPort
	) {
		return `${scheme}://${userinfo}${host}${rest}`; // explicit default port → strip
	}
	if (port === null || port === "") {
		// No port → add the scheme default.
		return `${scheme}://${userinfo}${host}:${String(defaultPort)}${rest}`;
	}
	return null; // non-standard port — no variant
}

/**
 * sms/adapter.py:_validate_twilio_signature — tries the configured URL, then
 * the default-port variant (Twilio may sign either form).
 */
export function verifyTwilioSignature(opts: {
	authToken: string;
	url: string;
	params: Record<string, string>;
	signature: string;
}): boolean {
	if (
		checkTwilioSignature(opts.authToken, opts.url, opts.params, opts.signature)
	) {
		return true;
	}
	const variant = twilioPortVariantUrl(opts.url);
	if (
		variant !== null &&
		checkTwilioSignature(opts.authToken, variant, opts.params, opts.signature)
	) {
		return true;
	}
	return false;
}

// ── strip_markdown port (gateway/platforms/helpers.py:196 parity) ────────────

// helpers.py:185-193 — the EXACT regex ladder, DOTALL/MULTILINE mapped to the
// s/m flags. Order matters: bold before italic-star, bold-under before
// italic-under, fences before inline code.
const RE_BOLD = /\*\*(.+?)\*\*/gs;
const RE_ITALIC_STAR = /\*(.+?)\*/gs;
const RE_BOLD_UNDER = /\b__(?![\s_])(.+?)(?<![\s_])__\b/gs;
const RE_ITALIC_UNDER = /\b_(?![\s_])(.+?)(?<![\s_])_\b/gs;
const RE_CODE_BLOCK = /```[a-zA-Z0-9_+-]*\n?/g;
const RE_INLINE_CODE = /`(.+?)`/g;
const RE_HEADING = /^#{1,6}\s+/gm;
const RE_LINK = /\[([^\]]+)\]\([^)]+\)/g;
const RE_MULTI_NEWLINE = /\n{3,}/g;

/**
 * helpers.py:strip_markdown — SMS renders markdown as literal characters.
 * Shared by iMessage-class plain-text platforms in the source; the plugin's
 * local _strip_markdown_for_sms duplicate is legacy glue (helpers wins).
 */
export function stripMarkdownForSms(text: string): string {
	let out = text;
	out = out.replace(RE_BOLD, "$1");
	out = out.replace(RE_ITALIC_STAR, "$1");
	out = out.replace(RE_BOLD_UNDER, "$1");
	out = out.replace(RE_ITALIC_UNDER, "$1");
	out = out.replace(RE_CODE_BLOCK, "");
	out = out.replace(RE_INLINE_CODE, "$1");
	out = out.replace(RE_HEADING, "");
	out = out.replace(RE_LINK, "$1");
	out = out.replace(RE_MULTI_NEWLINE, "\n\n");
	return out.trim();
}

// ── the adapter ───────────────────────────────────────────────────────────────

interface ParsedForm {
	fields: Record<string, string[]>;
}

export class SmsAdapter extends BasePlatformAdapter {
	readonly pluginManifest = SMS_PLUGIN_MANIFEST;
	readonly trustBoundary = declareSmsTrustBoundary();

	// ── config (__init__ parity) ──────────────────────────────────────────────
	readonly accountSid: string;
	readonly authToken: string;
	readonly fromNumber: string;
	readonly port: number;
	readonly host: string;
	readonly webhookUrl: string;
	readonly insecureNoSignature: boolean;
	readonly maxBodyBytes: number;

	private readonly secretReader: ScopedSecretReader;
	private readonly rest: SmsRestTransport | undefined;
	private readonly richProbe: SmsRichProbe | undefined;
	private readonly declaredMessageEditing: boolean;
	private connectedOnce = false;

	/** Observability: the parse seam must NEVER run after a gate rejection. */
	readonly counters = {
		dispatched: 0,
		echoIgnored: 0,
		missingFieldsIgnored: 0,
		oversizedRejected: 0,
		parseErrors: 0,
		missingSignatureRejected: 0,
		invalidSignatureRejected: 0,
		parseInvocations: 0,
	};

	/** Dispatched inbound events (row observability). */
	readonly dispatchedEvents: Array<{
		messageId: string;
		text: string;
		from: string;
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
	private allowAllClickers = true;
	private readonly clarifyArmedSet = new Set<string>();
	private holding = false;
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};

	/** Non-blocking dispatch tracking (asyncio.create_task parity). */
	private readonly pendingDispatches = new Set<Promise<void>>();

	constructor(opts: SmsAdapterOptions = {}) {
		const config = opts.config ?? {};
		super({
			manifestName: SMS_PLUGIN_MANIFEST.name,
			capabilities: SMS_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? MAX_SMS_LENGTH,
			...(opts.logger !== undefined ? { logger: opts.logger } : {}),
		});
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.rest = opts.rest;
		this.richProbe = opts.richProbe;
		this.declaredMessageEditing = opts.declaredMessageEditing ?? false;

		this.accountSid = this.resolveCredential(
			config.account_sid,
			"TWILIO_ACCOUNT_SID",
		);
		this.authToken = this.resolveCredential(
			config.auth_token,
			"TWILIO_AUTH_TOKEN",
		);
		this.fromNumber = this.resolveCredential(
			config.phone_number,
			"TWILIO_PHONE_NUMBER",
		);
		this.port = this.resolvePort(config.port);
		this.host = this.resolveEnvValue(
			config.host,
			"SMS_WEBHOOK_HOST",
			DEFAULT_WEBHOOK_HOST,
		);
		this.webhookUrl = (
			config.webhook_url ??
			this.secretReader("SMS_WEBHOOK_URL") ??
			""
		).trim();
		// adapter.py:95-100/:113 parity: SMS_INSECURE_NO_SIGNATURE is read from
		// the environment (`os.getenv(...).lower() == "true"` — no strip);
		// the config object is the explicit override. Env-only deployments
		// reach insecure mode.
		this.insecureNoSignature =
			config.insecure_no_signature ??
			this.secretReader("SMS_INSECURE_NO_SIGNATURE")?.toLowerCase() === "true";
		this.maxBodyBytes = Math.max(
			1,
			Number(config.max_body_bytes ?? TWILIO_WEBHOOK_MAX_BODY_BYTES),
		);

		// DEC-033 hygiene: credential VALUES registered for log redaction.
		if (this.authToken) this.registerLogSecret(this.authToken);
		if (this.accountSid) this.registerLogSecret(this.accountSid);

		// DEC-017: an incomplete trust boundary is a CONSTRUCTION-TIME error.
		const boundaryErrors = validateSmsTrustBoundary(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			const reason: DisableReason = {
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			};
			this.lifecycle.disable(reason);
		}

		// §11 step 3/4: missing required secret ⇒ LOUD disable at construction
		// (resolveEnablement parity of check_sms_requirements: SID+TOKEN gate
		// enablement; the PHONE NUMBER refusal stays in the connect ladder
		// where the source emits its named fatal).
		const enablement = resolveEnablement(
			SMS_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // stateless SMS; no native lanes
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

	/** Config first, scoped secret second (msgraph clientState resolution shape). */
	private resolveCredential(
		fromConfig: string | undefined,
		envName: string,
	): string {
		if (typeof fromConfig === "string" && fromConfig.trim()) {
			return fromConfig.trim();
		}
		return this.secretReader(envName)?.trim() ?? "";
	}

	/**
	 * adapter.py:__init__ os.getenv parity for STRING settings: config object
	 * first, scoped env read second, documented default last — an env-only
	 * deployment resolves identically to the source's direct getenv reads.
	 */
	private resolveEnvValue(
		fromConfig: string | undefined,
		envName: string,
		fallback: string,
	): string {
		if (typeof fromConfig === "string" && fromConfig.trim() !== "") {
			return fromConfig.trim();
		}
		const raw = this.secretReader(envName)?.trim();
		return raw !== undefined && raw !== "" ? raw : fallback;
	}

	/**
	 * SMS_WEBHOOK_PORT resolution — `int(os.getenv("SMS_WEBHOOK_PORT", …))`
	 * parity: config object first, scoped env read second, default last;
	 * non-numeric values fall back to the documented default port.
	 */
	private resolvePort(fromConfig: number | undefined): number {
		const raw =
			fromConfig !== undefined
				? String(fromConfig)
				: (this.secretReader("SMS_WEBHOOK_PORT") ?? "");
		const trimmed = raw.trim();
		if (trimmed === "") return DEFAULT_WEBHOOK_PORT;
		const parsed = Number(trimmed);
		return Number.isFinite(parsed) ? parsed : DEFAULT_WEBHOOK_PORT;
	}

	/**
	 * Per-chat length descriptor (§6.3/A15 relay-shaped override point): the
	 * harness's utf16-marked chats return budget AND unit TOGETHER; production
	 * chats resolve the manifest scalar (MAX_SMS_LENGTH=1600 chars).
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

	/**
	 * No draft streaming BY PROBE: the Twilio REST Messages API has no edit
	 * endpoint in the source — a native draft cursor could never be sealed or
	 * edited away. Flip the datum and this flips (lie-scan mutant proves a
	 * lying capability claim FAILS the streaming family rows).
	 */
	override supportsDraftStreaming(_chatType?: string): boolean {
		return this.declaredMessageEditing;
	}

	// ── connect ladder (@~100 parity) ─────────────────────────────────────────

	/**
	 * Refusal ladder ports the source exactly: missing TWILIO_PHONE_NUMBER ⇒
	 * logged error + FATAL sms_missing_phone_number + false; missing
	 * SMS_WEBHOOK_URL without the insecure flag ⇒ FATAL sms_missing_webhook_url
	 * + false; insecure-without-URL connects but LOGS the DISABLED-validation
	 * warning. With a URL configured, "server starts" — in-process seam only
	 * (no real listen): the routes POST ${SMS_WEBHOOK_PATH} / GET
	 * ${SMS_HEALTH_PATH} hang off handleWebhookPost/handleHealthGet.
	 */
	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (!this.fromNumber) {
			const msg = "[sms] TWILIO_PHONE_NUMBER not set — cannot send replies";
			this.logger?.error?.(msg);
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail:
					"sms_missing_phone_number: TWILIO_PHONE_NUMBER not set — cannot send replies",
			});
			return false;
		}

		if (!this.webhookUrl && !this.insecureNoSignature) {
			const msg =
				"[sms] Refusing to start: SMS_WEBHOOK_URL is required for Twilio " +
				"signature validation. Set it to the public URL configured in your " +
				`Twilio console (e.g. https://example.com${SMS_WEBHOOK_PATH}). ` +
				"For local development without validation, set " +
				"SMS_INSECURE_NO_SIGNATURE=true (NOT recommended for production).";
			this.logger?.error?.(msg);
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail:
					"sms_missing_webhook_url: SMS_WEBHOOK_URL is required for Twilio signature validation",
			});
			return false;
		}

		if (this.insecureNoSignature && !this.webhookUrl) {
			this.logger?.warn?.(
				`[sms] SMS_INSECURE_NO_SIGNATURE=true — Twilio signature validation ` +
					`is DISABLED. Any client that can reach port ${String(this.port)} can inject messages. ` +
					"Do NOT use this in production.",
			);
		}

		// Server starts — in-process seam only (NO real listen in the port).
		this.connectedOnce = true;
		this.logger?.info?.(
			`[sms] Twilio webhook server listening on ${this.host}:${String(this.port)}, from: ${redactPhone(this.fromNumber)}`,
		);
		return true;
	}

	override async disconnect(): Promise<void> {
		this.connectedOnce = false;
		this.logger?.info?.("[sms] Disconnected");
	}

	get isConnected(): boolean {
		return this.connectedOnce;
	}

	/** GET /health — plain-text "ok" (source lambda parity). */
	handleHealthGet(): SmsHttpResponse {
		return { status: 200, contentType: "text/plain", body: "ok" };
	}

	// ── POST ${SMS_WEBHOOK_PATH} (_handle_webhook @~250 parity) ──────────────

	/**
	 * Order ports the source exactly: declared Content-Length cap → actual
	 * bytes cap (both 413 empty-TwiML BEFORE the parse seam) → form parse
	 * (failure ⇒ 400 empty TwiML) → signature gate (only when the public URL
	 * is configured; missing header 403, invalid signature 403 with the
	 * default-port variant fallback) → field ladder (From+Body required ⇒ 200
	 * empty TwiML; own-number echo ignored) → NON-blocking dispatch → ALWAYS
	 * the empty-TwiML answer (replies ride the REST API, never inline TwiML).
	 */
	async handleWebhookPost(input: {
		headers?: Record<string, string> | undefined;
		rawBody: Buffer;
	}): Promise<SmsHttpResponse> {
		const headers = normalizeHeaders(input.headers);

		let form: Record<string, string[]>;
		try {
			// Gate 1: DECLARED Content-Length over the cap ⇒ 413 pre-read.
			const declaredRaw = headers["content-length"];
			const declaredLength =
				declaredRaw !== undefined && /^\d+$/.test(declaredRaw)
					? Number(declaredRaw)
					: null;
			if (declaredLength !== null && declaredLength > this.maxBodyBytes) {
				this.counters.oversizedRejected += 1;
				return twiml(413);
			}
			// Gate 2: ACTUAL bytes post-read (lying headers trip here) ⇒ 413.
			if (input.rawBody.length > this.maxBodyBytes) {
				this.counters.oversizedRejected += 1;
				return twiml(413);
			}
			form = this.parseForm(input.rawBody);
		} catch (err) {
			this.counters.parseErrors += 1;
			this.logger?.error?.(
				`[sms] webhook parse error: ${err instanceof Error ? err.message : String(err)}`,
			);
			return twiml(400);
		}

		// Validate the Twilio signature ONLY when the public URL is configured;
		// insecure-no-signature mode admits unsigned requests (connect logged
		// the DISABLED-validation warning).
		if (this.webhookUrl) {
			const signature = headers["x-twilio-signature"] ?? "";
			if (!signature) {
				this.counters.missingSignatureRejected += 1;
				this.logger?.warn?.(
					"[sms] Rejected: missing X-Twilio-Signature header",
				);
				return twiml(403);
			}
			const flatParams = firstValueWins(form);
			if (
				!verifyTwilioSignature({
					authToken: this.authToken,
					url: this.webhookUrl,
					params: flatParams,
					signature,
				})
			) {
				this.counters.invalidSignatureRejected += 1;
				this.logger?.warn?.("[sms] Rejected: invalid Twilio signature");
				return twiml(403);
			}
		}

		// Field extraction (parse_qs returns lists; first value wins, stripped).
		const fromNumber = (form["From"] ?? [""])[0]?.trim() ?? "";
		const toNumber = (form["To"] ?? [""])[0]?.trim() ?? "";
		const text = (form["Body"] ?? [""])[0]?.trim() ?? "";
		const messageSid = (form["MessageSid"] ?? [""])[0]?.trim() ?? "";

		if (!fromNumber || !text) {
			this.counters.missingFieldsIgnored += 1;
			return twiml(200);
		}

		// Echo prevention: our own number never becomes a turn.
		if (fromNumber === this.fromNumber) {
			this.counters.echoIgnored += 1;
			this.logger?.debug?.(
				`[sms] ignoring echo from own number ${redactPhone(fromNumber)}`,
			);
			return twiml(200);
		}

		this.logger?.info?.(
			`[sms] inbound from ${redactPhone(fromNumber)} -> ${redactPhone(toNumber)}: ${text.slice(0, 80)}`,
		);

		const event: IncomingEvent = {
			messageType: "text",
			text,
			...(messageSid ? { messageId: messageSid } : {}),
			source: {
				platform: SMS_PLUGIN_MANIFEST.name,
				chatType: "dm",
				userId: fromNumber,
				chatId: fromNumber,
				chatName: fromNumber,
			},
		};
		this.dispatchedEvents.push({
			messageId: messageSid,
			text,
			from: fromNumber,
		});
		this.counters.dispatched += 1;
		// Non-blocking: Twilio expects a fast response (asyncio.create_task
		// parity) — dispatch rides a tracked fire-and-forget promise.
		this.fireInbound(event, fromNumber);

		return twiml(200);
	}

	/** Await every pending non-blocking dispatch (fixture determinism seam). */
	async drainInbound(): Promise<void> {
		while (this.pendingDispatches.size > 0) {
			await Promise.all([...this.pendingDispatches]);
		}
	}

	private fireInbound(event: IncomingEvent, sessionKey: string): void {
		const task = this.deliverInbound(event, sessionKey).catch(() => {
			/* containment parity: one poisoned message never rejects the handler */
		});
		void task
			.finally(() => {
				this.pendingDispatches.delete(task);
			})
			.catch(() => {});
		this.pendingDispatches.add(task);
	}

	/**
	 * THE parse seam — urllib.parse.parse_qs(raw.decode("utf-8"),
	 * keep_blank_values=True) parity. Invalid UTF-8 THROWS (UnicodeDecodeError
	 * parity — TextDecoder fatal mode) so the 400 ladder catches it; blank
	 * values are kept; repeated keys collect in order (first value wins later);
	 * blank KEYS are dropped like parse_qs does.
	 */
	private parseForm(rawBody: Buffer): Record<string, string[]> {
		this.counters.parseInvocations += 1;
		const text = new TextDecoder("utf-8", { fatal: true }).decode(rawBody);
		const fields: Record<string, string[]> = {};
		for (const pair of text.split("&")) {
			if (pair.length === 0) continue;
			const eq = pair.indexOf("=");
			const rawKey = eq === -1 ? pair : pair.slice(0, eq);
			const rawValue = eq === -1 ? "" : pair.slice(eq + 1);
			const key = decodePlus(rawKey);
			if (!key) continue; // parse_qs drops blank keys
			const value = decodePlus(rawValue);
			(fields[key] ??= []).push(value);
		}
		return fields;
	}

	// ── guard wiring (reference-fixture inheritance) ──────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: SMS_REGISTRY,
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
	 * THE outbound SMS lane (adapter.py send @~150 parity): format_message
	 * strips markdown, truncate_message splits at MAX_MESSAGE_LENGTH via THE
	 * chat length policy, then EACH chunk POSTs Messages.json form-encoded
	 * From/To/Body. HTTP ≥400 stops the loop immediately with
	 * `Twilio ${status}: ${body.message}`; the last sid travels back as
	 * messageId. Chunks arrive here EITHER whole (door-1 send) or pre-split by
	 * the kit planner (deliverText) — re-splitting a fitting piece is a
	 * single-chunk passthrough, so both shapes observe the source behavior.
	 *
	 * §6.1 carve-out: the kit plain-text fallback notice carries ORIGINAL
	 * chunk bytes by contract (shared row) — strip_markdown applies to
	 * agent-authored bodies only, never to that system lane.
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		if (this.rest === undefined) {
			return {
				success: false,
				error: "[sms] no Messages.json REST transport bound",
			};
		}
		const formatted = content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			? content
			: stripMarkdownForSms(content);
		const policy = this.chatLengthPolicyForChat(chatId);
		const plan = chunkWithFenceCarry(formatted, policy);

		// adapter.py:194-197 parity — the REST URL and Basic auth header are
		// composed ONCE per send (outside the chunk loop) from the resolved
		// account SID/auth token, then reused for every chunk POST.
		const url = `${TWILIO_API_BASE}/${this.accountSid}/Messages.json`;
		const authorization = twilioBasicAuthHeader(
			this.accountSid,
			this.authToken,
		);

		let lastResult: SendResult = { success: true };
		for (const chunk of plan.chunks) {
			let response: SmsRestResponse;
			try {
				response = await this.rest.postMessages({
					url,
					authorization,
					from: this.fromNumber,
					to: chatId,
					body: chunk,
					metadata,
				});
			} catch (err) {
				this.logger?.error?.(
					`[sms] send error to ${redactPhone(chatId)}: ${err instanceof Error ? err.message : String(err)}`,
				);
				return {
					success: false,
					error: err instanceof Error ? err.message : String(err),
				};
			}
			if (response.status >= 400) {
				const errorMessage =
					typeof response.json["message"] === "string"
						? response.json["message"]
						: JSON.stringify(response.json);
				this.logger?.error?.(
					`[sms] send failed to ${redactPhone(chatId)}: ${String(response.status)} ${errorMessage}`,
				);
				return {
					success: false,
					error: `Twilio ${String(response.status)}: ${errorMessage}`,
					...(response.retryable !== undefined
						? { retryable: response.retryable }
						: {}),
					...(response.retryAfter !== undefined
						? { retryAfter: response.retryAfter }
						: {}),
				};
			}
			lastResult = {
				success: true,
				messageId:
					typeof response.json["sid"] === "string" ? response.json["sid"] : "",
			};
		}
		return lastResult;
	}

	/**
	 * Rich lane ABSENT on the real surface (plain-text SMS only): unless a
	 * conformance subject scripted a rich probe, answer the capability-error
	 * shape WITHOUT burning a roundtrip (§10.1 latch path probes once then
	 * never again — webhook reference adapter parity).
	 */
	protected override async wireRich(content: string): Promise<SendResult> {
		if (this.richProbe === undefined || !this.richProbe.hasRichScript("rich")) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return this.richProbe.transmitRich("__rich__", content);
	}

	/** adapter.py get_chat_info — trivial dm identity. */
	getChatInfo(chatId: string): { name: string; type: "dm" } {
		return { name: chatId, type: "dm" };
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

/** {k: v[0] for k, v in form.items() if v} — first value wins. */
function firstValueWins(
	form: Record<string, string[]>,
): Record<string, string> {
	const flat: Record<string, string> = {};
	for (const [key, values] of Object.entries(form)) {
		if (values.length > 0) flat[key] = values[0] ?? "";
	}
	return flat;
}

/** urllib unquote_plus parity: '+' means space; invalid %-sequences stay literal. */
function decodePlus(text: string): string {
	const withSpaces = text.replace(/\+/g, " ");
	try {
		return decodeURIComponent(withSpaces);
	} catch {
		return withSpaces; // malformed escape kept literally (unquote tolerance)
	}
}

/** redact_phone parity placeholder — E.164 numbers render +1•••••••123. */
export function redactPhone(phone: string): string {
	if (phone.length <= 4) return "***";
	return `${phone.slice(0, 2)}${"*".repeat(Math.max(0, phone.length - 5))}${phone.slice(-3)}`;
}

export type { ParsedForm };
