// pi_platforms/line/line-webhook-adapter — THE LINE Messaging API webhook
// adapter, ported from the READ-ONLY Hermes plugin
// (plugins/platforms/line/adapter.py) onto the kit base. Everything
// policy-shaped is inherited; this module supplies TRANSPORT (the signed
// webhook endpoint + the Reply/Push egress seam), MANIFEST DATA, and the
// slow-LLM postback state machine.
//
// Shape (DEC-002 third column — stateless webhook):
//   - capabilities AS DATA: supports_async_delivery=False +
//     interactive_resume=False (manifest DIVERGENCE note)
//   - ingress ports _handle_webhook exactly: declared-length cap → actual-
//     bytes cap (both pre-parse, aiohttp client_max_size parity) → signature
//     verify (401 on failure) → JSON parse (400 bad json) → per-event walk
//     (webhookEventId dedup → self-filter → allowlist gate) → 200 "ok"
//   - Reply-token-first egress with Push fallback (_send_text_chunks):
//     tokens are STASHED per chat at inbound (TTL 50 s under the ~60 s vendor
//     TTL) and CONSUMED single-use; reply rejection falls back to Push once.
//     EVERY text lane lands as ONE Reply/Push call of ≤5 bubbles with an
//     ellipsis tail (split_for_line parity — splits_long_messages=True
//     inheritance means the ADAPTER owns native splitting; no lossless
//     kit-chunked multi-push)
//   - system busy-acks (⚡ Interrupting / ⏳ Queued / ⏩ Steered / 💾) BYPASS
//     the pending-postback cache so they reach the user while a button is
//     outstanding (_is_system_bypass parity @656-667/:1185-1188, PR #18153)
//   - send_typing re-fires the loading indicator from the processing
//     heartbeat (send_typing parity @1240), not just once at inbound receipt
//   - slow-LLM postback state machine (RequestCache PENDING → READY →
//     DELIVERED / ERROR): the source fires the button from a 45 s typing
//     timer; the port exposes the SAME transition as a deterministic method
//     driven by the injected clock in rows (no wall-clock timers headlessly)
//   - media plane (image/audio/video via public HTTPS URLs + tempfile
//     serving): requires a publicly reachable HTTPS host and binary caching;
//     the download/serving seams stay INJECTED and unexercised headlessly —
//     documented probe-computed exclusion, never faked green
//
// Layering: imports pi_gateway downward + kit same-layer ONLY; no adapter
// cross-imports.

import { createHmac } from "node:crypto";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	secureCompare,
	resolveEnablement,
	PLAIN_TEXT_FALLBACK_PREFIX,
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
	LINE_BUTTON_ALT_TEXT_CAP,
	LINE_BUTTON_TEXT_CAP,
	LINE_CACHE_PENDING_TTL_SECONDS,
	LINE_CACHE_TTL_SECONDS,
	LINE_CAPABILITIES,
	LINE_DEDUP_MAX_ENTRIES,
	LINE_DEFAULT_PORT,
	LINE_DEFAULT_WEBHOOK_PATH,
	LINE_HEALTH_PATH_SUFFIX,
	LINE_LOADING_MAX_SECONDS,
	LINE_LOADING_MIN_SECONDS,
	LINE_LOADING_STEP_SECONDS,
	LINE_MAX_MESSAGES_PER_CALL,
	LINE_PER_BUBBLE_CHARS,
	LINE_PLUGIN_MANIFEST,
	LINE_POSTBACK_DISPLAY_TEXT_CAP,
	LINE_POSTBACK_LABEL_CAP,
	LINE_REPLY_TOKEN_TTL_SECONDS,
	LINE_SAFE_BUBBLE_CHARS,
	LINE_SLOW_RESPONSE_THRESHOLD_SECONDS,
	LINE_WEBHOOK_BODY_CAP_BYTES,
	validateLineTrustBoundary,
} from "./manifest.js";
import type { LineTrustBoundary } from "./manifest.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type { DisableReason } from "../kit/lifecycle-state.js";
import { BoundedSeenSet } from "../../pi_gateway/security/trust/index.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const LINE_REGISTRY: CommandRegistry = [
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

// ── configuration (__init__ @697 parity) ─────────────────────────────────────

export interface LineAdapterConfig {
	port?: number | undefined;
	webhook_path?: string | undefined;
	host?: string | null | undefined;
	channel_access_token?: string | undefined;
	channel_secret?: string | undefined;
	allow_all_users?: boolean | undefined;
	allowed_users?: readonly string[] | undefined;
	allowed_groups?: readonly string[] | undefined;
	allowed_rooms?: readonly string[] | undefined;
	slow_response_threshold?: number | undefined;
	pending_text?: string | undefined;
	button_label?: string | undefined;
	delivered_text?: string | undefined;
	interrupted_text?: string | undefined;
}

/**
 * OUTBOUND transport seam (adapter.py:_LineClient parity). `reply` POSTs the
 * free reply-token endpoint; `push` POSTs the metered Push endpoint. Both
 * take fully-built message objects.
 *
 * Optional vendor edges (same _LineClient surface):
 *   - `loading` — POST /v2/bot/chat/loading/start (DM only; clamp helper
 *     `clampLoadingSeconds` mirrors the source's 5..60 step-5 bound)
 *   - `botInfo` — GET /v2/bot/info, best-effort source of our own userId for
 *     self-echo filtering (get_bot_user_id parity; fail-open undefined)
 *   - `fetchContent` — GET api-data.line.me/v2/bot/message/{id}/content
 *     (Bearer); INJECTED and unexercised headlessly (probe-computed exclusion,
 *     DEC-GCHAT-style note in the module header) — when a harness binds it,
 *     inbound media binaries are cached and surfaced on the event.
 */
export interface LineApiTransport {
	reply(
		replyToken: string,
		messages: LineMessage[],
		metadata?: Record<string, unknown>,
	): Promise<SendResult>;
	push(
		chatId: string,
		messages: LineMessage[],
		metadata?: Record<string, unknown>,
	): Promise<SendResult>;
	loading?(chatId: string, seconds: number): Promise<SendResult>;
	botInfo?(): Promise<{ userId?: string | undefined }>;
	fetchContent?(messageId: string): Promise<Buffer>;
}

/**
 * Optional RICH probe seam (subject-supplied, msgraph captureWire parity):
 * LINE text bubbles have no native rich lane; when a harness scripts one,
 * the §10.1 latch path must still OBSERVE the probe exactly once.
 */
export interface LineCaptureWire {
	hasRichScript(opKind: string): boolean;
	transmitRich(chatId: string, content: string): Promise<SendResult>;
}

/** Wire message objects (media builders live beside them). */
export interface LineTextMessage {
	type: "text";
	text: string;
}

/** Template Buttons bubble (build_postback_button_message @~630 parity):
 * NO top-level text — api.line.me rejects {type:'text'} + altText/template. */
export interface LineTemplateButtonsMessage {
	type: "template";
	altText: string;
	template: {
		type: "buttons";
		text: string;
		actions: Array<{
			type: "postback";
			label: string;
			data: string;
			displayText: string;
		}>;
	};
}

/** _image_message parity (@595). */
export interface LineImageMessage {
	type: "image";
	originalContentUrl: string;
	previewImageUrl: string;
}

/** _audio_message parity (@603). */
export interface LineAudioMessage {
	type: "audio";
	originalContentUrl: string;
	duration: number;
}

/** _video_message parity (@611). */
export interface LineVideoMessage {
	type: "video";
	originalContentUrl: string;
	previewImageUrl: string;
}

export type LineMessage =
	| LineTextMessage
	| LineTemplateButtonsMessage
	| LineImageMessage
	| LineAudioMessage
	| LineVideoMessage;

/** The user-visible bubble text of any message object (template bubbles
 * render their inner template.text; text bubbles carry it top-level). */
export function lineBubbleText(message: LineMessage): string {
	if (message.type === "text") return message.text;
	if (message.type === "template") return message.template.text;
	return "";
}

// ── outbound media builders (_image_message/_audio_message/_video_message
//    @595-617 parity) — public HTTPS URLs are REQUIRED by the vendor; the
//    serving seams stay injected/unexercised headlessly (documented exclusion).
export function imageMessage(
	originalUrl: string,
	previewUrl?: string | undefined,
): LineImageMessage {
	return {
		type: "image",
		originalContentUrl: originalUrl,
		previewImageUrl: previewUrl ?? originalUrl,
	};
}

export function audioMessage(url: string, durationMs = 1000): LineAudioMessage {
	return {
		type: "audio",
		originalContentUrl: url,
		duration: Math.trunc(durationMs),
	};
}

export function videoMessage(
	url: string,
	previewUrl: string,
): LineVideoMessage {
	return {
		type: "video",
		originalContentUrl: url,
		previewImageUrl: previewUrl,
	};
}

export interface LineWebhookAdapterOptions {
	config?: LineAdapterConfig | undefined;
	secretReader?: ScopedSecretReader | undefined;
	nowMs?: (() => number) | undefined;
	scalarMaxUnits?: number | undefined;
	transport?: LineApiTransport | undefined;
	/** Conformance-harness rich probe capture (see LineCaptureWire). */
	captureWire?: LineCaptureWire | undefined;
	dedupCap?: number | undefined;
	/**
	 * Inbound media cache root — only exercised when the transport binds
	 * `fetchContent` (injected seam; headless runs leave both unbound and
	 * degrade to '[image]'-style placeholders exactly like the source's
	 * failed-download path).
	 */
	mediaCacheDir?: string | undefined;
}

// ── markdown stripping, URL-preserving (@216-330 parity) ────────────────────

const MD_CODE_BLOCK_RE = /```[a-zA-Z0-9_+-]*\n?(.*?)```/gs;
const MD_CODE_INLINE_RE = /`([^`]+)`/g;
const MD_LINK_RE = /\[([^\]]+)\]\((https?:\/\/[^\s)]+)\)/g;
const MD_BOLD_RE = /\*\*(.+?)\*\*/g;
const MD_ITAL_RE = /(?<!\*)\*(?!\s)(.+?)(?<!\s)\*(?!\*)/g;
const MD_HEADING_RE = /^#{1,6}\s+/gm;
const MD_BULLET_RE = /^[\s]*[-*+]\s+/gm;

/**
 * strip_markdown_preserving_urls parity: LINE text bubbles render zero
 * Markdown. URLs stay tappable only bare, so [label](url) becomes
 * "label (url)"; code-block CONTENT survives unfenced; bold/italic markers
 * and heading/bullet prefixes are stripped (bullets become "• ").
 */
export function stripMarkdownPreservingUrls(text: string): string {
	if (!text) return text;
	let out = text.replace(MD_CODE_BLOCK_RE, (_m, inner: string) =>
		(inner as string).replace(/\n+$/, ""),
	);
	out = out.replace(MD_CODE_INLINE_RE, "$1");
	out = out.replace(MD_LINK_RE, "$1 ($2)");
	out = out.replace(MD_BOLD_RE, "$1");
	out = out.replace(MD_ITAL_RE, "$1");
	out = out.replace(MD_HEADING_RE, "");
	out = out.replace(MD_BULLET_RE, "• ");
	return out;
}

/**
 * split_for_line parity: split into bubble-sized chunks preferring paragraph
 * > newline > space breaks; AT MOST 5 chunks — overflow truncates the final
 * chunk with an ellipsis so one Reply/Push call stays deliverable. This is
 * THE native splitter for EVERY text lane on this platform
 * (splits_long_messages=True inheritance): dispatchBubbles applies it to the
 * whole response so no lane ever issues a lossless kit-chunked multi-push.
 */
export function splitForLine(
	text: string,
	maxChars: number = LINE_SAFE_BUBBLE_CHARS,
): string[] {
	if (!text) return [];
	if (text.length <= maxChars) return [text];
	const chunks: string[] = [];
	let remaining = text;
	while (remaining && chunks.length < LINE_MAX_MESSAGES_PER_CALL) {
		if (remaining.length <= maxChars) {
			chunks.push(remaining);
			remaining = "";
			break;
		}
		let cut = remaining.lastIndexOf("\n\n", maxChars);
		if (cut < Math.floor(maxChars * 0.5))
			cut = remaining.lastIndexOf("\n", maxChars);
		if (cut < Math.floor(maxChars * 0.5))
			cut = remaining.lastIndexOf(" ", maxChars);
		if (cut <= 0) cut = maxChars;
		chunks.push(remaining.slice(0, cut).replace(/\s+$/, ""));
		remaining = remaining.slice(cut).replace(/^\s+/, "");
	}
	if (remaining) {
		if (chunks.length > 0) {
			let tail = chunks[chunks.length - 1] as string;
			if (tail.length > maxChars - 1) tail = tail.slice(0, maxChars - 1);
			chunks[chunks.length - 1] = `${tail.replace(/\s+$/, "")}…`;
		} else {
			chunks.push(`${remaining.slice(0, maxChars - 1)}…`);
		}
	}
	return chunks;
}

export function textMessage(raw: string): LineTextMessage {
	const text =
		raw.length > LINE_PER_BUBBLE_CHARS
			? `${raw.slice(0, LINE_PER_BUBBLE_CHARS - 1)}…`
			: raw;
	return { type: "text", text };
}

/**
 * _LineClient.loading clamp parity (@~545): "LINE caps loadingSeconds in
 * 5-step increments, max 60" — max(5, min(60, (seconds // 5) * 5 or 5)).
 */
export function clampLoadingSeconds(seconds: number): number {
	const step =
		Math.floor(seconds / LINE_LOADING_STEP_SECONDS) * LINE_LOADING_STEP_SECONDS;
	return Math.max(
		LINE_LOADING_MIN_SECONDS,
		Math.min(LINE_LOADING_MAX_SECONDS, step || LINE_LOADING_MIN_SECONDS),
	);
}

// ── system busy-ack bypass (_SYSTEM_BYPASS_PREFIXES @656 parity) ───────────

/**
 * Prefixes the gateway uses for system busy-acks (interrupting / queued /
 * steered / background-review summary). When the postback cache holds a
 * PENDING entry these BYPASS it and route directly to Reply/Push so they
 * reach the user as visible bubbles instead of being silently swallowed.
 * From PR #18153.
 */
export const SYSTEM_BYPASS_PREFIXES: readonly string[] = Object.freeze([
	"⚡ Interrupting",
	"⏳ Queued",
	"⏩ Steered",
	"💾", // background-review summary
]);

/** _is_system_bypass parity (@664). */
export function isSystemBypass(content: string): boolean {
	if (!content) return false;
	return SYSTEM_BYPASS_PREFIXES.some((p) => content.startsWith(p));
}

// ── slow-LLM postback cache (State/_CacheEntry/RequestCache @336-423) ───────

export type PostbackState = "pending" | "ready" | "delivered" | "error";

interface CacheEntry {
	state: PostbackState;
	payload: unknown;
	chatId: string;
	createdAtMs: number;
	updatedAtMs: number;
}

/** RequestCache parity with the injected clock (prune() walks both TTLs). */
export class PostbackRequestCache {
	private readonly entries = new Map<string, CacheEntry>();
	private seq = 0;

	constructor(
		private readonly nowMs: () => number,
		private readonly ttlMs = LINE_CACHE_TTL_SECONDS * 1000,
		private readonly pendingTtlMs = LINE_CACHE_PENDING_TTL_SECONDS * 1000,
	) {}

	registerPending(chatId: string): string {
		this.seq += 1;
		const rid = `rid-${this.seq}-${this.nowMs()}`;
		this.entries.set(rid, {
			state: "pending",
			payload: null,
			chatId,
			createdAtMs: this.nowMs(),
			updatedAtMs: this.nowMs(),
		});
		return rid;
	}

	get(requestId: string): CacheEntry | undefined {
		return this.entries.get(requestId);
	}

	setReady(requestId: string, payload: unknown): void {
		const entry = this.entries.get(requestId);
		if (entry === undefined || entry.state !== "pending") return;
		entry.state = "ready";
		entry.payload = payload;
		entry.updatedAtMs = this.nowMs();
	}

	setError(requestId: string, message: string): void {
		const entry = this.entries.get(requestId);
		if (entry === undefined || entry.state !== "pending") return;
		entry.state = "error";
		entry.payload = message;
		entry.updatedAtMs = this.nowMs();
	}

	markDelivered(requestId: string): void {
		const entry = this.entries.get(requestId);
		if (
			entry === undefined ||
			(entry.state !== "ready" && entry.state !== "error")
		) {
			return;
		}
		entry.state = "delivered";
		entry.updatedAtMs = this.nowMs();
	}

	findPendingForChat(chatId: string): string | undefined {
		for (const [rid, entry] of this.entries) {
			if (entry.state === "pending" && entry.chatId === chatId) return rid;
		}
		return undefined;
	}

	prune(): number {
		const now = this.nowMs();
		let removed = 0;
		for (const [rid, entry] of this.entries) {
			const ageMs =
				now -
				(entry.state === "pending" ? entry.createdAtMs : entry.updatedAtMs);
			const expired =
				entry.state === "pending"
					? ageMs > this.pendingTtlMs
					: ageMs > this.ttlMs;
			if (expired) {
				this.entries.delete(rid);
				removed += 1;
			}
		}
		return removed;
	}
}

/** build_postback_button_message parity (@~630): Template Buttons bubble.
 * Wire shape is {type:'template', altText, template:{type:'buttons',text,
 * actions}} — NO top-level text field (api.line.me rejects it). label caps
 * at 20 while displayText caps INDEPENDENTLY at 300 (two source slices). */
export function buildPostbackButtonMessage(
	text: string,
	buttonLabel: string,
	requestId: string,
): LineTemplateButtonsMessage {
	const truncated =
		text.length <= LINE_BUTTON_TEXT_CAP
			? text
			: `${text.slice(0, LINE_BUTTON_TEXT_CAP - 3)}...`;
	const alt =
		text.length <= LINE_BUTTON_ALT_TEXT_CAP
			? text
			: `${text.slice(0, LINE_BUTTON_ALT_TEXT_CAP - 3)}...`;
	const label = buttonLabel.slice(0, LINE_POSTBACK_LABEL_CAP) || "Get answer";
	const displayText =
		buttonLabel.slice(0, LINE_POSTBACK_DISPLAY_TEXT_CAP) || "Get answer";
	return {
		type: "template",
		altText: alt,
		template: {
			type: "buttons",
			text: truncated,
			actions: [
				{
					type: "postback",
					label,
					data: JSON.stringify({
						action: "show_response",
						request_id: requestId,
					}),
					displayText,
				},
			],
		},
	};
}

// ── handler response surface ─────────────────────────────────────────────────

export interface HandlerResponse {
	status: number;
	contentType?: "text/plain" | "application/json" | undefined;
	body?: string | Record<string, never> | undefined;
}

interface ResolvedChat {
	chatId: string;
	chatType: "dm" | "group" | "room";
}

/** _resolve_chat parity: group→groupId, room→roomId, user→userId. */
export function resolveLineChat(source: Record<string, unknown>): ResolvedChat {
	const srcType = String(source["type"] ?? "");
	if (srcType === "group")
		return { chatId: String(source["groupId"] ?? ""), chatType: "group" };
	if (srcType === "room")
		return { chatId: String(source["roomId"] ?? ""), chatType: "room" };
	return { chatId: String(source["userId"] ?? ""), chatType: "dm" };
}

/** _LINE_MESSAGE_TYPES parity (audio ⇒ voice; unknown ⇒ text). */
const MESSAGE_TYPES: Readonly<Record<string, IncomingEvent["messageType"]>> =
	Object.freeze({
		text: "text",
		image: "photo",
		video: "video",
		audio: "voice",
		file: "document",
		location: "location",
		sticker: "other",
	});

// ── THE adapter ──────────────────────────────────────────────────────────────

export class LineWebhookAdapter extends BasePlatformAdapter {
	readonly pluginManifest = LINE_PLUGIN_MANIFEST;
	readonly trustBoundary: LineTrustBoundary;

	readonly port: number;
	readonly webhookPath: string;
	readonly healthPath: string;
	readonly slowResponseThresholdSeconds: number;
	readonly pendingText: string;
	readonly buttonLabel: string;
	readonly deliveredText: string;
	readonly interruptedText: string;

	private readonly secretReader: ScopedSecretReader;
	private readonly configAccessToken: string | undefined;
	private readonly configChannelSecret: string | undefined;
	private readonly nowFn: () => number;
	private readonly transport: LineApiTransport | undefined;
	private readonly captureWire: LineCaptureWire | undefined;
	readonly mediaCacheDir: string;
	private readonly allowAll: boolean;
	private readonly allowedUsers: ReadonlySet<string>;
	private readonly allowedGroups: ReadonlySet<string>;
	private readonly allowedRooms: ReadonlySet<string>;

	/** chat_id → stashed reply token + expiry (single-use consumption). */
	private readonly replyTokens = new Map<
		string,
		{ token: string; expiresAtMs: number }
	>();
	private readonly dedup: BoundedSeenSet;
	private readonly cache = new PostbackRequestCache(() => this.nowFn());
	/** One outstanding postback button slot per chat. */
	private readonly pendingButtons = new Map<string, string>();

	readonly counters = {
		accepted: 0,
		duplicates: 0,
		rejectedSignature: 0,
		tooLarge: 0,
		badJson: 0,
		unauthorizedSource: 0,
		postbacksResolved: 0,
		pushFallbacks: 0,
		parseInvocations: 0,
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
	private allowAllClickers = true;
	private readonly clarifyArmedSet = new Set<string>();
	private holding = false;
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};

	constructor(opts: LineWebhookAdapterOptions = {}) {
		const config = opts.config ?? {};
		super({
			manifestName: LINE_PLUGIN_MANIFEST.name,
			capabilities: LINE_CAPABILITIES,
			scalarMaxUnits: opts.scalarMaxUnits ?? LINE_SAFE_BUBBLE_CHARS,
		});
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.transport = opts.transport;
		this.captureWire = opts.captureWire;
		this.mediaCacheDir =
			opts.mediaCacheDir ?? join(process.cwd(), "platforms", "line", "media");
		this.port = Number(config.port ?? LINE_DEFAULT_PORT);
		this.webhookPath = normalizePath(
			config.webhook_path ?? LINE_DEFAULT_WEBHOOK_PATH,
		);
		this.healthPath = normalizePath(
			`${config.webhook_path ?? LINE_DEFAULT_WEBHOOK_PATH}${LINE_HEALTH_PATH_SUFFIX}`,
		);
		this.slowResponseThresholdSeconds = Number(
			config.slow_response_threshold ?? LINE_SLOW_RESPONSE_THRESHOLD_SECONDS,
		);
		this.pendingText =
			config.pending_text ??
			"🤔 Still thinking. Tap below to fetch the answer when it's ready.";
		this.buttonLabel = config.button_label ?? "Get answer";
		this.deliveredText = config.delivered_text ?? "Already replied ✅";
		this.interruptedText =
			config.interrupted_text ?? "Run was interrupted before completion.";

		// Credentials: extra config first (Hermes parity), scoped secret second.
		const tokenFromConfig = nonEmpty(config.channel_access_token);
		const secretFromConfig = nonEmpty(config.channel_secret);
		this.configAccessToken =
			tokenFromConfig ?? this.secretReader("LINE_CHANNEL_ACCESS_TOKEN");
		this.configChannelSecret =
			secretFromConfig ?? this.secretReader("LINE_CHANNEL_SECRET");

		// Three-allowlist gating (_csv_set ∪ extra lists).
		this.allowAll = config.allow_all_users === true;
		this.allowedUsers = new Set([
			...(config.allowed_users ?? []).map((v) => v.trim()).filter(Boolean),
		]);
		this.allowedGroups = new Set([
			...(config.allowed_groups ?? []).map((v) => v.trim()).filter(Boolean),
		]);
		this.allowedRooms = new Set([
			...(config.allowed_rooms ?? []).map((v) => v.trim()).filter(Boolean),
		]);

		this.trustBoundary =
			LINE_PLUGIN_MANIFEST.trustBoundary as LineTrustBoundary;
		const boundaryErrors = validateLineTrustBoundary(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			const reason: DisableReason = {
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			};
			this.lifecycle.disable(reason);
		}

		// §11 step 3/4: missing required secrets ⇒ LOUD disable.
		const enablement = resolveEnablement(
			LINE_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		// Replay dedupe: bounded set of webhookEventId, NO TTL (source
		// _MessageDeduplicator shape; FIFO eviction vs the source's oldest-10%
		// batch drop — same observable bound, proposed DEC note).
		this.dedup = new BoundedSeenSet({
			maxEntries: Math.max(1, opts.dedupCap ?? LINE_DEDUP_MAX_ENTRIES),
			ttlMs: null,
			nowMs: this.nowFn,
		});

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // reply/push egress; no native lanes
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

	get channelAccessToken(): string | undefined {
		return this.configAccessToken;
	}

	// NOTE: no chatDescriptorFor override — Hermes' LineAdapter has NO
	// per-chat length budgets (split_for_line uses the fixed
	// LINE_SAFE_BUBBLE_CHARS bubble budget for every chat), and with
	// splitsLongMessages=True the base kit split never runs anyway.

	get channelSecret(): string | undefined {
		return this.configChannelSecret;
	}

	/** Bot identity for self-filtering (get_bot_user_id parity). Populated
	 * best-effort during connect() via the transport's botInfo() edge; a
	 * failed/absent lookup fails OPEN as undefined (source parity). */
	private botUserIdValue: string | undefined = undefined;
	get botUserId(): string | undefined {
		return this.botUserIdValue;
	}

	/** Observability: inbound media binaries cached through fetchContent. */
	readonly inboundMediaLog: Array<{
		chatId: string;
		urls: string[];
		types: string[];
	}> = [];

	// ── webhook signature verification (verify_line_signature parity) ────────

	/**
	 * HMAC-SHA256 over the RAW body keyed by the channel secret, BASE64-encoded
	 * digest, compared CONSTANT-TIME against X-Line-Signature bytes. Missing
	 * signature or secret rejects WITHOUT computing (fail-closed).
	 */
	verifyLineSignature(
		body: Buffer,
		signature: string,
		channelSecret: string,
	): boolean {
		if (!signature || !channelSecret) return false;
		const expected = createHmac("sha256", channelSecret)
			.update(body)
			.digest("base64");
		return secureCompare(expected, signature);
	}

	// ── HTTP handler surface (_handle_webhook @937 parity) ──────────────────

	handleHealthGet(): HandlerResponse {
		return {
			status: 200,
			contentType: "application/json",
			body: JSON.stringify({ status: "ok", platform: "line" }),
		};
	}

	/**
	 * Order ports the source exactly: body caps (declared length then actual
	 * bytes — aiohttp client_max_size + the explicit len() guard) run BEFORE
	 * signature verification, which runs BEFORE the parse seam.
	 */
	async handleWebhookPost(input: {
		headers?: Record<string, string> | undefined;
		rawBody: Buffer;
	}): Promise<HandlerResponse> {
		const headers = lowerKeys(input.headers);

		// Gate 1: honest declared Content-Length over the cap (client_max_size
		// rejects oversized bodies at the read layer before our handler runs).
		const declaredRaw = headers["content-length"];
		const declaredLength =
			declaredRaw !== undefined && /^\d+$/.test(declaredRaw)
				? Number(declaredRaw)
				: null;
		const cap = LINE_WEBHOOK_BODY_CAP_BYTES;
		if (declaredLength !== null && declaredLength > cap) {
			this.counters.tooLarge += 1;
			return {
				status: 413,
				contentType: "text/plain",
				body: "payload too large",
			};
		}
		// Gate 2: LYING Content-Length trips on actual bytes post-read.
		if (input.rawBody.length > cap) {
			this.counters.tooLarge += 1;
			return {
				status: 413,
				contentType: "text/plain",
				body: "payload too large",
			};
		}

		const signature = headers["x-line-signature"] ?? "";
		if (
			!this.verifyLineSignature(
				input.rawBody,
				signature,
				this.configChannelSecret ?? "",
			)
		) {
			this.counters.rejectedSignature += 1;
			return {
				status: 401,
				contentType: "text/plain",
				body: "invalid signature",
			};
		}

		const parsed = this.parseJsonBody(input.rawBody);
		if (!parsed.ok) {
			this.counters.badJson += 1;
			return { status: 400, contentType: "text/plain", body: "bad json" };
		}
		// Source does payload.get("events", []) — a non-object payload would
		// raise AttributeError ⇒ aiohttp 500. The port maps structural failures
		// to 400 like every other sender-config signal (proposed DEC).
		const payload = parsed.value;
		if (
			payload === null ||
			typeof payload !== "object" ||
			Array.isArray(payload)
		) {
			this.counters.badJson += 1;
			return { status: 400, contentType: "text/plain", body: "bad json" };
		}
		const events = (payload as Record<string, unknown>)["events"];
		const list = Array.isArray(events) ? events : [];

		for (const event of list) {
			try {
				await this.dispatchEvent(asRecord(event));
			} catch {
				/* containment parity: one poisoned event never fails the batch */
			}
		}
		return { status: 200, contentType: "text/plain", body: "ok" };
	}

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

	// ── per-event walk (_dispatch_event @968 parity) ─────────────────────────

	async dispatchEvent(event: Record<string, unknown>): Promise<void> {
		const eventType = String(event["type"] ?? "");
		const source = asRecord(event["source"]);
		const webhookEventId = String(event["webhookEventId"] ?? "");

		// Dedup retries (LINE webhooks may be re-delivered).
		if (webhookEventId && !this.dedup.add(webhookEventId)) {
			this.counters.duplicates += 1;
			return;
		}

		// Self-filter: our own bot userId echoes never dispatch. The harness
		// convention additionally drops userId "bot-self" (deliverInbound).
		const senderUserId = String(source["userId"] ?? "");
		if (this.botUserId && senderUserId && senderUserId === this.botUserId) {
			return;
		}

		// Three-allowlist gate.
		if (!this.allowedForSource(source)) {
			this.counters.unauthorizedSource += 1;
			return;
		}

		if (eventType === "message") {
			await this.handleMessageEvent(event);
		} else if (eventType === "postback") {
			await this.handlePostbackEvent(event);
		}
		// follow/unfollow/join/leave + unknown types: lifecycle log only.
	}

	/** _allowed_for_source parity: three separate allowlists + dev bypass. */
	allowedForSource(source: Record<string, unknown>): boolean {
		if (this.allowAll) return true;
		const srcType = String(source["type"] ?? "");
		if (srcType === "user") {
			const uid = String(source["userId"] ?? "");
			return uid.length > 0 && this.allowedUsers.has(uid);
		}
		if (srcType === "group") {
			const gid = String(source["groupId"] ?? "");
			return gid.length > 0 && this.allowedGroups.has(gid);
		}
		if (srcType === "room") {
			const rid = String(source["roomId"] ?? "");
			return rid.length > 0 && this.allowedRooms.has(rid);
		}
		return false;
	}

	// ── message events (@1003 parity) ────────────────────────────────────────

	protected async handleMessageEvent(
		event: Record<string, unknown>,
	): Promise<void> {
		const msg = asRecord(event["message"]);
		const msgType = String(msg["type"] ?? "");
		const messageId = String(msg["id"] ?? "");
		const replyToken = String(event["replyToken"] ?? "");
		const source = asRecord(event["source"]);
		const { chatId, chatType } = resolveLineChat(source);
		const userId = String(source["userId"] ?? "") || chatId;

		// Stash the reply token for outbound use (single-use, 50 s TTL).
		if (chatId && replyToken) {
			this.replyTokens.set(chatId, {
				token: replyToken,
				expiresAtMs: this.nowFn() + LINE_REPLY_TOKEN_TTL_SECONDS * 1000,
			});
		}

		// Media inbound downloads binaries from api-data.line.me via the
		// INJECTED fetchContent seam (probe-computed exclusion — headless runs
		// leave it unbound and degrade to the source's failed-download
		// placeholder). When bound, bytes are cached under mediaCacheDir with
		// the source's per-type extension map and surfaced as media_urls.
		const mediaUrls: string[] = [];
		const mediaTypes: string[] = [];
		let text: string;
		if (msgType === "text") {
			text = String(msg["text"] ?? "");
		} else if (msgType === "sticker") {
			const keywords = Array.isArray(msg["keywords"])
				? (msg["keywords"] as unknown[]).map(String)
				: [];
			text =
				keywords.length > 0 ? `[sticker: ${keywords.join(", ")}]` : "[sticker]";
		} else if (msgType === "location") {
			const title = String(msg["title"] ?? "");
			const address = String(msg["address"] ?? "");
			text = `[location: ${title} ${address}]`.trim();
		} else if (
			msgType === "image" ||
			msgType === "audio" ||
			msgType === "video" ||
			msgType === "file"
		) {
			text = `[${msgType}]`;
			const cached = await this.downloadInboundMedia(
				messageId,
				msgType,
				String(msg["fileName"] ?? msg["file_name"] ?? "") || undefined,
			);
			if (cached !== null) {
				mediaUrls.push(cached.path);
				mediaTypes.push(cached.mime);
				this.inboundMediaLog.push({
					chatId,
					urls: [cached.path],
					types: [cached.mime],
				});
			}
		} else {
			text = `[unsupported message type: ${msgType}]`;
		}

		// Best-effort loading indicator — DM ONLY (LINE rejects it for groups/
		// rooms; _handle_message_event fires it right after text resolution).
		if (chatType === "dm" && chatId.startsWith("U")) {
			void this.invokeLoading(chatId).catch(() => {
				/* best-effort; never raises */
			});
		}

		this.counters.accepted += 1;
		const incoming: IncomingEvent = {
			messageType: MESSAGE_TYPES[msgType] ?? "text",
			text,
			messageId,
			...(mediaUrls.length > 0 ? { mediaUrls } : {}),
			...(mediaTypes.length > 0 ? { mediaTypes } : {}),
			source: {
				platform: LINE_PLUGIN_MANIFEST.name,
				chatType: chatType === "group" || chatType === "room" ? "group" : "dm",
				userId,
				chatId,
				chatName: chatId,
			},
		};
		this.dispatchedEvents.push({ messageId, text });
		await this.deliverInbound(incoming, `line:${chatId}`);
	}

	/**
	 * _LineClient.loading parity: U-prefix guard (LINE rejects the indicator
	 * outside DMs) + clamp, then ONE best-effort POST. Absent transport ⇒
	 * silent skip; failures never raise.
	 */
	private async invokeLoading(chatId: string): Promise<void> {
		if (!chatId || !chatId.startsWith("U")) return;
		if (this.transport?.loading === undefined) return;
		await this.transport.loading(chatId, clampLoadingSeconds(60));
	}

	/**
	 * send_typing parity (@1240): re-fire the loading-animation indicator so
	 * long runs keep a refreshed indicator instead of the single inbound-receipt
	 * beat (the source's base _keep_typing heartbeat calls this every beat
	 * while the turn processes). invokeLoading carries the DM-only U-guard,
	 * the 5..60 step-5 clamp, and best-effort swallow; absent transport is a
	 * silent skip.
	 */
	async sendTyping(chatId: string): Promise<void> {
		await this.invokeLoading(chatId);
	}

	/**
	 * _download_media parity (@1130): fetchContent bytes → cached file under
	 * mediaCacheDir with the source's per-type extension map. Returns null on
	 * ANY failure (unbound seam included) — the caller keeps the placeholder
	 * text exactly like the source's failed-download path.
	 */
	private async downloadInboundMedia(
		messageId: string,
		msgType: string,
		filenameHint?: string | undefined,
	): Promise<{ path: string; mime: string } | null> {
		if (!messageId || this.transport?.fetchContent === undefined) return null;
		const extMap: Record<string, string> = {
			image: ".jpg",
			audio: ".m4a",
			video: ".mp4",
			file: ".bin",
		};
		const ext = extMap[msgType] ?? ".bin";
		let data: Buffer;
		try {
			data = await this.transport.fetchContent(messageId);
		} catch {
			return null;
		}
		try {
			mkdirSync(this.mediaCacheDir, { recursive: true });
			if (msgType === "image") {
				const path = join(this.mediaCacheDir, `${messageId}${ext}`);
				writeFileSync(path, data);
				return { path, mime: "image/jpeg" };
			}
			if (msgType === "audio") {
				const path = join(this.mediaCacheDir, `${messageId}${ext}`);
				writeFileSync(path, data);
				return { path, mime: "audio/mp4" };
			}
			if (msgType === "video") {
				const path = join(this.mediaCacheDir, `${messageId}${ext}`);
				writeFileSync(path, data);
				return { path, mime: "video/mp4" };
			}
			const name = filenameHint || `line_file${ext}`;
			const path = join(this.mediaCacheDir, name);
			writeFileSync(path, data);
			return { path, mime: "application/octet-stream" };
		} catch {
			return null;
		}
	}

	// ── postback events (@1071 parity) ───────────────────────────────────────

	protected async handlePostbackEvent(
		event: Record<string, unknown>,
	): Promise<void> {
		const postback = asRecord(event["postback"]);
		const data = String(postback["data"] ?? "");
		const replyToken = String(event["replyToken"] ?? "");
		const { chatId } = resolveLineChat(asRecord(event["source"]));

		let parsed: Record<string, unknown>;
		try {
			const value: unknown = JSON.parse(data);
			if (value === null || typeof value !== "object" || Array.isArray(value)) {
				return;
			}
			parsed = value as Record<string, unknown>;
		} catch {
			return;
		}
		if (parsed["action"] !== "show_response") return;
		const requestId = String(parsed["request_id"] ?? "");
		if (!requestId) return;

		const entry = this.cache.get(requestId);
		if (!replyToken || entry === undefined) return;

		if (entry.state === "ready") {
			const payload = String(entry.payload ?? "");
			const chunks = splitForLine(stripMarkdownPreservingUrls(payload));
			const messages = chunks.map(textMessage);
			let sent = await this.transportReply(replyToken, messages);
			if (!sent.success) {
				// Postback reply failure falls back to push once (source ladder).
				sent = await this.transportPush(chatId, messages);
			}
			if (sent.success) {
				this.cache.markDelivered(requestId);
				this.pendingButtons.delete(chatId);
				this.counters.postbacksResolved += 1;
			}
		} else if (entry.state === "error") {
			const text = String(entry.payload ?? "") || this.interruptedText;
			const sent = await this.transportReply(replyToken, [textMessage(text)]);
			if (sent.success) {
				this.cache.markDelivered(requestId);
				this.pendingButtons.delete(chatId);
				this.counters.postbacksResolved += 1;
			}
		} else if (entry.state === "delivered") {
			await this.transportReply(replyToken, [textMessage(this.deliveredText)]);
		} else {
			// Still working — re-issue the wait notice against the fresh token.
			await this.transportReply(replyToken, [textMessage(this.pendingText)]);
		}
	}

	// ── slow-LLM postback button (_keep_typing/_fire_postback parity) ────────

	/**
	 * Deterministic threshold trigger: the source arms asyncio.sleep(threshold)
	 * inside _keep_typing; headless rows invoke THIS on the injected clock once
	 * the threshold has elapsed. Burns the stashed reply token to deliver the
	 * Template Buttons bubble and opens the PENDING cache slot.
	 */
	async fireSlowResponseButton(chatId: string): Promise<boolean> {
		if (this.slowResponseThresholdSeconds <= 0) return false;
		if (!this.replyTokens.has(chatId)) return false;
		if (this.pendingButtons.has(chatId)) return false;
		const rid = this.cache.registerPending(chatId);
		const consumed = this.consumeReplyToken(chatId);
		if (!consumed.used) {
			this.pendingButtons.delete(chatId);
			return false;
		}
		this.pendingButtons.set(chatId, rid);
		const msg = buildPostbackButtonMessage(
			this.pendingText,
			this.buttonLabel,
			rid,
		);
		const sent = await this.transportReply(consumed.token, [msg]);
		if (!sent.success) {
			this.pendingButtons.delete(chatId);
			return false;
		}
		return true;
	}

	/** interrupt_session_activity parity: orphan PENDING slots resolve ERROR. */
	interruptSessionActivity(chatId: string): void {
		const rid = this.pendingButtons.get(chatId);
		if (rid !== undefined) {
			this.cache.setError(rid, this.interruptedText);
			this.pendingButtons.delete(chatId);
		}
	}

	// ── outbound send (@1172 parity) ─────────────────────────────────────────

	sendText(chatId: string, content: string): Promise<SendResult> {
		return this.send(chatId, content);
	}

	override async send(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
		metadata?: Metadata | undefined,
	): Promise<SendResult> {
		// System busy-acks BYPASS the postback cache and route straight to
		// Reply/Push so they reach the user as visible bubbles even while a
		// button is outstanding (_is_system_bypass parity @1185-1188).
		if (isSystemBypass(content)) {
			return super.send(chatId, content, replyTo, metadata);
		}
		// A PENDING postback slot routes the response into the cache — the user
		// fetches it via tap with a FRESH free reply token (no wire call here).
		const pendingRid = this.pendingButtons.get(chatId);
		if (
			pendingRid !== undefined &&
			!content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
		) {
			this.cache.setReady(pendingRid, content);
			return { success: true, messageId: pendingRid };
		}
		// Everything else rides DOOR 1 (the audited chokepoint) like every
		// adapter — formatting ladder + §6.1 retry live in the base pipeline;
		// splitting is the ADAPTER's native split_for_line (dispatchBubbles).
		return super.send(chatId, content, replyTo, metadata);
	}

	/**
	 * THE native splitter's wire hop (split_for_line + the [:5] slice :1210):
	 * ≤5 ellipsis-capped text bubbles ride ONE Reply/Push call — reply token
	 * first, ONE push fallback on rejection. Never more than one API call per
	 * piece reaches the wire on this platform.
	 */
	private async dispatchBubbles(
		chatId: string,
		content: string,
		metadata: Record<string, unknown> = {},
	): Promise<SendResult> {
		const chunks = splitForLine(content);
		if (chunks.length === 0) return { success: true };
		const messages = chunks.map(textMessage);

		const consumed = this.consumeReplyToken(chatId);
		if (consumed.used) {
			const replyResult = await this.transportReply(
				consumed.token,
				messages,
				metadata,
			);
			if (replyResult.success) return replyResult;
			// Reply token rejected (expired/burned) → fall back to push ONCE.
			this.counters.pushFallbacks += 1;
		}
		return this.transportPush(chatId, messages, metadata);
	}

	/**
	 * _consume_reply_token parity: POP then expiry-check — the token is
	 * single-use whether or not it is still fresh.
	 */
	consumeReplyToken(chatId: string): { token: string; used: boolean } {
		const entry = this.replyTokens.get(chatId);
		if (entry === undefined) return { token: "", used: false };
		this.replyTokens.delete(chatId);
		if (!entry.token || this.nowFn() >= entry.expiresAtMs) {
			return { token: "", used: false };
		}
		return { token: entry.token, used: true };
	}

	hasStashedReplyToken(chatId: string): boolean {
		return this.replyTokens.has(chatId);
	}

	get postbackCache(): PostbackRequestCache {
		return this.cache;
	}

	get outstandingButtons(): ReadonlyMap<string, string> {
		return this.pendingButtons;
	}

	// ── transport lanes (record + fallback accounting) ───────────────────────

	private async transportReply(
		token: string,
		messages: (LineMessage | ReturnType<typeof buildPostbackButtonMessage>)[],
		metadata: Record<string, unknown> = {},
	): Promise<SendResult> {
		if (this.transport === undefined) return { success: true };
		return this.transport.reply(token, messages as LineMessage[], metadata);
	}

	private async transportPush(
		chatId: string,
		messages: (LineMessage | ReturnType<typeof buildPostbackButtonMessage>)[],
		metadata: Record<string, unknown> = {},
	): Promise<SendResult> {
		if (this.transport === undefined) return { success: true };
		return this.transport.push(chatId, messages as LineMessage[], metadata);
	}

	// ── connect ladder (@800 parity) ─────────────────────────────────────────

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (!this.configAccessToken || !this.configChannelSecret) {
			this.logger?.error?.(
				"[line] Refusing to start without LINE_CHANNEL_ACCESS_TOKEN and LINE_CHANNEL_SECRET configured",
			);
			return false;
		}
		// Best-effort bot identity fetch for self-echo filtering
		// (get_bot_user_id parity): failure fails OPEN as undefined — "the cost
		// is minor (LINE doesn't echo every message) and offline tests must not
		// block connect".
		try {
			const info = await this.transport?.botInfo?.();
			if (info?.userId) this.botUserIdValue = info.userId;
		} catch {
			/* fail-open */
		}
		return true;
	}

	override async disconnect(): Promise<void> {}

	// ── guard wiring (reference-fixture inheritance) ─────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: LINE_REGISTRY,
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
	 * DOOR transport — ONE Reply/Push call (split_for_line parity). Content
	 * converts to LINE's plain dialect FIRST (URL-preserving markdown strip)
	 * except the §6.1 plain-text fallback envelope, which carries ORIGINAL
	 * chunk bytes by contract. dispatchBubbles then caps the response at ≤5
	 * ellipsis-truncated bubbles riding reply-first-with-push-fallback ONCE.
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const formatted = content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			? content
			: stripMarkdownPreservingUrls(content);
		return this.dispatchBubbles(chatId, formatted, { ...metadata });
	}

	/**
	 * Rich lane ABSENT on the real surface (plain-text bubbles only): unless a
	 * capture wire scripted a rich probe, answer the capability-error shape
	 * WITHOUT burning a roundtrip (§10.1 latch path parity).
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
		return this.dedup.size();
	}

	hasSeenEventId(webhookEventId: string): boolean {
		return (
			this.dedup.has(`id:${webhookEventId}`) || this.dedup.has(webhookEventId)
		);
	}

	/** Test/dedup observation: the live seen-set membership probe. */
	isDuplicateEventId(webhookEventId: string): boolean {
		return !this.dedup.add(webhookEventId);
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

function nonEmpty(v: string | undefined): string | undefined {
	if (typeof v !== "string") return undefined;
	const trimmed = v.trim();
	return trimmed.length > 0 ? trimmed : undefined;
}

function normalizePath(path: string): string {
	const raw = String(path ?? "").trim() || "/";
	return raw.startsWith("/") ? raw : `/${raw}`;
}

function asRecord(v: unknown): Record<string, unknown> {
	return v !== null && typeof v === "object" && !Array.isArray(v)
		? (v as Record<string, unknown>)
		: {};
}
