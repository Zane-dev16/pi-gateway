// pi_platforms/google-chat/google-chat-adapter — THE Google Chat adapter
// (HTTP events mode), ported from the READ-ONLY Hermes plugin
// (plugins/platforms/google_chat/adapter.py) onto the kit base. Everything
// policy-shaped is inherited; this module supplies TRANSPORT (the OIDC-
// authenticated event endpoint + the Chat REST egress seam), MANIFEST DATA,
// the Chat-dialect formatter, and the cardsV2 builder.
//
// Shape (DEC-002 third column — stateless webhook):
//   - capabilities AS DATA: supports_async_delivery=False +
//     interactive_resume=False (manifest DIVERGENCE note)
//   - ingress ports verify_http_event_request + dispatch_http_event exactly:
//     bearer verification reads ONLY headers BEFORE any body parse; body caps
//     (declared length then actual bytes) run pre-parse; JSON shape failures
//     are 400; recognized/deduped/non-MESSAGE envelopes all ACK 200
//   - Pub/Sub mode, SA credential loading, attachment downloads (SSRF-guarded
//     Drive/media fetches) and the /setup-files OAuth helper stay OUT of this
//     port's transport surface: they require GCP infrastructure or user-OAuth
//     daemons Hermes itself delegates — documented probe-computed exclusions,
//     never faked green
//   - outbound ports send()/_create_message/edit_message semantics: markdown→
//     Chat dialect conversion before chunking, thread resolution ladder incl.
//     the job_id new-thread rule, messageReplyOption data on threaded sends,
//     typing-marker PATCH-in-place (no delete tombstone), 403-fatal /
//     404-skip / 429-counter verdict ladder
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
import type { SessionSource } from "../../pi_gateway/resolution/session-key.js";
import {
	GCHAT_BODY_CAP_BYTES,
	GCHAT_CAPABILITIES,
	GCHAT_MAX_TEXT_LENGTH,
	GCHAT_PLUGIN_MANIFEST,
	GCHAT_RATE_LIMIT_WARN_THRESHOLD,
	GCHAT_RETRYABLE_HTTP_STATUSES,
	validateGchatTrustBoundary,
} from "./manifest.js";
import type { GchatTrustBoundary } from "./manifest.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type { DisableReason } from "../kit/lifecycle-state.js";
import { BoundedSeenSet } from "../../pi_gateway/security/trust/index.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const GCHAT_REGISTRY: CommandRegistry = [
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

// ── configuration (__init__ @647 parity, HTTP-mode subset) ───────────────────

export interface GchatAdapterConfig {
	http_events_url?: string | undefined;
	http_events_audience?: string | undefined;
	http_events_service_account_email?: string | readonly string[] | undefined;
}

/**
 * OIDC verifier seam — Hermes delegates to google-auth's
 * id_token.verify_oauth2_token (cert fetch + signature + aud check). The
 * injected seam carries the SAME contract: throws when the token is invalid
 * for the audience; resolves the verified claims otherwise. Production binds
 * a google-auth-backed implementation; fixtures bind claims-based ones.
 */
export interface OidcTokenVerifier {
	verify(token: string, audience: string): Promise<Record<string, unknown>>;
}

/**
 * Optional RICH probe seam (subject-supplied, msgraph captureWire parity):
 * Chat REST has no native rich lane; when a harness scripts one, the §10.1
 * latch path must still OBSERVE the probe exactly once.
 */
export interface GchatCaptureWire {
	hasRichScript(opKind: string): boolean;
	transmitRich(chatId: string, content: string): Promise<SendResult>;
}

/** Chat REST egress seam (_create_message/_patch_message parity). */
export interface GchatTransport {
	createMessage(
		chatId: string,
		body: Record<string, unknown>,
		metadata?: Record<string, unknown>,
	): Promise<GchatApiResponse>;
	patchMessage(
		messageName: string,
		body: Record<string, unknown>,
		metadata?: Record<string, unknown>,
	): Promise<GchatApiResponse>;
}

/** SendResult carrying the HTTP status the verdict ladder classifies on. */
export type GchatApiResponse = SendResult & { status?: number | undefined };

export interface GchatAdapterOptions {
	config?: GchatAdapterConfig | undefined;
	secretReader?: ScopedSecretReader | undefined;
	nowMs?: (() => number) | undefined;
	scalarMaxUnits?: number | undefined;
	transport?: GchatTransport | undefined;
	verifier?: OidcTokenVerifier | undefined;
	/** Conformance-harness rich probe capture (see GchatCaptureWire). */
	captureWire?: GchatCaptureWire | undefined;
	dedupCap?: number | undefined;
}

export type HandlerResponse = {
	status: number;
	contentType?: "application/json" | "text/plain" | undefined;
	body?: string | Record<string, never> | undefined;
};

export type BearerVerdict =
	| { ok: true }
	| {
			ok: false;
			reason:
				| "google_chat_http_events_not_configured"
				| "missing_google_bearer"
				| "invalid_google_bearer"
				| "unexpected_google_bearer_identity";
	  };

// ── markdown → Chat dialect (format_message @2403 parity) ───────────────────

/** adapter.py:_RETRYABLE_HTTP_STATUSES — the outbound verdict ladder set. */
const RETRYABLE = GCHAT_RETRYABLE_HTTP_STATUSES;

const INVISIBLE_RE =
	/[\u200B\u200C\u200D\u200E\u200F\u2060\uFEFF\uFE00-\uFE0F\u{E0100}-\u{E01EF}]/gu;

/**
 * format_message parity: Chat renders `*bold*`, `_italic_`, `~strike~`, and
 * code spans only. Standard Markdown needs conversion: **bold** → *bold*,
 * ***bold italic*** → *_bold italic_*, # headers → *title*, [text](url) →
 * <url|text>. Code spans/fences are protected from transformation via
 * placeholder substitution; invisible Unicode that renders as tofu is
 * stripped at the end.
 */
export function toChatDialect(content: string): string {
	if (!content) return content;
	const placeholders = new Map<string, string>();
	let counter = 0;
	const ph = (value: string): string => {
		const key = `\x00GC${counter++}\x00`;
		placeholders.set(key, value);
		return key;
	};

	let text = content.replace(/(```(?:[^\n]*\n)?[\s\S]*?```)/g, (m) => ph(m));
	text = text.replace(/(`[^`]+`)/g, (m) => ph(m));
	text = text.replace(/^#{1,6}\s+(.+)$/gm, (_m, title: string) =>
		ph(`*${title.trim()}*`),
	);
	text = text.replace(/\*\*\*(.+?)\*\*\*/g, (_m, inner: string) =>
		ph(`*_${inner}_*`),
	);
	text = text.replace(/\*\*(.+?)\*\*/g, (_m, inner: string) =>
		ph(`*${inner}*`),
	);
	text = text.replace(
		/\[([^\]]+)\]\(([^)]+)\)/g,
		(_m, label: string, url: string) => ph(`<${url}|${label}>`),
	);
	text = text.replace(INVISIBLE_RE, "");
	text = text.replace(/ {2,}/g, " ");
	for (const [key, value] of placeholders) {
		text = text.split(key).join(value);
	}
	return text;
}

/**
 * _chunk_text parity: newline-aware split at the 4000-char budget (cut ≥
 * half-budget else hard cut). The shared-row lane chunks via the kit instead
 * (lossless with (i/n) indicators); this SOURCE-shaped splitter stays for
 * parity probes and non-kit call sites.
 */
export function chunkTextForChat(text: string): string[] {
	if (!text) return [];
	if (text.length <= GCHAT_MAX_TEXT_LENGTH) return [text];
	const chunks: string[] = [];
	let remaining = text;
	while (remaining.length > 0) {
		if (remaining.length <= GCHAT_MAX_TEXT_LENGTH) {
			chunks.push(remaining);
			break;
		}
		let cut = remaining.lastIndexOf("\n", GCHAT_MAX_TEXT_LENGTH);
		if (cut < Math.floor(GCHAT_MAX_TEXT_LENGTH / 2))
			cut = GCHAT_MAX_TEXT_LENGTH;
		chunks.push(remaining.slice(0, cut));
		remaining = remaining.slice(cut).replace(/^\s+/, "");
	}
	return chunks;
}

// ── cardsV2 builder (card_spec_to_cards_v2 @475 parity) ─────────────────────

const CARD_WIDGET_TYPES: ReadonlySet<string> = new Set([
	"text",
	"text_paragraph",
	"decorated_text",
	"buttons",
	"button_list",
	"selection",
	"selection_input",
	"image",
	"divider",
]);

function requiredStr(
	mapping: Record<string, unknown>,
	key: string,
	context: string,
): string {
	const value = mapping[key];
	if (value === null || value === undefined) {
		throw new Error(`${context}.${key} is required`);
	}
	const text = String(value).trim();
	if (!text) throw new Error(`${context}.${key} is required`);
	return text;
}

function buttonToChat(
	button: Record<string, unknown>,
): Record<string, unknown> {
	const text = requiredStr(button, "text", "button");
	const action = requiredStr(button, "action", "button");
	const rawParams = button["parameters"];
	if (
		rawParams !== null &&
		typeof rawParams === "object" &&
		Array.isArray(rawParams)
	) {
		throw new Error("button.parameters must be an object");
	}
	const paramsObj =
		rawParams !== null &&
		typeof rawParams === "object" &&
		!Array.isArray(rawParams)
			? (rawParams as Record<string, unknown>)
			: {};
	const parameters = Object.keys(paramsObj)
		.sort()
		.map((key) => ({ key, value: String(paramsObj[key]) }));
	return {
		text,
		onClick: { action: { function: action, parameters } },
	};
}

function widgetToChat(widget: unknown): Record<string, unknown> {
	if (widget === null || typeof widget !== "object" || Array.isArray(widget)) {
		throw new Error("card widgets must be objects");
	}
	const w = widget as Record<string, unknown>;
	const widgetType = String(w["type"] ?? "").trim();
	if (!CARD_WIDGET_TYPES.has(widgetType)) {
		throw new Error(`unsupported widget type: ${widgetType || "<missing>"}`);
	}
	if (widgetType === "text" || widgetType === "text_paragraph") {
		return {
			textParagraph: {
				text: toChatDialect(requiredStr(w, "text", "widget")),
			},
		};
	}
	if (widgetType === "decorated_text") {
		const decorated: Record<string, unknown> = {
			text: toChatDialect(requiredStr(w, "text", "widget")),
			wrapText: w["wrap_text"] !== false,
		};
		if (w["top_label"]) decorated["topLabel"] = String(w["top_label"]);
		if (w["bottom_label"]) decorated["bottomLabel"] = String(w["bottom_label"]);
		return { decoratedText: decorated };
	}
	if (widgetType === "divider") return { divider: {} };
	if (widgetType === "image") {
		const image: Record<string, unknown> = {
			imageUrl: requiredStr(w, "image_url", "widget"),
		};
		if (w["alt_text"]) image["altText"] = String(w["alt_text"]);
		return { image };
	}
	if (widgetType === "buttons" || widgetType === "button_list") {
		const rawButtons = w["buttons"];
		if (!Array.isArray(rawButtons) || rawButtons.length === 0) {
			throw new Error("button widgets require at least one button");
		}
		return {
			buttonList: {
				buttons: rawButtons.map((b) =>
					buttonToChat(b as Record<string, unknown>),
				),
			},
		};
	}
	// selection / selection_input
	const name = requiredStr(w, "name", "widget");
	const rawItems = w["items"];
	if (!Array.isArray(rawItems) || rawItems.length === 0) {
		throw new Error("selection widgets require at least one item");
	}
	const items = rawItems.map((item) => {
		if (item === null || typeof item !== "object" || Array.isArray(item)) {
			throw new Error("selection items must be objects");
		}
		const rec = item as Record<string, unknown>;
		return {
			text: requiredStr(rec, "text", "selection item"),
			value: requiredStr(rec, "value", "selection item"),
			selected: rec["selected"] === true,
		};
	});
	return {
		selectionInput: {
			name,
			label: String(w["label"] ?? name),
			type: String(w["selection_type"] ?? "CHECK_BOX"),
			items,
		},
	};
}

/** card_spec_to_cards_v2 parity — raises on unsupported shapes (caller maps
 * the failure to a clean SendResult; send_clarify falls back to plain text). */
export function cardSpecToCardsV2(cardSpec: unknown): Record<string, unknown> {
	if (
		cardSpec === null ||
		typeof cardSpec !== "object" ||
		Array.isArray(cardSpec)
	) {
		throw new Error("card must be an object");
	}
	const spec = cardSpec as Record<string, unknown>;
	const rawSections = spec["sections"];
	if (!Array.isArray(rawSections) || rawSections.length === 0) {
		throw new Error("card.sections must contain at least one section");
	}
	const sections = rawSections.map((section) => {
		if (
			section === null ||
			typeof section !== "object" ||
			Array.isArray(section)
		) {
			throw new Error("card sections must be objects");
		}
		const s = section as Record<string, unknown>;
		const widgets = s["widgets"];
		if (!Array.isArray(widgets) || widgets.length === 0) {
			throw new Error("card section widgets must contain at least one widget");
		}
		const rendered: Record<string, unknown> = {
			widgets: widgets.map((w) => widgetToChat(w)),
		};
		if (s["header"]) rendered["header"] = String(s["header"]);
		return rendered;
	});

	const card: Record<string, unknown> = { sections };
	const header = spec["header"];
	if (header) {
		if (
			header === null ||
			typeof header !== "object" ||
			Array.isArray(header)
		) {
			throw new Error("card.header must be an object");
		}
		const h = header as Record<string, unknown>;
		const renderedHeader: Record<string, unknown> = {
			title: requiredStr(h, "title", "card.header"),
		};
		if (h["subtitle"]) renderedHeader["subtitle"] = String(h["subtitle"]);
		if (h["image_url"]) {
			renderedHeader["imageUrl"] = String(h["image_url"]);
			renderedHeader["imageType"] = String(h["image_type"] ?? "SQUARE");
		}
		if (h["image_alt_text"]) {
			renderedHeader["imageAltText"] = String(h["image_alt_text"]);
		}
		card["header"] = renderedHeader;
	}
	return {
		cardId: String(spec["card_id"] ?? "hermes-card"),
		card,
	};
}

// ── envelope extraction (_extract_message_payload @1255 parity) ─────────────

export type ExtractedPayload = {
	message: Record<string, unknown>;
	space: Record<string, unknown>;
	format: "workspace_addons" | "native_chat_api" | "relay_flat";
};

/**
 * Detect the three accepted envelope formats; None ⇒ silently dropped (ack).
 * Format 1 Workspace Add-ons (chat.messagePayload), Format 2 native Chat API
 * Pub/Sub (type=MESSAGE + message), Format 3 relay/flat (event_type/sender_email
 * fields; synthesizes a Chat-API-shaped message; honors sender_type so
 * bot-forwarded replies still self-filter).
 */
export function extractMessagePayload(
	envelope: Record<string, unknown>,
): ExtractedPayload | null {
	const chatBlock =
		envelope["chat"] !== null && typeof envelope["chat"] === "object"
			? (envelope["chat"] as Record<string, unknown>)
			: {};

	const wrapper =
		chatBlock !== undefined &&
		typeof chatBlock.messagePayload === "object" &&
		chatBlock.messagePayload !== null
			? (chatBlock.messagePayload as Record<string, unknown>)
			: null;
	if (wrapper !== null) {
		const msg = asRecord(wrapper["message"]);
		const space =
			asRecord(wrapper["space"]) !== undefined &&
			Object.keys(asRecord(wrapper["space"])).length > 0
				? asRecord(wrapper["space"])
				: asRecord(msg["space"]);
		return { message: msg, space, format: "workspace_addons" };
	}

	if (
		envelope["message"] !== null &&
		typeof envelope["message"] === "object" &&
		!Array.isArray(envelope["message"])
	) {
		if (String(envelope["type"] ?? "") !== "MESSAGE") return null;
		const msg = envelope["message"] as Record<string, unknown>;
		const space =
			Object.keys(asRecord(envelope["space"])).length > 0
				? asRecord(envelope["space"])
				: asRecord(msg["space"]);
		return { message: msg, space, format: "native_chat_api" };
	}

	if ("event_type" in envelope || "sender_email" in envelope) {
		if (String(envelope["event_type"] ?? "MESSAGE") !== "MESSAGE") return null;
		const senderEmail = String(envelope["sender_email"] ?? "").trim();
		const senderDisplay = String(
			envelope["sender_display_name"] ?? senderEmail ?? "Unknown",
		);
		const surrogate = `users/relay-${(senderEmail || "unknown")
			.replace("@", "_at_")
			.replace(/\./g, "_")}`;
		let senderType = String(envelope["sender_type"] ?? "HUMAN")
			.trim()
			.toUpperCase();
		if (senderType !== "HUMAN" && senderType !== "BOT") senderType = "HUMAN";
		const text = String(envelope["text"] ?? "");
		const msg: Record<string, unknown> = {
			name: String(envelope["message_name"] ?? ""),
			sender: {
				name: surrogate,
				email: senderEmail,
				displayName: senderDisplay,
				type: senderType,
			},
			text,
			argumentText: text,
		};
		const threadName = String(envelope["thread_name"] ?? "");
		if (threadName) msg["thread"] = { name: threadName };
		const space = {
			name: String(envelope["space_name"] ?? ""),
			spaceType: String(envelope["space_type"] ?? "SPACE"),
		};
		return { message: msg, space, format: "relay_flat" };
	}

	return null;
}

// ── THE adapter ──────────────────────────────────────────────────────────────

export class GoogleChatWebhookAdapter extends BasePlatformAdapter {
	readonly pluginManifest = GCHAT_PLUGIN_MANIFEST;
	readonly trustBoundary: GchatTrustBoundary;

	private readonly secretReader: ScopedSecretReader;
	private readonly nowFn: () => number;
	private readonly transport: GchatTransport | undefined;
	private readonly verifier: OidcTokenVerifier | undefined;
	private readonly captureWire: GchatCaptureWire | undefined;

	readonly httpEventsUrl: string;
	readonly httpEventsAudience: string;
	readonly saAllowlist: ReadonlySet<string>;

	private readonly dedup: BoundedSeenSet;
	private readonly lastInboundThread = new Map<string, string>();
	private readonly lastSenderByChat = new Map<string, string>();

	readonly counters = {
		accepted: 0,
		duplicates: 0,
		botSelfFiltered: 0,
		unrecognizedEnvelopes: 0,
		authRejected: 0,
		tooLarge: 0,
		badJson: 0,
		parseInvocations: 0,
		rateLimitHitsByChatWarned: 0,
	};
	private readonly rateLimitHits = new Map<string, number>();

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

	constructor(opts: GchatAdapterOptions = {}) {
		const config = opts.config ?? {};
		super({
			manifestName: GCHAT_PLUGIN_MANIFEST.name,
			capabilities: GCHAT_CAPABILITIES,
			scalarMaxUnits: opts.scalarMaxUnits ?? GCHAT_MAX_TEXT_LENGTH,
		});
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.transport = opts.transport;
		this.verifier = opts.verifier;
		this.captureWire = opts.captureWire;

		// HTTP-events config: extra first (Hermes parity), scoped secret second.
		this.httpEventsUrl = nonEmpty(
			config.http_events_url ??
				this.secretReader("GOOGLE_CHAT_HTTP_EVENTS_URL"),
		);
		this.httpEventsAudience = nonEmpty(
			config.http_events_audience ??
				this.secretReader("GOOGLE_CHAT_HTTP_EVENTS_AUDIENCE"),
		);
		const saRaw =
			config.http_events_service_account_email ??
			this.secretReader("GOOGLE_CHAT_HTTP_EVENTS_SERVICE_ACCOUNT_EMAIL") ??
			"";
		const saList = Array.isArray(saRaw) ? saRaw : String(saRaw).split(",");
		this.saAllowlist = new Set(
			saList.map((e) => e.trim().toLowerCase()).filter(Boolean),
		);

		this.trustBoundary =
			GCHAT_PLUGIN_MANIFEST.trustBoundary as GchatTrustBoundary;
		const boundaryErrors = validateGchatTrustBoundary(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			const reason: DisableReason = {
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			};
			this.lifecycle.disable(reason);
		}

		// §11 step 3/4: missing required secrets ⇒ LOUD disable.
		const enablement = resolveEnablement(
			GCHAT_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		// helpers.py MessageDeduplicator shape: TTL-bounded seen-set.
		this.dedup = new BoundedSeenSet({
			maxEntries: Math.max(1, opts.dedupCap ?? 2000),
			ttlMs: 300_000,
			nowMs: this.nowFn,
		});

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // REST create/patch; no native lanes
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			transmitEdit: async (chatId, messageId, content) =>
				this.wireEdit(chatId, messageId, content, { finalize: false }),
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

	// ── bearer verification (verify_http_event_request @1520 parity) ─────────

	/**
	 * Reads ONLY the Authorization header — never the body — so the verdict
	 * lands BEFORE any parse work. Reasons mirror the source verbatim:
	 * not-configured / missing / invalid / unexpected identity. The verifier
	 * seam is awaited exactly like Hermes awaits google-auth's cert-backed
	 * verification.
	 */
	async verifyHttpEventRequest(
		authHeader: string | undefined,
	): Promise<BearerVerdict> {
		if (!this.httpEventsAudience || this.saAllowlist.size === 0) {
			return { ok: false, reason: "google_chat_http_events_not_configured" };
		}
		if (authHeader === undefined || !authHeader.startsWith("Bearer ")) {
			return { ok: false, reason: "missing_google_bearer" };
		}
		const token = authHeader.slice(7).trim();
		if (!token || this.verifier === undefined) {
			return {
				ok: false,
				reason: token ? "invalid_google_bearer" : "missing_google_bearer",
			};
		}
		try {
			const claims = await this.verifier.verify(token, this.httpEventsAudience);
			const claimEmail = String(claims["email"] ?? "")
				.trim()
				.toLowerCase();
			if (!claimEmail || !this.saAllowlist.has(claimEmail)) {
				return { ok: false, reason: "unexpected_google_bearer_identity" };
			}
			return { ok: true };
		} catch {
			return { ok: false, reason: "invalid_google_bearer" };
		}
	}

	// ── HTTP handler surface ─────────────────────────────────────────────────

	/**
	 * Order ports the deployment front exactly: bearer gate (headers only) →
	 * declared-length cap → actual-bytes cap → JSON parse → dispatch. Auth
	 * status mapping (proposed DEC): 503 not-configured, 401 missing/invalid,
	 * 403 wrong identity — Hermes returns named reasons to the Cloud Run front,
	 * which owns the mapping; the port pins ONE mapping so rows can assert it.
	 */
	async handleHttpEventPost(input: {
		headers?: Record<string, string> | undefined;
		rawBody: Buffer;
	}): Promise<HandlerResponse> {
		const headers = lowerKeys(input.headers);
		const verdict = await this.verifyHttpEventRequest(headers["authorization"]);
		if (!verdict.ok) {
			this.counters.authRejected += 1;
			const status =
				verdict.reason === "google_chat_http_events_not_configured"
					? 503
					: verdict.reason === "unexpected_google_bearer_identity"
						? 403
						: 401;
			return { status, contentType: "text/plain", body: verdict.reason };
		}

		const cap = GCHAT_BODY_CAP_BYTES;
		const declaredRaw = headers["content-length"];
		const declaredLength =
			declaredRaw !== undefined && /^\d+$/.test(declaredRaw)
				? Number(declaredRaw)
				: null;
		if (declaredLength !== null && declaredLength > cap) {
			this.counters.tooLarge += 1;
			return { status: 413 };
		}
		if (input.rawBody.length > cap) {
			this.counters.tooLarge += 1;
			return { status: 413 };
		}

		const parsed = this.parseJsonBody(input.rawBody);
		if (!parsed.ok) {
			this.counters.badJson += 1;
			return { status: 400 };
		}
		const envelope = parsed.value;
		if (
			envelope === null ||
			typeof envelope !== "object" ||
			Array.isArray(envelope)
		) {
			this.counters.badJson += 1;
			return { status: 400 };
		}

		await this.dispatchHttpEvent(envelope as Record<string, unknown>);
		// Google expects a fast 200 or it retries; every acked outcome —
		// dispatched, deduped, BOT-filtered, unrecognized — answers 200 {}.
		return { status: 200, contentType: "application/json", body: {} };
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

	// ── dispatch (dispatch_http_event @1494 parity) ──────────────────────────

	async dispatchHttpEvent(envelope: Record<string, unknown>): Promise<void> {
		const extracted = extractMessagePayload(envelope);
		if (extracted === null) {
			this.counters.unrecognizedEnvelopes += 1;
			return;
		}
		const { message, space, format } = extracted;
		const sender = asRecord(message["sender"]);
		if (String(sender["type"] ?? "") === "BOT") {
			this.counters.botSelfFiltered += 1;
			return;
		}
		const msgName = String(message["name"] ?? "");
		if (msgName && !this.dedup.add(msgName)) {
			this.counters.duplicates += 1;
			return;
		}
		await this.dispatchMessage(message, space, format);
	}

	private async dispatchMessage(
		msg: Record<string, unknown>,
		space: Record<string, unknown>,
		format: ExtractedPayload["format"],
	): Promise<void> {
		try {
			const event = this.buildMessageEvent(msg, space);
			this.counters.accepted += 1;
			this.dispatchedEvents.push({
				messageId: String(msg["name"] ?? ""),
				text: event.text ?? "",
			});
			await this.deliverInbound(event, sessionKeyFor(event));
		} catch {
			/* containment parity: one poisoned payload never rejects the batch */
		}
		void format;
	}

	// ── event build (_build_message_event @1813 parity, transport subset) ────

	protected buildMessageEvent(
		msg: Record<string, unknown>,
		spaceEnvelope: Record<string, unknown>,
	): IncomingEvent {
		const space =
			Object.keys(asRecord(msg["space"])).length > 0
				? asRecord(msg["space"])
				: spaceEnvelope;
		const spaceName = String(space["name"] ?? ""); // "spaces/XXX" | "users/X"
		const spaceType = String(
			space["type"] ?? space["spaceType"] ?? "",
		).toUpperCase();
		const threadName = String(asRecord(msg["thread"])["name"] ?? "");
		const sender = asRecord(msg["sender"]);
		const senderName = String(sender["name"] ?? "");
		const senderDisplay = String(
			sender["displayName"] ?? sender["email"] ?? senderName,
		);
		const senderEmail = String(sender["email"] ?? "");

		if (senderEmail && spaceName) {
			this.lastSenderByChat.set(spaceName, senderEmail.trim().toLowerCase());
		}

		const chatType =
			spaceType === "DIRECT_MESSAGE" || spaceType === "DM" ? "dm" : "group";
		const text = String(msg["argumentText"] ?? msg["text"] ?? "").trim();

		// Attachments download through Google-owned hosts with SA credentials —
		// INJECTED seam left unexercised headlessly (probe-computed exclusion).

		// Session-thread routing for DMs: FIRST sight of a thread = main flow
		// (no thread isolation); later sights = side thread (isolate + reply
		// in-thread). Groups always isolate and always reply in-thread.
		let sessionThreadId: string | undefined;
		if (chatType === "dm") {
			const seenBefore = threadName
				? this.threadSeen(spaceName, threadName)
				: false;
			sessionThreadId = seenBefore ? threadName || undefined : undefined;
			if (threadName && seenBefore) {
				this.lastInboundThread.set(spaceName, threadName);
			} else {
				this.lastInboundThread.delete(spaceName);
			}
		} else {
			sessionThreadId = threadName || undefined;
			if (threadName) this.lastInboundThread.set(spaceName, threadName);
		}
		this.markThreadSeen(spaceName, threadName);

		const source: SessionSource = {
			platform: GCHAT_PLUGIN_MANIFEST.name,
			chatType: chatType === "dm" ? "dm" : "group",
			userId: senderEmail || senderName,
			chatId: spaceName,
			chatName: String(space["displayName"] ?? spaceName),
		};
		if (senderName) source.userIdAlt = senderName;
		if (sessionThreadId !== undefined && sessionThreadId !== "") {
			source.threadId = sessionThreadId;
		}
		return {
			messageType: "text",
			text,
			messageId: String(msg["name"] ?? ""),
			source,
		};
	}

	// ── thread resolution (_resolve_thread_id @2482 parity) ──────────────────

	resolveThreadId(
		replyTo: string | undefined,
		metadata: Metadata | undefined,
		chatId?: string,
	): string | null {
		if (metadata) {
			for (const key of ["thread_id", "thread_name", "thread_ts"]) {
				const value = metadata[key];
				if (typeof value === "string" && value) return value;
			}
		}
		if (
			replyTo &&
			replyTo.includes("/threads/") &&
			!replyTo.includes("/messages/")
		) {
			return replyTo;
		}
		// Cron deliveries post TOP-LEVEL unless a thread was explicitly pinned.
		if (metadata && metadata["job_id"]) return null;
		if (chatId) {
			const cached = this.lastInboundThread.get(chatId);
			if (cached) return cached;
		}
		return null;
	}

	/**
	 * Per-chat length descriptor (§6.3/A15 relay-shaped override point): the
	 * harness's utf16-marked chats return budget AND unit TOGETHER; production
	 * chats return undefined ⇒ manifest default (GCHAT_MAX_TEXT_LENGTH).
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

	// ── connect ladder (@985 parity, HTTP mode) ──────────────────────────────

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (!this.httpEventsAudience || this.saAllowlist.size === 0) {
			this.logger?.error?.(
				"[GoogleChat] HTTP events mode requires GOOGLE_CHAT_HTTP_EVENTS_AUDIENCE and GOOGLE_CHAT_HTTP_EVENTS_SERVICE_ACCOUNT_EMAIL",
			);
			return false;
		}
		return true;
	}

	override async disconnect(): Promise<void> {}

	// ── guard wiring (reference-fixture inheritance) ─────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: GCHAT_REGISTRY,
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
	 * DOOR transport — send() parity: convert to the Chat dialect FIRST
	 * (except the §6.1 fallback envelope, which carries ORIGINAL bytes), then
	 * ride ONE create call per kit-chunked piece. Thread resolution follows
	 * _resolve_thread_id; threaded creates carry messageReplyOption data.
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const formatted = content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			? content
			: toChatDialect(content);
		if (!formatted) return { success: false, error: "empty message" };

		const threadId = this.resolveThreadId(undefined, metadata, chatId);
		const body: Record<string, unknown> = { text: formatted };
		if (threadId) {
			body["thread"] = { name: threadId };
			// _create_message parity: without REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD
			// Chat silently ignores thread.name and starts a fresh thread.
			body["messageReplyOption"] = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD";
		}
		if (this.transport === undefined) return { success: true };
		const resp = await this.transport.createMessage(chatId, body, {
			...metadata,
		});
		// Verdict ladder (@2098 parity): 403 ⇒ FATAL chat_forbidden (removed
		// from space / perms revoked); 429 bumps the per-chat rate counter and
		// surfaces retryable; retryable-status classes ride SendResult.retryable.
		if (!resp.success) {
			const status = resp.status;
			if (status === 403) {
				this.lifecycle.markFatal({
					kind: "config_invalid",
					detail:
						"chat_forbidden: Bot lacks access (removed from space or perms revoked)",
				});
				return {
					success: false,
					error: resp.error ?? "HTTP 403",
					retryable: false,
				};
			}
			if (status === 404) {
				return { success: false, error: "target not found", retryable: false };
			}
			if (status === 429) this.noteRateLimitHit(chatId);
			return {
				success: false,
				error: resp.error ?? `HTTP ${status ?? 0}`,
				retryable: status !== undefined && RETRYABLE.has(status),
			};
		}
		return resp;
	}

	/**
	 * edit_message parity: messages.patch rewrites text IN PLACE (no delete
	 * tombstone); oversized edits truncate at the 4000-char cap with ellipsis.
	 */
	protected override async wireEdit(
		_chatId: string,
		messageId: string,
		content: string,
		_opts: { finalize: boolean },
	): Promise<SendResult> {
		if (!messageId) return { success: false, error: "missing message_id" };
		const capped =
			content.length > GCHAT_MAX_TEXT_LENGTH
				? `${content.slice(0, GCHAT_MAX_TEXT_LENGTH - 1)}…`
				: content;
		if (this.transport === undefined) return { success: true };
		return this.transport.patchMessage(messageId, { text: capped });
	}

	/**
	 * Rich lane ABSENT on the real surface (REST create/patch only): answer the
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

	/** send_card parity: cardsV2 create with clean failure mapping. */
	async sendCard(
		chatId: string,
		cardSpec: unknown,
		metadata?: Metadata | undefined,
	): Promise<SendResult> {
		let cardsV2: Record<string, unknown>;
		try {
			cardsV2 = cardSpecToCardsV2(cardSpec);
		} catch (err) {
			return {
				success: false,
				error: err instanceof Error ? err.message : String(err),
			};
		}
		if (this.transport === undefined) return { success: true };
		const threadId = this.resolveThreadId(undefined, metadata, chatId);
		const body: Record<string, unknown> = { cardsV2: [cardsV2] };
		if (threadId) {
			body["thread"] = { name: threadId };
			body["messageReplyOption"] = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD";
		}
		return this.transport.createMessage(chatId, body, { ...(metadata ?? {}) });
	}

	/** send_typing parity: a visible marker message tracked for patch-in-place. */
	async sendTyping(
		chatId: string,
		metadata?: Metadata | undefined,
	): Promise<SendResult> {
		if (this.transport === undefined) return { success: true };
		const threadId = this.resolveThreadId(undefined, metadata, chatId);
		const body: Record<string, unknown> = { text: "Hermes is thinking…" };
		if (threadId) {
			body["thread"] = { name: threadId };
			body["messageReplyOption"] = "REPLY_MESSAGE_FALLBACK_TO_NEW_THREAD";
		}
		return this.transport.createMessage(chatId, body, { ...(metadata ?? {}) });
	}

	/** 429 counter ladder (@2118 parity): warn threshold observable. */
	noteRateLimitHit(chatId: string): void {
		const hits = (this.rateLimitHits.get(chatId) ?? 0) + 1;
		this.rateLimitHits.set(chatId, hits);
		if (hits >= GCHAT_RATE_LIMIT_WARN_THRESHOLD) {
			this.counters.rateLimitHitsByChatWarned += 1;
		}
	}

	rateLimitHitsOf(chatId: string): number {
		return this.rateLimitHits.get(chatId) ?? 0;
	}

	// ── observability ─────────────────────────────────────────────────────────

	seenDedupSize(): number {
		return this.dedup.size();
	}

	hasSeenMessageName(name: string): boolean {
		return this.dedup.has(name);
	}

	lastSenderFor(chatId: string): string | undefined {
		return this.lastSenderByChat.get(chatId);
	}

	cachedOutboundThread(chatId: string): string | undefined {
		return this.lastInboundThread.get(chatId);
	}

	// thread-count store parity (in-memory here; persistence is disk state the
	// headless port keeps abstract behind markThreadSeen/threadSeen).
	private readonly threadCounts = new Map<string, number>();

	private threadSeen(spaceName: string, threadName: string): boolean {
		return (this.threadCounts.get(`${spaceName}\u0000${threadName}`) ?? 0) > 0;
	}

	private markThreadSeen(spaceName: string, threadName: string): void {
		if (!threadName || !spaceName) return;
		const key = `${spaceName}\u0000${threadName}`;
		this.threadCounts.set(key, (this.threadCounts.get(key) ?? 0) + 1);
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

function nonEmpty(v: string | undefined): string {
	if (typeof v !== "string") return "";
	const trimmed = v.trim();
	return trimmed.length > 0 ? trimmed : "";
}

function asRecord(v: unknown): Record<string, unknown> {
	if (v === null || typeof v !== "object" || Array.isArray(v)) return {};
	return v as Record<string, unknown>;
}

/** Session key mirrors gateway/session.py build_source usage for this shape:
 * groups isolate by chat+thread; DM main-flow keys by chat alone. */
function sessionKeyFor(event: IncomingEvent): string {
	const src = event.source;
	const thread = src?.threadId;
	if (src?.chatType !== "dm" && thread) {
		return `gchat:${src.chatId}:${thread}`;
	}
	return `gchat:${src?.chatId ?? "unknown"}`;
}
