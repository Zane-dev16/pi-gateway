// pi_platforms/whatsapp-cloud/wa-cloud-adapter — THE WhatsApp Cloud API
// adapter (Meta WhatsApp Business Platform), ported from the READ-ONLY Hermes
// built-in gateway/platforms/whatsapp_cloud.py + whatsapp_common.py onto the
// kit base. Everything policy-shaped is inherited; this module supplies
// TRANSPORT (Graph API edges over the WaCloudTransport seam) and MANIFEST DATA.
//
// Shape (DEC-002 third column — stateless webhook):
//   - capabilities AS DATA: supports_async_delivery=False +
//     interactive_resume=False; splits_long_messages=True (native splitting in
//     wireSend, quote-on-first-chunk parity)
//   - NO draft streaming / NO edits: reply-only egress — supportsDraftStreaming
//     stays the base default false, reconcile falls back to plain send
//   - DEC-017 trust boundary as manifest data; signature verification runs
//     through the KIT trust engine configured with WA scheme data
//     (X-Hub-Signature-256: sha256=<hex> HMAC of RAW body, constant-time)
//   - ingress pipeline (order ports whatsapp_cloud.py:_handle_webhook): body
//     cap 413 → app_secret-unset 503 → signature 401 → parse 400 → envelope
//     walk → wamid dedup → interactive claim → gating → guard ingress;
//     verified deliveries ALWAYS answer 200 (Meta retries non-200 for up to
//     7 days — a transient dispatch bug must not multiply downstream work)
//   - statuses[] callbacks are logged/counted, NEVER dispatched as turns
//   - messaging-window classification recorded per outbound send
//     (window-policy.ts); outsideWindowPolicy "record" (Hermes best-effort
//     parity) or "refuse" (hard template-required gate) as declared data
//
// Layering: imports pi_gateway downward + kit same-layer ONLY; no adapter
// cross-imports.

import { basename, extname, join } from "node:path";
import { mkdirSync, readFileSync, statSync, writeFileSync } from "node:fs";

import {
	ActionHandlerRegistry,
	BasePlatformAdapter,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	resolveEnablement,
	secureCompare,
	validateTrustBoundaryManifest,
} from "../kit/index.js";
import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	MessageType,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import { buildSessionKey } from "../../pi_gateway/resolution/session-key.js";
import { normalizeWhatsappIdentifier } from "../../pi_gateway/resolution/whatsapp-identity.js";
import { verifyHmacSignature } from "../kit/trust.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type {
	AdapterStatusSnapshot,
	DisableReason,
} from "../kit/lifecycle-state.js";
import { chunkWithFenceCarry } from "../kit/chunking.js";
import type { LengthUnit } from "../kit/length-policy.js";
import { PLAIN_TEXT_FALLBACK_PREFIX } from "../kit/send-retry.js";

import {
	BUTTON_TITLE_CAP,
	CAPTION_KINDS,
	DEFAULT_MEDIA_MIME,
	INTERACTIVE_BODY_CAP,
	LIST_ROW_TITLE_CAP,
	MAX_LIST_ROWS,
	MAX_QUICK_BUTTONS,
	MAX_TEXT_INJECT_BYTES,
	MEDIA_ID_SAFE_RE,
	MEDIA_SIZE_LIMITS,
	TEXT_INJECT_EXTENSIONS,
	VOICE_NOTE_MIME,
	WA_MAX_MESSAGE_LENGTH,
	WAMID_DEDUP_CACHE_SIZE,
	WHATSAPP_CLOUD_PLUGIN_MANIFEST,
	whatsAppCloudTrustBoundary,
	resolveMediaExtension,
	tryResolveMediaExtension,
	type WaMediaKind,
} from "./manifest.js";
import type {
	GraphResponse,
	WaCloudTransport,
	WaMediaUploadInput,
} from "./graph-wire.js";
import { MessagingWindowClassifier } from "./window-policy.js";
import { toWhatsappMarkup } from "./wa-markdown.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const WA_CLOUD_REGISTRY: CommandRegistry = [
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

export interface WaCloudAdapterOptions {
	transport: WaCloudTransport;
	/** Scoped reader over WHATSAPP_CLOUD_* names (fail-closed; DEC-003/009). */
	secretReader?: ScopedSecretReader | undefined;
	/** Injected LID-mapping dir (tests: mkdtemp; default resolves via PI_HOME). */
	whatsappSessionDir?: string | undefined;
	/** Inbound media cache root (tests: mkdtemp). */
	mediaCacheDir?: string | undefined;
	dedupCap?: number | undefined;
	interactiveStateCap?: number | undefined;
	scalarMaxUnits?: number | undefined;
	nowMs?: (() => number) | undefined;
	spawner?: TaskSpawner | undefined;
	/**
	 * Outside-window egress stance (manifest-declared class): "record" keeps
	 * Hermes' best-effort send behavior while AUDITING the session/template
	 * decision; "refuse" turns the decision into a hard pre-wire gate.
	 */
	outsideWindowPolicy?: "record" | "refuse" | undefined;
	dmAllowFrom?: readonly string[] | undefined;
	/**
	 * MP3→opus transcoder seam (send_voice/_convert_to_opus upstream shells
	 * out to ffmpeg when present). Given the source MP3 bytes it returns the
	 * converted opus-in-Ogg bytes (any temp artifact lifecycle is the
	 * converter's own), or null on missing tool/failure. Default: undefined ⇒
	 * the documented no-ffmpeg fallback path — voice MP3s ship verbatim as
	 * audio/mpeg attachments; no OS child is ever spawned from this port.
	 */
	transcodeMp3ToOpus?:
		| ((
				bytes: Buffer,
				filename: string,
		  ) => Promise<{ bytes: Buffer; filename: string } | null>)
		| undefined;
}

export interface ReceiptRecord {
	chatId: string;
	wamid: string;
	ok: boolean;
	rejectedCode?: number | undefined;
}

/**
 * The kit-built WhatsApp Cloud adapter.
 * Hermes anchors: whatsapp_cloud.py:WhatsAppCloudAdapter,
 * whatsapp_common.py:WhatsAppBehaviorMixin (gating/formatting semantics).
 */
export class WaCloudAdapter extends BasePlatformAdapter {
	readonly pluginManifest = WHATSAPP_CLOUD_PLUGIN_MANIFEST;
	readonly trustBoundary = whatsAppCloudTrustBoundary();
	readonly transport: WaCloudTransport;

	// Interactive surfaces (kit-owned, ONE router per adapter).
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore(); // appr:<id>:approve|deny family
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	readonly classifier: MessagingWindowClassifier;

	private readonly cp: EgressChokepoint;
	private readonly secretReader: ScopedSecretReader;
	private readonly nowFn: () => number;
	private readonly dedupCap: number;
	private readonly outsideWindowPolicy: "record" | "refuse";
	private readonly dmAllowSet: Set<string>;
	private readonly transcodeMp3ToOpus:
		| ((
				bytes: Buffer,
				filename: string,
		  ) => Promise<{ bytes: Buffer; filename: string } | null>)
		| undefined;
	readonly whatsappSessionDir: string | undefined;
	readonly mediaCacheDir: string;

	// ── webhook-plane state (whatsapp_cloud.py __init__ parity) ──────────────
	private readonly seenWamids = new Set<string>();
	private readonly lastInboundWamidByChat = new Map<string, string>();
	private readonly sentTextByChatWamid = new Map<
		string,
		{ chatId: string; text: string }
	>();

	readonly counters = {
		accepted: 0,
		duplicates: 0,
		rejectedSignature: 0,
		statusesSeen: 0,
		refusedGroupShaped: 0,
		windowRefusals: 0,
	};

	readonly receipts: ReceiptRecord[] = [];
	/** Observability: media refs attached to accepted inbound events. */
	readonly inboundMediaLog: Array<{
		chatId: string;
		urls: string[];
		types: string[];
	}> = [];
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly resolvedFamilies: string[] = [];

	private allowAllClickers = true;
	private readonly clarifyArmedSet = new Set<string>();
	private readonly replyWaiters = new Map<
		string,
		Array<(reply: string | null) => void>
	>();
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};
	private holding = false;

	constructor(opts: WaCloudAdapterOptions) {
		super({
			manifestName: WHATSAPP_CLOUD_PLUGIN_MANIFEST.name,
			capabilities: WHATSAPP_CLOUD_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? WA_MAX_MESSAGE_LENGTH,
		});
		this.transport = opts.transport;
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.dedupCap = opts.dedupCap ?? WAMID_DEDUP_CACHE_SIZE;
		this.outsideWindowPolicy = opts.outsideWindowPolicy ?? "record";
		this.transcodeMp3ToOpus = opts.transcodeMp3ToOpus;
		this.whatsappSessionDir = opts.whatsappSessionDir;
		this.mediaCacheDir =
			opts.mediaCacheDir ??
			join(process.cwd(), "platforms", "whatsapp_cloud", "media");
		this.dmAllowSet = new Set(
			(opts.dmAllowFrom ?? []).map((e) => normalizeWhatsappIdentifier(e)),
		);

		this.classifier = new MessagingWindowClassifier({ nowMs: this.nowFn });

		// DEC-017: an incomplete trust boundary is a CONSTRUCTION-TIME error.
		const boundaryErrors = validateTrustBoundaryManifest(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			const reason: DisableReason = {
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			};
			this.lifecycle.disable(reason);
		}

		// §11 step 3/4: missing required secret ⇒ LOUD disable (status-visible).
		const enablement = resolveEnablement(
			WHATSAPP_CLOUD_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // reply-only egress; no native lanes
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			transmitEdit: async (chatId, messageId, content) =>
				this.wireEdit(chatId, messageId, content, { finalize: false }),
			transmitSeal: async (_k, _chatId, _draftId, _content, _metadata) => ({
				success: false,
				error: "Not supported",
			}),
		});

		this.router = new CallbackQueryRouter({
			stores: {
				approvals: this.approvals,
				slashConfirms: this.slashConfirms,
				appr: this.appr,
				clarify: this.clarify,
			},
			authorizer: () => this.allowAllClickers,
			onExecApproval: async (_sessionKey) => {
				this.resolvedFamilies.push("ea");
				return "ok";
			},
			onSlashConfirm: async (_sessionKey, _id, _choice) => {
				this.resolvedFamilies.push("sc");
				return "ok";
			},
			onClarifyChoice: async (_sessionKey, _id, idx) => {
				this.resolvedFamilies.push("cl");
				return `answer-${idx}`;
			},
			onWhatsappApproval: async (_sessionKey, _id, _approve) => {
				this.resolvedFamilies.push("appr");
				return "ok";
			},
			onPickerNav: async (parsed) => ({
				answerText: `nav:${parsed.family}`,
			}),
		});
	}

	// ── secrets (scoped reads only — DEC-003/009) ───────────────────────────

	secret(name: string): string | undefined {
		return this.secretReader(name);
	}

	get appSecret(): string | undefined {
		return this.secret("WHATSAPP_CLOUD_APP_SECRET");
	}

	get verifyToken(): string | undefined {
		return this.secret("WHATSAPP_CLOUD_VERIFY_TOKEN");
	}

	// ── lifecycle snapshot ───────────────────────────────────────────────────

	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.lifecycle.statusSnapshot();
	}

	// ── GET subscription handshake (_handle_verify parity) ──────────────────

	handleVerifyRequest(query: Record<string, string>): {
		status: number;
		text: string;
	} {
		if (!this.verifyToken) {
			// Misconfigured server refuses rather than silently accepting any
			// token (which would let an attacker subscribe).
			return { status: 503, text: "verify_token not configured" };
		}
		if (query["hub.mode"] !== "subscribe")
			return { status: 400, text: "bad mode" };
		// Constant-time compare (_handle_verify hmac.compare_digest parity):
		// token-length/content timing must not leak the shared secret.
		if (!secureCompare(query["hub.verify_token"] ?? "", this.verifyToken)) {
			return { status: 403, text: "verify_token mismatch" };
		}
		const challenge = query["hub.challenge"] ?? "";
		if (!challenge) return { status: 400, text: "missing challenge" };
		return { status: 200, text: challenge };
	}

	// ── POST signature gate (kit trust engine, WA scheme data) ──────────────

	/**
	 * X-Hub-Signature-256 verification through verifyHmacSignature() from
	 * kit/trust.ts — constant-time hex compare over RAW body bytes. Parity:
	 * whatsapp_cloud.py:_verify_signature (sha256= prefix stripped by the kit
	 * helper; both sides lowercased before compare).
	 */
	verifySignature(rawBody: Buffer, header: string | undefined): boolean {
		const secret = this.appSecret;
		if (!secret || !header) return false;
		if (!header.startsWith("sha256=")) return false;
		return verifyHmacSignature(secret, rawBody, header);
	}

	/**
	 * THE POST entry point. Order ports _handle_webhook exactly: cap → secret
	 * unset 503 → signature 401 → parse 400 → dispatch; verified requests
	 * answer 200 once accepted (never signal Meta to retry).
	 */
	async handleWebhookPost(
		headers: Record<string, string>,
		rawBody: Buffer,
	): Promise<{ status: number; json: Record<string, unknown> }> {
		const cap = this.trustBoundary?.bodySizeCapBytes ?? Number.MAX_SAFE_INTEGER;
		if (rawBody.length > cap)
			return { status: 413, json: { error: "payload too large" } };

		if (!this.appSecret) {
			// Fail CLOSED: without the HMAC key the handler would be a
			// data-injection point (_handle_webhook 503 posture).
			return { status: 503, json: { error: "app_secret not configured" } };
		}

		if (!this.verifySignature(rawBody, headers["x-hub-signature-256"])) {
			this.counters.rejectedSignature += 1;
			return { status: 401, json: { error: "invalid X-Hub-Signature-256" } };
		}

		let payload: unknown;
		try {
			payload = JSON.parse(rawBody.toString("utf8"));
		} catch {
			return { status: 400, json: { error: "invalid JSON" } };
		}
		if (
			payload === null ||
			typeof payload !== "object" ||
			Array.isArray(payload)
		) {
			return { status: 400, json: { error: "expected JSON object" } };
		}

		await this.dispatchPayload(payload as Record<string, unknown>);
		return { status: 200, json: { status: "accepted" } };
	}

	// ── wamid replay protection (_dedup_wamid parity) ───────────────────────

	/** True when this wamid is seen for the FIRST time (FIFO-capped set). */
	dedupWamid(wamid: string): boolean {
		if (!wamid) return true; // no id ⇒ cannot dedup — let it through
		if (this.seenWamids.has(wamid)) {
			this.counters.duplicates += 1;
			return false;
		}
		this.seenWamids.add(wamid);
		while (this.seenWamids.size > this.dedupCap) {
			const oldest = this.seenWamids.values().next().value;
			if (oldest === undefined) break;
			this.seenWamids.delete(oldest);
		}
		return true;
	}

	// ── envelope walk (_dispatch_payload parity) ─────────────────────────────

	async dispatchPayload(payload: Record<string, unknown>): Promise<void> {
		if (payload["object"] !== "whatsapp_business_account") return; // non-WABA ignored
		const entries = asArray(payload["entry"]);
		for (const entry of entries) {
			if (entry === null || typeof entry !== "object") continue;
			for (const change of asArray(
				(entry as Record<string, unknown>)["changes"],
			)) {
				if (change === null || typeof change !== "object") continue;
				const changeRec = change as Record<string, unknown>;
				if (changeRec["field"] !== "messages") continue; // other fields: silent skip
				const value =
					changeRec["value"] !== null && typeof changeRec["value"] === "object"
						? (changeRec["value"] as Record<string, unknown>)
						: {};

				// contacts[].profile.name index (wa_id → display name).
				const contactsByWaid = new Map<string, string>();
				for (const contact of asArray(value["contacts"])) {
					if (contact === null || typeof contact !== "object") continue;
					const rec = contact as Record<string, unknown>;
					const waId = str(rec["wa_id"]);
					const profile = asRec(rec["profile"]);
					if (waId) contactsByWaid.set(waId, str(profile["name"]));
				}
				const meta = asRec(value["metadata"]);

				for (const rawMessage of asArray(value["messages"])) {
					if (rawMessage === null || typeof rawMessage !== "object") continue;
					const rec = rawMessage as Record<string, unknown>;
					const wamid = str(rec["id"]);
					if (!this.dedupWamid(wamid)) continue; // replay — skip quietly
					try {
						await this.processInboundMessage(rec, contactsByWaid, meta);
					} catch {
						// Contained per message: the wamid is already dedup-marked,
						// so bubbling here would make Meta retry the WHOLE batch and
						// every sibling would drop as a duplicate (_dispatch_payload
						// containment comment).
					}
				}

				// statuses[] callbacks: delivery receipts are LOGGED, never
				// dispatched — the agent does not consume them and forwarding
				// would synthesize noisy events.
				for (const status of asArray(value["statuses"])) {
					if (status !== null && typeof status === "object") {
						this.counters.statusesSeen += 1;
					}
				}
			}
		}
	}

	private async processInboundMessage(
		raw: Record<string, unknown>,
		contactsByWaid: Map<string, string>,
		metadata: Record<string, unknown>,
	): Promise<void> {
		const event = await this.buildInboundEvent(raw, contactsByWaid, metadata);
		if (event === null) return; // claimed tap / filtered / group-shaped
		this.counters.accepted += 1;
		const source = event.source ?? {
			platform: "whatsapp",
			chatType: "dm",
			userId: "",
			chatId: "",
		};
		const sessionKey = buildSessionKey(
			{
				...(source.userId !== undefined ? { userId: source.userId } : {}),
				...(source.chatId !== undefined ? { chatId: source.chatId } : {}),
				...(source.chatName !== undefined ? { chatName: source.chatName } : {}),
				platform: source.platform,
				chatType: source.chatType,
			},
			{},
			undefined,
			{
				whatsapp: {
					...(this.whatsappSessionDir
						? { sessionDir: this.whatsappSessionDir }
						: {}),
				},
			},
		);
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	/**
	 * MessageEvent construction (_build_message_event_from_cloud parity):
	 * interactive claim first, body extraction, DM-only refusal of group-
	 * shaped payloads, media two-step download, quoted-text resolution.
	 */
	private async buildInboundEvent(
		raw: Record<string, unknown>,
		contactsByWaid: Map<string, string>,
		metadata: Record<string, unknown>,
	): Promise<IncomingEvent | null> {
		const msgType = (str(raw["type"]) || "text").toLowerCase();

		// Interactive replies route through the resolver BEFORE text dispatch;
		// a CLAIMED tap never becomes a conversation turn.
		if (msgType === "interactive") {
			const claimed = await this.dispatchInteractiveReply(raw);
			if (claimed) return null;
		}

		let body = "";
		if (msgType === "text") {
			body = str(asRec(raw["text"])["body"]);
		} else if (msgType === "button") {
			body = str(asRec(raw["button"])["text"]);
		} else if (msgType === "interactive") {
			const inter = asRec(raw["interactive"]);
			const inner = Object.keys(inter).includes("button_reply")
				? asRec(inter["button_reply"])
				: asRec(inter["list_reply"]);
			body = str(inner["title"]);
		} else if (CAPTION_KINDS.includes(msgType as WaMediaKind)) {
			body = str(asRec(raw[msgType])["caption"]);
		}

		// Defensive refusal: group-shaped payloads carry a `chat` field; Cloud
		// support is DM-only here — refuse rather than misaddress.
		if (raw["chat"]) {
			this.counters.refusedGroupShaped += 1;
			return null;
		}

		const senderId = str(raw["from"]);
		if (!senderId) return null;
		if (!this.isDmAllowed(senderId)) return null;
		const senderName = contactsByWaid.get(senderId) ?? "";
		const chatId = senderId; // DMs: chat_id ≙ sender wa_id

		// Media kinds download via the two-step Graph endpoint.
		const mediaUrls: string[] = [];
		const mediaTypes: string[] = [];
		let messageType: MessageType = "text";
		switch (msgType) {
			case "image":
			case "sticker":
				messageType = "photo";
				break;
			case "video":
				messageType = "video";
				break;
			case "audio":
			case "voice":
				messageType = "voice";
				break;
			case "document":
				messageType = "document";
				break;
			default:
				messageType = "text";
		}
		if (
			["image", "video", "audio", "voice", "document", "sticker"].includes(
				msgType,
			)
		) {
			const inner = asRec(raw[msgType]);
			const mediaId = str(inner["id"]);
			const inboundMime = str(inner["mime_type"]);
			if (mediaId) {
				const downloaded = await this.downloadMediaToCache(
					mediaId,
					inboundMime,
				);
				if (downloaded !== null) {
					mediaUrls.push(downloaded.path);
					mediaTypes.push(
						downloaded.mime || inboundMime || "application/octet-stream",
					);
					this.inboundMediaLog.push({
						chatId,
						urls: [downloaded.path],
						types: [downloaded.mime],
					});
				}
				if (msgType === "document" && !body) {
					const fname = str(inner["filename"]);
					if (fname) body = `[Document: ${fname}]`;
				}
			}
		}

		// Text-readable documents inject their file content INLINE so the agent
		// can reason about the attachment without a separate read_file call
		// (_build_message_event_from_cloud @~2020; same heuristic as the Baileys
		// adapter). 100KB cap matches Telegram/Discord/Slack; oversize/failed
		// reads keep the metadata-only body. The injection PREPENDS (caption or
		// '[Document: fname]' marker rides AFTER the content).
		if (msgType === "document" && mediaUrls.length > 0 && mediaUrls[0]) {
			const docPath = mediaUrls[0];
			const ext = extname(docPath).toLowerCase();
			if (TEXT_INJECT_EXTENSIONS.has(ext)) {
				try {
					if (statSync(docPath).size <= MAX_TEXT_INJECT_BYTES) {
						// Node utf8 decoding replaces malformed sequences with
						// U+FFFD (read_text(errors="replace") parity).
						const content = readFileSync(docPath, "utf8");
						const displayName = basename(docPath);
						const injection = `[Content of ${displayName}]:\n${content}`;
						body = body ? `${injection}\n\n${body}` : injection;
					}
				} catch {
					/* best-effort (OSError parity): metadata-only body stands */
				}
			}
		}

		// Quoted-message context: Meta carries only the id (+ author); the text
		// resolves from our own send/receive index (rich_sent_store parity).
		// Without the resolved text the run loop cannot inject the
		// '[Replying to: …]' disambiguation prefix (it gates on reply_to_text).
		const context = asRec(raw["context"]);
		const replyToId = str(context["id"]) || undefined;
		let replyToIsOwn = false;
		let replyToText: string | undefined;
		if (replyToId) {
			replyToText = this.quotedTextOf(chatId, replyToId);
			const quotedFrom = str(context["from"]);
			const ourNumber = str(metadata["display_phone_number"]);
			replyToIsOwn = Boolean(
				quotedFrom && ourNumber && quotedFrom === ourNumber,
			);
		}
		// Post-gate state updates (parity: done HERE so filtered traffic never
		// leaks typing targets or window sessions).
		if (str(raw["id"])) {
			this.boundedMapPut(this.lastInboundWamidByChat, chatId, str(raw["id"]));
			if (body) {
				this.boundedMapPut(
					this.sentTextByChatWamid,
					`${chatId}\u0000${str(raw["id"])}`,
					{
						chatId,
						text: body,
					},
				);
			}
		}
		this.classifier.noteInbound(chatId, this.nowFn());

		return {
			...(str(raw["id"]) ? { messageId: str(raw["id"]) } : {}),
			text: body,
			messageType,
			...(mediaUrls.length > 0 ? { mediaUrls } : {}),
			...(mediaTypes.length > 0 ? { mediaTypes } : {}),
			...(replyToId !== undefined ? { replyToMessageId: replyToId } : {}),
			metadata: {
				...(replyToId !== undefined
					? { reply_to_is_own_message: replyToIsOwn }
					: {}),
				...(replyToText !== undefined ? { reply_to_text: replyToText } : {}),
			},
			source: {
				platform: "whatsapp",
				chatType: "dm",
				userId: senderId,
				chatId,
				chatName: senderName || chatId,
			},
		};
	}

	/** Quoted text lookup across BOTH directions of the send/receive index. */
	quotedTextOf(chatId: string, wamid: string): string | undefined {
		return this.sentTextByChatWamid.get(`${chatId}\u0000${wamid}`)?.text;
	}

	// ── interactive taps (_dispatch_interactive_reply parity) ────────────────

	/**
	 * Route an interactive button/list tap through the ONE kit router.
	 * Returns true when the tap is CLAIMED (never also dispatched as text).
	 * Stale/unknown ids fall through to TEXT dispatch (button title becomes a
	 * normal user message) — the graceful fallback covers restart/stale state.
	 *
	 * Cloud API has NO edit API: the consumed-keyboard edit degrades to a plain
	 * confirmation SEND (§3 matrix "Edit support: usually none → reconcile
	 * falls back to plain send").
	 */
	private async dispatchInteractiveReply(
		raw: Record<string, unknown>,
	): Promise<boolean> {
		const inter = asRec(raw["interactive"]);
		const inner = Object.keys(inter).includes("button_reply")
			? asRec(inter["button_reply"])
			: asRec(inter["list_reply"]);
		const buttonId = str(inner["id"]);
		if (!buttonId) return false;

		const senderId = str(raw["from"]);
		if (!senderId || !this.allowAllClickers) {
			// Unauthorized taps are CLAIMED (so they never re-enter the agent
			// loop as text) but not messaged — Hermes logs-and-drops posture.
			return true;
		}

		const answer = await this.router.route(buttonId, { userId: senderId });
		switch (answer.kind) {
			case "resolved":
				try {
					await this.send(senderId, answer.answerText);
				} catch {
					/* confirmation best-effort; tap stays claimed */
				}
				return true;
			case "nav":
				try {
					await this.send(senderId, answer.answerText);
				} catch {
					/* ignore */
				}
				return true;
			case "unauthorized":
				return true; // claimed silently (no ⛔ message on Cloud parity)
			default:
				// stale/unknown → NOT claimed: title flows through text dispatch.
				return false;
		}
	}

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	isDmAllowed(senderId: string): boolean {
		// Allowlist mode compares NORMALIZED bare wa_ids (_normalize_allow_ids
		// parity); open mode admits everything (Hermes default without config).
		if (this.dmAllowSet.size === 0) return true;
		const bare = normalizeWhatsappIdentifier(senderId);
		return this.dmAllowSet.has(bare);
	}

	// ── guard wiring (reference-fixture inheritance) ─────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: WA_CLOUD_REGISTRY,
				messageHandler: async (event, ctx) => {
					const text = event.text ?? `[${String(event.messageType)}]`;
					const sessionKey = String(
						event.metadata?.["gateway_session_key"] ?? "",
					);
					if (this.clarifyArmedSet.has(sessionKey) && !text.startsWith("/")) {
						this.clarifyCaptures.push(text);
						return null; // consumed by the clarify resolver (Lane C)
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
					this.resolveReplyWaiters(sessionKey, reply);
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
		// Self/echo filter parity: bot-authored echoes never become turns.
		if (String(event.source?.userId ?? "") === "bot-self") return;
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	/** Run one guarded turn and resolve with its reply (bounded-window lane). */
	async runAgentTurn(dispatch: {
		event: IncomingEvent;
		sessionKey: string;
	}): Promise<string | null> {
		const waiter = this.registerReplyWaiter(dispatch.sessionKey);
		await this.deliverInbound(dispatch.event, dispatch.sessionKey);
		return waiter.promise;
	}

	registerReplyWaiter(sessionKey: string): {
		promise: Promise<string | null>;
		resolve: (reply: string | null) => void;
	} {
		let resolve!: (reply: string | null) => void;
		const promise = new Promise<string | null>((r) => {
			resolve = r;
		});
		const waiters = this.replyWaiters.get(sessionKey) ?? [];
		waiters.push(resolve);
		this.replyWaiters.set(sessionKey, waiters);
		return {
			promise,
			resolve: (reply) => {
				const list = this.replyWaiters.get(sessionKey);
				if (list) {
					const idx = list.indexOf(resolve);
					if (idx >= 0) list.splice(idx, 1);
				}
				resolve(reply);
			},
		};
	}

	private resolveReplyWaiters(sessionKey: string, reply: string): void {
		const waiters = this.replyWaiters.get(sessionKey);
		if (waiters === undefined || waiters.length === 0) return;
		this.replyWaiters.set(sessionKey, []);
		for (const w of waiters) w(reply);
	}

	// ── egress doors ─────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * OUTBOUND recipient at the WIRE level: chatId VERBATIM on every /messages
	 * POST body's `to` field (whatsapp_cloud.py:send @~544 and _send_media post
	 * ``chat_id`` unchanged; Meta addressed us AT this wa_id, so it IS the
	 * deliverable recipient). Alias expansion + min-pick canonicalization is a
	 * SESSION-KEY-side concern ONLY (02 §4.3): letting a stale LID mapping
	 * rewrite the outbound `to` would put a LID-derived digit string where Meta
	 * expects the delivered wa_id. The dmAllowSet normalization upstream stays
	 * allowlist-side (_normalize_allow_ids parity).
	 */
	resolveRecipient(chatId: string): string {
		return chatId;
	}

	/**
	 * Per-chat length descriptor (§6.3/A15 relay-shaped override point): a chat
	 * whose negotiated descriptor differs returns budget AND unit TOGETHER;
	 * production chats return undefined ⇒ manifest default 4096 chars.
	 */
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

	/**
	 * DOOR transport — text egress. Native splitting lives HERE (the adapter's
	 * splits_long_messages=True contract): content converts to WhatsApp markup,
	 * chunks against THE one chat length policy, each chunk POSTs its own
	 * /messages edge, and reply context quotes on the FIRST chunk only
	 * (whatsapp_cloud.py:send loop parity).
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		// Blank/whitespace content short-circuits BEFORE any Graph call
		// (send parity: "if not content or not content.strip(): return success" —
		// Hermes never POSTs an empty text body).
		if (!content || content.trim() === "") {
			return { success: true, messageId: null };
		}

		// Messaging-window routing decision RECORDED before any wire call.
		const decision = this.classifier.decideForSend(chatId);
		if (!decision.withinWindow && this.outsideWindowPolicy === "refuse") {
			this.counters.windowRefusals += 1;
			return {
				success: false,
				error: `template_required: ${decision.reason}`,
				retryable: false,
			};
		}

		// The §6.1 plain-text fallback lane carries ORIGINAL chunk bytes by
		// contract — dialect conversion is SKIPPED for that envelope (its prefix
		// marks it); everything else converts to WhatsApp markup first.
		const formatted = content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			? content
			: toWhatsappMarkup(content);
		const policy = this.chatLengthPolicyForChat(chatId);
		const chunks =
			policy.lenFn(formatted) <= policy.maxUnits
				? [formatted]
				: chunkWithFenceCarry(formatted, policy).chunks;

		const recipient = this.resolveRecipient(chatId);
		const replyTo =
			typeof metadata["reply_to_message_id"] === "string"
				? metadata["reply_to_message_id"]
				: undefined;

		let lastId: string | null = null;
		for (let idx = 0; idx < chunks.length; idx++) {
			const payload: Record<string, unknown> = {
				messaging_product: "whatsapp",
				recipient_type: "individual",
				to: recipient,
				type: "text",
				text: { body: chunks[idx], preview_url: true },
			};
			if (idx === 0 && replyTo) payload["context"] = { message_id: replyTo };
			const resp = await this.transport.postMessages(payload, metadata);
			if (resp.status !== 200) {
				return { success: false, error: formatGraphError(resp) };
			}
			lastId = extractOutboundWamid(resp);
		}
		if (lastId !== null && formatted) {
			// Index OUR side of every conversation so a later quoted reply can
			// resolve its text (Meta carries only context.id; rich_sent_store
			// records BOTH directions — whatsapp_cloud.py:send best-effort index).
			this.boundedMapPut(this.sentTextByChatWamid, `${chatId}\u0000${lastId}`, {
				chatId,
				text: formatted,
			});
		}
		return { success: true, messageId: lastId };
	}

	/**
	 * Rich lane ABSENT on the real Cloud API (reply-only egress): unless the
	 * harness explicitly scripted a rich probe, answer the capability-error
	 * shape WITHOUT burning a wire roundtrip — the §10.1 latch path probes once
	 * then never again (webhook reference adapter parity).
	 */
	protected override async wireRich(
		content: string,
		_metadata: Metadata,
	): Promise<SendResult> {
		if (!(this.transport.hasRichScript?.() ?? false)) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		const resp = await this.transport.transmitRichProbe!("__rich__", content);
		return resp.status === 200
			? { success: true }
			: { success: false, error: formatGraphError(resp) };
	}

	/**
	 * Media send — the two-step shape: upload FIRST (cap-refused BEFORE any
	 * roundtrip), then the captioned media MESSAGE references the id
	 * (_upload_media/_send_media parity). HTTPS links skip step one entirely
	 * ("Prefers the link path… one fewer Graph round trip").
	 */
	async sendMedia(
		chatId: string,
		kind: WaMediaKind,
		source:
			| { bytes: Buffer; filename?: string; mime?: string }
			| { link: string },
		opts: {
			caption?: string | undefined;
			filename?: string | undefined;
			replyToMessageId?: string | undefined;
		} = {},
	): Promise<SendResult> {
		this.throwIfDisabled();
		const decision = this.classifier.decideForSend(chatId); // recorded either way
		if (!decision.withinWindow && this.outsideWindowPolicy === "refuse") {
			this.counters.windowRefusals += 1;
			return {
				success: false,
				error: `template_required: ${decision.reason}`,
				retryable: false,
			};
		}

		const mediaBlock: Record<string, unknown> = {};
		if (!("link" in source)) {
			let bytes = source.bytes;
			let mime = source.mime ?? DEFAULT_MEDIA_MIME[kind];
			let filename = source.filename ?? `${kind}${resolveMediaExtension(mime)}`;

			// Voice lane transcoder seam (send_voice @~1194 parity): WhatsApp
			// renders 'audio/ogg; codecs=opus' as the native voice-note bubble,
			// so local MP3s convert pre-upload. The lane keys off CALLER-declared
			// MP3 evidence (.mp3 filename / audio/mpeg mime) — never the derived
			// defaults, so undeclared bytes ship verbatim exactly as before.
			// Conversion failure degrades to the MP3 attachment (audio/mpeg),
			// never an error.
			const declaredMp3 =
				(source.filename !== undefined &&
					source.filename.toLowerCase().endsWith(".mp3")) ||
				source.mime === "audio/mpeg";
			if (
				kind === "audio" &&
				declaredMp3 &&
				this.transcodeMp3ToOpus !== undefined
			) {
				const converted = await this.transcodeMp3ToOpus(bytes, filename);
				if (converted !== null) {
					bytes = converted.bytes;
					filename = converted.filename;
					mime = VOICE_NOTE_MIME;
				} else {
					// Will deliver as MP3 attachment, not voice bubble.
					mime = "audio/mpeg";
				}
			}

			const cap = MEDIA_SIZE_LIMITS[kind];
			if (bytes.length > cap) {
				// PRE-upload refusal (whatsapp_cloud.py:_upload_media: refuse
				// above-cap uploads "instead of round-tripping to Graph").
				return {
					success: false,
					error: `File ${filename} is ${bytes.length} bytes; Cloud API ${kind} cap is ${cap} bytes`,
					retryable: false,
				};
			}
			const upload: WaMediaUploadInput = {
				kind,
				bytes,
				mime,
				filename,
				// Meta-required multipart fields (_upload_media parity).
				messagingProduct: "whatsapp",
				type: mime,
			};
			const resp = await this.transport.uploadMedia(upload);
			if (resp.status !== 200) {
				return { success: false, error: formatGraphError(resp) };
			}
			const id =
				typeof resp.json["id"] === "string" ? resp.json["id"] : undefined;
			if (!id) return { success: false, error: "Upload response missing 'id'" };
			mediaBlock["id"] = id;
		} else {
			mediaBlock["link"] = source.link;
		}

		if (opts.caption && CAPTION_KINDS.includes(kind)) {
			mediaBlock["caption"] = opts.caption; // caption RIDES the media block
		}
		if (opts.filename && kind === "document") {
			mediaBlock["filename"] = opts.filename;
		}

		const payload: Record<string, unknown> = {
			messaging_product: "whatsapp",
			recipient_type: "individual",
			to: this.resolveRecipient(chatId),
			type: kind,
			[kind]: mediaBlock,
		};
		if (opts.replyToMessageId)
			payload["context"] = { message_id: opts.replyToMessageId };
		const resp = await this.transport.postMessages(payload);
		if (resp.status !== 200)
			return { success: false, error: formatGraphError(resp) };
		return { success: true, messageId: extractOutboundWamid(resp) };
	}

	// ── read receipts + typing (send_typing parity) ─────────────────────────

	/**
	 * Mark the latest inbound message read AND show the typing indicator — ONE
	 * coupled POST (Meta couples them; the indicator auto-dismisses when we
	 * respond or after 25s). Best-effort: no inbound wamid yet ⇒ SKIP (no POST);
	 * code 131009 (wamid older than ~30 days) ⇒ info-class rejection, no throw.
	 */
	async markReadAndTyping(chatId: string): Promise<void> {
		this.throwIfDisabled();
		const wamid = this.lastInboundWamidByChat.get(chatId);
		if (!wamid) return; // nothing to attach to — next inbound repopulates
		const resp = await this.transport.postMessages({
			messaging_product: "whatsapp",
			status: "read",
			message_id: wamid,
			typing_indicator: { type: "text" },
		});
		if (resp.status === 200) {
			this.receipts.push({ chatId, wamid, ok: true });
			return;
		}
		const err = asRec(resp.json["error"]);
		const code = typeof err["code"] === "number" ? err["code"] : undefined;
		this.receipts.push({
			chatId,
			wamid,
			ok: false,
			...(code !== undefined ? { rejectedCode: code } : {}),
		});
	}

	// ── interactive sends ────────────────────────────────────────────────────

	/** Truncate a button/row label to its cap with an ellipsis. */
	truncateLabel(text: string, limit: number): string {
		const t = String(text ?? "").trim();
		if (t.length <= limit) return t;
		return `${t.slice(0, Math.max(1, limit - 1))}…`;
	}

	/** Truncate an interactive body to 1024 chars. */
	truncateBody(text: string): string {
		return text.length <= INTERACTIVE_BODY_CAP
			? text
			: `${text.slice(0, INTERACTIVE_BODY_CAP - 3)}...`;
	}

	private async postInteractive(
		chatId: string,
		interactive: Record<string, unknown>,
		replyToMessageId?: string | undefined,
	): Promise<SendResult> {
		const payload: Record<string, unknown> = {
			messaging_product: "whatsapp",
			recipient_type: "individual",
			to: this.resolveRecipient(chatId),
			type: "interactive",
			interactive,
		};
		if (replyToMessageId) payload["context"] = { message_id: replyToMessageId };
		const resp = await this.transport.postMessages(payload);
		if (resp.status !== 200)
			return { success: false, error: formatGraphError(resp) };
		return { success: true, messageId: extractOutboundWamid(resp) };
	}

	/**
	 * Pending-prompt registration. Hermes FIFO-caps these dicts at
	 * INTERACTIVE_STATE_CACHE_SIZE (1000) with oldest-eviction; in the Pi kit
	 * that retention policy IS the pending store's own contract — every
	 * register() call enforces the 1000-entry bound internally.
	 */
	private registerPending(
		store: OneShotPendingStore,
		id: string | number,
		sessionKey: string,
	): void {
		store.register(id, sessionKey);
	}

	/**
	 * Render an exec-approval prompt with the reduced `appr:` vocabulary into
	 * the SAME approval resolver (§9.1 cross-family clause). `smartDenied`
	 * appends the owner-override suffix line exactly like the source's
	 * send_exec_approval smart_denied=True branch.
	 */
	async sendWhatsappApproval(
		chatId: string,
		command: string,
		approvalId: string,
		sessionKey: string,
		description = "dangerous command",
		opts: {
			replyToMessageId?: string | undefined;
			smartDenied?: boolean | undefined;
		} = {},
	): Promise<SendResult> {
		this.throwIfDisabled();
		const cmdPreview =
			command.length <= 800 ? command : `${command.slice(0, 800)}...`;
		const bodyText = this.truncateBody(
			`⚠️ *Command Approval Required*\n\n\`\`\`\n${cmdPreview}\n\`\`\`\n\nReason: ${description}` +
				(opts.smartDenied === true
					? "\n\nSmart DENY: owner override applies to this one operation only."
					: ""),
		);
		const interactive = {
			type: "button",
			body: { text: bodyText },
			action: {
				buttons: [
					{
						type: "reply",
						reply: { id: `appr:${approvalId}:approve`, title: "✅ Approve" },
					},
					{
						type: "reply",
						reply: { id: `appr:${approvalId}:deny`, title: "❌ Deny" },
					},
				],
			},
		};
		const result = await this.postInteractive(
			chatId,
			interactive,
			opts.replyToMessageId,
		);
		if (result.success) this.registerPending(this.appr, approvalId, sessionKey);
		return result;
	}

	/**
	 * Render a 3-button slash-command confirmation prompt (send_slash_confirm
	 * @~903 parity; mirrors Telegram's Approve Once / Always / Cancel card).
	 * Button ids carry the sc:{once|always|cancel}:{confirm_id} grammar so taps
	 * route through THE one kit router; on a successful POST the confirm id is
	 * registered into slashConfirms for the inbound resolver. The caller owns
	 * the confirm_id (slash-command handler) and it must satisfy the kit
	 * callback grammar (short numeric ids — 64-byte callback_data cap).
	 */
	async sendSlashConfirm(
		chatId: string,
		title: string,
		message: string,
		sessionKey: string,
		confirmId: string | number,
		opts: { replyToMessageId?: string | undefined } = {},
	): Promise<SendResult> {
		this.throwIfDisabled();
		const bodyText = this.truncateBody(`*${title}*\n\n${message}`);
		const interactive = {
			type: "button",
			body: { text: bodyText },
			action: {
				buttons: [
					{
						type: "reply",
						reply: {
							id: `sc:once:${confirmId}`,
							title: "✅ Approve Once",
						},
					},
					{
						type: "reply",
						reply: { id: `sc:always:${confirmId}`, title: "🔒 Always" },
					},
					{
						type: "reply",
						reply: { id: `sc:cancel:${confirmId}`, title: "❌ Cancel" },
					},
				],
			},
		};
		const result = await this.postInteractive(
			chatId,
			interactive,
			opts.replyToMessageId,
		);
		if (result.success) {
			this.registerPending(this.slashConfirms, confirmId, sessionKey);
		}
		return result;
	}

	/**
	 * Clarify prompt: ≤3 choices render quick-reply buttons; more render a list
	 * sheet (≤10 rows + the ✏️ Other escape hatch); zero choices degrade to a
	 * plain question (gateway captures the next message).
	 */
	async sendClarifyPrompt(
		chatId: string,
		question: string,
		choices: readonly string[],
		clarifyId: string,
		sessionKey: string,
		opts: { replyToMessageId?: string | undefined } = {},
	): Promise<SendResult> {
		this.throwIfDisabled();
		if (choices.length === 0) {
			// Zero-choice clarify passes reply context through (send_clarify
			// parity: reply_to rides the plain question so context.message_id is
			// set where Hermes sets it).
			return this.send(chatId, `❓ ${question}`, opts.replyToMessageId, {});
		}
		const list = choices
			.slice(0, MAX_LIST_ROWS)
			.map((c) => c.trim())
			.filter(Boolean);
		const optionLines = list.map((c, i) => `${i + 1}. ${c}`).join("\n");
		const bodyText = this.truncateBody(`❓ ${question}\n\n${optionLines}`);

		let interactive: Record<string, unknown>;
		if (list.length <= MAX_QUICK_BUTTONS) {
			interactive = {
				type: "button",
				body: { text: bodyText },
				action: {
					buttons: list.map((_, idx) => ({
						type: "reply",
						reply: {
							id: `cl:${clarifyId}:${idx}`,
							title: this.truncateLabel(String(idx + 1), BUTTON_TITLE_CAP),
						},
					})),
				},
			};
		} else {
			const rows = list.map((choice, idx) => ({
				id: `cl:${clarifyId}:${idx}`,
				title: this.truncateLabel(String(idx + 1), LIST_ROW_TITLE_CAP),
				description: this.truncateLabel(choice, 72),
			}));
			rows.push({
				id: `cl:${clarifyId}:other`,
				title: "✏️ Other",
				description: "Type your own answer",
			});
			interactive = {
				type: "list",
				body: { text: bodyText },
				action: {
					button: "Choose",
					sections: [{ title: "Options", rows }],
				},
			};
		}
		const result = await this.postInteractive(
			chatId,
			interactive,
			opts.replyToMessageId,
		);
		if (result.success) {
			this.clarify.register(clarifyId, sessionKey);
		}
		return result;
	}

	// ── inbound media download (two-step Graph endpoint) ─────────────────────

	/**
	 * Two-step download: GET /{id} metadata (signed temp URL + mime) → GET the
	 * URL for bytes (auth still required), cached under the media dir with the
	 * override-map extension. Returns null on ANY failure (logged upstream).
	 */
	private async downloadMediaToCache(
		mediaId: string,
		hintMime?: string,
	): Promise<{ path: string; mime: string } | null> {
		// Defense in depth: refuse anything that isn't a plain Meta-style media
		// id so a hostile payload can't traverse paths (_download_media_to_cache).
		const id = mediaId.trim();
		if (!MEDIA_ID_SAFE_RE.test(id)) return null;
		const metaResp = await this.transport.getMediaMetadata(id);
		if (metaResp.status !== 200) return null;
		const url = str(metaResp.json["url"]);
		const metaMime = str(metaResp.json["mime_type"]);
		if (!url) return null;
		const bytesResp = await this.transport.fetchMediaBytes(url);
		if (bytesResp.status !== 200) return null;
		// Extension precedence (_download_media_to_cache @~1388 parity): the
		// WEBHOOK inner mime hint resolves FIRST; the Graph-metadata mime only
		// backfills when the hint is blank/unresolvable, then '.bin'. Divergent
		// mimes must not cache under the metadata-derived extension.
		const ext =
			(hintMime !== undefined ? tryResolveMediaExtension(hintMime) : null) ??
			(metaMime !== undefined ? tryResolveMediaExtension(metaMime) : null) ??
			".bin";
		mkdirSync(this.mediaCacheDir, { recursive: true });
		const outPath = join(this.mediaCacheDir, `${id}${ext}`);
		writeFileSync(outPath, bytesResp.bytes);
		return { path: outPath, mime: metaMime };
	}

	// ── misc seams ───────────────────────────────────────────────────────────

	private boundedMapPut<K, V>(map: Map<K, V>, key: K, value: V): void {
		map.set(key, value);
		while (map.size > this.dedupCap) {
			const oldest = map.keys().next().value;
			if (oldest === undefined) break;
			map.delete(oldest);
		}
	}

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		return true; // HTTP surface owned by the composition root's server layer
	}

	override async disconnect(): Promise<void> {}
}

// ── helpers ──────────────────────────────────────────────────────────────────

function asArray(v: unknown): unknown[] {
	return Array.isArray(v) ? v : [];
}

function asRec(v: unknown): Record<string, unknown> {
	return v !== null && typeof v === "object" && !Array.isArray(v)
		? (v as Record<string, unknown>)
		: {};
}

function str(v: unknown): string {
	return typeof v === "string" ? v.trim() : "";
}

/** Graph error rendering (_format_graph_error parity). */
function formatGraphError(resp: GraphResponse): string {
	const err = asRec(resp.json["error"]);
	const message = str(err["message"]) || "unknown error";
	const code = err["code"];
	if (typeof code === "number") {
		return `graph error ${code} (HTTP ${resp.status}): ${message}`;
	}
	return `HTTP ${resp.status}: ${message}`;
}

function extractOutboundWamid(resp: GraphResponse): string | null {
	const messages = asArray(resp.json["messages"]);
	const first = messages[0];
	if (first === null || typeof first !== "object") return null;
	const id = (first as Record<string, unknown>)["id"];
	return typeof id === "string" ? id : null;
}
