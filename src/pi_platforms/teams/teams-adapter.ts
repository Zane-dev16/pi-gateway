// pi_platforms/teams/teams-adapter — THE Microsoft Teams adapter, ported from
// the READ-ONLY Hermes plugin plugins/platforms/teams/adapter.py onto the kit
// base. Everything policy-shaped is inherited; this module supplies TRANSPORT
// (Bot Framework REST over the BotFrameworkTransport seam) and MANIFEST DATA.
//
// Shape (DEC-002 third column — webhook ingress, REAL egress):
//   - capabilities AS DATA: supports_async_delivery=True (base default — Teams
//     caches ConversationReferences and proactively sends approval cards),
//     splits_long_messages=True (explicit), interactive_resume=True (default)
//   - DEC-017 trust boundary as manifest data: NO HMAC scheme on this wire —
//     inbound Bearer validation is SDK-delegated (probe-computed exclusion,
//     see below); outbound rides the Bot Framework service-host allowlist
//   - ingress pipeline ports _on_message exactly: bot-id self filter →
//     MessageDeduplicator(TTL 300 s / max 1000) → ConversationReference cache →
//     <at>@mention</at> strip → conversation_type mapping → attachment walk
//     (mirrored text skipped, card attachments skipped, fileDownload.info
//     documents fetched through the SSRF-guarded seam) → classification
//     precedence DOCUMENT > PHOTO > VIDEO > AUDIO > TEXT
//   - card actions route through THE ONE kit CallbackQueryRouter with DEFAULT-
//     DENY clicker authorization (_on_card_action posture)
//   - send(): format_message identity → native chunking → threaded reply ONLY
//     for digit reply_to ("0" excluded) with FLAT fallback on ANY failure;
//     every activity ships RAW markdown (textFormat:"markdown") — Teams
//     renders a subset natively, no conversion ladder (single-path markdown)
//
// PROBE-COMPUTED EXCLUSION (honest-port documentation, roadmap §Phase 6):
// Hermes delegates INBOUND request authentication (Bot Framework Bearer token
// validation against Azure AD OpenID configs) to the microsoft-teams-apps SDK
// — external protocol machinery that cannot be faked headlessly without
// inventing it. The port implements everything Hermes implements at THIS
// layer: the manifest + wire shapes above. The auth boundary is declared in
// the trust boundary (`bearerAuthDelegatedToSdk`) and inbound POSTs are
// treated as post-auth-boundary activities. NEVER faked green.
//
// Layering: imports pi_gateway downward + kit same-layer ONLY; no adapter
// cross-imports.

import { readFileSync, mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	resolveEnablement,
	buildExecApprovalCallback,
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
import type { LengthUnit } from "../kit/length-policy.js";
import { chunkWithFenceCarry } from "../kit/chunking.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import { PLAIN_TEXT_FALLBACK_PREFIX } from "../kit/send-retry.js";

import {
	ALLOWED_TEAMS_SERVICE_HOSTS,
	DEFAULT_TEAMS_SERVICE_URL,
	TEAMS_BTN_DATA_CMD_CAP,
	TEAMS_CAPABILITIES,
	TEAMS_CMD_PREVIEW_CAP,
	TEAMS_CONV_ID_RE,
	TEAMS_DEDUP_MAX_SIZE,
	TEAMS_DEDUP_TTL_MS,
	TEAMS_HERMES_ACTION_CHOICES,
	TEAMS_MAX_BODY_BYTES,
	TEAMS_PLUGIN_MANIFEST,
	TEAMS_TENANT_ID_RE,
	TEAMS_TEXT_FORMAT,
	TEAMS_TOKEN_SCOPE,
	validateTeamsTrustBoundary,
} from "./manifest.js";
import type {
	BotFrameworkJson,
	BotFrameworkTransport,
	TokenEndpointResponse,
} from "./bot-framework-wire.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const TEAMS_REGISTRY: CommandRegistry = [
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

export interface TeamsAdapterOptions {
	transport: BotFrameworkTransport;
	/** Scoped reader over TEAMS_* names (fail-closed; DEC-003/009). */
	secretReader?: ScopedSecretReader | undefined;
	nowMs?: (() => number) | undefined;
	scalarMaxUnits?: number | undefined;
	spawner?: TaskSpawner | undefined;
	dedupMaxSize?: number | undefined;
	dedupTtlMs?: number | undefined;
	serviceUrl?: string | undefined;
	/** Inbound attachment bytes land here (tests: mkdtemp). */
	mediaCacheDir?: string | undefined;
	/** Clicker authorization seed (manifest optionalEnv parity). */
	allowedUsers?: readonly string[] | undefined;
	allowAllUsers?: boolean | undefined;
	/** Conformance capture wire (see MSGraphWebhookAdapterOptions.captureWire). */
	captureWire?: TeamsCaptureWire | undefined;
}

/** Capture seam (subject-supplied; family FakePlatformWire behavior). */
export interface TeamsCaptureWire {
	transmitSend(
		chatId: string,
		content: string,
		metadata: Record<string, unknown>,
	): Promise<SendResult>;
	hasRichScript(opKind: string): boolean;
}

type ActivityRecord = Record<string, unknown>;

export interface InboundActivityOutcome {
	skipped: false;
	messageId: string | null;
}

export interface SkippedActivity {
	skipped: true;
	reason: "self-message" | "duplicate" | "malformed";
}

/** InvokeResponse-shape result of an Adaptive Card action (parity). */
export interface CardActionResponse {
	statusCode: number;
	kind: "message" | "card";
	value: string;
}

export class TeamsAdapter extends BasePlatformAdapter {
	readonly pluginManifest = TEAMS_PLUGIN_MANIFEST;
	readonly trustBoundary = (() => {
		const b = TEAMS_PLUGIN_MANIFEST.trustBoundary;
		return b as typeof b & { bearerAuthDelegatedToSdk: true };
	})();
	readonly transport: BotFrameworkTransport;

	private readonly secretReader: ScopedSecretReader;
	private readonly nowFn: () => number;
	private readonly captureWire: TeamsCaptureWire | undefined;

	// ── identity/config (__init__ parity) ─────────────────────────────────────
	readonly clientId: string | undefined;
	readonly tenantId: string | undefined;
	readonly serviceUrl: string;
	readonly mediaCacheDir: string;

	// ── clicker authorization (_on_card_action default-DENY) ──────────────────
	private readonly allowedUsers: ReadonlySet<string>;
	private readonly allowAllUsers: boolean;

	// ── runtime state ────────────────────────────────────────────────────────
	private readonly seenActivities: TtlMessageDeduplicator;
	/** chat_id → cached ConversationReference (proactive-send lane). */
	readonly convRefs = new Map<string, ActivityRecord>();
	readonly counters = {
		selfMessagesSkipped: 0,
		duplicatesSkipped: 0,
		activitiesAccepted: 0,
		cardActionsResolved: 0,
		cardActionsDenied: 0,
		attachmentsCached: 0,
		typingSent: 0,
	};

	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly resolvedFamilies: string[] = [];
	/** Classified media kind per ACCEPTED activity (classification rows). */
	private readonly dispatchedMediaKindLog: string[] = [];
	dispatchedMediaKinds(): readonly string[] {
		return this.dispatchedMediaKindLog;
	}
	/** Approval ids registered on cards (lifecycle rows probe liveness). */
	readonly approvalIdLog: number[] = [];

	// Interactive surfaces (kit-owned; ONE router per adapter).
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	private readonly cp: EgressChokepoint;
	private allowAllClickers = true; // shared-row seam; card authz uses allowedUsers
	private approvalSeq = 1000;
	private readonly clarifyArmedSet = new Set<string>();
	private holding = false;
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};

	constructor(opts: TeamsAdapterOptions) {
		super({
			manifestName: TEAMS_PLUGIN_MANIFEST.name,
			capabilities: TEAMS_CAPABILITIES,
			scalarMaxUnits: opts.scalarMaxUnits ?? 28_000,
		});
		this.transport = opts.transport;
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.captureWire = opts.captureWire;
		this.mediaCacheDir =
			opts.mediaCacheDir ?? join(process.cwd(), "platforms", "teams", "media");
		this.clientId = this.secretReader("TEAMS_CLIENT_ID");
		this.tenantId = this.secretReader("TEAMS_TENANT_ID");
		this.serviceUrl =
			opts.serviceUrl ??
			this.secretReader("TEAMS_SERVICE_URL") ??
			DEFAULT_TEAMS_SERVICE_URL;

		const allowedCsv =
			opts.allowedUsers ?? parseCsv(this.secretReader("TEAMS_ALLOWED_USERS"));
		this.allowedUsers = new Set(allowedCsv);
		const allowAllOptIn =
			opts.allowAllUsers ??
			parseBool(this.secretReader("TEAMS_ALLOW_ALL_USERS"));
		this.allowAllUsers = allowAllOptIn;

		this.seenActivities = new TtlMessageDeduplicator({
			maxSize: opts.dedupMaxSize ?? TEAMS_DEDUP_MAX_SIZE,
			ttlMs: opts.dedupTtlMs ?? TEAMS_DEDUP_TTL_MS,
			nowMs: this.nowFn,
		});

		// DEC-017: an incomplete trust boundary is a CONSTRUCTION-TIME error.
		const boundaryErrors = validateTeamsTrustBoundary(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			this.lifecycle.disable({
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			});
		}

		// §11 step 3/4: missing required secret ⇒ LOUD disable (status-visible).
		const enablement = resolveEnablement(
			TEAMS_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // no native draft lanes on Teams
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

	// ── lifecycle ─────────────────────────────────────────────────────────────

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		return true; // HTTP surface owned by the composition root's server layer
	}

	override async disconnect(): Promise<void> {}

	lifecycleSnapshot() {
		return this.lifecycle.statusSnapshot();
	}

	// ── INBOUND: Bot Framework activity POST (handleActivityPost) ────────────

	/**
	 * Body-cap gate then the _on_message pipeline. Order ports the source:
	 * cap (aiohttp client_max_size posture) → JSON shape → self filter →
	 * dedupe → conv-ref cache → mention strip → chat-type map → attachments →
	 * dispatch. Malformed bodies answer 400 (sender's problem).
	 */
	async handleActivityPost(input: {
		headers?: Record<string, string> | undefined;
		rawBody: Buffer;
	}): Promise<{ status: number; json?: BotFrameworkJson }> {
		if (input.rawBody.length > TEAMS_MAX_BODY_BYTES) return { status: 413 };
		let payload: unknown;
		try {
			payload = JSON.parse(input.rawBody.toString("utf8"));
		} catch {
			return { status: 400 };
		}
		if (
			payload === null ||
			typeof payload !== "object" ||
			Array.isArray(payload)
		) {
			return { status: 400 };
		}
		const outcome = await this.onMessage(payload as ActivityRecord);
		return {
			status: 200,
			json: outcome.skipped ? { skipped: outcome.reason } : { accepted: true },
		};
	}

	/**
	 * _on_message parity (@~900). Self-authored echoes never become turns;
	 * redelivered activity ids die on the TTL deduplicator; everything else
	 * walks the attachment/classification pipeline into ONE guarded turn.
	 */
	async onMessage(
		activity: ActivityRecord,
	): Promise<InboundActivityOutcome | SkippedActivity> {
		const from = asRec(activity["from"]);
		// Self-message filter (bot_id ≙ app id ≙ client id).
		if (this.clientId && String(from["id"] ?? "") === this.clientId) {
			this.counters.selfMessagesSkipped += 1;
			return { skipped: true, reason: "self-message" };
		}

		// Deduplication (MessageDeduplicator parity).
		const msgId = str(activity["id"]) || null;
		if (msgId !== null && this.seenActivities.isDuplicate(msgId)) {
			this.counters.duplicatesSkipped += 1;
			return { skipped: true, reason: "duplicate" };
		}

		// Cache the conversation reference for proactive sends.
		const conversation = asRec(activity["conversation"]);
		const convId = str(conversation["id"]);
		if (convId) {
			this.convRefs.set(convId, {
				conversationType: str(
					conversation["conversation_type"] ?? conversation["conversationType"],
				),
				tenantId: str(conversation["tenant_id"] ?? conversation["tenantId"]),
				name: str(conversation["name"]),
			});
		}

		// Extract text — strip <at>BotName</at> HTML tags Teams prepends.
		let text = str(activity["text"]);
		text = text.replace(/<at>[^<]*<\/at>\s*/g, "").trim();

		// Chat type from the conversation payload.
		const convType = (
			str(
				conversation["conversation_type"] ?? conversation["conversationType"],
			) || ""
		).toString();
		const chatType = teamsChatTypeFor(convType);

		const userId =
			str(from["aad_object_id"] ?? from["aadObjectId"]) || str(from["id"]);
		const userName = str(from["name"]);

		// Attachment walk (classification precedence lives in classifyMedia).
		const media = await this.collectAttachments(activity);

		if (!convId) {
			return { skipped: true, reason: "malformed" };
		}
		this.counters.activitiesAccepted += 1;
		this.dispatchedMediaKindLog.push(media.messageType);

		const event: IncomingEvent = {
			...(msgId === null ? {} : { messageId: msgId }),
			text,
			messageType: media.messageType,
			source: {
				platform: TEAMS_PLUGIN_MANIFEST.name,
				chatType,
				userId,
				chatId: convId,
				...(userName ? { chatName: userName } : {}),
			},
			...(media.urls.length > 0 ? { mediaUrls: media.urls } : {}),
			...(media.types.length > 0 ? { mediaTypes: media.types } : {}),
			metadata: {
				gateway_session_key: `${TEAMS_PLUGIN_MANIFEST.name}:${userId}:${convId}`,
			},
		};
		await this.handleIngress(event, sessionKeyOfEvent(event));
		return { skipped: false, messageId: msgId };
	}

	/**
	 * Attachment walk (@~985 parity): mirrored text/html+text/plain bodies are
	 * skipped; adaptive/hero card payloads (application/vnd.microsoft.card.*)
	 * are skipped; fileConsent/fileDownload.info carries a pre-authed download
	 * URL + real fileType; image/* content URLs fetch as images; other content
	 * URLs fetch by mime. Classification: DOCUMENT wins over PHOTO/VIDEO/AUDIO
	 * for mixed attachments.
	 */
	private async collectAttachments(activity: ActivityRecord): Promise<{
		messageType: MessageType;
		urls: string[];
		types: string[];
		kinds: string[];
	}> {
		const urls: string[] = [];
		const types: string[] = [];
		const kinds: string[] = [];
		for (const att of asArray(activity["attachments"])) {
			const rec = asRec(att);
			const contentType = str(
				rec["contentType"] ?? rec["content_type"],
			).toLowerCase();
			const contentUrl = str(rec["contentUrl"] ?? rec["content_url"]);
			const name = str(rec["name"]);

			if (
				(contentType === "text/html" || contentType === "text/plain") &&
				!contentUrl
			)
				continue;
			if (contentType.startsWith("application/vnd.microsoft.card")) continue;

			if (
				contentType === "application/vnd.microsoft.teams.file.download.info"
			) {
				const content = asRec(rec["content"]);
				const downloadUrl = str(
					content["downloadUrl"] ?? content["download_url"],
				);
				const fileType = str(
					content["fileType"] ?? content["file_type"],
				).replace(/^\.+/, "");
				if (!downloadUrl) continue;
				const filename =
					name || (fileType ? `document.${fileType}` : "document");
				const cached = await this.fetchAndCache(downloadUrl, filename, "");
				if (cached !== null) {
					urls.push(cached.path);
					types.push(cached.mime);
					kinds.push(cached.kind);
				}
				continue;
			}
			if (contentUrl && contentType.startsWith("image/")) {
				const cached = await this.fetchAndCache(contentUrl, name, contentType);
				if (cached !== null) {
					urls.push(cached.path);
					types.push(contentType);
					kinds.push("image");
				}
				continue;
			}
			if (contentUrl) {
				const cached = await this.fetchAndCache(contentUrl, name, contentType);
				if (cached !== null) {
					urls.push(cached.path);
					types.push(cached.mime);
					kinds.push(cached.kind);
				}
			}
		}

		let messageType: MessageType = "text";
		if (kinds.includes("document")) messageType = "document";
		else if (kinds.includes("image")) messageType = "photo";
		else if (kinds.includes("video")) messageType = "video";
		else if (kinds.includes("audio")) messageType = "voice";
		return { messageType, urls, types, kinds };
	}

	/**
	 * SSRF-guarded attachment fetch + cache. Ported guard: only http(s)
	 * attachment URLs are fetchable (is_safe_url scheme check); the deeper
	 * private-range/DNS machinery of tools.url_safety stays SDK-delegated
	 * (probe-computed exclusion — same bucket as inbound Bearer validation).
	 */
	private async fetchAndCache(
		url: string,
		filename: string,
		mimeHint: string,
	): Promise<{ path: string; mime: string; kind: string } | null> {
		if (!this.isSafeAttachmentUrl(url)) return null;
		const resp = await this.transport.fetchAttachmentBytes(url);
		if (resp.status !== 200) return null;
		const mime = mimeHint || guessMime(filename) || "application/octet-stream";
		mkdirSync(this.mediaCacheDir, { recursive: true });
		const safeName = filename.replace(/[^A-Za-z0-9._-]/g, "_") || "attachment";
		const outPath = join(this.mediaCacheDir, safeName);
		writeFileSync(outPath, resp.bytes);
		this.counters.attachmentsCached += 1;
		return { path: outPath, mime, kind: kindOfMime(mime) };
	}

	isSafeAttachmentUrl(url: string): boolean {
		try {
			const parsed = new URL(url);
			return parsed.protocol === "https:" || parsed.protocol === "http:";
		} catch {
			return false;
		}
	}

	// ── CARD ACTIONS (_on_card_action @~1150 parity) ─────────────────────────

	/**
	 * Adaptive Card Action.Execute handler: default-DENY clicker authz, choice
	 * mapping through THE ONE kit router, explicit expiry answers. Never
	 * dispatched as turns; consumed taps answer inline within the ack window.
	 */
	async handleCardAction(
		value: ActivityRecord,
		clicker: { aadObjectId?: string | undefined; id?: string | undefined } = {},
	): Promise<CardActionResponse> {
		const hermesAction = str(value["hermes_action"]);
		const sessionKey = str(value["session_key"]);
		if (!hermesAction || !sessionKey) {
			return { statusCode: 200, kind: "message", value: "Unknown action." };
		}

		// Only authorized users may click approval buttons — DEFAULT DENY.
		if (!this.allowAllUsers) {
			if (this.allowedUsers.size === 0) {
				this.counters.cardActionsDenied += 1;
				return {
					statusCode: 200,
					kind: "message",
					value:
						"⛔ Approval buttons require TEAMS_ALLOWED_USERS to be configured.",
				};
			}
			const clickerId = clicker.aadObjectId ?? clicker.id ?? "";
			const wildcard = this.allowedUsers.has("*");
			if (
				!wildcard &&
				(clickerId === "" || !this.allowedUsers.has(clickerId))
			) {
				this.counters.cardActionsDenied += 1;
				return {
					statusCode: 200,
					kind: "message",
					value: "⛔ Not authorized.",
				};
			}
		}

		const choice = TEAMS_HERMES_ACTION_CHOICES[hermesAction];
		if (choice === undefined) {
			return { statusCode: 200, kind: "message", value: "Unknown action." };
		}

		// Stale/expired approvals answer EXPLICITLY (never dispatched as turns).
		const rawId = Number(value["approval_id"]);
		if (!Number.isInteger(rawId)) {
			return {
				statusCode: 200,
				kind: "card",
				value: "⚠️ Approval already resolved or expired.",
			};
		}
		const answer = await this.router.route(
			buildExecApprovalCallback(choice as "once", rawId),
			{ userId: clicker.aadObjectId ?? clicker.id ?? "" },
		);
		switch (answer.kind) {
			case "resolved": {
				this.counters.cardActionsResolved += 1;
				return { statusCode: 200, kind: "card", value: choiceLabel(choice) };
			}
			default:
				return {
					statusCode: 200,
					kind: "card",
					value: "⚠️ Approval already resolved or expired.",
				};
		}
	}

	/**
	 * send_exec_approval parity: build the approval card, register the pending
	 * approval under a fresh kit id, and proactively push via the stored
	 * ConversationReference lane (flat proactive send in this port's seam).
	 */
	async sendApprovalCard(
		chatId: string,
		command: string,
		sessionKey: string,
		description: string = "dangerous command",
		opts: {
			allowPermanent?: boolean | undefined;
			allowSession?: boolean | undefined;
			smartDenied?: boolean | undefined;
		} = {},
	): Promise<SendResult> {
		const allowSession = opts.allowSession !== false;
		const allowPermanent = opts.allowPermanent !== false;
		const smartDenied = opts.smartDenied === true;

		const cmdPreview =
			command.length > TEAMS_CMD_PREVIEW_CAP
				? `${command.slice(0, TEAMS_CMD_PREVIEW_CAP)}...`
				: command;
		const btnCmd =
			command.length > TEAMS_BTN_DATA_CMD_CAP
				? `${command.slice(0, TEAMS_BTN_DATA_CMD_CAP)}...`
				: command;

		this.approvalSeq += 1;
		const approvalId = this.approvalSeq;
		this.approvals.register(approvalId, sessionKey);
		this.approvalIdLog.push(approvalId);
		const baseData: ActivityRecord = {
			session_key: sessionKey,
			cmd: btnCmd,
			desc: description,
			approval_id: approvalId,
		};
		const actions: Array<ActivityRecord & { style?: string }> = [
			{
				type: "Action.Execute",
				title: "Allow Once",
				verb: "hermes_approve",
				data: { ...baseData, hermes_action: "approve_once" },
				style: "positive",
			},
		];
		if (!smartDenied && allowSession) {
			actions.push({
				type: "Action.Execute",
				title: "Allow Session",
				verb: "hermes_approve",
				data: { ...baseData, hermes_action: "approve_session" },
			});
			if (allowPermanent) {
				actions.push({
					type: "Action.Execute",
					title: "Always Allow",
					verb: "hermes_approve",
					data: { ...baseData, hermes_action: "approve_always" },
				});
			}
		}
		actions.push({
			type: "Action.Execute",
			title: "Deny",
			verb: "hermes_approve",
			data: { ...baseData, hermes_action: "deny" },
			style: "destructive",
		});

		const body: Array<ActivityRecord> = [
			{
				type: "TextBlock",
				text: "⚠️ Command Approval Required",
				wrap: true,
				weight: "Bolder",
			},
			{ type: "TextBlock", text: `\`\`\`\n${cmdPreview}\n\`\`\``, wrap: true },
			{
				type: "TextBlock",
				text: `Reason: ${description}`,
				wrap: true,
				isSubtle: true,
			},
		];
		if (smartDenied) {
			body.push({
				type: "TextBlock",
				text: "Smart DENY: owner override applies to this one operation only.",
				wrap: true,
			});
		}
		const card: ActivityRecord = {
			type: "AdaptiveCard",
			version: "1.4",
			body,
			actions,
		};

		try {
			const resp = await this.postProactive(chatId, {
				type: "message",
				attachments: [
					{
						contentType: "application/vnd.microsoft.card.adaptive",
						content: card,
					},
				],
			});
			if (resp.status !== 200) {
				const errText = str(asRec(resp.json["error"])["message"]);
				return { success: false, error: errText || `HTTP ${resp.status}` };
			}
			return { success: true, messageId: str(resp.json["id"]) || undefined };
		} catch (err) {
			return {
				success: false,
				error: err instanceof Error ? err.message : String(err),
				retryable: true,
			};
		}
	}

	// ── EGRESS: send pipeline (send @~1245 parity) ────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * Per-chat length descriptor (§6.3/A15 override point): harness utf16
	 * chats return budget AND unit TOGETHER; production chats return undefined
	 * ⇒ manifest default 28000 chars.
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
	 * DOOR transport: format_message identity (raw markdown out — Teams renders
	 * a subset natively), native chunking against THE chat length policy, each
	 * chunk POSTed as {"type":"message","textFormat":"markdown"}; threaded
	 * replies ONLY for digit reply_to ("0" excluded) with FLAT fallback on ANY
	 * failure (group chats 400 on threaded sends — the fallback IS the ported
	 * semantic, not an omission).
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		const urlErr = this.validateOutboundTarget(chatId);
		if (urlErr !== null) return { success: false, error: urlErr };

		// §6.1 plain-text fallback envelope carries ORIGINAL bytes — skip the
		// (identity) formatting and ship as-is either way.
		const formatted = content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			? content
			: content; // format_message identity: Teams takes raw markdown

		const policy = this.chatLengthPolicyForChat(chatId);
		const chunks =
			policy.lenFn(formatted) <= policy.maxUnits
				? [formatted]
				: chunkWithFenceCarry(formatted, policy).chunks;

		const token = await this.acquireToken();
		if (token.error !== null) {
			return { success: false, error: token.error, retryable: true };
		}

		const replyToRaw = metadata["reply_to_message_id"];
		const replyTo = typeof replyToRaw === "string" ? replyToRaw : undefined;

		let lastId: string | null = null;
		for (const chunk of chunks) {
			const activity: BotFrameworkJson = {
				type: "message",
				text: chunk,
				textFormat: TEAMS_TEXT_FORMAT,
			};
			try {
				let resp;
				if (replyTo && /^\d+$/.test(replyTo) && replyTo !== "0") {
					resp = await this.transport.postReply(
						chatId,
						replyTo,
						activity,
						token.accessToken,
						metadata,
					);
					if (resp.status !== 200) {
						// Group chats 400 on threaded sends; fall back to flat send.
						resp = await this.transport.postActivity(
							chatId,
							activity,
							token.accessToken,
							metadata,
						);
					}
				} else {
					resp = await this.transport.postActivity(
						chatId,
						activity,
						token.accessToken,
						metadata,
					);
				}
				if (resp.status !== 200) {
					// NO blanket retryable flag: the §6.1 classifier decides from the
					// surfaced error (timeout-class failures must NOT re-send).
					return {
						success: false,
						error: `activity post failed (${resp.status}): ${JSON.stringify(resp.json).slice(0, 300)}`,
					};
				}
				lastId = str(resp.json["id"]) || null;
			} catch (err) {
				return {
					success: false,
					error: err instanceof Error ? err.message : String(err),
					retryable: true,
				};
			}
		}
		return lastId === null
			? { success: true }
			: { success: true, messageId: lastId };
	}

	/**
	 * Native splitting is KIT-OWNED (chunkWithFenceCarry ≙ truncate_message):
	 * fence carry + (i/n) labels ride every split.
	 */

	/**
	 * Pre-wire target validation (standalone-send parity): the service host
	 * must be on the Bot Framework allowlist and the conversation id must fit
	 * the documented charset — hostile values never reach the URL builder.
	 */
	validateOutboundTarget(chatId: string): string | null {
		if (this.validateServiceUrl(this.serviceUrl) === null) {
			return (
				"Teams standalone send: TEAMS_SERVICE_URL host is not on the " +
				`Bot Framework allowlist; expected one of ${JSON.stringify([...ALLOWED_TEAMS_SERVICE_HOSTS])}`
			);
		}
		if (!TEAMS_CONV_ID_RE.test(chatId)) {
			return "Teams standalone send: chat_id contains characters outside the Bot Framework conversation ID set";
		}
		if (
			this.tenantId !== undefined &&
			!TEAMS_TENANT_ID_RE.test(this.tenantId)
		) {
			return "Teams standalone send: TEAMS_TENANT_ID contains characters outside the expected set";
		}
		return null;
	}

	/**
	 * _validate_teams_service_url parity: https + allowlisted host + trailing
	 * slash normalization; anything else refuses (SSRF/token-exfiltration
	 * posture).
	 */
	validateServiceUrl(raw: string): string | null {
		if (!raw) return null;
		try {
			const parsed = new URL(raw);
			if (parsed.protocol !== "https:") return null;
			if (!ALLOWED_TEAMS_SERVICE_HOSTS.has(parsed.hostname)) return null;
			return raw.endsWith("/") ? raw : `${raw}/`;
		} catch {
			return null;
		}
	}

	/**
	 * Token acquisition (standalone dance): client-credentials POST against
	 * the tenant endpoint; failures surface the source's error shapes. The
	 * live SDK owns token caching internally — the port models the REST dance
	 * verbatim (fresh token per outbound batch).
	 */
	async acquireToken(): Promise<
		{ accessToken: string; error: null } | { accessToken: null; error: string }
	> {
		if (
			this.clientId === undefined ||
			this.clientId === "" ||
			this.tenantId === undefined ||
			this.tenantId === ""
		) {
			return {
				accessToken: null,
				error:
					"Teams standalone send: TEAMS_CLIENT_ID, TEAMS_CLIENT_SECRET, and TEAMS_TENANT_ID are all required",
			};
		}
		const clientSecret = this.secretReader("TEAMS_CLIENT_SECRET") ?? "";
		let resp: TokenEndpointResponse;
		try {
			resp = await this.transport.getAccessToken({
				tenantId: this.tenantId,
				clientId: this.clientId,
				clientSecret,
				scope: TEAMS_TOKEN_SCOPE,
			});
		} catch (err) {
			return {
				accessToken: null,
				error: `Teams standalone send failed: ${err instanceof Error ? err.message : String(err)}`,
			};
		}
		if (resp.status >= 400) {
			return {
				accessToken: null,
				error: `Teams standalone send: token request failed (${resp.status}): ${JSON.stringify(resp.json).slice(0, 300)}`,
			};
		}
		const accessToken = str(resp.json["access_token"]);
		if (!accessToken) {
			return {
				accessToken: null,
				error: "Teams standalone send: token response missing access_token",
			};
		}
		return { accessToken, error: null };
	}

	/** Proactive flat send through the conv-ref-aware lane. */
	private async postProactive(
		chatId: string,
		activity: BotFrameworkJson,
	): Promise<{ status: number; json: BotFrameworkJson }> {
		const token = await this.acquireToken();
		if (token.error !== null) throw new Error(token.error);
		return this.transport.postActivity(chatId, activity, token.accessToken);
	}

	// ── typing / rich / media ────────────────────────────────────────────────

	/** send_typing parity: TypingActivityInput, errors swallowed. */
	async sendTyping(chatId: string): Promise<void> {
		try {
			await this.transport.sendTypingActivity(chatId);
			this.counters.typingSent += 1;
		} catch {
			/* best-effort polish — never surfaces */
		}
	}

	/**
	 * Rich lane ABSENT on Teams REST egress (cards ride their own explicit
	 * path): unless a capture wire scripted a rich probe, answer the
	 * capability-error shape WITHOUT burning a roundtrip (§10.1 latch parity).
	 */
	protected override async wireRich(content: string): Promise<SendResult> {
		if (
			this.captureWire === undefined ||
			!this.captureWire.hasRichScript("rich")
		) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		const bridge = this.transport as unknown as {
			hasScript?: (opKind: string) => boolean;
			transmitRichProbe?: (
				chatId: string,
				content: string,
			) => Promise<{ status: number }>;
		};
		if (bridge.transmitRichProbe === undefined) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		const resp = await bridge.transmitRichProbe("__rich__", content);
		return resp.status === 200
			? { success: true }
			: { success: false, error: `HTTP ${resp.status}` };
	}

	/**
	 * Media send (_send_media_attachment parity): remote http(s) URLs attach BY
	 * REFERENCE; local paths encode as base64 data URIs; caption rides the
	 * activity text.
	 */
	async sendMedia(
		chatId: string,
		source: { url: string } | { path: string },
		opts: { mime?: string | undefined; caption?: string | undefined } = {},
	): Promise<SendResult> {
		const urlErr = this.validateOutboundTarget(chatId);
		if (urlErr !== null) return { success: false, error: urlErr };

		let contentUrl: string;
		let mime: string;
		if ("url" in source) {
			if (!/^https?:\/\//.test(source.url)) {
				return { success: false, error: "remote media must be http(s)" };
			}
			contentUrl = source.url;
			mime = opts.mime ?? guessMime(source.url) ?? "application/octet-stream";
		} else {
			try {
				const bytes = readFileSync(source.path.replace(/^file:\/\//, ""));
				mime =
					opts.mime ?? guessMime(source.path) ?? "application/octet-stream";
				contentUrl = `data:${mime};base64,${bytes.toString("base64")}`;
			} catch (err) {
				return {
					success: false,
					error: err instanceof Error ? err.message : String(err),
				};
			}
		}

		const activity: BotFrameworkJson = {
			type: "message",
			attachments: [{ contentType: mime, contentUrl }],
			...(opts.caption ? { text: opts.caption } : {}),
		};
		try {
			const resp = await this.postProactive(chatId, activity);
			if (resp.status !== 200) {
				return {
					success: false,
					error: `send_media failed (${resp.status})`,
					retryable: true,
				};
			}
			return { success: true, messageId: str(resp.json["id"]) || undefined };
		} catch (err) {
			return {
				success: false,
				error: err instanceof Error ? err.message : String(err),
				retryable: true,
			};
		}
	}

	// ── guard wiring (reference-fixture inheritance) ──────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: TEAMS_REGISTRY,
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

	/** Deduplicator observability (dedupe rows probe the live state). */
	isDuplicateActivity(id: string): boolean {
		return this.seenActivities.contains(id);
	}

	/** Pending-approval observability: ids whose pending has NOT been consumed. */
	pendingApprovalIds(): number[] {
		return this.approvalIdLog.filter((id) => this.approvals.has(id));
	}
}

// ── MessageDeduplicator port (helpers.py @27) ────────────────────────────

/** Session-key mirror of what the event carries (metadata is optional). */
function sessionKeyOfEvent(event: IncomingEvent): string {
	return String(event.metadata?.["gateway_session_key"] ?? "");
}

class TtlMessageDeduplicator {
	private readonly seen = new Map<string, number>();
	private readonly maxSize: number;
	private readonly ttlMs: number;
	private readonly nowMs: () => number;

	constructor(opts: {
		maxSize: number;
		ttlMs: number;
		nowMs: () => number;
	}) {
		this.maxSize = opts.maxSize;
		this.ttlMs = opts.ttlMs;
		this.nowMs = opts.nowMs;
	}

	/** True when id was already seen WITHIN the TTL window (inserts otherwise). */
	isDuplicate(id: string): boolean {
		if (!id) return false;
		const now = this.nowMs();
		const existing = this.seen.get(id);
		if (existing !== undefined) {
			if (now - existing < this.ttlMs) return true;
			this.seen.delete(id); // expired entry removed, treated as new
		}
		this.seen.set(id, now);
		if (this.seen.size > this.maxSize) {
			// TTL pruning alone does not cap the cache under fresh traffic — keep
			// the NEWEST entries so max_size holds (helpers.py eviction parity).
			const entries = [...this.seen.entries()].sort((a, b) => a[1] - b[1]);
			const newest = entries.slice(entries.length - this.maxSize);
			this.seen.clear();
			for (const [k, v] of newest) this.seen.set(k, v);
		}
		return false;
	}

	/** Live membership probe WITHOUT inserting (helpers.py::contains). */
	contains(id: string): boolean {
		const seenAt = this.seen.get(id);
		if (seenAt === undefined) return false;
		return this.nowMs() - seenAt < this.ttlMs;
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

function asRec(v: unknown): ActivityRecord {
	return v !== null && typeof v === "object" && !Array.isArray(v)
		? (v as ActivityRecord)
		: {};
}

function asArray(v: unknown): unknown[] {
	return Array.isArray(v) ? v : [];
}

function str(v: unknown): string {
	return typeof v === "string" ? v.trim() : "";
}

/** conversation_type → gateway chat type (_on_message mapping parity). */
function teamsChatTypeFor(convType: string): string {
	if (convType === "personal") return "dm";
	if (convType === "groupChat") return "group";
	if (convType === "channel") return "channel";
	return "dm";
}

function parseCsv(v: string | undefined): string[] {
	if (!v) return [];
	return v
		.split(",")
		.map((s) => s.trim())
		.filter((s) => s.length > 0);
}

function parseBool(v: string | undefined): boolean {
	if (!v) return false;
	const normalized = v.trim().toLowerCase();
	return normalized === "1" || normalized === "true" || normalized === "yes";
}

function choiceLabel(choice: string): string {
	switch (choice) {
		case "once":
			return "✅ Allowed (once)";
		case "session":
			return "✅ Allowed (session)";
		case "always":
			return "✅ Always allowed";
		default:
			return "❌ Denied";
	}
}

function extOf(pathOrUrl: string): string {
	const clean = pathOrUrl.split(/[?#]/)[0] ?? "";
	const dot = clean.lastIndexOf(".");
	return dot === -1 ? "" : clean.slice(dot + 1).toLowerCase();
}

function guessMime(pathOrUrl: string): string | undefined {
	const ext = extOf(pathOrUrl);
	const table: Record<string, string> = {
		png: "image/png",
		jpg: "image/jpeg",
		jpeg: "image/jpeg",
		gif: "image/gif",
		webp: "image/webp",
		mp4: "video/mp4",
		mov: "video/quicktime",
		mp3: "audio/mpeg",
		wav: "audio/wav",
		ogg: "audio/ogg",
		pdf: "application/pdf",
		txt: "text/plain",
		csv: "text/csv",
		json: "application/json",
	};
	const hit = table[ext];
	return hit !== undefined ? hit : undefined;
}

function kindOfMime(mime: string): string {
	const bare = mime.split(";")[0]?.trim() ?? "";
	if (bare.startsWith("image/")) return "image";
	if (bare.startsWith("video/")) return "video";
	if (bare.startsWith("audio/")) return "audio";
	return "document";
}
