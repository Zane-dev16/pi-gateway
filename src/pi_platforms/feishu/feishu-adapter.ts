// pi_platforms/feishu/feishu-adapter — THE FEISHU/LARK CENSUS PORT (roadmap
// §Phase 6; A12 ride-along). Built ENTIRELY on the pi_platforms kit +
// pi_gateway seams over an in-process fake of the vendor long-conn protocol.
//
// Shape obligations (04 §3 matrix, Persistent WS column) realized for the
// lark_oapi ws client's actual semantics (READ-ONLY Hermes ground truth,
// cited file:symbol):
//   - Replay/dedup: NO resume cursor — the SDK redelivers unacked events
//     after reconnects; exactly-once downstream rides the PERSISTED 24h-TTL
//     message-id seen-set (adapter.py:_is_duplicate :4621, TTL :235,
//     persistence :4611/:4575).
//   - Heartbeat: ping/pong staleness reaps wedged sockets (websockets lib
//     ping_interval/ping_timeout defaults govern — manifest data).
//   - Reconnect ladder: FIXED interval (ws_reconnect_interval 120 s) with an
//     attempt budget (ws_reconnect_nonce 30) instead of exponential backoff;
//     server Retry-After close payloads remain AUTHORITATIVE overrides
//     (PROPOSED DEC-043/044 — see report).
//   - Markdown: WHOLE-MESSAGE decision locks per deliverText call —
//     markdown-shaped content ships as msg_type "post" with RAW bytes in md
//     rows, plain content as msg_type "text"; tables NEVER downgraded
//     (#52786); post-format rejection downgrades THAT chunk to stripped plain
//     text immediately (_POST_CONTENT_INVALID_RE :193). Chunk boundaries
//     cannot flip msg_type mid-send (#26841).
//   - Streaming: NONE declared (base supports_draft_streaming stays False —
//     Hermes parity). supportsDraftStreaming() returns False ALWAYS.
//
// A12 ingress classes ride the SAME two-guard pipeline as ordinary messages:
// generic card clicks → synthetic `/card …` COMMAND events (15-min token
// dedup); VC meeting invites → synthetic DM MessageEvents; Drive comments →
// gated prompt turns with comment-API delivery.

import {
	createHash,
	randomUUID,
	timingSafeEqual as nodeTimingSafe,
} from "node:crypto";
import { readFile, mkdir, writeFile } from "node:fs/promises";
import { basename, join as pathJoin } from "node:path";

import type {
	DraftFrameArgs,
	EditOptions,
	Metadata,
	SendResult,
	StreamEgressAdapter,
	StreamLogger,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	MessageHandler,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { SessionSource } from "../../pi_gateway/resolution/session-key.js";
import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	TokenLockManagerSeam,
	resolveEnablement,
	type CapabilityManifest,
	type ScopedSecretReader,
} from "../kit/index.js";
import { FormattingLadder } from "../kit/formatting-ladder.js";
import { chunkWithFenceCarry, type ChunkPlan } from "../kit/chunking.js";
import {
	classifySendError,
	sendWithRetry,
	plainTextFallbackBody,
	DELIVERY_FAILED_NOTICE,
} from "../kit/send-retry.js";
import { codePointLen, type LengthUnit } from "../kit/length-policy.js";

import type { AdapterClock } from "../persistent-ws/persistent-ws-adapter.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { ReconnectLadder } from "../persistent-ws/reconnect-ladder.js";

import {
	FEISHU_CARD_ACTION_DEDUP_TTL_MS,
	FEISHU_DEDUP_TTL_MS,
	FEISHU_MAX_MESSAGE_UNITS,
	FEISHU_MEDIA_BATCH_DELAY_MS,
	FEISHU_POST_CONTENT_INVALID_MARKER,
	FEISHU_PROCESSING_REACTION_CACHE_SIZE,
	FEISHU_SEND_ATTEMPTS,
	FEISHU_REACTION_FAILURE,
	FEISHU_REACTION_IN_PROGRESS,
	FEISHU_REPLY_FALLBACK_CODES,
	FEISHU_SENDER_NAME_TTL_MS,
	FEISHU_SPLIT_THRESHOLD_UNITS,
	FEISHU_TEXT_BATCH_DELAY_MS,
	FEISHU_TEXT_BATCH_MAX_CHARS,
	FEISHU_TEXT_BATCH_MAX_MESSAGES,
	FEISHU_TEXT_BATCH_SPLIT_DELAY_MS,
	FEISHU_WEBHOOK_DEFAULT_HOST,
	FEISHU_WEBHOOK_DEFAULT_PATH,
	FEISHU_WEBHOOK_DEFAULT_PORT,
	FEISHU_WEBHOOK_MAX_BODY_BYTES,
	FEISHU_WEBHOOK_RATE_LIMIT_MAX,
	FEISHU_WEBHOOK_RATE_MAX_KEYS,
	FEISHU_WEBHOOK_RATE_WINDOW_SECONDS,
	FEISHU_WS_PING_INTERVAL_MS,
	FEISHU_WS_PING_TIMEOUT_MS,
	FEISHU_WS_RECONNECT_ATTEMPTS,
	FEISHU_WS_RECONNECT_INTERVAL_MS,
} from "./manifest.js";
import { CardActionTokenStore, FeishuSeenMessageStore } from "./dedup.js";
import {
	buildGenericCardCommandText,
	buildResolvedApprovalCard,
	buildResolvedUpdatePromptCard,
	isInteractiveOperatorAuthorized,
	parseCardAction,
} from "./cards.js";
import {
	meetingDedupKey,
	parseMeetingInvitedEvent,
} from "./meeting-ingress.js";
import type { FeishuConnectionFactory } from "./fake-feishu.js";

// ── structural REST plane (vendor op level) ────────────────────────────────

export interface FeishuMessageSendOpts {
	/** Vendor receive_id_type enum incl. the thread-create leg
	 * (_build_create_message_request("thread_id", …) :4836). */
	receiveIdType: "chat_id" | "open_id" | "user_id" | "thread_id";
	receiveId: string;
	/** Vendor msg_type enum — text/post PLUS the media-bubble types shipped
	 * after upload (image/file/audio/media; _feishu_send_with_retry callers
	 * :2297/:4738/:4786/:4802). */
	msgType: "text" | "post" | "image" | "audio" | "media" | "file";
	/** The WIRE content string: plain text for the text lane, the JSON-STRING
	 * post payload ({"zh_cn":{"content":rows}}) for post, or the JSON string
	 * {image_key}/{file_key} for media bubbles (:4655/:4779/:4825). */
	content: string;
	replyToMessageId?: string | undefined;
	replyInThread?: boolean | undefined;
	uuid: string;
	/** Caller metadata passthrough (subject wire scripts key on it — e.g.
	 * forceFormattingError drives the markdown-rejection fake shape). */
	metadata?: Metadata | undefined;
}

/**
 * Vendor-op REST face. Tests bind the fake server (recordRest/scripted
 * behaviors); the conformance subject binds the harness FakePlatformWire so
 * egress capture rides the SHARED wire.
 */
export interface FeishuRestPlane {
	sendMessage(opts: FeishuMessageSendOpts): Promise<SendResult>;
	updateMessage(opts: {
		messageId: string;
		msgType: "text" | "post";
		content: string;
	}): Promise<SendResult>;
	addReaction(opts: {
		messageId: string;
		emojiType: string;
	}): Promise<SendResult>;
	removeReaction(opts: {
		messageId: string;
		reactionId: string;
	}): Promise<SendResult>;
	getBotInfo(): Promise<{
		openId: string;
		userId: string;
		name: string;
	} | null>;
	/** GET im/v1/messages/:id — reaction routing fetches the reacted-to
	 * message to verify THIS bot authored it and to recover chat context
	 * (adapter.py:_handle_reaction_event @2989; sender.id ≙ app id for bot
	 * messages — peer bots share sender_type="app" but differ on app id). */
	getMessage(messageId: string): Promise<{
		senderId: string;
		chatId: string;
		chatType: string;
	} | null>;
	/** Whether an explicit RICH probe lane is scripted (subject/test seam —
	 * mirrors the family richScriptedProbe pattern; production faces return
	 * false, which latches the §10.1 rich tier off without a roundtrip). */
	richScripted(): boolean;
	transmitRich(content: string, metadata: Metadata): Promise<SendResult>;
	/** POST im/v1/images — multipart upload, image_type enum value "message"
	 * (_FEISHU_IMAGE_UPLOAD_TYPE :203; _build_image_upload_body :5189).
	 * Returns the uploaded image_key on success. */
	createImage(opts: {
		imageType: string;
		filename: string;
		image: Uint8Array;
	}): Promise<SendResult & { imageKey?: string | undefined }>;
	/** POST im/v1/files — file_type stream|opus|mp4|pdf|doc|xls|ppt routing
	 * (:203–212/_resolve_outbound_file_routing :5234); duration attached ONLY
	 * when > 0 (_build_file_upload_body :5206). Returns file_key on success. */
	createFile(opts: {
		fileType: string;
		fileName: string;
		file: Uint8Array;
		durationMs?: number | undefined;
	}): Promise<SendResult & { fileKey?: string | undefined }>;
	/** GET im/v1/messages/:id/resources?type=image|file (:4001) — inbound
	 * image/file/audio/media bytes. */
	getMessageResource(opts: {
		messageId: string;
		fileKey: string;
		resourceType: "image" | "file";
	}): Promise<{
		bytes: Uint8Array;
		contentType: string;
		filename: string;
	} | null>;
	/** GET contact/v3/users/:id — display-name resolution with id-type
	 * routing open_id/union_id/user_id (:4205 _resolve_sender_name_from_api);
	 * null on ANY failure (silent — never blocks the pipeline). */
	resolveUserName(opts: {
		userId: string;
		userIdType: "open_id" | "union_id" | "user_id";
	}): Promise<string | null>;
	/** GET bot/v3/bots/basic_batch?bot_ids=… (:4257 _fetch_bot_names) — bot
	 * names divert here because contact/v3 has no bot rows. */
	resolveBotNames(
		botIds: readonly string[],
	): Promise<Record<string, string> | null>;
	/** GET im/v1/chats/:chat_id (:2424 get_chat_info) — real chat metadata
	 * for source attribution; null on failure (fallback name = chat id). */
	getChat(chatId: string): Promise<{ name: string; chatType: string } | null>;
}

/** Command registry — the five-command conformance registry (family parity). */
export const FEISHU_REGISTRY: CommandRegistry = [
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

/** Required secrets (plugin.yaml requires_env; loud-disable row). */
export const FEISHU_REQUIRED_SECRETS = [
	"FEISHU_APP_ID",
	"FEISHU_APP_SECRET",
] as const;

export interface FeishuAdapterDeps {
	manifestName?: string | undefined;
	capabilities?: Partial<CapabilityManifest> | undefined;
	scalarMaxUnits?: number | undefined;
	logger?: StreamLogger | undefined;

	transport: FeishuConnectionFactory;
	rest: FeishuRestPlane;
	clock?: AdapterClock | undefined;
	secretReader?: ScopedSecretReader | undefined;
	optionalEnvReader?: ((name: string) => string | undefined) | undefined;

	/** Our identity (hydrated from GET /bot/v3/info at connect when absent). */
	botIdentity?: { openId: string; userId: string; name: string } | undefined;
	/** Authorization inputs (:2771/_admit). */
	admins?: ReadonlySet<string> | undefined;
	allowedUsers?: ReadonlySet<string> | undefined;
	/** Per-chat group rules + default policy (group_rules/default_group_policy). */
	groupRules?:
		| ReadonlyMap<
				string,
				{
					policy: string;
					allowlist?: ReadonlySet<string>;
					blacklist?: ReadonlySet<string>;
					requireMention?: boolean | undefined;
				}
		  >
		| undefined;
	defaultGroupPolicy?: string | undefined;
	requireMention?: boolean | undefined;
	/** FEISHU_REACTIONS toggle (default true). */
	reactionsEnabled?: boolean | undefined;

	/** Persisted dedup state path (temp dir in tests; scoped home otherwise). */
	dedupStatePath?: string | undefined;
	/** Inbound image:/file: resource cache root (:4001 download → local
	 * cached path; whatsapp-cloud mediaCacheDir parity). Absent ⇒ caching
	 * disabled and scheme refs pass through untouched. */
	mediaCacheDir?: string | undefined;
	/** Webhook route path override (FEISHU_WEBHOOK_PATH; rate-key input). */
	webhookPath?: string | undefined;
	/** Tuning (manifest defaults; injected-clock determinism in tests). */
	pingIntervalMs?: number | undefined;
	pingTimeoutMs?: number | undefined;
	watchdogIntervalMs?: number | undefined;
	textBatchDelayMs?: number | undefined;
	mediaBatchDelayMs?: number | undefined;
	ladder?:
		| {
				intervalMs?: number | undefined;
				attemptBudget?: number | undefined;
				sleep?: ((ms: number) => Promise<void>) | undefined;
		  }
		| undefined;
}

export type FeishuRunPhase =
	| "new"
	| "connecting"
	| "live"
	| "reconnect-scheduled"
	| "fatal"
	| "stopped";

interface PendingBatchEntry {
	event: IncomingEvent;
	sessionKey: string;
}

/**
 * THE markdown decision regex — VERBATIM 13-alternative transcription of
 * adapter.py:_MARKDOWN_HINT_RE (@168, re.MULTILINE): pipe table (header +
 * separator pair), ATX headings, bullet/ordered lists, hr rule, fenced code,
 * inline code, bold, strike, underline (<u>), single-* italic, links, and
 * blockquotes. Every alternative matters: dropping one silently reclassifies
 * markdown-shaped bodies as msg_type=text.
 */
const MARKDOWN_HINT_RE =
	/(^\|.*\|\s*\n\|[-:|\s]+\|)|(^#{1,6}\s)|(^\s*[-*]\s)|(^\s*\d+\.\s)|(^\s*---+\s*$)|(```)|(`[^`\n]+`)|(\*\*[^*\n].+?\*\*)|(~~[^~\n].+?~~)|(<u>.+?<\/u>)|(\*[^*\n]+\*)|(\[[^\]]+\]\([^)]+\))|(^>\s)/m;

// ── post payload construction (module scope; adapter.py :188–190/:580–648) ──

/** _MARKDOWN_LINK_RE :188 — [label](url). */
const MARKDOWN_LINK_RE = /\[([^\]]+)\]\(([^)]+)\)/g;
/** _MARKDOWN_FENCE_OPEN_RE :189 — an OPENING fence (language allowed). */
const MARKDOWN_FENCE_OPEN_RE = /^```([^\n`]*)\s*$/;
/** _MARKDOWN_FENCE_CLOSE_RE :190 — a BARE closing fence. */
const MARKDOWN_FENCE_CLOSE_RE = /^```\s*$/;

/** One Feishu post element row entry — tag plus free-form string attrs
 * ({tag:"md",text} prose rows, {tag:"img",image_key}, {tag:"media",…}). */
export type FeishuPostElement = { tag: string } & Record<string, string>;
/** One Feishu post element row ([{tag:"md",text}, …]). */
export type FeishuPostRow = FeishuPostElement[];

/** Python str.splitlines() boundary set (incl. \x85/\u2028/\u2029). */
const SPLITLINES_RE = /\r\n|\r|\n|\v|\f|\x1c|\x1d|\x1e|\x85|\u2028|\u2029/;

/**
 * _build_markdown_post_rows (:604) — build post rows while ISOLATING fenced
 * code blocks: Feishu's `md` renderer can swallow trailing content when a
 * fenced block rides inside one large markdown element, so the reply splits
 * at REAL fence lines (prose before/after stays visible; code keeps a
 * dedicated row).
 */
export function buildMarkdownPostRows(content: string): FeishuPostRow[] {
	if (!content) return [[{ tag: "md", text: "" }]];
	if (!content.includes("```")) return [[{ tag: "md", text: content }]];

	const rows: FeishuPostRow[] = [];
	let current: string[] = [];
	let inCodeBlock = false;

	const flushCurrent = (): void => {
		if (current.length === 0) return;
		const segment = current.join("\n");
		if (segment.trim() !== "") rows.push([{ tag: "md", text: segment }]);
		current = [];
	};

	for (const rawLine of content.split(SPLITLINES_RE)) {
		const strippedLine = rawLine.trim();
		const isFence = inCodeBlock
			? MARKDOWN_FENCE_CLOSE_RE.test(strippedLine)
			: MARKDOWN_FENCE_OPEN_RE.test(strippedLine);

		if (isFence) {
			if (!inCodeBlock) flushCurrent();
			current.push(rawLine);
			inCodeBlock = !inCodeBlock;
			if (!inCodeBlock) flushCurrent();
			continue;
		}
		current.push(rawLine);
	}

	flushCurrent();
	return rows.length > 0 ? rows : [[{ tag: "md", text: content }]];
}

/**
 * _build_markdown_post_payload (:580) — the WIRE content for msg_type=post:
 * a JSON STRING {"zh_cn":{"content":rows}} (ensure_ascii=False parity —
 * JSON.stringify never escapes non-ASCII).
 */
export function buildMarkdownPostPayload(content: string): string {
	return JSON.stringify({ zh_cn: { content: buildMarkdownPostRows(content) } });
}

// ── plain-text stripper (_strip_markdown_to_plain_text :552 + shared
//    gateway.platforms.helpers.strip_markdown :196) — DOWNGRADE LANES ONLY ──

/**
 * THE downgrade stripper. Feishu-specific patterns first (CRLF normalise,
 * link → 'text (url)', blockquote markers, hr rule, strikethrough, <u>),
 * then the SHARED strip_markdown pass (bold/italic/bold-under/italic-under,
 * fenced + inline code removal, ATX headings, links, newline collapse,
 * trim). Order matters: the link REWRITE runs before the link REMOVAL so
 * rewritten 'text (url)' bodies survive.
 */
export function stripFeishuMarkdownToPlainText(text: string): string {
	let plain = text.replaceAll("\r\n", "\n");
	plain = plain.replace(
		MARKDOWN_LINK_RE,
		(_match: string, label: string, url: string) => `${label} (${url.trim()})`,
	);
	plain = plain.replace(/^>\s?/gm, "");
	plain = plain.replace(/^\s*---+\s*$/gm, "---");
	plain = plain.replace(/~~([^~\n]+)~~/g, "$1");
	plain = plain.replace(/<u>([\s\S]*?)<\/u>/g, "$1");
	// gateway.platforms.helpers.strip_markdown (:196)
	plain = plain.replace(/\*\*(.+?)\*\*/gs, "$1");
	plain = plain.replace(/\*(.+?)\*/gs, "$1");
	plain = plain.replace(/\b__(?![\s_])(.+?)(?<![\s_])__\b/gs, "$1");
	plain = plain.replace(/\b_(?![\s_])(.+?)(?<![\s_])_\b/gs, "$1");
	plain = plain.replace(/```[a-zA-Z0-9_+-]*\n?/g, "");
	plain = plain.replace(/`(.+?)`/g, "$1");
	plain = plain.replace(/^#{1,6}\s+/gm, "");
	plain = plain.replace(/\[([^\]]+)\]\([^)]+\)/g, "$1");
	plain = plain.replace(/\n{3,}/g, "\n\n");
	return plain.trim();
}

// ── inbound normalization shape (module scope; one arrival snapshot) ──────

interface NormalizedInbound {
	messageId: string;
	text: string;
	messageType: "text" | "photo" | "voice" | "video" | "document" | "other";
	mediaUrls: string[];
	mediaTypes: string[];
	chatId: string;
	chatType: "dm" | "group";
	threadId?: string | undefined;
	parentId?: string | undefined;
	senderIds: { openId: string; userId: string; unionId: string };
	senderType: "user" | "bot" | "app";
	mentionsSelf: boolean;
	rawMentionsAll: boolean;
	source: SessionSource;
	/** In-flight resource downloads (media dispatch awaits them). */
	resourceTask?: Promise<void> | undefined;
}

export class FeishuAdapter
	extends BasePlatformAdapter
	implements StreamEgressAdapter
{
	protected readonly rest: FeishuRestPlane;
	protected readonly clock: AdapterClock;
	private readonly transportFactory: FeishuConnectionFactory;
	private readonly optionalEnvReader: (name: string) => string | undefined;

	// ── identity / authorization ──────────────────────────────────────────
	private botOpenId: string;
	private botUserId: string;
	/** FEISHU_APP_ID — reaction routing compares GET-message sender ids
	 * against it (:3009 sender.id ≙ app id for bot-authored messages). */
	private readonly appId: string;
	private readonly admins: ReadonlySet<string>;
	private readonly allowedUsers: ReadonlySet<string>;
	private readonly groupRules: FeishuAdapterDeps["groupRules"];
	private readonly defaultGroupPolicy: string;
	private readonly requireMentionGlobal: boolean;
	readonly reactionsEnabled: boolean;

	// ── replay/dedup state (the subscription-shape core) ──────────────────
	readonly seenMessages: FeishuSeenMessageStore;
	readonly cardTokens: CardActionTokenStore;
	private readonly dedupStatePath: string | undefined;

	// ── ws session state ──────────────────────────────────────────────────
	private socket: import("./fake-feishu.js").FeishuClientSocket | null = null;
	private connectedAtMs: number | null = null;
	private phase: FeishuRunPhase = "new";
	private running = false;
	private reconnectPending = false;
	private watchdogGeneration = 0;
	readonly reconnectLadder: ReconnectLadder;
	readonly reconnectLog: Array<{ delayMs: number; authoritative: boolean }> =
		[];
	lastCapturedRetryAfterSeconds: number | null = null;

	private readonly pingIntervalMs: number;
	private readonly pingTimeoutMs: number;
	private readonly watchdogIntervalMs: number;

	// ── webhook listener surface (FEISHU_WEBHOOK_HOST/PORT/PATH :1650–1658) ─
	readonly webhookHost: string;
	readonly webhookPort: number;
	/** THE configured route path — composite rate-key component (:3562). */
	readonly webhookPath: string;
	/** Last composed `{app}:{path}:{ip}` key (rate-limiter observability). */
	lastWebhookRateKey = "";
	/** Inbound resource cache root (absent ⇒ caching disabled). */
	readonly mediaCacheDir: string | undefined;

	// ── inbound pipeline observability ────────────────────────────────────
	readonly inboundLog: Array<{ eventId: string; eventType: string }> = [];
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly clarifyArmed = new Set<string>();
	readonly resolvedFamilies: string[] = [];
	private routerResolved: string[] = [];
	private allowAllClickers = true;
	readonly droppedEvents: Array<{ eventId: string; reason: string }> = [];
	readonly admissionLog: Array<{ messageId: string; verdict: string }> = [];

	// ── batching state ─────────────────────────────────────────────────────
	private readonly textBatches = new Map<string, PendingBatchEntry[]>();
	private readonly textBatchTimers = new Map<string, () => void>();
	private readonly textBatchDelayMs: number;
	private readonly mediaBatchDelayMs: number;

	// ── egress machinery ───────────────────────────────────────────────────
	private readonly cp: EgressChokepoint;
	/** Whole-message markdown decision for the CURRENT deliverText call. */
	private pendingPreferPost: boolean | null = null;
	/** Per-chunk post→text downgrade memory (sticky across the chunk loop). */
	/**
	 * THE session-scoped §10.1 ladder — ONE instance so the rich downgrade
	 * latch persists across chunks/sends (probe once per process). Its tier-1
	 * lane IS wireRich (the POST probe).
	 */
	private formatLadderInstance: FormattingLadder | null = null;
	private ladderChatId = "";
	/** Rich-endpoint roundtrips actually made (A23 freeze observability). */
	richWireAttempts = 0;
	private chunkDowngradedToText = false;
	/** Chunks already downgraded carry plain text — never re-decide. */
	/** §6.1 plain-fallback leg forces the TEXT lane (parse-failure resend is a
	 * downgrade path — :2461 format_message semantics, never re-posted). */
	private forcePlainTextLane = false;

	// ── interactive surfaces (§9; DEC-016 parallel mechanism) ──────────────
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;
	/** Approval id → state (session_key/message_id/chat_id) for chat-match checks. */
	readonly approvalState = new Map<
		number,
		{ sessionKey: string; chatId: string }
	>();
	readonly resolvedApprovalCards: Array<{
		approvalId: number;
		choice: string;
		card: ReturnType<typeof buildResolvedApprovalCard>;
	}> = [];
	readonly updatePromptAnswers: Array<{ promptId: number; answer: string }> =
		[];
	readonly cardCommandAudit: Array<{ token: string; text: string }> = [];
	private readonly updatePrompts = new OneShotPendingStore();
	private approvalSeq = 1000;

	/** Optional e2e turn driver (subject/test wiring). */
	turnDriver:
		| ((event: IncomingEvent, ctxText: string) => Promise<string | null>)
		| null = null;

	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};
	private holding = false;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	/** Reaction lifecycle LRU (message_id → reaction_id). */
	private readonly pendingReactions = new Map<string, string>();

	// ── directory caches (:238/:2424/:4205) ───────────────────────────────
	/** Sender display names, 10-min TTL ("" cached ⇒ known nameless). */
	private readonly senderNameCache = new Map<
		string,
		{ name: string; expireAtMs: number }
	>();
	/** Chat metadata (im/v1/chats/:id), session-lived. */
	private readonly chatInfoCache = new Map<
		string,
		{ name: string; chatType: string }
	>();

	constructor(deps: FeishuAdapterDeps) {
		super({
			manifestName: deps.manifestName ?? "feishu",
			capabilities: {
				...(deps.capabilities ?? {}),
			},
			lengthUnit: "chars",
			scalarMaxUnits: deps.scalarMaxUnits ?? FEISHU_MAX_MESSAGE_UNITS,
			logger: deps.logger,
		});
		this.transportFactory = deps.transport;
		this.rest = deps.rest;
		this.clock = deps.clock ?? new ManualClock();
		this.optionalEnvReader = deps.optionalEnvReader ?? (() => undefined);
		const ident = deps.botIdentity;
		this.botOpenId = ident?.openId ?? "";
		this.botUserId = ident?.userId ?? "";
		this.appId = deps.secretReader?.(FEISHU_REQUIRED_SECRETS[0]) ?? "";
		this.admins = deps.admins ?? new Set();
		this.allowedUsers = deps.allowedUsers ?? new Set();
		this.groupRules = deps.groupRules;
		this.defaultGroupPolicy = deps.defaultGroupPolicy ?? "allowlist";
		this.requireMentionGlobal = deps.requireMention ?? true;
		this.reactionsEnabled = deps.reactionsEnabled ?? true;
		this.dedupStatePath = deps.dedupStatePath;
		this.textBatchDelayMs = deps.textBatchDelayMs ?? FEISHU_TEXT_BATCH_DELAY_MS;
		this.mediaBatchDelayMs =
			deps.mediaBatchDelayMs ?? FEISHU_MEDIA_BATCH_DELAY_MS;
		this.pingIntervalMs = deps.pingIntervalMs ?? FEISHU_WS_PING_INTERVAL_MS;
		this.pingTimeoutMs = deps.pingTimeoutMs ?? FEISHU_WS_PING_TIMEOUT_MS;
		this.watchdogIntervalMs =
			deps.watchdogIntervalMs ?? Math.min(5_000, this.pingIntervalMs);
		this.mediaCacheDir = deps.mediaCacheDir;
		this.webhookHost =
			deps.optionalEnvReader === undefined
				? FEISHU_WEBHOOK_DEFAULT_HOST
				: deps.optionalEnvReader("FEISHU_WEBHOOK_HOST")?.trim() ||
					FEISHU_WEBHOOK_DEFAULT_HOST;
		const portRaw =
			deps.optionalEnvReader === undefined
				? undefined
				: deps.optionalEnvReader("FEISHU_WEBHOOK_PORT");
		const portParsed = Number.parseInt(portRaw ?? "", 10);
		this.webhookPort = Number.isFinite(portParsed)
			? portParsed
			: FEISHU_WEBHOOK_DEFAULT_PORT;
		this.webhookPath =
			deps.webhookPath?.trim() ||
			(deps.optionalEnvReader === undefined
				? ""
				: (deps.optionalEnvReader("FEISHU_WEBHOOK_PATH")?.trim() ?? "")) ||
			FEISHU_WEBHOOK_DEFAULT_PATH;
		this.seenMessages = new FeishuSeenMessageStore({
			ttlMs: FEISHU_DEDUP_TTL_MS,
			nowMs: this.clock.nowMs,
			...(deps.dedupStatePath !== undefined
				? { statePath: deps.dedupStatePath }
				: {}),
		});
		this.cardTokens = new CardActionTokenStore({
			ttlMs: FEISHU_CARD_ACTION_DEDUP_TTL_MS,
			nowMs: this.clock.nowMs,
		});
		this.reconnectLadder = new ReconnectLadder({
			baseDelayMs: deps.ladder?.intervalMs ?? FEISHU_WS_RECONNECT_INTERVAL_MS,
			maxDelayMs: deps.ladder?.intervalMs ?? FEISHU_WS_RECONNECT_INTERVAL_MS,
			jitterFraction: 0, // fixed-interval ladder (SDK reconnect shape)
			// BUGFIX: the ladder MUST sleep on the INJECTED clock — a wall-clock
			// setTimeout made reconnects untestable under virtual time.
			sleep: deps.ladder?.sleep ?? ((ms) => this.clock.sleepMs(ms)),
		});

		// §11 step 3: required-secret enablement — missing ⇒ LOUD disable.
		if (deps.secretReader !== undefined) {
			const enablement = resolveEnablement(
				{
					name: this.manifestName,
					description: "feishu census port",
					transportShape: "ws",
					requiresEnv: FEISHU_REQUIRED_SECRETS.map((name) => ({ name })),
					capabilities: {},
				},
				deps.secretReader,
			);
			if (!enablement.enabled && enablement.reason) {
				this.lifecycle.disable(enablement.reason);
				this.phase = "stopped";
			} else {
				for (const spec of FEISHU_REQUIRED_SECRETS) {
					const value = deps.secretReader(spec);
					if (value !== undefined) this.registerLogSecret(value);
				}
			}
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // NO relay lanes exist on feishu
			transmitSend: async (chatId, content, metadata) =>
				this.transmitFeishuMessage(chatId, content, metadata),
			transmitEdit: async (_chatId, messageId, content, _opts) =>
				this.editMessageDecided(messageId, content),
			transmitSeal: async () =>
				// No native stream exists to seal — honest failure, never armed.
				Promise.resolve({ success: false, error: "no native stream lane" }),
		});

		this.router = new CallbackQueryRouter({
			stores: {
				approvals: this.approvals,
				slashConfirms: this.slashConfirms,
				appr: this.appr,
				clarify: this.clarify,
			},
			authorizer: () => this.allowAllClickers,
			onExecApproval: async (sessionKey) => {
				this.resolvedFamilies.push("ea");
				this.routerResolved.push(`ea:${sessionKey}`);
				return "ok";
			},
			onSlashConfirm: async (sessionKey, _id, choice) => {
				this.resolvedFamilies.push("sc");
				this.routerResolved.push(`sc:${sessionKey}:${choice}`);
				return "ok";
			},
			onClarifyChoice: async (sessionKey, _id, idx) => {
				this.resolvedFamilies.push("cl");
				this.routerResolved.push(`cl:${sessionKey}:${idx}`);
				return `answer-${idx}`;
			},
			onWhatsappApproval: async (sessionKey, _id, approve) => {
				this.resolvedFamilies.push("appr");
				this.routerResolved.push(`appr:${sessionKey}:${approve}`);
				return "ok";
			},
			onPickerNav: async (parsed) => ({
				answerText: `nav:${parsed.family}`,
				hostEditText: JSON.stringify(parsed),
			}),
		});
	}

	// ══════════════════════════════════════════════════════════════════════
	// Transport lifecycle (long-conn ws)
	// ══════════════════════════════════════════════════════════════════════

	get currentPhase(): FeishuRunPhase {
		return this.phase;
	}

	get isLive(): boolean {
		return (
			this.socket !== null &&
			this.socket.readyState === 1 &&
			this.phase === "live"
		);
	}

	async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		this.running = true;
		this.phase = "connecting";
		return this.openSocket();
	}

	async disconnect(): Promise<void> {
		this.running = false;
		this.phase = "stopped";
		this.watchdogGeneration += 1;
		const sock = this.socket;
		this.socket = null;
		sock?.close(1000);
		// Disconnect-time persistence (:4611 _persist_seen_message_ids).
		if (this.dedupStatePath !== undefined) {
			this.seenMessages.persist(this.dedupStatePath);
		}
	}

	private openSocket(): Promise<boolean> {
		if (this.socket !== null && this.socket.readyState === 1) {
			return Promise.resolve(true);
		}
		this.phase = "connecting";
		return new Promise<boolean>((resolve) => {
			let settled = false;
			const settleOnce = (v: boolean): void => {
				if (settled) return;
				settled = true;
				resolve(v);
			};
			this.socket = this.transportFactory.connect({
				onOpen: () => {
					this.connectedAtMs = this.clock.nowMs();
					// Subscribe handshake — ONLY once OPEN (a client cannot send
					// during CONNECTING; the fake server flips subscribed here).
					try {
						this.socket?.send({ type: "subscribe", service: "im" });
						this.phase = "live"; // session healthy
					} catch {
						/* refusal path closes asynchronously */
					}
					// Hydrate bot identity lazily (best-effort; never blocks).
					void this.hydrateBotIdentity();
					this.startWatchdog();
					settleOnce(true);
				},
				onFrame: (frame) => void this.onSocketFrame(frame),
				onClose: (info) => {
					settleOnce(false);
					this.onSocketClose(info);
				},
				onError: () => {
					/* close follows error on this plane */
				},
			});
		});
	}

	/** Best-effort bot identity hydration (:4497 _hydrate_bot_identity). */
	private async hydrateBotIdentity(): Promise<void> {
		if (this.botOpenId !== "") return;
		try {
			const info = await this.rest.getBotInfo();
			if (info === null) return;
			if (this.botOpenId === "" && info.openId) this.botOpenId = info.openId;
			if (this.botUserId === "" && info.userId) this.botUserId = info.userId;
		} catch {
			/* identity unknown ⇒ self-filter falls back to sender_type */
		}
	}

	private startWatchdog(): void {
		const gen = ++this.watchdogGeneration;
		void (async () => {
			while (this.running && gen === this.watchdogGeneration) {
				await this.clock.sleepMs(this.watchdogIntervalMs);
				if (!this.running || gen !== this.watchdogGeneration) return;
				this.runWatchdogTick();
			}
		})();
	}

	/** Drive one manual watchdog pass (tests invoke via the injected clock). */
	runWatchdogTick(): void {
		const sock = this.socket;
		if (sock === null || sock.readyState !== 1) {
			if (this.running && !this.reconnectPending)
				void this.scheduleReconnect(null);
			return;
		}
		const now = this.clock.nowMs();
		const pingImpl = sock as import("./fake-feishu.js").FeishuClientSocket & {
			lastPingSentAt: number | null;
			lastPongAt: number | null;
		};
		if (
			pingImpl.lastPingSentAt === null ||
			now - pingImpl.lastPingSentAt >= this.pingIntervalMs
		) {
			sock.ping();
		}
		const lastPong = pingImpl.lastPongAt;
		const staleBound = this.pingIntervalMs + this.pingTimeoutMs;
		if (lastPong === null) {
			// No first pong yet — suspicious only after interval+timeout past connect.
			if (
				this.connectedAtMs !== null &&
				now - this.connectedAtMs > staleBound * 2
			)
				this.reapStaleSocket("ping/pong stale (no first pong)");
			return;
		}
		if (now - lastPong > staleBound) this.reapStaleSocket("ping/pong stale");
	}

	private reapStaleSocket(reason: string): void {
		this.logger?.warn?.(
			`${this.manifestName}: reaping stale socket — ${reason}`,
		);
		this.socket?.close(4000);
	}

	private onSocketClose(info: {
		code: number;
		retryAfterSeconds?: number | undefined;
	}): void {
		this.socket = null;
		this.connectedAtMs = null;
		if (
			info.retryAfterSeconds !== undefined &&
			info.retryAfterSeconds !== null
		) {
			this.lastCapturedRetryAfterSeconds = info.retryAfterSeconds;
		}
		if (this.phase !== "stopped") this.phase = "reconnect-scheduled";
		if (this.running)
			void this.scheduleReconnect(info.retryAfterSeconds ?? null);
	}

	private async scheduleReconnect(
		retryAfterSeconds: number | null,
	): Promise<void> {
		if (!this.running || this.reconnectPending) return;
		if (
			this.reconnectLadder.attemptCount >= this.reconnectLadderAttemptBudget()
		) {
			this.phase = "fatal";
			this.lifecycle.markFatal({
				kind: "transport_fatal",
				detail: `ws reconnect attempts exhausted (${FEISHU_WS_RECONNECT_ATTEMPTS})`,
			} as never);
			return;
		}
		this.reconnectPending = true;
		const step = await this.reconnectLadder.wait(retryAfterSeconds);
		this.reconnectLog.push({
			delayMs: step.delayMs,
			authoritative: step.authoritative,
		});
		this.reconnectPending = false;
		if (!this.running) return;
		await this.openSocket();
	}

	private reconnectLadderAttemptBudget(): number {
		return FEISHU_WS_RECONNECT_ATTEMPTS;
	}

	// ══════════════════════════════════════════════════════════════════════
	// Frame routing (the event-subscription surface)
	// ══════════════════════════════════════════════════════════════════════

	private async onSocketFrame(frame: Record<string, unknown>): Promise<void> {
		const type = frame["type"];
		if (type === "pong") return;
		if (type === "subscribe_result") return;
		if (type !== "event") return;
		const header = (frame["header"] ?? {}) as Record<string, unknown>;
		const eventType = String(header["event_type"] ?? "");
		const eventId = String(header["event_id"] ?? "");
		const event = frame["event"] as Record<string, unknown> | undefined;

		switch (eventType) {
			case "im.message.receive_v1":
				await this.onImMessage(eventId, event ?? {});
				return;
			case "im.message.reaction.created_v1":
				await this.onReactionEvent(
					"im.message.reaction.created_v1",
					event ?? {},
				);
				return;
			case "im.message.reaction.deleted_v1":
				await this.onReactionEvent(
					"im.message.reaction.deleted_v1",
					event ?? {},
				);
				return;
			case "card.action.trigger":
				await this.handleCardActionTrigger(eventId, event ?? {});
				return;
			case "drive.notice.comment_add_v1":
				this.inboundLog.push({ eventId, eventType });
				await this.onDriveComment?.(eventId, event ?? {});
				return;
			case "vc.bot.meeting_invited_v1":
				this.inboundLog.push({ eventId, eventType });
				// The FULL FRAME rides in — root.header.event_id feeds the
				// vc_invite:{event_id} dedup key (:131/:159); the bare inner event
				// never carries it.
				await this.onMeetingInvited({
					header: { event_id: eventId },
					event: event ?? {},
				});
				return;
			default:
				// Unwired kinds tolerated (read/recall/member events log-only).
				this.inboundLog.push({ eventId, eventType });
		}
	}

	/** A12 hook point — bound by the wiring layer (comment-ingress). */
	onDriveComment:
		| ((eventId: string, event: Record<string, unknown>) => Promise<void>)
		| null = null;

	// ── im.message.receive_v1 ─────────────────────────────────────────────

	private async onImMessage(
		eventId: string,
		event: Record<string, unknown>,
	): Promise<void> {
		const message = (event["message"] ?? {}) as Record<string, unknown>;
		const sender = (event["sender"] ?? {}) as Record<string, unknown>;
		const messageId = String(message["message_id"] ?? eventId);

		// Dedup BEFORE dispatch (at-least-once redelivery suppression).
		if (this.seenMessages.isDuplicate(messageId)) return;
		if (this.dedupStatePath !== undefined) {
			this.seenMessages.persist(this.dedupStatePath);
		}

		const normalized = this.normalizeImMessage(messageId, message, sender);
		if (normalized === null) return;
		const verdict = this.admit(
			normalized.senderIds,
			normalized.senderType,
			normalized,
		);
		this.admissionLog.push({ messageId, verdict });
		if (verdict !== "allow") {
			this.droppedEvents.push({ eventId: messageId, reason: verdict });
			return;
		}
		this.inboundLog.push({
			eventId: messageId,
			eventType: "im.message.receive_v1",
		});

		// Sender-name cache warm (:4149 _resolve_sender_profile runs for EVERY
		// inbound message so card-click attribution reads a WARM cache).
		const nameLookupId =
			normalized.senderIds.openId || normalized.senderIds.userId;
		if (nameLookupId !== "") {
			void this.resolveSenderName(
				nameLookupId,
				normalized.senderType !== "user",
			).catch(() => {});
		}

		if (
			normalized.messageType === "text" ||
			normalized.messageType === "other"
		) {
			await this.enqueueTextBatch(normalized);
		} else {
			// Inbound image:/file:/audio:/media: refs download to the local
			// media cache BEFORE dispatch (:4001/:4032); failures keep the
			// vendor ref (graceful degradation).
			normalized.resourceTask = this.cacheInboundResources(normalized);
			await this.enqueueMediaBatch(normalized);
		}
	}

	/** Download audit (feishu-2 observability). */
	readonly resourceCacheLog: Array<{
		fileKey: string;
		path: string;
	}> = [];

	/**
	 * Fetch every scheme-ref on the event and rewrite it to the cached local
	 * path (_download_feishu_image :3960 / _download_feishu_message_resource
	 * :4001). Silent-failure per ref; the vendor ref survives any error.
	 */
	private async cacheInboundResources(n: NormalizedInbound): Promise<void> {
		if (this.mediaCacheDir === undefined || n.mediaUrls.length === 0) return;
		await mkdir(this.mediaCacheDir, { recursive: true });
		for (let i = 0; i < n.mediaUrls.length; i++) {
			const ref = n.mediaUrls[i] ?? "";
			const colon = ref.indexOf(":");
			if (colon <= 0) continue;
			const scheme = ref.slice(0, colon);
			const fileKey = ref.slice(colon + 1);
			if (fileKey === "") continue;
			const resourceType = scheme === "image" ? "image" : "file";
			try {
				const res = await this.rest.getMessageResource({
					messageId: n.messageId,
					fileKey,
					resourceType,
				});
				if (res === null) continue;
				const safeBase = safeCacheFilename(
					res.filename || `${scheme}_${fileKey}`,
				);
				const outPath = pathJoin(
					this.mediaCacheDir,
					`${safeBase}${safeBase.includes(".") ? "" : extensionForContentType(res.contentType)}`,
				);
				await writeFile(outPath, res.bytes);
				n.mediaUrls[i] = outPath;
				this.resourceCacheLog.push({ fileKey, path: outPath });
			} catch {
				/* silent-failure parity (:4041/:4096) — ref stays vendor-shaped */
			}
		}
	}

	// ── normalization ─────────────────────────────────────────────────────

	private normalizeImMessage(
		messageId: string,
		message: Record<string, unknown>,
		sender: Record<string, unknown>,
	): NormalizedInbound | null {
		let content: Record<string, unknown> = {};
		try {
			const parsed: unknown = JSON.parse(String(message["content"] ?? "{}"));
			if (parsed !== null && typeof parsed === "object")
				content = parsed as Record<string, unknown>;
		} catch {
			content = {};
		}
		const messageTypeRaw = String(message["message_type"] ?? "text");
		const chatType =
			String(message["chat_type"] ?? "p2p") === "p2p" ? "dm" : "group";
		const chatId = String(message["chat_id"] ?? "");
		const senderIds = {
			openId: String(
				(sender["sender_id"] as Record<string, unknown> | undefined)?.[
					"open_id"
				] ?? "",
			),
			userId: String(
				(sender["sender_id"] as Record<string, unknown> | undefined)?.[
					"user_id"
				] ?? "",
			),
			unionId: String(
				(sender["sender_id"] as Record<string, unknown> | undefined)?.[
					"union_id"
				] ?? "",
			),
		};
		const senderType = String(sender["sender_type"] ?? "user") as
			| "user"
			| "bot"
			| "app";

		// Mentions map (@_user_N → @Name rewrite; @_all synthesized).
		const mentions = Array.isArray(message["mentions"])
			? (message["mentions"] as Array<Record<string, unknown>>)
			: [];
		const mentionByKey = new Map<string, string>();
		for (const m of mentions) {
			mentionByKey.set(String(m["key"] ?? ""), String(m["name"] ?? ""));
		}

		let text = "";
		let messageType: NormalizedInbound["messageType"] = "text";
		let mediaUrls: string[] = [];
		let mediaTypes: string[] = [];
		let mentionsSelf = false;

		if (messageTypeRaw === "text") {
			text = String(content["text"] ?? "");
			for (const [key, name] of mentionByKey) {
				text = text.split(key).join(`@${name}`);
				if (this.mentionMatchesSelf(key, name)) {
					mentionsSelf = true;
					// Strip the self-mention token WITHOUT dynamic regex (ReDoS-safe).
					text = text.split(`@${name}`).join(" ").trim();
				}
			}
			if (text.includes("@_all")) mentionsSelf = true;
		} else if (messageTypeRaw === "post") {
			text = this.renderPostContent(content);
			for (const [key, name] of mentionByKey) {
				if (this.mentionMatchesSelf(key, name)) mentionsSelf = true;
			}
		} else if (messageTypeRaw === "image") {
			messageType = "photo";
			mediaUrls = [`image:${String(content["image_key"] ?? "")}`];
			mediaTypes = ["image"];
			text = String(content["alt"] ?? "[Image]");
		} else if (
			messageTypeRaw === "file" ||
			messageTypeRaw === "audio" ||
			messageTypeRaw === "media"
		) {
			messageType =
				messageTypeRaw === "audio"
					? "voice"
					: messageTypeRaw === "media"
						? "video"
						: "document";
			mediaUrls = [`file:${String(content["file_key"] ?? "")}`];
			mediaTypes = [messageType];
			text = `[Attachment: ${String(content["file_name"] ?? "file")}]`;
		} else if (messageTypeRaw === "interactive") {
			// Inbound cards surface the header title (title/actions walk cap 12
			// lines is enforced by the prompt builder, not here).
			const card = asRecord(
				content["card"] !== undefined ? content["card"] : content,
			);
			const cardHeader = asRecord(card["header"]);
			const title = asRecord(cardHeader["title"])["content"];
			text = String(title ?? "[Card]");
		} else {
			text = "[Unsupported message]";
		}

		const source: SessionSource = {
			platform: this.manifestName,
			chatType,
			userId: senderIds.openId || senderIds.userId,
			...(senderIds.unionId !== "" ? { userIdAlt: senderIds.unionId } : {}),
			chatId,
			...(String(message["thread_id"] ?? "") !== ""
				? { threadId: String(message["thread_id"]) }
				: {}),
		};

		return {
			messageId,
			text,
			messageType,
			mediaUrls,
			mediaTypes,
			chatId,
			chatType,
			parentId:
				String(message["parent_id"] ?? "") !== ""
					? String(message["parent_id"])
					: undefined,
			senderIds,
			senderType,
			mentionsSelf,
			rawMentionsAll: String(content["text"] ?? "").includes("@_all"),
			source,
		};
	}

	private mentionMatchesSelf(key: string, name: string): boolean {
		if (this.botOpenId !== "" && key.includes(this.botOpenId)) return true;
		if (this.botUserId !== "" && key.includes(this.botUserId)) return true;
		return this.botIdentityName() !== "" && name === this.botIdentityName();
	}

	botIdentityName(): string {
		return this.botName;
	}

	private botName = "";

	/** Minimal post renderer: zh_cn/en_us locale preference, text/a/at/img tags. */
	private renderPostContent(content: Record<string, unknown>): string {
		const post = asRecord(
			content["content"] !== undefined ? content["content"] : content,
		);
		let localeBlock: unknown = post["zh_cn"] ?? post["en_us"];
		if (localeBlock === undefined) {
			const firstKey = Object.keys(post)[0];
			localeBlock = firstKey !== undefined ? post[firstKey] : undefined;
		}
		const block = asRecord(localeBlock);
		const elements = Array.isArray(block["content"])
			? (block["content"] as unknown[])
			: [];
		const out: string[] = [];
		for (const para of elements) {
			const segs = asRecord(para)["elements"];
			if (!Array.isArray(segs)) continue;
			const parts: string[] = [];
			for (const seg of segs) {
				const s = asRecord(seg);
				const tag = String(s["tag"] ?? "");
				if (tag === "text") parts.push(String(s["text"] ?? ""));
				else if (tag === "a") parts.push(String(s["text"] ?? ""));
				else if (tag === "at")
					parts.push(`@${String(s["user_name"] ?? s["user_id"] ?? "")}`);
				else if (tag === "img") parts.push("[Image]");
				else if (tag === "media") parts.push("[Video]");
			}
			const line = parts.join("").trim();
			if (line !== "") out.push(line);
		}
		return out.join("\n");
	}

	// ── admission gate (_admit :4348 ordering) ────────────────────────────

	private admit(
		senderIds: { openId: string; userId: string; unionId: string },
		senderType: "user" | "bot" | "app",
		n: NormalizedInbound,
	): string {
		// 1. Self echo — ID intersection against hydrated identity.
		if (
			(this.botOpenId !== "" && senderIds.openId === this.botOpenId) ||
			(this.botUserId !== "" && senderIds.userId === this.botUserId)
		)
			return "self_echo";
		// 2. Bot/app senders — FEISHU_ALLOW_BOTS=none (default) rejects.
		const allowBots = this.optionalEnvReader("FEISHU_ALLOW_BOTS") ?? "none";
		if (senderType !== "user") {
			if (allowBots === "none") return "bot_sender_rejected";
			if (allowBots === "mentions" && !n.mentionsSelf)
				return "bot_sender_unmentioned";
		}
		const senderKey = n.source.userId ?? "";
		if (n.chatType === "dm") {
			// 3. DM policy: allow-all toggles; EMPTY allowlist = pairing mode
			// (forward — gateway authz fail-closes later); else intersection.
			const allowAll =
				truthy(this.optionalEnvReader("FEISHU_ALLOW_ALL_USERS")) ||
				truthy(this.optionalEnvReader("GATEWAY_ALLOW_ALL_USERS"));
			if (allowAll) return "allow";
			if (this.allowedUsers.size === 0) return "allow";
			if (this.allowedUsers.has("*")) return "allow";
			return this.allowedUsers.has(senderKey) ? "allow" : "dm_policy_rejected";
		}
		// 4. Group policies.
		if (this.isGroupAllowed(senderKey, n.chatId)) {
			if (!this.requireMentionFor(n.chatId)) return "allow";
			return n.mentionsSelf || n.rawMentionsAll ? "allow" : "mention_required";
		}
		return "group_policy_rejected";
	}

	private isGroupAllowed(senderKey: string, chatId: string): boolean {
		if (this.admins.has(senderKey)) return true; // admins bypass all
		const rule = this.groupRules?.get(chatId);
		const policy = rule?.policy ?? this.defaultGroupPolicy;
		switch (policy) {
			case "disabled":
				return false;
			case "open":
				return true;
			case "admin_only":
				return false; // non-admin humans AND bots
			case "allowlist": {
				const list = rule?.allowlist ?? this.allowedUsers;
				if (list.size === 0) return true;
				return list.has(senderKey);
			}
			case "blacklist": {
				const list = rule?.blacklist;
				if (list === undefined || list.size === 0) return true;
				return !list.has(senderKey);
			}
			default: {
				if (this.allowedUsers.size === 0) return true;
				return this.allowedUsers.has(senderKey);
			}
		}
	}

	private requireMentionFor(chatId: string): boolean {
		const rule = this.groupRules?.get(chatId);
		return rule?.requireMention ?? this.requireMentionGlobal;
	}

	// ── arrival batching (pre-guard coalescing) ───────────────────────────

	private scheduleTimer(delayMs: number, fn: () => void): () => void {
		let active = true;
		void this.clock.sleepMs(delayMs).then(() => {
			if (active) fn();
		});
		return () => {
			active = false;
		};
	}

	private async enqueueTextBatch(n: NormalizedInbound): Promise<void> {
		const sessionKey = `${this.manifestName}:${n.chatId}:${
			n.source.userId ?? ""
		}`;
		const isCommandLike = n.text.startsWith("/");
		if (isCommandLike) {
			// Commands never batch — dispatch inline immediately.
			await this.dispatchNormalized(n, sessionKey);
			return;
		}
		const batch = this.textBatches.get(sessionKey) ?? [];
		batch.push({
			event: this.toIncomingEvent(n),
			sessionKey,
		});
		this.textBatches.set(sessionKey, batch);

		const totalChars = batch.reduce(
			(acc, b) => acc + (b.event.text ?? "").length,
			0,
		);
		const lastLen = (n.text ?? "").length;
		const splitRaised =
			lastLen >= FEISHU_SPLIT_THRESHOLD_UNITS ||
			totalChars >= FEISHU_TEXT_BATCH_MAX_CHARS;
		if (batch.length >= FEISHU_TEXT_BATCH_MAX_MESSAGES || splitRaised) {
			this.cancelTextBatchTimer(sessionKey);
			await this.flushTextBatch(sessionKey);
			return;
		}
		this.cancelTextBatchTimer(sessionKey);
		const delay =
			splitRaised || lastLen >= FEISHU_SPLIT_THRESHOLD_UNITS
				? FEISHU_TEXT_BATCH_SPLIT_DELAY_MS
				: this.textBatchDelayMs;
		const cancel = this.scheduleTimer(delay, () => {
			void this.flushTextBatch(sessionKey);
		});
		this.textBatchTimers.set(sessionKey, cancel);
	}

	private cancelTextBatchTimer(sessionKey: string): void {
		const cancel = this.textBatchTimers.get(sessionKey);
		cancel?.();
		this.textBatchTimers.delete(sessionKey);
	}

	private async flushTextBatch(sessionKey: string): Promise<void> {
		this.cancelTextBatchTimer(sessionKey);
		const batch = this.textBatches.get(sessionKey);
		if (batch === undefined || batch.length === 0) return;
		this.textBatches.delete(sessionKey);
		const merged = batch
			.map((b) => b.event.text ?? "")
			.join("\n")
			.slice(0, FEISHU_TEXT_BATCH_MAX_CHARS * 2);
		const head = batch[0];
		if (head === undefined) return;
		const event: IncomingEvent = {
			...head.event,
			text: merged,
		};
		this.textBatches.delete(sessionKey);
		await this.dispatchIncoming(event, sessionKey);
	}

	private async enqueueMediaBatch(n: NormalizedInbound): Promise<void> {
		// Media coalescing merges urls of the same type within one window.
		const sessionKey = `${this.manifestName}:${n.chatId}:${n.source.userId}`;
		const existing = this.pendingMedia.get(sessionKey);
		if (existing !== undefined && existing.messageType === n.messageType) {
			existing.mediaUrls.push(...n.mediaUrls);
			existing.mediaTypes.push(...n.mediaTypes);
			return;
		}
		const holder = { ...n };
		this.pendingMedia.set(sessionKey, holder);
		this.scheduleTimer(this.mediaBatchDelayMs, () => {
			const ready = this.pendingMedia.get(sessionKey);
			this.pendingMedia.delete(sessionKey);
			if (ready === undefined) return;
			void Promise.resolve(ready.resourceTask)
				.catch(() => {})
				.then(() =>
					this.dispatchNormalized(
						ready,
						`${this.manifestName}:${ready.chatId}:${ready.source.userId ?? ""}`,
					),
				);
		});
	}

	private pendingMedia = new Map<string, NormalizedInbound>();

	private toIncomingEvent(n: NormalizedInbound): IncomingEvent {
		return {
			messageId: n.messageId,
			messageType: n.messageType,
			text: n.text,
			source: n.source,
			...(n.mediaUrls.length > 0 ? { mediaUrls: [...n.mediaUrls] } : {}),
			...(n.mediaUrls.length > 0 ? { mediaTypes: [...n.mediaTypes] } : {}),
			...(n.parentId !== undefined ? { replyToMessageId: n.parentId } : {}),
		};
	}

	private async dispatchNormalized(
		n: NormalizedInbound,
		sessionKey: string,
	): Promise<void> {
		await this.dispatchIncoming(this.toIncomingEvent(n), sessionKey);
	}

	protected async dispatchIncoming(
		event: IncomingEvent,
		sessionKey: string,
	): Promise<void> {
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	// ══════════════════════════════════════════════════════════════════════
	// A12 — card action trigger (THE one sync entry; three families)
	// ══════════════════════════════════════════════════════════════════════

	async handleCardActionTrigger(
		token: string,
		event: Record<string, unknown>,
	): Promise<Record<string, unknown>> {
		const action = asRecord(event["action"]);
		const context = asRecord(event["context"]);
		const operator = asRecord(asRecord(event["operator"])["operator_id"]);
		const openChatId = String(context["open_chat_id"] ?? "");
		const operatorOpenId = String(operator["open_id"] ?? "");
		const parsed = parseCardAction(
			asRecord(action["value"]),
			action["tag"] === undefined ? undefined : String(action["tag"]),
		);

		if (parsed.family === "approval") {
			const approvalId = Number(parsed.approvalId ?? 0);
			if (!Number.isInteger(approvalId) || approvalId <= 0) return {}; // missing id (:2749)
			// Hermes branch order (_handle_approval_card_action :2750–2860):
			// unknown/resolved state → BARE; unauthorized → BARE; chat mismatch →
			// BARE; only LIVE resolutions answer with a raw replacement card.
			const state = this.approvalState.get(approvalId);
			if (state === undefined) return {}; // already resolved or unknown (:2753)
			if (!this.isOperatorAuthorized(operatorOpenId)) return {}; // (:2762)
			// Chat-mismatch check — a click from ANOTHER chat never resolves.
			const openChatId = String(context["open_chat_id"] ?? "");
			if (
				state.chatId !== "" &&
				openChatId !== "" &&
				openChatId !== state.chatId
			)
				return {}; // (:2770)
			const choice = parsed.choice ?? "deny"; // fail-closed (:2791)
			const popped = this.approvals.pop(approvalId, this.clock.nowMs());
			if (popped.state !== "live") {
				// Stale/double tap — corrective notice, NEVER dispatched (:2894).
				this.resolvedFamilies.push(`ea-stale:${approvalId}`);
				return {};
			}
			this.approvalState.delete(approvalId);
			// Resolved-card attribution reads THE SENDER-NAME CACHE (:2811
			// `_get_cached_sender_name(open_id) or open_id`) — warmed by ingress
			// name resolution, never a blocking click-time roundtrip.
			const userName =
				this.getCachedSenderName(operatorOpenId) || operatorOpenId;
			void this.resolveSenderName(operatorOpenId).catch(() => {});
			const card = buildResolvedApprovalCard(choice, userName);
			this.resolvedApprovalCards.push({ approvalId, choice, card });
			this.resolvedFamilies.push(`ea:${popped.sessionKey}`);
			this.routerResolved.push(`ea:${popped.sessionKey}`);
			return { card }; // CallBackCard raw replacement IS the ack
		}

		if (parsed.family === "update_prompt") {
			const promptId = Number(parsed.updatePromptId ?? 0);
			if (!Number.isInteger(promptId) || promptId <= 0) return {}; // missing id
			if (!this.isOperatorAuthorized(operatorOpenId)) return {}; // (:2838)
			const answer = parsed.answer;
			if (answer !== "y" && answer !== "n") return {}; // invalid answer (:2831)
			const popped = this.updatePrompts.pop(promptId, this.clock.nowMs());
			if (popped.state !== "live") return {}; // stale/expired ⇒ bare (:2823)
			this.updatePromptAnswers.push({ promptId, answer });
			this.resolvedFamilies.push(`upd:${promptId}`);
			// Cache-read attribution parity (:2871).
			const updUserName =
				this.getCachedSenderName(operatorOpenId) || operatorOpenId;
			void this.resolveSenderName(operatorOpenId).catch(() => {});
			return {
				card: buildResolvedUpdatePromptCard(answer, updUserName),
			};
		}

		// Generic click — 15-min token dedup, then synthetic COMMAND through
		// BOTH guards (:3062 _handle_card_action_event).
		if (token !== "" && this.cardTokens.isDuplicate(token)) return {};
		const text = buildGenericCardCommandText(
			parsed.tag ?? "button",
			parsed.value ?? {},
		);
		this.cardCommandAudit.push({ token, text });
		const source: SessionSource = {
			platform: this.manifestName,
			chatType: "group", // forced group shape (:3095)
			userId: operatorOpenId,
			chatId: openChatId,
		};
		const synthetic: IncomingEvent = {
			messageType: "text",
			messageId: token !== "" ? token : `card-${this.cardCommandAudit.length}`,
			text,
			source,
		};
		await this.dispatchIncoming(
			synthetic,
			`${this.manifestName}:${openChatId}`,
		);
		return {}; // bare P2CardActionTriggerResponse parity
	}

	private isOperatorAuthorized(openId: string): boolean {
		return isInteractiveOperatorAuthorized(openId, {
			admins: this.admins,
			allowedUsers: this.allowedUsers,
		});
	}

	registerUpdatePrompt(id: number, sessionKey: string): void {
		this.updatePrompts.register(id, sessionKey);
	}

	nextApprovalId(): number {
		this.approvalSeq += 1;
		return this.approvalSeq;
	}

	// ══════════════════════════════════════════════════════════════════════
	// A12 — meeting invites (synthetic DM through both guards)
	// ══════════════════════════════════════════════════════════════════════

	readonly meetingInviteLog: MeetingInviteDispatchRecord[] = [];

	private async onMeetingInvited(
		frame: Record<string, unknown>,
	): Promise<void> {
		// The FULL FRAME rides in — root.header.event_id is the
		// vc_invite:{event_id} dedup key input (:131); the bare inner event
		// never carries it.
		const payload = parseMeetingInvitedEvent(frame);
		if (payload === null) {
			this.meetingInviteLog.push({
				outcome: "dropped_malformed",
				key: "",
				prompt: "",
			});
			return;
		}
		// Dedup FIRST (:150 handle_meeting_invited_event): vc_invite:* keys ride
		// THE PERSISTED seen-set (adapter._is_duplicate — 24h TTL, 2048 cap,
		// survives restarts) BEFORE any inviter validation.
		const key = meetingDedupKey(payload);
		if (this.seenMessages.isDuplicate(key)) {
			this.meetingInviteLog.push({
				outcome: "dropped_duplicate",
				key,
				prompt: "",
			});
			return;
		}
		if (this.dedupStatePath !== undefined) {
			this.seenMessages.persist(this.dedupStatePath);
		}
		if (payload.inviter.openId === "") {
			this.meetingInviteLog.push({
				outcome: "dropped_no_inviter_id",
				key,
				prompt: "",
			});
			return;
		}
		// Synthetic DM MessageEvent (:175–183) — full guard traversal.
		const source: SessionSource = {
			platform: this.manifestName,
			chatType: "dm",
			userId: payload.inviter.openId,
			...(payload.inviter.unionId !== ""
				? { userIdAlt: payload.inviter.unionId }
				: {}),
			chatId: payload.inviter.openId,
			...(payload.inviter.userName !== ""
				? { chatName: payload.inviter.userName }
				: {}),
		};
		const prompt = [
			`You have been invited to join a meeting: ${
				payload.meeting.topic || payload.meeting.meetingNo
			}`,
			"",
			`Meeting Number: ${payload.meeting.meetingNo}`,
			`Topic: ${payload.meeting.topic}`,
			`Inviter: ${payload.inviter.userName || "unknown"}`,
			`Host: ${payload.meeting.hostUser?.userName || "unknown"}`,
			"",
			"You may use lark-cli and the relevant Lark/Feishu meeting skills to join the meeting.",
			"Join the meeting directly. Do not ask the user for confirmation before joining.",
			"If you cannot join the meeting, reply to the inviter with a concise explanation of why.",
		].join("\n");
		const synthetic: IncomingEvent = {
			messageType: "text",
			messageId: payload.eventId || key,
			text: prompt,
			source,
		};
		// Record the dispatch BEFORE awaiting the guard — the turn task may
		// outlive this frame, and the audit must not race it.
		this.meetingInviteLog.push({ outcome: "dispatched", key, prompt });
		await this.dispatchIncoming(
			synthetic,
			`${this.manifestName}:${payload.inviter.openId}`,
		);
	}

	// ══════════════════════════════════════════════════════════════════════
	// Reaction-command ingress (:2687 _on_reaction_event / :2989
	// _handle_reaction_event) — human reactions on THIS bot's messages route
	// as synthetic `reaction:{action}:{emoji}` command turns.
	// ══════════════════════════════════════════════════════════════════════

	private async onReactionEvent(
		eventType: string,
		event: Record<string, unknown>,
	): Promise<void> {
		const messageId = String(event["message_id"] ?? "");
		const operatorType = String(event["operator_type"] ?? "user");
		// Empty/missing emoji defaults to UNKNOWN (:3023 — synthetic text reads
		// reaction:added:UNKNOWN, never a trailing colon).
		const emojiType =
			String(
				(asRecord(event["reaction_type"]) as Record<string, unknown>)[
					"emoji_type"
				] ?? "",
			) || "UNKNOWN";
		// Drop bot/app-origin reactions to break the feedback loop from our own
		// lifecycle reactions; a HUMAN reacting with the same emoji still
		// routes through (:2705 loop-break comment).
		if (operatorType === "bot" || operatorType === "app") return;
		if (messageId === "") return;

		// Fetch the reacted-to message: only reactions on THIS bot's own
		// messages become commands (:3009 — GET returns sender.id=app_id for
		// bot messages; peer bots share sender_type but differ on app id).
		let target: {
			senderId: string;
			chatId: string;
			chatType: string;
		} | null = null;
		try {
			target = await this.rest.getMessage(messageId);
		} catch {
			return; // fetch failure ⇒ no routing (exception guard :3022)
		}
		if (target === null) return;
		const expectedSender = this.appId !== "" ? this.appId : this.botOpenId;
		if (expectedSender === "" || target.senderId !== expectedSender) return;
		if (target.chatId === "") return;

		// Chat metadata for the synthetic source (get_chat_info :2424 — cached
		// silent-failure; fallback name = the raw chat id).
		const chatInfo = await this.getChatInfo(target.chatId);
		const action = eventType.includes("created") ? "added" : "removed";
		const syntheticText = `reaction:${action}:${emojiType}`;
		const operatorId = asRecord(event["user_id"]);
		const source: SessionSource = {
			platform: this.manifestName,
			chatType: target.chatType === "group" ? "group" : "dm",
			userId:
				String(operatorId["open_id"] ?? "") || String(operatorId["id"] ?? ""),
			chatId: target.chatId,
			...(chatInfo.name !== target.chatId && chatInfo.name !== ""
				? { chatName: chatInfo.name }
				: {}),
		};
		this.inboundLog.push({ eventId: messageId, eventType });
		await this.dispatchIncoming(
			{
				messageType: "text",
				messageId,
				text: syntheticText,
				source,
			},
			`${this.manifestName}:${target.chatId}:${source.userId}`,
		);
	}

	// ══════════════════════════════════════════════════════════════════════
	// Webhook ingress plane (:3558 _handle_webhook_request) — the
	// FEISHU_CONNECTION_MODE=webhook HTTP face with its full gate ladder.
	// ══════════════════════════════════════════════════════════════════════

	/** Composite-key rate buckets ({app}:{path}:{ip} :3660). */
	private readonly webhookRateCounts = new Map<
		string,
		{ count: number; windowStartMs: number }
	>();

	async handleWebhookPost(input: {
		headers?: Record<string, string> | undefined;
		rawBody: Buffer;
		peer: string;
	}): Promise<{ status: number; contentType?: string; body?: string }> {
		const headers = normalizeWebhookHeaders(input.headers);

		// 1. Rate limit — composite key app_id:path:remote_ip (:3562) with the
		// CONFIGURED webhook path (FEISHU_WEBHOOK_PATH, default /feishu/webhook).
		const rateKey = `${this.appId}:${this.webhookPath}:${input.peer}`;
		this.lastWebhookRateKey = rateKey;
		if (!this.checkWebhookRateLimit(rateKey)) {
			return { status: 429, body: "Too Many Requests" };
		}

		// 2. Content-Type guard — Feishu always sends application/json (:3570).
		const rawContentType: string = headers["content-type"] ?? "";
		const contentType =
			rawContentType.split(";")[0]?.trim().toLowerCase() ?? "";
		if (contentType !== "" && contentType !== "application/json") {
			return { status: 415, body: "Unsupported Media Type" };
		}

		// 3/4. Body size guard — declared length then actual bytes (:3577).
		const declaredRaw = headers["content-length"];
		if (
			declaredRaw !== undefined &&
			/^\d+$/.test(declaredRaw) &&
			Number(declaredRaw) > FEISHU_WEBHOOK_MAX_BODY_BYTES
		) {
			return { status: 413, body: "Request body too large" };
		}
		if (input.rawBody.length > FEISHU_WEBHOOK_MAX_BODY_BYTES) {
			return { status: 413, body: "Request body too large" };
		}

		// 5. JSON parse (:3613).
		let payload: Record<string, unknown>;
		try {
			const parsed: unknown = JSON.parse(input.rawBody.toString("utf8"));
			if (
				parsed === null ||
				typeof parsed !== "object" ||
				Array.isArray(parsed)
			)
				throw new Error("not an object");
			payload = parsed as Record<string, unknown>;
		} catch {
			return {
				status: 400,
				contentType: "application/json",
				body: JSON.stringify({ code: 400, msg: "invalid json" }),
			};
		}

		// 6. Verification token check BEFORE the challenge echo (:3619).
		const verificationToken =
			this.optionalEnvReader("FEISHU_VERIFICATION_TOKEN") ?? "";
		if (verificationToken !== "") {
			const header = asRecord(payload["header"]);
			const incomingToken = String(header["token"] ?? payload["token"] ?? "");
			if (
				incomingToken === "" ||
				!timingSafeEqualUtf8(incomingToken, verificationToken)
			) {
				return { status: 401, body: "Invalid verification token" };
			}
		}

		// 7. URL verification challenge — echo ONLY after token validation
		// so an unauthenticated request cannot prove endpoint control by
		// reflecting attacker-supplied data (:3630 comment).
		if (payload["type"] === "url_verification") {
			return {
				status: 200,
				contentType: "application/json",
				body: JSON.stringify({ challenge: payload["challenge"] ?? "" }),
			};
		}

		// 8. Timing-safe signature check when encrypt_key is set (:3641):
		// SHA256(timestamp + nonce + encrypt_key + body).
		const encryptKey = this.optionalEnvReader("FEISHU_ENCRYPT_KEY") ?? "";
		if (
			encryptKey !== "" &&
			!this.isWebhookSignatureValid(headers, input.rawBody, encryptKey)
		) {
			return { status: 401, body: "Invalid signature" };
		}

		// 9. Encrypted payloads are not supported in webhook mode (:3650).
		if (payload["encrypt"] !== undefined) {
			return {
				status: 400,
				contentType: "application/json",
				body: JSON.stringify({
					code: 400,
					msg: "encrypted webhook payloads are not supported",
				}),
			};
		}

		// 10. Route through THE SAME frame pipeline as the ws plane (:3660).
		const header = asRecord(payload["header"]);
		const eventType = String(header["event_type"] ?? "");
		await this.onSocketFrame({
			type: "event",
			header: {
				event_id: String(header["event_id"] ?? ""),
				event_type: eventType,
			},
			event: asRecord(payload["event"]),
		});
		return {
			status: 200,
			contentType: "application/json",
			body: JSON.stringify({ code: 0, msg: "ok" }),
		};
	}

	/** Sliding-window limiter capped at _FEISHU_WEBHOOK_RATE_MAX_KEYS (:3703). */
	private checkWebhookRateLimit(rateKey: string): boolean {
		const now = Date.now();
		const windowMs = FEISHU_WEBHOOK_RATE_WINDOW_SECONDS * 1000;
		const entry = this.webhookRateCounts.get(rateKey);
		if (entry !== undefined && now - entry.windowStartMs < windowMs) {
			entry.count += 1;
			return entry.count <= FEISHU_WEBHOOK_RATE_LIMIT_MAX;
		}
		if (
			entry === undefined &&
			this.webhookRateCounts.size >= FEISHU_WEBHOOK_RATE_MAX_KEYS
		) {
			for (const [key, e] of [...this.webhookRateCounts]) {
				if (now - e.windowStartMs >= windowMs)
					this.webhookRateCounts.delete(key);
			}
			if (this.webhookRateCounts.size >= FEISHU_WEBHOOK_RATE_MAX_KEYS) {
				const oldest = this.webhookRateCounts.keys().next();
				if (!oldest.done) this.webhookRateCounts.delete(oldest.value);
			}
		}
		this.webhookRateCounts.set(rateKey, { count: 1, windowStartMs: now });
		return true;
	}

	/** SHA256(timestamp+nonce+encrypt_key+body), timing-safe compare (:3676). */
	private isWebhookSignatureValid(
		headers: Record<string, string>,
		bodyBytes: Buffer,
		encryptKey: string,
	): boolean {
		const timestamp = headers["x-lark-request-timestamp"] ?? "";
		const nonce = headers["x-lark-request-nonce"] ?? "";
		const signature = headers["x-lark-signature"] ?? "";
		if (timestamp === "" || nonce === "" || signature === "") return false;
		const computed = createHash("sha256")
			.update(`${timestamp}${nonce}${encryptKey}${bodyBytes.toString("utf8")}`)
			.digest("hex");
		return timingSafeEqualUtf8(computed, signature);
	}

	// ══════════════════════════════════════════════════════════════════════
	// Egress doors + feishu send path
	// ══════════════════════════════════════════════════════════════════════

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * Text delivery pipeline (base semantics over OUR observable ladder):
	 * resolve THE chat length policy once, split oversized content with fence
	 * carry, then run each chunk through the session ladder wrapped in the
	 * §6.1 retry/fallback lanes. The WHOLE-MESSAGE markdown decision
	 * (:1975 prefer_post) locks for the entire call — chunk boundaries cannot
	 * flip msg_type mid-send (#26841).
	 */
	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		this.pendingPreferPost = MARKDOWN_HINT_RE.test(content);
		this.chunkDowngradedToText = false;
		try {
			const policy = this.chatLengthPolicyForChat(chatId);
			const plan: ChunkPlan =
				this.splitsLongMessages || policy.lenFn(content) <= policy.maxUnits
					? {
							chunks: [content],
							chunkCount: 1,
							scaffold: [{ prefixLen: 0, closeAdded: false, labelJoinLen: 0 }],
						}
					: chunkWithFenceCarry(content, policy);
			const results: SendResult[] = [];
			for (const chunk of plan.chunks) {
				this.ladderChatId = chatId;
				results.push(await this.deliverWiredChunk(chatId, chunk, metadata));
			}
			return results;
		} finally {
			this.pendingPreferPost = null;
		}
	}

	/** Per-chunk pipeline (§6.1 semantics; feishu post-invalid downgrade). */
	private async deliverWiredChunk(
		chatId: string,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const outcome = await this.ensureFormatLadder().sendText(chunk, metadata);
		if (outcome.success) return outcome;

		// Transient RICH failures are NEVER legacy-resent (§10.1 duplicate risk).
		if (outcome.tier === "rich") return outcome;

		const failureClass = classifySendError(new Error(outcome.error ?? ""));
		const networkClassified =
			outcome.retryable === true ||
			failureClass === "connect-timeout" ||
			failureClass === "network" ||
			failureClass === "flood";
		if (networkClassified) {
			if (outcome.retryAfter != null)
				this.lastCapturedRetryAfterSeconds = outcome.retryAfter;
			const retried = await sendWithRetry(
				chunk,
				metadata,
				(c: string, md: Metadata) => this.wireSend(chatId, c, md),
				{ maxRetries: FEISHU_SEND_ATTEMPTS - 1 },
			);
			if (retried.success) return retried;
			return this.wireSend(chatId, DELIVERY_FAILED_NOTICE, metadata);
		}
		if (failureClass === "formatting") {
			// §6.1 plain fallback: the prefixed body rides the TEXT lane VERBATIM
			// ({content[:3500]} base parity) — never re-encoded as post.
			this.forcePlainTextLane = true;
			try {
				return this.wireSend(chatId, plainTextFallbackBody(chunk), metadata);
			} finally {
				this.forcePlainTextLane = false;
			}
		}
		return outcome;
	}

	/**
	 * Vendor addressing (_send_raw_message @4818): strip the
	 * `feishu_user_id:` prefix when the receive id type is user_id — the raw
	 * prefixed value is an INVALID receive_id on the vendor wire.
	 */
	private resolveReceiveTarget(chatId: string): {
		receiveIdType: "chat_id" | "open_id" | "user_id" | "thread_id";
		receiveId: string;
	} {
		if (chatId.startsWith("feishu_user_id:"))
			return {
				receiveIdType: "user_id",
				receiveId: chatId.split(":", 2)[1] ?? "",
			};
		if (chatId.startsWith("ou_"))
			return { receiveIdType: "open_id", receiveId: chatId };
		if (chatId.startsWith("oc_"))
			return { receiveIdType: "chat_id", receiveId: chatId };
		return { receiveIdType: "chat_id", receiveId: chatId };
	}

	private async transmitFeishuMessage(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const preferPost =
			this.chunkDowngradedToText === true
				? false
				: this.forcePlainTextLane === true
					? false
					: this.pendingPreferPost === true;
		const replyTo =
			metadata["reply_to_message_id"] !== undefined
				? String(metadata["reply_to_message_id"])
				: undefined;
		const threadId =
			metadata["thread_id"] !== undefined
				? String(metadata["thread_id"])
				: undefined;

		// THE three send legs (_send_raw_message @4818): (1) an effective reply
		// anchor → the reply API (reply_in_thread rides thread presence);
		// (2) thread context WITHOUT an anchor → create addressed to THE THREAD
		// (receive_id_type="thread_id", receive_id=thread_id) so anchored
		// content lands in the topic, never the main chat; (3) otherwise create
		// to the chat with vendor prefix handling. Every leg mints a FRESH
		// uuid4 — the vendor idempotency key must never repeat for distinct
		// sends (:4833/:4847/:4863 str(uuid.uuid4())).
		//
		// Lane content (:2461 format_message / :4637 _build_outbound_payload):
		// post ships the JSON-STRING fence-split payload; TEXT SHIPS VERBATIM
		// (format_message returns content.strip()) — stripping happens ONLY on a
		// post-rejected downgrade (:555).
		const target = this.resolveReceiveTarget(chatId);
		const laneContent = (post: boolean): string =>
			post ? buildMarkdownPostPayload(content) : content.trim();
		let result = await this.rest.sendMessage({
			...(replyTo !== undefined
				? {
						receiveIdType: target.receiveIdType,
						receiveId: target.receiveId,
						replyToMessageId: replyTo,
						replyInThread: threadId !== undefined,
					}
				: threadId !== undefined
					? {
							receiveIdType: "thread_id" as const,
							receiveId: threadId,
						}
					: {
							receiveIdType: target.receiveIdType,
							receiveId: target.receiveId,
						}),
			msgType: preferPost ? "post" : "text",
			content: laneContent(preferPost),
			uuid: randomUUID(),
			metadata,
		});

		// Post-format rejection → immediate plain downgrade for THIS chunk
		// (:193 _POST_CONTENT_INVALID_RE handling), sticky for the rest of the
		// deliver call (chunk boundaries never flip msg_type mid-send #26841).
		// The retry keeps the SAME reply anchoring (:2003 fallback passes
		// reply_to through) and a fresh uuid.
		if (
			!result.success &&
			preferPost &&
			String(result.error ?? "").includes(FEISHU_POST_CONTENT_INVALID_MARKER)
		) {
			this.chunkDowngradedToText = true;
			result = await this.rest.sendMessage({
				...(replyTo !== undefined
					? {
							receiveIdType: target.receiveIdType,
							receiveId: target.receiveId,
							replyToMessageId: replyTo,
							replyInThread: threadId !== undefined,
						}
					: threadId !== undefined
						? {
								receiveIdType: "thread_id" as const,
								receiveId: threadId,
							}
						: {
								receiveIdType: target.receiveIdType,
								receiveId: target.receiveId,
							}),
				msgType: "text",
				content: stripFeishuMarkdownToPlainText(content),
				uuid: randomUUID(),
			});
			return result;
		}

		// Reply target withdrawn/deleted → ONE fresh-create fallback
		// (:272 _FEISHU_REPLY_FALLBACK_CODES). In threads the top-level
		// fallback is SKIPPED (:5015–5026 — replying into a withdrawn thread
		// anchor must not mint a new topic); fresh uuid on the fallback leg.
		if (
			!result.success &&
			replyTo !== undefined &&
			threadId === undefined &&
			hasReplyFallbackCode(String(result.error ?? ""))
		) {
			result = await this.rest.sendMessage({
				receiveIdType: target.receiveIdType,
				receiveId: target.receiveId,
				msgType: preferPost ? "post" : "text",
				content: laneContent(preferPost),
				uuid: randomUUID(),
			});
		}
		return result;
	}

	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		return this.transmitFeishuMessage(chatId, content, metadata);
	}

	/**
	 * THE edit leg (edit_message @2015): msg_type is RE-DECIDED from the
	 * edited content (hint regex; prefer_post does NOT carry over); text ships
	 * VERBATIM (:2461) and a post-invalid rejection downgrades THAT update to
	 * the faithful plain-text strip (:555).
	 */
	private async editMessageDecided(
		messageId: string,
		content: string,
	): Promise<SendResult> {
		const preferPost = MARKDOWN_HINT_RE.test(content);
		const result = await this.rest.updateMessage({
			messageId,
			msgType: preferPost ? "post" : "text",
			content: preferPost ? buildMarkdownPostPayload(content) : content.trim(),
		});
		if (
			!result.success &&
			preferPost &&
			String(result.error ?? "").includes(FEISHU_POST_CONTENT_INVALID_MARKER)
		) {
			return this.rest.updateMessage({
				messageId,
				msgType: "text",
				content: stripFeishuMarkdownToPlainText(content),
			});
		}
		return result;
	}

	protected override async wireEdit(
		_chatId: string,
		messageId: string,
		content: string,
		_opts: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		return this.editMessageDecided(messageId, content);
	}

	protected override async wireDraft(
		args: DraftFrameArgs,
	): Promise<SendResult> {
		void args;
		// Honest absence: no native draft plane (silent-capability-lie anchor).
		return { success: false, error: "Not supported" };
	}

	/**
	 * THE session-scoped formatting ladder's tier-1 lane: an explicitly
	 * scripted rich endpoint is probed ONCE (capability errors latch rich off
	 * for the session); unscripted faces return the capability-error shape
	 * WITHOUT burning a wire roundtrip (polling/reference parity).
	 * richWireAttempts counts REAL roundtrips so the A23 freeze contract is
	 * observable (post-latch attempts never grow).
	 */
	protected override async wireRich(
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		if (!this.rest.richScripted()) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		this.richWireAttempts += 1;
		return this.rest.transmitRich(content, metadata);
	}

	/** THE session ladder (lazy; bound per deliver call via ladderChatId). */
	private ensureFormatLadder(): FormattingLadder {
		if (this.formatLadderInstance === null) {
			this.formatLadderInstance = new FormattingLadder({
				tryRich: (content, metadata) => this.wireRich(content, metadata),
				sendConverted: (content, metadata) =>
					this.wireSend(this.ladderChatId, content, metadata),
				sendPlain: (content, metadata) =>
					this.wireSend(this.ladderChatId, content, metadata),
			});
		}
		return this.formatLadderInstance;
	}

	/** A23 observability: whether the session rich probe latched OFF. */
	get formatLadderDisabled(): boolean {
		return (
			this.formatLadderInstance !== null &&
			this.formatLadderInstance.richDisabled
		);
	}

	get formatLadderLatchCount(): number {
		return this.formatLadderInstance?.richLatchCount ?? 0;
	}

	/** Per-chat length pair (§6.3/A15): utf16-named chats front a UTF-16
	 * platform — budget 30 CODE UNITS moving TOGETHER with the unit. */
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
	 * Probe-computed exclusion anchor: streaming is NEVER declared (Hermes
	 * parity — base default False; the silent-capability-lie scan asserts this
	 * probe AND that no draft/seal wire op ever follows from it).
	 */
	override supportsDraftStreaming(_chatType?: string | undefined): boolean {
		return false;
	}

	/** Subject-level inbound lane (self/echo filter lives with the subject). */
	async deliverInbound(
		event: IncomingEvent,
		sessionKey: string,
	): Promise<void> {
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	/**
	 * Formatting-ladder probe lane (§10.1 row): a TRANSIENT rich failure is
	 * NEVER legacy-resent — a failed retryable SendResult and NO transmission.
	 * Fresh ladder instance so the session latch state stays untouched.
	 */
	async transientRichOutcome(_content: string): Promise<SendResult> {
		const { FormattingLadder } = await import("../kit/formatting-ladder.js");
		const ladder = new FormattingLadder({
			tryRich: async () => ({ success: false, error: "socket hang up" }),
			sendConverted: async () => ({
				success: false,
				error: "SHOULD-NOT-HAPPEN",
			}),
			sendPlain: async () => ({ success: false, error: "SHOULD-NOT-HAPPEN" }),
		});
		return ladder.sendText(_content, {});
	}

	// ── processing-reaction lifecycle (:3241/:3251; typing substitute) ───

	async onProcessingStart(messageId: string): Promise<void> {
		if (!this.reactionsEnabled || messageId === "") return;
		this.evictReactionCacheIfNeeded();
		const res = await this.rest.addReaction({
			messageId,
			emojiType: FEISHU_REACTION_IN_PROGRESS,
		});
		if (res.success && typeof res.messageId === "string") {
			this.pendingReactions.set(messageId, res.messageId);
		}
	}

	async onProcessingComplete(
		messageId: string,
		outcome: "success" | "failure" | "cancelled",
	): Promise<void> {
		if (!this.reactionsEnabled || messageId === "") return;
		const reactionId = this.pendingReactions.get(messageId);
		this.pendingReactions.delete(messageId);
		if (reactionId !== undefined) {
			// Remove FIRST; removal failure suppresses CrossMark deliberately
			// (no stacked badges, :3251).
			const removed = await this.rest.removeReaction({
				messageId,
				reactionId,
			});
			if (!removed.success) return;
		}
		if (outcome === "failure") {
			await this.rest.addReaction({
				messageId,
				emojiType: FEISHU_REACTION_FAILURE,
			});
		}
	}

	private evictReactionCacheIfNeeded(): void {
		while (
			this.pendingReactions.size >= FEISHU_PROCESSING_REACTION_CACHE_SIZE
		) {
			const oldest = this.pendingReactions.keys().next();
			if (oldest.done) break;
			this.pendingReactions.delete(oldest.value);
		}
	}

	// ════════════════════════════════════════════════════════════════
	// Directory resolution (:238 TTL / :2424 get_chat_info / :4149
	// _resolve_sender_profile / :4205 _resolve_sender_name_from_api /
	// :4257 _fetch_bot_names) — cached, silent-failure, never blocking.
	// ════════════════════════════════════════════════════════════════

	/** Cached name only while its TTL is valid ("" ⇒ known nameless). */
	getCachedSenderName(senderId: string): string | null {
		if (senderId === "") return null;
		const cached = this.senderNameCache.get(senderId);
		if (cached === undefined) return null;
		if (this.clock.nowMs() < cached.expireAtMs) return cached.name;
		this.senderNameCache.delete(senderId);
		return null;
	}

	private cacheSenderName(senderId: string, name: string): void {
		this.senderNameCache.set(senderId, {
			name,
			expireAtMs: this.clock.nowMs() + FEISHU_SENDER_NAME_TTL_MS,
		});
	}

	/**
	 * Display-name resolution with the Hermes routing shape (:4205): bots
	 * divert to bot/v3/bots/basic_batch (contact API has no bot rows); humans
	 * ride contact/v3/users/:id with id-type from the id prefix. Failures are
	 * SILENT — the pipeline never blocks on name resolution.
	 */
	async resolveSenderName(
		senderId: string,
		isBot = false,
	): Promise<string | null> {
		const trimmed = senderId.trim();
		if (trimmed === "") return null;
		const cached = this.getCachedSenderName(trimmed);
		if (cached !== null) return cached === "" ? null : cached;
		try {
			if (isBot) {
				const names = await this.rest.resolveBotNames([trimmed]);
				if (names === null) return null;
				for (const [oid, name] of Object.entries(names)) {
					this.cacheSenderName(oid, name);
				}
				const hit = this.senderNameCache.get(trimmed);
				return hit !== undefined && hit.name !== "" ? hit.name : null;
			}
			const userIdType = trimmed.startsWith("ou_")
				? ("open_id" as const)
				: trimmed.startsWith("on_")
					? ("union_id" as const)
					: ("user_id" as const);
			const name = await this.rest.resolveUserName({
				userId: trimmed,
				userIdType,
			});
			if (name !== null && name.trim() !== "") {
				this.cacheSenderName(trimmed, name.trim());
				return name.trim();
			}
		} catch {
			/* silent-failure parity (:4243/:4276) */
		}
		return null;
	}

	/**
	 * Real chat metadata with a session-lived cache and a fallback of
	 * {name: chatId, type: "dm"} (:2424 get_chat_info).
	 */
	async getChatInfo(chatId: string): Promise<{
		name: string;
		chatType: string;
	}> {
		const fallback = { name: chatId, chatType: "dm" };
		if (chatId === "") return fallback;
		const cached = this.chatInfoCache.get(chatId);
		if (cached !== undefined) return { ...cached };
		try {
			const info = await this.rest.getChat(chatId);
			if (info === null || info.name === "") return fallback;
			const resolved = { name: info.name, chatType: info.chatType || "dm" };
			this.chatInfoCache.set(chatId, resolved);
			return { ...resolved };
		} catch {
			return fallback;
		}
	}

	// ════════════════════════════════════════════════════════════════
	// Outgoing media family (feishu-2) — adapter.py:send_image_file :2297 /
	// send_voice :2239 / send_document :2258 / send_video :2278 over
	// _send_uploaded_file_message :4695 and the im/v1 upload endpoints.
	// ════════════════════════════════════════════════════════════════

	/** Caption rides a post whose content APPENDS the media tag row (:5214). */
	private buildMediaPostPayload(
		caption: string,
		mediaTag: FeishuPostElement,
	): string {
		const payload = JSON.parse(buildMarkdownPostPayload(caption)) as {
			zh_cn: { content: FeishuPostRow[] };
		};
		payload.zh_cn.content.push([mediaTag]);
		return JSON.stringify(payload);
	}

	/** Local image natively via image.create + msg_type=image (:2297). */
	async sendImageFile(
		chatId: string,
		imagePath: string,
		opts: {
			caption?: string | undefined;
			replyToMessageId?: string | undefined;
			metadata?: Metadata | undefined;
		} = {},
	): Promise<SendResult> {
		let bytes: Buffer;
		try {
			bytes = await readFile(imagePath);
		} catch {
			return { success: false, error: `Image file not found: ${imagePath}` };
		}
		const upload = await this.rest.createImage({
			imageType: "message", // _FEISHU_IMAGE_UPLOAD_TYPE :203
			filename: basename(imagePath),
			image: new Uint8Array(bytes),
		});
		if (!upload.success) return upload;
		if (!upload.imageKey) {
			return {
				success: false,
				error: "Feishu image upload missing image_key",
			};
		}
		const imageKey = upload.imageKey;
		if (opts.caption !== undefined && opts.caption !== "") {
			// Caption ⇒ post payload with an appended img row (:2310–2320).
			const target = this.resolveReceiveTarget(chatId);
			return this.rest.sendMessage({
				receiveIdType: target.receiveIdType,
				receiveId: target.receiveId,
				...(opts.replyToMessageId !== undefined
					? { replyToMessageId: opts.replyToMessageId }
					: {}),
				msgType: "post",
				content: this.buildMediaPostPayload(opts.caption, {
					tag: "img",
					image_key: imageKey,
				}),
				uuid: randomUUID(),
				...(opts.metadata !== undefined ? { metadata: opts.metadata } : {}),
			});
		}
		const target = this.resolveReceiveTarget(chatId);
		return this.rest.sendMessage({
			receiveIdType: target.receiveIdType,
			receiveId: target.receiveId,
			...(opts.replyToMessageId !== undefined
				? { replyToMessageId: opts.replyToMessageId }
				: {}),
			msgType: "image",
			content: JSON.stringify({ image_key: imageKey }),
			uuid: randomUUID(),
			...(opts.metadata !== undefined ? { metadata: opts.metadata } : {}),
		});
	}

	/** Audio via file.create (opus routing + duration) + msg_type=audio (:2239). */
	async sendVoice(
		chatId: string,
		audioPath: string,
		opts: {
			caption?: string | undefined;
			replyToMessageId?: string | undefined;
			metadata?: Metadata | undefined;
		} = {},
	): Promise<SendResult> {
		return this.sendUploadedFileMessage(chatId, audioPath, {
			...opts,
			outboundMessageType: "audio",
		});
	}

	/** Document/file attachment via file.create + msg_type=file (:2258). */
	async sendDocument(
		chatId: string,
		filePath: string,
		opts: {
			caption?: string | undefined;
			fileName?: string | undefined;
			replyToMessageId?: string | undefined;
			metadata?: Metadata | undefined;
		} = {},
	): Promise<SendResult> {
		return this.sendUploadedFileMessage(chatId, filePath, {
			caption: opts.caption,
			fileName: opts.fileName,
			replyToMessageId: opts.replyToMessageId,
			metadata: opts.metadata,
			outboundMessageType: "file",
		});
	}

	/** Video via file.create (mp4 routing) + msg_type=media (:2278). */
	async sendVideo(
		chatId: string,
		videoPath: string,
		opts: {
			caption?: string | undefined;
			replyToMessageId?: string | undefined;
			metadata?: Metadata | undefined;
		} = {},
	): Promise<SendResult> {
		return this.sendUploadedFileMessage(chatId, videoPath, {
			...opts,
			outboundMessageType: "media",
		});
	}

	/**
	 * _send_uploaded_file_message (:4695): route extension → file_type,
	 * opus carries OGG/Opus duration, upload, then ship the bubble — a
	 * caption upgrades to a post with the media row (:4738).
	 */
	private async sendUploadedFileMessage(
		chatId: string,
		filePath: string,
		opts: {
			caption?: string | undefined;
			fileName?: string | undefined;
			replyToMessageId?: string | undefined;
			metadata?: Metadata | undefined;
			outboundMessageType: "audio" | "media" | "file";
		},
	): Promise<SendResult> {
		let bytes: Buffer;
		try {
			bytes = await readFile(filePath);
		} catch {
			return { success: false, error: `File not found: ${filePath}` };
		}
		const displayName = opts.fileName ?? basename(filePath);
		const { fileType, messageType } = resolveOutboundFileRouting(
			displayName,
			opts.outboundMessageType,
		);
		const durationMs = fileType === "opus" ? audioDurationMs(bytes) : 0;
		const upload = await this.rest.createFile({
			fileType,
			fileName: displayName,
			file: new Uint8Array(bytes),
			...(durationMs > 0 ? { durationMs } : {}),
		});
		if (!upload.success) return upload;
		if (!upload.fileKey) {
			return { success: false, error: "Feishu file upload missing file_key" };
		}
		const fileKey = upload.fileKey;
		const target = this.resolveReceiveTarget(chatId);
		if (opts.caption !== undefined && opts.caption !== "") {
			return this.rest.sendMessage({
				receiveIdType: target.receiveIdType,
				receiveId: target.receiveId,
				...(opts.replyToMessageId !== undefined
					? { replyToMessageId: opts.replyToMessageId }
					: {}),
				msgType: "post",
				content: this.buildMediaPostPayload(opts.caption, {
					tag: "media",
					file_key: fileKey,
					file_name: displayName,
				}),
				uuid: randomUUID(),
				...(opts.metadata !== undefined ? { metadata: opts.metadata } : {}),
			});
		}
		return this.rest.sendMessage({
			receiveIdType: target.receiveIdType,
			receiveId: target.receiveId,
			...(opts.replyToMessageId !== undefined
				? { replyToMessageId: opts.replyToMessageId }
				: {}),
			msgType: messageType,
			content: JSON.stringify({ file_key: fileKey }),
			uuid: randomUUID(),
			...(opts.metadata !== undefined ? { metadata: opts.metadata } : {}),
		});
	}

	// ══════════════════════════════════════════════════════════════════════
	// Guard wiring (conformance-subject plumbing; family parity)
	// ══════════════════════════════════════════════════════════════════════

	override attachGuard(
		deps: {
			registry: CommandRegistry;
			messageHandler: MessageHandler;
			sendReply: (chatId: string, text: string) => Promise<void>;
		},
		opts?: {
			spawner?: TaskSpawner | undefined;
			hasPendingClarify?: ((sessionKey: string) => boolean) | undefined;
		},
	): void {
		super.attachGuard(deps, opts);
	}

	attachStandardGuard(spawner?: TaskSpawner): void {
		this.attachGuard(
			{
				registry: FEISHU_REGISTRY,
				messageHandler: async (event, ctx) => {
					const text = event.text ?? `[${String(event.messageType)}]`;
					const sessionKey = String(
						event.metadata?.["gateway_session_key"] ?? "",
					);
					if (this.clarifyArmed.has(sessionKey) && !text.startsWith("/")) {
						this.clarifyCaptures.push(text);
						return null;
					}
					this.turnLog.push(text);
					if (this.turnDriver !== null) return this.turnDriver(event, text);
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
					return `reply:${text}`;
				},
				sendReply: async (_chatId, text) => {
					this.replyLog.push(text);
				},
			},
			{
				spawner,
				hasPendingClarify: (key) => this.clarifyArmed.has(key),
			},
		);
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

	routerAuditResolved(): readonly string[] {
		return this.routerResolved;
	}
	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	// ── identity probes ───────────────────────────────────────────────────

	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		if (!this.lockHeld) {
			const first = this.acquireCredentialLock(
				this.lockManager,
				"feishu-app-id",
				this.botOpenId !== "" ? this.botOpenId : "app-id",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.acquireCredentialLock(
				this.lockManager,
				"feishu-app-id",
				this.botOpenId !== "" ? this.botOpenId : "app-id",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"feishu-app-id",
				this.botOpenId !== "" ? this.botOpenId : "app-id",
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}

	buildMissingSecretSibling(): FeishuAdapter {
		return new FeishuAdapter({
			manifestName: `${this.manifestName}-no-secret`,
			transport: this.transportFactory,
			rest: this.rest,
			clock: this.clock,
			secretReader: () => undefined,
		});
	}

	setBotName(name: string): void {
		this.botName = name;
	}

	/** Unit-length probe used by tests (codepoints — Python len() parity). */
	static unitLength(text: string): number {
		return codePointLen(text);
	}
}

// ── helpers ───────────────────────────────────────────────────────────────

function asRecord(value: unknown): Record<string, unknown> {
	return typeof value === "object" && value !== null
		? (value as Record<string, unknown>)
		: ({} as Record<string, unknown>);
}

function truthy(raw: string | undefined): boolean {
	if (raw === undefined) return false;
	const v = raw.trim().toLowerCase();
	return v === "1" || v === "true" || v === "yes" || v === "on";
}

function normalizeWebhookHeaders(
	headers: Record<string, string> | undefined,
): Record<string, string> {
	const out: Record<string, string> = {};
	for (const [k, v] of Object.entries(headers ?? {})) out[k.toLowerCase()] = v;
	return out;
}

/** hmac.compare_digest parity: unequal lengths fail WITHOUT content compare
 * (node timingSafeEqual throws on length mismatch). */
function timingSafeEqualUtf8(a: string, b: string): boolean {
	const ab = Buffer.from(a, "utf8");
	const bb = Buffer.from(b, "utf8");
	if (ab.length !== bb.length) return false;
	return nodeTimingSafe(ab, bb);
}

/**
 * _resolve_outbound_file_routing (:5234): extension → (file_type,
 * msg_type) — ogg/opus ⇒ opus/audio; mp4 family ⇒ mp4/media; known doc
 * types keep their enum value; everything else rides "stream"/"file".
 */
function resolveOutboundFileRouting(
	filePath: string,
	requestedMessageType: "audio" | "media" | "file",
): { fileType: string; messageType: "audio" | "media" | "file" } {
	const ext = extnameLower(filePath);
	if (ext === ".ogg" || ext === ".opus")
		return { fileType: "opus", messageType: "audio" };
	if (FEISHU_MEDIA_UPLOAD_EXTENSIONS.has(ext))
		return { fileType: "mp4", messageType: "media" };
	const docType = FEISHU_DOC_UPLOAD_TYPES.get(ext);
	if (docType !== undefined) return { fileType: docType, messageType: "file" };
	// requested_message_type is consulted only for the default leg (:5252).
	void requestedMessageType;
	return { fileType: "stream", messageType: "file" }; // _FEISHU_FILE_UPLOAD_TYPE :203
}

/** _FEISHU_MEDIA_UPLOAD_EXTENSIONS :205. */
const FEISHU_MEDIA_UPLOAD_EXTENSIONS = new Set([
	".mp4",
	".mov",
	".avi",
	".m4v",
]);
/** _FEISHU_DOC_UPLOAD_TYPES :206–212. */
const FEISHU_DOC_UPLOAD_TYPES = new Map([
	[".pdf", "pdf"],
	[".doc", "doc"],
	[".docx", "doc"],
	[".xls", "xls"],
	[".xlsx", "xls"],
	[".ppt", "ppt"],
	[".pptx", "ppt"],
]);

function extnameLower(filePath: string): string {
	const base = filePath.split("/").pop() ?? filePath;
	const dot = base.lastIndexOf(".");
	return dot <= 0 ? "" : base.slice(dot).toLowerCase();
}

function hasReplyFallbackCode(errorText: string): boolean {
	return FEISHU_REPLY_FALLBACK_CODES.some((c) => errorText.includes(String(c)));
}

/** Strip path separators/control chars from a vendor filename before it can
 * land under the media cache root (whatsapp-cloud MEDIA_ID_SAFE_RE spirit). */
function safeCacheFilename(filename: string): string {
	const cleaned = filename.replace(/[^A-Za-z0-9._-]/g, "_");
	return cleaned === "" || cleaned === "." || cleaned === ".."
		? "resource"
		: cleaned;
}

/** Extension backfill when the vendor filename carries none. */
function extensionForContentType(contentType: string): string {
	const mime = contentType.split(";")[0]?.trim().toLowerCase() ?? "";
	if (mime.startsWith("image/"))
		return `.${mime.slice("image/".length) || "jpg"}`;
	if (mime.startsWith("audio/"))
		return `.${mime.slice("audio/".length) || "ogg"}`;
	if (mime.startsWith("video/")) return ".mp4";
	return ".bin";
}

/**
 * _get_audio_duration_ms (:4662) — OGG/Opus duration in milliseconds from
 * the LAST granule position / 48000 Hz sample rate. Pure byte parsing, no
 * deps; 0 for non-OGG or malformed containers.
 */
export function audioDurationMs(data: Uint8Array): number {
	try {
		let pos = 0;
		let lastGranule = 0;
		while (pos < data.length - 27) {
			const idx = Buffer.from(data).indexOf("OggS", pos, "latin1");
			if (idx === -1) break;
			pos = idx;
			const granule = Number(
				Buffer.from(data.buffer, data.byteOffset + pos + 6, 8).readBigUInt64LE(
					0,
				),
			);
			const numSegments = data[pos + 26] ?? 0;
			if (granule > 0) lastGranule = granule;
			let pageSize = numSegments;
			for (let i = 0; i < numSegments; i++) {
				pageSize += data[pos + 27 + i] ?? 0;
			}
			pos += pageSize;
		}
		return lastGranule > 0 ? Math.trunc(lastGranule / 48) : 0; // /48000*1000
	} catch {
		return 0;
	}
}

export interface MeetingInviteDispatchRecord {
	outcome:
		| "dispatched"
		| "dropped_malformed"
		| "dropped_duplicate"
		| "dropped_no_inviter_id";
	key: string;
	prompt: string;
}
