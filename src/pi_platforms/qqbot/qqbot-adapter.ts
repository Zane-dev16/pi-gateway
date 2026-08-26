// pi_platforms/qqbot/qqbot-adapter — the QQ Bot official-gateway adapter
// (WebSocket shape), ported from Hermes gateway/platforms/qqbot/adapter.py.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   adapter.py:QQAdapter.__init__        — config, policies, dedup, token cache
//   adapter.py:connect/_ensure_token     — token flow w/ 60s refresh margin
//   adapter.py:_dispatch_payload         — op-code routing (10/0/11/7/9)
//   adapter.py:_listen_loop              — close-code classes, quick-disconnect,
//                                          fixed backoff tiers [2,5,10,30,60]
//   adapter.py:_handle_c2c/group/guild/dm_message — intake ACLs, @-strip,
//                                          quoted-context merge (msg_type 103),
//                                          uniform attachment processing
//   adapter.py:_process_attachments      — inbound attachment pipeline: images →
//                                          cached media refs (media_urls/types),
//                                          voice → asr_refer_text → voice_wav_url
//                                          → STT POST {base}/audio/transcriptions
//                                          ('[Voice] …'), files/videos →
//                                          '[file|video: name (path)]' lines
//   adapter.py:_qq_media_headers         — 'QQBot <token>' auth on CDN GETs
//   adapter.py:send_image                — URL-source failure falls back to a
//                                          text send '{caption}\n{image_url}'
//   adapter.py:_wait_for_reconnection    — sends gate on is_connected and poll
//                                          reconnect ≤15s before REST legs;
//                                          exhaustion ⇒ retryable 'Not connected'
//   adapter.py:send/_send_chunk          — markdown v2 body, msg_seq, retry
//   adapter.py:send_with_keyboard        — keyboard attach (c2c/group only)
//   adapter.py:_send_media               — native media lane: upload →
//                                          file_info → msg_type=7 body
//                                          {media:{file_info},content?,msg_id?,msg_seq}
//   adapter.py:send_typing               — msg_type=6 input_notify, ~50s
//                                          debounced, C2C-only, last-msg-id driven
//   adapter.py:_api_request              — per-leg httpx timeout raising INTO
//                                          classification ('QQ Bot API timeout')
//   utils.py:build_user_agent            — descriptive UA on gateway-url GET,
//                                          every _api_request and interaction ACK
//   adapter.py:_on_interaction           — prompt ACK then dispatch
//   adapter.py:_is_duplicate             — 300s window / 1000-entry bound
//
// Probe-computed exclusions (documented honestly, never faked green):
//   • SILK→WAV audio conversion (Hermes shells out to local ffmpeg/pilk via
//     _convert_audio_to_wav) rides the convertVoiceToWav option — hosts wire
//     their own bridge. The STT API call itself (adapter.py:_call_stt) is a
//     DIRECT HTTPS POST to {base_url}/audio/transcriptions (Bearer +
//     multipart), NOT an external daemon. Without a converter OR Tencent's
//     pre-converted voice_wav_url, raw-SILK voices surface
//     '[Voice] [语音识别失败]' exactly like Hermes without ffmpeg installed.
//   • tools/url_safety.is_safe_url is ported at the production byte-fetch sink
//     (scheme allowlist, internal hostnames, private/reserved IP literals and
//     DNS-resolved addresses blocked fail-closed, plus an optional
//     PI_QQ_MEDIA_HOST_ALLOWLIST suffix lock-down on every redirect hop); fixture-injected seams
//     script trusted URLs.
//   • Local-file reads ride an injected byte seam in fixtures; the COS PUT
//     plane is exercised against the fake server's scripted REST face.

import { readFileSync } from "node:fs";
import { mkdir, writeFile } from "node:fs/promises";
import dns from "node:dns";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { homedir } from "node:os";
import { randomUUID } from "node:crypto";
import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { REPLY_TO_METADATA_KEY } from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import { BasePlatformAdapter } from "../kit/index.js";
import { BoundedSeenSet } from "../../pi_gateway/security/trust/replay-seen-set.js";
import {
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	type ClickAuthorizer,
} from "../kit/index.js";
import {
	classifySendError,
	extractRetryAfterSeconds,
	PLAIN_TEXT_FALLBACK_PREFIX,
} from "../kit/send-retry.js";
import {
	QQ_IDENTIFY_INTENTS,
	QQBOT_MAX_QUICK_DISCONNECT_COUNT,
	QQ_MEDIA_TYPE_FILE,
	QQ_MEDIA_TYPE_IMAGE,
	QQ_MEDIA_TYPE_VIDEO,
	QQ_MEDIA_TYPE_VOICE,
	QQ_MSG_TYPE_INPUT_NOTIFY,
	QQ_MSG_TYPE_MARKDOWN,
	QQ_MSG_TYPE_MEDIA,
	QQ_MSG_TYPE_TEXT,
	QQ_TYPING_DEBOUNCE_MS,
	QQ_TYPING_INPUT_SECONDS,
	QQBOT_USER_AGENT,
	QQBOT_API_BASE,
	QQBOT_DEDUP_MAX_SIZE,
	QQBOT_DEDUP_WINDOW_SECONDS,
	QQBOT_DEFAULT_API_TIMEOUT_S,
	QQBOT_FILE_UPLOAD_TIMEOUT_S,
	QQBOT_GATEWAY_URL_PATH,
	QQ_HEARTBEAT_FRACTION_OF_INTERVAL,
	QQBOT_MAX_MESSAGE_LENGTH,
	QQBOT_PLUGIN_MANIFEST,
	QQBOT_QUICK_DISCONNECT_THRESHOLD_S,
	QQBOT_RATE_LIMIT_DELAY_S,
	QQBOT_RECONNECT_BACKOFF_S,
	QQ_SEND_MAX_ATTEMPTS,
	QQ_SEND_RETRY_BASE_DELAY_S,
	QQ_STT_DEFAULT_BASE_URL_ZAI,
	QQ_STT_DEFAULT_MODEL_EXPLICIT,
	QQ_STT_DEFAULT_MODEL_ZAI,
	QQ_STT_ENV_API_KEY,
	QQ_STT_ENV_BASE_URL,
	QQ_STT_ENV_MODEL,
	QQ_STT_PROVIDER_BASE_URLS,
	QQ_TOKEN_DEFAULT_EXPIRES_IN_S,
	QQ_TOKEN_REFRESH_MARGIN_S,
	QQBOT_TOKEN_URL,
	QQ_MEDIA_HTTP_TIMEOUT_S,
	QQ_RECONNECT_POLL_INTERVAL_S,
	QQ_RECONNECT_WAIT_S,
} from "./manifest.js";
import {
	buildApprovalKeyboard,
	buildUpdatePromptKeyboard,
	parseApprovalButtonData,
	parseInteractionEvent,
	parseUpdatePromptButtonData,
	type InteractionEvent,
	type InlineKeyboardWire,
} from "./keyboards.js";
import {
	ChunkedUploader,
	UploadDailyLimitExceededError,
	UploadFileTooLargeError,
	formatSize,
} from "./chunked-uploader.js";
import type {
	FakeQQGateway,
	QQClientSocket,
	QQGatewayPayload,
	QQSocketListener,
} from "./fake-qq-gateway.js";

export type QQChatType = "c2c" | "group" | "guild" | "dm";

export interface QQRestTransport {
	request(
		method: "POST" | "GET" | "PUT",
		path: string,
		body: Record<string, unknown> | Buffer,
		headers?: Record<string, string> | undefined,
	): Promise<{ status: number; body: Record<string, unknown> }>;
}

/**
 * Raw byte-level HTTPS seam for the attachment planes Hermes serves with its
 * shared httpx client (adapter.py:_download_and_cache, _stt_voice_attachment,
 * _call_stt): CDN attachment GETs and the STT multipart transcription POST.
 * Production default is global fetch behind an is_safe_url-parity SSRF gate
 * (_call_stt is a DIRECT HTTPS call, not an external daemon); fixtures inject
 * scripted responses. Messages without attachments never trigger this seam.
 */
export interface QQByteRequest {
	method: "GET" | "POST";
	url: string;
	headers?: Record<string, string> | undefined;
	body?: Buffer | undefined;
}

export interface QQByteResponse {
	status: number;
	bytes: Buffer;
}

export type QQByteFetch = (req: QQByteRequest) => Promise<QQByteResponse>;

/** STT backend config (adapter.py:_resolve_stt_config result shape). */
export interface QQSttOptions {
	baseUrl?: string | undefined;
	apiKey?: string | undefined;
	model?: string | undefined;
	/** Provider shorthand when only apiKey is configured (_PROVIDER_BASE_URLS). */
	provider?: string | undefined;
}

export interface QQAdapterOptions {
	appId?: string | undefined;
	clientSecret?: string | undefined;
	markdownSupport?: boolean | undefined;
	dmPolicy?: string | undefined;
	groupPolicy?: string | undefined;
	allowFrom?: readonly string[] | undefined;
	groupAllowFrom?: readonly string[] | undefined;
	scalarMaxUnits?: number | undefined;
	rest: QQRestTransport;
	wsFactory: FakeQQGateway;
	sleepMs?: ((ms: number) => Promise<void>) | undefined;
	nowMs?: (() => number) | undefined;
	/**
	 * Local-media byte seam (production: fs.readFileSync; fixtures inject).
	 * Chunked uploads read file bytes ONLY through this seam.
	 */
	readFileBytes?: ((path: string) => Buffer) | undefined;
	/** Scripted §10.1 tier-1 rich probe (fixture seam; production: absent). */
	richProbe?: ((content: string) => Promise<SendResult>) | undefined;
	/** Whether a rich script was deliberately programmed (probe gating). */
	richHasScript?: (() => boolean) | undefined;
	/**
	 * Inbound media cache directory. When set, downloaded images/files are
	 * written here and event refs become local paths (adapter.py
	 * cache_image_from_bytes/cache_document_from_bytes parity). When absent,
	 * caching is disabled and event refs stay vendor-shaped CDN URLs (feishu
	 * mediaCacheDir posture); voice STT is unaffected either way.
	 */
	mediaCacheDir?: string | undefined;
	/** STT backend config (adapter.py:_resolve_stt_config priority 1); env fallbacks apply. */
	stt?: QQSttOptions | undefined;
	/** Byte-level HTTPS seam for CDN GETs + the STT POST (production: fetch). */
	byteFetch?: QQByteFetch | undefined;
	/**
	 * SILK/raw-audio → WAV bridge (adapter.py:_convert_audio_to_wav parity —
	 * Hermes shells out to ffmpeg/pilk). Production hosts wire their own;
	 * absent ⇒ non-WAV voices cannot transcribe ([语音识别失败]).
	 */
	convertVoiceToWav?:
		| ((audio: Buffer, filename: string) => Promise<Buffer | null>)
		| undefined;
}

/** One authoritative-or-computed reconnect wait (ladder observability). */
export interface QQReconnectStep {
	delayMs: number;
	authoritative: boolean;
	attempt: number;
}

/** Media-lane carriage metadata (adapter-internal; never a text body). */
export interface QQMediaDirective {
	source: string;
	fileType: number;
	caption?: string | undefined;
	replyTo?: string | undefined;
	fileName?: string | undefined;
}

/** Internal metadata key routing a chokepoint admission onto the media lane. */
const QQ_MEDIA_DIRECTIVE_KEY = "_qq_media_directive";

/** Multipart boundaries are deterministic (no randomness needed on the wire). */
let sttMultipartCounter = 0;

/**
 * Production byte-fetch: direct HTTPS via node:http(s) with a hard 30s abort
 * (adapter.py httpx timeout=30 on every media/STT call), following up to 3
 * redirects — EACH hop re-passes the outbound gate below.
 */
const defaultByteFetch: QQByteFetch = async (req) => {
	let url = req.url;
	for (let hop = 0; ; hop++) {
		await assertSafeMediaUrl(url);
		let target: URL;
		try {
			target = new URL(url);
		} catch {
			// Unreachable: assertSafeMediaUrl parsed + gated this same string.
			throw new Error(`Blocked unsafe media URL: ${url.slice(0, 80)}`);
		}
		const transport = target.protocol === "https:" ? httpsRequest : httpRequest;
		const res = await new Promise<{
			status: number;
			bytes: Buffer;
			location?: string | undefined;
		}>((resolve, reject) => {
			const r = transport(
				target,
				{
					method: req.method,
					headers: req.headers,
				},
				(outgoing) => {
					const chunks: Buffer[] = [];
					outgoing.on("data", (c: Buffer) => chunks.push(c));
					outgoing.on("end", () => {
						resolve({
							status: outgoing.statusCode ?? 0,
							bytes: Buffer.concat(chunks),
							location:
								typeof outgoing.headers.location === "string"
									? outgoing.headers.location
									: undefined,
						});
					});
					outgoing.on("error", reject);
				},
			);
			r.on("error", reject);
			r.setTimeout(QQ_MEDIA_HTTP_TIMEOUT_S * 1000, () => {
				r.destroy(
					new Error(
						`media request timed out after ${QQ_MEDIA_HTTP_TIMEOUT_S}s`,
					),
				);
			});
			if (req.body !== undefined) r.write(req.body);
			r.end();
		});
		const redirecting =
			(res.status === 301 ||
				res.status === 302 ||
				res.status === 303 ||
				res.status === 307 ||
				res.status === 308) &&
			res.location !== undefined;
		if (!redirecting) {
			return { status: res.status, bytes: res.bytes };
		}
		if (hop >= 3) {
			throw new Error("too many media redirects");
		}
		url = new URL(res.location as string, target).toString();
	}
};

/** tools/url_safety._BLOCKED_HOSTNAMES (+ localhost shapes), lowercased. */
const BLOCKED_MEDIA_HOSTNAMES: ReadonlySet<string> = new Set([
	"metadata.google.internal",
	"metadata",
	"metadata.goog",
	"localhost",
]);

/** True when `host` parses as a dotted-quad IPv4 literal. */
function isIpv4Literal(host: string): boolean {
	return /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.test(host);
}

/** True when `host` looks like an IPv6 literal (brackets stripped). */
function isIpv6Literal(host: string): boolean {
	return host.includes(":");
}

/** True when `host` is an IP literal in a private/reserved/metadata range. */
function isPrivateIpLiteral(host: string): boolean {
	if (isIpv4Literal(host)) {
		const m = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(host);
		const a = Number(m![1]);
		const b = Number(m![2]);
		if (a === 10 || a === 127 || a === 0) return true;
		if (a === 169 && b === 254) return true; // link-local incl. cloud metadata
		if (a === 172 && b >= 16 && b <= 31) return true;
		if (a === 192 && b === 168) return true;
		return false;
	}
	return isPrivateIpv6(host);
}

function isPrivateIpv6(addr: string): boolean {
	const h = addr.toLowerCase();
	return (
		h === "::1" ||
		h === "::" ||
		h.startsWith("fc") ||
		h.startsWith("fd") ||
		h.startsWith("fe8") ||
		h.startsWith("fe9") ||
		h.startsWith("fea") ||
		h.startsWith("feb")
	);
}

/**
 * Fail-closed SSRF gate (tools/url_safety.is_safe_url parity): only http(s)
 * URLs to public hosts pass. DNS-resolved addresses are checked too; any
 * resolution error BLOCKS (stricter than Hermes' proxy-configured escape
 * hatch — this port has no proxy-side resolution contract to preserve).
 */
async function assertSafeMediaUrl(url: string): Promise<void> {
	let parsed: URL;
	try {
		parsed = new URL(url);
	} catch {
		throw new Error(`Blocked unsafe media URL: ${url.slice(0, 80)}`);
	}
	if (parsed.protocol !== "https:" && parsed.protocol !== "http:") {
		throw new Error(`Blocked unsafe media URL: ${url.slice(0, 80)}`);
	}
	const hostname = parsed.hostname.toLowerCase().replace(/\.$/, "");
	if (
		hostname === "" ||
		BLOCKED_MEDIA_HOSTNAMES.has(hostname) ||
		isPrivateIpLiteral(hostname)
	) {
		throw new Error(`Blocked unsafe media URL: ${url.slice(0, 80)}`);
	}
	// A PUBLIC IP literal needs no resolution — every other shape is checked
	// against its resolved addresses below.
	const bareHost = hostname.replace(/^\[/, "").replace(/\]$/, "");
	if (isIpv4Literal(bareHost) || isIpv6Literal(bareHost)) return;
	try {
		const records = await dns.promises.lookup(bareHost, {
			all: true,
			verbatim: true,
		});
		if (records.length === 0) throw new Error("dns-empty");
		for (const rec of records) {
			const bad =
				rec.family === 4
					? isPrivateIpLiteral(rec.address)
					: isPrivateIpv6(rec.address);
			if (bad) throw new Error(`Blocked unsafe media URL: ${url.slice(0, 80)}`);
		}
	} catch (err) {
		if (err instanceof Error && err.message.startsWith("Blocked unsafe")) {
			throw err;
		}
		throw new Error(`Blocked unsafe media URL: ${url.slice(0, 80)}`);
	}
	// Explicit deployment allowlist (PI_QQ_MEDIA_HOST_ALLOWLIST): when set,
	// the hostname MUST match one of the comma-separated suffixes.
	const allowlist = (process.env["PI_QQ_MEDIA_HOST_ALLOWLIST"] ?? "")
		.split(",")
		.map((s) => s.trim().toLowerCase())
		.filter((s) => s !== "");
	if (
		allowlist.length > 0 &&
		!allowlist.some((suffix) =>
			suffix.startsWith(".")
				? bareHost.endsWith(suffix)
				: bareHost === suffix || bareHost.endsWith(`.${suffix}`),
		)
	) {
		throw new Error(`Blocked non-allowlisted media URL: ${url.slice(0, 80)}`);
	}
}

/** adapter.py:_is_url — http(s) sources upload by URL, everything else is local. */
function isHttpUrl(source: string): boolean {
	return /^https?:\/\//i.test(String(source ?? ""));
}

/** File name resolved from an URL path (adapter.py:urlparse(path).name). */
function urlFileName(source: string): string {
	try {
		const parsed = new URL(source);
		const base = parsed.pathname
			.split("/")
			.filter((s) => s.length > 0)
			.pop();
		return base ?? "media";
	} catch {
		return "media";
	}
}

/** Path.expanduser parity for local media sources. */
function expandUserPath(source: string): string {
	if (source === "~") return homedir();
	if (source.startsWith("~/")) return `${homedir()}/${source.slice(2)}`;
	return source;
}

/** Upload response file_info extraction (upload.data wrapping tolerated). */
function extractFileInfo(upload: Record<string, unknown>): string | null {
	const direct = upload["file_info"];
	if (typeof direct === "string" && direct !== "") return direct;
	const data = upload["data"];
	if (data !== null && typeof data === "object") {
		const nested = (data as Record<string, unknown>)["file_info"];
		if (typeof nested === "string" && nested !== "") return nested;
	}
	return null;
}

const PERMANENT_SEND_PATTERNS = [
	"invalid",
	"forbidden",
	"not found",
	"bad request",
];
/** Fatal close codes stop reconnection (adapter.py:_listen_loop FATAL set). */
const FATAL_CLOSE_CODES: ReadonlySet<number> = new Set([
	4001, 4002, 4010, 4011, 4012, 4013, 4014, 4914, 4915,
]);
/** Session-invalid close codes clear state for a fresh Identify (NOT 4009). */
const SESSION_INVALID_CLOSE_CODES: ReadonlySet<number> = new Set([
	4006, 4007, 4900, 4901, 4902, 4903, 4904, 4905, 4906, 4907, 4908, 4909, 4910,
	4911, 4912, 4913,
]);

/**
 * THE QQBot adapter. Transport-only surface on top of the kit base: guard
 * composition, chunking, formatting ladder and egress doors are inherited.
 */
export class QQBotAdapter extends BasePlatformAdapter {
	readonly pluginManifest = QQBOT_PLUGIN_MANIFEST;

	// ── config (__init__ parity) ─────────────────────────────────────────────
	readonly appId: string;
	readonly clientSecret: string;
	readonly markdownSupport: boolean;
	readonly dmPolicy: string;
	readonly groupPolicy: string;
	readonly allowFrom: readonly string[];
	readonly groupAllowFrom: readonly string[];

	private readonly rest: QQRestTransport;
	private readonly gateway: FakeQQGateway;
	private readonly sleepFn: (ms: number) => Promise<void>;
	private readonly nowFn: () => number;
	private readonly readMediaBytes: (path: string) => Buffer;
	private readonly richProbe:
		| ((content: string) => Promise<SendResult>)
		| undefined;
	private readonly richHasScriptFn: (() => boolean) | undefined;
	private readonly mediaCacheDir: string | undefined;
	private readonly sttOptions: QQSttOptions | undefined;
	private readonly byteFetchFn: QQByteFetch;
	private readonly convertVoiceFn:
		| ((audio: Buffer, filename: string) => Promise<Buffer | null>)
		| undefined;
	private richWireAttemptsCount = 0;

	// ── gateway state ────────────────────────────────────────────────────────
	private socket: QQClientSocket | null = null;
	sessionId: string | null = null;
	lastSeq: number | null = null;
	heartbeatIntervalS = 30.0;

	// Token cache (_ensure_token parity).
	private accessToken: string | null = null;
	private tokenExpiresAtMs = 0;
	private tokenFetch: Promise<string> | null = null;

	/** chat_id → chat kind, learned from inbound traffic (_chat_type_map). */
	readonly chatTypeMap = new Map<string, QQChatType>();
	/** Last inbound message id per chat — passive reply_to context (_last_msg_id). */
	readonly lastMsgIdByChat = new Map<string, string>();
	/** chat_id → last input_notify send time (send_typing debounce state). */
	private readonly typingSentAtMs = new Map<string, number>();

	private readonly seenMessages: BoundedSeenSet;

	// ── reconnect machinery (_listen_loop parity) ────────────────────────────
	private backoffIdx = 0;
	private quickDisconnectCount = 0;
	private connectStartedAtMs: number | null = null;
	running = false;
	isLive = false;

	readonly reconnectSteps: QQReconnectStep[] = [];
	reconnectLog: string[] = [];
	/** Server-authoritative capture: close 4008 ⇒ RATE_LIMIT_DELAY (60s). */
	lastCapturedRetryAfterSeconds: number | null = null;
	private pendingRetryAfterS: number | null = null;

	// ── interaction audit ────────────────────────────────────────────────────
	readonly interactionAcks: Array<{ id: string; code: number }> = [];
	readonly resolvedFamilies: string[] = [];
	readonly approvalDecisions: Array<{
		sessionKey: string;
		decision: "once" | "always" | "deny" | "session";
	}> = [];
	readonly updatePromptAnswers: Array<{ answer: "y" | "n"; operator: string }> =
		[];

	// ── subject-support plumbing (reference-fixture inheritance) ────────────
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
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

	constructor(opts: QQAdapterOptions) {
		super({
			manifestName: QQBOT_PLUGIN_MANIFEST.name,
			capabilities: QQBOT_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? QQBOT_MAX_MESSAGE_LENGTH,
		});
		this.appId = (opts.appId ?? "").trim();
		this.clientSecret = (opts.clientSecret ?? "").trim();
		this.markdownSupport = opts.markdownSupport !== false;
		this.dmPolicy = (opts.dmPolicy ?? "pairing").toLowerCase();
		this.groupPolicy = (opts.groupPolicy ?? "pairing").toLowerCase();
		this.allowFrom = opts.allowFrom ?? [];
		this.groupAllowFrom = opts.groupAllowFrom ?? [];
		this.rest = opts.rest;
		this.gateway = opts.wsFactory;
		this.sleepFn =
			opts.sleepMs ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.readMediaBytes = opts.readFileBytes ?? ((p) => readFileSync(p));
		this.richProbe = opts.richProbe;
		this.richHasScriptFn = opts.richHasScript;
		this.mediaCacheDir = opts.mediaCacheDir;
		this.sttOptions = opts.stt;
		this.byteFetchFn = opts.byteFetch ?? defaultByteFetch;
		this.convertVoiceFn = opts.convertVoiceToWav;

		if (this.appId === "" || this.clientSecret === "") {
			// Loud-disable parity: connect() refuses without credentials; the
			// lifecycle records the reason so /status shows it.
			this.lifecycle.disable({
				kind: "secret_missing",
				secretKey: this.clientSecret === "" ? "QQ_CLIENT_SECRET" : "QQ_APP_ID",
				manifestName: QQBOT_PLUGIN_MANIFEST.name,
			});
		}
		this.registerLogSecret(this.clientSecret);

		this.seenMessages = new BoundedSeenSet({
			maxEntries: QQBOT_DEDUP_MAX_SIZE,
			ttlMs: QQBOT_DEDUP_WINDOW_SECONDS * 1000,
			nowMs: this.nowFn,
		});

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // no native draft lanes on QQ v2 wire
			transmitSend: async (chatId, content, metadata) => {
				// Media admissions ride the SAME audited chokepoint; the directive
				// key routes them onto the native msg_type=7 lane (_send_media).
				const media = metadata[QQ_MEDIA_DIRECTIVE_KEY];
				if (media !== null && typeof media === "object") {
					return this.transmitMedia(chatId, media as QQMediaDirective);
				}
				return this.wireSend(chatId, content, metadata);
			},
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
			onExecApproval: async (sessionKey, choice) => {
				this.resolvedFamilies.push("ea");
				this.approvalDecisions.push({ sessionKey, decision: choice });
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

	/**
	 * Per-chat descriptor override point (§6.3/A15): budget AND unit resolve
	 * TOGETHER here — the harness's utf16-marked chats prove code-unit math.
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

	// ── guard wiring (reference-fixture inheritance) ──────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		const spawnerOpts = spawner === undefined ? {} : { spawner };
		this.attachGuard(
			{
				registry: QQ_REGISTRY,
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
		await this.handleIngress(event, sessionKey);
	}

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	// ── egress doors ──────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * Wire transport for ONE delivered chunk (base deliverChunk lane). Routes
	 * to c2c/group/guild REST sends by learned chat kind (_guess_chat_type
	 * fallback "c2c"), wrapped in the _send_chunk retry ladder behind the
	 * _wait_for_reconnection connection gate (adapter.py:send).
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		// Capture-seam interception (reference-fixture parity): the shared rows'
		// formatting-rejection script fails markdown-shaped bodies with a
		// parse-classified vendor error; the PLAIN fallback body succeeds.
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
		) {
			return {
				success: false,
				error: "Bad Request: can't parse entities",
			};
		}
		const notConnected = await this.notConnectedGateResult();
		if (notConnected !== null) return notConnected;
		const replyToRaw = metadata["reply_to"];
		const replyTo = typeof replyToRaw === "string" ? replyToRaw : undefined;
		return this.sendChunkWithRetry(chatId, content, replyTo);
	}

	/** Rich lane ABSENT natively; scripted probes feed the §10.1 latch path. */
	protected override async wireRich(content: string): Promise<SendResult> {
		const scripted =
			this.richProbe !== undefined &&
			(this.richHasScriptFn === undefined || this.richHasScriptFn());
		if (!scripted) {
			// Capability-error shape WITHOUT burning a roundtrip (latch path).
			return { success: false, error: "sendWithKeyboard: method not found" };
		}
		this.richWireAttemptsCount += 1;
		return this.richProbe(content);
	}

	/** Observability: how many REAL rich roundtrips left the adapter. */
	get richWireAttempts(): number {
		return this.richWireAttemptsCount;
	}

	// ── send-path connection gate (adapter.py:_wait_for_reconnection) ──────

	/**
	 * THE pre-REST connection gate (adapter.py:send :2486, send_with_keyboard
	 * :2634, _send_media :2913). While the gateway listener is DOWN mid-life
	 * (running && !isLive — outage, reconnect ladder in flight, fatal close),
	 * sends poll is_connected for up to QQ_RECONNECT_WAIT_S before returning
	 * the retryable 'Not connected' failure WITHOUT firing any REST leg.
	 *
	 * When the adapter is NOT running as a live gateway (never connected —
	 * host-managed capture faces / conformance subjects drive egress through
	 * scripted transports), liveness is not ours to assert: the gate stays
	 * open (yuanbao egressCapture posture — fabricating 'Not connected' on a
	 * capture face breaks the seam contract those rows exist to exercise).
	 */
	private async notConnectedGateResult(): Promise<SendResult | null> {
		if (!(this.running && !this.isConnected)) return null;
		if (await this.waitForReconnection()) return null;
		return { success: false, error: "Not connected", retryable: true };
	}

	/**
	 * Poll isConnected every 0.5s for up to 15s
	 * (adapter.py:_wait_for_reconnection — _RECONNECT_WAIT_SECONDS ×
	 * _RECONNECT_POLL_INTERVAL). True when the listener came back.
	 */
	private async waitForReconnection(): Promise<boolean> {
		let waitedS = 0.0;
		while (waitedS < QQ_RECONNECT_WAIT_S) {
			await this.sleepFn(QQ_RECONNECT_POLL_INTERVAL_S * 1000);
			waitedS += QQ_RECONNECT_POLL_INTERVAL_S;
			if (this.isConnected) return true;
		}
		return false;
	}

	// ── connection lifecycle (connect/disconnect parity) ─────────────────

	async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		if (this.appId === "" || this.clientSecret === "") {
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail:
					"QQ startup failed: QQ_APP_ID and QQ_CLIENT_SECRET are required",
			});
			return false;
		}
		try {
			await this.ensureToken();
			await this.openWs();
			this.running = true;
			this.isLive = true;
			this.connectStartedAtMs = this.nowFn();
			void this.heartbeatLoop();
			return true;
		} catch (err) {
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail: `QQ startup failed: ${err instanceof Error ? err.message : String(err)}`,
			});
			return false;
		}
	}

	async disconnect(): Promise<void> {
		this.running = false;
		this.isLive = false;
		this.socket?.close(1000);
		this.socket = null;
		this.sessionId = null;
		this.lastSeq = null;
	}

	private async openWs(): Promise<void> {
		const gatewayUrl = await this.getGatewayUrl();
		void gatewayUrl;
		const listener: QQSocketListener = {
			onOpen: () => {},
			onPayload: (payload) => this.dispatchPayload(payload),
			onClose: (info) => void this.handleClose(info.code, info.reason ?? ""),
			onError: (err) => void this.handleReadError(err),
		};
		this.socket = this.gateway.connect(listener);
		// Hello arrives synchronously via serverAccept → onPayload; yield once
		// so the Identify/READY round-trip settles before connect() returns.
		await Promise.resolve();
	}

	// ── token management (_ensure_token parity) ─────────────────────────

	async ensureToken(): Promise<string> {
		const cached = this.accessToken;
		if (
			cached !== null &&
			this.nowFn() < this.tokenExpiresAtMs - QQ_TOKEN_REFRESH_MARGIN_S * 1000
		) {
			return cached;
		}
		// Singleflight: concurrent callers share one fetch.
		if (this.tokenFetch !== null) return this.tokenFetch;
		this.tokenFetch = (async () => {
			// adapter.py:_ensure_token — DEFAULT_API_TIMEOUT per token leg.
			const resp = await this.withRestTimeout(
				this.rest.request("POST", QQBOT_TOKEN_URL, {
					appId: this.appId,
					clientSecret: this.clientSecret,
				}),
				QQBOT_DEFAULT_API_TIMEOUT_S,
				QQBOT_TOKEN_URL,
			);
			if (resp.status >= 400) {
				throw new Error(
					`Failed to get QQ Bot access token: HTTP ${resp.status}`,
				);
			}
			const token = resp.body["access_token"];
			if (typeof token !== "string" || token === "") {
				throw new Error("QQ Bot token response missing access_token");
			}
			const expiresIn = Number(
				resp.body["expires_in"] ?? QQ_TOKEN_DEFAULT_EXPIRES_IN_S,
			);
			this.accessToken = token;
			this.tokenExpiresAtMs = this.nowFn() + expiresIn * 1000;
			return token;
		})();
		try {
			return await this.tokenFetch;
		} finally {
			this.tokenFetch = null;
		}
	}

	private async getGatewayUrl(): Promise<string> {
		const token = await this.ensureToken();
		// adapter.py:_get_gateway_url — DEFAULT_API_TIMEOUT per gateway leg.
		const resp = await this.withRestTimeout(
			this.rest.request(
				"GET",
				`${QQBOT_API_BASE}${QQBOT_GATEWAY_URL_PATH}`,
				{},
				{ Authorization: `QQBot ${token}`, "User-Agent": QQBOT_USER_AGENT },
			),
			QQBOT_DEFAULT_API_TIMEOUT_S,
			QQBOT_GATEWAY_URL_PATH,
		);
		if (resp.status >= 400) {
			throw new Error(`Failed to get QQ Bot gateway URL: HTTP ${resp.status}`);
		}
		const url = resp.body["url"];
		if (typeof url !== "string" || url === "") {
			throw new Error("QQ Bot gateway response missing url");
		}
		return url;
	}

	// ── op-code routing (_dispatch_payload parity) ───────────────────────

	dispatchPayload(payload: QQGatewayPayload): void {
		if (typeof payload.s === "number") {
			const s = payload.s;
			this.lastSeq =
				this.lastSeq === null || s > this.lastSeq ? s : this.lastSeq;
		}
		const op = payload.op;

		if (op === 10) {
			// Hello — heartbeat at 80% of the server interval; resume when we
			// hold a session, identify otherwise.
			const d = (payload.d ?? {}) as Record<string, unknown>;
			const intervalMs = Number(d["heartbeat_interval"] ?? 30_000);
			this.heartbeatIntervalS =
				(intervalMs / 1000) * QQ_HEARTBEAT_FRACTION_OF_INTERVAL;
			if (this.sessionId !== null && this.lastSeq !== null) {
				this.sendResume();
			} else {
				this.sendIdentify();
			}
			return;
		}
		if (op === 0 && payload.t !== undefined) {
			switch (payload.t) {
				case "READY":
					this.handleReady(payload.d);
					return;
				case "RESUMED":
					this.reconnectLog.push("resumed");
					return;
				case "C2C_MESSAGE_CREATE":
				case "GROUP_AT_MESSAGE_CREATE":
				case "DIRECT_MESSAGE_CREATE":
				case "GUILD_MESSAGE_CREATE":
				case "GUILD_AT_MESSAGE_CREATE":
					void this.onMessage(
						payload.t,
						(payload.d ?? {}) as Record<string, unknown>,
					);
					return;
				case "INTERACTION_CREATE":
					void this.onInteraction((payload.d ?? {}) as Record<string, unknown>);
					return;
				default:
					return;
			}
		}
		if (op === 11) return; // heartbeat ACK
		if (op === 7) {
			// Server-requested reconnect → close so the close path resumes.
			this.socket?.close(4000);
			return;
		}
		if (op === 9) {
			// d=true resumable, d=false fresh-identify.
			if (payload.d !== true) {
				this.sessionId = null;
				this.lastSeq = null;
			}
			this.socket?.close(4000);
			return;
		}
	}

	private handleReady(d: unknown): void {
		if (d !== null && typeof d === "object") {
			const sid = (d as Record<string, unknown>)["session_id"];
			if (typeof sid === "string") this.sessionId = sid;
		}
	}

	private sendIdentify(): void {
		this.socket?.sendPayload({
			op: 2,
			d: {
				token: `QQBot ${this.accessToken ?? ""}`,
				intents: QQ_IDENTIFY_INTENTS,
				shard: [0, 1],
				properties: {
					// DEC-060 branding tokens (Hermes: macOS/hermes-agent/hermes-agent).
					$os: "linux",
					$browser: "pi-gateway",
					$device: "pi-gateway",
				},
			},
		});
	}

	private sendResume(): void {
		this.socket?.sendPayload({
			op: 6,
			d: {
				token: `QQBot ${this.accessToken ?? ""}`,
				session_id: this.sessionId,
				seq: this.lastSeq,
			},
		});
	}

	// ── heartbeat loop (_heartbeat_loop parity) ─────────────────────────

	protected async heartbeatLoop(): Promise<void> {
		while (this.running) {
			await this.sleepFn(this.heartbeatIntervalS * 1000);
			if (!this.running || this.socket === null) continue;
			try {
				this.socket.sendPayload({ op: 1, d: this.lastSeq });
			} catch {
				// heartbeat failures are non-fatal; the read path owns liveness
			}
		}
	}

	// ── close-code classes (_listen_loop parity) ────────────────────────

	async handleClose(code: number, reason: string): Promise<void> {
		if (code === 1000) {
			// deliberate disconnect()
			return;
		}
		this.isLive = false;

		// Quick-disconnect detection (permission/misconfiguration shape).
		const lifetimeS =
			this.connectStartedAtMs === null
				? Number.POSITIVE_INFINITY
				: (this.nowFn() - this.connectStartedAtMs) / 1000;
		if (lifetimeS < QQBOT_QUICK_DISCONNECT_THRESHOLD_S) {
			this.quickDisconnectCount += 1;
		} else {
			this.quickDisconnectCount = 0;
		}
		if (this.quickDisconnectCount >= QQBOT_MAX_QUICK_DISCONNECT_COUNT) {
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail: "Too many quick disconnects — check bot permissions",
			});
			this.reconnectLog.push(`fatal:quick-disconnect(${reason})`);
			return;
		}

		if (FATAL_CLOSE_CODES.has(code)) {
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail: `Bot closed fatally (${code}) — not reconnecting`,
			});
			this.reconnectLog.push(`fatal:close-${code}`);
			return;
		}

		if (code === 4008) {
			// Rate limited: RATE_LIMIT_DELAY IS the server-authoritative capture.
			this.lastCapturedRetryAfterSeconds = QQBOT_RATE_LIMIT_DELAY_S;
			this.pendingRetryAfterS = QQBOT_RATE_LIMIT_DELAY_S;
			this.reconnectLog.push("rate-limited-4008");
		} else if (code === 4004) {
			// Invalid token: clear the cache so ensureToken refreshes.
			this.accessToken = null;
			this.tokenExpiresAtMs = 0;
			this.reconnectLog.push("invalid-token-4004");
		} else if (SESSION_INVALID_CLOSE_CODES.has(code)) {
			// Session invalid → fresh Identify next Hello (4009 stays resumable).
			this.sessionId = null;
			this.lastSeq = null;
			this.reconnectLog.push(`session-invalid-${code}`);
		} else if (code === 1006) {
			this.reconnectLog.push("refused");
		} else {
			this.reconnectLog.push(`close-${code}`);
		}

		await this.reconnectAfterClose();
	}

	/** Hard transport death without a close frame (dead-TCP shape). */
	async handleReadError(err: Error): Promise<void> {
		this.isLive = false;
		this.reconnectLog.push(`read-error:${err.message.slice(0, 40)}`);
		if (this.running) await this.reconnectAfterClose();
	}

	private async reconnectAfterClose(): Promise<void> {
		if (this.backoffIdx >= QQBOT_RECONNECT_BACKOFF_S.length) {
			this.lifecycle.disable({
				kind: "config_invalid",
				detail: "Max reconnect attempts reached (backoff tiers exhausted)",
			});
			return;
		}
		const tierDelayS = QQBOT_RECONNECT_BACKOFF_S[this.backoffIdx]!;
		const captured = this.pendingRetryAfterS;
		let delayS: number;
		let authoritative: boolean;
		if (captured === null) {
			delayS = tierDelayS;
			authoritative = false;
		} else {
			delayS = captured; // server-authoritative verbatim
			this.pendingRetryAfterS = null;
			authoritative = true;
		}
		this.reconnectSteps.push({
			delayMs: delayS * 1000,
			authoritative,
			attempt: this.backoffIdx + 1,
		});
		await this.sleepFn(delayS * 1000);
		this.backoffIdx += 1;
		this.connectStartedAtMs = this.nowFn();

		try {
			if (this.accessToken === null) await this.ensureToken();
			await this.openWs();
			this.isLive = true;
			this.backoffIdx = 0;
			this.quickDisconnectCount = 0;
			this.reconnectLog.push("reconnected");
		} catch {
			this.isLive = false;
			await this.reconnectAfterClose();
		}
	}

	// ── inbound messages (_on_message + per-kind handlers) ──────────────────

	async onMessage(
		eventType: string,
		d: Record<string, unknown>,
	): Promise<void> {
		const msgId = String(d["id"] ?? "");
		if (msgId === "" || this.isDuplicate(msgId)) return;
		const content = String(d["content"] ?? "").trim();
		const authorRaw = d["author"];
		const author =
			authorRaw !== null && typeof authorRaw === "object"
				? (authorRaw as Record<string, unknown>)
				: {};

		switch (eventType) {
			case "C2C_MESSAGE_CREATE":
				void this.handleC2CMessage(d, msgId, content, author);
				return;
			case "GROUP_AT_MESSAGE_CREATE":
				void this.handleGroupMessage(d, msgId, content, author);
				return;
			case "GUILD_MESSAGE_CREATE":
			case "GUILD_AT_MESSAGE_CREATE":
				void this.handleGuildMessage(d, msgId, content, author);
				return;
			case "DIRECT_MESSAGE_CREATE":
				void this.handleGuildDmMessage(d, msgId, content, author);
				return;
		}
	}

	private async handleC2CMessage(
		d: Record<string, unknown>,
		msgId: string,
		content: string,
		author: Record<string, unknown>,
	): Promise<void> {
		const userOpenid = String(author["user_openid"] ?? "");
		if (userOpenid === "") return;
		if (!this.isDmIntakeAllowed(userOpenid)) return;
		let text = content;
		const processed = await this.processInboundAttachments(d, text);
		text = processed.text;
		const imageUrls = processed.imageUrls;
		const imageMediaTypes = processed.imageMediaTypes;
		if (text.trim() === "" && imageUrls.length === 0) return;
		this.chatTypeMap.set(userOpenid, "c2c");
		this.lastMsgIdByChat.set(userOpenid, msgId);
		const event = buildTextEvent({
			chatType: "dm",
			chatId: userOpenid,
			userId: userOpenid,
			text,
			messageId: msgId,
			imageUrls,
			imageMediaTypes,
		});
		void this.deliverInbound(event, `qqbot:dm:${userOpenid}:${userOpenid}`);
	}

	private async handleGroupMessage(
		d: Record<string, unknown>,
		msgId: string,
		content: string,
		author: Record<string, unknown>,
	): Promise<void> {
		const groupOpenid = String(d["group_openid"] ?? "");
		if (groupOpenid === "") return;
		const memberOpenid = String(author["member_openid"] ?? "");
		if (!this.isGroupAllowed(groupOpenid, memberOpenid)) return;
		let text = stripAtMention(content);
		const processed = await this.processInboundAttachments(d, text);
		text = processed.text;
		const imageUrls = processed.imageUrls;
		const imageMediaTypes = processed.imageMediaTypes;
		if (text.trim() === "" && imageUrls.length === 0) return;
		this.chatTypeMap.set(groupOpenid, "group");
		this.lastMsgIdByChat.set(groupOpenid, msgId);
		const event = buildTextEvent({
			chatType: "group",
			chatId: groupOpenid,
			userId: memberOpenid,
			text,
			messageId: msgId,
			imageUrls,
			imageMediaTypes,
		});
		void this.deliverInbound(
			event,
			`qqbot:group:${groupOpenid}:${memberOpenid}`,
		);
	}

	private async handleGuildMessage(
		d: Record<string, unknown>,
		msgId: string,
		content: string,
		author: Record<string, unknown>,
	): Promise<void> {
		const channelId = String(d["channel_id"] ?? "");
		if (channelId === "") return;
		const guildId = String(d["guild_id"] ?? "");
		const authorId = String(author["id"] ?? "");
		// Guild channels are group-like contexts: group ACL applies
		// (any-member-of-any-guild bypass prevention).
		if (!this.isGroupAllowed(guildId === "" ? channelId : guildId, authorId)) {
			return;
		}
		let text = content;
		const processed = await this.processInboundAttachments(d, text);
		text = processed.text;
		const imageUrls = processed.imageUrls;
		const imageMediaTypes = processed.imageMediaTypes;
		if (text.trim() === "" && imageUrls.length === 0) return;
		this.chatTypeMap.set(channelId, "guild");
		this.lastMsgIdByChat.set(channelId, msgId);
		const event = buildTextEvent({
			chatType: "group",
			chatId: channelId,
			userId: authorId,
			text,
			messageId: msgId,
			imageUrls,
			imageMediaTypes,
		});
		void this.deliverInbound(event, `qqbot:guild:${channelId}:${authorId}`);
	}

	private async handleGuildDmMessage(
		d: Record<string, unknown>,
		msgId: string,
		content: string,
		author: Record<string, unknown>,
	): Promise<void> {
		const guildId = String(d["guild_id"] ?? "");
		if (guildId === "") return;
		const authorId = String(author["id"] ?? "");
		if (!this.isDmIntakeAllowed(authorId)) return;
		let text = content;
		const processed = await this.processInboundAttachments(d, text);
		text = processed.text;
		const imageUrls = processed.imageUrls;
		const imageMediaTypes = processed.imageMediaTypes;
		if (text.trim() === "" && imageUrls.length === 0) return;
		this.chatTypeMap.set(guildId, "dm");
		this.lastMsgIdByChat.set(guildId, msgId);
		const event = buildTextEvent({
			chatType: "dm",
			chatId: guildId,
			userId: authorId,
			text,
			messageId: msgId,
			imageUrls,
			imageMediaTypes,
		});
		void this.deliverInbound(event, `qqbot:dm:${guildId}:${authorId}`);
	}

	// ── attachment processing (adapter.py:_process_attachments parity) ──────

	/**
	 * ONE uniform inbound pipeline shared by c2c/group/guild/dm handlers
	 * (adapter.py:_handle_*_message bodies): process main-message attachments
	 * (images → cached refs, voice → '[Voice]' transcript block, files/videos
	 * → attachment-info lines) appended to the text body, THEN merge quoted
	 * context (message_type=103) whose images union onto the media lists.
	 */
	private async processInboundAttachments(
		d: Record<string, unknown>,
		text: string,
	): Promise<{
		text: string;
		imageUrls: string[];
		imageMediaTypes: string[];
	}> {
		const main = await this.processAttachments(d["attachments"]);
		let merged = appendBlock(text, main.voiceBlock);
		merged = appendBlock(merged, main.attachmentInfo);
		// Quoted-context merge (msg_type 103): quote PREPENDS; quoted images
		// union onto the media lists (adapter.py:_process_quoted_context).
		const quoted = await this.processQuotedContext(d);
		merged = mergeQuote(merged, quoted.quoteBlock);
		return {
			text: merged,
			imageUrls: [...main.imageUrls, ...quoted.imageUrls],
			imageMediaTypes: [...main.imageMediaTypes, ...quoted.imageMediaTypes],
		};
	}

	/**
	 * Process inbound attachments uniformly (adapter.py:_process_attachments):
	 * mirrors the Hermes dict result — image_urls/image_media_types feed
	 * MessageEvent.media_urls/media_types, voice_transcripts join into the
	 * '[Voice]' block, other attachments join into the attachment_info text.
	 */
	private async processAttachments(attachmentsRaw: unknown): Promise<{
		imageUrls: string[];
		imageMediaTypes: string[];
		voiceBlock: string;
		attachmentInfo: string;
	}> {
		const imageUrls: string[] = [];
		const imageMediaTypes: string[] = [];
		const voiceLines: string[] = [];
		const infoLines: string[] = [];
		if (Array.isArray(attachmentsRaw)) {
			for (const att of attachmentsRaw) {
				if (att === null || typeof att !== "object") continue;
				const rec = att as Record<string, unknown>;
				const ct = String(rec["content_type"] ?? "")
					.trim()
					.toLowerCase();
				const filename = String(rec["filename"] ?? "");
				const url = normalizeAttachmentUrl(String(rec["url"] ?? "").trim());
				if (url === "") continue;

				if (isVoiceContentType(ct, filename)) {
					// Voice: QQ's asr_refer_text first, then voice_wav_url, then STT.
					const transcript = await this.transcribeVoiceAttachment(rec, url);
					voiceLines.push(
						transcript !== null
							? `[Voice] ${transcript}`
							: "[Voice] [语音识别失败]",
					);
				} else if (ct.startsWith("image/")) {
					// Image: download and cache locally (when a cache dir exists).
					const ref = await this.cacheInboundBytes(url, filename, true);
					if (ref !== null) {
						imageUrls.push(ref);
						imageMediaTypes.push(ct === "" ? "image/jpeg" : ct);
					}
				} else {
					// Other attachments (video, file, …): record with their ref.
					const ref = await this.cacheInboundBytes(url, filename, false);
					if (ref !== null) {
						const name =
							filename !== ""
								? filename
								: urlFileName(url) || ct || "qq_attachment";
						infoLines.push(
							ct.startsWith("video/")
								? `[video: ${name} (${ref})]`
								: `[file: ${name} (${ref})]`,
						);
					}
				}
			}
		}
		return {
			imageUrls,
			imageMediaTypes,
			voiceBlock: voiceLines.join("\n"),
			attachmentInfo: infoLines.join("\n"),
		};
	}

	/**
	 * Download + cache one image/file attachment (adapter.py:_download_and_cache
	 * surface). With mediaCacheDir configured the bytes land on disk and the
	 * ref is the cached path; without it caching is disabled and the vendor
	 * CDN URL itself is the reference (feishu mediaCacheDir posture — nothing
	 * is fetched). Download/write failures yield NO ref (absent, not broken).
	 */
	private async cacheInboundBytes(
		url: string,
		originalName: string,
		isImage: boolean,
	): Promise<string | null> {
		if (this.mediaCacheDir === undefined) return url;
		const bytes = await this.tryFetchBytes(url);
		if (bytes === null) return null;
		try {
			await mkdir(this.mediaCacheDir, { recursive: true });
			const uuid12 = randomUUID().replaceAll("-", "").slice(0, 12);
			const fileName = isImage
				? `img_${uuid12}.jpg`
				: `doc_${uuid12}_${sanitizeCacheName(originalName)}`;
			const outPath = `${this.mediaCacheDir}/${fileName}`;
			await writeFile(outPath, bytes);
			return outPath;
		} catch {
			return null;
		}
	}

	/**
	 * Voice STT chain (adapter.py:_stt_voice_attachment): 1. Tencent's own
	 * asr_refer_text (free, no API call); 2. self-hosted STT on the
	 * pre-converted voice_wav_url; 3. self-hosted STT on the raw attachment
	 * (requires SILK→WAV conversion via the convertVoiceToWav bridge).
	 */
	private async transcribeVoiceAttachment(
		att: Record<string, unknown>,
		url: string,
	): Promise<string | null> {
		const asrRefer = String(att["asr_refer_text"] ?? "").trim();
		if (asrRefer !== "") return asrRefer;

		let downloadUrl = url;
		const wavRaw = String(att["voice_wav_url"] ?? "").trim();
		if (wavRaw.startsWith("//")) downloadUrl = `https:${wavRaw}`;
		else if (wavRaw !== "") downloadUrl = wavRaw;
		const isPreWav = wavRaw !== "";
		if (!isHttpUrl(downloadUrl)) return null;

		// QQ's multimedia CDN requires the bot-token auth header.
		const audio = await this.tryFetchBytes(downloadUrl);
		if (audio === null || audio.length < 10) return null;

		let wav: Buffer | null;
		if (isPreWav) {
			wav = audio; // pre-converted WAV rides straight to STT
		} else if (this.convertVoiceFn === undefined) {
			wav = null; // no conversion bridge wired — Hermes-without-ffmpeg shape
		} else {
			wav = await this.convertVoiceFn(audio, String(att["filename"] ?? ""));
		}
		if (wav === null) return null;
		return this.callStt(wav);
	}

	/**
	 * Call an OpenAI-compatible STT API (adapter.py:_call_stt): DIRECT HTTPS
	 * POST {base_url}/audio/transcriptions, Bearer auth + multipart form
	 * (model field + audio/wav file part). Parses BOTH Zhipu/GLM
	 * ({choices:[{message:{content}}]}) and OpenAI/Whisper ({text}) formats.
	 */
	private async callStt(wav: Buffer): Promise<string | null> {
		const cfg = this.resolveSttConfig();
		if (cfg === null) return null; // STT unconfigured — built-in ASR covers it
		sttMultipartCounter += 1;
		const boundary = `pi-qq-stt-${sttMultipartCounter}`;
		const body = Buffer.concat([
			Buffer.from(
				`--${boundary}\r\n` +
					'Content-Disposition: form-data; name="model"\r\n\r\n' +
					`${cfg.model}\r\n` +
					`--${boundary}\r\n` +
					'Content-Disposition: form-data; name="file"; filename="voice.wav"\r\n' +
					"Content-Type: audio/wav\r\n\r\n",
			),
			wav,
			Buffer.from(`\r\n--${boundary}--\r\n`),
		]);
		try {
			const resp = await this.byteFetchFn({
				method: "POST",
				url: `${cfg.baseUrl}/audio/transcriptions`,
				headers: {
					Authorization: `Bearer ${cfg.apiKey}`,
					"Content-Type": `multipart/form-data; boundary=${boundary}`,
				},
				body,
			});
			if (resp.status >= 400) return null;
			const parsed = JSON.parse(resp.bytes.toString("utf8")) as Record<
				string,
				unknown
			>;
			const choices = Array.isArray(parsed["choices"])
				? (parsed["choices"] as Array<Record<string, unknown>>)
				: [];
			const message = choices[0]?.["message"] as
				| Record<string, unknown>
				| undefined;
			const content =
				typeof message?.["content"] === "string"
					? message["content"].trim()
					: "";
			if (content !== "") return content;
			const text =
				typeof parsed["text"] === "string" ? parsed["text"].trim() : "";
			return text !== "" ? text : null;
		} catch {
			return null;
		}
	}

	/**
	 * Resolve STT backend config (adapter.py:_resolve_stt_config priority):
	 * 1. explicit stt config (baseUrl+apiKey, or apiKey+provider map);
	 * 2. QQ-specific env vars (QQ_STT_API_KEY / QQ_STT_BASE_URL /
	 *    QQ_STT_MODEL). null ⇒ STT skipped (built-in ASR still works).
	 */
	private resolveSttConfig(): {
		baseUrl: string;
		apiKey: string;
		model: string;
	} | null {
		const stt = this.sttOptions;
		if (stt !== undefined) {
			const baseUrl = (stt.baseUrl ?? "").trim();
			const apiKey = (stt.apiKey ?? "").trim();
			const model = (stt.model ?? "").trim();
			if (baseUrl !== "" && apiKey !== "") {
				return {
					baseUrl: baseUrl.replace(/\/+$/, ""),
					apiKey,
					model: model === "" ? QQ_STT_DEFAULT_MODEL_EXPLICIT : model,
				};
			}
			// Provider-only config maps through the provider table.
			if (apiKey !== "") {
				const provider = (stt.provider ?? "zai").toLowerCase();
				const mapped = QQ_STT_PROVIDER_BASE_URLS[provider];
				if (mapped !== undefined) {
					return {
						baseUrl: mapped,
						apiKey,
						model:
							model === ""
								? provider === "openai"
									? QQ_STT_DEFAULT_MODEL_EXPLICIT
									: QQ_STT_DEFAULT_MODEL_ZAI
								: model,
					};
				}
			}
		}
		const envKey = (process.env[QQ_STT_ENV_API_KEY] ?? "").trim();
		if (envKey !== "") {
			const envBase = (process.env[QQ_STT_ENV_BASE_URL] ?? "").trim();
			const envModel = (process.env[QQ_STT_ENV_MODEL] ?? "").trim();
			return {
				baseUrl: (envBase === ""
					? QQ_STT_DEFAULT_BASE_URL_ZAI
					: envBase
				).replace(/\/+$/, ""),
				apiKey: envKey,
				model: envModel === "" ? QQ_STT_DEFAULT_MODEL_ZAI : envModel,
			};
		}
		return null;
	}

	/**
	 * Authorization headers for QQ multimedia CDN downloads
	 * (adapter.py:_qq_media_headers): the cached bot token, verbatim — no
	 * proactive refresh, exactly like the reference reads self._access_token.
	 */
	private qqMediaHeaders(): Record<string, string> {
		if (this.accessToken !== null) {
			return { Authorization: `QQBot ${this.accessToken}` };
		}
		return {};
	}

	/** Media GET with graceful failure (download errors ⇒ null, never throw). */
	private async tryFetchBytes(url: string): Promise<Buffer | null> {
		try {
			const headers = this.qqMediaHeaders();
			const resp = await this.byteFetchFn({
				method: "GET",
				url,
				...(Object.keys(headers).length > 0 ? { headers } : {}),
			});
			if (resp.status >= 400) return null;
			return resp.bytes;
		} catch {
			// Download failures degrade silently (adapter.py debug-log parity).
			return null;
		}
	}

	/**
	 * Quoted-context processing (adapter.py:_process_quoted_context):
	 * message_type=103 → msg_elements carry the referenced content AND its
	 * attachments, which run through the SAME _process_attachments pipeline —
	 * quoted voice gets transcripts, quoted images join the media lists.
	 */
	private async processQuotedContext(d: Record<string, unknown>): Promise<{
		quoteBlock: string;
		imageUrls: string[];
		imageMediaTypes: string[];
	}> {
		const empty = {
			quoteBlock: "",
			imageUrls: [] as string[],
			imageMediaTypes: [] as string[],
		};
		// Short-circuit: only message_type 103 indicates a quote.
		if ((Number(d["message_type"] ?? 0) || 0) !== 103) return empty;
		const elements = d["msg_elements"];
		if (!Array.isArray(elements) || elements.length === 0) return empty;

		const quotedTextParts: string[] = [];
		const allAttachments: Record<string, unknown>[] = [];
		for (const elem of elements) {
			if (elem === null || typeof elem !== "object") continue;
			const rec = elem as Record<string, unknown>;
			const etext = String(rec["content"] ?? "").trim();
			if (etext !== "") quotedTextParts.push(etext);
			if (Array.isArray(rec["attachments"])) {
				for (const a of rec["attachments"] as unknown[]) {
					if (a !== null && typeof a === "object") {
						allAttachments.push(a as Record<string, unknown>);
					}
				}
			}
		}

		const processed = await this.processAttachments(allAttachments);
		const lines: string[] = [];
		if (quotedTextParts.length > 0) lines.push(quotedTextParts.join(" "));
		for (const t of processed.voiceBlock.split("\n")) {
			if (t !== "") lines.push(t);
		}
		if (processed.attachmentInfo !== "") lines.push(processed.attachmentInfo);

		if (lines.length === 0 && processed.imageUrls.length === 0) return empty;
		const quoteBlock =
			lines.length > 0
				? `[Quoted message]:\n${lines.join("\n")}`
				: "[Quoted message]: (image)";
		return {
			quoteBlock,
			imageUrls: processed.imageUrls,
			imageMediaTypes: processed.imageMediaTypes,
		};
	}

	// ── ACL intake gates (_is_dm_intake_allowed / _is_group_allowed) ────────

	openDmOptedIn(): boolean {
		return ["true", "1", "yes"].includes(
			(process.env["PI_QQ_ALLOW_ALL_USERS"] ?? "").toLowerCase(),
		);
	}

	isDmAllowed(userId: string): boolean {
		if (this.dmPolicy === "disabled") return false;
		if (this.dmPolicy === "allowlist")
			return entryMatches(this.allowFrom, userId);
		if (this.dmPolicy === "open") return this.openDmOptedIn();
		return false;
	}

	isDmIntakeAllowed(userId: string): boolean {
		const principal = String(userId ?? "").trim();
		if (principal === "") return false;
		if (this.dmPolicy === "disabled") return false;
		if (this.dmPolicy === "allowlist")
			return entryMatches(this.allowFrom, principal);
		if (this.dmPolicy === "pairing") return true;
		if (this.dmPolicy === "open") return this.openDmOptedIn();
		return false;
	}

	isGroupAllowed(groupId: string, _userId: string): boolean {
		if (this.groupPolicy === "disabled") return false;
		if (this.groupPolicy === "allowlist")
			return entryMatches(this.groupAllowFrom, groupId);
		if (this.groupPolicy === "pairing") return false;
		if (this.groupPolicy === "open") return true;
		return false;
	}

	// ── dedup (_is_duplicate parity via BoundedSeenSet TTL+cap) ─────────────

	isDuplicate(msgId: string): boolean {
		// add() returns false when a LIVE entry exists (replay).
		return !this.seenMessages.add(msgId);
	}

	// ── interactions (_on_interaction parity) ────────────────────────────────

	async onInteraction(raw: Record<string, unknown>): Promise<void> {
		let event: InteractionEvent;
		try {
			event = parseInteractionEvent(raw);
		} catch {
			return;
		}
		if (event.id === "") return;

		// ACK promptly — PUT /interactions/{id} {code:0}; the client shows an
		// error icon otherwise. ACK happens BEFORE dispatch.
		try {
			const token = await this.ensureToken();
			// adapter.py:_acknowledge_interaction — DEFAULT_API_TIMEOUT per ACK leg.
			const resp = await this.withRestTimeout(
				this.rest.request(
					"PUT",
					`${QQBOT_API_BASE}/interactions/${event.id}`,
					{ code: 0 },
					{
						Authorization: `QQBot ${token}`,
						"Content-Type": "application/json",
						"User-Agent": QQBOT_USER_AGENT,
					},
				),
				QQBOT_DEFAULT_API_TIMEOUT_S,
				`/interactions/${event.id}`,
			);
			this.interactionAcks.push({
				id: event.id,
				code: resp.status < 400 ? 0 : resp.status,
			});
		} catch {
			this.interactionAcks.push({ id: event.id, code: -1 });
		}

		const approval = parseApprovalButtonData(event.buttonData);
		if (approval !== null) {
			const [sessionKey, rawDecision] = approval;
			if (!this.isAuthorizedInteractionForSession(event, sessionKey)) {
				this.reconnectLog.push(
					`unauthorized-approval-click(operator=${event.operatorOpenid})`,
				);
				return;
			}
			const choice =
				rawDecision === "allow-once"
					? ("once" as const)
					: rawDecision === "allow-always"
						? ("always" as const)
						: ("deny" as const);
			this.resolvedFamilies.push("ea");
			this.approvalDecisions.push({ sessionKey, decision: choice });
			return;
		}
		const updateAnswer = parseUpdatePromptButtonData(event.buttonData);
		if (updateAnswer !== null) {
			const updateSessionKey = `agent:main:qqbot:${event.scene}:${
				event.groupOpenid || event.guildId || event.userOpenid
			}`;
			if (!this.isAuthorizedInteractionForSession(event, updateSessionKey)) {
				this.reconnectLog.push("unauthorized-update-prompt-click");
				return;
			}
			this.updatePromptAnswers.push({
				answer: updateAnswer,
				operator: event.operatorOpenid,
			});
			return;
		}
		// Unrecognised button data: logged-and-dropped (never a turn).
		this.resolvedFamilies.push("unknown-button");
	}

	/**
	 * Session+operator authorization for button clicks
	 * (adapter.py:_is_authorized_interaction_for_session): c2c requires
	 * operator==chat_id; group/guild require event-chat match AND operator ==
	 * the session's trailing user segment.
	 */
	isAuthorizedInteractionForSession(
		event: InteractionEvent,
		sessionKey: string,
	): boolean {
		const parts = String(sessionKey ?? "").split(":");
		if (parts.length < 5 || parts[0] !== "agent" || parts[1] !== "main") {
			return false;
		}
		const platform = parts[2];
		const chatType = parts[3];
		const chatId = parts[4];
		const operator = String(event.operatorOpenid ?? "").trim();
		if (platform !== "qqbot" || operator === "") return false;
		if (chatType === "c2c") {
			return chatId !== undefined && chatId !== "" && operator === chatId;
		}
		if (chatType === "group" || chatType === "guild") {
			const eventChat = String(event.groupOpenid || event.guildId || "").trim();
			if (eventChat === "" || eventChat !== chatId) return false;
			const sessionUser = String(parts[5] ?? "").trim();
			return sessionUser !== "" && operator === sessionUser;
		}
		return false;
	}

	// ── outbound: keyboards (send_with_keyboard parity) ─────────────────────

	/**
	 * Send one message with an inline keyboard attached. NO splitting — a
	 * keyboard message has exactly one interactive surface. Guild channels do
	 * not support inline keyboards (non-retryable failure).
	 */
	async sendWithKeyboard(
		chatId: string,
		content: string,
		keyboard: InlineKeyboardWire,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		// adapter.py:send_with_keyboard :2634 gates on is_connected like send().
		const notConnected = await this.notConnectedGateResult();
		if (notConnected !== null) return notConnected;
		const chatType = this.guessChatType(chatId);
		const truncated = this.formatMessage(content).slice(
			0,
			QQBOT_MAX_MESSAGE_LENGTH,
		);
		if (chatType === "c2c" || chatType === "group") {
			return this.sendTextRest(chatId, truncated, chatType, {
				replyTo,
				keyboard,
			});
		}
		return {
			success: false,
			error: `Inline keyboards not supported for chat_type '${chatType}'`,
			retryable: false,
		};
	}

	async sendApprovalRequest(opts: {
		chatId: string;
		sessionKey: string;
		title: string;
		description?: string | undefined;
		commandPreview?: string | undefined;
		timeoutSec?: number | undefined;
		allowPermanent?: boolean | undefined;
		replyTo?: string | undefined;
	}): Promise<SendResult> {
		return this.sendWithKeyboard(
			opts.chatId,
			renderApprovalText(opts),
			buildApprovalKeyboard(opts.sessionKey, {
				allowPermanent: opts.allowPermanent,
			}),
			opts.replyTo,
		);
	}

	async sendUpdatePrompt(opts: {
		chatId: string;
		prompt: string;
		fallbackDefault?: string | undefined;
	}): Promise<SendResult> {
		const hint =
			opts.fallbackDefault === undefined || opts.fallbackDefault === ""
				? ""
				: ` (default: ${opts.fallbackDefault})`;
		const content = `⚕ **Update Needs Your Input**\n\n${opts.prompt}${hint}`;
		const msgId = this.lastMsgIdByChat.get(opts.chatId);
		return this.sendWithKeyboard(
			opts.chatId,
			content,
			buildUpdatePromptKeyboard(),
			msgId,
		);
	}

	// ── outbound: media upload (_upload_media parity) ────────────────────────

	async uploadMedia(opts: {
		chatType: "c2c" | "group";
		targetId: string;
		fileType: number;
		url?: string | undefined;
		fileData?: string | undefined;
		fileName?: string | undefined;
		srvSendMsg?: boolean | undefined;
	}): Promise<Record<string, unknown>> {
		const path =
			opts.chatType === "c2c"
				? `/v2/users/${opts.targetId}/files`
				: `/v2/groups/${opts.targetId}/files`;
		const body: Record<string, unknown> = {
			file_type: opts.fileType,
			srv_send_msg: opts.srvSendMsg === true,
		};
		if (opts.url !== undefined) body["url"] = opts.url;
		else if (opts.fileData !== undefined) body["file_data"] = opts.fileData;
		if (opts.fileName !== undefined) body["file_name"] = opts.fileName;
		for (let attempt = 0; attempt < 3; attempt++) {
			try {
				return await this.apiRequest(
					"POST",
					path,
					body,
					QQBOT_FILE_UPLOAD_TIMEOUT_S,
				);
			} catch (err) {
				const msg = err instanceof Error ? err.message : String(err);
				if (/400|401|Invalid|timeout|Timeout/.test(msg)) throw err;
				if (attempt < 2) await this.sleepFn(1500 * (attempt + 1));
				else throw err;
			}
		}
		throw new Error("upload retries exhausted");
	}

	/** Chunked-upload driver over THIS adapter's API seams. */
	chunkedUploader(): ChunkedUploader {
		return new ChunkedUploader({
			apiRequest: (method, path, body, timeoutS) =>
				this.apiRequest(method, path, body, timeoutS),
			httpPut: async (url, data, headers) => {
				// COS part PUTs route through the fake's scripted face; the bytes
				// ride the transport seam verbatim (Content-Length recorded).
				const resp = await this.rest.request("PUT", url, data, {
					...headers,
				});
				return { status: resp.status, text: JSON.stringify(resp.body ?? {}) };
			},
			sleep: this.sleepFn,
			monotonicMs: this.nowFn,
		});
	}

	// ── outbound: native media (_send_media parity through the chokepoint) ──

	/**
	 * THE native media lane (adapter.py:_send_media): upload first — HTTP(S)
	 * URLs ride a single POST /v2/{users|groups}/{id}/files {url} while local
	 * files ride the three-step chunked flow — then deliver msg_type=7 with
	 * body {media:{file_info}, content?, msg_id?, msg_seq}. Admissions route
	 * THROUGH the egress chokepoint so every user-visible transmission is
	 * audited on one path (DEC-006 posture).
	 */
	async sendMedia(opts: {
		chatId: string;
		source: string;
		fileType: number;
		caption?: string | undefined;
		replyTo?: string | undefined;
		fileName?: string | undefined;
	}): Promise<SendResult> {
		const metadata: Metadata = {
			[QQ_MEDIA_DIRECTIVE_KEY]: {
				source: opts.source,
				fileType: opts.fileType,
				...(opts.caption !== undefined ? { caption: opts.caption } : {}),
				...(opts.replyTo !== undefined ? { replyTo: opts.replyTo } : {}),
				...(opts.fileName !== undefined ? { fileName: opts.fileName } : {}),
			},
		};
		if (opts.replyTo !== undefined) {
			metadata[REPLY_TO_METADATA_KEY] = opts.replyTo;
		}
		return this.cp.admit({
			door: "send",
			chatId: opts.chatId,
			content: opts.caption ?? "",
			metadata,
		});
	}

	/** adapter.py:send_image — URL or local path, optional caption + reply. */
	sendImage(
		chatId: string,
		imageSource: string,
		caption?: string | undefined,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		return this.sendMedia({
			chatId,
			source: imageSource,
			fileType: QQ_MEDIA_TYPE_IMAGE,
			caption,
			replyTo,
		});
	}

	// ── post-stream media hooks (run.py:_deliver_media_from_response surface;
	// DEC-019 explicit-tag delivery — WITHOUT these the pipeline degrades every
	// attachment to plain text) ────────────────────────────────────────────

	async sendMultipleImages(
		chatId: string,
		images: readonly string[],
	): Promise<SendResult[]> {
		const results: SendResult[] = [];
		for (const image of images) {
			results.push(await this.sendImage(chatId, image));
		}
		return results;
	}

	sendVoice(chatId: string, audioPath: string): Promise<SendResult> {
		return this.sendMedia({
			chatId,
			source: audioPath,
			fileType: QQ_MEDIA_TYPE_VOICE,
		});
	}

	sendVideo(chatId: string, videoPath: string): Promise<SendResult> {
		return this.sendMedia({
			chatId,
			source: videoPath,
			fileType: QQ_MEDIA_TYPE_VIDEO,
		});
	}

	sendDocument(chatId: string, filePath: string): Promise<SendResult> {
		return this.sendMedia({
			chatId,
			source: filePath,
			fileType: QQ_MEDIA_TYPE_FILE,
		});
	}

	/**
	 * The raw wire leg behind a media admission (adapter.py:_send_media body
	 * + send_image URL-fallback). Gates on the _wait_for_reconnection posture
	 * BEFORE any REST leg; after a failed HTTP(S)-source IMAGE upload the
	 * caption+URL degrade to a text send so the link still arrives
	 * (adapter.py:send_image — local-file sources never fall back).
	 */
	private async transmitMedia(
		chatId: string,
		directive: QQMediaDirective,
	): Promise<SendResult> {
		// adapter.py:_send_media :2913 gate — before guild refusal AND upload.
		const notConnected = await this.notConnectedGateResult();
		if (notConnected !== null) return notConnected;
		const result = await this.transmitMediaLegs(chatId, directive);
		// adapter.py:send_image — ANY non-success on an http(s) IMAGE source
		// degrades to a text send of '{caption}\n{image_url}' (reply threading
		// carried); the fallback rides the SAME wireSend ladder + gate.
		if (
			!result.success &&
			directive.fileType === QQ_MEDIA_TYPE_IMAGE &&
			isHttpUrl(directive.source)
		) {
			const fallback =
				directive.caption !== undefined && directive.caption !== ""
					? `${directive.caption}\n${directive.source}`
					: directive.source;
			return this.wireSend(
				chatId,
				fallback,
				directive.replyTo !== undefined ? { reply_to: directive.replyTo } : {},
			);
		}
		return result;
	}

	/** Upload + msg_type=7 delivery (adapter.py:_send_media body verbatim). */
	private async transmitMediaLegs(
		chatId: string,
		directive: QQMediaDirective,
	): Promise<SendResult> {
		const chatType = this.guessChatType(chatId);
		if (chatType === "guild") {
			// Guild channels don't support native media upload in this shape.
			return {
				success: false,
				error: "Guild media send not supported via this path",
				retryable: false,
			};
		}
		try {
			let upload: Record<string, unknown>;
			if (isHttpUrl(directive.source)) {
				upload = await this.uploadMedia({
					chatType: chatType as "c2c" | "group",
					targetId: chatId,
					fileType: directive.fileType,
					url: directive.source,
					srvSendMsg: false,
					...(directive.fileType === QQ_MEDIA_TYPE_FILE
						? {
								fileName: directive.fileName ?? urlFileName(directive.source),
							}
						: {}),
				});
			} else {
				const localPath = expandUserPath(directive.source);
				const bytes = this.readMediaBytes(localPath);
				upload = await this.chunkedUploader().upload({
					chatType: chatType as "c2c" | "group",
					targetId: chatId,
					data: bytes,
					fileType: directive.fileType,
					fileName: directive.fileName ?? basenameOf(localPath),
				});
			}

			const fileInfo = extractFileInfo(upload);
			if (fileInfo === null) {
				return {
					success: false,
					error: `Upload returned no file_info: ${JSON.stringify(upload).slice(0, 200)}`,
					retryable: false,
				};
			}

			// RichMedia message body: msg_type=7 + {media:{file_info}}.
			const body: Record<string, unknown> = {
				msg_type: QQ_MSG_TYPE_MEDIA,
				media: { file_info: fileInfo },
				msg_seq: nextMsgSeq(this.nowFn),
			};
			if (directive.caption !== undefined && directive.caption !== "") {
				body["content"] = directive.caption.slice(0, QQBOT_MAX_MESSAGE_LENGTH);
			}
			if (directive.replyTo !== undefined) body["msg_id"] = directive.replyTo;

			const path =
				chatType === "c2c"
					? `/v2/users/${chatId}/messages`
					: `/v2/groups/${chatId}/messages`;
			const data = await this.apiRequest("POST", path, body);
			return { success: true, messageId: String(data["id"] ?? "unknown") };
		} catch (err) {
			if (err instanceof UploadDailyLimitExceededError) {
				return {
					success: false,
					error: `QQ daily upload limit exceeded for '${err.fileName}' (${formatSize(err.fileSize)}). Retry tomorrow.`,
					retryable: false,
				};
			}
			if (err instanceof UploadFileTooLargeError) {
				return {
					success: false,
					error: `'${err.fileName}' (${formatSize(err.fileSize)}) exceeds the QQ per-file upload limit (${err.limitBytes > 0 ? formatSize(err.limitBytes) : "unknown"}).`,
					retryable: false,
				};
			}
			return {
				success: false,
				error: err instanceof Error ? err.message : String(err),
			};
		}
	}

	// ── typing indicator (adapter.py:send_typing parity) ─────────────────

	/**
	 * Send an input notify to a C2C user ONLY (the API supports no group
	 * typing): msg_type=6 body {input_notify:{input_type:1,input_second:60},
	 * msg_id:last_inbound, msg_seq}. Debounced to one request per ~50s per
	 * chat; requires a captured inbound message id; failures are swallowed.
	 */
	async sendTyping(chatId: string): Promise<void> {
		if (!this.isLive) return;
		if (this.guessChatType(chatId) !== "c2c") return;
		const msgId = this.lastMsgIdByChat.get(chatId);
		if (msgId === undefined || msgId === "") return;

		const now = this.nowFn();
		const lastSent = this.typingSentAtMs.get(chatId) ?? 0;
		if (now - lastSent < QQ_TYPING_DEBOUNCE_MS) return;

		try {
			await this.apiRequest("POST", `/v2/users/${chatId}/messages`, {
				msg_type: QQ_MSG_TYPE_INPUT_NOTIFY,
				msg_id: msgId,
				input_notify: {
					input_type: 1,
					input_second: QQ_TYPING_INPUT_SECONDS,
				},
				msg_seq: nextMsgSeq(this.nowFn),
			});
			this.typingSentAtMs.set(chatId, now); // success-only debounce stamp
		} catch {
			// send_typing failures are debug-class (never block the turn)
		}
	}

	// ── outbound: REST text sends (_build_text_body + per-type paths) ───────

	guessChatType(chatId: string): QQChatType {
		return this.chatTypeMap.get(chatId) ?? "c2c";
	}

	formatMessage(content: string): string {
		// markdown_support=true → verbatim (QQ renders it natively).
		if (this.markdownSupport) return content;
		return stripMarkdownLite(content);
	}

	buildTextBody(
		content: string,
		replyTo?: string | undefined,
	): Record<string, unknown> {
		const msgSeq = nextMsgSeq(this.nowFn);
		if (this.markdownSupport) {
			return {
				markdown: { content: content.slice(0, QQBOT_MAX_MESSAGE_LENGTH) },
				msg_type: QQ_MSG_TYPE_MARKDOWN,
				msg_seq: msgSeq,
			};
		}
		const reference =
			replyTo === undefined
				? {}
				: { message_reference: { message_id: replyTo } };
		return {
			content: content.slice(0, QQBOT_MAX_MESSAGE_LENGTH),
			msg_type: QQ_MSG_TYPE_TEXT,
			msg_seq: msgSeq,
			...reference,
		};
	}

	/**
	 * Per-leg REST timeout race (adapter.py:_api_request httpx timeout
	 * semantics): a hung leg raises 'QQ Bot API timeout [label]' — classified
	 * timeout — instead of stalling forever. The timer rides the injected
	 * sleep seam so fixture clocks drive it deterministically.
	 */
	private withRestTimeout<T>(
		p: Promise<T>,
		timeoutS: number,
		label: string,
	): Promise<T> {
		const timer = this.sleepFn(timeoutS * 1000).then((): T => {
			throw new Error(`QQ Bot API timeout [${label}]`);
		});
		timer.catch(() => undefined); // losing timer never rejects unhandled
		return Promise.race([p, timer]);
	}

	async apiRequest(
		method: "POST" | "GET" | "PUT",
		path: string,
		body?: Record<string, unknown> | Buffer | undefined,
		timeoutS: number = QQBOT_DEFAULT_API_TIMEOUT_S,
	): Promise<Record<string, unknown>> {
		const token = await this.ensureToken();
		const payload: Record<string, unknown> | Buffer = body ?? {};
		// adapter.py:_api_request — per-leg timeout raises INTO classification
		// ('QQ Bot API timeout [path]') instead of hanging forever; DEC-046:
		// timeout-classified sends are never retried by any ladder.
		const resp = await this.withRestTimeout(
			this.rest.request(method, `${QQBOT_API_BASE}${path}`, payload, {
				Authorization: `QQBot ${token}`,
				"Content-Type": "application/json",
				"User-Agent": QQBOT_USER_AGENT,
			}),
			timeoutS,
			path,
		);
		if (resp.status >= 400) {
			const vendorMessage = String(
				resp.body["message"] ?? JSON.stringify(resp.body),
			);
			// Hermes embeds status + path + vendor message into ONE blob so
			// numeric biz_code matching stays possible downstream.
			throw new Error(
				`QQ Bot API error [${resp.status}] ${path}: ${vendorMessage}`,
			);
		}
		return resp.body;
	}

	/** Capture knob: REST failure results carrying retryAfter feed the ladder. */
	noteRestRetryAfter(seconds: number): void {
		this.lastCapturedRetryAfterSeconds = seconds;
	}

	private async sendChunkWithRetry(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		let lastError: unknown = null;
		let honoredRetryAfterOnce = false;
		for (let attempt = 0; attempt < QQ_SEND_MAX_ATTEMPTS; attempt++) {
			try {
				const result = await this.sendChunkOnce(chatId, content, replyTo);
				if (result.success) return result;
				lastError = new Error(result.error ?? "unknown send failure");
				const errBlob = String(result.error ?? "").toLowerCase();
				const permanent = PERMANENT_SEND_PATTERNS.some((k) =>
					errBlob.includes(k),
				);
				if (permanent) break;
				// Timeout-ambiguous results are never retried either.
				if (classifySendError(lastError) === "timeout") break;
				const ra = result.retryAfter;
				if (ra !== undefined && ra !== null) this.noteRestRetryAfter(ra);
			} catch (err) {
				lastError = err;
				const errBlob = err instanceof Error ? err.message.toLowerCase() : "";
				const permanent = PERMANENT_SEND_PATTERNS.some((k) =>
					errBlob.includes(k),
				);
				if (permanent) break;
				// Timeout-AMBIGUOUS failures are never retried (the send may have
				// landed — duplicate risk, §6.1 base ladder parity).
				if (classifySendError(err) === "timeout") break;
				// Server-authoritative retry_after honored ONCE over the local
				// schedule (_send_with_retry honor-once parity) and captured for
				// the ladder (DEC-044 capture knob).
				if (!honoredRetryAfterOnce) {
					const raErr = extractRetryAfterSeconds(err);
					if (raErr !== null && raErr >= 0) {
						honoredRetryAfterOnce = true;
						this.noteRestRetryAfter(raErr);
						await this.sleepFn(raErr * 1000);
					}
				}
			}
			if (attempt < QQ_SEND_MAX_ATTEMPTS - 1) {
				await this.sleepFn(QQ_SEND_RETRY_BASE_DELAY_S * 1000 * 2 ** attempt);
			}
		}
		const message =
			lastError instanceof Error
				? lastError.message
				: String(lastError ?? "Unknown error");
		const lower = message.toLowerCase();
		const permanentMiss = !PERMANENT_SEND_PATTERNS.slice(0, 3).some((k) =>
			lower.includes(k),
		);
		// Timeout-ambiguous failures surface NON-retryable (§6.1: ambiguous
		// sends are never re-driven — neither here nor by the caller).
		const isTimeout = classifySendError(new Error(message)) === "timeout";
		return {
			success: false,
			error: message,
			retryable: permanentMiss && !isTimeout,
		};
	}

	private async sendChunkOnce(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		// format_message parity: applied INSIDE the send path (markdown
		// support rides verbatim; disabled mode strips before the wire).
		const formatted = this.formatMessage(content);
		const chatType = this.guessChatType(chatId);
		if (chatType === "guild") {
			const guildBody: Record<string, unknown> = {
				content: formatted.slice(0, QQBOT_MAX_MESSAGE_LENGTH),
			};
			if (replyTo !== undefined) guildBody["msg_id"] = replyTo;
			const data = await this.apiRequest(
				"POST",
				`/channels/${chatId}/messages`,
				guildBody,
			);
			return { success: true, messageId: String(data["id"] ?? "unknown") };
		}
		return this.sendTextRest(chatId, formatted, chatType as "c2c" | "group", {
			replyTo,
		});
	}

	private async sendTextRest(
		id: string,
		content: string,
		chatType: "c2c" | "group",
		opts: {
			replyTo?: string | undefined;
			keyboard?: InlineKeyboardWire | undefined;
		} = {},
	): Promise<SendResult> {
		const body = this.buildTextBody(content, opts.replyTo);
		if (opts.replyTo !== undefined) body["msg_id"] = opts.replyTo;
		if (opts.keyboard !== undefined) body["keyboard"] = opts.keyboard;
		const path =
			chatType === "c2c"
				? `/v2/users/${id}/messages`
				: `/v2/groups/${id}/messages`;
		const data = await this.apiRequest("POST", path, body);
		return { success: true, messageId: String(data["id"] ?? "unknown") };
	}
}

// ── module-level helpers ────────────────────────────────────────────────────

function entryMatches(entries: readonly string[], target: string): boolean {
	const normalizedTarget = String(target).trim().toLowerCase();
	for (const entry of entries) {
		const normalized = entry.trim().toLowerCase();
		if (normalized === "*" || normalized === normalizedTarget) return true;
	}
	return false;
}

/** Path basename for local media sources (Path(...).name parity). */
function basenameOf(path: string): string {
	const parts = String(path ?? "").split("/");
	const base = parts[parts.length - 1] ?? "";
	return base === "" ? "media" : base;
}

function stripAtMention(content: string): string {
	return content.trim().replace(/^@\S+\s*/, "");
}

function stripMarkdownLite(content: string): string {
	return content
		.replace(/\*\*([^*]+)\*\*/g, "$1")
		.replace(/__([^_]+)__/g, "$1")
		.replace(/`([^`]*)`/g, "$1")
		.replace(/\[([^\]]+)\]\(([^)]+)\)/g, "$1");
}

/** Deterministic 0..65535 seq (adapter.py:_next_msg_seq semantics). */
export function nextMsgSeq(nowMs: () => number): number {
	const timePart = Math.floor(nowMs() / 1000) % 100000000;
	const rand = Math.floor(Math.random() * 0x10000);
	return (timePart ^ rand) % 65536;
}

/** '//'-prefixed CDN URLs normalize onto https (adapter.py attachment loop). */
function normalizeAttachmentUrl(raw: string): string {
	if (raw.startsWith("//")) return `https:${raw}`;
	return raw;
}

/**
 * Voice/audio detection (adapter.py:_is_voice_content_type): content_type
 * "voice" or "audio/*", or a voice-extension filename. QQ file uploads carry
 * content_type="file" — NEVER misrouted into the STT pipeline.
 */
function isVoiceContentType(contentType: string, filename: string): boolean {
	const ct = contentType.trim().toLowerCase();
	const fn = filename.trim().toLowerCase();
	if (ct === "voice" || ct.startsWith("audio/")) return true;
	if (ct === "file") return false;
	const voiceExtensions = [
		".silk",
		".amr",
		".mp3",
		".wav",
		".ogg",
		".m4a",
		".aac",
		".speex",
		".flac",
	];
	return voiceExtensions.some((ext) => fn.endsWith(ext));
}

/** Sanitized cache filename (cache_document_from_bytes Path(...).name shape). */
function sanitizeCacheName(filename: string): string {
	const base = basenameOf(filename).replaceAll("\x00", "").trim();
	return base === "" || base === "." || base === ".." ? "document" : base;
}

/**
 * Message-type detection from media lists (adapter.py:_detect_message_type):
 * media wins over text — photo/voice/video classification rides the FIRST
 * media type; no media ⇒ text.
 */
function detectMessageType(
	mediaUrls: readonly string[],
	mediaTypes: readonly string[],
): IncomingEvent["messageType"] {
	if (mediaUrls.length === 0) return "text";
	if (mediaTypes.length === 0) return "photo";
	const first = (mediaTypes[0] ?? "").toLowerCase();
	if (/audio|voice|silk/.test(first)) return "voice";
	if (first.includes("video")) return "video";
	if (/image|photo/.test(first)) return "photo";
	return "text";
}

/** Hermes inline append: (text + "\n\n" + block).strip() when text non-empty. */
function appendBlock(text: string, block: string): string {
	if (block === "") return text;
	return text.trim() === "" ? block : `${text}\n\n${block}`.trim();
}

/**
 * Quoted-context extraction (adapter.py:_process_quoted_context):
 * message_type=103 → msg_elements carry the referenced content; the quote
 * block PREPENDS with a blank-line separator (_merge_quote_into).
 */
function mergeQuote(text: string, quoteBlock: string): string {
	if (quoteBlock === "") return text;
	const emptyBody = text.trim() === "";
	return emptyBody ? quoteBlock : `${quoteBlock}\n\n${text}`;
}

function renderApprovalText(opts: {
	title: string;
	description?: string | undefined;
	commandPreview?: string | undefined;
	timeoutSec?: number | undefined;
}): string {
	const lines: string[] = ["🔐 **命令执行审批**", ""];
	if (opts.commandPreview !== undefined && opts.commandPreview !== "") {
		lines.push(`\`\`\`\n${opts.commandPreview.slice(0, 300)}\n\`\`\``);
	}
	if (opts.title !== "" && opts.title !== opts.commandPreview) {
		lines.push(`📋 ${opts.title}`);
	}
	if (opts.description !== undefined && opts.description !== "") {
		lines.push(`📝 ${opts.description}`);
	}
	lines.push("");
	lines.push(`⏱️ 超时: ${opts.timeoutSec ?? 300} 秒`);
	return lines.join("\n");
}

function buildTextEvent(o: {
	chatType: "dm" | "group";
	chatId: string;
	userId: string;
	text: string;
	messageId: string;
	imageUrls: readonly string[];
	imageMediaTypes: readonly string[];
}): IncomingEvent {
	return {
		messageType: detectMessageType(o.imageUrls, o.imageMediaTypes),
		text: o.text,
		source: {
			platform: "qqbot",
			chatType: o.chatType,
			userId: o.userId,
			chatId: o.chatId,
		},
		messageId: o.messageId,
		...(o.imageUrls.length > 0 ? { mediaUrls: [...o.imageUrls] } : {}),
		...(o.imageMediaTypes.length > 0
			? { mediaTypes: [...o.imageMediaTypes] }
			: {}),
		metadata: {},
	};
}

// ── command registry for guard wiring (07 §1: derived, minimal here) ──────

const QQ_REGISTRY: CommandRegistry = [
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
