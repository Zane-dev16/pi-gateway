// pi_platforms/bluebubbles/bluebubbles-adapter — THE BlueBubbles iMessage
// adapter, ported from the READ-ONLY Hermes built-in gateway/platforms/
// bluebubbles.py onto the kit base. Everything policy-shaped is inherited;
// this module supplies TRANSPORT (outbound REST to the local BlueBubbles
// macOS server + inbound webhook POSTs it registers for) and MANIFEST DATA.
//
// Shape (DEC-002 third column): outbound REST + inbound WEBHOOK ⇒ 'webhook'
// family. Capabilities AS DATA carry the 04 §8 stateless pairing
// (supports_async_delivery=False + interactive_resume=False — manifest
// DIVERGENCE note) and splits_long_messages=True (the adapter owns native
// splitting inside its REST send engine).
//
// Ported surface (file:symbol anchors, gateway/platforms/bluebubbles.py):
//   __init__ (@~120)            → config resolution parity (env fallbacks,
//                                 require_mention truthy-set, mention-pattern
//                                 compilation via helpers.compile_mention_patterns)
//   connect (@~230)             → server_url+password refusal ladder; ping +
//                                 server/info capture private_api/helper_connected;
//                                 webhook REGISTRATION lifecycle
//   _register_webhook/@~330     → GET list reuse (crash resilience) else POST
//   _unregister_webhook/@~360   → DELETE every matching registration
//   _resolve_chat_guid (@~380)  → ';' passthrough; LRU cache (cap 500);
//                                 /chat/query STRICT identifier equality —
//                                 participant membership deliberately NOT a
//                                 fallback (#24157 leak guard)
//   send (@~450)                → strip_markdown → paragraph split → per-chunk
//                                 resolve+POST /message/text with private-api
//                                 reply enrichment matrix
//   truncate_message (@~440)    → base splitter + pagination-suffix STRIP
//   _handle_webhook (@~640)     → token gate → JSON-or-form parse → event
//                                 filter → record extraction → isFromMe/tapback
//                                 drops → field chains → mention gating → read
//                                 receipt fire-and-forget → dispatch; always 200 ok
//   typing/read (@~540-590)     → gated private_api && helper_connected
//
// NO REAL NETWORK: the REST plane runs against the injected BlueBubblesRestClient
// (production wires HTTP; conformance supplies FakeBlueBubblesServer), and the
// egress door mirrors every user-visible chunk onto the subject-supplied
// CAPTURE wire (FakePlatformWire) so shared rows observe bubbles with zero
// sockets. The attachment TRANSPORT legs ARE ported (_send_attachment @~470
// multipart upload + _download_attachment @~610 byte fetch); only the local-FS
// media-cache PERSISTENCE stays upstream — mime→ext classification rides the
// closed historical override maps kept as manifest data (BB_*_EXT_OVERRIDES).

import { randomUUID } from "node:crypto";
import { readFile, stat } from "node:fs/promises";
import { basename } from "node:path";

import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	secureCompare,
} from "../kit/index.js";
import type {
	Metadata,
	SendResult,
	StreamLogger,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { REPLY_TO_METADATA_KEY } from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { CommandRegistry } from "../../pi_gateway/guards/index.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type { DisableReason } from "../kit/lifecycle-state.js";
import {
	chunkWithFenceCarry,
	codePointLen,
	classifySendError,
	DELIVERY_FAILED_NOTICE,
	FormattingLadder,
	PLAIN_TEXT_FALLBACK_PREFIX,
	sendWithRetry,
	plainTextFallbackBody,
	type ChatLengthPolicy,
} from "../kit/index.js";
import {
	BB_DEFAULT_MENTION_PATTERNS,
	BB_DEFAULT_WEBHOOK_HOST,
	BB_DEFAULT_WEBHOOK_PATH,
	BB_DEFAULT_WEBHOOK_PORT,
	BB_GUID_CACHE_SIZE,
	BB_MAX_TEXT_LENGTH,
	BB_MESSAGE_EVENTS,
	BB_SUPPORTS_MESSAGE_EDITING,
	BB_TAPBACK_ADDED,
	BB_TAPBACK_REMOVED,
	BB_WEBHOOK_MAX_BODY_BYTES,
	BLUEBUBBLES_PLUGIN_MANIFEST,
	declareBlueBubblesTrustBoundary,
	validateBlueBubblesTrustBoundary,
} from "./manifest.js";
import { stripMarkdown } from "./strip-markdown.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const BLUEBUBBLES_REGISTRY: CommandRegistry = [
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
 * Adapter configuration — Hermes reads these from `extra` with env fallbacks
 * (bluebubbles.py __init__ @~120). Key names mirror the source verbatim.
 */
export interface BlueBubblesConfig {
	server_url?: string | undefined;
	password?: string | undefined;
	webhook_host?: string | undefined;
	webhook_port?: number | undefined;
	webhook_path?: string | undefined;
	send_read_receipts?: boolean | undefined;
	require_mention?: string | boolean | undefined;
	/** JSON-list string, comma/newline-separated string, or list (compile_mention_patterns). */
	mention_patterns?: string | readonly string[] | null | undefined;
	/**
	 * Test-observability knob for the LRU eviction row; DEFAULT stays the
	 * source constant BB_GUID_CACHE_SIZE=500 (_GUID_CACHE_SIZE).
	 */
	guid_cache_size?: number | undefined;
}

/** One multipart upload part (_send_attachment `files=` shape). */
export interface BlueBubblesMultipartFile {
	/** Form field carrying the bytes ('attachment' in the vendor request). */
	field: string;
	/** Filename written into the part's Content-Disposition. */
	name: string;
	bytes: Uint8Array;
	contentType?: string | undefined;
}

/** ONE multipart upload result: HTTP status + the FULL vendor JSON envelope. */
export interface BlueBubblesMultipartResponse {
	status: number;
	/**
	 * Parsed vendor envelope body ({status,message,data}) when present —
	 * BlueBubbles reports upload success through the BODY-level status field.
	 */
	body?: Record<string, unknown> | undefined;
}

/** REST seam the adapter drives (path-shaped like the httpx calls). */
export interface BlueBubblesRestClient {
	get(path: string): Promise<{ status: number; data?: unknown }>;
	post(
		path: string,
		payload: Record<string, unknown>,
	): Promise<{ status: number; data?: unknown }>;
	del(path: string): Promise<{ status: number; data?: unknown }>;
	/**
	 * multipart/form-data POST (_send_attachment @~470): flat string fields
	 * ({chatGuid,name,tempGuid[,isAudioMessage]}) plus ONE file part.
	 */
	postMultipart(
		path: string,
		fields: Record<string, string>,
		file: BlueBubblesMultipartFile,
	): Promise<BlueBubblesMultipartResponse>;
	/** Raw-bytes GET (_download_attachment @~610: attachment/{guid}/download). */
	getBinary(path: string): Promise<{ status: number; bytes: Uint8Array }>;
}

export interface BlueBubblesAdapterOptions {
	config?: BlueBubblesConfig | undefined;
	/** Scoped reader over BLUEBUBBLES_* names (fail-closed; DEC-003/009). */
	secretReader?: ScopedSecretReader | undefined;
	restClient: BlueBubblesRestClient;
	nowMs?: (() => number) | undefined;
	scalarMaxUnits?: number | undefined;
	spawner?: TaskSpawner | undefined;
	/**
	 * Conformance-harness egress CAPTURE wire (msgraph pattern): production
	 * construction leaves it unset and wireSend still drives the REST engine;
	 * when supplied, every user-visible chunk is mirrored there AFTER its REST
	 * post succeeds so shared rows observe bubbles on FakePlatformWire (zero
	 * network).
	 */
	captureWire?: BlueBubblesCaptureWire | undefined;
	/**
	 * Lie-scan datum override: defaults to the exported BB_SUPPORTS_MESSAGE_
	 * EDITING manifest constant (signal-adapter declaredMessageEditing
	 * pattern). Flipping it flips supportsDraftStreaming().
	 */
	declaredMessageEditing?: boolean | undefined;
}

/** Capture seam (subject-supplied; family FakePlatformWire shape). */
export interface BlueBubblesCaptureWire {
	transmitSend(
		chatId: string,
		content: string,
		metadata: Record<string, unknown>,
	): Promise<SendResult>;
	hasRichScript(opKind: string): boolean;
	transmitRich(chatId: string, content: string): Promise<SendResult>;
}

export type WebhookRecord = Record<string, unknown>;

export interface HandlerResponse {
	status: number;
	contentType?: "application/json" | "text/plain" | undefined;
	body?: string | Record<string, never> | undefined;
}

export class BlueBubblesAdapter extends BasePlatformAdapter {
	readonly pluginManifest = BLUEBUBBLES_PLUGIN_MANIFEST;
	readonly trustBoundary;

	// ── config (__init__ parity) ──────────────────────────────────────────────
	readonly serverUrl: string;
	readonly password: string;
	readonly webhookHost: string;
	readonly webhookPort: number;
	readonly webhookPath: string;
	readonly sendReadReceipts: boolean;
	readonly requireMention: boolean;
	/** Compiled group wake words (helpers.compile_mention_patterns parity). */
	readonly mentionPatterns: readonly RegExp[];
	readonly guidCacheSize: number;

	private readonly restClient: BlueBubblesRestClient;
	private readonly nowFn: () => number;
	private readonly secretReader: ScopedSecretReader;
	/** None until connect() captures server/info (source Optional[bool]). */
	private privateApiEnabled: boolean | null = null;
	private helperConnected = false;

	/** OrderedDict move-to-end LRU (Map insertion order ≙ popitem(last=False)). */
	private readonly guidCache = new Map<string, string>();

	// ── runtime state ─────────────────────────────────────────────────────────
	private connectedOnce = false;
	/** Fire-and-forget read-receipt tasks kept reachable for tests (#parity). */
	private readonly backgroundTasks = new Set<Promise<unknown>>();

	/** Observability counters (row probes). */
	readonly counters = {
		unauthorized: 0,
		invalidPayload: 0,
		eventFiltered: 0,
		fromMeDropped: 0,
		tapbackDropped: 0,
		mentionDropped: 0,
		missingFields: 0,
		dispatched: 0,
		readReceiptsRequested: 0,
		/** THE parse seam must NEVER run after a gate rejection. */
		parseInvocations: 0,
	};

	readonly dispatchedEvents: Array<{
		messageId: string;
		text: string;
		source: IncomingEvent["source"];
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
	private readonly captureWire: BlueBubblesCaptureWire | undefined;
	private readonly declaredMessageEditing: boolean;
	/** Session-scoped ladder — the rich latch persists across chunks (§10.1). */
	private restLadder: FormattingLadder | null = null;
	private restLadderChatId = "";
	private allowAllClickers = true;
	private readonly clarifyArmedSet = new Set<string>();
	private holding = false;
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};

	constructor(opts: BlueBubblesAdapterOptions) {
		const config = opts.config ?? {};
		super({
			manifestName: BLUEBUBBLES_PLUGIN_MANIFEST.name,
			capabilities: BLUEBUBBLES_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? BB_MAX_TEXT_LENGTH,
		});
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.restClient = opts.restClient;
		this.captureWire = opts.captureWire;
		this.declaredMessageEditing =
			opts.declaredMessageEditing ?? BB_SUPPORTS_MESSAGE_EDITING;

		// server_url: extra first then env (bluebubbles.py __init__), through
		// _normalize_server_url (trim, http:// prefix when scheme missing,
		// rstrip ALL trailing slashes).
		this.serverUrl = normalizeServerUrl(
			config.server_url ?? process.env["BLUEBUBBLES_SERVER_URL"] ?? "",
		);
		// password: falsy extra falls through to the scoped secret (source
		// `extra.get('password') or _get_scoped_secret(...)`), default ''.
		this.password =
			stringOrEmpty(config.password) ||
			this.secretReader("BLUEBUBBLES_PASSWORD") ||
			"";
		this.webhookHost =
			firstTruthy(
				config.webhook_host,
				process.env["BLUEBUBBLES_WEBHOOK_HOST"],
				BB_DEFAULT_WEBHOOK_HOST,
			) ?? BB_DEFAULT_WEBHOOK_HOST;
		this.webhookPort = Number(
			firstTruthy(
				config.webhook_port,
				parseEnvNumber(process.env["BLUEBUBBLES_WEBHOOK_PORT"]),
				BB_DEFAULT_WEBHOOK_PORT,
			),
		);
		this.webhookPath = normalizeWebhookPath(
			firstTruthy(
				config.webhook_path,
				process.env["BLUEBUBBLES_WEBHOOK_PATH"],
				BB_DEFAULT_WEBHOOK_PATH,
			) ?? BB_DEFAULT_WEBHOOK_PATH,
		);
		this.sendReadReceipts =
			config.send_read_receipts !== undefined
				? Boolean(config.send_read_receipts)
				: true;
		const requireMentionRaw = firstTruthy(
			config.require_mention,
			process.env["BLUEBUBBLES_REQUIRE_MENTION"],
		);
		// str(_require_mention).strip().lower() in {'true','1','yes','on'} parity.
		const requireMentionNormalized = String(requireMentionRaw ?? "")
			.trim()
			.toLowerCase();
		this.requireMention = REQUIRE_MENTION_TRUTHY.has(requireMentionNormalized);
		// Key-PRESENCE semantics (`'mention_patterns' in extra`): an explicit
		// null/undefined entry wins over env and compiles the DEFAULTS, exactly
		// like the source's in-test.
		const rawPatterns = Object.hasOwn(config, "mention_patterns")
			? config.mention_patterns
			: process.env["BLUEBUBBLES_MENTION_PATTERNS"];
		this.mentionPatterns = compileMentionPatterns(
			rawPatterns,
			BB_DEFAULT_MENTION_PATTERN_SOURCES,
			this.logger,
		);
		this.guidCacheSize = Math.max(
			1,
			Number(config.guid_cache_size ?? BB_GUID_CACHE_SIZE),
		);

		// DEC-017: an incomplete trust boundary is a CONSTRUCTION-TIME error.
		this.trustBoundary = declareBlueBubblesTrustBoundary();
		const boundaryErrors = validateBlueBubblesTrustBoundary(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			const reason: DisableReason = {
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			};
			this.lifecycle.disable(reason);
		}

		// §11 step 3/4: missing required secret ⇒ LOUD disable (status-visible).
		// Hermes refuses at connect() ("BLUEBUBBLES_SERVER_URL and
		// BLUEBUBBLES_PASSWORD are required"); the kit expresses the same
		// posture at construction so /status shows the reason.
		for (const spec of BLUEBUBBLES_PLUGIN_MANIFEST.requiresEnv) {
			if (this.secretReader(spec.name) === undefined) {
				this.lifecycle.disable({
					kind: "secret_missing",
					secretKey: spec.name,
					manifestName: BLUEBUBBLES_PLUGIN_MANIFEST.name,
				});
				break;
			}
		}

		// Register the resolved password so §8 redaction scrubs it everywhere.
		if (this.password) this.registerLogSecret(this.password);

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => this.declaredMessageEditing,
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
	 * Per-chat length descriptor (§6.3/A15 relay-shaped override point): the
	 * harness's utf16-marked chats return budget AND unit TOGETHER; production
	 * chats return undefined ⇒ manifest default (MAX_TEXT_LENGTH scalar).
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

	// ── streaming probe (class attr SUPPORTS_MESSAGE_EDITING=False parity) ───

	override supportsDraftStreaming(_chatType?: string): boolean {
		return this.declaredMessageEditing;
	}

	// ── API helpers (_api_url/_api_get/_api_post parity) ─────────────────────

	/** _api_url: password rides EVERY call as ?password=<quoted> (or &). */
	apiUrl(path: string): string {
		const sep = path.includes("?") ? "&" : "?";
		return `${this.serverUrl}${path}${sep}password=${pyQuote(this.password)}`;
	}

	/** raise_for_status parity: non-2xx throws (caller ladders handle it). */
	private async apiGet(
		path: string,
	): Promise<{ status: number; data?: unknown }> {
		const res = await this.restClient.get(path);
		if (res.status < 200 || res.status >= 300) {
			throw new Error(`GET ${path} failed with status ${res.status}`);
		}
		return res;
	}

	private async apiPost(
		path: string,
		payload: Record<string, unknown>,
	): Promise<{ status: number; data?: unknown }> {
		const res = await this.restClient.post(path, payload);
		if (res.status < 200 || res.status >= 300) {
			throw new Error(`POST ${path} failed with status ${res.status}`);
		}
		return res;
	}

	// ── lifecycle (connect @~230 / disconnect @~290 parity) ─────────────────

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (!this.serverUrl || !this.password) {
			this.logger?.error?.(
				"[bluebubbles] BLUEBUBBLES_SERVER_URL and BLUEBUBBLES_PASSWORD are required",
			);
			return false;
		}
		try {
			await this.apiGet("/api/v1/ping");
			const info = await this.apiGet("/api/v1/server/info");
			const serverData = asRecord(info.data);
			this.privateApiEnabled = Boolean(serverData["private_api"]);
			this.helperConnected = Boolean(serverData["helper_connected"]);
			this.logger?.info?.(
				`[bluebubbles] connected to ${this.serverUrl} (private_api=${String(this.privateApiEnabled)}, helper=${String(this.helperConnected)})`,
			);
		} catch (exc) {
			this.logger?.error?.(
				`[bluebubbles] cannot reach server at ${this.serverUrl}: ${errorMessage(exc)}`,
			);
			return false;
		}
		this.connectedOnce = true;
		// Registration failure only WARNS upstream (connect still returns True).
		await this.registerWebhook();
		return true;
	}

	override async disconnect(): Promise<void> {
		// Unregister webhook before cleaning up — removes ALL duplicates.
		await this.unregisterWebhook();
		this.connectedOnce = false;
	}

	get isConnected(): boolean {
		return this.connectedOnce;
	}

	/** private_api captured from /server/info at connect (None before). */
	get privateApiCaptured(): boolean | null {
		return this.privateApiEnabled;
	}

	get helperConnectedFlag(): boolean {
		return this.helperConnected;
	}

	// ── webhook URL shapes (@~305-345 parity) ────────────────────────────────

	/** _webhook_url: loopback-class hosts register as localhost. */
	get webhookUrl(): string {
		let host = this.webhookHost;
		if (
			host === "0.0.0.0" ||
			host === "127.0.0.1" ||
			host === "localhost" ||
			host === "::"
		) {
			host = "localhost";
		}
		return `http://${host}:${this.webhookPort}${this.webhookPath}`;
	}

	/**
	 * _webhook_register_url: the password rides as a query param because the
	 * BlueBubbles webhook registration API cannot send custom headers.
	 */
	get webhookRegisterUrl(): string {
		const base = this.webhookUrl;
		if (this.password) return `${base}?password=${pyQuote(this.password)}`;
		return base;
	}

	/** _webhook_register_url_for_log — NEVER let the target hit agent.log raw. */
	get webhookRegisterUrlForLog(): string {
		const base = this.webhookUrl;
		if (this.password) return `${base}?password=***`;
		return base;
	}

	// ── registration lifecycle (_register_webhook/_unregister_webhook) ───────

	async findRegisteredWebhooks(
		url: string,
	): Promise<Array<Record<string, unknown>>> {
		try {
			const res = await this.apiGet("/api/v1/webhook");
			const data = res.data;
			if (Array.isArray(data)) {
				return data.filter(
					(wh): wh is Record<string, unknown> => asRecord(wh)["url"] === url,
				);
			}
		} catch {
			/* swallowed — treated as an empty list (source parity) */
		}
		return [];
	}

	/**
	 * Crash resilience — reuse an existing matching registration if present;
	 * otherwise POST one carrying the message event subset.
	 */
	async registerWebhook(): Promise<boolean> {
		const webhookUrl = this.webhookRegisterUrl;
		const existing = await this.findRegisteredWebhooks(webhookUrl);
		if (existing.length > 0) {
			this.logger?.info?.(
				`[bluebubbles] webhook already registered: ${this.webhookRegisterUrlForLog}`,
			);
			return true;
		}
		const payload = {
			url: webhookUrl,
			events: [...REGISTER_WEBHOOK_EVENTS],
		};
		try {
			const res = await this.apiPost("/api/v1/webhook", payload);
			if (res.status >= 200 && res.status < 300) {
				this.logger?.info?.(
					`[bluebubbles] webhook registered with server: ${this.webhookRegisterUrlForLog}`,
				);
				return true;
			}
			this.logger?.warn?.(
				`[bluebubbles] webhook registration returned status ${String(res.status)}`,
			);
			return false;
		} catch (exc) {
			this.logger?.warn?.(
				`[bluebubbles] failed to register webhook with server: ${errorMessage(exc)}`,
			);
			return false;
		}
	}

	/** Removes ALL matching registrations (duplicate cleanup after crashes). */
	async unregisterWebhook(): Promise<boolean> {
		const webhookUrl = this.webhookRegisterUrl;
		let removed = false;
		try {
			for (const wh of await this.findRegisteredWebhooks(webhookUrl)) {
				const whId = wh["id"];
				if (whId === undefined || whId === null || whId === "") continue;
				await this.restClient.del(
					`/api/v1/webhook/${encodeURIComponent(String(whId))}`,
				);
				removed = true;
			}
			if (removed) {
				this.logger?.info?.(
					`[bluebubbles] webhook unregistered: ${this.webhookRegisterUrlForLog}`,
				);
			}
		} catch (exc) {
			this.logger?.debug?.(
				`[bluebubbles] failed to unregister webhook (non-critical): ${errorMessage(exc)}`,
			);
		}
		return removed;
	}

	// ── chat GUID resolution (_resolve_chat_guid @~380 parity) ───────────────

	/**
	 * Raw GUID passthrough when the target contains ';'; LRU-cached strict
	 * chatIdentifier/identifier match over /api/v1/chat/query. Participant
	 * membership is intentionally NOT a fallback: the same contact appears in
	 * DMs AND groups, so a participant match would leak an outbound DM reply
	 * into a group thread (#24157). Unresolved targets are NOT cached (a later
	 * attempt after chat creation must not read a stale miss).
	 */
	async resolveChatGuid(target: string): Promise<string | null> {
		const t = (target ?? "").trim();
		if (!t) return null;
		// Already a raw GUID.
		if (t.includes(";")) return t;
		const cached = this.guidCache.get(t);
		if (cached !== undefined) {
			// move_to_end parity.
			this.guidCache.delete(t);
			this.guidCache.set(t, cached);
			return cached;
		}
		try {
			const payload = await this.apiPost("/api/v1/chat/query", {
				limit: 100,
				offset: 0,
			});
			const chats = Array.isArray(payload.data) ? payload.data : [];
			for (const raw of chats) {
				const chat = asRecord(raw);
				const guid =
					stringOrNull(chat["guid"]) ?? stringOrNull(chat["chatGuid"]);
				const identifier =
					stringOrNull(chat["chatIdentifier"]) ??
					stringOrNull(chat["identifier"]);
				// STRICT identity equality only — participants never consulted.
				if (identifier === t) {
					if (guid !== null) {
						this.guidCache.set(t, guid);
						while (this.guidCache.size > this.guidCacheSize) {
							const oldest = this.guidCache.keys().next().value;
							if (oldest === undefined) break;
							this.guidCache.delete(oldest);
						}
					}
					return guid;
				}
			}
		} catch {
			/* swallowed — falls through to null (source parity) */
		}
		return null;
	}

	/** Row observability: current LRU keys oldest-first. */
	guidCacheSnapshot(): string[] {
		return [...this.guidCache.keys()];
	}

	/** _create_chat_for_handle: create a chat by sending the first message. */
	async createChatForHandle(
		address: string,
		message: string,
	): Promise<SendResult> {
		const payload = {
			addresses: [address],
			message,
			tempGuid: this.nextTempGuid(),
		};
		try {
			const res = await this.apiPost("/api/v1/chat/new", payload);
			const data = asRecord(res.data);
			const msgId =
				stringOrNull(data["guid"]) ?? stringOrNull(data["messageGuid"]) ?? "ok";
			return { success: true, messageId: String(msgId) };
		} catch (exc) {
			return { success: false, error: errorMessage(exc) };
		}
	}

	// ── text sending (send @~450 + truncate_message @~440 parity) ───────────

	/**
	 * Base splitter + pagination-suffix STRIP: iMessage bubbles flow naturally
	 * without '(1/3)' suffixes. The kit chunker ≙ base.truncate_message; the
	 * strip regex re.sub(r"\s*\(\d+/\d+\)$', '", c) removes what it added.
	 */
	static truncateMessage(
		content: string,
		maxLength: number = BB_MAX_TEXT_LENGTH,
	): string[] {
		const plan = chunkWithFenceCarry(content, truncatePolicy(maxLength));
		return plan.chunks.map((c) => c.replace(/\s*\(\d+\/\d+\)$/, ""));
	}

	/**
	 * THE Hermes send() engine (send @~450): format_message strips markdown,
	 * paragraphs split on blank lines so each thought becomes its own bubble,
	 * oversized paragraphs truncate WITHOUT pagination suffixes, then each
	 * chunk resolves its GUID (creating a fresh DM ONLY when private_api is on
	 * and the target looks like an address) and POSTs /api/v1/message/text
	 * with the private-api reply enrichment matrix.
	 */
	async sendText(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		const text = stripMarkdown(content);
		if (!text) {
			return { success: false, error: "BlueBubbles send requires text" };
		}
		// Split on paragraph breaks (double newlines); blanks dropped, trimmed.
		const paragraphs = text
			.split(/\n\s*\n/)
			.map((p) => p.trim())
			.filter((p) => p.length > 0);
		const sources = paragraphs.length > 0 ? paragraphs : [text];
		const chunks: string[] = [];
		for (const para of sources) {
			if (codePointLen(para) <= BB_MAX_TEXT_LENGTH) {
				chunks.push(para);
			} else {
				chunks.push(...BlueBubblesAdapter.truncateMessage(para));
			}
		}
		let last: SendResult = { success: true };
		for (const chunk of chunks) {
			// Address-like targets MAY create a fresh DM — but ONLY when the
			// server has private_api enabled (else fail, never guess).
			last = await this.postResolvedBubble(chatId, chunk, replyTo);
			if (!last.success) return last;
		}
		return last;
	}

	/**
	 * One REST bubble against a RESOLVED target: GUID resolve (raw ';;'
	 * passthrough / LRU-cached strict match) → create-chat fallback for
	 * address-like targets under private_api → POST /api/v1/message/text with
	 * the private-api reply enrichment matrix.
	 */
	private async postResolvedBubble(
		chatId: string,
		message: string,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		const guid = await this.resolveChatGuid(chatId);
		if (!guid) {
			if (
				this.privateApiEnabled &&
				(chatId.includes("@") || /^\+\d/.test(chatId))
			) {
				return await this.createChatForHandle(chatId, message);
			}
			return {
				success: false,
				error: `BlueBubbles chat not found for target: ${chatId}`,
			};
		}
		const payload: Record<string, unknown> = {
			chatGuid: guid,
			tempGuid: this.nextTempGuid(),
			message,
		};
		if (
			replyTo !== undefined &&
			this.privateApiEnabled &&
			this.helperConnected
		) {
			payload["method"] = "private-api";
			payload["selectedMessageGuid"] = replyTo;
			payload["partIndex"] = 0;
		}
		try {
			const res = await this.apiPost("/api/v1/message/text", payload);
			const data = asRecord(res.data);
			const msgId =
				stringOrNull(data["guid"]) ?? stringOrNull(data["messageGuid"]) ?? "ok";
			return { success: true, messageId: String(msgId) };
		} catch (exc) {
			return { success: false, error: errorMessage(exc) };
		}
	}

	private nextTempGuid(): string {
		// datetime.utcnow().timestamp() parity — float seconds.
		return `temp-${this.nowFn() / 1000}`;
	}

	// ── media attachments (_send_attachment/_download_attachment parity) ───

	/**
	 * THE Hermes _send_attachment engine (@~470): existence-gate the file,
	 * resolve the chat GUID (NO create-chat fallback here — source parity),
	 * then multipart POST /api/v1/message/attachment with flat fields
	 * {chatGuid,name,tempGuid[,isAudioMessage:"true"]} and ONE "attachment"
	 * part (application/octet-stream). HTTP-level failures throw into the same
	 * catch; the vendor envelope's OWN status field decides success (200 ⇒
	 * data.guid). A caption rides as a SEPARATE text bubble AFTER the upload
	 * resolves — vendor ordering fires it whenever the HTTP call succeeded,
	 * even before the body-status verdict is read.
	 */
	async sendAttachmentFile(
		chatId: string,
		filePath: string,
		opts: {
			filename?: string | undefined;
			caption?: string | undefined;
			isAudioMessage?: boolean | undefined;
		} = {},
	): Promise<SendResult> {
		// os.path.isfile gate precedes EVERYTHING else in the source.
		try {
			await stat(filePath);
		} catch {
			return { success: false, error: `File not found: ${filePath}` };
		}
		const guid = await this.resolveChatGuid(chatId);
		if (!guid) {
			return { success: false, error: `Chat not found: ${chatId}` };
		}
		const fname = opts.filename ?? basename(filePath);
		try {
			// httpx's async multipart iterator reads file-like objects through a
			// synchronous chunk generator upstream — read off the loop thread;
			// here a plain awaited read plays that role.
			const bytes = await readFile(filePath);
			const fields: Record<string, string> = {
				chatGuid: guid,
				name: fname,
				tempGuid: randomUUID().replaceAll("-", ""), // uuid4().hex
			};
			if (opts.isAudioMessage === true) fields["isAudioMessage"] = "true";
			const res = await this.restClient.postMultipart(
				"/api/v1/message/attachment",
				fields,
				{
					field: "attachment",
					name: fname,
					bytes,
					contentType: "application/octet-stream",
				},
			);
			if (res.status < 200 || res.status >= 300) {
				throw new Error(
					`POST /api/v1/message/attachment failed with status ${res.status}`,
				);
			}
			if (opts.caption !== undefined && opts.caption.length > 0) {
				await this.sendText(chatId, opts.caption);
			}
			const envelope = res.body ?? {};
			if (envelope["status"] === 200) {
				const data = asRecord(envelope["data"]);
				const msgId = stringOrNull(data["guid"]);
				return {
					success: true,
					...(msgId !== null ? { messageId: msgId } : {}),
				};
			}
			return {
				success: false,
				error: stringOrNull(envelope["message"]) ?? "Attachment upload failed",
			};
		} catch (exc) {
			return { success: false, error: errorMessage(exc) };
		}
	}

	/**
	 * _download_attachment TRANSPORT leg (@~610): GET /api/v1/attachment/
	 * {quoted-guid}/download and hand back the raw bytes, or null on ANY
	 * failure (source logs a warning and returns None). Mime→ext classification
	 * and local media-cache persistence stay upstream — the closed historical
	 * override tables ride as BB_*_EXT_OVERRIDES manifest data.
	 */
	async downloadAttachment(attGuid: string): Promise<Uint8Array | null> {
		try {
			const encoded = pyQuote(attGuid); // quote(att_guid, safe='') parity
			const res = await this.restClient.getBinary(
				`/api/v1/attachment/${encoded}/download`,
			);
			if (res.status < 200 || res.status >= 300) {
				throw new Error(
					`GET /api/v1/attachment/${encoded}/download failed with status ${res.status}`,
				);
			}
			return res.bytes;
		} catch (exc) {
			this.logger?.warn?.(
				`[bluebubbles] failed to download attachment ${attGuid}: ${errorMessage(exc)}`,
			);
			return null;
		}
	}

	// ── post-stream media lanes (DEC-019 explicit-tag delivery surface) ─────

	/**
	 * run.py:_deliver_media_from_response surface. WITHOUT these bindings the
	 * post-stream rescan pass optional-chains every MEDIA-tagged file into a
	 * silent no-op. Vendor mapping (bluebubbles.py @~490-540): each image goes
	 * out as its OWN attachment bubble, voice flags isAudioMessage=true, and
	 * video/document ride the plain multipart lane.
	 */
	async sendMultipleImages(
		chatId: string,
		images: readonly string[],
	): Promise<SendResult[]> {
		const results: SendResult[] = [];
		for (const image of images) {
			results.push(await this.sendAttachmentFile(chatId, image));
		}
		return results;
	}

	/** send_voice @~514: audio uploads carry isAudioMessage="true". */
	sendVoice(chatId: string, audioPath: string): Promise<SendResult> {
		return this.sendAttachmentFile(chatId, audioPath, {
			isAudioMessage: true,
		});
	}

	/** send_video @~524: plain multipart lane. */
	sendVideo(chatId: string, videoPath: string): Promise<SendResult> {
		return this.sendAttachmentFile(chatId, videoPath);
	}

	/** send_document @~531: filename passthrough supported. */
	sendDocument(
		chatId: string,
		filePath: string,
		filename?: string | undefined,
	): Promise<SendResult> {
		return this.sendAttachmentFile(chatId, filePath, {
			...(filename !== undefined ? { filename } : {}),
		});
	}

	// ── typing indicators / read receipts (@~540-590 parity) ────────────────

	private get privateApiSurfaceReady(): boolean {
		return this.privateApiEnabled === true && this.helperConnected;
	}

	async sendTyping(chatId: string): Promise<void> {
		if (!this.privateApiSurfaceReady) return;
		try {
			const guid = await this.resolveChatGuid(chatId);
			if (guid) {
				await this.restClient.post(
					`/api/v1/chat/${encodeURIComponent(guid)}/typing`,
					{},
				);
			}
		} catch {
			/* swallowed (source bare except) */
		}
	}

	async stopTyping(chatId: string): Promise<void> {
		if (!this.privateApiSurfaceReady) return;
		try {
			const guid = await this.resolveChatGuid(chatId);
			if (guid) {
				await this.restClient.del(
					`/api/v1/chat/${encodeURIComponent(guid)}/typing`,
				);
			}
		} catch {
			/* swallowed (source bare except) */
		}
	}

	async markRead(chatId: string): Promise<boolean> {
		if (!this.privateApiSurfaceReady) return false;
		try {
			const guid = await this.resolveChatGuid(chatId);
			if (guid) {
				await this.restClient.post(
					`/api/v1/chat/${encodeURIComponent(guid)}/read`,
					{},
				);
				return true;
			}
		} catch {
			/* fallthrough (source bare except) */
		}
		return false;
	}

	// ── format_message (@~600 parity) ─────────────────────────────────────────

	formatMessage(content: string): string {
		return stripMarkdown(content);
	}

	// ── group mention gating (@~625-660 helpers parity) ──────────────────────

	messageMatchesMentionPatterns(text: string): boolean {
		if (!text || this.mentionPatterns.length === 0) return false;
		return this.mentionPatterns.some((p) => p.test(text));
	}

	/**
	 * _clean_mention_text: custom patterns are REGEXES, so strip only a LEADING
	 * match (anchored at start like Python .match) — ordinary words later in
	 * the prompt survive. An empty remainder keeps the ORIGINAL text.
	 */
	cleanMentionText(text: string): string {
		if (!text) return text;
		const stripped = pythonLstrip(text);
		for (const pattern of this.mentionPatterns) {
			const m = pattern.exec(stripped);
			if (m !== null && m.index === 0) {
				const cleaned = stripped.slice(m[0].length).replace(/^[ ,:-]+/, "");
				return cleaned.length > 0 ? cleaned : text;
			}
		}
		return text;
	}

	// ── webhook POST handler (_handle_webhook @~640 order parity) ────────────

	/**
	 * Order ports the source EXACTLY: auth-token gate (query password/guid then
	 * x-password/x-guid/x-bluebubbles-guid headers; Python `or` chain semantics
	 * — EMPTY carrier values fall through to the next position) → body cap →
	 * JSON-or-form parse → event filter (EMPTY event type FALLS THROUGH, only a
	 * PRESENT non-message type acks) → record extraction → isFromMe/tapback
	 * drops → text/chat/sender chains (chats[0].guid fallback for v1.9+
	 * payloads) → missing-fields 400 → group detection → mention gating →
	 * dispatch + fire-and-forget read receipt. Always answers.
	 */
	async handleWebhookPost(input: {
		query?: Record<string, string> | undefined;
		headers?: Record<string, string> | undefined;
		rawBody: Buffer;
	}): Promise<HandlerResponse> {
		// 1. Token gate — BEFORE any body read (parseInvocations observable 0).
		const query = input.query ?? {};
		const headers = normalizeHeaders(input.headers);
		const token = firstTruthy(
			query["password"],
			query["guid"],
			headers["x-password"],
			headers["x-guid"],
			headers["x-bluebubbles-guid"],
		);
		// DEC-017: constant-time compare against the configured password
		// (bbPasswordTokenCompare IS this wire's authenticity mechanism).
		if (token === undefined || !secureCompare(token, this.password)) {
			this.counters.unauthorized += 1;
			return jsonResponse({ error: "unauthorized" }, 401);
		}

		// Body cap (declared client_max_size parity): aiohttp raises during
		// request.read(), which the source's parse try/except swallows into the
		// SAME 400 invalid-payload verdict — enforced here BEFORE decode.
		if (input.rawBody.length > BB_WEBHOOK_MAX_BODY_BYTES) {
			this.counters.invalidPayload += 1;
			return jsonResponse({ error: "invalid payload" }, 400);
		}

		// 2. THE parse seam — the ONLY place a request body is decoded. JSON
		// first; on failure urllib.parse_qs form semantics (payload/data/message
		// keys). Malformed input is a RESULT, never a throw.
		this.counters.parseInvocations += 1;
		let payload: unknown;
		try {
			const body = input.rawBody.toString("utf8"); // errors='replace' parity
			try {
				payload = JSON.parse(body);
			} catch {
				const form = new URLSearchParams(body);
				const payloadStr = firstTruthy(
					form.get("payload"),
					form.get("data"),
					form.get("message"),
					"",
				);
				payload = payloadStr ? JSON.parse(payloadStr) : {};
			}
		} catch (exc) {
			this.counters.invalidPayload += 1;
			this.logger?.error?.(
				`[bluebubbles] webhook parse error: ${errorMessage(exc)}`,
			);
			return jsonResponse({ error: "invalid payload" }, 400);
		}

		// 3. Event-type filter: only message events carry users; everything else
		// is silently acknowledged. An ABSENT event type falls THROUGH (source:
		// `if event_type and event_type not in _MESSAGE_EVENTS`).
		const payloadRecord = asRecord(payload);
		const eventType =
			firstTruthy(
				stringOrNull(payloadRecord["type"]),
				stringOrNull(payloadRecord["event"]),
			) ?? "";
		if (eventType && !BB_MESSAGE_EVENTS.has(eventType)) {
			this.counters.eventFiltered += 1;
			return okText();
		}

		// 4. Payload record extraction (_extract_payload_record): data dict →
		// data list FIRST dict → payload.message dict → payload itself.
		const record = extractPayloadRecord(payloadRecord);

		// 5. Self-authored echo drop.
		if (record["isFromMe"] || record["fromMe"] || record["is_from_me"]) {
			this.counters.fromMeDropped += 1;
			return okText();
		}

		// 6. Tapback reactions delivered as messages are dropped.
		const assocType = record["associatedMessageType"];
		if (
			typeof assocType === "number" &&
			(assocType in BB_TAPBACK_ADDED || assocType in BB_TAPBACK_REMOVED)
		) {
			this.counters.tapbackDropped += 1;
			return okText();
		}

		// 7. Text extraction chain.
		const text =
			firstTruthy(
				stringOrNull(record["text"]),
				stringOrNull(record["message"]),
				stringOrNull(record["body"]),
			) ?? "";

		// 8. Chat-GUID chain — v1.9+ payloads omit top-level chatGuid; the chat
		// GUID hides under data.chats[0].guid instead.
		let chatGuid =
			firstTruthy(
				stringOrNull(record["chatGuid"]),
				stringOrNull(payloadRecord["chatGuid"]),
				stringOrNull(record["chat_guid"]),
				stringOrNull(payloadRecord["chat_guid"]),
				stringOrNull(payloadRecord["guid"]),
			) ?? "";
		if (!chatGuid) {
			const chats = record["chats"];
			const firstChat = Array.isArray(chats) ? asRecord(chats[0]) : {};
			chatGuid =
				stringOrNull(firstChat["guid"]) ??
				stringOrNull(firstChat["chatGuid"]) ??
				"";
		}
		const chatIdentifier =
			firstTruthy(
				stringOrNull(record["chatIdentifier"]),
				stringOrNull(record["identifier"]),
				stringOrNull(payloadRecord["chatIdentifier"]),
				stringOrNull(payloadRecord["identifier"]),
			) ?? "";

		// 9. Sender chain: handle.address dict → sender → from → address,
		// falling to the identifiers. A sender-only payload backfills
		// chat_identifier so the session still has a surface id.
		const handle = asRecord(record["handle"]);
		const sender =
			firstTruthy(
				stringOrNull(handle["address"]),
				stringOrNull(record["sender"]),
				stringOrNull(record["from"]),
				stringOrNull(record["address"]),
			) ??
			chatIdentifier ??
			chatGuid;
		// Sender-only payloads backfill chat_identifier so the session still has
		// an identifier-shaped surface id (source @~700 parity).
		let effectiveChatIdentifier = chatIdentifier;
		if (!chatGuid && !effectiveChatIdentifier && sender) {
			effectiveChatIdentifier = sender;
		}

		const sessionChatId = chatGuid || effectiveChatIdentifier;
		const isGroup = Boolean(record["isGroup"]) || chatGuid.includes(";+;");

		// Missing-field verdict BEFORE mention gating (source ordering): no
		// sender OR no chat surface OR no text ⇒ 400.
		if (!sender || !(chatGuid || effectiveChatIdentifier) || !text) {
			this.counters.missingFields += 1;
			return jsonResponse({ error: "missing message fields" }, 400);
		}

		// Group mention gating: require_mention + no wake word ⇒ silently
		// acknowledge-drop; a match strips only a LEADING occurrence.
		let dispatchText = text;
		if (isGroup && this.requireMention) {
			if (!this.messageMatchesMentionPatterns(text)) {
				this.counters.mentionDropped += 1;
				this.logger?.debug?.(
					"[bluebubbles] ignoring group message (require_mention=true, no mention pattern matched)",
				);
				return okText();
			}
			dispatchText = this.cleanMentionText(text);
		}

		// 11. Dispatch the incoming event (build_source parity).
		const messageId = firstTruthy(
			stringOrNull(record["guid"]),
			stringOrNull(record["messageGuid"]),
			stringOrNull(record["id"]),
		);
		const replyToMessageId = firstTruthy(
			stringOrNull(record["threadOriginatorGuid"]),
			stringOrNull(record["associatedMessageGuid"]),
		);
		// Built as a variable so adapter-extended slots (chat_id_alt) ride
		// structurally without tripping fresh-literal excess checks.
		const source = {
			platform: BLUEBUBBLES_PLUGIN_MANIFEST.name,
			chatType: isGroup ? "group" : "dm",
			userId: sender,
			chatId: sessionChatId,
			chatName: effectiveChatIdentifier || sender,
			// chat_id_alt parity: participant-preferred identifier slot.
			...(effectiveChatIdentifier
				? { userIdAlt: effectiveChatIdentifier }
				: {}),
			...(effectiveChatIdentifier
				? { chatIdAlt: effectiveChatIdentifier }
				: {}),
		};
		const event: IncomingEvent = {
			messageType: "text",
			text: dispatchText,
			...(messageId !== undefined ? { messageId } : {}),
			...(replyToMessageId !== undefined ? { replyToMessageId } : {}),
			source,
		};
		this.dispatchedEvents.push({
			messageId: event.messageId ?? "",
			text: dispatchText,
			source: event.source,
		});
		this.counters.dispatched += 1;
		const sessionKey = sessionChatId;
		try {
			await this.deliverInbound(event, sessionKey);
		} catch {
			/* containment parity: one poisoned payload never rejects the POST */
		}

		// 12. Fire-and-forget read receipt (mark_read self-gates private_api &&
		// helper_connected internally).
		if (this.sendReadReceipts && sessionChatId) {
			this.counters.readReceiptsRequested += 1;
			const task = this.markRead(sessionChatId);
			this.backgroundTasks.add(task);
			void task.finally(() => this.backgroundTasks.delete(task));
		}

		// Always 200 'ok'.
		return okText();
	}

	// ── guard wiring (reference-fixture inheritance) ──────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: BLUEBUBBLES_REGISTRY,
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

	// ── egress doors ────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * THE text-delivery pipeline (splitsLongMessages=True ⇒ the ADAPTER owns
	 * native splitting — slack-adapter deliverText precedent): Hermes bubble
	 * semantics split paragraphs on blank lines FIRST, then any paragraph still
	 * over THE ONE chat length policy chunks with the kit fence-carry scaffold,
	 * and every chunk rides the §10.1 ladder + §6.1 retry lanes.
	 */
	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		const policy = this.chatLengthPolicyForChat(chatId);
		// The §6.1 fallback envelope is pre-rendered — it rides verbatim as ONE
		// chunk so the original chunk bytes survive to the wire.
		const chunks = content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			? [content]
			: this.chunkIntoBubbles(content, policy);
		const results: SendResult[] = [];
		for (const chunk of chunks) {
			results.push(await this.deliverRestChunk(chatId, chunk, metadata));
		}
		return results;
	}

	/**
	 * send() paragraph semantics (@~455): each thought becomes its own bubble;
	 * blanks dropped; a content with only blank separators stays one piece.
	 */
	private chunkIntoBubbles(
		content: string,
		policy: ChatLengthPolicy,
	): string[] {
		const paragraphs = content
			.split(/\n\s*\n/)
			.map((p) => p.trim())
			.filter((p) => p.length > 0);
		const sources = paragraphs.length > 0 ? paragraphs : [content];
		const chunks: string[] = [];
		for (const para of sources) {
			if (policy.lenFn(para) <= policy.maxUnits) {
				chunks.push(para);
			} else {
				chunks.push(...chunkWithFenceCarry(para, policy).chunks);
			}
		}
		return chunks;
	}

	/** Per-chunk §10.1/§6.1 lanes (slack-adapter deliverSlackChunk parity). */
	private async deliverRestChunk(
		chatId: string,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const outcome = await this.ensureRestLadder(chatId).sendText(
			chunk,
			metadata,
		);
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
			// §6.1 ladder — timeouts NOT retried inside, retry_after honored.
			const retried = await sendWithRetry(
				chunk,
				metadata,
				(c, md) => this.wireSend(chatId, c, md),
				{ maxRetries: 2 },
			);
			if (retried.success) return retried;
			return this.wireSend(chatId, DELIVERY_FAILED_NOTICE, metadata);
		}
		if (failureClass === "formatting") {
			return this.wireSend(chatId, plainTextFallbackBody(chunk), metadata);
		}
		return outcome;
	}

	/** ONE session-scoped ladder — the rich latch persists across chunks. */
	private ensureRestLadder(chatId: string): FormattingLadder {
		if (this.restLadder === null) {
			this.restLadder = new FormattingLadder({
				tryRich: (c, md) => this.wireRich(c, md),
				sendConverted: (c, md) => this.wireSend(chatId, c, md),
				sendPlain: (c, md) => this.wireSend(chatId, c, md),
			});
		}
		this.restLadderChatId = chatId;
		return this.restLadder;
	}

	/**
	 * The door wraps ONE bubble of the REST engine: markdown strip → GUID
	 * resolve → POST /api/v1/message/text → mirror onto the capture wire so
	 * shared rows observe bubbles. The kit §6.1 plain-text fallback body is
	 * ALREADY plain text by construction and must reach the wire byte-exact
	 * (original bytes preserved), so fallback-prefixed bodies bypass the strip.
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		void this.restLadderChatId;
		const replyToRaw = metadata[REPLY_TO_METADATA_KEY];
		const replyTo = typeof replyToRaw === "string" ? replyToRaw : undefined;
		const rest = content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			? await this.postBubble(chatId, content, undefined)
			: await this.postStrippedBubble(chatId, content, replyTo);
		if (!rest.success || this.captureWire === undefined) return rest;
		return this.captureWire.transmitSend(chatId, content, metadata);
	}

	/** One REST bubble post WITHOUT markdown transformation (verbatim lane). */
	private postBubble(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		return this.postResolvedBubble(chatId, content, replyTo);
	}

	/** format_message parity lane: strip markdown, then post ONE bubble. */
	private async postStrippedBubble(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
	): Promise<SendResult> {
		const text = stripMarkdown(content);
		if (!text) {
			return { success: false, error: "BlueBubbles send requires text" };
		}
		return this.postResolvedBubble(chatId, text, replyTo);
	}

	/**
	 * Rich lane ABSENT on the real surface (plain-text platform): unless a
	 * capture wire scripted a rich probe, answer the capability-error shape
	 * WITHOUT burning a roundtrip (§10.1 latch path probes once then never
	 * again — webhook reference adapter parity).
	 */
	protected override async wireRich(
		content: string,
		_metadata?: Metadata,
	): Promise<SendResult> {
		void _metadata;
		if (
			this.captureWire === undefined ||
			!this.captureWire.hasRichScript("rich")
		) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return this.captureWire.transmitRich("__rich__", content);
	}
}

async function drainBackgroundTasks(
	adapter: BlueBubblesAdapter,
): Promise<void> {
	await Promise.allSettled([...adapter["backgroundTasks"]]);
}

/** Exposed for fixture teardown determinism (read receipts settle). */
export function settleBackgroundTasks(
	adapter: BlueBubblesAdapter,
): Promise<void> {
	return drainBackgroundTasks(adapter);
}

// ── module-level constants/helpers ───────────────────────────────────────────

/** Source DEFAULT_MENTION_PATTERNS compiled IGNORECASE (helpers parity). */
const BB_DEFAULT_MENTION_PATTERN_SOURCES: readonly string[] =
	BB_DEFAULT_MENTION_PATTERNS;

const REGISTER_WEBHOOK_EVENTS: readonly string[] = [
	"new-message",
	"updated-message",
];

const REQUIRE_MENTION_TRUTHY: ReadonlySet<string> = new Set([
	"true",
	"1",
	"yes",
	"on",
]);

/** Synthetic policy for the truncate engine (codepoints ≙ Python len). */
function truncatePolicy(maxUnits: number): ChatLengthPolicy {
	return {
		chatId: "__truncate__",
		unit: "chars",
		lenFn: codePointLen,
		maxUnits,
	};
}

/** _normalize_server_url: trim, http:// prefix when scheme missing, rstrip /. */
export function normalizeServerUrl(raw: string): string {
	const value = (raw ?? "").trim();
	if (!value) return "";
	if (!/^https?:\/\//i.test(value))
		return `http://${value}`.replace(/\/+$/, "");
	return value.replace(/\/+$/, "");
}

/** Leading-slash normalization (__init__ webhook_path parity). */
export function normalizeWebhookPath(path: string): string {
	const raw = String(path ?? "").trim();
	return raw.startsWith("/") ? raw : `/${raw}`;
}

/**
 * urllib.parse.quote(password, safe='') parity: encodeURIComponent leaves
 * !'()* unescaped — quote(safe='') percent-encodes everything except
 * unreserved characters.
 */
export function pyQuote(value: string): string {
	return encodeURIComponent(value).replace(
		/[!'()*]/g,
		(c) => `%${c.charCodeAt(0).toString(16).toUpperCase()}`,
	);
}

/**
 * helpers.compile_mention_patterns WAKEWORD-STYLE branch (photon/bluebubbles):
 * raw None ⇒ defaults; string ⇒ JSON list or comma/newline split; list ⇒
 * entries; scalar ⇒ wrapped. Entries str().strip(), empties skipped, invalid
 * regexes warn-and-skip; everything compiles IGNORECASE.
 */
export function compileMentionPatterns(
	raw: string | readonly string[] | null | undefined,
	defaults: readonly string[],
	logger?: StreamLogger | undefined,
): RegExp[] {
	let patterns: readonly unknown[];
	if (raw === null || raw === undefined) {
		patterns = defaults;
	} else if (typeof raw === "string") {
		const text = raw.trim();
		let loaded: unknown;
		try {
			loaded = text.length > 0 ? JSON.parse(text) : [];
		} catch {
			loaded = undefined;
		}
		patterns = Array.isArray(loaded)
			? loaded
			: text
					.split("\n")
					.flatMap((line) => line.split(","))
					.map((part) => part.trim());
	} else if (Array.isArray(raw)) {
		patterns = raw;
	} else {
		patterns = [raw];
	}
	const compiled: RegExp[] = [];
	for (const entry of patterns) {
		const text = String(entry).trim();
		if (!text) continue;
		try {
			compiled.push(new RegExp(text, "i"));
		} catch (exc) {
			logger?.warn?.(
				`[bluebubbles] Invalid mention pattern ${JSON.stringify(text)}: ${errorMessage(exc)}`,
			);
		}
	}
	return compiled;
}

function jsonResponse(
	body: Record<string, unknown>,
	status: number,
): HandlerResponse {
	return {
		status,
		contentType: "application/json",
		body: body as Record<string, never>,
	};
}

function okText(): HandlerResponse {
	return { status: 200, contentType: "text/plain", body: "ok" };
}

function normalizeHeaders(
	headers: Record<string, string> | undefined,
): Record<string, string> {
	const out: Record<string, string> = {};
	for (const [k, v] of Object.entries(headers ?? {})) out[k.toLowerCase()] = v;
	return out;
}

/** _extract_payload_record parity (dict → list-first-dict → message → self). */
export function extractPayloadRecord(
	payload: Record<string, unknown>,
): Record<string, unknown> {
	const data = payload["data"];
	if (asRecordOrNil(data) !== null)
		return asRecordOrNil(data) as Record<string, unknown>;
	if (Array.isArray(data)) {
		for (const item of data) {
			const rec = asRecordOrNil(item);
			if (rec !== null) return rec;
		}
	}
	const message = asRecordOrNil(payload["message"]);
	if (message !== null) return message;
	return payload;
}

function asRecordOrNil(v: unknown): Record<string, unknown> | null {
	return v !== null && typeof v === "object" && !Array.isArray(v)
		? (v as Record<string, unknown>)
		: null;
}

function asRecord(v: unknown): Record<string, unknown> {
	return asRecordOrNil(v) ?? {};
}

function stringOrEmpty(v: unknown): string {
	return typeof v === "string" ? v : "";
}

function stringOrNull(v: unknown): string | null {
	if (v === null || v === undefined) return null;
	if (typeof v !== "string") return null;
	const text = v.trim();
	return text.length > 0 ? text : null;
}

/** Python `or`-chain parity: first TRUTHY candidate (null/empty/0 skipped). */
function firstTruthy<T>(
	...candidates: Array<T | null | undefined>
): T | undefined {
	for (const c of candidates) {
		if (c !== undefined && c !== null && c !== "" && c !== 0 && c !== false) {
			return c;
		}
	}
	return undefined;
}

function parseEnvNumber(raw: string | undefined): number | undefined {
	if (raw === undefined || raw.trim() === "") return undefined;
	const n = Number(raw);
	return Number.isFinite(n) ? n : undefined;
}

function errorMessage(exc: unknown): string {
	return exc instanceof Error ? exc.message : String(exc);
}

/** Python str.lstrip() (whitespace) parity — used by _clean_mention_text. */
function pythonLstrip(text: string): string {
	return text.replace(/^\s+/, "");
}
