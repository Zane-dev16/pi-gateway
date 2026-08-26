// pi_platforms/photon/photon-adapter — THE Photon Spectrum (iMessage) adapter,
// ported from the READ-ONLY Hermes plugin
// plugins/platforms/photon/adapter.py onto the kit base. Everything
// policy-shaped is inherited; this module supplies TRANSPORT (the injected
// sidecar control-plane seam + inbound NDJSON line semantics) and MANIFEST
// DATA.
//
// Shape (DEC-002 persistent-ws family — long-lived sidecar stream):
//   - capabilities AS DATA: supportsAsyncDelivery TRUE (sidecar pushes inbound
//     over gRPC; DEC-022 wakeLane ⇒ forged-event), interactiveResume FALSE per
//     SUPPORTS_MESSAGE_EDITING=False edit reality (manifest DIVERGENCE note)
//   - supportsDraftStreaming stays FALSE by METHOD probe (no draft lanes on
//     iMessage); edit/draft attempts return "Not supported" with ZERO sidecar
//     calls (static capability, never probed on the wire)
//   - inbound rides _on_inbound_line semantics: every line (message OR
//     heartbeat) notes upstream activity; JSON lines dedupe by messageId then
//     dispatch (_dispatch_inbound @~1222)
//   - outbound is /send {spaceId, text, format?} with the markdown kill-switch
//     dual path, URL-only rich-link routing, and the 8000-char budget; typing,
//     reactions, polls, effects ride sibling loopback endpoints
//     (adapter.py:send_effect → /send-effect {spaceId,text,effect})
//   - THE presence watchdog ports _probe_once/_presence_watchdog decision
//     logic as STEPWISE methods (injected clock, no internal timers): alive /
//     hung / inconclusive tri-state, N-consecutive-hung ⇒ EXACTLY ONE respawn
//     signal through an injected callback seam
//
// Layering: imports pi_gateway downward + kit same-layer ONLY; no adapter
// cross-imports.

import {
	BasePlatformAdapter,
	resolveEnablement,
	stripMarkdownMarkup,
	TokenLockManagerSeam,
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
import type { ScopedSecretReader } from "../kit/registration.js";
import type { DisableReason } from "../kit/lifecycle-state.js";

import {
	PHOTON_DEFAULT_MENTION_PATTERN_SOURCES,
	PHOTON_DEFAULT_SIDECAR_PORT,
	PHOTON_LAST_INBOUND_CHATS_MAX,
	PHOTON_MAX_MESSAGE_LENGTH,
	PHOTON_PLUGIN_MANIFEST,
	PHOTON_PROBE_INTERVAL_MS,
	PHOTON_PROBE_MAX_FAILURES,
	PHOTON_PROBE_TIMEOUT_MS,
	PHOTON_RETRYABLE_PATTERNS,
	PHOTON_RICHLINK_PREVIEW_ATTACHMENT_SUFFIX,
	PHOTON_RICHLINK_PREVIEW_SUPPRESS_MS,
	PHOTON_SENT_IDS_MAX,
	PHOTON_TARGET_NOT_ALLOWED_MESSAGE,
	PHOTON_TYPING_COOLDOWN_MS,
	normalizeChatKey,
	validatePhotonTrustBoundary,
} from "./manifest.js";
import type { PhotonTrustBoundary } from "./manifest.js";
import { declarePhotonTrustBoundary } from "./manifest.js";
import {
	SidecarHttpError,
	SidecarHungError,
	type SidecarTransport,
} from "./sidecar-wire.js";
import { DedupeWindow } from "./dedupe.js";

/** msgraph/raft-parity HTTP-handler response shape (unused lanes omitted). */
export interface HandlerResponse {
	status: number;
	body?: Record<string, unknown> | undefined;
}

export interface PhotonCaptureWire {
	transmitSend(
		chatId: string,
		content: string,
		metadata: Record<string, unknown>,
	): Promise<SendResult>;
	hasRichScript(opKind: string): boolean;
	transmitRich(chatId: string, content: string): Promise<SendResult>;
}

export type ProcessingOutcome = "success" | "failure" | "cancelled";

export type ProbeVerdict = "alive" | "hung" | "inconclusive";

/**
 * SendResult + the STRUCTURED sidecar failure classification
 * (base.py:SendResult.raw_response parity — error_class/retryable travel with
 * the result so callers never string-sniff for permanent-vs-retryable).
 */
export interface PhotonSendResult extends SendResult {
	rawResponse?: Record<string, unknown> | undefined;
}

export type WatchdogTickVerdict =
	| "disabled"
	| "skipped-idle"
	| ProbeVerdict
	| "respawned";

export interface PhotonAdapterOptions {
	/** PlatformConfig.extra parity (config.yaml keys win over env). */
	config?: Record<string, unknown> | undefined;
	/** Injected env reader — NEVER process.env in conformance runs. */
	envReader?: ScopedSecretReader | undefined;
	/** THE sidecar control plane (loopback HTTP in production). */
	sidecar: SidecarTransport;
	/** Injected monotonic clock (ms) — cooldowns, dedupe TTL, watchdog idle. */
	nowMs?: (() => number) | undefined;
	captureWire?: PhotonCaptureWire | undefined;
	/** Harness-scale chunk budget override; production default 8000 chars. */
	scalarMaxUnits?: number | undefined;
	/**
	 * Respawn callback seam — invoked EXACTLY ONCE per threshold crossing.
	 * Production would tear down + restart the Node child (documented
	 * exclusion); fixtures record the signal.
	 */
	onRespawn?: ((reason: string) => void | Promise<void>) | undefined;
	/** Fatal-notification seam (gateway handoff in production). */
	notifyFatalError?: (() => Promise<void>) | undefined;
	/** Retry-ladder sleep injection (latency-free rows). */
	sleepFn?: ((ms: number) => Promise<void>) | undefined;
}

// ── mention machinery (adapter.py:_compile_mention_patterns @~866) ──────────

function compilePattern(source: string): RegExp | null {
	try {
		return new RegExp(source);
	} catch {
		return null; // invalid regex SKIPPED — good ones stay live
	}
}

/**
 * adapter.py:_compile_mention_patterns / compile_mention_patterns: raw is a
 * list (config), a string (env: JSON list or comma/newline-separated), or
 * undefined (Hermes defaults). Invalid patterns drop SILENTLY keeping good
 * ones (test_invalid_pattern_skipped).
 */
export function compileMentionPatterns(
	raw: unknown,
	defaults: readonly string[] = PHOTON_DEFAULT_MENTION_PATTERN_SOURCES,
): RegExp[] {
	let sources: readonly string[];
	if (raw === undefined || raw === null || raw === "") {
		sources = defaults;
	} else if (Array.isArray(raw)) {
		sources = raw.map(String);
	} else if (typeof raw === "string") {
		const trimmed = raw.trim();
		if (trimmed.startsWith("[")) {
			try {
				const parsed: unknown = JSON.parse(trimmed);
				sources = Array.isArray(parsed) ? parsed.map(String) : defaults;
			} catch {
				sources = defaults;
			}
		} else {
			sources = trimmed
				.split(/[\n,]+/)
				.map((s) => s.trim())
				.filter((s) => s.length > 0);
		}
	} else {
		sources = defaults;
	}
	const out: RegExp[] = [];
	for (const source of sources) {
		if (!source) continue;
		const compiled = compilePattern(source);
		if (compiled !== null) out.push(compiled);
	}
	return out;
}

/** adapter.py:_message_matches_mention_patterns — any pattern searches. */
export function matchesMentionPatterns(
	patterns: readonly RegExp[],
	text: string,
): boolean {
	if (!text || patterns.length === 0) return false;
	return patterns.some((p) => p.test(text));
}

/**
 * adapter.py:_clean_mention_text — strip a LEADING wake word only (custom
 * patterns are regexes; stripping mid-text would delete ordinary words).
 * Python re.match anchors at start → JS exec at index 0.
 */
export function cleanMentionText(
	patterns: readonly RegExp[],
	text: string,
): string {
	if (!text) return text;
	for (const pattern of patterns) {
		pattern.lastIndex = 0;
		const match = pattern.exec(text.replace(/^\s+/, ""));
		if (match && match.index === 0) {
			const cleaned = text
				.replace(/^\s+/, "")
				.slice(match[0].length)
				.replace(/^[ ,:-]+/, "");
			return cleaned || text;
		}
	}
	return text;
}

// ── rich-link helpers (adapter.py @~599-700) ────────────────────────────────

const URL_ONLY_RE = /^https?:\/\/\S+$/i;

/** adapter.py:_url_only_candidate — WHOLE message is one http(s) URL. */
export function urlOnlyCandidate(text: string): string | null {
	const candidate = (text ?? "").trim();
	if (!URL_ONLY_RE.test(candidate)) return null;
	try {
		const parsed = new URL(candidate);
		if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
			return null;
		}
		if (parsed.hostname.length === 0) return null;
	} catch {
		return null;
	}
	return candidate;
}

/**
 * adapter.py:_richlink_url_from_content — candidate extraction RECURSES into
 * group items: a text item folds its whole-text candidate, a richlink item its
 * url, so MULTI-ITEM group messages arm the 30s preview-suppression window
 * from their embedded URL (adapter.py:_record_recent_richline call site @1464).
 */
export function richlinkUrlFromContent(
	content: Record<string, unknown>,
): string | null {
	const ctype = content["type"];
	if (ctype === "text") {
		return urlOnlyCandidate(String(content["text"] ?? ""));
	}
	if (ctype === "richlink") {
		return urlOnlyCandidate(String(content["url"] ?? ""));
	}
	if (ctype === "group") {
		const items = Array.isArray(content["items"]) ? content["items"] : [];
		for (const item of items) {
			if (item === null || typeof item !== "object" || Array.isArray(item)) {
				continue;
			}
			const itemContent = (item as Record<string, unknown>)["content"];
			if (
				itemContent === null ||
				typeof itemContent !== "object" ||
				Array.isArray(itemContent)
			) {
				continue;
			}
			const url = richlinkUrlFromContent(
				itemContent as Record<string, unknown>,
			);
			if (url !== null) return url;
		}
	}
	return null;
}

/**
 * adapter.py:_richlink_candidate — intentionally narrow: only exact http(s)
 * URL messages become rich links; prose containing URLs stays on the normal
 * markdown/text path. Markdown OFF disables the lane entirely.
 */
export function richlinkCandidate(
	text: string,
	markdownEnabled: boolean,
): string | null {
	if (!markdownEnabled) return null;
	return urlOnlyCandidate(text);
}

/** adapter.py:_format_richlink_content — title/summary/url line join. */
export function formatRichlinkContent(
	content: Record<string, unknown>,
): string {
	const url = String(content["url"] ?? "").trim();
	const title = String(content["title"] ?? "").trim();
	const summary = String(content["summary"] ?? "").trim();
	const parts: string[] = [];
	if (title) parts.push(title);
	if (summary && summary !== title) parts.push(summary);
	if (url) parts.push(url);
	return parts.length > 0
		? parts.join("\n")
		: "[Photon rich link received with no URL]";
}

/** adapter.py:_is_richlink_preview_attachment — OpenGraph art marker. */
export function isRichlinkPreviewAttachment(
	payload: Record<string, unknown>,
): boolean {
	if (payload["type"] !== "attachment") return false;
	const name = String(payload["name"] ?? "").toLowerCase();
	const attachmentId = String(payload["id"] ?? "").toLowerCase();
	const marker = PHOTON_RICHLINK_PREVIEW_ATTACHMENT_SUFFIX;
	return (
		name.endsWith(marker) ||
		attachmentId.endsWith(marker) ||
		name.includes(marker) ||
		attachmentId.includes(marker)
	);
}

/** adapter.py:_is_richlink_preview_content — attachment or whole-group check. */
export function isRichlinkPreviewContent(
	content: Record<string, unknown>,
): boolean {
	if (isRichlinkPreviewAttachment(content)) return true;
	if (content["type"] !== "group") return false;
	const items = content["items"];
	if (!Array.isArray(items) || items.length === 0) return false;
	for (const item of items) {
		if (item === null || typeof item !== "object" || Array.isArray(item)) {
			return false;
		}
		const itemContent = (item as Record<string, unknown>)["content"];
		if (
			itemContent === null ||
			typeof itemContent !== "object" ||
			Array.isArray(itemContent)
		) {
			return false;
		}
		if (!isRichlinkPreviewAttachment(itemContent as Record<string, unknown>)) {
			return false;
		}
	}
	return true;
}

/** Codepoint length (Python len() parity — the truncation budget unit). */
function codePointLen(text: string): number {
	let n = 0;
	for (const _ of text) n += 1;
	return n;
}

function truthyEnv(value: string | undefined): boolean {
	return ["true", "1", "yes", "on"].includes(
		(value ?? "").trim().toLowerCase(),
	);
}

function coerceNumber(value: unknown, fallback: number): number {
	const n = Number(value);
	return Number.isFinite(n) ? n : fallback;
}

/** adapter.py:_first_set — first non-undefined/non-null value ("or"-safe). */
function firstSet(...values: Array<unknown>): unknown {
	for (const v of values) {
		if (v !== undefined && v !== null) return v;
	}
	return undefined;
}

/** Reference-fixture registry (07 §1 derivation — mirrors the shared set). */
const PHOTON_REGISTRY: CommandRegistry = [
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

export class PhotonAdapter extends BasePlatformAdapter {
	readonly pluginManifest = PHOTON_PLUGIN_MANIFEST;
	readonly trustBoundary: PhotonTrustBoundary = declarePhotonTrustBoundary();

	// ── config (__init__ parity) ──────────────────────────────────────────────
	readonly sidecarPort: number;
	readonly requireMention: boolean;
	readonly mentionPatterns: readonly RegExp[];
	private readonly envReader: ScopedSecretReader;
	private readonly extra: Record<string, unknown>;
	private projectId: string;
	private projectSecret: string;

	// ── injected seams ────────────────────────────────────────────────────────
	private readonly transport: SidecarTransport;
	private readonly nowFn: () => number;
	private readonly captureWire: PhotonCaptureWire | undefined;
	private readonly onRespawn:
		| ((reason: string) => void | Promise<void>)
		| undefined;
	private readonly notifyFatalErrorSeam: () => Promise<void>;
	private readonly sleepFn: (ms: number) => Promise<void>;

	// ── watchdog config (@~762-790) ───────────────────────────────────────────
	readonly probeIntervalMs: number;
	readonly probeTimeoutMs: number;
	readonly probeMaxFailures: number;
	/** Non-positive interval disables the watchdog entirely (escape hatch). */
	readonly probeEnabled: boolean;

	// ── runtime state ─────────────────────────────────────────────────────────
	readonly seenMessages: DedupeWindow;
	private readonly sentMessageIds = new Map<string, number>();
	private readonly lastInboundByChat = new Map<string, string>();
	private readonly recentRichlinksByChat = new Map<string, number>();
	private readonly typingLastSent = new Map<string, number>();
	private readonly pendingFffc = new Map<string, number>();

	private probeFailures = 0;
	private lastUpstreamActivityAt = 0;
	private respawnInFlight = false;

	// allowlist gate (PHOTON_ALLOWED_USERS csv / ALLOW_ALL_USERS dev flag)
	private readonly allowedUsers: ReadonlySet<string>;
	private readonly allowAllUsers: boolean;

	// fatal-error classification (base.py:_set_fatal_error observable fields)
	fatalErrorCode: string | null = null;
	fatalErrorRetryable = true;
	fatalErrorMessage = "";
	get hasFatalError(): boolean {
		return this.fatalErrorCode !== null;
	}

	// observability
	readonly dispatchedEvents: IncomingEvent[] = [];
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly respawnSignals: string[] = [];
	readonly fatalNotificationsDispatched: string[] = [];
	readonly fatalNotificationsCompleted: string[] = [];
	readonly fatalNotificationWarnings: string[] = [];
	private connectedOnce = false;
	private handlerAttached = false;
	private holding = false;
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};
	private readonly clarifyArmedSet = new Set<string>();
	private allowAllClickers = true;
	private readonly cp: EgressChokepoint;

	constructor(opts: PhotonAdapterOptions) {
		super({
			manifestName: PHOTON_PLUGIN_MANIFEST.name,
			capabilities: PHOTON_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? PHOTON_MAX_MESSAGE_LENGTH,
		});
		this.extra = opts.config ?? {};
		this.envReader = opts.envReader ?? ((name) => process.env[name]);
		this.transport = opts.sidecar;
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.captureWire = opts.captureWire;
		this.onRespawn = opts.onRespawn;
		this.notifyFatalErrorSeam = opts.notifyFatalError ?? (async () => {});
		this.sleepFn =
			opts.sleepFn ??
			((ms) => new Promise<void>((resolve) => setTimeout(resolve, ms)));

		// Project credentials: env wins, then config.extra (auth.json storage is
		// part of the excluded CLI setup machinery). The RAW optional values
		// drive enablement (undefined = missing ⇒ loud-disable); the string
		// fields blank-coalesce for connect()'s credential guard.
		const projectIdRaw = this.readCredential("PHOTON_PROJECT_ID", "project_id");
		const projectSecretRaw = this.readCredential(
			"PHOTON_PROJECT_SECRET",
			"project_secret",
		);
		this.projectId = projectIdRaw ?? "";
		this.projectSecret = projectSecretRaw ?? "";

		this.sidecarPort = Math.trunc(
			coerceNumber(
				firstSet(this.extra["sidecar_port"], this.env("PHOTON_SIDECAR_PORT")),
				PHOTON_DEFAULT_SIDECAR_PORT,
			),
		);

		// Group-chat mention gating (BlueBubbles parity). Config key wins, env
		// next; DEFAULT FALSE (test_require_mention_defaults_off).
		const requireMentionRaw = firstSet(
			this.extra["require_mention"],
			this.env("PHOTON_REQUIRE_MENTION"),
		);
		this.requireMention =
			typeof requireMentionRaw === "boolean"
				? requireMentionRaw
				: truthyEnv(requireMentionRaw as string | undefined);
		this.mentionPatterns = compileMentionPatterns(
			"mention_patterns" in this.extra
				? this.extra["mention_patterns"]
				: this.env("PHOTON_MENTION_PATTERNS"),
		);

		// Presence watchdog thresholds (explicit values honored via _first_set
		// so an explicit 0 can disable — ``0 or X`` would silently fall through).
		this.probeIntervalMs =
			coerceNumber(
				firstSet(
					this.extra["probe_interval_seconds"],
					this.env("PHOTON_PROBE_INTERVAL_SECONDS"),
				),
				PHOTON_PROBE_INTERVAL_MS / 1000,
			) * 1000;
		this.probeTimeoutMs =
			coerceNumber(
				firstSet(
					this.extra["probe_timeout_seconds"],
					this.env("PHOTON_PROBE_TIMEOUT_SECONDS"),
				),
				PHOTON_PROBE_TIMEOUT_MS / 1000,
			) * 1000;
		this.probeMaxFailures = Math.trunc(
			coerceNumber(
				firstSet(
					this.extra["probe_max_failures"],
					this.env("PHOTON_PROBE_MAX_FAILURES"),
				),
				PHOTON_PROBE_MAX_FAILURES,
			),
		);
		this.probeEnabled = this.probeIntervalMs > 0;

		// Allowlist gate data (register(): allowed_users_env/allow_all_env).
		const allowedCsv = this.env("PHOTON_ALLOWED_USERS") ?? "";
		this.allowedUsers = new Set(
			allowedCsv
				.split(",")
				.map((entry) => entry.trim())
				.filter((entry) => entry.length > 0),
		);
		this.allowAllUsers = truthyEnv(this.env("PHOTON_ALLOW_ALL_USERS"));

		// At-least-once gRPC replay window (48h / 4000 entries, injected clock).
		this.seenMessages = new DedupeWindow({ nowMs: this.nowFn });

		// §11 step 3/4: missing (undefined) required secret ⇒ LOUD disable.
		// Present-but-EMPTY credentials stay enabled here so connect()'s own
		// credential guard can fire the MISSING_CREDENTIALS fatal instead —
		// matching Hermes' two-layer split (registration vs connect).
		const enablement = resolveEnablement(PHOTON_PLUGIN_MANIFEST, (name) => {
			if (name === "PHOTON_PROJECT_ID") return projectIdRaw;
			if (name === "PHOTON_PROJECT_SECRET") return projectSecretRaw;
			return this.env(name);
		});
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		// DEC-017: an incomplete trust boundary is a CONSTRUCTION-TIME error.
		const boundaryErrors = validatePhotonTrustBoundary(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			const reason: DisableReason = {
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			};
			this.lifecycle.disable(reason);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // no native stream lanes (iMessage)
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			// SUPPORTS_MESSAGE_EDITING=False ported as DATA: edits NEVER touch
			// the wire (zero sidecar calls, static capability).
			transmitEdit: async () => ({ success: false, error: "Not supported" }),
			transmitSeal: async () => ({ success: false, error: "Not supported" }),
		});
	}

	/**
	 * Credential resolution with THREE-state presence: env value (may be ""),
	 * extra value, or UNDEFINED when the key is absent everywhere. Undefined
	 * is what drives the §11 loud-disable; empty string falls through to
	 * connect()'s MISSING_CREDENTIALS fatal (Hermes' two-layer split).
	 */
	private readCredential(envKey: string, extraKey: string): string | undefined {
		const fromEnv = this.env(envKey);
		if (fromEnv !== undefined) return fromEnv;
		const fromExtra = this.extra[extraKey];
		if (typeof fromExtra === "string") return fromExtra;
		return undefined;
	}

	private env(name: string): string | undefined {
		return this.envReader(name);
	}

	// ── feature gates (env kill-switches, __init__/_markdown_enabled parity) ──

	/** adapter.py:_markdown_enabled — default TRUE; false|0|no kills it. */
	markdownEnabled(): boolean {
		const raw = (this.env("PHOTON_MARKDOWN") ?? "true").trim().toLowerCase();
		return !["false", "0", "no"].includes(raw);
	}

	/** supports_code_blocks mirrors the markdown kill-switch (test_markdown). */
	get supportsCodeBlocks(): boolean {
		return this.markdownEnabled();
	}

	/** adapter.py:_reactions_enabled — default FALSE (personal channel noise). */
	reactionsEnabled(): boolean {
		return truthyEnv(this.env("PHOTON_REACTIONS"));
	}

	// ── format_message (@~2381) — THE markdown dual path ─────────────────────

	/**
	 * Markdown passes through VERBATIM (sidecar sends it via spectrum-ts'
	 * markdown() builder; iMessage renders it). PHOTON_MARKDOWN=false reverts
	 * to the stripped plain-text path (shared strip_markdown equivalent).
	 */
	formatMessage(content: string): string {
		if (this.markdownEnabled()) return content;
		return stripMarkdownMarkup(content);
	}

	// ── connection lifecycle (connect @~891) ─────────────────────────────────

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (!this.projectId || !this.projectSecret) {
			this.setFatalError(
				"MISSING_CREDENTIALS",
				"PHOTON_PROJECT_ID and PHOTON_PROJECT_SECRET are required",
				false,
			);
			return false;
		}
		// Sidecar readiness ping (autostart/_start_sidecar healthz gate parity;
		// the spawn itself is a documented exclusion — the seam must answer).
		// Headers-only POST (adapter.py:_start_sidecar wait @~1720).
		try {
			await this.transport.call("/healthz");
		} catch (err) {
			this.setFatalError(
				"SIDECAR_FAILED",
				`failed to reach Photon sidecar /healthz: ${err instanceof Error ? err.message : String(err)}`,
				true,
			);
			return false;
		}
		this.connectedOnce = true;
		// Fresh session: give the watchdog a full interval before probing.
		this.noteUpstreamActivity();
		return true;
	}

	override async disconnect(): Promise<void> {
		this.connectedOnce = false;
		// Cancel any pending U+FFFC placeholder markers (disconnect parity).
		this.pendingFffc.clear();
	}

	get isConnected(): boolean {
		return this.connectedOnce;
	}

	// ── guard wiring (reference-fixture inheritance) ─────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.handlerAttached = true;
		this.attachGuard(
			{
				registry: PHOTON_REGISTRY,
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
					void ctx;
					return `reply:${text}`;
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
		// Self/echo filter parity (shared row contract).
		if (String(event.source?.userId ?? "") === "bot-self") return;
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		if (!this.handlerAttached) return; // handle_message capture parity
		await this.handleIngress(event, sessionKey);
	}

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	// ── inbound stream consumer (adapter.py @~1063/@~1146) ───────────────────

	/**
	 * _on_inbound_line: ANY line — message or heartbeat — proves the upstream
	 * channel live (note_upstream_activity FIRST), then JSON parse (non-JSON
	 * skipped), messageId dedupe, dispatch.
	 */
	async onInboundLine(line: string): Promise<void> {
		this.noteUpstreamActivity();
		let event: unknown;
		try {
			event = JSON.parse(line);
		} catch {
			return; // "[photon] skipping non-JSON inbound line"
		}
		const msgId =
			event !== null && typeof event === "object"
				? (event as Record<string, unknown>)["messageId"]
				: undefined;
		if (typeof msgId === "string" && msgId.length > 0) {
			if (this.seenMessages.isDuplicate(msgId)) return;
		}
		await this.dispatchInbound(event as Record<string, unknown>);
	}

	// ── THE dispatch pipeline (_dispatch_inbound @~1222) ─────────────────────

	async dispatchInbound(event: Record<string, unknown>): Promise<void> {
		const space = this.objectOf(event["space"]);
		const sender = this.objectOf(event["sender"]);
		const content = this.objectOf(event["content"]);

		const spaceId = typeof space["id"] === "string" ? space["id"] : "";
		if (!spaceId) return; // "[photon] inbound missing space.id"

		const chatType = space["type"] === "group" ? "group" : "dm";
		const senderId =
			(typeof sender["id"] === "string" && sender["id"]) ||
			(typeof space["phone"] === "string" && space["phone"]) ||
			spaceId;

		// Reaction routing BEFORE everything else: a tapback never carries a
		// wake word and only tapbacks on OUR messages are addressed to us.
		if (content["type"] === "reaction") {
			await this.dispatchReaction(
				event,
				space,
				content,
				spaceId,
				chatType,
				String(senderId),
			);
			return;
		}

		// U+FFFC placeholder — register the wait marker instead of dispatching
		// (timer task excluded; registration/cancel semantics kept).
		if (
			content["type"] === "text" &&
			String(content["text"] ?? "").trim() === "\uFFFC"
		) {
			this.pendingFffc.set(spaceId, this.nowFn());
			return;
		}
		// The real attachment arrived — cancel the pending placeholder.
		if (content["type"] === "attachment" || content["type"] === "voice") {
			this.pendingFffc.delete(spaceId);
		}

		// Preview art for an immediately preceding URL must not become a second
		// user prompt — suppress BEFORE recording last-inbound.
		if (this.isRecentRichlinkPreview(spaceId, content)) {
			return;
		}

		// Anything past here is a real (reactable) message — recorded BEFORE the
		// mention gate (a reaction to a non-wake-word group message is valid).
		this.recordLastInbound(
			spaceId,
			typeof event["messageId"] === "string" ? event["messageId"] : undefined,
		);

		let text: string;
		let messageType: IncomingEvent["messageType"] = "text";
		let mediaTypes: string[] = [];

		if (content["type"] === "poll_option") {
			// A native poll vote. A selection forwards the chosen option TEXT (the
			// gateway clarify-intercept resolves it); a deselection drops.
			if (content["selected"] === false) return;
			const choice = String(content["title"] ?? "").trim();
			if (!choice) return;
			text = choice;
		} else if (content["type"] === "text") {
			text = String(content["text"] ?? "");
		} else if (
			content["type"] === "attachment" ||
			content["type"] === "voice"
		) {
			// Byte caching to disk is a documented exclusion — the metadata-only
			// marker branch of _normalize_binary_payload is preserved verbatim.
			const normalized = normalizeBinaryPayload(content);
			text = normalized.text;
			messageType = normalized.messageType;
			mediaTypes = normalized.mediaTypes;
		} else if (content["type"] === "richlink") {
			text = formatRichlinkContent(content);
		} else if (content["type"] === "group") {
			const joined = this.formatGroupContent(content);
			text = joined.text;
			messageType = joined.messageType;
			mediaTypes = joined.mediaTypes;
		} else {
			text = `[Photon content type not handled: ${String(content["type"] ?? "")}]`;
		}

		// Allowlist gate FIRST (mission pipeline order; E.164 identity compare;
		// gateway authz owns the default-deny posture — see proposed DEC in the
		// port report).
		if (!this.isSenderAllowed(String(senderId))) return;

		// Group-mention gating (BlueBubbles parity): group chats with
		// require_mention enabled DROP messages without a wake word and STRIP
		// the leading wake word from ones that pass. DMs are NEVER gated.
		if (chatType === "group" && this.requireMention) {
			if (!matchesMentionPatterns(this.mentionPatterns, text)) {
				return;
			}
			text = cleanMentionText(this.mentionPatterns, text);
		}

		// adapter.py @1464: the candidate comes from _richlink_url_from_content
		// (RECURSES into group items) or falls back to the whole text.
		this.recordRecentRichlink(spaceId, richlinkUrlFromContent(content) ?? text);

		const outgoing: IncomingEvent = {
			messageType,
			text,
			source: {
				platform: PHOTON_PLUGIN_MANIFEST.name,
				chatType,
				userId: String(senderId),
				chatId: spaceId,
				chatName: spaceId,
			},
			...(typeof event["messageId"] === "string"
				? { messageId: event["messageId"] as string }
				: {}),
			...(mediaTypes.length > 0 ? { mediaTypes } : {}),
			metadata: {
				photon_timestamp:
					typeof event["timestamp"] === "string" ? event["timestamp"] : null,
				photon_space_phone:
					typeof space["phone"] === "string" ? space["phone"] : null,
			},
		};
		this.dispatchedEvents.push(outgoing);
		await this.deliverInbound(outgoing, normalizeChatKey(spaceId));
	}

	private objectOf(value: unknown): Record<string, unknown> {
		return value !== null && typeof value === "object" && !Array.isArray(value)
			? (value as Record<string, unknown>)
			: {};
	}

	/**
	 * Reaction routing (adapter.py @~1310): route ONLY tapbacks on messages WE
	 * sent — those are implicitly addressed to the bot. Correlate the target so
	 * the gateway injects the reply pointer; reply_to_text hydrates from the
	 * sidecar (null when the target carried no text).
	 */
	private async dispatchReaction(
		event: Record<string, unknown>,
		_space: Record<string, unknown>,
		content: Record<string, unknown>,
		spaceId: string,
		chatType: string,
		senderId: string,
	): Promise<void> {
		const targetId = content["targetMessageId"];
		const isOurs =
			content["targetDirection"] === "outbound" ||
			(typeof targetId === "string" &&
				targetId.length > 0 &&
				this.sentMessageIds.has(targetId));
		if (!isOurs) return; // human↔human tapbacks are not for us

		const emoji = String(content["emoji"] ?? "");
		const targetText = content["targetText"];
		const outgoing: IncomingEvent = {
			messageType: "text",
			text: `reaction:added:${emoji}`,
			source: {
				platform: PHOTON_PLUGIN_MANIFEST.name,
				chatType,
				userId: senderId,
				chatId: spaceId,
				chatName: spaceId,
			},
			...(typeof event["messageId"] === "string"
				? { messageId: event["messageId"] as string }
				: {}),
			...(typeof targetId === "string" ? { replyToMessageId: targetId } : {}),
			metadata: {
				reply_to_text: typeof targetText === "string" ? targetText : null,
				// is_ours guarantees the target is one of our messages.
				reply_to_is_own_message: true,
				photon_timestamp:
					typeof event["timestamp"] === "string" ? event["timestamp"] : null,
			},
		};
		this.dispatchedEvents.push(outgoing);
		await this.deliverInbound(outgoing, normalizeChatKey(spaceId));
	}

	/** adapter.py:_normalize_chat_key trackers — bounded insertion-order maps. */

	private recordLastInbound(
		chatId: string | undefined,
		messageId?: string,
	): void {
		if (!chatId || !messageId) return;
		const key = normalizeChatKey(chatId);
		if (this.lastInboundByChat.has(key)) this.lastInboundByChat.delete(key);
		this.lastInboundByChat.set(key, messageId);
		if (this.lastInboundByChat.size > PHOTON_LAST_INBOUND_CHATS_MAX) {
			const oldest = this.lastInboundByChat.keys().next();
			if (!oldest.done) this.lastInboundByChat.delete(oldest.value);
		}
	}

	lastInboundMessageId(chatId: string): string | undefined {
		return this.lastInboundByChat.get(normalizeChatKey(chatId));
	}

	recordSentMessage(messageId: string | undefined | null): void {
		if (!messageId) return;
		if (this.sentMessageIds.has(messageId))
			this.sentMessageIds.delete(messageId);
		this.sentMessageIds.set(messageId, this.nowFn());
		if (this.sentMessageIds.size > PHOTON_SENT_IDS_MAX) {
			const oldest = this.sentMessageIds.keys().next();
			if (!oldest.done) this.sentMessageIds.delete(oldest.value);
		}
	}

	hasSentMessageId(messageId: string): boolean {
		return this.sentMessageIds.has(messageId);
	}

	/**
	 * adapter.py:_record_recent_richlink — arms the 30s preview-suppression
	 * window for the chat when the (already-extracted) candidate is a url-only
	 * candidate; bounded insertion-order eviction under the injected clock.
	 */
	private recordRecentRichlink(chatId: string, candidateText: string): void {
		if (!chatId || !urlOnlyCandidate(candidateText)) return;
		const key = normalizeChatKey(chatId);
		if (this.recentRichlinksByChat.has(key))
			this.recentRichlinksByChat.delete(key);
		this.recentRichlinksByChat.set(key, this.nowFn());
		if (this.recentRichlinksByChat.size > PHOTON_LAST_INBOUND_CHATS_MAX) {
			const oldest = this.recentRichlinksByChat.keys().next();
			if (!oldest.done) this.recentRichlinksByChat.delete(oldest.value);
		}
	}

	/**
	 * adapter.py:_is_recent_richlink_preview — preview-artifact suppression
	 * within 30s of a recorded link for the SAME chat (injected clock).
	 */
	isRecentRichlinkPreview(
		chatId: string,
		content: Record<string, unknown>,
	): boolean {
		if (!chatId || !isRichlinkPreviewContent(content)) return false;
		const key = normalizeChatKey(chatId);
		const last = this.recentRichlinksByChat.get(key);
		if (last === undefined) return false;
		if (this.nowFn() - last > PHOTON_RICHLINK_PREVIEW_SUPPRESS_MS) {
			this.recentRichlinksByChat.delete(key);
			return false;
		}
		return true;
	}

	private isSenderAllowed(senderId: string): boolean {
		if (this.allowAllUsers) return true;
		if (this.allowedUsers.size === 0) return true;
		return this.allowedUsers.has(senderId);
	}

	private formatGroupContent(content: Record<string, unknown>): {
		text: string;
		messageType: IncomingEvent["messageType"];
		mediaTypes: string[];
	} {
		const items = Array.isArray(content["items"]) ? content["items"] : [];
		const parts: string[] = [];
		const mediaUrls: string[] = [];
		const mediaTypes: string[] = [];
		let messageType: IncomingEvent["messageType"] = "text";
		for (const item of items) {
			if (item === null || typeof item !== "object" || Array.isArray(item)) {
				continue;
			}
			const itemContent = this.objectOf(
				(item as Record<string, unknown>)["content"],
			);
			const itemType = itemContent["type"];
			if (itemType === "text") {
				const t = String(itemContent["text"] ?? "");
				if (t) parts.push(t);
				continue;
			}
			if (itemType === "richlink") {
				parts.push(formatRichlinkContent(itemContent));
				continue;
			}
			if (itemType === "attachment" || itemType === "voice") {
				const normalized = normalizeBinaryPayload(itemContent);
				if (messageType === "text") messageType = normalized.messageType;
				mediaTypes.push(...normalized.mediaTypes);
				mediaUrls.push(...normalized.mediaUrls);
				if (normalized.mediaUrls.length === 0) parts.push(normalized.text);
				continue;
			}
			if (itemType !== undefined) {
				parts.push(`[Photon content type not handled: ${String(itemType)}]`);
			}
		}
		if (mediaUrls.length > 0 && messageType === "text")
			messageType = "document";
		const text =
			parts
				.filter((p) => p.length > 0)
				.join("\n")
				.trim() ||
			(mediaUrls.length > 0 ? "(attachment)" : "[Photon empty group received]");
		return { text, messageType, mediaTypes };
	}

	get pendingFffcCount(): number {
		return this.pendingFffc.size;
	}

	// ── processing hooks (reactions lifecycle, base.py:5407 parity) ──────────

	/** Tapback 👀 on the triggering message while the agent works. */
	async onProcessingStart(event: IncomingEvent): Promise<boolean> {
		if (!this.reactionsEnabled()) return false;
		const chatId = event.source?.chatId;
		const messageId = event.messageId;
		if (!chatId || !messageId) return false;
		return this.addReaction(chatId, messageId, "\u{1F440}");
	}

	/**
	 * Shared reaction-ack flow: swap the 👀 progress tapback for 👍/👎.
	 * Remove-then-add (deterministic under replace-or-stack semantics);
	 * CANCELLED leaves the message unreacted. Soft-fail throughout.
	 */
	async onProcessingComplete(
		event: IncomingEvent,
		outcome: ProcessingOutcome,
	): Promise<boolean[]> {
		if (!this.reactionsEnabled()) return [];
		const chatId = event.source?.chatId;
		const messageId = event.messageId;
		if (!chatId || !messageId) return [];
		const results: boolean[] = [];
		results.push(await this.removeReaction(chatId, messageId));
		if (outcome === "success") {
			results.push(await this.addReaction(chatId, messageId, "\u{1F44D}"));
		} else if (outcome === "failure") {
			results.push(await this.addReaction(chatId, messageId, "\u{1F44E}"));
		}
		return results;
	}

	/** POST /react {spaceId, messageId, emoji}. Soft-fails, never raises. */
	async addReaction(
		chatId: string,
		messageId: string,
		emoji: string,
	): Promise<boolean> {
		try {
			await this.transport.call("/react", {
				spaceId: chatId,
				messageId,
				emoji,
			});
			return true;
		} catch {
			return false;
		}
	}

	/** POST /unreact {spaceId, messageId}. Soft-fails, never raises. */
	async removeReaction(chatId: string, messageId: string): Promise<boolean> {
		try {
			await this.transport.call("/unreact", { spaceId: chatId, messageId });
			return true;
		} catch {
			return false;
		}
	}

	// ── typing (send_typing/stop_typing @~2159) ──────────────────────────────

	/**
	 * Per-chat 5s cooldown under the INJECTED clock — rapid repeats suppressed
	 * to reduce upstream gRPC pressure during overflow events.
	 */
	async sendTyping(chatId: string): Promise<boolean> {
		const now = this.nowFn();
		const last = this.typingLastSent.get(chatId) ?? -Infinity;
		if (now - last < PHOTON_TYPING_COOLDOWN_MS) return false;
		this.typingLastSent.set(chatId, now);
		try {
			await this.transport.call("/typing", { spaceId: chatId, state: "start" });
			return true;
		} catch {
			return false;
		}
	}

	/** stop ALWAYS passes and clears the cooldown window. */
	async stopTyping(chatId: string): Promise<boolean> {
		this.typingLastSent.delete(chatId);
		try {
			await this.transport.call("/typing", { spaceId: chatId, state: "stop" });
			return true;
		} catch {
			return false;
		}
	}

	// ── egress doors ──────────────────────────────────────────────────────

	/**
	 * §6.3 relay-shaped override point: harness utf16-marked chats return
	 * budget AND unit TOGETHER; every other chat rides the scalar budget
	 * (production 8000 chars = PHOTON_MAX_MESSAGE_LENGTH, harness-scale via
	 * constructor injection).
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

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		const formatted = this.formatMessage(content);
		if (this.captureWire !== undefined) {
			return this.captureWire.transmitSend(chatId, formatted, metadata);
		}
		return this.sidecarSend(chatId, formatted);
	}

	/**
	 * The generic rich tier is a KIT ladder concept; photon's OWN rich lane is
	 * the URL-only /send-richlink routing inside sidecarSend. The harness may
	 * script a rich behavior to exercise the downgrade latch.
	 */
	protected override async wireRich(content: string): Promise<SendResult> {
		if (
			this.captureWire === undefined ||
			!this.captureWire.hasRichScript("rich")
		) {
			return { success: false, error: "method not found" };
		}
		return this.captureWire.transmitRich("__rich__", content);
	}

	/** Static no-stream capability: draft frames NEVER reach the wire. */
	protected override async wireDraft(): Promise<SendResult> {
		return { success: false, error: "Not supported" };
	}

	// ── outbound sidecar sends (adapter.py @~2415+) ──────────────────────────

	/**
	 * adapter.py:send — door 1 lands here: format_message then /send.
	 */
	async photonSend(chatId: string, content: string): Promise<SendResult> {
		return this.sidecarSend(chatId, this.formatMessage(content));
	}

	/**
	 * adapter.py:_sidecar_send — THE single text primitive: URL-only
	 * candidates divert to /send-richlink; the body carries format:"markdown"
	 * ONLY when the kill-switch allows (older-sidecar compat omits the key);
	 * oversized text truncates at 8000 codepoints.
	 */
	async sidecarSend(
		spaceId: string,
		text: string,
		opts: { richlink?: boolean; markdown?: boolean } = {},
	): Promise<SendResult> {
		let markdown = opts.markdown !== false;
		const richUrl =
			opts.richlink === false
				? null
				: richlinkCandidate(text, this.markdownEnabled());
		if (richUrl) {
			const richResult = await this.sidecarSendRichlink(spaceId, richUrl);
			if (richResult.success) return richResult;
			markdown = false; // fall back to plain text below
		}
		let out = text;
		if (codePointLen(out) > PHOTON_MAX_MESSAGE_LENGTH) {
			out = Array.from(out).slice(0, PHOTON_MAX_MESSAGE_LENGTH).join("");
		}
		const body: Record<string, unknown> = { spaceId, text: out };
		if (markdown && this.markdownEnabled()) body["format"] = "markdown";
		let data: Record<string, unknown>;
		try {
			data = await this.sidecarCall("/send", body);
		} catch (err) {
			return sidecarErrorResult(err);
		}
		const messageId =
			typeof data["messageId"] === "string" ? data["messageId"] : undefined;
		this.recordSentMessage(messageId);
		return { success: true, messageId };
	}

	/** adapter.py:_sidecar_send_richlink — spectrum-ts richlink() builder. */
	async sidecarSendRichlink(spaceId: string, url: string): Promise<SendResult> {
		let data: Record<string, unknown>;
		try {
			data = await this.sidecarCall("/send-richlink", { spaceId, url });
		} catch (err) {
			return sidecarErrorResult(err);
		}
		const messageId =
			typeof data["messageId"] === "string" ? data["messageId"] : undefined;
		this.recordSentMessage(messageId);
		return { success: true, messageId };
	}

	/**
	 * adapter.py:_sidecar_send_poll — native iMessage poll (clarify surface).
	 * Title required; ≥2 options required; both validated WITHOUT a call.
	 */
	async sidecarSendPoll(
		spaceId: string,
		title: string,
		options: readonly string[],
	): Promise<SendResult> {
		const opts = options
			.map((o) => String(o).trim())
			.filter((o) => o.length > 0);
		if (!title.trim()) {
			return { success: false, error: "poll title is required" };
		}
		if (opts.length < 2) {
			return { success: false, error: "poll needs at least two options" };
		}
		let data: Record<string, unknown>;
		try {
			data = await this.sidecarCall("/send-poll", {
				spaceId,
				title: title.trim().slice(0, PHOTON_MAX_MESSAGE_LENGTH),
				options: opts,
			});
		} catch (err) {
			return sidecarErrorResult(err);
		}
		const messageId =
			typeof data["messageId"] === "string" ? data["messageId"] : undefined;
		this.recordSentMessage(messageId);
		return { success: true, messageId };
	}

	/**
	 * adapter.py:send_effect — text with a native iMessage bubble or screen
	 * effect: POST /send-effect {spaceId, text, effect} with BOTH fields
	 * stripped and validated non-empty BEFORE any call; empty input fails
	 * without touching the sidecar.
	 */
	async sidecarSendEffect(
		spaceId: string,
		text: string,
		effect: string,
	): Promise<SendResult> {
		if (!text.trim() || !effect.trim()) {
			return { success: false, error: "text and effect are required" };
		}
		let data: Record<string, unknown>;
		try {
			data = await this.sidecarCall("/send-effect", {
				spaceId,
				text: text.trim(),
				effect: effect.trim(),
			});
		} catch (err) {
			return sidecarErrorResult(err);
		}
		const messageId =
			typeof data["messageId"] === "string" ? data["messageId"] : undefined;
		return { success: true, messageId };
	}

	/**
	 * adapter.py:send_clarify — multiple-choice clarify renders the NATIVE
	 * poll and flips the clarify into text-capture mode (the vote returns as a
	 * poll_option event → plain text). Poll failure falls back to the numbered-
	 * text clarify; open-ended clarifies keep the plain-text path.
	 */
	async sendClarify(
		chatId: string,
		question: string,
		choices: readonly string[] | undefined,
		claritySessionKey: string,
	): Promise<SendResult> {
		if (!choices || choices.length === 0) {
			// base.py:send_clarify open-ended render — bare '❓ ' + question.
			return this.send(chatId, `❓ ${question}`);
		}
		this.clarifyArmedSet.add(claritySessionKey);
		const result = await this.sidecarSendPoll(chatId, question, choices);
		if (!result.success) {
			// Fall back to the numbered-text clarify (base.py:send_clarify default
			// render, BYTE-EXACT) so the user can still answer.
			const lines = [`❓ ${question}`, ""];
			for (let i = 0; i < choices.length; i += 1) {
				lines.push(`  ${i + 1}. ${choices[i]}`);
			}
			lines.push("");
			lines.push("Reply with the number, the option text, or your own answer.");
			return this.send(chatId, lines.join("\n"));
		}
		return result;
	}

	// ── error classification (adapter.py @~2390-2401) ────────────────────────

	/**
	 * adapter.py:_is_retryable_error — PATTERN-BASED classification. This wire
	 * carries NO Retry-After field anywhere; verdicts come from the explicit
	 * retryable=false/auth_or_config veto plus the _PHOTON_RETRYABLE_PATTERNS
	 * list (and the kit network classes for transport-shaped errors).
	 */
	isRetryableError(error?: string | null): boolean {
		if (!error) return false;
		const lowered = error.toLowerCase();
		if (
			lowered.includes("retryable=false") ||
			lowered.includes("auth_or_config")
		) {
			return false;
		}
		if (PHOTON_RETRYABLE_PATTERNS.some((p) => lowered.includes(p))) return true;
		// Base-class network patterns (connection refused/reset, socket hang up…)
		return (
			lowered.includes("connecterror") ||
			lowered.includes("connectionreset") ||
			lowered.includes("connection refused") ||
			lowered.includes("socket hang up")
		);
	}

	/**
	 * adapter.py:_is_permanent_sidecar_failure — auth_or_config and
	 * target_not_allowed cannot be fixed by retrying or by the plain-text
	 * fallback resend (double-sends a doomed request).
	 */
	isPermanentSidecarFailure(result: SendResult): boolean {
		const raw = (result as PhotonSendResult).rawResponse as
			| Record<string, unknown>
			| undefined;
		return (
			raw !== undefined &&
			raw !== null &&
			typeof raw === "object" &&
			raw["retryable"] === false &&
			(raw["error_class"] === "auth_or_config" ||
				raw["error_class"] === "target_not_allowed")
		);
	}

	/**
	 * adapter.py:_send_with_retry — ONE retry for network-classified failures;
	 * permanent classes return as-is (never double-send); timeout-classified
	 * errors NEVER ride the ladder NOR the plain-text resend (DEC-046 veto —
	 * delivery outcome unknown). Everything else that failed — exhausted
	 * retries AND non-network non-timeout non-permanent failures alike — falls
	 * through to ONE richlink=False/markdown=False plain-text resend
	 * (adapter.py @2478), so a rich-link outage or a formatting-class blip does
	 * not strand an otherwise sendable message.
	 * NO numeric hint participates anywhere: delays derive from the local
	 * schedule alone (retry_after extraction deliberately absent — this wire
	 * has none).
	 */
	async sendWithRetryPhoton(
		chatId: string,
		content: string,
		maxRetries = 1,
		baseDelayMs = 2000,
	): Promise<SendResult> {
		const text = this.formatMessage(content);
		let result = await this.photonSend(chatId, text);
		if (result.success) return result;
		if (this.isPermanentSidecarFailure(result)) return result;
		const errorStr = result.error ?? "";
		const isNetwork =
			result.retryable === true || this.isRetryableError(errorStr);
		// DEC-046 timeout veto: classified timeouts return as-is (the request
		// may have been delivered — neither retry nor plain-text resend).
		if (errorStr.toLowerCase().includes("timed out")) return result;
		if (isNetwork) {
			for (let attempt = 1; attempt <= maxRetries; attempt += 1) {
				await this.sleepFn(baseDelayMs * 2 ** (attempt - 1));
				result = await this.photonSend(chatId, text);
				if (result.success) return result;
				if (this.isPermanentSidecarFailure(result)) return result;
				const nextError = result.error ?? "";
				if (!(result.retryable === true || this.isRetryableError(nextError)))
					break;
			}
		}
		// Plain-text fallback (adapter.py @2478 fall-through): reached by
		// exhausted retries AND non-network non-timeout non-permanent failures;
		// bypasses richlink so a rich-link outage does not strand an otherwise
		// sendable URL.
		return this.sidecarSend(chatId, text.slice(0, PHOTON_MAX_MESSAGE_LENGTH), {
			richlink: false,
			markdown: false,
		});
	}

	// ── THE presence watchdog (adapter.py @~1855-1990) ───────────────────────

	/**
	 * _note_upstream_activity: proof the upstream channel is live — resets the
	 * failure count AND stamps the activity clock (inbound lines + successful
	 * probes + post-respawn).
	 */
	noteUpstreamActivity(): void {
		this.lastUpstreamActivityAt = this.nowFn();
		this.probeFailures = 0;
	}

	get currentProbeFailures(): number {
		return this.probeFailures;
	}

	get lastUpstreamActivity(): number {
		return this.lastUpstreamActivityAt;
	}

	/**
	 * _probe_once tri-state against /probe:
	 *   alive        — real upstream round-trip completed (ok:true);
	 *   hung         — the call ITSELF timed out (SidecarHungError / timeout
	 *                  shapes) — the ONLY respawn-counting verdict;
	 *   inconclusive — anything else (non-200, transport errors). Never counts
	 *                  in EITHER direction: restarting can't fix a down network.
	 */
	async probeOnce(): Promise<ProbeVerdict> {
		try {
			// Headers-only POST (adapter.py:_probe_once @~1869 passes no body).
			await this.transport.call("/probe");
			return "alive";
		} catch (err) {
			if (err instanceof SidecarHungError || isTimeoutShapedError(err)) {
				return "hung";
			}
			return "inconclusive";
		}
	}

	/**
	 * _respawn_sidecar decision core: single-flight latch guarantees the
	 * threshold crossing signals EXACTLY ONCE even under overlapping triggers;
	 * the (injected) respawn mirrors the real flow by resetting failures +
	 * activity clock afterwards.
	 */
	async respawnSidecar(reason: string): Promise<boolean> {
		if (this.respawnInFlight) return false;
		this.respawnInFlight = true;
		try {
			this.respawnSignals.push(reason);
			if (this.onRespawn !== undefined) {
				await this.onRespawn(reason);
			}
			this.noteUpstreamActivity();
			return true;
		} finally {
			this.respawnInFlight = false;
		}
	}

	/**
	 * One watchdog ITERATION (_presence_watchdog loop body, stepwise — the
	 * interval sleep belongs to the driver):
	 *   disabled      — probe_interval ≤ 0 escape hatch;
	 *   skipped-idle  — natural traffic proved liveness within the interval;
	 *   alive         — success resets failures;
	 *   hung          — failure counted; N consecutive ⇒ EXACTLY ONE respawn;
	 *   inconclusive  — strictly no action either way.
	 */
	async watchdogTick(): Promise<WatchdogTickVerdict> {
		if (!this.probeEnabled) return "disabled";
		const idle = this.nowFn() - this.lastUpstreamActivityAt;
		if (idle < this.probeIntervalMs) return "skipped-idle";
		const verdict = await this.probeOnce();
		if (verdict === "alive") {
			this.noteUpstreamActivity();
			return "alive";
		}
		if (verdict === "hung") {
			this.probeFailures += 1;
			if (this.probeFailures >= this.probeMaxFailures) {
				await this.respawnSidecar(
					`${this.probeFailures} consecutive hung probes`,
				);
				return "respawned";
			}
			return "hung";
		}
		return "inconclusive";
	}

	// ── sidecar health monitor (adapter.py:_monitor_sidecar_health @~1113) ───

	/**
	 * One /healthz poll: a degraded upstream stream (stream.ok === false)
	 * promotes to FATAL UPSTREAM_STREAM_DEGRADED (retryable) and DISPATCHES the
	 * detached notification; unreachable sidecars just skip a beat.
	 */
	async runHealthCheckOnce(): Promise<"ok" | "degraded-fatal" | "unreachable"> {
		let data: Record<string, unknown>;
		try {
			data = await this.sidecarCall("/healthz", {});
		} catch {
			return "unreachable";
		}
		const stream = this.objectOf(data["stream"]);
		if (Object.keys(stream).length === 0) return "ok";
		if (stream["ok"] !== false) return "ok";
		const state = String(stream["state"] ?? "unknown");
		const degradedForMs = stream["degradedForMs"];
		const lastIssue = String(stream["lastIssue"] ?? "unknown stream issue");
		this.setFatalError(
			"UPSTREAM_STREAM_DEGRADED",
			`Photon upstream stream degraded (state=${state}, degradedForMs=${String(degradedForMs)}): ${lastIssue}`,
			true,
		);
		this.dispatchFatalNotification();
		return "degraded-fatal";
	}

	private setFatalError(
		code: string,
		message: string,
		retryable: boolean,
	): void {
		this.fatalErrorCode = code;
		this.fatalErrorRetryable = retryable;
		this.fatalErrorMessage = message;
		this.lifecycle.markFatal({ kind: "manual", detail: `${code}: ${message}` });
	}

	/**
	 * adapter.py:_dispatch_fatal_notification — the notification MUST be
	 * DETACHED from the caller's stack: disconnect() cancelling the detecting
	 * task used to cancel its own ultimate caller (self-cancellation bug,
	 * PR #69112 follow-up). Ported with a macrotask hop: dispatch RETURNS
	 * before the notification starts; cancellation aimed at the caller can
	 * never reach the handoff; notification failures warn, never throw.
	 */
	dispatchFatalNotification(): boolean {
		const code = this.fatalErrorCode ?? "UNKNOWN";
		this.fatalNotificationsDispatched.push(code);
		setTimeout(() => {
			void (async () => {
				try {
					await this.notifyFatalErrorSeam();
					this.fatalNotificationsCompleted.push(code);
				} catch (err) {
					this.fatalNotificationWarnings.push(
						`fatal-error notification failed: ${err instanceof Error ? err.message : String(err)}`,
					);
				}
			})();
		}, 0);
		return true;
	}

	/** Transport seam hop (mutant/test wrap point — raft handleWakePost style). */
	protected sidecarCall(
		path: string,
		body: Record<string, unknown>,
	): Promise<Record<string, unknown>> {
		return this.transport.call(path, body);
	}

	// ── identity/secrets probes (conformance subject plumbing) ───────────────

	/** Unique-credential probe (raft-subject parity; marks FATAL on refusal). */
	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		const manager = new TokenLockManagerSeam({ nowMs: () => 1_000 });
		const first = this.acquireCredentialLock(
			manager,
			"photon-project-credentials",
			"cred-photon-1",
			"instance-A",
		);
		if (!first.acquired) return { acquired: false, holderOwner: "?" };
		try {
			this.acquireCredentialLock(
				manager,
				"photon-project-credentials",
				"cred-photon-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = manager.holderOf(
				"photon-project-credentials",
				"cred-photon-1",
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}

	buildMissingSecretSibling(): PhotonAdapter {
		return new PhotonAdapter({
			sidecar: this.transport,
			envReader: () => undefined,
		});
	}
}

// ── module-level helpers ─────────────────────────────────────────────────────

interface NormalizedBinaryPayload {
	text: string;
	messageType: IncomingEvent["messageType"];
	mediaUrls: string[];
	mediaTypes: string[];
}

/**
 * adapter.py:_normalize_binary_payload metadata-only branch (byte caching is
 * a documented exclusion): voice promotion by type/name/MIME (CAF voice notes),
 * MIME-derived message types, marker text when bytes aren't inline-cacheable.
 */
function normalizeBinaryPayload(
	payload: Record<string, unknown>,
): NormalizedBinaryPayload {
	let isVoice = payload["type"] === "voice";
	const name = String(payload["name"] ?? (isVoice ? "voice" : "(unnamed)"));
	const mime = String(payload["mimeType"] ?? "");
	if (
		!isVoice &&
		(name.toLowerCase().endsWith(".caf") || mime === "audio/x-caf")
	) {
		isVoice = true; // iMessage voice notes use CAF
	}
	const messageType: IncomingEvent["messageType"] = isVoice
		? "voice"
		: mime.startsWith("image/")
			? "photo"
			: mime.startsWith("video/")
				? "video"
				: mime.startsWith("audio/")
					? "voice"
					: "document";
	const duration = payload["duration"];
	const durationText =
		typeof duration === "number" && Number.isFinite(duration)
			? `, duration: ${duration}s`
			: "";
	return {
		text: `[Photon ${isVoice ? "voice" : "attachment"} received: ${name} (${mime || "unknown MIME"}${durationText})]`,
		messageType,
		mediaUrls: [],
		mediaTypes: [mime || (isVoice ? "audio/mp4" : "application/octet-stream")],
	};
}

/**
 * adapter.py:_sidecar_error_from_response mapping — SendResult carries the
 * STRUCTURED classification (raw_response.error_class/retryable) so callers
 * can separate permanent from retryable without string sniffing.
 * target_not_allowed gets the canonical user-facing explanation, never raw
 * upstream error text.
 */
function sidecarErrorResult(err: unknown): PhotonSendResult {
	if (err instanceof SidecarHttpError) {
		let error = err.error;
		const errorClass = err.errorClass;
		let retryable = err.retryable;
		if (errorClass === "target_not_allowed") {
			error = PHOTON_TARGET_NOT_ALLOWED_MESSAGE;
			retryable = false;
		}
		return {
			success: false,
			error: `Photon sidecar ${err.path} returned ${err.statusCode} (${errorClass}, ${err.retryableMarker}): ${error}`,
			rawResponse: { error_class: errorClass, retryable },
			retryable,
		};
	}
	return {
		success: false,
		error: err instanceof Error ? err.message : String(err),
	};
}

/** httpx timeout-shape parity for probe verdicts (timeout/timed out). */
function isTimeoutShapedError(err: unknown): boolean {
	const message = err instanceof Error ? err.message.toLowerCase() : "";
	return message.includes("timeout") || message.includes("timed out");
}
