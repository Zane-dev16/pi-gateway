// pi_platforms/a2a/a2a-adapter — THE A2A v1.0 inbound platform adapter,
// ported from the READ-ONLY Hermes plugins/platforms/a2a/adapter.py onto the
// kit base. Everything policy-shaped is inherited; this module supplies
// TRANSPORT (the JSON-RPC request plane as HANDLER SEAMS) and MANIFEST DATA.
//
// Shape (DEC-002 webhook family — bounded sync window):
//   - The reference runs a stdlib http.server thread; the port NEVER BINDS
//     SOCKETS. The request plane is do_GET/do_POST reimplemented as
//     handleGet/handlePost seams invoked directly with synthesized requests
//     (msgraph precedent); SSE frames land in an injected sink, push
//     notifications go through an injected PushTransport.
//   - Reply plane: each inbound task registers a Deferred keyed by task id
//     (+ per-context FIFO order); the agent's final reply (guard sendReply →
//     send() with metadata.notify) resolves the OLDEST outstanding task for
//     that context — turning the async gateway into a synchronous
//     request/response for the A2A caller within the peer's HTTP window
//     (deadline A2A_REPLY_TIMEOUT, default 300 s).
//   - Every inbound task is filtered + framed (security.wrapInbound),
//     audit-logged, persisted, and routed into the live gateway session via
//     the normal ingress path — the agent that replies is the same one
//     talking to its user.
//
// Exclusions (runner wiring, NOT semantics — see module tail notes):
//   - the HTTP server/watchdog INTERVAL THREADS (failOrphans sweep stays a
//     callable method driven by an injected clock),
//   - cross-profile subprocess forwarding (_forward_to_profile spawns OS
//     children — the port never does; non-local served agents complete
//     FAILED with a deterministic notice),
//   - the live pi tool registry lookup for Agent Card skills (configured
//     advertised_toolsets only).
//
// Layering: imports pi_gateway downward + kit same-layer ONLY; no adapter
// cross-imports.

import { hostname } from "node:os";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

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
import type { ScopedSecretReader } from "../kit/registration.js";

import {
	A2A_BODY_CAP_BYTES,
	A2A_ACCEPTED_VERSIONS,
	A2A_DEFAULT_PORT,
	A2A_ORPHAN_TIMEOUT_SECONDS,
	A2A_PLUGIN_MANIFEST,
	A2A_PUBLIC_URL_ENV,
	A2A_REPLY_TIMEOUT_RULE,
	A2A_SSE_KEEPALIVE_SECONDS,
	AGENT_CARD_LEGACY_PATH,
	AGENT_CARD_PATH,
	DEFAULT_MAX_PINGPONG_TURNS,
	ERR_INVALID_PARAMS,
	ERR_METHOD_NOT_FOUND,
	ERR_PARSE,
	ERR_RATE_LIMITED,
	ENV_ADVERTISED_TOOLSETS,
	ENV_AGENT_DESCRIPTION,
	ENV_PROVIDER_ORG,
	ENV_PROVIDER_URL,
	ERR_TASK_NOT_CANCELABLE,
	ERR_TASK_NOT_FOUND,
	ERR_UNAUTHORIZED,
	ERR_UNTRUSTED_PEER,
	INPUT_REQUIRED_MARKER,
	METRICS_PATH,
	methodInfo,
	STATE_CANCELED,
	STATE_COMPLETED,
	STATE_FAILED,
	STATE_INPUT_REQUIRED,
	STATE_REJECTED,
	STATE_SUBMITTED,
	STATE_WORKING,
	TERMINAL_STATES,
	declareA2aTrustBoundary,
	validateA2aTrustBoundary,
	type MethodInfo,
} from "./manifest.js";
import {
	AuditLog,
	ConversationStore,
	Deferred,
	Metrics,
	RateLimiter,
	TaskStore,
	TurnTracker,
	buildAgentCard,
	buildTask,
	extractContextId,
	extractText,
	jsonrpcError,
	jsonrpcResult,
	newContextId,
	newTaskId,
	sendMessageResponse,
	skillsFromToolsets,
	statusUpdate,
	artifactUpdate,
	streamTask,
	sseData,
	sseDone,
	type JsonRpcId,
	type SecondsClock,
	type TaskRecord,
} from "./protocol.js";
import {
	authenticate,
	getPushSecret,
	isSafeCallbackUrl,
	isTrustedPeer,
	localhostOnly,
	redactOutbound,
	resolveBindHost,
	signPushPayload,
	wrapInbound,
	type EnvReader,
} from "./security.js";

/** msgraph-parity HTTP handler response shape. */
export interface HandlerResponse {
	status: number;
	contentType?: string | undefined;
	body?: string | Record<string, unknown> | undefined;
}

/** SSE frame sink seam (the port never owns a socket). */
export interface SseSink {
	write(chunk: string): void;
}

/** Injected push-notification transport (NO real network). */
export interface PushTransport {
	postCallback(
		url: string,
		body: string,
		headers: Record<string, string>,
	): Promise<{ status: number }>;
}

export interface A2aRequestInput {
	path?: string | undefined;
	headers?: Record<string, string> | undefined;
	rawBody?: Buffer | undefined;
	clientIp?: string | undefined;
	/** Required by stream/subscribe operations; buffered when omitted. */
	sseSink?: SseSink | undefined;
}

export interface ServedAgentConfig {
	slug?: string | undefined;
	id?: string | undefined;
	path?: string | undefined;
	name?: string | undefined;
	description?: string | undefined;
	tenant?: string | undefined;
	local?: boolean | undefined;
	profile?: string | undefined;
	advertised_toolsets?: readonly string[] | undefined;
	timeout?: number | undefined;
}

export interface A2aAdapterConfig {
	port?: number | undefined;
	host?: string | undefined;
	agent_name?: string | undefined;
	description?: string | undefined;
	advertised_toolsets?: readonly string[] | undefined;
	/** adapter.py:extra.agents — preferred served-agent routing table. */
	agents?:
		| Record<string, ServedAgentConfig>
		| readonly ServedAgentConfig[]
		| undefined;
	/** adapter.py:extra.served_agents alias (same lane, lower priority). */
	served_agents?:
		| Record<string, ServedAgentConfig>
		| readonly ServedAgentConfig[]
		| undefined;
}

/**
 * hermes_cli.config.load_config() snapshot parity
 * (adapter.py:_load_global_a2a_config) — INJECTED; the port never reads
 * ~/.hermes/config.yaml itself. Only the served-agent fallback lanes are
 * consulted, and only when BOTH extra lanes are absent/empty
 * (adapter.py:_load_served_agents).
 */
export interface A2aGlobalConfig {
	a2a_served_agents?:
		| Record<string, ServedAgentConfig>
		| readonly ServedAgentConfig[]
		| undefined;
	a2a?: { served_agents?: A2aGlobalConfig["a2a_served_agents"] } | undefined;
}

export interface A2aCaptureWire {
	transmitSend(
		chatId: string,
		content: string,
		metadata: Record<string, unknown>,
	): Promise<SendResult>;
	hasRichScript(opKind: string): boolean;
	transmitRich(chatId: string, content: string): Promise<SendResult>;
}

export interface A2aAdapterOptions {
	config?: A2aAdapterConfig | undefined;
	/** Scoped env reader (os.getenv parity; NEVER process.env directly). */
	envReader?: EnvReader | undefined;
	/** Global hermes-config snapshot for the a2a_served_agents fallback lane. */
	globalConfig?: A2aGlobalConfig | undefined;
	nowMs?: (() => number) | undefined;
	scalarMaxUnits?: number | undefined;
	captureWire?: A2aCaptureWire | undefined;
	pushTransport?: PushTransport | undefined;
	/** Persisted-conversation directory; lazily mkdtemp'd when unset. */
	storageDir?: string | undefined;
	auditPath?: string | undefined;
	/** Real-ms wake tick for the reply-wait loop (deadline checks stay on the INJECTED clock). */
	pollTickMs?: number | undefined;
	spawner?: TaskSpawner | undefined;
}

interface ServedAgent {
	slug: string;
	path: string;
	tenant: string;
	profile: string;
	local: boolean;
	name: string;
	description: string;
	advertisedToolsets: readonly string[];
	timeout: number;
}

interface PendingReply {
	taskId: string;
	contextId: string;
	peer: string;
	future: Deferred<{ state: string; text: string }>;
	createdIso: string;
	startedMs: number;
}

type ProcessingOutcome = "success" | "failure" | "cancelled";

const DEFAULT_DESCRIPTION =
	"Hermes Agent — a general-purpose agent reachable over A2A.";
const RESERVED_PATH_SEGMENTS = new Set(["health", "metrics", ".well-known"]);

/** adapter.py:_clean_slug — URL-safe single-segment slug ('' for default/root). */
function cleanSlug(value: unknown): string {
	const slug = String(value ?? "")
		.trim()
		.replace(/^\/+|\/+$/g, "");
	if (slug === "" || slug === "default" || slug === "root") return "";
	return slug.split("/")[0] ?? "";
}

/** adapter.py:_join_url — ensure trailing slash on base, single-segment join. */
function joinUrl(base: string, prefix: string): string {
	const trimmedBase = (base || "").trim() || "/";
	const withSlash = trimmedBase.endsWith("/") ? trimmedBase : `${trimmedBase}/`;
	const cleaned = (prefix ?? "").replace(/^\/+|\/+$/g, "");
	if (!cleaned) return withSlash;
	if (/^https?:\/\//i.test(withSlash)) {
		try {
			return new URL(`${cleaned}/`, withSlash).toString();
		} catch {
			/* fall through to manual join */
		}
	}
	return `${withSlash}${cleaned}/`;
}

function normalizeHeaders(
	input: Record<string, string> | undefined,
): Record<string, string> {
	const out: Record<string, string> = {};
	for (const [key, value] of Object.entries(input ?? {})) {
		out[key.toLowerCase()] = value;
	}
	return out;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function safeInt(value: unknown, fallback: number): number {
	if (value === null || value === undefined || value === "") return fallback;
	const parsed =
		typeof value === "number"
			? Math.trunc(value)
			: Number.parseInt(String(value), 10);
	return Number.isNaN(parsed) ? fallback : parsed;
}

function safeIntOrNull(value: unknown): number | null {
	if (value === null || value === undefined) return null;
	const parsed =
		typeof value === "number"
			? Math.trunc(value)
			: Number.parseInt(String(value), 10);
	return Number.isNaN(parsed) ? null : parsed;
}

const RACE_TIMEOUT = Symbol("a2a.reply.race-timeout");

/**
 * Bounded-poll wait (fut.result(timeout) adaptation): the FUTURE settles on
 * guard-pipeline completion; deadline decisions run on the INJECTED clock,
 * so the loop wakes on a small real tick and re-checks that clock — an
 * injected-clock jump past the deadline ends the wait deterministically
 * without real sleeping.
 */
function raceWithTimer<T>(
	promise: Promise<T>,
	tickMs: number,
): Promise<T | typeof RACE_TIMEOUT> {
	return new Promise((resolve) => {
		let settled = false;
		const timer = setTimeout(() => {
			if (!settled) {
				settled = true;
				resolve(RACE_TIMEOUT);
			}
		}, tickMs);
		void promise.then(
			(value) => {
				if (!settled) {
					settled = true;
					clearTimeout(timer);
					resolve(value);
				}
			},
			() => {
				if (!settled) {
					settled = true;
					clearTimeout(timer);
					resolve(RACE_TIMEOUT);
				}
			},
		);
	});
}

/** The one command registry (07 §1 derivation — mirrors the reference set). */
const A2A_REGISTRY: CommandRegistry = [
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

export class A2AAdapter extends BasePlatformAdapter {
	readonly pluginManifest = A2A_PLUGIN_MANIFEST;
	readonly trustBoundary = declareA2aTrustBoundary();

	// ── config (__init__ parity) ──────────────────────────────────────────────
	readonly port: number;
	readonly host: string;
	readonly agentName: string;
	readonly advertisedToolsets: readonly string[];
	readonly servedAgents: Map<string, ServedAgent>;

	private readonly env: EnvReader;
	private readonly nowFn: () => number;
	private readonly clock: SecondsClock;
	private readonly pollTickMs: number;
	private readonly captureWire: A2aCaptureWire | undefined;
	private readonly pushTransport: PushTransport;
	private readonly secretReader: ScopedSecretReader;
	private readonly globalConfig: A2aGlobalConfig | undefined;

	// ── per-adapter protocol state (not module-global) ─────────────────────────
	readonly tasks: TaskStore;
	readonly turns: TurnTracker;
	readonly rateLimiter: RateLimiter;
	readonly metrics: Metrics;

	private readonly pending = new Map<string, PendingReply>();
	private readonly pendingOrder = new Map<string, string[]>();

	private injectedStorageDir: string | undefined;
	private ownTempDir: string | null = null;
	private conversationStoreField: ConversationStore | null = null;
	private auditLogField: AuditLog | null = null;

	// ── runtime state ──────────────────────────────────────────────────────────
	private handlerAttached = false;
	private connectedOnce = false;

	/** Row observability + subject-support surface. */
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

	constructor(opts: A2aAdapterOptions = {}) {
		const config = opts.config ?? {};
		super({
			manifestName: A2A_PLUGIN_MANIFEST.name,
			capabilities: A2A_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? 4096,
		});
		this.env = opts.envReader ?? (() => undefined);
		this.secretReader = this.env;
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.clock = () => this.nowFn() / 1000;
		this.pollTickMs = Math.max(1, opts.pollTickMs ?? 25);
		this.captureWire = opts.captureWire;
		this.pushTransport = opts.pushTransport ?? {
			// Default transport: NO network — every callback reports failure.
			postCallback: async () => ({ status: 599 }),
		};
		this.globalConfig = opts.globalConfig;

		this.port = Number(config.port ?? A2A_DEFAULT_PORT) || A2A_DEFAULT_PORT;
		// Bind host: A2A_HOST env first, then extra-config host (04 §4.2
		// env>YAML precedence); loopback by construction.
		const bindEnv: EnvReader = (name) => {
			if (name === "A2A_HOST" && this.env("A2A_HOST") === undefined) {
				return config.host;
			}
			return this.env(name);
		};
		const bind = resolveBindHost(bindEnv, (warning) =>
			this.logger?.warn?.(`[a2a] ${warning}`),
		);
		this.host = bind.host;

		// Bind-safety escalation (DEC-064, text formerly proposed in manifest.ts): Hermes
		// answers an operator-requested non-loopback bind with NO configured
		// credential by WARNING and downgrading to 127.0.0.1. The kit expresses
		// the same refusal LOUDLY at construction (msgraph precedent: refusal
		// postures surface in /status instead of a silent downgrade) — the
		// security property (never serve wide-open) is preserved strictly
		// stronger; default/loopback constructions behave identically to the
		// reference.
		if (bind.widenedRequested && localhostOnly(this.env)) {
			this.lifecycle.disable({
				kind: "secret_missing",
				secretKey: "A2A_PEER_TOKENS",
				manifestName: A2A_PLUGIN_MANIFEST.name,
			});
		}
		this.agentName = defaultAgentName(this.env);
		// adapter.py:__init__ _advertised_toolsets — the configured list wins;
		// an EMPTY list falls back to the A2A_ADVERTISED_TOOLSETS csv (blank
		// entries dropped on both lanes).
		this.advertisedToolsets = resolveAdvertisedToolsets(config, this.env);
		this.injectedStorageDir = opts.storageDir;
		this.servedAgents = loadServedAgents(
			config,
			{
				agentName: this.agentName,
				// adapter.py:_load_served_agents default_desc — A2A_AGENT_DESCRIPTION
				// is THE root-agent description lane in the reference; config
				// .description is this port's extra-config extension (env > YAML,
				// §4.2 precedence — same ladder as A2A_HOST above).
				description:
					this.env(ENV_AGENT_DESCRIPTION) ??
					config.description ??
					DEFAULT_DESCRIPTION,
				host: this.host,
				port: this.port,
				toolsets: this.advertisedToolsets,
			},
			this.globalConfig,
		);

		this.tasks = new TaskStore(this.clock);
		this.turns = new TurnTracker(this.clock);
		this.rateLimiter = new RateLimiter(this.clock, this.env);
		this.metrics = new Metrics(this.clock);

		// DEC-017: an incomplete trust boundary is a CONSTRUCTION-TIME error.
		const boundaryErrors = validateA2aTrustBoundary(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			this.lifecycle.disable({
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			});
		}

		// plugin.yaml requires_env [] — enablement holds unconditionally; the
		// resolve call keeps the §11 posture explicit (bind safety is enforced
		// per-request, not by disabling the adapter).
		const enablement = resolveEnablement(
			A2A_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // bounded sync window; no native streams
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

	/** adapter.py:authorization_is_upstream — capability DATA with rationale. */
	get authorizationIsUpstream(): true {
		// A2A authenticates EVERY inbound request via bearer token (or
		// localhost-only binding) in the POST ladder BEFORE dispatch — the
		// identity is already authorized upstream. Without this override the
		// gateway's per-platform user allow-list would reject A2A peers,
		// because their identity is a token-derived name or pod IP, not a
		// platform account the operator configures in an env allow-list.
		// This is authorization DELEGATED TO THE TRANSPORT, not a fail-open:
		// every request is 401'd when the credential is wrong
		// (adapter.py:A2AAdapter.authorization_is_upstream).
		return true;
	}

	get isConnected(): boolean {
		return this.connectedOnce;
	}

	/** Rows probe the in-flight reply plane through this counter. */
	pendingCount(): number {
		return this.pending.size;
	}

	// ── lifecycle ──────────────────────────────────────────────────────────────

	/**
	 * connect() parity minus the SOCKET: the reference binds _A2AServer +
	 * starts http/watchdog threads; the port marks readiness only. The
	 * watchdog INTERVAL THREAD is runner wiring — sweepOrphans() below is the
	 * callable sweep body (injected clock).
	 */
	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		this.connectedOnce = true;
		return true;
	}

	override async disconnect(): Promise<void> {
		this.connectedOnce = false;
		// Fail any in-flight replies so blocked handlers don't hang.
		for (const entry of this.pending.values()) {
			entry.future.resolve({
				state: STATE_FAILED,
				text: "[agent shutting down]",
			});
		}
		this.pending.clear();
		this.pendingOrder.clear();
	}

	dispose(): void {
		if (this.ownTempDir !== null) {
			try {
				rmSync(this.ownTempDir, { recursive: true, force: true });
			} catch {
				/* best-effort cleanup */
			}
			this.ownTempDir = null;
		}
	}

	// ── persistence seams (mkdtemp-isolated when not injected) ────────────────

	private ensureMaterialized(): void {
		if (this.conversationStoreField !== null) return;
		let dir = this.injectedStorageDir;
		if (dir === undefined) {
			dir = mkdtempSync(path.join(tmpdir(), "a2a-conversations-"));
			this.ownTempDir = dir;
		}
		this.conversationStoreField = new ConversationStore(dir);
		this.auditLogField = new AuditLog(path.join(dir, "a2a_audit.jsonl"));
	}

	get conversations(): ConversationStore {
		this.ensureMaterialized();
		return this.conversationStoreField as ConversationStore;
	}

	get audit(): AuditLog {
		this.ensureMaterialized();
		return this.auditLogField as AuditLog;
	}

	// ── GET plane (do_GET @~215 parity) ───────────────────────────────────────

	/**
	 * adapter.py:_request_public_url — routable URL derivation, PURE over
	 * headers+env: A2A_PUBLIC_URL env > X-Forwarded-Host > Host (scheme from
	 * X-Forwarded-Proto) > "" ("caller has no info, fall back to bind host").
	 */
	requestPublicUrl(headers: Record<string, string>): string {
		const explicit = (this.env(A2A_PUBLIC_URL_ENV) ?? "").trim();
		if (explicit) return explicit;
		const h = normalizeHeaders(headers);
		let host = h["x-forwarded-host"] || h["host"] || "";
		if (!host) return "";
		host = host.split(",")[0]?.trim() ?? "";
		if (!host) return "";
		const scheme =
			(h["x-forwarded-proto"] || "http").split(",")[0]?.trim() || "http";
		return `${scheme}://${host}/`;
	}

	handleGet(
		rawPath: string,
		headers: Record<string, string> = {},
		clientIp = "",
	): HandlerResponse {
		const normalized = normalizeHeaders(headers);
		const route = this.routeForPath(rawPath);
		const subpath = route.subpath.replace(/\/+$/, "") || "/";
		if (subpath === AGENT_CARD_LEGACY_PATH || subpath === AGENT_CARD_PATH) {
			const publicUrl = this.requestPublicUrl(normalized) || null;
			return jsonResponse(
				200,
				this.buildCard(publicUrl ?? undefined, route.agent),
			);
		}
		if (subpath === "/" || subpath === "/health") {
			const payload: Record<string, unknown> = {
				status: "ok",
				agent: route.agent.name || this.agentName,
			};
			// Do not leak profile/tenant topology on remote UNauthenticated
			// GETs — Agent Cards are intentionally public; health topology is
			// not (adapter.py do_GET health branch).
			if (
				localhostOnly(this.env) ||
				authenticate(normalized["authorization"], clientIp, this.env) !== null
			) {
				payload["served_agents"] = this.servedAgentSummary(
					this.requestPublicUrl(normalized) || null,
				);
			}
			return jsonResponse(200, payload);
		}
		if (subpath === METRICS_PATH) {
			return jsonResponse(200, this.metrics.snapshot());
		}
		return jsonResponse(404, { error: "not found" });
	}

	// ── POST plane (do_POST @~250 parity) ─────────────────────────────────────

	async handlePost(input: A2aRequestInput): Promise<HandlerResponse> {
		const headers = normalizeHeaders(input.headers);
		const clientIp = input.clientIp ?? "";

		// Identity comes from the presented credential (or the socket in
		// localhost-only mode) — never from the request body.
		const identity = this.authIdentity(headers, clientIp);
		if (identity === null) {
			return jsonResponse(
				401,
				jsonrpcError(null, ERR_UNAUTHORIZED, "unauthorized"),
			);
		}

		let req: unknown;
		try {
			const declaredRaw = headers["content-length"] ?? "0";
			const declaredLength = Number.parseInt(declaredRaw, 10);
			if (Number.isNaN(declaredLength)) throw new Error("bad content-length");
			if (declaredLength > A2A_BODY_CAP_BYTES) {
				return jsonResponse(
					413,
					jsonrpcError(null, ERR_PARSE, "payload too large"),
				);
			}
			const raw =
				input.rawBody !== undefined && input.rawBody.length > 0
					? input.rawBody
					: Buffer.from("{}", "utf-8");
			req = JSON.parse(raw.toString("utf-8"));
		} catch {
			return jsonResponse(400, jsonrpcError(null, ERR_PARSE, "parse error"));
		}

		if (!isRecord(req)) {
			return jsonResponse(
				400,
				jsonrpcError(
					null,
					ERR_INVALID_PARAMS,
					"JSON-RPC request must be an object",
				),
			);
		}

		const reqId = req["id"] as JsonRpcId;
		const method = String(req["method"] ?? "");
		let paramsRaw: unknown = req["params"];
		if (paramsRaw === null || paramsRaw === undefined) paramsRaw = {};
		if (!isRecord(paramsRaw)) {
			return jsonResponse(
				200,
				jsonrpcError(reqId, ERR_INVALID_PARAMS, "params must be an object"),
			);
		}
		const params = paramsRaw;

		const version = (headers["a2a-version"] ?? "").trim();
		if (version && !A2A_ACCEPTED_VERSIONS.has(version)) {
			return jsonResponse(
				200,
				jsonrpcError(
					reqId,
					ERR_INVALID_PARAMS,
					`unsupported A2A-Version: ${version}`,
				),
			);
		}

		const info = methodInfo(method);
		const route = this.routeForRequest(input.path ?? "/", params);
		if ("error" in route) {
			return jsonResponse(
				400,
				jsonrpcError(reqId, ERR_INVALID_PARAMS, route.error),
			);
		}
		const agent = route.agent;

		if (!this.rateLimiter.allow(identity)) {
			this.metrics.rateLimitTriggers += 1;
			return jsonResponse(
				429,
				jsonrpcError(reqId, ERR_RATE_LIMITED, "rate limit exceeded"),
			);
		}

		if (!isTrustedPeer(identity, this.env)) {
			return jsonResponse(
				403,
				jsonrpcError(
					reqId,
					ERR_UNTRUSTED_PEER,
					`peer '${identity}' not trusted`,
				),
			);
		}

		if (info === null) {
			return jsonResponse(
				200,
				jsonrpcError(
					reqId,
					ERR_METHOD_NOT_FOUND,
					`method not found: ${method}`,
				),
			);
		}

		switch (info.operation) {
			case "send":
				return jsonResponse(
					200,
					await this.rpcMessageSend(reqId, params, identity, agent, info),
				);
			case "stream":
				return await this.rpcMessageStream(
					input,
					reqId,
					params,
					identity,
					agent,
				);
			case "get":
				return jsonResponse(200, this.rpcTasksGet(reqId, params, agent));
			case "list":
				return jsonResponse(200, this.rpcTasksList(reqId, params, agent));
			case "cancel":
				return jsonResponse(200, this.rpcTasksCancel(reqId, params, agent));
			case "subscribe":
				return await this.rpcTasksSubscribe(input, reqId, params, agent);
			case "push_create":
				return jsonResponse(
					200,
					this.rpcPushConfigCreate(reqId, params, agent),
				);
			case "push_get":
				return jsonResponse(200, this.rpcPushConfigGet(reqId, params, agent));
			case "push_list":
				return jsonResponse(200, this.rpcPushConfigList(reqId, params, agent));
			case "push_delete":
				return jsonResponse(
					200,
					this.rpcPushConfigDelete(reqId, params, agent),
				);
		}
		// methodInfo() is exhaustive over A2aOperation — unreachable.
		return jsonResponse(
			200,
			jsonrpcError(reqId, ERR_METHOD_NOT_FOUND, `method not found: ${method}`),
		);
	}

	/**
	 * Identity resolution seam (security.authenticate over the normalized
	 * Authorization header) — a single prototype method so conformance
	 * mutants can defeat the gate via defineProperty and be DETECTED.
	 */
	authIdentity(
		headers: Record<string, string>,
		clientIp: string,
	): string | null {
		return authenticate(headers["authorization"], clientIp, this.env);
	}

	// ── routing + Agent Cards ───────────────────────────────────────────────────

	routeForPath(rawPath: string): { agent: ServedAgent; subpath: string } {
		const p = (rawPath || "/").split("?")[0]?.split("#")[0] || "/";
		// Longest prefix wins; the default/root agent is the fallback.
		const sorted = [...this.servedAgents.values()].sort(
			(a, b) => b.path.length - a.path.length,
		);
		for (const agent of sorted) {
			const prefix = agent.path;
			if (prefix && (p === prefix || p.startsWith(`${prefix}/`))) {
				let subpath = p.slice(prefix.length) || "/";
				if (!subpath.startsWith("/")) subpath = `/${subpath}`;
				return { agent, subpath };
			}
		}
		return {
			agent: this.servedAgents.get("") as ServedAgent,
			subpath: p,
		};
	}

	routeForRequest(
		rawPath: string,
		params: Record<string, unknown>,
	): { agent: ServedAgent; subpath: string } | { error: string } {
		const route = this.routeForPath(rawPath);
		let agent = route.agent;
		const tenant = String(params["tenant"] ?? "");
		// If no URL prefix chose a non-default agent, allow v1.0 tenant routing.
		if (agent.slug === "" && tenant) {
			const matches = [...this.servedAgents.values()].filter(
				(a) => a.tenant === tenant,
			);
			if (matches.length > 0) agent = matches[0] as ServedAgent;
		}
		const expected = agent.tenant || "";
		if (tenant && expected && tenant !== expected) {
			return {
				error: `tenant '${tenant}' does not match routed agent ${
					agent.slug || "default"
				}`,
			};
		}
		return { agent, subpath: route.subpath };
	}

	scopeForAgent(agent: ServedAgent): [string, string] {
		return [agent.slug || "", agent.tenant || ""];
	}

	buildCard(
		publicUrl?: string | undefined,
		agent?: ServedAgent,
	): Record<string, unknown> {
		const entry = agent ?? (this.servedAgents.get("") as ServedAgent);
		const base =
			(publicUrl ?? "").trim() || `http://${this.host}:${this.port}/`;
		const url = joinUrl(base, entry.path);
		return buildAgentCard({
			name: entry.name || this.agentName,
			url,
			description: entry.description || DEFAULT_DESCRIPTION,
			skills: this.advertisedSkills(entry),
			streaming: entry.local,
			pushNotifications: true,
			authRequired: !localhostOnly(this.env),
			tenant: entry.tenant,
			// protocol.py:build_agent_card provider block — env overrides with
			// getenv semantics: UNset ORG ⇒ 'Hermes Agent' default (via ??), a
			// SET-but-empty ORG passes through as ''; UNset or empty URL ⇒ the
			// card url fallback.
			providerOrg: this.env(ENV_PROVIDER_ORG),
			providerUrl: this.env(ENV_PROVIDER_URL) || undefined,
		});
	}

	/**
	 * Card skills from CONFIGURED toolsets only — the live pi tool-registry
	 * lookup is runner wiring (exclusion note, module head).
	 */
	advertisedSkills(agent?: ServedAgent): Array<Record<string, unknown>> {
		const configured = agent?.advertisedToolsets ?? this.advertisedToolsets;
		return skillsFromToolsets([...configured]);
	}

	servedAgentSummary(
		publicUrl?: string | null,
	): Array<Record<string, unknown>> {
		const base =
			(publicUrl ?? "").trim() || `http://${this.host}:${this.port}/`;
		return [...this.servedAgents.values()].map((a) => ({
			slug: a.slug || "default",
			name: a.name,
			url: joinUrl(base, a.path),
			tenant: a.tenant || null,
			profile: a.profile,
			local: a.local,
		}));
	}

	// ── pending-reply plumbing ──────────────────────────────────────────────────

	private addPending(
		taskId: string,
		contextId: string,
	): Deferred<{ state: string; text: string }> {
		const fut = new Deferred<{ state: string; text: string }>();
		this.pending.set(taskId, {
			taskId,
			contextId,
			peer: "",
			future: fut,
			createdIso: "",
			startedMs: this.nowFn(),
		});
		const order = this.pendingOrder.get(contextId) ?? [];
		order.push(taskId);
		this.pendingOrder.set(contextId, order);
		return fut;
	}

	private popPending(taskId: string): void {
		const entry = this.pending.get(taskId);
		if (!entry) return;
		this.pending.delete(taskId);
		const order = this.pendingOrder.get(entry.contextId);
		if (order) {
			const idx = order.indexOf(taskId);
			if (idx >= 0) order.splice(idx, 1);
			if (order.length === 0) this.pendingOrder.delete(entry.contextId);
		}
	}

	/** Direct future resolution (adapter.py:_resolve_task). */
	resolveTask(taskId: string, state: string, text: string): boolean {
		const entry = this.pending.get(taskId);
		if (entry && !entry.future.done) {
			entry.future.resolve({ state, text });
			return true;
		}
		return false;
	}

	/**
	 * adapter.py:_resolve_oldest_for_context — send() only knows the CONTEXT;
	 * the oldest outstanding task for it receives the reply (no cross-talk
	 * between concurrent requests sharing a context).
	 */
	private resolveOldestForContext(
		contextId: string,
		state: string,
		text: string,
	): boolean {
		for (const taskId of this.pendingOrder.get(contextId) ?? []) {
			const entry = this.pending.get(taskId);
			if (entry && !entry.future.done) {
				entry.future.resolve({ state, text });
				return true;
			}
		}
		return false;
	}

	// ── inbound task handling (adapter.py:_prepare_task @~696 parity) ─────────

	private async prepareTask(
		params: Record<string, unknown>,
		peer: string,
		agent: ServedAgent,
	): Promise<{
		terminal: Record<string, unknown> | null;
		pending:
			| (Omit<PendingReply, "future"> & {
					future: Deferred<{ state: string; text: string }>;
			  })
			| null;
	}> {
		const [slug, tenant] = this.scopeForAgent(agent);
		const text = extractText(params);
		const contextId = extractContextId(params) || newContextId();
		const taskId = newTaskId();

		// Anti-loop ping-pong protection
		const turn = this.turns.track(contextId);
		if (turn > maxPingpongLimit(this.env)) {
			this.metrics.antiLoopTriggers += 1;
			const rec = this.tasks.create(taskId, contextId, peer, slug, tenant);
			this.tasks.complete(taskId, STATE_REJECTED, "");
			return {
				terminal: buildTask(
					taskId,
					contextId,
					STATE_REJECTED,
					`Anti-loop protection: context ${contextId} exceeded ` +
						`${maxPingpongLimit(this.env)} turns. Start a new context or ` +
						`increase A2A_MAX_PINGPONG_TURNS.`,
					{ createdAt: rec.created_iso },
				),
				pending: null,
			};
		}

		if (!text) {
			const rec = this.tasks.create(taskId, contextId, peer, slug, tenant);
			this.tasks.complete(taskId, STATE_REJECTED, "");
			return {
				terminal: buildTask(
					taskId,
					contextId,
					STATE_REJECTED,
					"Empty task — nothing to do.",
					{ createdAt: rec.created_iso },
				),
				pending: null,
			};
		}

		const framed = wrapInbound(peer, text);
		this.audit.append("inbound", peer, taskId, text);
		this.conversations.persistMessage(
			contextId,
			"user",
			text,
			taskId,
			this.clock,
		);
		this.metrics.inboundTotal += 1;

		const rec = this.tasks.create(taskId, contextId, peer, slug, tenant);
		this.registerInlinePush(taskId, params, slug, tenant);

		// Cross-profile forwarding EXCLUDED: the reference would spawn a
		// `hermes chat` child for local:false agents; the port never spawns
		// OS children, so a non-local routed agent completes FAILED with a
		// deterministic notice (DEC-064 scope boundary).
		if (!agent.local) {
			const msg = `[a2a] agent '${slug}' is not locally servable in this deployment`;
			this.tasks.complete(taskId, STATE_FAILED, msg);
			this.metrics.tasksFailed += 1;
			this.audit.append("outbound", peer, taskId, msg);
			return {
				terminal: buildTask(taskId, contextId, STATE_FAILED, msg, {
					createdAt: rec.created_iso,
				}),
				pending: null,
			};
		}

		if (!this.handlerAttached) {
			this.tasks.complete(taskId, STATE_FAILED, "");
			this.metrics.tasksFailed += 1;
			return {
				terminal: buildTask(
					taskId,
					contextId,
					STATE_FAILED,
					"Agent gateway not ready to accept A2A tasks.",
					{ createdAt: rec.created_iso },
				),
				pending: null,
			};
		}

		const future = this.addPending(taskId, contextId);
		const entry = this.pending.get(taskId);
		if (entry) {
			entry.peer = peer;
			entry.createdIso = rec.created_iso;
		}

		const event: IncomingEvent = {
			messageType: "text",
			text: framed,
			messageId: taskId,
			source: {
				platform: A2A_PLUGIN_MANIFEST.name,
				chatType: "dm",
				userId: peer,
				chatId: contextId,
				chatName: `a2a:${peer}`,
			},
		};

		try {
			await this.deliverInbound(event, contextId);
		} catch (err) {
			this.popPending(taskId);
			const msg = redactOutbound(`Dispatch failed: ${String(err)}`);
			this.tasks.complete(taskId, STATE_FAILED, msg);
			this.metrics.tasksFailed += 1;
			return {
				terminal: buildTask(taskId, contextId, STATE_FAILED, msg, {
					createdAt: rec.created_iso,
				}),
				pending: null,
			};
		}

		this.tasks.setState(taskId, STATE_WORKING);
		return {
			terminal: null,
			pending: {
				taskId,
				contextId,
				peer,
				future,
				createdIso: rec.created_iso,
				startedMs: entry?.startedMs ?? this.nowFn(),
			},
		};
	}

	/**
	 * adapter.py:_await_reply — block until the future resolves or the
	 * injected clock passes the deadline. `keepalive` runs on every wake (the
	 * SSE wrapper applies its own cadence); a THROW means the client is gone.
	 */
	private async awaitReply(
		pending: {
			future: Deferred<{ state: string; text: string }>;
			startedMs: number;
		},
		keepalive?: (() => void) | undefined,
	): Promise<{ state: string; text: string }> {
		const deadlineMs = pending.startedMs + replyTimeoutSeconds(this.env) * 1000;
		for (;;) {
			const winner = await raceWithTimer(
				pending.future.future,
				this.pollTickMs,
			);
			if (winner !== RACE_TIMEOUT) return winner;
			if (this.nowFn() >= deadlineMs) {
				return { state: STATE_FAILED, text: "[agent did not reply in time]" };
			}
			if (keepalive !== undefined) {
				try {
					keepalive();
				} catch {
					return { state: STATE_FAILED, text: "[client disconnected]" };
				}
			}
		}
	}

	/**
	 * adapter.py:_finalize_task — record the outcome after redaction and
	 * input-required detection.
	 */
	private async finalizeTask(
		pending: PendingReply,
		state: string,
		reply: string,
	): Promise<{ state: string; reply: string }> {
		const { taskId, contextId, peer } = pending;
		this.popPending(taskId);

		let outState = state;
		let outReply = redactOutbound(reply || "");

		// The agent flags clarification requests with a leading marker; map
		// them to INPUT_REQUIRED so the peer knows to answer (marker stripped).
		if (outState === STATE_COMPLETED) {
			const stripped = outReply.replace(/^\s+/, "");
			if (stripped.toUpperCase().startsWith(INPUT_REQUIRED_MARKER)) {
				outState = STATE_INPUT_REQUIRED;
				outReply = stripped.slice(INPUT_REQUIRED_MARKER.length).trim();
			}
		}

		this.conversations.persistMessage(
			contextId,
			"agent",
			outReply,
			taskId,
			this.clock,
		);
		this.audit.append("outbound", peer, taskId, outReply);

		if (outState === STATE_COMPLETED || outState === STATE_INPUT_REQUIRED) {
			this.metrics.outboundTotal += 1;
			this.metrics.tasksCompleted += 1;
			this.metrics.recordLatency(this.clock() - pending.startedMs / 1000);
		} else {
			this.metrics.tasksFailed += 1;
		}

		this.tasks.complete(taskId, outState, outReply);
		await this.sendPushNotification(taskId, contextId, outReply, outState);
		return { state: outState, reply: outReply };
	}

	// ── message/send ────────────────────────────────────────────────────────────

	private async rpcMessageSend(
		reqId: JsonRpcId,
		params: Record<string, unknown>,
		peer: string,
		agent: ServedAgent,
		info: MethodInfo,
	): Promise<Record<string, unknown>> {
		const prep = await this.prepareTask(params, peer, agent);
		if (prep.terminal !== null) {
			const result = info.isV1
				? sendMessageResponse(prep.terminal)
				: prep.terminal;
			return jsonrpcResult(reqId, result);
		}
		const pending = prep.pending as PendingReply;
		const awaited = await this.awaitReply(pending);
		const finalized = await this.finalizeTask(
			pending,
			awaited.state,
			awaited.text,
		);
		const task = buildTask(
			pending.taskId,
			pending.contextId,
			finalized.state,
			finalized.reply,
			{ createdAt: pending.createdIso },
		);
		const result = info.isV1 ? sendMessageResponse(task) : task;
		return jsonrpcResult(reqId, result);
	}

	// ── streaming (SSE) ─────────────────────────────────────────────────────────

	private sinkOf(input: A2aRequestInput): {
		sink: SseSink;
		buffered(): string;
	} {
		if (input.sseSink !== undefined) {
			return { sink: input.sseSink, buffered: () => "" };
		}
		const chunks: string[] = [];
		return {
			sink: {
				write: (chunk: string) => {
					chunks.push(chunk);
				},
			},
			buffered: () => chunks.join(""),
		};
	}

	private emitTerminal(
		sink: SseSink,
		taskId: string,
		contextId: string,
		state: string,
		reply: string,
		reqId: JsonRpcId,
	): void {
		// v1.0: closure signals terminal state, no `final` field; COMPLETED
		// emits artifact_update THEN the bare status_update.
		if (reply && state === STATE_COMPLETED) {
			this.sseWrite(
				sink,
				sseData(artifactUpdate(taskId, contextId, reply), reqId),
			);
			this.sseWrite(
				sink,
				sseData(statusUpdate(taskId, contextId, state), reqId),
			);
		} else {
			this.sseWrite(
				sink,
				sseData(statusUpdate(taskId, contextId, state, reply), reqId),
			);
		}
		this.sseWrite(sink, sseDone());
	}

	private sseWrite(sink: SseSink, chunk: string): void {
		sink.write(chunk);
	}

	private async rpcMessageStream(
		input: A2aRequestInput,
		reqId: JsonRpcId,
		params: Record<string, unknown>,
		peer: string,
		agent: ServedAgent,
	): Promise<HandlerResponse> {
		const { sink, buffered } = this.sinkOf(input);
		this.metrics.streamsStarted += 1;
		try {
			const prep = await this.prepareTask(params, peer, agent);
			if (prep.terminal !== null) {
				const terminal = prep.terminal;
				const status = terminal["status"] as Record<string, unknown>;
				const messageObj = isRecord(status["message"]) ? status["message"] : {};
				this.emitTerminal(
					sink,
					String(terminal["id"]),
					String(terminal["contextId"]),
					String(status["state"]),
					extractText(messageObj),
					reqId,
				);
				return sseResponse(buffered());
			}

			const pending = prep.pending as PendingReply;
			const { taskId, contextId } = pending;
			this.sseWrite(
				sink,
				sseData(
					streamTask(
						buildTask(taskId, contextId, STATE_SUBMITTED, "", {
							createdAt: pending.createdIso,
						}),
					),
					reqId,
				),
			);
			this.sseWrite(
				sink,
				sseData(statusUpdate(taskId, contextId, STATE_WORKING), reqId),
			);

			let lastKeepaliveAt = this.nowFn();
			const keepaliveWindowMs = A2A_SSE_KEEPALIVE_SECONDS * 1000;
			const awaited = await this.awaitReply(pending, () => {
				// Cadence measured on the INJECTED clock (deterministic rows);
				// wall-clock cadence is identical under the real clock.
				if (this.nowFn() - lastKeepaliveAt < keepaliveWindowMs) return;
				lastKeepaliveAt = this.nowFn();
				this.sseWrite(sink, ": keepalive\n\n");
			});
			const finalized = await this.finalizeTask(
				pending,
				awaited.state,
				awaited.text,
			);
			this.emitTerminal(
				sink,
				taskId,
				contextId,
				finalized.state,
				finalized.reply,
				reqId,
			);
		} catch {
			// BrokenPipeError/ConnectionResetError parity: stream client gone.
		}
		return sseResponse(buffered());
	}

	/** Reconnect to an existing task's stream (v1.0 SubscribeToTask). */
	private async rpcTasksSubscribe(
		input: A2aRequestInput,
		reqId: JsonRpcId,
		params: Record<string, unknown>,
		agent: ServedAgent,
	): Promise<HandlerResponse> {
		const { sink, buffered } = this.sinkOf(input);
		const taskId = String(params["taskId"] ?? params["id"] ?? "");
		const [slug, tenant] = this.scopeForAgent(agent);
		const rec = this.tasks.get(taskId, slug, tenant);
		if (!rec) {
			// Unknown task answers PLAIN JSON-RPC, not an SSE stream.
			return jsonResponse(
				200,
				jsonrpcError(reqId, ERR_TASK_NOT_FOUND, `task not found: ${taskId}`),
			);
		}
		try {
			const fut = this.tasks.watch(taskId, slug, tenant);
			if (fut === null) {
				this.sseWrite(sink, sseDone());
				return sseResponse(buffered());
			}
			const deadlineMs = this.nowFn() + replyTimeoutSeconds(this.env) * 1000;
			const keepaliveWindowMs = A2A_SSE_KEEPALIVE_SECONDS * 1000;
			let lastKeepaliveAt = this.nowFn();
			let state = rec.state;
			let reply = rec.reply;
			for (;;) {
				const winner = await raceWithTimer(fut.future, this.pollTickMs);
				if (winner !== RACE_TIMEOUT) {
					state = winner.state;
					reply = winner.reply;
					break;
				}
				if (this.nowFn() >= deadlineMs) {
					state = rec.state;
					reply = rec.reply;
					break;
				}
				if (this.nowFn() - lastKeepaliveAt >= keepaliveWindowMs) {
					lastKeepaliveAt = this.nowFn();
					this.sseWrite(sink, ": keepalive\n\n");
				}
			}
			this.emitTerminal(sink, taskId, rec.context_id, state, reply, reqId);
		} catch {
			/* subscribe client disconnected */
		}
		return sseResponse(buffered());
	}

	// ── task queries ─────────────────────────────────────────────────────────────

	private rpcTasksGet(
		reqId: JsonRpcId,
		params: Record<string, unknown>,
		agent: ServedAgent,
	): Record<string, unknown> {
		const taskId = String(params["taskId"] ?? params["id"] ?? "");
		const [slug, tenant] = this.scopeForAgent(agent);
		const rec = this.tasks.get(taskId, slug, tenant);
		if (!rec) {
			return jsonrpcError(
				reqId,
				ERR_TASK_NOT_FOUND,
				`task not found: ${taskId}`,
			);
		}
		const historyLen = safeIntOrNull(params["historyLength"]);
		return jsonrpcResult(reqId, TaskStore.toTask(rec, historyLen));
	}

	private rpcTasksList(
		reqId: JsonRpcId,
		params: Record<string, unknown>,
		agent: ServedAgent,
	): Record<string, unknown> {
		const [slug, tenant] = this.scopeForAgent(agent);
		const offset = Math.max(0, safeInt(params["pageToken"], 0));
		const pageSize = safeInt(params["pageSize"], 50);
		const listing = this.tasks.list({
			contextId: String(params["contextId"] ?? ""),
			state: String(params["status"] ?? params["state"] ?? ""),
			pageSize,
			offset,
			agentSlug: slug,
			tenant,
			withTotal: true,
		}) as { records: TaskRecord[]; nextOffset: number; total: number };
		const includeArtifacts = Boolean(params["includeArtifacts"] ?? false);
		const historyLen = safeIntOrNull(params["historyLength"]);
		return jsonrpcResult(reqId, {
			tasks: listing.records.map((r) =>
				TaskStore.toTask(r, historyLen, includeArtifacts),
			),
			nextPageToken: listing.nextOffset ? String(listing.nextOffset) : "",
			pageSize: Math.max(1, Math.min(pageSize, 100)),
			totalSize: listing.total,
		});
	}

	private rpcTasksCancel(
		reqId: JsonRpcId,
		params: Record<string, unknown>,
		agent: ServedAgent,
	): Record<string, unknown> {
		const taskId = String(params["taskId"] ?? params["id"] ?? "");
		const [slug, tenant] = this.scopeForAgent(agent);
		const rec = this.tasks.get(taskId, slug, tenant);
		if (!rec) {
			return jsonrpcError(
				reqId,
				ERR_TASK_NOT_FOUND,
				`task not found: ${taskId}`,
			);
		}
		if (TERMINAL_STATES.has(rec.state)) {
			return jsonrpcError(
				reqId,
				ERR_TASK_NOT_CANCELABLE,
				`task ${taskId} already ${rec.state}`,
			);
		}
		this.tasks.complete(taskId, STATE_CANCELED, "");
		this.turns.reset(rec.context_id);
		this.resolveTask(taskId, STATE_CANCELED, "");
		const fresh = this.tasks.get(taskId, slug, tenant) ?? rec;
		return jsonrpcResult(reqId, TaskStore.toTask(fresh));
	}

	// ── push notifications ───────────────────────────────────────────────────────

	/** v1.0: message/send can carry configuration.taskPushNotificationConfig inline. */
	private registerInlinePush(
		taskId: string,
		params: Record<string, unknown>,
		slug: string,
		tenant: string,
	): void {
		const configuration = params["configuration"];
		if (!isRecord(configuration)) return;
		const cfg = configuration["taskPushNotificationConfig"];
		if (!isRecord(cfg)) return;
		const nested = isRecord(cfg["pushNotificationConfig"])
			? cfg["pushNotificationConfig"]
			: undefined;
		const url = cfg["url"] ?? nested?.["url"] ?? "";
		if (url) this.tasks.setPushConfig(taskId, String(url), slug, tenant);
	}

	private rpcPushConfigCreate(
		reqId: JsonRpcId,
		params: Record<string, unknown>,
		agent: ServedAgent,
	): Record<string, unknown> {
		const taskId = String(params["taskId"] ?? "");
		let cfg: Record<string, unknown> = {};
		if (isRecord(params["pushNotificationConfig"])) {
			cfg = params["pushNotificationConfig"];
		} else if (isRecord(params["config"])) {
			cfg = params["config"];
		}
		const url = String(cfg["url"] ?? "");
		if (!taskId || !url) {
			return jsonrpcError(
				reqId,
				ERR_INVALID_PARAMS,
				"taskId and pushNotificationConfig.url required",
			);
		}
		const [slug, tenant] = this.scopeForAgent(agent);
		const stored = this.tasks.setPushConfig(taskId, url, slug, tenant);
		if (stored === null) {
			return jsonrpcError(
				reqId,
				ERR_TASK_NOT_FOUND,
				`task not found: ${taskId}`,
			);
		}
		return jsonrpcResult(reqId, stored);
	}

	private rpcPushConfigGet(
		reqId: JsonRpcId,
		params: Record<string, unknown>,
		agent: ServedAgent,
	): Record<string, unknown> {
		const taskId = String(params["taskId"] ?? "");
		const configId = String(params["id"] ?? params["configId"] ?? "");
		if (!taskId) {
			return jsonrpcError(reqId, ERR_INVALID_PARAMS, "taskId required");
		}
		const [slug, tenant] = this.scopeForAgent(agent);
		const cfg = this.tasks.getPushConfig(taskId, configId, slug, tenant);
		if (cfg === null) {
			return jsonrpcError(
				reqId,
				ERR_TASK_NOT_FOUND,
				`push config not found for task: ${taskId}`,
			);
		}
		return jsonrpcResult(reqId, cfg);
	}

	private rpcPushConfigList(
		reqId: JsonRpcId,
		params: Record<string, unknown>,
		agent: ServedAgent,
	): Record<string, unknown> {
		const taskId = String(params["taskId"] ?? "");
		if (!taskId) {
			return jsonrpcError(reqId, ERR_INVALID_PARAMS, "taskId required");
		}
		const [slug, tenant] = this.scopeForAgent(agent);
		const configs = this.tasks.listPushConfigs(taskId, slug, tenant);
		return jsonrpcResult(reqId, { configs, nextPageToken: "" });
	}

	private rpcPushConfigDelete(
		reqId: JsonRpcId,
		params: Record<string, unknown>,
		agent: ServedAgent,
	): Record<string, unknown> {
		const taskId = String(params["taskId"] ?? "");
		const configId = String(params["id"] ?? params["configId"] ?? "");
		if (!taskId) {
			return jsonrpcError(reqId, ERR_INVALID_PARAMS, "taskId required");
		}
		const [slug, tenant] = this.scopeForAgent(agent);
		const deleted = this.tasks.deletePushConfig(taskId, configId, slug, tenant);
		if (!deleted) {
			return jsonrpcError(
				reqId,
				ERR_TASK_NOT_FOUND,
				`push config not found for task: ${taskId}`,
			);
		}
		return jsonrpcResult(reqId, { deleted: true });
	}

	/**
	 * POST a v1.0 StreamResponse payload to the task's registered callback —
	 * through the INJECTED PushTransport (NO real network). Unsafe callback
	 * URLs are rejected BEFORE the transport is ever touched (SSRF ladder).
	 */
	private async sendPushNotification(
		taskId: string,
		contextId: string,
		reply: string,
		state: string,
	): Promise<void> {
		const callbackUrl = this.tasks.popPushUrl(taskId);
		if (!callbackUrl) return;

		if (!isSafeCallbackUrl(callbackUrl, this.env)) {
			this.logger?.warn?.(
				`[a2a] push notification for task ${taskId} blocked — unsafe callback URL: ${callbackUrl}`,
			);
			this.metrics.pushFailed += 1;
			return;
		}

		const payload = statusUpdate(
			taskId,
			contextId,
			state,
			(reply || "").slice(0, 2000),
		);
		const signature = signPushPayload(payload, getPushSecret(this.env));
		const headers: Record<string, string> = {
			"Content-Type": "application/json",
		};
		if (signature) headers["X-A2A-Signature"] = signature;

		try {
			const resp = await this.pushTransport.postCallback(
				callbackUrl,
				JSON.stringify(payload),
				headers,
			);
			if (resp.status >= 200 && resp.status < 300) {
				this.metrics.pushSent += 1;
			} else {
				this.metrics.pushFailed += 1;
			}
		} catch {
			this.metrics.pushFailed += 1;
		}
	}

	// ── sending (the agent's reply path) ────────────────────────────────────────

	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		const messageId = String(Math.trunc(this.clock() * 1000));
		// The gateway marks FINAL user-visible replies with metadata.notify;
		// progress/status/editable-preview sends lack the marker and MUST NOT
		// satisfy the JSON-RPC caller (adapter.py:A2AAdapter.send). Every send
		// still traverses the transport seam so conformance rows observe
		// chunk/fallback behavior; only the PENDING-FUTURE resolution is
		// gated on the final-reply marker.
		if (!metadata["notify"]) {
			this.logger?.debug?.(
				`[a2a] ignoring non-final send for context ${chatId}`,
			);
			if (this.captureWire !== undefined) {
				return this.captureWire.transmitSend(chatId, content, metadata);
			}
			return { success: true, messageId };
		}
		let result: SendResult = { success: true, messageId };
		if (this.captureWire !== undefined) {
			result = await this.captureWire.transmitSend(chatId, content, metadata);
		}
		if (!this.resolveOldestForContext(chatId, STATE_COMPLETED, content || "")) {
			// No waiter (e.g. a late chunk or out-of-band send) — dropped.
			this.logger?.debug?.(
				`[a2a] send() for context ${chatId} had no pending waiter`,
			);
		}
		return result;
	}

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
	 * Resolve the task future when processing ends WITHOUT a reply send —
	 * failures/cancellations/empty runs resolve promptly instead of waiting
	 * out the reply timeout (adapter.py:on_processing_complete).
	 */
	onProcessingComplete(messageId: string, outcome: ProcessingOutcome): void {
		const taskId = String(messageId || "");
		if (!taskId) return;
		if (outcome === "failure") {
			this.resolveTask(taskId, STATE_FAILED, "[agent processing failed]");
		} else if (outcome === "cancelled") {
			this.resolveTask(taskId, STATE_CANCELED, "");
		} else {
			this.resolveTask(taskId, STATE_COMPLETED, "");
		}
	}

	// ── orphan watchdog (sweep body; the INTERVAL THREAD is runner wiring) ─────

	sweepOrphans(): string[] {
		const failed = this.tasks.failOrphans(A2A_ORPHAN_TIMEOUT_SECONDS);
		this.metrics.tasksFailed += failed.length;
		return failed;
	}

	// ── guard wiring (reference-fixture inheritance) ────────────────────────────

	attachStandardGuard(
		opts: {
			spawner?: TaskSpawner | undefined;
			/** Gateway-runner reply script (fixtures substitute their own). */
			replyFor?: (framedText: string) => string | undefined;
		} = {},
	): void {
		this.handlerAttached = true;
		this.attachGuard(
			{
				registry: A2A_REGISTRY,
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
					const scripted = opts.replyFor?.(text);
					const reply = scripted ?? `reply:${text}`;
					this.replyLog.push(reply);
					return reply;
				},
				sendReply: async (chatId, text) => {
					// The gateway's FINAL-reply door: notify-marked send resolves
					// the oldest pending task future for this context.
					this.replyLog.push(text);
					await this.send(chatId, text, undefined, {
						notify: true,
					} as unknown as Metadata);
				},
			},
			{
				...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
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
		await this.handleIngress(event, sessionKey);
	}

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	// ── egress doors ─────────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}
}

// ── helpers ───────────────────────────────────────────────────────────────────

function jsonResponse(
	status: number,
	body: Record<string, unknown>,
): HandlerResponse {
	return { status, contentType: "application/json", body };
}

function sseResponse(body: string): HandlerResponse {
	return { status: 200, contentType: "text/event-stream", body };
}

/** adapter.py:_default_agent_name. */
function defaultAgentName(env: EnvReader): string {
	const configured = (env("A2A_AGENT_NAME") ?? "").trim();
	if (configured) return configured;
	try {
		return `hermes-${hostname()}`;
	} catch {
		return "hermes-agent";
	}
}

/** adapter.py:_reply_timeout — seconds to wait for the agent's answer. */
function replyTimeoutSeconds(env: import("./security.js").EnvReader): number {
	const raw = env(A2A_REPLY_TIMEOUT_RULE.envVar);
	if (raw === undefined || raw.trim() === "") {
		return A2A_REPLY_TIMEOUT_RULE.defaultSeconds;
	}
	const parsed = Number.parseFloat(raw);
	if (Number.isNaN(parsed)) return A2A_REPLY_TIMEOUT_RULE.defaultSeconds;
	return Math.max(A2A_REPLY_TIMEOUT_RULE.minSeconds, parsed);
}

/** protocol.py:max_pingpong_turns re-declared locally for adapter use. */
function maxPingpongLimit(env: import("./security.js").EnvReader): number {
	const raw = env("A2A_MAX_PINGPONG_TURNS");
	if (raw === undefined || raw.trim() === "") return DEFAULT_MAX_PINGPONG_TURNS;
	const parsed = Number.parseInt(raw, 10);
	if (Number.isNaN(parsed)) return DEFAULT_MAX_PINGPONG_TURNS;
	return Math.max(1, Math.min(parsed, 20));
}

/**
 * adapter.py:__init__ _advertised_toolsets — configured list first; an EMPTY
 * configured list falls back to the A2A_ADVERTISED_TOOLSETS csv; blank
 * entries are dropped on BOTH lanes (list comprehension `if str(t).strip()`).
 */
function resolveAdvertisedToolsets(
	config: A2aAdapterConfig,
	env: EnvReader,
): readonly string[] {
	const configured = (config.advertised_toolsets ?? [])
		.map((t) => String(t).trim())
		.filter((t) => t.length > 0);
	if (configured.length > 0) return configured;
	return (env(ENV_ADVERTISED_TOOLSETS) ?? "")
		.split(",")
		.map((t) => t.trim())
		.filter((t) => t.length > 0);
}

/** Python truthiness for the served-agent raw operands (None / [] / {}). */
function pyTruthy(
	value:
		| Record<string, ServedAgentConfig>
		| readonly ServedAgentConfig[]
		| undefined,
): boolean {
	if (value === undefined || value === null) return false;
	return Array.isArray(value)
		? value.length > 0
		: Object.keys(value).length > 0;
}

/**
 * adapter.py:_load_served_agents — minimal single-default-agent form plus
 * slug-prefixed entries from extra config. Root/default ALWAYS maps to the
 * live gateway session. Reserved/invalid path segments are SKIPPED with a
 * warning; duplicate tenants are skipped (first wins).
 *
 * Raw-source ladder EXACT (adapter.py:_load_served_agents): extra.agents →
 * extra.served_agents → global cfg.a2a_served_agents → cfg.a2a.served_agents,
 * where each hop is a PYTHON `or` — an empty dict/list operand FALLS THROUGH
 * to the next source, and the global lanes fire only when every extra lane
 * is falsy.
 */
function loadServedAgents(
	config: A2aAdapterConfig,
	ctx: {
		agentName: string;
		description: string;
		host: string;
		port: number;
		toolsets: readonly string[];
	},
	globalConfig?: A2aGlobalConfig | undefined,
): Map<string, ServedAgent> {
	const agents = new Map<string, ServedAgent>();
	agents.set("", {
		slug: "",
		path: "",
		tenant: "",
		profile: "default",
		local: true,
		name: ctx.agentName,
		description: ctx.description,
		advertisedToolsets: ctx.toolsets,
		timeout: A2A_REPLY_TIMEOUT_RULE.defaultSeconds,
	});
	const warnings: string[] = [];
	const seenTenants = new Map<string, string>();
	const raw = pyTruthy(config.agents)
		? config.agents
		: pyTruthy(config.served_agents)
			? config.served_agents
			: pyTruthy(globalConfig?.a2a_served_agents)
				? globalConfig?.a2a_served_agents
				: globalConfig?.a2a?.served_agents;
	const items: Array<[string, ServedAgentConfig]> = [];
	if (raw !== undefined && !Array.isArray(raw) && typeof raw === "object") {
		for (const [key, val] of Object.entries(raw)) {
			items.push([key, val]);
		}
	} else if (Array.isArray(raw)) {
		raw.forEach((val, idx) => {
			items.push([String(idx), val]);
		});
	}
	for (const [key, val] of items) {
		if (val === null || typeof val !== "object") continue;
		const slug = cleanSlug(val.slug ?? val.id ?? key);
		if (!slug) continue;
		const pathSegment = cleanSlug(val.path ?? slug);
		if (!pathSegment || RESERVED_PATH_SEGMENTS.has(pathSegment)) {
			warnings.push(
				`[a2a] ignoring served agent '${slug}' with reserved/invalid path '${pathSegment}'`,
			);
			continue;
		}
		const profile = String(val.profile ?? slug).trim();
		const local = val.local === true || ["", "default"].includes(profile);
		const tenant = String(val.tenant ?? slug).trim();
		if (tenant) {
			const existing = seenTenants.get(tenant);
			if (existing !== undefined) {
				warnings.push(
					`[a2a] ignoring served agent '${slug}' with duplicate tenant '${tenant}' already used by '${existing}'`,
				);
				continue;
			}
			seenTenants.set(tenant, slug);
		}
		agents.set(slug, {
			slug,
			path: `/${pathSegment}`,
			tenant,
			profile: profile || slug,
			local,
			name: String(val.name ?? `Hermes ${slug}`),
			description: String(
				val.description ??
					`Hermes profile '${profile || slug}' exposed over A2A.`,
			),
			advertisedToolsets: (val.advertised_toolsets ?? []).map((t) => String(t)),
			timeout: Math.max(
				1,
				Number(val.timeout ?? A2A_REPLY_TIMEOUT_RULE.defaultSeconds),
			),
		});
	}
	void warnings; // surfaced via logger in production wiring
	return agents;
}
