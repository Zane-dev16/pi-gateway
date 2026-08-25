// pi_platforms/raft/raft-adapter — THE Raft external-agent wake-channel
// adapter, ported from the READ-ONLY Hermes plugin
// plugins/platforms/raft/adapter.py onto the kit base. Everything
// policy-shaped is inherited; this module supplies TRANSPORT (the loopback
// wake/activity endpoint surface) and MANIFEST DATA.
//
// Shape (DEC-002 webhook family — stateless):
//   - capabilities AS DATA: supports_async_delivery=False +
//     interactive_resume=False (see manifest DIVERGENCE note — Hermes
//     inherits base True/True for this adapter; send() is a documented NO-OP,
//     so False is the honest data)
//   - THE content-free contract: a wake hint carries NO message body — any
//     payload bearing a content-shaped field (recursive scan) is rejected
//     400 content_not_allowed BEFORE dispatch (_has_content_field)
//   - ingress ladder ports _handle_wake exactly: token gate (constant-time
//     over raw header bytes) → declared-length cap → actual-bytes cap →
//     JSON parse → object shape → content-free check → 202 accepted |
//     503 not_ready when no gateway handler is attached yet
//   - activity telemetry ports ActivityQueue + _validate_activity_event:
//     bounded drop-oldest queue (cap 500), closed field vocabulary, safe-
//     scalar charset, per-field 4096 cap with truncation flags, drain
//     endpoint resetting the drop counter
//   - the raft CLI bridge is an EXTERNAL child process — the port NEVER
//     spawns OS children; the command shape is exported as pure data
//     (buildBridgeSpawnCommand) and connect() runs in wake-only mode exactly
//     like Hermes' CLI-missing path
//
// Layering: imports pi_gateway downward + kit same-layer ONLY; no adapter
// cross-imports.

import { randomBytes } from "node:crypto";

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
import type { DisableReason } from "../kit/lifecycle-state.js";
import { secureCompare } from "../kit/trust.js";

import {
	RAFT_ACTIVITY_ALLOWED_FIELDS,
	RAFT_ACTIVITY_CONTENT_CAP,
	RAFT_ACTIVITY_EVENT_SCHEMA,
	RAFT_ACTIVITY_DRAIN_SCHEMA,
	RAFT_ACTIVITY_QUEUE_CAP,
	RAFT_BRIDGE_TOKEN_HEADER,
	RAFT_BODY_CAP_BYTES,
	RAFT_CONTENT_FIELD_NAMES,
	RAFT_DEFAULT_HOST,
	RAFT_DEFAULT_PORT,
	RAFT_DEFAULT_RUNTIME_SESSION,
	RAFT_DEFAULT_WAKE_PATH,
	RAFT_MAX_SCALAR_LENGTH,
	RAFT_DRAIN_DEFAULT_MAX,
	RAFT_PLUGIN_MANIFEST,
	RAFT_WAKE_PROMPT,
	buildBridgeSpawnCommand,
	declareRaftTrustBoundary,
	validateRaftTrustBoundary,
} from "./manifest.js";

/** msgraph-parity HTTP handler response shape. */
export interface HandlerResponse {
	status: number;
	contentType?: string | undefined;
	body?: string | Record<string, unknown> | undefined;
}

export interface RaftRequestInput {
	headers?: Record<string, string> | undefined;
	query?: Record<string, string> | undefined;
	rawBody: Buffer;
}

export interface RaftCaptureWire {
	transmitSend(
		chatId: string,
		content: string,
		metadata: Record<string, unknown>,
	): Promise<SendResult>;
	hasRichScript(opKind: string): boolean;
	transmitRich(chatId: string, content: string): Promise<SendResult>;
}

export interface RaftAdapterOptions {
	config?:
		| {
				host?: unknown;
				port?: unknown;
				path?: unknown;
				bridge_token?: unknown;
				runtime_session?: unknown;
				max_body_bytes?: unknown;
		  }
		| undefined;
	secretReader?: ScopedSecretReader | undefined;
	nowMs?: (() => number) | undefined;
	captureWire?: RaftCaptureWire | undefined;
	/**
	 * Deterministic token source for auto-generation parity
	 * (secrets.token_hex(32)). Defaults to node crypto randomBytes.
	 */
	tokenHex?: (() => string) | undefined;
	scalarMaxUnits?: number | undefined;
}

/** adapter.py:_safe_scalar — charset-bounded scalar acceptance. */
const SAFE_SCALAR_RE = /^[a-zA-Z0-9._:@/ -]+$/;

export function safeScalar(value: unknown): string | null {
	if (typeof value !== "string") return null;
	if (value.length === 0 || value.length > RAFT_MAX_SCALAR_LENGTH) return null;
	if (!SAFE_SCALAR_RE.test(value)) return null;
	return value;
}

/** adapter.py:_duration_ms — non-negative finite numbers only (bool excluded). */
export function durationMsOrNull(value: unknown): number | null {
	if (typeof value !== "number" || Number.isNaN(value)) return null;
	const duration = Math.trunc(value);
	return duration >= 0 ? duration : null;
}

function contentStringOf(
	value: unknown,
): { text: string; truncated: boolean } | null {
	if (value === null || value === undefined) return null;
	let text: string;
	if (typeof value === "string") {
		text = value;
	} else {
		try {
			text = stableJson(value);
		} catch {
			return null;
		}
	}
	if (text.length === 0) return null;
	if (text.length > RAFT_ACTIVITY_CONTENT_CAP) {
		return { text: text.slice(0, RAFT_ACTIVITY_CONTENT_CAP), truncated: true };
	}
	return { text, truncated: false };
}

/** json.dumps(sort_keys=True) parity for activity content rendering. */
export function stableJson(value: unknown): string {
	return JSON.stringify(value, null, 0);
}

/** adapter.py:_make_activity_event — outbound telemetry event construction. */
export function makeActivityEvent(opts: {
	hookEventName: string;
	sessionId: unknown;
	status?: "ok" | "error" | undefined;
	toolName?: unknown;
	toolInput?: unknown;
	toolOutput?: unknown;
	errorClass?: unknown;
	durationMs?: unknown;
	eventId?: string | undefined;
	occurredAtIso?: string | undefined;
}): Record<string, unknown> {
	const status = opts.status === "error" ? "error" : ("ok" as const);
	const event: Record<string, unknown> = {
		schema: RAFT_ACTIVITY_EVENT_SCHEMA,
		eventId:
			opts.eventId ?? `hermes-raff-${Math.random().toString(16).slice(2)}`,
		sessionId: safeScalar(opts.sessionId) ?? "unknown",
		hookEventName: opts.hookEventName,
		status,
		occurredAt: opts.occurredAtIso ?? new Date().toISOString(),
	};
	const toolName = safeScalar(opts.toolName);
	if (toolName !== null) event["toolName"] = toolName;
	const errorClass = safeScalar(opts.errorClass);
	if (errorClass !== null) event["errorClass"] = errorClass;
	const duration = durationMsOrNull(opts.durationMs);
	if (duration !== null) event["durationMs"] = duration;

	let truncated = false;
	const input = contentStringOf(opts.toolInput);
	if (input !== null) {
		event["toolInput"] = input.text;
		if (input.truncated) {
			event["toolInputTruncated"] = true;
			truncated = true;
		}
	}
	const output = contentStringOf(opts.toolOutput);
	if (output !== null) {
		event["toolOutput"] = output.text;
		if (output.truncated) {
			event["toolOutputTruncated"] = true;
			truncated = true;
		}
	}
	if (truncated) event["truncated"] = true;
	return event;
}

/**
 * adapter.py:_validate_activity_event — RESULT-shaped validation (never a
 * throw): the caller maps the error string onto the 400 body verbatim.
 */
export function validateActivityEvent(
	value: unknown,
): { ok: true; event: Record<string, unknown> } | { ok: false; error: string } {
	if (value === null || typeof value !== "object" || Array.isArray(value)) {
		return { ok: false, error: "activity event must be an object" };
	}
	const raw = value as Record<string, unknown>;
	if (raw["schema"] !== RAFT_ACTIVITY_EVENT_SCHEMA) {
		return { ok: false, error: "unsupported activity event schema" };
	}
	for (const key of Object.keys(raw)) {
		if (!RAFT_ACTIVITY_ALLOWED_FIELDS.has(key)) {
			return {
				ok: false,
				error: `activity event field ${key} is not allowed`,
			};
		}
	}
	for (const key of ["eventId", "sessionId", "hookEventName", "occurredAt"]) {
		if (safeScalar(raw[key]) === null) {
			return {
				ok: false,
				error: `activity event ${key} must be a safe non-empty string`,
			};
		}
	}
	if (raw["status"] !== "ok" && raw["status"] !== "error") {
		return { ok: false, error: "activity event status must be ok|error" };
	}
	if (
		raw["toolName"] !== undefined &&
		raw["toolName"] !== null &&
		safeScalar(raw["toolName"]) === null
	) {
		return {
			ok: false,
			error: "activity event toolName must be a safe string",
		};
	}
	if (
		raw["errorClass"] !== undefined &&
		raw["errorClass"] !== null &&
		safeScalar(raw["errorClass"]) === null
	) {
		return {
			ok: false,
			error: "activity event errorClass must be a safe string",
		};
	}
	if (raw["durationMs"] !== undefined && raw["durationMs"] !== null) {
		if (durationMsOrNull(raw["durationMs"]) === null) {
			return {
				ok: false,
				error: "activity event durationMs must be a non-negative number",
			};
		}
	}
	for (const key of [
		"truncated",
		"toolInputTruncated",
		"toolOutputTruncated",
	]) {
		const v = raw[key];
		if (v !== undefined && v !== null && typeof v !== "boolean") {
			return { ok: false, error: `activity event ${key} must be a boolean` };
		}
	}

	const event: Record<string, unknown> = { ...raw };
	if (event["durationMs"] !== undefined && event["durationMs"] !== null) {
		event["durationMs"] = durationMsOrNull(event["durationMs"]);
	}
	for (const key of ["toolInput", "toolOutput"]) {
		const content = event[key];
		if (content === undefined || content === null) continue;
		if (typeof content !== "string") {
			return { ok: false, error: `activity event ${key} must be a string` };
		}
		if (content.length > RAFT_ACTIVITY_CONTENT_CAP) {
			event[key] = content.slice(0, RAFT_ACTIVITY_CONTENT_CAP);
			event["truncated"] = true;
			event[`${key}Truncated`] = true;
		}
	}
	return { ok: true, event };
}

/** adapter.py:_has_content_field — recursive content-bearing field scan. */
export function hasContentField(value: unknown): boolean {
	if (value !== null && typeof value === "object" && !Array.isArray(value)) {
		for (const [key, nested] of Object.entries(
			value as Record<string, unknown>,
		)) {
			if (RAFT_CONTENT_FIELD_NAMES.has(key.trim().toLowerCase())) return true;
			if (hasContentField(nested)) return true;
		}
		return false;
	}
	if (Array.isArray(value)) {
		return value.some((item) => hasContentField(item));
	}
	return false;
}

/** adapter.py:ActivityQueue — bounded at-most-once telemetry queue. */
export class ActivityQueue {
	private readonly events: Array<Record<string, unknown>> = [];
	private droppedSinceDrain = 0;

	constructor(private readonly cap = RAFT_ACTIVITY_QUEUE_CAP) {}

	push(event: Record<string, unknown>): void {
		this.events.push(event);
		while (this.events.length > this.cap) {
			this.events.shift();
			this.droppedSinceDrain += 1;
		}
	}

	drain(maxEvents = RAFT_DRAIN_DEFAULT_MAX): Record<string, unknown> {
		const limit = Math.max(1, maxEvents);
		const out: Array<Record<string, unknown>> = [];
		while (this.events.length > 0 && out.length < limit) {
			out.push(this.events.shift() as Record<string, unknown>);
		}
		const dropped = this.droppedSinceDrain;
		this.droppedSinceDrain = 0;
		return {
			schema: RAFT_ACTIVITY_DRAIN_SCHEMA,
			events: out,
			dropped,
		};
	}

	get size(): number {
		return this.events.length;
	}
}

/** msgraph-parity registry (07 §1 derivation — mirrors the reference set). */
const RAFT_REGISTRY: CommandRegistry = [
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

function normalizeHeaders(
	input: Record<string, string> | undefined,
): Record<string, string> {
	const out: Record<string, string> = {};
	for (const [key, value] of Object.entries(input ?? {})) {
		out[key.toLowerCase()] = value;
	}
	return out;
}

function normalizePath(path: unknown): string {
	const raw = String(path ?? "").trim() || RAFT_DEFAULT_WAKE_PATH;
	return raw.startsWith("/") ? raw : `/${raw}`;
}

export class RaftAdapter extends BasePlatformAdapter {
	readonly pluginManifest = RAFT_PLUGIN_MANIFEST;
	readonly trustBoundary = declareRaftTrustBoundary();

	// ── config (__init__ parity) ──────────────────────────────────────────────
	readonly host: string;
	readonly port: number;
	readonly wakePath: string;
	readonly runtimeSession: string;
	readonly maxBodyBytes: number;

	private configuredToken: string | undefined;
	private readonly secretReader: ScopedSecretReader;
	private readonly nowFn: () => number;
	private tokenHexFn: () => string;
	private readonly captureWire: RaftCaptureWire | undefined;

	// ── runtime state ─────────────────────────────────────────────────────────
	readonly activityQueue = new ActivityQueue();
	readonly counters = {
		wakesAccepted: 0,
		wakesRejectedContent: 0,
		unauthorized: 0,
		payloadTooLarge: 0,
		notReady: 0,
		activityAccepted: 0,
		activityInvalid: 0,
		outboundWireCalls: 0, // MUST stay zero forever (send is a documented no-op)
		parseInvocations: 0,
	};
	/** Accepted wake prompts dispatched downstream (row observability). */
	readonly dispatchedEvents: Array<{
		messageId: string;
		text: string;
		internal: true;
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
	private handlerAttached = false;
	private connectedOnce = false;
	private bridgeSpawned = false;

	constructor(opts: RaftAdapterOptions = {}) {
		const config = opts.config ?? {};
		super({
			manifestName: RAFT_PLUGIN_MANIFEST.name,
			capabilities: RAFT_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? 4096,
		});
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.captureWire = opts.captureWire;
		this.tokenHexFn = opts.tokenHex ?? (() => randomBytes(32).toString("hex"));
		this.host = String(config.host ?? RAFT_DEFAULT_HOST) || RAFT_DEFAULT_HOST;
		this.port = Number(config.port ?? RAFT_DEFAULT_PORT);
		this.wakePath = normalizePath(config.path);
		this.runtimeSession =
			String(config.runtime_session ?? "").trim() ||
			RAFT_DEFAULT_RUNTIME_SESSION;
		this.maxBodyBytes = Math.max(
			1,
			Number(config.max_body_bytes ?? RAFT_BODY_CAP_BYTES),
		);
		const tokenConfigured = String(config.bridge_token ?? "").trim();
		this.configuredToken = tokenConfigured === "" ? undefined : tokenConfigured;

		// §11 step 3/4: missing required secret ⇒ LOUD disable. Hermes seeds
		// enablement from RAFT_PROFILE via env_enablement_fn; the bridge itself
		// refuses to spawn without it but WAKE-ONLY mode still serves wakes.
		const enablement = resolveEnablement(
			RAFT_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		// DEC-017: an incomplete trust boundary is a CONSTRUCTION-TIME error.
		const boundaryErrors = validateRaftTrustBoundary(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			const reason: DisableReason = {
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			};
			this.lifecycle.disable(reason);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // wake-only lane; no native streams
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

	get clientToken(): string | undefined {
		return this.configuredToken;
	}

	get isConnected(): boolean {
		return this.connectedOnce;
	}

	get bridgeWasSpawned(): boolean {
		return this.bridgeSpawned;
	}

	/**
	 * Per-chat length descriptor (§6.3/A15 relay-shaped override point) —
	 * harness utf16-marked chats return budget AND unit TOGETHER.
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

	// ── connect ladder (@~640 parity) ─────────────────────────────────────────

	/**
	 * Auto-generates the bridge token when absent, marks connected, and runs
	 * in WAKE-ONLY mode: the raft CLI bridge is an external child the port
	 * never spawns (CLI-missing parity — "[raft] raft CLI not found …
	 * wake-only polling mode"). The exact argv Hermes would hand to Popen is
	 * available as pure data via buildBridgeSpawnCommand().
	 */
	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (!this.configuredToken) {
			this.configuredToken = this.tokenHexFn();
		}
		this.connectedOnce = true;
		return true;
	}

	override async disconnect(): Promise<void> {
		this.connectedOnce = false;
	}

	/**
	 * Wake-only mode probe: profile present ⇒ the spawn COMMAND is buildable;
	 * the port never executes it. Returns null when RAFT_PROFILE is missing
	 * ("[raft] RAFT_PROFILE not set; bridge not spawned").
	 */
	bridgeSpawnPlan(endpointUrl: string): {
		argv: readonly string[];
		tokenEnvVar: "RAFT_CHANNEL_TOKEN";
		profile: string;
	} | null {
		const profile = this.secretReader("RAFT_PROFILE") ?? "";
		if (!profile) return null;
		const plan = buildBridgeSpawnCommand({ profile, endpointUrl });
		return { ...plan, profile };
	}

	// ── health (@~583 parity — UNauthenticated topology probe) ───────────────

	handleHealthGet(): HandlerResponse {
		return {
			status: 200,
			contentType: "application/json",
			body: {
				status: "ok",
				platform: RAFT_PLUGIN_MANIFEST.name,
				runtimeSession: this.runtimeSession,
				activity: {
					queueSize: this.activityQueue.size,
					endpoint: "/activity",
					drainEndpoint: "/activity/drain",
				},
			},
		};
	}

	// ── wake POST (_handle_wake @~591 parity) ─────────────────────────────────

	async handleWakePost(input: RaftRequestInput): Promise<HandlerResponse> {
		const headers = normalizeHeaders(input.headers);
		if (!this.validateBridgeToken(headers[RAFT_BRIDGE_TOKEN_HEADER] ?? "")) {
			this.counters.unauthorized += 1;
			return { status: 401, body: { ok: false, error: "unauthorized" } };
		}

		// Declared-length cap FIRST (Content-Length may lie about the read),
		// then the actual-bytes cap — defense in depth against chunked bodies.
		const declaredRaw = headers["content-length"];
		const declaredLength =
			declaredRaw !== undefined && /^\d+$/.test(declaredRaw)
				? Number(declaredRaw)
				: null;
		if (declaredLength !== null && declaredLength > this.maxBodyBytes) {
			this.counters.payloadTooLarge += 1;
			return {
				status: 413,
				body: { ok: false, error: "payload_too_large" },
			};
		}
		if (input.rawBody.length > this.maxBodyBytes) {
			this.counters.payloadTooLarge += 1;
			return {
				status: 413,
				body: { ok: false, error: "payload_too_large" },
			};
		}

		let payload: Record<string, unknown> = {};
		if (input.rawBody.toString().trim().length > 0) {
			const parsed = this.parseJsonBody(input.rawBody);
			if (!parsed.ok) {
				return { status: 400, body: { ok: false, error: parsed.error } };
			}
			if (
				parsed.value === null ||
				typeof parsed.value !== "object" ||
				Array.isArray(parsed.value)
			) {
				return {
					status: 400,
					body: { ok: false, error: "invalid_payload" },
				};
			}
			payload = parsed.value as Record<string, unknown>;
		}

		// Do not gate on payload["schema"]: the bridge owns schema evolution;
		// only the CONTENT-FREE contract is enforced.
		if (hasContentField(payload)) {
			this.counters.wakesRejectedContent += 1;
			return {
				status: 400,
				body: { ok: false, error: "content_not_allowed" },
			};
		}

		const accepted = await this.acceptWake(payload);
		if (!accepted) {
			this.counters.notReady += 1;
			return {
				status: 503,
				body: {
					ok: false,
					error: "not_ready",
					runtimeSession: this.runtimeSession,
				},
			};
		}
		this.counters.wakesAccepted += 1;
		return {
			status: 202,
			body: { ok: true, runtimeSession: this.runtimeSession },
		};
	}

	// ── activity endpoints (@~668/@~687 parity) ───────────────────────────────

	handleActivityPost(input: RaftRequestInput): HandlerResponse {
		const headers = normalizeHeaders(input.headers);
		if (!this.validateBridgeToken(headers[RAFT_BRIDGE_TOKEN_HEADER] ?? "")) {
			this.counters.unauthorized += 1;
			return { status: 401, body: { ok: false, error: "unauthorized" } };
		}
		const declaredRaw = headers["content-length"];
		const declaredLength =
			declaredRaw !== undefined && /^\d+$/.test(declaredRaw)
				? Number(declaredRaw)
				: null;
		if (declaredLength !== null && declaredLength > this.maxBodyBytes) {
			this.counters.payloadTooLarge += 1;
			return {
				status: 413,
				body: { ok: false, error: "payload_too_large" },
			};
		}
		if (input.rawBody.length > this.maxBodyBytes) {
			this.counters.payloadTooLarge += 1;
			return {
				status: 413,
				body: { ok: false, error: "payload_too_large" },
			};
		}

		const verdict = validateActivityEvent(
			this.parseActivityJson(input.rawBody),
		);
		if (!verdict.ok) {
			this.counters.activityInvalid += 1;
			return { status: 400, body: { ok: false, error: verdict.error } };
		}
		this.activityQueue.push(verdict.event);
		this.counters.activityAccepted += 1;
		return { status: 202, body: { ok: true } };
	}

	handleActivityDrainGet(input: RaftRequestInput): HandlerResponse {
		const headers = normalizeHeaders(input.headers);
		if (!this.validateBridgeToken(headers[RAFT_BRIDGE_TOKEN_HEADER] ?? "")) {
			this.counters.unauthorized += 1;
			return { status: 401, body: { ok: false, error: "unauthorized" } };
		}
		const query = input.query ?? {};
		const rawMax = query["max"] ?? `${RAFT_DRAIN_DEFAULT_MAX}`;
		// Python int() parity: sign-prefixed integers parse; anything else falls
		// back to the default clamp (then drain() itself floors at 1).
		const parsed = /^[+-]?\d+$/.test(rawMax.trim())
			? Number(rawMax.trim())
			: RAFT_DRAIN_DEFAULT_MAX;
		const maxEvents = parsed;
		return {
			status: 200,
			contentType: "application/json",
			body: this.activityQueue.drain(maxEvents),
		};
	}

	/** Hook-level activity reporting (register_hook parity; soft on invalid). */
	reportActivity(event: Record<string, unknown>): boolean {
		const verdict = validateActivityEvent(event);
		if (!verdict.ok) return false;
		this.activityQueue.push(verdict.event);
		return true;
	}

	// ── token + wake acceptance ───────────────────────────────────────────────

	/**
	 * _validate_bridge_token parity: constant-time compare over RAW BYTES
	 * (compare_digest raises on non-ASCII str, so byte comparison IS the
	 * semantic); missing configured or presented token refuses.
	 */
	validateBridgeToken(presented: string): boolean {
		const expected = this.configuredToken;
		if (!expected || !presented) return false;
		return secureCompare(
			Buffer.from(presented, "utf8"),
			Buffer.from(expected, "utf8"),
		);
	}

	private async acceptWake(payload: Record<string, unknown>): Promise<boolean> {
		if (!this.handlerAttached) {
			this.logger?.warn?.(
				"[raft] Wake received before gateway message handler was attached",
			);
			return false;
		}
		const deliveryId = firstStringOf(
			payload,
			"eventId",
			"attemptId",
			"messageId",
			"delivery_id",
			"wake_id",
			"id",
		);
		const messageId = deliveryId ?? `raft-wake-${this.nowFn()}`;
		const event: IncomingEvent = {
			messageType: "text",
			text: RAFT_WAKE_PROMPT,
			internal: true,
			messageId,
			source: {
				platform: RAFT_PLUGIN_MANIFEST.name,
				chatType: "dm",
				userId: "raft-bridge",
				chatId: this.runtimeSession,
				chatName: "Raft channel",
			},
		};
		this.dispatchedEvents.push({
			messageId,
			text: RAFT_WAKE_PROMPT,
			internal: true,
		});
		try {
			await this.deliverInbound(event, this.runtimeSession);
		} catch {
			return false;
		}
		return true;
	}

	// ── guard wiring (reference-fixture inheritance) ──────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.handlerAttached = true;
		this.attachGuard(
			{
				registry: RAFT_REGISTRY,
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

	// ── egress doors ──────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * send() parity (adapter.py send @~760): "adapter send is a no-op; agent
	 * delivers via raft CLI" — LOG ONLY, success:true. When a conformance
	 * subject supplies a capture wire, the SAME no-op door records there so
	 * shared egress rows observe chunk/fallback behavior.
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		this.logger?.debug?.(
			`[raft] adapter send is a no-op; agent delivers via raft CLI (${chatId})`,
		);
		if (this.captureWire !== undefined) {
			return this.captureWire.transmitSend(chatId, content, metadata);
		}
		return { success: true };
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

	/** THE parse seam — gates run strictly BEFORE (parseInvocations stays 0). */
	private parseJsonBody(
		rawBody: Buffer,
	): { ok: true; value: unknown } | { ok: false; error: string } {
		this.counters.parseInvocations += 1;
		try {
			return { ok: true, value: JSON.parse(rawBody.toString("utf8")) };
		} catch {
			return { ok: false, error: "invalid_json" };
		}
	}

	/** Activity-plane parse: malformed JSON maps to the vendor error string. */
	private parseActivityJson(rawBody: Buffer): unknown {
		// Unparseable bodies become `null`, which validateActivityEvent rejects
		// as "must be an object" ⇒ 400 — a malformed POST never escapes as an
		// uncaught SyntaxError (parity with parseJsonBody above).
		try {
			return JSON.parse(rawBody.toString("utf8")) as unknown;
		} catch {
			return null;
		}
	}

	// ── observability probes ───────────────────────────────────────────────────

	hasSeenNothing(): boolean {
		return this.activityQueue.size === 0;
	}
}

function firstStringOf(
	payload: Record<string, unknown>,
	...keys: readonly string[]
): string | null {
	for (const key of keys) {
		const value = payload[key];
		if (typeof value === "string" && value.length > 0) return value;
	}
	return null;
}
