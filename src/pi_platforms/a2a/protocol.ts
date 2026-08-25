// pi_platforms/a2a/protocol — A2A protocol helpers ported from the READ-ONLY
// Hermes plugins/platforms/a2a/protocol.py: Agent Card construction,
// JSON-RPC framing, tolerant text extraction, task-store, anti-loop turn
// tracking, sliding-window rate limiting, metrics, and disk-backed
// conversation persistence.
//
// Wire shape (source module docstring): A2A Protocol v1.0, JSON-RPC 2.0
// binding over HTTP. StreamResponses are discriminated by MEMBER PRESENCE;
// task states / message roles are v1.0 SCREAMING_SNAKE_CASE enums; Parts are
// the v1.0 unified shape discriminated by member presence (no `kind`).
// extract_text stays tolerant of v0.3 peers.

import { randomBytes } from "node:crypto";
import {
	existsSync,
	mkdirSync,
	appendFileSync,
	readFileSync,
	readdirSync,
} from "node:fs";
import path from "node:path";

import {
	CONTEXT_ID_PREFIX,
	DEFAULT_MAX_PINGPONG_TURNS,
	HARD_MAX_PINGPONG_TURNS,
	MAX_PINGPONG_ENV,
	PROTOCOL_VERSION,
	PUSH_CONFIG_ID_PREFIX,
	RATE_LIMIT_DEFAULT_PER_MINUTE,
	RATE_LIMIT_ENV,
	RATE_WINDOW_SECONDS,
	ROLE_AGENT,
	STATE_COMPLETED,
	STATE_FAILED,
	STATE_SUBMITTED,
	TASK_ID_PREFIX,
	TERMINAL_STATES,
	TERMINAL_TRIM,
	TURN_TTL_SECONDS,
} from "./manifest.js";
import type { EnvReader } from "./security.js";

/** Injected wall clock in FLOAT SECONDS (time.time() parity). */
export type SecondsClock = () => number;

export function secondsClockOf(nowMs: () => number): SecondsClock {
	return () => nowMs() / 1000;
}

/** protocol.py:max_pingpong_turns — env clamp [1, HARD_MAX]. */
export function maxPingpongTurns(env: EnvReader): number {
	const raw = env(MAX_PINGPONG_ENV);
	if (raw === undefined || raw.trim() === "") return DEFAULT_MAX_PINGPONG_TURNS;
	const parsed = Number.parseInt(raw, 10);
	if (Number.isNaN(parsed)) return DEFAULT_MAX_PINGPONG_TURNS;
	return Math.max(1, Math.min(parsed, HARD_MAX_PINGPONG_TURNS));
}

/** protocol.py:now_iso — ISO 8601 UTC with millisecond precision + Z. */
export function nowIso(atMs?: number): string {
	return new Date(atMs ?? Date.now()).toISOString();
}

// ── Agent Card (v1.0) ────────────────────────────────────────────────────────

export interface AgentCardOptions {
	name: string;
	url: string;
	description: string;
	skills?: Array<Record<string, unknown>> | null | undefined;
	streaming?: boolean | undefined;
	pushNotifications?: boolean | undefined;
	authRequired?: boolean | undefined;
	tenant?: string | undefined;
	providerOrg?: string | undefined;
	providerUrl?: string | undefined;
}

/**
 * protocol.py:build_agent_card — v1.0 card document. `tenant` is the
 * optional multi-tenancy routing key advertised on supportedInterfaces; when
 * present clients MUST echo it in request params.
 */
export function buildAgentCard(
	opts: AgentCardOptions,
): Record<string, unknown> {
	const iface: Record<string, unknown> = {
		url: opts.url,
		protocolBinding: "JSONRPC",
		protocolVersion: PROTOCOL_VERSION,
	};
	if (opts.tenant) iface["tenant"] = opts.tenant;

	const card: Record<string, unknown> = {
		name: opts.name,
		description: opts.description,
		// convenience for pre-1.0 clients; canonical is supportedInterfaces
		url: opts.url,
		version: "1.0.0",
		provider: {
			organization: opts.providerOrg ?? "Hermes Agent",
			url: opts.providerUrl || opts.url,
		},
		supportedInterfaces: [iface],
		capabilities: {
			streaming: opts.streaming ?? false,
			pushNotifications: opts.pushNotifications ?? false,
			stateTransitionHistory: false,
			extendedAgentCard: false,
		},
		defaultInputModes: ["text/plain"],
		defaultOutputModes: ["text/plain"],
		skills: opts.skills ?? [],
	};
	if (opts.authRequired) {
		card["securitySchemes"] = { bearer: { type: "http", scheme: "bearer" } };
		card["security"] = [{ bearer: [] }];
	}
	return card;
}

/**
 * protocol.py:skills_from_toolsets — accepts a plain list of toolset names
 * or a mapping toolset → tool names (tool names become tags so peers can
 * match tasks to us); falls back to the single "general" skill.
 */
export function skillsFromToolsets(
	toolsets:
		| readonly string[]
		| Record<string, readonly string[]>
		| null
		| undefined,
): Array<Record<string, unknown>> {
	const skills: Array<Record<string, unknown>> = [];
	if (
		!Array.isArray(toolsets) &&
		toolsets !== null &&
		typeof toolsets === "object"
	) {
		const mapping = toolsets as Record<string, readonly string[]>;
		for (const tsName of Object.keys(mapping).sort()) {
			const toolNames = (mapping[tsName] ?? []).map((t) => String(t));
			skills.push({
				id: `toolset.${tsName}`,
				name: tsName,
				description: `Hermes '${tsName}' capabilities`,
				tags: [tsName, ...toolNames.slice(0, 10)],
			});
		}
	} else if (Array.isArray(toolsets)) {
		for (const ts of [...new Set(toolsets)].sort()) {
			skills.push({
				id: `toolset.${ts}`,
				name: ts,
				description: `Hermes '${ts}' capabilities`,
				tags: [ts],
			});
		}
	}
	if (skills.length === 0) {
		skills.push({
			id: "general",
			name: "general",
			description: "General-purpose conversational agent",
			tags: ["general"],
		});
	}
	return skills;
}

// ── JSON-RPC framing ─────────────────────────────────────────────────────────

export type JsonRpcId = unknown; // any JSON value incl null

export function jsonrpcResult(
	reqId: JsonRpcId,
	result: unknown,
): Record<string, unknown> {
	return { jsonrpc: "2.0", id: reqId, result };
}

export function jsonrpcError(
	reqId: JsonRpcId,
	code: number,
	message: string,
): Record<string, unknown> {
	return { jsonrpc: "2.0", id: reqId, error: { code, message } };
}

/**
 * protocol.py:send_message_response — v1.0 SendMessageResponse oneof wrapper:
 * exactly one of `task` or `message`. Legacy methods still return bare
 * payloads for compatibility.
 */
export function sendMessageResponse(
	payload: Record<string, unknown>,
): Record<string, unknown> {
	const status = (payload as Record<string, unknown>)["status"];
	const id = (payload as Record<string, unknown>)["id"];
	if (status && id) return { task: payload };
	return { message: payload };
}

/** protocol.py:unwrap_send_message_response — legacy payloads pass through. */
export function unwrapSendMessageResponse(result: unknown): unknown {
	if (result !== null && typeof result === "object") {
		const rec = result as Record<string, unknown>;
		if (rec["task"] !== null && typeof rec["task"] === "object")
			return rec["task"];
		if (rec["message"] !== null && typeof rec["message"] === "object") {
			return rec["message"];
		}
	}
	return result;
}

/** protocol.py:stream_task — v1.0 StreamResponse with a task member. */
export function streamTask(
	task: Record<string, unknown>,
): Record<string, unknown> {
	return { task };
}

/** protocol.py:stream_message — v1.0 StreamResponse with a message member. */
export function streamMessage(
	message: Record<string, unknown>,
): Record<string, unknown> {
	return { message };
}

export function newTaskId(): string {
	return TASK_ID_PREFIX + randomBytes(8).toString("hex");
}

export function newContextId(): string {
	return CONTEXT_ID_PREFIX + randomBytes(8).toString("hex");
}

/** protocol.py:text_part — v1.0 Part, member-presence discrimination. */
export function textPart(text: string): Record<string, unknown> {
	return { text, mediaType: "text/plain" };
}

export function filePart(
	opts: {
		url?: string | undefined;
		raw?: string | undefined;
		filename?: string | undefined;
		mediaType?: string | undefined;
	} = {},
): Record<string, unknown> {
	const part: Record<string, unknown> = {
		mediaType: opts.mediaType ?? "application/octet-stream",
	};
	if (opts.filename) part["filename"] = opts.filename;
	if (opts.url) part["url"] = opts.url;
	else if (opts.raw) part["raw"] = opts.raw;
	return part;
}

export function dataPart(
	data: unknown,
	mediaType = "application/json",
): Record<string, unknown> {
	return { data, mediaType };
}

export function textMessage(
	role: string,
	text: string,
	contextId = "",
): Record<string, unknown> {
	const msg: Record<string, unknown> = {
		role,
		parts: [textPart(text)],
		messageId: randomBytes(16).toString("hex"),
	};
	if (contextId) msg["contextId"] = contextId;
	return msg;
}

export function messageWithParts(
	role: string,
	parts: Array<Record<string, unknown>>,
	contextId = "",
): Record<string, unknown> {
	const msg: Record<string, unknown> = {
		role,
		parts,
		messageId: randomBytes(16).toString("hex"),
	};
	if (contextId) msg["contextId"] = contextId;
	return msg;
}

function isRecord(value: unknown): value is Record<string, unknown> {
	return value !== null && typeof value === "object" && !Array.isArray(value);
}

function fileLabel(part: Record<string, unknown>): string {
	const fname = part["filename"] ?? part["name"];
	return typeof fname === "string" && fname ? `[file: ${fname}]` : "[file]";
}

/**
 * protocol.py:extract_text — TOLERANT concatenation ladder:
 *   1. v1.0 text member presence (any part carrying a string `text`)
 *   2. v0.3 kind:"text"
 *   3. v1.0 file part with URL → "[file: name] url (mime)"
 *   4. v0.3 nested file.fileWithUri
 *   5. raw base64 file part → noted, NOT decoded
 *   6. data parts JSON-rendered (v1.0 + kind:"data")
 *   then "\n".join(chunks).strip().
 */
export function extractText(messageOrParams: Record<string, unknown>): string {
	const rawMsg = messageOrParams["message"] ?? messageOrParams;
	const parts =
		isRecord(rawMsg) && Array.isArray(rawMsg["parts"]) ? rawMsg["parts"] : [];
	const chunks: string[] = [];
	for (const part of parts) {
		if (!isRecord(part)) continue;
		// v1.0 text part (member-presence discrimination)
		const txt = part["text"];
		if (typeof txt === "string") {
			chunks.push(txt);
			continue;
		}
		// v0.3 compatibility: kind == "text"
		if (part["kind"] === "text" && typeof part["text"] === "string") {
			chunks.push(part["text"]);
			continue;
		}
		// v1.0 file part with URL
		const url = part["url"];
		if (typeof url === "string" && url) {
			const mtype = part["mediaType"] ?? part["mimeType"] ?? "";
			chunks.push(
				`${fileLabel(part)} ${url}` + (mtype ? ` (${String(mtype)})` : ""),
			);
			continue;
		}
		// v0.3 file part with nested file.fileWithUri
		const v03File = part["file"];
		if (isRecord(v03File) && typeof v03File["fileWithUri"] === "string") {
			const uri = v03File["fileWithUri"];
			const fname = v03File["name"];
			const mtype = v03File["mimeType"];
			const label =
				typeof fname === "string" && fname ? `[file: ${fname}]` : "[file]";
			chunks.push(`${label} ${uri}` + (mtype ? ` (${String(mtype)})` : ""));
			continue;
		}
		// v1.0 file part with raw bytes (base64) — note but don't decode
		const raw = part["raw"];
		if (typeof raw === "string") {
			const mtype = part["mediaType"] ?? "";
			const sizeNote = `${raw.length} bytes base64-encoded`;
			chunks.push(
				`${fileLabel(part)} ${sizeNote}` + (mtype ? ` (${String(mtype)})` : ""),
			);
			continue;
		}
		// v1.0 data part — include JSON content
		const data = part["data"];
		if (data !== undefined && data !== null) {
			let rendered: string;
			try {
				rendered = JSON.stringify(data) ?? String(data);
			} catch {
				rendered = String(data);
			}
			const mtype = part["mediaType"] ?? "application/json";
			chunks.push(`[data (${String(mtype)})]\n${rendered}`);
			continue;
		}
		// v0.3 data part: kind == "data"
		if (part["kind"] === "data" && part["data"] != null) {
			let rendered: string;
			try {
				rendered = JSON.stringify(part["data"]) ?? String(part["data"]);
			} catch {
				rendered = String(part["data"]);
			}
			chunks.push(`[data]\n${rendered}`);
		}
	}
	return chunks.join("\n").trim();
}

/** protocol.py:extract_context_id — message.contextId first, top-level fallback. */
export function extractContextId(params: Record<string, unknown>): string {
	const msg = params["message"] ?? {};
	let ctx = "";
	if (isRecord(msg)) ctx = String(msg["contextId"] ?? "");
	return ctx || String(params["contextId"] ?? "");
}

/**
 * protocol.py:build_task — v1.0 Task object for a message/send result.
 * `createdAt` is accepted for call-site compatibility but NOT serialized —
 * the A2A v1.0 Task proto has no createdAt/lastModified field and strict
 * ProtoJSON parsers reject unknown fields. Artifacts appear ONLY on
 * TASK_STATE_COMPLETED; status.message only when agent text is non-empty.
 */
export function buildTask(
	taskId: string,
	contextId: string,
	state: string,
	agentText = "",
	_opts: { createdAt?: string | undefined } = {},
): Record<string, unknown> {
	const now = nowIso();
	const status: Record<string, unknown> = { state, timestamp: now };
	const task: Record<string, unknown> = {
		id: taskId,
		contextId,
		status,
	};
	if (agentText) {
		status["message"] = textMessage(ROLE_AGENT, agentText, contextId);
		if (state === STATE_COMPLETED) {
			task["artifacts"] = [
				{
					artifactId: randomBytes(16).toString("hex"),
					parts: [textPart(agentText)],
				},
			];
		}
	}
	return task;
}

// ── Streaming (v1.0 StreamResponse events) ───────────────────────────────────

export function statusUpdate(
	taskId: string,
	contextId: string,
	state: string,
	text = "",
): Record<string, unknown> {
	const status: Record<string, unknown> = { state, timestamp: nowIso() };
	if (text) status["message"] = textMessage(ROLE_AGENT, text, contextId);
	return {
		statusUpdate: { taskId, contextId, status },
	};
}

export function artifactUpdate(
	taskId: string,
	contextId: string,
	text: string,
): Record<string, unknown> {
	return {
		artifactUpdate: {
			taskId,
			contextId,
			artifact: {
				artifactId: randomBytes(16).toString("hex"),
				parts: [textPart(text)],
			},
		},
	};
}

/**
 * protocol.py:sse_data — each SSE frame is a FULL JSON-RPC response
 * ({"jsonrpc":"2.0","id":…,"result":{StreamResponse}}) per v1.0 §9.4; a bare
 * StreamResponse breaks JSON-RPC clients incl the official a2a-sdk.
 */
export function sseData(
	payload: Record<string, unknown>,
	reqId?: JsonRpcId,
): string {
	const envelope =
		reqId !== undefined && reqId !== null
			? jsonrpcResult(reqId, payload)
			: payload; // legacy/fallback — no envelope
	return `data: ${JSON.stringify(envelope)}\n\n`;
}

/**
 * protocol.py:sse_done — stream-closure marker as an SSE COMMENT (ignored by
 * all SSE parsers); emitting `data: {}` would make JSON-RPC clients try to
 * parse an empty response.
 */
export function sseDone(): string {
	return ": done\n\n";
}

// ── Anti-loop ping-pong protection ───────────────────────────────────────────

/**
 * protocol.py:TurnTracker — counts inbound turns per context_id to stop
 * infinite agent↔agent loops; prunes contexts idle > TURN_TTL_SECONDS on
 * every track(). Clock INJECTED (deterministic rows).
 */
export class TurnTracker {
	private readonly counts = new Map<string, number>();
	private readonly timestamps = new Map<string, number>();

	constructor(private readonly clock: SecondsClock) {}

	track(contextId: string): number {
		const now = this.clock();
		for (const [cid, ts] of this.timestamps) {
			if (now - ts > TURN_TTL_SECONDS) {
				this.counts.delete(cid);
				this.timestamps.delete(cid);
			}
		}
		this.counts.set(contextId, (this.counts.get(contextId) ?? 0) + 1);
		this.timestamps.set(contextId, now);
		return this.counts.get(contextId) ?? 1;
	}

	reset(contextId: string): void {
		this.counts.delete(contextId);
		this.timestamps.delete(contextId);
	}
}

// ── Rate limiting (sliding window per authenticated identity) ────────────────

function rateLimitPerMinute(env: EnvReader): number {
	const raw = env(RATE_LIMIT_ENV);
	if (raw === undefined || raw.trim() === "")
		return RATE_LIMIT_DEFAULT_PER_MINUTE;
	const parsed = Number.parseInt(raw, 10);
	if (Number.isNaN(parsed)) return RATE_LIMIT_DEFAULT_PER_MINUTE;
	return Math.max(1, parsed);
}

/**
 * protocol.py:RateLimiter — sliding-window limiter, one deque bucket per
 * identity; entries older than RATE_WINDOW_SECONDS slide out. Clock INJECTED.
 */
export class RateLimiter {
	private readonly buckets = new Map<string, number[]>();

	constructor(
		private readonly clock: SecondsClock,
		private readonly env: EnvReader,
	) {}

	allow(identity: string): boolean {
		const limit = rateLimitPerMinute(this.env);
		const now = this.clock();
		let bucket = this.buckets.get(identity);
		if (bucket === undefined) {
			bucket = [];
			this.buckets.set(identity, bucket);
		}
		while (
			bucket.length > 0 &&
			now - (bucket[0] as number) > RATE_WINDOW_SECONDS
		) {
			bucket.shift();
		}
		if (bucket.length >= limit) return false;
		bucket.push(now);
		return true;
	}

	bucketSize(identity: string): number {
		return this.buckets.get(identity)?.length ?? 0;
	}
}

// ── Metrics collection ───────────────────────────────────────────────────────

const LATENCY_WINDOW = 100;

/**
 * protocol.py:Metrics — counters shared by the inbound adapter and the
 * outbound client tools. The port scopes ONE instance PER ADAPTER: the
 * outbound tools are NOT ported (scope decision), so the module-singleton's
 * sharing rationale vanishes and conformance rows need fresh state.
 * Not persisted.
 */
export class Metrics {
	inboundTotal = 0;
	outboundTotal = 0;
	streamsStarted = 0;
	pushSent = 0;
	pushFailed = 0;
	tasksCompleted = 0;
	tasksFailed = 0;
	antiLoopTriggers = 0;
	rateLimitTriggers = 0;

	private readonly startTime: number;
	private readonly latencies: number[] = [];

	constructor(private readonly clock: SecondsClock) {
		this.startTime = clock();
	}

	recordLatency(seconds: number): void {
		this.latencies.push(seconds);
		if (this.latencies.length > LATENCY_WINDOW) this.latencies.shift();
	}

	avgLatency(): number {
		if (this.latencies.length === 0) return 0;
		return this.latencies.reduce((a, b) => a + b, 0) / this.latencies.length;
	}

	snapshot(): Record<string, unknown> {
		const uptime = this.clock() - this.startTime;
		return {
			uptime_seconds: round1(uptime),
			inbound_total: this.inboundTotal,
			outbound_total: this.outboundTotal,
			streams_started: this.streamsStarted,
			push_sent: this.pushSent,
			push_failed: this.pushFailed,
			tasks_completed: this.tasksCompleted,
			tasks_failed: this.tasksFailed,
			anti_loop_triggers: this.antiLoopTriggers,
			rate_limit_triggers: this.rateLimitTriggers,
			avg_latency_ms: round1(this.avgLatency() * 1000),
		};
	}
}

function round1(value: number): number {
	return Math.round(value * 10) / 10;
}

// ── Deferred (concurrent.futures.Future parity for the reply plane) ─────────

export class Deferred<T> {
	private settledValue: T | undefined;
	private isSettled = false;
	private readonly resolvers: Array<(value: T) => void> = [];
	private readonly promise: Promise<T>;

	constructor() {
		this.promise = new Promise<T>((resolve) => {
			this.resolvers.push(resolve);
		});
	}

	get done(): boolean {
		return this.isSettled;
	}

	resolve(value: T): void {
		if (this.isSettled) return;
		this.isSettled = true;
		this.settledValue = value;
		for (const resolve of this.resolvers) resolve(value);
	}

	get settledWith(): T | undefined {
		return this.settledValue;
	}

	get future(): Promise<T> {
		return this.promise;
	}
}

export type TaskWatchFuture = Deferred<{ state: string; reply: string }>;

// ── Task store ───────────────────────────────────────────────────────────────

export interface TaskRecord {
	task_id: string;
	context_id: string;
	peer: string;
	agent_slug: string;
	tenant: string;
	state: string;
	reply: string;
	created_at: number;
	created_iso: string;
	push_url: string;
	push_config_id: string;
	completed_at?: number | undefined;
}

/**
 * protocol.py:TaskStore — pending AND completed tasks (queryable via
 * tasks/get, tasks/list). Insertion order preserved (OrderedDict parity via
 * Map). Records carry the routed agent slug and tenant; ALL read/write
 * accessors accept optional scope values and return not-found when the task
 * exists but is not visible in that scope (spec authorization-scoping rule).
 */
export class TaskStore {
	private readonly tasks = new Map<string, TaskRecord>();
	private readonly watchers = new Map<string, TaskWatchFuture[]>();

	constructor(private readonly clock: SecondsClock) {}

	private static inScope(
		rec: TaskRecord,
		agentSlug = "",
		tenant = "",
	): boolean {
		if (agentSlug && rec.agent_slug !== agentSlug) return false;
		if (tenant && rec.tenant !== tenant) return false;
		return true;
	}

	create(
		taskId: string,
		contextId: string,
		peer: string,
		agentSlug = "",
		tenant = "",
	): TaskRecord {
		const rec: TaskRecord = {
			task_id: taskId,
			context_id: contextId,
			peer,
			agent_slug: agentSlug || "",
			tenant: tenant || "",
			state: STATE_SUBMITTED,
			reply: "",
			created_at: this.clock(),
			created_iso: nowIso(),
			push_url: "",
			push_config_id: "",
		};
		this.tasks.set(taskId, rec);
		return { ...rec };
	}

	setState(taskId: string, state: string): void {
		const rec = this.tasks.get(taskId);
		if (rec && !TERMINAL_STATES.has(rec.state)) rec.state = state;
	}

	private static pushConfigView(rec: TaskRecord): Record<string, unknown> {
		return {
			configId: rec.push_config_id || "",
			taskId: rec.task_id,
			createdAt: rec.created_iso,
			pushNotificationConfig: { url: rec.push_url || "" },
		};
	}

	/** Attach a push config; returns the stored view or null when out of scope. */
	setPushConfig(
		taskId: string,
		url: string,
		agentSlug = "",
		tenant = "",
	): Record<string, unknown> | null {
		const rec = this.tasks.get(taskId);
		if (!rec || !TaskStore.inScope(rec, agentSlug, tenant)) return null;
		rec.push_url = url;
		rec.push_config_id = PUSH_CONFIG_ID_PREFIX + randomBytes(6).toString("hex");
		return TaskStore.pushConfigView(rec);
	}

	getPushConfig(
		taskId: string,
		configId = "",
		agentSlug = "",
		tenant = "",
	): Record<string, unknown> | null {
		const rec = this.tasks.get(taskId);
		if (!rec || !TaskStore.inScope(rec, agentSlug, tenant) || !rec.push_url) {
			return null;
		}
		if (configId && rec.push_config_id !== configId) return null;
		return TaskStore.pushConfigView(rec);
	}

	listPushConfigs(
		taskId: string,
		agentSlug = "",
		tenant = "",
	): Array<Record<string, unknown>> {
		const rec = this.tasks.get(taskId);
		if (!rec || !TaskStore.inScope(rec, agentSlug, tenant) || !rec.push_url) {
			return [];
		}
		return [TaskStore.pushConfigView(rec)];
	}

	deletePushConfig(
		taskId: string,
		configId = "",
		agentSlug = "",
		tenant = "",
	): boolean {
		const rec = this.tasks.get(taskId);
		if (!rec || !TaskStore.inScope(rec, agentSlug, tenant) || !rec.push_url) {
			return false;
		}
		if (configId && rec.push_config_id !== configId) return false;
		rec.push_url = "";
		rec.push_config_id = "";
		return true;
	}

	/** One-shot pop of the registered callback URL ('' when none). */
	popPushUrl(taskId: string): string {
		const rec = this.tasks.get(taskId);
		if (!rec) return "";
		const url = rec.push_url;
		rec.push_url = "";
		return url;
	}

	get(taskId: string, agentSlug = "", tenant = ""): TaskRecord | null {
		const rec = this.tasks.get(taskId);
		if (!rec || !TaskStore.inScope(rec, agentSlug, tenant)) return null;
		return { ...rec };
	}

	size(): number {
		return this.tasks.size;
	}

	/** Transition to a terminal state. IDEMPOTENT (second call returns null). */
	complete(taskId: string, state: string, reply = ""): TaskRecord | null {
		const rec = this.tasks.get(taskId);
		if (!rec || TERMINAL_STATES.has(rec.state)) return null;
		rec.state = state;
		rec.reply = reply;
		rec.completed_at = this.clock();
		const waiting = this.watchers.get(taskId) ?? [];
		this.watchers.delete(taskId);
		this.trimLocked();
		const out = { ...rec };
		// FIFO watcher resolution AFTER the record settles (source resolves
		// outside the lock).
		for (const fut of waiting) {
			if (!fut.done) fut.resolve({ state, reply });
		}
		return out;
	}

	watch(taskId: string, agentSlug = "", tenant = ""): TaskWatchFuture | null {
		const rec = this.tasks.get(taskId);
		if (!rec || !TaskStore.inScope(rec, agentSlug, tenant)) return null;
		const fut: TaskWatchFuture = new Deferred();
		if (TERMINAL_STATES.has(rec.state)) {
			fut.resolve({ state: rec.state, reply: rec.reply });
		} else {
			const list = this.watchers.get(taskId) ?? [];
			list.push(fut);
			this.watchers.set(taskId, list);
		}
		return fut;
	}

	/** Filtered task page, NEWEST FIRST; page size clamped 1..100. */
	list(opts: {
		contextId?: string | undefined;
		state?: string | undefined;
		pageSize?: number | undefined;
		offset?: number | undefined;
		agentSlug?: string | undefined;
		tenant?: string | undefined;
		withTotal?: boolean | undefined;
	}):
		| { records: TaskRecord[]; nextOffset: number }
		| { records: TaskRecord[]; nextOffset: number; total: number } {
		const pageSize = Math.max(
			1,
			Math.min(Math.trunc(opts.pageSize || 50), 100),
		);
		const offset = opts.offset ?? 0;
		let recs = [...this.tasks.values()].reverse();
		if (opts.agentSlug || opts.tenant) {
			recs = recs.filter((r) =>
				TaskStore.inScope(r, opts.agentSlug ?? "", opts.tenant ?? ""),
			);
		}
		if (opts.contextId)
			recs = recs.filter((r) => r.context_id === opts.contextId);
		if (opts.state) recs = recs.filter((r) => r.state === opts.state);
		const total = recs.length;
		const page = recs.slice(offset, offset + pageSize);
		const nextOffset = offset + pageSize < total ? offset + pageSize : 0;
		if (opts.withTotal) return { records: page, nextOffset, total };
		return { records: page, nextOffset };
	}

	/**
	 * Complete stale non-terminal tasks with "[task orphaned — no reply
	 * produced]"; returns their ids (watchdog sweep).
	 */
	failOrphans(timeoutSeconds = 300): string[] {
		const now = this.clock();
		const stale: string[] = [];
		for (const [tid, rec] of this.tasks) {
			if (
				!TERMINAL_STATES.has(rec.state) &&
				now - rec.created_at > timeoutSeconds
			) {
				stale.push(tid);
			}
		}
		const failed: string[] = [];
		for (const tid of stale) {
			if (this.complete(tid, STATE_FAILED, ORPHANED_REPLY_TEXT)) {
				failed.push(tid);
			}
		}
		return failed;
	}

	/** Keep at most TERMINAL_TRIM terminal records, dropping OLDEST first. */
	private trimLocked(): void {
		const terminal: string[] = [];
		for (const [tid, rec] of this.tasks) {
			if (TERMINAL_STATES.has(rec.state)) terminal.push(tid);
		}
		const excess = terminal.length - TERMINAL_TRIM;
		for (let i = 0; i < Math.max(0, excess); i++) {
			this.tasks.delete(terminal[i] as string);
		}
	}

	/** protocol.py:TaskStore.to_task — render a stored record as a v1.0 Task. */
	static toTask(
		rec: TaskRecord,
		historyLength?: number | null | undefined,
		includeArtifacts = true,
	): Record<string, unknown> {
		const task = buildTask(rec.task_id, rec.context_id, rec.state, rec.reply, {
			createdAt: rec.created_iso,
		}) as Record<string, unknown>;
		if (!includeArtifacts) delete task["artifacts"];
		if (historyLength === 0) delete task["history"];
		return structuredCloneish(task);
	}
}

/** Deep copy (copy.deepcopy parity over JSON-safe task shapes). */
function structuredCloneish(value: unknown): Record<string, unknown> {
	return JSON.parse(JSON.stringify(value)) as Record<string, unknown>;
}

/** adapter.py:_watchdog_loop / TaskStore.fail_orphans completion text. */
export const ORPHANED_REPLY_TEXT = "[task orphaned — no reply produced]";

// ── Conversation persistence (outside the compaction pipeline) ──────────────

/** protocol.py:_safe_name — keep alnum + "-_", else drop; fallback default. */
export function safeConversationName(contextId: string): string {
	const filtered = (contextId || "default")
		.split("")
		.filter((c) => /[a-zA-Z0-9_-]/.test(c))
		.join("");
	return filtered || "default";
}

/**
 * protocol.py persist_message/load_conversation/list_conversations over an
 * INJECTED storage directory (mkdtemp-isolated in fixtures; best-effort
 * writes never raise into the caller).
 */
export class ConversationStore {
	constructor(private readonly baseDir: string) {}

	private filePathFor(contextId: string): string {
		return path.join(this.baseDir, `${safeConversationName(contextId)}.jsonl`);
	}

	persistMessage(
		contextId: string,
		role: string,
		text: string,
		taskId = "",
		clock: SecondsClock = () => Date.now() / 1000,
	): void {
		try {
			mkdirSync(this.baseDir, { recursive: true });
			const rec = { ts: clock(), role, text, task_id: taskId };
			appendFileSync(
				this.filePathFor(contextId),
				`${JSON.stringify(rec)}\n`,
				"utf-8",
			);
		} catch {
			/* best-effort parity */
		}
	}

	loadConversation(
		contextId: string,
		limit = 50,
	): Array<Record<string, unknown>> {
		const file = this.filePathFor(contextId);
		if (!existsSync(file)) return [];
		const out: Array<Record<string, unknown>> = [];
		try {
			for (const line of readFileSync(file, "utf-8").split("\n")) {
				const trimmed = line.trim();
				if (!trimmed) continue;
				try {
					out.push(JSON.parse(trimmed) as Record<string, unknown>);
				} catch {
					// corrupt line — skipped (json.JSONDecodeError parity)
				}
			}
		} catch {
			// unreadable log — empty result (source except parity)
			return [];
		}
		return out.slice(-limit);
	}

	listConversations(): string[] {
		if (!existsSync(this.baseDir)) return [];
		try {
			return readdirSync(this.baseDir)
				.filter((f) => f.endsWith(".jsonl"))
				.map((f) => f.replace(/\.jsonl$/, ""))
				.sort();
		} catch {
			return [];
		}
	}
}

/** protocol.py audit record shape (adapter.py/security.py audit()). */
export interface AuditRecord {
	ts: number;
	direction: "inbound" | "outbound" | "push";
	peer: string;
	task_id: string;
	summary: string;
}

/**
 * security.py:audit — append-only JSONL of every inbound/outbound/push
 * exchange; summaries capped at 500 chars; writes are BEST-EFFORT (never
 * raise into the caller).
 */
export class AuditLog {
	constructor(private readonly filePath: string) {}

	append(
		direction: AuditRecord["direction"],
		peer: string,
		taskId: string,
		summary: string,
		clock: SecondsClock = () => Date.now() / 1000,
	): void {
		try {
			const rec: AuditRecord = {
				ts: clock(),
				direction,
				peer,
				task_id: taskId,
				summary: (summary || "").slice(0, 500),
			};
			mkdirSync(path.dirname(this.filePath), { recursive: true });
			appendFileSync(this.filePath, `${JSON.stringify(rec)}\n`, "utf-8");
		} catch {
			/* best-effort parity */
		}
	}

	readAll(): AuditRecord[] {
		try {
			if (!existsSync(this.filePath)) return [];
			const out: AuditRecord[] = [];
			for (const line of readFileSync(this.filePath, "utf-8").split("\n")) {
				const trimmed = line.trim();
				if (!trimmed) continue;
				try {
					out.push(JSON.parse(trimmed) as AuditRecord);
				} catch {
					// corrupt audit line — skipped (best-effort read)
				}
			}
			return out;
		} catch {
			// unreadable audit log — empty result
			return [];
		}
	}
}

/** Re-export for adapter typing convenience. */
export { STATE_SUBMITTED, STATE_COMPLETED };
