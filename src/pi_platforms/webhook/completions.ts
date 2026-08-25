// pi_platforms/webhook/completions — the /v1/chat/completions endpoint logic
// (api_server-class ingress): Bearer auth with CONSTANT-TIME compare, the
// opt-in session headers (DEC-017), RAW-key direct turns (DEC-022 stateless
// lane target), Idempotency-Key outcome caching keyed by REQUEST FINGERPRINT,
// OpenAI response enrichment (created/model/usage/finish_reason) and the
// stream=true SSE chunk writer ending `data: [DONE]`.
//
// Ported from the READ-ONLY Hermes reference (api_server.py):
//   _admit_api_agent_request (@1212) + _check_auth (@1896) — Bearer vs key
//     via hmac.compare_digest; failure ⇒ 401 gateway_auth_error envelope
//   X-Hermes-Session-Id continuation 403-gated when no API key is configured
//   _MAX_SESSION_HEADER_LEN=256 (@2255) — control-char/length sanitization
//   _handle_chat_completions (@5042) — "Invalid JSON in request body" 400,
//     "Missing or invalid 'messages' field", "No user message found in
//     messages", _coerce_request_bool(body.stream), completion_id
//     chatcmpl-<uuid>, created=int(time.time()), model fallback
//   response build (@5366) — usage{prompt_tokens,completion_tokens,
//     total_tokens}, finish_reason stop/length/error ladder
//   _write_sse_chat_completion (@5402) — role chunk → content chunks →
//     finish chunk (usage on the tail) → `data: [DONE]\n\n`
//   _IdempotencyCache.get_or_set (@1339) + _make_request_fingerprint —
//     replays require sha256(model/provider/model_options/messages/tools/
//     tool_choice/stream) to MATCH the cached entry's fingerprint

import { createHash, randomUUID } from "node:crypto";
import { secureCompare } from "../kit/trust.js";
import {
	MAX_SESSION_HEADER_LEN,
	DEFAULT_COMPLETIONS_MODEL,
} from "./manifest.js";
import {
	SESSION_KEY_HEADER,
	extractOptInSessionHeaders,
} from "../../pi_gateway/security/trust/session-headers.js";
import type { HeaderMap } from "./signatures.js";
import type { DeliveryIdempotencyStore } from "./idempotency.js";

export interface CompletionsRequest {
	headers: HeaderMap;
	bodyText: string;
}

export interface CompletionsResponse {
	status: number;
	json: Record<string, unknown>;
	/** Response headers (session-id echo). */
	headers: Record<string, string>;
	/**
	 * Pre-rendered raw body (the SSE chunk stream). When present the server
	 * writes THIS instead of JSON.stringify(json) and honors a text/event-stream
	 * content-type in headers.
	 */
	rawBody?: string | undefined;
}

export interface DirectTurnUsage {
	promptTokens: number;
	completionTokens: number;
	totalTokens: number;
}

export interface DirectTurnResult {
	reply: string;
	sessionId: string;
	/** Executor-driven usage (Hermes surfaces the agent's token counts). */
	usage?: DirectTurnUsage | undefined;
	/** Executor-driven finish_reason ("stop" default; "length"/"error"). */
	finish?: "stop" | "length" | "error" | undefined;
}

export interface CompletionsDeps {
	apiKeyProvider: () => string | undefined;
	/**
	 * Execute ONE direct turn bound to the RAW session key. `sessionKey` is
	 * the adopted X-Hermes-Session-Key memory scope (DEC-017) — independent
	 * of the continuity id, exactly like Hermes' gateway_session_key.
	 */
	runDirectTurn: (opts: {
		rawSessionId?: string | undefined;
		prompt: string;
		sessionKey?: string | undefined;
	}) => Promise<DirectTurnResult>;
	idempotency: DeliveryIdempotencyStore;
	nowMs: () => number;
}

function authEnvelope(
	status: number,
	code: string,
	message: string,
): CompletionsResponse {
	return {
		status,
		json: { error: { type: "gateway_auth_error", code, message } },
		headers: {},
	};
}

/** Session header sanitization (control chars/path-unsafe → null). */
const CONTROL_CHAR_PATTERN = /[\u0000-\u001f\u007f]/;

export function sanitizeSessionHeader(raw: string | undefined): string | null {
	if (raw === undefined || raw.length === 0) return null;
	if (raw.length > MAX_SESSION_HEADER_LEN) return null;
	// Control characters and path-shaping sequences never reach a key.
	if (CONTROL_CHAR_PATTERN.test(raw)) return null;
	if (raw.includes("..") || raw.startsWith("/")) return null;
	return raw;
}

/**
 * api_server.py:_coerce_request_bool parity — real JSON booleans pass through;
 * bool-ish STRINGS normalize ("false" must not misroute as truthy); everything
 * else falls back to the caller's default.
 */
const TRUE_REQUEST_BOOL_STRINGS = new Set(["1", "true", "yes", "on"]);
const FALSE_REQUEST_BOOL_STRINGS = new Set(["0", "false", "no", "off"]);

export function coerceRequestBool(value: unknown, fallback: boolean): boolean {
	if (typeof value === "boolean") return value;
	if (value === null || value === undefined) return fallback;
	if (typeof value === "string") {
		const normalized = value.trim().toLowerCase();
		if (TRUE_REQUEST_BOOL_STRINGS.has(normalized)) return true;
		if (FALSE_REQUEST_BOOL_STRINGS.has(normalized)) return false;
		return fallback;
	}
	if (typeof value === "number") return value !== 0;
	return fallback;
}

/** Deterministic JSON (sorted object keys) so fingerprints are stable. */
function stableStringify(value: unknown): string {
	if (value === null || typeof value !== "object") return JSON.stringify(value);
	if (Array.isArray(value)) {
		return `[${value.map((item) => stableStringify(item)).join(",")}]`;
	}
	const entries = Object.entries(value as Record<string, unknown>).filter(
		([, v]) => v !== undefined,
	);
	entries.sort(compareKeys);
	return `{${entries
		.map(([k, v]) => `${JSON.stringify(k)}:${stableStringify(v)}`)
		.join(",")}}}`;
}

function compareKeys([a]: [string, unknown], [b]: [string, unknown]): number {
	if (a < b) return -1;
	if (a > b) return 1;
	return 0;
}

/**
 * api_server.py:_make_request_fingerprint parity — sha256 over the request
 * subset that must match for an Idempotency-Key replay to be honored.
 */
export const FINGERPRINT_KEYS = [
	"model",
	"provider",
	"model_options",
	"messages",
	"tools",
	"tool_choice",
	"stream",
] as const;

export function requestFingerprint(parsed: Record<string, unknown>): string {
	const subset: Record<string, unknown> = {};
	for (const key of FINGERPRINT_KEYS) subset[key] = parsed[key];
	return createHash("sha256").update(stableStringify(subset)).digest("hex");
}

export class CompletionsEndpoint {
	constructor(private readonly deps: CompletionsDeps) {}

	async handle(req: CompletionsRequest): Promise<CompletionsResponse> {
		const apiKey = this.deps.apiKeyProvider();
		const apiKeyConfigured = apiKey !== undefined && apiKey.length > 0;

		// Opt-in session headers (DEC-017): the KEY gate runs FIRST —
		// api_server.py parses X-Hermes-Session-Key before the continuation
		// gate on every lane that honors it (@5104). 403 requires-auth / 400
		// injection / 400 length come straight from the shared trust engine.
		const sessionVerdict = extractOptInSessionHeaders(req.headers, {
			apiKeyConfigured,
		});
		if (
			!sessionVerdict.ok &&
			(apiKeyConfigured || req.headers[SESSION_KEY_HEADER] !== undefined)
		) {
			// A key-caused failure always surfaces; an id-shape failure surfaces
			// only once authenticated (the unauthenticated lane answers the
			// continuation 403 below instead — Hermes gates the id AFTER the key).
			return {
				status: sessionVerdict.status,
				json: {
					error: {
						message: sessionVerdict.error,
						type: sessionVerdict.errorType,
					},
				},
				headers: {},
			};
		}
		const memoryScopeKey = sessionVerdict.ok
			? (sessionVerdict.sessionKey ?? undefined)
			: undefined;

		const sessionHeader = req.headers["x-hermes-session-id"];

		// Continuation gate FIRST when no key is configured: history exposure
		// requires an authenticated server — 403 rather than a fresh session.
		if (!apiKeyConfigured) {
			if (sessionHeader !== undefined) {
				return authEnvelope(
					403,
					"session_continuation_requires_key",
					"X-Hermes-Session-Id continuation requires API_SERVER_KEY to be configured",
				);
			}
			return authEnvelope(401, "gateway_auth_failed", "missing bearer key");
		}

		const presented = extractBearer(req.headers["authorization"]);
		if (presented === undefined || !secureCompare(presented, apiKey)) {
			return authEnvelope(401, "gateway_auth_failed", "invalid bearer key");
		}

		let rawSessionId: string | undefined;
		const adoptedId = sessionVerdict.ok ? sessionVerdict.sessionId : null;
		if (adoptedId !== null) {
			// The pair helper already applied injection/length hygiene; THIS
			// pass retains the path-shaping defense (Hermes _is_path_unsafe
			// parity for ids interpolated into on-disk paths).
			const sanitized = sanitizeSessionHeader(adoptedId);
			if (sanitized === null) {
				return {
					status: 400,
					json: { error: { message: "Invalid X-Hermes-Session-Id" } },
					headers: {},
				};
			}
			rawSessionId = sanitized;
		}

		// Parse ONCE up front (api_server parses the body at handler top):
		// validation 400s precede both streaming and the idempotency claim.
		let parsed: Record<string, unknown>;
		try {
			const candidate: unknown = JSON.parse(req.bodyText);
			if (candidate === null || typeof candidate !== "object") {
				throw new Error("not an object");
			}
			parsed = candidate as Record<string, unknown>;
		} catch {
			return openaiErrorResponse(400, "Invalid JSON in request body");
		}
		if (
			parsed["messages"] === undefined ||
			parsed["messages"] === null ||
			!Array.isArray(parsed["messages"])
		) {
			return openaiErrorResponse(400, "Missing or invalid 'messages' field");
		}
		const prompt = extractLastUserMessage(parsed);
		if (prompt === null) {
			return openaiErrorResponse(400, "No user message found in messages");
		}

		// stream=true rides the SSE chunk writer ([DONE]-terminated) and —
		// Hermes parity — bypasses the non-streaming idempotency cache.
		if (coerceRequestBool(parsed["stream"], false)) {
			try {
				const result = await this.deps.runDirectTurn({
					rawSessionId,
					prompt,
					sessionKey: memoryScopeKey,
				});
				return this.renderChunkStream(result, parsed, memoryScopeKey);
			} catch (err) {
				return openaiErrorResponse(
					500,
					err instanceof Error ? err.message : "agent run failed",
					"server_error",
				);
			}
		}

		// Idempotency-Key outcome cache (api_server.py _IdempotencyCache
		// parity): the header key alone NEVER selects a replay — the request
		// fingerprint (sha256 over model/messages/stream/…) must match too, so
		// reusing a key with a different body computes fresh instead of
		// silently replaying a stale answer.
		const idemKey = req.headers["idempotency-key"];
		if (idemKey !== undefined && idemKey.length > 0) {
			const cacheKey = `idem:${idemKey}:${requestFingerprint(parsed)}`;
			const claim = this.deps.idempotency.begin(cacheKey);
			if (claim.replay && claim.outcome !== null) {
				return {
					status: claim.outcome.status,
					json: claim.outcome.body,
					headers: {},
				};
			}
			try {
				const result = await this.execute(rawSessionId, prompt, memoryScopeKey);
				this.deps.idempotency.recordOutcome(cacheKey, {
					status: result.status,
					body: result.json,
				});
				return result;
			} catch (err) {
				return openaiErrorResponse(
					500,
					err instanceof Error ? err.message : "agent run failed",
					"server_error",
				);
			}
		}

		try {
			return await this.execute(rawSessionId, prompt, memoryScopeKey);
		} catch (err) {
			return openaiErrorResponse(
				500,
				err instanceof Error ? err.message : "agent run failed",
				"server_error",
			);
		}
	}

	private async execute(
		rawSessionId: string | undefined,
		prompt: string,
		sessionKey: string | undefined,
	): Promise<CompletionsResponse> {
		const result = await this.deps.runDirectTurn({
			rawSessionId,
			prompt,
			sessionKey,
		});
		return {
			status: 200,
			json: completionPayload(result),
			headers: sessionKeyEcho(result.sessionId, sessionKey),
		};
	}

	/** api_server.py:_write_sse_chat_completion framing over the turn seam. */
	private renderChunkStream(
		result: DirectTurnResult,
		parsed: Record<string, unknown>,
		sessionKey: string | undefined,
	): CompletionsResponse {
		const completionId = `chatcmpl-${randomUUID()}`;
		const model =
			typeof parsed["model"] === "string" && parsed["model"].length > 0
				? parsed["model"]
				: DEFAULT_COMPLETIONS_MODEL;
		const created = Math.floor(this.deps.nowMs() / 1000);
		const usage = result.usage ?? {
			promptTokens: 0,
			completionTokens: 0,
			totalTokens: 0,
		};

		const frames: string[] = [];
		const frame = (payload: Record<string, unknown>): void => {
			frames.push(`data: ${JSON.stringify(payload)}\n\n`);
		};

		// Role chunk.
		frame({
			id: completionId,
			object: "chat.completion.chunk",
			created,
			model,
			choices: [
				{ index: 0, delta: { role: "assistant" }, finish_reason: null },
			],
		});
		// Content chunk(s) — the direct-turn seam yields ONE final segment.
		if (result.reply.length > 0) {
			frame({
				id: completionId,
				object: "chat.completion.chunk",
				created,
				model,
				choices: [
					{
						index: 0,
						delta: { content: result.reply },
						finish_reason: null,
					},
				],
			});
		}
		// Finish chunk carries the usage block (vendor parity).
		frame({
			id: completionId,
			object: "chat.completion.chunk",
			created,
			model,
			choices: [
				{ index: 0, delta: {}, finish_reason: result.finish ?? "stop" },
			],
			usage: {
				prompt_tokens: usage.promptTokens,
				completion_tokens: usage.completionTokens,
				total_tokens: usage.totalTokens,
			},
		});
		frames.push("data: [DONE]\n\n");

		return {
			status: 200,
			json: {},
			headers: {
				...sessionKeyEcho(result.sessionId, sessionKey),
				"content-type": "text/event-stream",
				"cache-control": "no-cache",
				"x-accel-buffering": "no",
			},
			rawBody: frames.join(""),
		};
	}
}

/**
 * Response headers: continuity-id echo plus the memory-scope echo when a
 * session key was adopted (api_server.py @5342/:4689 — every completion
 * response carries X-Hermes-Session-Key back to the caller).
 */
function sessionKeyEcho(
	sessionId: string,
	sessionKey: string | undefined,
): Record<string, string> {
	return {
		"x-hermes-session-id": sessionId,
		...(sessionKey !== undefined
			? { "x-hermes-session-key": sessionKey }
			: {}),
	};
}

/** Non-streaming response body (created/model/usage/finish_reason parity). */
function completionPayload(result: DirectTurnResult): Record<string, unknown> {
	const usage = result.usage ?? {
		promptTokens: 0,
		completionTokens: 0,
		totalTokens: 0,
	};
	return {
		id: `chatcmpl-${randomUUID()}`,
		object: "chat.completion",
		created: Math.floor(Date.now() / 1000),
		model: DEFAULT_COMPLETIONS_MODEL,
		choices: [
			{
				index: 0,
				message: { role: "assistant", content: result.reply },
				finish_reason: result.finish ?? "stop",
			},
		],
		usage: {
			prompt_tokens: usage.promptTokens,
			completion_tokens: usage.completionTokens,
			total_tokens: usage.totalTokens,
		},
	};
}

/** api_server.py:_openai_error envelope ({message,type,param,code}). */
export function openaiErrorBody(
	message: string,
	errType = "invalid_request_error",
	code?: string | undefined,
): Record<string, unknown> {
	return {
		error: {
			message,
			type: errType,
			param: null,
			code: code ?? null,
		},
	};
}

function openaiErrorResponse(
	status: number,
	message: string,
	errType = "invalid_request_error",
	code?: string | undefined,
): CompletionsResponse {
	return { status, json: openaiErrorBody(message, errType, code), headers: {} };
}

function extractBearer(header: string | undefined): string | undefined {
	if (header === undefined) return undefined;
	const m = /^Bearer\s+(.+)$/i.exec(header.trim());
	return m?.[1];
}

function extractLastUserMessage(parsed: { messages?: unknown }): string | null {
	if (!Array.isArray(parsed.messages)) return null;
	for (let i = parsed.messages.length - 1; i >= 0; i--) {
		const msg = parsed.messages[i] as
			| { role?: unknown; content?: unknown }
			| undefined;
		if (msg?.role !== "user") continue;
		if (typeof msg.content === "string" && msg.content.length > 0) {
			return msg.content;
		}
		return null;
	}
	return null;
}
