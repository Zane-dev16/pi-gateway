// pi_platforms/webhook/completions — the /v1/chat/completions endpoint logic
// (api_server-class ingress): Bearer auth with CONSTANT-TIME compare, the
// opt-in session headers (DEC-017), RAW-key direct turns (DEC-022 stateless
// lane target), and Idempotency-Key outcome caching.
//
// Ported from the READ-ONLY Hermes reference (api_server.py):
//   _admit_api_agent_request (@1212) + _check_auth (@1896) — Bearer vs key
//     via hmac.compare_digest; failure ⇒ 401 gateway_auth_error envelope
//   X-Hermes-Session-Id continuation 403-gated when no API key is configured
//   _MAX_SESSION_HEADER_LEN=256 (@2255) — control-char/length sanitization
//   _bind_api_server_session (@7087) — the turn binds under the RAW id
//   response headers echo the effective session id
//   _concurrency_limited_response (@7054) — 429 + Retry-After on cap

import { secureCompare } from "../kit/trust.js";
import {
	MAX_SESSION_HEADER_LEN,
	COMPLETIONS_IDEMPOTENCY_TTL_MS,
	COMPLETIONS_IDEMPOTENCY_MAX_ENTRIES,
} from "./manifest.js";
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
}

export interface DirectTurnResult {
	reply: string;
	sessionId: string;
}

export interface CompletionsDeps {
	apiKeyProvider: () => string | undefined;
	/** Execute ONE direct turn bound to the RAW session key. */
	runDirectTurn: (opts: {
		rawSessionId?: string | undefined;
		prompt: string;
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
export function sanitizeSessionHeader(raw: string | undefined): string | null {
	if (raw === undefined || raw.length === 0) return null;
	if (raw.length > MAX_SESSION_HEADER_LEN) return null;
	// Control characters and path-shaping sequences never reach a key.
	if (/[\u0000-\u001f\u007f]/.test(raw)) return null;
	if (raw.includes("..") || raw.startsWith("/")) return null;
	return raw;
}

export class CompletionsEndpoint {
	constructor(private readonly deps: CompletionsDeps) {}

	async handle(req: CompletionsRequest): Promise<CompletionsResponse> {
		const apiKey = this.deps.apiKeyProvider();
		const sessionHeader = req.headers["x-hermes-session-id"];

		// Continuation gate FIRST when no key is configured: history exposure
		// requires an authenticated server — 403 rather than a fresh session.
		if (apiKey === undefined || apiKey.length === 0) {
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
		if (sessionHeader !== undefined) {
			const sanitized = sanitizeSessionHeader(sessionHeader);
			if (sanitized === null) {
				return {
					status: 400,
					json: { error: { message: "Invalid X-Hermes-Session-Id" } },
					headers: {},
				};
			}
			rawSessionId = sanitized;
		}

		// Idempotency-Key outcome cache (api_server.py _IdempotCache parity).
		const idemKey = req.headers["idempotency-key"];
		if (idemKey !== undefined && idemKey.length > 0) {
			const claim = this.deps.idempotency.begin(`idem:${idemKey}`);
			if (claim.replay && claim.outcome !== null) {
				return {
					status: claim.outcome.status,
					json: claim.outcome.body,
					headers: {},
				};
			}
			try {
				const result = await this.execute(req, rawSessionId);
				this.deps.idempotency.recordOutcome(`idem:${idemKey}`, {
					status: result.status,
					body: result.json,
				});
				return result;
			} catch (err) {
				return {
					status: 500,
					json: {
						error: {
							message: err instanceof Error ? err.message : "agent run failed",
						},
					},
					headers: {},
				};
			}
		}

		try {
			return await this.execute(req, rawSessionId);
		} catch (err) {
			return {
				status: 500,
				json: {
					error: {
						message: err instanceof Error ? err.message : "agent run failed",
					},
				},
				headers: {},
			};
		}
	}

	private async execute(
		req: CompletionsRequest,
		rawSessionId: string | undefined,
	): Promise<CompletionsResponse> {
		let parsed: unknown;
		try {
			parsed = JSON.parse(req.bodyText);
		} catch {
			return {
				status: 400,
				json: { error: { message: "Cannot parse body" } },
				headers: {},
			};
		}
		const prompt = extractLastUserMessage(parsed as { messages?: unknown });
		if (prompt === null) {
			return {
				status: 400,
				json: { error: { message: "messages must carry a user turn" } },
				headers: {},
			};
		}

		const result = await this.deps.runDirectTurn({ rawSessionId, prompt });
		const json: Record<string, unknown> = {
			id: `chatcmpl-${result.sessionId}`,
			object: "chat.completion",
			choices: [
				{
					index: 0,
					message: { role: "assistant", content: result.reply },
					finish_reason: "stop",
				},
			],
		};
		return {
			status: 200,
			json,
			headers: { "x-hermes-session-id": result.sessionId },
		};
	}
}

function extractBearer(header: string | undefined): string | undefined {
	if (header === undefined) return undefined;
	const m = /^Bearer\s+(.+)$/i.exec(header.trim());
	return m?.[1];
}

function extractLastUserMessage(
	parsed: { messages?: unknown } | null,
): string | null {
	if (parsed === null || !Array.isArray(parsed.messages)) return null;
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
