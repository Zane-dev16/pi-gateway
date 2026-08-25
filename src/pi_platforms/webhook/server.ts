// pi_platforms/webhook/server — the in-process node:http binding for the
// webhook reference adapter. Ephemeral loopback port; NO external network.
// The trust pipeline itself is framework-free (http-ingress.ts); this layer
// only normalizes requests, enforces the body cap mid-stream, and renders
// responses incl. the /v1/runs SSE stream (api_server-class unbounded lane).
//
// Hermes anchors: api_server.py:_handle_run_events (@7937) — SSE headers
// (text/event-stream, X-Accel-Buffering: no), `: keepalive` comments, typed
// event frames `event: <type>\ndata: <json>\n\n` whose payloads carry
// snake_case run_id/session_id/seq/ts with a terminal done sentinel;
// _check_auth (@1920) — Bearer API_SERVER_KEY constant-time gate on every
// /v1/runs lane; _openai_error (@1219) envelopes; _handle_runs validation
// ladder (@7583); route table (@2214) — POST /v1/runs/{id}/approval.

import type { IncomingMessage, Server, ServerResponse } from "node:http";
import { createServer } from "node:http";
import { secureCompare } from "../kit/trust.js";
import type {
	WebhookIngressPipeline,
	IngressRequest,
	IngressResponse,
} from "./http-ingress.js";
import type { CompletionsEndpoint, CompletionsRequest } from "./completions.js";
import { coerceRequestBool, openaiErrorBody } from "./completions.js";
import type { RunEvent, RunRegistry } from "./runs.js";
import {
	SESSION_KEY_HEADER,
	extractSessionKeyHeader,
	type SessionKeyVerdict,
} from "../../pi_gateway/security/trust/session-headers.js";
import { DEFAULT_COMPLETIONS_MODEL } from "./manifest.js";

export interface WebhookServerDeps {
	pipeline: WebhookIngressPipeline;
	completions: CompletionsEndpoint;
	runs: RunRegistry;
	/** Body-size cap enforced MID-STREAM (lying Content-Length defense). */
	bodyCapBytes: number;
	/**
	 * api_server.py:_check_auth parity for the /v1/runs lanes — Bearer vs
	 * API_SERVER_KEY constant-time compare. Undefined/empty ⇒ ungated
	 * (the vendor's no-key test/manual-wiring behavior).
	 */
	apiKeyProvider?: (() => string | undefined) | undefined;
}

export class WebhookHttpServer {
	private server: Server | null = null;
	private port = 0;
	private readonly connections = new Set<import("node:net").Socket>();

	constructor(private readonly deps: WebhookServerDeps) {}

	/** Listen on a random loopback port; resolves with the base URL. */
	async listen(): Promise<string> {
		if (this.server !== null) return this.baseUrl();
		const server = createServer((req, res) => {
			void this.dispatch(req, res).catch(() => {
				if (!res.headersSent) {
					res.writeHead(500, { "content-type": "application/json" });
				}
				res.end(JSON.stringify({ error: "Internal server error" }));
			});
		});
		server.on("connection", (socket) => {
			this.connections.add(socket);
			socket.on("close", () => this.connections.delete(socket));
		});
		this.server = server;
		await new Promise<void>((resolve, reject) => {
			server.once("error", reject);
			server.listen(0, "127.0.0.1", () => resolve());
		});
		const address = server.address();
		this.port =
			typeof address === "object" && address !== null ? address.port : 0;
		return this.baseUrl();
	}

	baseUrl(): string {
		return `http://127.0.0.1:${this.port}`;
	}

	async close(): Promise<void> {
		const server = this.server;
		if (server === null) return;
		this.server = null;
		for (const socket of this.connections) socket.destroy();
		await new Promise<void>((resolve) => {
			server.close(() => resolve());
		});
	}

	private async dispatch(
		req: IncomingMessage,
		res: ServerResponse,
	): Promise<void> {
		const url = new URL(req.url ?? "/", "http://127.0.0.1");
		const path = url.pathname;
		const method = (req.method ?? "GET").toUpperCase();

		if (method === "GET" && path === "/health") {
			this.sendJson(res, 200, { status: "ok" });
			return;
		}

		if (
			path === "/v1/chat/completions" ||
			(path.startsWith("/v1/runs") && this.gateRunsAuth(req, res))
		) {
			await this.dispatchV1(method, path, req, res);
			return;
		}

		const ingressResponse = await this.deps.pipeline.handle(
			await this.normalize(req),
		);
		if (ingressResponse === null) {
			this.sendJson(res, 404, { error: "Not found" });
			return;
		}
		this.sendJson(res, ingressResponse.status, ingressResponse.json);
	}

	/**
	 * api_server.py:_check_auth parity for EVERY /v1/runs lane (start, status,
	 * events SSE, approval, steer, stop): Bearer token constant-time compare
	 * against the configured key; failure ⇒ the vendor's 401 gateway_auth_error
	 * envelope. No configured key ⇒ ungated (vendor test/manual-wiring parity).
	 * Returns false when the request was REJECTED (response written).
	 */
	private gateRunsAuth(req: IncomingMessage, res: ServerResponse): boolean {
		const apiKey = this.deps.apiKeyProvider?.();
		if (apiKey === undefined || apiKey.length === 0) return true;
		const header = lowerHeaders(req)["authorization"];
		const m =
			header === undefined ? undefined : /^Bearer\s+(.+)$/i.exec(header.trim());
		if (m?.[1] !== undefined && secureCompare(m[1], apiKey)) return true;
		this.sendJson(res, 401, {
			error: {
				message: "Invalid gateway API key (API_SERVER_KEY)",
				type: "gateway_auth_error",
				param: null,
				code: "gateway_auth_failed",
			},
		});
		return false;
	}

	/** The api_server-class lanes (completions + runs/SSE). */
	private async dispatchV1(
		method: string,
		path: string,
		req: IncomingMessage,
		res: ServerResponse,
	): Promise<void> {
		if (path === "/v1/chat/completions") {
			if (method !== "POST") {
				this.sendJson(res, 405, { error: "Method not allowed" });
				return;
			}
			const bodyText = await this.readBodyText(req);
			const response = await this.deps.completions.handle({
				headers: lowerHeaders(req),
				bodyText,
			} satisfies CompletionsRequest);
			if (response.rawBody !== undefined) {
				// SSE chunk stream ([DONE]-terminated): raw frames, vendor headers.
				res.writeHead(response.status, response.headers);
				res.end(response.rawBody);
				return;
			}
			this.sendJson(res, response.status, response.json, response.headers);
			return;
		}

		const runsMatch =
			/^\/v1\/runs\/([^/]+)(\/events|\/approvals?|\/steer|\/stop)?$/.exec(path);

		if (path === "/v1/runs") {
			if (method !== "POST") {
				this.sendJson(res, 405, { error: "Method not allowed" });
				return;
			}
			// X-Hermes-Session-Key memory-scope ladder FIRST (_handle_runs
			// parses the header before the body @7583): 403 requires-auth /
			// 400 injection / 400 length. The runs lane honors ONLY the key
			// (Hermes takes session identity here from the JSON body).
			const keyVerdict = extractSessionKeyHeader(lowerHeaders(req), {
				apiKeyConfigured: this.apiKeyConfigured(),
			});
			if (!keyVerdict.ok) {
				this.sendJson(res, keyVerdict.status, sessionKeyError(keyVerdict));
				return;
			}
			const memoryScopeKey = keyVerdict.sessionKey ?? undefined;
			// Validation ladder (api_server.py:_handle_runs @7583): invalid JSON,
			// missing 'input', and empty user messages all 400 BEFORE a run starts
			// — a malformed body never becomes a prompt.
			const bodyText = await this.readBodyText(req);
			let parsed: Record<string, unknown>;
			try {
				const candidate: unknown = JSON.parse(bodyText);
				if (candidate === null || typeof candidate !== "object") {
					throw new Error("not an object");
				}
				parsed = candidate as Record<string, unknown>;
			} catch {
				this.sendJson(res, 400, openaiErrorJson("Invalid JSON"));
				return;
			}
			const rawInput = parsed["input"];
			if (!rawInput) {
				this.sendJson(res, 400, openaiErrorJson("Missing 'input' field"));
				return;
			}
			const input =
				typeof rawInput === "string"
					? rawInput
					: Array.isArray(rawInput)
						? lastMessageContent(rawInput)
						: "";
			if (input.length === 0) {
				this.sendJson(
					res,
					400,
					openaiErrorJson("No user message found in input"),
				);
				return;
			}
			const sessionId =
				typeof parsed["session_id"] === "string" &&
				parsed["session_id"].length > 0
					? (parsed["session_id"] as string)
					: undefined;
			// Queued-status model field (@7690): body.get("model", _model_name).
			const model =
				typeof parsed["model"] === "string" && parsed["model"].length > 0
					? (parsed["model"] as string)
					: DEFAULT_COMPLETIONS_MODEL;
			const runId = this.startRunWithDefaultExecutor?.(
				input,
				sessionId,
				model,
				memoryScopeKey,
			);
			if (runId === undefined) {
				this.sendJson(res, 503, {
					error: "no run executor wired for HTTP lane",
				});
				return;
			}
			// Memory-scope echo (@8006): the 202 carries the adopted key back.
			this.sendJson(
				res,
				202,
				{ run_id: runId, status: "started" },
				memoryScopeKey !== undefined
					? { [SESSION_KEY_HEADER]: memoryScopeKey }
					: {},
			);
			return;
		}

		if (runsMatch === null || runsMatch[1] === undefined) {
			this.sendJson(res, 404, { error: "Not found" });
			return;
		}
		const runId = decodeURIComponent(runsMatch[1]);
		const sub = runsMatch[2] ?? "";

		if (sub === "" && method === "GET") {
			const view = this.deps.runs.status(runId);
			if (view === null) {
				this.sendJson(
					res,
					404,
					openaiErrorJson(`Run not found: ${runId}`, "run_not_found"),
				);
				return;
			}
			this.sendJson(res, 200, {
				object: view.object,
				run_id: view.runId,
				status: view.status,
				created_at: view.createdAt,
				updated_at: view.updatedAt,
				session_id: view.sessionId,
				...(view.model !== undefined ? { model: view.model } : {}),
				...(view.usage !== undefined
					? {
							usage: {
								prompt_tokens: view.usage.promptTokens,
								completion_tokens: view.usage.completionTokens,
								total_tokens: view.usage.totalTokens,
							},
					  }
					: {}),
				...(view.pendingSteer !== undefined
					? { pending_steer: view.pendingSteer }
					: {}),
				...(view.output !== undefined ? { output: view.output } : {}),
				...(view.error !== undefined ? { error: view.error } : {}),
				...(view.lastEvent !== undefined ? { last_event: view.lastEvent } : {}),
			});
			return;
		}
		if (sub === "/events") {
			if (method !== "GET") {
				this.sendJson(res, 405, { error: "Method not allowed" });
				return;
			}
			this.streamRunEvents(runId, res);
			return;
		}
		if (method !== "POST") {
			this.sendJson(res, 405, { error: "Method not allowed" });
			return;
		}
		const bodyText = await this.readBodyText(req);
		let payload: Record<string, unknown> = {};
		try {
			payload = JSON.parse(bodyText) as Record<string, unknown>;
		} catch {
			payload = {};
		}

		if (sub === "/approvals" || sub === "/approval") {
			// all/resolve_all booleans honor coerceRequestBool normalization
			// (_handle_run_approval @8121 parity); either truthy drains every
			// pending approval registered under THIS run.
			const resolveAll =
				coerceRequestBool(payload["all"], false) ||
				coerceRequestBool(payload["resolve_all"], false);
			const choice = String(payload["choice"] ?? "");
			const outcome = this.deps.runs.respondApprovalForRun(runId, choice, {
				resolveAll,
			});
			if (outcome.ok) {
				this.sendJson(res, 200, {
					object: "hermes.run.approval_response",
					run_id: runId,
					choice: outcome.choice,
					resolved: outcome.resolved,
				});
			} else if (outcome.code === "unknown_run") {
				this.sendJson(
					res,
					404,
					openaiErrorJson(`Run not found: ${runId}`, "run_not_found"),
				);
			} else if (outcome.code === "invalid_choice") {
				this.sendJson(
					res,
					400,
					openaiErrorJson(
						"Invalid approval choice; expected one of: once, session, always, deny",
						"invalid_approval_choice",
					),
				);
			} else {
				this.sendJson(
					res,
					409,
					openaiErrorJson(
						`Run has no active approval session: ${runId}`,
						outcome.code,
					),
				);
			}
			return;
		}
		if (sub === "/steer") {
			const text = String(
				payload["input"] ?? payload["message"] ?? payload["text"] ?? "",
			);
			if (text.trim().length === 0) {
				this.sendJson(
					res,
					400,
					openaiErrorJson(
						"Missing non-empty steer text; expected 'input', 'message', or 'text'.",
						"invalid_steer_input",
					),
				);
				return;
			}
			const outcome = this.deps.runs.steer(runId, text);
			if (!outcome.ok) {
				if (outcome.code === "unknown_run") {
					this.sendJson(
						res,
						404,
						openaiErrorJson(`Run not found: ${runId}`, "run_not_found"),
					);
				} else {
					this.sendJson(
						res,
						409,
						openaiErrorJson(
							`Run is not currently accepting steer input: ${runId}`,
							outcome.code,
						),
					);
				}
				return;
			}
			this.sendJson(res, 200, {
				object: "hermes.run.steer",
				run_id: runId,
				accepted: true,
			});
			return;
		}
		if (sub === "/stop") {
			// Terminal runs have NO live agent/task refs (api_server.py pops them
			// when the run closes out @7975), so a late stop answers 404
			// run_not_found exactly like an unknown id (@8199) — never 409.
			const outcome = this.deps.runs.stop(runId);
			if (!outcome.ok) {
				this.sendJson(
					res,
					404,
					openaiErrorJson(`Run not found: ${runId}`, "run_not_found"),
				);
				return;
			}
			this.sendJson(res, 200, { run_id: runId, status: "stopping" });
			return;
		}
		this.sendJson(res, 404, openaiErrorJson("Not found"));
	}

	/** Runs started over HTTP use the adapter-wired default executor. */
	startRunWithDefaultExecutor:
		| ((
				input: string,
				sessionId?: string | undefined,
				model?: string | undefined,
				memoryScopeKey?: string | undefined,
		  ) => string)
		| null = null;

	/** api_server-class auth posture for the session-key ladder. */
	private apiKeyConfigured(): boolean {
		const key = this.deps.apiKeyProvider?.();
		return key !== undefined && key.length > 0;
	}

	/**
	 * SSE (api_server.py:_handle_run_events parity): replay buffered events,
	 * then stream live until the terminal frame + done sentinel. Frames ride
	 * the `event:` line; data payloads carry snake_case run_id/session_id/seq/ts.
	 */
	private streamRunEvents(runId: string, res: ServerResponse): void {
		const view = this.deps.runs.status(runId);
		if (view === null) {
			this.sendJson(
				res,
				404,
				openaiErrorJson(`Run not found: ${runId}`, "run_not_found"),
			);
			return;
		}
		res.writeHead(200, {
			"content-type": "text/event-stream",
			"cache-control": "no-cache",
			connection: "keep-alive",
			"x-accel-buffering": "no",
			"x-hermes-session-id": view.sessionId,
		});
		let closed = false;
		const close = (): void => {
			if (closed) return;
			closed = true;
			clearInterval(keepalive);
			res.end();
		};
		const keepalive = setInterval(() => {
			if (!closed) res.write(": keepalive\n\n");
		}, 15_000);
		keepalive.unref?.();

		let seq = 0;
		const basePayload = (): Record<string, unknown> => ({
			run_id: runId,
			session_id: view.sessionId,
			seq: ++seq,
			ts: Date.now() / 1000,
		});
		const writeEvent = (event: RunEvent): void => {
			if (closed) return;
			const payload = runEventData(event, basePayload());
			if (payload === null) return;
			res.write(`event: ${event.type}\ndata: ${JSON.stringify(payload)}\n\n`);
			if (
				event.type === "run.completed" ||
				event.type === "run.failed" ||
				event.type === "run.cancelled"
			) {
				// Terminal done sentinel (api_server.py finally-block parity).
				res.write(`event: done\ndata: ${JSON.stringify(basePayload())}\n\n`);
				close();
			}
		};
		this.deps.runs.subscribe(runId, writeEvent);
		res.on("close", () => {
			closed = true;
			clearInterval(keepalive);
			this.deps.runs.detach(runId);
		});
	}

	/** Normalized request with lazy capped body read. */
	private async normalize(req: IncomingMessage): Promise<IngressRequest> {
		const headers = lowerHeaders(req);
		let bodyPromise: Promise<Buffer> | null = null;
		const contentLengthHeader = headers["content-length"];
		const declaredLength =
			contentLengthHeader !== undefined
				? Number.parseInt(contentLengthHeader, 10) || 0
				: 0;
		return {
			method: (req.method ?? "GET").toUpperCase(),
			path: new URL(req.url ?? "/", "http://127.0.0.1").pathname,
			headers,
			contentLength: Number.isFinite(declaredLength) ? declaredLength : 0,
			readBody: () => {
				if (bodyPromise === null) {
					bodyPromise = this.readBodyBuffer(req);
				}
				return bodyPromise;
			},
		};
	}

	private readBodyBuffer(req: IncomingMessage): Promise<Buffer> {
		return new Promise<Buffer>((resolve, reject) => {
			const chunks: Buffer[] = [];
			let total = 0;
			let settled = false;
			req.on("data", (chunk: Buffer) => {
				if (settled) return;
				total += chunk.length;
				if (total > this.deps.bodyCapBytes) {
					settled = true;
					const err = new Error("Payload too large") as Error & {
						statusCode?: number;
					};
					err.statusCode = 413;
					reject(err);
					// DRAIN the remainder so HTTP framing completes and the 413
					// response still reaches the client — destroying the socket
					// here would kill the connection mid-upload (UND_ERR_SOCKET).
					req.resume();
					return;
				}
				chunks.push(chunk);
			});
			req.on("end", () => {
				if (settled) return;
				settled = true;
				resolve(Buffer.concat(chunks));
			});
			req.on("error", (err) => {
				if (settled) return;
				settled = true;
				reject(err);
			});
		});
	}

	private async readBodyText(req: IncomingMessage): Promise<string> {
		const buf = await this.readBodyBuffer(req);
		return buf.toString("utf8");
	}

	private sendJson(
		res: ServerResponse,
		status: number,
		json: Record<string, unknown>,
		extraHeaders: Record<string, string> = {},
	): void {
		res.writeHead(status, {
			"content-type": "application/json",
			...extraHeaders,
		});
		res.end(JSON.stringify(json));
	}

	/** Ingress-response shape passthrough used by tests driving the socket lane. */
	handleIngressResponse(response: IngressResponse, res: ServerResponse): void {
		this.sendJson(res, response.status, response.json);
	}
}

/**
 * Session-key ladder failures render EXACTLY like api_server.py: the 400s are
 * plain {message,type} dicts (@2337/@2343); the 403 rides the shared
 * gateway_auth_error family from the trust-engine verdict.
 */
function sessionKeyError(
	verdict: Extract<SessionKeyVerdict, { ok: false }>,
): Record<string, unknown> {
	return { error: { message: verdict.error, type: verdict.errorType } };
}

/** _openai_error envelope ({message,type,param,code}) — imported type reused. */
function openaiErrorJson(
	message: string,
	code?: string | undefined,
): Record<string, unknown> {
	return openaiErrorBody(message, "invalid_request_error", code);
}

/** Multi-message input: the LAST message's content wins (Hermes parity). */
function lastMessageContent(rawInput: readonly unknown[]): string {
	const last = rawInput[rawInput.length - 1] as
		| { content?: unknown }
		| undefined;
	if (last === undefined || typeof last !== "object" || last === null) {
		return "";
	}
	const content = (last as { content?: unknown }).content;
	return typeof content === "string" ? content : "";
}

/** Map a typed registry event onto its snake_case wire payload. */
function runEventData(
	event: RunEvent,
	base: Record<string, unknown>,
): Record<string, unknown> | null {
	switch (event.type) {
		case "assistant.delta":
			return { ...base, delta: event.text };
		case "tool.started":
			return {
				...base,
				tool_name: event.toolName,
				...(event.preview !== undefined ? { preview: event.preview } : {}),
			};
		case "tool.completed":
			return {
				...base,
				tool_name: event.toolName,
				...(event.duration !== undefined ? { duration: event.duration } : {}),
				...(event.isError !== undefined ? { error: event.isError } : {}),
			};
		case "tool.failed":
			return {
				...base,
				tool_name: event.toolName,
				...(event.preview !== undefined ? { preview: event.preview } : {}),
			};
		case "approval.request":
			return {
				...base,
				approval_id: event.approvalId,
				command: event.command,
				choices: [...event.choices],
			};
		case "approval.responded":
			return {
				...base,
				approval_id: event.approvalId,
				choice: event.choice,
				resolved: event.resolved,
			};
		case "run.steered":
			return { ...base, text: event.text, accepted: true };
		case "run.completed":
			return {
				...base,
				output: event.output,
				...(event.pendingSteer !== undefined
					? { pending_steer: event.pendingSteer }
					: {}),
			};
		case "run.failed":
			return { ...base, error: event.error };
		case "run.cancelled":
			return base;
		default:
			return null;
	}
}

function lowerHeaders(req: IncomingMessage): Record<string, string> {
	const out: Record<string, string> = {};
	for (const [key, value] of Object.entries(req.headers)) {
		if (value === undefined) continue;
		out[key.toLowerCase()] = Array.isArray(value) ? value.join(", ") : value;
	}
	return out;
}
