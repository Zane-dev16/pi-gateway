// pi_platforms/webhook/server — the in-process node:http binding for the
// webhook reference adapter. Ephemeral loopback port; NO external network.
// The trust pipeline itself is framework-free (http-ingress.ts); this layer
// only normalizes requests, enforces the body cap mid-stream, and renders
// responses incl. the /v1/runs SSE stream (api_server-class unbounded lane).
//
// Hermes anchors: api_server.py:_handle_run_events (@7937) — SSE headers
// (text/event-stream, X-Accel-Buffering: no), `: keepalive` comments, typed
// event frames `event: <type>\ndata: <json>\n\n`.

import type { IncomingMessage, Server, ServerResponse } from "node:http";
import { createServer } from "node:http";
import type {
	WebhookIngressPipeline,
	IngressRequest,
	IngressResponse,
} from "./http-ingress.js";
import type { CompletionsEndpoint, CompletionsRequest } from "./completions.js";
import type { RunRegistry } from "./runs.js";

export interface WebhookServerDeps {
	pipeline: WebhookIngressPipeline;
	completions: CompletionsEndpoint;
	runs: RunRegistry;
	/** Body-size cap enforced MID-STREAM (lying Content-Length defense). */
	bodyCapBytes: number;
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

		if (path === "/v1/chat/completions" || path.startsWith("/v1/runs")) {
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
			this.sendJson(res, response.status, response.json, response.headers);
			return;
		}

		const runsMatch =
			/^\/v1\/runs\/([^/]+)(\/events|\/approvals|\/steer|\/stop)?$/.exec(path);

		if (path === "/v1/runs") {
			if (method !== "POST") {
				this.sendJson(res, 405, { error: "Method not allowed" });
				return;
			}
			const bodyText = await this.readBodyText(req);
			let input = "";
			try {
				const parsed = JSON.parse(bodyText) as { input?: unknown };
				input =
					typeof parsed.input === "string"
						? parsed.input
						: JSON.stringify(parsed.input ?? "");
			} catch {
				input = bodyText;
			}
			const runId = this.startRunWithDefaultExecutor?.(input);
			if (runId === undefined) {
				this.sendJson(res, 503, {
					error: "no run executor wired for HTTP lane",
				});
				return;
			}
			this.sendJson(res, 202, { run_id: runId, status: "started" });
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
				this.sendJson(res, 404, { error: `Unknown run: ${runId}` });
				return;
			}
			this.sendJson(res, 200, view as unknown as Record<string, unknown>);
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

		if (sub === "/approvals") {
			const choice = String(payload["choice"] ?? "");
			const outcome = this.deps.runs.respondApprovalForRun(runId, choice);
			if (outcome.ok) {
				this.sendJson(res, 200, {
					run_id: runId,
					choice: outcome.choice,
					status: "approval_responded",
				});
			} else if (outcome.code === "invalid_choice") {
				this.sendJson(res, 400, { error: { code: "invalid_choice" } });
			} else {
				this.sendJson(res, 409, { error: { code: outcome.code } });
			}
			return;
		}
		if (sub === "/steer") {
			const text = String(
				payload["input"] ?? payload["message"] ?? payload["text"] ?? "",
			);
			if (text.length === 0) {
				this.sendJson(res, 400, { error: { code: "steer_text_required" } });
				return;
			}
			const outcome = this.deps.runs.steer(runId, text);
			if (!outcome.ok) {
				this.sendJson(res, 409, { error: { code: outcome.code } });
				return;
			}
			this.sendJson(res, 200, { run_id: runId, status: "steered" });
			return;
		}
		if (sub === "/stop") {
			const outcome = this.deps.runs.stop(runId);
			if (!outcome.ok) {
				this.sendJson(res, 409, { error: { code: outcome.code } });
				return;
			}
			this.sendJson(res, 200, { run_id: runId, status: "stopping" });
			return;
		}
		this.sendJson(res, 404, { error: "Not found" });
	}

	/** Runs started over HTTP use the adapter-wired default executor. */
	startRunWithDefaultExecutor: ((input: string) => string) | null = null;

	/** SSE: replay buffered events, then stream live until terminal. */
	private streamRunEvents(runId: string, res: ServerResponse): void {
		const view = this.deps.runs.status(runId);
		if (view === null) {
			this.sendJson(res, 404, { error: `Unknown run: ${runId}` });
			return;
		}
		res.writeHead(200, {
			"content-type": "text/event-stream",
			"cache-control": "no-cache",
			connection: "keep-alive",
			"x-accel-buffering": "no",
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

		const writeEvent = (event: { type: string }): void => {
			if (closed) return;
			res.write(`event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`);
			if (
				event.type === "run.completed" ||
				event.type === "run.failed" ||
				event.type === "run.cancelled"
			) {
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

function lowerHeaders(req: IncomingMessage): Record<string, string> {
	const out: Record<string, string> = {};
	for (const [key, value] of Object.entries(req.headers)) {
		if (value === undefined) continue;
		out[key.toLowerCase()] = Array.isArray(value) ? value.join(", ") : value;
	}
	return out;
}
