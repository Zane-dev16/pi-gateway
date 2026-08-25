// pi_platforms/a2a/a2a-fixture — the REAL-engine fixture for the A2A
// shape-delta rows (MSGraphFixture pattern): the actual A2AAdapter driven at
// its HTTP-handler seams with an INJECTED clock, synthesized requests, a
// scripted push transport, and mkdtemp-isolated persistence. NO stubbed
// return values — rows drive the real auth gates, verdict ladder, task store,
// SSE emitter, rate limiter, and reply plane.
//
// NO REAL NETWORK: handler seams are invoked directly; push callbacks land in
// an in-process recorder; SSE frames are captured into an in-process sink.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";

import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";

import { A2AAdapter } from "./a2a-adapter.js";
import type { HandlerResponse, PushTransport, SseSink } from "./a2a-adapter.js";
import type { EnvReader } from "./security.js";

/** Injected epoch-ms clock (flake discipline): starts fixed; advance() moves it. */
export class FixtureClock {
	constructor(private nowValue: number = 1_700_000_000_000) {}
	get nowMs(): number {
		return this.nowValue;
	}
	advance(ms: number): void {
		this.nowValue += ms;
	}
}

export interface RecordedPushCall {
	url: string;
	body: string;
	headers: Record<string, string>;
}

/**
 * Scripted push transport: records every callback attempt; statuses consumed
 * FIFO from `statuses` (default 200 when exhausted).
 */
export class RecordingPushTransport implements PushTransport {
	readonly calls: RecordedPushCall[] = [];
	private statusQueue: number[] = [];

	queueStatuses(...statuses: number[]): void {
		this.statusQueue.push(...statuses);
	}

	async postCallback(
		url: string,
		body: string,
		headers: Record<string, string>,
	): Promise<{ status: number }> {
		this.calls.push({ url, body, headers });
		const status = this.statusQueue.shift() ?? 200;
		return { status };
	}
}

export interface A2aFixtureOptions {
	env?: Record<string, string> | undefined;
	config?: import("./a2a-adapter.js").A2aAdapterConfig | undefined;
	/** Reply script over the FRAMED inbound text (default raft-parity echo). */
	replyScript?: ((framedText: string) => string | undefined) | undefined;
	attachGuard?: boolean | undefined;
	pollTickMs?: number | undefined;
}

export interface FixtureJsonRpcResponse extends HandlerResponse {
	json: Record<string, unknown>;
	text: string;
	sse: string;
}

export class A2aFixture {
	readonly adapter: A2AAdapter;
	readonly clock = new FixtureClock();
	readonly scheduler = new ManualScheduler();
	readonly push = new RecordingPushTransport();
	readonly envTable: Record<string, string>;

	private readonly storageDir: string;
	private replyScript: ((framedText: string) => string | undefined) | undefined;

	constructor(opts: A2aFixtureOptions = {}) {
		this.envTable = { ...(opts.env ?? {}) };
		this.storageDir = mkdtempSync(path.join(tmpdir(), "a2a-fixture-"));
		const envReader: EnvReader = (key) => this.envTable[key];
		this.replyScript = opts.replyScript;
		this.adapter = new A2AAdapter({
			config: opts.config,
			envReader,
			nowMs: () => this.clock.nowMs,
			pushTransport: this.push,
			storageDir: this.storageDir,
			pollTickMs: opts.pollTickMs ?? 3,
		});
		if (opts.attachGuard !== false) {
			this.adapter.attachStandardGuard({
				spawner: this.scheduler.spawner,
				replyFor: (framed) => this.replyScript?.(framed),
			});
		}
	}

	setReplyScript(script: (framedText: string) => string | undefined): void {
		this.replyScript = script;
	}

	/** Hold gate passthrough (bounded-window / keepalive rows). */
	holdTurns(on: boolean): void {
		this.adapter.holdTurns(on);
	}

	advance(ms: number): void {
		this.clock.advance(ms);
	}

	dispose(): void {
		try {
			rmSync(this.storageDir, { recursive: true, force: true });
		} catch {
			/* best-effort cleanup */
		}
	}

	// ── transport-level requests ─────────────────────────────────────────────

	async postRaw(input: {
		path?: string | undefined;
		method?: string | undefined;
		params?: unknown;
		id?: unknown;
		headers?: Record<string, string> | undefined;
		rawBody?: string | Buffer | undefined;
		clientIp?: string | undefined;
		version?: string | undefined;
	}): Promise<HandlerResponse> {
		let headers = { ...(input.headers ?? {}) };
		if (input.version !== undefined) headers["A2A-Version"] = input.version;
		let rawBody: Buffer;
		if (input.rawBody !== undefined) {
			rawBody = Buffer.isBuffer(input.rawBody)
				? input.rawBody
				: Buffer.from(input.rawBody, "utf8");
		} else {
			const payload: Record<string, unknown> = {};
			if (input.method !== undefined) payload["method"] = input.method;
			if (input.params !== undefined) payload["params"] = input.params;
			payload["jsonrpc"] = "2.0";
			payload["id"] = input.id ?? 1;
			rawBody = Buffer.from(JSON.stringify(payload), "utf8");
		}
		if (
			headers["content-length"] === undefined &&
			input.rawBody === undefined
		) {
			headers = { ...headers, "content-length": String(rawBody.length) };
		}
		return this.adapter.handlePost({
			path: input.path ?? "/",
			headers,
			rawBody,
			clientIp: input.clientIp ?? "127.0.0.1",
		});
	}

	/** JSON-RPC POST with a captured SSE sink attached. */
	async postRpc(
		input: {
			path?: string | undefined;
			method?: string | undefined;
			params?: unknown;
			id?: unknown;
			headers?: Record<string, string> | undefined;
			rawBody?: string | Buffer | undefined;
			clientIp?: string | undefined;
			version?: string | undefined;
		},
		sink?: SseSink | undefined,
	): Promise<FixtureJsonRpcResponse> {
		let headers = { ...(input.headers ?? {}) };
		if (input.version !== undefined) headers["A2A-Version"] = input.version;
		let rawBody: Buffer;
		if (input.rawBody !== undefined) {
			rawBody = Buffer.isBuffer(input.rawBody)
				? input.rawBody
				: Buffer.from(input.rawBody, "utf8");
		} else {
			const payload: Record<string, unknown> = {};
			if (input.method !== undefined) payload["method"] = input.method;
			if (input.params !== undefined) payload["params"] = input.params;
			payload["jsonrpc"] = "2.0";
			payload["id"] = input.id ?? 1;
			rawBody = Buffer.from(JSON.stringify(payload), "utf8");
		}
		if (
			headers["content-length"] === undefined &&
			input.rawBody === undefined
		) {
			headers = { ...headers, "content-length": String(rawBody.length) };
		}
		const chunks: string[] = [];
		const resp = await this.adapter.handlePost({
			path: input.path ?? "/",
			headers,
			rawBody,
			clientIp: input.clientIp ?? "127.0.0.1",
			sseSink: sink ?? { write: (chunk) => chunks.push(chunk) },
		});
		const json =
			resp.contentType === "application/json" &&
			resp.body !== null &&
			typeof resp.body === "object"
				? (resp.body as Record<string, unknown>)
				: {};
		const text = typeof resp.body === "string" ? resp.body : "";
		return { ...resp, json, text, sse: chunks.join("") };
	}

	get(
		path: string,
		headers: Record<string, string> = {},
		clientIp = "127.0.0.1",
	): FixtureJsonRpcResponse {
		const resp = this.adapter.handleGet(path, headers, clientIp);
		const json =
			resp.contentType === "application/json" &&
			resp.body !== null &&
			typeof resp.body === "object"
				? (resp.body as Record<string, unknown>)
				: {};
		const text = typeof resp.body === "string" ? resp.body : "";
		return { ...resp, json, text, sse: "" };
	}

	// ── A2A wire-shape builders ────────────────────────────────────────────────

	/** v1.0 Message with a single text part (protocol.py:text_message shape). */
	message(
		text: string,
		extras: Record<string, unknown> = {},
	): Record<string, unknown> {
		const msg: Record<string, unknown> = {
			role: "ROLE_USER",
			parts: [{ text, mediaType: "text/plain" }],
			messageId: `fixture-msg-${text.length}-${Math.abs(hash(text))}`,
			...extras,
		};
		return msg;
	}

	sendParams(
		text: string,
		extras: Record<string, unknown> = {},
	): Record<string, unknown> {
		return { message: this.message(text), ...extras };
	}

	errorCode(resp: FixtureJsonRpcResponse): number | null {
		const err = resp.json["error"];
		if (err !== null && typeof err === "object") {
			return (err as Record<string, unknown>)["code"] as number;
		}
		return null;
	}

	errorMessage(resp: FixtureJsonRpcResponse): string {
		const err = resp.json["error"];
		if (err !== null && typeof err === "object") {
			return String((err as Record<string, unknown>)["message"] ?? "");
		}
		return "";
	}
}

function hash(text: string): number {
	let h = 0;
	for (let i = 0; i < text.length; i++) {
		h = (h * 31 + text.charCodeAt(i)) | 0;
	}
	return h;
}

/** Deterministic small wait letting spawned guard frames/loops progress. */
export async function settle(ms = 8): Promise<void> {
	await new Promise<void>((resolve) => setTimeout(resolve, ms));
}

/** Poll until the predicate holds or the budget runs out (real-time bound). */
export async function waitFor(
	predicate: () => boolean,
	timeoutMs = 5_000,
	everyMs = 4,
): Promise<boolean> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (predicate()) return true;
		await new Promise<void>((resolve) => setTimeout(resolve, everyMs));
	}
	return predicate();
}

export function makeA2aFixture(opts?: A2aFixtureOptions): A2aFixture {
	return new A2aFixture(opts);
}
