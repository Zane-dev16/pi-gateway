// pi_platforms/line/line-fixture — the REAL-engine fixture for the LINE
// shape-delta rows (MSGraphFixture/WaCloudFixture pattern): the actual
// LineWebhookAdapter driven at its HTTP-handler seams with an injected clock,
// an instrumented Reply/Push API transport (single-use token enforcement —
// the VENDOR's semantic, modeled server-side), and signed wire builders.
//
// NO REAL NETWORK: everything is in-process handler invocation; the transport
// never leaves the process. Vendor error SHAPES are modeled as generic ladder
// outcomes, never snapshotted strings.

import { createHmac } from "node:crypto";

import { LineWebhookAdapter } from "./line-webhook-adapter.js";
import type {
	LineAdapterConfig,
	LineApiTransport,
	LineMessage,
} from "./line-webhook-adapter.js";
import type { SendResult } from "../../pi_gateway/streaming/adapter-seam.js";
import type { WireBehavior } from "../conformance/wire.js";
import {
	FIXTURE_CHANNEL_ACCESS_TOKEN,
	FIXTURE_CHANNEL_SECRET,
} from "./fixture-secrets.js";

/**
 * Injected epoch-ms clock (flake discipline): starts at a fixed instant;
 * advance() moves it — the reply-token TTL row mutates THIS.
 */
export class FixtureClock {
	constructor(private nowValue: number = 1_700_000_000_000) {}
	get nowMs(): number {
		return this.nowValue;
	}
	advance(ms: number): void {
		this.nowValue += ms;
	}
}

export const DEFAULT_FIXTURE_CONFIG: LineAdapterConfig = {
	port: 8646,
	webhook_path: "/line/webhook",
	allow_all_users: true,
};

/** Sign a raw body the way LINE's sender does (base64 HMAC-SHA256). */
export function signLineBody(body: string | Buffer, secret?: string): string {
	return createHmac("sha256", secret ?? FIXTURE_CHANNEL_SECRET)
		.update(body)
		.digest("base64");
}

export interface LineFixtureOptions {
	config?: LineAdapterConfig | undefined;
	/** Resolve required secrets undefined (loud-disable probes). */
	withSecret?: boolean | undefined;
	dedupCap?: number | undefined;
}

/**
 * Instrumented Reply/Push endpoint. Enforces the VENDOR's single-use reply
 * token semantic server-side: a burned token presented twice is rejected
 * BEFORE any scripted behavior applies.
 */
export class FakeLineApi implements LineApiTransport {
	readonly replyCalls: Array<{ token: string; texts: string[] }> = [];
	readonly pushCalls: Array<{ chatId: string; texts: string[] }> = [];
	private readonly burnedReplyTokens = new Set<string>();
	private readonly scripts: {
		reply: WireBehavior[];
		push: WireBehavior[];
	} = { reply: [], push: [] };

	constructor(private readonly secret: string = FIXTURE_CHANNEL_SECRET) {}

	script(
		opKind: "reply" | "push",
		...behaviors: Array<Extract<WireBehavior, { kind: "fail" | "timeout" }>>
	): void {
		this.scripts[opKind].push(...behaviors);
	}

	private take(opKind: "reply" | "push"): WireBehavior {
		const queue = this.scripts[opKind];
		if (queue === undefined || queue.length === 0) return { kind: "ok" };
		return queue.shift() as WireBehavior;
	}

	private textsOf(messages: LineMessage[]): string[] {
		return messages.map((m) => m.text);
	}

	async reply(
		token: string,
		messages: LineMessage[],
		metadata?: Record<string, unknown>,
	): Promise<SendResult> {
		this.replyCalls.push({ token, texts: this.textsOf(messages) });
		const rejected = this.rejectionScript(messages, metadata);
		if (rejected !== null) return rejected;
		if (this.burnedReplyTokens.has(token)) {
			// Vendor shape: a consumed reply token is REJECTED by the API.
			return {
				success: false,
				error: "reply rejected: reply token invalid or already used",
			};
		}
		this.burnedReplyTokens.add(token);
		const behavior = this.take("reply");
		if (behavior.kind === "fail") {
			return { success: false, error: behavior.error };
		}
		if (behavior.kind === "timeout") {
			return { success: false, error: "request timed out" };
		}
		return { success: true, messageId: `reply-${this.replyCalls.length}` };
	}

	async push(
		chatId: string,
		messages: LineMessage[],
		metadata?: Record<string, unknown>,
	): Promise<SendResult> {
		this.pushCalls.push({ chatId, texts: this.textsOf(messages) });
		const rejected = this.rejectionScript(messages, metadata);
		if (rejected !== null) return rejected;
		const behavior = this.take("push");
		if (behavior.kind === "fail") {
			return { success: false, error: behavior.error };
		}
		if (behavior.kind === "timeout") {
			return { success: false, error: "request timed out" };
		}
		return { success: true, messageId: `push-${this.pushCalls.length}` };
	}

	replyCount(): number {
		return this.replyCalls.length;
	}

	/** Markdown-rendering rejection script (reference-fixture parity): a forced
	 * formatting error fails unless this IS already the §6.1 plain-text body. */
	private rejectionScript(
		messages: LineMessage[],
		metadata?: Record<string, unknown> | undefined,
	): SendResult | null {
		if ((metadata?.["forceFormattingError"] ?? null) !== true) return null;
		const joined = messages.map((m) => m.text).join("\n");
		if (!joined.startsWith("(Response formatting failed, plain text:")) {
			return { success: false, error: "Bad Request: can't parse entities" };
		}
		return null;
	}

	pushCount(): number {
		return this.pushCalls.length;
	}

	get channelSecret(): string {
		return this.secret;
	}
}

/** Handler response surface the rows assert against. */
export interface FixtureResponse {
	status: number;
	contentType: string | undefined;
	text: string;
}

export class LineFixture {
	readonly adapter: LineWebhookAdapter;
	readonly api: FakeLineApi;
	readonly clock = new FixtureClock();

	constructor(opts: LineFixtureOptions = {}) {
		const config: LineAdapterConfig = {
			...DEFAULT_FIXTURE_CONFIG,
			...(opts.config ?? {}),
		};
		this.api =
			opts.config?.channel_secret !== undefined
				? new FakeLineApi(opts.config.channel_secret)
				: new FakeLineApi();
		this.adapter = new LineWebhookAdapter({
			config,
			nowMs: () => this.clock.nowMs,
			transport: this.api,
			dedupCap: opts.dedupCap,
			secretReader: (name) => {
				if (opts.withSecret === false) return undefined;
				if (name === "LINE_CHANNEL_ACCESS_TOKEN") {
					return FIXTURE_CHANNEL_ACCESS_TOKEN;
				}
				if (name === "LINE_CHANNEL_SECRET") return FIXTURE_CHANNEL_SECRET;
				return undefined;
			},
		});
		this.adapter.attachStandardGuard();
	}

	advance(ms: number): void {
		this.clock.advance(ms);
	}

	dispose(): void {
		/* pure in-process fixture */
	}

	// ── HTTP-level requests ──────────────────────────────────────────────────

	postRaw(input: {
		headers?: Record<string, string> | undefined;
		body: string | Buffer;
	}): Promise<FixtureResponse> {
		const raw = Buffer.isBuffer(input.body)
			? input.body
			: Buffer.from(input.body, "utf8");
		return this.adapter
			.handleWebhookPost({
				headers: input.headers,
				rawBody: raw,
			})
			.then(toFixtureResponse);
	}

	/** Signed POST: signs THIS body with the fixture channel secret. */
	async postSigned(body: string | Buffer): Promise<FixtureResponse> {
		const raw = Buffer.isBuffer(body) ? body : Buffer.from(body, "utf8");
		return this.postRaw({
			headers: {
				"x-line-signature": signLineBody(raw, this.api.channelSecret),
			},
			body: raw,
		});
	}

	// ── LINE webhook wire-shape builders ─────────────────────────────────────

	messageEvent(extras: {
		webhookEventId?: string | undefined;
		sourceType?: "user" | "group" | "room" | undefined;
		sourceId?: string | undefined;
		userId?: string | undefined;
		text?: string | undefined;
		msgType?: string | undefined;
		messageId?: string | undefined;
		replyToken?: string | undefined;
	}): Record<string, unknown> {
		const srcId = extras.sourceId ?? "U-user1";
		const source: Record<string, unknown> = {
			type: extras.sourceType ?? "user",
			userId: extras.userId ?? srcId,
		};
		if ((extras.sourceType ?? "user") === "group") {
			source["type"] = "group";
			source["groupId"] = srcId;
		} else if (extras.sourceType === "room") {
			source["roomId"] = srcId;
		}
		const event: Record<string, unknown> = {
			type: "message",
			source,
			replyToken:
				extras.replyToken ?? `rt-${Math.random().toString(36).slice(2)}`,
			message: {
				id: extras.messageId ?? `msg-${Math.random().toString(36).slice(2)}`,
				type: extras.msgType ?? "text",
			},
		};
		if (extras.webhookEventId !== undefined)
			event["webhookEventId"] = extras.webhookEventId;
		if (extras.text !== undefined)
			(event["message"] as Record<string, unknown>)["text"] = extras.text;
		return event;
	}

	postbackEvent(extras: {
		data: unknown;
		sourceType?: "user" | "group" | "room" | undefined;
		sourceId?: string | undefined;
		replyToken?: string | undefined;
		webhookEventId?: string | undefined;
	}): Record<string, unknown> {
		const srcId = extras.sourceId ?? "U-user1";
		const source: Record<string, unknown> = {
			type: extras.sourceType ?? "user",
			userId: srcId,
		};
		if (extras.sourceType === "group") {
			source["groupId"] = srcId;
		} else if (extras.sourceType === "room") {
			source["roomId"] = srcId;
		}
		const event: Record<string, unknown> = {
			type: "postback",
			source,
			replyToken:
				extras.replyToken ?? `pb-${Math.random().toString(36).slice(2)}`,
			postback: {
				data:
					typeof extras.data === "string"
						? extras.data
						: JSON.stringify(extras.data),
			},
		};
		if (extras.webhookEventId !== undefined)
			event["webhookEventId"] = extras.webhookEventId;
		return event;
	}

	envelope(events: unknown[]): string {
		return JSON.stringify({ events });
	}

	async postEvents(events: unknown[]): Promise<FixtureResponse> {
		return this.postSigned(this.envelope(events));
	}
}

function toFixtureResponse(resp: {
	status: number;
	contentType?: "text/plain" | "application/json" | undefined;
	body?: string | Record<string, never> | undefined;
}): FixtureResponse {
	return {
		status: resp.status,
		contentType: resp.contentType,
		text: typeof resp.body === "string" ? resp.body : "",
	};
}

export function makeLineFixture(opts?: LineFixtureOptions): LineFixture {
	return new LineFixture(opts);
}
