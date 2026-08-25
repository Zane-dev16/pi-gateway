// pi_platforms/google-chat/gchat-fixture — the REAL-engine fixture for the
// Google Chat shape-delta rows (MSGraphFixture/WaCloudFixture pattern): the
// actual GoogleChatWebhookAdapter driven at its HTTP-handler seams with an
// injected clock, a claims-based OIDC verifier + token minter, an instrumented
// Chat REST transport, and the three vendor envelope shapes.
//
// NO REAL NETWORK: token minting/verification is in-process (HMAC over a
// claims blob — the OIDC semantics under test are audience binding, email
// claim allowlist, tamper rejection); the Chat API never leaves the process.

import { createHmac, timingSafeEqual } from "node:crypto";

import { GoogleChatWebhookAdapter } from "./google-chat-adapter.js";
import type {
	GchatAdapterConfig,
	GchatApiResponse,
	GchatTransport,
	OidcTokenVerifier,
} from "./google-chat-adapter.js";
import {
	FIXTURE_HTTP_EVENTS_AUDIENCE,
	FIXTURE_SA_EMAIL,
} from "./fixture-secrets.js";

/**
 * Injected epoch-ms clock (flake discipline): starts at a fixed instant;
 * advance() moves it — the dedup-TTL row mutates THIS.
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

const TOKEN_SIGNING_KEY = "gchat-fixture-signing-key";

/** Mint a fixture bearer token: b64(claims).hmac — audience-bound. */
export function mintBearerToken(
	claims: Record<string, unknown>,
	signingKey: string = TOKEN_SIGNING_KEY,
): string {
	const payload = Buffer.from(JSON.stringify(claims), "utf8").toString(
		"base64",
	);
	const sig = createHmac("sha256", signingKey).update(payload).digest("hex");
	return `${payload}.${sig}`;
}

/**
 * Claims-based verifier carrying google-auth's CONTRACT: throws on any
 * signature/shape failure, resolves verified claims on success. Audience
 * binding is checked HERE exactly like verify_oauth2_token(token, req, aud).
 */
export class ClaimsOidcVerifier implements OidcTokenVerifier {
	constructor(
		private readonly signingKey: string = TOKEN_SIGNING_KEY,
		private readonly nowMs: () => number = () => Date.now(),
	) {}

	async verify(
		token: string,
		audience: string,
	): Promise<Record<string, unknown>> {
		const dot = token.indexOf(".");
		if (dot <= 0) throw new Error("malformed token");
		const payload = token.slice(0, dot);
		const presented = token.slice(dot + 1);
		const expected = createHmac("sha256", this.signingKey)
			.update(payload)
			.digest("hex");
		const a = Buffer.from(presented, "utf8");
		const b = Buffer.from(expected, "utf8");
		if (a.length !== b.length || !timingSafeEqual(a, b)) {
			throw new Error("signature verification failed");
		}
		let claims: Record<string, unknown>;
		try {
			claims = JSON.parse(Buffer.from(payload, "base64").toString("utf8"));
		} catch {
			throw new Error("malformed token claims");
		}
		if (String(claims["aud"] ?? "") !== audience) {
			throw new Error(`audience mismatch: expected ${audience}`);
		}
		if (
			typeof claims["exp"] === "number" &&
			claims["exp"] * 1000 < this.nowMs()
		) {
			throw new Error("token expired");
		}
		return claims;
	}
}

type ScriptedBehavior =
	| { kind: "ok"; name?: string }
	| { kind: "status"; status: number; error?: string };

export interface GchatApiCall {
	op: "create" | "patch";
	target: string;
	body: Record<string, unknown>;
	metadata: Record<string, unknown>;
}

/**
 * Instrumented Chat REST endpoint. Records every create/patch with its FULL
 * body so rows assert wire shapes (thread.name, messageReplyOption,
 * cardsV2, text dialect); scripts status-code behaviors FIFO per op.
 */
export class FakeChatApi implements GchatTransport {
	readonly calls: GchatApiCall[] = [];
	private readonly scripts: {
		create: ScriptedBehavior[];
		patch: ScriptedBehavior[];
	} = { create: [], patch: [] };
	private seq = 0;

	script(opKind: "create" | "patch", ...behaviors: ScriptedBehavior[]): void {
		this.scripts[opKind].push(...behaviors);
	}

	private take(opKind: "create" | "patch"): ScriptedBehavior {
		const queue = this.scripts[opKind];
		if (queue === undefined || queue.length === 0) return { kind: "ok" };
		return queue.shift() as ScriptedBehavior;
	}

	async createMessage(
		chatId: string,
		body: Record<string, unknown>,
		metadata: Record<string, unknown> = {},
	): Promise<GchatApiResponse> {
		this.calls.push({ op: "create", target: chatId, body, metadata });
		const rejected = this.rejectionScript(body, metadata);
		if (rejected !== null) return rejected;
		const behavior = this.take("create");
		if (behavior.kind === "status") {
			return {
				success: false,
				status: behavior.status,
				error: behavior.error ?? `HTTP ${behavior.status}`,
			};
		}
		this.seq += 1;
		return { success: true, messageId: `spaces/x/messages/${this.seq}` };
	}

	async patchMessage(
		messageName: string,
		body: Record<string, unknown>,
		metadata: Record<string, unknown> = {},
	): Promise<GchatApiResponse> {
		this.calls.push({ op: "patch", target: messageName, body, metadata });
		const rejected = this.rejectionScript(body, metadata);
		if (rejected !== null) return rejected;
		const behavior = this.take("patch");
		if (behavior.kind === "status") {
			return {
				success: false,
				status: behavior.status,
				error: behavior.error ?? `HTTP ${behavior.status}`,
			};
		}
		return { success: true, messageId: messageName };
	}

	createsOf(target?: string): GchatApiCall[] {
		return this.calls.filter(
			(c) => c.op === "create" && (target === undefined || c.target === target),
		);
	}

	/** Markdown-rendering rejection script (reference-fixture parity): a forced
	 * formatting error fails unless this IS already the §6.1 plain-text body. */
	private rejectionScript(
		body: Record<string, unknown>,
		metadata?: Record<string, unknown> | undefined,
	): GchatApiResponse | null {
		if ((metadata?.["forceFormattingError"] ?? null) !== true) return null;
		const content = String(body["text"] ?? "");
		if (!content.startsWith("(Response formatting failed, plain text:")) {
			return {
				success: false,
				status: 400,
				error: "Bad Request: can't parse entities",
			};
		}
		return null;
	}

	textOfLastCreate(): string {
		for (let i = this.calls.length - 1; i >= 0; i--) {
			const call = this.calls[i] as GchatApiCall;
			if (call.op === "create" && typeof call.body["text"] === "string") {
				return call.body["text"];
			}
		}
		return "";
	}
}

export interface FixtureResponse {
	status: number;
	contentType: string | undefined;
	text: string;
	json: Record<string, unknown>;
}

export interface GchatFixtureOptions {
	config?: GchatAdapterConfig | undefined;
	/** Resolve required secrets undefined (loud-disable probes). */
	withSecret?: boolean | undefined;
	audience?: string | undefined;
	saEmails?: readonly string[] | undefined;
	dedupCap?: number | undefined;
	verifier?: OidcTokenVerifier | undefined;
}

export const DEFAULT_GCHAT_CONFIG: GchatAdapterConfig = {
	http_events_url: FIXTURE_HTTP_EVENTS_AUDIENCE,
};

export class GchatFixture {
	readonly adapter: GoogleChatWebhookAdapter;
	readonly api = new FakeChatApi();
	readonly verifier: OidcTokenVerifier;
	readonly clock = new FixtureClock();
	readonly audience: string;
	readonly saEmails: readonly string[];

	constructor(opts: GchatFixtureOptions = {}) {
		this.audience = opts.audience ?? FIXTURE_HTTP_EVENTS_AUDIENCE;
		this.saEmails = opts.saEmails ?? [FIXTURE_SA_EMAIL];
		this.verifier =
			opts.verifier ??
			new ClaimsOidcVerifier(TOKEN_SIGNING_KEY, () => this.clock.nowMs);
		this.adapter = new GoogleChatWebhookAdapter({
			config: { ...DEFAULT_GCHAT_CONFIG, ...(opts.config ?? {}) },
			nowMs: () => this.clock.nowMs,
			transport: this.api,
			verifier: this.verifier,
			dedupCap: opts.dedupCap,
			secretReader: (name) => {
				if (opts.withSecret === false) return undefined;
				if (name === "GOOGLE_CHAT_HTTP_EVENTS_AUDIENCE") return this.audience;
				if (name === "GOOGLE_CHAT_HTTP_EVENTS_SERVICE_ACCOUNT_EMAIL") {
					return this.saEmails.join(",");
				}
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

	bearerFor(saEmail: string = this.saEmails[0] ?? FIXTURE_SA_EMAIL): string {
		return mintBearerToken({
			aud: this.audience,
			email: saEmail,
			iss: "https://cloud.google.com/iap",
			exp: Math.floor((this.clock.nowMs + 300_000) / 1000),
		});
	}

	postRaw(input: {
		headers?: Record<string, string> | undefined;
		body: string | Buffer;
	}): Promise<FixtureResponse> {
		const raw = Buffer.isBuffer(input.body)
			? input.body
			: Buffer.from(input.body, "utf8");
		return this.adapter
			.handleHttpEventPost({
				headers: input.headers,
				rawBody: raw,
			})
			.then(toFixtureResponse);
	}

	/** Signed POST: minted bearer + JSON body. */
	postSigned(envelope: unknown): Promise<FixtureResponse> {
		return this.postRaw({
			headers: {
				authorization: `Bearer ${this.bearerFor()}`,
				"content-type": "application/json",
			},
			body: JSON.stringify(envelope),
		});
	}

	// ── envelope builders (the three accepted formats) ───────────────────────

	workspaceAddonsEnvelope(extras: {
		message?: Record<string, unknown> | undefined;
		space?: Record<string, unknown> | undefined;
		type?: string | undefined;
	}): Record<string, unknown> {
		const message = extras.message ?? this.chatMessage({});
		const space = extras.space ?? this.defaultSpace();
		return {
			type: extras.type ?? "google.workspace.chat.message.v1.created",
			chat: { messagePayload: { space, message } },
		};
	}

	nativeEnvelope(extras: {
		message?: Record<string, unknown> | undefined;
		space?: Record<string, unknown> | undefined;
		eventType?: string | undefined;
	}): Record<string, unknown> {
		return {
			type: extras.eventType ?? "MESSAGE",
			space: extras.space ?? this.defaultSpace(),
			message: extras.message ?? this.chatMessage({}),
		};
	}

	relayEnvelope(extras: {
		eventType?: string | undefined;
		senderEmail?: string | undefined;
		senderType?: string | undefined;
		text?: string | undefined;
		messageName?: string | undefined;
		spaceName?: string | undefined;
		threadName?: string | undefined;
	}): Record<string, unknown> {
		const out: Record<string, unknown> = {
			event_type: extras.eventType ?? "MESSAGE",
			sender_email: extras.senderEmail ?? "human@example.com",
			text: extras.text ?? "hello via relay",
			space_name: extras.spaceName ?? "spaces/AAAA",
		};
		if (extras.senderType !== undefined) out["sender_type"] = extras.senderType;
		if (extras.messageName !== undefined)
			out["message_name"] = extras.messageName;
		if (extras.threadName !== undefined) out["thread_name"] = extras.threadName;
		return out;
	}

	chatMessage(
		extras: {
			name?: string | undefined;
			text?: string | undefined;
			senderType?: string | undefined;
			senderEmail?: string | undefined;
			threadName?: string | undefined;
			space?: Record<string, unknown> | undefined;
		} = {},
	): Record<string, unknown> {
		const msg: Record<string, unknown> = {
			name:
				extras.name ??
				`spaces/AAAA/messages/${Math.random().toString(36).slice(2)}`,
			sender: {
				name: "users/111",
				email: extras.senderEmail ?? "human@example.com",
				displayName: "Human User",
				type: extras.senderType ?? "HUMAN",
			},
			text: extras.text ?? "hello chat",
			argumentText: extras.text ?? "hello chat",
			space: extras.space ?? this.defaultSpace(),
		};
		if (extras.threadName !== undefined)
			msg["thread"] = { name: extras.threadName };
		return msg;
	}

	defaultSpace(): Record<string, unknown> {
		return {
			name: "spaces/AAAA",
			spaceType: "SPACE",
			displayName: "The Space",
		};
	}

	dmSpace(): Record<string, unknown> {
		return { name: "spaces/DM1", spaceType: "DIRECT_MESSAGE" };
	}
}

function toFixtureResponse(resp: {
	status: number;
	contentType?: "application/json" | "text/plain" | undefined;
	body?: string | Record<string, never> | undefined;
}): FixtureResponse {
	let json: Record<string, unknown> = {};
	if (
		resp.contentType === "application/json" &&
		resp.body !== null &&
		typeof resp.body === "object"
	) {
		json = resp.body as Record<string, unknown>;
	}
	return {
		status: resp.status,
		contentType: resp.contentType,
		text: typeof resp.body === "string" ? resp.body : "",
		json,
	};
}

export function makeGchatFixture(opts?: GchatFixtureOptions): GchatFixture {
	return new GchatFixture(opts);
}
