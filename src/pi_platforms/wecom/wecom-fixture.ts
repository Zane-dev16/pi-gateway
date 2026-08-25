// pi_platforms/wecom/wecom-fixture — the REAL-engine fixture for the WeCom
// shape-delta rows (MSGraphFixture/WaCloudFixture pattern): the actual
// WecomCallbackAdapter driven at its HTTP-handler seams with an injected
// clock, a REAL BizMsgCrypt crypto stack (encrypt side builds valid vendor
// wire shapes; decrypt side verifies them), an instrumented proactive-send
// API, and the encrypted-XML envelope builders.
//
// NO REAL NETWORK: everything is in-process handler invocation. The crypto is
// the ported production module, not a stub — fixtures ENCRYPT with it so rows
// round-trip real envelopes (byte-exact xml recovery, receive_id binding).

import { WecomCallbackAdapter } from "./wecom-callback-adapter.js";
import type {
	WecomAdapterConfig,
	WecomApiResponse,
	WecomApiTransport,
	WecomAppConfig,
} from "./wecom-callback-adapter.js";
import type { HandlerResponse } from "./wecom-callback-adapter.js";
import { WxBizMsgCrypt } from "./wecom-crypto.js";
import {
	FIXTURE_CORP_ID_A,
	FIXTURE_CORP_ID_B,
	FIXTURE_WECOM_AES_KEY_A,
	FIXTURE_WECOM_AES_KEY_B,
	FIXTURE_WECOM_TOKEN_A,
	FIXTURE_WECOM_TOKEN_B,
} from "./fixture-secrets.js";

/**
 * Injected epoch-ms clock (flake discipline): starts at a fixed instant;
 * advance() moves it — the MsgId-TTL dedupe row mutates THIS.
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

export const DEFAULT_APP_A: WecomAppConfig = {
	name: "alpha",
	corp_id: FIXTURE_CORP_ID_A,
	corp_secret: "corp-secret-alpha",
	agent_id: 1000001,
	token: FIXTURE_WECOM_TOKEN_A,
	encoding_aes_key: FIXTURE_WECOM_AES_KEY_A,
};

export const DEFAULT_APP_B: WecomAppConfig = {
	name: "beta",
	corp_id: FIXTURE_CORP_ID_B,
	corp_secret: "corp-secret-beta",
	agent_id: 1000002,
	token: FIXTURE_WECOM_TOKEN_B,
	encoding_aes_key: FIXTURE_WECOM_AES_KEY_B,
};

export interface TokenFetch {
	corpId: string;
	corpSecret: string;
}
export interface SendCall {
	appName: string;
	payload: Record<string, unknown>;
}

type ScriptedSend =
	| { kind: "ok"; msgid?: string }
	| { kind: "errcode"; errcode: number };

/**
 * Instrumented qyapi.weixin.qq.com endpoint: token fetches + message/send
 * POSTs recorded with full payloads; errcode behaviors script FIFO.
 */
export class FakeWecomApi implements WecomApiTransport {
	readonly tokenFetches: TokenFetch[] = [];
	readonly sends: SendCall[] = [];
	private sendScripts: ScriptedSend[] = [];

	scriptSend(...behaviors: ScriptedSend[]): void {
		this.sendScripts.push(...behaviors);
	}

	private take(): ScriptedSend {
		if (this.sendScripts.length === 0)
			return { kind: "ok", msgid: `msg-${this.sends.length}` };
		return this.sendScripts.shift() as ScriptedSend;
	}

	async getAccessToken(app: WecomAppConfig): Promise<{
		token: string;
		expiresIn?: number | undefined;
	}> {
		this.tokenFetches.push({
			corpId: String(app.corp_id ?? ""),
			corpSecret: String(app.corp_secret ?? ""),
		});
		return { token: `token-${String(app.name ?? "")}`, expiresIn: 7200 };
	}

	async sendMessage(
		app: WecomAppConfig,
		_token: string,
		payload: Record<string, unknown>,
		metadata: Record<string, unknown> = {},
	): Promise<WecomApiResponse> {
		this.sends.push({ appName: String(app.name ?? ""), payload });
		// Markdown-rendering rejection script (reference-fixture parity): a
		// forced formatting error fails unless this IS the §6.1 plain-text body.
		// The script marker rides METADATA — never the vendor JSON body.
		if (
			metadata["forceFormattingError"] === true &&
			!String(
				(payload["text"] as Record<string, unknown> | undefined)?.["content"] ??
					"",
			).startsWith("(Response formatting failed, plain text:")
		) {
			return { success: false, errcode: 400, error: "can't parse entities" };
		}
		const behavior = this.take();
		if (behavior.kind === "errcode") {
			return {
				success: false,
				errcode: behavior.errcode,
				error: `wecom errcode ${behavior.errcode}`,
			};
		}
		return { success: true, messageId: behavior.msgid ?? "" };
	}

	sendsOfUser(touser: string): SendCall[] {
		return this.sends.filter(
			(s) => String(s.payload["touser"] ?? "") === touser,
		);
	}
}

export interface FixtureResponse {
	status: number;
	contentType: string | undefined;
	text: string;
}

export interface WecomFixtureOptions {
	config?: WecomAdapterConfig | undefined;
	/** Resolve required secrets undefined (loud-disable probes). */
	withSecret?: boolean | undefined;
	apps?: readonly WecomAppConfig[] | undefined;
	dedupCap?: number | undefined;
}

export class WecomFixture {
	readonly adapter: WecomCallbackAdapter;
	readonly api = new FakeWecomApi();
	readonly clock = new FixtureClock();
	readonly apps: readonly WecomAppConfig[];

	constructor(opts: WecomFixtureOptions = {}) {
		this.apps = opts.apps ??
			(opts.config?.apps as readonly WecomAppConfig[]) ?? [DEFAULT_APP_A];
		const config: WecomAdapterConfig = {
			...{ apps: this.apps },
			...(opts.config ?? {}),
			...(opts.config?.apps !== undefined ? {} : {}),
		};
		config.apps = opts.config?.apps ?? this.apps;
		this.adapter = new WecomCallbackAdapter({
			config,
			nowMs: () => this.clock.nowMs,
			transport: this.api,
			dedupCap: opts.dedupCap,
			secretReader: (name) => {
				if (opts.withSecret === false) return undefined;
				switch (name) {
					case "WECOM_CALLBACK_CORP_ID":
						return FIXTURE_CORP_ID_A;
					case "WECOM_CALLBACK_TOKEN":
						return FIXTURE_WECOM_TOKEN_A;
					case "WECOM_CALLBACK_ENCODING_AES_KEY":
						return FIXTURE_WECOM_AES_KEY_A;
					case "WECOM_CALLBACK_CORP_SECRET":
						return "corp-secret-alpha";
					default:
						return undefined;
				}
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

	cryptFor(app: WecomAppConfig): WxBizMsgCrypt {
		return new WxBizMsgCrypt(
			String(app.token ?? ""),
			String(app.encoding_aes_key ?? ""),
			String(app.corp_id ?? ""),
		);
	}

	// ── HTTP-level requests ──────────────────────────────────────────────────

	getVerify(query: Record<string, string>): FixtureResponse {
		return toFixtureResponse(this.adapter.handleVerify({ query }));
	}

	async postRaw(input: {
		query?: Record<string, string> | undefined;
		headers?: Record<string, string> | undefined;
		body: string | Buffer;
	}): Promise<FixtureResponse> {
		const raw = Buffer.isBuffer(input.body)
			? input.body
			: Buffer.from(input.body, "utf8");
		const resp = await this.adapter.handleCallbackPost({
			query: input.query ?? {},
			headers: input.headers,
			rawBody: raw,
		});
		return toFixtureResponse(resp);
	}

	// ── WeCom wire-shape builders (REAL encrypt path) ────────────────────────

	/**
	 * Build a VALID encrypted callback body for `app`: plaintext XML wrapped by
	 * the production-shape BizMsgCrypt envelope. `overrides.crypt` lets rows
	 * present envelopes encrypted under DIFFERENT credentials (negative matrix).
	 */
	buildEncryptedCallback(
		app: WecomAppConfig,
		xmlFields: Record<string, string | undefined>,
		opts: {
			crypt?: WxBizMsgCrypt | undefined;
			timestamp?: string | undefined;
			nonce?: string | undefined;
		} = {},
	): { body: string; query: Record<string, string> } {
		const inner = buildInnerXml(xmlFields);
		const crypt = opts.crypt ?? this.cryptFor(app);
		const timestamp =
			opts.timestamp ?? String(Math.floor(this.clock.nowMs / 1000));
		const nonce = opts.nonce ?? "nonce123";
		const envelope = crypt.encrypt(inner, nonce, timestamp);
		const msgSignature = extractSignature(envelope);
		return {
			body: envelope,
			query: { msg_signature: msgSignature, timestamp, nonce },
		};
	}

	postValidCallback(
		app: WecomAppConfig,
		xmlFields: Record<string, string | undefined>,
		opts: {
			timestamp?: string | undefined;
			nonce?: string | undefined;
		} = {},
	): Promise<FixtureResponse> {
		const built = this.buildEncryptedCallback(app, xmlFields, opts);
		return this.postRaw({ query: built.query, body: built.body });
	}
}

/** Inner plaintext XML (_build_event consumes exactly these fields). */
export function buildInnerXml(
	fields: Record<string, string | undefined>,
): string {
	const parts: string[] = [];
	for (const [tag, value] of Object.entries(fields)) {
		parts.push(`<${tag}>${value ?? ""}</${tag}>`);
	}
	return `<xml>${parts.join("")}</xml>`;
}

function extractSignature(envelopeXml: string): string {
	const match = /<MsgSignature>([^<]+)<\/MsgSignature>/.exec(envelopeXml);
	return match?.[1] ?? "";
}

function toFixtureResponse(resp: HandlerResponse): FixtureResponse {
	return {
		status: resp.status,
		contentType: resp.contentType,
		text: typeof resp.body === "string" ? resp.body : "",
	};
}

export function makeWecomFixture(opts?: WecomFixtureOptions): WecomFixture {
	return new WecomFixture(opts);
}
