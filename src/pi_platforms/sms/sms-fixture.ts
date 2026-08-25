// pi_platforms/sms/sms-fixture — the REAL-engine fixture for the sms
// shape-delta rows (MSGraphFixture pattern): the actual SmsAdapter driven at
// its HTTP-handler seams with an injected clock, real Twilio signature
// computations, and a scripted Messages.json REST edge (TwilioRestBridge over
// FakePlatformWire). NO stubbed return values — rows drive the real body-cap
// gates, parse seam, signature gate, field ladder, connect refusals, and REST
// error mapping.
//
// NO REAL NETWORK: webhook requests are in-process handler invocations;
// outbound SMS POSTs land on the in-memory bridge.

import type { StreamLogger } from "../../pi_gateway/streaming/adapter-seam.js";
import { FakePlatformWire } from "../conformance/wire.js";
import {
	SmsAdapter,
	signTwilioParams,
	type SmsAdapterConfig,
	type SmsHttpResponse,
} from "./sms-adapter.js";
import { TwilioRestBridge } from "./sms-rest-bridge.js";
import type { ScriptedSmsResponse } from "./sms-rest-bridge.js";
import {
	FIXTURE_ACCOUNT_SID,
	FIXTURE_AUTH_TOKEN,
	FIXTURE_FROM_NUMBER,
	FIXTURE_WEBHOOK_URL,
} from "./fixture-secrets.js";

/**
 * Injected epoch-ms clock (flake discipline): starts at a fixed instant;
 * advance() moves it. Local copy — adapter dirs never cross-import.
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

export interface CapturedLogLine {
	level: "debug" | "info" | "warn" | "error";
	message: string;
}

/** Injected logger capture — the warning/error ladders are OBSERVABLE data. */
export function captureLogger(): {
	logger: StreamLogger;
	lines: CapturedLogLine[];
} {
	const lines: CapturedLogLine[] = [];
	return {
		lines,
		logger: {
			debug: (message: string) => {
				lines.push({ level: "debug", message });
			},
			info: (message: string) => {
				lines.push({ level: "info", message });
			},
			warn: (message: string) => {
				lines.push({ level: "warn", message });
			},
			error: (message: string) => {
				lines.push({ level: "error", message });
			},
		},
	};
}

export interface SmsFixtureOptions {
	config?: Partial<SmsAdapterConfig> | undefined;
	/** Resolve required secrets undefined (loud-disable probes). */
	withSecret?: boolean | undefined;
}

const DEFAULT_CONFIG: SmsAdapterConfig = {
	phone_number: FIXTURE_FROM_NUMBER,
	webhook_url: FIXTURE_WEBHOOK_URL,
	port: 8080,
	host: "127.0.0.1",
};

/** Handler response surface the rows assert against. */
export interface FixtureResponse {
	status: number;
	contentType: string | undefined;
	text: string;
}

export class SmsFixture {
	readonly adapter: SmsAdapter;
	readonly clock = new FixtureClock();
	readonly rest: TwilioRestBridge;
	/** The bridge's own egress-capture wire (op recording + rich scripting). */
	readonly wire = new FakePlatformWire();
	readonly logLines: CapturedLogLine[];

	constructor(opts: SmsFixtureOptions = {}) {
		const captured = captureLogger();
		this.logLines = captured.lines;
		const config: SmsAdapterConfig = {
			...DEFAULT_CONFIG,
			...(opts.config ?? {}),
		};
		this.rest = new TwilioRestBridge(this.wire);
		this.adapter = new SmsAdapter({
			config,
			nowMs: () => this.clock.nowMs,
			logger: captured.logger,
			rest: this.rest,
			richProbe: this.rest,
			secretReader: (name) => {
				if (opts.withSecret === false) return undefined;
				switch (name) {
					case "TWILIO_ACCOUNT_SID":
						return FIXTURE_ACCOUNT_SID;
					case "TWILIO_AUTH_TOKEN":
						return FIXTURE_AUTH_TOKEN;
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
		/* no filesystem state — pure in-process fixture */
	}

	// ── webhook-plane requests ─────────────────────────────────────────────────

	async postWebhook(input: {
		headers?: Record<string, string> | undefined;
		body: string | Buffer;
	}): Promise<FixtureResponse> {
		const raw = Buffer.isBuffer(input.body)
			? input.body
			: Buffer.from(input.body, "utf8");
		const resp: SmsHttpResponse = await this.adapter.handleWebhookPost({
			headers: input.headers,
			rawBody: raw,
		});
		return {
			status: resp.status,
			contentType: resp.contentType,
			text: typeof resp.body === "string" ? resp.body : "",
		};
	}

	/**
	 * Build a Twilio-shaped form POST: urlencoded From/To/Body/MessageSid plus
	 * an X-Twilio-Signature computed over `url` (default: the CONFIGURED public
	 * webhook URL) with the sorted flat param concatenation — exactly what the
	 * adapter's verifier recomputes.
	 */
	signedForm(
		extras: {
			from?: string | undefined;
			to?: string | undefined;
			body?: string | undefined;
			sid?: string | undefined;
			url?: string | undefined;
			authToken?: string | undefined;
			extraParams?: Record<string, string> | undefined;
		} = {},
	): { headers: Record<string, string>; body: string } {
		const fields: Record<string, string> = {
			From: extras.from ?? "+15557654321",
			To: extras.to ?? FIXTURE_FROM_NUMBER,
			Body: extras.body ?? "hello from the fixture",
			MessageSid: extras.sid ?? "SMfixture0000000001",
			...(extras.extraParams ?? {}),
		};
		const signature = signTwilioParams(
			extras.authToken ?? this.adapter.authToken,
			extras.url ?? this.adapter.webhookUrl,
			fields,
		);
		return {
			headers: {
				"content-type": "application/x-www-form-urlencoded",
				"x-twilio-signature": signature,
			},
			body: formEncode(fields),
		};
	}

	/** POST a VALIDLY-SIGNED inbound SMS (the happy-path webhook request). */
	postSignedSms(
		extras: Parameters<SmsFixture["signedForm"]>[0] = {},
	): Promise<FixtureResponse> {
		const form = this.signedForm(extras);
		return this.postWebhook(form);
	}

	/** Await every pending non-blocking dispatch (determinism seam). */
	drainInbound(): Promise<void> {
		return this.adapter.drainInbound();
	}

	// ── outbound REST scripting (Messages.json verdicts) ────────────────────────

	scriptRest(...responses: ScriptedSmsResponse[]): void {
		this.rest.script(...responses);
	}
}

/**
 * application/x-www-form-urlencoded serializer (+ means space, literal +
 * percent-encoded — urllib.parse.urlencode parity).
 */
export function formEncode(fields: Record<string, string>): string {
	const enc = (value: string): string =>
		encodeURIComponent(value).replace(/\+/g, "%2B").replace(/%20/g, "+");
	return Object.entries(fields)
		.map(([k, v]) => `${enc(k)}=${enc(v)}`)
		.join("&");
}

export function makeSmsFixture(opts?: SmsFixtureOptions): SmsFixture {
	return new SmsFixture(opts);
}
