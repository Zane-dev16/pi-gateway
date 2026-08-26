// pi_platforms/sms/sms-rest-bridge — the FakeTwilioRestServer-style bridge:
// models the Twilio Messages.json edge ON TOP of the shared harness
// FakePlatformWire so every user-visible transmission lands in wire.ops (the
// wa-cloud WireBridge pattern), while scripted Twilio-shaped verdicts let the
// engine rows drive real HTTP-error mapping. Zero real network.

import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { PLAIN_TEXT_FALLBACK_PREFIX } from "../kit/send-retry.js";
import type { SmsRestResponse, SmsRestTransport } from "./sms-adapter.js";
import type { FakePlatformWire } from "../conformance/wire.js";

/** A scripted Twilio REST verdict consumed FIFO by postMessages. */
export interface ScriptedSmsResponse {
	status: number;
	json: Record<string, unknown>;
}

/** One observed Messages.json POST (URL/auth/From/To/Body + resolved verdict). */
export interface SmsPostRecord {
	/** Composed REST URL: {TWILIO_API_BASE}/{account_sid}/Messages.json. */
	url: string;
	/** Authorization header the adapter sent: Basic base64(sid:token). */
	authorization: string;
	from: string;
	to: string;
	body: string;
	status: number;
}

let sidCounter = 0;

/**
 * The REST edge the adapter's send lane POSTs against, bound to the harness
 * wire. Resolution order per POST:
 *   1. markdown-rendering rejection script (forceFormattingError) — EXACTLY
 *      like the reference fixtures: fails unless the body IS already the §6.1
 *      plain-text fallback body;
 *   2. a scripted Twilio verdict (engine rows program these via script());
 *   3. a scripted WIRE behavior — fail ⇒ HTTP 500 / timeout ⇒ HTTP 504 with
 *      the behavior's error text as `message`, retry fields PRESERVED so the
 *      §6.1 ladder classifies flood/timeout exactly;
 *   4. default: HTTP 201 carrying a fresh MessageSid.
 */
export class TwilioRestBridge implements SmsRestTransport {
	private responses: ScriptedSmsResponse[] = [];
	readonly posts: SmsPostRecord[] = [];

	constructor(private readonly raw: FakePlatformWire) {}

	/** Program the next N Twilio-shaped verdicts (consumed FIFO). */
	script(...responses: ScriptedSmsResponse[]): void {
		this.responses.push(...responses);
	}

	reset(): void {
		this.responses = [];
		this.posts.length = 0;
	}

	async postMessages(input: {
		url: string;
		authorization: string;
		from: string;
		to: string;
		body: string;
		metadata: Metadata;
	}): Promise<SmsRestResponse> {
		const md = input.metadata;
		if (
			md["forceFormattingError"] === true &&
			!input.body.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
		) {
			return {
				status: 400,
				json: { message: "Bad Request: can't parse entities" },
			};
		}
		const scripted = this.responses.shift();
		// The request hit the server regardless of the verdict — record it.
		const recorded = await this.raw.transmitSend(input.to, input.body, md);
		if (scripted !== undefined) {
			this.posts.push({
				url: input.url,
				authorization: input.authorization,
				from: input.from,
				to: input.to,
				body: input.body,
				status: scripted.status,
			});
			return {
				status: scripted.status,
				json: scripted.json,
				...(recorded.retryable !== undefined
					? { retryable: recorded.retryable }
					: {}),
				...(recorded.retryAfter != null
					? { retryAfter: recorded.retryAfter }
					: {}),
			};
		}
		this.posts.push({
			url: input.url,
			authorization: input.authorization,
			from: input.from,
			to: input.to,
			body: input.body,
			status: recorded.success ? 201 : errorStatusFor(recorded.error ?? ""),
		});
		if (!recorded.success) {
			const message = recorded.error ?? "send failed";
			return {
				status: errorStatusFor(message),
				json: { message },
				...(recorded.retryable !== undefined
					? { retryable: recorded.retryable }
					: {}),
				...(recorded.retryAfter != null
					? { retryAfter: recorded.retryAfter }
					: {}),
			};
		}
		sidCounter += 1;
		return {
			status: 201,
			json: {
				sid:
					typeof recorded.messageId === "string" && recorded.messageId
						? recorded.messageId
						: `SMfixture${String(sidCounter).padStart(10, "0")}`,
			},
		};
	}

	// ── rich-probe scripting seam (§10.1 latch rows) ──

	hasRichScript(opKind: string): boolean {
		return this.raw.hasScript(opKind as "rich");
	}

	async transmitRich(chatId: string, content: string): Promise<SendResult> {
		return this.raw.transmitRich(chatId, content, {});
	}
}

/** Wire-behavior → HTTP-status mapping for the capture seam. */
function errorStatusFor(message: string): number {
	return /timed out|timeout/i.test(message) ? 504 : 500;
}
