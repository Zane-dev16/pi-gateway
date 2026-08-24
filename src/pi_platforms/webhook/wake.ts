// pi_platforms/webhook/wake — the STATELESS wake lane (DEC-022 close-out):
// background completions re-enter a stateless adapter's session via a DIRECT
// turn under the RAW `X-Hermes-Session-Id` key, self-posted against this
// adapter's own /v1/chat/completions — the exact entry point real turns use.
//
// Ported from the READ-ONLY Hermes reference (gateway/wake.py):
//   adapter_supports_push        — lane choice reads supports_async_delivery
//   _self_post_chat_completion   — POST /v1/chat/completions with Bearer key
//                                  + RAW session id header; 429 → backoff
//                                  2s/5s/10s (4 attempts); other ≥400 fails
//                                  immediately; network errors retried;
//                                  exhaustion RAISES so callers rewind cursors
//   WAKE_TURN_TIMEOUT_SECONDS=600 ceiling; missing API key raises immediately
//   (continuation would be 403-gated — never run a fresh session nobody sees)

import { WAKE_RETRY_DELAYS_MS, WAKE_TURN_CEILING_MS } from "./manifest.js";
import type { HeaderMap } from "./signatures.js";

/** Lane selection (gateway/wake.py:adapter_supports_push parity). */
export function adapterSupportsPush(adapter: {
	supportsAsyncDelivery?: boolean | undefined;
}): boolean {
	return adapter.supportsAsyncDelivery !== false;
}

export type WakePostTransport = (
	url: string,
	init: { headers: Record<string, string>; body: string },
) => Promise<{ status: number; bodyText: string }>;

export interface WakeRailOptions {
	/** Own server base URL (loopback). */
	baseUrl: string;
	/** API_SERVER_KEY provider — undefined ⇒ the rail refuses loudly. */
	apiKeyProvider: () => string | undefined;
	/** HTTP transport seam (default fetch); tests script responses. */
	post?: WakePostTransport | undefined;
	/** Sleep seam (default setTimeout); tests resolve pending sleeps manually. */
	sleepMs?: ((ms: number) => Promise<void>) | undefined;
	retryDelaysMs?: readonly number[] | undefined;
	ceilingMs?: number | undefined;
	model?: string | undefined;
}

export type WakeOutcome =
	| { ok: true; status: number; reply: string }
	| { ok: false; transientExhausted: true; attempts: number }
	| { ok: false; permanentFailure: true; status: number; detail: string };

export class WakeRailMisconfiguredError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "WakeRailMisconfiguredError";
	}
}

/**
 * THE stateless rail. `wake()` either lands the completion in the REAL raw-key
 * session or RAISES loudly (`WakeRailError`) so callers rewind cursors and
 * retry later — the rail is at-least-once, never transactional, never silent.
 */
export class StatelessWakeRail {
	private readonly baseUrl: string;
	private readonly apiKeyProvider: () => string | undefined;
	private readonly post: WakePostTransport;
	private readonly sleepMs: (ms: number) => Promise<void>;
	private readonly retryDelaysMs: readonly number[];
	private readonly ceilingMs: number;
	private readonly model: string;

	constructor(opts: WakeRailOptions) {
		this.baseUrl = opts.baseUrl.replace(/\/$/, "");
		this.apiKeyProvider = opts.apiKeyProvider;
		this.post =
			opts.post ??
			(async (url, init) => {
				const res = await fetch(url, {
					method: "POST",
					headers: init.headers,
					body: init.body,
				});
				return { status: res.status, bodyText: await res.text() };
			});
		this.sleepMs = opts.sleepMs ?? defaultSleep;
		this.retryDelaysMs = opts.retryDelaysMs ?? WAKE_RETRY_DELAYS_MS;
		this.ceilingMs = opts.ceilingMs ?? WAKE_TURN_CEILING_MS;
		this.model = opts.model ?? "pi-gateway";
	}

	/**
	 * Deliver one background completion as a DIRECT turn under the RAW
	 * session id. Throws on misconfiguration and after exhausted retries.
	 */
	async wake(rawSessionId: string, userText: string): Promise<WakeOutcome> {
		if (rawSessionId.length === 0) {
			throw new WakeRailMisconfiguredError(
				"wake self-post requires the RAW X-Hermes-Session-Id — derived keys can never match a stateless chat",
			);
		}
		const apiKey = this.apiKeyProvider();
		if (apiKey === undefined || apiKey.length === 0) {
			// Continuation is 403-gated without credentials: raising NOW avoids
			// running a fresh session nobody is looking at.
			throw new WakeRailMisconfiguredError(
				"API_SERVER_KEY is not configured — stateless wake continuation would be 403-gated",
			);
		}
		const headers: Record<string, string> = {
			"content-type": "application/json",
			authorization: `Bearer ${apiKey}`,
			"x-hermes-session-id": rawSessionId,
		};
		const body = JSON.stringify({
			model: this.model,
			messages: [{ role: "user", content: userText }],
			stream: false,
		});
		const startedAtMs = Date.now();
		let attempts = 0;

		for (;;) {
			attempts += 1;
			let response: { status: number; bodyText: string };
			try {
				response = await this.post(`${this.baseUrl}/v1/chat/completions`, {
					headers,
					body,
				});
			} catch {
				// Network-classified — transient, retried on the ladder.
				response = { status: 0, bodyText: "" };
			}

			if (response.status >= 200 && response.status < 400) {
				return {
					ok: true,
					status: response.status,
					reply: extractReply(response.bodyText),
				};
			}
			if (response.status === 429 || response.status === 0) {
				if (attempts > this.retryDelaysMs.length) {
					// Exhaustion RAISES loudly — callers rewind cursors and retry
					// instead of marking delivered (at-least-once rail).
					throw new WakeRailError(
						`wake self-post gave up after ${attempts} attempts against ${this.baseUrl}`,
						true,
						attempts,
					);
				}
				const delay = this.retryDelaysMs[attempts - 1] ?? 10_000;
				if (Date.now() + delay - startedAtMs > this.ceilingMs) {
					throw new WakeRailError(
						"wake self-post exceeded the 600 s turn ceiling",
						true,
						attempts,
					);
				}
				await this.sleepMs(delay);
				continue;
			}
			// HTTP ≥400 non-transient: fail IMMEDIATELY with an excerpt.
			throw new WakeRailError(
				`wake self-post failed permanently: HTTP ${response.status}: ${response.bodyText.slice(0, 300)}`,
				false,
				attempts,
			);
		}
	}
}

export class WakeRailError extends Error {
	readonly transient: boolean;
	readonly attempts: number;
	constructor(message: string, transient: boolean, attempts: number) {
		super(message);
		this.name = "WakeRailError";
		this.transient = transient;
		this.attempts = attempts;
	}
}

function extractReply(bodyText: string): string {
	try {
		const parsed = JSON.parse(bodyText) as {
			choices?: Array<{ message?: { content?: string } }>;
		};
		return parsed.choices?.[0]?.message?.content ?? "";
	} catch {
		return "";
	}
}

function defaultSleep(ms: number): Promise<void> {
	return new Promise((resolve) => {
		const t = setTimeout(resolve, ms);
		t.unref?.();
	});
}

/** Header name constant shared with the completions lane. */
export const SESSION_ID_HEADER = "x-hermes-session-id";
export type { HeaderMap };
