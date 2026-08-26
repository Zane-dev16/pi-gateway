// pi_platforms/ntfy/ntfy-standalone — the OUT-OF-PROCESS publish sender,
// ported from the READ-ONLY Hermes reference plugin
// (plugins/platforms/ntfy/adapter.py:_standalone_send). Used by the cron /
// send_message_tool fallback lane when the gateway runner is NOT in this
// process — without it, deliver=ntfy cron jobs fail with "No live adapter".
//
// Hermes anchors (READ-ONLY; semantics ported, no code vendored):
//   _standalone_send      publish_topic chain chat_id → extra.publish_topic →
//                         NTFY_PUBLISH_TOPIC → extra.topic → NTFY_TOPIC;
//                         missing ⇒ "ntfy standalone send: NTFY_TOPIC not
//                         configured"; server = extra.server or
//                         NTFY_SERVER_URL (default DEFAULT_SERVER), rstrip("/");
//                         token = extra.token or scoped NTFY_TOKEN;
//                         markdown = bool(extra.markdown) or env ∈ {1,true,yes}
//   headers               Content-Type text/plain; charset=utf-8, X-Tags
//                         _ECHO_TAG ("hermes-agent"), auth via the SHARED
//                         _build_auth_header (strip → user:pass ⇒ Basic, else
//                         Bearer); X-Markdown: "true" when enabled
//   body                  _truncate_body(message, context="ntfy standalone") —
//                         4096-char cap, no chunking
//   POST                  httpx.AsyncClient(timeout=15.0) against
//                         {server}/{publish_topic}; ≥300 ⇒ "ntfy HTTP {s}:
//                         {text[:200]}"; JSON id (or uuid4().hex[:12]
//                         fallback on unparsable/absent id); success dict
//                         {"success", "platform", "chat_id", "message_id"};
//                         any exception ⇒ "ntfy standalone send failed: {e}"
//   thread_id / media_files / force_document — signature parity ONLY (ntfy
//   has no thread or attachment primitive); accepted and ignored here too.
//
// Python `or` fall-through is preserved on every config lane: a SET-BUT-EMPTY
// value defers to the next lane exactly like the reference.

import { randomBytes } from "node:crypto";

import type {
	AdapterDisabledError,
	PlatformFactory,
	PluginContext,
	ScopedSecretReader,
	StandaloneSenderFn,
} from "../kit/index.js";

import { buildAuthHeader } from "./ntfy-adapter.js";
import {
	NTFY_DEFAULT_SERVER,
	NTFY_ECHO_TAG,
	NTFY_MAX_MESSAGE_CHARS,
	NTFY_PLUGIN_MANIFEST,
	NTFY_PUBLISH_TIMEOUT_MS,
} from "./manifest.js";

/**
 * PlatformConfig.extra analog for the out-of-process lane (seeded env lanes:
 * server/publish_topic/topic/token/markdown keys verbatim).
 */
export interface NtfyStandaloneConfig {
	serverUrl?: string | undefined;
	publishTopic?: string | undefined;
	topic?: string | undefined;
	token?: string | undefined;
	markdown?: boolean | undefined;
}

/** Wire request of one standalone publish POST (transport seam input). */
export interface NtfyStandalonePostRequest {
	/** {server}/{publish_topic} — trailing-slash-stripped server. */
	url: string;
	/** Truncated text/plain body (_truncate_body parity). */
	body: string;
	headers: Record<string, string>;
	/** 15_000ms budget (httpx.AsyncClient(timeout=15.0) parity). */
	timeoutMs: number;
}

export interface NtfyStandalonePostResponse {
	status: number;
	/** Raw response text (error-text slicing parity). */
	text: string;
	/** Parsed JSON when the body parses; otherwise undefined. */
	json: unknown;
}

/**
 * Transport seam for the standalone publish. The default binds global fetch
 * with an AbortSignal timeout; tests script responses (and thrown transport
 * deaths) through this seam.
 */
export interface NtfyStandaloneTransport {
	post(req: NtfyStandalonePostRequest): Promise<NtfyStandalonePostResponse>;
}

function defaultTransport(): NtfyStandaloneTransport {
	// Destructured request fields keep the outbound call in the sanctioned
	// shape (wake.ts / simplex-adapter.ts precedent): fetch(<url ident>), with
	// the URL assembled by the SENDER from operator-owned env/config lanes.
	return {
		post: async ({ url, body, headers, timeoutMs }) => {
			const res = await fetch(url, {
				method: "POST",
				headers,
				body,
				signal: AbortSignal.timeout(timeoutMs),
			});
			const text = await res.text();
			let json: unknown;
			try {
				json = JSON.parse(text) as unknown;
			} catch {
				json = undefined;
			}
			return { status: res.status, text, json };
		},
	};
}

export interface NtfyStandaloneOptions {
	config?: NtfyStandaloneConfig | undefined;
	secretReader?: ScopedSecretReader | undefined;
	transport?: NtfyStandaloneTransport | undefined;
}

/** uuid.uuid4().hex[:12] fallback — data["id"] absent/unparsable/falsy. */
function fallbackMessageId(): string {
	return randomBytes(6).toString("hex");
}

/** data.get("id") truthiness ladder over a parsed JSON body. */
function messageIdFrom(json: unknown): string {
	if (typeof json === "object" && json !== null && !Array.isArray(json)) {
		const id = (json as Record<string, unknown>)["id"];
		if (typeof id === "string" && id.length > 0) return id;
		if (typeof id === "number" && Number.isFinite(id)) return String(id);
	}
	return fallbackMessageId();
}

const MARKDOWN_TRUE_VALUES = ["1", "true", "yes"];

/**
 * Build the standalone sender closing over its config lanes + secret reader +
 * transport (adapter.py:_standalone_send parity — see module header).
 */
export function makeNtfyStandaloneSender(
	options: NtfyStandaloneOptions = {},
): StandaloneSenderFn {
	const env: ScopedSecretReader =
		options.secretReader ?? ((name) => process.env[name]);
	const config = options.config ?? {};
	const transport = options.transport ?? defaultTransport();

	return async (args) => {
		try {
			const server = (
				config.serverUrl ||
				env("NTFY_SERVER_URL") ||
				NTFY_DEFAULT_SERVER
			).replace(/\/+$/u, "");
			const publishTopic =
				args.chatId ||
				config.publishTopic ||
				env("NTFY_PUBLISH_TOPIC")?.trim() ||
				config.topic ||
				env("NTFY_TOPIC")?.trim() ||
				"";
			if (publishTopic.length === 0) {
				return { error: "ntfy standalone send: NTFY_TOPIC not configured" };
			}

			const token = config.token || env("NTFY_TOKEN") || "";
			const markdownEnv = (env("NTFY_MARKDOWN") ?? "").trim().toLowerCase();
			const markdownEnabled =
				config.markdown === true || MARKDOWN_TRUE_VALUES.includes(markdownEnv);

			const headers: Record<string, string> = {
				"Content-Type": "text/plain; charset=utf-8",
				"X-Tags": NTFY_ECHO_TAG,
				...buildAuthHeader(token),
			};
			if (markdownEnabled) headers["X-Markdown"] = "true";

			// ntfy truncates at 4096 chars — NO chunking (splitsLongMessages=false).
			const body = args.message.slice(0, NTFY_MAX_MESSAGE_CHARS);

			const resp = await transport.post({
				url: `${server}/${publishTopic}`,
				body,
				headers,
				timeoutMs: NTFY_PUBLISH_TIMEOUT_MS,
			});
			if (resp.status >= 300) {
				return {
					error: `ntfy HTTP ${resp.status}: ${resp.text.slice(0, 200)}`,
				};
			}
			return {
				success: true,
				platform: "ntfy",
				chatId: publishTopic,
				messageId: messageIdFrom(resp.json),
			};
		} catch (err) {
			// Timeout deaths ride this same lane (httpx raises like any failure).
			return {
				error: `ntfy standalone send failed: ${
					err instanceof Error ? err.message : String(err)
				}`,
			};
		}
	};
}

/**
 * register(ctx) parity: registerPlatform(name="ntfy", …,
 * standalone_sender_fn=_standalone_send). Missing required secret still ⇒
 * LOUD disable at registration; the sender hook registers alongside.
 */
export function registerNtfyPlatform(
	ctx: PluginContext,
	factory: PlatformFactory,
	opts: { standalone?: NtfyStandaloneOptions | undefined } = {},
): AdapterDisabledError | null {
	return ctx.registerPlatform(NTFY_PLUGIN_MANIFEST, factory, {
		standaloneSenderFn: makeNtfyStandaloneSender(opts.standalone),
	});
}
