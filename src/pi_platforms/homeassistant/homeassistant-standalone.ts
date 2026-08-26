// pi_platforms/homeassistant/homeassistant-standalone — the OUT-OF-PROCESS
// notification sender, ported from the READ-ONLY Hermes plugin
// (plugins/platforms/homeassistant/adapter.py:_standalone_send). Used by the
// cron / send_message_tool fallback lane when the gateway runner is NOT in
// this process — without it, deliver=homeassistant cron jobs fail with
// "No live adapter".
//
// Hermes anchors (READ-ONLY; semantics ported, no code vendored):
//   _standalone_send      hass_url = extra.url or HASS_URL (NO default-URL
//                         fallback here — unset ⇒ error), rstrip("/");
//                         token = pconfig.token or scoped HASS_TOKEN, STRIPPED;
//                         either missing ⇒ "Home Assistant standalone send:
//                         HASS_URL and HASS_TOKEN must both be set"
//   POST                  aiohttp ClientTimeout(total=30) against
//                         {hass_url}/api/services/notify/notify with Bearer
//                         token + JSON payload {"message", "target": chat_id}
//                         — NO title key and NO message truncation on this
//                         path (unlike live send persistent_notification)
//   statuses              200/201 ⇒ success; anything else ⇒ "Home Assistant
//                         API error ({status}): {body}"; asyncio timeout ⇒
//                         "Timeout sending notification to Home Assistant";
//                         other exceptions ⇒ "Home Assistant send failed: {e}"
//   thread_id / media_files / force_document — signature parity ONLY (HA has
//   no native threading or attachment model); accepted and ignored here too.
//
// Python `or` fall-through is preserved on every config lane: a SET-BUT-EMPTY
// value defers to the next lane exactly like the reference.

import type {
	AdapterDisabledError,
	PlatformFactory,
	PluginContext,
	ScopedSecretReader,
	StandaloneSenderFn,
} from "../kit/index.js";

import {
	HA_PLUGIN_MANIFEST,
	HA_REST_NOTIFY_NOTIFY,
	HA_STANDALONE_TIMEOUT_MS,
} from "./manifest.js";

/** Transport outcome of one notify POST (mapped by the sender). */
export type HaStandaloneTransportOutcome =
	| { kind: "response"; status: number; body: string }
	| { kind: "timeout" }
	| { kind: "transport-failure"; error: string };

export interface HaStandaloneNotifyRequest {
	/** {hass_url}/api/services/notify/notify. */
	url: string;
	/** {"message": …, "target": chat_id} — verbatim wire shape. */
	payload: { message: string; target: string };
	headers: Record<string, string>;
	/** 30_000ms budget (aiohttp ClientTimeout(total=30) parity). */
	timeoutMs: number;
}

/**
 * Transport seam for the standalone notify POST. The default binds global
 * fetch with an AbortSignal timeout; tests script outcomes through it.
 */
export interface HaStandaloneTransport {
	postNotify(
		req: HaStandaloneNotifyRequest,
	): Promise<HaStandaloneTransportOutcome>;
}

function defaultTransport(): HaStandaloneTransport {
	// Destructured request fields keep the outbound call in the sanctioned
	// shape (wake.ts precedent): fetch(<url ident>), with the URL assembled
	// by the SENDER from operator-owned env/config lanes.
	return {
		postNotify: async ({ url, payload, headers, timeoutMs }) => {
			try {
				const res = await fetch(url, {
					method: "POST",
					headers,
					body: JSON.stringify(payload),
					signal: AbortSignal.timeout(timeoutMs),
				});
				return {
					kind: "response" as const,
					status: res.status,
					body: await res.text(),
				};
			} catch (err) {
				if (err instanceof Error && err.name === "TimeoutError") {
					return { kind: "timeout" as const };
				}
				return {
					kind: "transport-failure" as const,
					error: err instanceof Error ? err.message : String(err),
				};
			}
		},
	};
}

export interface HomeAssistantStandaloneOptions {
	/** PlatformConfig.extra/token analog (url + token lanes). */
	config?: { url?: string | undefined; token?: string | undefined } | undefined;
	secretReader?: ScopedSecretReader | undefined;
	transport?: HaStandaloneTransport | undefined;
}

/**
 * Build the standalone sender closing over its config lanes + secret reader +
 * transport (adapter.py:_standalone_send parity — see module header).
 */
export function makeHomeAssistantStandaloneSender(
	options: HomeAssistantStandaloneOptions = {},
): StandaloneSenderFn {
	const env: ScopedSecretReader =
		options.secretReader ?? ((name) => process.env[name]);
	const config = options.config ?? {};
	const transport = options.transport ?? defaultTransport();

	return async (args) => {
		const hassUrl = (config.url || env("HASS_URL") || "").replace(/\/+$/u, "");
		const token = (config.token || env("HASS_TOKEN") || "").trim();
		if (hassUrl.length === 0 || token.length === 0) {
			return {
				error:
					"Home Assistant standalone send: HASS_URL and HASS_TOKEN must both be set",
			};
		}

		const url = `${hassUrl}${HA_REST_NOTIFY_NOTIFY}`;
		const headers: Record<string, string> = {
			Authorization: `Bearer ${token}`,
			"Content-Type": "application/json",
		};
		const payload = { message: args.message, target: args.chatId };

		try {
			const outcome = await transport.postNotify({
				url,
				payload,
				headers,
				timeoutMs: HA_STANDALONE_TIMEOUT_MS,
			});
			switch (outcome.kind) {
				case "response":
					if (outcome.status === 200 || outcome.status === 201) {
						return {
							success: true,
							platform: "homeassistant",
							chatId: args.chatId,
						};
					}
					return {
						error: `Home Assistant API error (${outcome.status}): ${outcome.body}`,
					};
				case "timeout":
					return {
						error: "Timeout sending notification to Home Assistant",
					};
				case "transport-failure":
					return {
						error: `Home Assistant send failed: ${outcome.error}`,
					};
			}
		} catch (err) {
			return {
				error: `Home Assistant send failed: ${
					err instanceof Error ? err.message : String(err)
				}`,
			};
		}
	};
}

/**
 * register(ctx) parity: registerPlatform(name="homeassistant", …,
 * standalone_sender_fn=_standalone_send). Missing required secret still ⇒
 * LOUD disable at registration; the sender hook registers alongside.
 */
export function registerHomeAssistantPlatform(
	ctx: PluginContext,
	factory: PlatformFactory,
	opts: { standalone?: HomeAssistantStandaloneOptions | undefined } = {},
): AdapterDisabledError | null {
	return ctx.registerPlatform(HA_PLUGIN_MANIFEST, factory, {
		standaloneSenderFn: makeHomeAssistantStandaloneSender(opts.standalone),
	});
}
