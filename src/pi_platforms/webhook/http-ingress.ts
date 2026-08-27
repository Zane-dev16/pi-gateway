// pi_platforms/webhook/http-ingress — THE request pipeline for stateless
// webhook ingress, framework-free over normalized requests so tests drive it
// without sockets (server.ts binds the same pipeline to node:http).
//
// Check ORDER ports webhook.py:_handle_webhook (@624) exactly:
//   unknown route 404 → profile mismatch SAME-404-shape (anti-enumeration) →
//   disabled route 403 → Content-Length cap 413 (auth-before-body) → body
//   read failures 400/413 → post-read byte-count cap 413 (defense in depth)
//   → missing secret 403 → invalid signature 401 → rate limit 429 (after
//   auth) → parse 400 → event-type filter 200 ignored → route filters 200
//   ignored → delivery-id extraction → idempotency (replay = cached outcome,
//   never double-process) → deliver_only sync send → agent mode raced against
//   the route's BOUNDED sync window (C5): completed ⇒ answer inline 200;
//   expiry ⇒ bounded ack 202 and the late completion lands via the
//   obligations ledger seam (held-open split).

import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import { buildSessionKey } from "../../pi_gateway/resolution/session-key.js";
import {
	validateSignature,
	type HeaderMap,
	type SignatureScheme,
} from "./signatures.js";
import type { SlidingWindowRateLimiter } from "./rate-limit.js";
import type { CachedOutcome, DeliveryIdempotencyStore } from "./idempotency.js";
import type { TrustBoundaryManifest } from "../kit/trust.js";
import type { WebhookRouteConfig } from "./manifest.js";

/** Normalized request (headers lowercased once by the server layer). */
export interface IngressRequest {
	method: string;
	path: string;
	headers: HeaderMap;
	/** Declared length (Content-Length); checked BEFORE any body read. */
	contentLength: number;
	/** Lazy body reader (the server enforces its own stream cap). */
	readBody: () => Promise<Buffer>;
}

export interface IngressResponse {
	status: number;
	json: Record<string, unknown>;
	/** Session key when an agent turn was dispatched (observability/tests). */
	dispatchedSessionKey?: string | undefined;
}

/** Injectable timer — production setTimeout; tests advance manually. */
export interface TimerSeam {
	delay(ms: number): { done: Promise<"fired">; cancel(): void };
}

export function createTimeoutSeam(): TimerSeam {
	return {
		delay(ms: number) {
			let cancel = () => {};
			const done = new Promise<"fired">((resolve) => {
				const t = setTimeout(() => resolve("fired"), ms);
				cancel = () => clearTimeout(t);
			});
			return { done, cancel: () => cancel() };
		},
	};
}

/**
 * Held-open obligation sink (DEC-022/C5 split): a turn finishing AFTER the
 * bounded window must not vanish — its reply is durably recorded for later
 * redelivery instead of being dropped with the closed HTTP connection.
 */
export interface HeldOpenSink {
	holdOpen(entry: {
		sessionKey: string;
		chatId: string;
		content: string;
		messageRef: string;
		route: string;
	}): Promise<{ obligationId: string }>;
}

export interface AgentDispatch {
	route: WebhookRouteConfig;
	profile?: string | undefined;
	deliveryId: string;
	eventType: string;
	sessionKey: string;
	chatId: string;
	event: IncomingEvent;
}

export interface IngressDeps {
	trust: TrustBoundaryManifest;
	routes: ReadonlyMap<string, WebhookRouteConfig>;
	profilesAllowed?: ReadonlySet<string> | undefined;
	rateLimiter: SlidingWindowRateLimiter;
	idempotency: DeliveryIdempotencyStore;
	/** Injected epoch-second clock for signatures/idempotency. */
	nowSeconds: () => number;
	timers: TimerSeam;
	/** Global fallback secret when the route carries none (webhook.py parity). */
	globalSecret?: string | undefined;
	/**
	 * Parse seam — injected so "oversized body rejected BEFORE parse" is
	 * observable (a parse attempt on a rejected body fails the contract).
	 */
	parseJson: (bodyText: string) => Record<string, unknown>;
	/** Run one agent turn; resolves with the reply text (null = no reply). */
	runAgentTurn: (dispatch: AgentDispatch) => Promise<string | null>;
	/** deliver_only lane: synchronous push; ok=false surfaces 502. */
	deliverOnly?:
		| ((prompt: string, dispatch: AgentDispatch) => Promise<boolean>)
		| undefined;
	heldOpenSink?: HeldOpenSink | undefined;
}

function jsonResponse(
	status: number,
	json: Record<string, unknown>,
): IngressResponse {
	return { status, json };
}

interface ParsedRoute {
	routeName: string;
	profile?: string | undefined;
}

/** "/webhooks/:name" | "/p/:profile/webhooks/:name" | null. */
export function parseWebhookPath(path: string): ParsedRoute | null {
	const p = path.length > 1 && path.endsWith("/") ? path.slice(0, -1) : path;
	const plain = /^\/webhooks\/([^/]+)$/.exec(p);
	if (plain) return { routeName: decodeURIComponent(plain[1] as string) };
	const profiled = /^\/p\/([^/]+)\/webhooks\/([^/]+)$/.exec(p);
	if (profiled) {
		return {
			profile: decodeURIComponent(profiled[1] as string),
			routeName: decodeURIComponent(profiled[2] as string),
		};
	}
	return null;
}

/** Delivery-id priority chain (webhook.py @~880). */
export function extractDeliveryId(headers: HeaderMap, nowMs: number): string {
	return (
		headers["x-github-delivery"] ??
		headers["svix-id"] ??
		headers["x-request-id"] ??
		String(nowMs)
	);
}

export class WebhookIngressPipeline {
	constructor(private readonly deps: IngressDeps) {}

	/** Returns null when the path is not a webhook route (caller tries other lanes). */
	async handle(req: IngressRequest): Promise<IngressResponse | null> {
		const parsed = parseWebhookPath(req.path);
		if (parsed === null || req.method !== "POST") return null;

		const routeConfig = this.deps.routes.get(parsed.routeName);

		// Multi-profile resolution — FAIL CLOSED on every un-served prefix
		// (webhook.py:_resolve_profile_prefix, #91583 defect 2): historically
		// ignoring a foreign prefix served the gateway owner's config under
		// another profile's URL. Only a prefix naming a SERVED profile may
		// proceed; when `profilesAllowed` is undefined this gateway declares no
		// multiplex surface, so any prefix rejects as unconfigured.
		if (parsed.profile !== undefined) {
			const allowed = this.deps.profilesAllowed?.has(parsed.profile) ?? false;
			if (!allowed) {
				return jsonResponse(404, {
					error: "Unknown or unconfigured profile",
				});
			}
		}
		if (routeConfig === undefined) {
			return jsonResponse(404, { error: `Unknown route: ${parsed.routeName}` });
		}
		if (
			parsed.profile !== undefined &&
			this.profileBindingAllows(routeConfig, parsed.profile) === false
		) {
			return jsonResponse(404, { error: `Unknown route: ${parsed.routeName}` });
		}

		// Disabled routes stay registered but reject (enabled:false semantics).
		if (routeConfig.enabled === false) {
			return jsonResponse(403, {
				error: `Route disabled: ${parsed.routeName}`,
			});
		}

		const bodyCap = this.deps.trust.bodySizeCapBytes;

		// Auth-before-body: declared size trips the cap WITHOUT reading.
		if (req.contentLength > bodyCap) {
			return jsonResponse(413, { error: "Payload too large" });
		}

		let rawBody: Buffer;
		try {
			rawBody = await req.readBody();
		} catch (err) {
			if ((err as { statusCode?: number })?.statusCode === 413) {
				return jsonResponse(413, { error: "Payload too large" });
			}
			return jsonResponse(400, { error: "Bad request" });
		}
		// Defense in depth: enforce the cap on the ACTUAL bytes read even if a
		// lying Content-Length slipped past the header check.
		if (rawBody.length > bodyCap) {
			return jsonResponse(413, { error: "Payload too large" });
		}

		// Missing secrets fail CLOSED here (not only at registration) so direct
		// handler reuse cannot open an unauthenticated dispatch surface.
		const secret = routeConfig.secret ?? this.deps.globalSecret;
		if (!secret) {
			return jsonResponse(403, {
				error: "Webhook route is missing an HMAC secret",
			});
		}

		const verdict = validateSignature({
			secret,
			headers: req.headers,
			rawBody: rawBody.toString("utf8"),
			nowSeconds: this.deps.nowSeconds(),
			...(routeConfig.signatureScheme !== undefined
				? { pinned: routeConfig.signatureScheme as SignatureScheme }
				: {}),
		});
		if (!verdict.ok) {
			return jsonResponse(401, { error: "Invalid signature" });
		}

		// Rate limiting runs AFTER auth (authenticated traffic only).
		if (!this.deps.rateLimiter.tryRecord(parsed.routeName)) {
			return jsonResponse(429, { error: "Rate limit exceeded" });
		}

		// Parse (JSON, then form-encoded fallback).
		let payload: Record<string, unknown>;
		try {
			payload = this.deps.parseJson(rawBody.toString("utf8"));
		} catch {
			return jsonResponse(400, { error: "Cannot parse body" });
		}

		const eventType =
			req.headers["x-github-event"] ??
			req.headers["x-gitlab-event"] ??
			strOf(payload["event_type"]) ??
			strOf(payload["type"]) ??
			"unknown";
		const allowedEvents = routeConfig.events ?? [];
		if (allowedEvents.length > 0 && !allowedEvents.includes(eventType)) {
			return jsonResponse(200, { status: "ignored", event: eventType });
		}

		// Render the prompt: `{payload}` interpolates pretty-printed JSON;
		// no template ⇒ the payload itself serialized (smallest Hermes-
		// consistent rendering; _render_prompt parity at reference scope).
		const template = routeConfig.promptTemplate ?? "";
		const prompt = template.includes("{payload}")
			? template.replace("{payload}", JSON.stringify(payload))
			: template.length > 0
				? template
				: JSON.stringify(payload);

		// Delivery id + idempotency (AFTER filters, Hermes order).
		const nowMsValue = this.deps.nowSeconds() * 1000;
		const deliveryId = extractDeliveryId(req.headers, nowMsValue);
		const claim = this.deps.idempotency.begin(deliveryId);
		if (claim.replay) {
			// NEVER double-process: replays get the recorded original outcome.
			if (claim.outcome !== null) {
				return jsonResponse(claim.outcome.status, claim.outcome.body);
			}
			return jsonResponse(200, {
				status: "duplicate",
				delivery_id: deliveryId,
			});
		}

		const source = {
			platform: "webhook",
			chatType: "dm",
			userId: `webhook:${parsed.routeName}`,
			chatId: `webhook:${parsed.routeName}:${deliveryId}`,
			chatName: `webhook/${parsed.routeName}`,
		};
		const dispatch: AgentDispatch = {
			route: routeConfig,
			deliveryId,
			eventType,
			sessionKey: buildSessionKey(source),
			chatId: source.chatId,
			event: {
				messageType: "text",
				text: prompt,
				messageId: deliveryId,
				source,
			},
			...(parsed.profile !== undefined ? { profile: parsed.profile } : {}),
		};

		// deliver_only: synchronous zero-LLM push (webhook.py parity).
		if (
			routeConfig.deliverOnly === true &&
			this.deps.deliverOnly !== undefined
		) {
			let delivered: boolean;
			try {
				delivered = await this.deps.deliverOnly(prompt, dispatch);
			} catch {
				delivered = false;
			}
			const body: Record<string, unknown> = delivered
				? {
						status: "delivered",
						route: parsed.routeName,
						delivery_id: deliveryId,
					}
				: {
						status: "error",
						error: "Delivery failed",
						delivery_id: deliveryId,
					};
			const status = delivered ? 200 : 502;
			this.deps.idempotency.recordOutcome(deliveryId, { status, body });
			return jsonResponse(status, body);
		}

		// Agent mode raced against the route's BOUNDED provider window (C5):
		// inside ⇒ the answer rides the response; past ⇒ bounded ack NOW and
		// the late reply lands via the held-open ledger seam.
		const windowCapMs = routeConfig.windowCapMs ?? DEFAULT_WINDOW_MS;
		const turnPromise = this.deps.runAgentTurn(dispatch);
		const timer = this.deps.timers.delay(windowCapMs);
		const raced = await Promise.race([
			turnPromise.then(
				(reply) => ({ kind: "completed" as const, reply }),
				() => ({ kind: "failed" as const, reply: null }),
			),
			timer.done.then(() => ({ kind: "expired" as const })),
		]);

		if (raced.kind === "completed") {
			timer.cancel();
			const body: Record<string, unknown> = {
				status: "completed",
				route: parsed.routeName,
				event: eventType,
				delivery_id: deliveryId,
			};
			if (raced.reply !== null && raced.reply !== undefined) {
				body["reply"] = raced.reply;
			}
			this.cacheOutcome(deliveryId, 200, body);
			return jsonResponse(200, body);
		}

		const body: Record<string, unknown> = {
			status: "accepted",
			route: parsed.routeName,
			event: eventType,
			delivery_id: deliveryId,
		};
		this.cacheOutcome(deliveryId, 202, body);

		if (raced.kind === "expired") {
			// Held-open half of the split: observe the turn to completion and
			// durably record whatever reply materializes — a closed HTTP window
			// must not orphan work (DEC-022 stateless close-out).
			void turnPromise
				.then((reply) => {
					if (reply === null || reply === undefined || reply === "") return;
					const sink = this.deps.heldOpenSink;
					if (sink === undefined) return;
					return sink.holdOpen({
						sessionKey: dispatch.sessionKey,
						chatId: dispatch.chatId,
						content: reply,
						messageRef: deliveryId,
						route: parsed.routeName,
					});
				})
				.catch(() => {});
		}
		return jsonResponse(202, body);
	}

	private cacheOutcome(
		id: string,
		status: number,
		body: Record<string, unknown>,
	): void {
		const cached: CachedOutcome = { status, body };
		this.deps.idempotency.recordOutcome(id, cached);
	}

	/** Routes may pin an allowed profile list (undefined/"*" admits all). */
	private profileBindingAllows(
		route: WebhookRouteConfig,
		profile: string,
	): boolean {
		const allowed = route.profiles;
		if (allowed === undefined || allowed.includes("*")) return true;
		return allowed.includes(profile);
	}
}

const DEFAULT_WINDOW_MS = 5_000;

function strOf(v: unknown): string | undefined {
	return typeof v === "string" && v.length > 0 ? v : undefined;
}
