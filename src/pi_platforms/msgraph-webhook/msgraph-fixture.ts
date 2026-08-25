// pi_platforms/msgraph-webhook/msgraph-fixture — the REAL-engine fixture for
// the msgraph-webhook shape-delta rows (WaCloudFixture pattern): the actual
// MSGraphWebhookAdapter driven at its HTTP-handler seams with an injected
// clock, simulated socket peers (CIDR admission), and Microsoft Graph wire
// shapes. NO stubbed return values — rows drive the real admission gates,
// handshake, dedupe set, resource filter, and verdict ladder.
//
// NO REAL NETWORK: everything is in-process handler invocation; peers are
// strings the CIDR gate evaluates exactly like aiohttp's request.remote.

import { MSGraphWebhookAdapter } from "./msgraph-webhook-adapter.js";
import type { MSGraphWebhookConfig } from "./msgraph-webhook-adapter.js";
import { FIXTURE_CLIENT_STATE } from "./fixture-secrets.js";

/**
 * Injected epoch-ms clock (flake discipline): starts at a fixed instant;
 * advance() moves it — the passive-subscription-boundary row mutates THIS.
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

export interface MSGraphFixtureOptions {
	config?: Partial<MSGraphWebhookConfig> | undefined;
	/** Resolve required secrets undefined (loud-disable probes). */
	withSecret?: boolean | undefined;
	clientState?: string | undefined;
}

/**
 * Microsoft Graph webhook source ranges as FIXTURE DATA (vendor ground truth,
 * Graph change-notification "IP addresses for change notifications" doc).
 */
export const GRAPH_SOURCE_CIDRS: readonly string[] = [
	"20.190.128.0/18",
	"40.104.0.0/15",
];

const DEFAULT_CONFIG: MSGraphWebhookConfig = {
	host: null, // dual-stack all-interfaces ⇒ CIDR allowlist REQUIRED
	port: 8646,
	webhook_path: "/msgraph/webhook",
	client_state: FIXTURE_CLIENT_STATE,
	allowed_source_cidrs: [...GRAPH_SOURCE_CIDRS],
};

/** Handler response surface the rows assert against. */
export interface FixtureResponse {
	status: number;
	contentType: string | undefined;
	text: string;
	json: Record<string, unknown>;
}

export class MSGraphFixture {
	readonly adapter: MSGraphWebhookAdapter;
	readonly clock = new FixtureClock();

	constructor(opts: MSGraphFixtureOptions = {}) {
		const { client_state: _stripped, ...baseConfig } = DEFAULT_CONFIG;
		// withSecret:false strips the secret from BOTH surfaces (config extra
		// AND scoped reader) — the truly-unconfigured posture.
		const config: MSGraphWebhookConfig =
			opts.withSecret === false
				? { ...baseConfig, ...(opts.config ?? {}) }
				: { ...DEFAULT_CONFIG, ...(opts.config ?? {}) };
		this.adapter = new MSGraphWebhookAdapter({
			config,
			nowMs: () => this.clock.nowMs,
			secretReader: (name) => {
				if (opts.withSecret === false) return undefined;
				if (name === "MSGRAPH_CLIENT_STATE") {
					return opts.clientState ?? FIXTURE_CLIENT_STATE;
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
		/* no filesystem state — pure in-process fixture */
	}

	// ── transport-level requests (peer-aware) ─────────────────────────────────

	getValidation(
		query: Record<string, string>,
		peer = "20.190.160.7",
	): FixtureResponse {
		const resp = this.adapter.handleValidationGet(query, peer);
		return toFixtureResponse(resp);
	}

	getHealth(peer = "20.190.160.7"): FixtureResponse {
		return toFixtureResponse(this.adapter.handleHealthGet(peer));
	}

	async postRaw(input: {
		headers?: Record<string, string> | undefined;
		query?: Record<string, string> | undefined;
		body: string | Buffer;
		peer?: string | undefined;
	}): Promise<FixtureResponse> {
		const raw = Buffer.isBuffer(input.body)
			? input.body
			: Buffer.from(input.body, "utf8");
		const resp = await this.adapter.handleNotificationPost({
			headers: input.headers,
			query: input.query,
			rawBody: raw,
			peer: input.peer ?? "20.190.160.7", // in-range Graph peer by default
		});
		return toFixtureResponse(resp);
	}

	// ── Microsoft Graph wire-shape builders ───────────────────────────────────

	changeNotification(extras: {
		id?: string | undefined;
		subscriptionId?: string | undefined;
		resource?: string | undefined;
		changeType?: string | undefined;
		clientState?: string | undefined;
		lifecycleEvent?: string | undefined;
		encryptedContent?: Record<string, unknown> | undefined;
		resourceData?: Record<string, unknown> | undefined;
	}): Record<string, unknown> {
		// Key-PRESENCE semantics: an explicitly-undefined field is OMITTED from
		// the wire shape (JSON.stringify drops undefined anyway) — only ABSENT
		// keys fall back to the fixture defaults. This lets rows construct
		// genuinely-missing clientState/changeType payloads.
		const out: Record<string, unknown> = {};
		const put = (key: string, fallback: unknown): void => {
			if (key in extras) out[key] = (extras as Record<string, unknown>)[key];
			else out[key] = fallback;
		};
		put("subscriptionId", "sub-9e17");
		put("changeType", "created");
		put("clientState", FIXTURE_CLIENT_STATE);
		if (extras.id !== undefined) out["id"] = extras.id;
		if (extras.resource !== undefined) out["resource"] = extras.resource;
		if (extras.lifecycleEvent !== undefined)
			out["lifecycleEvent"] = extras.lifecycleEvent;
		if (extras.encryptedContent !== undefined)
			out["encryptedContent"] = extras.encryptedContent;
		if (extras.resourceData !== undefined)
			out["resourceData"] = extras.resourceData;
		return out;
	}

	notificationEnvelope(value: unknown[]): Record<string, unknown> {
		return { value };
	}

	async postNotifications(
		value: unknown[],
		opts: { peer?: string } = {},
	): Promise<FixtureResponse> {
		return this.postRaw({
			body: JSON.stringify(this.notificationEnvelope(value)),
			headers: { "content-type": "application/json" },
			...(opts.peer !== undefined ? { peer: opts.peer } : {}),
		});
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
	const text = typeof resp.body === "string" ? resp.body : "";
	return {
		status: resp.status,
		contentType: resp.contentType,
		text,
		json,
	};
}

export function makeMSGraphFixture(
	opts?: MSGraphFixtureOptions,
): MSGraphFixture {
	return new MSGraphFixture(opts);
}
