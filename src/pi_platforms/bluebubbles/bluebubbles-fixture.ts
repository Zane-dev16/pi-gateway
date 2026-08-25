// pi_platforms/bluebubbles/bluebubbles-fixture — the REAL-engine fixture for
// the BlueBubbles shape-delta rows (MSGraphFixture pattern): the actual
// BlueBubblesAdapter driven against the in-process FakeBlueBubblesServer at
// its HTTP-handler seams (webhook POST surface + REST registration/typing
// lifecycle). NO stubbed return values and NO REAL NETWORK — every REST call
// lands on the fake server's captured-call arrays.
//
// The postWebhook(...) helper builds TOKENIZED requests (the registered URL
// embeds the password as ?password= because the BlueBubbles webhook API
// cannot send custom headers), and record builders construct BlueBubbles
// message payloads with key-PRESENCE semantics: an explicitly-undefined field
// is OMITTED from the JSON body, so rows can construct genuinely-missing
// chatGuid/handle payloads.

import type { AdapterStatusSnapshot } from '../kit/lifecycle-state.js';
import {
	BlueBubblesAdapter,
	settleBackgroundTasks,
	type BlueBubblesConfig,
} from './bluebubbles-adapter.js';
import { FakeBlueBubblesServer } from './fake-server.js';
import type { FakeBBChat } from './fake-server.js';
import {
	FIXTURE_BB_PASSWORD,
	FIXTURE_BB_SERVER_URL,
} from './fixture-secrets.js';

/**
 * Injected epoch-ms clock (flake discipline): starts at a fixed instant;
 * advance() moves it.
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

export interface BlueBubblesFixtureOptions {
	config?: BlueBubblesConfig | undefined;
	/** Resolve required secrets undefined (loud-disable probes). */
	withSecret?: boolean | undefined;
	privateApi?: boolean | undefined;
	helperConnected?: boolean | undefined;
	/** Chat roster served by /api/v1/chat/query (strict identifier matches). */
	chats?: readonly FakeBBChat[] | undefined;
}

/** Handler response surface the rows assert against. */
export interface FixtureResponse {
	status: number;
	contentType: string | undefined;
	text: string;
	json: Record<string, unknown>;
}

const DEFAULT_CONFIG: BlueBubblesConfig = {
	server_url: FIXTURE_BB_SERVER_URL,
	password: FIXTURE_BB_PASSWORD,
};

export class BlueBubblesFixture {
	readonly server: FakeBlueBubblesServer;
	readonly adapter: BlueBubblesAdapter;
	readonly clock = new FixtureClock();

	constructor(opts: BlueBubblesFixtureOptions = {}) {
		this.server = new FakeBlueBubblesServer({
			privateApi: opts.privateApi ?? true,
			helperConnected: opts.helperConnected ?? true,
			...(opts.chats !== undefined ? { chats: opts.chats } : {}),
		});
		const secretReader = (name: string): string | undefined => {
			if (opts.withSecret === false) return undefined;
			if (name === 'BLUEBUBBLES_SERVER_URL') return FIXTURE_BB_SERVER_URL;
			if (name === 'BLUEBUBBLES_PASSWORD') return FIXTURE_BB_PASSWORD;
			return undefined;
		};
		this.adapter = new BlueBubblesAdapter({
			config:
				opts.config === undefined
					? DEFAULT_CONFIG
					: { ...DEFAULT_CONFIG, ...opts.config },
			secretReader,
			restClient: this.server,
			nowMs: () => this.clock.nowMs,
		});
		this.adapter.attachStandardGuard();
	}

	async connect(): Promise<boolean> {
		return this.adapter.connect({ isReconnect: false });
	}

	dispose(): void {
		void this.settle();
	}

	/** Let fire-and-forget read receipts settle deterministically. */
	async settle(): Promise<void> {
		await settleBackgroundTasks(this.adapter);
		await new Promise<void>((r) => setTimeout(r, 0));
	}

	// ── transport-level webhook requests ─────────────────────────────────────

	/** Raw POST against the registered webhook path (caller owns the token). */
	postRaw(input: {
		query?: Record<string, string> | undefined;
		headers?: Record<string, string> | undefined;
		body: string | Buffer;
	}): Promise<FixtureResponse> {
		const raw = Buffer.isBuffer(input.body)
			? input.body
			: Buffer.from(input.body, 'utf8');
		return this.adapter
			.handleWebhookPost({
				query: input.query,
				headers: input.headers,
				rawBody: raw,
			})
			.then(toFixtureResponse);
	}

	/**
	 * Tokenized event POST: defaults to a valid ?password= query carrier and a
	 * JSON-encoded payload — the shape the registered webhook produces.
	 */
	postWebhook(
		payload: unknown,
		opts: {
			password?: string | undefined;
			carrier?:
				| 'query-password'
				| 'query-guid'
				| 'header-x-password'
				| 'header-x-guid'
				| 'header-x-bluebubbles-guid'
				| 'none'
				| undefined;
		} = {},
	): Promise<FixtureResponse> {
		return this.postRaw({
			query: tokenQuery(opts),
			headers: tokenHeaders(opts),
			body: JSON.stringify(payload),
		});
	}

	/** Form-encoded variant (urllib.parse_qs parse path). */
	postWebhookForm(
		payload: unknown,
		opts: { password?: string | undefined } = {},
	): Promise<FixtureResponse> {
		const params = new URLSearchParams();
		params.set('payload', JSON.stringify(payload));
		return this.postRaw({
			query: { password: opts.password ?? FIXTURE_BB_PASSWORD },
			headers: { 'content-type': 'application/x-www-form-urlencoded' },
			body: params.toString(),
		});
	}

	// ── BlueBubbles wire-shape builders (key-PRESENCE semantics) ─────────────

	/**
	 * A 'new-message' event envelope. `extras` merges into the DATA record;
	 * an explicitly-undefined extra key is OMITTED so genuinely-absent fields
	 * exercise the fallback chains.
	 */
	messageEvent(extras: Record<string, unknown> = {}): Record<string, unknown> {
		const data: Record<string, unknown> = {};
		const defaults: Record<string, unknown> = {
			guid: 'bb-msg-inbound',
			text: 'hello from iMessage',
			chatGuid: 'iMessage;-;user@example.com',
			handle: { address: 'user@example.com' },
			isFromMe: false,
		};
		for (const [k, v] of Object.entries(defaults)) {
			data[k] = k in extras ? extras[k] : v;
		}
		for (const [k, v] of Object.entries(extras)) {
			if (!(k in data) || v === undefined || k in extras) data[k] = v;
		}
		return { type: 'new-message', data };
	}

	/** Group-event convenience: group GUID + chats[0] nesting (v1.9+ shape). */
	groupEvent(extras: Record<string, unknown> = {}): Record<string, unknown> {
		return this.messageEvent({
			isGroup: true,
			chats: [{ guid: 'iMessage;+;chat0000000-family-group' }],
			chatGuid: undefined, // omitted ⇒ chats[0].guid fallback drives
			handle: { address: '+15555550100' },
			text: 'casual family chatter',
			guid: 'bb-group-msg',
			...extras,
		});
	}

	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.adapter.lifecycle.statusSnapshot();
	}
}

function tokenQuery(opts: {
	password?: string | undefined;
	carrier?:
		| 'query-password'
		| 'query-guid'
		| 'header-x-password'
		| 'header-x-guid'
		| 'header-x-bluebubbles-guid'
		| 'none'
		| undefined;
}): Record<string, string> | undefined {
	const pw = opts.password ?? FIXTURE_BB_PASSWORD;
	switch (opts.carrier ?? 'query-password') {
		case 'query-password':
			return { password: pw };
		case 'query-guid':
			return { guid: pw };
		default:
			return undefined;
	}
}

function tokenHeaders(opts: {
	password?: string | undefined;
	carrier?:
		| 'query-password'
		| 'query-guid'
		| 'header-x-password'
		| 'header-x-guid'
		| 'header-x-bluebubbles-guid'
		| 'none'
		| undefined;
}): Record<string, string> | undefined {
	const pw = opts.password ?? FIXTURE_BB_PASSWORD;
	switch (opts.carrier) {
		case 'header-x-password':
			return { 'x-password': pw };
		case 'header-x-guid':
			return { 'x-guid': pw };
		case 'header-x-bluebubbles-guid':
			return { 'x-bluebubbles-guid': pw };
		default:
			return undefined;
	}
}

function toFixtureResponse(resp: {
	status: number;
	contentType?: 'application/json' | 'text/plain' | undefined;
	body?: string | Record<string, never> | undefined;
}): FixtureResponse {
	let json: Record<string, unknown> = {};
	if (
		resp.contentType === 'application/json' &&
		resp.body !== null &&
		typeof resp.body === 'object'
	) {
		json = resp.body as Record<string, unknown>;
	}
	const text = typeof resp.body === 'string' ? resp.body : '';
	return {
		status: resp.status,
		contentType: resp.contentType,
		text,
		json,
	};
}

export function makeBlueBubblesFixture(
	opts?: BlueBubblesFixtureOptions,
): BlueBubblesFixture {
	return new BlueBubblesFixture(opts);
}
