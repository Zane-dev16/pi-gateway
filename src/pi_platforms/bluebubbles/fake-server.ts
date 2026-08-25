// pi_platforms/bluebubbles/fake-server — the in-process BlueBubbles REST
// server fixture (FakePlatformWire discipline: pure memory, NO sockets, NO
// real network). Implements the adapter's BlueBubblesRestClient seam against
// the endpoint paths the Hermes adapter actually drives:
//
//   GET  /api/v1/ping                    (connect ladder)
//   GET  /api/v1/server/info             (private_api / helper_connected)
//   GET  /api/v1/webhook                 (registration list)
//   POST /api/v1/webhook                 (registration create)
//   DEL  /api/v1/webhook/{id}            (registration delete)
//   POST /api/v1/chat/query              (chat-GUID resolution roster)
//   POST /api/v1/chat/new                (create-chat-for-handle)
//   POST /api/v1/message/text            (text sends — captured)
//   POST/DEL /api/v1/chat/{guid}/typing  (typing indicators — captured)
//   POST /api/v1/chat/{guid}/read        (read receipts — captured)
//
// Rows assert on the CAPTURED calls, so wire shapes stay observable without a
// transport. Non-2xx simulation throws like httpx raise_for_status.

import type { BlueBubblesRestClient } from './bluebubbles-adapter.js';

/** One chat entry as /api/v1/chat/query returns it (BlueBubbles wire shape). */
export interface FakeBBChat {
	guid?: string | undefined;
	chatGuid?: string | undefined;
	chatIdentifier?: string | undefined;
	identifier?: string | undefined;
	participants?: Array<{ address?: string }> | undefined;
}

export interface FakeBlueBubblesServerOptions {
	privateApi?: boolean | undefined;
	helperConnected?: boolean | undefined;
	/**
	 * The operator's chat roster returned by /api/v1/chat/query. Resolution is
	 * CLIENT-side strict chatIdentifier equality (the query carries only
	 * {limit, offset}), so subjects whose shared-row chats must resolve seed
	 * the roster with DM entries for those ids.
	 */
	chats?: readonly FakeBBChat[] | undefined;
	/** Simulated REST failure for /api/v1/message/text (raise_for_status parity). */
	messageTextError?: string | undefined;
}

interface WebhookEntry {
	id: number;
	url: string;
	events: string[];
}

interface CapturedCall {
	path: string;
	payload: Record<string, unknown>;
}

export class FakeBlueBubblesServer implements BlueBubblesRestClient {
	private webhookSeq = 1;
	private messageSeq = 1;
	private webhooks: WebhookEntry[] = [];
	readonly chats: FakeBBChat[];

	// ── captured traffic (row observability) ──
	readonly messageTextCalls: CapturedCall[] = [];
	readonly chatNewCalls: CapturedCall[] = [];
	readonly chatQueryCalls: CapturedCall[] = [];
	readonly registerWebhookCalls: CapturedCall[] = [];
	readonly deletedWebhookIds: string[] = [];
	readonly typingCalls: string[] = [];
	readonly stopTypingCalls: string[] = [];
	readonly readCalls: string[] = [];
	pingCount = 0;
	serverInfoCount = 0;

	private privateApi: boolean;
	private helperConnected: boolean;
	private messageTextError: string | undefined;

	constructor(opts: FakeBlueBubblesServerOptions = {}) {
		this.privateApi = opts.privateApi ?? true;
		this.helperConnected = opts.helperConnected ?? true;
		this.chats = [...(opts.chats ?? [])];
		this.messageTextError = opts.messageTextError;
	}

	setPrivateApi(on: boolean): void {
		this.privateApi = on;
	}
	setHelperConnected(on: boolean): void {
		this.helperConnected = on;
	}
	setMessageTextError(error: string | undefined): void {
		this.messageTextError = error;
	}

	seedChat(chat: FakeBBChat): void {
		this.chats.push(chat);
	}

	seedWebhook(entry: { url: string; events?: string[] }): number {
		const id = this.webhookSeq++;
		this.webhooks.push({
			id,
			url: entry.url,
			events: entry.events ?? ['new-message'],
		});
		return id;
	}

	/** Currently-registered webhook URLs (registration-lifecycle rows). */
	webhookUrls(): string[] {
		return this.webhooks.map((w) => w.url);
	}

	reset(): void {
		this.messageTextCalls.length = 0;
		this.chatNewCalls.length = 0;
		this.chatQueryCalls.length = 0;
		this.registerWebhookCalls.length = 0;
		this.deletedWebhookIds.length = 0;
		this.typingCalls.length = 0;
		this.stopTypingCalls.length = 0;
		this.readCalls.length = 0;
	}

	// ── BlueBubblesRestClient seam ─────────────────────────────────────────────

	async get(path: string): Promise<{ status: number; data?: unknown }> {
		const route = path.split('?')[0] ?? path;
		if (route === '/api/v1/ping') {
			this.pingCount += 1;
			return { status: 200 };
		}
		if (route === '/api/v1/server/info') {
			this.serverInfoCount += 1;
			return {
				status: 200,
				data: {
					private_api: this.privateApi,
					helper_connected: this.helperConnected,
				},
			};
		}
		if (route === '/api/v1/webhook') {
			return { status: 200, data: this.webhooks.map((w) => ({ ...w })) };
		}
		throw new Error(`fake-bluebubbles: unexpected GET ${path}`);
	}

	async post(
		path: string,
		payload: Record<string, unknown>,
	): Promise<{ status: number; data?: unknown }> {
		const route = path.split('?')[0] ?? path;
		if (route === '/api/v1/message/text') {
			this.messageTextCalls.push({ path, payload });
			if (this.messageTextError !== undefined) {
				throw new Error(this.messageTextError);
			}
			const guid = `bb-msg-${this.messageSeq++}`;
			return { status: 200, data: { guid } };
		}
		if (route === '/api/v1/chat/query') {
			this.chatQueryCalls.push({ path, payload });
			return { status: 200, data: this.chats.map((c) => ({ ...c })) };
		}
		if (route === '/api/v1/chat/new') {
			this.chatNewCalls.push({ path, payload });
			return { status: 200, data: { guid: `bb-new-${this.messageSeq++}` } };
		}
		if (route === '/api/v1/webhook') {
			this.registerWebhookCalls.push({ path, payload });
			const id = this.webhookSeq++;
			this.webhooks.push({
				id,
				url: String(payload['url'] ?? ''),
				events: Array.isArray(payload['events'])
					? (payload['events'] as string[]).map(String)
					: [],
			});
			return { status: 200, data: { id } };
		}
		// POST /api/v1/chat/{guid}/typing and /read
		const typingRead = /^\/api\/v1\/chat\/([^/]+)\/(typing|read)$/.exec(route);
		if (typingRead !== null) {
			const guid = decodeURIComponent(typingRead[1] ?? '');
			if (typingRead[2] === 'typing') this.typingCalls.push(guid);
			else this.readCalls.push(guid);
			return { status: 200 };
		}
		throw new Error(`fake-bluebubbles: unexpected POST ${path}`);
	}

	async del(path: string): Promise<{ status: number; data?: unknown }> {
		const route = path.split('?')[0] ?? path;
		const delWebhook = /^\/api\/v1\/webhook\/(.+)$/.exec(route);
		if (delWebhook !== null) {
			const id = decodeURIComponent(delWebhook[1] ?? '');
			this.deletedWebhookIds.push(id);
			this.webhooks = this.webhooks.filter((w) => String(w.id) !== id);
			return { status: 200 };
		}
		const stopTyping = /^\/api\/v1\/chat\/([^/]+)\/typing$/.exec(route);
		if (stopTyping !== null) {
			this.stopTypingCalls.push(decodeURIComponent(stopTyping[1] ?? ''));
			return { status: 200 };
		}
		throw new Error(`fake-bluebubbles: unexpected DELETE ${path}`);
	}
}
