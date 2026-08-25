// pi_platforms/teams/bot-framework-wire — the FAKE Bot Framework server
// (family pattern: telegram-fake-server / FakeGraphServer / fake-socket-mode)
// plus the transport seam the adapter depends on.
//
// Vendor wire shapes modeled (Bot Framework REST ground truth, as exercised by
// the Hermes standalone sender _standalone_send):
//   POST https://login.microsoftonline.com/{tenant}/oauth2/v2.0/token
//        form-encoded grant_type=client_credentials&client_id&client_secret&
//        scope=https://api.botframework.com/.default → {access_token}
//   POST {service_url}v3/conversations/{conversationId}/activities
//        Authorization: Bearer <token>; body {"type":"message","text":…,
//        "textFormat":"markdown"} → {id}
//   (threaded replies target …/activities/{replyToActivityId} — the SDK's
//   app.reply(); group chats 400 on threaded sends per the Hermes fallback
//   comment, scriptable here.)
//
// NO REAL NETWORK: in-process capture + FIFO behavior scripts. The inbound
// activity POST boundary itself lives on the adapter (handleActivityPost) —
// the SDK-side Bearer validation of INBOUND requests is daemon-delegated and
// marked as a probe-computed exclusion (teams-adapter.ts header note).

import type { SendResult } from "../../pi_gateway/streaming/adapter-seam.js";

export type BotFrameworkJson = Record<string, unknown>;

export interface TokenEndpointResponse {
	status: number;
	json: BotFrameworkJson;
}

export interface ActivityPostResponse {
	status: number;
	json: BotFrameworkJson;
}

export interface BotFrameworkTokenRequest {
	tenantId: string;
	clientId: string;
	clientSecret: string;
	scope: string;
}

export interface RecordedActivity {
	conversationId: string;
	activity: BotFrameworkJson;
	kind: "send" | "reply";
	bearer: string;
	seq: number;
}

export interface BotFrameworkTransport {
	getAccessToken(req: BotFrameworkTokenRequest): Promise<TokenEndpointResponse>;
	postActivity(
		conversationId: string,
		activity: BotFrameworkJson,
		bearer: string,
		metadata?: Record<string, unknown> | undefined,
	): Promise<ActivityPostResponse>;
	postReply(
		conversationId: string,
		replyToActivityId: string,
		activity: BotFrameworkJson,
		bearer: string,
		metadata?: Record<string, unknown> | undefined,
	): Promise<ActivityPostResponse>;
	sendTypingActivity(conversationId: string): Promise<void>;
	fetchAttachmentBytes(url: string): Promise<{ status: number; bytes: Buffer }>;
}

/**
 * In-process Bot Framework fake. Records every request; scripted failures are
 * consumed FIFO (an exhausted script defaults to success).
 */
export class FakeBotFrameworkServer implements BotFrameworkTransport {
	readonly tokenRequests: BotFrameworkTokenRequest[] = [];
	readonly activities: RecordedActivity[] = [];
	readonly typingActivities: string[] = [];
	readonly attachmentFetches: string[] = [];

	private tokenScripts: TokenEndpointResponse[] = [];
	private activityFailScripts: ActivityPostResponse[] = [];
	private replyFailScripts: ActivityPostResponse[] = [];
	private seqCounter = 0;

	/** Program token-endpoint responses (FIFO; default 200 + access_token). */
	scriptToken(...responses: TokenEndpointResponse[]): void {
		this.tokenScripts.push(...responses);
	}

	/** Script flat-send failures (e.g. throttling 429). */
	scriptActivityFail(...responses: ActivityPostResponse[]): void {
		this.activityFailScripts.push(...responses);
	}

	/** Script threaded-reply failures — the group-chat 400 shape. */
	scriptReplyFail(...responses: ActivityPostResponse[]): void {
		this.replyFailScripts.push(...responses);
	}

	async getAccessToken(
		req: BotFrameworkTokenRequest,
	): Promise<TokenEndpointResponse> {
		this.tokenRequests.push(req);
		const scripted = this.tokenScripts.shift();
		if (scripted !== undefined) return scripted;
		return {
			status: 200,
			json: { access_token: `fake-bf-token.${this.tokenRequests.length}` },
		};
	}

	async postActivity(
		conversationId: string,
		activity: BotFrameworkJson,
		bearer: string,
	): Promise<ActivityPostResponse> {
		this.seqCounter += 1;
		this.activities.push({
			conversationId,
			activity,
			kind: "send",
			bearer,
			seq: this.seqCounter,
		});
		const fail = this.activityFailScripts.shift();
		if (fail !== undefined) return fail;
		return { status: 200, json: { id: `bf-${this.seqCounter}` } };
	}

	async postReply(
		conversationId: string,
		replyToActivityId: string,
		activity: BotFrameworkJson,
		bearer: string,
	): Promise<ActivityPostResponse> {
		this.seqCounter += 1;
		this.activities.push({
			conversationId,
			activity: { ...activity, replyToId: replyToActivityId },
			kind: "reply",
			bearer,
			seq: this.seqCounter,
		});
		const fail = this.replyFailScripts.shift();
		if (fail !== undefined) return fail;
		return { status: 200, json: { id: `bf-${this.seqCounter}` } };
	}

	async sendTypingActivity(conversationId: string): Promise<void> {
		this.typingActivities.push(conversationId);
	}

	async fetchAttachmentBytes(
		url: string,
	): Promise<{ status: number; bytes: Buffer }> {
		this.attachmentFetches.push(url);
		return { status: 200, bytes: Buffer.from(`bytes-of(${url})`) };
	}

	// ── observation helpers ──

	textSendsOf(conversationId?: string): RecordedActivity[] {
		return this.activities.filter(
			(a) =>
				a.kind === "send" &&
				(conversationId === undefined || a.conversationId === conversationId),
		);
	}

	textOf(activity: RecordedActivity): string {
		return String((activity.activity["text"] as string | undefined) ?? "");
	}
}

/**
 * Conformance-subject bridge: models the SAME transport on top of the shared
 * FakePlatformWire so every user-visible transmission lands in wire.ops.
 * Typing/attachment lanes are UX polish, not user-visible transmissions, and
 * refuse loudly (engine tests bind FakeBotFrameworkServer directly).
 */
export class WireBridgeTransport implements BotFrameworkTransport {
	constructor(
		private readonly raw: import("../conformance/wire.js").FakePlatformWire,
	) {}

	async getAccessToken(): Promise<TokenEndpointResponse> {
		// In-memory harness: no STS exists, so the token dance answers canned
		// success (shared egress rows exercise sends end-to-end).
		return { status: 200, json: { access_token: "subject-bridge-token" } };
	}
	async postActivity(
		conversationId: string,
		activity: BotFrameworkJson,
		_bearer: string,
		metadata: Record<string, unknown> = {},
	): Promise<ActivityPostResponse> {
		const result = await this.raw.transmitSend(
			conversationId,
			String(activity["text"] ?? ""),
			metadata as never,
		);
		return this.toResponse(result);
	}

	async postReply(
		conversationId: string,
		_replyToActivityId: string,
		activity: BotFrameworkJson,
		_bearer: string,
		metadata: Record<string, unknown> = {},
	): Promise<ActivityPostResponse> {
		const result = await this.raw.transmitSend(
			conversationId,
			String(activity["text"] ?? ""),
			metadata as never,
		);
		return this.toResponse(result);
	}

	async sendTypingActivity(): Promise<void> {
		throw new Error("typing requires the FakeBotFrameworkServer fixture");
	}

	async fetchAttachmentBytes(): Promise<{ status: number; bytes: Buffer }> {
		throw new Error("attachments require the FakeBotFrameworkServer fixture");
	}

	hasScript(opKind: string): boolean {
		return this.raw.hasScript(opKind as "send");
	}

	async transmitRichProbe(
		chatId: string,
		content: string,
	): Promise<ActivityPostResponse> {
		const result = await this.raw.transmitRich(chatId, content, {});
		return this.toResponse(result);
	}

	private toResponse(result: SendResult): ActivityPostResponse {
		return result.success
			? { status: 200, json: { id: result.messageId ?? "bridge" } }
			: {
					status: 400,
					json: { error: { message: result.error ?? "send failed" } },
				};
	}
}
