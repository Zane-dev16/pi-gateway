// pi_platforms/weixin/fake-ilink — the IN-PROCESS fake WeChat iLink server
// (04 §8: headless fake platform; NO real network).
//
// Faces:
//   • getupdates LONG POLL — messages queue server-side; the sync_buf cursor
//     advances ONLY on a successful pull (queue-preservation semantics the
//     outage row proves). Scriptable error codes (-14 session expired,
//     -2 rate limit) and long-poll HOLDS (no messages → wait until release
//     or injected timeout).
//   • ONE POST chokepoint (`post`) mirroring weixin.py:_api_post: EVERY
//     outgoing iLink POST (sendmessage / getconfig / sendtyping /
//     getuploadurl / getupdates) is recorded VERBATIM — endpoint, merged
//     payload (base_info included) and the full header plane — into `postLog`,
//     so tests can assert the Hermes request shape on every call.
//   • get_bot_qrcode / get_qrcode_status GET faces (weixin.py:qr_login) with
//     scriptable status responses and per-request baseUrl observability (the
//     scaned_but_redirect repoint is observable).
//   • CDN ciphertext upload face (weixin.py:_upload_ciphertext): POST
//     application/octet-stream → x-encrypted-param response header.
//
// Vendor error codes are matched NUMERICALLY by the adapter (Hermes parity:
// `ret == SESSION_EXPIRED_ERRCODE`), never via snapshotted strings.

import {
	EP_GET_BOT_QR,
	EP_GET_CONFIG,
	EP_GET_QR_STATUS,
	EP_GET_UPDATES,
	EP_GET_UPLOAD_URL,
	EP_SEND_MESSAGE,
	EP_SEND_TYPING,
} from "./manifest.js";

export interface ILinkMessage {
	from_user_id?: string | undefined;
	to_user_id?: string | undefined;
	room_id?: string | undefined;
	chat_room_id?: string | undefined;
	message_id?: string | undefined;
	context_token?: string | undefined;
	msg_type?: number | undefined;
	item_list?: Array<Record<string, unknown>> | undefined;
}

export type GetUpdatesBehavior =
	| { kind: "ok" }
	| {
			kind: "code";
			ret?: number | undefined;
			errcode?: number | undefined;
			errmsg?: string | undefined;
	  };

export interface SendCallRecord {
	to_user_id: string;
	text: string;
	context_token?: string | undefined;
	client_id: string;
	ret: number | null;
	errcode: number | null;
	seq: number;
}

export interface GetConfigCallRecord {
	ilink_user_id: string;
	context_token?: string | undefined;
	typing_ticket: string;
	seq: number;
}

/** One outgoing iLink POST as handed to the transport seam (VERBATIM). */
export interface ILinkPostRequest {
	endpoint: string;
	payload: Record<string, unknown>;
	headers: Record<string, string>;
}

/** postLog entry: request shape + numeric outcome (row observability). */
export interface ILinkPostRecord extends ILinkPostRequest {
	seq: number;
	ret: number | null;
	errcode: number | null;
	/** Merged channel-version envelope (cn-3) surfaced for row assertions. */
	base_info: unknown;
}

export interface ILinkPostResponse {
	ret: number;
	errcode: number;
	[key: string]: unknown;
}

export interface SendTypingCallRecord {
	ilink_user_id: string;
	typing_ticket: string;
	status: number;
	base_info: unknown;
	headers: Record<string, string>;
	ret: number;
	seq: number;
}

export interface GetUploadUrlCallRecord {
	filekey: string;
	media_type: number;
	to_user_id: string;
	rawsize: number;
	rawfilemd5: string;
	filesize: number;
	no_need_thumb: boolean;
	aeskey: string;
	base_info: unknown;
	headers: Record<string, string>;
	ret: number | null;
	errcode: number | null;
	/** What this face answered (upload_param / direct full URL). */
	response: { upload_param: string; upload_full_url: string };
	seq: number;
}

export interface CdnUploadCallRecord {
	url: string;
	ciphertextSize: number;
	ciphertext: Buffer;
	contentType: string;
	status: number;
	encryptedParam: string | null;
	seq: number;
}

export interface QrCodeRequestRecord {
	bot_type: string;
	baseUrl: string;
	headers: Record<string, string>;
	seq: number;
}

export interface QrStatusRequestRecord {
	qrcode: string;
	baseUrl: string;
	headers: Record<string, string>;
	seq: number;
}

/**
 * The fake platform. Long-poll holds are resolved by releaseUpdates() or by
 * the adapter's own timeout budget under the INJECTED clock.
 */
export class FakeILinkServer {
	/** Queued messages awaiting a pull (server-side queue). */
	private readonly queue: ILinkMessage[] = [];
	private bufCounter = 0;
	private seqCounter = 0;

	private getUpdatesScripts: GetUpdatesBehavior[] = [];
	private sendMessageScripts: Array<{
		ret?: number | undefined;
		errcode?: number | undefined;
		errmsg?: string | undefined;
	}> = [];
	private typingScripts: Array<{ ret?: number | undefined }> = [];
	private uploadUrlScripts: Array<{
		ret?: number | undefined;
		errcode?: number | undefined;
		upload_param?: string | undefined;
		upload_full_url?: string | undefined;
	}> = [];
	private cdnScripts: Array<{
		status?: number | undefined;
		encryptedParam?: string | null | undefined;
	}> = [];
	private qrCodeScripts: Array<Record<string, unknown>> = [];
	private qrStatusScripts: Array<Record<string, unknown>> = [];

	longPollHoldEnabled = false;
	private holdWaiters: Array<() => void> = [];

	/**
	 * Server-suggested long-poll hold budget (weixin.py response field
	 * longpolling_timeout_ms). null omits the field entirely; tests set it to
	 * prove the adapter ADOPTS the server budget over its local default.
	 */
	longPollingTimeoutMsOverride: number | null = null;

	readonly sendCalls: SendCallRecord[] = [];
	readonly getConfigCalls: GetConfigCallRecord[] = [];
	readonly sendTypingCalls: SendTypingCallRecord[] = [];
	readonly getUploadUrlCalls: GetUploadUrlCallRecord[] = [];
	readonly cdnUploadCalls: CdnUploadCallRecord[] = [];
	readonly qrCodeRequests: QrCodeRequestRecord[] = [];
	readonly qrStatusRequests: QrStatusRequestRecord[] = [];
	/** EVERY outgoing iLink POST — endpoint + payload + headers, verbatim. */
	readonly postLog: ILinkPostRecord[] = [];
	/** Every getupdates pull outcome, in order (row observability). */
	readonly pullLog: Array<{
		msgCount: number;
		ret: number | null;
		errcode: number | null;
		buf: string;
	}> = [];

	typingTicket = "ticket-1";

	/** Sync-buf cursors issued per successful pull. */
	lastBuf = "";

	get queuedCount(): number {
		return this.queue.length;
	}

	// ── scenario controls ─────────────────────────────────────────────────

	scriptGetUpdates(...behaviors: GetUpdatesBehavior[]): void {
		this.getUpdatesScripts.push(...behaviors);
	}

	scriptSendMessage(
		ret: number,
		errcode?: number | undefined,
		errmsg?: string | undefined,
	): void {
		this.sendMessageScripts.push({
			ret,
			...(errcode !== undefined ? { errcode } : {}),
			...(errmsg !== undefined ? { errmsg } : {}),
		});
	}

	scriptSendTyping(ret: number): void {
		this.typingScripts.push({ ret });
	}

	scriptGetUploadUrl(script: {
		ret?: number | undefined;
		errcode?: number | undefined;
		upload_param?: string | undefined;
		upload_full_url?: string | undefined;
	}): void {
		this.uploadUrlScripts.push(script);
	}

	/** Script the CDN leg: non-200 status or a MISSING x-encrypted-param. */
	scriptCdnUpload(script: {
		status?: number | undefined;
		encryptedParam?: string | null | undefined;
	}): void {
		this.cdnScripts.push(script);
	}

	scriptQrCodeResponse(...responses: Array<Record<string, unknown>>): void {
		this.qrCodeScripts.push(...responses);
	}

	scriptQrStatusResponse(...responses: Array<Record<string, unknown>>): void {
		this.qrStatusScripts.push(...responses);
	}

	pushMessage(msg: ILinkMessage): void {
		this.queue.push(msg);
		// A newly-arrived message ALWAYS wakes a pending long poll.
		for (const w of this.holdWaiters.splice(0)) w();
	}

	holdUpdates(): void {
		this.longPollHoldEnabled = true;
	}

	releaseUpdates(): void {
		this.longPollHoldEnabled = false;
		for (const w of this.holdWaiters.splice(0)) w();
	}

	clearQueue(): void {
		this.queue.length = 0;
	}

	// ── endpoint faces ─────────────────────────────────────────────────────

	/**
	 * THE _api_post parity chokepoint. Routes by endpoint, scripts errors per
	 * face, records the request (endpoint + merged payload + headers) and the
	 * numeric outcome into postLog.
	 */
	post(request: ILinkPostRequest): ILinkPostResponse {
		const seq = ++this.seqCounter;
		let ret = 0;
		let errcode = 0;
		const extra: Record<string, unknown> = {};

		if (request.endpoint === EP_SEND_MESSAGE) {
			const scripted =
				this.sendMessageScripts.length > 0
					? this.sendMessageScripts.shift()
					: undefined;
			ret = scripted?.ret ?? 0;
			errcode = scripted?.errcode ?? 0;
			if (scripted?.errmsg !== undefined) extra.errmsg = scripted.errmsg;
			this.recordSendCall(seq, request.payload["msg"], ret, errcode);
		} else if (request.endpoint === EP_GET_CONFIG) {
			extra.typing_ticket = this.typingTicket;
			this.recordConfigCall(seq, request.payload);
		} else if (request.endpoint === EP_SEND_TYPING) {
			const scripted =
				this.typingScripts.length > 0 ? this.typingScripts.shift() : undefined;
			ret = scripted?.ret ?? 0;
			this.sendTypingCalls.push({
				ilink_user_id: String(request.payload["ilink_user_id"] ?? ""),
				typing_ticket: String(request.payload["typing_ticket"] ?? ""),
				status: Number(request.payload["status"] ?? 0),
				base_info: request.payload["base_info"],
				headers: request.headers,
				ret,
				seq,
			});
		} else if (request.endpoint === EP_GET_UPLOAD_URL) {
			const script =
				this.uploadUrlScripts.length > 0
					? this.uploadUrlScripts.shift()
					: undefined;
			ret = script?.ret ?? 0;
			errcode = script?.errcode ?? 0;
			const upload_param = script?.upload_param ?? `up-${seq}`;
			const upload_full_url = script?.upload_full_url ?? "";
			if (ret === 0 && errcode === 0) {
				extra.upload_param = upload_param;
				if (upload_full_url !== "") extra.upload_full_url = upload_full_url;
			}
			this.getUploadUrlCalls.push({
				filekey: String(request.payload["filekey"] ?? ""),
				media_type: Number(request.payload["media_type"] ?? 0),
				to_user_id: String(request.payload["to_user_id"] ?? ""),
				rawsize: Number(request.payload["rawsize"] ?? 0),
				rawfilemd5: String(request.payload["rawfilemd5"] ?? ""),
				filesize: Number(request.payload["filesize"] ?? 0),
				no_need_thumb: request.payload["no_need_thumb"] === true,
				aeskey: String(request.payload["aeskey"] ?? ""),
				base_info: request.payload["base_info"],
				headers: request.headers,
				ret,
				errcode,
				response: { upload_param, upload_full_url },
				seq,
			});
		}

		const record: ILinkPostRecord = {
			seq,
			endpoint: request.endpoint,
			payload: request.payload,
			base_info: request.payload["base_info"],
			headers: request.headers,
			ret,
			errcode,
		};
		this.postLog.push(record);
		return { ret, errcode, ...extra };
	}

	private recordSendCall(
		seq: number,
		msgRaw: unknown,
		ret: number,
		errcode: number,
	): void {
		const msg = (msgRaw ?? {}) as Record<string, unknown>;
		const itemList = (msg["item_list"] ?? []) as Array<Record<string, unknown>>;
		const textItem = itemList.find((i) => i["type"] === 1);
		const text = String(
			(textItem?.["text_item"] as Record<string, unknown> | undefined)?.[
				"text"
			] ?? "",
		);
		this.sendCalls.push({
			to_user_id: String(msg["to_user_id"] ?? ""),
			text,
			context_token:
				typeof msg["context_token"] === "string"
					? msg["context_token"]
					: undefined,
			client_id: String(msg["client_id"] ?? ""),
			ret,
			errcode,
			seq,
		});
	}

	private recordConfigCall(
		seq: number,
		payload: Record<string, unknown>,
	): void {
		this.getConfigCalls.push({
			ilink_user_id: String(payload["ilink_user_id"] ?? ""),
			context_token:
				typeof payload["context_token"] === "string"
					? payload["context_token"]
					: undefined,
			typing_ticket: this.typingTicket,
			seq,
		});
	}

	/**
	 * The _api_get parity face (QR login). Routes by endpoint; unscripted
	 * answers are the vendor defaults (fresh hex token + liteapp URL / wait).
	 */
	getILink(request: {
		baseUrl: string;
		endpoint: string;
		query: Record<string, string>;
		headers: Record<string, string>;
	}): Record<string, unknown> {
		const seq = ++this.seqCounter;
		if (request.endpoint === EP_GET_BOT_QR) {
			this.qrCodeRequests.push({
				bot_type: request.query["bot_type"] ?? "",
				baseUrl: request.baseUrl,
				headers: request.headers,
				seq,
			});
			return (
				this.qrCodeScripts.shift() ?? {
					qrcode: `qr-${seq}`,
					qrcode_img_content: `https://liteapp.weixin.qq.com/c/${seq}`,
				}
			);
		}
		if (request.endpoint === EP_GET_QR_STATUS) {
			this.qrStatusRequests.push({
				qrcode: request.query["qrcode"] ?? "",
				baseUrl: request.baseUrl,
				headers: request.headers,
				seq,
			});
			return this.qrStatusScripts.shift() ?? { status: "wait" };
		}
		return {};
	}

	/**
	 * weixin.py:_upload_ciphertext face: raw ciphertext POST (octet-stream).
	 * Default answers 200 with an x-encrypted-param; scripts override status
	 * or DROP the param header (missing-param error path).
	 */
	cdnUpload(
		url: string,
		ciphertext: Buffer,
		headers: Record<string, string>,
	): { status: number; headers: Record<string, string> } {
		const seq = ++this.seqCounter;
		const script =
			this.cdnScripts.length > 0 ? this.cdnScripts.shift() : undefined;
		const status = script?.status ?? 200;
		let encryptedParam: string | null;
		if (script?.encryptedParam === null)
			encryptedParam = null; // MISSING header
		else if (typeof script?.encryptedParam === "string") {
			encryptedParam = script.encryptedParam;
		} else encryptedParam = `enc-${seq}`;
		this.cdnUploadCalls.push({
			url,
			ciphertextSize: ciphertext.length,
			ciphertext: Buffer.from(ciphertext),
			contentType: headers["Content-Type"] ?? "",
			status,
			encryptedParam,
			seq,
		});
		return {
			status,
			headers:
				encryptedParam === null ? {} : { "x-encrypted-param": encryptedParam },
		};
	}

	/**
	 * getupdates: consumes ONE scripted behavior when present; otherwise
	 * drains the queue. A successful pull ADVANCES the cursor and returns the
	 * new buf + suggested longpolling_timeout_ms.
	 */
	getUpdates(currentBuf: string): {
		ret?: number | undefined;
		errcode?: number | undefined;
		errmsg?: string | undefined;
		msgs?: ILinkMessage[] | undefined;
		get_updates_buf?: string | undefined;
		longpolling_timeout_ms?: number | undefined;
	} {
		const behavior =
			this.getUpdatesScripts.length > 0
				? (this.getUpdatesScripts.shift() as GetUpdatesBehavior)
				: { kind: "ok" as const };
		if (behavior.kind === "code") {
			const ret = behavior.ret ?? 0;
			const errcode = behavior.errcode ?? 0;
			this.pullLog.push({
				msgCount: 0,
				ret: behavior.ret ?? null,
				errcode: behavior.errcode ?? null,
				buf: currentBuf,
			});
			if (ret === 0 && errcode === 0) return this.drain(currentBuf);
			return {
				...(behavior.ret !== undefined ? { ret } : {}),
				...(behavior.errcode !== undefined ? { errcode } : {}),
				...(behavior.errmsg !== undefined ? { errmsg: behavior.errmsg } : {}),
			};
		}
		return this.drain(currentBuf);
	}

	private drain(currentBuf: string): Record<string, unknown> {
		const msgs = this.queue.splice(0);
		this.bufCounter += 1;
		const buf =
			this.bufCounter === 1
				? `${currentBuf || "buf-1"}`
				: `buf-${this.bufCounter}`;
		this.lastBuf = buf;
		this.pullLog.push({ msgCount: msgs.length, ret: null, errcode: null, buf });
		return {
			ret: 0,
			errcode: 0,
			msgs,
			get_updates_buf: buf,
			...(this.longPollingTimeoutMsOverride === null
				? {}
				: { longpolling_timeout_ms: this.longPollingTimeoutMsOverride }),
		};
	}

	/**
	 * Hold-aware async pull: when the long poll is HELD with an empty queue,
	 * the promise stays pending until releaseUpdates()/pushMessage() wakes it
	 * (the adapter races this against its injected timeout budget).
	 *
	 * `post` carries the adapter's getupdates REQUEST SHAPE (_api_post parity:
	 * merged payload with base_info + full header plane); when supplied, the
	 * resolved pull lands in postLog like any other outgoing POST.
	 */
	async pullAsync(
		currentBuf: string,
		isStale?: (() => boolean) | undefined,
		post?: {
			payload: Record<string, unknown>;
			headers: Record<string, string>;
		},
	): Promise<Record<string, unknown>> {
		// REAL long-poll semantics: an EMPTY queue holds the request open
		// (woken by pushMessage/releaseUpdates) — without this the fake would
		// answer instantly and the adapter's poll loop would spin freely.
		if (this.queue.length === 0 && this.getUpdatesScripts.length === 0) {
			await new Promise<void>((resolve) => {
				this.holdWaiters.push(resolve);
			});
			// A TIMED-OUT (abandoned) pull must NEVER drain messages — the
			// current cycle owns the queue. Stale pulls answer empty.
			if (isStale?.() === true || this.queue.length === 0) {
				const empty = {
					ret: 0,
					errcode: 0,
					msgs: [] as ILinkMessage[],
					get_updates_buf: currentBuf,
					...(this.longPollingTimeoutMsOverride === null
						? {}
						: { longpolling_timeout_ms: this.longPollingTimeoutMsOverride }),
				};
				if (post !== undefined) {
					this.postLog.push({
						seq: ++this.seqCounter,
						endpoint: EP_GET_UPDATES,
						payload: post.payload,
						base_info: post.payload["base_info"],
						headers: post.headers,
						ret: empty.ret,
						errcode: empty.errcode,
					});
				}
				return empty;
			}
		}
		const result = this.getUpdates(currentBuf);
		if (post !== undefined) {
			this.postLog.push({
				seq: ++this.seqCounter,
				endpoint: EP_GET_UPDATES,
				payload: post.payload,
				base_info: post.payload["base_info"],
				headers: post.headers,
				ret: typeof result.ret === "number" ? result.ret : null,
				errcode: typeof result.errcode === "number" ? result.errcode : null,
			});
		}
		return result as Record<string, unknown>;
	}

	// ── legacy direct faces (compat shims over the unified chokepoint) ────

	sendMessage(payload: Record<string, unknown>): {
		ret: number;
		errcode: number;
	} {
		const resp = this.post({
			endpoint: EP_SEND_MESSAGE,
			payload,
			headers: {},
		});
		return { ret: resp.ret, errcode: resp.errcode };
	}

	getConfig(payload: Record<string, unknown>): Record<string, unknown> {
		return this.post({ endpoint: EP_GET_CONFIG, payload, headers: {} });
	}

	sendTyping(payload: Record<string, unknown>): { ret: number } {
		return this.post({ endpoint: EP_SEND_TYPING, payload, headers: {} });
	}
}
