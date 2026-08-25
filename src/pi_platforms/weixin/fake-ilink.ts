// pi_platforms/weixin/fake-ilink — the IN-PROCESS fake WeChat iLink server
// (04 §8: headless fake platform; NO real network).
//
// Faces:
//   • getupdates LONG POLL — messages queue server-side; the sync_buf cursor
//     advances ONLY on a successful pull (queue-preservation semantics the
//     outage row proves). Scriptable error codes (-14 session expired,
//     -2 rate limit) and long-poll HOLDS (no messages → wait until release
//     or injected timeout).
//   • sendmessage / getconfig / sendtyping REST captures with scripted rets.
//
// Vendor error codes are matched NUMERICALLY by the adapter (Hermes parity:
// `ret == SESSION_EXPIRED_ERRCODE`), never via snapshotted strings.

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
	}> = [];

	longPollHoldEnabled = false;
	private holdWaiters: Array<() => void> = [];

	readonly sendCalls: SendCallRecord[] = [];
	readonly getConfigCalls: GetConfigCallRecord[] = [];
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

	scriptSendMessage(ret: number, errcode?: number | undefined): void {
		this.sendMessageScripts.push({
			ret,
			...(errcode !== undefined ? { errcode } : {}),
		});
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
		void this.seqCounter;
		return {
			ret: 0,
			errcode: 0,
			msgs,
			get_updates_buf: buf,
			longpolling_timeout_ms: 35_000,
		};
	}

	/**
	 * Hold-aware async pull: when the long poll is HELD with an empty queue,
	 * the promise stays pending until releaseUpdates()/pushMessage() wakes it
	 * (the adapter races this against its injected timeout budget).
	 */
	async pullAsync(
		currentBuf: string,
		isStale?: (() => boolean) | undefined,
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
				return {
					ret: 0,
					errcode: 0,
					msgs: [],
					get_updates_buf: currentBuf,
					longpolling_timeout_ms: 35_000,
				};
			}
		}
		return this.getUpdates(currentBuf) as Record<string, unknown>;
	}

	sendMessage(payload: { msg?: Record<string, unknown> | undefined }): {
		ret: number;
		errcode: number;
	} {
		const msg = (payload["msg"] ?? {}) as Record<string, unknown>;
		const itemList = (msg["item_list"] ?? []) as Array<Record<string, unknown>>;
		const textItem = itemList.find((i) => i["type"] === 1);
		const text = String(
			(textItem?.["text_item"] as Record<string, unknown> | undefined)?.[
				"text"
			] ?? "",
		);
		const scripted =
			this.sendMessageScripts.length > 0
				? this.sendMessageScripts.shift()
				: undefined;
		const rec: SendCallRecord = {
			to_user_id: String(msg["to_user_id"] ?? ""),
			text,
			context_token:
				typeof msg["context_token"] === "string"
					? msg["context_token"]
					: undefined,
			client_id: String(msg["client_id"] ?? ""),
			ret: scripted?.ret ?? null,
			errcode: scripted?.errcode ?? null,
			seq: ++this.seqCounter,
		};
		this.sendCalls.push(rec);
		return { ret: scripted?.ret ?? 0, errcode: scripted?.errcode ?? 0 };
	}

	getConfig(payload: Record<string, unknown>): Record<string, unknown> {
		this.getConfigCalls.push({
			ilink_user_id: String(payload["ilink_user_id"] ?? ""),
			context_token:
				typeof payload["context_token"] === "string"
					? payload["context_token"]
					: undefined,
			typing_ticket: this.typingTicket,
			seq: ++this.seqCounter,
		});
		return { typing_ticket: this.typingTicket };
	}

	sendTyping(_payload: Record<string, unknown>): { ret: number } {
		void _payload;
		return { ret: 0 };
	}
}
