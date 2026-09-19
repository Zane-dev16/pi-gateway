// pi_platforms/matrix/hs-client — REAL Matrix Client-Server API HTTP
// transport (DEC-073). Dependency-free (`global fetch`); implements the SAME
// seam the adapter consumes (`MatrixHomeserverSeam`) so the production
// factory binds it where tests bind `FakeMatrixHomeserver` — the fake stays
// the TEST seam untouched.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/matrix/adapter.py:connect            (whoami + login)
//   plugins/platforms/matrix/adapter.py:_sync_loop         (timeout/full-state)
//   plugins/platforms/matrix/adapter.py:_send_reaction     (relates_to-only PUT)
//   plugins/platforms/matrix/adapter.py:send_typing / stop_typing (30s / 0)
//   plugins/platforms/matrix/adapter.py:_background_read_receipt
//   plugins/platforms/matrix/adapter.py:_upload_and_send
//   plugins/platforms/matrix/adapter.py:_standalone_send   (Bearer auth,
//     PUT …/rooms/{id}/send/m.room.message/{txn_id}, 30 s timeout)

import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type {
	MatrixSyncResponse,
	MatrixSyncResult,
	MatrixTimelineEvent,
} from "./matrix-fake-server.js";
import {
	MatrixTransportError,
	MatrixUnknownSyncTokenError,
} from "./matrix-fake-server.js";

/**
 * THE seam the adapter consumes. `FakeMatrixHomeserver` satisfies it
 * structurally (no fake edits); this module's HTTP client implements it for
 * production. Directory getters tolerate: missing state ⇒ null (fake parity).
 */
export interface MatrixHomeserverSeam {
	whoami(): Promise<{ user_id: string; device_id: string }>;
	login(op: {
		identifier: string;
		password: string;
		deviceName?: string | undefined;
		deviceId?: string | undefined;
	}): Promise<{ user_id: string; device_id: string }>;
	sync(opts: {
		since: string | null;
		timeoutMs: number;
		fullState?: boolean | undefined;
	}): Promise<MatrixSyncResult>;
	closeSessions(): void;
	joinRoom(roomId: string): Promise<{ room_id: string }>;
	getRoomName(roomId: string): Promise<string | null>;
	getRoomCanonicalAlias(roomId: string): Promise<string | null>;
	getRoomTopic(roomId: string): Promise<string | null>;
	getJoinedMemberCount(roomId: string): Promise<number | null>;
	getDirectAccountData(): Promise<Record<string, string[]>>;
	setTyping(roomId: string, userId: string, timeoutMs: number): Promise<void>;
	sendReaction(
		roomId: string,
		targetEventId: string,
		key: string,
	): Promise<string>;
	redactEvent(roomId: string, eventId: string): Promise<void>;
	sendReadReceipt(roomId: string, eventId: string): Promise<void>;
	uploadMedia(op: {
		data: Uint8Array;
		mimeType: string;
		filename?: string | undefined;
	}): Promise<string>;
}

export interface HttpMatrixHomeserverOptions {
	/** Homeserver origin, e.g. https://matrix.example.org (MATRIX_HOMESERVER). */
	baseUrl: string;
	/** Access token (MATRIX_ACCESS_TOKEN); empty until password login. */
	accessToken?: string | undefined;
	/** Pre-known mxid (MATRIX_USER_ID) for the account-data path. */
	ownUserId?: string | undefined;
}

/** Non-sync endpoints get a 30 s abort (`_standalone_send` parity). */
const NON_SYNC_TIMEOUT_MS = 30_000;

interface WireError {
	errcode: string;
	error: string;
	retryAfterMs: number | null;
}

export class HttpMatrixHomeserver implements MatrixHomeserverSeam {
	private readonly baseUrl: string;
	private accessToken: string;
	private ownUserId: string | null;
	private seq = 0;
	private txnSeq = 0;
	private readonly inflight = new Set<AbortController>();

	constructor(opts: HttpMatrixHomeserverOptions) {
		this.baseUrl = opts.baseUrl.replace(/\/+$/, "");
		this.accessToken = opts.accessToken ?? "";
		this.ownUserId = opts.ownUserId ?? null;
	}

	/** adapter.py teardown parity: parked long-polls DIE (transport class). */
	closeSessions(): void {
		for (const c of this.inflight) {
			try {
				c.abort();
			} catch {
				/* already settled */
			}
		}
		this.inflight.clear();
	}

	async whoami(): Promise<{ user_id: string; device_id: string }> {
		const json = await this.call("GET", "/_matrix/client/v3/account/whoami");
		this.ownUserId = String(json["user_id"] ?? "");
		return {
			user_id: this.ownUserId,
			device_id: String(json["device_id"] ?? ""),
		};
	}

	async login(op: {
		identifier: string;
		password: string;
		deviceName?: string | undefined;
		deviceId?: string | undefined;
	}): Promise<{ user_id: string; device_id: string }> {
		// Unauthenticated by construction (no token yet) — CS-API login shape.
		const json = await this.call(
			"POST",
			"/_matrix/client/v3/login",
			{
				body: {
					type: "m.login.password",
					identifier: { type: "m.id.user", user: op.identifier },
					password: op.password,
					...(op.deviceId !== undefined ? { device_id: op.deviceId } : {}),
					initial_device_display_name: op.deviceName ?? "pi-gateway",
				},
			},
			{ auth: false },
		);
		// The session token arrives HERE (the seam returns no token slot).
		if (typeof json["access_token"] === "string") {
			this.accessToken = json["access_token"] as string;
		}
		this.ownUserId = String(json["user_id"] ?? op.identifier);
		return {
			user_id: this.ownUserId,
			device_id: String(json["device_id"] ?? op.deviceId ?? ""),
		};
	}

	async sync(opts: {
		since: string | null;
		timeoutMs: number;
		fullState?: boolean | undefined;
	}): Promise<MatrixSyncResult> {
		// The server `next_batch` is OPAQUE — passed through verbatim, never
		// parsed (DEC-073: no client-side token codec).
		const query: Record<string, string> = {
			timeout: String(Math.max(0, opts.timeoutMs)),
		};
		if (opts.since !== null) query["since"] = opts.since;
		if (opts.fullState === true) query["full_state"] = "true";
		let status: number;
		let json: Record<string, unknown>;
		try {
			({ status, json } = await this.raw("GET", "/_matrix/client/v3/sync", {
				query,
				sync: true,
			}));
		} catch (err) {
			throw asTransport(err, "sync");
		}
		if (status >= 200 && status < 300)
			return mapSyncResponse(json, () => ++this.seq);
		const wire = parseWireError(json, status);
		// nio soft-logout parity: auth death arrives as a RESULT OBJECT, not a
		// throw (the adapter string-checks `m_unknown_token` ⇒ loud fatal).
		if (
			wire.errcode === "M_UNKNOWN_TOKEN" ||
			wire.errcode === "M_MISSING_TOKEN"
		) {
			return { message: `${wire.errcode}: ${wire.error}` };
		}
		if (wire.errcode === "M_UNKNOWN_SYNC_TOKEN") {
			throw new MatrixUnknownSyncTokenError();
		}
		throw new MatrixTransportError(`sync: ${describeWire(wire, status)}`);
	}

	async joinRoom(roomId: string): Promise<{ room_id: string }> {
		const json = await this.call(
			"POST",
			`/_matrix/client/v3/join/${enc(roomId)}`,
			{ body: {} },
		);
		return { room_id: String(json["room_id"] ?? roomId) };
	}

	async getRoomName(roomId: string): Promise<string | null> {
		return this.roomStateString(roomId, "m.room.name", "name");
	}

	async getRoomCanonicalAlias(roomId: string): Promise<string | null> {
		return this.roomStateString(roomId, "m.room.canonical_alias", "alias");
	}

	async getRoomTopic(roomId: string): Promise<string | null> {
		return this.roomStateString(roomId, "m.room.topic", "topic");
	}

	async getJoinedMemberCount(roomId: string): Promise<number | null> {
		try {
			const json = await this.call(
				"GET",
				`/_matrix/client/v3/rooms/${enc(roomId)}/joined_members`,
			);
			const joined = json["joined"];
			if (typeof joined !== "object" || joined === null) return null;
			return Object.keys(joined as Record<string, unknown>).length;
		} catch {
			return null; // unknown, never fatal (fake-null parity)
		}
	}

	async getDirectAccountData(): Promise<Record<string, string[]>> {
		if (this.ownUserId === null) return {};
		try {
			const json = await this.call(
				"GET",
				`/_matrix/client/v3/user/${enc(this.ownUserId)}/account_data/m.direct`,
			);
			const out: Record<string, string[]> = {};
			for (const [owner, rooms] of Object.entries(json)) {
				if (Array.isArray(rooms) && rooms.every((r) => typeof r === "string")) {
					out[owner] = rooms as string[];
				}
			}
			return out;
		} catch {
			return {};
		}
	}

	async setTyping(
		roomId: string,
		userId: string,
		timeoutMs: number,
	): Promise<void> {
		try {
			await this.call(
				"PUT",
				`/_matrix/client/v3/rooms/${enc(roomId)}/typing/${enc(userId)}`,
				{ body: { typing: timeoutMs > 0, timeout: timeoutMs } },
			);
		} catch (err) {
			// Fake-identical M_LIMIT_EXCEEDED shape — the adapter's typing
			// honor-once site parses `retry_after_ms` out of the text.
			throw asLimitError(err, "setTyping");
		}
	}

	async sendReaction(
		roomId: string,
		targetEventId: string,
		key: string,
	): Promise<string> {
		const json = await this.call(
			"PUT",
			`/_matrix/client/v3/rooms/${enc(roomId)}/send/m.reaction/${enc(this.txn())}`,
			{
				body: {
					"m.relates_to": {
						rel_type: "m.annotation",
						event_id: targetEventId,
						key,
					},
				},
			},
		);
		return String(json["event_id"] ?? "");
	}

	async redactEvent(roomId: string, eventId: string): Promise<void> {
		await this.call(
			"PUT",
			`/_matrix/client/v3/rooms/${enc(roomId)}/redact/${enc(eventId)}/${enc(this.txn())}`,
			{ body: {} },
		);
	}

	async sendReadReceipt(roomId: string, eventId: string): Promise<void> {
		await this.call(
			"POST",
			`/_matrix/client/v3/rooms/${enc(roomId)}/receipt/m.read/${enc(eventId)}`,
			{ body: {} },
		);
	}

	/**
	 * `_standalone_send` parity: PUT the FULL vendor content dict (mentions +
	 * formatted_body ride inside) to `…/send/m.room.message/{txn}`.
	 */
	async sendMessage(
		roomId: string,
		content: Record<string, unknown>,
	): Promise<string> {
		const json = await this.call(
			"PUT",
			`/_matrix/client/v3/rooms/${enc(roomId)}/send/m.room.message/${enc(this.txn())}`,
			{ body: content },
		);
		return String(json["event_id"] ?? "");
	}

	async uploadMedia(op: {
		data: Uint8Array;
		mimeType: string;
		filename?: string | undefined;
	}): Promise<string> {
		const query = op.filename !== undefined ? { filename: op.filename } : {};
		const { status, json } = await this.raw(
			"POST",
			"/_matrix/media/v3/upload",
			{ query, rawBody: op.data, contentType: op.mimeType },
		);
		if (status >= 200 && status < 300) {
			return String(json["content_uri"] ?? "");
		}
		const wire = parseWireError(json, status);
		throw new Error(`uploadMedia: ${describeWire(wire, status)}`);
	}

	// ── internals ─────────────────────────────────────────────────────────

	private async roomStateString(
		roomId: string,
		eventType: string,
		key: string,
	): Promise<string | null> {
		try {
			const json = await this.call(
				"GET",
				`/_matrix/client/v3/rooms/${enc(roomId)}/state/${enc(eventType)}/`,
			);
			const value = json[key];
			return typeof value === "string" && value !== "" ? value : null;
		} catch {
			return null; // missing state ⇒ unknown (fake-null parity)
		}
	}

	/** Throwing call: 429 keeps the fake-identical limit shape, else status text. */
	private async call(
		method: string,
		path: string,
		opts: {
			query?: Record<string, string> | undefined;
			body?: Record<string, unknown> | undefined;
		} = {},
		flags: { auth?: boolean | undefined } = {},
	): Promise<Record<string, unknown>> {
		const { status, json } = await this.raw(method, path, {
			...(opts.query !== undefined ? { query: opts.query } : {}),
			...(opts.body !== undefined ? { jsonBody: opts.body } : {}),
			...(flags.auth !== undefined ? { auth: flags.auth } : {}),
		});
		if (status >= 200 && status < 300) return json;
		const wire = parseWireError(json, status);
		throw limitOrStatusError(`${method} ${path}`, wire, status);
	}

	private async raw(
		method: string,
		path: string,
		opts: {
			query?: Record<string, string> | undefined;
			jsonBody?: Record<string, unknown> | undefined;
			rawBody?: Uint8Array | undefined;
			contentType?: string | undefined;
			auth?: boolean | undefined;
			/** Sync long-polls: no client abort (watchdog owns stuck). */
			sync?: boolean | undefined;
		} = {},
	): Promise<{ status: number; json: Record<string, unknown> }> {
		const url = this.baseUrl + path + queryString(opts.query ?? {});
		const controller = new AbortController();
		this.inflight.add(controller);
		// Non-sync endpoints self-bound at 30 s; sync rides until the server
		// releases the long-poll or closeSessions() aborts it (DEC-073).
		const timer =
			opts.sync === true
				? null
				: setTimeout(() => {
						try {
							controller.abort();
						} catch {
							/* settled */
						}
					}, NON_SYNC_TIMEOUT_MS);
		timer?.unref?.();
		try {
			const headers: Record<string, string> = { Accept: "application/json" };
			if (this.accessToken !== "" && opts.auth !== false) {
				headers["Authorization"] = `Bearer ${this.accessToken}`;
			}
			const init: RequestInit = { method, headers, signal: controller.signal };
			if (opts.jsonBody !== undefined) {
				headers["Content-Type"] = "application/json";
				init.body = JSON.stringify(opts.jsonBody);
			} else if (opts.rawBody !== undefined) {
				headers["Content-Type"] =
					opts.contentType ?? "application/octet-stream";
				init.body = opts.rawBody as unknown as BodyInit;
			}
			let res: Response;
			try {
				res = await fetch(url, init);
			} catch (err) {
				throw new MatrixTransportError(`${method} ${path}: ${brief(err)}`);
			}
			return { status: res.status, json: await safeJson(res) };
		} finally {
			if (timer !== null) clearTimeout(timer);
			this.inflight.delete(controller);
		}
	}

	private txn(): string {
		this.txnSeq += 1;
		return `pigw_${Date.now()}_${this.txnSeq}`;
	}
}

/**
 * Production egress binding: the adapter builds THE FULL vendor event content
 * (`restSend` puts it at `metadata.event_content` — mentions + relations +
 * formatted_body inside) and this PUTs it to `…/send/m.room.message`. Edits
 * ride the same endpoint (an edit IS an `m.room.message` event carrying
 * `m.replace`). Draft/rich lanes stay on adapter defaults — the chokepoint
 * `seal-failed-plain-send` fallback covers unbound drafts.
 */
export interface MatrixProductionEgressTarget {
	wireTransmitSend: (
		chatId: string,
		content: string,
		metadata: Metadata,
	) => Promise<SendResult>;
	wireTransmitEdit: (
		chatId: string,
		messageId: string,
		content: string,
		metadata: Metadata,
	) => Promise<SendResult>;
}

export function bindMatrixProductionTransport(
	target: MatrixProductionEgressTarget,
	client: HttpMatrixHomeserver,
): void {
	const sendContent = async (
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> => {
		try {
			const md = metadata as unknown as Record<string, unknown>;
			const raw = md["event_content"];
			const eventContent =
				typeof raw === "object" && raw !== null
					? (raw as Record<string, unknown>)
					: { msgtype: "m.text", body: content };
			const eventId = await client.sendMessage(chatId, eventContent);
			return { success: true, messageId: eventId };
		} catch (err) {
			return { success: false, error: brief(err) };
		}
	};
	target.wireTransmitSend = (chatId, content, metadata) =>
		sendContent(chatId, content, metadata);
	target.wireTransmitEdit = (chatId, _messageId, content, metadata) =>
		sendContent(chatId, content, metadata);
}

// ── wire mapping ────────────────────────────────────────────────────────────

function mapSyncResponse(
	json: Record<string, unknown>,
	nextSeq: () => number,
): MatrixSyncResponse {
	const rooms = (json["rooms"] as Record<string, unknown> | undefined) ?? {};
	const joinWire = (rooms["join"] as Record<string, unknown> | undefined) ?? {};
	const inviteWire =
		(rooms["invite"] as Record<string, unknown> | undefined) ?? {};
	const join: MatrixSyncResponse["rooms"]["join"] = {};
	for (const [roomId, roomWire] of Object.entries(joinWire)) {
		const timeline =
			((roomWire as Record<string, unknown>)["timeline"] as
				| Record<string, unknown>
				| undefined) ?? {};
		const eventsWire = (timeline["events"] as unknown[] | undefined) ?? [];
		const events: MatrixTimelineEvent[] = [];
		for (const raw of eventsWire) {
			const evt = mapTimelineEvent(roomId, raw, nextSeq);
			if (evt !== null) events.push(evt);
		}
		join[roomId] = { timeline: { events } };
	}
	const invite: MatrixSyncResponse["rooms"]["invite"] = {};
	for (const [roomId, roomWire] of Object.entries(inviteWire)) {
		const state =
			((roomWire as Record<string, unknown>)["invite_state"] as
				| Record<string, unknown>
				| undefined) ?? {};
		invite[roomId] = {
			invite_state: {
				events: Array.isArray(state["events"])
					? (state["events"] as unknown[])
					: [],
			},
		};
	}
	return {
		next_batch: String(json["next_batch"] ?? ""),
		rooms: { join, invite },
	};
}

function mapTimelineEvent(
	roomId: string,
	raw: unknown,
	nextSeq: () => number,
): MatrixTimelineEvent | null {
	if (typeof raw !== "object" || raw === null) return null;
	const evt = raw as Record<string, unknown>;
	const sender = evt["sender"];
	const content = evt["content"];
	if (
		typeof sender !== "string" ||
		typeof content !== "object" ||
		content === null
	) {
		return null;
	}
	return {
		eventId: String(evt["event_id"] ?? ""),
		roomId,
		sender,
		originServerTsMs: Number(evt["origin_server_ts"] ?? 0),
		type: evt["type"] === "m.reaction" ? "m.reaction" : "m.room.message",
		content: content as Record<string, unknown>,
		seq: nextSeq(),
	};
}

function parseWireError(
	json: Record<string, unknown>,
	status: number,
): WireError {
	const errcode =
		typeof json["errcode"] === "string" ? json["errcode"] : `M_HTTP_${status}`;
	const error =
		typeof json["error"] === "string" ? json["error"] : "request failed";
	const retryAfterMs =
		typeof json["retry_after_ms"] === "number" ? json["retry_after_ms"] : null;
	return { errcode, error, retryAfterMs };
}

function describeWire(wire: WireError, status: number): string {
	const retry =
		wire.retryAfterMs !== null ? ` (retry_after_ms=${wire.retryAfterMs})` : "";
	return `${status} ${wire.errcode}: ${wire.error}${retry}`;
}

/** 429 keeps the fake-identical limit shape the adapter parses; else status. */
function limitOrStatusError(
	what: string,
	wire: WireError,
	status: number,
): Error {
	if (wire.errcode === "M_LIMIT_EXCEEDED") {
		const retry =
			wire.retryAfterMs !== null
				? ` (retry_after_ms=${wire.retryAfterMs})`
				: "";
		return new Error(`M_LIMIT_EXCEEDED: ${wire.error}${retry}`);
	}
	return new Error(`${what}: ${describeWire(wire, status)}`);
}

/** Re-wrap a call-site failure preserving an already-shaped limit error. */
function asLimitError(err: unknown, what: string): Error {
	if (err instanceof Error && err.message.includes("M_LIMIT_EXCEEDED")) {
		return err;
	}
	return err instanceof Error ? err : new Error(`${what}: ${String(err)}`);
}

function asTransport(err: unknown, what: string): Error {
	if (
		err instanceof MatrixTransportError ||
		err instanceof MatrixUnknownSyncTokenError
	) {
		return err;
	}
	return new MatrixTransportError(
		`${what}: ${err instanceof Error ? err.message : String(err)}`,
	);
}

async function safeJson(res: Response): Promise<Record<string, unknown>> {
	try {
		const json = (await res.json()) as unknown;
		if (typeof json === "object" && json !== null) {
			return json as Record<string, unknown>;
		}
		return {};
	} catch {
		return {};
	}
}

function queryString(query: Record<string, string>): string {
	const keys = Object.keys(query);
	if (keys.length === 0) return "";
	const params = new URLSearchParams();
	for (const key of keys) params.set(key, query[key] ?? "");
	return `?${params.toString()}`;
}

function enc(segment: string): string {
	return encodeURIComponent(segment);
}

function brief(err: unknown): string {
	return String(err instanceof Error ? err.message : err).slice(0, 120);
}
