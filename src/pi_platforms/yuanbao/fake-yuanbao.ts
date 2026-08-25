// pi_platforms/yuanbao/fake-yuanbao — the IN-PROCESS fake Yuanbao WS gateway.
// Frames are REAL binary ConnMsg protobufs (built via proto.ts), so the
// adapter's codec path is exercised end-to-end headlessly (04 §8).

import {
	CMD,
	CMD_TYPE,
	concat,
	decodeConnMsg,
	decodeGetGroupMemberListReq,
	decodeMsgBodyElement,
	encodeAuthBind,
	encodeConnMsgFull,
	encodeGetGroupMemberListRspFixture,
	encodeInboundPushFixture,
	encodePushAck,
	encodePing,
	parseFields,
	WT_LEN,
	type ConnMsg,
	type GroupMemberListResult,
	type Head,
	type MsgBodyElement,
} from "./proto.js";

export interface YbSocketListener {
	onOpen(): void;
	onFrame(frame: Uint8Array): void;
	onClose(info: { code: number; reason: string }): void;
	/** Hard transport death WITHOUT a close frame. */
	onError(err: Error): void;
}

export interface YbClientSocket {
	readonly readyState: "connecting" | "open" | "closed";
	sendFrame(frame: Uint8Array): void;
	close(code?: number): void;
	listener: YbSocketListener;
}

interface ServerConn {
	socket: YbClientSocketImpl;
	connectId: string | null;
}

/** A client→server send_message frame decoded for row assertions. */
export interface YbWireSend {
	cmd: string;
	msgId: string;
	fromAccount: string;
	/** C2C recipient (send_c2c_message field 2). */
	toAccount: string;
	/** Group target (send_group_message field 2). */
	groupCode: string;
	/** Quoted-message id (send_group_message field 7; "" when flat). */
	refMsgId: string;
	msgBody: MsgBodyElement[];
}

function lenStrField(data: Uint8Array, fn: number): string {
	for (const f of parseFields(data)) {
		if (
			f.fieldNumber === fn &&
			f.wireType === WT_LEN &&
			f.value instanceof Uint8Array
		) {
			return Buffer.from(f.value).toString("utf8");
		}
	}
	return "";
}

/** Decode an AUTH_BIND request payload into its asserted fields
 * (encodeAuthBind wire shape: req{1 bizId, 2 auth{1 uid,2 source,3 token},
 * 3 dev{1 appVersion,2 os,10 instance,24 botVersion}, 5 route_env}). */
function decodeAuthBindReq(
	data: Uint8Array,
): FakeYuanbaoGateway["authBinds"][number] {
	const auth = bytesAt(data, 2);
	const dev = bytesAt(data, 3);
	return {
		bizId: lenStrField(data, 1),
		uid: lenStrField(auth, 1),
		source: lenStrField(auth, 2),
		token: lenStrField(auth, 3),
		appVersion: lenStrField(dev, 1),
		operationSystem: lenStrField(dev, 2),
		instanceId: lenStrField(dev, 10),
		botVersion: lenStrField(dev, 24),
		routeEnv: lenStrField(data, 5),
	};
}

/** First LEN field payload at fn, copied (Buffer) to dodge view variance. */
function bytesAt(data: Uint8Array, fn: number): Uint8Array {
	for (const f of parseFields(data)) {
		if (
			f.fieldNumber === fn &&
			f.wireType === WT_LEN &&
			f.value instanceof Uint8Array
		) {
			return Buffer.from(f.value);
		}
	}
	return new Uint8Array();
}

function repeatedLenField(data: Uint8Array, fn: number): Uint8Array[] {
	const out: Uint8Array[] = [];
	for (const f of parseFields(data)) {
		if (
			f.fieldNumber === fn &&
			f.wireType === WT_LEN &&
			f.value instanceof Uint8Array
		) {
			out.push(f.value);
		}
	}
	return out;
}

/** Decode a send_c2c/send_group biz payload into an assertable record. */
function decodeWireSend(
	cmd: string,
	msgId: string,
	data: Uint8Array,
): YbWireSend {
	const isGroup = cmd === "send_group_message";
	return {
		cmd,
		msgId,
		fromAccount: lenStrField(data, 3),
		toAccount: isGroup ? "" : lenStrField(data, 2),
		// group target field 2 on group sends; group-ORIGIN code rides C2C
		// field 6 (send_dm → send_c2c_msg_body group_code parity).
		groupCode: isGroup ? lenStrField(data, 2) : lenStrField(data, 6),
		refMsgId: isGroup ? lenStrField(data, 7) : "",
		msgBody: repeatedLenField(data, isGroup ? 6 : 5).map(decodeMsgBodyElement),
	};
}

/**
 * The fake gateway: AUTH_BIND handshake (BIND_ACK carries connectId), ping →
 * pong, Push frames w/ need_ack (client must PushAck), scripted close codes,
 * hard drops, and connection refusal. Serves get_group_member_list from a
 * configurable per-group directory and audits decoded send_message frames.
 */
export class FakeYuanbaoGateway {
	private readonly connections: ServerConn[] = [];
	private connectCounter = 0;

	acceptNext = true;

	get openConnectionCount(): number {
		return this.connections.filter((c) => c.socket.readyState === "open")
			.length;
	}

	/** Pushes the fake has emitted for the CURRENT session (row audit). */
	readonly pushLog: Array<{ cmd: string; msgId: string; acked: boolean }> = [];
	/** Client→server frames received (identify/ping/ack audit). */
	readonly receivedFrames: Array<{ kind: string; msgId?: string | undefined }> =
		[];
	/** Decoded client→server send_message frames, in wire order (row audit). */
	readonly sentMessages: YbWireSend[] = [];
	/** Per-group member-list responses served for get_group_member_list;
	 * an absent group answers code=50004 "group not found" with no members. */
	readonly memberDirectory = new Map<string, GroupMemberListResult>();
	/** Decoded client→server AUTH_BIND requests, in wire order (audit):
	 * uid/source/token + device app/bot versions + env_name(route_env). */
	readonly authBinds: Array<{
		bizId: string;
		uid: string;
		source: string;
		token: string;
		appVersion: string;
		operationSystem: string;
		instanceId: string;
		botVersion: string;
		routeEnv: string;
	}> = [];
	/** When true the fake withholds BIND_ACK after AUTH_BIND — the client
	 * handshake must fail closed against AUTH_TIMEOUT_S instead. */
	withholdBindAck = false;
	/** Pushes emitted while NO live session existed — delivered on reconnect
	 * (at-least-once redelivery; the adapter's dedup absorbs duplicates). */
	private readonly replayQueue: Array<Uint8Array> = [];

	refuseConnections(): void {
		this.acceptNext = false;
	}

	acceptConnections(): void {
		this.acceptNext = true;
	}

	dropActive(code: number, reason = ""): void {
		for (const conn of [...this.connections]) {
			conn.socket.serverClose({ code, reason });
		}
	}

	hardDrop(): void {
		for (const conn of [...this.connections]) {
			conn.socket.serverHardDrop();
		}
	}

	pushMessage(
		push: Record<string, unknown>,
		opts: { needAck?: boolean } = {},
	): void {
		const bizPayload = encodeInboundPushFixture(push);
		const msgId = `push-${++this.connectCounter}`;
		const frame = encodeConnMsgFull(
			CMD_TYPE.Push,
			"InboundMessagePush",
			1,
			msgId,
			"conn_access",
			bizPayload,
			opts.needAck !== false,
		);
		this.pushLog.push({ cmd: "InboundMessagePush", msgId, acked: false });
		this.deliverOrQueue(frame);
	}

	/** JSON-frame push parity: the wire ships RAW-JSON payloads too
	 * (decodeFramePayload tries JSON before the binary proto). */
	pushJson(push: Record<string, unknown>): void {
		const payload = Buffer.from(JSON.stringify(push), "utf8");
		const msgId = `push-${++this.connectCounter}`;
		const frame = encodeConnMsgFull(
			CMD_TYPE.Push,
			"InboundMessagePush",
			1,
			msgId,
			"conn_access",
			payload,
			true,
		);
		this.pushLog.push({ cmd: "InboundMessagePush", msgId, acked: false });
		this.deliverOrQueue(frame);
	}

	/** Live delivery, or hold for redelivery on the next AUTH_BIND. */
	private deliverOrQueue(frame: Uint8Array): void {
		let delivered = false;
		for (const conn of this.connections) {
			if (conn.socket.readyState === "open") {
				conn.socket.serverDeliver(frame);
				delivered = true;
			}
		}
		if (!delivered) this.replayQueue.push(frame);
	}

	markPushAcked(msgId: string): void {
		const rec = this.pushLog.find((p) => p.msgId === msgId);
		if (rec !== undefined) rec.acked = true;
	}

	connect(listener: YbSocketListener): YbClientSocket {
		const socket = new YbClientSocketImpl(listener, this);
		if (!this.acceptNext) {
			queueMicrotask(() => socket.serverRefuse());
			return socket;
		}
		queueMicrotask(() => socket.serverAccept());
		return socket;
	}

	internalRegister(conn: ServerConn): void {
		this.connections.push(conn);
	}

	internalRemove(conn: ServerConn): void {
		const idx = this.connections.indexOf(conn);
		if (idx >= 0) this.connections.splice(idx, 1);
	}

	internalHandleClientFrame(conn: ServerConn, frame: Uint8Array): void {
		let decoded: ConnMsg;
		try {
			decoded = decodeConnMsg(frame);
		} catch {
			return;
		}
		const head = decoded.head;
		if (head.cmd_type === CMD_TYPE.Request && head.cmd === CMD.AuthBind) {
			this.receivedFrames.push({ kind: "auth-bind", msgId: head.msg_id });
			this.authBinds.push(decodeAuthBindReq(decoded.data));
			this.connectCounter += 1;
			conn.connectId = `connect-${this.connectCounter}`;
			if (this.withholdBindAck) return; // silence: client must time-box
			// BIND_ACK: Response w/ cmd auth-bind; data = AuthBindRsp{code=1,
			// connect_id=3} — field 1 varint 0 (ok), field 3 string connectId.
			const rspData = concat([
				new Uint8Array([0x08, 0x00]), // field 1 varint 0
				buildLenField(3, Buffer.from(conn.connectId, "utf8")),
			]);
			conn.socket.serverDeliver(
				encodeConnMsgFull(
					CMD_TYPE.Response,
					CMD.AuthBind,
					head.seq_no + 1,
					head.msg_id,
					head.module,
					rspData,
				),
			);
			// Replay anything queued while no session existed.
			const held = this.replayQueue.splice(0);
			for (const frame of held) conn.socket.serverDeliver(frame);
			return;
		}
		if (head.cmd_type === CMD_TYPE.Request && head.cmd === CMD.Ping) {
			this.receivedFrames.push({ kind: "ping", msgId: head.msg_id });
			conn.socket.serverDeliver(
				encodeConnMsgFull(
					CMD_TYPE.Response,
					CMD.Ping,
					head.seq_no + 1,
					head.msg_id,
					head.module,
					new Uint8Array(0),
				),
			);
			return;
		}
		if (head.cmd_type === CMD_TYPE.PushAck) {
			this.receivedFrames.push({ kind: "push-ack", msgId: head.msg_id });
			this.markPushAcked(head.msg_id);
			return;
		}
		if (
			head.cmd_type === CMD_TYPE.Request &&
			(head.cmd === "send_group_message" || head.cmd === "send_c2c_message")
		) {
			this.receivedFrames.push({ kind: head.cmd, msgId: head.msg_id });
			this.sentMessages.push(
				decodeWireSend(head.cmd, head.msg_id, decoded.data),
			);
			return;
		}
		if (
			head.cmd_type === CMD_TYPE.Request &&
			head.cmd === "get_group_member_list"
		) {
			this.receivedFrames.push({
				kind: "get_group_member_list",
				msgId: head.msg_id,
			});
			const req = decodeGetGroupMemberListReq(decoded.data);
			const result = this.memberDirectory.get(req.groupCode) ?? {
				code: 50004,
				message: "group not found",
				members: [],
				next_offset: 0,
				is_complete: true,
			};
			conn.socket.serverDeliver(
				encodeConnMsgFull(
					CMD_TYPE.Response,
					head.cmd,
					head.seq_no + 1,
					head.msg_id,
					head.module,
					encodeGetGroupMemberListRspFixture(result),
				),
			);
			return;
		}
		if (head.cmd_type === CMD_TYPE.Request) {
			this.receivedFrames.push({ kind: head.cmd, msgId: head.msg_id });
		}
	}
}

function buildLenField(fieldNumber: number, payload: Uint8Array): Uint8Array {
	// tag varint + length varint + payload
	const tag = encodeVarintSmall((fieldNumber << 3) | 2);
	return concat([tag, concat([encodeVarintSmall(payload.length), payload])]);
}

function encodeVarintSmall(value: number): Uint8Array {
	const out: number[] = [];
	let v = value;
	for (;;) {
		const bits = v & 0x7f;
		v >>>= 7;
		if (v !== 0) out.push(bits | 0x80);
		else {
			out.push(bits);
			break;
		}
	}
	return Uint8Array.from(out);
}

function encodeVarintSmallUnused(): void {}
void encodeVarintSmallUnused;

// silence unused import (Head re-exported for consumers)
export type { Head };
void encodePushAck;
void encodeAuthBind;
void encodePing;

class YbClientSocketImpl implements YbClientSocket {
	private state: "connecting" | "open" | "closed" = "connecting";
	private conn: ServerConn | null = null;
	listener: YbSocketListener;
	private readonly serverRef: FakeYuanbaoGateway;

	constructor(listener: YbSocketListener, server: FakeYuanbaoGateway) {
		this.listener = listener;
		this.serverRef = server;
	}

	get readyState(): "connecting" | "open" | "closed" {
		return this.state;
	}

	sendFrame(frame: Uint8Array): void {
		if (this.state !== "open") {
			throw new Error(`send on non-open socket (state=${this.state})`);
		}
		this.serverRef.internalHandleClientFrame(this.conn as ServerConn, frame);
	}

	close(code = 1000): void {
		if (this.state !== "open" && this.state !== "connecting") return;
		this.detach();
		this.state = "closed";
		this.listener.onClose({ code, reason: "" });
	}

	serverAccept(): void {
		if (this.state !== "connecting") return;
		this.conn = { socket: this, connectId: null };
		this.serverRef.internalRegister(this.conn);
		this.state = "open";
		this.listener.onOpen();
		// No Hello equivalent — the client drives AUTH_BIND immediately.
	}

	serverRefuse(): void {
		if (this.state !== "connecting") return;
		this.state = "closed";
		this.listener.onError(new Error("connect ECONNREFUSED"));
		this.listener.onClose({ code: 1006, reason: "" });
	}

	serverDeliver(frame: Uint8Array): void {
		if (this.state !== "open") return;
		this.listener.onFrame(frame);
	}

	serverClose(info: { code: number; reason: string }): void {
		if (this.state !== "open" && this.state !== "connecting") return;
		this.detach();
		this.state = "closed";
		this.listener.onClose(info);
	}

	serverHardDrop(): void {
		if (this.state !== "open") return;
		this.detach();
		this.state = "closed";
		this.listener.onError(new Error("socket hang up (ECONNRESET)"));
	}

	private detach(): void {
		if (this.conn) {
			this.serverRef.internalRemove(this.conn);
			this.conn = null;
		}
	}
}
