// pi_platforms/yuanbao/proto — hand-rolled protobuf wire codec for the
// Yuanbao WS gateway, ported from Hermes gateway/platforms/yuanbao_proto.py.
//
// Hermes anchors (READ-ONLY reference; schema + semantics ported):
//   yuanbao_proto.py:_encode_varint/_decode_varint/_parse_fields/_fields_to_dict
//   yuanbao_proto.py:_encode_head/_decode_head — Head{cmd_type=1,cmd=2,seq_no=3,
//     msg_id=4,module=5,need_ack=6,status=10}
//   yuanbao_proto.py:encode_conn_msg/decode_conn_msg — ConnMsg{head=1,data=2}
//   yuanbao_proto.py:CMD_TYPE / CMD / MODULE / _BIZ_PKG / HERMES_INSTANCE_ID
//   yuanbao_proto.py:encode_biz_msg/decode_biz_msg — biz wrapper (Request +
//     head.cmd=method, head.module=service)
//   yuanbao_proto.py:encode_auth_bind — AuthBindReq{biz_id=1, auth_info=2
//     {uid=1,source=2,token=3}, device_info=3{app_version=1,os=2,instance_id=10,
//     bot_version=24}, env_name=5}
//   yuanbao_proto.py:encode_send_c2c_message / encode_send_group_message
//   yuanbao_proto.py:encode_get_group_member_list / decode_get_group_member_list_rsp
//     — GetGroupMemberListReq{group_code=1, offset=2, limit=3} wrapped via
//     encode_biz_msg(method="get_group_member_list"); Rsp{code=1, message=2,
//     members=3 repeated MemberInfo{user_id=1,nickname=2,role=3,join_time=4,
//     name_card=5}, next_offset=4, is_complete=5}
//   yuanbao_proto.py:decode_inbound_push — InboundMessagePush fields 1..20

export const WT_VARINT = 0;
export const WT_64BIT = 1;
export const WT_LEN = 2;
export const WT_32BIT = 5;

/** cmd_type enum (yuanbao_proto.py:CMD_TYPE). */
export const CMD_TYPE = {
	Request: 0,
	Response: 1,
	Push: 2,
	PushAck: 3,
} as const;
/** Built-in commands (yuanbao_proto.py:CMD). */
export const CMD = {
	AuthBind: "auth-bind",
	Ping: "ping",
	Kickout: "kickout",
	UpdateMeta: "update-meta",
} as const;
/** Built-in modules (yuanbao_proto.py:MODULE). */
export const MODULE = { ConnAccess: "conn_access" } as const;
/** Biz service short name (yuanbao_proto.py:_BIZ_PKG). */
export const BIZ_PKG = "yuanbao_openclaw_proxy";
/** openclaw instance id (yuanbao_proto.py:HERMES_INSTANCE_ID). */
export const HERMES_INSTANCE_ID = 17;
/** Reply-heartbeat states (yuanbao_proto.py:WS_HEARTBEAT_*). */
export const WS_HEARTBEAT_RUNNING = 1;
export const WS_HEARTBEAT_FINISH = 2;

// ── varint ───────────────────────────────────────────────────────────────────

const TWO_POW_64 = 18446744073709551616n;

export function encodeVarint(value: number | bigint): Uint8Array {
	let v = typeof value === "bigint" ? value : BigInt(Math.trunc(value));
	if (v < 0n) v += TWO_POW_64; // two's complement, 64-bit
	const out: number[] = [];
	for (;;) {
		const bits = Number(v & 0x7fn);
		v >>= 7n;
		if (v !== 0n) out.push(bits | 0x80);
		else {
			out.push(bits);
			break;
		}
	}
	return Uint8Array.from(out);
}

export function decodeVarint(
	data: Uint8Array,
	pos: number,
): [value: number, pos: number] {
	let result = 0n;
	let shift = 0n;
	while (pos < data.length) {
		const b = data[pos]!;
		pos += 1;
		result |= BigInt(b & 0x7f) << shift;
		shift += 7n;
		if ((b & 0x80) === 0) break;
		if (shift >= 64n) throw new Error("varint too long");
	}
	return [Number(result), pos];
}

// ── field primitives ────────────────────────────────────────────────────────

function encTag(fieldNumber: number, wireType: number): Uint8Array {
	return encodeVarint((fieldNumber << 3) | wireType);
}

export function concat(chunks: Uint8Array[]): Uint8Array {
	const total = chunks.reduce((n, c) => n + c.length, 0);
	const out = new Uint8Array(total);
	let off = 0;
	for (const c of chunks) {
		out.set(c, off);
		off += c.length;
	}
	return out;
}

function encField(
	fieldNumber: number,
	wireType: number,
	value: Uint8Array,
): Uint8Array {
	return concat([encTag(fieldNumber, wireType), value]);
}

function encString(s: string): Uint8Array {
	const bytes = Buffer.from(s, "utf8");
	return concat([encodeVarint(bytes.length), bytes]);
}

function encBytes(b: Uint8Array): Uint8Array {
	return concat([encodeVarint(b.length), b]);
}

export interface PbField {
	fieldNumber: number;
	wireType: number;
	value: Uint8Array | number;
}

export function parseFields(data: Uint8Array): PbField[] {
	const fields: PbField[] = [];
	let pos = 0;
	while (pos < data.length) {
		const [tag, p1] = decodeVarint(data, pos);
		pos = p1;
		const fieldNumber = tag >> 3;
		const wireType = tag & 0x07;
		if (wireType === WT_VARINT) {
			const [val, p2] = decodeVarint(data, pos);
			pos = p2;
			fields.push({ fieldNumber, wireType, value: val });
		} else if (wireType === WT_LEN) {
			const [len, p2] = decodeVarint(data, pos);
			pos = p2;
			fields.push({
				fieldNumber,
				wireType,
				value: data.subarray(pos, pos + len),
			});
			pos += len;
		} else if (wireType === WT_64BIT) {
			fields.push({
				fieldNumber,
				wireType,
				value: data.subarray(pos, pos + 8),
			});
			pos += 8;
		} else if (wireType === WT_32BIT) {
			fields.push({
				fieldNumber,
				wireType,
				value: data.subarray(pos, pos + 4),
			});
			pos += 4;
		} else {
			throw new Error(`unknown wire type ${wireType} at pos ${pos - 1}`);
		}
	}
	return fields;
}

interface FieldDict {
	get(
		fn: number,
	): Array<{ wireType: number; value: Uint8Array | number }> | undefined;
}

function fieldsToDict(fields: PbField[]): FieldDict {
	const d = new Map<
		number,
		Array<{ wireType: number; value: Uint8Array | number }>
	>();
	for (const f of fields) {
		const list = d.get(f.fieldNumber) ?? [];
		list.push({ wireType: f.wireType, value: f.value });
		d.set(f.fieldNumber, list);
	}
	return { get: (fn) => d.get(fn) };
}

function getString(d: FieldDict, fn: number, fallback = ""): string {
	const first = d.get(fn)?.[0];
	if (first === undefined) return fallback;
	if (first.wireType === WT_LEN && first.value instanceof Uint8Array) {
		return Buffer.from(first.value).toString("utf8");
	}
	return fallback;
}

function getVarint(d: FieldDict, fn: number, fallback = 0): number {
	const first = d.get(fn)?.[0];
	if (first === undefined) return fallback;
	if (first.wireType === WT_VARINT && typeof first.value === "number")
		return first.value;
	return fallback;
}

function getBytes(d: FieldDict, fn: number): Uint8Array {
	const first = d.get(fn)?.[0];
	if (
		first !== undefined &&
		first.wireType === WT_LEN &&
		first.value instanceof Uint8Array
	) {
		return first.value;
	}
	return new Uint8Array(0);
}

function getRepeatedBytes(d: FieldDict, fn: number): Uint8Array[] {
	return (d.get(fn) ?? [])
		.filter((e) => e.wireType === WT_LEN && e.value instanceof Uint8Array)
		.map((e) => e.value as Uint8Array);
}

// ── Head / ConnMsg ──────────────────────────────────────────────────────────

export interface Head {
	cmd_type: number;
	cmd: string;
	seq_no: number;
	msg_id: string;
	module: string;
	need_ack: boolean;
	status: number;
}

export function encodeHead(
	cmdType: number,
	cmd: string,
	seqNo: number,
	msgId: string,
	module: string,
	needAck = false,
	status = 0,
): Uint8Array {
	const parts: Uint8Array[] = [];
	if (cmdType !== 0) parts.push(encField(1, WT_VARINT, encodeVarint(cmdType)));
	if (cmd !== "") parts.push(encField(2, WT_LEN, encString(cmd)));
	if (seqNo !== 0) parts.push(encField(3, WT_VARINT, encodeVarint(seqNo)));
	if (msgId !== "") parts.push(encField(4, WT_LEN, encString(msgId)));
	if (module !== "") parts.push(encField(5, WT_LEN, encString(module)));
	if (needAck) parts.push(encField(6, WT_VARINT, encodeVarint(1)));
	if (status !== 0) parts.push(encField(10, WT_VARINT, encodeVarint(status)));
	return concat(parts);
}

export function decodeHead(data: Uint8Array): Head {
	const d = fieldsToDict(parseFields(data));
	return {
		cmd_type: getVarint(d, 1, 0),
		cmd: getString(d, 2, ""),
		seq_no: getVarint(d, 3, 0),
		msg_id: getString(d, 4, ""),
		module: getString(d, 5, ""),
		need_ack: getVarint(d, 6, 0) !== 0,
		status: getVarint(d, 10, 0),
	};
}

export interface ConnMsg {
	msg_type: number;
	seq_no: number;
	data: Uint8Array;
	head: Head;
}

export function encodeConnMsgFull(
	cmdType: number,
	cmd: string,
	seqNo: number,
	msgId: string,
	module: string,
	data: Uint8Array,
	needAck = false,
): Uint8Array {
	const headBytes = encodeHead(cmdType, cmd, seqNo, msgId, module, needAck);
	const parts = [encField(1, WT_LEN, encBytes(headBytes))];
	if (data.length > 0) parts.push(encField(2, WT_LEN, encBytes(data)));
	return concat(parts);
}

export function decodeConnMsg(data: Uint8Array): ConnMsg {
	const d = fieldsToDict(parseFields(data));
	const headBytes = getBytes(d, 1);
	const payload = getBytes(d, 2);
	const head =
		headBytes.length > 0
			? decodeHead(headBytes)
			: {
					cmd_type: 0,
					cmd: "",
					seq_no: 0,
					msg_id: "",
					module: "",
					need_ack: false,
					status: 0,
				};
	return { msg_type: head.cmd_type, seq_no: head.seq_no, data: payload, head };
}

// ── sequence numbers (yuanbao_proto.py:next_seq_no) ─────────────────────────

let seqCounter = 0;
const SEQ_MAX = 2 ** 32 - 1;

export function nextSeqNo(): number {
	const val = seqCounter;
	seqCounter = (seqCounter + 1) & SEQ_MAX;
	return val;
}

/** Test seam: reset the module-level counter. */
export function resetSeqNo(): void {
	seqCounter = 0;
}

// ── biz wrapper ─────────────────────────────────────────────────────────────

export function encodeBizMsg(
	service: string,
	method: string,
	reqId: string,
	body: Uint8Array,
): Uint8Array {
	return encodeConnMsgFull(
		CMD_TYPE.Request,
		method,
		nextSeqNo(),
		reqId,
		service,
		body,
	);
}

export function decodeBizMsg(data: Uint8Array): {
	service: string;
	method: string;
	req_id: string;
	body: Uint8Array;
	is_response: boolean;
	head: Head;
} {
	const result = decodeConnMsg(data);
	return {
		service: result.head.module,
		method: result.head.cmd,
		req_id: result.head.msg_id,
		body: result.data,
		is_response: result.head.cmd_type === CMD_TYPE.Response,
		head: result.head,
	};
}

// ── MsgContent (fields per yuanbao_proto.py comments) ───────────────────────

export type MsgContent = Record<string, unknown>;

const CONTENT_STRINGS: Array<[number, string]> = [
	[1, "text"],
	[2, "uuid"],
	[4, "data"],
	[5, "desc"],
	[6, "ext"],
	[7, "sound"],
	[10, "url"],
	[12, "file_name"],
];
const CONTENT_VARINTS: Array<[number, string]> = [
	[3, "image_format"],
	[9, "index"],
	[11, "file_size"],
];

function encMapEntry(key: string, value: string): Uint8Array {
	const parts: Uint8Array[] = [];
	if (key !== "") parts.push(encField(1, WT_LEN, encString(key)));
	if (value !== "") parts.push(encField(2, WT_LEN, encString(value)));
	return concat(parts);
}

function encodeMsgContent(content: MsgContent): Uint8Array {
	const parts: Uint8Array[] = [];
	for (const [fn, key] of CONTENT_STRINGS) {
		const v = content[key];
		if (typeof v === "string" && v !== "")
			parts.push(encField(fn, WT_LEN, encString(v)));
	}
	for (const [fn, key] of CONTENT_VARINTS) {
		const v = Number(content[key] ?? 0);
		if (v !== 0) parts.push(encField(fn, WT_VARINT, encodeVarint(v)));
	}
	for (const img of (content["image_info_array"] as
		| Array<Record<string, unknown>>
		| undefined) ?? []) {
		const imgParts: Uint8Array[] = [];
		for (const [fn, key] of [
			[1, "type"],
			[2, "size"],
			[3, "width"],
			[4, "height"],
		] as Array<[number, string]>) {
			const iv = Number(img[key] ?? 0);
			if (iv !== 0) imgParts.push(encField(fn, WT_VARINT, encodeVarint(iv)));
		}
		const url = String(img["url"] ?? "");
		if (url !== "") imgParts.push(encField(5, WT_LEN, encString(url)));
		parts.push(encField(8, WT_LEN, encBytes(concat(imgParts))));
	}
	const extMap = content["ext_map"] as Record<string, string> | undefined;
	if (extMap !== undefined && typeof extMap === "object") {
		for (const [k, v] of Object.entries(extMap)) {
			parts.push(encField(999, WT_LEN, encBytes(encMapEntry(k, String(v)))));
		}
	}
	return concat(parts);
}

function decodeMsgContent(data: Uint8Array): MsgContent {
	const d = fieldsToDict(parseFields(data));
	const content: MsgContent = {};
	for (const [fn, key] of CONTENT_STRINGS) content[key] = getString(d, fn, "");
	for (const [fn, key] of CONTENT_VARINTS) content[key] = getVarint(d, fn, 0);
	content["image_info_array"] = getRepeatedBytes(d, 8).map((b) => {
		const img = fieldsToDict(parseFields(b));
		return {
			type: getVarint(img, 1),
			size: getVarint(img, 2),
			width: getVarint(img, 3),
			height: getVarint(img, 4),
			url: getString(img, 5),
		};
	});
	return content;
}

// ── MsgBodyElement ──────────────────────────────────────────────────────────

export interface MsgBodyElement {
	msg_type: string;
	msg_content: MsgContent;
}

export function encodeMsgBodyElement(el: MsgBodyElement): Uint8Array {
	const parts: Uint8Array[] = [];
	if (el.msg_type !== "")
		parts.push(encField(1, WT_LEN, encString(el.msg_type)));
	const contentBytes = encodeMsgContent(el.msg_content ?? {});
	if (Object.keys(el.msg_content ?? {}).length > 0) {
		parts.push(encField(2, WT_LEN, encBytes(contentBytes)));
	}
	return concat(parts);
}

export function decodeMsgBodyElement(data: Uint8Array): MsgBodyElement {
	const d = fieldsToDict(parseFields(data));
	const msgType = getString(d, 1, "");
	const contentBytes = getBytes(d, 2);
	return {
		msg_type: msgType,
		msg_content: contentBytes.length > 0 ? decodeMsgContent(contentBytes) : {},
	};
}

// ── send requests ───────────────────────────────────────────────────────────

export interface SendRequestOpts {
	toAccount: string;
	fromAccount?: string | undefined;
	msgBody: MsgBodyElement[];
	msgId?: string | undefined;
	groupCode?: string | undefined;
	traceId?: string | undefined;
}

export function encodeSendC2CMessage(opts: SendRequestOpts): Uint8Array {
	const { toAccount, fromAccount, msgBody, msgId, groupCode, traceId } = opts;
	const parts: Uint8Array[] = [];
	if (msgId !== undefined && msgId !== "")
		parts.push(encField(1, WT_LEN, encString(msgId)));
	parts.push(encField(2, WT_LEN, encString(toAccount)));
	if (fromAccount !== undefined && fromAccount !== "")
		parts.push(encField(3, WT_LEN, encString(fromAccount)));
	for (const el of msgBody) {
		parts.push(encField(5, WT_LEN, encBytes(encodeMsgBodyElement(el))));
	}
	if (groupCode !== undefined && groupCode !== "")
		parts.push(encField(6, WT_LEN, encString(groupCode)));
	if (traceId !== undefined && traceId !== "")
		parts.push(
			encField(8, WT_LEN, encBytes(encField(1, WT_LEN, encString(traceId)))),
		);
	const reqId =
		msgId !== undefined && msgId !== "" ? msgId : `c2c_${nextSeqNo()}`;
	return encodeConnMsgFull(
		CMD_TYPE.Request,
		"send_c2c_message",
		nextSeqNo(),
		reqId,
		BIZ_PKG,
		concat(parts),
	);
}

export function encodeSendGroupMessage(
	opts: SendRequestOpts & {
		random?: string | undefined;
		refMsgId?: string | undefined;
	},
): Uint8Array {
	const { groupCode, fromAccount, msgBody, msgId, random, refMsgId } = opts;
	const group = opts.groupCode ?? "";
	const parts: Uint8Array[] = [];
	if (msgId !== undefined && msgId !== "")
		parts.push(encField(1, WT_LEN, encString(msgId)));
	parts.push(encField(2, WT_LEN, encString(group)));
	if (fromAccount !== undefined && fromAccount !== "")
		parts.push(encField(3, WT_LEN, encString(fromAccount)));
	if (random !== undefined && random !== "")
		parts.push(encField(5, WT_LEN, encString(random)));
	for (const el of msgBody) {
		parts.push(encField(6, WT_LEN, encBytes(encodeMsgBodyElement(el))));
	}
	if (refMsgId !== undefined && refMsgId !== "")
		parts.push(encField(7, WT_LEN, encString(refMsgId)));
	const reqId =
		msgId !== undefined && msgId !== "" ? msgId : `grp_${nextSeqNo()}`;
	return encodeConnMsgFull(
		CMD_TYPE.Request,
		"send_group_message",
		nextSeqNo(),
		reqId,
		BIZ_PKG,
		concat(parts),
	);
}

/** Fixture-side encoder mirroring decodeInboundPush's field map. */
export function encodeInboundPushFixture(
	push: Record<string, unknown>,
): Uint8Array {
	const parts: Uint8Array[] = [];
	const str = (fn: number, v: unknown): void => {
		const s = String(v ?? "");
		if (s !== "") parts.push(encField(fn, WT_LEN, encString(s)));
	};
	const num = (fn: number, v: unknown): void => {
		const n = Number(v ?? 0);
		if (n !== 0) parts.push(encField(fn, WT_VARINT, encodeVarint(n)));
	};
	str(1, push["callback_command"]);
	str(2, push["from_account"]);
	str(3, push["to_account"]);
	str(4, push["sender_nickname"]);
	str(5, push["group_id"]);
	str(6, push["group_code"]);
	str(7, push["group_name"]);
	num(8, push["msg_seq"]);
	str(11, push["msg_key"]);
	str(12, push["msg_id"]);
	for (const el of (push["msg_body"] as MsgBodyElement[] | undefined) ?? []) {
		parts.push(encField(13, WT_LEN, encBytes(encodeMsgBodyElement(el))));
	}
	str(14, push["cloud_custom_data"]);
	str(16, push["bot_owner_id"]);
	str(19, push["private_from_group_code"]);
	return concat(parts);
}

// ── auth bind / ping / push ack ─────────────────────────────────────────────

export function encodeAuthBind(opts: {
	bizId: string;
	uid: string;
	source: string;
	token: string;
	msgId: string;
	appVersion?: string | undefined;
	operationSystem?: string | undefined;
	botVersion?: string | undefined;
	routeEnv?: string | undefined;
}): Uint8Array {
	const authBuf = concat([
		encField(1, WT_LEN, encString(opts.uid)),
		encField(2, WT_LEN, encString(opts.source)),
		encField(3, WT_LEN, encString(opts.token)),
	]);
	const devParts: Uint8Array[] = [];
	if (opts.appVersion !== undefined && opts.appVersion !== "")
		devParts.push(encField(1, WT_LEN, encString(opts.appVersion)));
	if (opts.operationSystem !== undefined && opts.operationSystem !== "")
		devParts.push(encField(2, WT_LEN, encString(opts.operationSystem)));
	devParts.push(encField(10, WT_LEN, encString(String(HERMES_INSTANCE_ID))));
	if (opts.botVersion !== undefined && opts.botVersion !== "")
		devParts.push(encField(24, WT_LEN, encString(opts.botVersion)));
	const devBuf = concat(devParts);
	let reqBuf = concat([
		encField(1, WT_LEN, encString(opts.bizId)),
		encField(2, WT_LEN, encBytes(authBuf)),
		encField(3, WT_LEN, encBytes(devBuf)),
	]);
	if (opts.routeEnv !== undefined && opts.routeEnv !== "") {
		reqBuf = concat([reqBuf, encField(5, WT_LEN, encString(opts.routeEnv))]);
	}
	return encodeConnMsgFull(
		CMD_TYPE.Request,
		CMD.AuthBind,
		nextSeqNo(),
		opts.msgId,
		MODULE.ConnAccess,
		reqBuf,
	);
}

export function encodePing(msgId: string): Uint8Array {
	return encodeConnMsgFull(
		CMD_TYPE.Request,
		CMD.Ping,
		nextSeqNo(),
		msgId,
		MODULE.ConnAccess,
		new Uint8Array(0),
	);
}

export function encodePushAck(originalHead: Head): Uint8Array {
	return encodeConnMsgFull(
		CMD_TYPE.PushAck,
		originalHead.cmd,
		nextSeqNo(),
		originalHead.msg_id,
		originalHead.module,
		new Uint8Array(0),
	);
}

// ── reply heartbeats ────────────────────────────────────────────────────────

export function encodeSendPrivateHeartbeat(
	fromAccount: string,
	toAccount: string,
	heartbeat = WS_HEARTBEAT_RUNNING,
): Uint8Array {
	const buf = concat([
		encField(1, WT_LEN, encString(fromAccount)),
		encField(2, WT_LEN, encString(toAccount)),
		encField(3, WT_VARINT, encodeVarint(heartbeat)),
	]);
	return encodeBizMsg(
		BIZ_PKG,
		"send_private_heartbeat",
		`hb_priv_${nextSeqNo()}`,
		buf,
	);
}

export function encodeSendGroupHeartbeat(
	fromAccount: string,
	groupCode: string,
	heartbeat = WS_HEARTBEAT_RUNNING,
	sendTimeMs = 0,
): Uint8Array {
	const ts = sendTimeMs !== 0 ? sendTimeMs : Date.now();
	const buf = concat([
		encField(1, WT_LEN, encString(fromAccount)),
		encField(2, WT_LEN, encString("")), // to_account empty for groups
		encField(3, WT_LEN, encString(groupCode)),
		encField(4, WT_VARINT, encodeVarint(ts)),
		encField(5, WT_VARINT, encodeVarint(heartbeat)),
	]);
	return encodeBizMsg(
		BIZ_PKG,
		"send_group_heartbeat",
		`hb_grp_${nextSeqNo()}`,
		buf,
	);
}

// ── get_group_member_list ───────────────────────────────────────────────────

export interface GroupMemberInfo {
	user_id: string;
	nickname: string;
	/** 0 = member, 1 = admin, 2 = owner. */
	role: number;
	join_time: number;
	/** Group card (group nickname). */
	name_card: string;
}

export interface GroupMemberListResult {
	code: number;
	message: string;
	members: GroupMemberInfo[];
	next_offset: number;
	is_complete: boolean;
}

/** GetGroupMemberListReq → ConnMsg bytes
 * (yuanbao_proto.py:encode_get_group_member_list). */
export function encodeGetGroupMemberList(
	groupCode: string,
	offset = 0,
	limit = 200,
): Uint8Array {
	const parts: Uint8Array[] = [encField(1, WT_LEN, encString(groupCode))];
	if (offset !== 0) parts.push(encField(2, WT_VARINT, encodeVarint(offset)));
	parts.push(encField(3, WT_VARINT, encodeVarint(limit)));
	const reqId = `gml_${nextSeqNo()}`;
	return encodeBizMsg(BIZ_PKG, "get_group_member_list", reqId, concat(parts));
}

export function decodeGetGroupMemberListReq(data: Uint8Array): {
	groupCode: string;
	offset: number;
	limit: number;
} {
	const d = fieldsToDict(parseFields(data));
	return {
		groupCode: getString(d, 1),
		offset: getVarint(d, 2),
		limit: getVarint(d, 3, 200),
	};
}

/** GetGroupMemberListRsp biz payload decode; null on parse failure
 * (yuanbao_proto.py:decode_get_group_member_list_rsp). Fields absent on the
 * wire normalize to ""/0 exactly like the reference's per-field defaults. */
export function decodeGetGroupMemberListRsp(
	data: Uint8Array,
): GroupMemberListResult | null {
	try {
		const d = fieldsToDict(parseFields(data));
		const members = getRepeatedBytes(d, 3).map((b) => {
			const md = fieldsToDict(parseFields(b));
			return {
				user_id: getString(md, 1),
				nickname: getString(md, 2),
				role: getVarint(md, 3),
				join_time: getVarint(md, 4),
				name_card: getString(md, 5),
			};
		});
		return {
			code: getVarint(d, 1),
			message: getString(d, 2),
			members,
			next_offset: getVarint(d, 4),
			is_complete: getVarint(d, 5) !== 0,
		};
	} catch {
		return null;
	}
}

/** Fixture-side encoder mirroring decodeGetGroupMemberListRsp's field map:
 * zero-valued/empty members are omitted from the wire exactly as the decoder
 * normalizes them back (byte-faithful round-trip for populated entries). */
export function encodeGetGroupMemberListRspFixture(
	result: GroupMemberListResult,
): Uint8Array {
	const parts: Uint8Array[] = [];
	if (result.code !== 0)
		parts.push(encField(1, WT_VARINT, encodeVarint(result.code)));
	if (result.message !== "")
		parts.push(encField(2, WT_LEN, encString(result.message)));
	for (const m of result.members) {
		const mp: Uint8Array[] = [];
		if (m.user_id !== "") mp.push(encField(1, WT_LEN, encString(m.user_id)));
		if (m.nickname !== "") mp.push(encField(2, WT_LEN, encString(m.nickname)));
		if (m.role !== 0) mp.push(encField(3, WT_VARINT, encodeVarint(m.role)));
		if (m.join_time !== 0)
			mp.push(encField(4, WT_VARINT, encodeVarint(m.join_time)));
		if (m.name_card !== "")
			mp.push(encField(5, WT_LEN, encString(m.name_card)));
		parts.push(encField(3, WT_LEN, encBytes(concat(mp))));
	}
	if (result.next_offset !== 0)
		parts.push(encField(4, WT_VARINT, encodeVarint(result.next_offset)));
	if (result.is_complete) parts.push(encField(5, WT_VARINT, encodeVarint(1)));
	return concat(parts);
}

// ── inbound push decode ─────────────────────────────────────────────────────

export interface DecodedPush {
	callback_command: string;
	from_account: string;
	to_account: string;
	sender_nickname: string;
	group_id: string;
	group_code: string;
	group_name: string;
	msg_seq: number;
	msg_key: string;
	msg_id: string;
	msg_body: MsgBodyElement[];
	cloud_custom_data: string;
	bot_owner_id: string;
	trace_id: string;
	recall_msg_seq_list: Array<{ msg_seq: number; msg_id: string }> | null;
}

export function decodeInboundPush(data: Uint8Array): DecodedPush | null {
	try {
		const d = fieldsToDict(parseFields(data));
		const msgBody = getRepeatedBytes(d, 13).map(decodeMsgBodyElement);
		const logExt = getBytes(d, 20);
		const traceId =
			logExt.length > 0
				? getString(fieldsToDict(parseFields(logExt)), 1, "")
				: "";
		const recallList = getRepeatedBytes(d, 17).map((b) => {
			const dd = fieldsToDict(parseFields(b));
			return { msg_seq: getVarint(dd, 1), msg_id: getString(dd, 2) };
		});
		return {
			callback_command: getString(d, 1),
			from_account: getString(d, 2),
			to_account: getString(d, 3),
			sender_nickname: getString(d, 4),
			group_id: getString(d, 5),
			group_code: getString(d, 6),
			group_name: getString(d, 7),
			msg_seq: getVarint(d, 8),
			msg_key: getString(d, 11),
			msg_id: getString(d, 12),
			msg_body: msgBody.map((el) => ({
				msg_type: el.msg_type,
				msg_content: el.msg_content,
			})),
			cloud_custom_data: getString(d, 14),
			bot_owner_id: getString(d, 16),
			trace_id: traceId,
			recall_msg_seq_list: recallList.length > 0 ? recallList : null,
		};
	} catch {
		return null;
	}
}
