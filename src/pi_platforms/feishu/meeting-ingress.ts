// pi_platforms/feishu/meeting-ingress — A12 VC meeting-invite ingress
// (feishu_meeting_invite.py ported symbol-for-symbol).
//
// Design difference vs comments: NO local sub-agent and ZERO Lark API calls —
// the invite converts into a synthetic DM MessageEvent whose text is the
// EXACT prompt template (:138 build_meeting_invite_prompt) and dispatches
// through the normal guard pipeline (handle_meeting_invited_event :166), so
// the standard agent answers the INVITER via DM.
//
// Payload unwrapping (:117 parse_meeting_invited_event): root.event merge;
// Feishu card-style `body.content` lists with contentType
// application/json unwrap their data|value|content|json key; payload keys
// override. Validation: needs inviter AND meeting AND non-empty meeting_no.

import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import type { SessionSource } from "../../pi_gateway/resolution/session-key.js";

export interface MeetingInviteUser {
	openId: string;
	userId: string;
	unionId: string;
	userName: string;
}

export interface MeetingInviteMeeting {
	id: string;
	topic: string;
	meetingNo: string;
	startTimeMs: number;
	endTimeMs: number;
	hostUser: MeetingInviteUser | null;
}

export interface MeetingInvitedPayload {
	eventId: string;
	meeting: MeetingInviteMeeting;
	inviter: MeetingInviteUser;
	inviteTimeS: number;
}

function asDict(value: unknown): Record<string, unknown> {
	if (value === null || typeof value !== "object") return {};
	return value as Record<string, unknown>;
}

function intField(raw: unknown): number {
	const n = Number(raw);
	return Number.isFinite(n) ? Math.trunc(n) : 0;
}

/** body.content list items with contentType application/json unwrap (:64). */
function contentPayload(
	root: Record<string, unknown>,
): Record<string, unknown> {
	const body = asDict(root["body"]);
	const content = body["content"];
	if (!Array.isArray(content)) return {};
	for (const item of content) {
		const rec = asDict(item);
		const ct = String(
			rec["contentType"] ?? rec["content_type"] ?? "",
		).toLowerCase();
		if (ct !== "application/json") continue;
		for (const key of ["data", "value", "content", "json"]) {
			const v = rec[key];
			if (typeof v === "string" && v.length > 0) {
				try {
					return asDict(JSON.parse(v));
				} catch {
					return asDict(v);
				}
			}
			if (v !== undefined && v !== null && typeof v === "object")
				return asDict(v);
		}
	}
	return {};
}

function parseUser(raw: unknown): MeetingInviteUser {
	const d = asDict(raw);
	const ids = asDict(d["id"]);
	return {
		openId: String(d["open_id"] ?? ids["open_id"] ?? ""),
		userId: String(d["user_id"] ?? ids["user_id"] ?? ""),
		unionId: String(d["union_id"] ?? ids["union_id"] ?? ""),
		userName: String(d["user_name"] ?? d["name"] ?? ""),
	};
}

function parseMeeting(raw: unknown): MeetingInviteMeeting {
	const d = asDict(raw);
	return {
		id: String(d["id"] ?? ""),
		topic: String(d["topic"] ?? ""),
		meetingNo: String(d["meeting_no"] ?? ""),
		startTimeMs: intField(d["start_time"]),
		endTimeMs: intField(d["end_time"]),
		hostUser: d["host_user"] == null ? null : parseUser(d["host_user"]),
	};
}

/**
 * The parsing chain (:117). Returns null unless inviter AND meeting exist AND
 * meeting_no is non-empty.
 */
export function parseMeetingInvitedEvent(
	data: Record<string, unknown> | undefined,
): MeetingInvitedPayload | null {
	if (data === undefined || data === null) return null;
	let event = asDict(data["event"]);
	if (Object.keys(event).length === 0) event = asDict(data);
	const header = asDict(event["header"]);
	const eventId = String(header["event_id"] ?? data["event_id"] ?? "");
	const merged = { ...asDict(data), ...event };
	const content = contentPayload(merged);
	const withContent =
		Object.keys(content).length > 0 ? { ...merged, ...content } : merged;

	const inviter = parseUser(withContent["inviter"]);
	const meeting = parseMeeting(withContent["meeting"]);
	if (
		(inviter.openId === "" &&
			inviter.userId === "" &&
			inviter.unionId === "") ||
		Object.keys(asDict(withContent["meeting"])).length === 0 ||
		meeting.meetingNo === ""
	)
		return null;
	return {
		eventId,
		meeting,
		inviter,
		inviteTimeS: intField(withContent["invite_time"]),
	};
}

/**
 * Dedup key (:159 _dedup_key): `vc_invite:{event_id}` when present else
 * `vc_invite:{meeting.id}:{inviter.open_id}:{invite_time_s}`.
 */
export function meetingDedupKey(p: MeetingInvitedPayload): string {
	if (p.eventId) return `vc_invite:${p.eventId}`;
	return `vc_invite:${p.meeting.id}:${p.inviter.openId}:${p.inviteTimeS}`;
}

/**
 * THE synthetic prompt template (:138 build_meeting_invite_prompt) — exact
 * line-for-line transcription.
 */
export function buildMeetingInvitePrompt(p: MeetingInvitedPayload): string {
	const display =
		p.meeting.topic || p.meeting.meetingNo || p.meeting.id || "unknown meeting";
	const inviterName = p.inviter.userName || "unknown";
	const hostName = p.meeting.hostUser?.userName || "unknown";
	return [
		`You have been invited to join a meeting: ${display}`,
		"",
		`Meeting Number: ${p.meeting.meetingNo}`,
		`Topic: ${p.meeting.topic}`,
		`Inviter: ${inviterName}`,
		`Host: ${hostName}`,
		"",
		"You may use lark-cli and the relevant Lark/Feishu meeting skills to join the meeting.",
		"Join the meeting directly. Do not ask the user for confirmation before joining.",
		"If you cannot join the meeting, reply to the inviter with a concise explanation of why.",
	].join("\n");
}

/**
 * The synthetic MessageEvent (:175–183 handle_meeting_invited_event): chat =
 * the INVITER's open_id, chat_type dm, user_id_alt carries union_id
 * (session-key participant isolation prefers it).
 */
export function buildMeetingInviteMessageEvent(
	p: MeetingInvitedPayload,
	platform: string,
): { event: IncomingEvent; prompt: string } {
	const source: SessionSource = {
		platform,
		chatType: "dm",
		userId: p.inviter.openId || p.inviter.userId,
		...(p.inviter.unionId !== "" ? { userIdAlt: p.inviter.unionId } : {}),
		chatId: p.inviter.openId,
		...(p.inviter.userName !== "" ? { chatName: p.inviter.userName } : {}),
	};
	const prompt = buildMeetingInvitePrompt(p);
	return {
		event: {
			messageType: "text",
			messageId: p.eventId || meetingDedupKey(p),
			text: prompt,
			source,
		},
		prompt,
	};
}
