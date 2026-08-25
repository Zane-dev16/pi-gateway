// pi_platforms/feishu/comment-ingress — A12 Drive-comment ingestion
// (feishu_comment.py semantics; gap-audit A12 third leg).
//
// Pipeline (feishu_comment.py:handle_drive_comment_event :1120):
//   parse → filters (self / addressed-to-bot / notice_type / required fields)
//   → access rules (3-tier + pairing) — denial = SILENT skip, no reaction
//   → OK reaction on replies (:153 add_comment_reaction)
//   → parallel fetch doc meta + comment timeline (eventual-consistency retry ×6)
//   → prompt build (timeline caps 20 local / 12 whole; text limit 220/500)
//   → agent turn (HERE: the gateway guard pipeline — see PROPOSED DEC-046)
//   → deliver reply as thread-reply comments (chunk 4000, HTML-escaped;
//     error 1069302 ⇒ whole-comment fallback for that+subsequent chunks)
//   → cleanup (delete OK reaction, best-effort).

import {
	isUserAllowed,
	resolveRule,
	type FeishuCommentRulesStore,
} from "./comment-rules.js";

export const COMMENT_RETRY_LIMIT = 6; // feishu_comment.py:300
export const REPLY_CHUNK_SIZE = 4000; // :533 _REPLY_CHUNK_SIZE
const PROMPT_TEXT_LIMIT = 220; // :790
const QUOTE_TEXT_LIMIT = 500; // inline in build_local_comment_prompt
const LOCAL_TIMELINE_LIMIT = 20; // :791
const WHOLE_TIMELINE_LIMIT = 12; // :792
export const NO_REPLY_SENTINEL = "NO_REPLY"; // :1114
/** Reply-not-allowed code ⇒ whole-comment downgrade (:554). */
export const REPLY_NOT_ALLOWED_CODE = 1069302;

export interface DriveCommentEvent {
	eventId: string;
	commentId: string;
	replyId: string;
	isMentioned: boolean;
	fileToken: string;
	fileType: string;
	noticeType: string;
	fromOpenId: string;
	toOpenId: string;
}

/**
 * Parse (:103 parse_drive_comment_event): the customized event body flattens
 * notice_meta.{file_token,file_type,notice_type,from_user_id,to_user_id}.
 * Returns null when the body lacks the event payload entirely.
 */
export function parseDriveCommentEvent(
	event: Record<string, unknown> | undefined,
): DriveCommentEvent | null {
	if (event === undefined || event === null) return null;
	const meta = (event["notice_meta"] ?? {}) as Record<string, unknown>;
	const from = (meta["from_user_id"] ?? {}) as Record<string, unknown>;
	const to = (meta["to_user_id"] ?? {}) as Record<string, unknown>;
	return {
		eventId: String(event["event_id"] ?? ""),
		commentId: String(event["comment_id"] ?? ""),
		replyId: String(event["reply_id"] ?? ""),
		isMentioned: event["is_mentioned"] === true,
		fileToken: String(meta["file_token"] ?? ""),
		fileType: String(meta["file_type"] ?? ""),
		noticeType: String(meta["notice_type"] ?? ""),
		fromOpenId: String(from["open_id"] ?? ""),
		toOpenId: String(to["open_id"] ?? ""),
	};
}

const ALLOWED_NOTICE_TYPES = new Set(["add_comment", "add_reply"]);

/**
 * Filter chain order (:1140–1157). Returns a reason string on drop (test
 * observability), null when admitted.
 */
export function filterDriveCommentEvent(
	evt: DriveCommentEvent | null,
	selfOpenId: string,
): string | null {
	if (evt === null) return "malformed";
	if (selfOpenId && evt.fromOpenId === selfOpenId) return "self_authored";
	if (!evt.toOpenId || (selfOpenId && evt.toOpenId !== selfOpenId))
		return "not_addressed_to_bot";
	if (evt.noticeType && !ALLOWED_NOTICE_TYPES.has(evt.noticeType))
		return "notice_type_not_allowed";
	if (!evt.fileToken || !evt.fileType || !evt.commentId)
		return "missing_required_fields";
	return null;
}

/** The scripted comment-API surface the fake server exposes. */
export interface CommentApi {
	/** GET doc meta batch_query → {title,url} or undefined on failure. */
	docMeta(
		fileToken: string,
		fileType: string,
	): { title: string; url: string } | undefined;
	/**
	 * batch_query_comment with eventual-consistency retry (×6 @1s injected);
	 * resolves to the comment detail or undefined after exhaustion.
	 */
	batchQueryComment(
		fileToken: string,
		fileType: string,
		commentId: string,
	): CommentDetail | undefined;
	listWholeComments(
		fileToken: string,
		fileType: string,
	): WholeCommentTimeline[];
	listCommentReplies(
		fileToken: string,
		fileType: string,
		commentId: string,
	): ReplyEntry[];
	addReaction(replyId: string, fileToken: string, fileType: string): boolean;
	deleteReaction(replyId: string, fileToken: string, fileType: string): boolean;
	postThreadReply(
		fileToken: string,
		fileType: string,
		commentId: string,
		text: string,
	): { ok: true } | { ok: false; code: number };
	postNewComment(
		fileToken: string,
		fileType: string,
		text: string,
	): { ok: true } | { ok: false; code: number };
}

export interface ReplyEntry {
	openId: string;
	text: string;
	replyId?: string | undefined;
}

export interface CommentDetail {
	isWhole: boolean;
	quote?: string | undefined;
	replies: ReplyEntry[];
}
export interface WholeCommentTimeline {
	commentId: string;
	replies: ReplyEntry[];
}

/** HTML-entity escape before posting (:465 _sanitize_comment_text). */
export function sanitizeCommentText(text: string): string {
	return text
		.replaceAll("&", "&amp;")
		.replaceAll("<", "&lt;")
		.replaceAll(">", "&gt;");
}

/** Chunk at 4000 chars preferring the last newline within window (:536). */
export function chunkReplyText(text: string): string[] {
	if (text.length <= REPLY_CHUNK_SIZE) return [text];
	const chunks: string[] = [];
	let rest = text;
	while (rest.length > REPLY_CHUNK_SIZE) {
		const windowText = rest.slice(0, REPLY_CHUNK_SIZE);
		const nl = windowText.lastIndexOf("\n");
		const cut = nl > 0 ? nl + 1 : REPLY_CHUNK_SIZE;
		chunks.push(rest.slice(0, cut));
		rest = rest.slice(cut);
	}
	if (rest.length > 0) chunks.push(rest);
	return chunks;
}

function truncate(text: string, limit: number): string {
	return text.length > limit ? `${text.slice(0, limit)}…` : text;
}

/** Timeline selection (:802/:833): ≤limit passes; else keep first/target/last
 * then expand outward from target alternating lo/hi. */
export function selectTimeline(
	timeline: ReplyEntry[],
	limit: number,
	targetIdx: number,
): ReplyEntry[] {
	if (timeline.length <= limit) return timeline;
	const keep = new Set<number>([0, targetIdx, timeline.length - 1]);
	let lo = targetIdx - 1;
	let hi = targetIdx + 1;
	while (keep.size < limit && (lo >= 0 || hi < timeline.length)) {
		if (hi < timeline.length) keep.add(hi++);
		if (keep.size >= limit) break;
		if (lo >= 0) keep.add(lo--);
	}
	return [...keep].sort((a, b) => a - b).map((i) => timeline[i] as ReplyEntry);
}

const COMMON_INSTRUCTIONS = [
	"Answer the question or request in the comment directly.",
	"Use plain text only; never call comment tools yourself.",
	"No reasoning preamble; answer in the same language as the comment.",
	`If no reply is needed, output exactly ${NO_REPLY_SENTINEL}.`,
].join("\n");

/** Local (threaded) prompt (:884 build_local_comment_prompt). */
export function buildLocalCommentPrompt(opts: {
	docTitle: string;
	docUrl: string;
	fileType: string;
	fileToken: string;
	commentId: string;
	quote: string;
	rootText: string;
	targetText: string;
	timeline: ReplyEntry[];
}): string {
	const lines = [
		`You are replying to a comment thread on the document "${opts.docTitle}".`,
		`Document URL: ${opts.docUrl}`,
		`File type: ${opts.fileType}  File token: ${opts.fileToken}`,
		`Comment ID: ${opts.commentId}`,
		`Quoted text: ${truncate(opts.quote, QUOTE_TEXT_LIMIT)}`,
		"Conversation so far:",
		...opts.timeline.map(
			(r) => `[${r.openId}] ${truncate(r.text, PROMPT_TEXT_LIMIT)}`,
		),
		`The comment you must answer: [${opts.timeline[opts.timeline.length - 1]?.openId ?? ""}] ${truncate(opts.targetText, PROMPT_TEXT_LIMIT)}`,
		COMMON_INSTRUCTIONS,
	];
	return lines.join("\n");
}

/** Whole-document prompt (:929 build_whole_comment_prompt). */
export function buildWholeCommentPrompt(opts: {
	docTitle: string;
	docUrl: string;
	fileType: string;
	fileToken: string;
	timeline: ReplyEntry[];
	currentText: string;
}): string {
	const lines = [
		`You are replying as a top-level comment on the document "${opts.docTitle}".`,
		`Document URL: ${opts.docUrl}`,
		`File type: ${opts.fileType}  File token: ${opts.fileToken}`,
		"Comment timeline:",
		...opts.timeline.map(
			(r) => `[${r.openId}] ${truncate(r.text, PROMPT_TEXT_LIMIT)}`,
		),
		`Latest comment to answer: ${truncate(opts.currentText, PROMPT_TEXT_LIMIT)}`,
		COMMON_INSTRUCTIONS,
	];
	return lines.join("\n");
}

export interface CommentIngressDeps {
	rulesStore: FeishuCommentRulesStore;
	api: CommentApi;
	selfOpenId: string;
	/**
	 * The agent leg: Hermes runs a LOCAL sub-agent (AIAgent w/ feishu_doc/
	 * drive toolsets); THIS gateway routes the prompt through the normal
	 * guard pipeline instead (PROPOSED DEC-046 — DEC-001 deviation space).
	 * Returning "" means "no delivery" (agent failure parity :1047).
	 */
	runTurn: (prompt: string) => Promise<string>;
	nowMs?: () => number;
	sleepMs?: ((ms: number) => Promise<void>) | undefined;
}

export interface CommentIngressResult {
	droppedReason: string | null;
	deniedByRules: boolean;
	promptBuilt: boolean;
	prompt: string;
	isWhole: boolean;
	deliveredChunks: number;
	fellBackToWholeComment: boolean;
	cleanedUp: boolean;
}

/**
 * THE handler (:1120 handle_drive_comment_event). Every outcome is recorded —
 * denials are SILENT toward the commenter but observable here.
 */
export async function handleDriveCommentEvent(
	raw: Record<string, unknown> | undefined,
	deps: CommentIngressDeps,
): Promise<CommentIngressResult> {
	const out: CommentIngressResult = {
		droppedReason: filterDriveCommentEvent(
			parseDriveCommentEvent(raw),
			deps.selfOpenId,
		),
		deniedByRules: false,
		promptBuilt: false,
		prompt: "",
		isWhole: false,
		deliveredChunks: 0,
		fellBackToWholeComment: false,
		cleanedUp: false,
	};
	if (out.droppedReason !== null) return out;

	const evt = parseDriveCommentEvent(raw) as DriveCommentEvent;

	// ── access rules (3-tier + pairing) — deny ⇒ silent return BEFORE any
	// reaction/fetch/turn (:1146 admission ordering).
	const cfg = deps.rulesStore.loadConfig();
	const rule = resolveRule(cfg, evt.fileType, evt.fileToken);
	const approved = deps.rulesStore.loadPairingApproved();
	if (rule.enabled === false) {
		out.droppedReason = "doc_disabled";
		return out;
	}
	if (!isUserAllowed(rule, evt.fromOpenId, approved)) {
		out.deniedByRules = true;
		out.droppedReason = "access_denied";
		return out;
	}

	// ── OK reaction on reply events (fire-and-forget parity :1128) —
	// synchronous best-effort here; failure never blocks flow.
	let reacted = false;
	if (evt.replyId) {
		reacted = deps.api.addReaction(evt.replyId, evt.fileToken, evt.fileType);
	}

	try {
		// ── parallel fetch (gather parity): meta + eventual-consistency reads.
		const meta = deps.api.docMeta(evt.fileToken, evt.fileType) ?? {
			title: "Untitled",
			url: "",
		};
		let detail: CommentDetail | undefined;
		for (let attempt = 0; attempt < COMMENT_RETRY_LIMIT; attempt++) {
			detail = deps.api.batchQueryComment(
				evt.fileToken,
				evt.fileType,
				evt.commentId,
			);
			if (detail !== undefined) break;
			await deps.sleepMs?.(1);
		}
		const isWhole = detail?.isWhole === true;
		out.isWhole = isWhole;

		let prompt: string;
		let timeline: ReplyEntry[] = [];
		if (isWhole) {
			const whole = deps.api.listWholeComments(evt.fileToken, evt.fileType);
			// Flatten each whole comment's reply_list into ONE timeline
			// (:355 list_whole_comments flattening parity).
			const flat: ReplyEntry[] = whole.flatMap((c) => c.replies);
			timeline = selectTimeline(
				flat,
				WHOLE_TIMELINE_LIMIT,
				Math.max(0, flat.length - 1),
			);
			const current =
				detail?.replies.at(-1) ?? timeline.at(-1);
			prompt = buildWholeCommentPrompt({
				docTitle: meta.title,
				docUrl: meta.url,
				fileType: evt.fileType,
				fileToken: evt.fileToken,
				timeline,
				currentText: current?.text ?? "",
			});
		} else {
			// Eventual-consistency: replies listing retries until the expected
			// reply id appears (or the budget is exhausted).
			let replies: ReplyEntry[] = [];
			for (let attempt = 0; attempt < COMMENT_RETRY_LIMIT; attempt++) {
				replies = deps.api.listCommentReplies(
					evt.fileToken,
					evt.fileType,
					evt.commentId,
				);
				if (replies.some((r) => r.replyId === evt.replyId)) break;
				await deps.sleepMs?.(1);
			}
			const targetIdx = replies.findIndex((r) => r.replyId === evt.replyId);
			const fallbackIdx = replies.length - 1;
			const idx = targetIdx >= 0 ? targetIdx : fallbackIdx;
			const target = idx >= 0 ? replies[idx] : undefined;
			const selected = selectTimeline(
				replies,
				LOCAL_TIMELINE_LIMIT,
				Math.max(0, idx),
			);
			prompt = buildLocalCommentPrompt({
				docTitle: meta.title,
				docUrl: meta.url,
				fileType: evt.fileType,
				fileToken: evt.fileToken,
				commentId: evt.commentId,
				quote: detail?.quote ?? "",
				rootText: replies[0]?.text ?? "",
				targetText: target?.text ?? "",
				timeline: selected,
			});
		}
		out.prompt = prompt;
		out.promptBuilt = true;

		// ── agent leg through the gateway pipeline (PROPOSED DEC-046).
		const response = await deps.runTurn(prompt);
		if (response === "" || response.includes(NO_REPLY_SENTINEL)) {
			return out; // delivery skipped; cleanup still runs
		}

		// ── deliver (chunked; 1069302 ⇒ whole-comment fallback sticks).
		let useWhole = isWhole;
		for (const chunk of chunkReplyText(response)) {
			const safe = sanitizeCommentText(chunk);
			const posted = useWhole
				? deps.api.postNewComment(evt.fileToken, evt.fileType, safe)
				: deps.api.postThreadReply(
						evt.fileToken,
						evt.fileType,
						evt.commentId,
						safe,
					);
			if (posted.ok) {
				out.deliveredChunks += 1;
				continue;
			}
			if (!useWhole && posted.code === REPLY_NOT_ALLOWED_CODE) {
				useWhole = true; // downgrade applies to this AND subsequent chunks
				out.fellBackToWholeComment = true;
				const retried = deps.api.postNewComment(
					evt.fileToken,
					evt.fileType,
					safe,
				);
				if (retried.ok) out.deliveredChunks += 1;
				continue;
			}
			break; // first failed chunk aborts the rest (:554)
		}
	} finally {
		// ── cleanup: delete the OK reaction, best-effort (:1112 cleanup).
		if (reacted && evt.replyId) {
			out.cleanedUp = deps.api.deleteReaction(
				evt.replyId,
				evt.fileToken,
				evt.fileType,
			);
		}
	}
	return out;
}
