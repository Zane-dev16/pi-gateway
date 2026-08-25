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
	hasWikiKeys,
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

// ── vendor request shapes (feishu_comment.py:_exec_request coordinates) ──

/** One `queries=[(name, value)]` pair on the vendor wire. */
export interface CommentQueryPair {
	name: string;
	value: string;
}

/** GET/POST list response page (has_more/page_token drive shape :362/:424). */
export interface CommentPage<T> {
	items: T[];
	hasMore: boolean;
	pageToken: string;
}

/** Whole-document comment entry as listed by list_whole_comments (:355). */
export interface WholeCommentItem {
	commentId: string;
	replies: ReplyEntry[];
}

/** The scripted comment-API surface the fake server exposes — every method
 * carries ITS VENDOR REQUEST COORDINATES (uri/paths/queries/body) so fakes
 * record and tests assert the exact wire shape (feishu_comment.py anchors).
 */
export interface CommentApi {
	/**
	 * POST /drive/v1/metas/batch_query — body {request_docs:
	 * [{doc_token,doc_type}],with_url:true} (:259/:267/:271); returns
	 * {title,url} or undefined on failure.
	 */
	docMeta(req: {
		fileToken: string;
		fileType: string;
	}): { title: string; url: string } | undefined;
	/**
	 * POST /drive/v1/files/:file_token/comments/batch_query — queries
	 * file_type + user_id_type=open_id, body {comment_ids:[id]} (:300/:318);
	 * eventual-consistency retry ×6 is the CALLER's loop (:300).
	 */
	batchQueryComment(req: {
		fileToken: string;
		fileType: string;
		userIdType: "open_id";
		commentIds: readonly [string];
	}): { items: CommentDetail[] } | undefined;
	/**
	 * ONE page of GET /drive/v1/files/:file_token/comments — queries
	 * file_type,is_whole=true,page_size=100,user_id_type=open_id[,page_token]
	 * (:362/:374); the caller walks ≤5 pages while has_more (:365).
	 */
	listWholeCommentsPage(req: {
		fileToken: string;
		fileType: string;
		isWhole: true;
		pageSize: 100;
		userIdType: "open_id";
		pageToken?: string | undefined;
	}): CommentPage<WholeCommentItem>;
	/**
	 * ONE page of GET …/comments/:comment_id/replies — queries file_type,
	 * page_size=100,user_id_type=open_id[,page_token] (:424); ≤5 pages per
	 * attempt in the caller's retry loop (:411).
	 */
	listRepliesPage(req: {
		fileToken: string;
		fileType: string;
		commentId: string;
		pageSize: 100;
		userIdType: "open_id";
		pageToken?: string | undefined;
	}): CommentPage<ReplyEntry>;
	/**
	 * POST /drive/v2/files/:file_token/comments/reaction?file_type — body
	 * {action:"add",reply_id,reaction_type} (:156/:172–177).
	 */
	addReaction(req: {
		fileToken: string;
		fileType: string;
		replyId: string;
		reactionType: string;
	}): boolean;
	/** Same endpoint, body {action:"delete",reply_id,reaction_type} (:206). */
	deleteReaction(req: {
		fileToken: string;
		fileType: string;
		replyId: string;
		reactionType: string;
	}): boolean;
	/**
	 * POST /drive/v1/files/:ft/comments/:cid/replies?file_type — body
	 * {content:{elements:[{type:"text_run",text_run:{text}}]}} (:489/:504).
	 */
	postThreadReply(req: {
		fileToken: string;
		fileType: string;
		commentId: string;
		textRunText: string;
	}): { ok: true } | { ok: false; code: number };
	/**
	 * POST /drive/v1/files/:ft/new_comments — body
	 * {file_type,reply_elements:[{type:"text",text}]} (:511/:524).
	 */
	postNewComment(req: {
		fileToken: string;
		fileType: string;
		replyElementsText: string;
	}): { ok: true } | { ok: false; code: number };
	/**
	 * GET /open-apis/wiki/v2/spaces/get_node?token=<obj_token>&obj_type=<type>
	 * (:711 _reverse_lookup_wiki_token) — the wiki node_token owning this
	 * document, null when the doc is not a wiki node or the call fails.
	 */
	reverseLookupWikiNode(objToken: string, objType: string): string | null;
	/** Forward node resolution (:732 _resolve_wiki_nodes): obj_type+obj_token
	 * behind a wiki token, null on failure. */
	getWikiNode(wikiToken: string): {
		objType: string;
		objToken: string;
	} | null;
}

export interface ReplyEntry {
	openId: string;
	text: string;
	replyId?: string | undefined;
}

/** Timeline tuple (user_id, text, is_self) (:812/:840 signatures). */
export interface TimelineEntry {
	openId: string;
	text: string;
	isSelf: boolean;
}

function toTimeline(
	replies: ReplyEntry[],
	selfOpenId: string,
): TimelineEntry[] {
	return replies.map((r) => ({
		openId: r.openId,
		text: r.text,
		isSelf: selfOpenId !== "" && r.openId === selfOpenId,
	}));
}

export interface CommentDetail {
	isWhole: boolean;
	quote?: string | undefined;
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

/** _truncate (:795): plain "..." continuation — never a unicode ellipsis. */
function truncate(text: string, limit = PROMPT_TEXT_LIMIT): string {
	return text.length > limit ? `${text.slice(0, limit)}...` : text;
}

/** Max pages per listing walk (feishu_comment.py:365/:428 "max 5 pages"). */
const COMMENT_LIST_MAX_PAGES = 5;

/**
 * list_whole_comments client walk (:355): ≤5 pages of 100 whole comments,
 * page_token chained while has_more; a failed/blank token ends the walk.
 */
function listWholeCommentsPaged(
	api: CommentApi,
	fileToken: string,
	fileType: string,
): WholeCommentItem[] {
	const all: WholeCommentItem[] = [];
	let pageToken: string | undefined;
	for (let page = 0; page < COMMENT_LIST_MAX_PAGES; page++) {
		const res = api.listWholeCommentsPage({
			fileToken,
			fileType,
			isWhole: true,
			pageSize: 100,
			userIdType: "open_id",
			...(pageToken !== undefined ? { pageToken } : {}),
		});
		all.push(...res.items);
		if (!res.hasMore) break;
		if (res.pageToken === "") break;
		pageToken = res.pageToken;
	}
	return all;
}

/**
 * list_comment_replies client walk (:411): ONE attempt = ≤5 pages of 100
 * replies with page_token chaining.
 */
function listRepliesPaged(
	api: CommentApi,
	fileToken: string,
	fileType: string,
	commentId: string,
): ReplyEntry[] {
	const all: ReplyEntry[] = [];
	let pageToken: string | undefined;
	for (let page = 0; page < COMMENT_LIST_MAX_PAGES; page++) {
		const res = api.listRepliesPage({
			fileToken,
			fileType,
			commentId,
			pageSize: 100,
			userIdType: "open_id",
			...(pageToken !== undefined ? { pageToken } : {}),
		});
		all.push(...res.items);
		if (!res.hasMore) break;
		if (res.pageToken === "") break;
		pageToken = res.pageToken;
	}
	return all;
}

/**
 * _select_local_timeline (:802): ≤limit passes through; else keep first,
 * target, and last, then expand OUTWARD from target alternating lo/hi
 * (lo attempted first each round), ascending output order.
 */
export function selectLocalTimeline(
	timeline: TimelineEntry[],
	targetIndex: number,
): TimelineEntry[] {
	if (timeline.length <= LOCAL_TIMELINE_LIMIT) return timeline;
	const n = timeline.length;
	const selected = new Set<number>([0, n - 1]);
	if (0 <= targetIndex && targetIndex < n) selected.add(targetIndex);
	let budget = LOCAL_TIMELINE_LIMIT - selected.size;
	let lo = targetIndex - 1;
	let hi = targetIndex + 1;
	while (budget > 0 && (lo >= 0 || hi < n)) {
		if (lo >= 0 && !selected.has(lo)) {
			selected.add(lo);
			budget -= 1;
		}
		lo -= 1;
		if (budget > 0 && hi < n && !selected.has(hi)) {
			selected.add(hi);
			budget -= 1;
		}
		hi += 1;
	}
	return [...selected]
		.sort((a, b) => a - b)
		.map((i) => timeline[i] as TimelineEntry);
}

/**
 * _select_whole_timeline (:833): prioritizes CURRENT entry + nearest self
 * reply, then expands outward from current; fallback = last N entries.
 */
export function selectWholeTimeline(
	timeline: TimelineEntry[],
	currentIndex: number,
	nearestSelfIndex: number,
): TimelineEntry[] {
	if (timeline.length <= WHOLE_TIMELINE_LIMIT) return timeline;
	const n = timeline.length;
	const selected = new Set<number>();
	if (0 <= currentIndex && currentIndex < n) selected.add(currentIndex);
	if (0 <= nearestSelfIndex && nearestSelfIndex < n)
		selected.add(nearestSelfIndex);
	let budget = WHOLE_TIMELINE_LIMIT - selected.size;
	let lo = currentIndex - 1;
	let hi = currentIndex + 1;
	while (budget > 0 && (lo >= 0 || hi < n)) {
		if (lo >= 0 && !selected.has(lo)) {
			selected.add(lo);
			budget -= 1;
		}
		lo -= 1;
		if (budget > 0 && hi < n && !selected.has(hi)) {
			selected.add(hi);
			budget -= 1;
		}
		hi += 1;
	}
	if (selected.size === 0) return timeline.slice(-WHOLE_TIMELINE_LIMIT);
	return [...selected]
		.sort((a, b) => a - b)
		.map((i) => timeline[i] as TimelineEntry);
}

/**
 * _COMMON_INSTRUCTIONS (:868) — VERBATIM 12-line block.
 */
const COMMON_INSTRUCTIONS = `
This is a Feishu document comment thread, not an IM chat.
Do NOT call feishu_drive_add_comment or feishu_drive_reply_comment yourself.
Your reply will be posted automatically. Just output the reply text.
Use the thread timeline above as the main context.
If the quoted content is not enough, use feishu_doc_read to read nearby context.
The quoted content is your primary anchor — insert/summarize/explain requests are about it.
Do not guess document content you haven't read.
Reply in the same language as the user's comment unless they request otherwise.
Use plain text only. Do not use Markdown, headings, bullet lists, tables, or code blocks.
Do not show your reasoning process. Do not start with "I will", "Let me", or "I'll first".
Output only the final user-facing reply.
If no reply is needed, output exactly NO_REPLY.
`.trim();

/**
 * build_local_comment_prompt (:884) — VERBATIM skeleton: opener, quoted
 * anchors, doc coordinates, the "(n/m)" timeline header, "[uid] text" lines
 * with "<-- YOU" self markers, the referenced-docs section when links
 * exist, then the common instruction block.
 */
export function buildLocalCommentPrompt(opts: {
	docTitle: string;
	docUrl: string;
	fileType: string;
	fileToken: string;
	commentId: string;
	quoteText: string;
	rootCommentText: string;
	targetReplyText: string;
	timeline: TimelineEntry[];
	targetIndex?: number | undefined;
	referencedDocs?: string | undefined;
}): string {
	const selected = selectLocalTimeline(opts.timeline, opts.targetIndex ?? -1);

	const lines = [
		`The user added a reply in "${opts.docTitle}".`,
		`Current user comment text: "${truncate(opts.targetReplyText)}"`,
		`Original comment text: "${truncate(opts.rootCommentText)}"`,
		`Quoted content: "${truncate(opts.quoteText, QUOTE_TEXT_LIMIT)}"`,
		"This comment mentioned you (@mention is for routing, not task content).",
		`Document link: ${opts.docUrl}`,
		"Current commented document:",
		`- file_type=${opts.fileType}`,
		`- file_token=${opts.fileToken}`,
		`- comment_id=${opts.commentId}`,
		"",
		`Current comment card timeline (${selected.length}/${opts.timeline.length} entries):`,
	];

	for (const entry of selected) {
		const marker = entry.isSelf ? " <-- YOU" : "";
		lines.push(`[${entry.openId}] ${truncate(entry.text)}${marker}`);
	}

	if (opts.referencedDocs) lines.push(opts.referencedDocs);

	lines.push("");
	lines.push(COMMON_INSTRUCTIONS);
	return lines.join("\n");
}

/**
 * build_whole_comment_prompt (:929) — VERBATIM whole-document skeleton.
 */
export function buildWholeCommentPrompt(opts: {
	docTitle: string;
	docUrl: string;
	fileType: string;
	fileToken: string;
	commentText: string;
	timeline: TimelineEntry[];
	currentIndex?: number | undefined;
	nearestSelfIndex?: number | undefined;
	referencedDocs?: string | undefined;
}): string {
	const selected = selectWholeTimeline(
		opts.timeline,
		opts.currentIndex ?? -1,
		opts.nearestSelfIndex ?? -1,
	);

	const lines = [
		`The user added a comment in "${opts.docTitle}".`,
		`Current user comment text: "${truncate(opts.commentText)}"`,
		"This is a whole-document comment.",
		"This comment mentioned you (@mention is for routing, not task content).",
		`Document link: ${opts.docUrl}`,
		"Current commented document:",
		`- file_type=${opts.fileType}`,
		`- file_token=${opts.fileToken}`,
		"",
		`Whole-document comment timeline (${selected.length}/${opts.timeline.length} entries):`,
	];

	for (const entry of selected) {
		const marker = entry.isSelf ? " <-- YOU" : "";
		lines.push(`[${entry.openId}] ${truncate(entry.text)}${marker}`);
	}

	if (opts.referencedDocs) lines.push(opts.referencedDocs);

	lines.push("");
	lines.push(COMMON_INSTRUCTIONS);
	return lines.join("\n");
}

// ── referenced docs (:678 _extract_docs_links / :770 _format_referenced_docs)

/** feishu/lark document URL pattern (:671 _FEISHU_DOC_URL_RE). */
const FEISHU_DOC_URL_RE =
	/(?:feishu\.cn|larkoffice\.com|larksuite\.com|lark\.suite\.com)\/(wiki|doc|docx|sheet|sheets|slides|mindnote|bitable|base|file)\/([A-Za-z0-9_-]{10,40})/g;

interface DocLink {
	url: string;
	docType: string;
	token: string;
	resolvedType?: string | undefined;
	resolvedToken?: string | undefined;
}

/** Unique doc links across reply TEXTS (the fake plane flattens elements to
 * text; the token regex rides the flattened body — :678 dedupe by token). */
function extractDocLinks(texts: string[]): DocLink[] {
	const seenTokens = new Set<string>();
	const links: DocLink[] = [];
	for (const text of texts) {
		for (const match of text.matchAll(FEISHU_DOC_URL_RE)) {
			const url = match[0] ?? "";
			const docType = match[1] ?? "";
			const token = match[2] ?? "";
			if (!url || !token || seenTokens.has(token)) continue;
			seenTokens.add(token);
			links.push({ url, docType, token });
		}
	}
	return links;
}

/** _format_referenced_docs (:770): section header + one bullet per link. */
function formatReferencedDocs(
	links: DocLink[],
	currentFileToken: string,
): string {
	if (links.length === 0) return "";
	const lines = ["", "Referenced documents in comments:"];
	for (const link of links) {
		const rtype = link.resolvedType ?? link.docType;
		const rtoken = link.resolvedToken ?? link.token;
		const suffix =
			rtoken === currentFileToken ? " (same as current document)" : "";
		lines.push(`- ${rtype}:${rtoken}${suffix} (${link.url.slice(0, 80)})`);
	}
	return lines.join("\n");
}

/** _resolve_wiki_nodes (:732): wiki links resolve to their underlying doc. */
async function resolveWikiNodes(
	links: DocLink[],
	api: CommentApi,
): Promise<DocLink[]> {
	for (const link of links) {
		if (link.docType !== "wiki") continue;
		const resolved = api.getWikiNode(link.token);
		if (resolved !== null && resolved.objType && resolved.objToken) {
			link.resolvedType = resolved.objType;
			link.resolvedToken = resolved.objToken;
		}
	}
	return links;
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
	let rule = resolveRule(cfg, evt.fileType, evt.fileToken);
	// Wiki re-resolution (:1172–1177): when no exact rule matched and the
	// config carries wiki:* keys, reverse-lookup the owning wiki node token
	// and RE-RESOLVE — wiki:{node}-keyed rules can otherwise never match a
	// plain doc token.
	if (
		(rule.matchSource === "wildcard" || rule.matchSource === "top") &&
		hasWikiKeys(cfg)
	) {
		const wikiToken = deps.api.reverseLookupWikiNode(
			evt.fileToken,
			evt.fileType,
		);
		if (wikiToken !== null && wikiToken !== "") {
			rule = resolveRule(cfg, evt.fileType, evt.fileToken, wikiToken);
		}
	}
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
	// synchronous best-effort here; failure never blocks flow. Vendor wire:
	// POST drive/v2 …/comments/reaction?file_type body
	// {action:"add",reply_id,reaction_type:"OK"} (:156).
	let reacted = false;
	if (evt.replyId) {
		reacted = deps.api.addReaction({
			fileToken: evt.fileToken,
			fileType: evt.fileType,
			replyId: evt.replyId,
			reactionType: "OK",
		});
	}

	try {
		// ── parallel fetch (gather parity): meta + eventual-consistency reads.
		const meta = deps.api.docMeta({
			fileToken: evt.fileToken,
			fileType: evt.fileType,
		}) ?? { title: "Untitled", url: "" };
		let detail: CommentDetail | undefined;
		for (let attempt = 0; attempt < COMMENT_RETRY_LIMIT; attempt++) {
			detail = deps.api.batchQueryComment({
				fileToken: evt.fileToken,
				fileType: evt.fileType,
				userIdType: "open_id",
				commentIds: [evt.commentId],
			})?.items[0];
			if (detail !== undefined) break;
			await deps.sleepMs?.(1);
		}
		const isWhole = detail?.isWhole === true;
		out.isWhole = isWhole;

		let prompt: string;
		if (isWhole) {
			const whole = listWholeCommentsPaged(
				deps.api,
				evt.fileToken,
				evt.fileType,
			);
			// Flatten each whole comment's reply_list into ONE timeline
			// (:355 list_whole_comments flattening parity), tracking the CURRENT
			// entry (last reply authored by the commenter) and the NEAREST SELF
			// entry (:1230–1249 walk parity).
			const flat: ReplyEntry[] = whole.flatMap((c) => c.replies);
			const timeline = toTimeline(flat, deps.selfOpenId);
			let currentText = "";
			let currentIndex = -1;
			let nearestSelfIndex = -1;
			for (let idx = 0; idx < timeline.length; idx++) {
				const entry = timeline[idx] as TimelineEntry;
				if (entry.openId === evt.fromOpenId) {
					currentText = entry.text;
					currentIndex = idx;
				}
				if (entry.isSelf) nearestSelfIndex = idx;
			}
			if (currentText === "") {
				for (let idx = timeline.length - 1; idx >= 0; idx--) {
					const entry = timeline[idx] as TimelineEntry;
					if (!entry.isSelf) {
						currentText = entry.text;
						currentIndex = idx;
						break;
					}
				}
			}
			const referencedDocs = formatReferencedDocs(
				await resolveWikiNodes(
					extractDocLinks(flat.map((r) => r.text)),
					deps.api,
				),
				evt.fileToken,
			);
			prompt = buildWholeCommentPrompt({
				docTitle: meta.title,
				docUrl: meta.url,
				fileType: evt.fileType,
				fileToken: evt.fileToken,
				commentText: currentText,
				timeline,
				currentIndex,
				nearestSelfIndex,
				referencedDocs,
			});
		} else {
			// Eventual-consistency: replies listing retries until the expected
			// reply id appears (or the budget is exhausted); each attempt walks
			// up to 5 pages of 100 (:411/:424).
			let replies: ReplyEntry[] = [];
			for (let attempt = 0; attempt < COMMENT_RETRY_LIMIT; attempt++) {
				replies = listRepliesPaged(
					deps.api,
					evt.fileToken,
					evt.fileType,
					evt.commentId,
				);
				if (replies.some((r) => r.replyId === evt.replyId)) break;
				await deps.sleepMs?.(1);
			}
			const timeline = toTimeline(replies, deps.selfOpenId);
			// Root = first entry; target = the replied-to id, else the LAST entry
			// authored by the commenter (:1291–1298 reversed fallback scan).
			const rootText = replies[0]?.text ?? "";
			let targetIndex = replies.findIndex((r) => r.replyId === evt.replyId);
			if (targetIndex < 0) {
				for (let idx = timeline.length - 1; idx >= 0; idx--) {
					if ((timeline[idx] as TimelineEntry).openId === evt.fromOpenId) {
						targetIndex = idx;
						break;
					}
				}
			}
			const targetText =
				targetIndex >= 0 ? (replies[targetIndex]?.text ?? "") : "";
			const referencedDocs = formatReferencedDocs(
				await resolveWikiNodes(
					extractDocLinks(replies.map((r) => r.text)),
					deps.api,
				),
				evt.fileToken,
			);
			prompt = buildLocalCommentPrompt({
				docTitle: meta.title,
				docUrl: meta.url,
				fileType: evt.fileType,
				fileToken: evt.fileToken,
				commentId: evt.commentId,
				quoteText: detail?.quote ?? "",
				rootCommentText: rootText,
				targetReplyText: targetText,
				timeline,
				targetIndex,
				referencedDocs,
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
				? deps.api.postNewComment({
						fileToken: evt.fileToken,
						fileType: evt.fileType,
						replyElementsText: safe,
					})
				: deps.api.postThreadReply({
						fileToken: evt.fileToken,
						fileType: evt.fileType,
						commentId: evt.commentId,
						textRunText: safe,
					});
			if (posted.ok) {
				out.deliveredChunks += 1;
				continue;
			}
			if (!useWhole && posted.code === REPLY_NOT_ALLOWED_CODE) {
				useWhole = true; // downgrade applies to this AND subsequent chunks
				out.fellBackToWholeComment = true;
				const retried = deps.api.postNewComment({
					fileToken: evt.fileToken,
					fileType: evt.fileType,
					replyElementsText: safe,
				});
				if (retried.ok) out.deliveredChunks += 1;
				continue;
			}
			break; // first failed chunk aborts the rest (:554)
		}
	} finally {
		// ── cleanup: delete the OK reaction, best-effort (:1112 cleanup).
		if (reacted && evt.replyId) {
			out.cleanedUp = deps.api.deleteReaction({
				fileToken: evt.fileToken,
				fileType: evt.fileType,
				replyId: evt.replyId,
				reactionType: "OK",
			});
		}
	}
	return out;
}
