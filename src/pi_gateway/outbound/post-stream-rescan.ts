// pi_gateway/outbound/post-stream-rescan.ts — post-stream media delivery
// (03-message-routing.md §9.3). Streaming already sent the reply text
// verbatim, so this pass deliberately DIVERGES from the non-streaming
// auto-detect chain:
//
//   - ONLY explicit MEDIA: tags trigger uploads. Bare local paths are NEVER
//     promoted post-stream (#20834): a bare path in streamed text was either
//     already shown to the user as text or is stale tool content.
//   - NO dedupe against prior turns (#73771): an explicit tag in this turn's
//     final reply is a deliberate attachment INCLUDING user-requested resends.
//   - [[as_document]] is captured BEFORE extraction strips it, so image files
//     route to byte-preserving document delivery instead of photo recompression.
//   - Images partition into ONE batched send; every other file dispatches
//     independently with per-file error isolation — one failed upload never
//     cancels siblings.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   run.py:_deliver_media_from_response        → rescanPostStream
//   base.py:should_send_media_as_audio         → shouldSendMediaAsAudio
//   base.py:filter_media_delivery_paths        → filterMediaDeliveryPaths (media-policy)

import {
	filterMediaDeliveryPaths,
	type PathValidationEnv,
} from "./media-policy.js";
import {
	extractMedia,
	hasAsDocumentDirective,
	type ProtectedSpanOptions,
} from "./media-grammar.js";

/** Image extensions that batch into ONE multi-image send (run.py::_IMAGE_EXTS). */
export const IMAGE_EXTS: ReadonlySet<string> = new Set([
	".jpg",
	".jpeg",
	".png",
	".webp",
	".gif",
]);

/** Video extensions routed to the video sender (run.py::_VIDEO_EXTS). */
export const VIDEO_EXTS: ReadonlySet<string> = new Set([
	".mp4",
	".mov",
	".avi",
	".mkv",
	".webm",
	".3gp",
]);

const TELEGRAM_VOICE_EXTS: ReadonlySet<string> = new Set([".ogg", ".opus"]);
const TELEGRAM_AUDIO_ATTACHMENT_EXTS: ReadonlySet<string> = new Set([
	".mp3",
	".m4a",
]);

/**
 * Whether a media file uses the platform's audio sender
 * (base.py:should_send_media_as_audio). Telegram's Bot API only accepts
 * MP3/M4A for sendAudio and Opus/OGG for sendVoice; Opus/OGG routes as audio
 * ONLY when flagged is_voice (a regular .ogg attachment must not become a
 * voice bubble by accident). Everything else falls through to document
 * delivery.
 */
export function shouldSendMediaAsAudio(
	platform: string,
	ext: string,
	isVoice = false,
): boolean {
	const normalizedExt = (ext ?? "").toLowerCase();
	if (!AUDIO_MEMBER(normalizedExt)) return false;
	const p = (platform ?? "").toLowerCase();
	if (p === "telegram") {
		if (TELEGRAM_VOICE_EXTS.has(normalizedExt)) return isVoice;
		return TELEGRAM_AUDIO_ATTACHMENT_EXTS.has(normalizedExt);
	}
	return true;
}

/** Recognized-audio membership (media-grammar AUDIO_EXTS shape, kept local to avoid a cycle-prone import surface). */
function AUDIO_MEMBER(ext: string): boolean {
	return [".mp3", ".m2a", ".wav", ".ogg", ".opus", ".m4a", ".flac"].includes(
		ext,
	);
}

/** Dispatch seam: the platform adapter surface this pass drives (no real transport). */
export interface PostStreamAdapter {
	name?: string;
	sendMultipleImages?(chatId: string, images: string[]): Promise<unknown>;
	sendVoice?(chatId: string, audioPath: string): Promise<unknown>;
	sendVideo?(chatId: string, videoPath: string): Promise<unknown>;
	sendDocument?(chatId: string, filePath: string): Promise<unknown>;
}

export type MediaDispatchKind =
	| "image_batch"
	| "voice_or_audio"
	| "video"
	| "document";

export interface MediaDispatchAttempt {
	kind: MediaDispatchKind;
	/** For image_batch: every image in the single batched call; else the one file. */
	paths: string[];
	status: "sent" | "failed";
	error?: string;
}

export interface RescanResult {
	attempts: MediaDispatchAttempt[];
	/** Cleaned text parity with extract_media (directives + tags removed). */
	cleaned: string;
	forceDocumentAttachments: boolean;
}

export interface RescanOptions extends ProtectedSpanOptions, PathValidationEnv {
	adapter: PostStreamAdapter;
	chatId: string;
	/** Logical platform name driving audio-delivery rules (event.source.platform). */
	chatPlatform?: string;
}

function extensionOf(path: string): string {
	const idx = path.lastIndexOf(".");
	if (idx <= path.lastIndexOf("/")) return "";
	return path.slice(idx).toLowerCase();
}

function filterWithValidator<T extends { path: string; isVoice: boolean }>(
	media: readonly T[],
	validatePath: (p: string) => string | null,
): T[] {
	const safe: T[] = [];
	for (const entry of media) {
		const safePath = validatePath(entry.path);
		if (safePath) safe.push({ ...entry, path: safePath });
	}
	return safe;
}

/**
 * Extract EXPLICIT MEDIA directives from an already-streamed final reply and
 * dispatch them. This is the streaming lane's attachment half: text went out
 * verbatim during streaming; only tagged attachments are handled here, and
 * each non-image file fails alone.
 */
export async function rescanPostStream(
	response: string,
	opts: RescanOptions,
): Promise<RescanResult> {
	const forceDocumentAttachments = hasAsDocumentDirective(response);

	const { media, cleaned } = extractMedia(response, opts);
	// Production path: the real fs-backed validation ladder. When a test
	// injects validatePath, it gates BOTH extraction and this filter so the
	// whole pass stays injectable end-to-end.
	const safeMedia = opts.validatePath
		? filterWithValidator(media, opts.validatePath)
		: filterMediaDeliveryPaths(media, opts);

	const imagePaths: string[] = [];
	const nonImageMedia: Array<{ path: string; isVoice: boolean }> = [];
	for (const entry of safeMedia) {
		const ext = extensionOf(entry.path);
		if (IMAGE_EXTS.has(ext) && !entry.isVoice && !forceDocumentAttachments) {
			imagePaths.push(entry.path);
		} else {
			nonImageMedia.push(entry);
		}
	}

	const attempts: MediaDispatchAttempt[] = [];

	if (imagePaths.length > 0 && opts.adapter.sendMultipleImages) {
		try {
			await opts.adapter.sendMultipleImages(opts.chatId, imagePaths);
			attempts.push({
				kind: "image_batch",
				paths: [...imagePaths],
				status: "sent",
			});
		} catch (e) {
			attempts.push({
				kind: "image_batch",
				paths: [...imagePaths],
				status: "failed",
				error: e instanceof Error ? e.message : String(e),
			});
		}
	}

	for (const entry of nonImageMedia) {
		const ext = extensionOf(entry.path);
		// Partition decided ONCE per file (§9.3): audio rules → video → document.
		const kind: MediaDispatchKind = shouldSendMediaAsAudio(
			opts.chatPlatform ?? "",
			ext,
			entry.isVoice,
		)
			? "voice_or_audio"
			: VIDEO_EXTS.has(ext)
				? "video"
				: "document";
		try {
			if (kind === "voice_or_audio") {
				await opts.adapter.sendVoice?.(opts.chatId, entry.path);
			} else if (kind === "video") {
				await opts.adapter.sendVideo?.(opts.chatId, entry.path);
			} else {
				await opts.adapter.sendDocument?.(opts.chatId, entry.path);
			}
			attempts.push({ kind, paths: [entry.path], status: "sent" });
		} catch (e) {
			attempts.push({
				kind,
				paths: [entry.path],
				status: "failed",
				error: e instanceof Error ? e.message : String(e),
			});
		}
	}

	return { attempts, cleaned, forceDocumentAttachments };
}
