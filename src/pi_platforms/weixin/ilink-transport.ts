// pi_platforms/weixin/ilink-transport — the iLink HTTP request-SHAPE plane,
// ported from Hermes gateway/platforms/weixin.py module helpers (READ-ONLY
// reference; semantics ported, no code vendored).
//
// Hermes anchors:
//   weixin.py:_base_info — {"channel_version": CHANNEL_VERSION} is merged into
//     EVERY outgoing iLink POST body by _api_post.
//   weixin.py:_headers — the exact POST header plane: Content-Type json,
//     AuthorizationType ilink_bot_token, Content-Length, X-WECHAT-UIN (a
//     random uint32 sent as base64 of its DECIMAL string), iLink-App-Id,
//     iLink-App-ClientVersion (+ Bearer token when one exists).
//   weixin.py:_api_get — the GET plane (QR login) carries ONLY the two
//     iLink-App-* identity headers; never a Bearer/body auth.
//   weixin.py:_cdn_upload_url — constructed CDN upload URL from upload_param.
//   weixin.py:_upload_ciphertext — the CDN leg speaks application/octet-stream
//     and answers with the x-encrypted-param response header.
//   weixin.py:_outbound_media_builder — image/video/voice(.silk)/file item
//     shapes; aes_key rides as base64(HEX STRING), never base64(raw bytes).

import { randomBytes } from "node:crypto";
import {
	CHANNEL_VERSION,
	ILINK_APP_CLIENT_VERSION,
	ILINK_APP_ID,
	ITEM_FILE,
	ITEM_IMAGE,
	ITEM_VIDEO,
	ITEM_VOICE,
	MEDIA_FILE,
	MEDIA_IMAGE,
	MEDIA_VIDEO,
	MEDIA_VOICE,
} from "./manifest.js";

export type RandomBytesFn = (n: number) => Buffer;

/** node:crypto CSPRNG (production default; fixtures may inject determinism). */
export function defaultRandomBytes(n: number): Buffer {
	return randomBytes(n);
}

/** weixin.py:_base_info parity — merged into EVERY outgoing POST body. */
export function baseInfo(): Record<string, unknown> {
	return { channel_version: CHANNEL_VERSION };
}

/** weixin.py:_random_wechat_uin — base64 of a decimal uint32 string. */
export function randomWechatUin(rng: RandomBytesFn): string {
	const value = rng(4).readUInt32BE(0);
	return Buffer.from(String(value), "utf8").toString("base64");
}

/**
 * weixin.py:_headers parity — the exact iLink POST header set. Content-Length
 * is the UTF-8 byte length of the serialized body (Hermes serializes first).
 */
export function buildILinkPostHeaders(
	token: string | undefined,
	bodyByteLength: number,
	rng: RandomBytesFn,
): Record<string, string> {
	const headers: Record<string, string> = {
		"Content-Type": "application/json",
		AuthorizationType: "ilink_bot_token",
		"Content-Length": String(bodyByteLength),
		"X-WECHAT-UIN": randomWechatUin(rng),
		"iLink-App-Id": ILINK_APP_ID,
		"iLink-App-ClientVersion": String(ILINK_APP_CLIENT_VERSION),
	};
	if (token !== undefined && token !== "") {
		headers.Authorization = `Bearer ${token}`;
	}
	return headers;
}

/** weixin.py:_api_get headers — QR-login GETs carry app identity ONLY. */
export function buildILinkGetHeaders(): Record<string, string> {
	return {
		"iLink-App-Id": ILINK_APP_ID,
		"iLink-App-ClientVersion": String(ILINK_APP_CLIENT_VERSION),
	};
}

/** weixin.py:_upload_ciphertext — the CDN PUT-leg replacement speaks raw bytes. */
export const CDN_UPLOAD_HEADERS: Readonly<Record<string, string>> =
	Object.freeze({ "Content-Type": "application/octet-stream" });

/**
 * weixin.py:_cdn_upload_url — constructed CDN upload URL from upload_param
 * when getuploadurl answers no direct upload_full_url.
 */
export function cdnUploadUrl(
	cdnBaseUrl: string,
	uploadParam: string,
	filekey: string,
): string {
	return `${cdnBaseUrl.replace(/\/+$/, "")}/upload?encrypted_query_param=${encodeURIComponent(uploadParam)}&filekey=${encodeURIComponent(filekey)}`;
}

// ── outbound media classification (_outbound_media_builder parity) ──────────

export type OutboundMediaKind = "image" | "video" | "voice" | "file";

/**
 * Extension → mime-type surface (mimetypes.guess_type coverage for the media
 * families weixin ships). Hermes classifies by MIME PREFIX, not by a fixed
 * extension list — .svg/.tiff/.ico ride image/* exactly like .png.
 */
const MIME_BY_EXT: ReadonlyMap<string, string> = new Map([
	["png", "image/png"],
	["jpg", "image/jpeg"],
	["jpeg", "image/jpeg"],
	["gif", "image/gif"],
	["webp", "image/webp"],
	["bmp", "image/bmp"],
	["heic", "image/heic"],
	["heif", "image/heif"],
	["svg", "image/svg+xml"],
	["tif", "image/tiff"],
	["tiff", "image/tiff"],
	["ico", "image/x-icon"],
	["avif", "image/avif"],
	["mp4", "video/mp4"],
	["mov", "video/quicktime"],
	["avi", "video/x-msvideo"],
	["mkv", "video/x-matroska"],
	["webm", "video/webm"],
	["3gp", "video/3gpp"],
	["mpg", "video/mpeg"],
	["mpeg", "video/mpeg"],
	["m4v", "video/x-m4v"],
	["mp3", "audio/mpeg"],
	["wav", "audio/wav"],
	["ogg", "audio/ogg"],
	["opus", "audio/opus"],
	["flac", "audio/flac"],
	["m4a", "audio/mp4"],
	["aac", "audio/aac"],
]);

/** mimetypes.guess_type parity: unknown extensions are octet-stream. */
function guessMimeType(filename: string): string {
	const dot = filename.lastIndexOf(".");
	const ext = dot >= 0 ? filename.slice(dot + 1).toLowerCase() : "";
	return MIME_BY_EXT.get(ext) ?? "application/octet-stream";
}

/**
 * weixin.py:_outbound_media_builder media_type selection (vendor ORDER kept):
 * image mime → image, video mime → video, .silk → voice (unless forced to
 * file — force_file_attachment gates ONLY the silk leg), audio/* WITHOUT
 * .silk and everything else ride MEDIA_FILE (vendor truth, not intuition).
 */
export function outboundMediaKind(
	filename: string,
	forceFileAttachment = false,
): OutboundMediaKind {
	const mime = guessMimeType(filename);
	if (mime.startsWith("image/")) return "image";
	if (mime.startsWith("video/")) return "video";
	const dot = filename.lastIndexOf(".");
	const ext = dot >= 0 ? filename.slice(dot + 1).toLowerCase() : "";
	if (ext === "silk" && !forceFileAttachment) return "voice";
	return "file"; // audio/* non-.silk AND every unknown mime
}

/** MEDIA_* code for a classified kind (getuploadurl media_type field). */
export function mediaTypeForKind(kind: OutboundMediaKind): number {
	switch (kind) {
		case "image":
			return MEDIA_IMAGE;
		case "video":
			return MEDIA_VIDEO;
		case "voice":
			return MEDIA_VOICE;
		case "file":
			return MEDIA_FILE;
	}
}

/**
 * THE byte-specific key transport rule (weixin.py:_send_file): the API carries
 * aes_key as base64 of the ASCII HEX STRING — NEVER base64 of the raw bytes.
 * base64(raw_bytes) decrypts to grey boxes on the receiver side because the
 * decryption key does not match.
 */
export function aesKeyForApi(aesKeyHex: string): string {
	return Buffer.from(aesKeyHex, "ascii").toString("base64");
}

export interface MediaItemParams {
	encryptQueryParam: string;
	aesKeyApi: string;
	ciphertextSize: number;
	plaintextSize: number;
	filename: string;
	rawfilemd5: string;
}

/**
 * weixin.py:_outbound_media_builder item shapes (image mid_size / video_size +
 * video_md5 / .silk voice encode_type 6 @ 24kHz 16-bit / file file_name+len).
 */
export function buildOutboundMediaItem(
	kind: OutboundMediaKind,
	p: MediaItemParams,
): Record<string, unknown> {
	const media = {
		encrypt_query_param: p.encryptQueryParam,
		aes_key: p.aesKeyApi,
		encrypt_type: 1,
	};
	switch (kind) {
		case "image":
			return {
				type: ITEM_IMAGE,
				image_item: { media, mid_size: p.ciphertextSize },
			};
		case "video":
			return {
				type: ITEM_VIDEO,
				video_item: {
					media,
					video_size: p.ciphertextSize,
					play_length: 0,
					video_md5: p.rawfilemd5,
				},
			};
		case "voice":
			return {
				type: ITEM_VOICE,
				voice_item: {
					media,
					encode_type: 6,
					bits_per_sample: 16,
					sample_rate: 24000,
					playtime: 0,
				},
			};
		case "file":
			return {
				type: ITEM_FILE,
				file_item: {
					media,
					file_name: p.filename,
					len: String(p.plaintextSize),
				},
			};
	}
}
