// pi_platforms/qqbot/chunked-uploader — the QQ v2 three-step chunked upload
// flow, ported from Hermes gateway/platforms/qqbot/chunked_upload.py.
//
// Hermes anchors:
//   chunked_upload.py:ChunkedUploader.upload — prepare → PUT parts (COS) →
//     upload_part_finish per part → complete_upload via /files {upload_id}.
//   chunked_upload.py:_parse_prepare_response — tolerates `data` wrapping and
//     `part_list`/`url` field aliases; missing upload_id/parts ⇒ ValueError.
//   chunked_upload.py error-code semantics:
//     40093002 upload_prepare daily quota → UploadDailyLimitExceededError
//              (NON-retryable, typed for user-friendly replies);
//     40093001 upload_part_finish transient → retry until server
//              retry_timeout elapses (local cap 600s).
//   chunked_upload.py:_compute_file_hashes — md5 + sha1 + md5_10m in ONE pass;
//     md5_10m covers the first 10,002,432 bytes (= full md5 for small files).
//   chunked_upload.py:_read_file_chunk — short read ⇒ IOError (truncated).

import { createHash } from "node:crypto";
import {
	QQ_BIZ_CODE_DAILY_LIMIT,
	QQ_BIZ_CODE_PART_RETRYABLE,
	QQ_COMPLETE_UPLOAD_BASE_DELAY_S,
	QQ_COMPLETE_UPLOAD_MAX_RETRIES,
	QQ_DEFAULT_CONCURRENT_PARTS,
	QQBOT_FILE_UPLOAD_TIMEOUT_S,
	QQ_MAX_CONCURRENT_PARTS,
	QQ_MD5_10M_SIZE,
	QQ_PART_FINISH_DEFAULT_TIMEOUT_S,
	QQ_PART_FINISH_MAX_TIMEOUT_S,
	QQ_PART_FINISH_RETRY_INTERVAL_S,
	QQ_PART_UPLOAD_MAX_RETRIES,
	QQ_PART_UPLOAD_TIMEOUT_S,
} from "./manifest.js";

export class UploadDailyLimitExceededError extends Error {
	constructor(
		readonly fileName: string,
		readonly fileSize: number,
		message = "",
	) {
		super(
			message || `Daily upload limit exceeded for ${JSON.stringify(fileName)}`,
		);
		this.name = "UploadDailyLimitExceededError";
	}
}

export class UploadFileTooLargeError extends Error {
	constructor(
		readonly fileName: string,
		readonly fileSize: number,
		readonly limitBytes = 0,
		message = "",
	) {
		const limitStr = limitBytes > 0 ? ` (${formatSize(limitBytes)})` : "";
		super(
			message ||
				`File ${JSON.stringify(fileName)} (${formatSize(fileSize)}) exceeds platform limit${limitStr}`,
		);
		this.name = "UploadFileTooLargeError";
	}
}

/** Human-readable size, e.g. '12.3 MB' (chunked_upload.py:format_size). */
export function formatSize(sizeBytes: number): string {
	let size = Number(sizeBytes);
	for (const unit of ["B", "KB", "MB", "GB"]) {
		if (size < 1024) return `${size.toFixed(1)} ${unit}`;
		size /= 1024;
	}
	return `${size.toFixed(1)} TB`;
}

export interface PreparePart {
	index: number;
	presignedUrl: string;
	blockSize: number;
}

interface PrepareResult {
	uploadId: string;
	blockSize: number;
	parts: PreparePart[];
	concurrency: number;
	retryTimeoutS: number;
}

/** Normalized prepare response (chunked_upload.py:_parse_prepare_response). */
export function parsePrepareResponse(
	raw: Record<string, unknown>,
): PrepareResult {
	const src =
		raw["data"] !== undefined &&
		raw["data"] !== null &&
		typeof raw["data"] === "object"
			? (raw["data"] as Record<string, unknown>)
			: raw;
	const uploadId = String(src["upload_id"] ?? "");
	if (!uploadId) {
		throw new Error(
			`upload_prepare response missing upload_id: ${JSON.stringify(raw).slice(0, 200)}`,
		);
	}
	const blockSize = Number(src["block_size"] ?? 0);
	const rawPartsRaw = src["parts"] ?? src["part_list"] ?? [];
	if (!Array.isArray(rawPartsRaw) || rawPartsRaw.length === 0) {
		throw new Error(
			`upload_prepare response missing parts: ${JSON.stringify(raw).slice(0, 200)}`,
		);
	}
	const parts: PreparePart[] = [];
	for (const p of rawPartsRaw) {
		if (p === null || typeof p !== "object") continue;
		const rec = p as Record<string, unknown>;
		parts.push({
			index: Number(rec["part_index"] ?? rec["index"] ?? 0),
			presignedUrl: String(rec["presigned_url"] ?? rec["url"] ?? ""),
			blockSize: Number(rec["block_size"] ?? 0),
		});
	}
	return {
		uploadId,
		blockSize,
		parts,
		concurrency:
			Number(src["concurrency"] ?? QQ_DEFAULT_CONCURRENT_PARTS) ||
			QQ_DEFAULT_CONCURRENT_PARTS,
		retryTimeoutS: Number(src["retry_timeout"] ?? 0) || 0,
	};
}

/**
 * The injected seams: apiRequest mirrors the adapter's _api_request(method,
 * path, body, timeout) and MUST embed the numeric biz_code in its error
 * message on API errors; httpPut performs a COS PUT and resolves an
 * http-like {status, text}; sleep is the injected clock's delay.
 */
export interface ChunkedUploaderSeams {
	apiRequest(
		method: "POST" | "PUT" | "GET",
		path: string,
		body?: Record<string, unknown> | undefined,
		timeoutS?: number | undefined,
	): Promise<Record<string, unknown>>;
	httpPut(
		url: string,
		data: Buffer,
		headers: Record<string, string>,
	): Promise<{ status: number; text?: string | undefined }>;
	sleep(ms: number): Promise<void>;
	monotonicMs(): number;
}

function bizCodeIn(errMsg: string, code: number): boolean {
	return errMsg.includes(String(code));
}

interface FileHashes {
	md5: string;
	sha1: string;
	md5_10m: string;
}

/** md5 + sha1 + md5_10m in ONE pass (chunked_upload.py:_compute_file_hashes). */
export function computeFileHashes(data: Buffer, fileSize: number): FileHashes {
	const md5 = createHash("md5");
	const sha1 = createHash("sha1");
	const md5_10m = createHash("md5");
	const need10m = fileSize > QQ_MD5_10M_SIZE;
	md5.update(data);
	sha1.update(data);
	if (need10m) {
		md5_10m.update(data.subarray(0, Math.min(QQ_MD5_10M_SIZE, data.length)));
	}
	const fullMd5 = md5.digest("hex");
	return {
		md5: fullMd5,
		sha1: sha1.digest("hex"),
		// For small files the "10m" hash IS the full md5.
		md5_10m: need10m ? md5_10m.digest("hex") : fullMd5,
	};
}

/** Slice one part with SHORT-READ detection (chunked_upload.py:_read_file_chunk). */
export function readFileChunk(
	data: Buffer,
	offset: number,
	length: number,
): Buffer {
	if (offset < 0 || offset + length > data.length) {
		throw new Error(
			`Short read: expected ${length} bytes at offset ${offset}, file may be truncated`,
		);
	}
	return data.subarray(offset, offset + length);
}

export class ChunkedUploader {
	constructor(private readonly seams: ChunkedUploaderSeams) {}

	/**
	 * Run prepare → PUT parts → finish → complete; returns the raw
	 * complete_upload response containing `file_info`
	 * (chunked_upload.py:ChunkedUploader.upload).
	 */
	async upload(opts: {
		chatType: "c2c" | "group";
		targetId: string;
		data: Buffer;
		fileType: number;
		fileName: string;
	}): Promise<Record<string, unknown>> {
		const { chatType, targetId, data, fileType, fileName } = opts;
		if (chatType !== "c2c" && chatType !== "group") {
			throw new Error(`ChunkedUploader: unsupported chat_type ${chatType}`);
		}
		const fileSize = data.length;

		// Step 1: hashes (single pass).
		const hashes = computeFileHashes(data, fileSize);

		// Step 2: upload_prepare (daily-limit typed failure).
		let prepare: PrepareResult;
		try {
			const raw = await this.seams.apiRequest(
				"POST",
				`${chatType === "c2c" ? "/v2/users" : "/v2/groups"}/${targetId}/upload_prepare`,
				{
					file_type: fileType,
					file_name: fileName,
					file_size: fileSize,
					md5: hashes.md5,
					sha1: hashes.sha1,
					md5_10m: hashes.md5_10m,
				},
				QQBOT_FILE_UPLOAD_TIMEOUT_S,
			);
			prepare = parsePrepareResponse(raw);
		} catch (err) {
			const msg = err instanceof Error ? err.message : String(err);
			if (bizCodeIn(msg, QQ_BIZ_CODE_DAILY_LIMIT)) {
				throw new UploadDailyLimitExceededError(fileName, fileSize, msg);
			}
			throw err;
		}
		const maxConcurrent = Math.max(
			1,
			Math.min(prepare.concurrency, QQ_MAX_CONCURRENT_PARTS),
		);
		const retryTimeoutS = Math.min(
			prepare.retryTimeoutS > 0
				? prepare.retryTimeoutS
				: QQ_PART_FINISH_DEFAULT_TIMEOUT_S,
			QQ_PART_FINISH_MAX_TIMEOUT_S,
		);

		// Step 3: PUT each part + notify (bounded concurrency).
		let cursor = 0;
		const runPart = async (): Promise<void> => {
			while (cursor < prepare.parts.length) {
				const part = prepare.parts[cursor++]!;
				await this.uploadOnePart({
					chatType,
					targetId,
					data,
					fileSize,
					uploadId: prepare.uploadId,
					responseBlockSize: prepare.blockSize,
					part,
					retryTimeoutS,
				});
			}
		};
		await Promise.all(Array.from({ length: maxConcurrent }, () => runPart()));

		// Step 4: complete_upload (same /files endpoint, upload_id-only body).
		return this.complete(chatType, targetId, prepare.uploadId);
	}

	private async uploadOnePart(ctx: {
		chatType: "c2c" | "group";
		targetId: string;
		data: Buffer;
		fileSize: number;
		uploadId: string;
		responseBlockSize: number;
		part: PreparePart;
		retryTimeoutS: number;
	}): Promise<void> {
		const { part } = ctx;
		// Per-part block size wins over the response-level value.
		const actualBlockSize =
			part.blockSize > 0 ? part.blockSize : ctx.responseBlockSize;
		const offset = (part.index - 1) * ctx.responseBlockSize;
		const length = Math.min(actualBlockSize, ctx.fileSize - offset);
		const chunk = readFileChunk(ctx.data, offset, length);
		const md5Hex = createHash("md5").update(chunk).digest("hex");

		await this.putToPresignedUrl(part.presignedUrl, chunk, part.index);
		await this.partFinishWithRetry({
			chatType: ctx.chatType,
			targetId: ctx.targetId,
			uploadId: ctx.uploadId,
			partIndex: part.index,
			blockSize: length,
			md5Hex,
			retryTimeoutS: ctx.retryTimeoutS,
		});
	}

	private async putToPresignedUrl(
		url: string,
		data: Buffer,
		partIndex: number,
	): Promise<void> {
		let lastErr: unknown = null;
		for (let attempt = 0; attempt <= QQ_PART_UPLOAD_MAX_RETRIES; attempt++) {
			try {
				const resp = await this.withTimeout(
					this.seams.httpPut(url, data, {
						"Content-Length": String(data.length),
					}),
					QQ_PART_UPLOAD_TIMEOUT_S * 1000,
				);
				if (resp.status >= 200 && resp.status < 300) return;
				throw new Error(
					`COS PUT returned ${resp.status}: ${resp.text?.slice(0, 200) ?? ""}`,
				);
			} catch (err) {
				lastErr = err;
				if (attempt < QQ_PART_UPLOAD_MAX_RETRIES) {
					await this.seams.sleep(1000 * 2 ** attempt);
				}
			}
		}
		throw new Error(
			`Part ${partIndex} upload failed after ${
				QQ_PART_UPLOAD_MAX_RETRIES + 1
			} attempts: ${lastErr instanceof Error ? lastErr.message : String(lastErr)}`,
		);
	}

	private async withTimeout<T>(p: Promise<T>, ms: number): Promise<T> {
		let timer: ReturnType<typeof setTimeout> | undefined;
		try {
			return await Promise.race([
				p,
				new Promise<never>((_, reject) => {
					timer = setTimeout(() => reject(new Error("request timed out")), ms);
				}),
			]);
		} finally {
			if (timer !== undefined) clearTimeout(timer);
		}
	}

	/**
	 * upload_part_finish, retrying ONLY biz_code 40093001 until the server
	 * retry_timeout elapses (chunked_upload.py:_part_finish_with_retry).
	 */
	private async partFinishWithRetry(ctx: {
		chatType: "c2c" | "group";
		targetId: string;
		uploadId: string;
		partIndex: number;
		blockSize: number;
		md5Hex: string;
		retryTimeoutS: number;
	}): Promise<void> {
		const path = `${ctx.chatType === "c2c" ? "/v2/users" : "/v2/groups"}/${ctx.targetId}/upload_part_finish`;
		const body = {
			upload_id: ctx.uploadId,
			part_index: ctx.partIndex,
			block_size: ctx.blockSize,
			md5: ctx.md5Hex,
		};
		const startMs = this.seams.monotonicMs();
		let attempts = 0;
		for (;;) {
			try {
				await this.seams.apiRequest(
					"POST",
					path,
					body,
					QQBOT_FILE_UPLOAD_TIMEOUT_S,
				);
				return;
			} catch (err) {
				const msg = err instanceof Error ? err.message : String(err);
				if (!bizCodeIn(msg, QQ_BIZ_CODE_PART_RETRYABLE)) throw err;
				const elapsedS = (this.seams.monotonicMs() - startMs) / 1000;
				if (elapsedS >= ctx.retryTimeoutS) {
					throw new Error(
						`upload_part_finish persistent retry timed out after ${Math.round(ctx.retryTimeoutS)}s (${attempts} retries): ${msg}`,
					);
				}
				attempts += 1;
				await this.seams.sleep(QQ_PART_FINISH_RETRY_INTERVAL_S * 1000);
			}
		}
	}

	/** complete_upload with exponential retry (chunked_upload.py:_complete). */
	private async complete(
		chatType: "c2c" | "group",
		targetId: string,
		uploadId: string,
	): Promise<Record<string, unknown>> {
		const path = `${chatType === "c2c" ? "/v2/users" : "/v2/groups"}/${targetId}/files`;
		const body = { upload_id: uploadId };
		let lastErr: unknown = null;
		for (
			let attempt = 0;
			attempt <= QQ_COMPLETE_UPLOAD_MAX_RETRIES;
			attempt++
		) {
			try {
				return await this.seams.apiRequest(
					"POST",
					path,
					body,
					QQBOT_FILE_UPLOAD_TIMEOUT_S,
				);
			} catch (err) {
				lastErr = err;
				if (attempt < QQ_COMPLETE_UPLOAD_MAX_RETRIES) {
					await this.seams.sleep(
						QQ_COMPLETE_UPLOAD_BASE_DELAY_S * 1000 * 2 ** attempt,
					);
				}
			}
		}
		throw new Error(
			`complete_upload failed after ${
				QQ_COMPLETE_UPLOAD_MAX_RETRIES + 1
			} attempts: ${lastErr instanceof Error ? lastErr.message : String(lastErr)}`,
		);
	}
}
