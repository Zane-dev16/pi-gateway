// pi_platforms/qqbot/onboard — QQBot scan-to-configure QR registration,
// ported from Hermes gateway/platforms/qqbot/onboard.py (+ constants.py).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   onboard.py:_create_bind_task  — POST {portal}/lite/create_bind_task
//     {key: base64-aes-key} with get_api_headers(); retcode≠0 ⇒ error;
//     data.task_id carries the bind task id.
//   onboard.py:_poll_bind_result  — POST {portal}/lite/poll_bind_result
//     {task_id}; returns BindStatus + bot_appid + bot_encrypt_secret +
//     user_openid.
//   onboard.py:build_connect_url / QR_URL_TEMPLATE — the QR target URL.
//   onboard.py:qr_register        — create → poll (2s interval, shared
//     deadline) → COMPLETED decrypts the secret locally; EXPIRED refreshes
//     the task up to _MAX_REFRESHES=3; deadline exhaust ⇒ null.
//
// Crypto rides the ALREADY contract-tested primitives in crypto.ts
// (generate_bind_key / decrypt_secret): the payload shape is exactly
// base64(IV[12] ‖ ciphertext ‖ tag[16]) AES-256-GCM — no new crypto invented.
// The portal host honors the QQ_PORTAL_HOST optionalEnv (constants.py:
// PORTAL_HOST) so corporate proxies/test environments can redirect.

import type { QQRestTransport } from "./qqbot-adapter.js";
import { decryptSecret, generateBindKey } from "./crypto.js";
import {
	QQ_BIND_STATUS_COMPLETED,
	QQ_BIND_STATUS_EXPIRED,
	QQ_ONBOARD_API_TIMEOUT_S,
	QQ_ONBOARD_CREATE_PATH,
	QQ_ONBOARD_MAX_REFRESHES,
	QQ_ONBOARD_POLL_INTERVAL_S,
	QQ_ONBOARD_POLL_PATH,
	QQ_PORTAL_HOST_DEFAULT,
	QQ_QR_CONNECT_URL_TEMPLATE,
	QQBOT_USER_AGENT,
} from "./manifest.js";

/** Bind task status codes (onboard.py:BindStatus). */
export const QQ_BIND_STATUS = {
	NONE: 0,
	PENDING: 1,
	COMPLETED: 2,
	EXPIRED: 3,
} as const;

/**
 * Portal REST headers (utils.py:get_api_headers parity): q.qq.com REQUIRES
 * Accept: application/json and an identifying User-Agent — without them the
 * server answers a JavaScript anti-bot challenge page.
 */
export function portalApiHeaders(): Record<string, string> {
	return {
		"Content-Type": "application/json",
		Accept: "application/json",
		"User-Agent": QQBOT_USER_AGENT,
	};
}

export interface QrRegisterCredentials {
	appId: string;
	clientSecret: string;
	userOpenid: string;
}

export interface CreateBindTaskResult {
	taskId: string;
	aesKey: string;
}

function asRecord(value: unknown): Record<string, unknown> {
	return value !== null && typeof value === "object"
		? (value as Record<string, unknown>)
		: {};
}

/**
 * Per-leg onboard HTTP timeout (onboard.py httpx.Client(timeout=…)): a hung
 * portal leg raises 'onboard request timed out' instead of stalling the QR
 * flow past its own deadline budget.
 */
async function withOnboardTimeout<T>(
	p: Promise<T>,
	timeoutS: number,
	label: string,
): Promise<T> {
	const timer = new Promise<never>((_, reject) => {
		setTimeout(
			() => reject(new Error(`onboard request timed out [${label}]`)),
			timeoutS * 1000,
		);
	});
	timer.catch(() => undefined); // losing timer never rejects unhandled
	return Promise.race([p, timer]);
}

/** onboard.py:_create_bind_task — create a bind task; *(task_id, aes_key)*. */
export async function createBindTask(
	rest: QQRestTransport,
	opts: { portalHost?: string | undefined; timeoutS?: number | undefined } = {},
): Promise<CreateBindTaskResult> {
	const aesKey = generateBindKey();
	const url = `https://${opts.portalHost ?? QQ_PORTAL_HOST_DEFAULT}${QQ_ONBOARD_CREATE_PATH}`;
	// onboard.py — ONBOARD_API_TIMEOUT per HTTP leg.
	const resp = await withOnboardTimeout(
		rest.request("POST", url, { key: aesKey }, portalApiHeaders()),
		opts.timeoutS ?? QQ_ONBOARD_API_TIMEOUT_S,
		QQ_ONBOARD_CREATE_PATH,
	);
	if (resp.status >= 400) {
		throw new Error(`create_bind_task failed: HTTP ${resp.status}`);
	}
	if (Number(resp.body["retcode"] ?? 0) !== 0) {
		throw new Error(String(resp.body["msg"] ?? "create_bind_task failed"));
	}
	const taskId = String(asRecord(resp.body["data"])["task_id"] ?? "");
	if (taskId === "") {
		throw new Error("create_bind_task: missing task_id in response");
	}
	return { taskId, aesKey };
}

export interface PollBindResult {
	status: number;
	botAppid: string;
	encryptedSecret: string;
	userOpenid: string;
}

/** onboard.py:_poll_bind_result — poll one bind task. */
export async function pollBindResult(
	rest: QQRestTransport,
	taskId: string,
	opts: { portalHost?: string | undefined; timeoutS?: number | undefined } = {},
): Promise<PollBindResult> {
	const url = `https://${opts.portalHost ?? QQ_PORTAL_HOST_DEFAULT}${QQ_ONBOARD_POLL_PATH}`;
	// onboard.py — ONBOARD_API_TIMEOUT per HTTP leg.
	const resp = await withOnboardTimeout(
		rest.request("POST", url, { task_id: taskId }, portalApiHeaders()),
		opts.timeoutS ?? QQ_ONBOARD_API_TIMEOUT_S,
		QQ_ONBOARD_POLL_PATH,
	);
	if (resp.status >= 400) {
		throw new Error(`poll_bind_result failed: HTTP ${resp.status}`);
	}
	if (Number(resp.body["retcode"] ?? 0) !== 0) {
		throw new Error(String(resp.body["msg"] ?? "poll_bind_result failed"));
	}
	const d = asRecord(resp.body["data"]);
	return {
		status: Number(d["status"] ?? QQ_BIND_STATUS.NONE),
		botAppid: String(d["bot_appid"] ?? ""),
		encryptedSecret: String(d["bot_encrypt_secret"] ?? ""),
		userOpenid: String(d["user_openid"] ?? ""),
	};
}

/** onboard.py:build_connect_url — the QR-code target URL for a task id. */
export function buildConnectUrl(taskId: string): string {
	return QQ_QR_CONNECT_URL_TEMPLATE.replace(
		"{task_id}",
		encodeURIComponent(taskId),
	);
}

export interface QrRegisterOptions {
	/** Portal host override (QQ_PORTAL_HOST optionalEnv parity). */
	portalHost?: string | undefined;
	/** Overall budget shared across refreshes (onboard.py timeout_seconds). */
	timeoutS?: number | undefined;
	nowMs?: (() => number) | undefined;
	sleepMs?: ((ms: number) => Promise<void>) | undefined;
	pollIntervalS?: number | undefined;
}

/**
 * onboard.py:qr_register — the full flow: create → poll → decrypt. Returns
 * the decrypted credentials on success, or null on failure / expiry
 * exhaustion / deadline. The client_secret NEVER travels plaintext: the
 * server encrypts it under THIS caller's generated key and decryption
 * happens locally (crypto.ts:decryptSecret).
 */
export async function qrRegister(
	rest: QQRestTransport,
	opts: QrRegisterOptions = {},
): Promise<QrRegisterCredentials | null> {
	const timeoutS = opts.timeoutS ?? 600;
	const nowMs = opts.nowMs ?? (() => Date.now());
	const sleepMs =
		opts.sleepMs ?? ((ms) => new Promise<void>((r) => setTimeout(r, ms)));
	const pollIntervalMs =
		(opts.pollIntervalS ?? QQ_ONBOARD_POLL_INTERVAL_S) * 1000;
	const deadlineMs = nowMs() + timeoutS * 1000;

	for (
		let refreshCount = 0;
		refreshCount <= QQ_ONBOARD_MAX_REFRESHES;
		refreshCount++
	) {
		let taskId: string;
		let aesKey: string;
		try {
			const created = await createBindTask(rest, {
				...(opts.portalHost !== undefined
					? { portalHost: opts.portalHost }
					: {}),
			});
			taskId = created.taskId;
			aesKey = created.aesKey;
		} catch {
			// "[QQBot onboard] Failed to create bind task"
			return null;
		}

		let refreshedForExpiry = false;
		while (nowMs() < deadlineMs) {
			let poll: PollBindResult;
			try {
				poll = await pollBindResult(rest, taskId, {
					...(opts.portalHost !== undefined
						? { portalHost: opts.portalHost }
						: {}),
				});
			} catch {
				await sleepMs(pollIntervalMs); // transient poll faults just skip a beat
				continue;
			}
			if (poll.status === QQ_BIND_STATUS_COMPLETED) {
				return {
					appId: poll.botAppid,
					clientSecret: decryptSecret(poll.encryptedSecret, aesKey),
					userOpenid: poll.userOpenid,
				};
			}
			if (poll.status === QQ_BIND_STATUS_EXPIRED) {
				if (refreshCount >= QQ_ONBOARD_MAX_REFRESHES) {
					// "QR code expired _MAX_REFRESHES times — giving up"
					return null;
				}
				refreshedForExpiry = true;
				break; // next outer iteration creates a fresh task
			}
			await sleepMs(pollIntervalMs);
		}
		if (!refreshedForExpiry) break; // deadline reached without completing
	}
	return null;
}

/** Default onboard API timeout (constants.py:ONBOARD_API_TIMEOUT), re-exported for callers. */
export const QQ_ONBOARD_TIMEOUT_S = QQ_ONBOARD_API_TIMEOUT_S;
