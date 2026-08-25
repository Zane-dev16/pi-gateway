// pi_platforms/yuanbao/sign-manager — sign-token acquisition for the Yuanbao
// WS gateway, ported from Hermes gateway/platforms/yuanbao.py:SignManager.
//
// Hermes anchors:
//   yuanbao.py:SignManager.TOKEN_PATH / RETRYABLE_CODE(10099) / MAX_RETRIES(3)
//   yuanbao.py:SignManager.CACHE_REFRESH_MARGIN_S(60) — treat as expiring 60s early
//   yuanbao.py:SignManager.compute_signature — HMAC-SHA256 over
//     nonce+timestamp+app_key+app_secret, keyed by app_secret
//   yuanbao.py:SignManager.build_timestamp — Beijing ISO-8601 (+08:00, no ms)
//   yuanbao.py:SignManager.get_token/force_refresh — per-app_key singleflight

import { createHmac, randomBytes } from "node:crypto";

export const TOKEN_PATH = "/api/v5/robotLogic/sign-token";
export const SIGN_RETRYABLE_CODE = 10099;
export const SIGN_MAX_RETRIES = 3;
export const SIGN_RETRY_DELAY_S = 1.0;
export const CACHE_REFRESH_MARGIN_S = 60;

export interface SignTokenData {
	token: string;
	bot_id: string;
	duration: number;
	expire_ts: number; // epoch seconds
}

export interface SignHttpSeam {
	postJson(
		url: string,
		payload: Record<string, unknown>,
		headers: Record<string, string>,
	): Promise<{ status: number; body: Record<string, unknown> }>;
	sleepMs?: ((ms: number) => Promise<void>) | undefined;
	nowMs?: (() => number) | undefined;
}

/** HMAC-SHA256 signature (yuanbao.py:SignManager.compute_signature). */
export function computeSignature(
	nonce: string,
	timestamp: string,
	appKey: string,
	appSecret: string,
): string {
	const plain = nonce + timestamp + appKey + appSecret;
	return createHmac("sha256", appSecret).update(plain).digest("hex");
}

/** Beijing-time ISO-8601 stamp without milliseconds (SignManager.build_timestamp). */
export function buildBeijingTimestamp(nowMs?: number): string {
	const ms = nowMs ?? Date.now();
	const bj = new Date(ms + 8 * 3600 * 1000);
	return bj.toISOString().slice(0, 19) + "+08:00";
}

interface CacheEntry extends SignTokenData {}

/**
 * Process-level token manager (per-instance in the port; class-level shared
 * state upstream — the kit registration path constructs one adapter).
 */
export class SignManager {
	private readonly cache = new Map<string, CacheEntry>();
	private readonly locks = new Map<string, Promise<SignTokenData>>();
	private readonly http: SignHttpSeam;

	constructor(http: SignHttpSeam) {
		this.http = http;
	}

	isCacheValid(entry: CacheEntry): boolean {
		const nowS = (this.http.nowMs?.() ?? Date.now()) / 1000;
		return entry.expire_ts - nowS > CACHE_REFRESH_MARGIN_S;
	}

	async get(opts: {
		appKey: string;
		appSecret: string;
		apiDomain: string;
		routeEnv?: string | undefined;
	}): Promise<SignTokenData> {
		this.purgeExpired();
		const cached = this.cache.get(opts.appKey);
		if (cached !== undefined && this.isCacheValid(cached)) return { ...cached };

		const existing = this.locks.get(opts.appKey);
		if (existing !== undefined) return existing; // singleflight

		const flight = this.fetchAndCache(opts).finally(() => {
			this.locks.delete(opts.appKey);
		});
		this.locks.set(opts.appKey, flight);
		return flight;
	}

	async forceRefresh(opts: {
		appKey: string;
		appSecret: string;
		apiDomain: string;
		routeEnv?: string | undefined;
	}): Promise<SignTokenData> {
		this.cache.delete(opts.appKey);
		return this.fetchAndCache(opts);
	}

	clearLocks(): void {
		this.locks.clear();
	}

	purgeExpired(): number {
		const nowS = (this.http.nowMs?.() ?? Date.now()) / 1000;
		let purged = 0;
		for (const [k, v] of this.cache) {
			if (nowS - v.expire_ts > 0) {
				this.cache.delete(k);
				purged += 1;
			}
		}
		return purged;
	}

	private async fetchAndCache(opts: {
		appKey: string;
		appSecret: string;
		apiDomain: string;
		routeEnv?: string | undefined;
	}): Promise<SignTokenData> {
		const data = await SignManager.fetch(this.http, opts);
		const duration = Number(data.duration ?? 0);
		const expireTs =
			(this.http.nowMs?.() ?? Date.now()) / 1000 +
			(duration > 0 ? duration : 3600);
		const entry: CacheEntry = { ...data, expire_ts: expireTs };
		this.cache.set(opts.appKey, entry);
		return { ...entry };
	}

	/** Sign-ticket HTTP request w/ auto-retry on code 10099 (SignManager.fetch). */
	static async fetch(
		http: SignHttpSeam,
		opts: {
			appKey: string;
			appSecret: string;
			apiDomain: string;
			routeEnv?: string | undefined;
		},
	): Promise<{ token: string; bot_id: string; duration: number }> {
		const url = `${opts.apiDomain.replace(/\/+$/, "")}${TOKEN_PATH}`;
		for (let attempt = 0; attempt <= SIGN_MAX_RETRIES; attempt++) {
			const nonce = randomBytes(16).toString("hex");
			const timestamp = buildBeijingTimestamp(http.nowMs?.());
			const signature = computeSignature(
				nonce,
				timestamp,
				opts.appKey,
				opts.appSecret,
			);
			const resp = await http.postJson(
				url,
				{
					app_key: opts.appKey,
					nonce,
					signature,
					timestamp,
				},
				{
					"Content-Type": "application/json",
					...(opts.routeEnv !== undefined && opts.routeEnv !== ""
						? { "X-Route-Env": opts.routeEnv }
						: {}),
				},
			);
			if (resp.status !== 200) {
				throw new Error(
					`Sign token API returned ${resp.status}: ${JSON.stringify(resp.body).slice(0, 200)}`,
				);
			}
			const code = Number(resp.body["code"] ?? -1);
			if (code === 0) {
				const data = resp.body["data"];
				if (data === null || typeof data !== "object") {
					throw new Error("Sign token response missing 'data' field");
				}
				const rec = data as Record<string, unknown>;
				return {
					token: String(rec["token"] ?? ""),
					bot_id: String(rec["bot_id"] ?? ""),
					duration: Number(rec["duration"] ?? 0),
				};
			}
			if (code === SIGN_RETRYABLE_CODE && attempt < SIGN_MAX_RETRIES) {
				await (http.sleepMs?.(SIGN_RETRY_DELAY_S * 1000) ??
					new Promise<void>((r) => setTimeout(r, SIGN_RETRY_DELAY_S * 1000)));
				continue;
			}
			throw new Error(
				`Sign token error: code=${code}, msg=${String(resp.body["msg"] ?? "")}`,
			);
		}
		throw new Error("Sign token failed: max retries exceeded");
	}
}
