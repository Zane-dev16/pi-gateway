// pi_gateway/security/trust/session-headers — api_server OPT-IN session
// continuity + memory-scoping header enforcement (06 §8.4; DEC-017
// "api_server opt-in session headers (X-Hermes-Session-Id/Key)").
//
// Ported from the READ-ONLY Hermes reference:
//   gateway/platforms/api_server.py:_extract_session_key (@~2258) — the
//     session key scopes long-term memory and REQUIRES API-key auth:
//     rejected otherwise, NEVER anonymous ("an unauthenticated client on a
//     local-only server can't inject itself into another user's long-term
//     memory scope by guessing a key").
//   api_server.py control-character gate — [\r\n\x00] rejects (header-
//     injection defense on the echo path), then _MAX_SESSION_HEADER_LEN=256.
//
// Absence of BOTH headers is the stateless default: opt-in means nothing is
// adopted unless the caller sends it.

/** api_server.py:2255 `_MAX_SESSION_HEADER_LEN`. */
export const MAX_SESSION_HEADER_LEN = 256;

export const SESSION_ID_HEADER = "x-hermes-session-id";
export const SESSION_KEY_HEADER = "x-hermes-session-key";

/** Lowercased header map (the server layer normalizes once). */
export type HeaderMap = Record<string, string>;

/** Key-only verdict (api_server.py:_parse_session_key_header parity). */
export type SessionKeyVerdict =
	| {
			ok: true;
			/** Adopted memory scope key; null when the header is absent/blank. */
			sessionKey: string | null;
	  }
	| {
			ok: false;
			status: 400 | 403;
			error: string;
			errorType: "invalid_request_error" | "gateway_auth_error";
	  };

/**
 * The X-Hermes-Session-Key ladder ALONE (api_server.py:_parse_session_key_header
 * @2296): blank/absent ⇒ adopted nothing; no API key configured ⇒ 403 (memory
 * scoping is NEVER anonymous); [\r\n\x00] ⇒ 400 "Invalid session key";
 * over-length ⇒ 400 "Session key too long". Lanes that honor ONLY the memory
 * header (POST /v1/runs — Hermes does not parse the id there) compose this;
 * the full pair helper below reuses it for its key half.
 */
export function extractSessionKeyHeader(
	headers: HeaderMap,
	opts: { apiKeyConfigured: boolean },
): SessionKeyVerdict {
	const rawKey = headers[SESSION_KEY_HEADER];
	if (typeof rawKey !== "string") return { ok: true, sessionKey: null };
	const key = rawKey.trim();
	if (key.length === 0) return { ok: true, sessionKey: null };
	if (!opts.apiKeyConfigured) {
		return fail(
			403,
			"gateway_auth_error",
			"X-Hermes-Session-Key requires API key authentication. " +
				"Configure API_SERVER_KEY to enable this feature.",
		);
	}
	if (HEADER_INJECTION_CHARS.test(key)) {
		return fail(400, "invalid_request_error", "Invalid session key");
	}
	if (key.length > MAX_SESSION_HEADER_LEN) {
		return fail(400, "invalid_request_error", "Session key too long");
	}
	return { ok: true, sessionKey: key };
}

export type SessionHeadersVerdict =
	| {
			ok: true;
			/** Adopted session id (opt-in continuity); null when absent. */
			sessionId: string | null;
			/** Adopted memory scope key; null when absent. */
			sessionKey: string | null;
	  }
	| {
			ok: false;
			status: 400 | 403;
			error: string;
			/** OpenAI-style error family (api_server.py _openai_error parity). */
			errorType: "invalid_request_error" | "gateway_auth_error";
	  };

function fail(
	status: 400 | 403,
	errorType: "invalid_request_error" | "gateway_auth_error",
	error: string,
): SessionHeadersVerdict {
	return { ok: false, status, error, errorType };
}

const HEADER_INJECTION_CHARS = /[\r\n\x00]/;

/**
 * Enforce the opt-in pair against an API-key-configured flag. The KEY check
 * runs FIRST (a key without API-key auth rejects even when the id would
 * validate): no API key configured ⇒ 403 gateway_auth_error; injection
 * characters ⇒ 400; over-length ⇒ 400. Session-id hygiene mirrors the key's
 * (same echo path, same caps) — smallest Hermes-consistent behavior.
 */
export function extractOptInSessionHeaders(
	headers: HeaderMap,
	opts: { apiKeyConfigured: boolean },
): SessionHeadersVerdict {
	const rawKey = headers[SESSION_KEY_HEADER];
	const rawId = headers[SESSION_ID_HEADER];

	// The KEY gate runs FIRST (a key without API-key auth rejects even when
	// the id would validate) — same core as the standalone key-only lane.
	const keyVerdict = extractSessionKeyHeader(headers, opts);
	if (!keyVerdict.ok) return keyVerdict;
	const sessionKey = keyVerdict.sessionKey;

	let sessionId: string | null = null;
	if (typeof rawId === "string" && rawId.trim().length > 0) {
		const id = rawId.trim();
		if (HEADER_INJECTION_CHARS.test(id)) {
			return fail(400, "invalid_request_error", "Invalid session id");
		}
		if (id.length > MAX_SESSION_HEADER_LEN) {
			return fail(400, "invalid_request_error", "Session id too long");
		}
		sessionId = id;
	}

	return { ok: true, sessionId, sessionKey };
}
