// pi_platforms/a2a/security — A2A security primitives ported from the
// READ-ONLY Hermes plugins/platforms/a2a/security.py. Shared by the inbound
// adapter; every function takes an INJECTED env reader (scoped, never
// process.env directly) so the identity/trust matrix is testable without
// mutating process state.
//
// Threat model (source docstring): A2A is a NETWORK surface — inbound text is
// adversarial (injection filters), outbound text may leak credentials
// (redaction), push callbacks must not become SSRF probes.
//
// Layer parity:
//   1. Bind safety       — no token ⇒ 127.0.0.1 only (resolve_bind_host)
//   2. Peer identity     — per-peer tokens map to authenticated identities;
//                          shared token falls back to ip:<addr>
//   3. Injection filters — defang ChatML/role-prefix/override markers
//   4. Outbound redaction— scrub credential-shaped strings
//   5. Trusted peers     — optional allow-list over AUTHENTICATED identities
//   6. Push auth         — HMAC-SHA256 signing + SSRF-safe callback URLs

import { createHmac } from "node:crypto";

import { secureCompare } from "../kit/trust.js";

import {
	ENV_ALLOW_ALL_USERS,
	ENV_BEARER_TOKEN,
	ENV_HOST,
	ENV_PEER_TOKENS,
	ENV_PUSH_SECRET,
	ENV_TRUSTED_PEERS,
	INJECTION_PATTERNS,
	INJECTION_REPLACEMENT,
	LOOPBACK_HOSTS,
	PRIVACY_PREFIX,
	REDACTION_PATTERNS,
	SSRF_BLOCKED_PREFIXES,
	SSRF_LOOPBACK_EXCEPTION_PREFIXES,
} from "./manifest.js";

/** Scoped env reader seam (os.getenv parity; injected for tests). */
export type EnvReader = (name: string) => string | undefined;

/** security.py:get_bearer_token — shared inbound token ('' when unset). */
export function getBearerToken(env: EnvReader): string {
	return (env(ENV_BEARER_TOKEN) ?? "").trim();
}

/**
 * security.py:get_peer_tokens — parse A2A_PEER_TOKENS
 * ("alice:tok1,bob:tok2") into {token → peer_name}. Later duplicate tokens
 * overwrite earlier ones (dict-assignment parity).
 */
export function getPeerTokens(env: EnvReader): Map<string, string> {
	const raw = (env(ENV_PEER_TOKENS) ?? "").trim();
	const out = new Map<string, string>();
	for (const pair of raw.split(",")) {
		const trimmedPair = pair.trim();
		if (!trimmedPair || !trimmedPair.includes(":")) continue;
		const colonAt = trimmedPair.indexOf(":");
		const name = trimmedPair.slice(0, colonAt).trim();
		const token = trimmedPair.slice(colonAt + 1).trim();
		if (name && token) out.set(token, name);
	}
	return out;
}

/** security.py:_parse_bearer — "Bearer <token>" scheme-insensitive. */
export function parseBearer(
	authHeader: string | null | undefined,
): string | null {
	if (!authHeader) return null;
	const parts = authHeader.split(/\s+/, 2);
	if (parts.length !== 2 || (parts[0] ?? "").toLowerCase() !== "bearer") {
		return null;
	}
	return (parts[1] ?? "").trim();
}

/**
 * security.py:authenticate — identity rules EXACT:
 *   - no credentials configured (localhost-only mode): identity ip:<addr>
 *     (ip:local when the socket address is unknown)
 *   - presented credential matches a peer entry: that peer's NAME
 *   - presented credential matches the shared bearer value: identity
 *       ip:<addr>, with an ip:unknown fallback
 *   - otherwise None (caller rejects with 401 ERR_UNAUTHORIZED)
 * Comparisons are CONSTANT-TIME via kit secureCompare (byte semantics of
 * hmac.compare_digest).
 */
export function authenticate(
	authHeader: string | null | undefined,
	clientIp: string,
	env: EnvReader,
): string | null {
	const peerTokens = getPeerTokens(env);
	const shared = getBearerToken(env);
	if (peerTokens.size === 0 && !shared) {
		return `ip:${clientIp || "local"}`;
	}
	const presented = parseBearer(authHeader);
	if (presented === null) return null;
	for (const [token, name] of peerTokens) {
		if (secureCompare(presented, token)) return name;
	}
	if (shared && secureCompare(presented, shared)) {
		return `ip:${clientIp || "unknown"}`;
	}
	return null;
}

/** security.py:localhost_only — true when NO token of any kind is set. */
export function localhostOnly(env: EnvReader): boolean {
	return !(getBearerToken(env) || getPeerTokens(env).size > 0);
}

export interface BindHostResolution {
	host: string;
	/** Present iff a requested widening was IGNORED (no-token warning path). */
	warning: string | undefined;
	/** True iff the operator EXPLICITLY requested a non-loopback host. */
	widenedRequested: boolean;
}

/**
 * security.py:resolve_bind_host — localhost unless the operator BOTH
 * configured a token AND explicitly asked for a wider host. A token alone
 * does not widen the bind; opting into remote exposure must be deliberate.
 */
export function resolveBindHost(
	env: EnvReader,
	onWarning?: ((message: string) => void) | undefined,
): BindHostResolution {
	const requested = (env(ENV_HOST) ?? "").trim() || "127.0.0.1";
	if (LOOPBACK_HOSTS.has(requested)) {
		return { host: requested, warning: undefined, widenedRequested: false };
	}
	if (localhostOnly(env)) {
		const warning =
			`A2A: A2A_HOST=${requested} ignored — no A2A_BEARER_TOKEN or A2A_PEER_TOKENS ` +
			"set; binding to 127.0.0.1. Configure a token to expose A2A remotely.";
		onWarning?.(warning);
		return { host: "127.0.0.1", warning, widenedRequested: true };
	}
	return { host: requested, warning: undefined, widenedRequested: true };
}

/** security.py:get_trusted_peers — env csv allow-list (config.yaml lane excluded). */
export function getTrustedPeers(env: EnvReader): Set<string> {
	const raw = (env(ENV_TRUSTED_PEERS) ?? "").trim();
	const out = new Set<string>();
	if (raw) {
		for (const peer of raw.split(",")) {
			const trimmed = peer.trim();
			if (trimmed) out.add(trimmed);
		}
	}
	return out;
}

/**
 * security.py:is_trusted_peer — open under A2A_ALLOW_ALL_USERS or in
 * localhost-only mode; with a configured allow-list the identity must be on
 * it; an EMPTY allow-list admits any AUTHENTICATED identity.
 */
export function isTrustedPeer(identity: string, env: EnvReader): boolean {
	const allowAll = (env(ENV_ALLOW_ALL_USERS) ?? "").trim().toLowerCase();
	if (allowAll === "1" || allowAll === "true" || allowAll === "yes")
		return true;
	if (localhostOnly(env)) return true;
	const trusted = getTrustedPeers(env);
	if (trusted.size === 0) return true;
	return trusted.has(identity);
}

/** security.py:filter_inbound — defang prompt-injection markers. */
export function filterInbound(text: string): string {
	if (!text) return text;
	let cleaned = text;
	for (const pattern of INJECTION_PATTERNS) {
		cleaned = cleaned.replace(pattern, INJECTION_REPLACEMENT);
	}
	return cleaned;
}

/** Python repr('alice') === 'alice' (single quotes) for the prefix slot. */
function pythonRepr(value: string): string {
	return `'${value}'`;
}

/**
 * security.py:wrap_inbound — EVERY inbound message is filtered and framed —
 * including text starting with "/". Remote peers must NEVER reach the
 * gateway's operator slash commands; a peer wanting an action asks in
 * natural language and the agent decides.
 */
export function wrapInbound(peer: string, text: string): string {
	const framedPeer = pythonRepr(peer || "unknown");
	return (
		PRIVACY_PREFIX.replace("{peer}", framedPeer) +
		filterInbound((text || "").trim())
	);
}

/** security.py:redact_outbound — ordered sequential substitution passes. */
export function redactOutbound(text: string): string {
	if (!text) return text;
	let out = text;
	for (const [pattern, replacement] of REDACTION_PATTERNS) {
		out = out.replace(pattern, replacement);
	}
	return out;
}

/** security.py:get_push_secret — dedicated secret falls back to bearer token. */
export function getPushSecret(env: EnvReader): string {
	const secret = (env(ENV_PUSH_SECRET) ?? "").trim();
	if (secret) return secret;
	return getBearerToken(env);
}

/** json.dumps(payload, sort_keys=True, ensure_ascii=False) — sorted-key compact JSON. */
export function sortKeysJson(value: unknown): string {
	return JSON.stringify(sortKeysDeep(value));
}

function sortKeysDeep(value: unknown): unknown {
	if (Array.isArray(value)) return value.map(sortKeysDeep);
	if (value !== null && typeof value === "object") {
		const out: Record<string, unknown> = {};
		for (const key of Object.keys(value as Record<string, unknown>).sort()) {
			out[key] = sortKeysDeep((value as Record<string, unknown>)[key]);
		}
		return out;
	}
	return value;
}

/**
 * security.py:sign_push_payload — HMAC-SHA256 over the sorted-keys JSON body;
 * hex digest. Empty string when no secret configured (unsigned mode).
 */
export function signPushPayload(
	payload: Record<string, unknown>,
	secret: string,
): string {
	if (!secret) return "";
	return createHmac("sha256", secret)
		.update(sortKeysJson(payload))
		.digest("hex");
}

/**
 * security.py:is_safe_callback_url — SSRF ladder EXACT:
 *   scheme http/https only → hostname required → "localhost" allowed iff
 *   localhost-only → blocked-prefix table (loopback prefixes conditionally
 *   admitted in localhost-only mode) → IP-literal loopback/link-local/
 *   private/reserved blocked unless loopback + localhost-only → else safe.
 */
export function isSafeCallbackUrl(
	url: string,
	env: EnvReader,
	localOnlyOverride?: boolean | undefined,
): boolean {
	if (!url || typeof url !== "string") return false;
	let parsed: URL;
	try {
		parsed = new URL(url);
	} catch {
		return false;
	}
	const scheme = parsed.protocol.replace(/:$/, "").toLowerCase();
	if (scheme !== "http" && scheme !== "https") return false;
	// URL.hostname lowercases and brackets IPv6 literals — strip brackets.
	const hostname = parsed.hostname.replace(/^\[|\]$/g, "");
	if (!hostname) return false;
	const hostnameLower = hostname.toLowerCase();
	const localOnly = localOnlyOverride ?? localhostOnly(env);
	if (hostnameLower === "localhost") {
		// Loopback callbacks only make sense for local testing.
		return localOnly;
	}
	for (const prefix of SSRF_BLOCKED_PREFIXES) {
		if (hostnameLower.startsWith(prefix.toLowerCase())) {
			if (localOnly && SSRF_LOOPBACK_EXCEPTION_PREFIXES.has(prefix))
				return true;
			return false;
		}
	}
	if (isBlockedIpLiteral(hostnameLower, localOnly)) return false;
	return true;
}

/** ipaddress module ladder for bare IP-literal hosts. */
function isBlockedIpLiteral(hostname: string, localOnly: boolean): boolean {
	const v4 = /^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/.exec(hostname);
	let loopback = false;
	let linkLocal = false;
	let isPrivate = false;
	let reserved = false;
	if (v4 !== null) {
		const octets = v4.slice(1).map((o) => Number(o));
		if (octets.some((o) => o > 255)) return false; // invalid literal — not an IP, treat as hostname
		const [a, b] = octets as [number, number];
		loopback = a === 127;
		linkLocal = a === 169 && b === 254;
		isPrivate =
			a === 10 || (a === 172 && b >= 16 && b <= 31) || (a === 192 && b === 168);
		reserved = a === 0 || a >= 240;
	} else if (hostname.includes(":")) {
		const lowered = hostname.toLowerCase();
		loopback = lowered === "::1";
		linkLocal = lowered.startsWith("fe80");
		isPrivate =
			lowered.startsWith("fc") ||
			lowered.startsWith("fd") ||
			lowered.startsWith("fec0"); // unique-local (+ deprecated site-local)
		reserved = lowered.startsWith("ff");
	} else {
		return false; // not an IP literal — it's a hostname, fine
	}
	if (loopback || linkLocal || isPrivate || reserved) {
		if (localOnly && loopback) return false; // admitted — NOT blocked
		return true; // blocked
	}
	return false;
}
