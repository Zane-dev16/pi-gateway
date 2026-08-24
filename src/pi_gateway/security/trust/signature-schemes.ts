// pi_gateway/security/trust/signature-schemes — THE canonical signature
// scheme registry: schemes are DATA (header names, algorithm, timestamp
// tolerance, replay binding) that adapters configure with their manifest,
// per DEC-017 ("feature flags alone are NOT a specification of the trust
// boundary"; "one centralized validator for all schemes" was REJECTED —
// scheme-specific timestamp/skew/handler semantics differ legitimately).
//
// Ported from the READ-ONLY Hermes reference
// (gateway/platforms/webhook.py:_validate_signature @1066 and
// _validate_svix_signature @1197; anchors cited, never vendored):
//
// | scheme        | detection headers                    | signed content      | replay            |
// |---------------|--------------------------------------|---------------------|-------------------|
// | svix-v1       | svix-id / svix-timestamp / signature | {id}.{ts}.{body}    | ±300 s skew       |
// | linear        | linear-signature                     | body hex HMAC       | NONE (vendor)     |
// | github-x-hub… | X-Hub-Signature-256                  | sha256=<hex>        | none              |
// | gitlab        | X-Gitlab-Token                       | plain secret        | none              |
// | hmac-v2       | X-Webhook-Signature-V2 (+timestamp)  | <ts>.<body> hex     | ±300 s window     |
// | legacy-v1     | X-Webhook-Signature                  | body hex HMAC       | none (deprecated) |
//
// Selection is BY HEADER PRESENCE, first match COMMITS (no fallthrough):
// a present-but-invalid signature NEVER falls through to another scheme.
// V2 anti-downgrade: presenting X-Webhook-Signature-V2 COMMITS to V2 — a
// missing/malformed/expired timestamp REJECTS rather than resurrecting the
// replayable legacy V1 branch. Secret configured + no recognized header ⇒
// REJECT (fail-closed).

import { createHmac } from "node:crypto";
import { compareAfterCompute, constantTimeEqual } from "./constant-time.js";
import type { BoundedSeenSet } from "./replay-seen-set.js";

export type SignatureSchemeId =
	| "svix-v1"
	| "linear"
	| "github-x-hub-signature-256"
	| "gitlab"
	| "hmac-v2-generic"
	| "hmac-v1-generic-legacy";

/** Digest presentation over the wire. */
export type SignatureEncoding = "hex" | "base64" | "plain-token";

/** How the signing key derives from the configured shared secret. */
export type KeyMaterial = "raw-secret" | "whsec-base64";

/** Byte-exact signed-content construction (over the RAW body bytes). */
export type SignedContentShape = "body" | "ts.body" | "id.ts.body";

/**
 * One scheme's trust data. `skewSeconds === null` means the vendor-documented
 * ABSENCE of a timestamp binding (Linear #87348 parity): absence is Linear's
 * scheme, not an omission — replay defense then belongs to the delivery-id
 * idempotency layer, not to this validator.
 */
export interface SignatureSchemeData {
	readonly id: SignatureSchemeId;
	/** Lowercased headers whose PRESENCE selects this scheme (any-of). */
	readonly detectionHeaders: readonly string[];
	/** Headers REQUIRED once committed (missing ⇒ reject; V2 anti-downgrade). */
	readonly requiredHeaders: readonly string[];
	readonly signedContent: SignedContentShape;
	readonly encoding: SignatureEncoding;
	/** Prefix carried on the PRESENTED value by the trusted side. */
	readonly presentedPrefix: string;
	/** The header that CARRIES the signature/token material (replay key). */
	readonly signatureHeader: string;
	readonly keyMaterial: KeyMaterial;
	/** Replay-window seconds in either direction; null = no binding. */
	readonly skewSeconds: number | null;
	/** Space-separated rotation entries "v1,<sig>" accepted (Svix). */
	readonly rotationEntries: boolean;
	readonly deprecated: boolean;
}

/** webhook.py tolerance_seconds=300 / V2 replay window (@300 s both). */
const SKEW_SECONDS = 300;

/** The registry, IN DISPATCH ORDER (first-presented-wins = array order). */
export const SIGNATURE_SCHEMES: readonly SignatureSchemeData[] = [
	{
		id: "svix-v1",
		detectionHeaders: ["svix-id", "svix-timestamp", "svix-signature"],
		requiredHeaders: ["svix-timestamp", "svix-signature"],
		signedContent: "id.ts.body",
		encoding: "base64",
		presentedPrefix: "",
		signatureHeader: "svix-signature",
		keyMaterial: "whsec-base64",
		skewSeconds: SKEW_SECONDS,
		rotationEntries: true,
		deprecated: false,
	},
	{
		id: "linear",
		detectionHeaders: ["linear-signature"],
		requiredHeaders: [],
		signedContent: "body",
		encoding: "hex",
		presentedPrefix: "",
		signatureHeader: "linear-signature",
		keyMaterial: "raw-secret",
		skewSeconds: null,
		rotationEntries: false,
		deprecated: false,
	},
	{
		id: "github-x-hub-signature-256",
		detectionHeaders: ["x-hub-signature-256"],
		requiredHeaders: [],
		signedContent: "body",
		encoding: "hex",
		presentedPrefix: "sha256=",
		signatureHeader: "x-hub-signature-256",
		keyMaterial: "raw-secret",
		skewSeconds: null,
		rotationEntries: false,
		deprecated: false,
	},
	{
		id: "gitlab",
		detectionHeaders: ["x-gitlab-token"],
		requiredHeaders: [],
		signedContent: "body",
		encoding: "plain-token",
		presentedPrefix: "",
		signatureHeader: "x-gitlab-token",
		keyMaterial: "raw-secret",
		skewSeconds: null,
		rotationEntries: false,
		deprecated: false,
	},
	{
		id: "hmac-v2-generic",
		detectionHeaders: ["x-webhook-signature-v2"],
		requiredHeaders: ["x-webhook-timestamp"],
		signedContent: "ts.body",
		encoding: "hex",
		presentedPrefix: "",
		signatureHeader: "x-webhook-signature-v2",
		keyMaterial: "raw-secret",
		skewSeconds: SKEW_SECONDS,
		rotationEntries: false,
		deprecated: false,
	},
	{
		id: "hmac-v1-generic-legacy",
		detectionHeaders: ["x-webhook-signature"],
		requiredHeaders: [],
		signedContent: "body",
		encoding: "hex",
		presentedPrefix: "",
		signatureHeader: "x-webhook-signature",
		keyMaterial: "raw-secret",
		skewSeconds: null,
		rotationEntries: false,
		deprecated: true,
	},
];

/** Lowercased header map (the server layer normalizes once). */
export type HeaderMap = Record<string, string>;

function headerOf(headers: HeaderMap, name: string): string | undefined {
	const v = headers[name];
	return typeof v === "string" && v.length > 0 ? v : undefined;
}

function bodyBytes(rawBody: string | Buffer): Buffer {
	return Buffer.isBuffer(rawBody) ? rawBody : Buffer.from(rawBody, "utf8");
}

function hmacHex(key: Buffer | string, content: Buffer): string {
	return createHmac("sha256", key).update(content).digest("hex");
}

/** Parse an integer unix-seconds timestamp; null when malformed. */
function parseTimestamp(value: string): number | null {
	if (!/^-?\d+$/.test(value)) return null;
	const ts = Number.parseInt(value, 10);
	return Number.isFinite(ts) ? ts : null;
}

function withinSkew(
	nowSeconds: number,
	ts: number,
	skewSeconds: number,
): boolean {
	return Math.abs(nowSeconds - ts) <= skewSeconds;
}

export type SignatureVerdict =
	| { ok: true; scheme: SignatureSchemeId }
	| {
			ok: false;
			reason: string;
			/** Committed scheme when one was selected, else null. */
			scheme: SignatureSchemeId | null;
	  };

function fail(reason: string): SignatureVerdict {
	return { ok: false, reason, scheme: null };
}

function failCommitted(
	reason: string,
	scheme: SignatureSchemeId,
): SignatureVerdict {
	return { ok: false, reason, scheme };
}

function pass(scheme: SignatureSchemeId): SignatureVerdict {
	return { ok: true, scheme };
}

// ── per-scheme validators (each consumes its own registry DATA entry) ────

interface SchemeContext {
	readonly spec: SignatureSchemeData;
	readonly secret: string;
	readonly body: Buffer;
	readonly nowSeconds: number;
	header(name: string): string | undefined;
}

function validateTimestampBound(
	ctx: SchemeContext,
	timestampRaw: string,
): { timestamp: string } | SignatureVerdict {
	const spec = ctx.spec;
	if (spec.skewSeconds === null) return { timestamp: timestampRaw };
	const ts = parseTimestamp(timestampRaw);
	if (ts === null) {
		return failCommitted(`${spec.id} timestamp malformed`, spec.id);
	}
	if (!withinSkew(ctx.nowSeconds, ts, spec.skewSeconds)) {
		return failCommitted(`${spec.id} timestamp outside replay window`, spec.id);
	}
	return { timestamp: timestampRaw };
}

function validatePlainBodyScheme(ctx: SchemeContext): SignatureVerdict {
	const presented = ctx.header(ctx.spec.detectionHeaders[0] as string);
	if (presented === undefined) return fail("header missing");
	switch (ctx.spec.encoding) {
		case "plain-token":
			// GitLab: plain secret compare, constant-time.
			if (!compareAfterCompute(() => ctx.secret, presented.trim())) {
				return failCommitted("token mismatch", ctx.spec.id);
			}
			return pass(ctx.spec.id);
		case "hex": {
			const expectedPrefix = ctx.spec.presentedPrefix;
			const expected = expectedPrefix + hmacHex(ctx.secret, ctx.body);
			if (!compareAfterCompute(() => expected, presented)) {
				return failCommitted("signature mismatch", ctx.spec.id);
			}
			return pass(ctx.spec.id);
		}
		case "base64":
			return failCommitted("unsupported encoding", ctx.spec.id);
	}
}

function validateSvix(ctx: SchemeContext): SignatureVerdict {
	const msgId = ctx.header("svix-id");
	const timestampRaw = ctx.header("svix-timestamp");
	const signatureHeader = ctx.header("svix-signature");
	if (
		msgId === undefined ||
		timestampRaw === undefined ||
		signatureHeader === undefined
	) {
		return failCommitted("svix component missing", ctx.spec.id);
	}
	const timeCheck = validateTimestampBound(ctx, timestampRaw);
	if ("ok" in timeCheck) return timeCheck;

	let key: Buffer;
	if (ctx.secret.startsWith("whsec_")) {
		try {
			// Node base64 decoding is lenient about stray characters; enforce
			// strict shape before decode (binascii.Error parity).
			const encoded = ctx.secret.slice("whsec_".length);
			if (!/^[A-Za-z0-9+/]+={0,2}$/.test(encoded)) {
				return failCommitted("svix whsec_ secret malformed", ctx.spec.id);
			}
			key = Buffer.from(encoded, "base64");
		} catch {
			return failCommitted("svix whsec_ secret malformed", ctx.spec.id);
		}
	} else {
		// Permissive for providers documenting Svix-style headers with raw
		// shared secrets (webhook.py raw-secret fallback).
		key = Buffer.from(ctx.secret, "utf8");
	}

	const signedContent = Buffer.concat([
		Buffer.from(msgId, "utf8"),
		Buffer.from(".", "utf8"),
		Buffer.from(timestampRaw, "utf8"),
		Buffer.from(".", "utf8"),
		ctx.body,
	]);
	const expected = createHmac("sha256", key)
		.update(signedContent)
		.digest("base64");

	if (ctx.spec.rotationEntries) {
		for (const part of signatureHeader.split(/\s+/)) {
			const trimmed = part.trim();
			const comma = trimmed.indexOf(",");
			if (comma <= 0) continue;
			const version = trimmed.slice(0, comma);
			const candidate = trimmed.slice(comma + 1);
			if (version !== "v1") continue;
			if (compareAfterCompute(() => expected, candidate)) {
				return pass(ctx.spec.id);
			}
		}
		return failCommitted("svix signature mismatch", ctx.spec.id);
	}
	return failCommitted("svix rotation entries disabled", ctx.spec.id);
}

function validateV2(ctx: SchemeContext): SignatureVerdict {
	const timestampRaw = ctx.header("x-webhook-timestamp");
	// ANTI-DOWNGRADE: V2 presence COMMITS. Missing/malformed/expired
	// timestamp rejects — it must NOT fall through to legacy V1 (stripping
	// the timestamp from a captured mixed-header request would otherwise
	// resurrect the replayable V1 validity).
	if (timestampRaw === undefined) {
		return failCommitted(
			"v2 committed but x-webhook-timestamp missing — no V1 fallback",
			ctx.spec.id,
		);
	}
	const timeCheck = validateTimestampBound(ctx, timestampRaw);
	if ("ok" in timeCheck) return timeCheck;
	const presented = ctx.header(ctx.spec.signatureHeader);
	if (presented === undefined) return fail("header missing");
	const signedContent = Buffer.concat([
		Buffer.from(timestampRaw, "utf8"),
		Buffer.from(".", "utf8"),
		ctx.body,
	]);
	const expected = hmacHex(ctx.secret, signedContent);
	if (!compareAfterCompute(() => expected, presented.trim().toLowerCase())) {
		return failCommitted("v2 signature mismatch", ctx.spec.id);
	}
	return pass(ctx.spec.id);
}

// ── dispatch ─────────────────────────────────────────────────────────────

export interface SignatureAdmissionInput {
	secret: string;
	headers: HeaderMap;
	rawBody: string | Buffer;
	/** Injected epoch seconds — signatures never read the wall clock. */
	nowSeconds: number;
	/** Restrict dispatch to exactly one declared scheme (route pinning). */
	pinned?: SignatureSchemeId | undefined;
	/**
	 * Bounded replay guard consulted for TIMESTAMP-BOUND schemes (svix-v1,
	 * hmac-v2-generic): after a crypto-valid verdict the exact signature
	 * material is recorded; a second identical request inside the skew window
	 * rejects as a replay. Schemes without a binding rely on delivery-id
	 * idempotency upstream (Linear/GitHub/GitLab parity).
	 */
	replayGuard?: BoundedSeenSet | undefined;
}

export interface SignatureValidatorHooks {
	/** Fired ONCE per route when the deprecated V1 branch commits. */
	onDeprecated?:
		| ((info: { route: string; scheme: SignatureSchemeId }) => void)
		| undefined;
}

export interface SignatureValidationCall extends SignatureAdmissionInput {
	/** Route name for deprecation warn-once bookkeeping. */
	route?: string | undefined;
}

function runValidators(input: SignatureAdmissionInput): SignatureVerdict {
	const body = bodyBytes(input.rawBody);
	for (const spec of SIGNATURE_SCHEMES) {
		if (input.pinned !== undefined && spec.id !== input.pinned) continue;
		const detectionValue = spec.detectionHeaders.find(
			(h) => headerOf(input.headers, h) !== undefined,
		);
		if (detectionValue === undefined) continue;

		// A scheme is COMMITTED the moment any detection header is present.
		const ctx: SchemeContext = {
			spec,
			secret: input.secret,
			body,
			nowSeconds: input.nowSeconds,
			header: (name: string) => headerOf(input.headers, name),
		};

		let verdict: SignatureVerdict;
		switch (spec.id) {
			case "svix-v1":
				verdict = validateSvix(ctx);
				break;
			case "linear":
			case "github-x-hub-signature-256":
			case "gitlab":
				verdict = validatePlainBodyScheme(ctx);
				break;
			case "hmac-v2-generic":
				verdict = validateV2(ctx);
				break;
			case "hmac-v1-generic-legacy":
				verdict = validatePlainBodyScheme(ctx);
				break;
		}
		return verdict; // first-presented-wins: NO fall-through on failure
	}
	return fail("no recognized signature header");
}

/**
 * Stateless validation entry point. The returned validator carries the
 * warn-once memory for the deprecated V1 branch (webhook.py
 * `_v1_signature_warned` parity) without module-global mutable state.
 */
export function createSignatureValidator(
	hooks: SignatureValidatorHooks = {},
): (call: SignatureValidationCall) => SignatureVerdict {
	const warnedRoutes = new Set<string>();
	return (call) => {
		const verdict = runValidators(call);
		if (
			verdict.ok &&
			verdict.scheme === "hmac-v1-generic-legacy" &&
			call.route !== undefined &&
			!warnedRoutes.has(call.route)
		) {
			warnedRoutes.add(call.route);
			hooks.onDeprecated?.({ route: call.route, scheme: verdict.scheme });
		}
		if (verdict.ok && call.replayGuard !== undefined) {
			const spec = SIGNATURE_SCHEMES.find((s) => s.id === verdict.scheme);
			if (spec !== undefined && spec.skewSeconds !== null) {
				// Key binds to the SIGNATURE material (spec.signatureHeader),
				// never to auxiliary headers like svix-id — two distinct
				// deliveries sharing an id must not collide, and a replay of
				// the exact signed request must always hit.
				const presentedSig = headerOf(call.headers, spec.signatureHeader) ?? "";
				if (!call.replayGuard.add(`${spec.id}:${presentedSig}`)) {
					return failCommitted("replayed signature within window", spec.id);
				}
			}
		}
		return verdict;
	};
}

/** One-shot stateless validation (no warn-once memory). */
export function validateSignature(
	call: SignatureAdmissionInput,
): SignatureVerdict {
	return runValidators(call);
}

export { constantTimeEqual as constantTimeCompare };
