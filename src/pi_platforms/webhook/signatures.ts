// pi_platforms/webhook/signatures — DEC-017 signature schemes, validated with
// CONSTANT-TIME compare (kit trust.secureCompare / verifyHmacSignature — the
// ONLY comparison primitives used; no `===` ever touches secret material).
//
// Ported from the READ-ONLY Hermes reference:
//   webhook.py:_validate_signature (@1053)  — dispatch on header PRESENCE,
//     FIRST-PRESENTED-WINS (a present-but-invalid signature never falls
//     through to another scheme)
//   webhook.py:_hmac_str_equal (@168)       — hmac.compare_digest over utf-8
//     (length mismatch ⇒ false without content comparison)
//   webhook.py:_validate_svix_signature (@1197) — "{id}.{timestamp}.{body}",
//     whsec_ prefix → base64 key, space-separated "v1,<b64>" parts (any wins),
//     ±300 s skew; claims on ANY trio member present
//   webhook.py Linear branch                — plain hex HMAC of body only
//   webhook.py GitHub branch                — trusted side carries "sha256="
//   webhook.py GitLab branch                — plain token equality
//   webhook.py V2 branch                    — "{ts}.{body}" hex HMAC; PRESENCE
//     of X-Webhook-Signature-V2 COMMITS to V2: missing/malformed/expired
//     timestamp REJECTS rather than falling through (anti-downgrade)
//   webhook.py V1 branch                    — deprecated hex-of-body, reachable
//     only when nothing else was presented

import { createHmac } from "node:crypto";
import { secureCompare, verifyHmacSignature } from "../kit/trust.js";
import { SIGNATURE_SKEW_SECONDS } from "./manifest.js";

export type SignatureScheme =
	| "github-x-hub-signature-256"
	| "gitlab"
	| "svix-v1"
	| "linear"
	| "hmac-v2-generic";

/** Lowercased header map (the server layer normalizes once). */
export type HeaderMap = Record<string, string>;

interface SignatureCoreInput {
	secret: string;
	headers: HeaderMap;
	rawBody: string | Buffer;
	/** Injected epoch seconds — signatures never read the wall clock. */
	nowSeconds: number;
}

export interface SignatureInput extends SignatureCoreInput {
	scheme: SignatureScheme;
}

export type SignatureVerdict =
	| { ok: true; scheme: SignatureScheme }
	| { ok: false; reason: string };

function fail(reason: string): SignatureVerdict {
	return { ok: false, reason };
}

function pass(scheme: SignatureScheme): SignatureVerdict {
	return { ok: true, scheme };
}

function headerOf(headers: HeaderMap, ...names: string[]): string | undefined {
	for (const n of names) {
		const v = headers[n.toLowerCase()];
		if (typeof v === "string" && v.length > 0) return v;
	}
	return undefined;
}

// ── per-scheme validators ────────────────────────────────────────────────

function validateGithub(
	input: SignatureCoreInput,
	presented: string,
): SignatureVerdict {
	// Trusted side carries the prefix (Hermes parity); kit strips an optional
	// "sha256=" and compares hex digests in constant time.
	if (!verifyHmacSignature(input.secret, input.rawBody, presented)) {
		return fail("github signature mismatch");
	}
	return pass("github-x-hub-signature-256");
}

function validateGitlab(
	input: SignatureCoreInput,
	presented: string,
): SignatureVerdict {
	// Plain token equality, constant-time (webhook.py GitLab branch).
	if (!secureCompare(presented, input.secret)) {
		return fail("gitlab token mismatch");
	}
	return pass("gitlab");
}

function validateLinear(
	input: SignatureCoreInput,
	presented: string,
): SignatureVerdict {
	if (!verifyHmacSignature(input.secret, input.rawBody, presented)) {
		return fail("linear signature mismatch");
	}
	return pass("linear");
}

function validateHmacV2(
	input: SignatureCoreInput,
	presented: string,
	timestamp: string | undefined,
): SignatureVerdict {
	// Anti-downgrade: presenting V2 COMMITS to V2. A missing/malformed/
	// expired timestamp rejects outright — it never falls back to V1.
	if (timestamp === undefined) return fail("v2 timestamp missing");
	const ts = Number.parseInt(timestamp, 10);
	if (!Number.isFinite(ts)) return fail("v2 timestamp malformed");
	if (Math.abs(input.nowSeconds - ts) > SIGNATURE_SKEW_SECONDS) {
		return fail("v2 timestamp outside replay window");
	}
	const expected = createHmac("sha256", input.secret)
		.update(`${timestamp}.${String(input.rawBody)}`, "utf8")
		.digest("hex");
	if (!secureCompare(expected, presented.trim().toLowerCase())) {
		return fail("v2 signature mismatch");
	}
	return pass("hmac-v2-generic");
}

function validateSvix(
	input: SignatureCoreInput,
	svixId: string,
	svixTimestamp: string | undefined,
	signatureHeader: string | undefined,
): SignatureVerdict {
	if (
		svixTimestamp === undefined ||
		signatureHeader === undefined ||
		signatureHeader.length === 0
	) {
		return fail("svix component missing");
	}
	const ts = Number.parseInt(svixTimestamp, 10);
	if (!Number.isFinite(ts)) return fail("svix timestamp malformed");
	if (Math.abs(input.nowSeconds - ts) > SIGNATURE_SKEW_SECONDS) {
		return fail("svix timestamp outside replay window");
	}
	// whsec_ prefix → strip + base64-decode the key; else raw secret bytes.
	let key: Buffer;
	if (input.secret.startsWith("whsec_")) {
		try {
			key = Buffer.from(input.secret.slice("whsec_".length), "base64");
		} catch {
			return fail("svix key malformed");
		}
	} else {
		key = Buffer.from(input.secret, "utf8");
	}
	const signedContent = `${svixId}.${svixTimestamp}.${String(input.rawBody)}`;
	const expected = createHmac("sha256", key)
		.update(signedContent, "utf8")
		.digest("base64");
	// Space-separated rotation parts ("v1,<b64> v1,<b64>") — ANY match wins.
	for (const part of signatureHeader.split(" ")) {
		const trimmed = part.trim();
		if (!trimmed.startsWith("v1,")) continue;
		if (secureCompare(expected, trimmed.slice(3))) {
			return pass("svix-v1");
		}
	}
	return fail("svix signature mismatch");
}

/**
 * THE validation entry point (first-presented-wins dispatch, Hermes parity).
 * A route-pinned scheme (`pinned`) restricts dispatch to exactly one scheme:
 * other schemes' headers are ignored entirely and the pinned header's absence
 * is a rejection ("wrong-scheme" requests can never slip through).
 */
export function validateSignature(
	input: SignatureCoreInput & {
		pinned?: SignatureScheme | undefined;
	},
): SignatureVerdict {
	const { headers } = input;

	const svixId = headerOf(headers, "svix-id");
	const svixTs = headerOf(headers, "svix-timestamp");
	const svixSig = headerOf(headers, "svix-signature");
	const linear = headerOf(headers, "linear-signature");
	const github = headerOf(headers, "x-hub-signature-256");
	const gitlab = headerOf(headers, "x-gitlab-token");
	const v2 = headerOf(headers, "x-webhook-signature-v2");
	const v2Ts = headerOf(headers, "x-webhook-timestamp");

	const svixClaimed =
		svixId !== undefined || svixTs !== undefined || svixSig !== undefined;

	const branches: Array<{
		scheme: SignatureScheme;
		claimed: boolean;
		run: () => SignatureVerdict;
	}> = [
		{
			scheme: "svix-v1",
			claimed: svixClaimed,
			run: () => validateSvix(input, svixId ?? "", svixTs, svixSig),
		},
		{
			scheme: "linear",
			claimed: linear !== undefined,
			run: () =>
				linear === undefined
					? fail("header missing")
					: validateLinear(input, linear),
		},
		{
			scheme: "github-x-hub-signature-256",
			claimed: github !== undefined,
			run: () =>
				github === undefined
					? fail("header missing")
					: validateGithub(input, github),
		},
		{
			scheme: "gitlab",
			claimed: gitlab !== undefined,
			run: () =>
				gitlab === undefined
					? fail("header missing")
					: validateGitlab(input, gitlab),
		},
		{
			scheme: "hmac-v2-generic",
			claimed: v2 !== undefined,
			run: () =>
				v2 === undefined
					? fail("header missing")
					: validateHmacV2(input, v2, v2Ts),
		},
	];

	for (const branch of branches) {
		if (input.pinned !== undefined && branch.scheme !== input.pinned) {
			continue; // pinned dispatch ignores every other scheme's headers
		}
		if (!branch.claimed) continue;
		return branch.run(); // first-presented-wins — NO fall-through
	}

	// Legacy generic V1 (deprecated): hex HMAC of body only. Reachable ONLY
	// when nothing else was presented AND nothing was pinned (pinning is
	// strict; V1 carries no replay protection).
	const v1 = headerOf(headers, "x-webhook-signature");
	if (v1 !== undefined && input.pinned === undefined) {
		if (verifyHmacSignature(input.secret, input.rawBody, v1)) {
			return pass("hmac-v2-generic"); // v1 shares the generic family label
		}
		return fail("legacy signature mismatch");
	}

	return fail("no recognized signature header validated");
}

/**
 * Structural non-early-exit proof point: the ONE comparison primitive chain
 * every scheme composes. Byte-position mutations of a valid signature are
 * rejected identically because comparison never short-circuits on content
 * (node:crypto.timingSafeEqual under secureCompare).
 */
export const COMPARISON_PRIMITIVE = secureCompare;
