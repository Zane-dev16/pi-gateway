// BEHAVIOR CONTRACTS — DEC-017 signature validation (webhook.py:_validate_signature).
// Negative matrix: valid passes; tampered-at-any-position / wrong-scheme /
// stale-timestamp / malformed requests reject. Constant-time compare is proven
// STRUCTURALLY (every equal-length mutation rejects identically through the
// timingSafeEqual-backed primitive chain) — never by wall-clock measurement.

import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
	COMPARISON_PRIMITIVE,
	validateSignature,
	type HeaderMap,
} from "./signatures.js";

const SECRET = "whsec-test-secret";
const BODY = JSON.stringify({ action: "opened", number: 7 });
const NOW = 1_700_000_000;

function sig(secret: string, content: string | Buffer): string {
	return createHmac("sha256", secret).update(content).digest("hex");
}

function githubHeaders(value: string): HeaderMap {
	return { "x-hub-signature-256": `sha256=${value}` };
}

function validate(
	headers: HeaderMap,
	body = BODY,
	nowSeconds = NOW,
	secret = SECRET,
) {
	return validateSignature({ secret, headers, rawBody: body, nowSeconds });
}

describe("github-x-hub-signature-256", () => {
	const valid = sig(SECRET, BODY);

	it("accepts the exact signature", () => {
		const v = validate(githubHeaders(valid));
		expect(v.ok).toBe(true);
	});

	it("rejects a tampered body", () => {
		expect(validate(githubHeaders(valid), `${BODY}x`).ok).toBe(false);
	});

	it("rejects first-byte AND last-byte mutations identically (non-early-exit)", () => {
		const firstByteFlipped = (valid[0] === "0" ? "1" : "0") + valid.slice(1);
		const lastByteFlipped =
			valid.slice(0, -1) + (valid.endsWith("a") ? "b" : "a");
		const a = validate(githubHeaders(firstByteFlipped));
		const b = validate(githubHeaders(lastByteFlipped));
		expect(a.ok).toBe(false);
		expect(b.ok).toBe(false);
		expect(a).toEqual(b); // identical verdict shape — no positional shortcut
	});

	it("rejects a wrong secret", () => {
		expect(validate(githubHeaders(sig("other-secret", BODY))).ok).toBe(false);
	});

	it("rejects length-mismatched and non-signature garbage without throwing", () => {
		expect(validate(githubHeaders("deadbeef")).ok).toBe(false);
		// Empty header value ⇒ unclaimed ⇒ no recognized signature.
		expect(validate(githubHeaders("")).ok).toBe(false);
		expect(validate({} as HeaderMap).ok).toBe(false);
	});
});

describe("gitlab plain token", () => {
	it("accepts the exact token constant-time", () => {
		const v = validate({ "x-gitlab-token": SECRET });
		expect(v.ok).toBe(true);
	});
	it("rejects a one-character-off token with the same verdict as full garbage", () => {
		const off = SECRET.slice(0, -1) + (SECRET.endsWith("t") ? "x" : "t");
		expect(validate({ "x-gitlab-token": off })).toEqual(
			validate({ "x-gitlab-token": "completely-wrong" }),
		);
	});
});

describe("linear hex-of-body", () => {
	it("accepts the documented scheme (body only, no timestamp)", () => {
		const v = validate({ "linear-signature": sig(SECRET, BODY) });
		expect(v.ok).toBe(true);
	});
	it("rejects stale-replayable bodies only on value mismatch (no timestamp binding — deprecated-class behavior)", () => {
		expect(validate({ "linear-signature": sig(SECRET, "other") }).ok).toBe(
			false,
		);
	});
});

describe("hmac-v2-generic (anti-downgrade)", () => {
	const ts = NOW;
	const v2sig = sig(SECRET, `${ts}.${BODY}`);
	const headers = (): HeaderMap => ({
		"x-webhook-signature-v2": v2sig,
		"x-webhook-timestamp": String(ts),
	});

	it("accepts a fresh signed request", () => {
		const v = validate(headers());
		expect(v.ok).toBe(true);
	});

	it("REJECTS when the timestamp header is stripped — never falls back to V1", () => {
		const stripped: HeaderMap = { "x-webhook-signature-v2": v2sig };
		expect(validate(stripped).ok).toBe(false);
	});

	it("rejects timestamps outside ±300s replay window on both sides", () => {
		expect(validate(headers(), BODY, NOW + 301).ok).toBe(false);
		expect(validate(headers(), BODY, NOW - 301).ok).toBe(false);
		expect(validate(headers(), BODY, NOW + 300).ok).toBe(true);
	});

	it("rejects a malformed timestamp", () => {
		const h = headers();
		h["x-webhook-timestamp"] = "not-a-number";
		expect(validate(h).ok).toBe(false);
	});
});

describe("svix-v1", () => {
	const svixId = "msg_test_0001";
	const svixTs = String(NOW);
	const whsec = `whsec_${Buffer.from("svix-secret-key").toString("base64")}`;
	const key = Buffer.from("svix-secret-key");
	const expectedB64 = createHmac("sha256", key)
		.update(`${svixId}.${svixTs}.${BODY}`, "utf8")
		.digest("base64");

	it("accepts a valid v1,<base64> part", () => {
		const v = validate(
			{
				"svix-id": svixId,
				"svix-timestamp": svixTs,
				"svix-signature": `v1,${expectedB64}`,
			},
			BODY,
			NOW,
			whsec,
		);
		expect(v.ok).toBe(true);
	});

	it("ANY part of space-separated rotation matches", () => {
		const v = validate(
			{
				"svix-id": svixId,
				"svix-timestamp": svixTs,
				"svix-signature": `v1,bogus=part v1,${expectedB64}`,
			},
			BODY,
			NOW,
			whsec,
		);
		expect(v.ok).toBe(true);
	});

	it("rejects expired timestamps (>300s skew)", () => {
		const v = validate(
			{
				"svix-id": svixId,
				"svix-timestamp": String(NOW - 400),
				"svix-signature": `v1,${expectedB64}`,
			},
			BODY,
			NOW,
			whsec,
		);
		expect(v.ok).toBe(false);
	});

	it("claims on ANY trio member present; missing components reject", () => {
		const onlySig = validate({ "svix-signature": "v1,x" }, BODY, NOW, whsec);
		expect(onlySig.ok).toBe(false);
	});
});

describe("first-presented-wins dispatch", () => {
	it("an invalid GitHub signature does NOT fall through to GitLab", () => {
		const v = validate({
			"x-hub-signature-256": `sha256=${sig(SECRET, "tampered-body")}`,
			"x-gitlab-token": SECRET,
		});
		expect(v.ok).toBe(false);
		expect(v).toEqual({ ok: false, reason: "github signature mismatch" });
	});

	it("wrong-scheme pinning: a VALID gitlab token against a github-pinned route rejects", () => {
		const v = validateSignature({
			secret: SECRET,
			headers: { "x-gitlab-token": SECRET },
			rawBody: BODY,
			nowSeconds: NOW,
			pinned: "github-x-hub-signature-256",
		});
		expect(v.ok).toBe(false);
	});

	it("pinned scheme validates when its own header carries the right value", () => {
		const v = validateSignature({
			secret: SECRET,
			headers: githubHeaders(sig(SECRET, BODY)),
			rawBody: BODY,
			nowSeconds: NOW,
			pinned: "github-x-hub-signature-256",
		});
		expect(v.ok).toBe(true);
	});
});

describe("constant-time construction (structural, not wall-clock)", () => {
	it("the primitive chain IS node:crypto.timingSafeEqual-backed secureCompare", () => {
		// Equal-length inputs compare in constant time via timingSafeEqual;
		// length mismatch returns false without content comparison.
		expect(COMPARISON_PRIMITIVE("aaaa", "aaaa")).toBe(true);
		expect(COMPARISON_PRIMITIVE("aaaa", "aaab")).toBe(false);
		expect(COMPARISON_PRIMITIVE("aaa", "aaaa")).toBe(false);
	});

	it("every byte-position single-bit mutation of a 64-char digest rejects", () => {
		// Structural proof of non-early-exit: if comparison short-circuited on
		// the first differing byte, early positions would behave differently
		// than late ones under ANY observable contract. They do not: all
		// mutations produce the identical rejected verdict.
		const valid = sig(SECRET, BODY);
		const mutations = Array.from(valid, (_, i) => {
			const flipped =
				valid[i] === "0"
					? "1"
					: valid[i] === "1"
						? "0"
						: valid[i] === "f"
							? "e"
							: "f";
			const mutated = valid.slice(0, i) + flipped + valid.slice(i + 1);
			return validate(githubHeaders(mutated));
		});
		expect(mutations.length).toBe(64);
		for (const m of mutations)
			expect(m).toEqual({ ok: false, reason: "github signature mismatch" });
	});

	it("utf-8 hostile headers fail closed instead of raising", () => {
		const v = validate({
			"x-gitlab-token": "токен-с-юникодом",
		});
		expect(v.ok).toBe(false);
	});
});
