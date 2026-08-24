// Behavior contracts for THE canonical signature scheme engine (06 §8.1;
// DEC-017). Required negative matrix PER SCHEME: valid / tampered /
// wrong-timestamp / expired-window / replayed. Plus: first-presented-wins
// dispatch (no fallthrough), V2 anti-downgrade commit, V1 deprecation
// warn-once, secret-without-headers fail-closed, and byte-position mutation
// invariance of the constant-time compare.

import { createHmac, randomBytes } from "node:crypto";
import { describe, expect, it } from "vitest";
import { BoundedSeenSet } from "./replay-seen-set.js";
import {
	SIGNATURE_SCHEMES,
	createSignatureValidator,
	validateSignature,
	type HeaderMap,
} from "./index.js";

const SECRET = "test-shared-secret";
const BODY = Buffer.from('{"event": "ping"}');
const NOW = 1_700_000_000;

function hexHmac(key: string | Buffer, content: Buffer | string): string {
	return createHmac("sha256", key).update(content).digest("hex");
}

function svixSign(key: Buffer, id: string, ts: string, body: Buffer): string {
	return createHmac("sha256", key)
		.update(Buffer.concat([Buffer.from(`${id}.${ts}.`, "utf8"), body]))
		.digest("base64");
}

function call(
	headers: HeaderMap,
	body: Buffer = BODY,
	nowSeconds = NOW,
): Parameters<typeof validateSignature>[0] {
	return {
		secret: SECRET,
		headers,
		rawBody: body,
		nowSeconds,
	};
}

describe("scheme registry data (DEC-017: boundaries are DATA)", () => {
	it("declares all six schemes with their detection headers + skew", () => {
		expect(SIGNATURE_SCHEMES.map((s) => s.id)).toEqual([
			"svix-v1",
			"linear",
			"github-x-hub-signature-256",
			"gitlab",
			"hmac-v2-generic",
			"hmac-v1-generic-legacy",
		]);
		const byId = new Map(SIGNATURE_SCHEMES.map((s) => [s.id, s]));
		expect(byId.get("svix-v1")?.skewSeconds).toBe(300);
		expect(byId.get("hmac-v2-generic")?.skewSeconds).toBe(300);
		// Linear's absence of a timestamp binding is VENDOR-documented.
		expect(byId.get("linear")?.skewSeconds).toBeNull();
		expect(byId.get("hmac-v2-generic")?.requiredHeaders).toContain(
			"x-webhook-timestamp",
		);
		expect(byId.get("hmac-v1-generic-legacy")?.deprecated).toBe(true);
	});
});

describe("svix-v1 scheme (±300 s skew, whsec_ key, rotation entries)", () => {
	const id = "msg_24KFil2";
	const rawKey = randomBytes(16);
	const whsecSecret = `whsec_${rawKey.toString("base64")}`;
	const ts = String(NOW);

	function svixHeaders(sig: string): HeaderMap {
		return {
			"svix-id": id,
			"svix-timestamp": ts,
			"svix-signature": sig,
		};
	}
	// Raw-secret path: the engine derives utf8 key bytes from SECRET.
	const validSig = svixSign(Buffer.from(SECRET, "utf8"), id, ts, BODY);
	// whsec_ path: base64-decoded key material.
	const whsecSig = svixSign(rawKey, id, ts, BODY);

	it("valid signature admits; ANY trio member claims the scheme", () => {
		expect(validateSignature(call(svixHeaders(`v1,${validSig}`)))).toEqual({
			ok: true,
			scheme: "svix-v1",
		});
		// Claiming via ONLY the timestamp header still commits to svix…
		expect(validateSignature(call({ "svix-timestamp": ts })).ok).toBe(false);
		// …and the failure is a COMMITTED svix rejection, not a fallthrough.
		const verdict = validateSignature(call({ "svix-timestamp": ts }));
		expect(verdict.ok === false && verdict.scheme).toBe("svix-v1");
	});

	it("whsec_ base64-decoded key admits; malformed whsec_ payload fails closed", () => {
		expect(
			validateSignature({
				...call(svixHeaders(`v1,${whsecSig}`)),
				secret: whsecSecret,
			}),
		).toEqual({ ok: true, scheme: "svix-v1" });
		// A non-base64 whsec_ payload must reject, not decode leniently.
		expect(
			validateSignature({
				...call(svixHeaders(`v1,${whsecSig}`)),
				secret: "whsec_!!!not-base64!!",
			}).ok,
		).toBe(false);
	});

	it("tampered signature rejects", () => {
		const tampered = validSig.slice(0, -4) + "AAAA";
		const verdict = validateSignature(call(svixHeaders(`v1,${tampered}`)));
		expect(verdict.ok).toBe(false);
	});

	it("wrong timestamp (signature bound to another ts) rejects", () => {
		const otherTs = String(NOW - 10);
		// Signature computed over otherTs while the header still says `ts`.
		const misbound = [svixSign(Buffer.from(SECRET, "utf8"), id, otherTs, BODY)];
		expect(
			validateSignature(call(svixHeaders(`v1,${misbound[0] as string}`))).ok,
		).toBe(false);
	});

	it("expired window (> 300 s skew) rejects", () => {
		const staleTs = String(NOW - 301);
		const staleSig = svixSign(Buffer.from(SECRET, "utf8"), id, staleTs, BODY);
		const verdict = validateSignature(
			call({
				"svix-id": id,
				"svix-timestamp": staleTs,
				"svix-signature": `v1,${staleSig}`,
			}),
		);
		expect(verdict.ok === false && verdict.reason).toMatch(/replay window/);
	});

	it("boundary: exactly 300 s skew admits", () => {
		const edgeTs = String(NOW - 300);
		const edgeSig = svixSign(Buffer.from(SECRET, "utf8"), id, edgeTs, BODY);
		expect(
			validateSignature(
				call({
					"svix-id": id,
					"svix-timestamp": edgeTs,
					"svix-signature": `v1,${edgeSig}`,
				}),
			).ok,
		).toBe(true);
	});

	it("rotation entries: any v1,<sig> part may win; malformed parts skipped", () => {
		const rotated = `v1,${randomBytes(44).toString("base64")} v1,${validSig}`;
		expect(validateSignature(call(svixHeaders(rotated)))).toEqual({
			ok: true,
			scheme: "svix-v1",
		});
		expect(validateSignature(call(svixHeaders("garbage-no-comma"))).ok).toBe(
			false,
		);
	});
});

describe("linear scheme (body-only hex HMAC — vendor-documented NO replay binding)", () => {
	const linearHeaders = (): HeaderMap => ({
		"linear-signature": hexHmac(SECRET, BODY),
	});
	it("valid admits; tampered rejects; no timestamp check exists", () => {
		expect(validateSignature(call(linearHeaders()))).toEqual({
			ok: true,
			scheme: "linear",
		});
		expect(
			validateSignature(
				call({ "linear-signature": hexHmac(SECRET, Buffer.from("other")) }),
			).ok,
		).toBe(false);
		// A wildly-stale clock changes nothing — Linear has NO binding.
		expect(
			validateSignature(call(linearHeaders(), BODY, NOW - 100_000)).ok,
		).toBe(true);
	});
});

describe("github-x-hub-signature-256 scheme", () => {
	const gh = `sha256=${hexHmac(SECRET, BODY)}`;
	it("valid sha256= prefixed admits; tampered/wrong-prefix reject", () => {
		expect(validateSignature(call({ "x-hub-signature-256": gh }))).toEqual({
			ok: true,
			scheme: "github-x-hub-signature-256",
		});
		expect(
			validateSignature(
				call({
					"x-hub-signature-256": `sha256=${hexHmac(SECRET, Buffer.from("x"))}`,
				}),
			).ok,
		).toBe(false);
		expect(
			validateSignature(call({ "x-hub-signature-256": hexHmac(SECRET, BODY) }))
				.ok,
		).toBe(false); // missing trusted-side prefix
	});
});

describe("gitlab scheme (plain token compare)", () => {
	it("exact token admits; any mutation rejects; non-ASCII fails closed", () => {
		expect(validateSignature(call({ "x-gitlab-token": SECRET }))).toEqual({
			ok: true,
			scheme: "gitlab",
		});
		expect(validateSignature(call({ "x-gitlab-token": `${SECRET}x` })).ok).toBe(
			false,
		);
		expect(validateSignature(call({ "x-gitlab-token": "sécret-éè" })).ok).toBe(
			false,
		);
	});
});

describe("hmac-v2-generic (anti-downgrade COMMIT)", () => {
	function v2(ts: number, body: Buffer = BODY): HeaderMap {
		return {
			"x-webhook-signature-v2": hexHmac(
				SECRET,
				`${ts}.${body.toString("utf8")}`,
			),
			"x-webhook-timestamp": String(ts),
		};
	}
	it("valid within ±300 s admits", () => {
		expect(validateSignature(call(v2(NOW - 299)))).toEqual({
			ok: true,
			scheme: "hmac-v2-generic",
		});
	});
	it("tampered signature rejects", () => {
		const headers = v2(NOW);
		headers["x-webhook-signature-v2"] =
			(headers["x-webhook-signature-v2"] ?? "").slice(0, -2) + "00";
		expect(validateSignature(call(headers)).ok).toBe(false);
	});
	it("wrong-timestamp binding rejects", () => {
		// Signature bound to NOW while the header claims NOW-5 — the signed
		// content no longer matches `<header-ts>.<body>`.
		const headers = v2(NOW);
		headers["x-webhook-timestamp"] = String(NOW - 5);
		expect(validateSignature(call(headers)).ok).toBe(false);
	});
	it("expired window (>300 s) rejects", () => {
		const verdict = validateSignature(call(v2(NOW - 301)));
		expect(verdict.ok === false && verdict.reason).toMatch(/replay window/);
	});
	it("ANTI-DOWNGRADE: stripping the timestamp does NOT resurrect V1", () => {
		// A sender migrating sends BOTH headers; an attacker who captured one
		// strips X-Webhook-Timestamp — the request must REJECT as committed-V2,
		// never fall through to the replayable legacy V1 branch.
		const both = v2(NOW);
		delete both["x-webhook-timestamp"];
		const verdict = validateSignature(call(both));
		expect(verdict.ok).toBe(false);
		expect(verdict.ok === false && verdict.scheme).toBe("hmac-v2-generic");
		expect(verdict.ok === false && verdict.reason).toMatch(/no V1 fallback/);
	});
	it("malformed timestamp rejects as committed-V2 (no fallthrough)", () => {
		const headers = v2(NOW);
		headers["x-webhook-timestamp"] = "not-a-number";
		const verdict = validateSignature(call(headers));
		expect(verdict.ok).toBe(false);
		expect(verdict.ok === false && verdict.scheme).toBe("hmac-v2-generic");
	});
});

describe("legacy hmac-v1 (deprecated, warn-once per route)", () => {
	it("valid admits with scheme label; deprecation hook fires EXACTLY once per route", () => {
		const seen: string[] = [];
		const validator = createSignatureValidator({
			onDeprecated: ({ route }) => void seen.push(route),
		});
		const headers = { "x-webhook-signature": hexHmac(SECRET, BODY) };
		const input = { ...call(headers), route: "r1" };
		const first = validator(input);
		expect(first).toEqual({ ok: true, scheme: "hmac-v1-generic-legacy" });
		expect(validator({ ...call(headers), route: "r1" }).ok).toBe(true);
		expect(seen).toEqual(["r1"]); // once-per-route, not once-ever-global
		expect(validator({ ...call(headers), route: "r2" }).ok).toBe(true);
		expect(seen).toEqual(["r1", "r2"]);
	});
});

describe("dispatch discipline", () => {
	it("first-presented-wins: present-but-invalid svix NEVER falls through", () => {
		// Valid GitHub signature ALSO on the wire; svix claimed-but-broken wins
		// the dispatch and must reject without consulting GitHub.
		const headers: HeaderMap = {
			"svix-timestamp": String(NOW), // claims svix, signature missing → fail
			"x-hub-signature-256": `sha256=${hexHmac(SECRET, BODY)}`,
		};
		const verdict = validateSignature(call(headers));
		expect(verdict.ok).toBe(false);
		expect(verdict.ok === false && verdict.scheme).toBe("svix-v1");
	});

	it("pinned dispatch ignores every other scheme's headers", () => {
		const headers: HeaderMap = {
			"svix-timestamp": String(NOW),
			"x-webhook-signature": hexHmac(SECRET, BODY),
		};
		const pinned = {
			...call(headers),
			pinned: "hmac-v1-generic-legacy" as const,
		};
		expect(validateSignature(pinned)).toEqual({
			ok: true,
			scheme: "hmac-v1-generic-legacy",
		});
		const pinnedAbsent = { ...call({}), pinned: "gitlab" as const };
		expect(validateSignature(pinnedAbsent).ok).toBe(false);
	});

	it("secret configured + NO recognized header ⇒ fail-closed", () => {
		const verdict = validateSignature(call({ "unrelated-header": "1" }));
		expect(verdict.ok).toBe(false);
		expect(verdict.ok === false && verdict.reason).toMatch(/no recognized/);
	});
});

describe("replay guard (bounded seen-set) for timestamp-bound schemes", () => {
	const nowMs = NOW * 1000;
	const guard = new BoundedSeenSet({
		maxEntries: 64,
		ttlMs: 400_000,
		nowMs: () => nowMs,
	});
	const validator = createSignatureValidator();
	const id = "msg_replay";
	const ts = String(NOW);
	const key = Buffer.from("k".repeat(16));
	const sig = svixSign(key, id, ts, BODY);
	const headers: HeaderMap = {
		"svix-id": id,
		"svix-timestamp": ts,
		"svix-signature": `v1,${sig}`,
	};

	it("first admission ok; identical re-request inside window rejects as REPLAY", () => {
		const first = validator({
			secret: "k".repeat(16),
			headers,
			rawBody: BODY,
			nowSeconds: NOW,
			replayGuard: guard,
		});
		expect(first).toEqual({ ok: true, scheme: "svix-v1" });
		const replay = validator({
			secret: "k".repeat(16),
			headers,
			rawBody: BODY,
			nowSeconds: NOW + 5,
			replayGuard: guard,
		});
		expect(replay.ok === false && replay.reason).toMatch(/replay/i);
	});

	it("a DIFFERENT delivery (fresh id/ts/signature) still admits", () => {
		const freshTs = String(NOW + 6);
		const freshSig = svixSign(key, id, freshTs, BODY);
		const ok = validator({
			secret: "k".repeat(16),
			headers: {
				"svix-id": id,
				"svix-timestamp": freshTs,
				"svix-signature": `v1,${freshSig}`,
			},
			rawBody: BODY,
			nowSeconds: NOW + 6,
			replayGuard: guard,
		});
		expect(ok.ok).toBe(true);
	});
});

describe("constant-time structure (byte-position invariance)", () => {
	it("every single-byte position mutation of a valid signature rejects identically", () => {
		const expected = hexHmac(SECRET, BODY);
		for (let i = 0; i < expected.length; i += 7) {
			const mutated =
				expected.slice(0, i) +
				(expected[i] === "a" ? "b" : "a") +
				expected.slice(i + 1);
			expect(validateSignature(call({ "linear-signature": mutated })).ok).toBe(
				false,
			);
		}
	});
});
