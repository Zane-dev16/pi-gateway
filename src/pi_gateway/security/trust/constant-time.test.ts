// Behavior contracts for the canonical constant-time comparator (the
// _hmac_str_equal port): unequal lengths fail WITHOUT divergence, byte
// mutations anywhere flip the verdict, and hostile (non-ASCII) input fails
// CLOSED instead of raising — the #500-instead-of-reject class.

import { describe, expect, it } from "vitest";
import { compareAfterCompute, constantTimeEqual } from "./constant-time.js";

describe("constantTimeEqual (webhook.py:_hmac_str_equal port)", () => {
	it("equal strings match; any single-byte mutation anywhere rejects", () => {
		const a = "v1,9Ga7dWL07Yu5tmrDZBnmctMmE9Q=";
		expect(constantTimeEqual(a, a)).toBe(true);
		for (let i = 0; i < a.length; i += 3) {
			const mutated =
				a.slice(0, i) + (a[i] === "x" ? "y" : "x") + a.slice(i + 1);
			expect(constantTimeEqual(mutated, a)).toBe(false);
		}
	});

	it("UNEQUAL-LENGTH inputs reject without content-comparison divergence", () => {
		// Longer-than / shorter-than / empty against the same expected value.
		expect(constantTimeEqual("", "abc")).toBe(false);
		expect(constantTimeEqual("ab", "abc")).toBe(false);
		expect(constantTimeEqual("abcd", "abc")).toBe(false);
		expect(constantTimeEqual("x".repeat(512), "abc")).toBe(false);
		// The burned comparison path must not throw on exotic sizes.
		expect(constantTimeEqual("\u{1F600}".repeat(300), "a")).toBe(false);
	});

	it("non-ASCII attacker headers fail CLOSED (no TypeError escape)", () => {
		// hmac.compare_digest(str-with-non-ASCII) raises in Python; the port
		// encodes UTF-8 first so a hostile header is a clean rejection.
		expect(constantTimeEqual("sigéè✓", "expected")).toBe(false);
		expect(constantTimeEqual("sigéè✓", "sigéè✓")).toBe(true);
	});

	it("compareAfterCompute computes EXPECTED before inspecting PRESENTED", () => {
		let computed = false;
		const verdict = compareAfterCompute(() => {
			computed = true;
			return "expected-digest";
		}, "wrong");
		expect(computed).toBe(true); // no early branch skipped digest computation
		expect(verdict).toBe(false);
	});
});
