// pi_gateway/security/trust/constant-time — THE canonical constant-time
// comparison primitive for every HTTP-ingress trust decision (06 §8.1;
// DEC-017 "signature schemes validated with CONSTANT-TIME compare").
//
// Ported from the READ-ONLY Hermes reference:
//   gateway/platforms/webhook.py:_hmac_str_equal (@158) — hmac.compare_digest
//     raises TypeError on str inputs containing non-ASCII characters; the
//     provided value is attacker-controlled on a public endpoint, so both
//     sides are encoded str→bytes and compared as UTF-8. A hostile header
//     therefore fails CLOSED with a clean rejection instead of raising out
//     of the request handler.
//   gateway/platforms/msgraph_webhook.py:_verify_client_state (@~365) — same
//     primitive guards the clientState echo ("a mismatch doesn't leak how
//     many leading characters matched").
//
// Layering (01 §5.3): pi_gateway cannot import pi_platforms, so this module
// is self-contained over node:crypto. It is the downward-importable
// canonical; kit/trust.secureCompare composes the identical node primitive.

import { timingSafeEqual } from "node:crypto";

/**
 * Constant-time equality for two strings (the `_hmac_str_equal` port).
 *
 * Structural non-early-exit construction: BOTH operands are fully
 * materialized as UTF-8 bytes BEFORE any comparison decision, and exactly
 * ONE `timingSafeEqual` executes on every path — including unequal lengths,
 * where a fixed scratch buffer of the expected side's length is burned so
 * the wall-time profile of a length-mismatch rejection does not diverge
 * from a content-mismatch rejection of the same length. Result parity with
 * `hmac.compare_digest(provided.encode(), expected.encode())`: False for
 * unequal lengths, data-independent comparison for equal lengths.
 */
export function constantTimeEqual(provided: string, expected: string): boolean {
	const a = Buffer.from(provided, "utf8");
	const b = Buffer.from(expected, "utf8");
	if (a.length === b.length) {
		return timingSafeEqual(a, b);
	}
	// Unequal lengths can never be equal; burn one comparison anyway so no
	// content- or length-dependent early exit exists in the construction.
	timingSafeEqual(b, Buffer.alloc(b.length));
	return false;
}

/**
 * Compute-then-compare HMAC verification shape: the EXPECTED digest is
 * always computed in full before any decision about the presented value.
 * Validators compose this so no branch on attacker-controlled content can
 * precede the comparison (the structural guarantee DEC-017 audits).
 */
export function compareAfterCompute(
	expected: () => string,
	presented: string,
): boolean {
	return constantTimeEqual(presented, expected());
}
