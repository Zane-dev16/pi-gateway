// pi_platforms/persistent-ws/capability-latch — the A23 capability latch for
// PERMANENT feature downgrades (04 §3 rate-limits cell + gap-audit A23:
// "_native_stream_unsupported capability latch on feature-gate errors").
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/slack/adapter.py:send_draft except-branch (~L3240) —
//     feature-gate markers (not_allowed / missing_scope / feature_not_enabled
//     / invalid_method / unknown_method / method_deprecated / not_authed /
//     streaming_not_allowed) set _native_stream_unsupported ONCE so "future
//     runs skip the native attempt entirely instead of erroring once per
//     response"; supports_draft_streaming() consults the latch first.

/**
 * Error-blob markers that mean the FEATURE ITSELF is unavailable (permanent)
 * — as opposed to a transient per-message failure.
 */
export const NATIVE_STREAM_UNSUPPORTED_MARKERS: readonly string[] = [
	"not_allowed",
	"missing_scope",
	"feature_not_enabled",
	"invalid_method",
	"unknown_method",
	"method_deprecated",
	"not_authed",
	"streaming_not_allowed",
];

/** True when the error text is a feature-gate class failure. */
export function isNativeStreamFeatureGateError(errorText: string): boolean {
	const s = errorText.toLowerCase();
	return NATIVE_STREAM_UNSUPPORTED_MARKERS.some((m) => s.includes(m));
}

/**
 * The latch. ONE instance per adapter session; latches at most once — every
 * later attempt SKIPS the wire entirely and takes the fallback lane directly
 * (asserted by attempt counts in tests).
 */
export class CapabilityLatch {
	private latched = false;
	private latchReason: string | null = null;
	/** Times the latch FIRED (must stay ≤ 1 for a session; observability). */
	latchCount = 0;
	/** Native attempts that actually reached the wire since construction. */
	wireAttempts = 0;

	get unsupported(): boolean {
		return this.latched;
	}

	get reason(): string | null {
		return this.latchReason;
	}

	/**
	 * Consult before a native attempt. Latched ⇒ caller must fall back WITHOUT
	 * touching the wire (no attempt counted).
	 */
	shouldSkipNative(): boolean {
		return this.latched;
	}

	/**
	 * Feed a native-attempt failure. Feature-gate text latches ONCE (returns
	 * true only on the transition); anything else leaves the latch untouched.
	 */
	maybeLatch(errorText: string): boolean {
		if (this.latched) return false;
		if (!isNativeStreamFeatureGateError(errorText)) return false;
		this.latched = true;
		this.latchReason = errorText;
		this.latchCount += 1;
		return true;
	}
}
