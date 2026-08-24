// pi_platforms/kit/capabilities — capability flags are DATA, read via
// attribute-style defaults with zero per-platform branching at call sites
// (04-platform-adapters.md §2) plus the Q17 rate-tier manifests.
//
// "Per-platform numbers live in adapter manifests, not core" (Q17 decision):
// adapters declare rate budgets as data; the runner consults before egress.

/** Verified base-class flags (§1/§2). Defaults match Hermes' base adapter. */
export interface CapabilityManifest {
	/**
	 * False ⇒ stateless shape (api_server/webhook): nowhere to push later
	 * completions; propagates into async-delivery session context.
	 */
	supportsAsyncDelivery: boolean;
	/**
	 * True ⇒ delivery router lets the adapter chunk natively (full output
	 * preserved); Slack/Discord set it.
	 */
	splitsLongMessages: boolean;
	/** Slack & Matrix ship "!" ("/" is reserved in Slack threads). */
	typedCommandPrefix: string;
	/** webhook/api_server set False (#57056 resume prompt). */
	interactiveResume: boolean;
	supportsInchannelContinuable: boolean;
	/** DingTalk AI-Cards class: explicit close required. Checked `is True`. */
	requiresEditFinalize: boolean;
}

export const DEFAULT_CAPABILITIES: Readonly<CapabilityManifest> = Object.freeze(
	{
		supportsAsyncDelivery: true,
		splitsLongMessages: false,
		typedCommandPrefix: "/",
		interactiveResume: true,
		supportsInchannelContinuable: false,
		requiresEditFinalize: false,
	},
);

/**
 * MagicMock-safe capability resolution (`is True` guard style — §2 guard
 * note): resolve booleans with `=== true` on a KNOWN default, never bare
 * truthiness of an arbitrary value.
 */
export function capabilityFlag(
	value: boolean | undefined,
	fallback: boolean,
): boolean {
	if (value === undefined || typeof value !== "boolean") return fallback;
	return value === true;
}

// ── Q17 rate tiers — budgets as MANIFEST DATA ────────────────────────────────

/** Egress operation classes a tier can budget. */
export type RateOp =
	| "send"
	| "edit"
	| "draft-start"
	| "draft-stop"
	| "typing"
	| "callback-answer";

export interface RateTier {
	name: string;
	ops: readonly RateOp[];
	limit: number;
	windowSeconds: number;
}

export interface RateBudget {
	tiers: readonly RateTier[];
}

/**
 * Consult a manifest before an egress op (runner-side consumer decides
 * scheduling; this helper only resolves WHICH tier governs an op).
 */
export function governingTier(
	budget: RateBudget | undefined,
	op: RateOp,
): RateTier | null {
	if (!budget) return null;
	for (const tier of budget.tiers) {
		if (tier.ops.includes(op)) return tier;
	}
	return null;
}
