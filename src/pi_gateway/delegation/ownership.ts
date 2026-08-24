// ownership.ts — §7.2 ownership decision-table groundwork, AS DATA.
//
// The delivery rail's verdict vocabulary and the user-boundary drop set are
// shared by BOTH consumers of a completion (06 §7.2): the pre-flight
// classifier (gateway/run.py:_classify_completion_target) that proves
// deliverability BEFORE adapter acceptance, and the in-pipeline resolver
// (gateway/run.py:_resolve_async_delegation_session) that owns actual
// routing. Hermes binds them to the SAME module-level tuple so verdict and
// routing can never drift (a disagreeing pair falsely acks permanent loss —
// 2026-08-09 staging incident, defect #2). This module is that shared data.
//
// RESOLUTION SCOPE: only the PURE mapping is realized here. The DB lookups
// that feed it (get_session / get_compression_tip) are Phase-5 watcher
// wiring (explicitly out of this phase); the classifier takes their results
// as plain data instead so every decision-table row is testable NOW.

/**
 * End reasons that mean the USER deliberately closed this thread of work
 * (/new → session_reset / new_session, an explicit exit, or a /switch).
 * Parity anchor: gateway/run.py:_USER_BOUNDARY_END_REASONS.
 * Completions whose parent ended at one of these are terminally DROPPED —
 * never resurrect a closed conversation (#55578).
 */
export const USER_BOUNDARY_END_REASONS = [
	"session_reset",
	"user_exit",
	"session_switch",
	"new_session",
] as const;

export type UserBoundaryEndReason = (typeof USER_BOUNDARY_END_REASONS)[number];

/** Pre-flight verdict parity of _classify_completion_target docstring. */
export type CompletionVerdict = "deliver" | "terminal" | "retry";

/** The parent-session facts the pure classifier needs (no DB access). */
export interface ParentSnapshot {
	/** null when the row is live (never ended). */
	endedAt: number | null;
	endReason: string | null;
	/** Parent row id — lets the lineage check reject a SELF-referential tip. */
	parentSessionId?: string;
}

/** Compression-lineage facts for a compression-ended parent (tip probe). */
export interface LineageTip {
	/** null/absent = tip not visible yet (rotation caught mid-flight). */
	tipSessionId: string | null;
	/** undefined when the tip row could not be read. */
	endedAt?: number | null;
}

export function isUserBoundaryEnd(
	endReason: string | null | undefined,
): endReason is UserBoundaryEndReason {
	return (
		endReason !== null &&
		endReason !== undefined &&
		(USER_BOUNDARY_END_REASONS as readonly string[]).includes(endReason)
	);
}

/**
 * Pure core of gateway/run.py:_classify_completion_target. `lookupError`
 * models "session DB unavailable / lookup threw"; a missing parent models an
 * unknown row; `tip` is only consulted for compression-ended parents.
 *
 * Decision table (06 §7.2):
 *   lookup error                        → retry   (release claim; cap bounds churn)
 *   unknown parent row                  → terminal (DROP fail-closed; queryable)
 *   live parent                         → deliver (pinned session)
 *   user-boundary end                   → terminal (DROP #55578)
 *   idle/timeout/lifecycle end          → deliver (chat's CURRENT session — retarget)
 *   compression end, no/self tip        → retry   (mid-rotation hold)
 *   compression end, tip ended/missing  → retry   (hold for later consumer)
 *   compression end, live tip           → deliver (lineage tip; route owns lineage)
 */
export function classifyCompletionTarget(
	parent: ParentSnapshot | null | undefined,
	tip?: LineageTip,
	lookupError?: boolean,
): CompletionVerdict {
	if (lookupError === true) return "retry";
	if (parent === null || parent === undefined) return "terminal"; // fail-closed
	if (parent.endedAt === null || parent.endedAt === undefined) return "deliver";
	const endReason = String(parent.endReason ?? "");
	if (endReason !== "compression") {
		if (isUserBoundaryEnd(endReason)) return "terminal";
		return "deliver"; // idle/timeout/lifecycle ends retarget
	}
	// Compression lineage: a stale route is honored ONLY with a verified live
	// continuation (route-owns-lineage invariant; DEC-018 rejected recency).
	const tipId = tip?.tipSessionId ?? null;
	if (!tipId || tipId === "") return "retry";
	if (
		parent.parentSessionId !== undefined &&
		tipId === parent.parentSessionId
	) {
		return "retry"; // rotation caught mid-flight: continuation not visible yet
	}
	const tipEnded = tip?.endedAt;
	if (tipEnded === undefined) return "retry"; // tip row unreadable
	if (tipEnded !== null && tipEnded !== undefined) return "retry"; // tip ended
	return "deliver";
}

/**
 * The durable disposition each verdict demands of the store handshake.
 * terminal ⇒ dropClaim (honest ack: NOT delivered, NOT pending-replay);
 * retry    ⇒ releaseClaim (transient failure; attempt cap converges);
 * deliver  ⇒ inject, then completeClaim/markDelivered AFTER adapter acceptance.
 */
export type StoreDisposition = "complete-after-inject" | "drop" | "release";

export function dispositionFor(verdict: CompletionVerdict): StoreDisposition {
	switch (verdict) {
		case "terminal":
			return "drop";
		case "retry":
			return "release";
		case "deliver":
			return "complete-after-inject";
		default: {
			const exhaustive: never = verdict;
			throw new Error(`unknown completion verdict: ${String(exhaustive)}`);
		}
	}
}
