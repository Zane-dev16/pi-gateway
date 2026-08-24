// delivery.ts — deliver a same-session GROUP of durable completions as ONE
// forged turn through the normal pipeline.
//
// Port of gateway/run.py:_deliver_completion_notification (per-event path:
// claim → pre-flight → inject → ack-after-acceptance, release-on-anything-
// else) composed under _deliver_async_delegation_group (#70300: a fan-out
// finishing together must enter as ONE consolidated turn, never N turns).
//
// Dispositions ride the Phase-4 rail handshake EXACTLY as 06 §7.1 binds them:
//   terminal → dropClaim        (honest ack: NOT delivered, NOT pending)
//   retry    → releaseClaim     (attempt counter burns at CLAIM time; the cap
//                               converges unroutable rows to 'dropped')
//   deliver  → dispatch forged turn, THEN completeClaim (ack ONLY after
//                               adapter acceptance — invariant 2)
//
// OBLIGATIONS SEAM NOTE: the watcher performs NO bespoke chat send. The
// forged event enters through the NORMAL ingress dispatcher (both guards,
// DEC-022 push lane), so the reply egress rides the standard delivery-
// obligations pipeline (03 §9 — obligation recorded before first attempt).
// Delivery "to the chat" therefore inherits at-least-once redelivery from the
// obligations ledger instead of a second, divergent send path here.
//
// BUSY GATE (invariant 1): before ANY claim, the resolved target session is
// probed for an active turn. A busy target leaves every row in the group
// unclaimed and untouched — re-entry WAITS for idle end (06 §10 idle-gate
// race row), and no attempt budget burns while a long turn runs.

import type { DelegationRail } from "../../pi_gateway/delegation/index.js";
import type { IncomingEvent } from "../../pi_gateway/guards/events.js";
import type { GatewayClock } from "./clock.js";
import { systemClock } from "./clock.js";
import {
	formatAsyncDelegation,
	formatCoalescedAsyncDelegations,
} from "./formatter.js";
import type { TurnLiveness } from "./idle-gate.js";
import type {
	SessionOwnershipResolver,
	OwnershipResolution,
	ResolvedSessionFacts,
} from "./ownership-resolver.js";
import type { PendingCompletion } from "./pending.js";

/** The normal-pipeline dispatch port (handoff SyntheticTurnDispatcher parity). */
export interface SyntheticTurnDispatcher {
	dispatch(event: IncomingEvent): Promise<void>;
}

export interface DeliveryLogger {
	debug?(message: string, meta?: Record<string, unknown>): void;
	info?(message: string, meta?: Record<string, unknown>): void;
	warn?(message: string, meta?: Record<string, unknown>): void;
}

export interface CompletionDeliveryDeps {
	rail: DelegationRail;
	resolver: SessionOwnershipResolver;
	liveness: TurnLiveness;
	/**
	 * The forged-turn port. Production composes it over the destination L1
	 * guard (GuardQuiesceDispatcher shape — see pi_embedded/handoff/dispatcher.ts);
	 * the completion rides BOTH guards like any turn (DEC-022 push lane).
	 */
	dispatcher: SyntheticTurnDispatcher;
	clock?: GatewayClock;
	log?: DeliveryLogger;
}

export type GroupDisposition =
	/** Forged turn accepted; every claimed row acked 'delivered'. */
	| "delivered"
	/** Target mid-turn — group left pending unclaimed; wait for idle end. */
	| "busy"
	/** Permanent loss — claimed rows terminally dropped (payload queryable). */
	| "dropped"
	/** Transient — claims released for a later consumer / attempt-cap churn. */
	| "retry"
	/** Another consumer owned every row before this tick could claim. */
	| "owned-elsewhere"
	/** No member formatted into a notification — rows left for the age cap. */
	| "unformattable";

export interface GroupDeliveryReport {
	disposition: GroupDisposition;
	delegationIds: string[];
	/** Rows excluded because another consumer won their claim (stay pending). */
	excluded: string[];
	targetSessionId: string | null;
	reason?: string;
}

export class CompletionDeliveryEngine {
	private readonly rail: DelegationRail;
	private readonly resolver: SessionOwnershipResolver;
	private readonly liveness: TurnLiveness;
	private readonly dispatcher: SyntheticTurnDispatcher;
	private readonly clock: GatewayClock;
	private readonly log: DeliveryLogger | undefined;

	constructor(deps: CompletionDeliveryDeps) {
		this.rail = deps.rail;
		this.resolver = deps.resolver;
		this.liveness = deps.liveness;
		this.dispatcher = deps.dispatcher;
		this.clock = deps.clock ?? systemClock;
		this.log = deps.log;
	}

	/**
	 * Deliver one same-route group. Members MUST share origin_session +
	 * parent_session_id (the watcher groups them so). Never throws: a throwing
	 * dispatcher becomes a release-and-retry, mirroring the Hermes finally arm.
	 */
	async deliverGroup(
		members: PendingCompletion[],
	): Promise<GroupDeliveryReport> {
		const ids = members.map((m) => m.delegationId);
		if (members.length === 0) {
			return {
				disposition: "unformattable",
				delegationIds: [],
				excluded: [],
				targetSessionId: null,
			};
		}
		const primary = members[0];
		if (primary === undefined) {
			return {
				disposition: "unformattable",
				delegationIds: ids,
				excluded: [],
				targetSessionId: null,
			};
		}

		// ---- Ownership resolution (pre-flight + routing) BEFORE any claim. ---
		let resolution: OwnershipResolution;
		try {
			resolution = this.resolver.resolve({
				originSessionKey: primary.originSession,
				parentSessionId: primary.parentSessionId,
			});
		} catch (err) {
			resolution = {
				verdict: "retry",
				target: null,
				reason: `ownership lookup failed: ${String(err)}`,
			};
		}

		// Busy gate FIRST on a deliver verdict — waiting must not burn budget.
		if (
			resolution.verdict === "deliver" &&
			resolution.target !== null &&
			this.liveness.isBusy(resolution.target.sessionId)
		) {
			this.log?.debug?.("async delegation waits for idle target", {
				delegation_id: primary.delegationId,
				target: resolution.target.sessionId,
			});
			return {
				disposition: "busy",
				delegationIds: ids,
				excluded: [],
				targetSessionId: resolution.target.sessionId,
				reason: "target session mid-turn; re-entry waits for idle end",
			};
		}

		// ---- Claim phase: atomic; losers stay pending OUT of our text. -------
		const claimed: PendingCompletion[] = [];
		const excluded: string[] = [];
		try {
			for (const m of members) {
				const claimId = this.rail.makeClaimId("gateway");
				const won = await this.rail.claimCompletion(m.delegationId, claimId);
				if (won) {
					claimed.push(m);
					this.claimTokens.set(m.delegationId, claimId);
				} else {
					excluded.push(m.delegationId);
				}
			}
		} catch (err) {
			await this.releaseAll(claimed);
			return {
				disposition: "retry",
				delegationIds: ids,
				excluded,
				targetSessionId: null,
				reason: `claim phase failed: ${err instanceof Error ? err.message : String(err)}`,
			};
		}
		if (claimed.length === 0) {
			return {
				disposition: "owned-elsewhere",
				delegationIds: ids,
				excluded,
				targetSessionId: resolution.target?.sessionId ?? null,
				reason: "another consumer holds every claim",
			};
		}

		try {
			switch (resolution.verdict) {
				case "terminal": {
					for (const m of claimed) {
						await this.rail.dropClaim(
							m.delegationId,
							this.claimIdOf(m.delegationId),
						);
					}
					this.log?.warn?.(
						"async delegation targets permanently-gone session; terminally dropping (result stays queryable)",
						{
							delegation_ids: claimed.map((m) => m.delegationId),
							parent_session_id: primary.parentSessionId,
							reason: resolution.reason,
						},
					);
					return {
						disposition: "dropped",
						delegationIds: claimed.map((m) => m.delegationId),
						excluded,
						targetSessionId: null,
						reason: resolution.reason,
					};
				}
				case "retry": {
					for (const m of claimed) {
						await this.rail.releaseClaim(
							m.delegationId,
							this.claimIdOf(m.delegationId),
						);
					}
					return {
						disposition: "retry",
						delegationIds: claimed.map((m) => m.delegationId),
						excluded,
						targetSessionId: null,
						reason: resolution.reason,
					};
				}
				case "deliver":
					return await this.dispatchClaimed(claimed, excluded, resolution);
				default: {
					const exhaustive: never = resolution.verdict;
					throw new Error(`unknown verdict ${String(exhaustive)}`);
				}
			}
		} catch (err) {
			// Containment: any unexpected throw releases every held claim —
			// never strand a row behind a live stale claim for 300 s.
			await this.releaseAll(claimed);
			return {
				disposition: "retry",
				delegationIds: claimed.map((m) => m.delegationId),
				excluded,
				targetSessionId: null,
				reason: `delivery error: ${err instanceof Error ? err.message : String(err)}`,
			};
		}
	}

	// ------------------------------------------------------------------

	private async dispatchClaimed(
		claimed: PendingCompletion[],
		excluded: string[],
		resolution: OwnershipResolution,
	): Promise<GroupDeliveryReport> {
		const target = resolution.target;
		if (target === null) {
			// Resolver said deliver without a concrete target — fail closed via release.
			await this.releaseAll(claimed);
			return {
				disposition: "retry",
				delegationIds: claimed.map((m) => m.delegationId),
				excluded,
				targetSessionId: null,
				reason: "deliver verdict without target (fail-closed)",
			};
		}
		const primary = claimed[0];
		if (primary === undefined) {
			await this.releaseAll(claimed);
			return {
				disposition: "retry",
				delegationIds: [],
				excluded,
				targetSessionId: target.sessionId,
				reason: "no claimed member",
			};
		}

		// Format AFTER claims so a sibling lost to another consumer is excluded
		// from the consolidated text entirely (double-delivery guard, #70300).
		const nowSeconds = this.clock.nowSeconds();
		const blocks: Array<{ member: PendingCompletion; text: string }> = [];
		for (const m of claimed) {
			const text = formatAsyncDelegation({ ...m.event }, nowSeconds);
			if (text !== null && text !== "") blocks.push({ member: m, text });
		}
		if (blocks.length === 0) {
			await this.releaseAll(claimed);
			return {
				disposition: "unformattable",
				delegationIds: claimed.map((m) => m.delegationId),
				excluded,
				targetSessionId: target.sessionId,
				reason: "no member formatted into a notification",
			};
		}

		const consolidated =
			blocks.length === 1
				? (blocks[0]?.text ?? "")
				: formatCoalescedAsyncDelegations(blocks.map((b) => b.text));

		const event = forgeCompletionEvent({
			text: consolidated,
			originSessionKey: primary.originSession,
			parentSessionId: primary.parentSessionId,
			target,
		});

		this.log?.info?.("injecting async-delegation completion as new turn", {
			delegation_ids: blocks.map((b) => b.member.delegationId),
			session_key: primary.originSession,
			target: target.sessionId,
			consolidated: blocks.length > 1,
		});

		// Adapter acceptance is the ack boundary. A rejection/throw lands here
		// as a throw ⇒ caller releases (at-least-once, never false-acked).
		await this.dispatcher.dispatch(event);

		for (const b of blocks) {
			await this.rail.completeClaim(
				b.member.delegationId,
				this.claimIdOf(b.member.delegationId),
			);
		}
		// Members we claimed but could not format (shouldn't happen — formatting
		// is deterministic over persisted events) still get released honestly.
		const unformatted = claimed.filter(
			(m) => !blocks.some((b) => b.member.delegationId === m.delegationId),
		);
		if (unformatted.length > 0) await this.releaseAll(unformatted);

		return {
			disposition: "delivered",
			delegationIds: blocks.map((b) => b.member.delegationId),
			excluded: [...excluded, ...unformatted.map((m) => m.delegationId)],
			targetSessionId: target.sessionId,
		};
	}

	private async releaseAll(members: PendingCompletion[]): Promise<void> {
		for (const m of members) {
			try {
				await this.rail.releaseClaim(
					m.delegationId,
					this.claimIdOf(m.delegationId),
				);
			} catch (err) {
				this.log?.debug?.("release failed", {
					delegation_id: m.delegationId,
					error: String(err),
				});
			}
		}
	}

	// Claim-token bookkeeping: delegation_id → token handed out at claim time.
	private readonly claimTokens = new Map<string, string>();

	private claimIdOf(delegationId: string): string {
		const id = this.claimTokens.get(delegationId) ?? "";
		this.claimTokens.delete(delegationId);
		return id;
	}
}

// --------------------------------------------------------------------
// Event forging (DEC-022 push lane)
// --------------------------------------------------------------------

/**
 * Forge the synthetic internal MessageEvent that carries the completion back
 * through the NORMAL pipeline:
 *   - internal=true — the push lane; traverses BOTH guards like any turn;
 *   - metadata.gateway_session_key — THE routing key captured at dispatch
 *     (delegations.origin_session); ingress resolves key → current session;
 *   - metadata.gateway_session_id — the spawning parent (_inject_watch_
 *     notification metadata parity);
 *   - source — the RESOLVED target's stored SessionSource snapshot
 *     (sessions.origin_json): single source of truth, richer than parsing the
 *     key back apart (proposed-DEC note in the module report).
 */
export function forgeCompletionEvent(input: {
	text: string;
	originSessionKey: string;
	parentSessionId: string | null;
	target: ResolvedSessionFacts;
}): IncomingEvent {
	const metadata: Record<string, unknown> = {
		gateway_session_key: input.originSessionKey,
	};
	if (input.parentSessionId) {
		metadata["gateway_session_id"] = input.parentSessionId;
	}
	return {
		messageType: "text",
		text: input.text,
		internal: true,
		...(input.target.source !== null ? { source: input.target.source } : {}),
		metadata,
	};
}
