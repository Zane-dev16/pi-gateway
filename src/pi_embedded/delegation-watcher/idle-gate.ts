// idle-gate.ts — the §7.2 invariant-1 gate: completions enter as NEW turns
// ONLY when the target session is idle; NEVER spliced between tool results
// (05 §5 alternation + cache-prefix contract; 06 §10 "idle-gate race" row).
//
// MECHANISM NOTE (vs Hermes letter, same observable contract): Hermes parks
// undeliverable-while-busy events in the shared in-memory completion queue —
// the post-turn drain picks them up when the turn ends, so the durable row is
// never claimed while busy and no attempt budget burns. Pi has no second
// in-memory lane to hide rows in; the DURABLE ROW ITSELF is the queue. The
// watcher therefore probes liveness BEFORE claiming: a busy target leaves the
// row fully untouched (delivery_state 'pending', attempts unchanged), and the
// next tick after idle end delivers it exactly once. Claim churn stays zero
// during long turns instead of exhausting MAX_DELIVERY_ATTEMPTS in ~16 s of
// 2 s polls.
//
// The gate is a structural seam so tests inject deterministic busy/idle
// states; production composes it over the L1 turn-lease registry
// (pi_gateway/guards/turn-lease.ts SessionTurnLeaseRegistry.holderOf) — the
// same registry every real turn holds through its whole agent loop.

/**
 * Liveness view over one resolved session id. `true` = an agent turn is
 * currently running on that session (lease held); `false` = idle.
 */
export interface TurnLiveness {
	isBusy(sessionId: string): boolean;
}

/** Minimal structural shape of the turn-lease registry this adapter reads. */
export interface TurnLeaseHeldView {
	holderOf(
		sessionId: string,
	): { readonly released?: boolean } | null | undefined;
}

/**
 * Production adapter: a session is busy exactly while it holds a turn lease
 * token that has not been released. Released tokens never count as busy.
 */
export function turnLeaseLiveness(registry: TurnLeaseHeldView): TurnLiveness {
	return {
		isBusy(sessionId: string): boolean {
			const holder = registry.holderOf(sessionId);
			return holder != null && holder.released !== true;
		},
	};
}
