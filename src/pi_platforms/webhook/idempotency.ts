// pi_platforms/webhook/idempotency — delivery-id idempotency store (DEC-017:
// "per-route rate limit + delivery-id idempotency"). Replays NEVER
// double-process: the first processing records its HTTP outcome and every
// replay within the TTL returns that cached outcome.
//
// Ported from the READ-ONLY Hermes reference:
//   webhook.py:_record_delivery_id (@449): Dict[delivery_id, seen_at],
//     _idempotency_ttl=3600 s; bounds = prune when len > max(rate_limit*2,
//     128); prune throttled to once per min(60, max(1, ttl/10)) s.
//   api_server.py:_IdempotCache (@1315): OUTCOME cache — replays return the
//     recorded response (max 1000 entries, TTL 300 s).
// The reference adapter unifies both: webhook.py's never-double-process
// seen-set carries api_server-style cached outcomes (proposed DEC-032 text
// in the phase report).

export interface CachedOutcome {
	status: number;
	body: Record<string, unknown>;
}

export interface IdempotencyOptions {
	/** Seen-entry TTL (webhook.py: 3_600_000 ms). */
	ttlMs?: number | undefined;
	/** Entry bound triggering prune (webhook.py: max(rate_limit*2, 128)). */
	maxEntries?: number | undefined;
	/** Prune throttle interval (webhook.py: 60_000 ms). */
	pruneIntervalMs?: number | undefined;
	/** Injected epoch-ms clock. */
	nowMs: () => number;
}

interface SeenEntry {
	seenAtMs: number;
	outcome?: CachedOutcome | undefined;
}

export type IdempotencyClaim =
	| { replay: false }
	| { replay: true; outcome: CachedOutcome | null };

export class DeliveryIdempotencyStore {
	private readonly seen = new Map<string, SeenEntry>();
	private readonly ttlMs: number;
	private readonly maxEntries: number;
	private readonly pruneIntervalMs: number;
	private readonly nowMs: () => number;
	private nextPruneAtMs = 0;

	constructor(opts: IdempotencyOptions) {
		this.ttlMs = opts.ttlMs ?? 3_600_000;
		this.maxEntries = opts.maxEntries ?? 128;
		this.pruneIntervalMs = opts.pruneIntervalMs ?? 60_000;
		this.nowMs = opts.nowMs;
	}

	/**
	 * Claim a delivery id (parity of _record_delivery_id): fresh/expired ids
	 * begin processing ({replay:false}); live ids return the recorded outcome
	 * when one exists, else a bare duplicate marker.
	 */
	begin(deliveryId: string): IdempotencyClaim {
		const now = this.nowMs();
		const existing = this.seen.get(deliveryId);
		if (existing !== undefined && now - existing.seenAtMs < this.ttlMs) {
			return { replay: true, outcome: existing.outcome ?? null };
		}
		if (existing !== undefined) {
			this.seen.delete(deliveryId); // expired entry re-arms
		}
		this.seen.set(deliveryId, { seenAtMs: now });
		if (this.seen.size > this.maxEntries) {
			this.prune(now);
		}
		return { replay: false };
	}

	/**
	 * Attach the processed outcome so replays answer with THE original
	 * response bytes-for-values (outcome-cache hit; never double-process).
	 */
	recordOutcome(deliveryId: string, outcome: CachedOutcome): void {
		const entry = this.seen.get(deliveryId);
		if (entry === undefined) return; // expired between begin and outcome
		entry.outcome = outcome;
	}

	size(): number {
		return this.seen.size;
	}

	/** webhook.py:_prune_seen_deliveries parity — drop TTL-expired entries. */
	private prune(now: number): void {
		if (now < this.nextPruneAtMs) return;
		const cutoff = now - this.ttlMs;
		for (const [id, entry] of this.seen) {
			if (entry.seenAtMs < cutoff) this.seen.delete(id);
		}
		this.nextPruneAtMs = now + this.pruneIntervalMs;
	}
}
