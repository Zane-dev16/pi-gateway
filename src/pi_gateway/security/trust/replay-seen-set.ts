// pi_gateway/security/trust/replay-seen-set — bounded seen-set for replay
// protection and receipt dedupe (06 §8.1/§8.3; DEC-017).
//
// Ported from the READ-ONLY Hermes reference:
//   gateway/platforms/msgraph_webhook.py:_remember_receipt (@378) — set +
//     insertion-order deque, evict OLDEST past _max_seen_receipts (default
//     DEFAULT_MAX_SEEN_RECEIPTS = 5000): memory is bounded by construction,
//     not by hope.
//   gateway/platforms/webhook.py idempotency seen-set — entries expire after
//     a TTL (3600 s) and are pruned lazily (bound ≈ rate_limit × TTL).

/** msgraph_webhook.py DEFAULT_MAX_SEEN_RECEIPTS (@42). */
export const DEFAULT_MAX_SEEN_ENTRIES = 5_000;
/** webhook.py `_idempotency_ttl` (@223) — entry lifetime in ms. */
export const SEEN_SET_TTL_MS = 3_600_000;

export interface BoundedSeenSetOptions {
	/** Hard entry bound; the OLDEST insertion is evicted past it. */
	maxEntries?: number | undefined;
	/**
	 * Entry lifetime; expired entries stop matching and are dropped lazily.
	 * Explicit null = NO TTL (pure FIFO bound — msgraph receipt-dedupe shape);
	 * omitted = the webhook idempotency default (3600 s).
	 */
	ttlMs?: number | null | undefined;
	/** Injected epoch-ms clock — no wall-clock reads anywhere. */
	nowMs: () => number;
}

interface SeenEntry {
	readonly insertedAtMs: number;
}

/**
 * FIFO/TTL-bounded membership set. `add` is the single admission primitive:
 * true ⇒ first sighting (admit), false ⇒ live duplicate (reject as replay).
 * Memory bound = min(maxEntries, live TTL window) — eviction keeps the map
 * at ≤ maxEntries even under unlimited adversarial key churn.
 */
export class BoundedSeenSet {
	private readonly entries = new Map<string, SeenEntry>();
	private readonly maxEntries: number;
	private readonly ttlMs: number | null;
	private readonly nowMs: () => number;

	constructor(options: BoundedSeenSetOptions) {
		this.maxEntries = Math.max(
			1,
			options.maxEntries ?? DEFAULT_MAX_SEEN_ENTRIES,
		);
		this.ttlMs =
			options.ttlMs === null ? null : (options.ttlMs ?? SEEN_SET_TTL_MS);
		this.nowMs = options.nowMs;
	}

	/**
	 * Record `key`; return FALSE when a LIVE entry already exists (replay).
	 * Expired entries are treated as unseen and re-armed (parity of the
	 * webhook idempotency store's expired-entry re-arm).
	 */
	add(key: string): boolean {
		const now = this.nowMs();
		const existing = this.entries.get(key);
		if (existing !== undefined && !this.expired(existing, now)) {
			return false;
		}
		if (existing !== undefined) {
			this.entries.delete(key); // re-insert to refresh FIFO position
		}
		this.entries.set(key, { insertedAtMs: now });
		while (this.entries.size > this.maxEntries) {
			const oldest = this.entries.keys().next();
			if (oldest.done === true) break;
			this.entries.delete(oldest.value);
		}
		return true;
	}

	/** Live-membership probe (does NOT insert). */
	has(key: string): boolean {
		const existing = this.entries.get(key);
		if (existing === undefined) return false;
		if (this.expired(existing, this.nowMs())) return false;
		return true;
	}

	/** Drop every TTL-expired entry; returns the count removed. */
	pruneExpired(): number {
		const now = this.nowMs();
		let removed = 0;
		for (const [key, entry] of this.entries) {
			if (this.expired(entry, now)) {
				this.entries.delete(key);
				removed += 1;
			}
		}
		return removed;
	}

	size(): number {
		return this.entries.size;
	}

	private expired(entry: SeenEntry, nowMs: number): boolean {
		return this.ttlMs !== null && nowMs - entry.insertedAtMs >= this.ttlMs;
	}
}
