// pi_platforms/photon/dedupe — the bounded at-least-once replay window.
//
// adapter.py:_is_duplicate (@~1176) EXACT semantics, ported with an INJECTED
// clock (workspace injected-clock rule; source reads time.time()):
//   - seen within the 48h window ⇒ DUPLICATE (True);
//   - new OR expired ⇒ record (expired ids REFRESH insertion order) and
//     return False;
//   - a HARD size bound evicts OLDEST insertion-order entries past 4000 so a
//     burst of unique ids can't grow the map without limit — not just an
//     expired-only prune.

export const PHOTON_DEDUP_MAX_ENTRIES = 4000; // adapter.py:_DEDUP_MAX_SIZE
export const PHOTON_DEDUP_TTL_MS = 48 * 3600 * 1000; // _DEDUP_WINDOW_SECONDS

export interface DedupeWindowOptions {
	maxEntries?: number | undefined;
	ttlMs?: number | undefined;
	/** Injected monotonic clock in milliseconds. */
	nowMs: () => number;
}

export class DedupeWindow {
	private readonly seen = new Map<string, number>();
	private readonly maxEntries: number;
	private readonly ttlMs: number;
	private readonly nowMs: () => number;

	constructor(opts: DedupeWindowOptions) {
		this.maxEntries = Math.max(1, opts.maxEntries ?? PHOTON_DEDUP_MAX_ENTRIES);
		this.ttlMs = Math.max(1, opts.ttlMs ?? PHOTON_DEDUP_TTL_MS);
		this.nowMs = opts.nowMs;
	}

	/**
	 * adapter.py:_is_duplicate verdict for one delivery id. True ⇒ drop.
	 * Records the id either way (duplicates refresh nothing — the FIRST
	 * timestamp wins inside the window; expired ids re-record fresh).
	 */
	isDuplicate(msgId: string): boolean {
		const now = this.nowMs();
		const t = this.seen.get(msgId);
		if (t !== undefined && now - t < this.ttlMs) {
			return true; // seen, unexpired
		}
		if (this.seen.has(msgId)) {
			this.seen.delete(msgId); // expired: refresh insertion order
		}
		this.seen.set(msgId, now);
		if (this.seen.size > this.maxEntries) {
			const excess = this.seen.size - this.maxEntries;
			const iter = this.seen.keys();
			for (let i = 0; i < excess; i += 1) {
				const oldest = iter.next();
				if (oldest.done) break;
				this.seen.delete(oldest.value);
			}
		}
		return false;
	}

	get size(): number {
		return this.seen.size;
	}

	has(msgId: string): boolean {
		return this.seen.has(msgId);
	}
}
