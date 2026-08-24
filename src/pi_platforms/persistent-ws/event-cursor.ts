// pi_platforms/persistent-ws/event-cursor — the resubscribe replay window's
// client state: the resume CURSOR (last delivered event id) plus the bounded
// redelivery DEDUPLICATOR that makes server at-least-once replay
// exactly-once downstream.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/slack/adapter.py:_slack_dedup_ttl_seconds — "Slack
//     buffers un-acked Socket Mode events and replays them when the websocket
//     reconnects. The replay can arrive several minutes after the original…
//     TTL must outlast Slack's worst-case reconnect-redelivery gap" → default
//     3600s, memory bounded by LRU pruning NOT by the TTL (#4777).
//   adapter.py event dedup call site: dedup BEFORE dispatch; workspace-scoped
//     ids (Pi: platform-scoped event ids from one fake server — same shape).

import type { NowFn } from "./manual-clock.js";

export const DEFAULT_DEDUP_TTL_MS = 3_600_000; // 1h — covers reconnect windows
export const DEFAULT_DEDUP_MAX_ENTRIES = 1000;

/**
 * Bounded TTL+LRU seen-set. `isDuplicate(id)` records AND answers in one call
 * (the adapter never needs a separate mark step).
 */
export class EventDeduplicator {
	private readonly seen: Map<string, number> = new Map();
	private readonly ttlMs: number;
	private readonly maxEntries: number;
	private readonly nowFn: NowFn;
	/** Times an incoming id was suppressed as already-delivered. */
	suppressedCount = 0;

	constructor(
		opts: { ttlMs?: number; maxEntries?: number; nowMs?: NowFn } = {},
	) {
		this.ttlMs = opts.ttlMs ?? DEFAULT_DEDUP_TTL_MS;
		this.maxEntries = opts.maxEntries ?? DEFAULT_DEDUP_MAX_ENTRIES;
		this.nowFn = opts.nowMs ?? (() => Date.now());
	}

	isDuplicate(id: string): boolean {
		const now = this.nowFn();
		// Expire lazily from the head (insertion-ordered).
		for (const [key, at] of this.seen) {
			if (now - at > this.ttlMs) this.seen.delete(key);
			else break;
		}
		const prior = this.seen.get(id);
		if (prior !== undefined && now - prior <= this.ttlMs) {
			this.suppressedCount += 1;
			// Refresh recency (LRU touch).
			this.seen.delete(id);
			this.seen.set(id, now);
			return true;
		}
		this.seen.delete(id); // re-insert to move to MRU end
		this.seen.set(id, now);
		while (this.seen.size > this.maxEntries) {
			const oldest = this.seen.keys().next();
			if (oldest.done) break;
			this.seen.delete(oldest.value);
		}
		return false;
	}

	get size(): number {
		return this.seen.size;
	}
}

/**
 * The resume cursor. Cold boot subscribes with null (server may treat stale
 * backlog per its own policy); every SUCCESSFULLY dispatched inbound event
 * advances it — reconnect resubscribes with THIS value so the server replays
 * exactly the undelivered window.
 */
export class ResumeCursor {
	private lastEventId: string | null = null;

	get value(): string | null {
		return this.lastEventId;
	}

	advance(eventId: string): void {
		this.lastEventId = eventId;
	}
}
