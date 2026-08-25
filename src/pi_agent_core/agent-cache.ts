// pi_agent_core/agent-cache.ts — cached agent instances (05 §4: "get-or-build
// per session so system prompt and toolset stay byte-stable across turns")
// with the DEC-021 MEMORY-PRESSURE BOUND on top of LRU entry counting.
//
// Port parity: gateway/run.py::_AGENT_CACHE_MAX_SIZE / _AGENT_CACHE_IDLE_TTL_SECS
// (OrderedDict + lock + cap enforcement + idle sweep; 128 entries / 3600s idle)
// and gateway/agent_cache_pressure.py:plan_pressure_evictions (byte-aware
// shedding: per-pass eviction cap + hottest-entries protection).
//
// Why bytes matter (DEC-021): warm transcripts average megabytes each, so a
// count-based cap alone lets the cache reach gigabytes. Byte estimates are
// INJECTED (entryBytes at set time), keeping this module RSS-source-agnostic —
// tests drive it with synthetic accounting per the DEC verification clause.

export interface AgentCacheOptions {
	/** LRU entry cap (parity _AGENT_CACHE_MAX_SIZE). Default 128. */
	maxEntries?: number;
	/**
	 * DEC-021 pressure bound in bytes; when totalBytes exceeds it the
	 * least-recently-used entries are shed until back under budget.
	 */
	maxTotalBytes?: number;
	/** Entries idle longer than this are evicted by sweepIdle(). Default 1h. */
	idleTtlMs?: number;
	/** Injected clock for recency/idle tracking. */
	now?: () => number;
}

export interface CacheEntryView {
	key: string;
	bytes: number;
	lastUsedAt: number;
	createdAt: number;
}

interface InternalEntry<V> {
	value: V;
	bytes: number;
	lastUsedAt: number;
	createdAt: number;
}

export class AgentInstanceCache<V = unknown> {
	private readonly entries = new Map<string, InternalEntry<V>>();
	private readonly maxEntries: number;
	private readonly maxTotalBytes: number;
	private readonly idleTtlMs: number;
	private readonly now: () => number;

	constructor(options: AgentCacheOptions = {}) {
		// Parity run.py:_AGENT_CACHE_MAX_SIZE = 128 / _AGENT_CACHE_IDLE_TTL_SECS
		// = 3600.0 — the shipped gateway cache bounds.
		this.maxEntries = Math.max(1, options.maxEntries ?? 128);
		this.maxTotalBytes = Math.max(
			0,
			options.maxTotalBytes ?? Number.POSITIVE_INFINITY,
		);
		this.idleTtlMs = Math.max(0, options.idleTtlMs ?? 3_600_000);
		this.now = options.now ?? (() => Date.now());
	}

	get size(): number {
		return this.entries.size;
	}

	get totalBytes(): number {
		let total = 0;
		for (const entry of this.entries.values()) total += entry.bytes;
		return total;
	}

	keys(): string[] {
		return [...this.entries.keys()];
	}

	view(): CacheEntryView[] {
		return [...this.entries.entries()].map(([key, e]) => ({
			key,
			bytes: e.bytes,
			lastUsedAt: e.lastUsedAt,
			createdAt: e.createdAt,
		}));
	}

	has(key: string): boolean {
		return this.entries.has(key);
	}

	/** Get-or-bump-recency. Map iteration order = LRU order (oldest first). */
	get(key: string): V | undefined {
		const entry = this.entries.get(key);
		if (!entry) return undefined;
		entry.lastUsedAt = this.now();
		// Refresh insertion position so iteration order tracks recency.
		this.entries.delete(key);
		this.entries.set(key, entry);
		return entry.value;
	}

	/** Peek WITHOUT bumping recency (diagnostics). */
	peek(key: string): V | undefined {
		return this.entries.get(key)?.value;
	}

	set(key: string, value: V, bytes: number): void {
		const existing = this.entries.get(key);
		if (existing) {
			existing.value = value;
			existing.bytes = bytes;
			existing.lastUsedAt = this.now();
			return;
		}
		const ts = this.now();
		this.entries.set(key, {
			value,
			bytes: Math.max(0, bytes),
			createdAt: ts,
			lastUsedAt: ts,
		});
		this.enforceBounds();
	}

	delete(key: string): boolean {
		return this.entries.delete(key);
	}

	clear(): void {
		this.entries.clear();
	}

	/**
	 * DEC-021 pressure shedding — port of agent_cache_pressure.py:
	 * plan_pressure_evictions. Evicts least-recently-used entries until total
	 * bytes fit the bound, under TWO guards Hermes learned the hard way:
	 *
	 *  • maxEvictionsPerPass caps ONE pass so a single call cannot stall the
	 *    caller tearing down dozens of clients; over-budget caches shed across
	 *    successive passes instead.
	 *  • protectRecent is an UPPER BOUND on the hottest entries spared, CLAMPED
	 *    to len//2 (parity protect_recent): a few huge sessions can exhaust the
	 *    budget alone, and an unclamped guard would then protect the entire
	 *    cache while the process climbs toward the OOM killer with nothing it
	 *    is willing to shed.
	 *
	 * Returns the evicted keys in eviction order (oldest first).
	 */
	shedPressure(
		options: { protectRecent?: number; maxEvictionsPerPass?: number } = {},
	): string[] {
		const maxEvictions = Math.trunc(
			options.maxEvictionsPerPass ?? DEFAULT_MAX_EVICTIONS_PER_PASS,
		);
		if (maxEvictions <= 0 || this.entries.size === 0) return [];
		if (this.totalBytes <= this.maxTotalBytes) return [];
		// Clamp to half the cache (plan_pressure_evictions' `min(..., len//2)`):
		// Map iteration order IS LRU→MRU order, so the protected tail is the
		// hottest slice.
		const requestedProtect = Math.max(
			0,
			Math.trunc(options.protectRecent ?? DEFAULT_PROTECT_RECENT),
		);
		const protect = Math.min(
			requestedProtect,
			Math.floor(this.entries.size / 2),
		);
		const candidates = [...this.entries.keys()];
		const evictable =
			protect > 0
				? candidates.slice(0, candidates.length - protect)
				: candidates;
		const evicted: string[] = [];
		for (const key of evictable) {
			if (
				evicted.length >= maxEvictions ||
				this.totalBytes <= this.maxTotalBytes
			) {
				break;
			}
			this.entries.delete(key);
			evicted.push(key);
		}
		return evicted;
	}

	/** Idle sweep (parity _evict-style idle removal): drop entries past TTL. */
	sweepIdle(): string[] {
		const cutoff = this.now() - this.idleTtlMs;
		const evicted: string[] = [];
		for (const [key, entry] of this.entries) {
			if (entry.lastUsedAt <= cutoff) {
				this.entries.delete(key);
				evicted.push(key);
			}
		}
		return evicted;
	}

	private enforceBounds(): void {
		// Entry-cap eviction first (LRU), then byte-pressure shedding (capped
		// per pass — see shedPressure).
		while (this.entries.size > this.maxEntries) {
			const oldest = this.entries.keys().next();
			if (oldest.done) break;
			this.entries.delete(oldest.value);
		}
		this.shedPressure();
	}
}

/** Parity agent_cache_pressure.py:_DEFAULT_MAX_EVICTIONS_PER_PASS. */
export const DEFAULT_MAX_EVICTIONS_PER_PASS = 16;

/** Parity agent_cache_pressure.py:_DEFAULT_PROTECT_RECENT. */
export const DEFAULT_PROTECT_RECENT = 8;
