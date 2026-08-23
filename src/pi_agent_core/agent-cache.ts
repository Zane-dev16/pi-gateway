// pi_agent_core/agent-cache.ts — cached agent instances (05 §4: "get-or-build
// per session so system prompt and toolset stay byte-stable across turns")
// with the DEC-021 MEMORY-PRESSURE BOUND on top of LRU entry counting.
//
// Port parity: gateway/run.py::_agent_cache (OrderedDict + lock + cap
// enforcement + idle sweep) and gateway/agent_cache_pressure.py (byte-aware
// shedding + idle-TTL blind-spot removal).
//
// Why bytes matter (DEC-021): warm transcripts average megabytes each, so a
// count-based cap alone lets the cache reach gigabytes. Byte estimates are
// INJECTED (entryBytes at set time), keeping this module RSS-source-agnostic —
// tests drive it with synthetic accounting per the DEC verification clause.

export interface AgentCacheOptions {
	/** LRU entry cap (parity _agent_cache max entries). Default 512. */
	maxEntries?: number;
	/**
	 * DEC-021 pressure bound in bytes; when totalBytes exceeds it the
	 * least-recently-used entries are shed until back under budget.
	 */
	maxTotalBytes?: number;
	/** Entries idle longer than this are evicted by sweepIdle(). */
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
		this.maxEntries = Math.max(1, options.maxEntries ?? 512);
		this.maxTotalBytes = Math.max(
			0,
			options.maxTotalBytes ?? Number.POSITIVE_INFINITY,
		);
		this.idleTtlMs = Math.max(0, options.idleTtlMs ?? 30 * 60 * 1000);
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
	 * DEC-021 pressure shedding: evict least-recently-used entries until total
	 * bytes fit the bound. The most-recently-used entry is NEVER a victim — an
	 * active session's agent must not be shed out from under its turn even when
	 * it alone exceeds the budget (kept rather than thrashed on every op).
	 * Returns the evicted keys.
	 */
	shedPressure(): string[] {
		if (this.totalBytes <= this.maxTotalBytes) return [];
		const keys = [...this.entries.keys()];
		if (keys.length <= 1) return [];
		const protect = keys.at(-1);
		if (protect === undefined) return [];
		const evicted: string[] = [];
		for (const key of keys) {
			if (this.totalBytes <= this.maxTotalBytes) break;
			if (key === protect) continue;
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
		// Entry-cap eviction first (LRU), then byte-pressure shedding.
		while (this.entries.size > this.maxEntries) {
			const oldest = this.entries.keys().next();
			if (oldest.done) break;
			this.entries.delete(oldest.value);
		}
		this.shedPressure();
	}
}
