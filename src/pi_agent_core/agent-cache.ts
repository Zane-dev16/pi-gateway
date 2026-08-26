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
//
// Where the default budget comes from: agent_cache_pressure.py derives it
// from the memory limit the process actually runs under — cgroup v2/v1 quota,
// else host RAM — at _AUTO_BUDGET_FRACTION (0.65), disabled below
// _AUTO_BUDGET_FLOOR_MB (512MB). resolveAutoMaxTotalBytes ports that ladder so
// the pressure bound is LIVE BY DEFAULT (run.py resolves "auto" bounds without
// operator config); an explicit maxTotalBytes option always wins.
//
// Eviction is never purely positional (stability round: secops-13). Hermes
// sheds ONLY through an is_evictable gate — gateway/run.py:_is_evictable
// (mid-turn agents excluded; transcript persistence must have caught up,
// agent_cache_pressure.py:transcript_persistence_caught_up) — because "a
// skipped eviction costs memory, a wrong one costs the user their
// conversation". shedPressure threads that predicate; the LRU ENTRY-CAP
// enforcer carries the weaker mid-turn-only skip of run.py:
// _enforce_agent_cache_cap. Both paths may leave the cache over its bound
// rather than evict a wrong one.

import { readFileSync } from "node:fs";
import { totalmem } from "node:os";

export interface AgentCacheOptions {
	/** LRU entry cap (parity _AGENT_CACHE_MAX_SIZE). Default 128. */
	maxEntries?: number;
	/**
	 * DEC-021 pressure bound in bytes; when totalBytes exceeds it the
	 * least-recently-used entries are shed until back under budget.
	 * DEFAULT: derived at startup from the cgroup quota / host memory
	 * (resolveAutoMaxTotalBytes — agent_cache_pressure.py "auto" parity), so
	 * shedPressure is armed on every production construction path.
	 */
	maxTotalBytes?: number;
	/** Entries idle longer than this are evicted by sweepIdle(). Default 1h. */
	idleTtlMs?: number;
	/** Injected clock for recency/idle tracking. */
	now?: () => number;
	/**
	 * Evictability gate for BYTE-PRESSURE shedding (the default isEvictable
	 * consulted by shedPressure, including its insert-time enforceBounds
	 * call). Full gateway/run.py:_is_evictable parity: admit an entry for
	 * soft eviction only when it is NOT mid-turn AND its transcript
	 * persistence has caught up (see transcriptPersistenceCaughtUp).
	 * Composition lives in the caller (GatewayRunner), exactly as
	 * GatewayRunner._sweep_agent_cache_under_pressure builds _is_evictable.
	 * (Typed against `unknown` so options stay usable unparameterized; the
	 * class narrows to its own `V`.)
	 */
	isEvictable?: (key: string, value: unknown) => boolean;
	/**
	 * Weaker gate for the LRU ENTRY-CAP enforcer — gateway/run.py:
	 * _enforce_agent_cache_cap parity: mid-turn agents are skipped there
	 * ("their clients, terminal sandboxes, background processes … are all in
	 * active use by the running turn") WITHOUT consulting the
	 * persistence-caught-up predicate the pressure path adds. A held slot is
	 * passed over without substituting a newer entry; the cache may stay
	 * temporarily over cap and is re-checked on the next insert.
	 */
	isCapEvictable?: (key: string, value: unknown) => boolean;
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
	private readonly isEvictable: ((key: string, value: V) => boolean) | null;
	private readonly isCapEvictable: ((key: string, value: V) => boolean) | null;

	constructor(options: AgentCacheOptions = {}) {
		// Parity run.py:_AGENT_CACHE_MAX_SIZE = 128 / _AGENT_CACHE_IDLE_TTL_SECS
		// = 3600.0 — the shipped gateway cache bounds. The byte bound defaults to
		// the STARTUP-DERIVED memory budget (agent_cache_pressure.py "auto"),
		// not an inert sentinel — DEC-021's bounded-resident-size guarantee must
		// hold without operator config.
		this.maxEntries = Math.max(1, options.maxEntries ?? 128);
		this.maxTotalBytes = Math.max(
			0,
			options.maxTotalBytes ?? defaultAgentCacheMaxTotalBytes(),
		);
		this.idleTtlMs = Math.max(0, options.idleTtlMs ?? 3_600_000);
		this.now = options.now ?? (() => Date.now());
		this.isEvictable = options.isEvictable ?? null;
		this.isCapEvictable = options.isCapEvictable ?? null;
	}

	get size(): number {
		return this.entries.size;
	}

	/** Effective pressure bound in bytes (startup-derived when not injected). */
	get byteBudget(): number {
		return this.maxTotalBytes;
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
	 * bytes fit the bound, under THREE guards Hermes learned the hard way:
	 *
	 *  • isEvictable gates EVERY eviction (run.py:_is_evictable parity): a
	 *    candidate failing the predicate — mid-turn agent, transcript not yet
	 *    fully on disk — is passed over WITHOUT consuming the per-pass budget
	 *    or being substituted by a newer entry. When no candidate is
	 *    evictable the cache stays over budget: "a skipped eviction costs
	 *    memory, a wrong one costs the user their conversation." Defaults to
	 *    the constructor gate; an explicit option here wins.
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
		options: {
			protectRecent?: number;
			maxEvictionsPerPass?: number;
			isEvictable?: (key: string, value: V) => boolean;
		} = {},
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
		// plan_pressure_evictions' `is_evictable(key, agent)` parameter: the
		// constructor gate is the default, a call-site override wins.
		const gate = options.isEvictable ?? this.isEvictable;
		const evicted: string[] = [];
		for (const key of evictable) {
			if (
				this.totalBytes <= this.maxTotalBytes ||
				evicted.length >= maxEvictions
			) {
				break;
			}
			const entry = this.entries.get(key);
			if (!entry) continue;
			if (gate && !gate(key, entry.value)) continue; // held: skip, don't count
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
		// Entry-cap eviction first (_enforce_agent_cache_cap parity: consider
		// ONLY the first `excess` LRU positions of the snapshot —
		// `ordered_keys[:excess]`; pass over held slots WITHOUT substituting a
		// newer entry — the cache may stay over cap and is re-checked on the
		// next insert), then byte-pressure shedding (capped per pass, gated by
		// isEvictable — see shedPressure).
		const excess = this.entries.size - this.maxEntries;
		if (excess > 0) {
			const gate = this.isCapEvictable;
			const window = [...this.entries.keys()].slice(0, excess);
			for (const key of window) {
				const entry = this.entries.get(key);
				if (!entry) continue;
				if (gate && !gate(key, entry.value)) continue; // held slot stays
				this.entries.delete(key);
			}
		}
		this.shedPressure();
	}
}

/** Parity agent_cache_pressure.py:_DEFAULT_MAX_EVICTIONS_PER_PASS. */
export const DEFAULT_MAX_EVICTIONS_PER_PASS = 16;

/** Parity agent_cache_pressure.py:_DEFAULT_PROTECT_RECENT. */
export const DEFAULT_PROTECT_RECENT = 8;

/**
 * gateway/agent_cache_pressure.py:transcript_persistence_caught_up parity.
 *
 * True when the cached agent's live transcript is fully on disk. Soft eviction
 * drops the live transcript and rebuilds it from the persisted session next
 * turn, so it is only safe once persistence has caught up: the flushed-through
 * marker is advanced to the live message count ONLY on a fully successful
 * write (run.py `_last_flushed_db_idx` / _flush_messages_to_session_db
 * semantics; GatewayRunner stamps it around each turn's assistant-row append).
 *
 * Duck-typed over the cached value and FAIL-CLOSED on unknown shapes: a
 * missing/non-array transcript or a missing/non-integer marker reads as NOT
 * caught up — "a skipped eviction costs memory, a wrong one costs the user
 * their conversation."
 */
export function transcriptPersistenceCaughtUp(cachedAgent: unknown): boolean {
	if (cachedAgent === null || typeof cachedAgent !== "object") return false;
	const messages = (
		cachedAgent as {
			session?: { agent?: { state?: { messages?: unknown } } };
		}
	).session?.agent?.state?.messages;
	if (!Array.isArray(messages)) return false;
	const flushed = (cachedAgent as { flushedDbIdx?: unknown }).flushedDbIdx;
	if (typeof flushed !== "number" || !Number.isSafeInteger(flushed)) {
		return false;
	}
	return flushed >= messages.length;
}

// ----------------------------------------------------------------------
// Startup-derived memory budget (agent_cache_pressure.py parity).
// ----------------------------------------------------------------------

/** Parity agent_cache_pressure.py:_AUTO_BUDGET_FRACTION. */
export const AUTO_BUDGET_FRACTION = 0.65;
/** Parity agent_cache_pressure.py:_AUTO_BUDGET_FLOOR_MB. */
export const AUTO_BUDGET_FLOOR_MB = 512;

const BYTES_PER_MB = 1024 * 1024;

/** Injectable filesystem / memory probes (deterministic tests). */
export interface MemoryProbeIo {
	/** Return file contents or undefined when unreadable. Default readFileSync. */
	readTextFile?(path: string): string | undefined;
	/** Total host memory in bytes. Default os.totalmem(). */
	totalMemoryBytes?(): number;
}

function defaultReadTextFile(path: string): string | undefined {
	try {
		return readFileSync(path, "utf-8");
	} catch {
		return undefined; // ENOENT/EACCES ⇒ candidate absent, keep walking
	}
}

/**
 * gateway/cgroup_cleanup.py:_own_cgroup_path — the calling process's cgroup v2
 * path from /proc/self/cgroup (`0::…` line), or null.
 */
export function ownCgroupPath(procCgroupText: string): string | null {
	const m = /^0::(.+)$/m.exec(procCgroupText);
	return m && m[1] !== undefined ? m[1].trim() : null;
}

function parseCgroupLimitBytes(raw: string | undefined): number | null {
	if (raw === undefined) return null;
	const text = raw.trim();
	if (!text || text === "max") return null; // "max" means unlimited
	const limit = Number.parseInt(text, 10);
	if (!Number.isFinite(limit)) return null;
	// cgroup v1 reports "unlimited" as a near-2^63 sentinel. Exact power-of-two
	// literal (NOT the 32-bit `1 << 62`, which overflows to 0 and would reject
	// EVERY real limit).
	if (limit <= 0 || limit >= 2 ** 62) return null;
	return limit;
}

/**
 * Parity agent_cache_pressure.py:_cgroup_limit_bytes — the memory limit this
 * process runs under, if cgroup-capped. Prefers cgroup v2 memory.high (the
 * throttling point) over memory.max for the process's OWN cgroup first (where
 * systemd MemoryHigh=/MemoryMax= lands), then root files for container-style
 * limits, then cgroup v1. Null when nothing caps us.
 */
export function resolveCgroupLimitBytes(io: MemoryProbeIo = {}): number | null {
	const readFile = io.readTextFile ?? defaultReadTextFile;
	const candidates: string[] = [];
	let own: string | null = null;
	const procText = readFile("/proc/self/cgroup");
	if (procText !== undefined) own = ownCgroupPath(procText);
	if (own && own !== "/") {
		candidates.push(
			`/sys/fs/cgroup${own}/memory.high`,
			`/sys/fs/cgroup${own}/memory.max`,
		);
	}
	candidates.push(
		"/sys/fs/cgroup/memory.high",
		"/sys/fs/cgroup/memory.max",
		"/sys/fs/cgroup/memory/memory.limit_in_bytes", // cgroup v1
	);
	for (const candidate of candidates) {
		const limit = parseCgroupLimitBytes(readFile(candidate));
		if (limit !== null) return limit;
	}
	return null;
}

/**
 * Parity agent_cache_pressure.py:_total_memory_bytes — total host RAM.
 * (os.totalmem() covers both the sysconf and psutil fallbacks of the
 * reference on Node.)
 */
export function resolveHostTotalMemoryBytes(
	io: MemoryProbeIo = {},
): number | null {
	try {
		const bytes = (io.totalMemoryBytes ?? totalmem)();
		return bytes > 0 ? bytes : null;
	} catch {
		return null;
	}
}

/**
 * Parity agent_cache_pressure.py:resolve_memory_high_mb("auto") math: budget =
 * floor(limit × _AUTO_BUDGET_FRACTION) in MB. Below _AUTO_BUDGET_FLOOR_MB the
 * budget is NOISE — small containers would evict on every pass and never keep
 * a warm prefix — so the pressure pass is switched OFF (null), never clamped.
 */
export function deriveAutoBudgetMb(limitBytes: number): number | null {
	if (!Number.isFinite(limitBytes) || limitBytes <= 0) return null;
	const mb = Math.floor((limitBytes * AUTO_BUDGET_FRACTION) / BYTES_PER_MB);
	return mb >= AUTO_BUDGET_FLOOR_MB ? mb : null;
}

/**
 * The startup-derived pressure bound in bytes: cgroup quota first, else host
 * RAM, at the auto fraction. Infinity ONLY where Hermes also runs the pass
 * disabled (no readable limit at all, or a sub-floor budget ⇒
 * resolve_memory_high_mb returns None ⇒ sweep returns 0) — never as a
 * stand-in for an unreadable operator intent.
 */
export function resolveAutoMaxTotalBytes(io: MemoryProbeIo = {}): number {
	const limit = resolveCgroupLimitBytes(io) ?? resolveHostTotalMemoryBytes(io);
	if (limit === null) return Number.POSITIVE_INFINITY;
	const mb = deriveAutoBudgetMb(limit);
	return mb === null ? Number.POSITIVE_INFINITY : mb * BYTES_PER_MB;
}

let memoizedDefaultMaxTotalBytes: number | undefined;

/**
 * Process-wide default bound, resolved ONCE per process (run.py:
 * GatewayRunner._agent_cache_bounds lazy-resolution parity). Every cache that
 * does not receive an explicit maxTotalBytes shares it.
 */
export function defaultAgentCacheMaxTotalBytes(): number {
	if (memoizedDefaultMaxTotalBytes === undefined) {
		memoizedDefaultMaxTotalBytes = resolveAutoMaxTotalBytes();
	}
	return memoizedDefaultMaxTotalBytes;
}

/** Test seam: clear the process-wide memoized default (wal.ts precedent). */
export function resetDefaultMaxTotalBytesForTests(): void {
	memoizedDefaultMaxTotalBytes = undefined;
}
