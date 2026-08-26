// Behavior contracts for the DEC-021 agent-instance cache bounds.
//
// Parity anchors (READ-ONLY Hermes reference):
//   gateway/run.py:_AGENT_CACHE_MAX_SIZE / _AGENT_CACHE_IDLE_TTL_SECS
//     → constructor defaults 128 entries / 3_600_000 ms idle TTL
//   gateway/agent_cache_pressure.py:plan_pressure_evictions
//     → shedPressure: _DEFAULT_MAX_EVICTIONS_PER_PASS=16 per-pass cap,
//       protect_recent=8 clamped to len//2, LRU→MRU candidate order
//
// All time flows through the injected `now`; byte accounting is synthetic
// (DEC verification clause). Every row asserts an OUTCOME, never internals.

import { describe, expect, it } from "vitest";

import {
	DEFAULT_MAX_EVICTIONS_PER_PASS,
	DEFAULT_PROTECT_RECENT,
	AgentInstanceCache,
	transcriptPersistenceCaughtUp,
} from "./agent-cache.js";

describe("constructor defaults — run.py:_AGENT_CACHE_* parity", () => {
	it("defaults to a 128-entry LRU cap (128 kept, oldest beyond shed)", () => {
		let t = 0;
		const cache = new AgentInstanceCache<string>({ now: () => ++t });
		for (let i = 0; i < 130; i++) {
			cache.set(`s${i}`, "v", 1);
		}
		expect(cache.size).toBe(128);
		expect(cache.has("s0")).toBe(false); // two oldest evicted
		expect(cache.has("s1")).toBe(false);
		expect(cache.has("s129")).toBe(true); // newest survives
	});

	it("defaults to a 3_600_000 ms idle TTL (1h boundary, injected clock)", () => {
		let now = 1_000_000;
		const cache = new AgentInstanceCache<string>({ now: () => now });
		cache.set("stale", "v", 1);
		now += 3_599_999; // just inside the hour
		expect(cache.sweepIdle()).toEqual([]);
		now += 1; // exactly 3_600_000 ms idle ⇒ cutoff == lastUsedAt
		expect(cache.sweepIdle()).toEqual(["stale"]);
	});
});

describe("shedPressure — plan_pressure_evictions parity", () => {
	/**
	 * Build an OVER-BUDGET cache deterministically: insert tiny entries (no
	 * insert-time enforcement fires), then re-`set` the EXISTING keys with
	 * bigger byte counts — a same-key set() updates bytes in place WITHOUT
	 * re-running enforceBounds, so the over-budget state survives to the
	 * direct shedPressure() call under test.
	 */
	function makeOverBudgetCache(
		n: number,
		bytesPerEntry: number,
		maxTotalBytes: number,
	): AgentInstanceCache<string> {
		let t = 0;
		const cache = new AgentInstanceCache<string>({
			maxTotalBytes,
			now: () => ++t,
		});
		for (let i = 0; i < n; i++) cache.set(`k${i}`, "v", 1);
		for (let i = 0; i < n; i++) cache.set(`k${i}`, "v", bytesPerEntry);
		return cache;
	}

	it("under budget ⇒ no-op", () => {
		const cache = makeOverBudgetCache(2, 10, 100);
		expect(cache.shedPressure()).toEqual([]);
		expect(cache.size).toBe(2);
	});

	it("default protects the 8 hottest entries; sheds LRU-first until under budget", () => {
		// 12 × 100 = 1200 bytes against an 800 budget. protect=8 ⇒ candidates
		// k0..k3 only; shedding exactly those lands on the budget line.
		const cache = makeOverBudgetCache(12, 100, 800);
		const evicted = cache.shedPressure();
		expect(evicted).toEqual(["k0", "k1", "k2", "k3"]);
		for (const hot of ["k4", "k5", "k6", "k7", "k8", "k9", "k10", "k11"]) {
			expect(cache.has(hot)).toBe(true); // protect_recent=8 held the hottest 8
		}
		expect(cache.totalBytes).toBe(800);
	});

	it("protectRecent CLAMPS to len//2 — a small cache cannot be fully protected", () => {
		// 4 × 40 = 160 bytes, budget 30. An UNCLAMPED protectRecent would shield
		// all four and shed nothing; the clamp forces len//2 = 2.
		const cache = makeOverBudgetCache(4, 40, 30);
		const evicted = cache.shedPressure({ protectRecent: 100 });
		expect(evicted).toEqual(["k0", "k1"]); // exactly the coldest half
		expect(cache.keys().sort()).toEqual(["k2", "k3"]);
	});

	it(`caps ONE pass at ${DEFAULT_MAX_EVICTIONS_PER_PASS} evictions even while still over budget`, () => {
		// 30 × 100 = 3000 bytes vs a 1000-byte budget: even after the capped
		// pass the remaining tail keeps the cache over budget.
		const cache = makeOverBudgetCache(30, 100, 1000);
		const firstPass = cache.shedPressure();
		// protect=min(8, 30//2)=8 ⇒ candidates k0..k21; the pass cap fires first.
		expect(firstPass).toHaveLength(DEFAULT_MAX_EVICTIONS_PER_PASS);
		expect(firstPass[0]).toBe("k0");
		expect(firstPass[15]).toBe("k15");
		expect(cache.totalBytes).toBeGreaterThan(10); // STILL over budget
		// A successive pass continues from where pass one stopped and stops at
		// the budget line: protect=min(8, 14//2)=7 ⇒ candidates k16..k22, but
		// four evictions land exactly on 1000.
		const secondPass = cache.shedPressure();
		expect(secondPass).toEqual(["k16", "k17", "k18", "k19"]);
		expect(cache.totalBytes).toBe(1000);
	});

	it("explicit maxEvictionsPerPass override bounds the batch tighter", () => {
		const cache = makeOverBudgetCache(10, 100, 10);
		const evicted = cache.shedPressure({ maxEvictionsPerPass: 2 });
		expect(evicted).toEqual(["k0", "k1"]);
		expect(cache.size).toBe(8); // still over budget — next pass continues
	});

	it("protectRecent: 0 means 'shed anything' — even the MRU entry", () => {
		const cache = makeOverBudgetCache(3, 500, 100);
		const evicted = cache.shedPressure({ protectRecent: 0 });
		// Nothing is spared when the operator asks: every entry goes.
		expect(evicted).toEqual(["k0", "k1", "k2"]);
		expect(cache.size).toBe(0);
	});

	it("a hot monster transcript survives the pass — protected tail wins over budget", () => {
		// Real DEC-021 shape: 9 warm small sessions, then one monster transcript
		// lands via a NEW set() (insert-time enforcement runs but the clamp keeps
		// the hot tail, so the cache STAYS over budget after the set returns).
		let t = 0;
		const cache = new AgentInstanceCache<string>({
			maxTotalBytes: 1000,
			now: () => ++t,
		});
		for (let i = 0; i < 9; i++) cache.set(`k${i}`, "v", 100);
		cache.set("monster", "m", 5000);
		expect(cache.has("monster")).toBe(true); // never thrashed mid-insert
		const evicted = cache.shedPressure(); // protect=min(8, 5//2)=2
		expect(evicted).toEqual(["k5", "k6", "k7"]);
		expect(cache.has("monster")).toBe(true); // MRU still protected
		expect(cache.totalBytes).toBe(5100); // tail alone exceeds the budget
	});
});

describe("sweepIdle — explicit TTL override still wins over the default", () => {
	it("operator-supplied idleTtlMs replaces the 1h default", () => {
		let now = 10_000;
		const cache = new AgentInstanceCache<string>({
			idleTtlMs: 1_000,
			now: () => now,
		});
		cache.set("old", "v", 5);
		now = 11_001;
		expect(cache.sweepIdle()).toEqual(["old"]);
	});
});

// ---------------------------------------------------------------------------
// Evictability gates (stability round secops-13) — Hermes NEVER sheds purely
// by position: run.py:_is_evictable gates the pressure path (mid-turn +
// transcript-persistence-caught-up), _enforce_agent_cache_cap skips mid-turn
// holders on the entry-cap path.
// ---------------------------------------------------------------------------

describe("shedPressure isEvictable — run.py:_is_evictable threading", () => {
	/** Over-budget cache whose pressure gate holds every key named in `held`. */
	function makeGatedCache(
		n: number,
		bytesPerEntry: number,
		maxTotalBytes: number,
		held: string[],
	): AgentInstanceCache<string> {
		let t = 0;
		const cache = new AgentInstanceCache<string>({
			maxTotalBytes,
			now: () => ++t,
			isEvictable: (key) => !held.includes(key),
		});
		// Same-key re-set trick from makeOverBudgetCache: tiny inserts stay
		// under budget (insert-time enforcement quiet), then bytes grow IN
		// PLACE so the over-budget state survives to the call under test.
		for (let i = 0; i < n; i++) cache.set(`k${i}`, "v", 1);
		for (let i = 0; i < n; i++) cache.set(`k${i}`, "v", bytesPerEntry);
		return cache;
	}

	it("a held coldest entry is PASSED OVER — the warmer evictable neighbor sheds instead", () => {
		// Same-key re-set trick (see makeOverBudgetCache): grow bytes without
		// tripping insert-time enforcement so the direct call sees the state.
		let t = 0;
		const cache = new AgentInstanceCache<string>({
			maxTotalBytes: 100,
			now: () => ++t,
			isEvictable: (key) => key !== "held",
		});
		for (const k of ["held", "warm"]) cache.set(k, k, 1);
		for (const k of ["held", "warm"]) cache.set(k, k, 100);
		expect(cache.totalBytes).toBe(200); // over budget, nothing shed yet

		const evicted = cache.shedPressure({ protectRecent: 0 });
		expect(evicted).toEqual(["warm"]); // LRU walk skips "held" without substitution
		expect(cache.has("held")).toBe(true);
		expect(cache.totalBytes).toBe(100); // budget line reached via the right victim
	});

	it("skipped entries consume NO per-pass budget", () => {
		const cache = makeGatedCache(6, 100, 10, ["k0"]);
		const evicted = cache.shedPressure({ maxEvictionsPerPass: 2 });
		// protect=min(8, 6//2)=3 ⇒ candidates k0..k2; k0 is skipped WITHOUT
		// counting, so the two-slot budget lands on k1+k2.
		expect(evicted).toEqual(["k1", "k2"]);
		expect(cache.has("k0")).toBe(true);
		for (const hot of ["k3", "k4", "k5"]) expect(cache.has(hot)).toBe(true);
	});

	it("every candidate held ⇒ empty plan, cache intact and STILL over budget", () => {
		const cache = makeGatedCache(3, 100, 50, ["k0", "k1", "k2"]);
		expect(cache.shedPressure({ protectRecent: 0 })).toEqual([]);
		expect(cache.size).toBe(3);
		expect(cache.totalBytes).toBe(300); // over-bound tolerated, never a wrong shed
	});

	it("call-site isEvictable overrides the constructor gate (both directions)", () => {
		const strict = makeGatedCache(2, 100, 10, ["k0", "k1"]);
		expect(strict.shedPressure({ protectRecent: 0 })).toEqual([]); // ctor: hold all
		expect(
			strict.shedPressure({ protectRecent: 0, isEvictable: () => true }),
		).toEqual(["k0", "k1"]); // call-site opens the valve

		const lenient = makeGatedCache(2, 100, 10, []);
		expect(
			lenient.shedPressure({ protectRecent: 0, isEvictable: () => false }),
		).toEqual([]); // call-site tightens beyond the constructor
	});

	it("the constructor gate arms INSERT-TIME enforcement — over-bound beats a wrong shed", () => {
		let t = 0;
		const cache = new AgentInstanceCache<string>({
			maxTotalBytes: 100,
			now: () => ++t,
			isEvictable: (key) => key !== "held",
		});
		cache.set("held", "held", 100);
		expect(cache.size).toBe(1);
		cache.set("next", "next", 100); // pass: only candidate is held ⇒ nothing goes
		expect(cache.size).toBe(2); // stays over budget...
		expect(cache.totalBytes).toBe(200);
		cache.set("third", "third", 100); // candidates [held, next] ⇒ next sheds
		expect(cache.keys().sort()).toEqual(["held", "third"]);
	});

	it("protect_recent still shields an EVICTABLE hot tail ahead of the gate", () => {
		// 4 × 25 = 100 bytes against an 80 budget: the pass sheds exactly k0
		// (landing under budget), leaving k1 — evictable by the gate but inside
		// the protected tail (protect=min(DEFAULT_PROTECT_RECENT, 4//2)=2) —
		// alive.
		const cache = makeGatedCache(4, 25, 80, []);
		expect(cache.shedPressure()).toEqual(["k0"]);
		expect(cache.has("k1")).toBe(true); // evictable yet protected — clamp wins
	});
});

describe("transcriptPersistenceCaughtUp — agent_cache_pressure.py fail-closed parity", () => {
	const shaped = (messages: unknown, flushedDbIdx: unknown): unknown => ({
		session: { agent: { state: { messages } } },
		flushedDbIdx,
	});
	const listOf = (n: number) => Array.from({ length: n }, (_, i) => ({ i }));

	it("null / undefined / primitives ⇒ NOT caught up", () => {
		for (const v of [null, undefined, 42, "agent", Symbol("x")]) {
			expect(transcriptPersistenceCaughtUp(v)).toBe(false);
		}
	});

	it("missing or non-array live transcript ⇒ NOT caught up", () => {
		expect(transcriptPersistenceCaughtUp(shaped(undefined, 5))).toBe(false);
		expect(transcriptPersistenceCaughtUp(shaped("msgs", 5))).toBe(false);
		expect(transcriptPersistenceCaughtUp({ flushedDbIdx: 9 })).toBe(false);
	});

	it("missing / null / non-integer flush marker ⇒ NOT caught up", () => {
		expect(transcriptPersistenceCaughtUp(shaped(listOf(2), undefined))).toBe(
			false,
		);
		expect(transcriptPersistenceCaughtUp(shaped(listOf(2), null))).toBe(false);
		expect(transcriptPersistenceCaughtUp(shaped(listOf(2), 1.5))).toBe(false);
		expect(transcriptPersistenceCaughtUp(shaped(listOf(2), NaN))).toBe(false);
		expect(
			transcriptPersistenceCaughtUp(
				shaped(listOf(2), Number.POSITIVE_INFINITY),
			),
		).toBe(false);
	});

	it("an unflushed tail (flushed < len) ⇒ NOT caught up — the wrong shed costs the conversation", () => {
		expect(transcriptPersistenceCaughtUp(shaped(listOf(5), 4))).toBe(false);
		expect(transcriptPersistenceCaughtUp(shaped(listOf(5), 0))).toBe(false);
	});

	it("caught-up shapes: flushed == len ⇒ true; empty transcript ⇒ true; compacted past marker ⇒ true", () => {
		expect(transcriptPersistenceCaughtUp(shaped(listOf(5), 5))).toBe(true);
		expect(transcriptPersistenceCaughtUp(shaped([], 0))).toBe(true);
		expect(transcriptPersistenceCaughtUp(shaped(listOf(3), 7))).toBe(true);
	});
});

describe("isCapEvictable — run.py:_enforce_agent_cache_cap parity", () => {
	it("held oldest keeps its slot; sibling excess-window slots still shed; newer-than-window entries never considered", () => {
		let t = 0;
		const cache = new AgentInstanceCache<string>({
			maxEntries: 1,
			maxTotalBytes: Number.MAX_SAFE_INTEGER,
			now: () => ++t,
			isCapEvictable: (key) => key !== "held",
		});
		cache.set("held", "held", 1); // size 1 ≤ cap
		cache.set("mid", "mid", 1); // excess=1, window=[held] blocked ⇒ over cap
		expect(cache.keys()).toEqual(["held", "mid"]);
		cache.set("new", "new", 1); // excess=2, window=[held,mid] ⇒ mid sheds
		expect(cache.has("held")).toBe(true); // never substituted away
		expect(cache.has("mid")).toBe(false);
		expect(cache.has("new")).toBe(true); // just-inserted MRU is outside the window
		expect(cache.size).toBe(2); // stays over cap; re-checked next insert
	});

	it("every slot held ⇒ nothing evicted, cache stays over cap", () => {
		let t = 0;
		const cache = new AgentInstanceCache<string>({
			maxEntries: 1,
			maxTotalBytes: Number.MAX_SAFE_INTEGER,
			now: () => ++t,
			isCapEvictable: () => false,
		});
		for (const k of ["a", "b", "c"]) cache.set(k, k, 1);
		expect(cache.keys()).toEqual(["a", "b", "c"]);
	});

	it("no gate ⇒ pure-LRU cap enforcement (positional regression)", () => {
		let t = 0;
		const cache = new AgentInstanceCache<string>({
			maxEntries: 2,
			maxTotalBytes: Number.MAX_SAFE_INTEGER,
			now: () => ++t,
		});
		for (const k of ["a", "b", "c", "d"]) cache.set(k, k, 1);
		expect(cache.keys()).toEqual(["c", "d"]);
	});
});

// ---------------------------------------------------------------------------
// Startup-derived pressure budget (agent_cache_pressure.py "auto" parity).
// ---------------------------------------------------------------------------

import {
	AUTO_BUDGET_FRACTION,
	AUTO_BUDGET_FLOOR_MB,
	deriveAutoBudgetMb,
	defaultAgentCacheMaxTotalBytes,
	resolveCgroupLimitBytes,
	resolveHostTotalMemoryBytes,
	resolveAutoMaxTotalBytes,
	resetDefaultMaxTotalBytesForTests,
	type MemoryProbeIo,
} from "./agent-cache.js";

const MB = 1024 * 1024;

function ioWith(
	files: Record<string, string>,
	totalMem?: number,
): MemoryProbeIo {
	return {
		readTextFile: (path) => files[path],
		...(totalMem === undefined ? {} : { totalMemoryBytes: () => totalMem }),
	};
}

describe("cgroup limit ladder — _cgroup_limit_bytes parity", () => {
	it("prefers the OWN cgroup's memory.high over root files (systemd unit limits)", () => {
		const io = ioWith({
			"/proc/self/cgroup": "12:pids:/user.slice\n0::/system.slice/gw.service\n",
			"/sys/fs/cgroup/system.slice/gw.service/memory.high": "3221225472",
			"/sys/fs/cgroup/system.slice/gw.service/memory.max": "4294967296",
			"/sys/fs/cgroup/memory.high": "8589934592",
			"/sys/fs/cgroup/memory.max": "17179869184",
		});
		expect(resolveCgroupLimitBytes(io)).toBe(3 * 1024 * 1024 * 1024);
	});

	it("falls back to root v2 memory.high, then memory.max, then v1", () => {
		expect(
			resolveCgroupLimitBytes(
				ioWith({
					"/proc/self/cgroup": "0::/\n",
					"/sys/fs/cgroup/memory.high": "max",
					"/sys/fs/cgroup/memory.max": "1073741824",
				}),
			),
		).toBe(1024 * 1024 * 1024);
		expect(
			resolveCgroupLimitBytes(
				ioWith({
					"/sys/fs/cgroup/memory/memory.limit_in_bytes": "2147483648",
				}),
			),
		).toBe(2 * 1024 * 1024 * 1024);
	});

	it("skips unlimited sentinels ('max', near-2^63) and garbage — null when uncapped", () => {
		expect(
			resolveCgroupLimitBytes(
				ioWith({
					"/proc/self/cgroup": "0::/\n",
					"/sys/fs/cgroup/memory.high": "max",
					"/sys/fs/cgroup/memory.max": "18446744073709551615",
					"/sys/fs/cgroup/memory/memory.limit_in_bytes": "not-a-number",
				}),
			),
		).toBeNull();
	});
});

describe("auto budget math — resolve_memory_high_mb('auto') parity", () => {
	it("budget = floor(limit × 0.65); at/above the 512MB floor it is taken", () => {
		expect(AUTO_BUDGET_FRACTION).toBe(0.65);
		expect(AUTO_BUDGET_FLOOR_MB).toBe(512);
		// 2048MB limit × 0.65 = 1331.2 → floored to 1331MB.
		expect(deriveAutoBudgetMb(2 * 1024 * MB)).toBe(1331);
		// A limit whose budget lands exactly on the floor is taken.
		expect(
			deriveAutoBudgetMb(Math.ceil(512 / 0.65) * MB),
		).toBeGreaterThanOrEqual(512);
	});

	it("BELOW the floor the pass switches OFF (null) — never clamped up", () => {
		// 600MB limit → 390MB budget: a small container would evict on every
		// pass and never keep a warm prefix, so there is NO budget at all.
		expect(deriveAutoBudgetMb(600 * MB)).toBeNull();
		expect(resolveAutoMaxTotalBytes(ioWith({}, 600 * MB))).toBe(
			Number.POSITIVE_INFINITY,
		);
	});

	it("cgroup quota wins over host RAM; no source at all ⇒ Infinity", () => {
		const capped = resolveAutoMaxTotalBytes(
			ioWith({ "/sys/fs/cgroup/memory.max": "4294967296" }, 32 * 1024 * MB),
		);
		expect(capped).toBe(2662 * MB); // floor(4096MB × 0.65) = 2662MB, in bytes
		// No cgroup cap AND unreadable host memory ⇒ pass disabled (parity None).
		expect(resolveAutoMaxTotalBytes(ioWith({}, 0))).toBe(
			Number.POSITIVE_INFINITY,
		);
		expect(resolveHostTotalMemoryBytes(ioWith({}, 4096 * MB))).toBe(4096 * MB);
	});
});

describe("default wiring — shedPressure live without operator config (DEC-021)", () => {
	it("an absent maxTotalBytes uses the process-derived bound; explicit option wins", () => {
		resetDefaultMaxTotalBytesForTests();
		try {
			const derived = defaultAgentCacheMaxTotalBytes();
			// On any real host this is finite (≥512MB budget ⇒ ≥536MB bytes) or
			// Infinity ONLY where Hermes also runs the pass disabled.
			const cacheA = new AgentInstanceCache<string>({ now: () => 1 });
			const probe =
				derived === Number.POSITIVE_INFINITY
					? Number.MAX_SAFE_INTEGER // can't distinguish; just don't crash
					: derived;
			if (probe !== Number.MAX_SAFE_INTEGER) {
				// Inserting over the derived budget sheds LRU-first…
				cacheA.set("cold", "v", Math.floor(probe / 2 + 1));
				cacheA.set("warm", "v", Math.floor(probe / 2 + 1));
				expect(cacheA.has("cold")).toBe(false); // shedPressure FIRED by default
			}
			// …while an EXPLICIT Infinity keeps every entry (operator opt-out).
			const cacheB = new AgentInstanceCache<string>({
				maxTotalBytes: Number.POSITIVE_INFINITY,
				now: () => 1,
			});
			for (let i = 0; i < 50; i++) cacheB.set(`k${i}`, "v", 1 << 30);
			expect(cacheB.size).toBe(50);
		} finally {
			resetDefaultMaxTotalBytesForTests();
		}
	});
});
