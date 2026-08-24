// Behavior contracts for the (fn, scope) availability-probe cache (05 §3.2;
// tools/registry.py port). Binding rows: TTL + transient-failure grace under
// an INJECTED clock, FIFO cap at 512, @no_cache_check_fn escape, invalidate,
// peek-never-probes, request-bound BYPASS, and the multiplex-isolation row
// "profile A's failing probe never suppresses profile B's tool" — including
// flipping setMultiplexActive(true) mid-process WITHOUT a restart.

import { afterEach, describe, expect, it } from "vitest";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { setMultiplexActive } from "../secretscope/index.js";
import {
	CHECK_FN_CACHE_BYPASS,
	CHECK_FN_CACHE_MAX,
	CHECK_FN_TTL_SECONDS,
	CheckFnCache,
	currentRequestIdentity,
	runWithRequestIdentity,
} from "./index.js";
import { withProfileIsolation } from "./profile-turn.js";

afterEach(() => {
	setMultiplexActive(false);
});

/** Injected monotonic-style clock. */
function manualClock() {
	let now = 1_000;
	return {
		nowSeconds: () => now,
		advance(seconds: number) {
			now += seconds;
		},
	};
}

function makeCache() {
	const h = manualClock();
	const warnings: unknown[] = [];
	const cache = new CheckFnCache({
		nowSeconds: h.nowSeconds,
		onWarning: (w) => warnings.push(w),
	});
	return { cache, h, warnings };
}

describe("TTL caching keyed (fn, scope)", () => {
	it("caches per fn identity within the TTL and re-probes after expiry", () => {
		const { cache, h } = makeCache();
		let probes = 0;
		const fn = (): boolean => {
			probes += 1;
			return true;
		};

		expect(cache.run(fn)).toBe(true);
		cache.run(fn);
		expect(probes).toBe(1); // served from cache

		h.advance(CHECK_FN_TTL_SECONDS - 0.5);
		cache.run(fn);
		expect(probes).toBe(1);

		h.advance(1); // exactly past the TTL boundary
		cache.run(fn);
		expect(probes).toBe(2);
	});

	it("different fn objects NEVER collide even in the same scope", () => {
		const { cache } = makeCache();
		const a = () => true;
		const b = () => true;
		let bProbes = 0;
		const bCounting = () => {
			bProbes += 1;
			return false;
		};
		void b;
		expect(cache.run(a)).toBe(true);
		expect(cache.run(bCounting)).toBe(false);
		expect(cache.peek(a)).toBe(true);
		expect(cache.peek(bCounting)).toBe(false);
		expect(bProbes).toBe(1);
	});
});

describe("multiplex-OFF bypass semantics (historical process-wide entry)", () => {
	it("OFF: every caller shares ONE process-wide verdict regardless of ambient context", () => {
		const { cache } = makeCache();
		setMultiplexActive(false);
		let probes = 0;
		const fn = () => {
			probes += 1;
			return true;
		};

		const home = mkdtempSync(join(tmpdir(), "pi-gw-multiplex-cfc-"));
		try {
			// One caller runs inside a fully-stamped profile turn, another bare.
			withProfileIsolation({ profile: "a", home }, () =>
				expect(cache.run(fn)).toBe(true),
			);
			expect(cache.run(fn)).toBe(true);
			expect(probes).toBe(1); // SAME null-scope entry served both callers

			// The scope component is literally absent (null), not "" (BYPASS).
			expect(cache.cacheScope()).toBeNull();
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
	});

	it("flip ON without restart: the shared entry goes unreachable — identities re-key or bypass", () => {
		const { cache, h } = makeCache();
		setMultiplexActive(false);
		let probes = 0;
		const fn = () => {
			probes += 1;
			return true;
		};
		cache.run(fn);
		expect(probes).toBe(1);

		setMultiplexActive(true); // ← mid-process flip, no restart
		// No resolvable profile identity → FAIL-CLOSED bypass: probe every call.
		expect(cache.cacheScope()).toBe(CHECK_FN_CACHE_BYPASS);
		cache.run(fn);
		cache.run(fn);
		expect(probes).toBe(3);

		// A stamped profile turn keys on ITS OWN resolved home — still not the
		// old process-wide entry.
		const home = mkdtempSync(join(tmpdir(), "pi-gw-multiplex-cfc2-"));
		try {
			withProfileIsolation({ profile: "a", home }, () => {
				expect(cache.cacheScope()).toBe(home);
				cache.run(fn);
				expect(probes).toBe(4); // fresh key → re-probe
				cache.run(fn);
				expect(probes).toBe(4); // then cached for THAT profile
			});
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
		h.advance(CHECK_FN_TTL_SECONDS * 10);
	});

	it("multiplex ON with NO profile turn context bypasses (fail closed) instead of aliasing", () => {
		const { cache } = makeCache();
		setMultiplexActive(true);
		let probes = 0;
		const fn = () => {
			probes += 1;
			return false;
		};
		expect(cache.run(fn)).toBe(false);
		expect(cache.run(fn)).toBe(false);
		expect(probes).toBe(2); // uncached both times
		expect(cache.size).toBe(0);
	});
});

describe("per-profile isolation (05 §10 check_fn multiplex isolation row)", () => {
	it("profile A's failing probe never suppresses profile B's tool", () => {
		const { cache, h } = makeCache();
		setMultiplexActive(true);
		let backendUp = false;
		const probe = () => backendUp;

		const homeA = mkdtempSync(join(tmpdir(), "pi-gw-multiplex-cfcA-"));
		const homeB = mkdtempSync(join(tmpdir(), "pi-gw-multiplex-cfcB-"));
		try {
			const verdict = (home: string): boolean =>
				withProfileIsolation(
					{ profile: home.endsWith("A-") ? "a" : "b", home },
					() => cache.run(probe),
				);

			// A probes while the backend is down → False cached FOR A.
			expect(verdict(homeA)).toBe(false);

			backendUp = true;
			// B must NOT inherit A's stale False — its own key re-probes.
			expect(verdict(homeB)).toBe(true);

			// A's cached False persists for A only until its own TTL expires.
			expect(verdict(homeA)).toBe(false);
			h.advance(CHECK_FN_TTL_SECONDS + 1);
			expect(verdict(homeA)).toBe(true);
		} finally {
			rmSync(homeA, { recursive: true, force: true });
			rmSync(homeB, { recursive: true, force: true });
		}
	});

	it("same profile across sequential turns shares one stable entry (the boundary IS the home)", () => {
		const { cache } = makeCache();
		setMultiplexActive(true);
		let probes = 0;
		const fn = () => {
			probes += 1;
			return true;
		};
		const home = mkdtempSync(join(tmpdir(), "pi-gw-multiplex-cfcS-"));
		try {
			for (let turnNo = 0; turnNo < 2; turnNo++) {
				withProfileIsolation({ profile: "stable", home }, () => {
					cache.run(fn);
				});
			}
			expect(probes).toBe(1);
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
	});
});

describe("request-bound sessions bypass both layers", () => {
	it("full session id + principal + transport family ⇒ BYPASS even inside a profile turn", () => {
		const { cache } = makeCache();
		setMultiplexActive(true);
		let probes = 0;
		const fn = () => {
			probes += 1;
			return true;
		};
		const home = mkdtempSync(join(tmpdir(), "pi-gw-multiplex-cfcR-"));
		try {
			withProfileIsolation({ profile: "a", home }, () =>
				runWithRequestIdentity(
					{
						sessionId: "s1",
						principal: "user-9",
						transportFamily: "ws",
					},
					() => {
						expect(currentRequestIdentity()?.principal).toBe("user-9");
						cache.run(fn);
						cache.run(fn);
					},
				),
			);
			expect(probes).toBe(2); // live control re-probes every call
			expect(cache.size).toBe(0); // nothing leaked into any shared map
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
	});

	it("partial identity does NOT trigger the bypass (all three fields required)", () => {
		const { cache } = makeCache();
		setMultiplexActive(true);
		const home = mkdtempSync(join(tmpdir(), "pi-gw-multiplex-cfcP-"));
		try {
			withProfileIsolation({ profile: "a", home }, () =>
				runWithRequestIdentity(
					{ sessionId: "s1", principal: "", transportFamily: "ws" },
					() => {
						expect(cache.cacheScope()).toBe(home); // profile key, not bypass
					},
				),
			);
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
	});
});

describe("failure semantics (#21658/#5304)", () => {
	function flaky(h: ReturnType<typeof manualClock>, warnings: unknown[]) {
		const cache = new CheckFnCache({
			nowSeconds: h.nowSeconds,
			onWarning: (w) => warnings.push(w),
		});
		let up = true;
		return { cache, setUp: (v: boolean) => (up = v), probe: () => up };
	}

	it("failure within the grace window serves last-good True WITHOUT caching the failure", () => {
		const h = manualClock();
		const warnings: unknown[] = [];
		const { cache, setUp, probe } = flaky(h, warnings);

		expect(cache.run(probe)).toBe(true); // success recorded
		h.advance(10);
		setUp(false);
		expect(cache.run(probe)).toBe(true); // grace: last-good served

		// The failure was NOT cached: after the TTL the next call re-probes...
		h.advance(CHECK_FN_TTL_SECONDS + 1);
		expect(cache.run(probe)).toBe(true); // still inside 60s grace vs last good
		expect(
			warnings.some(
				(w) =>
					(w as { kind: string }).kind === "transient_failure_served_last_good",
			),
		).toBe(true);

		// ...and once the FAILURE itself is older than the grace window from
		// the last success, it is honored: backend really is down.
		h.advance(60);
		expect(cache.run(probe)).toBe(false);
		expect(
			warnings.some((w) => (w as { kind: string }).kind === "probe_failed"),
		).toBe(true);
	});

	it("raised probes count as failures; bypass paths flag unresolvedScope", () => {
		const { cache, warnings } = makeCache();
		setMultiplexActive(true); // no profile ctx → BYPASS path
		const boom = (): boolean => {
			throw new Error("daemon exploded");
		};
		expect(cache.run(boom)).toBe(false);
		expect(warnings).toEqual([{ kind: "probe_raised", unresolvedScope: true }]);

		const scopedWarnings: unknown[] = [];
		const scoped = new CheckFnCache({
			nowSeconds: manualClock().nowSeconds,
			onWarning: (w) => scopedWarnings.push(w),
		});
		scoped.markUncached(boom);
		expect(scoped.run(boom)).toBe(false);
		expect(scopedWarnings).toEqual([
			{ kind: "probe_raised", unresolvedScope: false },
		]);
	});
});

describe("escapes + read-only surfaces", () => {
	it("markUncached always probes; invalidate clears everything", () => {
		const { cache, h } = makeCache();
		let probes = 0;
		const local = () => {
			probes += 1;
			return true;
		};
		cache.markUncached(local);
		cache.run(local);
		cache.run(local);
		expect(probes).toBe(2);

		const cached = () => true;
		cache.run(cached);
		expect(cache.peek(cached)).toBe(true);
		cache.invalidate();
		expect(cache.peek(cached)).toBeNull();
		h.advance(1);
	});

	it("peek NEVER executes the probe and respects TTL + bypass", () => {
		const { cache, h } = makeCache();
		setMultiplexActive(true);
		let probes = 0;
		const fn = () => {
			probes += 1;
			return false;
		};
		const home = mkdtempSync(join(tmpdir(), "pi-gw-multiplex-cfcK-"));
		try {
			withProfileIsolation({ profile: "peek", home }, () => {
				expect(cache.peek(fn)).toBeNull(); // nothing cached — no probe fired
				expect(probes).toBe(0);

				cache.run(fn);
				expect(probes).toBe(1);
				expect(cache.peek(fn)).toBe(false); // served WITHOUT re-probing
				expect(probes).toBe(1);

				h.advance(CHECK_FN_TTL_SECONDS + 1);
				expect(cache.peek(fn)).toBeNull(); // expired
			});

			// Bypass scope (no resolvable identity) → no trustworthy verdict.
			setMultiplexActive(true);
			expect(cache.cacheScope()).toBe(CHECK_FN_CACHE_BYPASS);
			expect(cache.peek(fn)).toBeNull();
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
	});

	it("FIFO cap at 512 entries prunes oldest first", () => {
		const { cache } = makeCache();
		setMultiplexActive(true);
		const home = mkdtempSync(join(tmpdir(), "pi-gw-multiplex-cfcC-"));
		try {
			const fns: Array<() => boolean> = [];
			withProfileIsolation({ profile: "cap", home }, () => {
				for (let i = 0; i <= CHECK_FN_CACHE_MAX; i++) {
					const fn = () => true;
					fns.push(fn);
					cache.run(fn);
				}
				expect(cache.size).toBeLessThanOrEqual(CHECK_FN_CACHE_MAX);
				// Oldest evicted, newest present (same profile scope).
				expect(cache.peek(fns[0] as () => boolean)).toBeNull();
				expect(cache.peek(fns[fns.length - 1] as () => boolean)).toBe(true);
			});
		} finally {
			rmSync(home, { recursive: true, force: true });
		}
	});
});
