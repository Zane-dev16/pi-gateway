// Behavior contracts: the binding ten-stage startup order (01 §3.1), engine
// idempotency, optional-stage loud degradation vs required-stage abort.
//
// Banned test shapes avoided: no source regexes, no catalog counts — every
// assertion is a relationship between the event trace and the stage sequence.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { STAGE_IDS, type StageId } from "./stages.js";
import {
	GatewayLifecycle,
	type StageBody,
	type StageContext,
} from "./lifecycle.js";
import type { Logger } from "./shutdown.js";

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-lifecycle-stages-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

function spyBodies(
	calls: StageId[],
	failAt?: Partial<Record<StageId, Error>>,
): { bodies: Partial<Record<StageId, StageBody>>; contexts: StageContext[] } {
	const contexts: StageContext[] = [];
	const bodies: Partial<Record<StageId, StageBody>> = {};
	for (const id of STAGE_IDS) {
		bodies[id] = async (ctx) => {
			const failure = failAt?.[id];
			if (failure) throw failure;
			contexts.push(ctx);
			calls.push(id);
		};
	}
	return { bodies, contexts };
}

function captureLogger(): {
	logger: Logger;
	lines: { level: string; msg: string }[];
} {
	const lines: { level: string; msg: string }[] = [];
	return {
		lines,
		logger: {
			info: (m: string) => lines.push({ level: "info", msg: m }),
			warn: (m: string) => lines.push({ level: "warn", msg: m }),
			error: (m: string) => lines.push({ level: "error", msg: m }),
		},
	};
}

describe("ten-stage startup order (01 §3.1 — order is binding)", () => {
	it("event trace equals EXACTLY the declared ten-stage sequence", async () => {
		const calls: StageId[] = [];
		const { bodies } = spyBodies(calls);
		const lifecycle = new GatewayLifecycle({ home, stageBodies: bodies });

		const result = await lifecycle.startup();

		expect(result.ok).toBe(true);
		expect(calls).toEqual([...STAGE_IDS]);
		// Trace relationship: one ok event per stage, in stage order.
		expect(lifecycle.events.map((e) => e.stage)).toEqual([...STAGE_IDS]);
		expect(lifecycle.events.every((e) => e.ok && !e.degraded)).toBe(true);
		expect(lifecycle.state).toBe("running");
		expect(result.degradedStages).toEqual([]);
	});

	it("startup is IDEMPOTENT: a second call re-runs nothing", async () => {
		const calls: StageId[] = [];
		const { bodies } = spyBodies(calls);
		const lifecycle = new GatewayLifecycle({ home, stageBodies: bodies });
		await lifecycle.startup();
		const eventsAfterFirst = lifecycle.events.length;

		const again = await lifecycle.startup();

		expect(again.ok).toBe(true);
		expect(calls).toEqual([...STAGE_IDS]); // unchanged — no body re-ran
		expect(lifecycle.events.length).toBe(eventsAfterFirst);
	});
});

describe("optional-stage degradation (01 §3.1 per-service degraded start)", () => {
	for (const failing of [
		"cron_scheduler",
		"embedded_watchers",
		"platform_adapters",
	] as const) {
		it(`stage ${failing} failing degrades LOUDLY and later stages still run`, async () => {
			const calls: StageId[] = [];
			const boom = new Error(`${failing} exploded`);
			const { bodies } = spyBodies(calls, { [failing]: boom });
			const captured = captureLogger();
			const lifecycle = new GatewayLifecycle({
				home,
				stageBodies: bodies,
				logger: captured.logger,
			});

			const result = await lifecycle.startup();

			// Startup CONTINUES: the failing body never completes, but EVERY
			// later stage still ran (degraded, not blocking).
			expect(result.ok).toBe(true);
			expect(calls).toEqual([...STAGE_IDS.filter((id) => id !== failing)]);
			// The failed stage is recorded degraded in the trace…
			const event = lifecycle.events.find((e) => e.stage === failing);
			expect(event?.ok).toBe(false);
			expect(event?.degraded).toBe(true);
			expect(event?.error).toContain("exploded");
			// …surfaced in the result AND logged at ERROR level with reason code.
			expect(result.degradedStages).toEqual([failing]);
			const loud = captured.lines.filter(
				(l) => l.level === "error" && l.msg.includes(failing),
			);
			expect(loud.length).toBeGreaterThan(0);
			expect(lifecycle.state).toBe("running");
		});
	}

	it("ALL optional stages may fail together; startup still reaches running", async () => {
		const calls: StageId[] = [];
		const { bodies } = spyBodies(calls, {
			cron_scheduler: new Error("cron down"),
			embedded_watchers: new Error("watchers down"),
			platform_adapters: new Error("adapters down"),
		});
		const lifecycle = new GatewayLifecycle({ home, stageBodies: bodies });

		const result = await lifecycle.startup();

		expect(result.ok).toBe(true);
		const failedSet = new Set<StageId>([
			"cron_scheduler",
			"embedded_watchers",
			"platform_adapters",
		]);
		expect(result.degradedStages.sort((a, b) => a.localeCompare(b))).toEqual([
			"cron_scheduler",
			"embedded_watchers",
			"platform_adapters",
		]);
		expect(calls).toEqual([...STAGE_IDS.filter((id) => !failedSet.has(id))]);
	});
});

describe("required-stage failure aborts startup", () => {
	for (const failing of [
		"profile_override",
		"load_config",
		"duplicate_guard",
		"runtime_lock",
		"open_state_db",
	] as const) {
		it(`stage ${failing} failing ABORTS — stages 7–10 never run`, async () => {
			const calls: StageId[] = [];
			const { bodies } = spyBodies(calls, {
				[failing]: new Error(`${failing} fatal`),
			});
			const lifecycle = new GatewayLifecycle({ home, stageBodies: bodies });

			const result = await lifecycle.startup();

			expect(result.ok).toBe(false);
			expect(result.failedStage).toBe(failing);
			// Ordering relationship: the failing stage never completed, nothing
			// AFTER it was attempted, and the last COMPLETED stage is exactly its
			// predecessor (empty when the very first stage aborts).
			expect(calls).not.toContain(failing);
			for (const later of STAGE_IDS.slice(STAGE_IDS.indexOf(failing) + 1)) {
				expect(calls).not.toContain(later);
			}
			const idx = STAGE_IDS.indexOf(failing);
			if (idx > 0) {
				expect(calls.at(-1)).toBe(STAGE_IDS[idx - 1]);
			} else {
				expect(calls).toEqual([]);
			}
			expect(lifecycle.state).toBe("aborted");
		});
	}

	it("a failed startup stays failed (no silent re-entry into running)", async () => {
		let shouldFail = true;
		const lifecycle = new GatewayLifecycle({
			home,
			stageBodies: {
				open_state_db: async () => {
					if (shouldFail) throw new Error("db unavailable");
				},
			},
		});
		const first = await lifecycle.startup();
		expect(first.ok).toBe(false);

		shouldFail = false;
		const second = await lifecycle.startup();
		expect(second.ok).toBe(false); // memoized result, not a re-run
		expect(second.failedStage).toBe(first.failedStage);
	});
});
