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
	type ServiceEntry,
	type ServiceStartOutcome,
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

/** Recording entry factory: fires are pushed onto the shared timeline. */
function recordingEntry(
	name: string,
	timeline: string[],
	outcome?: () => ServiceStartOutcome | Promise<ServiceStartOutcome>,
): ServiceEntry {
	return {
		name,
		start: async () => {
			timeline.push(`service:${name}`);
			return outcome ? outcome() : { ok: true };
		},
	};
}

describe("ten-stage startup order (01 §3.1 — order is binding)", () => {
	it("event trace equals EXACTLY the declared ten-stage sequence", async () => {
		// Stages 1–6 and 9–10 are spied; stages 7–8 keep their DEFAULT bodies so
		// the DEC-040 registered entries fire through the real wiring.
		const timeline: string[] = [];
		const bodies: Partial<Record<StageId, StageBody>> = {};
		for (const id of STAGE_IDS) {
			if (id === "cron_scheduler" || id === "embedded_watchers") continue;
			bodies[id] = async () => {
				timeline.push(`stage:${id}`);
			};
		}
		const captured = captureLogger();
		const lifecycle = new GatewayLifecycle({
			home,
			stageBodies: bodies,
			logger: captured.logger,
			services: {
				cron_scheduler: [recordingEntry("cron.ticker", timeline)],
				embedded_watchers: [
					recordingEntry("hooks.extensions", timeline),
					recordingEntry("kanban.dispatcher", timeline),
					recordingEntry("delegation.watcher", timeline),
					recordingEntry("handoff.watcher", timeline),
				],
			},
		});

		const result = await lifecycle.startup();

		expect(result.ok).toBe(true);
		// The ten-stage ORDER stays binding at the trace level…
		expect(lifecycle.events.map((e) => e.stage)).toEqual([...STAGE_IDS]);
		expect(lifecycle.events.every((e) => e.ok && !e.degraded)).toBe(true);
		expect(lifecycle.state).toBe("running");
		expect(result.degradedStages).toEqual([]);
		// …and each registered service fired strictly INSIDE its own stage slot
		// (cron providers in stage 7, watcher-slot entries in stage 8, siblings
		// in registration order, nothing firing anywhere else). The default 7/8
		// bodies don't write timeline markers — the ENGINE's own per-service start
		// logs carry the stage attribution instead.
		expect(timeline).toEqual([
			"stage:profile_override",
			"stage:load_config",
			"stage:boot_fingerprint",
			"stage:duplicate_guard",
			"stage:runtime_lock",
			"stage:open_state_db",
			"service:cron.ticker",
			"service:hooks.extensions",
			"service:kanban.dispatcher",
			"service:delegation.watcher",
			"service:handoff.watcher",
			"stage:platform_adapters",
			"stage:runtime_identity",
		]);
		const started = (name: string): string | undefined =>
			captured.lines.find((l) => l.msg.includes(`service ${name} started`))
				?.msg;
		expect(started("cron.ticker")).toContain("(stage cron_scheduler)");
		for (const name of [
			"hooks.extensions",
			"kanban.dispatcher",
			"delegation.watcher",
			"handoff.watcher",
		]) {
			expect(started(name)).toContain("(stage embedded_watchers)");
		}
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

describe("registered per-service entries: isolation + shutdown (DEC-040)", () => {
	function partialBodies(timeline: string[]): {
		bodies: Partial<Record<StageId, StageBody>>;
		lateCtx: StageContext[];
	} {
		const lateCtx: StageContext[] = [];
		const bodies: Partial<Record<StageId, StageBody>> = {};
		for (const id of STAGE_IDS) {
			if (id === "cron_scheduler" || id === "embedded_watchers") continue;
			bodies[id] = async (ctx) => {
				timeline.push(`stage:${id}`);
				if (id === "platform_adapters") lateCtx.push(ctx);
			};
		}
		return { bodies, lateCtx };
	}

	it("a THROWING or degraded service is isolated: siblings still start, later stages still run, the STAGE completes ok", async () => {
		const timeline: string[] = [];
		const { bodies, lateCtx } = partialBodies(timeline);
		const captured = captureLogger();
		const lifecycle = new GatewayLifecycle({
			home,
			stageBodies: bodies,
			logger: captured.logger,
			services: {
				cron_scheduler: [
					{
						name: "cron.ticker",
						start: async () => {
							timeline.push("service:cron.ticker");
							throw new Error("tick lock exploded");
						},
					},
					{
						name: "cron.backup",
						start: async () => {
							timeline.push("service:cron.backup");
							return {
								ok: true,
								handle: { name: "cron.backup", stop: async () => {} },
							};
						},
					},
				],
				embedded_watchers: [
					{
						name: "kanban.dispatcher",
						start: async () => {
							timeline.push("service:kanban.dispatcher");
							return { ok: false, degraded: true, reason: "board refused" };
						},
					},
					{
						name: "handoff.watcher",
						start: async () => {
							timeline.push("service:handoff.watcher");
							return {
								ok: true,
								handle: { name: "handoff.watcher", stop: async () => {} },
							};
						},
					},
				],
			},
		});

		const result = await lifecycle.startup();

		// Startup CONTINUES and the stage slots complete ok — degradation is
		// PER SERVICE, never a stage-level event here.
		expect(result.ok).toBe(true);
		expect(result.degradedStages).toEqual([]);
		for (const stage of ["cron_scheduler", "embedded_watchers"] as const) {
			const event = lifecycle.events.find((e) => e.stage === stage);
			expect(event?.ok).toBe(true);
			expect(event?.degraded).toBeUndefined();
		}
		// Both failures recorded per service, in fire order…
		expect(lifecycle.degradedServices).toEqual([
			{
				stage: "cron_scheduler",
				service: "cron.ticker",
				reason: "tick lock exploded",
			},
			{
				stage: "embedded_watchers",
				service: "kanban.dispatcher",
				reason: "board refused",
			},
		]);
		// …logged LOUDLY at ERROR level with the degraded_start reason code.
		const loud = captured.lines.filter((l) => l.level === "error");
		expect(
			loud.some(
				(l) =>
					l.msg.includes("cron.ticker DEGRADED") &&
					l.msg.includes("tick lock exploded"),
			),
		).toBe(true);
		expect(
			loud.some(
				(l) =>
					l.msg.includes("kanban.dispatcher DEGRADED") &&
					l.msg.includes("board refused"),
			),
		).toBe(true);
		// Sibling services fired AFTER their failing peers; later stages ran.
		expect(timeline.indexOf("service:cron.backup")).toBeGreaterThan(
			timeline.indexOf("service:cron.ticker"),
		);
		expect(timeline.indexOf("service:handoff.watcher")).toBeGreaterThan(
			timeline.indexOf("service:kanban.dispatcher"),
		);
		expect(timeline).toContain("stage:platform_adapters");
		expect(timeline).toContain("stage:runtime_identity");
		// Successful handles landed in the supervised ctx slots (drain input).
		const ctx = lateCtx[0];
		if (!ctx) throw new Error("stage 9 body never received ctx");
		expect(ctx.services.cron.map((s) => s.name)).toEqual(["cron.backup"]);
		expect(ctx.services.watchers.map((s) => s.name)).toEqual([
			"handoff.watcher",
		]);
	});

	it("a DISABLED outcome (ok:false, no degraded flag) is loud but NOT a degradation", async () => {
		const timeline: string[] = [];
		const { bodies, lateCtx } = partialBodies(timeline);
		const captured = captureLogger();
		const lifecycle = new GatewayLifecycle({
			home,
			stageBodies: bodies,
			logger: captured.logger,
		});
		lifecycle.registerService("embedded_watchers", {
			name: "kanban.dispatcher",
			start: async () => ({
				ok: false,
				reason: "disabled via HERMES_KANBAN_DISPATCH_IN_GATEWAY",
			}),
		});

		const result = await lifecycle.startup();

		expect(result.ok).toBe(true);
		expect(lifecycle.degradedServices).toEqual([]);
		const warns = captured.lines.filter((l) => l.level === "warn");
		expect(
			warns.some((l) => l.msg.includes("kanban.dispatcher not started")),
		).toBe(true);
		expect(captured.lines.some((l) => l.level === "error")).toBe(false);
		expect(lateCtx[0]?.services.watchers).toEqual([]);
		expect(timeline).toContain("stage:runtime_identity");
	});

	it("registered handles are STOPPED by the graceful drain", async () => {
		let stopped = false;
		const bodies: Partial<Record<StageId, StageBody>> = {};
		for (const id of STAGE_IDS) {
			if (id === "cron_scheduler" || id === "embedded_watchers") continue;
			bodies[id] = async () => {};
		}
		const lifecycle = new GatewayLifecycle({
			home,
			stageBodies: bodies,
		});
		lifecycle.registerService("embedded_watchers", {
			name: "handoff.watcher",
			start: async () => ({
				ok: true,
				handle: {
					name: "handoff.watcher",
					stop: async () => {
						stopped = true;
					},
				},
			}),
		});
		await lifecycle.startup();

		await lifecycle.requestShutdown();

		expect(stopped).toBe(true);
	});
});
