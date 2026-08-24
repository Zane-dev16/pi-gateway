// Behavior contracts for the cron turn executor: DEC-012 (memory ENABLED,
// provable at the constructor seam) and normal-pipeline execution through the
// REAL GatewayAgentRunner with a scripted model.

import { describe, expect, it } from "vitest";

import {
	CronMemoryPolicyError,
	CronTurnExecutor,
	constructCronAgentPlan,
	cronExecutorAsRunner,
	cronSessionId,
} from "./executor.js";
import type { TimestampActivityLog } from "./inactivity.js";
import {
	createRunnerHarness,
	type RunnerHarness,
} from "../../pi_agent_core/testing/runner-harness.js";
import { fauxAssistantMessage } from "../../pi_agent_core/testing/faux-model.js";

describe("DEC-012 — cron agents run WITH memory (constructor seam)", () => {
	it("the construction plan emits skip_memory=false EXPLICITLY", () => {
		const plan = constructCronAgentPlan("cron:abc");
		expect(plan.skipMemory).toBe(false); // literal runtime value
		expect(plan.platform).toBe("cron");
	});

	it("constructing a memory-off executor throws BEFORE any turn runs", () => {
		expect(
			() =>
				new CronTurnExecutor(
					{
						handleTurn: () => Promise.reject(new Error("unreachable")),
						interrupt: () => Promise.resolve(false),
					},
					// A `true` can only arrive through an unsafe cast — exactly the
					// attempted-deviation shape this seam exists to stop loudly.
					{ skipMemory: true as unknown as false },
				),
		).toThrow(CronMemoryPolicyError);
		try {
			new CronTurnExecutor(
				{
					handleTurn: () => Promise.reject(new Error("unreachable")),
					interrupt: () => Promise.resolve(false),
				},
				{ skipMemory: true as unknown as false },
			);
		} catch (err) {
			expect((err as Error).message).toMatch(/DEC-012/);
			expect((err as Error).message).toMatch(/skip_memory=False/);
		}
	});

	it("the production default constructs cleanly and records the plan on every run", async () => {
		let seenRequests: Array<{ sessionId: string; text: string }> = [];
		const executor = new CronTurnExecutor({
			handleTurn: async (request) => {
				seenRequests = [...seenRequests, request];
				return {
					exitReason: "finalized",
					finalText: "ok",
					iterations: 1,
					repairs: 0,
					userRowId: null,
					assistantRowId: null,
					usage: null,
				};
			},
			interrupt: async () => true,
		});
		const constructed: Array<{ skipMemory: boolean }> = [];
		await executor.run({
			jobId: "jobX",
			prompt: "do it",
			onConstructed: (plan) => {
				constructed.push({ skipMemory: plan.skipMemory });
			},
		});
		expect(constructed).toEqual([{ skipMemory: false }]);
		expect(executor.lastConstruction?.skipMemory).toBe(false);
		const seen = seenRequests[0];
		expect(seen?.sessionId).toBe(cronSessionId("jobX"));
	});
});

describe("cron turns through the NORMAL runner pipeline (real host loop)", () => {
	let harness: RunnerHarness;

	async function freshHarness(memorySpies?: {
		prefetched?: string[];
		synced?: Array<{ userText: string; responseText: string }>;
	}): Promise<RunnerHarness> {
		return createRunnerHarness({
			systemPrompt: "cron pipeline test prompt",
			...(memorySpies
				? {
						memoryHooks: {
							prefetchAll: (query) => {
								memorySpies.prefetched?.push(query);
								return "prefetched memory context";
							},
							syncAll: (input) => {
								memorySpies.synced?.push(input);
							},
						},
					}
				: {}),
		});
	}

	it("runs the job prompt through handleTurn into the job's OWN session; rows persist there", async () => {
		harness = await freshHarness();
		harness.faux.setResponses([fauxAssistantMessage("cron output")]);
		const executor = new CronTurnExecutor(harness.runner);
		const { outcome } = await executor.run({
			jobId: "job42",
			prompt: "run nightly brief",
			ensureSession: (sessionId) => harness.ensureSession(sessionId),
		});

		expect(outcome.exitReason).toBe("finalized");
		expect(outcome.finalText).toBe("cron output");
		// Deliveries originate in the job's OWN session (isolation invariant).
		expect(executor.lastConstruction?.sessionId).toBe("cron:job42");
		const rows = harness.store.listMessages("cron:job42");
		expect(rows.map((r) => r.role)).toEqual(["user", "assistant"]);
		expect(rows[1]!.content).toBe("cron output");
		await harness.close();
	});

	it("memory hooks FIRE for the cron turn — skip_memory=False is provable end-to-end", async () => {
		const spies: {
			prefetched: string[];
			synced: Array<{ userText: string; responseText: string }>;
		} = { prefetched: [], synced: [] };
		harness = await freshHarness(spies);
		harness.faux.setResponses([fauxAssistantMessage("with memory")]);
		const executor = new CronTurnExecutor(harness.runner);
		await executor.run({
			jobId: "mem-job",
			prompt: "remember this",
			ensureSession: (sessionId) => harness.ensureSession(sessionId),
		});

		expect(spies.prefetched).toEqual(["remember this"]); // prefetch leg ran
		expect(spies.synced).toEqual([
			{ userText: "remember this", responseText: "with memory" }, // sync leg ran
		]);
		await harness.close();
	});
});

describe("ScheduledJobRunner adapter", () => {
	it("maps finalized outcomes to ok + output and touches activity around the turn", async () => {
		let nowCalls = 0;
		const executor = new CronTurnExecutor({
			handleTurn: async () => ({
				exitReason: "finalized" as const,
				finalText: "done!",
				iterations: 1,
				repairs: 0,
				userRowId: null,
				assistantRowId: null,
				usage: null,
			}),
			interrupt: async () => true,
		});
		const runner = cronExecutorAsRunner(executor, {
			clock: {
				nowSeconds: () => {
					nowCalls++;
					return 777;
				},
				sleepMs: async () => undefined,
			},
		});
		const touches: number[] = [];
		const activity = {
			touch: (t: number) => {
				touches.push(t);
			},
			secondsSinceActivity: (): number => 0,
		} as unknown as TimestampActivityLog; // structural probe stand-in
		const outcome = await runner.run({
			job: {
				id: "j1",
				name: "n",
				prompt: "p",
				schedule: { kind: "once", run_at: "" },
				enabled: true,
				state: "scheduled",
				created_at: "",
				next_run_at: null,
			},
			activity,
		});
		expect(outcome.ok).toBe(true);
		expect(outcome.outputText).toBe("done!");
		expect(touches.length).toBeGreaterThanOrEqual(2); // start + settle stamps
		expect(nowCalls).toBeGreaterThanOrEqual(2); // stamps read the injected clock
	});

	it("interrupt routes to the job's session through the executor", async () => {
		let interruptedSession: string | null = null;
		const executor = new CronTurnExecutor({
			handleTurn: () => Promise.reject(new Error("unused")),
			interrupt: async (sessionId) => {
				interruptedSession = sessionId;
				return true;
			},
		});
		const runner = cronExecutorAsRunner(executor);
		await runner.interrupt("job9");
		expect(interruptedSession).toBe("cron:job9");
	});
});
