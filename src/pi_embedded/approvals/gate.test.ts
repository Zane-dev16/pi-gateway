// Behavior contracts for the blocking gate (07 §8.2 binding, §8.4 coalescing,
// §8.6 failure table). EVERY timing assertion runs on ManualClock — a
// wall-clock read anywhere would make these tests lie, so none exists.

import { describe, expect, it } from "vitest";

import {
	ActivityHeartbeat,
	awaitCoalescedLeader,
	awaitGatewayDecision,
	type AwaitDecisionDeps,
	type DecisionResult,
	type ObserverEmit,
} from "./gate.js";
import { ApprovalEntry, ApprovalQueues } from "./queue.js";
import { HumanWaitAccounting } from "./human-wait.js";
import { ManualClock } from "./testing/manual-clock.js";
import { systemClock } from "./clock.js";

const REQUEST = {
	command: "rm -rf /srv/data",
	description: "hardline delete",
	patternKey: "rm_rf",
	patternKeys: ["rm_rf"],
};

function makeDeps(
	clock: ManualClock,
	overrides: Partial<AwaitDecisionDeps> = {},
): AwaitDecisionDeps {
	const deps: AwaitDecisionDeps = {
		queues: new ApprovalQueues(),
		clock,
		timeoutSeconds: 300,
		humanWait: new HumanWaitAccounting(clock, 360),
		notify: async () => {},
	};
	return { ...deps, ...overrides };
}

function launch(
	deps: AwaitDecisionDeps,
	sessionKey: string,
	request = REQUEST,
): Promise<DecisionResult> {
	return awaitGatewayDecision(deps, sessionKey, request);
}

/** Drain macrotasks so parked coroutines advance to their next await point. */
async function flush(rounds = 6): Promise<void> {
	for (let i = 0; i < rounds; i++) {
		await new Promise<void>((resolve) => setTimeout(resolve, 0));
	}
}

describe("wait loop — deadline, slices, heartbeats", () => {
	it("unanswered wait times out at exactly approvals.timeout, fail-closed", async () => {
		const clock = new ManualClock();
		const deps = makeDeps(clock);
		const pending = launch(deps, "s");

		const result = await pending;
		expect(result.resolved).toBe(false);
		expect(result.choice).toBeNull();
		// Deadline honored on the injected clock: queue empty again.
		expect(deps.queues.hasBlocking("s")).toBe(false);
		// Logical clock walked to the deadline (300s) in ≤1s slices.
		expect(Math.round(clock.nowSeconds() - 1000)).toBe(300);
	});

	it("fires activity heartbeats every ~10s while a human thinks", async () => {
		const clock = new ManualClock();
		const notes: string[] = [];
		const heartbeat = new ActivityHeartbeat(clock, (note) => notes.push(note));
		const deps = makeDeps(clock, { heartbeat });

		await launch(deps, "s");

		// Slices run 1s each; a touch fires whenever ≥10s accrued since the last
		// one. The post-slice evaluation lands touches at t=10…1300-1000 → 30.
		expect(notes.length).toBe(30);
		expect(notes[0]).toContain("waiting for user approval");
		expect(notes[0]).toContain("10s elapsed");
	});

	it("resolution during the wait unblocks before the deadline", async () => {
		const clock = new ManualClock();
		let notifyCount = 0;
		const deps = makeDeps(clock, {
			notify: async () => {
				notifyCount += 1;
				// The human taps approve 5 virtual seconds after the prompt.
				clock.at(5000, () => {
					deps.queues.resolve("s", "session");
				});
			},
		});
		const hooks: string[] = [];
		const emit: ObserverEmit = async (event) => {
			hooks.push(event);
		};
		deps.hooks = { emit };

		const result = await launch(deps, "s");

		expect(notifyCount).toBe(1);
		expect(result).toEqual({
			resolved: true,
			choice: "session",
			reason: null,
		});
		expect(clock.nowSeconds() - 1000).toBeLessThan(300);
		expect(hooks).toEqual(["pre_approval_request", "post_approval_response"]);
	});

	it("interrupt-first resolves DENY so the loop unwinds via the denial path (#8697)", async () => {
		const clock = new ManualClock();
		let interrupted = false;
		const deps = makeDeps(clock, {
			isInterrupted: () => interrupted,
			notify: async () => {
				// /stop arrives mid-wait, 2.5s into the human-think window.
				clock.at(2500, () => {
					interrupted = true;
				});
			},
		});

		const result = await launch(deps, "s");
		expect(result.resolved).toBe(true);
		expect(result.choice).toBe("deny");
	});

	it("timeout ≠ explicit deny for telemetry: post hook reports 'timeout'", async () => {
		const clock = new ManualClock();
		const postChoices: string[] = [];
		const deps = makeDeps(clock, {
			hooks: {
				emit: async (event, context) => {
					if (event === "post_approval_response") {
						postChoices.push(String(context?.choice));
					}
				},
			},
		});
		await launch(deps, "s");
		expect(postChoices).toEqual(["timeout"]);
	});
});

describe("bridge down / notify failure — loud, never hanging", () => {
	it("notify raising drops the entry and returns notify_failed immediately", async () => {
		const clock = new ManualClock();
		const dropped: unknown[] = [];
		const deps = makeDeps(clock, {
			notify: async () => {
				throw new Error("platform send exploded");
			},
		});
		const postChoices: string[] = [];
		deps.hooks = {
			emit: async (event, context) => {
				if (event === "post_approval_response") {
					postChoices.push(String(context?.choice));
				}
			},
		};

		const result = await launch(deps, "s");
		expect(result.notifyFailed).toBe(true);
		expect(result.resolved).toBe(false);
		expect(deps.queues.hasBlocking("s")).toBe(false); // entry dropped
		expect(postChoices).toEqual(["notify_failed"]);
		expect(clock.nowSeconds() - 1000).toBe(0); // no silent hang
		void dropped;
	});

	it("UNREGISTERED session (no notify wired) fails loudly, not with a hang", async () => {
		const clock = new ManualClock();
		const deps = makeDeps(clock, {
			notify: async (_sessionKey) => {
				throw new Error(
					"approval bridge down: no delivery registered for session s",
				);
			},
		});
		const result = await Promise.race([
			launch(deps, "never-registered"),
			clock.sleepMs(50).then(() => ({ hung: true })),
		]);
		expect(result).toMatchObject({ notifyFailed: true });
	});
});

describe("coalescing — N duplicates ⇒ ONE card, strict consent adoption", () => {
	it("N identical requests fire ONE notify; one answer satisfies all waiters", async () => {
		const clock = new ManualClock();
		let notifyCount = 0;
		let preHooks = 0;
		const queues = new ApprovalQueues();
		const deps: AwaitDecisionDeps = {
			queues,
			clock,
			timeoutSeconds: 300,
			humanWait: new HumanWaitAccounting(clock, 360),
			notify: async () => {
				notifyCount += 1;
			},
			hooks: {
				emit: async (event) => {
					if (event === "pre_approval_request") preHooks += 1;
				},
			},
		};

		// Deterministic sequencing: park the leader FIRST, then launch followers
		// so each finds the identical pending entry and coalesces onto it.
		const leaderPromise = launch(deps, "s");
		await flush(); // leader enqueued + notified + parked in its poll loop

		const followerPromises = [
			launch(deps, "s"),
			launch(deps, "s"),
			launch(deps, "s"),
		];
		await flush(); // followers parked on the leader's entry

		// ONE answer satisfies ALL waiters.
		expect(queues.resolve("s", "session")).toBe(1);

		const leader = await leaderPromise;
		const followerResults = await Promise.all(followerPromises);

		expect(notifyCount).toBe(1); // ONE prompt, not four
		expect(leader.resolved).toBe(true);
		expect(leader.choice).toBe("session");
		expect(leader.coalesced).toBeUndefined(); // the LEADER prompted directly
		for (const result of followerResults) {
			expect(result.resolved).toBe(true);
			expect(result.choice).toBe("session");
			expect(result.coalesced).toBe(true);
		}
		// Leader + 3 follower pre hooks, still only one user-facing prompt.
		expect(preHooks).toBe(4);
	});

	it("'once' is single-use consent: the follower falls through to a FRESH prompt", async () => {
		const clock = new ManualClock();
		const notifyCommands: string[] = [];
		const deps = makeDeps(clock, {
			notify: async (_key, data) => {
				notifyCommands.push(data.command);
			},
		});

		const leaderPromise = launch(deps, "s");
		await flush();

		const followerPromise = launch(deps, "s");
		await flush(); // follower parked on the leader's event

		// The human answers "once" — single-use consent covering ONLY the leader.
		deps.queues.resolve("s", "once");
		const leaderResult = await leaderPromise;
		await flush(); // follower re-prompts (fresh entry #2)

		// Leader consumed the single-use consent…
		expect(leaderResult.choice).toBe("once");
		// …the follower got its OWN fresh prompt and its own decision slot.
		expect(notifyCommands.length).toBeGreaterThanOrEqual(2);

		deps.queues.resolve("s", "deny");
		const followerResult = await followerPromise;
		expect(followerResult.resolved).toBe(true);
		expect(followerResult.choice).toBe("deny");
		expect(followerResult.coalesced).toBeUndefined();
	});

	it("deny adopts the refusal for followers (re-asking is an evasion path)", async () => {
		const clock = new ManualClock();
		const deps = makeDeps(clock);

		const leaderPromise = launch(deps, "s");
		await flush();
		const followerPromise = launch(deps, "s");
		await flush();

		deps.queues.resolve("s", "deny", { reason: "absolutely not" });

		const [leader, follower] = await Promise.all([
			leaderPromise,
			followerPromise,
		]);
		expect(follower.resolved).toBe(true);
		expect(follower.choice).toBe("deny");
		expect(follower.reason).toBe("absolutely not"); // leader's reason relayed
		expect(leader.choice).toBe("deny");
	});

	it("a follower's interrupt denies ONLY the follower; the leader keeps waiting", async () => {
		const clock = new ManualClock();
		let followerInterrupted = false;
		const queues = new ApprovalQueues();
		const deps: AwaitDecisionDeps = {
			queues,
			clock,
			timeoutSeconds: 300,
			humanWait: new HumanWaitAccounting(clock, 360),
			isInterrupted: () => false, // replaced per-call below
			notify: async () => {},
		};

		const leaderPromise = awaitGatewayDecision(deps, "s", REQUEST);

		// Wait until the leader's entry is queued, then launch the follower with
		// its own interrupt flag.
		await Promise.resolve();
		const followerDeps: AwaitDecisionDeps = {
			...deps,
			isInterrupted: () => followerInterrupted,
		};
		followerInterrupted = true;
		const followerResult = await awaitGatewayDecision(
			followerDeps,
			"s",
			REQUEST,
		);
		expect(followerResult.choice).toBe("deny");
		// The LEADER's entry survives the follower's interrupt.
		expect(queues.hasBlocking("s")).toBe(true);
		const leaderEntry = queues.listApprovals("s")[0];
		expect(leaderEntry?.command).toBe(REQUEST.command);
		void leaderPromise;
	});
});

describe("coalesced leader primitive (direct)", () => {
	it("adopts the leader's decision and flags the result as coalesced", async () => {
		const clock = new ManualClock();
		const deps = makeDeps(clock);
		const leader = new ApprovalEntry({ ...REQUEST });
		deps.queues.enqueue("s", leader);
		clock.at(1000, () => {
			leader.result = "always";
			leader.settle();
		});

		const followerData = new ApprovalEntry({ ...REQUEST }).data;
		const result = await awaitCoalescedLeader(deps, "s", leader, followerData);
		expect(result).toEqual({
			resolved: true,
			choice: "always",
			reason: null,
			coalesced: true,
		});
	});

	it("returns null ONLY for a 'once' resolution (fresh-prompt contract)", async () => {
		const clock = new ManualClock();
		const deps = makeDeps(clock);
		const leader = new ApprovalEntry({ ...REQUEST });
		deps.queues.enqueue("s", leader);
		clock.at(1000, () => {
			leader.result = "once";
			leader.settle();
		});

		const followerData = new ApprovalEntry({ ...REQUEST }).data;
		await expect(
			awaitCoalescedLeader(deps, "s", leader, followerData),
		).resolves.toBeNull();
	});
});

describe("human-wait accounting (#79719)", () => {
	it("the whole approval wait counts as human-wait time, clamped at the ceiling", async () => {
		const clock = new ManualClock();
		const accounting = new HumanWaitAccounting(clock, 360);
		const deps = makeDeps(clock, { humanWait: accounting });
		await launch(deps, "s");
		// 300 s waited → 300 s recorded (< ceiling of 360).
		expect(accounting.seconds("s")).toBe(300);
	});

	it("a wedged window contributes at most the ceiling, never the overstay", () => {
		const clock = new ManualClock();
		const accounting = new HumanWaitAccounting(clock, 360);
		const close = accounting.begin("s");
		clock.advance(400_000); // window overstays badly (400 s; wedged closer)
		close();
		expect(accounting.seconds("s")).toBe(360);
	});

	it("overlapping windows coalesce instead of double-counting", () => {
		const clock = new ManualClock();
		const accounting = new HumanWaitAccounting(clock, 360);
		const outer = accounting.begin("s");
		clock.advance(30_000);
		const inner = accounting.begin("s");
		clock.advance(30_000);
		inner();
		outer();
		// One continuous 60 s wall span — counted once, not twice.
		expect(accounting.seconds("s")).toBe(60);
	});
});

describe("system clock sanity", () => {
	it("production clock reads real time (single wall-clock boundary)", () => {
		expect(systemClock.nowSeconds()).toBeGreaterThan(1_700_000_000);
	});
});

// ── cron-session contexts (secops-9; tools/approval.py parity) ────────────
// Cron jobs are NEVER gateway-approval contexts: no human listens on any
// chat surface, so the gate must resolve IMMEDIATELY from approvals.cron_mode
// (default deny) WITHOUT enqueueing a pending prompt that would block the job
// for the full timeout.
describe("cron-session gate branch", () => {
	it("default cron_mode DENIES immediately: no entry, no notify, no wait", async () => {
		const clock = new ManualClock();
		let notifyCalls = 0;
		const hooks: Array<{ event: string; choice?: string | undefined }> = [];
		const emit: ObserverEmit = async (event, context) => {
			hooks.push({
				event,
				choice: (context as { choice?: string } | undefined)?.choice,
			});
		};
		const deps = makeDeps(clock, {
			isCronSession: () => true,
			hooks: { emit },
			notify: async () => {
				notifyCalls += 1;
			},
		});

		const result = await awaitGatewayDecision(deps, cronSessionId(), REQUEST, {
			surface: "cron",
		});

		expect(result).toMatchObject({ resolved: true, choice: "deny" });
		expect(result.reason).toContain("approvals.cron_mode: approve");
		expect(notifyCalls).toBe(0); // NO prompt was ever sent
		expect(deps.queues.hasBlocking(cronSessionId())).toBe(false); // NOTHING pending
		expect(deps.queues.listApprovals(cronSessionId())).toEqual([]);
		expect(clock.nowMs).toBe(1_000_000); // resolved without consuming ANY time
		expect(hooks.map((h) => h.event)).toEqual([
			"pre_approval_request",
			"post_approval_response",
		]);
		expect(hooks[1]?.choice).toBe("deny");
	});

	it("cron_mode approve resolves as single-use consent, still without enqueueing", async () => {
		const clock = new ManualClock();
		const deps = makeDeps(clock, {
			isCronSession: () => true,
			cronMode: "approve",
		});
		const result = await launch(deps, "cron:job");
		expect(result).toMatchObject({ resolved: true, choice: "once" });
		expect(result.reason).toBeUndefined();
		expect(deps.queues.hasBlocking("cron:job")).toBe(false);
	});

	it("raw config vocabulary normalizes: allow/yes/off approve, garbage denies", async () => {
		for (const [raw, expected] of [
			["allow", "once"],
			[" yes ", "once"],
			["off", "once"],
			["banana", "deny"],
			["", "deny"],
		] as const) {
			const clock = new ManualClock();
			const deps = makeDeps(clock, {
				isCronSession: () => true,
				cronMode: () => raw,
			});
			const result = await launch(deps, "cron:job");
			expect(result.choice).toBe(expected);
		}
	});

	it("a resolver THROWING fails safe to deny (parity of the except-deny guard)", async () => {
		const clock = new ManualClock();
		const deps = makeDeps(clock, {
			isCronSession: () => true,
			cronMode: (): string => {
				throw new Error("config read failed");
			},
		});
		const result = await launch(deps, "cron:job");
		expect(result).toMatchObject({ resolved: true, choice: "deny" });
	});

	it("non-cron sessions keep the NORMAL blocking path (marker absent ⇒ untouched)", async () => {
		const clock = new ManualClock();
		let notifyCalls = 0;
		const deps = makeDeps(clock, {
			isCronSession: () => false,
			notify: async () => {
				notifyCalls += 1;
			},
		});
		const pending = launch(deps, "s");
		await flush();
		expect(notifyCalls).toBe(1); // prompt delivered normally
		expect(deps.queues.hasBlocking("s")).toBe(true); // pending approval exists
		deps.queues.resolve("s", "once");
		const result = await pending;
		expect(result.resolved).toBe(true);
	});
});

function cronSessionId(): string {
	return "cron:job-secops9";
}
