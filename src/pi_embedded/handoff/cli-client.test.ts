// Behavior contracts for the CLI side of DEC-008
// (cli_commands_mixin.py:_handle_handoff_command DB half): pending request,
// poll-block on terminal state at 0.5s cadence, 60s deadline, and the
// timeout recovery write — ALL timing through the injected clock (zero wall
// sleeps; the whole 60s deadline elapses in virtual milliseconds via the
// pump).

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { StateStore } from "../../pi_state/index.js";
import { ManualClock, pumpClock } from "./testing/manual-clock.js";
import {
	HANDOFF_TIMED_OUT_MESSAGE,
	HandoffCliClient,
	HandoffQueue,
} from "./index.js";

let dir: string;
let store: StateStore;
let queue: HandoffQueue;
const clock = new ManualClock();
let client: HandoffCliClient;

beforeEach(async () => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-handoff-cli-"));
	store = await StateStore.open(join(dir, "state.db"));
	queue = new HandoffQueue(store.db, { clock });
	client = new HandoffCliClient(queue, { clock });
});

afterEach(async () => {
	await store.close();
});

async function seedRequested(): Promise<void> {
	await queue.ensureSessionRow("cli-1");
	expect(await client.requestHandoff("cli-1", "telegram")).toBe(true);
}

describe("HandoffCliClient.requestHandoff", () => {
	it("refuses a second request while in flight (user must wait)", async () => {
		await seedRequested();
		expect(await client.requestHandoff("cli-1", "slack")).toBe(false);
	});
});

describe("HandoffCliClient.pollUntilTerminal", () => {
	it("returns completed as soon as the row completes (mid-poll)", async () => {
		await seedRequested();
		// The gateway claims+completes at virtual t≈1.5s.
		clock.at(1500, async () => {
			await queue.claimHandoff("cli-1");
			await queue.completeHandoff("cli-1");
		});
		const polling = client.pollUntilTerminal("cli-1");
		await Promise.all([polling, pumpClock(clock, 20, 500)]);
		await expect(polling).resolves.toEqual({ kind: "completed" });
	});

	it("returns failed with the recorded error payload", async () => {
		await seedRequested();
		clock.at(1000, () =>
			queue.failHandoff("cli-1", "no home channel configured for telegram"),
		);
		const polling = client.pollUntilTerminal("cli-1");
		await Promise.all([polling, pumpClock(clock, 20, 500)]);
		await expect(polling).resolves.toEqual({
			kind: "failed",
			error: "no home channel configured for telegram",
		});
	});

	it("polls at the 0.5s cadence until terminal", async () => {
		await seedRequested();
		clock.at(2000, () => queue.completeHandoff("cli-1"));
		const before = clock.totalSleepRequests;
		const polling = client.pollUntilTerminal("cli-1");
		await Promise.all([polling, pumpClock(clock, 20, 250)]);
		void polling;
		// ~4 sleeps over 2s of virtual time at the 0.5s cadence.
		const slept = clock.totalSleepRequests - before;
		expect(slept).toBeGreaterThanOrEqual(3);
		expect(slept).toBeLessThanOrEqual(5);
	});

	it("times out at the deadline and RECOVERS the row to failed(retryable)", async () => {
		await seedRequested();
		const before = clock.totalSleepRequests;
		const polling = client.pollUntilTerminal("cli-1");
		// Pump past the 60s deadline in 0.5s steps (121 steps = 60.5s).
		await Promise.all([polling, pumpClock(clock, 130, 500)]);
		expect(await polling).toEqual({ kind: "timeout", lastState: "pending" });

		// Deadline honored: ~120 ticks of 0.5s across 60s.
		const slept = clock.totalSleepRequests - before;
		expect(slept).toBeGreaterThanOrEqual(119);
		expect(slept).toBeLessThanOrEqual(121);

		// THE RECOVERY CONTRACT: the timeout write is unconditional, so even a
		// 'running' row stranded by a crashed gateway converges to failed.
		expect(queue.getHandoffState("cli-1")).toMatchObject({
			state: "failed",
			error: HANDOFF_TIMED_OUT_MESSAGE,
		});
		// …and a retry request succeeds again (terminal ⇒ requestable).
		expect(await client.requestHandoff("cli-1", "telegram")).toBe(true);
	});

	it("recovers a RUNNING-stranded row via the same timeout path", async () => {
		await seedRequested();
		await queue.claimHandoff("cli-1"); // gateway crashed post-claim
		const polling = client.pollUntilTerminal("cli-1");
		await Promise.all([polling, pumpClock(clock, 130, 500)]);
		expect(await polling).toEqual({ kind: "timeout", lastState: "running" });
		expect(queue.getHandoffState("cli-1")?.state).toBe("failed");
	});
});
