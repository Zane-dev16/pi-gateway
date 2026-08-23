// 03 §5.1 Lane A cancel-handoff + §11 row "Cancel-handoff order": /new while
// running responds BEFORE cancelling the old task (#18912 — the "/new"
// confirmation must not be dropped by cancellation side effects); pending is
// drained exactly once; a racing follow-up lands in the new turn(s); failure
// restores the prior guard if the entry is still command-scoped.

import { describe, expect, it } from "vitest";
import { AdapterSessionGuard } from "./l1-adapter-guard.js";
import { makeFixture } from "./testing/manual-spawner.js";

const KEY = "agent:main:telegram:dm:100";

describe("cancel-handoff (Lane A, §5.1)", () => {
	it("/new responds BEFORE cancelling the old task (#18912 exact ordering)", async () => {
		const f = makeFixture();
		f.holdTurns(true);
		await f.guard.handleMessage(f.text("running turn"), KEY);
		f.scheduler.queue.shift();

		// Deterministic ORDER PROBE: the old owner's cancel() appends to the
		// same log the reply sink uses, so response-vs-cancel order becomes a
		// byte-level assertion.
		const { AdapterSessionGuard } = await import("./l1-adapter-guard.js");
		const orderLog: string[] = [];
		let cancelled = false;
		let settled = false;
		let settleOwner: () => void = () => {};
		const ownerTask = {
			result: new Promise<void>((resolve) => {
				settleOwner = resolve;
			}),
			isDone: () => settled,
			cancel: () => {
				cancelled = true;
				orderLog.push("cancelled-old-task");
			},
			cancelRequested: () => cancelled,
		};
		const probeGuard = new AdapterSessionGuard({
			messageHandler: async (event) => `reply:${String(event.text)}`,
			sendReply: async (_chatId, text) => {
				orderLog.push(`sent:${text}`);
			},
			registry: [
				{
					name: "new",
					busyPolicy: "interrupt_then_dispatch",
					busyHandler: "new",
				},
			],
			spawner: () => null,
			cancelWaitTimeoutMs: 40,
		});
		probeGuard.forceInstallGuardForTests(KEY);
		probeGuard.installOwnerForTests(KEY, {
			...ownerTask,
			result: ownerTask.result.then(() => {
				settled = true;
			}),
		} as never);

		await probeGuard.dispatchActiveSessionCommand(f.text("/new"), KEY, "new");
		settleOwner(); // let the bounded wait see the unwound owner

		// EXACT sequence: confirmation hits the wire BEFORE any cancellation.
		expect(orderLog).toEqual(["sent:reply:/new", "cancelled-old-task"]);
		void f;
	}, 30_000);

	it("racing follow-ups during the handoff are each served exactly once", async () => {
		const f = makeFixture();
		f.holdTurns(true);
		await f.guard.handleMessage(f.text("turn-1"), KEY);
		const first = f.scheduler.queue.shift()!;
		first.start();

		// Follow-up queued behind the running turn…
		await f.guard.handleMessage(f.text("followup-a"), KEY);
		// …then /new dispatch begins (parks on its handler):
		const dispatchDone = f.guard.handleMessage(f.text("/new"), KEY);
		// A racing message arrives DURING the command dispatch:
		await f.guard.handleMessage(f.text("racer"), KEY);

		f.holdTurns(false);
		await dispatchDone;
		await f.scheduler.quiesce();

		// Racing TEXT follow-ups COALESCE per §3.1 (merge_text): the slot held
		// "followup-a", so the racer appended to the same composite. Each
		// fragment is served exactly once across the whole chain.
		const joined = f.turns.join("|");
		expect(joined).toContain("followup-a");
		expect(joined).toContain("racer");
		expect(joined.split("followup-a").length - 1).toBe(1);
		expect(joined.split("racer").length - 1).toBe(1);
		expect(f.maxHandlerConcurrency).toBe(1);
		// Session ends clean.
		expect(f.guard.isActive(KEY)).toBe(false);
	}, 30_000);

	it("command failure restores the ORIGINAL guard when the entry is still command-scoped", async () => {
		const f = makeFixture();
		f.holdTurns(true);
		await f.guard.handleMessage(f.text("original"), KEY);
		const originalOwner = f.scheduler.queue.shift()!;
		originalOwner.start();
		const originalGuard = f.guard.guardOf(KEY)!;

		// Deterministic probe with a throwing runner handler and sentinel
		// spawner (no frames spawn in this probe).
		const probes: string[] = [];
		const probeGuard = new AdapterSessionGuard({
			messageHandler: async (event) => {
				if (event.text === "/new") throw new Error("dispatch exploded");
				return `ok:${String(event.text)}`;
			},
			sendReply: async (_chatId, text) => {
				probes.push(text);
			},
			registry: [
				{
					name: "new",
					busyPolicy: "interrupt_then_dispatch",
					busyHandler: "new",
				},
			],
			spawner: () => null,
		});
		probeGuard.forceInstallGuardForTests(KEY);
		const prior = probeGuard.guardOf(KEY)!;
		probeGuard.installOwnerForTests(KEY, {
			result: Promise.resolve(),
			isDone: () => false,
			cancel: () => {},
			cancelRequested: () => false,
		});

		await expect(
			probeGuard.dispatchActiveSessionCommand(f.text("/new"), KEY, "new"),
		).rejects.toThrow("dispatch exploded");

		// Entry still points at the PRIOR guard — restored, not left half-reset.
		expect(probeGuard.isActive(KEY)).toBe(true);
		expect(probeGuard.guardOf(KEY)).toBe(prior);
		void originalGuard; // fixture-side guard unaffected by the probe

		f.holdTurns(false);
		await f.scheduler.quiesce();
	});

	it("cancel is BOUNDED: a wedged owner that ignores cancellation cannot stall dispatch", async () => {
		const CANCEL_BUDGET_MS = 40;
		const f = makeFixture({ cancelWaitTimeoutMs: CANCEL_BUDGET_MS });

		f.holdTurns(true);
		await f.guard.handleMessage(f.text("wedged"), KEY);
		const wedged = f.scheduler.queue.shift()!;
		wedged.start(); // parks on the gate…

		const startedAt = Date.now();
		const dispatch = f.guard.handleMessage(f.text("/stop"), KEY);
		f.holdTurns(false);
		wedged.cancel(); // …and will IGNORE the flag entirely (wedged parity)

		await dispatch; // must NOT hang on the never-settling task
		const elapsed = Date.now() - startedAt;
		expect(elapsed).toBeLessThan(5000); // bounded well under Hermes' 5s ceiling
		// Guard released even though the wedged frame never finished:
		await new Promise<void>((r) => setTimeout(r, CANCEL_BUDGET_MS + 30));
		expect(f.guard.isActive(KEY)).toBe(false);
	}, 30_000);
});
