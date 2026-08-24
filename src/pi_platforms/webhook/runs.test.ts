// BEHAVIOR CONTRACTS — the /v1/runs lane (api_server.py parity): unbounded
// window held open under lifecycle management; approval/steer/stop each
// reachable; cooperative interruption; pop-or-409 approvals with the run_id
// namespace; terminal retention under injected clock.

import { describe, expect, it } from "vitest";
import {
	RunRegistry,
	RunStoppedError,
	normalizeApprovalChoice,
	type RunEvent,
} from "./runs.js";

function deferred<T>() {
	let resolve!: (v: T) => void;
	let reject!: (e: unknown) => void;
	const promise = new Promise<T>((res, rej) => {
		resolve = res;
		reject = rej;
	});
	return { promise, resolve, reject };
}

describe("run lifecycle + typed events", () => {
	it("start → deltas → completed, with buffered replay for late subscribers", async () => {
		const registry = new RunRegistry();
		const events: RunEvent[] = [];
		const done = deferred<void>();
		let runId = "";
		registry.start("do the thing", async (controls) => {
			runId = controls.runId;
			controls.emitDelta("chunk one ");
			controls.emitDelta("chunk two");
			return "the final output";
		});
		registry.subscribe(runId, (event) => {
			events.push(event);
			if (event.type === "run.completed") done.resolve();
		});
		await done.promise;
		expect(events.map((e) => e.type)).toEqual([
			"message.delta",
			"message.delta",
			"run.completed",
		]);
		const completed = events.find((e) => e.type === "run.completed");
		expect(completed).toMatchObject({ output: "the final output" });
		expect(registry.status(runId)?.status).toBe("completed");
	});

	it("executor failure surfaces run.failed with the error", async () => {
		const registry = new RunRegistry();
		const failed = deferred<Extract<RunEvent, { type: "run.failed" }>>();
		let runId = "";
		registry.start("y", async (controls) => {
			runId = controls.runId;
			registry.subscribe(runId, (e) => {
				if (e.type === "run.failed") failed.resolve(e);
			});
			throw new Error("tool exploded");
		});
		const event = await failed.promise;
		expect(event.type).toBe("run.failed");
		if (event.type === "run.failed") {
			expect(event.error).toBe("tool exploded");
		}
		expect(registry.status(runId)?.status).toBe("failed");
	});
});

describe("approvals — hold-open + pop-or-409 (approval namespace = run_id)", () => {
	it("waiting_for_approval holds the window OPEN; choice resumes the run", async () => {
		const registry = new RunRegistry();
		const requestSeen =
			deferred<Extract<RunEvent, { type: "approval.request" }>>();
		const responded = deferred<void>();
		let runId = "";
		const finalP = deferred<string>();
		registry.start("deploy now?", async (controls) => {
			runId = controls.runId;
			registry.subscribe(runId, (e) => {
				if (e.type === "approval.request") requestSeen.resolve(e);
				if (e.type === "approval.responded") responded.resolve();
			});
			const choice = await controls.requestApproval("rm -rf /tmp/x");
			finalP.resolve(`approved:${choice}`);
			return `ran with ${choice}`;
		});
		const request = await requestSeen.promise;
		expect(registry.status(runId)?.status).toBe("waiting_for_approval");

		// The bounded-window sibling would have answered long ago; THIS lane
		// holds open until a human decides.
		const approve = registry.respondApproval(request.approvalId, "once");
		if (!approve.ok)
			throw new Error(`approval failed: ${JSON.stringify(approve)}`);
		expect(approve.choice).toBe("once");
		await responded.promise;
		expect(await finalP.promise).toBe("approved:once");
		expect(registry.status(runId)?.status).toBe("completed");
	});

	it("aliases normalize (approve/approved/allow → once)", () => {
		expect(normalizeApprovalChoice("approve")).toBe("once");
		expect(normalizeApprovalChoice("ALLOW")).toBe("once");
		expect(normalizeApprovalChoice("session")).toBe("session");
		expect(normalizeApprovalChoice("bogus")).toBeNull();
	});

	it("double-respond → 409 approval_not_pending; no pending approval → approval_not_active", async () => {
		const registry = new RunRegistry();
		const requestSeen = deferred<number>();
		const released = deferred<void>();
		let runId = "";
		registry.start("a", async (controls) => {
			runId = controls.runId;
			registry.subscribe(runId, (e) => {
				if (e.type === "approval.request") requestSeen.resolve(e.approvalId);
			});
			await controls.requestApproval("cmd");
			released.resolve();
			return "ok";
		});
		const approvalId = await requestSeen.promise;
		expect(registry.respondApproval(approvalId, "deny").ok).toBe(true);
		await released.promise;

		const again = registry.respondApproval(approvalId, "deny");
		expect(again).toEqual({ ok: false, code: "approval_not_pending" });

		const none = registry.respondApproval(-999, "once");
		expect(none).toEqual({ ok: false, code: "approval_not_pending" });

		// A live run WITHOUT an open approval answers approval_not_active.
		const started = deferred<string>();
		registry.start("b", async (controls) => {
			started.resolve(controls.runId);
			return "ok";
		});
		const other = await started.promise;
		expect(registry.respondApprovalForRun(other, "once")).toEqual({
			ok: false,
			code: "approval_not_active",
		});
	});

	it("invalid choice → invalid_choice without consuming the pending slot", async () => {
		const registry = new RunRegistry();
		const requestSeen = deferred<number>();
		registry.start("a", async (controls) => {
			registry.subscribe(controls.runId, (e) => {
				if (e.type === "approval.request") requestSeen.resolve(e.approvalId);
			});
			return await controls.requestApproval("cmd");
		});
		const approvalId = await requestSeen.promise;
		expect(registry.respondApproval(approvalId, "nonsense")).toEqual({
			ok: false,
			code: "invalid_choice",
		});
		// The slot survives — a real choice still resolves.
		expect(registry.respondApproval(approvalId, "always").ok).toBe(true);
	});
});

describe("steer — only running runs accept text", () => {
	it("steer while running records text + emits run.steered; executor consumes it", async () => {
		const registry = new RunRegistry();
		const steered = deferred<string>();
		let consumedByExecutor: string | null = null;
		let runId = "";
		registry.start("t", async (controls) => {
			runId = controls.runId;
			registry.subscribe(runId, (e) => {
				if (e.type === "run.steered") steered.resolve(e.text);
			});
			await new Promise<void>((r) => setTimeout(r, 5));
			consumedByExecutor = registry.consumeSteer(runId);
			return "done";
		});
		const steerOutcome = registry.steer(runId, "focus on the tests");
		expect(steerOutcome).toEqual({ ok: true });
		expect(await steered.promise).toBe("focus on the tests");
		await new Promise<void>((r) => setTimeout(r, 10));
		expect(consumedByExecutor).toBe("focus on the tests");
	});

	it("steering a waiting_for_approval or terminal run → 409 run_not_accepting_steer", async () => {
		const registry = new RunRegistry();
		const requestSeen = deferred<void>();
		registry.start("a", async (controls) => {
			registry.subscribe(controls.runId, () => {});
			requestSeen.resolve();
			return await controls.requestApproval("cmd");
		});
		// find the run: single run so far
		const statusView = registry.runIds()[0] ?? "";
		await requestSeen.promise;
		expect(registry.steer(statusView, "text")).toEqual({
			ok: false,
			code: "run_not_accepting_steer",
		});
		expect(registry.steer("unknown-run", "text")).toEqual({
			ok: false,
			code: "unknown_run",
		});
	});
});

describe("stop — COOPERATIVE interruption", () => {
	it("stop flips stopping + interrupts; executor unwinds into run.cancelled", async () => {
		const registry = new RunRegistry();
		let runId = "";
		let sawStop = false;
		const cancelled = deferred<void>();
		registry.start("long work", async (controls) => {
			runId = controls.runId;
			registry.subscribe(runId, (e) => {
				if (e.type === "run.cancelled") cancelled.resolve();
			});
			while (!controls.shouldStop()) {
				await new Promise<void>((r) => setTimeout(r, 1));
			}
			sawStop = true;
			throw new RunStoppedError(); // executor unwinds cooperatively
		});
		await new Promise<void>((r) => setTimeout(r, 5));
		const stopOutcome = registry.stop(runId);
		expect(stopOutcome.ok).toBe(true);
		expect(registry.status(runId)?.status).toBe("stopping");
		await cancelled.promise;
		expect(sawStop).toBe(true); // the EXECUTOR observed the stop flag
		expect(registry.status(runId)?.status).toBe("cancelled");
	});

	it("stopping during waiting_for_approval rejects the approval gate and cancels", async () => {
		const registry = new RunRegistry();
		let runId = "";
		const gateOpen = deferred<void>();
		const cancelled = deferred<void>();
		registry.start("a", async (controls) => {
			runId = controls.runId;
			registry.subscribe(runId, (e) => {
				if (e.type === "run.cancelled") cancelled.resolve();
			});
			gateOpen.resolve();
			try {
				await controls.requestApproval("cmd");
			} catch {
				throw new RunStoppedError();
			}
			return "never";
		});
		await gateOpen.promise;
		registry.stop(runId);
		await cancelled.promise;
		expect(registry.status(runId)?.status).toBe("cancelled");
	});

	it("terminal runs refuse stop", async () => {
		const registry = new RunRegistry();
		const completed = deferred<string>();
		let runId = "";
		registry.start("quick", async (controls) => {
			runId = controls.runId;
			completed.resolve("done");
			return await completed.promise;
		});
		await completed.promise;
		await new Promise<void>((r) => setTimeout(r, 2));
		expect(registry.stop(runId)).toEqual({
			ok: false,
			code: "run_already_finished",
		});
	});
});

describe("terminal retention (injected clock)", () => {
	it("pruneExpired drops terminal runs past _RUN_STATUS_TTL only", async () => {
		let now = 1_000_000;
		const registry = new RunRegistry({
			nowMs: () => now,
			spawn: (task) => {
				void task().catch(() => {});
			},
		});
		const done = deferred<void>();
		registry.start("x", async () => {
			done.resolve();
			return "out";
		});
		await done.promise;
		now += 1000;
		expect(registry.pruneExpired()).toBe(0); // inside retention
		now += 3_600_000;
		expect(registry.pruneExpired()).toBeGreaterThanOrEqual(1);
	});
});
