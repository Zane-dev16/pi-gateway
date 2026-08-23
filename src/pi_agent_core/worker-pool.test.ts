// Worker pool behavior contracts (01 §2.1): >N demands never exceed N
// concurrent; structured cancellation with generation tokens — queued stale
// demands never execute and never occupy a slot; running ones get aborted via
// their signal; a cancelled slot frees immediately.

import { describe, expect, it } from "vitest";

import { TaskCancelledError, TurnWorkerPool } from "./worker-pool.js";

function deferred<T>() {
	let resolve!: (v: T) => void;
	const promise = new Promise<T>((r) => {
		resolve = r;
	});
	return { promise, resolve };
}

describe("TurnWorkerPool — bounded concurrency", () => {
	it("25 demands on 10 workers never exceed 10 concurrent and all complete", async () => {
		const pool = new TurnWorkerPool({ maxWorkers: 10 });
		let active = 0;
		let maxActive = 0;
		const release = deferred<void>();

		const demands = Array.from({ length: 25 }, (_, i) =>
			pool.submit({
				key: `s${i % 5}`,
				generation: 1,
				run: async () => {
					active += 1;
					maxActive = Math.max(maxActive, active);
					await release.promise;
					active -= 1;
					return i;
				},
			}),
		);

		// Let the queue drain into workers.
		await new Promise((r) => setTimeout(r, 20));
		expect(pool.active).toBe(10);
		expect(pool.pending).toBe(15);
		release.resolve();

		const results = await Promise.all(demands);
		expect(results).toHaveLength(25);
		expect([...results].sort((a, b) => (a ?? 0) - (b ?? 0))[24]).toBe(24);
		expect(maxActive).toBeLessThanOrEqual(10);
		expect(pool.active).toBe(0);
	});

	it("FIFO order is preserved within a session's demands", async () => {
		const pool = new TurnWorkerPool({ maxWorkers: 1 });
		const order: number[] = [];
		const jobs = [1, 2, 3].map((n) =>
			pool.submit({
				key: "one",
				generation: 1,
				run: async () => {
					order.push(n);
					return n;
				},
			}),
		);
		await Promise.all(jobs);
		expect(order).toEqual([1, 2, 3]);
	});
});

describe("TurnWorkerPool — structured cancellation (generation tokens)", () => {
	it("cancelKey rejects QUEUED demands without executing them; slots free for later work", async () => {
		const pool = new TurnWorkerPool({ maxWorkers: 1 });
		const blocked = deferred<void>();
		let runningStarted = false;

		const first = pool.submit({
			key: "sess",
			generation: 1,
			run: async () => {
				runningStarted = true;
				await blocked.promise;
				return "first";
			},
		});

		const queuedCancel = pool.submit({
			key: "sess",
			generation: 1,
			run: async () => "should-never-run",
		});
		await new Promise((r) => setTimeout(r, 10));
		expect(runningStarted).toBe(true);
		expect(pool.pending).toBe(1);

		const cancelledCount = pool.cancelKey("sess");
		expect(cancelledCount).toBe(2); // 1 running (aborted) + 1 queued (rejected)

		await expect(queuedCancel).rejects.toBeInstanceOf(TaskCancelledError);
		blocked.resolve();
		await expect(first).resolves.toBe("first"); // running task finishes its body

		// Slot freed → new demand executes.
		const next = await pool.submit({
			key: "other",
			generation: 1,
			run: async () => "next",
		});
		expect(next).toBe("next");
		expect(pool.pending).toBe(0);
	});

	it("a demand whose generation was invalidated BEFORE submit is rejected without starting", async () => {
		const pool = new TurnWorkerPool({ maxWorkers: 4 });
		pool.cancelKey("sess", 5); // generations <= 5 are stale
		let started = false;
		await expect(
			pool.submit({
				key: "sess",
				generation: 3,
				run: async () => {
					started = true;
					return "x";
				},
			}),
		).rejects.toBeInstanceOf(TaskCancelledError);
		expect(started).toBe(false);

		// A NEWER generation still runs.
		const fresh = await pool.submit({
			key: "sess",
			generation: 6,
			run: async () => "fresh-turn",
		});
		expect(fresh).toBe("fresh-turn");
	});

	it("running tasks observe abort on their signal when their generation is invalidated", async () => {
		const pool = new TurnWorkerPool({ maxWorkers: 1 });
		const sawAbort = deferred<string>();
		const gate = deferred<void>();

		const running = pool.submit({
			key: "sess",
			generation: 2,
			run: (signal) =>
				new Promise<string>((resolve) => {
					signal.addEventListener(
						"abort",
						() => {
							sawAbort.resolve(`aborted:${signal.reason ?? ""}`);
							resolve("aborted-run");
						},
						{ once: true },
					);
					void gate.promise.then(() => resolve("completed"));
				}),
		});

		await new Promise((r) => setTimeout(r, 10));
		pool.cancelKey("sess", 2);
		const observed = await sawAbort.promise;
		expect(observed.startsWith("aborted")).toBe(true);
		gate.resolve();
		await running;
	});
});
