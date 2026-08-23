// pi_agent_core/worker-pool.ts — the bounded worker pool that executes agent
// turns (01 §2.1: "Synchronous function run on a bounded worker pool —
// ThreadPoolExecutor(max_workers=10)"). On this runtime (DEC-023) turns are
// event-loop tasks, so the pool is a bounded FIFO executor: >N demands never
// exceed N concurrently executing, and leases — not thread counts — own
// correctness.
//
// Structured cancellation rides GENERATION TOKENS (02 §5 in-process layer
// semantics): each demand carries the session's turn generation; invalidating
// a generation (a) rejects queued demands of that generation WITHOUT executing
// them, and (b) aborts already-running ones through their AbortSignal. A stale
// unwind can therefore never execute against, or free a slot for, the wrong
// turn.

export class TaskCancelledError extends Error {
	readonly key: string;
	readonly generation: number;

	constructor(key: string, generation: number, reason: string) {
		super(`turn cancelled: ${key} gen ${generation} (${reason})`);
		this.name = "TaskCancelledError";
		this.key = key;
		this.generation = generation;
	}
}

export interface PoolDemand<T> {
	/** Contention key — the resolved session id. */
	key: string;
	/** Turn generation this demand belongs to. */
	generation: number;
	run: (signal: AbortSignal) => Promise<T>;
}

interface QueueNode {
	demand: PoolDemand<unknown>;
	resolve: (value: unknown) => void;
	reject: (err: unknown) => void;
	controller: AbortController;
}

export interface WorkerPoolOptions {
	maxWorkers?: number;
}

export class TurnWorkerPool {
	private readonly maxWorkers: number;
	private readonly queue: QueueNode[] = [];
	private readonly running = new Map<string, Set<QueueNode>>();
	private readonly invalidated = new Map<string, number>();
	private activeCount = 0;

	constructor(options: WorkerPoolOptions = {}) {
		this.maxWorkers = Math.max(1, options.maxWorkers ?? 10);
	}

	get maxConcurrent(): number {
		return this.maxWorkers;
	}

	get active(): number {
		return this.activeCount;
	}

	get pending(): number {
		return this.queue.length;
	}

	/**
	 * Highest generation invalidated for `key` so far (0 = none). Demands whose
	 * generation is <= this value at submit OR dequeue time are cancelled.
	 */
	staleBefore(key: string): number {
		return this.invalidated.get(key) ?? 0;
	}

	submit<T>(demand: PoolDemand<T>): Promise<T> {
		const stale = this.staleBefore(demand.key);
		if (demand.generation <= stale) {
			return Promise.reject(
				new TaskCancelledError(
					demand.key,
					demand.generation,
					"generation invalidated before start",
				),
			);
		}
		const controller = new AbortController();
		return new Promise<T>((resolve, reject) => {
			const node: QueueNode = {
				demand: demand as PoolDemand<unknown>,
				resolve: resolve as (value: unknown) => void,
				reject,
				controller,
			};
			this.queue.push(node);
			this.drain();
		});
	}

	/**
	 * Invalidate every queued/running demand of `key` with generation <=
	 * `upToGeneration` (default: all). Queued nodes are rejected immediately —
	 * freeing their slots; running ones receive abort() on their signal.
	 * Returns the number of demands cancelled.
	 */
	cancelKey(key: string, upToGeneration = Number.POSITIVE_INFINITY): number {
		if (!Number.isFinite(upToGeneration)) {
			this.invalidated.set(
				key,
				Math.max(this.staleBefore(key), Number.MAX_SAFE_INTEGER - 1),
			);
		} else {
			this.invalidated.set(
				key,
				Math.max(this.staleBefore(key), upToGeneration),
			);
		}
		let cancelled = 0;

		// Queued: reject + drop (their slots were never occupied).
		for (let i = this.queue.length - 1; i >= 0; i--) {
			const node = this.queue[i];
			if (!node) continue;
			if (
				node.demand.key === key &&
				node.demand.generation <= this.staleBefore(key)
			) {
				this.queue.splice(i, 1);
				node.reject(
					new TaskCancelledError(
						key,
						node.demand.generation,
						"cancelled while queued",
					),
				);
				cancelled += 1;
			}
		}

		// Running: abort via signal; completion path frees the slot.
		const runSet = this.running.get(key);
		if (runSet) {
			for (const node of [...runSet]) {
				if (node.demand.generation <= this.staleBefore(key)) {
					node.controller.abort();
					cancelled += 1;
				}
			}
		}
		return cancelled;
	}

	private drain(): void {
		while (this.activeCount < this.maxWorkers && this.queue.length > 0) {
			const node = this.queue.shift();
			if (!node) break;
			// Dequeue-time staleness check: a demand queued before an
			// invalidation must NEVER execute.
			if (node.demand.generation <= this.staleBefore(node.demand.key)) {
				node.reject(
					new TaskCancelledError(
						node.demand.key,
						node.demand.generation,
						"cancelled while queued",
					),
				);
				continue;
			}
			this.start(node);
		}
	}

	private start(node: QueueNode): void {
		this.activeCount += 1;
		let set = this.running.get(node.demand.key);
		if (!set) {
			set = new Set();
			this.running.set(node.demand.key, set);
		}
		set.add(node);

		void (async () => {
			try {
				const value = await node.demand.run(node.controller.signal);
				node.resolve(value);
			} catch (err) {
				node.reject(err);
			} finally {
				const set2 = this.running.get(node.demand.key);
				if (set2) {
					set2.delete(node);
					if (set2.size === 0) this.running.delete(node.demand.key);
				}
				this.activeCount -= 1;
				this.drain();
			}
		})();
	}

	/** True while any demand for `key` occupies a worker slot. */
	isRunning(key: string): boolean {
		return this.running.has(key);
	}
}
