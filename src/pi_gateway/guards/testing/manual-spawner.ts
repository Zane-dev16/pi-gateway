// TEST INFRASTRUCTURE — deterministic scheduling harness for the L1 adapter
// guard. The manual scheduler replaces asyncio.create_task's "run soon"
// semantics with an explicit queue so tests interleave events at exact points
// (03 §11: interleaved-events race, drain boundary, late-arrival requeue).

import type {
	AdapterGuardDeps,
	GatewayTask,
	TaskSpawner,
} from "../l1-adapter-guard.js";
import { AdapterSessionGuard } from "../l1-adapter-guard.js";
import type { IncomingEvent } from "../events.js";

/** A queued frame. `start()` is the deterministic execution point. */
export class ManualTask implements GatewayTask {
	readonly result: Promise<void>;
	started = false;
	private settled = false;
	private cancelled = false;
	private settleFn: () => void = () => {};

	constructor(
		readonly label: string,
		private readonly run: (task: GatewayTask) => Promise<void>,
	) {
		this.result = new Promise<void>((resolve) => {
			this.settleFn = resolve;
		});
	}

	start(): void {
		if (this.started) return;
		this.started = true;
		void this.run(this)
			.catch(() => {})
			.then(() => {
				this.settled = true;
				this.settleFn();
			});
	}

	attachFinishTracker(scheduler: ManualScheduler): void {
		const prev = this.settleFn;
		this.settleFn = () => {
			prev();
			scheduler.markFinished(this);
		};
	}

	isDone(): boolean {
		return this.settled;
	}

	cancel(): void {
		this.cancelled = true; // cooperative — frames poll cancelRequested()
	}

	cancelRequested(): boolean {
		return this.cancelled;
	}
}

export class ManualScheduler {
	readonly queue: ManualTask[] = [];
	readonly finished: ManualTask[] = [];
	private readonly finishedSet = new Set<ManualTask>();
	/** Max frames whose handler section overlapped (stack/parallelism probe). */
	maxConcurrentFrames = 0;
	private inHandler = 0;
	private active = 0;

	spawner: TaskSpawner = (run) => {
		const task = new ManualTask(`frame#${this.queue.length}`, async (self) => {
			this.active++;
			try {
				return await run(self);
			} finally {
				this.active--;
			}
		});
		task.attachFinishTracker(this);
		this.queue.push(task);
		return task;
	};

	/** Start the head frame and await its completion (deterministic step). */
	async step(): Promise<ManualTask | undefined> {
		const task = this.queue.shift();
		if (!task) return undefined;
		this.enterHandler();
		task.start();
		await task.result;
		this.leaveHandler();
		this.finished.push(task);
		return task;
	}

	/** Run every frame the chain produces (drain tasks included), FIFO order. */
	async runToEnd(limit = 1000): Promise<number> {
		let ran = 0;
		for (;;) {
			const next = this.queue.shift();
			if (!next) break;
			ran++;
			if (ran > limit) throw new Error("manual scheduler overflow");
			next.start();
			await next.result;
			this.finished.push(next);
		}
		return ran;
	}

	/** True while any frame is queued or executing (started by anyone). */
	get busy(): boolean {
		return this.active > 0 || this.queue.length > 0;
	}

	/**
	 * Drive every frame — including ones started OUTSIDE the scheduler (held
	 * heads woken later) — until the whole chain settles. Deterministic
	 * replacement for sleep-and-hope.
	 */
	async quiesce(timeoutMs = 10_000): Promise<void> {
		const deadline = Date.now() + timeoutMs;
		for (;;) {
			const next = this.queue.shift();
			if (next !== undefined) {
				next.start();
			} else if (!this.busy) {
				return;
			}
			if (Date.now() > deadline) throw new Error("quiesce timeout");
			await new Promise<void>((r) => setTimeout(r, 1));
		}
	}

	/** Record settled tasks for assertions (called by ManualTask completion). */
	markFinished(task: ManualTask): void {
		if (!this.finishedSet.has(task)) {
			this.finishedSet.add(task);
			this.finished.push(task);
		}
	}

	private enterHandler(): void {
		this.inHandler++;
		this.maxConcurrentFrames = Math.max(
			this.maxConcurrentFrames,
			this.inHandler,
		);
	}

	private leaveHandler(): void {
		this.inHandler--;
	}
}

export interface FixtureOptions {
	debounceWindowMs?: number;
	debounceHardCapMs?: number;
	busyTextMode?: "queue" | "interrupt";
	hasPendingClarify?: (sessionKey: string) => boolean;
	spawner?: TaskSpawner;
	/** Injected monotonic ms clock (debounce timing). */
	nowMs?: () => number;
	/** Injected timer seam (debounce flush scheduling). */
	scheduleTimer?: (delayMs: number, fn: () => void) => () => void;
	/** Bounded cancel wait override (wedged-owner tests). */
	cancelWaitTimeoutMs?: number;
}

/**
 * Full fixture: guard + deterministic scheduler + recorded turns/replies/
 * warnings + a hold gate for parking handlers mid-turn.
 */
export function makeFixture(options: FixtureOptions = {}): {
	guard: AdapterSessionGuard;
	scheduler: ManualScheduler;
	turns: string[];
	replies: string[];
	warnings: string[];
	readonly maxHandlerConcurrency: number;
	holdTurns(on: boolean): void;
	text(text: string, extra?: Partial<IncomingEvent>): IncomingEvent;
	event(
		extra: Partial<IncomingEvent> & {
			messageType: IncomingEvent["messageType"];
		},
	): IncomingEvent;
} {
	const scheduler = new ManualScheduler();
	const turns: string[] = [];
	const replies: string[] = [];
	const warnings: string[] = [];
	let handlerInFlight = 0;
	let maxHandlerConcurrency = 0;
	let holdTurnsNow = false;
	let releaseHeld: () => void = () => {};
	let heldGate: Promise<void> = Promise.resolve();

	function armHoldGate(): void {
		heldGate = new Promise<void>((resolve) => {
			releaseHeld = resolve;
		});
	}
	armHoldGate();

	async function waitWhileHeld(ctxTask: GatewayTask): Promise<void> {
		while (holdTurnsNow && !ctxTask.cancelRequested()) {
			await Promise.race([
				heldGate.then(() => undefined),
				new Promise<void>((r) => setTimeout(r, 1)),
			]);
		}
	}

	const deps: AdapterGuardDeps = {
		messageHandler: async (event, ctx) => {
			// Concurrency probe counts TURN-PROCESSOR frames only: Lane A/B
			// inline command dispatches legitimately overlap the dying turn
			// (base.py runs them inline while the old task unwinds).
			const isControlDispatch = (event.text ?? "").startsWith("/");
			if (!isControlDispatch) {
				handlerInFlight++;
				maxHandlerConcurrency = Math.max(
					maxHandlerConcurrency,
					handlerInFlight,
				);
			}
			try {
				turns.push(event.text ?? `[${String(event.messageType)}]`);
				await waitWhileHeld(ctx.task);
				ctx.throwIfCancelled();
				return `reply:${event.text ?? String(event.messageType)}`;
			} finally {
				if (!isControlDispatch) handlerInFlight--;
			}
		},
		sendReply: async (_chatId, text) => {
			replies.push(text);
		},
		registry: [
			{
				name: "new",
				aliases: ["reset"],
				busyPolicy: "interrupt_then_dispatch",
				busyHandler: "new",
			},
			{
				name: "stop",
				busyPolicy: "interrupt_then_dispatch",
				busyHandler: "stop",
			},
			{ name: "model", busyPolicy: "reject", busyHandler: "model" },
			{ name: "approve", busyPolicy: "dispatch" },
			{ name: "status", busyPolicy: "dispatch" },
			{ name: "queue", busyPolicy: "dispatch", busyHandler: "queue" },
		],
		spawner: options.spawner ?? scheduler.spawner,
		onWarning: (m) => warnings.push(m),
		debounceWindowMs: options.debounceWindowMs ?? 350,
		debounceHardCapMs: options.debounceHardCapMs ?? 1000,
	};
	if (options.busyTextMode !== undefined) {
		deps.busyTextMode = options.busyTextMode;
	}
	if (options.hasPendingClarify !== undefined) {
		deps.hasPendingClarify = options.hasPendingClarify;
	}
	if (options.nowMs !== undefined) {
		deps.nowMs = options.nowMs;
	}
	if (options.scheduleTimer !== undefined) {
		deps.scheduleTimer = options.scheduleTimer;
	}
	if (options.cancelWaitTimeoutMs !== undefined) {
		deps.cancelWaitTimeoutMs = options.cancelWaitTimeoutMs;
	}
	const guard = new AdapterSessionGuard(deps);

	return {
		guard,
		scheduler,
		turns,
		replies,
		warnings,
		/** Peak simultaneous messageHandler invocations — the exactly-one-turn probe. */
		get maxHandlerConcurrency(): number {
			return maxHandlerConcurrency;
		},
		holdTurns(on: boolean): void {
			// Arm a FRESH gate per hold cycle: racing an already-resolved gate
			// would busy-spin microtasks and starve the event loop.
			if (on && !holdTurnsNow) armHoldGate();
			holdTurnsNow = on;
			if (!on) releaseHeld();
		},
		text(text: string, extra: Partial<IncomingEvent> = {}): IncomingEvent {
			return {
				messageType: "text",
				text,
				// Real platform arrivals ALWAYS carry a source snapshot; the
				// debounce merge predicate treats source-less events as
				// unmergeable (parity of _can_merge_text_debounce_events).
				source: {
					platform: "telegram",
					chatType: "dm",
					userId: "u1",
					chatId: "100",
				},
				...extra,
			};
		},
		event(extra) {
			return { ...extra } as IncomingEvent;
		},
	};
}

// Deterministic fake timers for debounce tests.
export class FakeTimers {
	nowVal = 0;
	private timers: Array<{ at: number; fn: () => void }> = [];

	readonly nowMs = (): number => this.nowVal;

	readonly scheduleTimer = (delayMs: number, fn: () => void): (() => void) => {
		const entry = { at: this.nowVal + delayMs, fn };
		this.timers.push(entry);
		return () => {
			const idx = this.timers.indexOf(entry);
			if (idx >= 0) this.timers.splice(idx, 1);
		};
	};

	get pendingCount(): number {
		return this.timers.length;
	}

	fireAllDue(): void {
		const due = this.timers.filter((t) => t.at <= this.nowVal);
		for (const t of due) {
			const idx = this.timers.indexOf(t);
			if (idx >= 0) this.timers.splice(idx, 1);
			t.fn();
		}
	}

	advance(ms: number): void {
		this.nowVal += ms;
		this.fireAllDue();
	}
}
