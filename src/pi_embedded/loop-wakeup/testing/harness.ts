// TEST INFRASTRUCTURE — shared harness for /loop behavior contracts.
//
// Each test gets an isolated mkdtemp StateStore (production open path), a
// ManualClock-driven manager/watcher pair, and a RECORDING dispatcher standing
// in for the L1-guard composition. No wall-clock reads, no real child
// processes. The store-level durability claims are contracted in
// src/pi_state/loops.test.ts; these suites drive the orchestration layer.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { IncomingEvent } from "../../../pi_gateway/guards/index.js";
import { StateStore } from "../../../pi_state/index.js";
import { ManualClock } from "./manual-clock.js";
import type { LoopWatcherDeps, SyntheticTurnDispatcher } from "../watcher.js";
import { LoopWakeupWatcher } from "../watcher.js";

/** Recording dispatcher: captures forged events; can fail on command. */
export class RecordingDispatcher implements SyntheticTurnDispatcher {
	readonly events: IncomingEvent[] = [];
	private failNext: Error | null = null;

	failOnceWith(err: Error): void {
		this.failNext = err;
	}

	async dispatch(event: IncomingEvent): Promise<void> {
		const fail = this.failNext;
		this.failNext = null;
		if (fail) throw fail;
		this.events.push(event);
	}

	texts(): string[] {
		return this.events.map((e) => e.text ?? "");
	}
}

export interface LoopHarness {
	dir: string;
	dbPath: string;
	store: StateStore;
	clock: ManualClock;
	dispatcher: RecordingDispatcher;
	watcher: LoopWakeupWatcher;
	close: () => Promise<void>;
}

export async function openLoopHarness(
	label = "loop-wakeup",
	deps: Partial<LoopWatcherDeps> = {},
	opts: { clockStartSeconds?: number } = {},
): Promise<LoopHarness> {
	const dir = mkdtempSync(join(tmpdir(), `pi-gw-${label}-`));
	const dbPath = join(dir, "state.db");
	const clock = new ManualClock();
	clock.setSeconds(opts.clockStartSeconds ?? 1_775_000_000);
	const store = await StateStore.open(dbPath);
	return buildLoopHarnessOn(dir, dbPath, store, clock, label, deps);
}

/**
 * Compose ANOTHER watcher over an ALREADY-OPEN store + clock (deferral
 * variants that need their own seams but must see the same persisted rows).
 */
export function buildLoopHarnessOn(
	dir: string,
	dbPath: string,
	store: StateStore,
	clock: ManualClock,
	label = "loop-wakeup",
	deps: Partial<LoopWatcherDeps> = {},
): LoopHarness {
	const dispatcher = new RecordingDispatcher();
	const watcher = new LoopWakeupWatcher({
		db: store.db,
		dispatcher,
		clock,
		startupDelayMs: 0,
		intervalMs: 1000,
		...deps,
	});
	return {
		dir,
		dbPath,
		store,
		clock,
		dispatcher,
		watcher,
		close: () => store.close(false),
	};
}
