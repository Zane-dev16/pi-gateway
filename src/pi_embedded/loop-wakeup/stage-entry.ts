// stage-entry.ts — loop-wakeup watcher's OPTIONAL-STAGE entry (01 §3.1 stage 8
// `embedded_watchers` slot; DEC-040 registration wiring).
//
// The watcher module is the scan-and-inject loop only; THIS shim binds it to
// the stage: the watcher instance is built INSIDE start() so a broken
// dependency THROWS there and the lifecycle engine classifies it as THIS
// service's loud per-service degrade (never blocks siblings). Success ⇒
// stoppable handle wrapping stop()'s deterministic join. Layering: lifecycle
// shape mirrored via ../service-entry.js (delegation-watcher/stage-entry.ts
// is the sibling precedent).

import type { LoopWakeupWatcher } from "./watcher.js";
import type { EmbeddedServiceEntry } from "../service-entry.js";

/** Registration name of the loop-wakeup watcher service entry. */
export const LOOP_WAKEUP_WATCHER_SERVICE_NAME = "loop-wakeup-watcher";

export interface LoopWakeupWatcherEntryInput {
	/**
	 * Builds the watcher (binding db/dispatcher/busy+goal seams) at START time.
	 * A throw here is the engine's classified degrade for THIS service.
	 */
	create: () => LoopWakeupWatcher;
}

export function loopWakeupWatcherServiceEntry(
	input: LoopWakeupWatcherEntryInput,
): EmbeddedServiceEntry {
	return {
		name: LOOP_WAKEUP_WATCHER_SERVICE_NAME,
		async start() {
			const watcher = input.create();
			watcher.start();
			return {
				ok: true,
				handle: {
					name: LOOP_WAKEUP_WATCHER_SERVICE_NAME,
					stop: () => watcher.stop(),
				},
			};
		},
	};
}
