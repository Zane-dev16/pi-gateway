// stage-entry.ts — handoff watcher's OPTIONAL-STAGE entry (01 §3.1 stage 8
// `embedded_watchers` slot; DEC-040 registration wiring).
//
// The watcher module is the poll loop only; THIS shim binds it to the stage:
// the watcher instance is built INSIDE start() so a broken dependency THROWS
// there and the lifecycle engine classifies it as THIS service's loud
// per-service degrade (never blocks siblings). Success ⇒ stoppable handle
// wrapping stop()'s deterministic join. Layering: lifecycle shape mirrored
// via ../service-entry.js.

import type { HandoffWatcher } from "./watcher.js";
import type { EmbeddedServiceEntry } from "../service-entry.js";

/** Registration name of the handoff watcher service entry. */
export const HANDOFF_WATCHER_SERVICE_NAME = "handoff-watcher";

export interface HandoffWatcherEntryInput {
	/**
	 * Builds the watcher (binding queue + DEC-008 pipeline deps) at START
	 * time. A throw here is the engine's classified degrade for THIS service.
	 */
	create: () => HandoffWatcher;
}

export function handoffWatcherServiceEntry(
	input: HandoffWatcherEntryInput,
): EmbeddedServiceEntry {
	return {
		name: HANDOFF_WATCHER_SERVICE_NAME,
		async start() {
			const watcher = input.create();
			watcher.start();
			return {
				ok: true,
				handle: {
					name: HANDOFF_WATCHER_SERVICE_NAME,
					stop: () => watcher.stop(),
				},
			};
		},
	};
}
