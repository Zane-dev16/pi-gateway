// stage-entry.ts — delegation watcher's OPTIONAL-STAGE entry (01 §3.1 stage 8
// `embedded_watchers` slot; DEC-040 registration wiring).
//
// The watcher module is the poll-and-deliver loop only; THIS shim binds it to
// the stage: the watcher instance is built INSIDE start() so a broken
// dependency THROWS there and the lifecycle engine classifies it as THIS
// service's loud per-service degrade (never blocks siblings). Success ⇒
// stoppable handle wrapping stop()'s deterministic join. Layering: lifecycle
// shape mirrored via ../service-entry.js.

import type { DelegationWatcher } from "./watcher.js";
import type { EmbeddedServiceEntry } from "../service-entry.js";

/** Registration name of the delegation watcher service entry. */
export const DELEGATION_WATCHER_SERVICE_NAME = "delegation-watcher";

export interface DelegationWatcherEntryInput {
	/**
	 * Builds the watcher (binding db/rail/liveness/dispatcher deps) at START
	 * time. A throw here is the engine's classified degrade for THIS service.
	 */
	create: () => DelegationWatcher;
}

export function delegationWatcherServiceEntry(
	input: DelegationWatcherEntryInput,
): EmbeddedServiceEntry {
	return {
		name: DELEGATION_WATCHER_SERVICE_NAME,
		async start() {
			const watcher = input.create();
			watcher.start();
			return {
				ok: true,
				handle: {
					name: DELEGATION_WATCHER_SERVICE_NAME,
					stop: () => watcher.stop(),
				},
			};
		},
	};
}
