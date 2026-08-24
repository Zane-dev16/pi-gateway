// stage-entry.ts — embedded kanban dispatcher's OPTIONAL-STAGE entry
// (01 §3.1 stage 8 `embedded_watchers` slot; DEC-040 registration wiring).
//
// Maps startKanbanDispatcher's native ServiceStartResult onto the shared
// EmbeddedServiceOutcome vocabulary, preserving ALL THREE classifications:
//   ok=true                        ⇒ { ok:true } + stoppable loop handle
//   ok=false + degraded=true       ⇒ loud per-service DEGRADE (board refusal…)
//   ok=false + degraded=false      ⇒ disabled/skipped (env gate off, singleton
//                                    role held elsewhere) — loud, NOT a failure
// Layering: lifecycle shape mirrored via ../service-entry.js.

import {
	startKanbanDispatcher,
	type StartKanbanDispatcherOptions,
} from "./service.js";
import type { EmbeddedServiceEntry } from "../service-entry.js";

/** Registration name of the kanban dispatcher service entry. */
export const KANBAN_DISPATCHER_SERVICE_NAME = "kanban-dispatcher";

export interface KanbanDispatcherEntryDeps
	extends StartKanbanDispatcherOptions {
	/** Log line sink forwarded to the service (default: console.error). */
	logLines?: (line: string) => void;
}

export function kanbanDispatcherServiceEntry(
	deps: KanbanDispatcherEntryDeps,
): EmbeddedServiceEntry {
	const { logLines, ...opts } = deps;
	const log = logLines ?? ((line: string) => console.error(line));
	return {
		name: KANBAN_DISPATCHER_SERVICE_NAME,
		async start() {
			const { result, running } = await startKanbanDispatcher(opts, log);
			if (result.ok) {
				return running !== undefined
					? {
							ok: true,
							handle: {
								name: KANBAN_DISPATCHER_SERVICE_NAME,
								stop: () => running.stop(),
							},
						}
					: { ok: true };
			}
			if (result.degraded) {
				return {
					ok: false,
					degraded: true,
					reason: result.reason ?? "kanban dispatcher degraded",
				};
			}
			return {
				ok: false,
				reason: result.reason ?? "kanban dispatcher not started",
			};
		},
	};
}
