// notifier-stage-entry.ts — embedded kanban notifier's OPTIONAL-STAGE entry
// (01 §3.1 stage 8 `embedded_watchers` slot; DEC-040 registration wiring).
//
// Same three-classification mapping as the dispatcher's stage entry:
//   ok=true                        ⇒ { ok:true } + stoppable loop handle
//   ok=false + degraded=true       ⇒ loud per-service DEGRADE (board refusal,
//                                    store open failure)
//   ok=false + degraded=false      ⇒ disabled/skipped (env gate off, singleton
//                                    role held elsewhere) — loud, NOT a failure
// Layering: lifecycle shape mirrored via ../service-entry.js.

import {
	startKanbanNotifier,
	type StartKanbanNotifierOptions,
} from "./notifier.js";
import type { EmbeddedServiceEntry } from "../service-entry.js";

/** Registration name of the kanban notifier service entry. */
export const KANBAN_NOTIFIER_SERVICE_NAME = "kanban-notifier";

export interface KanbanNotifierEntryDeps extends StartKanbanNotifierOptions {
	/** Log line sink forwarded to the service (default: console.error). */
	logLines?: (line: string) => void;
}

export function kanbanNotifierServiceEntry(
	deps: KanbanNotifierEntryDeps,
): EmbeddedServiceEntry {
	const { logLines, ...opts } = deps;
	const log = logLines ?? ((line: string) => console.error(line));
	return {
		name: KANBAN_NOTIFIER_SERVICE_NAME,
		async start() {
			const { result, running } = await startKanbanNotifier(opts, log);
			if (result.ok) {
				return running !== undefined
					? {
							ok: true,
							handle: {
								name: KANBAN_NOTIFIER_SERVICE_NAME,
								stop: () => running.stop(),
							},
						}
					: { ok: true };
			}
			if (result.degraded) {
				return {
					ok: false,
					degraded: true,
					reason: result.reason ?? "kanban notifier degraded",
				};
			}
			return {
				ok: false,
				reason: result.reason ?? "kanban notifier not started",
			};
		},
	};
}
