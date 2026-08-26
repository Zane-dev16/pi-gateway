// stage-entry.ts — cron ticker's OPTIONAL-STAGE entry (01 §3.1 stage 7
// `cron_scheduler`; DEC-040 registration wiring).
//
// Maps startCronTickerOrDegraded's native CronStartupResult onto the shared
// EmbeddedServiceOutcome vocabulary: success ⇒ stoppable handle; failure ⇒
// LOUD per-service degrade (the ticker has no "disabled" state — registering
// it means running it). Layering: the lifecycle entry shape is mirrored via
// ../service-entry.js; pi_embedded never imports pi_gateway/lifecycle.

import {
	startCronTickerOrDegraded,
	type CronServiceHandle,
	type StartCronTickerOptions,
} from "./scheduler.js";
import type {
	EmbeddedServiceEntry,
	EmbeddedServiceHandle,
} from "../service-entry.js";

/** Registration name of the cron ticker service entry. */
export const CRON_TICKER_SERVICE_NAME = "cron-ticker";

function toEmbeddedHandle(handle: CronServiceHandle): EmbeddedServiceHandle {
	return {
		name: handle.name,
		stop: () => handle.stop(),
		// Drain input (#60432/#82161): live in-flight run count for the
		// lifecycle's own-budget cron wait.
		inflightCount: () => handle.inflightCount(),
	};
}

/**
 * Entry factory: bind the ticker's construction deps ONCE; start() runs at
 * stage 7 and returns the classified outcome. startCronTickerOrDegraded
 * already contains construct/start throws into
 * `{ ok:false, degraded:true }` — the exact per-service degrade contract.
 */
export function cronTickerServiceEntry(
	deps: StartCronTickerOptions,
): EmbeddedServiceEntry {
	return {
		name: CRON_TICKER_SERVICE_NAME,
		start() {
			const result = startCronTickerOrDegraded(deps);
			if (result.ok) {
				return result.handle !== undefined
					? { ok: true, handle: toEmbeddedHandle(result.handle) }
					: { ok: true };
			}
			return {
				ok: false,
				degraded: true,
				reason: result.degradedReason ?? "cron ticker failed to start",
			};
		},
	};
}
