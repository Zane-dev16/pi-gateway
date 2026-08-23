// pi_gateway/lifecycle/status-persist.ts — exit-status persistence rules.
//
// Spec: /root/pi-gateway/08-operations.md §1.2 (#42675): an UNEXPECTED
// external signal must NOT persist gateway_state="stopped", or container boot
// refuses to auto-start the gateway; planned stops DO persist "stopped";
// planned takeover also lands as a clean stop (the replacer immediately
// overwrites the file with its own running stamp at its stage 10).
// Hermes anchor: gateway/run.py:_stop_impl suppresses the stopped persist when
// _signal_initiated_shutdown is set; run.py:shutdown_signal_handler mirrors the
// flag onto the runner BEFORE teardown begins.

import {
	writeRuntimeStatus,
	type RuntimeStatusRecord,
} from "./status-stamp.js";
import {
	SHUTDOWN_EXIT_CODES,
	persistsStopped,
	type DrainOutcome,
} from "./shutdown.js";
import type { StatusIdentity } from "./status-stamp.js";

export function persistExitStatusPatch(
	home: string,
	outcome: DrainOutcome,
	identity: StatusIdentity,
): RuntimeStatusRecord {
	if (persistsStopped(outcome.klass)) {
		return writeRuntimeStatus(
			home,
			{
				gateway_state: "stopped",
				exit_reason:
					outcome.klass === "takeover" ? "planned_takeover" : outcome.klass,
			},
			identity,
		);
	}
	// Unexpected signal: patch ONLY the exit reason. The gateway_state field is
	// left untouched so a stale "running" never reads as a clean stop (#42675).
	return writeRuntimeStatus(
		home,
		{
			exit_reason: `unexpected_signal:${SHUTDOWN_EXIT_CODES.unexpected_signal}`,
		},
		identity,
	);
}

export { persistsStopped, SHUTDOWN_EXIT_CODES };
export type { DrainOutcome, StatusIdentity };
