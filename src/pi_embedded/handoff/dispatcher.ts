// dispatcher.ts — reference SyntheticTurnDispatcher composition: forge the
// synthetic event through an L1 AdapterSessionGuard (the NORMAL ingress),
// then await frame-chain settlement so the handoff's complete/fail decision
// stays synchronous-error-visible.
//
// WHY NOT adapter.handle_message fire-and-forget (run.py _process_handoff
// comment parity): the watcher must OBSERVE the turn's success or failure to
// mark the row terminal. Hermes achieves that by calling _handle_message
// inline; Pi achieves it by dispatching through L1 (BOTH guards traverse per
// DEC-022/DEC-008) and joining the spawned frames before resolving.
//
// Failure visibility: L1 contains handler throws inside its frame (it sends
// the radio-silence error notice instead of raising). The dispatcher
// therefore consults a composition-supplied failure probe AFTER settlement —
// production wires it to the runner outcome ledger, tests to a recorded
// array. A probe failure REJECTS dispatch ⇒ watcher marks failed(+error).

import type { IncomingEvent } from "../../pi_gateway/guards/events.js";
import type { SyntheticTurnDispatcher } from "./pipeline.js";

/** Narrow structural view of the destination platform's L1 guard. */
export interface GuardDispatchView {
	handleMessage(event: IncomingEvent, sessionKey: string): Promise<void>;
}

/**
 * Composition-supplied settlement + failure observation over the frames its
 * spawner produced:
 *   - awaitIdle resolves when every frame spawned since the last idle point
 *     has settled (drain chains included);
 *   - drainFailures returns and clears handler failures observed by the
 *     composition's messageHandler wrapper.
 */
export interface DispatchSettlementProbe {
	awaitIdle(): Promise<void>;
	drainFailures(): Array<{ sessionKey: string; error: string }>;
}

export class GuardQuiesceDispatcher implements SyntheticTurnDispatcher {
	private readonly guard: GuardDispatchView;
	private readonly probe: DispatchSettlementProbe;

	constructor(guard: GuardDispatchView, probe: DispatchSettlementProbe) {
		this.guard = guard;
		this.probe = probe;
	}

	async dispatch(event: IncomingEvent): Promise<void> {
		// The forged event carries metadata.gateway_session_key; L1 drops
		// mismatched keys loudly, so derive the ingress key from the SAME
		// source of truth (single derivation — no re-key drift).
		const sessionKey = String(
			(event.metadata ?? {})["gateway_session_key"] ?? "",
		);
		if (!sessionKey) {
			throw new Error(
				"handoff synthetic event is missing gateway_session_key metadata",
			);
		}
		await this.guard.handleMessage(event, sessionKey);
		await this.probe.awaitIdle();

		const failures = this.probe.drainFailures();
		const last = failures.at(-1);
		if (last !== undefined) {
			throw new Error(
				`handoff turn failed on ${last.sessionKey}: ${last.error}`,
			);
		}
	}
}
