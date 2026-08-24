// pi_platforms/kit/lifecycle-state — adapter fatal-error state machine and the
// LOUD disable path (04 §7 error-handling table; §4.2 "Missing secrets disable
// the adapter LOUDLY (visible in /status), never silently").
//
// Ported from the READ-ONLY Hermes reference, semantics only:
//   gateway/platforms/base.py:_set_fatal_error (fatal state stamps status;
//     detached strong-ref'd handler decides restart)
//   plugin.yaml requires_env enablement (missing secret ⇒ loud disable)

import type { StreamLogger } from "../../pi_gateway/streaming/adapter-seam.js";

export type AdapterRunState = "active" | "degraded" | "disabled" | "fatal";

/** Structured disable reason — surfaced verbatim in /status. */
export type DisableReason =
	| { kind: "secret_missing"; secretKey: string; manifestName: string }
	| {
			kind: "token_lock_conflict";
			scope: string;
			credentialId: string;
			holder: string;
	  }
	| { kind: "config_invalid"; detail: string }
	| { kind: "manual"; detail: string };

export class AdapterDisabledError extends Error {
	readonly reason: DisableReason;
	constructor(reason: DisableReason) {
		super(`adapter disabled (${reason.kind}): ${describeReason(reason)}`);
		this.name = "AdapterDisabledError";
		this.reason = reason;
	}
}

/** Named-holder refusal surfaced as a FATAL adapter error (06 §5 shape). */
export class TokenLockConflictError extends AdapterDisabledError {
	constructor(scope: string, credentialId: string, holder: string) {
		super({ kind: "token_lock_conflict", scope, credentialId, holder });
		this.name = "TokenLockConflictError";
	}
}

export function describeReason(reason: DisableReason): string {
	switch (reason.kind) {
		case "secret_missing":
			return `required secret "${reason.secretKey}" is not configured for ${reason.manifestName}`;
		case "token_lock_conflict":
			return `credential ${reason.credentialId} in scope ${reason.scope} is held by another instance (${reason.holder})`;
		case "config_invalid":
			return reason.detail;
		case "manual":
			return reason.detail;
	}
}

/** One /status-shaped snapshot line per adapter. */
export interface AdapterStatusSnapshot {
	state: AdapterRunState;
	reason?: DisableReason | undefined;
	detail: string;
}

/**
 * The fatal/disable state machine. Transitions:
 *   active → degraded   (non-fatal capability loss, e.g. rich latch)
 *   active → disabled   (loud: secret missing, manual)
 *   active → fatal      (transport death, token-lock conflict)
 *   disabled/fatal are TERMINAL for the process lifetime — re-enable is a
 *   restart decision owned by the runner's reconnect watcher.
 */
export class AdapterLifecycleState {
	private _state: AdapterRunState = "active";
	private _reason: DisableReason | undefined;
	private readonly listeners: Array<(s: AdapterStatusSnapshot) => void> = [];

	constructor(private readonly log?: StreamLogger | undefined) {}

	get state(): AdapterRunState {
		return this._state;
	}

	get reason(): DisableReason | undefined {
		return this._reason;
	}

	get isActive(): boolean {
		return this._state === "active";
	}

	onTransition(listener: (s: AdapterStatusSnapshot) => void): void {
		this.listeners.push(listener);
	}

	/** Loud disable — ALWAYS logs; visible in /status; throws nothing. */
	disable(reason: DisableReason): void {
		if (this._state === "disabled" || this._state === "fatal") return;
		this._state = "disabled";
		this._reason = reason;
		// LOUD: never a silent skip.
		this.log?.error?.(`ADAPTER DISABLED: ${describeReason(reason)}`, {
			kind: reason.kind,
		});
		this.emit();
	}

	markFatal(reason: DisableReason): void {
		if (this._state === "disabled" || this._state === "fatal") return;
		this._state = "fatal";
		this._reason = reason;
		this.log?.error?.(`ADAPTER FATAL: ${describeReason(reason)}`, {
			kind: reason.kind,
		});
		this.emit();
	}

	markDegraded(detail: string): void {
		if (this._state !== "active") return;
		this._state = "degraded";
		this.log?.warn?.(`adapter degraded: ${detail}`);
		this.emit();
	}

	statusSnapshot(): AdapterStatusSnapshot {
		return {
			state: this._state,
			reason: this._reason,
			detail: this._reason ? describeReason(this._reason) : "",
		};
	}

	private emit(): void {
		const snapshot = this.statusSnapshot();
		for (const l of this.listeners) l(snapshot);
	}
}
