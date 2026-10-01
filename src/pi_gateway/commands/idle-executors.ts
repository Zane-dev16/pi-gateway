// pi_gateway/commands/idle-executors.ts — idle-path executor table (DEC-078).
//
// Data shape first: a ReadonlyMap from CANONICAL command name to its idle
// executor. The table owns exactly the commands with a REAL local execution
// on the cold path; absence means passthrough — the original message bytes
// flow into the normal turn untouched (today's behavior, and the 07 §1.4
// unknown-as-text rule for names with no executor).
//
// Real today: /help renders gateway help lines derived from the live
// registry rows (pi names, no Hermes-only rows). Host session commands with
// no gateway-reachable seam (/compact's AgentSession.compact, model/export
// TUI handlers) stay passthrough until their seams land — the runner owns
// those sessions and this layer may not reach past its handleTurn edge.
// The /new + /resume + /switch-path + /new-path repoints live one layer up
// (guard-wiring switch executors over the binder); this table stays the
// passthrough default for everything else. A passthrough is never an error.

import type { TurnOutcome } from "../../pi_agent_core/runner-types.js";
import type { CommandDef } from "./command-def.js";
import { gatewayHelpLines } from "./derived.js";

/** Structural turn edge executors consume (guard-wiring ChatTurnRunner). */
export interface IdleTurnRunner {
	handleTurn(request: {
		sessionId: string;
		routingKey: string;
		text: string;
	}): Promise<TurnOutcome>;
}

export interface IdleExecutorContext {
	sessionKey: string;
	/** Dash-repaired args after the command word. */
	args: string;
	/** Live registry rows the reply derives from (help/menus read THESE). */
	rows: readonly CommandDef[];
	runner: IdleTurnRunner;
	/** Original message bytes (passthrough echoes THESE, never a rebuild). */
	eventText: string;
}

export type IdleExecutorResult =
	| { kind: "reply"; text: string | null }
	| { kind: "passthrough"; text: string };

export type IdleExecutor = (
	ctx: IdleExecutorContext,
) => Promise<IdleExecutorResult> | IdleExecutorResult;

/** /help: the registry rendered as reply text. Zero model calls. */
export function helpIdleExecutor(ctx: IdleExecutorContext): IdleExecutorResult {
	return {
		kind: "reply",
		text: gatewayHelpLines(ctx.rows).join("\n"),
	};
}

/**
 * The idle executor table over live rows. Exactly one entry today (/help);
 * every other known command falls through to the model turn on its original
 * bytes until its real seam lands (see module header).
 */
export function buildIdleExecutors(
	_rows: readonly CommandDef[],
): ReadonlyMap<string, IdleExecutor> {
	return new Map<string, IdleExecutor>([["help", helpIdleExecutor]]);
}
