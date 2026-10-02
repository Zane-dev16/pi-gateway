// pi_gateway/commands/idle-executors.ts — idle-path executor table (DEC-078).
//
// Data shape first: a ReadonlyMap from CANONICAL command name to its idle
// executor. The table owns exactly the commands with a REAL local execution
// on the cold path; absence means passthrough — the original message bytes
// flow into the normal turn untouched (today's behavior, and the 07 §1.4
// unknown-as-text rule for names with no executor).
//
// Real today: /help renders gateway help lines derived from the live
// registry rows (pi names, no Hermes-only rows); /compact runs the REAL host
// AgentSession.compact() through the runner's cached host session (the seam
// blocker 1 asked for). Model/export stay passthrough: AgentSession.setModel
// needs runner-owned model resolution + per-session override (no seam), and
// session-export.ts:exportSessionToJsonl is not index-reachable and writes
// files (no gateway path policy). The /new + /resume + /switch-path +
// /new-path repoints live one layer up (guard-wiring switch executors over
// the binder); this table stays the passthrough default for everything else.
// A passthrough is never an error.

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
	/**
	 * Manual compaction over the runner's cached host session
	 * (runner.ts:GatewayAgentRunner.compactSession). Optional — runners
	 * without a host-session cache omit it and /compact stays passthrough.
	 */
	compactSession?:
		| ((
				sessionId: string,
				customInstructions?: string,
			) => Promise<{ summary: string; tokensBefore: number }>)
		| undefined;
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
	/**
	 * Binder-resolved host session id (guard-wiring driveSessionId).
	 * Absent ⇒ sessionKey (the pre-hop path where they are identical).
	 */
	hostSessionId?: string | undefined;
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
 * /compact: the REAL host compaction over the runner's cached host session.
 * Custom instructions ride on the args tail ("/compact focus on the API");
 * a runner without the seam stays passthrough on original bytes. A host
 * refusal (session too small) renders as reply text — never a throw, never
 * silence — so the user sees why nothing compacted and no turn is consumed.
 */
export async function compactIdleExecutor(
	ctx: IdleExecutorContext,
): Promise<IdleExecutorResult> {
	const runner = ctx.runner;
	if (runner.compactSession === undefined) {
		return { kind: "passthrough", text: ctx.eventText };
	}
	const sessionId = ctx.hostSessionId ?? ctx.sessionKey;
	const instructions = ctx.args.trim();
	try {
		const result =
			instructions === ""
				? await runner.compactSession(sessionId)
				: await runner.compactSession(sessionId, instructions);
		return {
			kind: "reply",
			text: `Compacted ${result.tokensBefore} tokens of context.\n\n${result.summary}`,
		};
	} catch (err) {
		const reason = err instanceof Error ? err.message : String(err);
		return { kind: "reply", text: `Compaction failed: ${reason}` };
	}
}

/**
 * The idle executor table over live rows. /help answers locally; /compact
 * compacts through the runner seam; every other known command falls through
 * to the model turn on its original bytes until its real seam lands.
 */
export function buildIdleExecutors(
	_rows: readonly CommandDef[],
): ReadonlyMap<string, IdleExecutor> {
	return new Map<string, IdleExecutor>([
		["help", helpIdleExecutor],
		["compact", compactIdleExecutor],
	]);
}
