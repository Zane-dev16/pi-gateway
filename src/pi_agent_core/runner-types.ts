// pi_agent_core/runner-types.ts — shared type vocabulary for the runner loop.
// Kept separate so leaf modules (conversation-state, memory-turns, budget)
// can reference these without importing the runner implementation.

/** Exit reasons are recorded, never silent (05 §4.1). */
export type TurnExitReason =
	/** Model produced the authoritative final text. */
	| "finalized"
	/** Interrupt requested; host loop stopped at the next boundary. */
	| "interrupted_by_user"
	/** Iteration budget + the single grace call both consumed. */
	| "budget_exhausted"
	/** Assistant turn ended with an error message. */
	| "error";

export interface TurnUsageSnapshot {
	inputTokens: number;
	outputTokens: number;
	cacheReadTokens: number;
	cacheWriteTokens: number;
	totalTokens: number;
}

export interface TurnOutcome {
	exitReason: TurnExitReason;
	/** Authoritative final payload (joined assistant text blocks). */
	finalText: string;
	/** Model calls consumed by this turn (1 = no tool iterations). */
	iterations: number;
	/** Repairs applied by the pre-request alternation pass. */
	repairs: number;
	userRowId: number | null;
	assistantRowId: number | null;
	errorMessage?: string;
	usage: TurnUsageSnapshot | null;
}
