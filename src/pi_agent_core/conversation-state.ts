// pi_agent_core/conversation-state.ts — DEC-020: session-scoped runner state
// is ONE consolidated ConversationState behind context-local identity
// (AsyncLocalStorage ≙ Python ContextVar), never ad-hoc session_key-keyed
// dicts and never process-global env mutation.
//
// Port parity: gateway/session_state.py:ConversationState +
// gateway/session_state.py:_CONVERSATION_SCOPED_STATE + gateway/session_context.py.
//
// The STRUCTURAL BOUNDARY REGISTRY below enumerates every conversation-scoped
// field. DEC-020's verification clause ("structural test asserting every new
// session-keyed field registers in the boundary registry") is enforced by
// conversationStateShapeViolations(): instance keys ⇄ registry must match
// exactly, so adding a field without registering it fails the suite.

import { AsyncLocalStorage } from "node:async_hooks";

import { TurnCheckpointLedger } from "./checkpoints.js";
import type { TurnExitReason } from "./runner-types.js";

/**
 * The boundary registry: EVERY conversation-scoped field of ConversationState.
 * Adding a field to the class REQUIRES adding it here (same commit) — the
 * structural test enforces the bijection.
 */
export const CONVERSATION_STATE_FIELDS = [
	/** Resolved session id this state belongs to. */
	"sessionId",
	/** Routing key that produced the current turn (diagnostics/telemetry). */
	"routingKey",
	/** Monotonic turn generation for cancellation scoping. */
	"generation",
	/** Exit reason of the last completed turn (null while idle). */
	"exitReason",
	/** Model calls consumed by the current/last turn. */
	"iterations",
	/**
	 * Prefetch-once cache (05 §4.2 `TurnContext.ext_prefetch_cache` analogue):
	 * filled ONCE before the loop, reused across EVERY iteration of the turn.
	 */
	"extPrefetchCache",
	/** Per-turn checkpoint snapshot ledger (checkpoint dedup). */
	"checkpointLedger",
	/** Repairs applied by the last pre-request alternation pass. */
	"repairCount",
	/** Wall-clock ms timestamp of current turn start (injected clock). */
	"turnStartedAt",
] as const;

export type ConversationStateFieldName =
	(typeof CONVERSATION_STATE_FIELDS)[number];

export class ConversationState {
	sessionId: string;
	routingKey: string;
	generation: number;
	exitReason: TurnExitReason | null;
	iterations: number;
	extPrefetchCache: string | null;
	checkpointLedger: TurnCheckpointLedger;
	repairCount: number;
	turnStartedAt: number | null;

	constructor(
		sessionId: string,
		options: {
			routingKey?: string;
			generation?: number;
			checkpointLedger?: TurnCheckpointLedger;
		} = {},
	) {
		this.sessionId = sessionId;
		this.routingKey = options.routingKey ?? "";
		this.generation = options.generation ?? 0;
		this.exitReason = null;
		this.iterations = 0;
		this.extPrefetchCache = null;
		this.checkpointLedger =
			options.checkpointLedger ?? new TurnCheckpointLedger();
		this.repairCount = 0;
		this.turnStartedAt = null;
	}
}

const storage = new AsyncLocalStorage<ConversationState>();

/** Install `state` as the conversation identity for `fn` and everything it spawns. */
export function runWithConversation<T>(
	state: ConversationState,
	fn: () => T,
): T {
	return storage.run(state, fn);
}

/** Current conversation-scoped state, or undefined outside any turn. */
export function currentConversation(): ConversationState | undefined {
	return storage.getStore();
}

/** Current state or throw — for code paths that must run inside a turn. */
export function requireConversation(): ConversationState {
	const state = storage.getStore();
	if (!state) {
		throw new Error(
			"no ConversationState in scope — wrap turn execution in runWithConversation (DEC-020)",
		);
	}
	return state;
}

/**
 * Structural boundary check (DEC-020 verification): every own enumerable
 * property of a fresh instance must be registered, and every registered name
 * must exist on the instance. Returns the violation list (empty = shape holds).
 */
export function conversationStateShapeViolations(): string[] {
	const probe = new ConversationState("probe-session");
	const instanceKeys = new Set(Object.keys(probe));
	const registryKeys = new Set<string>(CONVERSATION_STATE_FIELDS);
	const violations: string[] = [];
	for (const key of instanceKeys) {
		if (!registryKeys.has(key)) {
			violations.push(`unregistered conversation-scoped field: ${key}`);
		}
	}
	for (const key of registryKeys) {
		if (!instanceKeys.has(key)) {
			violations.push(`registry names missing field: ${key}`);
		}
	}
	return violations;
}
