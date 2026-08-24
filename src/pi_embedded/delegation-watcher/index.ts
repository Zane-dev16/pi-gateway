// pi_embedded/delegation-watcher — §7.2 re-entry protocol (DEC-018, Phase 5
// scope item 5). Completes the rail whose STORE side landed in Phase 4:
// idle-gated re-entry of durable async-delegation completions as NEW forged
// turns through the normal pipeline, with fail-closed ownership resolution
// (user-boundary drop / lineage-tip / idle-end retarget).

export { systemClock, type GatewayClock } from "./clock.js";
export {
	turnLeaseLiveness,
	type TurnLeaseHeldView,
	type TurnLiveness,
} from "./idle-gate.js";
export {
	delegationAttributionLine,
	formatAge,
	formatAsyncDelegation,
	formatCoalescedAsyncDelegations,
	formatCompletionNotification,
} from "./formatter.js";
export { listPendingCompletions, type PendingCompletion } from "./pending.js";
export {
	SessionOwnershipResolver,
	type OwnershipResolution,
	type OwnershipResolverOptions,
	type ResolvedSessionFacts,
} from "./ownership-resolver.js";
export {
	CompletionDeliveryEngine,
	forgeCompletionEvent,
	type CompletionDeliveryDeps,
	type DeliveryLogger,
	type GroupDeliveryReport,
	type GroupDisposition,
	type SyntheticTurnDispatcher,
} from "./delivery.js";
export {
	DELEGATION_POLL_INTERVAL_MS,
	DELEGATION_WATCHER_STARTUP_DELAY_MS,
	DelegationWatcher,
	type DelegationTickReport,
	type DelegationWatcherDeps,
} from "./watcher.js";
