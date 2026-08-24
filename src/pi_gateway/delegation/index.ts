// pi_gateway/delegation — async-delegation DURABLE RAIL (06 §7 store side;
// DEC-018). Claim/release/complete handshake, restore-undelivered-on-boot,
// 48 h replay-age prune + durable retention GC, and the §7.2 ownership
// decision-table groundwork (verdicts + user-boundary drop set AS DATA).
// Watcher wiring and idle-gated re-entry are Phase 5.

export { systemClock, type GatewayClock } from "./clock.js";
export {
	ACTIVE_DISPATCH_STATES,
	CLAIM_STALE_SECONDS,
	COMPLETION_REPLAY_AGE_SECONDS,
	DelegationNotFoundError,
	DelegationRail,
	type DelegationRailError,
	type DelegationRailOptions,
	type DelegationRow,
	DURABLE_RETENTION_SECONDS,
	MAX_DELIVERY_ATTEMPTS,
	MAX_DURABLE_PENDING,
	MAX_RETAINED_TERMINAL,
	type CompletionPublishInput,
	type DeliveryState,
	type DispatchInput,
	type NowOptions,
	type OwnerStamp,
	readProcessStartTime,
} from "./rail.js";
export {
	classifyCompletionTarget,
	dispositionFor,
	isUserBoundaryEnd,
	type CompletionVerdict,
	type LineageTip,
	type ParentSnapshot,
	type StoreDisposition,
	USER_BOUNDARY_END_REASONS,
	type UserBoundaryEndReason,
} from "./ownership.js";
