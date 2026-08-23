// obligations — public surface barrel (delivery-obligations ledger, DEC-007).
//
// Layer rank 3 (pi_gateway): imports pi_state freely, never upward into
// pi_agent_core. Hermes anchors: gateway/delivery_ledger.py (engine),
// gateway/run.py:_redeliver_pending_obligations (recovery driver semantics).

export {
	systemClock,
	type GatewayClock,
} from "./clock.js";
export {
	RECOVERED_MARKER,
	composeDeliveryContent,
	normalizeSendFailure,
	type DeliveryOutcome,
	type DeliveryRequest,
	type DeliverySender,
} from "./sender.js";
export {
	MAX_ATTEMPTS,
	MAX_ROWS,
	RETRY_BASE_SECONDS,
	RETRY_GROWTH_FACTOR,
	RETRY_MAX_SECONDS,
	STALE_AFTER_SECONDS,
	LAST_ERROR_MAX_CHARS,
	OBLIGATION_STATES,
	IllegalTransitionError,
	ObligationLedgerError,
	ObligationNotFoundError,
	DeliveryLedger,
	computeObligationId,
	nextRetryDelaySeconds,
	readProcessStartTime,
	type ClaimedObligation,
	type DeliveryLedgerOptions,
	type InlineDeliveryReport,
	type NewObligation,
	type NowOptions,
	type ObligationState,
	type OwnerStamp,
	type SettleReport,
	type SweepOptions,
} from "./ledger.js";
export { obligationHealthSnapshot, type ObligationsHealth } from "./health.js";
export {
	DEFAULT_TICK_INTERVAL_SECONDS,
	ObligationRetryScheduler,
	type RetrySchedulerOptions,
	type SchedulerTickReport,
} from "./scheduler.js";
