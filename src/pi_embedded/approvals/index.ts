// pi_embedded/approvals — the exec-approval bridge over messaging
// (07-integrations.md §8). Layer rank 4. A dangerous-command approval inside
// an agent turn blocks the turn exactly like the CLI's synchronous input(),
// while the human answers on whatever chat surface started the session.
//
// THE INVARIANT SPLIT: ONE resolution primitive (ApprovalQueues.resolve)
// beneath ALL answer surfaces — inline buttons, /approve, /deny, bare words,
// api request_id targeting — with FIFO + targeting + coalescing queue
// semantics; card-first/text-fallback delivery whose send classifier treats
// ambiguous sends as possibly-delivered (never re-sent); fail-closed
// timeouts; honest stale-tap rendering.

export { systemClock, type GatewayClock } from "./clock.js";

export {
	ApprovalEntry,
	ApprovalQueues,
	APPROVAL_CHOICES,
	type ApprovalChoice,
	type ApprovalRequestData,
	type NormalizedApprovalData,
	type NotifyCallback,
	type ResolveOptions,
	isApprovalChoice,
	normalizeApprovalData,
} from "./queue.js";

export {
	HUMAN_WAIT_MARGIN_S,
	HumanWaitAccounting,
	clampedWindowSeconds,
	humanWaitCeiling,
} from "./human-wait.js";

export {
	GATEWAY_SECRET_PATTERNS,
	redactApprovalCommand,
	type PrimarySecretScrub,
} from "./redact.js";

export {
	ALREADY_RESOLVED_TEXT,
	APPROVAL_EXPIRED,
	APPROVE_NO_PENDING,
	DENY_NO_PENDING,
	DENY_STALE,
	EXPIRED_EDIT_TEXT,
	EXPIRED_LABEL,
	FALLBACK_HEADING,
	INVALID_CALLBACK_TEXT,
	NOT_AUTHORIZED_TEXT,
	OUTCOME_LABELS,
	SMART_DENIED_HEADING,
	approveConfirmation,
	buildApprovalKeyboard,
	clickOutcomeLabel,
	denyConfirmation,
	execApprovalCallbackData,
	formatExecApprovalFallback,
	parseApprovalCallbackData,
	type CardButton,
	type ParsedApprovalCallback,
} from "./render.js";

export {
	ApprovalCardLedger,
	DeliveryBridge,
	SEND_CLASSIFY_TIMEOUT_MS,
	classifySendOutcome,
	hasExecApprovalCard,
	type ApprovalSendResult,
	type DeliveryBridgeDeps,
	type DeliveryTarget,
	type ExecApprovalSendArgs,
	type NotifyRequest,
	type SendOutcome,
} from "./delivery.js";

export {
	ACTIVITY_HEARTBEAT_INTERVAL_S,
	DEFAULT_APPROVAL_TIMEOUT_SECONDS,
	WAIT_SLICE_MS,
	ActivityHeartbeat,
	awaitCoalescedLeader,
	awaitGatewayDecision,
	cronDenyReason,
	fireApprovalHook,
	normalizeCronApprovalMode,
	type ApprovalHookPayload,
	type ApprovalHooks,
	type AwaitDecisionDeps,
	type AwaitDecisionOptions,
	type AwaitDecisionRequest,
	type CronApprovalMode,
	type DecisionResult,
	type ObserverEmit,
	type SessionNotify,
} from "./gate.js";

export {
	AnswerRejected,
	BARE_APPROVE_WORDS,
	BARE_DENY_WORDS,
	bindingMatches,
	classifyBareWord,
	handleApprovalClick,
	handleApprove,
	handleDeny,
	parseApproveArgs,
	parseDenyArgs,
	resolveByRequestId,
	routeBareAnswer,
	type AnswerRejectionCode,
	type AnswerSource,
	type BareRouteResult,
	type BareWordRoute,
	type ClickOutcome,
	type ClickResult,
	type IsAnswerAuthorized,
	type ParsedApproveArgs,
	type ParsedDenyArgs,
	type SlashAnswerResult,
	type SurfaceDeps,
} from "./surfaces.js";

export {
	ExecApprovalBridge,
	type ApprovalBridgeOptions,
	type RegisterSessionOptions,
} from "./bridge.js";
