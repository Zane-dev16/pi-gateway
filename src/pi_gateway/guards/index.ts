// pi_gateway/guards — the TWO-GUARD module (03-message-routing.md; DEC-004,
// DEC-005, DEC-022). Layer rank 3 (pi_gateway): may import pi_state, never
// upward. Public surface consumed by the runner and platform adapters.

// L1 — adapter-side guard state machine (§2.1, §3, §4, §5 lanes).
export {
	AdapterSessionGuard,
	CANCEL_WAIT_TIMEOUT_MS,
	DEFAULT_DEBOUNCE_HARD_CAP_MS,
	DEFAULT_DEBOUNCE_WINDOW_MS,
	immediateSpawner,
	InterruptEvent,
	TaskCancelledError,
	type AdapterGuardDeps,
	type GatewayTask,
	type MessageHandler,
	type ProcessingOutcome,
	type TaskSpawner,
	type TurnContext,
} from "./l1-adapter-guard.js";

// L1/L2 shared event shapes (§3.1 merge table, debounce identity, intake
// coercion).
export {
	allowsGatewayControl,
	canMergeTextDebounceEvents,
	coercePlaintextGatewayCommand,
	PLAINTEXT_GATEWAY_RESTART_PATTERNS,
	getCommand,
	getCommandArgs,
	isCommand,
	mergeCaption,
	mergePendingEvent,
	type IncomingEvent,
	type MessageType,
	type PendingSlotMap,
	type TextDebounceState,
} from "./events.js";

// L2 — registry-derived busy policy (§2.2, §5.4; DEC-005).
export {
	BUSY_POLICIES,
	BUSY_REJECT_TEXT,
	bypassCommandNames,
	buildCommandLookup,
	catchAllBusyRejectText,
	DEFAULT_BUSY_POLICY,
	effectiveBusyPolicy,
	isInterruptThenDispatch,
	PREGATE_COMMANDS,
	resolveBusyDispatch,
	resolveCommand,
	shouldBypassActiveSession,
	SPECIAL_BUSY_HANDLERS,
	type BusyDispatch,
	type BusyPolicy,
	type CommandDef,
	type CommandRegistry,
} from "./busy-policy.js";

// L2 — runner-side FIFO overflow + busy dispatch execution (#28503, cap 32)
// + staleness eviction (run.py ~17208) + slash-access gate sites.
export {
	AGENT_TIMEOUT_SECONDS_ENV,
	BUSY_QUEUE_MAX_PENDING,
	DEFAULT_AGENT_TIMEOUT_SECONDS,
	RunnerBusyGuard,
	type BusyInputMode,
	isStaleRunningEntry,
	type PendingSlotView,
	type PlainHandler,
	resolveAgentTimeoutSeconds,
	staleRunningAgentWallTtlSeconds,
	type RunnerBusyOptions,
	type SpecialHandler,
	type StaleRunningEntryInputs,
} from "./runner-busy.js";

// L2 — slash-command access control (gap-audit R14; gateway/slash_access.py
// port; byte-stable denial text lives here ONLY).
export {
	ALWAYS_ALLOWED_USER_COMMANDS,
	SLASH_ACCESS_DISABLED,
	type SlashAccessPolicy,
	type SlashAccessSourceLike,
	type SlashAccessGatewayConfigLike,
	isSlashAdmin,
	canRunSlashCommand,
	coerceIdSet,
	coerceCommandSet,
	scopeForChatType,
	keysForScope,
	platformExtraOf,
	policyFromExtra,
	policyForSource,
	slashAccessDenialText,
	checkSlashAccess,
	checkSourceSlashAccess,
} from "./slash-access.js";

// Layer 1 of the turn lease (DEC-004, 03 §7) — in-process registry.
export {
	DEFAULT_LEASE_WAIT_MS,
	DEFAULT_MAX_LEASES,
	SessionTurnLeaseRegistry,
	TurnLeaseTimeoutError,
	TurnLeaseToken,
	type AcquireOptions,
	type RegistryOptions,
} from "./turn-lease.js";
