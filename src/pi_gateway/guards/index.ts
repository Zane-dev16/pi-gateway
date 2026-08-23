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

// L1/L2 shared event shapes (§3.1 merge table, debounce identity).
export {
	allowsGatewayControl,
	canMergeTextDebounceEvents,
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

// L2 — runner-side FIFO overflow + busy dispatch execution (#28503, cap 32).
export {
	BUSY_QUEUE_MAX_PENDING,
	RunnerBusyGuard,
	type BusyInputMode,
	type PendingSlotView,
	type PlainHandler,
	type RunnerBusyOptions,
	type SpecialHandler,
} from "./runner-busy.js";

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
