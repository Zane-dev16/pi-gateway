// index.ts — public surface of the embedded kanban dispatcher (07 §6).

export {
	DEFAULT_BOARD,
	InvalidBoardSlugError,
	KANBAN_BOARD_ENV,
	normalizeBoardSlug,
	resolveBoardSlug,
	workerBoardEnv,
	BOARD_SLUG_RE,
} from "./board.js";
export type { BoardResolution, BoardResolutionInput } from "./board.js";
export { systemClock } from "./clock.js";
export type { GatewayClock } from "./clock.js";
export {
	DEFAULT_CLAIM_TTL_SECONDS,
	dispatchOnce,
} from "./dispatcher.js";
export type { DispatchOnceOptions } from "./dispatcher.js";
export {
	startKanbanDispatcher,
	resolveDispatcherServiceConfig,
	KANBAN_DISPATCH_IN_GATEWAY_ENV,
} from "./service.js";
export type {
	KanbanDispatcherConfig,
	ServiceStartResult,
	StartKanbanDispatcherOptions,
	RunningKanbanDispatcher,
} from "./service.js";
export { SqliteKanbanBoard } from "./sqlite-board.js";
export {
	normalizeTenant,
	namespacedKey,
	workerTenantEnv,
	KANBAN_TENANT_ENV,
} from "./tenant.js";
export {
	DEFAULT_FAILURE_LIMIT,
	resolveDispatcherConfig,
} from "./types.js";
export type {
	BoardClient,
	CardStatus,
	ClaimRequest,
	DispatchResult,
	FailureOutcome,
	KanbanCard,
	NewCard,
	SpawnFn,
} from "./types.js";
export {
	KANBAN_DISPATCHER_SERVICE_NAME,
	kanbanDispatcherServiceEntry,
	type KanbanDispatcherEntryDeps,
} from "./stage-entry.js";
export {
	MAX_SEND_FAILURES,
	NOTIFY_TERMINAL_KINDS,
	NOTIFIER_GC_INTERVAL_SECONDS,
	SILENT_EVENT_KINDS,
	renderNotifyMessage,
	resolveNotifierServiceConfig,
	runNotifierTick,
	startKanbanNotifier,
	KANBAN_NOTIFY_IN_GATEWAY_ENV,
} from "./notifier.js";
export type {
	NotifyTerminalKind,
	NotifyTickOptions,
	NotifyTickResult,
	NotifierStartResult,
	RunningKanbanNotifier,
	StartKanbanNotifierOptions,
	NotifyDeliverFn,
	KanbanNotifierConfig,
} from "./notifier.js";
export {
	SqliteKanbanNotifyStore,
	subKeyOf,
	DEFAULT_DONE_SUB_RETENTION_DAYS,
} from "./notify-store.js";
export {
	KANBAN_NOTIFIER_SERVICE_NAME,
	kanbanNotifierServiceEntry,
	type KanbanNotifierEntryDeps,
} from "./notifier-stage-entry.js";
export type {
	ClaimedEvents,
	NotifyEvent,
	NotifySubStore,
	NotifySubscription,
	NotifyTaskView,
	SubKey,
} from "./notify-store.js";
