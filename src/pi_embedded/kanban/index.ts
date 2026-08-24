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
