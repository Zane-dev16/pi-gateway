// pi_embedded/loop-wakeup — recurring in-session wakeups, the /loop command.
//
// Persistence half lives in pi_state/loops.ts (state_meta `loop:<session_id>`
// rows); this module owns the orchestration surface (manager.ts) and the
// supervised stage-8 idle wakeup scanner (watcher.ts) that injects due wakeup
// prompts through the NORMAL synthetic-message ingress with busy/goal
// deferrals (run.py:_loop_wakeup_watcher + hermes_cli/loops.py parity).

export { systemClock, type GatewayClock } from "./clock.js";
export {
	DEFAULT_MAX_TICKS,
	DEFAULT_MIN_INTERVAL_SECONDS,
	DEFAULT_SELF_PACED_CEILING_SECONDS,
	DEFAULT_SELF_PACED_FLOOR_SECONDS,
	JUDGE_UNAVAILABLE_REASON,
	LOOP_BUSY_SET_REJECT_TEXT,
	LOOP_COMPLETE_MARKER,
	LOOP_MIDRUN_CONTROL_ARGS,
	WAKEUP_PROMPT_TEMPLATE,
	WAKEUP_PROMPT_WITH_UNTIL_TEMPLATE,
	digestResponse,
	dispatchLoopCommand,
	formatInterval,
	isLoopMidrunControlArg,
	parseIntervalToken,
	parseLoopArgs,
	resolveMaxTicksDefault,
	resolveMinIntervalSeconds,
	resolveSelfPacedCeilingSeconds,
	resolveSelfPacedFloorSeconds,
	responseSignalsComplete,
	routeFromSource,
	LoopManager,
	LoopValueError,
	type CompleteTickDecision,
	type LoopsConfig,
	type LoopsConfigOf,
	type LoopDispatchResult,
	type LoopManagerDeps,
	type ParsedLoopArgs,
	type UntilJudge,
	type UntilJudgeVerdict,
} from "./manager.js";
export {
	LOOP_WAKEUP_SCAN_INTERVAL_MS,
	LOOP_WAKEUP_STARTUP_DELAY_MS,
	forgeLoopWakeupEvent,
	sourceFromRoute,
	LoopWakeupWatcher,
	type LoopWatcherDeps,
	type LoopWakeupLogger,
	type LoopWakeupTickReport,
	type SyntheticTurnDispatcher,
} from "./watcher.js";
export {
	LOOP_WAKEUP_WATCHER_SERVICE_NAME,
	loopWakeupWatcherServiceEntry,
	type LoopWakeupWatcherEntryInput,
} from "./stage-entry.js";
