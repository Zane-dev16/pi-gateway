// pi_gateway/lifecycle/stages.ts — the binding ten-stage startup order.
//
// Spec: /root/pi-gateway/01-architecture.md §3.1 ("Stages (order is binding)";
// parity anchor for the whole sequence: gateway/run.py:start_gateway) and
// /root/pi-gateway/08-operations.md §1.1 (the same sequence condensed to 8
// numbered steps with identical relative order). The TEN-stage enumeration of
// 01 §3.1 is canonical for this skeleton; roadmap Phase 1 item 5 binds it.
//
// Degradation classification (01 §3.1): "Degraded-start is allowed per service
// with a loud log" — that is stages 7 (cron scheduler), 8 (embedded watchers),
// and 9 (platform adapters, where a missing secret ⇒ loud disable per adapter).
// Every other stage is REQUIRED: a failure aborts startup.

export const STAGE_IDS = [
	"profile_override", //   1 — override installed BEFORE any imports (01 §6 step 1)
	"load_config", //        2 — load + validate config
	"boot_fingerprint", //   3 — record boot-code fingerprint (01 §3.3)
	"duplicate_guard", //    4 — duplicate-instance guard / takeover handshake (01 §3.2)
	"runtime_lock", //       5 — claim runtime lock + write PID file
	"open_state_db", //      6 — open state.db → reconcile → FTS version check
	"cron_scheduler", //     7 — resolve + start cron scheduler provider
	"embedded_watchers", //  8 — spawn embedded watchers as supervised tasks
	"platform_adapters", //  9 — instantiate adapters (missing secret ⇒ loud disable)
	"runtime_identity", // 10 — stamp runtime identity into gateway_state.json; READY
] as const;

export type StageId = (typeof STAGE_IDS)[number];

/** Stages whose failure degrades loudly WITHOUT blocking later stages. */
export const OPTIONAL_STAGES: ReadonlySet<StageId> = new Set<StageId>([
	"cron_scheduler",
	"embedded_watchers",
	"platform_adapters",
]);

export function isOptionalStage(id: StageId): boolean {
	return OPTIONAL_STAGES.has(id);
}

export interface StageEvent {
	stage: StageId;
	ok: boolean;
	/** Optional-stage failure recorded as degraded (startup continued). */
	degraded?: boolean;
	error?: string;
	durationMs?: number;
}

export type StartupState =
	| "idle"
	| "starting"
	| "running"
	| "draining"
	| "stopped"
	| "aborted";

export const STARTUP_STATE_TRANSITIONS: ReadonlyArray<
	readonly [StartupState, StartupState[]]
> = [
	["idle", ["starting"]],
	["starting", ["running", "aborted", "draining"]],
	["running", ["draining", "stopped", "aborted"]],
	["draining", ["stopped", "aborted"]],
	// terminal: stopped | aborted
];

export function canTransition(from: StartupState, to: StartupState): boolean {
	const row = STARTUP_STATE_TRANSITIONS.find(([s]) => s === from);
	return row !== undefined && row[1].includes(to);
}
