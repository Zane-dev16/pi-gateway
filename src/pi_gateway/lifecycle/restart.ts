// pi_gateway/lifecycle/restart.ts — supervisor restart contract: exit codes,
// supervisor/container detection, drain-budget clamps.
//
// Spec: /root/pi-gateway/08-operations.md §1.3 (loop-liveness hard-exit exits
// 75 "service-restart code so supervisors recycle instead of giving up"),
// §2 supervision parity; 01-architecture.md §3.4. Hermes anchors (READ-ONLY
// reference; semantics ported, no code vendored — gateway/restart.py):
//   GATEWAY_SERVICE_RESTART_EXIT_CODE (=75, EX_TEMPFAIL) → SERVICE_RESTART_EXIT_CODE
//   GATEWAY_FATAL_CONFIG_EXIT_CODE   (=78, EX_CONFIG; s6
//     RestartPreventExitStatus)                       → FATAL_CONFIG_EXIT_CODE
//   is_gateway_supervisor_process    → isGatewaySupervisorProcess
//     (systemd INVOCATION_ID; s6 supervised-child env; launchd XPC_SERVICE_NAME
//     ≠ "0"; external-supervisor env truthy {1,true,yes,on})
//   is_container_restart_context     → isContainerRestartContext
//     (/ .dockerenv | /run/.containerenv ⇒ detached respawn dies with the
//     cgroup, exit-75 service restart is the only viable path)
//   resolve_cron_drain_budget        → resolveCronDrainBudget
//     (cron floor clamped to the watchdog leash minus CRON_DRAIN_CLEANUP_RESERVE_S=10s;
//     never below drain_timeout — the floor only ever extends the wait)
//   resolve_restart_exit_wait_budget → resolveRestartExitWaitBudget
//     (drain + after-turn wait + 15s headroom for CLI hard-kill fallbacks)
//
// Environment-variable names are the pi-side spellings of the Hermes contract:
// PI_S6_SUPERVISED_CHILD and PI_GATEWAY_EXTERNAL_SUPERVISOR (proposed DEC text
// in the phase report). INVOCATION_ID / XPC_SERVICE_NAME are the managers' own
// variables and are read as-is.

import { existsSync } from "node:fs";

/** EX_TEMPFAIL — ask the service manager to restart us after a graceful drain. */
export const SERVICE_RESTART_EXIT_CODE = 75;

/** EX_CONFIG — fatal configuration error; supervisors must STOP restarting. */
export const FATAL_CONFIG_EXIT_CODE = 78;

/** LifecycleError reasonCode that classifies a startup abort as fatal config. */
export const FATAL_CONFIG_REASON_CODE = "fatal_config";

/** Set by wrappers that supervise this gateway outside systemd/launchd/s6. */
export const EXTERNAL_GATEWAY_SUPERVISOR_ENV = "PI_GATEWAY_EXTERNAL_SUPERVISOR";
/** s6 overlay marker on the supervised gateway child. */
export const S6_SUPERVISED_CHILD_ENV = "PI_S6_SUPERVISED_CHILD";

const TRUTHY = new Set(["1", "true", "yes", "on"]);

export type ProcessEnviron = Readonly<Record<string, string | undefined>>;

/**
 * Is THIS process owned by a service supervisor? Parity of
 * restart.py:is_gateway_supervisor_process — systemd (INVOCATION_ID), s6
 * (supervised-child env), launchd (XPC_SERVICE_NAME set and not "0"), or the
 * explicit external-supervisor env (survives `env -i` wrappers by design).
 */
export function isGatewaySupervisorProcess(
	environ: ProcessEnviron = process.env,
): boolean {
	if (environ["INVOCATION_ID"]) return true;
	if (environ[S6_SUPERVISED_CHILD_ENV]) return true;
	const xpcService = environ["XPC_SERVICE_NAME"] ?? "";
	if (xpcService !== "" && xpcService !== "0") return true;
	return TRUTHY.has(
		(environ[EXTERNAL_GATEWAY_SUPERVISOR_ENV] ?? "").trim().toLowerCase(),
	);
}

/**
 * Container probe for restart routing (restart.py:is_container_restart_context):
 * Docker/Podman ⇒ the detached setsid respawn path dies with the cgroup, so
 * exit-75 service restart is the only viable path. Injectable existence check
 * keeps tests hermetic (a real /.dockerenv on a containerized CI runner would
 * otherwise flip routing under the test).
 */
export function isContainerRestartContext(
	exists?: (path: string) => boolean,
): boolean {
	const probe =
		exists ??
		((path: string) => {
			try {
				return existsSync(path);
			} catch {
				return false;
			}
		});
	return probe("/.dockerenv") || probe("/run/.containerenv");
}

export function parseNonNegativeSeconds(
	raw: unknown,
	fallback: number,
): number {
	try {
		if (raw === null || raw === undefined) return fallback;
		if (typeof raw === "string" && raw.trim() === "") return fallback;
		const value = Number(raw);
		if (!Number.isFinite(value)) return fallback;
		return Math.max(value, 0);
	} catch {
		return fallback;
	}
}

/**
 * Seconds of the shutdown-watchdog leash held back for post-drain teardown
 * (interrupt agents, kill tool subprocesses, disconnect adapters).
 * restart.py:CRON_DRAIN_CLEANUP_RESERVE_S.
 */
export const CRON_DRAIN_CLEANUP_RESERVE_S = 10.0;

function seconds(value: unknown, fallback = 0): number {
	try {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? Math.max(parsed, 0) : fallback;
	} catch {
		return fallback;
	}
}

/**
 * Seconds the drain may spend waiting on in-flight cron work
 * (restart.py:resolve_cron_drain_budget). The configured cron floor is
 * clamped to what this process can actually honour: the shutdown watchdog
 * hard-exits at watchdog_delay, so waiting past that leash minus the cleanup
 * reserve would swap a cleanly-interrupted job for a SIGKILL mid-run (#82161).
 * NEVER returns less than drain_timeout — the floor only ever extends.
 */
export function resolveCronDrainBudget(
	drainTimeoutS: number,
	cronDrainTimeoutS: number,
	options: {
		watchdogDelayS: number;
		elapsedS?: number;
		cleanupReserveS?: number;
	},
): number {
	const drain = seconds(drainTimeoutS);
	const floor = seconds(cronDrainTimeoutS);
	if (floor <= 0) return drain;
	const ceiling =
		seconds(options.watchdogDelayS) -
		seconds(options.elapsedS ?? 0) -
		seconds(options.cleanupReserveS ?? CRON_DRAIN_CLEANUP_RESERVE_S);
	return Math.max(drain, Math.min(floor, ceiling));
}

/**
 * Seconds a CLI should wait for the gateway PID to exit after SIGUSR1
 * (restart.py:resolve_restart_exit_wait_budget): in-band restart may wait out
 * active turns AND the drain; hard-kill fallbacks must cover both plus
 * headroom (default 15s) or they reintroduce #77184.
 */
export function resolveRestartExitWaitBudget(
	drainTimeoutS: number,
	afterTurnTimeoutS: number,
	headroomS = 15.0,
): number {
	return (
		seconds(drainTimeoutS) + seconds(afterTurnTimeoutS) + seconds(headroomS)
	);
}

export type ServiceRestartRouting = "service_exit" | "detached_respawn";

/**
 * Restart routing (run.py:/restart handler parity): under ANY supervisor, or
 * inside a container, exiting 75 is the only path that actually replaces us —
 * a detached setsid child would die with the cgroup. Bare host installs may
 * detach-respawn themselves instead.
 */
export function routeServiceRestart(options: {
	isSupervisor?: boolean;
	isContainer?: boolean;
}): ServiceRestartRouting {
	if (options.isSupervisor === true || options.isContainer === true) {
		return "service_exit";
	}
	return "detached_respawn";
}
