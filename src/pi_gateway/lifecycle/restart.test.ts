// Behavior contracts for the supervisor restart contract (08 §1.3/§2;
// gateway/restart.py port): exit codes 75/78, supervisor + container
// detection, cron-drain clamp against the watchdog leash, CLI exit-wait
// budget, service-restart routing.

import { describe, expect, it } from "vitest";
import {
	CRON_DRAIN_CLEANUP_RESERVE_S,
	FATAL_CONFIG_EXIT_CODE,
	FATAL_CONFIG_REASON_CODE,
	SERVICE_RESTART_EXIT_CODE,
	isContainerRestartContext,
	isGatewaySupervisorProcess,
	resolveCronDrainBudget,
	resolveRestartExitWaitBudget,
	routeServiceRestart,
} from "./restart.js";
import { SHUTDOWN_EXIT_CODES } from "./shutdown.js";

describe("supervisor exit-code contract (restart.py constants)", () => {
	it("service restart exits 75 (EX_TEMPFAIL) and fatal config exits 78 (EX_CONFIG)", () => {
		expect(SERVICE_RESTART_EXIT_CODE).toBe(75);
		expect(FATAL_CONFIG_EXIT_CODE).toBe(78);
		// The drain outcome map carries BOTH classes alongside the originals.
		expect(SHUTDOWN_EXIT_CODES.service_restart).toBe(75);
		expect(SHUTDOWN_EXIT_CODES.fatal_config).toBe(78);
		expect(SHUTDOWN_EXIT_CODES.takeover).toBe(0);
		expect(SHUTDOWN_EXIT_CODES.planned_stop).toBe(0);
		expect(SHUTDOWN_EXIT_CODES.unexpected_signal).toBe(1);
	});

	it("fatal-config reason code is the lifecycle abort classification", () => {
		expect(FATAL_CONFIG_REASON_CODE).toBe("fatal_config");
	});
});

describe("supervisor detection (is_gateway_supervisor_process parity)", () => {
	it("systemd INVOCATION_ID marks supervision", () => {
		expect(isGatewaySupervisorProcess({ INVOCATION_ID: "abc" })).toBe(true);
	});

	it("s6 supervised-child env marks supervision", () => {
		expect(isGatewaySupervisorProcess({ PI_S6_SUPERVISED_CHILD: "1" })).toBe(
			true,
		);
	});

	it("launchd XPC_SERVICE_NAME counts unless it is the literal '0'", () => {
		expect(
			isGatewaySupervisorProcess({ XPC_SERVICE_NAME: "com.example.pi" }),
		).toBe(true);
		expect(isGatewaySupervisorProcess({ XPC_SERVICE_NAME: "0" })).toBe(false);
		expect(isGatewaySupervisorProcess({ XPC_SERVICE_NAME: "" })).toBe(false);
	});

	it("external-supervisor env accepts only truthy spellings", () => {
		for (const v of ["1", "true", "yes", "on", "TRUE"]) {
			expect(
				isGatewaySupervisorProcess({ PI_GATEWAY_EXTERNAL_SUPERVISOR: v }),
			).toBe(true);
		}
		for (const v of ["0", "false", "", "nope"]) {
			expect(
				isGatewaySupervisorProcess({ PI_GATEWAY_EXTERNAL_SUPERVISOR: v }),
			).toBe(false);
		}
	});

	it("no supervisor markers ⇒ bare process", () => {
		expect(isGatewaySupervisorProcess({})).toBe(false);
	});
});

describe("container restart routing", () => {
	it("either container marker file routes to the service-exit path", () => {
		expect(isContainerRestartContext((p) => p === "/.dockerenv")).toBe(true);
		expect(isContainerRestartContext((p) => p === "/run/.containerenv")).toBe(
			true,
		);
		expect(isContainerRestartContext(() => false)).toBe(false);
	});

	it("supervised or containerized gateways MUST exit 75 — a detached respawn dies with the cgroup", () => {
		expect(routeServiceRestart({ isSupervisor: true })).toBe("service_exit");
		expect(
			routeServiceRestart({ isSupervisor: false, isContainer: true }),
		).toBe("service_exit");
		// Bare host installs keep the detached-respawn option.
		expect(
			routeServiceRestart({ isSupervisor: false, isContainer: false }),
		).toBe("detached_respawn");
	});
});

describe("cron-drain budget clamp (#82161, resolve_cron_drain_budget parity)", () => {
	const watchdogDelay = 60; // drain 0 + 60s grace leash

	it("cron floor EXTENDS a zero chat-drain but never below drain_timeout", () => {
		expect(
			resolveCronDrainBudget(0, 20, { watchdogDelayS: watchdogDelay }),
		).toBe(20);
		expect(
			resolveCronDrainBudget(5, 3, { watchdogDelayS: watchdogDelay }),
		).toBe(5);
	});

	it("floor clamps to the watchdog leash minus elapsed minus the 10s cleanup reserve", () => {
		expect(CRON_DRAIN_CLEANUP_RESERVE_S).toBe(10);
		// ceiling = 60 - 10(elapsed) - 10(reserve) = 40 < floor 55 ⇒ 40
		expect(
			resolveCronDrainBudget(0, 55, { watchdogDelayS: 60, elapsedS: 10 }),
		).toBe(40);
	});

	it("zero/negative cron floor opts out entirely (pre-#82161 behavior)", () => {
		expect(
			resolveCronDrainBudget(7, 0, { watchdogDelayS: watchdogDelay }),
		).toBe(7);
		expect(
			resolveCronDrainBudget(7, -3, { watchdogDelayS: watchdogDelay }),
		).toBe(7);
	});
});

describe("CLI exit-wait budget (resolve_restart_exit_wait_budget)", () => {
	it("covers after-turn wait + drain + 15s headroom (#77184)", () => {
		expect(resolveRestartExitWaitBudget(10, 30)).toBe(55);
		expect(resolveRestartExitWaitBudget(0, 0)).toBe(15);
		expect(resolveRestartExitWaitBudget(2.5, 4, 5)).toBe(11.5);
	});
});
