// status-liveness.test.ts — DEC-086 reader contracts for gateway_state.json.
//
// One shared file, one boss writer. Readers never trust the file alone:
// pid alive plus heartbeat freshness decide live against crashed.
// Missing reads as absent, never as stopped.

import { existsSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
	interpretRuntimeStatus,
	runtimeStatusHeartbeatAgeS,
	runtimeStatusIsStale,
	runtimeStatusPidIsLive,
	workerStatusPath,
	writeRuntimeStatus,
	writeWorkerStatus,
	RUNTIME_STATUS_STALE_TTL_S,
} from "./status-stamp.js";

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-status-liveness-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

function seedRecord(partial: Record<string, unknown>): void {
	writeFileSync(join(home, "gateway_state.json"), JSON.stringify(partial));
}

const DEAD_PID = 999999999;

describe("heartbeat freshness", () => {
	it("fresh stamp is not stale and reports age zero", () => {
		const now = Date.now();
		const age = runtimeStatusHeartbeatAgeS(
			{ updated_at: new Date(now).toISOString() },
			() => now,
		);
		expect(age).toBe(0);
		expect(
			runtimeStatusIsStale(
				{ updated_at: new Date(now).toISOString() },
				RUNTIME_STATUS_STALE_TTL_S,
				() => now,
			),
		).toBe(false);
	});

	it("old stamp is stale", () => {
		expect(
			runtimeStatusIsStale(
				{ updated_at: "2020-01-01T00:00:00.000Z" },
				RUNTIME_STATUS_STALE_TTL_S,
				() => Date.now(),
			),
		).toBe(true);
	});

	it("missing stamp is stale with null age", () => {
		expect(runtimeStatusHeartbeatAgeS(null)).toBe(null);
		expect(runtimeStatusIsStale(null)).toBe(true);
		expect(runtimeStatusIsStale({ updated_at: "garbage" })).toBe(true);
	});
});

describe("interpretRuntimeStatus", () => {
	it("missing file reads as absent, never as stopped", () => {
		const view = interpretRuntimeStatus(home);
		expect(view.outcome).toBe("absent");
		expect(view.record).toBe(null);
	});

	it("stale file plus dead pulse reports crashed, never live", () => {
		seedRecord({
			pid: DEAD_PID,
			kind: "pi-gateway",
			argv: ["old-gateway"],
			start_time: 1111111111,
			pi_home: home,
			gateway_state: "running",
			exit_reason: null,
			restart_requested: false,
			active_agents: 0,
			platforms: {},
			updated_at: "2020-01-01T00:00:00.000Z",
			code_sha: null,
			code_version: null,
		});
		const view = interpretRuntimeStatus(home);
		expect(view.outcome).toBe("crashed");
		expect(view.pidAlive).toBe(false);
		expect(view.stale).toBe(true);
		expect(view.record?.gateway_state).toBe("running");
	});

	it("fresh file plus dead pulse still reports crashed", () => {
		seedRecord({
			pid: DEAD_PID,
			kind: "pi-gateway",
			argv: ["old-gateway"],
			start_time: 1111111111,
			pi_home: home,
			gateway_state: "running",
			exit_reason: null,
			restart_requested: false,
			active_agents: 0,
			platforms: {},
			updated_at: new Date().toISOString(),
			code_sha: null,
			code_version: null,
		});
		const view = interpretRuntimeStatus(home);
		expect(view.outcome).toBe("crashed");
		expect(view.pidAlive).toBe(false);
		expect(view.stale).toBe(false);
	});

	it("live boss stamp reads live", () => {
		const livePid = process.pid;
		writeRuntimeStatus(home, { gateway_state: "running" }, { pid: livePid, home });
		const view = interpretRuntimeStatus(home);
		expect(view.outcome).toBe("live");
		expect(view.pidAlive).toBe(true);
		expect(view.stale).toBe(false);
		expect(view.record?.pid).toBe(livePid);
	});

	it("recycled pid with mismatched start_time reads crashed", () => {
		seedRecord({
			pid: 12345,
			kind: "pi-gateway",
			argv: ["gw"],
			start_time: 1111111111,
			pi_home: home,
			gateway_state: "running",
			exit_reason: null,
			restart_requested: false,
			active_agents: 0,
			platforms: {},
			updated_at: new Date().toISOString(),
			code_sha: null,
			code_version: null,
		});
		const view = interpretRuntimeStatus(home, {
			pidAlive: () => true,
			liveStartTimeSec: () => 2222222222,
		});
		expect(view.outcome).toBe("crashed");
		expect(view.pidAlive).toBe(false);
		expect(view.startTimeMatches).toBe(false);
	});

	it("stale heartbeat with live pid stays live with stale warning", () => {
		seedRecord({
			pid: 12345,
			kind: "pi-gateway",
			argv: ["gw"],
			start_time: 1111111111,
			pi_home: home,
			gateway_state: "running",
			exit_reason: null,
			restart_requested: false,
			active_agents: 0,
			platforms: {},
			updated_at: "2020-01-01T00:00:00.000Z",
			code_sha: null,
			code_version: null,
		});
		const view = interpretRuntimeStatus(home, {
			pidAlive: () => true,
			liveStartTimeSec: () => 1111111111,
		});
		expect(view.outcome).toBe("live");
		expect(view.pidAlive).toBe(true);
		expect(view.stale).toBe(true);
	});
});

describe("runtimeStatusPidIsLive", () => {
	it("dead pid is not live", () => {
		expect(runtimeStatusPidIsLive({ pid: DEAD_PID, start_time: 1 })).toBe(false);
	});

	it("live pid with matching start_time is live", () => {
		expect(
			runtimeStatusPidIsLive(
				{ pid: 12345, start_time: 1111111111 },
				{ pidAlive: () => true, liveStartTimeSec: () => 1111111112 },
			),
		).toBe(true);
	});

	it("null record is not live", () => {
		expect(runtimeStatusPidIsLive(null)).toBe(false);
	});
});

describe("worker-owned state", () => {
	it("worker scratch writes its own file and never the shared file", () => {
		const shared = join(home, "gateway_state.json");
		const path = writeWorkerStatus(home, "chat-1", { facts: 1 });
		expect(path).toBe(workerStatusPath(home, "chat-1"));
		expect(existsSync(path)).toBe(true);
		expect(existsSync(shared)).toBe(false);
		expect(JSON.parse(readFileSync(path, "utf8"))).toEqual({ facts: 1 });
	});

	it("worker ids sanitize to one directory", () => {
		const path = workerStatusPath(home, "../../evil");
		expect(path.startsWith(join(home, "workers") + "/")).toBe(true);
		expect(path).not.toContain("..");
	});
});
