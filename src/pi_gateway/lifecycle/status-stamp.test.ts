// status-stamp pid convergence — the stamp names the RUNNING process, so a
// bounce must reconverge pid/argv/start_time onto the live identity instead
// of preserving the dead row from the previous life.
import { describe, expect, it } from "vitest";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
	readRuntimeStatus,
	writeRuntimeStatus,
} from "./status-stamp.js";

const DEAD_PID = 999999999;
const DEAD_START = 1111111111;

function seedDeadStamp(home: string): void {
	writeFileSync(
		join(home, "gateway_state.json"),
		JSON.stringify({
			pid: DEAD_PID,
			kind: "pi-gateway",
			argv: ["old-gateway"],
			start_time: DEAD_START,
			pi_home: home,
			gateway_state: "running",
			exit_reason: null,
			restart_requested: false,
			active_agents: 2,
			platforms: {},
			updated_at: "2020-01-01T00:00:00.000Z",
			code_sha: null,
			code_version: null,
		}),
	);
}

describe("runtime status stamp pid convergence", () => {
	it("bounce replaces the dead pid with the live pid", () => {
		const home = mkdtempSync(join(tmpdir(), "pi-stamp-bounce-"));
		seedDeadStamp(home);
		const livePid = process.pid;
		const next = writeRuntimeStatus(
			home,
			{ gateway_state: "running" },
			{ pid: livePid, home },
		);
		expect(next.pid).toBe(livePid);
		expect(next.start_time).not.toBe(DEAD_START);
		expect(next.argv).toEqual(process.argv);
		expect(next.gateway_state).toBe("running");
		expect(readRuntimeStatus(home)?.pid).toBe(livePid);
	});

	it("steady-state write from the same process preserves identity fields", () => {
		const home = mkdtempSync(join(tmpdir(), "pi-stamp-steady-"));
		const livePid = process.pid;
		writeRuntimeStatus(
			home,
			{ gateway_state: "running" },
			{ pid: livePid, startTimeSec: 1234567890, argv: ["gw"], home },
		);
		const next = writeRuntimeStatus(
			home,
			{ active_agents: 3 },
			{ pid: livePid, home },
		);
		expect(next.pid).toBe(livePid);
		expect(next.start_time).toBe(1234567890);
		expect(next.argv).toEqual(["gw"]);
		expect(next.active_agents).toBe(3);
	});

	it("competing starter never restamps the live holder", () => {
		const home = mkdtempSync(join(tmpdir(), "pi-stamp-single-writer-"));
		const bossPid = 424242;
		const bossStart = 1111111111;
		const loserPid = 424243;
		writeFileSync(
			join(home, "gateway_state.json"),
			JSON.stringify({
				pid: bossPid,
				kind: "pi-gateway",
				argv: ["boss-gateway"],
				start_time: bossStart,
				pi_home: home,
				gateway_state: "running",
				exit_reason: null,
				restart_requested: false,
				active_agents: 0,
				platforms: {},
				updated_at: new Date().toISOString(),
				code_sha: null,
				code_version: null,
			}),
		);
		const before = readFileSync(join(home, "gateway_state.json"), "utf8");
		const probes = {
			pidAlive: () => true,
			liveStartTimeSec: () => bossStart,
		};
		const next = writeRuntimeStatus(
			home,
			{ exit_reason: "startup_failed:duplicate_guard" },
			{ pid: loserPid, home },
			probes,
		);
		expect(next.pid).toBe(bossPid);
		expect(next.exit_reason).toBe(null);
		expect(readFileSync(join(home, "gateway_state.json"), "utf8")).toBe(
			before,
		);
		expect(readRuntimeStatus(home)?.pid).toBe(bossPid);
	});
});
