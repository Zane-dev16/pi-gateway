// TEST INFRASTRUCTURE — gateway-unit driver for the two-process contracts.
//
// Simulates ONE manually-supervised gateway unit on a profile home:
//   argv: <mode> <homeDir>
//     drain-current:<shaFile>  SIGUSR1 ⇒ respawn a replacement stamped with
//                              the sha READ FROM <shaFile> at drain time,
//                              rewrite gateway_state.json, exit 0.
//     drain-stale:<oldSha>     SIGUSR1 ⇒ respawn a replacement stamped with
//                              the GIVEN (pre-update) sha, rewrite state,
//                              exit 0 — the stale-gateway drill arm.
//     stubborn                 ignores SIGUSR1 AND SIGTERM — a wedged unit
//                              that must fail the phase closed (#78574).
//
// The handler order is deliberate and synchronous: replacement spawned →
// state rewritten → exit. When the updater's liveness poll observes death,
// the state file ALREADY names the replacement, mirroring how a real
// supervisor respawns while the updater watches the ORIGINAL pid die.

import { spawn } from "node:child_process";
import { readFileSync, rmSync, renameSync, writeFileSync } from "node:fs";
import { join } from "node:path";

const [mode, home] = process.argv.slice(2);

if (!mode || !home) {
	console.error("usage: unit-driver.ts <mode> <home>");
	process.exit(2);
}
const driverHome: string = home;
const pidFile = `${driverHome}.unit.pid`;
writeFileSync(pidFile, String(process.pid));

function respawnStamping(sha: string): void {
	const replacement = spawn(
		process.execPath,
		["-e", "setInterval(() => {}, 1 << 30)"],
		{ detached: true, stdio: "ignore" },
	);
	replacement.unref();
	if (replacement.pid === undefined) {
		// Spawn failed outright — leave the OLD stamp in place; the drill then
		// observes a drained (dead) unit, not a fabricated replacement.
		return;
	}
	writeGatewayStateRecord(driverHome, replacement.pid as number, sha);
}

/**
 * Write a gateway_state.json record in the EXACT 08 §4 verified field set.
 * Layering (01 §5.3) forbids importing pi_gateway/lifecycle's writer here,
 * so the driver produces the documented schema directly; atomicity rides
 * write-temp-rename like every other state file.
 */
function writeGatewayStateRecord(home: string, pid: number, sha: string): void {
	const path = join(home, "gateway_state.json");
	const record = {
		pid,
		kind: "pi-gateway",
		argv: ["replacement-unit"],
		start_time: Math.floor(Date.now() / 1000),
		pi_home: home,
		gateway_state: "running",
		exit_reason: null,
		restart_requested: false,
		active_agents: 0,
		platforms: {},
		updated_at: new Date().toISOString(),
		code_sha: sha,
		code_version: "0.0.0-test",
	};
	const tmp = `${path}.tmp`;
	writeFileSync(tmp, JSON.stringify(record, null, 2));
	renameSync(tmp, path);
}

if (mode === "stubborn") {
	process.on("SIGUSR1", () => {
		/* wedge: ignore the drain request */
	});
	process.on("SIGTERM", () => {
		/* wedge: ignore the stop-after-window request */
	});
} else {
	const colon = mode.indexOf(":");
	const kind = colon === -1 ? mode : mode.slice(0, colon);
	const arg = colon === -1 ? "" : mode.slice(colon + 1);
	if (kind !== "drain-current" && kind !== "drain-stale") {
		console.error(`unknown mode: ${mode}`);
		process.exit(2);
	}
	process.on("SIGUSR1", () => {
		if (kind === "drain-current") {
			respawnStamping(readFileSync(arg, "utf8").trim());
		} else {
			respawnStamping(arg);
		}
		process.exit(0);
	});
}

// Stay alive until signaled.
setInterval(() => {}, 1 << 30);
