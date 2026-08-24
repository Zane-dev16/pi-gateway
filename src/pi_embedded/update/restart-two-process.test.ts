// Two-OS-PROCESS restart-per-kind contracts (08 §7): fleet-wide drain-first
// SIGUSR1 across TWO profile units; survivors stop AFTER the window; a
// wedged unit fails the phase closed (#78574); per-unit failure isolates.

import { readFileSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { restartFleet, restartPhaseFailureIsIncomplete } from "./restart.js";

const DRIVER_TS = fileURLToPath(
	new URL("./testing/unit-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("../../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let dir: string;
const children: ChildProcess[] = [];

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-update-restart-"));
});

afterEach(() => {
	for (const child of children.splice(0)) {
		try {
			child.kill("SIGKILL");
		} catch {
			/* already gone */
		}
	}
	rmSync(dir, { recursive: true, force: true });
});

/** Spawn one unit driver; resolves once its pidfile appears. */
async function launchUnit(
	mode: string,
): Promise<{ pid: number; pidFile: string }> {
	const home = join(dir, `unit-${Math.random().toString(36).slice(2)}`);
	const pidFile = `${home}.unit.pid`;
	const child = spawn(
		process.execPath,
		["--import", RESOLVE_MJS, DRIVER_TS, mode, home],
		{ stdio: ["ignore", "pipe", "pipe"] },
	);
	children.push(child);
	let stderr = "";
	child.stderr?.on("data", (d: Buffer) => {
		stderr += d.toString("utf8");
	});
	for (let i = 0; i < 100; i++) {
		try {
			return {
				pid: Number.parseInt(readFileSync(pidFile, "utf8").trim(), 10),
				pidFile,
			};
		} catch {
			await new Promise<void>((resolvePromise) => {
				setTimeout(resolvePromise, 50);
			});
		}
	}
	throw new Error(`unit driver never started: ${mode}: ${stderr}`);
}

describe("fleet-wide drain-first restart (real signals)", () => {
	it("drains BOTH profile units via SIGUSR1 and reports completed with no survivors", async () => {
		const a = await launchUnit(`drain-current:${join(dir, "newsha")}`);
		const b = await launchUnit(`drain-current:${join(dir, "newsha")}`);
		writeFileSync(join(dir, "newsha"), "c".repeat(40));
		const result = await restartFleet(
			[
				{ profile: "default", supervisor: "manual", pid: a.pid },
				{ profile: "work", supervisor: "manual", pid: b.pid },
			],
			{ drainTimeoutMs: 8_000 },
		);
		expect(result.outcome).toBe("completed");
		expect(result.units.map((u) => u.drained)).toEqual([true, true]);
		expect(result.survivingPids).toEqual([]);
	}, 20_000);

	it("a survivor that ignores the drain is STOPPED after the window; phase completes only when it dies", async () => {
		// stubborn mode ignores SIGUSR1 AND SIGTERM — the stop-after-window
		// cannot reap it, so the final probe still sees it alive ⇒ fail closed.
		const wedged = await launchUnit("stubborn");
		const drained = await launchUnit(`drain-current:${join(dir, "newsha")}`);
		const result = await restartFleet(
			[
				{ profile: "wedged", supervisor: "manual", pid: wedged.pid },
				{ profile: "healthy", supervisor: "manual", pid: drained.pid },
			],
			{ drainTimeoutMs: 2_000 },
		);
		// Per-unit isolation: healthy unit STILL drained despite the wedge.
		const healthy = result.units.find((u) => u.profile === "healthy");
		const wedgedTrace = result.units.find((u) => u.profile === "wedged");
		expect(healthy?.drained).toBe(true);
		expect(wedgedTrace?.drainSignaled).toBe(true);
		expect(wedgedTrace?.stoppedAfterWindow).toBe(true); // SIGTERM sent after window…
		expect(result.survivingPids).toEqual([wedged.pid]); // …but it ignored that too
		expect(result.outcome).toBe("incomplete"); // survivors ⇒ assume stale, fail closed
	}, 20_000);

	it("the fail-closed verdict table matches _restart_phase_failure_is_incomplete verbatim", () => {
		// surviving null — probe could not determine state ⇒ assume stale.
		expect(restartPhaseFailureIsIncomplete(null, [])).toBe(true);
		// surviving non-empty — old-code gateways still running.
		expect(restartPhaseFailureIsIncomplete([101], [101])).toBe(true);
		// surviving empty + provably nothing ran before ⇒ the ONE safe shape.
		expect(restartPhaseFailureIsIncomplete([], [])).toBe(false);
		// surviving empty BUT gateways ran pre-restart (or pre-state unreadable)
		// ⇒ stopped without verified replacement ⇒ incomplete (#78574).
		expect(restartPhaseFailureIsIncomplete([], [202])).toBe(true);
		expect(restartPhaseFailureIsIncomplete([], null)).toBe(true);
	});

	it("a unit that dies on its own between resolve and drain counts as drained — replacement proof is VERIFY's job", async () => {
		const doomed = await launchUnit(`drain-current:${join(dir, "newsha")}`);
		process.kill(doomed.pid, "SIGKILL"); // claimed-live unit vanishes pre-drain
		await new Promise<void>((resolvePromise) => {
			setTimeout(resolvePromise, 150);
		});
		// The phase signaled and observed death — indistinguishable from a fast
		// graceful drain. If a supervisor respawns it STALE, the verify stage's
		// fleet sha matrix catches that (#88654); the restart phase owns only
		// ORIGINAL-pid liveness. The #78574 stopped-without-replacement rule is
		// covered verbatim by the pure-table test below.
		const result = await restartFleet([
			{ profile: "x", supervisor: "manual", pid: doomed.pid },
		]);
		expect(result.outcome).toBe("completed");
	}, 20_000);
});
