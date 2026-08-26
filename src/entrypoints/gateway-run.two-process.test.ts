// gateway-run.two-process.test.ts — REAL two-process startup/shutdown
// contracts for the composition root (structure-7 integration risk mandate:
// "verify with two-process startup/shutdown suites").
//
// A real second OS process runs runGateway() — the full composed stack:
// engine stages, cron ticker over a real jobs store, embedded extensions
// discovery, a manifest-derived adapter entry with a conforming
// connect/disconnect surface, production drain overlays, and REAL installed
// signal handlers. The parent SIGTERMs it and asserts the supervisor
// contract end-to-end (08 §1.2): exit 0, gateway_state=stopped persisted,
// PID file released, .clean_shutdown receipt written, adapter disconnected,
// self-held turn leases swept while foreign rows survive.

import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { spawn, type ChildProcess } from "node:child_process";
import { cleanShutdownMarkerPath } from "../pi_gateway/lifecycle/shutdown.js";
import { writePlannedStopMarker } from "../pi_gateway/lifecycle/markers.js";
import { pidFilePath } from "../pi_gateway/lifecycle/instance-guard.js";
import { readRuntimeStatus } from "../pi_gateway/lifecycle/status-stamp.js";
import { StateStore } from "../pi_state/index.js";
import { structuredHolder } from "../pi_state/leases.js";

const DRIVER_TS = fileURLToPath(
	new URL("./testing/gateway-run-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let home: string;
let coord: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-gateway-runproc-home-"));
	coord = mkdtempSync(join(tmpdir(), "pi-gateway-runproc-coord-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
	rmSync(coord, { recursive: true, force: true });
});

async function waitReadyMarker(timeoutMs = 30_000): Promise<void> {
	const marker = join(coord, "ready");
	const deadline = Date.now() + timeoutMs;
	while (!existsSync(marker)) {
		if (Date.now() > deadline) throw new Error("child never became ready");
		await new Promise((r) => setTimeout(r, 20));
	}
}

function waitExit(child: ChildProcess): Promise<void> {
	return new Promise((resolve) => {
		if (child.exitCode !== null || child.signalCode !== null) resolve();
		else child.once("exit", () => resolve());
	});
}

interface ChildRunResult {
	code: number | null;
	stdout: string;
	stderr: string;
}

function parseResultJson(stdout: string): { exitCode: number; ran: boolean } {
	for (const line of stdout.split("\n").reverse()) {
		if (line.startsWith("RESULT_JSON ")) {
			return JSON.parse(line.slice("RESULT_JSON ".length)) as {
				exitCode: number;
				ran: boolean;
			};
		}
	}
	throw new Error(`no RESULT_JSON line in child stdout:\n${stdout}`);
}

describe("gateway-run — two-process composition contracts", () => {
	it("SIGTERM against the fully-composed child drains gracefully: exit 0, stopped stamp, PID release, clean-shutdown receipt, adapter disconnect, self-lease sweep with foreign survival", async () => {
		const child = spawn(
			process.execPath,
			["--import", RESOLVE_MJS, DRIVER_TS, "--home", home, "--coord", coord],
			{
				env: { ...process.env, DRIVER_TOKEN: "tok-two-process" },
				stdio: ["ignore", "pipe", "pipe"],
			},
		);
		let stdout = "";
		let stderr = "";
		child.stdout?.on("data", (chunk: Buffer) => {
			stdout += String(chunk);
		});
		child.stderr?.on("data", (chunk: Buffer) => {
			stderr += String(chunk);
		});

		try {
			await waitReadyMarker();
			const childPid = child.pid;
			expect(typeof childPid).toBe("number");

			// Seed leases AFTER READY so the child's stage 6 already opened its
			// store: one row held by the CHILD process pid (swept at drain), one
			// foreign row that must survive.
			const seed = await StateStore.open(join(home, "state.db"));
			const now = Date.now() / 1000;
			seed.db
				.prepare(
					"INSERT INTO session_turn_leases (conversation_id, holder, acquired_at, expires_at) VALUES (?, ?, ?, ?)",
				)
				.run(
					"conv-child",
					structuredHolder("turn-lease", childPid as number),
					now - 5,
					now + 600,
				);
			seed.db
				.prepare(
					"INSERT INTO session_turn_leases (conversation_id, holder, acquired_at, expires_at) VALUES (?, ?, ?, ?)",
				)
				.run(
					"conv-foreign",
					structuredHolder("other-gateway", 999999999),
					now - 5,
					now + 600,
				);
			seed.close();

			// Marker-before-SIGTERM ordering (08 §1.2): `gateway stop` marks the
			// stop as planned BEFORE signalling, so the child classifies the
			// signal planned_stop (exit 0), never unexpected_signal (exit 1).
			expect(writePlannedStopMarker(home, childPid as number)).toBe(true);
			child.kill("SIGTERM");
			await waitExit(child);
		} catch (err) {
			child.kill("SIGKILL");
			throw new Error(
				`${String(err)}\n--- child stderr ---\n${stderr.slice(-4000)}`,
			);
		}

		// Supervisor contract (08 §1.2): planned stop exits 0.
		const result = parseResultJson(stdout);
		expect(result.ran).toBe(true);
		expect(result.exitCode).toBe(0);

		// gateway_state=stopped persisted for planned stops (#42675 inverse).
		const status = readRuntimeStatus(home);
		expect(status?.gateway_state).toBe("stopped");

		// PID file released on the drain path (never stranded).
		expect(existsSync(pidFilePath(home))).toBe(false);

		// Graceful completion wrote the receipt; next boot skips suspension.
		expect(existsSync(cleanShutdownMarkerPath(home))).toBe(true);

		// Adapter entry handle drove disconnect during stop_ingress.
		const adapterLog = JSON.parse(
			readFileSync(join(coord, "adapter-log"), "utf8"),
		) as { connects: number; disconnects: number };
		expect(adapterLog.connects).toBe(1);
		expect(adapterLog.disconnects).toBe(1);

		// Drain overlays: child-pid lease gone, foreign lease survives.
		const verify = await StateStore.open(join(home, "state.db"));
		const remaining = verify.db
			.prepare("SELECT conversation_id FROM session_turn_leases")
			.all() as Array<{ conversation_id: string }>;
		verify.close();
		expect(remaining.map((r) => r.conversation_id)).toEqual(["conv-foreign"]);
	});
});
