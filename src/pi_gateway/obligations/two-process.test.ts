// Two-OS-PROCESS crash-recovery contracts for the obligations ledger
// (01 §3.2 posture: cross-process behavior needs real processes, not mocks):
//
//   1. A gateway crashes mid-send (attempting row, dead owner).
//   2. The NEXT boot's sweep re-drives it EXACTLY ONCE — with the visible
//      recovered-reply marker — and lands it delivered.
//   3. A THIRD boot (or a repeat sweep) finds nothing: reclaim is idempotent,
//      the redelivery budget was spent exactly once.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, type ChildProcess } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { RECOVERED_MARKER } from "./sender.js";

const DRIVER_TS = fileURLToPath(
	new URL("./testing/ledger-driver.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("../../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let dir: string;
let dbPath: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-obligations-twoproc-"));
	dbPath = join(dir, "state.db");
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

interface ChildRun {
	code: number | null;
	stdout: string;
	stderr: string;
}

function runDriver(args: string[]): Promise<ChildRun> {
	return new Promise((resolvePromise) => {
		const child: ChildProcess = spawn(
			process.execPath,
			["--import", RESOLVE_MJS, DRIVER_TS, ...args],
			{ stdio: ["ignore", "pipe", "pipe"] },
		);
		let stdout = "";
		let stderr = "";
		child.stdout?.on("data", (d: Buffer) => {
			stdout += d.toString("utf8");
		});
		child.stderr?.on("data", (d: Buffer) => {
			stderr += d.toString("utf8");
		});
		const timer = setTimeout(() => {
			child.kill("SIGKILL");
		}, 20_000);
		child.on("close", (code) => {
			clearTimeout(timer);
			resolvePromise({ code, stdout, stderr });
		});
	});
}

function parseResult(run: ChildRun): Record<string, unknown> {
	expect(run.code).toBe(0);
	const lines = run.stdout
		.split("\n")
		.filter((l) => l.startsWith("RESULT_JSON "));
	if (lines.length === 0) {
		throw new Error(`no RESULT_JSON; stderr=${run.stderr}`);
	}
	const last = lines.at(-1);
	if (!last) throw new Error(`empty RESULT_JSON; out=${run.stdout}`);
	try {
		return JSON.parse(last.slice("RESULT_JSON ".length)) as Record<
			string,
			unknown
		>;
	} catch (err) {
		throw new Error(
			`malformed RESULT_JSON (${String(err)}); line=${last}; stderr=${run.stderr}`,
		);
	}
}

describe("two-process crash recovery (real processes)", () => {
	it("crash mid-send is re-driven exactly once with the marker; further boots see nothing", {
		timeout: 60_000,
	}, async () => {
		// Boot 1: record + beginAttempt, then die before settling.
		const crashed = parseResult(await runDriver(["record-stuck", dbPath]));
		const obligationId = crashed.obligationId;
		expect(typeof obligationId).toBe("string");

		// Boot 2: sweep recovers exactly the orphaned row.
		const firstRecover = parseResult(
			await runDriver(["recover", dbPath, "telegram"]),
		);
		expect(firstRecover.claimedCount).toBe(1);
		const sends = firstRecover.sends as Array<{
			obligationId: string;
			content: string;
			needsMarker: boolean;
			attempts: number;
		}>;
		expect(sends).toHaveLength(1);
		const send = sends[0];
		if (!send) throw new Error("unreachable");
		expect(send.obligationId).toBe(obligationId);
		expect(send.needsMarker).toBe(true); // attempting ⇒ ambiguous ⇒ marker
		expect(send.content.startsWith(RECOVERED_MARKER)).toBe(true);
		expect(send.content.endsWith("crash-surviving answer")).toBe(true);
		expect(send.attempts).toBe(1); // budget spent once

		const states = firstRecover.states as Record<string, string | null>;
		expect(states[obligationId as string]).toBe("delivered");

		// Boot 3: idempotent — nothing left to claim, no second send.
		const thirdBoot = parseResult(
			await runDriver(["recover", dbPath, "telegram"]),
		);
		expect(thirdBoot.claimedCount).toBe(0);
		expect(thirdBoot.sends).toEqual([]);
	});

	it("an absent platform this boot leaves the row untouched for a later boot", {
		timeout: 60_000,
	}, async () => {
		const crashed = parseResult(await runDriver(["record-stuck", dbPath]));
		const obligationId = crashed.obligationId as string;

		const skipped = parseResult(
			await runDriver(["recover", dbPath, "discord"]),
		);
		expect(skipped.claimedCount).toBe(0);
		expect(skipped.sends).toEqual([]);

		// Platform returns on a later boot → delivered then, budget intact-ish.
		const later = parseResult(await runDriver(["recover", dbPath, "telegram"]));
		expect(later.claimedCount).toBe(1);
		expect((later.states as Record<string, string>)[obligationId]).toBe(
			"delivered",
		);
	});
});
