// Two-OS-PROCESS SIGHUP contracts (DEC-042, 08 §8 closing note) — the claims
// mocks cannot prove:
//   1. a command chain spawned through the update runner survives
//      self-delivered SIGHUP across TWO exec hops (trap-wrapped sh execs a
//      second sh which kills itself) — behavioral survival, not mask reading;
//   2. a NON-NODE child (the git/pip class) really carries the SIG_IGN bit in
//      /proc/<pid>/status while running — parser-derived /proc evidence;
//   3. an UNWRAPPED child dies by SIGHUP (host honesty: default disposition);
//   4. the parent window installs/restores cleanly.
//
// NOTE ON NODE CHILDREN (measured on this host): Node RESETS its own
// inherited SIGHUP during bootstrap (SigIgn lacks the HUP bit even right
// after trap+exec into node) — so a Node process can never serve as the
// survival probe. The binding property targets git/package-manager children,
// which do not touch SIGHUP disposition at startup.

import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { spawn, spawnSync } from "node:child_process";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { nodeUpdateCommandRunner } from "./run.js";
import {
	HANGUP_SAFE_ARGV_PREFIX,
	installHangupProtection,
	wrapHangupSafe,
} from "./hangup.js";

const PROBE_TS = fileURLToPath(
	new URL("./testing/sighup-probe.ts", import.meta.url),
);
const RESOLVE_MJS = fileURLToPath(
	new URL("../../pi_state/testing/node-ts-resolve.mjs", import.meta.url),
);

let dir: string;

beforeEach(() => {
	dir = mkdtempSync(join(tmpdir(), "pi-gw-update-hup-"));
});

afterEach(() => {
	rmSync(dir, { recursive: true, force: true });
});

describe("child-exec inheritance (trap '' HUP ⇒ SIG_IGN survives exec)", () => {
	it("a runner-spawned chain SURVIVES self-delivered SIGHUP across two exec hops", () => {
		if (process.platform === "win32") return; // no SIGHUP there
		// Outer sh (wrapped by the runner) execs an INNER sh; POSIX preserves
		// SIG_IGN across BOTH hops. The inner sh kills itself — survival is
		// behavioral proof the disposition stayed SIG_IGN end-to-end.
		const result = nodeUpdateCommandRunner(
			["sh", "-c", "kill -HUP $$; sleep 0.3; echo ALIVE-HUP-SAFE"],
			dir,
		);
		expect(result.spawnError).toBeNull();
		expect(result.status).toBe(0);
		expect(result.stdout).toContain("ALIVE-HUP-SAFE");
	});

	it("a NON-NODE child carries the SIG_IGN bit in /proc while running (the git/pip class)", async () => {
		if (process.platform === "win32") return;
		const wrapped = wrapHangupSafe(["sleep", "5"]);
		const child = spawn(wrapped.spawnArgv[0] as string, [
			...wrapped.spawnArgv.slice(1),
		]);
		try {
			await new Promise<void>((resolvePromise) => {
				setTimeout(resolvePromise, 200);
			});
			const status = readFileSync(`/proc/${child.pid}/status`, "utf8");
			const sigIgn = /^SigIgn:\s+([0-9a-f]+)$/m.exec(status)?.[1];
			// Mask bit N-1 maps to signal N: SIGHUP(1) = bit 0 = 0x1 must be SET.
			expect(sigIgn).toBeDefined();
			expect(BigInt(`0x${sigIgn as string}`) & 0x1n).toBe(0x1n);
		} finally {
			child.kill("SIGKILL");
		}
	}, 10_000);

	it("an UNWRAPPED node probe dies by SIGHUP — proving the host default kills", () => {
		if (process.platform === "win32") return;
		const bare = spawnSync(
			process.execPath,
			["--import", RESOLVE_MJS, PROBE_TS],
			{
				cwd: dir,
				encoding: "utf8",
				timeout: 10_000,
			},
		);
		// Default disposition: terminated BY the signal, never reaching ALIVE.
		expect(bare.signal).toBe("SIGHUP");
		expect(bare.stdout ?? "").not.toContain("ALIVE");
	});

	it("the wrapper prefix is POSIX-only and preserves the logical argv", () => {
		const wrapped = wrapHangupSafe(["git", "pull", "--ff-only"]);
		if (process.platform === "win32") {
			expect(wrapped.spawnArgv).toEqual(["git", "pull", "--ff-only"]);
		} else {
			expect(HANGUP_SAFE_ARGV_PREFIX.length).toBe(4);
			expect(wrapped.spawnArgv.slice(-3)).toEqual(["git", "pull", "--ff-only"]);
			expect(wrapped.logicalArgv).toEqual(["git", "pull", "--ff-only"]);
		}
	});
});

describe("update-window absorption in THIS process (DEC-042(a))", () => {
	it("install absorbs a self-SIGHUP; restore returns listenerCount to baseline", async () => {
		if (process.platform === "win32") return;
		const baseline = process.listenerCount("SIGHUP");
		const guard = installHangupProtection();
		expect(process.listenerCount("SIGHUP")).toBe(baseline + 1);
		expect(() => {
			process.kill(process.pid, "SIGHUP");
		}).not.toThrow();
		// Let the absorbed signal drain through the event loop.
		await new Promise<void>((resolvePromise) => {
			setTimeout(resolvePromise, 100);
		});
		guard.restore();
		guard.restore(); // idempotent
		expect(process.listenerCount("SIGHUP")).toBe(baseline);
	});
});
