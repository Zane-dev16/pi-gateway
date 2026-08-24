// Canonical process-matcher contracts (08 §9): parser-derived identity from
// /proc data structures; argv-substring inference is BANNED. Includes the §11
// adversarial argv matrix — mimic-named processes must NOT match.

import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	discoverLiveGateways,
	gatewayCommandSubcommand,
	holderValueFlags,
	listProcIdentities,
	looksLikeGatewayCommandLine,
	looksLikeGatewayRuntimeCommandLine,
	piHolderSubcommand,
	readProcCmdline,
	tokenizeCommandLine,
	TOP_LEVEL_OPTION_SPECS,
} from "./proc-matchers.js";

describe("tokenizeCommandLine (quote-aware, shlex posix=False parity)", () => {
	it("splits on whitespace but keeps quoted phrases as one token", () => {
		expect(tokenizeCommandLine('pi -p "my profile" gateway run')).toEqual([
			"pi",
			"-p",
			"my profile",
			"gateway",
			"run",
		]);
		expect(
			tokenizeCommandLine("'C:\\Program Files\\pi\\pi.exe' gateway run"),
		).toHaveLength(3);
	});

	it("degrades to whitespace splitting on unterminated quotes", () => {
		expect(tokenizeCommandLine('pi gateway "run')).toEqual([
			"pi",
			"gateway",
			"run",
		]);
	});
});

describe("looksLikeGatewayCommandLine — strict public predicate", () => {
	it("matches real gateway-run command lines in every entry shape", () => {
		for (const cmdline of [
			"pi gateway run",
			"pi gateway", // bare `gateway` defaults to run
			"/usr/local/bin/pi -p work gateway run",
			"node --import x pi_cli/main.ts gateway run",
			"python -m pi_cli.main --profile work gateway run",
			"/opt/pi/bin/pi-gateway", // dedicated entrypoint
			"node /srv/app/pi_gateway/run.ts",
		]) {
			expect(looksLikeGatewayCommandLine(cmdline), cmdline).toBe(true);
		}
	});

	it("REJECTS management subcommands and runtime-variant-only lines", () => {
		expect(looksLikeGatewayCommandLine("pi gateway status")).toBe(false);
		expect(looksLikeGatewayCommandLine("pi gateway restart")).toBe(false);
		expect(looksLikeGatewayCommandLine("pi gateway stop")).toBe(false);
	});

	it("REJECTS the adversarial mimic matrix — substrings and flag values lie", () => {
		for (const cmdline of [
			"vim my-gateway-run-notes.txt", // contains 'gateway run' inside a token
			"tail -f /var/log/gateway.log run", // words present, no pi entry
			"python -m tui_gateway run", // entry ends with gateway-ish module
			"echo gateway run", // no pi/entry token at all
			"pi dashboard serve",
		]) {
			expect(looksLikeGatewayCommandLine(cmdline), cmdline).toBe(false);
		}
		// Empty/truncated cmdlines hide truth — the matcher returns null, never guesses.
		expect(looksLikeGatewayCommandLine("")).toBe(false);
		expect(looksLikeGatewayCommandLine(null)).toBe(false);
		// A profile VALUE named 'gateway' consumes the word (status.py parity:
		// "This consumes a profile VALUE of 'gateway' too") — that argv contains
		// NO gateway subcommand, so null is the correct verdict.
		expect(gatewayCommandSubcommand("pi --profile gateway status")).toBeNull();
		expect(gatewayCommandSubcommand("pi -p gateway stop")).toBeNull();
		// Positive control: real profile selector plus a real subcommand.
		expect(gatewayCommandSubcommand("pi --profile work gateway status")).toBe(
			"status",
		);
	});

	it("keeps the relaxed runtime variant separate (run + restart only)", () => {
		expect(looksLikeGatewayRuntimeCommandLine("pi gateway restart")).toBe(true);
		expect(looksLikeGatewayRuntimeCommandLine("pi gateway run")).toBe(true);
		expect(looksLikeGatewayRuntimeCommandLine("pi gateway status")).toBe(false);
	});
});

describe("holderValueFlags — DERIVED from the declared option surface (#91869)", () => {
	it("contains exactly the flags of value-taking specs plus pre-argparse selectors", () => {
		const flags = holderValueFlags();
		for (const spec of TOP_LEVEL_OPTION_SPECS) {
			for (const flag of spec.flags) {
				expect(flags.has(flag)).toBe(spec.takesValue);
			}
		}
		expect(flags.has("--profile")).toBe(true);
		expect(flags.has("-p")).toBe(true);
		expect(flags.has("--config")).toBe(true);
		// Non-value flags must NOT be treated as consuming a value:
		expect(flags.has("--help")).toBe(false);
		expect(flags.has("--version")).toBe(false);
		expect(flags.has("-v")).toBe(false);
	});
});

describe("piHolderSubcommand — token-based, never substring (#90778)", () => {
	it("finds the subcommand after skipping flags and their VALUES", () => {
		expect(piHolderSubcommand("pi kanban serve")).toBe("kanban");
		expect(piHolderSubcommand("/usr/bin/pi --reasoning high serve")).toBe(
			"serve",
		);
		expect(piHolderSubcommand("pi -m fable-5 serve")).toBe("serve");
		expect(piHolderSubcommand("python -m pi_cli.main gateway run")).toBe(
			"gateway",
		);
		expect(piHolderSubcommand("pi --model=x serve")).toBe("serve"); // =value consumes ONE token
	});

	it("returns null when undetermined — callers must NOT guess a label", () => {
		expect(piHolderSubcommand("pi --preserve-cache")).toBeNull(); // flag with no following bare token
		expect(piHolderSubcommand("pi -m")).toBeNull();
		expect(piHolderSubcommand("totally-unrelated-process")).toBeNull();
		expect(piHolderSubcommand("")).toBeNull();
		expect(piHolderSubcommand(null)).toBeNull();
	});

	it("never mistakes flag VALUES for subcommands (the #91869 wrong-hint class)", () => {
		// `-m x serve`: model value consumed ⇒ real subcommand IS serve.
		expect(piHolderSubcommand("pi -m x serve")).toBe("serve");
		// But `--preserve-cache` (non-value flag) does not swallow the next token.
		expect(piHolderSubcommand("pi kanban --preserve-cache")).toBe("kanban");
	});
});

// --- /proc identity ---------------------------------------------------------

let procDir: string;

beforeEach(() => {
	procDir = mkdtempSync(join(tmpdir(), "pi-gw-update-proc-"));
});

afterEach(() => {
	rmSync(procDir, { recursive: true, force: true });
});

function fakeProc(pid: number, argv: string[]): void {
	const dir = join(procDir, String(pid));
	mkdirSync(dir, { recursive: true });
	writeFileSync(join(dir, "cmdline"), argv.join("\0") + "\0");
}

describe("/proc identity (parser-derived, full cmdlines)", () => {
	it("reads one process's FULL cmdline, dropping only the trailing NUL", () => {
		fakeProc(10, ["node", "/app/pi_gateway/run.ts"]);
		fakeProc(11, []); // kernel-thread-shaped: empty cmdline
		expect(readProcCmdline(10, procDir)).toBe("node /app/pi_gateway/run.ts");
		expect(readProcCmdline(11, procDir)).toBeNull();
		expect(readProcCmdline(9999, procDir)).toBeNull();
	});

	it("enumerates numeric entries only, matching on FULL cmdlines", () => {
		fakeProc(20, ["pi", "gateway", "run"]);
		fakeProc(21, ["bash", "-c", "tail -f gateway.log"]); // 'gateway' only inside a token
		mkdirSync(join(procDir, "self")); // non-numeric: ignored
		writeFileSync(join(procDir, "notapid"), ""); // non-numeric: ignored
		// Enumeration lists EVERY readable identity — classification is not its job.
		const found = listProcIdentities(procDir);
		expect(found.map((p) => p.pid)).toEqual([20, 21]);
		// Discovery applies the canonical matcher + liveness on top.
		expect(
			discoverLiveGateways({ procRoot: procDir, liveness: () => true }).map(
				(g) => g.pid,
			),
		).toEqual([20]);
	});

	it("adversarial /proc matrix — mimic-named processes are NOT gateways", () => {
		fakeProc(30, ["./fake-pi-gateway", "run"]); // mimic basename, no pi entry semantics
		fakeProc(31, ["grep", "gateway run", "notes.txt"]);
		fakeProc(32, ["pi", "--profile", "gateway", "dashboard"]); // value named gateway
		fakeProc(33, ["pi", "gateway", "run"]); // the ONE real unit
		const live = discoverLiveGateways({
			procRoot: procDir,
			liveness: () => true,
		});
		expect(live.map((g) => g.pid)).toEqual([33]);
	});

	it("applies the caller's liveness probe after identity (dead units drop out)", () => {
		fakeProc(40, ["pi", "gateway", "run"]);
		const live = discoverLiveGateways({
			procRoot: procDir,
			liveness: () => false,
		});
		expect(live).toEqual([]);
	});
});
