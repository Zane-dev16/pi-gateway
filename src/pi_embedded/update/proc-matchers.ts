// proc-matchers.ts — canonical process identity, parser-derived (08 §9).
//
// ~10 production bugs in the reference came from argv substring scans; the
// binding rules here are absolute:
//   - NEVER classify by `"subcommand" in cmdline` — flag VALUES can equal
//     subcommands (`-m dashboard serve`), truncated cmdlines hide truth.
//   - Match FULL cmdlines; truncate at display time only.
//
// Hermes anchors (READ-ONLY reference; semantics ported):
//   gateway/status.py:_gateway_command_subcommand
//                                    → gatewayCommandSubcommand
//   gateway/status.py:looks_like_gateway_command_line
//                                    → looksLikeGatewayCommandLine (strict)
//   gateway/status.py:looks_like_gateway_runtime_command_line
//                                    → looksLikeGatewayRuntimeCommandLine
//   hermes_cli/update_cmd.py:_hermes_holder_subcommand
//                                    → piHolderSubcommand
//   hermes_cli/update_cmd.py:_holder_value_flags
//                                    → holderValueFlags — DERIVED from the
//     declared option surface (TOP_LEVEL_OPTION_SPECS), never a handwritten
//     subset: every option with takesValue:true contributes its flags. In
//     TS there is no argparse object to introspect, so this repo's option
//     table IS the parser definition the matcher derives from — one source
//     of truth, same drift-proofing property (#91869).

import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";

/** Quote-aware tokenizer (parity of shlex.split(posix=False) + per-token strip). */
export function tokenizeCommandLine(command: string): string[] {
	const tokens: string[] = [];
	let current = "";
	let hasToken = false;
	let quote: '"' | "'" | null = null;
	for (const ch of command) {
		if (quote !== null) {
			if (ch === quote) {
				quote = null;
			} else {
				current += ch;
			}
			continue;
		}
		if (ch === '"' || ch === "'") {
			quote = ch;
			hasToken = true;
			continue;
		}
		if (/\s/.test(ch)) {
			if (hasToken) tokens.push(current);
			current = "";
			hasToken = false;
			continue;
		}
		current += ch;
		hasToken = true;
	}
	if (hasToken) tokens.push(current);
	return tokens;
}

function normalizeToken(token: string): string {
	return token
		.replace(/^["']+/, "")
		.replace(/["']+$/, "")
		.replace(/\\/g, "/")
		.toLowerCase();
}

const GATEWAY_DEDICATED_BASENAMES = new Set(["pi-gateway", "pi-gateway.exe"]);

function basenameOf(token: string): string {
	return token.replace(/\\/g, "/").split("/").pop() as string;
}

/**
 * The actual gateway SUBCOMMAND a command line runs ("run" | "restart" |
 * ...), or null. Strict public predicate material — see status.py anchor.
 */
export function gatewayCommandSubcommand(
	command: string | null,
): string | null {
	if (!command) return null;
	const rawTokens = tokenizeCommandLine(command);
	const tokens = rawTokens.map(normalizeToken);
	if (tokens.length === 0) return null;

	// Gateway-dedicated entrypoints carry no subcommand to inspect.
	for (const token of tokens) {
		if (
			token === "pi_gateway/run.ts" ||
			token === "gateway/run.py" ||
			token.endsWith("/pi_gateway/run.ts") ||
			token.endsWith("/gateway/run.py")
		) {
			return "run";
		}
		const base = basenameOf(token);
		if (GATEWAY_DEDICATED_BASENAMES.has(base)) return "run";
	}

	const joined = tokens.join(" ");
	const hasGatewayEntry =
		joined.includes("pi_cli.main") ||
		joined.includes("pi_cli/main.ts") ||
		tokens.some((t) => {
			const base = basenameOf(t);
			return base === "pi" || base === "pi.exe";
		});
	if (!hasGatewayEntry) return null;

	// Drop profile selectors anywhere (--profile X / -p X / --profile=X / -p=X).
	// Consuming a profile VALUE of "gateway" too means the real subcommand is
	// what we land on below.
	const filtered: string[] = [];
	let skipNext = false;
	for (const token of tokens) {
		if (skipNext) {
			skipNext = false;
			continue;
		}
		if (token === "--profile" || token === "-p") {
			skipNext = true;
			continue;
		}
		if (token.startsWith("--profile=") || token.startsWith("-p=")) continue;
		filtered.push(token);
	}

	for (let i = 0; i < filtered.length; i++) {
		if (filtered[i] !== "gateway") continue;
		if (i + 1 >= filtered.length) return "run"; // bare `pi gateway` ⇒ run
		return filtered[i + 1] as string;
	}
	return null;
}

/** STRICT: only a real `gateway run` process command line. */
export function looksLikeGatewayCommandLine(command: string | null): boolean {
	return gatewayCommandSubcommand(command) === "run";
}

/**
 * Broader runtime variant kept SEPARATE (status.py parity): `gateway restart`
 * can host the runtime when no service manager executes the restart inline.
 * Used only for validating pi-owned runtime records / no-supervisor scans.
 */
export function looksLikeGatewayRuntimeCommandLine(
	command: string | null,
): boolean {
	return (
		gatewayCommandSubcommand(command) === "restart" ||
		looksLikeGatewayCommandLine(command)
	);
}

// --- Parser-derived flag-value sets ----------------------------------------

export interface OptionSpec {
	readonly flags: readonly string[];
	readonly takesValue: boolean;
}

/**
 * THE top-level option surface of this repo's CLI — the single source of
 * truth both the future CLI builder and this matcher derive from. Parity of
 * hermes_cli/_parser.py:build_top_level_parser adapted to pi naming; the
 * pre-argparse profile selectors ride along explicitly (parity comment in
 * _holder_value_flags: they are stripped before argparse sees argv).
 */
export const TOP_LEVEL_OPTION_SPECS: readonly OptionSpec[] = [
	{ flags: ["--profile", "-p"], takesValue: true },
	{ flags: ["--config"], takesValue: true },
	{ flags: ["--model", "-m"], takesValue: true },
	{ flags: ["--provider"], takesValue: true },
	{ flags: ["--reasoning"], takesValue: true },
	{ flags: ["--toolsets", "-t"], takesValue: true },
	{ flags: ["--skills", "-s"], takesValue: true },
	{ flags: ["--session", "--continue", "-c"], takesValue: true },
	{ flags: ["--resume", "-r"], takesValue: true },
	{ flags: ["--mode"], takesValue: true },
	{ flags: ["--help", "-h"], takesValue: false },
	{ flags: ["--version"], takesValue: false },
	{ flags: ["--verbose", "-v"], takesValue: false },
];

const PROFILE_PRE_ARGPARSE_SELECTORS = new Set(["--profile", "-p", "--config"]);

/** Flags that consume a value — DERIVED, never hand-listed at use sites. */
export function holderValueFlags(
	specs: readonly OptionSpec[] = TOP_LEVEL_OPTION_SPECS,
): ReadonlySet<string> {
	const flags = new Set<string>(PROFILE_PRE_ARGPARSE_SELECTORS);
	for (const spec of specs) {
		if (!spec.takesValue) continue;
		for (const flag of spec.flags) flags.add(flag);
	}
	return flags;
}

/**
 * The top-level subcommand any pi-holder argv runs, or NULL — callers must
// NOT guess a label when null (update_cmd.py docstring verbatim). Token-based,
 * never substring (#90778): finds the entry token (`-m pi_cli.main` or a
 * `pi` basename), then returns the first following token that is neither a
 * flag nor a flag's value, skipping profile selectors like the gateway
 * matcher does.
 */
export function piHolderSubcommand(command: string | null): string | null {
	if (!command) return null;
	let tokens: string[];
	try {
		tokens = tokenizeCommandLine(command);
	} catch {
		tokens = command.split(/\s+/).filter(Boolean);
	}
	let entryIdx: number | null = null;
	for (let i = 0; i < tokens.length; i++) {
		const low = tokens[i]!.toLowerCase().replace(/^"/, "").replace(/"$/, "");
		if (low.endsWith("pi_cli.main") && i > 0 && tokens[i - 1] === "-m") {
			entryIdx = i;
			break;
		}
		const base = low.replace(/\\/g, "/").split("/").pop() as string;
		if (base === "pi" || base === "pi.exe") {
			entryIdx = i;
			break;
		}
	}
	if (entryIdx === null) return null;

	const valueFlags = holderValueFlags();
	let i = entryIdx + 1;
	while (i < tokens.length) {
		const token = tokens[i]!;
		if (
			valueFlags.has(token) ||
			valueFlags.has(token.split("=", 1)[0] as string)
		) {
			// --flag value consumes two tokens; --flag=value consumes one.
			i += token.includes("=") ? 1 : 2;
			continue;
		}
		if (token.startsWith("-")) {
			i += 1;
			continue;
		}
		return token.toLowerCase();
	}
	return null;
}

// --- /proc identity ---------------------------------------------------------

/** Read ONE process's full cmdline from /proc (NUL-separated argv joined by spaces). */
export function readProcCmdline(
	pid: number,
	procRoot = "/proc",
): string | null {
	try {
		const raw = readFileSync(join(procRoot, String(pid), "cmdline"), "utf8");
		// Trailing NUL produces an empty tail segment — drop empties at the END
		// only (an empty middle segment would be argv[0] == "", not our business).
		const parts = raw.split("\0");
		while (parts.length > 0 && parts[parts.length - 1] === "") parts.pop();
		if (parts.length === 0) return null;
		return parts.join(" ");
	} catch {
		return null;
	}
}

export interface ProcessIdentity {
	pid: number;
	cmdline: string;
}

/**
 * Enumerate live processes with their FULL cmdlines from /proc data
 * structures. Identity classification happens ONLY through the canonical
 * predicates above — never here.
 */
export function listProcIdentities(procRoot = "/proc"): ProcessIdentity[] {
	if (!existsSync(procRoot)) return [];
	let entries: string[];
	try {
		entries = readdirSync(procRoot);
	} catch {
		return [];
	}
	const identities: ProcessIdentity[] = [];
	for (const name of entries.sort()) {
		if (!/^\d+$/.test(name)) continue;
		const pid = Number.parseInt(name, 10);
		const cmdline = readProcCmdline(pid, procRoot);
		if (cmdline !== null) identities.push({ pid, cmdline });
	}
	return identities;
}

/**
 * Live gateway units on this host, derived via the STRICT canonical matcher.
 * Fleet restart + pause machinery consume THIS — nothing else may decide
 * gateway-ness (08 §9).
 */
export function discoverLiveGateways(options?: {
	procRoot?: string;
	liveness?(pid: number): boolean;
}): Array<{ pid: number; profileHint: string | null }> {
	const liveness =
		options?.liveness ??
		((pid: number) => {
			try {
				process.kill(pid, 0);
				return true;
			} catch {
				return false;
			}
		});
	return listProcIdentities(options?.procRoot ?? "/proc")
		.filter((identity) => looksLikeGatewayCommandLine(identity.cmdline))
		.filter((identity) => liveness(identity.pid))
		.map((identity) => ({
			pid: identity.pid,
			profileHint: extractProfileHint(identity.cmdline),
		}));
}

/** Display-time-only extraction: --profile/-p VALUE if present, else null. */
function extractProfileHint(cmdline: string): string | null {
	const tokens = tokenizeCommandLine(cmdline).map((t) => t.toLowerCase());
	for (let i = 0; i < tokens.length; i++) {
		const token = tokens[i] as string;
		if (token === "--profile" || token === "-p") {
			return (tokens[i + 1] as string) ?? null;
		}
		if (token.startsWith("--profile=")) return token.slice("--profile=".length);
		if (token.startsWith("-p=")) return token.slice("-p=".length);
	}
	return null;
}
