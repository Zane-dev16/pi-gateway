// entrypoints/pi-gateway.ts — standalone `pi-gateway` CLI (DEC-080).
//
// Packaging plus ownership, not a new loop: `run` composes the SAME
// composition root the extension uses (composeGatewayLifecycle via
// runGateway — DEC-023 stands, nothing reimplemented) with its own
// lifecycle (PID file, runtime lock, state.db open/repair, log files).
// The extension stays a thin control shim; dual boot is guarded by the
// runtime lock — a second `run` against the same home exits 0 with a
// lock-held message instead of double-polling.
//
// Strip-safe posture: platform-hosting and production-runner statically
// import adapter chains that bare-node runners cannot parse, so this
// module loads them LAZILY (dynamic import) only when a platform
// allowlist is configured. Lock/status/stop and platform-less runs stay
// runnable under the repo's spawned-child TS path.

import { appendFileSync, mkdirSync } from "node:fs";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { resolvePiHome } from "../pi_home.js";
import {
	getRunningPid,
	isRuntimeLockActive,
	readPidFile,
} from "../pi_gateway/lifecycle/instance-guard.js";
import { writePlannedStopMarker } from "../pi_gateway/lifecycle/markers.js";
import type { Logger } from "../pi_gateway/lifecycle/index.js";
import type {
	ComposedGateway,
	GatewayRunInput,
	PlatformHosting,
	StartupOkHook,
} from "./gateway-run.js";
import type { TurnRunnerFactory } from "./guard-wiring.js";

/** Stable lock-held line — the dual-boot contract (DEC-080). */
export const LOCK_HELD_MESSAGE = "gateway already running — lock held, exiting";

/** One typed record over scattered argv locals (not three string params). */
export interface PiGatewayCliOptions {
	home: string;
	replace?: boolean;
	out?: (line: string) => void;
}

export interface CliResult {
	exitCode: number;
	output: string[];
}

/** Test/production overrides for the run path (same shape as GatewayRunInput). */
export interface RunOverrides {
	platforms?: readonly PlatformHosting[];
	turnRunnerFactory?: TurnRunnerFactory;
	secretReader?: (name: string) => string | undefined;
	logger?: Logger;
	installSignals?: boolean;
	onStartupOk?: StartupOkHook;
}

function printer(
	out: ((line: string) => void) | undefined,
	lines: string[],
): (line: string) => void {
	return (line: string) => {
		lines.push(line);
		(out ?? ((l: string) => console.log(l)))(line);
	};
}

function stderrSink(): Logger {
	const line = (level: string, message: string) => {
		try {
			process.stderr.write(`[pi-gateway] ${level} ${message}\n`);
		} catch {
			/* stderr unavailable */
		}
	};
	return {
		info: (m) => line("INFO", m),
		warn: (m) => line("WARN", m),
		error: (m) => line("ERROR", m),
	};
}

/**
 * Smallest log-file seam: tee lifecycle lines into
 * <home>/logs/gateway.log while still forwarding to the sink. File
 * writes are best-effort — logging must never crash the gateway.
 */
export function fileTeeLogger(home: string, sink?: Logger): Logger {
	const base = sink ?? stderrSink();
	const write = (level: string, message: string) => {
		try {
			mkdirSync(join(home, "logs"), { recursive: true });
			appendFileSync(
				join(home, "logs", "gateway.log"),
				`${new Date().toISOString()} ${level} ${message}\n`,
			);
		} catch {
			/* logging never crashes the gateway */
		}
	};
	return {
		info: (m, meta) => {
			write("INFO", m);
			base.info(m, meta);
		},
		warn: (m, meta) => {
			write("WARN", m);
			base.warn(m, meta);
		},
		error: (m, meta) => {
			write("ERROR", m);
			base.error(m, meta);
		},
	};
}

/**
 * Standalone boot: compose the production stack and park via runGateway.
 * A live instance (or a lock-held startup abort — the pre-check race) maps
 * to exit 0 with the lock-held message, never double-polling. A failure
 * that is NOT lock contention keeps runGateway's own exit code.
 */
export async function runCommand(
	opts: PiGatewayCliOptions,
	overrides: RunOverrides = {},
): Promise<CliResult> {
	const lines: string[] = [];
	const print = printer(opts.out, lines);

	if (opts.replace !== true) {
		const live = getRunningPid(opts.home, { cleanupStale: false });
		if (live !== null) {
			print(`${LOCK_HELD_MESSAGE} (pid=${live.pid})`);
			return { exitCode: 0, output: lines };
		}
	}

	const logger = fileTeeLogger(opts.home, overrides.logger);
	const input: GatewayRunInput = {
		home: opts.home,
		logger,
		...(opts.replace === true ? { replace: true as const } : {}),
		...(overrides.secretReader !== undefined
			? { secretReader: overrides.secretReader }
			: {}),
		...(overrides.installSignals !== undefined
			? { installSignals: overrides.installSignals }
			: {}),
	};

	if (overrides.platforms !== undefined) {
		if (overrides.platforms.length > 0)
			input.platforms = [...overrides.platforms];
		if (overrides.turnRunnerFactory !== undefined)
			input.turnRunnerFactory = overrides.turnRunnerFactory;
	} else {
		// Lazy census load (see module header): no allowlist ⇒ no adapter
		// chains enter the process at all.
		const hosting = await import("./platform-hosting.js");
		const raw = (process.env[hosting.PI_GATEWAY_PLATFORMS_ENV] ?? "").trim();
		if (raw !== "") {
			const platforms = hosting.resolveConfiguredPlatforms(raw);
			if (platforms.length > 0) {
				const runner = await import("./production-runner.js");
				input.platforms = platforms;
				input.turnRunnerFactory = runner.buildProductionTurnRunnerFactory({
					home: opts.home,
				});
			}
		}
	}

	const extraHook = overrides.onStartupOk;
	input.onStartupOk = async (
		gateway: ComposedGateway,
		result: Parameters<StartupOkHook>[1],
	) => {
		print(
			`gateway running — home=${gateway.lifecycle.home} platforms=[${[...gateway.connectedPlatforms()].join(",") || "none"}]`,
		);
		if (extraHook !== undefined) await extraHook(gateway, result);
	};

	const result = await (await import("./gateway-run.js")).runGateway(input);
	if (!result.ran) {
		const live = getRunningPid(opts.home, { cleanupStale: false });
		if (
			(live !== null && live.pid !== process.pid) ||
			isRuntimeLockActive(opts.home)
		) {
			print(
				live !== null && live.pid !== process.pid
					? `${LOCK_HELD_MESSAGE} (pid=${live.pid})`
					: LOCK_HELD_MESSAGE,
			);
			return { exitCode: 0, output: lines };
		}
		print(`gateway failed to start (exit ${result.exitCode})`);
		return { exitCode: result.exitCode, output: lines };
	}
	return { exitCode: result.exitCode, output: lines };
}

/** Status reads the PID file + lock liveness without starting anything. */
export function statusCommand(opts: PiGatewayCliOptions): CliResult {
	const lines: string[] = [];
	const print = printer(opts.out, lines);
	const live = getRunningPid(opts.home, { cleanupStale: false });
	if (live !== null) {
		print(
			`gateway: running — pid=${live.pid} home=${opts.home} lock=${isRuntimeLockActive(opts.home) ? "held" : "released"}`,
		);
		return { exitCode: 0, output: lines };
	}
	const record = readPidFile(opts.home);
	print(
		record !== null
			? `gateway: not running (stale pid file, pid=${record.pid})`
			: "gateway: not running",
	);
	return { exitCode: 0, output: lines };
}

/** Stop marks the stop planned BEFORE signalling, then SIGTERMs the recorded PID. */
export function stopCommand(opts: PiGatewayCliOptions): CliResult {
	const lines: string[] = [];
	const print = printer(opts.out, lines);
	const live = getRunningPid(opts.home, { cleanupStale: false });
	if (live === null) {
		print("gateway: not running");
		return { exitCode: 0, output: lines };
	}
	writePlannedStopMarker(opts.home, live.pid);
	try {
		process.kill(live.pid, "SIGTERM");
	} catch (err) {
		if ((err as NodeJS.ErrnoException).code === "ESRCH") {
			print("gateway: not running (process already exited)");
			return { exitCode: 0, output: lines };
		}
		throw err;
	}
	print(`gateway: stop signalled — pid=${live.pid}`);
	return { exitCode: 0, output: lines };
}

// ── argv surface ─────────────────────────────────────────────────────────────

export interface ParsedCli {
	command: string;
	homeFlag?: string;
	replace: boolean;
}

export function parseCliArgs(argv: string[]): ParsedCli {
	let command = "";
	let homeFlag: string | undefined;
	let replace = false;
	for (let i = 0; i < argv.length; i++) {
		const token = argv[i];
		if (token === undefined) continue;
		if (i === 0 && !token.startsWith("--")) {
			command = token;
			continue;
		}
		if (token === "--home") {
			homeFlag = argv[i + 1] ?? "";
			i++;
		} else if (token.startsWith("--home=")) {
			homeFlag = token.slice("--home=".length);
		} else if (token === "--replace") {
			replace = true;
		}
	}
	return {
		command,
		...(homeFlag !== undefined ? { homeFlag } : {}),
		replace,
	};
}

function usageLine(): string {
	return "usage: pi-gateway <run|status|stop> [--home <dir>] [--replace]";
}

type CommandFn = (
	opts: PiGatewayCliOptions,
	overrides: RunOverrides,
) => Promise<CliResult> | CliResult;

const COMMANDS: Record<string, CommandFn> = {
	run: (opts, overrides) => runCommand(opts, overrides),
	status: (opts) => statusCommand(opts),
	stop: (opts) => stopCommand(opts),
};

/** `pi-gateway run|status|stop` — argv in, exit code plus printed lines out. */
export async function piGatewayMain(
	argv: string[],
	ctx: { out?: (line: string) => void; overrides?: RunOverrides } = {},
): Promise<CliResult> {
	const parsed = parseCliArgs(argv);
	const fn = COMMANDS[parsed.command];
	if (fn === undefined) {
		const lines = [usageLine()];
		(ctx.out ?? ((l: string) => console.log(l)))(lines[0] as string);
		return { exitCode: 2, output: lines };
	}
	const home =
		parsed.homeFlag !== undefined && parsed.homeFlag !== ""
			? parsed.homeFlag
			: resolvePiHome();
	const opts: PiGatewayCliOptions = {
		home,
		...(parsed.replace ? { replace: true as const } : {}),
		...(ctx.out !== undefined ? { out: ctx.out } : {}),
	};
	return fn(opts, ctx.overrides ?? {});
}

const invokedAsMain =
	process.argv[1] !== undefined &&
	import.meta.url === pathToFileURL(process.argv[1]).href;
if (invokedAsMain) {
	piGatewayMain(process.argv.slice(2)).then(
		(result) => {
			process.exitCode = result.exitCode;
		},
		(err) => {
			console.error(`pi-gateway: ${String(err)}`);
			process.exitCode = 1;
		},
	);
}
