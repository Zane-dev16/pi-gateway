// entrypoints/setup-cli — terminal `pi-gateway setup` (no chat state, no
// TUI). Arg parsing and exit codes live here; the platform table lives in
// pi_gateway/setup/platforms and the ordered prompt flows in
// pi_gateway/setup/prompts. The writer stays the sole env writer and the
// single write happens only after a platform flow completes, so reruns and
// aborts converge. Secret values never reach print lines or error strings.

import { validateTelegramToken } from "./setup-wiring.js";
import {
	defaultAllowlistMirrorForHome,
} from "../pi_gateway/security/authz/env-mirror.js";
import {
	allowAllVarFor,
	filterPlatforms,
	PLATFORMS_ENV_VAR,
	PLATFORM_REGISTRY,
	statusOf,
	telegramTokenFormatOk,
	type PlatformSpec,
} from "../pi_gateway/setup/platforms.js";
import {
	PromptTerminal,
	runPlatformFlow,
	type FlowDeps,
	type LiveCheck,
} from "../pi_gateway/setup/prompts.js";
import {
	readSetupVar,
	writeSetupVars,
} from "../pi_gateway/setup/writer.js";

export interface SetupCommandResult {
	readonly exitCode: number;
	readonly output: string[];
}

export interface SetupCliDeps {
	readonly isTTY?: boolean | undefined;
	readonly createTerm?: (() => PromptTerminal) | undefined;
	readonly validateTelegram?: LiveCheck | undefined;
	readonly validateDiscord?: LiveCheck | undefined;
}

interface SetupArgs {
	platform?: string | undefined;
	token?: string | undefined;
	allowlist?: string | undefined;
	homeChannel?: string | undefined;
	yes: boolean;
	list: boolean;
	status: boolean;
	home?: string | undefined;
	allowOpen: boolean;
	vars: Map<string, string>;
	unknown?: string | undefined;
}

function parseSetupArgs(argv: string[]): SetupArgs {
	const args: SetupArgs = {
		yes: false,
		list: false,
		status: false,
		allowOpen: false,
		vars: new Map(),
	};
	for (let i = 0; i < argv.length; i++) {
		const token = argv[i] as string;
		const next = argv[i + 1];
		const take = (): string | undefined => {
			i++;
			return next;
		};
		if (token === "--platform") args.platform = take();
		else if (token.startsWith("--platform=")) {
			args.platform = token.slice("--platform=".length);
		} else if (token === "--token") args.token = take();
		else if (token.startsWith("--token=")) {
			args.token = token.slice("--token=".length);
		} else if (token === "--allowlist") args.allowlist = take();
		else if (token.startsWith("--allowlist=")) {
			args.allowlist = token.slice("--allowlist=".length);
		} else if (token === "--home-channel") args.homeChannel = take();
		else if (token.startsWith("--home-channel=")) {
			args.homeChannel = token.slice("--home-channel=".length);
		} else if (token === "--home") args.home = take();
		else if (token.startsWith("--home=")) {
			args.home = token.slice("--home=".length);
		} else if (token === "--var") {
			const pair = take() ?? "";
			const eq = pair.indexOf("=");
			if (eq <= 0) {
				args.unknown = "--var";
				break;
			}
			args.vars.set(pair.slice(0, eq).trim(), pair.slice(eq + 1).trim());
		} else if (token.startsWith("--var=")) {
			const pair = token.slice("--var=".length);
			const eq = pair.indexOf("=");
			if (eq <= 0) {
				args.unknown = "--var";
				break;
			}
			args.vars.set(pair.slice(0, eq).trim(), pair.slice(eq + 1).trim());
		} else if (token === "--yes") args.yes = true;
		else if (token === "--list") args.list = true;
		else if (token === "--status") args.status = true;
		else if (token === "--allow-open") args.allowOpen = true;
		else {
			args.unknown = token;
			break;
		}
	}
	return args;
}

export function setupUsage(): string {
	return [
		"usage: pi-gateway setup [--home <dir>] [--list] [--status]",
		"       pi-gateway setup --platform <name> --yes --token <value>",
		"         [--allowlist <ids> --allow-open] [--home-channel <id>] [--var KEY=VAL]",
		"Set up one platform per round; no args opens the picker.",
	].join("\n");
}

/** Real Discord token check: GET /users/@me as the bot. Never echoes token. */
export async function validateDiscordToken(
	token: string,
): Promise<{ readonly ok: true } | { readonly ok: false; readonly error: string }> {
	const trimmed = token.trim();
	if (trimmed === "") return { ok: false, error: "bot token is required" };
	try {
		const res = await fetch("https://discord.com/api/v10/users/@me", {
			headers: { Authorization: `Bot ${trimmed}` },
			signal: AbortSignal.timeout(15000),
		});
		if (res.ok) return { ok: true };
		if (res.status === 401 || res.status === 403) {
			return {
				ok: false,
				error: "discord: validation failed (unauthorized — check the bot token)",
			};
		}
		return {
			ok: false,
			error: `discord: validation failed (HTTP ${String(res.status)})`,
		};
	} catch (err) {
		return {
			ok: false,
			error: `discord: validation failed (${(err as Error)?.message ?? String(err)})`,
		};
	}
}

const defaultTelegramCheck: LiveCheck = (token) => validateTelegramToken(token);

interface Ctx {
	home: string;
	print: (line: string) => void;
	lines: string[];
	read: (name: string) => string | undefined;
	specs: readonly PlatformSpec[];
	warnings: readonly string[];
	deps: SetupCliDeps;
}

function listLine(spec: PlatformSpec): string {
	return `${spec.name} — requires ${spec.required.map((v) => v.name).join(", ")}`;
}

function statusLines(spec: PlatformSpec, read: (name: string) => string | undefined): string[] {
	const report = statusOf(spec, read);
	if (report.missing.length === 0) return [`${spec.name}: ${report.status}`];
	return [
		`${spec.name}: ${report.status} (missing: ${report.missing.join(", ")})`,
	];
}

/** Single write at completion. Empty values remove the var instead. */
async function commitValues(
	home: string,
	values: ReadonlyMap<string, string>,
): Promise<string[]> {
	const writes = new Map<string, string>();
	const clears: string[] = [];
	for (const [name, value] of values) {
		if (value === "") clears.push(name);
		else writes.set(name, value);
	}
	if (writes.size > 0) await writeSetupVars(home, writes);
	if (clears.length > 0) {
		const mirror = defaultAllowlistMirrorForHome(home);
		for (const name of clears) {
			mirror.removeVar(name);
			delete process.env[name];
		}
	}
	return [...writes.keys(), ...clears.map((name) => `${name} (cleared)`)];
}

async function runNonInteractive(
	args: SetupArgs,
	ctx: Ctx,
): Promise<SetupCommandResult> {
	const fail = (line: string): SetupCommandResult => {
		ctx.print(line);
		ctx.print(setupUsage());
		return { exitCode: 2, output: ctx.lines };
	};
	const name = (args.platform ?? "").trim().toLowerCase();
	if (name === "") return fail("setup: --platform is required with --yes");
	const spec = ctx.specs.find((s) => s.name === name);
	if (spec === undefined) {
		const known = PLATFORM_REGISTRY.some((s) => s.name === name);
		return fail(
			known
				? `setup: ${name} is filtered out by ${PLATFORMS_ENV_VAR}`
				: `setup: unknown platform ${JSON.stringify(name)}`,
		);
	}
	const values = new Map<string, string>();
	for (const v of spec.required) {
		const fromVar = args.vars.get(v.name);
		const direct =
			spec.live !== "manual" && v.password === true ? args.token : undefined;
		const value = (fromVar ?? direct ?? "").trim();
		if (value === "") return fail(`setup: ${v.name} is required with --yes`);
		values.set(v.name, value);
	}
	if (spec.live === "telegram") {
		const token = values.get("TELEGRAM_BOT_TOKEN") ?? "";
		if (!telegramTokenFormatOk(token)) return fail("setup: Invalid token format");
		const live =
			ctx.deps.validateTelegram ?? defaultTelegramCheck;
		const checked = await live(token);
		if (!checked.ok) {
			ctx.print(checked.error);
			return { exitCode: 1, output: ctx.lines };
		}
	}
	if (spec.live === "discord") {
		const live = ctx.deps.validateDiscord ?? validateDiscordToken;
		const checked = await live(values.get("DISCORD_BOT_TOKEN") ?? "");
		if (!checked.ok) {
			ctx.print(checked.error);
			return { exitCode: 1, output: ctx.lines };
		}
	}
	const allowlist = (args.allowlist ?? "").trim();
	if (spec.allowlistVar !== undefined) {
		if (allowlist !== "") {
			values.set(spec.allowlistVar, allowlist);
		} else if (args.allowOpen) {
			const flag = allowAllVarFor(spec.name);
			if (flag !== undefined) values.set(flag, "true");
		} else {
			return fail(
				"setup: empty allowlist is rejected with --yes (pass --allowlist or --allow-open)",
			);
		}
	}
	const channel = (args.homeChannel ?? "").trim();
	if (channel !== "") {
		if (spec.homeChannelVar === undefined) {
			return fail(`setup: ${spec.name} has no home channel var`);
		}
		values.set(spec.homeChannelVar, channel);
	}
	const saved = await commitValues(ctx.home, values);
	ctx.print(`setup ${spec.name}: saved ${saved.join(", ")}`);
	return { exitCode: 0, output: ctx.lines };
}

async function runPickerLoop(ctx: Ctx): Promise<SetupCommandResult> {
	const term =
		ctx.deps.createTerm !== undefined
			? ctx.deps.createTerm()
			: new PromptTerminal(process.stdin, process.stdout, ctx.print);
	const deps: FlowDeps = {
		term,
		read: ctx.read,
		validateTelegram: ctx.deps.validateTelegram ?? defaultTelegramCheck,
		validateDiscord: ctx.deps.validateDiscord ?? validateDiscordToken,
	};
	try {
		for (;;) {
			for (const w of ctx.warnings) term.print(w);
			ctx.specs.forEach((spec, i) => {
				term.print(
					`${String(i + 1)}. ${spec.name} (${statusOf(spec, ctx.read).status})`,
				);
			});
			term.print(`${String(ctx.specs.length + 1)}. Done`);
			const answer = await term.ask("Choose a platform: ", {
				onSigint: "exit130",
			});
			if (term.exit130) return { exitCode: 130, output: ctx.lines };
			const trimmed = (answer ?? "").trim().toLowerCase();
			if (trimmed === "" || trimmed === "done" || trimmed === String(ctx.specs.length + 1)) {
				return { exitCode: 0, output: ctx.lines };
			}
			const n = Number(trimmed);
			const spec =
				Number.isInteger(n) && n >= 1 && n <= ctx.specs.length
					? (ctx.specs[n - 1] as PlatformSpec)
					: ctx.specs.find((s) => s.name === trimmed);
			if (spec === undefined) {
				term.print(`unknown platform ${JSON.stringify(trimmed ?? "")} — try again`);
				continue;
			}
			const outcome = await runPlatformFlow(spec, deps);
			if (term.exit130) return { exitCode: 130, output: ctx.lines };
			if (!outcome.completed) continue;
			const saved = await commitValues(ctx.home, outcome.values);
			term.print(`setup ${spec.name}: saved ${saved.join(", ")}`);
			for (const note of outcome.notes) term.print(note);
		}
	} finally {
		if (ctx.deps.createTerm !== undefined) term.close();
	}
}

/** `pi-gateway setup` — argv in, exit code plus printed lines out. */
export async function runSetupCommand(
	opts: { home: string; argv: string[]; out?: (line: string) => void },
	deps: SetupCliDeps = {},
): Promise<SetupCommandResult> {
	const lines: string[] = [];
	const print = (line: string): void => {
		lines.push(line);
		(opts.out ?? ((l: string) => console.log(l)))(line);
	};
	const args = parseSetupArgs(opts.argv);
	const home = args.home !== undefined && args.home !== "" ? args.home : opts.home;
	if (args.unknown !== undefined) {
		print(`setup: unknown flag ${JSON.stringify(args.unknown)}`);
		print(setupUsage());
		return { exitCode: 2, output: lines };
	}
	const read = (name: string): string | undefined => readSetupVar(home, name);
	const { specs, warnings } = filterPlatforms(read(PLATFORMS_ENV_VAR));
	const ctx: Ctx = {
		home,
		print,
		lines,
		read,
		specs,
		warnings,
		deps,
	};
	if (args.list) {
		for (const w of warnings) print(w);
		for (const spec of specs) print(listLine(spec));
		return { exitCode: 0, output: lines };
	}
	if (args.status) {
		for (const w of warnings) print(w);
		for (const spec of specs) {
			for (const line of statusLines(spec, read)) print(line);
		}
		return { exitCode: 0, output: lines };
	 }
	if (args.yes) return runNonInteractive(args, ctx);
	const tty = deps.isTTY ?? process.stdin.isTTY === true;
	if (!tty) {
		print("setup: interactive prompts require a TTY (use --yes for non-interactive)");
		print(setupUsage());
		return { exitCode: 2, output: lines };
	}
	if (specs.length === 0) {
		print(`setup: no platforms available (${PLATFORMS_ENV_VAR} filters everything)`);
		return { exitCode: 2, output: lines };
	}
	return runPickerLoop(ctx);
}
