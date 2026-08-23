// pi_gateway/commands/derived.ts — every consumer DERIVES from the one
// registry (07 §1.2). Zero per-surface hardcoded lists: adding a row or an
// alias touches exactly the registry and every surface below reflects it on
// its next call.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   hermes_cli/commands.py:_build_description      → buildCliDescription
//   hermes_cli/commands.py:COMMANDS / COMMANDS_BY_CATEGORY
//                                                  → cliCommandDescriptions /
//                                                    cliCommandsByCategory
//   hermes_cli/commands.py:SUBCOMMANDS (+ _PIPE_SUBS_RE)
//                                                  → completionCatalog
//   hermes_cli/commands.py:GATEWAY_KNOWN_COMMANDS  → gatewayKnownCommands
//   hermes_cli/commands.py:is_gateway_known_command→ isGatewayKnownCommand
//   hermes_cli/commands.py:gateway_help_lines      → gatewayHelpLines
//   hermes_cli/commands.py:telegram_bot_commands / _sanitize_telegram_name /
//     _requires_argument                           → telegramMenuModel /
//                                                    sanitizeTelegramName /
//                                                    requiresArgument

import {
	type CommandDef,
	aliasesOf,
	argsHintOf,
	subcommandsOf,
} from "./command-def.js";
import { type RawConfigReader, isGatewayAvailable } from "./config-gates.js";

// -- gateway known-command set ----------------------------------------------

/**
 * commands.py:GATEWAY_KNOWN_COMMANDS — names + aliases of every row where
 * `not cli_only or gateway_config_gate`. Config-GATED commands are ALWAYS in
 * this set (routable); visibility on surfaces is a separate, gate-checked
 * decision. Unknown "/foo" is not a member ⇒ queues as plain text.
 */
export function gatewayKnownCommands(
	rows: readonly CommandDef[],
): ReadonlySet<string> {
	const names = new Set<string>();
	for (const cmd of rows) {
		if (!cmd.cliOnly || cmd.gatewayConfigGate) {
			names.add(cmd.name);
			for (const alias of aliasesOf(cmd)) names.add(alias);
		}
	}
	return names;
}

/** commands.py:is_gateway_known_command. Plugin entries join later via the same rails (07 §1.5). */
export function isGatewayKnownCommand(
	known: ReadonlySet<string>,
	name: string | null | undefined,
): boolean {
	if (!name) return false;
	return known.has(name.toLowerCase().replace(/^\/+/, ""));
}

// -- help renderer -----------------------------------------------------------

function internalAlias(alias: string, name: string): boolean {
	// Skip internal aliases like reload_mcp (underscore variant of the name).
	return alias.replace(/-/g, "_") === name.replace(/-/g, "_") && alias !== name;
}

/**
 * commands.py:gateway_help_lines — one `` `/name [args]` -- description ``
 * line per gateway-available row, alias note appended for non-internal
 * aliases. Gate overrides shared across all consumers come from ONE
 * resolveConfigGates call.
 */
export function gatewayHelpLines(
	rows: readonly CommandDef[],
	options: {
		configOverrides?: Set<string> | null;
		readRawConfig?: RawConfigReader;
	} = {},
): string[] {
	const lines: string[] = [];
	for (const cmd of rows) {
		if (
			!isGatewayAvailable(cmd, options.configOverrides, options.readRawConfig)
		) {
			continue;
		}
		const args = argsHintOf(cmd) ? ` ${argsHintOf(cmd)}` : "";
		const aliasParts: string[] = [];
		for (const alias of aliasesOf(cmd)) {
			if (internalAlias(alias, cmd.name)) continue;
			aliasParts.push(`\`/${alias}\``);
		}
		const aliasNote =
			aliasParts.length > 0 ? ` (alias: ${aliasParts.join(", ")})` : "";
		lines.push(`\`/${cmd.name}${args}\` -- ${cmd.description}${aliasNote}`);
	}
	return lines;
}

// -- CLI-facing catalog (COMMANDS / COMMANDS_BY_CATEGORY parity) --------------

/** commands.py:_build_description — usage hint folded into the description. */
export function buildCliDescription(cmd: CommandDef): string {
	const argsHint = argsHintOf(cmd);
	return argsHint
		? `${cmd.description} (usage: /${cmd.name} ${argsHint})`
		: cmd.description;
}

/**
 * commands.py:COMMANDS — flat map "/command" → description for every
 * non-gateway_only row; aliases map to "<description> (alias for /name)".
 * This is the CLI surface: cli_only rows appear regardless of gateway gates.
 */
export function cliCommandDescriptions(
	rows: readonly CommandDef[],
): Map<string, string> {
	const catalog = new Map<string, string>();
	for (const cmd of rows) {
		if (cmd.gatewayOnly) continue;
		catalog.set(`/${cmd.name}`, buildCliDescription(cmd));
		for (const alias of aliasesOf(cmd)) {
			catalog.set(`/${alias}`, `${cmd.description} (alias for /${cmd.name})`);
		}
	}
	return catalog;
}

/** commands.py:COMMANDS_BY_CATEGORY — the same entries grouped by category. */
export function cliCommandsByCategory(
	rows: readonly CommandDef[],
): Map<string, Map<string, string>> {
	const byCategory = new Map<string, Map<string, string>>();
	for (const [command, description] of cliCommandDescriptions(rows)) {
		const name = command.slice(1);
		const cmd = rows.find((c) => c.name === name || c.aliases?.includes(name));
		if (cmd === undefined) continue;
		let bucket = byCategory.get(cmd.category);
		if (bucket === undefined) {
			bucket = new Map();
			byCategory.set(cmd.category, bucket);
		}
		bucket.set(command, description);
	}
	return byCategory;
}

// -- completions --------------------------------------------------------------

export interface CompletionCatalog {
	/** Flat slash-command keys ("/new", "/reset", …) in derivation order. */
	commands: string[];
	/** "/cmd" → tab-completable subcommands. */
	subcommands: Map<string, string[]>;
}

export type CompletionSurface = "cli" | "gateway";

/** commands.py:_PIPE_SUBS_RE — "[on|off|tts]" style hints become subcommands. */
const PIPE_SUBS_RE = /[a-z]+(?:\|[a-z]+)+/;

export function subcommandsFor(cmd: CommandDef): string[] {
	const explicit = subcommandsOf(cmd);
	if (explicit.length > 0) return [...explicit];
	if (!argsHintOf(cmd)) return [];
	const match = PIPE_SUBS_RE.exec(argsHintOf(cmd));
	return match === null ? [] : match[0].split("|");
}

/**
 * Autocomplete/completion provider derived from the registry.
 *
 * - surface "cli": every non-gateway_only row, canonical + alias keys
 *   (commands.py COMMANDS dict feeding SlashCommandCompleter).
 * - surface "gateway": only gateway-AVAILABLE rows (gate-aware, sharing the
 *   one predicate with help/menus — commands.catalog minus gateway-only).
 */
export function completionCatalog(
	rows: readonly CommandDef[],
	options: {
		surface?: CompletionSurface;
		configOverrides?: Set<string> | null;
		readRawConfig?: RawConfigReader;
	} = {},
): CompletionCatalog {
	const surface = options.surface ?? "gateway";
	const commands: string[] = [];
	const subcommands = new Map<string, string[]>();
	for (const cmd of rows) {
		if (
			surface === "gateway" &&
			!isGatewayAvailable(cmd, options.configOverrides, options.readRawConfig)
		) {
			continue;
		}
		if (surface === "cli" && cmd.gatewayOnly) continue;
		commands.push(`/${cmd.name}`);
		for (const alias of aliasesOf(cmd)) commands.push(`/${alias}`);
		const subs = subcommandsFor(cmd);
		if (subs.length > 0) subcommands.set(`/${cmd.name}`, subs);
	}
	return { commands, subcommands };
}

// -- menu model builder --------------------------------------------------------

export interface MenuCommand {
	/** Telegram-shaped command name (sanitized, no leading slash). */
	command: string;
	description: string;
}

/** A plugin-registered command entry riding the same menu rails (07 §1.5). */
export interface PluginMenuEntry {
	name: string;
	description: string;
	argsHint?: string;
}

/**
 * commands.py:_sanitize_telegram_name — lowercase → hyphens to underscores →
 * strip everything outside [a-z0-9_] → collapse runs → trim underscores.
 */
export function sanitizeTelegramName(raw: string): string {
	return raw
		.toLowerCase()
		.replace(/-/g, "_")
		.replaceAll(/[^a-z0-9_]/g, "")
		.replaceAll(/_{2,}/g, "_")
		.replace(/^_+|_+$/g, "");
}

/** commands.py:_requires_argument — bare selection would be incomplete. */
export function requiresArgument(argsHint: string | undefined): boolean {
	return (argsHint ?? "").trim().startsWith("<");
}

/**
 * commands.py:telegram_bot_commands — Telegram-menu-SHAPED data with NO
 * Telegram import. One entry per canonical available row (aliases skipped);
 * built-in arg-taking commands stay INCLUDED (#24312 discoverability), while
 * plugin entries that require arguments are EXCLUDED (no guaranteed no-arg
 * usage fallback). Empty sanitized names drop out.
 */
export function telegramMenuModel(
	rows: readonly CommandDef[],
	options: {
		configOverrides?: Set<string> | null;
		readRawConfig?: RawConfigReader;
		pluginEntries?: readonly PluginMenuEntry[];
	} = {},
): MenuCommand[] {
	const menu: MenuCommand[] = [];
	for (const cmd of rows) {
		if (
			!isGatewayAvailable(cmd, options.configOverrides, options.readRawConfig)
		) {
			continue;
		}
		const tgName = sanitizeTelegramName(cmd.name);
		if (tgName) menu.push({ command: tgName, description: cmd.description });
	}
	for (const plugin of options.pluginEntries ?? []) {
		if (requiresArgument(plugin.argsHint)) continue;
		const tgName = sanitizeTelegramName(plugin.name);
		if (tgName) {
			menu.push({ command: tgName, description: plugin.description });
		}
	}
	return menu;
}
