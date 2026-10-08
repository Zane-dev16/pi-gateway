// pi_gateway/commands/builtins — the SHIPPED command rows (07 §1).
//
// DEC-078 derivation: rows derive from the HOST census
// (`BUILTIN_SLASH_COMMANDS` via pi_agent_core/host.js), not the Hermes
// COMMAND_REGISTRY. Names, descriptions, and args hints come from the host
// verbatim; this module adds only presentational category metadata,
// TUI-bound cliOnly flags, and the gateway mid-run dispatch overlay
// (busyPolicy/busyHandler) the guard machinery consumes. Gateway-only rows
// (no host meaning) are listed explicitly below — nothing Hermes-only
// ships. The registry MACHINERY lives in registry.ts; this module is the
// single built-and-frozen set gateway assembly consumes.

import { BUILTIN_SLASH_COMMANDS } from "../../pi_agent_core/host.js";
import type { BusyPolicy, CommandDef } from "./command-def.js";
import { CommandRegistry } from "./registry.js";

/** Presentational + dispatch overlay for one host builtin. */
interface HostOverlay {
	category: string;
	cliOnly?: true;
	aliases?: readonly string[];
	busyPolicy?: BusyPolicy;
	busyHandler?: string;
}

/**
 * Overlay per host builtin name. Category is presentational (the host
 * census carries none). cliOnly marks TUI-bound rows (menus, navigators,
 * interactive auth, process exit) that must stay out of gateway menus.
 * busyPolicy/busyHandler preserve the mid-run dispatch the guard needs
 * (new/model keep their Hermes-parity interrupt/reject behavior).
 */
const HOST_OVERLAYS: Readonly<Record<string, HostOverlay>> = {
	settings: { category: "Configuration", cliOnly: true },
	model: {
		category: "Configuration",
		busyPolicy: "reject",
		busyHandler: "model",
	},
	tree: { category: "Session", cliOnly: true },
	thinking: { category: "Configuration" },
	"scoped-models": { category: "Configuration", cliOnly: true },
	export: { category: "Session" },
	import: { category: "Session" },
	share: { category: "Session" },
	bug: { category: "Info" },
	copy: { category: "Info" },
	name: { category: "Session" },
	session: { category: "Session" },
	changelog: { category: "Info" },
	hotkeys: { category: "Configuration", cliOnly: true },
	fork: { category: "Session" },
	clone: { category: "Session" },
	trust: { category: "Configuration" },
	login: { category: "Configuration", cliOnly: true },
	logout: { category: "Configuration", cliOnly: true },
	new: {
		category: "Session",
		aliases: ["reset"],
		busyPolicy: "interrupt_then_dispatch",
		busyHandler: "new",
	},
	compact: { category: "Session" },
	resume: { category: "Session" },
	reload: { category: "Tools & Skills" },
	quit: { category: "Exit", cliOnly: true },
};

/** Host census projected onto CommandDef rows (names/descriptions verbatim). */
function hostRows(): CommandDef[] {
	return BUILTIN_SLASH_COMMANDS.map((cmd): CommandDef => {
		const overlay = HOST_OVERLAYS[cmd.name];
		if (overlay === undefined) {
			throw new Error(
				`host builtin /${cmd.name} has no DEC-078 overlay entry — add its category`,
			);
		}
		return {
			name: cmd.name,
			description: cmd.description,
			category: overlay.category,
			...(cmd.argumentHint !== undefined && cmd.argumentHint !== ""
				? { argsHint: cmd.argumentHint }
				: {}),
			...(overlay.aliases !== undefined
				? { aliases: [...overlay.aliases] }
				: {}),
			...(overlay.cliOnly === true ? { cliOnly: true as const } : {}),
			...(overlay.busyPolicy !== undefined
				? { busyPolicy: overlay.busyPolicy }
				: {}),
			...(overlay.busyHandler !== undefined
				? { busyHandler: overlay.busyHandler }
				: {}),
		};
	});
}

/**
 * Gateway-only survivors: real gateway machinery with no host meaning.
 * Every row here names its executor owner via `execute`, or it does
 * not ship (DEC-078: no inert rows). DEC-083 cut thirteen rows
 * (queue, steer, background, agents, stop, pause, approve, deny, sethome,
 * sessions, topic, platform, version); the five below are the survivors.
 * Path rows execute in RpcTurnRunner native path commands (DEC-085).
 */
const GATEWAY_ONLY_ROWS: readonly CommandDef[] = [
	{
		name: "start",
		description: "Acknowledge platform start pings without a reply",
		category: "Session",
		gatewayOnly: true,
		busyPolicy: "dispatch",
		busyHandler: "start",
	},
	{
		name: "switch-path",
		description:
			"List paths holding pi sessions, or re-root this chat onto one",
		category: "Session",
		gatewayOnly: true,
		execute: "switch_path",
		argsHint: "[path]",
	},
	{
		name: "new-path",
		description: "Start a fresh session under a given path",
		category: "Session",
		gatewayOnly: true,
		execute: "new_path",
		argsHint: "<path>",
	},
	{
		name: "help",
		description:
			"Show available commands (/help skills lists skill commands, /help <text> filters)",
		category: "Info",
		busyPolicy: "dispatch",
		execute: "gateway_help",
		argsHint: "[skills|<filter>]",
	},
	{
		name: "restart",
		description: "Gracefully restart the gateway after draining active runs",
		category: "Session",
		gatewayOnly: true,
		busyPolicy: "dispatch",
	},
];

/**
 * The shipped census: host-derived rows in host order, then gateway-only
 * rows. Count = host builtins + survivors (24 + 5 = 29 today); the count
 * test pins the sum shape, not a Hermes number.
 */
export const BUILTIN_COMMAND_ROWS: readonly CommandDef[] = [
	...hostRows(),
	...GATEWAY_ONLY_ROWS,
];

/**
 * Built-and-frozen registry: constructed ONCE at gateway assembly; every
 * derived consumer takes rows() snapshots or the live lookup() map from
 * THIS instance.
 */
export function createBuiltinCommandRegistry(): CommandRegistry {
	return CommandRegistry.frozen(BUILTIN_COMMAND_ROWS);
}
