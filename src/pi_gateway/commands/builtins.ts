// pi_gateway/commands/builtins — the SHIPPED builtin command rows (07 §1).
//
// Ported from the READ-ONLY Hermes reference, cited as file:symbol anchors —
// no code vendored:
//   hermes_cli/commands.py:COMMAND_REGISTRY → BUILTIN_COMMAND_ROWS
//     (~90 CommandDef rows; names, aliases, descriptions, categories, args
//     hints, subcommands, cli/gateway flags, busy policies/handlers, config
//     gates, execute keys — field-for-field onto pi's CommandDef schema)
//   hermes_cli/commands.py:_COMMAND_LOOKUP / CommandRegistry.frozen
//     → createBuiltinCommandRegistry()
//   hermes_constants.py:INDICATOR_STYLES ("ascii","emoji","kaomoji","unicode")
//     → inlined for /indicator (the only computed row in the census)
//
// The registry MACHINERY lives in registry.ts; this module is pure DATA plus
// one frozen constructor so gateway assembly has a single built-and-frozen
// builtin set (07 §9: no hand-built command lists anywhere else). Row order
// follows the reference census exactly — derived consumers render in this
// order.

import type { CommandDef } from "./command-def.js";
import { CommandRegistry } from "./registry.js";

/** hermes_constants.py:INDICATOR_STYLES — /indicator's completable styles. */
const INDICATOR_STYLES = ["ascii", "emoji", "kaomoji", "unicode"] as const;

/**
 * The COMMAND_REGISTRY row census, verbatim. Every downstream consumer (help,
 menus, completions, known-command classification, Guard-2 busy dispatch)
 derives from rows of exactly this shape via the one registry.
 */
export const BUILTIN_COMMAND_ROWS: readonly CommandDef[] = [
	// Session
	{
		name: "start",
		description: "Acknowledge platform start pings without a reply",
		category: "Session",
		gatewayOnly: true,
		busyPolicy: "dispatch",
		busyHandler: "start",
	},
	{
		name: "new",
		description: "Start a new session (fresh session ID + history)",
		category: "Session",
		aliases: ["reset"],
		argsHint: "[name]",
		busyPolicy: "interrupt_then_dispatch",
		busyHandler: "new",
	},
	{
		name: "topic",
		description: "Enable or inspect Telegram DM topic sessions",
		category: "Session",
		gatewayOnly: true,
		argsHint: "[off|help|session-id]",
	},
	{
		name: "clear",
		description: "Clear screen and start a new session",
		category: "Session",
		cliOnly: true,
	},
	{
		name: "redraw",
		description: "Force a full UI repaint (recovers from terminal drift)",
		category: "Session",
		cliOnly: true,
	},
	{
		name: "history",
		description: "Show conversation history",
		category: "Session",
		cliOnly: true,
	},
	{
		name: "save",
		description: "Export the current conversation (bare /save shows usage)",
		category: "Session",
		argsHint: "<json|md|html> [filename] [redact]",
	},
	{
		name: "retry",
		description: "Retry the last message (resend to agent)",
		category: "Session",
	},
	{
		name: "prompt",
		description: "Compose your next prompt in $EDITOR (markdown), then send it",
		category: "Session",
		cliOnly: true,
		argsHint: "[initial text]",
		aliases: ["compose"],
	},
	{
		name: "undo",
		description: "Back up N user turns and re-prompt (default 1)",
		category: "Session",
		argsHint: "[N]",
	},
	{
		name: "title",
		description: "Set a title for the current session",
		category: "Session",
		argsHint: "[name]",
	},
	{
		name: "handoff",
		description:
			"Hand off this session to a messaging platform (Telegram, Discord, etc.)",
		category: "Session",
		argsHint: "<platform>",
		cliOnly: true,
	},
	{
		name: "branch",
		description: "Branch the current session (explore a different path)",
		category: "Session",
		aliases: ["fork"],
		argsHint: "[name]",
	},
	{
		name: "worktree",
		description: "Show, list, create, or prune isolated git worktrees",
		category: "Session",
		cliOnly: true,
		argsHint: "[new [name]|list|prune [--dry-run]]",
		subcommands: ["new", "list", "prune"],
	},
	{
		name: "compress",
		description:
			"Compress conversation context (add 'here [N]' to keep recent N turns; --preview shows what would happen)",
		category: "Session",
		aliases: ["compact"],
		argsHint: "[here [N] | focus topic | --preview|--dry-run]",
	},
	{
		name: "rollback",
		description:
			"List or restore filesystem checkpoints (restores keep your hand-edits; --all overrides)",
		category: "Session",
		argsHint: "[number] [--all]",
	},
	{
		name: "snapshot",
		description: "Create or restore state snapshots of Hermes config/state",
		category: "Session",
		cliOnly: true,
		aliases: ["snap"],
		argsHint: "[create|restore <id>|prune]",
	},
	{
		name: "export",
		description:
			"Export a profile (config, skills, theme) to a shareable archive",
		category: "Configuration",
		cliOnly: true,
		argsHint: "[profile] [-o output.tar.gz]",
	},
	{
		name: "import",
		description: "Import a shared profile archive as a new profile",
		category: "Configuration",
		cliOnly: true,
		argsHint: "<archive.tar.gz> [--name <name>]",
	},
	{
		name: "stop",
		description: "Kill all running background processes",
		category: "Session",
		busyPolicy: "interrupt_then_dispatch",
		busyHandler: "stop",
	},
	{
		name: "pause",
		description:
			"Pause new work globally (emergency stop); '/pause off' resumes",
		category: "Session",
		gatewayOnly: true,
		argsHint: "[reason | off]",
		busyPolicy: "dispatch",
	},
	{
		name: "approve",
		description: "Approve a pending dangerous command",
		category: "Session",
		gatewayOnly: true,
		argsHint: "[session|always]",
		busyPolicy: "dispatch",
	},
	{
		name: "deny",
		description: "Deny a pending dangerous command (optionally with a reason)",
		category: "Session",
		gatewayOnly: true,
		argsHint: "[all] [reason]",
		busyPolicy: "dispatch",
	},
	{
		name: "background",
		description: "Run a prompt in the background",
		category: "Session",
		aliases: ["bg", "btw"],
		argsHint: "<prompt>",
		busyPolicy: "dispatch",
	},
	{
		name: "agents",
		description: "Show active agents and running tasks",
		category: "Session",
		aliases: ["tasks"],
		busyPolicy: "dispatch",
	},
	{
		name: "journey",
		description: "Open the learning journey timeline",
		category: "Session",
		aliases: ["learning", "memory-graph"],
		cliOnly: true,
		argsHint: "[list|delete <id>|edit <id>]",
		subcommands: ["list", "delete", "edit"],
	},
	{
		name: "queue",
		description: "Queue a prompt for the next turn (doesn't interrupt)",
		category: "Session",
		aliases: ["q"],
		argsHint: "<prompt>",
		busyPolicy: "dispatch",
		busyHandler: "queue",
	},
	{
		name: "steer",
		description:
			"Inject a message after the next tool call without interrupting",
		category: "Session",
		argsHint: "<prompt>",
		busyPolicy: "dispatch",
		busyHandler: "steer",
	},
	{
		name: "goal",
		description:
			"Set a standing goal Hermes works on across turns until achieved",
		category: "Session",
		argsHint:
			"[text | draft <text> | show | gate add <cmd> | pause | resume | clear | status | wait <pid> | unwait]",
		busyPolicy: "dispatch",
		busyHandler: "goal",
	},
	{
		name: "heartbeat",
		description: "Set a recurring prompt that re-enters this session when idle",
		category: "Session",
		aliases: ["hb"],
		argsHint: "[every <interval> <prompt> | status | pause | resume | clear]",
		subcommands: ["status", "pause", "resume", "clear"],
		busyPolicy: "dispatch",
	},
	{
		name: "refine",
		description:
			"Review this conversation now and save lessons to memory/skills",
		category: "Session",
		argsHint: "[focus instructions]",
	},
	{
		name: "review",
		description:
			"Spawn an independent subagent to review the work just discussed (PR, code, docs)",
		category: "Session",
		argsHint: "[review instructions]",
	},
	{
		name: "moa",
		description:
			"Run one prompt through the default Mixture of Agents preset, then restore your model",
		category: "Session",
		argsHint: "<prompt>",
		busyPolicy: "reject",
		busyHandler: "moa",
	},
	{
		name: "subgoal",
		description: "Add or manage extra criteria on the active goal",
		category: "Session",
		argsHint: "[text | remove N | clear]",
		busyPolicy: "dispatch",
	},
	{
		name: "status",
		description: "Show session, model, token, and context info",
		category: "Session",
		busyPolicy: "dispatch",
	},
	{
		name: "egress",
		description: "Show Docker egress proxy status",
		category: "Session",
		argsHint: "[status]",
		subcommands: ["status"],
		busyPolicy: "dispatch",
		busyHandler: "egress",
		execute: "egress",
	},
	{
		name: "context",
		description:
			"Show detailed context window view with usage gauge, category breakdown, compression stats, and throughput",
		category: "Session",
		aliases: ["ctx"],
		argsHint: "[all]",
		subcommands: ["all"],
		busyPolicy: "dispatch",
	},
	{
		name: "whoami",
		description: "Show your slash command access (admin / user)",
		category: "Info",
	},
	{
		name: "profile",
		description: "Show active profile name and home directory",
		category: "Info",
		busyPolicy: "dispatch",
		execute: "profile",
	},
	{
		name: "sethome",
		description: "Set this chat as the home channel",
		category: "Session",
		gatewayOnly: true,
		aliases: ["set-home"],
	},
	{
		name: "resume",
		description: "Resume a previously-named session",
		category: "Session",
		argsHint: "[name]",
	},
	{
		name: "sessions",
		description: "Browse and resume previous sessions",
		category: "Session",
	},
	{
		name: "config",
		description: "Show current configuration",
		category: "Configuration",
		cliOnly: true,
	},
	{
		name: "model",
		description: "Switch model (session-scoped; --global to persist)",
		category: "Configuration",
		argsHint: "[model] [--provider name] [--global|--session] [--refresh]",
		busyPolicy: "reject",
		busyHandler: "model",
	},
	{
		name: "codex-runtime",
		description: "Toggle codex app-server runtime for OpenAI/Codex models",
		category: "Configuration",
		aliases: ["codex_runtime"],
		argsHint: "[auto|codex_app_server]",
		busyPolicy: "reject",
		busyHandler: "codex-runtime",
	},
	{
		name: "personality",
		description: "Set a predefined personality",
		category: "Configuration",
		argsHint: "[name]",
	},
	{
		name: "statusbar",
		description: "Toggle the context/model status bar",
		category: "Configuration",
		cliOnly: true,
		aliases: ["sb"],
	},
	{
		name: "battery",
		description: "Toggle a color-coded battery indicator in the status bar",
		category: "Configuration",
		cliOnly: true,
		argsHint: "[on|off|status]",
		subcommands: ["on", "off", "status"],
	},
	{
		name: "timestamps",
		description: "Toggle [HH:MM] timestamps on messages and /history",
		category: "Configuration",
		cliOnly: true,
		argsHint: "[on|off|status]",
		subcommands: ["on", "off", "status"],
		aliases: ["ts"],
	},
	{
		name: "diff",
		description: "Show git changes in the working directory",
		category: "Info",
		argsHint: "[staged|all|session] [--stat] [path...]",
		subcommands: ["staged", "all", "session"],
	},
	{
		name: "verbose",
		description: "Cycle tool progress display: off -> new -> all -> verbose",
		category: "Configuration",
		cliOnly: true,
		gatewayConfigGate: "display.tool_progress_command",
		busyPolicy: "dispatch",
	},
	{
		name: "focus",
		description:
			"Toggle focus view — show only your prompt and the final response",
		category: "Configuration",
		cliOnly: true,
		argsHint: "[on|off|status]",
		subcommands: ["on", "off", "status"],
	},
	{
		name: "footer",
		description: "Toggle gateway runtime-metadata footer on final replies",
		category: "Configuration",
		argsHint: "[on|off|status]",
		subcommands: ["on", "off", "status"],
		busyPolicy: "dispatch",
	},
	{
		name: "yolo",
		description: "Toggle YOLO mode (skip all dangerous command approvals)",
		category: "Configuration",
		busyPolicy: "dispatch",
	},
	{
		name: "approvals",
		description: "Show or set the persistent dangerous-command approval mode",
		category: "Configuration",
		argsHint: "[manual|smart|off]",
		subcommands: ["manual", "smart", "off"],
	},
	{
		name: "reasoning",
		description: "Manage reasoning effort and display",
		category: "Configuration",
		argsHint: "[level|show|hide|full|clamp] [--global]",
		subcommands: [
			"none",
			"minimal",
			"low",
			"medium",
			"high",
			"xhigh",
			"max",
			"ultra",
			"show",
			"hide",
			"on",
			"off",
			"full",
			"clamp",
			"--global",
		],
	},
	{
		name: "fast",
		description:
			"Toggle fast mode — OpenAI Priority Processing / Anthropic Fast Mode (Normal/Fast)",
		category: "Configuration",
		argsHint: "[normal|fast|status] [--global]",
		subcommands: ["normal", "fast", "status", "on", "off", "--global"],
	},
	{
		name: "skin",
		description: "Show or change the display skin/theme",
		category: "Configuration",
		cliOnly: true,
		argsHint: "[name]",
	},
	{
		name: "indicator",
		description: "Pick the TUI busy-indicator style",
		category: "Configuration",
		cliOnly: true,
		argsHint: `[${INDICATOR_STYLES.join("|")}]`,
		subcommands: [...INDICATOR_STYLES],
	},
	{
		name: "voice",
		description: "Toggle voice mode",
		category: "Configuration",
		argsHint: "[on|off|tts|status]",
		subcommands: ["on", "off", "tts", "status"],
	},
	{
		name: "wake",
		description: "Toggle the 'Hey Hermes' wake word listener",
		category: "Configuration",
		cliOnly: true,
		argsHint: "[on|off|status]",
		subcommands: ["on", "off", "status"],
	},
	{
		name: "busy",
		description: "Control what Enter does while Hermes is working",
		category: "Configuration",
		cliOnly: true,
		argsHint: "[queue|steer|interrupt|status]",
		subcommands: ["queue", "steer", "interrupt", "status"],
	},
	{
		name: "tools",
		description: "Manage tools: /tools [list|disable|enable] [name...]",
		category: "Tools & Skills",
		argsHint: "[list|disable|enable] [name...]",
		cliOnly: true,
	},
	{
		name: "toolsets",
		description: "List available toolsets",
		category: "Tools & Skills",
		cliOnly: true,
	},
	{
		name: "skills",
		description: "Search, install, inspect, or manage skills",
		category: "Tools & Skills",
		cliOnly: true,
		gatewayConfigGate: "skills.write_approval",
		subcommands: [
			"search",
			"browse",
			"inspect",
			"install",
			"audit",
			"pending",
			"approve",
			"reject",
			"diff",
			"approval",
		],
	},
	{
		name: "memory",
		description: "Review pending memory writes / toggle the approval gate",
		category: "Tools & Skills",
		argsHint: "[pending|approve|reject|approval] [id|on|off]",
		subcommands: ["pending", "approve", "reject", "approval"],
	},
	{
		name: "bundles",
		description: "List skill bundles (aliases /<name> for multiple skills)",
		category: "Tools & Skills",
		execute: "bundles",
	},
	{
		name: "pet",
		description:
			"Toggle or adopt a petdex mascot (/pet, /pet list, /pet <slug>)",
		category: "Tools & Skills",
		cliOnly: true,
		argsHint: "[toggle|list|scale <n>|<slug>]",
		subcommands: ["toggle", "list", "scale", "off"],
	},
	{
		name: "hatch",
		description: "Generate a new petdex pet from a description",
		category: "Tools & Skills",
		cliOnly: true,
		aliases: ["generate-pet"],
		argsHint: "[description]",
	},
	{
		name: "learn",
		description:
			"Learn a reusable skill from anything you describe (dirs, URLs, this chat, notes)",
		category: "Tools & Skills",
		argsHint: "<what to learn from>",
	},
	{
		name: "init",
		description:
			"Generate or update AGENTS.md project instructions from a repo scan",
		category: "Tools & Skills",
		argsHint: "[notes]",
	},
	{
		name: "cron",
		description: "Manage scheduled tasks",
		category: "Tools & Skills",
		cliOnly: true,
		argsHint: "[subcommand]",
		subcommands: [
			"list",
			"add",
			"create",
			"edit",
			"pause",
			"resume",
			"run",
			"remove",
		],
	},
	{
		name: "suggestions",
		description: "Review suggested automations (accept/dismiss)",
		category: "Tools & Skills",
		aliases: ["suggest"],
		argsHint: "[accept|dismiss N | catalog]",
		subcommands: ["accept", "dismiss", "catalog", "clear"],
	},
	{
		name: "blueprint",
		description: "Set up an automation from a blueprint template",
		category: "Tools & Skills",
		aliases: ["bp"],
		argsHint: "[name] [slot=value ...]",
	},
	{
		name: "curator",
		description:
			"Background skill maintenance (status, run, pin, archive, list-archived)",
		category: "Tools & Skills",
		argsHint: "[subcommand]",
		subcommands: [
			"status",
			"run",
			"pause",
			"resume",
			"pin",
			"unpin",
			"restore",
			"list-archived",
		],
	},
	{
		name: "reload",
		description: "Reload .env variables into the running session",
		category: "Tools & Skills",
		cliOnly: true,
	},
	{
		name: "reload-mcp",
		description: "Reload MCP servers from config",
		category: "Tools & Skills",
		aliases: ["reload_mcp"],
	},
	{
		name: "reload-skills",
		description:
			"Re-scan ~/.hermes/skills/ for newly installed or removed skills",
		category: "Tools & Skills",
		aliases: ["reload_skills"],
	},
	{
		name: "browser",
		description:
			"Connect browser tools to your live Chromium-family browser via CDP, or switch to Browser Use mode",
		category: "Tools & Skills",
		cliOnly: true,
		argsHint: "[connect|disconnect|status|use]",
		subcommands: ["connect", "disconnect", "status", "use"],
	},
	{
		name: "plugins",
		description: "List installed plugins and their status",
		category: "Tools & Skills",
		cliOnly: true,
	},
	{
		name: "commands",
		description: "Browse all commands and skills (paginated)",
		category: "Info",
		gatewayOnly: true,
		argsHint: "[page]",
		busyPolicy: "dispatch",
		execute: "gateway_commands",
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
		name: "palette",
		description: "Open the fuzzy command palette (also Ctrl+P)",
		category: "Info",
		cliOnly: true,
		busyPolicy: "dispatch",
	},
	{
		name: "restart",
		description: "Gracefully restart the gateway after draining active runs",
		category: "Session",
		gatewayOnly: true,
		busyPolicy: "dispatch",
	},
	{
		name: "usage",
		description:
			"Show token usage and rate limits; `reset` redeems a banked Codex limit reset",
		category: "Info",
		argsHint: "[reset [--force]]",
	},
	{
		name: "subscription",
		description: "View your Nous plan and change it in the browser",
		category: "Info",
		cliOnly: true,
		aliases: ["upgrade"],
	},
	{
		name: "topup",
		description: "Show your Nous balance and manage billing on the portal",
		category: "Info",
	},
	{
		name: "insights",
		description: "Show usage insights and analytics",
		category: "Info",
		argsHint: "[days]",
	},
	{
		name: "platforms",
		description: "Show gateway/messaging platform status",
		category: "Info",
		cliOnly: true,
		aliases: ["gateway"],
	},
	{
		name: "platform",
		description: "Pause, resume, or list a failing gateway platform",
		category: "Info",
		gatewayOnly: true,
		argsHint: "<pause|resume|list> [name]",
	},
	{
		name: "copy",
		description: "Copy the last assistant response to clipboard",
		category: "Info",
		cliOnly: true,
		argsHint: "[number]",
	},
	{
		name: "paste",
		description: "Attach clipboard image from your clipboard",
		category: "Info",
		cliOnly: true,
	},
	{
		name: "image",
		description: "Attach a local image file for your next prompt",
		category: "Info",
		cliOnly: true,
		argsHint: "<path>",
	},
	{
		name: "update",
		description: "Update Hermes Agent to the latest version",
		category: "Info",
		busyPolicy: "dispatch",
	},
	{
		name: "version",
		description: "Show Hermes Agent version",
		category: "Info",
		aliases: ["v"],
		busyPolicy: "dispatch",
		execute: "version",
	},
	{
		name: "debug",
		description:
			"Upload debug report (system info + logs) and get shareable links",
		category: "Info",
		argsHint: "[nous|local]",
	},
	{
		name: "quit",
		description: "Exit the CLI (use --delete to also remove session history)",
		category: "Exit",
		cliOnly: true,
		aliases: ["exit"],
		argsHint: "[--delete]",
	},
];

/**
 * Built-and-frozen builtin registry (commands.py:_COMMAND_LOOKUP analogue):
 * constructed ONCE at gateway assembly; every derived consumer takes rows()
 * snapshots or the live lookup() map from THIS instance.
 */
export function createBuiltinCommandRegistry(): CommandRegistry {
	return CommandRegistry.frozen(BUILTIN_COMMAND_ROWS);
}
