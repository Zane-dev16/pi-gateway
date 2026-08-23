// Derived-consumer contracts (07 §1.2/§1.3): known-set membership, help line
// format, CLI catalog shapes, subcommand derivation (explicit + pipe hint),
// Telegram-shaped menu data. Byte-exact where Hermes is byte-exact.

import { describe, expect, it } from "vitest";
import type { CommandDef } from "./command-def.js";
import {
	buildCliDescription,
	cliCommandDescriptions,
	cliCommandsByCategory,
	completionCatalog,
	gatewayHelpLines,
	gatewayKnownCommands,
	isGatewayKnownCommand,
	requiresArgument,
	subcommandsFor,
	telegramMenuModel,
} from "./derived.js";

const row = (name: string, extra: Partial<CommandDef>): CommandDef => ({
	name,
	description: `${name} description`,
	category: "Session",
	...extra,
});

const ROWS: CommandDef[] = [
	row("new", {
		aliases: ["reset"],
		argsHint: "[name]",
		busyPolicy: "interrupt_then_dispatch",
	}),
	row("stop", { busyPolicy: "interrupt_then_dispatch", busyHandler: "stop" }),
	row("background", {
		aliases: ["bg", "btw"],
		argsHint: "<prompt>",
		busyPolicy: "dispatch",
	}),
	row("redraw", { cliOnly: true }),
	row("skills", {
		cliOnly: true,
		gatewayConfigGate: "skills.enabled",
		category: "Tools & Skills",
	}),
	row("start", {
		gatewayOnly: true,
		busyPolicy: "dispatch",
		busyHandler: "start",
	}),
];

describe("gatewayKnownCommands (GATEWAY_KNOWN_COMMANDS parity)", () => {
	it("includes names+aliases of every non-cli_only row — gateway_only included", () => {
		const known = gatewayKnownCommands(ROWS);
		for (const token of [
			"new",
			"reset",
			"stop",
			"background",
			"bg",
			"btw",
			"skills",
			"start",
		]) {
			expect(known.has(token)).toBe(true);
		}
	});

	it("EXCLUDES cli_only rows without a gate; gated cli_only rows are ALWAYS routable", () => {
		const known = gatewayKnownCommands(ROWS);
		expect(known.has("redraw")).toBe(false);
		expect(known.has("skills")).toBe(true);
	});

	it("isGatewayKnownCommand is slash/case-insensitive and null-safe", () => {
		const known = gatewayKnownCommands(ROWS);
		expect(isGatewayKnownCommand(known, "/RESET")).toBe(true);
		expect(isGatewayKnownCommand(known, "bg")).toBe(true);
		expect(isGatewayKnownCommand(known, "/foo")).toBe(false);
		expect(isGatewayKnownCommand(known, null)).toBe(false);
		expect(isGatewayKnownCommand(known, "")).toBe(false);
	});
});

describe("gatewayHelpLines byte format (gateway_help_lines parity)", () => {
	it("renders `/name [args]` -- description with alias notes; skips unavailable", () => {
		const lines = gatewayHelpLines(ROWS, {
			configOverrides: new Set(["skills"]),
		});
		expect(lines).toContain(
			"`/new [name]` -- new description (alias: `/reset`)",
		);
		expect(lines).toContain("`/stop` -- stop description");
		expect(lines).toContain("`/skills` -- skills description");
		expect(
			lines.some((l) => l.startsWith("/redraw") || l.includes("redraw")),
		).toBe(false);
	});

	it("closed gate hides the gated command; internal underscore aliases are skipped", () => {
		const closed = gatewayHelpLines(ROWS);
		expect(closed.some((l) => l.includes("skills"))).toBe(false);

		const withInternalAlias = [
			row("reload-mcp", { aliases: ["reload_mcp"], category: "Configuration" }),
		];
		expect(gatewayHelpLines(withInternalAlias)).toEqual([
			"`/reload-mcp` -- reload-mcp description",
		]);
	});
});

describe("CLI catalog (COMMANDS / _build_description parity)", () => {
	it("usage hint folds into canonical entries only; aliases get the plain form", () => {
		const catalog = cliCommandDescriptions(ROWS);
		expect(catalog.get("/background")).toBe(
			"background description (usage: /background <prompt>)",
		);
		expect(catalog.get("/bg")).toBe(
			"background description (alias for /background)",
		);
		expect(catalog.get("/new")).toBe("new description (usage: /new [name])");
		expect(catalog.get("/reset")).toBe("new description (alias for /new)");
	});

	it("gateway_only rows never appear on the CLI surface", () => {
		const catalog = cliCommandDescriptions(ROWS);
		expect(catalog.has("/start")).toBe(false);
	});

	it("by-category grouping mirrors the flat entries", () => {
		const byCategory = cliCommandsByCategory(ROWS);
		expect(byCategory.get("Session")?.get("/stop")).toBe("stop description");
		expect(byCategory.get("Tools & Skills")?.get("/skills")).toBe(
			"skills description",
		);
		expect(byCategory.get("Session")?.has("/start")).toBe(false);
	});

	it("buildCliDescription appends usage only when an args_hint exists", () => {
		expect(buildCliDescription(row("a", {}))).toBe("a description");
		expect(buildCliDescription(row("a", { argsHint: "<x>" }))).toBe(
			"a description (usage: /a <x>)",
		);
	});
});

describe("completions (COMMANDS + SUBCOMMANDS parity)", () => {
	it("explicit subcommands win; pipe-pattern extracted from args_hint otherwise", () => {
		expect(
			subcommandsFor(row("worktree", { subcommands: ["new", "list"] })),
		).toEqual(["new", "list"]);
		expect(
			subcommandsFor(row("topic", { argsHint: "[off|help|session-id]" })),
		).toEqual([
			"off",
			"help",
			// Hermes-faithful: _PIPE_SUBS_RE stops at the first non-[a-z|] char,
			// so "session-id" contributes only "session".
			"session",
		]);
		expect(subcommandsFor(row("plain", { argsHint: "<prompt>" }))).toEqual([]);
		expect(subcommandsFor(row("bare", {}))).toEqual([]);
	});

	it("cli surface lists canonical+alias keys for non-gateway_only rows", () => {
		const catalog = completionCatalog(ROWS, { surface: "cli" });
		expect(catalog.commands).toContain("/new");
		expect(catalog.commands).toContain("/reset");
		expect(catalog.commands).toContain("/redraw"); // cli_only visible to the CLI
		expect(catalog.commands).not.toContain("/start");
		expect(catalog.subcommands.has("/new")).toBe(false); // "[name]": no pipes ⇒ no entry
	});

	it("gateway surface shares the availability predicate with help/menus", () => {
		const open = completionCatalog(ROWS, {
			configOverrides: new Set(["skills"]),
		});
		expect(open.commands).toContain("/skills");
		expect(open.commands).toContain("/start");
		expect(open.commands).not.toContain("/redraw");
		const closed = completionCatalog(ROWS);
		expect(closed.commands).not.toContain("/skills");
		expect(closed.commands).toContain("/start");
	});
});

describe("telegramMenuModel (telegram_bot_commands parity, no Telegram import)", () => {
	it("one sanitized entry per available canonical row; aliases skipped", () => {
		const menu = telegramMenuModel(ROWS);
		const names = menu.map((m) => m.command);
		expect(names).toContain("new");
		expect(names).toContain("background");
		// Gated cli_only rows stay hidden while the gate is closed (default here).
		expect(names).not.toContain("skills");
		expect(names).not.toContain("reset");
		expect(names).not.toContain("bg");
	});

	it("hyphens → underscores; invalid chars stripped; arg-taking built-ins INCLUDED", () => {
		const menu = telegramMenuModel([row("reload-mcp", {})]);
		expect(menu[0]?.command).toBe("reload_mcp");
		// /background requires <prompt> but stays in the menu (#24312).
		expect(telegramMenuModel(ROWS).map((m) => m.command)).toContain(
			"background",
		);
	});

	it("plugin entries ride the rails but arg-requiring ones are excluded (07 §1.5)", () => {
		const menu = telegramMenuModel(ROWS, {
			configOverrides: new Set(),
			pluginEntries: [
				{ name: "deploy-site", description: "Deploy the site" },
				{
					name: "ask",
					description: "Ask with context",
					argsHint: "<question>",
				},
			],
		});
		const names = menu.map((m) => m.command);
		expect(names).toContain("deploy_site");
		expect(names).not.toContain("ask");
	});

	it("closed gate drops gated rows from the menu", () => {
		expect(telegramMenuModel(ROWS).map((m) => m.command)).not.toContain(
			"skills",
		);
	});
});

describe("requiresArgument", () => {
	it("only a leading '<' marks bare selection incomplete", () => {
		expect(requiresArgument("<prompt>")).toBe(true);
		expect(requiresArgument(" [name] ")).toBe(false);
		expect(requiresArgument(undefined)).toBe(false);
	});
});
