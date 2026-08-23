// THE DERIVATION PROPERTY TEST (Phase-2 exit criterion, 10 §Phase2):
// registering a TEST-ONLY CommandDef with a NEW alias at runtime must update
// EVERY derived consumer IN THE SAME PROCESS with NO consumer-code change.
// All six surfaces asserted simultaneously:
//   1. help renderer            4. Telegram-shaped menu model
//   2. gateway known-command set 5. busy-policy resolver (guard L2 feed)
//   3. completion provider      6. unknown-/foo handling

import { describe, expect, it } from "vitest";
import type { CommandDef } from "./command-def.js";
import { BusyResolver } from "./busy-resolver.js";
import {
	completionCatalog,
	gatewayHelpLines,
	gatewayKnownCommands,
	isGatewayKnownCommand,
	telegramMenuModel,
} from "./derived.js";
import { classifySlashIntake } from "./slash-intake.js";
import { CommandRegistry } from "./registry.js";

const row = (name: string, extra: Partial<CommandDef>): CommandDef => ({
	name,
	description: `${name} description`,
	category: "Session",
	...extra,
});

const BUILTIN: CommandDef[] = [
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
];

/** Snapshot of ALL SIX derived surfaces at one instant. */
function survey(registry: CommandRegistry) {
	const rows = registry.rows();
	const overrides = new Set<string>();
	return {
		help: gatewayHelpLines(rows, { configOverrides: overrides }),
		known: gatewayKnownCommands(rows),
		isKnown: (token: string) =>
			isGatewayKnownCommand(gatewayKnownCommands(rows), token),
		completions: completionCatalog(rows, { configOverrides: overrides }),
		menu: telegramMenuModel(rows),
		busy: BusyResolver.fromRows(rows),
		intake: (text: string) =>
			classifySlashIntake((n) => registry.resolve(n), text),
	};
}

describe("derivation property: one registration updates all six surfaces", () => {
	it("runtime test-only CommandDef + new alias reflects EVERYWHERE with zero consumer changes", () => {
		const registry = new CommandRegistry(BUILTIN);

		// Baseline: the future command exists nowhere.
		const before = survey(registry);
		expect(before.help.some((l) => l.includes("/deploy"))).toBe(false);
		expect(before.known.has("deploy")).toBe(false);
		expect(before.known.has("dp")).toBe(false);
		expect(before.isKnown("dp")).toBe(false);
		expect(before.completions.commands).not.toContain("/deploy");
		expect(before.completions.commands).not.toContain("/dp");
		expect(before.menu.map((m) => m.command)).not.toContain("deploy");
		expect(before.busy.resolve("dp")).toBeNull();
		expect(before.intake("/dp ship it")).toEqual({
			kind: "text",
			reason: "unknown-command",
			text: "/dp ship it",
		});
		const beforeBypass = [...before.busy.bypassCommandNames()].sort();

		// ONE registration — no consumer code touched.
		registry.register(
			row("deploy", {
				aliases: ["dp"],
				argsHint: "<env>",
				category: "Tools & Skills",
				description: "Deploy the current workspace",
				busyPolicy: "reject",
			}),
		);

		const after = survey(registry);
		// 1. help renderer
		expect(after.help).toContainEqual(
			"`/deploy <env>` -- Deploy the current workspace (alias: `/dp`)",
		);
		// 2. gateway known-command set (canonical AND alias)
		expect(after.isKnown("deploy")).toBe(true);
		expect(after.isKnown("/DP")).toBe(true);
		// 3. completions
		expect(after.completions.commands).toContain("/deploy");
		expect(after.completions.commands).toContain("/dp");
		// 4. menu model (sanitized canonical only)
		expect(after.menu.map((m) => m.command)).toContain("deploy");
		expect(after.menu.map((m) => m.command)).not.toContain("dp");
		// 5. busy resolver — alias routes; policy derives; bypass set grows by exactly this row
		expect(after.busy.resolve("dp")?.name).toBe("deploy");
		expect(after.busy.policyOf("dp")).toBe("reject");
		expect(after.busy.shouldBypassActiveSession("dp")).toBe(true); // resolvable ⇒ never queued
		expect([...after.busy.bypassCommandNames()].sort()).toEqual(
			beforeBypass, // reject-policy rows do not join the bypass set
		);
		// 6. unknown-/foo handling flips: "/dp" now resolves as a command
		const intake = after.intake("/dp prod");
		expect(intake.kind).toBe("command");
		if (intake.kind === "command") {
			expect(intake.cmd.name).toBe("deploy");
			expect(intake.args).toBe("prod");
		}
	});

	it("a dispatch-policy registration reaches the guard bypass set too", () => {
		const registry = new CommandRegistry(BUILTIN);
		const before = survey(registry);
		expect(before.busy.bypassCommandNames().has("whisper")).toBe(false);
		registry.register(
			row("whisper", { aliases: ["w"], busyPolicy: "dispatch" }),
		);
		const after = survey(registry);
		expect(after.busy.bypassCommandNames().has("whisper")).toBe(true);
	});
});

describe("config-gate consistency across consumers (07 §1.3)", () => {
	const GATED = row("skills", {
		cliOnly: true,
		gatewayConfigGate: "skills.enabled",
		category: "Tools & Skills",
		description: "Manage skills",
	});

	function gatedSurvey(overrides: Set<string>) {
		const rows = [GATED];
		return {
			help: gatewayHelpLines(rows, { configOverrides: overrides }),
			known: gatewayKnownCommands(rows),
			completions: completionCatalog(rows, { configOverrides: overrides }),
			menu: telegramMenuModel(rows, { configOverrides: overrides }),
			available:
				gatewayHelpLines(rows, { configOverrides: overrides }).length > 0,
		};
	}

	it("gate CLOSED: hidden from help/menu/completions yet ALWAYS routable in the known set", () => {
		const s = gatedSurvey(new Set());
		expect(s.help).toEqual([]);
		expect(s.menu).toEqual([]);
		expect(s.completions.commands).toEqual([]);
		// Routable regardless of visibility (handler re-checks the gate).
		expect(s.known.has("skills")).toBe(true);
	});

	it("gate OPEN: every availability-consuming surface shows the row consistently", () => {
		const s = gatedSurvey(new Set(["skills"]));
		expect(s.help).toHaveLength(1);
		expect(s.menu.map((m) => m.command)).toEqual(["skills"]);
		expect(s.completions.commands).toContain("/skills");
		expect(s.known.has("skills")).toBe(true);
	});

	it("reader failure degrades CLOSED-SAFE identically across surfaces", () => {
		const rows = [GATED];
		const reader = () => {
			throw new Error("unreadable");
		};
		expect(gatewayHelpLines(rows, { readRawConfig: reader })).toEqual([]);
		expect(telegramMenuModel(rows, { readRawConfig: reader })).toEqual([]);
		expect(completionCatalog(rows, { readRawConfig: reader }).commands).toEqual(
			[],
		);
		expect(gatewayKnownCommands(rows).has("skills")).toBe(true); // still routable
	});
});
