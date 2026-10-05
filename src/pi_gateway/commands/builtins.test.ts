// Builtin registry row census CONTRACTS (07 §1; DEC-078 derivation). The
// shipped set must be POPULATED and FAITHFUL: every derived consumer
// derives from these rows, so an empty/wrong census poisons help, menus,
// completions, known-command classification AND Guard-2 busy dispatch
// simultaneously. Row count is pinned to host builtins + gateway survivors.

import { describe, expect, it } from "vitest";
import { BUILTIN_SLASH_COMMANDS } from "../../pi_agent_core/host.js";
import {
	BUILTIN_COMMAND_ROWS,
	createBuiltinCommandRegistry,
} from "./builtins.js";
import { DEFAULT_BUSY_POLICY, VALID_BUSY_POLICIES } from "./command-def.js";
import { buildBusyLookup, toGuardRows, BusyResolver } from "./busy-resolver.js";
import {
	completionCatalog,
	gatewayHelpLines,
	gatewayKnownCommands,
	telegramMenuModel,
} from "./derived.js";

describe("BUILTIN_COMMAND_ROWS — the shipped census", () => {
	it("row count equals host builtins plus gateway-only survivors", () => {
		// DEC-078 derivation: the census is the host BUILTIN_SLASH_COMMANDS
		// (24 rows) plus the gateway-only survivors — never a Hermes number.
		// /status duplicates host /session; /commands duplicates /help
		// filtering plus skill modes. DEC-083 cut thirteen rows (queue,
		// steer, background, agents, stop, pause, approve, deny, sethome,
		// sessions, topic, platform, version); survivors are help, start,
		// restart, switch-path, new-path. Adding/removing a row is still a
		// conscious census change, never an accident.
		expect(BUILTIN_COMMAND_ROWS.length).toBe(29);
		expect(BUILTIN_COMMAND_ROWS.length).toBe(
			BUILTIN_SLASH_COMMANDS.length + 5,
		);
		expect(BUILTIN_SLASH_COMMANDS.length).toBe(24);
	});

	it("every row validates against the CommandDef schema (registry accepts all)", () => {
		const registry = createBuiltinCommandRegistry();
		expect(registry.frozen).toBe(true);
		expect(registry.size).toBe(BUILTIN_COMMAND_ROWS.length);
		// Registration order preserved for derived renderers.
		expect(registry.rows().map((r) => r.name)).toEqual(
			BUILTIN_COMMAND_ROWS.map((r) => r.name),
		);
	});

	it("host canonical names win (compact, not compress) and Hermes-only rows are gone", () => {
		const names = BUILTIN_COMMAND_ROWS.map((r) => r.name);
		expect(names).toContain("compact");
		expect(names).not.toContain("compress");
		for (const dropped of [
			"subscription",
			"topup",
			"pet",
			"hatch",
			"wake",
			"moa",
			"yolo",
			"egress",
			"blueprint",
			"curator",
			"queue",
			"steer",
			"background",
			"agents",
			"stop",
			"pause",
			"approve",
			"deny",
			"sethome",
			"sessions",
			"topic",
			"platform",
			"version",
		]) {
			expect(names, dropped).not.toContain(dropped);
		}
		// Every host builtin ships exactly once.
		for (const cmd of BUILTIN_SLASH_COMMANDS) {
			expect(names, cmd.name).toContain(cmd.name);
		}
	});
});

describe("resolve_command parity over the builtin registry", () => {
	const registry = createBuiltinCommandRegistry();

	it("'/new' resolves non-null with its interrupt_then_dispatch policy", () => {
		const cmd = registry.resolve("/new");
		expect(cmd).not.toBeNull();
		expect(cmd?.busyPolicy).toBe("interrupt_then_dispatch");
		expect(cmd?.aliases).toContain("reset");
		expect(registry.busyPolicyOf("/new")).toBe("interrupt_then_dispatch");
	});

	it("'//new' resolves non-null (lstrip('/') strips ALL slashes)", () => {
		const cmd = registry.resolve("//new");
		expect(cmd).not.toBeNull();
		expect(cmd?.name).toBe("new");
		expect(cmd?.busyPolicy).toBe("interrupt_then_dispatch");
	});

	it("aliases resolve to their owning row ('reset' → new)", () => {
		expect(registry.resolve("reset")?.name).toBe("new");
	});

	it("cut rows resolve null (queueable-text classification intact)", () => {
		for (const cut of [
			"queue",
			"q",
			"steer",
			"background",
			"bg",
			"btw",
			"agents",
			"tasks",
			"stop",
			"pause",
			"approve",
			"deny",
			"sethome",
			"set-home",
			"sessions",
			"topic",
			"platform",
			"version",
			"v",
		]) {
			expect(registry.resolve(cut), cut).toBeNull();
			expect(registry.resolve(`/${cut}`), cut).toBeNull();
		}
	});

	it("unknown commands still resolve null (queueable-text classification intact)", () => {
		expect(registry.resolve("/definitely-not-a-command")).toBeNull();
	});
});

describe("derived consumers are NON-EMPTY over the builtin rows", () => {
	const registry = createBuiltinCommandRegistry();
	const rows = registry.rows();

	it("gateway help lines cover every gateway-available row", () => {
		const lines = gatewayHelpLines(rows);
		expect(lines.length).toBeGreaterThan(15);
		expect(lines.some((l) => l.startsWith("`/new"))).toBe(true);
		expect(lines.some((l) => l.startsWith("`/help"))).toBe(true);
		// Cut rows stay cut: /status duplicates host /session, /commands
		// duplicates /help filtering plus skill modes, and DEC-083 cut
		// thirteen rows with no gateway executor.
		expect(lines.some((l) => l.startsWith("`/status"))).toBe(false);
		expect(lines.some((l) => l.startsWith("`/commands"))).toBe(false);
		for (const cut of [
			"queue",
			"steer",
			"background",
			"agents",
			"stop",
			"pause",
			"approve",
			"deny",
			"sethome",
			"sessions",
			"topic",
			"platform",
			"version",
		]) {
			expect(lines.some((l) => l.startsWith(`/${cut} `) || l.startsWith(`/${cut}\``)), cut).toBe(false);
		}
	});

	it("completion catalogs (cli + gateway) carry names AND aliases", () => {
		for (const surface of ["cli", "gateway"] as const) {
			const catalog = completionCatalog(rows, { surface });
			// The catalog derives purely from the surviving census (host
			// builtins + gateway survivors + their aliases).
			expect(catalog.commands.length).toBeGreaterThanOrEqual(20);
			expect(catalog.commands).toContain("/new");
			expect(catalog.commands).toContain("/reset");
			expect(catalog.commands).toContain("/compact");
			expect(catalog.commands).not.toContain("/subscription");
		}
		// No surviving gateway row carries explicit subcommands (/platform cut).
		const gw = completionCatalog(rows, { surface: "gateway" });
		expect(gw.subcommands.size).toBe(0);
	});

	it("the telegram menu model carries sanitized gateway-available entries", () => {
		const menu = telegramMenuModel(rows);
		expect(menu.length).toBeGreaterThan(15);
		const names = menu.map((m) => m.command);
		expect(names).toContain("new");
		expect(names).toContain("compact");
		expect(names).toContain("switch_path");
		expect(names).toContain("new_path");
		expect(names).toContain("restart");
		expect(names).not.toContain("subscription");
		expect(names.every((n) => /^[a-z0-9_]+$/.test(n))).toBe(true);
	});

	it("the known-command set classifies real commands vs unknown text", () => {
		const known = gatewayKnownCommands(rows);
		expect(known.size).toBeGreaterThan(20);
		for (const token of ["new", "reset", "help", "compact", "restart", "start"]) {
			expect(known.has(token), token).toBe(true);
		}
		// Dropped Hermes-only rows classify as unknown text now.
		expect(known.has("subscription")).toBe(false);
		expect(known.has("compress")).toBe(false);
		for (const cut of [
			"queue",
			"steer",
			"background",
			"agents",
			"stop",
			"pause",
			"approve",
			"deny",
			"sethome",
			"sessions",
			"topic",
			"platform",
			"version",
		]) {
			expect(known.has(cut), cut).toBe(false);
		}
	});
});

describe("Guard-2 busy coverage — EVERY resolvable token has a policy", () => {
	const registry = createBuiltinCommandRegistry();
	const resolver = BusyResolver.fromLookup(registry.lookup());

	it("all surviving canonical rows project into the guard feed with valid policies", () => {
		const guardRows = toGuardRows(BUILTIN_COMMAND_ROWS);
		expect(guardRows).toHaveLength(BUILTIN_COMMAND_ROWS.length);
		const lookup = buildBusyLookup(guardRows);
		for (const row of BUILTIN_COMMAND_ROWS) {
			for (const token of [row.name, ...(row.aliases ?? [])]) {
				const resolved = lookup.get(token);
				expect(resolved, `token ${token}`).toBeDefined();
				expect(
					VALID_BUSY_POLICIES.has(resolved?.busyPolicy ?? DEFAULT_BUSY_POLICY),
				).toBe(true);
			}
		}
	});

	it("every name/alias in the live lookup resolves to a BusyPolicy (never null)", () => {
		let checked = 0;
		for (const [token] of registry.lookup()) {
			const policy = resolver.policyOf(token);
			expect(policy, `token ${token}`).not.toBeNull();
			expect(VALID_BUSY_POLICIES.has(policy as string)).toBe(true);
			checked += 1;
		}
		expect(checked).toBeGreaterThan(25); // names + aliases
	});

	it("interrupt-class routing covers the /new cancel-handoff class", () => {
		expect(resolver.isInterruptThenDispatch("/new")).toBe(true);
		expect(resolver.isInterruptThenDispatch("/stop")).toBe(false); // cut: unknown ⇒ never interrupt
		// Dispatch-class commands bypass queueing; reject-class do not exist
		// as unresolvable tokens — DEC-005 default only applies to rows that
		// deliberately omit busy_policy.
		expect(resolver.shouldBypassActiveSession("/restart")).toBe(true);
		expect(resolver.policyOf("/model")).toBe("reject");
		expect(resolver.shouldBypassActiveSession("/nope")).toBe(false); // unknown ⇒ queueable text
	});

	it("busy handlers ALWAYS pair with an explicit policy (never the silent default)", () => {
		for (const row of BUILTIN_COMMAND_ROWS) {
			if (row.busyHandler != null) {
				expect(row.busyPolicy, `/${row.name}`).toBeDefined();
			}
		}
		// Spot-check the surviving handler classes: /model's custom
		// busy-reject vs the /new interrupt and /start special handlers.
		expect(registry.resolve("model")?.busyHandler).toBe("model");
		expect(registry.resolve("new")?.busyHandler).toBe("new");
		expect(registry.resolve("start")?.busyHandler).toBe("start");
	});
});
