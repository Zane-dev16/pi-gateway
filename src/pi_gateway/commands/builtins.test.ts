// Builtin registry row census CONTRACTS (07 §1; hermes_cli/commands.py:
// COMMAND_REGISTRY). The shipped set must be POPULATED and FAITHFUL: every
// derived consumer derives from these rows, so an empty/wrong census poisons
// help, menus, completions, known-command classification AND Guard-2 busy
// dispatch simultaneously. Row count is pinned to the reference census.

import { describe, expect, it } from "vitest";
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
	it("row count matches the hermes COMMAND_REGISTRY census exactly", () => {
		// Pinned against /tmp/hermes-upstream hermes_cli/commands.py
		// COMMAND_REGISTRY (99 CommandDef rows). Adding/removing a row is a
		// conscious census change, never an accident.
		expect(BUILTIN_COMMAND_ROWS).toHaveLength(99);
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

	it("canonical names are unique across the whole census", () => {
		const names = BUILTIN_COMMAND_ROWS.map((r) => r.name);
		expect(new Set(names).size).toBe(names.length);
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

	it("'//stop' resolves non-null (lstrip('/') strips ALL slashes)", () => {
		const cmd = registry.resolve("//stop");
		expect(cmd).not.toBeNull();
		expect(cmd?.name).toBe("stop");
		expect(cmd?.busyPolicy).toBe("interrupt_then_dispatch");
	});

	it("aliases resolve to their owning row ('reset' → new, 'bg' → background)", () => {
		expect(registry.resolve("reset")?.name).toBe("new");
		expect(registry.resolve("bg")?.name).toBe("background");
		expect(registry.resolve("/q")?.name).toBe("queue");
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
		expect(lines.length).toBeGreaterThan(50);
		expect(lines.some((l) => l.startsWith("`/new [name]`"))).toBe(true);
		expect(lines.some((l) => l.startsWith("`/help"))).toBe(true);
	});

	it("completion catalogs (cli + gateway) carry names AND aliases", () => {
		for (const surface of ["cli", "gateway"] as const) {
			const catalog = completionCatalog(rows, { surface });
			expect(catalog.commands.length).toBeGreaterThanOrEqual(80);
			expect(catalog.commands).toContain("/new");
			expect(catalog.commands).toContain("/reset");
			expect(catalog.subcommands.get("/kanban")?.length).toBeGreaterThan(20);
			expect(catalog.subcommands.get("/voice")).toEqual([
				"on",
				"off",
				"tts",
				"status",
			]);
		}
	});

	it("the telegram menu model carries sanitized gateway-available entries", () => {
		const menu = telegramMenuModel(rows);
		expect(menu.length).toBeGreaterThan(30);
		const names = menu.map((m) => m.command);
		expect(names).toContain("new");
		expect(names).toContain("sethome"); // set-home sanitized to underscores
		expect(names.every((n) => /^[a-z0-9_]+$/.test(n))).toBe(true);
	});

	it("the known-command set classifies real commands vs unknown text", () => {
		const known = gatewayKnownCommands(rows);
		expect(known.size).toBeGreaterThan(60);
		for (const token of ["new", "reset", "stop", "help", "background"]) {
			expect(known.has(token), token).toBe(true);
		}
		// cli_only rows without gates stay OUT of the gateway known-set.
		expect(known.has("clear")).toBe(false);
		// …but a config-gated cli_only row is ALWAYS routable.
		expect(known.has("verbose")).toBe(true);
	});
});

describe("Guard-2 busy coverage — EVERY resolvable token has a policy", () => {
	const registry = createBuiltinCommandRegistry();
	const resolver = BusyResolver.fromLookup(registry.lookup());

	it("all 99 canonical rows project into the guard feed with valid policies", () => {
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
		expect(checked).toBeGreaterThan(110); // 99 names + aliases
	});

	it("interrupt-class routing covers the /stop, /new cancel-handoff class", () => {
		expect(resolver.isInterruptThenDispatch("/stop")).toBe(true);
		expect(resolver.isInterruptThenDispatch("/new")).toBe(true);
		// Dispatch-class commands bypass queueing; reject-class do not exist
		// as unresolvable tokens — DEC-005 default only applies to rows that
		// deliberately omit busy_policy.
		expect(resolver.shouldBypassActiveSession("/background")).toBe(true);
		expect(resolver.policyOf("/model")).toBe("reject");
		expect(resolver.shouldBypassActiveSession("/nope")).toBe(false); // unknown ⇒ queueable text
	});

	it("busy handlers ALWAYS pair with an explicit policy (never the silent default)", () => {
		for (const row of BUILTIN_COMMAND_ROWS) {
			if (row.busyHandler != null) {
				expect(row.busyPolicy, `/${row.name}`).toBeDefined();
			}
		}
		// Spot-check the documented classes (CommandDef field docs): /moa's
		// custom busy-reject text vs the /queue FIFO enqueue handler.
		expect(registry.resolve("moa")?.busyHandler).toBe("moa");
		expect(registry.resolve("queue")?.busyHandler).toBe("queue");
	});
});
