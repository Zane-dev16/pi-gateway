// Registry store contract: canonical uniqueness, bidirectional alias
// collisions REJECT (never silently overwrite), resolve semantics, freeze.

import { describe, expect, it } from "vitest";
import type { CommandDef } from "./command-def.js";
import {
	CommandRegistry,
	RegistryCollisionError,
	RegistryFrozenError,
} from "./registry.js";

const row = (
	name: string,
	aliases: string[] = [],
	extra: Partial<CommandDef> = {},
): CommandDef => ({
	name,
	description: `${name} description`,
	category: "Session",
	...(aliases.length > 0 ? { aliases } : {}),
	...extra,
});

describe("CommandRegistry registration enforcement", () => {
	it("rejects a duplicate canonical name — no silent overwrite", () => {
		const registry = new CommandRegistry([row("new", ["reset"])]);
		expect(() => registry.register(row("new"))).toThrowError(
			RegistryCollisionError,
		);
		expect(registry.resolve("new")?.description).toBe("new description");
		expect(registry.size).toBe(1);
	});

	it("rejects an alias colliding with an existing canonical name", () => {
		const registry = new CommandRegistry([row("new")]);
		expect(() => registry.register(row("renew", ["new"]))).toThrowError(
			/"new" is already owned by \/new/,
		);
	});

	it("rejects a canonical name colliding with an existing alias", () => {
		const registry = new CommandRegistry([row("new", ["reset"])]);
		expect(() => registry.register(row("reset"))).toThrowError(
			/"reset" is already owned by \/new/,
		);
	});

	it("rejects an alias colliding with another row's alias", () => {
		const registry = new CommandRegistry([row("background", ["bg"])]);
		expect(() => registry.register(row("bground", ["bg"]))).toThrowError(
			RegistryCollisionError,
		);
	});

	it("valid collisions-free registration succeeds and maps every alias", () => {
		const registry = new CommandRegistry([row("new", ["reset"]), row("stop")]);
		expect(registry.lookup().get("reset")).toBe(registry.resolve("new"));
		expect(registry.lookup().get("stop")).toBe(registry.resolve("stop"));
	});

	it("idempotent re-registration of the identical row is allowed only when opted in", () => {
		const r = row("model", [], { busyPolicy: "reject", busyHandler: "model" });
		const registry = new CommandRegistry();
		registry.register(r);
		expect(() => registry.register(r)).toThrowError(RegistryCollisionError);
		registry.register(r, { idempotent: true });
		expect(registry.size).toBe(1);
	});
});

describe("resolve_command semantics (commands.py parity)", () => {
	const registry = CommandRegistry.frozen([
		row("new", ["reset"]),
		row("background", ["bg", "btw"]),
	]);

	it("resolves names and aliases case-insensitively", () => {
		expect(registry.resolve("NEW")?.name).toBe("new");
		expect(registry.resolve("Reset")?.name).toBe("new");
		expect(registry.resolve("bg")?.name).toBe("background");
	});

	it("strips ALL leading slashes (lstrip('/') parity)", () => {
		expect(registry.resolve("/new")?.name).toBe("new");
		expect(registry.resolve("///new")?.name).toBe("new");
		expect(registry.resolve("/")).toBeNull();
	});

	it("returns null for unknown/empty names — never throws", () => {
		expect(registry.resolve("nope")).toBeNull();
		expect(registry.resolve("")).toBeNull();
		expect(registry.resolve(null)).toBeNull();
		expect(registry.resolve(undefined)).toBeNull();
	});

	it("busyPolicyOf applies the DEC-005 default to unannotated rows", () => {
		expect(registry.busyPolicyOf("new")).toBe("reject");
		expect(registry.busyPolicyOf("background")).toBe("reject");
		expect(registry.busyPolicyOf("nope")).toBeNull();
		const withPolicy = CommandRegistry.frozen([
			row("queue", ["q"], { busyPolicy: "dispatch", busyHandler: "queue" }),
		]);
		expect(withPolicy.busyPolicyOf("q")).toBe("dispatch");
	});
});

describe("frozen central registry", () => {
	it("freeze() blocks further registration loudly", () => {
		const registry = CommandRegistry.frozen([row("help")]);
		expect(registry.frozen).toBe(true);
		expect(() => registry.register(row("late"))).toThrowError(
			RegistryFrozenError,
		);
		expect(registry.size).toBe(1);
	});

	it("rows() snapshots; lookup() stays live across mutation", () => {
		const registry = new CommandRegistry([row("a")]);
		const before = registry.rows();
		registry.register(row("b"));
		expect(before.map((r) => r.name)).toEqual(["a"]);
		expect(registry.lookup().has("b")).toBe(true);
	});
});
