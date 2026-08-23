// Config-gate semantics (07 §1.3): pure evaluation, injectable reader,
// degrade-to-closed-safe on ANY read error; gated rows stay ROUTABLE while
// hidden.

import { describe, expect, it } from "vitest";
import type { CommandDef } from "./command-def.js";
import {
	isGatewayAvailable,
	resolveConfigGates,
	walkConfigDotPath,
} from "./config-gates.js";

const row = (extra: Partial<CommandDef>): CommandDef => ({
	name: "cmd",
	description: "d",
	category: "Configuration",
	...extra,
});

const GATED_CLI_ONLY: CommandDef = {
	name: "skills",
	description: "Manage skills",
	category: "Tools & Skills",
	cliOnly: true,
	gatewayConfigGate: "skills.enabled",
};

describe("walkConfigDotPath", () => {
	it("walks plain-object nodes only; arrays and scalars terminate as undefined", () => {
		const cfg = { a: { b: { c: true } }, arr: [{ x: 1 }], n: 5 };
		expect(walkConfigDotPath(cfg, "a.b.c")).toBe(true);
		expect(walkConfigDotPath(cfg, "arr.x")).toBeUndefined();
		expect(walkConfigDotPath(cfg, "n.x")).toBeUndefined();
		expect(walkConfigDotPath(cfg, "missing.deep")).toBeUndefined();
	});
});

describe("resolveConfigGates — degrade closed-safe (07 §1.3)", () => {
	it("collects canonical names whose gates are truthy", () => {
		const open = resolveConfigGates([GATED_CLI_ONLY], () => ({
			skills: { enabled: "yes" },
		}));
		expect([...open]).toEqual(["skills"]);
	});

	it("closed gate (falsy/missing) stays out of the set", () => {
		expect(
			resolveConfigGates([GATED_CLI_ONLY], () => ({ skills: { enabled: 0 } })),
		).toEqual(new Set());
		expect(resolveConfigGates([GATED_CLI_ONLY], () => ({}))).toEqual(new Set());
	});

	it("ANY reader throw degrades to the EMPTY set — never propagates", () => {
		expect(
			resolveConfigGates([GATED_CLI_ONLY], () => {
				throw new Error("config unreadable");
			}),
		).toEqual(new Set());
	});

	it("no gated rows → empty set without touching the reader", () => {
		let reads = 0;
		const open = resolveConfigGates([row({ name: "plain" })], () => {
			reads += 1;
			return {};
		});
		expect(open.size).toBe(0);
		expect(reads).toBe(0);
	});
});

describe("isGatewayAvailable — the ONE surface predicate", () => {
	it("non-cli_only rows are unconditionally visible", () => {
		expect(isGatewayAvailable(row({ name: "help" }))).toBe(true);
		expect(isGatewayAvailable(row({ name: "help" }), new Set())).toBe(true);
	});

	it("cli_only without a gate is never gateway-visible", () => {
		expect(isGatewayAvailable(row({ name: "redraw", cliOnly: true }))).toBe(
			false,
		);
	});

	it("cli_only + gate: visible iff the override set contains the canonical name", () => {
		expect(isGatewayAvailable(GATED_CLI_ONLY, new Set())).toBe(false);
		expect(isGatewayAvailable(GATED_CLI_ONLY, new Set(["skills"]))).toBe(true);
	});

	it("without overrides, evaluates via an injected reader; reader throw ⇒ closed", () => {
		expect(
			isGatewayAvailable(GATED_CLI_ONLY, undefined, () => ({
				skills: { enabled: true },
			})),
		).toBe(true);
		expect(
			isGatewayAvailable(GATED_CLI_ONLY, undefined, () => {
				throw new Error("boom");
			}),
		).toBe(false);
		expect(isGatewayAvailable(GATED_CLI_ONLY)).toBe(false);
	});
});
