// Schema defaults + validation contract (07 §1.1 field-for-field).

import { describe, expect, it } from "vitest";
import {
	BUSY_POLICIES,
	CommandDefValidationError,
	type CommandDef,
	DEFAULT_BUSY_POLICY,
	aliasesOf,
	effectiveBusyPolicy,
	isTruthyValue,
	isValidCommandToken,
	validateCommandDef,
} from "./command-def.js";

describe("CommandDef schema (07 §1.1)", () => {
	it("carries every spec field; name/description/category required, rest defaulted", () => {
		const row: CommandDef = {
			name: "background",
			description: "Run a prompt in the background",
			category: "Session",
			aliases: ["bg", "btw"],
			argsHint: "<prompt>",
			subcommands: ["new", "list"],
			cliOnly: false,
			gatewayOnly: false,
			gatewayConfigGate: null,
			busyPolicy: "dispatch",
			busyHandler: null,
			execute: null,
		};
		expect(Object.keys(row).sort()).toEqual(
			[
				"aliases",
				"argsHint",
				"busyHandler",
				"busyPolicy",
				"category",
				"cliOnly",
				"description",
				"execute",
				"gatewayConfigGate",
				"gatewayOnly",
				"name",
				"subcommands",
			].sort(),
		);
	});

	it("busy-policy enum is EXACTLY {dispatch,reject,interrupt_then_dispatch}; default reject (DEC-005)", () => {
		expect([...BUSY_POLICIES]).toEqual([
			"dispatch",
			"reject",
			"interrupt_then_dispatch",
		]);
		const bare: CommandDef = {
			name: "x",
			description: "d",
			category: "Session",
		};
		expect(effectiveBusyPolicy(bare)).toBe(DEFAULT_BUSY_POLICY);
		expect(effectiveBusyPolicy(bare)).toBe("reject");
	});

	it("accessor helpers default absent optional fields", () => {
		const bare: CommandDef = {
			name: "x",
			description: "d",
			category: "Session",
		};
		expect(aliasesOf(bare)).toEqual([]);
		expect(aliasesOf({ ...bare, aliases: ["a"] })).toEqual(["a"]);
	});
});

describe("registration-time validation", () => {
	const base: CommandDef = {
		name: "ok",
		description: "d",
		category: "Session",
	};

	it("accepts a well-formed row and returns it unchanged", () => {
		expect(validateCommandDef(base)).toBe(base);
	});

	it("rejects slash-prefixed, empty, whitespace-y, or non-lowercase names/aliases", () => {
		for (const bad of ["/new", "", "has space", "New", "a/b"]) {
			expect(() => validateCommandDef({ ...base, name: bad })).toThrowError(
				CommandDefValidationError,
			);
			expect(() =>
				validateCommandDef({ ...base, aliases: [bad] }),
			).toThrowError(CommandDefValidationError);
		}
	});

	it("rejects busy policies outside the DEC-005 enum (no 'queue')", () => {
		expect(() =>
			validateCommandDef({
				...base,
				busyPolicy: "queue" as never,
			}),
		).toThrowError(/not in \{dispatch, reject, interrupt_then_dispatch\}/);
	});

	it("rejects duplicate aliases within one definition and alias===name", () => {
		expect(() =>
			validateCommandDef({ ...base, aliases: ["a", "a"] }),
		).toThrowError(/duplicate alias/);
		expect(() => validateCommandDef({ ...base, aliases: ["ok"] })).toThrowError(
			CommandDefValidationError,
		);
	});

	it("requires non-empty description and category", () => {
		expect(() =>
			validateCommandDef({ name: "x", description: "", category: "S" }),
		).toThrowError(/description/);
		expect(() =>
			validateCommandDef({ name: "x", description: "d", category: "" }),
		).toThrowError(/category/);
	});

	it("isValidCommandToken matches the accepted grammar", () => {
		expect(isValidCommandToken("reload-mcp")).toBe(true);
		expect(isValidCommandToken("reload_mcp")).toBe(true);
		expect(isValidCommandToken("")).toBe(false);
		expect(isValidCommandToken("/x")).toBe(false);
	});
});

describe("isTruthyValue (utils.py parity)", () => {
	it("truthy strings are exactly {1,true,yes,on}, case/space-insensitive", () => {
		for (const v of ["1", "true", "TRUE", " Yes ", "on"]) {
			expect(isTruthyValue(v)).toBe(true);
		}
		for (const v of ["0", "false", "no", "off", "", "enabled"]) {
			expect(isTruthyValue(v)).toBe(false);
		}
	});

	it("bool passthrough, nullish default, other types coerce via Boolean()", () => {
		expect(isTruthyValue(true)).toBe(true);
		expect(isTruthyValue(false)).toBe(false);
		expect(isTruthyValue(null)).toBe(false);
		expect(isTruthyValue(undefined)).toBe(false);
		expect(isTruthyValue(undefined, true)).toBe(true);
		expect(isTruthyValue(0)).toBe(false);
		expect(isTruthyValue([])).toBe(true);
	});
});
