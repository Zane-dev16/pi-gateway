// Busy-policy resolver derivation (DEC-005) + guard feed projection.

import { describe, expect, it } from "vitest";
import type { CommandDef } from "./command-def.js";
import { BusyResolver, buildBusyLookup, toGuardRows } from "./busy-resolver.js";

const row = (name: string, extra: Partial<CommandDef>): CommandDef => ({
	name,
	description: `${name} description`,
	category: "Session",
	...extra,
});

const ROWS: CommandDef[] = [
	row("new", {
		aliases: ["reset"],
		busyPolicy: "interrupt_then_dispatch",
		busyHandler: "new",
	}),
	row("stop", { busyPolicy: "interrupt_then_dispatch", busyHandler: "stop" }),
	row("queue", {
		aliases: ["q"],
		argsHint: "<prompt>",
		busyPolicy: "dispatch",
		busyHandler: "queue",
	}),
	row("model", { busyPolicy: "reject", busyHandler: "model" }),
	row("title", {}), // default reject, no handler
];

describe("toGuardRows — minimal L2 feed projection", () => {
	it("projects exactly the guard-consumed fields; schema extras never leak", () => {
		const [projected] = toGuardRows([
			row("background", {
				aliases: ["bg"],
				busyPolicy: "dispatch",
				argsHint: "<prompt>",
				gatewayConfigGate: "x.y",
			}),
		]);
		expect(projected).toEqual({
			name: "background",
			aliases: ["bg"],
			busyPolicy: "dispatch",
		});
		expect(Object.keys(projected ?? {}).sort()).toEqual([
			"aliases",
			"busyPolicy",
			"name",
		]);
	});

	it("absent optional fields stay absent (no explicit undefined under exactOptionalPropertyTypes)", () => {
		const [projected] = toGuardRows([row("bare", {})]);
		expect(projected).toEqual({ name: "bare" });
	});
});

describe("BusyResolver predicates (commands.py parity)", () => {
	const resolver = BusyResolver.fromRows(ROWS);

	it("resolves names AND aliases case/slash-insensitively; unknown → null", () => {
		expect(resolver.resolve("/RESET")?.name).toBe("new");
		expect(resolver.resolve("Q")?.name).toBe("queue");
		expect(resolver.resolve("foo")).toBeNull();
	});

	it("shouldBypassActiveSession: EVERY resolvable command bypasses; unknown does not", () => {
		for (const name of [
			"new",
			"reset",
			"stop",
			"queue",
			"q",
			"model",
			"title",
		]) {
			expect(resolver.shouldBypassActiveSession(name)).toBe(true);
		}
		expect(resolver.shouldBypassActiveSession("/foo")).toBe(false);
		expect(resolver.shouldBypassActiveSession(null)).toBe(false);
	});

	it("interrupt-class routing resolves THROUGH aliases ('reset' → /new)", () => {
		expect(resolver.isInterruptThenDispatch("reset")).toBe(true);
		expect(resolver.isInterruptThenDispatch("stop")).toBe(true);
		expect(resolver.isInterruptThenDispatch("queue")).toBe(false);
		expect(resolver.isInterruptThenDispatch("model")).toBe(false);
	});

	it("bypassCommandNames derives policy≠reject canonicals from rows only", () => {
		expect([...resolver.bypassCommandNames()].sort()).toEqual([
			"new",
			"queue",
			"stop",
		]);
	});

	it("policyOf applies the DEC-005 default and returns null for unknowns", () => {
		expect(resolver.policyOf("title")).toBe("reject");
		expect(resolver.policyOf("q")).toBe("dispatch");
		expect(resolver.policyOf("foo")).toBeNull();
	});
});

describe("live lookup wiring", () => {
	it("fromLookup sees runtime registrations made after construction", () => {
		const lookup = new Map(buildBusyLookup([row("help", {})]));
		const resolver = BusyResolver.fromLookup(lookup);
		expect(resolver.resolve("late")).toBeNull();
		lookup.set("late", row("late", { busyPolicy: "dispatch" }));
		expect(resolver.resolve("/late")?.name).toBe("late");
		expect(resolver.policyOf("late")).toBe("dispatch");
	});
});
