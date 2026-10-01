// idle-executors.test.ts — DEC-078 idle executor behavior contracts.
//
// The table owns exactly the commands with a REAL local execution on the
// cold path. These contracts call the executors the way guard-wiring does
// (live builtin rows, stub runner) and assert the observed reply text
// against literals — never the table's own shape.

import { describe, expect, it } from "vitest";
import {
	BUILTIN_COMMAND_ROWS,
	createBuiltinCommandRegistry,
} from "./builtins.js";
import { buildIdleExecutors } from "./idle-executors.js";

const rows = createBuiltinCommandRegistry().rows();

function throwingRunner(): {
	handleTurn(request: {
		sessionId: string;
		routingKey: string;
		text: string;
	}): Promise<never>;
} {
	return {
		handleTurn: async () => {
			throw new Error("help must answer locally — zero model calls");
		},
	};
}

describe("idle /help executor (DEC-078)", () => {
	it("answers from the live registry: pi names in, Hermes-only rows out", async () => {
		const table = buildIdleExecutors(rows);
		const help = table.get("help");
		if (help === undefined) throw new Error("/help has no idle executor");
		const result = await help({
			sessionKey: "s",
			args: "",
			rows,
			runner: throwingRunner(),
			eventText: "/help",
		});
		if (result.kind !== "reply" || result.text === null) {
			throw new Error("/help must reply with text");
		}
		expect(result.text).toContain("`/compact");
		expect(result.text).toContain("`/new");
		expect(result.text).toContain("`/resume");
		expect(result.text).not.toContain("subscription");
		expect(result.text).not.toContain("compress");
	});

	it("derives from the rows it is handed, not a frozen list", async () => {
		const table = buildIdleExecutors(BUILTIN_COMMAND_ROWS);
		const help = table.get("help");
		if (help === undefined) throw new Error("/help has no idle executor");
		const slim = BUILTIN_COMMAND_ROWS.filter(
			(r) => r.name === "compact" || r.name === "help",
		);
		const result = await help({
			sessionKey: "s",
			args: "",
			rows: slim,
			runner: throwingRunner(),
			eventText: "/help",
		});
		if (result.kind !== "reply" || result.text === null) {
			throw new Error("/help must reply with text");
		}
		expect(result.text).toContain("`/compact");
		expect(result.text).not.toContain("`/new");
	});
});
