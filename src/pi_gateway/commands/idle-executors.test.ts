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
import {
	buildIdleExecutors,
	compactIdleExecutor,
	type IdleExecutorContext,
	type IdleTurnRunner,
} from "./idle-executors.js";

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

describe("idle /compact executor (blocker 1)", () => {
	function ctxWith(
		runner: IdleTurnRunner,
		extra?: Partial<IdleExecutorContext>,
	): IdleExecutorContext {
		return {
			sessionKey: "chat-7",
			args: "",
			rows,
			runner,
			eventText: "/compact",
			...extra,
		};
	}

	function recordingRunner(): {
		turns: string[];
		sessions: string[];
		runner: IdleTurnRunner;
	} {
		const turns: string[] = [];
		const sessions: string[] = [];
		return {
			turns,
			sessions,
			runner: {
				handleTurn: async (request: {
					sessionId: string;
				routingKey: string;
				text: string;
			}) => {
					turns.push(request.text);
					throw new Error("compact must not consume a turn");
				},
				compactSession: async (sessionId: string) => {
					sessions.push(sessionId);
					return { summary: "condensed talk", tokensBefore: 42424 };
				},
			},
		};
	}

	it("runs the runner seam and replies with tokens plus summary", async () => {
		const stub = recordingRunner();
		const result = await compactIdleExecutor(
			ctxWith(stub.runner, { hostSessionId: "drive-9" }),
		);
		if (result.kind !== "reply") throw new Error("/compact must reply");
		expect(stub.sessions).toEqual(["drive-9"]);
		expect(result.text).toContain("Compacted 42424 tokens");
		expect(result.text).toContain("condensed talk");
		expect(stub.turns).toEqual([]);
	});

	it("falls back to the chat key without a resolved host session", async () => {
		const stub = recordingRunner();
		const result = await compactIdleExecutor(ctxWith(stub.runner));
		if (result.kind !== "reply") throw new Error("/compact must reply");
		expect(stub.sessions).toEqual(["chat-7"]);
	});

	it("without the runner seam stays passthrough on original bytes", async () => {
		const table = buildIdleExecutors(rows);
		const compact = table.get("compact");
		if (compact === undefined) throw new Error("/compact has no idle executor");
		const result = await compact(
			ctxWith(throwingRunner(), { eventText: "/compact focus" }),
		);
		expect(result).toEqual({ kind: "passthrough", text: "/compact focus" });
	});

	it("a host refusal renders as reply text, never a throw", async () => {
		const runner: IdleTurnRunner = {
			handleTurn: async () => {
				throw new Error("must not turn");
			},
			compactSession: async () => {
				throw new Error("Nothing to compact (session too small)");
			},
		};
		const result = await compactIdleExecutor(ctxWith(runner));
		if (result.kind !== "reply") throw new Error("refusal must reply");
		expect(result.text).toContain("Compaction failed");
		expect(result.text).toContain("Nothing to compact");
	});
});
