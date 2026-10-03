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
	exportIdleExecutor,
	modelIdleExecutor,
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

describe("idle /model executor", () => {
	function ctxWithModel(
		runner: IdleTurnRunner,
		args: string,
		extra?: Partial<IdleExecutorContext>,
	): IdleExecutorContext {
		return {
			sessionKey: "chat-7",
			args,
			rows,
			runner,
			eventText: args === "" ? "/model" : `/model ${args}`,
			...extra,
		};
	}

	function modelRunner(): {
		turns: string[];
		sessions: Array<{ sessionId: string; ref: string }>;
		runner: IdleTurnRunner;
	} {
		const turns: string[] = [];
		const sessions: Array<{ sessionId: string; ref: string }> = [];
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
					throw new Error("model must not consume a turn");
				},
				setSessionModel: async (sessionId: string, ref: string) => {
					sessions.push({ sessionId, ref });
					if (ref === "fauxb/faux-2") return { provider: "fauxb", id: "faux-2" };
					throw new Error(`Unknown model "${ref}".`);
				},
				getSessionModel: async () => ({ provider: "faux", id: "faux-1" }),
				listAvailableModels: () => [
					{ provider: "faux", id: "faux-1" },
					{ provider: "fauxb", id: "faux-2" },
				],
			},
		};
	}

	it("switches through the runner seam and replies with the new identity", async () => {
		const stub = modelRunner();
		const result = await modelIdleExecutor(
			ctxWithModel(stub.runner, "fauxb/faux-2", { hostSessionId: "drive-9" }),
		);
		if (result.kind !== "reply") throw new Error("/model must reply");
		expect(stub.sessions).toEqual([
			{ sessionId: "drive-9", ref: "fauxb/faux-2" },
		]);
		expect(result.text).toBe("Model: fauxb/faux-2");
		expect(stub.turns).toEqual([]);
	});

	it("bare /model lists the current plus the catalog", async () => {
		const stub = modelRunner();
		const result = await modelIdleExecutor(ctxWithModel(stub.runner, ""));
		if (result.kind !== "reply") throw new Error("bare /model must reply");
		expect(result.text).toContain("Current model: faux/faux-1");
		expect(result.text).toContain("fauxb/faux-2");
		expect(stub.turns).toEqual([]);
	});

	it("an unknown ref renders as reply text, never a throw", async () => {
		const stub = modelRunner();
		const result = await modelIdleExecutor(
			ctxWithModel(stub.runner, "nope/nothing"),
		);
		if (result.kind !== "reply") throw new Error("refusal must reply");
		expect(result.text).toContain("Model switch failed");
		expect(result.text).toContain("nope/nothing");
		expect(stub.turns).toEqual([]);
	});

	it("without the runner seam stays passthrough on original bytes", async () => {
		const table = buildIdleExecutors(rows);
		const model = table.get("model");
		if (model === undefined) throw new Error("/model has no idle executor");
		const result = await model(
			ctxWithModel(throwingRunner(), "fauxb/faux-2", {
				eventText: "/model fauxb/faux-2",
			}),
		);
		expect(result).toEqual({ kind: "passthrough", text: "/model fauxb/faux-2" });
	});
});

describe("idle /export executor", () => {
	function ctxWithExport(
		runner: IdleTurnRunner,
		extra?: Partial<IdleExecutorContext>,
	): IdleExecutorContext {
		return {
			sessionKey: "chat-7",
			args: "",
			rows,
			runner,
			eventText: "/export",
			...extra,
		};
	}

	it("renders the runner bytes inline and consumes no turn", async () => {
		const turns: string[] = [];
		const sessions: string[] = [];
		const runner: IdleTurnRunner = {
			handleTurn: async (request: { sessionId: string; text: string }) => {
				turns.push(request.text);
				throw new Error("export must not consume a turn");
			},
			exportSessionJsonl: async (sessionId: string) => {
				sessions.push(sessionId);
				return '{"type":"session"}\n{"role":"user"}';
			},
		};
		const result = await exportIdleExecutor(
			ctxWithExport(runner, { hostSessionId: "drive-9" }),
		);
		if (result.kind !== "reply") throw new Error("/export must reply");
		expect(sessions).toEqual(["drive-9"]);
		expect(result.text).toContain('"type":"session"');
		expect(turns).toEqual([]);
	});

	it("without the runner seam stays passthrough on original bytes", async () => {
		const table = buildIdleExecutors(rows);
		const exporter = table.get("export");
		if (exporter === undefined) throw new Error("/export has no idle executor");
		const result = await exporter(
			ctxWithExport(throwingRunner(), { eventText: "/export out.jsonl" }),
		);
		expect(result).toEqual({ kind: "passthrough", text: "/export out.jsonl" });
	});

	it("a host refusal renders as reply text, never a throw", async () => {
		const runner: IdleTurnRunner = {
			handleTurn: async () => {
				throw new Error("must not turn");
			},
			exportSessionJsonl: async () => {
				throw new Error("runner is closed");
			},
		};
		const result = await exportIdleExecutor(ctxWithExport(runner));
		if (result.kind !== "reply") throw new Error("refusal must reply");
		expect(result.text).toContain("Export failed");
	});

	it("/export html renders the branch as an HTML document", async () => {
		const turns: string[] = [];
		const runner: IdleTurnRunner = {
			handleTurn: async (request: { sessionId: string; text: string }) => {
				turns.push(request.text);
				throw new Error("export must not consume a turn");
			},
			exportSessionJsonl: async () =>
				'{"type":"session","id":"s1"}\n{"role":"user","content":"hello <b>world</b>"}',
		};
		const result = await exportIdleExecutor(
			ctxWithExport(runner, { args: "html", eventText: "/export html" }),
		);
		if (result.kind !== "reply") throw new Error("/export html must reply");
		if (result.text === null) throw new Error("/export html must send text");
		expect(result.text.startsWith("<!DOCTYPE html>")).toBe(true);
		expect(result.text).toContain("hello &lt;b&gt;world&lt;/b&gt;");
		expect(result.text).not.toContain("<b>world</b>");
		expect(result.text.trimEnd().endsWith("</html>")).toBe(true);
		expect(turns).toEqual([]);
	});

	it("/export transcript.html renders HTML while bare stays JSONL", async () => {
		const runner: IdleTurnRunner = {
			handleTurn: async () => {
				throw new Error("export must not consume a turn");
			},
			exportSessionJsonl: async () => '{"type":"session"}\n{"role":"user"}',
		};
		const html = await exportIdleExecutor(
			ctxWithExport(runner, {
				args: "transcript.html",
				eventText: "/export transcript.html",
			}),
		);
		if (html.kind !== "reply") throw new Error("*.html must reply");
		expect(html.text).toContain("<!DOCTYPE html>");
		const jsonl = await exportIdleExecutor(ctxWithExport(runner));
		if (jsonl.kind !== "reply") throw new Error("bare /export must reply");
		expect(jsonl.text).toContain('{"type":"session"}');
		expect(jsonl.text).not.toContain("<!DOCTYPE html>");
	});
});
