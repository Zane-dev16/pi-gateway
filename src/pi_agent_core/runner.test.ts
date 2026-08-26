// Behavior contracts: happy-path final delivery, cache stability (byte-
// identical system prompt + toolset across consecutive turns), DEC-015
// alternation repair as a PRE-CALL chokepoint on the WIRE (every model call,
// INCLUDING the freshly appended user turn; persisted bytes untouched), the
// two-layer turn-lease prologue (02 §5 / DEC-004), the UNLIMITED turn-limit
// default (config.py:TURN_LIMIT_UNLIMITED parity), and the periodic agent-
// cache idle sweep (_session_expiry_watcher wiring).

import { describe, expect, it } from "vitest";

import { Type, defineTool } from "./host.js";
import type { Context } from "./host.js";
import {
	SESSION_EXPIRY_SWEEP_INTERVAL_MS,
	SessionTurnLeaseTimeoutError,
	TURN_LIMIT_UNLIMITED,
	type IntervalHandle,
	type RunnerTurnLeaseRegistry,
} from "./runner.js";
import {
	createRunnerHarness,
	type RunnerHarness,
} from "./testing/runner-harness.js";
import {
	fauxAssistantMessage,
	fauxProvider,
	fauxToolCall,
} from "./testing/faux-model.js";
import { structuredHolder, type MessageRow } from "../pi_state/index.js";
import { rowToLoopMessage } from "./runner.js";

describe("runner loop — happy path", () => {
	it("drives the REAL host loop to final text and persists both rows", async () => {
		const h = await createRunnerHarness();
		try {
			h.ensureSession("sess-1");
			h.faux.setResponses([fauxAssistantMessage("Hello from the host loop")]);
			const outcome = await h.runner.handleTurn({
				sessionId: "sess-1",
				routingKey: "agent:main:test:dm:c1",
				text: "hi there",
			});
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("Hello from the host loop");
			expect(outcome.iterations).toBe(1);
			expect(outcome.repairs).toBe(0);

			// Persisted rows: user row (clean display content) + assistant row.
			const rows = h.store.listMessages("sess-1");
			expect(rows.map((r) => r.role)).toEqual(["user", "assistant"]);
			expect(rows[0]?.content).toBe("hi there");
			expect(rows[0]?.api_content).toBe("hi there"); // composed == clean here
			expect(rows[1]?.content).toBe("Hello from the host loop");

			// api_content sidecar binds the EXACT wire bytes of the assistant turn.
			const expectedWire = JSON.stringify({
				role: "assistant",
				content: [{ type: "text", text: "Hello from the host loop" }],
			});
			expect(h.store.getApiContent(rows[1]!.id)).toBe(expectedWire);
			expect(outcome.assistantRowId).toBe(rows[1]!.id);
			expect(outcome.userRowId).toBe(rows[0]!.id);
		} finally {
			await h.close();
		}
	});

	it("a tool-calling turn iterates inside ONE prompt and delivers the final text", async () => {
		const h = await createRunnerHarness({ maxIterations: 5 });
		try {
			h.ensureSession("sess-tool");
			h.faux.setResponses([
				fauxAssistantMessage([
					fauxToolCall("echo", { say: "step" }, { id: "tc-1" }),
				]),
				fauxAssistantMessage("all done"),
			]);
			const outcome = await h.runner.handleTurn({
				sessionId: "sess-tool",
				routingKey: "rk",
				text: "do the thing",
			});
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.finalText).toBe("all done");
			expect(outcome.iterations).toBe(2); // two model calls, one prompt()
			expect(h.faux.state.callCount).toBe(2);
			// Rows: user, assistant(toolCall), tool(result), assistant(final).
			// The host loop's internal assistant+toolResult pair is session-local
			// (in-memory SessionManager); the gateway persists the authoritative
			// final payload per turn plus the user row.
			const roles = h.store.listMessages("sess-tool").map((r) => r.role);
			expect(roles).toEqual(["user", "assistant"]);
		} finally {
			await h.close();
		}
	});
});

describe("cache stability (05 §8)", () => {
	it("two consecutive turns observe byte-identical system prompt + toolset hash via the REAL request context", async () => {
		const seen: Array<{ sys: string; tools: string; n: number }> = [];
		const h = await createRunnerHarness({
			systemPrompt: "STABLE SYSTEM PROMPT BYTES v1",
		});
		try {
			h.ensureSession("cache-sess");
			h.faux.setResponses([]);
			h.faux.appendResponses([
				(context: Context) => {
					seen.push({
						sys: context.systemPrompt ?? "",
						tools: JSON.stringify((context.tools ?? []).map((t) => t.name)),
						n: seen.length + 1,
					});
					return fauxAssistantMessage(`reply ${seen.length}`);
				},
				(context: Context) => {
					seen.push({
						sys: context.systemPrompt ?? "",
						tools: JSON.stringify((context.tools ?? []).map((t) => t.name)),
						n: seen.length + 1,
					});
					return fauxAssistantMessage(`reply ${seen.length}`);
				},
			]);
			await h.runner.handleTurn({
				sessionId: "cache-sess",
				routingKey: "rk",
				text: "turn one",
			});
			await h.runner.handleTurn({
				sessionId: "cache-sess",
				routingKey: "rk",
				text: "turn two",
			});
			expect(seen).toHaveLength(2);
			// The configured override anchors the prompt (the host composes
			// further deterministic sections around it).
			expect(seen[0]!.sys.startsWith("STABLE SYSTEM PROMPT BYTES v1")).toBe(
				true,
			);
			// Byte equality across turns: same cached session → identical prefix.
			expect(seen[1]!.sys).toBe(seen[0]!.sys);
			expect(seen[1]!.tools).toBe(seen[0]!.tools);
			// The cache actually held ONE entry for both turns.
			expect(h.runner.cacheStats.entries).toBe(1);
		} finally {
			await h.close();
		}
	});
});

describe("alternation repair PRE-CALL CHOKEPOINT (DEC-015)", () => {
	/** Flatten a wire user message's content to plain text. */
	const userText = (content: unknown): string => {
		if (typeof content === "string") return content;
		return (content as Array<{ type: string; text?: string }>)
			.filter((b) => b.type === "text")
			.map((b) => b.text ?? "")
			.join("");
	};

	it("compacts a crash-tail user→user→fresh-ask onto ONE wire user; persisted rows untouched", async () => {
		const h = await createRunnerHarness();
		try {
			h.ensureSession("repair-sess");
			// Seed malformed history DIRECTLY as rows (multi-queue replay shape):
			// two durable users adjacent, neither ever answered.
			await h.store.appendMessage({
				sessionId: "repair-sess",
				role: "user",
				content: "first queued message",
				apiContent: "first queued message",
			});
			await h.store.appendMessage({
				sessionId: "repair-sess",
				role: "user",
				content: "second queued message",
				apiContent: "second queued message",
			});
			const beforeRows = h.store.listMessages("repair-sess");

			const wireUserContents: string[] = [];
			h.faux.setResponses([
				(context: Context) => {
					for (const m of context.messages) {
						if (m.role === "user") {
							wireUserContents.push(userText(m.content));
						}
					}
					return fauxAssistantMessage("repaired");
				},
			]);

			const outcome = await h.runner.handleTurn({
				sessionId: "repair-sess",
				routingKey: "rk",
				text: "live third message",
			});
			expect(outcome.repairs).toBe(2); // tail pair merge, then tail+fresh-ask merge
			expect(outcome.exitReason).toBe("finalized");

			// Wire copy repaired at THE API CALL (conversation_loop parity): the
			// chokepoint sees the freshly appended ask, so exactly ONE user
			// message reaches the model — no input lost, NO user;user adjacency.
			expect(wireUserContents).toEqual([
				"first queued message\n\nsecond queued message\n\nlive third message",
			]);

			// Persisted bytes UNTOUCHED: the two original rows keep their own
			// content AND sidecar bytes (no rewrite outside compression).
			const afterRows = h.store
				.listMessages("repair-sess")
				.filter((r) => r.role === "user");
			expect(afterRows).toHaveLength(3);
			expect(afterRows[0]!.id).toBe(beforeRows[0]!.id);
			expect(afterRows[0]!.content).toBe("first queued message");
			expect(afterRows[0]!.api_content).toBe("first queued message");
			expect(afterRows[1]!.id).toBe(beforeRows[1]!.id);
			expect(afterRows[1]!.content).toBe("second queued message");
			expect(afterRows[1]!.api_content).toBe("second queued message");
		} finally {
			await h.close();
		}
	});

	it("a SINGLE crash-orphaned user row merges WITH the fresh ask pre-request; every model call of the turn stays alternation-clean", async () => {
		// The shape the old one-shot pre-append pass missed: repair ran before
		// prompt() appended the live user turn, so a trailing durable row
		// (crash between the user-row persist and the assistant reply) plus the
		// new ask reached the provider as consecutive users.
		const h = await createRunnerHarness();
		try {
			h.ensureSession("crash-tail");
			await h.store.appendMessage({
				sessionId: "crash-tail",
				role: "user",
				content: "queued before the crash",
				apiContent: "queued before the crash",
			});

			const requestUserShapes: Array<Array<{ role: string; text: string }>> =
				[];
			const capture = (context: Context): void => {
				requestUserShapes.push(
					context.messages.map((m) => ({
						role: m.role,
						text: m.role === "user" ? userText(m.content) : "",
					})),
				);
			};
			h.faux.setResponses([
				(context: Context) => {
					capture(context);
					return fauxAssistantMessage([
						fauxToolCall("echo", { say: "step" }, { id: "tc-1" }),
					]);
				},
				(context: Context) => {
					capture(context);
					return fauxAssistantMessage("done");
				},
			]);

			const outcome = await h.runner.handleTurn({
				sessionId: "crash-tail",
				routingKey: "rk",
				text: "live after restart",
			});
			expect(outcome.exitReason).toBe("finalized");
			expect(outcome.repairs).toBeGreaterThanOrEqual(1);

			// Request 1: orphaned tail + fresh ask merged into ONE user message.
			expect(requestUserShapes[0]).toEqual([
				{ role: "user", text: "queued before the crash\n\nlive after restart" },
			]);
			// Request 2 (post-toolResult): still exactly one, merged user — the
			// chokepoint re-runs before EVERY model call.
			expect(requestUserShapes[1]!.map((m) => m.role)).toEqual([
				"user",
				"assistant",
				"toolResult",
			]);
			for (const shape of requestUserShapes) {
				expect(shape.filter((m) => m.role === "user")).toEqual([
					{
						role: "user",
						text: "queued before the crash\n\nlive after restart",
					},
				]);
			}
			for (const shape of requestUserShapes) {
				for (let i = 1; i < shape.length; i++) {
					expect(
						shape[i]!.role === "user" && shape[i - 1]!.role === "user",
						`adjacent user;user pair leaked into a request`,
					).toBe(false);
				}
			}

			// Durable rows stay byte-distinct: orphan row + this turn's own row.
			const rows = h.store
				.listMessages("crash-tail")
				.filter((r) => r.role === "user");
			expect(rows.map((r) => r.content)).toEqual([
				"queued before the crash",
				"live after restart",
			]);
		} finally {
			await h.close();
		}
	});

	it("TWO-PROCESS interleave: a ghost process's crash tail discovered on lease-wait resume never reaches the wire as user;user", async () => {
		// Cross-process shape for the same hazard: process A holds the durable
		// turn lease, persists its user row, then dies without replying (the
		// ghost release below models the crash); process B waits on the lease,
		// resumes through the waited path (resume-tip re-resolve + transcript
		// reload), appends its own ask — and the pre-call chokepoint must send
		// ONE merged user, never the ghost tail + ask as consecutive users.
		const h = await createRunnerHarness({
			withTurnLeases: true,
			leasePollIntervalSeconds: 0.05,
		});
		try {
			h.ensureSession("interleave");
			const ghost = structuredHolder("ghost-process", process.pid);
			expect(h.store.leases.tryAcquire("interleave", ghost)).toBe(true);

			const wireUserContents: string[] = [];
			h.faux.setResponses([
				(context: Context) => {
					for (const m of context.messages) {
						if (m.role === "user") {
							wireUserContents.push(userText(m.content));
						}
					}
					return fauxAssistantMessage("resumed cleanly");
				},
			]);

			const turnPromise = h.runner
				.handleTurn({
					sessionId: "interleave",
					routingKey: "rk",
					text: "waiter ask",
				})
				.catch((err: unknown) => ({ error: err }));

			// While the waiter polls, the ghost process's last acts land: its
			// durable user row, then the crash (holder released, nothing replied).
			await h.store.appendMessage({
				sessionId: "interleave",
				role: "user",
				content: "ghost process ask",
				apiContent: "ghost process ask",
				timestamp: Date.now() / 1000,
			});
			h.store.leases.releaseHolder("interleave", ghost);

			const result = (await turnPromise) as
				| Awaited<ReturnType<RunnerHarness["runner"]["handleTurn"]>>
				| { error: unknown };
			if ("error" in result) throw result.error;
			expect(result.exitReason).toBe("finalized");

			// The waited turn reloaded the transcript (ghost tail present) and
			// STILL merged it with the fresh ask before the request went out.
			expect(wireUserContents).toEqual(["ghost process ask\n\nwaiter ask"]);

			// Both processes' rows persist byte-distinct.
			const rows = h.store
				.listMessages("interleave")
				.filter((r) => r.role === "user");
			expect(rows.map((r) => r.content)).toEqual([
				"ghost process ask",
				"waiter ask",
			]);
		} finally {
			await h.close();
		}
	});

	it("corrupt stored tool_calls JSON repairs instead of silently dropping calls", () => {
		// parseToolCalls used to map unparseable tool_calls to [] at replay
		// seeding — dropping the CALL while its persisted result survived as an
		// orphan (_repair_tool_call_arguments family parity).
		const model = fauxProvider().getModel();

		// Truncated ARRAY tail (crash between writes): closable by the ladder.
		const truncated = rowToLoopMessage(
			makeAssistantRow(
				'[ {"id":"tc-1","name":"echo","arguments":{"say":"hi"}}',
			),
			model,
		);
		expect(toolCallBlocksOf(truncated)).toHaveLength(1);
		expect(toolCallBlocksOf(truncated)[0]!.arguments).toEqual({ say: "hi" });

		// Corrupt ARGUMENTS STRING inside otherwise-valid JSON: repaired to an
		// object instead of degrading.
		const badArgs = rowToLoopMessage(
			makeAssistantRow(
				'[ {"id":"tc-2","name":"echo","arguments":"{\\"x\\": 1,}"} ]',
			),
			model,
		);
		expect(toolCallBlocksOf(badArgs)[0]!.arguments).toEqual({ x: 1 });

		// Empty tool_calls arrays still yield NO blocks (drop-empty parity).
		expect(
			toolCallBlocksOf(rowToLoopMessage(makeAssistantRow("[]"), model)),
		).toHaveLength(0);
	});
});

function makeAssistantRow(toolCalls: string): MessageRow {
	return {
		id: 1,
		session_id: "seed",
		role: "assistant",
		content: "",
		api_content: null,
		tool_call_id: null,
		tool_calls: toolCalls,
		tool_name: null,
		effect_disposition: null,
		finish_reason: null,
		token_count: null,
		reasoning: null,
		reasoning_content: null,
		reasoning_details: null,
		codex_reasoning_items: null,
		codex_message_items: null,
		platform_message_id: null,
		observed: 0,
		active: 1,
		compacted: 0,
		timestamp: 1_700_000_000,
		display_kind: null,
		display_metadata: null,
	};
}

function toolCallBlocksOf(message: {
	content: unknown;
}): Array<{ type: string; arguments?: unknown }> {
	return (
		message.content as Array<{ type: string; arguments?: unknown }>
	).filter((b) => b.type === "toolCall");
}

// ---------------------------------------------------------------------------
// Turn-limit default (hermes_cli/config.py:TURN_LIMIT_UNLIMITED parity).

describe("turn-limit default is UNLIMITED (config.py:resolve_turn_limit)", () => {
	it("the sentinel is the JS parity of sys.maxsize", () => {
		expect(TURN_LIMIT_UNLIMITED).toBe(Number.MAX_SAFE_INTEGER);
	});

	it("an unconfigured runner does NOT force-abort long turns at 32 calls", async () => {
		// The old default (32 + 1 grace) killed every turn past 33 model calls;
		// Hermes default is UNLIMITED — only a configured agent.max_turns caps.
		const h = await createRunnerHarness();
		try {
			h.ensureSession("unlimited");
			const responses = [];
			for (let i = 0; i < 40; i++) {
				responses.push(
					fauxAssistantMessage([fauxToolCall("x", {}, { id: `t${i}` })]),
				);
			}
			responses.push(fauxAssistantMessage("finally done"));
			h.faux.setResponses(responses);
			const outcome = await h.runner.handleTurn({
				sessionId: "unlimited",
				routingKey: "rk",
				text: "long research",
			});
			expect(outcome.exitReason).toBe("finalized"); // NOT budget_exhausted
			expect(outcome.iterations).toBe(41); // 40 tool iterations + final
		} finally {
			await h.close();
		}
	}, 20_000);
});

// ---------------------------------------------------------------------------
// Agent-cache idle sweep wiring (gateway/run.py:_session_expiry_watcher).

describe("periodic cache idle sweep (_session_expiry_watcher wiring)", () => {
	interface Ticker {
		fn: () => void;
		ms: number;
	}

	function manualTimers() {
		const tickers: Ticker[] = [];
		return {
			tickers,
			startInterval(fn: () => void, ms: number): IntervalHandle {
				const t: Ticker = { fn, ms };
				tickers.push(t);
				return {
					cancel: () => {
						const idx = tickers.indexOf(t);
						if (idx >= 0) tickers.splice(idx, 1);
					},
				};
			},
		};
	}

	function makeCacheClock() {
		let t = 1_000_000;
		return { advance: (ms: number) => (t += ms), now: () => t };
	}

	it("schedules sweepIdle on an unref'd-equivalent timer at the 300s cadence", async () => {
		const timers = manualTimers();
		const clock = makeCacheClock();
		const h = await createRunnerHarness({
			cacheOptions: { now: clock.now, idleTtlMs: 5_000 },
			startInterval: timers.startInterval,
		});
		try {
			// Exactly ONE sweep timer, at the watcher cadence.
			expect(timers.tickers).toHaveLength(1);
			expect(timers.tickers[0]!.ms).toBe(SESSION_EXPIRY_SWEEP_INTERVAL_MS);

			h.ensureSession("sweep-sess");
			h.faux.setResponses([fauxAssistantMessage("cached")]);
			await h.runner.handleTurn({
				sessionId: "sweep-sess",
				routingKey: "rk",
				text: "hi",
			});
			expect(h.runner.cacheStats.entries).toBe(1);

			// Entry fresh → a tick keeps it.
			timers.tickers[0]!.fn();
			expect(h.runner.cacheStats.entries).toBe(1);

			// Age past the idle TTL → the next tick sweeps it.
			clock.advance(6_000);
			timers.tickers[0]!.fn();
			expect(h.runner.cacheStats.entries).toBe(0);
		} finally {
			await h.close();
		}
		// close() cancels the sweep timer.
		expect(timers.tickers).toHaveLength(0);
	});
});

// ---------------------------------------------------------------------------
// Two-layer turn-lease prologue (02 §5 / DEC-004; run_agent.py turn prologue).

/** L1 registry double: records acquire/release; optional foreign holder. */
class FakeTurnLeaseRegistry implements RunnerTurnLeaseRegistry {
	acquired: Array<{
		sessionId: string;
		ownerKey: string;
		generation: number;
	}> = [];
	releasedTokens: unknown[] = [];
	foreignHolder = false;

	async acquire(
		sessionId: string,
		options: { ownerKey: string; generation: number },
	): Promise<unknown> {
		this.acquired.push({ sessionId, ...options });
		if (this.foreignHolder) {
			// gateway/turn_lease.py TurnLeaseTimeoutError shape.
			const err = new Error(
				`turn lease wait timed out on session ${sessionId}`,
			) as Error & { name: string };
			err.name = "TurnLeaseTimeoutError";
			throw err;
		}
		return { sessionId, ownerKey: options.ownerKey };
	}

	release(token: unknown): boolean {
		this.releasedTokens.push(token);
		return true;
	}
}

/** A tool whose execution is released by an external gate. */
function gatedTool(name = "gate_tool") {
	let release: () => void = () => {};
	const gate = new Promise<void>((r) => {
		release = r;
	});
	let markEntered: () => void = () => {};
	const entered = new Promise<void>((r) => {
		markEntered = r;
	});
	const tool = defineTool({
		name,
		label: name,
		description: "blocks until released",
		parameters: Type.Object({}),
		execute: async (_id, _params, signal) => {
			markEntered();
			await Promise.race([
				gate,
				new Promise<void>((resolve) => {
					signal?.addEventListener("abort", () => resolve(), {
						once: true,
					});
				}),
			]);
			return {
				content: [{ type: "text", text: "gate released" }],
				details: {},
			};
		},
	});
	return { tool, release: () => release(), entered };
}

describe("two-layer turn-lease prologue (02 §5)", () => {
	it("a turn holds BOTH layers and releases them in finally", async () => {
		const registry = new FakeTurnLeaseRegistry();
		const h = await createRunnerHarness({
			withTurnLeases: true,
			turnLeaseRegistry: registry,
		});
		try {
			h.ensureSession("lease-1");
			let holderDuringTurn: string | null = null;
			h.faux.setResponses([
				(context: Context) => {
					holderDuringTurn =
						h.store.leases.probeOwner("lease-1")?.holder ?? null;
					return fauxAssistantMessage("under lease");
				},
			]);
			const outcome = await h.runner.handleTurn({
				sessionId: "lease-1",
				routingKey: "agent:main:telegram:dm:42",
				text: "go",
			});
			expect(outcome.exitReason).toBe("finalized");

			// L1 acquired with routing key + generation 1, released afterwards.
			expect(registry.acquired).toEqual([
				{
					sessionId: "lease-1",
					ownerKey: "agent:main:telegram:dm:42",
					generation: 1,
				},
			]);
			expect(registry.releasedTokens).toHaveLength(1);

			// Durable layer held DURING the turn with a structured pid/turn/
			// platform holder, and RELEASED after it.
			expect(holderDuringTurn).toContain(`pid=${String(process.pid)}`);
			expect(holderDuringTurn).toContain("turn=g1:");
			expect(holderDuringTurn).toContain("platform=telegram");
			expect(h.store.leases.probeOwner("lease-1")).toBeNull();
		} finally {
			await h.close();
		}
	});

	it("L1 timeout propagates fail-closed (caller sends the resend notice)", async () => {
		const registry = new FakeTurnLeaseRegistry();
		registry.foreignHolder = true;
		const h = await createRunnerHarness({
			withTurnLeases: true,
			turnLeaseRegistry: registry,
		});
		try {
			h.ensureSession("busy-alias");
			await expect(
				h.runner.handleTurn({
					sessionId: "busy-alias",
					routingKey: "rk-b",
					text: "rival ask",
				}),
			).rejects.toMatchObject({ name: "TurnLeaseTimeoutError" });

			// The durable layer was never touched by the loser.
			expect(h.store.leases.probeOwner("busy-alias")).toBeNull();

			// Once free, the SAME session turns normally.
			registry.foreignHolder = false;
			h.faux.setResponses([fauxAssistantMessage("free now")]);
			const outcome = await h.runner.handleTurn({
				sessionId: "busy-alias",
				routingKey: "rk-a",
				text: "ask again",
			});
			expect(outcome.exitReason).toBe("finalized");
		} finally {
			await h.close();
		}
	});

	it("durable-lease contention waits, then times out FAIL-CLOSED (resend notice)", async () => {
		const h = await createRunnerHarness({
			withTurnLeases: true,
			leaseWaitSeconds: 0.3,
			leasePollIntervalSeconds: 0.05,
		});
		try {
			h.ensureSession("contended");
			// Same-process ghost holder models another LIVE process: PID-liveness
			// must never reclaim it, so the waiter burns its whole wait budget.
			const ghost = structuredHolder("ghost-process", process.pid);
			expect(h.store.leases.tryAcquire("contended", ghost)).toBe(true);

			await expect(
				h.runner.handleTurn({
					sessionId: "contended",
					routingKey: "rk",
					text: "blocked ask",
				}),
			).rejects.toBeInstanceOf(SessionTurnLeaseTimeoutError);

			// Release; the next turn acquires and completes.
			h.store.leases.releaseHolder("contended", ghost);
			h.faux.setResponses([fauxAssistantMessage("admitted")]);
			const outcome = await h.runner.handleTurn({
				sessionId: "contended",
				routingKey: "rk",
				text: "retry ask",
			});
			expect(outcome.exitReason).toBe("finalized");
		} finally {
			await h.close();
		}
	});

	it("waited ⇒ resume-tip re-resolve: the turn continues on the rotated tip", async () => {
		const h = await createRunnerHarness({
			withTurnLeases: true,
			leasePollIntervalSeconds: 0.05,
		});
		try {
			h.ensureSession("rot");
			const ghost = structuredHolder("ghost-process", process.pid);
			expect(h.store.leases.tryAcquire("rot", ghost)).toBe(true);

			h.faux.setResponses([fauxAssistantMessage("post-wait reply")]);
			const turnPromise = h.runner
				.handleTurn({
					sessionId: "rot",
					routingKey: "rk",
					text: "post-wait ask",
				})
				.catch((err: unknown) => ({ error: err }));

			// While the waiter polls, another process compresses+rotates:
			h.store.db
				.prepare(
					"UPDATE sessions SET end_reason = 'compression' WHERE id = 'rot'",
				)
				.run();
			h.store.db
				.prepare(
					"INSERT INTO sessions (id, parent_session_id, source, started_at) VALUES ('rot2', 'rot', 'gateway', ?)",
				)
				.run(Math.floor(Date.now() / 1000));
			await h.store.appendMessage({
				sessionId: "rot2",
				role: "user",
				content: "pre-rotation tail from the other process",
				apiContent: "pre-rotation tail from the other process",
				timestamp: Date.now() / 1000,
			});

			h.store.leases.releaseHolder("rot", ghost);
			const result = (await turnPromise) as
				| Awaited<ReturnType<RunnerHarness["runner"]["handleTurn"]>>
				| { error: unknown };
			if ("error" in result) throw result.error;
			expect(result.exitReason).toBe("finalized");

			// The turn PERSISTED to the resolved tip, not the stale id:
			const tipRows = h.store.listMessages("rot2").map((r) => r.role);
			expect(tipRows).toEqual(["user", "user", "assistant"]);
			expect(h.store.listMessages("rot2")[1]!.content).toBe("post-wait ask");
			// Stale lineage segment got nothing.
			expect(h.store.listMessages("rot")).toHaveLength(0);
		} finally {
			await h.close();
		}
	});

	it("the refresh daemon extends the lease during long turns and release lands in finally", async () => {
		const gated = gatedTool();
		const h = await createRunnerHarness({
			withTurnLeases: true,
			leaseRefreshIntervalMs: 15,
			customTools: [gated.tool],
		});
		try {
			h.ensureSession("refresher");
			h.faux.setResponses([
				fauxAssistantMessage([fauxToolCall("gate_tool", {}, { id: "g1" })]),
				(context: Context) => {
					void context;
					return fauxAssistantMessage("done after gate");
				},
			]);
			const probe = (): number =>
				h.store.leases.probeOwner("refresher")!.expiresAt;
			const turnPromise = h.runner.handleTurn({
				sessionId: "refresher",
				routingKey: "rk",
				text: "long task",
			});
			await gated.entered;
			const beforeExtend = probe();
			// Park past several 15ms refresh ticks: each extends expires_at.
			await new Promise<void>((r) => setTimeout(r, 80));
			const afterExtend = probe();
			gated.release();
			const outcome = await turnPromise;
			expect(outcome.exitReason).toBe("finalized");
			expect(afterExtend).toBeGreaterThan(beforeExtend); // daemon refreshed
			// Released in finally.
			expect(h.store.leases.probeOwner("refresher")).toBeNull();
		} finally {
			await h.close();
		}
	});

	it("LOSING the lease mid-turn aborts the turn to protect the transcript", async () => {
		const gated = gatedTool();
		let syncCalls = 0;
		const h = await createRunnerHarness({
			withTurnLeases: true,
			leaseRefreshIntervalMs: 10,
			customTools: [gated.tool],
			memoryHooks: {
				prefetchAll: async () => "ctx",
				syncAll: () => {
					syncCalls += 1;
				},
				queuePrefetchAll: () => {},
			},
		});
		try {
			h.ensureSession("stolen");
			h.faux.setResponses([
				fauxAssistantMessage([fauxToolCall("gate_tool", {}, { id: "g1" })]),
				fauxAssistantMessage("never reached"),
			]);
			const turnPromise = h.runner.handleTurn({
				sessionId: "stolen",
				routingKey: "rk",
				text: "long task",
			});
			await gated.entered;

			// Hostile mutation: another process steals the lineage slot mid-turn
			// (our row deleted, thief's inserted under the same root).
			h.store.db
				.prepare(
					"DELETE FROM session_turn_leases WHERE conversation_id = 'stolen'",
				)
				.run();
			const stolen = structuredHolder("thief", process.pid);
			expect(h.store.leases.tryAcquire("stolen", stolen)).toBe(true);

			const outcome = await turnPromise;
			// Abort to protect the transcript — recorded as error, never silent.
			expect(outcome.exitReason).toBe("error");
			expect(outcome.errorMessage).toContain("lease lost");
			// Memory stays silent for a lease-lost abort (#15218 posture).
			expect(syncCalls).toBe(0);
			gated.release();
		} finally {
			await h.close();
		}
	});
});
