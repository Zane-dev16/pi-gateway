// pi_agent_core/runner.ts — the runner loop: resolved session + inbound
// message → REAL host pi AgentSession driven to its final text (DEC-023: the
// host agent loop is reused DIRECTLY via createAgentSession/prompt/steer/
// abort — never re-implemented, never shimmed).
//
// Loop-integration responsibilities ported around the host loop (05 §4):
//   - cached agent instances (byte-stable system prompt + toolset per session)
//     with LRU + DEC-021 memory-pressure shedding;
//   - replay seeding from pi_state rows (persist-what-you-send sidecars);
//   - alternation repair PRE-REQUEST on the live history (DEC-015) — the wire
//     copy is derived from live history at request time by the host loop, so
//     repairing state.messages pre-request repairs the wire; persisted bytes
//     are never rewritten;
//   - budget/grace iteration semantics + recorded exit reasons (05 §4.1),
//     enforced over the host loop's turn_end event stream — the narrowest real
//     SDK seam for "iterations" (each turn_end = one completed model call);
//   - interrupt at boundaries via session.abort() (/stop analogue);
//   - steer drain placement delegated to the host queue (delivered after the
//     current assistant turn finishes its tool calls, before the next model
//     call — docs/sdk.md steer());
//   - checkpoint dedup ledger reset per turn (05 §4 pseudocode);
//   - memory turn-boundary contract (05 §4.2): prefetch once before the tool
//     loop; interrupted turns skip BOTH sync and warming (#15218).
//
// Hermes anchors: run_agent.py:AIAgent.run_conversation →
// agent/conversation_loop.py::run_conversation; gateway/run.py::_agent_cache.

import type DatabaseType from "better-sqlite3";

import { AgentInstanceCache, type AgentCacheOptions } from "./agent-cache.js";
import { repairMessageSequence } from "./alternation-repair.js";
import {
	ConversationState,
	runWithConversation,
} from "./conversation-state.js";
import {
	createAgentSession,
	DefaultResourceLoader,
	SessionManager,
	SettingsManager,
	type Api,
	type AssistantMessage,
	type CreateAgentSessionOptions,
	type Message,
	type Model,
	type StopReason,
	type TextContent,
	type ToolCall,
	type ToolResultMessage,
	type Usage,
	type ToolDefinition,
	type AgentSession,
} from "./host.js";
import {
	readReplayMessages,
	substituteApiContent,
	type MessageRow,
	type NewMessage,
	type TokenDelta,
} from "../pi_state/index.js";
import type {
	TurnExitReason,
	TurnOutcome,
	TurnUsageSnapshot,
} from "./runner-types.js";
import { TurnWorkerPool } from "./worker-pool.js";

/** Memory turn-boundary hooks (05 §4.2). All optional; failures never break a turn. */
export interface MemoryTurnHooks {
	/** Prefetch ONCE per turn, before the first model call. */
	prefetchAll?(query: string): Promise<string> | string;
	/** End-of-turn sync of durable conversational truth. */
	syncAll?(input: {
		userText: string;
		responseText: string;
	}): void | Promise<void>;
	/** Warm NEXT-turn recall in the background after a normal finalize. */
	queuePrefetchAll?(query: string): void;
	/** Trivial-prompt gate (greetings/acks skip prefetch AND warming). */
	isTrivialPrompt?(query: string): boolean;
}

/**
 * Minimal store surface the runner consumes (structural subset of
 * pi_state's StateStore — composition stays downward-only).
 */
export interface RunnerStore {
	db: DatabaseType.Database;
	appendMessage(message: NewMessage): number | Promise<number>;
	queueTokenCounts(sessionId: string, delta: TokenDelta): unknown;
}

export interface GatewayAgentRunnerOptions {
	store: RunnerStore;
	systemPrompt: string;
	model: Model<Api>;
	modelRuntime: import("./host.js").ModelRuntime;
	customTools?: ToolDefinition[];
	/**
	 * Per-turn model-call budget (Hermes max_iterations). Default 32. When the
	 * host loop would exceed it, exactly ONE grace call is allowed, then the
	 * turn ends with exitReason "budget_exhausted".
	 */
	maxIterations?: number;
	memoryHooks?: MemoryTurnHooks;
	cacheOptions?: AgentCacheOptions;
	poolMaxWorkers?: number;
	/** Injected clock (ms) for cache recency + turn timestamps. */
	now?: () => number;
}

interface CachedHostSession {
	session: AgentSession;
}

interface InflightTurn {
	session: AgentSession;
	interruptRequested: boolean;
	budgetAborted: boolean;
}

const DEFAULT_MAX_ITERATIONS = 32;

export class GatewayAgentRunner {
	private readonly store: RunnerStore;
	private readonly systemPrompt: string;
	private readonly model: Model<Api>;
	private readonly modelRuntime: GatewayAgentRunnerOptions["modelRuntime"];
	private readonly customTools: ToolDefinition[] | undefined;
	private readonly maxIterations: number;
	private readonly memoryHooks: MemoryTurnHooks | undefined;
	private readonly now: () => number;
	private readonly cache: AgentInstanceCache<CachedHostSession>;
	private readonly pool: TurnWorkerPool;
	private readonly inflight = new Map<string, InflightTurn>();
	private readonly generations = new Map<string, number>();
	private closed = false;

	constructor(options: GatewayAgentRunnerOptions) {
		this.store = options.store;
		this.systemPrompt = options.systemPrompt;
		this.model = options.model;
		this.modelRuntime = options.modelRuntime;
		this.customTools = options.customTools;
		this.maxIterations = options.maxIterations ?? DEFAULT_MAX_ITERATIONS;
		this.memoryHooks = options.memoryHooks;
		this.now = options.now ?? (() => Date.now());
		this.cache = new AgentInstanceCache<CachedHostSession>(
			options.cacheOptions,
		);
		this.pool = new TurnWorkerPool({
			maxWorkers: options.poolMaxWorkers ?? 10,
		});
	}

	get poolStats(): { active: number; pending: number; max: number } {
		return {
			active: this.pool.active,
			pending: this.pool.pending,
			max: this.pool.maxConcurrent,
		};
	}

	/** Diagnostics: current cache entry count / byte estimate total. */
	get cacheStats(): { entries: number; bytes: number } {
		return { entries: this.cache.size, bytes: this.cache.totalBytes };
	}

	/** True while a turn occupies a worker slot for this session. */
	isTurnActive(sessionId: string): boolean {
		return this.inflight.has(sessionId) || this.pool.isRunning(sessionId);
	}

	/**
	 * Drive ONE turn end-to-end. Executes on the bounded worker pool under the
	 * session's ConversationState context (DEC-020).
	 */
	async handleTurn(request: {
		sessionId: string;
		routingKey: string;
		text: string;
	}): Promise<TurnOutcome> {
		if (this.closed) throw new Error("runner is closed");
		const generation = (this.generations.get(request.sessionId) ?? 0) + 1;
		this.generations.set(request.sessionId, generation);

		return this.pool.submit({
			key: request.sessionId,
			generation,
			run: async () => {
				const state = new ConversationState(request.sessionId, {
					routingKey: request.routingKey,
					generation,
				});
				return runWithConversation(state, () => this.runTurn(request, state));
			},
		});
	}

	/** Queue a steering message onto the in-flight turn (host drain placement). */
	async steer(sessionId: string, text: string): Promise<void> {
		const turn = this.inflight.get(sessionId);
		if (!turn) throw new Error(`no in-flight turn for ${sessionId}`);
		await turn.session.steer(text);
	}

	/**
	 * /stop analogue: mark the in-flight turn interrupted and abort the host
	 * loop; abort() resolves when the agent is idle again.
	 */
	async interrupt(sessionId: string): Promise<boolean> {
		const turn = this.inflight.get(sessionId);
		if (!turn) return false;
		turn.interruptRequested = true;
		await turn.session.abort();
		return true;
	}

	/** Drop the cached session for a session id (e.g. post-compression rekey). */
	dropCachedSession(sessionId: string): void {
		const entry = this.cache.peek(sessionId);
		entry?.session.dispose();
		this.cache.delete(sessionId);
	}

	async close(): Promise<void> {
		this.closed = true;
		for (const key of this.cache.keys()) this.dropCachedSession(key);
	}

	// ------------------------------------------------------------------

	private async runTurn(
		request: { sessionId: string; routingKey: string; text: string },
		state: ConversationState,
	): Promise<TurnOutcome> {
		state.turnStartedAt = this.now();
		state.checkpointLedger.newTurn();
		state.iterations = 0;
		state.exitReason = null;

		const host = await this.acquireHostSession(request.sessionId);
		const session = host.session;

		// ---- PRE-REQUEST alternation repair on LIVE history (DEC-015) --------
		// The host loop derives the wire copy from state.messages at request
		// time; repairing here repairs both copies while persisted rows stay
		// untouched.
		let repairs = 0;
		const live = session.agent.state.messages as unknown as Message[];
		repairs = repairMessageSequence(live);
		if (repairs > 0) {
			session.agent.state.messages =
				live as unknown as typeof session.agent.state.messages;
		}
		state.repairCount = repairs;

		// ---- Persist user row BEFORE prompting (crash-safe ordering) --------
		const userRowId = await this.store.appendMessage({
			sessionId: request.sessionId,
			role: "user",
			content: request.text,
			apiContent: request.text,
		});

		// ---- Memory: prefetch ONCE before the tool loop ----------------------
		const query = request.text;
		if (
			this.memoryHooks?.prefetchAll &&
			!(this.memoryHooks.isTrivialPrompt?.(query) ?? false)
		) {
			try {
				state.extPrefetchCache = await this.memoryHooks.prefetchAll(query);
			} catch {
				state.extPrefetchCache = null; // provider failure never blocks a turn
			}
		}

		// ---- Event-driven enforcement over the REAL loop ---------------------
		const inflight: InflightTurn = {
			session,
			interruptRequested: false,
			budgetAborted: false,
		};
		this.inflight.set(request.sessionId, inflight);

		let lastAssistantError: string | undefined;
		let lastStopReason: StopReason | undefined;
		// Mutable holder: TS flow analysis can't see callback assignments to a
		// `let`, so read observed state through properties, never a bare let.
		const observed: { final: AssistantMessage | null } = { final: null };
		/** Model calls STARTED (turn_start precedes every model call). */
		let startedCalls = 0;

		const unsubscribe = session.subscribe((event) => {
			if (event.type === "turn_start") {
				startedCalls += 1;
				state.checkpointLedger.record(
					`iteration:${state.iterations + 1}:start`,
				);
				// Budget gate BEFORE the model call (Hermes checks the budget at
				// the loop top, before call_model): allowance = maxIterations
				// normal calls + exactly ONE grace call. Anything beyond is cut
				// off before it streams.
				if (
					startedCalls > this.maxIterations + 1 &&
					!inflight.budgetAborted &&
					!inflight.interruptRequested
				) {
					inflight.budgetAborted = true;
					void session.abort().catch(() => {});
				}
				return;
			}
			if (event.type === "turn_end") {
				const finished = event.message as unknown as AssistantMessage;
				lastStopReason = finished.stopReason;
				lastAssistantError = finished.errorMessage;
				// An aborted/errored cycle is NOT a completed iteration (the
				// budget gate kills the not-allowed call mid-stream; it must not
				// count against the turn's iteration record).
				if (
					finished.stopReason !== "aborted" &&
					finished.stopReason !== "error"
				) {
					state.iterations += 1;
					observed.final = finished;
				}
				state.checkpointLedger.record(`iteration:${state.iterations}:end`);
				// Budget + grace (05 §4.1): after `maxIterations` calls the NEXT
				// call IS the single grace call; when it completes we stop.
				if (
					state.iterations >= this.maxIterations + 1 &&
					!inflight.budgetAborted &&
					!inflight.interruptRequested
				) {
					inflight.budgetAborted = true;
					void session.abort().catch(() => {});
				}
				return;
			}
			if (event.type === "message_end") {
				const msg = event.message as unknown as Message;
				if (msg.role === "assistant") {
					lastStopReason = msg.stopReason;
					lastAssistantError = msg.errorMessage;
				}
			}
		});

		try {
			await session.prompt(request.text);
		} finally {
			unsubscribe();
			this.inflight.delete(request.sessionId);
		}

		// ---- Exit reason (recorded, never silent) ----------------------------
		// Precedence mirrors Hermes: an EXTERNAL interrupt wins; a budget abort
		// counts as budget_exhausted UNLESS the grace call delivered the final
		// payload (stopReason "stop" ⇒ the loop would have finalized anyway);
		// provider errors surface as "error".
		const interrupted =
			inflight.interruptRequested || lastStopReason === "aborted";
		let exitReason: TurnExitReason;
		if (inflight.interruptRequested) exitReason = "interrupted_by_user";
		else if (
			inflight.budgetAborted &&
			lastStopReason !== "stop" &&
			lastStopReason !== "error"
		) {
			exitReason = "budget_exhausted";
		} else if (lastAssistantError || lastStopReason === "error") {
			exitReason = "error";
		} else if (interrupted && !inflight.budgetAborted) {
			exitReason = "interrupted_by_user"; // foreign abort
		} else exitReason = "finalized";
		state.exitReason = exitReason;

		// ---- Authoritative final payload --------------------------------------
		const assistantMsg: AssistantMessage | null = observed.final;
		const finalText = assistantText(assistantMsg);

		// ---- Memory sync leg (interrupted turns SKIP BOTH legs, #15218) ------
		if (exitReason !== "interrupted_by_user") {
			if (request.text.length > 0 && finalText.length > 0) {
				try {
					await this.memoryHooks?.syncAll?.({
						userText: request.text,
						responseText: finalText,
					});
				} catch {
					/* swallowed — misconfigured backend must not block delivery */
				}
			}
			if (
				request.text.length > 0 &&
				finalText.length > 0 &&
				!(this.memoryHooks?.isTrivialPrompt?.(query) ?? false)
			) {
				try {
					this.memoryHooks?.queuePrefetchAll?.(query);
				} catch {
					/* swallowed */
				}
			}
		}

		// ---- Persist assistant row + api_content sidecar (byte-exact) --------
		let assistantRowId: number | null = null;
		let usageSnapshot: TurnUsageSnapshot | null = null;
		if (assistantMsg) {
			const u: Usage | undefined = assistantMsg.usage;
			usageSnapshot = u
				? {
						inputTokens: u.input,
						outputTokens: u.output,
						cacheReadTokens: u.cacheRead,
						cacheWriteTokens: u.cacheWrite,
						totalTokens: u.totalTokens,
					}
				: null;
			const wireBytes = JSON.stringify({
				role: "assistant",
				content: assistantMsg.content,
			});
			assistantRowId = await this.store.appendMessage({
				sessionId: request.sessionId,
				role: "assistant",
				content: finalText,
				apiContent: wireBytes,
				finishReason: assistantMsg.stopReason,
				...(usageSnapshot ? { tokenCount: usageSnapshot.outputTokens } : {}),
			});
			if (usageSnapshot) {
				this.store.queueTokenCounts(request.sessionId, {
					inputTokens: usageSnapshot.inputTokens,
					outputTokens: usageSnapshot.outputTokens,
					cacheReadTokens: usageSnapshot.cacheReadTokens,
					cacheWriteTokens: usageSnapshot.cacheWriteTokens,
					apiCallCount: 1,
					model: `${assistantMsg.provider}/${assistantMsg.model}`,
				});
			}
		}

		const outcome: TurnOutcome = {
			exitReason,
			finalText,
			iterations: state.iterations,
			repairs,
			userRowId,
			assistantRowId,
			usage: usageSnapshot,
		};
		if (lastAssistantError !== undefined) {
			outcome.errorMessage = lastAssistantError;
		}
		return outcome;
	}

	// ------------------------------------------------------------------

	/** Cache get-or-build so consecutive turns reuse byte-identical prompt+tools. */
	private async acquireHostSession(
		sessionId: string,
	): Promise<CachedHostSession> {
		const hit = this.cache.get(sessionId);
		if (hit) return hit;
		const built = await this.buildHostSession(sessionId);
		this.cache.set(sessionId, built, estimateSessionBytes(built.session));
		return built;
	}

	private async buildHostSession(
		sessionId: string,
	): Promise<CachedHostSession> {
		const settingsManager = SettingsManager.inMemory({});
		const resourceLoader = new DefaultResourceLoader({
			cwd: process.cwd(),
			agentDir: process.cwd(),
			systemPromptOverride: () => this.systemPrompt,
			// Deterministic prompt bytes: the gateway owns prompt composition;
			// ambient project context files (AGENTS.md discovery) must never leak
			// into messaging-gateway turns nor flip between turns.
			agentsFilesOverride: () => ({ agentsFiles: [] }),
			settingsManager,
		});
		await resourceLoader.reload();

		const createOptions: CreateAgentSessionOptions = {
			cwd: process.cwd(),
			agentDir: process.cwd(),
			sessionManager: SessionManager.inMemory(process.cwd()),
			settingsManager,
			resourceLoader,
			modelRuntime: this.modelRuntime,
			model: this.model,
			thinkingLevel: "off",
			noTools: "builtin",
		};
		if (this.customTools) {
			createOptions.customTools = [...this.customTools];
		}
		const { session } = await createAgentSession(createOptions);

		await this.seedReplay(session, sessionId);
		return { session };
	}

	/**
	 * Seed the host loop with persisted history (02 §7.3 replay projection):
	 * sidecar substitution applies (api_content over content), tool rows map to
	 * toolResult messages, ancestors included, clone-defense dedupe on.
	 */
	private async seedReplay(
		session: AgentSession,
		sessionId: string,
	): Promise<void> {
		const rows: MessageRow[] = readReplayMessages(this.store.db, sessionId, {
			includeAncestors: true,
			dedupeReplayedUserRows: true,
		});
		if (rows.length === 0) return;
		const seeded: Message[] = [];
		for (const row of rows) {
			seeded.push(rowToLoopMessage(row, this.model));
		}
		session.agent.state.messages =
			seeded as unknown as typeof session.agent.state.messages;
	}
}

// ----------------------------------------------------------------------

/** Map one persisted row onto the host loop's message vocabulary. */
export function rowToLoopMessage(row: MessageRow, model: Model<Api>): Message {
	const tsMs = Math.round(row.timestamp * 1000);
	const content = substituteApiContent(row) ?? row.content ?? "";
	if (row.role === "user") {
		return { role: "user", content, timestamp: tsMs };
	}
	if (row.role === "assistant") {
		const calls = parseToolCalls(row.tool_calls);
		const contentBlocks: Array<TextContent | ToolCall> = [
			...(content ? [{ type: "text" as const, text: content }] : []),
			...calls,
		];
		return {
			role: "assistant",
			content: contentBlocks,
			api: model.api,
			provider: model.provider,
			model: model.id,
			usage: zeroUsage(),
			stopReason: (row.finish_reason as StopReason | null) ?? "stop",
			timestamp: tsMs,
		};
	}
	// role === "tool": a persisted tool result.
	const toolResult: ToolResultMessage = {
		role: "toolResult",
		toolCallId: row.tool_call_id ?? "",
		toolName: row.tool_name ?? "",
		content: [{ type: "text", text: content }],
		isError: row.effect_disposition === "error",
		timestamp: tsMs,
	};
	return toolResult;
}

function zeroUsage(): Usage {
	return {
		input: 0,
		output: 0,
		cacheRead: 0,
		cacheWrite: 0,
		totalTokens: 0,
		cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
	};
}

function parseToolCalls(raw: string | null): ToolCall[] {
	if (!raw) return [];
	try {
		const parsed = JSON.parse(raw) as Array<{
			id?: string;
			name?: string;
			arguments?: Record<string, unknown>;
		}>;
		if (!Array.isArray(parsed)) return [];
		return parsed.flatMap((c) =>
			c && c.id && c.name
				? [
						{
							type: "toolCall" as const,
							id: c.id,
							name: c.name,
							arguments: c.arguments ?? {},
						},
					]
				: [],
		);
	} catch {
		return [];
	}
}

function assistantText(msg: AssistantMessage | null): string {
	if (!msg) return "";
	const out: string[] = [];
	for (const b of msg.content) {
		if (b.type === "text") out.push(b.text);
	}
	return out.join("");
}

function estimateSessionBytes(session: AgentSession): number {
	let total = 4096; // scaffolding baseline
	for (const m of session.agent.state.messages) {
		total += JSON.stringify(m).length * 2; // UTF-16-ish estimate
	}
	return total;
}
