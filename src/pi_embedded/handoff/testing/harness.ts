// TEST INFRASTRUCTURE — the composed handoff E2E rig: isolated StateStore
// (mkdtemp) + REAL GatewayAgentRunner over the scripted faux provider + REAL
// L1 AdapterSessionGuard + fake destination transport, all wired through the
// production seams (HandoffQueue / RoutingBinder / HandoffPipeline /
// GuardQuiesceDispatcher / HandoffWatcher). Nothing here intercepts the guard
// pipeline or the host loop — the synthetic event traverses BOTH guards into
// the runner exactly like production traffic.
//
// Layering: pi_embedded (rank 4) importing pi_state (1) and pi_agent_core
// testing helpers (2) — downward only.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type DatabaseType from "better-sqlite3";

import {
	GatewayAgentRunner,
	type RunnerStore,
} from "../../../pi_agent_core/runner.js";
import {
	createScriptedModelEnv,
	fauxAssistantMessage,
	type FauxProviderHandle,
	type ScriptedModelEnv,
} from "../../../pi_agent_core/testing/faux-model.js";
import {
	AdapterSessionGuard,
	immediateSpawner,
	type CommandRegistry,
	type GatewayTask,
	type IncomingEvent,
	type TaskSpawner,
	type TurnContext,
} from "../../../pi_gateway/guards/index.js";
import { StateStore } from "../../../pi_state/index.js";
import { ManualClock } from "./manual-clock.js";
import {
	GuardQuiesceDispatcher,
	type DispatchSettlementProbe,
} from "../dispatcher.js";
import {
	HandoffCliClient,
	HandoffPipeline,
	HandoffQueue,
	HandoffWatcher,
	type HandoffHomeChannel,
	type HandoffLogger,
	type HandoffTransport,
} from "../index.js";
import { RoutingBinder } from "../binder.js";

export { fauxAssistantMessage };

/** Recorded turn observation (what the NORMAL pipeline was asked to run). */
export interface TurnRecord {
	sessionKey: string;
	resolvedSessionId: string;
	text: string;
}

/** Fake destination platform: thread creation + recorded sends. */
export class FakeHandoffTransport implements HandoffTransport {
	readonly threadsCreated: Array<{
		chatId: string;
		name: string;
		threadId: string;
	}> = [];
	readonly sends: Array<{ chatId: string; text: string }> = [];
	private counter = 0;

	constructor(
		readonly opts: {
			platform?: string;
			createThreads?: boolean;
			throwOnCreate?: boolean;
		} = {},
	) {}

	get platform(): string {
		return this.opts.platform ?? "telegram";
	}

	async createHandoffThread(
		chatId: string,
		name: string,
	): Promise<string | null> {
		if (this.opts.throwOnCreate === true) {
			throw new Error("thread creation exploded");
		}
		if (this.opts.createThreads === false) return null;
		const threadId = `topic-${++this.counter}`;
		this.threadsCreated.push({ chatId, name, threadId });
		return threadId;
	}
}

interface TrackedState {
	active: number;
	/** Live task objects (drain hand-off may legitimately overlap two). */
	maxConcurrency: number;
	/** §11 handler-section overlap probe — never exceeds 1 for one key. */
	maxHandlerConcurrency: number;
	waiters: Array<() => void>;
	failures: Array<{ sessionKey: string; error: string }>;
}

function makeTrackedSpawner(state: TrackedState): TaskSpawner {
	const immediate = immediateSpawner();
	return ((run: (task: GatewayTask) => Promise<void>) => {
		state.active++; // BEFORE construction: ImmediateTask starts running now
		state.maxConcurrency = Math.max(state.maxConcurrency, state.active);
		const task = immediate(async (self) => {
			try {
				await run(self);
			} finally {
				state.active--;
				if (state.active === 0) {
					const waiters = state.waiters;
					state.waiters = [];
					for (const wake of waiters) wake();
				}
			}
		});
		return task;
	}) as TaskSpawner;
}

const TEST_REGISTRY: CommandRegistry = [
	{ name: "new", aliases: ["reset"], busyPolicy: "interrupt_then_dispatch" },
	{ name: "stop", busyPolicy: "interrupt_then_dispatch" },
];

export interface HandoffHarnessOptions {
	/** Home channel served by the fake destination platform. */
	home?: Partial<HandoffHomeChannel>;
	transport?: Partial<FakeHandoffTransport["opts"]>;
	platform?: string;
	isolationFlags?: {
		groupSessionsPerUser?: boolean;
		threadSessionsPerUser?: boolean;
	};
	systemPrompt?: string;
	log?: HandoffLogger;
}

export interface HandoffHarness {
	dir: string;
	clock: ManualClock;
	store: StateStore;
	db: DatabaseType.Database;
	env: ScriptedModelEnv;
	faux: FauxProviderHandle;
	runner: GatewayAgentRunner;
	queue: HandoffQueue;
	binder: RoutingBinder;
	pipeline: HandoffPipeline;
	watcher: HandoffWatcher;
	cliClient: HandoffCliClient;
	guard: AdapterSessionGuard;
	transport: FakeHandoffTransport;
	home: HandoffHomeChannel;
	/** Turns the pipeline actually ran (resolved session id proves re-bind). */
	turns: TurnRecord[];
	replies: string[];
	tracker: TrackedState & { awaitIdle(): Promise<void> };
	holdTurns(on: boolean): void;
	seedCliSession(sessionId: string, turns: Array<[string, string]>): void;
	close(): Promise<void>;
}

export async function createHandoffHarness(
	options: HandoffHarnessOptions = {},
): Promise<HandoffHarness> {
	const dir = mkdtempSync(join(tmpdir(), "pi-gw-handoff-"));
	const store = await StateStore.open(join(dir, "state.db"));
	const clock = new ManualClock();
	const env = await createScriptedModelEnv({
		...(options.systemPrompt !== undefined
			? { systemPrompt: options.systemPrompt }
			: {}),
	});
	const model = env.faux.getModel();
	if (!model) throw new Error("faux provider exposed no model");

	const storeAdapter: RunnerStore = {
		db: store.db,
		appendMessage: (m) => store.appendMessage(m),
		queueTokenCounts: (sessionId, delta) =>
			store.queueTokenCounts(sessionId, delta),
	};
	const runner = new GatewayAgentRunner({
		store: storeAdapter,
		systemPrompt: env.systemPrompt,
		model,
		modelRuntime: env.modelRuntime,
	});

	// Default scripted reply: every handoff turn succeeds unless a test
	// overrides the script (setResponses/appendResponses) or breaks a seam.
	env.faux.setResponses([fauxAssistantMessage("confirming handoff")]);

	const queue = new HandoffQueue(store.db, { clock });
	const binder = new RoutingBinder(store.db, { clock });

	const platform = options.platform ?? "telegram";
	const home: HandoffHomeChannel = {
		platform,
		chatId: options.home?.chatId ?? "100",
		name: options.home?.name ?? "Home Chat",
		...(options.home?.threadId !== undefined
			? { threadId: options.home.threadId }
			: {}),
	};
	const transport = new FakeHandoffTransport({
		platform,
		...(options.transport ?? {}),
	});

	const trackerState: TrackedState & { awaitIdle(): Promise<void> } = {
		active: 0,
		maxConcurrency: 0,
		maxHandlerConcurrency: 0,
		waiters: [],
		failures: [],
		awaitIdle(): Promise<void> {
			if (this.active === 0) return Promise.resolve();
			return new Promise<void>((resolve) => {
				this.waiters.push(resolve);
			});
		},
	};

	let heldNow = false;
	let releaseHeld: () => void = () => {};
	const heldGate = (): Promise<void> =>
		new Promise<void>((resolve) => {
			releaseHeld = resolve;
		});
	let gate = Promise.resolve();

	const turns: TurnRecord[] = [];
	const replies: string[] = [];
	let handlerInFlight = 0;

	async function runHandlerBody(
		event: IncomingEvent,
		ctx: TurnContext,
		sessionKey: string,
	): Promise<string | null | undefined> {
		// THE RE-BIND PROOF: the handler resolves the turn's session from
		// the ROUTING ENTRY (never from the event), exactly like the real
		// resolution chain will. A handoff that failed to re-bind would
		// run against a stale/missing session here.
		const entry = binder.entryOf(sessionKey);
		if (!entry) {
			throw new Error(`no routing entry for key ${sessionKey}`);
		}
		const record: TurnRecord = {
			sessionKey,
			resolvedSessionId: entry.session_id,
			text: event.text ?? "",
		};
		turns.push(record);
		while (heldNow && !ctx.task.cancelRequested()) {
			await gate;
		}
		ctx.throwIfCancelled();
		const outcome = await runner.handleTurn({
			sessionId: entry.session_id,
			routingKey: sessionKey,
			text: event.text ?? "",
		});
		if (outcome.exitReason === "error") {
			throw new Error(outcome.errorMessage ?? "turn error");
		}
		return outcome.finalText;
	}

	const guard = new AdapterSessionGuard({
		messageHandler: async (event, ctx) => {
			const sessionKey = String(
				(event.metadata ?? {})["gateway_session_key"] ?? "",
			);
			// §11-style overlap probe: HANDLER sections only (drain hand-off
			// legitimately overlaps two live task objects, never two handlers).
			handlerInFlight++;
			trackerState.maxHandlerConcurrency = Math.max(
				trackerState.maxHandlerConcurrency,
				handlerInFlight,
			);
			try {
				return await runHandlerBody(event, ctx, sessionKey);
			} catch (err) {
				trackerState.failures.push({
					sessionKey,
					error: err instanceof Error ? err.message : String(err),
				});
				throw err;
			} finally {
				handlerInFlight--;
			}
		},
		sendReply: async (_chatId, text) => {
			replies.push(text);
			transport.sends.push({ chatId: home.chatId, text });
		},
		registry: TEST_REGISTRY,
		spawner: makeTrackedSpawner(trackerState),
	});

	const probe: DispatchSettlementProbe = {
		awaitIdle: () => trackerState.awaitIdle(),
		drainFailures: () => {
			const drained = [...trackerState.failures];
			trackerState.failures = [];
			return drained;
		},
	};

	const pipeline = new HandoffPipeline({
		resolveTransport: (p) => (p === transport.platform ? transport : null),
		resolveHomeChannel: (p) => (p === home.platform ? home : null),
		...(options.isolationFlags !== undefined
			? { isolationFlags: () => options.isolationFlags }
			: {}),
		binder,
		dispatcher: new GuardQuiesceDispatcher(guard, probe),
		clock,
		...(options.log !== undefined ? { log: options.log } : {}),
	});

	const watcher = new HandoffWatcher({
		queue,
		processRow: (row) => pipeline.process(row),
		clock,
		...(options.log !== undefined ? { log: options.log } : {}),
	});

	const cliClient = new HandoffCliClient(queue, { clock });

	function seedCliSession(
		sessionId: string,
		cliTurns: Array<[string, string]>,
	): void {
		void store.withWrite((db) => {
			db.prepare(
				"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'cli', ?)",
			).run(sessionId, clock.nowSeconds());
		});
		let i = 0;
		for (const [userText, assistantText] of cliTurns) {
			i++;
			void store.withWrite((db) => {
				db.prepare(
					"INSERT INTO messages (session_id, role, content, active, timestamp) VALUES (?, 'user', ?, 1, ?)",
				).run(sessionId, userText, clock.nowSeconds() + i);
			});
			i++;
			void store.withWrite((db) => {
				db.prepare(
					"INSERT INTO messages (session_id, role, content, active, timestamp) VALUES (?, 'assistant', ?, 1, ?)",
				).run(sessionId, assistantText, clock.nowSeconds() + i);
			});
		}
	}

	return {
		dir,
		clock,
		store,
		db: store.db,
		env,
		faux: env.faux,
		runner,
		queue,
		binder,
		pipeline,
		watcher,
		cliClient,
		guard,
		transport,
		home,
		turns,
		replies,
		tracker: trackerState,
		holdTurns(on: boolean): void {
			heldNow = on;
			if (!on) {
				releaseHeld();
				gate = Promise.resolve();
			} else {
				gate = heldGate();
			}
		},
		seedCliSession,
		async close() {
			await watcher.stop().catch(() => undefined);
			await runner.close();
			await store.close();
		},
	};
}
