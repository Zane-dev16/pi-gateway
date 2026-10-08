// pi_agent_core/chat-proc-registry.ts — per-chat pi process table (DEC-084).
//
// Data shape first: ONE Map keyed by chat key. Each entry holds the chat
// identity (chatKey plus the child-owned host sessionId), the process handle
// (ProcClient, which owns the child), the spawn opts for replacements, the
// generation, the lifecycle state, and supervision counters. A state machine,
// not scattered booleans, so an illegal state (two live children for one
// chat) is unrepresentable: spawn converges to exactly one child per key.
//
// Scope: spawn, supervise, and stop only. No turn wiring: nothing here sends
// a prompt, and `busy` is set by the future turn owner (Todo 3), never by
// this module. Liveness probes use the public getState round-trip only; the
// registry never reaches into client privates.

import { PI_CLI_PATH, RpcClient } from "./host.js";
import type { RpcClientOptions } from "./host.js";

/** Lifecycle of one chat child. `busy` is owned by the turn layer (Todo 3):
 * this module never sets it, only preserves it across healthy probes. */
export type ChatProcState =
	| "starting"
	| "ready"
	| "busy"
	| "dead"
	| "backoff";

/**
 * Structural child surface. The real RpcClient satisfies this; tests inject
 * fakes. getState is the liveness probe: a throw means the child is gone.
 */
export interface ProcClient {
	start(): Promise<void>;
	stop(): Promise<void>;
	getState(): Promise<{ sessionId: string; isStreaming: boolean }>;
}

/** One supervised child: identity, handle, generation, state, counters. */
export interface ChatProcEntry {
	/** Opaque chat key (the resolved session/routing key bytes). */
	readonly chatKey: string;
	/** Process handle. RpcClient owns the ChildProcess underneath. */
	readonly client: ProcClient;
	/** Opts the replacement spawn reuses. Updated on every spawn call. */
	readonly spawnOpts: ChatProcSpawnOpts;
	/** Spawn generation for this key. Replacements increment, never reuse. */
	readonly generation: number;
	state: ChatProcState;
	/** Host session id read from the child after start. Null until ready. */
	sessionId: string | null;
	/** Replacement spawns so far (the initial spawn is not a restart). */
	restarts: number;
	/** Consecutive failed liveness probes since the last healthy one. */
	consecutiveFailures: number;
	/** Last failure text (start or probe). Null while healthy. */
	lastError: string | null;
	/** When the entry last transitioned to dead. Null while never dead. */
	lastExitAtMs: number | null;
}

/** Where a child lives plus which model surface it boots with. */
export interface ChatProcSpawnOpts {
	/** PI_HOME for the child. Production shares the gateway home across
	 * chats; per-chat working dirs ride cwd, not separate homes. */
	homeDir: string;
	/** Agent working directory. Defaults to homeDir. */
	cwd?: string | undefined;
	provider?: string | undefined;
	model?: string | undefined;
}

export type ProcClientFactory = (opts: ChatProcSpawnOpts) => ProcClient;

export interface ChatProcRegistryOpts {
	makeClient?: ProcClientFactory | undefined;
	now?: (() => number) | undefined;
	/** Dead entries stop retrying after this many replacements. */
	maxRestarts?: number | undefined;
}

export type SuperviseAction =
	| "alive"
	| "restarted"
	| "backoff"
	| "dead-capped"
	| "missing";

export interface SuperviseReport {
	readonly chatKey: string;
	readonly action: SuperviseAction;
	readonly state: ChatProcState;
	readonly generation: number;
	readonly restarts: number;
}

const DEFAULT_MAX_RESTARTS = 5;
const MAX_BACKOFF_MS = 30_000;

function backoffMs(restarts: number): number {
	return Math.min(MAX_BACKOFF_MS, 1000 * 2 ** restarts);
}

/** Production factory: one RpcClient per chat over the sanctioned RPC mode. */
export function makeRpcProcClient(opts: ChatProcSpawnOpts): ProcClient {
	const clientOpts: RpcClientOptions = {
		cliPath: PI_CLI_PATH,
		cwd: opts.cwd ?? opts.homeDir,
		env: { PI_HOME: opts.homeDir },
	};
	if (opts.provider !== undefined) clientOpts.provider = opts.provider;
	if (opts.model !== undefined) clientOpts.model = opts.model;
	return new RpcClient(clientOpts);
}

function errorText(error: unknown): string {
	return error instanceof Error ? error.message : String(error);
}

export class ChatProcRegistry {
	private readonly procs = new Map<string, ChatProcEntry>();
	private readonly makeClient: ProcClientFactory;
	private readonly now: () => number;
	private readonly maxRestarts: number;

	constructor(opts: ChatProcRegistryOpts = {}) {
		this.makeClient = opts.makeClient ?? makeRpcProcClient;
		this.now = opts.now ?? Date.now;
		this.maxRestarts = opts.maxRestarts ?? DEFAULT_MAX_RESTARTS;
	}

	get size(): number {
		return this.procs.size;
	}

	get(chatKey: string): ChatProcEntry | undefined {
		return this.procs.get(chatKey);
	}

	chatKeys(): string[] {
		return [...this.procs.keys()];
	}

	/**
	 * Converge to exactly one child for the key. A live entry (starting,
	 * ready, busy) is returned as is: spawn never doubles a child. A
	 * dead or backoff entry is replaced at the next generation. Start
	 * failures keep the dead entry and rethrow, so supervise can retry.
	 */
	async spawn(
		chatKey: string,
		opts: ChatProcSpawnOpts,
	): Promise<ChatProcEntry> {
		const existing = this.procs.get(chatKey);
		if (
			existing !== undefined &&
			(existing.state === "starting" ||
				existing.state === "ready" ||
				existing.state === "busy")
		) {
			return existing;
		}
		const entry: ChatProcEntry = {
			chatKey,
			client: this.makeClient(opts),
			spawnOpts: opts,
			generation: (existing?.generation ?? -1) + 1,
			state: "starting",
			sessionId: null,
			restarts: existing?.restarts ?? 0,
			consecutiveFailures: 0,
			lastError: null,
			lastExitAtMs: existing?.lastExitAtMs ?? null,
		};
		this.procs.set(chatKey, entry);
		await this.startEntry(entry);
		return entry;
	}

	/**
	 * One ensure-alive pass over a key. Healthy probes clear the failure
	 * counters and leave the state alone, except dead and backoff entries
	 * whose child answers again: those return to ready. A failed probe
	 * marks dead, then restarts past backoff, waits in backoff, or caps
	 * at maxRestarts. A failed replacement stays dead and rethrows nothing:
	 * the report carries the state and the next pass retries past backoff.
	 */
	async supervise(chatKey: string): Promise<SuperviseReport> {
		const entry = this.procs.get(chatKey);
		if (entry === undefined) {
			return {
				chatKey,
				action: "missing",
				state: "dead",
				generation: 0,
				restarts: 0,
			};
		}
		if (entry.state === "starting") {
			return this.report(entry, "alive");
		}
		let probeFailed: string | null = null;
		try {
			await entry.client.getState();
		} catch (error) {
			probeFailed = errorText(error);
		}
		if (probeFailed === null) {
			entry.consecutiveFailures = 0;
			entry.lastError = null;
			if (entry.state === "dead" || entry.state === "backoff") {
				entry.state = "ready";
			}
			return this.report(entry, "alive");
		}
		entry.consecutiveFailures += 1;
		entry.lastError = probeFailed;
		const previousExitAtMs = entry.lastExitAtMs;
		entry.lastExitAtMs = this.now();
		if (entry.restarts >= this.maxRestarts) {
			entry.state = "dead";
			return this.report(entry, "dead-capped");
		}
		const sincePreviousExit = this.now() - (previousExitAtMs ?? 0);
		if (
			previousExitAtMs !== null &&
			sincePreviousExit < backoffMs(entry.restarts)
		) {
			entry.state = "backoff";
			return this.report(entry, "backoff");
		}
		const replacement: ChatProcEntry = {
			chatKey: entry.chatKey,
			client: this.makeClient(entry.spawnOpts),
			spawnOpts: entry.spawnOpts,
			generation: entry.generation + 1,
			state: "starting",
			sessionId: null,
			restarts: entry.restarts + 1,
			consecutiveFailures: 0,
			lastError: null,
			lastExitAtMs: entry.lastExitAtMs,
		};
		this.procs.set(chatKey, replacement);
		try {
			await this.startEntry(replacement);
		} catch {
			/* dead entry plus report carry the failure; next pass retries */
		}
		return this.report(replacement, "restarted");
	}

	/** Ensure-alive pass over every key, in insertion order. */
	async superviseAll(): Promise<SuperviseReport[]> {
		const reports: SuperviseReport[] = [];
		for (const chatKey of this.procs.keys()) {
			reports.push(await this.supervise(chatKey));
		}
		return reports;
	}

	/**
	 * Converge to zero children for the key. The entry leaves the map even
	 * when the child kill fails. True when an entry existed.
	 */
	async stop(chatKey: string): Promise<boolean> {
		const entry = this.procs.get(chatKey);
		if (entry === undefined) return false;
		try {
			await entry.client.stop();
		} finally {
			this.procs.delete(chatKey);
		}
		return true;
	}

	/** Converge to zero children for every key. */
	async stopAll(): Promise<void> {
		for (const chatKey of [...this.procs.keys()]) {
			await this.stop(chatKey);
		}
	}

	/**
	 * Start one entry in place. Success reads the host session id and marks
	 * ready. Failure marks dead with the error and rethrows, so spawn
	 * surfaces the cause while supervise retries past backoff.
	 */
	private async startEntry(entry: ChatProcEntry): Promise<void> {
		try {
			await entry.client.start();
			const state = await entry.client.getState();
			entry.sessionId = state.sessionId;
			entry.state = "ready";
		} catch (error) {
			entry.state = "dead";
			entry.lastError = errorText(error);
			entry.lastExitAtMs = this.now();
			throw error;
		}
	}

	private report(
		entry: ChatProcEntry,
		action: SuperviseAction,
	): SuperviseReport {
		return {
			chatKey: entry.chatKey,
			action,
			state: entry.state,
			generation: entry.generation,
			restarts: entry.restarts,
		};
	}
}
