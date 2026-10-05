// pi_agent_core/rpc-turn-runner.ts — per-chat turns over RPC children (DEC-084).
//
// Data shape first: a turn is one request in (sessionId plus routingKey plus
// text) and one TurnOutcome out. The registry entry is the single-flight
// record: anything but `ready` means a turn (or a start) already owns this
// chat, so a second turn fails fast instead of stacking model calls. No
// lease table, no queue, no new flags.
//
// Delivery: the final text returns through TurnOutcome, which the production
// messageHandler hands to the existing guard delivery path unchanged.
// RPC events are subscribed per turn, counted for iterations, and forwarded
// to the optional sink. Interim streaming edits to chat surfaces are future
// work: the obligation path stays final-text, same as the dissolved runner.

import type {
	ChatProcRegistry,
	ChatProcSpawnOpts,
	ProcClient,
} from "./chat-proc-registry.js";
import type { TurnOutcome } from "./runner-types.js";
import type { NewMessage } from "../pi_state/index.js";

/** Minimal gateway message store (StateStore satisfies this structurally). */
export interface TurnMessageStore {
	appendMessage(m: NewMessage): Promise<number>;
}

/** One RPC session event, JSON shape as emitted by the child. */
export interface TurnRpcEvent {
	readonly type: string;
	readonly [key: string]: unknown;
}

/**
 * Turn-capable child surface. The real RpcClient satisfies this; tests
 * inject fakes. Prompt dispositions: `started` and `queued` mean a run is
 * (or will be) live, so the turn waits for idle; `handled` means the host
 * took it without a run, so the turn reads the last assistant text at once.
 */
export interface TurnClient extends ProcClient {
	prompt(message: string): Promise<string>;
	waitForIdle(timeoutMs?: number): Promise<void>;
	getLastAssistantText(): Promise<string>;
	onEvent(
		listener: (event: TurnRpcEvent) => void,
	): () => void;
}

/** Parse the registry's ProcClient at this module's boundary. Null when the
 * wired client cannot drive a turn (a composition bug, reported as an error
 * outcome, never a TypeError). */
function asTurnClient(client: ProcClient): TurnClient | null {
	const candidate = client as Partial<TurnClient>;
	if (
		typeof candidate.prompt !== "function" ||
		typeof candidate.waitForIdle !== "function" ||
		typeof candidate.getLastAssistantText !== "function" ||
		typeof candidate.onEvent !== "function"
	) {
		return null;
	}
	return candidate as TurnClient;
}

export interface TurnRequest {
	sessionId: string;
	routingKey: string;
	text: string;
}

export interface RpcTurnRunnerOpts {
	registry: ChatProcRegistry;
	/** Child PI_HOME for a chat key. Production shares the gateway home. */
	resolveHome: (routingKey: string) => string;
	/**
	 * Gateway message store. Absent means turns run with null row ids.
	 * Present means the session row already exists: the production
	 * handler ensures it before the turn, and the schema enforces it.
	 */
	store?: TurnMessageStore | null | undefined;
	provider?: string | undefined;
	model?: string | undefined;
	/** Idle-wait budget per turn. Absent keeps the RpcClient default. */
	turnTimeoutMs?: number | undefined;
	/** Per-event sink for future streaming delivery. Absent means count only. */
	onTurnEvent?: ((event: TurnRpcEvent) => void) | undefined;
}

export class RpcTurnRunner {
	private readonly registry: ChatProcRegistry;
	private readonly resolveHome: (routingKey: string) => string;
	private readonly store: TurnMessageStore | null;
	private readonly provider: string | undefined;
	private readonly model: string | undefined;
	private readonly turnTimeoutMs: number | undefined;
	private readonly onTurnEvent:
		| ((event: TurnRpcEvent) => void)
		| undefined;

	constructor(opts: RpcTurnRunnerOpts) {
		this.registry = opts.registry;
		this.resolveHome = opts.resolveHome;
		this.store = opts.store ?? null;
		this.provider = opts.provider;
		this.model = opts.model;
		this.turnTimeoutMs = opts.turnTimeoutMs;
		this.onTurnEvent = opts.onTurnEvent;
	}

	/**
	 * Drive one turn on the chat's child and return its outcome. Never
	 * throws for turn failures: they come back as error outcomes so the
	 * guard renders its radio-silence notice. Busy chats fail fast.
	 */
	async handleTurn(request: TurnRequest): Promise<TurnOutcome> {
		const spawnOpts: ChatProcSpawnOpts = {
			homeDir: this.resolveHome(request.routingKey),
		};
		if (this.provider !== undefined) spawnOpts.provider = this.provider;
		if (this.model !== undefined) spawnOpts.model = this.model;

		let entry;
		try {
			entry = await this.registry.spawn(request.routingKey, spawnOpts);
		} catch (error) {
			return RpcTurnRunner.errorOutcome(
				`spawn failed: ${RpcTurnRunner.errorText(error)}`,
				null,
				null,
			);
		}
		if (entry.state !== "ready") {
			return RpcTurnRunner.errorOutcome(
				"turn already in progress for this chat",
				null,
				null,
			);
		}
		const client = asTurnClient(entry.client);
		if (client === null) {
			return RpcTurnRunner.errorOutcome(
				"chat child cannot drive a turn",
				null,
				null,
			);
		}
		entry.state = "busy";

		let userRowId: number | null = null;
		if (this.store !== null) {
			try {
				userRowId = await this.store.appendMessage({
					sessionId: request.sessionId,
					role: "user",
					content: request.text,
				});
			} catch (error) {
				entry.state = "ready";
				return RpcTurnRunner.errorOutcome(
					`user row persist failed: ${RpcTurnRunner.errorText(error)}`,
					null,
					null,
				);
			}
		}

		let turnStarts = 0;
		const unsubscribe = client.onEvent((event) => {
			if (event.type === "turn_start") turnStarts += 1;
			this.onTurnEvent?.(event);
		});
		try {
			let disposition: string;
			try {
				disposition = await client.prompt(request.text);
			} catch (error) {
				return await this.fail(
					request.routingKey,
					`prompt failed: ${RpcTurnRunner.errorText(error)}`,
					userRowId,
					null,
				);
			}
			if (disposition !== "handled") {
				try {
					await client.waitForIdle(this.turnTimeoutMs);
				} catch (error) {
					return await this.fail(
						request.routingKey,
						`turn timed out: ${RpcTurnRunner.errorText(error)}`,
						userRowId,
						null,
					);
				}
			}
			let finalText: string;
			try {
				finalText = await client.getLastAssistantText();
			} catch (error) {
				return await this.fail(
					request.routingKey,
					`final text read failed: ${RpcTurnRunner.errorText(error)}`,
					userRowId,
					null,
				);
			}
			let assistantRowId: number | null = null;
			if (this.store !== null) {
				try {
					assistantRowId = await this.store.appendMessage({
						sessionId: request.sessionId,
						role: "assistant",
						content: finalText,
					});
				} catch (error) {
					return await this.fail(
						request.routingKey,
						`assistant row persist failed: ${RpcTurnRunner.errorText(error)}`,
						userRowId,
						null,
					);
				}
			}
			entry.state = "ready";
			return {
				exitReason: "finalized",
				finalText,
				iterations: Math.max(1, turnStarts),
				repairs: 0,
				userRowId,
				assistantRowId,
				usage: null,
			};
		} finally {
			unsubscribe();
		}
	}

	/**
	 * Settle a failed turn. A live child goes back to ready: the turn
	 * failed, not the process. A dead child is marked dead so the next
	 * spawn replaces it at the next generation (self-heal without a
	 * supervisor loop). The liveness probe is the public getState
	 * round-trip only.
	 */
	private async fail(
		chatKey: string,
		message: string,
		userRowId: number | null,
		assistantRowId: number | null,
	): Promise<TurnOutcome> {
		const entry = this.registry.get(chatKey);
		if (entry !== undefined && entry.state === "busy") {
			try {
				await entry.client.getState();
				entry.state = "ready";
			} catch {
				entry.state = "dead";
				entry.lastError = message;
			}
		}
		return RpcTurnRunner.errorOutcome(message, userRowId, assistantRowId);
	}

	private static errorOutcome(
		message: string,
		userRowId: number | null,
		assistantRowId: number | null,
	): TurnOutcome {
		return {
			exitReason: "error",
			finalText: "",
			iterations: 0,
			repairs: 0,
			userRowId,
			assistantRowId,
			errorMessage: message,
			usage: null,
		};
	}

	private static errorText(error: unknown): string {
		return error instanceof Error ? error.message : String(error);
	}
}
