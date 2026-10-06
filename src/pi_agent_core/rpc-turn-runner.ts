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

/**
 * Session-capable child surface. The real RpcClient satisfies this;
 * tests inject fakes. Checked per session command at the boundary:
 * a missing method is a composition bug, reported as an error outcome.
 */
/** One host command row as returned by get_commands (no model call). */
export interface HostCommandRow {
	readonly name: string;
	readonly description: string;
}

export interface SessionRpcClient extends TurnClient {
	newSession(parentSession?: string): Promise<unknown>;
	switchSession(sessionPath: string): Promise<unknown>;
	setModel(
		provider: string,
		modelId: string,
	): Promise<{ provider: string; id: string }>;
	getAvailableModels(): Promise<Array<{ provider: string; id: string }>>;
	compact(
		customInstructions?: string,
	): Promise<{ summary: string; tokensBefore: number }>;
	getEntries(
		since?: string,
	): Promise<{ entries: Array<unknown>; leafId: unknown }>;
	getCommands(): Promise<HostCommandRow[]>;
}

/** Parse the turn client for session capability. Null when any native
 * session call is missing (a composition bug, reported as an error
 * outcome, never a TypeError). */
function asSessionClient(client: TurnClient): SessionRpcClient | null {
	const candidate = client as Partial<SessionRpcClient>;
	if (
		typeof candidate.newSession !== "function" ||
		typeof candidate.switchSession !== "function" ||
		typeof candidate.setModel !== "function" ||
		typeof candidate.getAvailableModels !== "function" ||
		typeof candidate.compact !== "function" ||
		typeof candidate.getEntries !== "function" ||
		typeof candidate.getCommands !== "function"
	) {
		return null;
	}
	return candidate as SessionRpcClient;
}

/** Slash parse: leading "/" word plus the args tail. Null when the text
 * is plain chat (no leading slash) or a bare "/". Name is lowercased;
 * args keep original bytes minus surrounding whitespace. */
export function parseSlashCommand(
	text: string,
): { name: string; args: string } | null {
	const trimmed = text.trimStart();
	if (!trimmed.startsWith("/")) return null;
	const firstSpace = trimmed.search(/\s/);
	const word =
		firstSpace === -1 ? trimmed : trimmed.slice(0, firstSpace);
	const name = word.slice(1).toLowerCase();
	if (name === "") return null;
	const args = firstSpace === -1 ? "" : trimmed.slice(firstSpace + 1).trim();
	return { name, args };
}

/** Native session commands: exactly these ride RPC, never prompt text.
 * Every other slash word falls through to the host prompt path, where the
 * child loop owns it natively. One deterministic reply path per name.
 * Help rides get_commands so it answers with zero model calls. */
const NATIVE_SESSION_COMMANDS = new Set([
	"new",
	"resume",
	"model",
	"compact",
	"export",
	"help",
]);

/** Escape once for HTML text content (order matters: & first). */
function escapeHtml(s: string): string {
	return s
		.replace(/&/g, "&amp;")
		.replace(/</g, "&lt;")
		.replace(/>/g, "&gt;")
		.replace(/"/g, "&quot;");
}

/** Minimal self-contained HTML transcript from JSONL branch bytes. */
export function renderSessionExportHtml(jsonl: string): string {
	const cards = jsonl
		.split("\n")
		.filter((line) => line.trim() !== "")
		.map((line) => {
			let entry: Record<string, unknown>;
			try {
				entry = JSON.parse(line) as Record<string, unknown>;
			} catch {
				return `<article class="entry"><pre>${escapeHtml(line)}</pre></article>`;
			}
			const role =
				typeof entry["role"] === "string"
					? (entry["role"] as string)
					: typeof entry["type"] === "string"
						? (entry["type"] as string)
						: "entry";
			const content = entry["content"];
			const body =
				typeof content === "string" ? content : JSON.stringify(entry);
			return `<article class="entry"><h2>${escapeHtml(role)}</h2><pre>${escapeHtml(body)}</pre></article>`;
		})
		.join("\n");
	return `<!DOCTYPE html>\n<html><head><meta charset="utf-8"><title>Session export</title></head><body>\n${cards}\n</body></html>\n`;
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
	 *
	 * Session commands (/new, /resume, /model, /compact, /export) ride
	 * native RPC calls, never prompt text: one deterministic reply path
	 * per name. Every other slash word falls through to the host prompt
	 * path, where the child loop owns it natively.
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

		const slash = parseSlashCommand(request.text);
		if (slash !== null && NATIVE_SESSION_COMMANDS.has(slash.name)) {
			return await this.runSessionCommand(
				client,
				request,
				slash.name,
				slash.args,
				userRowId,
			);
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
	 * One native session command on an already-busy entry. The user row
	 * is already persisted; this persists the assistant reply row and
	 * returns the entry to ready. Native RPC throws settle through fail()
	 * so a dead child self-heals at the next generation. User errors
	 * (empty /resume, unknown model) return finalized replies, never
	 * error outcomes, and never touch prompt text.
	 */
	private async runSessionCommand(
		client: TurnClient,
		request: TurnRequest,
		name: string,
		args: string,
		userRowId: number | null,
	): Promise<TurnOutcome> {
		const session = asSessionClient(client);
		if (session === null) {
			return await this.fail(
				request.routingKey,
				"chat child cannot drive a session command",
				userRowId,
				null,
			);
		}
		let reply: string;
		try {
			switch (name) {
				case "new":
					reply = await this.runNewSession(session);
					break;
				case "resume":
					reply = await this.runResume(session, args);
					break;
				case "model":
					reply = await this.runModel(session, args);
					break;
				case "compact":
					reply = await this.runCompact(session, args);
					break;
				case "export":
					reply = await this.runExport(session, args);
					break;
				case "help":
					reply = await this.runHelp(session, args);
					break;
				default:
					return await this.fail(
						request.routingKey,
						`unknown session command: ${name}`,
						userRowId,
						null,
					);
			}
		} catch (error) {
			return await this.fail(
				request.routingKey,
				`${name} failed: ${RpcTurnRunner.errorText(error)}`,
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
					content: reply,
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
		const entry = this.registry.get(request.routingKey);
		if (entry !== undefined && entry.state === "busy") {
			entry.state = "ready";
		}
		return {
			exitReason: "finalized",
			finalText: reply,
			iterations: 0,
			repairs: 0,
			userRowId,
			assistantRowId,
			usage: null,
		};
	}

	/** Fresh host session; the reply names the new child session id. */
	private async runNewSession(client: SessionRpcClient): Promise<string> {
		await client.newSession();
		const state = (await client.getState()) as {
			sessionId: string;
		};
		return `Started a new session (${state.sessionId}).`;
	}

	/** Rebind onto a session file path. Empty args render usage. */
	private async runResume(
		client: SessionRpcClient,
		args: string,
	): Promise<string> {
		const target = args.trim().split(/\s+/, 1)[0] ?? "";
		if (target === "") {
			return "Usage: /resume <session-id> — rebinds this chat onto that session's history.";
		}
		await client.switchSession(target);
		const state = (await client.getState()) as {
			sessionId: string;
		};
		return `Resumed session (${state.sessionId}). The next turn replays its history.`;
	}

	/** Bare lists current plus catalog; a ref switches provider/model. */
	private async runModel(
		client: SessionRpcClient,
		args: string,
	): Promise<string> {
		const ref = args.trim();
		if (ref === "") {
			const [state, available] = await Promise.all([
				client.getState() as Promise<{
					model?: { provider?: string; id?: string } | null;
				}>,
				client.getAvailableModels(),
			]);
			const current =
				state.model?.provider !== undefined &&
				state.model?.id !== undefined
					? `${state.model.provider}/${state.model.id}`
					: "(none)";
			const names = available.map((m) => `${m.provider}/${m.id}`);
			return `Current model: ${current}\nAvailable: ${names.join(", ")}`;
		}
		let provider: string;
		let id: string;
		const slashAt = ref.indexOf("/");
		if (slashAt !== -1) {
			provider = ref.slice(0, slashAt).trim();
			id = ref.slice(slashAt + 1).trim();
			if (provider === "" || id === "") {
				return `Model switch failed: unknown model "${ref}".`;
			}
			const catalog = await client.getAvailableModels();
			const exact = catalog.some(
				(m) => m.provider === provider && m.id === id,
			);
			if (!exact) {
				return `Model switch failed: unknown model "${ref}".`;
			}
		} else {
			const catalog = await client.getAvailableModels();
			const hits = catalog.filter((m) => m.id === ref);
			if (hits.length === 0) {
				return `Model switch failed: unknown model "${ref}".`;
			}
			if (hits.length > 1) {
				const qualified = hits.map((m) => `${m.provider}/${m.id}`);
				return `Model switch failed: "${ref}" is ambiguous — use one of ${qualified.join(", ")}.`;
			}
			const hit = hits[0] as { provider: string; id: string };
			provider = hit.provider;
			id = hit.id;
		}
		try {
			const switched = await client.setModel(provider, id);
			return `Model: ${switched.provider}/${switched.id}`;
		} catch (error) {
			return `Model switch failed: ${RpcTurnRunner.errorText(error)}`;
		}
	}

	/** Host compaction; refusals render as reply text, never a throw. */
	private async runCompact(
		client: SessionRpcClient,
		args: string,
	): Promise<string> {
		const instructions = args.trim();
		try {
			const result =
				instructions === ""
					? await client.compact()
					: await client.compact(instructions);
			return `Compacted ${result.tokensBefore} tokens of context.\n\n${result.summary}`;
		} catch (error) {
			return `Compaction failed: ${RpcTurnRunner.errorText(error)}`;
		}
	}

	/** Host command census as `/name` lines. Zero model calls.
	 * Bare lists all. `skills` lists skill rows only. Any other filter
	 * matches the substring against name plus description. */
	private async runHelp(
		client: SessionRpcClient,
		args: string,
	): Promise<string> {
		const commands = await client.getCommands();
		const filter = args.trim().toLowerCase();
		const rows = commands.filter((c) => {
			if (filter === "") return true;
			if (filter === "skills") return c.name.startsWith("skill:");
			return (
				c.name.toLowerCase().includes(filter) ||
				c.description.toLowerCase().includes(filter)
			);
		});
		if (rows.length === 0) return "No commands match.";
		return rows.map((c) => `\`/${c.name}\` -- ${c.description}`).join("\n");
	}

	/** Branch entries as inline JSONL, or minimal HTML on html disposition. */
	private async runExport(
		client: SessionRpcClient,
		args: string,
	): Promise<string> {
		try {
			const { entries } = await client.getEntries();
			const jsonl =
				entries.map((e) => JSON.stringify(e)).join("\n") +
				(entries.length > 0 ? "\n" : "");
			const arg = args.trim().toLowerCase();
			if (arg === "html" || arg.endsWith(".html")) {
				return renderSessionExportHtml(jsonl);
			}
			return jsonl;
		} catch (error) {
			return `Export failed: ${RpcTurnRunner.errorText(error)}`;
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
