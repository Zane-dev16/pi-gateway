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

import { resolve } from "node:path";
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
 * Help rides get_commands so it answers with zero model calls. Path
 * commands live in PATH_COMMANDS below: they stop plus respawn the child
 * at the target cwd before any rebind, so they never reach this set. */
const NATIVE_SESSION_COMMANDS = new Set([
	"new",
	"resume",
	"model",
	"compact",
	"export",
	"help",
]);

/** Path commands: stop the chat child plus respawn it at the target cwd.
 * Matched on the hyphen canonical form; the Telegram menu sends the
 * underscore spelling, so callers canonicalize before lookup. */
const PATH_COMMANDS = new Set(["switch-path", "new-path"]);

/** Hyphen canonical form of a slash name (`switch_path` to `switch-path`).
 * Existing native names carry neither mark, so canonicalizing is a no-op
 * for them. */
function canonicalCommandName(name: string): string {
	return name.replace(/_/g, "-");
}

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

/**
 * Trimmed provider/id pair (gateway/session.py:sanitize_model_override
 * parity — blank halves are not a model). Null when either half is blank.
 */
function sanitizeModelOverride(
	provider: string,
	modelId: string,
): { provider: string; id: string } | null {
	const cleanProvider = provider.trim();
	const cleanId = modelId.trim();
	if (cleanProvider === "" || cleanId === "") return null;
	return { provider: cleanProvider, id: cleanId };
}

/**
 * Bare-select wait bound (DEC-090 parity without importing rank-3:
 * bare resume and model own the turn with no agent timeout, so the
 * generic select carries the same 120s expiry the bespoke asks had).
 */
const BARE_SELECT_TIMEOUT_MS = 120_000;

function titleOf(event: TurnRpcEvent, fallback: string): string {
	const title = event["title"];
	if (typeof title === "string" && title.trim() !== "") return title;
	const message = event["message"];
	if (typeof message === "string" && message.trim() !== "") return message;
	return fallback;
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
	/** Transport chat id for picker egress (guard messageHandler fills it
	 * from the event source; absent means pickers render text-only). */
	chatId?: string | undefined;
}

/**
 * Picker outcome shapes (mirrors pi_gateway/pickers outcomes WITHOUT
 * importing upward — this module is rank 2, the service is rank 3).
 */
export type TurnPickerOutcome =
	| { kind: "value"; value: string }
	| { kind: "confirmed"; confirmed: boolean }
	| { kind: "cancelled" };

export type TurnModelPickOutcome =
	| { status: "switched"; provider: string; id: string; text: string }
	| { status: "cancelled" };

export type TurnResumePickOutcome =
	| { status: "resumed"; id: string; text: string }
	| { status: "cancelled" };

export interface TurnRecentSession {
	id: string;
	file: string;
	label: string;
}

/**
 * Structural picker-service seam (the rank-3 PickerService satisfies this;
 * tests inject fakes). Asks settle exactly one outcome each — value,
 * confirmed, or cancelled on timeout/abort — so no extension_ui_request
 * ever hangs the turn.
 */
export interface TurnPickerService {
	askSelect(opts: {
		chatId?: string | undefined;
		sessionKey: string;
		uiId: string;
		title: string;
		options: string[];
		timeoutMs?: number | null | undefined;
		signal?: AbortSignal | undefined;
		respond: (uiId: string, body: Record<string, unknown>) => Promise<void>;
	}): Promise<TurnPickerOutcome>;
	askConfirm(opts: {
		chatId?: string | undefined;
		sessionKey: string;
		uiId: string;
		title: string;
		message: string;
		timeoutMs?: number | null | undefined;
		signal?: AbortSignal | undefined;
		respond: (uiId: string, body: Record<string, unknown>) => Promise<void>;
	}): Promise<TurnPickerOutcome>;
	askText(opts: {
		chatId?: string | undefined;
		sessionKey: string;
		uiId: string;
		title: string;
		hint?: string | undefined;
		timeoutMs?: number | null | undefined;
		signal?: AbortSignal | undefined;
		respond: (uiId: string, body: Record<string, unknown>) => Promise<void>;
	}): Promise<TurnPickerOutcome>;
	askModel(opts: {
		chatId?: string | undefined;
		sessionKey: string;
		catalog: Array<{ provider: string; id: string }>;
		current: { provider: string; id: string } | null;
		timeoutMs?: number | null | undefined;
		signal?: AbortSignal | undefined;
	}): Promise<TurnModelPickOutcome>;
	askResume(opts: {
		chatId?: string | undefined;
		sessionKey: string;
		sessions: TurnRecentSession[];
		timeoutMs?: number | null | undefined;
		signal?: AbortSignal | undefined;
	}): Promise<TurnResumePickOutcome>;
	/**
	 * Drop every live picker for a session (parked-picker takeover: a new
	 * turn aborts the parked wait so the chat never wedges behind a picker
	 * nobody taps). Returns how many were live.
	 */
	cancelSession(sessionKey: string): number;
}

/**
 * extension_ui_response writer (DEC-088). The default writes a raw JSONL
 * line preserving the host-issued id through the RpcClient-owned child
 * stdin (the only write path that keeps server correlation — send()
 * overwrites id). Tests inject a capturing fake.
 */
export type ExtensionUIResponder = (
	client: TurnClient,
	response: Record<string, unknown>,
) => Promise<void>;

export function defaultExtensionUIResponder(
	client: TurnClient,
	response: Record<string, unknown>,
): Promise<void> {
	const proc = client as unknown as {
		process?: { stdin?: { write(chunk: string): boolean } | null } | null;
	};
	const stdin = proc.process?.stdin;
	if (stdin === undefined || stdin === null) {
		return Promise.reject(new Error("extension_ui_response has no child stdin handle"));
	}
	return new Promise<void>((resolve, reject) => {
		try {
			writeResponseLine(stdin, `${JSON.stringify(response)}\n`, (err?: Error | null) => {
				if (err !== undefined && err !== null) reject(err);
				else resolve();
			});
		} catch (err) {
			reject(err);
		}
	});
}

function writeResponseLine(
	stdin: { write(chunk: string): boolean },
	line: string,
	done: (err?: Error | null) => void,
): void {
	try {
		const flushed = stdin.write(line);
		void flushed;
		done();
	} catch (err) {
		done(err instanceof Error ? err : new Error(String(err)));
	}
}

export interface RpcTurnRunnerOpts {
	registry: ChatProcRegistry;
	/** Child PI_HOME for a chat key. Production shares the gateway home. */
	resolveHome: (routingKey: string) => string;
	/**
	 * Session discovery at a home dir. Production scans <home>/sessions
	 * through pi_gateway/discovery (injected here so this module never
	 * imports upward). Absent means no paths and no rebind targets:
	 * switch-path reports none while the cwd move still lands.
	 */
	listDiscoveryPaths?: ((homeDir: string) => string[]) | undefined;
	/** Newest session file at a path, or null when the path holds none. */
	findSessionFileAtPath?:
		| ((homeDir: string, rawPath: string) => string | null)
		| undefined;
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
	/**
	 * Shared picker service (rank-3 PickerService through the structural
	 * seam above). Absent means extension_ui_requests go unanswered and the
	 * agent auto-resolves on its own timeout; bare /resume|/model keep the
	 * legacy text renders.
	 */
	pickers?: TurnPickerService | undefined;
	/** extension_ui_response writer. Absent keeps the DEC-088 default. */
	extensionUIResponder?: ExtensionUIResponder | undefined;
	/**
	 * Recent sessions newest-first for the bare /resume picker. Production
	 * flattens the discovery index (injected here so this module never
	 * imports upward). Absent means bare /resume renders usage.
	 */
	listRecentSessions?: ((homeDir: string) => TurnRecentSession[]) | undefined;
}

export class RpcTurnRunner {
	private readonly registry: ChatProcRegistry;
	private readonly resolveHome: (routingKey: string) => string;
	private readonly listDiscoveryPaths:
		| ((homeDir: string) => string[])
		| undefined;
	private readonly findSessionFileAtPath:
		| ((homeDir: string, rawPath: string) => string | null)
		| undefined;
	/** Per-chat cwd keyed by routing key. Absent means the gateway home. */
	private readonly chatCwd = new Map<string, string>();
	private readonly store: TurnMessageStore | null;
	private readonly provider: string | undefined;
	private readonly model: string | undefined;
	private readonly turnTimeoutMs: number | undefined;
	private readonly onTurnEvent:
		| ((event: TurnRpcEvent) => void)
		| undefined;
	private readonly pickers: TurnPickerService | undefined;
	private readonly extensionUIResponder: ExtensionUIResponder;
	private readonly listRecentSessions:
		| ((homeDir: string) => TurnRecentSession[])
		| undefined;
	/**
	 * Per-chat model override from picker commits (sanitize_model_override
	 * parity — trimmed provider/id pairs). Reads at spawn so RESPAWNS keep
	 * the picked model (override write-through); the live child switches
	 * via setModel with no evict (DEC-084: the child owns its session and
	 * model natively — there is no gateway agent cache to drop, and the
	 * commit text it returns rides the turn reply, so no pending-note
	 * prepend is needed either).
	 */
	private readonly chatModel = new Map<string, { provider: string; id: string }>();

	constructor(opts: RpcTurnRunnerOpts) {
		this.registry = opts.registry;
		this.resolveHome = opts.resolveHome;
		this.listDiscoveryPaths = opts.listDiscoveryPaths;
		this.findSessionFileAtPath = opts.findSessionFileAtPath;
		this.store = opts.store ?? null;
		this.provider = opts.provider;
		this.model = opts.model;
		this.turnTimeoutMs = opts.turnTimeoutMs;
		this.onTurnEvent = opts.onTurnEvent;
		this.pickers = opts.pickers;
		this.extensionUIResponder = opts.extensionUIResponder ?? defaultExtensionUIResponder;
		this.listRecentSessions = opts.listRecentSessions;
	}

	/** Picker commit seam the service drives (tap → setModel on the child). */
	async commitPickerModel(sessionKey: string, provider: string, modelId: string): Promise<string> {
		const clean = sanitizeModelOverride(provider, modelId);
		if (clean === null) throw new Error(`unknown model "${provider}/${modelId}"`);
		const entry = this.registry.get(sessionKey);
		if (entry === undefined) throw new Error("chat child is gone");
		const turn = asTurnClient(entry.client);
		const session = turn !== null ? asSessionClient(turn) : null;
		if (session === null) throw new Error("chat child cannot switch models");
		const switched = await session.setModel(clean.provider, clean.id);
		this.chatModel.set(sessionKey, { provider: switched.provider, id: switched.id });
		return `Model: ${switched.provider}/${switched.id}`;
	}

	/** Picker commit seam the service drives (tap → switchSession). */
	async commitPickerResume(sessionKey: string, file: string): Promise<string> {
		const entry = this.registry.get(sessionKey);
		if (entry === undefined) throw new Error("chat child is gone");
		const turn = asTurnClient(entry.client);
		const session = turn !== null ? asSessionClient(turn) : null;
		if (session === null) throw new Error("chat child cannot resume sessions");
		await session.switchSession(file);
		const state = (await session.getState()) as { sessionId: string };
		return `Resumed session (${state.sessionId}). The next turn replays its history.`;
	}

	/**
	 * The rank-3 PickerCommit surface bound to this runner (the composition
	 * root feeds it to the PickerService constructor next to this runner).
	 */
	pickerCommitSurface(): {
		commitModel(sessionKey: string, provider: string, modelId: string): Promise<string>;
		commitResume(sessionKey: string, file: string): Promise<string>;
	} {
		return {
			commitModel: (sessionKey, provider, id) => this.commitPickerModel(sessionKey, provider, id),
			commitResume: (sessionKey, file) => this.commitPickerResume(sessionKey, file),
		};
	}

	/** Chat cwd for one routing key. Absent means the gateway home. */
	cwdFor(routingKey: string, homeDir: string): string {
		return this.chatCwd.get(routingKey) ?? homeDir;
	}

	/**
	 * Bounded wait for a parked turn to release its entry after its picker
	 * was cancelled (the parked waiter settles on microtasks plus the
	 * response write plus row persists — dozens of setImmediate yields is
	 * plenty; a genuinely running turn never reaches here because
	 * cancelSession settled nothing for it). Undefined when still held.
	 */
	private async waitForEntryRelease(
		routingKey: string,
	): Promise<ReturnType<ChatProcRegistry["get"]> | undefined> {
		for (let i = 0; i < 200; i += 1) {
			await new Promise((resolve) => setImmediate(resolve));
			const live = this.registry.get(routingKey);
			if (live !== undefined && live.state === "ready") return live;
		}
		return undefined;
	}

	/**
	 * Drive one turn on the chat's child and return its outcome. Never
	 * throws for turn failures: they come back as error outcomes so the
	 * guard renders its radio-silence notice. Busy chats fail fast.
	 *
	 * Session commands (/new, /resume, /model, /compact, /export) ride
	 * native RPC calls, never prompt text: one deterministic reply path
	 * per name. Path commands (/switch-path, /new-path) stop plus respawn
	 * the child at the target cwd first, then rebind. Every other slash
	 * word falls through to the host prompt path, where the child loop
	 * owns it natively.
	 */
	async handleTurn(request: TurnRequest): Promise<TurnOutcome> {
		const homeDir = this.resolveHome(request.routingKey);
		const earlySlash = parseSlashCommand(request.text);
		if (
			earlySlash !== null &&
			PATH_COMMANDS.has(canonicalCommandName(earlySlash.name))
		) {
			return await this.runPathCommand(
				request,
				canonicalCommandName(earlySlash.name),
				earlySlash.args,
				homeDir,
			);
		}
		const override = this.chatModel.get(request.routingKey);
		const spawnOpts: ChatProcSpawnOpts = {
			homeDir,
			cwd: this.cwdFor(request.routingKey, homeDir),
		};
		if (override !== undefined) {
			spawnOpts.provider = override.provider;
			spawnOpts.model = override.id;
		} else {
			if (this.provider !== undefined) spawnOpts.provider = this.provider;
			if (this.model !== undefined) spawnOpts.model = this.model;
		}

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
			// Parked-picker takeover: a picker nobody taps must never wedge
			// the chat behind busy. Abort it, let the parked turn release
			// the entry, then take over when free (one re-check, bounded).
			const cancelled = this.pickers?.cancelSession(request.routingKey) ?? 0;
			if (cancelled > 0) {
				const freed = await this.waitForEntryRelease(request.routingKey);
				if (freed !== undefined) {
					entry = freed;
				} else {
					return RpcTurnRunner.errorOutcome(
						"turn already in progress for this chat",
						null,
						null,
					);
				}
			} else {
				return RpcTurnRunner.errorOutcome(
					"turn already in progress for this chat",
					null,
					null,
				);
			}
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
				homeDir,
			);
		}

		let turnStarts = 0;
		// Per-turn abort: parked picker waits settle cancelled when the turn
		// settles or fails, so no extension_ui_request ever dangles past it.
		const pickerAbort = new AbortController();
		const unsubscribe = client.onEvent((event) => {
			if (event.type === "turn_start") turnStarts += 1;
			if (event.type === "extension_ui_request") {
				void this.handleExtensionUI(client, request, event, pickerAbort.signal);
			}
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
				// The real client resolves undefined when the turn left no
				// assistant text (handled extension command): coerce at the
				// boundary so downstream persist/delivery never see null.
				finalText = (await client.getLastAssistantText()) ?? "";
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
			pickerAbort.abort();
			unsubscribe();
		}
	}

	/**
	 * One extension_ui_request parked on this turn: forward select/confirm
	 * to the picker service (chat-native controls + extension_ui_response),
	 * everything else (input/editor/unknown incl. any future custom
	 * payload) to the numbered-text fallback lane. Fire-and-forget from the
	 * event callback — waitForIdle settles once the child consumes the
	 * response. Without a picker service the request goes unanswered and
	 * the agent auto-resolves on its own timeout (rpc-extension-ui.md).
	 * Never throws: a raising forward still settles cancelled inside the
	 * service, and the event loop must never break on a UI tap.
	 */
	private async handleExtensionUI(
		client: TurnClient,
		request: TurnRequest,
		event: TurnRpcEvent,
		signal: AbortSignal,
	): Promise<void> {
		const pickers = this.pickers;
		if (pickers === undefined) return;
		const uiId = event["id"];
		if (typeof uiId !== "string" || uiId === "") return;
		const method = event["method"];
		const methodName = typeof method === "string" ? method : "";
		// Fire-and-forget methods (rpc-extension-ui.md) carry information,
		// never a waiter: answering or prompting on them would render bogus
		// chat noise (a "question" the agent never asked) and send a
		// response the server drops (no pending entry). Only dialogs park.
		if (
			methodName === "notify" ||
			methodName === "setStatus" ||
			methodName === "setWidget" ||
			methodName === "setTitle" ||
			methodName === "set_editor_text"
		) {
			return;
		}
		const timeout = event["timeout"];
		const timeoutMs = typeof timeout === "number" && Number.isFinite(timeout) && timeout > 0 ? timeout : null;
		const respond = (responseId: string, body: Record<string, unknown>): Promise<void> =>
			this.extensionUIResponder(client, { type: "extension_ui_response", id: responseId, ...body });
		const base = {
			...(request.chatId !== undefined ? { chatId: request.chatId } : {}),
			sessionKey: request.routingKey,
			signal,
			...(timeoutMs !== null ? { timeoutMs } : {}),
		};
		try {
			if (methodName === "select") {
				const raw = event["options"];
				const options = Array.isArray(raw) ? raw.filter((o): o is string => typeof o === "string") : [];
				await pickers.askSelect({
					...base,
					uiId,
					title: titleOf(event, "Select"),
					options,
					respond,
				});
			} else if (methodName === "confirm") {
				await pickers.askConfirm({
					...base,
					uiId,
					title: titleOf(event, "Confirm"),
					message: typeof event["message"] === "string" ? event["message"] : "",
					respond,
				});
			} else {
				// input/editor/unknown: numbered-text fallback when options
				// ride along, open-text question otherwise. Either way the
				// turn completes on the typed reply.
				const raw = event["options"];
				const options = Array.isArray(raw) ? raw.filter((o): o is string => typeof o === "string") : [];
				if (options.length > 0) {
					await pickers.askSelect({
						...base,
						uiId,
						title: titleOf(event, "Select"),
						options,
						respond,
					});
				} else {
					const placeholder = typeof event["placeholder"] === "string" ? event["placeholder"] : undefined;
					await pickers.askText({
						...base,
						uiId,
						title: titleOf(event, "Input requested"),
						...(placeholder !== undefined ? { hint: placeholder } : {}),
						respond,
					});
				}
			}
		} catch {
			/* the service settles cancelled on every failure path already */
		}
	}

	/**
	 * One path command (DEC-085). Bare /switch-path lists paths without
	 * touching the registry. Bare /new-path renders usage. Otherwise the
	 * live entry stops even when dead, the chat cwd moves to the resolved
	 * target, a fresh child spawns with cwd set, and the rebind lands:
	 * switch-path onto the newest session file at the target, new-path
	 * onto a fresh session. A mid-flight child refuses instead of dying.
	 */
	private async runPathCommand(
		request: TurnRequest,
		name: string,
		args: string,
		homeDir: string,
	): Promise<TurnOutcome> {
		if (name === "switch-path" && args === "") {
			const paths = this.listDiscoveryPaths?.(homeDir) ?? [];
			const reply =
				paths.length === 0
					? "No paths holding pi sessions."
					: `Paths holding pi sessions:\n${paths.join("\n")}`;
			return await this.persistedReply(request, reply);
		}
		const rawTarget = args.split(/\s+/, 1)[0] ?? "";
		if (rawTarget === "") {
			return await this.persistedReply(
				request,
				"Usage: /new-path <path> — starts a fresh session under that path.",
			);
		}
		const target = resolve(this.cwdFor(request.routingKey, homeDir), rawTarget);
		const current = this.registry.get(request.routingKey);
		if (
			current !== undefined &&
			(current.state === "busy" || current.state === "starting")
		) {
			return RpcTurnRunner.errorOutcome(
				"turn already in progress for this chat",
				null,
				null,
			);
		}
		try {
			await this.registry.stop(request.routingKey);
		} catch {
			/* the entry already left the map; respawn anyway */
		}
		this.chatCwd.set(request.routingKey, target);
		const spawnOpts: ChatProcSpawnOpts = { homeDir, cwd: target };
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
		const turnClient = asTurnClient(entry.client);
		if (turnClient === null) {
			return RpcTurnRunner.errorOutcome(
				"chat child cannot drive a turn",
				null,
				null,
			);
		}
		const session = asSessionClient(turnClient);
		if (session === null) {
			return RpcTurnRunner.errorOutcome(
				"chat child cannot drive a session command",
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
		let reply: string;
		try {
			if (name === "switch-path") {
				const file = this.findSessionFileAtPath?.(homeDir, target) ?? null;
				if (file === null) {
					reply = `Switched to ${target}. No sessions there yet — starting fresh.`;
				} else {
					await session.switchSession(file);
					const state = (await session.getState()) as {
						sessionId: string;
					};
					reply = `Switched to ${target}. Resumed session (${state.sessionId}).`;
				}
			} else {
				await session.newSession();
				const state = (await session.getState()) as {
					sessionId: string;
				};
				reply = `Started a fresh session (${state.sessionId}) under ${target}.`;
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
		const live = this.registry.get(request.routingKey);
		if (live !== undefined && live.state === "busy") {
			live.state = "ready";
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

	/** Finalized reply with both message rows, no child involved. */
	private async persistedReply(
		request: TurnRequest,
		reply: string,
	): Promise<TurnOutcome> {
		let userRowId: number | null = null;
		let assistantRowId: number | null = null;
		if (this.store !== null) {
			try {
				userRowId = await this.store.appendMessage({
					sessionId: request.sessionId,
				role: "user",
				content: request.text,
				});
				assistantRowId = await this.store.appendMessage({
					sessionId: request.sessionId,
				role: "assistant",
				content: reply,
				});
			} catch (error) {
				return RpcTurnRunner.errorOutcome(
					`assistant row persist failed: ${RpcTurnRunner.errorText(error)}`,
					userRowId,
					null,
				);
			}
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
		homeDir: string,
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
					reply = await this.runResume(session, request, args, homeDir);
					break;
				case "model":
					reply = await this.runModel(session, request, args);
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

	/**
	 * Rebind onto a session file path. Bare with a picker service plus a
	 * session lister routes to the generic select (DEC-091): options are
	 * session labels, tap or typed number resolves, then the runner drives
	 * switchSession plus getState itself; otherwise empty args render usage.
	 */
	private async runResume(
		client: SessionRpcClient,
		request: TurnRequest,
		args: string,
		homeDir: string,
	): Promise<string> {
		const target = args.trim().split(/\s+/, 1)[0] ?? "";
		if (target === "") {
			if (
				this.pickers !== undefined &&
				request.chatId !== undefined &&
				this.listRecentSessions !== undefined
			) {
				const sessions = this.listRecentSessions(homeDir);
				if (sessions.length === 0) {
					return "No recent sessions to resume.";
				}
				const outcome = await this.pickers.askSelect({
					chatId: request.chatId,
					sessionKey: request.routingKey,
					uiId: "bare-resume",
					title: "Resume session",
					options: sessions.map((row) => row.label),
					timeoutMs: BARE_SELECT_TIMEOUT_MS,
					respond: async () => {},
				});
				if (outcome.kind !== "value") return "Resume cancelled.";
				const row =
					sessions.find((candidate) => candidate.label === outcome.value) ??
					sessions.find(
						(candidate) =>
							candidate.label.toLowerCase() === outcome.value.toLowerCase(),
					) ??
					null;
				if (row === null) return "Resume cancelled.";
				try {
					await client.switchSession(row.file);
				} catch (error) {
					return `Resume failed: ${RpcTurnRunner.errorText(error)}`;
				}
				const state = (await client.getState()) as {
					sessionId: string;
				};
				this.pickers.cancelSession(request.routingKey);
				return `Resumed session (${state.sessionId}). The next turn replays its history.`;
			}
			return "Usage: /resume <session-id> — rebinds this chat onto that session's history.";
		}
		await client.switchSession(target);
		const state = (await client.getState()) as {
			sessionId: string;
		};
		return `Resumed session (${state.sessionId}). The next turn replays its history.`;
	}

	/**
	 * Bare routes to the model picker (provider drill-down; tap commits
	 * through setModel with the override written through for respawns).
	 * A ref switches provider/model directly as before. Without a picker
	 * service (or chat id) bare lists current plus catalog as before.
	 */
	private async runModel(
		client: SessionRpcClient,
		request: TurnRequest,
		args: string,
	): Promise<string> {
		const ref = args.trim();
		if (ref === "") {
			if (this.pickers !== undefined && request.chatId !== undefined) {
				const [state, catalog] = await Promise.all([
					client.getState() as Promise<{
						model?: { provider?: string; id?: string } | null;
					}>,
					client.getAvailableModels(),
				]);
				const current =
					state.model?.provider !== undefined && state.model?.id !== undefined
						? { provider: state.model.provider, id: state.model.id }
						: null;
				const outcome = await this.pickers.askModel({
					chatId: request.chatId,
					sessionKey: request.routingKey,
					catalog,
					current,
				});
				if (outcome.status === "switched") return outcome.text;
				return "Model picker cancelled.";
			}
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
			const clean = sanitizeModelOverride(switched.provider, switched.id);
			if (clean !== null) this.chatModel.set(request.routingKey, clean);
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
