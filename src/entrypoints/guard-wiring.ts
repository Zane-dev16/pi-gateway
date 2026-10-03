// entrypoints/guard-wiring.ts — PRODUCTION guard wiring (DEC-074).
//
// The gap: stage-9 entries constructed adapters and connect()ed them, but no
// production path ever attached the runner guard, so every ingress threw
// "<platform>: no guard attached — wire the runner first" (base-adapter.ts:
// handleIngress) and turns died before authz/leases/model could run.
// *-subject.ts harnesses attach stub guards; production attached none.
//
// The fix (generic over EVERY hosted adapter — matrix today, telegram
// wherever it shares the stage-9 path): stage-9 wires handlers in the
// instantiate → wire-handlers → connect window the lifecycle documents
// (lifecycle.ts:stagePlatformAdapters), Hermes run.py:_create_adapter parity
// (adapter.set_message_handler(self._primary_message_handler()) BEFORE
// connect; gateway/platforms/base.py:set_message_handler).
//
// Strip posture: this module is importable from gateway-run.ts, which stays
// strip-safe for bare-node runners — so kit types arrive as TYPE-ONLY
// imports (erased) and adapters are touched STRUCTURALLY (attachGuard /
// deliverText detected, never imported). Runtime project imports are the
// authz decision chain (pi_gateway/security/authz — no parameter properties,
// no enums anywhere in its import closure) plus the pure slash idle path
// (commands/* + guards/slash-access — no node builtins, no platform code).

import { randomUUID } from "node:crypto";
import { statSync } from "node:fs";
import { resolve } from "node:path";
import { createBuiltinCommandRegistry } from "../pi_gateway/commands/builtins.js";
import {
	buildIdleExecutors,
	type IdleExecutor,
	type IdleExecutorResult,
} from "../pi_gateway/commands/idle-executors.js";
import {
	adoptSessionFile,
	type AdoptionStore,
} from "../pi_embedded/handoff/adoption.js";
import type { RoutingBinder } from "../pi_embedded/handoff/binder.js";
import type { SessionDriverLock } from "../pi_embedded/handoff/session-lock.js";
import {
	buildDiscoveryIndex,
	listDiscoveryPaths,
	mostRecentSessionAtPath,
} from "../pi_gateway/discovery.js";
import { classifySlashIntake } from "../pi_gateway/commands/slash-intake.js";
import { isUserAuthorized } from "../pi_gateway/security/authz/decision.js";
import type {
	AuthzDecisionRecord,
	AuthzSource,
} from "../pi_gateway/security/authz/decision.js";
import {
	checkSlashAccess,
	type SlashAccessPolicy,
} from "../pi_gateway/guards/slash-access.js";
import type {
	CommandDef as GuardCommandDef,
	CommandRegistry,
	IncomingEvent,
	MessageHandler,
	TurnContext,
} from "../pi_gateway/guards/index.js";
import type { CommandDef as RegistryCommandDef } from "../pi_gateway/commands/command-def.js";
import type { TurnOutcome } from "../pi_agent_core/runner-types.js";
import type { StateStore } from "../pi_state/index.js";

/**
 * Structural turn-runner surface (GatewayAgentRunner subset — the cron
 * executor's precedent: consume the runner structurally, never import it).
 * handleTurn owns the two-layer turn-lease prologue + the host model turn
 * (pi_agent_core/runner.ts:GatewayAgentRunner.handleTurn).
 */
export interface ChatTurnRunner {
	handleTurn(request: {
		sessionId: string;
		routingKey: string;
		text: string;
		/**
		 * Bound working path for this chat (the persisted binder entry
		 * cwd). Optional — absent falls back to the session's stored
		 * sessions.cwd, then process.cwd(). Guard-wiring threads the
		 * entry through; runners without a cache simply ignore it.
		 */
		cwd?: string | undefined;
	}): Promise<TurnOutcome>;
	/**
	 * Drop the cached host session so the next turn rebuilds and reseeds
	 * (DEC-079 switch executors: the binding moved, the cache must follow).
	 * Optional — runners without a cache simply omit it. The production
	 * GatewayAgentRunner already carries this exact method; no runner
	 * change was needed.
	 */
	dropCachedSession?: ((sessionId: string) => void) | undefined;
	/**
	 * Manual compaction over the runner's cached host session
	 * (runner.ts:GatewayAgentRunner.compactSession). Optional — without it
	 * /compact stays passthrough on original bytes.
	 */
	compactSession?:
		| ((
				sessionId: string,
				customInstructions?: string,
			) => Promise<{ summary: string; tokensBefore: number }>)
		| undefined;
	/**
	 * /model over the runner's cached host session
	 * (runner.ts:GatewayAgentRunner.setSessionModel). Optional — without it
	 * /model stays passthrough on original bytes.
	 */
	setSessionModel?:
		| ((sessionId: string, modelRef: string) => Promise<{
					provider: string;
					id: string;
			  }>)
		| undefined;
	/** Current model of the cached host session (bare-/model listing). */
	getSessionModel?:
		| ((sessionId: string) => Promise<{ provider: string; id: string }>)
		| undefined;
	/** Catalog /model refs resolve against (catalog order). */
	listAvailableModels?:
		| (() => ReadonlyArray<{ provider: string; id: string }>)
		| undefined;
	/**
	 * /export over the runner's cached host session
	 * (runner.ts:GatewayAgentRunner.exportSessionJsonl — INLINE bytes,
	 * never a file write). Optional — without it /export stays passthrough.
	 */
	exportSessionJsonl?: ((sessionId: string) => Promise<string>) | undefined;
}

/**
 * Session-hopping deps (DEC-079): the chat-key → host-session-id binder,
 * the host agent dir scanned for <agentDir>/sessions, the dir holding
 * gateway.lock.db (drive sidecars live beside it), the adoption store, and
 * an id minter (tests pin it deterministic; production mints randomUUIDs).
 * Absent ⇒ today's behavior byte-identical: sessionId = chat key and the
 * switch names stay passthrough on original bytes.
 */
export interface SessionHopDeps {
	binder: RoutingBinder;
	agentDir: string;
	lockDir: string;
	adoptStore: AdoptionStore;
	newSessionId?: (() => string) | undefined;
}

/**
 * Factory seam (GatewayRunInput.turnRunnerFactory): a FACTORY, not an
 * instance, because the runner needs the lifecycle-owned stage-6 store,
 * which does not exist at composition time. Stage 9 calls it ONCE per
 * process (memoized — Hermes one-runner parity: every adapter shares the
 * single composed handler).
 */
export type TurnRunnerFactory = (deps: {
	store: StateStore | null;
}) => ChatTurnRunner | Promise<ChatTurnRunner>;

/** Minimal structural logger (lifecycle Logger subset). */
export interface GuardWiringLogger {
	info(message: string, meta?: Record<string, unknown>): void;
	warn(message: string, meta?: Record<string, unknown>): void;
	error(message: string, meta?: Record<string, unknown>): void;
}

/** Structural session-ensure store (StateStore satisfies this). */
export interface EnsureSessionStore {
	withWrite<T>(fn: (db: unknown) => T): Promise<T>;
}

/**
 * Idle-path slash dispatch (DEC-078): resolve → slash-access gate → executor
 * table → turn fallthrough. Absent (the production default) resolves against
 * the frozen builtin registry with no access gating (backward-compat: no
 * admin list ⇒ every allowed user keeps every command); tests inject a stub.
 */
export interface SlashIdleDispatch {
	resolve: (rawName: string | null | undefined) => RegistryCommandDef | null;
	executors: ReadonlyMap<string, IdleExecutor>;
	rows: readonly RegistryCommandDef[];
	policyOf?: ((event: IncomingEvent) => SlashAccessPolicy) | undefined;
}

/** One switch executor: chat key + dash-repaired args ⇒ reply or turn. */
type SwitchExecutor = (
	sessionKey: string,
	args: string,
) => Promise<IdleExecutorResult>;

/**
 * The DEC-079 switch table over the binder: /new repoints the chat at a
 * fresh host session; /resume lands on history via binder switchSession +
 * replay through the normal pipeline; /switch-path lists every path
 * holding pi sessions and re-roots the chat there (adopting the newest
 * file under the drive lock); /new-path starts a fresh session under a
 * given path. /sessions stays absent by design — pure host passthrough.
 * Held drive locks ride a per-handler chat map: the next switch for that
 * chat releases before rebinding, and process death frees via the fd.
 */
function buildSwitchExecutors(
	hop: SessionHopDeps,
	locks: Map<string, SessionDriverLock>,
	runner: ChatTurnRunner,
): ReadonlyMap<string, SwitchExecutor> {
	const mintId = hop.newSessionId ?? randomUUID;
	const releaseChatLock = (sessionKey: string): void => {
		const held = locks.get(sessionKey);
		if (held === undefined) return;
		locks.delete(sessionKey);
		held.release();
	};
	const rebind = async (sessionKey: string, id: string): Promise<void> => {
		await hop.binder.ensureEntry(sessionKey, { origin: "gateway" });
		await hop.binder.switchSession(sessionKey, id);
	};
	return new Map<string, SwitchExecutor>([
		[
			"new",
			async (sessionKey): Promise<IdleExecutorResult> => {
				const id = mintId();
				await rebind(sessionKey, id);
				releaseChatLock(sessionKey);
				runner.dropCachedSession?.(id);
				return { kind: "reply", text: `Started a new session (${id}).` };
			},
		],
		[
			"resume",
			async (sessionKey, args): Promise<IdleExecutorResult> => {
				const id = args.trim().split(/\s+/, 1)[0] ?? "";
				if (id === "") {
					return {
						kind: "reply",
						text: "Usage: /resume <session-id> — rebinds this chat onto that session's history.",
					};
				}
				await rebind(sessionKey, id);
				releaseChatLock(sessionKey);
				runner.dropCachedSession?.(id);
				return {
					kind: "reply",
					text: `Resumed session (${id}). The next turn replays its history.`,
				};
			},
		],
		[
			"switch-path",
			async (sessionKey, args): Promise<IdleExecutorResult> => {
				const index = buildDiscoveryIndex(hop.agentDir);
				const target = args.trim();
				if (target === "") {
					const paths = listDiscoveryPaths(index);
					if (paths.length === 0) {
						return {
							kind: "reply",
							text: `No pi sessions found under ${hop.agentDir}.`,
						};
					}
					return {
						kind: "reply",
						text:
							`Paths holding pi sessions:\n${paths.map((p) => `- ${p}`).join("\n")}` +
							"\n/switch-path <path> re-roots this chat there.",
					};
				}
				const found = mostRecentSessionAtPath(index, target);
				if (found === null) {
					const paths = listDiscoveryPaths(index);
					const known =
						paths.length === 0
							? "none — run /switch-path to confirm"
							: paths.map((p) => `- ${p}`).join("\n");
					return {
						kind: "reply",
						text: `No pi sessions under ${target}. Known paths:\n${known}`,
					};
				}
				await hop.binder.ensureEntry(sessionKey, { origin: "gateway" });
				const disposition = await adoptSessionFile({
					sessionFile: found.file,
					sessionId: found.id,
					lockDir: hop.lockDir,
					store: hop.adoptStore,
				});
				if (disposition.kind === "readonly-plus-takeover") {
					return {
						kind: "reply",
						text:
							`Session ${found.id} is live in another process, so this chat stays ` +
							`read-only: ask its owner to exit, then run /switch-path ${target} ` +
							`again to take over.`,
					};
				}
				releaseChatLock(sessionKey);
				locks.set(sessionKey, disposition.lock);
				await hop.binder.switchSession(sessionKey, found.id);
				await hop.binder.setEntryCwd(sessionKey, found.cwd);
				runner.dropCachedSession?.(found.id);
				return {
					kind: "reply",
					text:
						`Chat re-rooted onto ${target}: adopted ${disposition.entryCount} ` +
						`entries from session ${found.id}.`,
				};
			},
		],
		[
			"new-path",
			async (sessionKey, args): Promise<IdleExecutorResult> => {
				const target = args.trim();
				if (target === "") {
					return {
						kind: "reply",
						text: "Usage: /new-path <path> — starts a fresh session under that path.",
					};
				}
				let isDir = false;
				try {
					isDir = statSync(target).isDirectory();
				} catch {
					isDir = false;
				}
				if (!isDir) {
					return { kind: "reply", text: `No such directory: ${target}.` };
				}
				const id = mintId();
				await rebind(sessionKey, id);
				await hop.binder.setEntryCwd(sessionKey, resolve(target));
				releaseChatLock(sessionKey);
				runner.dropCachedSession?.(id);
				return {
					kind: "reply",
					text: `Fresh session (${id}) under ${target}.`,
				};
			},
		],
	]);
}

export interface ProductionMessageHandlerDeps {
	runner: ChatTurnRunner;
	/** Stage-6 store for the durable session row (absent ⇒ ensure skipped). */
	store?: EnsureSessionStore | null | undefined;
	/**
	 * Session-hopping seam (DEC-079). Absent ⇒ the pre-079 path exactly:
	 * the chat key drives the turn and switch names stay passthrough.
	 */
	sessionHop?: SessionHopDeps | null | undefined;
	/** Authz override (tests); default is the ported decision chain. */
	isAuthorized?: ((source: AuthzSource) => AuthzDecisionRecord) | undefined;
	log?: GuardWiringLogger | undefined;
	/** Idle slash dispatch override (tests); default derives from builtins. */
	slashIdle?: SlashIdleDispatch | null | undefined;
}

/** Production default: frozen builtin registry rows + the idle table. */
function defaultSlashIdle(): SlashIdleDispatch {
	const registry = createBuiltinCommandRegistry();
	const rows = registry.rows();
	return {
		resolve: (raw) => registry.resolve(raw),
		executors: buildIdleExecutors(rows),
		rows,
	};
}

/**
 * THE production messageHandler (run.py:_handle_message parity): durable
 * session-ensure → allowlist authz → runner turn → final text.
 *
 * - sessionId is the binder-resolved host session for the adapter-derived
 *   ingress sessionKey (DEC-079; pre-hop: sessionId = routingKey = the key,
 *   the cron-executor precedent routingKey === sessionId). Ensuring the
 *   durable row engages the runner's DB turn-lease layer; a fresh id would
 *   skip it (runner.ts:runTurn — "process-unique, nothing to race over").
 * - Denied senders drop SILENTLY in-chat (Hermes _handle_message returns
 *   None; 06 §2.4 groups stay silent) with the denial LOGGED carrying
 *   reason_code + gate (06 §2.3 — reason codes make silent drops
 *   debuggable). DM pairing is a deferred follow-up, not this commit.
 * - Runner error outcomes re-throw (harness parity) so the guard renders
 *   the radio-silence error notice + failure outcome (👀→❌ lane).
 */
export function buildProductionMessageHandler(
	deps: ProductionMessageHandlerDeps,
): MessageHandler {
	const { runner, log } = deps;
	const decide =
		deps.isAuthorized ?? ((source: AuthzSource) => isUserAuthorized(source));
	// Held foreign-drive locks by chat key (DEC-079): one handler, one map.
	const chatLocks = new Map<string, SessionDriverLock>();
	return async (
		event: IncomingEvent,
		_ctx: TurnContext,
	): Promise<string | null | undefined> => {
		const sessionKey = String(
			(event.metadata ?? {})["gateway_session_key"] ?? "",
		);
		// DEC-079: the chat key resolves onto its bound HOST session id
		// (binder entry, minted on first contact). Without the hop seam the
		// driving id IS the chat key — the pre-079 path, byte-identical.
		const hop = deps.sessionHop ?? null;
		const switchExecutors =
			hop !== null ? buildSwitchExecutors(hop, chatLocks, runner) : null;
		let driveSessionId = sessionKey;
		let driveCwd: string | null = null;
		if (hop !== null && sessionKey !== "") {
			const bound =
				hop.binder.entryOf(sessionKey) ??
				(await hop.binder.ensureEntry(sessionKey, { origin: "gateway" }));
			driveSessionId = bound.session_id;
			driveCwd =
				typeof bound.cwd === "string" && bound.cwd !== "" ? bound.cwd : null;
		}
		if (
			deps.store !== undefined &&
			deps.store !== null &&
			driveSessionId !== ""
		) {
			await deps.store.withWrite((db) => {
				(db as { prepare(sql: string): { run(...args: unknown[]): void } })
					.prepare(
						"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'gateway', ?)",
					)
					.run(driveSessionId, Math.floor(Date.now() / 1000));
			});
		}
		const source = event.source;
		const record = decide({
			platform: source?.platform ?? null,
			userId: source?.userId ?? null,
			chatId: source?.chatId ?? null,
			chatType: source?.chatType ?? null,
		});
		if (!record.allowed) {
			log?.warn("ingress denied", {
				reason_code: record.reasonCode,
				gate: record.gate,
				platform: record.platform,
				user_id: record.userId,
				chat_id: record.chatId,
			});
			return null;
		}
		const runTurn = async (
			text: string,
		): Promise<string | null | undefined> => {
			const outcome = await runner.handleTurn({
				sessionId: driveSessionId,
				routingKey: sessionKey,
				text,
				...(driveCwd !== null ? { cwd: driveCwd } : {}),
			});
			if (outcome.exitReason === "error") {
				throw new Error(outcome.errorMessage ?? "turn error");
			}
			return outcome.finalText;
		};
		// DEC-078 idle path: recognized commands dispatch (or deny) locally;
		// plain text AND unknown "/foo" take the turn on ORIGINAL bytes.
		const idle = deps.slashIdle ?? defaultSlashIdle();
		const text = event.text ?? "";
		const intake = classifySlashIntake(
			idle.resolve,
			text,
			event.allowGatewayControl === undefined
				? {}
				: { allowGatewayControl: event.allowGatewayControl },
		);
		if (intake.kind === "text") return runTurn(text);
		if (idle.policyOf !== undefined) {
			const denied = checkSlashAccess(
				idle.policyOf(event),
				event.source?.userId ?? null,
				intake.cmd.name,
			);
			if (denied !== null) return denied;
		}
		// DEC-079 switch table owns the session-hopping names; the idle
		// table owns the rest. Absent hop ⇒ switchExecutors is null and the
		// recognized-but-executorless names (/new, /resume, /sessions …)
		// fall through to the turn on ORIGINAL bytes, as before.
		const switchExecutor = switchExecutors?.get(intake.cmd.name);
		if (switchExecutor !== undefined) {
			const result = await switchExecutor(sessionKey, intake.args);
			if (result.kind === "reply") return result.text;
			return runTurn(result.text);
		}
		const executor = idle.executors.get(intake.cmd.name);
		if (executor !== undefined) {
			const result = await executor({
				sessionKey,
				args: intake.args,
				rows: idle.rows,
				runner,
				eventText: text,
				hostSessionId: driveSessionId,
			});
			if (result.kind === "reply") return result.text;
			return runTurn(result.text);
		}
		return runTurn(text);
	};
}

export interface ProductionGuardDeps {
	registry: CommandRegistry;
	messageHandler: MessageHandler;
	sendReply: (chatId: string, text: string) => Promise<void>;
}

/**
 * Project the frozen class registry onto the guard's row subset (07 §9 —
 * no hand-built lists: the guard reads THE registry through this
 * projection). Busy-policy values are the identical triple on both sides;
 * a null busyHandler projects to absent (exactOptionalPropertyTypes).
 */
export function toGuardRegistry(
	rows: readonly RegistryCommandDef[],
): CommandRegistry {
	return rows.map(
		(row): GuardCommandDef => ({
			name: row.name,
			...(row.aliases !== undefined ? { aliases: [...row.aliases] } : {}),
			...(row.busyPolicy !== undefined ? { busyPolicy: row.busyPolicy } : {}),
			...(row.busyHandler !== undefined && row.busyHandler !== null
				? { busyHandler: row.busyHandler }
				: {}),
		}),
	);
}

/** Structural guard slot (kit BasePlatformAdapter.attachGuard, detected). */
interface GuardSlot {
	attachGuard(deps: ProductionGuardDeps): void;
}

function asGuardSlot(adapter: unknown): GuardSlot | null {
	if (
		typeof adapter === "object" &&
		adapter !== null &&
		typeof (adapter as { attachGuard?: unknown }).attachGuard === "function"
	) {
		return adapter as GuardSlot;
	}
	return null;
}

/**
 * Adapter-bound reply sink: the adapter's OWN text pipeline
 * (BasePlatformAdapter.deliverText — chunking + ladder + retry), falling
 * back to raw send. Null when the surface exposes neither (not a kit
 * adapter — wiring skips loudly).
 */
export function buildAdapterSendReply(
	adapter: unknown,
): ((chatId: string, text: string) => Promise<void>) | null {
	if (typeof adapter !== "object" || adapter === null) return null;
	const deliver = (adapter as Record<string, unknown>)["deliverText"];
	if (typeof deliver === "function") {
		const fn = deliver as (
			chatId: string,
			content: string,
		) => Promise<unknown> | unknown;
		const self = adapter;
		return async (chatId: string, text: string): Promise<void> => {
			await fn.call(self, chatId, text);
		};
	}
	const send = (adapter as Record<string, unknown>)["send"];
	if (typeof send === "function") {
		const fn = send as (
			chatId: string,
			content: string,
		) => Promise<unknown> | unknown;
		const self = adapter;
		return async (chatId: string, text: string): Promise<void> => {
			await fn.call(self, chatId, text);
		};
	}
	return null;
}

/**
 * Attach the production guard to one constructed adapter. TRUE when wired;
 * FALSE when the surface exposes no guard slot (exotic surfaces connect
 * unwired — today's behavior, logged loudly by the caller).
 */
export function tryAttachProductionGuard(
	adapter: unknown,
	deps: ProductionGuardDeps,
): boolean {
	const slot = asGuardSlot(adapter);
	if (slot === null) return false;
	slot.attachGuard(deps);
	return true;
}
