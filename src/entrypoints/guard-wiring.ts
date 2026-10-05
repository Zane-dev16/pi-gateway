// entrypoints/guard-wiring.ts — PRODUCTION guard wiring (DEC-074, DEC-084).
//
// DEC-084 subtraction: the embedded turn path dissolved (runner, binder,
// hop, chat lock map, idle and switch executors, lease registry, worker
// pool, agent cache, production factory). This module keeps the transport
// contract only: durable session-ensure, allowlist authz, then the injected
// turn runner. Per-chat RPC children own turns next (Todo 3). All slash
// bytes ride the turn untouched. No binder, hop, lock, or executor remains.

import { isUserAuthorized } from "../pi_gateway/security/authz/decision.js";
import type {
	AuthzDecisionRecord,
	AuthzSource,
} from "../pi_gateway/security/authz/decision.js";
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
 * Structural turn-runner surface. The RPC child owns the turn next.
 * handleTurn carries session plus routing key plus original bytes.
 */
export interface ChatTurnRunner {
	handleTurn(request: {
		sessionId: string;
		routingKey: string;
		text: string;
	}): Promise<TurnOutcome>;
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

export interface ProductionMessageHandlerDeps {
	runner: ChatTurnRunner;
	/** Stage-6 store for the durable session row (absent ⇒ ensure skipped). */
	store?: EnsureSessionStore | null | undefined;
	/** Authz override (tests); default is the ported decision chain. */
	isAuthorized?: ((source: AuthzSource) => AuthzDecisionRecord) | undefined;
	log?: GuardWiringLogger | undefined;
}

/**
 * THE production messageHandler: durable session-ensure, allowlist authz,
 * then the turn on original bytes. No slash table remains. The host owns
 * every command natively through RPC next.
 *
 * Denied senders drop SILENTLY in-chat with the denial LOGGED carrying
 * reason_code plus gate. Runner error outcomes re-throw so the guard
 * renders the radio-silence error notice.
 */
export function buildProductionMessageHandler(
	deps: ProductionMessageHandlerDeps,
): MessageHandler {
	const { runner, log } = deps;
	const decide =
		deps.isAuthorized ?? ((source: AuthzSource) => isUserAuthorized(source));
	return async (
		event: IncomingEvent,
		_ctx: TurnContext,
	): Promise<string | null | undefined> => {
		const sessionKey = String(
			(event.metadata ?? {})["gateway_session_key"] ?? "",
		);
		const driveSessionId = sessionKey;
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
		const text = event.text ?? "";
		const outcome = await runner.handleTurn({
			sessionId: driveSessionId,
			routingKey: sessionKey,
			text,
		});
		if (outcome.exitReason === "error") {
			throw new Error(outcome.errorMessage ?? "turn error");
		}
		return outcome.finalText;
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
