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
// deliverText detected, never imported). The only runtime project import is
// the authz decision chain (pi_gateway/security/authz — no parameter
// properties, no enums anywhere in its import closure).

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
 * THE production messageHandler (run.py:_handle_message parity): durable
 * session-ensure → allowlist authz → runner turn → final text.
 *
 * - sessionId = routingKey = the adapter-derived ingress sessionKey (cron
 *   executor precedent: routingKey === sessionId). Ensuring the durable row
 *   engages the runner's DB turn-lease layer; a fresh id would skip it
 *   (runner.ts:runTurn — "process-unique, nothing to race over").
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
	return async (
		event: IncomingEvent,
		_ctx: TurnContext,
	): Promise<string | null | undefined> => {
		const sessionKey = String(
			(event.metadata ?? {})["gateway_session_key"] ?? "",
		);
		if (deps.store !== undefined && deps.store !== null && sessionKey !== "") {
			await deps.store.withWrite((db) => {
				(db as { prepare(sql: string): { run(...args: unknown[]): void } })
					.prepare(
						"INSERT OR IGNORE INTO sessions (id, source, started_at) VALUES (?, 'gateway', ?)",
					)
					.run(sessionKey, Math.floor(Date.now() / 1000));
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
		const outcome = await runner.handleTurn({
			sessionId: sessionKey,
			routingKey: sessionKey,
			text: event.text ?? "",
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
