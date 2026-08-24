// pipeline.ts — process ONE claimed handoff row end-to-end (DEC-008 step
// sequence). Port of gateway/run.py:_process_handoff:
//
//   1. Validate handoff_platform.
//   2. Resolve the destination platform transport (must be live).
//   3. Require a configured home channel.
//   4. Try a dedicated thread on the destination (optional capability;
//      failure/absence falls back to the home channel's own thread).
//   5. Derive the destination SessionSource with Hermes' exact keying rules
//      (Telegram private-chat DM-topic shape; Discord thread keys on the
//      THREAD id, not the parent channel).
//   6. Compute the session_key via THE shared builder (02 §4.4 — one
//      predicate, no re-derivation).
//   7. Ensure a routing entry exists, then switch_session re-bind it onto the
//      CLI session id — the transcript-replay guarantee.
//   8. Forge ONE synthetic internal MessageEvent and dispatch through the
//      NORMAL pipeline (BOTH guards, forged-event machinery per DEC-022).
//
// Errors THROWN here are the watcher's fail signal: the caller records the
// message into sessions.handoff_error so the poll-blocked CLI can show it.
//
// COMPOSITION NOTE (vs Hermes letter): Hermes calls GatewayRunner
// ._handle_message INLINE and then sends the returned text via
// transport.send itself, because adapter.handle_message would spawn a fire-
// and-forget frame. Pi dispatches THROUGH the destination L1 guard's normal
// ingress instead (stronger traversal: L1 + L2 both apply), so egress rides
// the guard's own sendReply wiring — the watcher must NOT also send, or the
// reply would double-deliver. Observable behavior is identical: the agent's
// reply lands on the destination chat/thread.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/run.py:_handoff_watcher        → HandoffWatcher (watcher.ts)
//   gateway/run.py:_process_handoff        → HandoffPipeline.process
//   gateway/delivery.py:looks_like_telegram_private_chat_id
//                                          → looksLikeTelegramPrivateChatId
//   gateway/session.py:build_session_key   → shared buildSessionKey (imported)
//   hermes_state.py:switch_session         → RoutingBinder.switchSession

import type { IncomingEvent } from "../../pi_gateway/guards/events.js";
import {
	buildSessionKey,
	type IsolationFlags,
	type SessionSource,
} from "../../pi_gateway/resolution/session-key.js";
import type { RoutingEntry, RoutingEntrySeed } from "./binder.js";
import type { HandoffRow } from "./queue.js";
import { systemClock, type GatewayClock } from "./clock.js";

export { systemClock };
export type { GatewayClock };

/** Destination-platform liveness + optional capabilities (transport seam). */
export interface HandoffTransport {
	readonly platform: string;
	/**
	 * Create a fresh thread on the destination so the handoff has its own
	 * scrollback (base.py:create_handoff_thread). Absent capability,
	 * throwing implementation, or null return ⇒ fall back to the configured
	 * home channel (the synthetic turn still lands, just without thread
	 * isolation).
	 */
	createHandoffThread?(chatId: string, name: string): Promise<string | null>;
}

/** Configured home channel for a platform (config.get_home_channel shape). */
export interface HandoffHomeChannel {
	platform: string;
	chatId: string;
	threadId?: string;
	name?: string;
}

/**
 * The NORMAL-pipeline dispatch port. Production composition: the destination
 * platform's L1 AdapterSessionGuard.handleMessage (event already carries
 * internal=true + metadata.gateway_session_key), awaited to frame-chain
 * settlement so success/failure stays observable for complete/fail marking —
 * the stated reason Hermes dispatches inline. Rejecting resolves the row to
 * failed(+error) in the watcher.
 */
export interface SyntheticTurnDispatcher {
	dispatch(event: IncomingEvent): Promise<void>;
}

export interface HandoffLogger {
	debug?(message: string, meta?: Record<string, unknown>): void;
	info?(message: string, meta?: Record<string, unknown>): void;
	warn?(message: string, meta?: Record<string, unknown>): void;
}

/**
 * Structural binder view the pipeline consumes (RoutingBinder satisfies it;
 * tests/driver compositions may stub it).
 */
export interface HandoffBinderView {
	ensureEntry(
		sessionKey: string,
		seed?: RoutingEntrySeed,
		scope?: string,
	): Promise<RoutingEntry>;
	entryOf(sessionKey: string, scope?: string): RoutingEntry | null;
	switchSession(
		sessionKey: string,
		targetSessionId: string,
		scope?: string,
	): Promise<RoutingEntry | null>;
}

export interface HandoffPipelineDeps {
	/** Destination transport resolver (resolve_delivery_transport seam). */
	resolveTransport(platform: string): HandoffTransport | null;
	/** Home-channel resolution chain (config.get_home_channel seam). */
	resolveHomeChannel(platform: string): HandoffHomeChannel | null;
	/**
	 * Platform isolation flags (platform_cfg.extra parity). Absent ⇒ module
	 * defaults (group per-user TRUE, thread per-user FALSE — verified Hermes
	 * config defaults).
	 */
	isolationFlags?(platform: string): IsolationFlags | undefined;
	/** Profile namespace for session keys (default profile ⇒ agent:main). */
	profileName?: string;
	binder: HandoffBinderView;
	dispatcher: SyntheticTurnDispatcher;
	clock?: GatewayClock;
	log?: HandoffLogger;
}

/** Telegram private-chat heuristic: positive integer chat id. */
export function looksLikeTelegramPrivateChatId(
	chatId: string | null | undefined,
): boolean {
	if (chatId === null || chatId === undefined) return false;
	const text = String(chatId).trim();
	if (!/^[+-]?\d+$/.test(text)) return false; // Python int() strictness
	return Number.parseInt(text, 10) > 0;
}

interface DestinationDerivation {
	homeChatId: string;
	effectiveThreadId: string | undefined;
	destChatType: "thread" | "dm";
	destUserId: string;
	destChatId: string;
}

/**
 * The destination-source derivation block of _process_handoff, verbatim:
 *
 * - A created thread ⇒ 'thread' context UNLESS the destination is a Telegram
 *   private chat: DM-topic messages bind a `dm` source there, so the handoff
 *   turn must use the SAME shape as the user's next real message (else the
 *   synthetic turn keys `…:thread:{t}:{t}` while replies arrive on
 *   `…:dm:{chat}` — two sessions).
 * - Discord threads key on the THREAD's OWN id because organic in-thread
 *   messages carry chat_id == thread id; keying on the parent would strand
 *   the next real reply in a fresh session.
 */
export function deriveDestinationSource(input: {
	platform: string;
	newThreadId: string | null;
	home: HandoffHomeChannel;
}): DestinationDerivation {
	const { platform, newThreadId, home } = input;
	const homeChatId = String(home.chatId);
	const effectiveThreadId = newThreadId ?? home.threadId ?? undefined;

	const telegramPrivateChat =
		platform === "telegram" && looksLikeTelegramPrivateChatId(homeChatId);

	let destChatType: "thread" | "dm";
	let destUserId: string;
	if (newThreadId !== null && !telegramPrivateChat) {
		destChatType = "thread";
		destUserId = "system:handoff";
	} else {
		destChatType = "dm";
		destUserId = telegramPrivateChat ? homeChatId : "system:handoff";
	}

	let destChatId: string;
	if (
		platform === "discord" &&
		destChatType === "thread" &&
		effectiveThreadId !== undefined
	) {
		destChatId = String(effectiveThreadId);
	} else {
		destChatId = homeChatId;
	}

	return {
		homeChatId,
		effectiveThreadId,
		destChatType,
		destUserId,
		destChatId,
	};
}

const HANDOFF_NOTICE_TEMPLATE =
	'[Session was just handed off from CLI ("{title}") to this channel. ' +
	"The full prior conversation history is loaded above. Briefly confirm " +
	"you're working here and summarize what we were working on, so the user " +
	"can continue from this device.]";

export class HandoffPipeline {
	private readonly deps: HandoffPipelineDeps;
	private readonly clock: GatewayClock;

	constructor(deps: HandoffPipelineDeps) {
		this.deps = deps;
		this.clock = deps.clock ?? systemClock;
	}

	/**
	 * Execute one CLAIMED handoff row. Throws on failure — the watcher turns
	 * a throw into fail_handoff(row.id, str(error)) exactly like Hermes.
	 */
	async process(row: HandoffRow): Promise<void> {
		const cliSessionId = row.id;
		const platform = (row.handoffPlatform ?? "").trim().toLowerCase();
		if (!platform) throw new Error("handoff_platform is empty");

		// Transport must be live. (Hermes raises a distinct "unknown platform"
		// error because Platform is an enum; our platform vocabulary is open
		// (04 §4.2 registry), so an unresolvable name IS "not active".)
		const transport = this.deps.resolveTransport(platform);
		if (!transport) {
			throw new Error(`platform '${platform}' is not active in this gateway`);
		}

		// Home channel must be configured.
		const home = this.deps.resolveHomeChannel(platform);
		if (!home || !home.chatId) {
			throw new Error(
				`no home channel configured for ${platform}; ` +
					`run /sethome on the desired chat first`,
			);
		}

		const cliTitle = row.title || cliSessionId.slice(0, 8);

		// Optional dedicated thread on the destination. create_handoff_thread
		// raising is contained (debug log + fall-through) — never fails the
		// handoff.
		const threadName = `Pi — ${cliTitle}`;
		let newThreadId: string | null = null;
		if (transport.createHandoffThread) {
			try {
				newThreadId = await transport.createHandoffThread(
					String(home.chatId),
					threadName,
				);
			} catch (err) {
				this.deps.log?.debug?.("handoff: createHandoffThread raised", {
					platform,
					error: String(err),
				});
				newThreadId = null;
			}
		}

		const derived = deriveDestinationSource({
			platform,
			newThreadId,
			home,
		});

		const destSource: SessionSource = {
			platform,
			chatId: derived.destChatId,
			...(home.name !== undefined ? { chatName: home.name } : {}),
			chatType: derived.destChatType,
			userId: derived.destUserId,
			...(derived.effectiveThreadId !== undefined
				? { threadId: derived.effectiveThreadId }
				: {}),
		};

		// The SAME key rules the adapters use, so the next real user message on
		// that chat/thread resolves to THIS session (continuity invariant).
		const flags = this.deps.isolationFlags?.(platform) ?? {};
		const sessionKey = buildSessionKey(
			destSource,
			flags,
			this.deps.profileName,
		);

		// Ensure an entry exists for a never-used home channel, then re-bind.
		await this.deps.binder.ensureEntry(sessionKey, {
			origin: "handoff",
			display_name: home.name ?? null,
			platform,
			chat_type: derived.destChatType,
		});
		const switched = await this.deps.binder.switchSession(
			sessionKey,
			cliSessionId,
		);
		if (switched === null) {
			throw new Error(
				`could not switch session key ${sessionKey} → ${cliSessionId}`,
			);
		}

		const syntheticText = HANDOFF_NOTICE_TEMPLATE.replaceAll(
			"{title}",
			cliTitle,
		);

		// DEC-022 forged-event machinery: internal push-lane event carrying the
		// key it was forged for; traverses BOTH guards like any turn.
		const syntheticEvent: IncomingEvent = {
			messageType: "text",
			text: syntheticText,
			internal: true,
			source: destSource,
			metadata: { gateway_session_key: sessionKey },
		};

		this.deps.log?.info?.(
			"handoff: dispatching synthetic turn for CLI session",
			{
				cli_session: cliSessionId,
				platform,
				home: derived.homeChatId,
				thread: derived.effectiveThreadId ?? null,
				session_key: sessionKey,
			},
		);

		await this.deps.dispatcher.dispatch(syntheticEvent);
	}
}
