// pi_platforms/slack/slack-adapter — THE Slack census port (roadmap Phase 6,
// DEC-001/DEC-002: every remaining platform arrives as a PORT behind the
// conformance gate, not a design exercise).
//
// Transport family: persistent-WS (Socket Mode). The ENTIRE reference engine
// is inherited unchanged from ../persistent-ws — watchdog (`_socket_ping_pong_stale`
// semantics), reconnect ladder with Retry-After authority, resume cursor +
// bounded redelivery dedup (#4777), A23 capability latch, dual-path markdown
// split, egress chokepoint (DEC-006 both doors), kit guards/router/stores/
// block-kit machinery. Only SHAPE DELTAS live here:
//
//   1. Socket-Mode envelope handling — hello/subscribed handshake mapping,
//      PER-ENVELOPE ack frames ({type:"ack",envelope_id}) emitted on receipt
//      (adapter.py:_start_socket_mode_handler — slack_bolt's SocketModeClient
//      acks EVERY envelope within 3s), retry-flagged redeliveries shaping
//      workspace-scoped replay dedup, cursor advance AS the durable ack point
//      (module header of ./fake-socket-mode.ts carries the full mapping table).
//   2. Manifest data — plugin.yaml env specs, Q17 Tier-2 rate budgets consumed
//      BEFORE egress per method class (./rate-gate.ts), 39000-unit budget
//      (adapter.py:MAX_MESSAGE_LENGTH), "!" typed prefix, splits-long-messages.
//   3. Dual-path binding at THE Slack REST boundary — EVERY
//      chat.postMessage/chat.update-shaped transmission converts through the
//      faithful format_message port (./mrkdwn.ts) exactly once; the native
//      *Stream path ships RAW (§10.2; §5 invariant 1). Authored-mrkdwan lanes
//      (Block Kit cards, §6.1 fallback bodies) are byte-exempt.
//   4. Block Kit cards through the kit parallel mechanism (DEC-016 §9.2) —
//      stable hermes_* action_ids mapped onto THE namespaced grammar via ONE
//      interactivity handler; bounded workspace-scoped resolved maps
//      (_APPROVAL_RESOLVED_MAX=1000); block-rejection retries drop blocks
//      (_is_block_payload_rejection codes).
//   5. Approvals bridge seam — class-level sendExecApproval declared on the
//      prototype so the pi_embedded DeliveryBridge picks CARD-FIRST delivery
//      (hasExecApprovalCard walks prototypes; MagicMock-safe class probe
//      parity).
//   6. Identity + per-turn emoji lanes — auth.test at connect resolves
//      selfUserId/team scope from the TOKEN (adapter.py:connect :1968);
//      SLACK_REACTIONS-default-true 👀→✅/❌ lifecycle rides reactions.add/
//      reactions.remove around dispatch (:4217-4284); chat.delete exposes the
//      opt-in cleanup_progress capability (delete_message :3085); message_changed
//      envelopes normalize onto changed-ts-deduped fresh turns (:5773).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/slack/adapter.py:connect            (Socket Mode up)
//   plugins/platforms/slack/adapter.py:_handle_slack_message
//     (subtype/bot filtering; workspace-scoped dedup id
//     _workspace_event_id; thread_ts → session threading
//     _build_thread_session_key)
//   plugins/platforms/slack/adapter.py:send               (convert → chunk →
//     blocks on single-chunk only → block-rejection retry)
//   plugins/platforms/slack/adapter.py:edit_message       (chat.update:
//     finalize-only blocks; transient transport error keeps the message id
//     retryable)
//   plugins/platforms/slack/adapter.py:send_draft/_seal_stream (RAW stream;
//     suffix-only seal)
//   plugins/platforms/slack/adapter.py:send_exec_approval /
//     _handle_approval_action / _is_interactive_user_authorized
//   plugins/platforms/slack/adapter.py:_slack_dedup_ttl_seconds (3600s)

import type {
	Metadata,
	SendResult,
	EditOptions,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type {
	IncomingEvent,
	CommandRegistry,
} from "../../pi_gateway/guards/index.js";
import type {
	WsConnectionFactory,
	WsClientSocket,
	WsSocketListener,
	WsFrame,
	WsPlatformEvent,
} from "../persistent-ws/fake-ws.js";
import {
	PersistentWsAdapter,
	type PersistentWsAdapterDeps,
	type RestPlane,
} from "../persistent-ws/persistent-ws-adapter.js";
import { EventDeduplicator } from "../persistent-ws/event-cursor.js";
import type { CapabilityManifest } from "../kit/capabilities.js";
import type { RateBudget } from "../kit/capabilities.js";
import {
	resolveEnablement,
	type EnvVarSpec,
	type ScopedSecretReader,
} from "../kit/registration.js";
import { FormattingLadder } from "../kit/formatting-ladder.js";
import type { FormattingTransport } from "../kit/formatting-ladder.js";
import {
	classifySendError,
	sendWithRetry,
	plainTextFallbackBody,
	DELIVERY_FAILED_NOTICE,
	PLAIN_TEXT_FALLBACK_PREFIX,
} from "../kit/send-retry.js";
import { chunkWithFenceCarry } from "../kit/chunking.js";
import {
	buildClarifyCallback,
	buildExecApprovalCallback,
	buildSlashConfirmCallback,
	CLARIFY_CHOICE_ACTION_RE,
} from "../kit/index.js";
import { SLASH_CONFIRM_ACTION_IDS } from "../kit/block-kit.js";
import type {
	ExecApprovalSendArgs,
	ApprovalSendResult,
} from "../../pi_embedded/approvals/delivery.js";

import {
	SLACK_MANIFEST,
	SLACK_DEDUP_TTL_MS,
	SLACK_RESOLVED_MAP_MAX,
} from "./manifest.js";
import { RateBudgetGate } from "./rate-gate.js";
import { convertMarkdownToSlackMrkdwn } from "./mrkdwn.js";
import { isBlockPayloadRejectionError } from "./block-rejection.js";
import {
	buildClarifyCard,
	buildContentBlocks,
	buildExecApprovalCard,
	buildSlashConfirmCard,
} from "./block-cards.js";
import type { SlackInteractivePayload } from "./fake-socket-mode.js";

/** Slack's five-command conformance registry (identical to the ws shape). */
export const SLACK_REGISTRY: CommandRegistry = [
	{
		name: "new",
		aliases: ["reset"],
		busyPolicy: "interrupt_then_dispatch" as const,
		busyHandler: "new",
	},
	{
		name: "stop",
		busyPolicy: "interrupt_then_dispatch" as const,
		busyHandler: "stop",
	},
	{ name: "model", busyPolicy: "reject" as const, busyHandler: "model" },
	{ name: "approve", busyPolicy: "dispatch" as const },
	{ name: "status", busyPolicy: "dispatch" as const },
];

export interface SlackAdapterDeps extends PersistentWsAdapterDeps {
	/** platforms.slack.extra.rich_blocks opt-in (§9.2/§10.2). */
	richBlocks?: boolean | undefined;
	/** Q17 budget override; defaults to SLACK_MANIFEST.rateBudget. */
	rateBudget?: RateBudget | undefined;
	/** Workspace scope for event-id/dedup keying (_workspace_event_id). */
	workspaceId?: string | undefined;
	/**
	 * SLACK_REACTIONS override (config `reactions` fold parity). Unset ⇒ the
	 * SLACK_REACTIONS env decides (default true; "false"/"0"/"no" disables).
	 */
	reactionsEnabled?: boolean | undefined;
}

interface InteractiveTapContext {
	userId: string;
	channelId: string;
	msgTs: string;
	teamId: string;
	/** Host message blocks from the interaction payload (resolve edit). */
	messageBlocks?: unknown[] | undefined;
}

/** Late-bound state the pre-super rest wrapper closures read. */
interface SlackRenderHooks {
	richBlocksEnabled(): boolean;
	/** Local audit for block-rejection retries (keys left OFF the wire). */
	onBlocksDroppedOnRetry(): void;
	onBlocksClearedOnRetry(): void;
	/** Channel→team scope for stream START recipient_team_id (:2600/_channel_team). */
	recipientTeamFor(chatId: string): string | undefined;
}

/** auth.test response slice the adapter consumes (adapter.py:connect :1968). */
export interface SlackAuthIdentity {
	/** Slack answers {ok:true,…}; ok=false is an explicit auth failure. */
	ok: boolean;
	/** Bot user id resolved FROM THE TOKEN (`user_id`). */
	userId?: string | undefined;
	/** Workspace team scope resolved from the token (`team_id`). */
	teamId?: string | undefined;
	/** Bot display name (`user`) / workspace name (`team`) — audit only. */
	user?: string | undefined;
	team?: string | undefined;
	error?: string | undefined;
}

/**
 * Optional Slack-specific REST extensions beyond the shared RestPlane.
 * assistant.threads.setStatus / files_upload_v2 / conversations.open /
 * reactions.add·remove / auth.test / chat.delete are REAL provider calls the
 * family interface doesn't model; subjects bind them through dedicated
 * capture lanes (adapter.py:send_typing/_upload_file/_ensure_dm_conversation/
 * _add_reaction/_remove_reaction/connect/delete_message).
 */
export interface SlackRestExtras {
	transmitThreadStatus?(
		channelId: string,
		threadTs: string,
		status: string,
	): Promise<void>;
	transmitUpload?(op: {
		channel: string;
		filename: string;
		initialComment: string;
		threadTs?: string | undefined;
	}): Promise<SendResult>;
	openDirectMessage?(userId: string): Promise<string | null>;
	/**
	 * reactions.add / reactions.remove {channel,timestamp,name} — per-turn
	 * processing/result emojis (adapter.py:_add_reaction :4217 /
	 * _remove_reaction :4233).
	 */
	transmitReaction?(
		channelId: string,
		ts: string,
		name: string,
		action: "add" | "remove",
	): Promise<SendResult>;
	/** auth.test — token identity probe at connect (adapter.py:1968). */
	authTest?(): Promise<SlackAuthIdentity>;
	/** chat.delete {channel,ts} — opt-in cleanup_progress lane (:3085). */
	transmitDelete?(channelId: string, messageId: string): Promise<SendResult>;
}

function extrasOf(rest: RestPlane): SlackRestExtras {
	return rest as RestPlane & SlackRestExtras;
}

/**
 * adapter.py:_resolve_thread_ts — metadata thread_id/thread_ts wins over
 * reply_to (which may be a child message's ts); reply_to_message_id is THE
 * gateway chokepoint's reply stamp. Resolved ONCE here and emitted as the
 * vendor wire key `thread_ts` on send/startStream args.
 */
export function resolveSlackThreadTs(metadata: Metadata): string | undefined {
	const md = metadata as Record<string, unknown>;
	for (const key of ["thread_id", "thread_ts", "reply_to_message_id"]) {
		const v = md[key];
		if (typeof v === "string" && v.length > 0) return v;
	}
	return undefined;
}

/** Bare U/W targets need conversations.open before postMessage/upload. */
export function isDmUserTarget(chatId: string): boolean {
	return /^[UW]/.test(chatId);
}

/**
 * adapter.py:_reactions_enabled — SLACK_REACTIONS default true; the literal
 * "false"/"0"/"no" (any case) disables the per-turn emoji lifecycle.
 */
export function isSlackReactionsEnabled(env: string | undefined): boolean {
	const v = (env ?? "true").toLowerCase();
	return !(v === "false" || v === "0" || v === "no");
}

/** Bounded processed-original-ts window (:985/_PROCESSED_MESSAGE_TS_MAX). */
export const SLACK_PROCESSED_MESSAGE_TS_MAX = 5000;

/**
 * THE Slack REST boundary. Every postMessage/update-shaped transmission
 * passes exactly once: RATE GATE first (Q17 consult-before-egress), then the
 * dialect conversion (native-stream frames exempt — they arrive via
 * transmitDraft which stays RAW), then Block Kit attachment, then the
 * block-rejection retry WITHOUT blocks.
 */
export function makeSlackRestPlane(
	inner: RestPlane,
	gate: RateBudgetGate,
	hooks: SlackRenderHooks,
): RestPlane & SlackRestExtras {
	const refused = (error: string, retryAfterMs: number): SendResult => ({
		success: false,
		error,
		retryable: false,
		retryAfter: Math.max(1, Math.ceil(retryAfterMs / 1000)),
	});

	/** Authored-mrkdwan/§6.1-fallback lanes ship byte-exact (see header). */
	function needsConversion(content: string, metadata: Metadata): boolean {
		if (metadata["_slack_render"] === "as-is") return false;
		if (content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)) return false;
		return true;
	}

	function extractBlocks(metadata: Metadata): {
		blocks: unknown[] | null;
		rest: Metadata;
	} {
		const {
			_slack_blocks: candidate,
			_slack_render: _render,
			...restMd
		} = metadata as Record<string, unknown>;
		void _render;
		const blocks = Array.isArray(candidate) ? (candidate as unknown[]) : null;
		return { blocks, rest: restMd as Metadata };
	}

	/**
	 * Emit the RESOLVED thread root under the vendor wire key (`thread_ts`) and
	 * strip the gateway-internal targeting stamps that fed the resolution —
	 * chat.postMessage carries thread_ts, never the chokepoint's reply stamp.
	 */
	function withWireThreadTs(md: Metadata): Metadata {
		const resolved = resolveSlackThreadTs(md);
		const out: Metadata = { ...md };
		// The internal stamps FEED the resolution; only the vendor key ships.
		delete out["reply_to_message_id"];
		delete out["thread_id"];
		if (resolved !== undefined) out["thread_ts"] = resolved;
		else delete out["thread_ts"];
		return out;
	}

	/** conversations.open ahead of send/upload (adapter.py:_ensure_dm_conversation). */
	async function resolveChatTarget(chatId: string): Promise<string> {
		if (!isDmUserTarget(chatId)) return chatId;
		const dmId = await extrasOf(inner).openDirectMessage?.(chatId);
		return dmId ?? chatId;
	}

	async function transmitSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const decision = gate.check("send");
		if (!decision.admitted) {
			return refused(`rate_limited:${decision.tier}`, decision.retryAfterMs);
		}
		const target = await resolveChatTarget(chatId);
		const { blocks, rest } = extractBlocks(metadata);
		const text = needsConversion(content, metadata)
			? convertMarkdownToSlackMrkdwn(content)
			: content;
		const md = withWireThreadTs(rest as Metadata);
		if (blocks !== null) md["blocks"] = blocks;
		const sent = await inner.transmitSend(target, text, md);
		if (
			!sent.success &&
			blocks !== null &&
			isBlockPayloadRejectionError(sent.error ?? "")
		) {
			// Block Kit is a PROGRESSIVE ENHANCEMENT — retry without blocks so a
			// rendering bug can never drop the response (send() parity). The
			// drop is LOCAL audit state only — no invented flags ride the wire.
			hooks.onBlocksDroppedOnRetry();
			const { blocks: _dropped, ...withoutBlocks } = md as Record<
				string,
				unknown
			>;
			return inner.transmitSend(target, text, withoutBlocks as Metadata);
		}
		return sent;
	}

	async function transmitEdit(
		chatId: string,
		messageId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const decision = gate.check("edit");
		if (!decision.admitted) {
			return refused(`rate_limited:${decision.tier}`, decision.retryAfterMs);
		}
		const { blocks: candidate, rest } = extractBlocks(metadata);
		const finalize = metadata["finalize_edit"] === true;
		let blocks = candidate;
		if (blocks === null && finalize && hooks.richBlocksEnabled()) {
			// Finalize-only Block Kit pass (edit_message parity: intermediate
			// updates stay plain mrkdwn — re-deriving layout per flush would be
			// wasteful and jittery).
			blocks = buildContentBlocks(content);
		}
		const text = needsConversion(content, metadata)
			? convertMarkdownToSlackMrkdwn(content)
			: content;
		const md: Metadata = blocks !== null ? { ...rest, blocks } : { ...rest };
		let updated = await inner.transmitEdit(chatId, messageId, text, md);
		if (
			!updated.success &&
			blocks !== null &&
			isBlockPayloadRejectionError(updated.error ?? "")
		) {
			// Explicitly CLEAR stale blocks on the flat-text update path —
			// otherwise the prior block layout survives the edit. Recorded as
			// LOCAL audit state; the wire payload just ships `blocks: []`.
			hooks.onBlocksClearedOnRetry();
			updated = await inner.transmitEdit(chatId, messageId, text, {
				...withWireThreadTs(rest as Metadata),
				blocks: [],
			} as Metadata);
		}
		return updated;
	}

	async function transmitDraft(
		chatId: string,
		draftId: number,
		content: string,
		final: boolean,
		metadata: Metadata,
	): Promise<SendResult> {
		// Native *Stream plane: Tier-2 streaming class gating, RAW bytes —
		// conversion belongs to the REST path only (§10.2).
		const decision = gate.check(final ? "draft-stop" : "draft-start");
		if (!decision.admitted) {
			return refused(`rate_limited:${decision.tier}`, decision.retryAfterMs);
		}
		const {
			_slack_blocks: _cand,
			_slack_render: _render,
			...rest
		} = metadata as Record<string, unknown>;
		void _cand;
		void _render;
		const md = rest as Metadata;
		if (final !== true && md["stream_op"] === "start") {
			// chat.startStream REQUIRES a thread target (send_draft parity:
			// thread_ts rides the START args; appends inherit the stream). The
			// guard fires BEFORE the API call (:3196-3201 — "Streamed messages
			// must anchor to a thread_ts … this is rare").
			if (resolveSlackThreadTs(md) === undefined) {
				return { success: false, error: "no thread_ts for native stream" };
			}
			const start = withWireThreadTs(md);
			// Channels require the recipient team/user pair; harmless extras for
			// DMs, so include them whenever known (:3199-3213 — metadata
			// user_id/sender_id renamed to recipient_user_id, team scope from
			// the channel→team map). Only recipient_* ships on the vendor frame.
			const recipientUser = md["user_id"] ?? md["sender_id"];
			delete start["user_id"];
			delete start["sender_id"];
			if (typeof recipientUser === "string" && recipientUser !== "") {
				start["recipient_user_id"] = recipientUser;
			}
			const teamId = hooks.recipientTeamFor(chatId);
			if (teamId !== undefined && teamId !== "") {
				start["recipient_team_id"] = teamId;
			}
			return inner.transmitDraft(chatId, draftId, content, final, start);
		}
		return inner.transmitDraft(
			chatId,
			draftId,
			content,
			final,
			withWireThreadTs(md),
		);
	}

	const plane: RestPlane & SlackRestExtras = {
		transmitSend,
		transmitEdit,
		transmitDraft,
		transmitRich: (chatId, content, metadata) =>
			// Rich probe lane — no Slack REST analog (Block Kit renders locally);
			// ungated passthrough keeps the shared §10.1 rows meaningful.
			inner.transmitRich(chatId, content, metadata),
		hasScript: (opKind) => inner.hasScript(opKind),
		// Slack-specific provider calls ride UNGATED (assistant.threads.setStatus
		// and files_upload_v2 are outside the chat.postMessage/update/stream
		// method classes the Q17 budgets model).
		transmitThreadStatus: async (channelId, threadTs, status) => {
			await extrasOf(inner).transmitThreadStatus?.(channelId, threadTs, status);
		},
		transmitUpload: async (op) => {
			const upload = extrasOf(inner).transmitUpload;
			if (upload === undefined) {
				return { success: false, error: "files_upload_v2 not available" };
			}
			return upload(op);
		},
		openDirectMessage: async (userId) =>
			extrasOf(inner).openDirectMessage?.(userId) ?? null,
		// reactions.add/reactions.remove, auth.test, chat.delete ride UNGATED —
		// outside the chat.postMessage/update/stream method classes the Q17
		// budgets model (same posture as setStatus/uploads above).
		transmitReaction: async (channelId, ts, name, action) => {
			const fired = await extrasOf(inner).transmitReaction?.(
				channelId,
				ts,
				name,
				action,
			);
			return fired ?? { success: false, error: "reactions lane not available" };
		},
		transmitDelete: async (channelId, messageId) => {
			const del = extrasOf(inner).transmitDelete;
			if (del === undefined) {
				return { success: false, error: "chat.delete not available" };
			}
			return del(channelId, messageId);
		},
	};
	// auth.test forwards ONLY when the inner plane binds the lane — a
	// synthesized ok:false would fail every bare-wire connect loudly.
	const innerAuthTest = extrasOf(inner).authTest;
	if (innerAuthTest !== undefined) {
		plane.authTest = innerAuthTest;
	}
	return plane;
}

/** Transient chat.update transport classes keep the message id retryable. */
export function isTransientTransportError(errorText: string): boolean {
	const s = (errorText ?? "").toLowerCase();
	if (s.includes("ssl") || s.includes("certificate")) return false;
	return s.includes("timeout") || s.includes("connection");
}

export class SlackAdapter extends PersistentWsAdapter {
	private readonly rateGate: RateBudgetGate;
	private readonly richBlocks: boolean;
	private readonly workspaceId: string;
	/**
	 * Bot user id driving the self/echo filter. Constructor seed is the
	 * injected deps.botUserId (or 'bot-self'); a successful connect-time
	 * auth.test RE-RESOLVES it from the token (adapter.py:connect :1968 —
	 * "this picks up the current token's identity even on reconnect") unless
	 * the identity was explicitly injected.
	 */
	private selfUserId: string;
	/** True ⇔ deps.botUserId was injected (explicit injection wins over auth). */
	private readonly botIdentityInjected: boolean;
	/** Primary workspace team scope from auth.test (`team_id`). */
	private primaryTeamId: string | null = null;
	/** Channel→team map feeding stream START recipient_team_id (_channel_team). */
	private readonly channelTeamIds = new Map<string, string>();
	private readonly slackTransport: WsConnectionFactory;

	/** Workspace-scoped Socket-Mode redelivery dedup (#4777, TTL 3600s). */
	private readonly slackDedup: EventDeduplicator;

	/** Session keys derived per inbound event (thread_ts mapping audit). */
	readonly sessionKeysSeen: string[] = [];
	/** Retry-flagged envelopes observed (replay×dedup interplay audit). */
	readonly redeliveryLog: Array<{
		id: string;
		retryAttempt: number;
		reason?: string | undefined;
	}> = [];
	/** One entry per interactivity payload handled (ack-window audit). */
	readonly interactiveAudit: Array<{
		actionIds: readonly string[];
		acked: boolean;
		ackedWithinMs: number;
		handlerError?: string | undefined;
	}> = [];

	/** Workspace-scoped resolved markers keyed `${team}:${msgTs}` (bounded). */
	private readonly resolvedMarkers = new Map<string, boolean>();

	/** Block-rejection retries — LOCAL audit only, never wire flags (slack-5). */
	readonly blockRetryAudit = { droppedOnRetries: 0, clearedOnRetries: 0 };

	/** conversations.open resolutions served from cache (audit surface). */
	dmResolutionCacheHits = 0;

	/** auth.test probes issued per connect (identity-lane audit). */
	readonly authProbes: Array<{
		ok: boolean;
		userId?: string | undefined;
		teamId?: string | undefined;
	}> = [];

	/** Per-turn emoji lifecycle audit (👀→✅/❌ swap, adapter.py :4252-4284). */
	readonly reactionAudit: Array<{
		phase: "start" | "complete";
		messageTs: string;
		outcome?: "success" | "failure" | undefined;
	}> = [];

	private slackLadderInst: FormattingLadder | null = null;
	private slackLadderChatId = "";

	constructor(deps: SlackAdapterDeps) {
		const gate = new RateBudgetGate(
			deps.rateBudget ?? SLACK_MANIFEST.rateBudget,
			deps.clock.nowMs,
		);
		const hooks: SlackRenderHooks = {
			richBlocksEnabled: () => false,
			onBlocksDroppedOnRetry: () => {},
			onBlocksClearedOnRetry: () => {},
			recipientTeamFor: () => undefined,
		};
		const rawTransport: WsConnectionFactory = deps.transport;
		const selfRef: { target: SlackAdapter | null } = { target: null };

		// Shape delta: intercept INTERACTIVITY envelopes at the socket seam and
		// route them to THE one handler (slack_bolt in-process dispatch parity)
		// while every other frame flows into the inherited engine untouched.
		// EVERY events_api envelope is ACKED on receipt with its envelope_id
		// (adapter.py:_start_socket_mode_handler — slack_bolt's SocketModeClient
		// answers each envelope within the 3-second window, before/independent
		// of processing); the durable replay cursor stays engine bookkeeping.
		const interactiveFactory: WsConnectionFactory = {
			connect(listener: WsSocketListener): WsClientSocket {
				const socket = rawTransport.connect({
					onOpen: () => listener.onOpen(),
					onFrame: (frame: WsFrame) => {
						if (frame["type"] === "interactive") {
							void selfRef.target?.handleInteractivePayload(
								frame["action"] as SlackInteractivePayload,
							);
							return;
						}
						if (frame["type"] === "event") {
							const envelopeId = (
								frame["event"] as { envelopeId?: unknown } | undefined
							)?.envelopeId;
							if (typeof envelopeId === "string" && envelopeId !== "") {
								try {
									socket.send({ type: "ack", envelope_id: envelopeId });
								} catch {
									/* non-open socket — close path handles redelivery */
								}
							}
						}
						listener.onFrame(frame);
					},
					onClose: (info) => listener.onClose(info),
					onError: (err: Error) => listener.onError(err),
				});
				return socket;
			},
		};

		super({
			...deps,
			transport: interactiveFactory,
			rest: makeSlackRestPlane(deps.rest, gate, hooks),
			scalarMaxUnits: deps.scalarMaxUnits ?? 39_000,
			capabilities: mergeCapabilities(deps.capabilities),
			dedupTtlMs: deps.dedupTtlMs ?? SLACK_DEDUP_TTL_MS,
		});

		this.rateGate = gate;
		this.richBlocks = deps.richBlocks === true;
		this.reactionsOverride = deps.reactionsEnabled;
		this.workspaceId = deps.workspaceId ?? "W0";
		this.selfUserId = deps.botUserId ?? "bot-self";
		this.botIdentityInjected = deps.botUserId !== undefined;
		this.slackTransport = rawTransport;
		hooks.richBlocksEnabled = () => this.richBlocks;
		hooks.onBlocksDroppedOnRetry = () => {
			this.blockRetryAudit.droppedOnRetries += 1;
		};
		hooks.onBlocksClearedOnRetry = () => {
			this.blockRetryAudit.clearedOnRetries += 1;
		};
		hooks.recipientTeamFor = (chatId) =>
			this.channelTeamIds.get(chatId) ?? this.primaryTeamId ?? undefined;
		selfRef.target = this;
		this.slackDedup = new EventDeduplicator({
			ttlMs: deps.dedupTtlMs ?? SLACK_DEDUP_TTL_MS,
			nowMs: deps.clock.nowMs,
		});

		// Required-secret enablement beyond the base default: BOTH manifest
		// tokens gate loudly (plugin.yaml requires_env transcription).
		if (deps.requiresEnv && deps.secretReader) {
			const enablement = resolveEnablement(
				{
					name: this.manifestName,
					description: "slack adapter",
					transportShape: "ws",
					requiresEnv: deps.requiresEnv,
					capabilities: {},
				},
				deps.secretReader,
			);
			if (!enablement.enabled && enablement.reason) {
				this.lifecycle.disable(enablement.reason);
			} else {
				for (const spec of deps.requiresEnv as readonly EnvVarSpec[]) {
					const value = (deps.secretReader as ScopedSecretReader)(spec.name);
					if (value !== undefined) this.registerLogSecret(value);
				}
			}
		}
	}

	get rateBudgetInUse(): RateBudget | undefined {
		return SLACK_MANIFEST.rateBudget;
	}

	/** Token-resolved bot identity probe (auth.test lane audit). */
	get resolvedSelfUserId(): string {
		return this.selfUserId;
	}

	/** Workspace team scope resolved at connect (null before auth.test). */
	get primaryTeamScope(): string | null {
		return this.primaryTeamId;
	}

	/**
	 * Redelivery suppressions counted against the WORKSPACE-scoped
	 * Socket-Mode dedup window (the engine-level counter stays idle — this
	 * adapter owns its own #4777 window).
	 */
	override get dedupSuppressedCount(): number {
		return this.slackDedup.suppressedCount;
	}

	gateSnapshot(op: Parameters<RateBudgetGate["usedInCurrentWindow"]>[0]) {
		return this.rateGate.usedInCurrentWindow(op);
	}

	// ── lifecycle ──────────────────────────────────────────────────────────

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		// Registered action handlers drain AT CONNECT (plugins.py parity).
		this.actionRegistry.drainAtConnect();
		// auth.test BEFORE the socket comes up (adapter.py:connect :1968 —
		// each token's identity/team scope resolves during connect setup); an
		// explicit auth failure fails the connect loudly (the reconnect ladder
		// covers retrying it).
		if (!(await this.resolveAuthScope())) return false;
		return super.connect(_opts);
	}

	/**
	 * adapter.py:connect :1968 parity — auth.test at connect resolving
	 * bot_user_id + team_id FROM THE TOKEN. Feeds the self/echo filter
	 * (selfUserId), the workspace team scope (primaryTeamId) and the stream-
	 * START recipient_team_id map. Unbound lane ⇒ no-op (identity stays the
	 * injected/default seed).
	 */
	private async resolveAuthScope(): Promise<boolean> {
		const probe = extrasOf(this.rest).authTest;
		if (probe === undefined) return true;
		let identity: SlackAuthIdentity;
		try {
			identity = await probe();
		} catch (err) {
			this.authProbes.push({ ok: false });
			this.logger?.error?.(
				`${this.manifestName}: auth.test failed: ${err instanceof Error ? err.message : String(err)}`,
			);
			return false;
		}
		this.authProbes.push({
			ok: identity.ok === true,
			...(identity.userId !== undefined ? { userId: identity.userId } : {}),
			...(identity.teamId !== undefined ? { teamId: identity.teamId } : {}),
		});
		if (identity.ok !== true) {
			this.logger?.error?.(
				`${this.manifestName}: auth.test rejected the token${identity.error ? `: ${identity.error}` : ""}`,
			);
			return false;
		}
		if (
			!this.botIdentityInjected &&
			typeof identity.userId === "string" &&
			identity.userId !== ""
		) {
			// Token wins over the 'bot-self' seed; explicit injection stays.
			this.selfUserId = identity.userId;
		}
		if (typeof identity.teamId === "string" && identity.teamId !== "") {
			this.primaryTeamId = identity.teamId;
		}
		return true;
	}

	// ── ingress: socket-mode event pipeline (shape delta) ──────────────────

	/** Bounded original-ts → processed-at record (:985/:6855). */
	/** Claim bookkeeping for routed-original-ts (:6855); value = claim time.
	 */
	private readonly processedMessageTs = new Map<string, number>();

	/** Fresh claims indexed by ENVELOPE id so the guard's asynchronous
	 * onTurnFailure hook can unwind exactly the failed invocation even though
	 * turns run inside a spawned frame (pi containment parity of upstream
	 * adapter.py:_handle_slack_message thin-wrapper, 39a5838f0). Same bound
	 * as the ts map. */
	private readonly freshClaimByMessageId = new Map<string, string>();

	/** 👀→✅/❌ swap anchor: message ts → channel that received 👀. */
	private readonly pendingReactions = new Map<string, string>();

	/** Per-turn emoji names (adapter.py :4263/:4280/:4282/:4284). */
	static readonly REACTION_PROCESSING = "eyes";
	static readonly REACTION_SUCCESS = "white_check_mark";
	static readonly REACTION_FAILURE = "x";

	private readonly reactionsOverride: boolean | undefined;

	/** SLACK_REACTIONS gate (:4250 — env default true; config fold override). */
	private reactionsEnabled(): boolean {
		if (this.reactionsOverride !== undefined) return this.reactionsOverride;
		return isSlackReactionsEnabled(process.env["SLACK_REACTIONS"]);
	}

	/** _remember_channel_team — bounded channel→team memory (:1199). */
	private rememberChannelTeam(channelId: string, teamId: string): void {
		if (teamId === "") return;
		this.channelTeamIds.set(channelId, teamId);
		while (this.channelTeamIds.size > SLACK_RESOLVED_MAP_MAX) {
			const oldest = this.channelTeamIds.keys().next();
			if (oldest.done) break;
			this.channelTeamIds.delete(oldest.value);
		}
	}

	/** Record a routed original ts — bounded 5000, oldest discarded (:6855). */
	private markProcessedMessageTs(ts: string): void {
		if (ts === "") return;
		this.processedMessageTs.set(ts, Date.now());
		while (this.processedMessageTs.size > SLACK_PROCESSED_MESSAGE_TS_MAX) {
			const oldest = this.processedMessageTs.keys().next();
			if (oldest.done) break;
			this.processedMessageTs.delete(oldest.value);
		}
	}

	/** Remember (envelope → original-ts) so a later asynchronous turn failure
	 * can unwind THIS invocation's claim via onGuardTurnFailure. Entries live
	 * until their single outcome fires or LRU-trim bounds them (dedup means
	 * one envelope has at most ONE turn outcome, so surviving success-marker
	 * entries are inert). */
	private trackFreshClaim(ts: string, messageId: string): void {
		this.markProcessedMessageTs(ts);
		if (ts !== "" && messageId !== "") {
			this.freshClaimByMessageId.set(messageId, ts);
		}
		while (this.freshClaimByMessageId.size > SLACK_PROCESSED_MESSAGE_TS_MAX) {
			const oldest = this.freshClaimByMessageId.keys().next();
			if (oldest.done) break;
			this.freshClaimByMessageId.delete(oldest.value);
		}
	}

	/** Failure unwind for ONE invoked claim; pre-existing claims untouched. */
	private noteFailedInvocation(messageId: string): void {
		const originalTs = this.freshClaimByMessageId.get(messageId);
		if (originalTs === undefined) return;
		this.freshClaimByMessageId.delete(messageId);
		if (this.processedMessageTs.delete(originalTs)) {
			this.logger?.warn?.(
				`${this.manifestName}: turn failed after claiming ts=${originalTs}; claim released so a retry or edit can re-drive`,
			);
		}
	}

	/** Guard seam (AdapterSessionGuard opts.onTurnFailure): turn RAISED or was
	 * CANCELLED (cancellation is BaseException territory for upstream's
	 * except-clause) — release the failed invocation's fresh claim. */
	onGuardTurnFailure(event: IncomingEvent): void {
		this.noteFailedInvocation(String(event.messageId ?? ""));
	}

	/**
	 * Slack-shaped event pipeline: message_changed normalization (incl. the
	 * already-addressed-original guard :5779) → deletions → redelivery flags →
	 * WORKSPACE-SCOPED dedup (:5797 order — BEFORE the sender filters; EDITED
	 * messages key under their CHANGED-EVENT ts, never the original message
	 * identity) → self/echo → allow_bots → channel→team memory → thread_ts-
	 * keyed session derivation → guards → cursor advance (= the durable
	 * Socket-Mode ack point). The per-turn emoji lifecycle (👀 at processing
	 * start; ✅/❌ swap at completion) rides AROUND dispatch. Dispatch failures
	 * are contained and leave the cursor unmoved (healthy replay covers them).
	 */
	override async handlePlatformEvent(evt: WsPlatformEvent): Promise<void> {
		let env = evt as WsPlatformEvent & {
			retryAttempt?: number | undefined;
			retryReason?: string | undefined;
			ts?: string | undefined;
			threadTs?: string | undefined;
			subtype?: string | undefined;
			botId?: string | undefined;
			teamId?: string | undefined;
			eventTs?: string | undefined;
			message?:
				| {
						ts: string;
						user?: string | undefined;
						text?: string | undefined;
						threadTs?: string | undefined;
						botId?: string | undefined;
						edited?: { ts?: string | undefined } | undefined;
				  }
				| undefined;
		};

		// message_changed normalization FIRST (_handle_slack_message :5773):
		// the nested edited message REPLACES the event payload; channel/team
		// keys inherit from the outer envelope. Dedup keys on the CHANGED-event
		// ts ladder — event_ts → edited.ts → outer ts (≠ original) →
		// `${original}:changed` — so redelivered edits suppress exactly once
		// while distinct edits stay distinct events.
		let changedEventTs: string | null = null;
		if (env.subtype === "message_changed") {
			const updated = env.message;
			if (!updated || typeof updated.ts !== "string" || updated.ts === "") {
				return; // malformed envelope — nothing to re-process
			}
			const originalTs = updated.ts;
			// Already-addressed originals never re-trigger (:5779 — "avoid
			// duplicate responses when an already-addressed message is edited").
			if (this.processedMessageTs.has(originalTs)) return;
			const editedTs =
				typeof updated.edited?.ts === "string" ? updated.edited.ts : "";
			const outerTs = typeof env.ts === "string" ? env.ts : "";
			changedEventTs = env.eventTs ?? editedTs ?? "";
			if (changedEventTs === "" && outerTs !== "" && outerTs !== originalTs) {
				changedEventTs = outerTs;
			}
			if (changedEventTs === "") changedEventTs = `${originalTs}:changed`;
			env = {
				...evt,
				text: updated.text ?? "",
				userId: updated.user ?? "",
				ts: originalTs,
				threadTs: updated.threadTs ?? env.threadTs,
				botId: updated.botId ?? env.botId,
				subtype: undefined, // normalized — downstream filters see a plain message
			};
		}

		if (env.subtype === "message_deleted") return; // deletions ignored
		if ((env.retryAttempt ?? 0) > 0) {
			this.redeliveryLog.push({
				id: evt.id,
				retryAttempt: env.retryAttempt ?? 0,
				...(env.retryReason !== undefined ? { reason: env.retryReason } : {}),
			});
		}
		// Dedup BEFORE the sender filters (:5797 Hermes order — the changed-ts
		// window sees every delivered envelope, filtered or not).
		const teamScope = env.teamId ?? this.workspaceId;
		if (this.slackDedup.isDuplicate(`${teamScope}:${changedEventTs ?? evt.id}`))
			return;
		if (env.userId === this.selfUserId) return; // self/echo filter (§8)
		if (typeof env.botId === "string" && env.botId.length > 0) {
			// allow_bots="none" (default): bot/app-authored messages dropped.
			return;
		}
		// Channel→team memory feeds stream START recipient_team_id (:4974/
		// :6005-6014 _remember_channel_team call sites).
		if (typeof env.teamId === "string") {
			this.rememberChannelTeam(String(env.chatId), env.teamId);
		}
		// thread_ts mapping onto session threading: thread replies key under
		// their root; top-level messages synthesize their own ts as the thread
		// root (reply_in_thread default parity). Edited messages key under the
		// ORIGINAL ts — same session/thread as the first turn.
		const threadRoot = env.threadTs ?? env.ts ?? evt.id;
		const chatType = String(env.chatId).startsWith("D") ? "dm" : "channel";
		const sessionKey = `${this.manifestName}:${String(env.chatId)}:${threadRoot}`;
		// Reactions anchor at the TRIGGERING message's own ts (message_id=ts
		// parity; on_processing_start :4256-4263).
		const reactionAnchor = env.ts ?? evt.id;
		// Routed-original-ts bookkeeping lands BEFORE dispatch (:6855 —
		// even a failed turn marks its message addressed). Failure-path
		// guard (adapter.py:_handle_slack_message thin wrapper): if THIS
		// invocation freshly claimed the ts and the turn later RAISES or
		// is CANCELLED, onGuardTurnFailure releases it — a held-by-a-
		// failed-invocation claim would permanently swallow the message
		// (neither a Slack retry nor a user edit could re-drive it).
		// Pre-existing claims from an earlier successful turn are never
		// released.
		const claimTs = typeof env.ts === "string" && env.ts !== "" ? env.ts : null;
		try {
			this.sessionKeysSeen.push(sessionKey);
			await this.onProcessingStart(String(env.chatId), reactionAnchor);
			// Turn-start processing indicator (adapter.py:send_typing —
			// assistant.threads.setStatus "is thinking..."); cleared at finalize.
			await this.sendTyping(String(env.chatId), { thread_id: threadRoot });
			if (claimTs !== null) this.trackFreshClaim(claimTs, evt.id);
			await this.dispatchIncoming(
				{
					messageId: evt.id,
					messageType: "text",
					text: env.text,
					source: {
						platform: this.manifestName,
						chatType,
						userId: env.userId,
						chatId: String(env.chatId),
					},
				},
				sessionKey,
			);
		} catch (err) {
			// Synchronous dispatch failures unwind through the same rule as the
			// asynchronous guard hook: only THIS invocation's claim goes.
			this.noteFailedInvocation(evt.id);
			this.logger?.error?.(
				`${this.manifestName}: dispatch failed for ${evt.id}: ${err instanceof Error ? err.message : String(err)}`,
			);
			// ❌ failure swap (:4284).
			await this.onProcessingComplete(reactionAnchor, "failure");
			await this.stopTyping(String(env.chatId), { thread_id: threadRoot });
			return; // cursor NOT advanced — replay window still covers this
		}
		this.inboundLog.push(env);
		this.cursor.advance(evt.id); // THE Socket-Mode ack point
		// ✅ completion swap (adapter.py:on_processing_complete :4265-4284).
		await this.onProcessingComplete(reactionAnchor, "success");
		// NO index purge here: the spawned turn frame may STILL fail and invoke
		// onGuardTurnFailure afterwards (that is exactly the upstream release
		// window the thin wrapper owns).
		// Turn-finalized: Slack auto-clears the status on a posted reply; the
		// explicit clear covers contained failures and non-reply turns
		// (adapter.py:stop_typing / _clear_thread_status_quietly).
		await this.stopTyping(String(env.chatId), { thread_id: threadRoot });
	}

	protected override async dispatchIncoming(
		event: IncomingEvent,
		sessionKey: string,
	): Promise<void> {
		event.metadata = { ...(event.metadata ?? {}) };
		// Sender identity rides the turn metadata so stream START args can rename
		// it to recipient_user_id whenever known (send_draft :2600/:3201 reads
		// metadata user_id/sender_id).
		if (event.source?.userId !== undefined) {
			event.metadata["user_id"] = event.source.userId;
		}
		await this.handleIngress(event, sessionKey);
	}

	// ── assistant.threads.setStatus (adapter.py:send_typing/stop_typing) ────

	/**
	 * Show the processing indicator on the target thread. Requires a
	 * resolvable thread root — bare channel sends never activate an
	 * assistant thread (#24117 guard). Failures are swallowed (may lack
	 * assistant:write scope).
	 */
	async sendTyping(chatId: string, metadata: Metadata = {}): Promise<void> {
		const threadTs = resolveSlackThreadTs({
			...(metadata as Record<string, unknown>),
			reply_to_message_id:
				(metadata["reply_to_message_id"] as string | undefined) ??
				(metadata["message_id"] as string | undefined),
		});
		if (threadTs === undefined) return;
		try {
			await extrasOf(this.rest).transmitThreadStatus?.(
				chatId,
				threadTs,
				"is thinking...",
			);
		} catch {
			/* scope/context failures ignored — indicator is best-effort */
		}
	}

	/** Clear the processing indicator (status="" — stop_typing parity). */
	async stopTyping(chatId: string, metadata: Metadata = {}): Promise<void> {
		const threadTs = resolveSlackThreadTs({
			...(metadata as Record<string, unknown>),
			reply_to_message_id:
				(metadata["reply_to_message_id"] as string | undefined) ??
				(metadata["message_id"] as string | undefined),
		});
		if (threadTs === undefined) return;
		try {
			await extrasOf(this.rest).transmitThreadStatus?.(chatId, threadTs, "");
		} catch {
			/* swallowed */
		}
	}

	// ── per-turn emoji lifecycle (adapter.py:on_processing_start/complete) ──

	/**
	 * 👀 on dispatch → removed at completion, then ✅ (success) or ❌
	 * (failure) (:4252-4284). Gated by SLACK_REACTIONS (default true);
	 * reaction failures never break processing (:4227 debug-only parity).
	 */
	async onProcessingStart(channelId: string, messageTs: string): Promise<void> {
		if (!this.reactionsEnabled()) return;
		if (!channelId || !messageTs) return;
		try {
			const fired = await extrasOf(this.rest).transmitReaction?.(
				channelId,
				messageTs,
				SlackAdapter.REACTION_PROCESSING,
				"add",
			);
			if (fired?.success) this.pendingReactions.set(messageTs, channelId);
			this.reactionAudit.push({ phase: "start", messageTs });
		} catch {
			/* reaction failures never break processing */
		}
	}

	/** Completion swap: remove 👀, then ✅/❌ (:4265-4284). */
	async onProcessingComplete(
		messageTs: string,
		outcome: "success" | "failure",
	): Promise<void> {
		const channelId = this.pendingReactions.get(messageTs);
		if (!this.reactionsEnabled()) return;
		this.pendingReactions.delete(messageTs);
		if (channelId === undefined) return;
		try {
			await extrasOf(this.rest).transmitReaction?.(
				channelId,
				messageTs,
				SlackAdapter.REACTION_PROCESSING,
				"remove",
			);
		} catch {
			/* swallowed */
		}
		try {
			await extrasOf(this.rest).transmitReaction?.(
				channelId,
				messageTs,
				outcome === "success"
					? SlackAdapter.REACTION_SUCCESS
					: SlackAdapter.REACTION_FAILURE,
				"add",
			);
		} catch {
			/* swallowed */
		}
		this.reactionAudit.push({
			phase: "complete",
			messageTs,
			outcome,
		});
	}

	// ── chat.delete — opt-in cleanup_progress lane (:3085/run.py:28572) ────

	/**
	 * Delete a previously sent bot message (chat.delete {channel,ts}).
	 * CLASS-LEVEL method on purpose: run.py:28580 probes
	 * `getattr(type(adapter), "delete_message")` to arm the opt-in
	 * display.platforms.<platform>.cleanup_progress cleanup — method presence
	 * IS the capability; the host config flag decides usage. Best-effort:
	 * failures return false and never throw (:3099 best-effort parity).
	 */
	async deleteMessage(chatId: string, messageId: string): Promise<boolean> {
		try {
			const result = await extrasOf(this.rest).transmitDelete?.(
				chatId,
				messageId,
			);
			return result?.success === true;
		} catch {
			return false;
		}
	}

	// ── egress: dual-path delivery (own ladder — single conversion point) ───

	private ensureSlackLadder(): FormattingLadder {
		if (this.slackLadderInst === null) {
			const textSend = (
				content: string,
				metadata: Metadata,
			): Promise<SendResult> =>
				// Text/UI sends ALWAYS carry the suppression flag (DEC-034(iii)
				// parity with the engine's restSendWithLinkPreviewPolicy).
				this.rest.transmitSend(this.slackLadderChatId, content, {
					link_preview_suppressed: true,
					...metadata,
				});
			const transports: FormattingTransport = {
				tryRich: (content, metadata) => this.wireRich(content, metadata),
				sendConverted: (content, metadata) =>
					// RAW bytes handed to the REST boundary — THE wrapper converts
					// exactly once (conversion belongs to the PATH, §10.2).
					textSend(content, metadata),
				sendPlain: (content, metadata) => textSend(content, metadata),
			};
			this.slackLadderInst = new FormattingLadder(transports, {
				log: (m, meta) => this.logger?.warn?.(m, meta),
			});
		}
		return this.slackLadderInst;
	}

	/**
	 * Native-splitting delivery with the Slack send() obligations layered on:
	 * blocks attach ONLY to single-chunk messages (a >budget response is
	 * pathological for Block Kit caps), and the mrkdwn fallback text ALWAYS
	 * rides along as the accessible/notification field.
	 */
	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		const policy = this.chatLengthPolicyForChat(chatId);
		const plan = chunkWithFenceCarry(content, policy);
		const single = plan.chunks.length === 1;
		const results: SendResult[] = [];
		for (let i = 0; i < plan.chunks.length; i++) {
			const chunk = plan.chunks[i]!;
			let chunkMd = metadata;
			if (i === 0 && single && this.richBlocks) {
				const blocks = buildContentBlocks(content);
				if (blocks !== null) {
					chunkMd = { ...metadata, _slack_blocks: blocks };
				}
			}
			this.slackLadderChatId = chatId;
			results.push(await this.deliverSlackChunk(chatId, chunk, chunkMd));
		}
		return results;
	}

	/** Per-chunk §6.1 lanes (mirrors the engine's deliverWiredChunk). */
	private async deliverSlackChunk(
		chatId: string,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const outcome = await this.ensureSlackLadder().sendText(chunk, metadata);
		if (outcome.success) return outcome;
		// Transient RICH failures are NEVER legacy-resent (§10.1 duplicate risk).
		if (outcome.tier === "rich") return outcome;

		const failureClass = classifySendError(new Error(outcome.error ?? ""));
		const networkClassified =
			outcome.retryable === true ||
			failureClass === "connect-timeout" ||
			failureClass === "network" ||
			failureClass === "flood";
		if (networkClassified) {
			if (outcome.retryAfter != null) {
				this.lastCapturedRetryAfterSeconds = outcome.retryAfter;
			}
			const retried = await sendWithRetry(
				chunk,
				metadata,
				(c: string, md: Metadata) => this.rest.transmitSend(chatId, c, md),
				{ maxRetries: 2 },
			);
			if (retried.success) return retried;
			return this.rest.transmitSend(chatId, DELIVERY_FAILED_NOTICE, {
				link_preview_suppressed: true,
				...metadata,
			});
		}
		if (failureClass === "formatting") {
			return this.rest.transmitSend(chatId, plainTextFallbackBody(chunk), {
				link_preview_suppressed: true,
				...metadata,
			});
		}
		return outcome;
	}

	/**
	 * chat.update parity: RAW bytes to the REST boundary (which converts +
	 * attaches finalize-only blocks); a TRANSIENT transport failure keeps the
	 * message id retryable so later edits can catch up instead of re-posting.
	 */
	protected override wireEdit(
		chatId: string,
		messageId: string,
		content: string,
		opts: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		return this.rest
			.transmitEdit(chatId, messageId, content, {
				finalize_edit: opts.finalize,
			})
			.then((result) => {
				if (!result.success && isTransientTransportError(result.error ?? "")) {
					return { ...result, retryable: true };
				}
				return result;
			});
	}

	/** Per-chat descriptor inheritance kept; ordinary chats hit the scalar. */

	// ── capability probes ─────────────────────────────────────────────────

	override supportsDraftStreaming(_chatType?: string | undefined): boolean {
		// A23 latch answers first; Slack native streaming works DMs, threads,
		// AND channels (unlike the dm-only reference default).
		if (this.nativeStreamLatch.unsupported) return false;
		return true;
	}

	// ── Block Kit cards + the ONE interactivity handler (§9.2) ─────────────

	/**
	 * Card-FIRST exec-approval delivery (class-level method — the prototype
	 * declaration is what enables the bridge's card path). Renders through
	 * buildExecApprovalCard, arms the pending store + workspace resolved
	 * marker, ships the mrkdwn fallback alongside the blocks.
	 */
	async sendExecApproval(
		args: ExecApprovalSendArgs,
	): Promise<ApprovalSendResult> {
		this.throwIfDisabled();
		const card = buildExecApprovalCard(args);
		const md: Metadata = { _slack_render: "as-is" };
		if (card.blocks !== undefined) md["_slack_blocks"] = card.blocks;
		const result = await this.rest.transmitSend(
			args.chatId,
			card.mrkdwnText,
			md,
		);
		if (result.success && typeof result.messageId === "string") {
			this.approvals.register(args.approvalId, args.sessionKey);
			this.setResolvedMarker(result.messageId, false);
		}
		return {
			success: result.success,
			messageId: result.messageId ?? null,
			error: result.error ?? null,
		};
	}

	/** Clarify card: indexed choices + free-text flip button (§9.2). */
	async sendClarifyCard(opts: {
		chatId: string;
		question: string;
		choices: readonly string[];
		clarifyId: number;
		sessionKey: string;
	}): Promise<ApprovalSendResult> {
		this.throwIfDisabled();
		const card = buildClarifyCard({
			question: opts.question,
			choices: opts.choices,
			clarifyId: opts.clarifyId,
		});
		const md: Metadata = { _slack_render: "as-is" };
		if (card.blocks !== undefined) md["_slack_blocks"] = card.blocks;
		const result = await this.rest.transmitSend(
			opts.chatId,
			card.mrkdwnText,
			md,
		);
		if (result.success && typeof result.messageId === "string") {
			this.clarify.register(opts.clarifyId, opts.sessionKey);
			this.setResolvedMarker(result.messageId, false);
		}
		return {
			success: result.success,
			messageId: result.messageId ?? null,
			error: result.error ?? null,
		};
	}

	/** Slash-confirmation card (hermes_confirm_* family). */
	async sendSlashConfirmCard(opts: {
		chatId: string;
		promptText: string;
		confirmId: number;
		sessionKey: string;
	}): Promise<ApprovalSendResult> {
		this.throwIfDisabled();
		const card = buildSlashConfirmCard({
			promptText: opts.promptText,
			confirmId: opts.confirmId,
		});
		const md: Metadata = { _slack_render: "as-is" };
		if (card.blocks !== undefined) md["_slack_blocks"] = card.blocks;
		const result = await this.rest.transmitSend(
			opts.chatId,
			card.mrkdwnText,
			md,
		);
		if (result.success && typeof result.messageId === "string") {
			this.slashConfirms.register(opts.confirmId, opts.sessionKey);
			this.setResolvedMarker(result.messageId, false);
		}
		return {
			success: result.success,
			messageId: result.messageId ?? null,
			error: result.error ?? null,
		};
	}

	/**
	 * THE one interactivity handler (§9.2: a single registered handler routes
	 * ALL senders). Ack discipline: EVERY payload acks inside the 3-second
	 * window even when routing raises (plugin-wrapper parity). Taps map onto
	 * THE namespaced callback grammar and resolve through the SAME stores and
	 * gateway-side resolvers as the button platforms (DEC-016 parallel
	 * mechanism, not a divergent grammar).
	 */
	async handleInteractivePayload(
		payload: SlackInteractivePayload,
	): Promise<void> {
		const started = this.clock.nowMs();
		const ctx: InteractiveTapContext = {
			userId: payload.user?.id ?? "",
			channelId: payload.channel?.id ?? "",
			msgTs: payload.message?.ts ?? "",
			teamId: payload.team?.id ?? this.workspaceId,
			...(payload.message?.blocks !== undefined
				? { messageBlocks: payload.message.blocks }
				: {}),
		};
		const actions = payload.actions ?? [];
		let handlerError: string | undefined;
		for (const action of actions) {
			try {
				await this.routeInteractiveAction(
					action.action_id,
					action.value ?? "",
					ctx,
				);
			} catch (err) {
				handlerError = err instanceof Error ? err.message : String(err);
			}
		}
		this.interactiveAudit.push({
			actionIds: actions.map((a) => a.action_id),
			acked: true,
			ackedWithinMs: this.clock.nowMs() - started,
			...(handlerError !== undefined ? { handlerError } : {}),
		});
	}

	private async routeInteractiveAction(
		actionId: string,
		value: string,
		ctx: InteractiveTapContext,
	): Promise<void> {
		const isResolutionFamily =
			actionId.startsWith("hermes_approve") ||
			actionId === "hermes_deny" ||
			actionId === SLASH_CONFIRM_ACTION_IDS.once ||
			actionId === SLASH_CONFIRM_ACTION_IDS.always ||
			actionId === SLASH_CONFIRM_ACTION_IDS.cancel ||
			CLARIFY_CHOICE_ACTION_RE.test(actionId) ||
			actionId === "hermes_clarify_other";
		if (isResolutionFamily && ctx.msgTs !== "") {
			// Atomic-pop double-click guard keyed workspace-scoped
			// (_approval_resolved.pop(key, True) parity: absent ⇒ resolved).
			// Identity-less payloads (no host ts) skip the map and rely on the
			// pending-store pops underneath.
			if (this.popResolvedMarker(`${ctx.teamId}:${ctx.msgTs}`)) return;
		}

		let data: string | null = null;
		const approvalChoice = APPROVAL_CHOICE_BY_ACTION_ID.get(actionId);
		if (approvalChoice !== undefined) {
			data = buildExecApprovalCallback(approvalChoice, Number(value));
		} else {
			const confirmChoice = SLASH_CONFIRM_CHOICE_BY_ACTION_ID.get(actionId);
			if (confirmChoice !== undefined) {
				data = buildSlashConfirmCallback(confirmChoice, Number(value));
			} else {
				const clarifyIdx = CLARIFY_CHOICE_ACTION_RE.exec(actionId);
				if (clarifyIdx) {
					const [clarifyIdRaw, idxRaw] = value.split("|");
					data = buildClarifyCallback(
						Number(clarifyIdRaw),
						idxRaw === "other" ? "other" : Number(idxRaw),
					);
				} else if (actionId === "hermes_clarify_other") {
					const [clarifyIdRaw] = value.split("|");
					data = buildClarifyCallback(Number(clarifyIdRaw), "other");
				}
			}
		}
		if (data === null) {
			// Unknown action_id — answered (spinner clears), never dispatched.
			return;
		}
		const answer = await this.router.route(data, {
			userId: ctx.userId,
			...(ctx.channelId !== "" ? { chatId: ctx.channelId } : {}),
		});
		if (answer.kind === "resolved") {
			// Consumed state visible ON the host message: chat.update REPLACES
			// the block layout — section(original text) + context(decision) —
			// which is how buttons disappear (_handle_approval_action parity:
			// updated_blocks = [section(original), context(decision_text)]).
			const decisionText = answer.hostEdit.text.slice(0, 3000);
			await this.rest.transmitEdit(ctx.channelId, ctx.msgTs, decisionText, {
				_slack_render: "as-is",
				_slack_blocks: buildResolvedHostBlocks(
					extractSectionText(ctx.messageBlocks),
					decisionText,
				),
			} as Metadata);
		}
	}

	// ── files_upload_v2 + conversations.open (adapter.py:_upload_file) ─────

	/** Bounded conversations.open cache keyed `${team}:${userId}`. */
	private readonly dmConversationCache = new Map<string, string>();

	/**
	 * Resolve a bare U/W user target to a D… conversation id ONCE per target
	 * (adapter.py:_ensure_dm_conversation — postMessage/files_upload_v2
	 * reject user ids). Non-user targets pass through unchanged.
	 */
	async ensureDmConversation(chatId: string): Promise<string> {
		if (!isDmUserTarget(chatId)) return chatId;
		const cacheKey = `${this.workspaceId}:${chatId}`;
		const cached = this.dmConversationCache.get(cacheKey);
		if (cached !== undefined) {
			this.dmResolutionCacheHits += 1;
			return cached;
		}
		const dmId = await extrasOf(this.rest).openDirectMessage?.(chatId);
		if (dmId !== undefined && dmId !== null && dmId !== "") {
			this.dmConversationCache.set(cacheKey, dmId);
			while (this.dmConversationCache.size > SLACK_RESOLVED_MAP_MAX) {
				const oldest = this.dmConversationCache.keys().next();
				if (oldest.done) break;
				this.dmConversationCache.delete(oldest.value);
			}
			return dmId;
		}
		return chatId; // resolution failed — downstream surfaces the real error
	}

	/**
	 * Media delivery through files_upload_v2 ({channel,file,filename,
	 * initial_comment,thread_ts?}) — adapter.py:_upload_file shape, DM
	 * targets resolved ahead of the upload.
	 */
	async deliverFile(
		chatId: string,
		file: { filename: string },
		opts: {
			caption?: string | undefined;
			replyTo?: string | undefined;
			metadata?: Metadata | undefined;
		} = {},
	): Promise<SendResult> {
		this.throwIfDisabled();
		const channel = await this.ensureDmConversation(chatId);
		const threadTs = resolveSlackThreadTs({
			...(opts.metadata ?? {}),
			...(opts.replyTo !== undefined
				? { reply_to_message_id: opts.replyTo }
				: {}),
		});
		const upload = extrasOf(this.rest).transmitUpload;
		if (upload === undefined) {
			return { success: false, error: "files_upload_v2 not available" };
		}
		try {
			return await upload({
				channel,
				filename: file.filename,
				initialComment: opts.caption ?? "",
				...(threadTs !== undefined ? { threadTs } : {}),
			});
		} catch (err) {
			return {
				success: false,
				error: err instanceof Error ? err.message : String(err),
			};
		}
	}

	// ── resolved-marker bookkeeping (bounded 1000, oldest discarded) ───────

	private setResolvedMarker(messageTs: string, resolved: boolean): void {
		this.resolvedMarkers.set(`${this.workspaceId}:${messageTs}`, resolved);
		while (this.resolvedMarkers.size > SLACK_RESOLVED_MAP_MAX) {
			const oldest = this.resolvedMarkers.keys().next();
			if (oldest.done) break;
			this.resolvedMarkers.delete(oldest.value);
		}
	}

	/** True ⇔ ALREADY RESOLVED (absent counts as resolved — Hermes default). */
	private popResolvedMarker(key: string): boolean {
		const prior = this.resolvedMarkers.get(key);
		if (prior === undefined) return true;
		this.resolvedMarkers.delete(key);
		return prior;
	}

	get resolvedMarkerCount(): number {
		return this.resolvedMarkers.size;
	}

	// ── identity probes ─────────────────────────────────────────────────────

	override buildMissingSecretSibling(): SlackAdapter {
		return new SlackAdapter({
			manifestName: `${this.manifestName}-no-secret`,
			transport: this.slackTransport,
			rest: this.rest,
			clock: this.clock,
			requiresEnv: SLACK_MANIFEST.requiresEnv,
			secretReader: () => undefined,
		});
	}
}

/**
 * Replacement blocks for a consumed card: section(original text) plus
 * context(decision) — buttons live ONLY in actions blocks, so replacing
 * the layout strips them (adapter.py:_handle_approval_action).
 */
function buildResolvedHostBlocks(
	originalText: string,
	decisionText: string,
): unknown[] {
	return [
		{
			type: "section",
			text: {
				type: "mrkdwn",
				text: originalText.slice(0, 3000) || "Command approval request",
			},
		},
		{ type: "context", elements: [{ type: "mrkdwn", text: decisionText }] },
	];
}

/** First section-block text of an interaction payload's message blocks. */
function extractSectionText(blocks: unknown[] | undefined): string {
	for (const block of blocks ?? []) {
		const b = block as { type?: string; text?: { text?: unknown } };
		if (b?.type === "section" && typeof b.text?.text === "string") {
			return b.text.text;
		}
	}
	return "";
}

const APPROVAL_CHOICE_BY_ACTION_ID = new Map(
	Object.entries({
		hermes_approve_once: "once",
		hermes_approve_session: "session",
		hermes_approve_always: "always",
		hermes_deny: "deny",
	}) as Array<[string, "once" | "session" | "always" | "deny"]>,
);

const SLASH_CONFIRM_CHOICE_BY_ACTION_ID = new Map(
	Object.entries({
		hermes_confirm_once: "once",
		hermes_confirm_always: "always",
		hermes_confirm_cancel: "cancel",
	}) as Array<[string, "once" | "always" | "cancel"]>,
);

function mergeCapabilities(
	partial: Partial<CapabilityManifest> | undefined,
): Partial<CapabilityManifest> {
	return {
		splitsLongMessages: true,
		typedCommandPrefix: "!",
		...(partial ?? {}),
	};
}
