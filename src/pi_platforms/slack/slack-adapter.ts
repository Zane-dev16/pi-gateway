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
//      retry-flagged redeliveries shaping workspace-scoped replay dedup,
//      cursor advance AS the durable ack point (module header of
//      ./fake-socket-mode.ts carries the full mapping table).
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
	StreamLogger,
	EditOptions,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type {
	IncomingEvent,
	TaskSpawner,
	MessageHandler,
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
	type AdapterClock,
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
}

interface InteractiveTapContext {
	userId: string;
	channelId: string;
	msgTs: string;
	teamId: string;
}

/** Late-bound state the pre-super rest wrapper closures read. */
interface SlackRenderHooks {
	richBlocksEnabled(): boolean;
}

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
): RestPlane {
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

	async function transmitSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const decision = gate.check("send");
		if (!decision.admitted) {
			return refused(`rate_limited:${decision.tier}`, decision.retryAfterMs);
		}
		const { blocks, rest } = extractBlocks(metadata);
		const text = needsConversion(content, metadata)
			? convertMarkdownToSlackMrkdwn(content)
			: content;
		const md: Metadata = blocks !== null ? { ...rest, blocks } : rest;
		const sent = await inner.transmitSend(chatId, text, md);
		if (
			!sent.success &&
			blocks !== null &&
			isBlockPayloadRejectionError(sent.error ?? "")
		) {
			// Block Kit is a PROGRESSIVE ENHANCEMENT — retry without blocks so a
			// rendering bug can never drop the response (send() parity).
			const { blocks: _dropped, ...withoutBlocks } = md as Record<
				string,
				unknown
			>;
			return inner.transmitSend(chatId, text, {
				...withoutBlocks,
				blocks_dropped_on_retry: true,
			} as Metadata);
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
			// otherwise the prior block layout survives the edit.
			updated = await inner.transmitEdit(chatId, messageId, text, {
				...rest,
				blocks: [],
				blocks_cleared_on_retry: true,
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
		return inner.transmitDraft(
			chatId,
			draftId,
			content,
			final,
			rest as Metadata,
		);
	}

	return {
		transmitSend,
		transmitEdit,
		transmitDraft,
		transmitRich: (chatId, content, metadata) =>
			// Rich probe lane — no Slack REST analog (Block Kit renders locally);
			// ungated passthrough keeps the shared §10.1 rows meaningful.
			inner.transmitRich(chatId, content, metadata),
		hasScript: (opKind) => inner.hasScript(opKind),
	};
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
	private readonly selfUserId: string;
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

	private readonly hooks: SlackRenderHooks;

	private slackLadderInst: FormattingLadder | null = null;
	private slackLadderChatId = "";

	constructor(deps: SlackAdapterDeps) {
		const gate = new RateBudgetGate(
			deps.rateBudget ?? SLACK_MANIFEST.rateBudget,
			deps.clock.nowMs,
		);
		const hooks: SlackRenderHooks = { richBlocksEnabled: () => false };
		const rawTransport: WsConnectionFactory = deps.transport;
		const selfRef: { target: SlackAdapter | null } = { target: null };

		// Shape delta: intercept INTERACTIVITY envelopes at the socket seam and
		// route them to THE one handler (slack_bolt in-process dispatch parity)
		// while every other frame flows into the inherited engine untouched.
		const interactiveFactory: WsConnectionFactory = {
			connect(listener: WsSocketListener): WsClientSocket {
				return rawTransport.connect({
					onOpen: () => listener.onOpen(),
					onFrame: (frame: WsFrame) => {
						if (frame["type"] === "interactive") {
							void selfRef.target?.handleInteractivePayload(
								frame["action"] as SlackInteractivePayload,
							);
							return;
						}
						listener.onFrame(frame);
					},
					onClose: (info) => listener.onClose(info),
					onError: (err: Error) => listener.onError(err),
				});
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
		this.workspaceId = deps.workspaceId ?? "W0";
		this.selfUserId = deps.botUserId ?? "bot-self";
		this.slackTransport = rawTransport;
		this.hooks = hooks;
		hooks.richBlocksEnabled = () => this.richBlocks;
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
		return super.connect(_opts);
	}

	// ── ingress: socket-mode event pipeline (shape delta) ──────────────────

	/**
	 * Slack-shaped event pipeline: bot/subtype filters → WORKSPACE-SCOPED
	 * dedup (event ids are unique per workspace only — _workspace_event_id
	 * parity) → thread_ts-keyed session derivation → guards → cursor advance
	 * (= the durable Socket-Mode ack point). Dispatch failures are contained
	 * and leave the cursor unmoved (healthy replay covers them).
	 */
	override async handlePlatformEvent(evt: WsPlatformEvent): Promise<void> {
		const env = evt as WsPlatformEvent & {
			retryAttempt?: number | undefined;
			retryReason?: string | undefined;
			ts?: string | undefined;
			threadTs?: string | undefined;
			subtype?: string | undefined;
			botId?: string | undefined;
			teamId?: string | undefined;
		};
		if (env.userId === this.selfUserId) return; // self/echo filter (§8)
		if (env.subtype === "message_deleted") return; // deletions ignored
		if (typeof env.botId === "string" && env.botId.length > 0) {
			// allow_bots="none" (default): bot/app-authored messages dropped.
			return;
		}
		if ((env.retryAttempt ?? 0) > 0) {
			this.redeliveryLog.push({
				id: evt.id,
				retryAttempt: env.retryAttempt ?? 0,
				...(env.retryReason !== undefined ? { reason: env.retryReason } : {}),
			});
		}
		const teamScope = env.teamId ?? this.workspaceId;
		if (this.slackDedup.isDuplicate(`${teamScope}:${evt.id}`)) return;
		try {
			// thread_ts mapping onto session threading: thread replies key under
			// their root; top-level messages synthesize their own ts as the
			// thread root (reply_in_thread default parity).
			const threadRoot = env.threadTs ?? env.ts ?? evt.id;
			const chatType = String(env.chatId).startsWith("D") ? "dm" : "channel";
			const sessionKey = `${this.manifestName}:${String(env.chatId)}:${threadRoot}`;
			this.sessionKeysSeen.push(sessionKey);
			await this.dispatchIncoming(
				{
					messageId: evt.id,
					messageType: "text",
					text: evt.text,
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
			this.logger?.error?.(
				`${this.manifestName}: dispatch failed for ${evt.id}: ${err instanceof Error ? err.message : String(err)}`,
			);
			return; // cursor NOT advanced — replay window still covers this
		}
		this.inboundLog.push(env);
		this.cursor.advance(evt.id); // THE Socket-Mode ack point
	}

	protected override async dispatchIncoming(
		event: IncomingEvent,
		sessionKey: string,
	): Promise<void> {
		event.metadata = { ...(event.metadata ?? {}) };
		await this.handleIngress(event, sessionKey);
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
			// Consumed state visible ON the host message: rewrite with the
			// router's HOST text and the buttons REMOVED (clamped 3000).
			const decisionText = answer.hostEdit.text.slice(0, 3000);
			await this.rest.transmitEdit(ctx.channelId, ctx.msgTs, decisionText, {
				_slack_render: "as-is",
				buttons_removed: true,
			} as Metadata);
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
