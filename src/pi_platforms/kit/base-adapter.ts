// pi_platforms/kit/base-adapter — THE one base platform adapter
// (04-platform-adapters.md §1). Every platform implements it. The base owns:
// guard L1 composition, pending/debounce semantics (via pi_gateway guards),
// chunking, send-retry/flood fallbacks, fatal-error state, and the egress
// doors with seal-interception (via pi_gateway EgressChokepoint — DEC-006).
// Platforms supply transport + formatting + capabilities AS DATA.
//
// Layering: pi_platforms sits ABOVE pi_gateway/pi_agent_core/pi_state
// (01 §5.3) — this file imports downward only. No adapter imports another.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   gateway/platforms/base.py:BasePlatformAdapter.__init__ (guard L1 ownership)
//   base.py:max_message_length_for_chat / message_len_fn_for_chat (§6.3 pair)
//   base.py:_send_with_retry (§6.1 ladder)        → send-retry.ts
//   base.py:truncate_message                      → outbound segmentation + kit chunking
//   base.py:send_draft / edit_message defaults    → StreamEgressAdapter impl
//   base.py:_set_fatal_error                      → AdapterLifecycleState

import {
	type IncomingEvent,
	AdapterSessionGuard,
	type CommandRegistry,
	type MessageHandler,
	type TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { SessionSource } from "../../pi_gateway/resolution/session-key.js";
import type {
	EgressChokepoint,
	DoorTransport,
} from "../../pi_gateway/streaming/egress-door.js";
import type {
	DraftFrameArgs,
	EditOptions,
	Metadata,
	SendResult,
	StreamLogger,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { chunkWithFenceCarry, type ChunkPlan } from "./chunking.js";
import {
	resolveChatLengthPolicy,
	type ChatLengthPolicy,
	type LengthUnit,
} from "./length-policy.js";
import { FormattingLadder } from "./formatting-ladder.js";
import {
	sendWithRetry,
	plainTextFallbackBody,
	classifySendError,
	DELIVERY_FAILED_NOTICE,
} from "./send-retry.js";
import {
	AdapterLifecycleState,
	TokenLockConflictError,
	type DisableReason,
} from "./lifecycle-state.js";
import { SecretRedactor, createRedactingLogger } from "./log-redaction.js";
import type { TokenLockManagerSeam, LockAcquisition } from "./token-lock.js";
import type { CapabilityManifest } from "./capabilities.js";
import { capabilityFlag, DEFAULT_CAPABILITIES } from "./capabilities.js";

export const MAX_MESSAGE_LENGTH_DEFAULT = 4096;

export interface BaseAdapterDeps {
	manifestName: string;
	/** Capability flags are DATA (04 §2). */
	capabilities?: Partial<CapabilityManifest> | undefined;
	/** Adapter-wide length-unit default (message_len_fn property parity). */
	lengthUnit?: LengthUnit | undefined;
	scalarMaxUnits?: number | undefined;
	logger?: StreamLogger | undefined;
}

/**
 * Abstract base. Concrete adapters implement TRANSPORT only; everything
 * policy-shaped lives here or in the kit modules it composes.
 */
export abstract class BasePlatformAdapter {
	readonly manifestName: string;
	readonly lifecycle: AdapterLifecycleState;
	/**
	 * DEC-033: the base wraps EVERY injected logger with redaction, so all
	 * adapters inherit §8 log hygiene (tokens/session keys/secrets never in
	 * emitted lines). Subclasses register resolved secret values through
	 * registerLogSecret; credential SHAPES are scrubbed even unregistered.
	 */
	protected readonly logger: StreamLogger | undefined;
	private readonly logRedactor = new SecretRedactor();
	private readonly caps: Partial<CapabilityManifest>;
	private readonly lengthUnitDefault: LengthUnit;
	private readonly scalarMaxUnits: number;

	// ── guard L1 + owner-task map (base.py __init__ ownership) ──────────────
	/**
	 * Built by the runner wiring (`attachGuard`) once a command registry and
	 * handler exist. Adapters without runner wiring still pass conformance
	 * rows that don't need ingress.
	 */
	protected guard: AdapterSessionGuard | null = null;

	/**
	 * Session-scoped rich-downgrade latch state (§10.1 "probe once per
	 * process"). The FormattingLadder instance itself is per-chunk (its
	 * transport closures bind the CURRENT delivery's chat); this state is
	 * carried in and out of every chunk so the latch still persists across
	 * chunks and sends. Subclasses override wireRich/wireSend.
	 */
	private ladderRichDisabled = false;
	private ladderLatchCount = 0;

	constructor(deps: BaseAdapterDeps) {
		this.manifestName = deps.manifestName;
		this.logger = createRedactingLogger(deps.logger, this.logRedactor);
		this.caps = deps.capabilities ?? {};
		this.lengthUnitDefault = deps.lengthUnit ?? "chars";
		this.scalarMaxUnits = deps.scalarMaxUnits ?? MAX_MESSAGE_LENGTH_DEFAULT;
		this.lifecycle = new AdapterLifecycleState(this.logger);
	}

	/** DEC-033 seam: adapters register resolved secret VALUES post-enablement. */
	protected registerLogSecret(value: string): void {
		this.logRedactor.register(value);
	}

	/** The session redactor (subject/test observability + row probes). */
	get redactor(): SecretRedactor {
		return this.logRedactor;
	}

	// ── capabilities are DATA (getattr-with-default parity) ─────────────────

	get supportsAsyncDelivery(): boolean {
		return capabilityFlag(
			this.caps.supportsAsyncDelivery,
			DEFAULT_CAPABILITIES.supportsAsyncDelivery,
		);
	}
	get splitsLongMessages(): boolean {
		return capabilityFlag(
			this.caps.splitsLongMessages,
			DEFAULT_CAPABILITIES.splitsLongMessages,
		);
	}
	get typedCommandPrefix(): string {
		return (
			this.caps.typedCommandPrefix ?? DEFAULT_CAPABILITIES.typedCommandPrefix
		);
	}
	get interactiveResume(): boolean {
		return capabilityFlag(
			this.caps.interactiveResume,
			DEFAULT_CAPABILITIES.interactiveResume,
		);
	}
	get requiresEditFinalize(): boolean {
		return capabilityFlag(
			this.caps.requiresEditFinalize,
			DEFAULT_CAPABILITIES.requiresEditFinalize,
		);
	}

	/**
	 * DEC-022 wake-lane declaration: push shapes forge internal events through
	 * the normal pipeline; STATELESS shapes (async delivery impossible) take
	 * the raw-key direct-turn lane.
	 */
	get wakeLane(): "forged-event" | "raw-key-direct" {
		return this.supportsAsyncDelivery ? "forged-event" : "raw-key-direct";
	}

	// ── §6.3: THE ONE chat resolution pair ──────────────────────────────────

	/**
	 * Budget AND unit resolve HERE, together, per chat. Subclasses whose chats
	 * differ in cap OR unit override this to consult their descriptor — never
	 * mix a scalar budget with a per-chat unit (A15 obligation).
	 */
	chatLengthPolicyForChat(chatId: string): ChatLengthPolicy {
		return resolveChatLengthPolicy({
			chatId,
			unit: this.lengthUnitDefault,
			descriptor: this.chatDescriptorFor(chatId),
			scalarMaxUnits: this.scalarMaxUnits,
		});
	}

	/** Relay-shaped override point: per-chat descriptor or undefined. */
	protected chatDescriptorFor(_chatId: string):
		| {
				maxMessageLength?: number | undefined;
				lenUnit?: LengthUnit | undefined;
		  }
		| undefined {
		return undefined;
	}

	// ── runner wiring ────────────────────────────────────────────────────────

	attachGuard(
		deps: {
			registry: CommandRegistry;
			messageHandler: MessageHandler;
			sendReply: (chatId: string, text: string) => Promise<void>;
			onTurnFailure?:
				| ((event: IncomingEvent) => void | Promise<void>)
				| undefined;
		},
		opts: {
			spawner?: TaskSpawner | undefined;
			/** Lane C clarify-intercept predicate (§5.3). */
			hasPendingClarify?: ((sessionKey: string) => boolean) | undefined;
			/** Telegram DM topic-recovery hook (run.py:_recover_telegram_topic_thread_id). */
			topicThreadRecovery?:
				| ((source: SessionSource) => string | null | undefined)
				| undefined;
			/** Key rebuilder applied after a recovery rewrite (build_session_key parity). */
			rebuildSessionKey?: ((source: SessionSource) => string) | undefined;
		} = {},
	): void {
		if (this.guard !== null) return;
		this.guard = new AdapterSessionGuard({
			registry: deps.registry,
			messageHandler: async (event, ctx) => {
				this.throwIfDisabled();
				return deps.messageHandler(event, ctx);
			},
			sendReply: deps.sendReply,
			...(deps.onTurnFailure !== undefined
				? { onTurnFailure: deps.onTurnFailure }
				: {}),
			...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
			...(opts.hasPendingClarify !== undefined
				? { hasPendingClarify: opts.hasPendingClarify }
				: {}),
			...(opts.topicThreadRecovery !== undefined
				? { topicThreadRecovery: opts.topicThreadRecovery }
				: {}),
			...(opts.rebuildSessionKey !== undefined
				? { rebuildSessionKey: opts.rebuildSessionKey }
				: {}),
		});
	}

	async handleIngress(event: IncomingEvent, sessionKey: string): Promise<void> {
		this.throwIfDisabled();
		const guard = this.guard;
		if (guard === null) {
			throw new Error(
				`${this.manifestName}: no guard attached — wire the runner first`,
			);
		}
		await guard.handleMessage(event, sessionKey);
	}

	// ── egress doors (ALL text sends route through these) ───────────────────

	/**
	 * DOOR 1. Seal-interception rides the shared audited chokepoint (DEC-006);
	 * the wire transport below supplies bytes-on-the-wire.
	 */
	async send(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
		metadata?: Metadata | undefined,
	): Promise<SendResult> {
		this.throwIfDisabled();
		return this.chokepoint.admit({
			door: "send",
			chatId,
			content,
			replyTo,
			metadata,
		});
	}

	/** DOOR 2 — delivery-resolver lane (finding #7 both-door coverage). */
	async sendForPlatform(
		logicalPlatform: string,
		chatId: string,
		content: string,
		replyTo?: string | undefined,
		metadata?: Metadata | undefined,
	): Promise<SendResult> {
		this.throwIfDisabled();
		return this.chokepoint.admit({
			door: "send_for_platform",
			chatId,
			content,
			replyTo,
			metadata,
			platform: logicalPlatform,
		});
	}

	/** Default edit support is "not supported" (base.py edit_message default). */
	async editMessage(
		chatId: string,
		messageId: string,
		content: string,
		opts?: EditOptions | undefined,
	): Promise<SendResult> {
		return this.wireEdit(chatId, messageId, content, {
			finalize: opts?.finalize === true,
		});
	}

	/** One cumulative native draft frame (draftAdmission gate first). */
	async sendDraft(args: DraftFrameArgs): Promise<SendResult> {
		const verdict = this.chokepoint.draftAdmission(args);
		if (verdict.swallow) return { success: true };
		const result = await this.wireDraft(args);
		if (result.success && verdict.arm) {
			this.chokepoint.armOpenDraft(verdict.key, args.draftId);
		}
		return result;
	}

	/** Per-chat capability probe (METHOD, default false — DEC-006). */
	supportsDraftStreaming(_chatType?: string): boolean {
		return false;
	}

	// ── the full text-delivery pipeline (chunk → ladder → retry) ───────────

	/**
	 * Deliver user-visible text: resolve THE chat length policy once, split
	 * oversized content with fence-carry, then push every chunk through the
	 * formatting ladder wrapped in the §6.1 retry ladder. Formatting-rejected
	 * chunks fall back to the plain-text body; exhausted retries surface the
	 * resend notice.
	 */
	async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		const policy = this.chatLengthPolicyForChat(chatId);
		const plan: ChunkPlan =
			this.splitsLongMessages || policy.lenFn(content) <= policy.maxUnits
				? {
						chunks: [content],
						chunkCount: 1,
						scaffold: [{ prefixLen: 0, closeAdded: false, labelJoinLen: 0 }],
					}
				: chunkWithFenceCarry(content, policy);
		const results: SendResult[] = [];
		for (const chunk of plan.chunks) {
			results.push(await this.deliverChunk(chatId, chunk, metadata));
		}
		return results;
	}

	private async deliverChunk(
		chatId: string,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		// The rich-downgrade latch is SESSION-scoped (§10.1 "probe once per
		// process") — but the transport seams must bind THIS delivery's chat:
		// caching ONE ladder whose closures capture the first chatId misroutes
		// every later cross-chat tier-2/tier-3 send. So: fresh ladder per
		// chunk, latch state carried in and back out (identical observable
		// semantics, correct chat routing).
		const ladder = new FormattingLadder(
			{
				tryRich: (c, md) => this.wireRich(c, md),
				sendConverted: (c, md) => this.wireSend(chatId, c, md),
				sendPlain: (c, md) => this.wireSend(chatId, c, md),
			},
			{
				log: this.logger?.warn
					? (m, meta) => this.logger?.warn?.(m, meta)
					: undefined,
			},
		);
		ladder.richSendDisabled = this.ladderRichDisabled;
		ladder.richLatchCount = this.ladderLatchCount;

		const outcome = await ladder.sendText(chunk, metadata);
		this.ladderRichDisabled = ladder.richSendDisabled;
		this.ladderLatchCount = ladder.richLatchCount;
		if (outcome.success) return outcome;

		// A transient RICH failure is NEVER legacy-resent (§10.1 duplicate
		// risk) — the failed SendResult goes back to the caller as-is.
		if (outcome.tier === "rich") return outcome;

		// Network-classified? ONE shared classifier decides (§6.1:
		// "result.retryable OR retryable error patterns" — a FloodWait carries
		// retry_after without any retryable flag).
		const failureClass = classifySendError(new Error(outcome.error ?? ""));
		const networkClassified =
			outcome.retryable === true ||
			failureClass === "connect-timeout" ||
			failureClass === "network" ||
			failureClass === "flood";
		if (networkClassified) {
			// §6.1 ladder — timeouts NOT retried inside, retry_after honored.
			const retried = await sendWithRetry(
				chunk,
				metadata,
				(c, md) => this.wireSend(chatId, c, md),
				{ maxRetries: 2 },
			);
			if (retried.success) return retried;
			// Exhausted retries → user-facing notice (§6.1).
			return this.wireSend(chatId, DELIVERY_FAILED_NOTICE, metadata);
		}

		// Formatting-classified final failure → plain-text fallback lane.
		if (failureClass === "formatting") {
			return this.wireSend(chatId, plainTextFallbackBody(chunk), metadata);
		}
		return outcome;
	}

	// ── fatal/disable surfaces ───────────────────────────────────────────────

	/**
	 * Unique-credential acquisition (06 §5 preview): synchronous tuple-returning
	 * acquisition; named-holder refusal becomes a FATAL adapter error.
	 */
	acquireCredentialLock(
		manager: TokenLockManagerSeam,
		scope: string,
		credentialId: string,
		owner: string,
	): LockAcquisition {
		const acquisition = manager.tryAcquire(scope, credentialId, owner);
		if (!acquisition.acquired) {
			const reason: DisableReason = {
				kind: "token_lock_conflict",
				scope,
				credentialId,
				holder: acquisition.holder.owner,
			};
			this.lifecycle.markFatal(reason);
			throw new TokenLockConflictError(
				scope,
				credentialId,
				acquisition.holder.owner,
			);
		}
		return acquisition;
	}

	protected throwIfDisabled(): void {
		const s = this.lifecycle.state;
		if (s === "disabled" || s === "fatal") {
			throw new Error(
				`${this.manifestName} is ${s}: ${this.lifecycle.statusSnapshot().detail}`,
			);
		}
	}

	// ── transport contract (platforms implement these) ───────────────────────

	protected abstract get chokepoint(): EgressChokepoint;

	protected abstract wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult>;

	protected wireEdit(
		_chatId: string,
		_messageId: string,
		_content: string,
		_opts: { finalize: boolean },
	): Promise<SendResult> {
		return Promise.resolve({ success: false, error: "Not supported" });
	}

	protected wireDraft(_args: DraftFrameArgs): Promise<SendResult> {
		return Promise.resolve({ success: false, error: "Not supported" });
	}

	protected wireRich(
		_content: string,
		_metadata: Metadata,
	): Promise<SendResult> {
		// Default: no rich endpoint (capability error shape ⇒ latch path).
		return Promise.resolve({ success: false, error: "method not found" });
	}

	abstract connect(opts: { isReconnect: boolean }): Promise<boolean>;
	abstract disconnect(): Promise<void>;
}

/** Re-export for adapter authors building DoorTransports against the seam. */
export type { DoorTransport };
