// pi_platforms/webhook/webhook-subject — the WEBHOOK reference adapter wired
// as a ConformanceSubject: the 04 §8 shared rows run against the REAL kit-built
// adapter (guards, doors, ladder, callbacks, locks) with FakePlatformWire
// egress capture, plus webhook-shape probes (flags/trust boundary, bounded
// window, SSE lanes).

import {
	type ActionHandlerRegistry,
	TokenLockManagerSeam,
	resolveEnablement,
} from "../kit/index.js";
import type { CallbackQueryRouter } from "../kit/callback-router.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import { WebhookAdapter } from "./webhook-adapter.js";
import { validateTrustBoundaryManifest } from "../kit/trust.js";

const REQUIRED_SECRET = "WEBHOOK_SECRET";

export interface WebhookSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	streamIsMessageChatIds?: ReadonlySet<string> | undefined;
	spawner?: ManualScheduler["spawner"] | undefined;
	/** Harness-stamped deterministic scheduler for the ingress rows. */
	scheduler?: ManualScheduler | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
}

/**
 * Harness wrapper around the raw wire: models the markdown-RENDERING
 * rejection script (`forceFormattingError`) exactly like the reference
 * fixture — the §6.1 plain-text fallback body succeeds on the wire.
 */
function wrapWire(raw: FakePlatformWire): WebhookAdapter["wire"] {
	return {
		transmitSend: async (
			chatId: string,
			content: string,
			metadata: Metadata,
		): Promise<SendResult> => {
			if (
				metadata["forceFormattingError"] === true &&
				!content.startsWith("(Response formatting failed, plain text:")
			) {
				return {
					success: false,
					error: "Bad Request: can't parse entities",
				};
			}
			return raw.transmitSend(chatId, content, metadata);
		},
		transmitEdit: async (
			chatId: string,
			messageId: string,
			content: string,
			metadata: Metadata,
		): Promise<SendResult> =>
			raw.transmitEdit(chatId, messageId, content, metadata),
		transmitDraft: async (
			chatId: string,
			draftId: number,
			content: string,
			final: boolean,
			metadata: Metadata,
		): Promise<SendResult> =>
			raw.transmitDraft(chatId, draftId, content, final, metadata),
		transmitRich: async (
			chatId: string,
			content: string,
			metadata: Metadata,
		): Promise<SendResult> => raw.transmitRich(chatId, content, metadata),
		hasScript: (opKind: string) => raw.hasScript(opKind as "send"),
	};
}

/** The webhook-shaped ConformanceSubject over the REAL WebhookAdapter. */
export class WebhookSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: WebhookAdapter;
	readonly wire: FakePlatformWire;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: WebhookSubjectOptions) {
		this.name = opts.name ?? "webhook-reference";
		this.wire = opts.wire;
		this.adapter = new WebhookAdapter({
			wire: wrapWire(opts.wire),
			globalSecretReader: () =>
				opts.withSecret === false ? undefined : "webhook-secret",
			apiKeyProvider: () =>
				opts.withSecret === false ? undefined : "api-server-key",
			...(opts.streamIsMessageChatIds !== undefined
				? { streamIsMessageChatIds: opts.streamIsMessageChatIds }
				: {}),
		});
		this.adapter.attachStandardGuard(opts.spawner);
		// Harness-stamped deterministic scheduler for ingress rows (rows read
		// it back through SCHEDULER_SYMBOL).
		if (opts.scheduler !== undefined) {
			(this as unknown as Record<symbol, unknown>)[SCHEDULER_SYMBOL] =
				opts.scheduler;
		}
	}

	// ── observability ──
	doorAudit() {
		return this.adapter.doorAudit();
	}
	turns(): readonly string[] {
		return this.adapter.turnLog;
	}
	replies(): readonly string[] {
		return this.adapter.replyLog;
	}
	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.adapter.lifecycle.statusSnapshot();
	}

	// ── ingress lane ──
	deliverInbound(event: IncomingEvent, sessionKey: string): Promise<void> {
		return this.adapter.deliverInbound(event, sessionKey);
	}
	holdTurnsForBurst(on: boolean): void {
		this.adapter.holdTurns(on);
	}
	armClarifyIntercept(sessionKey: string): void {
		this.adapter.clarifyArmed.add(sessionKey);
	}
	disarmClarifyIntercept(): void {
		this.adapter.clarifyArmed.clear();
	}
	clarifyCaptures(): readonly string[] {
		return this.adapter.clarifyCaptures;
	}

	// ── egress lanes ──
	sendThroughDoor1(
		chatId: string,
		content: string,
		metadata?: Metadata,
	): Promise<SendResult> {
		return this.adapter.send(chatId, content, undefined, metadata);
	}
	sendThroughDoor2(
		platform: string,
		chatId: string,
		content: string,
		metadata?: Metadata,
	): Promise<SendResult> {
		return this.adapter.sendForPlatform(
			platform,
			chatId,
			content,
			undefined,
			metadata,
		);
	}
	sendInterim(chatId: string, content: string): Promise<SendResult> {
		return this.adapter.send(chatId, content, undefined, {
			_interim_send: true,
		} as unknown as Metadata);
	}
	deliverLongText(chatId: string, content: string): Promise<SendResult[]> {
		return this.adapter.deliverText(chatId, content);
	}
	deliverToUtf16Chat(chatId: string, content: string): Promise<SendResult[]> {
		return this.adapter.deliverText(chatId, content);
	}
	async deliverFormattingRejected(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		const results = await this.adapter.deliverText(chatId, content, {
			forceFormattingError: true,
		} as Metadata);
		return results[results.length - 1] ?? { success: false };
	}
	transientRichFailureOutcome(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		void chatId;
		// Fresh ladder lane against a rich endpoint failing TRANSIENTLY:
		// the outcome must be a failed retryable SendResult and NO legacy send.
		const ladder = new FormattingLadder({
			tryRich: async () => ({ success: false, error: "socket hang up" }),
			sendConverted: async () => ({
				success: false,
				error: "SHOULD-NOT-HAPPEN",
			}),
			sendPlain: async () => ({ success: false, error: "SHOULD-NOT-HAPPEN" }),
		});
		return ladder.sendText(content, {});
	}
	async parseFailurePlainResend(
		chatId: string,
		content: string,
	): Promise<string> {
		await this.deliverFormattingRejected(chatId, content);
		const sends = this.wire.sendsOf(chatId);
		return sends[sends.length - 1]?.content ?? "";
	}
	chatPolicyFor(chatId: string): ChatLengthPolicy {
		return this.adapter.chatLengthPolicyForChat(chatId);
	}

	// ── streaming seam ──
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(chatId: string, draftId: number): Promise<void> {
		return this.adapter.armNativeStream(chatId, draftId);
	}
	failNextSeals(n: number): void {
		this.wire.script(
			"seal",
			...Array.from({ length: n }, () => ({
				kind: "fail" as const,
				error: "forced seal failure",
			})),
		);
	}

	// ── interactive surfaces ──
	callbackRouter(): CallbackQueryRouter {
		return this.adapter.router;
	}
	actionRegistry(): ActionHandlerRegistry {
		return this.adapter.actionRegistry;
	}
	registerApprovalPending(id: number, sessionKey: string): void {
		this.adapter.approvals.register(id, sessionKey);
	}
	registerSlashConfirmPending(id: number, sessionKey: string): void {
		this.adapter.slashConfirms.register(id, sessionKey);
	}
	registerClarifyPending(id: number, sessionKey: string): void {
		this.adapter.clarify.register(id, sessionKey);
	}
	registerApprPending(id: number, sessionKey: string): void {
		this.adapter.appr.register(id, sessionKey);
	}
	setClickerAuthorization(allow: boolean): void {
		this.adapter.setClickerAuthorization(allow);
	}
	resolvedFamilies(): readonly string[] {
		return this.adapter.resolvedFamilies;
	}
	resolvedTurnDispatches(): readonly string[] {
		// The router NEVER dispatches turns for stale/unknown taps — the
		// resolved-family log is the dispatch audit at reference scope.
		return [...this.adapter.resolvedFamilies];
	}

	// ── identity/secrets probes ──
	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		if (!this.lockHeld) {
			const first = this.adapter.acquireCredentialLock(
				this.lockManager,
				"webhook-endpoint-secret",
				"cred-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"webhook-endpoint-secret",
				"cred-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"webhook-endpoint-secret",
				"cred-1",
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new WebhookAdapter({
			wire: new FakePlatformWire(),
			globalSecretReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "webhook-scoped-probe",
				description: "",
				transportShape: "webhook",
				requiresEnv: [{ name: envKey }],
				capabilities: {},
			},
			() => undefined,
		).enabled;
	}

	// ── DEC-022 declaration ──
	wakeLaneDeclaration(): "forged-event" | "raw-key-direct" {
		return this.adapter.wakeLane;
	}

	// ── webhook-shape probes (transport rows) ──

	/** Flag pairing + trust-boundary completeness for makeWebhookRows. */
	flagsAndTrustProbe(): {
		interactiveResumeFalse: boolean;
		supportsAsyncDeliveryFalse: boolean;
		trustBoundaryComplete: boolean;
	} {
		const errors = validateTrustBoundaryManifest(this.adapter.trustBoundary);
		return {
			interactiveResumeFalse: this.adapter.interactiveResume === false,
			supportsAsyncDeliveryFalse: this.adapter.supportsAsyncDelivery === false,
			trustBoundaryComplete: errors.length === 0,
		};
	}
}

// Local import indirection kept out of module scope to avoid a cycle through
// the subject's dynamic probe path.
import { FormattingLadder } from "../kit/formatting-ladder.js";

export { REQUIRED_SECRET };
export function makeWebhookSubject(
	opts: WebhookSubjectOptions & { wire: FakePlatformWire },
): WebhookSubject {
	return new WebhookSubject(opts);
}
