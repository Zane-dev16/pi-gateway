// pi_platforms/google-chat/gchat-subject — the Google Chat webhook adapter
// wired as a ConformanceSubject (04 §8 merge-gate wiring). Shared rows run
// against the REAL kit-built adapter with FakePlatformWire egress capture;
// the HTTP-events plane is exercised by the engine fixture against the
// adapter's handler seams.

import { TokenLockManagerSeam, resolveEnablement } from "../kit/index.js";
import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { CallbackQueryRouter } from "../kit/callback-router.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import type { StreamEgressAdapter } from "../../pi_gateway/streaming/adapter-seam.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FormattingLadder } from "../kit/formatting-ladder.js";
import { validateGchatTrustBoundary } from "./manifest.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import { GoogleChatWebhookAdapter } from "./google-chat-adapter.js";
import type {
	GchatApiResponse,
	GchatTransport,
} from "./google-chat-adapter.js";
import {
	FIXTURE_HTTP_EVENTS_AUDIENCE,
	FIXTURE_SA_EMAIL,
} from "./fixture-secrets.js";

export interface GchatSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: ManualScheduler["spawner"] | undefined;
	/** Harness-stamped deterministic scheduler for ingress rows. */
	scheduler?: ManualScheduler | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
}

/**
 * Capture transport (subject-supplied): every Chat REST create records ONE
 * user-visible transmission onto FakePlatformWire — text bodies land as send
 * ops; cardsV2 renders as a tagged placeholder. The markdown-rendering
 * rejection script (`forceFormattingError`) behaves EXACTLY like the
 * reference fixtures: it fails unless this IS the §6.1 plain-text fallback.
 */
function makeCaptureTransport(raw: FakePlatformWire): GchatTransport {
	return {
		createMessage: async (
			chatId,
			body,
			metadata = {},
		): Promise<GchatApiResponse> => {
			const text = body["text"];
			const content =
				typeof text === "string"
					? text
					: `[cardsV2:${JSON.stringify(body["cardsV2"] ?? "").length}]`;
			if (
				metadata["forceFormattingError"] === true &&
				!content.startsWith("(Response formatting failed, plain text:")
			) {
				return {
					success: false,
					status: 400,
					error: "Bad Request: can't parse entities",
				};
			}
			const result = await raw.transmitSend(chatId, content, metadata);
			return result.success
				? { success: true, messageId: result.messageId }
				: { success: false, error: result.error };
		},
		patchMessage: async (
			messageName,
			body,
			metadata = {},
			_updateMask?: string,
		): Promise<GchatApiResponse> => {
			if (
				metadata["forceFormattingError"] === true &&
				!String(body["text"] ?? "").startsWith(
					"(Response formatting failed, plain text:",
				)
			) {
				return {
					success: false,
					status: 400,
					error: "Bad Request: can't parse entities",
				};
			}
			const result = await raw.transmitEdit(
				messageName,
				messageName,
				String(body["text"] ?? ""),
				metadata,
			);
			return result.success
				? { success: true, messageId: messageName }
				: { success: false, error: result.error };
		},
	};
}

/** The Google-Chat-shaped ConformanceSubject over the REAL adapter. */
export class GchatWebhookSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: GoogleChatWebhookAdapter;
	readonly wire: FakePlatformWire;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: GchatSubjectOptions) {
		this.name = opts.name ?? "google-chat";
		this.wire = opts.wire;
		this.adapter = new GoogleChatWebhookAdapter({
			scalarMaxUnits: 64, // harness-scale budget mirrors the reference subjects
			transport: makeCaptureTransport(opts.wire),
			captureWire: {
				hasRichScript: (opKind) => opts.wire.hasScript(opKind as "rich"),
				transmitRich: async (chatId, content) =>
					opts.wire.transmitRich(chatId, content, {}),
			},
			secretReader: (name) => {
				if (opts.withSecret === false) return undefined;
				if (name === "GOOGLE_CHAT_HTTP_EVENTS_AUDIENCE") {
					return FIXTURE_HTTP_EVENTS_AUDIENCE;
				}
				if (name === "GOOGLE_CHAT_HTTP_EVENTS_SERVICE_ACCOUNT_EMAIL") {
					return FIXTURE_SA_EMAIL;
				}
				return undefined;
			},
			config: {},
		});
		this.adapter.attachStandardGuard(opts.spawner);
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
		this.adapter.setClarifyIntercept(sessionKey, true);
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
		_chatId: string,
		content: string,
	): Promise<SendResult> {
		// Fresh ladder lane against a rich endpoint failing TRANSIENTLY:
		// outcome must be a retryable failure and NO legacy send.
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

	// ── streaming seam (REST create/patch; no native lanes) ──
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		void _chatId;
		void _draftId;
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
	actionRegistry(): import("../kit/block-kit.js").ActionHandlerRegistry {
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
		return [];
	}

	// ── identity/secrets probes ──
	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		if (!this.lockHeld) {
			const first = this.adapter.acquireCredentialLock(
				this.lockManager,
				"gchat-http-events-audience",
				"cred-gchat-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"gchat-http-events-audience",
				"cred-gchat-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"gchat-http-events-audience",
				"cred-gchat-1",
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new GoogleChatWebhookAdapter({
			secretReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "gchat-scoped-probe",
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

	// ── webhook-shape probes (inherited transport rows) ──

	flagsAndTrustProbe(): {
		interactiveResumeFalse: boolean;
		supportsAsyncDeliveryFalse: boolean;
		trustBoundaryComplete: boolean;
	} {
		const errors = validateGchatTrustBoundary(this.adapter.trustBoundary);
		return {
			interactiveResumeFalse: this.adapter.interactiveResume === false,
			supportsAsyncDeliveryFalse: this.adapter.supportsAsyncDelivery === false,
			trustBoundaryComplete: errors.length === 0,
		};
	}
}

export function makeGchatSubject(
	opts: GchatSubjectOptions & { wire: FakePlatformWire },
): GchatWebhookSubject {
	return new GchatWebhookSubject(opts);
}

// Re-export for row files that probe the SA email constant.
export { FIXTURE_SA_EMAIL };
