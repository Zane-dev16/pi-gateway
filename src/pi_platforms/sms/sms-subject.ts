// pi_platforms/sms/sms-subject — the SMS (Twilio) adapter wired as a
// ConformanceSubject (04 §8 merge-gate wiring). Shared rows run against the
// REAL kit-built adapter; egress rides the TwilioRestBridge bound to the
// harness FakePlatformWire (Messages.json POSTs record as send ops), and the
// webhook plane is exercised by the engine fixture against the adapter's
// handleWebhookPost seam.

import { TokenLockManagerSeam, resolveEnablement } from "../kit/index.js";
import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { CallbackQueryRouter } from "../kit/callback-router.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FormattingLadder } from "../kit/formatting-ladder.js";
import { validateSmsTrustBoundary } from "./manifest.js";
import { SmsAdapter, type SmsAdapterConfig } from "./sms-adapter.js";
import { TwilioRestBridge } from "./sms-rest-bridge.js";
import {
	FIXTURE_ACCOUNT_SID,
	FIXTURE_AUTH_TOKEN,
	FIXTURE_FROM_NUMBER,
	FIXTURE_WEBHOOK_URL,
} from "./fixture-secrets.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

export interface SmsSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: ManualScheduler["spawner"] | undefined;
	/** Harness-stamped deterministic scheduler for ingress rows. */
	scheduler?: ManualScheduler | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	/**
	 * Lie-scan fixture seam ONLY: flips THE probe datum behind
	 * supportsDraftStreaming() so the negative gate can prove a lying
	 * capability claim FAILS the streaming family rows. Never set in production.
	 */
	declaredMessageEditing?: boolean | undefined;
}

/** The sms-shaped ConformanceSubject over the REAL adapter. */
export class SmsSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: SmsAdapter;
	readonly wire: FakePlatformWire;
	readonly rest: TwilioRestBridge;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: SmsSubjectOptions) {
		this.name = opts.name ?? "sms";
		this.wire = opts.wire;
		this.rest = new TwilioRestBridge(opts.wire);
		this.adapter = new SmsAdapter({
			scalarMaxUnits: 64, // harness-scale budget mirrors the reference subjects
			secretReader: (key) =>
				opts.withSecret === false ? undefined : smsHarnessSecret(key),
			rest: this.rest,
			richProbe: this.rest,
			...(opts.declaredMessageEditing !== undefined
				? { declaredMessageEditing: opts.declaredMessageEditing }
				: {}),
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

	// ── streaming seam (stateless pairing: no native lanes) ──
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		void _chatId;
		void _draftId; // no native stream lanes on stateless SMS
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
				"sms-from-number",
				"cred-sms-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"sms-from-number",
				"cred-sms-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf("sms-from-number", "cred-sms-1");
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new SmsAdapter({
			config: smsSiblingConfig(),
			secretReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "sms-scoped-probe",
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
		const errors = validateSmsTrustBoundary(this.adapter.trustBoundary);
		return {
			interactiveResumeFalse: this.adapter.interactiveResume === false,
			supportsAsyncDeliveryFalse: this.adapter.supportsAsyncDelivery === false,
			trustBoundaryComplete: errors.length === 0,
		};
	}
}

/** Scoped harness secrets (fail-closed reader shape; never process.env). */
function smsHarnessSecret(name: string): string | undefined {
	switch (name) {
		case "TWILIO_ACCOUNT_SID":
			return FIXTURE_ACCOUNT_SID;
		case "TWILIO_AUTH_TOKEN":
			return FIXTURE_AUTH_TOKEN;
		case "TWILIO_PHONE_NUMBER":
			return FIXTURE_FROM_NUMBER;
		default:
			return undefined;
	}
}

/**
 * Sibling config keeps the webhook plane configured so the loud-disable under
 * probe is the SECRET lane (identity.missing-secret-loud-disable asserts the
 * detail names a secret), never an unrelated config refusal.
 */
function smsSiblingConfig(): SmsAdapterConfig {
	return { webhook_url: FIXTURE_WEBHOOK_URL };
}

export function makeSmsSubject(
	opts: SmsSubjectOptions & { wire: FakePlatformWire },
): SmsSubject {
	return new SmsSubject(opts);
}
