// pi_platforms/line/line-subject — the LINE webhook adapter wired as a
// ConformanceSubject (04 §8 merge-gate wiring). Shared rows run against the
// REAL kit-built adapter; egress records onto FakePlatformWire through a
// capture transport that tags each call with its endpoint (reply vs push) so
// rows can observe the reply/push mode distinction as DATA.

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
import { validateLineTrustBoundary } from "./manifest.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import { LineWebhookAdapter } from "./line-webhook-adapter.js";
import type { LineApiTransport, LineMessage } from "./line-webhook-adapter.js";
import {
	FIXTURE_CHANNEL_ACCESS_TOKEN,
	FIXTURE_CHANNEL_SECRET,
} from "./fixture-secrets.js";

export interface LineSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: ManualScheduler["spawner"] | undefined;
	/** Harness-stamped deterministic scheduler for ingress rows. */
	scheduler?: ManualScheduler | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
}

/**
 * Capture transport (subject-supplied): every Reply/Push API call records ONE
 * user-visible transmission onto FakePlatformWire with `line_via` metadata —
 * the shared egress rows observe chunk/fallback behavior while delta-style
 * probes can read the endpoint kind back out of the wire ops.
 */
function makeCaptureTransport(raw: FakePlatformWire): LineApiTransport {
	const record = async (
		chatId: string,
		via: "reply" | "push",
		messages: LineMessage[],
		metadata: Record<string, unknown> = {},
	): Promise<SendResult> => {
		const content = messages.map((m) => m.text).join("\n");
		// Markdown-rendering rejection script — EXACTLY like the reference
		// fixtures: a forced formatting error fails unless this IS already the
		// plain-text fallback body (§6.1 lane succeeds on the wire).
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith("(Response formatting failed, plain text:")
		) {
			return {
				success: false,
				error: "Bad Request: can't parse entities",
			};
		}
		return raw.transmitSend(chatId, content, {
			...metadata,
			line_via: via,
		});
	};
	return {
		reply: async (token, messages, metadata) =>
			record(`reply-token:${token.slice(0, 8)}`, "reply", messages, metadata),
		push: async (chatId, messages, metadata) =>
			record(chatId, "push", messages, metadata),
	};
}

/** The LINE-shaped ConformanceSubject over the REAL adapter. */
export class LineWebhookSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: LineWebhookAdapter;
	readonly wire: FakePlatformWire;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: LineSubjectOptions) {
		this.name = opts.name ?? "line";
		this.wire = opts.wire;
		this.adapter = new LineWebhookAdapter({
			scalarMaxUnits: 64, // harness-scale budget mirrors the reference subjects
			transport: makeCaptureTransport(opts.wire),
			captureWire: {
				hasRichScript: (opKind) => opts.wire.hasScript(opKind as "rich"),
				transmitRich: async (chatId, content) =>
					opts.wire.transmitRich(chatId, content, {}),
			},
			secretReader: () =>
				opts.withSecret === false ? undefined : FIXTURE_CHANNEL_SECRET,
			config: { channel_access_token: FIXTURE_CHANNEL_ACCESS_TOKEN },
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

	// ── streaming seam (no native lanes on reply/push egress) ──
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
				"line-channel-access-token",
				"cred-line-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"line-channel-access-token",
				"cred-line-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"line-channel-access-token",
				"cred-line-1",
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new LineWebhookAdapter({
			secretReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "line-scoped-probe",
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
		const errors = validateLineTrustBoundary(this.adapter.trustBoundary);
		return {
			interactiveResumeFalse: this.adapter.interactiveResume === false,
			supportsAsyncDeliveryFalse: this.adapter.supportsAsyncDelivery === false,
			trustBoundaryComplete: errors.length === 0,
		};
	}
}

export function makeLineSubject(
	opts: LineSubjectOptions & { wire: FakePlatformWire },
): LineWebhookSubject {
	return new LineWebhookSubject(opts);
}
