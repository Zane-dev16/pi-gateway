// pi_platforms/raft/raft-subject — the Raft wake-channel adapter wired as a
// ConformanceSubject (04 §8 merge-gate wiring). Shared rows run against the
// REAL kit-built adapter with FakePlatformWire egress capture (the no-op send()
// door lands there); the wake/activity plane is exercised by the engine
// fixture against the adapter's HTTP-handler seams (no sockets, headless).

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
import type { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import { RaftAdapter, type RaftCaptureWire } from "./raft-adapter.js";
import { validateRaftTrustBoundary } from "./manifest.js";

export interface RaftSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: ManualScheduler["spawner"] | undefined;
	/** Harness-stamped deterministic scheduler for ingress rows. */
	scheduler?: ManualScheduler | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
}

/**
 * Capture seam (subject-supplied): models the markdown-RENDERING rejection
 * script (`forceFormattingError`) exactly like the reference fixtures — the
 * §6.1 plain-text fallback body succeeds on the wire — while recording every
 * user-visible transmission into FakePlatformWire.ops.
 */
function makeCaptureWire(raw: FakePlatformWire): RaftCaptureWire {
	return {
		transmitSend: async (
			chatId: string,
			content: string,
			metadata: Record<string, unknown>,
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
		hasRichScript: (opKind: string) => raw.hasScript(opKind as "send"),
		transmitRich: async (
			chatId: string,
			content: string,
		): Promise<SendResult> => raw.transmitRich(chatId, content, {}),
	};
}

/** The raft-shaped ConformanceSubject over the REAL adapter. */
export class RaftSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: RaftAdapter;
	readonly wire: FakePlatformWire;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: RaftSubjectOptions) {
		this.name = opts.name ?? "raft";
		this.wire = opts.wire;
		this.adapter = new RaftAdapter({
			scalarMaxUnits: 64, // harness-scale budget mirrors the reference subjects
			secretReader: () =>
				opts.withSecret === false ? undefined : FIXTURE_RAFT_PROFILE,
			captureWire: makeCaptureWire(opts.wire),
			tokenHex: () => "fixture-bridge-token-hex-0000000000000000000000000000",
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

	// ── streaming seam (wake-only lane: no native lanes) ──
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		void _chatId;
		void _draftId; // no native stream lanes on the wake channel
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
	callbackRouter(): CallbackQueryRouter | null {
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
				"raft-bridge-token",
				"cred-raft-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"raft-bridge-token",
				"cred-raft-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"raft-bridge-token",
				"cred-raft-1",
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new RaftAdapter({
			secretReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "raft-scoped-probe",
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
		const errors = validateRaftTrustBoundary(this.adapter.trustBoundary);
		return {
			interactiveResumeFalse: this.adapter.interactiveResume === false,
			supportsAsyncDeliveryFalse: this.adapter.supportsAsyncDelivery === false,
			trustBoundaryComplete: errors.length === 0,
		};
	}
}

/** Scoped harness secret (fail-closed reader shape; never process.env). */
export const FIXTURE_RAFT_PROFILE = "fixture-agent-profile";

export function makeRaftSubject(
	opts: RaftSubjectOptions & { wire: FakePlatformWire },
): RaftSubject {
	return new RaftSubject(opts);
}
