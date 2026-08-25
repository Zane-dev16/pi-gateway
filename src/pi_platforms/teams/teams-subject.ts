// pi_platforms/teams/teams-subject — the Microsoft Teams adapter wired as a
// ConformanceSubject (04 §8 merge-gate wiring). Shared rows run against the
// REAL kit-built adapter with FakePlatformWire egress capture (the Bot
// Framework REST lane lands there via WireBridgeTransport); the token dance,
// threaded replies, attachments, and card actions are exercised by the engine
// fixture against FakeBotFrameworkServer directly.

import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { TokenLockManagerSeam, resolveEnablement } from "../kit/index.js";
import type { CallbackQueryRouter } from "../kit/callback-router.js";
import type { ActionHandlerRegistry } from "../kit/block-kit.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FormattingLadder } from "../kit/formatting-ladder.js";
import { validateTeamsTrustBoundary } from "./manifest.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import { TeamsAdapter } from "./teams-adapter.js";
import { FIXTURE_CLIENT_ID } from "./fixture-secrets.js";

export interface TeamsSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: ManualScheduler["spawner"] | undefined;
	/** Harness-stamped deterministic scheduler for ingress rows. */
	scheduler?: ManualScheduler | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
}

/**
 * The harness bridge around the raw wire: models the markdown-RENDERING
 * rejection script (`forceFormattingError`) exactly like the reference
 * fixtures — the §6.1 plain-text fallback body succeeds on the wire.
 */
function wrapWire(raw: FakePlatformWire) {
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
	};
}

/** The Teams-shaped ConformanceSubject over the REAL adapter. */
export class TeamsSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: TeamsAdapter;
	readonly wire: FakePlatformWire;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: TeamsSubjectOptions) {
		this.name = opts.name ?? "teams";
		this.wire = opts.wire;
		const wrapped = wrapWire(opts.wire);
		this.adapter = new TeamsAdapter({
			transport: new SubjectBridgeTransport(opts.wire, wrapped.transmitSend),
			scalarMaxUnits: 64, // harness-scale budget mirrors the reference subjects
			mediaCacheDir: harnessMediaDir(), // NEVER the real cwd
			captureWire: {
				transmitSend: wrapped.transmitSend,
				hasRichScript: (opKind) => opts.wire.hasScript(opKind as "send"),
			},
			secretReader: (key) =>
				opts.withSecret === false ? undefined : teamsHarnessSecret(key),
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

	// ── streaming seam (webhook ingress + proactive egress; no native drafts) ──
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
		return [...this.adapter.resolvedFamilies];
	}

	// ── identity/secrets probes ──
	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		if (!this.lockHeld) {
			const first = this.adapter.acquireCredentialLock(
				this.lockManager,
				"teams-bot-registration",
				"cred-teams-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"teams-bot-registration",
				"cred-teams-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"teams-bot-registration",
				"cred-teams-1",
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new TeamsAdapter({
			transport: new SubjectBridgeTransport(this.wire, async () => ({
				success: true,
			})),
			secretReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "teams-scoped-probe",
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

	// ── webhook-shape probes (COMPUTED — teams is ASYNC-CAPABLE) ──

	/**
	 * Flag pairing is asserted FROM THE DECLARED DATA, not against the
	 * stateless pairing: supports_async_delivery=True (proactive sends are
	 * real), so the stateless both-false clause does NOT apply BY THE PROBE.
	 * Flip the capability and the strict pairing requirement re-includes.
	 */
	flagsAndTrustProbe(): {
		statelessPairingRequired: boolean;
		pairingSatisfied: boolean;
		trustBoundaryComplete: boolean;
		bearerAuthDeclared: boolean;
	} {
		const errors = validateTeamsTrustBoundary(this.adapter.trustBoundary);
		const statelessPairingRequired =
			this.adapter.supportsAsyncDelivery === false &&
			this.adapter.interactiveResume === false;
		return {
			statelessPairingRequired,
			pairingSatisfied: statelessPairingRequired
				? this.adapter.interactiveResume === false &&
					this.adapter.supportsAsyncDelivery === false
				: this.adapter.supportsAsyncDelivery === true,
			trustBoundaryComplete: errors.length === 0,
			bearerAuthDeclared:
				this.adapter.trustBoundary.bearerAuthDelegatedToSdk === true &&
				FIXTURE_CLIENT_ID.length > 0,
		};
	}
}

/**
 * Subject-side transport: bridges Bot Framework REST onto FakePlatformWire so
 * every user-visible transmission lands in wire.ops. The token endpoint
 * answers canned success (in-memory harness); typing/attachments refuse loudly
 * (engine tests bind FakeBotFrameworkServer directly).
 */
class SubjectBridgeTransport {
	constructor(
		private readonly raw: FakePlatformWire,
		private readonly send: (
			chatId: string,
			content: string,
			metadata: Metadata,
		) => Promise<SendResult>,
	) {}

	async getAccessToken(): Promise<{
		status: number;
		json: Record<string, unknown>;
	}> {
		return { status: 200, json: { access_token: "subject-bridge-token" } };
	}

	async postActivity(
		conversationId: string,
		activity: Record<string, unknown>,
		_bearer: string,
		metadata: Record<string, unknown> = {},
	): Promise<{ status: number; json: Record<string, unknown> }> {
		const result = await this.send(
			conversationId,
			String(activity["text"] ?? ""),
			metadata as Metadata,
		);
		return toResponse(result);
	}

	async postReply(
		conversationId: string,
		_replyToActivityId: string,
		activity: Record<string, unknown>,
		_bearer: string,
		metadata: Record<string, unknown> = {},
	): Promise<{ status: number; json: Record<string, unknown> }> {
		const result = await this.send(
			conversationId,
			String(activity["text"] ?? ""),
			metadata as Metadata,
		);
		return toResponse(result);
	}

	async sendTypingActivity(): Promise<void> {
		throw new Error("typing requires the FakeBotFrameworkServer fixture");
	}

	async fetchAttachmentBytes(): Promise<{ status: number; bytes: Buffer }> {
		throw new Error("attachments require the FakeBotFrameworkServer fixture");
	}

	hasScript(opKind: string): boolean {
		return this.raw.hasScript(opKind as "send");
	}

	async transmitRichProbe(
		chatId: string,
		content: string,
	): Promise<{ status: number }> {
		const result = await this.raw.transmitRich(chatId, content, {});
		return { status: result.success ? 200 : 400 };
	}
}

function toResponse(result: SendResult): {
	status: number;
	json: Record<string, unknown>;
} {
	return result.success
		? { status: 200, json: { id: result.messageId ?? "bridge" } }
		: {
				status: 400,
				json: { error: { message: result.error ?? "send failed" } },
			};
}

/** Scoped harness secrets (fail-closed reader shape; never process.env). */
function teamsHarnessSecret(name: string): string | undefined {
	switch (name) {
		case "TEAMS_CLIENT_ID":
			return FIXTURE_CLIENT_ID;
		case "TEAMS_CLIENT_SECRET":
			return "teams-fixture-secret";
		case "TEAMS_TENANT_ID":
			return "teams-fixture-tenant";
		default:
			return undefined;
	}
}

/**
 * One throwaway media dir per WORKER PROCESS: attachment caching must NEVER
 * write into the repo tree (mkdtemp isolation).
 */
let harnessMediaDirCache: string | undefined;
function harnessMediaDir(): string {
	if (harnessMediaDirCache === undefined) {
		harnessMediaDirCache = mkdtempSync(join(tmpdir(), "teams-subject-"));
	}
	return harnessMediaDirCache;
}

export function makeTeamsSubject(
	opts: TeamsSubjectOptions & { wire: FakePlatformWire },
): TeamsSubject {
	return new TeamsSubject(opts);
}
