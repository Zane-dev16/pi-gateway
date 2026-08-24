// pi_platforms/discord/discord-subject — the Discord adapter wired as a
// ConformanceSubject (04 §8 merge gate). Egress capture rides the SHARED
// harness wire (FakePlatformWire) through a subject-level DiscordRestPlane
// wrapper; the gateway transport rides the in-process DiscordGatewayFake;
// every surface mirrors the persistent-ws/polling/webhook subjects. The REAL
// DiscordAdapter engine runs underneath — no stubs.

import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy, LengthUnit } from "../kit/length-policy.js";
import {
	type ActionHandlerRegistry,
	type CallbackQueryRouter,
	resolveEnablement,
	TokenLockManagerSeam,
	PLAIN_TEXT_FALLBACK_PREFIX,
} from "../kit/index.js";
import type {
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import type { ConformanceSubject } from "../conformance/harness.js";
import { SCHEDULER_SYMBOL } from "../conformance/harness.js";

import {
	DiscordAdapter,
	DISCORD_REQUIRED_SECRET,
	type AdapterClock,
	type DiscordRestPlane,
	type HistoryProvider,
} from "./discord-adapter.js";
import { DiscordGatewayFake } from "./gateway-fake.js";
import { ManualClock } from "./clock.js";
import type { ReconnectLadderOptions } from "../persistent-ws/reconnect-ladder.js";

export interface DiscordSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	gateway?: DiscordGatewayFake | undefined;
	clock?: ManualClock | undefined;
	spawner?: TaskSpawner | undefined;
	scheduler?: ManualScheduler | undefined;
	scalarMaxUnits?: number | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	/**
	 * Rate gating OFF by default for shared-row determinism (tuning parity
	 * with scalarMaxUnits); rate-contract worlds enable the production table.
	 */
	rateGate?: boolean | undefined;
	ladder?: ReconnectLadderOptions | undefined;
	historyProvider?: HistoryProvider | undefined;
}

interface TypingAuditOp {
	chatId: string;
	metadata: Metadata;
}

interface AckAuditOp {
	interactionId: string;
	kind: string;
}

/**
 * Subject-level REST plane: models the markdown-RENDERING rejection script
 * (`forceFormattingError`) exactly like the reference subjects — the §6.1
 * plain-text fallback body succeeds on the wire — and delegates everything
 * else to the shared harness wire. Typing/interaction-ack ops ride dedicated
 * audit lanes so they never pollute send-op accounting.
 */
function wrapWire(
	raw: FakePlatformWire,
	audit: { typing: TypingAuditOp[]; acks: AckAuditOp[] },
): DiscordRestPlane {
	return {
		transmitSend: async (
			chatId: string,
			content: string,
			metadata: Metadata,
		): Promise<SendResult> => {
			if (
				metadata["forceFormattingError"] === true &&
				!content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
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
			_chatId: string,
			content: string,
			metadata: Metadata,
		): Promise<SendResult> => raw.transmitRich("__embed__", content, metadata),
		transmitThreadCreate: async (
			chatId: string,
			name: string,
			metadata: Metadata,
		): Promise<SendResult> =>
			raw.transmitSend(chatId, name, { ...metadata, thread_create: true }),
		transmitTyping: async (
			chatId: string,
			metadata: Metadata,
		): Promise<SendResult> => {
			audit.typing.push({ chatId, metadata });
			if (raw.hasScript("send")) {
				return raw.transmitSend(chatId, "__typing__", {
					...metadata,
					typing_probe: true,
				});
			}
			return { success: true };
		},
		transmitInteractionAck: async (
			interactionId: string,
			kind: string,
		): Promise<SendResult> => {
			audit.acks.push({ interactionId, kind });
			return { success: true };
		},
		hasScript: (opKind) => raw.hasScript(opKind),
	};
}

/** The persistent-ws-shaped ConformanceSubject over the REAL Discord engine. */
export class DiscordSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: DiscordAdapter;
	readonly wire: FakePlatformWire;
	readonly gateway: DiscordGatewayFake;
	readonly clock: ManualClock;
	readonly typingOps: TypingAuditOp[] = [];
	readonly interactionAcks: AckAuditOp[] = [];

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: DiscordSubjectOptions) {
		this.name = opts.name ?? "discord-reference";
		this.wire = opts.wire;
		this.gateway = opts.gateway ?? new DiscordGatewayFake();
		this.clock = opts.clock ?? new ManualClock();
		const withSecret = opts.withSecret !== false;

		this.adapter = new DiscordAdapter({
			manifestName: this.name,
			transport: this.gateway,
			rest: wrapWire(opts.wire, {
				typing: this.typingOps,
				acks: this.interactionAcks,
			}),
			clock: this.clock as AdapterClock,
			botUserId: this.gateway.botUserId,
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
			requiresEnv: [{ name: DISCORD_REQUIRED_SECRET }],
			secretReader: (key) =>
				withSecret
					? key === DISCORD_REQUIRED_SECRET
						? "discord-fake-token"
						: undefined
					: undefined,
			rateGate: opts.rateGate ?? false,
			...(opts.ladder !== undefined ? { ladder: opts.ladder } : {}),
			...(opts.historyProvider !== undefined
				? { historyProvider: opts.historyProvider }
				: {}),
		});
		if (withSecret) this.adapter.registerToken("discord-fake-token");
		// §6.3/A15 conformance lane: utf16-named chats front a UTF-16 platform —
		// budget AND unit move together through THE one descriptor seam.
		this.adapter.setChatDescriptor("chat-utf16", {
			maxMessageLength: 30,
			lenUnit: "utf16",
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
		logicalPlatform: string,
		chatId: string,
		content: string,
		metadata?: Metadata,
	): Promise<SendResult> {
		return this.adapter.sendForPlatform(
			logicalPlatform,
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
		});
		return results[results.length - 1] ?? { success: false };
	}
	async transientRichFailureOutcome(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		return this.adapter.transientRichOutcome(chatId, content);
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
	/** Relay-descriptor seam seeding (§6.3/A15 pair moves together). */
	setChatDescriptor(
		chatId: string,
		descriptor: {
			maxMessageLength?: number | undefined;
			lenUnit?: LengthUnit | undefined;
		},
	): void {
		this.adapter.setChatDescriptor(chatId, descriptor);
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
		return this.adapter.routerAuditResolved();
	}

	// ── identity/secrets probes ──
	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		if (!this.lockHeld) {
			const first = this.adapter.acquireCredentialLock(
				this.lockManager,
				"bot-token",
				"cred-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"bot-token",
				"cred-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf("bot-token", "cred-1");
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		return this.adapter.buildMissingSecretSibling().lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		return resolveEnablement(
			{
				name: "discord-scoped-probe",
				description: "",
				transportShape: "ws",
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
}

export function makeDiscordSubject(
	opts: DiscordSubjectOptions & { wire: FakePlatformWire },
): DiscordSubject {
	return new DiscordSubject(opts);
}
