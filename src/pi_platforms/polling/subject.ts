// pi_platforms/polling/subject — the polling reference adapter wired as a
// ConformanceSubject (04 §8): egress capture rides the SHARED harness wire
// (FakePlatformWire), the polling transport rides the fake Bot API server,
// and every shared row surface mirrors the reference-correct subject.

import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { DraftFrameArgs } from "../../pi_gateway/streaming/adapter-seam.js";
import type {
	AdapterStatusSnapshot,
	DisableReason,
} from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import {
	type ActionHandlerRegistry,
	type CallbackQueryRouter,
	type ClickAuthorizer,
	resolveEnablement,
} from "../kit/index.js";
import type {
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import type { ConformanceSubject } from "../conformance/harness.js";
import { SCHEDULER_SYMBOL } from "../conformance/harness.js";
import { PollingAdapterCore } from "./polling-adapter.js";
import { ManualPollingClock } from "./clock.js";
import type { FakeTelegramServer } from "./fake-server.js";

export interface PollingSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	tg: FakeTelegramServer;
	clock?: ManualPollingClock | undefined;
	spawner?: TaskSpawner | undefined;
	scheduler?: ManualScheduler | undefined;
	scalarMaxUnits?: number | undefined;
	longPollTimeoutMs?: number | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
}

/**
 * The polling-shape ConformanceSubject: same row surface as the kit's
 * reference subject; transport = fake getUpdates long-poll + shared wire.
 */
export class PollingSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: PollingAdapterCore;
	readonly wire: FakePlatformWire;
	readonly tg: FakeTelegramServer;
	readonly clock: ManualPollingClock;

	constructor(opts: PollingSubjectOptions) {
		this.name = opts.name ?? "polling-reference";
		this.wire = opts.wire;
		this.tg = opts.tg;
		this.clock = opts.clock ?? new ManualPollingClock();
		const withSecret = opts.withSecret !== false;

		this.adapter = new PollingAdapterCore({
			wire: opts.tg,
			clock: this.clock,
			timer: this.clock.timer,
			...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
			...(opts.longPollTimeoutMs !== undefined
				? { longPollTimeoutMs: opts.longPollTimeoutMs }
				: {}),
			...(opts.scalarMaxUnits !== undefined
				? { scalarMaxUnits: opts.scalarMaxUnits }
				: {}),
			manifestName: this.name,
			secretReader: (key) =>
				withSecret
					? key === "TELEGRAM_BOT_TOKEN"
						? "tok"
						: undefined
					: undefined,
		});

		// Bind the engine's egress transports to the shared harness wire.
		this.adapter.wireTransmitSend = (chatId, content, metadata) =>
			this.wire.transmitSend(chatId, content, metadata);
		this.adapter.wireTransmitDraft = (args: DraftFrameArgs) =>
			this.wire.transmitDraft(
				args.chatId,
				args.draftId,
				args.content,
				false,
				args.metadata ?? {},
			);
		this.adapter.wireTransmitDraftFinal = (args: DraftFrameArgs) =>
			this.wire.transmitDraft(
				args.chatId,
				args.draftId,
				args.content,
				true,
				args.metadata ?? {},
			);
		this.adapter.editTransmit = (chatId, messageId, content, metadata) =>
			this.wire.transmitEdit(chatId, messageId, content, metadata ?? {});
		this.adapter.lastSendContentReader = (chatId) => {
			const sends = this.wire.sendsOf(chatId);
			return sends[sends.length - 1]?.content ?? "";
		};
		this.adapter.wireTransmitRich = (content, metadata) => {
			if (!this.wire.hasScript("rich")) {
				return Promise.resolve({
					success: false,
					error: "sendRichMessage: method not found",
				});
			}
			return this.wire.transmitRich("__rich__", content, metadata);
		};
		this.adapter.richScriptedProbe = () => this.wire.hasScript("rich");

		this.adapter.attachStandardGuard(opts.spawner);
		if (opts.scheduler !== undefined) {
			(this as unknown as Record<symbol, unknown>)[SCHEDULER_SYMBOL] =
				opts.scheduler;
		}
	}

	// ── observability ─────────────────────────────────────────────────────

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

	// ── ingress lane ──────────────────────────────────────────────────────

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

	// ── egress lanes ──────────────────────────────────────────────────────

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
	parseFailurePlainResend(chatId: string, content: string): Promise<string> {
		return this.adapter.parseFailureResendContent(chatId, content);
	}
	transientRichFailureOutcome(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		return this.adapter.transientRichOutcome(chatId, content);
	}
	chatPolicyFor(chatId: string): ChatLengthPolicy {
		return this.adapter.chatLengthPolicyForChat(chatId);
	}

	// ── streaming seam ────────────────────────────────────────────────────

	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	armOpenNativeStream(chatId: string, draftId: number): Promise<void> {
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

	// ── interactive surfaces ──────────────────────────────────────────────

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

	// ── identity/secrets probes ───────────────────────────────────────────

	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		return this.adapter.secondInstanceTokenLockAttempt();
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		return this.adapter.buildMissingSecretSibling().lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		const enablement = resolveEnablement(
			{
				name: "scoped-probe",
				description: "",
				transportShape: "polling",
				requiresEnv: [{ name: envKey }],
				capabilities: {},
			},
			() => undefined, // scoped store misses — process env HAS the key
		);
		return enablement.enabled;
	}

	// ── DEC-022 declaration ───────────────────────────────────────────────

	wakeLaneDeclaration(): "forged-event" | "raw-key-direct" {
		return this.adapter.wakeLane;
	}
}

/** Authorizer seam re-export (subject parity with kit types). */
export type { ClickAuthorizer };

export interface MakePollingSubjectOptions extends PollingSubjectOptions {}

export function makePollingSubject(
	opts: MakePollingSubjectOptions & {
		wire: FakePlatformWire;
		tg: FakeTelegramServer;
	},
): PollingSubject {
	return new PollingSubject(opts);
}

/** Unused-import guard: DisableReason flows through lifecycle snapshots. */
export type { DisableReason };
