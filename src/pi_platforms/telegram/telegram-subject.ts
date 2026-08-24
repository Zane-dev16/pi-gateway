// pi_platforms/telegram/telegram-subject — the TELEGRAM adapter wired as a
// ConformanceSubject (04 §8 census-port gate). Mirrors the polling reference
// subject 1:1: egress capture rides the SHARED harness wire (FakePlatformWire),
// the transport rides the REAL-shape Telegram Bot API fake, timing rides the
// INJECTED ManualPollingClock (inherited reference-fixture machinery).

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
import { ManualPollingClock } from "../polling/clock.js";
import { TelegramAdapter } from "./telegram-adapter.js";
import type { StickerDescriptionCache } from "./sticker-cache.js";
import type { TelegramBotApiFake } from "./telegram-fake-server.js";

export interface TelegramSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	tg: TelegramBotApiFake;
	clock?: ManualPollingClock | undefined;
	spawner?: TaskSpawner | undefined;
	scheduler?: ManualScheduler | undefined;
	/** Shared-row budgets are SMALL (reference-subject parity); the adapter's
	 * production default stays the manifest's 4096 UTF-16 units. */
	scalarMaxUnits?: number | undefined;
	longPollTimeoutMs?: number | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	reactionsEnv?: string | undefined;
	stickerCache?: StickerDescriptionCache | undefined;
}

export class TelegramSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: TelegramAdapter;
	readonly wire: FakePlatformWire;
	readonly tg: TelegramBotApiFake;
	readonly clock: ManualPollingClock;

	constructor(opts: TelegramSubjectOptions) {
		this.name = opts.name ?? "telegram";
		this.wire = opts.wire;
		this.tg = opts.tg;
		this.clock = opts.clock ?? new ManualPollingClock();
		const withSecret = opts.withSecret !== false;

		this.adapter = new TelegramAdapter({
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
			...(opts.stickerCache !== undefined
				? { stickerCache: opts.stickerCache }
				: {}),
			manifestName: this.name,
			secretReader: (key) =>
				withSecret
					? key === "TELEGRAM_BOT_TOKEN"
						? "tok"
						: undefined
					: undefined,
			optionalEnvReader: () => opts.reactionsEnv,
		});

		// Bind the inherited engine's egress transports to the SHARED harness
		// wire (identical binding to the reference subjects so every shared
		// row observes ops through one capture ledger).
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
		this.adapter.editTransmit = (chatId, messageId, content) =>
			this.wire.transmitEdit(chatId, messageId, content, {});
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
		const sibling = new TelegramAdapter({
			wire: this.tg,
			manifestName: `${this.name}-no-secret`,
			secretReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
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

export interface MakeTelegramSubjectOptions extends TelegramSubjectOptions {}

export function makeTelegramSubject(
	opts: MakeTelegramSubjectOptions & {
		wire: FakePlatformWire;
		tg: TelegramBotApiFake;
	},
): TelegramSubject {
	return new TelegramSubject(opts);
}
