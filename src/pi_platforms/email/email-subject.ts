// pi_platforms/email/email-subject — the Email adapter wired as a
// ConformanceSubject (04 §8): egress capture rides the SHARED harness wire,
// the IMAP/SMTP planes ride the fake mail servers. The REAL EmailAdapter runs
// underneath — no stubs.

import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import {
	type ActionHandlerRegistry,
	type CallbackQueryRouter,
	PLAIN_TEXT_FALLBACK_PREFIX,
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

import { EmailAdapter } from "./email-adapter.js";
import type { FakeImapServer, FakeSmtpServer } from "./fake-mail-servers.js";
import { AutoAdvanceClock } from "./clock.js";
import type { PacingClockLike } from "./clock.js";

export interface EmailSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	imap: FakeImapServer;
	smtp: FakeSmtpServer;
	clock?: PacingClockLike | undefined;
	spawner?: TaskSpawner | undefined;
	scheduler?: ManualScheduler | undefined;
	scalarMaxUnits?: number | undefined;
	withSecret?: boolean | undefined;
	requireAuthenticatedSender?: boolean | undefined;
	allowAllUsers?: boolean | undefined;
	declaredDraftStreaming?: boolean | undefined;
}

/** The email-shaped ConformanceSubject over the REAL adapter engine. */
export class EmailSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: EmailAdapter;
	readonly wire: FakePlatformWire;
	readonly imap: FakeImapServer;
	readonly smtp: FakeSmtpServer;
	readonly clock: PacingClockLike;

	constructor(opts: EmailSubjectOptions) {
		this.name = opts.name ?? "email";
		this.wire = opts.wire;
		this.imap = opts.imap;
		this.smtp = opts.smtp;
		this.clock = opts.clock ?? new AutoAdvanceClock();
		const withSecret = opts.withSecret !== false;

		const rawWire = this.wire;
		this.adapter = new EmailAdapter({
			imap: opts.imap,
			smtp: opts.smtp,
			clock: this.clock,
			...(opts.spawner === undefined ? {} : { spawner: opts.spawner }),
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
			manifestName: this.name,
			...(opts.requireAuthenticatedSender !== undefined
				? {
						config: {
							requireAuthenticatedSender: opts.requireAuthenticatedSender,
						},
					}
				: {}),
			...(opts.declaredDraftStreaming === undefined
				? {}
				: { declaredDraftStreaming: opts.declaredDraftStreaming }),
			secretReader: (key) => {
				if (!withSecret) return undefined;
				if (key === "EMAIL_ADDRESS") return "agent@fake.example";
				if (key === "EMAIL_PASSWORD") return "fake-app-password";
				if (key === "EMAIL_IMAP_HOST") return opts.imap.host;
				if (key === "EMAIL_SMTP_HOST") return opts.smtp.host;
				if (key === "EMAIL_ALLOWED_USERS" && opts.allowAllUsers !== true) {
					return "alice@example.com, bob@example.com";
				}
				if (key === "EMAIL_ALLOW_ALL_USERS" && opts.allowAllUsers === true) {
					return "true";
				}
				return undefined;
			},
		});

		// Bind the engine's egress transport to the shared harness wire.
		// Models the markdown-RENDERING rejection script exactly like the
		// reference fixtures; failures RETURN (polling-reference style).
		this.adapter.wireTransmitSend = async (
			toAddr: string,
			bodyText: string,
			metadata: Metadata,
		): Promise<SendResult> => {
			if (
				metadata["forceFormattingError"] === true &&
				!bodyText.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			) {
				return { success: false, error: "Bad Request: can't parse entities" };
			}
			return rawWire.transmitSend(toAddr, bodyText, metadata);
		};
		this.adapter.lastSendContentReader = (chatId) => {
			const sends = rawWire.sendsOf(chatId);
			return sends[sends.length - 1]?.content ?? "";
		};
		this.adapter.richScriptedProbe = () => rawWire.hasScript("rich");
		this.adapter.wireTransmitRich = (content, metadata) =>
			rawWire.transmitRich("__rich__", content, metadata);

		this.adapter.attachStandardGuard(opts.spawner);
		if (opts.scheduler !== undefined) {
			(this as unknown as Record<symbol, unknown>)[SCHEDULER_SYMBOL] =
				opts.scheduler;
		}
	}

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
	transientRichFailureOutcome(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		return this.adapter.transientRichOutcome(chatId, content);
	}
	parseFailurePlainResend(chatId: string, content: string): Promise<string> {
		return this.adapter.parseFailureResendContent(chatId, content);
	}
	chatPolicyFor(chatId: string): ChatLengthPolicy {
		return this.adapter.chatLengthPolicyForChat(chatId);
	}

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

	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		return this.adapter.secondInstanceTokenLockAttempt();
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		return this.adapter.buildMissingSecretSibling().lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		return resolveEnablement(
			{
				name: "em-scoped-probe",
				description: "",
				transportShape: "polling",
				requiresEnv: [{ name: envKey }],
				capabilities: {},
			},
			() => undefined,
		).enabled;
	}

	wakeLaneDeclaration(): "forged-event" | "raw-key-direct" {
		return this.adapter.wakeLane;
	}
}

export function makeEmailSubject(
	opts: EmailSubjectOptions & {
		wire: FakePlatformWire;
		imap: FakeImapServer;
		smtp: FakeSmtpServer;
	},
): EmailSubject {
	return new EmailSubject(opts);
}
