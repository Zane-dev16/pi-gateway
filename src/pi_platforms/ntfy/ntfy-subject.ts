// pi_platforms/ntfy/ntfy-subject — the ntfy adapter wired as a
// ConformanceSubject (04 §8): publish capture rides the SHARED harness wire,
// the /json stream rides the fake ntfy server. The REAL NtfyAdapter runs
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

import { NtfyAdapter } from "./ntfy-adapter.js";
import type { FakeNtfyServer } from "./fake-ntfy-server.js";
import { AutoAdvanceClock } from "./clock.js";
import type { PacingClockLike } from "./clock.js";

export interface NtfySubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	server: FakeNtfyServer;
	clock?: PacingClockLike | undefined;
	spawner?: TaskSpawner | undefined;
	scheduler?: ManualScheduler | undefined;
	scalarMaxUnits?: number | undefined;
	/** Config-lane token (fixture seam for token-protected topic rows). */
	token?: string | undefined;
	withSecret?: boolean | undefined;
	declaredDraftStreaming?: boolean | undefined;
	/** Test seam for the X-Markdown datum (world dual-path leg). */
	markdownEnabled?: boolean | undefined;
}

/** The ntfy-shaped ConformanceSubject over the REAL adapter engine. */
export class NtfySubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: NtfyAdapter;
	readonly wire: FakePlatformWire;
	readonly server: FakeNtfyServer;
	readonly clock: PacingClockLike;

	constructor(opts: NtfySubjectOptions) {
		this.name = opts.name ?? "ntfy";
		this.wire = opts.wire;
		this.server = opts.server;
		this.clock = opts.clock ?? new AutoAdvanceClock();

		const rawWire = this.wire;
		const withSecret = (): boolean => opts.withSecret !== false;
		this.adapter = new NtfyAdapter({
			server: opts.server,
			clock: this.clock,
			...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
			...(opts.token !== undefined ? { config: { token: opts.token } } : {}),
			manifestName: this.name,
			...(opts.declaredDraftStreaming !== undefined
				? { declaredDraftStreaming: opts.declaredDraftStreaming }
				: {}),
			secretReader: (key) => {
				if (!withSecret()) return undefined;
				if (key === "NTFY_TOPIC") return "hermes-in";
				return undefined;
			},
		});
		if (opts.markdownEnabled) this.adapter.setMarkdownEnabledForTests(true);

		// Bind the engine's publish transport to the shared harness wire.
		this.adapter.wireTransmitPublish = async (
			topic: string,
			body: string,
			metadata: Metadata,
		): Promise<SendResult> => {
			if (
				metadata["forceFormattingError"] === true &&
				!body.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			) {
				return { success: false, error: "Bad Request: can't parse entities" };
			}
			// Harness-wire keying: record under the LOGICAL chat id (differs
			// from the wire topic when publish_topic is configured).
			const logicalChatId =
				typeof metadata["ntfy_target_chat_id"] === "string"
					? (metadata["ntfy_target_chat_id"] as string)
					: topic;
			const result = await rawWire.transmitSend(logicalChatId, body, metadata);
			if (!result.success) return result;
			// Mirror onto the fake HTTP surface so header contracts observe
			// exactly what shipped (X-Tags/X-Markdown/Authorization).
			this.server.publish(
				topic,
				body,
				(metadata["ntfy_headers"] ?? {}) as Record<string, string>,
			);
			return result;
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
				name: "ntfy-scoped-probe",
				description: "",
				transportShape: "ws",
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

export function makeNtfySubject(
	opts: NtfySubjectOptions & { wire: FakePlatformWire; server: FakeNtfyServer },
): NtfySubject {
	return new NtfySubject(opts);
}
