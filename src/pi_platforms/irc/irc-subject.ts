// pi_platforms/irc/irc-subject — the IRC adapter wired as a ConformanceSubject
// (04 §8): egress capture rides the SHARED harness wire (FakePlatformWire)
// through the adapter's PRIVMSG lane; the line-protocol transport rides the
// fake ircd. The REAL IrcAdapter runs underneath — no stubs.

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

import { IrcAdapter } from "./irc-adapter.js";
import type { FakeIrcServer } from "./fake-irc-server.js";
import type { ManualClock } from "../persistent-ws/manual-clock.js";
import type { PacingClock } from "./clock.js";
import { AutoAdvanceClock } from "./clock.js";

export interface IrcSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	server: FakeIrcServer;
	clock?: PacingClock | undefined;
	spawner?: TaskSpawner | undefined;
	scheduler?: ManualScheduler | undefined;
	scalarMaxUnits?: number | undefined;
	withSecret?: boolean | undefined;
	allowedUsers?: readonly string[] | undefined;
	/** LIE-SCAN datum: flips supportsDraftStreaming (probe-computed exclusion). */
	declaredDraftStreaming?: boolean | undefined;
}

/**
 * The IRC-shaped ConformanceSubject over the REAL adapter engine.
 */
export class IrcSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: IrcAdapter;
	readonly wire: FakePlatformWire;
	readonly server: FakeIrcServer;
	readonly clock: PacingClock;

	constructor(opts: IrcSubjectOptions) {
		this.name = opts.name ?? "irc";
		this.wire = opts.wire;
		this.server = opts.server;
		this.clock = opts.clock ?? new AutoAdvanceClock();
		const withSecret = opts.withSecret !== false;

		const rawWire = this.wire;
		this.adapter = new IrcAdapter({
			fakeServer: opts.server,
			clock: this.clock,
			...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
			manifestName: this.name,
			allowedUsers: opts.allowedUsers,
			...(opts.declaredDraftStreaming !== undefined
				? { declaredDraftStreaming: opts.declaredDraftStreaming }
				: {}),
			secretReader: (key) => {
				if (!withSecret) return undefined;
				if (key === "IRC_SERVER") return opts.server.address;
				if (key === "IRC_CHANNEL") return "#hermes";
				if (key === "IRC_NICKNAME") return "pi-bot";
				return undefined;
			},
		});

		// Bind the engine's egress transport to the shared harness wire.
		// Models the markdown-RENDERING rejection script (forceFormattingError)
		// exactly like the reference fixtures — the §6.1 plain-text fallback
		// body succeeds on the wire. Failures RETURN as SendResults (polling-
		// reference style): the ladder/classifier upstream must observe them.
		this.adapter.wireTransmitPrivmsg = async (
			target: string,
			line: string,
			metadata: Metadata,
		): Promise<SendResult> => {
			if (
				metadata["forceFormattingError"] === true &&
				!line.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			) {
				return {
					success: false,
					error: "Bad Request: can't parse entities",
				};
			}
			return rawWire.transmitSend(target, line, metadata);
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

	// ── streaming seam ──

	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		void _chatId;
		void _draftId; // no native stream lanes on the line protocol
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
		return this.adapter.secondInstanceTokenLockAttempt();
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		return this.adapter.buildMissingSecretSibling().lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		return resolveEnablement(
			{
				name: "irc-scoped-probe",
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

export function makeIrcSubject(
	opts: IrcSubjectOptions & {
		wire: FakePlatformWire;
		server: FakeIrcServer;
	},
): IrcSubject {
	return new IrcSubject(opts);
}
