// pi_platforms/feishu/feishu-subject — the feishu adapter wired as a
// ConformanceSubject (04 §8). Egress capture rides the SHARED harness wire
// (FakePlatformWire) through a subject-level FeishuRestPlane wrapper; the ws
// transport rides the in-process FakeFeishuServer; every row surface mirrors
// the ws/webhook subjects. The REAL FeishuAdapter engine runs underneath.

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

import {
	FeishuAdapter,
	FEISHU_REQUIRED_SECRETS,
	type FeishuRestPlane,
} from "./feishu-adapter.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { FakeFeishuServer } from "./fake-feishu.js";

export interface FeishuSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	server?: FakeFeishuServer | undefined;
	clock?: ManualClock | undefined;
	spawner?: TaskSpawner | undefined;
	scheduler?: ManualScheduler | undefined;
	scalarMaxUnits?: number | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	botIdentity?: { openId: string; userId: string; name: string } | undefined;
	allowedUsers?: ReadonlySet<string> | undefined;
	admins?: ReadonlySet<string> | undefined;
	dedupStatePath?: string | undefined;
	pingIntervalMs?: number | undefined;
	pingTimeoutMs?: number | undefined;
	watchdogIntervalMs?: number | undefined;
	textBatchDelayMs?: number | undefined;
}

/**
 * Subject-level REST plane: models the markdown-RENDERING rejection script
 * (forceFormattingError → parse-entity-classified failure) exactly like the
 * reference fixture, and delegates everything else to the shared harness wire
 * for egress capture.
 */
function wrapWire(raw: FakePlatformWire): FeishuRestPlane {
	let reactionSeq = 0;
	return {
		sendMessage: async (opts): Promise<SendResult> => {
			// The markdown decision rides metadata so rows observe post-vs-text
			// per op; the forced formatting error models a markdown-RENDERING
			// rejection (parse-entity class ⇒ §6.1 plain fallback succeeds).
			if (
				opts.metadata?.["forceFormattingError"] === true &&
				!opts.content.startsWith("(Response formatting failed, plain text:")
			) {
				return {
					success: false,
					error: "Bad Request: can't parse entities",
				};
			}
			const metadata: Metadata = {
				msg_type: opts.msgType,
				receive_id_type: opts.receiveIdType,
			};
			if (opts.replyToMessageId !== undefined)
				metadata["reply_to_message_id"] = opts.replyToMessageId;
			return raw.transmitSend(opts.receiveId, opts.content, metadata);
		},
		updateMessage: async (opts): Promise<SendResult> =>
			raw.transmitEdit("", opts.messageId, opts.content, {
				msg_type: opts.msgType,
			}),
		addReaction: async (opts): Promise<SendResult> => {
			reactionSeq += 1;
			return raw
				.transmitRich(`reaction:${opts.messageId}`, opts.emojiType, {
					reaction_op: "add",
				})
				.then((r) =>
					r.success ? { ...r, messageId: `reaction-${reactionSeq}` } : r,
				);
		},
		removeReaction: async (opts): Promise<SendResult> =>
			raw.transmitRich(`reaction:${opts.messageId}`, opts.reactionId, {
				reaction_op: "remove",
			}),
		getBotInfo: async () => ({
			openId: "bot-self",
			userId: "bot-user",
			name: "PiBot",
		}),
		richScripted: () => raw.hasScript("rich"),
		transmitRich: async (content, metadata) =>
			raw.transmitRich("__rich__", content, metadata),
	};
}

/**
 * The feishu-shaped ConformanceSubject over the REAL adapter engine.
 */
export class FeishuSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: FeishuAdapter;
	readonly wire: FakePlatformWire;
	readonly server: FakeFeishuServer;
	readonly clock: ManualClock;

	constructor(opts: FeishuSubjectOptions) {
		this.name = opts.name ?? "feishu";
		this.wire = opts.wire;
		this.server = opts.server ?? new FakeFeishuServer();
		this.clock = opts.clock ?? new ManualClock();
		const withSecret = opts.withSecret !== false;

		this.adapter = new FeishuAdapter({
			manifestName: this.name,
			transport: this.server,
			rest: wrapWire(opts.wire),
			clock: this.clock,
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
			secretReader: (key) =>
				withSecret
					? key === FEISHU_REQUIRED_SECRETS[0]
						? "cli_fake_app_id"
						: key === FEISHU_REQUIRED_SECRETS[1]
							? "fake-app-secret"
							: undefined
					: undefined,
			botIdentity:
				opts.botIdentity ??
				({ openId: "bot-self", userId: "bot-user", name: "PiBot" } as const),
			...(opts.allowedUsers !== undefined
				? { allowedUsers: opts.allowedUsers }
				: {}),
			...(opts.admins !== undefined ? { admins: opts.admins } : {}),
			...(opts.dedupStatePath !== undefined
				? { dedupStatePath: opts.dedupStatePath }
				: {}),
			...(opts.pingIntervalMs !== undefined
				? { pingIntervalMs: opts.pingIntervalMs }
				: {}),
			...(opts.pingTimeoutMs !== undefined
				? { pingTimeoutMs: opts.pingTimeoutMs }
				: {}),
			...(opts.watchdogIntervalMs !== undefined
				? { watchdogIntervalMs: opts.watchdogIntervalMs }
				: {}),
			...(opts.textBatchDelayMs !== undefined
				? { textBatchDelayMs: opts.textBatchDelayMs }
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
		const senderId = String(event.source?.userId ?? "");
		if (senderId === "bot-self") return Promise.resolve(); // self filter (§8)
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
		_chatId: string,
		content: string,
	): Promise<SendResult> {
		return this.adapter.transientRichOutcome(content);
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

	// ── streaming seam (probe-excluded for feishu — honest absence) ────────
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		// NO native lanes exist on feishu — a deliberate no-op (wa-cloud parity).
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
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "feishu-scoped-probe",
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

export function makeFeishuSubject(
	opts: FeishuSubjectOptions & { wire: FakePlatformWire },
): FeishuSubject {
	return new FeishuSubject(opts);
}
