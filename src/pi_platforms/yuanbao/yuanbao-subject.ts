// pi_platforms/yuanbao/yuanbao-subject — the Yuanbao adapter wired as a
// ConformanceSubject (04 §8 merge-gate wiring). Shared rows run against the
// REAL kit-built adapter over FakePlatformWire egress capture; the binary
// protobuf WS plane is exercised by the engine fixture against
// FakeYuanbaoGateway.

import {
	TokenLockManagerSeam,
	FormattingLadder,
	resolveEnablement,
} from "../kit/index.js";
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
import type { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import {
	YuanbaoAdapter,
	type YuanbaoAdapterOptions,
} from "./yuanbao-adapter.js";
import { FakeYuanbaoGateway } from "./fake-yuanbao.js";
import { YUANBAO_PLUGIN_MANIFEST } from "./manifest.js";

export interface YBSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: ManualScheduler["spawner"] | undefined;
	scheduler?: ManualScheduler | undefined;
	gateway?: FakeYuanbaoGateway | undefined;
	withSecret?: boolean | undefined;
	appKey?: string | undefined;
	appSecret?: string | undefined;
	nowMs?: (() => number) | undefined;
	sleepMs?: ((ms: number) => Promise<void>) | undefined;
	replyHeartbeatIntervalMs?: number | undefined;
	slowResponseTimeoutMs?: number | undefined;
	/**
	 * OFF disables the egress-capture seam entirely (fixture worlds drive the
	 * REAL binary WS face); GOVERN (default) lets shared-row scripts govern
	 * send results while unscripted sends record-and-succeed.
	 */
	captureMode?: "govern" | "off" | undefined;
}

/** The yuanbao-shaped ConformanceSubject over the REAL adapter. */
export class YuanbaoSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: YuanbaoAdapter;
	readonly wire: FakePlatformWire;
	readonly gateway: FakeYuanbaoGateway;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: YBSubjectOptions) {
		this.name = opts.name ?? "yuanbao";
		this.wire = opts.wire;
		this.gateway = opts.gateway ?? new FakeYuanbaoGateway();
		const hasSecret = opts.withSecret !== false;
		const adapterOpts: YuanbaoAdapterOptions = {
			scalarMaxUnits: 64, // harness-scale budget mirrors reference subjects
			appKey: opts.appKey ?? (hasSecret ? "fake-app-key" : ""),
			appSecret: opts.appSecret ?? (hasSecret ? "fake-app-secret" : ""),
			gateway: this.gateway,
			signHttp: {
				postJson: async () => ({
					status: 200,
					body: {
						code: 0,
						data: {
							token: "fake-sign-token",
							bot_id: "bot-self",
							duration: 7200,
						},
					},
				}),
				...(opts.sleepMs !== undefined ? { sleepMs: opts.sleepMs } : {}),
				...(opts.nowMs !== undefined ? { nowMs: opts.nowMs } : {}),
			},
			...(opts.nowMs !== undefined ? { nowMs: opts.nowMs } : {}),
			...(opts.sleepMs !== undefined ? { sleepMs: opts.sleepMs } : {}),
			...(opts.replyHeartbeatIntervalMs !== undefined
				? { replyHeartbeatIntervalMs: opts.replyHeartbeatIntervalMs }
				: {}),
			...(opts.slowResponseTimeoutMs !== undefined
				? { slowResponseTimeoutMs: opts.slowResponseTimeoutMs }
				: {}),
			// §10.1 tier-1 rich probe rides the capture wire; unscripted probes
			// answer the capability-error shape WITHOUT a roundtrip.
			richProbe: (content) => this.wire.transmitRich("__rich__", content, {}),
			richHasScript: () => this.wire.hasScript("rich"),
			...(opts.captureMode === "off"
				? {}
				: {
						// Egress capture: scripted behaviors GOVERN results;
						// unscripted sends RECORD and SUCCEED via the capture
						// (shared rows have no live WS face by design).
						egressCapture: async (
							chatId: string,
							content: string,
							metadata: Metadata,
						): Promise<SendResult> => {
							// Scripted behaviors GOVERN; unscripted sends record AND
							// succeed (shared subjects have no live WS face).
							const scripted = this.wire.hasScript("send");
							const res = await this.wire.transmitSend(
								chatId,
								content,
								metadata,
							);
							return scripted
								? res
								: { success: true, messageId: res.messageId };
						},
					}),
		};
		this.adapter = new YuanbaoAdapter(adapterOpts);
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

	// ── streaming seam ──
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
				"yuanbao-app-key",
				`cred-${this.adapter.appKey}`,
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"yuanbao-app-key",
				`cred-${this.adapter.appKey}`,
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"yuanbao-app-key",
				`cred-${this.adapter.appKey}`,
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new YuanbaoAdapter({
			appKey: "",
			appSecret: "",
			gateway: this.gateway,
			signHttp: {
				postJson: async () => ({ status: 200, body: {} }),
			},
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		return resolveEnablement(
			{
				name: "yb-scoped-probe",
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

	get manifestName(): string {
		return YUANBAO_PLUGIN_MANIFEST.name;
	}
}

export function makeYBSubject(
	opts: YBSubjectOptions & { wire: FakePlatformWire },
): YuanbaoSubject {
	return new YuanbaoSubject(opts);
}
