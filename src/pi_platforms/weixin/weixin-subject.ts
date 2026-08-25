// pi_platforms/weixin/weixin-subject — the WeChat iLink adapter wired as a
// ConformanceSubject (04 §8 merge-gate wiring). Shared rows run against the
// REAL kit-built adapter over FakePlatformWire egress capture; the long-poll
// plane is exercised by the engine fixture against FakeILinkServer.

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

import { WeixinAdapter, type WeixinSyncStore } from "./weixin-adapter.js";
import { FakeILinkServer } from "./fake-ilink.js";
import { WEIXIN_PLUGIN_MANIFEST } from "./manifest.js";

export interface WXSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: ManualScheduler["spawner"] | undefined;
	scheduler?: ManualScheduler | undefined;
	server?: FakeILinkServer | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	token?: string | undefined;
	nowMs?: (() => number) | undefined;
	/** Intake policy passthrough (default pairing, Hermes __init__ parity). */
	dmPolicy?: string | undefined;
}

class MemorySyncStore implements WeixinSyncStore {
	private readonly bufs = new Map<string, string>();
	load(accountId: string): string {
		return this.bufs.get(accountId) ?? "";
	}
	save(accountId: string, buf: string): void {
		this.bufs.set(accountId, buf);
	}
}

/** The weixin-shaped ConformanceSubject over the REAL adapter. */
export class WeixinSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: WeixinAdapter;
	readonly wire: FakePlatformWire;
	readonly server: FakeILinkServer;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: WXSubjectOptions) {
		this.name = opts.name ?? "weixin";
		this.wire = opts.wire;
		this.server = opts.server ?? new FakeILinkServer();
		const hasSecret = opts.withSecret !== false;
		const token = opts.token ?? (hasSecret ? "fake-ilink-token" : "");
		this.adapter = new WeixinAdapter({
			scalarMaxUnits: 64, // harness-scale budget mirrors reference subjects
			token,
			...(opts.dmPolicy !== undefined ? { dmPolicy: opts.dmPolicy } : {}),
			server: this.server,
			syncStore: new MemorySyncStore(),
			spawner: opts.spawner,
			// Egress capture: scripted behaviors GOVERN results; unscripted
			// sends record into the wire for shared-row observability.
			...(opts.nowMs !== undefined ? { nowMs: opts.nowMs } : {}),
			sendCapture: (chatId, content, metadata) =>
				this.wire.transmitSend(chatId, content, metadata),
			captureHasScript: () => this.wire.hasScript("send"),
			// §10.1 tier-1 rich probe rides the SAME capture wire; unscripted
			// probes answer the capability-error shape WITHOUT a roundtrip.
			richProbe: (content) => this.wire.transmitRich("__rich__", content, {}),
			richHasScript: () => this.wire.hasScript("rich"),
		});
		// Guard ALWAYS attached: with the row-supplied ManualScheduler when one
		// is passed (shared rows), else the immediate production spawner.
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

	// ── streaming seam (no native lanes on iLink) ──
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
				"weixin-token",
				`cred-${this.adapter.accountId}`,
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"weixin-token",
				`cred-${this.adapter.accountId}`,
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"weixin-token",
				`cred-${this.adapter.accountId}`,
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new WeixinAdapter({
			token: "",
			server: this.server,
			syncStore: new MemorySyncStore(),
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "wx-scoped-probe",
				description: "",
				transportShape: "polling",
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

	get manifestName(): string {
		return WEIXIN_PLUGIN_MANIFEST.name;
	}
}

export function makeWXSubject(
	opts: WXSubjectOptions & { wire: FakePlatformWire },
): WeixinSubject {
	return new WeixinSubject(opts);
}
