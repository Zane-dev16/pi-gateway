// pi_platforms/persistent-ws/ws-subject — the persistent-ws reference adapter
// wired as a ConformanceSubject (04 §8; DEC-032): egress capture rides the
// SHARED harness wire (FakePlatformWire) through a subject-level RestPlane
// wrapper, the ws transport rides the in-process FakeWsServer, and every row
// surface mirrors the polling/webhook subjects. The REAL PersistentWsAdapter
// engine runs underneath — no stubs.

import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type {
	AdapterStatusSnapshot,
	DisableReason,
} from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
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
import { FormattingLadder } from "../kit/formatting-ladder.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import type { ConformanceSubject } from "../conformance/harness.js";
import { SCHEDULER_SYMBOL } from "../conformance/harness.js";

import {
	PersistentWsAdapter,
	WS_REQUIRED_SECRET,
	type AdapterClock,
	type RestPlane,
} from "./persistent-ws-adapter.js";
import type { ReconnectLadderOptions } from "./reconnect-ladder.js";
import { ManualClock } from "./manual-clock.js";
import { FakeWsServer } from "./fake-ws.js";

export interface WsSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	ws?: FakeWsServer | undefined;
	clock?: ManualClock | undefined;
	spawner?: TaskSpawner | undefined;
	scheduler?: ManualScheduler | undefined;
	scalarMaxUnits?: number | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	/** Watchdog tuning passthrough (fixture determinism; defaults port A23). */
	pingIntervalMs?: number | undefined;
	pingStaleFactor?: number | undefined;
	firstPingGraceMs?: number | undefined;
	watchdogIntervalMs?: number | undefined;
	ladder?: ReconnectLadderOptions | undefined;
}

/**
 * Subject-level REST plane: models the markdown-RENDERING rejection script
 * (`forceFormattingError`) exactly like the reference fixture — the §6.1
 * plain-text fallback body succeeds on the wire — and delegates everything
 * else to the shared harness wire for egress capture.
 */
function wrapWire(raw: FakePlatformWire): RestPlane {
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
		): Promise<SendResult> => raw.transmitRich("__rich__", content, metadata),
		hasScript: (opKind) => raw.hasScript(opKind),
	};
}

/**
 * The persistent-ws-shaped ConformanceSubject over the REAL adapter engine.
 */
export class WsSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: PersistentWsAdapter;
	readonly wire: FakePlatformWire;
	readonly ws: FakeWsServer;
	readonly clock: ManualClock;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: WsSubjectOptions) {
		this.name = opts.name ?? "ws-reference";
		this.wire = opts.wire;
		this.ws = opts.ws ?? new FakeWsServer();
		this.clock = opts.clock ?? new ManualClock();
		const withSecret = opts.withSecret !== false;

		this.adapter = new PersistentWsAdapter({
			manifestName: this.name,
			transport: this.ws,
			rest: wrapWire(opts.wire),
			clock: this.clock as AdapterClock,
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
			requiresEnv: [{ name: WS_REQUIRED_SECRET }],
			secretReader: (key) =>
				withSecret
					? key === WS_REQUIRED_SECRET
						? "xoxb-fake-token"
						: undefined
					: undefined,
			...(opts.pingIntervalMs !== undefined
				? { pingIntervalMs: opts.pingIntervalMs }
				: {}),
			...(opts.pingStaleFactor !== undefined
				? { pingStaleFactor: opts.pingStaleFactor }
				: {}),
			...(opts.firstPingGraceMs !== undefined
				? { firstPingGraceMs: opts.firstPingGraceMs }
				: {}),
			...(opts.watchdogIntervalMs !== undefined
				? { watchdogIntervalMs: opts.watchdogIntervalMs }
				: {}),
			...(opts.ladder !== undefined ? { ladder: opts.ladder } : {}),
		});

		this.adapter.attachStandardGuard(opts.spawner);
		// Harness-stamped deterministic scheduler for ingress rows (rows read
		// it back through SCHEDULER_SYMBOL).
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
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "ws-scoped-probe",
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

/** Unused-import guard: DisableReason flows through lifecycle snapshots. */
export type { DisableReason };

export function makeWsSubject(
	opts: WsSubjectOptions & { wire: FakePlatformWire },
): WsSubject {
	return new WsSubject(opts);
}

/** Re-export for world factories. */
export { FormattingLadder };
