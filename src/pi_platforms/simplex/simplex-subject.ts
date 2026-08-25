// pi_platforms/simplex/simplex-subject — the SimpleX adapter wired as a
// ConformanceSubject (04 §8 merge-gate wiring). Guard/router plumbing is the
// msgraph-webhook reference plumbing copied verbatim; egress capture rides
// the SHARED harness wire (FakePlatformWire) through a subject-level capture
// bridge ON TOP of the shared wire, and every row surface mirrors the
// signal/ws subjects. The daemon/engine lanes are exercised by simplex-rows
// engine worlds against FakeSimplexDaemon + ManualClock directly.

import {
	PLAIN_TEXT_FALLBACK_PREFIX,
	resolveEnablement,
	TokenLockManagerSeam,
} from "../kit/index.js";
import { FormattingLadder } from "../kit/formatting-ladder.js";
import type { CallbackQueryRouter } from "../kit/callback-router.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import type { TaskSpawner } from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import { SIMPLEX_PLUGIN_MANIFEST } from "./manifest.js";
import { FakeSimplexDaemon } from "./simplex-wire.js";
import {
	SimplexAdapter,
	type SimplexEgressCapture,
} from "./simplex-adapter.js";

export interface SimplexSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: TaskSpawner | undefined;
	/** Harness-stamped deterministic scheduler for ingress rows. */
	scheduler?: ManualScheduler | undefined;
	scalarMaxUnits?: number | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	/**
	 * Lie-scan fixture seam ONLY: flips THE manifest datum that drives the
	 * streaming-exclusion probe so the negative gate can prove a lying
	 * capability claim FAILS the streaming family rows. Never set in production.
	 */
	declaredMessageEditing?: boolean | undefined;
}

/**
 * Subject-level capture bridge: models the markdown-RENDERING rejection
 * script (`forceFormattingError`) EXACTLY like the reference subjects — a
 * forced formatting error fails unless this IS already the §6.1 plain-text
 * fallback body — and delegates everything else to the shared harness wire so
 * every user-visible transmission lands in wire.ops.
 */
class SubjectCaptureBridge implements SimplexEgressCapture {
	constructor(private readonly raw: FakePlatformWire) {}

	async transmitSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
		) {
			return { success: false, error: "can't parse entities" };
		}
		return this.raw.transmitSend(chatId, content, metadata);
	}

	hasRichScript(_opKind: string): boolean {
		// The capture seam only ever probes the rich lane.
		return this.raw.hasScript("rich");
	}

	async transmitRich(chatId: string, content: string): Promise<SendResult> {
		return this.raw.transmitRich(chatId, content, {});
	}
}

/** The ws-shaped ConformanceSubject over the REAL SimpleX engine. */
export class SimplexSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: SimplexAdapter;
	readonly wire: FakePlatformWire;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: SimplexSubjectOptions) {
		this.name = opts.name ?? "simplex-reference";
		this.wire = opts.wire;
		const withSecret = opts.withSecret !== false;
		this.adapter = new SimplexAdapter({
			wsFactory: new FakeSimplexDaemon(),
			captureWire: new SubjectCaptureBridge(opts.wire),
			scalarMaxUnits: opts.scalarMaxUnits ?? 64, // harness-scale budget
			secretReader: (key) =>
				withSecret
					? key === "SIMPLEX_WS_URL"
						? "ws://127.0.0.1:5225"
						: undefined
					: undefined,
			...(opts.declaredMessageEditing !== undefined
				? { declaredMessageEditing: opts.declaredMessageEditing }
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
		} as unknown as Metadata);
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

	// ── streaming seam (probe-computed exclusion: no edits ⇒ no drafts) ──────
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		void _chatId;
		void _draftId; // no native stream lanes on SimpleX
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
		// The router NEVER dispatches turns for stale/unknown taps.
		return [...this.adapter.resolvedFamilies];
	}

	// ── identity/secrets probes ──
	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		if (!this.lockHeld) {
			const first = this.adapter.acquireCredentialLock(
				this.lockManager,
				"simplex-ws-url",
				"cred-simplex-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"simplex-ws-url",
				"cred-simplex-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"simplex-ws-url",
				"cred-simplex-1",
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		// A SIBLING subject built without required secrets disables LOUDLY:
		// SIMPLEX_WS_URL missing ⇒ secret_missing reason naming the key.
		const sibling = new SimplexAdapter({
			wsFactory: new FakeSimplexDaemon(),
			secretReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "simplex-scoped-probe",
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

	pluginManifest() {
		return SIMPLEX_PLUGIN_MANIFEST;
	}
}

export function makeSimplexSubject(
	opts: SimplexSubjectOptions & { wire: FakePlatformWire },
): SimplexSubject {
	return new SimplexSubject(opts);
}
