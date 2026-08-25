// pi_platforms/a2a/a2a-subject — the A2A adapter wired as a ConformanceSubject
// (04 §8 merge-gate wiring). Shared rows run against the REAL kit-built
// adapter with FakePlatformWire egress capture (the bounded-window reply door
// lands there); the JSON-RPC/SSE plane is exercised through the adapter's
// HTTP-handler seams (no sockets, headless).

import { TokenLockManagerSeam, resolveEnablement } from "../kit/index.js";
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
import { FormattingLadder } from "../kit/formatting-ladder.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import { A2AAdapter } from "./a2a-adapter.js";
import {
	AGENT_CARD_LEGACY_PATH,
	AGENT_CARD_PATH,
	validateA2aTrustBoundary,
} from "./manifest.js";
import {
	authenticate,
	localhostOnly,
	resolveBindHost,
	type EnvReader,
} from "./security.js";

export interface A2aSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: ManualScheduler["spawner"] | undefined;
	/** Harness-stamped deterministic scheduler for ingress rows. */
	scheduler?: ManualScheduler | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	/** Injected env table for the security layer (default: none ⇒ localhost-only). */
	env?: Record<string, string> | undefined;
}

/**
 * Capture seam (subject-supplied): models markdown-rendering rejection
 * scripts like the reference fixtures while recording every user-visible
 * transmission into FakePlatformWire.ops.
 */
function makeCaptureWire(raw: FakePlatformWire) {
	return {
		transmitSend: async (
			chatId: string,
			content: string,
			metadata: Record<string, unknown>,
		): Promise<SendResult> => {
			if (
				metadata["forceFormattingError"] === true &&
				!content.startsWith("(Response formatting failed, plain text:")
			) {
				return {
					success: false,
					error: "Bad Request: can't parse entities",
				};
			}
			return raw.transmitSend(chatId, content, metadata);
		},
		hasRichScript: (opKind: string) => raw.hasScript(opKind as "send"),
		transmitRich: async (
			chatId: string,
			content: string,
		): Promise<SendResult> => raw.transmitRich(chatId, content, {}),
	};
}

/** The A2A-shaped ConformanceSubject over the REAL adapter. */
export class A2aSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: A2AAdapter;
	readonly wire: FakePlatformWire;

	/** SSE frames captured from the last stream/subscribe handled WITH this sink. */
	readonly sseChunks: string[] = [];

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;
	private readonly envTable: Record<string, string>;

	constructor(opts: A2aSubjectOptions) {
		this.name = opts.name ?? "a2a";
		this.wire = opts.wire;
		this.envTable = opts.env ?? {};
		const envReader: EnvReader = (key) => this.envTable[key];
		this.adapter = new A2AAdapter({
			scalarMaxUnits: 64, // harness-scale budget mirrors the reference subjects
			envReader,
			captureWire: makeCaptureWire(opts.wire),
			pollTickMs: 5,
		});
		this.adapter.attachStandardGuard({ spawner: opts.spawner });
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

	// ── streaming seam (bounded sync window: no native lanes) ──
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		void _chatId;
		void _draftId; // SSE-over-handler is the only stream shape here
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
	callbackRouter(): CallbackQueryRouter | null {
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
				"a2a-bearer-token",
				"cred-a2a-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"a2a-bearer-token",
				"cred-a2a-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"a2a-bearer-token",
				"cred-a2a-1",
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		// A2A declares requiresEnv [] (safe to LOAD with zero secrets —
		// localhost-only posture). The secret-gated lane is REMOTE EXPOSURE:
		// a sibling asked to widen its bind (A2A_HOST=0.0.0.0) while resolving
		// NO credential is LOUD-DISABLED at construction instead of silently
		// downgrading (see the escalation note in a2a-adapter.ts + proposed
		// DEC text in manifest.ts).
		const sibling = new A2AAdapter({
			config: { host: "0.0.0.0" },
			envReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "a2a-scoped-probe",
				description: "",
				transportShape: "webhook",
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

	// ── A2A-specific probes ─────────────────────────────────────────────────

	/** Agent Card discovery probe (v1.0 canonical + legacy alias paths). */
	cardProbe(): {
		canonicalStatus: number;
		legacyStatus: number;
		protocolBinding: string;
		protocolVersion: string;
		cardCapabilitiesStreaming: boolean;
		cardPushNotifications: boolean;
		draftStreamingProbeFalse: boolean;
	} {
		const canonical = this.adapter.handleGet(AGENT_CARD_PATH);
		const legacy = this.adapter.handleGet(AGENT_CARD_LEGACY_PATH);
		const canonicalBody = canonical.body as Record<string, unknown>;
		const interfaces = canonicalBody["supportedInterfaces"] as Array<
			Record<string, unknown>
		>;
		const iface = interfaces[0] ?? {};
		const caps = canonicalBody["capabilities"] as Record<string, unknown>;
		return {
			canonicalStatus: canonical.status,
			legacyStatus: legacy.status,
			protocolBinding: String(iface["protocolBinding"] ?? ""),
			protocolVersion: String(iface["protocolVersion"] ?? ""),
			cardCapabilitiesStreaming: caps["streaming"] === true,
			cardPushNotifications: caps["pushNotifications"] === true,
			draftStreamingProbeFalse: this.adapter.supportsDraftStreaming() === false,
		};
	}

	/** Identity-matrix probe over the adapter's own security layer. */
	securityProbe(): {
		noCredentialsIdentity: string | null;
		bindHostLoopbackForced: boolean;
		widenedBindRequiresCredentialAndHost: boolean;
		peerMatchYieldsName: string | null;
		sharedMatchYieldsIp: string | null;
		wrongCredentialRejected: boolean;
		localhostOnlyMode: boolean;
	} {
		const env = (key: string): string | undefined => this.envTable[key];
		const noCreds = (key: string): string | undefined => undefined;
		const peerEnv: EnvReader = (key) =>
			key === "A2A_PEER_TOKENS" ? "alice:tok-alice-1" : undefined;
		const sharedEnv: EnvReader = (key) =>
			key === "A2A_BEARER_TOKEN" ? "shared-secret-value-1" : undefined;

		const widenedNoCred = resolveBindHost((key) =>
			key === "A2A_HOST" ? "0.0.0.0" : undefined,
		);
		const widenedWithCred = resolveBindHost((key) => {
			if (key === "A2A_HOST") return "0.0.0.0";
			if (key === "A2A_BEARER_TOKEN") return "some-token";
			return undefined;
		});

		return {
			noCredentialsIdentity: authenticate(undefined, "9.9.9.9", noCreds),
			bindHostLoopbackForced: widenedNoCred.host === "127.0.0.1",
			widenedBindRequiresCredentialAndHost: widenedWithCred.host === "0.0.0.0",
			peerMatchYieldsName: authenticate(
				"Bearer tok-alice-1",
				"10.1.1.1",
				peerEnv,
			),
			sharedMatchYieldsIp: authenticate(
				"Bearer shared-secret-value-1",
				"10.1.1.2",
				sharedEnv,
			),
			wrongCredentialRejected:
				authenticate("Bearer wrong-guess", "10.1.1.3", peerEnv) === null,
			localhostOnlyMode: localhostOnly(noCreds),
		};
	}

	/** In-flight reply-plane depth (bounded-window rows). */
	pendingCount(): number {
		return this.adapter.pendingCount();
	}

	/** Captured SSE byte stream from the last handler-driven stream. */
	sseSinkCapture(): string {
		return this.sseChunks.join("");
	}

	// ── webhook-shape probes (inherited transport rows) ──

	flagsAndTrustProbe(): {
		interactiveResumeFalse: boolean;
		supportsAsyncDeliveryFalse: boolean;
		trustBoundaryComplete: boolean;
	} {
		const errors = validateA2aTrustBoundary(this.adapter.trustBoundary);
		return {
			interactiveResumeFalse: this.adapter.interactiveResume === false,
			supportsAsyncDeliveryFalse: this.adapter.supportsAsyncDelivery === false,
			trustBoundaryComplete: errors.length === 0,
		};
	}
}

export function makeA2aSubject(
	opts: A2aSubjectOptions & { wire: FakePlatformWire },
): A2aSubject {
	return new A2aSubject(opts);
}
