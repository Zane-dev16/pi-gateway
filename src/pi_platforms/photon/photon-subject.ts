// pi_platforms/photon/photon-subject — the Photon adapter wired as a
// ConformanceSubject (04 §8 merge-gate wiring). Shared rows run against the
// REAL kit-built PhotonAdapter with FakePlatformWire egress capture bound
// through a capture-wire seam (formatting-rejection script modeled exactly
// like the reference fixtures); the sidecar control plane rides
// FakeSidecarServer; inbound rides PushIngress line semantics — no sockets,
// no OS children, headless.

import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import {
	CallbackQueryRouter,
	ActionHandlerRegistry,
	ClarifyPendingStore,
	OneShotPendingStore,
	PLAIN_TEXT_FALLBACK_PREFIX,
	resolveEnablement,
	FormattingLadder,
} from "../kit/index.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import type {
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import { PhotonAdapter } from "./photon-adapter.js";
import type { ProcessingOutcome } from "./photon-adapter.js";
import type { PhotonCaptureWire } from "./photon-adapter.js";
import type { FakeSidecarServer } from "./sidecar-wire.js";
import type { ManualClock } from "../persistent-ws/manual-clock.js";

/** Scoped harness credentials (fail-closed reader shape; never process.env). */
export const FIXTURE_PHOTON_PROJECT_ID = "test-project-id";
export const FIXTURE_PHOTON_PROJECT_SECRET = "test-project-secret";

export interface PhotonSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	sidecar: FakeSidecarServer;
	clock: ManualClock;
	spawner?: TaskSpawner | undefined;
	scheduler?: ManualScheduler | undefined;
	/** Harness-scale chunk budget (mirrors the reference subjects' 64). */
	scalarMaxUnits?: number | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	/** Per-test PHOTON_* env overrides (scoped; never process.env). */
	env?: Record<string, string | undefined> | undefined;
	/** Respawn seam (watchdog rows record signals). */
	onRespawn?: ((reason: string) => void | Promise<void>) | undefined;
	/** Fatal-notification seam (self-cancel row). */
	notifyFatalError?: (() => Promise<void>) | undefined;
	/** Retry-ladder sleep injection (latency-free rows). */
	sleepFn?: ((ms: number) => Promise<void>) | undefined;
}

/**
 * Capture seam (subject-supplied): models the markdown-RENDERING rejection
 * script (`forceFormattingError`) exactly like the reference fixtures — the
 * §6.1 plain-text fallback body succeeds on the wire — while recording every
 * user-visible transmission into FakePlatformWire.ops and driving the REAL
 * /send through the fake sidecar.
 */
function makeCaptureWire(
	raw: FakePlatformWire,
	engineRef: { current: PhotonAdapter | null },
): PhotonCaptureWire {
	return {
		transmitSend: async (
			chatId: string,
			content: string,
			metadata: Record<string, unknown>,
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
			// The harness wire script is the OUTCOME AUTHORITY (flood/timeout
			// scripts behave exactly like the reference subjects); a successful
			// modeled send ALSO flows through the REAL /send so the sidecar's
			// call capture stays truthful (dual observability, both fakes agree).
			const result = await raw.transmitSend(chatId, content, metadata);
			if (!result.success) return result;
			const real = await engineRef.current?.sidecarSend(chatId, content);
			return real ?? result;
		},
		hasRichScript: (opKind: string) => raw.hasScript(opKind as "send"),
		transmitRich: async (
			chatId: string,
			content: string,
		): Promise<SendResult> => raw.transmitRich(chatId, content, {}),
	};
}

/** The photon-shaped ConformanceSubject over the REAL adapter. */
export class PhotonSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: PhotonAdapter;
	readonly wire: FakePlatformWire;
	readonly sidecar: FakeSidecarServer;
	readonly clock: ManualClock;

	private readonly approvalsStore = new OneShotPendingStore();
	private readonly slashConfirmsStore = new OneShotPendingStore();
	private readonly apprStore = new OneShotPendingStore();
	private readonly clarifyStore = new ClarifyPendingStore();
	private readonly actions = new ActionHandlerRegistry();
	private readonly resolvedFamilyLog: string[] = [];
	private routerField: CallbackQueryRouter | null = null;
	private allowClickers = true;

	constructor(opts: PhotonSubjectOptions) {
		this.name = opts.name ?? "photon";
		this.wire = opts.wire;
		this.sidecar = opts.sidecar;
		this.clock = opts.clock;
		const withSecret = opts.withSecret !== false;

		const envReader = (key: string): string | undefined => {
			if (!withSecret) {
				if (key === "PHOTON_PROJECT_ID" || key === "PHOTON_PROJECT_SECRET") {
					return undefined;
				}
			} else if (key === "PHOTON_PROJECT_ID") {
				return FIXTURE_PHOTON_PROJECT_ID;
			} else if (key === "PHOTON_PROJECT_SECRET") {
				return FIXTURE_PHOTON_PROJECT_SECRET;
			}
			return opts.env?.[key];
		};

		const engineRef: { current: PhotonAdapter | null } = { current: null };
		this.adapter = new PhotonAdapter({
			sidecar: opts.sidecar,
			nowMs: () => opts.clock.nowMs(),
			envReader,
			captureWire: makeCaptureWire(this.wire, engineRef),
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
			...(opts.onRespawn !== undefined ? { onRespawn: opts.onRespawn } : {}),
			...(opts.notifyFatalError !== undefined
				? { notifyFatalError: opts.notifyFatalError }
				: {}),
			...(opts.sleepFn !== undefined ? { sleepFn: opts.sleepFn } : {}),
		});
		engineRef.current = this.adapter;

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

	// ── streaming seam (no native lanes on iMessage) ──
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		void _chatId;
		void _draftId; // no native stream lanes on this platform
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

	// ── interactive surfaces (shared callback machinery, lazily alive) ──

	private routerInstance(): CallbackQueryRouter {
		if (this.routerField === null) {
			this.routerField = new CallbackQueryRouter({
				stores: {
					approvals: this.approvalsStore,
					slashConfirms: this.slashConfirmsStore,
					appr: this.apprStore,
					clarify: this.clarifyStore,
				},
				authorizer: () => this.allowClickers,
				onExecApproval: async () => {
					this.resolvedFamilyLog.push("ea");
					return "ok";
				},
				onSlashConfirm: async () => {
					this.resolvedFamilyLog.push("sc");
					return "ok";
				},
				onClarifyChoice: async (_k, _id, idx) => {
					this.resolvedFamilyLog.push("cl");
					return `answer-${idx}`;
				},
				onWhatsappApproval: async () => {
					this.resolvedFamilyLog.push("appr");
					return "ok";
				},
				onPickerNav: async (parsed) => ({
					answerText: `nav:${parsed.family}`,
				}),
			});
		}
		return this.routerField;
	}

	callbackRouter(): CallbackQueryRouter | null {
		return this.routerInstance();
	}
	actionRegistry(): ActionHandlerRegistry {
		void this.routerInstance(); // both surfaces alive together
		return this.actions;
	}
	registerApprovalPending(id: number, sessionKey: string): void {
		void this.routerInstance();
		this.approvalsStore.register(id, sessionKey);
	}
	registerSlashConfirmPending(id: number, sessionKey: string): void {
		void this.routerInstance();
		this.slashConfirmsStore.register(id, sessionKey);
	}
	registerClarifyPending(id: number, sessionKey: string): void {
		void this.routerInstance();
		this.clarifyStore.register(id, sessionKey);
	}
	registerApprPending(id: number, sessionKey: string): void {
		void this.routerInstance();
		this.apprStore.register(id, sessionKey);
	}
	setClickerAuthorization(allow: boolean): void {
		this.allowClickers = allow;
	}
	resolvedFamilies(): readonly string[] {
		return this.resolvedFamilyLog;
	}
	resolvedTurnDispatches(): readonly string[] {
		return [];
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
				name: "photon-scoped-probe",
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

	/** Processing-outcome probe for reaction-lifecycle rows. */
	async runProcessingLifecycle(
		event: IncomingEvent,
		outcome: ProcessingOutcome,
	): Promise<{ started: boolean; completed: boolean[] }> {
		const started = await this.adapter.onProcessingStart(event);
		const completed = await this.adapter.onProcessingComplete(event, outcome);
		return { started, completed };
	}
}

export function makePhotonSubject(opts: PhotonSubjectOptions): PhotonSubject {
	return new PhotonSubject(opts);
}
