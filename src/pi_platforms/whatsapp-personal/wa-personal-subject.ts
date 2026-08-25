// pi_platforms/whatsapp-personal/wa-personal-subject — the WhatsApp PERSONAL
// adapter wired as a ConformanceSubject (04 §8 merge-gate wiring). Shared rows
// run against the REAL kit-built adapter with FakePlatformWire egress capture;
// the bridge plane (poll/read-receipt lanes) is exercised by the world fixture
// against FakeBridgeServer directly. No stubs — the real engine runs beneath.

import {
	FormattingLadder,
	TokenLockManagerSeam,
	resolveEnablement,
} from "../kit/index.js";
import { mkdtempSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { CallbackQueryRouter } from "../kit/callback-router.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import { PLAIN_TEXT_FALLBACK_PREFIX } from "../kit/send-retry.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import {
	SCHEDULER_SYMBOL,
	type ConformanceSubject,
} from "../conformance/harness.js";

import { WaPersonalAdapter } from "./wa-personal-adapter.js";
import {
	FakeBridgeServer,
	type BridgeCallEnvelope,
	type BridgeTransport,
} from "./bridge-wire.js";
import { ManualPollingClock } from "../polling/clock.js";

export interface WaPersonalSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	/** Shared bridge server (inbound queue + scripted failures). */
	bridge?: FakeBridgeServer | undefined;
	clock?: ManualPollingClock | undefined;
	spawner?: ManualScheduler["spawner"] | undefined;
	/** Harness-stamped deterministic scheduler for ingress rows. */
	scheduler?: ManualScheduler | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	/** Explicit reader override (delta rows: mutable-env map). */
	secretReader?: ScopedSecretReader | undefined;
	/** config.extra passthrough (gating/debounce scenarios). */
	config?: Record<string, unknown> | undefined;
	aliasResolver?: import("./behavior.js").AliasResolver | undefined;
	credsPresent?: boolean | undefined;
	nodePresent?: boolean | undefined;
	bridgeScriptPresent?: boolean | undefined;
	scalarMaxUnits?: number | undefined;
	/**
	 * Present a CONNECTED adapter to the shared egress doors (default true).
	 * Delta rows that exercise the connect LADDER itself set false.
	 */
	autoConnect?: boolean | undefined;
}

/**
 * Composite transport: inbound/failure scripting rides the FakeBridgeServer
 * while every USER-VISIBLE transmission lands in FakePlatformWire.ops (the
 * harness egress-capture contract). Read receipts are UX polish, not
 * user-visible transmissions — captured on the server only.
 */
class HarnessBridgeTransport implements BridgeTransport {
	constructor(
		private readonly server: FakeBridgeServer,
		private readonly raw: FakePlatformWire,
	) {}

	async getMessages(): Promise<BridgeCallEnvelope> {
		return this.server.getMessages();
	}

	async sendText(
		payload: {
			chatId: string;
			message: string;
			replyTo?: string | undefined;
		},
		metadata?: Record<string, unknown>,
	): Promise<BridgeCallEnvelope> {
		// Server-side failure scripting first (bridge-down / HTTP errors);
		// then the formatting-rejection marker models the markdown-rendering
		// rejection EXACTLY like the reference fixtures: non-plain-fallback
		// bodies fail, the §6.1 plain-text lane succeeds on the wire.
		const scripted = this.server.nextFailure("send");
		if (scripted !== undefined) return FakeBridgeServer.shapeFailure(scripted);
		if (
			metadata?.["forceFormattingError"] === true &&
			!payload.message.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
		) {
			// Vendor-shaped markdown-rendering rejection (classifySendError maps
			// this message onto the §6.1 formatting class).
			return { status: 400, text: "Bad Request: can't parse entities" };
		}
		const result = await this.raw.transmitSend(
			payload.chatId,
			payload.message,
			{},
		);
		return result.success
			? { status: 200, json: { messageId: result.messageId ?? "wamid.bridge" } }
			: { status: 500, text: result.error ?? "send failed" };
	}

	async editMessage(payload: {
		chatId: string;
		messageId: string;
		message: string;
	}): Promise<BridgeCallEnvelope> {
		const result = await this.raw.transmitEdit(
			payload.chatId,
			payload.messageId,
			payload.message,
			{},
		);
		return result.success
			? { status: 200, json: {} }
			: { status: 500, text: result.error ?? "edit failed" };
	}

	async markRead(key: Record<string, unknown>): Promise<{ status: number }> {
		return this.server.markRead(key);
	}

	async getHealthStatus(): Promise<BridgeCallEnvelope> {
		return this.server.getHealthStatus();
	}
}

/** Shared-row budgets stay SMALL (reference parity). */
const SHARED_ROW_BUDGET_UNITS = 64;

/** Scoped harness secrets (fail-closed reader shape; never process.env). */
function waHarnessSecret(name: string): string | undefined {
	switch (name) {
		case "WHATSAPP_ENABLED":
			return "true";
		default:
			return undefined;
	}
}

/**
 * One throwaway LID-mapping dir per WORKER PROCESS: session-key construction
 * walks the identity module on every dispatch, so subjects must NEVER read
 * the real PI_HOME session dir (mkdtemp isolation).
 */
let harnessSessionDirValue: string | undefined;
function harnessSessionDir(): string {
	if (harnessSessionDirValue === undefined) {
		harnessSessionDirValue = mkdtempSync(
			join(tmpdir(), "wa-personal-subject-"),
		);
	}
	return harnessSessionDirValue;
}

/** The WhatsApp-PERSONAL-shaped ConformanceSubject over the REAL adapter. */
export class WaPersonalSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: WaPersonalAdapter;
	readonly wire: FakePlatformWire;
	readonly bridge: FakeBridgeServer;
	readonly clock: ManualPollingClock;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: WaPersonalSubjectOptions) {
		this.name = opts.name ?? "whatsapp-personal";
		this.wire = opts.wire;
		this.bridge = opts.bridge ?? new FakeBridgeServer();
		this.clock = opts.clock ?? new ManualPollingClock();
		const withSecret = opts.withSecret !== false;
		const secretReader: ScopedSecretReader =
			opts.secretReader ??
			((key: string) => (withSecret ? waHarnessSecret(key) : undefined));

		this.adapter = new WaPersonalAdapter({
			transport: new HarnessBridgeTransport(this.bridge, this.wire),
			clock: this.clock,
			scalarMaxUnits: opts.scalarMaxUnits ?? SHARED_ROW_BUDGET_UNITS,
			secretReader,
			whatsappSessionDir: harnessSessionDir(), // NEVER the real home
			...(opts.config !== undefined ? { config: opts.config } : {}),
			...(opts.aliasResolver !== undefined
				? { aliasResolver: opts.aliasResolver }
				: {}),
			...(opts.credsPresent === undefined
				? {}
				: { credsPresent: opts.credsPresent }),
			...(opts.nodePresent === undefined
				? {}
				: { nodePresent: opts.nodePresent }),
			...(opts.bridgeScriptPresent === undefined
				? {}
				: { bridgeScriptPresent: opts.bridgeScriptPresent }),
		});
		this.adapter.bindRichProbe({
			hasScript: (opKind) => this.wire.hasScript(opKind as "send"),
			transmitRich: async (chatId, content) =>
				this.wire.transmitRich(chatId, content, {}),
		});
		this.adapter.attachStandardGuard(opts.spawner);
		// Shared egress rows exercise the doors WITHOUT an explicit connect —
		// present a CONNECTED adapter (source gates wireSend on _running).
		// connect() has no await before the flag flips, so this settles
		// synchronously; a loud-disabled sibling simply refuses (caught below).
		if (opts.autoConnect !== false) {
			void this.adapter.connect({ isReconnect: false }).catch(() => undefined);
		}
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

	// ── streaming seam (no native draft lanes on the bridge wire) ──
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		void _chatId;
		void _draftId; // no native stream lanes on the polling bridge shape
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
		// The router NEVER dispatches turns for stale/unknown taps.
		return [];
	}

	// ── identity/secrets probes ──
	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		if (!this.lockHeld) {
			const first = this.adapter.acquireCredentialLock(
				this.lockManager,
				"whatsapp-personal-creds",
				"cred-wa-p-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"whatsapp-personal-creds",
				"cred-wa-p-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"whatsapp-personal-creds",
				"cred-wa-p-1",
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new WaPersonalAdapter({
			transport: new FakeBridgeServer(),
			secretReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "wa-scoped-probe",
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
}

export function makeWaPersonalSubject(
	opts: WaPersonalSubjectOptions & { wire: FakePlatformWire },
): WaPersonalSubject {
	return new WaPersonalSubject(opts);
}
