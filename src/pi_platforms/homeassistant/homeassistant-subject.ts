// pi_platforms/homeassistant/homeassistant-subject — the Home Assistant
// adapter wired as a ConformanceSubject (04 §8 merge-gate wiring). The REAL
// HomeAssistantAdapter engine runs underneath — egress capture rides the
// SHARED harness wire (FakePlatformWire) through a subject-level REST bridge
// over the dedicated notification session; the ws event plane rides the
// in-process FakeHaServer under an injected ManualClock. Guard/router
// plumbing is copied VERBATIM from the reference subjects (msgraph/persistent-ws).

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
import { PLAIN_TEXT_FALLBACK_PREFIX } from "../kit/index.js";

import {
	HomeAssistantAdapter,
	type HaClock,
	type HaRestOutcome,
	type HaRestSession,
} from "./homeassistant-adapter.js";
import { FakeHaServer } from "./ha-fake-server.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { HA_PLUGIN_MANIFEST, type HaWatchConfig } from "./manifest.js";

export interface HaSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	spawner?: ManualScheduler["spawner"] | undefined;
	/** Harness-stamped deterministic scheduler for ingress rows. */
	scheduler?: ManualScheduler | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	server?: FakeHaServer | undefined;
	clock?: ManualClock | undefined;
	config?: HaWatchConfig | undefined;
	scalarMaxUnits?: number | undefined;
	/**
	 * Lie-scan fixture seam ONLY: flips THE manifest datum that drives the
	 * streaming-exclusion probe so the negative gate can prove a lying
	 * capability claim FAILS the streaming family rows. Never set in production.
	 */
	declaredMessageEditing?: boolean | undefined;
}

/** The fixed error text wire.ts' timeout behavior surfaces (fixture contract). */
const WIRE_TIMEOUT_SENTINEL = "request timed out";

/**
 * Subject-level REST bridge: models the markdown-RENDERING rejection script
 * (`forceFormattingError`) exactly like the reference fixtures — the §6.1
 * plain-text fallback body succeeds on the wire — while recording every
 * user-visible transmission into FakePlatformWire.ops AND the fake server's
 * REST recorder. Wire-scripted failures flow through as transport-failures;
 * server-scripted REST outcomes (engine rows) layer on top of successful
 * posts.
 */
function makeSubjectRestSession(
	raw: FakePlatformWire,
	ha: FakeHaServer,
): HaRestSession {
	return {
		async post(req, chatId, metadata): Promise<HaRestOutcome> {
			// §6.1 lane: a forced formatting error fails unless this IS already
			// the plain-text fallback body (reference-fixture parity).
			if (
				metadata["forceFormattingError"] === true &&
				!req.payload.message.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			) {
				return {
					kind: "http",
					status: 400,
					body: "Bad Request: can't parse entities",
				};
			}
			const result = await raw.transmitSend(chatId, req.payload.message, {
				...metadata,
				ha_notification_title: req.payload.title,
			});
			ha.restRequests.push({
				path: req.path,
				headers: req.headers,
				payload: req.payload,
			});
			if (!result.success) {
				if (result.error === WIRE_TIMEOUT_SENTINEL) return { kind: "timeout" };
				return {
					kind: "transport-failure",
					error: result.error ?? "notification post failed",
				};
			}
			const scripted = ha.pullRestScript();
			if (scripted !== undefined) return scripted;
			return { kind: "ok" };
		},
		hasRichScript: (opKind) => raw.hasScript(opKind as "send"),
		transmitRich: async (chatId, content) =>
			raw.transmitRich(chatId, content, {}),
	};
}

/** The homeassistant-shaped ConformanceSubject over the REAL engine. */
export class HomeAssistantSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: HomeAssistantAdapter;
	readonly wire: FakePlatformWire;
	readonly server: FakeHaServer;
	readonly clock: ManualClock;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: HaSubjectOptions) {
		this.name = opts.name ?? "homeassistant-reference";
		this.wire = opts.wire;
		this.server = opts.server ?? new FakeHaServer();
		this.clock = opts.clock ?? new ManualClock();
		const withSecret = opts.withSecret !== false;
		const clock: HaClock = {
			nowMs: this.clock.nowMs,
			sleepMs: this.clock.sleepMs,
		};

		this.adapter = new HomeAssistantAdapter({
			ws: this.server,
			rest: makeSubjectRestSession(this.wire, this.server),
			clock,
			scalarMaxUnits: opts.scalarMaxUnits ?? 64, // harness-scale budget mirrors the reference subjects
			config: opts.config,
			secretReader: (key) =>
				withSecret
					? key === "HASS_TOKEN"
						? "ha-long-lived-token"
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
		void _draftId; // no native stream lanes on Home Assistant
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
		return [...this.adapter.routerAuditResolved()];
	}

	// ── identity/secrets probes ──
	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		if (!this.lockHeld) {
			const first = this.adapter.acquireCredentialLock(
				this.lockManager,
				"hass-token",
				"cred-ha-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"hass-token",
				"cred-ha-1",
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf("hass-token", "cred-ha-1");
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
				name: "ha-scoped-probe",
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
		return HA_PLUGIN_MANIFEST;
	}
}

export function makeHaSubject(
	opts: HaSubjectOptions & { wire: FakePlatformWire },
): HomeAssistantSubject {
	return new HomeAssistantSubject(opts);
}

// Re-exported for the wiring suite's world factories.
export { FakeHaServer, ManualClock };
