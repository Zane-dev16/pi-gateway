// pi_platforms/buzz/buzz-subject — the Buzz adapter wired as a
// ConformanceSubject (04 §8 merge-gate wiring). Shared rows run against the
// REAL kit-built adapter with FakePlatformWire egress capture; the CLI polling
// transport rides FakeBuzzCli (injected executor — NO OS children, NO
// sockets); every row surface mirrors the raft/matrix subjects.

import {
	TokenLockManagerSeam,
	resolveEnablement,
	FormattingLadder,
} from "../kit/index.js";
import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
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

import { BuzzAdapter } from "./buzz-adapter.js";
import type { FakeBuzzCli } from "./cli-wire.js";
import { validateBuzzTrustBoundary } from "./manifest.js";
import { FIXED_NSEC } from "./vectors.js";

export interface BuzzSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	cli: FakeBuzzCli;
	spawner?: TaskSpawner | undefined;
	/** Harness-stamped deterministic scheduler for ingress rows. */
	scheduler?: ManualScheduler | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	scalarMaxUnits?: number | undefined;
}

/** Fixture identity: the relay URL and the nsec whose pubkey owns the relay. */
export const FIXTURE_BUZZ_RELAY = "https://fake.buzz.example";
export const FIXTURE_BUZZ_NSEC = FIXED_NSEC;

/**
 * Capture seam: models the markdown-RENDERING rejection script
 * (`forceFormattingError`) exactly like the reference fixtures while recording
 * every user-visible transmission into FakePlatformWire.ops.
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
				return { success: false, error: "Bad Request: can't parse entities" };
			}
			return raw.transmitSend(chatId, content, metadata);
		},
	};
}

/** The buzz-shaped ConformanceSubject over the REAL adapter. */
export class BuzzSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: BuzzAdapter;
	readonly wire: FakePlatformWire;
	readonly cli: FakeBuzzCli;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	/** Exposed for contention fixtures (rival adapters share the registry). */
	readonly getLockManager = (): TokenLockManagerSeam => this.lockManager;
	private lockHeld = false;

	constructor(opts: BuzzSubjectOptions) {
		this.name = opts.name ?? "buzz";
		this.wire = opts.wire;
		this.cli = opts.cli;
		const withSecret = opts.withSecret !== false;
		const secretReader = (key: string): string | undefined => {
			if (!withSecret) return undefined;
			if (key === "BUZZ_PRIVATE_KEY") return FIXTURE_BUZZ_NSEC;
			if (key === "BUZZ_RELAY_URL") return FIXTURE_BUZZ_RELAY;
			return undefined;
		};

		this.adapter = new BuzzAdapter({
			config: { cli_path: "/usr/local/bin/buzz" },
			pathProbes: { fileExists: () => true },
			secretReader,
			executor: opts.cli.executor(),
			nowMs: () => opts.cli.nowSeconds * 1000,
			lockManager: this.lockManager,
			lockOwner: `${this.name}-instance`,
			scalarMaxUnits: opts.scalarMaxUnits ?? 64, // harness-scale budget mirrors reference subjects
		});

		// Bind the harness egress lanes onto the engine's transport seams
		// (mattermost/matrix precedent): sends record through the shared wire
		// with the formatting-rejection script; rich only when scripted.
		const capture = makeCaptureWire(opts.wire);
		this.adapter.wireTransmitSend = (chatId, content, metadata) =>
			capture.transmitSend(
				chatId,
				content,
				metadata as Record<string, unknown>,
			);
		this.adapter.wireTransmitRich = (content, metadata) =>
			opts.wire.transmitRich("__rich__", content, metadata);
		this.adapter.richScriptedProbe = () => opts.wire.hasScript("rich");

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
		void _chatId; // fresh ladder lane against a TRANSIENT rich failure
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

	// ── streaming seam (no native lanes on the CLI plane) ──
	streamAdapter(): StreamEgressAdapter {
		return this.adapter as unknown as StreamEgressAdapter;
	}
	async armOpenNativeStream(_chatId: string, _draftId: number): Promise<void> {
		void _chatId;
		void _draftId; // no native stream lanes
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
	callbackRouter() {
		return this.adapter.router;
	}
	actionRegistry() {
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
				"buzz-conformance-identity",
				`${FIXTURE_BUZZ_RELAY}:${this.cli.selfPubkey}`,
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.adapter.acquireCredentialLock(
				this.lockManager,
				"buzz-conformance-identity",
				`${FIXTURE_BUZZ_RELAY}:${this.cli.selfPubkey}`,
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf(
				"buzz-conformance-identity",
				`${FIXTURE_BUZZ_RELAY}:${this.cli.selfPubkey}`,
			);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}
	missingSecretSubjectLifecycle(): AdapterStatusSnapshot {
		const sibling = new BuzzAdapter({
			secretReader: () => undefined,
		});
		return sibling.lifecycle.statusSnapshot();
	}
	resolveEnablementIgnoringProcessEnv(envKey: string): boolean {
		// The scoped reader NEVER consults process.env — a scoped miss is
		// terminal even when the variable exists in the environment.
		return resolveEnablement(
			{
				name: "buzz-scoped-probe",
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

	// ── trust boundary probe ──
	flagsAndTrustProbe(): { trustBoundaryComplete: boolean } {
		return {
			trustBoundaryComplete:
				validateBuzzTrustBoundary(this.adapter.trustBoundary).length === 0,
		};
	}
}

export function makeBuzzSubject(
	opts: BuzzSubjectOptions & { wire: FakePlatformWire; cli: FakeBuzzCli },
): BuzzSubject {
	return new BuzzSubject(opts);
}
