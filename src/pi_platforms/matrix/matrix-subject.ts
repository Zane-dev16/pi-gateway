// pi_platforms/matrix/matrix-subject — the Matrix adapter wired as a
// ConformanceSubject (04 §8): egress capture rides the SHARED harness wire
// (FakePlatformWire), the sync transport rides the fake homeserver, and every
// row surface mirrors the polling/webhook subjects. The REAL MatrixAdapterCore
// engine runs underneath — no stubs.

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

import { MatrixAdapterCore } from "./matrix-adapter.js";
import type { FakeMatrixHomeserver } from "./matrix-fake-server.js";
import { ManualPollingClock } from "../polling/clock.js";

export interface MatrixSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	hs: FakeMatrixHomeserver;
	clock?: ManualPollingClock | undefined;
	spawner?: TaskSpawner | undefined;
	scheduler?: ManualScheduler | undefined;
	scalarMaxUnits?: number | undefined;
	/** Tiny wall budget for parked long-polls in tests (family pattern). */
	syncLongPollTimeoutMs?: number | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
	isKnownCommand?: ((name: string) => boolean) | undefined;
	requireMention?: boolean | undefined;
	freeRooms?: ReadonlySet<string> | undefined;
	allowedRooms?: ReadonlySet<string> | undefined;
}

/** Shared-row budgets stay SMALL (reference parity); production default 16000. */
const SHARED_ROW_BUDGET_UNITS = 64;

/**
 * The Matrix-shaped ConformanceSubject over the REAL adapter engine.
 */
export class MatrixSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: MatrixAdapterCore;
	readonly wire: FakePlatformWire;
	readonly hs: FakeMatrixHomeserver;
	readonly clock: ManualPollingClock;

	constructor(opts: MatrixSubjectOptions) {
		this.name = opts.name ?? "matrix";
		this.wire = opts.wire;
		this.hs = opts.hs;
		this.clock = opts.clock ?? new ManualPollingClock();
		const withSecret = opts.withSecret !== false;

		this.adapter = new MatrixAdapterCore({
			hs: opts.hs,
			clock: opts.clock,
			timer: opts.clock?.timer,
			...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
			scalarMaxUnits: opts.scalarMaxUnits ?? SHARED_ROW_BUDGET_UNITS,
			syncLongPollTimeoutMs: opts.syncLongPollTimeoutMs ?? 25,
			manifestName: this.name,
			secretReader: (key) =>
				withSecret
					? key === "MATRIX_HOMESERVER"
						? "https://matrix.fake.example"
						: key === "MATRIX_ACCESS_TOKEN"
							? "syt_fake_token"
							: undefined
					: undefined,
			...(opts.isKnownCommand !== undefined
				? { isKnownCommand: opts.isKnownCommand }
				: {}),
			...(opts.requireMention !== undefined
				? { requireMention: opts.requireMention }
				: {}),
			...(opts.freeRooms !== undefined ? { freeRooms: opts.freeRooms } : {}),
			...(opts.allowedRooms !== undefined
				? { allowedRooms: opts.allowedRooms }
				: {}),
		});

		// Bind the engine's egress transports to the shared harness wire.
		this.adapter.wireTransmitSend = (chatId, content, metadata) =>
			this.wire.transmitSend(chatId, content, metadata);
		this.adapter.wireTransmitEdit = (chatId, messageId, content, metadata) =>
			this.wire.transmitEdit(chatId, messageId, content, metadata);
		this.adapter.richScriptedProbe = () => this.wire.hasScript("rich");
		this.adapter.wireTransmitRich = (content, metadata) =>
			this.wire.transmitRich("__rich__", content, metadata);
		this.adapter.wireTransmitDraft = (
			chatId,
			draftId,
			content,
			final,
			metadata,
		) => this.wire.transmitDraft(chatId, draftId, content, final, metadata);

		this.adapter.attachStandardGuard(opts.spawner);
		if (opts.scheduler !== undefined) {
			(this as unknown as Record<symbol, unknown>)[SCHEDULER_SYMBOL] =
				opts.scheduler;
		}
	}

	// ── observability ───────────────────────────────────────────────────────

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

	// ── ingress lane ────────────────────────────────────────────────────────

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

	// ── egress lanes ────────────────────────────────────────────────────────

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
	transientRichFailureOutcome(
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

	// ── streaming seam ──────────────────────────────────────────────────────

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

	// ── interactive surfaces ────────────────────────────────────────────────

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

	// ── identity/secrets probes ──────────────────────────────────────────────

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
				name: "matrix-scoped-probe",
				description: "",
				transportShape: "polling",
				requiresEnv: [{ name: envKey }],
				capabilities: {},
			},
			() => undefined,
		).enabled;
	}

	// ── DEC-022 declaration ─────────────────────────────────────────────────

	wakeLaneDeclaration(): "forged-event" | "raw-key-direct" {
		return this.adapter.wakeLane;
	}
}

export function makeMatrixSubject(
	opts: MatrixSubjectOptions & {
		wire: FakePlatformWire;
		hs: FakeMatrixHomeserver;
	},
): MatrixSubject {
	return new MatrixSubject(opts);
}
