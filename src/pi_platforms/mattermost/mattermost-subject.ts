// pi_platforms/mattermost/mattermost-subject — the Mattermost adapter wired
// as a ConformanceSubject (04 §8): egress capture rides the SHARED harness
// wire (FakePlatformWire) through the adapter's bound create/patch lanes, the
// WS transport + REST backfill ride the fake MM server, and every row surface
// mirrors the persistent-ws subjects. The REAL MattermostAdapterCore runs
// underneath — no stubs.

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
	PLAIN_TEXT_FALLBACK_PREFIX,
} from "../kit/index.js";
import type {
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import type { FakePlatformWire } from "../conformance/wire.js";
import type { ConformanceSubject } from "../conformance/harness.js";
import { SCHEDULER_SYMBOL } from "../conformance/harness.js";

import { MattermostAdapterCore } from "./mattermost-adapter.js";
import type { FakeMattermost } from "./mm-fake-server.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";

export interface MattermostSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	mm: FakeMattermost;
	clock?: ManualClock | undefined;
	spawner?: TaskSpawner | undefined;
	scheduler?: ManualScheduler | undefined;
	scalarMaxUnits?: number | undefined;
	withSecret?: boolean | undefined;
	replyMode?: "thread" | "off" | undefined;
	requireMention?: boolean | undefined;
	freeResponseChannels?: ReadonlySet<string> | undefined;
	allowedChannels?: ReadonlySet<string> | undefined;
	pingIntervalMs?: number | undefined;
	watchdogIntervalMs?: number | undefined;
	firstPingGraceMs?: number | undefined;
}

/**
 * Subject-level REST plane wrapper: models the markdown-RENDERING rejection
 * script (forceFormattingError) exactly like the reference fixtures; every
 * other op delegates to the shared harness wire for egress capture.
 */
function wrapCreateLane(raw: FakePlatformWire) {
	return async (payload: Record<string, unknown>) => {
		const chatId = String(payload["channel_id"] ?? "");
		const content = String(payload["message"] ?? "");
		const metadata = (payload["metadata"] ?? {}) as Metadata;
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
		) {
			throw Object.assign(new Error("Bad Request: can't parse entities"), {
				status: 400,
			});
		}
		const result = await raw.transmitSend(chatId, content, {
			...metadata,
			mm_payload: payload,
		});
		if (!result.success) {
			throw Object.assign(new Error(result.error ?? "send failed"), {
				status: 500,
			});
		}
		return { id: result.messageId ?? "wire-post" };
	};
}

/**
 * The Mattermost-shaped ConformanceSubject over the REAL adapter engine.
 */
export class MattermostSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: MattermostAdapterCore;
	readonly wire: FakePlatformWire;
	readonly mm: FakeMattermost;
	readonly clock: ManualClock;

	constructor(opts: MattermostSubjectOptions) {
		this.name = opts.name ?? "mattermost";
		this.wire = opts.wire;
		this.mm = opts.mm;
		this.clock = opts.clock ?? new ManualClock();
		const withSecret = opts.withSecret !== false;

		this.adapter = new MattermostAdapterCore({
			mm: opts.mm,
			clock: this.clock,
			...(opts.spawner !== undefined ? { spawner: opts.spawner } : {}),
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
			manifestName: this.name,
			replyMode: opts.replyMode,
			requireMention: opts.requireMention,
			freeResponseChannels: opts.freeResponseChannels,
			allowedChannels: opts.allowedChannels,
			...(opts.pingIntervalMs !== undefined
				? { pingIntervalMs: opts.pingIntervalMs }
				: {}),
			...(opts.watchdogIntervalMs !== undefined
				? { watchdogIntervalMs: opts.watchdogIntervalMs }
				: {}),
			...(opts.firstPingGraceMs !== undefined
				? { firstPingGraceMs: opts.firstPingGraceMs }
				: {}),
			secretReader: (key) =>
				withSecret
					? key === "MATTERMOST_URL"
						? "https://mm.fake.example"
						: key === "MATTERMOST_TOKEN"
							? "mm-fake-token"
							: undefined
					: undefined,
		});

		// Bind the engine's egress transports to the shared harness wire.
		const rawWire = this.wire;
		this.adapter.bindWire(async (payload, wireMetadata) => {
			const chatId = String(payload["channel_id"] ?? "");
			const content = String(payload["message"] ?? "");
			const forceFormattingError =
				wireMetadata["forceFormattingError"] === true;
			if (
				forceFormattingError &&
				!content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			) {
				const err = new Error("Bad Request: can't parse entities");
				throw Object.assign(err, { status: 400 });
			}
			const result = await rawWire.transmitSend(chatId, content, {
				...wireMetadata,
				mm_props: payload["props"],
				mm_root_id: payload["root_id"],
				mm_file_ids: payload["file_ids"],
			});
			if (!result.success) {
				throw Object.assign(new Error(result.error ?? "send failed"), {
					status: result.retryAfter != null ? 429 : 500,
					retryAfterSeconds: result.retryAfter,
					scriptedRetryable: result.retryable,
				});
			}
			return { id: result.messageId ?? `wire-${rawWire.ops.length}` };
		});
		this.adapter.bindPatch(
			async (chatId, postId, message, finalize, streamMeta) => {
				const patched = await rawWire.transmitDraft(
					chatId,
					postId.length,
					message,
					finalize,
					{ mm_patch_post_id: postId, ...streamMeta },
				);
				if (!patched.success) {
					throw Object.assign(new Error(patched.error ?? "patch failed"), {
						status: 500,
					});
				}
			},
		);
		this.adapter.bindTyping(async () => {});
		this.adapter.richScriptedProbe = () => this.wire.hasScript("rich");
		this.adapter.wireTransmitRich = (content, metadata) =>
			rawWire.transmitRich("__rich__", content, metadata);

		this.adapter.attachStandardGuard(opts.spawner);
		if (opts.scheduler !== undefined) {
			(this as unknown as Record<symbol, unknown>)[SCHEDULER_SYMBOL] =
				opts.scheduler;
		}
		void wrapCreateLane;
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
		return resolveEnablement(
			{
				name: "mm-scoped-probe",
				description: "",
				transportShape: "ws",
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

export function makeMattermostSubject(
	opts: MattermostSubjectOptions & {
		wire: FakePlatformWire;
		mm: FakeMattermost;
	},
): MattermostSubject {
	return new MattermostSubject(opts);
}
