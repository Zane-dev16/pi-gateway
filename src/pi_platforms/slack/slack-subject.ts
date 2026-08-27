// pi_platforms/slack/slack-subject — the Slack port wired as a
// ConformanceSubject (04 §8 merge gate; DEC-032 posture). Mirrors the
// persistent-ws subject 1:1: egress capture rides the SHARED harness wire
// (FakePlatformWire) through a subject-level RestPlane wrapper, the transport
// rides the Socket-Mode-shaped fake server, and every row surface matches the
// polling/webhook/ws subjects. The REAL SlackAdapter runs underneath — no
// stubs.
//
// Subject-level scalar budget defaults to a small HARNESS value (64 units) so
// the shared splitting/chunking rows exercise the mechanics; the REAL 39000
// cap is MANIFEST DATA (SLACK_MAX_MESSAGE_UNITS) asserted separately.

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
	MessageHandler,
} from "../../pi_gateway/guards/index.js";
import type { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "../conformance/wire.js";
import type { ConformanceSubject } from "../conformance/harness.js";
import { SCHEDULER_SYMBOL } from "../conformance/harness.js";

import {
	SlackAdapter,
	SLACK_REGISTRY,
	type SlackAdapterDeps,
	type SlackRestExtras,
	type SlackAuthIdentity,
} from "./slack-adapter.js";
import { SLACK_MANIFEST } from "./manifest.js";
import type { RateBudget } from "../kit/capabilities.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import type { ReconnectLadderOptions } from "../persistent-ws/reconnect-ladder.js";
import {
	SlackSocketModeServer,
	type SlackInteractivePayload,
} from "./fake-socket-mode.js";

export interface SlackSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	ws?: SlackSocketModeServer | undefined;
	clock?: ManualClock | undefined;
	spawner?: TaskSpawner | undefined;
	scheduler?: ManualScheduler | undefined;
	/** Harness-splitting budget; real cap lives in the manifest (39000). */
	scalarMaxUnits?: number | undefined;
	richBlocks?: boolean | undefined;
	rateBudget?: RateBudget | undefined;
	pingIntervalMs?: number | undefined;
	pingStaleFactor?: number | undefined;
	firstPingGraceMs?: number | undefined;
	watchdogIntervalMs?: number | undefined;
	ladder?: ReconnectLadderOptions | undefined;
	/** SLACK_REACTIONS override; unset ⇒ env decides (default true). */
	reactionsEnabled?: boolean | undefined;
	/** Deterministic auth.test identity override (tests). */
	authIdentity?: SlackAuthIdentity | undefined;
}

/**
 * Capturing wire: adds dedicated audit lanes for the Slack-specific REST
 * extensions (assistant.threads.setStatus / files_upload_v2 /
 * conversations.open / reactions.add·remove / auth.test / chat.delete) so
 * they never pollute send-op accounting.
 */
export class SlackCapturingWire extends FakePlatformWire {
	readonly statusOps: Array<{
		channelId: string;
		threadTs: string;
		status: string;
	}> = [];
	readonly uploadOps: Array<{
		channel: string;
		filename: string;
		initialComment: string;
		threadTs?: string | undefined;
	}> = [];
	/** conversations.open calls — resolved D id returned per user target. */
	readonly dmOpens: Array<{ userId: string; resolvedDmId: string | null }> = [];
	/** reactions.add/remove ops (per-turn emoji lifecycle audit). */
	readonly reactionOps: Array<{
		channelId: string;
		ts: string;
		name: string;
		action: "add" | "remove";
	}> = [];
	/** auth.test probes (identity-lane audit). */
	authTestCalls = 0;
	/** chat.delete ops (opt-in cleanup_progress lane audit). */
	readonly deleteOps: Array<{ channel: string; ts: string }> = [];
}

/** Same markdown-rejection script modeling as the reference subjects. */
function wrapWire(
	raw: FakePlatformWire,
	opts: { authIdentity?: SlackAuthIdentity | undefined } = {},
): SlackAdapterDeps["rest"] & SlackRestExtras {
	const capturing = raw as SlackCapturingWire;
	const authIdentityOverride = opts.authIdentity;
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
		transmitThreadStatus: async (
			channelId: string,
			threadTs: string,
			status: string,
		): Promise<void> => {
			capturing.statusOps.push({ channelId, threadTs, status });
		},
		transmitUpload: async (op: {
			channel: string;
			filename: string;
			initialComment: string;
			threadTs?: string | undefined;
		}): Promise<SendResult> => {
			capturing.uploadOps.push(op);
			return raw.transmitSend(op.channel, op.initialComment, {
				files_upload_v2: true,
				filename: op.filename,
				...(op.threadTs !== undefined ? { thread_ts: op.threadTs } : {}),
			});
		},
		openDirectMessage: async (userId: string): Promise<string | null> => {
			// conversations.open {users} → channel.id (im:write scope modeled).
			const resolvedDmId = `D${userId.replace(/^[UW]/, "")}`;
			capturing.dmOpens.push({ userId, resolvedDmId });
			return resolvedDmId;
		},
		transmitReaction: async (
			channelId: string,
			ts: string,
			name: string,
			action: "add" | "remove",
		): Promise<SendResult> => {
			// reactions.add/reactions.remove {channel,timestamp,name} (:4217/:4233)
			// — their OWN method class, captured off the send-op accounting.
			capturing.reactionOps.push({ channelId, ts, name, action });
			return { success: true };
		},
		authTest: async (): Promise<SlackAuthIdentity> => {
			// client.auth_test :1968 parity — token-derived identity, deterministic.
			capturing.authTestCalls += 1;
			if (authIdentityOverride !== undefined) return authIdentityOverride;
			return {
				ok: true,
				userId: "UBOTAUTH0",
				teamId: "TWORKSPACE0",
				user: "gateway-bot",
				team: "Workspace Zero",
			};
		},
		transmitDelete: async (
			channelId: string,
			messageId: string,
		): Promise<SendResult> => {
			// chat.delete {channel,ts} (:3085) — best-effort lane, always ok on
			// the fake; failures are modeled by omitting the extra entirely.
			capturing.deleteOps.push({ channel: channelId, ts: messageId });
			return { success: true, messageId };
		},
		hasScript: (opKind) => raw.hasScript(opKind),
	};
}
export class SlackSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: SlackAdapter;
	readonly wire: FakePlatformWire;
	readonly socketServer: SlackSocketModeServer;
	readonly clock: ManualClock;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	/** Deterministic hold gate over turn handlers (burst rows). */
	holding = false;
	holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};

	constructor(opts: SlackSubjectOptions) {
		this.name = opts.name ?? "slack";
		this.wire = opts.wire;
		this.socketServer = opts.ws ?? new SlackSocketModeServer();
		this.clock = opts.clock ?? new ManualClock();

		const deps: SlackAdapterDeps = {
			manifestName: this.name,
			transport: this.socketServer,
			rest: wrapWire(opts.wire, { authIdentity: opts.authIdentity }),
			clock: this.clock,
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
			...(opts.richBlocks !== undefined ? { richBlocks: opts.richBlocks } : {}),
			...(opts.rateBudget !== undefined ? { rateBudget: opts.rateBudget } : {}),
			requiresEnv: SLACK_MANIFEST.requiresEnv,
			secretReader: (key: string): string | undefined =>
				key === "SLACK_BOT_TOKEN"
					? "xoxb-fake-bot-token"
					: key === "SLACK_APP_TOKEN"
						? "xapp-fake-app-token"
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
			...(opts.reactionsEnabled !== undefined
				? { reactionsEnabled: opts.reactionsEnabled }
				: {}),
		};
		this.adapter = new SlackAdapter(deps);
		const adapter = this.adapter;

		const messageHandler: MessageHandler = async (event, ctx) => {
			const text = event.text ?? `[${String(event.messageType)}]`;
			const sessionKey = String(event.metadata?.["gateway_session_key"] ?? "");
			if (adapter.clarifyArmed.has(sessionKey) && !text.startsWith("/")) {
				adapter.clarifyCaptures.push(text);
				return null; // consumed by the clarify resolver
			}
			adapter.turnLog.push(text);
			if (adapter.turnDriver !== null) {
				return adapter.turnDriver(event, text);
			}
			const isInlineDispatch =
				text.startsWith("/") ||
				(ctx.task.cancelRequested() === false && ctx.task.isDone());
			if (!isInlineDispatch) {
				while (this.holding && !ctx.task.cancelRequested()) {
					await Promise.race([
						this.holdGate.then(() => undefined),
						new Promise<void>((r) => setTimeout(r, 1)),
					]);
				}
			}
			ctx.throwIfCancelled();
			return `reply:${text}`;
		};

		adapter.attachGuard(
			{
				registry: SLACK_REGISTRY,
				messageHandler,
				sendReply: async (_chatId: string, replyText: string) => {
					adapter.replyLog.push(replyText);
				},
				onTurnFailure: (event) => adapter.onGuardTurnFailure(event),
			},
			{
				// The harness-stamped deterministic scheduler drives task
				// spawning for the ingress rows; the Lane C predicate lets the
				// guard resolve pending clarifies INLINE (§5.3).
				spawner: opts.spawner,
				hasPendingClarify: (sessionKey: string) =>
					adapter.clarifyArmed.has(sessionKey),
			},
		);
		if (opts.scheduler !== undefined) {
			(this as unknown as Record<symbol, unknown>)[SCHEDULER_SYMBOL] =
				opts.scheduler;
		}
	}

	/** Push an interactive payload through the socket seam (fixture helper). */
	pushInteractive(payload: SlackInteractivePayload): void {
		this.socketServer.pushInteractive(payload);
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
		if (on && !this.holding) {
			this.holdGate = new Promise<void>((resolve) => {
				this.releaseHold = resolve;
			});
		}
		this.holding = on;
		if (!on) this.releaseHold();
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
		// chat.startStream requires an anchor (pre-wire guard parity); the
		// arming frame carries a deterministic turn identity the way the
		// production gateway always stamps metadata.thread_id.
		await this.adapter.sendDraft({
			chatId,
			draftId,
			content: "",
			metadata: {
				thread_id: `1700000000.${String(draftId).padStart(6, "0")}`,
			},
		});
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
		return resolveEnablement(
			{
				name: "slack-scoped-probe",
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

export type { DisableReason };

export function makeSlackSubject(
	opts: SlackSubjectOptions & { wire: FakePlatformWire },
): SlackSubject {
	return new SlackSubject(opts);
}
