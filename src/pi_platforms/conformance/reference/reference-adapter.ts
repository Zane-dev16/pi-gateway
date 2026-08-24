// pi_platforms/conformance/reference/reference-adapter — a REFERENCE-CORRECT
// fake adapter built ENTIRELY on the kit. The conformance self-test proves the
// suite itself by requiring this adapter to pass every currently-encoded row;
// real adapters (Phase 3/4) are held to the same bar.

import {
	BasePlatformAdapter,
	CallbackQueryRouter,
	ActionHandlerRegistry,
	OneShotPendingStore,
	ClarifyPendingStore,
	TokenLockManagerSeam,
	resolveEnablement,
} from "../../kit/index.js";
import type {
	Metadata,
	SendResult,
} from "../../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../../pi_gateway/streaming/egress-door.js";
import type {
	DraftFrameArgs,
	EditOptions,
	StreamEgressAdapter,
} from "../../../pi_gateway/streaming/adapter-seam.js";
import type {
	AdapterStatusSnapshot,
	DisableReason,
} from "../../kit/lifecycle-state.js";
import type { ChatLengthPolicy, LengthUnit } from "../../kit/length-policy.js";
import type {
	IncomingEvent,
	TaskSpawner,
} from "../../../pi_gateway/guards/index.js";
import type { FakePlatformWire } from "../wire.js";
import type { ConformanceSubject } from "../harness.js";
import { SCHEDULER_SYMBOL } from "../harness.js";
import type { ManualScheduler } from "../../../pi_gateway/guards/testing/manual-spawner.js";

export const REFERENCE_REGISTRY = [
	{
		name: "new",
		aliases: ["reset"],
		busyPolicy: "interrupt_then_dispatch" as const,
		busyHandler: "new",
	},
	{
		name: "stop",
		busyPolicy: "interrupt_then_dispatch" as const,
		busyHandler: "stop",
	},
	{ name: "model", busyPolicy: "reject" as const, busyHandler: "model" },
	{ name: "approve", busyPolicy: "dispatch" as const },
	{ name: "status", busyPolicy: "dispatch" as const },
];

const REQUIRED_SECRET = "REFERENCE_BOT_TOKEN";

export interface ReferenceSubjectOptions {
	name?: string | undefined;
	wire: FakePlatformWire;
	/** Chats where ONE native stream IS the message (relay-shaped lanes). */
	streamIsMessageChatIds?: ReadonlySet<string> | undefined;
	capabilities?: ConstructorParameters<
		typeof BasePlatformAdapter
	>[0]["capabilities"];
	lengthUnit?: LengthUnit | undefined;
	scalarMaxUnits?: number | undefined;
	spawner?: TaskSpawner | undefined;
	/** When false, required secrets resolve undefined (loud-disable row). */
	withSecret?: boolean | undefined;
}

/**
 * The reference-correct ConformanceSubject over ReferenceAdapter mechanics.
 */
export class ReferenceSubject implements ConformanceSubject {
	readonly name: string;
	readonly adapter: ReferenceCore;
	readonly wire: FakePlatformWire;

	constructor(
		opts: ReferenceSubjectOptions & { scheduler?: ManualScheduler | undefined },
	) {
		this.name = opts.name ?? "reference";
		this.wire = opts.wire;
		this.adapter = new ReferenceCore({
			wire: opts.wire,
			manifestName: opts.name ?? "reference",
			capabilities: opts.capabilities,
			lengthUnit: opts.lengthUnit,
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
			streamIsMessageChatIds: opts.streamIsMessageChatIds,
			withSecret: opts.withSecret !== false,
			secretReader: (key) =>
				opts.withSecret === false
					? undefined
					: key === REQUIRED_SECRET
						? "tok"
						: undefined,
		});
		this.adapter.attachStandardGuard(opts.spawner);
		// Harness-stamped deterministic scheduler for ingress rows.
		if (opts.scheduler !== undefined) {
			(this as unknown as Record<symbol, unknown>)[SCHEDULER_SYMBOL] =
				opts.scheduler;
		}
	}

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
	deliverInbound(
		event: Parameters<ConformanceSubject["deliverInbound"]>[0],
		sessionKey: string,
	): Promise<void> {
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
		});
		return results[results.length - 1] ?? { success: false };
	}
	transientRichFailureOutcome(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		return this.adapter.transientRichOutcome(chatId, content);
	}
	parseFailurePlainResend(chatId: string, content: string): Promise<string> {
		return this.adapter.parseFailureResendContent(chatId, content);
	}
	chatPolicyFor(chatId: string): ChatLengthPolicy {
		return this.adapter.chatLengthPolicyForChat(chatId);
	}
	streamAdapter(): StreamEgressAdapter {
		// The core implements StreamEgressAdapter through its doors.
		return this.adapter as unknown as StreamEgressAdapter;
	}
	armOpenNativeStream(chatId: string, draftId: number): Promise<void> {
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
		const enablement = resolveEnablement(
			{
				name: "scoped-probe",
				description: "",
				transportShape: "polling",
				requiresEnv: [{ name: envKey }],
				capabilities: {},
			},
			() => undefined, // scoped store misses — process env HAS the key
		);
		return enablement.enabled;
	}
	wakeLaneDeclaration(): "forged-event" | "raw-key-direct" {
		return this.adapter.wakeLane;
	}
}

/**
 * The kit-built adapter core. Transport = FakePlatformWire; everything else
 * inherited from BasePlatformAdapter.
 */
class ReferenceCore extends BasePlatformAdapter implements StreamEgressAdapter {
	readonly wire: FakePlatformWire;
	private readonly cp: EgressChokepoint;
	private readonly isMessageChats: ReadonlySet<string>;
	private readonly secretReader: (k: string) => string | undefined;

	// Interactive surfaces.
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly resolvedFamilies: string[] = [];
	private routerResolved: string[] = [];
	private readonly clarifyArmedSet = new Set<string>();
	private allowAllClickers = true;

	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};
	private holding = false;

	private readonly lockManager = new TokenLockManagerSeam({
		nowMs: () => 1_000,
	});
	private lockHeld = false;

	constructor(opts: {
		wire: FakePlatformWire;
		manifestName: string;
		capabilities?: ConstructorParameters<
			typeof BasePlatformAdapter
		>[0]["capabilities"];
		lengthUnit?: LengthUnit | undefined;
		scalarMaxUnits?: number | undefined;
		streamIsMessageChatIds?: ReadonlySet<string> | undefined;
		withSecret: boolean;
		secretReader: (k: string) => string | undefined;
	}) {
		super({
			manifestName: opts.manifestName,
			capabilities: opts.capabilities,
			lengthUnit: opts.lengthUnit,
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
		});
		this.wire = opts.wire;
		this.isMessageChats = opts.streamIsMessageChatIds ?? new Set();
		this.secretReader = opts.secretReader;

		// §11 step 3/4 + §8 identity rows: missing required secret ⇒ LOUD
		// disable at construction (visible in /status), never silent skip.
		const enablement = resolveEnablement(
			{
				name: opts.manifestName,
				description: "reference",
				transportShape: "polling",
				requiresEnv: [{ name: REQUIRED_SECRET }],
				capabilities: {},
			},
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: (chatId) =>
				this.isMessageChats.has(String(chatId)),
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			transmitEdit: async (chatId, messageId, content) =>
				this.wire.transmitEdit(chatId, messageId, content, {}),
			transmitSeal: async (_k, chatId, draftId, content, metadata) => {
				if (metadata["forceSealFailure"] === true) {
					return { success: false, error: "forced seal failure" };
				}
				return this.wire.transmitDraft(
					chatId,
					draftId,
					content,
					true,
					metadata,
				);
			},
		});

		this.router = new CallbackQueryRouter({
			stores: {
				approvals: this.approvals,
				slashConfirms: this.slashConfirms,
				appr: this.appr,
				clarify: this.clarify,
			},
			authorizer: () => this.allowAllClickers,
			onExecApproval: async (sessionKey) => {
				this.resolvedFamilies.push("ea");
				this.routerResolved.push(`ea:${sessionKey}`);
				return "ok";
			},
			onSlashConfirm: async (sessionKey, _id, choice) => {
				this.resolvedFamilies.push("sc");
				this.routerResolved.push(`sc:${sessionKey}:${choice}`);
				return "ok";
			},
			onClarifyChoice: async (sessionKey, _id, idx) => {
				this.resolvedFamilies.push("cl");
				this.routerResolved.push(`cl:${sessionKey}:${idx}`);
				return `answer-${idx}`;
			},
			onWhatsappApproval: async (sessionKey, _id, approve) => {
				this.resolvedFamilies.push("appr");
				this.routerResolved.push(`appr:${sessionKey}:${approve}`);
				return "ok";
			},
			onPickerNav: async (parsed) => ({
				answerText: `nav:${parsed.family}`,
				hostEditText: JSON.stringify(parsed),
			}),
		});
	}

	get clarifyArmed(): Set<string> {
		return this.clarifyArmedSet;
	}
	routerAuditResolved(): readonly string[] {
		return this.routerResolved;
	}
	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	holdTurns(on: boolean): void {
		if (on && !this.holding) {
			this.holdGate = new Promise<void>((resolve) => {
				this.releaseHold = resolve;
			});
		}
		this.holding = on;
		if (!on) this.releaseHold();
	}

	async deliverInbound(
		event: IncomingEvent,
		sessionKey: string,
	): Promise<void> {
		// Self/echo filter (§8 ingress row).
		const senderId = String(event.source?.userId ?? "");
		if (senderId === "bot-self") return;
		// Stamp the routing key so the handler can consult the clarify-armed
		// set per session (guard's internal-routing check reads the same key).
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	attachStandardGuard(spawner?: TaskSpawner): void {
		this.attachGuard(
			{
				registry: REFERENCE_REGISTRY,
				messageHandler: async (event, ctx) => {
					const text = event.text ?? `[${String(event.messageType)}]`;
					// Clarify intercept (Lane C) resolves BEFORE any turn work —
					// captured answers are NOT turns and never enter the log.
					const sessionKey = String(
						event.metadata?.["gateway_session_key"] ?? "",
					);
					if (this.clarifyArmedSet.has(sessionKey) && !text.startsWith("/")) {
						this.clarifyCaptures.push(text);
						return null; // consumed by the clarify resolver
					}
					this.turnLog.push(text);
					// The hold gate models a BUSY TURN. Inline lanes (control
					// commands, clarify answers under DETACHED_TASK) must never
					// block — they run while the old turn unwinds.
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
				},
				sendReply: async (_chatId, text) => {
					this.replyLog.push(text);
				},
			},
			{
				spawner,
				hasPendingClarify: (key) => this.clarifyArmedSet.has(key),
			},
		);
	}

	// ── egress probes ──

	doorAudit() {
		return this.cp.audit;
	}

	async transientRichOutcome(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		// Fresh ladder lane against a rich endpoint that fails TRANSIENTLY:
		// the outcome must be a failed retryable SendResult and NO legacy send.
		const { FormattingLadder } = await import("../../kit/formatting-ladder.js");
		const ladder = new FormattingLadder({
			tryRich: async () => ({ success: false, error: "socket hang up" }),
			sendConverted: async (_c, md) =>
				this.wire.transmitSend(chatId, "SHOULD-NOT-HAPPEN", md),
			sendPlain: async (_c, md) =>
				this.wire.transmitSend(chatId, "SHOULD-NOT-HAPPEN", md),
		});
		return ladder.sendText(content, {});
	}

	async parseFailureResendContent(
		chatId: string,
		content: string,
	): Promise<string> {
		const results = await this.deliverText(chatId, content, {
			forceFormattingError: true,
		});
		void results;
		const sends = this.wire.sendsOf(chatId);
		return sends[sends.length - 1]?.content ?? "";
	}

	// ── identity probes ──

	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		if (!this.lockHeld) {
			const first = this.acquireCredentialLock(
				this.lockManager,
				"bot-token",
				"cred-1",
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.acquireCredentialLock(
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

	buildMissingSecretSibling(): ReferenceCore {
		return new ReferenceCore({
			wire: this.wire,
			manifestName: `${this.manifestName}-no-secret`,
			withSecret: false,
			secretReader: () => undefined,
		});
	}

	// ── transport contract over the fake wire ──

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	/**
	 * Per-chat length descriptors (§6.3/A15): chats whose id names "utf16"
	 * front a Telegram-class platform — budget 30 CODE UNITS. This is the
	 * ONE chat resolution the chunker consumes; budget AND unit move
	 * together.
	 */
	protected override chatDescriptorFor(chatId: string):
		| {
				maxMessageLength?: number | undefined;
				lenUnit?: LengthUnit | undefined;
		  }
		| undefined {
		if (chatId.includes("utf16")) {
			return { maxMessageLength: 30, lenUnit: "utf16" };
		}
		return undefined;
	}

	protected override async wireDraft(
		args: DraftFrameArgs,
	): Promise<SendResult> {
		return this.wire.transmitDraft(
			args.chatId,
			args.draftId,
			args.content,
			false,
			args.metadata ?? {},
		);
	}

	protected override async wireRich(
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		// Rich endpoint ABSENT by default (capability-error shape ⇒ the §10.1
		// latch path). An explicit wire script opts in:
		//   wire.script("rich", …) programs the endpoint's answers.
		if (!this.wire.hasScript("rich")) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return this.wire.transmitRich("__rich__", content, metadata);
	}

	protected override wireEdit(
		chatId: string,
		messageId: string,
		content: string,
		_opts: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		return this.wire.transmitEdit(chatId, messageId, content, {});
	}

	protected override wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		// The forced formatting error models a markdown-RENDERING rejection:
		// markdown-shaped sends fail; the §6.1 plain-text fallback body
		// (parse_mode=None lane) succeeds on the wire.
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith("(Response formatting failed, plain text:")
		) {
			return Promise.resolve({
				success: false,
				error: "Bad Request: can't parse entities",
			});
		}
		return this.wire.transmitSend(chatId, content, metadata);
	}

	override supportsDraftStreaming(chatType?: string | undefined): boolean {
		return chatType === undefined || chatType === "dm";
	}

	/** Relay lanes arm seal-interception via one emitted draft frame. */
	async armNativeStream(chatId: string, draftId: number): Promise<void> {
		await this.sendDraft({ chatId, draftId, content: "" });
	}

	/** Seal-failure knob for the failed-seal row (metadata-driven). */
	markSealFailureExpected(): DisableReason | undefined {
		return undefined;
	}

	async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		return true;
	}
	async disconnect(): Promise<void> {}
}

export function makeReferenceSubject(
	opts: ReferenceSubjectOptions & {
		wire: FakePlatformWire;
		scheduler?: ManualScheduler | undefined;
	},
): ReferenceSubject {
	return new ReferenceSubject(opts);
}
