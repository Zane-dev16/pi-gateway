// pi_platforms/webhook/webhook-adapter — THE WEBHOOK reference adapter
// (DEC-002 third shape: WhatsApp-Cloud/api_server-like). Everything policy-
// shaped comes from the kit; this module supplies TRANSPORT + manifest DATA:
//
//   - capabilities AS DATA: supports_async_delivery=False +
//     interactive_resume=False (stateless pairing; 04 §8 webhook row)
//   - DEC-017 trust boundary AS MANIFEST DATA (validated at construction)
//   - ingress pipeline: caps → signature → rate → idempotency → route table
//     → bounded-window agent turns / deliver_only (http-ingress.ts)
//   - api_server-class lanes: /v1/chat/completions RAW-key direct turns +
//     /v1/runs SSE with approval/steer/stop (completions.ts, runs.ts)
//   - wakeLane == "raw-key-direct" (inherited getter): background completions
//     re-enter via StatelessWakeRail self-posts (wake.ts; DEC-022 close-out)
//   - held-open replies land via the DeliveryLedger seam (obligations-seam.ts)
//
// Hermes anchors: gateway/platforms/webhook.py:WebhookAdapter,
// api_server.py:APIServerAdapter (flags @1472/@1478), gateway/wake.py.
// Layering: imports pi_gateway/pi_state downward ONLY; no adapter cross-imports.

import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	resolveEnablement,
	validateTrustBoundaryManifest,
} from "../kit/index.js";
import type { CapabilityManifest } from "../kit/capabilities.js";
import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	DraftFrameArgs,
	EditOptions,
} from "../../pi_gateway/streaming/adapter-seam.js";
import type {
	CommandRegistry,
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { LengthUnit } from "../kit/length-policy.js";
import type {
	AdapterStatusSnapshot,
	DisableReason,
} from "../kit/lifecycle-state.js";

import {
	WEBHOOK_PLUGIN_MANIFEST,
	webhookTrustBoundary,
	WEBHOOK_CAPABILITIES,
} from "./manifest.js";
import type { WebhookRouteConfig } from "./manifest.js";
import {
	WebhookIngressPipeline,
	createTimeoutSeam,
	type AgentDispatch,
	type HeldOpenSink,
	type TimerSeam,
} from "./http-ingress.js";
import { CompletionsEndpoint, type DirectTurnResult } from "./completions.js";
import { RunRegistry } from "./runs.js";
import { StatelessWakeRail, WakeRailError } from "./wake.js";
import type { ScopedSecretReader } from "../kit/registration.js";

/** The egress capture seam (production: platform API; tests: FakePlatformWire). */
export interface WebhookWireTransport {
	transmitSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult>;
	transmitEdit(
		chatId: string,
		messageId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult>;
	transmitDraft(
		chatId: string,
		draftId: number,
		content: string,
		final: boolean,
		metadata: Metadata,
	): Promise<SendResult>;
	transmitRich(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult>;
	/** Optional harness seam: whether a behavior script exists for an op kind.
	 * Narrowed to the wire op vocabulary so FakePlatformWire binds directly
	 * (a `(opKind: string)` parameter would break assignability). */
	hasScript?:
		| ((opKind: "send" | "edit" | "draft" | "seal" | "rich") => boolean)
		| undefined;
}

export const WEBHOOK_REGISTRY: CommandRegistry = [
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

const REQUIRED_SECRET = "WEBHOOK_SECRET";

export interface WebhookAdapterOptions {
	wire: WebhookWireTransport;
	routes?: readonly WebhookRouteConfig[] | undefined;
	globalSecretReader?: ScopedSecretReader | undefined;
	/** Chats where ONE native stream IS the message (SSE-shaped lanes). */
	streamIsMessageChatIds?: ReadonlySet<string> | undefined;
	capabilities?: Partial<CapabilityManifest> | undefined;
	scalarMaxUnits?: number | undefined;
	lengthUnit?: LengthUnit | undefined;
	spawner?: TaskSpawner | undefined;
	heldOpenSink?: HeldOpenSink | undefined;
	timers?: TimerSeam | undefined;
	nowSeconds?: (() => number) | undefined;
	apiKeyProvider?: (() => string | undefined) | undefined;
}

/**
 * The kit-built stateless reference adapter. Transport = injected wire +
 * in-process HTTP lanes; everything else inherited from BasePlatformAdapter.
 */
export class WebhookAdapter extends BasePlatformAdapter {
	readonly pluginManifest = WEBHOOK_PLUGIN_MANIFEST;
	readonly trustBoundary = webhookTrustBoundary();
	readonly routes: Map<string, WebhookRouteConfig>;
	readonly wire: WebhookWireTransport;

	// Interactive surfaces (kit-owned).
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	// api_server-class lanes.
	readonly runs: RunRegistry;
	readonly completions: CompletionsEndpoint;

	private readonly cp: EgressChokepoint;
	private readonly isMessageChats: ReadonlySet<string>;
	private readonly secretReader: ScopedSecretReader;
	private readonly timers: TimerSeam;
	private readonly nowSecondsFn: () => number;
	private readonly apiKeyProvider: () => string | undefined;

	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly resolvedFamilies: string[] = [];

	private readonly clarifyArmedSet = new Set<string>();
	private allowAllClickers = true;
	private readonly replyWaiters = new Map<
		string,
		Array<(reply: string | null) => void>
	>();
	/** Sessions bound via RAW-key direct turns (DEC-022 observability). */
	private readonly boundRawKeySet = new Set<string>();

	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};
	private holding = false;

	constructor(opts: WebhookAdapterOptions) {
		super({
			manifestName: WEBHOOK_PLUGIN_MANIFEST.name,
			capabilities: {
				...WEBHOOK_CAPABILITIES,
				...(opts.capabilities ?? {}),
			},
			lengthUnit: opts.lengthUnit,
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
		});
		this.wire = opts.wire;
		this.routes = new Map((opts.routes ?? []).map((r) => [r.name, r]));
		this.isMessageChats = opts.streamIsMessageChatIds ?? new Set();
		this.secretReader =
			opts.globalSecretReader ?? (() => process.env[REQUIRED_SECRET]);
		this.timers = opts.timers ?? createTimeoutSeam();
		this.nowSecondsFn =
			opts.nowSeconds ?? (() => Math.floor(Date.now() / 1000));
		this.apiKeyProvider =
			opts.apiKeyProvider ?? (() => process.env["API_SERVER_KEY"]);

		// Trust boundary completeness is a CONSTRUCTION-TIME hard error
		// (DEC-017: boundaries are auditable data, not flags).
		const boundaryErrors = validateTrustBoundaryManifest(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			const reason: DisableReason = {
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			};
			this.lifecycle.disable(reason);
		}

		// §11 step 3/4: missing required secret ⇒ LOUD disable (status-visible),
		// never silent skip.
		const enablement = resolveEnablement(
			WEBHOOK_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: (chatId) =>
				this.isMessageChats.has(String(chatId)),
			transmitSend: async (chatId, content, metadata) =>
				this.wire.transmitSend(chatId, content, metadata),
			transmitEdit: async (chatId, messageId, content) =>
				this.wire.transmitEdit(chatId, messageId, content, {}),
			transmitSeal: async (_k, chatId, draftId, content, metadata) =>
				this.wire.transmitDraft(chatId, draftId, content, true, metadata),
		});

		this.router = new CallbackQueryRouter({
			stores: {
				approvals: this.approvals,
				slashConfirms: this.slashConfirms,
				appr: this.appr,
				clarify: this.clarify,
			},
			authorizer: () => this.allowAllClickers,
			onExecApproval: async (_sessionKey) => {
				this.resolvedFamilies.push("ea");
				return "ok";
			},
			onSlashConfirm: async (_sessionKey, _id, _choice) => {
				this.resolvedFamilies.push("sc");
				return "ok";
			},
			onClarifyChoice: async (_sessionKey, _id, idx) => {
				this.resolvedFamilies.push("cl");
				return `answer-${idx}`;
			},
			onWhatsappApproval: async (_sessionKey, _id, _approve) => {
				this.resolvedFamilies.push("appr");
				return "ok";
			},
			onPickerNav: async (parsed) => ({
				answerText: `nav:${parsed.family}`,
				hostEditText: JSON.stringify(parsed),
			}),
		});

		this.runs = new RunRegistry();
		this.completions = new CompletionsEndpoint({
			apiKeyProvider: this.apiKeyProvider,
			idempotency: this.idempotency(),
			nowMs: () => this.nowSecondsFn() * 1000,
			runDirectTurn: async ({ rawSessionId, prompt }) =>
				this.runDirectTurn(rawSessionId, prompt),
		});
	}

	// ── guard wiring ─────────────────────────────────────────────────────────

	attachStandardGuard(spawner?: TaskSpawner): void {
		this.attachGuard(
			{
				registry: WEBHOOK_REGISTRY,
				messageHandler: async (event, ctx) => {
					const text = event.text ?? `[${String(event.messageType)}]`;
					const sessionKey = String(
						event.metadata?.["gateway_session_key"] ?? "",
					);
					if (this.clarifyArmedSet.has(sessionKey) && !text.startsWith("/")) {
						this.clarifyCaptures.push(text);
						return null; // consumed by the clarify resolver (Lane C)
					}
					this.turnLog.push(text);
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
					const reply = `reply:${text}`;
					// The turn's REPLY (handler return) is what bounded-window
					// responses carry and what held-open obligations record.
					this.resolveReplyWaiters(sessionKey, reply);
					return reply;
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

	get clarifyArmed(): Set<string> {
		return this.clarifyArmedSet;
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
		// Self/echo filter parity (§8 ingress row): bot-authored echoes never
		// become turns. The reference sender id "bot-self" models the
		// adapter's own transmissions arriving back.
		if (String(event.source?.userId ?? "") === "bot-self") return;
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	// ── the bounded-window agent turn (C5 split, inbound half) ──────────────

	/** Test/diagnostic seam: is the L1 guard installed for this session? */
	guardIsActiveForTest(sessionKey: string): boolean {
		return this.guard?.isActive(sessionKey) ?? false;
	}

	/** Test/diagnostic seam: pending-slot occupancy (single slot ⇒ 0|1). */
	guardPendingCountForTest(sessionKey: string): number {
		const guard = this.guard;
		if (guard === null) return 0;
		return guard.pendingOf(sessionKey) !== undefined ? 1 : 0;
	}

	/** Snapshot of every turn the handler has STARTED (drain turns included). */
	turnsDrained(): readonly string[] {
		return [...this.turnLog];
	}

	/**
	 * Run one turn through BOTH guards and resolve with its reply. Registered
	 * as the ingress pipeline's runAgentTurn seam; the PIPELINE owns the race
	 * against the route window and the held-open ledger landing.
	 */
	async runAgentTurn(dispatch: AgentDispatch): Promise<string | null> {
		const waiter = this.registerReplyWaiter(dispatch.sessionKey);
		await this.deliverInbound(dispatch.event, dispatch.sessionKey);
		return waiter.promise;
	}

	registerReplyWaiter(sessionKey: string): {
		promise: Promise<string | null>;
		resolve: (reply: string | null) => void;
	} {
		let resolve!: (reply: string | null) => void;
		const promise = new Promise<string | null>((r) => {
			resolve = r;
		});
		const waiters = this.replyWaiters.get(sessionKey) ?? [];
		waiters.push(resolve);
		this.replyWaiters.set(sessionKey, waiters);
		return {
			promise,
			resolve: (reply) => {
				this.removeReplyWaiter(sessionKey, resolve);
				resolve(reply);
			},
		};
	}

	private removeReplyWaiter(
		sessionKey: string,
		resolve: (reply: string | null) => void,
	): void {
		const waiters = this.replyWaiters.get(sessionKey);
		if (waiters === undefined) return;
		const idx = waiters.indexOf(resolve);
		if (idx >= 0) waiters.splice(idx, 1);
	}

	private resolveReplyWaiters(sessionKey: string, reply: string): void {
		const waiters = this.replyWaiters.get(sessionKey);
		if (waiters === undefined || waiters.length === 0) return;
		this.replyWaiters.set(sessionKey, []);
		for (const w of waiters) w(reply);
	}

	// ── RAW-key direct turns (/v1/chat/completions executor) ────────────────

	private async runDirectTurn(
		rawSessionId: string | undefined,
		prompt: string,
	): Promise<DirectTurnResult> {
		// The RAW session id IS the key real turns bind under (DEC-022):
		// no derivation, no namespace reshaping.
		const sessionId = rawSessionId ?? `derived-${Date.now()}`;
		this.boundRawKeySet.add(sessionId);
		const event: IncomingEvent = {
			messageType: "text",
			text: prompt,
			source: {
				platform: "webhook",
				chatType: "dm",
				chatId: sessionId,
			},
			// Stamp the routing key so the guard's internal-routing check and
			// the reply-waiter registry agree on the RAW key.
			metadata: { gateway_session_key: sessionId },
		};
		const waiter = this.registerReplyWaiter(sessionId);
		await this.handleIngress(event, sessionId);
		const reply = (await waiter.promise) ?? "";
		return { reply, sessionId };
	}

	/** Public seam for harness/e2e wiring of the RAW-key direct turn. */
	runDirectTurnForTest(
		rawSessionId: string | undefined,
		prompt: string,
	): Promise<DirectTurnResult> {
		return this.runDirectTurn(rawSessionId, prompt);
	}

	/** Session keys bound via RAW-key direct turns (DEC-022 observability). */
	rawKeyBoundSessions(): readonly string[] {
		return [...this.boundRawKeySet];
	}

	/** Alias used by e2e wiring to assert RAW-key binding. */
	guardSessionsForTest(): readonly string[] {
		return [...this.boundRawKeySet];
	}

	// ── ingress pipeline assembly ────────────────────────────────────────────

	buildPipeline(
		heldOpenSink?: HeldOpenSink | undefined,
	): WebhookIngressPipeline {
		return new WebhookIngressPipeline({
			trust: this.trustBoundary,
			routes: this.routes,
			rateLimiter: this.rateLimiter(),
			idempotency: this.idempotency(),
			nowSeconds: this.nowSecondsFn,
			timers: this.timers,
			globalSecret: this.secretReader("WEBHOOK_SECRET"),
			parseJson: defaultParseJson,
			runAgentTurn: (dispatch) => this.runAgentTurn(dispatch),
			deliverOnly: async (prompt, dispatch) => {
				const results = await this.deliverText(dispatch.chatId, prompt);
				return results.every((r) => r.success);
			},
			...(heldOpenSink !== undefined ? { heldOpenSink } : {}),
		});
	}

	private rateLimiter(): SlidingWindowRateLimiter {
		return new SlidingWindowRateLimiter({
			limit: 30,
			nowMs: () => this.nowSecondsFn() * 1000,
		});
	}

	private idempotency(): DeliveryIdempotencyStore {
		return new DeliveryIdempotencyStore({
			maxEntries: this.trustBoundary.idempotency?.seenSetMaxEntries ?? 128,
			nowMs: () => this.nowSecondsFn() * 1000,
		});
	}

	/** Wake rail bound to THIS adapter's own HTTP surface. */
	buildWakeRail(
		baseUrl: string,
		sleepMs?: ((ms: number) => Promise<void>) | undefined,
	): StatelessWakeRail {
		return new StatelessWakeRail({
			baseUrl,
			apiKeyProvider: this.apiKeyProvider,
			...(sleepMs !== undefined ? { sleepMs } : {}),
		});
	}

	static WakeRailError = WakeRailError;

	// ── egress doors (BasePlatformAdapter contract) ─────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	override supportsDraftStreaming(chatType?: string | undefined): boolean {
		// The SSE lane streams natively (dm-shaped runs); everything else is
		// reply-only egress per the §3 matrix.
		return chatType === undefined || chatType === "dm";
	}

	async armNativeStream(chatId: string, draftId: number): Promise<void> {
		await this.sendDraft({ chatId, draftId, content: "" });
	}

	/** Per-chat length descriptor (§6.3/A15 pair resolution point). */
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

	protected override wireEdit(
		chatId: string,
		messageId: string,
		content: string,
		_opts: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		return this.wire.transmitEdit(chatId, messageId, content, {});
	}

	protected override async wireRich(
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		// Rich endpoint ABSENT unless explicitly scripted (capability-error
		// shape ⇒ §10.1 latch path, probe once — no wire roundtrip burned).
		if (!(this.wire.hasScript?.("rich") ?? false)) {
			void metadata;
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return this.wire.transmitRich("__rich__", content, {});
	}

	protected override wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		return this.wire.transmitSend(chatId, content, metadata);
	}

	// ── lifecycle ────────────────────────────────────────────────────────────

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		return true; // server start owned by WebhookHttpServer.listen()
	}

	override async disconnect(): Promise<void> {}

	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.lifecycle.statusSnapshot();
	}
}

function defaultParseJson(bodyText: string): Record<string, unknown> {
	let parsed: unknown;
	try {
		parsed = JSON.parse(bodyText);
	} catch {
		throw new Error("Cannot parse body: invalid JSON");
	}
	if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
		throw new Error("Cannot parse body: expected a JSON object");
	}
	return parsed as Record<string, unknown>;
}

import { SlidingWindowRateLimiter } from "./rate-limit.js";
import { DeliveryIdempotencyStore } from "./idempotency.js";
