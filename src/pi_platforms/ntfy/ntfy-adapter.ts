// pi_platforms/ntfy/ntfy-adapter — THE ntfy adapter, ported from the READ-ONLY
// Hermes reference plugin (plugins/platforms/ntfy/adapter.py) onto the kit
// base. TRANSPORT (HTTP /json stream in, POST publish out) and MANIFEST DATA.
//
// Ported semantics (file:symbol anchors):
//   _build_auth_header  strip whitespace; "user:pass" ⇒ Basic, else Bearer.
//   connect/_run_stream subscribe {server}/{topic}/json?poll=false; 401 ⇒
//                       FATAL ntfy_unauthorized (stop reconnecting); 404 ⇒
//                       FATAL ntfy_topic_not_found; other errors ride the
//                       FIXED ladder [2,5,10,30,60]s with reset after ≥60s
//                       alive; read-timeout 90s > keepalive 55s.
//   _on_message         dedup by id (300s window, max 1000, cutoff eviction) →
//                       echo-tag skip (X-Tags hermes-agent) → empty-body skip →
//                       user_id FIXED to the topic (title is publisher-
//                       controlled and NEVER trusted for authorization).
//   send                publish_topic chain metadata.publish_topic →
//                       configured publish topic → chat_id; body truncated to
//                       4096 chars; X-Tags echo tag; X-Markdown when enabled;
//                       <300 ⇒ success with server id (or uuid fallback);
//                       timeout ⇒ "Timeout publishing to ntfy".

import {
	ActionHandlerRegistry,
	BasePlatformAdapter,
	CallbackQueryRouter,
	ClarifyPendingStore,
	FormattingLadder,
	DELIVERY_FAILED_NOTICE,
	OneShotPendingStore,
	classifySendError,
	sendWithRetry,
	plainTextFallbackBody,
	resolveEnablement,
	TokenLockManagerSeam,
} from "../kit/index.js";
import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
	DraftFrameArgs,
	EditOptions,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import { immediateSpawner } from "../../pi_gateway/guards/index.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";

import {
	NTFY_DEDUP_MAX_SIZE,
	NTFY_DEDUP_WINDOW_MS,
	NTFY_ECHO_TAG,
	NTFY_MAX_MESSAGE_CHARS,
	NTFY_PLUGIN_MANIFEST,
	NTFY_PUBLISH_TIMEOUT_MS,
	NTFY_RECONNECT_BACKOFF_S,
	NTFY_STREAM_TIMEOUT_MS,
} from "./manifest.js";
import type { PacingClockLike } from "./clock.js";
import { NTFY_LADDER_RESET_ALIVE_MS } from "./manifest.js";
import type { NtfyEvent } from "./fake-ntfy-server.js";
import type { FakeNtfyServer, FakeNtfyStream } from "./fake-ntfy-server.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const NTFY_REGISTRY: CommandRegistry = [
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
	{ name: "approve", busyPolicy: "dispatch" },
	{ name: "status", busyPolicy: "dispatch" },
];

export class FatalStreamError extends Error {}

export function buildAuthHeader(token: string): Record<string, string> {
	const trimmed = token.trim();
	if (trimmed.length === 0) return {};
	if (trimmed.includes(":")) {
		return {
			Authorization: `Basic ${Buffer.from(trimmed, "utf8").toString("base64")}`,
		};
	}
	return { Authorization: `Bearer ${trimmed}` };
}

export interface NtfyAdapterDeps {
	server: FakeNtfyServer;
	clock?: PacingClockLike | undefined;
	secretReader?: ScopedSecretReader | undefined;
	spawner?: TaskSpawner | undefined;
	manifestName?: string | undefined;
	scalarMaxUnits?: number | undefined;
	config?:
		| {
				serverUrl?: string | undefined;
				publishTopic?: string | undefined;
				token?: string | undefined;
				markdown?: boolean | undefined;
		  }
		| undefined;
	declaredDraftStreaming?: boolean | undefined;
}

interface StreamEventMessage {
	id: string;
	topic: string;
	message?: string | undefined;
	title?: string | undefined;
	tags?: string[] | undefined;
}

export class NtfyAdapter
	extends BasePlatformAdapter
	implements StreamEgressAdapter
{
	readonly pluginManifest = NTFY_PLUGIN_MANIFEST;
	readonly fakeServer: FakeNtfyServer;
	readonly clock: PacingClockLike | undefined;

	private readonly cp: EgressChokepoint;
	private readonly secretReader: ScopedSecretReader;
	private readonly spawn: TaskSpawner;
	private readonly deps: NtfyAdapterDeps;

	readonly serverUrl: string;
	readonly topic: string;
	readonly publishTopic: string;
	private readonly token: string;
	private readonly markdownEnabled: boolean;

	// ── runtime state ──
	private readonly seenMessages = new Map<string, number>();
	private activeStream: FakeNtfyStream | null = null;
	private streamTask: Promise<void> | null = null;
	private backoffIdx = 0;
	private streamStartAtMs = 0;
	private running = false;

	/** Observability: ladder steps actually slept ("[delayMs@attemptN]"). */
	readonly reconnectLog: string[] = [];
	reconnectCount = 0;
	fatalCodes: Array<{ code: string; detail: string }> = [];
	lastCapturedRetryAfterSeconds: number | null = null;
	readonly dedupHits = { duplicates: 0, echoSkips: 0 };

	// ── interactive surfaces (kit census posture) ──
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

	wireTransmitPublish: (
		topic: string,
		body: string,
		metadata: Metadata,
	) => Promise<SendResult> = async () => ({
		success: false,
		error: "no wire bound",
	});
	lastSendContentReader: (chatId: string) => string = () => "";
	richScriptedProbe: () => boolean = () => false;
	wireTransmitRich: (
		content: string,
		metadata: Metadata,
	) => Promise<SendResult> = () =>
		Promise.resolve({
			success: false,
			error: "sendRichMessage: method not found",
		});

	constructor(deps: NtfyAdapterDeps) {
		super({
			manifestName: deps.manifestName ?? "ntfy",
			capabilities: NTFY_PLUGIN_MANIFEST.capabilities,
			lengthUnit: "chars",
			scalarMaxUnits: deps.scalarMaxUnits ?? NTFY_MAX_MESSAGE_CHARS,
		});
		this.deps = deps;
		this.fakeServer = deps.server;
		this.clock = deps.clock;
		this.secretReader = deps.secretReader ?? ((name) => process.env[name]);
		this.spawn = deps.spawner ?? immediateSpawner();

		const env = (k: string): string | undefined => this.secretReader(k);
		this.topic = env("NTFY_TOPIC") ?? "";
		this.serverUrl = (
			deps.config?.serverUrl ??
			env("NTFY_SERVER_URL") ??
			deps.server.baseUrl
		).replace(/\/+$/u, "");
		this.publishTopic =
			deps.config?.publishTopic ?? env("NTFY_PUBLISH_TOPIC") ?? this.topic;
		this.token = deps.config?.token ?? env("NTFY_TOKEN") ?? "";
		const markdownEnv = (env("NTFY_MARKDOWN") ?? "").toLowerCase();
		this.markdownEnabled =
			deps.config?.markdown ?? ["1", "true", "yes"].includes(markdownEnv);

		// §11 step 3/4: missing required secret ⇒ LOUD disable at construction.
		const enablement = resolveEnablement(
			NTFY_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: (chatId) =>
				this.deps.declaredDraftStreaming === true &&
				this.isMessageChats.has(String(chatId)),
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			transmitEdit: async () => ({ success: false, error: "Not supported" }),
			transmitSeal: async () => ({ success: false, error: "Not supported" }),
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
			onWhatsappApproval: async (sessionKey) => {
				this.resolvedFamilies.push("appr");
				this.routerResolved.push(`appr:${sessionKey}`);
				return "ok";
			},
			onPickerNav: async (parsed) => ({ answerText: `nav:${parsed.family}` }),
		});
	}

	private readonly isMessageChats = new Set<string>();
	markStreamIsMessage(chatId: string): void {
		this.isMessageChats.add(chatId);
	}
	override supportsDraftStreaming(): boolean {
		return this.deps.declaredDraftStreaming === true;
	}

	get isConnected(): boolean {
		return this.running;
	}
	get authHeaders(): Record<string, string> {
		return buildAuthHeader(this.token);
	}
	get seenCount(): number {
		return this.seenMessages.size;
	}
	private markdownOverride: boolean | null = null;
	/** Fixture/test seam for the markdown datum. */
	setMarkdownEnabledForTests(v: boolean): void {
		this.markdownOverride = v;
	}
	get markdownActive(): boolean {
		return this.markdownOverride ?? this.markdownEnabled;
	}
	/** Fixture observability: the live stream handle. */
	activeStreamForTests(): FakeNtfyStream | null {
		return this.activeStream;
	}
	get currentBackoffIdx(): number {
		return this.backoffIdx;
	}

	// ══════════════════════════════════════════════════════════════════════
	// Connection lifecycle + stream loop (_run_stream / _consume_stream)
	// ══════════════════════════════════════════════════════════════════════

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (this.topic.length === 0) return false;
		this.running = true;
		this.streamStartAtMs = this.nowMs();
		this.activeStream = this.openStream();
		this.streamTask = this.consumeStream(this.activeStream);
		// _mark_connected parity: drain the held-inbound window.
		this.drainHeldInbound();
		return true;
	}

	private openStream(): FakeNtfyStream {
		try {
			return this.fakeServer.subscribe(this.topic);
		} catch (err) {
			const message = String(err instanceof Error ? err.message : err);
			if (this.fakeServer.authRejectMode && message.includes("401")) {
				this.markFatal(
					"ntfy_unauthorized",
					"ntfy server rejected auth (401). Check NTFY_TOKEN.",
				);
				throw new FatalStreamError(message);
			}
			if (message.includes("404")) {
				this.markFatal(
					"ntfy_topic_not_found",
					`ntfy topic '${this.topic}' returned 404. Check NTFY_TOPIC.`,
				);
				throw new FatalStreamError(message);
			}
			throw err;
		}
	}

	/** One stream generation. Fatal errors STOP the loop (vendor semantics). */
	private async consumeStream(stream: FakeNtfyStream): Promise<void> {
		while (this.running) {
			const outcome = await this.nextEventWithReadTimeout(stream);
			if (outcome === "timeout") {
				// Read timeout (>90s without keepalive/message): treat like any
				// stream error → reconnect ladder.
				this.recoveryNote("read-timeout");
				break;
			}
			if (outcome === null) break; // EOF / closed
			if (outcome.event !== "message") continue; // keepalives feed nothing
			await this.handleMessageEvent(outcome as unknown as StreamEventMessage);
		}
	}

	/**
	 * Race the next event against the 90s READ timeout (STREAM_TIMEOUT_SECONDS
	 * vs the 55s keepalive cadence). Returns null on close, "timeout" when the
	 * read budget expired first.
	 */
	private async nextEventWithReadTimeout(
		stream: FakeNtfyStream,
	): Promise<NtfyEvent | null | "timeout"> {
		let timedOut = false;
		const timer = this.sleepCancellable(NTFY_STREAM_TIMEOUT_MS, () => {
			timedOut = true;
		});
		const event = await Promise.race([
			stream.closed ? Promise.resolve(null) : stream.nextEvent(),
			timer.promise,
		]);
		timer.cancel();
		if (timedOut && event === undefined) return "timeout";
		if (event === undefined) return null;
		return event;
	}

	private sleepCancellable(
		ms: number,
		onFire: () => void,
	): { promise: Promise<undefined>; cancel(): void } {
		let cancelled = false;
		const promise = (async () => {
			await this.sleepMs(ms);
			if (!cancelled) onFire();
			return undefined;
		})();
		return { promise, cancel: () => (cancelled = true) };
	}

	/**
	 * THE reconnect loop body (public for deterministic fixture driving):
	 * reopen the stream after ladder[backoffIdx] seconds; reset the index when
	 * the previous stream stayed alive ≥60s. Fatal codes stop everything.
	 */
	async runReconnectCycle(): Promise<boolean> {
		if (!this.running) return false;
		const aliveMs = this.nowMs() - this.streamStartAtMs;
		if (aliveMs >= NTFY_LADDER_RESET_ALIVE_MS) this.backoffIdx = 0;
		const delayS =
			NTFY_RECONNECT_BACKOFF_S[
				Math.min(this.backoffIdx, NTFY_RECONNECT_BACKOFF_S.length - 1)
			] ?? 60;
		this.reconnectLog.push(`[${delayS}s@${this.backoffIdx}]`);
		this.reconnectCount += 1;
		await this.sleepMs(delayS * 1000);
		this.backoffIdx += 1;
		if (!this.running) return false;
		try {
			this.streamStartAtMs = this.nowMs();
			this.activeStream = this.openStream();
		} catch (err) {
			if (err instanceof FatalStreamError) return false;
			return true; // transient — caller may cycle again
		}
		this.streamTask = this.consumeStream(this.activeStream as FakeNtfyStream);
		return true;
	}

	override async disconnect(): Promise<void> {
		this.running = false;
		this.activeStream?.close();
		this.activeStream = null;
	}

	// ── inbound message processing (_on_message gates, exact order) ──

	private async handleMessageEvent(event: StreamEventMessage): Promise<void> {
		const msgId = event.id || "uuid-fallback";
		if (this.isDuplicate(msgId)) {
			this.dedupHits.duplicates += 1;
			return;
		}
		// Echo-loop prevention: our own X-Tags marker.
		if ((event.tags ?? []).includes(NTFY_ECHO_TAG)) {
			this.dedupHits.echoSkips += 1;
			return;
		}
		const text = (event.message ?? "").trim();
		if (text.length === 0) return; // empty bodies skipped

		// Trust boundary: user_id FIXED to the topic; title NEVER consulted.
		const chatTopic = event.topic || this.topic;
		const incoming: IncomingEvent = {
			messageType: "text",
			text,
			source: {
				platform: "ntfy",
				chatType: "dm",
				userId: chatTopic,
				chatId: chatTopic,
				chatName: chatTopic,
			},
		};
		if (!this.running || this.inboundHoldGate) {
			// In-flight window discipline (held across stream death / gate).
			if (this.heldInbound.length >= 64) this.heldInbound.shift();
			this.heldInbound.push(incoming);
			return;
		}
		await this.deliverInbound(incoming, sessionKeyFor(incoming));
	}

	private readonly heldInbound: IncomingEvent[] = [];
	private inboundHoldGate = false;
	/**
	 * Fixture seam arming the in-flight hold window deterministically (the
	 * production path also holds whenever !running — same queue, same drain).
	 */
	setInboundHoldGate(on: boolean): void {
		this.inboundHoldGate = on;
	}
	get heldInboundCount(): number {
		return this.heldInbound.length;
	}

	private drainHeldInbound(): void {
		if (this.heldInbound.length === 0) return;
		const drained = [...this.heldInbound];
		this.heldInbound.length = 0;
		for (const event of drained) {
			this.spawn(async () => {
				await this.deliverInbound(event, sessionKeyFor(event));
			});
		}
	}

	/** adapter.py:_is_duplicate — window eviction past DEDUP_MAX_SIZE. */
	isDuplicate(msgId: string): boolean {
		const now = this.nowMs();
		if (this.seenMessages.size > NTFY_DEDUP_MAX_SIZE) {
			const cutoff = now - NTFY_DEDUP_WINDOW_MS;
			for (const [k, ts] of [...this.seenMessages]) {
				if (ts <= cutoff) this.seenMessages.delete(k);
			}
		}
		if (this.seenMessages.has(msgId)) return true;
		this.seenMessages.set(msgId, now);
		return false;
	}

	// ══════════════════════════════════════════════════════════════════════
	// Guard wiring + egress doors
	// ══════════════════════════════════════════════════════════════════════

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: NTFY_REGISTRY,
				messageHandler: async (event, ctx) => {
					const text = event.text ?? `[${String(event.messageType)}]`;
					const sessionKey = String(
						event.metadata?.["gateway_session_key"] ?? "",
					);
					if (this.clarifyArmedSet.has(sessionKey) && !text.startsWith("/")) {
						this.clarifyCaptures.push(text);
						return null;
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
					return `reply:${text}`;
				},
				sendReply: async (_chatId, text) => {
					this.replyLog.push(text);
				},
			},
			{
				...(spawner === undefined ? {} : { spawner }),
				hasPendingClarify: (key) => this.clarifyArmedSet.has(key),
			},
		);
	}

	get clarifyArmed(): Set<string> {
		return this.clarifyArmedSet;
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
		if (String(event.source?.userId ?? "") === "bot-self") return;
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	protected override chatDescriptorFor(chatId: string):
		| {
				maxMessageLength?: number | undefined;
				lenUnit?: import("../kit/length-policy.js").LengthUnit | undefined;
		  }
		| undefined {
		if (chatId.includes("utf16")) {
			return { maxMessageLength: 30, lenUnit: "utf16" };
		}
		return undefined;
	}

	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith("(Response formatting failed, plain text:")
		) {
			return Promise.resolve({
				success: false,
				error: "Bad Request: can't parse entities",
			});
		}
		const publishTopic =
			typeof metadata["publish_topic"] === "string"
				? (metadata["publish_topic"] as string)
				: this.publishTopic || chatId;
		// ntfy truncates at 4096 chars — NO chunking (splitsLongMessages=false).
		const body = content.slice(0, NTFY_MAX_MESSAGE_CHARS);
		const headers: Record<string, string> = {
			...this.authHeaders,
			"Content-Type": "text/plain; charset=utf-8",
			"X-Tags": NTFY_ECHO_TAG,
		};
		if (this.markdownActive) headers["X-Markdown"] = "true";
		void NTFY_PUBLISH_TIMEOUT_MS; // modeled by the fake's sync surface

		const response = await this.wireTransmitPublish(publishTopic, body, {
			...metadata,
			ntfy_headers: headers,
			// Harness-wire keying: the LOGICAL chat id travels beside the wire
			// topic (they differ when publish_topic is configured separately).
			ntfy_target_chat_id: chatId,
		});
		this.captureRetryAfter(response.retryAfter);
		if (!response.success) return response;
		return { success: true, messageId: response.messageId };
	}

	async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		const policy = this.chatLengthPolicyForChat(chatId);
		if (policy.lenFn(content) <= policy.maxUnits) {
			return [await this.deliverViaLadder(chatId, content, metadata)];
		}
		// §6.3/A15: the chat length PAIR governs ALL text delivery. The vendor
		// 4096 cap stays as the DOOR-level protocol safety net below.
		const lines = planLabeledParagraphs(content, policy.maxUnits, policy.lenFn);
		const total = lines.length;
		const out: SendResult[] = [];
		for (let i = 0; i < total; i++) {
			const raw = lines[i] ?? "";
			const labeled = total > 1 ? `${raw} (${i + 1}/${total})` : raw;
			out.push(await this.deliverViaLadder(chatId, labeled, metadata));
		}
		return out;
	}

	private ladderInstance: FormattingLadder | null = null;
	private ladderChatId = "";

	private ensureLadder(): FormattingLadder {
		if (this.ladderInstance === null) {
			this.ladderInstance = new FormattingLadder({
				tryRich: (content, md) => this.wireRich(content, md),
				sendConverted: (content, md) =>
					this.wireSend(this.ladderChatId, content, md),
				sendPlain: (content, md) =>
					this.wireSend(this.ladderChatId, content, md),
			});
		}
		return this.ladderInstance;
	}

	private async deliverViaLadder(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		this.ladderChatId = chatId;
		const outcome = await this.ensureLadder().sendText(content, metadata);
		if (outcome.success) return outcome;
		if (outcome.tier === "rich") return outcome;
		const failureClass = classifySendError(new Error(outcome.error ?? ""));
		const networkClassified =
			outcome.retryable === true ||
			failureClass === "connect-timeout" ||
			failureClass === "network" ||
			failureClass === "flood";
		if (networkClassified) {
			this.captureRetryAfter(outcome.retryAfter);
			const clock = this.clock;
			const retried = await sendWithRetry(
				content,
				metadata,
				(c, md) => this.wireSend(chatId, c, md),
				{
					maxRetries: 2,
					...(clock === undefined
						? {}
						: { sleep: (ms: number) => this.sleepMs(ms) }),
				},
			);
			if (retried.success) return retried;
			return this.wireSend(chatId, DELIVERY_FAILED_NOTICE, metadata);
		}
		if (failureClass === "formatting") {
			const { forceFormattingError: _ignored, ...plainMeta } = metadata;
			void _ignored;
			return this.wireSend(
				chatId,
				plainTextFallbackBody(content),
				plainMeta as Metadata,
			);
		}
		return outcome;
	}

	private captureRetryAfter(seconds: number | null | undefined): void {
		if (seconds !== null && seconds !== undefined) {
			this.lastCapturedRetryAfterSeconds = seconds;
		}
	}

	async transientRichOutcome(
		_chatId: string,
		content: string,
	): Promise<SendResult> {
		const { FormattingLadder } = await import("../kit/formatting-ladder.js");
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

	async parseFailureResendContent(
		chatId: string,
		content: string,
	): Promise<string> {
		await this.deliverText(chatId, content, { forceFormattingError: true });
		return this.lastSendContentReader(chatId);
	}

	protected override async wireRich(
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		if (!this.richScriptedProbe()) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return this.wireTransmitRich(content, metadata);
	}

	protected override wireEdit(
		_c: string,
		_m: string,
		_x: string,
		_o: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		return Promise.resolve({ success: false, error: "Not supported" });
	}
	protected override wireDraft(_a: DraftFrameArgs): Promise<SendResult> {
		return Promise.resolve({ success: false, error: "Not supported" });
	}
	sendTyping(_chatId: string): void {}

	// ── identity probes ──

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}
	routerAuditResolved(): readonly string[] {
		return this.routerResolved;
	}

	secondInstanceTokenLockAttempt():
		| { acquired: false; holderOwner: string }
		| { acquired: true } {
		const credentialId = `topic:${this.topic}`;
		if (!this.lockHeld) {
			const first = this.acquireCredentialLock(
				this.lockManager,
				"ntfy-topic",
				credentialId,
				"instance-A",
			);
			if (!first.acquired) return { acquired: false, holderOwner: "?" };
			this.lockHeld = true;
		}
		try {
			this.acquireCredentialLock(
				this.lockManager,
				"ntfy-topic",
				credentialId,
				"instance-B",
			);
			return { acquired: true };
		} catch {
			const holder = this.lockManager.holderOf("ntfy-topic", credentialId);
			return { acquired: false, holderOwner: holder?.owner ?? "?" };
		}
	}

	buildMissingSecretSibling(): NtfyAdapter {
		return new NtfyAdapter({
			...this.deps,
			manifestName: `${this.manifestName}-no-secret`,
			secretReader: () => undefined,
		});
	}

	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.lifecycle.statusSnapshot();
	}

	// ── seams ──

	private markFatal(code: string, detail: string): void {
		this.fatalCodes.push({ code, detail });
		this.lifecycle.markFatal({
			kind: "config_invalid",
			detail: `[${code}] ${detail}`,
		});
	}

	private recoveryNote(reason: string): void {
		this.reconnectLog.push(`reason:${reason}`);
	}

	private nowMs(): number {
		if (this.clock !== undefined) return this.clock.nowMs();
		return Date.now();
	}

	private async sleepMs(ms: number): Promise<void> {
		if (this.clock !== undefined) {
			await this.clock.sleepMs(ms);
			return;
		}
		await new Promise<void>((r) => setTimeout(r, Math.min(ms, 5)));
	}
}

// ── helpers ──────────────────────────────────────────────────────────────

function sessionKeyFor(event: IncomingEvent): string {
	return `ntfy:${String(event.source?.chatId ?? "?")}`;
}

/** Policy-split paragraphs with label-width reserve (kit scaffold parity). */
function planLabeledParagraphs(
	content: string,
	maxUnits: number,
	lenFn: (s: string) => number,
): string[] {
	const budget = maxUnits - 8;
	let lines = hardSplit(content, budget, lenFn);
	if (lines.length <= 1) return lines;
	for (let pass = 0; pass < 4; pass++) {
		const n = lines.length;
		const labelWidth = lenFn(` (${n}/${n})`);
		lines = hardSplit(content, budget - labelWidth, lenFn);
		if (lines.length === n) break;
	}
	return lines;
}

function hardSplit(
	content: string,
	budget: number,
	lenFn: (s: string) => number,
): string[] {
	const out: string[] = [];
	for (const paragraph of content.split("\n")) {
		if (paragraph.trim().length === 0) continue;
		if (lenFn(paragraph) <= budget) {
			out.push(paragraph);
			continue;
		}
		let rest = paragraph;
		while (lenFn(rest) > budget && rest.length > 0) {
			let low = 1;
			let high = rest.length;
			let bestCut = 0;
			while (low <= high) {
				const mid = (low + high) >> 1;
				if (lenFn(rest.slice(0, mid)) <= budget) {
					bestCut = mid;
					low = mid + 1;
				} else {
					high = mid - 1;
				}
			}
			if (bestCut === 0) break;
			out.push(rest.slice(0, bestCut).trimEnd());
			rest = rest.slice(bestCut).trimStart();
		}
		if (rest.length > 0) out.push(rest);
	}
	return out.length > 0 ? out : [content];
}

function brief(err: unknown): string {
	return String(err instanceof Error ? err.message : err).slice(0, 120);
}
void brief;
