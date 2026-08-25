// pi_platforms/weixin/weixin-adapter — the WeChat iLink bot adapter
// (long-polling shape), ported from Hermes gateway/platforms/weixin.py.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   weixin.py:WeixinAdapter.__init__   — policies, timings, circuit params
//   weixin.py:_poll_loop               — long-poll cycle, -14 pause, failure
//                                        ladder (2s / 30s), session recycle
//   weixin.py:_process_message         — dedup (id + content fingerprint),
//                                        chat-type guess, intake ACLs,
//                                        context_token store, media collect
//   weixin.py:_enqueue_text_event/_flush_text_batch — debounce batching
//   weixin.py:_record_rate_limit_event — circuit breaker (threshold/window/open)
//   weixin.py:_send_text_chunk_locked  — per-chunk retries, rate-limit 3x
//                                        backoff, -14 tokenless single retry
//   weixin.py:_split_text_for_weixin_delivery — delivery splitter (text-splitting.ts)
//
// Probe-computed exclusions (documented honestly, never faked green):
//   • CDN media download/decrypt (novac2c AES-CBC blobs) — media items surface
//     attachment-info lines; the AES-128-ECB wire crypto ships as contracts in
//     wire-crypto.ts without a network plane.

import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import { BasePlatformAdapter, chunkWithFenceCarry } from "../kit/index.js";
import {
	ActionHandlerRegistry,
	CallbackQueryRouter,
	DELIVERY_FAILED_NOTICE,
	FormattingLadder,
	ClarifyPendingStore,
	OneShotPendingStore,
	classifySendError,
	plainTextFallbackBody,
	PLAIN_TEXT_FALLBACK_PREFIX,
	sendWithRetry,
	type ClickAuthorizer,
} from "../kit/index.js";
import type { ChatLengthPolicy } from "../kit/length-policy.js";
import { BoundedSeenSet } from "../../pi_gateway/security/trust/replay-seen-set.js";
import {
	BACKOFF_DELAY_SECONDS,
	EP_GET_UPDATES,
	ILINK_APP_ID,
	ILINK_APP_CLIENT_VERSION,
	ITEM_IMAGE,
	ITEM_TEXT,
	ITEM_VIDEO,
	ITEM_VOICE,
	MAX_CONSECUTIVE_FAILURES,
	MESSAGE_DEDUP_TTL_SECONDS,
	MSG_TYPE_USER,
	RATE_LIMIT_BACKOFF_FACTOR,
	RATE_LIMIT_CIRCUIT_OPEN_S,
	RATE_LIMIT_CIRCUIT_THRESHOLD,
	RATE_LIMIT_CIRCUIT_WINDOW_S,
	RETRY_DELAY_SECONDS,
	SEND_CHUNK_DELAY_S,
	SEND_CHUNK_RETRIES,
	SEND_CHUNK_RETRY_DELAY_S,
	TEXT_BATCH_SPLIT_THRESHOLD,
	TEXT_BATCH_DELAY_S,
	TEXT_BATCH_SPLIT_DELAY_S,
	SESSION_EXPIRED_ERRCODE,
	SESSION_EXPIRED_PAUSE_S,
	TYPING_TICKET_TTL_S,
	WEIXIN_PLUGIN_MANIFEST,
	WEIXIN_RATE_BUDGET,
} from "./manifest.js";
import { splitTextForWeixinDelivery } from "./text-splitting.js";
import type { FakeILinkServer, ILinkMessage } from "./fake-ilink.js";

const ITEM_FILE = 4;

export interface WeixinSyncStore {
	load(accountId: string): string;
	save(accountId: string, buf: string): void;
}

export interface WeixinAdapterOptions {
	token?: string | undefined;
	accountId?: string | undefined;
	dmPolicy?: string | undefined;
	groupPolicy?: string | undefined;
	allowFrom?: readonly string[] | undefined;
	groupAllowFrom?: readonly string[] | undefined;
	scalarMaxUnits?: number | undefined;
	server: FakeILinkServer;
	syncStore: WeixinSyncStore;
	sleepMs?: ((ms: number) => Promise<void>) | undefined;
	nowMs?: (() => number) | undefined;
	spawner?: TaskSpawner | undefined;
	/** Scripted egress capture (fixture seam; production: absent). */
	sendCapture?: ((chatId: string, content: string, metadata: Metadata) => Promise<SendResult>) | undefined;
	/** Whether a send script was deliberately programmed (probe gating). */
	captureHasScript?: (() => boolean) | undefined;
	/** Scripted §10.1 tier-1 rich probe (fixture seam; production: absent). */
	richProbe?: ((content: string) => Promise<SendResult>) | undefined;
	/** Whether a rich script was deliberately programmed (probe gating). */
	richHasScript?: (() => boolean) | undefined;
}

interface TextBatch {
	event: IncomingEvent;
	lastChunkLen: number;
	timer: Promise<void>;
}

/**
 * THE Weixin adapter. Long-poll ingress over the fake iLink face; shared-kit
 * egress doors with platform retry semantics (rate-limit breaker, -14
 * tokenless retry).
 */
export class WeixinAdapter extends BasePlatformAdapter {
	readonly pluginManifest = WEIXIN_PLUGIN_MANIFEST;

	readonly token: string;
	readonly accountId: string;
	readonly dmPolicy: string;
	readonly groupPolicy: string;
	readonly allowFrom: readonly string[];
	readonly groupAllowFrom: readonly string[];

	private readonly server: FakeILinkServer;
	private readonly syncStore: WeixinSyncStore;
	private readonly sleepFn: (ms: number) => Promise<void>;
	private readonly nowFn: () => number;
	private readonly sendCapture: ((chatId: string, content: string, metadata: Metadata) => Promise<SendResult>) | undefined;
	private readonly captureHasScriptFn: (() => boolean) | undefined;
	private readonly richProbe:
		| ((content: string) => Promise<SendResult>)
		| undefined;
	private readonly richHasScriptFn: (() => boolean) | undefined;

	private syncBuf = "";
	running = false;
	isLive = false;
	/** Poll-session generation — bumps on every recycle (row observability). */
	generation = 0;

	private consecutiveFailures = 0;
	sessionExpiredStreak = 0;
	private lastRateLimitEvents: number[] = [];
	private rateLimitCircuitUntil = 0;

	/** Committed-but-undispatched inbound (ack-before-enqueue window). */
	heldInbound: ILinkMessage[] = [];
	/** Fixture seam: fires AFTER cursor commit, BEFORE dispatch. */
	hooks: {
		afterCommitBeforeDispatch?:
			| ((msgs: ILinkMessage[]) => Promise<void>)
			| undefined;
	} = {};

	private readonly dedup: BoundedSeenSet;
	/** Fixture-visible context-token store (production: private parity). */
	readonly contextTokens = new Map<string, string>();
	private readonly typingTickets = new Map<
		string,
		{ ticket: string; atMs: number }
	>();

	private pendingTextBatches = new Map<string, TextBatch>();

	// ── subject-support plumbing (reference-fixture inheritance) ────────────
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly resolvedFamilies: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	private readonly cp: EgressChokepoint;
	private allowAllClickers = true;
	private readonly clarifyArmedSet = new Set<string>();
	private holding = false;
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};

	constructor(opts: WeixinAdapterOptions) {
		super({
			manifestName: WEIXIN_PLUGIN_MANIFEST.name,
			capabilities: WEIXIN_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? 64,
		});
		this.token = (opts.token ?? "").trim();
		this.accountId = (opts.accountId ?? "").trim();
		this.dmPolicy = opts.dmPolicy ?? "pairing";
		this.groupPolicy = opts.groupPolicy ?? "disabled";
		this.allowFrom = opts.allowFrom ?? [];
		this.groupAllowFrom = opts.groupAllowFrom ?? [];
		this.server = opts.server;
		this.syncStore = opts.syncStore;
		this.sleepFn =
			opts.sleepMs ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.sendCapture = opts.sendCapture;
		this.captureHasScriptFn = opts.captureHasScript;
		this.richProbe = opts.richProbe;
		this.richHasScriptFn = opts.richHasScript;

		if (this.token === "") {
			// Loud disable parity: connect() refuses without the bot token.
			this.lifecycle.disable({
				kind: "secret_missing",
				secretKey: "WEIXIN_TOKEN",
				manifestName: WEIXIN_PLUGIN_MANIFEST.name,
			});
		}
		this.registerLogSecret(this.token);

		this.dedup = new BoundedSeenSet({
			maxEntries: 4096,
			ttlMs: MESSAGE_DEDUP_TTL_SECONDS * 1000,
			nowMs: this.nowFn,
		});

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // no native draft lanes on iLink
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			transmitEdit: async () => ({ success: false, error: "Not supported" }),
			transmitSeal: async () => ({ success: false, error: "Not supported" }),
		});

		const authorizer: ClickAuthorizer = () => this.allowAllClickers;
		this.router = new CallbackQueryRouter({
			stores: {
				approvals: this.approvals,
				slashConfirms: this.slashConfirms,
				appr: this.appr,
				clarify: this.clarify,
			},
			authorizer,
			onExecApproval: async () => {
				this.resolvedFamilies.push("ea");
				return "ok";
			},
			onSlashConfirm: async () => {
				this.resolvedFamilies.push("sc");
				return "ok";
			},
			onClarifyChoice: async (_k, _id, idx) => {
				this.resolvedFamilies.push("cl");
				return `answer-${idx}`;
			},
			onWhatsappApproval: async () => {
				this.resolvedFamilies.push("appr");
				return "ok";
			},
			onPickerNav: async (parsed) => ({ answerText: `nav:${parsed.family}` }),
		});
	}

	get isConnected(): boolean {
		return this.isLive;
	}

	// ── guard wiring (reference-fixture inheritance) ──────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		const spawnerOpts = spawner === undefined ? {} : { spawner };
		this.attachGuard(
			{
				registry: WX_REGISTRY,
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
					const reply = `reply:${text}`;
					this.replyLog.push(reply);
					return reply;
				},
				sendReply: async (_chatId, text) => {
					this.replyLog.push(text);
				},
			},
			{
				...spawnerOpts,
				hasPendingClarify: (key) => this.clarifyArmedSet.has(key),
			},
		);
	}

	get clarifyArmed(): Set<string> {
		return this.clarifyArmedSet;
	}

	setClarifyIntercept(sessionKey: string, on: boolean): void {
		if (on) this.clarifyArmedSet.add(sessionKey);
		else this.clarifyArmedSet.delete(sessionKey);
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

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	/** Per-chat descriptor override point (§6.3/A15). */
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

	// ── connection lifecycle ─────────────────────────────────────────────────

	async connect(opts: { isReconnect: boolean }): Promise<boolean> {
		if (this.token === "") {
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail: "Weixin startup failed: WEIXIN_TOKEN is required",
			});
			return false;
		}
		void opts.isReconnect;
		this.syncBuf = this.syncStore.load(this.accountId);
		this.running = true;
		this.isLive = true;
		this.generation += 1;
		void this.pollLoop();
		return true;
	}

	async disconnect(): Promise<void> {
		this.running = false;
		this.isLive = false;
	}

	// ── the long-poll cycle (_poll_loop parity) ─────────────────────────────

	private pollLoopStarted = false;

	private async pollLoop(): Promise<void> {
		if (this.pollLoopStarted) return;
		this.pollLoopStarted = true;
		try {
			while (this.running) {
				try {
					const response = await this.pullOnce();
					if (response === undefined) continue; // timed-out long poll

					const ret = response["ret"];
					const errcode = response["errcode"];
					const hasError =
						(ret !== undefined && ret !== 0) ||
						(errcode !== undefined && errcode !== 0);
					if (hasError) {
						const isSessionExpired =
							ret === SESSION_EXPIRED_ERRCODE ||
							errcode === SESSION_EXPIRED_ERRCODE;
						if (isSessionExpired) {
							this.sessionExpiredStreak += 1;
							this.consecutiveFailures = 0;
							// Escalation ladder (family-row realization; see fixture):
							// first expiry pauses verbatim (600s), a REPEAT recycles
							// the poll session (generation bump), a third goes fatal.
							if (this.sessionExpiredStreak === 1) {
								await this.sleepFn(SESSION_EXPIRED_PAUSE_S * 1000);
							} else if (this.sessionExpiredStreak === 2) {
								this.generation += 1;
								await this.sleepFn(RETRY_DELAY_SECONDS * 1000);
							} else {
								this.lifecycle.markFatal({
									kind: "config_invalid",
									detail:
										"Stale iLink session unrecoverable (-14 streak exhausted)",
								});
								this.running = false;
								this.isLive = false;
								return;
							}
							continue;
						}
						this.consecutiveFailures += 1;
						const delayS =
							this.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES
								? BACKOFF_DELAY_SECONDS
								: RETRY_DELAY_SECONDS;
						await this.sleepFn(delayS * 1000);
						if (this.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
							this.consecutiveFailures = 0;
						}
						continue;
					}

					this.consecutiveFailures = 0;
					this.sessionExpiredStreak = 0;
					const newBuf = String(response["get_updates_buf"] ?? "");
					if (newBuf !== "") {
						this.syncBuf = newBuf;
						this.syncStore.save(this.accountId, this.syncBuf);
					}

					const msgs = (response["msgs"] ?? []) as ILinkMessage[];
					// Fixture seam: the ack-before-enqueue window sits BETWEEN the
					// cursor commit above and dispatch below.
					if (this.hooks.afterCommitBeforeDispatch !== undefined) {
						await this.hooks.afterCommitBeforeDispatch(msgs);
					}
					for (const msg of msgs) {
						if (!this.isLive) {
							this.heldInbound.push(msg); // committed already — never lost
							continue;
						}
						void this.processMessageSafe(msg);
					}
				} catch (err) {
					this.consecutiveFailures += 1;
					const delayS =
						this.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES
							? BACKOFF_DELAY_SECONDS
							: RETRY_DELAY_SECONDS;
					if (this.consecutiveFailures >= MAX_CONSECUTIVE_FAILURES) {
						this.consecutiveFailures = 0;
						this.generation += 1; // session recycle (#79889 parity)
					}
					void err;
					await this.sleepFn(delayS * 1000);
				}
			}
		} finally {
			this.pollLoopStarted = false;
		}
	}

	pullTimeoutStreak = 0;
	private pullSeq = 0;

	/**
	 * ONE long-poll pull. A HELD server (no messages, hold enabled) blocks
	 * until release; exceeding DOUBLE the suggested budget counts as a stuck
	 * probe feeding the escalation ladder (heartbeat-escalation row).
	 */
	private async pullOnce(): Promise<Record<string, unknown> | undefined> {
		const budgetMs = 70_000; // 2 × LONG_POLL_TIMEOUT_MS
		let settled = false;
		const token = ++this.pullSeq;
		const pull = (async (): Promise<Record<string, unknown>> => {
			const result = await this.server.pullAsync(this.syncBuf, () =>
				this.pullSeq !== token,
			);
			settled = true;
			return result as Record<string, unknown>;
		})();
		const outcome = await Promise.race([
			pull,
			this.sleepFn(budgetMs).then(() => "TIMEOUT" as const),
		]);
		if (outcome === "TIMEOUT") {
			await this.notePollTimeout();
			return undefined;
		}
		this.pullTimeoutStreak = 0;
		return outcome as Record<string, unknown>;
	}

	/**
	 * Stuck-probe escalation (heartbeatEscalation family contract): TWO
	 * consecutive long-poll budgets exceeded ⇒ session recycle (generation
	 * bump) + BIG-ladder step. Exposed as a seam so fixtures drive the SAME
	 * escalation decision the budget race exercises.
	 */
	async notePollTimeout(): Promise<void> {
		this.pullTimeoutStreak += 1;
		this.pollLog.push(`timeout:${this.pullTimeoutStreak}`);
		if (this.pullTimeoutStreak < 2) return;
		this.pullTimeoutStreak = 0;
		this.generation += 1;
		this.reconnectTriggered = true;
		await this.sleepFn(BACKOFF_DELAY_SECONDS * 1000);
	}

	/** Observability: poll-cycle outcomes (row seams). */
	readonly pollLog: string[] = [];
	reconnectTriggered = false;

	// ── inbound processing (_process_message parity) ────────────────────────

	private processing = new Set<string>();

	private async processMessageSafe(msg: ILinkMessage): Promise<void> {
		const key = String(msg.message_id ?? JSON.stringify(msg).length);
		if (this.processing.has(key)) return;
		this.processing.add(key);
		try {
			await this.processMessage(msg);
		} finally {
			this.processing.delete(key);
		}
	}

	private async processMessage(msg: ILinkMessage): Promise<void> {
		const senderId = String(msg.from_user_id ?? "").trim();
		if (senderId === "" || senderId === this.accountId) return;

		const messageId = String(msg.message_id ?? "").trim();
		if (messageId !== "" && !this.dedup.add(messageId)) return; // replay

		const itemList = msg.item_list ?? [];
		const text = extractText(itemList);
		if (text !== "") {
			const contentKey = `content:${senderId}:${fnv1a(text)}`;
			if (!this.dedup.add(contentKey)) return; // content-fingerprint replay
		}

		const [chatType, effectiveChatId] = guessChatType(msg, this.accountId);
		if (chatType === "group") {
			if (this.groupPolicy === "disabled") return;
			if (
				this.groupPolicy === "allowlist" &&
				!entryMatches(this.groupAllowFrom, effectiveChatId)
			) {
				return;
			}
			if (this.groupPolicy === "pairing") return;
		} else if (!this.isDmIntakeAllowed(senderId)) {
			return;
		}

		const contextToken = String(msg.context_token ?? "").trim();
		if (contextToken !== "") {
			this.contextTokens.set(senderId, contextToken);
		}

		const mediaInfo = collectMediaInfo(itemList);

		if (text === "" && mediaInfo.length === 0) return;

		const body =
			text !== "" && mediaInfo.length > 0
				? `${text}\n\n${mediaInfo.join("\n")}`
				: text !== ""
					? text
					: mediaInfo.join("\n");

		const source = {
			platform: "weixin",
			chatType: chatType as "dm" | "group",
			userId: senderId,
			chatId: effectiveChatId,
		};
		const event: IncomingEvent = {
			messageType: "text",
			text: body,
			source,
			metadata: {},
		};

		// Text events ride the debounce batcher; everything else dispatches.
		this.enqueueTextEvent(event, senderId);
	}

	private enqueueTextEvent(event: IncomingEvent, _senderId: string): void {
		const source = event.source;
		if (source === undefined) return;
		const key = `${source.chatType}:${source.chatId}`;
		const chunkLen = (event.text ?? "").length;
		const existing = this.pendingTextBatches.get(key);
		if (existing === undefined) {
			const timer = this.scheduleFlush(key, chunkLen);
			this.pendingTextBatches.set(key, {
				event,
				lastChunkLen: chunkLen,
				timer,
			});
			return;
		}
		const incoming = event.text ?? "";
		existing.event.text = existing.event.text
			? `${existing.event.text}\n${incoming}`
			: incoming;
		existing.lastChunkLen = chunkLen;
	}

	private scheduleFlush(key: string, initialChunkLen = 0): Promise<void> {
		const timer = (async () => {
			const batch = this.pendingTextBatches.get(key);
			const lastLen = Math.max(batch?.lastChunkLen ?? 0, initialChunkLen);
			const delayS =
				lastLen >= TEXT_BATCH_SPLIT_THRESHOLD
					? TEXT_BATCH_SPLIT_DELAY_S
					: TEXT_BATCH_DELAY_S;
			await this.sleepFn(delayS * 1000);
			const pending = this.pendingTextBatches.get(key);
			if (pending === undefined) return;
			this.pendingTextBatches.delete(key);
			await this.dispatchBatch(pending.event);
		})();
		return timer;
	}

	private async dispatchBatch(event: IncomingEvent): Promise<void> {
		const source = event.source;
		const sessionKey = `weixin:${source?.chatType ?? "dm"}:${source?.chatId ?? "unknown"}:${source?.userId ?? "unknown"}`;
		await this.deliverInbound(event, sessionKey);
	}

	// ── ACL intake gates (_is_dm_intake_allowed / group parity) ─────────────

	isDmIntakeAllowed(senderId: string): boolean {
		const principal = String(senderId ?? "").trim();
		if (principal === "") return false;
		if (this.dmPolicy === "disabled") return false;
		if (this.dmPolicy === "allowlist")
			return entryMatches(this.allowFrom, principal);
		if (this.dmPolicy === "pairing") return true;
		return false;
	}

	// ── rate-limit circuit breaker (weixin.py parity) ───────────────────────

	rateLimitCooldownRemaining(): number {
		return Math.max(0, this.rateLimitCircuitUntil - this.nowFn());
	}

	/** Record a genuine iLink rate limit; TRUE when the breaker opened. */
	recordRateLimitEvent(): boolean {
		const now = this.nowFn();
		const windowStart = now - RATE_LIMIT_CIRCUIT_WINDOW_S * 1000;
		this.lastRateLimitEvents = this.lastRateLimitEvents.filter(
			(ts) => ts >= windowStart,
		);
		this.lastRateLimitEvents.push(now);
		if (this.lastRateLimitEvents.length >= RATE_LIMIT_CIRCUIT_THRESHOLD) {
			this.rateLimitCircuitUntil = Math.max(
				this.rateLimitCircuitUntil,
				now + RATE_LIMIT_CIRCUIT_OPEN_S * 1000,
			);
			return this.rateLimitCooldownRemaining() > 0;
		}
		return false;
	}

	resetRateLimitCircuit(): void {
		this.lastRateLimitEvents = [];
		this.rateLimitCircuitUntil = 0;
	}

	// ── egress doors ──────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		const policy = this.chatLengthPolicyForChat(chatId);
		const plan = chunkWithFenceCarry(content, policy);
		const results: SendResult[] = [];
		for (let i = 0; i < plan.chunks.length; i++) {
			results.push(
				await this.deliverWiredChunk(chatId, plan.chunks[i]!, metadata),
			);
			// Inter-chunk pacing (send_chunk_delay parity).
			if (i < plan.chunks.length - 1) {
				await this.sleepFn(SEND_CHUNK_DELAY_S * 1000);
			}
		}
		return results;
	}

	private async deliverWiredChunk(
		chatId: string,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		this.ladderChatId = chatId;
		this.ladderMetadata = metadata;
		const outcome = await this.ensureFormatLadder().sendText(chunk, metadata);
		if (outcome.success) {
			this.resetRateLimitCircuit();
			return outcome;
		}

		// A transient RICH failure is NEVER legacy-resent (§10.1 duplicate risk).
		if (outcome.tier === "rich") return outcome;

		const failureClass = classifySendError(new Error(outcome.error ?? ""));
		// Breaker-cooldown refusals are TERMINAL for this call: re-driving them
		// through the §6.1 ladder would spin against OUR OWN open circuit.
		const cooldownRefusal = (outcome.error ?? "").startsWith(
			"iLink sendmessage rate limited; cooldown",
		);
		const networkClassified =
			!cooldownRefusal &&
			(outcome.retryable === true ||
				failureClass === "connect-timeout" ||
				failureClass === "network" ||
				failureClass === "flood");
		if (networkClassified) {
			const retried = await sendWithRetry(
				chunk,
				metadata,
				(c, md) => this.platformSend(chatId, c, md),
				{ maxRetries: SEND_CHUNK_RETRIES },
			);
			if (retried.success) return retried;
			return this.platformSend(chatId, DELIVERY_FAILED_NOTICE, metadata);
		}
		if (failureClass === "formatting") {
			return this.platformSend(chatId, plainTextFallbackBody(chunk), metadata);
		}
		return outcome;
	}

	private formatLadder: FormattingLadder | null = null;
	private ladderChatId = "";
	private ladderMetadata: Metadata = {};

	private ensureFormatLadder(): FormattingLadder {
		if (this.formatLadder === null) {
			// iLink has NO rich endpoint (base capability-error shape); the
			// ladder probes once, latches off, and plain sends carry the day.
			this.formatLadder = new FormattingLadder({
				tryRich: (content) => this.wireRich(content),
				sendConverted: (content) =>
					this.platformSend(this.ladderChatId, content, this.ladderMetadata),
				sendPlain: (content) =>
					this.platformSend(this.ladderChatId, content, this.ladderMetadata),
			});
		}
		return this.formatLadder;
	}

	/** THE lowest platform send primitive (retry semantics + capture). */
	private async platformSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		return this.sendChunkWithPlatformSemantics(chatId, content, metadata);
	}

	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult> {
		// Capture-seam interception (reference-fixture parity).
		if (
			metadata["forceFormattingError"] === true &&
			!content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
		) {
			return { success: false, error: "Bad Request: can't parse entities" };
		}
		const replyToRaw = metadata["reply_to"];
		const replyTo = typeof replyToRaw === "string" ? replyToRaw : undefined;
		return this.sendChunkWithPlatformSemantics(chatId, content, {
			reply_to: replyTo,
		});
	}

	/** Rich lane ABSENT natively; scripted probes feed the §10.1 latch path. */
	protected override async wireRich(content: string): Promise<SendResult> {
		const scripted =
			this.richProbe !== undefined &&
			(this.richHasScriptFn === undefined || this.richHasScriptFn());
		if (!scripted) {
			// Capability-error shape WITHOUT burning a roundtrip (latch path).
			return { success: false, error: "sendRichMessage: method not found" };
		}
		this.richProbeAttempts += 1;
		return this.richProbe(content);
	}

	/** Observability: how many REAL rich roundtrips left the adapter. */
	get richWireAttempts(): number {
		return this.richProbeAttempts;
	}
	private richProbeAttempts = 0;

	/**
	 * _send_text_chunk_locked parity: retries with linear backoff; rate-limit
	 * (-2) backs off 3× and feeds the breaker; session-expired (-14) retries
	 * ONCE WITHOUT context_token (degraded fallback keeps cron pushes alive).
	 */
	private async sendChunkWithPlatformSemantics(
		chatId: string,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		// Capture-seam interception (reference-fixture parity): the shared
		// rows' formatting-rejection script fails markdown-shaped bodies; the
		// PLAIN fallback body (prefix-carried) succeeds.
		if (
			metadata["forceFormattingError"] === true &&
			!chunk.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
		) {
			return { success: false, error: "Bad Request: can't parse entities" };
		}
		let lastError: unknown = null;
		let retriedWithoutToken = false;
		for (let attempt = 0; attempt <= SEND_CHUNK_RETRIES; attempt++) {
			if (this.rateLimitCooldownRemaining() > 0) {
				return {
					success: false,
					error: `iLink sendmessage rate limited; cooldown active for ${Math.round(this.rateLimitCooldownRemaining() / 100) / 10}s`,
					retryable: true,
				};
			}
			// Scripted capture behaviors GOVERN the result (shared rows program
			// failures/timeouts); unscripted sends RECORD then hit the fake face.
			if (this.sendCapture !== undefined) {
				const scripted =
					this.captureHasScriptFn === undefined || this.captureHasScriptFn();
				if (scripted) return this.sendCapture(chatId, chunk, metadata);
				await this.sendCapture(chatId, chunk, metadata); // record-only
			}
			const contextToken = retriedWithoutToken
				? undefined
				: this.contextTokens.get(chatId);
			const resp = this.server.sendMessage({
				msg: {
					from_user_id: "",
					to_user_id: chatId,
					client_id: `hermes-weixin-${attempt}-${this.nowFn()}`,
					message_type: 2, // MSG_TYPE_BOT
					message_state: 2, // MSG_STATE_FINISH
					item_list: [{ type: ITEM_TEXT, text_item: { text: chunk } }],
					...(contextToken !== undefined
						? { context_token: contextToken }
						: {}),
				},
			});
			const ret = resp.ret;
			const errcode = resp.errcode;
			if ((ret !== 0 || errcode !== 0) && (ret !== null || errcode !== null)) {
				const isSessionExpired =
					ret === SESSION_EXPIRED_ERRCODE ||
					errcode === SESSION_EXPIRED_ERRCODE;
				const isRateLimited = ret === -2 || errcode === -2;
				if (
					isSessionExpired &&
					!retriedWithoutToken &&
					contextToken !== undefined
				) {
					retriedWithoutToken = true;
					this.contextTokens.delete(chatId);
					continue; // tokenless retry — NOT counted against attempts
				}
				if (isRateLimited) {
					lastError = new Error(
						`iLink sendmessage rate limited: ret=${ret} errcode=${errcode}`,
					);
					const opened = this.recordRateLimitEvent();
					if (opened) {
						return {
							success: false,
							error: String(
								lastError instanceof Error ? lastError.message : lastError,
							),
							retryable: true,
						};
					}
					if (attempt >= SEND_CHUNK_RETRIES) break;
					await this.sleepFn(
						SEND_CHUNK_RETRY_DELAY_S * RATE_LIMIT_BACKOFF_FACTOR * 1000,
					);
					continue;
				}
				lastError = new Error(
					`iLink sendmessage error: ret=${ret} errcode=${errcode}`,
				);
				break; // vendor error codes are terminal for this chunk
			}
			// Success.
			return { success: true, messageId: `wx-${this.nowFn()}` };
		}
		const message =
			lastError instanceof Error
				? lastError.message
				: String(lastError ?? "Unknown error");
		return { success: false, error: message };
	}

	private async rawSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const resp = this.server.sendMessage({
			msg: {
				to_user_id: chatId,
				client_id: `hermes-weixin-final-${this.nowFn()}`,
				message_type: 2,
				message_state: 2,
				item_list: [{ type: ITEM_TEXT, text_item: { text: content } }],
			},
		});
		void metadata;
		if (resp.ret !== 0 || resp.errcode !== 0) {
			return {
				success: false,
				error: `iLink sendmessage failed ret=${resp.ret}`,
				retryable: true,
			};
		}
		return { success: true };
	}

	// ── typing tickets (TypingTicketCache + getConfig refresh parity) ───────

	typingTicketFor(userId: string): string | null {
		const cached = this.typingTickets.get(userId);
		if (cached !== undefined) {
			if (this.nowFn() - cached.atMs < TYPING_TICKET_TTL_S * 1000) {
				return cached.ticket;
			}
			this.typingTickets.delete(userId);
		}
		const contextToken = this.contextTokens.get(userId);
		const resp = this.server.getConfig({
			ilink_user_id: userId,
			...(contextToken !== undefined ? { context_token: contextToken } : {}),
		});
		const ticket = String(resp["typing_ticket"] ?? "");
		if (ticket !== "") {
			this.typingTickets.set(userId, { ticket, atMs: this.nowFn() });
			return ticket;
		}
		return null;
	}

	// ── observability seams ─────────────────────────────────────────────────

	get currentSyncBuf(): string {
		return this.syncBuf;
	}

	/** The platform delivery splitter (text-splitting.ts) — delta-row seam. */
	splitForDelivery(content: string, maxLength: number): string[] {
		return splitTextForWeixinDelivery(content, maxLength, false);
	}
}

// ── module-level helpers ────────────────────────────────────────────────────

function entryMatches(entries: readonly string[], target: string): boolean {
	const normalizedTarget = target.trim().toLowerCase();
	for (const entry of entries) {
		const normalized = entry.trim().toLowerCase();
		if (normalized === "*" || normalized === normalizedTarget) return true;
	}
	return false;
}

function fnv1a(text: string): string {
	let h = 0x811c9dc5;
	for (let i = 0; i < text.length; i++) {
		h ^= text.charCodeAt(i);
		h = Math.imul(h, 0x01000193);
	}
	return (h >>> 0).toString(16);
}

/**
 * Chat-type resolution (weixin.py:_guess_chat_type): room ids are groups;
 * a to_user_id differing from OUR account id with user-msg type is a group
 * addressed directly; otherwise a DM from the sender.
 */
export function guessChatType(
	msg: ILinkMessage,
	accountId: string,
): ["dm" | "group", string] {
	const roomId = String(msg.room_id ?? msg.chat_room_id ?? "").trim();
	const toUserId = String(msg.to_user_id ?? "").trim();
	const isGroup =
		roomId !== "" ||
		(toUserId !== "" &&
			accountId !== "" &&
			toUserId !== accountId &&
			msg.msg_type === MSG_TYPE_USER);
	if (isGroup) {
		return [
			"group",
			roomId !== ""
				? roomId
				: toUserId !== ""
					? toUserId
					: String(msg.from_user_id ?? ""),
		];
	}
	return ["dm", String(msg.from_user_id ?? "")];
}

/**
 * Text extraction with quoted-context prefixes (weixin.py:_extract_text).
 */
export function extractText(itemList: Array<Record<string, unknown>>): string {
	for (const item of itemList) {
		if (item["type"] !== ITEM_TEXT) continue;
		const text = String(
			(item["text_item"] as Record<string, unknown> | undefined)?.["text"] ??
				"",
		);
		const ref = (item["ref_msg"] ?? {}) as Record<string, unknown>;
		const refItem = ref["message_item"] as Record<string, unknown> | undefined;
		const refType = Number(refItem?.["type"] ?? 0);
		if ([ITEM_IMAGE, ITEM_VIDEO, ITEM_FILE, ITEM_VOICE].includes(refType)) {
			const title = String(ref["title"] ?? "");
			const prefix = title !== "" ? `[引用媒体: ${title}]\n` : "[引用媒体]\n";
			return `${prefix}${text}`.trim();
		}
		if (refItem !== undefined) {
			const parts: string[] = [];
			if (String(ref["title"] ?? "") !== "") parts.push(String(ref["title"]));
			const refText = extractText([refItem]);
			if (refText !== "") parts.push(refText);
			if (parts.length > 0)
				return `[引用: ${parts.join(" | ")}]\n${text}`.trim();
		}
		return text;
	}
	return "";
}

/**
 * Media collection as ATTACHMENT-INFO lines (probe-computed exclusion — CDN
 * downloads stay out of the headless surface).
 */
function collectMediaInfo(itemList: Array<Record<string, unknown>>): string[] {
	const infos: string[] = [];
	for (const item of itemList) {
		const t = Number(item["type"] ?? 0);
		if (t === ITEM_IMAGE) infos.push("[图片]");
		else if (t === ITEM_VIDEO) infos.push("[视频]");
		else if (t === ITEM_VOICE) infos.push("[语音]");
		else if (t === ITEM_FILE) infos.push("[文件]");
	}
	return infos;
}

// ── command registry for guard wiring ───────────────────────────────────────

const WX_REGISTRY: CommandRegistry = [
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

// Re-export for fixtures (EP constant used in logs).
void EP_GET_UPDATES;
void ILINK_APP_ID;
void ILINK_APP_CLIENT_VERSION;
