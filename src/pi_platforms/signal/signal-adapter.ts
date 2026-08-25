// pi_platforms/signal/signal-adapter — THE Signal adapter, ported from the
// READ-ONLY Hermes built-in gateway/platforms/signal.py onto the kit base.
// Everything policy-shaped is inherited or manifest DATA; this module supplies
// TRANSPORT (JSON-RPC 2.0 over the local signal-cli daemon + SSE inbound) and
// the envelope pipeline.
//
// Shape (DEC-002 ws family — persistent-pull inbound stream):
//   - inbound: SSE /api/v1/events — reconnect ladder initial 2s doubling ×2
//     capped 60s with 20% jitter, RESET on successful connect; ":" keepalive
//     comments refresh activity; health monitor probes /api/v1/check after
//     120s of SSE silence (alive ⇒ refresh, dead ⇒ force reconnect)
//   - outbound: JSON-RPC "send"/"sendTyping"/"sendReaction"; native markdown
//     formatting rides textStyle(s) bodyRanges (signal-format.ts)
//   - NO edits (SUPPORTS_MESSAGE_EDITING=False ⇒ supportsDraftStreaming is
//     PROBE-COMPUTED false; seal reality matches the declared exclusion) and
//     NO native buttons — the ONE kit callback router exists purely as the
//     resolution seam (clarify prompts render as plain-text numbered choices;
//     free-form answers resolve via the Lane C intercept)
//   - echo/self-sync discipline: recent sent-timestamp LRU (512 · TTL 300s)
//     suppresses Note-to-Self echoes; FIFO sent-timestamp cache (500) lets
//     inbound quotes detect replies-to-own-messages
//
// Daemon-dependent paths documented honestly (never faked green):
//   - attachment AAC→M4A remux is an INJECTED seam defaulting to the
//     no-ffmpeg pass-through (upstream shells out to ffmpeg when present);
//   - the daemon owns message history/replay — there is NO resume cursor;
//     loss-free reconnect coverage is modeled at the fixture (backlog), see
//     signal-wire.ts.
//
// Layering: imports pi_gateway downward + kit same-layer ONLY; no adapter
// cross-imports.

import { mkdirSync, statSync, writeFileSync } from "node:fs";
import { join } from "node:path";

import {
	ActionHandlerRegistry,
	BasePlatformAdapter,
	CallbackQueryRouter,
	ClarifyPendingStore,
	DELIVERY_FAILED_NOTICE,
	FormattingLadder,
	classifySendError,
	chunkWithFenceCarry,
	OneShotPendingStore,
	PLAIN_TEXT_FALLBACK_PREFIX,
	sendWithRetry,
	plainTextFallbackBody,
	resolveEnablement,
} from "../kit/index.js";
import type { ChunkPlan } from "../kit/chunking.js";
import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	MessageType,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type {
	AdapterStatusSnapshot,
	DisableReason,
} from "../kit/lifecycle-state.js";
import type { LengthUnit } from "../kit/length-policy.js";
import { buildSessionKey } from "../../pi_gateway/resolution/session-key.js";

import {
	SIGNAL_CAPABILITIES,
	SIGNAL_MAX_ATTACHMENT_SIZE,
	SIGNAL_MAX_ATTACHMENTS_PER_MSG,
	SIGNAL_MAX_MESSAGE_LENGTH,
	SIGNAL_PLUGIN_MANIFEST,
	SIGNAL_RATE_LIMIT_MAX_ATTEMPTS,
	SIGNAL_BATCH_PACING_NOTICE_THRESHOLD_S,
	SIGNAL_SUPPORTS_MESSAGE_EDITING,
	SSE_JITTER_FRACTION,
	SSE_RETRY_DELAY_INITIAL_MS,
	SSE_RETRY_DELAY_MAX_MS,
	HEALTH_CHECK_INTERVAL_MS,
	HEALTH_CHECK_STALE_THRESHOLD_MS,
	typingBackoffSeconds,
} from "./manifest.js";
import {
	extractRetryAfterSeconds,
	formatWait,
	isSignalRateLimitError,
	getScheduler,
	type SignalAttachmentScheduler,
	SignalRateLimitError,
	signalSendTimeout,
} from "./rate-limit.js";
import { extToMime, guessExtension } from "./media.js";
import { markdownToSignal } from "./signal-format.js";
import type { SignalCliTransport, SignalEventStream } from "./signal-wire.js";

/** The one command registry (07 §1 derivation — mirrors the reference set). */
export const SIGNAL_REGISTRY: CommandRegistry = [
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

export type ProcessingOutcome = "success" | "failure" | "cancelled";

export interface SignalAdapterOptions {
	transport: SignalCliTransport;
	/** The sending account (E.164 or service-id); required secret-resolved. */
	account?: string | undefined;
	/** Scoped reader over SIGNAL_* names (fail-closed; DEC-003/009). */
	secretReader?: ScopedSecretReader | undefined;
	scalarMaxUnits?: number | undefined;
	nowMs?: (() => number) | undefined;
	sleepMs?: ((ms: number) => Promise<void>) | undefined;
	rng?: (() => number) | undefined;
	spawner?: TaskSpawner | undefined;
	groupAllowFrom?: readonly string[] | undefined;
	dmAllowFrom?: readonly string[] | undefined;
	requireMention?: boolean | undefined;
	ignoreStories?: boolean | undefined;
	reactionsEnabled?: boolean | undefined;
	ignoreAttachments?: boolean | undefined;
	mediaCacheDir?: string | undefined;
	/**
	 * AAC→M4A remux seam (ffmpeg shell-out upstream). Default: pass-through
	 * (the documented no-ffmpeg path) — never spawns an OS child.
	 */
	remuxAac?: ((bytes: Buffer) => Promise<Uint8Array | null>) | undefined;
	scheduler?: SignalAttachmentScheduler | undefined;
	/**
	 * Lie-scan injection point: THE manifest datum that drives the
	 * streaming-exclusion probe. Production defaults to the frozen constant;
	 * only the lying-mutant fixture overrides it (to prove a lying capability
	 * claim FAILS the streaming family rows against seal reality).
	 */
	declaredMessageEditing?: boolean | undefined;
}

export interface EnvelopeCounters {
	envelopes: number;
	echoSuppressed: number;
	noteToSelfPromoted: number;
	noSender: number;
	selfMessage: number;
	storyFiltered: number;
	noDataMessage: number;
	groupDisabled: number;
	groupNotAllowed: number;
	mentionRequired: number;
	contentless: number;
	accepted: number;
}

const UUID_RE =
	/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

/** signal.py:_is_signal_service_id — PNI:/u: prefixes or a bare UUID. */
export function isSignalServiceId(value: string): boolean {
	if (!value) return false;
	if (value.startsWith("PNI:") || value.startsWith("u:")) return true;
	return UUID_RE.test(value);
}

/** signal.py:_looks_like_e164_number — +[7..15 digits]. */
export function looksLikeE164Number(value: string): boolean {
	return /^\+\d{7,15}$/.test(value);
}

/**
 * signal.py:_render_mentions — replace \uFFFC placeholders with readable
 * @identifiers, replacing end→start so indices never shift.
 */
export function renderMentions(
	text: string,
	mentions: Array<Record<string, unknown>>,
): string {
	if (mentions.length === 0 || !text.includes("\uFFFC")) return text;
	const sorted = [...mentions].sort(
		(a, b) => Number(b["start"] ?? 0) - Number(a["start"] ?? 0),
	);
	for (const mention of sorted) {
		const start = Number(mention["start"] ?? 0);
		const length = Number(mention["length"] ?? 1);
		const identifier =
			typeof mention["number"] === "string" && mention["number"]
				? mention["number"]
				: typeof mention["uuid"] === "string" && mention["uuid"]
					? mention["uuid"]
					: "user";
		text = text.slice(0, start) + `@${identifier}` + text.slice(start + length);
	}
	return text;
}

/**
 * The kit-built Signal adapter. Hermes anchors:
 * gateway/platforms/signal.py:SignalAdapter (all method-level anchors cited
 * inline below).
 */
export class SignalAdapter extends BasePlatformAdapter {
	readonly pluginManifest = SIGNAL_PLUGIN_MANIFEST;
	readonly transport: SignalCliTransport;
	readonly account: string;

	private readonly secretReader: ScopedSecretReader;
	private readonly nowFn: () => number;
	private readonly sleepFn: (ms: number) => Promise<void>;
	private readonly rngFn: () => number;
	private readonly ignoreStories: boolean;
	private readonly requireMention: boolean;
	private readonly reactionsEnabledFlag: boolean;
	private readonly ignoreAttachments: boolean;
	readonly mediaCacheDir: string;
	private readonly remuxAac:
		| ((bytes: Buffer) => Promise<Uint8Array | null>)
		| undefined;
	private readonly declaredMessageEditing: boolean;

	private readonly cp: EgressChokepoint;

	// ── allowlists (signal.py __init__ env parity, injected as data) ────────
	private readonly groupAllowSet: Set<string>;
	private readonly dmAllowSet: Set<string>;

	// ── listener state ───────────────────────────────────────────────────────
	private running = false;
	private lastSseActivityMs = 0;
	private sseStream: SignalEventStream | null = null;
	private sseTask: Promise<void> | null = null;
	private healthTask: Promise<void> | null = null;
	/** Reconnect ladder steps chosen so far (observability). */
	readonly reconnectLog: Array<{ delayMs: number; jittered: boolean }> = [];
	readonly forcedReconnects: Array<{ reason: string; atMs: number }> = [];

	// ── echo/self-sync caches (signal.py __init__ bounds) ───────────────────
	private readonly recentSentTimestamps = new Map<number, number>();
	private readonly maxRecentTimestamps = 512;
	private readonly recentSentTtlSeconds = 300;
	private readonly sentMessageTimestamps = new Map<string, null>();
	private readonly maxSentMessageTimestamps = 500;

	// ── recipient identifier cache ───────────────────────────────────────────
	private readonly recipientUuidByNumber = new Map<string, string>();
	private readonly recipientNumberByUuid = new Map<string, string>();
	private recipientResolveInFlight: Promise<string> | null = null;

	// ── typing breaker state (per chat) ──────────────────────────────────────
	private readonly typingFailures = new Map<string, number>();
	private readonly typingSkipUntil = new Map<string, number>();

	readonly scheduler: SignalAttachmentScheduler;
	readonly counts: EnvelopeCounters = {
		envelopes: 0,
		echoSuppressed: 0,
		noteToSelfPromoted: 0,
		noSender: 0,
		selfMessage: 0,
		storyFiltered: 0,
		noDataMessage: 0,
		groupDisabled: 0,
		groupNotAllowed: 0,
		mentionRequired: 0,
		contentless: 0,
		accepted: 0,
	};
	/** Every RPC attempt (observability; log_failures parity lives here). */
	readonly rpcLog: Array<{
		method: string;
		ok: boolean;
		error?: string | undefined;
	}> = [];
	readonly reactionLog: Array<{
		op: "send" | "remove";
		chatId: string;
		emoji: string;
		targetAuthor: string;
		targetTimestamp: number;
		ok: boolean;
	}> = [];

	// Interactive surfaces (kit-owned resolution seam — no native buttons).
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	// Subject/test observability lanes.
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly resolvedFamilies: string[] = [];
	private allowAllClickers = true;
	private readonly clarifyArmedSet = new Set<string>();
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};
	private holding = false;

	/** Interruptible-sleep registry: disconnect() flushes in-flight waits. */
	private readonly pendingSleepWakes = new Set<() => void>();

	constructor(opts: SignalAdapterOptions) {
		super({
			manifestName: SIGNAL_PLUGIN_MANIFEST.name,
			capabilities: SIGNAL_CAPABILITIES,
			scalarMaxUnits: opts.scalarMaxUnits ?? SIGNAL_MAX_MESSAGE_LENGTH,
		});
		this.transport = opts.transport;
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.account = (
			opts.account ??
			this.secretReader("SIGNAL_ACCOUNT") ??
			""
		).trim();
		this.nowFn = opts.nowMs ?? (() => Date.now());
		this.sleepFn =
			opts.sleepMs ?? ((ms) => new Promise((r) => setTimeout(r, ms)));
		this.rngFn = opts.rng ?? (() => Math.random());
		this.ignoreStories = opts.ignoreStories ?? true;
		this.requireMention = opts.requireMention ?? false;
		this.reactionsEnabledFlag = opts.reactionsEnabled ?? true;
		this.ignoreAttachments = opts.ignoreAttachments ?? false;
		this.mediaCacheDir =
			opts.mediaCacheDir ?? join(process.cwd(), "platforms", "signal", "media");
		this.remuxAac = opts.remuxAac;
		this.scheduler = opts.scheduler ?? getScheduler();
		this.declaredMessageEditing =
			opts.declaredMessageEditing ?? SIGNAL_SUPPORTS_MESSAGE_EDITING;
		this.groupAllowSet = new Set(opts.groupAllowFrom ?? []);
		this.dmAllowSet = new Set(opts.dmAllowFrom ?? ["*"]);

		// §11 step 3/4: missing required secret ⇒ LOUD disable (status-visible).
		const enablement = resolveEnablement(
			SIGNAL_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}
		if (opts.account === undefined && !this.secretReader("SIGNAL_ACCOUNT")) {
			const reason: DisableReason = {
				kind: "config_invalid",
				detail: "SIGNAL_ACCOUNT is required",
			};
			this.lifecycle.disable(reason);
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // no native draft lanes (no edits)
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			transmitEdit: async (chatId, messageId, content) =>
				this.wireEdit(chatId, messageId, content, { finalize: false }),
			transmitSeal: async (_k, _chatId, _draftId, _content, _metadata) => ({
				success: false,
				error: "Not supported",
			}),
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
			}),
		});
	}

	// ── capabilities: THE probe-computed streaming exclusion ────────────────

	/**
	 * Native draft streaming is excluded BY THE PROBE from the manifest datum
	 * SUPPORTS_MESSAGE_EDITING=False (signal.py class attr @~258: "Signal has
	 * no real edit API for already-sent messages") — a draft cursor could
	 * never be sealed or edited away, so advertising streaming would be the
	 * silent-capability-lie the mutation suite hunts. Flip the datum and this
	 * flips — the lie-scan mutant proves the flip FAILS the streaming rows.
	 */
	override supportsDraftStreaming(_chatType?: string): boolean {
		return this.declaredMessageEditing;
	}

	// ── lifecycle snapshot ───────────────────────────────────────────────────

	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.lifecycle.statusSnapshot();
	}

	// ── connect/disconnect (signal.py connect/disconnect parity) ────────────

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		if (!this.account) {
			this.logger?.error?.(
				"Signal: SIGNAL_HTTP_URL and SIGNAL_ACCOUNT are required",
			);
			return false;
		}
		// Health check — verify the daemon is reachable BEFORE starting listeners.
		let healthy: boolean;
		try {
			healthy = await this.transport.checkHealth();
		} catch {
			healthy = false;
		}
		if (!healthy) {
			this.logger?.error?.(`Signal: cannot reach signal-cli daemon`);
			return false;
		}
		if (!this.running) {
			this.running = true;
			this.lastSseActivityMs = this.nowFn();
			this.sseTask = this.sseListener();
			this.healthTask = this.healthMonitor();
		}
		return true;
	}

	override async disconnect(): Promise<void> {
		this.running = false;
		this.sseStream?.close();
		this.sseStream = null;
		// Flush any in-flight ladder/monitor waits so both loops observe the
		// stop flag promptly instead of hanging on an un-advanced clock.
		for (const wake of [...this.pendingSleepWakes]) wake();
		this.pendingSleepWakes.clear();
		for (const task of [this.sseTask, this.healthTask]) {
			await task?.catch(() => undefined);
		}
		this.sseTask = null;
		this.healthTask = null;
	}

	get isRunning(): boolean {
		return this.running;
	}

	/** Injected-clock sleep that disconnect() can interrupt. */
	private interruptibleSleep(ms: number): Promise<void> {
		return new Promise<void>((resolve) => {
			let settled = false;
			const settle = () => {
				if (settled) return;
				settled = true;
				this.pendingSleepWakes.delete(settle);
				resolve();
			};
			this.pendingSleepWakes.add(settle);
			void this.sleepFn(ms).then(() => settle());
		});
	}

	// ── SSE listener (signal.py:_sse_listener parity) ────────────────────────

	private async sseListener(): Promise<void> {
		let backoffMs = SSE_RETRY_DELAY_INITIAL_MS;
		while (this.running) {
			try {
				const stream = await this.transport.openEventStream();
				this.sseStream = stream;
				backoffMs = SSE_RETRY_DELAY_INITIAL_MS; // reset on successful connect
				this.lastSseActivityMs = this.nowFn();

				let buffer = "";
				for await (const chunk of stream.chunks) {
					if (!this.running) break;
					buffer += chunk;
					for (;;) {
						const nl = buffer.indexOf("\n");
						if (nl === -1) break;
						const line = buffer.slice(0, nl).trim();
						buffer = buffer.slice(nl + 1);
						if (!line) continue;
						// Keepalive comments prove liveness — refresh activity so the
						// health monitor never reports false idle.
						if (line.startsWith(":")) {
							this.lastSseActivityMs = this.nowFn();
							continue;
						}
						if (line.startsWith("data:")) {
							const dataStr = line.slice(5).trim();
							if (!dataStr) continue;
							this.lastSseActivityMs = this.nowFn();
							try {
								const data: unknown = JSON.parse(dataStr);
								if (data !== null && typeof data === "object") {
									await this.handleEnvelope(data as Record<string, unknown>);
								}
							} catch {
								// Contained per event: invalid JSON or handler errors
								// must never kill the stream (_sse_listener containment).
							}
						}
					}
				}
				this.sseStream = null;
			} catch {
				// openEventStream failure — fall through to the ladder sleep.
			}
			if (this.running) {
				const jitter = backoffMs * SSE_JITTER_FRACTION * this.rngFn();
				const delayMs = backoffMs + jitter;
				this.reconnectLog.push({
					delayMs,
					jittered: jitter > 0,
				});
				await this.interruptibleSleep(delayMs);
				backoffMs = Math.min(backoffMs * 2, SSE_RETRY_DELAY_MAX_MS);
			}
		}
	}

	// ── health monitor (signal.py:_health_monitor parity) ───────────────────

	private async healthMonitor(): Promise<void> {
		while (this.running) {
			await this.interruptibleSleep(HEALTH_CHECK_INTERVAL_MS);
			if (!this.running) break;
			const elapsed = this.nowFn() - this.lastSseActivityMs;
			if (elapsed <= HEALTH_CHECK_STALE_THRESHOLD_MS) continue;
			let healthy: boolean;
			try {
				healthy = await this.transport.checkHealth();
			} catch {
				healthy = false;
			}
			if (healthy) {
				// Daemon alive, stream merely quiet — refresh activity.
				this.lastSseActivityMs = this.nowFn();
			} else {
				this.forcedReconnects.push({
					reason: "stale-sse-and-daemon-unreachable",
					atMs: this.nowFn(),
				});
				this.forceReconnect();
			}
		}
	}

	/** Force SSE reconnection by tearing the live stream. */
	private forceReconnect(): void {
		this.sseStream?.close();
		this.sseStream = null;
	}

	// ── envelope walk (signal.py:_handle_envelope parity) ───────────────────

	async handleEnvelope(envelope: Record<string, unknown>): Promise<void> {
		this.counts.envelopes += 1;
		const data = this.unwrap(envelope);

		// syncMessage: promote genuine Note-to-Self, swallow own-send echoes.
		let noteToSelf = false;
		const syncRaw = data["syncMessage"];
		if (syncRaw !== null && typeof syncRaw === "object") {
			const sync = syncRaw as Record<string, unknown>;
			const sentRaw = sync["sentMessage"];
			if (sentRaw !== null && typeof sentRaw === "object") {
				const sent = sentRaw as Record<string, unknown>;
				const dest = str(sent["destinationNumber"]) || str(sent["destination"]);
				const groupInfoRaw = sent["groupInfo"];
				const sentGroupId =
					groupInfoRaw !== null && typeof groupInfoRaw === "object"
						? str((groupInfoRaw as Record<string, unknown>)["groupId"])
						: "";
				if (dest === this.account || sentGroupId) {
					if (this.consumeSentTimestamp(sent["timestamp"])) {
						this.counts.echoSuppressed += 1;
						return; // our own outbound echo — never a turn
					}
					noteToSelf = true;
					this.counts.noteToSelfPromoted += 1;
					data["dataMessage"] = sent;
				}
			}
			if (!noteToSelf) return; // read receipts / typing sync events filtered
		}

		const sender =
			str(data["sourceNumber"]) ||
			str(data["sourceUuid"]) ||
			str(data["source"]);
		const senderName = str(data["sourceName"]);
		const senderUuid = str(data["sourceUuid"]);
		this.rememberRecipientIdentifiers(sender, senderUuid);

		if (!sender) {
			this.counts.noSender += 1;
			return;
		}
		if (this.account && sender === this.account && !noteToSelf) {
			this.counts.selfMessage += 1;
			return;
		}
		if (this.ignoreStories && data["storyMessage"]) {
			this.counts.storyFiltered += 1;
			return;
		}

		// dataMessage, else the editMessage wrapper's inner dataMessage
		// (asRec({}) is a non-nullish {} so the fallthrough must be EXPLICIT).
		let dmRaw: unknown = data["dataMessage"];
		if (dmRaw === undefined || dmRaw === null) {
			dmRaw = asRec(data["editMessage"])["dataMessage"];
		}
		const dm = asRec(dmRaw);
		if (Object.keys(dm).length === 0) {
			this.counts.noDataMessage += 1;
			return;
		}

		const groupInfo = asRec(dm["groupInfo"]);
		const groupId = str(groupInfo["groupId"]) || undefined;
		const isGroup = groupId !== undefined;

		if (isGroup) {
			if (this.groupAllowSet.size === 0) {
				this.counts.groupDisabled += 1;
				return;
			}
			if (!this.groupAllowSet.has("*") && !this.groupAllowSet.has(groupId)) {
				this.counts.groupNotAllowed += 1;
				return;
			}
		}

		const chatId = isGroup ? `group:${groupId}` : sender;

		let text = str2(dm["message"]);
		const mentions = arrayRec(dm["mentions"]);
		if (text && mentions.length > 0) text = renderMentions(text, mentions);

		if (isGroup && this.requireMention) {
			const mentionedInText = this.account && text.includes(`@${this.account}`);
			const mentionedInMetadata = mentions.some(
				(m) => m["number"] === this.account || m["uuid"] === this.account,
			);
			if (!mentionedInText && !mentionedInMetadata) {
				this.counts.mentionRequired += 1;
				return;
			}
		}

		// Strip the bot's own @mention wherever it appears (every group).
		if (isGroup && text && this.account) {
			text = text.replaceAll(`@${this.account}`, "");
			const botUuid = this.recipientUuidByNumber.get(this.account);
			if (botUuid) text = text.replaceAll(`@${botUuid}`, "");
			text = text.split("  ").join(" ").trim();
		}

		// Quote context (reply-to).
		const quote = asRec(dm["quote"]);
		const quoteId = quote["id"];
		const replyToId =
			quoteId === undefined || quoteId === null ? undefined : String(quoteId);
		const replyToText = str2(quote["text"]);
		const replyToAuthor = extractQuoteAuthor(quote);
		const replyToAuthorName =
			str2(quote["authorName"]) || str2(quote["authorProfileName"]);
		const replyToIsOwn = this.quoteReferencesOwnMessage(
			replyToId,
			replyToAuthor,
		);

		// Attachments.
		const mediaUrls: string[] = [];
		const mediaTypes: string[] = [];
		if (!this.ignoreAttachments) {
			for (const att of asArray(dm["attachments"])) {
				const attId = str(att["id"]);
				const attSize = Number(att["size"] ?? 0);
				if (!attId) continue;
				if (attSize > SIGNAL_MAX_ATTACHMENT_SIZE) continue;
				const fetched = await this.fetchAttachment(attId);
				if (fetched !== null) {
					mediaUrls.push(fetched.path);
					mediaTypes.push(str(att["contentType"]) || fetched.mime);
				}
			}
		}

		// Contentless envelopes (profile key updates, empty messages) skipped.
		if ((!text || !text.trim()) && mediaUrls.length === 0) {
			this.counts.contentless += 1;
			return;
		}

		let messageType: MessageType = "text";
		if (mediaTypes.length > 0) {
			if (mediaTypes.some((mt) => mt.startsWith("audio/")))
				messageType = "voice";
			else if (mediaTypes.some((mt) => mt.startsWith("image/")))
				messageType = "photo";
			else if (mediaTypes.some((mt) => mt.startsWith("video/")))
				messageType = "video";
			else messageType = "document";
		}

		const source = {
			platform: "signal",
			chatType: isGroup ? ("group" as const) : ("dm" as const),
			userId: sender,
			chatId,
			...(isGroup ? {} : { ...(senderName ? { chatName: senderName } : {}) }),
			...(senderUuid ? { userIdAlt: senderUuid } : {}),
			...(isGroup ? { chatIdAlt: groupId } : {}),
			...(isGroup && str(groupInfo["groupName"])
				? { chatName: str(groupInfo["groupName"]) }
				: {}),
		};

		this.counts.accepted += 1;
		const sessionKey = buildSessionKey(
			{
				platform: source.platform,
				chatType: source.chatType,
				...(source.userId ? { userId: source.userId } : {}),
				...(source.chatId ? { chatId: source.chatId } : {}),
				...(source.chatName !== undefined ? { chatName: source.chatName } : {}),
			},
			{},
			undefined,
		);

		const event: IncomingEvent = {
			text,
			messageType,
			...(replyToId !== undefined ? { replyToMessageId: replyToId } : {}),
			...(mediaUrls.length > 0 ? { mediaUrls } : {}),
			...(mediaTypes.length > 0 ? { mediaTypes } : {}),
			metadata: {
				gateway_session_key: sessionKey,
				signal_reaction_target: {
					author: sender,
					timestamp: data["timestamp"],
				},
				...(replyToText ? { reply_to_text: replyToText } : {}),
				...(replyToAuthor ? { reply_to_author_id: replyToAuthor } : {}),
				...(replyToAuthorName
					? { reply_to_author_name: replyToAuthorName }
					: {}),
				...(replyToId !== undefined
					? { reply_to_is_own_message: replyToIsOwn }
					: {}),
			},
			source,
		};
		await this.handleIngress(event, sessionKey);
	}

	/** Unwrap the nested envelope key when present (_handle_envelope step 1). */
	private unwrap(envelope: Record<string, unknown>): Record<string, unknown> {
		const inner = envelope["envelope"];
		return inner !== null && typeof inner === "object" && !Array.isArray(inner)
			? (inner as Record<string, unknown>)
			: envelope;
	}

	private rememberRecipientIdentifiers(
		number_: string | undefined,
		serviceId: string | undefined,
	): void {
		if (!number_ || !serviceId || !isSignalServiceId(serviceId)) return;
		this.recipientUuidByNumber.set(number_, serviceId);
		this.recipientNumberByUuid.set(serviceId, number_);
	}

	/** Test/engine seam: seed an observed number↔UUID mapping. */
	noteRecipient(number_: string, serviceId: string): void {
		this.rememberRecipientIdentifiers(number_, serviceId);
	}

	/**
	 * True when a quote points at THIS adapter's outbound message
	 * (_quote_references_own_message parity): quoted timestamp in our sent
	 * cache, author == account, or author == account's known UUID/number.
	 */
	private quoteReferencesOwnMessage(
		replyToId: string | undefined,
		replyToAuthor: string | undefined,
	): boolean {
		if (replyToId && this.sentMessageTimestamps.has(replyToId)) return true;
		if (!replyToAuthor) return false;
		const author = replyToAuthor.trim();
		if (this.account && author === this.account) return true;
		const cachedUuid = this.account
			? this.recipientUuidByNumber.get(this.account)
			: undefined;
		if (cachedUuid && author === cachedUuid) return true;
		const cachedNumber = this.recipientNumberByUuid.get(author);
		return cachedNumber !== undefined && cachedNumber === this.account;
	}

	// ── attachment fetch (signal.py:_fetch_attachment parity) ───────────────

	private async fetchAttachment(
		attachmentId: string,
	): Promise<{ path: string; mime: string } | null> {
		const outcome = await this.rpc("getAttachment", {
			account: this.account,
			id: attachmentId,
		});
		if (!outcome.ok) return null;
		let b64: string | undefined;
		if (
			outcome.result !== null &&
			typeof outcome.result === "object" &&
			!Array.isArray(outcome.result)
		) {
			b64 = str((outcome.result as Record<string, unknown>)["data"]);
			if (!b64) return null;
		} else if (typeof outcome.result === "string") {
			b64 = outcome.result;
		}
		if (!b64) return null;
		let bytes = Buffer.from(b64, "base64");
		let ext = guessExtension(bytes);
		if (ext === ".aac" && this.remuxAac) {
			const remuxed = await this.remuxAac(bytes);
			if (remuxed !== null) {
				bytes = Buffer.from(remuxed); // copy into an owned ArrayBuffer
				ext = ".m4a";
			}
		}
		mkdirSync(this.mediaCacheDir, { recursive: true });
		const outPath = join(this.mediaCacheDir, `${attachmentId}${ext}`);
		writeFileSync(outPath, bytes);
		return { path: outPath, mime: extToMime(ext) };
	}

	// ── JSON-RPC (signal.py:_rpc parity) ─────────────────────────────────────

	async rpc(
		method: string,
		params: Record<string, unknown>,
		opts: {
			id?: string | undefined;
			logFailures?: boolean | undefined;
			raiseOnRateLimit?: boolean | undefined;
			metadata?: Metadata | undefined;
		} = {},
	): Promise<{ ok: boolean; result?: unknown; error?: string }> {
		const rpcId = opts.id ?? `${method}_${this.nowFn()}`;
		let outcome;
		try {
			outcome = await this.transport.rpc(method, params, {
				id: rpcId,
				...(opts.metadata !== undefined ? { metadata: opts.metadata } : {}),
			});
		} catch (err) {
			this.logRpcFailure(method, opts, errText(err));
			return { ok: false, error: errText(err) };
		}
		if (!outcome.ok) {
			if (opts.raiseOnRateLimit && isSignalRateLimitError(outcome.error)) {
				throw new SignalRateLimitError(
					outcome.error.message,
					extractRetryAfterSeconds(outcome.error),
				);
			}
			this.logRpcFailure(method, opts, JSON.stringify(outcome.error));
			return { ok: false, error: JSON.stringify(outcome.error) };
		}
		const result = outcome.result;
		// Batch-result RATE_LIMIT_FAILURE surfacing (raise-on-rate-limit leg 2).
		if (
			opts.raiseOnRateLimit &&
			result !== null &&
			typeof result === "object"
		) {
			const results = (result as Record<string, unknown>)["results"];
			if (Array.isArray(results)) {
				for (const r of results) {
					const rec = asRec(r);
					if (rec["type"] === "RATE_LIMIT_FAILURE") {
						const retryAfterRaw = rec["retryAfterSeconds"];
						const retryAfter =
							retryAfterRaw === undefined || retryAfterRaw === null
								? null
								: Number(retryAfterRaw);
						throw new SignalRateLimitError(
							"Rate limit exceeded for recipient",
							Number.isFinite(retryAfter) ? retryAfter : null,
						);
					}
				}
			}
		}
		return { ok: true, result };
	}

	private logRpcFailure(
		method: string,
		opts: { logFailures?: boolean | undefined },
		error: string,
	): void {
		void opts.logFailures; // warning-vs-debug split upstream; captured here
		this.rpcLog.push({ method, ok: false, error });
	}

	// ── recipient resolution (signal.py:_resolve_recipient parity) ──────────

	/**
	 * Preferred Signal recipient identifier for a DM: groups/service-ids/
	 * non-E.164 pass through; phone numbers upgrade to the cached (or
	 * listContacts-discovered) UUID.
	 */
	async resolveRecipient(chatId: string): Promise<string> {
		if (
			!chatId ||
			chatId.startsWith("group:") ||
			isSignalServiceId(chatId) ||
			!looksLikeE164Number(chatId)
		) {
			return chatId;
		}
		const cached = this.recipientUuidByNumber.get(chatId);
		if (cached) return cached;

		if (this.recipientResolveInFlight !== null) {
			await this.recipientResolveInFlight;
			return this.recipientUuidByNumber.get(chatId) ?? chatId;
		}
		this.recipientResolveInFlight = (async () => {
			const contacts = await this.rpc("listContacts", {
				account: this.account,
				allRecipients: true,
			});
			if (contacts.ok && Array.isArray(contacts.result)) {
				for (const c of contacts.result) {
					const rec = asRec(c);
					const number_ = str(rec["number"]);
					const serviceId = this.extractContactUuid(rec, chatId);
					if (number_ && serviceId) {
						this.rememberRecipientIdentifiers(number_, serviceId);
					}
				}
			}
			return chatId;
		})();
		try {
			await this.recipientResolveInFlight;
		} finally {
			this.recipientResolveInFlight = null;
		}
		return this.recipientUuidByNumber.get(chatId) ?? chatId;
	}

	/** Best-effort service-id extraction from one listContacts row. */
	private extractContactUuid(
		contact: Record<string, unknown>,
		phoneNumber: string,
	): string | undefined {
		const number_ = str(contact["number"]);
		const recipient = str(contact["recipient"]);
		const profile = asRec(contact["profile"]);
		const serviceId =
			str(contact["uuid"]) ||
			str(contact["serviceId"]) ||
			str(profile["serviceId"]) ||
			str(profile["uuid"]);
		if (serviceId && isSignalServiceId(serviceId)) {
			if (number_ === phoneNumber || recipient === phoneNumber)
				return serviceId;
		}
		return undefined;
	}

	// ── egress doors ─────────────────────────────────────────────────────────

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * DOOR transport — text egress. Converts markdown → plain + bodyRanges
	 * (signal.py:send parity), addresses groups by stripped id vs recipients
	 * by resolved identifier, validates the results[] block, and tracks the
	 * outbound timestamp for echo filtering. messageId stays NULL on success:
	 * Signal has no editable message identifier, and pretending otherwise
	 * would put the stream consumer on an edit path that cannot exist.
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		_metadata: Metadata,
	): Promise<SendResult> {
		await this.stopTypingIndicator(chatId);
		// The §6.1 plain-text fallback lane carries ORIGINAL chunk bytes by
		// contract — dialect conversion is SKIPPED for that envelope (its prefix
		// marks it); everything else converts to plain + bodyRanges first.
		const [plainText, textStyles] = content.startsWith(
			PLAIN_TEXT_FALLBACK_PREFIX,
		)
			? [content, [] as string[]]
			: markdownToSignal(content);
		const params: Record<string, unknown> = {
			account: this.account,
			message: plainText,
		};
		if (textStyles.length === 1) params["textStyle"] = textStyles[0];
		else if (textStyles.length > 1) params["textStyles"] = textStyles;

		params[this.addressKey(chatId)] = await this.addressValue(chatId);
		const result = await this.rpc("send", params, { metadata: _metadata });
		if (!result.ok) {
			return { success: false, error: result.error ?? "RPC send failed" };
		}
		const verdict = validateSendResult(result.result);
		if (!verdict.success) {
			return { success: false, error: verdict.error, retryable: false };
		}
		this.trackSentTimestamp(result.result);
		return { success: true, messageId: null };
	}

	/**
	 * Rich lane ABSENT on the real signal-cli RPC surface: unless the harness
	 * explicitly scripted a rich probe, answer the capability-error shape
	 * WITHOUT burning a wire roundtrip — the §10.1 latch path probes once then
	 * never again (webhook reference adapter parity).
	 */
	protected override async wireRich(
		content: string,
		_metadata: Metadata,
	): Promise<SendResult> {
		if (!(this.transport.hasRichScript?.() ?? false)) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		const ok = await this.transport.transmitRichProbe!("__rich__", content);
		return ok ? { success: true } : { success: false, error: "rich failed" };
	}

	/** "groupId" (stripped prefix) for groups, else "recipient" list value. */
	private addressKey(chatId: string): string {
		return chatId.startsWith("group:") ? "groupId" : "recipient";
	}

	private async addressValue(chatId: string): Promise<unknown> {
		if (chatId.startsWith("group:")) return chatId.slice(6);
		return [await this.resolveRecipient(chatId)];
	}

	// ── echo tracking (signal.py:_track_sent_timestamp parity) ─────────────

	private trackSentTimestamp(rpcResult: unknown): void {
		if (rpcResult === null || typeof rpcResult !== "object") return;
		const tsRaw = (rpcResult as Record<string, unknown>)["timestamp"];
		if (tsRaw === undefined || tsRaw === null || tsRaw === 0) return;
		const ts = Number(tsRaw);
		this.sentMessageTimestamps.delete(String(ts));
		this.sentMessageTimestamps.set(String(ts), null);
		while (this.sentMessageTimestamps.size > this.maxSentMessageTimestamps) {
			const oldest = this.sentMessageTimestamps.keys().next().value;
			if (oldest === undefined) break;
			this.sentMessageTimestamps.delete(oldest);
		}
		// Recent-echo LRU with TTL.
		const now = this.nowFn();
		this.recentSentTimestamps.delete(ts);
		this.recentSentTimestamps.set(ts, now);
		const cutoff = now - this.recentSentTtlSeconds * 1000;
		for (;;) {
			const oldestEntry = this.recentSentTimestamps.entries().next();
			const oldestPair = oldestEntry.value;
			if (oldestEntry.done || oldestPair === undefined) break;
			if (oldestPair[1] < cutoff)
				this.recentSentTimestamps.delete(oldestPair[0]);
			else break;
		}
		while (this.recentSentTimestamps.size > this.maxRecentTimestamps) {
			const oldest = this.recentSentTimestamps.keys().next().value;
			if (oldest === undefined) break;
			this.recentSentTimestamps.delete(oldest);
		}
	}

	/** Pop a timestamp if it matches one we sent; true ⇒ echo. */
	private consumeSentTimestamp(tsRaw: unknown): boolean {
		if (tsRaw === undefined || tsRaw === null) return false;
		const ts = Number(tsRaw);
		if (this.recentSentTimestamps.has(ts)) {
			this.recentSentTimestamps.delete(ts);
			return true;
		}
		return false;
	}

	// ── typing (signal.py:sendTyping/_stop_typing_indicator parity) ─────────

	/**
	 * Typing indicator with the consecutive-failure BREAKER: after ≥3
	 * consecutive failures the RPC is SKIPPED entirely during an exponential
	 * skip window (min(60, 16·2^(fails−3)) seconds); success clears state.
	 */
	async sendTypingSignal(chatId: string): Promise<boolean> {
		const now = this.nowFn();
		const skipUntil = this.typingSkipUntil.get(chatId) ?? 0;
		if (now < skipUntil) return false;

		const params: Record<string, unknown> = { account: this.account };
		params[this.addressKey(chatId)] = await this.addressValue(chatId);

		const fails = this.typingFailures.get(chatId) ?? 0;
		const result = await this.rpc("sendTyping", params, { id: "typing" });
		if (!result.ok) {
			const nextFails = fails + 1;
			this.typingFailures.set(chatId, nextFails);
			if (nextFails >= 3) {
				const backoffS = typingBackoffSeconds(nextFails);
				this.typingSkipUntil.set(chatId, now + backoffS * 1000);
			}
			return false;
		}
		this.typingFailures.delete(chatId);
		this.typingSkipUntil.delete(chatId);
		return true;
	}

	/**
	 * Stop-typing: cancel any loop AND send the explicit stop:true RPC so the
	 * recipient's device drops the indicator immediately; best-effort — the
	 * breaker state clears regardless (_stop_typing_indicator parity).
	 */
	async stopTypingIndicator(chatId: string): Promise<void> {
		try {
			const params: Record<string, unknown> = {
				account: this.account,
				stop: true,
			};
			params[this.addressKey(chatId)] = await this.addressValue(chatId);
			await this.rpc("sendTyping", params, {
				id: "typing-stop",
				logFailures: false,
			});
		} catch {
			/* best-effort: cleanup continues */
		}
		this.typingFailures.delete(chatId);
		this.typingSkipUntil.delete(chatId);
	}

	// ── reactions (signal.py:send_reaction/remove_reaction parity) ──────────

	async sendReaction(
		chatId: string,
		emoji: string,
		targetAuthor: string,
		targetTimestamp: number,
	): Promise<boolean> {
		const params: Record<string, unknown> = {
			account: this.account,
			emoji,
			targetAuthor,
			targetTimestamp,
		};
		params[this.addressKey(chatId)] = chatId.startsWith("group:")
			? chatId.slice(6)
			: chatId;
		const result = await this.rpc("sendReaction", params);
		this.reactionLog.push({
			op: "send",
			chatId,
			emoji,
			targetAuthor,
			targetTimestamp,
			ok: result.ok,
		});
		return result.ok;
	}

	async removeReaction(
		chatId: string,
		targetAuthor: string,
		targetTimestamp: number,
	): Promise<boolean> {
		const params: Record<string, unknown> = {
			account: this.account,
			emoji: "",
			targetAuthor,
			targetTimestamp,
			remove: true,
		};
		params[this.addressKey(chatId)] = chatId.startsWith("group:")
			? chatId.slice(6)
			: chatId;
		const result = await this.rpc("sendReaction", params);
		this.reactionLog.push({
			op: "remove",
			chatId,
			emoji: "",
			targetAuthor,
			targetTimestamp,
			ok: result.ok,
		});
		return result.ok;
	}

	// ── processing lifecycle hooks (👀 → ✅/❌ progress reactions) ───────────

	/**
	 * Reaction gates (_reactions_enabled parity): global off-switch plus the
	 * DM allowlist (unauthorized contacts must not see 👀 fire before authz).
	 */
	reactionsEnabled(event?: IncomingEvent): boolean {
		if (!this.reactionsEnabledFlag) return false;
		if (event !== undefined) {
			const sender = String(event.source?.userId ?? "");
			if (sender && !this.dmAllowSet.has("*") && !this.dmAllowSet.has(sender)) {
				return false;
			}
		}
		return true;
	}

	private reactionTargetOf(
		event: IncomingEvent,
	): { author: string; timestamp: number } | null {
		const raw = asRec(event.metadata?.["signal_reaction_target"]);
		const author = str(raw["author"]);
		const ts = Number(raw["timestamp"] ?? 0);
		if (!author || !ts) return null;
		return { author, timestamp: ts };
	}

	/** React 👀 when processing begins. */
	async onProcessingStart(event: IncomingEvent): Promise<void> {
		if (!this.reactionsEnabled(event)) return;
		const target = this.reactionTargetOf(event);
		if (target && event.source?.chatId) {
			await this.sendReaction(
				event.source.chatId,
				"👀",
				target.author,
				target.timestamp,
			);
		}
	}

	/**
	 * Swap 👀 for ✅ (success) / ❌ (failure). CANCELLED leaves 👀 in place —
	 * no terminal outcome means still in progress (Telegram-matched parity).
	 */
	async onProcessingComplete(
		event: IncomingEvent,
		outcome: ProcessingOutcome,
	): Promise<void> {
		if (!this.reactionsEnabled(event)) return;
		if (outcome === "cancelled") return;
		const target = this.reactionTargetOf(event);
		const chatId = event.source?.chatId;
		if (!target || !chatId) return;
		await this.removeReaction(chatId, target.author, target.timestamp);
		if (outcome === "success")
			await this.sendReaction(chatId, "✅", target.author, target.timestamp);
		else if (outcome === "failure")
			await this.sendReaction(chatId, "❌", target.author, target.timestamp);
	}

	// ── chat info (signal.py:get_chat_info parity) ───────────────────────────

	async getChatInfo(
		chatId: string,
	): Promise<{ name: string; type: string; chatId: string }> {
		if (chatId.startsWith("group:")) {
			return { name: chatId, type: "group", chatId };
		}
		const result = await this.rpc("getContact", {
			account: this.account,
			contactAddress: chatId,
		});
		let name = chatId;
		if (
			result.ok &&
			result.result !== null &&
			typeof result.result === "object"
		) {
			const rec = result.result as Record<string, unknown>;
			name = str(rec["name"]) || str(rec["profileName"]) || chatId;
		}
		return { name, type: "dm", chatId };
	}

	// ── batch attachment sends (signal.py:send_multiple_images parity) ──────

	/**
	 * Chunked attachment batches paced through the rate-limit scheduler:
	 * acquire(n) before every RPC attempt, reportRpcDuration(n) after success,
	 * SignalRateLimitError ⇒ feedback(retry_after, n) then retry (max
	 * SIGNAL_RATE_LIMIT_MAX_ATTEMPTS), transient failures backoff 2^attempt s
	 * once. Estimated waits past the notice threshold emit a pacing notice
	 * through the normal send lane.
	 */
	async batchSendAttachments(
		chatId: string,
		attachmentPaths: readonly string[],
		opts: { notify?: ((text: string) => Promise<void>) | undefined } = {},
	): Promise<Array<{ batchIndex: number; success: boolean; error?: string }>> {
		if (attachmentPaths.length === 0) return [];
		await this.stopTypingIndicator(chatId);

		const baseParams: Record<string, unknown> = {
			account: this.account,
			message: "",
		};
		baseParams[this.addressKey(chatId)] = await this.addressValue(chatId);

		const batches: string[][] = [];
		for (
			let i = 0;
			i < attachmentPaths.length;
			i += SIGNAL_MAX_ATTACHMENTS_PER_MSG
		) {
			batches.push([
				...attachmentPaths.slice(i, i + SIGNAL_MAX_ATTACHMENTS_PER_MSG),
			]);
		}

		const outcomes: Array<{
			batchIndex: number;
			success: boolean;
			error?: string;
		}> = [];
		for (let idx = 0; idx < batches.length; idx++) {
			const batch = batches[idx] ?? [];
			const n = batch.length;
			const estimated = this.scheduler.estimateWait(n);
			if (estimated >= SIGNAL_BATCH_PACING_NOTICE_THRESHOLD_S) {
				await (
					opts.notify ??
					(async (text: string) => {
						await this.send(chatId, text);
					})
				)(
					`(More images coming — pausing ~${formatWait(estimated)} ` +
						`for Signal rate limit, batch ${idx + 1}/${batches.length}.)`,
				);
			}

			const params = { ...baseParams, attachments: batch };
			const timeout = signalSendTimeout(n);

			let settled = false;
			let lastError: string | undefined;
			for (
				let attempt = 1;
				attempt <= SIGNAL_RATE_LIMIT_MAX_ATTEMPTS;
				attempt++
			) {
				await this.scheduler.acquire(n);
				try {
					const t0 = this.nowFn();
					const result = await this.rpc("send", params, {
						raiseOnRateLimit: true,
					});
					const durationS = (this.nowFn() - t0) / 1000;
					void timeout; // production transports apply the scaled HTTP timeout
					if (result.ok) {
						const verdict = validateSendResult(result.result);
						if (verdict.success) {
							this.trackSentTimestamp(result.result);
							await this.scheduler.reportRpcDuration(durationS, n);
							outcomes.push({ batchIndex: idx, success: true });
							settled = true;
						} else {
							lastError = verdict.error;
							if (attempt < SIGNAL_RATE_LIMIT_MAX_ATTEMPTS) {
								await this.sleepFn(2000 * attempt); // 2^attempt seconds
								continue;
							}
						}
					} else {
						lastError = result.error ?? "RPC send failed";
						if (attempt < SIGNAL_RATE_LIMIT_MAX_ATTEMPTS) {
							await this.sleepFn(2000 * attempt);
							continue;
						}
					}
				} catch (e) {
					if (e instanceof SignalRateLimitError) {
						this.scheduler.feedback(e.retryAfter, n);
						if (attempt >= SIGNAL_RATE_LIMIT_MAX_ATTEMPTS) {
							lastError = `rate-limit retries exhausted (${e.message})`;
						}
						continue;
					}
					throw e;
				}
				break;
			}
			if (!settled) {
				outcomes.push({
					batchIndex: idx,
					success: false,
					...(lastError !== undefined ? { error: lastError } : {}),
				});
			}
		}
		return outcomes;
	}

	// ── single-attachment send (signal.py:_send_attachment parity) ──────────

	/**
	 * ONE file as ONE RPC send whose message body carries the caption
	 * (_send_attachment @~1502): stop-typing first, size-capped against
	 * SIGNAL_MAX_ATTACHMENT_SIZE, addressed through the SAME account +
	 * recipient|groupId resolution as text sends, validated through the same
	 * results[] walk, and its timestamp feeds the Note-to-Self echo filter.
	 */
	async sendAttachment(
		chatId: string,
		filePath: string,
		opts: {
			caption?: string | undefined;
			mediaLabel?: string | undefined;
		} = {},
	): Promise<SendResult> {
		await this.stopTypingIndicator(chatId);
		const label = opts.mediaLabel ?? "File";
		let fileSize: number;
		try {
			fileSize = statSync(filePath).size;
		} catch {
			return { success: false, error: `${label} file not found: ${filePath}` };
		}
		if (fileSize > SIGNAL_MAX_ATTACHMENT_SIZE) {
			return {
				success: false,
				error: `${label} too large (${fileSize} bytes)`,
			};
		}

		const params: Record<string, unknown> = {
			account: this.account,
			message: opts.caption ?? "",
			attachments: [filePath],
		};
		params[this.addressKey(chatId)] = await this.addressValue(chatId);

		const result = await this.rpc("send", params);
		if (!result.ok) {
			return {
				success: false,
				error: result.error ?? `RPC send ${label.toLowerCase()} failed`,
			};
		}
		const verdict = validateSendResult(result.result);
		if (!verdict.success) {
			return { success: false, error: verdict.error, retryable: false };
		}
		this.trackSentTimestamp(result.result);
		return { success: true, messageId: null };
	}

	// ── post-stream media lanes (DEC-019 explicit-tag delivery surface) ─────

	/**
	 * run.py:_deliver_media_from_response surface. WITHOUT these bindings the
	 * post-stream rescan pass optional-chains every MEDIA-tagged file into a
	 * silent no-op. Vendor mapping (signal.py @~1330/1560-1600): image batches
	 * ride the scheduler-paced batch lane; voice/video/document each ride the
	 * single-attachment send.
	 */
	async sendMultipleImages(
		chatId: string,
		images: readonly string[],
	): Promise<SendResult[]> {
		const outcomes = await this.batchSendAttachments(chatId, images);
		return outcomes.map((o) =>
			o.success
				? { success: true }
				: {
						success: false,
						error: o.error ?? "attachment batch failed",
					},
		);
	}

	/** send_voice @~1602: Signal has no distinct voice API — same RPC send. */
	sendVoice(chatId: string, audioPath: string): Promise<SendResult> {
		return this.sendAttachment(chatId, audioPath, { mediaLabel: "Audio" });
	}

	/** send_video @~1611. */
	sendVideo(chatId: string, videoPath: string): Promise<SendResult> {
		return this.sendAttachment(chatId, videoPath, { mediaLabel: "Video" });
	}

	/** send_document @~1595. */
	sendDocument(chatId: string, filePath: string): Promise<SendResult> {
		return this.sendAttachment(chatId, filePath, { mediaLabel: "File" });
	}

	// ── guard wiring (reference-fixture inheritance) ─────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: SIGNAL_REGISTRY,
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
					await this.onProcessingStart(event);
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
					await this.onProcessingComplete(event, "success");
					return reply;
				},
				sendReply: async (_chatId, text) => {
					this.replyLog.push(text);
				},
			},
			{
				...(spawner !== undefined ? { spawner } : {}),
				hasPendingClarify: (key) => this.clarifyArmedSet.has(key),
			},
		);
	}

	setClarifyIntercept(sessionKey: string, on: boolean): void {
		if (on) this.clarifyArmedSet.add(sessionKey);
		else this.clarifyArmedSet.delete(sessionKey);
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

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	async deliverInbound(
		event: IncomingEvent,
		sessionKey: string,
	): Promise<void> {
		// Self/echo filter parity: bot-authored echoes never become turns.
		if (String(event.source?.userId ?? "") === "bot-self") return;
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	/** Relay-shaped descriptor seeding (§6.3/A15 conformance lane). */
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

	// ── multi-chat-safe delivery pipeline (base parity, per-call binding) ──

	/**
	 * ONE session-scoped formatting ladder (the §10.1 rich-downgrade latch
	 * must persist across chunks AND sends) whose door closures bind the
	 * CURRENT chatId dynamically. The base's lazily-built ladder captures the
	 * FIRST call's chatId in its closures — fine for single-chat rows but a
	 * latent mis-addressing for any multi-chat session; this override keeps
	 * every base behavior (chunk plan, ladder tiers, §6.1 retry ladder,
	 * plain-text fallback lane) while addressing each chunk at ITS chat.
	 * Discord precedent: adapters may override deliverText wholesale.
	 */
	private sessionLadder: FormattingLadder | null = null;
	private activeChatId = "";

	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		const policy = this.chatLengthPolicyForChat(chatId);
		const plan: ChunkPlan =
			this.splitsLongMessages || policy.lenFn(content) <= policy.maxUnits
				? {
						chunks: [content],
						chunkCount: 1,
						scaffold: [{ prefixLen: 0, closeAdded: false, labelJoinLen: 0 }],
					}
				: chunkWithFenceCarry(content, policy);

		if (this.sessionLadder === null) {
			this.sessionLadder = new FormattingLadder(
				{
					tryRich: (c, md) => this.wireRich(c, md),
					sendConverted: (c, md) => this.wireSend(this.activeChatId, c, md),
					sendPlain: (c, md) => this.wireSend(this.activeChatId, c, md),
				},
				{
					log: this.logger?.warn
						? (m, meta) => this.logger?.warn?.(m, meta)
						: undefined,
				},
			);
		}
		const ladder = this.sessionLadder;

		const results: SendResult[] = [];
		for (const chunk of plan.chunks) {
			this.activeChatId = chatId; // per-chunk door target binding
			results.push(await this.deliverChunkOn(ladder, chunk, metadata));
		}
		return results;
	}

	/** Base deliverChunk parity over the SHARED session ladder. */
	private async deliverChunkOn(
		ladder: FormattingLadder,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const outcome = await ladder.sendText(chunk, metadata);
		if (outcome.success) return outcome;

		// A transient RICH failure is NEVER legacy-resent (§10.1 duplicate risk).
		if (outcome.tier === "rich") return outcome;

		const failureClass = classifySendError(new Error(outcome.error ?? ""));
		const networkClassified =
			outcome.retryable === true ||
			failureClass === "connect-timeout" ||
			failureClass === "network" ||
			failureClass === "flood";
		if (networkClassified) {
			const retried = await sendWithRetry(
				chunk,
				metadata,
				(c, md) => this.wireSend(this.activeChatId, c, md),
				{ maxRetries: 2 },
			);
			if (retried.success) return retried;
			return this.wireSend(this.activeChatId, DELIVERY_FAILED_NOTICE, metadata);
		}

		if (failureClass === "formatting") {
			return this.wireSend(
				this.activeChatId,
				plainTextFallbackBody(chunk),
				metadata,
			);
		}
		return outcome;
	}
}

// ── helpers ──────────────────────────────────────────────────────────────────

function asArray(v: unknown): Array<Record<string, unknown>> {
	if (!Array.isArray(v)) return [];
	return v.filter(
		(x): x is Record<string, unknown> =>
			x !== null && typeof x === "object" && !Array.isArray(x),
	);
}

function arrayRec(v: unknown): Array<Record<string, unknown>> {
	return asArray(v);
}

function asRec(v: unknown): Record<string, unknown> {
	return v !== null && typeof v === "object" && !Array.isArray(v)
		? (v as Record<string, unknown>)
		: {};
}

function str(v: unknown): string {
	return typeof v === "string" ? v.trim() : "";
}

/** Raw string WITHOUT trimming (message bodies keep interior whitespace). */
function str2(v: unknown): string {
	return typeof v === "string" ? v : "";
}

/**
 * Validate a signal-cli send response results[] block
 * (signal.py:_validate_send_result parity): any typed non-SUCCESS entry or
 * success:false entry fails the send with its error shape.
 */
export function validateSendResult(result: unknown): {
	success: boolean;
	error?: string;
} {
	if (result === null || typeof result !== "object") return { success: true };
	const results = (result as Record<string, unknown>)["results"];
	if (Array.isArray(results)) {
		for (const r of results) {
			if (r === null || typeof r !== "object") continue;
			const rec = r as Record<string, unknown>;
			const rtype = rec["type"];
			if (
				typeof rtype === "string" &&
				rtype.length > 0 &&
				rtype !== "SUCCESS"
			) {
				return { success: false, error: rtype };
			}
			if ("success" in rec && rec["success"] !== true) {
				const fail = rec["failure"];
				return {
					success: false,
					error:
						fail !== undefined && fail !== null
							? String(fail)
							: "Recipient delivery failed",
				};
			}
		}
	}
	return { success: true };
}

function extractQuoteAuthor(
	quote: Record<string, unknown>,
): string | undefined {
	for (const key of [
		"author",
		"authorNumber",
		"authorUuid",
		"authorAci",
		"authorServiceId",
		"authorServiceIdString",
	]) {
		const value = quote[key];
		if (value !== undefined && value !== null && String(value).length > 0) {
			return String(value);
		}
	}
	return undefined;
}

function errText(err: unknown): string {
	return err instanceof Error ? err.message : String(err);
}
