// pi_platforms/whatsapp-personal/wa-personal-adapter — THE WhatsApp PERSONAL
// adapter (Baileys local Node bridge over loopback HTTP), ported from the
// READ-ONLY Hermes plugin plugins/platforms/whatsapp/adapter.py +
// gateway/platforms/whatsapp_common.py onto the kit base. Everything
// policy-shaped is inherited (kit base + behavior.ts); this module supplies
// TRANSPORT (the injected BridgeTransport seam — the port NEVER spawns the
// Node bridge) and CONFIG PRECEDENCE.
//
// Shape (DEC-002 polling family):
//   - GET /messages poll every 1s (30s client timeout parity as DATA); a poll
//     error sleeps 5s and CONTINUES — the loop never dies on transport noise
//   - text debounce batching mirrors Telegram-pattern __init__ defaults:
//     5.0s quiet period / 10.0s split delay when the latest chunk ≥ 6000
//     chars, joined "\n", timer RESET per arrival, injected clock throughout
//   - fire-and-forget read receipts (POST /read only when enabled AND the key
//     is object-shaped; a slow/failing bridge NEVER delays dispatch)
//   - send() chunks through the prefix-budgeted limit (max(1024, 4096−prefix)),
//     reply-context quoted on the FIRST chunk only, 0.3s inter-chunk pacing
//     ONLY while multiple chunks, continuation ids = all-but-last
//   - connect ladder ports the pre-flight ORDER: enabled-gate loud disable →
//     whatsapp_node_missing → whatsapp_bridge_missing → whatsapp_not_paired
//     (non-retryable pairing pre-flight)
//   - managed-bridge-exit classification distinguishes intentional shutdown
//     (-15/-2/0 while _shutting_down) from crash; stale/zombie bridge eviction
//     exists as DECISION DATA with an injected pid probe (OS mechanics
//     excluded — documented)
//
// EXCLUSIONS (probe-computed, never silent): media FILE download/caching,
// QR pairing flow, npm install, session-lock files, pid-file OS mechanics.
//
// Layering: imports pi_gateway downward + kit same-layer + whatsapp-cloud/
// wa-markdown.ts READ-ONLY (shared dialect converter — the SAME conversion as
// whatsapp_common.format_message); NO edits to sibling adapters.

import {
	ActionHandlerRegistry,
	BasePlatformAdapter,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	chunkWithFenceCarry,
	resolveEnablement,
} from "../kit/index.js";
import type {
	Metadata,
	SendResult,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { REPLY_TO_METADATA_KEY } from "../../pi_gateway/streaming/adapter-seam.js";
import { PLAIN_TEXT_FALLBACK_PREFIX } from "../kit/send-retry.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	CommandRegistry,
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import { buildSessionKey } from "../../pi_gateway/resolution/session-key.js";
import type { SessionSource } from "../../pi_gateway/resolution/session-key.js";
import { toWhatsappJid } from "../../pi_gateway/resolution/whatsapp-identity.js";
import type { ScopedSecretReader } from "../kit/registration.js";
import type { LengthUnit } from "../kit/length-policy.js";
import type { TimerSeam } from "../polling/clock.js";
import { realPollingClock } from "../polling/clock.js";

import { toWhatsappMarkup } from "../whatsapp-cloud/wa-markdown.js";
import type { BridgeTransport } from "./bridge-wire.js";
import {
	FATAL_BRIDGE_EXITED,
	FATAL_BRIDGE_MISSING,
	FATAL_NODE_MISSING,
	FATAL_NOT_PAIRED,
	WA_DEFAULT_MODE,
	WA_ENV_ALLOWED_USERS,
	WA_ENV_DM_POLICY,
	WA_ENV_FREE_RESPONSE_CHATS,
	WA_ENV_GROUP_POLICY,
	WA_ENV_MENTION_PATTERNS,
	WA_ENV_MODE,
	WA_ENV_REPLY_PREFIX,
	WA_ENV_REQUIRE_MENTION,
	WA_INTER_CHUNK_DELAY_MS,
	WA_MAX_MESSAGE_LENGTH,
	WA_OWNER_REPLY_PREFIX,
	WA_PENDING_BATCH_CAP,
	WA_POLL_ERROR_BACKOFF_MS,
	WHATSAPP_PERSONAL_PLUGIN_MANIFEST as WA_PERSONAL_PLUGIN_MANIFEST,
	WA_TEXT_BATCH_DELAY_SECONDS,
	WA_TEXT_BATCH_SPLIT_DELAY_SECONDS,
	WA_TEXT_SPLIT_THRESHOLD_CHARS,
	validateWaPersonalTrustBoundary,
	waPersonalTrustBoundary,
	type WaPersonalTrustBoundary,
} from "./manifest.js";
import {
	cleanBotMentionText,
	coerceAllowList,
	coerceBoolFlag,
	compileMentionPatterns,
	dmIntakeAllowed,
	effectiveReplyPrefix,
	isBroadcastChat,
	isReplyToBot,
	matchesMentionPatterns,
	mentionsBot,
	normalizeWhatsAppId,
	openDmOptedIn,
	outgoingChunkLimit,
	shouldProcessMessage,
	sanitizeOutboundText,
	type AliasResolver,
} from "./behavior.js";

/** adapter.py command registry derivation (07 §1 — mirrors reference set). */
export const WA_PERSONAL_REGISTRY: CommandRegistry = [
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

/** Injected time: sleep records/advances under tests; timers schedule flushes. */
export interface WaClock {
	nowMs(): number;
	sleep(ms: number): Promise<void>;
	timer?: TimerSeam | undefined;
}

export interface WaPersonalAdapterOptions {
	transport: BridgeTransport;
	/** Scoped reader over WHATSAPP_* names (fail-closed; DEC-003/009). */
	secretReader?: ScopedSecretReader | undefined;
	/** config.extra analog (dm_policy, allow_from, require_mention, …). */
	config?: Record<string, unknown> | undefined;
	clock?: WaClock | undefined;
	timer?: TimerSeam | undefined;
	aliasResolver?: AliasResolver | undefined;
	/**
	 * LID-mapping dir for session-key canonicalization (tests inject mkdtemp;
	 * default resolves PI_HOME per call).
	 */
	whatsappSessionDir?: string | undefined;
	/** Connect pre-flight injectables (source: fs/process probes). */
	credsPresent?: boolean | undefined;
	nodePresent?: boolean | undefined;
	bridgeScriptPresent?: boolean | undefined;
	/** Harness rich-probe tap (§10.1 latch row wiring). */
	richProbe?:
		| {
				hasScript(opKind: string): boolean;
				transmitRich(chatId: string, content: string): Promise<SendResult>;
		  }
		| undefined;
	scalarMaxUnits?: number | undefined;
}

interface PendingTextBatch {
	event: IncomingEvent;
	/** Length of the LATEST arrival — the split-delay switch reads THIS. */
	lastChunkLen: number;
}

/** Pure classification port of _check_managed_bridge_exit's verdict logic. */
export function classifyBridgeExit(
	exitCode: number | null | undefined,
	shuttingDown: boolean,
): "running" | "intentional-shutdown" | "crash" {
	if (exitCode === null || exitCode === undefined) return "running";
	if (shuttingDown && (exitCode === 0 || exitCode === -2 || exitCode === -15)) {
		return "intentional-shutdown";
	}
	return "crash";
}

// ── stale/zombie bridge eviction DECISION DATA ──────────────────────────────

/**
 * Pidfile record (bridge.pid line 1 = pid, optional line 2 = kernel start
 * ticks written by _write_bridge_pidfile).
 */
export interface PidfileRecord {
	pid: number;
	startTicks?: number | undefined;
}

/** Injected liveness probe standing in for the OS process mechanics. */
export interface PidProbe {
	alive(pid: number): boolean;
	startTicksOf(pid: number): number | null;
	cmdlineOf(pid: number): readonly string[] | null;
}

export type StaleBridgeDecision =
	| { action: "absent" }
	| { action: "kill"; pid: number }
	| { action: "skip-recycled"; pid: number };

/**
 * Decision port of _kill_stale_bridge_by_pidfile/_bridge_pid_is_ours: the
 * recorded PID is re-validated against the live process before any signal so
 * a RECYCLED pid (different kernel start time, or — for legacy pidfiles — an
 * unrelated cmdline) is never killed. Process mechanics excluded; this is the
 * decision as data.
 */
export function staleBridgeEvictionDecision(
	record: PidfileRecord | null | undefined,
	sessionPath: string,
	probe: PidProbe,
): StaleBridgeDecision {
	if (!record) return { action: "absent" };
	if (!probe.alive(record.pid)) return { action: "absent" };
	if (record.startTicks !== undefined) {
		return probe.startTicksOf(record.pid) === record.startTicks
			? { action: "kill", pid: record.pid }
			: { action: "skip-recycled", pid: record.pid };
	}
	// Legacy pidfile without a baseline: cmdline signature or refuse.
	const cmdline = probe.cmdlineOf(record.pid);
	if (!cmdline) return { action: "skip-recycled", pid: record.pid };
	const ours =
		cmdline.some((part) => part.includes("node")) &&
		cmdline.some((part) => part.includes(sessionPath));
	return ours
		? { action: "kill", pid: record.pid }
		: { action: "skip-recycled", pid: record.pid };
}

/** _is_connected value vocabulary — {"true","1","yes"} (no "on"). */
function isEnabledValue(value: string | undefined): boolean {
	if (!value) return false;
	return ["true", "1", "yes"].includes(value.trim().toLowerCase());
}

export class WaPersonalAdapter extends BasePlatformAdapter {
	readonly pluginManifest = WA_PERSONAL_PLUGIN_MANIFEST;
	readonly trustBoundary: WaPersonalTrustBoundary = waPersonalTrustBoundary();
	readonly transport: BridgeTransport;

	// Interactive surfaces (kit-owned, ONE router per adapter).
	readonly approvals = new OneShotPendingStore();
	readonly slashConfirms = new OneShotPendingStore();
	readonly appr = new OneShotPendingStore();
	readonly clarify = new ClarifyPendingStore();
	readonly actionRegistry = new ActionHandlerRegistry();
	readonly router: CallbackQueryRouter;

	private readonly cp: EgressChokepoint;
	private readonly secretReader: ScopedSecretReader;
	private readonly extra: Record<string, unknown>;
	private readonly clock: WaClock;
	private readonly timerFn: TimerSeam;
	private readonly aliasResolver: AliasResolver | undefined;
	readonly whatsappSessionDir: string | undefined;

	// ── config precedence (__init__ parity) ───────────────────────────────────
	readonly dmPolicy: string;
	groupPolicy: string;
	/**
	 * WHICH source won the DM allowlist ("config" | env-name | null). Env-
	 * sourced allowlists re-read PER CHECK (pairing approve/revoke takes
	 * effect without restart); config-sourced stay frozen — a lower-precedence
	 * or stale env value must never broaden access.
	 */
	readonly dmAllowlistSource: string | null;
	private readonly configuredDmAllowFrom: ReadonlySet<string>;
	readonly groupAllowFrom: ReadonlySet<string>;
	readonly sendReadReceipts: boolean;
	private readonly replyPrefixConfig: string | null;
	readonly textBatchDelaySeconds: number;
	readonly textBatchSplitDelaySeconds: number;
	readonly mentionPatterns: readonly RegExp[];
	/** Patterns skipped for invalid regex syntax (invalid-skip contract). */
	readonly invalidMentionPatterns: readonly string[];

	// ── pre-flight injectables ────────────────────────────────────────────────
	private readonly credsPresent: boolean;
	private readonly nodePresent: boolean;
	private readonly bridgeScriptPresent: boolean;

	// ── runtime state ─────────────────────────────────────────────────────────
	private running_ = false;
	private generation_ = 0;
	/** Set BEFORE signalling the child so exit classification stays honest. */
	shuttingDown = false;
	/** Managed child .poll() analog: undefined=unmodeled, null=alive, number=exited. */
	private managedExitCode: number | null | undefined = undefined;
	fatalCode: string | null = null;
	/** Source _set_fatal_error(retryable=…) flag — pairing pre-flight FALSE. */
	fatalRetryable: boolean | null = null;

	readonly counters = {
		polls: 0,
		pollErrors: 0,
		pollLoopBreaks: 0,
		consecutivePollFailures: 0,
		filteredInbound: 0,
		dispatchImmediate: 0,
		debouncedEnqueues: 0,
		debouncedFlushes: 0,
		receiptsAttempted: 0,
		receiptsFailed: 0,
		chunksSent: 0,
		editsSent: 0,
		escalations: 0,
	};
	readonly turnLog: string[] = [];
	readonly replyLog: string[] = [];
	readonly clarifyCaptures: string[] = [];
	readonly resolvedFamilies: string[] = [];
	readonly recoveryLog: string[] = [];
	/** Per-wireSend breakdown (continuation-id contract observability). */
	readonly sendLog: Array<{
		jid: string;
		payloads: Array<Record<string, unknown>>;
		messageIds: string[];
	}> = [];

	private readonly pendingTextBatches = new Map<string, PendingTextBatch>();
	private readonly pendingTimers = new Map<string, () => void>();
	private readonly inflightReceipts = new Set<Promise<void>>();

	private allowAllClickers = true;
	private readonly clarifyArmedSet = new Set<string>();
	private holdGate: Promise<void> = Promise.resolve();
	private releaseHold: () => void = () => {};
	private holding = false;

	constructor(opts: WaPersonalAdapterOptions) {
		super({
			manifestName: WA_PERSONAL_PLUGIN_MANIFEST.name,
			capabilities: WA_PERSONAL_PLUGIN_MANIFEST.capabilities,
			scalarMaxUnits: opts.scalarMaxUnits ?? WA_MAX_MESSAGE_LENGTH,
		});
		this.transport = opts.transport;
		this.secretReader = opts.secretReader ?? ((name) => process.env[name]);
		this.extra = opts.config ?? {};
		this.aliasResolver = opts.aliasResolver;
		this.whatsappSessionDir = opts.whatsappSessionDir;
		this.credsPresent = opts.credsPresent ?? true;
		this.nodePresent = opts.nodePresent ?? true;
		this.bridgeScriptPresent = opts.bridgeScriptPresent ?? true;
		const clock: WaClock = opts.clock ?? realPollingClock();
		this.clock = clock;
		this.timerFn = opts.timer ?? clock.timer ?? realPollingClock().timer;

		// __init__ config precedence (KEY-PRESENCE selection).
		this.replyPrefixConfig =
			this.extra["reply_prefix"] === undefined ||
			this.extra["reply_prefix"] === null
				? null
				: String(this.extra["reply_prefix"]);
		this.dmPolicy = String(
			this.extra["dm_policy"] ?? this.env(WA_ENV_DM_POLICY) ?? "pairing",
		)
			.trim()
			.toLowerCase();
		let allowRaw: unknown = null;
		if ("allow_from" in this.extra) {
			this.dmAllowlistSource = "config";
			allowRaw = this.extra["allow_from"];
		} else if ("allowFrom" in this.extra) {
			this.dmAllowlistSource = "config";
			allowRaw = this.extra["allowFrom"];
		} else if (this.env(WA_ENV_ALLOWED_USERS)) {
			this.dmAllowlistSource = WA_ENV_ALLOWED_USERS;
			allowRaw = this.env(WA_ENV_ALLOWED_USERS);
		} else {
			this.dmAllowlistSource = null;
			allowRaw = null;
		}
		this.configuredDmAllowFrom = coerceAllowList(allowRaw);
		this.groupPolicy = String(
			this.extra["group_policy"] ?? this.env(WA_ENV_GROUP_POLICY) ?? "pairing",
		)
			.trim()
			.toLowerCase();
		this.groupAllowFrom = coerceAllowList(
			this.extra["group_allow_from"] ?? this.extra["groupAllowFrom"],
		);
		this.sendReadReceipts = coerceBoolFlag(
			this.extra["send_read_receipts"] ?? false,
			false,
		);
		const batch = coerceFloatExtra(
			this.extra,
			"text_batch_delay_seconds",
			WA_TEXT_BATCH_DELAY_SECONDS,
		);
		const split = coerceFloatExtra(
			this.extra,
			"text_batch_split_delay_seconds",
			WA_TEXT_BATCH_SPLIT_DELAY_SECONDS,
		);
		this.textBatchDelaySeconds = batch;
		this.textBatchSplitDelaySeconds = split;
		const compiled = compileMentionPatterns(this.extra, (n) => this.env(n));
		this.mentionPatterns = compiled.compiled;
		this.invalidMentionPatterns = compiled.invalid;

		// §11 step 3/4: missing required secret ⇒ LOUD disable (status-visible).
		const enablement = resolveEnablement(
			WA_PERSONAL_PLUGIN_MANIFEST,
			this.secretReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		}

		// DEC-017: an incomplete trust boundary is a CONSTRUCTION-TIME error.
		const boundaryErrors = validateWaPersonalTrustBoundary(this.trustBoundary);
		if (boundaryErrors.length > 0) {
			this.lifecycle.disable({
				kind: "config_invalid",
				detail: boundaryErrors.join("; "),
			});
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: () => false, // no native draft lanes on the bridge
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			transmitEdit: async (chatId, messageId, content) =>
				this.editViaBridge(chatId, messageId, content),
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

	private env(name: string): string | undefined {
		return this.secretReader(name);
	}

	get running(): boolean {
		return this.running_;
	}

	get generation(): number {
		return this.generation_;
	}

	lifecycleSnapshot() {
		return this.lifecycle.statusSnapshot();
	}

	// ── scoped config resolvers (live where Hermes re-reads live) ────────────

	/**
	 * _live_dm_allow_from: env-sourced allowlists RE-READ the same key per
	 * check (including an EMPTY value while the key is present); a REMOVED key
	 * yields an empty set — the stale construction snapshot must not revive.
	 * Config-sourced adapters keep the frozen snapshot.
	 */
	liveDmAllowFrom(): ReadonlySet<string> {
		const source = this.dmAllowlistSource;
		if (typeof source === "string" && source !== "config") {
			const current = this.env(source);
			if (current === undefined) return new Set(); // key removed — fail closed
			return coerceAllowList(current);
		}
		return this.configuredDmAllowFrom;
	}

	private resolveRequireMention(): boolean {
		const configured = this.extra["require_mention"];
		if (configured !== null && configured !== undefined) {
			return coerceBoolFlag(configured);
		}
		return coerceBoolFlag(this.env(WA_ENV_REQUIRE_MENTION) ?? "false");
	}

	private resolveFreeResponseChats(): ReadonlySet<string> {
		let raw: unknown = this.extra["free_response_chats"];
		if (raw === null || raw === undefined) {
			raw = this.env(WA_ENV_FREE_RESPONSE_CHATS) ?? "";
		}
		if (Array.isArray(raw)) {
			return coerceAllowList(raw);
		}
		return coerceAllowList(String(raw));
	}

	resolveMode(): string {
		return this.env(WA_ENV_MODE) || WA_DEFAULT_MODE;
	}

	effectiveReplyPrefixValue(): string {
		return effectiveReplyPrefix({
			mode: this.resolveMode(),
			configuredPrefix: this.replyPrefixConfig,
			envPrefix: this.env(WA_ENV_REPLY_PREFIX),
		});
	}

	openDmOptedInNow(): boolean {
		return openDmOptedIn((n) => this.env(n) ?? "");
	}

	/** The gating policy VIEW handed to behavior.ts per check. */
	gatingPolicy() {
		return {
			dmPolicy: this.dmPolicy,
			dmAllowFrom: () => this.liveDmAllowFrom(),
			groupPolicy: this.groupPolicy,
			groupAllowFrom: this.groupAllowFrom,
			freeResponseChats: this.resolveFreeResponseChats(),
			requireMention: this.resolveRequireMention(),
			mentionPatterns: this.mentionPatterns,
			aliasResolver: this.aliasResolver,
			openDmOptedIn: () => this.openDmOptedInNow(),
		};
	}

	shouldProcess(data: Record<string, unknown>): boolean {
		return shouldProcessMessage(
			this.gatingPolicy(),
			data,
			(n) => this.env(n) ?? "",
		);
	}

	// ── connect ladder (order ports connect() pre-flight) ────────────────────

	private markFatalCode(
		code: string,
		message: string,
		retryable: boolean,
	): void {
		this.fatalCode = code;
		this.fatalRetryable = retryable;
		this.lifecycle.markFatal({
			kind: "config_invalid",
			detail: `${code}: ${message}`,
		});
	}

	override async connect(_opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		// Loud-disable gate: WhatsApp runs ONLY when explicitly enabled
		// (_is_connected vocabulary {"true","1","yes"}).
		if (!isEnabledValue(this.env("WHATSAPP_ENABLED"))) {
			this.lifecycle.disable({
				kind: "manual",
				detail:
					"WHATSAPP_ENABLED is falsy — WhatsApp stays disabled (loud disable)",
			});
			return false;
		}
		if (!this.nodePresent) {
			this.markFatalCode(
				FATAL_NODE_MISSING,
				"Node.js is not installed — install Node.js and re-run the gateway.",
				false,
			);
			return false;
		}
		if (!this.bridgeScriptPresent) {
			this.markFatalCode(
				FATAL_BRIDGE_MISSING,
				"WhatsApp bridge script missing.",
				false,
			);
			return false;
		}
		// Pairing pre-flight: skip bootstrap entirely when creds.json was never
		// written — NON-retryable so the user gets a clear pairing message.
		if (!this.credsPresent) {
			this.markFatalCode(
				FATAL_NOT_PAIRED,
				"WhatsApp enabled but not paired — pair from the dashboard or run the pairing CLI.",
				false,
			);
			return false;
		}
		this.shuttingDown = false;
		this.managedExitCode = undefined;
		this.running_ = true;
		this.generation_ += 1;
		return true;
	}

	override async disconnect(): Promise<void> {
		// Flip BEFORE any child signal so exit classification stays honest.
		this.shuttingDown = true;
		for (const cancel of this.pendingTimers.values()) cancel();
		this.pendingTimers.clear();
		this.pendingTextBatches.clear();
		this.running_ = false;
	}

	// ── managed-bridge-exit machinery ────────────────────────────────────────

	injectBridgeExit(code: number | null | undefined): void {
		this.managedExitCode = code;
	}

	/**
	 * _check_managed_bridge_exit: fatal message when the MANAGED child exited
	 * outside planned shutdown; intentional shutdown codes (-15/-2/0) during
	 * disconnect are informational. A crash marks FATAL ONCE (retryable=True —
	 * restart ownership belongs to the runner watcher).
	 */
	checkManagedBridgeExit(): string | null {
		const verdict = classifyBridgeExit(this.managedExitCode, this.shuttingDown);
		if (verdict !== "crash") return null;
		const code = this.managedExitCode;
		const message = `WhatsApp bridge process exited unexpectedly (code ${String(code)}).`;
		if (this.fatalCode === null) {
			this.markFatalCode(FATAL_BRIDGE_EXITED, message, true);
		}
		return this.fatalCode === FATAL_BRIDGE_EXITED
			? message
			: `${this.fatalCode}: ${message}`;
	}

	// ── poll plane ───────────────────────────────────────────────────────────

	/**
	 * ONE poll cycle (adapter.py:_poll_messages body). Errors sleep the 5s
	 * backoff and RETURN (loop-alive parity — never fatal); a detected managed
	 * exit breaks the loop. Text events enter the debounce batcher; everything
	 * else dispatches straight through.
	 */
	async pollOnce(): Promise<void> {
		if (!this.running_) return;
		const exitMessage = this.checkManagedBridgeExit();
		if (exitMessage) {
			this.counters.pollLoopBreaks += 1;
			return; // break parity
		}
		let json: unknown;
		try {
			const resp = await this.transport.getMessages();
			if (resp.status !== 200) {
				throw new Error(`bridge poll HTTP ${resp.status}`);
			}
			json = resp.json;
		} catch {
			this.counters.pollErrors += 1;
			this.counters.consecutivePollFailures += 1;
			await this.clock.sleep(WA_POLL_ERROR_BACKOFF_MS); // error → sleep 5s → continue
			return;
		}
		this.counters.consecutivePollFailures = 0;
		this.counters.polls += 1;
		const messages = Array.isArray(json) ? json : [];
		for (const entry of messages) {
			if (entry === null || typeof entry !== "object") continue;
			const data = entry as Record<string, unknown>;
			const event = this.buildMessageEvent(data);
			if (!event) continue;
			// Fire-and-forget: a slow bridge /read must not delay dispatch.
			this.dispatchReadReceipt(data);
			if (event.messageType === "text") {
				this.enqueueTextEvent(event);
			} else {
				this.counters.dispatchImmediate += 1;
				await this.dispatchInbound(event);
			}
		}
	}

	/**
	 * Heartbeat-escalation rung: TWO consecutive poll errors keep the loop
	 * alive with backoff (source: error → sleep 5s → continue — NEVER fatal);
	 * a /health "disconnected" verdict escalates into the reconnect ladder
	 * (generation bump + counter reset). Port ladder synthesized from source
	 * mechanics: Hermes tolerates disconnects indefinitely and relies on the
	 * managed-exit break + external supervision; the port pins the escalation
	 * as observable data instead of silent retry forever.
	 */
	async escalateIfUnhealthy(): Promise<boolean> {
		if (!this.running_) return false;
		let unhealthy = this.counters.consecutivePollFailures >= 2;
		if (!unhealthy) {
			try {
				const health = await this.transport.getHealthStatus();
				const status =
					typeof health.json === "object" && health.json !== null
						? (health.json as Record<string, unknown>)["status"]
						: undefined;
				unhealthy = status === "disconnected";
			} catch {
				return false; // transport down — the poll-error ladder owns it
			}
		}
		if (!unhealthy) return false;
		this.counters.escalations += 1;
		this.recoveryLog.push(
			`escalate-reconnect:stuck=${this.counters.consecutivePollFailures}`,
		);
		this.counters.consecutivePollFailures = 0;
		this.generation_ += 1;
		return true;
	}

	// ── inbound event construction (_build_message_event mapping) ───────────

	buildMessageEvent(data: Record<string, unknown>): IncomingEvent | null {
		if (!this.shouldProcess(data)) {
			this.counters.filteredInbound += 1;
			return null;
		}
		let messageType: IncomingEvent["messageType"] = "text";
		const mediaType = String(data["mediaType"] ?? "").toLowerCase();
		if (mediaType === "location" || mediaType === "live_location") {
			messageType = "location";
		} else if (mediaType === "sticker") {
			// No sticker member in the Pi MessageType union: rides "other"
			// (non-text lanes bypass the debounce batcher — semantic preserved).
			messageType = "other";
		} else if (data["hasMedia"]) {
			if (mediaType.includes("image")) messageType = "photo";
			else if (mediaType.includes("video")) messageType = "video";
			else if (mediaType.includes("ptt"))
				messageType = "voice"; // ptt = voice note
			else if (mediaType.includes("audio")) messageType = "voice";
			else messageType = "document";
		}
		const isGroup = Boolean(data["isGroup"]);

		let body = String(data["body"] ?? "");
		if (isGroup) body = cleanBotMentionText(body, data);

		const metadata: Record<string, unknown> = {};
		const nativeType = String(data["nativeType"] ?? "").trim();
		if (nativeType) metadata["whatsapp_native_type"] = nativeType;
		const nativeMetadata = data["nativeMetadata"];
		if (
			nativeMetadata !== null &&
			typeof nativeMetadata === "object" &&
			!Array.isArray(nativeMetadata)
		) {
			metadata["whatsapp_native"] = nativeMetadata;
		}
		if (data["hasQuotedMessage"]) {
			const rawReplyId = data["quotedMessageId"];
			if (rawReplyId !== null && rawReplyId !== undefined) {
				metadata["reply_to_message_id"] = String(rawReplyId);
			}
			const quotedParticipant = normalizeWhatsAppId(data["quotedParticipant"]);
			if (quotedParticipant) {
				metadata["reply_to_author_id"] = quotedParticipant;
			}
			metadata["reply_to_is_own_message"] = isReplyToBot(data);
			const quotedText = String(data["quotedText"] ?? "").trim();
			if (quotedText) metadata["reply_to_text"] = quotedText;
		}
		if (data["fromOwner"]) {
			metadata["whatsapp_from_owner"] = true;
			if (!body.startsWith(WA_OWNER_REPLY_PREFIX)) {
				body = `${WA_OWNER_REPLY_PREFIX}${body}`;
			}
		}

		// Media refs pass through AS DATA. EXCLUSION (documented): the source
		// downloads images/audio/documents into cache dirs with allowlist path
		// validation (_is_allowed_bridge_path); file-system caching is out of
		// scope for this port — URLs ride the event verbatim.
		const mediaUrls = Array.isArray(data["mediaUrls"])
			? (data["mediaUrls"] as unknown[]).filter(
					(u): u is string => typeof u === "string",
				)
			: undefined;
		const mime = String(data["mime"] ?? "").trim();

		return {
			...(typeof data["messageId"] === "string"
				? { messageId: data["messageId"] }
				: {}),
			text: body,
			messageType,
			...(mediaUrls !== undefined && mediaUrls.length > 0 ? { mediaUrls } : {}),
			...(mediaUrls !== undefined && mediaUrls.length > 0
				? {
						mediaTypes: mediaUrls.map(() => mime || "application/octet-stream"),
					}
				: {}),
			metadata,
			source: {
				platform: WA_PERSONAL_PLUGIN_MANIFEST.name,
				chatType: isGroup ? "group" : "dm",
				userId: String(data["senderId"] ?? ""),
				chatId: String(data["chatId"] ?? ""),
				...(data["senderName"] !== undefined &&
				String(data["senderName"] ?? "").length > 0
					? { chatName: String(data["senderName"]) }
					: {}),
			},
		};
	}

	// ── read receipts (fire-and-forget) ─────────────────────────────────────

	private dispatchReadReceipt(data: Record<string, unknown>): void {
		if (!this.sendReadReceipts) return;
		const key = data["readReceiptKey"];
		if (key === null || typeof key !== "object" || Array.isArray(key)) return;
		this.counters.receiptsAttempted += 1;
		const flight = this.transport
			.markRead(key as Record<string, unknown>)
			.then((resp) => {
				if (resp.status !== 200) this.counters.receiptsFailed += 1; // warn parity
			})
			.catch(() => {
				this.counters.receiptsFailed += 1;
			})
			.finally(() => {
				this.inflightReceipts.delete(flight);
			});
		this.inflightReceipts.add(flight);
	}

	/** Deterministic drain for tests (fire-and-forget semantics unchanged). */
	async settleReceipts(): Promise<void> {
		await Promise.all([...this.inflightReceipts]);
	}

	// ── text debounce batching ──────────────────────────────────────────────

	private textBatchKeyOf(event: IncomingEvent): string {
		const fallback: SessionSource = {
			platform: WA_PERSONAL_PLUGIN_MANIFEST.name,
			chatType: "dm",
		};
		const source = event.source ?? fallback;
		return buildSessionKey(
			{
				platform: source.platform,
				chatType: source.chatType,
				...(source.userId !== undefined ? { userId: source.userId } : {}),
				...(source.chatId !== undefined ? { chatId: source.chatId } : {}),
				...(source.chatName !== undefined ? { chatName: source.chatName } : {}),
			},
			{},
			undefined,
			this.whatsappSessionDir
				? { whatsapp: { sessionDir: this.whatsappSessionDir } }
				: {},
		);
	}

	/**
	 * _enqueue_text_event: buffer + RESET the flush timer per arrival. The
	 * delay is chosen from the LATEST arrival's chunk length (≥6000 ⇒ split
	 * delay). Bounded drop-oldest at WA_PENDING_BATCH_CAP (hardening bound —
	 * see manifest).
	 */
	enqueueTextEvent(event: IncomingEvent): void {
		const key = this.textBatchKeyOf(event);
		const existing = this.pendingTextBatches.get(key);
		const chunkLen = (event.text ?? "").length;
		if (existing === undefined) {
			if (this.pendingTextBatches.size >= WA_PENDING_BATCH_CAP) {
				const oldest = this.pendingTextBatches.keys().next().value;
				if (oldest !== undefined) {
					this.pendingTextBatches.delete(oldest);
					this.pendingTimers.get(oldest)?.();
					this.pendingTimers.delete(oldest);
				}
			}
			this.pendingTextBatches.set(key, { event, lastChunkLen: chunkLen });
		} else {
			if (event.text) {
				existing.event.text = existing.event.text
					? `${existing.event.text}\n${event.text}`
					: event.text;
			}
			existing.lastChunkLen = chunkLen;
			if (event.mediaUrls && event.mediaUrls.length > 0) {
				existing.event.mediaUrls = [
					...(existing.event.mediaUrls ?? []),
					...event.mediaUrls,
				];
			}
		}
		this.counters.debouncedEnqueues += 1;

		const prior = this.pendingTimers.get(key);
		if (prior) prior(); // reset-the-timer parity (cancel prior flush)
		const batch = this.pendingTextBatches.get(key);
		const lastLen = batch?.lastChunkLen ?? 0;
		const delaySeconds =
			lastLen >= WA_TEXT_SPLIT_THRESHOLD_CHARS
				? this.textBatchSplitDelaySeconds
				: this.textBatchDelaySeconds;
		const canceller = this.timerFn(Math.round(delaySeconds * 1000), () => {
			void this.flushTextBatch(key);
		});
		this.pendingTimers.set(key, canceller);
	}

	private async flushTextBatch(key: string): Promise<void> {
		const pending = this.pendingTextBatches.get(key);
		if (!pending) return;
		this.pendingTextBatches.delete(key);
		this.pendingTimers.delete(key);
		this.counters.debouncedFlushes += 1;
		await this.dispatchInbound(pending.event);
	}

	get heldTextBatchCount(): number {
		return this.pendingTextBatches.size;
	}

	cancelAllTimers(): void {
		for (const cancel of this.pendingTimers.values()) cancel();
		this.pendingTimers.clear();
	}

	// ── ingress plumbing ─────────────────────────────────────────────────────

	async dispatchInbound(event: IncomingEvent): Promise<void> {
		const fallback: SessionSource = {
			platform: WA_PERSONAL_PLUGIN_MANIFEST.name,
			chatType: "dm",
			userId: "",
			chatId: "",
		};
		const source = event.source ?? fallback;
		const sessionKey = buildSessionKey(
			{
				platform: source.platform,
				chatType: source.chatType,
				...(source.userId !== undefined ? { userId: source.userId } : {}),
				...(source.chatId !== undefined ? { chatId: source.chatId } : {}),
				...(source.chatName !== undefined ? { chatName: source.chatName } : {}),
			},
			{},
			undefined,
			this.whatsappSessionDir
				? { whatsapp: { sessionDir: this.whatsappSessionDir } }
				: {},
		);
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.deliverInbound(event, sessionKey);
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

	// ── guard wiring (reference-fixture inheritance) ─────────────────────────

	attachStandardGuard(spawner?: TaskSpawner | undefined): void {
		this.attachGuard(
			{
				registry: WA_PERSONAL_REGISTRY,
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
					this.replyLog.push(reply);
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

	setClickerAuthorization(allow: boolean): void {
		this.allowAllClickers = allow;
	}

	// ── egress doors ─────────────────────────────────────────────────────────

	/**
	 * Source parity: a DETECTED BRIDGE EXIT fails the send CLEANLY —
	 * SendResult(success=False, error=bridge_exit) instead of the kit's
	 * disabled-throw (adapter.py:send returns an error result so callers see
	 * WHY, and _check_managed_bridge_exit's message must survive). Every other
	 * disabled state keeps the kit contract.
	 */
	override async send(
		chatId: string,
		content: string,
		replyTo?: string | undefined,
		metadata?: Metadata | undefined,
	): Promise<SendResult> {
		if (
			this.lifecycle.state === "fatal" &&
			this.fatalCode === FATAL_BRIDGE_EXITED
		) {
			const exitMessage = this.checkManagedBridgeExit();
			if (exitMessage !== null) {
				return { success: false, error: exitMessage };
			}
		}
		return super.send(chatId, content, replyTo, metadata);
	}

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * Per-chat length descriptor (§6.3/A15 relay-shaped override point):
	 * harness utf16-marked chats return budget AND unit TOGETHER.
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

	/**
	 * DOOR transport — adapter.py:send parity. Content sanitizes THEN converts
	 * to WhatsApp markup (format_message sanitizes internally), chunks through
	 * the PREFIX-BUDGETED limit, quotes reply context on the FIRST chunk only,
	 * paces 0.3s between chunks ONLY while multiple chunks remain, and reports
	 * continuation ids = all-but-last.
	 */
	protected override async wireSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		if (!this.running_) {
			return { success: false, error: "Not connected" };
		}
		const exitMessage = this.checkManagedBridgeExit();
		if (exitMessage) return { success: false, error: exitMessage };
		if (!content || !content.trim().length) {
			return { success: true }; // SendResult(success=True, message_id=None) parity
		}

		const jid = toWhatsappJid(chatId);
		// The §6.1 plain-text fallback lane carries ORIGINAL chunk bytes by
		// contract — dialect conversion is SKIPPED for that envelope (its
		// prefix marks it); everything else sanitizes THEN converts.
		const formatted = content.startsWith(PLAIN_TEXT_FALLBACK_PREFIX)
			? content
			: toWhatsappMarkup(sanitizeOutboundText(content));

		// THE one chat length policy, intersected with the prefix budget
		// (chars-unit chats only — a utf16 budget cannot mix with a char budget).
		const policy = this.chatLengthPolicyForChat(chatId);
		const prefixBudget = outgoingChunkLimit(
			this.effectiveReplyPrefixValue().length,
		);
		const budget =
			policy.unit === "chars"
				? Math.min(policy.maxUnits, prefixBudget)
				: policy.maxUnits;
		const plan =
			policy.lenFn(formatted) <= budget
				? [formatted]
				: chunkWithFenceCarry(formatted, {
						...policy,
						maxUnits: budget,
					}).chunks;

		const replyTo =
			typeof metadata[REPLY_TO_METADATA_KEY] === "string"
				? (metadata[REPLY_TO_METADATA_KEY] as string)
				: undefined;

		const payloads: Array<Record<string, unknown>> = [];
		const messageIds: string[] = [];
		let lastId: string | null = null;
		for (const [idx, chunk] of plan.entries()) {
			const payload: Record<string, unknown> = {
				chatId: jid,
				message: chunk,
			};
			if (idx === 0 && replyTo) payload["replyTo"] = replyTo;
			payloads.push(payload);

			let resp;
			try {
				resp = await this.transport.sendText(
					{
						chatId: jid,
						message: chunk,
						...(idx === 0 && replyTo ? { replyTo } : {}),
					},
					metadata,
				);
			} catch (err) {
				return {
					success: false,
					error: err instanceof Error ? err.message : String(err),
				};
			}
			if (resp.status !== 200) {
				return {
					success: false,
					error: resp.text ?? `send HTTP ${resp.status}`,
				};
			}
			const mid =
				typeof resp.json === "object" &&
				resp.json !== null &&
				typeof (resp.json as Record<string, unknown>)["messageId"] === "string"
					? ((resp.json as Record<string, unknown>)["messageId"] as string)
					: null;
			if (mid) {
				messageIds.push(mid);
				lastId = mid;
			}
			// Small delay between chunks to avoid rate limiting — ONLY multi-chunk.
			if (plan.length > 1) {
				await this.clock.sleep(WA_INTER_CHUNK_DELAY_MS);
			}
		}
		this.counters.chunksSent += plan.length;
		this.sendLog.push({ jid, payloads, messageIds });
		return {
			success: true,
			...(lastId !== null ? { messageId: lastId } : {}),
		};
	}

	/** POST /edit parity — 15s window modeled as call-site data. */
	async editViaBridge(
		chatId: string,
		messageId: string,
		content: string,
	): Promise<SendResult> {
		if (!this.running_) return { success: false, error: "Not connected" };
		const exitMessage = this.checkManagedBridgeExit();
		if (exitMessage) return { success: false, error: exitMessage };
		this.counters.editsSent += 1;
		try {
			const resp = await this.transport.editMessage({
				chatId: toWhatsappJid(chatId),
				messageId,
				message: content,
			});
			return resp.status === 200
				? { success: true, messageId }
				: { success: false, error: resp.text ?? `edit HTTP ${resp.status}` };
		} catch (err) {
			return {
				success: false,
				error: err instanceof Error ? err.message : String(err),
			};
		}
	}

	/** Base editMessage door routes here (transmitEdit binding above). */
	protected override async wireEdit(
		chatId: string,
		messageId: string,
		content: string,
		_opts: { finalize: boolean },
	): Promise<SendResult> {
		return this.editViaBridge(chatId, messageId, content);
	}

	/** Rich lane ABSENT on the bridge wire unless the harness scripts one. */
	protected override async wireRich(
		content: string,
		_metadata: Metadata,
	): Promise<SendResult> {
		const probe = this.richProbeRef;
		if (probe === undefined || !probe.hasScript("rich")) {
			return { success: false, error: "sendRichMessage: method not found" };
		}
		return probe.transmitRich("__rich__", content);
	}

	private richProbeRef?: WaPersonalAdapterOptions["richProbe"];

	/** Subject-side wiring for the harness rich tap. */
	bindRichProbe(
		probe: NonNullable<WaPersonalAdapterOptions["richProbe"]>,
	): void {
		this.richProbeRef = probe;
	}
}

/** __init__._coerce_float_extra: NaN/Inf/negative/unparseable fall back. */
function coerceFloatExtra(
	extra: Record<string, unknown>,
	key: string,
	defaultValue: number,
): number {
	const value = extra[key];
	if (value === null || value === undefined) return defaultValue;
	const parsed = typeof value === "number" ? value : Number(value);
	if (!Number.isFinite(parsed) || parsed < 0) return defaultValue;
	return parsed;
}
