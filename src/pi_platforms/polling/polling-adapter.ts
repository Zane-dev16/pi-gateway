// pi_platforms/polling/polling-adapter — THE polling-shape reference adapter
// (DEC-002 shape 1; 04-platform-adapters.md §3.1 + §8 polling rows).
//
// Ported from the READ-ONLY Hermes reference, semantics only, cited as
// file:symbol anchors — no code vendored:
//   plugins/platforms/telegram/adapter.py:_hold_inbound_event /
//     _schedule_held_inbound_redispatch / _redispatch_held_inbound
//     → holdInbound / scheduleHeldRedispatch / redispatchHeldInbound
//       (ack-before-enqueue ⇒ hold-and-redispatch, never drop; cap 64,
//        drop-oldest, identity-dedup; fatal discards explicitly)
//   adapter.py:_handle_polling_conflict → handlePollingConflict
//     (retries ≤ MAX_CONFLICT_RETRIES=5, RETRY_DELAY = 10 + count·10 s, then a
//      restart passing drop_pending_updates=True — the only way to kill the
//      stale server-side session — under a bumped generation; exhaustion ⇒ FATAL)
//   adapter.py:_record_polling_progress → recordPollingProgress
//     (a generation counts healthy only after recorded progress)
//   adapter.py:_polling_heartbeat / _probe_pending_updates → heartbeatTick
//     (HEARTBEAT_INTERVAL probe; TWO consecutive stuck probes or
//      updater-not-running ×2 feed the SAME reconnect ladder)
//   adapter.py FloodWait extraction at EVERY site → sendTyping honors the
//     server retry_after once via the injected clock; edits surface
//     flood_control:<wait> with retryAfter set WITHOUT blocking the caller.
//
// Offset-commit-before-enqueue discipline: the fake server ACKs updates when
// a later call carries offset > update_id (PTB advances its offset before
// handlers run). The engine commits server-side BEFORE dispatch; an event
// that cannot be dispatched NOW is HELD and redispatched on _mark_connected,
// so the outage window between commit and enqueue loses NOTHING.

import type {
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	DraftFrameArgs,
	EditOptions,
} from "../../pi_gateway/streaming/adapter-seam.js";
import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	TokenLockManagerSeam,
	extractRetryAfterSeconds,
	resolveEnablement,
	type LockAcquisition,
} from "../kit/index.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { LengthUnit } from "../kit/length-policy.js";
import type {
	IncomingEvent,
	TaskSpawner,
} from "../../pi_gateway/guards/index.js";
import { immediateSpawner } from "../../pi_gateway/guards/index.js";
import type { GatewayTask } from "../../pi_gateway/guards/index.js";
import { AdapterDisabledError } from "../kit/lifecycle-state.js";
import type { FakeTelegramServer, FakeUpdate } from "./fake-server.js";
import {
	realPollingClock,
	type PollingClock,
	type TimerSeam,
} from "./clock.js";

/** adapter.py:HELD_INBOUND_MAX — cap with drop-oldest overflow policy. */
export const HELD_INBOUND_MAX = 64;
/** adapter.py:MAX_CONFLICT_RETRIES. */
export const MAX_CONFLICT_RETRIES = 5;
/** RETRY_DELAY = 10 + conflictCount·10 seconds (15s, 25s, … 65s). */
export function conflictRetryDelayMs(conflictCount: number): number {
	return (10 + conflictCount * 10) * 1000;
}
/** adapter.py:_polling_heartbeat constants. */
export const HEARTBEAT_INTERVAL_MS = 90_000;
export const PROBE_TIMEOUT_MS = 15_000;
/** Base-owned typing refresh cadence (~2 s per refresh). */
export const TYPING_REFRESH_MS = 2_000;

const REQUIRED_SECRET = "TELEGRAM_BOT_TOKEN";

/** Command registry — same five-command conformance registry as the kit base. */
export const POLLING_REGISTRY = [
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

export interface PollingEngineDeps {
	wire: FakeTelegramServer;
	secretReader: (name: string) => string | undefined;
	clock?: PollingClock | undefined;
	timer?: TimerSeam | undefined;
	spawner?: TaskSpawner | undefined;
	longPollTimeoutMs?: number | undefined;
	scalarMaxUnits?: number | undefined;
	manifestName?: string | undefined;
	logger?: ConstructorParameters<typeof BasePlatformAdapter>[0]["logger"];
}

/**
 * Test-observation seam installed BETWEEN the two halves of the outage
 * window: runs AFTER the server-side offset commit, BEFORE any dispatch/
 * enqueue. The contract test kills the connection here to prove the held-
 * inbound redispatch covers exactly this window.
 */
export interface PollingHooks {
	afterCommitBeforeDispatch?: () => Promise<void> | void;
}

interface EngineOptions {
	dropPendingUpdates: boolean;
}

export class PollingAdapterCore
	extends BasePlatformAdapter
	implements StreamEgressAdapter
{
	readonly tg: FakeTelegramServer;
	readonly clock: PollingClock;

	private readonly cp: EgressChokepoint;
	private readonly timer: TimerSeam;
	private readonly spawn: TaskSpawner;
	private readonly longPollTimeoutMs: number;
	private readonly secretReader: (name: string) => string | undefined;

	// ── lifecycle state (§3.1) ────────────────────────────────────────────
	connected = false;
	teardownStarted = false;
	generation = 0;
	conflictCount = 0;
	committedOffset = 0;
	/** Observable recovery-ladder feedings ("what fed the reconnect ladder"). */
	readonly recoveryLog: string[] = [];
	/** Sizes of completed held-inbound drains, in order. */
	readonly redispatchLog: number[] = [];
	/** Conflict-recovery restarts that carried drop_pending_updates=true. */
	recoveryRestartsWithDropPending = 0;
	polledOnce = false;

	hooks: PollingHooks | undefined;

	private heldInbound: IncomingEvent[] = [];
	private readonly heldIdentity = new Set<IncomingEvent>();
	private drainInFlight = false;
	private pendingFollowUpDrain = false;

	private currentPollTask: GatewayTask | null = null;
	private sessionToken: number | null = null;
	private heartbeatCanceller: (() => void) | null = null;
	private probing = false;
	private notRunningStreak = 0;
	private stuckPendingStreak = 0;

	/** Test/diagnostic view of the stuck-probe streak (escalation anchors). */
	get stuckProbeStreakForTests(): number {
		return this.stuckPendingStreak;
	}
	private typingRefreshers = new Map<string, () => void>();
	private typingStatus = new Map<string, string>();

	// ── interactive surfaces (§9; DEC-016) ────────────────────────────────
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

	/** Optional e2e turn driver: replaces the scripted echo handler body. */
	turnDriver:
		| ((event: IncomingEvent, ctxText: string) => Promise<string | null>)
		| null = null;

	constructor(deps: PollingEngineDeps) {
		super({
			manifestName: deps.manifestName ?? "telegram-polling",
			capabilities: {},
			lengthUnit: "utf16", // Bot API counts UTF-16 code units (04 §6.3)
			scalarMaxUnits: deps.scalarMaxUnits ?? 64,
			...(deps.logger !== undefined ? { logger: deps.logger } : {}),
		});
		const real = realPollingClock();
		this.clock = deps.clock ?? real;
		this.timer = deps.timer ?? real.timer;
		this.spawn = deps.spawner ?? immediateSpawner();
		this.longPollTimeoutMs = deps.longPollTimeoutMs ?? 25_000;
		this.secretReader = deps.secretReader;
		this.tg = deps.wire;

		// §11 step 3/4 + §8 identity rows: missing required secret ⇒ LOUD
		// disable at construction, never silent skip.
		const enablement = resolveEnablement(
			{
				name: this.manifestName,
				description: "polling reference adapter (Telegram-like)",
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
				this.editTransmit(chatId, messageId, content),
			transmitSeal: async (_k, chatId, draftId, content, metadata) => {
				if (metadata["forceSealFailure"] === true) {
					return { success: false, error: "forced seal failure" };
				}
				// The sealed stream IS the message: final=true records a SEAL op.
				return this.wireTransmitDraftFinal({
					chatId,
					draftId,
					content,
					metadata,
				});
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

	// ── conformance-subject plumbing shared across adapters ───────────────

	private readonly isMessageChats: ReadonlySet<string> = new Set(["__none__"]);
	markStreamIsMessage(chatId: string): void {
		(this.isMessageChats as Set<string>).add(chatId);
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

	// ══════════════════════════════════════════════════════════════════════
	// §3.1 transport lifecycle — connect/disconnect/generations
	// ══════════════════════════════════════════════════════════════════════

	/**
	 * adapter.py::connect parity: is_reconnect=True PRESERVES the server-side
	 * update queue (drop_pending_updates=false); cold boot MAY drop stale
	 * ones. _mark_connected drains the held-inbound queue.
	 */
	async connect(opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		this.teardownStarted = false;
		if (this.sessionToken === null) {
			this.sessionToken = this.tg.openSession();
		}
		this.connected = true; // _mark_connected
		this.startGeneration({ dropPendingUpdates: !opts.isReconnect });
		this.ensureHeartbeat();
		void this.scheduleHeldRedispatch();
		return true;
	}

	async disconnect(): Promise<void> {
		this.connected = false;
		this.generation += 1; // running loops exit at their next check
		this.stopHeartbeat();
		this.notRunningStreak = 0;
		this.stuckPendingStreak = 0;
	}

	/** Simulate a mid-cycle kill right after the server-side offset commit. */
	simulateCrashMidCycle(): void {
		this.connected = false;
		this.generation += 1;
		this.stopHeartbeat();
	}

	startGeneration(opts: EngineOptions): void {
		const gen = ++this.generation;
		const task = this.spawn((self) =>
			this.pollLoop(gen, opts.dropPendingUpdates, self),
		);
		this.currentPollTask = normalizeTask(task);
		if (this.currentPollTask !== null) {
			this.currentPollTaskOwnerGeneration = gen;
		}
	}

	/** updater.running parity — a live poll task for the CURRENT generation. */
	get updaterRunning(): boolean {
		return (
			this.connected &&
			this.currentPollTask !== null &&
			!this.currentPollTask.isDone() &&
			this.currentPollTaskOwnerGeneration === this.generation
		);
	}
	private currentPollTaskOwnerGeneration = -1;

	/** The session token this engine polls with (diagnostics/fixtures). */
	get activeSessionToken(): number | null {
		return this.sessionToken;
	}

	// ══════════════════════════════════════════════════════════════════════
	// The poll loop — fetch → COMMIT → enqueue-or-hold
	// ══════════════════════════════════════════════════════════════════════

	private async pollLoop(
		generation: number,
		dropPendingUpdates: boolean,
		task: GatewayTask,
	): Promise<void> {
		this.currentPollTaskOwnerGeneration =
			generation > this.currentPollTaskOwnerGeneration ||
			this.currentPollTask === task
				? generation
				: this.currentPollTaskOwnerGeneration;
		try {
			while (
				!this.teardownStarted &&
				this.connected &&
				generation === this.generation &&
				!task.cancelRequested()
			) {
				if (this.sessionToken === null) {
					this.sessionToken = this.tg.openSession();
				}

				let updates: FakeUpdate[];
				try {
					const batch = await this.tg.getUpdates({
						sessionToken: this.sessionToken,
						offset: this.committedOffset + 1,
						timeoutMs: this.longPollTimeoutMs,
						...(dropPendingUpdates ? { dropPendingUpdates: true } : {}),
					});
					updates = batch.updates;
				} catch (err) {
					if (err instanceof AdapterDisabledError) return;
					if (!this.isTransportAlive(generation)) return;
					if (isConflict(err)) {
						await this.handlePollingConflict(generation);
						return; // a fresh generation owns polling from here
					}
					if (isNetwork(err)) {
						this.scheduleRecovery(`poll-network-error: ${brief(err)}`);
						return;
					}
					this.scheduleRecovery(`poll-unexpected: ${brief(err)}`);
					return;
				}
				dropPendingUpdates = false; // only a fresh generation's FIRST call carries it

				this.recordPollingProgress(generation);
				if (updates.length === 0) continue;

				// ACK-BEFORE-ENQUEUE: confirm server-side BEFORE any enqueue.
				const lastId = updates[updates.length - 1]?.updateId ?? 0;
				this.committedOffset = Math.max(this.committedOffset, lastId);
				this.tg.commitOffset(this.sessionToken, lastId + 1);

				if (this.hooks?.afterCommitBeforeDispatch !== undefined) {
					await this.hooks.afterCommitBeforeDispatch();
				}
				// Offsets are COMMITTED — these updates will never be redelivered.
				// A transport death inside the window must HOLD them (never skip).
				for (const update of updates) {
					const ev = toIncomingEvent(update);
					if (!this.isTransportAlive(generation)) {
						this.holdInbound(ev, "batch-interrupted");
						continue;
					}
					await this.dispatchOrHold(ev);
				}
			}
		} catch (err) {
			if (task.cancelRequested()) return;
			this.scheduleRecovery(`poll-loop-error: ${brief(err)}`);
		}
	}

	private isTransportAlive(generation: number): boolean {
		return (
			this.connected &&
			!this.teardownStarted &&
			generation === this.generation &&
			this.lifecycle.state !== "fatal"
		);
	}

	/** adapter.py:_record_polling_progress — health marker for THIS generation. */
	private recordPollingProgress(generation: number): void {
		if (generation !== this.generation) return; // stale generation progress ignored
		this.polledOnce = true;
		// A generation counts healthy only after recorded progress: the conflict
		// episode AND the recovery-attempt budget reset here, never on bare restart.
		this.conflictCount = 0;
		this.recoveryAttempts = 0;
	}

	/** The pipeline step pollLoop uses per update; also the fixture ingest path. */
	async ingestUpdate(update: FakeUpdate): Promise<void> {
		this.committedOffset = Math.max(this.committedOffset, update.updateId);
		this.tg.commitOffset(this.requireSessionToken(), update.updateId + 1);
		await this.dispatchOrHold(toIncomingEvent(update));
	}

	private requireSessionToken(): number {
		if (this.sessionToken === null) {
			this.sessionToken = this.tg.openSession();
		}
		return this.sessionToken;
	}

	private async dispatchOrHold(event: IncomingEvent): Promise<void> {
		if (!this.canDispatchNow()) {
			this.holdInbound(event, "disconnected");
			return;
		}
		try {
			await this.handleIngress(event, sessionKeyOf(event));
		} catch (err) {
			if (err instanceof AdapterDisabledError || !this.canDispatchNow()) {
				this.holdInbound(event, "disabled");
				return;
			}
			// Retryable dispatch failure: re-hold rather than destroy (#55971 —
			// the offset already advanced; destroying means permanent loss).
			this.holdInbound(event, "dispatch-failed");
		}
	}

	private canDispatchNow(): boolean {
		return (
			this.connected &&
			!this.teardownStarted &&
			this.lifecycle.state !== "fatal"
		);
	}

	// ══════════════════════════════════════════════════════════════════════
	// Held-inbound machinery — adapter.py hold/redispatch parity
	// ══════════════════════════════════════════════════════════════════════

	get heldInboundCount(): number {
		return this.heldInbound.length;
	}

	/** Test/diagnostic copy of the hold queue (cap/dedup observation). */
	heldInboundForTests(): readonly IncomingEvent[] {
		return [...this.heldInbound];
	}

	/** Direct hold-path exercise (identity-dedup contracts). */
	holdForTests(event: IncomingEvent): void {
		this.holdInbound(event, "for-tests");
	}

	private holdInbound(event: IncomingEvent, where: string): void {
		if (this.lifecycle.state === "fatal") {
			// Permanent fatal discards the queue EXPLICITLY (never silently).
			return;
		}
		if (this.heldIdentity.has(event)) return; // identity dedup
		while (this.heldInbound.length >= HELD_INBOUND_MAX) {
			const dropped = this.heldInbound.shift();
			if (dropped !== undefined) this.heldIdentity.delete(dropped);
		}
		this.heldInbound.push(event);
		this.heldIdentity.add(event);
		this.scheduleHeldRedispatch(where);
	}

	/**
	 * Drain trigger: _mark_connected after reconnect, any hold created while
	 * connected, or arrivals mid-drain. No-ops while disconnected/tearing
	 * down or after permanent fatal. Never stacks duplicate drains.
	 */
	private scheduleHeldRedispatch(_where = ""): boolean {
		if (this.lifecycle.state === "fatal") return false;
		if (this.teardownStarted || !this.connected) return false;
		if (this.heldInbound.length === 0) return false;
		if (this.drainInFlight) {
			this.pendingFollowUpDrain = true; // the running pass schedules follow-up
			return false;
		}
		this.drainInFlight = true;
		this.pendingFollowUpDrain = false;
		const drainTask = normalizeTask(
			this.spawn((self) => this.redispatchHeldInbound(self)),
		);
		if (drainTask !== null) void drainTask.result.catch(() => {});
		return true;
	}

	private async redispatchHeldInbound(task: GatewayTask): Promise<void> {
		// adapter.py parity (allow_followup_schedule): a FAILED pass must NOT
		// immediately reschedule — a poison event would tight-loop. The next
		// _mark_connected or connected-path hold drains the remainder.
		let allowFollowUpSchedule = true;
		try {
			// Take ownership atomically so concurrent holds append to a fresh
			// list and are picked up by a follow-up schedule.
			const events = this.heldInbound;
			this.heldInbound = [];
			for (const ev of events) this.heldIdentity.delete(ev);

			let dispatched = 0;
			for (let idx = 0; idx < events.length; idx++) {
				if (!this.canDispatchNow() || task.cancelRequested()) {
					// Disconnect/fatal mid-drain — re-hold current + remainder.
					for (const rest of events.slice(idx)) {
						this.holdInbound(rest, "redispatch-interrupted");
					}
					break;
				}
				const event = events[idx] as IncomingEvent;
				try {
					await this.handleIngress(event, sessionKeyOf(event));
					dispatched += 1;
				} catch {
					this.holdInbound(event, "redispatch-failed");
					for (const rest of events.slice(idx + 1)) {
						this.holdInbound(rest, "redispatch-interrupted");
					}
					allowFollowUpSchedule = false;
					break;
				}
			}
			this.redispatchLog.push(dispatched);
		} finally {
			this.drainInFlight = false;
			// Events arrived mid-drain while still connected need another pass —
			// NEVER after a failed pass (poison-event tight-loop guard).
			if (
				allowFollowUpSchedule &&
				(this.pendingFollowUpDrain || this.heldInbound.length > 0)
			) {
				this.pendingFollowUpDrain = false;
				this.scheduleHeldRedispatch("follow-up");
			}
		}
	}

	private discardHeldExplicitly(): number {
		const n = this.heldInbound.length;
		this.heldInbound = [];
		for (const ev of [...this.heldIdentity]) this.heldIdentity.delete(ev);
		return n;
	}

	// ══════════════════════════════════════════════════════════════════════
	// 409-conflict ladder — adapter.py:_handle_polling_conflict parity
	// ══════════════════════════════════════════════════════════════════════

	private async handlePollingConflict(
		_failedGeneration: number,
	): Promise<void> {
		if (this.teardownStarted) return;
		if (this.lifecycle.state === "fatal") {
			return;
		}
		this.conflictCount += 1;

		if (this.conflictCount <= MAX_CONFLICT_RETRIES) {
			this.recoveryLog.push(
				`conflict-retry-${this.conflictCount}/${MAX_CONFLICT_RETRIES}`,
			);
			// Wait long enough for the rival server-side session to expire
			// (RETRY_DELAY grows with each attempt) — virtual under the injected
			// clock, wall-safe by construction in tests.
			await this.clock.sleep(conflictRetryDelayMs(this.conflictCount));
			if (!this.lifecycle.isActive) return; // fatal/disabled while suspended
			if (this.teardownStarted || !this.connected) return;

			// FRESH generation owns the poll stream; drop_pending_updates=True
			// is the ONLY way to terminate the stale server-side session
			// (#75017) — without it each restart is immediately 409'd by the
			// previous one.
			const gen = ++this.generation;
			this.recoveryRestartsWithDropPending += 1;
			const task = this.spawn((self) => this.pollLoop(gen, true, self));
			this.currentPollTask = normalizeTask(task);
			this.currentPollTaskOwnerGeneration = gen;
			return;
		}

		// Exhausted all retries — declare FATAL so the runner surfaces it.
		this.recoveryLog.push(
			`conflict-exhausted-after-${MAX_CONFLICT_RETRIES}-retries`,
		);
		this.lifecycle.markFatal({
			kind: "config_invalid",
			detail:
				`polling could not recover after ${MAX_CONFLICT_RETRIES} conflict retries ` +
				"— another consumer still holds this bot token's poll stream",
		});
		const discarded = this.discardHeldExplicitly();
		void discarded;
	}

	// ══════════════════════════════════════════════════════════════════════
	// Heartbeat — adapter.py:_polling_heartbeat / _probe_pending_updates
	// ══════════════════════════════════════════════════════════════════════

	private ensureHeartbeat(): void {
		if (this.heartbeatCanceller !== null) return;
		this.scheduleHeartbeatTick();
	}

	private stopHeartbeat(): void {
		// Cancels cadence ONLY — never resets the escalation streaks here:
		// every tick reschedules through this path and the consecutive-probe
		// counters must SURVIVE between probes.
		this.heartbeatCanceller?.();
		this.heartbeatCanceller = null;
	}

	private scheduleHeartbeatTick(): void {
		this.stopHeartbeat();
		this.heartbeatCanceller = this.timer(HEARTBEAT_INTERVAL_MS, () => {
			void this.heartbeatTick();
		});
	}

	private async heartbeatTick(): Promise<void> {
		if (!this.connected || this.teardownStarted || this.probing) {
			if (!this.connected || this.teardownStarted) return;
			this.scheduleHeartbeatTick();
			return;
		}
		this.probing = true;
		try {
			if (this.lifecycle.state !== "fatal") {
				try {
					// General-path probe (get_me): healthy send path does NOT prove
					// a live getUpdates consumer — the pending-count probe below
					// exposes the wedged long-poll (#42909).
					await this.withProbeTimeout(this.tg.getMe());
					await this.probePendingUpdates();
				} catch (err) {
					// Probe failure feeds the SAME reconnect ladder as a network
					// failure (§3.1 escalation).
					this.notRunningStreak = 0;
					this.stuckPendingStreak = 0;
					this.scheduleRecovery(`heartbeat-probe: ${brief(err)}`);
				}
			}
		} finally {
			this.probing = false;
			if (this.connected && !this.teardownStarted) {
				this.scheduleHeartbeatTick();
			} else {
				this.heartbeatCanceller = null;
			}
		}
	}

	/**
	 * adapter.py:_probe_pending_updates: pending_update_count growing/stuck
	 * while we believe we're polling ⇒ wedged consumer; escalate after TWO
	 * consecutive stuck probes (a single in-flight update must not trip it).
	 * Updater-not-running ×2 escalates through the same ladder (#55769).
	 */
	private async probePendingUpdates(): Promise<void> {
		if (!this.updaterRunning) {
			this.stuckPendingStreak = 0;
			this.notRunningStreak += 1;
			if (this.notRunningStreak >= 2) {
				this.notRunningStreak = 0;
				this.scheduleRecovery("heartbeat-updater-not-running");
			}
			return;
		}
		this.notRunningStreak = 0;
		const info = await this.withProbeTimeout(this.tg.getWebhookInfo());
		// A growing/stuck queue while we believe we're polling means the
		// consumer is dead (#42909). TWO consecutive stuck probes escalate —
		// a single in-flight update never trips recovery.
		if (info.pending_update_count > 0) {
			this.stuckPendingStreak += 1;
			if (this.stuckPendingStreak >= 2) {
				this.stuckPendingStreak = 0;
				this.scheduleRecovery("heartbeat-stuck-pending");
			}
			return;
		}
		this.stuckPendingStreak = 0;
	}

	/** Probe timeout race on the injected timer seam (PROBE_TIMEOUT parity). */
	private withProbeTimeout<T>(p: Promise<T>): Promise<T> {
		return new Promise<T>((resolve, reject) => {
			const settled = { done: false };
			const cancelTimer = this.timer(PROBE_TIMEOUT_MS, () => {
				if (settled.done) return;
				settled.done = true;
				reject(new Error("probe timed out"));
			});
			p.then(
				(value) => {
					if (settled.done) return;
					settled.done = true;
					cancelTimer();
					resolve(value);
				},
				(err) => {
					if (settled.done) return;
					settled.done = true;
					cancelTimer();
					reject(err instanceof Error ? err : new Error(String(err)));
				},
			);
		});
	}

	/**
	 * THE reconnect ladder every escalation feeds: a FRESH generation that
	 * PRESERVES the server-side queue (is_reconnect=true parity).
	 *
	 * Bounded like Hermes' ladder: single-flight (stacked escalations never
	 * spawn duplicate ladders), growing RETRY_DELAY per attempt, exhaustion ⇒
	 * FATAL so the gateway runner surfaces it (adapter.py escalates instead of
	 * looping silently). Recorded progress resets the attempt budget.
	 */
	private recovering = false;
	private recoveryAttempts = 0;

	scheduleRecovery(reason: string): void {
		if (!this.connected || this.lifecycle.state === "fatal") return;
		if (this.recovering) return; // single-flight: no stacked duplicate ladders
		this.recovering = true;
		this.recoveryLog.push(reason);
		const task = this.spawn(async (self: GatewayTask) => {
			try {
				while (
					this.connected &&
					!this.teardownStarted &&
					this.lifecycle.isActive &&
					!self.cancelRequested()
				) {
					this.recoveryAttempts += 1;
					if (this.recoveryAttempts > MAX_CONFLICT_RETRIES) {
						this.recoveryLog.push(
							`recovery-exhausted-after-${MAX_CONFLICT_RETRIES}-attempts`,
						);
						this.lifecycle.markFatal({
							kind: "config_invalid",
							detail:
								`transport unreachable after ${MAX_CONFLICT_RETRIES} recovery attempts ` +
								"— handing off to the gateway reconnector",
						});
						this.discardHeldExplicitly();
						return;
					}
					// Growing delay before the fresh generation takes the poll.
					await this.clock.sleep(conflictRetryDelayMs(this.recoveryAttempts));
					if (!this.connected || this.teardownStarted) return;
					if (!this.lifecycle.isActive) return;
					const gen = ++this.generation;
					const spawned = this.spawn((s) => this.pollLoop(gen, false, s));
					this.currentPollTask = normalizeTask(spawned);
					if (this.currentPollTask !== null) {
						this.currentPollTaskOwnerGeneration = gen;
					}
					return; // the fresh generation owns polling; ladder disarms
				}
			} finally {
				this.recovering = false;
			}
		});
		void task;
	}

	// ══════════════════════════════════════════════════════════════════════
	// Typing indicator — FloodWait honored at the typing site
	// ══════════════════════════════════════════════════════════════════════

	/**
	 * Send-site parity of the Telegram FloodWait extraction: the authoritative
	 * server retry_after is honored ONCE over any local schedule, via the
	 * INJECTED clock (never wall sleeps in contracts).
	 */
	async sendTyping(chatId: string, action = "typing"): Promise<SendResult> {
		let honoredOnce = false;
		for (;;) {
			const res = await this.tg.sendChatAction(chatId, action);
			if (res.success) return res;
			const ra =
				res.retryAfter !== undefined && res.retryAfter !== null
					? res.retryAfter
					: extractRetryAfterSeconds(res.error);
			if (ra !== null && !honoredOnce) {
				honoredOnce = true;
				await this.clock.sleep(ra * 1000);
				continue;
			}
			return res;
		}
	}

	/** Start the ~2 s refresh loop for a chat's dynamic status text. */
	startTypingRefresh(chatId: string, statusText?: string): void {
		if (statusText !== undefined) this.typingStatus.set(chatId, statusText);
		if (this.typingRefreshers.has(chatId)) return;
		// Arm the CADENCE SYNCHRONOUSLY (before any awaited beat) so the loop
		// exists the moment this call returns; the first beat fires alongside.
		const schedule = (): void => {
			const cancel = this.timer(TYPING_REFRESH_MS, () => {
				if (!this.typingRefreshers.has(chatId)) return;
				beat();
			});
			this.typingRefreshers.set(chatId, cancel);
		};
		const beat = (): void => {
			void this.sendTyping(chatId).then(() => {
				if (this.typingRefreshers.has(chatId)) schedule();
			});
		};
		schedule();
		beat();
	}

	stopTypingRefresh(chatId: string): void {
		const cancel = this.typingRefreshers.get(chatId);
		if (cancel !== undefined) {
			cancel();
			this.typingRefreshers.delete(chatId);
		}
	}

	statusTextFor(chatId: string): string | undefined {
		return this.typingStatus.get(chatId);
	}

	// ══════════════════════════════════════════════════════════════════════
	// Guard wiring + egress doors
	// ══════════════════════════════════════════════════════════════════════

	attachStandardGuard(spawner?: TaskSpawner): void {
		this.attachGuard(
			{
				registry: POLLING_REGISTRY,
				messageHandler: async (event, ctx) => {
					const text = event.text ?? `[${String(event.messageType)}]`;
					// Lane C clarify intercept resolves BEFORE any turn work.
					const sessionKey = String(
						event.metadata?.["gateway_session_key"] ?? "",
					);
					if (this.clarifyArmedSet.has(sessionKey) && !text.startsWith("/")) {
						this.clarifyCaptures.push(text);
						return null; // consumed by the clarify resolver
					}
					this.turnLog.push(text);
					if (this.turnDriver !== null) {
						return this.turnDriver(event, text);
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
		const senderId = String(event.source?.userId ?? "");
		if (senderId === "bot-self") return; // self/echo filter (§8 ingress row)
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	/**
	 * Formatting ladder probe lanes (§10.1 rows): transient rich failure is
	 * NEVER legacy-resent; parse failure falls back to the §6.1 plain body.
	 */
	async transientRichOutcome(
		chatId: string,
		content: string,
	): Promise<SendResult> {
		const { FormattingLadder } = await import("../kit/formatting-ladder.js");
		const ladder = new FormattingLadder({
			tryRich: async () => ({ success: false, error: "socket hang up" }),
			sendConverted: async (_c, md) =>
				this.wireTransmitSend(chatId, "SHOULD-NOT-HAPPEN", md),
			sendPlain: async (_c, md) =>
				this.wireTransmitSend(chatId, "SHOULD-NOT-HAPPEN", md),
		});
		return ladder.sendText(content, {});
	}

	async parseFailureResendContent(
		chatId: string,
		content: string,
	): Promise<string> {
		await this.deliverText(chatId, content, { forceFormattingError: true });
		// The plain-resend content is the LAST send op on that chat's wire lane;
		// the subject binds a reader against the shared harness wire.
		return this.lastSendContentReader(chatId);
	}

	/** Bound by the subject: reads the latest captured send for a chat. */
	lastSendContentReader: (chatId: string) => string = () => "";

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	doorAudit() {
		return this.cp.audit;
	}

	/**
	 * Per-chat length pair (§6.3/A15): chats whose id names "utf16" front a
	 * Bot-API chat — budget 30 CODE UNITS. Budget AND unit move together.
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
		return this.wireTransmitSend(chatId, content, metadata);
	}

	/** Plugged to the harness wire at subject level. */
	wireTransmitSend: (
		chatId: string,
		content: string,
		metadata: Metadata,
	) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });

	/** Plugged to the harness wire at subject level. */
	wireTransmitDraft: (args: DraftFrameArgs) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });

	/** Seal lane (final=true) — bound by the subject alongside the draft lane. */
	wireTransmitDraftFinal: (args: DraftFrameArgs) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });

	protected override async wireDraft(
		args: DraftFrameArgs,
	): Promise<SendResult> {
		return this.wireTransmitDraft(args);
	}

	protected override wireRich(
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		// Rich endpoint ABSENT unless the wire scripts an answer — a scripted
		// capability-error shape drives the §10.1 downgrade-latch path.
		if (!this.richScriptedProbe()) {
			return Promise.resolve({
				success: false,
				error: "sendRichMessage: method not found",
			});
		}
		return this.wireTransmitRich(content, metadata);
	}

	/** Bound by the subject: does the harness wire carry a "rich" script? */
	richScriptedProbe: () => boolean = () => false;
	wireTransmitRich: (
		content: string,
		metadata: Metadata,
	) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });

	/**
	 * Edit-site FloodWait parity: edits NEVER block the caller — the wait is
	 * surfaced as `error="flood_control:<wait>"` with retry_after set.
	 */
	protected override async wireEdit(
		chatId: string,
		messageId: string,
		content: string,
		_opts: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		const res = await this.editTransmit(chatId, messageId, content);
		if (!res.success) {
			const ra =
				res.retryAfter !== undefined && res.retryAfter !== null
					? res.retryAfter
					: extractRetryAfterSeconds(res.error);
			if (ra !== null) {
				return { success: false, error: `flood_control:${ra}`, retryAfter: ra };
			}
		}
		return res;
	}

	editTransmit: (
		chatId: string,
		messageId: string,
		content: string,
	) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });

	override supportsDraftStreaming(chatType?: string | undefined): boolean {
		return chatType === undefined || chatType === "dm"; // Bot API drafts, DM-only
	}

	/** Relay lanes arm seal-interception via one emitted draft frame. */
	async armNativeStream(chatId: string, draftId: number): Promise<void> {
		await this.sendDraft({ chatId, draftId, content: "" });
	}

	// ══════════════════════════════════════════════════════════════════════
	// Identity probes (token lock; missing-secret sibling)
	// ══════════════════════════════════════════════════════════════════════

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

	buildMissingSecretSibling(): PollingAdapterCore {
		return new PollingAdapterCore({
			wire: this.tg,
			manifestName: `${this.manifestName}-no-secret`,
			secretReader: () => undefined,
		});
	}

	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.lifecycle.statusSnapshot();
	}
}

// ── helpers ──────────────────────────────────────────────────────────────

function isConflict(err: unknown): boolean {
	return err instanceof Error && err.name === "TelegramConflictError";
}

function isNetwork(err: unknown): boolean {
	return err instanceof Error && err.name === "TelegramTransportError";
}

function brief(err: unknown): string {
	return String(err instanceof Error ? err.message : err).slice(0, 120);
}

function toIncomingEvent(update: FakeUpdate): IncomingEvent {
	return {
		messageId: String(update.updateId),
		messageType: "text",
		text: update.text,
		source: {
			platform: "telegram",
			chatType: "dm",
			userId: update.senderId,
			chatId: update.chatId,
		},
	};
}

/** Session-key derivation for engine-driven ingress (stable per chat). */
function sessionKeyOf(event: IncomingEvent): string {
	return `tg:${String(event.source?.chatId ?? "unknown")}`;
}

function normalizeTask(
	task: GatewayTask | null | undefined,
): GatewayTask | null {
	if (
		task === null ||
		task === undefined ||
		typeof task.isDone !== "function" ||
		typeof task.cancel !== "function"
	) {
		return null;
	}
	return task;
}

/** Re-export for subject wiring. */
export type { LockAcquisition };
