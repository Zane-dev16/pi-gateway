// pi_platforms/matrix/matrix-adapter — THE Matrix platform adapter on the
// long-poll sync transport family (DEC-002 polling column; roadmap Phase 6
// census port). Built on the pi_platforms kit + pi_gateway seams; the fake
// homeserver stands in for the vendor CS-API headlessly.
//
// SHAPE DELTAS realized here (vs the polling reference adapter):
//   - The cursor is an OPAQUE SYNC TOKEN (next_batch), committed once per
//     RESPONSE — not a numeric per-update offset. Commit order is Hermes-exact:
//     adapter.py:_sync_loop advances next_batch (put_next_batch) BEFORE
//     dispatching the batch; the family ack-before-enqueue hazard window is
//     covered by hold-and-redispatch, never drop (held queue owns committed
//     events across transport death).
//   - Redelivery shield: Matrix has NO client→server sync ack. A crash in the
//     fetch→commit window replays the same token on reconnect, so event-id
//     dedup (bounded deque of 1000, adapter.py:__init__ ~L1253) makes
//     downstream delivery exactly-once ("gap-free replay on reconnect" is the
//     homeserver's since-token contract).
//   - Auth classes: m_unknown_token from sync STOPS immediately with a LOUD
//     fatal (adapter.py:_sync_loop "permanent auth error … stopping"); an
//     M_UNKNOWN_SYNC_TOKEN epoch death recovers via full-state restarts that
//     abandon the stale incremental stream (the only escape), bounded by the
//     family ladder before fatal.
//   - Watchdog: each long-poll races a 45 s watchdog against its 30 s poll
//     window (adapter.py:_sync_loop asyncio.wait_for comment); TWO consecutive
//     stuck watchdogs feed the SAME recovery ladder.
//
// Family machinery reused per Phase-6 heuristic 2: held-inbound
// hold/redispatch, bounded recovery ladder, ManualPollingClock timer seams.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   plugins/platforms/matrix/adapter.py:_sync_loop            (loop/ladders)
//   plugins/platforms/matrix/adapter.py:connect               (whoami + initial sync)
//   plugins/platforms/matrix/adapter.py:_on_room_message      (filter chain)
//   plugins/platforms/matrix/adapter.py:_resolve_message_context
//     (mention gating / threads / reply fallback)
//   plugins/platforms/matrix/adapter.py:_is_self_sender       (#15763 parity)
//   plugins/platforms/matrix/adapter.py:_is_system_or_bridge_sender
//   plugins/platforms/matrix/adapter.py:_is_duplicate_event   (deque+set)
//   plugins/platforms/matrix/adapter.py:_extract_reply_fallback /
//     _strip_reply_fallback
//   plugins/platforms/matrix/adapter.py:_resolve_room_identity (A9 overlay)
//   plugins/platforms/matrix/adapter.py:send / _build_text_message_content
//   plugins/platforms/matrix/adapter.py:send_typing / stop_typing
//   plugins/platforms/matrix/adapter.py:on_processing_start /
//     on_processing_complete (👀 → ✅/❌ reaction-ack lifecycle)
//   gateway/platforms/base.py:supports_draft_streaming docstring (edit-based
//     streaming fallback realized as the native draft plane)

import type {
	DraftFrameArgs,
	EditOptions,
	Metadata,
	SendResult,
	StreamEgressAdapter,
} from "../../pi_gateway/streaming/adapter-seam.js";
import { EgressChokepoint } from "../../pi_gateway/streaming/egress-door.js";
import type {
	IncomingEvent,
	TaskSpawner,
	CommandRegistry,
} from "../../pi_gateway/guards/index.js";
import {
	BasePlatformAdapter,
	ActionHandlerRegistry,
	CallbackQueryRouter,
	ClarifyPendingStore,
	OneShotPendingStore,
	TokenLockManagerSeam,
	extractRetryAfterSeconds,
	classifySendError,
	sendWithRetry,
	plainTextFallbackBody,
	DELIVERY_FAILED_NOTICE,
	resolveEnablement,
} from "../kit/index.js";
import type { AdapterStatusSnapshot } from "../kit/lifecycle-state.js";
import type { LengthUnit } from "../kit/length-policy.js";
import type { GatewayTask } from "../../pi_gateway/guards/index.js";
import { immediateSpawner } from "../../pi_gateway/guards/index.js";
import { FormattingLadder } from "../kit/formatting-ladder.js";
import type { FormattingTransport } from "../kit/formatting-ladder.js";
import { chunkWithFenceCarry } from "../kit/chunking.js";

import type { PollingClock, TimerSeam } from "../polling/clock.js";
import { realPollingClock } from "../polling/clock.js";
import {
	MATRIX_EVENT_DEDUP_CAPACITY,
	MATRIX_MAX_MESSAGE_LENGTH_CEILING,
	MATRIX_MAX_MESSAGE_LENGTH_FLOOR,
	MATRIX_MAX_RECOVERY_ATTEMPTS,
	MATRIX_REACTION_EYES,
	MATRIX_REACTION_FAILURE,
	MATRIX_REACTION_SUCCESS,
	MATRIX_ROOM_IDENTITY_CACHE_MAX,
	MATRIX_ROOM_IDENTITY_TTL_MS,
	MATRIX_STARTUP_GRACE_SECONDS,
	MATRIX_SYNC_LONGPOLL_TIMEOUT_MS,
	MATRIX_SYNC_RETRY_DELAY_MS,
	MATRIX_SYNC_WATCHDOG_TIMEOUT_MS,
	buildTextMessageContent,
	normalizeBangCommand,
} from "./manifest.js";
import type {
	FakeMatrixHomeserver,
	MatrixSyncResponse,
	MatrixSyncResult,
	MatrixTimelineEvent,
} from "./matrix-fake-server.js";

/** adapter.py:HELD_INBOUND discipline shared across the polling family. */
export const HELD_INBOUND_MAX = 64;

const REQUIRED_SECRETS = ["MATRIX_HOMESERVER", "MATRIX_ACCESS_TOKEN"] as const;

/** Command registry — same five-command conformance registry as the kit base. */
export const MATRIX_REGISTRY: CommandRegistry = [
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

export interface MatrixRoomIdentity {
	roomId: string;
	roomName: string | null;
	roomTopic: string | null;
	canonicalAlias: string | null;
	serverName: string | null;
	joinedMemberCount: number | null;
	isDirectAccountData: boolean;
	displayName: string;
	hasExplicitName: boolean;
	chatType: "dm" | "room";
	conflict: boolean;
}

export interface MatrixAdapterDeps {
	hs: FakeMatrixHomeserver;
	clock?: PollingClock | undefined;
	timer?: TimerSeam | undefined;
	spawner?: TaskSpawner | undefined;
	manifestName?: string | undefined;
	scalarMaxUnits?: number | undefined;
	/**
	 * Long-poll window handed to /sync (adapter.py:_sync_loop timeout=30000).
	 * Tests pass a tiny wall budget so parked long-polls self-resolve fast —
	 * the polling family's LONG_POLL_TIMEOUT_MS pattern.
	 */
	syncLongPollTimeoutMs?: number | undefined;
	secretReader: (name: string) => string | undefined;
	/** adapter.py bang-command resolution predicate (gateway registry probe). */
	isKnownCommand?: ((name: string) => boolean) | undefined;
	/** adapter.py:MATRIX_IGNORED_USERS regexes. */
	ignoredUserPatterns?: readonly RegExp[] | undefined;
	allowedRooms?: ReadonlySet<string> | undefined;
	freeRooms?: ReadonlySet<string> | undefined;
	requireMention?: boolean | undefined;
	processNotices?: boolean | undefined;
	streamIsMessageChatIds?: ReadonlySet<string> | undefined;
}

interface EngineOptions {
	fullState: boolean;
}

/**
 * Test-observation seam installed BETWEEN token commit and dispatch — the
 * outage window the hold-and-redispatch machinery must cover. Receives the
 * committed response so scenarios can target NON-EMPTY batches.
 */
export interface MatrixHooks {
	afterCommitBeforeDispatch?: (
		response: MatrixSyncResponse,
	) => Promise<void> | void;
}

/** A23-style feature-gate latch for the edit-stream plane. */
class NativeStreamLatch {
	private latched = false;
	latchCount = 0;
	wireAttempts = 0;
	get unsupported(): boolean {
		return this.latched;
	}
	shouldSkipNative(): boolean {
		return this.latched;
	}
	maybeLatch(errorText: string): boolean {
		if (this.latched) return false;
		const s = errorText.toLowerCase();
		const gated =
			s.includes("forbidden") ||
			s.includes("permissions") ||
			s.includes("not allowed") ||
			s.includes("unauthorized");
		if (!gated) return false;
		this.latched = true;
		this.latchCount += 1;
		return true;
	}
}

interface OpenNativeStream {
	eventId: string;
	draftId: number;
	/** Cumulative RAW bytes already transmitted on this stream. */
	sent: string;
}

function isStuckWatchdog(err: unknown): boolean {
	return err instanceof Error && err.name === "MatrixSyncWatchdogError";
}
function isUnknownSyncToken(err: unknown): boolean {
	return err instanceof Error && err.name === "MatrixUnknownSyncTokenError";
}
function isTransport(err: unknown): boolean {
	return err instanceof Error && err.name === "MatrixTransportError";
}
function brief(err: unknown): string {
	return String(err instanceof Error ? err.message : err).slice(0, 120);
}

export class MatrixAdapterCore
	extends BasePlatformAdapter
	implements StreamEgressAdapter
{
	readonly hs: FakeMatrixHomeserver;
	readonly clock: PollingClock;

	private readonly cp: EgressChokepoint;
	private readonly timer: TimerSeam;
	private readonly spawn: TaskSpawner;
	private readonly syncLongPollTimeoutMs: number;
	private readonly secretReader: (name: string) => string | undefined;
	private readonly knownCommandProbe: (name: string) => boolean;
	private readonly ignoredUserPatterns: readonly RegExp[];
	private readonly allowedRooms: ReadonlySet<string>;
	private readonly freeRooms: ReadonlySet<string>;
	private readonly requireMention: boolean;
	private readonly processNoticesFlag: boolean;

	// ── lifecycle state ─────────────────────────────────────────────────────
	connected = false;
	generation = 0;
	committedSyncToken: string | null = null;
	startupTsMs = 0;
	polledOnce = false;
	recoveryAttempts = 0;
	recovering = false;
	/** Full-state recovery restarts (M_UNKNOWN_SYNC_TOKEN escapes). */
	recoveryRestartsWithFullState = 0;
	/** Recovery-ladder feedings ("what fed the ladder"). */
	readonly recoveryLog: string[] = [];
	readonly redispatchLog: number[] = [];
	hooks: MatrixHooks | undefined;

	ownUserId = "";
	/** Resolved device id (whoami / login response parity). */
	private deviceId: string | null = null;
	private heldInbound: IncomingEvent[] = [];
	private readonly heldIdentity = new Set<IncomingEvent>();
	private drainInFlight = false;
	private pendingFollowUpDrain = false;
	private syncTask: GatewayTask | null = null;
	private syncTaskOwnerGeneration = -1;
	private teardownStarted = false;
	private stuckWatchdogStreak = 0;
	private currentRaceCanceller: (() => void) | null = null;

	// ── intake dedup (deque+set parity) ──────────────────────────────────────
	private readonly processedEvents: string[] = [];
	private readonly processedSet = new Set<string>();

	// ── room identity cache (A9) ────────────────────────────────────────────
	private readonly roomIdentityCache = new Map<string, MatrixRoomIdentity>();
	private readonly roomIdentityCachedAt = new Map<string, number>();
	/** Threads the bot participated in (mention-gate bypass). */
	private readonly participatedThreads = new Set<string>();

	readonly nativeStreamLatch = new NativeStreamLatch();

	// ── interactive surfaces ────────────────────────────────────────────────
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

	turnDriver:
		| ((event: IncomingEvent, ctxText: string) => Promise<string | null>)
		| null = null;

	constructor(deps: MatrixAdapterDeps) {
		super({
			manifestName: deps.manifestName ?? "matrix",
			capabilities: { typedCommandPrefix: "/", splitsLongMessages: true },
			lengthUnit: "chars", // Python len() parity — code points (04 §6.3)
			scalarMaxUnits: deps.scalarMaxUnits ?? 16000,
		});
		this.hs = deps.hs;
		const real = realPollingClock();
		this.clock = deps.clock ?? real;
		this.timer = deps.timer ?? real.timer;
		this.spawn = deps.spawner ?? immediateSpawner();
		this.syncLongPollTimeoutMs =
			deps.syncLongPollTimeoutMs ?? MATRIX_SYNC_LONGPOLL_TIMEOUT_MS;
		this.secretReader = deps.secretReader;
		this.knownCommandProbe =
			deps.isKnownCommand ??
			((name) => MATRIX_REGISTRY.some((c) => c.name === name));
		this.ignoredUserPatterns = deps.ignoredUserPatterns ?? [];
		this.allowedRooms = deps.allowedRooms ?? new Set();
		this.freeRooms = deps.freeRooms ?? new Set();
		this.requireMention = deps.requireMention ?? true; // MATRIX_REQUIRE_MENTION default true
		this.processNoticesFlag = deps.processNotices ?? false;

		// §11 step 3: required secrets enablement — missing ⇒ LOUD disable.
		// adapter.py connect() accepts EITHER MATRIX_ACCESS_TOKEN (whoami
		// branch) OR MATRIX_USER_ID + MATRIX_PASSWORD (login branch) — a
		// password-configured deployment is fully credentialed, so the token
		// requirement is satisfied by that pair (matrix-7).
		const hasPasswordPair =
			this.secretReader("MATRIX_USER_ID") !== undefined &&
			this.secretReader("MATRIX_PASSWORD") !== undefined &&
			this.secretReader("MATRIX_PASSWORD") !== "";
		const enablementReader = hasPasswordPair
			? (key: string): string | undefined =>
					this.secretReader(key) ??
					(key === "MATRIX_ACCESS_TOKEN" ? "password-login" : undefined)
			: this.secretReader;
		const enablement = resolveEnablement(
			{
				name: this.manifestName,
				description: "matrix adapter on the sync long-poll family",
				transportShape: "polling",
				requiresEnv: REQUIRED_SECRETS.map((name) => ({ name })),
				capabilities: {},
			},
			enablementReader,
		);
		if (!enablement.enabled && enablement.reason) {
			this.lifecycle.disable(enablement.reason);
		} else {
			for (const spec of REQUIRED_SECRETS) {
				const value = this.secretReader(spec);
				if (value !== undefined) this.registerLogSecret(value);
			}
		}

		this.cp = new EgressChokepoint({
			streamIsMessageForChat: (chatId) =>
				(this.streamIsMessageChats as ReadonlySet<string>).has(String(chatId)),
			transmitSend: async (chatId, content, metadata) =>
				this.wireSend(chatId, content, metadata),
			transmitEdit: async (chatId, messageId, content, opts) =>
				this.wireEdit(chatId, messageId, content, opts),
			transmitSeal: async (_k, chatId, draftId, content, metadata) =>
				this.sealNativeStream(chatId, draftId, content, metadata),
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

	// ── conformance-subject plumbing ─────────────────────────────────────────

	private readonly streamIsMessageChats: ReadonlySet<string> = new Set([
		"__none__",
	]);
	markStreamIsMessage(chatId: string): void {
		(this.streamIsMessageChats as Set<string>).add(chatId);
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
	// Transport lifecycle — connect / disconnect / generations
	// ══════════════════════════════════════════════════════════════════════

	/**
	 * adapter.py::connect parity: whoami validates the token and resolves the
	 * bot mxid; a FULL-STATE initial sync seeds joined rooms + next_batch; the
	 * initial-sync batch runs through the SAME filter pipeline (startup grace
	 * drops backlog); then the incremental sync loop starts under a generation.
	 * `isReconnect` preserves the server-side queue (resume from the committed
	 * token) — only epoch recovery restarts full-state.
	 */
	async connect(opts: { isReconnect: boolean }): Promise<boolean> {
		this.throwIfDisabled();
		this.teardownStarted = false;
		const password = this.secretReader("MATRIX_PASSWORD");
		const configuredUserId = this.secretReader("MATRIX_USER_ID");
		if (password !== undefined && password !== "" && configuredUserId) {
			// adapter.py connect PASSWORD branch — client.login(identifier,
			// password, device_name, device_id); whoami is not required after.
			try {
				const resp = await this.hs.login({
					identifier: configuredUserId,
					password,
					deviceName: "pi-gateway",
					...(this.deviceId !== null ? { deviceId: this.deviceId } : {}),
				});
				this.ownUserId = resp.user_id;
				this.deviceId = resp.device_id;
			} catch (err) {
				this.logger?.error?.(
					`${this.manifestName}: login failed — ${brief(err)}`,
				);
				return false;
			}
		} else {
			// TOKEN branch: whoami validates the token and resolves the bot
			// mxid AND device_id (adapter.py parity).
			try {
				const me = await this.hs.whoami();
				this.ownUserId = me.user_id;
				this.deviceId = me.device_id;
			} catch (err) {
				this.logger?.error?.(
					`${this.manifestName}: whoami failed — ${brief(err)}`,
				);
				return false;
			}
		}
		if (!opts.isReconnect || this.committedSyncToken === null) {
			// adapter.py: client.sync(timeout=10000, full_state=True) BEFORE loop start.
			try {
				this.startupTsMs = this.clock.nowMs();
				const data = await this.hs.sync({
					since: null,
					timeoutMs: Math.min(10_000, this.syncLongPollTimeoutMs),
					fullState: true,
				});
				if (isSyncError(data)) return false;
				this.absorbJoinedRooms(data);
				this.committedSyncToken = data.next_batch;
				await this.dispatchBatch(data, { duringInitialSync: true });
			} catch (err) {
				this.logger?.warn?.(
					`${this.manifestName}: initial sync error — ${brief(err)}`,
				);
			}
		}
		this.connected = true;
		this.startGeneration({ fullState: false });
		void this.scheduleHeldRedispatch();
		return true;
	}

	async disconnect(): Promise<void> {
		this.connected = false;
		this.teardownStarted = true;
		this.generation += 1;
		this.currentRaceCanceller?.();
		this.currentRaceCanceller = null;
		// Connection teardown kills parked long-polls (transport class) — a
		// dead session must never receive pushed events.
		this.hs.closeSessions();
	}

	startGeneration(opts: EngineOptions): void {
		const gen = ++this.generation;
		this.syncTaskOwnerGeneration = gen;
		const task = this.spawn((self) => this.syncLoop(gen, opts.fullState, self));
		this.syncTask = normalizeTask(task);
	}

	get updaterRunning(): boolean {
		return (
			this.connected &&
			this.syncTask !== null &&
			!this.syncTask.isDone() &&
			this.syncTaskOwnerGeneration === this.generation
		);
	}

	// ══════════════════════════════════════════════════════════════════════
	// The sync loop — fetch → COMMIT TOKEN → dispatch-or-hold
	// ══════════════════════════════════════════════════════════════════════

	private async syncLoop(
		generation: number,
		fullState: boolean,
		task: GatewayTask,
	): Promise<void> {
		try {
			while (
				!this.teardownStarted &&
				this.connected &&
				generation === this.generation &&
				!task.cancelRequested()
			) {
				let result: MatrixSyncResult;
				try {
					result = await this.racedSync(fullState);
				} catch (err) {
					if (!this.isTransportAlive(generation)) return;
					if (isUnknownSyncToken(err)) {
						await this.handleEpochDeath(generation);
						return; // a fresh generation owns the stream from here
					}
					if (isStuckWatchdog(err)) {
						// TWO consecutive stuck watchdogs feed the recovery ladder —
						// a single timeout never trips it (polling probe parity).
						this.stuckWatchdogStreak += 1;
						if (this.stuckWatchdogStreak >= 2) {
							this.stuckWatchdogStreak = 0;
							this.scheduleRecovery("sync-watchdog-stuck-streak");
							return; // the ladder's fresh generation owns the stream
						}
						continue; // re-probe on the next iteration
					}
					if (isTransport(err)) {
						this.scheduleRecovery(`sync-network: ${brief(err)}`);
						return;
					}
					this.scheduleRecovery(`sync-unexpected: ${brief(err)}`);
					return;
				}
				fullState = false; // only a fresh generation's FIRST call is full-state

				if (isSyncError(result)) {
					if (result.message.toLowerCase().includes("m_unknown_token")) {
						// adapter.py:_sync_loop — permanent auth error stops IMMEDIATELY.
						this.recoveryLog.push("auth-unknown-token-stop");
						this.lifecycle.markFatal({
							kind: "config_invalid",
							detail: `permanent auth error from sync: ${result.message}`,
						});
						return;
					}
					if (!this.isTransportAlive(generation)) return;
					this.scheduleRecovery(`sync-error-object: ${brief(result.message)}`);
					return;
				}

				this.recordProgress(generation);
				// COMMIT BEFORE DISPATCH (Hermes order): the token advances now;
				// every event below is dispatched or HELD — never dropped.
				this.committedSyncToken = result.next_batch;
				if (this.hooks?.afterCommitBeforeDispatch !== undefined) {
					await this.hooks.afterCommitBeforeDispatch(result);
				}
				await this.dispatchBatch(result, { duringInitialSync: false });
			}
		} catch (err) {
			if (task.cancelRequested()) return;
			this.scheduleRecovery(`sync-loop-error: ${brief(err)}`);
		}
	}

	/** One long-poll raced against the 45 s watchdog (_sync_loop wait_for). */
	private racedSync(fullState: boolean): Promise<MatrixSyncResult> {
		return new Promise<MatrixSyncResult>((resolve, reject) => {
			const settled = { done: false };
			const cancelTimer = this.timer(MATRIX_SYNC_WATCHDOG_TIMEOUT_MS, () => {
				if (settled.done) return;
				settled.done = true;
				const err = new Error("sync long-poll exceeded the watchdog window");
				err.name = "MatrixSyncWatchdogError";
				reject(err);
			});
			this.currentRaceCanceller = () => {
				cancelTimer();
			};
			this.hs
				.sync({
					since: fullState ? null : this.committedSyncToken,
					timeoutMs: this.syncLongPollTimeoutMs,
					...(fullState ? { fullState: true } : {}),
				})
				.then(
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

	private isTransportAlive(generation: number): boolean {
		return (
			this.connected &&
			!this.teardownStarted &&
			generation === this.generation &&
			this.lifecycle.state !== "fatal"
		);
	}

	private recordProgress(generation: number): void {
		if (generation !== this.generation) return;
		if (this.stuckWatchdogStreak > 0)
			console.error(
				`DBG progress-reset streak=${this.stuckWatchdogStreak}`,
				new Error("trace").stack,
			);
		this.polledOnce = true;
		this.stuckWatchdogStreak = 0;
		this.recoveryAttempts = 0;
	}

	private absorbJoinedRooms(data: MatrixSyncResponse): void {
		for (const roomId of Object.keys(data.rooms.join)) {
			this.roomIdentityCache.delete(roomId); // state changed — identity re-resolves
			this.roomIdentityCachedAt.delete(roomId);
		}
	}

	/**
	 * adapter.py:_join_room_by_id / _schedule_invite_join — process sync
	 * INVITE memberships: join with BOUNDED retry so invited rooms deliver.
	 */
	private async handleRoomInvite(roomId: string): Promise<void> {
		const MAX_ATTEMPTS = 3;
		for (let attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
			try {
				await this.hs.joinRoom(roomId);
				this.logger?.warn?.(
					`${this.manifestName}: joined invited room ${roomId}`,
				);
				return;
			} catch (err) {
				this.logger?.warn?.(
					`${this.manifestName}: join ${roomId} failed (attempt ${attempt}/${MAX_ATTEMPTS}): ${brief(err)}`,
				);
				if (attempt < MAX_ATTEMPTS) {
					await this.clock.sleep(100 * attempt);
				}
			}
		}
	}

	/** Dispatch one sync response's events; hold when the transport is dead. */
	private async dispatchBatch(
		data: MatrixSyncResponse,
		opts: { duringInitialSync: boolean },
	): Promise<void> {
		// INVITE state first — startup reconciliation AND live invites ride
		// the same bounded-retry join path (dead invites simply exhaust).
		for (const roomId of Object.keys(data.rooms.invite ?? {})) {
			await this.handleRoomInvite(roomId);
		}
		const events: MatrixTimelineEvent[] = [];
		for (const roomEvents of Object.values(data.rooms.join)) {
			events.push(...roomEvents.timeline.events);
		}
		for (const evt of events) {
			if (
				!opts.duringInitialSync &&
				!this.isTransportAlive(this.syncTaskOwnerGeneration)
			) {
				// Committed-but-undeliverable: HOLD (never drop). The built event
				// bypasses re-filtering on drain — filters ran once, here.
				const built = await this.buildIncomingFromRoomEvent(evt);
				if (built !== null) this.holdInbound(built, "batch-interrupted");
				continue;
			}
			await this.dispatchOrHold(evt, opts.duringInitialSync);
		}
	}

	private async dispatchOrHold(
		evt: MatrixTimelineEvent,
		forceDispatch = false,
	): Promise<void> {
		const built = await this.buildIncomingFromRoomEvent(evt);
		if (built === null) return; // filtered upstream (never enqueued)
		this.inboundEventLog.push(built);
		if (!forceDispatch && !this.canDispatchNow()) {
			this.holdInbound(built, "disconnected");
			return;
		}
		// base.py:_process_message_background turn lifecycle — typing bubble up
		// (send_typing → set_typing timeout=30000), 👀 processing-start reaction
		// (on_processing_start), THEN the handler. DEC-052 excludes only the
		// room-mgmt/presence/history planes: typing and the emoji hooks are core
		// turn lifecycle (base.py:6418 runs them for every adapter).
		await this.sendTyping(evt.roomId);
		await this.onProcessingStart(evt.roomId, evt.eventId);
		let outcome: "success" | "failure" | "cancelled" = "success";
		try {
			await this.handleIngress(built, sessionKeyOf(built));
		} catch (err) {
			if (err instanceof Error && err.name === "AdapterDisabledError") {
				this.holdInbound(built, "disabled");
				// Deferred, not failed (CancelledError analog): eyes cleared,
				// NO final emoji, no read receipt — redispatch re-runs the turn.
				await this.onProcessingComplete(evt.roomId, evt.eventId, "cancelled");
				await this.stopTyping(evt.roomId);
				return;
			}
			this.holdInbound(built, "dispatch-failed");
			outcome = "failure";
		}
		// on_processing_complete — swap 👀 for ✅/❌; then stop_typing (timeout=0)
		// clears the bubble once the turn settles (base.py finalize ordering).
		await this.onProcessingComplete(evt.roomId, evt.eventId, outcome);
		await this.stopTyping(evt.roomId);
		// adapter.py:_background_read_receipt — mark the processed event read
		// (fully_read marker + m.read receipt), FIRE-AND-FORGET: failures are
		// swallowed and never block the turn.
		void this.hs
			.sendReadReceipt(evt.roomId, evt.eventId)
			.catch(() => undefined);
	}

	private canDispatchNow(): boolean {
		return (
			this.connected &&
			!this.teardownStarted &&
			this.lifecycle.state !== "fatal"
		);
	}

	// ── M_UNKNOWN_SYNC_TOKEN epoch recovery ─────────────────────────────────

	/**
	 * An unknown since-token means the incremental stream is DEAD (server
	 * rollback/restore). The ONLY escape is a fresh full-state generation that
	 * abandons the stale stream (drop-stale-backlog parity of the polling
	 * conflict row's drop_pending_updates=true). Unkillable churn exhausts to
	 * a loud FATAL.
	 */
	private async handleEpochDeath(_failedGeneration: number): Promise<void> {
		if (this.teardownStarted || !this.connected) return;
		if (this.lifecycle.state === "fatal") return;
		this.recoveryAttempts += 1;
		if (this.recoveryAttempts > MATRIX_MAX_RECOVERY_ATTEMPTS) {
			this.recoveryLog.push(
				`epoch-recovery-exhausted-after-${MATRIX_MAX_RECOVERY_ATTEMPTS}-attempts`,
			);
			this.lifecycle.markFatal({
				kind: "config_invalid",
				detail:
					"sync stream unrecoverable: server keeps rejecting since-tokens " +
					`(M_UNKNOWN_SYNC_TOKEN after ${MATRIX_MAX_RECOVERY_ATTEMPTS} full-state restarts)`,
			});
			return;
		}
		this.recoveryLog.push(
			`epoch-restart-${this.recoveryAttempts}/${MATRIX_MAX_RECOVERY_ATTEMPTS}`,
		);
		this.recoveryRestartsWithFullState += 1;
		await this.clock.sleep(MATRIX_SYNC_RETRY_DELAY_MS);
		if (!this.connected || this.teardownStarted) return;
		if (!this.lifecycle.isActive) return;
		this.committedSyncToken = null; // abandon the dead stream
		const gen = ++this.generation;
		this.syncTaskOwnerGeneration = gen;
		const spawned = this.spawn((self) => this.syncLoop(gen, true, self));
		this.syncTask = normalizeTask(spawned);
	}

	// ── bounded recovery ladder (transient failures + stuck watchdogs) ─────

	scheduleRecovery(reason: string): void {
		if (!this.connected || this.lifecycle.state === "fatal") return;
		if (this.recovering) return; // single-flight
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
					if (this.recoveryAttempts > MATRIX_MAX_RECOVERY_ATTEMPTS) {
						this.recoveryLog.push(
							`recovery-exhausted-after-${MATRIX_MAX_RECOVERY_ATTEMPTS}-attempts`,
						);
						this.lifecycle.markFatal({
							kind: "config_invalid",
							detail: `transport unreachable after ${MATRIX_MAX_RECOVERY_ATTEMPTS} recovery attempts`,
						});
						return;
					}
					// adapter.py:_sync_loop flat retry delay ("retrying in 5s").
					await this.clock.sleep(MATRIX_SYNC_RETRY_DELAY_MS);
					if (!this.connected || this.teardownStarted) return;
					if (!this.lifecycle.isActive) return;
					const gen = ++this.generation;
					this.syncTaskOwnerGeneration = gen;
					const spawned = this.spawn((s) =>
						this.syncLoop(gen, this.committedSyncToken === null, s),
					);
					this.syncTask = normalizeTask(spawned);
					return; // the fresh generation owns the stream; ladder disarms
				}
			} finally {
				this.recovering = false;
			}
		});
		void task;
	}

	/** Stuck-watchdog streak (test observability; TWO escalate). */
	get stuckProbeStreakForTests(): number {
		return this.stuckWatchdogStreak;
	}

	// ══════════════════════════════════════════════════════════════════════
	// Held-inbound machinery (family hold/redispatch parity)
	// ══════════════════════════════════════════════════════════════════════

	get heldInboundCount(): number {
		return this.heldInbound.length;
	}

	private holdInbound(event: IncomingEvent, _where: string): void {
		if (this.lifecycle.state === "fatal") return; // explicit discard on fatal
		if (this.heldIdentity.has(event)) return; // identity dedup
		while (this.heldInbound.length >= HELD_INBOUND_MAX) {
			const dropped = this.heldInbound.shift();
			if (dropped !== undefined) this.heldIdentity.delete(dropped);
		}
		this.heldInbound.push(event);
		this.heldIdentity.add(event);
		void this.scheduleHeldRedispatch();
	}

	private scheduleHeldRedispatch(): boolean {
		if (this.lifecycle.state === "fatal") return false;
		if (this.teardownStarted || !this.connected) return false;
		if (this.heldInbound.length === 0) return false;
		if (this.drainInFlight) {
			this.pendingFollowUpDrain = true;
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
		let allowFollowUpSchedule = true;
		try {
			const events = this.heldInbound;
			this.heldInbound = [];
			for (const ev of events) this.heldIdentity.delete(ev);

			let dispatched = 0;
			for (let idx = 0; idx < events.length; idx++) {
				if (!this.canDispatchNow() || task.cancelRequested()) {
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
			if (
				allowFollowUpSchedule &&
				(this.pendingFollowUpDrain || this.heldInbound.length > 0)
			) {
				this.pendingFollowUpDrain = false;
				this.scheduleHeldRedispatch();
			}
		}
	}

	// ══════════════════════════════════════════════════════════════════════
	// Intake filter chain — adapter.py:_on_room_message order
	// ══════════════════════════════════════════════════════════════════════

	/** adapter.py:_is_self_sender — case-insensitive; unresolved own ⇒ self. */
	isSelfSender(sender: string): boolean {
		const own = (this.ownUserId || "").trim().toLowerCase();
		if (!own) return true; // defensive: never echo-loop while unidentified
		return sender.trim().toLowerCase() === own;
	}

	/** adapter.py:_is_system_or_bridge_sender — appservice namespace rules. */
	static isSystemOrBridgeSender(sender: string): boolean {
		let s = (sender || "").trim();
		if (!s) return true;
		if (s.startsWith("@")) s = s.slice(1);
		const localpart = s.includes(":") ? (s.split(":")[0] ?? "") : s;
		if (!localpart) return true; // malformed / empty localpart
		return localpart.startsWith("_");
	}

	private matchesIgnoredUserPattern(sender: string): boolean {
		return this.ignoredUserPatterns.some((re) => re.test(sender));
	}

	/** adapter.py:_is_duplicate_event — bounded deque keeps newest 1000. */
	isDuplicateEvent(eventId: string): boolean {
		if (!eventId) return false;
		if (this.processedSet.has(eventId)) return true;
		if (this.processedEvents.length >= MATRIX_EVENT_DEDUP_CAPACITY) {
			const evicted = this.processedEvents.shift();
			if (evicted !== undefined) this.processedSet.delete(evicted);
		}
		this.processedEvents.push(eventId);
		this.processedSet.add(eventId);
		return false;
	}

	/** Oldest-first view of the dedup deque (test observability). */
	processedEventsForTests(): readonly string[] {
		return [...this.processedEvents];
	}

	/** Built IncomingEvents that were ENQUEUED for dispatch, in order. */
	readonly inboundEventLog: IncomingEvent[] = [];

	/**
	 * The full pipeline for one timeline event. Returns null when the event is
	 * filtered (never enqueued); otherwise the built IncomingEvent.
	 */
	async buildIncomingFromRoomEvent(
		evt: MatrixTimelineEvent,
	): Promise<IncomingEvent | null> {
		const sender = evt.sender;
		if (this.isSelfSender(sender)) return null; // self/echo (#15763)
		if (MatrixAdapterCore.isSystemOrBridgeSender(sender)) return null;
		if (this.matchesIgnoredUserPattern(sender)) return null;

		// Room allowlist — DM rooms exempt (adapter.py:_is_allowed_matrix_room_event).
		if (
			this.allowedRooms.size > 0 &&
			!this.allowedRooms.has(evt.roomId) &&
			!(await this.isDmRoom(evt.roomId))
		) {
			return null;
		}

		// Event-id dedup.
		if (this.isDuplicateEvent(evt.eventId)) return null;

		// Startup grace: initial-sync backlog older than startup−5s drops.
		if (
			evt.originServerTsMs > 0 &&
			evt.originServerTsMs <
				this.startupTsMs - MATRIX_STARTUP_GRACE_SECONDS * 1000
		) {
			return null;
		}

		if (evt.type === "m.reaction") return null; // inbound reactions: unwired kind, tolerated
		const content = evt.content;
		const msgtype = String(content["msgtype"] ?? "");
		const relatesTo = (content["m.relates_to"] ?? {}) as Record<
			string,
			unknown
		>;

		// Edits (m.replace) skip — the original already delivered.
		if (relatesTo["rel_type"] === "m.replace") return null;
		// m.notice skipped unless configured (bot-to-bot loop guard).
		if (msgtype === "m.notice" && !this.processNoticesFlag) return null;

		if (msgtype === "m.text" || msgtype === "m.notice") {
			return this.buildTextIncoming(evt, relatesTo);
		}
		// Media msgtypes (m.image/m.audio/m.video/m.file): tolerated, no turn
		// (inbound media download is a documented exclusion — DEC-052).
		return null;
	}

	private async buildTextIncoming(
		evt: MatrixTimelineEvent,
		relatesTo: Record<string, unknown>,
	): Promise<IncomingEvent | null> {
		let body = String(evt.content["body"] ?? "");
		if (!body) return null;

		// Bang-command normalization BEFORE context resolution
		// (adapter.py:_handle_text_message ordering).
		body = normalizeBangCommand(body, this.knownCommandProbe);

		const identity = await this.resolveRoomIdentity(evt.roomId);
		const chatType = identity.chatType;
		const threadIdRaw = relatesTo["event_id"];
		const threadId =
			relatesTo["rel_type"] === "m.thread" && typeof threadIdRaw === "string"
				? threadIdRaw
				: undefined;
		const inReplyTo = relatesTo["m.in_reply_to"] as
			| Record<string, unknown>
			| undefined;
		const replyTo =
			inReplyTo && typeof inReplyTo["event_id"] === "string"
				? (inReplyTo["event_id"] as string)
				: undefined;

		// Reply-fallback extraction BEFORE stripping — the gateway prompt layer
		// renders "[Replying to: …]" from BOTH the quoted text and its author.
		let replyToText: string | undefined;
		let extractedAuthor: string | undefined;
		if (replyTo !== undefined && body.startsWith("> ")) {
			const extracted = extractReplyFallback(body);
			body = stripReplyFallback(body);
			replyToText = extracted.text ?? undefined;
			extractedAuthor = extracted.authorId ?? undefined;
		}
		// Re-run normalization on the post-strip body (quoted !command parity).
		body = normalizeBangCommand(body, this.knownCommandProbe);

		// Mention gating — non-DM rooms only.
		const mentionsBlock = (evt.content["m.mentions"] ?? {}) as {
			user_ids?: unknown;
		};
		const mentionUserIds = Array.isArray(mentionsBlock.user_ids)
			? (mentionsBlock.user_ids as string[])
			: undefined;
		const isMentioned = this.isBotMentioned(body, evt.content, mentionUserIds);

		if (chatType !== "dm") {
			// Whitelist first — silent drop even when mentioned.
			if (this.allowedRooms.size > 0 && !this.allowedRooms.has(evt.roomId)) {
				return null;
			}
			const isFreeRoom = this.freeRooms.has(evt.roomId);
			const inBotThread =
				threadId !== undefined && this.participatedThreads.has(threadId);
			const isCommandBody = body.startsWith("/");
			if (
				this.requireMention &&
				!isFreeRoom &&
				!inBotThread &&
				!isMentioned &&
				!isCommandBody
			) {
				return null;
			}
		}
		// Strip mention tokens when gating is active (bare-localpart words kept).
		if (isMentioned && this.requireMention) {
			body = this.stripMentionTokens(body);
		}
		if (threadId !== undefined) this.participatedThreads.add(threadId);

		return {
			messageId: evt.eventId,
			messageType: "text",
			text: body,
			source: {
				platform: "matrix",
				chatType: chatType === "dm" ? "dm" : "channel",
				userId: evt.sender,
				chatId: evt.roomId,
				...(threadId !== undefined ? { threadId } : {}),
				chatName: identity.displayName,
			},
			metadata: {
				...(replyToText !== undefined ? { reply_to_text: replyToText } : {}),
				...(extractedAuthor !== undefined
					? { reply_to_author_id: extractedAuthor }
					: {}),
				matrix_origin_server_ts: evt.originServerTsMs,
			},
		};
	}

	/**
	 * adapter.py:_is_bot_mentioned — MSC3952 m.mentions.user_ids is
	 * authoritative; body mxid substring, localpart word-boundary regex and
	 * formatted_body matrix.to pills are the fallback signals.
	 */
	isBotMentioned(
		body: string,
		content: Record<string, unknown>,
		mentionUserIds: readonly string[] | undefined,
	): boolean {
		if (
			mentionUserIds !== undefined &&
			this.ownUserId &&
			(mentionUserIds as readonly string[]).includes(this.ownUserId)
		) {
			return true;
		}
		const formatted = content["formatted_body"];
		if (!body && typeof formatted !== "string") return false;
		if (this.ownUserId && body.includes(this.ownUserId)) return true;
		if (this.ownUserId && this.ownUserId.includes(":")) {
			const localpart = this.ownUserId.split(":")[0]?.replace(/^@/, "") ?? "";
			if (
				localpart &&
				new RegExp(`\\b${escapeRegExp(localpart)}\\b`, "i").exec(body) !== null
			) {
				return true;
			}
		}
		if (
			typeof formatted === "string" &&
			formatted.includes(`matrix.to/#/${this.ownUserId}`)
		) {
			return true;
		}
		return false;
	}

	/**
	 * adapter.py:_strip_mention — explicit MXID/@localpart tokens only; bare
	 * localpart words are NOT stripped ("Hermes Agent" stays whole).
	 */
	stripMentionTokens(body: string): string {
		if (!body) return "";
		let out = body;
		if (this.ownUserId) out = out.split(this.ownUserId).join("");
		if (this.ownUserId && this.ownUserId.includes(":")) {
			const localpart = this.ownUserId.split(":")[0]?.replace(/^@/, "") ?? "";
			if (localpart) {
				out = out.replace(
					new RegExp(`(?<![\\w])@${escapeRegExp(localpart)}\\b`, "gi"),
					"",
				);
			}
		}
		return out.replace(/[ \t]{2,}/g, " ").trim();
	}

	// ══════════════════════════════════════════════════════════════════════
	// Room identity — channel directory + alias overlay (A9)
	// ══════════════════════════════════════════════════════════════════════

	/**
	 * adapter.py:_resolve_room_identity — TTL cache; display_name resolves
	 * room_name → canonical_alias → room_id; DM classification: member_count ≤ 2
	 * PRIMARY, m.direct fallback without explicit names; explicitly named
	 * rooms WIN over stale m.direct (conflict flagged).
	 */
	async resolveRoomIdentity(roomId: string): Promise<MatrixRoomIdentity> {
		const cached = this.roomIdentityCache.get(roomId);
		const cachedAt = this.roomIdentityCachedAt.get(roomId) ?? 0;
		const fresh = this.clock.nowMs() - cachedAt <= MATRIX_ROOM_IDENTITY_TTL_MS;
		if (cached !== undefined && fresh) return cached;

		const [roomName, canonicalAlias, topic, memberCount, directMap] =
			await Promise.all([
				this.hs.getRoomName(roomId),
				this.hs.getRoomCanonicalAlias(roomId),
				this.hs.getRoomTopic(roomId),
				this.hs.getJoinedMemberCount(roomId),
				this.hs.getDirectAccountData(),
			]);
		const isDirectAccountData = Object.values(directMap).some((rooms) =>
			rooms.includes(roomId),
		);
		const hasExplicitName = Boolean(roomName);
		const likelyDm =
			(memberCount !== null && memberCount <= 2) ||
			(isDirectAccountData && !hasExplicitName);
		const conflict =
			isDirectAccountData &&
			hasExplicitName &&
			(memberCount === null || memberCount > 2);
		const identity: MatrixRoomIdentity = {
			roomId,
			roomName,
			canonicalAlias,
			roomTopic: topic,
			serverName: roomServerName(roomId),
			joinedMemberCount: memberCount,
			isDirectAccountData,
			displayName: roomName ?? canonicalAlias ?? roomId,
			hasExplicitName,
			chatType: likelyDm ? "dm" : "room",
			conflict,
		};
		if (this.roomIdentityCache.size >= MATRIX_ROOM_IDENTITY_CACHE_MAX) {
			let oldestKey: string | null = null;
			let oldestAt = Infinity;
			for (const [key, at] of this.roomIdentityCachedAt) {
				if (at < oldestAt) {
					oldestAt = at;
					oldestKey = key;
				}
			}
			if (oldestKey !== null) {
				this.roomIdentityCache.delete(oldestKey);
				this.roomIdentityCachedAt.delete(oldestKey);
			}
		}
		this.roomIdentityCache.set(roomId, identity);
		this.roomIdentityCachedAt.set(roomId, this.clock.nowMs());
		return identity;
	}

	async isDmRoom(roomId: string): Promise<boolean> {
		return (await this.resolveRoomIdentity(roomId)).chatType === "dm";
	}

	// ══════════════════════════════════════════════════════════════════════
	// Guard wiring + ingress lane
	// ══════════════════════════════════════════════════════════════════════

	attachStandardGuard(spawner?: TaskSpawner): void {
		this.attachGuard(
			{
				registry: MATRIX_REGISTRY,
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
		// Self/echo filter (§8 ingress row). The UNRESOLVED-identity defensive
		// drop lives on the TRANSPORT path only — the harness lane delivers
		// already-vetted platform events.
		if (
			(senderId !== "" &&
				this.ownUserId !== "" &&
				this.isSelfSender(senderId)) ||
			senderId === "bot-self" // harness echo-lane convention
		) {
			return;
		}
		event.metadata = {
			...(event.metadata ?? {}),
			gateway_session_key: sessionKey,
		};
		await this.handleIngress(event, sessionKey);
	}

	doorAudit() {
		return this.cp.audit;
	}

	/** Relay lanes arm seal-interception via one emitted draft frame. */
	async armNativeStream(chatId: string, draftId: number): Promise<void> {
		await this.sendDraft({ chatId, draftId, content: "" });
	}

	async transientRichOutcome(
		_chatId: string,
		content: string,
	): Promise<SendResult> {
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

	// ══════════════════════════════════════════════════════════════════════
	// Egress doors — send/edit/draft/seal over the CS-API shapes
	// ══════════════════════════════════════════════════════════════════════

	protected override get chokepoint(): EgressChokepoint {
		return this.cp;
	}

	override async deliverText(
		chatId: string,
		content: string,
		metadata: Metadata = {},
	): Promise<SendResult[]> {
		this.throwIfDisabled();
		// adapter.py:format_message strips image markdown BEFORE chunking.
		const formatted = stripImageMarkdown(content);
		const policy = this.chatLengthPolicyForChat(chatId);
		const plan = chunkWithFenceCarry(formatted, policy);
		const results: SendResult[] = [];
		for (const chunk of plan.chunks) {
			this.ladderChatId = chatId;
			results.push(await this.deliverWiredChunk(chatId, chunk, metadata));
		}
		return results;
	}

	private async deliverWiredChunk(
		chatId: string,
		chunk: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const outcome = await this.ensureFormatLadder().sendText(chunk, metadata);
		if (outcome.success) return outcome;
		if (outcome.tier === "rich") return outcome; // transient rich: NEVER resent

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
				(c: string, md: Metadata) => this.restSend(chatId, c, md),
				{ maxRetries: 2 },
			);
			if (retried.success) return retried;
			return this.restSend(chatId, DELIVERY_FAILED_NOTICE, metadata);
		}
		if (failureClass === "formatting") {
			return this.restSend(chatId, plainTextFallbackBody(chunk), metadata);
		}
		return outcome;
	}

	private formatLadder: FormattingLadder | null = null;
	private ladderChatId = "";

	private ensureFormatLadder(): FormattingLadder {
		if (this.formatLadder === null) {
			// Matrix has NO rich endpoint (base default capability error shape);
			// the ladder probes once, latches off, and plain sends carry the day.
			const transports: FormattingTransport = {
				tryRich: (content, metadata) => this.wireRich(content, metadata),
				sendConverted: (content, metadata) =>
					this.wireSend(this.ladderChatId, content, metadata),
				sendPlain: (content, metadata) =>
					this.wireSend(this.ladderChatId, content, metadata),
			};
			this.formatLadder = new FormattingLadder(transports, {
				log: (m, meta) => this.logger?.warn?.(m, meta),
			});
		}
		return this.formatLadder;
	}

	/**
	 * adapter.py:send — build the structured content (body + m.mentions) for
	 * each chunk; egress bytes ride the subject-bound wire lane.
	 */
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
		return this.restSend(chatId, content, metadata);
	}

	/** Bound by the subject to the shared harness wire (egress capture lane). */
	wireTransmitSend: (
		chatId: string,
		content: string,
		metadata: Metadata,
	) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });

	private restSend(
		chatId: string,
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const md0 = metadata as Record<string, unknown>;
		// THE FULL VENDOR EVENT CONTENT — the exact dict a real client PUTs to
		// /rooms/{id}/send/m.room.message. Mentions ride m.mentions INSIDE the
		// content (MSC3952); format/formatted_body ride when HTML rendering
		// differs (_build_text_message_content parity).
		const eventContent: Record<string, unknown> =
			buildTextMessageContent(content);
		// adapter.py:_apply_relation_metadata — reply_to → m.in_reply_to;
		// thread_id → rel_type m.thread + is_falling_back (+ reply fallback).
		const replyTo =
			typeof md0["reply_to_message_id"] === "string"
				? md0["reply_to_message_id"]
				: undefined;
		const threadId =
			typeof md0["thread_id"] === "string" ? md0["thread_id"] : undefined;
		const relatesTo: Record<string, unknown> = {};
		if (replyTo !== undefined) {
			relatesTo["m.in_reply_to"] = { event_id: replyTo };
		}
		if (threadId !== undefined && threadId !== "") {
			relatesTo["rel_type"] = "m.thread";
			relatesTo["event_id"] = threadId;
			relatesTo["is_falling_back"] = true;
			if (relatesTo["m.in_reply_to"] === undefined) {
				relatesTo["m.in_reply_to"] = { event_id: threadId };
			}
		}
		if (Object.keys(relatesTo).length > 0) {
			eventContent["m.relates_to"] = relatesTo;
		}
		const md: Record<string, unknown> = { ...metadata };
		delete md["reply_to_message_id"]; // converted into m.relates_to
		delete md["thread_id"]; // converted into m.relates_to
		md["event_content"] = eventContent;
		return this.wireTransmitSend(chatId, content, md as Metadata);
	}

	/**
	 * adapter.py:edit_message — edits are NEW events carrying an m.replace
	 * relation pointing at the target event id. They ride the EDIT wire lane
	 * (reconcile-by-edit observability), not the plain send lane.
	 */
	protected override async wireEdit(
		chatId: string,
		messageId: string,
		content: string,
		opts: EditOptions & { finalize: boolean },
	): Promise<SendResult> {
		void opts;
		// adapter.py:edit_message — an edit is a NEW event whose body is
		// "* "+text, carrying m.new_content (the FULL rebuilt content with
		// mentions copied + '* '-prefixed formatted_body) and m.relates_to
		// {rel_type:m.replace,event_id}. Vendor clients apply edits via
		// m.new_content; without it nothing is replaced.
		const newContent = buildTextMessageContent(content);
		const editContent: Record<string, unknown> = {
			msgtype: "m.text",
			body: `* ${content}`,
			"m.new_content": newContent,
			"m.relates_to": { rel_type: "m.replace", event_id: messageId },
		};
		if ("m.mentions" in newContent) {
			editContent["m.mentions"] = newContent["m.mentions"];
		}
		if ("formatted_body" in newContent) {
			editContent["format"] = "org.matrix.custom.html";
			editContent["formatted_body"] =
				`* ${String(newContent["formatted_body"])}`;
		}
		return this.wireTransmitEdit(chatId, messageId, content, {
			event_content: editContent,
		} as unknown as Metadata);
	}

	/** Bound by the subject to the shared harness wire's edit lane. */
	wireTransmitEdit: (
		chatId: string,
		messageId: string,
		content: string,
		metadata: Metadata,
	) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });

	/** Bound by the subject: does the harness wire carry a "rich" script? */
	richScriptedProbe: () => boolean = () => false;
	wireTransmitRich: (
		content: string,
		metadata: Metadata,
	) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });

	protected override async wireRich(
		content: string,
		metadata: Metadata,
	): Promise<SendResult> {
		// No native rich endpoint exists on the CS-API message plane; a
		// scripted capability-error shape still reaches the wire ONCE so the
		// §10.1 probe-once latch is observable.
		if (!this.richScriptedProbe()) {
			return Promise.resolve({ success: false, error: "method not found" });
		}
		return this.wireTransmitRich(content, metadata);
	}

	// ── native draft plane (edit-based streaming preview) ──────────────────

	private readonly openStreams = new Map<string, OpenNativeStream>();

	/**
	 * Edit-based native streaming (base.py:supports_draft_streaming fallback):
	 * START creates the preview post; APPEND transmits the cumulative RAW
	 * bytes (prefix-checked); SEAL finalizes. Feature-gate errors latch the
	 * plane OFF permanently (A23 family shape).
	 */
	protected override async wireDraft(
		args: DraftFrameArgs,
	): Promise<SendResult> {
		if (this.nativeStreamLatch.shouldSkipNative()) {
			return { success: false, error: "native streaming unsupported" };
		}
		this.nativeStreamLatch.wireAttempts += 1;

		const text = args.content;
		let stream = this.openStreams.get(args.chatId);
		if (stream !== undefined && stream.draftId !== args.draftId) {
			await this.sealNativeStream(args.chatId, stream.draftId, stream.sent, {});
			stream = undefined;
		}

		if (stream === undefined) {
			const started = await this.wireTransmitDraft(
				args.chatId,
				args.draftId,
				text,
				false,
				{
					stream_op: "start",
				},
			);
			if (!started.success) {
				this.nativeStreamLatch.maybeLatch(started.error ?? "");
				return started;
			}
			this.openStreams.set(args.chatId, {
				eventId: started.messageId ?? `stream-${args.draftId}`,
				draftId: args.draftId,
				sent: text,
			});
			return started;
		}

		if (text === stream.sent)
			return { success: true, messageId: stream.eventId };
		if (!text.startsWith(stream.sent)) {
			await this.sealNativeStream(args.chatId, stream.draftId, stream.sent, {});
			this.openStreams.delete(args.chatId);
			return { success: false, error: "stream prefix mismatch" };
		}
		// Edit-based previews transmit CUMULATIVE content (vendor-real patch
		// semantics — there is no append delta on an event edit).
		const appended = await this.wireTransmitDraft(
			args.chatId,
			args.draftId,
			text,
			false,
			{ stream_op: "append" },
		);
		if (!appended.success) {
			this.nativeStreamLatch.maybeLatch(appended.error ?? "");
			this.openStreams.delete(args.chatId);
			return appended;
		}
		stream.sent = text;
		return { success: true, messageId: stream.eventId };
	}

	wireTransmitDraft: (
		chatId: string,
		draftId: number,
		content: string,
		final: boolean,
		metadata: Metadata,
	) => Promise<SendResult> = () =>
		Promise.resolve({ success: false, error: "no wire bound" });

	private async sealNativeStream(
		chatId: string,
		draftId: number,
		finalText: string,
		metadata: Metadata,
	): Promise<SendResult> {
		const stream = this.openStreams.get(chatId);
		this.openStreams.delete(chatId);
		const sealed = await this.wireTransmitDraft(
			chatId,
			draftId,
			finalText,
			true,
			{ ...metadata },
		);
		if (!sealed.success) {
			this.nativeStreamLatch.maybeLatch(sealed.error ?? "");
			return sealed;
		}
		return {
			success: true,
			messageId: sealed.messageId ?? stream?.eventId ?? `sealed-${draftId}`,
		};
	}

	override supportsDraftStreaming(_chatType?: string | undefined): boolean {
		if (this.nativeStreamLatch.unsupported) return false;
		// Edit-based previews work on every room type (base fallback path).
		return true;
	}

	// ══════════════════════════════════════════════════════════════════════
	// Media plane (adapter.py:_upload_and_send)
	// ══════════════════════════════════════════════════════════════════════

	/**
	 * Upload bytes then send a TYPED media event: msgtype m.image/m.file/…,
	 * body caption||filename, info{mimetype,size}, url mxc://. Mirrors
	 * _upload_and_send field-for-field (encryption excluded — no crypto
	 * stack on this port).
	 */
	async sendMedia(
		chatId: string,
		file: {
			bytes: Uint8Array;
			filename: string;
			mimeType: string;
			msgtype?: "m.image" | "m.video" | "m.audio" | "m.file";
		},
		opts: { caption?: string | undefined } = {},
	): Promise<SendResult> {
		this.throwIfDisabled();
		let mxcUrl: string;
		try {
			mxcUrl = await this.hs.uploadMedia({
				data: file.bytes,
				mimeType: file.mimeType,
				filename: file.filename,
			});
		} catch (err) {
			return {
				success: false,
				error: brief(err),
			};
		}
		const msgtype =
			file.msgtype ??
			(file.mimeType.startsWith("image/") ? "m.image" : "m.file");
		const content: Record<string, unknown> = {
			msgtype,
			body:
				opts.caption !== undefined && opts.caption !== ""
					? opts.caption
					: file.filename,
			info: {
				mimetype: file.mimeType,
				size: file.bytes.byteLength,
			},
			url: mxcUrl,
		};
		return this.wireTransmitSend(
			chatId,
			content["body"] as string,
			{
				event_content: content,
			} as unknown as Metadata,
		);
	}

	// ══════════════════════════════════════════════════════════════════════
	// Typing + reaction-ack lifecycle (A11/A1 ride-alongs)
	// ══════════════════════════════════════════════════════════════════════

	/**
	 * adapter.py:send_typing — set_typing(timeout=30000); M_LIMIT_EXCEEDED
	 * retry_after_ms honored ONCE via the injected clock (typing site parity).
	 */
	async sendTyping(chatId: string): Promise<SendResult> {
		let honoredOnce = false;
		for (;;) {
			try {
				await this.hs.setTyping(chatId, this.ownUserId, 30_000);
				return { success: true };
			} catch (err) {
				const res: SendResult = { success: false, error: brief(err) };
				const ra = extractMatrixRetryAfterSeconds(res.error ?? "");
				if (ra !== null && !honoredOnce) {
					honoredOnce = true;
					await this.clock.sleep(ra * 1000);
					continue;
				}
				return res;
			}
		}
	}

	async stopTyping(chatId: string): Promise<SendResult> {
		try {
			await this.hs.setTyping(chatId, this.ownUserId, 0);
			return { success: true };
		} catch (err) {
			return { success: false, error: brief(err) };
		}
	}

	/** adapter.py:on_processing_start — 👀 on the triggering message. */
	async onProcessingStart(roomId: string, messageId: string): Promise<void> {
		if (!messageId || !roomId) return;
		try {
			const reactionEventId = await this.hs.sendReaction(
				roomId,
				messageId,
				MATRIX_REACTION_EYES,
			);
			this.pendingReactions.set(`${roomId}|${messageId}`, reactionEventId);
		} catch {
			// scripted failures swallowed — hooks never break flow
		}
	}

	private readonly pendingReactions = new Map<string, string>();

	/** adapter.py:on_processing_complete — swap eyes; cancelled clears. */
	async onProcessingComplete(
		roomId: string,
		messageId: string,
		outcome: "success" | "failure" | "cancelled",
	): Promise<void> {
		if (!messageId || !roomId) return;
		const key = `${roomId}|${messageId}`;
		const eyesEventId = this.pendingReactions.get(key);
		if (eyesEventId !== undefined) {
			this.pendingReactions.delete(key);
			try {
				await this.hs.redactEvent(roomId, eyesEventId);
			} catch {
				/* swallowed */
			}
		}
		if (outcome === "cancelled") return;
		const emoji =
			outcome === "success" ? MATRIX_REACTION_SUCCESS : MATRIX_REACTION_FAILURE;
		try {
			await this.hs.sendReaction(roomId, messageId, emoji);
		} catch {
			/* swallowed */
		}
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

	buildMissingSecretSibling(): MatrixAdapterCore {
		return new MatrixAdapterCore({
			hs: this.hs,
			manifestName: `${this.manifestName}-no-secret`,
			secretReader: () => undefined,
		});
	}

	lifecycleSnapshot(): AdapterStatusSnapshot {
		return this.lifecycle.statusSnapshot();
	}

	/**
	 * Per-chat length pair (§6.3/A15): chats whose id names "utf16" front a
	 * UTF-16-budgeted platform — budget 30 CODE UNITS. Budget AND unit move
	 * together through THE one chat resolution.
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
		if (chatId.includes("smallcap")) {
			return {
				maxMessageLength: Math.min(
					Math.max(
						Number(chatId.match(/smallcap(\d+)/)?.[1] ?? 500),
						MATRIX_MAX_MESSAGE_LENGTH_FLOOR,
					),
					MATRIX_MAX_MESSAGE_LENGTH_CEILING,
				),
				lenUnit: "chars",
			};
		}
		return undefined;
	}
}

// ── helpers ───────────────────────────────────────────────────────────────

function isSyncError(data: MatrixSyncResult): data is { message: string } {
	return typeof (data as { message?: unknown }).message === "string";
}

/** adapter.py:_extract_reply_fallback — quoted lines + leading pill author. */
export function extractReplyFallback(body: string): {
	text: string | null;
	authorId: string | null;
} {
	if (!body || !body.startsWith("> ")) return { text: null, authorId: null };
	const pillRe = /^>\s*<(@[^>]+)>\s*(.*)$/;
	const quotedLines: string[] = [];
	let authorId: string | null = null;
	for (const line of body.split("\n")) {
		if (!line.startsWith("> ")) break;
		let content = line.slice(2);
		if (authorId === null) {
			const pill = pillRe.exec(line);
			if (pill !== null) {
				authorId = pill[1] ?? null;
				content = pill[2] ?? "";
			}
		}
		quotedLines.push(content);
	}
	const quotedText = quotedLines.join("\n").trim() || null;
	return { text: quotedText, authorId };
}

/** adapter.py:_strip_reply_fallback — remove the quoted prefix block. */
export function stripReplyFallback(body: string): string {
	if (!body || !body.startsWith("> ")) return body;
	const lines = body.split("\n");
	const stripped: string[] = [];
	let pastFallback = false;
	for (const line of lines) {
		if (!pastFallback) {
			if (line.startsWith("> ") || line === ">") continue;
			pastFallback = true;
			if (line === "") continue;
		}
		stripped.push(line);
	}
	// Hermes parity: nothing left after stripping ⇒ return the body unchanged.
	return stripped.length > 0 ? stripped.join("\n") : body;
}

/** adapter.py:format_message — image markdown strips to the bare URL. */
export function stripImageMarkdown(content: string): string {
	return content.replace(/!\[([^\]]*)\]\(([^)]+)\)/g, "$2");
}

function roomServerName(roomId: string): string | null {
	if (!roomId.includes(":")) return null;
	const server = roomId.split(":").pop()?.trim();
	return server || null;
}

function escapeRegExp(text: string): string {
	return text.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
}

/** M_LIMIT_EXCEEDED carries retry_after_ms (CS-API error shape). */
export function extractMatrixRetryAfterSeconds(
	errorText: string,
): number | null {
	const ms = /retry_after_ms[":=\s]+(\d+)/.exec(errorText);
	if (ms !== null) return Number(ms[1]) / 1000;
	const generic = extractRetryAfterSeconds(new Error(errorText));
	return generic;
}

function sessionKeyOf(event: IncomingEvent): string {
	return `mx:${String(event.source?.chatId ?? "unknown")}`;
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
