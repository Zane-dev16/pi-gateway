// pi_platforms/matrix/matrix-fake-server — the IN-PHASE fake Matrix
// homeserver (04 §8: rows run headless against fake platform servers; NO
// external network). In-process transport reproducing exactly the
// server-side behaviors the sync-token discipline is cut against:
//
//   - GET /sync long-poll with `since` tokens: a response carries every
//     timeline event AFTER the token, gap-free, plus next_batch. Tokens are
//     opaque monotonic strings in ONE epoch space; a server epoch change
//     (rollback/restore) makes old tokens fail with M_UNKNOWN_SYNC_TOKEN.
//   - Initial sync (no `since`) replays the bounded recent backlog — old
//     events carry old origin_server_ts so client-side startup grace filters.
//   - Soft-logout / revoked token: sync returns an ERROR OBJECT with message
//     m_unknown_token (nio returns SyncError objects, not throws — Hermes
//     string-checks them), whoami rejects.
//   - ONE consumer per account is NOT enforced (Matrix tokens are not
//     exclusive poll sessions) — the family conflict row maps to the epoch/
//     unknown-token stream-death instead, which IS vendor-real.
//   - Wedged consumers: pushed events never wake parked long-polls (#42909
//     parity) while pending counts grow server-side.

export interface MatrixTimelineEvent {
	eventId: string;
	roomId: string;
	sender: string;
	originServerTsMs: number;
	type: "m.room.message" | "m.reaction";
	content: Record<string, unknown>;
	/** Server-assigned token index (gap-free replay key). */
	seq: number;
}

/** Sync-error object shape (nio SyncError parity — Hermes checks .message). */
export interface MatrixSyncError {
	message: string;
}

export interface MatrixSyncResponse {
	next_batch: string;
	rooms: {
		join: Record<string, { timeline: { events: MatrixTimelineEvent[] } }>;
	};
}

export type MatrixSyncResult = MatrixSyncResponse | MatrixSyncError;

export class MatrixTransportError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "MatrixTransportError";
	}
}

export class MatrixUnknownSyncTokenError extends Error {
	constructor() {
		super(
			"400 M_UNKNOWN_SYNC_TOKEN: unknown since token — server epoch changed",
		);
		this.name = "MatrixUnknownSyncTokenError";
	}
}

/** Fixture identity constants (single source; worlds re-export). */
export const ALICE = "@alice:fake.example";
export const BOT_MXID = "@pi-bot:fake.example";

interface SyncWaiter {
	sinceSeq: number;
	settle: (r: MatrixSyncResult) => void;
	fail: (err: Error) => void;
	cancelTimer: () => void;
	/** Coalescing flush scheduled (bursts land in ONE response). */
	flushTimer?: ReturnType<typeof setTimeout> | undefined;
}

export interface RoomRecord {
	name?: string | undefined;
	canonicalAlias?: string | undefined;
	topic?: string | undefined;
	memberCount: number;
}

let serverSeq = 0;

/**
 * Fake homeserver. Message-SEND egress capture lives on the shared harness
 * wire; this server owns the SYNC surface + state (directory/identity) +
 * typing/reaction planes.
 */
export class FakeMatrixHomeserver {
	readonly id = ++serverSeq;

	// ── identity ────────────────────────────────────────────────────────────
	private ownUserId = "@pi-bot:fake.example";

	// ── timeline + tokens ───────────────────────────────────────────────────
	private readonly events: MatrixTimelineEvent[] = [];
	private eventSeq = 0;
	private tokenSeq = 0;
	/** Server epoch — bumping invalidates all previously issued tokens. */
	private epoch = 0;
	/** Token index watermark each issued next_batch covers. */
	private committedWatermark = 0;
	readonly initialSyncBacklogLimit = 64;

	// ── scenario knobs ──────────────────────────────────────────────────────
	private reachable = { general: true, poll: true };
	private authRevoked = false;
	private wedged = false;
	private epochChurn = false;

	// ── state store (directory/identity plane) ──────────────────────────────
	private readonly rooms = new Map<string, RoomRecord>();
	/** m.direct account data: owner mxid → room ids. */
	private readonly directRooms = new Map<string, Set<string>>();
	private displayNames = new Map<string, string>();

	// ── capture planes ──────────────────────────────────────────────────────
	readonly typingEvents: Array<{
		roomId: string;
		userId: string;
		timeoutMs: number;
	}> = [];
	private readonly typingRateLimits: number[] = [];
	readonly reactions: Array<{
		roomId: string;
		sender: string;
		targetEventId: string;
		key: string;
		eventId: string;
		redacted: boolean;
	}> = [];
	readonly readReceipts: Array<{ roomId: string; eventId: string }> = [];
	whoamiCount = 0;
	initialSyncCount = 0;
	unknownTokenSyncResponses = 0;
	dropStaleBacklogOnRestart = false; // engine restart policy observation

	constructor(opts: { nowMs?: () => number } = {}) {
		this.nowMs = opts.nowMs ?? (() => Date.now());
	}

	/** Injected clock (virtual time drives origin_server_ts). */
	nowMs: () => number;

	// ── scenario controls ───────────────────────────────────────────────────

	setOwnUserId(mxid: string): void {
		this.ownUserId = mxid;
	}
	setReachable(
		reachable: boolean,
		pool: "all" | "general" | "poll" = "all",
	): void {
		if (pool === "all")
			this.reachable = { general: reachable, poll: reachable };
		else this.reachable = { ...this.reachable, [pool]: reachable };
		if (!this.reachable.poll)
			this.failAllWaiters(new MatrixTransportError("fetch failed"));
	}
	revokeAuth(): void {
		this.authRevoked = true;
		// Soft-logout delivers the unknown-token SyncError to IN-FLIGHT syncs
		// (nio surfaces auth failures as result objects, not throws).
		for (const waiter of [...this.waiters]) {
			this.removeWaiter(waiter);
			waiter.cancelTimer();
			waiter.settle({ message: "m_unknown_token: Invalid auth token" });
		}
	}
	/** Client connection teardown: parked long-polls DIE (transport class). */
	closeSessions(): void {
		this.failAllWaiters(
			new MatrixTransportError("sync cancelled: client closed"),
		);
	}
	/** Rollback/restore: every previously issued token becomes unknown. */
	invalidateEpoch(): void {
		this.epoch += 1;
		this.failAllWaiters(new MatrixUnknownSyncTokenError());
	}
	/**
	 * Unkillable epoch churn (conflict-exhaustion scenario): EVERY sync call
	 * hits a fresh invalidation first — the recovery ladder can never converge
	 * and must exhaust to FATAL.
	 */
	setEpochChurn(on: boolean): void {
		this.epochChurn = on;
	}
	/** #42909 wedge: long-polls park forever while the queue grows. */
	setWedged(wedged: boolean): void {
		this.wedged = wedged;
		if (!wedged) this.wakeWaiters();
	}

	// ── state surface ───────────────────────────────────────────────────────

	addRoom(roomId: string, record: RoomRecord = { memberCount: 2 }): void {
		this.rooms.set(roomId, record);
	}
	setRoomState(roomId: string, patch: Partial<RoomRecord>): void {
		const rec = this.rooms.get(roomId) ?? { memberCount: 2 };
		this.rooms.set(roomId, { ...rec, ...patch });
	}
	setDirect(ownerMxid: string, roomId: string): void {
		const set = this.directRooms.get(ownerMxid) ?? new Set<string>();
		set.add(roomId);
		this.directRooms.set(ownerMxid, set);
	}
	setDisplayName(userId: string, name: string): void {
		this.displayNames.set(userId, name);
	}

	// ── Client-Server API surface ───────────────────────────────────────────

	async whoami(): Promise<{ user_id: string }> {
		this.assertReachable("general", "whoami");
		this.whoamiCount += 1;
		if (this.authRevoked)
			throw new Error("M_UNKNOWN_TOKEN: Invalid auth token");
		return { user_id: this.ownUserId };
	}

	async getRoomName(roomId: string): Promise<string | null> {
		return this.rooms.get(roomId)?.name ?? null;
	}
	async getRoomCanonicalAlias(roomId: string): Promise<string | null> {
		return this.rooms.get(roomId)?.canonicalAlias ?? null;
	}
	async getRoomTopic(roomId: string): Promise<string | null> {
		return this.rooms.get(roomId)?.topic ?? null;
	}
	async getJoinedMemberCount(roomId: string): Promise<number | null> {
		return this.rooms.get(roomId)?.memberCount ?? null;
	}
	async getDirectAccountData(): Promise<Record<string, string[]>> {
		const out: Record<string, string[]> = {};
		for (const [owner, rooms] of this.directRooms) out[owner] = [...rooms];
		return out;
	}
	async getProfileDisplayname(userId: string): Promise<string | null> {
		return this.displayNames.get(userId) ?? null;
	}

	/** Unconfirmed-by-watermark event count — webhook-info parity probe. */
	get pendingEventCount(): number {
		return this.events.filter((e) => e.seq > this.committedWatermark).length;
	}

	get currentEpoch(): number {
		return this.epoch;
	}

	/** Push one room message into the timeline and wake parked long-polls. */
	pushRoomMessage(
		roomId: string,
		sender: string,
		content: Record<string, unknown>,
		opts: {
			originServerTsMs?: number;
			type?: "m.room.message" | "m.reaction";
		} = {},
	): MatrixTimelineEvent {
		this.eventSeq += 1;
		const evt: MatrixTimelineEvent = {
			eventId: `$evt${this.eventSeq}`,
			roomId,
			sender,
			originServerTsMs: opts.originServerTsMs ?? this.nowMs(),
			type: opts.type ?? "m.room.message",
			content,
			seq: this.eventSeq,
		};
		this.events.push(evt);
		if (!this.wedged) this.wakeWaiters();
		return evt;
	}

	// ── /sync ───────────────────────────────────────────────────────────────

	/**
	 * Long-poll sync. `since` = previously issued next_batch (or null for
	 * initial). Resolves with events strictly after the token's position,
	 * gap-free, or parks up to timeoutMs.
	 */
	async sync(opts: {
		since: string | null;
		timeoutMs: number;
		fullState?: boolean | undefined;
	}): Promise<MatrixSyncResult> {
		this.assertReachable("poll", "sync");
		if (this.epochChurn) this.invalidateEpoch();
		if (this.authRevoked) {
			this.unknownTokenSyncResponses += 1;
			return { message: "m_unknown_token: Invalid auth token" };
		}
		// Rollback-storm churn: the server cannot serve ANY stream (even initial
		// syncs fail while the DB flaps) — recovery can never converge.
		if (this.epochChurn) {
			throw new MatrixUnknownSyncTokenError();
		}
		// #42909 wedged consumers: the connection is a zombie — NO response
		// ever arrives (not even empty batches), whatever the queue holds.
		if (this.wedged) {
			const zombie: SyncWaiter = {
				sinceSeq: -1,
				settle: () => {},
				fail: () => {},
				cancelTimer: () => {},
			};
			this.waiters.push(zombie);
			return new Promise<MatrixSyncResult>((resolve, reject) => {
				zombie.settle = resolve;
				zombie.fail = reject;
			});
		}
		let sinceSeq = 0;
		if (opts.since !== null) {
			const parsed = parseSinceToken(opts.since);
			if (parsed === null || parsed.epoch !== this.epoch) {
				throw new MatrixUnknownSyncTokenError();
			}
			sinceSeq = parsed.seq;
			if (opts.fullState === true && parsed.seq < this.eventSeq) {
				// A full-state call with a STALE token still re-reads history —
				// the recovery restart's drop-stale-backlog policy lives
				// CLIENT-side (startup grace re-applied).
				sinceSeq = 0;
			}
		} else {
			this.initialSyncCount += 1;
		}
		const available = this.events.filter((e) => e.seq > sinceSeq);
		if (available.length > 0 || opts.timeoutMs <= 0) {
			return this.respond(available);
		}
		return new Promise<MatrixSyncResult>((resolve, reject) => {
			// #42909 wedged consumers: the long-poll HANGS — no timeout fires,
			// only client teardown or unwedging ends the park.
			const t = this.wedged
				? null
				: setTimeout(
						() => {
							// A wedged consumer never receives events — even the natural
							// long-poll expiry stays silent (#42909 starvation parity).
							if (this.wedged) return;
							const idx = this.waiters.indexOf(waiter);
							if (idx >= 0) this.waiters.splice(idx, 1);
							resolve(
								this.respond(
									this.events.filter((e) => e.seq > waiter.sinceSeq),
								),
							);
						},
						Math.max(1, opts.timeoutMs),
					);
			const waiter: SyncWaiter = {
				sinceSeq,
				settle: resolve,
				fail: reject,
				cancelTimer: () => {
					if (t !== null) clearTimeout(t);
				},
			};
			t?.unref?.();
			waiter.cancelTimer = () => t !== null && clearTimeout(t);
			this.waiters.push(waiter);
			// Coalescing window: synchronous multi-push bursts land in ONE
			// response (FakeTelegramServer flush parity).
			this.scheduleCoalescedFlush(waiter);
		});
	}

	private scheduleCoalescedFlush(waiter: SyncWaiter): void {
		if (waiter.flushTimer !== undefined) return;
		const t = setTimeout(() => {
			waiter.flushTimer = undefined;
			// A wedged consumer stays starved even across flush windows.
			if (this.wedged) return;
			if (!this.waiters.includes(waiter)) return;
			const ready = this.events.filter((e) => e.seq > waiter.sinceSeq);
			if (ready.length === 0) return;
			this.removeWaiter(waiter);
			waiter.cancelTimer();
			waiter.settle(this.respond(ready));
		}, 2);
		t.unref?.();
		waiter.flushTimer = t;
	}

	// ── typing / reaction / receipt planes ────────────────────────────────

	async setTyping(
		roomId: string,
		userId: string,
		timeoutMs: number,
	): Promise<void> {
		this.assertReachable("general", "setTyping");
		const limited = this.typingRateLimits.shift();
		if (limited !== undefined) {
			throw new Error(
				`M_LIMIT_EXCEEDED: Too many requests (retry_after_ms=${limited * 1000})`,
			);
		}
		this.typingEvents.push({ roomId, userId, timeoutMs });
	}

	/** Script ONE M_LIMIT_EXCEEDED for the next typing call (seconds). */
	scriptRateLimitOnce(seconds: number): void {
		this.typingRateLimits.push(seconds);
	}

	async sendReaction(
		roomId: string,
		sender: string,
		targetEventId: string,
		key: string,
	): Promise<string> {
		this.assertReachable("general", "redact/sendReaction");
		const eventId = `$reaction${this.reactions.length + 1}`;
		this.reactions.push({
			roomId,
			sender,
			targetEventId,
			key,
			eventId,
			redacted: false,
		});
		return eventId;
	}

	async redactEvent(_roomId: string, eventId: string): Promise<void> {
		this.assertReachable("general", "redact");
		for (const r of this.reactions) {
			if (r.eventId === eventId) r.redacted = true;
		}
	}

	async sendReadReceipt(roomId: string, eventId: string): Promise<void> {
		this.readReceipts.push({ roomId, eventId });
	}

	// ── internals ─────────────────────────────────────────────────────────

	private waiters: SyncWaiter[] = [];

	private respond(events: MatrixTimelineEvent[]): MatrixSyncResponse {
		this.tokenSeq = Math.max(this.tokenSeq, ...events.map((e) => e.seq), 0);
		this.committedWatermark = Math.max(this.committedWatermark, this.tokenSeq);
		const join: MatrixSyncResponse["rooms"]["join"] = {};
		for (const evt of events) {
			const bucket = (join[evt.roomId] ??= { timeline: { events: [] } });
			bucket.timeline.events.push(evt);
		}
		return {
			next_batch: makeSinceToken(this.epoch, this.tokenSeq),
			rooms: { join },
		};
	}

	private wakeWaiters(): void {
		for (const waiter of [...this.waiters]) {
			const readyEvents = this.events.filter((e) => e.seq > waiter.sinceSeq);
			if (readyEvents.length === 0) continue;
			// Coalesce: a 2ms window lets synchronous multi-push bursts land in
			// ONE sync response (Bot-API flush parity) without wall-time asserts.
			this.scheduleCoalescedFlush(waiter);
		}
	}

	private failAllWaiters(err: Error): void {
		for (const waiter of [...this.waiters]) {
			this.removeWaiter(waiter);
			waiter.cancelTimer();
			waiter.fail(err);
		}
	}

	private removeWaiter(waiter: SyncWaiter): void {
		const idx = this.waiters.indexOf(waiter);
		if (idx >= 0) this.waiters.splice(idx, 1);
	}

	private assertReachable(pool: "general" | "poll", method: string): void {
		if (!this.reachable[pool]) {
			throw new MatrixTransportError(
				`${method}: getaddrinfo ENOTFOUND matrix.fake.example`,
			);
		}
	}
}

// ── token codec ────────────────────────────────────────────────────────────

export function makeSinceToken(epoch: number, seq: number): string {
	return `s${epoch}_${seq}`;
}

export function parseSinceToken(
	token: string,
): { epoch: number; seq: number } | null {
	const m = /^s(\d+)_(\d+)$/.exec(token);
	if (m === null) return null;
	return { epoch: Number(m[1]), seq: Number(m[2]) };
}
