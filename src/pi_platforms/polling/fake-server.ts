// pi_platforms/polling/fake-server — the IN-PHASE fake Telegram-like Bot API
// (04 §8: rows run headless against fake platform servers; NO external
// network). In-process transport reproducing exactly the server-side
// behaviors the polling discipline (§3.1) is cut against:
//
//   - getUpdates long-poll with offset confirmation: an update is ACKED when
//     a later call carries offset > update_id; acked updates are pruned and
//     NEVER redelivered (the ack-before-enqueue hazard window).
//   - ONE live getUpdates consumer per bot token; a second consumer's call
//     raises 409 Conflict until the stale session expires or is evicted.
//   - drop_pending_updates=true terminates every OTHER getUpdates session and
//     clears unconfirmed updates (#75017 zombie-eviction parity).
//   - get_webhook_info exposes pending_update_count (heartbeat stuck-probe).
//   - FloodWait scripting for sendChatAction (typing site); text-send/edit
//     FloodWait rides the shared FakePlatformWire scripts.

import type { SendResult } from "../../pi_gateway/streaming/adapter-seam.js";

export interface FakeUpdate {
	updateId: number;
	chatId: string;
	text: string;
	senderId: string;
}

export type TgBehavior =
	| { kind: "ok" }
	| { kind: "flood"; retryAfter: number }
	| { kind: "fail"; error: string };

/** 409 Conflict — another getUpdates consumer holds the poll. */
export class TelegramConflictError extends Error {
	constructor() {
		super(
			"409 Conflict: terminated by other getUpdates request; make sure that only one bot instance is running",
		);
		this.name = "TelegramConflictError";
	}
}

/** Transport-level failure (unreachable server, terminated session). */
export class TelegramTransportError extends Error {
	constructor(message: string) {
		super(message);
		this.name = "TelegramTransportError";
	}
}

interface PollWaiter {
	token: number;
	offset: number;
	settle: (result: GetUpdatesResult) => void;
	reject: (err: Error) => void;
	cancelTimer: () => void;
	/** Coalescing flush scheduled (real servers batch available updates). */
	flushTimer?: ReturnType<typeof setTimeout> | undefined;
}

export interface GetUpdatesResult {
	updates: FakeUpdate[];
}

export interface GetUpdatesOptions {
	sessionToken: number;
	/** Next-unfetched update id (highest confirmed + 1). */
	offset: number;
	timeoutMs: number;
	/** Cold-boot / conflict-recovery parity: evict rival sessions. */
	dropPendingUpdates?: boolean | undefined;
	/**
	 * allowed_updates wire parity (tg-1): the update kinds this poll requests.
	 * The reference family fake ignores it; the telegram fake models real
	 * filtering semantics (unlisted kinds are never delivered).
	 */
	allowedUpdates?: readonly string[] | undefined;
}

let serverSeq = 0;

/**
 * Fake Bot API endpoint. Egress TEXT capture lives on the shared harness wire;
 * this server owns the POLLING surface plus typing capture.
 */
export class FakeTelegramServer {
	readonly id = ++serverSeq;

	// ── update queue + confirmation state ────────────────────────────────
	private updates: FakeUpdate[] = [];
	private nextUpdateId = 1000;
	/** Highest update_id the server will never redeliver (client-acked). */
	private confirmedThrough = 0;

	// ── getUpdates session model ──────────────────────────────────────────
	private sessions = new Map<number, { terminated: boolean }>();
	private sessionSeq = 0;
	private holderToken: number | null = null;
	private pollWaiters: PollWaiter[] = [];

	/** Every drop_pending_updates=true observed, in call order. */
	readonly dropPendingFlags: boolean[] = [];
	/** Offset-commit audit: the ack-before-enqueue observation point. */
	readonly commitLog: Array<{ token: number; offset: number }> = [];

	// ── scripted egress behaviors ─────────────────────────────────────────
	private typingScripts: TgBehavior[] = [];

	// ── wire capture ──────────────────────────────────────────────────────
	readonly chatActions: Array<{
		chatId: string;
		action: string;
		seq: number;
	}> = [];
	private actionSeq = 0;

	// ── scenario knobs ────────────────────────────────────────────────────
	/**
	 * Reachability per request pool — Hermes probes the GENERAL path (get_me,
	 * webhook info) separately from the getUpdates long-poll pool, so a wedge
	 * on one pool is invisible to the other.
	 */
	private reachable = { all: true, general: true, poll: true };
	/**
	 * When true, every drop-pending takeover is immediately re-stolen by a
	 * fresh zombie — the conflict ladder can never win and must exhaust to
	 * FATAL (#75017 retry-loop parity).
	 */
	private unkillableZombie = false;
	/** Wedged consumers (#42909): their long-polls never see updates. */
	private readonly wedgedTokens = new Set<number>();

	scriptTyping(...behaviors: TgBehavior[]): void {
		this.typingScripts.push(...behaviors);
	}

	setReachable(
		reachable: boolean,
		pool: "all" | "general" | "poll" = "all",
	): void {
		if (pool === "all") {
			this.reachable = { all: reachable, general: reachable, poll: reachable };
		} else {
			this.reachable = { ...this.reachable, [pool]: reachable };
		}
		if (!this.reachable.poll) {
			this.rejectAllWaiters(new TelegramTransportError("fetch failed"));
		}
	}

	setUnkillableZombie(on: boolean): void {
		this.unkillableZombie = on;
	}

	/**
	 * Simulate the WEDGED getUpdates consumer (#42909/#55769): the socket
	 * stays open but updates never reach handlers — the server-side queue
	 * grows while the client believes it is polling.
	 */
	setConsumerWedged(sessionToken: number, wedged: boolean): void {
		if (wedged) this.wedgedTokens.add(sessionToken);
		else this.wedgedTokens.delete(sessionToken);
	}

	// ── queue surface ─────────────────────────────────────────────────────

	pushUpdate(chatId: string, text: string, senderId = "user-1"): FakeUpdate {
		const update: FakeUpdate = {
			updateId: this.nextUpdateId++,
			chatId,
			text,
			senderId,
		};
		this.updates.push(update);
		this.wakeWaiters();
		return update;
	}

	/** Unconfirmed (would-redeliver) update count — webhook-info parity. */
	get pendingUpdateCount(): number {
		return this.updates.filter((u) => u.updateId > this.confirmedThrough)
			.length;
	}

	/** Server-side ACK: offsets ≤ `offset - 1` are confirmed and pruned. */
	commitOffset(token: number, offset: number): void {
		this.commitLog.push({ token, offset });
		if (offset - 1 > this.confirmedThrough) this.confirmedThrough = offset - 1;
		this.updates = this.updates.filter((u) => u.updateId >= offset);
	}

	// ── session surface ───────────────────────────────────────────────────

	openSession(): number {
		const token = ++this.sessionSeq;
		this.sessions.set(token, { terminated: false });
		return token;
	}

	/**
	 * Simulate a STALE previous process still holding the poll: a foreign
	 * session becomes the holder without any visible getUpdates call.
	 */
	stealHolderAsZombie(): number {
		const token = this.openSession();
		this.holderToken = token;
		return token;
	}

	get currentHolder(): number | null {
		return this.holderToken;
	}

	liveSessionCount(): number {
		let n = 0;
		for (const s of this.sessions.values()) if (!s.terminated) n += 1;
		return n;
	}

	isSessionTerminated(token: number): boolean {
		return this.sessions.get(token)?.terminated ?? true;
	}

	// ── Bot API surface ───────────────────────────────────────────────────

	async getMe(): Promise<{ username: string }> {
		this.assertReachable("general", "getMe");
		return { username: "pi_gateway_reference_bot" };
	}

	async getWebhookInfo(): Promise<{ pending_update_count: number }> {
		this.assertReachable("general", "getWebhookInfo");
		return { pending_update_count: this.pendingUpdateCount };
	}

	/**
	 * Long-poll fetch. Conflict/termination checks run BEFORE waiting so a
	 * fresh generation under a zombie gets its 409 immediately.
	 */
	async getUpdates(opts: GetUpdatesOptions): Promise<GetUpdatesResult> {
		this.assertReachable("poll", "getUpdates");
		const session = this.sessions.get(opts.sessionToken);
		if (session === undefined || session.terminated) {
			throw new TelegramConflictError();
		}

		if (opts.dropPendingUpdates === true) {
			this.dropPendingFlags.push(true);
			this.evictRivalSessions(opts.sessionToken);
			this.updates = []; // unconfirmed queue dropped with the zombies
		}

		// ONE consumer owns the poll: a live rival holder conflicts this call.
		if (
			this.holderToken !== null &&
			this.holderToken !== opts.sessionToken &&
			!(this.sessions.get(this.holderToken)?.terminated ?? true)
		) {
			throw new TelegramConflictError();
		}
		this.holderToken = opts.sessionToken;

		const available = (): FakeUpdate[] =>
			this.updates.filter((u) => u.updateId >= opts.offset);
		const ready = available();
		if (ready.length > 0) return { updates: ready };
		if (opts.timeoutMs <= 0) return { updates: [] };

		return new Promise<GetUpdatesResult>((resolve, reject) => {
			const waiter: PollWaiter = {
				token: opts.sessionToken,
				offset: opts.offset,
				settle: resolve,
				reject,
				cancelTimer: () => clearTimeout(t),
			};
			const t = setTimeout(
				() => {
					const idx = this.pollWaiters.indexOf(waiter);
					if (idx >= 0) this.pollWaiters.splice(idx, 1);
					resolve({ updates: available() });
				},
				Math.max(1, opts.timeoutMs),
			);
			t.unref?.();
			waiter.cancelTimer = () => clearTimeout(t);
			this.pollWaiters.push(waiter);
		});
	}

	/** Typing indicator site (NOT user-visible text → no chokepoint door). */
	async sendChatAction(chatId: string, action = "typing"): Promise<SendResult> {
		this.assertReachable("general", "sendChatAction");
		const behavior =
			this.typingScripts.shift() ?? ({ kind: "ok" } as TgBehavior);
		this.actionSeq += 1;
		this.chatActions.push({ chatId, action, seq: this.actionSeq });
		switch (behavior.kind) {
			case "ok":
				return { success: true };
			case "flood":
				return {
					success: false,
					error: `Too Many Requests: retry after ${behavior.retryAfter}`,
					retryAfter: behavior.retryAfter,
				};
			case "fail":
				return { success: false, error: behavior.error };
		}
	}

	// ── internals ─────────────────────────────────────────────────────────

	private assertReachable(pool: "general" | "poll", method: string): void {
		if (!this.reachable.all || !this.reachable[pool]) {
			throw new TelegramTransportError(
				`${method}: getaddrinfo ENOTFOUND api.fake.telegram.example`,
			);
		}
	}

	/**
	 * drop_pending_updates=True parity (#75017): terminate EVERY other live
	 * getUpdates session — their in-flight long-polls reject — and take the
	 * holder seat. This is the ONLY way to kill the stale server-side session.
	 */
	private evictRivalSessions(takerToken: number): void {
		for (const [token, session] of this.sessions) {
			if (token === takerToken || session.terminated) continue;
			session.terminated = true;
		}
		for (const waiter of [...this.pollWaiters]) {
			if (waiter.token === takerToken) continue;
			this.removeWaiter(waiter);
			this.clearFlush(waiter);
			waiter.cancelTimer();
			waiter.reject(
				new TelegramTransportError(
					"long-poll terminated by a drop_pending_updates takeover",
				),
			);
		}
		this.holderToken = takerToken;
		if (this.unkillableZombie) {
			// The rival process immediately re-steals the poll — recovery can
			// never converge and the ladder must exhaust.
			this.stealHolderAsZombie();
		}
	}

	private wakeWaiters(): void {
		for (const waiter of [...this.pollWaiters]) {
			const session = this.sessions.get(waiter.token);
			if (session === undefined || session.terminated) {
				this.removeWaiter(waiter);
				this.clearFlush(waiter);
				waiter.cancelTimer();
				waiter.reject(new TelegramConflictError());
				continue;
			}
			if (this.wedgedTokens.has(waiter.token)) continue; // wedged: never wakes
			// Coalesce: a 2ms window lets synchronous multi-push bursts land in
			// ONE getUpdates batch (Bot API parity) without wall-time asserts.
			if (waiter.flushTimer === undefined) {
				const t = setTimeout(() => {
					waiter.flushTimer = undefined;
					this.flushWaiter(waiter);
				}, 2);
				t.unref?.();
				waiter.flushTimer = t;
			}
		}
	}

	private clearFlush(waiter: PollWaiter): void {
		if (waiter.flushTimer !== undefined) {
			clearTimeout(waiter.flushTimer);
			waiter.flushTimer = undefined;
		}
	}

	private flushWaiter(waiter: PollWaiter): void {
		const session = this.sessions.get(waiter.token);
		if (session === undefined || session.terminated) {
			this.removeWaiter(waiter);
			waiter.cancelTimer();
			waiter.reject(new TelegramConflictError());
			return;
		}
		if (this.wedgedTokens.has(waiter.token)) return; // wedged: starve
		const ready = this.updates.filter((u) => u.updateId >= waiter.offset);
		if (ready.length > 0 && this.holderToken === waiter.token) {
			this.removeWaiter(waiter);
			waiter.cancelTimer();
			waiter.settle({ updates: ready });
		}
	}

	private rejectAllWaiters(err: Error): void {
		for (const waiter of [...this.pollWaiters]) {
			this.removeWaiter(waiter);
			waiter.cancelTimer();
			waiter.reject(err);
		}
	}

	private removeWaiter(waiter: PollWaiter): void {
		const idx = this.pollWaiters.indexOf(waiter);
		if (idx >= 0) this.pollWaiters.splice(idx, 1);
	}
}
