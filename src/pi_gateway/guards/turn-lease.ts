// pi_gateway/guards/turn-lease.ts — Layer 1 of the two-layer turn lease
// (02-session-and-state.md §5, DEC-004): the in-process, single-event-loop
// lease registry keyed on the RESOLVED session_id, with generation-scoped,
// identity-checked release. The cross-process DB layer (pi_state/leases.ts)
// keys the compression-lineage ROOT of that same resolved id; a turn holds
// BOTH (03 §7).
//
// Semantic port of hermes gateway/turn_lease.py:SessionTurnLeaseRegistry.
// Hermes reference is READ-ONLY: semantics ported, no code vendored. Anchors:
//   gateway/turn_lease.py:TurnLeaseToken        → TurnLeaseToken (released flag = idempotent release)
//   gateway/turn_lease.py:TurnLeaseTimeoutError → fail-closed timeout signal
//   gateway/turn_lease.py:SessionTurnLeaseRegistry.acquire / release / rebind / _evict_idle
//   gateway/turn_lease.py:_SessionLease.idle    → eviction only ever drops idle entries
//
// JS mapping notes (proven by the Phase-0 spike, spike/lease/
// turn-lease-registry.ts — shape ported as production code per DEC-023):
//   asyncio.Lock  → FIFO waiter queue published by release() synchronously.
//     The Python source tracks pending_acquires across the Lock.release()
//     handoff gap ("Lock.release() wakes a waiter while leaving the lock
//     momentarily unlocked"). Here release() publishes the new holder BEFORE
//     resolving the waiter's promise, so no observable handoff window exists;
//     pendingAcquires still tracks queued waiters to keep _evict_idle parity
//     (a session with a pending acquire is never evictable).
//   asyncio.wait_for(timeout) → per-waiter timer that dequeues + rejects with
//     TurnLeaseTimeoutError (fail-closed: no token is produced on timeout).

export const DEFAULT_MAX_LEASES = 512;

/** gateway/turn_lease.py:DEFAULT_LEASE_WAIT = 1800.0s, as milliseconds. */
export const DEFAULT_LEASE_WAIT_MS = 1_800_000;

export class TurnLeaseTimeoutError extends Error {
	readonly sessionId: string;
	readonly ownerKey: string;
	readonly generation: number;
	readonly waitMs: number;

	constructor(
		sessionId: string,
		ownerKey: string,
		generation: number,
		waitMs: number,
	) {
		super(
			`turn lease wait timed out after ${waitMs}ms on session ${sessionId} ` +
				`for routing key ${ownerKey} (gen ${generation})`,
		);
		this.name = "TurnLeaseTimeoutError";
		this.sessionId = sessionId;
		this.ownerKey = ownerKey;
		this.generation = generation;
		this.waitMs = waitMs;
	}
}

/** gateway/turn_lease.py:TurnLeaseToken — every token handed out is a HELD lease. */
export class TurnLeaseToken {
	sessionId: string;
	readonly ownerKey: string;
	readonly generation: number;
	released = false;

	constructor(sessionId: string, ownerKey: string, generation: number) {
		this.sessionId = sessionId;
		this.ownerKey = ownerKey;
		this.generation = generation;
	}

	toJSON(): Record<string, unknown> {
		return {
			sessionId: this.sessionId,
			ownerKey: this.ownerKey,
			generation: this.generation,
			released: this.released,
		};
	}
}

interface Waiter {
	token: TurnLeaseToken;
	resolve: (t: TurnLeaseToken) => void;
	reject: (e: TurnLeaseTimeoutError) => void;
	timer: NodeJS.Timeout | undefined;
}

/**
 * Per-session slot. Parity of gateway/turn_lease.py:_SessionLease: holder +
 * timestamps + pending-acquire counter + eviction idleness.
 */
class SessionLeaseSlot {
	holder: TurnLeaseToken | null = null;
	acquiredAt = 0;
	lastUsed: number;
	pendingAcquires = 0;
	readonly waiters: Waiter[] = [];

	constructor(now: () => number) {
		this.lastUsed = now();
	}

	/** gateway/turn_lease.py:_SessionLease.idle — evictable only when unheld AND uncontended. */
	get idle(): boolean {
		return (
			this.holder === null &&
			this.pendingAcquires === 0 &&
			this.waiters.length === 0
		);
	}
}

export interface AcquireOptions {
	ownerKey: string;
	generation: number;
	/** Wait budget in ms; non-positive/omitted falls back to DEFAULT_LEASE_WAIT_MS. */
	timeoutMs?: number;
}

export interface RegistryOptions {
	maxEntries?: number;
	/** Injected clock (ms) for acquired_at/last_used — tests assert timing with it. */
	now?: () => number;
	/** Contention warning sink (parity of the acquire-time WARNING in turn_lease.py). */
	onContended?: (info: {
		sessionId: string;
		waitingOwnerKey: string;
		waitingGeneration: number;
		holderOwnerKey?: string;
		holderGeneration?: number;
		heldMs: number;
	}) => void;
	/**
	 * unref() waiter timers so a default-budget waiter can't pin process exit.
	 * Default true; set false when timers must keep the loop alive.
	 */
	unrefTimers?: boolean;
}

export class SessionTurnLeaseRegistry {
	private readonly leases = new Map<string, SessionLeaseSlot>();
	private readonly maxEntries: number;
	private readonly now: () => number;
	private readonly onContended: NonNullable<
		RegistryOptions["onContended"]
	> | null;
	private readonly unrefTimers: boolean;

	constructor(options: RegistryOptions = {}) {
		this.maxEntries = Math.max(1, options.maxEntries ?? DEFAULT_MAX_LEASES);
		this.now = options.now ?? (() => Date.now());
		this.onContended = options.onContended ?? null;
		this.unrefTimers = options.unrefTimers ?? true;
	}

	get size(): number {
		return this.leases.size;
	}

	/** Test/diagnostic observability: current holder token, if any. */
	holderOf(sessionId: string): Readonly<TurnLeaseToken> | null {
		return this.leases.get(sessionId)?.holder ?? null;
	}

	private getOrCreate(sessionId: string): SessionLeaseSlot {
		let lease = this.leases.get(sessionId);
		if (!lease) {
			this.evictIdle();
			lease = new SessionLeaseSlot(this.now);
			this.leases.set(sessionId, lease);
		}
		lease.lastUsed = this.now();
		return lease;
	}

	/** gateway/turn_lease.py:_evict_idle — drop oldest idle entries; NEVER a held/contended one. */
	private evictIdle(): void {
		const overflow = this.leases.size - this.maxEntries + 1;
		if (overflow <= 0) return;
		const idleIds = [...this.leases.entries()]
			.filter(([, lease]) => lease.idle)
			.sort((a, b) => a[1].lastUsed - b[1].lastUsed)
			.map(([sid]) => sid);
		for (const sid of idleIds.slice(0, overflow)) {
			this.leases.delete(sid);
		}
	}

	async acquire(
		sessionId: string,
		options: AcquireOptions,
	): Promise<TurnLeaseToken | null> {
		if (!sessionId) return null; // parity: acquire returns None for falsy session_id
		const waitMs =
			options.timeoutMs !== undefined && options.timeoutMs > 0
				? options.timeoutMs
				: DEFAULT_LEASE_WAIT_MS;
		const token = new TurnLeaseToken(
			sessionId,
			options.ownerKey,
			options.generation,
		);
		const lease = this.getOrCreate(sessionId);

		if (lease.holder !== null) {
			// Parity of the contention WARNING: two routing keys mapped to one
			// session_id (#64934); serialize behind the previous turn's flush.
			this.onContended?.({
				sessionId,
				waitingOwnerKey: token.ownerKey,
				waitingGeneration: token.generation,
				holderOwnerKey: lease.holder.ownerKey,
				holderGeneration: lease.holder.generation,
				heldMs: lease.acquiredAt > 0 ? this.now() - lease.acquiredAt : -1,
			});
		}

		// Fast path: uncontended. Synchronous grant mirrors the Python lock being
		// free — no await between check and publication, so eviction cannot race.
		if (lease.holder === null) {
			publish(lease, token, this.now);
			return token;
		}

		lease.pendingAcquires += 1;
		return new Promise<TurnLeaseToken>((resolve, reject) => {
			const waiter: Waiter = {
				token,
				resolve: (t) => {
					clearTimeout(waiter.timer);
					resolve(t);
				},
				reject: (e) => {
					clearTimeout(waiter.timer);
					reject(e);
				},
				timer: undefined,
			};
			waiter.timer = setTimeout(() => {
				const idx = lease.waiters.indexOf(waiter);
				if (idx >= 0) lease.waiters.splice(idx, 1);
				lease.pendingAcquires -= 1;
				// Fail closed: no token escapes; caller must reject the turn rather
				// than run it against the still-held lease (gateway/turn_lease.py
				// TurnLeaseTimeoutError contract).
				waiter.reject(
					new TurnLeaseTimeoutError(
						sessionId,
						token.ownerKey,
						token.generation,
						waitMs,
					),
				);
			}, waitMs);
			if (this.unrefTimers) waiter.timer.unref?.();
			lease.waiters.push(waiter);
		});
	}

	/**
	 * gateway/turn_lease.py:release — identity-checked, generation-scoped,
	 * idempotent. Returns true only when THIS exact token was the current
	 * holder; a stale unwind can never release a newer turn's lease.
	 */
	release(token: TurnLeaseToken | null | undefined): boolean {
		if (!token || token.released) return false;
		token.released = true;
		const lease = this.leases.get(token.sessionId);
		if (!lease) return false;
		if (lease.holder !== token) return false; // stale/unowned → safe no-op
		lease.holder = null;
		lease.acquiredAt = 0;
		lease.lastUsed = this.now();

		const next = lease.waiters.shift();
		if (next) {
			lease.pendingAcquires -= 1;
			// Publish BEFORE resolve: no unlocked-handoff window exists between
			// wake and holder publication (see file header mapping note).
			publish(lease, next.token, this.now);
			next.resolve(next.token);
		}
		return true;
	}

	/**
	 * gateway/turn_lease.py:rebind — alias a HELD lease onto new_session_id
	 * after mid-turn compression rotation by registering the SAME slot object
	 * under both ids. Only the current holder may rebind; blocked (fail-open)
	 * when the target id already has a live lease of its own.
	 */
	rebind(
		token: TurnLeaseToken | null | undefined,
		newSessionId: string,
	): boolean {
		if (
			!token ||
			token.released ||
			!newSessionId ||
			newSessionId === token.sessionId
		) {
			return false;
		}
		const lease = this.leases.get(token.sessionId);
		if (!lease || lease.holder !== token) return false;

		const existing = this.leases.get(newSessionId);
		if (existing && existing !== lease && !existing.idle) return false; // target live → blocked

		this.leases.set(newSessionId, lease); // same object under both ids
		lease.lastUsed = this.now();
		token.sessionId = newSessionId;
		return true;
	}
}

function publish(
	lease: SessionLeaseSlot,
	token: TurnLeaseToken,
	now: () => number,
): void {
	lease.holder = token;
	lease.acquiredAt = now();
	lease.lastUsed = lease.acquiredAt;
}
