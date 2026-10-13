// pi_gateway/pickers/store — pending picker entries plus the session index.
//
// Ported semantics from the READ-ONLY Hermes reference (no code vendored),
// cited as file:symbol:
//   tools/clarify_gateway.py:_entries/_session_index (id → entry plus
//     session_key → [id] FIFO for text intercept and scope clearing)
//   clarify_gateway.py:register/wait_for_response (block until resolve or
//     timeout; timeout/unknown id settles None)
//   clarify_gateway.py:resolve_gateway_clarify (record the response and wake
//     the waiter; False when already resolved/expired/unknown — the entry is
//     popped by the waiter, so a resolve landing before the waiter attaches
//     still wins)
//   clarify_gateway.py:mark_awaiting_text (flip into text-capture mode)
//   clarify_gateway.py:clear_session (/new, shutdown, eviction cancel)
//   clarify_gateway.py:get_pending_for_session (oldest awaiting-text entry)
//
// Ids are process-monotonic ints, never uuids: the strictest wire cap in
// scope (Telegram 64-byte callback_data) forbids anything longer, and the
// kit grammar enforces the int shape at build time. In-process like Hermes:
// a restart drops pending pickers and every waiter settles cancelled, never
// resurrected by a late tap.

/** What a settled picker wait produces. */
export type PickerOutcome =
	| { kind: "value"; value: string }
	| { kind: "confirmed"; confirmed: boolean }
	| { kind: "cancelled" };

/** Entry kinds the service asks about. */
export type PickerKind = "select" | "confirm" | "text" | "model" | "resume";

export interface PickerEntrySnapshot {
	id: number;
	sessionKey: string;
	kind: PickerKind;
	title: string;
	options: readonly string[];
	awaitingText: boolean;
}

interface PickerEntry {
	id: number;
	sessionKey: string;
	kind: PickerKind;
	title: string;
	options: string[];
	awaitingText: boolean;
	timeoutMs: number | null;
	/** Null while live; set once by settle (the waiter consumes and drops). */
	outcome: PickerOutcome | null;
	resolve: ((outcome: PickerOutcome) => void) | null;
	timer: ReturnType<typeof setTimeout> | null;
	onAbort: (() => void) | null;
	signal: AbortSignal | null;
}

const CANCELLED: PickerOutcome = { kind: "cancelled" };

function snapshotOf(entry: PickerEntry): PickerEntrySnapshot {
	return {
		id: entry.id,
		sessionKey: entry.sessionKey,
		kind: entry.kind,
		title: entry.title,
		options: [...entry.options],
		awaitingText: entry.awaitingText,
	};
}

function isLive(entry: PickerEntry): boolean {
	return entry.outcome === null;
}

export class PickerStore {
	private nextId = 1;
	private readonly entries = new Map<number, PickerEntry>();
	private readonly sessionIndex = new Map<string, number[]>();

	/** Register a pending picker; the caller then waits on waitFor. */
	register(opts: {
		sessionKey: string;
		kind: PickerKind;
		title: string;
		options?: readonly string[] | undefined;
		/** Open-text entries start capturing at once (open-ended clarify parity). */
		awaitingText?: boolean | undefined;
		/** Wait budget in ms; null/undefined means wait until tapped/aborted. */
		timeoutMs?: number | null | undefined;
	}): number {
		const id = this.nextId++;
		this.entries.set(id, {
			id,
			sessionKey: opts.sessionKey,
			kind: opts.kind,
			title: opts.title,
			options: [...(opts.options ?? [])],
			awaitingText: opts.awaitingText ?? false,
			timeoutMs:
				typeof opts.timeoutMs === "number" &&
				Number.isFinite(opts.timeoutMs) &&
				opts.timeoutMs > 0
					? opts.timeoutMs
					: null,
			outcome: null,
			resolve: null,
			timer: null,
			onAbort: null,
			signal: null,
		});
		const ids = this.sessionIndex.get(opts.sessionKey) ?? [];
		ids.push(id);
		this.sessionIndex.set(opts.sessionKey, ids);
		return id;
	}

	get(id: number): PickerEntrySnapshot | null {
		const entry = this.entries.get(id);
		return entry === undefined || !isLive(entry) ? null : snapshotOf(entry);
	}

	has(id: number): boolean {
		const entry = this.entries.get(id);
		return entry !== undefined && isLive(entry);
	}

	/** Flip an entry into text-capture mode (user picked "Other"); False when unknown/settled. */
	markAwaitingText(id: number): boolean {
		const entry = this.entries.get(id);
		if (entry === undefined || !isLive(entry)) return false;
		entry.awaitingText = true;
		return true;
	}

	/** True when the session holds at least one live entry. */
	hasPending(sessionKey: string): boolean {
		for (const id of this.sessionIndex.get(sessionKey) ?? []) {
			const entry = this.entries.get(id);
			if (entry !== undefined && isLive(entry)) return true;
		}
		return false;
	}

	/**
	 * Oldest live entry awaiting free text (open-ended, or after "Other");
	 * with includeChoicePrompts, the oldest live entry of any kind (typed
	 * text at an active choice prompt resolves it rather than queueing a
	 * follow-up turn).
	 */
	pendingForSession(
		sessionKey: string,
		opts: { includeChoicePrompts?: boolean | undefined } = {},
	): PickerEntrySnapshot | null {
		for (const id of this.sessionIndex.get(sessionKey) ?? []) {
			const entry = this.entries.get(id);
			if (entry === undefined || !isLive(entry)) continue;
			if (entry.awaitingText || opts.includeChoicePrompts === true) {
				return snapshotOf(entry);
			}
		}
		return null;
	}

	/**
	 * Block until the entry settles or timeoutMs elapses (expiry settles
	 * cancelled). A resolve that landed before the waiter attached is
	 * consumed here, never lost. Exactly one waiter per entry: a second
	 * waitFor on a live entry settles cancelled at once. An already-aborted
	 * signal settles cancelled without arming.
	 */
	waitFor(id: number, opts: { signal?: AbortSignal | undefined } = {}): Promise<PickerOutcome> {
		const entry = this.entries.get(id);
		if (entry === undefined) {
			return Promise.resolve(CANCELLED);
		}
		if (!isLive(entry)) {
			const outcome = entry.outcome ?? CANCELLED;
			this.drop(id);
			return Promise.resolve(outcome);
		}
		if (entry.resolve !== null) {
			return Promise.resolve(CANCELLED);
		}
		const signal = opts.signal ?? null;
		if (signal?.aborted === true) {
			this.settle(id, CANCELLED);
			return Promise.resolve(CANCELLED);
		}
		return new Promise<PickerOutcome>((resolve) => {
			entry.resolve = resolve;
			entry.signal = signal;
			if (entry.timeoutMs !== null) {
				entry.timer = setTimeout(() => {
					this.settle(id, CANCELLED);
				}, entry.timeoutMs);
				if (typeof entry.timer === "object" && entry.timer !== null) {
					(entry.timer as { unref?: () => void }).unref?.();
				}
			}
			if (signal !== null) {
				const onAbort = (): void => {
					this.settle(id, CANCELLED);
				};
				entry.onAbort = onAbort;
				signal.addEventListener("abort", onAbort, { once: true });
			}
		});
	}

	/**
	 * Record the outcome for a live entry; False when already
	 * resolved/expired/unknown (resolve_gateway_clarify parity — a late tap
	 * must NOT claim success). The record is synchronous so two same-tick
	 * taps resolve exactly once. A live waiter wakes at once and the entry
	 * drops with it; otherwise the outcome waits for the waiter to consume
	 * it. Either way the entry leaves the session index immediately, so a
	 * resolved picker never intercepts later text.
	 */
	settle(id: number, outcome: PickerOutcome): boolean {
		const entry = this.entries.get(id);
		if (entry === undefined || !isLive(entry)) return false;
		entry.outcome = outcome;
		if (entry.timer !== null) {
			clearTimeout(entry.timer);
			entry.timer = null;
		}
		if (entry.signal !== null && entry.onAbort !== null) {
			entry.signal.removeEventListener("abort", entry.onAbort);
		}
		entry.onAbort = null;
		entry.signal = null;
		this.unindex(id, entry.sessionKey);
		if (entry.resolve !== null) {
			const wake = entry.resolve;
			entry.resolve = null;
			this.entries.delete(id);
			wake(outcome);
		}
		return true;
	}

	/**
	 * Drop every live picker for a session (/new, resume-switch,
	 * shutdown); waiters settle cancelled. Returns how many were live.
	 * Already-resolved entries keep their recorded outcome for the waiter
	 * that is still coming (first-writer-wins parity).
	 */
	cancelSession(sessionKey: string): number {
		let cancelled = 0;
		for (const id of [...(this.sessionIndex.get(sessionKey) ?? [])]) {
			if (this.settle(id, CANCELLED)) cancelled += 1;
		}
		return cancelled;
	}

	/** Remove one id from its session index. */
	private unindex(id: number, sessionKey: string): void {
		const ids = this.sessionIndex.get(sessionKey);
		if (ids === undefined) return;
		const at = ids.indexOf(id);
		if (at >= 0) ids.splice(at, 1);
		if (ids.length === 0) this.sessionIndex.delete(sessionKey);
	}

	/** Drop a consumed entry outright. */
	private drop(id: number): void {
		const entry = this.entries.get(id);
		if (entry === undefined) return;
		this.entries.delete(id);
		this.unindex(id, entry.sessionKey);
	}
}
