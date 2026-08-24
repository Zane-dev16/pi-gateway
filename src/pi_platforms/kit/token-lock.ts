// pi_platforms/kit/token-lock — the scoped unique-credential lock SEAM
// (06-security-and-profiles.md §5 shape preview; full DB-backed engine lands
// Phase 4). Minimal by mandate — do NOT overbuild.
//
// Ported semantics (04 §3 token-lock row + §7 table row):
//   - Bot token scoped lock (polling) / app-token + channel lock (ws) /
//     per-endpoint secret (webhook)
//   - "Token taken over by another host → Scoped lock conflict; retryable
//     conflict failure; takeover only via explicit replace flag; no silent theft"
//
// THE SEAM CONTRACT: acquisition is SYNCHRONOUS and returns a TUPLE-shaped
// result ({acquired, holder}) — never a promise, never an exception for the
// refusal path. Callers surface named-holder refusal as a FATAL adapter error
// (lifecycle-state.TokenLockConflictError).

export interface LockHolderInfo {
	/** Who holds the lock (instance id / pid string). */
	owner: string;
	/** When the current hold began (ms since arbitrary epoch). */
	heldSinceMs: number;
}

export type LockAcquisition =
	| { acquired: true; lock: AcquiredTokenLock }
	| { acquired: false; holder: LockHolderInfo };

export interface AcquiredTokenLock {
	scope: string;
	credentialId: string;
	release(): void;
}

export interface TokenLockManagerOptions {
	/** Monotonic clock seam (tests inject). Default Date.now. */
	nowMs?: (() => number) | undefined;
}

/**
 * In-process lock registry keyed on (scope, credentialId). Two instances in
 * ONE process contend here; cross-process contention arrives with the
 * DB-backed engine in Phase 4 behind the same interface.
 */
export class TokenLockManagerSeam {
	private readonly locks = new Map<
		string,
		{ owner: string; heldSinceMs: number }
	>();
	private readonly now: () => number;

	constructor(opts: TokenLockManagerOptions = {}) {
		this.now = opts.nowMs ?? (() => Date.now());
	}

	/**
	 * SYNCHRONOUS tuple-returning acquisition. `replace=true` is the explicit
	 * takeover flag (06 §7: "takeover only via explicit replace flag") — the
	 * prior holder's handle is invalidated and its release() becomes a no-op.
	 */
	tryAcquire(
		scope: string,
		credentialId: string,
		owner: string,
		options: { replace?: boolean | undefined } = {},
	): LockAcquisition {
		const key = `${scope}\u0000${credentialId}`;
		const existing = this.locks.get(key);
		if (existing !== undefined && existing.owner !== owner) {
			if (options.replace !== true) {
				return {
					acquired: false,
					holder: { owner: existing.owner, heldSinceMs: existing.heldSinceMs },
				};
			}
		}
		this.locks.set(key, { owner, heldSinceMs: this.now() });
		let released = false;
		return {
			acquired: true,
			lock: {
				scope,
				credentialId,
				release: () => {
					if (released) return;
					released = true;
					const current = this.locks.get(key);
					if (current?.owner === owner) this.locks.delete(key);
				},
			},
		};
	}

	holderOf(scope: string, credentialId: string): LockHolderInfo | null {
		const entry = this.locks.get(`${scope}\u0000${credentialId}`);
		return entry
			? { owner: entry.owner, heldSinceMs: entry.heldSinceMs }
			: null;
	}
}
