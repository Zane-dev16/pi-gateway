// pi_platforms/kit/callback-router — the SINGLE query handler per adapter
// routing every button sender into gateway-side resolvers (04 §9.1; DEC-016).
//
// Ported from the READ-ONLY Hermes reference, semantics only:
//   plugins/platforms/telegram/adapter.py:_handle_callback_query
//     (sole CallbackQueryHandler registration — prefix dispatch)
//   adapter.py:_is_callback_user_authorized
//     (unauthorized taps are ANSWERED and NOT resolved — fail-closed)
//   adapter.py:ea/sc/cl arms  (_approval_state.pop / _slash_confirm_state.pop /
//     _clarify_state.get — POP = one-shot double-tap dedup; clarify KEPT)
//   adapter.py:_notify_clarify_expired
//     (expired taps answer an explicit expiry message, never dispatch a turn)
//   adapter.py resolution sites (edit host message + reply_markup=None —
//     consumed state is visible on the host message)
//
// Contract clauses enforced here:
//   - EVERY tap answers (spinner always clears) — unknown/garbage/stale included
//   - unknown/stale/expired taps NEVER dispatch turns
//   - unauthorized clicker ignored (answered ⛔, no resolution)
//   - one-shot families pop atomically → double-tap resolves exactly once
//   - clarify state KEPT until terminal; `other` flips to free-text capture

import {
	type ExecApprovalChoice,
	type ParsedCallback,
	type SlashConfirmChoice,
	parseCallbackData,
} from "./callback-grammar.js";

/** What the adapter must render after a routed tap. */
export type CallbackAnswer =
	/** A resolver fired — the host message MUST be edited with the keyboard
	 * STRIPPED (consumed state is visible; §9.1). */
	| {
			kind: "resolved";
			answerText: string;
			hostEdit: { text: string; keyboardRemoved: true };
	  }
	/** Unauthorized / stale / expired / garbage — answered so the spinner
	 * always clears, host message untouched, NEVER dispatched as a turn. */
	| {
			kind: "unauthorized" | "stale" | "unknown";
			answerText: string;
			hostEdit: null;
	  }
	/** Picker navigation — adapter-owned state machine may re-render WITH its
	 * keyboard (navigation continues); never a resolution. */
	| {
			kind: "nav";
			answerText: string;
			hostEdit: { text: string; keyboardRemoved: false } | null;
	  };

export interface CallbackTapContext {
	/** Platform user id of the clicker ("" when absent — fail-closed). */
	userId: string;
	chatId?: string | undefined;
	/** Host-message chat shape as delivered by the wire ("private" | "group"
	 * | "supergroup" | "channel" …). Untouched by the router itself — adapters
	 * with real authorization postures (tg-11: Telegram's
	 * `_is_callback_user_authorized` SessionSource construction :1183) map it
	 * onto their authz source vocabulary and need thread ids / display names
	 * for that mapping. Always OPTIONAL so every existing tap builder keeps
	 * compiling; absent fields simply carry no authorization signal. */
	chatType?: string | undefined;
	threadId?: string | undefined;
	userName?: string | undefined;
}

/** Fail-closed clicker authorization against user allowlists (§9.1). */
export type ClickAuthorizer = (
	ctx: CallbackTapContext,
	family: ParsedCallback["family"],
) => boolean;

const UNAUTHORIZED_TEXT = "⛔ You are not authorized.";

interface PendingEntry {
	sessionKey: string;
	/** Monotonic deadline in ms; Infinity = no TTL (clarify keeps until terminal). */
	expiresAtMs: number;
}

/**
 * INTERACTIVE_STATE_CACHE_SIZE parity (whatsapp_cloud.py:@~102; the same bound
 * Hermes applies to _clarify_state/_exec_approval_state/_slash_confirm_state
 * across adapters): pending-prompt stores are FIFO-capped at 1000 with
 * OLDEST eviction so long-running processes stay bounded.
 */
export const PENDING_STATE_CACHE_SIZE = 1000;

/** Oldest-first eviction past `maxEntries` (Map preserves insertion order). */
function evictOldestPast(
	entries: Map<string, unknown>,
	maxEntries: number,
	onEvict?: ((key: string) => void) | undefined,
): void {
	while (entries.size > maxEntries) {
		const oldest = entries.keys().next();
		if (oldest.done === true) break;
		entries.delete(oldest.value);
		onEvict?.(oldest.value);
	}
}

/** Atomic-pop one-shot store (ea:/sc:/appr:) — double-tap dedup by POP.
 * Bounded at PENDING_STATE_CACHE_SIZE with oldest eviction (_bounded_put
 * parity) so unbounded prompt churn cannot grow the process. */
export type PopOutcome =
	| { state: "live"; sessionKey: string }
	| { state: "expired" }
	| { state: "absent" };

export class OneShotPendingStore {
	private readonly entries = new Map<string, PendingEntry>();
	private readonly maxEntries: number;

	constructor(maxEntries: number = PENDING_STATE_CACHE_SIZE) {
		this.maxEntries = Math.max(1, maxEntries);
	}

	register(
		id: string | number,
		sessionKey: string,
		expiresAtMs = Infinity,
	): void {
		this.entries.set(String(id), { sessionKey, expiresAtMs });
		evictOldestPast(this.entries, this.maxEntries);
	}

	/** Atomic POP: first tap wins; second tap finds nothing (resolved once).
	 * Expired entries are consumed AND reported distinctly (#63501 — a tap
	 * landing after the wait timed out answers an EXPLICIT expiry message,
	 * never "approved"). */
	pop(id: string | number, nowMs: number): PopOutcome {
		const key = String(id);
		const entry = this.entries.get(key);
		if (entry === undefined) return { state: "absent" };
		this.entries.delete(key); // pop FIRST — even an expired entry is consumed
		if (nowMs >= entry.expiresAtMs) return { state: "expired" };
		return { state: "live", sessionKey: entry.sessionKey };
	}

	has(id: string | number): boolean {
		return this.entries.has(String(id));
	}

	/**
	 * Oldest (insertion-order) pending id registered for a session key, or
	 * null. has_blocking_approval parity probe for wires that resolve clicks
	 * WITHOUT carrying an approval id in the button payload (Teams
	 * _on_card_action resolves through session_key; tools/approval.py:2882).
	 */
	oldestIdForSession(sessionKey: string): string | null {
		for (const [key, entry] of this.entries) {
			if (entry.sessionKey === sessionKey) return key;
		}
		return null;
	}
}

/**
 * Clarify store: state KEPT until terminal (§9.1). `other` flips the entry to
 * free-text capture WITHOUT popping; numeric choice pops and resolves.
 */
export class ClarifyPendingStore {
	private readonly entries = new Map<string, PendingEntry>();
	private readonly awaitingText = new Set<string>();
	private readonly maxEntries: number;

	constructor(maxEntries: number = PENDING_STATE_CACHE_SIZE) {
		this.maxEntries = Math.max(1, maxEntries);
	}

	register(
		id: string | number,
		sessionKey: string,
		expiresAtMs = Infinity,
	): void {
		const key = String(id);
		this.entries.set(key, { sessionKey, expiresAtMs });
		evictOldestPast(this.entries, this.maxEntries, (evicted) => {
			this.awaitingText.delete(evicted);
		});
	}

	get(id: string | number): string | null {
		const entry = this.entries.get(String(id));
		return entry?.sessionKey ?? null;
	}

	has(id: string | number): boolean {
		return this.entries.has(String(id));
	}

	isAwaitingText(id: string | number): boolean {
		return this.awaitingText.has(String(id));
	}

	/** mark_awaiting_text parity — flip to free-text capture, KEEP the entry.
	 * Expired entries evict and refuse the flip (a typed answer would go
	 * nowhere — _notify_clarify_expired parity). */
	markAwaitingText(id: string | number, nowMs = Infinity): boolean {
		const key = String(id);
		const entry = this.entries.get(key);
		if (entry === undefined) return false;
		if (nowMs >= entry.expiresAtMs) {
			this.entries.delete(key);
			this.awaitingText.delete(key);
			return false;
		}
		this.awaitingText.add(key);
		return true;
	}

	/** Terminal resolve (numeric choice or captured text) — pops the entry. */
	pop(id: string | number, nowMs: number): string | null {
		const key = String(id);
		const entry = this.entries.get(key);
		if (entry === undefined) return null;
		this.entries.delete(key);
		this.awaitingText.delete(key);
		if (nowMs >= entry.expiresAtMs) return null;
		return entry.sessionKey;
	}
}

export interface RouterStores {
	approvals: OneShotPendingStore;
	slashConfirms: OneShotPendingStore;
	appr: OneShotPendingStore;
	clarify: ClarifyPendingStore;
}

export interface CreateRouterOptions {
	stores?: Partial<RouterStores> | undefined;
	authorizer: ClickAuthorizer;
	/** Injected clock for expiry decisions (flake discipline). */
	nowMs?: (() => number) | undefined;
	/**
	 * Gateway-side resolvers. Each returns the human label rendered onto the
	 * host edit ("✅ Approved once", …). A resolver returning null reports
	 * expiry to the user (#63501: a tap landing after the wait timed out must
	 * NOT claim success).
	 */
	onExecApproval?:
		| ((
				sessionKey: string,
				choice: ExecApprovalChoice,
		  ) => Promise<string | null>)
		| undefined;
	onSlashConfirm?:
		| ((
				sessionKey: string,
				confirmId: number,
				choice: SlashConfirmChoice,
		  ) => Promise<string | null>)
		| undefined;
	onClarifyChoice?:
		| ((
				sessionKey: string,
				clarifyId: number,
				idx: number,
		  ) => Promise<string | null>)
		| undefined;
	onClarifyOther?:
		| ((sessionKey: string, clarifyId: number) => Promise<string | null>)
		| undefined;
	onWhatsappApproval?:
		| ((
				sessionKey: string,
				id: number,
				approve: boolean,
		  ) => Promise<string | null>)
		| undefined;
	/**
	 * Model-picker / choice-picker navigation hook — the adapter owns its own
	 * picker state machine; the router hands over parsed nav callbacks. Return
	 * the label to answer with (host edit optional via outcome).
	 */
	onPickerNav?:
		| ((
				parsed: ParsedCallback,
				ctx: CallbackTapContext,
		  ) => Promise<{ answerText: string; hostEditText?: string }>)
		| undefined;
}

const EXPIRED_APPROVAL_TEXT =
	"⌛ Approval expired — no command was waiting. It already timed out (and was denied) or was resolved elsewhere.";
const ALREADY_RESOLVED_TEXT = "This prompt has already been resolved.";
const CLARIFY_EXPIRED_TEXT =
	"⌛ This question expired before an answer landed. Ask again or reply in text.";

export class CallbackQueryRouter {
	private readonly approvals: OneShotPendingStore;
	private readonly slashConfirms: OneShotPendingStore;
	private readonly appr: OneShotPendingStore;
	private readonly clarify: ClarifyPendingStore;
	private readonly authorizer: ClickAuthorizer;
	private readonly nowMs: () => number;
	private readonly opts: CreateRouterOptions;

	/** Audit trail: every route() call appends exactly one entry. */
	readonly audit: Array<{
		data: string;
		outcome: "resolved" | "unauthorized" | "stale" | "unknown" | "nav";
	}> = [];

	constructor(opts: CreateRouterOptions) {
		this.opts = opts;
		this.approvals = opts.stores?.approvals ?? new OneShotPendingStore();
		this.slashConfirms =
			opts.stores?.slashConfirms ?? new OneShotPendingStore();
		this.appr = opts.stores?.appr ?? new OneShotPendingStore();
		this.clarify = opts.stores?.clarify ?? new ClarifyPendingStore();
		this.authorizer = opts.authorizer;
		this.nowMs = opts.nowMs ?? (() => Date.now());
	}

	storeViews(): RouterStores {
		return {
			approvals: this.approvals,
			slashConfirms: this.slashConfirms,
			appr: this.appr,
			clarify: this.clarify,
		};
	}

	/**
	 * THE single wire entry point. Adapters register EXACTLY ONE SDK handler
	 * and forward every callback query here — prefix dispatch happens inside.
	 */
	async route(data: string, tap: CallbackTapContext): Promise<CallbackAnswer> {
		const parsed = parseCallbackData(data);
		try {
			if (parsed.family === "unknown") {
				// Garbage/unknown/stale-family taps still ANSWER (spinner clears),
				// never dispatch.
				return this.finish(data, "unknown", {
					kind: "unknown",
					answerText: ALREADY_RESOLVED_TEXT,
					hostEdit: null,
				});
			}
			if (parsed.family === "mx") {
				// Inert page counter — answered, nothing else.
				return this.finish(data, "nav", {
					kind: "nav",
					answerText: " ",
					hostEdit: null,
				});
			}

			// Authorization gate — fail closed on empty ids. Unauthorized taps
			// are IGNORED for resolution purposes but still answered so the
			// client spinner clears (Hermes answers ⛔ then returns).
			if (!this.authorizer(tap, parsed.family)) {
				return this.finish(data, "unauthorized", {
					kind: "unauthorized",
					answerText: UNAUTHORIZED_TEXT,
					hostEdit: null,
				});
			}

			switch (parsed.family) {
				case "ea":
					return await this.routeOneShot(
						data,
						parsed.family,
						this.approvals.pop(parsed.approvalId, this.nowMs()),
						parsed.choice,
						tap.userId,
						this.opts.onExecApproval,
						{
							once: "✅ Approved once",
							session: "✅ Approved for session",
							always: "✅ Approved permanently",
							deny: "❌ Denied",
						},
						(stale) => (stale ? EXPIRED_APPROVAL_TEXT : ALREADY_RESOLVED_TEXT),
					);
				case "appr":
					return await this.routeOneShot(
						data,
						parsed.family,
						this.appr.pop(parsed.id, this.nowMs()),
						parsed.choice === "approve" ? "once" : "deny",
						tap.userId,
						this.opts.onWhatsappApproval === undefined
							? undefined
							: async (sessionKey, _choice) =>
									(
										this.opts.onWhatsappApproval as NonNullable<
											CreateRouterOptions["onWhatsappApproval"]
										>
									)(sessionKey, parsed.id, parsed.choice === "approve"),
						{ once: "✅ Approved", deny: "❌ Denied" },
						(stale) => (stale ? EXPIRED_APPROVAL_TEXT : ALREADY_RESOLVED_TEXT),
					);
				case "sc":
					return await this.routeOneShot(
						data,
						parsed.family,
						this.slashConfirms.pop(parsed.confirmId, this.nowMs()),
						parsed.choice,
						tap.userId,
						(sessionKey, choice) =>
							this.opts.onSlashConfirm
								? this.opts.onSlashConfirm(sessionKey, parsed.confirmId, choice)
								: Promise.resolve(choice),
						{
							once: "✅ Approved once",
							always: "🔒 Always approve",
							cancel: "❌ Cancelled",
						},
						(stale) => (stale ? EXPIRED_APPROVAL_TEXT : ALREADY_RESOLVED_TEXT),
					);
				case "cl":
					return await this.routeClarify(data, parsed, tap);
				default: {
					// Picker/model navigation — adapter-owned state machine.
					if (this.opts.onPickerNav === undefined) {
						return this.finish(data, "unknown", {
							kind: "unknown",
							answerText: " ",
							hostEdit: null,
						});
					}
					const nav = await this.opts.onPickerNav(parsed, tap);
					return this.finish(data, "nav", {
						kind: "nav",
						answerText: nav.answerText,
						hostEdit:
							nav.hostEditText !== undefined
								? { text: nav.hostEditText, keyboardRemoved: false }
								: null,
					});
				}
			}
		} catch (err) {
			// A raising handler must STILL answer (spinner clears; plugin-style
			// exceptions never wedge the client).
			return this.finish(data, "unknown", {
				kind: "unknown",
				answerText: `⚠️ ${err instanceof Error ? err.message.slice(0, 80) : "error"}`,
				hostEdit: null,
			});
		}
	}

	private async routeOneShot(
		data: string,
		_family: string,
		pop: PopOutcome,
		choice: string,
		_userId: string,
		resolve:
			| ((sessionKey: string, choice: never) => Promise<string | null>)
			| undefined,
		labels: Record<string, string>,
		staleText: (expired: boolean) => string,
	): Promise<CallbackAnswer> {
		if (pop.state !== "live") {
			// Expired ⇒ explicit expiry message; absent ⇒ already-resolved.
			// Neither dispatches a turn (#63501).
			return this.finish(data, "stale", {
				kind: "stale",
				answerText: staleText(pop.state === "expired"),
				hostEdit: null,
			});
		}
		let label: string;
		if (resolve === undefined) {
			label = labels[choice] ?? "Resolved";
		} else {
			const result = await resolve(pop.sessionKey, choice as never);
			label = result === null ? staleText(true) : (labels[choice] ?? result);
		}
		return this.finish(data, "resolved", {
			kind: "resolved",
			answerText: label,
			// Resolution EDITS the host message and strips the keyboard —
			// consumed state is visible (§9.1).
			hostEdit: { text: label, keyboardRemoved: true },
		});
	}

	private async routeClarify(
		data: string,
		parsed: Extract<ParsedCallback, { family: "cl" }>,
		tap: CallbackTapContext,
	): Promise<CallbackAnswer> {
		void tap;
		if (parsed.idx === "other") {
			const sessionKey = this.clarify.get(parsed.clarifyId);
			if (sessionKey === null) {
				return this.finish(data, "stale", {
					kind: "stale",
					answerText: CLARIFY_EXPIRED_TEXT,
					hostEdit: null,
				});
			}
			const flipped = this.clarify.markAwaitingText(
				parsed.clarifyId,
				this.nowMs(),
			);
			if (!flipped) {
				// Entry evicted between ask and tap — a typed answer would go
				// nowhere; surface expiry instead of a misleading ✓.
				return this.finish(data, "stale", {
					kind: "stale",
					answerText: CLARIFY_EXPIRED_TEXT,
					hostEdit: null,
				});
			}
			// Flip edits the host prompt (awaiting-typed-response notice) and
			// strips its keyboard — Hermes parity (_handle_callback_query cl:other).
			return this.finish(data, "resolved", {
				kind: "resolved",
				answerText: "✏️ Type your answer in the chat.",
				hostEdit: {
					text: "❓ Awaiting typed response…",
					keyboardRemoved: true,
				},
			});
		}
		const idx = typeof parsed.idx === "number" ? parsed.idx : -1;
		const sessionKey = this.clarify.get(parsed.clarifyId);
		if (sessionKey === null) {
			return this.finish(data, "stale", {
				kind: "stale",
				answerText: CLARIFY_EXPIRED_TEXT,
				hostEdit: null,
			});
		}
		const popped = this.clarify.pop(parsed.clarifyId, this.nowMs());
		if (popped === null) {
			return this.finish(data, "stale", {
				kind: "stale",
				answerText: CLARIFY_EXPIRED_TEXT,
				hostEdit: null,
			});
		}
		const label = this.opts.onClarifyChoice
			? ((await this.opts.onClarifyChoice(popped, parsed.clarifyId, idx)) ??
				CLARIFY_EXPIRED_TEXT)
			: `choice ${idx + 1}`;
		return this.finish(data, "resolved", {
			kind: "resolved",
			answerText: `✓ ${label.slice(0, 60)}`,
			hostEdit: { text: label, keyboardRemoved: true },
		});
	}

	private finish(
		data: string,
		outcome: "resolved" | "unauthorized" | "stale" | "unknown" | "nav",
		answer: CallbackAnswer,
	): CallbackAnswer {
		this.audit.push({ data, outcome });
		return answer;
	}
}
