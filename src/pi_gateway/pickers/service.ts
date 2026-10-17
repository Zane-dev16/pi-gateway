// pi_gateway/pickers/service — the chat-native picker service.
//
// Strategy (DEC-023): never mount the TUI SessionSelector/ModelSelector
// (TUI-only overlays with no RPC wire form). The gateway runs the real pi
// RPC child; the turn runner forwards its `extension_ui_request`
// {select,confirm,input,editor} events here, and this service renders
// chat-native controls through the adapter sender seam, then replies
// `extension_ui_response` (DEC-088). Bare `/resume` and `/model` route to
// the same entries. An unknown dialog method (incl. any future `custom`
// payload — today `custom()` returns undefined with no request at all)
// falls back to a numbered-text prompt so the turn always completes.
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   tools/clarify_gateway.py:resolve_gateway_clarify/mark_awaiting_text
//   tools/approval.py:resolve_gateway_approval
//   tools/slash_confirm.py:resolve
//   gateway/slash_commands_session.py:_handle_resume_command (≤10 listing,
//     tap switches plus clears scope)
//   gateway/slash_commands_model.py:_model_listing_reply/_perform_model_switch/
//     _commit_model_switch_locked/_record_model_switch
//   gateway/run_turn_runner.py:_prepend_pending_note
//   gateway/session.py:sanitize_model_override
//   gateway/run_inbound.py (typed text resolves the pending entry)
//
// Layering: rank 3 (pi_gateway) imports pi_state/pi_agent_core shapes only
// as structural types declared HERE — never pi_platforms/kit (upward) and
// never the runner. Adapters satisfy PickerSender structurally; the turn
// runner consumes this service through its own local seam.

import type {
	Metadata,
	SendResult,
} from "../streaming/adapter-seam.js";
import { PickerStore, type PickerOutcome } from "./store.js";

/** One selectable row (choice-picker {"value","label","is_current"} parity). */
export interface PickerChoice {
	value: string;
	label: string;
	isCurrent?: boolean | undefined;
}

/** One provider row for the model drill-down. */
export interface ModelProviderRow {
	slug: string;
	name: string;
	models: string[];
	totalModels: number;
	isCurrent: boolean;
}

/** Semantic model-picker screens; the ADAPTER renders these to native
 * keyboards (it owns the kit grammar — this layer cannot import it). */
export type ModelScreen =
	| {
			kind: "providers";
			text: string;
			providers: ModelProviderRow[];
			page: number;
			totalPages: number;
	  }
	| {
			kind: "models";
			text: string;
			providerName: string;
			models: string[];
			start: number;
			page: number;
			totalPages: number;
	  }
	| { kind: "warn"; text: string; index: number }
	| { kind: "result"; text: string };

/** Model drill-down tap actions (adapter-parsed from mp:/mpv:/mm:/mc:/mb). */
export type ModelTapAction =
	| { kind: "provider"; slug: string }
	| { kind: "providerPage"; page: number }
	| { kind: "modelPage"; page: number }
	| { kind: "model"; index: number }
	| { kind: "confirm"; index: number }
	| { kind: "back" }
	| { kind: "cancel" };

/** Tap claim results — resolved taps render, stale taps toast, never a turn. */
export type TapClaim =
	| { status: "resolved"; value: string }
	| { status: "awaiting-text" }
	| { status: "stale" };

/**
 * Adapter sender seam. Keyboard sends return Not-supported on text-only
 * platforms; the service falls back to a numbered list and latches there
 * (egress-door capability-miss parity). Senders register nothing — the
 * service owns entries; adapters only render.
 */
export interface ChoicePageInfo {
	page: number;
	totalPages: number;
	start: number;
	total: number;
}

/** Paginated choice screen (DEC-089 — model-screen parity for `cp:`). */
export interface ChoiceNavScreen {
	title: string;
	choices: PickerChoice[];
	page: number;
	totalPages: number;
	start: number;
	total: number;
}

export interface PickerSender {
	/** Manifest data fast-path (interactiveModel:false ⇒ text at once). */
	readonly interactivePickers?: boolean | undefined;
	sendChoicePicker(
		chatId: string,
		title: string,
		choices: PickerChoice[],
		sessionKey: string,
		pickerId: number,
		metadata?: Metadata | undefined,
		page?: ChoicePageInfo | undefined,
	): Promise<SendResult>;
	sendSlashConfirm(
		chatId: string,
		title: string,
		message: string,
		sessionKey: string,
		pickerId: number,
		metadata?: Metadata | undefined,
	): Promise<SendResult>;
	sendModelPicker(
		chatId: string,
		screen: Extract<ModelScreen, { kind: "providers" }>,
		sessionKey: string,
		pickerId: number,
		metadata?: Metadata | undefined,
	): Promise<SendResult>;
	/**
	 * Clarify keyboard (one button per choice plus Other). Convention shape
	 * — text-only adapters inherit the numbered-text default.
	 */
	sendClarify(
		chatId: string,
		question: string,
		choices: readonly string[] | undefined,
		sessionKey: string,
		pickerId?: number | undefined,
		metadata?: Metadata | undefined,
	): Promise<SendResult>;
	sendPickerText(
		chatId: string,
		text: string,
		metadata?: Metadata | undefined,
	): Promise<SendResult>;
}

/** extension_ui_response body writer (DEC-088; best-effort, never throws). */
export type PickerRespond = (
	uiId: string,
	body: Record<string, unknown>,
) => Promise<void>;

export interface AskBase {
	chatId?: string | undefined;
	sessionKey: string;
	timeoutMs?: number | null | undefined;
	signal?: AbortSignal | undefined;
	metadata?: Metadata | undefined;
}

export interface AskSelectOpts extends AskBase {
	uiId: string;
	title: string;
	options: string[];
	respond: PickerRespond;
}

export interface AskConfirmOpts extends AskBase {
	uiId: string;
	title: string;
	message: string;
	respond: PickerRespond;
}

export interface AskTextOpts extends AskBase {
	uiId: string;
	title: string;
	hint?: string | undefined;
	respond: PickerRespond;
}

export interface AskClarifyOpts extends AskBase {
	uiId: string;
	question: string;
	options: string[];
	respond: PickerRespond;
}

/** Choice-keyboard page size (DEC-089 — provider-page parity, 10 per page). */
export const CHOICE_PAGE_SIZE = 10;

function clampPage(page: number, totalPages: number): number {
	if (!Number.isFinite(page)) return 0;
	return Math.max(0, Math.min(totalPages - 1, Math.trunc(page)));
}

function pageOf<T>(items: readonly T[], page: number, perPage: number): { slice: T[]; page: number; totalPages: number; start: number } {
	const totalPages = Math.max(1, Math.ceil(items.length / perPage));
	const clamped = clampPage(page, totalPages);
	const start = clamped * perPage;
	return { slice: items.slice(start, start + perPage), page: clamped, totalPages, start };
}

/** Title plus ` (N–M of T)` page info when paged (DEC-089 — _format_choice_page parity). */
function pageTitle(base: string, start: number, shown: number, total: number, totalPages: number): string {
	return totalPages > 1 ? `${base} (${start + 1}–${start + shown} of ${total})` : base;
}

const NOT_SUPPORTED_SENDER: PickerSender = {
	sendChoicePicker: async () => ({ success: false, error: "Not supported" }),
	sendSlashConfirm: async () => ({ success: false, error: "Not supported" }),
	sendClarify: async () => ({ success: false, error: "Not supported" }),
	sendModelPicker: async () => ({ success: false, error: "Not supported" }),
	sendPickerText: async () => ({ success: false, error: "Not supported" }),
};

export class PickerService {
	private sender: PickerSender;
	private readonly store: PickerStore;
	/** Capability-miss latch: once a keyboard send reports Not-supported,
	 * every later picker goes straight to the numbered-text lane. */
	private textFallback = false;

	constructor(opts: {
		sender?: PickerSender | undefined;
		store?: PickerStore | undefined;
	}) {
		this.sender = opts.sender ?? NOT_SUPPORTED_SENDER;
		this.store = opts.store ?? new PickerStore();
	}

	/** Late sender binding (the composition root points the shared service
	 * at the first keyboard-capable adapter; until then every ask runs the
	 * numbered-text lane — single-sender: multi-adapter picker routing is
	 * out of scope and noted in the phase report). */
	setSender(sender: PickerSender): void {
		this.sender = sender;
		this.textFallback = false;
	}

	/** True once a keyboard-capable sender is bound. */
	get hasSender(): boolean {
		return this.sender !== NOT_SUPPORTED_SENDER;
	}

	get storeRef(): PickerStore {
		return this.store;
	}

	/** L1 clarify-intercept predicate (Lane C): the agent waits on typed text. */
	hasPendingTextCapture(sessionKey: string): boolean {
		return this.store.pendingForSession(sessionKey) !== null;
	}

	/** Any live picker (keyboard or text) for the session. */
	hasPending(sessionKey: string): boolean {
		return this.store.hasPending(sessionKey);
	}

	/** Current keyboard page per paged choice picker (DEC-089). */
	private readonly choicePages = new Map<number, { sessionKey: string; page: number }>();

	/**
	 * Drop every live picker for a session (parked-picker takeover, /new
	 * scope clearing, shutdown). Waiters settle cancelled. Returns how
	 * many were live.
	 */
	cancelSession(sessionKey: string): number {
		for (const [id, nav] of [...this.choicePages]) {
			if (nav.sessionKey === sessionKey) this.choicePages.delete(id);
		}
		return this.store.cancelSession(sessionKey);
	}

	// ── dialog asks (turn-runner forwarding) ────────────────────────────

	/** RPC select → choice keyboard, else numbered-text fallback. */
	async askSelect(opts: AskSelectOpts): Promise<PickerOutcome> {
		const id = this.store.register({
			sessionKey: opts.sessionKey,
			kind: "select",
			title: opts.title,
			options: opts.options,
			...(opts.timeoutMs !== undefined && opts.timeoutMs !== null
				? { timeoutMs: opts.timeoutMs }
				: {}),
		});
		// DEC-089: paginate the keyboard (absolute cp:<i>, cpv: nav,
		// mx:noop counter, in-place edits) — never a 100-button dump.
		const paged = pageOf(opts.options, 0, CHOICE_PAGE_SIZE);
		this.choicePages.set(id, { sessionKey: opts.sessionKey, page: paged.page });
		const choices: PickerChoice[] = paged.slice.map((option, offset) => {
			const abs = paged.start + offset;
			return {
				value: option,
				label: abs === 0 ? `✓ ${option}` : option,
				...(abs === 0 ? { isCurrent: true as const } : {}),
			};
		});
		const title = pageTitle(opts.title, paged.start, paged.slice.length, opts.options.length, paged.totalPages);
		const pageInfo: ChoicePageInfo = {
			page: paged.page,
			totalPages: paged.totalPages,
			start: paged.start,
			total: opts.options.length,
		};
		const sent = await this.sendKeyboardOrText(
			opts.chatId,
			() =>
				this.sender.sendChoicePicker(
					opts.chatId ?? "",
					title,
					choices,
					opts.sessionKey,
					id,
					opts.metadata,
					pageInfo,
				),
			() => this.selectFallbackText(opts.title, opts.options, 0),
		);
		if (!sent) this.store.settle(id, { kind: "cancelled" });
		const outcome = await this.waitAndRespond(id, opts, (result) =>
			result.kind === "value" ? { value: result.value } : { cancelled: true as const },
		);
		this.choicePages.delete(id);
		return outcome;
	}

	/** RPC confirm → slash-confirm keyboard, else yes/no text fallback. */
	async askConfirm(opts: AskConfirmOpts): Promise<PickerOutcome> {
		const id = this.store.register({
			sessionKey: opts.sessionKey,
			kind: "confirm",
			title: opts.title,
			...(opts.timeoutMs !== undefined && opts.timeoutMs !== null
				? { timeoutMs: opts.timeoutMs }
				: {}),
		});
		const sent = await this.sendKeyboardOrText(
			opts.chatId,
			() =>
				this.sender.sendSlashConfirm(
					opts.chatId ?? "",
					opts.title,
					opts.message,
					opts.sessionKey,
					id,
					opts.metadata,
				),
			() => `${opts.title}\n${opts.message}\n\nReply yes or no.`,
		);
		if (!sent) this.store.settle(id, { kind: "cancelled" });
		return await this.waitAndRespond(id, opts, (outcome) =>
			outcome.kind === "confirmed" ? { confirmed: outcome.confirmed } : { cancelled: true as const },
		);
	}

	/**
	 * Open-text ask (RPC input/editor, unknown methods incl. any future
	 * custom payload, clarify open-ended): question as text, next message
	 * resolves it. Never hangs: timeout/abort settles cancelled.
	 */
	async askText(opts: AskTextOpts): Promise<PickerOutcome> {
		const id = this.store.register({
			sessionKey: opts.sessionKey,
			kind: "text",
			title: opts.title,
			awaitingText: true,
			...(opts.timeoutMs !== undefined && opts.timeoutMs !== null
				? { timeoutMs: opts.timeoutMs }
				: {}),
		});
		let sent = false;
		if (opts.chatId !== undefined && opts.chatId !== "") {
			const hint = opts.hint !== undefined && opts.hint !== "" ? `\n${opts.hint}` : "";
			try {
				const result = await this.sender.sendPickerText(opts.chatId, `❓ ${opts.title}${hint}`, opts.metadata);
				sent = result.success;
			} catch {
				sent = false;
			}
		}
		if (!sent) this.store.settle(id, { kind: "cancelled" });
		return await this.waitAndRespond(id, opts, (outcome) =>
			outcome.kind === "value" ? { value: outcome.value } : { cancelled: true as const },
		);
	}

	/**
	 * Gateway clarify prompt (clarify_gateway ask-side parity): choice
	 * keyboard with the Other escape hatch, else numbered-text fallback.
	 * Non-matching text nags (entry kept); Other flips to free capture.
	 */
	async askClarify(opts: AskClarifyOpts): Promise<PickerOutcome> {
		const id = this.store.register({
			sessionKey: opts.sessionKey,
			kind: "select",
			title: opts.question,
			options: opts.options,
			...(opts.timeoutMs !== undefined && opts.timeoutMs !== null
				? { timeoutMs: opts.timeoutMs }
				: {}),
		});
		const sent = await this.sendKeyboardOrText(
			opts.chatId,
			() =>
				this.sender.sendClarify(
					opts.chatId ?? "",
					opts.question,
					opts.options,
					opts.sessionKey,
					id,
					opts.metadata,
				),
			() => this.selectFallbackText(opts.question, opts.options),
		);
		if (!sent) this.store.settle(id, { kind: "cancelled" });
		return await this.waitAndRespond(id, opts, (outcome) =>
			outcome.kind === "value" ? { value: outcome.value } : { cancelled: true as const },
		);
	}


	// ── tap entry points (adapter-parsed; sync claim ⇒ exactly-once) ─────

	/** Choice tap cp:<index> for a select entry (absolute index). */
	choiceTap(pickerId: number, index: number): TapClaim {
		const entry = this.store.get(pickerId);
		if (entry === null || entry.kind !== "select") {
			return { status: "stale" };
		}
		const option = entry.options[index];
		if (option === undefined) return { status: "stale" };
		const ok = this.store.settle(pickerId, { kind: "value", value: option });
		if (ok) this.choicePages.delete(pickerId);
		return ok ? { status: "resolved", value: option } : { status: "stale" };
	}

	/**
	 * Choice page tap cpv:<page> (DEC-089 — never settles; re-renders the
	 * keyboard in place with the page slice plus ` (N–M of T)` info).
	 */
	choicePageTap(
		pickerId: number,
		page: number,
	): { answer: string; screen: ChoiceNavScreen } | { answer: string; stale: true } {
		const nav = this.choicePages.get(pickerId);
		if (nav === undefined || !this.store.has(pickerId)) {
			return { answer: "This picker has expired — send the command again.", stale: true };
		}
		const entry = this.store.get(pickerId);
		if (entry === null || entry.kind !== "select") {
			return { answer: "This picker has expired — send the command again.", stale: true };
		}
		const paged = pageOf(entry.options, page, CHOICE_PAGE_SIZE);
		nav.page = paged.page;
		const choices: PickerChoice[] = paged.slice.map((option, offset) => {
			const abs = paged.start + offset;
			return {
				value: option,
				label: abs === 0 ? `✓ ${option}` : option,
				...(abs === 0 ? { isCurrent: true as const } : {}),
			};
		});
		return {
			answer: " ",
			screen: {
				title: pageTitle(entry.title, paged.start, paged.slice.length, entry.options.length, paged.totalPages),
				choices,
				page: paged.page,
				totalPages: paged.totalPages,
				start: paged.start,
				total: entry.options.length,
			},
		};
	}

	/** Slash-confirm tap sc:<choice>:<id> (once/always ⇒ true, cancel ⇒ false). */
	confirmTap(pickerId: number, choice: "once" | "always" | "cancel"): { label: string | null } {
		const entry = this.store.get(pickerId);
		if (entry === null || entry.kind !== "confirm") return { label: null };
		const confirmed = choice !== "cancel";
		const ok = this.store.settle(pickerId, { kind: "confirmed", confirmed });
		if (!ok) return { label: null };
		return {
			label:
				choice === "once"
					? "✅ Approved once"
					: choice === "always"
						? "🔒 Always approve"
						: "❌ Cancelled",
		};
	}

	/** Clarify tap cl:<id>:<idx|other> (state kept until terminal). */
	clarifyTap(pickerId: number, idx: number | "other"): TapClaim {
		const entry = this.store.get(pickerId);
		if (entry === null) return { status: "stale" };
		if (idx === "other") {
			const flipped = this.store.markAwaitingText(pickerId);
			return flipped ? { status: "awaiting-text" } : { status: "stale" };
		}
		const choice = entry.options[idx] ?? `choice ${idx + 1}`;
		const ok = this.store.settle(pickerId, { kind: "value", value: choice });
		return ok ? { status: "resolved", value: choice } : { status: "stale" };
	}

	// ── typed-text intercept (run_inbound parity) ─────────────────────────

	/**
	 * Offer one inbound text line to the pending pickers. Leading-slash
	 * command bytes never consume (the command still runs). Returns the
	 * chat reply when consumed, null when the text is not picker input.
	 */
	takeText(sessionKey: string, text: string): string | null {
		if (text.trimStart().startsWith("/")) return null;
		const trimmed = text.trim();
		if (trimmed === "") return null;
		// Open-text entries first (open-ended, clarify-Other, text fallback).
		const open = this.store.pendingForSession(sessionKey);
		if (open !== null) {
			if (this.store.settle(open.id, { kind: "value", value: text })) {
				return `✓ ${trimmed.slice(0, 80)}`;
			}
			return null;
		}
		// Choice prompts: number or label match resolves; anything else is
		// consumed with a nag so a parked turn never double-dispatches.
		const choice = this.store.pendingForSession(sessionKey, { includeChoicePrompts: true });
		if (choice === null) return null;
		if (choice.kind === "confirm") {
			const verdict = parseYesNo(trimmed);
			if (verdict !== null) {
				if (this.store.settle(choice.id, { kind: "confirmed", confirmed: verdict })) {
					return verdict ? "✅ Approved" : "❌ Cancelled";
				}
				return null;
			}
			return "Reply yes or no.";
		}
		const match = matchOption(choice.options, trimmed);
		if (match !== null) {
			if (this.store.settle(choice.id, { kind: "value", value: match })) {
				this.choicePages.delete(choice.id);
				return `✓ ${match.slice(0, 80)}`;
			}
			return null;
		}
		// DEC-089: text-lane paging — `next`/`more`/`prev`/`back` turn the
		// numbered page without settling (label/number matches win first,
		// so an option literally named "next" still resolves).
		const turned = this.turnChoiceTextPage(choice.id, trimmed);
		if (turned !== null) return turned;
		const total = choice.options.length;
		const nav = this.choicePages.get(choice.id);
		const totalPages = nav !== undefined
			? Math.max(1, Math.ceil(total / CHOICE_PAGE_SIZE))
			: 1;
		return totalPages > 1
			? `Reply with a number 1–${total} (or 'next' for more).`
			: `Reply with a number 1–${total}.`;
	}

	/**
	 * Text-lane page turn (DEC-089): `next`/`more` advances, `prev`/`back`
	 * retreats, returning the numbered page text with the entry kept alive.
	 * Null when the word is not a page command or the picker is single-page.
	 */
	private turnChoiceTextPage(pickerId: number, word: string): string | null {
		const lowered = word.toLowerCase();
		const dir = lowered === "next" || lowered === "more" ? 1 : lowered === "prev" || lowered === "back" ? -1 : 0;
		if (dir === 0) return null;
		const nav = this.choicePages.get(pickerId);
		if (nav === undefined || !this.store.has(pickerId)) return null;
		const entry = this.store.get(pickerId);
		if (entry === null || entry.kind !== "select") return null;
		const totalPages = Math.max(1, Math.ceil(entry.options.length / CHOICE_PAGE_SIZE));
		if (totalPages <= 1) return null;
		const next = clampPage(nav.page + dir, totalPages);
		nav.page = next;
		return this.selectFallbackText(entry.title, entry.options, next);
	}

	// ── internals ─────────────────────────────────────────────────────────

	/**
	 * Keyboard send with text fallback. A missing chat id, a manifest
	 * interactivePickers:false, or a Not-supported send all land in the
	 * numbered-text lane; the MISS latches so later pickers skip the
	 * keyboard attempt outright.
	 */
	private async sendKeyboardOrText(
		chatId: string | undefined,
		sendKeyboard: () => Promise<SendResult>,
		fallbackText: () => string,
	): Promise<boolean> {
		if (this.textFallback || chatId === undefined || chatId === "" || this.sender.interactivePickers === false) {
			this.textFallback ||= this.sender.interactivePickers === false;
			return this.sendText(chatId, fallbackText());
		}
		let result: SendResult;
		try {
			result = await sendKeyboard();
		} catch {
			result = { success: false, error: "Not supported" };
		}
		if (!result.success) {
			this.textFallback = true;
			return this.sendText(chatId, fallbackText());
		}
		return true;
	}

	private async sendText(chatId: string | undefined, text: string): Promise<boolean> {
		if (chatId === undefined || chatId === "") return false;
		try {
			const result = await this.sender.sendPickerText(chatId, text);
			return result.success;
		} catch {
			return false;
		}
	}

	private async waitAndRespond(
		id: number,
		opts: { uiId: string; respond: PickerRespond; signal?: AbortSignal | undefined },
		toBody: (outcome: PickerOutcome) => Record<string, unknown>,
	): Promise<PickerOutcome> {
		const outcome = await this.store.waitFor(id, signalOf(opts));
		// DEC-088: every dialog settles exactly one response — value,
		// confirmed, or cancelled on timeout/abort/dead-child. Best-effort:
		// a dead child rejects the write and the turn still settles.
		try {
			await opts.respond(opts.uiId, toBody(outcome));
		} catch {
			/* the waiter already settled; the child is gone */
		}
		return outcome;
	}

	private selectFallbackText(title: string, options: readonly string[], page = 0): string {
		// DEC-089: paged fallback — absolute 1-based numbers plus page info,
		// never an unbounded dump.
		const paged = pageOf(options, page, CHOICE_PAGE_SIZE);
		const numbered = paged.slice.map((option, offset) => `${paged.start + offset + 1}. ${option}`);
		const info = paged.totalPages > 1 ? ` (${paged.start + 1}–${paged.start + paged.slice.length} of ${options.length})` : "";
		const hint = paged.totalPages > 1 ? "Reply with the number (or 'next' for more)." : "Reply with the number.";
		return `${title}${info}\n\n${numbered.join("\n")}\n\n${hint}`;
	}

}

function signalOf(opts: { signal?: AbortSignal | undefined }): { signal?: AbortSignal | undefined } {
	return opts.signal !== undefined ? { signal: opts.signal } : {};
}

/** Number ("2"), or case-insensitive label match; null when neither. */
function matchOption(options: readonly string[], text: string): string | null {
	const numeric = /^\d+$/.test(text) ? Number(text) : NaN;
	if (Number.isInteger(numeric) && numeric >= 1 && numeric <= options.length) {
		return options[numeric - 1] ?? null;
	}
	const wanted = text.toLowerCase();
	for (const option of options) {
		if (option.toLowerCase() === wanted) return option;
	}
	return null;
}

function parseYesNo(text: string): boolean | null {
	const word = text.trim().toLowerCase();
	if (["yes", "y", "approve", "ok", "1"].includes(word)) return true;
	if (["no", "n", "deny", "cancel", "0"].includes(word)) return false;
	return null;
}



