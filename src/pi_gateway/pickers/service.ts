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

/** Child-commit seam (runner-bound closures; fakes in tests). */
export interface PickerCommit {
	commitModel(
		sessionKey: string,
		provider: string,
		modelId: string,
	): Promise<string>;
	commitResume(sessionKey: string, file: string): Promise<string>;
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

export interface AskModelOpts extends AskBase {
	catalog: Array<{ provider: string; id: string }>;
	current: { provider: string; id: string } | null;
}

export interface ResumeSessionRow {
	id: string;
	file: string;
	label: string;
}

export interface AskResumeOpts extends AskBase {
	sessions: ResumeSessionRow[];
}

export type ModelPickOutcome =
	| { status: "switched"; provider: string; id: string; text: string }
	| { status: "cancelled" };

export type ResumePickOutcome =
	| { status: "resumed"; id: string; text: string }
	| { status: "cancelled" };

/** Hermes _PROVIDER_PAGE_SIZE/_MODEL_PAGE_SIZE (telegram adapter class attrs). */
export const MODEL_PROVIDER_PAGE_SIZE = 10;
export const MODEL_PAGE_SIZE = 8;

/** Bare /resume lists at most this many sessions (_handle_resume_command parity). */
export const RESUME_LIST_LIMIT = 10;

interface ModelPickerState {
	pickerId: number;
	sessionKey: string;
	providers: ModelProviderRow[];
	current: { provider: string; id: string } | null;
	selectedSlug: string | null;
	modelList: string[];
	providerPage: number;
	modelPage: number;
	settled: boolean;
}

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

const NOT_SUPPORTED_SENDER: PickerSender = {
	sendChoicePicker: async () => ({ success: false, error: "Not supported" }),
	sendSlashConfirm: async () => ({ success: false, error: "Not supported" }),
	sendClarify: async () => ({ success: false, error: "Not supported" }),
	sendModelPicker: async () => ({ success: false, error: "Not supported" }),
	sendPickerText: async () => ({ success: false, error: "Not supported" }),
};

export class PickerService {
	private sender: PickerSender;
	private readonly commit: PickerCommit;
	private readonly store: PickerStore;
	/**
	 * Expensive-model warner (combined_selection_warning parity). Default
	 * null ⇒ direct switch, exactly Hermes with warning=null. The host
	 * catalog carries no pricing, so production injects no warner; tests
	 * prove the mm→mc confirm path through this hook.
	 */
	private readonly warnForModel: ((provider: string, modelId: string) => string | null) | null;
	/** Capability-miss latch: once a keyboard send reports Not-supported,
	 * every later picker goes straight to the numbered-text lane. */
	private textFallback = false;

	constructor(opts: {
		sender?: PickerSender | undefined;
		commit: PickerCommit;
		store?: PickerStore | undefined;
		warnForModel?: ((provider: string, modelId: string) => string | null) | undefined;
	}) {
		this.sender = opts.sender ?? NOT_SUPPORTED_SENDER;
		this.commit = opts.commit;
		this.store = opts.store ?? new PickerStore();
		this.warnForModel = opts.warnForModel ?? null;
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

	/**
	 * Drop every live picker for a session (parked-picker takeover, /new
	 * scope clearing, shutdown). Waiters settle cancelled. Returns how
	 * many were live.
	 */
	cancelSession(sessionKey: string): number {
		for (const [id, state] of [...this.modelStates]) {
			if (state.sessionKey === sessionKey) {
				state.settled = true;
				this.modelStates.delete(id);
				this.modelResults.delete(id);
			}
		}
		for (const [id, state] of [...this.resumeStates]) {
			if (state.sessionKey === sessionKey) {
				state.settled = true;
				this.resumeStates.delete(id);
			}
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
		const choices: PickerChoice[] = opts.options.map((option, index) => ({
			value: option,
			label: index === 0 ? `✓ ${option}` : option,
			...(index === 0 ? { isCurrent: true as const } : {}),
		}));
		await this.sendKeyboardOrText(
			opts.chatId,
			() =>
				this.sender.sendChoicePicker(
					opts.chatId ?? "",
					opts.title,
					choices,
					opts.sessionKey,
					id,
					opts.metadata,
				),
			() => this.selectFallbackText(opts.title, opts.options),
		);
		return await this.waitAndRespond(id, opts, (outcome) =>
			outcome.kind === "value" ? { value: outcome.value } : { cancelled: true as const },
		);
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
		await this.sendKeyboardOrText(
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
		if (opts.chatId !== undefined && opts.chatId !== "") {
			const hint = opts.hint !== undefined && opts.hint !== "" ? `\n${opts.hint}` : "";
			await this.sender.sendPickerText(opts.chatId, `❓ ${opts.title}${hint}`, opts.metadata).catch(() => ({ success: false as const }));
		}
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
		await this.sendKeyboardOrText(
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
		return await this.waitAndRespond(id, opts, (outcome) =>
			outcome.kind === "value" ? { value: outcome.value } : { cancelled: true as const },
		);
	}

	// ── bare-command pickers (runner-owned flows, no extension response) ──

	/** Bare /model → provider drill-down; tap commits via the committer. */
	async askModel(opts: AskModelOpts): Promise<ModelPickOutcome> {
		const providers = toProviderRows(opts.catalog, opts.current);
		if (providers.length === 0) return { status: "cancelled" };
		const id = this.store.register({
			sessionKey: opts.sessionKey,
			kind: "model",
			title: "Select model",
			...(opts.timeoutMs !== undefined && opts.timeoutMs !== null
				? { timeoutMs: opts.timeoutMs }
				: {}),
		});
		const state: ModelPickerState = {
			pickerId: id,
			sessionKey: opts.sessionKey,
			providers,
			current: opts.current,
			selectedSlug: null,
			modelList: [],
			providerPage: 0,
			modelPage: 0,
			settled: false,
		};
		this.modelStates.set(id, state);
		const screen = this.providersScreen(state, 0);
		await this.sendKeyboardOrText(
			opts.chatId,
			() => this.sender.sendModelPicker(opts.chatId ?? "", screen, opts.sessionKey, id, opts.metadata),
			() => this.modelFallbackText(providers, opts.current),
		);
		if (this.textFallback) {
			// Text lane: the typed model ref resolves through takeText below.
			this.store.markAwaitingText(id);
		}
		const outcome = await this.store.waitFor(id, signalOf(opts));
		this.modelStates.delete(id);
		const tapped = this.modelResults.get(id);
		this.modelResults.delete(id);
		if (tapped !== undefined) return { status: "switched", ...tapped };
		if (outcome.kind === "value") {
			const parsed = parseModelRef(outcome.value, opts.catalog);
			if (parsed === null) return { status: "cancelled" };
			try {
				const text = await this.commit.commitModel(opts.sessionKey, parsed.provider, parsed.id);
				return { status: "switched", provider: parsed.provider, id: parsed.id, text };
			} catch {
				return { status: "cancelled" };
			}
		}
		return { status: "cancelled" };
	}

	/** Bare /resume → ≤10 recent sessions; tap switches plus clears scope. */
	async askResume(opts: AskResumeOpts): Promise<ResumePickOutcome> {
		const rows = opts.sessions.slice(0, RESUME_LIST_LIMIT);
		if (rows.length === 0) return { status: "cancelled" };
		const id = this.store.register({
			sessionKey: opts.sessionKey,
			kind: "resume",
			title: "Resume session",
			options: rows.map((row) => row.label),
			...(opts.timeoutMs !== undefined && opts.timeoutMs !== null
				? { timeoutMs: opts.timeoutMs }
				: {}),
		});
		this.resumeStates.set(id, { sessionKey: opts.sessionKey, rows, settled: false });
		const choices: PickerChoice[] = rows.map((row, index) => ({
			value: row.label,
			label: index === 0 ? `✓ ${row.label}` : row.label,
			...(index === 0 ? { isCurrent: true as const } : {}),
		}));
		await this.sendKeyboardOrText(
			opts.chatId,
			() =>
				this.sender.sendChoicePicker(
					opts.chatId ?? "",
					"Resume session",
					choices,
					opts.sessionKey,
					id,
					opts.metadata,
				),
			() => this.resumeFallbackText(rows),
		);
		const outcome = await this.store.waitFor(id, signalOf(opts));
		const state = this.resumeStates.get(id);
		this.resumeStates.delete(id);
		if (outcome.kind === "value" && state !== undefined) {
			const row = matchResumeRow(state.rows, outcome.value);
			if (row === null) return { status: "cancelled" };
			try {
				const text = await this.commit.commitResume(opts.sessionKey, row.file);
				// Tap switches PLUS clears scope: no stale picker survives the rebind.
				this.store.cancelSession(opts.sessionKey);
				return { status: "resumed", id: row.id, text };
			} catch {
				return { status: "cancelled" };
			}
		}
		return { status: "cancelled" };
	}

	private readonly modelStates = new Map<number, ModelPickerState>();
	private readonly resumeStates = new Map<number, { sessionKey: string; rows: ResumeSessionRow[]; settled: boolean }>();
	/** Tap-lane commit results, consumed once by the askModel waiter
	 * (single-commit ownership: the tap commits for its result screen,
	 * the waiter commits only for the text lane). */
	private readonly modelResults = new Map<number, { provider: string; id: string; text: string }>();

	// ── tap entry points (adapter-parsed; sync claim ⇒ exactly-once) ─────

	/** Choice tap cp:<index> for a select or resume entry. */
	choiceTap(pickerId: number, index: number): TapClaim {
		const resume = this.resumeStates.get(pickerId);
		if (resume !== undefined && !resume.settled) {
			const row = resume.rows[index];
			if (row === undefined) return { status: "stale" };
			resume.settled = true;
			const ok = this.store.settle(pickerId, { kind: "value", value: row.label });
			return ok ? { status: "resolved", value: row.label } : { status: "stale" };
		}
		const entry = this.store.get(pickerId);
		if (entry === null || (entry.kind !== "select" && entry.kind !== "resume")) {
			return { status: "stale" };
		}
		const option = entry.options[index];
		if (option === undefined) return { status: "stale" };
		const ok = this.store.settle(pickerId, { kind: "value", value: option });
		return ok ? { status: "resolved", value: option } : { status: "stale" };
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

	/**
	 * Model drill-down tap. Returns the next screen to render; the terminal
	 * commit performs the switch FIRST (resolve-first parity: a tap landing
	 * after expiry must NOT claim success) and the result screen reflects
	 * what actually happened.
	 */
	async modelTap(
		pickerId: number,
		action: ModelTapAction,
	): Promise<{ answer: string; screen: ModelScreen } | { answer: string; stale: true }> {
		const state = this.modelStates.get(pickerId);
		if (state === undefined || state.settled || !this.store.has(pickerId)) {
			return { answer: "This picker has expired — run /model again.", stale: true };
		}
		switch (action.kind) {
			case "provider": {
				const provider = state.providers.find((row) => row.slug === action.slug);
				if (provider === undefined) {
					return { answer: "Unknown provider.", stale: true };
				}
				state.selectedSlug = provider.slug;
				state.modelList = [...provider.models];
				state.modelPage = 0;
				return { answer: " ", screen: this.modelsScreen(state, 0) };
			}
			case "providerPage":
				return { answer: " ", screen: this.providersScreen(state, action.page) };
			case "modelPage": {
				if (state.selectedSlug === null) {
					return { answer: "Pick a provider first.", stale: true };
				}
				return { answer: " ", screen: this.modelsScreen(state, action.page) };
			}
			case "model": {
				const modelId = state.modelList[action.index];
				if (modelId === undefined || state.selectedSlug === null) {
					return { answer: "Unknown model.", stale: true };
				}
				const warning =
					this.warnForModel === null
						? null
						: this.warnForModel(state.selectedSlug, modelId);
				if (warning !== null) {
					return {
						answer: "Confirm this model.",
						screen: { kind: "warn", text: `⚠️ ${warning}`, index: action.index },
					};
				}
				return await this.commitModelTap(state, action.index);
			}
			case "confirm":
				return await this.commitModelTap(state, action.index);
			case "back": {
				state.selectedSlug = null;
				state.modelList = [];
				return { answer: " ", screen: this.providersScreen(state, state.providerPage) };
			}
			case "cancel": {
				state.settled = true;
				this.modelStates.delete(pickerId);
				this.store.settle(pickerId, { kind: "cancelled" });
				return { answer: " ", screen: { kind: "result", text: "Model picker cancelled." } };
			}
		}
	}

	private async commitModelTap(
		state: ModelPickerState,
		index: number,
	): Promise<{ answer: string; screen: ModelScreen } | { answer: string; stale: true }> {
		const modelId = state.modelList[index];
		const slug = state.selectedSlug;
		if (modelId === undefined || slug === null) {
			return { answer: "Unknown model.", stale: true };
		}
		state.settled = true;
		this.modelStates.delete(state.pickerId);
		// Claim FIRST (synchronous: a concurrent second tap or a timeout
		// racing the commit finds no state and goes stale), commit second:
		// the waiter wakes inside settle below, AFTER the single-commit
		// record exists, so it never takes the text lane and commits twice.
		// A timeout landing mid-commit still wins the waiter (cancelled)
		// while this tap reports expiry instead of a false "Switched"
		// (#63501 parity) — settle returns False and the result is dropped.
		let text: string;
		try {
			text = await this.commit.commitModel(state.sessionKey, slug, modelId);
		} catch (error) {
			text = `Model switch failed: ${error instanceof Error ? error.message : String(error)}`;
		}
		// Single-commit record for the askModel waiter (text lane commits
		// there instead); the waiter consumes this exactly once.
		this.modelResults.set(state.pickerId, { provider: slug, id: modelId, text });
		const claimed = this.store.settle(state.pickerId, { kind: "value", value: `${slug}/${modelId}` });
		if (!claimed) {
			this.modelResults.delete(state.pickerId);
			return { answer: "This picker has expired — run /model again.", stale: true };
		}
		return { answer: "Switched.", screen: { kind: "result", text } };
	}

	private providersScreen(state: ModelPickerState, page: number): Extract<ModelScreen, { kind: "providers" }> {
		const paged = pageOf(state.providers, page, MODEL_PROVIDER_PAGE_SIZE);
		state.providerPage = paged.page;
		const current =
			state.current !== null ? `${state.current.provider}/${state.current.id}` : "(none)";
		return {
			kind: "providers",
			text: `⚙ Model picker\n\nCurrent: ${current}\n\nSelect a provider${paged.totalPages > 1 ? ` (${paged.start + 1}–${paged.start + paged.slice.length} of ${state.providers.length})` : ""}`,
			providers: paged.slice,
			page: paged.page,
			totalPages: paged.totalPages,
		};
	}

	private modelsScreen(state: ModelPickerState, page: number): Extract<ModelScreen, { kind: "models" }> {
		const paged = pageOf(state.modelList, page, MODEL_PAGE_SIZE);
		state.modelPage = paged.page;
		const provider = state.providers.find((row) => row.slug === state.selectedSlug);
		return {
			kind: "models",
			text: `⚙ Model picker\n\n${provider?.name ?? state.selectedSlug ?? ""}${paged.totalPages > 1 ? ` (${paged.start + 1}–${paged.start + paged.slice.length} of ${state.modelList.length})` : ""}\n\nSelect a model`,
			providerName: provider?.name ?? state.selectedSlug ?? "",
			models: paged.slice,
			start: paged.start,
			page: paged.page,
			totalPages: paged.totalPages,
		};
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
		if (choice.kind === "model") {
			// Text-lane model ref (provider/id, bare id, or 1-based number).
			if (this.store.settle(choice.id, { kind: "value", value: trimmed })) {
				return `✓ ${trimmed.slice(0, 80)}`;
			}
			return null;
		}
		const match = matchOption(choice.options, trimmed);
		if (match !== null) {
			if (this.store.settle(choice.id, { kind: "value", value: match })) {
				return `✓ ${match.slice(0, 80)}`;
			}
			return null;
		}
		return `Reply with a number 1–${choice.options.length}.`;
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
	): Promise<void> {
		if (this.textFallback || chatId === undefined || chatId === "" || this.sender.interactivePickers === false) {
			this.textFallback ||= this.sender.interactivePickers === false;
			await this.sendText(chatId, fallbackText());
			return;
		}
		let result: SendResult;
		try {
			result = await sendKeyboard();
		} catch {
			result = { success: false, error: "Not supported" };
		}
		if (!result.success) {
			this.textFallback = true;
			await this.sendText(chatId, fallbackText());
		}
	}

	private async sendText(chatId: string | undefined, text: string): Promise<void> {
		if (chatId === undefined || chatId === "") return;
		await this.sender.sendPickerText(chatId, text).catch(() => ({ success: false as const }));
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

	private selectFallbackText(title: string, options: string[]): string {
		const numbered = options.map((option, index) => `${index + 1}. ${option}`);
		return `${title}\n\n${numbered.join("\n")}\n\nReply with the number.`;
	}

	private resumeFallbackText(rows: ResumeSessionRow[]): string {
		const numbered = rows.map((row, index) => `${index + 1}. ${row.label}`);
		return `Resume session\n\n${numbered.join("\n")}\n\nReply with the number.`;
	}

	private modelFallbackText(
		providers: ModelProviderRow[],
		current: { provider: string; id: string } | null,
	): string {
		const lines = providers.flatMap((provider) =>
			provider.models.map((model) => `${provider.slug}/${model}`),
		);
		const at = current !== null ? `${current.provider}/${current.id}` : "(none)";
		return `Current model: ${at}\nAvailable: ${lines.join(", ")}\n\nReply with provider/id.`;
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

function matchResumeRow(rows: ResumeSessionRow[], value: string): ResumeSessionRow | null {
	const numeric = /^\d+$/.test(value.trim()) ? Number(value.trim()) : NaN;
	if (Number.isInteger(numeric) && numeric >= 1 && numeric <= rows.length) {
		return rows[numeric - 1] ?? null;
	}
	const byLabel = rows.find((row) => row.label === value);
	if (byLabel !== undefined) return byLabel;
	const folded = value.toLowerCase();
	return rows.find((row) => row.label.toLowerCase() === folded) ?? null;
}

/** Model ref: provider/id, bare id (unique), or 1-based catalog number. */
function parseModelRef(
	ref: string,
	catalog: Array<{ provider: string; id: string }>,
): { provider: string; id: string } | null {
	const trimmed = ref.trim();
	const numeric = /^\d+$/.test(trimmed) ? Number(trimmed) : NaN;
	if (Number.isInteger(numeric) && numeric >= 1 && numeric <= catalog.length) {
		const hit = catalog[numeric - 1];
		return hit !== undefined ? { provider: hit.provider, id: hit.id } : null;
	}
	const slashAt = trimmed.indexOf("/");
	if (slashAt !== -1) {
		const provider = trimmed.slice(0, slashAt).trim();
		const id = trimmed.slice(slashAt + 1).trim();
		if (provider === "" || id === "") return null;
		const exact = catalog.some((row) => row.provider === provider && row.id === id);
		return exact ? { provider, id } : null;
	}
	const hits = catalog.filter((row) => row.id === trimmed);
	if (hits.length !== 1) return null;
	const hit = hits[0] as { provider: string; id: string };
	return { provider: hit.provider, id: hit.id };
}

function toProviderRows(
	catalog: Array<{ provider: string; id: string }>,
	current: { provider: string; id: string } | null,
): ModelProviderRow[] {
	const bySlug = new Map<string, { name: string; models: string[] }>();
	for (const row of catalog) {
		const bucket = bySlug.get(row.provider) ?? { name: row.provider, models: [] };
		if (!bucket.models.includes(row.id)) bucket.models.push(row.id);
		bySlug.set(row.provider, bucket);
	}
	return [...bySlug].map(([slug, bucket]) => ({
		slug,
		name: bucket.name,
		models: bucket.models,
		totalModels: bucket.models.length,
		isCurrent: current !== null && current.provider === slug,
	}));
}
