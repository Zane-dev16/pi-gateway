// manager.ts — per-session /loop state + tick decisions, the orchestration
// surface every driver talks to (CLI/gateway/TUI parity of
// hermes_cli/loops.py:LoopManager + its shared dispatch).
//
// Ported semantics, bug-for-bug against Hermes truth at /tmp/hermes-upstream:
//   loops.py:parse_interval_token        → parseIntervalToken
//   loops.py:parse_loop_args             → parseLoopArgs
//   loops.py:format_interval             → formatInterval (Python round-half-even)
//   loops.py:min_interval_seconds et al  → resolveMinIntervalSeconds family
//                                          (loops.* config knobs, clamped,
//                                          garbage fails safe to defaults)
//   loops.py:response_signals_complete   → responseSignalsComplete
//   loops.py:_digest_response            → digestResponse (sha256 over
//                                          timestamp-stripped normalized text)
//   loops.py:WAKEUP_PROMPT(_WITH_UNTIL)_TEMPLATE → byte-exact templates
//   loops.py:LoopManager                 → LoopManager
//   loops.py:dispatch_loop_command       → dispatchLoopCommand ({output,created})
//   gateway/slash_commands.py:_handle_loop_command route capture
//                                        → routeFromSource
//   gateway/run.py:_busy_loop_command    → isLoopMidrunControlArg +
//                                          LOOP_BUSY_SET_REJECT_TEXT
//
// Tick lifecycle contract (module docstring parity): fire_tick() CLAIMS a due
// tick and returns the wakeup text to inject; drivers MUST follow up with
// completeTick() after the injected turn's response — or abandonTick() when
// injection failed. awaiting_response keeps a tick from double-firing while
// its turn runs. All state persists through pi_state loop rows
// (state_meta `loop:<session_id>`) so /resume picks loops back up.
//
// The --until judge is an AUX-LLM call in Hermes (goals.judge_goal); pi has no
// goals subsystem yet, so it is an injected `judge` hook. Absent/throwing
// judge = the Python ImportError arm: fail-open CONTINUE ("a broken judge
// never wedges the loop; the tick budget is the backstop") with reason
// "judge unavailable".

import { createHash } from "node:crypto";

import type Database from "better-sqlite3";

import {
	type LoopRoute,
	type LoopState,
	type LoopStatus,
	LOOP_DEFAULT_MAX_TICKS,
	loadLoopRow,
	saveLoopRow,
} from "../../pi_state/index.js";
import type { SessionSource } from "../../pi_gateway/resolution/session-key.js";
import type { GatewayClock } from "./clock.js";
import { systemClock } from "./clock.js";

// ──────────────────────────────────────────────────────────────────────
// Constants & defaults (loops.py header block)
// ──────────────────────────────────────────────────────────────────────

/**
 * Floor for fixed intervals. Claude Code allows 30s; anything tighter is
 * almost always an accident that burns tokens polling unchanged state.
 * Overridable via loops.min_interval_seconds (still clamped ≥ 5).
 */
export const DEFAULT_MIN_INTERVAL_SECONDS = 30;

/** Backstop tick budget so an unattended loop can't run forever by default. */
export const DEFAULT_MAX_TICKS = 100;

/** Self-paced mode: start at the floor, double while replies are unchanged. */
export const DEFAULT_SELF_PACED_FLOOR_SECONDS = 60;
export const DEFAULT_SELF_PACED_CEILING_SECONDS = 15 * 60;

/**
 * The completion sentinel the wakeup prompt teaches the agent to emit when
 * the loop's task is finished or no longer applicable.
 */
export const LOOP_COMPLETE_MARKER = "LOOP_COMPLETE";

/**
 * Matches the marker on its own line (possibly with surrounding whitespace
 * or trailing punctuation the model added despite instructions).
 */
const LOOP_COMPLETE_RE = /^\s*LOOP_COMPLETE\s*[.!]?\s*$/im;

/**
 * Interval token: 30s / 5m / 2h / 1h30m (compound units allowed, at least
 * one). A bare number is NOT an interval (too easy to collide with prompt
 * text like `/loop 3 things to check`) — units are required.
 */
const INTERVAL_TOKEN_RE = /^(?=\d)(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$/i;

export const WAKEUP_PROMPT_TEMPLATE = `[/loop wakeup #{tick}{cadence}]
Recurring task: {prompt}

This is an automatic wakeup from the /loop the user set. Perform the task now against the CURRENT state (re-check files, processes, or services fresh — do not assume anything from earlier iterations still holds). Report concisely what you found or did this iteration.
If the task is now complete, no longer applicable, or the thing you were watching has finished, say so and end your reply with ${LOOP_COMPLETE_MARKER} on its own line — that stops the loop.`;

export const WAKEUP_PROMPT_WITH_UNTIL_TEMPLATE = `[/loop wakeup #{tick}{cadence}]
Recurring task: {prompt}

Stop condition: {until}

This is an automatic wakeup from the /loop the user set. Perform the task now against the CURRENT state (re-check files, processes, or services fresh — do not assume anything from earlier iterations still holds). Report concisely what you found or did this iteration, and show concrete evidence of the stop condition's status.
If the stop condition is met, or the task is no longer applicable, say so and end your reply with ${LOOP_COMPLETE_MARKER} on its own line — that stops the loop.`;

// ──────────────────────────────────────────────────────────────────────
// Interval parsing
// ──────────────────────────────────────────────────────────────────────

/** Total seconds for a compact interval token, or null when not an interval. */
export function parseIntervalToken(token: string): number | null {
	if (!token) return null;
	const m = INTERVAL_TOKEN_RE.exec(token.trim());
	if (!m) return null;
	const h = Number(m[1] ?? 0);
	const mnt = Number(m[2] ?? 0);
	const s = Number(m[3] ?? 0);
	const total = h * 3600 + mnt * 60 + s;
	return total > 0 ? total : null;
}

export interface ParsedLoopArgs {
	intervalSeconds: number | null;
	prompt: string;
	times: number;
	until: string;
	error: string | null;
}

/** Python raw.split(None, 1) — head word + remainder (leading ws dropped). */
function splitHead(raw: string): [string, string | null] {
	const trimmed = raw.replace(/^\s+/, "");
	const m = /^(\S+)(?:\s+([\s\S]*))?$/.exec(trimmed);
	if (!m) return ["", null];
	return [m[1]!, m[2] ?? null];
}

/**
 * Parse the argument string of `/loop [interval] <prompt> [flags]`.
 *
 * Recognized shapes:
 *   /loop 5m check the deploy status
 *   /loop every 10m /babbe-prs            (every-sugar)
 *   /loop keep fixing tests until green   (self-paced — no interval token)
 *   /loop 2m poll CI --times 30
 *   /loop 5m watch the queue --until queue depth reaches zero
 *
 * intervalSeconds null ⇒ self-paced. error is set for unusable input (empty,
 * interval-only prompt, bad --times). Trailing flags pull FIRST so an
 * interval-looking token inside the --until clause can't confuse the front
 * parse; --times may follow --until (it is matched before --until consumes
 * the rest of the line).
 */
export function parseLoopArgs(text: string): ParsedLoopArgs {
	let raw = (text ?? "").trim();
	const result: ParsedLoopArgs = {
		intervalSeconds: null,
		prompt: "",
		times: 0,
		until: "",
		error: null,
	};
	if (!raw) {
		result.error = "empty";
		return result;
	}

	let times = 0;
	let until = "";

	const mTimes = /\s--times\s+(\S+)/.exec(raw);
	if (mTimes !== null) {
		const token = mTimes[1] ?? "";
		if (!/^[+-]?\d+$/.test(token.trim())) {
			result.error = `--times expects a positive integer, got '${token}'`;
			return result;
		}
		times = Number.parseInt(token.trim(), 10);
		if (!(times >= 1)) {
			result.error = `--times expects a positive integer, got '${token}'`;
			return result;
		}
		raw = (
			raw.slice(0, mTimes.index) + raw.slice(mTimes.index + mTimes[0].length)
		).trim();
	}

	const mUntil = /\s--until\s+([\s\S]+)$/.exec(raw);
	if (mUntil !== null) {
		until = (mUntil[1] ?? "").trim();
		raw = raw.slice(0, mUntil.index).trim();
	}

	// Leading "every" sugar: /loop every 5m <prompt>
	let tokens = splitHead(raw);
	if (tokens[0].toLowerCase() === "every" && tokens[1] !== null) {
		raw = tokens[1];
		tokens = splitHead(raw);
	} else if (tokens[0].toLowerCase() === "every") {
		raw = "";
		tokens = ["", null];
	}

	let interval: number | null = null;
	if (tokens[0] !== "") {
		const maybe = parseIntervalToken(tokens[0]);
		if (maybe !== null) {
			interval = maybe;
			raw = tokens[1] === null ? "" : tokens[1].trim();
		}
	}

	if (!raw) {
		result.error = "missing prompt (usage: /loop [interval] <prompt>)";
		return result;
	}

	result.intervalSeconds = interval;
	result.prompt = raw;
	result.times = times;
	result.until = until;
	return result;
}

/** Python round() — half-to-even (banker's rounding), unlike Math.round. */
function pyRound(x: number): number {
	const floor = Math.floor(x);
	const diff = x - floor;
	if (diff > 0.5) return floor + 1;
	if (diff < 0.5) return floor;
	return floor % 2 === 0 ? floor : floor + 1;
}

/** Render seconds as a compact human interval (`90` → `1m30s`). */
export function formatInterval(seconds: number): string {
	const s = Math.max(0, pyRound(seconds));
	const h = Math.floor(s / 3600);
	const rem = s % 3600;
	const m = Math.floor(rem / 60);
	const sec = rem % 60;
	const parts: string[] = [];
	if (h) parts.push(`${h}h`);
	if (m) parts.push(`${m}m`);
	if (sec || parts.length === 0) parts.push(`${sec}s`);
	return parts.join("");
}

// ──────────────────────────────────────────────────────────────────────
// Config (loops.py `_loops_config` section readers; clamped, fail-safe)
// ──────────────────────────────────────────────────────────────────────

/** The `loops:` config section (pi composition reads config.yaml). */
export interface LoopsConfig {
	min_interval_seconds?: unknown;
	max_ticks?: unknown;
	self_paced_floor_seconds?: unknown;
	self_paced_ceiling_seconds?: unknown;
}

export type LoopsConfigOf = () => LoopsConfig | undefined | null;

/** Python int() coercion for one knob value; garbage ⇒ fallback (except arm). */
function readIntKnobValue(value: unknown, fallback: number): number {
	if (value === undefined || value === null || value === "") return fallback;
	if (typeof value === "number") {
		return Number.isFinite(value) ? Math.trunc(value) : fallback;
	}
	if (typeof value === "string") {
		const trimmed = value.trim();
		if (/^[+-]?\d+$/.test(trimmed)) return Number.parseInt(trimmed, 10);
	}
	return fallback;
}

export function resolveMinIntervalSeconds(configOf?: LoopsConfigOf): number {
	try {
		const value = readIntKnob(
			configOf,
			"min_interval_seconds",
			DEFAULT_MIN_INTERVAL_SECONDS,
		);
		return Math.max(5, value);
	} catch {
		return DEFAULT_MIN_INTERVAL_SECONDS;
	}
}

export function resolveMaxTicksDefault(configOf?: LoopsConfigOf): number {
	try {
		const value = readIntKnob(configOf, "max_ticks", DEFAULT_MAX_TICKS);
		return Math.max(0, value);
	} catch {
		return DEFAULT_MAX_TICKS;
	}
}

export function resolveSelfPacedFloorSeconds(configOf?: LoopsConfigOf): number {
	try {
		const value = readIntKnob(
			configOf,
			"self_paced_floor_seconds",
			DEFAULT_SELF_PACED_FLOOR_SECONDS,
		);
		return Math.max(10, value);
	} catch {
		return DEFAULT_SELF_PACED_FLOOR_SECONDS;
	}
}

export function resolveSelfPacedCeilingSeconds(
	configOf?: LoopsConfigOf,
): number {
	const floor = resolveSelfPacedFloorSeconds(configOf);
	try {
		const value = readIntKnob(
			configOf,
			"self_paced_ceiling_seconds",
			DEFAULT_SELF_PACED_CEILING_SECONDS,
		);
		return Math.max(floor, value);
	} catch {
		return Math.max(floor, DEFAULT_SELF_PACED_CEILING_SECONDS);
	}
}

function readIntKnob(
	configOf: LoopsConfigOf | undefined,
	key: keyof LoopsConfig,
	fallback: number,
): number {
	const section = configOf === undefined ? undefined : configOf();
	const value =
		section === undefined || section === null ? undefined : section[key];
	return readIntKnobValue(value, fallback);
}

// ──────────────────────────────────────────────────────────────────────
// Response evaluation helpers
// ──────────────────────────────────────────────────────────────────────

/** True when the agent ended its reply with the LOOP_COMPLETE marker. */
export function responseSignalsComplete(response: string): boolean {
	if (!response) return false;
	return LOOP_COMPLETE_RE.test(response);
}

/**
 * Stable digest for self-paced change detection. Normalizes whitespace and
 * strips volatile timestamp-ish tokens so a reply that differs only by
 * 'checked at 14:02:33' doesn't defeat the backoff.
 */
export function digestResponse(response: string): string {
	let text = (response ?? "").trim().toLowerCase();
	text = text.replace(/\d{1,2}:\d{2}(?::\d{2})?/g, "");
	text = text.replace(/\d{4}-\d{2}-\d{2}/g, "");
	text = text.replace(
		/\b\d+(\.\d+)?\s*(s|sec|secs|seconds|m|min|mins|minutes|h|hr|hrs|hours)\b/g,
		"",
	);
	text = text.replace(/\s+/g, " ");
	return createHash("sha256").update(text, "utf8").digest("hex");
}

// ──────────────────────────────────────────────────────────────────────
// --until judge seam (goals.py:judge_goal port point; fail-open)
// ──────────────────────────────────────────────────────────────────────

export interface UntilJudgeVerdict {
	verdict: "done" | "continue";
	reason: string;
}

/**
 * Evidence-based stop judge. Hermes wires goals.judge_goal here (sync aux-LLM
 * call, run off-loop via run_in_executor). Throwing judges are caught by the
 * caller and fail open.
 */
export type UntilJudge = (until: string, response: string) => UntilJudgeVerdict;

/** Reason recorded when the judge is unavailable (ImportError arm parity). */
export const JUDGE_UNAVAILABLE_REASON = "judge unavailable";

// ──────────────────────────────────────────────────────────────────────
// LoopManager
// ──────────────────────────────────────────────────────────────────────

export interface LoopManagerDeps {
	sessionId: string;
	/** Open state.db handle (rows live in state_meta `loop:<session_id>`). */
	db: Database.Database;
	/** `loops:` config reader; absent ⇒ module defaults. */
	configOf?: LoopsConfigOf | undefined;
	/** Injected clock (tests drive tick timing deterministically). */
	clock?: GatewayClock | undefined;
	/** --until evidence judge; absent ⇒ fail-open continue. */
	judge?: UntilJudge | undefined;
}

export interface CompleteTickDecision {
	status: LoopStatus | null;
	stopped: boolean;
	reason: string;
	message: string;
}

const NO_TICK_IN_FLIGHT = "no tick in flight";

/** dispatch_loop_command's surface-agnostic result. */
export interface LoopDispatchResult {
	output: string;
	created: boolean;
}

/** ValueError arm of loops.py:set()/dispatch_loop_command. */
export class LoopValueError extends Error {}

/**
 * Per-session /loop state + tick decisions. Drivers call:
 *   set/pause/resume/clear — user controls;
 *   isDue — should a wakeup fire now? (cheap, in-memory);
 *   fireTick — claim the tick; returns the wakeup message to inject;
 *   completeTick(lastResponse) — evaluate the finished wakeup turn and
 *     schedule what's next;
 *   abandonTick — roll back a fired tick whose injection failed.
 */
export class LoopManager {
	readonly sessionId: string;
	/** `loops:` section reader (dispatch surfaces read the ceiling through it). */
	readonly configOf: LoopsConfigOf | undefined;
	private readonly db: Database.Database;
	private readonly clock: GatewayClock;
	private readonly judge: UntilJudge | undefined;
	private _state: LoopState | null;

	constructor(deps: LoopManagerDeps) {
		this.sessionId = deps.sessionId;
		this.db = deps.db;
		this.configOf = deps.configOf;
		this.clock = deps.clock ?? systemClock;
		this.judge = deps.judge;
		this._state = loadLoopRow(deps.db, deps.sessionId);
	}

	// -- introspection ---------------------------------------------------

	get state(): LoopState | null {
		return this._state;
	}

	/** Re-read state from the DB (cross-process safety for the gateway). */
	refresh(): void {
		this._state = loadLoopRow(this.db, this.sessionId);
	}

	isActive(): boolean {
		return this._state !== null && this._state.status === "active";
	}

	hasLoop(): boolean {
		return (
			this._state !== null &&
			(this._state.status === "active" || this._state.status === "paused")
		);
	}

	/** Printable one-liner (loops.py:status_line byte-shapes). */
	statusLine(nowSeconds?: number): string {
		const s = this._state;
		if (s === null || s.status === "cleared") {
			return "No loop set. Start one with /loop [interval] <prompt>.";
		}
		const fired = `${s.ticksFired} tick${s.ticksFired !== 1 ? "s" : ""}`;
		const caps: string[] = [];
		if (s.times) caps.push(`${s.ticksFired}/${s.times} runs`);
		else if (s.maxTicks) caps.push(`${s.ticksFired}/${s.maxTicks} budget`);
		else caps.push(fired);
		if (s.until) caps.push(`until: ${s.until}`);
		const meta = `${this.cadenceLabel(s)}, ${caps.join(", ")}`;
		if (s.status === "active") {
			const remaining = this.remainingLabel(s, nowSeconds);
			let tail = remaining ? `, ${remaining}` : "";
			if (s.awaitingResponse) tail = ", wakeup running";
			return `↻ Loop (active, ${meta}${tail}): ${s.prompt}`;
		}
		if (s.status === "paused") {
			const extra = s.pausedReason ? ` — ${s.pausedReason}` : "";
			return `⏸ Loop (paused, ${meta}${extra}): ${s.prompt}`;
		}
		if (s.status === "done") {
			const extra = s.lastStopReason ? ` — ${s.lastStopReason}` : "";
			return `✓ Loop finished (${fired}${extra}): ${s.prompt}`;
		}
		return `Loop (${s.status}, ${meta}): ${s.prompt}`;
	}

	// -- mutation --------------------------------------------------------

	/**
	 * Start a new loop (replaces any existing one for the session). Throws
	 * LoopValueError on an empty prompt (dispatch renders "/loop: <msg>").
	 */
	async set(
		prompt: string,
		opts: {
			intervalSeconds?: number | null | undefined;
			times?: number | undefined;
			until?: string | undefined;
			route?: LoopRoute | null | undefined;
		} = {},
	): Promise<LoopState> {
		const cleaned = (prompt ?? "").trim();
		if (!cleaned) throw new LoopValueError("loop prompt is empty");

		const now = this.clock.nowSeconds();
		let state: LoopState;
		if (opts.intervalSeconds != null) {
			const interval = Math.max(
				Math.trunc(opts.intervalSeconds),
				resolveMinIntervalSeconds(this.configOf),
			);
			state = blankState(cleaned);
			state.mode = "interval";
			state.intervalSeconds = interval;
			state.currentDelay = interval;
			state.nextDueAt = now + interval;
		} else {
			const floor = resolveSelfPacedFloorSeconds(this.configOf);
			state = blankState(cleaned);
			state.mode = "self_paced";
			state.intervalSeconds = 0;
			state.currentDelay = floor;
			state.nextDueAt = now + floor;
		}
		state.times = Math.max(0, Math.trunc(opts.times ?? 0));
		state.until = (opts.until ?? "").trim();
		state.maxTicks = resolveMaxTicksDefault(this.configOf);
		state.createdAt = now;
		state.route = { ...(opts.route ?? {}) };
		this._state = state;
		await saveLoopRow(this.db, this.sessionId, state);
		return state;
	}

	async pause(reason = "user-paused"): Promise<LoopState | null> {
		const s = this._state;
		if (s === null || (s.status !== "active" && s.status !== "paused")) {
			return null;
		}
		s.status = "paused";
		s.pausedReason = reason;
		s.awaitingResponse = false;
		await saveLoopRow(this.db, this.sessionId, s);
		return s;
	}

	async resume(): Promise<LoopState | null> {
		const s = this._state;
		if (s === null || s.status === "cleared") return null;
		s.status = "active";
		s.pausedReason = null;
		s.awaitingResponse = false;
		// Re-arm relative to now so a long pause doesn't fire instantly N times.
		const delay =
			s.currentDelay ||
			s.intervalSeconds ||
			resolveSelfPacedFloorSeconds(this.configOf);
		s.nextDueAt = this.clock.nowSeconds() + Math.min(delay, 5);
		await saveLoopRow(this.db, this.sessionId, s);
		return s;
	}

	async clear(): Promise<boolean> {
		const s = this._state;
		if (s === null || s.status === "cleared") return false;
		s.status = "cleared";
		await saveLoopRow(this.db, this.sessionId, s);
		this._state = null;
		return true;
	}

	async markDone(reason: string): Promise<void> {
		const s = this._state;
		if (s === null) return;
		s.status = "done";
		s.lastStopReason = reason;
		s.awaitingResponse = false;
		await saveLoopRow(this.db, this.sessionId, s);
	}

	// -- tick lifecycle ----------------------------------------------------

	/** Cheap check: active, not mid-wakeup, and the clock has passed. */
	isDue(nowSeconds?: number): boolean {
		const s = this._state;
		if (s === null || s.status !== "active" || s.awaitingResponse) return false;
		const now = nowSeconds ?? this.clock.nowSeconds();
		return now >= s.nextDueAt;
	}

	/**
	 * Claim a due tick. Returns the message to inject, or null.
	 *
	 * The returned text is either the wakeup-framed prompt or — when the
	 * loop's prompt is itself a slash command (`/loop 10m /recap`) — the raw
	 * command so the surface's normal slash dispatch handles it. Marks
	 * awaiting_response so the tick can't double-fire while its turn runs;
	 * drivers MUST follow up with completeTick (or abandonTick on injection
	 * failure).
	 */
	async fireTick(): Promise<string | null> {
		const s = this._state;
		if (s === null || !this.isDue()) return null;
		const now = this.clock.nowSeconds();
		s.ticksFired += 1;
		s.lastFiredAt = now;
		s.awaitingResponse = true;
		// Provisionally schedule the next tick from NOW; completeTick
		// reschedules from turn end (so a 10-minute turn doesn't cause an
		// instant re-fire), but if the process dies mid-turn the provisional
		// schedule keeps the persisted loop from being 'due' in a tight loop.
		const delay =
			s.currentDelay ||
			s.intervalSeconds ||
			resolveSelfPacedFloorSeconds(this.configOf);
		s.nextDueAt = s.lastFiredAt + delay;
		await saveLoopRow(this.db, this.sessionId, s);

		if (s.prompt.trimStart().startsWith("/")) {
			return s.prompt.trim();
		}
		const cadence =
			s.mode === "interval" ? `, ${this.cadenceLabel(s)}` : ", self-paced";
		const template = s.until
			? WAKEUP_PROMPT_WITH_UNTIL_TEMPLATE
			: WAKEUP_PROMPT_TEMPLATE;
		return template
			.replaceAll("{tick}", String(s.ticksFired))
			.replaceAll("{cadence}", cadence)
			.replaceAll("{prompt}", s.prompt)
			.replaceAll("{until}", s.until);
	}

	/** Roll back a fired tick whose injection failed (nothing ran). */
	async abandonTick(): Promise<void> {
		const s = this._state;
		if (s === null || !s.awaitingResponse) return;
		s.awaitingResponse = false;
		s.ticksFired = Math.max(0, s.ticksFired - 1);
		await saveLoopRow(this.db, this.sessionId, s);
	}

	/**
	 * Evaluate the finished wakeup turn and schedule what's next.
	 *
	 * Arms, in Hermes order:
	 *   1. agent self-stop marker (LOOP_COMPLETE on its own line);
	 *   2. --until evidence judge (fail-open; absent judge continues);
	 *   3. --times user cap;
	 *   4. loops.max_ticks config backstop → PAUSE (recoverable), not done;
	 *   5. still looping — schedule the next tick from turn end (with
	 *      self-paced backoff when applicable).
	 */
	async completeTick(lastResponse: string): Promise<CompleteTickDecision> {
		const s = this._state;
		if (s === null || !s.awaitingResponse) {
			return {
				status: s === null ? null : s.status,
				stopped: false,
				reason: NO_TICK_IN_FLIGHT,
				message: "",
			};
		}
		s.awaitingResponse = false;
		const now = this.clock.nowSeconds();

		// 1. Agent self-stop marker.
		if (responseSignalsComplete(lastResponse)) {
			s.status = "done";
			s.lastStopReason = "agent signaled the task is complete";
			await saveLoopRow(this.db, this.sessionId, s);
			return {
				status: "done",
				stopped: true,
				reason: s.lastStopReason,
				message: `✓ Loop finished after ${s.ticksFired} tick${s.ticksFired !== 1 ? "s" : ""} — task complete.`,
			};
		}

		// 2. Evidence-based --until judge (fail-open; absent judge continues).
		if (s.until && (lastResponse ?? "").trim()) {
			let verdict = "continue";
			let reason = JUDGE_UNAVAILABLE_REASON;
			try {
				if (this.judge === undefined) {
					throw new Error("no judge configured");
				}
				const outcome = this.judge(s.until, lastResponse ?? "");
				verdict = outcome.verdict;
				reason = outcome.reason;
			} catch {
				verdict = "continue";
				reason = JUDGE_UNAVAILABLE_REASON;
			}
			if (verdict === "done") {
				s.status = "done";
				s.lastStopReason = `stop condition met: ${reason}`;
				await saveLoopRow(this.db, this.sessionId, s);
				return {
					status: "done",
					stopped: true,
					reason: s.lastStopReason,
					message: `✓ Loop finished after ${s.ticksFired} tick${s.ticksFired !== 1 ? "s" : ""} — ${reason}`,
				};
			}
		}

		// 3. --times user cap.
		if (s.times && s.ticksFired >= s.times) {
			s.status = "done";
			s.lastStopReason = `completed the requested ${s.times} runs`;
			await saveLoopRow(this.db, this.sessionId, s);
			return {
				status: "done",
				stopped: true,
				reason: s.lastStopReason,
				message: `✓ Loop finished — ran ${s.times}/${s.times} times.`,
			};
		}

		// 4. Config backstop budget → pause (recoverable), not done.
		if (s.maxTicks && s.ticksFired >= s.maxTicks) {
			s.status = "paused";
			s.pausedReason = `tick budget exhausted (${s.ticksFired}/${s.maxTicks})`;
			await saveLoopRow(this.db, this.sessionId, s);
			return {
				status: "paused",
				stopped: true,
				reason: s.pausedReason,
				message: `⏸ Loop paused — ${s.ticksFired}/${s.maxTicks} ticks used (loops.max_ticks). /loop resume to keep going, /loop stop to end it.`,
			};
		}

		// 5. Still looping — schedule the next tick from turn end.
		if (s.mode === "self_paced") {
			const digest = digestResponse(lastResponse);
			const floor = resolveSelfPacedFloorSeconds(this.configOf);
			const ceiling = resolveSelfPacedCeilingSeconds(this.configOf);
			if (digest && digest === s.lastResponseDigest) {
				// Nothing changed — back off.
				s.currentDelay = Math.min(Math.max(s.currentDelay, floor) * 2, ceiling);
			} else {
				s.currentDelay = floor;
			}
			s.lastResponseDigest = digest;
		} else {
			s.currentDelay = s.intervalSeconds;
		}
		s.nextDueAt = now + s.currentDelay;
		await saveLoopRow(this.db, this.sessionId, s);
		return {
			status: "active",
			stopped: false,
			reason: "loop continues",
			message: "",
		};
	}

	// -- labels ------------------------------------------------------------

	/** loops.py:LoopState.cadence_label. */
	cadenceLabel(s: LoopState = this.requireState()): string {
		if (s.mode === "self_paced") {
			const live = s.currentDelay
				? `, currently ${formatInterval(s.currentDelay)}`
				: "";
			return `self-paced${live}`;
		}
		return `every ${formatInterval(s.intervalSeconds)}`;
	}

	/** loops.py:LoopState.remaining_label. */
	remainingLabel(
		s: LoopState = this.requireState(),
		nowSeconds?: number,
	): string {
		if (s.status !== "active") return "";
		const remaining = s.nextDueAt - (nowSeconds ?? this.clock.nowSeconds());
		if (remaining <= 0) return "due now";
		return `next in ${formatInterval(remaining)}`;
	}

	private requireState(): LoopState {
		if (this._state === null) {
			throw new Error("loop state required");
		}
		return this._state;
	}
}

function blankState(prompt: string): LoopState {
	return {
		prompt,
		status: "active",
		mode: "interval",
		intervalSeconds: 0,
		currentDelay: 0,
		times: 0,
		until: "",
		maxTicks: LOOP_DEFAULT_MAX_TICKS,
		ticksFired: 0,
		createdAt: 0,
		lastFiredAt: 0,
		nextDueAt: 0,
		awaitingResponse: false,
		lastResponseDigest: "",
		pausedReason: null,
		lastStopReason: null,
		route: {},
	};
}

// ──────────────────────────────────────────────────────────────────────
// Shared slash-command dispatch (CLI + gateway + TUI use the same logic)
// ──────────────────────────────────────────────────────────────────────

/**
 * Surface-agnostic handler for `/loop <args>` (dispatch_loop_command port).
 * Returns ready-to-send output; each surface only decorates it. `route` is
 * stored on newly created loops so the idle watcher can inject wakeups back
 * into the right chat; CLI/TUI pass none.
 */
export async function dispatchLoopCommand(
	mgr: LoopManager,
	args: string,
	opts: { route?: LoopRoute | null } = {},
): Promise<LoopDispatchResult> {
	const arg = (args ?? "").trim();
	const lower = arg.toLowerCase();

	if (!arg || lower === "status") {
		return { output: mgr.statusLine(), created: false };
	}

	if (lower === "pause") {
		const state = await mgr.pause("user-paused");
		if (state === null) return { output: "No loop set.", created: false };
		return {
			output: `⏸ Loop paused: ${state.prompt}\nUse /loop resume to continue.`,
			created: false,
		};
	}

	if (lower === "resume") {
		const state = await mgr.resume();
		if (state === null) return { output: "No loop to resume.", created: false };
		return {
			output: `▶ Loop resumed (${mgr.cadenceLabel(state)}): ${state.prompt}`,
			created: false,
		};
	}

	if (lower === "stop" || lower === "clear" || lower === "cancel") {
		const had = await mgr.clear();
		return {
			output: had ? "✓ Loop stopped." : "No active loop.",
			created: false,
		};
	}

	if (lower === "help" || lower === "--help" || lower === "-h") {
		return {
			output: [
				"Usage: /loop [interval] <prompt> [--times N] [--until <condition>]",
				"  /loop 5m check the deploy status      — fixed cadence",
				"  /loop every 10m /recap                — loop a slash command",
				"  /loop keep fixing tests until green   — self-paced (backs off while output is unchanged)",
				"  /loop 2m poll CI --times 30           — stop after 30 runs",
				"  /loop 5m watch the queue --until queue is empty",
				"Controls: /loop status · /loop pause · /loop resume · /loop stop",
				`The loop also stops itself when the agent replies with ${LOOP_COMPLETE_MARKER}.`,
			].join("\n"),
			created: false,
		};
	}

	const parsed = parseLoopArgs(arg);
	if (parsed.error !== null) {
		if (parsed.error === "empty") {
			return {
				output: "Usage: /loop [interval] <prompt> — see /loop help.",
				created: false,
			};
		}
		return { output: `/loop: ${parsed.error}`, created: false };
	}

	const replacing = mgr.hasLoop();
	let state: LoopState;
	try {
		state = await mgr.set(parsed.prompt, {
			intervalSeconds: parsed.intervalSeconds,
			times: parsed.times,
			until: parsed.until,
			route: opts.route,
		});
	} catch (err) {
		if (err instanceof LoopValueError) {
			return { output: `/loop: ${err.message}`, created: false };
		}
		throw err;
	}

	const lines: string[] = [
		`↻ Loop set (${mgr.cadenceLabel(state)}): ${state.prompt}`,
	];
	if (
		parsed.intervalSeconds !== null &&
		parsed.intervalSeconds < state.intervalSeconds
	) {
		lines.push(
			`(interval raised to the ${formatInterval(state.intervalSeconds)} minimum — loops.min_interval_seconds)`,
		);
	}
	if (state.mode === "self_paced") {
		lines.push(
			`Self-paced: first check in ${formatInterval(state.currentDelay)}; backs off up to ${formatInterval(resolveSelfPacedCeilingSeconds(mgr.configOf))} while nothing changes.`,
		);
	}
	if (state.times) {
		lines.push(
			`Runs ${state.times} time${state.times !== 1 ? "s" : ""}, then stops.`,
		);
	}
	if (state.until) lines.push(`Stops when: ${state.until}`);
	if (!state.times && state.maxTicks) {
		lines.push(
			`Backstop budget: ${state.maxTicks} ticks (loops.max_ticks; 0 = unlimited).`,
		);
	}
	lines.push(
		`First wakeup ${mgr.remainingLabel(state)}. Controls: /loop status · pause · resume · stop.`,
	);
	if (replacing) {
		lines.splice(1, 0, "(replaced the previous loop for this session)");
	}
	return { output: lines.join("\n"), created: true };
}

// ──────────────────────────────────────────────────────────────────────
// Gateway-side seams (run.py/_busy_loop_command + slash_commands route capture)
// ──────────────────────────────────────────────────────────────────────

/**
 * run.py:_busy_loop_command mid-run control verbs: /loop mirrors /goal —
 * control verbs are safe mid-run (state only — read at the next idle
 * boundary); setting a new loop mid-run is rejected so we don't race the
 * current turn. Compared against the FULL stripped/lowercased arg string.
 */
export const LOOP_MIDRUN_CONTROL_ARGS: ReadonlySet<string> = new Set([
	"",
	"status",
	"pause",
	"resume",
	"stop",
	"clear",
	"cancel",
	"help",
	"--help",
	"-h",
]);

/** run.py:_busy_loop_command rejection text (byte-stable). */
export const LOOP_BUSY_SET_REJECT_TEXT =
	"Agent is running — use /loop status / pause / stop mid-run, or /stop before setting a new loop.";

/** True when the mid-run /loop variant may execute (control verb / bare). */
export function isLoopMidrunControlArg(arg: string): boolean {
	const cleaned = (arg ?? "").trim().toLowerCase();
	return LOOP_MIDRUN_CONTROL_ARGS.has(cleaned);
}

/**
 * slash_commands.py:_handle_loop_command route capture: persist the event's
 * routing onto newly created loops so the idle wakeup watcher can inject
 * ticks back into this chat even after a restart. Empty values drop out
 * (Hermes filters falsy). Pi's SessionSource carries no display-name field,
 * so `user_name` stays unset (the watcher never reads it for keying).
 */
export function routeFromSource(source: SessionSource | undefined): LoopRoute {
	if (source === undefined || source === null) return {};
	const route: LoopRoute = {
		platform: String(source.platform ?? ""),
		chat_id: String(source.chatId ?? ""),
		chat_type: String(source.chatType ?? ""),
		thread_id: String(source.threadId ?? ""),
		user_id: String(source.userId ?? ""),
	};
	for (const key of Object.keys(route)) {
		if (!route[key]) delete route[key];
	}
	return route;
}
