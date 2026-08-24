// service.ts — the EMBEDDED kanban dispatcher service (stage 8
// "embedded_watchers" slice; 01 §4 row "Kanban dispatcher", 07 §6).
//
// Hermes anchor: gateway/kanban_watchers.py::_kanban_dispatcher_watcher —
//   * gated by kanban.dispatch_in_gateway (default true) with a false-y ENV
//     escape hatch (HERMES_KANBAN_DISPATCH_IN_GATEWAY ∈ {0,false,no,off});
//   * config read ONCE at boot — flipping requires restart (DEC-013: no live
//     reload; SIGHUP is not a reload signal);
//   * one tick every dispatch_interval_seconds (default 60, sanity floor 1s);
//   * per-tick failures NEVER stop subsequent ticks;
//   * invalid interval/failure-limit values fall back to defaults WITH a
//     loud warning instead of refusing the whole service.
//
// Lifecycle integration (optional-stage pattern): this service is an OPTIONAL
// stage — on any startup failure it DEGRADES LOUDLY (returns
// { ok: false, degraded: true, reason }) WITHOUT blocking sibling services.
// It never imports runner internals (layering: pi_embedded may not reach
// pi_gateway/lifecycle); it returns the stage-event shape so the runner's
// stage 8 wiring can map it onto StageEvent verbatim.
//
// Hard boundary (module header of board.ts): when an explicitly pinned board
// slug is INVALID, the service refuses to dispatch at all this boot and
// degrades loudly — falling through to another board would silently dispatch
// cards across the exact boundary HERMES_KANBAN_BOARD exists to enforce.
//
// Divergence / PROPOSED DEC text: Hermes resolves the dispatcher singleton
// via a machine-global flock file (<kanban_home>/kanban/.dispatcher.lock);
// Node has no flock and new native lock deps are suspect (DEC-027 rationale),
// so v0.1 relies on (a) the config gate + env escape hatch as PRIMARY
// control, exactly like Hermes ("lock is backstop"), and (b) the board's own
// CAS transitions, which make double-ticks benign rather than corrupting. A
// SQLite-held singleton backstop mirroring DEC-027 remains future work for
// the runner wiring phase; the seam accepts an external `hasSingleton`
// probe so that wiring can assert it without touching this module.

import { KANBAN_BOARD_ENV, resolveBoardSlug } from "./board.js";
import { systemClock, type GatewayClock } from "./clock.js";
import {
	dispatchOnce,
	DEFAULT_DISPATCH_INTERVAL_SECONDS,
	MIN_DISPATCH_INTERVAL_SECONDS,
} from "./dispatcher.js";
import type { BoardClient, SpawnFn } from "./types.js";
import { DEFAULT_FAILURE_LIMIT } from "./types.js";

/** False-y env values (parity of the watcher's env_override check). */
const FALSEY_ENV = new Set(["0", "false", "no", "off"]);

/** The gateway-hosted-dispatcher gate env var (parity escape hatch). */
export const KANBAN_DISPATCH_IN_GATEWAY_ENV =
	"HERMES_KANBAN_DISPATCH_IN_GATEWAY";

export interface KanbanDispatcherConfig {
	board: string;
	boardSource: "pinned" | "env" | "default";
	intervalSeconds: number;
	failureLimit: number;
	maxSpawn?: number | null;
	enabled: boolean;
	warnings: string[];
}

/**
 * Resolve ALL dispatcher config up front (DEC-013: no live reload). Invalid
 * numeric settings fall back to defaults with recorded warnings — parity of
 * the watcher's per-field try/except warnings.
 */
export function resolveDispatcherServiceConfig(input: {
	pinnedBoard?: string | null | undefined;
	env?: Record<string, string | undefined> | undefined;
	config?: Record<string, unknown> | undefined;
}): KanbanDispatcherConfig {
	const cfg = input.config ?? {};
	const warnings: string[] = [];

	let enabled = true;
	const envOverride = (input.env?.[KANBAN_DISPATCH_IN_GATEWAY_ENV] ?? "")
		.trim()
		.toLowerCase();
	if (FALSEY_ENV.has(envOverride)) {
		enabled = false;
	}

	// Board resolution — HARD boundary rules apply. An invalid or
	// fell-through pinned/env slug returns an UNUSABLE config (board: "")
	// with a loud warning; start() refuses to dispatch on it.
	const resolved = resolveBoardSlug({
		pinned: input.pinnedBoard,
		env: input.env,
	});
	if (resolved.fellThrough) {
		return {
			board: "",
			boardSource: "pinned",
			intervalSeconds: DEFAULT_DISPATCH_INTERVAL_SECONDS,
			failureLimit: DEFAULT_FAILURE_LIMIT,
			maxSpawn: null,
			enabled,
			warnings: [
				`kanban dispatcher: ${resolved.reason ?? "board resolution fell through"}; ` +
					`DEGRADED LOUDLY — refusing to dispatch this boot ` +
					`(hard board boundary; fix ${KANBAN_BOARD_ENV} and RESTART, DEC-013)`,
			],
		};
	}
	const board = resolved.board;
	const boardSource = resolved.source;

	const intervalSeconds = positiveNumberOrDefault(
		cfg.dispatch_interval_seconds,
		DEFAULT_DISPATCH_INTERVAL_SECONDS,
		(w) => warnings.push(w),
		"dispatch_interval_seconds",
	);

	const failureLimitRaw = cfg.failure_limit;
	let failureLimit = DEFAULT_FAILURE_LIMIT;
	if (typeof failureLimitRaw === "number" && Number.isFinite(failureLimitRaw)) {
		failureLimit = Math.trunc(failureLimitRaw);
	}
	if (failureLimit < 1) {
		warnings.push(
			`kanban dispatcher: kanban.failure_limit=${JSON.stringify(failureLimitRaw)} is below 1; using default ${DEFAULT_FAILURE_LIMIT}`,
		);
		failureLimit = DEFAULT_FAILURE_LIMIT;
	}

	let maxSpawn: number | null = null;
	if (
		typeof cfg.max_spawn === "number" &&
		Number.isFinite(cfg.max_spawn) &&
		cfg.max_spawn >= 1
	) {
		maxSpawn = Math.trunc(cfg.max_spawn);
	}

	return {
		board,
		boardSource,
		intervalSeconds,
		failureLimit,
		maxSpawn,
		enabled,
		warnings,
	};
}

function positiveNumberOrDefault(
	raw: unknown,
	dflt: number,
	warn: (message: string) => void,
	label: string,
): number {
	if (raw === undefined || raw === null || raw === "") return dflt;
	const n = typeof raw === "number" ? raw : Number(raw);
	if (!Number.isFinite(n)) {
		warn(
			`kanban dispatcher: invalid ${label}=${JSON.stringify(raw)}, using default ${dflt}`,
		);
		return dflt;
	}
	const floored = Math.max(Math.trunc(n), MIN_DISPATCH_INTERVAL_SECONDS);
	if (floored !== n) {
		warn(
			`kanban dispatcher: ${label} clamped to floor ${MIN_DISPATCH_INTERVAL_SECONDS}s`,
		);
	}
	return floored;
}

export interface ServiceStartResult {
	/** True when the dispatcher loop actually started. */
	ok: boolean;
	/** Optional-stage classification: failed loudly but did NOT block others. */
	degraded: boolean;
	reason?: string;
	warnings: string[];
}

export interface StartKanbanDispatcherOptions {
	/**
	 * Factory producing the board client for the RESOLVED board. Throwing
	 * here (missing store, unwritable path, wrong-board refusal) degrades
	 * the service loudly without blocking other services.
	 */
	openBoard: (board: string) => Promise<BoardClient> | BoardClient;
	/** Worker spawner handed to every tick (injectable; tests use fakes). */
	spawn: SpawnFn;
	pinnedBoard?: string | null;
	env?: Record<string, string | undefined>;
	config?: Record<string, unknown>;
	clock?: GatewayClock;
	/**
	 * External singleton-backstop probe (see module header divergence note):
	 * when provided and false, the service does NOT dispatch (another
	 * process owns the machine-global dispatcher role).
	 */
	hasSingleton?: () => boolean;
}

export interface RunningKanbanDispatcher {
	stop(): Promise<void>;
}

/**
 * Start the embedded dispatcher. Never throws: every failure mode collapses
 * into a classified ServiceStartResult (optional-stage contract). The tick
 * loop runs until stop(); a THROWING tick is logged loudly and the loop
 * continues (parity: "Per-tick failures don't stop subsequent ticks").
 */
export async function startKanbanDispatcher(
	opts: StartKanbanDispatcherOptions,
	log: (line: string) => void = console.error,
): Promise<{ result: ServiceStartResult; running?: RunningKanbanDispatcher }> {
	const clock = opts.clock ?? systemClock;

	// Config ONCE at boot (DEC-013).
	const cfg = resolveDispatcherServiceConfig({
		pinnedBoard: opts.pinnedBoard,
		env: opts.env,
		config: opts.config,
	});
	for (const warning of cfg.warnings) log(`[kanban] WARNING ${warning}`);

	if (!cfg.enabled) {
		const reason = `disabled via ${KANBAN_DISPATCH_IN_GATEWAY_ENV} env`;
		log(`[kanban] dispatcher: ${reason}`);
		return { result: { ok: false, degraded: false, reason, warnings: [] } };
	}
	if (!cfg.board) {
		const reason =
			cfg.warnings.find((w) => w.includes("board")) ??
			"invalid pinned board slug";
		log(`[kanban] dispatcher: DEGRADED — ${reason}`);
		return {
			result: { ok: false, degraded: true, reason, warnings: cfg.warnings },
		};
	}
	if (opts.hasSingleton && !opts.hasSingleton()) {
		const reason = "another gateway holds the machine-global dispatcher role";
		log(`[kanban] dispatcher: ${reason}; this gateway will NOT dispatch.`);
		return { result: { ok: false, degraded: false, reason, warnings: [] } };
	}

	// Board open — a wrong/unopenable board degrades LOUDLY, never falls
	// through to a different board.
	let board: BoardClient;
	try {
		board = await opts.openBoard(cfg.board);
	} catch (err) {
		const reason = err instanceof Error ? err.message : String(err);
		const full = `cannot open board ${JSON.stringify(cfg.board)}: ${reason}`;
		log(`[kanban] dispatcher: DEGRADED — ${full}`);
		return {
			result: { ok: false, degraded: true, reason: full, warnings: [] },
		};
	}
	if (board.board !== cfg.board) {
		const full =
			`board client resolved to ${JSON.stringify(board.board)} but ` +
			`dispatcher pinned ${JSON.stringify(cfg.board)} — refusing (hard board boundary)`;
		log(`[kanban] dispatcher: DEGRADED — ${full}`);
		return {
			result: { ok: false, degraded: true, reason: full, warnings: [] },
		};
	}

	log(
		`[kanban] dispatcher: holding dispatch duty for board ${JSON.stringify(cfg.board)} ` +
			`(interval=${cfg.intervalSeconds}s, failure_limit=${cfg.failureLimit}` +
			(cfg.maxSpawn !== null ? `, max_spawn=${cfg.maxSpawn}` : "") +
			")",
	);

	let running = true;
	const loopDone = runLoop({
		board,
		spawnFn: opts.spawn,
		cfg,
		clock,
		log,
		shouldRun: () => running,
	});

	return {
		result: { ok: true, degraded: false, warnings: [] },
		running: {
			stop: async () => {
				running = false;
				await loopDone;
			},
		},
	};
}

async function runLoop(state: {
	board: BoardClient;
	spawnFn: SpawnFn;
	cfg: KanbanDispatcherConfig;
	clock: GatewayClock;
	log: (line: string) => void;
	shouldRun: () => boolean;
}): Promise<void> {
	const { board, spawnFn, cfg, clock, log, shouldRun } = state;
	while (shouldRun()) {
		try {
			await dispatchOnce(board, {
				nowSeconds: clock.nowSeconds(),
				failureLimit: cfg.failureLimit,
				maxSpawn: cfg.maxSpawn ?? null,
				spawn: spawnFn,
			});
		} catch (err) {
			// One bad tick must never stop subsequent ticks — parity of
			// _kanban_dispatcher_watcher ("Failures in one tick don't stop
			// subsequent ticks"). Logged LOUDLY, loop continues.
			log(
				`[kanban] dispatcher: tick FAILED loudly, continuing: ` +
					(err instanceof Error ? err.message : String(err)),
			);
		}
		await clock.sleepMs(cfg.intervalSeconds * 1000);
	}
}
