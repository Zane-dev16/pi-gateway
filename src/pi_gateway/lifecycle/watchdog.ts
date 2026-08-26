// pi_gateway/lifecycle/watchdog.ts — out-of-loop liveness backstops.
// 08 §1.3(c): flush protects DATA, forensics preserves EVIDENCE, the watchdog
// restores LIVENESS when the drain itself is wedged — an asyncio/JS deadline
// cannot recover a frozen event loop because every async recovery path needs
// the same stuck loop. Hermes anchors (READ-ONLY reference; semantics ported,
// no code vendored — gateway/shutdown_watchdog.py):
//   resolve_shutdown_watchdog_delay   → resolveShutdownWatchdogDelayMs
//     (drain_timeout + 60s grace so a slow-but-progressing drain isn't cut)
//   arm_shutdown_watchdog             → armShutdownWatchdog
//     (hard-exit ≤ drain+60s; metadata snapshot; dump to
//     logs/gateway-shutdown-watchdog.log AND stderr; PID file + runtime lock
//     released BEFORE os._exit by the CALLER's exit callback — locks must
//     never be stranded)
//   start_loop_liveness_watchdog      → startLoopLivenessGuard
//     (probe every 30s; 10s probe timeout; 3 strikes ⇒ hard-exit 75 =
//     GATEWAY_SERVICE_RESTART_EXIT_CODE so supervisors recycle)
//   write_loop_heartbeat / loop_heartbeat_forever → writeLoopHeartbeat /
//     startLoopHeartbeat (<home>/state/gateway.heartbeat; frozen loop ⇒ stale
//     mtime distinguishes "process alive" from "loop live" externally)
//
// Runtime-idiom adaptation under DEC-023 (same semantics): Python arms daemon
// THREADS; Node gives us no in-process threads with an independent scheduler,
// so the watchdog is an UNREF'D OS-LEVEL TIMER (fires even while no work is
// queued) and the loop-liveness probe uses a TWO-WITNESS timer pair on the
// event loop itself: a zero-delay witness task must run within probe_timeout
// of each interval or the loop is declared blocked. All timers are injectable.

import { mkdirSync, renameSync, writeFileSync } from "node:fs";
import { randomUUID } from "node:crypto";
import { dirname, join } from "node:path";
import { SERVICE_RESTART_EXIT_CODE } from "./restart.js";

/** Injectable timing surface (tests drive the clock; production unrefs). */
export interface TimerPort {
	setTimeout(fn: () => void, ms: number): unknown;
	clearTimeout(handle: unknown): void;
	nowMs(): number;
}

/** Real timers; handles are unref'd so they never hold the process open. */
export function realTimerPort(): TimerPort {
	return {
		setTimeout(fn: () => void, ms: number): unknown {
			const t = setTimeout(fn, ms);
			t.unref();
			return t;
		},
		clearTimeout(handle: unknown): void {
			clearTimeout(handle as NodeJS.Timeout);
		},
		nowMs(): number {
			return Date.now();
		},
	};
}

export const DEFAULT_SHUTDOWN_WATCHDOG_GRACE_MS = 60_000;
export const DEFAULT_HEARTBEAT_INTERVAL_MS = 30_000;
export const LOOP_LIVENESS_PROBE_INTERVAL_MS = 30_000;
export const LOOP_LIVENESS_PROBE_TIMEOUT_MS = 10_000;
/** 3 sustained misses escalate (~90–120s of loop block). #90502. */
export const LOOP_LIVENESS_MAX_STRIKES = 3;

export function loopHeartbeatPath(home: string): string {
	return join(home, "state", "gateway.heartbeat");
}

export function shutdownWatchdogDumpPath(home: string): string {
	return join(home, "logs", "gateway-shutdown-watchdog.log");
}

/**
 * Wall-clock leash for the shutdown watchdog (parity of
 * resolve_shutdown_watchdog_delay): drain budget + grace (default 60s).
 */
export function resolveShutdownWatchdogDelayMs(
	drainTimeoutMs: number,
	graceMs = DEFAULT_SHUTDOWN_WATCHDOG_GRACE_MS,
): number {
	const drain = Number.isFinite(drainTimeoutMs)
		? Math.max(drainTimeoutMs, 0)
		: 0;
	const grace = Number.isFinite(graceMs)
		? Math.max(graceMs, 0)
		: DEFAULT_SHUTDOWN_WATCHDOG_GRACE_MS;
	return drain + grace;
}

function writeAtomicLine(path: string, payload: unknown): void {
	try {
		mkdirSync(dirname(path), { recursive: true });
		const tmp = `${path}.${randomUUID()}.tmp`;
		writeFileSync(tmp, JSON.stringify(payload), { mode: 0o600 });
		renameSync(tmp, path);
	} catch {
		/* best-effort — never raise from a dying process */
	}
}

function appendDumpLine(path: string, line: string): void {
	try {
		mkdirSync(dirname(path), { recursive: true });
		writeFileSync(path, `${line}\n`, { flag: "a", mode: 0o600 });
	} catch {
		/* wedged disk was one of the #66892 hypotheses — keep hard-exiting */
	}
}

export interface ArmShutdownWatchdogOptions {
	delayMs: number;
	/** Exit status for the hard exit (Hermes default 1 for stop()-wedged). */
	exitCode?: number;
	/** Where the metadata snapshot is appended (default <home>/logs/…). */
	dumpPath?: string;
	/** Cheap sync metadata captured at fire time (phase, counts…). */
	snapshotFn?: () => Record<string, unknown>;
	/**
	 * The actual exit primitive — MUST release the PID file + runtime lock
	 * BEFORE exiting (§1.3c ordering lives at this call site).
	 */
	hardExit: (code: number) => void;
	timer?: TimerPort;
	writeStderr?: (line: string) => void;
}

export interface WatchdogHandle {
	/** Disarm after a normal completion — the watchdog then exits quietly. */
	disarm(): void;
	/** True once the backstop FIRED (diagnostics only). */
	fired(): boolean;
}

/**
 * Arm the wedged-shutdown hard-exit backstop around a drain. If disarm() has
 * not run within delayMs, capture the metadata snapshot, append it to the
 * dump log + stderr, and call hardExit(exitCode). Never throws; delay ≤ 0
 * disables arming entirely.
 */
export function armShutdownWatchdog(
	options: ArmShutdownWatchdogOptions,
): WatchdogHandle {
	const handle: WatchdogHandle = {
		disarm(): void {
			disarmed = true;
			if (timer !== null) options.timer?.clearTimeout(timer);
			timer = null;
		},
		fired(): boolean {
			return fired;
		},
	};
	let disarmed = false;
	let fired = false;
	let timer: unknown = null;

	const delay = Number.isFinite(options.delayMs)
		? Math.max(options.delayMs, 0)
		: DEFAULT_SHUTDOWN_WATCHDOG_GRACE_MS;
	if (delay <= 0 || !options.timer) return handle;

	timer = options.timer.setTimeout(() => {
		timer = null;
		if (disarmed || fired) return;
		fired = true;
		let snapshot: Record<string, unknown> = {};
		if (options.snapshotFn) {
			try {
				snapshot = options.snapshotFn();
			} catch (err) {
				snapshot = { snapshot_error: String(err) };
			}
		}
		const record = {
			event: "shutdown_watchdog_fired",
			pid: process.pid,
			delay_s: Math.round(delay / 1000),
			fired_at: new Date().toISOString(),
			snapshot,
		};
		const json = JSON.stringify(record);
		if (options.dumpPath !== undefined) {
			appendDumpLine(options.dumpPath, json);
		}
		const stderr =
			options.writeStderr ??
			((line) => {
				try {
					process.stderr.write(`${line}\n`);
				} catch {
					/* stderr unavailable */
				}
			});
		stderr(
			`pi-gateway shutdown watchdog fired after ${Math.round(delay / 1000)}s ` +
				"(pid=" +
				process.pid +
				"); teardown appears wedged — forcing exit.",
		);
		options.hardExit(options.exitCode ?? 1);
	}, delay);
	return handle;
}

export interface LoopLivenessGuardOptions {
	probeIntervalMs?: number;
	probeTimeoutMs?: number;
	maxStrikes?: number;
	/** Breach exit code; Hermes uses the service-restart code 75. */
	exitCode?: number;
	onBreach?: (strikes: number) => void;
	hardExit: (code: number) => void;
	timer?: TimerPort;
}

export interface LoopLivenessGuardHandle {
	stop(): void;
	strikes(): number;
}

/**
 * Event-loop liveness guard (parity of the run.py loop-liveness thread):
 * every probeIntervalMs schedule a ZERO-DELAY WITNESS on the event loop; if
 * the witness has not executed within probeTimeoutMs the loop was blocked for
 * the whole window ⇒ one strike. maxStrikes CONSECUTIVE misses ⇒ onBreach()
 * then hardExit(exitCode). A responding probe resets the strike count. The
 * two-witness shape mirrors shutdown_watchdog.py's off-loop watcher + on-loop
 * probe pair (#90502), adapted to Node's single-loop runtime.
 */
export function startLoopLivenessGuard(
	options: LoopLivenessGuardOptions,
): LoopLivenessGuardHandle {
	const timer = options.timer ?? realTimerPort();
	const intervalMs = options.probeIntervalMs ?? LOOP_LIVENESS_PROBE_INTERVAL_MS;
	const timeoutMs = options.probeTimeoutMs ?? LOOP_LIVENESS_PROBE_TIMEOUT_MS;
	const maxStrikes = options.maxStrikes ?? LOOP_LIVENESS_MAX_STRIKES;
	const exitCode = options.exitCode ?? SERVICE_RESTART_EXIT_CODE;

	let strikeCount = 0;
	let stopped = false;
	let intervalHandle: unknown = null;
	let judgeHandle: unknown = null;

	function probe(): void {
		if (stopped) return;
		// Per-round closed-over verdict: the zero-delay witness proves the loop
		// can run callbacks; the delayed judge decides whether it did within
		// probeTimeoutMs. Local state keeps late/queued rounds independent.
		let witnessRan = false;
		timer.setTimeout(() => {
			witnessRan = true;
		}, 0);
		judgeHandle = timer.setTimeout(() => {
			judgeHandle = null;
			if (stopped) return;
			if (witnessRan) {
				strikeCount = 0; // loop answered — sustained-miss counter resets
				return;
			}
			strikeCount++;
			if (strikeCount < maxStrikes) return;
			try {
				options.onBreach?.(strikeCount);
			} catch {
				/* breach reporting must not block the exit */
			}
			options.hardExit(exitCode);
		}, timeoutMs);
	}

	intervalHandle = timer.setTimeout(function tick() {
		probe();
		if (!stopped) intervalHandle = timer.setTimeout(tick, intervalMs);
	}, intervalMs);

	return {
		stop(): void {
			stopped = true;
			if (intervalHandle !== null) timer.clearTimeout(intervalHandle);
			if (judgeHandle !== null) timer.clearTimeout(judgeHandle);
		},
		strikes(): number {
			return strikeCount;
		},
	};
}

export interface HeartbeatOptions {
	intervalMs?: number;
	pid?: number;
	startTimeSec?: number | null;
	timer?: TimerPort;
	extra?: Record<string, unknown>;
}

export interface HeartbeatHandle {
	stop(): void;
	path(): string;
}

/**
 * Rewrite `<home>/state/gateway.heartbeat` immediately and then on a cadence
 * until cancelled (loop_heartbeat_forever parity). Runs ON the event loop, so
 * a frozen loop stops refreshing it — that stale mtime IS the signal external
 * monitors consume. Best-effort writes; never raises.
 */
export function startLoopHeartbeat(
	home: string,
	options: HeartbeatOptions = {},
): HeartbeatHandle {
	const timer = options.timer ?? realTimerPort();
	const intervalMs = Math.max(
		1_000,
		options.intervalMs ?? DEFAULT_HEARTBEAT_INTERVAL_MS,
	);
	const path = loopHeartbeatPath(home);
	let stopped = false;
	let handle: unknown = null;

	function beat(): void {
		if (stopped) return;
		const payload: Record<string, unknown> = {
			pid: options.pid ?? process.pid,
			updated_at: new Date().toISOString(),
			monotonic: timer.nowMs() / 1000,
		};
		if (options.startTimeSec != null) {
			payload["start_time"] = options.startTimeSec;
		}
		if (options.extra) Object.assign(payload, options.extra);
		writeAtomicLine(path, payload);
	}

	beat(); // immediate first write so monitors see a fresh file right away
	handle = timer.setTimeout(function tick() {
		beat();
		if (!stopped) handle = timer.setTimeout(tick, intervalMs);
	}, intervalMs);

	return {
		stop(): void {
			stopped = true;
			if (handle !== null) timer.clearTimeout(handle);
		},
		path(): string {
			return path;
		},
	};
}
