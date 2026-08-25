// Behavior contracts for graceful shutdown (08 §1.2 + §1.3(a)):
// - signal classification: takeover marker > SIGINT/planned-stop marker >
//   unexpected external signal
// - recorded outcomes differ by class: planned persists "stopped", unexpected
//   must NOT (#42675); exit codes 0/0/1
// - flush-before-clear binding order, including the persistence-broken path
//   where recovery files are written BEFORE in-memory slots clear (#72680)
// - second-signal fast-exit releases pid file + lock before hard exit

import { existsSync, mkdtempSync, readdirSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import { GatewayLifecycle } from "./lifecycle.js";
import {
	DRAIN_PHASE_ORDER,
	SHUTDOWN_EXIT_CODES,
	classifySignalForSelf,
	cleanShutdownMarkerPath,
	executeDrain,
	incrementRestartFailureCounts,
	readRestartFailureCounts,
	writeCleanShutdownMarker,
	normalizePendingPayload,
	recoveryDir,
	type DrainHooks,
	type Logger,
} from "./shutdown.js";
import { writePlannedStopMarker, writeTakeoverMarker } from "./markers.js";
import { getRunningPid } from "./instance-guard.js";
import { readRuntimeStatus } from "./status-stamp.js";
import type { StageBody } from "./lifecycle.js";

let home: string;

beforeEach(() => {
	home = mkdtempSync(join(tmpdir(), "pi-lifecycle-shutdown-"));
});

afterEach(() => {
	rmSync(home, { recursive: true, force: true });
});

const QUIET_LOGGER: Logger = { info() {}, warn() {}, error() {} };

/** A lifecycle that reached running with stub bodies and no real services. */
function runningLifecycle(
	extra: {
		stageBodies?: Partial<Record<string, StageBody>>;
		logger?: Logger;
		options?: Partial<ConstructorParameters<typeof GatewayLifecycle>[0]>;
	} = {},
): GatewayLifecycle {
	return new GatewayLifecycle({
		home,
		logger: extra.logger ?? QUIET_LOGGER,
		// Unit tests stay hermetic: no detached ps-walks from the signal path.
		forensics: { snapshot: false, spawnDiagnostic: false },
		...extra.options,
	});
}

function noopDrainBodies(): Partial<Record<string, StageBody>> {
	const body: StageBody = async () => {};
	return {
		cron_scheduler: body,
		embedded_watchers: body,
		platform_adapters: body,
	};
}

/** Stubs ONLY the optional service stages — stage 6 opens a REAL StateStore. */
function stubsForRealStore(): Partial<Record<string, StageBody>> {
	return noopDrainBodies();
}

describe("signal classification (run.py:shutdown_signal_handler parity)", () => {
	it("takeover marker naming us ⇒ takeover (consumed first regardless of signal)", () => {
		writeTakeoverMarker(home, process.pid);
		expect(classifySignalForSelf(home, "SIGTERM")).toBe("takeover");
	});

	it("SIGINT is a planned stop BY DEFINITION — even without any marker", () => {
		expect(classifySignalForSelf(home, "SIGINT")).toBe("planned_stop");
	});

	it("SIGUSR1 IS an in-band restart by definition ⇒ service_restart (exit 75)", () => {
		// run.py:restart_signal_handler parity — the fleet updater drains
		// gateways with SIGUSR1 first (08 §7); it must NEVER read as unexpected.
		expect(classifySignalForSelf(home, "SIGUSR1")).toBe("service_restart");
	});

	it("planned-stop marker + SIGTERM ⇒ planned_stop; bare unmarked SIGTERM ⇒ unexpected_signal", () => {
		expect(classifySignalForSelf(home, "SIGTERM")).toBe("unexpected_signal");

		writePlannedStopMarker(home, process.pid);
		expect(classifySignalForSelf(home, "SIGTERM")).toBe("planned_stop");
		// The marker was consumed; repeating the signal reclassifies as unexpected.
		expect(classifySignalForSelf(home, "SIGTERM")).toBe("unexpected_signal");
	});
});

describe("recorded outcomes per shutdown class", () => {
	it("planned stop: exitCode 0 AND gateway_state=stopped persisted", async () => {
		const lifecycle = runningLifecycle({ stageBodies: noopDrainBodies() });
		await lifecycle.startup();

		const outcome = await lifecycle.requestShutdown("planned_stop");

		expect(outcome.klass).toBe("planned_stop");
		expect(outcome.exitCode).toBe(SHUTDOWN_EXIT_CODES.planned_stop);
		expect(outcome.persistedStopped).toBe(true);
		const status = readRuntimeStatus(home);
		expect(status?.gateway_state).toBe("stopped");
		expect(lifecycle.state).toBe("stopped");
	});

	it("UNEXPECTED signal: exitCode 1 and gateway_state is NEVER persisted stopped (#42675)", async () => {
		const lifecycle = runningLifecycle();
		await lifecycle.startup();
		expect(lifecycle.statusSnapshot()?.gateway_state).toBe("running");

		lifecycle.handleSignal("SIGTERM"); // no markers → unexpected
		const outcome = await lifecycle.drainSettled;

		expect(outcome.klass).toBe("unexpected_signal");
		expect(outcome.exitCode).toBe(1);
		expect(outcome.persistedStopped).toBe(false);
		expect(lifecycle.unexpectedSignalInitiated).toBe(true);
		const status = readRuntimeStatus(home);
		expect(status?.gateway_state).toBe("running"); // untouched!
		expect(status?.exit_reason ?? "").toContain("unexpected_signal");
	});

	it("takeover class exits 0 like a planned stop (systemd must not flap-fight the replacer)", async () => {
		const lifecycle = runningLifecycle();
		await lifecycle.startup();
		writeTakeoverMarker(home, process.pid);

		lifecycle.handleSignal("SIGTERM");
		const outcome = await lifecycle.drainSettled;

		expect(outcome.klass).toBe("takeover");
		expect(outcome.exitCode).toBe(0);
		expect(readRuntimeStatus(home)?.gateway_state).toBe("stopped");
	});
});

describe("ordered drain (08 §1.2) and flush-before-clear backstop (§1.3a)", () => {
	it("drain trace covers every phase IN the binding order", async () => {
		const lifecycle = runningLifecycle();
		await lifecycle.startup();

		const outcome = await lifecycle.requestShutdown("planned_stop");

		const steps = outcome.trace.map((t) => t.step);
		const expectedOrder = DRAIN_PHASE_ORDER.filter((p) => steps.includes(p));
		expect(steps).toEqual([...expectedOrder]);
		for (const phase of DRAIN_PHASE_ORDER) expect(steps).toContain(phase);
	});

	it("flush_pending_messages strictly PRECEDES clear_pending_slots (happy path)", async () => {
		const lifecycle = runningLifecycle();
		await lifecycle.startup();
		lifecycle.registerPendingMessage({ session_id: "s1", text: "queued" });

		const outcome = await lifecycle.requestShutdown("planned_stop");

		const flushIdx = outcome.trace.findIndex(
			(t) => t.step === "flush_pending_messages",
		);
		const clearIdx = outcome.trace.findIndex(
			(t) => t.step === "clear_pending_slots",
		);
		expect(flushIdx).toBeGreaterThanOrEqual(0);
		expect(clearIdx).toBeGreaterThan(flushIdx);
		// Slots actually emptied only after the flush.
		expect(lifecycle.pendingCount).toBe(0);
		expect(outcome.flushesFailed).toBe(false);
		// §1.3(a) flushes pending messages to recovery files UNCONDITIONALLY
		// (memory slots are cleared at teardown either way — the file IS the
		// durable copy until next boot replays it into the DB).
		expect(existsSync(recoveryDir(home))).toBe(true);
	});

	it("PERSISTENCE BROKEN: agent-history + pending flushes raise ⇒ recovery files land BEFORE slots clear; data survives", async () => {
		// Raw executor contract with injected hooks: the agent-history flush
		// RAISES (the FTS-corruption shape of #72680) carrying a recovery
		// snapshot on the error; pending messages have no working DB either.
		// executeDrain must serialize BOTH to files BEFORE the clear step, keep
		// teardown going, and record a clean-stop-suppressing unexpected class.
		const order: string[] = [];
		const boom = Object.assign(
			new Error("INSERT INTO messages failed: FTS corruption"),
			{
				agentHistorySnapshot: {
					sessionId: "sess-broken",
					messages: [{ role: "user", content: "only copy" }],
				},
			},
		);
		const hooks: DrainHooks = {
			async stopIngress() {
				order.push("stop_ingress");
			},
			async flushAgentHistories() {
				order.push("finalize_agent_histories-attempted");
				throw boom;
			},
		};

		const outcome = await executeDrain({
			home,
			klass: "unexpected_signal",
			graceMs: 0,
			hooks,
			takePendingSlots: () => [{ session_id: "s3", text: "survivor" }],
			log: QUIET_LOGGER,
		});

		// Binding ORDER: the finalize attempt precedes the pending flush, which
		// strictly precedes the slot clear (§1.3a).
		const idxOf = (step: string) =>
			outcome.trace.findIndex((t) => t.step === step);
		expect(order).toEqual([
			"stop_ingress",
			"finalize_agent_histories-attempted",
		]);
		expect(idxOf("finalize_agent_histories")).toBeLessThan(
			idxOf("flush_pending_messages"),
		);
		expect(idxOf("flush_pending_messages")).toBeLessThan(
			idxOf("clear_pending_slots"),
		);

		// Both payloads survived to disk — operator snapshot + pending spool.
		const files = readdirSync(recoveryDir(home));
		expect(files.some((f) => f.startsWith("agent-history-sess-broken-"))).toBe(
			true,
		);
		expect(
			files.some((f) => f.startsWith("pending-") && f.endsWith(".json")),
		).toBe(true);

		// Data survived ⇒ NOT a failed flush; teardown continued through close.
		expect(outcome.flushesFailed).toBe(false);
		expect(outcome.klass).toBe("unexpected_signal");
		expect(outcome.exitCode).toBe(1);
		expect(outcome.persistedStopped).toBe(false);
		const steps = outcome.trace.map((t) => t.step);
		expect(steps).toContain("close_database");
	});
});

describe("payload discipline (08 §1.3a)", () => {
	it("MessageEvent-shaped objects keep text+session; strings pass; others degrade to string form", () => {
		const shaped = normalizePendingPayload({
			session_id: "abc",
			text: "hello",
			platform: "x",
		});
		expect(shaped.sessionId).toBe("abc");
		expect(shaped.text).toBe("hello");

		const raw = normalizePendingPayload("plain text");
		expect(raw.sessionId).toBeNull();
		expect(raw.text).toBe("plain text");

		const dict = normalizePendingPayload({ foo: "bar" });
		expect(dict.text).toBe(JSON.stringify({ foo: "bar" }));

		const numberish = normalizePendingPayload(42);
		expect(numberish.text).toBe("42");
	});

	it("replay-on-boot recovers flushed pending messages EXACTLY once; invalid files preserved", async () => {
		// First life: register pending messages and shut down. Real StateStore
		// (only the optional service stages are stubbed).
		const first = new GatewayLifecycle({
			home,
			logger: QUIET_LOGGER,
			stateStorePath: join(home, "state.db"),
			stageBodies: stubsForRealStore(),
		});
		await first.startup();
		first.registerPendingMessage({ session_id: "sess-1", text: "recover me" });
		await first.requestShutdown("planned_stop");
		const flushed = readdirSync(recoveryDir(home)).filter((f) =>
			f.endsWith(".json"),
		);
		expect(flushed.length).toBe(1);

		// Drop an invalid file + an agent-history snapshot: both must survive.
		const { flushAgentHistoryToFile } = await import("./shutdown.js");
		flushAgentHistoryToFile(home, "sess-1", [{ role: "user" }], {
			reason: "shutdown-with-unpersisted-agent-history",
		});
		const { writeFileSync: wf } = await import("node:fs");
		wf(join(recoveryDir(home), "pending-broken.json"), "{nope", {
			mode: 0o600,
		});

		// Second boot: replay inserts through the store's message surface
		// (SessionDB-equivalent append — FTS triggers + metadata stay correct).
		const second = new GatewayLifecycle({
			home,
			logger: QUIET_LOGGER,
			stateStorePath: join(home, "state.db"),
			stageBodies: stubsForRealStore(),
		});
		await second.startup();

		const store = second.store;
		if (store === null) {
			throw new Error("stage 6 did not open a real StateStore");
		}
		const rows = store.listMessages("sess-1");
		expect(rows.some((r) => r.content === "recover me")).toBe(true);
		// Recovered file consumed exactly once…
		const remaining = readdirSync(recoveryDir(home));
		expect(remaining.some((f) => f === flushed[0])).toBe(false);
		// …while invalid + agent-history files are PRESERVED, never deleted.
		expect(remaining.some((f) => f === "pending-broken.json")).toBe(true);
		expect(remaining.length).toBe(2);
	}, 20_000);
});

describe("double-signal escalation (08 §1.2 fast-exit)", () => {
	it("second signal hard-exits AFTER releasing pid file + runtime lock", async () => {
		const exitSpy: { code: number | null } = { code: null };
		const lifecycle = new GatewayLifecycle({
			home,
			logger: QUIET_LOGGER,
			forensics: { snapshot: false, spawnDiagnostic: false },
			hardExit: (code) => {
				exitSpy.code = code;
				// At hard-exit time ownership MUST already be released (§1.3c).
				expect(getRunningPid(home)).toBeNull();
				expect(lifecycle.didEscalate).toBe(true);
			},
		});
		await lifecycle.startup();
		expect(getRunningPid(home)?.pid).toBe(process.pid);

		lifecycle.handleSignal("SIGTERM"); // schedules the drain
		lifecycle.handleSignal("SIGTERM"); // escalates immediately

		expect(exitSpy.code).toBe(1);
	});
});

describe("notify + pre-drain resume-pending phases (run.py:_stop_impl_body)", () => {
	it("notify runs FIRST (adapters still connected); pre-drain marks precede stop_ingress", async () => {
		const lifecycle = runningLifecycle();
		await lifecycle.startup();
		const order: string[] = [];
		const outcome = await executeDrain({
			home,
			klass: "planned_stop",
			graceMs: 0,
			hooks: {
				notifyActiveSessions: async () => {
					order.push("notify");
				},
				markResumePendingPreDrain: async () => {
					order.push("pre-drain-marks");
					return ["s1"];
				},
				stopIngress: async () => {
					order.push("stop_ingress");
				},
			},
			takePendingSlots: () => [],
			log: QUIET_LOGGER,
		});
		expect(order).toEqual(["notify", "pre-drain-marks", "stop_ingress"]);

		const idxOf = (step: string) =>
			outcome.trace.findIndex((t) => t.step === step);
		expect(idxOf("notify_active_sessions")).toBeLessThan(
			idxOf("pre_drain_resume_pending"),
		);
		expect(idxOf("pre_drain_resume_pending")).toBeLessThan(
			idxOf("stop_ingress"),
		);
	});

	it("graceful drain writes .clean_shutdown and CLEARS pre-drain marks; counts recorded", async () => {
		const cleared: string[][] = [];
		const outcome = await executeDrain({
			home,
			klass: "service_restart",
			graceMs: 0,
			hooks: {
				markResumePendingPreDrain: async () => ["done-s1", "done-s2"],
				clearResumePending: async (keys) => {
					cleared.push([...keys]);
				},
				activeSessionKeys: () => ["done-s1"],
			},
			takePendingSlots: () => [],
			log: QUIET_LOGGER,
		});
		expect(outcome.klass).toBe("service_restart");
		expect(outcome.exitCode).toBe(75);
		expect(outcome.timedOut).toBe(false);
		expect(outcome.cleanShutdownWritten).toBe(true);
		expect(existsSync(cleanShutdownMarkerPath(home))).toBe(true);
		expect(cleared).toEqual([["done-s1", "done-s2"]]);
		// Sessions active at teardown land in the stuck-loop counters (#7536).
		expect(readRestartFailureCounts(home)).toEqual({ "done-s1": 1 });
	});

	it("TIMED-OUT drain skips the clean marker, skips clearing, still records active sessions", async () => {
		const lifecycle = runningLifecycle();
		await lifecycle.startup();
		writeCleanShutdownMarker(home); // stale receipt from an earlier life
		const cleared: string[][] = [];

		const hooks = lifecycle["drainHooks"]();
		const outcome = await executeDrain({
			home,
			klass: "unexpected_signal",
			graceMs: 5,
			hooks: {
				...hooks,
				markResumePendingPreDrain: async () => ["stuck-s"],
				clearResumePending: async (keys) => {
					cleared.push([...keys]);
				},
				awaitActiveTurns: (graceMs) => graceMs > 0, // turns STILL running
				activeSessionKeys: () => ["stuck-s"],
			},
			takePendingSlots: () => [],
			log: QUIET_LOGGER,
		});

		expect(outcome.timedOut).toBe(true);
		expect(outcome.cleanShutdownWritten).toBe(false);
		expect(existsSync(cleanShutdownMarkerPath(home))).toBe(true); // stale receipt untouched
		expect(cleared).toEqual([]);
		expect(readRestartFailureCounts(home)).toEqual({ "stuck-s": 1 });
	});
});

describe("SIGUSR1 in-band restart (restart_signal_handler parity)", () => {
	it("handleSignal(SIGUSR1) drives the graceful drain to class service_restart / exit 75", async () => {
		const lifecycle = runningLifecycle();
		await lifecycle.startup();

		lifecycle.handleSignal("SIGUSR1");
		const outcome = await lifecycle.drainSettled;

		expect(outcome.klass).toBe("service_restart");
		expect(outcome.exitCode).toBe(SHUTDOWN_EXIT_CODES.service_restart);
		expect(outcome.exitCode).toBe(75);
		// A completed restart drain is CLEAN — the supervisor replaces us.
		expect(outcome.persistedStopped).toBe(true);
		expect(lifecycle.unexpectedSignalInitiated).toBe(false);
		expect(readRuntimeStatus(home)?.gateway_state).toBe("stopped");
	});
});
