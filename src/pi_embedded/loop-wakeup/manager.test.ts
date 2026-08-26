// Behavior contracts for the /loop orchestration surface (hermes_cli/loops.py
// LoopManager + dispatch_loop_command port): argument parsing, tick lifecycle
// (claim → evaluate → schedule), every completeTick stop arm in Hermes order,
// self-paced backoff, status lines, the shared slash dispatch texts, and the
// gateway-side route capture / mid-run control guard.

import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { afterEach, beforeEach, describe, expect, it } from "vitest";

import {
	DEFAULT_MAX_TICKS,
	LOOP_BUSY_SET_REJECT_TEXT,
	LOOP_COMPLETE_MARKER,
	LoopManager,
	LoopValueError,
	type LoopsConfig,
	dispatchLoopCommand,
	digestResponse,
	formatInterval,
	isLoopMidrunControlArg,
	parseIntervalToken,
	parseLoopArgs,
	responseSignalsComplete,
	routeFromSource,
} from "./index.js";
import { ManualClock } from "./testing/manual-clock.js";
import { StateStore } from "../../pi_state/index.js";
import type { SessionSource } from "../../pi_gateway/resolution/session-key.js";

describe("parse_interval_token", () => {
	it.each([
		["30s", 30],
		["5m", 300],
		["2h", 7200],
		["1h30m", 5400],
		["1h30m10s", 5410],
		["90S", 90],
	])("parses %s → %is", (token, expected) => {
		expect(parseIntervalToken(token)).toBe(expected);
	});

	it.each([
		[""], // empty
		["0s"], // zero total
		["120"], // bare number — collides with prompt text
		["5x"],
		["m"],
		["-5m"],
		["check the deploy"],
	])("rejects %j", (token) => {
		expect(parseIntervalToken(token)).toBeNull();
	});
});

describe("format_interval", () => {
	it.each([
		[90, "1m30s"],
		[3600, "1h"],
		[45, "45s"],
		[0, "0s"],
		[3661, "1h1m1s"],
		[60, "1m"],
	])("%is → %s", (seconds, expected) => {
		expect(formatInterval(seconds)).toBe(expected);
	});

	it("rounds half-to-even like Python round()", () => {
		expect(formatInterval(89.5)).toBe("1m30s"); // 90
		expect(formatInterval(88.5)).toBe("1m28s"); // 88, not 89
	});
});

describe("parse_loop_args", () => {
	it("parses fixed-interval prompts", () => {
		expect(parseLoopArgs("5m check the deploy status")).toEqual({
			intervalSeconds: 300,
			prompt: "check the deploy status",
			times: 0,
			until: "",
			error: null,
		});
	});

	it("parses 'every' sugar onto slash-command loops", () => {
		const parsed = parseLoopArgs("every 10m /recap");
		expect(parsed.intervalSeconds).toBe(600);
		expect(parsed.prompt).toBe("/recap");
	});

	it("no interval token ⇒ self-paced with the full prompt", () => {
		const parsed = parseLoopArgs("keep fixing the failing test until green");
		expect(parsed.intervalSeconds).toBeNull();
		expect(parsed.prompt).toBe("keep fixing the failing test until green");
	});

	it("--times caps and strips out of the prompt", () => {
		const parsed = parseLoopArgs("2m poll CI --times 30");
		expect(parsed).toMatchObject({
			intervalSeconds: 120,
			prompt: "poll CI",
			times: 30,
		});
	});

	it("--until consumes to end-of-line; --times after --until still binds", () => {
		const parsed = parseLoopArgs(
			"watch queue --until queue depth reaches zero --times 7",
		);
		expect(parsed.prompt).toBe("watch queue");
		expect(parsed.until).toBe("queue depth reaches zero");
		expect(parsed.times).toBe(7);
	});

	it("an interval-looking token inside --until stays prompt text", () => {
		// Only the FRONT token is parsed as the interval; '5m' sits mid-prompt.
		const parsed = parseLoopArgs("watch 5m things --until after 2m passes");
		expect(parsed.intervalSeconds).toBeNull();
		expect(parsed.prompt).toBe("watch 5m things");
		expect(parsed.until).toBe("after 2m passes");
		expect(parseLoopArgs("5m watch things --until 2m passes")).toMatchObject({
			intervalSeconds: 300,
			prompt: "watch things",
			until: "2m passes",
		});
	});

	it("errors: empty, interval-only, missing prompt, bad --times", () => {
		expect(parseLoopArgs("").error).toBe("empty");
		expect(parseLoopArgs("   ").error).toBe("empty");
		expect(parseLoopArgs("5m").error).toBe(
			"missing prompt (usage: /loop [interval] <prompt>)",
		);
		expect(parseLoopArgs("every").error).toBe(
			"missing prompt (usage: /loop [interval] <prompt>)",
		);
		expect(parseLoopArgs("poll --times x").error).toBe(
			"--times expects a positive integer, got 'x'",
		);
		expect(parseLoopArgs("poll --times 3.5").error).toBe(
			"--times expects a positive integer, got '3.5'",
		);
		expect(parseLoopArgs("poll --times 0").error).toBe(
			"--times expects a positive integer, got '0'",
		);
	});
});

describe("config knobs (loops.* section readers)", () => {
	it("module defaults without a getter", () => {
		expect(parseIntervalToken("30s")).toBe(30);
	});

	function cfg(section: LoopsConfig | undefined) {
		return (): LoopsConfig | undefined => section;
	}

	it("honors overrides and clamps floors", async () => {
		const dir = mkdtempSync(join(tmpdir(), "pi-gw-loop-cfg-"));
		const store = await StateStore.open(join(dir, "state.db"));
		try {
			const clock = new ManualClock();
			clock.setSeconds(1000);
			const mgr = new LoopManager({
				sessionId: "s",
				db: store.db,
				configOf: cfg({ min_interval_seconds: 45 }),
				clock,
			});
			const state = await mgr.set("tiny check", { intervalSeconds: 10 });
			expect(state.intervalSeconds).toBe(45); // raised to configured floor

			const unlimited = new LoopManager({
				sessionId: "u",
				db: store.db,
				configOf: cfg({ max_ticks: 0, self_paced_floor_seconds: 15 }),
				clock,
			});
			const paced = await unlimited.set("pace me");
			expect(paced.maxTicks).toBe(0); // explicit unlimited survives
			expect(paced.currentDelay).toBe(15);

			const mgr2 = new LoopManager({
				sessionId: "c",
				db: store.db,
				configOf: cfg({ min_interval_seconds: 2 }), // below absolute floor 5
				clock,
			});
			expect(
				(await mgr2.set("x", { intervalSeconds: 1 })).intervalSeconds,
			).toBe(5);
		} finally {
			await store.close(false);
			rmSync(dir, { recursive: true, force: true });
		}
	});

	it("garbage knob values fail safe to defaults", async () => {
		const dir = mkdtempSync(join(tmpdir(), "pi-gw-loop-cfg2-"));
		const store = await StateStore.open(join(dir, "state.db"));
		try {
			const clock = new ManualClock();
			const mgr2 = new LoopManager({
				sessionId: "g",
				db: store.db,
				configOf: cfg({
					min_interval_seconds: "banana",
					max_ticks: "12",
					self_paced_floor_seconds: "not-a-number",
				}),
				clock,
			});
			// sub-floor request with a GARBAGE min knob falls back to the default 30
			const tiny = await mgr2.set("p", { intervalSeconds: 10 });
			expect(tiny.intervalSeconds).toBe(30);
			const paced = await mgr2.set("p2");
			expect(paced.maxTicks).toBe(12); // numeric string coerces
			expect(paced.currentDelay).toBe(60); // garbage floor → default 60
		} finally {
			await store.close(false);
			rmSync(dir, { recursive: true, force: true });
		}
	});
});

describe("LoopManager lifecycle over a real store", () => {
	let dir: string;
	let store: StateStore;
	let clock: ManualClock;

	beforeEach(async () => {
		dir = mkdtempSync(join(tmpdir(), "pi-gw-loop-mgr-"));
		store = await StateStore.open(join(dir, "state.db"));
		clock = new ManualClock();
		clock.setSeconds(1_775_000_000);
	});

	afterEach(async () => {
		await store.close(false);
		rmSync(dir, { recursive: true, force: true });
	});

	function manager(sessionId = "sess"): LoopManager {
		return new LoopManager({ sessionId, db: store.db, clock });
	}

	it("set() persists an active interval loop with route and budget", async () => {
		const mgr = manager();
		const state = await mgr.set("watch the queue", {
			intervalSeconds: 300,
			until: "queue empty",
			times: 4,
			route: { platform: "telegram", chat_id: "42", chat_type: "dm" },
		});
		expect(state.status).toBe("active");
		expect(state.mode).toBe("interval");
		expect(state.intervalSeconds).toBe(300);
		expect(state.nextDueAt).toBe(1_775_000_300);
		expect(state.maxTicks).toBe(DEFAULT_MAX_TICKS);
		expect(state.route).toEqual({
			platform: "telegram",
			chat_id: "42",
			chat_type: "dm",
		});
		expect(store.loadLoop("sess")).toEqual(state);
		expect(mgr.isActive()).toBe(true);
		expect(mgr.hasLoop()).toBe(true);
	});

	it("set() rejects empty prompts with the ValueError arm", async () => {
		await expect(manager().set("   ")).rejects.toThrow(LoopValueError);
	});

	it("self-paced set starts at the floor", async () => {
		const state = await manager().set("refine until green");
		expect(state.mode).toBe("self_paced");
		expect(state.intervalSeconds).toBe(0);
		expect(state.currentDelay).toBe(60);
		expect(state.nextDueAt).toBe(1_775_000_060);
	});

	it("pause/resume re-arm near-now; resume of nothing fails honestly", async () => {
		const mgr = manager();
		expect(await mgr.resume()).toBeNull(); // no loop at all

		await mgr.set("p", { intervalSeconds: 3600 });
		clock.setSeconds(1_775_001_200);
		const paused = await mgr.pause();
		expect(paused?.status).toBe("paused");
		expect(paused?.pausedReason).toBe("user-paused");
		expect(paused?.awaitingResponse).toBe(false);
		expect(await mgr.pause()).not.toBeNull(); // pause idempotent on paused

		clock.setSeconds(1_775_009_999); // long pause must not fire instantly N times
		const resumed = await mgr.resume();
		expect(resumed?.status).toBe("active");
		expect(resumed?.nextDueAt).toBeLessThanOrEqual(1_775_010_000 + 5);
		expect(resumed?.nextDueAt).toBeGreaterThan(1_775_010_000);
		expect(resumed?.pausedReason).toBeNull();

		// resume of a cleared loop fails honestly
		await mgr.clear();
		expect(await mgr.resume()).toBeNull();
	});

	it("clear stops the loop and reports honestly", async () => {
		const mgr = manager();
		expect(await mgr.clear()).toBe(false);
		await mgr.set("x", { intervalSeconds: 60 });
		expect(await mgr.clear()).toBe(true);
		expect(mgr.state).toBeNull();
		expect(store.loadLoop("sess")?.status).toBe("cleared"); // audit preserved
		expect(await mgr.clear()).toBe(false);
	});

	it("markDone stamps the stop reason and clears awaiting", async () => {
		const mgr = manager();
		await mgr.set("x", { intervalSeconds: 60 });
		await mgr.markDone("user said so");
		const s = mgr.state;
		expect(s?.status).toBe("done");
		expect(s?.lastStopReason).toBe("user said so");
		expect(s?.awaitingResponse).toBe(false);
	});

	it("refresh() picks up cross-process mutations", async () => {
		const writer = manager("shared");
		await writer.set("from elsewhere", { intervalSeconds: 60 });
		const reader = manager("shared");
		expect(reader.state?.prompt).toBe("from elsewhere"); // constructed fresh
		await writer.set("changed under us", { intervalSeconds: 90 });
		reader.refresh();
		expect(reader.state?.prompt).toBe("changed under us");
	});

	describe("tick lifecycle", () => {
		it("is_due gates on active+idle+clock", async () => {
			const mgr = manager();
			expect(mgr.isDue()).toBe(false); // no loop
			await mgr.set("x", { intervalSeconds: 300 }); // due at ...300
			expect(mgr.isDue()).toBe(false);
			clock.setSeconds(1_775_000_299);
			expect(mgr.isDue()).toBe(false);
			clock.setSeconds(1_775_000_300);
			expect(mgr.isDue()).toBe(true);
			await mgr.pause();
			expect(mgr.isDue()).toBe(false); // paused never fires
		});

		it("fire_tick claims exactly once and provisionally reschedules", async () => {
			const mgr = manager();
			await mgr.set("check CI", { intervalSeconds: 300 });
			clock.setSeconds(1_775_000_301);

			const wakeup = await mgr.fireTick();
			expect(wakeup).toContain("[/loop wakeup #1, every 5m]");
			expect(wakeup).toContain("Recurring task: check CI");
			expect(wakeup).toContain(LOOP_COMPLETE_MARKER);
			const s = mgr.state!;
			expect(s.ticksFired).toBe(1);
			expect(s.awaitingResponse).toBe(true);
			expect(s.lastFiredAt).toBe(1_775_000_301);
			expect(s.nextDueAt).toBe(1_775_000_601); // provisional from NOW

			expect(await mgr.fireTick()).toBeNull(); // awaiting_response blocks
			clock.setSeconds(1_775_001_000);
			expect(await mgr.fireTick()).toBeNull(); // still blocked past due time
		});

		it("fire_tick returns the RAW command for slash prompts", async () => {
			const mgr = manager();
			await mgr.set("/recap latest", { intervalSeconds: 600 });
			clock.setSeconds(1_775_000_700);
			expect(await mgr.fireTick()).toBe("/recap latest");
		});

		it("the --until variant renders the evidence template", async () => {
			const mgr = manager();
			await mgr.set("watch queue", {
				intervalSeconds: 60,
				until: "queue empty",
			});
			clock.setSeconds(1_775_000_060);
			const wakeup = await mgr.fireTick();
			expect(wakeup).toContain("Stop condition: queue empty");
			expect(wakeup).toContain("show concrete evidence");
		});

		it("abandon_tick rolls the claim back; no-op when nothing in flight", async () => {
			const mgr = manager();
			await mgr.abandonTick(); // no state
			await mgr.set("x", { intervalSeconds: 60 });
			await mgr.abandonTick(); // not awaiting
			expect(mgr.state?.ticksFired).toBe(0);

			clock.setSeconds(1_775_000_060);
			await mgr.fireTick();
			expect(mgr.state?.ticksFired).toBe(1);
			await mgr.abandonTick();
			expect(mgr.state?.ticksFired).toBe(0);
			expect(mgr.state?.awaitingResponse).toBe(false);
			expect(store.loadLoop("sess")?.ticksFired).toBe(0); // persisted rollback
		});
	});

	describe("complete_tick arms (Hermes order)", () => {
		async function fired(opts?: Parameters<LoopManager["set"]>[1]) {
			const mgr = manager();
			await mgr.set("task", { intervalSeconds: 60, ...opts });
			clock.setSeconds(1_775_000_060);
			await mgr.fireTick();
			return mgr;
		}

		it("arm 1: LOOP_COMPLETE marker ends the loop with the ✓ message", async () => {
			const mgr = await fired();
			const decision = await mgr.completeTick(
				`all good\n${LOOP_COMPLETE_MARKER}\n`,
			);
			expect(decision).toEqual({
				status: "done",
				stopped: true,
				reason: "agent signaled the task is complete",
				message: "✓ Loop finished after 1 tick — task complete.",
			});
			expect(mgr.state?.status).toBe("done");
		});

		it("marker requires its OWN line — mid-sentence mentions keep looping", async () => {
			const fired = async (
				id: string,
				atSeconds: number,
			): Promise<LoopManager> => {
				const m = new LoopManager({ sessionId: id, db: store.db, clock });
				await m.set("task", { intervalSeconds: 60 });
				clock.setSeconds(atSeconds);
				await m.fireTick();
				return m;
			};

			const mgr = await fired("own-line-a", 1_775_000_100);
			const decision = await mgr.completeTick(
				`I will not say ${LOOP_COMPLETE_MARKER} prematurely`,
			);
			expect(decision.stopped).toBe(false);
			expect(mgr.state?.status).toBe("active");

			const punctuated = await fired("own-line-b", 1_775_000_200);
			const d2 = await punctuated.completeTick(
				`  ${LOOP_COMPLETE_MARKER}!  \n`,
			);
			expect(d2.stopped).toBe(true); // whitespace/trailing punctuation tolerated
		});

		it("arm 2: --until judge done verdict stops with evidence reason", async () => {
			const judgeMgr = new LoopManager({
				sessionId: "judged",
				db: store.db,
				clock,
				judge: (until) =>
					until.includes("green")
						? { verdict: "done", reason: "all criteria met" }
						: { verdict: "continue", reason: "not yet" },
			});
			await judgeMgr.set("task", { intervalSeconds: 60, until: "suite green" });
			clock.setSeconds(1_775_000_060);
			await judgeMgr.fireTick();
			const decision = await judgeMgr.completeTick(
				"tests pass, suite green now",
			);
			expect(decision.stopped).toBe(true);
			expect(decision.message).toContain("— all criteria met");
			expect(judgeMgr.state?.lastStopReason).toBe(
				"stop condition met: all criteria met",
			);
		});

		it("arm 2 fail-open: absent judge continues the loop (never wedges)", async () => {
			const mgr = await fired({ until: "suite green" });
			const decision = await mgr.completeTick("still red but trying");
			expect(decision).toMatchObject({
				status: "active",
				stopped: false,
				reason: "loop continues",
			});
			expect(mgr.state?.status).toBe("active");
		});

		it("arm 2 fail-open: a throwing judge continues too", async () => {
			const dir2 = mkdtempSync(join(tmpdir(), "pi-gw-loop-judge-"));
			const store2 = await StateStore.open(join(dir2, "state.db"));
			try {
				const mgr = new LoopManager({
					sessionId: "j",
					db: store2.db,
					clock,
					judge: () => {
						throw new Error("aux LLM down");
					},
				});
				await mgr.set("t", { intervalSeconds: 60, until: "x" });
				clock.setSeconds(1_775_000_060);
				await mgr.fireTick();
				const decision = await mgr.completeTick("evidence here");
				expect(decision.stopped).toBe(false);
			} finally {
				await store2.close(false);
				rmSync(dir2, { recursive: true, force: true });
			}
		});

		it("arm 3: --times cap completes with the ran-N/N message", async () => {
			const mgr = await fired({ times: 2 });
			await mgr.completeTick("run A");
			clock.setSeconds(1_775_000_121); // next due per reschedule
			expect(await mgr.fireTick()).not.toBeNull();
			const decision = await mgr.completeTick("run B");
			expect(decision).toEqual({
				status: "done",
				stopped: true,
				reason: "completed the requested 2 runs",
				message: "✓ Loop finished — ran 2/2 times.",
			});
		});

		it("arm 4: max_ticks backstop PAUSES (recoverable), not done", async () => {
			// Simulate a config backstop of 1 baked into the persisted row.
			const mgr = manager();
			await mgr.set("budget case");
			mgr.state!.maxTicks = 1;
			await store.saveLoop("sess", mgr.state!);

			clock.setSeconds(1_775_000_060);
			await mgr.fireTick();
			const decision = await mgr.completeTick("one run only");
			expect(decision.status).toBe("paused");
			expect(decision.reason).toBe("tick budget exhausted (1/1)");
			expect(decision.message).toContain("⏸ Loop paused — 1/1 ticks used");
			expect(mgr.state?.status).toBe("paused");
		});

		it("arm 5: still looping reschedules FROM TURN END", async () => {
			const mgr = await fired();
			clock.setSeconds(1_775_000_459); // turn took ~7 minutes
			const decision = await mgr.completeTick("checked, all quiet");
			expect(decision).toEqual({
				status: "active",
				stopped: false,
				reason: "loop continues",
				message: "",
			});
			expect(mgr.state?.nextDueAt).toBe(1_775_000_459 + 60);
			expect(mgr.state?.awaitingResponse).toBe(false);
		});

		it("complete_tick with no tick in flight is an honest no-op", async () => {
			const mgr = manager();
			expect(await mgr.completeTick("hello")).toEqual({
				status: null,
				stopped: false,
				reason: "no tick in flight",
				message: "",
			});
			await mgr.set("x", { intervalSeconds: 60 });
			expect((await mgr.completeTick("hello")).reason).toBe(
				"no tick in flight",
			);
		});
	});

	describe("self-paced backoff", () => {
		it("backs off ×2 while replies are unchanged, resets to floor on change", async () => {
			const mgr = manager();
			await mgr.set("refine the failing test");
			clock.setSeconds(1_775_000_060);
			await mgr.fireTick();

			await mgr.completeTick("checked: still failing at 14:02:33");
			expect(mgr.state?.currentDelay).toBe(60); // first evaluation: changed → floor
			const d1 = mgr.state?.lastResponseDigest ?? "";

			clock.setSeconds(1_775_000_130);
			await mgr.fireTick();
			await mgr.completeTick("checked: still failing at 15:22:00");
			expect(mgr.state?.currentDelay).toBe(120); // identical modulo timestamps
			expect(mgr.state?.lastResponseDigest).toBe(d1);

			clock.setSeconds(1_775_000_260);
			await mgr.fireTick();
			await mgr.completeTick("NEW FAILURE SIGNATURE detected");
			expect(mgr.state?.currentDelay).toBe(60); // change snapped back to floor
		});

		it("backoff caps at the ceiling (default 15m)", async () => {
			const mgr = manager();
			await mgr.set("pace me");
			let t = 1_775_000_000;
			for (let i = 0; i < 8; i++) {
				t += mgr.state!.currentDelay;
				clock.setSeconds(t);
				await mgr.fireTick();
				await mgr.completeTick("nothing changed yet");
			}
			expect(mgr.state?.currentDelay).toBe(900); // 60→120→240→480→960 capped
		});
	});

	describe("digest normalization", () => {
		it("strips clock/timestamp/duration tokens before hashing", () => {
			const a = digestResponse(
				"Checked at 14:02:33 on 2026-07-26 — retry in 25 minutes.",
			);
			const b = digestResponse(
				"checked at 09:11 on 1999-01-01 — retry in 3 hours.",
			);
			expect(a).toBe(b);
			expect(digestResponse("queue depth 5")).not.toBe(
				digestResponse("queue depth 6"),
			);
			expect(digestResponse("")).toBe(digestResponse("")); // stable on empty
		});

		it("response_signals_complete matches own-line markers only", () => {
			expect(responseSignalsComplete(`a\n${LOOP_COMPLETE_MARKER}`)).toBe(true);
			expect(responseSignalsComplete(`${LOOP_COMPLETE_MARKER}.`)).toBe(true);
			expect(responseSignalsComplete(`x ${LOOP_COMPLETE_MARKER} y`)).toBe(
				false,
			);
			expect(responseSignalsComplete("")).toBe(false);
		});
	});

	describe("status_line", () => {
		it("renders each lifecycle shape byte-stably", async () => {
			const none = manager("none");
			expect(none.statusLine()).toBe(
				"No loop set. Start one with /loop [interval] <prompt>.",
			);

			const mgr = manager();
			await mgr.set("watch deploy", { intervalSeconds: 300 });
			expect(mgr.statusLine(1_775_000_100)).toBe(
				"↻ Loop (active, every 5m, 0/100 budget, next in 3m20s): watch deploy",
			);
			expect(mgr.statusLine(1_775_000_400)).toContain("due now");
			clock.setSeconds(1_775_000_400); // fireTick reads the injected clock
			await mgr.fireTick();
			expect(mgr.statusLine()).toContain(", wakeup running");

			await mgr.abandonTick();
			await mgr.pause("waiting on user");
			expect(mgr.statusLine()).toBe(
				"⏸ Loop (paused, every 5m, 0/100 budget — waiting on user): watch deploy",
			);
			await mgr.resume();
			await mgr.markDone("agent signaled the task is complete");
			expect(mgr.statusLine()).toBe(
				"✓ Loop finished (0 ticks — agent signaled the task is complete): watch deploy",
			);
		});

		it("--times rows show runs caps; --until appends the condition", async () => {
			const mgr = manager();
			await mgr.set("poll", {
				intervalSeconds: 60,
				times: 5,
				until: "done flag",
			});
			expect(mgr.statusLine(1_775_000_030)).toBe(
				"↻ Loop (active, every 1m, 0/5 runs, until: done flag, next in 30s): poll",
			);
		});
	});
});

describe("dispatch_loop_command (surface-agnostic dispatch)", () => {
	let dir: string;
	let store: StateStore;
	let clock: ManualClock;

	beforeEach(async () => {
		dir = mkdtempSync(join(tmpdir(), "pi-gw-loop-dispatch-"));
		store = await StateStore.open(join(dir, "state.db"));
		clock = new ManualClock();
		clock.setSeconds(1_775_000_000);
	});

	afterEach(async () => {
		await store.close(false);
		rmSync(dir, { recursive: true, force: true });
	});

	function manager(): LoopManager {
		return new LoopManager({ sessionId: "d", db: store.db, clock });
	}

	it("bare /loop and /loop status print the status line", async () => {
		expect(await dispatchLoopCommand(manager(), "")).toEqual({
			output: "No loop set. Start one with /loop [interval] <prompt>.",
			created: false,
		});
		await manager().set("x", { intervalSeconds: 60 });
		const result = await dispatchLoopCommand(manager(), "status");
		expect(result.output).toContain("↻ Loop (active");
		expect(result.created).toBe(false);
	});

	it.each(["stop", "clear", "cancel"])(
		"/loop %s stops politely",
		async (verb) => {
			expect(await dispatchLoopCommand(manager(), verb)).toEqual({
				output: "No active loop.",
				created: false,
			});
			const mgr = manager();
			await mgr.set("y", { intervalSeconds: 60 });
			const result = await dispatchLoopCommand(mgr, verb);
			expect(result.output).toBe("✓ Loop stopped.");
			expect(store.loadLoop("d")?.status).toBe("cleared");
		},
	);

	it("pause/resume report their control outputs", async () => {
		expect(await dispatchLoopCommand(manager(), "pause")).toEqual({
			output: "No loop set.",
			created: false,
		});
		const mgr = manager();
		await mgr.set("y", { intervalSeconds: 60 });
		expect((await dispatchLoopCommand(mgr, "pause")).output).toBe(
			"⏸ Loop paused: y\nUse /loop resume to continue.",
		);
		const resumed = await dispatchLoopCommand(mgr, "resume");
		expect(resumed.output).toBe("▶ Loop resumed (every 1m): y");
	});

	it("/loop help prints the usage block ending with the sentinel note", async () => {
		const { output } = await dispatchLoopCommand(manager(), "--help");
		const lines = output.split("\n");
		expect(lines[0]).toBe(
			"Usage: /loop [interval] <prompt> [--times N] [--until <condition>]",
		);
		expect(lines.at(-1)).toContain(LOOP_COMPLETE_MARKER);
		expect(output).toContain(
			"self-paced (backs off while output is unchanged)",
		);
	});

	it("creation assembles the confirmation block in order", async () => {
		const result = await dispatchLoopCommand(
			manager(),
			"5m check deploy --times 3",
			{
				route: { platform: "slack", chat_id: "C9", chat_type: "channel" },
			},
		);
		expect(result.created).toBe(true);
		// --times rows suppress the backstop line (Hermes `not state.times and …`)
		expect(result.output.split("\n")).toEqual([
			"↻ Loop set (every 5m): check deploy",
			"Runs 3 times, then stops.",
			`First wakeup next in 5m. Controls: /loop status · pause · resume · stop.`,
		]);
		const row = store.loadLoop("d");
		expect(row?.route).toEqual({
			platform: "slack",
			chat_id: "C9",
			chat_type: "channel",
		});
	});

	it("uncapped rows surface the config backstop budget line", async () => {
		const { output } = await dispatchLoopCommand(manager(), "5m plain task");
		expect(output.split("\n")).toEqual([
			"↻ Loop set (every 5m): plain task",
			"Backstop budget: 100 ticks (loops.max_ticks; 0 = unlimited).",
			`First wakeup next in 5m. Controls: /loop status · pause · resume · stop.`,
		]);
	});

	it("replacing a loop notes it on line two", async () => {
		await manager().set("old task", { intervalSeconds: 60 });
		const result = await dispatchLoopCommand(manager(), "10m new task");
		const lines = result.output.split("\n");
		expect(lines[0]).toBe("↻ Loop set (every 10m): new task");
		expect(lines[1]).toBe("(replaced the previous loop for this session)");
	});

	it("sub-floor intervals raise with the minimum note", async () => {
		const result = await dispatchLoopCommand(manager(), "10s quick probe");
		const lines = result.output.split("\n");
		expect(lines[0]).toBe("↻ Loop set (every 30s): quick probe");
		expect(lines[1]).toBe(
			"(interval raised to the 30s minimum — loops.min_interval_seconds)",
		);
	});

	it("self-paced creation explains the backoff envelope", async () => {
		const { output } = await dispatchLoopCommand(
			manager(),
			"keep polishing docs",
		);
		expect(output.split("\n")[1]).toBe(
			"Self-paced: first check in 1m; backs off up to 15m while nothing changes.",
		);
	});

	it("--until creation states the stop condition; errors pass through", async () => {
		const ok = await dispatchLoopCommand(manager(), "5m watch --until calm");
		expect(ok.output).toContain("Stops when: calm");

		const bad = await dispatchLoopCommand(manager(), "5m");
		expect(bad).toEqual({
			output: "/loop: missing prompt (usage: /loop [interval] <prompt>)",
			created: false,
		});
	});
});

describe("gateway seams (run.py:_busy_loop_command + route capture)", () => {
	it("mid-run control args are exact-match on the stripped lowercase arg", () => {
		for (const arg of [
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
		]) {
			expect(isLoopMidrunControlArg(arg)).toBe(true);
			expect(isLoopMidrunControlArg(`  ${arg.toUpperCase()} `)).toBe(true);
		}
		// setting a NEW loop (any other payload) is NOT safe mid-run
		expect(isLoopMidrunControlArg("5m check again")).toBe(false);
		expect(isLoopMidrunControlArg("status extra")).toBe(false);
	});

	it("the mid-run rejection text is the run.py byte shape", () => {
		expect(LOOP_BUSY_SET_REJECT_TEXT).toBe(
			"Agent is running — use /loop status / pause / stop mid-run, or /stop before setting a new loop.",
		);
	});

	it("routeFromSource captures the five dimensions and drops empties", () => {
		const source: SessionSource = {
			platform: "whatsapp",
			chatType: "dm",
			userId: "15551234567",
			chatId: "15550001111",
			threadId: "",
		};
		expect(routeFromSource(source)).toEqual({
			platform: "whatsapp",
			chat_id: "15550001111",
			chat_type: "dm",
			user_id: "15551234567",
		});
		expect(routeFromSource(undefined)).toEqual({});
	});
});
