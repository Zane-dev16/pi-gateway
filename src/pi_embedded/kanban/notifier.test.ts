// Behavior contracts for the embedded kanban notifier (07 §6; port of
// gateway/kanban_watchers.py:_kanban_notifier_watcher):
//   • terminal-event claim/deliver/advance-cursor discipline (no replay),
//   • silent claimed kinds (archived/unblocked advance WITHOUT delivering),
//   • unsubscribe ONLY on archive (done stays reversible),
//   • transient send-failure rewind + MAX_SEND_FAILURES drop,
//   • hourly stale-done-sub GC with retention disable,
//   • per-subscription and per-tick failure isolation,
//   • the DEC-040 optional-stage entry mappings.
// Every timing assertion runs on ManualClock or direct deterministic tick
// drives — no wall-clock reads anywhere.

import Database from "better-sqlite3";

import { describe, expect, it } from "vitest";

import {
	SqliteKanbanNotifyStore,
	subKeyOf,
	type ClaimedEvents,
	type NotifyEvent,
	type NotifySubStore,
	type NotifySubscription,
	type NotifyTaskView,
	type SubKey,
} from "./notify-store.js";
import {
	MAX_SEND_FAILURES,
	NOTIFY_TERMINAL_KINDS,
	SILENT_EVENT_KINDS,
	renderNotifyMessage,
	resolveNotifierServiceConfig,
	runNotifierTick,
	startKanbanNotifier,
	KANBAN_NOTIFY_IN_GATEWAY_ENV,
} from "./notifier.js";
import {
	kanbanNotifierServiceEntry,
	KANBAN_NOTIFIER_SERVICE_NAME,
} from "./notifier-stage-entry.js";
import type { GatewayClock } from "./clock.js";
import { ManualClock } from "./testing/manual-clock.js";
import { openKanbanHarness } from "./testing/harness.js";

const TICK_NOW = 1_777_000_000;

// ── in-memory fake store (claim semantics mirror the SQLite CAS store) ─────

class FakeNotifyStore implements NotifySubStore {
	readonly board: string;
	subs = new Map<string, NotifySubscription>();
	events = new Map<string, NotifyEvent[]>();
	tasks = new Map<string, NotifyTaskView>();
	listSubsError: Error | null = null;

	constructor(board = "default") {
		this.board = board;
	}

	addTask(task: Partial<NotifyTaskView> & { id: string }): void {
		this.tasks.set(task.id, {
			title: "",
			status: "todo",
			assignee: null,
			result: null,
			...task,
		});
	}

	/** Subscribe CAUGHT UP (parity add_notify_sub cursor snapping). */
	addSub(input: {
		taskId: string;
		platform: string;
		chatId: string;
		threadId?: string;
	}): void {
		const newest = this.events.get(input.taskId)?.at(-1)?.id ?? 0;
		const sub: NotifySubscription = {
			taskId: input.taskId,
			platform: input.platform,
			chatId: input.chatId,
			threadId: input.threadId ?? "",
			lastEventId: newest,
		};
		this.subs.set(subKeyOf(sub), sub);
	}

	/** Force a legacy cursor shape (rows created before events existed). */
	resetCursor(sub: SubKey, to: number): void {
		const live = this.subs.get(subKeyOf(sub));
		if (live) live.lastEventId = to;
	}

	appendEvent(
		taskId: string,
		kind: string,
		id: number,
		payload?: unknown,
		createdAt = TICK_NOW,
	): void {
		const list = this.events.get(taskId) ?? [];
		list.push({
			id,
			taskId,
			kind,
			payload:
				typeof payload === "object" && payload !== null
					? (payload as Record<string, unknown>)
					: null,
			createdAt,
		});
		this.events.set(taskId, list);
	}

	async listSubs(): Promise<NotifySubscription[]> {
		if (this.listSubsError !== null) throw this.listSubsError;
		return [...this.subs.values()];
	}

	async claimUnseenEvents(
		sub: SubKey,
		kinds: readonly string[],
	): Promise<ClaimedEvents | null> {
		const live = this.subs.get(subKeyOf(sub));
		if (!live) return null;
		const kindSet = new Set(kinds);
		const unseen = (this.events.get(sub.taskId) ?? []).filter(
			(e) => e.id > live.lastEventId && kindSet.has(e.kind),
		);
		if (unseen.length === 0) {
			return {
				oldCursor: live.lastEventId,
				newCursor: live.lastEventId,
				events: [],
			};
		}
		const oldCursor = live.lastEventId;
		const newCursor = unseen[unseen.length - 1]!.id;
		live.lastEventId = newCursor; // THE atomic claim
		return { oldCursor, newCursor, events: unseen };
	}

	async advanceCursor(sub: SubKey, newCursor: number): Promise<void> {
		const live = this.subs.get(subKeyOf(sub));
		if (live) live.lastEventId = newCursor;
	}

	async rewindCursor(
		sub: SubKey,
		claimedCursor: number,
		oldCursor: number,
	): Promise<boolean> {
		const live = this.subs.get(subKeyOf(sub));
		if (!live || live.lastEventId !== claimedCursor) return false;
		live.lastEventId = oldCursor;
		return true;
	}

	async removeSub(sub: SubKey): Promise<boolean> {
		return this.subs.delete(subKeyOf(sub));
	}

	async getTask(taskId: string): Promise<NotifyTaskView | null> {
		return this.tasks.get(taskId) ?? null;
	}

	async purgeStaleDoneSubs(opts: {
		maxAgeDays: number;
		nowSeconds: number;
	}): Promise<number> {
		if (opts.maxAgeDays <= 0) return 0;
		let purged = 0;
		for (const sub of [...this.subs.values()]) {
			const task = this.tasks.get(sub.taskId);
			const newest = this.events.get(sub.taskId)?.at(-1);
			if (
				task?.status === "done" &&
				opts.nowSeconds - (newest?.createdAt ?? 0) > opts.maxAgeDays * 86400
			) {
				this.subs.delete(subKeyOf(sub));
				purged += 1;
			}
		}
		return purged;
	}
}

function recorder(): {
	sent: Array<{ chat: string; message: string }>;
	deliver: (sub: NotifySubscription, message: string) => Promise<void>;
	failNext: () => void;
} {
	const sent: Array<{ chat: string; message: string }> = [];
	const state = { failNext: false };
	return {
		sent,
		async deliver(sub, message) {
			if (state.failNext) {
				state.failNext = false;
				throw new Error("adapter send reported failure");
			}
			sent.push({ chat: `${sub.platform}:${sub.chatId}`, message });
		},
		failNext() {
			state.failNext = true;
		},
	};
}

const TICK_OPTS_BASE = { nowSeconds: TICK_NOW } as const;

// ── rendering ──────────────────────────────────────────────────────────────

describe("renderNotifyMessage — message-shape parity", () => {
	const task: NotifyTaskView = {
		id: "t1",
		title: "Ship the thing",
		status: "running",
		assignee: "alice",
		result: null,
	};

	it("completed carries worker handoff summary (first line, payload wins)", () => {
		const msg = renderNotifyMessage(
			{
				id: 1,
				taskId: "t1",
				kind: "completed",
				payload: { summary: "line1\nline2" },
				createdAt: 0,
			},
			task,
			"default",
		);
		// ✔ prefix + board/identity tags are watcher parity
		// (kanban_watchers.py drift re-audit vs upstream@77001a6b).
		expect(msg).toBe(
			"✔ [default] @alice Kanban t1 done — Ship the thing\nline1",
		);
	});

	it("completed task.result fallback slices at 160, not 200 (watcher parity)", () => {
		const msg = renderNotifyMessage(
			{ id: 1, taskId: "t1", kind: "completed", payload: null, createdAt: 0 },
			{ ...task, result: "r".repeat(180) },
			null,
		);
		expect(msg).toBe(
			`✔ @alice Kanban t1 done — Ship the thing\n${"r".repeat(160)}`,
		);
	});

	it("blocked includes the block reason (raw 160 slice, no clamp)", () => {
		const msg = renderNotifyMessage(
			{
				id: 2,
				taskId: "t1",
				kind: "blocked",
				payload: { reason: "needs creds" },
				createdAt: 0,
			},
			null,
			null,
		);
		expect(msg).toBe("⏸ Kanban t1 blocked: needs creds");
		// Reasons beyond 160 chars are SLICED raw — no whitespace collapse and
		// no [local path]/[REDACTED] scrub on this kind (watcher parity).
		const long = "a ".repeat(120);
		expect(
			renderNotifyMessage(
				{
					id: 2,
					taskId: "t1",
					kind: "blocked",
					payload: { reason: long },
					createdAt: 0,
				},
				null,
				null,
			),
		).toContain(`blocked: ${long.slice(0, 160)}`);
	});

	it("gave_up / crashed / timed_out / status shapes", () => {
		expect(
			renderNotifyMessage(
				{
					id: 3,
					taskId: "t1",
					kind: "gave_up",
					payload: { error: "boom" },
					createdAt: 0,
				},
				task,
				null,
			),
		).toContain("✖ @alice Kanban t1 gave up after repeated spawn failures");
		expect(
			renderNotifyMessage(
				{ id: 4, taskId: "t1", kind: "crashed", payload: null, createdAt: 0 },
				task,
				null,
			),
		).toContain(
			"✖ @alice Kanban t1 worker crashed (pid gone); dispatcher will retry",
		);
		expect(
			renderNotifyMessage(
				{
					id: 5,
					taskId: "t1",
					kind: "timed_out",
					payload: { limit_seconds: 600 },
					createdAt: 0,
				},
				task,
				null,
			),
		).toContain("⏱ @alice Kanban t1 timed out (max_runtime=600s); will retry");
		expect(
			renderNotifyMessage(
				{
					id: 6,
					taskId: "t1",
					kind: "status",
					payload: { status: "review" },
					createdAt: 0,
				},
				task,
				null,
			),
		).toContain("🔄 @alice Kanban t1 \u2192 review");
	});

	it("review_requested wakes with RAW summary; block_loop pings TRIAGE loudly", () => {
		// 👀 + the RAW multi-line summary slice — no whitespace collapse, no
		// external-delivery clamp (kanban_watchers.py sends it raw).
		const review = renderNotifyMessage(
			{
				id: 7,
				taskId: "t1",
				kind: "review_requested",
				payload: { summary: "line a\nline b" },
				createdAt: 0,
			},
			task,
			null,
		);
		expect(review).toBe(
			"👀 @alice Kanban t1 ready for review — Ship the thing\nline a\nline b",
		);
		const triage = renderNotifyMessage(
			{
				id: 8,
				taskId: "t1",
				kind: "block_loop_detected",
				payload: { reason: "flaky", recurrences: 3 },
				createdAt: 0,
			},
			task,
			null,
		);
		expect(triage).toBe(
			"🛑 @alice Kanban t1 routed to TRIAGE — needs a human decision (blocked 3x for the same cause): flaky",
		);
	});

	it("long titles truncate at 120 chars (chat-legibility parity)", () => {
		const long: NotifyTaskView = { ...task, title: "x".repeat(300) };
		const msg = renderNotifyMessage(
			{ id: 9, taskId: "t1", kind: "completed", payload: null, createdAt: 0 },
			long,
			null,
		);
		expect(msg.length).toBeLessThan(200);
		expect(msg).not.toContain("x".repeat(121));
	});
});

describe("terminal-event vocabulary", () => {
	it("claims the FULL Hermes TERMINAL_KINDS set", () => {
		expect([...NOTIFY_TERMINAL_KINDS].sort()).toEqual(
			[
				"archived",
				"block_loop_detected",
				"blocked",
				// kanban_watchers.py:TERMINAL_KINDS now includes the review-lane
				// reviewer-BLOCK kind — unclaimed, it pinged nobody.
				"changes_requested",
				"completed",
				"crashed",
				"gave_up",
				"review_requested",
				"status",
				"timed_out",
				"unblocked",
			].sort(),
		);
	});

	it("renders changes_requested with redacted reason + provenance (kanban_watchers.py parity)", () => {
		const event: NotifyEvent = {
			id: 1,
			taskId: "t_chg",
			kind: "changes_requested",
			payload: {
				reason:
					"see /home/alice/secrets.txt for the failing case, token sk-abcdefghijklmnopqrstuvwx",
				reviewer: "reviewer-person",
				implementer: "worker-person",
			},
			createdAt: 0,
		};
		const out = renderNotifyMessage(event, null, null);
		expect(out).toContain("review requested changes/BLOCK:");
		expect(out).toContain("[local path]");
		expect(out).toContain("[REDACTED]");
		expect(out).toContain("— reviewer @reviewer-person");
		expect(out).toContain("→ implementer @worker-person");
		// The default reason flows through the same clamp when payload is bare.
		const bare = renderNotifyMessage({ ...event, payload: null }, null, null);
		expect(bare).toContain("reviewer feedback requires changes");
	});

	it("changes_requested drops the @assignee tag; other kinds keep it (watcher parity)", () => {
		const task: NotifyTaskView = {
			id: "t_chg",
			title: "Review me",
			status: "review",
			assignee: "alice",
			result: null,
		};
		// Upstream composes the 🛑 line WITHOUT {tag} — the reviewer/implementer
		// provenance already names the parties.
		const event: NotifyEvent = {
			id: 2,
			taskId: "t_chg",
			kind: "changes_requested",
			payload: { reason: "s" },
			createdAt: 0,
		};
		expect(renderNotifyMessage(event, task, "default")).toMatch(
			/^🛑 \[default\] Kanban t_chg review requested changes\/BLOCK: s/,
		);
		// …while completed/blocked/etc. DO carry the identity prefix.
		expect(
			renderNotifyMessage(
				{
					id: 3,
					taskId: "t_chg",
					kind: "blocked",
					payload: null,
					createdAt: 0,
				},
				task,
				null,
			),
		).toMatch(/^⏸ @alice Kanban t_chg blocked/);
	});

	it("archived/unblocked are claimed-but-SILENT (cursor hygiene)", () => {
		expect([...SILENT_EVENT_KINDS].sort()).toEqual(["archived", "unblocked"]);
	});
});

// ── one-tick discipline ────────────────────────────────────────────────────

describe("runNotifierTick — claim/deliver/cursor discipline", () => {
	it("delivers new terminal events and ADVANCES the cursor (second tick: silence)", async () => {
		const store = new FakeNotifyStore();
		store.addTask({ id: "t1", title: "Job", status: "done" });
		store.appendEvent("t1", "claimed", 1); // non-terminal: never notified
		store.appendEvent("t1", "completed", 2, { summary: "all good" });
		store.addSub({ taskId: "t1", platform: "telegram", chatId: "42" });
		// Legacy row created before the events existed (cursor 0).
		store.resetCursor(
			{ taskId: "t1", platform: "telegram", chatId: "42", threadId: "" },
			0,
		);

		const r = recorder();
		const first = await runNotifierTick(store, {
			...TICK_OPTS_BASE,
			board: "default",
			deliver: r.deliver,
		});
		expect(first.delivered).toEqual([
			{
				taskId: "t1",
				platform: "telegram",
				chatId: "42",
				threadId: "",
				kind: "completed",
				eventId: 2,
			},
		]);
		expect(r.sent).toHaveLength(1);
		expect(r.sent[0]?.message).toContain("done — Job");

		// Replay prevention: the SAME events never deliver twice.
		const second = await runNotifierTick(store, {
			...TICK_OPTS_BASE,
			board: "default",
			deliver: r.deliver,
		});
		expect(second.delivered).toEqual([]);
		expect(r.sent).toHaveLength(1);
	});

	it("silent kinds (archived/unblocked) advance the cursor WITHOUT delivering", async () => {
		const store = new FakeNotifyStore();
		store.addTask({ id: "t1", title: "J", status: "todo" });
		store.appendEvent("t1", "completed", 1);
		store.appendEvent("t1", "unblocked", 2);
		const key: SubKey = {
			taskId: "t1",
			platform: "slack",
			chatId: "C1",
			threadId: "T9",
		};
		store.addSub(key);
		store.resetCursor(key, 0);
		const r = recorder();
		const result = await runNotifierTick(store, {
			...TICK_OPTS_BASE,
			deliver: r.deliver,
		});
		expect(result.delivered.map((d) => d.kind)).toEqual(["completed"]);
		// Cursor advanced past the silent event even though nothing was sent…
		expect(store.subs.get(subKeyOf(key))?.lastEventId).toBe(2);
		// …so it can never wedge behind an unclaimed row later.
		const again = await runNotifierTick(store, {
			...TICK_OPTS_BASE,
			deliver: r.deliver,
		});
		expect(again.delivered).toEqual([]);
	});

	it("unsubscribes ONLY when the task is archived — done survives for reopen cycles", async () => {
		const doneStore = new FakeNotifyStore();
		doneStore.addTask({ id: "tDone", title: "D", status: "done" });
		doneStore.appendEvent("tDone", "completed", 1);
		doneStore.addSub({ taskId: "tDone", platform: "telegram", chatId: "1" });
		doneStore.resetCursor(
			{ taskId: "tDone", platform: "telegram", chatId: "1", threadId: "" },
			0,
		);

		const archStore = new FakeNotifyStore();
		archStore.addTask({ id: "tArch", title: "A", status: "archived" });
		archStore.appendEvent("tArch", "completed", 1);
		archStore.appendEvent("tArch", "archived", 2);
		archStore.addSub({ taskId: "tArch", platform: "telegram", chatId: "2" });
		archStore.resetCursor(
			{ taskId: "tArch", platform: "telegram", chatId: "2", threadId: "" },
			0,
		);

		await runNotifierTick(doneStore, {
			...TICK_OPTS_BASE,
			deliver: recorder().deliver,
		});
		expect(doneStore.subs.size).toBe(1); // done ⇒ subscription RETAINED

		const result = await runNotifierTick(archStore, {
			...TICK_OPTS_BASE,
			deliver: recorder().deliver,
		});
		expect(archStore.subs.size).toBe(0); // archived ⇒ UNSUBSCRIBED
		expect(result.unsubscribedArchived).toEqual([
			{ taskId: "tArch", platform: "telegram", chatId: "2", threadId: "" },
		]);
	});

	it("delivery failure REWINDS the claim so the next tick retries the event", async () => {
		const store = new FakeNotifyStore();
		store.addTask({ id: "t1", title: "J", status: "done" });
		store.appendEvent("t1", "completed", 1);
		const key: SubKey = {
			taskId: "t1",
			platform: "telegram",
			chatId: "7",
			threadId: "",
		};
		store.addSub(key);
		store.resetCursor(key, 0);
		const r = recorder();

		r.failNext(); // first attempt fails
		const attempt1 = await runNotifierTick(store, {
			...TICK_OPTS_BASE,
			deliver: r.deliver,
		});
		expect(attempt1.delivered).toEqual([]);
		expect(attempt1.rewound).toEqual([key]);
		expect(store.subs.get(subKeyOf(key))?.lastEventId).toBe(0); // rewound

		const attempt2 = await runNotifierTick(store, {
			...TICK_OPTS_BASE,
			deliver: r.deliver,
		});
		expect(attempt2.delivered.map((d) => d.kind)).toEqual(["completed"]); // retried, not lost
		expect(r.sent).toHaveLength(1);
	});

	it(`drops the subscription after ${MAX_SEND_FAILURES} CONSECUTIVE failures`, async () => {
		const store = new FakeNotifyStore();
		store.addTask({ id: "t1", title: "J", status: "running" });
		store.appendEvent("t1", "blocked", 1, { reason: "stuck" });
		const key: SubKey = {
			taskId: "t1",
			platform: "telegram",
			chatId: "8",
			threadId: "",
		};
		store.addSub(key);
		store.resetCursor(key, 0);
		const failingDeliver = async (): Promise<void> => {
			throw new Error("dead chat");
		};
		const failCounts = new Map<string, number>();
		const opts = { ...TICK_OPTS_BASE, deliver: failingDeliver, failCounts };
		// Ticks 1..11: rewind + counter climbs.
		for (let i = 1; i < MAX_SEND_FAILURES; i++) {
			const res = await runNotifierTick(store, opts);
			expect(res.droppedAfterFailures).toEqual([]);
			expect(store.subs.size).toBe(1);
		}
		// Tick 12: budget exhausted ⇒ subscription DROPPED (no infinite spin).
		const final = await runNotifierTick(store, opts);
		expect(final.droppedAfterFailures).toEqual([key]);
		expect(store.subs.size).toBe(0);
	});

	it("one dead subscription cannot block another's delivery (per-sub isolation)", async () => {
		const store = new FakeNotifyStore();
		store.addTask({ id: "a", title: "A", status: "done" });
		store.addTask({ id: "b", title: "B", status: "done" });
		store.appendEvent("a", "completed", 1);
		store.appendEvent("b", "completed", 2);
		for (const [taskId, chat] of [
			["a", "dead"],
			["b", "alive"],
		] as const) {
			store.addSub({ taskId, platform: "telegram", chatId: chat });
			store.resetCursor(
				{ taskId, platform: "telegram", chatId: chat, threadId: "" },
				0,
			);
		}
		const sentChats: string[] = [];
		const result = await runNotifierTick(store, {
			...TICK_OPTS_BASE,
			deliver: async (sub) => {
				if (sub.chatId === "dead") throw new Error("gone");
				sentChats.push(sub.chatId);
			},
		});
		expect(sentChats).toEqual(["alive"]);
		expect(result.rewound).toHaveLength(1); // the dead one rewinds for retry
	});

	it("GC sweep purges stale done-task subs; retention 0 disables; not-due skips", async () => {
		const store = new FakeNotifyStore();
		store.addTask({ id: "old", title: "", status: "done" });
		store.addTask({ id: "fresh", title: "", status: "done" });
		store.addTask({ id: "active", title: "", status: "running" });
		store.appendEvent("old", "status", 1, undefined, TICK_NOW - 40 * 86400);
		store.appendEvent("fresh", "status", 2, undefined, TICK_NOW - 60);
		for (const t of ["old", "fresh", "active"]) {
			store.addSub({ taskId: t, platform: "telegram", chatId: t });
		}

		// Not due ⇒ nothing purged.
		const idle = await runNotifierTick(store, {
			...TICK_OPTS_BASE,
			gcDue: false,
			deliver: recorder().deliver,
		});
		expect(idle.gcPurged).toBe(0);

		// Due ⇒ only the STALE DONE sub goes (age measured from newest event).
		const swept = await runNotifierTick(store, {
			...TICK_OPTS_BASE,
			gcDue: true,
			gcRetentionDays: 30,
			deliver: recorder().deliver,
		});
		expect(swept.gcPurged).toBe(1);
		const survivors = [...store.subs.keys()].map((k) => k.split("\u0000")[0]);
		expect(survivors.sort()).toEqual(["active", "fresh"]);

		// Retention 0 DISABLES the sweep entirely (operator opt-out).
		store.addTask({ id: "ancient", title: "", status: "done" });
		store.appendEvent(
			"ancient",
			"status",
			3,
			undefined,
			TICK_NOW - 400 * 86400,
		);
		store.addSub({
			taskId: "ancient",
			platform: "telegram",
			chatId: "ancient",
		});
		const disabled = await runNotifierTick(store, {
			...TICK_OPTS_BASE,
			gcDue: true,
			gcRetentionDays: 0,
			deliver: recorder().deliver,
		});
		expect(disabled.gcPurged).toBe(0);
		expect(
			store.subs.has(
				subKeyOf({
					taskId: "ancient",
					platform: "telegram",
					chatId: "ancient",
					threadId: "",
				}),
			),
		).toBe(true);
	});

	it("a throwing listSubs surfaces as a loud tick failure (loop keeps its contract)", async () => {
		const store = new FakeNotifyStore();
		store.listSubsError = new Error("board db locked");
		await expect(
			runNotifierTick(store, {
				...TICK_OPTS_BASE,
				deliver: recorder().deliver,
			}),
		).rejects.toThrow("board db locked");
	});
});

// ── service loop ───────────────────────────────────────────────────────────

describe("startKanbanNotifier — optional-stage service contracts", () => {
	function lines(): { log: (s: string) => void; out: string[] } {
		const out: string[] = [];
		return { log: (s: string) => out.push(s), out };
	}

	it("starts, ticks on the injected cadence, and stops cleanly", async () => {
		const l = lines();
		const clock = new ManualClock(TICK_NOW);
		let ticks = 0;
		const store = new FakeNotifyStore();
		const originalList = store.listSubs.bind(store);
		store.listSubs = async () => {
			ticks += 1;
			return originalList();
		};
		const started = await startKanbanNotifier(
			{
				openStore: () => store,
				pinnedBoard: "default",
				env: {},
				config: { notify_interval_seconds: 1, notify_initial_delay_seconds: 1 },
				deliver: recorder().deliver,
				clock,
			},
			l.log,
		);
		expect(started.result.ok).toBe(true);
		const running = started.running!;
		try {
			const deadline = Date.now() + 5000;
			while (ticks < 2 && Date.now() < deadline) {
				await clock.sleepMs(50);
			}
			expect(ticks).toBeGreaterThanOrEqual(2);
		} finally {
			await running.stop();
		}
		const afterStop = ticks;
		await clock.sleepMs(100);
		expect(ticks).toBe(afterStop); // stopped cleanly
	});

	it("invalid pinned board slug ⇒ degraded loudly, hard-boundary warning", async () => {
		const l = lines();
		const { result } = await startKanbanNotifier(
			{
				openStore: () => new FakeNotifyStore(),
				pinnedBoard: "NOT VALID!!",
				env: {},
				deliver: recorder().deliver,
				clock: new ManualClock(),
			},
			l.log,
		);
		expect(result.ok).toBe(false);
		expect(result.degraded).toBe(true);
		expect(`${result.reason} ${l.out.join(" ")}`).toContain(
			"hard board boundary",
		);
	});

	it(`disabled via ${KANBAN_NOTIFY_IN_GATEWAY_ENV} is a CLEAN skip (not degraded)`, async () => {
		const l = lines();
		const { result } = await startKanbanNotifier(
			{
				openStore: () => new FakeNotifyStore(),
				env: { [KANBAN_NOTIFY_IN_GATEWAY_ENV]: "off" },
				deliver: recorder().deliver,
				clock: new ManualClock(),
			},
			l.log,
		);
		expect(result.ok).toBe(false);
		expect(result.degraded).toBe(false);
		expect(l.out.join(" ")).toContain(KANBAN_NOTIFY_IN_GATEWAY_ENV);
	});

	it("store pinned to a DIFFERENT board ⇒ refusal (hard boundary)", async () => {
		const impostor = new FakeNotifyStore("other-board");
		const l = lines();
		const { result } = await startKanbanNotifier(
			{
				openStore: () => impostor,
				pinnedBoard: "default",
				env: {},
				deliver: recorder().deliver,
				clock: new ManualClock(),
			},
			l.log,
		);
		expect(result.ok).toBe(false);
		expect(result.degraded).toBe(true);
		expect(result.reason).toContain("hard board boundary");
	});

	it("openStore failure ⇒ degraded loudly without throwing", async () => {
		const { result } = await startKanbanNotifier({
			openStore: () => {
				throw new Error("subs.db missing");
			},
			env: {},
			deliver: recorder().deliver,
			clock: new ManualClock(),
		});
		expect(result.ok).toBe(false);
		expect(result.degraded).toBe(true);
		expect(result.reason).toContain("subs.db missing");
	});

	it("ownsSingleton=false ⇒ the tick claims/delivers NOTHING (legacy rows are lock-owner-only)", async () => {
		const l = lines();
		const clock = new ManualClock(TICK_NOW);
		const store = new FakeNotifyStore();
		store.addTask({ id: "t1", title: "Job", status: "done" });
		store.appendEvent("t1", "completed", 1, { summary: "all good" });
		store.addSub({ taskId: "t1", platform: "telegram", chatId: "42" });
		store.resetCursor(
			{ taskId: "t1", platform: "telegram", chatId: "42", threadId: "" },
			0,
		);
		const r = recorder();
		let owns = false; // another gateway holds the machine-global role
		let listCalls = 0;
		const originalList = store.listSubs.bind(store);
		store.listSubs = async () => {
			listCalls += 1;
			return originalList();
		};
		const started = await startKanbanNotifier(
			{
				openStore: () => store,
				pinnedBoard: "default",
				env: {},
				config: {
					notify_interval_seconds: 1,
					notify_initial_delay_seconds: 0,
				},
				deliver: r.deliver,
				clock,
				ownsSingleton: () => owns,
			},
			l.log,
		);
		expect(started.result.ok).toBe(true);
		const running = started.running!;
		try {
			const deadline = Date.now() + 5000;
			while (listCalls < 1 && Date.now() < deadline) await clock.sleepMs(20);
			// Wait for a tick to elapse WITHOUT ownership: no rows claimed, no
			// deliveries, cursor untouched — silent skip (include_unowned parity).
			expect(r.sent).toHaveLength(0);
			expect(
				store.subs.get("t1\u0000telegram\u000042\u0000")!.lastEventId,
			).toBe(0);

			// The dispatcher role lands on THIS gateway ⇒ next tick delivers.
			owns = true;
			const deliverDeadline = Date.now() + 5000;
			while (r.sent.length < 1 && Date.now() < deliverDeadline) {
				await clock.sleepMs(20);
			}
			expect(r.sent).toHaveLength(1);
			expect(r.sent[0]?.message).toContain("done — Job");
		} finally {
			await running.stop();
		}
	});

	it("external singleton holder ⇒ clean skip (backstop probe parity)", async () => {
		const { result } = await startKanbanNotifier({
			openStore: () => new FakeNotifyStore(),
			env: {},
			deliver: recorder().deliver,
			clock: new ManualClock(),
			hasSingleton: () => false,
		});
		expect(result.ok).toBe(false);
		expect(result.degraded).toBe(false);
		expect(result.reason).toContain("notifier role");
	});

	it("resolveNotifierServiceConfig defaults + validation fallback warnings", () => {
		const cfg = resolveNotifierServiceConfig({ env: {} });
		expect(cfg).toMatchObject({
			board: "default",
			boardSource: "default",
			intervalSeconds: 5,
			initialDelaySeconds: 5,
			doneSubRetentionDays: 30,
			enabled: true,
		});
		const bad = resolveNotifierServiceConfig({
			config: {
				notify_interval_seconds: "banana",
				done_sub_retention_days: -3,
			},
		});
		expect(bad.intervalSeconds).toBe(5);
		expect(bad.doneSubRetentionDays).toBe(30); // falls back WITH warning
		expect(bad.warnings.length).toBeGreaterThanOrEqual(1);
	});
});

// ── DEC-040 stage entry ────────────────────────────────────────────────────

describe("kanbanNotifierServiceEntry (DEC-040 stage wiring)", () => {
	const clock: GatewayClock = {
		nowSeconds: () => TICK_NOW,
		sleepMs: async () => {},
	};

	it("maps a running notifier onto ok:true with a stoppable handle", async () => {
		const entry = kanbanNotifierServiceEntry({
			openStore: () => new FakeNotifyStore(),
			env: {},
			deliver: recorder().deliver,
			clock,
			logLines: () => {},
		});
		expect(entry.name).toBe(KANBAN_NOTIFIER_SERVICE_NAME);
		const outcome = await entry.start();
		expect(outcome.ok).toBe(true);
		expect(outcome.handle?.name).toBe(KANBAN_NOTIFIER_SERVICE_NAME);
		await outcome.handle?.stop?.();
	});

	it("maps a refused board onto ok:false + degraded:true", async () => {
		const outcome = await kanbanNotifierServiceEntry({
			openStore: () => new FakeNotifyStore(),
			pinnedBoard: "BAD SLUG!!",
			env: {},
			deliver: recorder().deliver,
			clock,
			logLines: () => {},
		}).start();
		expect(outcome.ok).toBe(false);
		expect(outcome.degraded).toBe(true);
		expect(outcome.handle).toBeUndefined();
	});

	it("maps a DISABLED env gate onto ok:false WITHOUT degraded", async () => {
		const outcome = await kanbanNotifierServiceEntry({
			openStore: () => new FakeNotifyStore(),
			env: { [KANBAN_NOTIFY_IN_GATEWAY_ENV]: "0" },
			deliver: recorder().deliver,
			clock,
			logLines: () => {},
		}).start();
		expect(outcome.ok).toBe(false);
		expect(outcome.degraded).toBeUndefined();
		expect(outcome.reason).toContain(KANBAN_NOTIFY_IN_GATEWAY_ENV);
	});
});

// ── SQLite store integration (production persistence shape) ────────────────

describe("SqliteKanbanNotifyStore — production persistence contracts", () => {
	it("addSub snaps the cursor to MAX(event seq): subscribing never replays history", async () => {
		const h = await openKanbanHarness("notify-snap");
		try {
			SqliteKanbanNotifyStore.ensureSchema(h.db);
			const store = new SqliteKanbanNotifyStore(h.db, { board: h.board.board });
			h.board.createCard({ id: "t1", title: "Historic", status: "done" });
			h.board.completeCard("t1", h.clock.nowSeconds()); // writes 'completed' event
			store.addSub({ taskId: "t1", platform: "telegram", chatId: "u1" });
			const r = recorder();
			const tick = await runNotifierTick(store, {
				...TICK_OPTS_BASE,
				deliver: r.deliver,
			});
			expect(tick.delivered).toEqual([]); // historical completion NOT replayed
		} finally {
			h.close();
		}
	});

	it("events written by BOARD operations flow through delivery (same event log)", async () => {
		const h = await openKanbanHarness("notify-board-events");
		try {
			SqliteKanbanNotifyStore.ensureSchema(h.db);
			const store = new SqliteKanbanNotifyStore(h.db, { board: "default" });
			h.board.createCard({ id: "t2", assignee: "alice", status: "running" });
			store.addSub({ taskId: "t2", platform: "discord", chatId: "c1" });
			// Pre-date the subscription cursor so the completion IS unseen.
			h.db.prepare("UPDATE kanban_notify_subs SET last_event_id = 0").run();
			await h.board.completeCard("t2", h.clock.nowSeconds());
			const r = recorder();
			const tick = await runNotifierTick(store, {
				...TICK_OPTS_BASE,
				deliver: r.deliver,
			});
			expect(tick.delivered.map((d) => d.kind)).toEqual(["completed"]);
			expect(tick.unsubscribedArchived).toEqual([]); // done ≠ archived: retained
			expect(r.sent[0]?.message).toContain("@alice Kanban t2 done");
		} finally {
			h.close();
		}
	});

	it("claimUnseenEvents is single-owner across RIVAL connections (CAS serialization)", async () => {
		const h = await openKanbanHarness("notify-race");
		try {
			SqliteKanbanNotifyStore.ensureSchema(h.db);
			const store = new SqliteKanbanNotifyStore(h.db, { board: "default" });
			h.board.createCard({ id: "t3", title: "Racy" });
			h.db
				.prepare(
					"INSERT INTO kanban_events (card_id, event, at) VALUES ('t3', 'completed', 100), ('t3', 'blocked', 101)",
				)
				.run();
			store.addSub({ taskId: "t3", platform: "telegram", chatId: "x" });
			h.db.prepare("UPDATE kanban_notify_subs SET last_event_id = 0").run();

			// Rival connection to the SAME file (second watcher process shape).
			const rivalDb = new Database(h.dbPath);
			rivalDb.pragma("busy_timeout = 5000");
			try {
				const rivalStore = new SqliteKanbanNotifyStore(rivalDb, {
					board: "default",
				});
				const key = {
					taskId: "t3",
					platform: "telegram",
					chatId: "x",
					threadId: "",
				};
				// Exactly ONE of the two watchers claims the range…
				const first = await store.claimUnseenEvents(key, NOTIFY_TERMINAL_KINDS);
				const second = await rivalStore.claimUnseenEvents(
					key,
					NOTIFY_TERMINAL_KINDS,
				);
				const claimedKinds = [
					...(first?.events ?? []),
					...(second?.events ?? []),
				].map((e) => e.kind);
				expect(claimedKinds.sort()).toEqual(["blocked", "completed"]);
				// …and whoever claimed SECOND sees an empty range (cursor moved).
				expect(second?.events ?? []).toEqual([]);
			} finally {
				rivalDb.close();
			}
		} finally {
			h.close();
		}
	});

	it("rewindCursor honors the CAS guard — a stale claim never clobbers newer progress", async () => {
		const h = await openKanbanHarness("notify-rewind");
		try {
			SqliteKanbanNotifyStore.ensureSchema(h.db);
			const store = new SqliteKanbanNotifyStore(h.db, { board: "default" });
			h.board.createCard({ id: "t4", title: "Rw" });
			h.db
				.prepare(
					"INSERT INTO kanban_events (card_id, event, at) VALUES ('t4', 'completed', 5)",
				)
				.run();
			store.addSub({ taskId: "t4", platform: "telegram", chatId: "y" });
			const key = {
				taskId: "t4",
				platform: "telegram",
				chatId: "y",
				threadId: "",
			};
			h.db.prepare("UPDATE kanban_notify_subs SET last_event_id = 0").run();
			const claimed = await store.claimUnseenEvents(key, NOTIFY_TERMINAL_KINDS);
			expect(claimed?.newCursor).toBeGreaterThan(0);
			// Someone else advances further…
			await store.advanceCursor(key, 99);
			// …the FAILED sender's stale rewind must NOT apply.
			const rewound = await store.rewindCursor(
				key,
				claimed!.newCursor,
				claimed!.oldCursor,
			);
			expect(rewound).toBe(false);
			const row = h.db
				.prepare(
					"SELECT last_event_id FROM kanban_notify_subs WHERE chat_id = 'y'",
				)
				.get() as { last_event_id: number };
			expect(Number(row.last_event_id)).toBe(99);
		} finally {
			h.close();
		}
	});

	it("purgeStaleDoneSubs: age from newest event, done-only, retention<=0 disabled", async () => {
		const h = await openKanbanHarness("notify-gc");
		try {
			SqliteKanbanNotifyStore.ensureSchema(h.db);
			const store = new SqliteKanbanNotifyStore(h.db, { board: "default" });
			h.board.createCard({ id: "old", status: "done", createdAt: 1_000_000 });
			h.db
				.prepare(
					"INSERT INTO kanban_events (card_id, event, at) VALUES ('old', 'completed', 1100000)",
				)
				.run();
			h.board.createCard({ id: "live", status: "running", createdAt: 1 });
			for (const [taskId, chat] of [
				["old", "o"],
				["live", "l"],
			] as const) {
				store.addSub({ taskId, platform: "telegram", chatId: chat });
			}
			// 31 days AFTER the newest event of 'old' ⇒ purgeable; 'live' stays.
			const cutoffNow = 1_100_000 + 31 * 86400;
			const purged = await store.purgeStaleDoneSubs({
				maxAgeDays: 30,
				nowSeconds: cutoffNow,
			});
			expect(purged).toBe(1);
			expect(
				await store.listSubs().then((subs) => subs.map((s) => s.taskId)),
			).toEqual(["live"]);
			// Disabled sweep deletes nothing.
			expect(
				await store.purgeStaleDoneSubs({
					maxAgeDays: 0,
					nowSeconds: cutoffNow,
				}),
			).toBe(0);
		} finally {
			h.close();
		}
	});
});
