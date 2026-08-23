// DEC-005 L2 policy machinery (03 §2.2, §5) + runner FIFO overflow with the
// cap-32 drop-newest contract (#28503/#17758 family). Registry-derived only —
// a hand-written command list is a spec violation.

import { describe, expect, it } from "vitest";
import type { IncomingEvent } from "./events.js";
import {
	BUSY_POLICIES,
	DEFAULT_BUSY_POLICY,
	type CommandDef,
	buildCommandLookup,
	bypassCommandNames,
	catchAllBusyRejectText,
	effectiveBusyPolicy,
	isInterruptThenDispatch,
	resolveBusyDispatch,
	resolveCommand,
	shouldBypassActiveSession,
} from "./busy-policy.js";
import { getCommand } from "./events.js";
import {
	BUSY_QUEUE_MAX_PENDING,
	RunnerBusyGuard,
	type RunnerBusyOptions,
} from "./runner-busy.js";

const REGISTRY: CommandDef[] = [
	{
		name: "new",
		aliases: ["reset"],
		busyPolicy: "interrupt_then_dispatch",
		busyHandler: "new",
	},
	{ name: "stop", busyPolicy: "interrupt_then_dispatch", busyHandler: "stop" },
	{ name: "approve", busyPolicy: "dispatch" },
	{ name: "deny", busyPolicy: "dispatch" },
	{ name: "status", busyPolicy: "dispatch" },
	{ name: "model", busyPolicy: "reject", busyHandler: "model" },
	{ name: "queue", busyPolicy: "dispatch", busyHandler: "queue" },
];

const textEvent = (text: string): IncomingEvent => ({
	messageType: "text",
	text,
});

describe("busy_policy enum and registry resolution (DEC-005)", () => {
	it("enum is EXACTLY {dispatch, reject, interrupt_then_dispatch}; default is reject", () => {
		expect([...BUSY_POLICIES]).toEqual([
			"dispatch",
			"reject",
			"interrupt_then_dispatch",
		]);
		expect(DEFAULT_BUSY_POLICY).toBe("reject");
		const row: CommandDef = { name: "bare" };
		expect(effectiveBusyPolicy(row)).toBe("reject"); // absent field ⇒ default
	});

	it("resolve_command matches names AND aliases, case/slash-insensitive; 'reset'→'new'", () => {
		const lookup = buildCommandLookup(REGISTRY);
		expect(resolveCommand(lookup, "new")?.name).toBe("new");
		expect(resolveCommand(lookup, "reset")?.name).toBe("new"); // alias
		expect(resolveCommand(lookup, "/NEW")?.name).toBe("new");
		// @mention stripping lives in the EVENT layer (getCommand), exactly as in
		// Hermes where MessageEvent.get_command() normalizes before resolution:
		expect(
			getCommand({ messageType: "text", text: "/stop@botname args" }),
		).toBe("stop");
		expect(resolveCommand(lookup, "nosuchcmd")).toBeNull();
	});

	it("should_bypass covers ANY resolvable command — including reject-policy /model (#5057); unknown /foo does NOT bypass", () => {
		const lookup = buildCommandLookup(REGISTRY);
		expect(shouldBypassActiveSession(lookup, "approve")).toBe(true);
		expect(shouldBypassActiveSession(lookup, "model")).toBe(true);
		expect(shouldBypassActiveSession(lookup, "reset")).toBe(true);
		expect(shouldBypassActiveSession(lookup, "foo")).toBe(false);
		expect(shouldBypassActiveSession(lookup, null)).toBe(false);
	});

	it("interrupt_then_dispatch set derives from registry rows (Lane A membership)", () => {
		const lookup = buildCommandLookup(REGISTRY);
		expect(isInterruptThenDispatch(lookup, "new")).toBe(true);
		expect(isInterruptThenDispatch(lookup, "reset")).toBe(true);
		expect(isInterruptThenDispatch(lookup, "stop")).toBe(true);
		expect(isInterruptThenDispatch(lookup, "approve")).toBe(false);
		expect(isInterruptThenDispatch(lookup, "model")).toBe(false);

		const names = bypassCommandNames(REGISTRY);
		expect(names.has("new")).toBe(true);
		expect(names.has("approve")).toBe(true);
		expect(names.has("model")).toBe(false); // reject rows never in bypass set
	});

	it("resolution order: pre-gate → special handler → policy dispatch → catch-all reject", () => {
		const lookup = buildCommandLookup(REGISTRY);
		expect(resolveBusyDispatch(lookup, "status")?.kind).toBe("pregate");
		expect(resolveBusyDispatch(lookup, "queue")?.kind).toBe("special");
		expect(resolveBusyDispatch(lookup, "approve")?.kind).toBe("plain");
		const model = resolveBusyDispatch(lookup, "model");
		expect(model?.kind).toBe("reject");
		expect(model?.rejectText).toBe(
			"Agent is running — wait or /stop first, then switch models.",
		);

		// Catch-all text is BYTE-STABLE:
		const bare: CommandDef[] = [{ name: "frobnicate" }];
		const resolved = resolveBusyDispatch(
			buildCommandLookup(bare),
			"frobnicate",
		);
		expect(resolved?.rejectText).toBe(catchAllBusyRejectText("frobnicate"));
		expect(resolved?.rejectText).toBe(
			"⏳ Agent is running — `/frobnicate` can't run mid-turn. Wait for the current response or `/stop` first.",
		);
	});
});

function makeRunner(extra: Partial<RunnerBusyOptions> = {}) {
	let slots = new Map<string, IncomingEvent>();
	const warnings: string[] = [];
	const runner = new RunnerBusyGuard({
		registry: REGISTRY,
		slots,
		onWarning: (m) => warnings.push(m),
		...extra,
	});
	return {
		runner,
		warnings,
		set slots(next: Map<string, IncomingEvent>) {
			slots = next;
		},
		get slots() {
			return slots;
		},
	};
}

describe("runner-side FIFO overflow (§3.1, #28503)", () => {
	it("first item takes the adapter slot; further items append to the overflow tail", () => {
		const f = makeRunner();
		expect(f.runner.enqueueFifo(KEY, textEvent("first"))).toBe("slot");
		expect(f.runner.enqueueFifo(KEY, textEvent("second"))).toBe("overflow");
		expect(f.runner.enqueueFifo(KEY, textEvent("third"))).toBe("overflow");
		expect(f.slots.get(KEY)?.text).toBe("first");
		expect(f.runner.overflowOf(KEY).map((e) => e.text)).toEqual([
			"second",
			"third",
		]);
		expect(f.runner.queueDepth(KEY)).toBe(3); // slot + overflow
	});

	it("cap 32: depth slot+overflow accepted at 32, the 33rd is DROPPED with a warning; oldest still served in order", () => {
		const f = makeRunner();
		for (let i = 0; i < BUSY_QUEUE_MAX_PENDING; i++) {
			expect(f.runner.enqueueFifo(KEY, textEvent(`m${i}`))).not.toBe("dropped");
		}
		expect(f.runner.queueDepth(KEY)).toBe(BUSY_QUEUE_MAX_PENDING);

		expect(f.runner.enqueueFifo(KEY, textEvent("overflow-33"))).toBe("dropped");
		expect(f.warnings.length).toBe(1);
		expect(f.warnings[0]).toContain("cap (32)");
		expect(f.runner.queueDepth(KEY)).toBe(32); // newest dropped, not oldest evicted

		// Drain order stays FIFO: slot head then overflow in arrival order.
		const drainedOrder: string[] = [];
		for (;;) {
			let pending: IncomingEvent | null = f.slots.get(KEY) ?? null;
			f.slots.delete(KEY);
			pending = f.runner.promoteQueuedEvent(KEY, pending);
			if (!pending) break;
			drainedOrder.push(pending.text ?? "");
		}
		expect(drainedOrder.length).toBe(32);
		expect(drainedOrder[0]).toBe("m0");
		expect(drainedOrder[31]).toBe("m31");
		expect(drainedOrder).not.toContain("overflow-33");
	});

	it("queue-mode texts each get their OWN turn — no merging across follow-ups (#28503)", () => {
		const f = makeRunner();
		f.runner.queueOrReplacePendingEvent(KEY, textEvent("alpha"));
		f.runner.queueOrReplacePendingEvent(KEY, textEvent("beta"));
		f.runner.queueOrReplacePendingEvent(KEY, textEvent("gamma"));
		// Distinct event objects preserved in arrival order:
		expect(f.slots.get(KEY)?.text).toBe("alpha");
		expect(f.runner.overflowOf(KEY).map((e) => e.text)).toEqual([
			"beta",
			"gamma",
		]);
	});

	it("photo/media events STILL merge into the head slot when security context matches", () => {
		const f = makeRunner();
		f.slots.set(KEY, {
			messageType: "photo",
			mediaUrls: ["/a.png"],
			metadata: { gateway_session_key: KEY },
		});
		f.runner.queueOrReplacePendingEvent(KEY, {
			messageType: "photo",
			mediaUrls: ["/b.png"],
			metadata: { gateway_session_key: KEY },
		});
		// Merged into the head slot — album semantics preserved:
		expect(f.slots.get(KEY)?.mediaUrls).toEqual(["/a.png", "/b.png"]);
		expect(f.runner.overflowOf(KEY).length).toBe(0);

		// Different security context ⇒ NO merge; FIFO append instead.
		f.runner.queueOrReplacePendingEvent(KEY, {
			messageType: "text",
			text: "untrusted lane",
			internal: true,
		});
		expect(f.runner.overflowOf(KEY).length).toBe(1);
	});

	it("promotion: emptied slot receives overflow head; occupied slot stages behind it", () => {
		const f = makeRunner();
		f.runner.enqueueFifo(KEY, textEvent("head"));
		f.runner.enqueueFifo(KEY, textEvent("tail-1"));

		// Drain consumed the slot: promotion RETURNS the overflow head.
		f.slots.delete(KEY);
		const next = f.runner.promoteQueuedEvent(KEY, null);
		expect(next?.text).toBe("tail-1");

		// Slot re-populated by an interrupt follow-up while overflow remains:
		f.slots.set(KEY, textEvent("urgent")); // occupies the slot…
		expect(f.runner.enqueueFifo(KEY, textEvent("tail-2"))).toBe("overflow");
		const staged = f.runner.promoteQueuedEvent(KEY, f.slots.get(KEY) ?? null);
		expect(staged?.text).toBe("urgent"); // current drain keeps ITS event…
		expect(f.slots.get(KEY)?.text).toBe("tail-2"); // …overflow head staged for NEXT drain

		// Empty overflow leaves pending untouched.
		expect(f.runner.promoteQueuedEvent(KEY, textEvent("keep"))?.text).toBe(
			"keep",
		);
	});

	it("/new clears slot AND overflow so stale text never replays (#2170)", async () => {
		const f = makeRunner();
		f.runner.enqueueFifo(KEY, textEvent("stale-a"));
		f.runner.enqueueFifo(KEY, textEvent("stale-b"));
		f.runner.clearQueue(KEY);
		expect(f.runner.queueDepth(KEY)).toBe(0);
		expect(f.slots.has(KEY)).toBe(false);
	});

	it("L2 dispatch executes specials, plain handlers, and byte-stable rejects through one entry point", async () => {
		const calls: string[] = [];
		const f = makeRunner({
			specialHandlers: {
				new: () => "fresh session started",
				queue: (ev) => {
					calls.push(`fifo:${ev.text}`);
					f.runner.enqueueFifo(KEY, ev);
					return `queued ${String(ev.text)}`;
				},
			},
			plainHandlers: {
				status: () => "all agents idle",
				approve: () => "approved",
			},
		});

		expect(
			await f.runner.dispatchBusySlashCommand("new", textEvent("/new"), KEY),
		).toBe("fresh session started");
		expect(
			await f.runner.dispatchBusySlashCommand(
				"reset",
				textEvent("/reset"),
				KEY,
			),
		).toBe("fresh session started"); // alias resolves to same special
		expect(
			await f.runner.dispatchBusySlashCommand(
				"status",
				textEvent("/status"),
				KEY,
			),
		).toBe("all agents idle");
		expect(
			await f.runner.dispatchBusySlashCommand(
				"model",
				textEvent("/model x"),
				KEY,
			),
		).toContain("switch models");
		expect(
			await f.runner.dispatchBusySlashCommand(
				"unknowncmd",
				textEvent("/unknowncmd"),
				KEY,
			),
		).toBeNull();

		// /queue special enqueues its OWN turn — messages are NOT merged:
		expect(
			await f.runner.dispatchBusySlashCommand(
				"queue",
				textEvent("/queue hello"),
				KEY,
			),
		).toBe("queued /queue hello");
		expect(f.runner.queueDepth(KEY)).toBe(1);
	});
});

const KEY = "agent:main:telegram:dm:100";
