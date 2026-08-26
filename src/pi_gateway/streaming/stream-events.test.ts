// Typed stream-event vocabulary + dispatcher CONTRACTS (04 §5.3 clauses,
// copied verbatim): rendering lives adapter-side; presentation never breaks
// the turn; "new"-mode dedup is dispatcher-state.

import { describe, expect, it } from "vitest";
import {
	defaultFormatToolEvent,
	defaultRenderMessageEvent,
	type ConsumerSink,
	type StreamRenderAdapter,
} from "./adapter-seam.js";
import { GatewayEventDispatcher } from "./dispatcher.js";
import {
	commentary,
	gatewayNotice,
	longToolHint,
	messageChunk,
	messageStop,
	toolCallChunk,
	toolCallFinished,
} from "./stream-events.js";

function recordingSink(): ConsumerSink & {
	calls: Array<{ fn: string; arg: string }>;
} {
	const calls: Array<{ fn: string; arg: string }> = [];
	return {
		calls,
		onDelta(text) {
			calls.push({ fn: "onDelta", arg: text });
		},
		onSegmentBreak() {
			calls.push({ fn: "onSegmentBreak", arg: "" });
		},
		onCommentary(text) {
			calls.push({ fn: "onCommentary", arg: text });
		},
	};
}

describe("typed stream-event vocabulary (§5.3)", () => {
	it("frozen factories produce immutable events naming WHAT happened", () => {
		const chunk = messageChunk("hi");
		expect(Object.isFrozen(chunk)).toBe(true);
		expect(chunk).toEqual({ type: "message_chunk", text: "hi" });
		expect(messageStop(true)).toEqual({ type: "message_stop", final: true });
		expect(toolCallChunk("bash", { preview: "ls", index: 2 })).toEqual({
			type: "tool_call_chunk",
			toolName: "bash",
			preview: "ls",
			args: undefined,
			index: 2,
		});
		expect(
			toolCallFinished("bash", { durationSeconds: 1.5, ok: false, index: 2 }),
		).toEqual({
			type: "tool_call_finished",
			toolName: "bash",
			durationSeconds: 1.5,
			ok: false,
			index: 2,
		});
		const notice = gatewayNotice("restart", "brb", { a: 1 });
		expect(notice.notice).toBe("restart");
		expect(() => {
			(notice as { notice: string }).notice = "x";
		}).toThrow();
	});
});

describe("GatewayEventDispatcher routing (base defaults = legacy behavior)", () => {
	it("MessageChunk → sink.onDelta; EMPTY chunk dropped", () => {
		const sink = recordingSink();
		new GatewayEventDispatcher({ sink }).dispatch(messageChunk("abc"));
		new GatewayEventDispatcher({ sink }).dispatch(messageChunk(""));
		expect(sink.calls).toEqual([{ fn: "onDelta", arg: "abc" }]);
	});

	it("MessageStop(final:false) → segment break; MessageStop(final:true) → NOTHING at the sink", () => {
		const sink = recordingSink();
		const d = new GatewayEventDispatcher({ sink });
		d.dispatch(messageStop(false));
		d.dispatch(messageStop(true)); // terminal stop signalled via finish(), never here
		expect(sink.calls).toEqual([{ fn: "onSegmentBreak", arg: "" }]);
	});

	it("Commentary → its own beat via onCommentary (FIFO ingress)", () => {
		const sink = recordingSink();
		new GatewayEventDispatcher({ sink }).dispatch(
			commentary("inspecting first"),
		);
		expect(sink.calls).toEqual([
			{ fn: "onCommentary", arg: "inspecting first" },
		]);
	});

	it("null sink ⇒ message events dropped, final still flows via normal send path", () => {
		const d = new GatewayEventDispatcher({
			sink: null,
			enqueueToolLine: () => {
				throw new Error("must not be called");
			},
		});
		expect(() => d.dispatch(messageChunk("x"))).not.toThrow();
		expect(() => d.dispatch(messageStop(false))).not.toThrow();
	});

	it("adapter render hook OVERRIDES the default and receives event+sink", () => {
		const sink = recordingSink();
		const seen: string[] = [];
		const render: StreamRenderAdapter = {
			renderMessageEvent(event, s) {
				seen.push(event.type);
				if (event.type === "message_chunk") s.onCommentary(`EAT:${event.text}`);
			},
		};
		new GatewayEventDispatcher({ sink, render }).dispatch(messageChunk("raw"));
		expect(seen).toEqual(["message_chunk"]);
		expect(sink.calls).toEqual([{ fn: "onCommentary", arg: "EAT:raw" }]);
	});

	it("defaultRenderMessageEvent maps all three message kinds onto sink primitives", () => {
		const sink = recordingSink();
		defaultRenderMessageEvent(messageChunk("t"), sink);
		defaultRenderMessageEvent(messageStop(false), sink);
		defaultRenderMessageEvent(commentary("c"), sink);
		defaultRenderMessageEvent(messageStop(true), sink); // ignored by contract
		expect(sink.calls.map((c) => c.fn)).toEqual([
			"onDelta",
			"onSegmentBreak",
			"onCommentary",
		]);
	});
});

describe("tool-progress routing (per-channel mode + preview cap)", () => {
	it("mode 'all': every formatted line enqueued; adapter returning null EATS the event", () => {
		const lines: string[] = [];
		const eat: StreamRenderAdapter = { formatToolEvent: () => null };
		const dEat = new GatewayEventDispatcher({
			sink: null,
			render: eat,
			enqueueToolLine: (l) => lines.push(l),
		});
		dEat.dispatch(toolCallChunk("read", { preview: "/etc/passwd" }));
		expect(lines).toEqual([]);

		const dDefault = new GatewayEventDispatcher({
			sink: null,
			enqueueToolLine: (l) => lines.push(l),
		});
		dDefault.dispatch(toolCallChunk("read", { preview: "/etc/passwd" }));
		expect(lines).toHaveLength(1);
		expect(lines[0]).toContain("read");
		expect(lines[0]).toContain("/etc/passwd");
	});

	it("mode 'new': dedup emits ONLY on tool CHANGE — dispatcher-state, not consumer-state", () => {
		const lines: string[] = [];
		const d = new GatewayEventDispatcher({
			sink: null,
			toolMode: "new",
			enqueueToolLine: (l) => lines.push(l),
		});
		d.dispatch(toolCallChunk("bash", { index: 0 }));
		d.dispatch(toolCallChunk("bash", { preview: "changed args", index: 1 })); // same tool → eaten
		d.dispatch(toolCallChunk("read", { index: 2 })); // changed → emitted
		d.dispatch(toolCallChunk("read", { index: 3 })); // same again → eaten
		expect(lines).toHaveLength(2);
	});

	it("mode 'off' and missing queue ⇒ nothing enqueued", () => {
		let called = false;
		const d = new GatewayEventDispatcher({
			sink: null,
			toolMode: "off",
			enqueueToolLine: () => {
				called = true;
			},
		});
		d.dispatch(toolCallChunk("bash"));
		expect(called).toBe(false);

		const d2 = new GatewayEventDispatcher({
			sink: null,
			enqueueToolLine: null,
		});
		d2.dispatch(toolCallChunk("bash"));
		expect(called).toBe(false);
	});

	it("verbose mode renders full args JSON, truncated with '...' under a positive cap; 0 = uncapped", () => {
		const longArgs = { path: "/x".repeat(80) };
		const capped = defaultFormatToolEvent(
			toolCallChunk("read", { args: longArgs }),
			{
				mode: "verbose",
				previewMaxLen: 30,
			},
		);
		expect(capped?.length).toBeLessThanOrEqual(60);
		expect(capped?.endsWith("...")).toBe(true);

		const uncapped = defaultFormatToolEvent(
			toolCallChunk("read", { args: { a: 1 } }),
			{
				mode: "verbose",
				previewMaxLen: 0,
			},
		);
		expect(uncapped).toContain('"a":1');
	});

	it("ToolCallFinished drives NO completion chrome by default (hints arrive as events)", () => {
		let enqueued = false;
		new GatewayEventDispatcher({
			sink: null,
			enqueueToolLine: () => {
				enqueued = true;
			},
		}).dispatch(toolCallFinished("bash", { durationSeconds: 9 }));
		expect(enqueued).toBe(false);
	});

	it("LongToolHint / GatewayNotice route to gateway-owned hooks", () => {
		const seen: string[] = [];
		const d = new GatewayEventDispatcher({
			sink: null,
			onLongTool: (e) => seen.push(`long:${e.toolName}`),
			onNotice: (e) => seen.push(`notice:${e.notice}`),
		});
		d.dispatch(longToolHint("grep", 42));
		d.dispatch(gatewayNotice("online"));
		expect(seen).toEqual(["long:grep", "notice:online"]);
	});
});

describe("presentation NEVER breaks the turn", () => {
	it("dispatch swallows sink/render/queue/hook exceptions (sync, worker-thread safe)", () => {
		const boomSink: ConsumerSink = {
			onDelta: () => {
				throw new Error("sink exploded");
			},
			onSegmentBreak: () => {
				throw new Error("break exploded");
			},
			onCommentary: () => {
				throw new Error("commentary exploded");
			},
		};
		const d = new GatewayEventDispatcher({
			sink: boomSink,
			enqueueToolLine: () => {
				throw new Error("queue exploded");
			},
			onLongTool: () => {
				throw new Error("hint exploded");
			},
			onNotice: () => {
				throw new Error("notice exploded");
			},
		});
		expect(() => d.dispatch(messageChunk("x"))).not.toThrow();
		expect(() => d.dispatch(messageStop(false))).not.toThrow();
		expect(() => d.dispatch(commentary("c"))).not.toThrow();
		expect(() => d.dispatch(toolCallChunk("bash"))).not.toThrow();
		expect(() => d.dispatch(longToolHint("bash", 1))).not.toThrow();
		expect(() => d.dispatch(gatewayNotice("online"))).not.toThrow();
	});
});
