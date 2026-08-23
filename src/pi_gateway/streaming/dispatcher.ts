// pi_gateway/streaming/dispatcher — adapter-driven routing of typed stream
// events onto a delivery sink (04-platform-adapters.md §5.3).
//
// Port of the READ-ONLY Hermes reference, semantics only, cited as file:symbol:
//   - gateway/stream_dispatch.py:GatewayEventDispatcher.dispatch / _dispatch
//   - gateway/platforms/base.py:render_message_event / format_tool_event (defaults)
//
// Contract clauses copied verbatim from 04 §5.3:
//   * Presentation NEVER breaks the turn: dispatch() catches+logs render-hook
//     exceptions; the dispatcher is synchronous, callable from the agent's
//     worker thread, no asyncio.
//   * Rendering decisions live ADAPTER-side: base defaults reproduce legacy
//     behavior; adapters override; a hook returning null EATS a tool event.
//   * Tool-progress mode ("all"/"new"/"verbose"/"off") and the preview cap are
//     resolved PER CHANNEL into the dispatcher; "new"-mode dedup (emit only
//     when the tool CHANGES) is dispatcher-state, not consumer-state.

import {
	defaultFormatToolEvent,
	defaultRenderMessageEvent,
	type ConsumerSink,
	type StreamLogger,
	type StreamRenderAdapter,
	type ToolProgressMode,
} from "./adapter-seam.js";
import type {
	GatewayNotice,
	LongToolHint,
	StreamEvent,
	ToolCallChunk,
	ToolCallFinished,
} from "./stream-events.js";

export interface DispatcherOptions {
	/**
	 * The delivery sink (the GatewayStreamConsumer). May be null when streaming
	 * is disabled — message events are dropped; the final still goes out via
	 * the normal send path.
	 */
	sink: ConsumerSink | null;
	render?: StreamRenderAdapter | undefined;
	/** Places a rendered tool-progress line on the gateway's progress queue. */
	enqueueToolLine?: ((line: string) => void) | null | undefined;
	toolMode?: ToolProgressMode | undefined;
	/** Resolved tool_preview_length (0 = no cap in verbose mode). */
	previewMaxLen?: number | undefined;
	onLongTool?: ((event: LongToolHint) => void) | undefined;
	onNotice?: ((event: GatewayNotice) => void) | undefined;
	log?: StreamLogger | undefined;
}

export class GatewayEventDispatcher {
	private readonly sink: ConsumerSink | null;
	private readonly render: StreamRenderAdapter;
	private readonly enqueueToolLine: ((line: string) => void) | null;
	private readonly toolMode: ToolProgressMode;
	private readonly previewMaxLen: number;
	private readonly onLongTool?: ((event: LongToolHint) => void) | undefined;
	private readonly onNotice?: ((event: GatewayNotice) => void) | undefined;
	private readonly log: StreamLogger | undefined;
	// "new" mode dedup — only report when the tool changes (dispatcher-state).
	private lastTool: string | null = null;

	constructor(options: DispatcherOptions) {
		this.sink = options.sink;
		this.render = options.render ?? {};
		this.enqueueToolLine = options.enqueueToolLine ?? null;
		this.toolMode = options.toolMode ?? "all";
		this.previewMaxLen = options.previewMaxLen ?? 40;
		this.onLongTool = options.onLongTool;
		this.onNotice = options.onNotice;
		this.log = options.log;
	}

	/** Route a single event. Never raises into the agent's worker thread. */
	dispatch(event: StreamEvent): void {
		try {
			this.route(event);
		} catch (err) {
			// presentation must never break the agent loop
			this.log?.debug("stream-event dispatch error", { error: String(err) });
		}
	}

	private route(
		event:
			| StreamEvent
			| ToolCallChunk
			| ToolCallFinished
			| LongToolHint
			| GatewayNotice,
	): void {
		switch (event.type) {
			case "message_chunk":
			case "message_stop":
			case "commentary": {
				if (this.sink !== null) {
					const hook =
						this.render.renderMessageEvent ?? defaultRenderMessageEvent;
					hook.call(this.render, event, this.sink);
				}
				return;
			}
			case "tool_call_chunk": {
				if (this.toolMode === "off" || this.enqueueToolLine === null) return;
				// "new" mode: only emit when the tool changes.
				if (this.toolMode === "new" && event.toolName === this.lastTool) {
					return;
				}
				this.lastTool = event.toolName;
				const format = this.render.formatToolEvent ?? defaultFormatToolEvent;
				const line = format.call(this.render, event, {
					mode: this.toolMode,
					previewMaxLen: this.previewMaxLen,
				});
				// null == adapter chose to eat this event (can't render chrome).
				if (line) this.enqueueToolLine(line);
				return;
			}
			case "tool_call_finished": {
				// Default: NO completion chrome (matches legacy). Completion drives
				// onboarding hints only — which arrive as LongToolHint events.
				return;
			}
			case "long_tool_hint": {
				this.onLongTool?.(event);
				return;
			}
			case "gateway_notice": {
				this.onNotice?.(event);
				return;
			}
		}
	}
}
