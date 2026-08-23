// pi_gateway/streaming/capability — per-chat capability discovery (04 §5, DEC-006).
//
// Capability discovery is PER-CHAT METHOD PROBES, never class data. This module
// LATCHES probe results per chat so a flapping platform or a renegotiated
// descriptor cannot flip transport mid-turn, and so repeated consumer runs over
// the same adapter do not re-probe. A raising probe latches UNSUPPORTED
// (stream_consumer.py:_resolve_draft_streaming logs and treats it as false).
//
// Hermes anchors (READ-ONLY reference; semantics ported, no code vendored):
//   - gateway/platforms/base.py:supports_draft_streaming      (METHOD probe)
//   - gateway/stream_consumer.py:GatewayStreamConsumer._stream_is_message
//       (per-chat probe preferred; class attribute `draft_stream_is_message`
//        as fallback; both resolved with `is True` MagicMock-safe discipline)
//   - gateway/stream_consumer.py:_resolve_draft_streaming (probe raise ⇒ false)

import type { Metadata, StreamLogger } from "./adapter-seam.js";

/** Structural source of the two per-chat probes + class fallback flag. */
export interface CapabilityProbeSource {
	supportsDraftStreaming?(
		chatType?: string | undefined,
		metadata?: Metadata | undefined,
		chatId?: string | number | undefined,
	): boolean;
	streamIsMessageForChat?(chatId: string): boolean;
	draftStreamIsMessage?: boolean | undefined;
}

export class StreamingCapabilities {
	private readonly draftSupport = new Map<string, boolean>();
	private readonly streamIsMsg = new Map<string, boolean>();

	constructor(
		private readonly source: CapabilityProbeSource,
		private readonly log?: StreamLogger | undefined,
	) {}

	/**
	 * Whether native draft streaming is supported for THIS chat type. Probed
	 * ONCE per chatType key and latched; a raising probe latches false.
	 */
	supportsDraftStreaming(
		chatType?: string | undefined,
		metadata?: Metadata | undefined,
		chatId?: string | number | undefined,
	): boolean {
		const key = chatType ?? "";
		const latched = this.draftSupport.get(key);
		if (latched !== undefined) return latched;
		let result = false;
		const probe = this.source.supportsDraftStreaming;
		if (typeof probe === "function") {
			try {
				result = probe.call(this.source, chatType, metadata, chatId) === true;
			} catch (err) {
				result = false;
				this.log?.debug("supports_draft_streaming probe raised", {
					error: String(err),
					chatType,
				});
			}
		}
		this.draftSupport.set(key, result);
		return result;
	}

	/**
	 * Whether THIS chat's transport treats the stream as the message. Per-chat
	 * METHOD probe preferred (one relay adapter fronts N platforms — review r2
	 * finding 2); class-level `draftStreamIsMessage` fallback only when the
	 * adapter ships no probe. Result latched per chat id.
	 */
	streamIsMessage(chatId: string): boolean {
		const key = String(chatId);
		const latched = this.streamIsMsg.get(key);
		if (latched !== undefined) return latched;
		let result = false;
		const probe = this.source.streamIsMessageForChat;
		if (typeof probe === "function") {
			try {
				result = probe.call(this.source, key) === true;
			} catch (err) {
				result = false;
				this.log?.debug("stream_is_message_for_chat probe raised", {
					error: String(err),
					chatId: key,
				});
			}
		} else {
			result = this.source.draftStreamIsMessage === true;
		}
		this.streamIsMsg.set(key, result);
		return result;
	}
}
