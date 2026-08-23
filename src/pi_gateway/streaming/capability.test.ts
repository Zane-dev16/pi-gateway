// StreamingCapabilities BEHAVIOR CONTRACTS — per-chat METHOD probes with a
// PER-CHAT LATCH (DEC-006; 04 §5). Probes are counted by the test sources so
// latch behavior is observed through probe-call counts, not internals.

import { describe, expect, it } from "vitest";
import type { Metadata } from "./adapter-seam.js";
import {
	StreamingCapabilities,
	type CapabilityProbeSource,
} from "./capability.js";

function countingSource(opts?: {
	supports?: boolean | (() => boolean);
	isMessage?: boolean | Record<string, boolean> | (() => boolean);
	throwOnSupports?: boolean;
	throwOnIsMessage?: boolean;
	withProbes?: boolean;
	draftStreamIsMessage?: boolean | undefined;
}): {
	source: CapabilityProbeSource;
	supportsCalls: () => number;
	isMessageCalls: (chatId: string) => number;
} {
	let supportsCount = 0;
	const isMessageCounts = new Map<string, number>();
	const withProbes = opts?.withProbes ?? true;
	const source: CapabilityProbeSource = {
		draftStreamIsMessage: opts?.draftStreamIsMessage,
	};
	if (withProbes) {
		source.supportsDraftStreaming = (
			_chatType?: string,
			_metadata?: Metadata,
			_chatId?: string | number,
		): boolean => {
			supportsCount += 1;
			if (opts?.throwOnSupports) throw new Error("probe exploded");
			const s = opts?.supports;
			return typeof s === "function" ? s() : (s ?? true);
		};
		source.streamIsMessageForChat = (chatId: string): boolean => {
			isMessageCounts.set(chatId, (isMessageCounts.get(chatId) ?? 0) + 1);
			if (opts?.throwOnIsMessage) throw new Error("probe exploded");
			const im = opts?.isMessage;
			if (typeof im === "function") return im();
			if (typeof im === "object" && im !== null) return im[chatId] ?? false;
			return im ?? true;
		};
	}
	return {
		source,
		supportsCalls: () => supportsCount,
		isMessageCalls: (chatId: string) => isMessageCounts.get(chatId) ?? 0,
	};
}

describe("StreamingCapabilities — probe latch", () => {
	it("stream-is-message probe is probed ONCE per chat and LATCHED across calls", () => {
		const flip = { v: true };
		const h = countingSource({ isMessage: () => flip.v });
		const caps = new StreamingCapabilities(h.source);

		expect(caps.streamIsMessage("chat-a")).toBe(true);
		flip.v = false; // platform answer flaps AFTER first resolution
		expect(caps.streamIsMessage("chat-a")).toBe(true); // latched TRUE
		expect(h.isMessageCalls("chat-a")).toBe(1);
	});

	it("latch is PER CHAT: different chats probe independently", () => {
		const h = countingSource({
			isMessage: { "chat-a": true, "chat-b": false },
		});
		const caps = new StreamingCapabilities(h.source);
		expect(caps.streamIsMessage("chat-a")).toBe(true);
		expect(caps.streamIsMessage("chat-b")).toBe(false);
		expect(caps.streamIsMessage("chat-a")).toBe(true);
		expect(h.isMessageCalls("chat-a")).toBe(1);
		expect(h.isMessageCalls("chat-b")).toBe(1);
	});

	it("a RAISING stream-is-message probe latches UNSUPPORTED and does not re-probe", () => {
		const h = countingSource({ throwOnIsMessage: true });
		const caps = new StreamingCapabilities(h.source);
		expect(caps.streamIsMessage("chat-x")).toBe(false);
		expect(caps.streamIsMessage("chat-x")).toBe(false);
		expect(h.isMessageCalls("chat-x")).toBe(1);
	});

	it("per-chat METHOD probe is PREFERRED over the class attribute (review r2 finding 2)", () => {
		const h = countingSource({
			isMessage: false,
			draftStreamIsMessage: undefined,
		});
		h.source.draftStreamIsMessage = true; // class flag says yes…
		const caps = new StreamingCapabilities(h.source);
		expect(caps.streamIsMessage("chat-a")).toBe(false); // …probe says NO and wins
	});

	it("class-level draft_stream_is_message is the FALLBACK when no method probe exists", () => {
		const noProbeTrue = countingSource({ withProbes: false });
		noProbeTrue.source.draftStreamIsMessage = true;
		expect(
			new StreamingCapabilities(noProbeTrue.source).streamIsMessage("c"),
		).toBe(true);

		const noProbeFalse = countingSource({ withProbes: false });
		noProbeFalse.source.draftStreamIsMessage = false;
		expect(
			new StreamingCapabilities(noProbeFalse.source).streamIsMessage("c"),
		).toBe(false);

		// `is True` discipline: truthy-but-not-true class values resolve FALSE
		// (MagicMock-safe; base.py::_stream_is_message).
		const noProbeTruthy = countingSource({ withProbes: false });
		noProbeTruthy.source.draftStreamIsMessage = 1 as unknown as boolean;
		expect(
			new StreamingCapabilities(noProbeTruthy.source).streamIsMessage("c"),
		).toBe(false);

		const noProbeAbsent = countingSource({ withProbes: false });
		expect(
			new StreamingCapabilities(noProbeAbsent.source).streamIsMessage("c"),
		).toBe(false);
	});

	it("supports_draft_streaming latches PER chat type; raising probe latches false", () => {
		const h = countingSource();
		const caps = new StreamingCapabilities(h.source);
		expect(caps.supportsDraftStreaming("dm")).toBe(true);
		expect(caps.supportsDraftStreaming("dm")).toBe(true);
		expect(caps.supportsDraftStreaming("group")).toBe(true);
		expect(h.supportsCalls()).toBe(2); // one per chatType key

		const boom = countingSource({ throwOnSupports: true });
		const caps2 = new StreamingCapabilities(boom.source);
		expect(caps2.supportsDraftStreaming("dm")).toBe(false);
		expect(caps2.supportsDraftStreaming("dm")).toBe(false);
		expect(boom.supportsCalls()).toBe(1);
	});

	it("adapters without ANY probes resolve unsupported (base.py default False)", () => {
		const h = countingSource({ withProbes: false });
		const caps = new StreamingCapabilities(h.source);
		expect(caps.supportsDraftStreaming("dm")).toBe(false);
	});
});
