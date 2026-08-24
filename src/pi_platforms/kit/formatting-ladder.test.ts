// Formatting ladder contracts (04 §10.1) + §6.1 retry ladder: downgrade latch
// fires ONCE and persists for the session; transient rich failures NEVER
// legacy-resent; parse-failure plain resend carries stripped content
// deterministically; timeouts excluded from retry; retry_after authoritative.

import { describe, expect, it, vi } from "vitest";
import {
	FormattingLadder,
	classifyRichFailure,
	stripMarkdownMarkup,
	type FormattingTransport,
} from "./formatting-ladder.js";
import {
	DELIVERY_FAILED_NOTICE,
	PLAIN_TEXT_FALLBACK_CAP,
	PLAIN_TEXT_FALLBACK_PREFIX,
	classifySendError,
	plainTextFallbackBody,
	sendWithRetry,
} from "./send-retry.js";

function transport(
	overrides: Partial<FormattingTransport> = {},
): FormattingTransport & {
	richCalls: number[];
	convertedCalls: number[];
	plainCalls: string[];
} {
	const richCalls: number[] = [];
	const convertedCalls: number[] = [];
	const plainCalls: string[] = [];
	return {
		richCalls,
		convertedCalls,
		plainCalls,
		async tryRich(content) {
			richCalls.push(1);
			return overrides.tryRich
				? overrides.tryRich(content, {})
				: { success: false, error: "bad request" };
		},
		async sendConverted(content) {
			convertedCalls.push(1);
			return overrides.sendConverted
				? overrides.sendConverted(content, {})
				: { success: true, messageId: "m1" };
		},
		async sendPlain(content) {
			plainCalls.push(content);
			return overrides.sendPlain
				? overrides.sendPlain(content, {})
				: { success: true, messageId: "m2" };
		},
	};
}

describe("rich/capability downgrade latch (§8 formatting row)", () => {
	it("capability error latches rich OFF ONCE; later sends skip tier 1 entirely", async () => {
		const t = transport({
			tryRich: async () => ({
				success: false,
				error: "sendRichMessage: Method not found",
			}),
		});
		const ladder = new FormattingLadder(t);
		await ladder.sendText("first");
		expect(ladder.richDisabled).toBe(true);
		expect(ladder.richLatchCount).toBe(1);
		expect(t.richCalls).toHaveLength(1); // probed exactly once per process
		for (let i = 0; i < 5; i++) await ladder.sendText(`later ${i}`);
		expect(t.richCalls).toHaveLength(1); // never retried — latch persists
		expect(t.convertedCalls).toHaveLength(6); // legacy path owns delivery
	});

	it("BadRequest-class failures fall back WITHOUT latching", async () => {
		const t = transport({
			tryRich: async () => ({
				success: false,
				error: "Bad Request: can't parse entities",
			}),
		});
		const ladder = new FormattingLadder(t);
		await ladder.sendText("a");
		expect(ladder.richDisabled).toBe(false); // next message may be fine
		await ladder.sendText("b");
		expect(t.richCalls).toHaveLength(2); // rich re-probed every send
	});

	it("TRANSIENT rich failures are NEVER legacy-resent (duplicate risk)", async () => {
		const t = transport({
			tryRich: async () => ({ success: false, error: "socket hang up" }),
		});
		const ladder = new FormattingLadder(t);
		const outcome = await ladder.sendText("payload");
		expect(outcome.tier).toBe("rich");
		expect(outcome.success).toBe(false);
		expect(outcome.retryable).toBe(true);
		expect(t.convertedCalls).toHaveLength(0); // no fallback send
		expect(t.plainCalls).toHaveLength(0);
	});

	it("expect-edits metadata skips tier 1 without latching", async () => {
		const t = transport();
		const ladder = new FormattingLadder(t);
		await ladder.sendText("x", { expect_edits: true });
		expect(t.richCalls).toHaveLength(0);
		expect(ladder.richDisabled).toBe(false);
	});
});

describe("parse-failure plain resend (tier 3)", () => {
	it("parse-classified converted failure resends STRIPPED content plain", async () => {
		const t = transport({
			sendConverted: async () => ({
				success: false,
				error: "Bad Request: can't parse entities",
			}),
		});
		const ladder = new FormattingLadder(t, {
			expectEditsMetadataKey: "expect_edits",
		});
		const outcome = await ladder.sendText("**bold** and snake_case_name");
		expect(outcome.tier).toBe("plain");
		expect(t.plainCalls[0]).toBe("bold and snake_case_name"); // stripped, byte-deterministic
	});

	it("non-parse converted failures surface as-is (no plain lane)", async () => {
		const t = transport({
			sendConverted: async () => ({ success: false, error: "forbidden" }),
		});
		const ladder = new FormattingLadder(t);
		const outcome = await ladder.sendText("x");
		expect(outcome.tier).toBe("converted");
		expect(t.plainCalls).toHaveLength(0);
	});
});

describe("classifier agreement (ONE shared blob)", () => {
	it("classifyRichFailure sorts capability / fallback / transient", () => {
		expect(classifyRichFailure("Endpoint Not Found")).toBe("capability");
		expect(classifyRichFailure("error 404")).toBe("capability");
		expect(classifyRichFailure("no such method")).toBe("capability");
		expect(classifyRichFailure("Bad Request: message too long")).toBe(
			"fallback",
		);
		expect(classifyRichFailure("unsupported")).toBe("fallback");
		expect(classifyRichFailure("connection reset by peer")).toBe("transient");
	});

	it("strip is deterministic — same input, same output bytes", () => {
		const input = "*i* **b** ~s~ ||sp|| \\_esc\\_ my_variable stays";
		expect(stripMarkdownMarkup(input)).toBe(stripMarkdownMarkup(input));
		expect(stripMarkdownMarkup(input)).not.toContain("*");
		expect(stripMarkdownMarkup(input)).toContain("my_variable"); // \b-guarded
	});
});

describe("§6.1 retry ladder", () => {
	it("timeout-classified errors are NOT retried and returned as-is", async () => {
		const attempts = vi.fn(async () => ({
			success: false,
			error: "request timed out",
		}));
		const sleep = vi.fn(async () => {});
		const result = await sendWithRetry("x", {}, attempts, { sleep });
		expect(result.success).toBe(false);
		expect(attempts).toHaveBeenCalledTimes(1);
		expect(sleep).not.toHaveBeenCalled();
	});

	it("network-classified errors retry ≤2 with backoff; exhaustion surfaces the notice lane", async () => {
		let calls = 0;
		const sleeps: number[] = [];
		const result = await sendWithRetry(
			"x",
			{},
			async () => {
				calls += 1;
				return {
					success: false,
					error: "ECONNRESET: connection reset",
					retryable: true,
				};
			},
			{
				sleep: async (ms) => {
					sleeps.push(ms);
				},
				jitterFraction: 0,
			},
		);
		expect(calls).toBe(3); // initial + 2 retries
		expect(sleeps).toEqual([2000, 4000]);
		expect(result.success).toBe(false);
		expect(result.retryable).toBe(true);
		void DELIVERY_FAILED_NOTICE;
	});

	it("server retry_after is AUTHORITATIVE over the local schedule, honored once", async () => {
		const sleeps: number[] = [];
		let calls = 0;
		await sendWithRetry(
			"x",
			{},
			async () => {
				calls += 1;
				return calls === 1
					? {
							success: false,
							error: "flood control: retry after 7",
							retryAfter: 7,
						}
					: { success: false, error: "connection refused", retryable: true };
			},
			{
				sleep: async (ms) => {
					sleeps.push(ms);
				},
				baseDelayMs: 500,
			},
		);
		expect(sleeps[0]).toBe(7000); // server value wins first
		expect(sleeps[1]).toBe(1000); // then local schedule: base·2^tryIndex = 500·2
	});

	it("connect-timeout IS retried (no connection was established)", async () => {
		let calls = 0;
		await sendWithRetry(
			"x",
			{},
			async () => {
				calls += 1;
				return { success: false, error: "ConnectTimeout: no connection" };
			},
			{ sleep: async () => {}, maxRetries: 1 },
		);
		expect(calls).toBe(2);
	});

	it("formatting-classified errors do NOT loop — caller routes to the plain lane", async () => {
		let calls = 0;
		const result = await sendWithRetry(
			"x",
			{},
			async () => {
				calls += 1;
				return { success: false, error: "Bad Request: can't parse entities" };
			},
			{ sleep: async () => {} },
		);
		expect(calls).toBe(1);
		expect(result.success).toBe(false);
	});

	it("classifySendError agrees with the fallback classifier on one blob", () => {
		expect(classifySendError(new Error("Request timed out"))).toBe("timeout");
		expect(classifySendError(new Error("getaddrinfo ENOTFOUND x"))).toBe(
			"network",
		);
		expect(classifySendError({ message: "retry after 12" })).toBe("flood");
		expect(classifySendError(new Error("can't parse entities"))).toBe(
			"formatting",
		);
		expect(classifySendError(new Error("Forbidden: bot was blocked"))).toBe(
			"permission",
		);
	});
});

describe("plain-text fallback body (§6.1)", () => {
	it("prefix + capped content, ≤3500 chars body", () => {
		const body = plainTextFallbackBody(
			"y".repeat(PLAIN_TEXT_FALLBACK_CAP + 500),
		);
		expect(body.startsWith(`${PLAIN_TEXT_FALLBACK_PREFIX}\n\n`)).toBe(true);
		expect(body.length).toBeLessThanOrEqual(
			PLAIN_TEXT_FALLBACK_PREFIX.length + 2 + PLAIN_TEXT_FALLBACK_CAP,
		);
	});
});
