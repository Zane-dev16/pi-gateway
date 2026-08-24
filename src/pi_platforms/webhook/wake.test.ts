// BEHAVIOR CONTRACTS — DEC-022 wake-lane close-out for the stateless shape.
//
// 1. Lane selection derives from supports_async_delivery (raw-key-direct here).
// 2. THE STATELESS LANE END-TO-END: background completions re-enter via
//    self-post /v1/chat/completions carrying the RAW X-Hermes-Session-Id;
//    the turn runs through BOTH guards bound to the RAW key (no derived key).
// 3. 429-backoff ladder 2s/5s/10s (4 attempts) under injected sleeps;
//    exhaustion RAISES loudly so callers REWIND cursors (at-least-once).
//    Non-transient ≥400 fails immediately; missing key refuses before any post.
// 4. The FORGED-EVENT lane still traverses BOTH guards for this adapter's
//    pipeline: an internal wake is a TURN — busy semantics apply, never a
//    guard bypass (DEC-022 rejects guard-bypass outright).

import { describe, expect, it } from "vitest";
import { FakePlatformWire } from "../conformance/wire.js";
import { WebhookAdapter } from "./webhook-adapter.js";
import {
	StatelessWakeRail,
	WakeRailMisconfiguredError,
	adapterSupportsPush,
} from "./wake.js";
import { internalWakeEvent } from "../conformance/harness.js";
import { ManualTimers } from "./testing/manual-timers.js";

function makeAdapter(opts?: {
	streamIsMessageChatIds?: ReadonlySet<string> | undefined;
}) {
	const wire = new FakePlatformWire();
	const timers = new ManualTimers();
	const adapter = new WebhookAdapter({
		wire,
		globalSecretReader: () => "webhook-secret",
		apiKeyProvider: () => "test-api-key",
		...(opts?.streamIsMessageChatIds !== undefined
			? { streamIsMessageChatIds: opts.streamIsMessageChatIds }
			: {}),
	});
	adapter.attachStandardGuard();
	return { adapter, wire, timers };
}

describe("lane selection (gateway/wake.py:adapter_supports_push)", () => {
	it("declares raw-key-direct because supports_async_delivery is False", () => {
		const { adapter } = makeAdapter();
		expect(adapter.supportsAsyncDelivery).toBe(false);
		expect(adapter.interactiveResume).toBe(false);
		expect(adapter.wakeLane).toBe("raw-key-direct");
		expect(adapterSupportsPush(adapter)).toBe(false);
	});

	it("undeclared capability defaults push-capable (getattr-with-default parity)", () => {
		expect(adapterSupportsPush({})).toBe(true);
	});
});

describe("stateless wake rail — self-post mechanics", () => {
	function makeRail(opts?: {
		statusScript?: number[] | undefined;
		apiKey?: string | undefined;
	}) {
		const { timers } = makeAdapter();
		const posts: Array<{
			url: string;
			headers: Record<string, string>;
			body: string;
		}> = [];
		let call = 0;
		const statuses = opts?.statusScript ?? [200];
		const rail = new StatelessWakeRail({
			baseUrl: "http://127.0.0.1:8642",
			apiKeyProvider: () =>
				opts?.apiKey === undefined ? "wake-key" : opts.apiKey,
			post: async (url, init) => {
				posts.push({ url, headers: init.headers, body: init.body });
				call += 1;
				const status = statuses[Math.min(call - 1, statuses.length - 1)] ?? 500;
				return {
					status,
					bodyText:
						status === 200
							? JSON.stringify({
									choices: [{ message: { content: "wake-reply" } }],
								})
							: "rate limited",
				};
			},
			sleepMs: (ms) => timers.sleep(ms),
		});
		return { rail, posts, timers };
	}

	async function drainSleeps(timers: ManualTimers): Promise<number[]> {
		// Let production code reach its first await before draining.
		await new Promise<void>((r) => setTimeout(r, 2));
		const delays: number[] = [];
		while (timers.sleepWaiters.length > 0) {
			const waiter = timers.sleepWaiters[0];
			delays.push(waiter?.ms ?? -1);
			timers.releaseOneSleep();
			await new Promise<void>((r) => setTimeout(r, 1));
		}
		return delays;
	}

	it("self-posts Bearer + RAW X-Hermes-Session-Id to its own /v1/chat/completions", async () => {
		const { rail, posts } = makeRail();
		const outcome = await rail.wake(
			"agent:main:api_server:dm:abc",
			"done: build ok",
		);
		expect(outcome).toEqual({ ok: true, status: 200, reply: "wake-reply" });
		const post = posts[0];
		expect(post?.url).toBe("http://127.0.0.1:8642/v1/chat/completions");
		expect(post?.headers["authorization"]).toBe("Bearer wake-key");
		// THE RAW KEY rides the header — never a derived routing key (DEC-022).
		expect(post?.headers["x-hermes-session-id"]).toBe(
			"agent:main:api_server:dm:abc",
		);
		expect(post?.body).toContain("done: build ok");
	});

	it("429 backs off 2s/5s/10s then succeeds on the 4th attempt", async () => {
		const { rail, posts, timers } = makeRail({
			statusScript: [429, 429, 429, 200],
		});
		const pending = rail.wake("sid-1", "completion text");
		const delays = await drainSleeps(timers);
		const outcome = await pending;
		expect(outcome.ok).toBe(true);
		expect(posts).toHaveLength(4);
		expect(delays).toEqual([2_000, 5_000, 10_000]);
	});

	it("exhaustion RAISES loudly — callers rewind cursors instead of marking delivered", async () => {
		const { rail, posts, timers } = makeRail({
			statusScript: [429, 429, 429, 429],
		});
		const pending = rail.wake("sid-2", "completion text");
		pending.catch(() => {});
		await new Promise<void>((r) => setTimeout(r, 2)); // park on the ladder
		await drainSleeps(timers);
		await expect(pending).rejects.toMatchObject({
			name: "WakeRailError",
			transient: true,
			attempts: 4,
		});
		expect(posts).toHaveLength(4);
	});

	it("non-transient ≥400 fails IMMEDIATELY (no retries burned)", async () => {
		const { rail, posts, timers } = makeRail({ statusScript: [403] });
		await expect(rail.wake("sid-3", "text")).rejects.toMatchObject({
			name: "WakeRailError",
			transient: false,
		});
		expect(posts).toHaveLength(1);
		expect(timers.sleepWaiters).toHaveLength(0);
	});

	it("network-classified failures retry on the same ladder", async () => {
		const call = 0;
		const timers = new ManualTimers();
		let attempts = 0;
		const rail = new StatelessWakeRail({
			baseUrl: "http://127.0.0.1:9",
			apiKeyProvider: () => "k",
			post: async () => {
				attempts += 1;
				if (attempts < 3) throw new Error("ECONNRESET");
				return { status: 200, bodyText: "{}" };
			},
			sleepMs: (ms) => timers.sleep(ms),
		});
		const pending = rail.wake("sid-4", "t");
		await new Promise<void>((r) => setTimeout(r, 2)); // park on the ladder
		while (timers.sleepWaiters.length > 0) {
			timers.releaseOneSleep();
			await new Promise<void>((r) => setTimeout(r, 1));
		}
		const outcome = await pending;
		void call;
		expect(outcome.ok).toBe(true);
		expect(attempts).toBe(3);
	});

	it("missing API_SERVER_KEY refuses BEFORE posting (continuation would be 403-gated)", async () => {
		const { rail, posts } = makeRail({ apiKey: "" });
		await expect(rail.wake("sid-5", "t")).rejects.toBeInstanceOf(
			WakeRailMisconfiguredError,
		);
		expect(posts).toHaveLength(0);
	});

	it("empty raw session id refuses — derived keys can never match stateless chats", async () => {
		const { rail, posts } = makeRail();
		await expect(rail.wake("", "t")).rejects.toBeInstanceOf(
			WakeRailMisconfiguredError,
		);
		expect(posts).toHaveLength(0);
	});
});

describe("forged-event lane traverses BOTH guards even on the stateless adapter", () => {
	it("an idle internal wake becomes ONE real turn", async () => {
		const { adapter } = makeAdapter();
		await adapter.deliverInbound(internalWakeEvent(), "sess-w1");
		await new Promise<void>((r) => setTimeout(r, 5));
		expect(adapter.turnLog).toEqual(["[internal wake]"]);
	});

	it("a BUSY internal wake obeys the busy ladder: single pending slot, drain turn — never inline bypass", async () => {
		const { adapter } = makeAdapter();
		adapter.holdTurns(true);

		const head = adapter.deliverInbound(
			{
				messageType: "text",
				text: "head turn",
				source: {
					platform: "webhook",
					chatType: "dm",
					chatId: "c-busy",
				},
			},
			"sess-w2",
		);
		await new Promise<void>((r) => setTimeout(r, 5));
		// Guard L1 installed synchronously: the session IS active mid-turn.
		expect(adapter.guardIsActiveForTest("sess-w2")).toBe(true);

		// The forged wake arrives while busy — it MUST NOT run inline.
		const wake = adapter.deliverInbound(internalWakeEvent(), "sess-w2");
		await Promise.race([wake, new Promise<void>((r) => setTimeout(r, 5))]);
		expect(adapter.turnLog).toEqual(["head turn"]); // no bypass turn
		expect(adapter.guardPendingCountForTest("sess-w2")).toBe(1);

		adapter.holdTurns(false);
		await head;
		await new Promise<void>((r) => setTimeout(r, 20));
		expect(adapter.turnsDrained()).toEqual(["head turn", "[internal wake]"]);
	});
});
