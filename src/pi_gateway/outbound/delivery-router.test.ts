// Delivery router contracts (03 §9.5; §11 "Multi-target routing" row):
// transport resolution precedence, dead-target short-circuit + self-healing,
// oversize audit-save/truncation with INJECTED clock, silence-narration
// anti-loop guard, relay metadata re-attach.

import {
	mkdtempSync,
	existsSync,
	readFileSync,
	rmSync,
	writeFileSync,
	readdirSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { DeadTargetRegistry } from "./dead-targets.js";
import {
	classifiesWholeChatDeath,
	DeliveryRouter,
	isSilenceNarration,
	MAX_PLATFORM_OUTPUT,
	resolveDeliveryTransport,
	type RouterAdapter,
	type RouterConfig,
	type SendResult,
} from "./delivery-router.js";

let root: string;
let outputDir: string;

beforeAll(() => {
	root = mkdtempSync(join(tmpdir(), "pi-outbound-router-"));
	outputDir = join(root, "cron", "output");
});

afterAll(() => {
	rmSync(root, { recursive: true, force: true });
});

const INJECTED_NOW = new Date("2026-08-23T12:34:56Z");

interface SentRecord {
	platform: string;
	chatId: string;
	content: string;
	metadata: Record<string, unknown> | undefined;
}

function makeAdapter(
	name: string,
	opts?: Partial<
		Pick<RouterAdapter, "splitsLongMessages" | "frontsPlatform">
	> & {
		failWith?: string;
	},
): { adapter: RouterAdapter; sent: SentRecord[] } {
	const sent: SentRecord[] = [];
	const adapter: RouterAdapter = {
		name,
		send: async (platform, chatId, content, metadata) => {
			sent.push({ platform, chatId, content, metadata });
			if (opts?.failWith) return { success: false, error: opts.failWith };
			return { success: true } satisfies SendResult;
		},
	};
	if (opts?.splitsLongMessages !== undefined)
		adapter.splitsLongMessages = opts.splitsLongMessages;
	if (opts?.frontsPlatform !== undefined)
		adapter.frontsPlatform = opts.frontsPlatform;
	return { adapter, sent };
}

const CONFIG: RouterConfig = {
	platforms: {
		telegram: { enabled: true },
		discord: { enabled: true },
		slack: { enabled: false }, // disabled native must NOT shadow relay
	},
	getHomeChannel: (platform) =>
		platform === "telegram"
			? { chatId: "home-tg", userId: "u1", scopeId: "s1" }
			: null,
};

describe("silence-narration anti-loop tokens", () => {
	it.each([
		["*(silent)*"],
		["🔇"],
		["."],
		["…"],
		["silent"],
		["( no response )"],
		["_silent_"],
	])("%s drops", (token) => {
		expect(isSilenceNarration(token)).toBe(true);
	});

	it.each([
		["The deployment ran silently."],
		["Silence is golden — here is the plan..."],
		["Report attached. Silent failures: none."],
		[""],
	])("%j delivers", (prose) => {
		expect(isSilenceNarration(prose)).toBe(false);
	});

	it("null/undefined content never matches", () => {
		expect(isSilenceNarration(null)).toBe(false);
		expect(isSilenceNarration(undefined)).toBe(false);
	});

	it("over-length buffers are never narration even if they look like it", () => {
		expect(isSilenceNarration(`silent ${"x".repeat(70)}`)).toBe(false);
	});
});

describe("resolve_delivery_transport — native wins, relay only fronts advertised platforms", () => {
	it("a live enabled native adapter always wins over relay", () => {
		const native = makeAdapter("tg").adapter;
		const relay = makeAdapter("relay", { frontsPlatform: () => true }).adapter;
		const t = resolveDeliveryTransport("telegram", CONFIG, {
			telegram: native,
			relay,
		});
		expect(t).not.toBeNull();
		expect(t?.isRelay).toBe(false);
		expect(t?.adapter.name).toBe("tg");
	});

	it("relay becomes the transport ONLY for platforms it advertises", () => {
		const relay = makeAdapter("relay", {
			frontsPlatform: (p) => p === "whatsapp",
		}).adapter;
		const wa = resolveDeliveryTransport("whatsapp", CONFIG, { relay });
		expect(wa?.isRelay).toBe(true);
		expect(resolveDeliveryTransport("discord", CONFIG, { relay })).toBeNull(); // never hijacks
	});

	it("an explicitly DISABLED native does not shadow an enabled relay transport", () => {
		const disabledNative = makeAdapter("slack-native").adapter;
		const relay = makeAdapter("relay", { frontsPlatform: () => true }).adapter;
		const t = resolveDeliveryTransport("slack", CONFIG, {
			slack: disabledNative,
			relay,
		});
		expect(t?.isRelay).toBe(true);
	});
});

describe("dead-target short-circuit + self-healing", () => {
	function freshRouter(
		adapters: Record<string, RouterAdapter>,
	): DeliveryRouter {
		return new DeliveryRouter(
			CONFIG,
			adapters,
			new DeadTargetRegistry(
				join(root, `dead-${Math.random().toString(36).slice(2)}.json`),
			),
			outputDir,
		);
	}
	const T666 = [
		{
			targetString: "telegram:666",
			platform: "telegram",
			chatId: "666",
			isExplicit: true,
		},
	];

	it("confirmed-dead targets skip resends; a later successful send clears the flag", async () => {
		const failing = makeAdapter("tg", {
			failWith: "Forbidden: bot was blocked by the user",
		});
		const router = freshRouter({ telegram: failing.adapter });
		const first = await router.deliver({
			content: "tick",
			targets: T666,
			now: INJECTED_NOW,
		});
		expect(first["telegram:666"]?.success).toBe(false);

		// Second attempt short-circuits without touching the adapter.
		const sendsBefore = failing.sent.length;
		const second = await router.deliver({
			content: "tick",
			targets: T666,
			now: INJECTED_NOW,
		});
		expect(second["telegram:666"]?.skipped).toBe("dead_target");
		expect(failing.sent.length).toBe(sendsBefore);

		// Recovery: the registry's clear() IS the self-healing API (invoked after
		// any successful send / liveness signal); once clear, delivery proceeds.
		const registry = new DeadTargetRegistry(
			join(root, `dead-${Math.random().toString(36).slice(2)}.json`),
		);
		const failingB = makeAdapter("tg", {
			failWith: "Forbidden: group chat was deleted",
		});
		const routerB = new DeliveryRouter(
			CONFIG,
			{ telegram: failingB.adapter },
			registry,
			outputDir,
		);
		await routerB.deliver({ content: "x", targets: T666, now: INJECTED_NOW });
		expect(registry.isDead("telegram", "666")).toBe(true);
		// While flagged, even a HEALTHY adapter is short-circuited (Hermes parity).
		const healthyEarly = makeAdapter("tg-early");
		const early = new DeliveryRouter(
			CONFIG,
			{ telegram: healthyEarly.adapter },
			registry,
			outputDir,
		);
		const skipped = await early.deliver({
			content: "back",
			targets: T666,
			now: INJECTED_NOW,
		});
		expect(skipped["telegram:666"]?.skipped).toBe("dead_target");
		expect(healthyEarly.sent).toEqual([]);
		// Liveness signal (user re-adds the bot / manual send elsewhere) clears:
		expect(registry.clear("telegram", "666")).toBe(true);
		const healthy = makeAdapter("tg-ok");
		const healed = new DeliveryRouter(
			CONFIG,
			{ telegram: healthy.adapter },
			registry,
			outputDir,
		);
		const third = await healed.deliver({
			content: "back",
			targets: T666,
			now: INJECTED_NOW,
		});
		expect(third["telegram:666"]?.success).toBe(true);
		expect(registry.isDead("telegram", "666")).toBe(false); // stays healed
	});

	it("thread-level not_found NEVER marks a whole chat dead (topic deletions self-heal upstream)", async () => {
		const failing = makeAdapter("tg", {
			failWith: "Bad Request: thread not found",
		});
		const router = freshRouter({ telegram: failing.adapter });
		await router.deliver({
			content: "x",
			targets: [
				{
					targetString: "telegram:5:t9",
					platform: "telegram",
					chatId: "5",
					threadId: "t9",
					isExplicit: true,
				},
			],
			now: INJECTED_NOW,
		});
		expect(classifiesWholeChatDeath("Bad Request: thread not found")).toBe(
			false,
		);
		expect(
			classifiesWholeChatDeath("Forbidden: the group chat was deleted"),
		).toBe(true);
		expect(classifiesWholeChatDeath("Chat not found")).toBe(true);
		expect(classifiesWholeChatDeath("rate limited")).toBe(false);
	});
});

describe("oversize handling (>4000 chars): audit save ALWAYS, truncation per capability", () => {
	const LONG = "y".repeat(MAX_PLATFORM_OUTPUT + 500);
	const T1 = [
		{
			targetString: "telegram:1",
			platform: "telegram",
			chatId: "1",
			isExplicit: true,
		},
	];

	it("non-chunking adapters receive truncated body + footer pointing at the saved audit file", async () => {
		const { adapter, sent } = makeAdapter("tg");
		const router = new DeliveryRouter(
			CONFIG,
			{ telegram: adapter },
			new DeadTargetRegistry(),
			outputDir,
		);
		await router.deliver({
			content: LONG,
			targets: T1,
			jobId: "job-a",
			now: INJECTED_NOW,
		});
		const delivered = sent[0]?.content ?? "";
		expect(delivered.length).toBeLessThanOrEqual(MAX_PLATFORM_OUTPUT);
		expect(delivered).toContain("[truncated, full output saved to ");
		const auditPath = delivered.match(/saved to ([^\]]+)\]/)?.[1] ?? "";
		expect(existsSync(auditPath)).toBe(true);
		expect(readFileSync(auditPath, "utf8")).toHaveLength(LONG.length); // FULL audit copy
	});

	it("chunking-capable adapters receive the FULL payload and split natively; audit copy still written", async () => {
		const { adapter, sent } = makeAdapter("chunker", {
			splitsLongMessages: true,
		});
		const router = new DeliveryRouter(
			CONFIG,
			{ telegram: adapter },
			new DeadTargetRegistry(),
			outputDir,
		);
		await router.deliver({
			content: LONG,
			targets: T1,
			jobId: "job-b",
			now: INJECTED_NOW,
		});
		expect(sent[0]?.content).toHaveLength(LONG.length);
		const files = readdirSync(outputDir).filter((f) => f.startsWith("job-b_"));
		expect(files.length).toBeGreaterThan(0);
	});

	it("audit-save failure makes the non-chunking path fail that target (footer needs its path)", async () => {
		const okTarget = makeAdapter("tg");
		const fileAsDir = join(root, "occupied");
		writeFileSync(fileAsDir, "occupied", "utf8"); // outputDir points AT A FILE ⇒ saves throw
		const router = new DeliveryRouter(
			CONFIG,
			{ telegram: okTarget.adapter },
			new DeadTargetRegistry(),
			fileAsDir,
		);
		const results = await router.deliver({
			content: LONG,
			targets: T1,
			jobId: "job-c",
			now: INJECTED_NOW,
		});
		expect(results["telegram:1"]?.success).toBe(false);
	});
});

describe("per-target behavior", () => {
	it("LOCAL delivery writes framed markdown under cron/output; never filtered, never dead-tracked", async () => {
		const router = new DeliveryRouter(
			CONFIG,
			{},
			new DeadTargetRegistry(),
			outputDir,
		);
		const r = await router.deliver({
			content: "*(silent)*", // narration filter does NOT apply to local
			targets: [{ targetString: "local", platform: "local" }],
			jobId: "job-d",
			jobName: "Nightly digest",
			metadata: { tick: 7 },
			now: INJECTED_NOW,
		});
		const path = (r.local?.result as { path?: string })?.path ?? "";
		expect(existsSync(path)).toBe(true);
		const text = readFileSync(path, "utf8");
		expect(text).toContain("# Nightly digest");
		expect(text).toContain("**Job ID:** job-d");
		expect(text).toContain("**tick:** 7");
		expect(text.endsWith("*(silent)*")).toBe(true);
	});

	it("silence narration to a PLATFORM target drops before egress (no adapter send)", async () => {
		const { adapter, sent } = makeAdapter("tg");
		const router = new DeliveryRouter(
			CONFIG,
			{ telegram: adapter },
			new DeadTargetRegistry(),
			outputDir,
		);
		const r = await router.deliver({
			content: ".",
			targets: [
				{
					targetString: "telegram:9",
					platform: "telegram",
					chatId: "9",
					isExplicit: true,
				},
			],
			now: INJECTED_NOW,
		});
		expect(r["telegram:9"]).toEqual({
			success: true,
			result: { filtered: "silence_narration", delivered: false },
		});
		expect(sent).toEqual([]);
	});

	it("relay transports re-attach user_id/scope_id on HOME-channel sends ONLY", async () => {
		const calls: SentRecord[] = [];
		const relay: RouterAdapter = {
			name: "relay",
			frontsPlatform: () => true,
			send: async (platform, chatId, content, metadata) => {
				calls.push({ platform, chatId, content, metadata });
				return { success: true };
			},
		};
		const router = new DeliveryRouter(
			CONFIG,
			{ relay },
			new DeadTargetRegistry(),
			outputDir,
		);
		await router.deliver({
			content: "hi",
			targets: [
				{
					targetString: "telegram:home-tg",
					platform: "telegram",
					chatId: "home-tg",
					isExplicit: true,
				},
				{
					targetString: "telegram:other",
					platform: "telegram",
					chatId: "other",
					isExplicit: true,
				},
			],
			now: INJECTED_NOW,
		});
		expect(calls).toHaveLength(2);
		expect(calls[0]?.metadata?.user_id).toBe("u1"); // home channel
		expect(calls[0]?.metadata?.scope_id).toBe("s1");
		expect(calls[1]?.metadata?.user_id).toBeUndefined(); // unrelated target
	});

	it("explicit thread id lands in thread metadata when unset (group chat ⇒ generic stamping)", async () => {
		const { adapter, sent } = makeAdapter("tg");
		const router = new DeliveryRouter(
			CONFIG,
			{ telegram: adapter },
			new DeadTargetRegistry(),
			outputDir,
		);
		await router.deliver({
			content: "hi",
			targets: [
				{
					targetString: "telegram:-1003:T7",
					platform: "telegram",
					chatId: "-1003",
					threadId: "T7",
					isExplicit: true,
				},
			],
			now: INJECTED_NOW,
		});
		expect(sent[0]?.metadata?.thread_id).toBe("T7");
	});
});

// Named Telegram private DM topics (delivery.py:_deliver_to_platform; egress-5).
// A non-numeric thread id on a POSITIVE chat id is a topic NAME — resolved via
// adapter.ensure_dm_topic BEFORE the send, never sent to the wire verbatim.
describe("named Telegram private DM topics (_deliver_to_platform)", () => {
	interface TopicCall {
		chatId: string;
		topicName: string;
		forceCreate: boolean | undefined;
	}

	function topicAdapter(opts?: {
		failFirstSendWith?: string;
		ensureReturns?: string | null;
	}) {
		const sent: SentRecord[] = [];
		const ensureCalls: TopicCall[] = [];
		const adapter: RouterAdapter = {
			name: "tg-topics",
			send: async (platform, chatId, content, metadata) => {
				// Snapshot metadata AT SEND TIME (Hermes RecordingAdapter parity:
				// dict(metadata or {})) — the refresh ladder mutates the dict
				// between attempt 1 and the retry.
				sent.push({
					platform,
					chatId,
					content,
					metadata: metadata ? { ...metadata } : metadata,
				});
				if (opts?.failFirstSendWith !== undefined && sent.length === 1) {
					return { success: false, error: opts.failFirstSendWith };
				}
				return { success: true } satisfies SendResult;
			},
			ensureDmTopic: async (chatId, topicName, forceCreate) => {
				ensureCalls.push({ chatId, topicName, forceCreate });
				return opts?.ensureReturns !== undefined
					? opts.ensureReturns
					: forceCreate
						? "38064"
						: "38049";
			},
		};
		return { adapter, sent, ensureCalls };
	}

	function routerWith(adapter: RouterAdapter): DeliveryRouter {
		return new DeliveryRouter(
			CONFIG,
			{ telegram: adapter },
			new DeadTargetRegistry(),
			outputDir,
		);
	}

	const NAMED = {
		targetString: "telegram:722341991:Hermes API Test",
		platform: "telegram",
		chatId: "722341991",
		threadId: "Hermes API Test",
		isExplicit: true,
	};

	it("named private topic is created BEFORE delivery; wire metadata carries the created thread id", async () => {
		const { adapter, sent, ensureCalls } = topicAdapter();
		const r = await routerWith(adapter).deliver({
			content: "hello",
			targets: [NAMED],
			now: INJECTED_NOW,
		});
		expect(r[NAMED.targetString]?.success).toBe(true);
		expect(ensureCalls).toEqual([
			{
				chatId: "722341991",
				topicName: "Hermes API Test",
				forceCreate: undefined,
			},
		]);
		expect(sent).toHaveLength(1);
		expect(sent[0]?.metadata).toEqual({
			thread_id: "38049",
			telegram_dm_topic_created_for_send: true,
		});
	});

	it("FAILS CLOSED when the adapter cannot create named topics — raw name never reaches the wire", async () => {
		const bare = makeAdapter("tg-no-topics");
		const r = await routerWith(bare.adapter).deliver({
			content: "hello",
			targets: [NAMED],
			now: INJECTED_NOW,
		});
		expect(r[NAMED.targetString]?.success).toBe(false);
		expect(r[NAMED.targetString]?.error).toContain(
			"cannot create named private DM topics",
		);
		expect(bare.sent).toEqual([]); // no wire send with a NAME as thread_id
	});

	it("creation returning null fails closed", async () => {
		const { adapter, sent } = topicAdapter({ ensureReturns: null });
		const r = await routerWith(adapter).deliver({
			content: "hello",
			targets: [NAMED],
			now: INJECTED_NOW,
		});
		expect(r[NAMED.targetString]?.error).toBe(
			"Failed to create Telegram private DM topic 'Hermes API Test'",
		);
		expect(sent).toEqual([]);
	});

	it("legacy NUMERIC private topic WITH a reply anchor uses the reply-fallback lane", async () => {
		const { adapter, sent, ensureCalls } = topicAdapter();
		await routerWith(adapter).deliver({
			content: "hello",
			metadata: { telegram_reply_to_message_id: "9001" },
			targets: [
				{
					targetString: "telegram:722341991:32344",
					platform: "telegram",
					chatId: "722341991",
					threadId: "32344",
					isExplicit: true,
				},
			],
			now: INJECTED_NOW,
		});
		expect(ensureCalls).toEqual([]); // numeric id ⇒ no creation
		expect(sent[0]?.metadata).toEqual({
			telegram_reply_to_message_id: "9001",
			thread_id: "32344",
			telegram_dm_topic_reply_fallback: true,
		});
	});

	it("legacy NUMERIC private topic WITHOUT a reply anchor fails closed pre-flight", async () => {
		const { adapter, sent } = topicAdapter();
		const r = await routerWith(adapter).deliver({
			content: "hello",
			targets: [
				{
					targetString: "telegram:722341991:32344",
					platform: "telegram",
					chatId: "722341991",
					threadId: "32344",
					isExplicit: true,
				},
			],
			now: INJECTED_NOW,
		});
		expect(r["telegram:722341991:32344"]?.success).toBe(false);
		expect(r["telegram:722341991:32344"]?.error).toContain(
			"requires telegram_reply_to_message_id",
		);
		expect(sent).toEqual([]);
		// Pre-flight failure is not a dead target (no forbidden/chat not_found).
		expect(classifiesWholeChatDeath(r["telegram:722341991:32344"]?.error)).toBe(
			false,
		);
	});

	it("stale created topic: 'thread not found' failure refreshes (force_create) and retries ONCE", async () => {
		const { adapter, sent, ensureCalls } = topicAdapter({
			failFirstSendWith: "Bad Request: message thread not found",
		});
		const r = await routerWith(adapter).deliver({
			content: "hello",
			targets: [NAMED],
			now: INJECTED_NOW,
		});
		expect(r[NAMED.targetString]?.success).toBe(true);
		expect(ensureCalls).toEqual([
			{
				chatId: "722341991",
				topicName: "Hermes API Test",
				forceCreate: undefined,
			},
			{ chatId: "722341991", topicName: "Hermes API Test", forceCreate: true },
		]);
		expect(sent).toHaveLength(2); // exactly ONE retry
		expect(sent[0]?.metadata?.thread_id).toBe("38049");
		expect(sent[1]?.metadata?.thread_id).toBe("38064"); // refreshed binding
		expect(sent[1]?.metadata?.telegram_dm_topic_created_for_send).toBe(true);
	});

	it("refresh retry failing too surfaces the send error; thread-level failure never marks the chat dead", async () => {
		const registry = new DeadTargetRegistry();
		const sent: SentRecord[] = [];
		const adapter: RouterAdapter = {
			name: "tg-always-stale",
			ensureDmTopic: async () => "38099",
			send: async (platform, chatId, content, metadata) => {
				sent.push({ platform, chatId, content, metadata });
				return {
					success: false,
					error: "Bad Request: message thread not found",
				};
			},
		};
		const router = new DeliveryRouter(
			CONFIG,
			{ telegram: adapter },
			registry,
			outputDir,
		);
		const r = await router.deliver({
			content: "hello",
			targets: [NAMED],
			now: INJECTED_NOW,
		});
		expect(r[NAMED.targetString]?.success).toBe(false);
		expect(sent).toHaveLength(2); // initial + one refresh retry, then give up
		expect(registry.isDead("telegram", "722341991")).toBe(false);
	});

	it("metadata thread keys WIN over the target string: no creation, no stamping", async () => {
		const { adapter, sent, ensureCalls } = topicAdapter();
		await routerWith(adapter).deliver({
			content: "hi",
			metadata: { thread_id: "555" },
			targets: [NAMED],
			now: INJECTED_NOW,
		});
		expect(ensureCalls).toEqual([]);
		expect(sent[0]?.metadata?.thread_id).toBe("555");
		expect(
			sent[0]?.metadata?.telegram_dm_topic_created_for_send,
		).toBeUndefined();
	});

	it("explicit direct-messages-topic metadata bypasses the named-topic branch entirely", async () => {
		const { adapter, sent, ensureCalls } = topicAdapter();
		await routerWith(adapter).deliver({
			content: "hi",
			metadata: { direct_messages_topic_id: "777" },
			targets: [NAMED],
			now: INJECTED_NOW,
		});
		expect(ensureCalls).toEqual([]);
		expect(sent[0]?.metadata?.direct_messages_topic_id).toBe("777");
		expect(sent[0]?.metadata?.thread_id).toBeUndefined();
	});

	it("GROUP chats (negative ids) never enter the ladder — plain generic stamping", async () => {
		const { adapter, sent, ensureCalls } = topicAdapter();
		await routerWith(adapter).deliver({
			content: "hi",
			targets: [
				{
					targetString: "telegram:-100777:Hermes API Test",
					platform: "telegram",
					chatId: "-100777",
					threadId: "Hermes API Test",
					isExplicit: true,
				},
			],
			now: INJECTED_NOW,
		});
		expect(ensureCalls).toEqual([]);
		expect(sent[0]?.metadata).toEqual({ thread_id: "Hermes API Test" });
	});
});
