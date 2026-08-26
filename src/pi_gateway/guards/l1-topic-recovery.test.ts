// Guard-level contracts for the Telegram DM topic-recovery hook (fix cluster
// "telegram-topic-bindings"): base.py handle_message applies
// _apply_topic_recovery BEFORE build_session_key, so a lobby-shaped DM reply
// (''/General "1") is keyed onto the user's last-active topic lane instead of
// splitting history across a fresh lobby session.
//
// Hermes anchors: gateway/run.py:_recover_telegram_topic_thread_id,
// run.py:_handle_message_with_agent ("after route recovery" drop),
// gateway/platforms/base.py:_apply_topic_recovery + set_topic_recovery_fn.

import { describe, expect, it } from "vitest";

import type { TelegramTopicBindingRow } from "../../pi_state/index.js";
import { buildSessionKey } from "../resolution/session-key.js";
import type { SessionSource } from "../resolution/session-key.js";
import { recoverTelegramTopicThreadId } from "../resolution/telegram-topic-recovery.js";
import { TELEGRAM_GENERAL_TOPIC_IDS } from "../resolution/telegram-topic-recovery.js";
import type { IncomingEvent } from "./events.js";
import { makeFixture } from "./testing/manual-spawner.js";

const LOBBY_KEY = "agent:main:telegram:dm:100";
const LANE_222_KEY = "agent:main:telegram:dm:100:222";

function bindingRow(threadId: string): TelegramTopicBindingRow {
	return {
		chat_id: "100",
		thread_id: threadId,
		user_id: "u1",
		session_key: `agent:main:telegram:dm:100:${threadId}`,
		session_id: `s-${threadId}`,
		managed_mode: "auto",
		linked_at: 1,
		updated_at: 2,
	};
}

function fakeRecoverFn(newestLane: string | null) {
	const storeRows = newestLane === null ? [] : [bindingRow(newestLane)];
	return (source: SessionSource): string | null =>
		recoverTelegramTopicThreadId(source, {
			topicModeEnabled: () => true,
			listTelegramTopicBindingsForChat: () =>
				storeRows.filter((r) => r.user_id === String(source.userId ?? "")),
		});
}

function dmEvent(
	threadId: string | undefined,
	extra: Partial<IncomingEvent> = {},
): IncomingEvent {
	return {
		messageType: "text",
		text: "hello from the client",
		source: {
			platform: "telegram",
			chatType: "dm",
			userId: "u1",
			chatId: "100",
			...(threadId !== undefined ? { threadId } : {}),
		},
		...extra,
	};
}

describe("L1 pre-key topic recovery (base.py:_apply_topic_recovery ordering)", () => {
	it("lobby reply ('' thread) rewrites source.threadId and keys the session onto the recovered lane", async () => {
		const f = makeFixture({
			topicThreadRecovery: fakeRecoverFn("222"),
			rebuildSessionKey: (s) => buildSessionKey(s),
		});
		const event = dmEvent(undefined);

		await f.guard.handleMessage(event, LOBBY_KEY);
		expect(f.scheduler.queue.length).toBe(1);

		// The caller's key was the lobby; the guard routed the turn under the
		// RE-DERIVED lane key instead.
		expect(f.guard.isActive(LANE_222_KEY)).toBe(true);
		expect(f.guard.isActive(LOBBY_KEY)).toBe(false);

		// The event object carries the rewrite (caller-visible mutation parity
		// of dataclasses.replace(event.source, thread_id=...)).
		expect(event.source?.threadId).toBe("222");

		await f.scheduler.quiesce();
		expect(f.turns).toEqual(["hello from the client"]);
	});

	it("General ('1') is lobby-shaped too", async () => {
		const f = makeFixture({
			topicThreadRecovery: fakeRecoverFn("17585"),
			rebuildSessionKey: (s) => buildSessionKey(s),
		});
		const event = dmEvent("1");
		await f.guard.handleMessage(event, `${LOBBY_KEY}:1`);

		expect(f.guard.isActive("agent:main:telegram:dm:100:17585")).toBe(true);
		expect(f.guard.isActive(`${LOBBY_KEY}:1`)).toBe(false);
		await f.scheduler.quiesce();
	});

	it("non-lobby unknown thread id passes through UNTOUCHED — brand-new topics keep their own lane (#31086)", async () => {
		const f = makeFixture({
			topicThreadRecovery: fakeRecoverFn("222"),
			rebuildSessionKey: (s) => buildSessionKey(s),
		});
		const event = dmEvent("99999");
		const originalSource = event.source;

		await f.guard.handleMessage(event, LOBBY_KEY);

		expect(event.source).toBe(originalSource); // identity preserved
		expect(f.guard.isActive(LOBBY_KEY)).toBe(true);
		await f.scheduler.quiesce();
	});

	it("recovery hitting the SAME thread is a no-op: source identity preserved, key unchanged", async () => {
		const f = makeFixture({
			topicThreadRecovery: fakeRecoverFn("222"),
			rebuildSessionKey: (s) => buildSessionKey(s),
		});
		const event = dmEvent("222");
		const originalSource = event.source;

		await f.guard.handleMessage(event, LANE_222_KEY);

		expect(event.source).toBe(originalSource);
		expect(f.guard.isActive(LANE_222_KEY)).toBe(true);
		await f.scheduler.quiesce();
	});

	it("group arrivals never consult the hook (base.py needs_topic_recovery gate)", async () => {
		let consulted = 0;
		const f = makeFixture({
			topicThreadRecovery: (source) => {
				consulted++;
				return fakeRecoverFn("222")(source);
			},
			rebuildSessionKey: (s) => buildSessionKey(s),
		});
		const event: IncomingEvent = {
			messageType: "text",
			text: "group chatter",
			source: {
				platform: "telegram",
				chatType: "group",
				userId: "u1",
				chatId: "-100123",
				threadId: "555",
			},
		};
		const groupKey = buildSessionKey(event.source!);
		await f.guard.handleMessage(event, groupKey);

		expect(consulted).toBe(0);
		expect(f.guard.isActive(groupKey)).toBe(true);
		await f.scheduler.quiesce();
	});

	it("internally-routed event whose stamped key no longer matches AFTER recovery is DROPPED loudly (run.py 'after route recovery')", async () => {
		const f = makeFixture({
			topicThreadRecovery: fakeRecoverFn("222"),
			rebuildSessionKey: (s) => buildSessionKey(s),
		});
		const event = dmEvent(undefined, {
			internal: true,
			metadata: { gateway_session_key: LOBBY_KEY },
		});

		await f.guard.handleMessage(event, LOBBY_KEY);

		// Derived (lane 222) != expected (lobby) ⇒ drop, no turn anywhere.
		expect(f.turns).toEqual([]);
		expect(f.scheduler.queue.length).toBe(0);
		expect(f.guard.isActive(LANE_222_KEY)).toBe(false);
		expect(f.warnings.some((w) => w.includes("expected session="))).toBe(true);
	});

	it("hook failure degrades to the ORIGINAL key with a warning — never blocks dispatch", async () => {
		const f = makeFixture({
			topicThreadRecovery: () => {
				throw new Error("store exploded");
			},
			rebuildSessionKey: (s) => buildSessionKey(s),
		});
		const event = dmEvent(undefined);

		await f.guard.handleMessage(event, LOBBY_KEY);

		expect(f.guard.isActive(LOBBY_KEY)).toBe(true);
		expect(
			f.warnings.some((w) => w.includes("Topic recovery hook failed")),
		).toBe(true);
		await f.scheduler.quiesce();
		expect(f.turns).toEqual(["hello from the client"]);
	});

	it("recovery hook WITHOUT the key rebuilder is inert (pair required)", async () => {
		const f = makeFixture({
			topicThreadRecovery: fakeRecoverFn("222"),
		});
		const event = dmEvent(undefined);
		const originalSource = event.source;

		await f.guard.handleMessage(event, LOBBY_KEY);

		expect(event.source).toBe(originalSource);
		expect(f.guard.isActive(LOBBY_KEY)).toBe(true);
		await f.scheduler.quiesce();
	});

	it("a busy lobby session does NOT swallow a lane-bound reply: the reply keys to its own lane", async () => {
		const f = makeFixture({
			topicThreadRecovery: fakeRecoverFn("222"),
			rebuildSessionKey: (s) => buildSessionKey(s),
		});

		// Parked turn on the LOBBY key (thread '' arrives while mode binds 222;
		// first arrival predates binding knowledge in this scenario).
		f.holdTurns(true);
		const head = dmEvent(undefined);
		await f.guard.handleMessage(head, LOBBY_KEY);
		const headFrame = f.scheduler.queue.shift()!;
		headFrame.start(); // parks inside handler

		// The follow-up reply arrives from the BOUND lane: it must NOT queue
		// behind the lobby's pending slot — different key, own session.
		const laneReply = dmEvent(undefined);
		laneReply.source!.userIdAlt = "u1";
		await f.guard.handleMessage(laneReply, LOBBY_KEY);

		expect(f.guard.pendingOf(LOBBY_KEY)).toBeUndefined();
		expect(f.guard.isActive(LANE_222_KEY)).toBe(true);

		f.holdTurns(false);
		await f.scheduler.quiesce();
		expect(f.maxHandlerConcurrency).toBeGreaterThanOrEqual(1);
		expect(f.turns.filter((t) => t === "hello from the client")).toHaveLength(
			2,
		);
	});
});

describe("TELEGRAM_GENERAL_TOPIC_IDS parity (run.py frozenset)", () => {
	it("'' and '1' only", () => {
		expect([...TELEGRAM_GENERAL_TOPIC_IDS].sort()).toEqual(["", "1"].sort());
	});
});
