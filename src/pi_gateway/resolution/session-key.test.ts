// Behavior contracts: routing-key construction + the ONE shared
// participant-isolation predicate (02-session-and-state.md §4 preamble,
// §4.4; DEC-028). Roadmap Phase 1 required contract #5.
//
// Asserted by relationship — never by echoing implementations:
//   - DMs are never shared (predicate false under every flag combination).
//   - Thread-context sessions are shared unless thread_sessions_per_user.
//   - Non-thread group/channel sessions isolate per sender by default
//     (group_sessions_per_user=true); flipping the flag shares one session.
//   - DEC-028 property: the initiating message of an auto-thread classifies
//     CONSISTENTLY between key construction and the predicate — both read the
//     effective thread slot (thread_id ?? prospective_thread_id), and the
//     initiator byte-matches the in-thread follow-ups (chat_type slot
//     rewritten to `thread`).
//   - ONE-predicate invariant: the participant slot is appended IFF the
//     predicate says isolated AND a participant identifier exists — swept
//     across contexts × flags, so key construction cannot drift from the
//     predicate (02 §14 review checklist row).

import { describe, expect, it } from "vitest";

import {
	buildSessionKey,
	isSharedMultiUserSession,
	sessionKeyNamespace,
	type IsolationFlags,
	type SessionSource,
} from "./session-key.js";

/** Every flag combination consumers can configure (§4.4 two flags). */
const FLAG_MATRIX: readonly (readonly [string, IsolationFlags])[] = [
	["defaults", {}],
	["group-shared", { groupSessionsPerUser: false }],
	["threads-isolated", { threadSessionsPerUser: true }],
	[
		"both-flipped",
		{ groupSessionsPerUser: false, threadSessionsPerUser: true },
	],
];

describe("sessionKeyNamespace (gateway/session.py:_session_key_namespace)", () => {
	it("stays byte-identical `agent:main` for the default profile so positional parsers hold", () => {
		expect(sessionKeyNamespace()).toBe("agent:main");
		expect(sessionKeyNamespace(undefined)).toBe("agent:main");
		expect(sessionKeyNamespace("")).toBe("agent:main");
		expect(sessionKeyNamespace("default")).toBe("agent:main");

		for (const profile of [undefined, "default"] as const) {
			const parts = buildSessionKey(
				{ platform: "telegram", chatType: "group", chatId: "c1" },
				{},
				profile,
			).split(":");
			expect(parts[0]).toBe("agent");
			expect(parts[1]).toBe("main");
			// positional layout: parts[2] is the platform slot in every key ever
			// generated (compatibility freeze).
			expect(parts[2]).toBe("telegram");
		}
	});

	it("named profiles keep the same layout under their own namespace", () => {
		const key = buildSessionKey(
			{ platform: "slack", chatType: "group", chatId: "C1" },
			{},
			"acme",
		);
		expect(key.startsWith("agent:acme:slack:group:")).toBe(true);
		const parts = key.split(":");
		expect(parts[2]).toBe("slack");
	});
});

describe("isSharedMultiUserSession — the §4.4 policy table", () => {
	it("DMs are never shared under any flag combination", () => {
		for (const [, flags] of FLAG_MATRIX) {
			expect(isSharedMultiUserSession({ chatType: "dm" }, flags)).toBe(false);
			expect(
				isSharedMultiUserSession({ chatType: "dm", threadId: "t" }, flags),
			).toBe(false);
		}
	});

	it("non-thread group/channel sessions isolate per sender by default; flipping the flag shares them", () => {
		expect(isSharedMultiUserSession({ chatType: "group" })).toBe(false);
		expect(isSharedMultiUserSession({ chatType: "channel" })).toBe(false);

		expect(
			isSharedMultiUserSession(
				{ chatType: "group" },
				{ groupSessionsPerUser: false },
			),
		).toBe(true);
		expect(
			isSharedMultiUserSession(
				{ chatType: "channel" },
				{ groupSessionsPerUser: false },
			),
		).toBe(true);
	});

	it("thread-context sessions are shared unless thread_sessions_per_user", () => {
		for (const thread of [
			{ threadId: "t" },
			{ prospectiveThreadId: "t" }, // DEC-028: effective slot, not raw field
		]) {
			expect(isSharedMultiUserSession({ chatType: "thread", ...thread })).toBe(
				true,
			);
			expect(isSharedMultiUserSession({ chatType: "group", ...thread })).toBe(
				true,
			);
			expect(
				isSharedMultiUserSession(
					{ chatType: "thread", ...thread },
					{ threadSessionsPerUser: true },
				),
			).toBe(false);
			// thread flag does not leak into non-thread contexts…
			expect(
				isSharedMultiUserSession(
					{ chatType: "group" },
					{ threadSessionsPerUser: true },
				),
			).toBe(false);
		}
	});

	it("reads the EFFECTIVE thread slot: prospective thread ≡ real thread for the same id (DEC-028)", () => {
		for (const [, flags] of FLAG_MATRIX) {
			const initiator = { chatType: "channel", prospectiveThreadId: "42" };
			const followUp = { chatType: "thread", threadId: "42" };
			expect(isSharedMultiUserSession(initiator, flags)).toEqual(
				isSharedMultiUserSession(followUp, flags),
			);
		}
	});
});

describe("buildSessionKey — DM rules (§4 key-construction rules)", () => {
	it("chat_id isolates each private conversation; thread_id differentiates threaded DMs", () => {
		expect(
			buildSessionKey({
				platform: "telegram",
				chatType: "dm",
				chatId: "111",
			}),
		).toBe("agent:main:telegram:dm:111");
		expect(
			buildSessionKey({
				platform: "telegram",
				chatType: "dm",
				chatId: "111",
				threadId: "t9",
			}),
		).toBe("agent:main:telegram:dm:111:t9");
		expect(
			buildSessionKey({ platform: "telegram", chatType: "dm", chatId: "222" }),
		).not.toBe(
			buildSessionKey({ platform: "telegram", chatType: "dm", chatId: "111" }),
		);
	});

	it("without chat_id the sender identifier becomes the chat slot BEFORE the bare per-platform sink (no cross-user history bleed)", () => {
		const alice = buildSessionKey({
			platform: "telegram",
			chatType: "dm",
			userId: "alice",
		});
		const bob = buildSessionKey({
			platform: "telegram",
			chatType: "dm",
			userId: "bob",
		});
		expect(alice).toBe("agent:main:telegram:dm:alice");
		expect(bob).toBe("agent:main:telegram:dm:bob");
		expect(alice).not.toBe(bob);
	});

	it("without ANY identifier DMs share exactly one session per platform/chat_type (documented sink)", () => {
		const a = buildSessionKey({ platform: "telegram", chatType: "dm" });
		const b = buildSessionKey({
			platform: "telegram",
			chatType: "dm",
			chatName: "whatever",
		});
		expect(a).toBe("agent:main:telegram:dm");
		expect(a).toBe(b);
	});

	it("Slack workspace scope precedes chat/thread slots in DMs", () => {
		expect(
			buildSessionKey({
				platform: "slack",
				chatType: "dm",
				scopeId: "W1",
				chatId: "D1",
				threadId: "t1",
			}),
		).toBe("agent:main:slack:dm:W1:D1:t1");
	});
});

describe("buildSessionKey — group/channel/thread rules", () => {
	it("default flags isolate groups per sender: participant slot appended (user_id_alt preferred)", () => {
		const key = buildSessionKey({
			platform: "telegram",
			chatType: "group",
			userId: "u-plain",
			userIdAlt: "u-alt",
			chatId: "g1",
		});
		expect(key).toBe("agent:main:telegram:group:g1:u-alt");
	});

	it("flipping group_sessions_per_user shares ONE session per chat — chat_id retained, never a bare per-platform sink", () => {
		const shared = buildSessionKey(
			{
				platform: "telegram",
				chatType: "group",
				userId: "u1",
				chatId: "g1",
			},
			{ groupSessionsPerUser: false },
		);
		expect(shared).toBe("agent:main:telegram:group:g1");
		// a different sender lands on the SAME shared key
		expect(
			buildSessionKey(
				{ platform: "telegram", chatType: "group", userId: "u2", chatId: "g1" },
				{ groupSessionsPerUser: false },
			),
		).toBe(shared);
	});

	it("isolatable context without a participant identifier falls back to ONE shared session per chat", () => {
		const noSender = buildSessionKey({
			platform: "telegram",
			chatType: "group",
			chatId: "g1",
		});
		expect(noSender).toBe("agent:main:telegram:group:g1");
		expect(
			buildSessionKey(
				{ platform: "telegram", chatType: "group", chatId: "g1", userId: "u1" },
				{ groupSessionsPerUser: false },
			),
		).toBe(noSender);
	});

	it("threads share across participants by default; thread_sessions_per_user isolates", () => {
		const sharedThread = buildSessionKey({
			platform: "telegram",
			chatType: "thread",
			chatId: "g1",
			threadId: "t1",
			userId: "u1",
		});
		expect(sharedThread).toBe("agent:main:telegram:thread:g1:t1");
		expect(
			buildSessionKey(
				{
					platform: "telegram",
					chatType: "thread",
					chatId: "g1",
					threadId: "t1",
					userId: "u1",
				},
				{ threadSessionsPerUser: true },
			),
		).toBe("agent:main:telegram:thread:g1:t1:u1");
	});

	it("prospective-thread continuity: initiating channel message byte-matches the in-thread follow-ups", () => {
		const initiator = buildSessionKey({
			platform: "discord",
			chatType: "channel",
			chatId: "chan-7",
			userId: "u1",
			prospectiveThreadId: "msg-100",
		});
		const followUp = buildSessionKey({
			platform: "discord",
			chatType: "thread",
			chatId: "chan-7",
			userId: "u1",
			threadId: "msg-100",
		});
		expect(initiator).toBe(followUp);
		// chat_type slot rewritten to `thread` for the initiator
		expect(initiator).toBe("agent:main:discord:thread:chan-7:msg-100");
		// …and under isolation the initiator carries the participant slot too
		expect(
			buildSessionKey(
				{
					platform: "discord",
					chatType: "channel",
					chatId: "chan-7",
					userId: "u1",
					prospectiveThreadId: "msg-100",
				},
				{ threadSessionsPerUser: true },
			),
		).toBe(
			buildSessionKey(
				{
					platform: "discord",
					chatType: "thread",
					chatId: "chan-7",
					userId: "u1",
					threadId: "msg-100",
				},
				{ threadSessionsPerUser: true },
			),
		);
	});

	it("Slack prepends workspace scope before chat/thread/participant slots", () => {
		expect(
			buildSessionKey({
				platform: "slack",
				chatType: "group",
				scopeId: "W1",
				chatId: "G1",
				threadId: "T1",
				userId: "U1",
			}),
		).toBe("agent:main:slack:group:W1:G1:T1");
		// isolation off → participant drops, scope/chat stay
		expect(
			buildSessionKey(
				{
					platform: "slack",
					chatType: "group",
					scopeId: "W1",
					chatId: "G1",
					userId: "U1",
				},
				{ groupSessionsPerUser: false },
			),
		).toBe("agent:main:slack:group:W1:G1");
	});

	it("Discord guild scope is deliberately NOT added (compatibility freeze): scopeId on non-Slack platforms is ignored", () => {
		expect(
			buildSessionKey({
				platform: "discord",
				chatType: "group",
				scopeId: "guild-99",
				chatId: "chan-1",
			}),
		).toBe("agent:main:discord:group:chan-1");
	});
});

describe("ONE-predicate invariant (§4.4): participant slot ⇔ predicate says isolated ∧ identifier exists", () => {
	const PARTICIPANT = "sender-1";

	function keyWithParticipant(source: SessionSource, flags: IsolationFlags) {
		return buildSessionKey({ ...source, userId: PARTICIPANT }, flags);
	}

	function keyWithoutParticipant(source: SessionSource, flags: IsolationFlags) {
		return buildSessionKey({ ...source }, flags);
	}

	const CONTEXTS: readonly (readonly [string, SessionSource])[] = [
		["plain group", { platform: "telegram", chatType: "group", chatId: "g" }],
		["channel", { platform: "telegram", chatType: "channel", chatId: "c" }],
		[
			"real thread",
			{
				platform: "telegram",
				chatType: "thread",
				chatId: "g",
				threadId: "t",
			},
		],
		[
			"prospective thread (auto-thread initiator)",
			{
				platform: "discord",
				chatType: "channel",
				chatId: "c",
				prospectiveThreadId: "m1",
			},
		],
	];

	for (const [contextName, source] of CONTEXTS) {
		for (const [flagName, flags] of FLAG_MATRIX) {
			it(`${contextName} × ${flagName}: key construction agrees with isSharedMultiUserSession`, () => {
				const sourceWithSender: SessionSource = {
					...source,
					userId: PARTICIPANT,
				};
				const shared = isSharedMultiUserSession(sourceWithSender, flags);
				const withP = keyWithParticipant(source, flags);
				const withoutP = keyWithoutParticipant(source, flags);

				if (shared) {
					// shared: the participant identifier must NOT change the key
					expect(withP).toBe(withoutP);
				} else {
					// isolated: the slot is appended and is the LAST slot; dropping
					// the identifier falls back to the shared per-chat key
					expect(withP).toBe(`${withoutP}:${PARTICIPANT}`);
				}
				// THE invariant under test: one predicate drives both decisions.
				expect(withP !== withoutP).toBe(!shared);
			});
		}
	}

	it("DM keys are unaffected by the predicate sweep (chat slot IS the isolation)", () => {
		for (const [, flags] of FLAG_MATRIX) {
			const source: SessionSource = {
				platform: "telegram",
				chatType: "dm",
				chatId: "d1",
			};
			expect(keyWithParticipant(source, flags)).toBe(
				keyWithoutParticipant(source, flags),
			);
			expect(isSharedMultiUserSession(source, flags)).toBe(false);
		}
	});
});
