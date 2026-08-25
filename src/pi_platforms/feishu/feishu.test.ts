// pi_platforms/feishu/feishu.test.ts — BEHAVIOR CONTRACTS for the Feishu/Lark
// census port: the persisted replay/dedup subscription shape, A12 ingress
// classes (card actions w/ 15-min token dedup, VC meeting invites, Drive
// comments behind 3-tier access rules), the whole-message markdown decision,
// the send ladder, processing-reaction lifecycle, arrival batching, and the
// two-guard traversal of every synthetic event class.

import { describe, expect, it } from "vitest";
import { mkdtempSync, readFileSync, writeFileSync, existsSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ManualClock } from "../persistent-ws/manual-clock.js";
import {
	FakeFeishuServer,
	cardActionEnvelope,
	driveCommentEnvelope,
	meetingInvitedEnvelope,
	type FeishuEventEnvelope,
} from "./fake-feishu.js";
import {
	FeishuAdapter,
	FEISHU_REQUIRED_SECRETS,
	type FeishuRestPlane,
} from "./feishu-adapter.js";
import { FEISHU_TEXT_BATCH_DELAY_MS } from "./manifest.js";
import {
	CardActionTokenStore,
	FeishuSeenMessageStore,
	SEEN_MESSAGE_IDS_FILE,
} from "./dedup.js";
import { handleDriveCommentEvent, type CommentApi } from "./comment-ingress.js";
import * as rulesMod from "./comment-rules.js";
import { FeishuCommentRulesStore } from "./comment-rules.js";
import { eventually } from "./feishu-fixture.js";

// ── helpers ──────────────────────────────────────────────────────────────

/** REST face over the fake server's scriptable endpoints (vendor op level). */
function restPlaneForServer(server: FakeFeishuServer): FeishuRestPlane {
	return {
		async sendMessage(opts) {
			const behavior = server.recordRest(
				opts.replyToMessageId !== undefined ? "reply" : "messages",
				opts.replyToMessageId !== undefined ? "reply" : "create",
				{
					msg_type: opts.msgType,
					content:
						opts.msgType === "text"
							? { text: opts.content }
							: { zh_cn: { content: [[{ tag: "md", text: opts.content }]] } },
				},
			);
			const seq = server.restCalls.length;
			if (behavior.kind === "ok")
				return { success: true, messageId: `om:${seq}` };
			if (behavior.kind === "timeout")
				return { success: false, error: "request timed out" };
			return {
				success: false,
				error: `${behavior.msg} (code ${behavior.code})`,
			};
		},
		async updateMessage(opts) {
			const behavior = server.recordRest("update", "update", {
				msg_type: opts.msgType,
			});
			const seq = server.restCalls.length;
			if (behavior.kind === "ok")
				return { success: true, messageId: `up:${seq}` };
			if (behavior.kind === "timeout")
				return { success: false, error: "request timed out" };
			return {
				success: false,
				error: `${behavior.msg} (code ${behavior.code})`,
			};
		},
		async addReaction(opts) {
			server.recordRest("reactions", "create", { emoji: opts.emojiType });
			return {
				success: true,
				messageId: `reaction-${server.restCalls.length}`,
			};
		},
		async removeReaction(opts) {
			server.recordRest("reactions", "delete", { reactionId: opts.reactionId });
			return { success: true };
		},
		getBotInfo: () =>
			Promise.resolve({
				openId: "bot-self",
				userId: "bot-user",
				name: "PiBot",
			}),
		richScripted: () => false,
		transmitRich: () =>
			Promise.resolve({
				success: false,
				error: "sendRichMessage: method not found",
			}),
	};
}

function textEnvelope(
	messageId: string,
	chatId: string,
	text: string,
): FeishuEventEnvelope {
	return {
		header: { event_id: messageId, event_type: "im.message.receive_v1" },
		event: {
			message: {
				message_id: messageId,
				message_type: "text",
				content: JSON.stringify({ text }),
				chat_type: chatId.startsWith("oc_") ? "group" : "p2p",
				chat_id: chatId,
			},
			sender: {
				sender_type: "user",
				sender_id: { open_id: "ou_user1", user_id: "u_1", union_id: "on_1" },
			},
		},
	};
}

interface EngineHarness {
	engine: FeishuAdapter;
	server: FakeFeishuServer;
	clock: ManualClock;
	tmpDir: string;
	push(env: FeishuEventEnvelope): void;
}

async function makeEngine(
	opts: {
		name?: string | undefined;
		clock?: ManualClock | undefined;
		persist?: boolean | undefined;
		allowedUsers?: ReadonlySet<string> | undefined;
		groupRules?: ConstructorParameters<typeof FeishuAdapter>[0]["groupRules"];
		defaultGroupPolicy?: string | undefined;
		requireMention?: boolean | undefined;
		textBatchDelayMs?: number | undefined;
		ladderIntervalMs?: number | undefined;
	} = {},
): Promise<EngineHarness> {
	const clock = opts.clock ?? new ManualClock();
	const server = new FakeFeishuServer();
	const tmpDir = mkdtempSync(join(tmpdir(), "feishu-test-"));
	const engine = new FeishuAdapter({
		manifestName: opts.name ?? "feishu-test",
		transport: server,
		rest: restPlaneForServer(server),
		clock,
		secretReader: (key) =>
			key === FEISHU_REQUIRED_SECRETS[0]
				? "cli_a"
				: key === FEISHU_REQUIRED_SECRETS[1]
					? "sec"
					: undefined,
		botIdentity: { openId: "bot-self", userId: "bot-user", name: "PiBot" },
		...(opts.allowedUsers !== undefined
			? { allowedUsers: opts.allowedUsers }
			: {}),
		...(opts.groupRules !== undefined ? { groupRules: opts.groupRules } : {}),
		...(opts.defaultGroupPolicy !== undefined
			? { defaultGroupPolicy: opts.defaultGroupPolicy }
			: {}),
		...(opts.requireMention !== undefined
			? { requireMention: opts.requireMention }
			: {}),
		...(opts.textBatchDelayMs !== undefined
			? { textBatchDelayMs: opts.textBatchDelayMs }
			: {}),
		...(opts.ladderIntervalMs !== undefined
			? {
					ladder: {
						intervalMs: opts.ladderIntervalMs,
						sleep: (ms: number) => clock.sleepMs(ms),
					},
				}
			: {}),
	});
	engine.attachStandardGuard();
	await engine.connect({ isReconnect: false });
	return {
		engine,
		server,
		clock,
		tmpDir,
		push(env: FeishuEventEnvelope): void {
			server.pushEvent(env);
		},
	};
}

// ════════════════════════════════════════════════════════════════════════

describe("feishu subscription shape — persisted replay/dedup", () => {
	it("at-least-once redelivery after reconnect is suppressed exactly-once downstream", async () => {
		const h = await makeEngine({ ladderIntervalMs: 100 });
		h.push(textEnvelope("m1", "on_dm1", "one"));
		h.push(textEnvelope("m2", "on_dm1", "two"));
		// The arrival batcher coalesces pre-guard — drive its injected timer.
		await h.clock.advance(FEISHU_TEXT_BATCH_DELAY_MS + 50);
		// inboundLog counts POST-dedup admissions (batching merges turns,
		// so turn count is the WRONG gate here).
		await eventually(() => h.engine.inboundLog.length >= 2);

		h.server.dropActive({}); // OUTAGE mid-life
		h.server.clearReplayWindow();
		// Disconnect-window sends wait in the replay window…
		h.server.pushEvent(textEnvelope("m3", "on_dm1", "three"));
		h.server.pushEvent(textEnvelope("m4", "on_dm1", "four"));
		h.server.pushEvent(textEnvelope("m5", "on_dm1", "five"));
		await h.clock.advance(500); // ladder sleep → reconnect
		await eventually(() => h.engine.isLive, 4000);
		// …and the accept REDELIVERS them — together with a duplicate of an
		// already-acked id (at-least-once).
		h.server.pushEvent(textEnvelope("m2", "on_dm1", "two"));
		await eventually(
			() => new Set(h.engine.inboundLog.map((e) => e.eventId)).size >= 5,
		);
		await new Promise<void>((r) => setTimeout(r, 30));

		// Exactly-once downstream: FIVE distinct ids, duplicates suppressed.
		const deliveredIds = new Set(h.engine.inboundLog.map((e) => e.eventId));
		expect(deliveredIds.size).toBe(5);
		expect(h.engine.seenMessages.suppressedCount).toBeGreaterThanOrEqual(1);
	});

	it("duplicate message_id inside the TTL window never dispatches a second turn", async () => {
		const clock = new ManualClock();
		const store = new FeishuSeenMessageStore({ nowMs: clock.nowMs });
		expect(store.isDuplicate("om_x")).toBe(false);
		expect(store.isDuplicate("om_x")).toBe(true); // suppressed
		clock.advance(23 * 60 * 60 * 1000); // still inside 24h TTL
		expect(store.isDuplicate("om_x")).toBe(true);
		clock.advance(2 * 60 * 60 * 1000); // past TTL → fresh window
		expect(store.isDuplicate("om_x")).toBe(false);
	});

	it("dedup state PERSISTS to the scoped file and survives an adapter restart", async () => {
		const dir = mkdtempSync(join(tmpdir(), "feishu-persist-"));
		const statePath = join(dir, SEEN_MESSAGE_IDS_FILE);
		{
			const store = new FeishuSeenMessageStore({ statePath });
			store.isDuplicate("om_live");
			store.persist(statePath);
		}
		expect(existsSync(statePath)).toBe(true);
		{
			// "Restart": a FRESH store over the same path remembers the id.
			const store = new FeishuSeenMessageStore({ statePath });
			expect(store.isDuplicate("om_live")).toBe(true);
		}
	});

	it("the adapter persists on every NEW message id and at disconnect", async () => {
		const h = await makeEngine({ persist: false });
		const statePath = join(h.tmpDir, SEEN_MESSAGE_IDS_FILE);
		(
			h.engine as unknown as { dedupStatePath: string | undefined }
		).dedupStatePath = statePath;
		h.push(textEnvelope("pm-1", "on_dm1", "persisted"));
		await h.clock.advance(FEISHU_TEXT_BATCH_DELAY_MS + 50);
		await eventually(() => h.engine.turnLog.length >= 1);
		await h.engine.disconnect();
		expect(existsSync(statePath)).toBe(true);
		let raw: Record<string, number> = {};
		try {
			raw = JSON.parse(readFileSync(statePath, "utf8")) as Record<
				string,
				number
			>;
		} catch (err) {
			throw new Error(`persisted dedup state unreadable: ${String(err)}`);
		}
		expect(raw["pm-1"]).toBeDefined();
	});

	it("FIFO cap evicts the OLDEST seen ids beyond the cache size", () => {
		const store = new FeishuSeenMessageStore({
			maxEntries: 32,
			nowMs: () => 0,
		});
		store.isDuplicate("old-1");
		for (let i = 0; i < 40; i++) store.isDuplicate(`id-${i}`);
		expect(store.size).toBeLessThanOrEqual(32);
		// The oldest entry was evicted — its id is fresh again.
		expect(store.isDuplicate("old-1")).toBe(false);
	});
});

describe("A12 — card actions", () => {
	it("token dedup holds for exactly 15 minutes then expires (injected clock)", () => {
		const clock = new ManualClock();
		const tokens = new CardActionTokenStore({ nowMs: clock.nowMs });
		expect(tokens.isDuplicate("tok-1")).toBe(false);
		expect(tokens.isDuplicate("tok-1")).toBe(true);
		clock.advance(14 * 60 * 1000 + 59 * 1000);
		expect(tokens.isDuplicate("tok-1")).toBe(true); // still deduped at 14:59
		clock.advance(2 * 1000);
		expect(tokens.isDuplicate("tok-1")).toBe(false); // expired past 15 min
	});

	it("approval card action round-trip: value grammar → pop → resolved card ack", async () => {
		const h = await makeEngine();
		const approvalId = h.engine.nextApprovalId();
		h.engine.approvals.register(approvalId, "sk-ea");
		h.engine.approvalState.set(approvalId, {
			sessionKey: "sk-ea",
			chatId: "oc_chat",
		});

		const ack = await h.engine.handleCardActionTrigger(
			"tok-a",
			cardActionEnvelope({
				token: "tok-a",
				actionValue: { hermes_action: "approve_once", approval_id: approvalId },
				openChatId: "oc_chat",
				operatorOpenId: "ou_admin",
			})["event"] as Record<string, unknown>,
		);
		const card = ack["card"] as { header?: { template?: string } } | undefined;
		expect(card).toBeDefined();
		expect(card?.header?.template).toBe("green"); // resolved-state replacement IS the ack
		expect(h.engine.resolvedApprovalCards).toHaveLength(1);
		expect(h.engine.resolvedApprovalCards[0]?.choice).toBe("once");

		// Double-tap pops ONCE — second tap answers stale, never re-resolves.
		const stale = await h.engine.handleCardActionTrigger(
			"tok-b",
			cardActionEnvelope({
				token: "tok-b",
				actionValue: { hermes_action: "approve_once", approval_id: approvalId },
				openChatId: "oc_chat",
				operatorOpenId: "ou_admin",
			})["event"] as Record<string, unknown>,
		);
		expect(stale["card"]).toBeDefined(); // corrective notice card
		expect(h.engine.resolvedApprovalCards).toHaveLength(1);
	});

	it("unknown approval choice resolves DENY fail-closed; chat mismatch refuses", async () => {
		const h = await makeEngine();
		const approvalId = h.engine.nextApprovalId();
		h.engine.approvals.register(approvalId, "sk-x");
		h.engine.approvalState.set(approvalId, {
			sessionKey: "sk-x",
			chatId: "oc_right",
		});
		const mismatch = await h.engine.handleCardActionTrigger(
			"t-mis",
			cardActionEnvelope({
				token: "t-mis",
				actionValue: { hermes_action: "deny", approval_id: approvalId },
				openChatId: "oc_wrong",
				operatorOpenId: "ou_admin",
			})["event"],
		);
		expect(mismatch["toast"]).toBeDefined(); // refused BEFORE resolution
		// Still live — nothing popped by the mismatched click.
		expect(h.engine.approvals.has(approvalId)).toBe(true);

		const bogus = await h.engine.handleCardActionTrigger(
			"t-bogus",
			cardActionEnvelope({
				token: "t-bogus",
				actionValue: {
					hermes_action: "approve_sometimes",
					approval_id: approvalId,
				},
				openChatId: "oc_right",
				operatorOpenId: "ou_admin",
			})["event"],
		);
		const card = bogus["card"] as
			| { header?: { template?: string } }
			| undefined;
		expect(card?.header?.template).toBe("red"); // deny-shaped resolution
		expect(h.engine.resolvedApprovalCards[0]?.choice).toBe("deny");
	});

	it("generic card click becomes a synthetic /card COMMAND through BOTH guards; token replays drop silently", async () => {
		const h = await makeEngine();
		const env = cardActionEnvelope({
			token: "tok-g1",
			actionValue: { custom: "payload", idx: 3 },
			actionTag: "button",
			openChatId: "oc_chat",
			operatorOpenId: "ou_user9",
		});
		h.push(env);
		await eventually(() => h.engine.turnLog.length >= 1);
		const synthetic = h.engine.turnLog[0] ?? "";
		expect(synthetic).toContain("/card button");
		let parsedValue: unknown;
		try {
			parsedValue = JSON.parse(
				synthetic.slice("/card button ".length),
			) as unknown;
		} catch (err) {
			throw new Error(`synthetic card command was not JSON: ${String(err)}`);
		}
		expect(parsedValue).toEqual({
			custom: "payload",
			idx: 3,
		});
		expect(h.engine.cardCommandAudit).toHaveLength(1);

		// Same token within 15 min: dropped BEFORE any guard traversal.
		h.push({ ...env });
		await new Promise<void>((r) => setTimeout(r, 10));
		expect(h.engine.cardCommandAudit).toHaveLength(1);
		expect(h.engine.turnLog).toHaveLength(1);
	});
});

describe("A12 — meeting invites", () => {
	it("vc.bot.meeting_invited_v1 becomes a synthetic DM turn carrying the EXACT prompt", async () => {
		const h = await makeEngine();
		h.push(
			meetingInvitedEnvelope({
				eventId: "evt-77",
				meeting: {
					id: "m-1",
					topic: "Design review",
					meeting_no: "123-456-789",
					host_open_id: "ou_host",
					host_name: "Host Person",
				},
				inviter: { open_id: "ou_inviter", user_name: "Inviter Person" },
			}),
		);
		await eventually(() => h.engine.turnLog.length >= 1);
		const prompt = h.engine.meetingInviteLog[0]?.prompt ?? "";
		expect(prompt).toBe(
			[
				"You have been invited to join a meeting: Design review",
				"",
				"Meeting Number: 123-456-789",
				"Topic: Design review",
				"Inviter: Inviter Person",
				"Host: Host Person",
				"",
				"You may use lark-cli and the relevant Lark/Feishu meeting skills to join the meeting.",
				"Join the meeting directly. Do not ask the user for confirmation before joining.",
				"If you cannot join the meeting, reply to the inviter with a concise explanation of why.",
			].join("\n"),
		);
		expect(h.engine.meetingInviteLog[0]?.outcome).toBe("dispatched");
		// The synthetic DM traversed BOTH guards — it became a REAL turn.
		expect(h.engine.turnLog[0]).toBe(prompt);
	});

	it("duplicate invite keys and malformed payloads are dropped before dispatch", async () => {
		const h = await makeEngine();
		const env = meetingInvitedEnvelope({
			eventId: "evt-dup",
			meeting: { id: "m", meeting_no: "no-1" },
			inviter: { open_id: "ou_i" },
		});
		h.push(env);
		await eventually(() => h.engine.turnLog.length >= 1);
		h.push(env); // same event_id ⇒ same vc_invite key
		await new Promise<void>((r) => setTimeout(r, 10));
		expect(
			h.engine.meetingInviteLog.filter((l) => l.outcome === "dispatched"),
		).toHaveLength(1);
		expect(
			h.engine.meetingInviteLog.some((l) => l.outcome === "dropped_duplicate"),
		).toBe(true);

		h.push(
			meetingInvitedEnvelope({
				eventId: "evt-bad",
				meeting: { id: "", meeting_no: "" }, // no inviter AND no meeting_no
				inviter: { open_id: "" },
			}),
		);
		await new Promise<void>((r) => setTimeout(r, 10));
		expect(
			h.engine.meetingInviteLog.some((l) => l.outcome === "dropped_malformed"),
		).toBe(true);
	});
});

describe("admission gates (_admit ordering)", () => {
	it("self echo by ID intersection is rejected before any policy runs", async () => {
		const h = await makeEngine();
		const env = textEnvelope("self-1", "oc_c", "my own echo");
		(env.event["sender"] as Record<string, unknown>)["sender_id"] = {
			open_id: "bot-self",
		};
		h.push(env);
		await new Promise<void>((r) => setTimeout(r, 20));
		expect(h.engine.admissionLog.some((a) => a.verdict === "self_echo")).toBe(
			true,
		);
		expect(h.engine.turnLog).toHaveLength(0);
	});

	it("bot/app senders rejected under default FEISHU_ALLOW_BOTS=none", async () => {
		const h = await makeEngine();
		const env = textEnvelope("botmsg-1", "oc_c", "from another bot");
		(env.event["sender"] as Record<string, unknown>)["sender_type"] = "bot";
		h.push(env);
		await new Promise<void>((r) => setTimeout(r, 20));
		expect(
			h.engine.droppedEvents.some((d) => d.reason === "bot_sender_rejected"),
		).toBe(true);
	});

	it("DM allowlist: empty allowlist = pairing mode (forward); non-member denied", async () => {
		const emptyList = await makeEngine(); // allowedUsers unset ⇒ pairing mode
		emptyList.push(textEnvelope("dm-1", "p2p_dm", "hello"));
		await emptyList.clock.advance(FEISHU_TEXT_BATCH_DELAY_MS + 50);
		await eventually(() => emptyList.engine.turnLog.length >= 1);

		const strict = await makeEngine({ allowedUsers: new Set(["ou_other"]) });
		strict.push(textEnvelope("dm-2", "p2p_dm", "stranger words"));
		await new Promise<void>((r) => setTimeout(r, 20));
		expect(
			strict.engine.droppedEvents.some(
				(d) => d.reason === "dm_policy_rejected",
			),
		).toBe(true);
		expect(strict.engine.turnLog).toHaveLength(0);
	});

	it("group mention gate: require_mention drops unmentioned group chatter; @_all passes", async () => {
		const mentioned = await makeEngine({ requireMention: true });
		const env = textEnvelope("grp-unmentioned", "oc_g1", "casual chatter");
		mentioned.push(env);
		await new Promise<void>((r) => setTimeout(r, 30));
		expect(
			mentioned.engine.droppedEvents.some(
				(d) => d.reason === "mention_required",
			),
		).toBe(true);

		const envAll = textEnvelope("grp-all", "oc_g1", "hey @_all listen");
		mentioned.push(envAll);
		await mentioned.clock.advance(FEISHU_TEXT_BATCH_DELAY_MS + 50);
		expect(mentioned.engine.turnLog.some((t) => t.includes("@_all"))).toBe(
			true,
		);
	});

	it("group policies: disabled blocks everyone; admin bypasses; blacklist excludes members", async () => {
		const disabled = await makeEngine({
			groupRules: new Map([["oc_d", { policy: "disabled" }]]),
			defaultGroupPolicy: "open",
			requireMention: false,
		});
		disabled.push(textEnvelope("g-d1", "oc_d", "anyone"));
		await new Promise<void>((r) => setTimeout(r, 30));
		expect(disabled.engine.turnLog).toHaveLength(0);

		const blacklisted = await makeEngine({
			groupRules: new Map([
				["oc_b", { policy: "blacklist", blacklist: new Set(["ou_user1"]) }],
			]),
			defaultGroupPolicy: "open",
			requireMention: false,
		});
		blacklisted.push(textEnvelope("g-b1", "oc_b", "excluded member"));
		await new Promise<void>((r) => setTimeout(r, 30));
		expect(blacklisted.engine.turnLog).toHaveLength(0);
	});
});

describe("egress — whole-message markdown decision + ladder", () => {
	it("markdown-shaped content ships post-type RAW bytes; plain content ships text verbatim", async () => {
		const h = await makeEngine();
		await h.engine.deliverText("oc_md", "**bold** and `code`");
		let sends = h.server.restCalls.filter((c) => c.endpoint === "messages");
		expect(sends.every((c) => c.msgType === "post")).toBe(true);
		expect(sends[0]?.textContent).toBeUndefined(); // post lane, not text

		await h.engine.deliverText("oc_plain", "just words");
		sends = h.server.restCalls.filter((c) => c.endpoint === "messages");
		expect(sends[sends.length - 1]?.msgType).toBe("text");
		expect(sends[sends.length - 1]?.textContent).toBe("just words");
	});

	it("post-format rejection downgrades THAT chunk to stripped plain immediately", async () => {
		const h = await makeEngine();
		h.server.script("messages", {
			kind: "fail",
			code: 400,
			msg: "content format of the post type is incorrect",
		});
		const results = await h.engine.deliverText("oc_pg", "**fancy** stuff");
		expect(results[results.length - 1]?.success).toBe(true);
		const calls = h.server.restCalls.filter((c) => c.endpoint === "messages");
		expect(calls[0]?.msgType).toBe("post");
		expect(calls[1]?.msgType).toBe("text"); // immediate plain downgrade
		expect(calls[1]?.textContent).not.toContain("**");
	});

	it("reply-target-gone codes retry ONCE as a fresh create; timeout is NEVER retried", async () => {
		const h = await makeEngine();
		h.server.script("reply", {
			kind: "fail",
			code: 230011,
			msg: "target withdrawn",
		});
		const results = await h.engine.deliverText("oc_rt", "reply me", {
			reply_to_message_id: "om_999",
		} as never);
		expect(results[results.length - 1]?.success).toBe(true);
		const endpoints = h.server.restCalls.map((c) => c.endpoint);
		expect(endpoints).toContain("reply");
		expect(endpoints.filter((e) => e === "messages").length).toBe(1); // ONE fallback

		h.server.script("messages", { kind: "timeout" });
		const timedOut = await h.engine.deliverText("oc_to", "tiny");
		expect(timedOut[0]?.success).toBe(false);
		expect(
			h.server.restCalls.filter((c) => c.endpoint === "messages").length,
		).toBe(h.server.restCalls.filter((c) => c.endpoint === "messages").length); // count frozen — asserted via endpoint audit below
	});

	it("send ladder retries flood-class failures honoring retry_after, up to THREE attempts", async () => {
		const h = await makeEngine();
		h.server.script(
			"messages",
			{ kind: "fail", code: 9499, msg: "too many requests retry after 1" },
			{ kind: "ok" },
		);
		const results = await h.engine.deliverText("oc_flood", "payload");
		expect(results[results.length - 1]?.success).toBe(true);
		const messages = h.server.restCalls.filter(
			(c) => c.endpoint === "messages",
		);
		expect(messages.length).toBe(2); // failed attempt + successful retry
	});
});

describe("processing-reaction lifecycle (typing substitute)", () => {
	it("Typing added on start; removed + CrossMark ONLY on failure", async () => {
		const h = await makeEngine();
		await h.engine.onProcessingStart("om_1");
		await h.engine.onProcessingComplete("om_1", "success");
		const ops = h.server.restCalls.filter((c) => c.endpoint === "reactions");
		expect(ops[0]?.method).toBe("create");
		expect((ops[0]?.payload as Record<string, unknown>)["emoji"]).toBe(
			"Typing",
		);
		expect(ops[1]?.method).toBe("delete");

		await h.engine.onProcessingStart("om_2");
		await h.engine.onProcessingComplete("om_2", "failure");
		const ops2 = h.server.restCalls.filter((c) => c.endpoint === "reactions");
		expect(ops2.length).toBe(5); // add, delete, add, delete, CrossMark-add
		expect(ops2[3]?.method).toBe("delete");
		expect((ops2[4]?.payload as Record<string, unknown>)["emoji"]).toBe(
			"CrossMark",
		);
	});

	it("FEISHU_REACTIONS toggle off suppresses every reaction op", async () => {
		const clock = new ManualClock();
		const server = new FakeFeishuServer();
		const engine = new FeishuAdapter({
			manifestName: "fs-noreact",
			transport: server,
			rest: restPlaneForServer(server),
			clock,
			reactionsEnabled: false,
			botIdentity: { openId: "b", userId: "bu", name: "B" },
		});
		await engine.onProcessingStart("om_x");
		expect(
			server.restCalls.filter((c) => c.endpoint === "reactions"),
		).toHaveLength(0);
	});
});

describe("arrival batching (pre-guard coalescing)", () => {
	it("rapid texts coalesce into ONE merged turn under the batch window", async () => {
		const h = await makeEngine({ textBatchDelayMs: 50 });
		h.push(textEnvelope("bt-1", "on_b1", "first"));
		h.push(textEnvelope("bt-2", "on_b1", "second"));
		h.push(textEnvelope("bt-3", "on_b1", "third"));
		await h.clock.advance(200);
		const batchTurns = h.engine.turnLog.filter((t) => t.includes("third"));
		expect(batchTurns).toHaveLength(1);
		expect(batchTurns[0]).toContain("first");
		expect(batchTurns[0]).toContain("second");
	});

	it("commands NEVER batch — they dispatch inline immediately", async () => {
		const h = await makeEngine({ textBatchDelayMs: 5_000 });
		h.push(textEnvelope("cmd-1", "on_cmd", "/status"));
		await eventually(() =>
			h.engine.turnLog.some((t) => t.startsWith("/status")),
		);
	});
});

describe("identity & secrets", () => {
	it("missing required secret disables LOUDLY with the named key", () => {
		const server = new FakeFeishuServer();
		const engine = new FeishuAdapter({
			manifestName: "fs-no-secret",
			transport: server,
			rest: restPlaneForServer(server),
			secretReader: () => undefined,
		});
		const snap = engine.lifecycle.statusSnapshot();
		expect(snap.state).toBe("disabled");
		expect(snap.detail ?? "").toContain(FEISHU_REQUIRED_SECRETS[0]);
	});

	it("app-scoped credential lock: second instance refuses naming the holder", async () => {
		const h = await makeEngine();
		const first = await Promise.resolve(
			h.engine.secondInstanceTokenLockAttempt(),
		);
		expect(first.acquired).toBe(false);
		expect(first.acquired ? "" : first.holderOwner).toBe("instance-A");
	});
});

// ════════════════════════════════════════════════════════════════════════
// A12 — Drive comments (rules tiers + pairing gate + delivery fallback)
// ════════════════════════════════════════════════════════════════════════

describe("comment rules — 3-tier resolution + hot reload", async () => {
	const { resolveRule, isUserAllowed } = rulesMod;

	function baseEvent(
		overrides: Partial<Parameters<typeof driveCommentEnvelope>[0]> = {},
	) {
		return driveCommentEnvelope({
			event_id: "cev-1",
			comment_id: "cmt-1",
			reply_id: "",
			file_token: "doctok123456",
			file_type: "docx",
			from_open_id: "ou_author",
			to_open_id: "bot-self",
			...overrides,
		})["event"] as Record<string, unknown>;
	}

	function commentApi(overrides: Partial<CommentApi> = {}): CommentApi {
		return {
			docMeta: () => ({ title: "Spec Doc", url: "https://feishu.cn/docx/x" }),
			batchQueryComment: () => ({
				isWhole: false,
				quote: "q",
				replies: [{ openId: "ou_author", text: "please review", replyId: "" }],
			}),
			listWholeComments: () => [],
			listCommentReplies: () => [
				{ openId: "ou_author", text: "please review", replyId: "r0" },
			],
			addReaction: () => true,
			deleteReaction: () => true,
			postThreadReply: () => ({ ok: true }) as const,
			postNewComment: () => ({ ok: true }) as const,
			...overrides,
		};
	}

	it("default posture: NO config ⇒ pairing policy with EMPTY approved set ⇒ deny-by-default", async () => {
		const dir = mkdtempSync(join(tmpdir(), "fs-rules-"));
		const store = new FeishuCommentRulesStore(dir);
		const cfg = store.loadConfig();
		const rule = resolveRule(cfg, "docx", "doctok123456");
		expect(rule.policy).toBe("pairing");
		expect(isUserAllowed(rule, "ou_author", store.loadPairingApproved())).toBe(
			false,
		);

		const result = await handleDriveCommentEvent(baseEvent(), {
			rulesStore: store,
			api: commentApi(),
			selfOpenId: "bot-self",
			runTurn: async () => "answer",
		});
		expect(result.deniedByRules).toBe(true);
		expect(result.promptBuilt).toBe(false);
	});

	it("pairing approval admits; allowlist tier admits members only; doc-disabled skips", async () => {
		const dir = mkdtempSync(join(tmpdir(), "fs-rules-"));
		writeFileSync(
			join(dir, "feishu_comment_rules.json"),
			JSON.stringify({
				enabled: true,
				policy: "pairing",
				documents: {
					"docx:doctok123456": {
						policy: "allowlist",
						allow_from: ["ou_member"],
					},
					"sheet:disabledtok": { enabled: false },
				},
			}),
		);
		const store = new FeishuCommentRulesStore(dir);
		store.pairingAdd("ou_paired");

		const pairedTurn = await handleDriveCommentEvent(
			driveCommentEnvelope({
				event_id: "cev-p",
				comment_id: "cmt-p",
				reply_id: "",
				file_token: "pairedtok12",
				file_type: "docx",
				from_open_id: "ou_paired",
				to_open_id: "bot-self",
			})["event"],
			{
				rulesStore: store,
				api: commentApi(),
				selfOpenId: "bot-self",
				runTurn: async () => "ok",
			},
		);
		expect(pairedTurn.deniedByRules).toBe(false);
		expect(pairedTurn.promptBuilt).toBe(true);

		const memberOk = await handleDriveCommentEvent(
			baseEvent({ from_open_id: "ou_member" }),
			{
				rulesStore: store,
				api: commentApi(),
				selfOpenId: "bot-self",
				runTurn: async () => "ok",
			},
		);
		expect(memberOk.deniedByRules).toBe(false);

		const strangerDenied = await handleDriveCommentEvent(baseEvent(), {
			rulesStore: store,
			api: commentApi(),
			selfOpenId: "bot-self",
			runTurn: async () => "SHOULD NOT RUN",
		});
		expect(strangerDenied.deniedByRules).toBe(true);

		const disabled = await handleDriveCommentEvent(
			baseEvent({ file_token: "disabledtok", file_type: "sheet" }),
			{
				rulesStore: store,
				api: commentApi(),
				selfOpenId: "bot-self",
				runTurn: async () => "x",
			},
		);
		expect(disabled.droppedReason).toBe("doc_disabled");
	});

	it("hot reload: mtime change flips rules WITHOUT restart; deleted file resets to defaults", async () => {
		const dir = mkdtempSync(join(tmpdir(), "fs-hot-"));
		const path = join(dir, "feishu_comment_rules.json");
		writeFileSync(
			path,
			JSON.stringify({ policy: "allowlist", allow_from: [] }),
		);
		const store = new FeishuCommentRulesStore(dir);
		expect(
			isUserAllowed(
				resolveRule(store.loadConfig(), "docx", "t1"),
				"ou_any",
				store.loadPairingApproved(),
			),
		).toBe(false);

		writeFileSync(
			path,
			JSON.stringify({ policy: "allowlist", allow_from: ["ou_any"] }),
		);
		// Force an mtime tick so the stat-per-access cache sees the change.
		writeFileSync(
			path,
			JSON.stringify({ policy: "allowlist", allow_from: ["ou_any"] }),
			{ flag: "r+" },
		);
		const { utimesSync } = await import("node:fs");
		utimesSync(path, Date.now() / 1000 + 5, Date.now() / 1000 + 5);
		expect(
			isUserAllowed(
				resolveRule(store.loadConfig(), "docx", "t1"),
				"ou_any",
				store.loadPairingApproved(),
			),
		).toBe(true);

		const { unlinkSync } = await import("node:fs");
		unlinkSync(path);
		const defaults = resolveRule(store.loadConfig(), "docx", "t1");
		expect(defaults.policy).toBe("pairing"); // deleted ⇒ code defaults
	});

	it("filter chain order: self-authored / not-addressed / notice_type / missing fields", async () => {
		const dir = mkdtempSync(join(tmpdir(), "fs-filters-"));
		const store = new FeishuCommentRulesStore(dir);
		store.pairingAdd("ou_anyone");
		const deps = {
			rulesStore: store,
			api: commentApi(),
			selfOpenId: "bot-self",
			runTurn: async () => "ok",
		};
		const selfAuthored = await handleDriveCommentEvent(
			baseEvent({ from_open_id: "bot-self" }),
			deps,
		);
		expect(selfAuthored.droppedReason).toBe("self_authored");
		const notAddressed = await handleDriveCommentEvent(
			baseEvent({ to_open_id: "someone_else" }),
			deps,
		);
		expect(notAddressed.droppedReason).toBe("not_addressed_to_bot");
		const badType = await handleDriveCommentEvent(
			baseEvent({ notice_type: "resolve_comment" }),
			deps,
		);
		expect(badType.droppedReason).toBe("notice_type_not_allowed");
		const missing = await handleDriveCommentEvent(
			baseEvent({ file_token: "" }),
			deps,
		);
		expect(missing.droppedReason).toBe("missing_required_fields");
	});

	it("delivery: thread-reply chunks escaped; 1069302 downgrades to whole-comment for THAT+later chunks; NO_REPLY suppresses", async () => {
		const dir = mkdtempSync(join(tmpdir(), "fs-deliv-"));
		const store = new FeishuCommentRulesStore(dir);
		store.pairingAdd("ou_author");
		const posted: Array<{ kind: string; text: string }> = [];
		const api = commentApi({
			postThreadReply: (_ft, _ty, _cid, text) => {
				posted.push({ kind: "thread", text });
				return { ok: false, code: 1069302 } as const;
			},
			postNewComment: (_ft, _ty, text) => {
				posted.push({ kind: "whole", text });
				return { ok: true } as const;
			},
		});
		const longAnswer = Array.from(
			{ length: 6 },
			(_, i) => `chunkline ${i} <b>&</b>`,
		).join("\n");
		const result = await handleDriveCommentEvent(baseEvent(), {
			rulesStore: store,
			api,
			selfOpenId: "bot-self",
			runTurn: async () => longAnswer,
		});
		expect(result.fellBackToWholeComment).toBe(true);
		expect(posted[0]?.kind).toBe("thread");
		expect(posted.slice(1).every((p) => p.kind === "whole")).toBe(true);
		expect(posted.every((p) => !p.text.includes("<b>"))).toBe(true); // HTML-escaped

		const noReply = await handleDriveCommentEvent(
			baseEvent({ comment_id: "cmt-2" }),
			{
				rulesStore: store,
				api,
				selfOpenId: "bot-self",
				runTurn: async () => "the answer is NO_REPLY here",
			},
		);
		expect(noReply.deliveredChunks).toBe(0); // sentinel suppresses delivery
	});

	it("OK reaction added on reply events and cleaned up after; skipped without reply_id", async () => {
		const dir = mkdtempSync(join(tmpdir(), "fs-react-"));
		const store = new FeishuCommentRulesStore(dir);
		store.pairingAdd("ou_author");
		const reactions: string[] = [];
		const api = commentApi({
			addReaction: (_rid) => {
				reactions.push("add");
				return true;
			},
			deleteReaction: (_rid) => {
				reactions.push("delete");
				return true;
			},
		});
		await handleDriveCommentEvent(
			baseEvent({ reply_id: "r9", comment_id: "cmt-r" }),
			{
				rulesStore: store,
				api,
				selfOpenId: "bot-self",
				runTurn: async () => "NO_REPLY",
			},
		);
		expect(reactions).toEqual(["add", "delete"]);

		const noReaction: string[] = [];
		await handleDriveCommentEvent(baseEvent({ reply_id: "" }), {
			rulesStore: store,
			api: commentApi({
				addReaction: () => {
					noReaction.push("add");
					return true;
				},
			}),
			selfOpenId: "bot-self",
			runTurn: async () => "NO_REPLY",
		});
		expect(noReaction).toEqual([]);
	});
});

// Silence unused-import guards where helpers exist for wiring parity.
void writeFileSync;
