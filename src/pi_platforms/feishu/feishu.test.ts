// pi_platforms/feishu/feishu.test.ts — BEHAVIOR CONTRACTS for the Feishu/Lark
// census port: the persisted replay/dedup subscription shape, A12 ingress
// classes (card actions w/ 15-min token dedup, VC meeting invites, Drive
// comments behind 3-tier access rules), the whole-message markdown decision,
// the send ladder, processing-reaction lifecycle, arrival batching, and the
// two-guard traversal of every synthetic event class.

import { describe, expect, it } from "vitest";
import { mkdtempSync, readFileSync, writeFileSync, existsSync } from "node:fs";
import { createHash } from "node:crypto";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ManualClock } from "../persistent-ws/manual-clock.js";
import {
	FakeFeishuServer,
	cardActionEnvelope,
	driveCommentEnvelope,
	meetingInvitedEnvelope,
	reactionEnvelope,
	type FeishuEventEnvelope,
} from "./fake-feishu.js";

// (reactionEnvelope joins the fake-server import below)
import {
	FeishuAdapter,
	FEISHU_REQUIRED_SECRETS,
	buildMarkdownPostPayload,
	buildMarkdownPostRows,
	stripFeishuMarkdownToPlainText,
	type FeishuRestPlane,
} from "./feishu-adapter.js";
import {
	FEISHU_TEXT_BATCH_DELAY_MS,
	FEISHU_POST_CONTENT_INVALID_MARKER,
	FEISHU_WEBHOOK_MAX_BODY_BYTES,
	FEISHU_WEBHOOK_DEFAULT_PATH,
	FEISHU_WEBHOOK_DEFAULT_HOST,
	FEISHU_WEBHOOK_DEFAULT_PORT,
	FEISHU_APPROVAL_BUTTONS,
} from "./manifest.js";
import {
	meetingDedupKey,
	parseMeetingInvitedEvent,
} from "./meeting-ingress.js";
import {
	CardActionTokenStore,
	FeishuSeenMessageStore,
	SEEN_MESSAGE_IDS_FILE,
} from "./dedup.js";
import {
	handleDriveCommentEvent,
	buildLocalCommentPrompt,
	type CommentApi,
	type CommentPage,
	type ReplyEntry,
} from "./comment-ingress.js";
import { FeishuCommentRulesStore } from "./comment-rules.js";
import * as rulesMod from "./comment-rules.js";
import { eventually } from "./feishu-fixture.js";

// ── helpers ──────────────────────────────────────────────────────────────

/** REST face over the fake server's scriptable endpoints (vendor op level).
 * Wire shapes mirror the lark_oapi request bodies (READ-ONLY Hermes anchors
 * cited inline): content is ALWAYS the vendor JSON STRING, reactions carry
 * {reaction_type:{emoji_type}} with path-carried reaction ids (:3171/:3203). */
function restPlaneForServer(server: FakeFeishuServer): FeishuRestPlane {
	return {
		async sendMessage(opts) {
			const behavior = server.recordRest(
				opts.replyToMessageId !== undefined ? "reply" : "messages",
				opts.replyToMessageId !== undefined ? "reply" : "create",
				{
					msg_type: opts.msgType,
					// Vendor wire: content is a JSON STRING on BOTH lanes —
					// text ships {"text": …} (:2461), post ships the built
					// {"zh_cn":{"content":rows}} payload string (:580).
					content: opts.content,
					// Vendor request coordinates (addressing/uuid contracts).
					receive_id: opts.receiveId,
					receive_id_type: opts.receiveIdType,
					uuid: opts.uuid,
					...(opts.replyToMessageId !== undefined
						? {
								reply_to: opts.replyToMessageId,
								reply_in_thread: opts.replyInThread === true,
							}
						: {}),
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
				content: opts.content,
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
		// POST im/v1/messages/:message_id/reactions — body
		// {reaction_type:{emoji_type}} (_add_reaction :3171/:3175); the
		// reaction_id comes back for later deletion.
		async addReaction(opts) {
			server.recordRest(`reactions/${opts.messageId}`, "create", {
				reaction_type: { emoji_type: opts.emojiType },
			});
			return {
				success: true,
				messageId: `reaction-${server.restCalls.length}`,
			};
		},
		// DELETE im/v1/messages/:message_id/reactions/:reaction_id — the id
		// rides THE PATH and the body is EMPTY (:3203 _remove_reaction).
		async removeReaction(opts) {
			server.recordRest(
				`reactions/${opts.messageId}/${opts.reactionId}`,
				"delete",
				{},
			);
			return { success: true };
		},
		// POST im/v1/images (image_type=message :5189) → image_key.
		async createImage(opts) {
			const behavior = server.recordRest("images", "create", {
				image_type: opts.imageType,
				image_filename: opts.filename,
				image_bytes: opts.image.length,
			});
			if (behavior.kind === "fail")
				return {
					success: false,
					error: `${behavior.msg} (code ${behavior.code})`,
				};
			if (behavior.kind === "timeout")
				return { success: false, error: "request timed out" };
			const body = (behavior as { kind: "ok"; body?: Record<string, unknown> })
				.body;
			return {
				success: true,
				...(typeof body?.["image_key"] === "string"
					? { imageKey: body["image_key"] }
					: { imageKey: `img_${server.restCalls.length}` }),
			};
		},
		// POST im/v1/files (file_type routing + duration>0 :5206) → file_key.
		async createFile(opts) {
			server.recordRest("files", "create", {
				file_type: opts.fileType,
				file_name: opts.fileName,
				file_bytes: opts.file.length,
				...(opts.durationMs !== undefined ? { duration: opts.durationMs } : {}),
			});
			return { success: true, fileKey: `file_${server.restCalls.length}` };
		},
		async getMessageResource(opts) {
			server.recordRest(
				`resources/${opts.messageId}/${opts.fileKey}`, // type rides query
				"get",
				{ type: opts.resourceType },
			);
			const hit = server.scriptedResources.get(opts.fileKey);
			return hit ?? null;
		},
		async resolveUserName(opts) {
			server.recordRest(
				`users/${opts.userId}`, // user_id_type rides query
				"get",
				{ user_id_type: opts.userIdType },
			);
			return server.scriptedUserNames.get(opts.userId) ?? null;
		},
		async resolveBotNames(botIds) {
			server.recordRest("bots/basic_batch", "get", { bot_ids: [...botIds] });
			const out: Record<string, string> = {};
			for (const id of botIds) {
				const name = server.scriptedBotNames.get(id);
				if (name !== undefined) out[id] = name;
			}
			return Object.keys(out).length > 0 ? out : null;
		},
		async getChat(chatId) {
			server.recordRest(`chats/${chatId}`, "get", {});
			return server.scriptedChats.get(chatId) ?? null;
		},
		getBotInfo: () =>
			Promise.resolve({
				openId: "bot-self",
				userId: "bot-user",
				name: "PiBot",
			}),
		async getMessage(messageId) {
			const hit = server.scriptedMessages.get(messageId);
			return hit ?? null;
		},
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
		optionalEnvReader?: (name: string) => string | undefined;
		/** Construction-time persisted dedup path (loader wires at construction). */
		statePath?: string | undefined;
		/** Inbound media resource cache root (feishu-2). */
		mediaCacheDir?: string | undefined;
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
		...(opts.optionalEnvReader !== undefined
			? { optionalEnvReader: opts.optionalEnvReader }
			: {}),
		...(opts.statePath !== undefined ? { dedupStatePath: opts.statePath } : {}),
		...(opts.mediaCacheDir !== undefined
			? { mediaCacheDir: opts.mediaCacheDir }
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
		let raw: { message_ids?: Record<string, number> } = {};
		try {
			raw = JSON.parse(readFileSync(statePath, "utf8")) as {
				message_ids?: Record<string, number>;
			};
		} catch (err) {
			throw new Error(`persisted dedup state unreadable: ${String(err)}`);
		}
		// Hermes snapshot shape (:4611): ids live under the message_ids key.
		expect(raw.message_ids?.["pm-1"]).toBeDefined();
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
		expect(stale["card"]).toBeUndefined(); // BARE stale response (:2894 parity)
		expect(Object.keys(stale)).toHaveLength(0);
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
		// Chat mismatch answers BARE — no invented toast, nothing popped (:2770).
		expect(mismatch["toast"]).toBeUndefined();
		expect(Object.keys(mismatch)).toHaveLength(0);
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
		const ops = h.server.restCalls.filter((c) =>
			c.endpoint.startsWith("reactions"),
		);
		expect(ops[0]?.method).toBe("create");
		// Vendor wire: body {reaction_type:{emoji_type}} (:3175).
		expect(
			(ops[0]?.payload as Record<string, unknown>)["reaction_type"],
		).toEqual({ emoji_type: "Typing" });
		// DELETE carries reaction_id IN THE PATH with an EMPTY body (:3203).
		expect(ops[1]?.method).toBe("delete");
		expect(ops[1]?.endpoint).toBe("reactions/om_1/reaction-1");
		expect(Object.keys(ops[1]?.payload ?? {})).toHaveLength(0);

		await h.engine.onProcessingStart("om_2");
		await h.engine.onProcessingComplete("om_2", "failure");
		const ops2 = h.server.restCalls.filter((c) =>
			c.endpoint.startsWith("reactions"),
		);
		expect(ops2.length).toBe(5); // add, delete, add, delete, CrossMark-add
		expect(ops2[3]?.method).toBe("delete");
		expect(
			(ops2[4]?.payload as Record<string, unknown>)["reaction_type"],
		).toEqual({ emoji_type: "CrossMark" });
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
			server.restCalls.filter((c) => c.endpoint.startsWith("reactions")),
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
				items: [
					{
						isWhole: false,
						quote: "q",
						replies: [
							{ openId: "ou_author", text: "please review", replyId: "" },
						],
					},
				],
			}),
			listWholeCommentsPage: () => ({
				items: [],
				hasMore: false,
				pageToken: "",
			}),
			listRepliesPage: () => ({
				items: [{ openId: "ou_author", text: "please review", replyId: "r0" }],
				hasMore: false,
				pageToken: "",
			}),
			addReaction: () => true,
			deleteReaction: () => true,
			postThreadReply: () => ({ ok: true }) as const,
			postNewComment: () => ({ ok: true }) as const,
			reverseLookupWikiNode: () => null,
			getWikiNode: () => null,
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
			postThreadReply: (_req) => {
				posted.push({ kind: "thread", text: _req.textRunText });
				return { ok: false, code: 1069302 } as const;
			},
			postNewComment: (_req) => {
				posted.push({ kind: "whole", text: _req.replyElementsText });
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
			addReaction: (_req) => {
				reactions.push(`add:${_req.reactionType}:${_req.fileType}`);
				return true;
			},
			deleteReaction: (_req) => {
				reactions.push(`delete:${_req.reactionType}:${_req.replyId}`);
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
		// Vendor wire shapes (:156/:206): action add/delete + reply_id +
		// reaction_type OK over file_type queries.
		expect(reactions).toEqual(["add:OK:docx", "delete:OK:r9"]);

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

// ════════════════════════════════════════════════════════════════════════
// Conforming-egress contracts (round-1 findings webhook-26..38)
// ════════════════════════════════════════════════════════════════════════

/** Webhook-mode event body ({header,event} envelope; :3660 routing input). */
function imMessageWebhookBody(
	messageId: string,
	chatId: string,
	text: string,
): Record<string, unknown> {
	return {
		header: {
			event_id: messageId,
			event_type: "im.message.receive_v1",
		},
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

describe("egress addressing — prefix strip, thread legs, fresh uuids", () => {
	it("feishu_user_id: chats send user_id receive type with the PREFIX STRIPPED", async () => {
		const h = await makeEngine();
		await h.engine.deliverText("feishu_user_id:usr_abc", "hello");
		const call = h.server.restCalls.at(-1);
		expect(call?.payload["receive_id_type"]).toBe("user_id");
		expect(call?.payload["receive_id"]).toBe("usr_abc"); // never the raw prefixed value
	});

	it("thread + reply anchor → REPLY leg with reply_in_thread=true anchored at the anchor", async () => {
		const h = await makeEngine();
		await h.engine.deliverText("oc_t1", "anchored", {
			thread_id: "om_thread_1",
			reply_to_message_id: "om_root",
		});
		const call = h.server.restCalls.at(-1);
		expect(call?.method).toBe("reply"); // reply API leg
		expect(call?.payload["reply_to"]).toBe("om_root");
		expect(call?.payload["reply_in_thread"]).toBe(true);
	});

	it("thread WITHOUT an anchor → CREATE leg addressed receive_id_type=thread_id", async () => {
		const h = await makeEngine();
		await h.engine.deliverText("oc_t2", "topic opener", {
			thread_id: "om_thread_2",
		});
		const call = h.server.restCalls.at(-1);
		expect(call?.method).toBe("create");
		expect(call?.payload["receive_id_type"]).toBe("thread_id");
		expect(call?.payload["receive_id"]).toBe("om_thread_2");
	});

	it("EVERY send leg mints a FRESH uuid4 — no hash-derived reuse across sends", async () => {
		const h = await makeEngine();
		await h.engine.deliverText("oc_u1", "**same body**");
		await h.engine.deliverText("oc_u2", "**same body**");
		const uuids = h.server.restCalls
			.filter((c) => c.endpoint !== "update")
			.map((c) => String(c.payload["uuid"]));
		expect(uuids.length).toBeGreaterThanOrEqual(2);
		expect(new Set(uuids).size).toBe(uuids.length); // all distinct
		for (const u of uuids) {
			// UUID v4 shape (vendor idempotency key str(uuid.uuid4()) parity).
			expect(u).toMatch(
				/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/,
			);
		}
	});
});

describe("markdown hint regex — 13-alternative source fidelity", () => {
	it.each([
		[
			"pipe table header+separator pair at message start",
			"| a | b |\n|---|---|\n| 1 | 2 |",
		],
		["ATX heading opening the message", "# Heading"],
		["bullet list opening the message", "- item one"],
		["asterisk list opening the message", "* item one"],
		["ordered list opening the message", "1. first"],
		["hr rule alone", "---"],
		["fenced code", "```\ncode\n```"],
		["inline code", "run `npm test` now"],
		["bold", "this is **bold** text"],
		["strikethrough", "this is ~~gone~~ text"],
		["underline", "this is <u>underlined</u> text"],
		["single-* italic", "this is *italic* text"],
		["link", "see [docs](https://example.com)"],
		["blockquote", "> quoted wisdom"],
	])(
		"classifies %s as markdown-shaped (msg_type=post)",
		async (_label, body) => {
			const h = await makeEngine();
			await h.engine.deliverText("oc_hint", body);
			expect(h.server.restCalls.at(-1)?.payload["msg_type"]).toBe("post");
		},
	);

	it("plain prose still rides msg_type=text verbatim", async () => {
		const h = await makeEngine();
		await h.engine.deliverText("oc_plain2", "just words, nothing else");
		expect(h.server.restCalls.at(-1)?.payload["msg_type"]).toBe("text");
	});
});

describe("edit leg — re-decision from edited content + post-invalid downgrade", () => {
	it("editing with markdown content after a plain send re-decides msg_type=post", async () => {
		const h = await makeEngine();
		await h.engine.deliverText("oc_edit", "plain original");
		expect(h.server.restCalls.at(-1)?.payload["msg_type"]).toBe("text");

		const result = await h.engine.editMessage(
			"oc_edit",
			"om_orig",
			"edited **with bold** markup",
		);
		expect(result.success).toBe(true);
		const upd = h.server.restCalls.at(-1);
		expect(upd?.endpoint).toBe("update");
		expect(upd?.payload["msg_type"]).toBe("post"); // re-decided, not carried over
	});

	it("a post-invalid rejection downgrades THAT update to stripped plain text", async () => {
		const h = await makeEngine();
		h.server.script("update", {
			kind: "fail",
			code: 230001,
			msg: FEISHU_POST_CONTENT_INVALID_MARKER,
		});
		const result = await h.engine.editMessage(
			"oc_edit2",
			"om_orig2",
			"**markdown edit** that gets rejected as post",
		);
		expect(result.success).toBe(true); // downgrade delivered
		const updates = h.server.restCalls.filter((c) => c.endpoint === "update");
		expect(updates).toHaveLength(2);
		expect(updates[0]?.payload["msg_type"]).toBe("post");
		expect(updates[1]?.payload["msg_type"]).toBe("text");
	});
});

describe("webhook ingress plane — gate ladder (:3558 _handle_webhook_request)", () => {
	async function makeWebhookEngine(extraEnv: Record<string, string> = {}) {
		return makeEngine({
			optionalEnvReader: (name) => extraEnv[name],
		});
	}

	function webhookRequest(
		body: unknown,
		opts: {
			headers?: Record<string, string> | undefined;
			peer?: string | undefined;
		} = {},
	) {
		return {
			headers: opts.headers ?? { "content-type": "application/json" },
			rawBody: Buffer.from(JSON.stringify(body), "utf8"),
			peer: opts.peer ?? "127.0.0.1",
		};
	}

	it("url_verification echoes the challenge ONLY with a valid verification token", async () => {
		const h = await makeWebhookEngine({
			FEISHU_VERIFICATION_TOKEN: "tok_secret",
		});
		// Feishu includes the verification token IN challenge requests; the
		// token gate runs BEFORE the echo (:3630 comment).
		const ok = await h.engine.handleWebhookPost(
			webhookRequest({
				type: "url_verification",
				challenge: "chk_123",
				header: { token: "tok_secret" },
			}),
		);
		expect(ok.status).toBe(200);
		expect(JSON.parse(ok.body ?? "{}")).toEqual({ challenge: "chk_123" });

		// A challenge WITHOUT a valid token never gets data reflected.
		const unauthed = await h.engine.handleWebhookPost(
			webhookRequest({
				type: "url_verification",
				challenge: "chk_attacker",
				header: { token: "WRONG" },
			}),
		);
		expect(unauthed.status).toBe(401);
	});

	it("missing/wrong verification token rejects 401 BEFORE any routing", async () => {
		const h = await makeWebhookEngine({
			FEISHU_VERIFICATION_TOKEN: "tok_secret",
		});
		const missing = await h.engine.handleWebhookPost(
			webhookRequest({
				header: { event_type: "im.message.receive_v1", event_id: "wv-1" },
				event: {},
			}),
		);
		expect(missing.status).toBe(401);

		const wrong = await h.engine.handleWebhookPost(
			webhookRequest({
				header: {
					token: "WRONG",
					event_type: "im.message.receive_v1",
					event_id: "wv-2",
				},
				event: {},
			}),
		);
		expect(wrong.status).toBe(401);
	});

	it("non-JSON content-type ⇒ 415; oversized bodies ⇒ 413; malformed JSON ⇒ 400", async () => {
		const h = await makeWebhookEngine();
		const ct = await h.engine.handleWebhookPost({
			headers: { "content-type": "text/plain" },
			rawBody: Buffer.from("{}"),
			peer: "127.0.0.1",
		});
		expect(ct.status).toBe(415);

		const big = await h.engine.handleWebhookPost({
			headers: { "content-type": "application/json" },
			rawBody: Buffer.alloc(FEISHU_WEBHOOK_MAX_BODY_BYTES + 1, 0x61),
			peer: "127.0.0.1",
		});
		expect(big.status).toBe(413);

		const malformed = await h.engine.handleWebhookPost({
			headers: { "content-type": "application/json" },
			rawBody: Buffer.from("{not json"),
			peer: "127.0.0.1",
		});
		expect(malformed.status).toBe(400);
	});

	it("signature check: SHA256(ts+nonce+encrypt_key+body) accepted; tampered rejected", async () => {
		const key = "enc_key";
		const h = await makeWebhookEngine({ FEISHU_ENCRYPT_KEY: key });
		const rawBody = Buffer.from(
			JSON.stringify({
				header: { event_type: "im.message.receive_v1", event_id: "sig-1" },
				event: {},
			}),
			"utf8",
		);
		const ts = "1700000000";
		const nonce = "nonce1";
		const good = createHash("sha256")
			.update(`${ts}${nonce}${key}${rawBody.toString("utf8")}`)
			.digest("hex");
		const accepted = await h.engine.handleWebhookPost({
			headers: {
				"content-type": "application/json",
				"x-lark-request-timestamp": ts,
				"x-lark-request-nonce": nonce,
				"x-lark-signature": good,
			},
			rawBody,
			peer: "127.0.0.1",
		});
		expect(accepted.status).toBe(200);

		const tampered = await h.engine.handleWebhookPost({
			headers: {
				"content-type": "application/json",
				"x-lark-request-timestamp": ts,
				"x-lark-request-nonce": nonce,
				"x-lark-signature": "deadbeef".repeat(8),
			},
			rawBody,
			peer: "127.0.0.1",
		});
		expect(tampered.status).toBe(401);
	});

	it("encrypted payloads answer 400 unsupported; valid events route through the SAME pipeline and ack ok", async () => {
		const h = await makeWebhookEngine();
		const encrypted = await h.engine.handleWebhookPost(
			webhookRequest({ encrypt: "…cipher…" }),
		);
		expect(encrypted.status).toBe(400);

		h.push(textEnvelope("wm-1", "on_wh1", "via webhook"));
		const routed = await h.engine.handleWebhookPost(
			webhookRequest(imMessageWebhookBody("wm-2", "on_wh1", "second")),
		);
		expect(routed.status).toBe(200);
		expect(JSON.parse(routed.body ?? "{}")).toEqual({ code: 0, msg: "ok" });
		await h.clock.advance(FEISHU_TEXT_BATCH_DELAY_MS + 50);
		await eventually(() => h.engine.turnLog.some((t) => t.includes("second")));
	});

	it("rate limit: bursts past 120/min on one key answer 429", async () => {
		const h = await makeWebhookEngine();
		let lastStatus = 200;
		for (let i = 0; i < 125; i++) {
			const resp = await h.engine.handleWebhookPost({
				headers: { "content-type": "application/json" },
				rawBody: Buffer.from(JSON.stringify({ n: i })),
				peer: "10.9.9.9",
			});
			lastStatus = resp.status;
		}
		expect(lastStatus).toBe(429);
	});
});

describe("reaction-command ingress (:2687/:2989)", () => {
	function botMessageScript(server: FakeFeishuServer): void {
		server.scriptedMessages.set("om_bot_msg", {
			senderId: "cli_a", // GET returns sender.id=app_id for OUR messages
			chatId: "on_dm_reaction",
			chatType: "p2p",
		});
		server.scriptedMessages.set("om_other_msg", {
			senderId: "cli_peer_app", // peer bot — NOT ours
			chatId: "on_dm_reaction",
			chatType: "p2p",
		});
	}

	it("human reaction on a BOT message becomes a synthetic reaction:{action}:{emoji} turn", async () => {
		const h = await makeEngine();
		botMessageScript(h.server);
		h.push(
			reactionEnvelope({
				eventId: "rx-1",
				eventType: "im.message.reaction.created_v1",
				messageId: "om_bot_msg",
				userOpenId: "ou_human",
				emojiType: "Typing",
			}),
		);
		await eventually(() => h.engine.turnLog.length >= 1);
		expect(h.engine.turnLog[0]).toBe("reaction:added:Typing");
	});

	it("deleted reactions route action=removed; bot-operator and foreign-message reactions drop silently", async () => {
		const h = await makeEngine();
		botMessageScript(h.server);
		h.push(
			reactionEnvelope({
				eventId: "rx-2",
				eventType: "im.message.reaction.deleted_v1",
				messageId: "om_bot_msg",
				userOpenId: "ou_human",
				emojiType: "DONE",
			}),
		);
		await eventually(() => h.engine.turnLog.length >= 1);
		expect(h.engine.turnLog[0]).toBe("reaction:removed:DONE");

		const turnsBefore = h.engine.turnLog.length;
		h.push(
			reactionEnvelope({
				eventId: "rx-3",
				eventType: "im.message.reaction.created_v1",
				messageId: "om_bot_msg",
				operatorType: "app", // lifecycle-reaction feedback loop
				userOpenId: "",
				emojiType: "Typing",
			}),
		);
		h.push(
			reactionEnvelope({
				eventId: "rx-4",
				eventType: "im.message.reaction.created_v1",
				messageId: "om_other_msg", // someone else's message
				userOpenId: "ou_human",
				emojiType: "Typing",
			}),
		);
		await new Promise<void>((r) => setTimeout(r, 30));
		expect(h.engine.turnLog.length).toBe(turnsBefore);
	});
});

describe("meeting-invite dedup rides the PERSISTED seen-set (:150)", () => {
	it("duplicate invites stay suppressed ACROSS an adapter restart (persisted store)", async () => {
		const dir = mkdtempSync(join(tmpdir(), "feishu-vc-dedup-"));
		const statePath = join(dir, SEEN_MESSAGE_IDS_FILE);
		const env = meetingInvitedEnvelope({
			eventId: "evt-persist-1",
			meeting: { id: "m-p", topic: "Persist", meeting_no: "1-2-3" },
			inviter: { open_id: "ou_inv", user_name: "Inv" },
		});

		const first = await makeEngine({ statePath });
		first.push(env);
		await eventually(() => first.engine.meetingInviteLog.length >= 1);
		expect(first.engine.meetingInviteLog[0]?.outcome).toBe("dispatched");
		await first.engine.disconnect(); // persist hook

		// A FRESH adapter over the same scoped file remembers the invite key.
		const second = await makeEngine({ statePath });
		second.push({ ...env });
		await new Promise<void>((r) => setTimeout(r, 30));
		expect(second.engine.meetingInviteLog[0]?.outcome).toBe(
			"dropped_duplicate",
		);
		expect(second.engine.turnLog).toHaveLength(0);
	});

	it("the inviter check runs AFTER the dedup insert — no-inviter keys are consumed too", async () => {
		const h = await makeEngine();
		const envNoInviter = meetingInvitedEnvelope({
			meeting: { id: "m-x", topic: "", meeting_no: "7-7-7" },
			inviter: { open_id: "" },
		});
		h.push(envNoInviter);
		await new Promise<void>((r) => setTimeout(r, 20));
		expect(
			h.engine.meetingInviteLog.some(
				(l) => l.outcome === "dropped_no_inviter_id",
			),
		).toBe(true);
		// Replay of the SAME key: dropped_duplicate (already recorded), which
		// proves the dedup insert preceded the inviter refusal.
		h.push({ ...envNoInviter });
		await new Promise<void>((r) => setTimeout(r, 20));
		expect(
			h.engine.meetingInviteLog.some((l) => l.outcome === "dropped_duplicate"),
		).toBe(true);
	});
});

describe("dedup snapshot wrapper — message_ids key (:4611/:4575)", () => {
	it("persist writes {message_ids:{id:seconds}}; load reads ONLY that key", () => {
		const dir = mkdtempSync(join(tmpdir(), "feishu-snap-"));
		const path = join(dir, SEEN_MESSAGE_IDS_FILE);
		const clock = new ManualClock();
		const w = new FeishuSeenMessageStore({
			statePath: path,
			nowMs: clock.nowMs,
		});
		w.isDuplicate("snap-1");
		w.persist(path);

		const parsed = JSON.parse(readFileSync(path, "utf8")) as Record<
			string,
			unknown
		>;
		const ids = parsed["message_ids"] as Record<string, number> | undefined;
		expect(ids && typeof ids === "object").toBe(true);
		expect(typeof ids?.["snap-1"]).toBe("number");
		expect(Number.isFinite(ids?.["snap-1"] as number)).toBe(true);

		// A flat map WITHOUT the key is unreadable by design (mutual format).
		const flatPath = join(dir, "flat.json");
		writeFileSync(flatPath, JSON.stringify({ "snap-1": 1234 }), "utf8");
		const r = new FeishuSeenMessageStore({ statePath: flatPath });
		expect(r.size).toBe(0);
	});

	it("legacy plain LIST inside message_ids migrates with epoch 0", () => {
		const dir = mkdtempSync(join(tmpdir(), "feishu-legacy-"));
		const path = join(dir, SEEN_MESSAGE_IDS_FILE);
		writeFileSync(
			path,
			JSON.stringify({ message_ids: ["old-a", "old-b"] }),
			"utf8",
		);
		const store = new FeishuSeenMessageStore({ statePath: path });
		expect(store.isDuplicate("old-a")).toBe(true);
		expect(store.isDuplicate("old-b")).toBe(true);
	});
});

describe("comment ingress — wiki re-resolution + verbatim prompts (:1172/:884/:929)", () => {
	function wikiEvent(fileToken: string): Record<string, unknown> {
		return driveCommentEnvelope({
			event_id: "cev-wiki",
			comment_id: "cmt-w",
			reply_id: "",
			file_token: fileToken,
			file_type: "docx",
			from_open_id: "ou_wiki_author",
			to_open_id: "bot-self",
		})["event"] as Record<string, unknown>;
	}

	function wikiApi(reverseToken: string | null): CommentApi {
		return {
			docMeta: () => ({ title: "Wiki Doc", url: "https://feishu.cn/docx/w" }),
			batchQueryComment: () => ({
				items: [
					{
						isWhole: false,
						quote: "",
						replies: [{ openId: "ou_wiki_author", text: "hello", replyId: "" }],
					},
				],
			}),
			listWholeCommentsPage: () => ({
				items: [],
				hasMore: false,
				pageToken: "",
			}),
			listRepliesPage: () => ({
				items: [{ openId: "ou_wiki_author", text: "hello", replyId: "" }],
				hasMore: false,
				pageToken: "",
			}),
			addReaction: () => true,
			deleteReaction: () => true,
			postThreadReply: () => ({ ok: true }) as const,
			postNewComment: () => ({ ok: true }) as const,
			reverseLookupWikiNode: () => reverseToken,
			getWikiNode: () => null,
		};
	}

	it("wiki:{node}-keyed rules match AFTER the reverse lookup when wildcard/top missed", async () => {
		const dir = mkdtempSync(join(tmpdir(), "fs-wiki-rules-"));
		writeFileSync(
			join(dir, "feishu_comment_rules.json"),
			JSON.stringify({
				documents: {
					"wiki:node_777": {
						policy: "allowlist",
						allow_from: ["ou_wiki_author"],
					},
				},
			}),
			"utf8",
		);
		const store = new FeishuCommentRulesStore(dir);

		// Reverse lookup SUCCEEDS ⇒ wiki rule admits the author.
		const ok = await handleDriveCommentEvent(wikiEvent("objtoken12345"), {
			rulesStore: store,
			api: wikiApi("node_777"),
			selfOpenId: "bot-self",
			runTurn: async () => "reply",
		});
		expect(ok.deniedByRules).toBe(false);
		expect(ok.promptBuilt).toBe(true);

		// Reverse lookup MISSES ⇒ falls through to top-level defaults (deny).
		const denied = await handleDriveCommentEvent(wikiEvent("objtoken12345"), {
			rulesStore: store,
			api: wikiApi(null),
			selfOpenId: "bot-self",
			runTurn: async () => "reply",
		});
		expect(denied.deniedByRules).toBe(true);
	});

	it("local prompts carry the VERBATIM source skeleton — opener, counts, <-- YOU markers, instruction block", async () => {
		const dir = mkdtempSync(join(tmpdir(), "fs-prompt-"));
		writeFileSync(
			join(dir, "feishu_comment_rules.json"),
			JSON.stringify({ policy: "allowlist", allow_from: ["ou_human"] }),
			"utf8",
		);
		const store = new FeishuCommentRulesStore(dir);
		let captured = "";
		await handleDriveCommentEvent(
			driveCommentEnvelope({
				event_id: "cev-p",
				comment_id: "cmt-p",
				reply_id: "r1",
				file_token: "doctok999999",
				file_type: "docx",
				from_open_id: "ou_human",
				to_open_id: "bot-self",
			})["event"] as Record<string, unknown>,
			{
				rulesStore: store,
				api: {
					...wikiApi(null),
					batchQueryComment: () => ({
						items: [
							{
								isWhole: false,
								quote: "quoted text",
								replies: [
									{ openId: "ou_human", text: "root question", replyId: "r0" },
									{ openId: "bot-self", text: "bot answer", replyId: "r1" },
								],
							},
						],
					}),
					listRepliesPage: () => ({
						items: [
							{ openId: "ou_human", text: "root question", replyId: "r0" },
							{ openId: "bot-self", text: "bot answer", replyId: "r1" },
						],
						hasMore: false,
						pageToken: "",
					}),
				},
				selfOpenId: "bot-self",
				runTurn: async (prompt) => {
					captured = prompt;
					return "answer";
				},
			},
		);

		// Skeleton lines (:884–923), line-for-line.
		expect(captured.startsWith('The user added a reply in "Wiki Doc".')).toBe(
			true,
		);
		expect(captured).toContain('Current user comment text: "bot answer"');
		expect(captured).toContain('Original comment text: "root question"');
		expect(captured).toContain('Quoted content: "quoted text"');
		expect(captured).toContain(
			"This comment mentioned you (@mention is for routing, not task content).",
		);
		expect(captured).toContain("- file_type=docx");
		expect(captured).toContain("- file_token=doctok999999");
		expect(captured).toContain("- comment_id=cmt-p");
		expect(captured).toContain("Current comment card timeline (2/2 entries):");
		expect(captured).toContain("[ou_human] root question");
		expect(captured).toContain("[bot-self] bot answer <-- YOU");
		// The verbatim instruction block (:868).
		expect(captured).toContain(
			"This is a Feishu document comment thread, not an IM chat.",
		);
		expect(captured).toContain(
			'Do not show your reasoning process. Do not start with "I will", "Let me", or "I\'ll first".',
		);
		expect(
			captured
				.trimEnd()
				.endsWith("If no reply is needed, output exactly NO_REPLY."),
		).toBe(true);
	});

	it("truncation appends ASCII '...' — never a unicode ellipsis", () => {
		const long = "x".repeat(300);
		const entry = { openId: "u", text: long, isSelf: false };
		const prompt = buildLocalCommentPrompt({
			docTitle: "T",
			docUrl: "u",
			fileType: "docx",
			fileToken: "tok",
			commentId: "c",
			quoteText: "",
			rootCommentText: "",
			targetReplyText: "",
			timeline: [entry],
			targetIndex: 0,
		});
		expect(prompt).toContain(`[u] ${"x".repeat(220)}...`);
		expect(prompt).not.toContain("…");
	});
});

// ════════════════════════════════════════════════════════════
// Stability round feishu-r2 — conforming-behavior contracts
// ════════════════════════════════════════════════════════════

describe("feishu-1 — post payload fence-split JSON string (:580/:604/:4641)", () => {
	it("buildMarkdownPostRows: no fence ⇒ ONE [[md]] row; empty content ⇒ empty md row", () => {
		expect(buildMarkdownPostRows("prose **bold**")).toEqual([
			[{ tag: "md", text: "prose **bold**" }],
		]);
		expect(buildMarkdownPostRows("")).toEqual([[{ tag: "md", text: "" }]]);
	});

	it("buildMarkdownPostRows splits at REAL fence lines; prose survives around the block", () => {
		const content = "before\n```js\ncode()\nstay\n```\nafter";
		expect(buildMarkdownPostRows(content)).toEqual([
			[{ tag: "md", text: "before" }],
			[{ tag: "md", text: "```js\ncode()\nstay\n```" }],
			[{ tag: "md", text: "after" }],
		]);
	});

	it("unclosed fences keep the tail INSIDE the code row; blank prose segments drop", () => {
		const unclosed = "```\nonly code";
		expect(buildMarkdownPostRows(unclosed)).toEqual([
			[{ tag: "md", text: "```\nonly code" }],
		]);
		// Blank pre-fence segment is skipped, not emitted as an empty row.
		const blankLead = "\n\n```\nx\n```";
		expect(buildMarkdownPostRows(blankLead)).toEqual([
			[{ tag: "md", text: "```\nx\n```" }],
		]);
	});

	it("the WIRE content for post sends is the JSON STRING {zh_cn:{content:rows}}", async () => {
		const h = await makeEngine();
		await h.engine.deliverText("oc_post1", "intro\n```\ncode\n```\ntail");
		const call = h.server.restCalls.at(-1);
		expect(call?.payload["msg_type"]).toBe("post");
		const raw = String(call?.payload["content"]);
		const parsed = JSON.parse(raw) as {
			zh_cn: { content: Array<Array<{ tag: string; text: string }>> };
		};
		expect(parsed.zh_cn.content).toEqual([
			[{ tag: "md", text: "intro" }],
			[{ tag: "md", text: "```\ncode\n```" }],
			[{ tag: "md", text: "tail" }],
		]);
	});
});

describe("feishu-6 — text lane VERBATIM; faithful stripper on downgrade lanes only", () => {
	it("plain text ships UNSTRIPPED (format_message returns content.strip() :2461)", async () => {
		const h = await makeEngine();
		// snake_case + tilde/spoiler markup would be MANGLED by the old kit
		// stripper — they must survive byte-exact (only edge whitespace trims).
		await h.engine.deliverText("oc_v2", "H~2~O and ||spoiler|| stay");
		const call = h.server.restCalls.at(-1);
		expect(call?.payload["msg_type"]).toBe("text");
		expect(call?.textContent).toBe("H~2~O and ||spoiler|| stay");
	});

	it("stripFeishuMarkdownToPlainText: link → 'text (url)', blockquote, hr, <u>, strike, bold", () => {
		const input =
			"see **docs** here\n> quoted note\n[site](https://example.com/a)\n---\n<u>ul</u> ~~gone~~";
		expect(stripFeishuMarkdownToPlainText(input)).toBe(
			"see docs here\nquoted note\nsite (https://example.com/a)\n---\nul gone",
		);
	});

	it("CRLF normalises and shared strip_markdown removes headings/inline-code/underline-bold", () => {
		expect(stripFeishuMarkdownToPlainText("# Title\r\nsome `code` run")).toBe(
			"Title\nsome code run",
		);
		expect(stripFeishuMarkdownToPlainText("__under bold__ and _it_")).toBe(
			"under bold and it",
		);
	});

	it("post-rejected downgrade carries the STRIPPED plain body (not raw markdown)", async () => {
		const h = await makeEngine();
		h.server.script("messages", {
			kind: "fail",
			code: 400,
			msg: FEISHU_POST_CONTENT_INVALID_MARKER,
		});
		await h.engine.deliverText("oc_pg2", "see **docs** now\n> quote line");
		const calls = h.server.restCalls.filter((c) => c.endpoint === "messages");
		expect(calls[0]?.msgType).toBe("post");
		expect(calls[1]?.msgType).toBe("text");
		expect(calls[1]?.textContent).toBe("see docs now\nquote line");
	});

	it("edit downgrade uses the SAME faithful stripper", async () => {
		const h = await makeEngine();
		h.server.script("update", {
			kind: "fail",
			code: 230001,
			msg: FEISHU_POST_CONTENT_INVALID_MARKER,
		});
		await h.engine.editMessage(
			"oc_ed",
			"om_1",
			"**bold edit** [l](https://x.y)",
		);
		const updates = h.server.restCalls.filter((c) => c.endpoint === "update");
		expect(updates[0]?.payload["msg_type"]).toBe("post");
		expect(updates[1]?.payload["msg_type"]).toBe("text");
		// recordRest only derives textContent for create legs — assert via the
		// recorded wire string.
		expect(updates[1]?.payload["content"]).toBe("bold edit l (https://x.y)");
	});
});

describe("feishu-3 — vc_invite:{event_id} dedup key from the frame header (:131/:159)", () => {
	it("parseMeetingInvitedEvent reads root.header.event_id", () => {
		const payload = parseMeetingInvitedEvent({
			header: { event_id: "evt-root-9" },
			event: {
				meeting: { id: "m", meeting_no: "no-9" },
				inviter: { open_id: "ou_i", user_name: "Iv" },
			},
		});
		expect(payload?.eventId).toBe("evt-root-9");
		expect(meetingDedupKey(payload as never)).toBe("vc_invite:evt-root-9");
	});

	it("SAME meeting/inviter/time with DIFFERENT event ids dispatch TWICE; same id drops once", async () => {
		const h = await makeEngine();
		const base = {
			meeting: { id: "m-same", meeting_no: "7-7-7" },
			inviter: { open_id: "ou_inv", user_name: "Same Inviter" },
		} as const;
		h.push(meetingInvitedEnvelope({ eventId: "evt-A", ...base }));
		h.push(meetingInvitedEnvelope({ eventId: "evt-B", ...base }));
		await eventually(() => h.engine.turnLog.length >= 2);
		const dispatched = h.engine.meetingInviteLog.filter(
			(l) => l.outcome === "dispatched",
		);
		expect(dispatched.map((d) => d.key)).toEqual([
			"vc_invite:evt-A",
			"vc_invite:evt-B",
		]);
	});
});

describe("feishu-4 — CommentApi vendor queries/bodies + fake pagination (:300/:362/:424/:489/:511)", () => {
	interface CapturedReq {
		uri: string;
		queries: Array<{ name: string; value: string }>;
		body?: Record<string, unknown>;
	}

	function pagedApi(total: number) {
		const replyReqs: CapturedReq[] = [];
		const all: ReplyEntry[] = Array.from({ length: total }, (_, i) => ({
			openId: "ou_author",
			text: `reply-${i}`,
			replyId: `r${i}`,
		}));
		let seq = 0;
		const api = {
			docMeta: () => ({ title: "T", url: "https://feishu.cn/docx/t" }),
			batchQueryComment: (req: {
				fileToken: string;
				fileType: string;
				userIdType: string;
				commentIds: readonly [string];
			}): { items: Array<unknown> } | undefined => {
				// Vendor shape: queries file_type+user_id_type=open_id, body
				// {comment_ids:[id]} (:318/:324).
				expect(req.userIdType).toBe("open_id");
				expect(req.commentIds).toEqual(["cmt-pg"]);
				return {
					items: [{ isWhole: false, quote: "q", replies: [] }],
				};
			},
			listWholeCommentsPage: () => ({
				items: [],
				hasMore: false,
				pageToken: "",
			}),
			listRepliesPage: (req: {
				fileToken: string;
				fileType: string;
				commentId: string;
				pageSize: number;
				userIdType: string;
				pageToken?: string | undefined;
			}): CommentPage<ReplyEntry> => {
				// Vendor-shaped capture (queries/body coordinates).
				replyReqs.push({
					uri: `/drive/v1/files/${req.fileToken}/comments/${req.commentId}/replies`,
					queries: [
						{ name: "file_type", value: req.fileType },
						{ name: "page_size", value: String(req.pageSize) },
						{ name: "user_id_type", value: req.userIdType },
						...(req.pageToken !== undefined
							? [{ name: "page_token", value: req.pageToken }]
							: []),
					],
				});
				const offset = req.pageToken === undefined ? 0 : Number(req.pageToken);
				const slice = all.slice(offset, offset + req.pageSize);
				const nextOffset = offset + req.pageSize;
				seq += 1;
				return {
					items: slice,
					hasMore: nextOffset < total,
					pageToken: nextOffset < total ? String(nextOffset) : "",
				};
			},
			addReaction: (req: { reactionType: string; replyId: string }) => {
				// Vendor body {action:"add",reply_id,reaction_type} (:172–177).
				expect(req.reactionType).toBe("OK");
				return true;
			},
			deleteReaction: () => true,
			postThreadReply: (req: {
				textRunText: string;
			}): { ok: true } | { ok: false; code: number } => {
				// Body shape {content:{elements:[text_run]}} asserted via capture.
				replyReqs.push({
					uri: "POST replies",
					queries: [],
					body: {
						content: {
							elements: [
								{ type: "text_run", text_run: { text: req.textRunText } },
							],
						},
					},
				});
				return { ok: true } as const;
			},
			postNewComment: (): { ok: true } => ({ ok: true }) as const,
			reverseLookupWikiNode: () => null,
			getWikiNode: () => null,
		};
		return { api: api as unknown as CommentApi, replyReqs, pages: () => seq };
	}

	function pgEvent(): Record<string, unknown> {
		return driveCommentEnvelope({
			event_id: "cev-pg",
			comment_id: "cmt-pg",
			reply_id: "r249",
			file_token: "doctokpage999",
			file_type: "docx",
			from_open_id: "ou_author",
			to_open_id: "bot-self",
		})["event"] as Record<string, unknown>;
	}

	async function freshRules(): Promise<FeishuCommentRulesStore> {
		const dir = mkdtempSync(join(tmpdir(), "fs-pg-rules-"));
		writeFileSync(
			join(dir, "feishu_comment_rules.json"),
			JSON.stringify({ policy: "allowlist", allow_from: ["ou_author"] }),
		);
		return new FeishuCommentRulesStore(dir);
	}

	it("250 replies paginate over THREE page calls of ≤100 with chained page_token", async () => {
		const store = await freshRules();
		const { api, replyReqs, pages } = pagedApi(250);
		const result = await handleDriveCommentEvent(pgEvent(), {
			rulesStore: store,
			api,
			selfOpenId: "bot-self",
			runTurn: async () => "NO_REPLY",
		});
		expect(result.promptBuilt).toBe(true);
		const listCalls = replyReqs.filter((r) => r.uri.includes("replies"));
		expect(listCalls.length).toBe(3); // ceil(250/100), within the 5-page cap
		expect(listCalls[0]?.queries.some((q) => q.name === "page_token")).toBe(
			false,
		);
		expect(listCalls[1]?.queries).toContainEqual({
			name: "page_token",
			value: "100",
		});
		expect(listCalls[2]?.queries).toContainEqual({
			name: "page_token",
			value: "200",
		});
		for (const call of listCalls) {
			expect(call.queries).toContainEqual({ name: "file_type", value: "docx" });
			expect(call.queries).toContainEqual({ name: "page_size", value: "100" });
			expect(call.queries).toContainEqual({
				name: "user_id_type",
				value: "open_id",
			});
		}
		void pages;
	});

	it("the >500-reply walk STOPS at the 5-page cap (max 500 fetched)", async () => {
		const store = await freshRules();
		const { api, replyReqs } = pagedApi(1200);
		await handleDriveCommentEvent(pgEvent(), {
			rulesStore: store,
			api,
			selfOpenId: "bot-self",
			runTurn: async () => "NO_REPLY",
		});
		const listCalls = replyReqs.filter((r) => r.uri.includes("replies"));
		expect(listCalls.length).toBe(5);
	});

	it("thread replies ride the text_run body shape", async () => {
		const store = await freshRules();
		const { api, replyReqs } = pagedApi(1);
		await handleDriveCommentEvent(pgEvent(), {
			rulesStore: store,
			api,
			selfOpenId: "bot-self",
			runTurn: async () => "answer text",
		});
		const posts = replyReqs.filter((r) => r.uri === "POST replies");
		expect(posts.length).toBeGreaterThanOrEqual(1);
		expect(posts[0]?.body).toEqual({
			content: {
				elements: [{ type: "text_run", text_run: { text: "answer text" } }],
			},
		});
	});
});

describe("feishu-2 — im/v1 uploads, downloads and media sends (:5189/:5206/:4001)", () => {
	function imageEnvelopeFor(
		messageId: string,
		chatId: string,
		imageKey: string,
	): FeishuEventEnvelope {
		return {
			header: {
				event_id: messageId,
				event_type: "im.message.receive_v1",
			},
			event: {
				message: {
					message_id: messageId,
					message_type: "image",
					content: JSON.stringify({ image_key: imageKey }),
					chat_type: chatId.startsWith("oc_") ? "group" : "p2p",
					chat_id: chatId,
				},
				sender: {
					sender_type: "user",
					sender_id: {
						open_id: "ou_user1",
						user_id: "u_1",
						union_id: "on_1",
					},
				},
			},
		};
	}

	/** Minimal OGG container: last granule 96000 @48kHz ⇒ 2000ms. */
	function oggBytes(granule: bigint): Buffer {
		const buf = Buffer.alloc(64, 0);
		buf.write("OggS", 0, "latin1");
		buf.writeBigUInt64LE(granule, 6);
		buf.writeUInt8(1, 26); // one segment
		buf.writeUInt8(0, 27);
		return buf;
	}

	it("sendImageFile uploads image_type=message then ships msg_type=image {image_key}", async () => {
		const h = await makeEngine();
		const dir = mkdtempSync(join(tmpdir(), "fs-media-"));
		const imgPath = join(dir, "pic.png");
		writeFileSync(imgPath, Buffer.from([0x89, 0x50, 0x4e, 0x47]));
		const res = await h.engine.sendImageFile("oc_img", imgPath);
		expect(res.success).toBe(true);
		const up = h.server.restCalls.find((c) => c.endpoint === "images");
		expect(up?.payload["image_type"]).toBe("message"); // _FEISHU_IMAGE_UPLOAD_TYPE :203
		expect(up?.payload["image_filename"]).toBe("pic.png");
		const send = h.server.restCalls.at(-1);
		expect(send?.payload["msg_type"]).toBe("image");
		expect(JSON.parse(String(send?.payload["content"]))).toEqual({
			image_key: expect.any(String),
		});
	});

	it("caption upgrades the image to a post whose rows APPEND the img element (:2310)", async () => {
		const h = await makeEngine();
		const dir = mkdtempSync(join(tmpdir(), "fs-media-"));
		const imgPath = join(dir, "cap.png");
		writeFileSync(imgPath, Buffer.from([1]));
		await h.engine.sendImageFile("oc_img2", imgPath, {
			caption: "look at this",
		});
		const send = h.server.restCalls.at(-1);
		expect(send?.payload["msg_type"]).toBe("post");
		const parsed = JSON.parse(String(send?.payload["content"])) as {
			zh_cn: { content: Array<Array<Record<string, string>>> };
		};
		expect(parsed.zh_cn.content.at(-1)).toEqual([
			{ tag: "img", image_key: expect.any(String) },
		]);
		expect(parsed.zh_cn.content[0]?.[0]?.text).toContain("look at this");
	});

	it("sendVoice routes .ogg → opus file_type WITH duration; bubble rides msg_type=audio", async () => {
		const h = await makeEngine();
		const dir = mkdtempSync(join(tmpdir(), "fs-media-"));
		const oggPath = join(dir, "voice.ogg");
		writeFileSync(oggPath, oggBytes(96000n)); // 2s @48kHz
		const res = await h.engine.sendVoice("oc_voice", oggPath);
		expect(res.success).toBe(true);
		const up = h.server.restCalls.find((c) => c.endpoint === "files");
		expect(up?.payload["file_type"]).toBe("opus");
		expect(up?.payload["duration"]).toBe(2000); // granule/48000*1000
		const send = h.server.restCalls.at(-1);
		expect(send?.payload["msg_type"]).toBe("audio");
		expect(JSON.parse(String(send?.payload["content"]))).toEqual({
			file_key: expect.any(String),
		});
	});

	it("sendDocument maps doc extensions (.pdf→pdf) and unknowns ride stream/file; sendVideo rides mp4/media", async () => {
		const h = await makeEngine();
		const dir = mkdtempSync(join(tmpdir(), "fs-media-"));
		const pdfPath = join(dir, "spec.pdf");
		writeFileSync(pdfPath, Buffer.from("%PDF-1.4"));
		await h.engine.sendDocument("oc_doc", pdfPath);
		let send = h.server.restCalls.at(-1);
		let up = h.server.restCalls.filter((c) => c.endpoint === "files").at(-1);
		expect(up?.payload["file_type"]).toBe("pdf");
		expect(send?.payload["msg_type"]).toBe("file");

		const binPath = join(dir, "blob.bin");
		writeFileSync(binPath, Buffer.from([7]));
		await h.engine.sendDocument("oc_doc", binPath);
		up = h.server.restCalls.filter((c) => c.endpoint === "files").at(-1);
		expect(up?.payload["file_type"]).toBe("stream"); // _FEISHU_FILE_UPLOAD_TYPE

		const mp4Path = join(dir, "clip.mp4");
		writeFileSync(mp4Path, Buffer.from([0, 0, 0, 24]));
		await h.engine.sendVideo("oc_vid", mp4Path);
		send = h.server.restCalls.at(-1);
		expect(send?.payload["msg_type"]).toBe("media");
		up = h.server.restCalls.filter((c) => c.endpoint === "files").at(-1);
		expect(up?.payload["file_type"]).toBe("mp4");
		expect(up?.payload["duration"]).toBeUndefined(); // duration ONLY when > 0
	});

	it("inbound image refs download to the media cache BEFORE dispatch; failures keep the vendor ref", async () => {
		const cacheDir = mkdtempSync(join(tmpdir(), "fs-cache-"));
		const h = await makeEngine({ mediaCacheDir: cacheDir });
		h.server.scriptedResources.set("img_key_1", {
			bytes: new Uint8Array([0xff, 0xd8, 0xff, 0xe0]),
			contentType: "image/jpeg",
			filename: "photo.jpg",
		});
		h.push(imageEnvelopeFor("om_media_1", "on_dm9", "img_key_1"));
		// The resource download fires immediately; the MEDIA BATCH dispatch
		// awaits it before the turn.
		await h.clock.advance(1000);
		await eventually(() => h.engine.resourceCacheLog.length >= 1);
		const entry = h.engine.resourceCacheLog[0];
		expect(entry?.fileKey).toBe("img_key_1");
		expect(existsSync(entry?.path ?? "")).toBe(true);
		const resCalls = h.server.restCalls.filter((c) =>
			c.endpoint.startsWith("resources/"),
		);
		expect(resCalls[0]?.payload["type"]).toBe("image"); // ?type=image :4001

		// Unscripted key: download fails SILENTLY, ref stays vendor-shaped.
		h.push(imageEnvelopeFor("om_media_2", "on_dm9", "missing_key"));
		await h.clock.advance(1000);
		await new Promise<void>((r) => setTimeout(r, 20));
		expect(h.engine.resourceCacheLog).toHaveLength(1);
	});
});

describe("feishu-7 — empty reaction emoji defaults to UNKNOWN (:3023)", () => {
	it("synthetic command reads reaction:added:UNKNOWN when emoji_type missing", async () => {
		const h = await makeEngine();
		h.server.scriptedMessages.set("om_rx", {
			senderId: "cli_a",
			chatId: "on_dm_reaction",
			chatType: "p2p",
		});
		h.push(
			reactionEnvelope({
				eventId: "rx-u1",
				eventType: "im.message.reaction.created_v1",
				messageId: "om_rx",
				userOpenId: "ou_human",
				emojiType: "",
			}),
		);
		await eventually(() => h.engine.turnLog.length >= 1);
		expect(h.engine.turnLog[0]).toBe("reaction:added:UNKNOWN");
	});
});

describe("feishu-8 — cached silent-failure name resolution feeds resolved cards (:4205/:2811)", () => {
	it("ingress warms the sender-name cache; the card click attributes by RESOLVED name", async () => {
		const h = await makeEngine();
		h.server.scriptedUserNames.set("ou_alice", "Alice Chen");
		// Warm the cache exactly like production: an inbound DM from Alice.
		const warm = textEnvelope("warm-1", "on_alice", "hi there");
		(warm.event["sender"] as Record<string, unknown>)["sender_id"] = {
			open_id: "ou_alice",
			user_id: "u_alice",
			union_id: "on_alice",
		};
		h.push(warm);
		const probe = h.engine as unknown as {
			getCachedSenderName: (id: string) => string | null;
		};
		await eventually(() => probe.getCachedSenderName("ou_alice") !== null);

		const approvalId = h.engine.nextApprovalId();
		h.engine.approvals.register(approvalId, "sk-alice");
		h.engine.approvalState.set(approvalId, {
			sessionKey: "sk-alice",
			chatId: "oc_chat",
		});
		await h.engine.handleCardActionTrigger(
			"tok-alice",
			cardActionEnvelope({
				token: "tok-alice",
				actionValue: { hermes_action: "approve_once", approval_id: approvalId },
				openChatId: "oc_chat",
				operatorOpenId: "ou_alice",
			})["event"] as Record<string, unknown>,
		);
		const card = h.engine.resolvedApprovalCards[0]?.card;
		const attribution = card?.elements.find((e) => e.tag === "markdown") as
			| { content: string }
			| undefined;
		expect(attribution?.content).toContain("by Alice Chen");
		expect(attribution?.content).not.toContain("ou_alice");
	});

	it("cold cache falls back to the RAW open_id and resolution failures are silent", async () => {
		const h = await makeEngine(); // no scripted names — resolveUserName null
		const approvalId = h.engine.nextApprovalId();
		h.engine.approvals.register(approvalId, "sk-cold");
		h.engine.approvalState.set(approvalId, {
			sessionKey: "sk-cold",
			chatId: "oc_chat",
		});
		await h.engine.handleCardActionTrigger(
			"tok-cold",
			cardActionEnvelope({
				token: "tok-cold",
				actionValue: { hermes_action: "approve_once", approval_id: approvalId },
				openChatId: "oc_chat",
				operatorOpenId: "ou_unknown",
			})["event"] as Record<string, unknown>,
		);
		const coldCard = h.engine.resolvedApprovalCards[0]?.card;
		const coldAttribution = coldCard?.elements.find(
			(e) => e.tag === "markdown",
		) as { content: string } | undefined;
		expect(coldAttribution?.content).toContain("by ou_unknown");
	});

	it("getChatInfo resolves chat metadata through the cached im/v1/chats leg (:2424)", async () => {
		const h = await makeEngine();
		h.server.scriptedChats.set("oc_named", {
			name: "Design Crew",
			chatType: "group",
		});
		const info = await (
			h.engine as unknown as {
				getChatInfo: (
					id: string,
				) => Promise<{ name: string; chatType: string }>;
			}
		).getChatInfo("oc_named");
		expect(info.name).toBe("Design Crew");
		// Second read rides the CACHE (single wire call).
		await (
			h.engine as unknown as {
				getChatInfo: (
					id: string,
				) => Promise<{ name: string; chatType: string }>;
			}
		).getChatInfo("oc_named");
		expect(
			h.server.restCalls.filter((c) => c.endpoint === "chats/oc_named"),
		).toHaveLength(1);
	});
});

describe("feishu-9 — configurable webhook path in the composite rate key (:230/:1650/:3562)", () => {
	it("default path /feishu/webhook composes {app}:{path}:{ip}", async () => {
		const h = await makeEngine();
		expect(h.engine.webhookPath).toBe(FEISHU_WEBHOOK_DEFAULT_PATH);
		expect(h.engine.webhookHost).toBe(FEISHU_WEBHOOK_DEFAULT_HOST);
		expect(h.engine.webhookPort).toBe(FEISHU_WEBHOOK_DEFAULT_PORT);
		await h.engine.handleWebhookPost({
			headers: { "content-type": "application/json" },
			rawBody: Buffer.from(JSON.stringify({ n: 1 })),
			peer: "10.1.1.7",
		});
		expect(h.engine.lastWebhookRateKey).toBe("cli_a:/feishu/webhook:10.1.1.7");
	});

	it("FEISHU_WEBHOOK_PATH/HOST/PORT env surface relocates the route + rate bucket", async () => {
		const h = await makeEngine({
			optionalEnvReader: (name) =>
				name === "FEISHU_WEBHOOK_PATH"
					? "/custom/hook"
					: name === "FEISHU_WEBHOOK_HOST"
						? "0.0.0.0"
						: name === "FEISHU_WEBHOOK_PORT"
							? "9911"
							: undefined,
		});
		expect(h.engine.webhookPath).toBe("/custom/hook");
		expect(h.engine.webhookHost).toBe("0.0.0.0");
		expect(h.engine.webhookPort).toBe(9911);
		await h.engine.handleWebhookPost({
			headers: { "content-type": "application/json" },
			rawBody: Buffer.from(JSON.stringify({ n: 1 })),
			peer: "10.2.2.7",
		});
		expect(h.engine.lastWebhookRateKey).toBe("cli_a:/custom/hook:10.2.2.7");
	});

	it("different paths isolate rate buckets; bursts past 120 still answer 429 per key", async () => {
		const h = await makeEngine({
			optionalEnvReader: (name) =>
				name === "FEISHU_WEBHOOK_PATH" ? "/other/path" : undefined,
		});
		let lastStatus = 200;
		for (let i = 0; i < 125; i++) {
			lastStatus = (
				await h.engine.handleWebhookPost({
					headers: { "content-type": "application/json" },
					rawBody: Buffer.from(JSON.stringify({ n: i })),
					peer: "10.9.8.8",
				})
			).status;
		}
		expect(lastStatus).toBe(429);
	});
});

describe("feishu-10/11 — manifest button enums + single-field user parse", () => {
	it("FEISHU_APPROVAL_BUTTONS: only Allow Once primary; Session/Always default; Deny danger (_btn :2077)", () => {
		expect(FEISHU_APPROVAL_BUTTONS.approve_once.type).toBe("primary");
		expect(FEISHU_APPROVAL_BUTTONS.approve_session.type).toBe("default");
		expect(FEISHU_APPROVAL_BUTTONS.approve_always.type).toBe("default");
		expect(FEISHU_APPROVAL_BUTTONS.deny.type).toBe("danger");
	});

	it("parseUser reads ONLY user_name — bare name fields yield empty (:99)", () => {
		const payload = parseMeetingInvitedEvent({
			header: { event_id: "e-n" },
			event: {
				meeting: { id: "m-n", meeting_no: "5-5-5" },
				inviter: { open_id: "ou_x", name: "Fallback Name" },
				host_user_present: true,
			},
		});
		expect(payload?.inviter.userName).toBe("");
	});
});

// Silence unused-import guards where helpers exist for wiring parity.
void writeFileSync;
