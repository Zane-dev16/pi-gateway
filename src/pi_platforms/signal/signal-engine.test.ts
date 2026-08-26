// pi_platforms/signal/signal-engine.test — REAL-engine behavior contracts for
// the Signal port: every scenario drives the actual SignalAdapter over
// FakeSignalCliServer (the in-process signal-cli double) with an INJECTED
// clock and mkdtemp isolation. Behavior contracts only — no change-detectors,
// no vendor-error-string snapshots.

import { describe, expect, it } from "vitest";
import {
	mkdtempSync,
	readFileSync,
	rmSync,
	truncateSync,
	writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import { ManualClock } from "../persistent-ws/manual-clock.js";
import { FakeSignalCliServer } from "./signal-wire.js";
import {
	HEALTH_CHECK_INTERVAL_MS,
	HEALTH_CHECK_STALE_THRESHOLD_MS,
	SIGNAL_MAX_ATTACHMENT_SIZE,
	SIGNAL_MAX_ATTACHMENTS_PER_MSG,
} from "./manifest.js";
import {
	extractRetryAfterSeconds,
	formatWait,
	isSignalRateLimitError,
	parseRetryAfterSeconds,
	SignalAttachmentScheduler,
	SignalRateLimitError,
	SignalSchedulerError,
	signalSendTimeout,
} from "./rate-limit.js";
import { guessExtension, extToMime } from "./media.js";
import { markdownToSignal } from "./signal-format.js";
import {
	type PostStreamAdapter,
	rescanPostStream,
} from "../../pi_gateway/outbound/post-stream-rescan.js";
import {
	SignalAdapter,
	isSignalServiceId,
	looksLikeE164Number,
	renderMentions,
	validateSendResult,
} from "./signal-adapter.js";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";

/** A full engine world: adapter + fake daemon + injected clock + tmp dirs. */
function makeWorld(
	opts: Partial<ConstructorParameters<typeof SignalAdapter>[0]> = {},
): {
	adapter: SignalAdapter;
	daemon: FakeSignalCliServer;
	clock: ManualClock;
	mediaDir: string;
	cleanup: () => void;
} {
	const clock = new ManualClock();
	const daemon = new FakeSignalCliServer();
	daemon.setClock(clock.nowMs);
	const mediaDir = mkdtempSync(join(tmpdir(), "signal-engine-"));
	const adapter = new SignalAdapter({
		transport: daemon,
		account: "+15550001111",
		// Engine worlds resolve secrets through a SCOPED reader (fail-closed).
		secretReader: (name) => {
			if (name === "SIGNAL_HTTP_URL") return "http://127.0.0.1:8080";
			if (name === "SIGNAL_ACCOUNT") return "+15550001111";
			return undefined;
		},
		nowMs: clock.nowMs,
		sleepMs: clock.sleepMs,
		rng: () => 0, // deterministic ladder: zero jitter component
		mediaCacheDir: join(mediaDir, "media"),
		...opts,
	});
	return {
		adapter,
		daemon,
		clock,
		mediaDir,
		cleanup: () => rmSync(mediaDir, { recursive: true, force: true }),
	};
}

/** Adapter whose guard captures inbound metadata instead of the canned reply. */
function makeCapturingWorld(): {
	adapter: SignalAdapter;
	daemon: FakeSignalCliServer;
	lastMetadata: () => Record<string, unknown> | null;
} {
	const w = makeWorld();
	let seen: Record<string, unknown> | null = null;
	w.adapter.attachGuard({
		registry: [],
		messageHandler: async (event) => {
			seen = { ...(event.metadata ?? {}) };
			return "ok";
		},
		sendReply: async () => {},
	});
	return {
		adapter: w.adapter,
		daemon: w.daemon,
		lastMetadata: () => seen,
	};
}

const DM_ENVELOPE = (
	text: string,
	overrides: Record<string, unknown> = {},
): Record<string, unknown> => ({
	envelope: {
		sourceNumber: "+15551234567",
		sourceUuid: "9d6fe03a-2b7c-4f21-a5de-9a3c1e83b7aa",
		sourceName: "Alice",
		timestamp: 1_700_000_000_123,
		dataMessage: { message: text, timestamp: 1_700_000_000_123 },
		...overrides,
	},
});

// ── envelope pipeline (_handle_envelope parity) ──────────────────────────────

describe("signal envelope pipeline", () => {
	it("accepts a plain DM: one turn, counters accept", async () => {
		const w = makeWorld();
		w.adapter.attachStandardGuard();
		await w.adapter.handleEnvelope(DM_ENVELOPE("hello gateway"));
		expect(w.adapter.turnLog).toEqual(["hello gateway"]);
		expect(w.adapter.counts.accepted).toBe(1);
	});

	it("swallows own-send sync echoes; promotes GENUINE Note-to-Self to a turn", async () => {
		const w = makeWorld();
		w.adapter.attachStandardGuard();

		// We sent ts=42 ourselves → the sync echo must be consumed.
		w.daemon.scriptRpcResult("send", { timestamp: 42, results: [] });
		await w.adapter.send("+15559998888", "we said this");
		await w.adapter.handleEnvelope({
			envelope: {
				syncMessage: {
					sentMessage: { destinationNumber: "+15550001111", timestamp: 42 },
				},
			},
		});
		expect(w.adapter.counts.echoSuppressed).toBe(1);
		expect(w.adapter.turnLog).toEqual([]);

		// Same destination but an UNKNOWN timestamp ⇒ genuine user Note-to-Self.
		// Faithful wire shape: linked-device sync envelopes carry OUR OWN source.
		await w.adapter.handleEnvelope({
			envelope: {
				sourceNumber: "+15550001111",
				sourceUuid: "44444444-4444-4444-8444-444444444444",
				syncMessage: {
					sentMessage: {
						destinationNumber: "+15550001111",
						timestamp: 77,
						message: "note to self",
					},
				},
			},
		});
		expect(w.adapter.counts.noteToSelfPromoted).toBe(1);
		expect(w.adapter.turnLog).toEqual(["note to self"]);
	});

	it("filters: self messages, stories, sender-less envelopes, non-data sync events", async () => {
		const w = makeWorld();
		w.adapter.attachStandardGuard();
		await w.adapter.handleEnvelope({
			envelope: {
				sourceNumber: "+15550001111",
				dataMessage: { message: "from myself" },
			},
		});
		await w.adapter.handleEnvelope({
			envelope: { sourceNumber: "+15551234567", storyMessage: {} },
		});
		await w.adapter.handleEnvelope({ envelope: { sourceName: "ghost" } });
		await w.adapter.handleEnvelope({
			// Non-sentMessage sync events return EARLY (quietly filtered) without
			// reaching the dataMessage check (_handle_envelope sync leg parity).
			envelope: { sourceNumber: "+15551234567", syncMessage: { read: [] } },
		});
		await w.adapter.handleEnvelope({
			envelope: { sourceNumber: "+15551234567", editMessage: {} },
		});
		expect(w.adapter.counts.selfMessage).toBe(1);
		expect(w.adapter.counts.storyFiltered).toBe(1);
		expect(w.adapter.counts.noSender).toBe(1);
		expect(w.adapter.counts.noDataMessage).toBe(1); // the empty editMessage
		expect(w.adapter.turnLog).toEqual([]);
	});

	it("editMessage envelopes unwrap their inner dataMessage", async () => {
		const w = makeWorld();
		w.adapter.attachStandardGuard();
		await w.adapter.handleEnvelope({
			envelope: {
				sourceNumber: "+15551234567",
				editMessage: {
					targetTimestamp: 55,
					dataMessage: { message: "the EDITED body" },
				},
			},
		});
		expect(w.adapter.turnLog).toEqual(["the EDITED body"]);
	});

	it("groups: disabled by default; '*' opens all; explicit ids gate membership", async () => {
		const closed = makeWorld();
		closed.adapter.attachStandardGuard();
		await closed.adapter.handleEnvelope(
			DM_ENVELOPE("ignored", {
				dataMessage: {
					message: "ignored",
					groupInfo: { groupId: "abc123", groupName: "Test Group" },
				},
			}),
		);
		expect(closed.adapter.counts.groupDisabled).toBe(1);
		expect(closed.adapter.turnLog).toEqual([]);

		const open = makeWorld({ groupAllowFrom: ["*"] });
		open.adapter.attachStandardGuard();
		await open.adapter.handleEnvelope(
			DM_ENVELOPE("ignored2", {
				dataMessage: {
					message: "open group msg",
					groupInfo: { groupId: "abc123" },
				},
			}),
		);
		expect(open.adapter.counts.groupNotAllowed).toBe(0);
		expect(open.adapter.turnLog).toEqual(["open group msg"]);

		const gated = makeWorld({ groupAllowFrom: ["zzz999"] });
		gated.adapter.attachStandardGuard();
		await gated.adapter.handleEnvelope(
			DM_ENVELOPE("ignored3", {
				dataMessage: {
					message: "not my group",
					groupInfo: { groupId: "abc123" },
				},
			}),
		);
		expect(gated.adapter.counts.groupNotAllowed).toBe(1);
		expect(gated.adapter.turnLog).toEqual([]);
	});

	it("require_mention gates groups on rendered text or mention metadata; self-mention stripped", async () => {
		const w = makeWorld({ requireMention: true, groupAllowFrom: ["*"] });
		w.adapter.attachStandardGuard();

		// No mention anywhere → dropped.
		await w.adapter.handleEnvelope({
			envelope: {
				sourceNumber: "+15551234567",
				timestamp: 1,
				dataMessage: {
					message: "no invite for the bot",
					groupInfo: { groupId: "g1" },
					mentions: [],
				},
			},
		});
		expect(w.adapter.counts.mentionRequired).toBe(1);

		// Mention via METADATA (number match) → accepted; the bot's own
		// @+15550001111 tag is STRIPPED from the delivered text.
		await w.adapter.handleEnvelope({
			envelope: {
				sourceNumber: "+15551234567",
				timestamp: 2,
				dataMessage: {
					message: "\uFFFC what's up",
					groupInfo: { groupId: "g1" },
					mentions: [{ number: "+15550001111", start: 0, length: 1 }],
				},
			},
		});
		expect(w.adapter.turnLog.length).toBe(1);
		expect(w.adapter.turnLog[0]).not.toContain("@+15550001111");
		expect(w.adapter.turnLog[0]).toContain("what's up");

		// Mention via RENDERED placeholder + METADATA uuid match → accepted;
		// the rendered @<bot-uuid> tag is stripped via the recipient map.
		const uuidBot = "11111111-2222-4333-8444-555555555555";
		const w2 = makeWorld({ groupAllowFrom: ["*"] });
		w2.adapter.attachStandardGuard();
		w2.adapter.noteRecipient("+15550001111", uuidBot);
		await w2.adapter.handleEnvelope({
			envelope: {
				sourceNumber: "+15551234567",
				timestamp: 3,
				dataMessage: {
					message: "\uFFFC ping",
					groupInfo: { groupId: "g1" },
					mentions: [{ uuid: uuidBot, start: 0, length: 1 }],
				},
			},
		});
		expect(w2.adapter.turnLog.length).toBe(1);
		expect(w2.adapter.turnLog[0]).not.toContain(uuidBot);
		expect(w2.adapter.turnLog[0]).toContain("ping");
	});

	it("contentless dataMessages (profile-key updates) never become turns", async () => {
		const w = makeWorld();
		w.adapter.attachStandardGuard();
		await w.adapter.handleEnvelope({
			envelope: {
				sourceNumber: "+15551234567",
				dataMessage: { message: "" },
			},
		});
		expect(w.adapter.counts.contentless).toBe(1);
		expect(w.adapter.turnLog).toEqual([]);
	});

	it("quote metadata resolves reply context and detects OWN-message quotes", async () => {
		const w = makeCapturingWorld();
		// Record that we sent ts=9001 (the quote target).
		w.daemon.scriptRpcResult("send", { timestamp: 9001, results: [] });
		await w.adapter.send("+15559998888", "original words");
		await w.adapter.handleEnvelope({
			envelope: {
				sourceNumber: "+15551234567",
				dataMessage: {
					message: "replying to you",
					quote: {
						id: 9001,
						text: "original words",
						author: "+15550001111",
					},
				},
			},
		});
		const md = w.lastMetadata();
		expect(md?.["reply_to_is_own_message"]).toBe(true);
		expect(md?.["reply_to_text"]).toBe("original words");
		expect(md?.["reply_to_author_id"]).toBe("+15550001111");

		// Someone else's quote is NOT ours.
		const w2 = makeCapturingWorld();
		await w2.adapter.handleEnvelope({
			envelope: {
				sourceNumber: "+15551234567",
				dataMessage: {
					message: "replying to alice",
					quote: { id: 4242, author: "+15550009999" },
				},
			},
		});
		expect(w2.lastMetadata()?.["reply_to_is_own_message"]).toBe(false);
	});

	it("attachments fetch over getAttachment, cache by sniffed extension, classify message type", async () => {
		const pngBytes = Buffer.from([
			0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a, 0x1a, 0x0a,
		]);
		const w = makeWorld();
		const attId = w.daemon.seedAttachment(pngBytes);
		let seenType: string | undefined;
		let seenUrls: string[] | undefined;
		w.adapter.attachGuard({
			registry: [],
			messageHandler: async (event) => {
				seenType = event.messageType;
				seenUrls = event.mediaUrls;
				return "ok";
			},
			sendReply: async () => {},
		});
		await w.adapter.handleEnvelope({
			envelope: {
				sourceNumber: "+15551234567",
				dataMessage: {
					message: "",
					attachments: [
						{ id: attId, size: pngBytes.length, contentType: "image/png" },
					],
				},
			},
		});
		expect(seenType).toBe("photo");
		expect(seenUrls?.length).toBe(1);
		expect(w.daemon.callsOf("getAttachment").length).toBe(1);
		const cachedPath = join(w.mediaDir, "media", `${attId}.png`);
		expect(readFileSync(cachedPath)).toEqual(pngBytes);

		// Oversize attachments are skipped BEFORE any RPC.
		const before = w.daemon.callsOf("getAttachment").length;
		await w.adapter.handleEnvelope({
			envelope: {
				sourceNumber: "+15551234567",
				dataMessage: {
					message: "",
					attachments: [{ id: "x", size: 101 * 1024 * 1024 }],
				},
			},
		});
		expect(w.daemon.callsOf("getAttachment").length).toBe(before);
		expect(w.adapter.counts.contentless).toBe(1);
	});
});

// ── send wire shapes ─────────────────────────────────────────────────────────

describe("signal send wire shapes", () => {
	it("converts markdown to native styles: single style ⇒ textStyle, many ⇒ textStyles", async () => {
		const w = makeWorld();
		const r1 = await w.adapter.send("+15551234567", "**bold** day");
		expect(r1.success).toBe(true);
		expect(r1.messageId).toBeNull(); // NO editable identity — deliberate
		let calls = w.daemon.callsOf("send");
		expect(calls[0]?.params["message"]).toBe("bold day");
		expect(calls[0]?.params["textStyle"]).toBe("0:4:BOLD");
		expect(calls[0]?.params["textStyles"]).toBeUndefined();

		await w.adapter.send("+15551234567", "**bold** and `code` too");
		calls = w.daemon.callsOf("send");
		const styles = calls[1]?.params["textStyles"];
		expect(Array.isArray(styles)).toBe(true);
		expect(styles as string[]).toContain("0:4:BOLD");
		expect(styles as string[]).toContain("9:4:MONOSPACE");
	});

	it("addresses groups by stripped id; upgrades phone recipients via listContacts once", async () => {
		const w = makeWorld();
		await w.adapter.send("group:abc123==", "to the group");
		expect(w.daemon.callsOf("send")[0]?.params["groupId"]).toBe("abc123==");
		expect(w.daemon.callsOf("send")[0]?.params["recipient"]).toBeUndefined();

		// Unknown E.164 passes through verbatim when listContacts knows nothing.
		// ONE send resolves its recipient TWICE (stop-typing leg + wire leg);
		// both legs miss on an unknown number ⇒ TWO discovery RPCs (Hermes
		// _resolve_recipient has no negative cache either).
		await w.adapter.send("+15551112222", "dm passthrough");
		expect(w.daemon.callsOf("send")[1]?.params["recipient"]).toEqual([
			"+15551112222",
		]);
		expect(w.daemon.callsOf("listContacts").length).toBe(2);

		// A contact row with a service-id upgrades the SAME chat id forever
		// after. First resolution leg discovers + caches; second leg hits it.
		w.daemon.setContacts([
			{ number: "+15553334444", uuid: "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee" },
		]);
		await w.adapter.send("+15553334444", "upgrade please");
		expect(w.daemon.callsOf("listContacts").length).toBe(3);
		await w.adapter.send("+15553334444", "cached now");
		const sends = w.daemon.callsOf("send");
		expect(sends[2]?.params["recipient"]).toEqual([
			"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		]);
		expect(sends[3]?.params["recipient"]).toEqual([
			"aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee",
		]);
		// The cache holds — NO further discovery RPCs once populated.
		expect(w.daemon.callsOf("listContacts").length).toBe(3);
	});

	it("typed results failures fail the send with their shape (never snapshot strings)", async () => {
		const w = makeWorld();
		w.daemon.scriptRpcResult("send", {
			results: [{ type: "NETWORK_FAILURE" }],
		});
		const r1 = await w.adapter.send("+15551234567", "unreachable");
		expect(r1.success).toBe(false);
		expect(r1.error).toBe("NETWORK_FAILURE");

		w.daemon.scriptRpcResult("send", { results: [{ success: false }] });
		const r2 = await w.adapter.send("+15551234567", "unspecified failure");
		expect(r2.success).toBe(false);

		w.daemon.scriptRpcFailure("send", { code: -1, message: "boom" });
		const r3 = await w.adapter.send("+15551234567", "rpc error");
		expect(r3.success).toBe(false);
	});

	it("outbound timestamps feed the Note-to-Self echo filter end-to-end", async () => {
		const w = makeWorld();
		w.adapter.attachStandardGuard();
		await w.adapter.send("+15551234567", "we said this");
		const sendCall = w.daemon.callsOf("send")[0];
		const sendTs = Number(
			(sendCall?.result as Record<string, unknown> | undefined)?.["timestamp"],
		);
		await w.adapter.handleEnvelope({
			envelope: {
				syncMessage: {
					sentMessage: {
						// Sync-sent echoes address OUR OWN account.
						destinationNumber: "+15550001111",
						timestamp: sendTs,
					},
				},
			},
		});
		expect(w.adapter.counts.echoSuppressed).toBe(1);
		expect(w.adapter.turnLog).toEqual([]);
	});
});

// ── typing breaker ladder (A24 shape) ────────────────────────────────────────

describe("typing breaker ladder", () => {
	it("≥3 consecutive failures arm an exponential skip window saturating at 60s; success clears", async () => {
		const w = makeWorld();
		for (let i = 0; i < 5; i++) {
			w.daemon.scriptRpcFailure("sendTyping", {
				code: -1,
				message: "NETWORK_FAILURE",
			});
		}
		await w.adapter.sendTypingSignal("chat-t");
		await w.adapter.sendTypingSignal("chat-t");
		await w.adapter.sendTypingSignal("chat-t"); // fails=3 → window 16s
		expect(w.daemon.callsOf("sendTyping").length).toBe(3);
		await w.adapter.sendTypingSignal("chat-t"); // SKIPPED inside window
		expect(w.daemon.callsOf("sendTyping").length).toBe(3);

		await w.clock.advance(16_000); // window expired → tries → fails=4 → 32s
		await w.adapter.sendTypingSignal("chat-t");
		expect(w.daemon.callsOf("sendTyping").length).toBe(4);
		await w.adapter.sendTypingSignal("chat-t"); // skipped within 32s
		expect(w.daemon.callsOf("sendTyping").length).toBe(4);
		await w.clock.advance(32_000); // fails=5 → min(60, 16·4)=60s
		await w.adapter.sendTypingSignal("chat-t");
		expect(w.daemon.callsOf("sendTyping").length).toBe(5);

		// Success clears breaker state entirely.
		await w.clock.advance(60_000);
		await w.adapter.sendTypingSignal("chat-t"); // succeeds (script drained)
		expect(w.daemon.callsOf("sendTyping").length).toBe(6);
		await w.adapter.sendTypingSignal("chat-t");
		expect(w.daemon.callsOf("sendTyping").length).toBe(7);
	});

	it("stop-typing sends stop:true and clears breaker state even when the RPC fails", async () => {
		const w = makeWorld();
		for (let i = 0; i < 5; i++) {
			w.daemon.scriptRpcFailure("sendTyping", {
				code: -1,
				message: i < 3 ? "NETWORK_FAILURE" : "stop also fails",
			});
		}
		await w.adapter.sendTypingSignal("chat-s");
		await w.adapter.sendTypingSignal("chat-s");
		await w.adapter.sendTypingSignal("chat-s"); // breaker armed

		await w.adapter.stopTypingIndicator("chat-s");
		const stopCall = w.daemon
			.callsOf("sendTyping")
			.find((c) => c.params["stop"] === true);
		expect(stopCall).toBeDefined();
		// Breaker cleared: the NEXT typing attempt goes straight out.
		await w.adapter.sendTypingSignal("chat-s");
		const attempts = w.daemon
			.callsOf("sendTyping")
			.filter((c) => c.params["stop"] !== true && c.id === "typing").length;
		expect(attempts).toBe(4);
	});
});

// ── reaction lifecycle ───────────────────────────────────────────────────────

function eventWithTarget(): IncomingEvent {
	return {
		messageType: "text",
		text: "do the thing",
		metadata: {
			signal_reaction_target: {
				author: "+15551234567",
				timestamp: 1700000000123,
			},
		},
		source: {
			platform: "signal",
			chatType: "dm",
			userId: "+15551234567",
			chatId: "+15551234567",
		},
	};
}

describe("reaction lifecycle", () => {
	it("start reacts 👀; success swaps to ✅ via remove+add; CANCELLED leaves 👀", async () => {
		const w = makeWorld();
		const ev = eventWithTarget();
		await w.adapter.onProcessingStart(ev);
		await w.adapter.onProcessingComplete(ev, "success");
		const reactions = w.daemon.callsOf("sendReaction");
		expect(reactions[0]?.params["emoji"]).toBe("👀");
		// remove (empty emoji + remove:true) then the final ✅
		expect(reactions[1]?.params["emoji"]).toBe("");
		expect(reactions[1]?.params["remove"]).toBe(true);
		expect(reactions[2]?.params["emoji"]).toBe("✅");

		const w2 = makeWorld();
		const ev2 = eventWithTarget();
		await w2.adapter.onProcessingStart(ev2);
		await w2.adapter.onProcessingComplete(ev2, "cancelled");
		expect(w2.daemon.callsOf("sendReaction")).toHaveLength(1); // 👀 stays
	});

	it("gates: global off-switch AND DM allowlist suppress every reaction", async () => {
		const gatedOff = makeWorld({ reactionsEnabled: false });
		await gatedOff.adapter.onProcessingStart(eventWithTarget());
		expect(gatedOff.daemon.callsOf("sendReaction")).toHaveLength(0);

		const allowlisted = makeWorld({ dmAllowFrom: ["*"] });
		await allowlisted.adapter.onProcessingStart(eventWithTarget());
		expect(allowlisted.daemon.callsOf("sendReaction")).toHaveLength(1);

		const restricted = makeWorld({ dmAllowFrom: ["+15551110000"] });
		await restricted.adapter.onProcessingStart(eventWithTarget());
		expect(restricted.daemon.callsOf("sendReaction")).toHaveLength(0);
	});
});

// ── SSE reconnect ladder + health monitor ────────────────────────────────────

/** Deterministic wait-for predicate (tiny wall budget; no timing asserts). */
async function eventually(
	predicate: () => boolean,
	timeoutMs = 2_000,
): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	for (;;) {
		if (predicate()) return;
		if (Date.now() > deadline) throw new Error("eventually: condition not met");
		await new Promise<void>((r) => setTimeout(r, 4));
	}
}

/** One macrotask yield — lets started async chains register clock waits. */
function yieldMacrotask(): Promise<void> {
	return new Promise((r) => setImmediate(r));
}

describe("SSE reconnect ladder + health monitor", () => {
	it("ladder ESCALATES ×2 while the daemon is down and RESETS on healthy connect", async () => {
		const w = makeWorld();
		// Phase 1 — daemon unreachable: every open attempt throws; the ladder
		// doubles 2s→4s→8s with NO connection resetting it.
		w.daemon.refuseEventConnections(true);
		void w.adapter.connect({ isReconnect: false });
		await eventually(() => w.adapter.reconnectLog.length >= 1);
		await w.clock.advance(2_000);
		await eventually(() => w.adapter.reconnectLog.length >= 2);
		await w.clock.advance(4_000);
		await eventually(() => w.adapter.reconnectLog.length >= 3);
		await w.clock.advance(8_000);
		await eventually(() => w.adapter.reconnectLog.length >= 4);
		expect(w.adapter.reconnectLog.slice(0, 4).map((s) => s.delayMs)).toEqual([
			2000, 4000, 8000, 16000,
		]);
		expect(w.daemon.connectionLog.length).toBe(0); // never connected

		// Phase 2 — daemon returns: the NEXT attempt succeeds and the ladder
		// RESETS (a later single blip waits the INITIAL delay again).
		w.daemon.refuseEventConnections(false);
		await w.clock.advance(16_000);
		await eventually(() => w.daemon.hasLiveStream);
		w.daemon.dropStream("healthy-life blip");
		await eventually(() => w.adapter.reconnectLog.length >= 5);
		await w.clock.advance(2_000);
		await eventually(() => w.daemon.hasLiveStream);
		expect(
			w.adapter.reconnectLog[w.adapter.reconnectLog.length - 1]?.delayMs,
		).toBe(2000);
		await w.adapter.disconnect();
	});

	it("backlog flushes after reconnect: zero loss, exactly-once downstream", async () => {
		const w = makeWorld();
		w.adapter.attachStandardGuard();
		await w.adapter.connect({ isReconnect: false });
		await eventually(() => w.daemon.hasLiveStream);

		w.daemon.pushEvent(DM_ENVELOPE("before outage"));
		await new Promise<void>((r) => setTimeout(r, 10));
		// Emitted while detached → the daemon holds them in its backlog.
		w.daemon.dropStream("outage mid-life");
		await eventually(() => w.adapter.reconnectLog.length >= 1); // sleep armed
		w.daemon.pushEvent(DM_ENVELOPE("held 1"));
		w.daemon.pushEvent(DM_ENVELOPE("held 2"));
		expect(w.daemon.backlogDepth).toBe(2);

		await w.clock.advance(2_000); // ladder sleep → reopen → backlog first
		await eventually(() => w.daemon.hasLiveStream);
		w.daemon.pushEvent(DM_ENVELOPE("after reconnect"));
		await eventually(() => w.adapter.counts.accepted >= 4);

		// Exactly-once downstream: every envelope ACCEPTED exactly once; the
		// guard may coalesce TURNS under burst arrival, never drop/redup events.
		const joined = w.adapter.turnLog.join("\n");
		for (const t of ["before outage", "held 1", "held 2", "after reconnect"]) {
			expect(joined.includes(t)).toBe(true);
		}
		expect(w.adapter.counts.accepted).toBe(4);
		await w.adapter.disconnect();
	});

	it("keepalives refresh activity; stale silence + dead daemon forces reconnect", async () => {
		const w = makeWorld();
		await w.adapter.connect({ isReconnect: false });
		await eventually(() => w.daemon.hasLiveStream);
		for (let i = 0; i < 10; i++) {
			w.daemon.pushKeepalive();
			await w.clock.advance(HEALTH_CHECK_INTERVAL_MS);
		}
		// Activity stayed fresh across ~300s of keepalives.
		expect(w.adapter.forcedReconnects).toHaveLength(0);

		// Silence past the stale threshold while the daemon is DOWN.
		w.daemon.setHealth(false);
		await w.clock.advance(
			HEALTH_CHECK_STALE_THRESHOLD_MS + HEALTH_CHECK_INTERVAL_MS * 2,
		);
		expect(w.adapter.forcedReconnects).toHaveLength(1);
		expect(w.adapter.forcedReconnects[0]?.reason).toContain("stale");

		// Daemon returns: the next tick finds it alive — no repeat force.
		w.daemon.setHealth(true);
		await w.clock.advance(HEALTH_CHECK_INTERVAL_MS * 6);
		expect(w.adapter.forcedReconnects).toHaveLength(1);
		await w.adapter.disconnect();
	});

	it("every stream open is ACCOUNT-SCOPED: ?account= rides connect AND reconnects", async () => {
		const w = makeWorld();
		await w.adapter.connect({ isReconnect: false });
		await eventually(() => w.daemon.hasLiveStream);
		expect(w.daemon.connectionLog.length).toBe(1);
		expect(w.daemon.connectionLog[0]?.account).toBe("+15550001111");

		// The reconnect ladder re-opens with the SAME account (signal-cli routes
		// the event stream BY the query parameter — a bare GET would subscribe to
		// whatever default account the daemon picks).
		w.daemon.dropStream("forced outage");
		await eventually(() => w.adapter.reconnectLog.length >= 1);
		await w.clock.advance(4_000);
		await eventually(() => w.daemon.hasLiveStream);
		const last = w.daemon.connectionLog[w.daemon.connectionLog.length - 1];
		expect(last?.account).toBe("+15550001111");
		await w.adapter.disconnect();
	});
});

// ── batch sends through the rate-limit scheduler ─────────────────────────────

describe("batch attachment sends paced by the scheduler", () => {
	it("splits at 32/batch and consults the scheduler per batch (full bucket ⇒ zero waits)", async () => {
		const w = makeWorld();
		try {
			// Batch lane stat-gates every entry (sigbb-8) — paths must EXIST.
			const paths = Array.from({ length: 40 }, (_, i) => {
				const p = join(w.mediaDir, `img-${i}.png`);
				writeFileSync(p, Buffer.from("png-bytes"));
				return p;
			});
			const notices: string[] = [];
			const outcomes = await w.adapter.batchSendAttachments(
				"chat-batch",
				paths,
				{
					notify: async (t) => {
						notices.push(t);
					},
				},
			);
			expect(outcomes.map((o) => o.success)).toEqual([true, true]);
			const sends = w.daemon.callsOf("send");
			expect(sends).toHaveLength(2);
			const firstBatch = sends[0]?.params["attachments"];
			const secondBatch = sends[1]?.params["attachments"];
			expect(Array.isArray(firstBatch)).toBe(true);
			expect(Array.isArray(secondBatch)).toBe(true);
			expect((firstBatch as string[]).length).toBe(
				SIGNAL_MAX_ATTACHMENTS_PER_MSG,
			);
			expect((secondBatch as string[]).length).toBe(8);
			// Scaled send budget rides EVERY batch RPC (sigbb-3): 5s/attachment
			// with a 60s floor (_signal_send_timeout parity).
			expect(sends[0]?.timeoutMs).toBe(160_000); // 32 × 5s
			expect(sends[1]?.timeoutMs).toBe(60_000); // 8 × 5s = 40s → 60s floor
			expect(notices).toHaveLength(0);
		} finally {
			w.cleanup();
		}
	});

	it("rate-limited batches retry ONCE with server feedback recalibrating the bucket", async () => {
		// One SHARED injected clock paces BOTH the adapter sleeps and the
		// scheduler bucket — a single deterministic advance drives everything.
		const clock = new ManualClock();
		const daemon = new FakeSignalCliServer();
		daemon.setClock(clock.nowMs);
		const scheduler = new SignalAttachmentScheduler({
			clock: { nowMs: clock.nowMs, sleepMs: clock.sleepMs },
		});
		const mediaDir = mkdtempSync(join(tmpdir(), "signal-engine-"));
		const adapter = new SignalAdapter({
			transport: daemon,
			account: "+15550001111",
			secretReader: (name) =>
				name === "SIGNAL_HTTP_URL"
					? "http://127.0.0.1:8080"
					: name === "SIGNAL_ACCOUNT"
						? "+15550001111"
						: undefined,
			nowMs: clock.nowMs,
			sleepMs: clock.sleepMs,
			rng: () => 0,
			mediaCacheDir: join(mediaDir, "media"),
			scheduler,
		});
		try {
			// Drain the modeled bucket so acquire() actually paces.
			await scheduler.reportRpcDuration(0, 50);

			// Attempt 1: RATE_LIMIT_FAILURE result WITH structured retryAfterSeconds;
			// attempt 2 (post-feedback) succeeds — exactly one success, two calls.
			daemon.scriptRpcResult("send", {
				timestamp: 1,
				results: [{ type: "RATE_LIMIT_FAILURE", retryAfterSeconds: 7 }],
			});

			// Batch entries are stat-gated — real files on disk (sigbb-8).
			const pa = join(mediaDir, "a.png");
			const pb = join(mediaDir, "b.png");
			writeFileSync(pa, Buffer.from("a"));
			writeFileSync(pb, Buffer.from("b"));
			const outcomesP = adapter.batchSendAttachments("chat-rl", [pa, pb]);
			// Let the chain reach its FIRST paced acquire (8s default-rate wait).
			await eventually(() => clock.pendingWaits > 0);
			await clock.advance(30_000);
			const outcomes = await outcomesP;

			expect(outcomes.map((o) => o.success)).toEqual([true]);
			const rlSends = daemon.callsOf("send");
			expect(rlSends.length).toBe(2);
			// BOTH attempts carry the scaled budget for n=2 (60s floor).
			for (const call of rlSends) {
				expect(call.timeoutMs).toBe(60_000);
			}
			// Server feedback WAS authoritative: seconds-per-token recalibrated to 7.
			expect(scheduler.state().refillSecondsPerToken).toBe(7);
		} finally {
			rmSync(mediaDir, { recursive: true, force: true });
		}
	});

	it("batch lane stat-gates missing and oversize entries BEFORE slicing batches", async () => {
		const w = makeWorld();
		try {
			const v1 = join(w.mediaDir, "a.png");
			writeFileSync(v1, Buffer.from("a"));
			const ghost = join(w.mediaDir, "ghost.png"); // never created
			const v2 = join(w.mediaDir, "b.png");
			writeFileSync(v2, Buffer.from("b"));
			// Sparse oversize entry: size past SIGNAL_MAX_ATTACHMENT_SIZE without
			// materializing 100 MB (send_multiple_images oversize-skip parity).
			const huge = join(w.mediaDir, "huge.png");
			writeFileSync(huge, Buffer.from("PNG"));
			truncateSync(huge, SIGNAL_MAX_ATTACHMENT_SIZE + 1);

			const outcomes = await w.adapter.batchSendAttachments("chat-gate", [
				v1,
				ghost,
				v2,
				huge,
			]);
			// One bad URL must not lose the rest of the batch: exactly ONE rpc
			// carrying ONLY the valid entries.
			expect(outcomes.map((o) => o.success)).toEqual([true]);
			const sends = w.daemon.callsOf("send");
			expect(sends).toHaveLength(1);
			expect(sends[0]?.params["attachments"]).toEqual([v1, v2]);

			// ALL entries invalid ⇒ zero RPCs, zero outcomes.
			w.daemon.rpcCalls.length = 0;
			const none = await w.adapter.batchSendAttachments("chat-gate", [
				ghost,
				huge,
			]);
			expect(none).toEqual([]);
			expect(w.daemon.callsOf("send")).toHaveLength(0);
		} finally {
			w.cleanup();
		}
	});
});

// ── rate-limit module contracts (mutation-checked shapes) ───────────────────

// ── converted-plan splitting (sigbb-4: convert WHOLE, then split) ────────────

describe("deliverText converts whole message first, splits converted text", () => {
	it("a style crossing NO boundary lands chunk-local; labels ride every chunk; no ** leaks", async () => {
		const w = makeWorld({ scalarMaxUnits: 40 });
		const content = `start ${"x".repeat(30)} **bold** ${"y".repeat(30)} end`;
		// Converted (markers stripped) — the splitter walks THIS text:
		const converted = `start ${"x".repeat(30)} bold ${"y".repeat(30)} end`;
		expect(markdownToSignal(content)).toEqual([converted, ["37:4:BOLD"]]);

		const results = await w.adapter.deliverText("chat-split", content);
		expect(results.every((r) => r.success)).toBe(true);

		const sends = w.daemon.callsOf("send");
		expect(sends).toHaveLength(3);
		// BYTE-EXACT plan shape (DEC-047 convention, converted-plan form):
		// 30-unit body budget + Hermes' " (i/n)" label.
		const expected = [
			`${converted.slice(0, 30)} (1/3)`,
			`${converted.slice(30, 60)} (2/3)`,
			`${converted.slice(60)} (3/3)`,
		];
		sends.forEach((call, i) => {
			expect(call.params["message"]).toBe(expected[i]);
		});
		// The bold range [37,41) lives in chunk 2 ([30,60)) → re-anchored to
		// [7,11); chunks 1/3 carry no ranges; ZERO literal markers anywhere.
		expect(sends[0]?.params["textStyle"]).toBeUndefined();
		expect(sends[0]?.params["textStyles"]).toBeUndefined();
		expect(sends[1]?.params["textStyle"]).toBe("7:4:BOLD");
		expect(sends[2]?.params["textStyle"]).toBeUndefined();
	});

	it("a style SPANNING the cut is clipped into EVERY overlapping chunk", async () => {
		const w = makeWorld({ scalarMaxUnits: 13 }); // 16-unit text ⇒ six 3-unit bodies
		const content = "ab**0123456789**tail";
		const results = await w.adapter.deliverText("chat-span", content);
		expect(results.every((r) => r.success)).toBe(true);

		const sends = w.daemon.callsOf("send");
		expect(sends).toHaveLength(6);
		const bodies = ["ab0", "123", "456", "789", "tai", "l"];
		sends.forEach((call, i) => {
			expect(call.params["message"]).toBe(`${bodies[i]} (${i + 1}/6)`);
		});
		// Bold source range [2,12): clipped into chunks 1–4, absent from 5–6.
		expect(sends[0]?.params["textStyle"]).toBe("2:1:BOLD");
		expect(sends[1]?.params["textStyle"]).toBe("0:3:BOLD");
		expect(sends[2]?.params["textStyle"]).toBe("0:3:BOLD");
		expect(sends[3]?.params["textStyle"]).toBe("0:3:BOLD");
		expect(sends[4]?.params["textStyle"]).toBeUndefined();
		expect(sends[5]?.params["textStyle"]).toBeUndefined();
	});

	it("short content stays ONE chunk with its full global style set", async () => {
		const w = makeWorld({ scalarMaxUnits: 8000 });
		await w.adapter.deliverText("chat-one", "**bold** intro");
		const sends = w.daemon.callsOf("send");
		expect(sends).toHaveLength(1);
		expect(sends[0]?.params["message"]).toBe("bold intro");
		expect(sends[0]?.params["textStyle"]).toBe("0:4:BOLD");
	});
});

describe("rate-limit module contracts", () => {
	const frozenScheduler = () =>
		new SignalAttachmentScheduler({
			clock: { nowMs: () => 0, sleepMs: async () => {} },
		});

	it("acquire is a READ-ONLY model: tokens deduct ONLY in reportRpcDuration", async () => {
		const s = frozenScheduler();
		await s.acquire(50);
		expect(s.tokens).toBe(50);
		await s.reportRpcDuration(0.1, 20);
		expect(s.tokens).toBe(30);
	});

	it("acquire blocks until refill covers the deficit; n>capacity refuses loudly", async () => {
		const clock = new ManualClock();
		const s = new SignalAttachmentScheduler({
			clock: { nowMs: clock.nowMs, sleepMs: clock.sleepMs },
		});
		await s.reportRpcDuration(0, 50); // drain
		let done = false;
		const acquireP = s.acquire(2).then(() => {
			done = true;
		}); // 2 tokens @ 0.25/s ⇒ 8s
		await yieldMacrotask(); // let acquire register its clock wait
		await clock.advance(7_999);
		expect(done).toBe(false);
		await clock.advance(1);
		await acquireP;
		expect(done).toBe(true);

		const fat = frozenScheduler();
		await expect(fat.acquire(51)).rejects.toBeInstanceOf(SignalSchedulerError);
	});

	it("reportRpcDuration deducts WITHOUT crediting upload-window refill", async () => {
		let n = 0;
		const s = new SignalAttachmentScheduler({
			clock: {
				nowMs: () => {
					n += 100_000; // 100s elapse per read (upload)
					return n;
				},
				sleepMs: async () => {},
			},
		});
		s.tokens = 40;
		await s.reportRpcDuration(100, 10);
		expect(s.tokens).toBe(30); // refill during upload NOT credited
	});

	it("feedback treats the server hint as AUTHORITATIVE calibration", () => {
		const s = frozenScheduler();
		expect(s.state().refillSecondsPerToken).toBe(4);
		s.feedback(7, 3);
		expect(s.state().refillSecondsPerToken).toBe(7);
		expect(s.tokens).toBe(0);
		s.feedback(null, 3); // null ⇒ drain only, rate unchanged
		expect(s.state().refillSecondsPerToken).toBe(7);
	});

	it("estimate_wait projects deficit/refill; state() is read-only", () => {
		const s = frozenScheduler();
		s.tokens = 48;
		expect(s.estimateWait(49)).toBeCloseTo(4, 5);
		expect(s.estimateWait(10)).toBe(0);
		const before = JSON.stringify(s.state());
		void s.state();
		expect(JSON.stringify(s.state())).toBe(before);
	});

	it("detection helpers cover ALL THREE vendor error shapes", () => {
		expect(
			isSignalRateLimitError({ code: -5, message: "RateLimitException" }),
		).toBe(true);
		expect(isSignalRateLimitError({ message: "[429] slow down" })).toBe(true);
		expect(
			isSignalRateLimitError({
				message: "AttachmentInvalidException wrapping RetryLaterException",
			}),
		).toBe(true);
		expect(
			isSignalRateLimitError({ message: "Please RETRY AFTER 4 seconds" }),
		).toBe(true); // case-insensitive substring parity (loose upstream too)
		expect(
			isSignalRateLimitError({ code: -1, message: "NETWORK_FAILURE" }),
		).toBe(false);
	});

	it("retry_after extraction: structured MAX wins, then the message-string source", () => {
		expect(
			extractRetryAfterSeconds({
				code: -5,
				message: "RateLimitException",
				data: {
					response: {
						results: [{ retryAfterSeconds: 3 }, { retryAfterSeconds: 9 }],
					},
				},
			}),
		).toBe(9);
		expect(
			extractRetryAfterSeconds({
				message:
					"AttachmentInvalidException: io exception: Retry after 12 seconds",
			}),
		).toBe(12);
		expect(extractRetryAfterSeconds({ message: "nothing here" })).toBeNull();
	});

	it("pacing helpers: formatWait buckets and scaled send timeouts", () => {
		expect(formatWait(9.4)).toBe("9s");
		expect(formatWait(89)).toBe("89s");
		expect(formatWait(90)).toBe("2 min");
		expect(formatWait(3600)).toBe("60 min");
		expect(signalSendTimeout(0)).toBe(30_000);
		expect(signalSendTimeout(3)).toBe(60_000); // floor
		expect(signalSendTimeout(20)).toBe(100_000); // 5s/attachment scaling
	});

	it("MUTATION CHECK: a scheduler that DEDUCTS in acquire fails the read-only contract", async () => {
		class DeductOnAcquireMutant extends SignalAttachmentScheduler {
			override async acquire(n: number): Promise<number> {
				const slept = await super.acquire(n);
				this.tokens = Math.max(0, this.tokens - n); // THE LIE
				return slept;
			}
		}
		const mutant = new DeductOnAcquireMutant({
			clock: { nowMs: () => 0, sleepMs: async () => {} },
		});
		await mutant.acquire(50);
		expect(mutant.tokens).not.toBe(50); // mutant reproduces the defect…
		const real = frozenScheduler();
		await real.acquire(50);
		expect(real.tokens).toBe(50); // …and the contract pins the REAL one
	});

	it("MUTATION CHECK: reportRpcDuration crediting upload refill drifts the model", async () => {
		class UploadRefillMutant extends SignalAttachmentScheduler {
			override async reportRpcDuration(
				rpcDurationS: number,
				nAttachments: number,
			): Promise<void> {
				// THE LIE: credit refill for the upload window before deduction
				// (upstream documents this causes cumulative drift ⇒ 429s).
				this.tokens = Math.min(
					this.capacity,
					this.tokens + rpcDurationS * this.refillRate,
				);
				await super.reportRpcDuration(0, nAttachments);
			}
		}
		const mutant = new UploadRefillMutant({
			clock: { nowMs: () => 1_000_000, sleepMs: async () => {} },
		});
		mutant.tokens = 20;
		await mutant.reportRpcDuration(100, 10);
		expect(mutant.tokens).toBe(35); // drifted: 20 + 25 − 10

		const real = new SignalAttachmentScheduler({
			clock: { nowMs: () => 1_000_000, sleepMs: async () => {} },
		});
		real.tokens = 20;
		await real.reportRpcDuration(100, 10);
		expect(real.tokens).toBe(10); // honest: NO upload-window refill credited
	});

	it("MUTATION CHECK: feedback that ignores the server hint never calibrates", () => {
		class IgnoreFeedbackMutant extends SignalAttachmentScheduler {
			override feedback(_retryAfter: number | null, nAttempted: number): void {
				this.tokens = 0;
				void nAttempted; // drops the calibration — THE LIE
			}
		}
		const mutant = new IgnoreFeedbackMutant({
			clock: { nowMs: () => 0, sleepMs: async () => {} },
		});
		mutant.feedback(9, 1);
		expect(mutant.state().refillSecondsPerToken).toBe(4); // still default
		const real = frozenScheduler();
		real.feedback(9, 1);
		expect(real.state().refillSecondsPerToken).toBe(9);
	});
});

// ── formatting module contracts (A18) ────────────────────────────────────────

describe("markdown_to_signal module contracts", () => {
	it("inline styles convert with markers stripped and exact ranges", () => {
		expect(markdownToSignal("**bold** and *it* and ~~gone~~")).toEqual([
			"bold and it and gone",
			["0:4:BOLD", "9:2:ITALIC", "16:4:STRIKETHROUGH"],
		]);
	});

	it("headings become BOLD lines without markers; bullets become • outside fences", () => {
		const [text, styles] = markdownToSignal("# Title\n- item one\n* item two");
		expect(text).toBe("Title\n• item one\n• item two");
		expect(styles).toEqual(["0:5:BOLD"]);
	});

	it("fenced code blocks protect bullets byte-for-byte and emit MONOSPACE", () => {
		const src = "prose:\n```\n- not a bullet\n```";
		const [text, styles] = markdownToSignal(src);
		expect(text).toBe("prose:\n- not a bullet");
		expect(styles).toEqual(["7:14:MONOSPACE"]);
	});

	it("positions are UTF-16 CODE UNITS: an astral prefix shifts ranges correctly", () => {
		// 🎉 = 2 UTF-16 units; bold starts AFTER emoji + space ⇒ u16 pos 3.
		const [text, styles] = markdownToSignal("🎉 **boom**");
		expect(text).toBe("🎉 boom");
		expect(styles).toEqual(["3:4:BOLD"]);
		// A CODEPOINT-position converter would emit "2:4:BOLD" — the exact
		// mutation this contract rejects. Build that mutant and show the
		// difference is observable:
		function convertWithCodePointPositions(src: string): string[] {
			const [convertedText, styles] = markdownToSignal(src);
			return styles.map((s) => {
				const [startStr, length, style] = s.split(":");
				const cpStart = Array.from(
					convertedText.slice(0, Number(startStr)),
				).length;
				return `${cpStart}:${length}:${style}`;
			});
		}
		expect(convertWithCodePointPositions("🎉 **boom**")).toEqual(["2:4:BOLD"]);
	});

	it("triple newlines collapse and edges trim", () => {
		expect(markdownToSignal("  a\n\n\n\n\nb  ")[0]).toBe("a\n\nb");
	});

	it("overlap precedence: FIRST matching pattern owns a span; overlapping spans skip whole", () => {
		// BOLD claims the whole region FIRST; the inner ITALIC overlaps an
		// occupied region and is skipped ENTIRELY — its markers survive as
		// literal bytes inside the bold span (signal_format.py semantics).
		const [text, styles] = markdownToSignal("**a *b* c**");
		expect(text).toBe("a *b* c");
		expect(styles).toEqual(["0:7:BOLD"]);
	});

	it("monospace inline and bold combine across one line", () => {
		const [text, styles] = markdownToSignal("run `npm x` now");
		expect(text).toBe("run npm x now");
		expect(styles).toEqual(["4:5:MONOSPACE"]);
	});
});

// ── media magic bytes + identifier helpers ───────────────────────────────────

describe("media magic-byte contracts", () => {
	it("sniffs the historical table exactly", () => {
		expect(
			guessExtension(Buffer.from([0x89, 0x50, 0x4e, 0x47, 0x0d, 0x0a])),
		).toBe(".png");
		expect(guessExtension(Buffer.from([0xff, 0xd8, 0xff, 0xe0]))).toBe(".jpg");
		expect(guessExtension(Buffer.from("GIF89a"))).toBe(".gif");
		expect(guessExtension(Buffer.from("%PDF-1.7"))).toBe(".pdf");
		expect(guessExtension(Buffer.from("PK\u0003\u0004"))).toBe(".zip");
		expect(guessExtension(Buffer.from("totally unknown"))).toBe(".bin");
	});

	it("ADTS AAC shares the sync word with MP3; bit layout disambiguates", () => {
		expect(guessExtension(Buffer.from([0xff, 0xf1, 0x4c, 0x80]))).toBe(".aac");
		expect(guessExtension(Buffer.from([0xff, 0xfb, 0x90, 0x00]))).toBe(".mp3");
	});

	it("ext→mime table preserves the historical Signal mapping", () => {
		expect(extToMime(".m4a")).toBe("audio/mp4");
		expect(extToMime(".mp3")).toBe("audio/mpeg");
		expect(extToMime(".xyz")).toBe("application/octet-stream");
	});
});

describe("identifier + result-validation helpers", () => {
	it("service-id and E.164 classifiers port exactly", () => {
		expect(isSignalServiceId("9d6fe03a-2b7c-4f21-a5de-9a3c1e83b7aa")).toBe(
			true,
		);
		expect(isSignalServiceId("PNI:abcd")).toBe(true);
		expect(isSignalServiceId("u:abcd")).toBe(true);
		expect(isSignalServiceId("+15551234567")).toBe(false);
		expect(isSignalServiceId("")).toBe(false);
		expect(looksLikeE164Number("+15551234567")).toBe(true);
		expect(looksLikeE164Number("+123")).toBe(false);
		expect(looksLikeE164Number("15551234567")).toBe(false);
	});

	it("renderMentions replaces placeholders end→start; untouched without placeholders", () => {
		// "hi \uFFFC and \uFFFC!": placeholders at u16 index 3 and 9.
		const out = renderMentions("hi \uFFFC and \uFFFC!", [
			{ start: 9, length: 1, number: "+15550001111" },
			{ start: 3, length: 1, uuid: "aaaa-bbbb" },
		]);
		expect(out).toBe("hi @aaaa-bbbb and @+15550001111!");
		expect(renderMentions("plain", [])).toBe("plain");
	});

	it("validateSendResult ports the results[] walk", () => {
		expect(validateSendResult(undefined).success).toBe(true);
		expect(validateSendResult({}).success).toBe(true);
		expect(validateSendResult({ results: [{ type: "SUCCESS" }] }).success).toBe(
			true,
		);
		const failed = validateSendResult({
			results: [{ type: "IDENTITY_FAILURE" }],
		});
		expect(failed.success).toBe(false);
		expect(failed.error).toBe("IDENTITY_FAILURE");
		const unspecified = validateSendResult({ results: [{ success: false }] });
		expect(unspecified.success).toBe(false);
	});

	it("SignalRateLimitError carries the server retry_after; numeric parse core", () => {
		const e = new SignalRateLimitError("limited", 12);
		expect(e.retryAfter).toBe(12);
		expect(parseRetryAfterSeconds("7")).toBe(7);
		expect(parseRetryAfterSeconds(-1)).toBeNull();
		expect(parseRetryAfterSeconds("soon")).toBeNull();
	});
});

// ── post-stream media lanes (signal.py:_send_attachment parity) ─────────────

describe("post-stream media lanes (signal.py:_send_attachment parity)", () => {
	it("single-attachment send carries the caption as the message body over the SAME rpc/wire path", async () => {
		const w = makeWorld();
		try {
			const pdf = join(w.mediaDir, "report.pdf");
			writeFileSync(pdf, Buffer.from("%PDF-1.4 attachment payload"));
			const r = await w.adapter.sendAttachment("+15551112222", pdf, {
				caption: "see attached",
				mediaLabel: "File",
			});
			expect(r.success).toBe(true);
			expect(r.messageId).toBeNull(); // NO editable identity — same as text
			const send = w.daemon.callsOf("send")[0];
			expect(send?.params["account"]).toBe("+15550001111");
			expect(send?.params["message"]).toBe("see attached");
			expect(send?.params["attachments"]).toEqual([pdf]);
			expect(send?.params["recipient"]).toEqual(["+15551112222"]);
			expect(send?.params["groupId"]).toBeUndefined();
		} finally {
			w.cleanup();
		}
	});

	it("voice/video lanes resolve addresses identically to text sends: groups strip the prefix, DMs pass through", async () => {
		const w = makeWorld();
		try {
			const audio = join(w.mediaDir, "note.mp3");
			writeFileSync(audio, Buffer.from("ID3 voice payload"));
			const video = join(w.mediaDir, "clip.mp4");
			writeFileSync(video, Buffer.from("ftypisom video payload"));

			await w.adapter.sendVoice("group:abc123==", audio);
			let send = w.daemon.callsOf("send")[0];
			expect(send?.params["groupId"]).toBe("abc123==");
			expect(send?.params["recipient"]).toBeUndefined();
			expect(send?.params["account"]).toBe("+15550001111");
			expect(send?.params["message"]).toBe("");
			expect(send?.params["attachments"]).toEqual([audio]);

			await w.adapter.sendVideo("+15551112222", video);
			send = w.daemon.callsOf("send")[1];
			expect(send?.params["recipient"]).toEqual(["+15551112222"]);
			expect(send?.params["groupId"]).toBeUndefined();
			expect(send?.params["attachments"]).toEqual([video]);
		} finally {
			w.cleanup();
		}
	});

	it("missing files fail with the source verdict WITHOUT burning an RPC", async () => {
		const w = makeWorld();
		try {
			const ghost = join(w.mediaDir, "ghost.pdf");
			const r = await w.adapter.sendDocument("+15551112222", ghost);
			expect(r.success).toBe(false);
			expect(r.error).toBe(`File file not found: ${ghost}`);
			expect(w.daemon.callsOf("send")).toHaveLength(0);
		} finally {
			w.cleanup();
		}
	});

	it("image batches ride the scheduler-paced batch lane: ONE rpc, empty message body", async () => {
		const w = makeWorld();
		try {
			const images = ["a.png", "b.png", "c.png"].map((n) => {
				const p = join(w.mediaDir, n);
				writeFileSync(p, Buffer.from("png-bytes"));
				return p;
			});
			const results = await w.adapter.sendMultipleImages(
				"+15551112222",
				images,
			);
			expect(results.map((r) => r.success)).toEqual([true]); // ONE batch outcome
			const send = w.daemon.callsOf("send")[0];
			expect(send?.params["attachments"]).toEqual(images);
			expect(send?.params["message"]).toBe("");
			expect(send?.params["recipient"]).toEqual(["+15551112222"]);
		} finally {
			w.cleanup();
		}
	});

	it("attachment-send timestamps feed the Note-to-Self echo filter end-to-end", async () => {
		const w = makeWorld();
		try {
			w.adapter.attachStandardGuard();
			const p = join(w.mediaDir, "f.pdf");
			writeFileSync(p, Buffer.from("%PDF"));
			await w.adapter.sendAttachment("+15551112222", p);
			const sendResult = w.daemon.callsOf("send")[0]?.result as
				| Record<string, unknown>
				| undefined;
			const ts = Number(sendResult?.["timestamp"]);
			await w.adapter.handleEnvelope({
				envelope: {
					syncMessage: {
						sentMessage: {
							destinationNumber: "+15550001111",
							timestamp: ts,
						},
					},
				},
			});
			expect(w.adapter.counts.echoSuppressed).toBe(1);
			expect(w.adapter.turnLog).toEqual([]);
		} finally {
			w.cleanup();
		}
	});

	it("the four lanes bind the core rescan seam end-to-end (MEDIA tags dispatch real RPCs)", async () => {
		const w = makeWorld();
		try {
			const png = join(w.mediaDir, "shot.png");
			writeFileSync(png, Buffer.from("png"));
			const png2 = join(w.mediaDir, "shot2.png");
			writeFileSync(png2, Buffer.from("png2"));
			const ogg = join(w.mediaDir, "memo.ogg");
			writeFileSync(ogg, Buffer.from("ogg"));
			const mov = join(w.mediaDir, "clip.mov");
			writeFileSync(mov, Buffer.from("mov"));
			const pdf = join(w.mediaDir, "paper.pdf");
			writeFileSync(pdf, Buffer.from("pdf"));

			const lane: PostStreamAdapter = w.adapter;
			const opts = (adapter: PostStreamAdapter) => ({
				adapter,
				chatId: "+15551112222",
				chatPlatform: "signal",
				validatePath: (p: string): string | null => p,
			});

			const r1 = await rescanPostStream(
				`MEDIA:${png} MEDIA:${png2} MEDIA:${ogg}`,
				opts(lane),
			);
			expect(r1.attempts.map((a) => [a.kind, a.status])).toEqual([
				["image_batch", "sent"],
				["voice_or_audio", "sent"],
			]);
			// Batch first (ONE rpc carrying both images), then the voice send.
			let sends = w.daemon.callsOf("send");
			expect(sends[0]?.params["attachments"]).toEqual([png, png2]);
			expect(sends[0]?.params["message"]).toBe("");
			expect(sends[1]?.params["attachments"]).toEqual([ogg]);

			const r2 = await rescanPostStream(
				`MEDIA:${mov} MEDIA:${pdf}`,
				opts(lane),
			);
			expect(r2.attempts.map((a) => [a.kind, a.status])).toEqual([
				["video", "sent"],
				["document", "sent"],
			]);
			sends = w.daemon.callsOf("send");
			expect(sends[2]?.params["attachments"]).toEqual([mov]);
			expect(sends[3]?.params["attachments"]).toEqual([pdf]);
		} finally {
			w.cleanup();
		}
	});
});
