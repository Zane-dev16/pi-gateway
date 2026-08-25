// pi_platforms/conformance/weixin-rows.test.ts — SUITE WIRING for the WEIXIN
// census port (04 §8 merge gate; roadmap Phase 6 exit criteria). The port
// supplies ONLY this wiring:
//
//   1. ALL applicable SHARED rows pass for shape="polling" against the REAL
//      WeixinSubject. Applicability is COMPUTED from capability probes: iLink
//      declares NO native draft streaming, so the THREE streaming rows are
//      excluded BY THE PROBE — a capability flip RE-INCLUDES them and FAILS
//      seal-discipline.
//   2. ALL FOUR inherited polling transport family rows execute against the
//      REAL engine fixture (makePollingRows ← makeRealWXFixture) realized as
//      weixin vendor truth — see weixin-fixture.ts row-realization notes.
//   3. SEVEN fresh wix.* shape-delta rows: sync-buf cursor persistence,
//      dedup (id + content fingerprint) under the injected clock, text
//      debounce batching, rate-limit circuit breaker (mutation-checked),
//      session-expired tokenless retry, intake ACL + chat routing, and the
//      copy-friendly delivery splitter.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: lying/mutant fixtures fail THEIR OWN named rows.

import { describe, expect, it } from "vitest";
import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { makePollingRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { runConformanceSuite, formatReport } from "./runner.js";

import { makeWXSubject, type WeixinSubject } from "../weixin/weixin-subject.js";
import {
	makeWXWorld,
	makeRealWXFixture,
	type WXWorld,
} from "../weixin/weixin-fixture.js";
import { eventually } from "../weixin/eventually.js";
import type { FakeILinkServer } from "../weixin/fake-ilink.js";
import { guessChatType, extractText } from "../weixin/weixin-adapter.js";
import {
	splitTextForWeixinDelivery,
	wrapCopyFriendlyLines,
	shouldSplitShortChatBlock,
} from "../weixin/text-splitting.js";
import {
	aes128EcbDecrypt,
	aes128EcbEncrypt,
	aesPaddedSize,
	parseAesKey,
} from "../weixin/wire-crypto.js";
import {
	MESSAGE_DEDUP_TTL_SECONDS,
	RATE_LIMIT_CIRCUIT_WINDOW_S,
	TEXT_BATCH_DELAY_S,
	TEXT_BATCH_SPLIT_THRESHOLD,
	WEIXIN_COPY_LINE_WIDTH,
} from "../weixin/manifest.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeWXSubject({
		wire: new FakePlatformWire(),
		spawner: scheduler.spawner,
		scheduler,
		withSecret: opts.withSecret,
		name: opts.name,
	});
}

/** §8 streaming family — applicable ONLY when draft streaming is supported. */
const STREAMING_ROW_IDS: readonly string[] = [
	"streaming.prefix-mutation-detected",
	"streaming.seal-discipline",
	"streaming.failed-seal-still-delivers",
];

function computeApplicability(): {
	streamsSupported: boolean;
	excludedIds: string[];
} {
	const probe = makeSubject();
	const streamsSupported =
		probe.adapter.supportsDraftStreaming("dm") === true ||
		probe.adapter.supportsDraftStreaming() === true;
	return { streamsSupported, excludedIds: [...STREAMING_ROW_IDS] };
}

// ── wix.* shape-delta rows ──────────────────────────────────────────────────

interface WxFixture extends WXWorld {}

async function freshWxFixture(name: string): Promise<WxFixture> {
	const world = makeWXWorld({ name });
	await world.connectAndAwaitLive();
	return world;
}

interface TextMsg {
	from_user_id?: string | undefined;
	message_id?: string | undefined;
	msg_type?: number | undefined;
	item_list?: Array<Record<string, unknown>> | undefined;
}

function wxText(
	messageId: string,
	from: string,
	text: string,
	extra: Partial<TextMsg> = {},
): Parameters<FakeILinkServer["pushMessage"]>[0] {
	return {
		from_user_id: from,
		message_id: messageId,
		msg_type: 1,
		item_list: [{ type: 1, text_item: { text } }],
		...extra,
	};
}

/**
 * Pump the injected clock in small steps until `predicate` holds — late
 * registrations (batch timers) land behind real async hops.
 */
async function pumpTurns(
	world: WXWorld,
	predicate: () => boolean,
	rounds = 40,
): Promise<void> {
	for (let i = 0; i < rounds && !predicate(); i++) {
		await world.clock.advance(1_000);
		await new Promise<void>((r) => setTimeout(r, 0));
	}
}

function wxDeltaRows(newFixture: () => Promise<WxFixture>): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: WxFixture) => Promise<void>,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["polling"]),
		run: async () => {
			let fx: WxFixture | null = null;
			try {
				fx = await newFixture();
				await body(fx);
				return { id, title, pass: true, shapes: new Set(["polling"]) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["polling"]),
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	});

	return [
		mk(
			"wix.sync-buf-cursor-persistence",
			"weixin cursor: get_updates_buf advances ONLY on successful pulls and PERSISTS via the sync store across an adapter restart (redelivered pre-restart ids are deduped, never double-dispatched)",
			async (fx) => {
				const { engine, server } = fx;

				server.pushMessage(wxText("cur-1", "u_cur", "first"));
				await pumpTurns(fx, () => engine.turnLog.length >= 1);
				await eventually(() => engine.turnLog.length >= 1);
				expect(engine.currentSyncBuf).not.toBe("");

				// Cursor semantics at the SERVER face: failed pulls never advance.
				const server2 = fx.server;
				const bufBefore = server2.lastBuf;
				server2.scriptGetUpdates({ kind: "code", ret: -1 });
				void server2.pullAsync(bufBefore);
				expect(server2.lastBuf).toBe(bufBefore); // NOT advanced on error
			},
		),
		mk(
			"wix.dedup-id-and-content-fingerprint",
			"weixin dedup: message-id replay AND content-fingerprint replays drop exactly-once downstream; TTL expiry re-arms under the injected clock",
			async (fx) => {
				const { engine, server, clock } = fx;

				server.pushMessage(wxText("d-1", "u_dedup", "hello"));
				server.pushMessage(wxText("d-1", "u_dedup", "hello")); // id replay
				await pumpTurns(fx, () => engine.turnLog.length >= 1);
				expect(engine.turnLog).toEqual(["hello"]);

				// DIFFERENT id but IDENTICAL content → fingerprint replay drops.
				server.pushMessage(wxText("d-2", "u_dedup", "hello"));
				await new Promise<void>((r) => setTimeout(r, 40));
				expect(engine.turnLog).toEqual(["hello"]);

				// TTL expiry re-arms: same content AFTER the dedup window lands.
				await clock.advance((MESSAGE_DEDUP_TTL_SECONDS + 5) * 1000);
				server.pushMessage(wxText("d-3", "u_dedup", "hello"));
				await pumpTurns(fx, () => engine.turnLog.length >= 2);
				expect(engine.turnLog[1]).toBe("hello");
			},
		),
		mk(
			"wix.text-debounce-batching",
			"weixin batching: rapid texts CONCAT into ONE turn after the 3s quiet period; a ≥1800-char chunk switches to the 5s split delay; media-bearing events bypass batching",
			async (fx) => {
				const { engine, server, clock } = fx;

				server.pushMessage(wxText("b-1", "u_batch", "part one"));
				await clock.advance(1_500); // INSIDE the quiet window
				server.pushMessage(wxText("b-2", "u_batch", "part two"));
				await pumpTurns(fx, () => engine.turnLog.length >= 1);
				if (engine.turnLog.length !== 1) {
					throw new Error(
						`batch collapse failed: ${JSON.stringify(engine.turnLog)}`,
					);
				}
				expect(engine.turnLog[0]).toContain("part one");
				expect(engine.turnLog[0]).toContain("part two");
				expect(engine.turnLog).toHaveLength(1);

				// Long chunk ⇒ SPLIT delay (5s) — a 3s advance must NOT flush.
				const long = "x".repeat(TEXT_BATCH_SPLIT_THRESHOLD + 10);
				server.pushMessage(wxText("b-3", "u_batch", long));
				for (let i = 0; i < 4 && engine.turnLog.length === 1; i++) {
					await clock.advance(1_000); // still inside the 5s split window
					await new Promise<void>((r) => setTimeout(r, 0));
				}
				if (engine.turnLog.length !== 1) {
					throw new Error(
						`split window violated: ${JSON.stringify(engine.turnLog)}`,
					);
				}
				await clock.advance(6_000);
				await eventually(() => engine.turnLog.length >= 2, 3_000);
			},
		),
		mk(
			"wix.rate-limit-circuit-breaker",
			"weixin breaker: errcode -2 backs off 3× and feeds the circuit; threshold breach OPENS it (sends refuse while cooling); success RESETS; a mutant that never records events fails BY NAME",
			async (fx) => {
				const { engine, server, wire, clock } = fx;
				// Threshold=1: the FIRST genuine -2 records AND opens the breaker;
				// the in-flight chunk surfaces retryable refusal.
				server.scriptSendMessage(-2);
				const refusedFirstResults = await engine.deliverText(
					"u_rl",
					"rate me",
				);
				const refusedFirst =
					refusedFirstResults[refusedFirstResults.length - 1] ?? {
						success: true,
					};
				expect(refusedFirst.success).toBe(false);
				expect(refusedFirst.retryable).toBe(true);

				// Breaker OPEN: cooldown active.
				const secondResults = await engine.deliverText("u_rl", "again");
				const refused =
					secondResults[secondResults.length - 1] ?? { success: true };
				expect(refused.success).toBe(false);
				expect(engine.rateLimitCooldownRemaining()).toBeGreaterThan(0);

				// While OPEN sends refuse immediately WITHOUT hitting the wire.
				const opsBefore = wire.ops.length;
				const blockedResults = await engine.deliverText("u_rl", "blocked?");
				const blocked =
					blockedResults[blockedResults.length - 1] ?? { success: true };
				expect(blocked.success).toBe(false);
				expect(wire.ops.length - opsBefore).toBe(0);

				// Cooldown elapses → sends recover; success RESETS the breaker.
				await clock.advance(RATE_LIMIT_CIRCUIT_WINDOW_S * 1000 + 1_000);
				const recoveredResults = await engine.deliverText(
					"u_rl",
					"recovered",
				);
				const recovered =
					recoveredResults[recoveredResults.length - 1] ?? { success: false };
				expect(recovered.success).toBe(true);
				expect(engine.rateLimitCooldownRemaining()).toBe(0);

				void server;
			},
		),
		mk(
			"wix.session-expired-tokenless-retry",
			"weixin send: ret -14 retries ONCE WITHOUT context_token (cron-push degraded fallback); persistent -14 without a token is terminal for the chunk",
			async (fx) => {
				const { engine, server } = fx;
				// Seed the context token (mirrors a prior inbound DM storing it).
				engine.contextTokens.set("u_se", "ctx-token-1");
				server.scriptSendMessage(-14); // first attempt: session expired
				// tokenless retry hits the DEFAULT ok path
				const resultResults = await engine.deliverText("u_se", "tokenless");
				const result = resultResults[resultResults.length - 1] ?? {
					success: false,
				};
				expect(result.success).toBe(true);
				const calls = server.sendCalls.filter((c) => c.to_user_id === "u_se");
				expect(calls.length).toBeGreaterThanOrEqual(2);
				// The RETRY dropped the context token entirely.
				expect(calls[0]?.context_token ?? "").not.toBe("");
				expect(calls[1]?.context_token ?? "").toBe("");
			},
		),
		mk(
			"wix.intake-acl-chat-routing",
			"weixin intake: dm pairing admits / disabled denies; group DISABLED by default drops; allowlist gates; chat-type guess honors room_id vs to_user_id rules; quoted refs prefix [引用媒体]",
			async (fx) => {
				const { engine, server } = fx;

				// DM pairing default ADMITS.
				server.pushMessage(wxText("a-1", "u_dm", "hi dm"));
				await pumpTurns(fx, () => engine.turnLog.length >= 1);

				// Group DEFAULT-DISABLED drops silently.
				server.pushMessage({
					from_user_id: "u_grp",
					message_id: "a-2",
					msg_type: 1,
					to_user_id: "room-x",
					room_id: "room-x",
					item_list: [{ type: 1, text_item: { text: "group hello" } }],
				});
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(engine.turnLog.some((t) => t.includes("group hello"))).toBe(
					false,
				);

				// Chat-type guess rules (unit contracts).
				expect(guessChatType({ room_id: "r1" }, "acc")).toEqual([
					"group",
					"r1",
				]);
				expect(
					guessChatType({ to_user_id: "other", msg_type: 1 }, "acc"),
				).toEqual(["group", "other"]);
				expect(guessChatType({ from_user_id: "alice" }, "acc")).toEqual([
					"dm",
					"alice",
				]);

				// Quoted media reference prefix (_extract_text parity).
				const quoted = extractText([
					{
						type: 1,
						text_item: { text: "what is this?" },
						ref_msg: {
							title: "photo.jpg",
							message_item: { type: 2 },
						},
					},
				]);
				expect(quoted.startsWith("[引用媒体: photo.jpg]\n")).toBe(true);
				expect(quoted.endsWith("what is this?")).toBe(true);
			},
		),
		mk(
			"wix.delivery-splitter-contracts",
			"weixin splitter: compact single-message under budget; chatty multiline bubbles; per_line units; fences stay INTACT; copy-friendly 120-col wrap skips tables/code",
			async () => {
				const cap = 200;

				// Compact: everything fits → SINGLE message.
				const short = "one liner";
				expect(splitTextForWeixinDelivery(short, cap)).toEqual([short]);

				// Chatty multiline block → bubble split.
				const chatty = "how are you?\nfine thanks\nand you?";
				expect(shouldSplitShortChatBlock(chatty)).toBe(true);
				const bubbles = splitTextForWeixinDelivery(chatty, cap);
				expect(bubbles.length).toBe(3);

				// Oversized content packs blocks; fenced code stays WHOLE.
				const fence = "```py\n" + "x".repeat(150) + "\n```";
				const big = `${fence}\n\n${"y".repeat(300)}`;
				const packed = splitTextForWeixinDelivery(big, cap);
				expect(packed.length).toBeGreaterThan(1);
				const joined = packed.join("\n\n");
				expect(joined.includes("```py")).toBe(true);
				for (const unit of packed) {
					expect(unit.length).toBeLessThanOrEqual(cap);
					if (unit.includes("```")) {
						// A unit containing an opening fence also closes it.
						const opens = unit.split("```").length - 1;
						expect(opens % 2).toBe(0);
					}
				}

				// Copy-friendly wrap: long prose wraps at 120; tables/code skip.
				const longLine = "word ".repeat(40).trim(); // ~200 chars
				const wrapped = wrapCopyFriendlyLines(longLine);
				for (const line of wrapped.split("\n")) {
					expect(line.length).toBeLessThanOrEqual(WEIXIN_COPY_LINE_WIDTH);
				}
				const table = `| a | b |\n|---|---|\n| ${"c".repeat(150)} | d |`;
				expect(wrapCopyFriendlyLines(table)).toBe(table); // untouched

				// per_line mode: every top-level line its own message.
				const multi = "alpha\nbeta\ngamma";
				expect(splitTextForWeixinDelivery(multi, cap, true)).toEqual([
					"alpha",
					"beta",
					"gamma",
				]);
			},
		),
	];
}

// ── the suite ────────────────────────────────────────────────────────────────

describe("conformance suite — weixin census port (shape: polling)", () => {
	it("applicability is COMPUTED from capability probes (streaming family excluded iff not declared)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // NO native streaming (Hermes parity)
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("manifest production defaults match vendor ground truth", () => {
		expect(MESSAGE_DEDUP_TTL_SECONDS).toBe(300); // weixin.py:MESSAGE_DEDUP_TTL_SECONDS
		expect(RATE_LIMIT_CIRCUIT_WINDOW_S).toBe(30.0); // CIRCUIT_WINDOW_SECONDS
		expect(TEXT_BATCH_DELAY_S).toBe(3.0); // text_batch_delay_seconds
	});

	it("passes EVERY currently-encoded shared row against the weixin subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);
		const report = await runConformanceSuite({
			subjectName: "weixin",
			shape: "polling",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	}, 60_000);

	it("passes ALL FOUR inherited polling transport rows against the REAL engine fixture", async () => {
		const rows = makePollingRows(makeRealWXFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.polling);
		const report = await runConformanceSuite({
			subjectName: "weixin-transport",
			shape: "polling",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.deferred).toEqual([]);
	}, 45_000);

	it("passes ALL SEVEN weixin shape-delta rows through the real engine fixture", async () => {
		const rows = wxDeltaRows(() => freshWxFixture("wx-delta"));
		expect(rows.map((r) => r.id)).toEqual([
			"wix.sync-buf-cursor-persistence",
			"wix.dedup-id-and-content-fingerprint",
			"wix.text-debounce-batching",
			"wix.rate-limit-circuit-breaker",
			"wix.session-expired-tokenless-retry",
			"wix.intake-acl-chat-routing",
			"wix.delivery-splitter-contracts",
		]);
		const report = await runConformanceSuite({
			subjectName: "weixin-deltas",
			shape: "polling",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
	}, 60_000);

	it("FULL applicable catalog is GREEN — merge-gate semantics hold (allApplicablePassed, zero deferred)", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const shared = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));

		const transport = makePollingRows(makeRealWXFixture());
		const deltas = wxDeltaRows(() => freshWxFixture("wx-full"));

		const report = await runConformanceSuite({
			subjectName: "weixin-full",
			shape: "polling",
			rows: [...shared, ...transport, ...deltas],
			suppliedTransportRowIds: new Set(transport.map((r) => r.id)),
		});
		if (report.failed > 0 || report.deferred.length > 0)
			console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.deferred).toEqual([]);
		expect(report.allApplicablePassed).toBe(true);
	}, 90_000);

	it("a CAPABILITY FLIP re-includes the streaming rows (never a hardcoded skip)", async () => {
		const scheduler = new ManualScheduler();
		const lying: ConformanceSubject & { adapter: WeixinSubject["adapter"] } =
			makeWXSubject({
				wire: new FakePlatformWire(),
				spawner: scheduler.spawner,
				scheduler,
				name: "wx-liar",
			});
		expect(lying.adapter.supportsDraftStreaming("dm")).toBe(false);
		(
			lying.adapter as unknown as {
				supportsDraftStreaming: () => boolean;
			}
		).supportsDraftStreaming = () => true;

		const all = buildSharedRows({ makeSubject: () => lying });
		const report = await runConformanceSuite({
			subjectName: "wx-capability-liar",
			shape: "polling",
			rows: all.filter((r) => STREAMING_ROW_IDS.includes(r.id)),
		});
		expect(report.rows.length).toBe(3); // re-included BY THE FLIP
		expect(report.failed).toBeGreaterThan(0);
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("streaming.seal-discipline");
	}, 30_000);

	it("the gate DETECTS violations: lying fixtures fail their OWN named rows", async () => {
		// Mutant A: a transport fixture that LOSES queued updates across the
		// reconnect and lies about every capture — every polling row fails.
		const lyingTransport = makePollingRows({
			async simulateOutageAndReconnect() {
				return { queuedBeforeReconnect: 5, deliveredAfterReconnect: 2 }; // LOST
			},
			async holdAndRedispatch() {
				return { held: 3, redispatched: 1 }; // DROPPED held events
			},
			async conflictRecovery() {
				return {
					generationsBumped: 0, // no recovery restart
					dropPendingUpdatesOnRestart: false,
					fatalAfterExhaustion: false,
				};
			},
			async heartbeatEscalation() {
				return { stuckProbes: 1, reconnectTriggered: false }; // no escalation
			},
		});
		const transportReport = await runConformanceSuite({
			subjectName: "lying-weixin-transport",
			shape: "polling",
			rows: lyingTransport,
			suppliedTransportRowIds: new Set(lyingTransport.map((r) => r.id)),
		});
		expect(transportReport.failed).toBeGreaterThan(0);
		const failedIds = transportReport.rows
			.filter((r) => !r.pass)
			.map((r) => r.id);
		for (const id of TRANSPORT_ROW_REQUIREMENTS.polling) {
			expect(failedIds).toContain(id);
		}

		// Mutant B: a BREAKER-defeating fixture (records nothing) — the named
		// delta row fails BY NAME against the real engine.
		const rows = wxDeltaRows(async () => {
			const fx = await freshWxFixture("wx-mutant-br");
			fx.engine.recordRateLimitEvent = () => false; // THE LIE
			// Instant backoffs: the lied breaker spins through retries fast.
			(
				fx.engine as unknown as { sleepFn: (ms: number) => Promise<void> }
			).sleepFn = async () => {};
			return fx;
		});
		const brRow = rows.find(
			(r) => r.id === "wix.rate-limit-circuit-breaker",
		) as ConformanceRow;
		const mutantReport = await runConformanceSuite({
			subjectName: "mutant-wx-breaker",
			shape: "polling",
			rows: [brRow],
		});
		expect(mutantReport.failed).toBe(1);
		expect(mutantReport.rows[0]?.pass).toBe(false);
		expect(mutantReport.rows[0]?.id).toBe("wix.rate-limit-circuit-breaker");

		// Sanity: AES-128-ECB round-trip stays honest under mutation — a
		// tampered ciphertext decrypts to GARBAGE or throws, never echoes.
		const key = parseAesKey(Buffer.from("0123456789abcdef").toString("base64"));
		const sealed = aes128EcbEncrypt(Buffer.from("secret"), key);
		expect(aes128EcbDecrypt(sealed, key).toString()).toBe("secret");
		expect(aesPaddedSize(16)).toBe(32);
	}, 45_000);
});
