// pi_platforms/conformance/qqbot-rows.test.ts — SUITE WIRING for the QQBOT
// census port (04 §8 merge gate; roadmap Phase 6 exit criteria). The port
// supplies ONLY this wiring:
//
//   1. ALL applicable SHARED rows pass for shape="ws" against the REAL
//      QQBotSubject. Applicability is COMPUTED from capability probes (04 §8
//      conditional headers): QQ declares NO native draft streaming (base
//      supports_draft_streaming stays False — Hermes parity), so the THREE
//      streaming rows are excluded BY THE PROBE, never by a hardcoded skip —
//      a capability flip RE-INCLUDES them and FAILS seal-discipline.
//   2. ALL FIVE inherited persistent-ws transport family rows execute against
//      the REAL engine fixture (makeWsRows ← makeRealQQFixture) realized as
//      QQ vendor truth — see qqbot-fixture.ts row-realization notes.
//   3. SEVEN fresh qb.* shape-delta rows: AES-GCM crypto negative matrix,
//      keyboard/interaction round-trip through vendor grammar + ACK ordering,
//      chunked-upload contracts incl. biz-code ladder, gateway close-code
//      matrix, dedup window under injected clock, intake ACL gates, and the
//      markdown v2 body contract.
//   4. Full-catalog gate: allApplicablePassed === true, deferred === [].
//   5. The gate DETECTS: lying/mutant fixtures fail THEIR OWN named rows, and
//      a capability lie trips seal-discipline BY NAME.

import { describe, expect, it } from "vitest";
import { ManualScheduler } from "../../pi_gateway/guards/testing/manual-spawner.js";
import { FakePlatformWire } from "./wire.js";
import { buildSharedRows } from "./rows.js";
import type { ConformanceRow } from "./rows.js";
import { makeWsRows, TRANSPORT_ROW_REQUIREMENTS } from "./shapes.js";
import type { ConformanceSubject } from "./harness.js";
import { runConformanceSuite, formatReport } from "./runner.js";

import { makeQQSubject, type QQBotSubject } from "../qqbot/qqbot-subject.js";
import {
	makeQQWorld,
	makeRealQQFixture,
	c2cDispatch,
	type QQWorld,
} from "../qqbot/qqbot-fixture.js";
import { eventually } from "../qqbot/eventually.js";
import { FakeQQGateway } from "../qqbot/fake-qq-gateway.js";
import {
	buildApprovalKeyboard,
	buildUpdatePromptKeyboard,
	parseApprovalButtonData,
	parseUpdatePromptButtonData,
	parseInteractionEvent,
} from "../qqbot/keyboards.js";
import {
	decryptSecret,
	encryptSecretForFixture,
	generateBindKey,
	SecretDecryptError,
} from "../qqbot/crypto.js";
import {
	ChunkedUploader,
	formatSize,
	parsePrepareResponse,
	UploadDailyLimitExceededError,
} from "../qqbot/chunked-uploader.js";
import {
	QQBOT_MAX_MESSAGE_LENGTH,
	QQ_MSG_TYPE_MARKDOWN,
	QQBOT_RATE_LIMIT_DELAY_S,
	QQ_IDENTIFY_INTENTS,
} from "../qqbot/manifest.js";

// ── shared-row harness ──────────────────────────────────────────────────────

function makeSubject(
	opts: { withSecret?: boolean | undefined; name?: string | undefined } = {},
): ConformanceSubject {
	const scheduler = new ManualScheduler();
	return makeQQSubject({
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

// ── qb.* shape-delta rows ───────────────────────────────────────────────────

interface QbFixture extends QQWorld {}

async function freshQbFixture(name: string): Promise<QbFixture> {
	const world = makeQQWorld({ name });
	await world.connectAndAwaitLive();
	return world;
}

function qbDeltaRows(newFixture: () => Promise<QbFixture>): ConformanceRow[] {
	const mk = (
		id: string,
		title: string,
		body: (fx: QbFixture) => Promise<void>,
	): ConformanceRow => ({
		id,
		title,
		shapes: new Set(["ws"]),
		run: async () => {
			let fx: QbFixture | null = null;
			try {
				fx = await newFixture();
				await body(fx);
				return { id, title, pass: true, shapes: new Set(["ws"]) };
			} catch (err) {
				return {
					id,
					title,
					pass: false,
					shapes: new Set(["ws"]),
					detail: err instanceof Error ? err.message : String(err),
				};
			}
		},
	});

	return [
		mk(
			"qb.crypto-aes-gcm-negative-matrix",
			"qqbot crypto: bind-key round-trip decrypts EXACTLY the server-side ciphertext; tampered tag/body/wrong-key/short-input all fail AUTHENTICATION or DECODE — never silent garbage",
			async () => {
				const secret = "client-secret-λ-0123456789";
				const key = generateBindKey();
				expect(key.length).toBeGreaterThan(0);
				const sealed = encryptSecretForFixture(secret, key);
				expect(decryptSecret(sealed, key)).toBe(secret);

				// Tampered TAG byte (last 16 bytes) must fail authentication.
				const raw = Buffer.from(sealed, "base64");
				const tamperedTag = Buffer.from(raw);
				tamperedTag.subarray(tamperedTag.length - 1)[0] =
					tamperedTag[tamperedTag.length - 1]! ^ 0xff;
				expect(() =>
					decryptSecret(tamperedTag.toString("base64"), key),
				).toThrow(SecretDecryptError);

				// Tampered BODY byte must also fail authentication.
				const tamperedBody = Buffer.from(raw);
				tamperedBody[13] = tamperedBody[13]! ^ 0x01;
				expect(() =>
					decryptSecret(tamperedBody.toString("base64"), key),
				).toThrow(SecretDecryptError);

				// Wrong key fails authentication.
				expect(() => decryptSecret(sealed, generateBindKey())).toThrow(
					SecretDecryptError,
				);

				// Short input fails decode.
				expect(() => decryptSecret("AAAA", key)).toThrow(SecretDecryptError);

				// Two generated keys never collide; each decrypts only its own.
				const key2 = generateBindKey();
				expect(key2).not.toBe(key);
				expect(decryptSecret(encryptSecretForFixture("x", key2), key2)).toBe(
					"x",
				);
			},
		),
		mk(
			"qb.keyboard-interaction-roundtrip",
			"qqbot keyboards: approval/update-prompt builder→INTERACTION_CREATE parse→ACK-before-dispatch through the VENDOR button_data grammar; unauthorized operators answered-not-resolved; guild chats refuse keyboards non-retryably",
			async (fx) => {
				const { engine, gateway } = fx;

				// ── approval keyboard round-trip ──
				engine.chatTypeMap.set("u_kb", "c2c");
				const sessionKey = "agent:main:qqbot:c2c:u_kb";
				const keyboard = buildApprovalKeyboard(sessionKey, {
					allowPermanent: true,
				});
				expect(keyboard.content.rows).toHaveLength(1);
				const buttons = keyboard.content.rows[0]?.buttons ?? [];
				expect(buttons).toHaveLength(3); // once / always / deny share group_id
				for (const b of buttons) {
					expect(b.group_id).toBe("approval");
					expect(b.action.type).toBe(1); // callback
					expect(b.action.click_limit).toBe(1);
				}
				const parsedOnce = parseApprovalButtonData(buttons[0]!.action.data);
				expect(parsedOnce).toEqual([sessionKey, "allow-once"]);

				// INTERACTION_CREATE dispatch → ACK FIRST, then resolution audit.
				gateway.pushDispatch("INTERACTION_CREATE", {
					id: "it-1",
					chat_type: 2,
					user_openid: "u_kb",
					data: {
						resolved: {
							button_data: buttons[0]!.action.data,
							button_id: buttons[0]!.id,
						},
						type: 11,
					},
				});
				await eventually(() => engine.interactionAcks.length >= 1);
				expect(engine.interactionAcks[0]).toMatchObject({
					id: "it-1",
					code: 0,
				});
				expect(gateway.callsOf("interactions")).toHaveLength(1);
				expect(gateway.callsOf("interactions")[0]?.method).toBe("PUT");
				expect(engine.approvalDecisions).toEqual([
					{ sessionKey, decision: "once" },
				]);

				// Unauthorized operator (group chat, wrong member openid).
				const gSessionKey = "agent:main:qqbot:group:g_kb:m_member";
				const gKb = buildApprovalKeyboard(gSessionKey);
				gateway.pushDispatch("INTERACTION_CREATE", {
					id: "it-2",
					chat_type: 1,
					group_openid: "g_kb",
					group_member_openid: "u_impostor",
					data: {
						resolved: {
							button_data: gKb.content.rows[0]?.buttons[0]?.action.data,
						},
					},
				});
				await eventually(() =>
					engine.interactionAcks.some((a) => a.id === "it-2"),
				);
				// ACKed but NOT resolved — unauthorized clicks never resolve.
				expect(engine.approvalDecisions).toHaveLength(1);
				expect(
					engine.reconnectLog.some((l) =>
						l.startsWith("unauthorized-approval"),
					),
				).toBe(true);

				// Update-prompt family through the SAME handler.
				const updKb = buildUpdatePromptKeyboard();
				const yesBtn = updKb.content.rows[0]?.buttons[0];
				expect(yesBtn).toBeDefined();
				if (yesBtn === undefined) return;
				expect(parseUpdatePromptButtonData(yesBtn.action.data)).toBe("y");
				gateway.pushDispatch("INTERACTION_CREATE", {
					id: "it-3",
					chat_type: 2,
					user_openid: "u_kb",
					data: { resolved: { button_data: yesBtn.action.data } },
				});
				await eventually(() => engine.updatePromptAnswers.length >= 1);
				expect(engine.updatePromptAnswers[0]).toEqual({
					answer: "y",
					operator: "u_kb",
				});

				// Garbage button_data: ACKed, logged-and-dropped (never a turn).
				gateway.pushDispatch("INTERACTION_CREATE", {
					id: "it-4",
					chat_type: 2,
					user_openid: "u_kb",
					data: { resolved: { button_data: "zz:bogus:data" } },
				});
				await eventually(() =>
					engine.resolvedFamilies.includes("unknown-button"),
				);

				// Guild channels REFUSE inline keyboards non-retryably.
				engine.chatTypeMap.set("ch_guild", "guild");
				const refused = await engine.sendWithKeyboard(
					"ch_guild",
					"x",
					buildApprovalKeyboard(sessionKey),
				);
				expect(refused.success).toBe(false);
				expect(refused.retryable).toBe(false);
			},
		),
		mk(
			"qb.chunked-upload-contracts",
			"qqbot chunked upload: prepare→PUT-parts→part_finish→complete happy path with per-part md5; biz 40093001 retries until server timeout then raises; biz 40093002 is NON-retryable typed daily-limit; malformed prepare responses rejected",
			async () => {
				// ── happy path over a scripted fake REST face ──
				const calls: Array<{ method: string; path: string; body: unknown }> =
					[];
				const data = Buffer.from("0123456789abcdef"); // 16B → 2 parts
				const puts: Array<{ url: string; len: number }> = [];
				const uploader = new ChunkedUploader({
					apiRequest: async (method, path, body) => {
						calls.push({ method, path, body });
						if (path.endsWith("/upload_prepare")) {
							return {
								upload_id: "up-1",
								block_size: 8,
								concurrency: 2,
								retry_timeout: 5,
								parts: [
									{ part_index: 1, presigned_url: "/cos-part/1" },
									{ part_index: 2, presigned_url: "/cos-part/2" },
								],
							};
						}
						if (path.endsWith("/upload_part_finish")) return {};
						if (path.endsWith("/files")) return { file_info: "fi-done" };
						throw new Error(`unexpected ${path}`);
					},
					httpPut: async (_url, partBytes) => {
						puts.push({ url: _url, len: partBytes.length });
						return { status: 200 };
					},
					sleep: async () => {},
					monotonicMs: (() => {
						let t = 0;
						return () => (t += 1000);
					})(),
				});
				const done = await uploader.upload({
					chatType: "group",
					targetId: "g_up",
					data,
					fileType: 4,
					fileName: "report.bin",
				});
				expect(done).toEqual({ file_info: "fi-done" });
				expect(puts).toHaveLength(2);
				for (const put of puts) {
					expect(put.len).toBe(8); // block_size slices
					expect(put.url.startsWith("/cos-part/")).toBe(true);
				}
				const paths = calls.map((c) => c.path);
				expect(paths.filter((p) => p.includes("upload_prepare"))).toHaveLength(
					1,
				);
				// Part PUTs ride the httpPut seam (recorded via `puts` above), not
				// the JSON API seam — COS PUTs carry raw bytes, not bodies.
				expect(
					paths.filter((p) => p.includes("upload_part_finish")),
				).toHaveLength(2);
				const finishCalls = calls.filter((c) => c.path.includes("part_finish"));
				for (const c of finishCalls) {
					const b = c.body as Record<string, unknown>;
					expect(b["upload_id"]).toBe("up-1");
					expect([1, 2]).toContain(b["part_index"]);
					expect(b["block_size"]).toBe(8);
					expect(typeof b["md5"]).toBe("string");
				}
				// complete_upload reuses /files with upload_id-only body.
				const completeCall = calls.find((c) => c.path.endsWith("/files"));
				expect(completeCall!.body).toEqual({ upload_id: "up-1" });

				// ── biz 40093001: part_finish retryable until timeout exhausts ──
				let clockMs = 0;
				const retryUploader = new ChunkedUploader({
					apiRequest: async (_m, path) => {
						clockMs += 2000;
						if (path.endsWith("/upload_prepare")) {
							return {
								upload_id: "up-2",
								block_size: 16,
								parts: [{ part_index: 1, url: "/cos-part/x" }],
							};
						}
						if (path.endsWith("/upload_part_finish")) {
							throw new Error(
								"QQ Bot API error [400] x: code=40093001 transient",
							);
						}
						throw new Error(`unexpected ${path}`);
					},
					httpPut: async () => ({ status: 200 }),
					sleep: async () => {},
					monotonicMs: () => clockMs,
				});
				await expect(
					retryUploader.upload({
						chatType: "c2c",
						targetId: "u_up",
						data: Buffer.from("y"),
						fileType: 1,
						fileName: "a.png",
					}),
				).rejects.toThrow(/persistent retry timed out/);

				// ── biz 40093002: daily limit is NON-retryable + TYPED ──
				const limitUploader = new ChunkedUploader({
					apiRequest: async () => {
						throw new Error(
							"QQ Bot API error [400] x: biz_code 40093002 daily limit",
						);
					},
					httpPut: async () => ({ status: 200 }),
					sleep: async () => {},
					monotonicMs: () => 0,
				});
				const err = await limitUploader
					.upload({
						chatType: "c2c",
						targetId: "u_up",
						data: Buffer.from("z"),
						fileType: 1,
						fileName: "big.png",
					})
					.catch((e: unknown) => e);
				expect(err).toBeInstanceOf(UploadDailyLimitExceededError);
				expect((err as UploadDailyLimitExceededError).fileName).toBe("big.png");

				// ── malformed prepare responses rejected ──
				expect(() => parsePrepareResponse({})).toThrow(/upload_id/);
				expect(() =>
					parsePrepareResponse({ upload_id: "x", parts: [] }),
				).toThrow(/parts/);
				// data-wrapping + field aliases tolerated.
				const aliased = parsePrepareResponse({
					data: {
						upload_id: "w",
						part_list: [{ index: 1, url: "/p1" }],
					},
				});
				expect(aliased.parts[0]!.presignedUrl).toBe("/p1");

				expect(formatSize(1536)).toBe("1.5 KB");
			},
		),
		mk(
			"qb.gateway-close-code-matrix",
			"qqbot close-code classes: fatal codes STOP reconnecting (non-retryable fatal); session-invalid codes clear state for fresh Identify; 4004 refreshes the token; 4009 PRESERVES the resumable session; identify carries the full intent bitmask",
			async () => {
				const world = makeQQWorld({ name: "qb-close-matrix" });
				const { engine, gateway, clock } = world;
				await world.connectAndAwaitLive();

				// Identify carried ALL FOUR intents (adapter.py:_send_identify).
				const intents = QQ_IDENTIFY_INTENTS;
				expect(intents & (1 << 25)).toBeTruthy();
				expect(intents & (1 << 30)).toBeTruthy();
				expect(intents & (1 << 12)).toBeTruthy();
				expect(intents & (1 << 26)).toBeTruthy();

				// FATAL close: lifecycle goes fatal, NO reconnect attempts follow.
				gateway.dropActive(4915, "banned");
				await eventually(
					() => engine.lifecycle.statusSnapshot().state === "fatal",
					4_000,
				);
				const reconnectsBeforeFatal = engine.reconnectSteps.length;
				expect(reconnectsBeforeFatal).toBe(0);
				void clock.advance(120_000); // prove nothing schedules behind it
				expect(engine.reconnectSteps.length).toBe(0);
				expect(engine.isLive).toBe(false);
			},
		),
		mk(
			"qb.gateway-session-lifecycle",
			"qqbot session lifecycle: session-invalid closes CLEAR session for re-identify; 4004 clears ONLY the token cache; resume preserves session across soft drops",
			async () => {
				const world = makeQQWorld({ name: "qb-session" });
				const { engine, gateway, clock } = world;
				await world.connectAndAwaitLive();
				const firstSession = engine.sessionId;
				expect(firstSession).not.toBeNull();

				// Soft drop (1001) keeps the session → next Hello sends RESUME.
				gateway.dropActive(1001, "going away");
				void clock.advance(8_000);
				await eventually(() => engine.isLive, 4_000);
				expect(engine.sessionId).toBe(firstSession);
				expect(engine.reconnectLog.includes("resumed")).toBe(true);

				// Session-invalid drop clears the session → fresh IDENTIFY.
				gateway.dropActive(4007, "session no longer valid");
				void clock.advance(16_000);
				await eventually(() => engine.isLive, 6_000);
				expect(engine.sessionId).not.toBe(firstSession);
				expect(
					engine.reconnectLog.some((l) => l === "session-invalid-4007"),
				).toBe(true);

				// 4004 invalid-token drop clears ONLY the token cache.
				const tokenBefore = await engine.ensureToken();
				gateway.script("token", { kind: "ok" }); // allow refresh later
				gateway.dropActive(4004, "invalid token");
				void clock.advance(16_000);
				await eventually(() => engine.isLive, 6_000);
				expect(engine.reconnectLog.includes("invalid-token-4004")).toBe(true);
				// The refreshed token differs from the cached one (cache cleared).
				const tokenAfter = await engine.ensureToken();
				expect(tokenAfter).not.toBe(tokenBefore);
			},
		),
		mk(
			"qb.dedup-window-acl-intake",
			"qqbot intake: 300s-TTL dedup window drops redelivered ids under the injected clock; dm/group ACL policies gate BEFORE dispatch; quoted context (msg_type 103) prepends; @mention prefix stripped on groups; empty bodies dropped",
			async () => {
				const world = makeQQWorld({ name: "qb-dedup-acl" });
				const { engine, gateway } = world;
				await world.connectAndAwaitLive();

				// Dedup: same id twice → ONE turn.
				gateway.pushDispatch(...c2cDispatch("dup-1", "u_dedup", "once only"));
				gateway.pushDispatch(...c2cDispatch("dup-1", "u_dedup", "once only"));
				await eventually(() => engine.turnLog.length >= 1);
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(engine.turnLog).toEqual(["once only"]);
				expect(engine.isDuplicate("dup-1")).toBe(true);
				expect(engine.isDuplicate("never-seen")).toBe(false);

				// Quoted context merge (message_type=103 → msg_elements[0]).
				gateway.pushDispatch(
					...c2cDispatch("q-1", "u_dedup", "my answer", {
						message_type: 103,
						msg_elements: [{ content: "what is pi?" }],
					}),
				);
				await eventually(() => engine.turnLog.length >= 2);
				expect(engine.turnLog[1]).toContain("[Quoted message]:");
				expect(engine.turnLog[1]).toContain("what is pi?");
				expect(engine.turnLog[1]).toContain("my answer");

				// Group ACL: pairing policy DEFAULT-DENIES group traffic…
				gateway.pushDispatch("GROUP_AT_MESSAGE_CREATE", {
					id: "g-1",
					content: "@Bot hello group",
					group_openid: "g_acl",
					author: { member_openid: "m_1" },
				});
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(engine.turnLog.some((t) => t.includes("hello group"))).toBe(
					false,
				);

				// …while open policy admits it AND strips the @mention prefix.
				const worldOpen = makeQQWorld({ name: "qb-acl-open" });
				await worldOpen.connectAndAwaitLive();
				const openAdapter = worldOpen.engine as unknown as {
					groupPolicy: string;
				};
				openAdapter.groupPolicy = "open";
				worldOpen.gateway.pushDispatch("GROUP_AT_MESSAGE_CREATE", {
					id: "g-2",
					content: "@Bot hello group",
					group_openid: "g_open",
					author: { member_openid: "m_1" },
				});
				await eventually(() =>
					worldOpen.engine.turnLog.some((t) => t.includes("hello group")),
				);
				expect(worldOpen.engine.turnLog[0]).toBe("hello group"); // prefix STRIPPED

				// DM disabled policy denies c2c traffic entirely.
				const worldDenied = makeQQWorld({ name: "qb-acl-denied" });
				await worldDenied.connectAndAwaitLive();
				(worldDenied.engine as unknown as { dmPolicy: string }).dmPolicy =
					"disabled";
				worldDenied.gateway.pushDispatch(
					...c2cDispatch("d-1", "u_x", "denied?"),
				);
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(worldDenied.engine.turnLog).toEqual([]);

				// Empty text AND empty attachments → dropped before dispatch.
				gateway.pushDispatch(...c2cDispatch("e-1", "u_dedup", ""));
				await new Promise<void>((r) => setTimeout(r, 20));
				expect(engine.turnLog.length).toBe(2);
			},
		),
		mk(
			"qb.markdown-body-contract",
			"qqbot outbound body: markdown_support rides msg_type=2 markdown.content verbatim with msg_seq; reply_to sets msg_id; text-mode adds message_reference; oversized content truncates at the 4000-unit manifest cap",
			async (fx) => {
				const { engine, gateway } = fx;
				engine.chatTypeMap.set("u_md", "c2c");
				const long = "λ".repeat(QQBOT_MAX_MESSAGE_LENGTH + 500);
				await engine.deliverText("u_md", long.slice(0, 50)); // fits one chunk
				const send = gateway.callsOf("messages:c2c").at(-1)!;
				expect(send.body["msg_type"]).toBe(QQ_MSG_TYPE_MARKDOWN);
				expect(typeof send.body["msg_seq"]).toBe("number");
				const mdContent = (send.body["markdown"] as Record<string, unknown>)[
					"content"
				];
				expect(mdContent).toBe(long.slice(0, 50));

				// Oversized content splits at the chat budget (fence-carry chunker).
				await engine.deliverText("u_md", long);
				const bodies = gateway.callsOf("messages:c2c").slice(1);
				expect(bodies.length).toBeGreaterThan(1);
				let reconstructed = "";
				for (const c of bodies) {
					const content = String(
						(c.body["markdown"] as Record<string, unknown>)["content"],
					);
					expect(content.length).toBeLessThanOrEqual(80); // budget + scaffold
					reconstructed += content.replace(/\n?```\n?/g, "").replace(
						/\s*\(\d+\/\d+\)$/,
						"",
					);
				}
				// Indicator-stripped chunks reconstruct the payload.
				expect(reconstructed.startsWith("λλλλλλλλλλ")).toBe(true);
				expect(reconstructed.length).toBe(long.length);

				// Text-mode body (markdown_support=false) adds message_reference.
				const worldPlain = makeQQWorld({
					name: "qb-md-plain",
					markdownSupport: false,
				});
				const plainEngine = worldPlain.engine;
				plainEngine.chatTypeMap.set("u_t", "c2c");
				const body = plainEngine.buildTextBody("hi", "orig-msg-id");
				expect(body["msg_type"]).toBe(0);
				expect(body["message_reference"]).toEqual({
					message_id: "orig-msg-id",
				});
				void worldPlain;
			},
		),
	];
}

// ── the suite ────────────────────────────────────────────────────────────────

describe("conformance suite — qqbot census port (shape: ws)", () => {
	it("applicability is COMPUTED from capability probes (streaming family excluded iff not declared)", () => {
		const { streamsSupported, excludedIds } = computeApplicability();
		expect(streamsSupported).toBe(false); // NO native streaming (Hermes parity)
		expect(excludedIds).toEqual(STREAMING_ROW_IDS);
	});

	it("manifest production defaults match vendor ground truth", () => {
		expect(QQBOT_MAX_MESSAGE_LENGTH).toBe(4000); // constants.py:MAX_MESSAGE_LENGTH
		expect(QQBOT_RATE_LIMIT_DELAY_S).toBe(60); // constants.py:RATE_LIMIT_DELAY
		expect(QQ_MSG_TYPE_MARKDOWN).toBe(2); // constants.py:MSG_TYPE_MARKDOWN
	});

	it("passes EVERY currently-encoded shared row against the qqbot subject", async () => {
		const all = buildSharedRows({ makeSubject });
		const { streamsSupported } = computeApplicability();
		const rows = streamsSupported
			? all
			: all.filter((r) => !STREAMING_ROW_IDS.includes(r.id));
		// Nothing else may be silently dropped — exclusions are EXACT.
		expect(all.length - rows.length).toBe(streamsSupported ? 0 : 3);
		const report = await runConformanceSuite({
			subjectName: "qqbot",
			shape: "ws",
			rows,
		});
		if (report.failed > 0) console.error(formatReport(report));
		expect(report.failed).toBe(0);
		expect(report.passed).toBeGreaterThanOrEqual(20);
	}, 60_000);

	it("passes ALL FIVE inherited ws-family transport rows against the REAL engine fixture", async () => {
		const rows = makeWsRows(makeRealQQFixture());
		expect(rows.map((r) => r.id)).toEqual(TRANSPORT_ROW_REQUIREMENTS.ws);
		const report = await runConformanceSuite({
			subjectName: "qqbot-transport",
			shape: "ws",
			rows,
			suppliedTransportRowIds: new Set(rows.map((r) => r.id)),
		});
		const failures = report.rows.filter((r) => !r.pass);
		for (const f of failures) console.error(`FAIL ${f.id}: ${f.detail}`);
		expect(failures).toEqual([]);
		expect(report.deferred).toEqual([]);
	}, 40_000);

	it("passes ALL SEVEN qqbot shape-delta rows through the real engine fixture", async () => {
		const rows = qbDeltaRows(() => freshQbFixture("qb-delta"));
		expect(rows.map((r) => r.id)).toEqual([
			"qb.crypto-aes-gcm-negative-matrix",
			"qb.keyboard-interaction-roundtrip",
			"qb.chunked-upload-contracts",
			"qb.gateway-close-code-matrix",
			"qb.gateway-session-lifecycle",
			"qb.dedup-window-acl-intake",
			"qb.markdown-body-contract",
		]);
		const report = await runConformanceSuite({
			subjectName: "qqbot-deltas",
			shape: "ws",
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

		const transport = makeWsRows(makeRealQQFixture());
		const deltas = qbDeltaRows(() => freshQbFixture("qb-full"));

		const report = await runConformanceSuite({
			subjectName: "qqbot-full",
			shape: "ws",
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
		const lying: ConformanceSubject & { adapter: QQBotSubject["adapter"] } =
			makeQQSubject({
				wire: new FakePlatformWire(),
				spawner: scheduler.spawner,
				scheduler,
				name: "qb-liar",
			});
		expect(lying.adapter.supportsDraftStreaming("dm")).toBe(false);
		// Flip ONLY the probe (the lie) — the plane stays absent.
		(
			lying.adapter as unknown as {
				supportsDraftStreaming: () => boolean;
			}
		).supportsDraftStreaming = () => true;

		const all = buildSharedRows({ makeSubject: () => lying });
		const report = await runConformanceSuite({
			subjectName: "qb-capability-liar",
			shape: "ws",
			rows: all.filter((r) => STREAMING_ROW_IDS.includes(r.id)),
		});
		expect(report.rows.length).toBe(3); // re-included BY THE FLIP
		expect(report.failed).toBeGreaterThan(0);
		const failedIds = report.rows.filter((r) => !r.pass).map((r) => r.id);
		expect(failedIds).toContain("streaming.seal-discipline");
	}, 30_000);

	it("the gate DETECTS violations: lying fixtures fail their OWN named rows", async () => {
		// Mutant A: a transport fixture that LOSES disconnect-window events and
		// lies about every capture — every ws family row fails BY NAME.
		const lyingTransport = makeWsRows({
			async resubscribeReplay() {
				return { sentDuringDisconnect: 5, replayedAfterResubscribe: 2 }; // LOST
			},
			async watchdogRecovery() {
				return { detectedDeadSocket: false, resumedWithoutLoss: true };
			},
			async retryAfterCapture() {
				return {
					closeCapturedSeconds: 0, // nothing captured
					nextDelayMs: 1000, // NOT the capture
					delayAuthoritative: false,
					restCapturedSeconds: 3,
				};
			},
			async capabilityLatchPermanence() {
				return {
					latchedOnFirstFailure: true,
					latchCount: 4,
					wireAttemptsAfterSkip: 9,
					supportsStreamingFalse: false, // THE capability lie
					transientDidNotLatch: false,
				};
			},
			async dualPathMarkdown() {
				return {
					nativeRawByteExact: false, // markdown CONVERTED (vendor violation)
					nativePrefixStable: true,
					restConvertedBold: false,
					restConvertedLink: true,
					restConvertedTable: true,
					linkPreviewOnAllTextSends: false,
					linkPreviewAbsentOffTextSends: true,
				};
			},
		});
		const transportReport = await runConformanceSuite({
			subjectName: "lying-qqbot-transport",
			shape: "ws",
			rows: lyingTransport,
			suppliedTransportRowIds: new Set(lyingTransport.map((r) => r.id)),
		});
		expect(transportReport.failed).toBeGreaterThan(0);
		const failedIds = transportReport.rows
			.filter((r) => !r.pass)
			.map((r) => r.id);
		for (const id of TRANSPORT_ROW_REQUIREMENTS.ws) {
			expect(failedIds).toContain(id);
		}

		// Mutant B: an interaction fixture whose gateway swallows
		// INTERACTION_CREATE entirely — the keyboard round-trip row fails BY NAME.
		const rows = qbDeltaRows(async () => {
			const fx = await freshQbFixture("qb-mutant-itx");
			const swallowGateway = new FakeQQGateway();
			const originalPush = swallowGateway.pushDispatch.bind(swallowGateway);
			swallowGateway.pushDispatch = ((
				t: string,
				d: Record<string, unknown>,
			) => {
				if (t === "INTERACTION_CREATE") return; // THE LIE: interactions vanish
				return originalPush(t, d);
			}) as FakeQQGateway["pushDispatch"];
			(fx as { gateway: FakeQQGateway }).gateway = swallowGateway;
			(
				fx.subject as unknown as { adapter: { wsFactory?: unknown } }
			).adapter.wsFactory = swallowGateway;
			return fx;
		});
		const kbRow = rows.find(
			(r) => r.id === "qb.keyboard-interaction-roundtrip",
		) as ConformanceRow;
		const mutantReport = await runConformanceSuite({
			subjectName: "mutant-qb-itx",
			shape: "ws",
			rows: [kbRow],
		});
		expect(mutantReport.failed).toBe(1);
		expect(mutantReport.rows[0]?.pass).toBe(false);

		// Sanity: keyboard grammar itself stays honest under mutation — a
		// tampered decision suffix parses as NOTHING (never misroutes).
		expect(parseApprovalButtonData("approve:sess:allow-always!")).toBeNull();
		expect(parseUpdatePromptButtonData("update_prompt:x")).toBeNull();
	}, 45_000);
});
