// pi_platforms/qqbot/qqbot-attachments.test.ts — the INBOUND ATTACHMENT +
// VOICE-STT + CONNECTION-GATE + PER-LEG-TIMEOUT behavior contracts
// (stability round-2 cluster qqbot-r2, Hermes anchors):
//
//   qq-1 adapter.py:_process_attachments/_stt_voice_attachment/_call_stt —
//        CDN GETs carry 'QQBot <token>' (_qq_media_headers); voice resolves
//        asr_refer_text → voice_wav_url → DIRECT STT POST
//        {base_url}/audio/transcriptions (Bearer + multipart); '[Voice] …'
//        appends so voice-only turns DELIVER (never hit the empty-text/
//        no-image drop gate).
//   qq-2 adapter.py:_handle_c2c_message — media_urls/media_types populate
//        MessageEvent; '[file|video: name (path)]' attachment_info appends;
//        quoted images (msg_type 103) union onto the media lists.
//   qq-4 adapter.py:_wait_for_reconnection — sends gate on the listener and
//        poll ≤15s BEFORE any REST leg; exhaustion ⇒ retryable 'Not connected'
//        (DEC-044 Retry-After capture untouched; DEC-046 timeouts unretried).
//   qq-7 adapter.py:_api_request — per-leg timeouts raise INTO classification;
//        hung legs never stall forever; timeout results are NEVER re-driven.

import { describe, expect, it } from "vitest";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { IncomingEvent } from "../../pi_gateway/guards/index.js";
import { ManualClock } from "../persistent-ws/manual-clock.js";
import { FakeQQGateway } from "./fake-qq-gateway.js";
import type { QQByteFetch, QQByteRequest } from "./qqbot-adapter.js";
import { QQBotAdapter } from "./qqbot-adapter.js";
import { makeQQWorld, c2cDispatch, type QQWorld } from "./qqbot-fixture.js";
import { eventually } from "./eventually.js";

async function liveWorld(
	name: string,
	opts: Parameters<typeof makeQQWorld>[0] = {},
): Promise<QQWorld> {
	const world = makeQQWorld({ name, ...opts });
	await world.connectAndAwaitLive();
	return world;
}

/** Capture every IncomingEvent the handlers deliver (instance-lane wrap). */
function captureEvents(engine: QQWorld["engine"]): IncomingEvent[] {
	const seen: IncomingEvent[] = [];
	const original = engine.deliverInbound.bind(engine);
	(
		engine as unknown as {
			deliverInbound: (event: IncomingEvent, key: string) => Promise<void>;
		}
	).deliverInbound = async (event, key) => {
		seen.push(structuredClone(event));
		await original(event, key);
	};
	return seen;
}

/** Scripted byte seam recording every outbound media/STT call. */
interface ByteCall {
	method: string;
	url: string;
	headers: Record<string, string> | undefined;
	body: Buffer | undefined;
}

function scriptedByteFetch(
	handler: (
		req: QQByteRequest,
		call: ByteCall[],
	) => Promise<{ status: number; bytes: Buffer }>,
): { fetch: QQByteFetch; calls: ByteCall[] } {
	const calls: ByteCall[] = [];
	const fetch: QQByteFetch = async (req) => {
		calls.push({
			method: req.method,
			url: req.url,
			headers: req.headers,
			body: req.body,
		});
		return handler(req, calls);
	};
	return { fetch, calls };
}

/** Save/clear/restore the QQ_STT_* env band around a test body. */
async function withoutSttEnv(body: () => Promise<void>): Promise<void> {
	const saved = {
		QQ_STT_API_KEY: process.env["QQ_STT_API_KEY"],
		QQ_STT_BASE_URL: process.env["QQ_STT_BASE_URL"],
		QQ_STT_MODEL: process.env["QQ_STT_MODEL"],
	};
	delete process.env["QQ_STT_API_KEY"];
	delete process.env["QQ_STT_BASE_URL"];
	delete process.env["QQ_STT_MODEL"];
	try {
		await body();
	} finally {
		for (const [k, v] of Object.entries(saved)) {
			if (v !== undefined) process.env[k] = v;
		}
	}
}

interface WalkableClock {
	advance(ms: number): Promise<void>;
}

/**
 * THE injected-clock walk idiom (retryAfterCapture-row parity): the send
 * pipeline reaches the wire on macrotask yields INSIDE advance(), so tests
 * walk the clock stepwise until a predicate settles instead of assuming
 * registration order.
 */
async function walkClock(
	clock: WalkableClock,
	opts: {
		budgetMs: number;
		stepMs?: number;
		until?: () => boolean;
	},
): Promise<number> {
	const stepMs = opts.stepMs ?? 250;
	let walked = 0;
	while (walked < opts.budgetMs) {
		await clock.advance(stepMs);
		walked += stepMs;
		if (opts.until?.() === true) break;
	}
	return walked;
}

describe("qq-1: voice attachments ride asr_refer_text → wav_url → STT POST", () => {
	it("short-circuits on Tencent's own asr_refer_text with ZERO network calls — and the voice-only turn DELIVERS", async () => {
		await withoutSttEnv(async () => {
			const { fetch, calls } = scriptedByteFetch(async () => ({
				status: 200,
				bytes: Buffer.alloc(0),
			}));
			const world = await liveWorld("qb-att-asr", { byteFetch: fetch });
			const { engine, gateway } = world;
			const seen = captureEvents(engine);

			gateway.pushDispatch(
				...c2cDispatch("att-asr-1", "u_voice", "", {
					attachments: [
						{
							content_type: "voice",
							url: "//multimedia.nt.qq.com.cn/api/v1/attachment/g1/download",
							asr_refer_text: "你好世界",
						},
					],
				}),
			);

			// Voice-only messages previously hit the empty-text/no-image gate
			// and vanished — the '[Voice]' block makes the turn deliverable.
			await eventually(() => engine.turnLog.includes("[Voice] 你好世界"));
			expect(seen).toHaveLength(1);
			// _detect_message_type reads ONLY the cached-image lists — voice
			// rides as transcript TEXT, exactly like Hermes MessageEvent.
			expect(seen[0]!.messageType).toBe("text");
			expect(seen[0]!.text).toBe("[Voice] 你好世界");
			expect(calls).toHaveLength(0); // built-in ASR: free, no API call
			expect(gateway.callsOf("messages:c2c")).toHaveLength(0);
		});
	});

	it("downloads voice_wav_url with the 'QQBot <token>' auth header and transcribes via a DIRECT Bearer+multipart STT POST (GLM shape)", async () => {
		await withoutSttEnv(async () => {
			const { fetch, calls } = scriptedByteFetch(async (req) => {
				if (req.method === "GET") {
					return {
						status: 200,
						bytes: Buffer.from("0123456789abcdef-WAV"), // >10 bytes
					};
				}
				// STT POST: multipart form carries model + audio/wav file part.
				const contentType = req.headers?.["Content-Type"] ?? "";
				const boundary = /boundary=(.+)$/.exec(contentType)?.[1] ?? "";
				expect(boundary).not.toBe("");
				const body = (req.body ?? Buffer.alloc(0)).toString("latin1");
				expect(body).toContain(`--${boundary}`);
				expect(body).toContain('Content-Disposition: form-data; name="model"');
				expect(body).toContain("whisper-1");
				expect(body).toContain('filename="voice.wav"');
				expect(body).toContain("Content-Type: audio/wav");
				expect(req.headers?.["Authorization"]).toBe("Bearer stt-key-1");
				return {
					status: 200,
					bytes: Buffer.from(
						JSON.stringify({
							choices: [{ message: { content: "  转写结果  " } }],
						}),
					),
				};
			});
			const world = await liveWorld("qb-att-stt", {
				byteFetch: fetch,
				stt: { baseUrl: "https://stt.example/v1", apiKey: "stt-key-1" },
			});
			const { engine, gateway } = world;
			const seen = captureEvents(engine);

			gateway.pushDispatch(
				...c2cDispatch("att-stt-1", "u_voice2", "", {
					attachments: [
						{
							content_type: "voice",
							url: "//multimedia.nt.qq.com.cn/api/v1/attachment/raw/download",
							voice_wav_url:
								"//multimedia.nt.qq.com.cn/api/v1/attachment/wav/download",
						},
					],
				}),
			);

			await eventually(() => engine.turnLog.includes("[Voice] 转写结果"));
			// TWO byte calls: the pre-converted WAV download + the STT POST.
			expect(calls).toHaveLength(2);
			const cdnGet = calls[0]!;
			expect(cdnGet.method).toBe("GET");
			expect(cdnGet.url).toBe(
				"https://multimedia.nt.qq.com.cn/api/v1/attachment/wav/download",
			); // '//' normalized onto https
			expect(cdnGet.headers?.["Authorization"]).toMatch(/^QQBot /);
			// Voice rides as TRANSCRIPT text (media lists carry only cached
			// images) — _detect_message_type yields text, Hermes parity.
			expect(seen[0]!.messageType).toBe("text");
		});
	});

	it("parses the OpenAI/Whisper {text} response shape when GLM choices are absent", async () => {
		await withoutSttEnv(async () => {
			const { fetch } = scriptedByteFetch(async (req) => {
				if (req.method === "GET") {
					return {
						status: 200,
						bytes: Buffer.from("0123456789abcdef-WAV"),
					};
				}
				return {
					status: 200,
					bytes: Buffer.from(JSON.stringify({ text: "plain whisper text" })),
				};
			});
			const world = await liveWorld("qb-att-stt-openai", {
				byteFetch: fetch,
				stt: {
					baseUrl: "https://api.openai.com/v1",
					apiKey: "k",
					model: "whisper-1",
				},
			});
			const { engine, gateway } = world;

			gateway.pushDispatch(
				...c2cDispatch("att-stt-2", "u_voice3", "", {
					attachments: [
						{
							content_type: "audio/silk",
							filename: "voice.silk",
							url: "https://multimedia.nt.qq.com.cn/api/v1/attachment/x/download",
						},
					],
				}),
			);
			void gateway;

			// Raw silk + NO conversion bridge ⇒ transcription fails honestly…
			await eventually(() => engine.turnLog.includes("[Voice] [语音识别失败]"));
		});
	});

	it("without STT config OR converter, raw-silk voices surface [语音识别失败] and STILL deliver the turn", async () => {
		await withoutSttEnv(async () => {
			const { fetch, calls } = scriptedByteFetch(async () => ({
				status: 200,
				bytes: Buffer.from("0123456789abcdef-SILK"),
			}));
			const world = await liveWorld("qb-att-nostt", { byteFetch: fetch });
			const { engine, gateway } = world;

			gateway.pushDispatch(
				...c2cDispatch("att-stt-3", "u_voice4", "", {
					attachments: [
						{
							content_type: "voice",
							url: "https://multimedia.nt.qq.com.cn/api/v1/attachment/y/download",
						},
					],
				}),
			);

			await eventually(() => engine.turnLog.includes("[Voice] [语音识别失败]"));
			// Only the CDN GET ran — no STT endpoint was contacted unconfigured.
			expect(calls).toHaveLength(1);
			expect(calls[0]!.url).toContain("/api/v1/attachment/");
			// …and the turn was NOT dropped.
			expect(engine.turnLog).toEqual(["[Voice] [语音识别失败]"]);
		});
	});

	it("routes through the convertVoiceToWav bridge for raw audio when one is wired", async () => {
		await withoutSttEnv(async () => {
			const { fetch } = scriptedByteFetch(async () => ({
				status: 200,
				bytes: Buffer.from("0123456789abcdef-SILK"),
			}));
			let converted: Buffer | null = null;
			const world = await liveWorld("qb-att-convert", {
				byteFetch: fetch,
				stt: { baseUrl: "https://stt.example/v1", apiKey: "k" },
				convertVoiceToWav: async (audio: Buffer, filename: string) => {
					expect(filename).toBe("");
					converted = Buffer.concat([audio, Buffer.from("+wav")]);
					return converted;
				},
			});
			const { engine, gateway } = world;

			gateway.pushDispatch(
				...c2cDispatch("att-stt-4", "u_voice5", "", {
					attachments: [
						{
							content_type: "voice",
							url: "https://multimedia.nt.qq.com.cn/api/v1/attachment/z/download",
						},
					],
				}),
			);

			await eventually(() => engine.turnLog.length >= 1);
			void gateway;
			expect(converted).not.toBeNull();
			expect(engine.turnLog[0]).toMatch(/^\[Voice\] /);
		});
	});
});

describe("qq-2: media_urls/media_types + attachment-info lines reach the event", () => {
	it("carries image attachments as event media refs (URL-shaped without cache dir) and classifies the turn PHOTO", async () => {
		const world = await liveWorld("qb-att-img");
		const { engine, gateway } = world;
		const seen = captureEvents(engine);

		gateway.pushDispatch(
			...c2cDispatch("att-img-1", "u_img", "", {
				attachments: [
					{
						content_type: "image/jpeg",
						filename: "cat.jpg",
						url: "//multimedia.nt.qq.com.cn/api/v1/attachment/img/download",
					},
				],
			}),
		);

		// Image-only: empty text used to be dropped entirely — now it arrives
		// with media references (Hermes MessageEvent.media_urls parity).
		await eventually(() => engine.turnLog.length >= 1);
		expect(seen).toHaveLength(1);
		expect(seen[0]!.messageType).toBe("photo");
		expect(seen[0]!.mediaUrls).toEqual([
			"https://multimedia.nt.qq.com.cn/api/v1/attachment/img/download",
		]);
		expect(seen[0]!.mediaTypes).toEqual(["image/jpeg"]);
	});

	it("appends '[file: name (ref)]'/'[video: name (ref)]' info lines; file-only turns deliver as text", async () => {
		const world = await liveWorld("qb-att-file");
		const { engine, gateway } = world;

		gateway.pushDispatch(
			...c2cDispatch("att-file-1", "u_file", "", {
				attachments: [
					{
						content_type: "file",
						filename: "report.pdf",
						url: "https://multimedia.nt.qq.com.cn/api/v1/attachment/f1/download",
					},
					{
						content_type: "video/mp4",
						filename: "clip.mp4",
						url: "https://multimedia.nt.qq.com.cn/api/v1/attachment/v1/download",
					},
				],
			}),
		);

		await eventually(() =>
			engine.turnLog.some((t) => t.includes("[file: report.pdf")),
		);
		expect(engine.turnLog.join("\n")).toContain(
			"[file: report.pdf (https://multimedia.nt.qq.com.cn/api/v1/attachment/f1/download)]",
		);
		expect(engine.turnLog.join("\n")).toContain(
			"[video: clip.mp4 (https://multimedia.nt.qq.com.cn/api/v1/attachment/v1/download)]",
		);
		// QQ content_type="file" must NOT be misrouted into the voice pipeline.
		expect(engine.turnLog.join("\n")).not.toContain("[Voice]");
	});

	it("merges quoted images (msg_type 103) onto the media lists and quoted voice into the quote block", async () => {
		const world = await liveWorld("qb-att-quote");
		const { engine, gateway } = world;
		const seen = captureEvents(engine);

		gateway.pushDispatch(
			...c2cDispatch("att-quote-1", "u_quote", "my answer", {
				message_type: 103,
				msg_elements: [
					{
						content: "what is pi?",
						attachments: [
							{
								content_type: "image/png",
								filename: "q.png",
								url: "https://multimedia.nt.qq.com.cn/api/v1/attachment/qi/download",
							},
							{
								content_type: "voice",
								url: "https://multimedia.nt.qq.com.cn/api/v1/attachment/qv/download",
								asr_refer_text: "quoted speech",
							},
						],
					},
				],
			}),
		);

		await eventually(() => engine.turnLog.length >= 1);
		expect(seen).toHaveLength(1);
		const event = seen[0]!;
		expect(event.messageType).toBe("photo"); // image leads the merged list
		expect(event.mediaUrls).toEqual([
			"https://multimedia.nt.qq.com.cn/api/v1/attachment/qi/download",
		]);
		expect(event.mediaTypes).toEqual(["image/png"]);
		// Quote block PREPENDS; quoted voice rides INSIDE the block.
		expect(event.text).toContain("[Quoted message]:");
		expect(event.text).toContain("quoted speech");
		expect((event.text ?? "").endsWith("my answer")).toBe(true);
	});

	it("with mediaCacheDir set, downloads cache to disk and event refs become local paths", async () => {
		const cacheDir = await mkdtemp(join(tmpdir(), "qq-cache-"));
		try {
			const png = Buffer.from(
				"\x89PNG\r\n\x1a\n-fake-bytes-over-ten-long",
				"latin1",
			);
			const { fetch } = scriptedByteFetch(async () => ({
				status: 200,
				bytes: png,
			}));
			const world = await liveWorld("qb-att-cache", {
				byteFetch: fetch,
				mediaCacheDir: cacheDir,
			});
			const { engine, gateway } = world;
			const seen = captureEvents(engine);

			gateway.pushDispatch(
				...c2cDispatch("att-cache-1", "u_cache", "", {
					attachments: [
						{
							content_type: "image/png",
							filename: "dog.png",
							url: "https://multimedia.nt.qq.com.cn/api/v1/attachment/d1/download",
						},
					],
				}),
			);

			await eventually(() => seen.length >= 1);
			const ref = seen[0]!.mediaUrls?.[0] ?? "";
			expect(ref.startsWith(cacheDir)).toBe(true);
			expect(ref.endsWith(".jpg")).toBe(true); // ext falls back .jpg per mime table
			expect(await readFile(ref)).toEqual(png);
		} finally {
			await rm(cacheDir, { recursive: true, force: true });
		}
	});
});

describe("qq-4: sends gate on the listener and poll reconnect ≤15s before ANY REST leg", () => {
	function gateWorld(name: string): {
		engine: QQBotAdapter;
		gateway: FakeQQGateway;
		clock: ManualClock;
	} {
		const clock = new ManualClock();
		const gateway = new FakeQQGateway();
		const engine = new QQBotAdapter({
			appId: `gate-app-${name}`,
			clientSecret: "gate-secret",
			rest: {
				async request(method, path, body, headers) {
					const bare = path.startsWith("https://api.sgroup.qq.com")
						? path.slice("https://api.sgroup.qq.com".length)
						: path;
					return gateway.handleRest(
						method,
						bare,
						(body as Record<string, unknown>) ?? {},
						headers,
					);
				},
			},
			wsFactory: gateway,
			sleepMs: clock.sleepMs,
			nowMs: clock.nowMs,
		});
		return { engine, gateway, clock };
	}

	it("a mid-life outage holds sends for the bounded wait, then returns retryable 'Not connected' with ZERO REST traffic", async () => {
		const { engine, gateway, clock } = gateWorld("qb-gate-outage");
		await engine.connect({ isReconnect: false });
		await eventually(() => engine.isConnected && engine.sessionId !== null);
		engine.chatTypeMap.set("u_gate", "c2c");

		// Keep the listener DOWN deterministically: close 4004 clears the
		// cached token AND every reconnect attempt's token leg fails, so the
		// ladder burns its tiers into disable without ever reopening a socket
		// (the fake's async serverRefuse would oscillate liveness instead).
		gateway.script(
			"token",
			...Array.from({ length: 8 }, () => ({
				kind: "fail" as const,
				message: "maintenance window",
			})),
		);
		gateway.dropActive(4004, "invalid token");
		await eventually(() =>
			engine.reconnectLog.some((l) => l.includes("invalid-token")),
		);

		// THE wire lane under test (qq-4 target: adapter.py:send :2486 gate).
		// Driven at wireSend level: the §6.1 base ladder above it sleeps on
		// REAL timers between retries (kit sendWithRetry default), which an
		// instantly-advanced injected clock cannot elapse — the gate contract
		// itself is fully observable here.
		const wiredSend = (
			engine as unknown as {
				wireSend: (
					chatId: string,
					content: string,
					metadata?: Record<string, unknown>,
				) => Promise<
					import("../../pi_gateway/streaming/adapter-seam.js").SendResult
				>;
			}
		).wireSend.bind(engine);

		let settled:
			| import("../../pi_gateway/streaming/adapter-seam.js").SendResult
			| null = null;
		void wiredSend("u_gate", "during outage").then((r) => {
			settled = r;
			return r;
		});
		const walkedMs = await walkClock(clock, {
			budgetMs: 120_000,
			until: () => settled !== null,
		});

		expect(settled).not.toBeNull();
		const result = settled!;
		expect(result.success).toBe(false);
		expect(result.error).toBe("Not connected"); // adapter.py :2487 verbatim
		expect(result.retryable).toBe(true); // retryable per Hermes SendResult
		// The bounded wait was HONORED (≥15s of polls before giving up)…
		expect(walkedMs).toBeGreaterThanOrEqual(15_000);
		// …and the gate NEVER let the chunk ladder touch the REST face.
		expect(gateway.callsOf("messages:c2c")).toHaveLength(0);
	});

	it("recovers MID-WAIT: once the listener reconnects, the held send completes on the wire", async () => {
		const { engine, gateway, clock } = gateWorld("qb-gate-recover");
		await engine.connect({ isReconnect: false });
		await eventually(() => engine.isConnected);
		engine.chatTypeMap.set("u_gate2", "c2c");

		gateway.dropActive(1001, "going away"); // resumable outage; ladder recovers at the 2s tier
		await eventually(() => engine.reconnectLog.length >= 1);

		type Deliver = Awaited<ReturnType<typeof engine.deliverText>>;
		let settled: Deliver | null = null;
		void engine.deliverText("u_gate2", "after recovery").then((r) => {
			settled = r;
			return r;
		});
		await walkClock(clock, { budgetMs: 40_000, until: () => settled !== null });

		expect(settled).not.toBeNull();
		const results = settled!;
		expect(results.every((r) => r.success)).toBe(true);
		expect(gateway.callsOf("messages:c2c").length).toBeGreaterThanOrEqual(1);
		expect(engine.isConnected).toBe(true); // ladder restored the listener mid-wait
	});

	it("transmitMedia is gated identically — no files/messages legs fire while down", async () => {
		const { engine, gateway, clock } = gateWorld("qb-gate-media");
		await engine.connect({ isReconnect: false });
		await eventually(() => engine.isConnected);
		engine.chatTypeMap.set("u_gate3", "c2c");

		// Keep the listener DOWN deterministically: close 4004 clears the
		// cached token AND every reconnect attempt's token leg fails, so the
		// ladder burns its tiers into disable without ever reopening a socket
		// (the fake's async serverRefuse would oscillate liveness instead).
		gateway.script(
			"token",
			...Array.from({ length: 8 }, () => ({
				kind: "fail" as const,
				message: "maintenance window",
			})),
		);
		gateway.dropActive(4004, "invalid token");
		await eventually(() =>
			engine.reconnectLog.some((l) => l.includes("invalid-token")),
		);

		type SendResultT = Awaited<ReturnType<typeof engine.sendImage>>;
		let settled: SendResultT | null = null;
		void engine
			.sendImage("u_gate3", "https://cdn.example/pic.png", "caption")
			.then((r) => {
				settled = r;
				return r;
			});
		await walkClock(clock, {
			budgetMs: 200_000,
			until: () => settled !== null,
		});

		expect(settled).not.toBeNull();
		const result = settled!;
		expect(result.success).toBe(false);
		expect(result.error).toBe("Not connected");
		expect(result.retryable).toBe(true);
		// NO upload leg, NO delivery leg, and NO text-URL fallback either —
		// adapter.py returns 'Not connected' out of _send_media :2913 BEFORE
		// any lane exists to fall back from.
		expect(gateway.callsOf("files")).toHaveLength(0);
		expect(gateway.callsOf("messages:c2c")).toHaveLength(0);
	});

	it("sendWithKeyboard is gated identically (adapter.py :2634)", async () => {
		const { engine, gateway, clock } = gateWorld("qb-gate-kb");
		await engine.connect({ isReconnect: false });
		await eventually(() => engine.isConnected);
		engine.chatTypeMap.set("u_gate4", "c2c");

		// Keep the listener DOWN deterministically: close 4004 clears the
		// cached token AND every reconnect attempt's token leg fails, so the
		// ladder burns its tiers into disable without ever reopening a socket
		// (the fake's async serverRefuse would oscillate liveness instead).
		gateway.script(
			"token",
			...Array.from({ length: 8 }, () => ({
				kind: "fail" as const,
				message: "maintenance window",
			})),
		);
		gateway.dropActive(4004, "invalid token");
		await eventually(() =>
			engine.reconnectLog.some((l) => l.includes("invalid-token")),
		);

		type KbResult = Awaited<ReturnType<typeof engine.sendWithKeyboard>>;
		let settled: KbResult | null = null;
		void engine
			.sendWithKeyboard("u_gate4", "hi", { rows: [] } as never)
			.then((r) => {
				settled = r;
				return r;
			});
		await walkClock(clock, {
			budgetMs: 200_000,
			until: () => settled !== null,
		});

		expect(settled).not.toBeNull();
		const result = settled!;
		expect(result.error).toBe("Not connected");
		expect(result.retryable).toBe(true);
	});
});

describe("qq-7: per-leg timeouts raise INTO classification instead of discarding", () => {
	function hangingWorld(): {
		engine: QQBotAdapter;
		clock: ManualClock;
		gateway: FakeQQGateway;
		attempts: string[];
		releaseAll: () => void;
	} {
		const clock = new ManualClock();
		const gateway = new FakeQQGateway();
		const attempts: string[] = [];
		let resolveHang: (() => void) | null = null;
		const hangPromise = new Promise<{
			status: number;
			body: Record<string, unknown>;
		}>((resolve) => {
			resolveHang = () => resolve({ status: 200, body: { id: "late-reply" } });
		});
		const engine = new QQBotAdapter({
			appId: "to-app",
			clientSecret: "to-secret",
			rest: {
				async request(method, path) {
					attempts.push(`${method} ${path}`);
					// Token + gateway legs resolve instantly; message legs HANG.
					if (path.includes("getAppAccessToken")) {
						return {
							status: 200,
							body: { access_token: "tok", expires_in: 7200 },
						};
					}
					if (path.endsWith("/gateway")) {
						return { status: 200, body: { url: gateway.gatewayUrl } };
					}
					return hangPromise;
				},
			},
			wsFactory: gateway,
			sleepMs: clock.sleepMs,
			nowMs: clock.nowMs,
		});
		return {
			engine,
			clock,
			gateway,
			attempts,
			releaseAll: () => resolveHang?.(),
		};
	}

	it("a hung message leg times out at DEFAULT_API_TIMEOUT (30s), classifies TIMEOUT, and is NEVER retried (DEC-046)", async () => {
		const { engine, clock, attempts, releaseAll } = hangingWorld();
		await engine.connect({ isReconnect: false });
		await eventually(() => engine.isConnected);
		engine.chatTypeMap.set("u_to", "c2c");

		type Deliver = Awaited<ReturnType<typeof engine.deliverText>>;
		let settled: Deliver | null = null;
		void engine.deliverText("u_to", "hang test").then((r) => {
			settled = r;
			return r;
		});
		// Walk past the 30s per-leg budget (the walk ALSO yields the pipeline
		// onto the wire — retryAfterCapture-row idiom).
		await walkClock(clock, { budgetMs: 60_000, until: () => settled !== null });

		expect(settled).not.toBeNull();
		const results = settled!;
		const last = results[results.length - 1]!;
		expect(last.success).toBe(false);
		expect(last.retryable).toBe(false); // timeout-classified ⇒ non-retryable
		expect(String(last.error)).toContain("QQ Bot API timeout");

		// The ladder did NOT re-drive the ambiguous send: exactly ONE
		// message-leg attempt despite QQ_SEND_MAX_ATTEMPTS=3 (DEC-046).
		const messageLegs = attempts.filter((a) => a.includes("/messages")).length;
		expect(messageLegs).toBe(1);
		releaseAll();
	});

	it("the upload leg honors FILE_UPLOAD_TIMEOUT (120s) — still pending at the 30s mark", async () => {
		const { engine, clock, attempts, releaseAll } = hangingWorld();
		await engine.connect({ isReconnect: false });
		await eventually(() => engine.isConnected);
		engine.chatTypeMap.set("u_up", "c2c");

		let err: unknown = null;
		void engine
			.uploadMedia({
				chatType: "c2c",
				targetId: "u_up",
				fileType: 1,
				url: "https://x.example/a.png",
			})
			.catch((e: unknown) => {
				err = e;
				return {} as Record<string, unknown>;
			});

		// Walk to ~31s: a DEFAULT_API_TIMEOUT leg would have fired by now.
		await walkClock(clock, { budgetMs: 31_000 });
		expect(err).toBeNull(); // uploads wait the 120s FILE_UPLOAD budget

		// Keep walking to ~125s: the upload race raises INTO classification.
		await walkClock(clock, {
			budgetMs: 100_000,
			until: () => err !== null,
		});
		expect(String(err)).toContain("QQ Bot API timeout [/v2/users/u_up/files]");
		// Timeout-classified upload failures are rethrown IMMEDIATELY by the
		// _upload_media ladder (no transient-retry burn).
		const uploadLegs = attempts.filter((a) => a.includes("/files")).length;
		expect(uploadLegs).toBe(1);
		releaseAll();
	});

	it("interaction ACKs carry the 30s budget; swallowed failures never spam the wire", async () => {
		const { engine, clock, attempts, releaseAll } = hangingWorld();
		await engine.connect({ isReconnect: false });
		await eventually(() => engine.isConnected);

		void engine.onInteraction({
			id: "it-to",
			chat_type: 11,
			user_openid: "u_to",
			data: { resolved: { button_data: "zz:bogus" } },
		});
		await walkClock(clock, {
			budgetMs: 60_000,
			until: () => engine.interactionAcks.length >= 1,
		});

		expect(engine.interactionAcks[0]).toMatchObject({
			id: "it-to",
			code: -1, // hung ACK timed out and was swallowed (debug-class)
		});
		expect(attempts.filter((a) => a.includes("/interactions/"))).toHaveLength(
			1,
		); // exactly ONE attempt — failures are never spammed
		releaseAll();
	});
});
