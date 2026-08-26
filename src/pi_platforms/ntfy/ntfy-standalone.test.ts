// BEHAVIOR CONTRACTS — stability round cluster `cron-standalone-delivery`
// (nthaha-10). Every test pins CONFORMING behavior against the READ-ONLY
// Hermes reference (plugins/platforms/ntfy/adapter.py:_standalone_send),
// never the pre-fix port gap:
//
//   nthaha-10  Pi had NO standaloneSenderFn anywhere — the kit hook was a
//              comment-only reservation, so deliver=ntfy cron jobs running
//              out-of-process failed with "No live adapter for platform".
//              The ported sender POSTs {server}/{publish_topic} with
//              text/plain + X-Tags echo tag + optional X-Markdown under a
//              15s budget, reusing the manifest constants and the SHARED
//              auth builder (adapter.py:_build_auth_header is common to the
//              live adapter and _standalone_send — both paths must follow
//              the same auth shape and whitespace rules).

import { describe, expect, it } from "vitest";

import { PluginContext } from "../kit/index.js";
import {
	NTFY_DEFAULT_SERVER,
	NTFY_ECHO_TAG,
	NTFY_MAX_MESSAGE_CHARS,
	NTFY_PUBLISH_TIMEOUT_MS,
} from "./manifest.js";
import {
	makeNtfyStandaloneSender,
	registerNtfyPlatform,
	type NtfyStandalonePostRequest,
	type NtfyStandalonePostResponse,
	type NtfyStandaloneTransport,
} from "./ntfy-standalone.js";

// ── scripted transport ───────────────────────────────────────────────────────

interface CapturedRequest extends NtfyStandalonePostRequest {}

function scriptedTransport(
	outcomes: Array<NtfyStandalonePostResponse | Error> = [],
): { transport: NtfyStandaloneTransport; requests: CapturedRequest[] } {
	const requests: CapturedRequest[] = [];
	let i = 0;
	return {
		requests,
		transport: {
			post: async (req) => {
				requests.push({ ...req });
				const outcome = outcomes[i];
				i += 1;
				if (outcome instanceof Error) throw outcome;
				return (
					outcome ?? {
						status: 200,
						text: JSON.stringify({ id: "pub-1" }),
						json: { id: "pub-1" },
					}
				);
			},
		},
	};
}

function okJson(json: unknown): NtfyStandalonePostResponse {
	return { status: 200, text: JSON.stringify(json), json };
}

const NO_ENV = () => undefined;

// ── the wire contract ────────────────────────────────────────────────────────

describe("nthaha-10: standalone publish wire shape (adapter.py:_standalone_send)", () => {
	it("POSTs {server}/{publish_topic} text/plain with X-Tags echo tag + Bearer auth under the 15s budget", async () => {
		const { transport, requests } = scriptedTransport([
			okJson({ id: "abc123" }),
		]);
		const send = makeNtfyStandaloneSender({
			config: { serverUrl: "https://ntfy.ops.example", topic: "home" },
			secretReader: (k) => (k === "NTFY_TOKEN" ? " tk_1 " : undefined),
			transport,
		});
		const result = await send({ chatId: "", message: "hello cron" });

		expect(requests).toHaveLength(1);
		const req = requests[0]!;
		expect(req.url).toBe("https://ntfy.ops.example/home");
		expect(req.headers["Content-Type"]).toBe("text/plain; charset=utf-8");
		expect(req.headers["X-Tags"]).toBe(NTFY_ECHO_TAG);
		expect(req.headers["Authorization"]).toBe("Bearer tk_1");
		expect(req.body).toBe("hello cron");
		expect(req.timeoutMs).toBe(NTFY_PUBLISH_TIMEOUT_MS);
		expect(NTFY_PUBLISH_TIMEOUT_MS).toBe(15_000);
		expect(result).toEqual({
			success: true,
			platform: "ntfy",
			chatId: "home",
			messageId: "abc123",
		});
	});

	it("returns the server id verbatim; unparsable body mints a uuid4().hex[:12] fallback", async () => {
		const fallback = scriptedTransport([
			{ status: 200, text: "<html>not json</html>", json: undefined },
		]);
		const send = makeNtfyStandaloneSender({
			config: { topic: "t" },
			secretReader: NO_ENV,
			transport: fallback.transport,
		});
		const result = await send({ chatId: "chat-a", message: "m" });
		expect(result).toEqual({
			success: true,
			platform: "ntfy",
			chatId: "chat-a",
			messageId: expect.stringMatching(/^[0-9a-f]{12}$/u),
		});

		const noId = scriptedTransport([okJson({ time: 1 })]);
		const send2 = makeNtfyStandaloneSender({
			config: { topic: "t" },
			secretReader: NO_ENV,
			transport: noId.transport,
		});
		const result2 = await send2({ chatId: "chat-b", message: "m" });
		if ("error" in result2) throw new Error(result2.error);
		expect(result2.messageId).toMatch(/^[0-9a-f]{12}$/u);

		// Falsy id (empty string) ALSO takes the fallback (data.get("id") or …).
		const emptyId = scriptedTransport([okJson({ id: "" })]);
		const send3 = makeNtfyStandaloneSender({
			config: { topic: "t" },
			secretReader: NO_ENV,
			transport: emptyId.transport,
		});
		const result3 = await send3({ chatId: "chat-c", message: "m" });
		if ("error" in result3) throw new Error(result3.error);
		expect(result3.messageId).toMatch(/^[0-9a-f]{12}$/u);
	});

	it("truncates the body at the 4096-char manifest cap (_truncate_body parity)", async () => {
		const { transport, requests } = scriptedTransport();
		const send = makeNtfyStandaloneSender({
			config: { topic: "t" },
			secretReader: NO_ENV,
			transport,
		});
		await send({
			chatId: "",
			message: "x".repeat(NTFY_MAX_MESSAGE_CHARS + 900),
		});
		expect(requests[0]!.body).toHaveLength(4096);
		expect(requests[0]!.body.startsWith("xxxxxxxxxx")).toBe(true);
		// Under the cap the message rides VERBATIM (no relabeling/chunking).
		await send({ chatId: "", message: "short one" });
		expect(requests[1]!.body).toBe("short one");
	});
});

// ── resolution ladders (Python `or` fall-through) ────────────────────────────

describe("nthaha-10: publish_topic chain chat_id → extra.publish_topic → NTFY_PUBLISH_TOPIC → extra.topic → NTFY_TOPIC", () => {
	const make = (
		opts: {
			config?: { publishTopic?: string; topic?: string };
			env?: Record<string, string>;
		} = {},
	): {
		send: ReturnType<typeof makeNtfyStandaloneSender>;
		requests: CapturedRequest[];
	} => {
		const { transport, requests } = scriptedTransport();
		const send = makeNtfyStandaloneSender({
			config: opts.config,
			secretReader: (k) => opts.env?.[k],
			transport,
		});
		return { send, requests };
	};

	it("chat_id wins over every configured lane; URL carries the winning topic", async () => {
		const { send, requests } = make({
			config: { publishTopic: "pub", topic: "sub" },
			env: { NTFY_PUBLISH_TOPIC: "env-pub", NTFY_TOPIC: "env-sub" },
		});
		const r = await send({ chatId: "caller-topic", message: "m" });
		expect(requests[0]!.url).toBe(`${NTFY_DEFAULT_SERVER}/caller-topic`);
		if ("error" in r) throw new Error(r.error);
		expect(r.chatId).toBe("caller-topic");
	});

	it("empty chat_id defers to configured publishTopic, then NTFY_PUBLISH_TOPIC", async () => {
		const a = make({
			config: { publishTopic: "pub", topic: "sub" },
			env: { NTFY_PUBLISH_TOPIC: "env-pub" },
		});
		await a.send({ chatId: "", message: "m" });
		expect(a.requests[0]!.url).toBe(`${NTFY_DEFAULT_SERVER}/pub`);

		const b = make({
			config: { topic: "sub" },
			env: { NTFY_PUBLISH_TOPIC: "  env-pub  " },
		});
		await b.send({ chatId: "", message: "m" });
		expect(b.requests[0]!.url).toBe(`${NTFY_DEFAULT_SERVER}/env-pub`);
	});

	it("extra.topic then NTFY_TOPIC close the chain (env value stripped)", async () => {
		const a = make({ config: { topic: "sub" } });
		await a.send({ chatId: "", message: "m" });
		expect(a.requests[0]!.url).toBe(`${NTFY_DEFAULT_SERVER}/sub`);

		const b = make({ env: { NTFY_TOPIC: "  env-sub " } });
		await b.send({ chatId: "", message: "m" });
		expect(b.requests[0]!.url).toBe(`${NTFY_DEFAULT_SERVER}/env-sub`);
	});

	it("an exhausted chain fails LOUDLY with the reference error, sending nothing", async () => {
		const { send, requests } = make({});
		// Raw chat_id participates with Python truthiness: whitespace is truthy
		// and WINS; only an empty/absent chat_id lets the chain fall through.
		const ws = make({});
		await ws.send({ chatId: "  ", message: "m" });
		expect(ws.requests[0]!.url).toBe(`${NTFY_DEFAULT_SERVER}/  `);

		const r = await send({ chatId: "", message: "m" });
		expect(r).toEqual({
			error: "ntfy standalone send: NTFY_TOPIC not configured",
		});
		expect(requests).toHaveLength(0);
	});

	it("server lane: config.serverUrl → NTFY_SERVER_URL → DEFAULT_SERVER, trailing slashes stripped", async () => {
		const a = make({ env: { NTFY_SERVER_URL: "https://env.example//" } });
		await a.send({ chatId: "t", message: "m" });
		expect(a.requests[0]!.url).toBe("https://env.example/t");

		const b = make({});
		await b.send({ chatId: "t", message: "m" });
		expect(b.requests[0]!.url).toBe(`https://ntfy.sh/t`);
		expect(NTFY_DEFAULT_SERVER).toBe("https://ntfy.sh");
	});

	it("SET-BUT-EMPTY lanes fall through like Python `or`", async () => {
		const { send, requests } = make({
			config: { publishTopic: "", topic: "fallback-topic" },
			env: { NTFY_PUBLISH_TOPIC: "" },
		});
		await send({ chatId: "", message: "m" });
		expect(requests[0]!.url).toBe(`${NTFY_DEFAULT_SERVER}/fallback-topic`);
	});
});

// ── auth + markdown gates ────────────────────────────────────────────────────

describe("nthaha-10: auth builder + markdown lanes", () => {
	it("user:pass tokens become Basic (shared _build_auth_header shape); bare tokens Bearer", async () => {
		const basic = scriptedTransport();
		const sendBasic = makeNtfyStandaloneSender({
			config: { topic: "t", token: "user:pass" },
			secretReader: NO_ENV,
			transport: basic.transport,
		});
		await sendBasic({ chatId: "", message: "m" });
		expect(basic.requests[0]!.headers["Authorization"]).toBe(
			`Basic ${Buffer.from("user:pass", "utf8").toString("base64")}`,
		);

		const ws = scriptedTransport();
		const sendWs = makeNtfyStandaloneSender({
			config: { topic: "t", token: "  tk_9 \n" },
			secretReader: NO_ENV,
			transport: ws.transport,
		});
		await sendWs({ chatId: "", message: "m" });
		expect(ws.requests[0]!.headers["Authorization"]).toBe("Bearer tk_9");
	});

	it("no token on ANY lane ⇒ NO Authorization header at all", async () => {
		const { transport, requests } = scriptedTransport();
		const send = makeNtfyStandaloneSender({
			config: { topic: "t" },
			secretReader: NO_ENV,
			transport,
		});
		await send({ chatId: "", message: "m" });
		expect(requests[0]!.headers["Authorization"]).toBeUndefined();
	});

	it("X-Markdown rides ONLY when extra.markdown is truthy OR env ∈ {1,true,yes} (case-insensitive)", async () => {
		const cases: Array<{
			name: string;
			configMarkdown?: boolean;
			env?: string;
			present: boolean;
		}> = [
			{ name: "extra true, env unset", configMarkdown: true, present: true },
			{ name: "env 1", env: "1", present: true },
			{ name: "env yes", env: "yes", present: true },
			{ name: "env TRUE (case-fold)", env: "TRUE", present: true },
			{ name: "env 0", env: "0", present: false },
			{ name: "env blank", env: "  ", present: false },
			{ name: "neither", present: false },
			{
				name: "extra false defers to env (bool(extra)=false)",
				configMarkdown: false,
				env: "true",
				present: true,
			},
		];
		for (const c of cases) {
			const { transport, requests } = scriptedTransport();
			const send = makeNtfyStandaloneSender({
				config:
					c.configMarkdown === undefined
						? { topic: "t" }
						: { topic: "t", markdown: c.configMarkdown },
				secretReader: (k) =>
					k === "NTFY_MARKDOWN" && c.env !== undefined ? c.env : undefined,
				transport,
			});
			await send({ chatId: "", message: "m" });
			if (c.present) {
				expect(requests[0]!.headers["X-Markdown"], c.name).toBe("true");
			} else {
				expect(requests[0]!.headers["X-Markdown"], c.name).toBeUndefined();
			}
		}
	});
});

// ── failure mapping ──────────────────────────────────────────────────────────

describe("nthaha-10: failure mapping (≥300 / transport death)", () => {
	it("status ≥300 ⇒ 'ntfy HTTP {s}: {text[:200]}' — body sliced at 200 chars", async () => {
		const longText = "e".repeat(500);
		const { transport } = scriptedTransport([
			{ status: 500, text: longText, json: undefined },
		]);
		const send = makeNtfyStandaloneSender({
			config: { topic: "t" },
			secretReader: NO_ENV,
			transport,
		});
		const r = await send({ chatId: "t2", message: "m" });
		expect(r).toEqual({ error: `ntfy HTTP 500: ${"e".repeat(200)}` });
	});

	it("thrown transport deaths (incl. timeouts) ride the generic failure lane", async () => {
		const { transport } = scriptedTransport([new Error("boom")]);
		const send = makeNtfyStandaloneSender({
			config: { topic: "t" },
			secretReader: NO_ENV,
			transport,
		});
		const r = await send({ chatId: "", message: "m" });
		expect(r).toEqual({ error: "ntfy standalone send failed: boom" });
	});

	it("thread/media kwargs are signature-parity ONLY — the wire request is identical", async () => {
		const plain = scriptedTransport();
		const kwar = scriptedTransport();
		const mk = (t: NtfyStandaloneTransport) =>
			makeNtfyStandaloneSender({
				config: { topic: "t" },
				secretReader: NO_ENV,
				transport: t,
			});
		const a = await mk(plain.transport)({ chatId: "c", message: "m" });
		const b = await mk(kwar.transport)({
			chatId: "c",
			message: "m",
			threadId: "th-1",
			mediaFiles: ["/tmp/a.png"],
			forceDocument: true,
		});
		expect(b).toEqual(a);
		expect(kwar.requests[0]).toEqual(plain.requests[0]);
	});
});

// ── registration seam ────────────────────────────────────────────────────────

describe("nthaha-10: kit registration seam wires standaloneSenderFn", () => {
	it("registerNtfyPlatform registers under 'ntfy' with the sender retrievable and drivable", async () => {
		const { transport, requests } = scriptedTransport([okJson({ id: "w1" })]);
		const ctx = new PluginContext(() => "topic-value"); // NTFY_TOPIC present
		const disabled = registerNtfyPlatform(ctx, () => ({}), {
			standalone: {
				secretReader: (k) => (k === "NTFY_TOPIC" ? "topic-value" : undefined),
				transport,
			},
		});
		expect(disabled).toBeNull();

		const registered = ctx.getPlatform("ntfy");
		expect(registered?.manifestName).toBe("ntfy");

		const sender = ctx.getStandaloneSender("ntfy");
		expect(sender).toBeTypeOf("function");
		const result = await sender!({ chatId: "", message: "cron says hi" });
		// Topic resolved through the SENDER's own reader (chat_id empty ⇒ lane 4).
		expect(result).toEqual({
			success: true,
			platform: "ntfy",
			chatId: "topic-value",
			messageId: "w1",
		});
		expect(requests[0]!.url).toBe(`https://ntfy.sh/topic-value`);
	});

	it("the hook stays OPTIONAL — platforms registering without it expose no sender", () => {
		const ctx = new PluginContext(() => undefined);
		ctx.registerPlatform(
			{
				name: "bare",
				description: "",
				transportShape: "polling",
				requiresEnv: [],
				capabilities: {},
			},
			() => ({}),
		);
		expect(ctx.getStandaloneSender("bare")).toBeUndefined();
	});
});
