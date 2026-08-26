// BEHAVIOR CONTRACTS — stability round cluster `cron-standalone-delivery`
// (nthaha-10, homeassistant arm). Every test pins CONFORMING behavior
// against the READ-ONLY Hermes reference
// (plugins/platforms/homeassistant/adapter.py:_standalone_send):
//
//   POST {hass_url}/api/services/notify/notify with Bearer {HASS_TOKEN} and
//   JSON payload {"message", "target": chat_id} — NO title key and NO
//   message truncation on this path; 200/201 accepted, anything else ⇒
//   "Home Assistant API error ({s}): {body}"; timeout ⇒ the dedicated
//   timeout sentence; other transport deaths ⇒ "Home Assistant send
//   failed: {e}"; missing url OR token ⇒ the loud both-required error.

import { describe, expect, it } from "vitest";

import { PluginContext } from "../kit/index.js";
import { HA_REST_NOTIFY_NOTIFY, HA_STANDALONE_TIMEOUT_MS } from "./manifest.js";
import {
	makeHomeAssistantStandaloneSender,
	registerHomeAssistantPlatform,
	type HaStandaloneNotifyRequest,
	type HaStandaloneTransport,
	type HaStandaloneTransportOutcome,
} from "./homeassistant-standalone.js";

// ── scripted transport ───────────────────────────────────────────────────────

interface CapturedRequest extends HaStandaloneNotifyRequest {}

function scriptedTransport(
	outcomes: Array<HaStandaloneTransportOutcome | Error> = [],
): { transport: HaStandaloneTransport; requests: CapturedRequest[] } {
	const requests: CapturedRequest[] = [];
	let i = 0;
	return {
		requests,
		transport: {
			postNotify: async (req) => {
				requests.push({
					...req,
					payload: { ...req.payload },
					headers: { ...req.headers },
				});
				const outcome = outcomes[i];
				i += 1;
				if (outcome instanceof Error) throw outcome;
				return outcome ?? { kind: "response", status: 200, body: "" };
			},
		},
	};
}

const NO_ENV = () => undefined;

function make(
	opts: {
		config?: { url?: string; token?: string };
		env?: Record<string, string>;
		outcomes?: Array<HaStandaloneTransportOutcome | Error>;
	} = {},
): {
	send: ReturnType<typeof makeHomeAssistantStandaloneSender>;
	requests: CapturedRequest[];
} {
	const { transport, requests } = scriptedTransport(opts.outcomes);
	const send = makeHomeAssistantStandaloneSender({
		config: opts.config,
		secretReader: (k) => opts.env?.[k],
		transport,
	});
	return { send, requests };
}

// ── the wire contract ────────────────────────────────────────────────────────

describe("nthaha-10 HA: notify.notify wire shape (adapter.py:_standalone_send)", () => {
	it("POSTs {hass_url}/api/services/notify/notify with Bearer + payload {message, target} — no title, under the 30s budget", async () => {
		const { send, requests } = make({
			config: { url: "http://ha.local:8123", token: " tok-1 " },
		});
		const r = await send({ chatId: "kitchen_speaker", message: "cron done" });

		expect(requests).toHaveLength(1);
		const req = requests[0]!;
		expect(req.url).toBe(`http://ha.local:8123${HA_REST_NOTIFY_NOTIFY}`);
		expect(req.headers["Authorization"]).toBe("Bearer tok-1");
		expect(req.headers["Content-Type"]).toBe("application/json");
		// The standalone payload carries NO title key (unlike live send) and
		// rides chat_id as `target` VERBATIM.
		expect(req.payload).toEqual({
			message: "cron done",
			target: "kitchen_speaker",
		});
		expect(Object.keys(req.payload).sort()).toEqual(["message", "target"]);
		expect(req.timeoutMs).toBe(HA_STANDALONE_TIMEOUT_MS);
		expect(HA_STANDALONE_TIMEOUT_MS).toBe(30_000);
		expect(r).toEqual({
			success: true,
			platform: "homeassistant",
			chatId: "kitchen_speaker",
		});
	});

	it("the message is NOT truncated on this path (>4096 rides whole — reference sends it raw)", async () => {
		const long = "y".repeat(5000);
		const { send, requests } = make({
			config: { url: "http://ha.local", token: "t" },
		});
		await send({ chatId: "c", message: long });
		expect(requests[0]!.payload.message).toHaveLength(5000);
	});

	it("url lane: config.url → HASS_URL env, trailing slashes stripped; token lane strips whitespace", async () => {
		const a = make({
			env: { HASS_URL: "http://from-env:8123///", HASS_TOKEN: " tk-e " },
		});
		await a.send({ chatId: "c", message: "m" });
		expect(a.requests[0]!.url).toBe(
			`http://from-env:8123${HA_REST_NOTIFY_NOTIFY}`,
		);
		expect(a.requests[0]!.headers["Authorization"]).toBe("Bearer tk-e");

		// SET-BUT-EMPTY config.url defers to env (Python `or` fall-through).
		const b = make({
			config: { url: "", token: "" },
			env: { HASS_URL: "http://env-wins:8123", HASS_TOKEN: "tk2" },
		});
		await b.send({ chatId: "c", message: "m" });
		expect(b.requests[0]!.url).toBe(
			`http://env-wins:8123${HA_REST_NOTIFY_NOTIFY}`,
		);
	});

	it("thread/media kwargs are signature-parity ONLY — ignored on the wire", async () => {
		const plain = make({});
		const kwar = make({});
		await plain.send({ chatId: "c", message: "m" });
		await kwar.send({
			chatId: "c",
			message: "m",
			threadId: "th",
			mediaFiles: ["/tmp/x.mp3"],
			forceDocument: true,
		});
		expect(kwar.requests[0]).toEqual(plain.requests[0]);
	});
});

// ── admission gate ───────────────────────────────────────────────────────────

describe("nthaha-10 HA: both-credentials admission gate", () => {
	const BOTH_REQUIRED =
		"Home Assistant standalone send: HASS_URL and HASS_TOKEN must both be set";

	it("missing either lane fails LOUDLY before any wire op (no default URL here)", async () => {
		for (const opts of [
			{},
			{ config: { token: "only-token" } },
			{ config: { url: "http://only-url" } },
			{
				config: { url: "", token: "  " },
				env: { HASS_URL: "", HASS_TOKEN: "  " },
			},
		]) {
			const { send, requests } = make(opts);
			const r = await send({ chatId: "c", message: "m" });
			expect(r).toEqual({ error: BOTH_REQUIRED });
			expect(requests).toHaveLength(0);
		}
	});

	it("a whitespace-only token counts as MISSING (strip precedes the check)", async () => {
		const { send, requests } = make({
			config: { url: "http://ha.local", token: " \n\t " },
		});
		expect(await send({ chatId: "c", message: "m" })).toEqual({
			error: BOTH_REQUIRED,
		});
		expect(requests).toHaveLength(0);
	});
});

// ── status + failure mapping ────────────────────────────────────────────────

describe("nthaha-10 HA: status/failure mapping", () => {
	it("200 AND 201 succeed; every other status ⇒ 'Home Assistant API error ({s}): {body}'", async () => {
		for (const status of [200, 201]) {
			const { send } = make({
				config: { url: "http://ha", token: "t" },
				outcomes: [{ kind: "response", status, body: "" }],
			});
			expect(await send({ chatId: "c", message: "m" })).toEqual({
				success: true,
				platform: "homeassistant",
				chatId: "c",
			});
		}
		for (const status of [202, 204, 301, 400, 401, 500]) {
			const { send } = make({
				config: { url: "http://ha", token: "t" },
				outcomes: [{ kind: "response", status, body: "<html>oops</html>" }],
			});
			expect(await send({ chatId: "c", message: "m" })).toEqual({
				error: `Home Assistant API error (${status}): <html>oops</html>`,
			});
		}
	});

	it("timeout deaths map to the dedicated sentence, not the generic lane", async () => {
		const { send } = make({
			config: { url: "http://ha", token: "t" },
			outcomes: [{ kind: "timeout" }],
		});
		expect(await send({ chatId: "c", message: "m" })).toEqual({
			error: "Timeout sending notification to Home Assistant",
		});
	});

	it("other transport failures ⇒ 'Home Assistant send failed: {e}'", async () => {
		const thrown = make({
			config: { url: "http://ha", token: "t" },
			outcomes: [new Error("socket reset")],
		});
		expect(await thrown.send({ chatId: "c", message: "m" })).toEqual({
			error: "Home Assistant send failed: socket reset",
		});

		const scripted = make({
			config: { url: "http://ha", token: "t" },
			outcomes: [{ kind: "transport-failure", error: "ECONNREFUSED" }],
		});
		expect(await scripted.send({ chatId: "c", message: "m" })).toEqual({
			error: "Home Assistant send failed: ECONNREFUSED",
		});
	});
});

// ── registration seam ────────────────────────────────────────────────────────

describe("nthaha-10 HA: kit registration seam wires standaloneSenderFn", () => {
	it("registerHomeAssistantPlatform registers under 'homeassistant' with the sender retrievable and drivable", async () => {
		const { transport, requests } = scriptedTransport();
		const ctx = new PluginContext((k) =>
			k === "HASS_TOKEN" ? "tok" : undefined,
		);
		const disabled = registerHomeAssistantPlatform(ctx, () => ({}), {
			standalone: {
				transport,
				// The SENDER resolves lanes through ITS OWN scoped reader (Hermes
				// _get_scoped_secret parity), independent of the context's reader.
				secretReader: (k) => (k === "HASS_TOKEN" ? "tok" : process.env[k]),
			},
		});
		expect(disabled).toBeNull();
		expect(ctx.getPlatform("homeassistant")?.manifestName).toBe(
			"homeassistant",
		);

		const sender = ctx.getStandaloneSender("homeassistant");
		expect(sender).toBeTypeOf("function");
		// Env lanes drive resolution when no config overrides exist.
		process.env.HASS_URL = "http://reg-ha:8123";
		try {
			const result = await sender!({ chatId: "garage", message: "hi" });
			expect(result).toEqual({
				success: true,
				platform: "homeassistant",
				chatId: "garage",
			});
		} finally {
			delete process.env.HASS_URL;
		}
		expect(requests[0]!.url).toBe(`http://reg-ha:8123${HA_REST_NOTIFY_NOTIFY}`);
		expect(requests[0]!.payload.target).toBe("garage");
	});
});
