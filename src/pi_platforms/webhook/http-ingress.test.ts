// BEHAVIOR CONTRACTS — THE request pipeline (webhook.py:_handle_webhook order)
// driven framework-free over normalized requests. Deterministic: window races
// ride ManualTimers; the turn runner is scripted; NO sockets needed here (the
// socket-bound e2e row lives in conformance.test.ts).

import { createHmac } from "node:crypto";
import { describe, expect, it } from "vitest";
import {
	WebhookIngressPipeline,
	parseWebhookPath,
	type AgentDispatch,
	type HeldOpenSink,
} from "./http-ingress.js";
import { SlidingWindowRateLimiter } from "./rate-limit.js";
import { DeliveryIdempotencyStore } from "./idempotency.js";
import type { WebhookRouteConfig } from "./manifest.js";
import { webhookTrustBoundary } from "./manifest.js";
import { resolveScriptPath } from "./script-confinement.js";
import { ManualTimers } from "./testing/manual-timers.js";

const SECRET = "route-secret";

/** Test parse seam — mirrors production defaultParseJson's throw-on-invalid
 * contract, with the throw made explicit for lint clarity. */
/** Test parse seam — mirrors production defaultParseJson's throw-on-invalid
 * contract, with the throw made explicit for lint clarity. */
function testParseJson(text: string): Record<string, unknown> {
	let parsed: unknown;
	try {
		parsed = JSON.parse(text);
	} catch (err) {
		throw new Error(`Cannot parse body: ${String(err)}`);
	}
	if (parsed === null || typeof parsed !== "object" || Array.isArray(parsed)) {
		throw new Error("Cannot parse body: expected object");
	}
	return parsed as Record<string, unknown>;
}

function sign(body: string): string {
	return createHmac("sha256", SECRET).update(body).digest("hex");
}

interface HarnessOpts {
	routes?: WebhookRouteConfig[];
	windowCapMs?: number | undefined;
	/** Explicit null disables the global fallback secret. */
	globalSecret?: string | null | undefined;
	/** Harness rate limit (default 3 keeps trip tests cheap). */
	rateLimit?: number | undefined;
	/** Served-profile set (multiplex); omitted = single-profile gateway. */
	profilesAllowed?: ReadonlySet<string> | undefined;
}

function makePipeline(opts: HarnessOpts = {}) {
	const timers = new ManualTimers();
	const routes = opts.routes ?? [
		{
			name: "ci",
			secret: SECRET,
			events: ["push", "pull_request"],
			...(opts.windowCapMs !== undefined
				? { windowCapMs: opts.windowCapMs }
				: {}),
		},
	];
	let parseCalls = 0;
	const pipeline = new WebhookIngressPipeline({
		...(opts.profilesAllowed !== undefined
			? { profilesAllowed: opts.profilesAllowed }
			: {}),
		trust: webhookTrustBoundary(),
		routes: new Map(routes.map((r) => [r.name, r])),
		rateLimiter: new SlidingWindowRateLimiter({
			limit: opts.rateLimit ?? 3,
			nowMs: timers.nowMs,
		}),
		idempotency: new DeliveryIdempotencyStore({
			maxEntries: 128,
			nowMs: timers.nowMs,
		}),
		nowSeconds: timers.nowSeconds,
		timers,
		globalSecret:
			opts.globalSecret === null ? undefined : (opts.globalSecret ?? SECRET),
		parseJson: (text) => {
			parseCalls += 1;
			return testParseJson(text);
		},
		runAgentTurn: (dispatch) => turns.run(dispatch),
	});
	const turns = {
		script: [] as Array<(d: AgentDispatch) => Promise<string | null>>,
		dispatches: [] as AgentDispatch[],
		run(d: AgentDispatch): Promise<string | null> {
			this.dispatches.push(d);
			const next = this.script.shift();
			if (next === undefined) return Promise.resolve(`reply:${d.event.text}`);
			return next(d);
		},
	};
	function request(
		body: string,
		headerOverrides: Record<string, string> = {},
		path = "/webhooks/ci",
		contentLengthOverride?: number,
	) {
		return {
			method: "POST" as const,
			path,
			headers: {
				"content-type": "application/json",
				"x-hub-signature-256": `sha256=${sign(body)}`,
				"x-github-delivery": "delivery-1",
				"x-github-event": "push",
				...headerOverrides,
			},
			contentLength: contentLengthOverride ?? Buffer.byteLength(body),
			readBody: () => Promise.resolve(Buffer.from(body, "utf8")),
		};
	}
	return { pipeline, timers, turns, request, parseCalls: () => parseCalls };
}

describe("pipeline check ORDER (webhook.py:_handle_webhook parity)", () => {
	it("unknown route → 404 with the exact error shape", async () => {
		const h = makePipeline();
		const res = await h.pipeline.handle(h.request("{}", {}, "/webhooks/nope"));
		expect(res?.status).toBe(404);
		expect(res?.json).toEqual({ error: "Unknown route: nope" });
	});

	it("foreign prefix → 404 unconfigured-profile envelope (fail closed, #91583 defect 2)", async () => {
		// Single-profile gateway (no served set): a prefix naming ANY profile —
		// including one bound to the route elsewhere — must NOT fall through to
		// the default bot's routes. Upstream webhook.py:_resolve_profile_prefix
		// rejects _PROFILE_REJECTED with this exact envelope.
		const h = makePipeline({
			routes: [{ name: "ci", secret: SECRET, profiles: ["team-a"] }],
		});
		const res = await h.pipeline.handle(
			h.request("{}", {}, "/p/other/webhooks/ci"),
		);
		expect(res?.status).toBe(404);
		expect(res?.json).toEqual({ error: "Unknown or unconfigured profile" });
	});

	it("profile mismatch answers the SAME unknown-route shape (anti-enumeration)", async () => {
		const h = makePipeline({
			routes: [{ name: "ci", secret: SECRET, profiles: ["team-a"] }],
			profilesAllowed: new Set(["other"]),
		});
		const mismatch = await h.pipeline.handle(
			h.request("{}", {}, "/p/other/webhooks/ci"),
		);
		expect(mismatch?.status).toBe(404);
		// Same generic shape as an unknown route: probing profile bindings
		// reveals nothing about which routes exist.
		expect(mismatch?.json).toEqual({ error: "Unknown route: ci" });
		const unknownShape = await h.pipeline.handle(
			h.request("{}", {}, "/webhooks/ghost"),
		);
		expect(Object.keys(unknownShape?.json ?? {})).toEqual(
			Object.keys(mismatch?.json ?? {}),
		); // identical single-`error` envelope
	});

	it("disabled route → 403", async () => {
		const h = makePipeline({
			routes: [{ name: "off", secret: SECRET, enabled: false }],
		});
		const res = await h.pipeline.handle(h.request("{}", {}, "/webhooks/off"));
		expect(res?.status).toBe(403);
	});

	it("invalid signature → 401 BEFORE rate limit or parse", async () => {
		const h = makePipeline();
		const res = await h.pipeline.handle(
			h.request("{}", { "x-hub-signature-256": "sha256=bad" }),
		);
		expect(res?.status).toBe(401);
		expect(h.parseCalls()).toBe(0);
	});

	it("rate limit trips after signature passes; rejected requests skip parse", async () => {
		const h = makePipeline(); // limit 3 in harness
		for (let i = 0; i < 3; i++) {
			h.turns.script.push(() => Promise.resolve(null));
			const ok = await h.pipeline.handle(
				h.request("{}", { "x-github-delivery": `rl-${i}` }),
			);
			expect(ok?.status).toBe(200);
		}
		const tripped = await h.pipeline.handle(
			h.request("{}", { "x-github-delivery": "rl-over" }),
		);
		expect(tripped?.status).toBe(429);
		expect(tripped?.json).toEqual({ error: "Rate limit exceeded" });
		expect(h.parseCalls()).toBe(3); // the tripped one never parsed
	});
});

describe("body-size caps", () => {
	it("oversized Content-Length rejected BEFORE any body read or parse", async () => {
		const h = makePipeline();
		const tiny = "{}";
		const res = await h.pipeline.handle(
			h.request(tiny, {}, "/webhooks/ci", 2 * 1_048_576),
		);
		expect(res?.status).toBe(413);
		expect(res?.json).toEqual({ error: "Payload too large" });
	});

	it("lying Content-Length caught by the post-read byte count (defense in depth)", async () => {
		const h = makePipeline();
		const bigBody = "x".repeat(1_048_577);
		const res = await h.pipeline.handle({
			method: "POST",
			path: "/webhooks/ci",
			headers: {
				"x-hub-signature-256": `sha256=${sign(bigBody)}`,
				"x-github-delivery": "big-1",
			},
			contentLength: 2,
			readBody: () => Promise.resolve(Buffer.from(bigBody, "utf8")),
		});
		expect(res?.status).toBe(413);
		expect(h.parseCalls()).toBe(0);
	});

	it("unreadable body → 400", async () => {
		const h = makePipeline();
		const res = await h.pipeline.handle({
			method: "POST",
			path: "/webhooks/ci",
			headers: {},
			contentLength: 5,
			readBody: () => Promise.reject(new Error("socket blew up")),
		});
		expect(res?.status).toBe(400);
	});

	it("missing route secret fails CLOSED at request time too", async () => {
		const h = makePipeline({
			routes: [{ name: "nosecret" }],
			globalSecret: null,
		});
		const res = await h.pipeline.handle(
			h.request("{}", {}, "/webhooks/nosecret"),
		);
		expect(res?.status).toBe(403);
		expect(String(res?.json["error"])).toContain("HMAC secret");
	});
});

describe("event filters + idempotency + agent mode", () => {
	it("disallowed event types answer 200 ignored without dispatching a turn", async () => {
		const h = makePipeline();
		const res = await h.pipeline.handle(
			h.request("{}", { "x-github-event": "fork" }),
		);
		expect(res?.status).toBe(200);
		expect(res?.json).toEqual({ status: "ignored", event: "fork" });
		expect(h.turns.dispatches).toHaveLength(0);
	});

	it("agent mode completes inside the bounded window → reply rides the response", async () => {
		const h = makePipeline({ windowCapMs: 5_000 });
		const resP = h.pipeline.handle(h.request("{}"));
		const res = await resP;
		expect(res?.status).toBe(200);
		expect(res?.json["status"]).toBe("completed");
		expect(res?.json["reply"]).toBe("reply:{}");
	});

	it("turn outliving the window → bounded ack 202 NOW + late reply lands via the held-open ledger seam", async () => {
		const h = makePipeline({ windowCapMs: 5_000 });
		const heldOpen: Array<Record<string, unknown>> = [];
		// Rebuild the pipeline with a sink (harness-level injection).
		const sink: HeldOpenSink = {
			async holdOpen(entry) {
				heldOpen.push(entry as unknown as Record<string, unknown>);
				return { obligationId: `obl-${heldOpen.length}` };
			},
		};
		let pendingResolve: ((r: string | null) => void) | undefined;
		const gated = new WebhookIngressPipeline({
			trust: webhookTrustBoundary(),
			routes: new Map([
				["ci", { name: "ci", secret: SECRET, windowCapMs: 5_000 }],
			]),
			rateLimiter: new SlidingWindowRateLimiter({
				limit: 30,
				nowMs: h.timers.nowMs,
			}),
			idempotency: new DeliveryIdempotencyStore({
				maxEntries: 128,
				nowMs: h.timers.nowMs,
			}),
			nowSeconds: h.timers.nowSeconds,
			timers: h.timers,
			globalSecret: SECRET,
			parseJson: testParseJson,
			runAgentTurn: () =>
				new Promise<string | null>((resolve) => {
					pendingResolve = resolve;
				}),
			heldOpenSink: sink,
		});

		// Fire the request WITHOUT awaiting; the handler parks inside the
		// window race. Yield to let it reach the race, then expire the window.
		const responseP = gated.handle(h.request('{"job":"slow"}'));
		await new Promise<void>((r) => setTimeout(r, 2));
		h.timers.advance(5_100);
		const response = await responseP;
		// Bounded ack arrives IMMEDIATELY — the provider window is respected.
		expect(response?.status).toBe(202);
		expect(response?.json["status"]).toBe("accepted");

		// The turn eventually completes AFTER the window closed…
		pendingResolve?.("the late final answer");
		await new Promise<void>((r) => setTimeout(r, 2));

		// …and its reply is DURABLY recorded via the ledger seam, not dropped.
		expect(heldOpen).toHaveLength(1);
		expect(heldOpen[0]?.["content"]).toBe("the late final answer");
	});

	it("replayed delivery-id returns the CACHED outcome and never re-dispatches", async () => {
		const h = makePipeline({ windowCapMs: 5_000, rateLimit: 30 });
		h.turns.script.push(() => Promise.resolve("first-turn-reply"));
		const first = await h.pipeline.handle(
			h.request("{}", { "x-github-delivery": "same-id" }),
		);
		expect(first?.status).toBe(200);
		expect(first?.json).toEqual({
			status: "completed",
			route: "ci",
			event: "push",
			delivery_id: "same-id",
			reply: "first-turn-reply",
		});
		const turnCount = h.turns.dispatches.length;

		for (let i = 0; i < 3; i++) {
			const replay = await h.pipeline.handle(
				h.request("{}", { "x-github-delivery": "same-id" }),
			);
			expect(replay?.status).toBe(first?.status);
			expect(replay?.json).toEqual(first?.json);
		}
		expect(h.turns.dispatches.length).toBe(turnCount); // processed ONCE
	});

	it("deliver_only routes push synchronously: delivered 200 / failure 502", async () => {
		const h = makePipeline({
			routes: [{ name: "notify", secret: SECRET, deliverOnly: true }],
		});
		const gated = new WebhookIngressPipeline({
			trust: webhookTrustBoundary(),
			routes: new Map([
				["notify", { name: "notify", secret: SECRET, deliverOnly: true }],
			]),
			rateLimiter: new SlidingWindowRateLimiter({
				limit: 30,
				nowMs: h.timers.nowMs,
			}),
			idempotency: new DeliveryIdempotencyStore({
				maxEntries: 128,
				nowMs: h.timers.nowMs,
			}),
			nowSeconds: h.timers.nowSeconds,
			timers: h.timers,
			globalSecret: SECRET,
			parseJson: testParseJson,
			runAgentTurn: () => Promise.resolve(null),
			deliverOnly: async () => true,
		});
		const okRes = await gated.handle(
			h.request("{}", { "x-github-delivery": "do-1" }, "/webhooks/notify"),
		);
		expect(okRes?.status).toBe(200);
		expect(okRes?.json["status"]).toBe("delivered");
	});
});

describe("script confinement (DEC-017 relative_to)", () => {
	it("accepts scripts under home", () => {
		const r = resolveScriptPath("/home/hermes", "scripts/transform.py");
		expect(r.ok).toBe(true);
	});
	it("rejects traversal escapes", () => {
		const r = resolveScriptPath("/home/hermes", "../../etc/passwd");
		expect(r.ok).toBe(false);
	});
	it("rejects absolute paths", () => {
		expect(resolveScriptPath("/home/hermes", "/etc/passwd").ok).toBe(false);
	});
	it("rejects sibling-prefix escapes (/home/x-evil vs /home/x)", () => {
		const r = resolveScriptPath("/home/x", "../x-evil/payload.sh");
		expect(r.ok).toBe(false);
	});
});

describe("path parsing", () => {
	it("parses plain and profiled webhook paths", () => {
		expect(parseWebhookPath("/webhooks/ci")).toEqual({
			routeName: "ci",
		});
		expect(parseWebhookPath("/p/team-a/webhooks/ci")).toEqual({
			routeName: "ci",
			profile: "team-a",
		});
		expect(parseWebhookPath("/v1/runs")).toBeNull();
	});
});
